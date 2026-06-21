/*-------------------------------------------------------------------------
 *
 * pagestore_test.c
 *	  Standalone test harness for the pagestore daemon and its shared-memory
 *	  IPC protocol -- runs WITHOUT PostgreSQL.
 *
 * The daemon is the "independent service" half of the design, and it speaks a
 * self-contained shared-memory protocol (pagestore_ipc.h).  This program plays
 * the role of the engine-side client: it fork/execs the daemon, attaches the
 * shared memory, claims a channel, and drives requests directly -- exercising
 * the storage logic, COW versioning, segment rollover, crash recovery and
 * page-size independence with no PostgreSQL instance involved.
 *
 * Usage: pagestore_test <path-to-pagestore_daemon>
 * Exit status: 0 = all tests passed, 1 = one or more failed.
 *
 * src/../contrib/pagestore/pagestore_test.c
 *
 *-------------------------------------------------------------------------
 */
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"

/* ===================== tiny test framework ============================= */

static int	tests_run = 0;
static int	tests_failed = 0;

static void check(int cond, const char *fmt, ...)
	__attribute__((format(printf, 2, 3)));

static void
check(int cond, const char *fmt, ...)
{
	va_list		ap;

	tests_run++;
	if (cond)
		return;
	tests_failed++;
	va_start(ap, fmt);
	fprintf(stderr, "  FAIL: ");
	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");
	va_end(ap);
}

/* Best-effort recursive remove of a directory (test setup/teardown). */
static void
rm_rf(const char *path)
{
	char		cmd[512];

	snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
	if (system(cmd) != 0)
	{
		/* ignore: cleanup is best-effort */
	}
}

/* ===================== client side of the IPC protocol ================= */

static void *cl_shm;
static int	cl_shm_fd;
static int	cl_pool[PS_MAX_SHARDS]; /* channel claimed in each shard pool, -1 = none */
static uint32_t cl_nshards;			/* shard count published by the daemon */
static uint32_t cl_nchannels;		/* channel count published by the daemon */
static uint32_t g_nshards = 1;		/* --nshards to spawn the daemon with */
static const char *g_object_dir = NULL;	/* if set, spawn the daemon with --object-dir */
static uint32_t cl_page_size;

static void
client_attach(const char *shm_name, uint32_t expect_page_size)
{
	PsShmHeader *hdr;

	cl_shm_fd = shm_open(shm_name, O_RDWR, 0600);
	if (cl_shm_fd < 0)
	{
		perror("client shm_open");
		exit(2);
	}
	cl_shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
				  cl_shm_fd, 0);
	if (cl_shm == MAP_FAILED)
	{
		perror("client mmap");
		exit(2);
	}
	hdr = (PsShmHeader *) cl_shm;
	/* acquire-load magic (the daemon's readiness sentinel) so the descriptive
	 * fields published before it -- nshards, geometry -- are visible here; pairs
	 * with the daemon's release fence before the magic store */
	check(ps_load_acquire(&hdr->magic) == PS_SHM_MAGIC, "shm magic");
	check(hdr->version == PS_SHM_VERSION, "shm version %u expected %u",
		  hdr->version, PS_SHM_VERSION);
	check(hdr->page_size == expect_page_size,
		  "header page_size=%u expected %u", hdr->page_size, expect_page_size);
	cl_page_size = hdr->page_size;

	/* Adopt the daemon's published shard count and route to match.  It must fit
	 * the client's compile-time cap (both build from PS_MAX_SHARDS); reject an
	 * out-of-range count rather than mis-size the pool array. */
	cl_nchannels = hdr->nchannels;
	cl_nshards = hdr->nshards ? hdr->nshards : 1;
	if (cl_nshards > PS_MAX_SHARDS)
	{
		fprintf(stderr, "daemon nshards=%u exceeds client PS_MAX_SHARDS=%u\n",
				cl_nshards, (uint32_t) PS_MAX_SHARDS);
		exit(2);
	}
	for (uint32_t s = 0; s < cl_nshards; s++)
		cl_pool[s] = -1;
}

static void
client_detach(void)
{
	if (cl_shm)
	{
		for (uint32_t s = 0; s < cl_nshards; s++)
			if (cl_pool[s] >= 0)
				ps_store_release(&ps_channel(cl_shm, cl_pool[s])->claimed, 0);
		munmap(cl_shm, PS_SHM_SIZE);
		cl_shm = NULL;
	}
	if (cl_shm_fd >= 0)
	{
		close(cl_shm_fd);
		cl_shm_fd = -1;
	}
}

static PsChannel *
cl_exec(PsChannel *ch)
{
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;					/* busy wait; single in-flight request */
	return ch;
}

/* the channel this client uses for shard 's', claiming one in that shard's pool
 * on first use */
static PsChannel *
cl_claim_shard(uint32_t s)
{
	if (cl_pool[s] < 0)
	{
		uint32_t	first,
					cnt;

		ps_shard_channel_range(s, cl_nshards, cl_nchannels, &first, &cnt);
		for (uint32_t i = first; i < first + cnt; i++)
			if (ps_cas(&ps_channel(cl_shm, i)->claimed, 0, 1))
			{
				cl_pool[s] = (int) i;
				break;
			}
		if (cl_pool[s] < 0)
		{
			fprintf(stderr, "no free channel in shard %u\n", s);
			exit(2);
		}
	}
	return ps_channel(cl_shm, cl_pool[s]);
}

/* route a request for (rel, fork) to the channel in that key's shard pool */
static PsChannel *
cl_route(uint32_t rel, int32_t fork)
{
	PsKey		k = {1, 1, rel, fork};

	return cl_claim_shard(ps_shard_for_key(&k, cl_nshards));
}

/* keyless ops (branch/timeline create) go to shard 0's channel */
static PsChannel *
cl_route0(void)
{
	return cl_claim_shard(0);
}

static void
cl_setkey(PsChannel *ch, uint32_t rel, int32_t fork)
{
	ch->key.spcOid = 1;
	ch->key.dbOid = 1;
	ch->key.relNumber = rel;
	ch->key.forkNum = fork;
	ch->timeline = 0;			/* default to the main timeline */
}

/* --- typed operations --- */

static void
op_create(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_CREATE;
	cl_exec(ch);
}

static int
op_exists(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_EXISTS;
	return cl_exec(ch)->result != 0;
}

static void
op_unlink(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_UNLINK;
	cl_exec(ch);
}

static uint32_t
op_nblocks(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_NBLOCKS;
	return cl_exec(ch)->result;
}

static void
op_truncate(uint32_t rel, int32_t fork, uint32_t nblocks)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_TRUNCATE;
	ch->nblocks = nblocks;
	cl_exec(ch);
}

static void
op_zeroextend(uint32_t rel, int32_t fork, uint32_t block, uint32_t nblocks)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = block;
	ch->nblocks = nblocks;
	cl_exec(ch);
}

static void
op_write_one(uint32_t rel, int32_t fork, uint32_t block, const unsigned char *page)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, cl_page_size);
	cl_exec(ch);
}

static void
op_read_one(uint32_t rel, int32_t fork, uint32_t block, unsigned char *out)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = 1;
	cl_exec(ch);
	memcpy(out, ch->data, cl_page_size);
}

/* Vectored write of n contiguous pages in a single op. */
static void
op_writev(uint32_t rel, int32_t fork, uint32_t block, const unsigned char *pages,
		  uint32_t n)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = n;
	memcpy(ch->data, pages, (size_t) n * cl_page_size);
	cl_exec(ch);
}

/* Vectored read of n contiguous pages in a single op. */
static void
op_readv(uint32_t rel, int32_t fork, uint32_t block, unsigned char *out,
		 uint32_t n)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = n;
	cl_exec(ch);
	memcpy(out, ch->data, (size_t) n * cl_page_size);
}

static void
op_read_at(uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn,
		   unsigned char *out)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec(ch);
	memcpy(out, ch->data, cl_page_size);
}

/* --- timeline-aware operations (for branch tests) --- */

/* Create timeline new_tl as a branch of parent_tl forked at branch_lsn. */
static void
op_create_branch(uint32_t new_tl, uint32_t parent_tl, uint64_t branch_lsn)
{
	PsChannel  *ch = cl_route0();

	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	cl_exec(ch);
}

/* Like op_create_branch but returns the daemon's status (for negative tests). */
static int
op_create_branch_status(uint32_t new_tl, uint32_t parent_tl, uint64_t branch_lsn)
{
	PsChannel  *ch = cl_route0();

	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	return cl_exec(ch)->status;
}

/* Write one page on a specific timeline. */
static void
op_write_tl(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
			const unsigned char *page)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, cl_page_size);
	cl_exec(ch);
}

/* Read one page (current) on a specific timeline. */
static void
op_read_tl(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
		   unsigned char *out)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = 1;
	cl_exec(ch);
	memcpy(out, ch->data, cl_page_size);
}

/* Read one page as-of an LSN on a specific timeline. */
static void
op_read_at_tl(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
			  uint64_t lsn, unsigned char *out)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec(ch);
	memcpy(out, ch->data, cl_page_size);
}

/* --- shipped-WAL operations --- */

/* Append len WAL bytes at start_lsn on a timeline. */
static void
op_wal_append(uint32_t tl, uint64_t start_lsn, const void *data, uint32_t len)
{
	PsChannel  *ch = cl_route0();

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	memcpy(ch->data, data, len);
	cl_exec(ch);
}

/* Return the end LSN of a timeline's shipped WAL. */
static uint64_t
op_wal_size(uint32_t tl)
{
	PsChannel  *ch = cl_route0();

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_SIZE;
	cl_exec(ch);
	return ch->req_lsn;
}

/* Read len WAL bytes from start_lsn into out; returns bytes filled. */
static uint32_t
op_wal_read(uint32_t tl, uint64_t start_lsn, uint32_t len, void *out)
{
	PsChannel  *ch = cl_route0();

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	cl_exec(ch);
	memcpy(out, ch->data, len);
	return ch->result;
}

/* --- per-page WAL index operations --- */

static void
op_walidx_add(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_INDEX_ADD;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec(ch);
}

/* Record: this fork's size became nblocks as of WAL record 'lsn' (is_trunc=1 for
 * a truncation -- exact shrink; is_trunc=0 for an extension -- grow-only). */
static void
op_forksize_add(uint32_t tl, uint32_t rel, int32_t fork, uint64_t lsn,
				uint32_t nblocks, int is_trunc)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_FORK_SIZE_ADD;
	ch->req_lsn = lsn;
	ch->nblocks = nblocks;
	ch->blocknum = (uint32_t) is_trunc;
	cl_exec(ch);
}

/* The fork's size (blocks) as of 'lsn'; PS_FORKSIZE_UNKNOWN if none at/below it. */
static uint32_t
op_forksize_at(uint32_t tl, uint32_t rel, int32_t fork, uint64_t lsn)
{
	PsChannel  *ch = cl_route(rel, fork);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_FORK_SIZE_AT;
	ch->req_lsn = lsn;
	return cl_exec(ch)->result;
}

/* Returns count; fills out[] with the record LSNs <= lsn_max, and -- when
 * out_tl != NULL -- the parallel source timeline of each LSN. */
static int
op_walidx_get(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
			  uint64_t lsn_max, uint64_t *out, uint32_t *out_tl)
{
	PsChannel  *ch = cl_route(rel, fork);
	int			total,
				ndata;

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_INDEX_GET;
	ch->blocknum = block;
	ch->req_lsn = lsn_max;
	total = (int) cl_exec(ch)->result;	/* true match count */
	ndata = total < PS_WALIDX_CAP ? total : PS_WALIDX_CAP;	/* pairs in payload */
	memcpy(out, ch->data, (size_t) ndata * sizeof(uint64_t));
	if (out_tl)
		memcpy(out_tl, ch->data + (size_t) PS_WALIDX_CAP * sizeof(uint64_t),
			   (size_t) ndata * sizeof(uint32_t));
	return total;
}

/* ===================== page helpers ==================================== */

/* Encode lsn into the page's first 8 bytes (xlogid, xrecoff), tag the rest. */
static void
fill_page(unsigned char *buf, uint32_t ps, uint64_t lsn, unsigned char tag)
{
	uint32_t	xlogid = (uint32_t) (lsn >> 32);
	uint32_t	xrecoff = (uint32_t) (lsn & 0xFFFFFFFF);

	memcpy(buf, &xlogid, 4);
	memcpy(buf + 4, &xrecoff, 4);
	for (uint32_t i = 8; i < ps; i++)
		buf[i] = (unsigned char) (tag ^ (i & 0xFF));
}

static int
page_has_tag(const unsigned char *buf, uint32_t ps, unsigned char tag)
{
	for (uint32_t i = 8; i < ps; i++)
		if (buf[i] != (unsigned char) (tag ^ (i & 0xFF)))
			return 0;
	return 1;
}

static int
page_all_zero(const unsigned char *buf, uint32_t ps)
{
	for (uint32_t i = 0; i < ps; i++)
		if (buf[i] != 0)
			return 0;
	return 1;
}

/* ===================== daemon lifecycle ================================ */

static pid_t
spawn_daemon(const char *daemon_path, const char *shm, const char *store,
			 uint32_t page_size)
{
	pid_t		pid = fork();

	if (pid < 0)
	{
		perror("fork");
		exit(2);
	}
	if (pid == 0)
	{
		char		psbuf[16];
		char		nsbuf[16];
		const char *argv[24];
		int			a = 0;

		snprintf(psbuf, sizeof(psbuf), "%u", page_size);
		snprintf(nsbuf, sizeof(nsbuf), "%u", g_nshards);
		/* small segments exercise rollover; a small flush threshold makes the
		 * tests flush into image layers so the layer read path is exercised */
		argv[a++] = daemon_path;
		argv[a++] = "--shm";		argv[a++] = shm;
		argv[a++] = "--store";		argv[a++] = store;
		argv[a++] = "--page-size";	argv[a++] = psbuf;
		argv[a++] = "--segment-size"; argv[a++] = "65536";
		argv[a++] = "--flush-pages"; argv[a++] = "8";
		argv[a++] = "--compact-layers"; argv[a++] = "3";
		argv[a++] = "--nshards";	argv[a++] = nsbuf;
		if (g_object_dir != NULL)
		{
			argv[a++] = "--object-dir";
			argv[a++] = g_object_dir;
		}
		argv[a] = NULL;
		execv(daemon_path, (char *const *) argv);
		perror("execv daemon");
		_exit(127);
	}
	return pid;
}

/* Wait until the daemon has published a valid header. */
static void
wait_ready(const char *shm, uint32_t page_size)
{
	for (int i = 0; i < 500; i++)	/* up to ~5s */
	{
		int			fd = shm_open(shm, O_RDWR, 0600);

		if (fd >= 0)
		{
			PsShmHeader *h = mmap(NULL, sizeof(PsShmHeader), PROT_READ,
								  MAP_SHARED, fd, 0);

			if (h != MAP_FAILED)
			{
				/* acquire-load magic first: it is the daemon's readiness sentinel,
				 * so observing it published makes the fields written before it
				 * visible (pairs with the daemon's pre-magic release fence) */
				int			ready = (ps_load_acquire(&h->magic) == PS_SHM_MAGIC &&
									 h->version == PS_SHM_VERSION &&
									 h->page_size == page_size &&
									 h->nchannels == PS_MAX_CHANNELS);

				munmap(h, sizeof(PsShmHeader));
				close(fd);
				if (ready)
					return;
			}
			else
				close(fd);
		}
		usleep(10000);
	}
	fprintf(stderr, "daemon did not become ready\n");
	exit(2);
}

static void
stop_daemon(pid_t pid)
{
	kill(pid, SIGTERM);
	waitpid(pid, NULL, 0);
}

/* Hard crash: SIGKILL gives the daemon no chance to flush the memtable, so only
 * what is durable in the segment log may survive the restart. */
static void
kill_daemon_hard(pid_t pid)
{
	kill(pid, SIGKILL);
	waitpid(pid, NULL, 0);
}

/* ===================== the test suite ================================== */

#define REL_A	16000
#define REL_B	17000
#define FORK0	0

static void
run_suite(const char *daemon_path, const char *tmpbase, uint32_t page_size)
{
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	unsigned char *pa,
			   *pb,
			   *rb;

	fprintf(stderr, "== page_size=%u ==\n", page_size);

	snprintf(shm, sizeof(shm), "/pstest_%d_%u", (int) getpid(), page_size);
	snprintf(store, sizeof(store), "%s/store_%u", tmpbase, page_size);
	rm_rf(store);				/* start from a clean store */
	shm_unlink(shm);

	pa = malloc(page_size);
	pb = malloc(page_size);
	rb = malloc(page_size);

	dpid = spawn_daemon(daemon_path, shm, store, page_size);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);

	/* --- lifecycle / metadata --- */
	check(!op_exists(REL_A, FORK0), "fork should not exist before create");
	op_create(REL_A, FORK0);
	check(op_exists(REL_A, FORK0), "fork exists after create");
	check(op_nblocks(REL_A, FORK0) == 0, "empty fork has 0 blocks");

	/* --- write / read round-trip across many blocks (forces rollover) --- */
	for (uint32_t b = 0; b < 24; b++)
	{
		fill_page(pa, page_size, 1000 + b, (unsigned char) (b + 1));
		op_write_one(REL_A, FORK0, b, pa);
	}
	check(op_nblocks(REL_A, FORK0) == 24, "nblocks==24 after writes");
	{
		int			ok = 1;

		for (uint32_t b = 0; b < 24; b++)
		{
			op_read_one(REL_A, FORK0, b, rb);
			if (!page_has_tag(rb, page_size, (unsigned char) (b + 1)))
				ok = 0;
		}
		check(ok, "all 24 blocks read back correctly (across segments)");
	}

	/* --- zeroextend then read zeros --- */
	op_zeroextend(REL_A, FORK0, 24, 4);
	check(op_nblocks(REL_A, FORK0) == 28, "nblocks==28 after zeroextend");
	op_read_one(REL_A, FORK0, 26, rb);
	check(page_all_zero(rb, page_size), "zero-extended block reads as zeros");

	/* --- truncate --- */
	op_truncate(REL_A, FORK0, 10);
	check(op_nblocks(REL_A, FORK0) == 10, "nblocks==10 after truncate");

	/* --- COW / time-travel read on a dedicated block --- */
	fill_page(pa, page_size, 5000, 100);	/* version 1 @ lsn 5000 */
	op_write_one(REL_A, FORK0, 0, pa);
	fill_page(pb, page_size, 9000, 200);	/* version 2 @ lsn 9000 */
	op_write_one(REL_A, FORK0, 0, pb);

	op_read_one(REL_A, FORK0, 0, rb);
	check(page_has_tag(rb, page_size, 200), "current read returns newest version");
	op_read_at(REL_A, FORK0, 0, 7000, rb);	/* between the two versions */
	check(page_has_tag(rb, page_size, 100), "read_at(7000) returns old version (COW)");
	op_read_at(REL_A, FORK0, 0, 9000, rb);
	check(page_has_tag(rb, page_size, 200), "read_at(9000) returns new version");
	op_read_at(REL_A, FORK0, 0, ~0ull, rb);
	check(page_has_tag(rb, page_size, 200), "read_at(max) returns newest");

	/* a fresh write right before the crash (while still attached): it is
	 * acknowledged and durable in the segment log, but likely still unflushed --
	 * exactly the tail a layer-only restart would drop */
	fill_page(pa, page_size, 12000, 222);
	op_write_one(REL_A, FORK0, 9, pa);

	client_detach();

	/* --- crash recovery: restart the daemon and confirm acknowledged writes
	 * survive.  For a backend whose writes are durable on ack (the POSIX file
	 * backend -- the bytes are in the OS page cache) use a real hard crash
	 * (SIGKILL, no clean shutdown -> the memtable is never flushed), exercising
	 * segment-tail recovery.  The SPDK backend buffers the current append segment
	 * in DMA memory and flushes only on roll-over / clean shutdown, so a SIGKILL
	 * would legitimately drop the unflushed tail; restart it cleanly (which
	 * flushes) rather than assert a durability it does not promise. --- */
	if (strstr(daemon_path, "spdk") != NULL)
		stop_daemon(dpid);
	else
		kill_daemon_hard(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);

	check(op_exists(REL_A, FORK0), "fork still exists after a daemon crash/restart");
	op_read_one(REL_A, FORK0, 9, rb);
	check(page_has_tag(rb, page_size, 222),
		  "write just before the crash survives the restart");
	op_read_one(REL_A, FORK0, 5, rb);
	check(page_has_tag(rb, page_size, 6), "block 5 survives the restart (recovered)");
	op_read_at(REL_A, FORK0, 0, 7000, rb);
	check(page_has_tag(rb, page_size, 100),
		  "COW history survives the restart (read_at old version)");

	/* --- unlink --- */
	op_unlink(REL_A, FORK0);
	check(!op_exists(REL_A, FORK0), "fork gone after unlink");

	client_detach();
	stop_daemon(dpid);

	rm_rf(store);
	shm_unlink(shm);
	free(pa);
	free(pb);
	free(rb);
}

/*
 * Branch / snapshot isolation: a branch is an instant, copy-on-write clone.
 * It shares the parent's pages by read-through until it writes; its writes do
 * not affect the parent or sibling branches; and it sees the parent frozen at
 * the branch LSN, not the parent's later writes.
 */
static void
run_branch_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	uint32_t	ps = 8192;
	unsigned char *p,
			   *rb;

	fprintf(stderr, "== branches ==\n");

	snprintf(shm, sizeof(shm), "/pstest_%d_br", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_br", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	p = malloc(ps);
	rb = malloc(ps);

	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);

	/* base data on the main timeline (block 0 = tag 11 @ lsn 1000) */
	fill_page(p, ps, 1000, 11);
	op_write_tl(0, REL_B, FORK0, 0, p);

	/* branch T1 off main at LSN 1500 -- instant, no data copied */
	op_create_branch(1, 0, 1500);
	op_read_tl(1, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 11), "branch sees parent page via read-through (no copy)");

	/* writing on T1 diverges it (copy-on-write) */
	fill_page(p, ps, 2000, 22);
	op_write_tl(1, REL_B, FORK0, 0, p);
	op_read_tl(1, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 22), "branch read sees its own write");
	op_read_tl(0, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 11), "parent unaffected by branch write (isolation)");

	/* a second, independent branch off main */
	op_create_branch(2, 0, 1500);
	op_read_tl(2, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 11), "second branch independent, sees parent");
	fill_page(p, ps, 3000, 33);
	op_write_tl(2, REL_B, FORK0, 0, p);
	op_read_tl(2, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 33), "T2 read sees T2 write");
	op_read_tl(1, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 22), "T1 unaffected by T2 (three-way isolation)");
	op_read_tl(0, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 11), "main unaffected by T1/T2");

	/* as-of read on a branch, before its own write, falls through to parent */
	op_read_at_tl(1, REL_B, FORK0, 0, 1900, rb);
	check(page_has_tag(rb, ps, 11), "as-of read on branch before its write -> parent");

	/* snapshot semantics: parent evolves after the branch point (LSN > 1500) */
	fill_page(p, ps, 1200, 51);
	op_write_tl(0, REL_B, FORK0, 5, p);		/* block5 @ lsn 1200 (< branch) */
	fill_page(p, ps, 2000, 52);
	op_write_tl(0, REL_B, FORK0, 5, p);		/* block5 @ lsn 2000 (> branch) */
	op_read_tl(1, REL_B, FORK0, 5, rb);
	check(page_has_tag(rb, ps, 51),
		  "branch sees parent as-of branch LSN, not later writes (snapshot)");
	op_read_tl(0, REL_B, FORK0, 5, rb);
	check(page_has_tag(rb, ps, 52), "main sees its latest write to block5");

	/* branches survive a daemon restart (timeline metadata is persisted) */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_read_tl(1, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 22), "branch write survives daemon restart");
	op_read_tl(1, REL_B, FORK0, 5, rb);
	check(page_has_tag(rb, ps, 51), "branch snapshot view survives restart");

	/* CREATE_BRANCH validation: reject requests that would corrupt the parent
	 * walk (timelines 0,1,2 are defined here) */
	check(op_create_branch_status(1, 0, 1500) == PS_STATUS_ERROR,
		  "reject re-creating an existing timeline id");
	check(op_create_branch_status(9, 900, 1500) == PS_STATUS_ERROR,
		  "reject branch off an undefined parent");
	check(op_create_branch_status(9, 9, 1500) == PS_STATUS_ERROR,
		  "reject self-parent (would loop the read path)");
	check(op_create_branch_status(0, 0, 1500) == PS_STATUS_ERROR,
		  "reject redefining the root timeline");
	check(op_create_branch_status(7, 2, 1500) == PS_STATUS_OK,
		  "valid branch off a defined branch still accepted");
	op_read_tl(7, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 11), "new valid branch reads through to root");

	/*
	 * Cross-shard branch coordination (sharding step 6): a branch is created on
	 * shard 0, but its pages route by key to every shard, so each shard must see
	 * the timeline (timeline_define broadcasts the definition to all shards'
	 * replicas).  Use several relations whose keys hash to different shards and
	 * check read-through + copy-on-write + isolation on each -- so a branch that
	 * reached only shard 0 would fail here at nshards > 1.
	 */
	{
		uint32_t	rels[] = {41001, 41002, 41003, 41004,
			41005, 41006, 41007, 41008};
		uint32_t	nrels = sizeof(rels) / sizeof(rels[0]);
		uint32_t	shardmask = 0;

		/* seed each rel on main (@ lsn 1000, before the T8 branch point) */
		for (uint32_t i = 0; i < nrels; i++)
		{
			PsKey		k = {1, 1, rels[i], FORK0};

			if (g_nshards > 1)
				shardmask |= 1u << (ps_shard_for_key(&k, g_nshards) % 32);
			fill_page(p, ps, 1000, (unsigned char) (100 + i));
			op_write_tl(0, rels[i], FORK0, 0, p);
		}
		if (g_nshards > 1)
			check(__builtin_popcount(shardmask) > 1,
				  "cross-shard branch test spans multiple shards");

		op_create_branch(8, 0, 1500);	/* branch off main @ 1500 */

		for (uint32_t i = 0; i < nrels; i++)
		{
			/* the branch must be visible on this rel's shard: read-through to
			 * the parent's page seeded above */
			op_read_tl(8, rels[i], FORK0, 0, rb);
			check(page_has_tag(rb, ps, (unsigned char) (100 + i)),
				  "cross-shard branch reads through to parent on its shard");
			/* copy-on-write on the branch, on this shard */
			fill_page(p, ps, 2000, (unsigned char) (200 + i));
			op_write_tl(8, rels[i], FORK0, 0, p);
			op_read_tl(8, rels[i], FORK0, 0, rb);
			check(page_has_tag(rb, ps, (unsigned char) (200 + i)),
				  "cross-shard branch sees its own write on its shard");
			/* parent unaffected (isolation) on this shard */
			op_read_tl(0, rels[i], FORK0, 0, rb);
			check(page_has_tag(rb, ps, (unsigned char) (100 + i)),
				  "cross-shard parent unaffected by branch write");
		}
	}

	client_detach();
	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
	free(p);
	free(rb);
}

/*
 * Shipped WAL: the store persists a per-timeline WAL log durably, branches keep
 * their own log, and the end LSN survives a daemon restart.  (This is the
 * transport/durability layer; replaying it to pages -- redo -- is future work.)
 */
static void
run_wal_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	uint32_t	ps = 8192;
	unsigned char bufa[500];
	unsigned char bufb[500];
	unsigned char rback[512];
	int			ok;

	fprintf(stderr, "== shipped WAL ==\n");
	memset(bufa, 0xAA, sizeof(bufa));	/* WAL [1000,1500) */
	memset(bufb, 0xBB, sizeof(bufb));	/* WAL [1500,2000) */

	snprintf(shm, sizeof(shm), "/pstest_%d_wal", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_wal", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);

	check(op_wal_size(0) == 0, "empty timeline has no WAL");
	op_wal_append(0, 1000, bufa, 500);
	check(op_wal_size(0) == 1500, "WAL end LSN advances after append");
	op_wal_append(0, 1500, bufb, 500);
	check(op_wal_size(0) == 2000, "WAL end LSN advances after second append");

	/* read the WAL back, spanning the two records, and check positions */
	check(op_wal_read(0, 1400, 200, rback) == 200, "WAL read returns the requested bytes");
	ok = 1;
	for (int i = 0; i < 100; i++)	/* [1400,1500) came from the 0xAA record */
		if (rback[i] != 0xAA)
			ok = 0;
	for (int i = 100; i < 200; i++)	/* [1500,1600) from the 0xBB record */
		if (rback[i] != 0xBB)
			ok = 0;
	check(ok, "WAL read assembles the right bytes across records (at their LSNs)");

	/* a branch keeps its own WAL log */
	op_create_branch(1, 0, 2000);
	op_wal_append(1, 2000, bufa, 300);
	check(op_wal_size(1) == 2300, "branch WAL advances independently");
	check(op_wal_size(0) == 2000, "parent WAL unaffected by branch WAL");

	/* WAL end LSNs survive a daemon restart (recovered from the logs) */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_wal_size(0) == 2000, "main WAL end LSN survives daemon restart");
	check(op_wal_size(1) == 2300, "branch WAL end LSN survives daemon restart");

	client_detach();
	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
}

/*
 * Per-page WAL index: record which WAL LSNs modify each page and query the ones
 * visible as-of an LSN -- the lookup single-page materialization will use.
 */
static void
run_walidx_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	uint32_t	ps = 8192;
	uint64_t	out[16];
	int		n;

	fprintf(stderr, "== per-page WAL index ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_widx", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_widx", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);

	/* block 0 changed by records at LSN 100, 200, 300; block 1 at 150 */
	op_walidx_add(0, REL_A, FORK0, 0, 100);
	op_walidx_add(0, REL_A, FORK0, 0, 200);
	op_walidx_add(0, REL_A, FORK0, 0, 300);
	op_walidx_add(0, REL_A, FORK0, 1, 150);

	n = op_walidx_get(0, REL_A, FORK0, 0, 250, out, NULL);
	check(n == 2 && out[0] == 100 && out[1] == 200,
		  "index returns records <= lsn (block 0 as-of 250 -> [100,200])");
	n = op_walidx_get(0, REL_A, FORK0, 0, 1000000, out, NULL);
	check(n == 3 && out[2] == 300, "index returns all records up to a high lsn");
	check(op_walidx_get(0, REL_A, FORK0, 0, 50, out, NULL) == 0, "no records below the first lsn");
	n = op_walidx_get(0, REL_A, FORK0, 1, 200, out, NULL);
	check(n == 1 && out[0] == 150, "per-block separation (block 1 -> [150])");
	check(op_walidx_get(0, REL_A, FORK0, 9, 1000000, out, NULL) == 0, "unindexed block -> empty");

	/* a branch sees its own records plus the parent's, capped at the fork LSN,
	 * and each returned LSN is tagged with the timeline it came from */
	op_create_branch(1, 0, 250);
	op_walidx_add(1, REL_A, FORK0, 0, 400);
	{
		uint32_t	tls[64];
		int			from_child = 0,
					from_parent = 0;

		n = op_walidx_get(1, REL_A, FORK0, 0, 1000000, out, tls);
		/* branch's 400, plus parent's <= branch_lsn 250 (100,200; not 300) */
		check(n == 3, "branch index reads through to parent capped at the branch lsn");
		for (int i = 0; i < n; i++)
		{
			if (out[i] == 400)
				from_child += (tls[i] == 1);
			else
				from_parent += (tls[i] == 0);
		}
		check(from_child == 1 && from_parent == 2,
			  "branch index tags each LSN with its source timeline (400->tl1, 100/200->tl0)");
	}

	/* --- LSN-versioned fork size (the redo step-0 "is block live as-of LSN" check) --- */

	/* REL_B fork grows to 10 blocks @100, truncated to 3 @200, re-extended to 7 @300 */
	op_forksize_add(0, REL_B, FORK0, 100, 10, 0);
	op_forksize_add(0, REL_B, FORK0, 200, 3, 1);
	op_forksize_add(0, REL_B, FORK0, 300, 7, 0);
	/* an extension to a block below the current size must NOT shrink it */
	op_forksize_add(0, REL_B, FORK0, 320, 4, 0);

	check(op_forksize_at(0, REL_B, FORK0, 50) == PS_FORKSIZE_UNKNOWN,
		  "fork size unknown before the first size event");
	check(op_forksize_at(0, REL_B, FORK0, 150) == 10, "fork size as-of 150 -> 10 (after extend)");
	check(op_forksize_at(0, REL_B, FORK0, 250) == 3, "fork size as-of 250 -> 3 (after truncate)");
	check(op_forksize_at(0, REL_B, FORK0, 1000) == 7, "fork size as-of 1000 -> 7 (re-extend; low write ignored)");
	/* block 5 is gone at 250 (size 3) but live again at 1000 (size 7) */
	check(op_forksize_at(0, REL_B, FORK0, 250) <= 5 && op_forksize_at(0, REL_B, FORK0, 1000) > 5,
		  "truncate-then-re-extend: block 5 dead as-of 250, live as-of 1000");

	/* a branch inherits the parent's size as-of the fork LSN, then overrides */
	op_create_branch(2, 0, 250);		/* fork REL_B's timeline at lsn 250 (size 3) */
	op_forksize_add(2, REL_B, FORK0, 400, 9, 0);
	check(op_forksize_at(2, REL_B, FORK0, 300) == 3,
		  "branch sees parent size capped at the fork lsn (3, not the parent's later 7)");
	check(op_forksize_at(2, REL_B, FORK0, 500) == 9, "branch's own later size event wins (9)");

	client_detach();
	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
}

/*
 * Vectored I/O: a single WRITEV/READV carrying many pages, exercising the
 * daemon's multi-block loop (the single-block suites never do nblocks > 1).
 */
static void
run_vectored_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	uint32_t	ps = 8192;
	uint32_t	nb = 16;		/* fits one io_unit (256K / 8K = 32) */
	unsigned char *wbuf,
			   *rbuf;
	int			ok = 1;

	fprintf(stderr, "== vectored I/O ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_vec", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_vec", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	wbuf = malloc((size_t) nb * ps);
	rbuf = malloc((size_t) nb * ps);

	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);

	for (uint32_t i = 0; i < nb; i++)
		fill_page(wbuf + (size_t) i * ps, ps, 1000 + i, (unsigned char) (i + 1));

	op_writev(REL_A, FORK0, 5, wbuf, nb);	/* one op, nb pages, at block 5 */
	check(op_nblocks(REL_A, FORK0) == 5 + nb, "nblocks after vectored write");

	op_readv(REL_A, FORK0, 5, rbuf, nb);	/* one op, read them all back */
	for (uint32_t i = 0; i < nb; i++)
		if (!page_has_tag(rbuf + (size_t) i * ps, ps, (unsigned char) (i + 1)))
			ok = 0;
	check(ok, "vectored read returns every page intact");

	/* and each block is individually addressable */
	op_read_one(REL_A, FORK0, 5 + nb / 2, rbuf);
	check(page_has_tag(rbuf, ps, (unsigned char) (nb / 2 + 1)),
		  "single-block read of a vector-written block");

	client_detach();
	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
	free(wbuf);
	free(rbuf);
}

/* One concurrent client: own channel, own relation, write then verify. */
static int
conc_child(const char *shm_name, uint32_t ps, int id)
{
	uint32_t	rel = 20000 + (uint32_t) id;
	unsigned char *p = malloc(ps);
	unsigned char *r = malloc(ps);
	int			rc = 0;

	client_attach(shm_name, ps);
	for (uint32_t b = 0; b < 50; b++)
	{
		fill_page(p, ps, 1000 + b, (unsigned char) (id * 7 + b + 1));
		op_write_one(rel, FORK0, b, p);
	}
	for (uint32_t b = 0; b < 50; b++)
	{
		op_read_one(rel, FORK0, b, r);
		if (!page_has_tag(r, ps, (unsigned char) (id * 7 + b + 1)))
			rc = 2;
	}
	client_detach();
	free(p);
	free(r);
	return rc;
}

/*
 * Concurrency: many clients, each claiming its own channel, hit the daemon at
 * once on independent relations.  Exercises channel allocation and the
 * multi-channel poll loop.
 */
static void
run_concurrency_suite(const char *daemon_path, const char *tmpbase)
{
#define NKIDS 8
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	pid_t		kids[NKIDS];
	uint32_t	ps = 8192;

	fprintf(stderr, "== concurrency (%d clients) ==\n", NKIDS);
	snprintf(shm, sizeof(shm), "/pstest_%d_conc", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_conc", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);		/* parent does not claim a channel */

	for (int i = 0; i < NKIDS; i++)
	{
		pid_t		pid = fork();

		if (pid == 0)
			_exit(conc_child(shm, ps, i));
		kids[i] = pid;
	}
	for (int i = 0; i < NKIDS; i++)
	{
		int			st = 1;

		waitpid(kids[i], &st, 0);
		check(WIFEXITED(st) && WEXITSTATUS(st) == 0,
			  "concurrent client %d completed correctly", i);
	}

	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
#undef NKIDS
}

/*
 * Unit-test the shard-routing contract (pure helpers in pagestore_ipc.h, no
 * daemon).  Clients and the daemon both rely on these for any nshards, so verify
 * them for nshards > 1 too even though the daemon currently runs nshards = 1.
 */
/* count files named obj_* in 'dir' (the object-tier uploads) */
static int
count_obj_files(const char *dir)
{
	DIR		   *d = opendir(dir);
	struct dirent *e;
	int			n = 0;

	if (!d)
		return -1;
	while ((e = readdir(d)) != NULL)
	{
		size_t		len = strlen(e->d_name);

		/* count only durably-renamed objects, not the "<obj>.tmp" staging file */
		if (strncmp(e->d_name, "obj_", 4) == 0 &&
			!(len >= 4 && strcmp(e->d_name + len - 4, ".tmp") == 0))
			n++;
	}
	closedir(d);
	return n;
}

/*
 * Object tier (LSM phase 4): with --object-dir set, the daemon uploads sealed
 * image layers off the write path, so after some flushed writes the object store
 * fills, and the data is still readable locally -- across a restart, where the
 * already-uploaded layers stay durable (no re-upload) and reads still serve.
 */
static void
run_object_tier_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	char		objdir[256];
	pid_t		dpid;
	uint32_t	ps = 8192;
	unsigned char pg[8192];
	unsigned char rb[8192];
	int			objs = 0;

	fprintf(stderr, "== object tier ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_obj", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_obj", tmpbase);
	snprintf(objdir, sizeof(objdir), "%s/objstore", tmpbase);
	rm_rf(store);
	rm_rf(objdir);
	shm_unlink(shm);
	if (mkdir(objdir, 0700) != 0)
	{
		check(0, "object tier: mkdir objdir");
		return;
	}

	g_object_dir = objdir;
	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);

	/* write enough pages (flush-pages=8) to seal a few image layers */
	op_create(REL_A, FORK0);
	for (uint32_t b = 0; b < 24; b++)
	{
		memset(pg, (unsigned char) (0x40 + b), ps);
		op_write_one(REL_A, FORK0, b, pg);
	}

	/* the daemon uploads when idle; poll the object dir (up to ~3s) */
	for (int t = 0; t < 300 && objs <= 0; t++)
	{
		struct timespec ts = {0, 10 * 1000 * 1000};	/* 10ms */

		nanosleep(&ts, NULL);
		objs = count_obj_files(objdir);
	}
	check(objs > 0, "object tier: layers uploaded to the object store (%d)", objs);

	/* data still served locally */
	memset(pg, 0x40 + 5, ps);
	op_read_one(REL_A, FORK0, 5, rb);
	check(memcmp(rb, pg, ps) == 0, "object tier: read serves correct bytes");

	/* clean restart: uploaded layers stay durable, reads still work.  Recreate the
	 * shm (as the other restart tests do) so wait_ready cannot return against the
	 * previous daemon's stale magic before the new one re-initializes it. */
	stop_daemon(dpid);
	client_detach();
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_read_one(REL_A, FORK0, 5, rb);
	check(memcmp(rb, pg, ps) == 0, "object tier: read correct after restart");
	op_read_one(REL_A, FORK0, 23, rb);
	memset(pg, (unsigned char) (0x40 + 23), ps);
	check(memcmp(rb, pg, ps) == 0, "object tier: last block correct after restart");

	stop_daemon(dpid);
	client_detach();
	rm_rf(store);
	rm_rf(objdir);
	shm_unlink(shm);
	g_object_dir = NULL;
}

static void
run_sharding_contract(void)
{
	/* include large, non-divisible shard counts up to PS_MAX_CHANNELS */
	uint32_t	cases[] = {1, 2, 3, 5, 7, 8, 33, 96, PS_MAX_CHANNELS};

	for (uint32_t ci = 0; ci < sizeof(cases) / sizeof(cases[0]); ci++)
	{
		uint32_t	ns = cases[ci];
		uint32_t	prev_end = 0,
					covered = 0,
					mincnt = PS_MAX_CHANNELS,
					maxcnt = 0;
		int			tiled = 1;

		/* the channel pools tile [0, PS_MAX_CHANNELS): contiguous, no gap/overlap,
		 * no empty pool, and balanced to within one channel (no last-pool dumping) */
		for (uint32_t s = 0; s < ns; s++)
		{
			uint32_t	first,
						cnt;

			ps_shard_channel_range(s, ns, PS_MAX_CHANNELS, &first, &cnt);
			if (first != prev_end || cnt == 0)
				tiled = 0;
			if (cnt < mincnt)
				mincnt = cnt;
			if (cnt > maxcnt)
				maxcnt = cnt;
			prev_end = first + cnt;
			covered += cnt;
		}
		check(tiled && prev_end == PS_MAX_CHANNELS && covered == PS_MAX_CHANNELS,
			  "shard channel pools tile [0,nchannels) for nshards=%u", ns);
		check(maxcnt - mincnt <= 1,
			  "shard channel pools balanced within 1 for nshards=%u", ns);

		/* key -> shard is in range, deterministic, and (for ns>1) spreads */
		{
			int			inrange = 1,
						stable = 1;
			uint32_t	hit0 = 0,
						hitN = 0;

			for (uint32_t rel = 0; rel < 256; rel++)
			{
				PsKey		k = {1, 1, rel, 0};
				uint32_t	a = ps_shard_for_key(&k, ns);

				if (a >= ns)
					inrange = 0;
				if (a != ps_shard_for_key(&k, ns))
					stable = 0;
				if (a == 0)
					hit0++;
				else
					hitN++;
			}
			check(inrange && stable,
				  "ps_shard_for_key in [0,%u) and deterministic", ns);
			if (ns > 1)
				check(hit0 > 0 && hitN > 0,
					  "ps_shard_for_key spreads keys across shards (nshards=%u)",
					  ns);
		}
	}
}

int
main(int argc, char **argv)
{
	const char *daemon_path;
	char		tmpl[] = "/tmp/pstestXXXXXX";
	char	   *tmpbase;
	uint32_t	sizes[] = {4096, 8192, 16384};

	if (argc < 2)
	{
		fprintf(stderr, "usage: %s <path-to-pagestore_daemon>\n", argv[0]);
		return 2;
	}
	daemon_path = argv[1];

	tmpbase = mkdtemp(tmpl);
	if (!tmpbase)
	{
		perror("mkdtemp");
		return 2;
	}

	/* shard-routing contract (pure helpers; no daemon) */
	run_sharding_contract();

	/*
	 * Run the whole daemon-driven battery once single-shard and once multi-shard.
	 * At nshards > 1 the client routes each key to its shard's channel pool and
	 * the (still single-threaded) daemon serves every pool; identical results
	 * prove the data partitions correctly across shards end-to-end.
	 */
	uint32_t	shard_cases[] = {1, 4};

	for (uint32_t si = 0; si < sizeof(shard_cases) / sizeof(shard_cases[0]); si++)
	{
		g_nshards = shard_cases[si];
		fprintf(stderr, "\n#### nshards=%u ####\n", g_nshards);

		/* run the whole suite once per page size: proves page-size independence */
		for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++)
			run_suite(daemon_path, tmpbase, sizes[i]);

		/* branch / snapshot isolation (page-size independent, run once) */
		run_branch_suite(daemon_path, tmpbase);

		/* shipped-WAL durability */
		run_wal_suite(daemon_path, tmpbase);

		/* per-page WAL index */
		run_walidx_suite(daemon_path, tmpbase);

		/* vectored multi-page I/O */
		run_vectored_suite(daemon_path, tmpbase);

		/* many concurrent clients */
		run_concurrency_suite(daemon_path, tmpbase);
	}

	/* object tier (single shard is enough to exercise upload + restart) */
	g_nshards = 1;
	run_object_tier_suite(daemon_path, tmpbase);

	rmdir(tmpbase);

	fprintf(stderr, "\n%d checks, %d failed\n", tests_run, tests_failed);
	return tests_failed ? 1 : 0;
}
