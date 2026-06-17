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
static int	cl_chan;
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
	check(hdr->magic == PS_SHM_MAGIC, "shm magic");
	check(hdr->page_size == expect_page_size,
		  "header page_size=%u expected %u", hdr->page_size, expect_page_size);
	cl_page_size = hdr->page_size;

	cl_chan = -1;
	for (uint32_t i = 0; i < hdr->nchannels; i++)
	{
		PsChannel  *ch = ps_channel(cl_shm, i);

		if (ps_cas(&ch->claimed, 0, 1))
		{
			cl_chan = (int) i;
			break;
		}
	}
	if (cl_chan < 0)
	{
		fprintf(stderr, "no free channel\n");
		exit(2);
	}
}

static void
client_detach(void)
{
	if (cl_shm)
	{
		if (cl_chan >= 0)
			ps_store_release(&ps_channel(cl_shm, cl_chan)->claimed, 0);
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
cl_exec(void)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;					/* busy wait; single in-flight request */
	return ch;
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
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_CREATE;
	cl_exec();
}

static int
op_exists(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_EXISTS;
	return cl_exec()->result != 0;
}

static void
op_unlink(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_UNLINK;
	cl_exec();
}

static uint32_t
op_nblocks(uint32_t rel, int32_t fork)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_NBLOCKS;
	return cl_exec()->result;
}

static void
op_truncate(uint32_t rel, int32_t fork, uint32_t nblocks)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_TRUNCATE;
	ch->nblocks = nblocks;
	cl_exec();
}

static void
op_zeroextend(uint32_t rel, int32_t fork, uint32_t block, uint32_t nblocks)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = block;
	ch->nblocks = nblocks;
	cl_exec();
}

static void
op_write_one(uint32_t rel, int32_t fork, uint32_t block, const unsigned char *page)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, cl_page_size);
	cl_exec();
}

static void
op_read_one(uint32_t rel, int32_t fork, uint32_t block, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = 1;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

static void
op_read_at(uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn,
		   unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

/* --- timeline-aware operations (for branch tests) --- */

/* Create timeline new_tl as a branch of parent_tl forked at branch_lsn. */
static void
op_create_branch(uint32_t new_tl, uint32_t parent_tl, uint64_t branch_lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	cl_exec();
}

/* Like op_create_branch but returns the daemon's status (for negative tests). */
static int
op_create_branch_status(uint32_t new_tl, uint32_t parent_tl, uint64_t branch_lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	return cl_exec()->status;
}

/* Write one page on a specific timeline. */
static void
op_write_tl(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
			const unsigned char *page)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, cl_page_size);
	cl_exec();
}

/* Read one page (current) on a specific timeline. */
static void
op_read_tl(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
		   unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = 1;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

/* Read one page as-of an LSN on a specific timeline. */
static void
op_read_at_tl(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
			  uint64_t lsn, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
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

		snprintf(psbuf, sizeof(psbuf), "%u", page_size);
		/* small segments so the tests exercise segment rollover */
		execl(daemon_path, daemon_path, "--shm", shm, "--store", store,
			  "--page-size", psbuf, "--segment-size", "65536", (char *) NULL);
		perror("execl daemon");
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
				int			ready = (h->magic == PS_SHM_MAGIC &&
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

	client_detach();

	/* --- crash recovery: restart daemon, rebuild index from segments --- */
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);

	check(op_exists(REL_A, FORK0), "fork still exists after daemon restart");
	op_read_one(REL_A, FORK0, 5, rb);
	check(page_has_tag(rb, page_size, 6), "block 5 survives restart (recovered)");
	op_read_at(REL_A, FORK0, 0, 7000, rb);
	check(page_has_tag(rb, page_size, 100),
		  "COW history survives restart (read_at old version)");

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

	client_detach();
	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
	free(p);
	free(rb);
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

	/* run the whole suite once per page size: proves page-size independence */
	for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++)
		run_suite(daemon_path, tmpbase, sizes[i]);

	/* branch / snapshot isolation (page-size independent, run once) */
	run_branch_suite(daemon_path, tmpbase);

	rmdir(tmpbase);

	fprintf(stderr, "\n%d checks, %d failed\n", tests_run, tests_failed);
	return tests_failed ? 1 : 0;
}
