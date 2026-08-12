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
 * Usage: pagestore_test <path-to-pagestore_daemon> <path-to-pagestore_inspect>
 * Exit status: 0 = all tests passed, 1 = one or more failed.
 *
 * src/../contrib/pagestore/pagestore_test.c
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <glob.h>
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
static uint32_t test_nshards = 1;
static const char *inspect_path;

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

/* The inspector must observe a live daemon without claiming an I/O channel. */
static int
run_inspector(const char *shm, const char *operation, char *output, size_t output_size)
{
	int		pipefd[2];
	pid_t		pid;
	int		status;
	ssize_t		nread;

	if (pipe(pipefd) != 0)
	{
		perror("pipe inspector");
		exit(2);
	}
	pid = fork();
	if (pid < 0)
	{
		perror("fork inspector");
		exit(2);
	}
	if (pid == 0)
	{
		close(pipefd[0]);
		dup2(pipefd[1], STDOUT_FILENO);
		close(pipefd[1]);
		execl(inspect_path, inspect_path, "--shm", shm, operation, (char *) NULL);
		_exit(127);
	}
	close(pipefd[1]);
	nread = read(pipefd[0], output, output_size - 1);
	close(pipefd[0]);
	if (nread < 0)
	{
		perror("read inspector");
		exit(2);
	}
	output[nread] = '\0';
	waitpid(pid, &status, 0);
	return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static void
check_inspector(const char *shm, uint32_t page_size)
{
	char		output[1024];
	char		page_size_json[64];

	check(run_inspector(shm, "health", output, sizeof(output)),
		  "read-only inspector health exits cleanly");
	snprintf(page_size_json, sizeof(page_size_json), "\"page_size\":%u", page_size);
	check(strstr(output, "\"protocol_version\":") != NULL &&
		  strstr(output, page_size_json) != NULL &&
		  strstr(output, "\"nchannels\":128") != NULL,
		  "read-only inspector reports the live shared-memory health");
	check(run_inspector(shm, "backpressure", output, sizeof(output)),
		  "read-only inspector backpressure exits cleanly");
	check(strstr(output, "\"idle\":128") != NULL &&
		  strstr(output, "\"claimed\":0") != NULL &&
		  strstr(output, "\"request\":0") != NULL &&
		  strstr(output, "\"done\":0") != NULL &&
		  strstr(output, "\"wal_index_pending_bytes\":0") != NULL &&
		  strstr(output, "\"wal_index_lagging_timelines\":0") != NULL,
		  "read-only inspector reports idle mailbox backpressure state");
	check(run_inspector(shm, "pruning", output, sizeof(output)),
		  "read-only inspector pruning exits cleanly");
	check(strstr(output, "\"compactions\":") != NULL &&
		  strstr(output, "\"versions_scanned\":") != NULL &&
		  strstr(output, "\"versions_kept\":") != NULL &&
		  strstr(output, "\"versions_deleted\":") != NULL,
		  "read-only inspector reports page-pruning counters");
}

/* An offline segment-format migration must invalidate derived LSM metadata. */
static void
remove_lsm_metadata(const char *store)
{
	char		cmd[768];

	snprintf(cmd, sizeof(cmd), "rm -f '%s'/layer_* '%s'/layers.manifest",
			 store, store);
	check(system(cmd) == 0, "removed derived LSM metadata for offline migration");
}

/* ===================== client side of the IPC protocol ================= */

static void *cl_shm;
static int	cl_shm_fd;
static int	cl_chan;
static uint32_t cl_page_size;
static uint32_t cl_nshards = 1;

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
	cl_nshards = hdr->nshards ? hdr->nshards : 1;
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

static PsChannel *
exec_channel(PsChannel *ch)
{
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;
	return ch;
}

static void
cl_setkey(PsChannel *ch, uint32_t rel, int32_t fork)
{
	ch->key.spcOid = 1;
	ch->key.dbOid = 1;
	ch->key.relNumber = rel;
	ch->key.forkNum = fork;
	ch->key.klass = PS_KLASS_RELATION;
	ch->timeline = 0;			/* default to the main timeline */
	ch->req_lsn = 0;			/* explicit: channels are reused across op kinds */
	ch->req_seq = 0;
}

static uint32_t
find_relation_on_shard(uint32_t shard, uint32_t nshards)
{
	PsKey		key;

	memset(&key, 0, sizeof(key));
	key.spcOid = 1;
	key.dbOid = 1;
	key.forkNum = 0;
	key.klass = PS_KLASS_RELATION;
	for (uint32_t rel = 16001; rel < 116000; rel++)
	{
		key.relNumber = rel;
		if (ps_key_shard(&key, nshards) == shard)
			return rel;
	}
	return 0;
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

/* --- lsn-stamped fork mutations + as-of queries (fork-size history) --- */

static void
op_create_at(uint32_t rel, int32_t fork, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = lsn;
	cl_exec();
}

static void
op_truncate_at(uint32_t rel, int32_t fork, uint32_t nblocks, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_TRUNCATE;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
	cl_exec();
}

static void
op_unlink_at(uint32_t rel, int32_t fork, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_UNLINK;
	ch->req_lsn = lsn;
	cl_exec();
}

static void
op_zeroextend_at(uint32_t rel, int32_t fork, uint32_t block, uint32_t nblocks,
				 uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = block;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
	cl_exec();
}

static uint32_t
op_nblocks_asof(uint32_t rel, int32_t fork, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = lsn;
	return cl_exec()->result;
}

static uint32_t
op_nblocks_asof_seq(uint32_t rel, int32_t fork, uint64_t lsn, uint64_t seq)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	return cl_exec()->result;
}

static int
op_exists_asof(uint32_t rel, int32_t fork, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_EXISTS;
	ch->req_lsn = lsn;
	return cl_exec()->result != 0;
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

static uint64_t
op_write_one_seq(uint32_t rel, int32_t fork, uint32_t block,
				 const unsigned char *page)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, cl_page_size);
	cl_exec();
	return ch->req_seq;
}

static void
op_read_one(uint32_t rel, int32_t fork, uint32_t block, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READV;
	ch->req_lsn = 0;	/* explicit: channels are reused across op kinds */
	ch->blocknum = block;
	ch->nblocks = 1;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

/* Vectored write of n contiguous pages in a single op. */
static void
op_writev(uint32_t rel, int32_t fork, uint32_t block, const unsigned char *pages,
		  uint32_t n)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = n;
	memcpy(ch->data, pages, (size_t) n * cl_page_size);
	cl_exec();
}

/* Vectored read of n contiguous pages in a single op. */
static void
op_readv(uint32_t rel, int32_t fork, uint32_t block, unsigned char *out,
		 uint32_t n)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READV;
	ch->req_lsn = 0;	/* explicit: channels are reused across op kinds */
	ch->blocknum = block;
	ch->nblocks = n;
	cl_exec();
	memcpy(out, ch->data, (size_t) n * cl_page_size);
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

static void
op_read_at_seq(uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn,
			   uint64_t seq, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

/* Like op_read_at_seq but reports found-ness (ch->result). */
static int
op_read_at_seq_found(uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn,
					 uint64_t seq, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	if (cl_exec()->result == 0)
		return 0;
	memcpy(out, ch->data, cl_page_size);
	return 1;
}

/* Like op_read_at but reports found-ness (ch->result). */
static int
op_read_at_found(uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn,
				 unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	if (cl_exec()->result == 0)
		return 0;
	memcpy(out, ch->data, cl_page_size);
	return 1;
}

static int
op_read_at_tl_found(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
					uint64_t lsn, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	if (cl_exec()->result == 0)
		return 0;
	memcpy(out, ch->data, cl_page_size);
	return 1;
}

/*
 * SLRU-class write: the version is the caller-supplied LSN (req_lsn), NOT pd_lsn or
 * a daemon counter -- so a snapshot keyed by its proven cutoff C reads back as-of an
 * LSN >= C.  'obj' is the SLRU object id (slru_klass_id); 'block' is the segment.
 */
static void
op_write_slru(uint32_t obj, uint32_t block, const unsigned char *page,
			  uint64_t version)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, obj, 0);			/* fork 0 */
	ch->key.klass = PS_KLASS_SLRU;
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->req_lsn = version;
	memcpy(ch->data, page, cl_page_size);
	cl_exec();
}

static uint32_t
op_nblocks_slru(uint32_t obj)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, obj, 0);
	ch->key.klass = PS_KLASS_SLRU;
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = 0;
	return cl_exec()->result;
}

static void
op_read_slru(uint32_t obj, uint32_t block, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, obj, 0);
	ch->key.klass = PS_KLASS_SLRU;
	ch->opcode = PS_OP_READV;
	ch->req_lsn = 0;
	ch->blocknum = block;
	ch->nblocks = 1;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

/* SLRU-class as-of read (READ_AT zero-fills on no-version-<=lsn). */
static void
op_read_at_slru(uint32_t obj, uint32_t block, uint64_t lsn, unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, obj, 0);			/* fork 0 */
	ch->key.klass = PS_KLASS_SLRU;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
}

static int
op_read_at_slru_status(uint32_t obj, uint32_t block, uint64_t lsn,
						unsigned char *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, obj, 0);
	ch->key.klass = PS_KLASS_SLRU;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec();
	memcpy(out, ch->data, cl_page_size);
	return ch->status;
}

/*
 * Control-class write at 'block' (0 = the pg_control image, 1 = the retention
 * floor note) versioned by the caller-supplied update LSN, mirroring
 * contrib/pagestore's control shipper.
 */
static void
op_write_control(uint32_t block, const unsigned char *page, uint64_t version)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);
	uint32_t	nb;

	/* same shape as the backend's obj_write: create, size, extend-or-overwrite.
	 * The control key is ALL-zero (spc/db/rel/fork), unlike cl_setkey's 1/1. */
	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->timeline = 0;
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = 1;
	cl_exec();

	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->timeline = 0;
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_NBLOCKS;
	cl_exec();
	nb = ch->result;

	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->timeline = 0;
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = (block < nb) ? PS_OP_WRITEV : PS_OP_EXTEND;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->req_lsn = version;
	memcpy(ch->data, page, cl_page_size);
	cl_exec();
}

/* Durable WAL retention floor for a timeline (0 = unconstrained). */
static uint64_t
op_wal_retain_floor(uint32_t timeline)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_WAL_RETAIN_FLOOR;
	ch->timeline = timeline;
	ch->blocknum = 0;
	ch->req_lsn = 0;
	cl_exec();
	return ch->req_lsn;
}

static int
op_retention_set_fenced(uint32_t timeline, uint32_t owner_kind,
						uint64_t owner_id, uint32_t generation,
						uint32_t resources, uint64_t lsn,
						uint64_t admission_seq)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_RETENTION_PIN_SET;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->parent_timeline = resources;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	ch->nblocks = (uint32_t) admission_seq;
	ch->pad1 = (uint32_t) (admission_seq >> 32);
	return cl_exec()->status;
}

static int
op_retention_set_seq(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id,
					 uint32_t generation, uint32_t resources, uint64_t lsn,
					 uint64_t admission_seq)
{
	return op_retention_set_fenced(timeline, owner_kind, owner_id, generation,
								resources, lsn, admission_seq);
}

static int
op_admission_barrier(uint64_t *admission_seq_out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_ADMISSION_BARRIER;
	cl_exec();
	if (admission_seq_out != NULL)
		*admission_seq_out = ch->req_seq;
	return ch->req_seq == 0 ? PS_STATUS_ERROR : ch->status;
}

static int
op_retention_set(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id,
				 uint32_t generation, uint32_t resources, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->parent_timeline = resources;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	return cl_exec()->status;
}

static int
op_retention_drop(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id,
				  uint32_t generation)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_RETENTION_PIN_DROP;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	return cl_exec()->status;
}

static int
op_retention_get_consistent(uint32_t index, PsRetentionPin *pin,
							uint32_t *count, uint64_t *epoch, int *found)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);
	PsRetentionGetResult result;

	ch->opcode = PS_OP_RETENTION_PIN_GET;
	ch->blocknum = index;
	ch->req_lsn = *epoch;
	cl_exec();
	*found = 0;
	if (count)
		*count = ch->nblocks;
	if (ch->status != PS_STATUS_OK)
	{
		if (ch->status == PS_STATUS_STALE)
			*epoch = ch->req_lsn;
		return ch->status;
	}
	if (ch->datalen != sizeof(result))
		return PS_STATUS_ERROR;
	memcpy(&result, ch->data, sizeof(result));
	*epoch = result.mutation_epoch;
	if (ch->result == 0)
		return PS_STATUS_OK;
	*found = 1;
	if (pin)
	{
		memset(pin, 0, sizeof(*pin));
		pin->timeline = ch->timeline;
		pin->owner_kind = ch->blocknum;
		pin->resources = ch->parent_timeline;
		pin->generation = ch->old_nblocks;
		pin->owner_id = ch->req_seq;
		pin->lsn = ch->req_lsn;
		pin->admission_seq = result.admission_seq;
	}
	return PS_STATUS_OK;
}

static int
op_retention_get(uint32_t index, PsRetentionPin *pin, uint32_t *count)
{
	uint64_t	epoch = 0;
	int			found = 0;

	return op_retention_get_consistent(index, pin, count, &epoch, &found) ==
		PS_STATUS_OK && found;
}

static int
op_retention_lookup(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id,
					PsRetentionPin *pin)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_RETENTION_PIN_LOOKUP;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->req_seq = owner_id;
	cl_exec();
	if (ch->status != PS_STATUS_OK || ch->result == 0)
		return 0;
	if (pin)
	{
		memset(pin, 0, sizeof(*pin));
		pin->timeline = ch->timeline;
		pin->owner_kind = ch->blocknum;
		pin->resources = ch->parent_timeline;
		pin->generation = ch->old_nblocks;
		pin->owner_id = ch->req_seq;
		pin->lsn = ch->req_lsn;
		if (ch->datalen != sizeof(pin->admission_seq))
			return 0;
		memcpy(&pin->admission_seq, ch->data, sizeof(pin->admission_seq));
	}
	return 1;
}

static int
op_retention_floor(uint32_t timeline, uint32_t resource, uint64_t *floor)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_RETENTION_FLOOR;
	ch->timeline = timeline;
	ch->parent_timeline = resource;
	ch->req_lsn = 0;
	cl_exec();
	if (floor)
		*floor = ch->req_lsn;
	return ch->status;
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

/* Like op_create_branch but only validates the request (no metadata mutation). */
static int
op_check_branch_status(uint32_t new_tl, uint32_t parent_tl, uint64_t branch_lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_CHECK_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	return cl_exec()->status;
}

static int
op_require_branch_status(uint32_t new_tl, uint32_t parent_tl,
						 uint64_t branch_lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->opcode = PS_OP_REQUIRE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	return cl_exec()->status;
}

/* Write one page on a specific timeline. */
static uint32_t
op_nblocks_tl(uint32_t tl, uint32_t rel, int32_t fork)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_NBLOCKS;
	return cl_exec()->result;
}

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

static int
op_write_tl_status(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
				   const unsigned char *page)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, cl_page_size);
	return cl_exec()->status;
}

static uint32_t
op_nblocks_asof_tl(uint32_t tl, uint32_t rel, int32_t fork, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = lsn;
	return cl_exec()->result;
}

static void
op_truncate_at_tl(uint32_t tl, uint32_t rel, int32_t fork,
				  uint32_t nblocks, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_TRUNCATE;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
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
	ch->req_lsn = 0;	/* explicit: channels are reused across op kinds */
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

/* --- shipped-WAL operations --- */

#define TEST_WAL_MAGIC	0x57414c52

static void
write_torn_wal_header(const char *store, uint32_t tl, uint64_t start_lsn,
					  uint32_t len)
{
	struct
	{
		uint32_t	magic;
		uint32_t	len;
		uint64_t	start_lsn;
	}			h;
	char		path[512];
	int			fd;

	snprintf(path, sizeof(path), "%s/wal_%u", store, tl);
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	check(fd >= 0, "test creates a torn WAL log");
	if (fd < 0)
		return;
	h.magic = TEST_WAL_MAGIC;
	h.len = len;
	h.start_lsn = start_lsn;
	check(write(fd, &h, sizeof(h)) == (ssize_t) sizeof(h) && fsync(fd) == 0,
		  "test writes a WAL header without its payload");
	check(close(fd) == 0, "test closes the torn WAL log");
}

static void
write_short_wal_payload(const char *store, uint32_t tl, uint64_t start_lsn,
						const void *data, uint32_t len, uint32_t written)
{
	struct
	{
		uint32_t	magic;
		uint32_t	len;
		uint64_t	start_lsn;
	}			h;
	char		path[512];
	int			fd;

	snprintf(path, sizeof(path), "%s/wal_%u", store, tl);
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	check(fd >= 0, "test creates a short WAL log");
	if (fd < 0)
		return;
	h.magic = TEST_WAL_MAGIC;
	h.len = len;
	h.start_lsn = start_lsn;
	check(write(fd, &h, sizeof(h)) == (ssize_t) sizeof(h) &&
		  write(fd, data, written) == (ssize_t) written && fsync(fd) == 0,
		  "test writes a WAL record with a short payload");
	check(close(fd) == 0, "test closes the short WAL log");
}

static off_t
append_torn_timeline_tail(const char *store)
{
	static const unsigned char tail[] = {0x54, 0x4c, 0x4d};
	char		path[512];
	struct stat st;
	int			fd;
	off_t		committed_size = -1;

	snprintf(path, sizeof(path), "%s/timelines", store);
	fd = open(path, O_WRONLY | O_APPEND);
	check(fd >= 0, "test opens timeline metadata for torn-tail injection");
	if (fd < 0)
		return -1;
	if (fstat(fd, &st) == 0)
		committed_size = st.st_size;
	check(committed_size >= 0 &&
		  write(fd, tail, sizeof(tail)) == (ssize_t) sizeof(tail) &&
		  fsync(fd) == 0,
		  "test appends an incomplete timeline metadata record");
	check(close(fd) == 0, "test closes torn timeline metadata");
	return committed_size;
}

/* Append len WAL bytes at start_lsn on a timeline. */
static void
op_wal_append(uint32_t tl, uint64_t start_lsn, const void *data, uint32_t len)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	memcpy(ch->data, data, len);
	cl_exec();
}

/* Like op_wal_append but reports the daemon's status (for negative tests). */
static int
op_wal_append_status(uint32_t tl, uint64_t start_lsn, const void *data,
					 uint32_t len)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	memcpy(ch->data, data, len);
	return cl_exec()->status;
}

/* Return the end LSN of a timeline's shipped WAL. */
static uint64_t
op_wal_size(uint32_t tl)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_SIZE;
	cl_exec();
	return ch->req_lsn;
}

/* Read a timeline's ancestry metadata; return whether it has a parent. */
static int
op_timeline_info(uint32_t tl, uint32_t *parent_tl, uint64_t *branch_lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->timeline = tl;
	ch->opcode = PS_OP_TIMELINE_INFO;
	cl_exec();
	*parent_tl = ch->parent_timeline;
	*branch_lsn = ch->req_lsn;
	return ch->result != 0;
}

/* Read len WAL bytes from start_lsn into out; returns bytes filled. */
static uint32_t
op_wal_read(uint32_t tl, uint64_t start_lsn, uint32_t len, void *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	cl_exec();
	memcpy(out, ch->data, len);
	return ch->result;
}

/* --- per-page WAL index operations --- */

static void
op_walidx_add(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_INDEX_ADD;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	cl_exec();
}

static void
op_walidx_add_batch(uint32_t tl, uint32_t rel, int32_t fork,
					const uint32_t *blocks, const uint64_t *lsns, uint32_t nentries)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);
	PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;

	cl_setkey(ch, rel, fork);
	for (uint32_t i = 0; i < nentries; i++)
	{
		entries[i].key = ch->key;
		entries[i].block = blocks[i];
		entries[i].pad = 0;
		entries[i].lsn = lsns[i];
	}
	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
	ch->nblocks = nentries;
	ch->datalen = nentries * sizeof(*entries);
	cl_exec();
}

/* Returns count; fills out[] with the record LSNs <= lsn_max. */
static int
op_walidx_get_after(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
					uint64_t lsn_max, int have_cursor, uint64_t cursor_lsn,
					uint32_t cursor_timeline, uint32_t max_out, PsWalRec *out)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);
	int			n;

	cl_setkey(ch, rel, fork);
	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_INDEX_GET;
	ch->blocknum = block;
	ch->nblocks = max_out;
	ch->req_lsn = lsn_max;
	ch->pad1 = have_cursor;
	ch->req_seq = cursor_lsn;
	ch->parent_timeline = cursor_timeline;
	n = (int) cl_exec()->result;
	memcpy(out, ch->data, (size_t) n * sizeof(PsWalRec));
	return n;
}

static int
op_walidx_get(uint32_t tl, uint32_t rel, int32_t fork, uint32_t block,
			  uint64_t lsn_max, PsWalRec *out)
{
	return op_walidx_get_after(tl, rel, fork, block, lsn_max, 0, 0, 0, 0, out);
}

static int
op_walidx_progress(uint32_t tl, uint64_t start, uint64_t end, uint64_t *progress)
{
	PsChannel  *ch = ps_channel(cl_shm, cl_chan);
	int			ok;

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = start;
	ch->req_seq = end;
	ok = cl_exec()->status == PS_STATUS_OK;
	if (ok && progress)
		*progress = ch->req_lsn;
	return ok ? 0 : -1;
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
spawn_daemon_fault(const char *daemon_path, const char *shm, const char *store,
				   uint32_t page_size, uint32_t nshards, int fail_seg_writes,
				   int crash_after_seg_writes, int fail_fork_meta_append_at,
				   int segment_gc)
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
		char		shbuf[16];

		unsetenv("PAGESTORE_TEST_FAIL_SEG_WRITES");
		unsetenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES");
		unsetenv("PAGESTORE_TEST_FAIL_FORK_META_APPEND_AT");
		if (fail_seg_writes > 0)
		{
			char		failbuf[16];

			snprintf(failbuf, sizeof(failbuf), "%d", fail_seg_writes);
			if (setenv("PAGESTORE_TEST_FAIL_SEG_WRITES", failbuf, 1) != 0)
			{
				perror("setenv PAGESTORE_TEST_FAIL_SEG_WRITES");
				_exit(127);
			}
		}
		if (crash_after_seg_writes > 0)
		{
			char		crashbuf[16];

			snprintf(crashbuf, sizeof(crashbuf), "%d", crash_after_seg_writes);
			if (setenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES", crashbuf, 1) != 0)
			{
				perror("setenv PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES");
				_exit(127);
			}
		}
		if (fail_fork_meta_append_at > 0)
		{
			char		failbuf[16];

			snprintf(failbuf, sizeof(failbuf), "%d", fail_fork_meta_append_at);
			if (setenv("PAGESTORE_TEST_FAIL_FORK_META_APPEND_AT", failbuf, 1) != 0)
			{
				perror("setenv PAGESTORE_TEST_FAIL_FORK_META_APPEND_AT");
				_exit(127);
			}
		}

		snprintf(psbuf, sizeof(psbuf), "%u", page_size);
		snprintf(shbuf, sizeof(shbuf), "%u", nshards);
		/* small segments exercise rollover; a small flush threshold makes the
		 * tests flush into image layers so the layer read path is exercised */
		execl(daemon_path, daemon_path, "--shm", shm, "--store", store,
			  "--page-size", psbuf, "--segment-size", "65536", "--nshards",
			  shbuf, "--flush-pages", "8", "--compact-layers", "3",
			  "--segment-gc", segment_gc ? "1" : "0", (char *) NULL);
		perror("execl daemon");
		_exit(127);
	}
	return pid;
}

static pid_t
spawn_daemon_fail_seg(const char *daemon_path, const char *shm, const char *store,
					 uint32_t page_size, uint32_t nshards, int fail_seg_writes)
{
	return spawn_daemon_fault(daemon_path, shm, store, page_size, nshards,
							  fail_seg_writes, 0, 0, 0);
}

static pid_t
spawn_daemon_crash_after_seg(const char *daemon_path, const char *shm,
							 const char *store, uint32_t page_size,
							 uint32_t nshards, int crash_after_seg_writes)
{
	return spawn_daemon_fault(daemon_path, shm, store, page_size, nshards, 0,
							  crash_after_seg_writes, 0, 0);
}

static pid_t
spawn_daemon_fail_fork_meta(const char *daemon_path, const char *shm,
							const char *store, uint32_t page_size,
							uint32_t nshards, int fail_append_at)
{
	return spawn_daemon_fault(daemon_path, shm, store, page_size, nshards, 0, 0,
							  fail_append_at, 0);
}

static pid_t
spawn_daemon(const char *daemon_path, const char *shm, const char *store,
			 uint32_t page_size, uint32_t nshards)
{
	return spawn_daemon_fault(daemon_path, shm, store, page_size, nshards,
							  0, 0, 0, 0);
}

static pid_t
spawn_daemon_gc(const char *daemon_path, const char *shm, const char *store,
				uint32_t page_size, uint32_t nshards)
{
	return spawn_daemon_fault(daemon_path, shm, store, page_size, nshards,
							  0, 0, 0, 1);
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
expect_daemon_open_failure(pid_t pid, const char *shm, const char *message)
{
	int			status = 0;
	int			exited = 0;

	for (int i = 0; i < 500; i++)
	{
		pid_t		r = waitpid(pid, &status, WNOHANG);

		if (r == pid)
		{
			exited = 1;
			break;
		}
		usleep(10000);
	}
	if (!exited)
	{
		kill(pid, SIGKILL);
		waitpid(pid, &status, 0);
	}
	check(exited && WIFEXITED(status) && WEXITSTATUS(status) != 0,
		  "%s", message);
	{
		int			fd = shm_open(shm, O_RDWR, 0600);

		check(fd < 0, "%s does not publish shared-memory readiness", message);
		if (fd >= 0)
			close(fd);
	}
}

static void
stop_daemon(pid_t pid)
{
	kill(pid, SIGTERM);
	waitpid(pid, NULL, 0);
}

/* On-disk fork-meta record mirror, used only to synthesize a pre-marker store. */
typedef struct TestForkMetaRec
{
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} TestForkMetaRec;

#define TEST_FORK_META_V2_MAGIC 0x324d4b46
typedef struct TestForkMetaRecV2
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint64_t	order_id;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} TestForkMetaRecV2;

#define TEST_FEV_SEG_GROW_BOUND		7
#define TEST_FEV_SEG_COMMIT_BOUND	8
#define TEST_FEV_SEG_ID				9
#define TEST_SEG_WALLESS_MAGIC		0x53454730
#define TEST_SEG_WALLESS_BOUND_MAGIC 0x53454734
#define TEST_SEG_WALLESS_ADMISSION_MAGIC 0x53454737

typedef struct TestSegRecHdr
{
	uint32_t	magic;
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	uint32_t	len;
} TestSegRecHdr;

static int
strip_forkmeta_markers(const char *store, int strip_start, int strip_done)
{
	char		path[512];
	TestForkMetaRec rec;
	TestForkMetaRecV2 rec2;
	off_t		in = 0;
	off_t		out = 0;
	int			fd;

	snprintf(path, sizeof(path), "%s/forkmeta", store);
	fd = open(path, O_RDWR);
	if (fd < 0)
		return -1;
	for (;;)
	{
		uint32_t	first;
		void	   *buf;
		size_t		sz;
		uint8_t		kind;

		if (pread(fd, &first, sizeof(first), in) != (ssize_t) sizeof(first))
			break;
		if (first == TEST_FORK_META_V2_MAGIC)
		{
			if (pread(fd, &rec2, sizeof(rec2), in) != (ssize_t) sizeof(rec2))
				break;
			buf = &rec2;
			sz = sizeof(rec2);
			kind = rec2.kind;
		}
		else
		{
			if (pread(fd, &rec, sizeof(rec), in) != (ssize_t) sizeof(rec))
				break;
			buf = &rec;
			sz = sizeof(rec);
			kind = rec.kind;
		}
		in += (off_t) sz;
		if ((strip_done && kind == 3) ||	/* FEV_MIGRATED */
			(strip_start && kind == 4))	/* FEV_MIGRATING */
			continue;
		if (pwrite(fd, buf, sz, out) != (ssize_t) sz)
		{
			close(fd);
			return -1;
		}
		out += (off_t) sz;
	}
	if (ftruncate(fd, out) != 0 || fsync(fd) != 0 || close(fd) != 0)
		return -1;
	return 0;
}

static int
strip_bound_forkmeta_markers(const char *store)
{
	char		path[512];
	TestForkMetaRec rec;
	TestForkMetaRecV2 rec2;
	off_t		in = 0;
	off_t		out = 0;
	int			fd;

	snprintf(path, sizeof(path), "%s/forkmeta", store);
	fd = open(path, O_RDWR);
	if (fd < 0)
		return -1;
	for (;;)
	{
		uint32_t	first;
		void	   *buf;
		size_t		sz;
		uint8_t		kind;

		if (pread(fd, &first, sizeof(first), in) != (ssize_t) sizeof(first))
			break;
		if (first == TEST_FORK_META_V2_MAGIC)
		{
			if (pread(fd, &rec2, sizeof(rec2), in) != (ssize_t) sizeof(rec2))
				break;
			buf = &rec2;
			sz = sizeof(rec2);
			kind = rec2.kind;
		}
		else
		{
			if (pread(fd, &rec, sizeof(rec), in) != (ssize_t) sizeof(rec))
				break;
			buf = &rec;
			sz = sizeof(rec);
			kind = rec.kind;
		}
		in += (off_t) sz;
		if (kind == TEST_FEV_SEG_GROW_BOUND ||
			kind == TEST_FEV_SEG_COMMIT_BOUND || kind == TEST_FEV_SEG_ID)
			continue;
		if (pwrite(fd, buf, sz, out) != (ssize_t) sz)
		{
			close(fd);
			return -1;
		}
		out += (off_t) sz;
	}
	if (ftruncate(fd, out) != 0 || fsync(fd) != 0 || close(fd) != 0)
		return -1;
	return 0;
}

static int
downgrade_bound_record_to_seg0(const char *store, uint32_t rel,
								 uint32_t page_size)
{
	char		path[512];
	TestSegRecHdr hdr;
	PsKey		key = {1, 1, rel, 0, PS_KLASS_RELATION};
	unsigned char *page = malloc(page_size);
	uint32_t	shard = ps_key_shard(&key, test_nshards);
	int			fd = -1;
	int			rc = -1;

	if (shard == 0)
		snprintf(path, sizeof(path), "%s/seg_%08d", store, 0);
	else
		snprintf(path, sizeof(path), "%s/seg_%u_%08d", store, shard, 0);
	fd = open(path, O_RDWR);
	if (fd < 0 || page == NULL)
		goto out;
	if (pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t) sizeof(hdr) ||
		(hdr.magic != TEST_SEG_WALLESS_BOUND_MAGIC &&
		 hdr.magic != TEST_SEG_WALLESS_ADMISSION_MAGIC) || hdr.len != page_size ||
		pread(fd, page, page_size, sizeof(hdr) +
			  (hdr.magic == TEST_SEG_WALLESS_ADMISSION_MAGIC ?
			   2 * sizeof(uint64_t) : sizeof(uint64_t))) !=
		(ssize_t) page_size)
		goto out;
	hdr.magic = TEST_SEG_WALLESS_MAGIC;
	if (pwrite(fd, &hdr, sizeof(hdr), 0) != (ssize_t) sizeof(hdr) ||
		pwrite(fd, page, page_size, sizeof(hdr)) != (ssize_t) page_size ||
		ftruncate(fd, sizeof(hdr) + page_size) != 0 || fsync(fd) != 0)
		goto out;
	rc = 0;
out:
	if (fd >= 0)
		close(fd);
	free(page);
	return rc;
}

static void
run_migration_failure_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;

	fprintf(stderr, "== legacy migration failures ==\n");
	for (int fail_at = 1; fail_at <= 2; fail_at++)
	{
		pid_t		pid;
		char		message[96];

		snprintf(shm, sizeof(shm), "/pstest_%d_migrate_%d",
				 (int) getpid(), fail_at);
		snprintf(store, sizeof(store), "%s/store_migrate_%d", tmpbase, fail_at);
		rm_rf(store);
		shm_unlink(shm);
		pid = spawn_daemon_fail_fork_meta(daemon_path, shm, store, ps,
									  test_nshards, fail_at);
		snprintf(message, sizeof(message),
				 "migration append %d failure aborts daemon startup", fail_at);
		expect_daemon_open_failure(pid, shm, message);

		/* The failed process published no writes; a normal restart retries and
		 * seals either the absent start marker or the surviving start marker. */
		pid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
		wait_ready(shm, ps);
		stop_daemon(pid);
		rm_rf(store);
		shm_unlink(shm);
	}

	/* A crash can leave a prefix shorter than one ForkMetaRec.  Startup must
	 * remove it before appending the migration-start marker. */
	{
		char		path[512];
		TestForkMetaRec rec;
		TestForkMetaRecV2 rec2;
		pid_t		pid;
		int			fd;
		struct stat st;

		snprintf(shm, sizeof(shm), "/pstest_%d_migrate_torn", (int) getpid());
		snprintf(store, sizeof(store), "%s/store_migrate_torn", tmpbase);
		rm_rf(store);
		shm_unlink(shm);
		check(mkdir(store, 0700) == 0, "created torn forkmeta test store");
		snprintf(path, sizeof(path), "%s/forkmeta", store);
		fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
		memset(&rec, 0xa5, sizeof(rec));
		check(fd >= 0 && write(fd, &rec, sizeof(rec) / 2) ==
			  (ssize_t) (sizeof(rec) / 2) && fsync(fd) == 0 && close(fd) == 0,
			  "created a torn first forkmeta record");

		pid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
		wait_ready(shm, ps);
		stop_daemon(pid);
		fd = open(path, O_RDONLY);
		memset(&rec2, 0, sizeof(rec2));
		check(fd >= 0 && pread(fd, &rec2, sizeof(rec2), 0) ==
			  (ssize_t) sizeof(rec2) && rec2.magic == TEST_FORK_META_V2_MAGIC &&
			  rec2.kind == 4,
			  "migration marker replaces the torn forkmeta prefix");
		check(fd >= 0 && fstat(fd, &st) == 0 &&
			  st.st_size % (off_t) sizeof(rec2) == 0,
			  "repaired forkmeta contains only complete records");
		if (fd >= 0)
			close(fd);
		rm_rf(store);
		shm_unlink(shm);
	}
}

static void
run_order_marker_failure_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	const uint32_t rel = 29000;
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	pid_t		pid;

	fprintf(stderr, "== segment order-marker failure ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_order_fail", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_order_fail", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	/* Fresh startup writes migration start/done (#1/#2), CREATE is #3, and
	 * the ordered segment-growth marker is #4. */
	/* After the ordered record's two segment writes and failed marker, crash
	 * on the next record's header (successful segment write #3). */
	pid = spawn_daemon_fault(daemon_path, shm, store, ps, test_nshards,
							 0, 3, 4, 0);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 1000);
	fill_page(page, ps, 0, 70);
	check(op_write_tl_status(0, rel, 0, 0, page) == PS_STATUS_ERROR,
		  "order-marker failure rejects the growing segment write");

	/* A normal SEG6 header at the same offset must not borrow the failed
	 * ordered record's complete body if the daemon dies before writing its own
	 * body.  Run the request in a child because it waits on the dead daemon. */
	fill_page(page, ps, 3000, 71);
	client_detach();
	{
		pid_t		writer = fork();
		int			status;

		if (writer == 0)
		{
			client_attach(shm, ps);
			op_write_one(rel, 0, 0, page);
			_exit(0);
		}
		if (writer < 0)
		{
			perror("fork stale-body writer");
			exit(2);
		}
		waitpid(pid, &status, 0);
		check(WIFEXITED(status) && WEXITSTATUS(status) == 86,
			  "injected crash landed after the replacement header");
		kill(writer, SIGKILL);
		waitpid(writer, NULL, 0);
	}

	shm_unlink(shm);
	pid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_nblocks(rel, 0) == 0,
		  "markerless ordered record does not grow after restart");
	op_read_one(rel, 0, 0, readback);
	check(page_all_zero(readback, ps),
		  "markerless ordered record does not publish bytes after restart");

	/* Retry the exact WAL-less write at the same key/block/growth LSN.  Its
	 * bound marker must commit this new record, never the older failed one. */
	fill_page(page, ps, 0, 72);
	op_write_one(rel, 0, 0, page);
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_nblocks(rel, 0) == 1,
		  "successful same-LSN retry recovers its committed growth");
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 72),
		  "bound marker recovers retry bytes, not failed bytes");
	client_detach();
	stop_daemon(pid);

	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_markerless_seg0_dedup_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	const uint32_t rel = 29100;
	unsigned char *page = malloc(ps);
	pid_t		pid;

	fprintf(stderr, "== markerless SEG0 growth dedup ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_seg0_dedup", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_seg0_dedup", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 1000);
	op_zeroextend_at(rel, 0, 0, 1, 1000);
	fill_page(page, ps, 0, 73);
	op_write_one(rel, 0, 0, page);
	op_truncate_at(rel, 0, 0, 1000);
	client_detach();
	stop_daemon(pid);

	/* Model the transition store that persisted SEG0 growth in forkmeta before
	 * SEG0 became self-describing.  Its existing growth precedes the same-LSN
	 * truncate and must not be derived again after that definitive event. */
	check(downgrade_bound_record_to_seg0(store, rel, ps) == 0,
		  "converted bound zero-version record to markerless SEG0");
	check(strip_bound_forkmeta_markers(store) == 0,
		  "removed bound marker while preserving definitive fork history");
	remove_lsm_metadata(store);

	shm_unlink(shm);
	pid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_nblocks(rel, 0) == 0,
		  "pre-persisted SEG0 growth is not replayed after same-LSN truncate");
	client_detach();
	stop_daemon(pid);

	rm_rf(store);
	shm_unlink(shm);
	free(page);
}

static int
segment_exists(const char *store, uint32_t shard, int seg)
{
	char		path[512];

	if (shard == 0)
		snprintf(path, sizeof(path), "%s/seg_%08d", store, seg);
	else
		snprintf(path, sizeof(path), "%s/seg_%u_%08d", store, shard, seg);
	return access(path, F_OK) == 0;
}

static int
local_layer_count(const char *store)
{
	char		pattern[512];
	glob_t		matches;
	int			count;

	snprintf(pattern, sizeof(pattern), "%s/layer_*", store);
	memset(&matches, 0, sizeof(matches));
	if (glob(pattern, 0, NULL, &matches) == GLOB_NOMATCH)
		return 0;
	count = (int) matches.gl_pathc;
	globfree(&matches);
	return count;
}

static uint64_t
local_layer_bytes(const char *store)
{
	char		pattern[512];
	glob_t		matches;
	uint64_t	bytes = 0;

	snprintf(pattern, sizeof(pattern), "%s/layer_*", store);
	memset(&matches, 0, sizeof(matches));
	if (glob(pattern, 0, NULL, &matches) == GLOB_NOMATCH)
		return 0;
	for (size_t i = 0; i < matches.gl_pathc; i++)
	{
		struct stat st;

		if (stat(matches.gl_pathv[i], &st) == 0)
			bytes += (uint64_t) st.st_size;
	}
	globfree(&matches);
	return bytes;
}

static int
read_pruning_metrics(const char *shm, uint64_t *compactions, uint64_t *scanned,
					 uint64_t *kept, uint64_t *deleted)
{
	char output[512];
	unsigned long long c,
					s,
					k,
					d;

	if (!run_inspector(shm, "pruning", output, sizeof(output)) ||
		sscanf(output,
			   "{\"compactions\":%llu,\"versions_scanned\":%llu,"
			   "\"versions_kept\":%llu,\"versions_deleted\":%llu}",
			   &c, &s, &k, &d) != 4)
		return 0;
	*compactions = (uint64_t) c;
	*scanned = (uint64_t) s;
	*kept = (uint64_t) k;
	*deleted = (uint64_t) d;
	return 1;
}

static int
wait_for_compacted_layers(const char *store, int maximum)
{
	for (int i = 0; i < 500; i++)
	{
		int count = local_layer_count(store);

		if (count > 0 && count <= maximum)
			return 1;
		usleep(10000);
	}
	return 0;
}

static void
run_segment_gc_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	const uint32_t rel = 29200;
	PsKey		key = {1, 1, rel, 0, PS_KLASS_RELATION};
	uint32_t	shard = ps_key_shard(&key, test_nshards);
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	uint64_t	fence_seq;
	uint64_t	fork_fence_seq;
	uint64_t	durable_barrier_seq = 0;
	pid_t		pid;

	fprintf(stderr, "== segment GC ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_segment_gc", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_segment_gc", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 29200,
						   1, PS_RETENTION_RESOURCE_PAGE_HISTORY, 5000) ==
		  PS_STATUS_OK,
		  "reader pins admission-fenced page history during compaction");
	op_create_at(rel, 0, 1000);
	fill_page(page, ps, 5000, 80);
	fence_seq = op_write_one_seq(rel, 0, 0, page);
	check(op_retention_set_fenced(0, PS_RETENTION_OWNER_READER, 29200,
								  1, PS_RETENTION_RESOURCE_PAGE_HISTORY,
								  5000, fence_seq) == PS_STATUS_OK,
		  "reader refines its provisional pin to the exact admission fence");
	/* The sequence fence disambiguates mutations only at the boundary LSN.
	 * An older-LSN page admitted later remains part of that as-of view, first
	 * in the memtable and later in its image layer. */
	op_create_at(rel + 3, 0, 4000);
	fill_page(page, ps, 4000, 79);
	op_write_one(rel + 3, 0, 0, page);
	op_read_at_seq(rel + 3, 0, 0, 5000, fence_seq, readback);
	check(page_has_tag(readback, ps, 79),
		  "admission fence does not exclude a later-admitted older-LSN memtable page");
	for (uint32_t block = 1; block < 16; block++)
	{
		fill_page(page, ps, 6000 + block, (unsigned char) block);
		op_write_one(rel, 0, block, page);
	}
	/* Same-LSN latest-wins must survive after both source segments disappear. */
	fill_page(page, ps, 5000, 99);
	op_write_one(rel, 0, 0, page);
	for (uint32_t block = 16; block < 40; block++)
	{
		fill_page(page, ps, 6000 + block, (unsigned char) block);
		op_write_one(rel, 0, block, page);
	}
	for (int i = 0; i < 500 && segment_exists(store, shard, 0); i++)
		usleep(10000);
	check(!segment_exists(store, shard, 0),
		  "layer-covered segment 0 is physically reclaimed");
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 99),
		  "same-LSN rewrite serves newest layer bytes after segment reclamation");
	op_read_at_seq(rel, 0, 0, 5000, fence_seq, readback);
	check(page_has_tag(readback, ps, 80),
		  "admission fence excludes a later same-LSN layer rewrite");
	op_read_at_seq(rel + 3, 0, 0, 5000, fence_seq, readback);
	check(page_has_tag(readback, ps, 79),
		  "admission fence does not exclude a later-admitted older-LSN layer page");

	/* Fork metadata uses the same fence: a same-LSN truncate admitted later
	 * must not shrink the pinned view. */
	op_create_at(rel + 1, 0, 9000);
	fill_page(page, ps, 9000, 100);
	fork_fence_seq = op_write_one_seq(rel + 1, 0, 0, page);
	op_truncate_at(rel + 1, 0, 0, 9000);
	check(op_nblocks_asof(rel + 1, 0, 9000) == 0,
		  "uncapped same-LSN fork metadata serves the later truncate");
	check(op_nblocks_asof_seq(rel + 1, 0, 9000, fork_fence_seq) == 1,
		  "admission fence excludes a later same-LSN truncate");

	/* Reproduce the deferred-control-drain race.  A relation write posted after
	 * the hook's shared gate must remain pending even when it shares a worker
	 * with the barrier request; the worker skips it and serves the barrier/read
	 * channel, then completes it only after the gate is released. */
	{
		PsShmHeader *hdr = (PsShmHeader *) cl_shm;
		PsChannel  *write_ch = ps_channel(cl_shm, cl_chan);
		PsChannel  *barrier_ch = NULL;
		uint64_t	epoch;

		op_create_at(rel + 2, 0, 12000);
		fill_page(page, ps, 12000, 101);
		op_write_one(rel + 2, 0, 0, page);
		check(ps_cas(&hdr->admission_fence_owner, 0, (uint32_t) getpid()),
			  "test checkpoint gate claims its shared owner slot");
		epoch = ps_fetch_add_u64(&hdr->admission_fence_epoch, 1) + 1;
		ps_store_release_u64(&hdr->admission_pending_lsn, 12000);
		ps_store_release_u64(&hdr->admission_pending_epoch, epoch);

		cl_setkey(write_ch, rel + 2, 0);
		write_ch->opcode = PS_OP_WRITEV;
		write_ch->blocknum = 0;
		write_ch->nblocks = 1;
		fill_page(write_ch->data, ps, 12000, 102);
		ps_store_release(&write_ch->state, PS_STATE_REQUEST);

		for (uint32_t i = (uint32_t) cl_chan + hdr->nshards;
			 i < hdr->nchannels; i += hdr->nshards)
		{
			PsChannel  *candidate = ps_channel(cl_shm, i);

			if (ps_cas(&candidate->claimed, 0, 1))
			{
				barrier_ch = candidate;
				break;
			}
		}
		check(barrier_ch != NULL,
			  "admission race test claims a second channel on the same worker");
		if (barrier_ch)
		{
			memset((void *) &barrier_ch->key, 0, sizeof(barrier_ch->key));
			barrier_ch->opcode = PS_OP_ADMISSION_BARRIER;
			barrier_ch->req_lsn = 0;
			barrier_ch->req_seq = 0;
			exec_channel(barrier_ch);
			durable_barrier_seq = barrier_ch->req_seq;
			check(durable_barrier_seq != 0,
				  "admission barrier returns a durable sequence");
			check(ps_load_acquire(&write_ch->state) == PS_STATE_REQUEST,
				  "post-boundary same-LSN write remains gated");

			cl_setkey(barrier_ch, rel + 2, 0);
			barrier_ch->opcode = PS_OP_READ_AT;
			barrier_ch->blocknum = 0;
			barrier_ch->req_lsn = 12000;
			barrier_ch->req_seq = durable_barrier_seq;
			exec_channel(barrier_ch);
			check(page_has_tag(barrier_ch->data, ps, 101),
				  "barrier-capped read excludes the gated same-LSN write");

		}
		check(ps_cas_u64(&hdr->admission_pending_epoch, epoch, 0),
			  "test checkpoint gate releases its epoch");
		ps_store_release_u64(&hdr->admission_pending_lsn, 0);
		ps_store_release(&hdr->admission_fence_owner, 0);
		while (ps_load_acquire(&write_ch->state) != PS_STATE_DONE)
			;
		if (barrier_ch)
		{
			check(write_ch->req_seq > durable_barrier_seq,
				  "released same-LSN write is admitted after the barrier");
			ps_store_release(&barrier_ch->claimed, 0);
		}
	}
	/* Leave a sub-threshold tail outside the committed watermark and crash. */
	for (uint32_t block = 40; block < 42; block++)
	{
		fill_page(page, ps, 6000 + block, (unsigned char) block);
		op_write_one(rel, 0, block, page);
	}
	client_detach();
	kill(pid, SIGKILL);
	waitpid(pid, NULL, 0);

	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 99),
		  "same-LSN rewrite survives layer-prefix recovery after GC");
	op_read_at_seq(rel, 0, 0, 5000, fence_seq, readback);
	check(page_has_tag(readback, ps, 80),
		  "admission-fenced same-LSN page survives compaction, GC, and restart");
	op_read_at_seq(rel + 3, 0, 0, 5000, fence_seq, readback);
	check(page_has_tag(readback, ps, 79),
		  "older-LSN page remains visible through admission-fenced recovery");
	check(op_nblocks_asof_seq(rel + 1, 0, 9000, fork_fence_seq) == 1,
		  "admission-fenced fork metadata survives restart");
	op_read_one(rel, 0, 41, readback);
	check(page_has_tag(readback, ps, 41),
		  "unflushed segment tail survives watermark-boundary crash recovery");
	check(op_nblocks(rel, 0) == 42,
		  "fork growth survives layer-prefix recovery after GC");
	{
		uint64_t	restarted_barrier_seq = 0;

		check(op_admission_barrier(&restarted_barrier_seq) == PS_STATUS_OK &&
			  restarted_barrier_seq > durable_barrier_seq,
			  "restart allocates above an unclaimed durable admission barrier");
	}
	check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, 29201, 1,
					   PS_RETENTION_RESOURCE_PAGE_HISTORY, 12000) == PS_STATUS_OK,
		  "a durable replacement cutoff protects current page history");
	check(op_retention_drop(0, PS_RETENTION_OWNER_READER, 29200, 1) ==
		  PS_STATUS_OK,
		  "dropping the reader releases admission-fenced page history");
	{
		PsChannel  *ch = ps_channel(cl_shm, cl_chan);

		for (int i = 0; i < 500; i++)
		{
			cl_setkey(ch, rel, 0);
			ch->opcode = PS_OP_READ_AT;
			ch->blocknum = 0;
			ch->req_lsn = 5000;
			ch->req_seq = fence_seq;
			cl_exec();
			if (ch->status == PS_STATUS_OK && ch->result == 0)
				break;
			usleep(10000);
		}
		check(ch->status == PS_STATUS_OK && ch->result == 0,
			  "retention release recomputes a lone layer without new writes");
	}
	for (uint32_t block = 42; block < 66; block++)
	{
		fill_page(page, ps, 13000 + block, (unsigned char) block);
		op_write_one(rel, 0, block, page);
	}
	for (int i = 0; i < 500 && segment_exists(store, shard, 7); i++)
		usleep(10000);
	check(!segment_exists(store, shard, 7),
		  "released page history is covered before its segment is reclaimed");
	{
		PsChannel  *ch = ps_channel(cl_shm, cl_chan);

		for (int i = 0; i < 500; i++)
		{
			cl_setkey(ch, rel, 0);
			ch->opcode = PS_OP_READ_AT;
			ch->blocknum = 0;
			ch->req_lsn = 5000;
			ch->req_seq = fence_seq;
			cl_exec();
			if (ch->result == 0)
				break;
			usleep(10000);
		}
		check(ch->status == PS_STATUS_OK && ch->result == 0,
			  "live page index forgets history removed by compaction");
	}
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(!op_read_at_seq_found(rel, 0, 0, 5000, fence_seq, readback),
		  "compaction durably prunes released admission-fenced page history");
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 29202, 1,
						   PS_RETENTION_RESOURCE_PAGE_HISTORY, 7000) ==
		  PS_STATUS_ERROR,
		  "restart rejects a pin below the durable page reclamation frontier");
	check(op_retention_set_fenced(0, PS_RETENTION_OWNER_READER, 29203, 1,
								  PS_RETENTION_RESOURCE_PAGE_HISTORY,
								  12000, 1) == PS_STATUS_ERROR,
		  "restart rejects a lower admission fence at the reclaimed frontier LSN");
	check(op_retention_set_fenced(0, PS_RETENTION_OWNER_READER, 29204, 1,
								  PS_RETENTION_RESOURCE_PAGE_HISTORY,
								  13000, 1) == PS_STATUS_OK,
		  "restart accepts a lower admission sequence above the frontier LSN");
	check(op_create_branch_status(20, 0, 7000) == PS_STATUS_ERROR,
		  "restart rejects a branch below the durable page reclamation frontier");
	client_detach();
	stop_daemon(pid);

	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_prune_branch_retention_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	uint32_t	target_shard = test_nshards > 1 ? 1 : 0;
	uint32_t	rel = find_relation_on_shard(target_shard, test_nshards);
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	const struct
	{
		uint64_t lsn;
		unsigned char tag;
	} versions[] = {{500, 50}, {1000, 100}, {2000, 120},
				   {3000, 130}, {4000, 140}};
	pid_t		pid;

	fprintf(stderr, "== branch-projected page pruning ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_prune_branch", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_prune_branch", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++)
	{
		fill_page(page, ps, versions[i].lsn, versions[i].tag);
		op_write_tl(0, rel, 0, 0, page);
	}
	op_create_branch(20, 0, 3500);
	op_create_branch(21, 20, 2500);
	check(op_retention_set(21, PS_RETENTION_OWNER_READER, 2100, 1,
						   PS_RETENTION_RESOURCE_PAGE_HISTORY, 1500) ==
		  PS_STATUS_OK,
		  "descendant reader registers a page-history floor below both fork caps");
	for (uint32_t block = 1; block <= 48; block++)
	{
		fill_page(page, ps, 5000 + block, (unsigned char) block);
		op_write_tl(0, rel, 0, block, page);
	}
	check(wait_for_compacted_layers(store, 3),
		  "descendant-pinned history reaches a bounded compacted layer set");
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(!op_read_at_tl_found(0, rel, 0, 0, 750, readback),
		  "compaction prunes history older than the descendant reader base");
	op_read_at_tl(21, rel, 0, 0, 1500, readback);
	check(page_has_tag(readback, ps, 100),
		  "descendant reader floor preserves parent history through nested caps");

	check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, 2101, 1,
					   PS_RETENTION_RESOURCE_PAGE_HISTORY, 4000) == PS_STATUS_OK,
		  "branch test installs a replacement operational cutoff");
	check(op_retention_drop(21, PS_RETENTION_OWNER_READER, 2100, 1) ==
		  PS_STATUS_OK,
		  "descendant reader releases its projected page-history floor");
	for (uint32_t block = 49; block <= 96; block++)
	{
		fill_page(page, ps, 6000 + block, (unsigned char) block);
		op_write_tl(0, rel, 0, block, page);
	}
	check(wait_for_compacted_layers(store, 3),
		  "fork-capped history reaches a bounded compacted layer set");
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(!op_read_at_tl_found(0, rel, 0, 0, 1500, readback),
		  "nested structural floor prunes history released by the descendant");
	op_read_tl(20, rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 130),
		  "direct child retains the newest parent page below its fork cap");
	op_read_tl(21, rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 120),
		  "nested child retains the newest parent page below its lower fork cap");

	client_detach();
	stop_daemon(pid);
	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_prune_relation_lifecycle_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	uint32_t	rel = find_relation_on_shard(0, test_nshards);
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	pid_t		pid;

	fprintf(stderr, "== relation-lifecycle page pruning ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_prune_relation", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_prune_relation", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 2200, 1,
						   PS_RETENTION_RESOURCE_PAGE_HISTORY, 6500) ==
		  PS_STATUS_OK,
		  "reader pins relation history before truncate/drop/recreate");
	op_create_at(rel, 0, 1000);
	fill_page(page, ps, 1500, 15);
	op_write_tl(0, rel, 0, 0, page);
	fill_page(page, ps, 6000, 60);
	op_write_tl(0, rel, 0, 0, page);
	op_truncate_at(rel, 0, 0, 7000);
	op_zeroextend_at(rel, 0, 0, 1, 8000);
	op_unlink_at(rel, 0, 9000);
	op_create_at(rel, 0, 10000);
	/* WAL replay can observe the new CREATE before an older UNLINK.  The
	 * empty generation must survive even though no new page growth follows it. */
	op_create_at(rel + test_nshards, 0, 1000);
	fill_page(page, ps, 6000, 61);
	op_write_tl(0, rel + test_nshards, 0, 0, page);
	op_create_at(rel + test_nshards, 0, 10000);
	op_unlink_at(rel + test_nshards, 0, 9000);
	check(op_exists_asof(rel + test_nshards, 0, 10500) &&
		  op_nblocks_asof(rel + test_nshards, 0, 10500) == 0,
		  "create before delayed unlink preserves an empty recreated generation");
	fill_page(page, ps, 11000, 110);
	op_write_tl(0, rel, 0, 0, page);
	for (uint32_t block = 1; block <= 48; block++)
	{
		fill_page(page, ps, 12000 + block, (unsigned char) block);
		op_write_tl(0, rel, 0, block, page);
	}
	check(wait_for_compacted_layers(store, 3),
		  "relation-lifecycle history reaches a bounded compacted layer set");
	{
		char output[512];

		check(run_inspector(shm, "pruning", output, sizeof(output)) &&
			  strstr(output, "\"compactions\":0") == NULL &&
			  strstr(output, "\"versions_scanned\":0") == NULL &&
			  strstr(output, "\"versions_deleted\":0") == NULL,
			  "pruning inspection exposes nonzero compaction work and deletion");
	}
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(!op_read_at_found(rel, 0, 0, 2000, readback),
		  "compaction prunes the obsolete page version below the reader base");
	check(op_read_at_found(rel, 0, 0, 6500, readback) &&
		  page_has_tag(readback, ps, 60),
		  "reader base survives relation compaction and restart");
	check(op_exists_asof(rel, 0, 6500) && op_nblocks_asof(rel, 0, 6500) == 1,
		  "pre-truncate relation metadata remains visible at the retained floor");
	check(op_exists_asof(rel, 0, 7500) && op_nblocks_asof(rel, 0, 7500) == 0,
		  "truncate remains visible after page pruning");
	check(!op_read_at_found(rel, 0, 0, 7500, readback),
		  "truncated interval cannot expose the retained page");
	check(!op_read_at_found(rel, 0, 0, 8500, readback),
		  "regrowth cannot revive the pre-truncate page bytes");
	check(!op_exists_asof(rel, 0, 9500),
		  "drop remains visible between drop and recreate");
	check(op_exists_asof(rel, 0, 10500) &&
		  op_nblocks_asof(rel, 0, 10500) == 0,
		  "recreated relation is empty before its new generation page");
	check(!op_read_at_found(rel, 0, 0, 10500, readback),
		  "empty recreated generation cannot expose the dropped generation page");
	check(op_exists_asof(rel + test_nshards, 0, 10500) &&
		  op_nblocks_asof(rel + test_nshards, 0, 10500) == 0,
		  "empty generation from create-before-unlink survives restart");
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 110),
		  "new relation generation remains current after compaction and restart");

	check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, 2201, 1,
					   PS_RETENTION_RESOURCE_PAGE_HISTORY, 12000) == PS_STATUS_OK,
		  "relation lifecycle establishes a durable replacement cutoff");
	check(op_retention_drop(0, PS_RETENTION_OWNER_READER, 2200, 1) ==
		  PS_STATUS_OK,
		  "reader releases relation-lifecycle history");
	for (uint32_t block = 49; block <= 96; block++)
	{
		fill_page(page, ps, 13000 + block, (unsigned char) block);
		op_write_tl(0, rel, 0, block, page);
	}
	{
		int pruned = 0;

		for (int i = 0; i < 500; i++)
		{
			if (!op_read_at_found(rel, 0, 0, 6500, readback))
			{
				pruned = 1;
				break;
			}
			usleep(10000);
		}
		check(pruned, "released relation history is compacted again");
	}
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(!op_read_at_found(rel, 0, 0, 6500, readback),
		  "unprotected pre-truncate page history is durably pruned");
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 110),
		  "pruning released history cannot expose an older relation generation");

	client_detach();
	stop_daemon(pid);
	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_prune_publication_crash_case(const char *daemon_path, const char *tmpbase,
								 const char *phase, int case_no)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	uint32_t	rel = find_relation_on_shard(0, test_nshards);
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	int			crashed_layer_count;
	char		marker[512];
	pid_t		pid;

	snprintf(shm, sizeof(shm), "/pstest_%d_prune_crash_%d",
			 (int) getpid(), case_no);
	snprintf(store, sizeof(store), "%s/store_prune_crash_%d", tmpbase, case_no);
	rm_rf(store);
	shm_unlink(shm);

	check(setenv("PAGESTORE_TEST_FAULT", "1", 1) == 0 &&
		  setenv("PAGESTORE_TEST_CRASH_COMPACTION_PHASE", phase, 1) == 0,
		  "configure guarded compaction fault %s", phase);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	unsetenv("PAGESTORE_TEST_FAULT");
	unsetenv("PAGESTORE_TEST_CRASH_COMPACTION_PHASE");
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 500);
	for (uint32_t batch = 1; batch <= 3; batch++)
	{
		fill_page(page, ps, batch * 1000, (unsigned char) (batch * 10));
		op_write_tl(0, rel, 0, 0, page);
		for (uint32_t i = 1; i < 8; i++)
		{
			uint32_t block = (batch - 1) * 7 + i;

			fill_page(page, ps, batch * 1000 + i, (unsigned char) block);
			op_write_tl(0, rel, 0, block, page);
		}
	}
	fill_page(page, ps, 4000, 40);
	op_write_tl(0, rel, 0, 0, page);
	snprintf(marker, sizeof(marker), "%s/.test-crash-compaction-armed", store);
	{
		int fd = open(marker, O_CREAT | O_EXCL | O_WRONLY, 0600);

		check(fd >= 0, "arm compaction publication fault %s", phase);
		if (fd >= 0)
			close(fd);
	}
	check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED,
					   23000 + (uint64_t) case_no, 1,
					   PS_RETENTION_RESOURCE_PAGE_HISTORY, 3500) == PS_STATUS_OK,
		  "crash case establishes a durable page cutoff");
	client_detach();
	check(wait_for_daemon_exit(pid, 88),
		  "compaction crashes at deterministic phase %s", phase);
	unlink(marker);
	crashed_layer_count = local_layer_count(store);

	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 40),
		  "restart after %s serves the published newest page", phase);
	check(!op_read_at_found(rel, 0, 0, 1000, readback),
		  "restart after %s cannot resurrect pruned source history", phase);
	for (int i = 0; i < 500 &&
		 local_layer_count(store) >= crashed_layer_count; i++)
		usleep(10000);
	check(local_layer_count(store) < crashed_layer_count,
		  "restart after %s removes a specifically marked source layer", phase);
	check(wait_for_compacted_layers(store, 3),
		  "restart after %s resumes deletion and bounds live layers", phase);
	check(!op_read_at_found(rel, 0, 0, 1000, readback),
		  "recovered cleanup after %s removes obsolete page history", phase);
	client_detach();
	stop_daemon(pid);

	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 40),
		  "second restart after %s preserves the recovered compacted layer", phase);
	client_detach();
	stop_daemon(pid);
	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_prune_publication_recovery_suite(const char *daemon_path,
								 const char *tmpbase)
{
	fprintf(stderr, "== page-pruning publication recovery ==\n");
	run_prune_publication_crash_case(daemon_path, tmpbase,
								 "after_publish", 1);
	run_prune_publication_crash_case(daemon_path, tmpbase,
								 "after_mark_delete", 2);
	run_prune_publication_crash_case(daemon_path, tmpbase,
								 "after_frontier", 3);
}

static void
run_prune_bounded_churn_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	uint32_t	rel = find_relation_on_shard(0, test_nshards);
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	uint64_t	compactions = 0,
				scanned = 0,
				kept = 0,
				deleted = 0;
	uint64_t	bytes;
	pid_t		pid;

	fprintf(stderr, "== bounded page-history churn ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_prune_churn", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_prune_churn", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 500);
	for (uint32_t cycle = 0; cycle < 12; cycle++)
	{
		for (uint32_t write = 0; write < 32; write++)
		{
			uint64_t lsn = 1000 + cycle * 100 + write;

			fill_page(page, ps, lsn, (unsigned char) (20 + cycle));
			op_write_tl(0, rel, 0, write % 4, page);
		}
		check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, 2400, 1,
						   PS_RETENTION_RESOURCE_PAGE_HISTORY,
						   1000 + cycle * 100 + 31) == PS_STATUS_OK,
			  "churn cycle %u advances its durable page cutoff", cycle);
		check(wait_for_compacted_layers(store, 3),
			  "churn cycle %u returns to the configured live-layer bound", cycle);
	}
	bytes = local_layer_bytes(store);
	check(bytes > 0 && bytes < (uint64_t) ps * 64,
		  "twelve churn cycles retain fewer than 64 page images on disk");
	check(read_pruning_metrics(shm, &compactions, &scanned, &kept, &deleted) &&
		  compactions >= 12 && scanned == kept + deleted && deleted > kept,
		  "pruning counters account for churn and delete more versions than remain");
	for (uint32_t block = 0; block < 4; block++)
	{
		op_read_one(rel, 0, block, readback);
		check(page_has_tag(readback, ps, 31),
			  "bounded churn serves the newest page for block %u", block);
	}

	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(local_layer_bytes(store) < (uint64_t) ps * 64,
		  "bounded retained page history survives restart");
	for (uint32_t block = 0; block < 4; block++)
	{
		op_read_one(rel, 0, block, readback);
		check(page_has_tag(readback, ps, 31),
			  "restarted bounded churn serves newest block %u", block);
	}
	client_detach();
	stop_daemon(pid);
	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_orphan_layer_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	char		orphan[512];
	const uint32_t ps = 8192;
	const uint32_t rel = 29300;
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	pid_t		pid;
	int			fd;
	struct stat orphan_stat;

	fprintf(stderr, "== orphan layer ID recovery ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_orphan_layer", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_orphan_layer", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon(daemon_path, shm, store, ps, 1);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 1000);
	fill_page(page, ps, 5000, 74);
	op_write_one(rel, 0, 0, page);
	client_detach();
	kill(pid, SIGKILL);
	waitpid(pid, NULL, 0);

	snprintf(orphan, sizeof(orphan), "%s/layer_0_%016llx", store, 1ULL);
	fd = open(orphan, O_WRONLY | O_CREAT | O_EXCL, 0600);
	check(fd >= 0 && close(fd) == 0, "created orphan layer ID 1");
	shm_unlink(shm);
	pid = spawn_daemon(daemon_path, shm, store, ps, 1);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 74),
		  "recovery skips orphan layer ID and preserves tail bytes");
	client_detach();
	stop_daemon(pid);
	check(stat(orphan, &orphan_stat) == 0 && orphan_stat.st_size == 0,
		  "recovery leaves the orphan ID untouched while allocating above it");

	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_reshard_segment_gc_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	uint32_t	rel = 29400;
	PsKey		key;
	unsigned char *page = malloc(ps);
	unsigned char *readback = malloc(ps);
	pid_t		pid;

	memset(&key, 0, sizeof(key));
	key.spcOid = 1;
	key.dbOid = 1;
	key.forkNum = 0;
	key.klass = PS_KLASS_RELATION;
	while ((key.relNumber = rel, ps_key_shard(&key, 4)) == 0)
		rel++;
	fprintf(stderr, "== 1-to-4 shard segment GC ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_reshard_gc", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_reshard_gc", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	pid = spawn_daemon(daemon_path, shm, store, ps, 1);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 1000);
	for (uint32_t block = 0; block < 40; block++)
	{
		fill_page(page, ps, 5000 + block, (unsigned char) (90 + block));
		op_write_one(rel, 0, block, page);
	}
	client_detach();
	stop_daemon(pid);
	check(segment_exists(store, 0, 0),
		  "single-shard source segment remains while GC is disabled");

	shm_unlink(shm);
	pid = spawn_daemon_gc(daemon_path, shm, store, ps, 4);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	for (int i = 0; i < 500 && segment_exists(store, 0, 0); i++)
		usleep(10000);
	check(!segment_exists(store, 0, 0),
		  "legacy shard-0 segment is reclaimed after opening with four shards");
	op_read_one(rel, 0, 0, readback);
	check(page_has_tag(readback, ps, 90),
		  "resharded page remains layer-readable after source segment deletion");
	client_detach();
	stop_daemon(pid);

	rm_rf(store);
	shm_unlink(shm);
	free(page);
	free(readback);
}

static void
run_shard_count_change_rejection_suite(const char *daemon_path,
									   const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	char		marker[512];
	const uint32_t ps = 8192;
	uint32_t	rel = find_relation_on_shard(1, 2);
	unsigned char *page = malloc(ps);
	pid_t		pid;

	fprintf(stderr, "== reject unsupported shard-count changes ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_shard_count", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_shard_count", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	check(rel != 0 && page != NULL, "prepare a two-shard relation");
	pid = spawn_daemon(daemon_path, shm, store, ps, 2);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_create_at(rel, 0, 1000);
	fill_page(page, ps, 5000, 88);
	op_write_one(rel, 0, 0, page);
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);

	check(snprintf(marker, sizeof(marker), "%s/.pagestore-nshards", store) > 0,
		  "build shard-count marker path");
	check(unlink(marker) == 0, "simulate a pre-marker multi-shard store");
	pid = spawn_daemon(daemon_path, shm, store, ps, 4);
	expect_daemon_open_failure(pid, shm,
							   "markerless multi-shard segment store rejects unsupported shard-count changes");
	rm_rf(store);
	shm_unlink(shm);
	free(page);
}

static void
run_legacy_walidx_reshard_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	const uint32_t ps = 8192;
	uint32_t	rel = find_relation_on_shard(1, 4);
	unsigned char wal[512] = {0};
	PsWalRec	out[4];
	uint64_t	progress;
	pid_t		pid;
	int			n;

	fprintf(stderr, "== legacy WAL-index reshard replay ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_legacy_widx", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_legacy_widx", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	check(rel != 0, "test harness can find a relation that moves to shard 1");
	pid = spawn_daemon(daemon_path, shm, store, ps, 1);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_wal_append(0, 0, wal, sizeof(wal));
	op_walidx_add(0, rel, 0, 0, 320);
	check(op_walidx_progress(0, 0, 350, &progress) == 0,
		  "single-shard WAL-index progress commits before reshard");
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);

	pid = spawn_daemon(daemon_path, shm, store, ps, 4);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	n = op_walidx_get(0, rel, 0, 0, 1000000, out);
	check(n == 1 && out[0].lsn == 320,
		  "reshard replays legacy physical shard-zero WAL-index record");
	check(op_walidx_progress(0, 350, 400, &progress) == 0,
		  "post-reshard progress keeps legacy WAL-index ownership physical");
	client_detach();
	stop_daemon(pid);
	shm_unlink(shm);

	pid = spawn_daemon(daemon_path, shm, store, ps, 4);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	n = op_walidx_get(0, rel, 0, 0, 1000000, out);
	check(n == 1 && out[0].lsn == 320,
		  "post-reshard WAL-index progress survives a second restart");
	client_detach();
	stop_daemon(pid);
	rm_rf(store);
	shm_unlink(shm);
}

/* ===================== the test suite ================================== */

#define REL_A	16000
#define REL_B	17000
#define REL_C	18000
#define REL_D	19000
#define REL_E	20000
#define REL_F	21000
#define REL_G	22000
#define REL_H	23000
#define REL_I	24000
#define REL_J	25000
#define REL_K	26000
#define REL_L	27000
#define REL_M	28000
#define REL_N	29000
#define REL_O	30000
#define REL_P	31000
#define REL_Q	32000
#define FORK0	0
#define SLRU_ZERO_OBJ	4243

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

	dpid = spawn_daemon(daemon_path, shm, store, page_size, test_nshards);
	wait_ready(shm, page_size);
	check_inspector(shm, page_size);
	client_attach(shm, page_size);
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 29001, 1,
						   PS_RETENTION_RESOURCE_PAGE_HISTORY, 1) == PS_STATUS_OK,
		  "generic as-of tests pin the history they later read");

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

	/* --- fork-size history: as-of NBLOCKS/EXISTS ---------------------- */
	check(!op_exists_asof(REL_C, FORK0, UINT64_MAX), "no history: fork not found at any horizon");
	op_create_at(REL_C, FORK0, 1000);
	check(!op_exists_asof(REL_C, FORK0, 999), "created@1000: invisible at 999");
	check(op_exists_asof(REL_C, FORK0, 1000), "created@1000: visible at 1000");
	check(op_nblocks_asof(REL_C, FORK0, 1000) == 0, "created empty");
	fill_page(pa, page_size, 2000, 1);
	op_write_one(REL_C, FORK0, 0, pa);
	fill_page(pa, page_size, 3000, 2);
	op_write_one(REL_C, FORK0, 1, pa);
	fill_page(pa, page_size, 4000, 3);
	op_write_one(REL_C, FORK0, 2, pa);
	check(op_nblocks_asof(REL_C, FORK0, 1500) == 0, "no blocks below the first write's LSN");
	check(op_nblocks_asof(REL_C, FORK0, 2000) == 1, "one block as of its pd_lsn");
	check(op_nblocks_asof(REL_C, FORK0, 3500) == 2, "two blocks between the 2nd and 3rd writes");
	check(op_nblocks(REL_C, FORK0) == 3, "newest size after three writes");
	fill_page(pa, page_size, 5000, 9);
	op_write_one(REL_C, FORK0, 0, pa);	/* rewrite: no size change */
	check(op_nblocks_asof(REL_C, FORK0, 4999) == 3, "a rewrite adds no size event");
	op_zeroextend_at(REL_C, FORK0, 3, 2, 6000);
	check(op_nblocks_asof(REL_C, FORK0, 5999) == 3, "zero-extend invisible below its LSN");
	check(op_nblocks_asof(REL_C, FORK0, 6000) == 5, "zero-extend visible at its LSN");
	op_truncate_at(REL_C, FORK0, 1, 7000);
	check(op_nblocks_asof(REL_C, FORK0, 6999) == 5, "pre-truncate horizon keeps the old size");
	check(op_nblocks_asof(REL_C, FORK0, 7000) == 1, "truncate visible at its LSN");
	check(op_nblocks(REL_C, FORK0) == 1, "newest size after truncate");
	fill_page(pa, page_size, 8000, 7);
	op_write_one(REL_C, FORK0, 1, pa);	/* regrow past the truncate */
	check(op_nblocks_asof(REL_C, FORK0, 7500) == 1, "between truncate and regrow: truncated size");
	check(op_nblocks_asof(REL_C, FORK0, 8000) == 2, "regrow after truncate counts from the truncated size");
	op_unlink_at(REL_C, FORK0, 9000);
	check(!op_exists(REL_C, FORK0), "unlinked: gone at newest");
	check(op_nblocks(REL_C, FORK0) == 0, "unlinked: zero blocks at newest");
	check(op_exists_asof(REL_C, FORK0, 8999), "as-of below the unlink still exists");
	check(op_nblocks_asof(REL_C, FORK0, 8500) == 2, "as-of size below the unlink");
	op_create_at(REL_C, FORK0, 10000);
	check(op_exists(REL_C, FORK0), "recreate after unlink");
	check(op_nblocks(REL_C, FORK0) == 0, "recreated fork is empty");
	check(!op_exists_asof(REL_C, FORK0, 9500), "between unlink and recreate: not exists");

	/* WAL-less growth (pd_lsn 0, unlogged relations): ordered at the
	 * definitive floor instead of sorting below it and being covered */
	op_create_at(REL_D, FORK0, 1000);
	fill_page(pa, page_size, 0, 41);
	op_write_one(REL_D, FORK0, 0, pa);
	check(op_nblocks(REL_D, FORK0) == 1,
		  "pd_lsn-0 growth raises the newest size past the create SET");
	op_truncate_at(REL_D, FORK0, 0, 2000);
	check(op_nblocks(REL_D, FORK0) == 0, "truncate resets the WAL-less fork");
	fill_page(pa, page_size, 0, 42);
	op_write_one(REL_D, FORK0, 0, pa);
	check(op_nblocks(REL_D, FORK0) == 1,
		  "pd_lsn-0 regrow is ordered at the truncate floor, not under it");
	check(op_nblocks_asof(REL_D, FORK0, 1999) == 1,
		  "the pre-truncate WAL-less block stays visible below the truncate");
	/* the resurrection case: WAL-less growth then truncate, NO regrow --
	 * recovery must not reorder the growth above the truncate */
	op_create_at(REL_E, FORK0, 1000);
	fill_page(pa, page_size, 0, 43);
	op_write_one(REL_E, FORK0, 0, pa);
	op_truncate_at(REL_E, FORK0, 0, 2000);
	check(op_nblocks(REL_E, FORK0) == 0, "WAL-less fork truncated to empty");
	/* Equal LSNs still have an operation order.  SEG0's fork-meta ordering
	 * marker must keep the later definitive event after the earlier growth. */
	op_create_at(REL_L, FORK0, 3000);
	fill_page(pa, page_size, 0, 68);
	op_write_one(REL_L, FORK0, 0, pa);
	op_truncate_at(REL_L, FORK0, 0, 3000);
	check(op_nblocks(REL_L, FORK0) == 0,
		  "same-LSN truncate wins over earlier WAL-less growth");
	op_create_at(REL_M, FORK0, 4000);
	fill_page(pa, page_size, 0, 69);
	op_write_one(REL_M, FORK0, 0, pa);
	op_unlink_at(REL_M, FORK0, 4000);
	check(!op_exists(REL_M, FORK0),
		  "same-LSN unlink wins over earlier WAL-less growth");
	/* The same ordering rule applies when a copied nonzero page is clamped
	 * from its source pd_lsn to the fork's definitive floor. */
	op_create_at(REL_N, FORK0, 5000);
	fill_page(pa, page_size, 3000, 71);
	op_write_one(REL_N, FORK0, 0, pa);
	op_truncate_at(REL_N, FORK0, 0, 5000);
	check(op_nblocks(REL_N, FORK0) == 0,
		  "same-LSN truncate wins over earlier clamped copied growth");
	op_create_at(REL_O, FORK0, 6000);
	fill_page(pa, page_size, 4000, 72);
	op_write_one(REL_O, FORK0, 0, pa);
	op_unlink_at(REL_O, FORK0, 6000);
	check(!op_exists(REL_O, FORK0),
		  "same-LSN unlink wins over earlier clamped copied growth");
	/* Ordered records need a commit marker even when an earlier growth already
	 * covers their block.  Otherwise recovery would re-derive markerless growth
	 * after the same-LSN definitive event and resurrect the fork. */
	op_create_at(REL_P, FORK0, 7000);
	op_zeroextend_at(REL_P, FORK0, 0, 1, 7000);
	fill_page(pa, page_size, 0, 73);
	op_write_one(REL_P, FORK0, 0, pa);
	op_truncate_at(REL_P, FORK0, 0, 7000);
	check(op_nblocks(REL_P, FORK0) == 0,
		  "same-LSN truncate wins over non-growing WAL-less write");
	op_create_at(REL_Q, FORK0, 8000);
	op_zeroextend_at(REL_Q, FORK0, 0, 1, 8000);
	fill_page(pa, page_size, 6000, 74);
	op_write_one(REL_Q, FORK0, 0, pa);
	op_unlink_at(REL_Q, FORK0, 8000);
	check(!op_exists(REL_Q, FORK0),
		  "same-LSN unlink wins over non-growing clamped write");
	/* copied pages keep their SOURCE pd_lsn, which can sit below the new
	 * fork's create SET (skip-WAL relation rewrites): growth clamps to the
	 * definitive floor instead of vanishing under it */
	op_create_at(REL_F, FORK0, 5000);
	fill_page(pa, page_size, 3000, 44);	/* pd_lsn below the create */
	op_write_one(REL_F, FORK0, 0, pa);
	check(op_nblocks(REL_F, FORK0) == 1,
		  "below-floor copied-page growth clamps to the create, not under it");
	check(op_nblocks_asof(REL_F, FORK0, 4999) == 0,
		  "the copied page is not visible below the fork's creation");
	/* the record itself is stamped at the floor, so the BYTES agree with
	 * the size: an as-of read in the pre-create gap finds nothing */
	check(!op_read_at_found(REL_F, FORK0, 0, 4000, rb),
		  "as-of page bytes agree with the size below the create (not found)");
	check(op_read_at_found(REL_F, FORK0, 0, 5000, rb) &&
		  page_has_tag(rb, page_size, 44),
		  "the copied page serves at the fork's creation floor");

	/* A retained dirty buffer can flush after truncate with an older pd_lsn.
	 * It is not growth at the truncate floor, so preserve its raw version. */
	op_create_at(REL_G, FORK0, 1000);
	fill_page(pa, page_size, 1500, 60);
	op_write_one(REL_G, FORK0, 0, pa);
	fill_page(pa, page_size, 1600, 61);
	op_write_one(REL_G, FORK0, 1, pa);
	op_truncate_at(REL_G, FORK0, 1, 3000);
	fill_page(pa, page_size, 2000, 62);
	op_write_one(REL_G, FORK0, 0, pa);
	check(op_read_at_found(REL_G, FORK0, 0, 2500, rb) &&
		  page_has_tag(rb, page_size, 62),
		  "retained-block flush keeps its pre-truncate page LSN");

	/* A real pre-truncate growth can have the same target size as a later
	 * WAL-less regrow at the truncate floor.  Markerless recovery must not
	 * mistake that later GROW for proof that the raw record was clamped. */
	op_create_at(REL_H, FORK0, 1000);
	fill_page(pa, page_size, 2000, 63);
	op_write_one(REL_H, FORK0, 2, pa);
	op_truncate_at(REL_H, FORK0, 1, 3000);
	fill_page(pa, page_size, 0, 64);
	op_write_one(REL_H, FORK0, 2, pa);
	check(op_nblocks_asof(REL_H, FORK0, 2500) == 3,
		  "real growth remains visible before same-size WAL-less regrow");

	/* Zeroextend at CREATE's LSN can already cover the copied block at the
	 * floor, but the bytes still must not be visible before CREATE. */
	op_create_at(REL_J, FORK0, 5000);
	op_zeroextend_at(REL_J, FORK0, 0, 2, 5000);
	fill_page(pa, page_size, 3000, 67);
	op_write_one(REL_J, FORK0, 0, pa);
	check(!op_read_at_found(REL_J, FORK0, 0, 4000, rb),
		  "same-LSN pre-extension does not expose copied bytes before CREATE");
	check(op_read_at_found(REL_J, FORK0, 0, 5000, rb) &&
		  page_has_tag(rb, page_size, 67),
		  "copied bytes serve at the pre-extended CREATE floor");

	/* A replayed/retried zeroextend can carry an LSN below the definitive
	 * floor.  Persist only its clamped position, never the raw request LSN. */
	op_create_at(REL_K, FORK0, 5000);
	op_zeroextend_at(REL_K, FORK0, 0, 3, 3000);
	check(op_nblocks_asof(REL_K, FORK0, 4000) == 0,
		  "below-floor zeroextend is invisible before CREATE");
	check(op_nblocks_asof(REL_K, FORK0, 5000) == 3,
		  "below-floor zeroextend appears at its clamped floor");
	op_read_at(REL_A, FORK0, 0, ~0ull, rb);
	check(page_has_tag(rb, page_size, 200), "read_at(max) returns newest");

	/* --- SLRU-class versioning: version is the caller's LSN, not a daemon counter ---
	 * Two snapshots of an SLRU segment keyed by cutoffs C1=5000 and C2=9000.  An
	 * as-of read must resolve by those exact LSNs; a daemon max+1 counter (1,2) would
	 * make read_at(7000) and read_at(4000) resolve wrongly. */
	{
		const uint32_t SLRU_OBJ = 4242;	/* an slru_klass_id stand-in */

		fill_page(pa, page_size, 0, 111);	/* pd_lsn irrelevant for SLRU; tag 111 */
		op_write_slru(SLRU_OBJ, 0, pa, 5000);
		fill_page(pb, page_size, 0, 222);
		op_write_slru(SLRU_OBJ, 0, pb, 9000);

		op_read_at_slru(SLRU_OBJ, 0, 7000, rb);
		check(page_has_tag(rb, page_size, 111),
			  "slru read_at(7000) = C1 snapshot (version is the caller LSN 5000)");
		op_read_at_slru(SLRU_OBJ, 0, 9000, rb);
		check(page_has_tag(rb, page_size, 222), "slru read_at(9000) = C2 snapshot");
		op_read_at_slru(SLRU_OBJ, 0, 4000, rb);
		check(page_all_zero(rb, page_size),
			  "slru read_at(4000) = none (no snapshot at/below 4000; not a counter)");
		op_read_at_slru(SLRU_OBJ, 0, ~0ull, rb);
		check(page_has_tag(rb, page_size, 222), "slru read_at(max) = newest snapshot");
	}

	/* Version zero is a legitimate newest-only object image.  It uses SEG0 so
	 * the complete segment record recovers both the bytes and block growth. */
	fill_page(pa, page_size, 0, 110);
	op_write_slru(SLRU_ZERO_OBJ, 2, pa, 0);
	check(op_nblocks_slru(SLRU_ZERO_OBJ) == 3,
		  "zero-version SLRU write grows its object fork");
	op_read_slru(SLRU_ZERO_OBJ, 2, rb);
	check(page_has_tag(rb, page_size, 110),
		  "zero-version SLRU object serves before restart");
	check(op_read_at_slru_status(SLRU_ZERO_OBJ, 2, 1, rb) == PS_STATUS_ERROR &&
		  page_all_zero(rb, page_size),
		  "finite as-of read rejects newest-only zero-version SLRU image");
	op_read_at_slru(SLRU_ZERO_OBJ, 2, UINT64_MAX, rb);
	check(page_has_tag(rb, page_size, 110),
		  "max-horizon read accepts newest-only zero-version SLRU image");

	/* --- WAL retention floor from mirrored pg_control notes ------------- */
	{
		unsigned char *note = calloc(1, page_size);
		unsigned char *image = calloc(1, page_size);
		uint64_t	redo1 = 5000,
					redo2 = 9000,
					exact_redo = 5000;

		memset(image, 0x5c, 64);	/* stand-in pg_control bytes */

		check(op_wal_retain_floor(0) == 0,
			  "wal retention floor starts unconstrained (no control image)");
		/* A checkpoint fence also publishes an exact-redo control copy.  It
		 * is separately restorable, so it must carry a note at that exact
		 * version rather than relying on the later record-end copy's note. */
		memcpy(note, &exact_redo, sizeof(exact_redo));
		op_write_control(1, note, exact_redo);
		op_write_control(0, image, exact_redo);
		check(op_wal_retain_floor(0) == exact_redo,
			  "exact-redo control image has a same-version floor note");
		/* two mirrored control writes, note first then image at the same
		 * version -- exactly the shipper's order; the floor must be the MIN
		 * redo over the restorable images */
		memcpy(note, &redo1, sizeof(redo1));
		op_write_control(1, note, 6000);
		op_write_control(0, image, 6000);
		memcpy(note, &redo2, sizeof(redo2));
		op_write_control(1, note, 9500);
		op_write_control(0, image, 9500);
		check(op_wal_retain_floor(0) == 5000,
			  "wal retention floor = min redo over noted control images");
		/* an image with NO covering note (pre-note-format store) collapses
		 * the floor to retain-everything, not an error and not min(notes) */
		op_write_control(0, image, 12000);
		check(op_wal_retain_floor(0) == 1,
			  "an unnoted control image collapses the floor to retain-all");
		memcpy(note, &redo2, sizeof(redo2));
		op_write_control(1, note, 12000);	/* backfill its note */
		check(op_wal_retain_floor(0) == 5000,
			  "a backfilled note restores the min-redo floor");
		free(image);
		free(note);
	}

	/* Crash exactly after a growing WAL-less page body reaches the segment but
	 * before its ordering marker commits the new SEG1 record.  Recovery must
	 * reject both bytes and growth.  The request process waits on the dead
	 * daemon, so run it in a disposable child. */
	op_create_at(REL_I, FORK0, 11000);
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon_crash_after_seg(daemon_path, shm, store, page_size,
									 test_nshards, 2);
	wait_ready(shm, page_size);
	{
		pid_t		writer = fork();
		int			status;

		if (writer == 0)
		{
			client_attach(shm, page_size);
			fill_page(pa, page_size, 0, 65);
			op_write_one(REL_I, FORK0, 0, pa);
			_exit(0);
		}
		if (writer < 0)
		{
			perror("fork crash-window writer");
			exit(2);
		}
		waitpid(dpid, &status, 0);
		check(WIFEXITED(status) && WEXITSTATUS(status) == 86,
			  "injected crash landed after the WAL-less segment body");
		kill(writer, SIGKILL);
		waitpid(writer, NULL, 0);
	}
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size, test_nshards);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);
	check(op_nblocks(REL_I, FORK0) == 0,
		  "uncommitted ordered record does not recover size after a crash");
	op_read_one(REL_I, FORK0, 0, rb);
	check(page_all_zero(rb, page_size),
		  "uncommitted ordered record does not recover page bytes");

	client_detach();

	/* --- crash recovery: restart daemon, rebuild index from segments --- */
	stop_daemon(dpid);
	/* Simulate the immediately preceding fork-event format: definitive records
	 * exist, but migration markers do not.  This must use normal replay, not
	 * legacy lsn-0 replay against the already-loaded truncate/unlink history. */
	check(strip_forkmeta_markers(store, 1, 1) == 0,
		  "synthesized a nonempty pre-marker fork-meta log");
	remove_lsm_metadata(store);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size, test_nshards);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);

	check(op_exists(REL_A, FORK0), "fork still exists after daemon restart");
	op_read_one(REL_A, FORK0, 5, rb);
	check(page_has_tag(rb, page_size, 6), "block 5 survives restart (recovered)");
	op_read_at(REL_A, FORK0, 0, 7000, rb);
	check(page_has_tag(rb, page_size, 100),
		  "COW history survives restart (read_at old version)");
	check(op_wal_retain_floor(0) == 5000,
		  "wal retention floor survives daemon restart (durable via segment log)");
	check(op_nblocks_slru(SLRU_ZERO_OBJ) == 3,
		  "zero-version SLRU growth survives daemon restart");
	op_read_slru(SLRU_ZERO_OBJ, 2, rb);
	check(page_has_tag(rb, page_size, 110),
		  "zero-version SLRU object survives daemon restart");
	check(op_read_at_slru_status(SLRU_ZERO_OBJ, 2, 1, rb) == PS_STATUS_ERROR &&
		  page_all_zero(rb, page_size),
		  "finite as-of read rejects recovered zero-version SLRU image");

	/* fork-size history survives: definitive events from the fork-meta log,
	 * growth re-derived from the segment records' own LSNs */
	check(op_nblocks_asof(REL_C, FORK0, 6999) == 5, "as-of size history survives restart");
	check(op_nblocks_asof(REL_C, FORK0, 7000) == 1, "truncate event survives restart");
	check(op_nblocks_asof(REL_C, FORK0, 8000) == 2, "post-truncate regrow survives restart");
	check(op_exists_asof(REL_C, FORK0, 8999), "pre-unlink existence survives restart");
	check(!op_exists_asof(REL_C, FORK0, 9500), "unlink event survives restart");
	check(op_nblocks(REL_C, FORK0) == 0, "recreated-empty newest size survives restart");

	/* WAL-less growth replays from its persisted clamped position, so a
	 * pre-truncate lsn-0 page cannot resurrect the fork size (the raw
	 * segment record is skipped; the fully preloaded meta log would have
	 * supplied a FUTURE floor) */
	check(op_nblocks(REL_E, FORK0) == 0,
		  "pre-marker forkmeta uses normal replay: truncated WAL-less fork stays empty");
	check(op_nblocks(REL_L, FORK0) == 0,
		  "same-LSN truncate remains after SEG0 growth on recovery");
	check(!op_exists(REL_M, FORK0),
		  "same-LSN unlink remains after SEG0 growth on recovery");
	check(op_nblocks(REL_N, FORK0) == 0,
		  "same-LSN truncate remains after clamped copied growth on recovery");
	check(!op_exists(REL_O, FORK0),
		  "same-LSN unlink remains after clamped copied growth on recovery");
	check(op_nblocks(REL_P, FORK0) == 0,
		  "same-LSN truncate remains after non-growing WAL-less recovery");
	check(!op_exists(REL_Q, FORK0),
		  "same-LSN unlink remains after non-growing clamped recovery");
	check(op_nblocks_asof(REL_E, FORK0, 1999) == 1,
		  "pre-truncate WAL-less growth keeps its floor position after restart");
	check(op_nblocks(REL_D, FORK0) == 1,
		  "post-truncate WAL-less regrow survives restart at its floor");
	check(op_nblocks(REL_F, FORK0) == 1,
		  "clamped below-floor growth survives restart (persisted event)");
	check(op_nblocks_asof(REL_F, FORK0, 4000) == 0,
		  "the raw below-create record is not re-derived: the gap stays invisible after restart");
	check(op_nblocks_asof(REL_F, FORK0, 5000) == 1,
		  "the clamped position (the create floor) serves after restart");
	check(op_read_at_found(REL_G, FORK0, 0, 2500, rb) &&
		  page_has_tag(rb, page_size, 62),
		  "retained-block raw LSN survives markerless recovery");
	check(op_nblocks_asof(REL_H, FORK0, 2500) == 3,
		  "markerless recovery preserves real pre-truncate growth");
	check(!op_read_at_found(REL_J, FORK0, 0, 4000, rb) &&
		  op_read_at_found(REL_J, FORK0, 0, 5000, rb) &&
		  page_has_tag(rb, page_size, 67),
		  "pre-extended CREATE keeps copied bytes clamped after recovery");
	check(op_nblocks_asof(REL_K, FORK0, 4000) == 0 &&
		  op_nblocks_asof(REL_K, FORK0, 5000) == 3,
		  "zeroextend persists only its clamped floor across recovery");
	op_read_one(REL_I, FORK0, 0, rb);
	check(page_all_zero(rb, page_size),
		  "uncommitted ordered record stays absent after another restart");

	/* --- unlink --- */
	op_unlink(REL_A, FORK0);
	check(!op_exists(REL_A, FORK0), "fork gone after unlink");

	/* --- legacy stores: an absent fork-meta log --- */
	/* a pre-events store has lsn-0 segment records and no fork-meta log at
	 * all; recovery must fall back to applying that growth verbatim (there
	 * are no definitive events to misorder against) or every unlogged fork
	 * would come back empty */
	client_detach();
	stop_daemon(dpid);
	{
		char		fmpath[512];

		snprintf(fmpath, sizeof(fmpath), "%s/forkmeta", store);
		unlink(fmpath);
	}
	remove_lsm_metadata(store);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size, test_nshards);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);
	check(op_nblocks(REL_D, FORK0) == 1,
		  "legacy mode restores WAL-less fork sizes from raw lsn-0 records");
	check(op_exists(REL_D, FORK0),
		  "legacy mode existence follows the raw growth");
	check(op_nblocks(REL_F, FORK0) == 1,
		  "legacy mode restores below-floor growth at its raw LSN");
	/* with the meta log gone the truncate is gone too: REL_E's growth
	 * reappears -- exactly the documented pre-events behavior */
	check(op_nblocks(REL_E, FORK0) == 1,
		  "legacy mode has no truncate events to order against (documented)");

	/* The start/done markers make an interrupted migration distinguishable
	 * from the pre-marker event format above.  Once sealed, migrated lsn-0
	 * growth must survive every later normal restart. */
	op_create_at(REL_F, FORK0, 20000);
	client_detach();
	stop_daemon(dpid);
	check(strip_forkmeta_markers(store, 0, 1) == 0,
		  "synthesized an interrupted migration with its start marker intact");
	remove_lsm_metadata(store);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size, test_nshards);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);
	check(op_nblocks(REL_D, FORK0) == 1,
		  "migrated legacy sizes survive a post-legacy restart");
	check(op_exists(REL_D, FORK0),
		  "interrupted legacy migration resumes without losing existence");

	/* The resumed scan seals the migration; the following boot is normal. */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, page_size, test_nshards);
	wait_ready(shm, page_size);
	client_attach(shm, page_size);
	check(op_nblocks(REL_D, FORK0) == 1,
		  "resumed migration seals and survives a normal restart");

	client_detach();
	stop_daemon(dpid);

	rm_rf(store);
	shm_unlink(shm);
	free(pa);
	free(pb);
	free(rb);
}

/* Durable, enumerable retention pins and the effective per-resource floor. */
static void
run_retention_suite(const char *daemon_path, const char *tmpbase)
{
	char		shm[64];
	char		store[256];
	char		path[512];
	pid_t		dpid;
	uint32_t	ps = 8192;
	uint64_t	floor = UINT64_MAX;
	uint32_t	count = 0;
	PsRetentionPin pin;
	unsigned char *note = calloc(1, ps);
	unsigned char *image = calloc(1, ps);
	uint64_t	redo = 800;
	struct stat before,
				after;
	int			fd;
	unsigned char byte;

	fprintf(stderr, "== retention registry ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_retention", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_retention", tmpbase);
	snprintf(path, sizeof(path), "%s/retention.meta", store);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 0,
		  "page retention starts unconstrained");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_WAL, &floor) ==
		  PS_STATUS_OK && floor == 0,
		  "WAL retention starts unconstrained");
	check(op_retention_set(99, PS_RETENTION_OWNER_READER, 1,
						   1, PS_RETENTION_RESOURCE_ALL, 1000) == PS_STATUS_ERROR,
		  "a pin cannot reference an undefined timeline");
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 1, 1, 0, 1000) ==
		  PS_STATUS_ERROR,
		  "a pin must name at least one known resource");
	check(op_retention_drop(0, PS_RETENTION_OWNER_READER, 0, 1) == PS_STATUS_ERROR,
		  "owner id zero cannot drop a pin");
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 101,
						   0, PS_RETENTION_RESOURCE_ALL, 1000) == PS_STATUS_ERROR &&
		  op_retention_drop(0, PS_RETENTION_OWNER_READER, 101, 0) ==
			PS_STATUS_ERROR,
		  "current retention IPC rejects reserved generation zero");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_ALL, &floor) ==
		  PS_STATUS_ERROR,
		  "a floor query names exactly one resource");

	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 101,
						   1, PS_RETENTION_RESOURCE_ALL, 5000) == PS_STATUS_OK,
		  "reader pin is durably registered");
	check(op_retention_set_fenced(0, PS_RETENTION_OWNER_MATERIALIZER, 202,
						   1,
						   PS_RETENTION_RESOURCE_WAL |
						   PS_RETENTION_RESOURCE_WAL_INDEX, 3000, 0x100000002ULL) ==
		  PS_STATUS_OK,
		  "materializer pin can retain WAL without page history");
	check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, 303,
						   1, PS_RETENTION_RESOURCE_PAGE_HISTORY, 4000) == PS_STATUS_OK,
		  "configured page-history pin is registered");
	check(op_retention_get(0, &pin, &count) && count == 3 &&
		  pin.timeline == 0 && pin.owner_kind == PS_RETENTION_OWNER_READER &&
		  pin.owner_id == 101 && pin.generation == 1 && pin.lsn == 5000,
		  "registry enumeration returns the first pin and total count");
	check(op_retention_get(2, &pin, &count) && count == 3 &&
		  pin.owner_kind == PS_RETENTION_OWNER_CONFIGURED && pin.owner_id == 303,
		  "registry enumeration returns every owner kind");
	check(op_retention_lookup(0, PS_RETENTION_OWNER_MATERIALIZER, 202, &pin) &&
		  pin.generation == 1 && pin.lsn == 3000 &&
		  pin.admission_seq == 0x100000002ULL,
		  "registry atomically looks up one owner by stable key");
	check(!op_retention_lookup(0, PS_RETENTION_OWNER_READER, 9999, &pin),
		  "keyed owner lookup reports an absent owner atomically");
	check(!op_retention_get(3, &pin, &count) && count == 3,
		  "registry enumeration ends at the reported count");
	{
		uint64_t	epoch = 0;
		int			found = 0;

		check(op_retention_get_consistent(0, &pin, &count, &epoch, &found) ==
			  PS_STATUS_OK && found && epoch != 0,
			  "registry enumeration returns a stable mutation epoch");
		check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, 304,
							   1, PS_RETENTION_RESOURCE_PAGE_HISTORY, 4500) ==
			  PS_STATUS_OK,
			  "a concurrent owner mutation changes the registry epoch");
		check(op_retention_get_consistent(1, &pin, &count, &epoch, &found) ==
			  PS_STATUS_STALE && !found && epoch == 0,
			  "enumeration detects a mutation and requires restart");
		check(op_retention_get_consistent(0, &pin, &count, &epoch, &found) ==
			  PS_STATUS_OK && found && epoch != 0,
			  "enumeration restarts successfully with its cleared epoch");
		check(op_retention_drop(0, PS_RETENTION_OWNER_CONFIGURED, 304, 1) ==
			  PS_STATUS_OK,
			  "temporary mutation owner is removed");
	}
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 4000,
		  "page floor is the minimum matching explicit pin");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_WAL, &floor) ==
		  PS_STATUS_OK && floor == 3000,
		  "WAL floor ignores page-only pins");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_WAL_INDEX, &floor) ==
		  PS_STATUS_OK && floor == 3000,
		  "WAL-index floor shares the resource registry");

	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 101,
						   1, PS_RETENTION_RESOURCE_ALL, 2000) == PS_STATUS_OK,
		  "SET atomically replaces the same owner generation");
	check(op_retention_get(0, &pin, &count) && pin.owner_id == 101,
		  "updated owner exposes its exact admission fence");
	check(stat(path, &before) == 0, "retention log exists after its first pin");
	check(op_retention_set_seq(0, PS_RETENTION_OWNER_READER, 101,
							   1, PS_RETENTION_RESOURCE_ALL, 2000,
							   pin.admission_seq) == PS_STATUS_OK &&
		  stat(path, &after) == 0 && before.st_size == after.st_size,
		  "an exact SET retry is idempotent without log churn");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 2000,
		  "an owner update immediately lowers the effective floor");
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 101,
						   2, PS_RETENTION_RESOURCE_ALL, 2500) == PS_STATUS_OK &&
		  op_retention_set(0, PS_RETENTION_OWNER_READER, 101,
						   1, PS_RETENTION_RESOURCE_ALL, 1000) == PS_STATUS_STALE,
		  "a takeover fences SET from an older owner generation");
	check(op_retention_drop(0, PS_RETENTION_OWNER_READER, 101, 1) ==
		  PS_STATUS_STALE,
		  "an older owner generation cannot drop its replacement's pin");
	check(op_retention_drop(0, PS_RETENTION_OWNER_READER, 101, 2) == PS_STATUS_OK &&
		  op_retention_drop(0, PS_RETENTION_OWNER_READER, 101, 2) == PS_STATUS_OK,
		  "DROP is durable and idempotent");
	check(op_retention_set(0, PS_RETENTION_OWNER_READER, 101,
						   2, PS_RETENTION_RESOURCE_ALL, 2500) == PS_STATUS_STALE,
		  "a released generation cannot resurrect its pin");

	op_create_branch(1, 0, 1500);
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 4000,
		  "a child fork point does not lower the operational page floor");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_WAL_INDEX, &floor) ==
		  PS_STATUS_OK && floor == 1500,
		  "a child fork point structurally pins the parent WAL index");
	check(op_retention_set(1, PS_RETENTION_OWNER_READER, 404,
						   1, PS_RETENTION_RESOURCE_ALL, 1000) == PS_STATUS_OK,
		  "a descendant reader pin is registered on its own timeline");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 1000,
		  "a descendant pin projects through branch ancestry to the parent");
	check(op_retention_floor(1, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 1000,
		  "a descendant pin directly constrains its own timeline");
	op_create_branch(2, 1, 900);
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 1000,
		  "a nested branch remains a discrete root base requirement");
	check(op_retention_floor(1, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 1000,
		  "a nested branch remains a discrete direct-parent base requirement");

	/* A restorable root control image is another WAL-only authority, and its
	 * version is below the fork so it constrains both timelines. */
	memcpy(note, &redo, sizeof(redo));
	memset(image, 0x5c, 64);
	op_write_control(1, note, 1400);
	op_write_control(0, image, 1400);
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_WAL, &floor) ==
		  PS_STATUS_OK && floor == redo,
		  "restorable control images participate in the unified WAL floor");
	check(op_retention_floor(1, PS_RETENTION_RESOURCE_WAL, &floor) ==
		  PS_STATUS_OK && floor == redo,
		  "control WAL requirements follow branch visibility");

	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_retention_get(2, &pin, &count) && count == 3 &&
		  pin.timeline == 1 && pin.owner_id == 404,
		  "retention pins survive daemon restart and remain enumerable");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 1000,
		  "projected effective floor survives restart");
	check(op_retention_drop(1, PS_RETENTION_OWNER_READER, 404, 1) == PS_STATUS_OK,
		  "a restarted controller can release its durable pin");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 4000,
		  "dropping the reader restores the configured operational floor");
	for (uint64_t owner = 1000; owner < 2025; owner++)
		check(op_retention_set(0, PS_RETENTION_OWNER_CONFIGURED, owner,
						   1, PS_RETENTION_RESOURCE_PAGE_HISTORY, 6000 + owner) ==
			  PS_STATUS_OK, "registry admits more owners than timelines");
	check(op_retention_floor(0, PS_RETENTION_RESOURCE_PAGE_HISTORY, &floor) ==
		  PS_STATUS_OK && floor == 4000,
		  "floor snapshot covers every owner beyond MAX_TIMELINES");
	client_detach();
	stop_daemon(dpid);

	/* Only an incomplete final record is recoverable: it cannot be committed,
	 * and truncating a partial DROP conservatively keeps the old pin. */
	fd = open(path, O_WRONLY | O_APPEND);
	check(fd >= 0 && write(fd, "partial", 7) == 7 && fsync(fd) == 0,
		  "synthesized a short retention-log tail");
	if (fd >= 0)
		close(fd);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_retention_get(1, &pin, &count) && count == 1027,
		  "restart truncates a short tail without losing committed pins");
	client_detach();
	stop_daemon(dpid);

	/* A complete corrupt record could have been the last live SET.  Refuse the
	 * store rather than guessing that it was an ignorable torn tail. */
	fd = open(path, O_RDWR);
	check(fd >= 0 && pread(fd, &byte, 1, 0) == 1,
		  "opened a complete retention record for corruption testing");
	if (fd >= 0)
	{
		byte ^= 0x80;
		check(pwrite(fd, &byte, 1, 0) == 1 && fsync(fd) == 0,
			  "corrupted one byte in a complete retention record");
		close(fd);
	}
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	expect_daemon_open_failure(dpid, shm,
						   "complete retention-record corruption fails closed");

	rm_rf(store);
	shm_unlink(shm);
	free(note);
	free(image);
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

	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
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

	/* fork size is versioned too: growth after the branch point stays the
	 * parent's own -- the branch's size walk caps at its fork LSN */
	fill_page(p, ps, 3000, 53);
	op_write_tl(0, REL_B, FORK0, 7, p);		/* block7 @ lsn 3000 (> branch) */
	check(op_nblocks_tl(0, REL_B, FORK0) == 8, "parent size includes post-branch growth");
	check(op_nblocks_tl(1, REL_B, FORK0) == 6, "branch size capped at its fork point");

	/* A branch has no local CREATE event.  Its first definitive event may be a
	 * later truncate; recovery must retain real branch-local growth before it. */
	op_create_branch(3, 0, 5000);
	fill_page(p, ps, 6000, 54);
	op_write_tl(3, REL_B, FORK0, 9, p);
	op_truncate_at_tl(3, REL_B, FORK0, 2, 7000);
	check(op_nblocks_asof_tl(3, REL_B, FORK0, 6500) == 10,
		  "branch-local growth is visible before its first truncate");
	check(op_nblocks_tl(3, REL_B, FORK0) == 2,
		  "branch truncate remains definitive at newest");

	/* A later regrowth and less-severe shrink cannot erase the older hole in
	 * inherited storage: block 7 was in the parent at the branch point, but the
	 * truncate to 2 permanently fenced those bytes on this timeline. */
	fill_page(p, ps, 8000, 58);
	op_write_tl(3, REL_B, FORK0, 9, p);
	op_truncate_at_tl(3, REL_B, FORK0, 8, 9000);
	check(!op_read_at_tl_found(3, REL_B, FORK0, 7, 9500, rb),
		  "older branch truncate still fences parent bytes after regrowth");

	/* Arrival order is not version order.  A delayed, older truncate must not
	 * invalidate a page whose LSN is newer, even though its admission sequence
	 * was allocated first. */
	fill_page(p, ps, 10000, 59);
	op_write_tl(3, REL_B, FORK0, 6, p);
	op_truncate_at_tl(3, REL_B, FORK0, 2, 9500);
	check(op_read_at_tl_found(3, REL_B, FORK0, 6, 11000, rb) &&
		  page_has_tag(rb, ps, 59),
		  "delayed older truncate does not invalidate a newer page");
	{
		uint32_t current_nblocks = op_nblocks_tl(3, REL_B, FORK0);

		check(current_nblocks == 7,
			  "delayed older truncate reconstructs growth from the newer page (got %u)",
			  current_nblocks);
	}

	/* Kept event-free locally for the failed WAL-less write test below. */
	op_create_branch(4, 0, 5000);
	check(op_nblocks_tl(4, REL_B, FORK0) == 8,
		  "event-free branch inherits the parent fork size");
	op_create_branch(6, 0, 5000);
	check(op_nblocks_tl(6, REL_B, FORK0) == 8,
		  "second event-free branch inherits the parent fork size");

	/* A copied branch-local page keeps the parent's old source pd_lsn, but the
	 * write happened after the branch snapshot.  Stamp it just above the branch
	 * point so an as-of read AT the fork still inherits the parent image. */
	op_create_branch(5, 0, 5000);
	fill_page(p, ps, 1000, 56);
	op_write_tl(5, REL_B, FORK0, 0, p);
	op_read_at_tl(5, REL_B, FORK0, 0, 5000, rb);
	check(page_has_tag(rb, ps, 11),
		  "branch-point read hides a later copied-page rewrite");
	op_read_tl(5, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 56),
		  "newest branch read sees its copied-page rewrite");

	/* A crash can leave part of the next append at EOF.  Recovery preserves
	 * every complete branch record and truncates the tail before new appends. */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	{
		off_t		committed_size = append_torn_timeline_tail(store);
		char		path[512];
		struct stat st;

		dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
		wait_ready(shm, ps);
		snprintf(path, sizeof(path), "%s/timelines", store);
		check(committed_size >= 0 && stat(path, &st) == 0 &&
			  st.st_size == committed_size,
			  "restart truncates an incomplete timeline metadata record");
	}
	client_attach(shm, ps);
	op_read_tl(1, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 22), "branch write survives daemon restart");
	op_read_tl(1, REL_B, FORK0, 5, rb);
	check(page_has_tag(rb, ps, 51), "branch snapshot view survives restart");
	check(op_nblocks_asof_tl(3, REL_B, FORK0, 6500) == 10,
		  "pre-truncate branch-local growth survives restart");
	{
		uint32_t recovered_nblocks = op_nblocks_tl(3, REL_B, FORK0);

		check(recovered_nblocks == 7,
			  "reconstructed post-truncate growth survives restart (got %u)",
			  recovered_nblocks);
	}
	check(op_read_at_tl_found(3, REL_B, FORK0, 6, 11000, rb) &&
		  page_has_tag(rb, ps, 59),
		  "newer page remains readable after delayed-truncate recovery");
	op_read_at_tl(5, REL_B, FORK0, 0, 5000, rb);
	check(page_has_tag(rb, ps, 11),
		  "branch-point copied-page floor survives restart");
	op_read_tl(5, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 56),
		  "branch copied-page rewrite survives restart above its floor");

	/* Force the segment write to fail before its ordering marker.  No local
	 * growth may appear or mask the inherited size (8) on the next recovery. */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon_fail_seg(daemon_path, shm, store, ps, test_nshards, 1);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	fill_page(p, ps, 0, 55);
	check(op_write_tl_status(4, REL_B, FORK0, 8, p) == PS_STATUS_ERROR,
		  "injected segment failure rejects the WAL-less branch write");
	check(op_create_branch_status(4, 0, 5000) == PS_STATUS_OK,
		  "failed first branch write leaves CREATE_BRANCH retry idempotent");
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_nblocks_tl(4, REL_B, FORK0) == 8,
		  "failed segment write preserves the inherited fork size");
	op_read_tl(4, REL_B, FORK0, 0, rb);
	check(page_has_tag(rb, ps, 11),
		  "failed segment write does not mask inherited parent pages");

	/* Leave a complete first branch-local record without its commit marker.
	 * Recovery must discard it without marking the timeline used, so an exact
	 * retry of the still-event-free branch definition remains idempotent. */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon_fail_fork_meta(daemon_path, shm, store, ps,
								   test_nshards, 1);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	fill_page(p, ps, 0, 57);
	check(op_write_tl_status(6, REL_B, FORK0, 8, p) == PS_STATUS_ERROR,
		  "missing marker rejects the first branch-local ordered record");
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_create_branch_status(6, 0, 5000) == PS_STATUS_OK,
		  "discarded ordered record leaves CREATE_BRANCH retry idempotent");
	check(op_nblocks_tl(6, REL_B, FORK0) == 8,
		  "discarded ordered record preserves inherited branch size");

	/* CREATE_BRANCH validation: reject requests that would corrupt the parent
	 * walk (timelines 0,1,2 are defined here) */
	check(op_check_branch_status(1, 0, 1500) == PS_STATUS_ERROR,
		  "CHECK_BRANCH rejects retry of a timeline with branch-local writes");
	check(op_check_branch_status(1, 0, 1501) == PS_STATUS_ERROR,
		  "CHECK_BRANCH rejects mismatched ancestry for existing timeline");
	check(op_check_branch_status(9, 0, 1500) == PS_STATUS_OK,
		  "CHECK_BRANCH accepts a valid not-yet-created branch request");
	check(op_require_branch_status(1, 0, 1500) == PS_STATUS_OK,
		  "REQUIRE_BRANCH accepts existing matching timeline metadata");
	check(op_require_branch_status(1, 0, 1501) == PS_STATUS_ERROR,
		  "REQUIRE_BRANCH rejects mismatched existing timeline metadata");
	check(op_require_branch_status(9, 0, 1500) == PS_STATUS_ERROR,
		  "REQUIRE_BRANCH rejects not-yet-created branch timelines");
	check(op_create_branch_status(1, 0, 1501) == PS_STATUS_ERROR,
		  "CREATE_BRANCH rejects re-creating existing timeline with mismatched ancestry");
	check(op_create_branch_status(1, 0, 1500) == PS_STATUS_ERROR,
		  "re-create of a written timeline id is rejected (would expose its pages)");
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

	/* the idempotent-retry window: an unused timeline may be re-created (a
	 * prepare retry), but the first branch-local write closes it -- reads on
	 * the timeline do not */
	check(op_check_branch_status(7, 2, 1500) == PS_STATUS_OK,
		  "CHECK_BRANCH accepts retry of an unused timeline after reads");
	check(op_create_branch_status(7, 2, 1500) == PS_STATUS_OK,
		  "re-create of an unused timeline with identical ancestry is idempotent");
	fill_page(p, ps, 4000, 77);
	op_write_tl(7, REL_B, FORK0, 0, p);
	check(op_create_branch_status(7, 2, 1500) == PS_STATUS_ERROR,
		  "the timeline's first write closes the idempotent-retry window");

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
	unsigned char rback[1024];
	int			ok;

	fprintf(stderr, "== shipped WAL ==\n");
	memset(bufa, 0xAA, sizeof(bufa));	/* WAL [1000,1500) */
	memset(bufb, 0xBB, sizeof(bufb));	/* WAL [1500,2000) */

	snprintf(shm, sizeof(shm), "/pstest_%d_wal", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_wal", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
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

	/* overlap policy: identical bytes re-ship idempotently; divergent
	 * bytes for a covered range are two histories claiming the same LSNs
	 * (a divergent compute) and are refused */
	check(op_wal_append_status(0, 1000, bufa, 500) == PS_STATUS_OK,
		  "an identical re-ship of covered WAL is accepted (idempotent retry)");
	check(op_wal_size(0) == 2000, "the re-ship does not move the end LSN");
	check(op_wal_append_status(0, 1200, bufb, 100) != PS_STATUS_OK,
		  "divergent bytes for an already-covered WAL range are refused");
	memset(rback, 0, sizeof(rback));
	check(op_wal_read(0, 1200, 100, rback) == 100 && rback[0] == 0xAA,
		  "the recorded history is intact after the refused overwrite");

	/* a branch keeps its own WAL log */
	op_create_branch(1, 0, 2000);
	op_wal_append(1, 2000, bufa, 300);
	check(op_wal_size(1) == 2300, "branch WAL advances independently");
	check(op_wal_size(0) == 2000, "parent WAL unaffected by branch WAL");

	/* read-through: the branch's history below its fork point is the
	 * parent's, and a read spanning the fork stitches the two logs */
	memset(rback, 0, sizeof(rback));
	check(op_wal_read(1, 1400, 200, rback) == 200,
		  "branch read below the fork serves the parent's bytes");
	ok = 1;
	for (int i = 0; i < 100; i++)
		if (rback[i] != 0xAA)
			ok = 0;
	for (int i = 100; i < 200; i++)
		if (rback[i] != 0xBB)
			ok = 0;
	check(ok, "branch read below the fork carries the parent's contents");
	memset(rback, 0, sizeof(rback));
	check(op_wal_read(1, 1900, 200, rback) == 200,
		  "branch read across the fork stitches parent and own bytes");
	ok = 1;
	for (int i = 0; i < 100; i++)	/* [1900,2000): parent's 0xBB record */
		if (rback[i] != 0xBB)
			ok = 0;
	for (int i = 100; i < 200; i++) /* [2000,2100): the branch's own 0xAA */
		if (rback[i] != 0xAA)
			ok = 0;
	check(ok, "fork-spanning read assembles parent then branch bytes");
	check(op_wal_read(1, 1400, 700, rback) == 700,
		  "fork-spanning read counts every distinct byte exactly once");

	/* a branch whose OWN first shipped segment spans its fork carries a
	 * valid pre-fork prefix the parent may never ship (the parent's copy
	 * of that segment can still be partial): the prefix must serve from
	 * the branch's own log, and holes below it must not be miscounted */
	op_create_branch(2, 0, 2500);
	op_wal_append(2, 2400, bufb, 200);	/* spans the fork at 2500 */
	memset(rback, 0, sizeof(rback));
	check(op_wal_read(2, 2400, 200, rback) == 200,
		  "the branch's own pre-fork prefix serves when the parent lacks those bytes");
	ok = 1;
	for (int i = 0; i < 200; i++)
		if (rback[i] != 0xBB)
			ok = 0;
	check(ok, "own-prefix bytes come from the branch's log");
	check(op_wal_read(2, 2300, 300, rback) == 200,
		  "a hole below the branch's own coverage is not double-counted");

	/* a retry that carries an already-accepted prefix plus new bytes (a
	 * partially shipped chunk re-sent whole) appends only the uncovered
	 * suffix -- no duplicate chunk, exact distinct-byte counts */
	{
		unsigned char bufc[200];

		memset(bufc, 0xCC, 100);
		op_wal_append(0, 3000, bufc, 100);	/* fresh chunk [3000,3100) */
		memset(bufc + 100, 0xDD, 100);		/* retry whole: CC prefix + DD tail */
		check(op_wal_append_status(0, 3000, bufc, 200) == PS_STATUS_OK,
			  "a partial re-ship with new suffix bytes is accepted");
		check(op_wal_size(0) == 3200,
			  "the suffix advances the end LSN");
		memset(rback, 0, sizeof(rback));
		check(op_wal_read(0, 3000, 200, rback) == 200,
			  "the trimmed re-ship leaves exact distinct-byte counts");
		ok = 1;
		for (int i = 0; i < 100; i++)
			if (rback[i] != 0xCC)
				ok = 0;
		for (int i = 100; i < 200; i++)
			if (rback[i] != 0xDD)
				ok = 0;
		check(ok, "prefix bytes stay the originals; suffix bytes are the new ones");
	}

	/* WAL end LSNs survive a daemon restart (recovered from the logs) */
	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_wal_size(0) == 3200, "main WAL end LSN survives daemon restart");
	check(op_wal_size(1) == 2300, "branch WAL end LSN survives daemon restart");
	check(op_create_branch_status(1, 0, 2000) == PS_STATUS_ERROR,
		  "shipped WAL also closes the idempotent re-create window (post-restart)");

	client_detach();
	stop_daemon(dpid);
	write_short_wal_payload(store, 4, 4000, bufa, 500, 100);
	shm_unlink(shm);
	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	check(op_wal_size(4) == 0,
		  "WAL recovery ignores and truncates a short-payload suffix");
	check(op_wal_append_status(4, 4000, bufa, 500) == PS_STATUS_OK &&
		  op_wal_size(4) == 4500,
		  "archiver retry succeeds after short-payload WAL recovery");

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
	uint32_t	walidx_nshards = test_nshards < 2 ? 4 : test_nshards;
	uint32_t	nonzero_shard = walidx_nshards > 1 ? 1 : 0;
	uint32_t	nonzero_rel = walidx_nshards > 1 ?
		find_relation_on_shard(nonzero_shard, walidx_nshards) : 0;
	PsWalRec	out[16];
	unsigned char wal[512] = {0};
	int		n;

	fprintf(stderr, "== per-page WAL index ==\n");
	snprintf(shm, sizeof(shm), "/pstest_%d_widx", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_widx", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps, walidx_nshards);
	wait_ready(shm, ps);
	client_attach(shm, ps);
	op_wal_append(0, 0, wal, sizeof(wal));

	/* block 0 changed by records at LSN 100, 200, 300; block 1 at 150 */
	op_walidx_add(0, REL_A, FORK0, 0, 100);
	op_walidx_add(0, REL_A, FORK0, 0, 200);
	op_walidx_add(0, REL_A, FORK0, 0, 300);
	op_walidx_add(0, REL_A, FORK0, 1, 150);
	{
		const uint32_t blocks[] = {7, 8, 7};
		const uint64_t lsns[] = {175, 225, 275};

		op_walidx_add_batch(0, REL_A, FORK0, blocks, lsns, 3);
	}
	check(nonzero_rel != 0, "test harness can find a nonzero WAL-index shard relation");
	if (nonzero_rel != 0)
	{
		op_walidx_add(0, nonzero_rel, FORK0, 0, 320);
		 op_walidx_add(0, nonzero_rel, FORK0, 0, 330);
	}
	check(op_walidx_get(0, REL_A, FORK0, 0, 1000000, out) == 0,
		  "WAL index entries stay hidden before their progress commit");
	{
		uint64_t	progress;

		check(op_walidx_progress(0, 0, 350, &progress) == 0,
			  "contiguous indexed WAL interval commits a durable progress marker");
	}

	n = op_walidx_get(0, REL_A, FORK0, 0, 250, out);
	check(n == 2 && out[0].lsn == 100 && out[1].lsn == 200,
		  "index returns records <= lsn (block 0 as-of 250 -> [100,200])");
	n = op_walidx_get_after(0, REL_A, FORK0, 0, 1000000, 0, 0, 0, 1, out);
	check(n == 1 && out[0].lsn == 100,
		  "index query honors a caller-requested page size");
	n = op_walidx_get_after(0, REL_A, FORK0, 0, 1000000, 1, 100, 0, 0, out);
	check(n == 2 && out[0].lsn == 200 && out[1].lsn == 300,
		  "index cursor returns only records after the (LSN,timeline) boundary");
	n = op_walidx_get(0, REL_A, FORK0, 0, 1000000, out);
	check(n == 3 && out[2].lsn == 300, "index returns all records up to a high lsn");
	check(out[0].timeline == 0 && out[2].timeline == 0,
		  "records on the root timeline are tagged timeline 0");
	check(op_walidx_get(0, REL_A, FORK0, 0, 50, out) == 0, "no records below the first lsn");
	n = op_walidx_get(0, REL_A, FORK0, 1, 200, out);
	check(n == 1 && out[0].lsn == 150, "per-block separation (block 1 -> [150])");
	check(op_walidx_get(0, REL_A, FORK0, 9, 1000000, out) == 0, "unindexed block -> empty");
	n = op_walidx_get(0, REL_A, FORK0, 7, 1000000, out);
	check(n == 2 && out[0].lsn == 175 && out[1].lsn == 275,
		  "batched WAL-index entries are queryable in LSN order");
	{
		uint64_t	progress;
		uint64_t	high = 0x100000000ULL;

		check(op_walidx_progress(0, 0, 0, &progress) == 0 && progress == 350,
			  "WAL index progress reports the committed end");
		check(op_walidx_progress(UINT32_MAX, 0, 0, &progress) != 0,
			  "WAL index progress rejects an out-of-range timeline");
		op_wal_append(0, high, wal, 16);
		check(op_walidx_progress(0, 350, high + 16, &progress) != 0,
			  "WAL index progress rejects a marker crossing unshipped WAL");
		check(op_walidx_progress(0, 0, 0, &progress) == 0 && progress == 350,
			  "rejected WAL index progress leaves the committed end unchanged");
		op_create_branch(2, 0, 0);
		op_wal_append(2, high, wal, 16);
		check(op_walidx_progress(2, 0, 0, &progress) == 0 &&
			  progress == high,
			  "first WAL append publishes the initial indexing boundary");
		check(op_walidx_progress(2, high, high + 16, &progress) == 0,
			  "WAL index progress accepts a 64-bit LSN start");
		check(op_walidx_progress(2, 0, 0, &progress) == 0 &&
			  progress == high + 16,
			  "WAL index progress reports a 64-bit committed end");
		op_create_branch(3, 0, 0);
		op_create_branch(4, 0, 0);
		op_wal_append(4, high + 32, wal, 16);
	}

	/* The index is durable independently of the daemon's in-memory hash tables. */
	client_detach();
	stop_daemon(dpid);
	write_torn_wal_header(store, 3, 0x100000000ULL, 16);
	/* Do not let wait_ready observe the stopped daemon's valid SHM header. */
	shm_unlink(shm);
	check(setenv("PAGESTORE_TEST_MAX_LOG_READ", "17", 1) == 0,
		  "enable short log reads during WAL index recovery");
	dpid = spawn_daemon(daemon_path, shm, store, ps, walidx_nshards);
	unsetenv("PAGESTORE_TEST_MAX_LOG_READ");
	wait_ready(shm, ps);
	client_attach(shm, ps);
	n = op_walidx_get(0, REL_A, FORK0, 0, 1000000, out);
	check(n == 3 && out[0].lsn == 100 && out[2].lsn == 300,
		  "WAL index survives daemon restart");
	n = op_walidx_get(0, REL_A, FORK0, 1, 1000000, out);
	check(n == 1 && out[0].lsn == 150,
		  "restart replays WAL index records without duplicating them");
	n = op_walidx_get(0, REL_A, FORK0, 7, 1000000, out);
	check(n == 2 && out[0].lsn == 175 && out[1].lsn == 275,
		  "batched WAL-index entries survive daemon restart");
	{
		uint64_t	progress;
		uint64_t	high = 0x100000000ULL;

		check(op_walidx_progress(0, 0, 0, &progress) == 0 && progress == 350,
			  "WAL index progress survives daemon restart");
		check(op_walidx_progress(2, 0, 0, &progress) == 0 &&
			  progress == high + 16,
			  "64-bit WAL index progress survives daemon restart");
		check(op_walidx_progress(3, high, high + 16, &progress) != 0,
			  "WAL index progress rejects WAL with a missing payload");
		check(op_walidx_progress(4, 0, 0, &progress) == 0 &&
			  progress == high + 32,
			  "initial indexing boundary is recovered from durable WAL");
	}

	/* a branch sees its own records plus the parent's, capped at the fork LSN */
	op_create_branch(1, 0, 250);
	{
		uint32_t	parent_tl = UINT32_MAX;
		uint64_t	branch_lsn = 0;

		check(!op_timeline_info(0, &parent_tl, &branch_lsn),
			  "root timeline reports no parent");
		check(op_timeline_info(1, &parent_tl, &branch_lsn) &&
			  parent_tl == 0 && branch_lsn == 250,
			  "branch timeline reports its parent and fork LSN");
	}
	op_wal_append(1, 400, wal, 16);
	op_walidx_add(1, REL_A, FORK0, 0, 400);
	check(op_walidx_progress(1, 400, 416, NULL) == 0,
		  "branch WAL index entries become visible with branch progress");
	n = op_walidx_get(1, REL_A, FORK0, 0, 1000000, out);
	/* branch's 400, plus parent's <= branch_lsn 250 (100,200; not 300) */
	check(n == 3, "branch index reads through to parent capped at the branch lsn");
	/* merged in ascending LSN order across the ancestry */
	check(out[0].lsn == 100 && out[1].lsn == 200 && out[2].lsn == 400,
		  "branch records are merged in ascending LSN order");
	/* each record is tagged with the timeline it lives on: parent's on 0, branch's on 1 */
	check(out[0].timeline == 0 && out[1].timeline == 0 && out[2].timeline == 1,
		  "each record carries its source timeline tag (parent=0, branch=1)");

	client_detach();
	stop_daemon(dpid);
	shm_unlink(shm);
	if (nonzero_rel != 0)
	{
		char		path[512];
		struct stat st;

		snprintf(path, sizeof(path), "%s/walidx_0_%u", store, nonzero_shard);
		check(stat(path, &st) == 0 && st.st_size > 1 &&
			  truncate(path, st.st_size / 2) == 0,
			  "truncate a committed nonzero WAL-index shard");
		dpid = spawn_daemon(daemon_path, shm, store, ps, walidx_nshards);
		expect_daemon_open_failure(dpid, shm,
								   "truncated committed nonzero WAL-index shard fails closed");
	}
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

	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
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

	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
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
 * Stress the per-shard + map locking.  Writers hammer their own rels (distinct
 * shards) hard enough to trigger repeated memtable flushes and compactions (each
 * of which takes map_wr), while readers continuously re-read a write-once shared
 * dataset (taking shard_rd + map_rd).  The shared dataset is never rewritten, so
 * a reader must ALWAYS see the correct page; any mismatch means a concurrent map
 * mutation corrupted a read -- i.e. the map lock is wrong.  Run with nshards>1
 * (the test's optional argv[2]) to exercise true cross-shard concurrency.
 */
static int
stress_writer_child(const char *shm_name, uint32_t ps, int id)
{
	uint32_t	rel = 30000 + (uint32_t) id;
	unsigned char *p = malloc(ps);

	client_attach(shm_name, ps);
	/* 3 passes x 200 blocks: flush-pages=8 -> ~75 flushes -> many compactions */
	for (int pass = 0; pass < 3; pass++)
		for (uint32_t b = 0; b < 200; b++)
		{
			fill_page(p, ps, 7000 + b, (unsigned char) (id * 11 + pass + b + 1));
			op_write_one(rel, FORK0, b, p);
		}
	client_detach();
	free(p);
	return 0;
}

static int
stress_reader_child(const char *shm_name, uint32_t ps, uint32_t rel,
					uint32_t nblocks, unsigned char tagbase)
{
	unsigned char *r = malloc(ps);
	int			rc = 0;

	client_attach(shm_name, ps);
	for (int iter = 0; iter < 150 && rc == 0; iter++)
		for (uint32_t b = 0; b < nblocks; b++)
		{
			op_read_one(rel, FORK0, b, r);
			if (!page_has_tag(r, ps, (unsigned char) (tagbase + b)))
				rc = 2;				/* concurrent map mutation corrupted a read */
		}
	client_detach();
	free(r);
	return rc;
}

static int
stress_populate_child(const char *shm_name, uint32_t ps, uint32_t rel,
					  uint32_t nblocks, unsigned char tagbase)
{
	unsigned char *p = malloc(ps);

	client_attach(shm_name, ps);
	for (uint32_t b = 0; b < nblocks; b++)
	{
		fill_page(p, ps, 5000 + b, (unsigned char) (tagbase + b));
		op_write_one(rel, FORK0, b, p);
	}
	client_detach();
	free(p);
	return 0;
}

static void
run_stress_suite(const char *daemon_path, const char *tmpbase)
{
#define NWRITERS 8
#define NREADERS 8
	char		shm[64];
	char		store[256];
	pid_t		dpid;
	pid_t		pop;
	pid_t		kids[NWRITERS + NREADERS];
	int			st = 1;
	uint32_t	ps = 8192;
	uint32_t	shared_rel = 29999;
	uint32_t	shared_n = 64;
	unsigned char tagbase = 100;

	fprintf(stderr, "== stress (%d writers + %d readers, nshards=%u) ==\n",
			NWRITERS, NREADERS, test_nshards);
	snprintf(shm, sizeof(shm), "/pstest_%d_stress", (int) getpid());
	snprintf(store, sizeof(store), "%s/store_stress", tmpbase);
	rm_rf(store);
	shm_unlink(shm);

	dpid = spawn_daemon(daemon_path, shm, store, ps, test_nshards);
	wait_ready(shm, ps);

	/* populate the write-once shared dataset before the concurrent phase */
	pop = fork();
	if (pop == 0)
		_exit(stress_populate_child(shm, ps, shared_rel, shared_n, tagbase));
	waitpid(pop, &st, 0);
	check(WIFEXITED(st) && WEXITSTATUS(st) == 0, "stress: shared dataset populated");

	for (int i = 0; i < NWRITERS; i++)
	{
		pid_t		pid = fork();

		if (pid == 0)
			_exit(stress_writer_child(shm, ps, i));
		kids[i] = pid;
	}
	for (int i = 0; i < NREADERS; i++)
	{
		pid_t		pid = fork();

		if (pid == 0)
			_exit(stress_reader_child(shm, ps, shared_rel, shared_n, tagbase));
		kids[NWRITERS + i] = pid;
	}
	for (int i = 0; i < NWRITERS + NREADERS; i++)
	{
		int			s = 1;

		waitpid(kids[i], &s, 0);
		check(WIFEXITED(s) && WEXITSTATUS(s) == 0,
			  "stress: client %d finished clean (no deadlock, no map corruption)", i);
	}

	stop_daemon(dpid);
	rm_rf(store);
	shm_unlink(shm);
#undef NWRITERS
#undef NREADERS
}

int
main(int argc, char **argv)
{
	const char *daemon_path;
	char		tmpl[] = "/tmp/pstestXXXXXX";
	char	   *tmpbase;
	uint32_t	sizes[] = {4096, 8192, 16384};

	if (argc < 3)
	{
		fprintf(stderr, "usage: %s <path-to-pagestore_daemon> "
				"<path-to-pagestore_inspect> [nshards]\n", argv[0]);
		return 2;
	}
	daemon_path = argv[1];
	inspect_path = argv[2];
	if (argc > 3)
		test_nshards = (uint32_t) strtoul(argv[3], NULL, 10);
	if (test_nshards == 0)
		test_nshards = 1;
	if (test_nshards > PS_MAX_CHANNELS)
		test_nshards = PS_MAX_CHANNELS;

	tmpbase = mkdtemp(tmpl);
	if (!tmpbase)
	{
		perror("mkdtemp");
		return 2;
	}

	/* Legacy migration must seal before the daemon publishes readiness. */
	run_migration_failure_suite(daemon_path, tmpbase);
	/* A failed ordering-marker append must not commit segment bytes. */
	run_order_marker_failure_suite(daemon_path, tmpbase);
	/* Transitional SEG0 records may duplicate already-persisted growth. */
	run_markerless_seg0_dedup_suite(daemon_path, tmpbase);
	/* Layer watermarks permit complete, covered POSIX segments to be removed. */
	run_segment_gc_suite(daemon_path, tmpbase);
	/* Descendant owners and nested fork caps constrain parent page pruning. */
	run_prune_branch_retention_suite(daemon_path, tmpbase);
	/* Fork lifecycle metadata must continue to gate pruned page generations. */
	run_prune_relation_lifecycle_suite(daemon_path, tmpbase);
	/* Compaction publication and old-layer deletion are crash-restartable. */
	run_prune_publication_recovery_suite(daemon_path, tmpbase);
	/* Repeated updates and compactions must bound retained page history. */
	run_prune_bounded_churn_suite(daemon_path, tmpbase);
	/* Recovery must not reuse a sealed layer file absent from the manifest. */
	run_orphan_layer_suite(daemon_path, tmpbase);
	/* Legacy physical shard 0 can feed page indexes on every new logical shard. */
	run_reshard_segment_gc_suite(daemon_path, tmpbase);
	/* Stores already using more than one shard must keep their shard count. */
	run_shard_count_change_rejection_suite(daemon_path, tmpbase);
	run_legacy_walidx_reshard_suite(daemon_path, tmpbase);

	/* run the whole suite once per page size: proves page-size independence */
	for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++)
		run_suite(daemon_path, tmpbase, sizes[i]);

	/* durable pins and structural/effective retention floors */
	run_retention_suite(daemon_path, tmpbase);

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

	/* stress the per-shard + map locking under sustained flush/compaction load */
	run_stress_suite(daemon_path, tmpbase);

	rmdir(tmpbase);

	fprintf(stderr, "\n%d checks, %d failed\n", tests_run, tests_failed);
	return tests_failed ? 1 : 0;
}
