/*-------------------------------------------------------------------------
 *
 * pagestore_test_client.h
 *	  Shared static-inline IPC test-client code for the POSIX pagestore
 *	  daemon, factored out of pagestore_soak_test.c so other freestanding
 *	  test clients (the op-sequence fuzzer, and future ones) do not have to
 *	  re-derive the daemon lifecycle and channel protocol from scratch.
 *
 * This is a literal extraction/adaptation of pagestore_soak_test.c's
 * daemon-lifecycle and IPC-primitive sections: spawn/wait/attach/detach,
 * clean/crash stop, cl_exec() with a hang-detecting timeout, and thin op_*
 * wrappers around each PsChannel opcode used by stage 1 of the op-sequence
 * fuzzer (contrib/pagestore/pagestore_fuzz_test.c).  pagestore_soak_test.c
 * itself is intentionally left untouched; this header does not replace it,
 * and the two evolve independently (the soak test keeps its own local
 * copies of the same shapes for the same reason ARTIFACT_LIFECYCLE.md gives
 * for not sharing state across other standalone test binaries: a freestanding
 * client with no PostgreSQL headers should not depend on another test's
 * internal layout).
 *
 * Included only by freestanding (libc + pagestore_ipc.h/pagestore_shm.h)
 * test clients.  Every symbol here is `static`, so each including .c file
 * gets its own private copy -- this is a header-only library, not a shared
 * translation unit.
 *
 * src/../contrib/pagestore/pagestore_test_client.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_TEST_CLIENT_H
#define PAGESTORE_TEST_CLIENT_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"
#include "pagestore_shm.h"

/* ===================== configuration ================================== */

#define PSC_PAGE_SIZE		8192u
#define PSC_EXEC_TIMEOUT_NS	(120ull * 1000000000ull)

/* ===================== process-global client state ===================== */

static const char *psc_daemon_path;
static char psc_shm_name[64];
static char psc_store_dir[512];
static pid_t psc_daemon_pid = -1;
static int	psc_keep_store;

static void *psc_shm;
static int	psc_shm_fd = -1;
static int	psc_chan = -1;

/* ===================== small helpers ================================== */

static uint64_t
psc_now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
}

static void
psc_sleep_ms(long ms)
{
	struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};

	nanosleep(&ts, NULL);
}

static void
psc_remove_tree(const char *path)
{
	char		cmd[1024];

	if (snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", path) > 0 &&
		system(cmd) != 0)
		fprintf(stderr, "warning: could not remove %s\n", path);
}

/* Self-describing page content: bytes 0..7 carry the LSN, the rest a tag
 * XORed with the byte offset, so any returned page's provenance can be
 * checked without an external model lookup. */
static void
psc_fill_page(unsigned char *buf, uint64_t lsn, unsigned char tag)
{
	uint32_t	hi = (uint32_t) (lsn >> 32);
	uint32_t	lo = (uint32_t) lsn;

	memcpy(buf, &hi, 4);
	memcpy(buf + 4, &lo, 4);
	for (uint32_t i = 8; i < PSC_PAGE_SIZE; i++)
		buf[i] = (unsigned char) (tag ^ (i & 0xFF));
}

static int
psc_page_has_tag(const unsigned char *buf, unsigned char tag)
{
	for (uint32_t i = 8; i < PSC_PAGE_SIZE; i++)
		if (buf[i] != (unsigned char) (tag ^ (i & 0xFF)))
			return 0;
	return 1;
}

static uint64_t
psc_page_lsn(const unsigned char *buf)
{
	uint32_t	hi,
				lo;

	memcpy(&hi, buf, 4);
	memcpy(&lo, buf + 4, 4);
	return ((uint64_t) hi << 32) | lo;
}

/* ===================== daemon lifecycle ================================ */

static void
psc_kill_daemon(void)
{
	if (psc_daemon_pid > 0)
	{
		(void) kill(psc_daemon_pid, SIGKILL);
		while (waitpid(psc_daemon_pid, NULL, 0) < 0 && errno == EINTR)
			;
		psc_daemon_pid = -1;
	}
}

/* Infra-level fatal error (daemon would not start, hung beyond the exec
 * timeout, ...).  This is distinct from an oracle mismatch: callers that
 * want to keep the store and print a repro before exiting handle that
 * themselves and do not route through here. */
static void
psc_fatal(const char *fmt, ...)
{
	va_list		ap;

	fprintf(stderr, "FATAL: ");
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
	psc_kill_daemon();
	ps_shm_unlink(psc_shm_name);
	if (!psc_keep_store)
		psc_remove_tree(psc_store_dir);
	exit(2);
}

/*
 * Spawn the daemon with maintenance tuned as aggressive as its flags allow:
 * tiny flush/segment/compaction thresholds and low reclaim high-water marks,
 * so compaction, pruning, forkmeta cutover and WAL reclaim happen
 * continuously during a short fuzz run instead of only at soak-test scale.
 */
static void
psc_spawn_daemon(uint32_t page_size, uint32_t segment_size, uint32_t nshards,
				 uint32_t flush_pages, uint32_t compact_layers,
				 uint64_t page_high_water, uint64_t page_catch_up,
				 uint64_t wal_high_water, uint64_t wal_catch_up,
				 uint64_t walidx_high_water, uint64_t walidx_catch_up,
				 uint64_t forkmeta_high_water, uint64_t forkmeta_catch_up)
{
	char		page_size_s[32],
				segment_size_s[32],
				nshards_s[32],
				flush_pages_s[32],
				compact_layers_s[32],
				page_high[32],
				page_catch[32],
				wal_high[32],
				wal_catch[32],
				walidx_high[32],
				walidx_catch[32],
				forkmeta_high[32],
				forkmeta_catch[32];
	pid_t		pid = fork();

	if (pid < 0)
		psc_fatal("fork daemon: %s", strerror(errno));
	if (pid == 0)
	{
		snprintf(page_size_s, sizeof(page_size_s), "%u", page_size);
		snprintf(segment_size_s, sizeof(segment_size_s), "%u", segment_size);
		snprintf(nshards_s, sizeof(nshards_s), "%u", nshards);
		snprintf(flush_pages_s, sizeof(flush_pages_s), "%u", flush_pages);
		snprintf(compact_layers_s, sizeof(compact_layers_s), "%u", compact_layers);
		snprintf(page_high, sizeof(page_high), "%llu", (unsigned long long) page_high_water);
		snprintf(page_catch, sizeof(page_catch), "%llu", (unsigned long long) page_catch_up);
		snprintf(wal_high, sizeof(wal_high), "%llu", (unsigned long long) wal_high_water);
		snprintf(wal_catch, sizeof(wal_catch), "%llu", (unsigned long long) wal_catch_up);
		snprintf(walidx_high, sizeof(walidx_high), "%llu", (unsigned long long) walidx_high_water);
		snprintf(walidx_catch, sizeof(walidx_catch), "%llu", (unsigned long long) walidx_catch_up);
		snprintf(forkmeta_high, sizeof(forkmeta_high), "%llu", (unsigned long long) forkmeta_high_water);
		snprintf(forkmeta_catch, sizeof(forkmeta_catch), "%llu", (unsigned long long) forkmeta_catch_up);
		execl(psc_daemon_path, psc_daemon_path, "--shm", psc_shm_name,
			  "--store", psc_store_dir,
			  "--page-size", page_size_s, "--segment-size", segment_size_s,
			  "--nshards", nshards_s, "--flush-pages", flush_pages_s,
			  "--compact-layers", compact_layers_s,
			  "--segment-gc", "1",
			  "--page-high-water-bytes", page_high,
			  "--page-catch-up-bytes", page_catch,
			  "--wal-high-water-bytes", wal_high,
			  "--wal-catch-up-bytes", wal_catch,
			  "--walidx-high-water-bytes", walidx_high,
			  "--walidx-catch-up-bytes", walidx_catch,
			  "--forkmeta-high-water-bytes", forkmeta_high,
			  "--forkmeta-catch-up-bytes", forkmeta_catch,
			  (char *) NULL);
		perror("execl daemon");
		_exit(127);
	}
	psc_daemon_pid = pid;
}

static void
psc_wait_ready(uint32_t page_size)
{
	for (int i = 0; i < 3000; i++)	/* up to ~30s: recovery scans segments */
	{
		int			fd = ps_shm_open(psc_shm_name, O_RDWR, 0600);
		int			status;
		struct stat st;

		if (fd >= 0 && (fstat(fd, &st) != 0 || st.st_size < (off_t) PS_SHM_SIZE))
		{
			close(fd);
			fd = -1;
		}
		if (fd >= 0)
		{
			PsShmHeader *h = mmap(NULL, sizeof(PsShmHeader), PROT_READ,
								  MAP_SHARED, fd, 0);
			int			ready = 0;

			if (h != MAP_FAILED)
			{
				ready = h->magic == PS_SHM_MAGIC &&
					h->version == PS_SHM_VERSION &&
					h->startup_state == PS_SHM_READY &&
					h->page_size == page_size;
				munmap(h, sizeof(PsShmHeader));
			}
			close(fd);
			if (ready)
				return;
		}
		if (waitpid(psc_daemon_pid, &status, WNOHANG) == psc_daemon_pid)
		{
			psc_daemon_pid = -1;
			psc_fatal("daemon exited during startup (status %d)", status);
		}
		psc_sleep_ms(10);
	}
	psc_fatal("daemon did not become ready");
}

static void
psc_client_attach(void)
{
	PsShmHeader *hdr;

	psc_shm_fd = ps_shm_open(psc_shm_name, O_RDWR, 0600);
	if (psc_shm_fd < 0)
		psc_fatal("client shm_open: %s", strerror(errno));
	psc_shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
				  psc_shm_fd, 0);
	if (psc_shm == MAP_FAILED)
		psc_fatal("client mmap: %s", strerror(errno));
	hdr = (PsShmHeader *) psc_shm;
	psc_chan = -1;
	for (uint32_t i = 0; i < hdr->nchannels; i++)
		if (ps_cas(&ps_channel(psc_shm, i)->claimed, 0, 1))
		{
			psc_chan = (int) i;
			break;
		}
	if (psc_chan < 0)
		psc_fatal("no free channel");
}

static void
psc_client_detach(void)
{
	if (psc_shm != NULL)
	{
		if (psc_chan >= 0)
			ps_store_release(&ps_channel(psc_shm, psc_chan)->claimed, 0);
		munmap(psc_shm, PS_SHM_SIZE);
		psc_shm = NULL;
		psc_chan = -1;
	}
	if (psc_shm_fd >= 0)
	{
		close(psc_shm_fd);
		psc_shm_fd = -1;
	}
}

static void
psc_start_daemon(uint32_t page_size)
{
	psc_wait_ready(page_size);
	psc_client_attach();
}

/* Clean stop: SIGTERM, bounded wait, then escalate.  Returns 1 iff the
 * daemon exited cleanly (status 0); callers decide whether that is a check
 * failure or, for a deliberately racy/abandoned-channel scenario, tolerated. */
static int
psc_stop_daemon_clean(void)
{
	int			status;

	psc_client_detach();
	if (psc_daemon_pid <= 0)
		return 1;
	(void) kill(psc_daemon_pid, SIGTERM);
	for (int i = 0; i < 6000; i++)
	{
		pid_t		r = waitpid(psc_daemon_pid, &status, WNOHANG);

		if (r == psc_daemon_pid)
		{
			int			ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;

			psc_daemon_pid = -1;
			ps_shm_unlink(psc_shm_name);
			return ok;
		}
		psc_sleep_ms(10);
	}
	psc_kill_daemon();
	ps_shm_unlink(psc_shm_name);
	return 0;
}

static void
psc_stop_daemon_crash(void)
{
	psc_client_detach();
	psc_kill_daemon();
	ps_shm_unlink(psc_shm_name);
}

/* ===================== IPC primitives =================================== */

static PsChannel *
psc_chan_ptr(void)
{
	return ps_channel(psc_shm, psc_chan);
}

/*
 * Execute the currently-staged request on the owned channel.  A request that
 * does not complete within PSC_EXEC_TIMEOUT_NS is a hang -- an infra-level
 * failure under this fuzzer's contract, not an oracle mismatch -- and is
 * fatal (keeps the store when PAGESTORE_FUZZ_KEEP is set, via psc_fatal()).
 */
static PsChannel *
psc_cl_exec(void)
{
	PsChannel  *ch = psc_chan_ptr();
	uint64_t	start = psc_now_ns();
	uint32_t	opcode = ch->opcode;

	ps_request_generation_next(ch);
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	for (unsigned long spins = 0;; spins++)
	{
		uint32_t	state = ps_load_acquire(&ch->state);

		if (state == PS_STATE_DONE)
			break;
		if (spins < 2000)
			sched_yield();
		else
			usleep(50);
		if ((spins & 1023) == 0 && psc_now_ns() - start > PSC_EXEC_TIMEOUT_NS)
			psc_fatal("request opcode %u did not complete within %llus (state %u)",
					  opcode, (unsigned long long) (PSC_EXEC_TIMEOUT_NS / 1000000000ull),
					  state);
	}
	return ch;
}

static void
psc_set_channel_key(PsChannel *ch, uint32_t tl, uint64_t incarnation,
					uint32_t klass, uint32_t relnum)
{
	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->key.spcOid = 1;
	ch->key.dbOid = 1;
	ch->key.relNumber = 1000 + relnum;
	ch->key.forkNum = 0;
	ch->key.klass = klass;
	ch->timeline = tl;
	ch->incarnation = incarnation;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	ch->is_redo = 0;
	ch->skip_fsync = 0;
	ch->blocknum = 0;
	ch->nblocks = 0;
	ch->old_nblocks = 0;
	ch->parent_timeline = 0;
	ch->datalen = 0;
	ch->pad1 = 0;
}

static void
psc_setmeta(PsChannel *ch, uint32_t tl, uint64_t incarnation)
{
	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->timeline = tl;
	ch->incarnation = incarnation;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	ch->is_redo = 0;
	ch->skip_fsync = 0;
	ch->blocknum = 0;
	ch->nblocks = 0;
	ch->old_nblocks = 0;
	ch->parent_timeline = 0;
	ch->datalen = 0;
	ch->pad1 = 0;
}

/* ===================== op wrappers: relation ops ========================= */

static int
psc_op_create(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel, uint64_t lsn)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = lsn;
	return psc_cl_exec()->status;
}

static int
psc_op_unlink(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel, uint64_t lsn)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_UNLINK;
	ch->req_lsn = lsn;
	return psc_cl_exec()->status;
}

static int
psc_op_truncate(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
				uint32_t nblocks, uint64_t lsn)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_TRUNCATE;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
	return psc_cl_exec()->status;
}

static int
psc_op_zeroextend(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
				  uint32_t block, uint32_t nblocks, uint64_t lsn)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = block;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
	return psc_cl_exec()->status;
}

/* PS_OP_EXTEND: a single page write at blocknum, growing the fork; the
 * page's own encoded LSN (bytes 0..7, via psc_fill_page) is what the daemon
 * uses as this write's version -- req_lsn/req_seq here are the caller's
 * conflict-detection tuple (0 means "no prior write assumed"), separate from
 * the page content's LSN stamp. */
static int
psc_op_extend(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
			  uint32_t block, const unsigned char *page,
			  uint64_t req_lsn, uint64_t req_seq)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_EXTEND;
	ch->blocknum = block;
	ch->req_lsn = req_lsn;
	ch->req_seq = req_seq;
	memcpy(ch->data, page, PSC_PAGE_SIZE);
	return psc_cl_exec()->status;
}

static int
psc_op_writev(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
			  uint32_t block, const unsigned char *pages, uint32_t n)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = n;
	if (n > 0)
		memcpy(ch->data, pages, (size_t) n * PSC_PAGE_SIZE);
	return psc_cl_exec()->status;
}

static int
psc_op_readv(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
			 uint32_t block, uint64_t req_lsn, uint64_t req_seq,
			 unsigned char *out, uint32_t n)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = n;
	ch->req_lsn = req_lsn;
	ch->req_seq = req_seq;
	psc_cl_exec();
	if (ch->status == PS_STATUS_OK && n > 0)
		memcpy(out, ch->data, (size_t) n * PSC_PAGE_SIZE);
	return ch->status;
}

static int
psc_op_read_at(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
			   uint32_t block, uint64_t lsn, uint64_t seq,
			   unsigned char *out, int *found, uint64_t *resolved_lsn,
			   uint64_t *resolved_seq)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	psc_cl_exec();
	*found = ch->result != 0;
	if (resolved_lsn)
		*resolved_lsn = ch->req_lsn;
	if (resolved_seq)
		*resolved_seq = ch->req_seq;
	if (ch->status == PS_STATUS_OK && *found)
		memcpy(out, ch->data, PSC_PAGE_SIZE);
	return ch->status;
}

static int
psc_op_nblocks(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
			   uint64_t lsn, uint64_t seq, uint32_t *nblocks)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	psc_cl_exec();
	*nblocks = ch->result;
	return ch->status;
}

static int
psc_op_exists(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
			  uint64_t lsn, int *exists)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_EXISTS;
	ch->req_lsn = lsn;
	psc_cl_exec();
	*exists = ch->result != 0;
	return ch->status;
}

static int
psc_op_block_death(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
					uint32_t block, uint64_t lsn, uint64_t seq,
					uint64_t *death_lsn, uint64_t *death_seq)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_BLOCK_DEATH;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	psc_cl_exec();
	*death_lsn = ch->req_lsn;
	*death_seq = ch->req_seq;
	return ch->status;
}

static int
psc_op_immedsync(void)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, 0, 0);
	ch->opcode = PS_OP_IMMEDSYNC;
	return psc_cl_exec()->status;
}

/* PS_OP_ADMISSION_BARRIER: global (no timeline/incarnation association --
 * the daemon serves it before any per-timeline validation, see
 * pagestore_daemon.c's dispatcher around PS_OP_BEGIN_DELETE/ADMISSION_BARRIER
 * special-casing).  Out: req_seq, the admission sequence after every prior
 * mutation this process has observed complete. */
static int
psc_op_admission_barrier(uint64_t *seq_out)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, 0, 0);
	ch->opcode = PS_OP_ADMISSION_BARRIER;
	psc_cl_exec();
	if (seq_out)
		*seq_out = ch->req_seq;
	return ch->status;
}

/* PS_OP_ARTIFACT_BEGIN/COMMIT/DROP wrappers live further down, near
 * psc_op_write_control() -- see there for the field mapping and the
 * supersedable-flag parameter. */

/* ===================== op wrappers: WAL ================================= */

static int
psc_op_wal_append(uint32_t tl, uint64_t inc, uint64_t start, const void *data,
				  uint32_t len)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start;
	ch->datalen = len;
	if (len > 0)
		memcpy(ch->data, data, len);
	return psc_cl_exec()->status;
}

static int
psc_op_wal_size(uint32_t tl, uint64_t inc, uint64_t *end_lsn)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_SIZE;
	psc_cl_exec();
	*end_lsn = ch->req_lsn;
	return ch->status;
}

static int
psc_op_wal_read(uint32_t tl, uint64_t inc, uint64_t lsn, uint32_t len,
				unsigned char *out, uint32_t *nread)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = lsn;
	ch->datalen = len;
	psc_cl_exec();
	*nread = ch->result;
	if (ch->status == PS_STATUS_OK && ch->result > 0)
		memcpy(out, ch->data, ch->result);
	return ch->status;
}

static int
psc_op_walidx_add(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
				  uint32_t block, uint64_t lsn)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_WAL_INDEX_ADD;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	return psc_cl_exec()->status;
}

static int
psc_op_walidx_add_batch(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
						const uint32_t *blocks, uint32_t n, uint64_t lsn,
						uint64_t end_lsn)
{
	PsChannel  *ch = psc_chan_ptr();
	PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;

	psc_set_channel_key(ch, tl, inc, klass, rel);
	for (uint32_t i = 0; i < n; i++)
	{
		entries[i].key = ch->key;
		entries[i].block = blocks[i];
		entries[i].flags = PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI;
		entries[i].lsn = lsn;
		entries[i].end_lsn = end_lsn;
	}
	ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
	ch->nblocks = n;
	ch->datalen = n * (uint32_t) sizeof(*entries);
	return psc_cl_exec()->status;
}

static int
psc_op_walidx_get(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
				  uint32_t block, uint64_t max_lsn, PsWalRec *out,
				  int max_out, int *n_out)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_WAL_INDEX_GET;
	ch->blocknum = block;
	ch->req_lsn = max_lsn;
	ch->nblocks = (uint32_t) max_out;
	psc_cl_exec();
	*n_out = (int) ch->result;
	if (ch->status == PS_STATUS_OK && ch->result > 0)
		memcpy(out, ch->data, (size_t) ch->result * sizeof(PsWalRec));
	return ch->status;
}

static int
psc_op_walidx_progress_read(uint32_t tl, uint64_t inc, uint64_t *progress)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	psc_cl_exec();
	*progress = ch->req_lsn;
	return ch->status;
}

static int
psc_op_walidx_progress_commit(uint32_t tl, uint64_t inc, uint64_t start, uint64_t end)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = start;
	ch->req_seq = end;
	return psc_cl_exec()->status;
}

static int
psc_op_wal_retain_floor(uint32_t tl, uint64_t inc, uint64_t *floor, int *proven)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_WAL_RETAIN_FLOOR;
	psc_cl_exec();
	*floor = ch->req_lsn;
	*proven = ch->result != 0;
	return ch->status;
}

/* ===================== op wrappers: timelines ============================ */

static int
psc_op_timeline_state(uint32_t tl, PsTimelineState *state, uint64_t *incarnation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, 0);
	ch->opcode = PS_OP_TIMELINE_STATE;
	psc_cl_exec();
	if (state)
		*state = (PsTimelineState) ch->result;
	if (incarnation)
		*incarnation = ch->req_seq;
	return ch->status;
}

static int
psc_op_timeline_info(uint32_t tl, uint64_t inc, int *has_parent,
					 uint32_t *parent, uint64_t *branch_lsn,
					 uint64_t *parent_incarnation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_TIMELINE_INFO;
	psc_cl_exec();
	*has_parent = ch->result != 0;
	*parent = ch->parent_timeline;
	*branch_lsn = ch->req_lsn;
	*parent_incarnation = ch->req_seq;
	return ch->status;
}

static int
psc_op_check_branch(uint32_t tl, uint32_t parent, uint64_t branch_lsn,
					uint64_t target_incarnation, uint64_t parent_incarnation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, target_incarnation);
	ch->opcode = PS_OP_CHECK_BRANCH;
	ch->parent_timeline = parent;
	ch->req_lsn = branch_lsn;
	ch->req_seq = parent_incarnation;
	return psc_cl_exec()->status;
}

static int
psc_op_require_branch(uint32_t tl, uint32_t parent, uint64_t branch_lsn,
					  uint64_t target_incarnation, uint64_t parent_incarnation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, target_incarnation);
	ch->opcode = PS_OP_REQUIRE_BRANCH;
	ch->parent_timeline = parent;
	ch->req_lsn = branch_lsn;
	ch->req_seq = parent_incarnation;
	return psc_cl_exec()->status;
}

/* Fetches the parent's current incarnation itself, like the soak's
 * op_create_branch, so callers only supply the fork point. */
static int
psc_op_create_branch(uint32_t tl, uint32_t parent, uint64_t branch_lsn,
					 uint64_t target_incarnation, uint64_t *new_incarnation)
{
	PsChannel  *ch = psc_chan_ptr();
	PsTimelineState pstate;
	uint64_t	parent_incarnation;

	if (psc_op_timeline_state(parent, &pstate, &parent_incarnation) != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	psc_setmeta(ch, tl, target_incarnation);
	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->parent_timeline = parent;
	ch->req_lsn = branch_lsn;
	ch->req_seq = parent_incarnation;
	psc_cl_exec();
	if (ch->status == PS_STATUS_OK && new_incarnation)
		*new_incarnation = ch->incarnation;
	return ch->status;
}

/* Variant that lets the caller supply an explicit (possibly wrong/stale)
 * parent-incarnation token, for adversarial coverage. */
static int
psc_op_create_branch_raw(uint32_t tl, uint32_t parent, uint64_t branch_lsn,
						 uint64_t target_incarnation,
						 uint64_t parent_incarnation_token,
						 uint64_t *new_incarnation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, target_incarnation);
	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->parent_timeline = parent;
	ch->req_lsn = branch_lsn;
	ch->req_seq = parent_incarnation_token;
	psc_cl_exec();
	if (ch->status == PS_STATUS_OK && new_incarnation)
		*new_incarnation = ch->incarnation;
	return ch->status;
}

static int
psc_op_begin_delete(uint32_t tl, uint64_t incarnation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, incarnation);
	ch->opcode = PS_OP_BEGIN_DELETE;
	ch->req_seq = incarnation;
	psc_cl_exec();
	return ch->status;
}

/* result on error is a PsDeleteRefuseReason */
static int
psc_op_begin_delete_r(uint32_t tl, uint64_t incarnation, uint32_t *reason)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, incarnation);
	ch->opcode = PS_OP_BEGIN_DELETE;
	ch->req_seq = incarnation;
	psc_cl_exec();
	*reason = ch->result;
	return ch->status;
}

/* ===================== op wrappers: retention ============================ */

static int
psc_op_retention_reserve(uint32_t tl, uint32_t owner_kind, uint64_t owner_id,
						 uint32_t generation, uint32_t resources, uint64_t lsn,
						 uint64_t *seq_out)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, 0);
	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->blocknum = owner_kind;
	ch->parent_timeline = resources;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	psc_cl_exec();
	if (ch->status == PS_STATUS_OK && seq_out != NULL)
		memcpy(seq_out, ch->data, sizeof(*seq_out));
	return ch->status;
}

static int
psc_op_retention_set(uint32_t tl, uint32_t owner_kind, uint64_t owner_id,
					 uint32_t generation, uint32_t resources, uint64_t lsn,
					 uint64_t admission_seq)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, 0);
	ch->opcode = PS_OP_RETENTION_PIN_SET;
	ch->blocknum = owner_kind;
	ch->parent_timeline = resources;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	ch->nblocks = (uint32_t) admission_seq;
	ch->pad1 = (uint32_t) (admission_seq >> 32);
	return psc_cl_exec()->status;
}

static int
psc_op_retention_drop(uint32_t tl, uint32_t owner_kind, uint64_t owner_id,
					  uint32_t generation)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, 0);
	ch->opcode = PS_OP_RETENTION_PIN_DROP;
	ch->blocknum = owner_kind;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	return psc_cl_exec()->status;
}

static int
psc_op_retention_lookup(uint32_t tl, uint64_t inc, uint32_t owner_kind,
						uint64_t owner_id, PsRetentionPin *pin)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_RETENTION_PIN_LOOKUP;
	ch->blocknum = owner_kind;
	ch->req_seq = owner_id;
	psc_cl_exec();
	if (ch->status == PS_STATUS_OK && ch->result != 0)
	{
		memset(pin, 0, sizeof(*pin));
		pin->timeline = ch->timeline;
		pin->owner_kind = ch->blocknum;
		pin->resources = ch->parent_timeline;
		pin->generation = ch->old_nblocks;
		pin->owner_id = ch->req_seq;
		pin->lsn = ch->req_lsn;
		memcpy(&pin->admission_seq, ch->data, sizeof(pin->admission_seq));
		return 1;
	}
	return 0;
}

/*
 * index enumerates every currently-active retention pin across all owners
 * (not filtered by owner_kind: see ps_retention_get_consistent()).  *epoch
 * is the caller's last-known mutation_epoch; pass 0 to start enumeration
 * unconditionally.  A stale (nonzero, mismatching) epoch returns
 * PS_STATUS_STALE with the current epoch written back through *epoch, and
 * the caller must restart enumeration from index 0.
 */
static int
psc_op_retention_get(uint32_t index, uint64_t *epoch, PsRetentionPin *pin,
					 uint32_t *count)
{
	PsChannel  *ch = psc_chan_ptr();
	PsRetentionGetResult result;

	psc_setmeta(ch, 0, 0);
	ch->opcode = PS_OP_RETENTION_PIN_GET;
	ch->blocknum = index;
	ch->req_lsn = *epoch;
	psc_cl_exec();
	*count = ch->nblocks;
	if (ch->status != PS_STATUS_OK && ch->status != PS_STATUS_STALE)
		return -1;
	if (ch->status == PS_STATUS_STALE)
	{
		*epoch = ch->req_lsn;
		return -2;			/* caller must restart enumeration from index 0 */
	}
	memcpy(&result, ch->data, sizeof(result));
	*epoch = result.mutation_epoch;
	if (ch->result == 0)
		return 0;			/* not found at this index */
	memset(pin, 0, sizeof(*pin));
	pin->timeline = ch->timeline;
	pin->owner_kind = ch->blocknum;
	pin->resources = ch->parent_timeline;
	pin->generation = ch->old_nblocks;
	pin->owner_id = ch->req_seq;
	pin->lsn = ch->req_lsn;
	pin->admission_seq = result.admission_seq;
	return 1;
}

static int
psc_op_retention_floor(uint32_t tl, uint64_t inc, uint32_t resources,
					   uint64_t *floor, int *proven)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_setmeta(ch, tl, inc);
	ch->opcode = PS_OP_RETENTION_FLOOR;
	ch->parent_timeline = resources;
	psc_cl_exec();
	*floor = ch->req_lsn;
	*proven = ch->result != 0;
	return ch->status;
}

/* Control-class write mirroring the backend's/soak's obj_write shape, built
 * only out of stage-1 opcodes (CREATE/NBLOCKS/WRITEV/EXTEND) against
 * PS_KLASS_CONTROL, for a materializer-style publication. */
static int
psc_op_write_control(uint32_t block, const unsigned char *page, uint64_t version)
{
	PsChannel  *ch = psc_chan_ptr();
	uint32_t	nb = 0;

	psc_setmeta(ch, 0, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = 1;
	if (psc_cl_exec()->status != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	psc_setmeta(ch, 0, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_NBLOCKS;
	if (psc_cl_exec()->status != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	nb = ch->result;
	psc_setmeta(ch, 0, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = block < nb ? PS_OP_WRITEV : PS_OP_EXTEND;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->req_lsn = version;
	memcpy(ch->data, page, PSC_PAGE_SIZE);
	return psc_cl_exec()->status;
}

/* ===================== op wrappers: artifact lifecycle =================== */
/*
 * PS_OP_ARTIFACT_BEGIN/COMMIT/DROP (see pagestore_artifact_lifecycle.inc's
 * ps_artifact_begin/commit/drop and pagestore_daemon.c's handle_request()
 * cases for the exact field mapping used here). The data itself is written
 * through the *ordinary* PS_OP_EXTEND/PS_OP_WRITEV wrappers above with
 * key.klass in {PS_KLASS_SLRU, PS_KLASS_READER_SNAPSHOT} and req_lsn/req_seq
 * set to the open attempt's (lsn, token) -- there is no separate "write"
 * opcode. klass/rel here follow the same (spc=1,db=1,rel=1000+rel,forkNum=0)
 * convention as every other psc_op_* wrapper (artifact_data_key() requires
 * forkNum==0, which psc_set_channel_key() already zeroes).
 *
 * On error, *reason_out carries PsArtifactRefuseReason (pagestore_artifact_
 * format.h) from ch->result; 0 (PS_ARTIFACT_REFUSE_NONE) on success or when
 * refused before reaching artifact-specific logic (the timeline/incarnation
 * gate in ps_handle_meta, or the klass/opcode gate at the top of
 * handle_request()).
 */
static int
psc_op_artifact_begin(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
					   uint64_t lsn, int supersedable, uint64_t *token_out,
					   uint32_t *reason_out)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_ARTIFACT_BEGIN;
	ch->req_lsn = lsn;
	ch->parent_timeline = supersedable ? PS_ARTIFACT_REQ_SUPERSEDABLE : 0;
	psc_cl_exec();
	if (token_out)
		*token_out = ch->req_seq;
	if (reason_out)
		*reason_out = ch->result;
	return ch->status;
}

static int
psc_op_artifact_commit(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
						uint64_t lsn, uint64_t token, uint64_t count,
						int supersedable, uint32_t *reason_out)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_ARTIFACT_COMMIT;
	ch->req_lsn = lsn;
	ch->req_seq = token;
	ch->nblocks = (uint32_t) count;
	ch->parent_timeline = supersedable ? PS_ARTIFACT_REQ_SUPERSEDABLE : 0;
	psc_cl_exec();
	if (reason_out)
		*reason_out = ch->result;
	return ch->status;
}

static int
psc_op_artifact_drop(uint32_t tl, uint64_t inc, uint32_t klass, uint32_t rel,
					  uint64_t lsn, int supersedable, uint32_t *reason_out)
{
	PsChannel  *ch = psc_chan_ptr();

	psc_set_channel_key(ch, tl, inc, klass, rel);
	ch->opcode = PS_OP_ARTIFACT_DROP;
	ch->req_lsn = lsn;
	ch->parent_timeline = supersedable ? PS_ARTIFACT_REQ_SUPERSEDABLE : 0;
	psc_cl_exec();
	if (reason_out)
		*reason_out = ch->result;
	return ch->status;
}

#endif							/* PAGESTORE_TEST_CLIENT_H */
