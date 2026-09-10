/*-------------------------------------------------------------------------
 *
 * pagestore_soak_test.c
 *	  R6 bounded-space soak for the POSIX page-store daemon.
 *
 * A deterministic, seeded workload drives one real pagestore_daemon over the
 * shared-memory IPC protocol while playing every retention role the MVP
 * topology has: a WAL-shipping writer with a bounded live data set, a
 * materializer that publishes durable (LSN, admission_seq) cutoffs, fixed and
 * advancing readers that pin and release history, short-lived copy-on-write
 * branches that are written, read, and durably deleted, and clean/crash
 * daemon restarts.  Between rounds it measures the physical bytes of every
 * persisted category under the store directory and the daemon's published
 * reclaim/backpressure metrics.
 *
 * The test passes only when
 *   - every retained view stays correct: latest reads, pinned as-of reads,
 *     as-of relation sizes, branch isolation, and post-restart recovery;
 *   - every physical category stays within its declared bound while the
 *     workload continues (bounds are stated below and printed in the report);
 *   - after ingestion stops, every reclaimer catches up within its declared
 *     interval and the quiescent footprint is within the tighter steady-state
 *     bound;
 *   - the daemon never poisons a registry or manifest.
 *
 * Environment:
 *   PAGESTORE_SOAK_ROUNDS   workload rounds (default 2400; CI-sized)
 *   PAGESTORE_SOAK_SEED     PRNG seed (default 20260909)
 *   PAGESTORE_SOAK_KEEP     keep the store directory on exit
 *   PAGESTORE_SOAK_REPORT   also write the JSON report to this path
 *
 * Usage: pagestore_soak_test <path-to-pagestore_daemon> [store-base-dir]
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
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

/* ===================== configuration ================================== */

#define PAGE_SIZE		8192u
#define SEGMENT_SIZE	65536u		/* 8 pages per page-log segment */
#define NSHARDS			2u
#define FLUSH_PAGES		8u
#define COMPACT_LAYERS	2u

/* Controller configuration handed to the daemon.  Each reclaimer's declared
 * maximum lag is its high-water mark; its declared catch-up target is the
 * matching catch-up value. */
#define PAGE_HIGH_WATER		(512u * 1024u)
#define PAGE_CATCH_UP		(128u * 1024u)
#define WAL_HIGH_WATER		(2u * 1024u * 1024u)
#define WAL_CATCH_UP		(1024u * 1024u)
#define WALIDX_HIGH_WATER	(128u * 1024u)
#define WALIDX_CATCH_UP		(32u * 1024u)
#define FORKMETA_HIGH_WATER	(64u * 1024u)
#define FORKMETA_CATCH_UP	(16u * 1024u)

/* Workload shape: bounded live data, unbounded update history. */
#define NREL			6u
#define MAXBLK			48u
#define LIVE_BYTES_MAX	((uint64_t) NREL * MAXBLK * PAGE_SIZE)
#define MAT_INTERVAL	40		/* rounds between materializer publications */
#define READER_VERIFY	25		/* rounds between pinned-view verifications */
#define READER_LIFE		120		/* rounds a reader holds one fence */
#define BRANCH_INTERVAL	200		/* rounds between branch creations */
#define BRANCH_LIFE		150		/* rounds a branch lives before deletion */
#define RESTART_INTERVAL 500	/* rounds between daemon restarts */
#define SAMPLE_INTERVAL	20		/* rounds between physical measurements */
#define NREADERS		2u
#define NBRANCH_IDS		2u		/* timeline IDs rotated for reuse coverage */

/* Declared bounds.  "During" bounds hold at every sample while ingestion
 * continues; "quiescent" bounds hold after the reclaimers catch up.  They are
 * derived from the configuration above:
 *
 *   page log:  reclaim debt is capped by the PAGE controller at its high-water
 *              mark, plus at most one unflushed segment per shard and the
 *              flush window itself.
 *   layers:    each timeline/shard keeps at most COMPACT_LAYERS+1 image layers
 *              before compaction merges them; a merged layer holds the live
 *              set plus the history above the materializer floor (at most one
 *              MAT_INTERVAL of writes, bounded by 4 pages per round).
 *   WAL:       controller high-water plus the 1 MiB immutable segment
 *              granularity and one interval of un-materialized WAL.
 *   WAL index: controller high-water plus one snapshot generation.
 *   forkmeta:  controller high-water plus one snapshot generation.
 *   registry/timeline logs: tiny, compacted off the request path.
 */
#define ROUND_WRITE_BYTES_MAX	((uint64_t) 4 * PAGE_SIZE)
#define ROUND_WAL_BYTES_MAX		((uint64_t) 2048 + 4 * 1024)
#define INTERVAL_PAGE_BYTES		((uint64_t) MAT_INTERVAL * ROUND_WRITE_BYTES_MAX)
#define INTERVAL_WAL_BYTES		((uint64_t) MAT_INTERVAL * ROUND_WAL_BYTES_MAX)

typedef struct Bounds
{
	uint64_t	page;
	uint64_t	layers;
	uint64_t	wal;
	uint64_t	walidx;
	uint64_t	forkmeta;
	uint64_t	retention;
	uint64_t	timelines;
	uint64_t	other;
	uint64_t	files;			/* regular files in the store, inodes included */
} Bounds;

static const Bounds during_bound = {
	.page = PAGE_HIGH_WATER + 2u * NSHARDS * SEGMENT_SIZE + FLUSH_PAGES * PAGE_SIZE * NSHARDS + 4u * NSHARDS * SEGMENT_SIZE,
	.layers = (COMPACT_LAYERS + 2) * (LIVE_BYTES_MAX + INTERVAL_PAGE_BYTES * 2) * 2,
	/* A materializer pins WAL and the WAL index but no page history, so
	 * stored pages cannot replace the FPI-led chains at its horizon and the
	 * raw WAL they name stays until the horizon advances: allow two more
	 * publication intervals beyond the reclaimer's high water. */
	.wal = WAL_HIGH_WATER + 2u * 1024u * 1024u + INTERVAL_WAL_BYTES * 4,
	.walidx = WALIDX_HIGH_WATER * 4,
	.forkmeta = FORKMETA_HIGH_WATER * 4,
	.retention = 256u * 1024u,
	.timelines = 64u * 1024u,
	.other = 256u * 1024u,
	.files = 96,
};

static const Bounds quiescent_bound = {
	.page = PAGE_CATCH_UP + 2u * NSHARDS * SEGMENT_SIZE + FLUSH_PAGES * PAGE_SIZE * NSHARDS + 2u * NSHARDS * SEGMENT_SIZE,
	.layers = (COMPACT_LAYERS + 2) * (LIVE_BYTES_MAX + INTERVAL_PAGE_BYTES * 2),
	.wal = WAL_CATCH_UP + 2u * 1024u * 1024u + INTERVAL_WAL_BYTES,
	.walidx = WALIDX_HIGH_WATER * 2,
	.forkmeta = FORKMETA_HIGH_WATER * 2,
	.retention = 128u * 1024u,
	.timelines = 64u * 1024u,
	.other = 256u * 1024u,
	.files = 64,
};

/* Declared catch-up interval after ingestion stops. */
#define CATCH_UP_SECONDS	90

/* ===================== bookkeeping ==================================== */

static int	checks;
static int	failed;
static const char *daemon_path;
static char shm_name[64];
static char store_dir[512];
static pid_t daemon_pid = -1;
static int	keep_store;
static int	trace;
static uint64_t rng_state;

static void *cl_shm;
static int	cl_shm_fd = -1;
static int	cl_chan = -1;

static uint64_t g_wal_end;			/* next WAL byte on timeline 0 */
static uint64_t g_wal_start;		/* first shipped WAL byte on timeline 0 */
static uint64_t g_progress;			/* durable WAL-index progress on timeline 0 */
static uint64_t g_mat_lsn;			/* last materializer publication */
static uint64_t g_mat_seq;
static int	g_mat_registered;
static uint64_t logical_page_bytes;	/* workload page bytes written */
static uint64_t logical_wal_bytes;	/* workload WAL bytes shipped */
static uint64_t logical_ops;
static unsigned int restarts_clean;
static unsigned int restarts_crash;
static unsigned int branches_created;
static unsigned int branches_deleted;
static unsigned int reader_pins;
static unsigned int reader_verifications;
static unsigned int latest_verifications;

typedef struct RelModel
{
	int			exists;
	uint32_t	nblocks;
	unsigned char tag[MAXBLK];
	uint64_t	lsn[MAXBLK];
} RelModel;

static RelModel model[NREL];

typedef struct Reader
{
	int			held;
	uint64_t	owner_id;
	uint32_t	generation;
	uint64_t	lsn;
	uint64_t	seq;
	int			age;
	RelModel	snap[NREL];
} Reader;

static Reader readers[NREADERS];

typedef struct Branch
{
	int			state;			/* 0 none, 1 live, 2 deleting */
	uint32_t	tl;
	uint64_t	incarnation;
	uint64_t	branch_lsn;
	uint64_t	wal_end;		/* the branch compute's own WAL stream */
	int			age;
	RelModel	parent_snap[NREL];
	unsigned char child_written[NREL][MAXBLK];
	unsigned char child_tag[NREL][MAXBLK];
} Branch;

static Branch branch;
static uint64_t branch_incarnation[NBRANCH_IDS];	/* last known per ID */
static unsigned int branch_next_id;

/* ===================== small helpers ================================== */

static void
check(int cond, const char *fmt, ...)
{
	va_list		ap;

	checks++;
	if (cond)
		return;
	failed++;
	fprintf(stderr, "FAIL: ");
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
}

static uint64_t
rng_next(void)
{
	uint64_t	x = rng_state;

	x ^= x << 13;
	x ^= x >> 7;
	x ^= x << 17;
	rng_state = x;
	return x;
}

static uint32_t
rng_below(uint32_t n)
{
	return (uint32_t) (rng_next() % n);
}

static uint64_t
now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
}

static void
sleep_ms(long ms)
{
	struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};

	nanosleep(&ts, NULL);
}

static void
remove_tree(const char *path)
{
	char		cmd[1024];

	if (snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", path) > 0 &&
		system(cmd) != 0)
		fprintf(stderr, "warning: could not remove %s\n", path);
}

static void
fill_page(unsigned char *buf, uint64_t lsn, unsigned char tag)
{
	uint32_t	hi = (uint32_t) (lsn >> 32);
	uint32_t	lo = (uint32_t) lsn;

	memcpy(buf, &hi, 4);
	memcpy(buf + 4, &lo, 4);
	for (uint32_t i = 8; i < PAGE_SIZE; i++)
		buf[i] = (unsigned char) (tag ^ (i & 0xFF));
}

static int
page_has_tag(const unsigned char *buf, unsigned char tag)
{
	for (uint32_t i = 8; i < PAGE_SIZE; i++)
		if (buf[i] != (unsigned char) (tag ^ (i & 0xFF)))
			return 0;
	return 1;
}

static uint64_t
page_lsn(const unsigned char *buf)
{
	uint32_t	hi,
				lo;

	memcpy(&hi, buf, 4);
	memcpy(&lo, buf + 4, 4);
	return ((uint64_t) hi << 32) | lo;
}

/* ===================== daemon lifecycle =============================== */

static void
kill_daemon(void)
{
	if (daemon_pid > 0)
	{
		(void) kill(daemon_pid, SIGKILL);
		while (waitpid(daemon_pid, NULL, 0) < 0 && errno == EINTR)
			;
		daemon_pid = -1;
	}
}

static void
fatal(const char *fmt, ...)
{
	va_list		ap;

	fprintf(stderr, "FATAL: ");
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
	kill_daemon();
	shm_unlink(shm_name);
	if (!keep_store)
		remove_tree(store_dir);
	exit(2);
}

static void
spawn_daemon(void)
{
	char		page_high[32],
				page_catch[32],
				wal_high[32],
				wal_catch[32],
				walidx_high[32],
				walidx_catch[32],
				forkmeta_high[32],
				forkmeta_catch[32];
	pid_t		pid = fork();

	if (pid < 0)
		fatal("fork daemon: %s", strerror(errno));
	if (pid == 0)
	{
		snprintf(page_high, sizeof(page_high), "%u", PAGE_HIGH_WATER);
		snprintf(page_catch, sizeof(page_catch), "%u", PAGE_CATCH_UP);
		snprintf(wal_high, sizeof(wal_high), "%u", WAL_HIGH_WATER);
		snprintf(wal_catch, sizeof(wal_catch), "%u", WAL_CATCH_UP);
		snprintf(walidx_high, sizeof(walidx_high), "%u", WALIDX_HIGH_WATER);
		snprintf(walidx_catch, sizeof(walidx_catch), "%u", WALIDX_CATCH_UP);
		snprintf(forkmeta_high, sizeof(forkmeta_high), "%u", FORKMETA_HIGH_WATER);
		snprintf(forkmeta_catch, sizeof(forkmeta_catch), "%u", FORKMETA_CATCH_UP);
		execl(daemon_path, daemon_path, "--shm", shm_name, "--store", store_dir,
			  "--page-size", "8192", "--segment-size", "65536",
			  "--nshards", "2", "--flush-pages", "8", "--compact-layers", "2",
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
	daemon_pid = pid;
}

static void
wait_ready(void)
{
	for (int i = 0; i < 3000; i++)	/* up to ~30s: recovery scans segments */
	{
		int			fd = shm_open(shm_name, O_RDWR, 0600);
		int			status;
		struct stat st;

		/* the object exists before the daemon sizes it; a mapping of the
		 * empty object faults, so wait for the size as well */
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
					h->page_size == PAGE_SIZE;
				munmap(h, sizeof(PsShmHeader));
			}
			close(fd);
			if (ready)
				return;
		}
		if (waitpid(daemon_pid, &status, WNOHANG) == daemon_pid)
		{
			daemon_pid = -1;
			fatal("daemon exited during startup (status %d)", status);
		}
		sleep_ms(10);
	}
	fatal("daemon did not become ready");
}

static void
client_attach(void)
{
	PsShmHeader *hdr;

	cl_shm_fd = shm_open(shm_name, O_RDWR, 0600);
	if (cl_shm_fd < 0)
		fatal("client shm_open: %s", strerror(errno));
	cl_shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
				  cl_shm_fd, 0);
	if (cl_shm == MAP_FAILED)
		fatal("client mmap: %s", strerror(errno));
	hdr = (PsShmHeader *) cl_shm;
	cl_chan = -1;
	for (uint32_t i = 0; i < hdr->nchannels; i++)
		if (ps_cas(&ps_channel(cl_shm, i)->claimed, 0, 1))
		{
			cl_chan = (int) i;
			break;
		}
	if (cl_chan < 0)
		fatal("no free channel");
}

static void
client_detach(void)
{
	if (cl_shm != NULL)
	{
		if (cl_chan >= 0)
			ps_store_release(&ps_channel(cl_shm, cl_chan)->claimed, 0);
		munmap(cl_shm, PS_SHM_SIZE);
		cl_shm = NULL;
		cl_chan = -1;
	}
	if (cl_shm_fd >= 0)
	{
		close(cl_shm_fd);
		cl_shm_fd = -1;
	}
}

static void
start_daemon(void)
{
	spawn_daemon();
	wait_ready();
	client_attach();
}

/* Clean stop: SIGTERM, bounded wait, then escalate. */
static void
stop_daemon_clean(void)
{
	int			status;

	client_detach();
	if (daemon_pid <= 0)
		return;
	(void) kill(daemon_pid, SIGTERM);
	for (int i = 0; i < 6000; i++)
	{
		pid_t		r = waitpid(daemon_pid, &status, WNOHANG);

		if (r == daemon_pid)
		{
			check(WIFEXITED(status) && WEXITSTATUS(status) == 0,
				  "daemon exits cleanly on SIGTERM (status %d)", status);
			daemon_pid = -1;
			shm_unlink(shm_name);
			return;
		}
		sleep_ms(10);
	}
	check(0, "daemon stopped within 60s of SIGTERM");
	kill_daemon();
	shm_unlink(shm_name);
}

static void
stop_daemon_crash(void)
{
	client_detach();
	kill_daemon();
	shm_unlink(shm_name);
}

/* ===================== metrics ======================================== */

typedef struct Metrics
{
	PsBackpressureMetrics page,
				wal,
				walidx,
				forkmeta;
	uint64_t	page_debt_segments;
	uint32_t	page_debt_unavailable;
	uint64_t	deleting_layers;
	uint64_t	gc_deleting_layers;
	uint64_t	remote_cleanup_pending;
	uint32_t	forkmeta_pending;
	uint32_t	forkmeta_poisoned;
	uint32_t	manifest_poisoned;
	uint32_t	metadata_poisoned;
	uint32_t	retention_poisoned;
	uint64_t	layer_count;
	uint64_t	live_timelines;
	uint64_t	deleting_timelines;
	uint64_t	deleted_timelines;
	uint64_t	owner_count;
	uint64_t	prune_compactions;
	uint64_t	prune_deleted;
	uint64_t	wal_index_pending_bytes;
} Metrics;

static void
read_metrics(Metrics *m)
{
	PsShmHeader *hdr = (PsShmHeader *) cl_shm;

	for (int attempt = 0; attempt < 100000; attempt++)
	{
		uint64_t	b1 = ps_load_acquire_u64(&hdr->backpressure_metrics_seq);
		uint64_t	b2;

		if (b1 & 1)
		{
			usleep(100);
			continue;
		}
		m->page = hdr->page_backpressure;
		m->wal = hdr->wal_backpressure;
		m->walidx = hdr->walidx_backpressure;
		m->forkmeta = hdr->forkmeta_backpressure;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		b2 = ps_load_acquire_u64(&hdr->backpressure_metrics_seq);
		if (b1 == b2)
			break;
	}
	for (int attempt = 0; attempt < 100000; attempt++)
	{
		uint64_t	i1 = ps_load_acquire_u64(&hdr->inspection_metrics_seq);
		uint64_t	i2;

		if (i1 & 1)
		{
			usleep(100);
			continue;
		}
		m->page_debt_segments = hdr->inspection.page_debt_segments;
		m->page_debt_unavailable = hdr->inspection.page_debt_unavailable;
		m->deleting_layers = hdr->inspection.deleting_layers;
		m->gc_deleting_layers = hdr->inspection.gc_deleting_layers;
		m->remote_cleanup_pending = hdr->inspection.remote_cleanup_pending;
		m->forkmeta_pending = hdr->inspection.forkmeta_pending;
		m->forkmeta_poisoned = hdr->inspection.forkmeta_poisoned;
		m->manifest_poisoned = hdr->inspection.manifest_poisoned;
		m->metadata_poisoned = hdr->inspection.metadata_poisoned;
		m->retention_poisoned = hdr->inspection.retention_poisoned;
		m->layer_count = hdr->inspection.layer_count;
		m->live_timelines = hdr->inspection.live_timelines;
		m->deleting_timelines = hdr->inspection.deleting_timelines;
		m->deleted_timelines = hdr->inspection.deleted_timelines;
		m->owner_count = hdr->inspection.owner_count;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		i2 = ps_load_acquire_u64(&hdr->inspection_metrics_seq);
		if (i1 == i2)
			break;
	}
	for (int attempt = 0; attempt < 100000; attempt++)
	{
		uint64_t	p1 = ps_load_acquire_u64(&hdr->page_prune_metrics_seq);
		uint64_t	p2;

		if (p1 & 1)
		{
			usleep(100);
			continue;
		}
		m->prune_compactions = hdr->page_prune_compactions;
		m->prune_deleted = hdr->page_prune_versions_deleted;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		p2 = ps_load_acquire_u64(&hdr->page_prune_metrics_seq);
		if (p1 == p2)
			break;
	}
	m->wal_index_pending_bytes = ps_load_acquire_u64(&hdr->wal_index_pending_bytes);
}

static void
dump_metrics(FILE *out, const Metrics *m)
{
	fprintf(out,
			"  page: lag=%llu throttled=%u enters=%llu wait_ms=%llu debt_segments=%llu%s\n"
			"  wal: lag=%llu throttled=%u enters=%llu wait_ms=%llu\n"
			"  walidx: lag=%llu throttled=%u enters=%llu wait_ms=%llu pending_bytes=%llu\n"
			"  forkmeta: lag=%llu throttled=%u enters=%llu wait_ms=%llu pending=%u\n"
			"  layers=%llu deleting=%llu remote_pending=%llu owners=%llu"
			" timelines live=%llu deleting=%llu deleted=%llu"
			" prune compactions=%llu deleted=%llu poisoned=%u%u%u%u\n",
			(unsigned long long) m->page.lag_bytes, m->page.throttled,
			(unsigned long long) m->page.throttle_enters,
			(unsigned long long) (m->page.foreground_wait_ns / 1000000),
			(unsigned long long) m->page_debt_segments,
			m->page_debt_unavailable ? " (unavailable)" : "",
			(unsigned long long) m->wal.lag_bytes, m->wal.throttled,
			(unsigned long long) m->wal.throttle_enters,
			(unsigned long long) (m->wal.foreground_wait_ns / 1000000),
			(unsigned long long) m->walidx.lag_bytes, m->walidx.throttled,
			(unsigned long long) m->walidx.throttle_enters,
			(unsigned long long) (m->walidx.foreground_wait_ns / 1000000),
			(unsigned long long) m->wal_index_pending_bytes,
			(unsigned long long) m->forkmeta.lag_bytes, m->forkmeta.throttled,
			(unsigned long long) m->forkmeta.throttle_enters,
			(unsigned long long) (m->forkmeta.foreground_wait_ns / 1000000),
			m->forkmeta_pending,
			(unsigned long long) m->layer_count,
			(unsigned long long) m->deleting_layers,
			(unsigned long long) m->remote_cleanup_pending,
			(unsigned long long) m->owner_count,
			(unsigned long long) m->live_timelines,
			(unsigned long long) m->deleting_timelines,
			(unsigned long long) m->deleted_timelines,
			(unsigned long long) m->prune_compactions,
			(unsigned long long) m->prune_deleted,
			m->manifest_poisoned, m->metadata_poisoned, m->retention_poisoned,
			m->forkmeta_poisoned);
}

/* ===================== physical measurement =========================== */

typedef struct Physical
{
	uint64_t	page;
	uint64_t	layers;
	uint64_t	wal;
	uint64_t	walidx;
	uint64_t	forkmeta;
	uint64_t	retention;
	uint64_t	timelines;
	uint64_t	other;
	uint64_t	total;
	uint64_t	files;
} Physical;

/* Space a file occupies: the larger of its logical length and the blocks the
 * filesystem allocated to it, so neither sparse files nor slack from many
 * small files hides behind the other measure. */
static uint64_t
file_bytes(const struct stat *st)
{
	uint64_t	logical = (uint64_t) st->st_size;
	uint64_t	allocated = (uint64_t) st->st_blocks * 512u;

	return allocated > logical ? allocated : logical;
}

typedef struct FileSize
{
	char		path[512];
	uint64_t	size;
	int			seen;
} FileSize;

static FileSize *file_sizes;
static unsigned int nfile_sizes;
static unsigned int file_sizes_cap;
static uint64_t physical_written;	/* approximate bytes written to disk */

static void
note_file(const char *path, uint64_t size)
{
	for (unsigned int i = 0; i < nfile_sizes; i++)
		if (strcmp(file_sizes[i].path, path) == 0)
		{
			if (size > file_sizes[i].size)
				physical_written += size - file_sizes[i].size;
			else if (size < file_sizes[i].size)
				physical_written += size;	/* rewritten or replaced */
			file_sizes[i].size = size;
			file_sizes[i].seen = 1;
			return;
		}
	if (nfile_sizes == file_sizes_cap)
	{
		unsigned int ncap = file_sizes_cap ? file_sizes_cap * 2 : 256;
		FileSize   *n = realloc(file_sizes, ncap * sizeof(*n));

		if (n == NULL)
			fatal("out of memory tracking files");
		file_sizes = n;
		file_sizes_cap = ncap;
	}
	snprintf(file_sizes[nfile_sizes].path, sizeof(file_sizes[0].path), "%s", path);
	file_sizes[nfile_sizes].size = size;
	file_sizes[nfile_sizes].seen = 1;
	nfile_sizes++;
	physical_written += size;
}

static uint64_t
dir_bytes(const char *path, Physical *p)
{
	DIR		   *dir = opendir(path);
	struct dirent *ent;
	uint64_t	total = 0;

	if (dir == NULL)
		return 0;
	while ((ent = readdir(dir)) != NULL)
	{
		char		child[1024];
		struct stat st;

		if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
			continue;
		snprintf(child, sizeof(child), "%s/%s", path, ent->d_name);
		if (lstat(child, &st) != 0)
			continue;
		if (S_ISDIR(st.st_mode))
			total += dir_bytes(child, p);
		else if (S_ISREG(st.st_mode))
		{
			total += file_bytes(&st);
			p->files++;
			note_file(child, file_bytes(&st));
		}
	}
	closedir(dir);
	return total;
}

static void
measure(Physical *p)
{
	DIR		   *dir = opendir(store_dir);
	struct dirent *ent;

	memset(p, 0, sizeof(*p));
	for (unsigned int i = 0; i < nfile_sizes; i++)
		file_sizes[i].seen = 0;
	if (dir == NULL)
		fatal("opendir store: %s", strerror(errno));
	while ((ent = readdir(dir)) != NULL)
	{
		char		child[1024];
		struct stat st;
		uint64_t	bytes;
		const char *n = ent->d_name;

		if (strcmp(n, ".") == 0 || strcmp(n, "..") == 0)
			continue;
		snprintf(child, sizeof(child), "%s/%s", store_dir, n);
		if (lstat(child, &st) != 0)
			continue;
		if (S_ISDIR(st.st_mode))
			bytes = dir_bytes(child, p);
		else if (S_ISREG(st.st_mode))
		{
			bytes = file_bytes(&st);
			p->files++;
			note_file(child, bytes);
		}
		else
			continue;
		if (strncmp(n, "seg_", 4) == 0)
			p->page += bytes;
		else if (strncmp(n, "layer", 5) == 0)
			p->layers += bytes;
		else if (strncmp(n, "walidx", 6) == 0)
			p->walidx += bytes;
		else if (strncmp(n, "wal_", 4) == 0)
			p->wal += bytes;
		else if (strncmp(n, "forkmeta", 8) == 0)
			p->forkmeta += bytes;
		else if (strncmp(n, "retention", 9) == 0)
			p->retention += bytes;
		else if (strncmp(n, "timelines", 9) == 0)
			p->timelines += bytes;
		else
			p->other += bytes;
		p->total += bytes;
	}
	closedir(dir);
	/* Forget files that disappeared so a later same-name file counts fresh. */
	{
		unsigned int w = 0;

		for (unsigned int i = 0; i < nfile_sizes; i++)
			if (file_sizes[i].seen)
				file_sizes[w++] = file_sizes[i];
		nfile_sizes = w;
	}
}

static void
physical_max(Physical *acc, const Physical *p)
{
#define MAXF(f) if (p->f > acc->f) acc->f = p->f
	MAXF(page);
	MAXF(layers);
	MAXF(wal);
	MAXF(walidx);
	MAXF(forkmeta);
	MAXF(retention);
	MAXF(timelines);
	MAXF(other);
	MAXF(total);
	MAXF(files);
#undef MAXF
}

static int
physical_within(const Physical *p, const Bounds *b, const char *phase,
				int report)
{
	int			ok = 1;

#define BOUNDF(f) \
	do { \
		if (p->f > b->f) \
		{ \
			ok = 0; \
			if (report) \
				fprintf(stderr, "  %s: %s %llu > bound %llu\n", phase, #f, \
						(unsigned long long) p->f, (unsigned long long) b->f); \
		} \
	} while (0)
	BOUNDF(page);
	BOUNDF(layers);
	BOUNDF(wal);
	BOUNDF(walidx);
	BOUNDF(forkmeta);
	BOUNDF(retention);
	BOUNDF(timelines);
	BOUNDF(other);
	BOUNDF(files);
#undef BOUNDF
	return ok;
}

static void
dump_physical(FILE *out, const char *label, const Physical *p)
{
	fprintf(out, "  %s: total=%llu page=%llu layers=%llu wal=%llu walidx=%llu"
			" forkmeta=%llu retention=%llu timelines=%llu other=%llu files=%llu\n",
			label, (unsigned long long) p->total, (unsigned long long) p->page,
			(unsigned long long) p->layers, (unsigned long long) p->wal,
			(unsigned long long) p->walidx, (unsigned long long) p->forkmeta,
			(unsigned long long) p->retention, (unsigned long long) p->timelines,
			(unsigned long long) p->other, (unsigned long long) p->files);
}

/* ===================== IPC primitives ================================= */

#define EXEC_TIMEOUT_NS	(120ull * 1000000000ull)

static PsChannel *
chan(void)
{
	return ps_channel(cl_shm, cl_chan);
}

static PsChannel *
cl_exec(void)
{
	PsChannel  *ch = chan();
	uint64_t	start = now_ns();
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
		if ((spins & 1023) == 0 && now_ns() - start > EXEC_TIMEOUT_NS)
		{
			Metrics		m;

			fprintf(stderr, "request opcode %u did not complete within %llus\n",
					opcode, (unsigned long long) (EXEC_TIMEOUT_NS / 1000000000ull));
			read_metrics(&m);
			dump_metrics(stderr, &m);
			fatal("daemon stalled (state %u)", state);
		}
	}
	return ch;
}

static void
setkey(PsChannel *ch, uint32_t tl, uint64_t incarnation, uint32_t rel)
{
	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->key.spcOid = 1;
	ch->key.dbOid = 1;
	ch->key.relNumber = 1000 + rel;
	ch->key.forkNum = 0;
	ch->key.klass = PS_KLASS_RELATION;
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
setmeta(PsChannel *ch, uint32_t tl, uint64_t incarnation)
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

static int
op_create(uint32_t tl, uint64_t inc, uint32_t rel, uint64_t lsn)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = lsn;
	return cl_exec()->status;
}

static int
op_unlink(uint32_t tl, uint64_t inc, uint32_t rel, uint64_t lsn)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_UNLINK;
	ch->req_lsn = lsn;
	return cl_exec()->status;
}

static int
op_truncate(uint32_t tl, uint64_t inc, uint32_t rel, uint32_t nblocks,
			uint64_t lsn)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_TRUNCATE;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
	return cl_exec()->status;
}

static int
op_zeroextend(uint32_t tl, uint64_t inc, uint32_t rel, uint32_t block,
			  uint32_t nblocks, uint64_t lsn)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = block;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn;
	return cl_exec()->status;
}

static int
op_writev(uint32_t tl, uint64_t inc, uint32_t rel, uint32_t block,
		  const unsigned char *pages, uint32_t n)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = n;
	memcpy(ch->data, pages, (size_t) n * PAGE_SIZE);
	return cl_exec()->status;
}

static int
op_readv(uint32_t tl, uint64_t inc, uint32_t rel, uint32_t block,
		 unsigned char *out, uint32_t n)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = n;
	cl_exec();
	if (ch->status == PS_STATUS_OK)
		memcpy(out, ch->data, (size_t) n * PAGE_SIZE);
	return ch->status;
}

static int
op_read_at(uint32_t tl, uint64_t inc, uint32_t rel, uint32_t block,
		   uint64_t lsn, uint64_t seq, unsigned char *out, int *found)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	cl_exec();
	*found = ch->result != 0;
	if (ch->status == PS_STATUS_OK && *found)
		memcpy(out, ch->data, PAGE_SIZE);
	return ch->status;
}

static int
op_nblocks(uint32_t tl, uint64_t inc, uint32_t rel, uint64_t lsn,
		   uint64_t seq, uint32_t *nblocks)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = lsn;
	ch->req_seq = seq;
	cl_exec();
	*nblocks = ch->result;
	return ch->status;
}

static int
op_exists(uint32_t tl, uint64_t inc, uint32_t rel, uint64_t lsn, int *exists)
{
	PsChannel  *ch = chan();

	setkey(ch, tl, inc, rel);
	ch->opcode = PS_OP_EXISTS;
	ch->req_lsn = lsn;
	cl_exec();
	*exists = ch->result != 0;
	return ch->status;
}

static int
op_wal_append(uint32_t tl, uint64_t inc, uint64_t start, const void *data,
			  uint32_t len)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start;
	ch->datalen = len;
	memcpy(ch->data, data, len);
	return cl_exec()->status;
}

static int
op_walidx_add_batch(uint32_t tl, uint64_t inc, uint32_t rel,
					const uint32_t *blocks, uint32_t n, uint64_t lsn,
					uint64_t end_lsn)
{
	PsChannel  *ch = chan();
	PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;

	setkey(ch, tl, inc, rel);
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
	return cl_exec()->status;
}

static int
op_walidx_progress(uint32_t tl, uint64_t inc, uint64_t start, uint64_t end)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, inc);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = start;
	ch->req_seq = end;
	return cl_exec()->status;
}

static int
op_wal_retain_floor(uint32_t tl, uint64_t *floor)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_WAL_RETAIN_FLOOR;
	cl_exec();
	*floor = ch->req_lsn;
	return ch->status;
}

static int
op_retention_reserve(uint32_t tl, uint32_t owner_kind, uint64_t owner_id,
					 uint32_t generation, uint32_t resources, uint64_t lsn,
					 uint64_t *seq_out)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, 0);
	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->blocknum = owner_kind;
	ch->parent_timeline = resources;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	cl_exec();
	if (ch->status == PS_STATUS_OK && seq_out != NULL)
		memcpy(seq_out, ch->data, sizeof(*seq_out));
	return ch->status;
}

static int
op_retention_drop(uint32_t tl, uint32_t owner_kind, uint64_t owner_id,
				  uint32_t generation)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, 0);
	ch->opcode = PS_OP_RETENTION_PIN_DROP;
	ch->blocknum = owner_kind;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	return cl_exec()->status;
}

static int
op_retention_lookup(uint32_t tl, uint32_t owner_kind, uint64_t owner_id,
					PsRetentionPin *pin)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, 0);
	ch->opcode = PS_OP_RETENTION_PIN_LOOKUP;
	ch->blocknum = owner_kind;
	ch->req_seq = owner_id;
	cl_exec();
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

/* Control-class write mirroring the backend's obj_write shape. */
static int
op_write_control(uint32_t block, const unsigned char *page, uint64_t version)
{
	PsChannel  *ch = chan();
	uint32_t	nb = 0;

	setmeta(ch, 0, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = 1;
	if (cl_exec()->status != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	setmeta(ch, 0, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_NBLOCKS;
	if (cl_exec()->status != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	nb = ch->result;
	setmeta(ch, 0, 0);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = block < nb ? PS_OP_WRITEV : PS_OP_EXTEND;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->req_lsn = version;
	memcpy(ch->data, page, PAGE_SIZE);
	return cl_exec()->status;
}

static int
op_timeline_state(uint32_t tl, PsTimelineState *state, uint64_t *incarnation)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, 0);
	ch->opcode = PS_OP_TIMELINE_STATE;
	cl_exec();
	if (ch->status != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	*state = (PsTimelineState) ch->result;
	*incarnation = ch->req_seq;
	return PS_STATUS_OK;
}

static int
op_create_branch(uint32_t tl, uint32_t parent, uint64_t branch_lsn,
				 uint64_t target_incarnation, uint64_t *new_incarnation)
{
	PsChannel  *ch = chan();
	PsTimelineState pstate;
	uint64_t	parent_incarnation;

	if (op_timeline_state(parent, &pstate, &parent_incarnation) != PS_STATUS_OK)
		return PS_STATUS_ERROR;
	setmeta(ch, tl, target_incarnation);
	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->parent_timeline = parent;
	ch->req_lsn = branch_lsn;
	ch->req_seq = parent_incarnation;
	cl_exec();
	if (ch->status == PS_STATUS_OK)
		*new_incarnation = ch->incarnation;
	return ch->status;
}

static int
op_begin_delete(uint32_t tl, uint64_t incarnation)
{
	PsChannel  *ch = chan();

	setmeta(ch, tl, incarnation);
	ch->opcode = PS_OP_BEGIN_DELETE;
	ch->req_seq = incarnation;
	return cl_exec()->status;
}

/* ===================== workload: writer =============================== */

static unsigned char page_buf[4 * PAGE_SIZE];
static unsigned char read_buf[4 * PAGE_SIZE];
static unsigned char wal_buf[PS_IO_UNIT];

/* Ship one self-describing WAL record covering the mutation and index it. */
static uint64_t
ship_wal_on(uint64_t *wal_end, uint32_t tl, uint64_t inc, uint32_t rel,
			const uint32_t *blocks, uint32_t nblocks, uint32_t payload)
{
	uint64_t	start = *wal_end;
	uint64_t	end = start + payload;

	memset(wal_buf, (int) (0x40 + (start / 4096) % 64), payload);
	memcpy(wal_buf, &start, sizeof(start));
	check(op_wal_append(tl, inc, start, wal_buf, payload) == PS_STATUS_OK,
		  "WAL append on timeline %u at %llu", tl, (unsigned long long) start);
	if (nblocks > 0)
		check(op_walidx_add_batch(tl, inc, rel, blocks, nblocks, start, end) ==
			  PS_STATUS_OK, "WAL index batch on timeline %u", tl);
	logical_wal_bytes += payload;
	*wal_end = end;
	return end;
}

/* Timeline 0 is the writer's own WAL stream. */
static uint64_t
ship_wal(uint32_t tl, uint64_t inc, uint32_t rel, const uint32_t *blocks,
		 uint32_t nblocks, uint32_t payload)
{
	return ship_wal_on(&g_wal_end, tl, inc, rel, blocks, nblocks, payload);
}

static void
writer_round(void)
{
	uint32_t	rel = rng_below(NREL);
	RelModel   *m = &model[rel];
	uint32_t	choice = rng_below(100);

	logical_ops++;
	if (!m->exists)
	{
		uint64_t	lsn = ship_wal(0, 0, rel, NULL, 0, 512);

		check(op_create(0, 0, rel, lsn) == PS_STATUS_OK, "create relation %u", rel);
		m->exists = 1;
		m->nblocks = 0;
		return;
	}
	if (choice < 70 || m->nblocks == 0)
	{
		/* Write 1..4 pages; grow the relation when the range passes its end. */
		uint32_t	n = 1 + rng_below(4);
		uint32_t	limit = m->nblocks + n <= MAXBLK ? m->nblocks + 1 : MAXBLK - n + 1;
		uint32_t	block = rng_below(limit);
		uint32_t	blocks[4];
		uint64_t	lsn;

		if (block + n > MAXBLK)
			block = MAXBLK - n;
		for (uint32_t i = 0; i < n; i++)
			blocks[i] = block + i;
		lsn = ship_wal(0, 0, rel, blocks, n, 2048 + n * 1024);
		if (block + n > m->nblocks)
		{
			check(op_zeroextend(0, 0, rel, m->nblocks, block + n - m->nblocks,
								lsn) == PS_STATUS_OK,
				  "zero-extend relation %u to %u", rel, block + n);
			for (uint32_t b = m->nblocks; b < block + n; b++)
			{
				m->tag[b] = 0;
				m->lsn[b] = 0;
			}
			m->nblocks = block + n;
		}
		for (uint32_t i = 0; i < n; i++)
		{
			unsigned char tag = (unsigned char) (1 + rng_below(255));

			fill_page(page_buf + (size_t) i * PAGE_SIZE, lsn, tag);
			m->tag[block + i] = tag;
			m->lsn[block + i] = lsn;
		}
		check(op_writev(0, 0, rel, block, page_buf, n) == PS_STATUS_OK,
			  "write %u pages of relation %u at %u", n, rel, block);
		logical_page_bytes += (uint64_t) n * PAGE_SIZE;
		return;
	}
	if (choice < 82)
	{
		uint32_t	to = rng_below(m->nblocks);
		uint64_t	lsn = ship_wal(0, 0, rel, NULL, 0, 512);

		check(op_truncate(0, 0, rel, to, lsn) == PS_STATUS_OK,
			  "truncate relation %u to %u", rel, to);
		m->nblocks = to;
		return;
	}
	if (choice < 88)
	{
		uint64_t	lsn = ship_wal(0, 0, rel, NULL, 0, 512);

		check(op_unlink(0, 0, rel, lsn) == PS_STATUS_OK, "unlink relation %u", rel);
		m->exists = 0;
		m->nblocks = 0;
		memset(m->tag, 0, sizeof(m->tag));
		memset(m->lsn, 0, sizeof(m->lsn));
		return;
	}
	/* Latest-view verification of a random range. */
	{
		uint32_t	n = 1 + rng_below(4);
		uint32_t	block;
		uint32_t	nb = 0;

		if (n > m->nblocks)
			n = m->nblocks;
		block = rng_below(m->nblocks - n + 1);
		check(op_nblocks(0, 0, rel, 0, 0, &nb) == PS_STATUS_OK && nb == m->nblocks,
			  "latest nblocks of relation %u is %u (got %u)", rel, m->nblocks, nb);
		check(op_readv(0, 0, rel, block, read_buf, n) == PS_STATUS_OK,
			  "latest read of relation %u at %u", rel, block);
		for (uint32_t i = 0; i < n; i++)
		{
			const unsigned char *pg = read_buf + (size_t) i * PAGE_SIZE;

			if (m->tag[block + i] == 0)
			{
				int			zero = 1;

				for (uint32_t k = 0; k < PAGE_SIZE && zero; k++)
					zero = pg[k] == 0;
				check(zero, "latest read of unwritten block %u/%u is zero", rel,
					  block + i);
			}
			else
				check(page_has_tag(pg, m->tag[block + i]) &&
					  page_lsn(pg) == m->lsn[block + i],
					  "latest read of block %u/%u carries its newest tag", rel,
					  block + i);
		}
		latest_verifications++;
	}
}

/* Verify one complete latest view (after restarts). */
static void
verify_latest_all(const char *phase)
{
	for (uint32_t rel = 0; rel < NREL; rel++)
	{
		RelModel   *m = &model[rel];
		int			exists;
		uint32_t	nb = 0;

		check(op_exists(0, 0, rel, 0, &exists) == PS_STATUS_OK &&
			  exists == m->exists, "%s: relation %u existence", phase, rel);
		if (!m->exists)
			continue;
		check(op_nblocks(0, 0, rel, 0, 0, &nb) == PS_STATUS_OK && nb == m->nblocks,
			  "%s: relation %u nblocks %u (got %u)", phase, rel, m->nblocks, nb);
		for (uint32_t b = 0; b < m->nblocks; b++)
		{
			check(op_readv(0, 0, rel, b, read_buf, 1) == PS_STATUS_OK,
				  "%s: read %u/%u", phase, rel, b);
			if (m->tag[b] != 0)
				check(page_has_tag(read_buf, m->tag[b]) &&
					  page_lsn(read_buf) == m->lsn[b],
					  "%s: block %u/%u newest content", phase, rel, b);
		}
	}
}

/* ===================== workload: materializer ========================= */

static void
materialize(void)
{
	uint64_t	lsn = g_wal_end;
	uint64_t	seq = 0;
	unsigned char note[PAGE_SIZE];
	unsigned char image[PAGE_SIZE];

	/* Restorable control image with a same-version retention-floor note. */
	memset(note, 0, sizeof(note));
	memcpy(note, &lsn, sizeof(lsn));
	memset(image, 0x5c, sizeof(image));
	memcpy(image, &lsn, sizeof(lsn));
	check(op_write_control(1, note, lsn) == PS_STATUS_OK,
		  "materializer writes control note at %llu", (unsigned long long) lsn);
	check(op_write_control(0, image, lsn) == PS_STATUS_OK,
		  "materializer writes control image at %llu", (unsigned long long) lsn);
	/* Durable progress marker (control block 3), as the real materializer
	 * publishes after its relation pages are durable: the daemon derives the
	 * page-history cutoff from it, since the materializer pins no history. */
	memset(image, 0x3d, sizeof(image));
	memcpy(image, &lsn, sizeof(lsn));
	check(op_write_control(3, image, lsn) == PS_STATUS_OK,
		  "materializer writes its progress marker at %llu",
		  (unsigned long long) lsn);
	/* Durable index progress for the shipped prefix. */
	if (op_walidx_progress(0, 0, g_progress, lsn) != PS_STATUS_OK)
	{
		PsChannel  *ch = chan();
		uint64_t	current;
		uint64_t	wal_size;

		setmeta(ch, 0, 0);
		ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
		cl_exec();
		current = ch->req_lsn;
		setmeta(ch, 0, 0);
		ch->opcode = PS_OP_WAL_SIZE;
		cl_exec();
		wal_size = ch->req_lsn;
		check(0, "materializer publishes WAL-index progress [%llu,%llu)"
			  " (daemon progress %llu, WAL end %llu)",
			  (unsigned long long) g_progress, (unsigned long long) lsn,
			  (unsigned long long) current, (unsigned long long) wal_size);
	}
	else
		g_progress = lsn;
	/* Exact durable (LSN, admission_seq) cutoff for WAL and the WAL index,
	 * the resources the real materializer pins; page history follows its
	 * marker instead. */
	{
		int			rc = op_retention_reserve(0, PS_RETENTION_OWNER_MATERIALIZER,
											 1, 1,
											 PS_RETENTION_RESOURCE_WAL |
											 PS_RETENTION_RESOURCE_WAL_INDEX,
											 lsn, &seq);

		check(rc == PS_STATUS_OK && seq != 0,
			  "materializer advances its owner pin to %llu (status %d)",
			  (unsigned long long) lsn, rc);
		if (rc == PS_STATUS_OK && seq != 0)
		{
			g_mat_lsn = lsn;
			g_mat_seq = seq;
			g_mat_registered = 1;
		}
	}
	if (trace)
	{
		uint64_t	floor = 0;
		int			rc = op_wal_retain_floor(0, &floor);

		fprintf(stderr, "trace: materialized %llu seq %llu; WAL floor %llu (status %d)\n",
				(unsigned long long) lsn, (unsigned long long) seq,
				(unsigned long long) floor, rc);
	}
}

/* ===================== workload: readers ============================== */

static void
reader_snapshot(Reader *r)
{
	memcpy(r->snap, model, sizeof(model));
}

static void
reader_verify(Reader *r, const char *phase)
{
	for (int i = 0; i < 6; i++)
	{
		uint32_t	rel = rng_below(NREL);
		RelModel   *s = &r->snap[rel];
		uint32_t	nb = 0;
		int			found;

		{
			int			rc = op_nblocks(0, 0, rel, r->lsn, r->seq, &nb);

			check(rc == PS_STATUS_OK && nb == s->nblocks,
				  "%s: reader %llu as-of nblocks of %u is %u (got %u, status %d,"
				  " pin %llu/%llu age %d, materializer %llu/%llu)", phase,
				  (unsigned long long) r->owner_id, rel, s->nblocks, nb, rc,
				  (unsigned long long) r->lsn, (unsigned long long) r->seq, r->age,
				  (unsigned long long) g_mat_lsn, (unsigned long long) g_mat_seq);
		}
		if (s->nblocks == 0)
			continue;
		{
			uint32_t	b = rng_below(s->nblocks);
			int			rc = op_read_at(0, 0, rel, b, r->lsn, r->seq, read_buf, &found);

			check(rc == PS_STATUS_OK, "%s: reader %llu as-of read %u/%u", phase,
				  (unsigned long long) r->owner_id, rel, b);
			if (rc != PS_STATUS_OK)
				continue;
			if (s->tag[b] == 0)
				check(!found || page_lsn(read_buf) == 0,
					  "%s: reader %llu as-of read of unwritten %u/%u", phase,
					  (unsigned long long) r->owner_id, rel, b);
			else
				check(found && page_has_tag(read_buf, s->tag[b]) &&
					  page_lsn(read_buf) == s->lsn[b],
					  "%s: reader %llu as-of read %u/%u sees its pinned version",
					  phase, (unsigned long long) r->owner_id, rel, b);
		}
	}
	reader_verifications++;
}

static void
reader_pin(Reader *r, uint64_t lsn)
{
	uint64_t	seq = 0;

	int			rc = op_retention_reserve(0, PS_RETENTION_OWNER_READER,
										 r->owner_id, r->generation,
										 PS_RETENTION_RESOURCE_ALL, lsn, &seq);

	check(rc == PS_STATUS_OK && seq != 0, "reader %llu pins %llu (status %d)",
		  (unsigned long long) r->owner_id, (unsigned long long) lsn, rc);
	if (rc != PS_STATUS_OK || seq == 0)
		return;
	r->held = 1;
	r->lsn = lsn;
	r->seq = seq;
	r->age = 0;
	reader_snapshot(r);
	reader_pins++;
}

static void
reader_round(int round)
{
	for (uint32_t i = 0; i < NREADERS; i++)
	{
		Reader	   *r = &readers[i];

		if (!r->held)
		{
			if (rng_below(20) == 0)
				reader_pin(r, g_wal_end);
			continue;
		}
		r->age++;
		if (r->age % READER_VERIFY == 0)
			reader_verify(r, "steady");
		if (r->age >= READER_LIFE + (int) i * 37)
		{
			if (i == 0)
			{
				/* Advancing reader: re-pin at the newest position. */
				reader_pin(r, g_wal_end);
			}
			else
			{
				check(op_retention_drop(0, PS_RETENTION_OWNER_READER, r->owner_id,
										r->generation) == PS_STATUS_OK,
					  "reader %llu drops its pin", (unsigned long long) r->owner_id);
				r->held = 0;
				/* A dropped owner stays fenced at its old generation; the next
				 * session of this logical reader is a controller takeover. */
				r->generation++;
			}
		}
	}
	(void) round;
}

/* ===================== workload: branches ============================= */

static void
branch_create(void)
{
	uint32_t	id = 1 + branch_next_id;
	uint64_t	target = branch_incarnation[branch_next_id] == 0 ? 0 :
	branch_incarnation[branch_next_id] + 1;
	uint64_t	inc = 0;
	int			rc;

	/* Fork at an exact materialized horizon. */
	materialize();
	rc = op_create_branch(id, 0, g_mat_lsn, target, &inc);
	check(rc == PS_STATUS_OK, "create branch %u at %llu (incarnation %llu)", id,
		  (unsigned long long) g_mat_lsn, (unsigned long long) target);
	if (rc != PS_STATUS_OK)
		return;
	branch.state = 1;
	branch.tl = id;
	branch.incarnation = inc;
	branch.branch_lsn = g_mat_lsn;
	branch.wal_end = g_mat_lsn;
	branch.age = 0;
	memcpy(branch.parent_snap, model, sizeof(model));
	memset(branch.child_written, 0, sizeof(branch.child_written));
	branch_incarnation[branch_next_id] = inc;
	branch_next_id = (branch_next_id + 1) % NBRANCH_IDS;
	branches_created++;
}

static void
branch_verify(const char *phase)
{
	for (int i = 0; i < 6; i++)
	{
		uint32_t	rel = rng_below(NREL);
		RelModel   *s = &branch.parent_snap[rel];
		uint32_t	nb = 0;

		if (!s->exists)
		{
			int			exists;

			check(op_exists(branch.tl, branch.incarnation, rel, 0, &exists) ==
				  PS_STATUS_OK && !exists,
				  "%s: branch %u does not see a relation absent at its fork", phase,
				  branch.tl);
			continue;
		}
		check(op_nblocks(branch.tl, branch.incarnation, rel, 0, 0, &nb) ==
			  PS_STATUS_OK && nb == s->nblocks,
			  "%s: branch %u nblocks of %u is %u (got %u)", phase, branch.tl, rel,
			  s->nblocks, nb);
		if (s->nblocks == 0)
			continue;
		{
			uint32_t	b = rng_below(s->nblocks);
			unsigned char expect = branch.child_written[rel][b] ?
			branch.child_tag[rel][b] : s->tag[b];

			check(op_readv(branch.tl, branch.incarnation, rel, b, read_buf, 1) ==
				  PS_STATUS_OK, "%s: branch %u read %u/%u", phase, branch.tl, rel, b);
			if (expect != 0)
				check(page_has_tag(read_buf, expect),
					  "%s: branch %u block %u/%u sees %s", phase, branch.tl, rel, b,
					  branch.child_written[rel][b] ? "its own write" :
					  "the fork-point parent version");
		}
	}
}

static void
branch_write(void)
{
	uint32_t	rel = rng_below(NREL);
	RelModel   *s = &branch.parent_snap[rel];
	uint32_t	b;
	uint32_t	blocks[1];
	uint64_t	lsn;
	unsigned char tag;

	if (!s->exists || s->nblocks == 0)
		return;
	b = rng_below(s->nblocks);
	blocks[0] = b;
	lsn = ship_wal_on(&branch.wal_end, branch.tl, branch.incarnation, rel,
					  blocks, 1, 1024);
	tag = (unsigned char) (1 + rng_below(255));
	fill_page(page_buf, lsn, tag);
	check(op_writev(branch.tl, branch.incarnation, rel, b, page_buf, 1) ==
		  PS_STATUS_OK, "branch %u writes %u/%u", branch.tl, rel, b);
	branch.child_written[rel][b] = 1;
	branch.child_tag[rel][b] = tag;
	logical_page_bytes += PAGE_SIZE;
}

static void
branch_round(int round)
{
	PsTimelineState state;
	uint64_t	inc;

	if (branch.state == 0)
	{
		if (round % BRANCH_INTERVAL == BRANCH_INTERVAL / 2)
			branch_create();
		return;
	}
	if (branch.state == 1)
	{
		branch.age++;
		if (branch.age % 10 == 0)
			branch_write();
		if (branch.age % 30 == 0)
			branch_verify("steady");
		if (branch.age >= BRANCH_LIFE)
		{
			check(op_begin_delete(branch.tl, branch.incarnation) == PS_STATUS_OK,
				  "begin deleting branch %u", branch.tl);
			branch.state = 2;
		}
		return;
	}
	if (op_timeline_state(branch.tl, &state, &inc) == PS_STATUS_OK &&
		state == PS_TIMELINE_DELETED)
	{
		uint32_t	nb = 0;

		check(inc == branch.incarnation, "deleted branch %u keeps incarnation",
			  branch.tl);
		check(op_nblocks(branch.tl, branch.incarnation, 0, 0, 0, &nb) !=
			  PS_STATUS_OK, "deleted branch %u rejects requests", branch.tl);
		branch.state = 0;
		branches_deleted++;
	}
}

static void
branch_wait_deleted(void)
{
	uint64_t	start = now_ns();

	while (branch.state == 2)
	{
		PsTimelineState state;
		uint64_t	inc;

		if (op_timeline_state(branch.tl, &state, &inc) == PS_STATUS_OK &&
			state == PS_TIMELINE_DELETED)
		{
			branch.state = 0;
			branches_deleted++;
			return;
		}
		if (now_ns() - start > (uint64_t) CATCH_UP_SECONDS * 1000000000ull)
		{
			check(0, "branch %u reaches DELETED within %d seconds", branch.tl,
				  CATCH_UP_SECONDS);
			return;
		}
		sleep_ms(20);
	}
}

/* ===================== restarts ====================================== */

static void
verify_after_restart(const char *phase)
{
	verify_latest_all(phase);
	for (uint32_t i = 0; i < NREADERS; i++)
	{
		Reader	   *r = &readers[i];
		PsRetentionPin pin;

		if (!r->held)
			continue;
		check(op_retention_lookup(0, PS_RETENTION_OWNER_READER, r->owner_id, &pin) &&
			  pin.lsn == r->lsn && pin.admission_seq == r->seq &&
			  pin.generation == r->generation,
			  "%s: reader %llu pin survives", phase, (unsigned long long) r->owner_id);
		reader_verify(r, phase);
	}
	if (g_mat_registered)
	{
		PsRetentionPin pin;

		check(op_retention_lookup(0, PS_RETENTION_OWNER_MATERIALIZER, 1, &pin) &&
			  pin.lsn == g_mat_lsn && pin.admission_seq == g_mat_seq,
			  "%s: materializer pin survives", phase);
	}
	if (branch.state == 1)
		branch_verify(phase);
}

static void
restart_daemon(int crash)
{
	if (crash)
	{
		stop_daemon_crash();
		restarts_crash++;
	}
	else
	{
		stop_daemon_clean();
		restarts_clean++;
	}
	start_daemon();
	verify_after_restart(crash ? "after crash" : "after clean restart");
}

/* ===================== quiescence ===================================== */

/* A controller is caught up when it is not throttling and its lag is below
 * its declared maximum (the high-water mark).  Hysteresis means a controller
 * that never throttled may idle anywhere below high water; the catch-up
 * target only governs release after a throttle. */
static int
reclaimers_caught_up(const Metrics *m)
{
	return m->page.lag_bytes <= PAGE_HIGH_WATER &&
		m->wal.lag_bytes <= WAL_HIGH_WATER &&
		m->walidx.lag_bytes <= WALIDX_HIGH_WATER &&
		m->forkmeta.lag_bytes <= FORKMETA_HIGH_WATER &&
		!m->page.throttled && !m->wal.throttled && !m->walidx.throttled &&
		!m->forkmeta.throttled &&
		m->deleting_layers == 0 && m->gc_deleting_layers == 0 &&
		m->remote_cleanup_pending == 0 && m->deleting_timelines == 0;
}

static int
poisoned(const Metrics *m)
{
	return m->manifest_poisoned || m->metadata_poisoned ||
		m->retention_poisoned || m->forkmeta_poisoned;
}

/* ===================== report ========================================= */

static void
write_report(FILE *out, int rounds, uint64_t seed, const Physical *max,
			 const Physical *quiescent, const Metrics *m,
			 double catch_up_seconds, int during_ok, int quiescent_ok)
{
	uint64_t	live = 0;

	for (uint32_t rel = 0; rel < NREL; rel++)
		if (model[rel].exists)
			live += (uint64_t) model[rel].nblocks * PAGE_SIZE;
	fprintf(out,
			"{\"soak\":\"pagestore-r6\",\"rounds\":%d,\"seed\":%llu,"
			"\"logical\":{\"live_bytes\":%llu,\"live_bytes_max\":%llu,"
			"\"page_bytes_written\":%llu,\"wal_bytes_shipped\":%llu,\"ops\":%llu},"
			"\"physical_written_approx\":%llu,\"write_amplification\":%.2f,"
			"\"physical_max\":{\"total\":%llu,\"page\":%llu,\"layers\":%llu,"
			"\"wal\":%llu,\"walidx\":%llu,\"forkmeta\":%llu,\"retention\":%llu,"
			"\"timelines\":%llu,\"other\":%llu,\"files\":%llu},"
			"\"physical_quiescent\":{\"total\":%llu,\"page\":%llu,\"layers\":%llu,"
			"\"wal\":%llu,\"walidx\":%llu,\"forkmeta\":%llu,\"retention\":%llu,"
			"\"timelines\":%llu,\"other\":%llu,\"files\":%llu},"
			"\"bounds_during\":{\"page\":%llu,\"layers\":%llu,\"wal\":%llu,"
			"\"walidx\":%llu,\"forkmeta\":%llu,\"retention\":%llu,"
			"\"timelines\":%llu,\"other\":%llu,\"files\":%llu},"
			"\"bounds_quiescent\":{\"page\":%llu,\"layers\":%llu,\"wal\":%llu,"
			"\"walidx\":%llu,\"forkmeta\":%llu,\"retention\":%llu,"
			"\"timelines\":%llu,\"other\":%llu,\"files\":%llu},"
			"\"reclaimers\":{"
			"\"page\":{\"high_water\":%u,\"catch_up\":%u,\"throttle_enters\":%llu,\"wait_ms\":%llu,\"final_lag\":%llu},"
			"\"wal\":{\"high_water\":%u,\"catch_up\":%u,\"throttle_enters\":%llu,\"wait_ms\":%llu,\"final_lag\":%llu},"
			"\"walidx\":{\"high_water\":%u,\"catch_up\":%u,\"throttle_enters\":%llu,\"wait_ms\":%llu,\"final_lag\":%llu},"
			"\"forkmeta\":{\"high_water\":%u,\"catch_up\":%u,\"throttle_enters\":%llu,\"wait_ms\":%llu,\"final_lag\":%llu},"
			"\"catch_up_seconds\":%.2f,\"catch_up_limit_seconds\":%d},"
			"\"activity\":{\"restarts_clean\":%u,\"restarts_crash\":%u,"
			"\"branches_created\":%u,\"branches_deleted\":%u,\"reader_pins\":%u,"
			"\"reader_verifications\":%u,\"latest_verifications\":%u,"
			"\"page_prune_compactions\":%llu,\"page_versions_deleted\":%llu,"
			"\"active_owners\":%llu,\"layers\":%llu},"
			"\"result\":{\"checks\":%d,\"failures\":%d,\"during_bounds_ok\":%s,"
			"\"quiescent_bounds_ok\":%s}}\n",
			rounds, (unsigned long long) seed,
			(unsigned long long) live, (unsigned long long) LIVE_BYTES_MAX,
			(unsigned long long) logical_page_bytes,
			(unsigned long long) logical_wal_bytes,
			(unsigned long long) logical_ops,
			(unsigned long long) physical_written,
			logical_page_bytes + logical_wal_bytes == 0 ? 0.0 :
			(double) physical_written / (double) (logical_page_bytes + logical_wal_bytes),
			(unsigned long long) max->total, (unsigned long long) max->page,
			(unsigned long long) max->layers, (unsigned long long) max->wal,
			(unsigned long long) max->walidx, (unsigned long long) max->forkmeta,
			(unsigned long long) max->retention, (unsigned long long) max->timelines,
			(unsigned long long) max->other, (unsigned long long) max->files,
			(unsigned long long) quiescent->total, (unsigned long long) quiescent->page,
			(unsigned long long) quiescent->layers, (unsigned long long) quiescent->wal,
			(unsigned long long) quiescent->walidx,
			(unsigned long long) quiescent->forkmeta,
			(unsigned long long) quiescent->retention,
			(unsigned long long) quiescent->timelines,
			(unsigned long long) quiescent->other,
			(unsigned long long) quiescent->files,
			(unsigned long long) during_bound.page, (unsigned long long) during_bound.layers,
			(unsigned long long) during_bound.wal, (unsigned long long) during_bound.walidx,
			(unsigned long long) during_bound.forkmeta,
			(unsigned long long) during_bound.retention,
			(unsigned long long) during_bound.timelines,
			(unsigned long long) during_bound.other,
			(unsigned long long) during_bound.files,
			(unsigned long long) quiescent_bound.page,
			(unsigned long long) quiescent_bound.layers,
			(unsigned long long) quiescent_bound.wal,
			(unsigned long long) quiescent_bound.walidx,
			(unsigned long long) quiescent_bound.forkmeta,
			(unsigned long long) quiescent_bound.retention,
			(unsigned long long) quiescent_bound.timelines,
			(unsigned long long) quiescent_bound.other,
			(unsigned long long) quiescent_bound.files,
			PAGE_HIGH_WATER, PAGE_CATCH_UP,
			(unsigned long long) m->page.throttle_enters,
			(unsigned long long) (m->page.foreground_wait_ns / 1000000),
			(unsigned long long) m->page.lag_bytes,
			WAL_HIGH_WATER, WAL_CATCH_UP,
			(unsigned long long) m->wal.throttle_enters,
			(unsigned long long) (m->wal.foreground_wait_ns / 1000000),
			(unsigned long long) m->wal.lag_bytes,
			WALIDX_HIGH_WATER, WALIDX_CATCH_UP,
			(unsigned long long) m->walidx.throttle_enters,
			(unsigned long long) (m->walidx.foreground_wait_ns / 1000000),
			(unsigned long long) m->walidx.lag_bytes,
			FORKMETA_HIGH_WATER, FORKMETA_CATCH_UP,
			(unsigned long long) m->forkmeta.throttle_enters,
			(unsigned long long) (m->forkmeta.foreground_wait_ns / 1000000),
			(unsigned long long) m->forkmeta.lag_bytes,
			catch_up_seconds, CATCH_UP_SECONDS,
			restarts_clean, restarts_crash, branches_created, branches_deleted,
			reader_pins, reader_verifications, latest_verifications,
			(unsigned long long) m->prune_compactions,
			(unsigned long long) m->prune_deleted,
			(unsigned long long) m->owner_count,
			(unsigned long long) m->layer_count,
			checks, failed, during_ok ? "true" : "false",
			quiescent_ok ? "true" : "false");
}

/* ===================== main =========================================== */

int
main(int argc, char **argv)
{
	const char *base = argc >= 3 ? argv[2] : "/tmp";
	const char *env;
	int			rounds = 2400;
	uint64_t	seed = 20260909ull;
	Physical	sample,
				max,
				quiescent;
	Metrics		m;
	Metrics		quiescent_metrics;
	int			during_ok = 1;
	int			quiescent_ok;
	int			during_violations = 0;
	double		catch_up_seconds = -1;
	uint64_t	t0;
	uint64_t	floor = 0;

	memset(&quiescent_metrics, 0, sizeof(quiescent_metrics));
	if (argc < 2)
	{
		fprintf(stderr, "usage: %s <path-to-pagestore_daemon> [store-base-dir]\n",
				argv[0]);
		return 2;
	}
	daemon_path = argv[1];
	if ((env = getenv("PAGESTORE_SOAK_ROUNDS")) != NULL && atoi(env) > 0)
		rounds = atoi(env);
	if ((env = getenv("PAGESTORE_SOAK_SEED")) != NULL)
		seed = strtoull(env, NULL, 10);
	keep_store = getenv("PAGESTORE_SOAK_KEEP") != NULL;
	trace = getenv("PAGESTORE_SOAK_TRACE") != NULL;
	rng_state = seed ? seed : 1;
	snprintf(shm_name, sizeof(shm_name), "/pssoak_%d", (int) getpid());
	snprintf(store_dir, sizeof(store_dir), "%s/pagestore-soak-%d", base,
			 (int) getpid());
	shm_unlink(shm_name);
	remove_tree(store_dir);
	if (mkdir(store_dir, 0700) != 0)
		fatal("mkdir %s: %s", store_dir, strerror(errno));
	memset(&max, 0, sizeof(max));
	memset(&quiescent, 0, sizeof(quiescent));

	/* WAL starts at a nonzero, immutable-segment-aligned position, as a real
	 * archive ships whole segments; LSN 0 means "unconstrained". */
	g_wal_start = 1024 * 1024;
	g_wal_end = g_wal_start;
	g_progress = g_wal_start;
	for (uint32_t i = 0; i < NREADERS; i++)
	{
		readers[i].owner_id = 100 + i;
		readers[i].generation = 1;
	}

	start_daemon();
	fprintf(stderr, "pagestore soak: %d rounds, seed %llu, store %s\n", rounds,
			(unsigned long long) seed, store_dir);

	/* Bootstrap: every relation exists with a few pages, and the materializer
	 * has published its first durable cutoff before history can be pruned. */
	for (uint32_t rel = 0; rel < NREL; rel++)
	{
		uint64_t	lsn = ship_wal(0, 0, rel, NULL, 0, 512);

		check(op_create(0, 0, rel, lsn) == PS_STATUS_OK, "bootstrap create %u", rel);
		model[rel].exists = 1;
	}
	for (int i = 0; i < 40; i++)
		writer_round();
	materialize();

	t0 = now_ns();
	for (int round = 1; round <= rounds; round++)
	{
		writer_round();
		reader_round(round);
		branch_round(round);
		if (round % MAT_INTERVAL == 0)
			materialize();
		if (round % RESTART_INTERVAL == 0)
			restart_daemon((round / RESTART_INTERVAL) % 2 == 0);
		if (round % SAMPLE_INTERVAL == 0)
		{
			measure(&sample);
			physical_max(&max, &sample);
			if (!physical_within(&sample, &during_bound, "during",
								 during_violations < 8))
			{
				during_ok = 0;
				during_violations++;
			}
			read_metrics(&m);
			check(!poisoned(&m), "round %d: no registry or manifest is poisoned",
				  round);
			check(!m.page_debt_unavailable,
				  "round %d: page reclaim debt remains observable", round);
		}
		if (round % 400 == 0)
		{
			read_metrics(&m);
			fprintf(stderr, "round %d (%.1fs): wal_end=%llu mat=%llu\n", round,
					(double) (now_ns() - t0) / 1e9,
					(unsigned long long) g_wal_end,
					(unsigned long long) g_mat_lsn);
			dump_physical(stderr, "sample", &sample);
			dump_metrics(stderr, &m);
		}
	}
	check(during_ok, "every physical category stays within its declared bound"
		  " while the workload continues (%d violating samples)",
		  during_violations);

	/* Quiesce: finish the branch, release readers, publish the final cutoff,
	 * and let every reclaimer catch up within its declared interval. */
	if (branch.state == 1)
	{
		check(op_begin_delete(branch.tl, branch.incarnation) == PS_STATUS_OK,
			  "begin deleting the final branch %u", branch.tl);
		branch.state = 2;
	}
	branch_wait_deleted();
	for (uint32_t i = 0; i < NREADERS; i++)
		if (readers[i].held)
		{
			reader_verify(&readers[i], "final");
			check(op_retention_drop(0, PS_RETENTION_OWNER_READER,
									readers[i].owner_id,
									readers[i].generation) == PS_STATUS_OK,
				  "final drop of reader %llu",
				  (unsigned long long) readers[i].owner_id);
			readers[i].held = 0;
			readers[i].generation++;
		}
	materialize();
	t0 = now_ns();
	for (;;)
	{
		read_metrics(&m);
		if (reclaimers_caught_up(&m))
		{
			catch_up_seconds = (double) (now_ns() - t0) / 1e9;
			break;
		}
		if (now_ns() - t0 > (uint64_t) CATCH_UP_SECONDS * 1000000000ull)
			break;
		sleep_ms(50);
	}
	check(catch_up_seconds >= 0,
		  "every reclaimer catches up within %d seconds after ingestion stops",
		  CATCH_UP_SECONDS);
	if (catch_up_seconds < 0)
		dump_metrics(stderr, &m);
	/* Let idle maintenance finish trailing GC before the quiescent measurement. */
	sleep_ms(1500);
	read_metrics(&m);
	quiescent_metrics = m;
	measure(&quiescent);
	quiescent_ok = physical_within(&quiescent, &quiescent_bound, "quiescent", 1);
	check(quiescent_ok, "quiescent footprint is within the steady-state bound");
	check(logical_wal_bytes <= quiescent_bound.wal ||
		  quiescent.wal < logical_wal_bytes,
		  "shipped WAL was reclaimed (%llu retained of %llu shipped)",
		  (unsigned long long) quiescent.wal, (unsigned long long) logical_wal_bytes);
	check(logical_page_bytes <= quiescent_bound.layers ||
		  quiescent.layers + quiescent.page < logical_page_bytes,
		  "page history was reclaimed (%llu retained of %llu written)",
		  (unsigned long long) (quiescent.layers + quiescent.page),
		  (unsigned long long) logical_page_bytes);
	check(!poisoned(&m), "final: no registry or manifest is poisoned");
	{
		int			rc = op_wal_retain_floor(0, &floor);

		check(rc == PS_STATUS_OK && floor != 0 && floor <= g_mat_lsn &&
			  floor + 4 * INTERVAL_WAL_BYTES >= g_mat_lsn,
			  "final WAL retention floor %llu follows the materializer %llu"
			  " within four intervals (status %d)",
			  (unsigned long long) floor, (unsigned long long) g_mat_lsn, rc);
	}
	verify_latest_all("final");

	/* Final restart pair: the quiescent store must recover to the same view. */
	restart_daemon(0);
	restart_daemon(1);
	verify_latest_all("final after restarts");

	read_metrics(&m);
	/* Controller wait/throttle counters are cumulative per daemon boot; report
	 * the ones observed before the final restart pair. */
	quiescent_metrics.owner_count = m.owner_count;
	quiescent_metrics.layer_count = m.layer_count;
	m = quiescent_metrics;
	write_report(stdout, rounds, seed, &max, &quiescent, &m, catch_up_seconds,
				 during_ok, quiescent_ok);
	if ((env = getenv("PAGESTORE_SOAK_REPORT")) != NULL)
	{
		FILE	   *f = fopen(env, "w");

		if (f != NULL)
		{
			write_report(f, rounds, seed, &max, &quiescent, &m, catch_up_seconds,
						 during_ok, quiescent_ok);
			fclose(f);
		}
	}
	dump_physical(stderr, "max", &max);
	dump_physical(stderr, "quiescent", &quiescent);
	dump_metrics(stderr, &m);

	stop_daemon_clean();
	if (!keep_store)
		remove_tree(store_dir);
	else
		fprintf(stderr, "store kept at %s\n", store_dir);
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
