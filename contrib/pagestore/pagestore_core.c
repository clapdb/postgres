/*-------------------------------------------------------------------------
 *
 * pagestore_core.c
 *	  Shared "brain" of the page-store daemon (see pagestore_core.h).
 *
 * Design (see also pagestore_ipc.h):
 *
 *	- Page-size agnostic.  The logical page size is configured with --page-size
 *	  (8192 for PostgreSQL, 16384 for InnoDB, ...) and published in the shm
 *	  header; nothing about the on-disk format assumes a particular value.
 *
 *	- Log-structured storage.  Every page write is appended to a growing
 *	  segment as a self-describing record [SegRecHdr | page bytes].  Writes are
 *	  therefore large and sequential regardless of how small individual logical
 *	  pages are.  Old versions are never overwritten, so the log is also the COW
 *	  history.  How the segments are physically stored is the storage backend's
 *	  business (pagestore_storage.h): files for POSIX, device regions for SPDK.
 *
 *	- Indirection map.  An in-memory index maps (timeline, key, block) -> a
 *	  chain of versions {lsn, segment, offset}.  This lets a single small
 *	  logical page be addressed inside a large physical segment (ranged read).
 *	  Startup rebuilds it from the durable image-layer prefix plus the uncovered
 *	  segment tail; SPDK, which does not yet use layers, scans all segments.
 *
 * This file holds everything backend- and loop-agnostic; each frontend (the
 * POSIX daemon, the SPDK daemon) supplies its own request loop and page byte
 * I/O.  Includes only pagestore_ipc.h/pagestore_storage.h and libc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_layer_store.h"
#include "pagestore_manifest.h"
#include "pagestore_memtable.h"
#include "pagestore_pgcache.h"
#include "pagestore_prune.h"
#include "pagestore_retention.h"
#include "pagestore_wal_store.h"
#include "pagestore_walidx_prune.h"
#include "pagestore_walidx_snapshot.h"
#include "pagestore_fault.h"
#include "pagestore_forkmeta_prune.h"
#include "pagestore_forkmeta_snapshot.h"

/* configuration, set by the frontend before ps_core_open() */
uint32_t	page_size = PS_DEFAULT_PAGE_SIZE;
uint64_t	segment_size = 8 * 1024 * 1024;
int			flush_pages = 256;	/* memtable flush threshold (pages) */
int			compact_layers = 8;	/* compact a timeline past this many image layers */
int			segment_gc_enabled = 1;
int			cache_pages = 1024;	/* materialized-page cache size (pages; 0=off) */
uint64_t	page_reclaim_high_water_bytes;
uint64_t	page_reclaim_catchup_bytes;
uint64_t	wal_reclaim_high_water_bytes;
uint64_t	wal_reclaim_catchup_bytes;
uint64_t	walidx_reclaim_high_water_bytes;
uint64_t	walidx_reclaim_catchup_bytes;
uint64_t	forkmeta_reclaim_high_water_bytes;
uint64_t	forkmeta_reclaim_catchup_bytes;
/*
 * Use the LSM read path: rebuild the index from image layers on restart and
 * (in the frontend) serve reads via read_resolve.  The POSIX daemon enables it;
 * the SPDK daemon leaves it off for now because its async read path serves pages
 * by segment offset (async layer reads are a later step), so it must keep the
 * segment-scan recovery that gives versions real segment locations.
 */
int			use_layers = 1;

static pthread_t tier_upload_thread;
static PsLayerDesc tier_upload_candidate;
static volatile int tier_upload_state; /* 0 idle, 1 running, 2 success, 3 failed */
static uint32_t tier_upload_shard_cursor;
static struct timespec tier_upload_retry_at;
static uint64_t tier_upload_layer_cursor[PS_MAX_CHANNELS];
static int tier_one_layer(void);
static int finish_upload(const PsLayerDesc *candidate);
static int map_locks_ready;
static const PsLayerLocation *tier_local_location(const PsLayerDesc *layer);
static int refresh_remote_only_layer(const PsLayerDesc *layer);
static int read_image_index_refreshing(const PsLayerDesc *layer,
									   PsImgIndexEnt **idx, uint32_t *n);
static int read_layer_block_refreshing(const PsLayerDesc *layer, uint64_t off,
									   void *buf, uint32_t len);
static int verify_image_layer_refreshing(const PsLayerDesc *layer);
static pthread_t gc_remote_thread;
static PsLayerDesc gc_remote_candidate;
static volatile int gc_remote_state; /* 0 idle, 1 running, 2 remote success, 3 failed */
static uint64_t gc_remote_layer_cursor;
static uint32_t gc_remote_map_cursor;
static struct timespec gc_remote_retry_at;
static pthread_t evict_local_thread;
static PsLayerDesc evict_local_candidate;
static volatile int evict_local_state; /* 0 idle, 1 verifying, 2 verified, 3 failed */
static uint32_t evict_local_map_cursor;
static int gc_finish_local(uint64_t layer_id, int remote_done);
static int finish_evict(const PsLayerDesc *candidate);
static void page_remove_compacted_versions(uint32_t timeline,
										   const PsImgRec *recs, uint32_t nrec);
static int retention_project_lsn(uint32_t descendant, uint32_t target,
								 uint64_t *lsn);
static int timeline_has_parent(uint32_t timeline);
static int timeline_delete_active(void);
static int timeline_delete_recovery_skip(void);
static int timeline_recovery_allowed(uint32_t timeline);
static int timeline_delete_wal_cleanup_one(void);
static int timeline_delete_page_cleanup_one(void);
static int timeline_delete_publish_ready(uint32_t timeline);
static int timeline_delete_publish_one(void);
static int branch_frontiers_allow(int parent, uint64_t branch_lsn);
static int page_prune_fences(uint32_t timeline, PsPruneFence **fences_out,
								 uint32_t *nfences_out);
static int walidx_prune_fences(uint32_t timeline, uint64_t **fences_out,
								   uint32_t *nfences_out);
static int retention_effective_floor(uint32_t timeline, uint32_t resource,
									 uint64_t *floor_out);
static int wal_reclaim_frontier_ancestry_allows(uint32_t timeline,
											uint64_t lsn);
static int wal_segment_reclaim_one(void);

/* the active storage backend (POSIX by default; the frontend may override) */
const PsStorage *ps_storage = &PsStoragePosix;

/* Forkmeta state is declared early because the append helpers precede the
 * detailed source-record definitions below. */
static int fork_meta_poisoned;
static uint64_t fork_meta_bytes;

static inline int
fork_meta_poisoned_load(void)
{
	return __atomic_load_n(&fork_meta_poisoned, __ATOMIC_ACQUIRE);
}

static inline void
fork_meta_poisoned_store(int value)
{
	__atomic_store_n(&fork_meta_poisoned, value, __ATOMIC_RELEASE);
}

static inline uint64_t
fork_meta_bytes_load(void)
{
	return __atomic_load_n(&fork_meta_bytes, __ATOMIC_RELAXED);
}

static inline void
fork_meta_bytes_store(uint64_t value)
{
	__atomic_store_n(&fork_meta_bytes, value, __ATOMIC_RELAXED);
}

static inline void
fork_meta_bytes_add(uint64_t value)
{
	(void) __atomic_fetch_add(&fork_meta_bytes, value, __ATOMIC_RELAXED);
}

/* configured logical shards for this daemon (set by frontend main before open()) */
uint32_t	ps_nshards = 1;

/* Durable identity for newly bound ordered segment records.  Recovery observes
 * every persisted identity before the daemon accepts writes, so allocation
 * continues above both committed and uncommitted records after a restart. */
static uint64_t next_segment_order_id = 1;
static uint64_t next_admission_seq = 1;
/* These static synchronization objects intentionally survive ps_core_open()
 * / ps_core_close() cycles.  Open/close must only run after callers have
 * released their sections; reinitializing the lock or turnstile counters
 * would invalidate an in-flight ownership record.  The rwlock is the
 * lifecycle gate.  The small turnstile around it closes the
 * POSIX rwlock's unspecified reader/writer scheduling gap: once a writer is
 * queued, new readers wait until that writer has acquired and released the
 * rwlock.  This keeps BEGIN_DELETE from being starved by a stream of requests
 * on platforms whose default pthread rwlock is reader-preferred. */
static pthread_rwlock_t lifecycle_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_mutex_t lifecycle_turnstile = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t lifecycle_turnstile_cond = PTHREAD_COND_INITIALIZER;
static uint32_t lifecycle_active_readers;
static uint32_t lifecycle_waiting_writers;
static int lifecycle_writer_active;
static pthread_rwlock_t admission_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_mutex_t admission_turnstile = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t admission_turnstile_cond = PTHREAD_COND_INITIALIZER;
static uint32_t admission_active_readers;
static uint32_t admission_waiting_writers;
static int admission_writer_active;
static PsForkmetaCutoverTestHook forkmeta_cutover_test_hook;
static void *forkmeta_cutover_test_hook_arg;
static PsForkmetaBaselineInitTestHook forkmeta_baseline_init_test_hook;
static void *forkmeta_baseline_init_test_hook_arg;
static PsAdmissionReadTestHook admission_read_test_hook;
static void *admission_read_test_hook_arg;
static PsLifecycleReadTestHook lifecycle_read_test_hook;
static void *lifecycle_read_test_hook_arg;
static PsTierUploadBeforePublishTestHook tier_upload_before_publish_test_hook;
static void *tier_upload_before_publish_test_hook_arg;
static PsLifecycleReadQueuedTestHook lifecycle_read_queued_test_hook;
static void *lifecycle_read_queued_test_hook_arg;
static PsLifecycleWriteQueuedTestHook lifecycle_write_queued_test_hook;
static void *lifecycle_write_queued_test_hook_arg;
static PsAdmissionWriteLockTestHook admission_write_lock_test_hook;
static void *admission_write_lock_test_hook_arg;
static PsAdmissionWriteQueuedTestHook admission_write_queued_test_hook;
static void *admission_write_queued_test_hook_arg;
static PsLifecycleWriteLockTestHook lifecycle_write_lock_test_hook;
static void *lifecycle_write_lock_test_hook_arg;
static PsWalReclaimAttemptTestHook wal_reclaim_attempt_test_hook;
static void *wal_reclaim_attempt_test_hook_arg;
static PsWalReclaimBeforeFloorTestHook wal_reclaim_before_floor_test_hook;
static void *wal_reclaim_before_floor_test_hook_arg;
static PsWalReadBeforeLockTestHook wal_read_before_lock_test_hook;
static void *wal_read_before_lock_test_hook_arg;
static PsWalIdxObservationErrorTestHook walidx_observation_error_test_hook;
static void *walidx_observation_error_test_hook_arg;
/* A page-history pin must not change between a compaction floor snapshot and
 * publication of the pruned replacement layer. */
static pthread_rwlock_t page_prune_lock = PTHREAD_RWLOCK_INITIALIZER;
/* WAL-index pins must stay fixed from replacement-chain planning through the
 * durable frontier and generation-manifest cutover. */
static pthread_rwlock_t walidx_prune_lock = PTHREAD_RWLOCK_INITIALIZER;
static PsShmHeader *metrics_header;

typedef struct PsBackpressureController
{
	uint64_t	high_water_bytes;
	uint64_t	catchup_bytes;
	uint64_t	lag_bytes;
	int		throttled;
	uint64_t	throttle_enters;
	uint64_t	throttle_exits;
	uint64_t	foreground_wait_ns;
} PsBackpressureController;

static pthread_mutex_t backpressure_lock = PTHREAD_MUTEX_INITIALIZER;
static PsBackpressureController page_backpressure;
static PsBackpressureController wal_backpressure;
static PsBackpressureController walidx_backpressure;
static PsBackpressureController forkmeta_backpressure;
#define MAX_TIMELINES	1024
static unsigned char walidx_snapshot_force_due[MAX_TIMELINES];
static unsigned char walidx_snapshot_gc_force_due[MAX_TIMELINES];
static uint64_t walidx_observation_next_ns;
static uint64_t walidx_observation_count;
#define WALIDX_AUTO_OBSERVATION_INTERVAL_NS UINT64_C(100000000)
static uint64_t forkmeta_observation_next_ns;
static uint64_t forkmeta_observation_count;
#define FORKMETA_AUTO_OBSERVATION_INTERVAL_NS UINT64_C(100000000)
static int backpressure_shutdown_requested;
static uint32_t backpressure_gate_mask;
static PsBackpressureSlowPathTestHook backpressure_slow_path_test_hook;
static void *backpressure_slow_path_test_hook_arg;
static char wal_segment_root[4096];

#define PS_BACKPRESSURE_GATE_PAGE_ENABLED	(1u << 0)
#define PS_BACKPRESSURE_GATE_WAL_ENABLED	(1u << 1)
#define PS_BACKPRESSURE_GATE_PAGE_THROTTLED	(1u << 2)
#define PS_BACKPRESSURE_GATE_WAL_THROTTLED	(1u << 3)
#define PS_BACKPRESSURE_GATE_WALIDX_ENABLED	(1u << 4)
#define PS_BACKPRESSURE_GATE_WALIDX_THROTTLED	(1u << 5)
#define PS_BACKPRESSURE_GATE_FORKMETA_ENABLED	(1u << 6)
#define PS_BACKPRESSURE_GATE_FORKMETA_THROTTLED	(1u << 7)
#define PS_BACKPRESSURE_GATE_THROTTLED_MASK \
	(PS_BACKPRESSURE_GATE_PAGE_THROTTLED | PS_BACKPRESSURE_GATE_WAL_THROTTLED | \
	 PS_BACKPRESSURE_GATE_WALIDX_THROTTLED | PS_BACKPRESSURE_GATE_FORKMETA_THROTTLED)

static uint64_t page_reclaim_lag_bytes(void);
static uint64_t wal_reclaim_lag_bytes(void);
static uint64_t walidx_reclaim_lag_bytes(unsigned char *tail_candidates,
									unsigned char *gc_candidates);
static uint64_t forkmeta_reclaim_lag_bytes(void);
static int fork_meta_backpressure_throttled(void);
static int admission_write_lock(void);
static void backpressure_publish_locked(void);
static void backpressure_update_locked(PsBackpressureController *controller,
										 uint64_t lag, uint64_t high,
										 uint64_t catchup);
static uint32_t core_shards(void);

#define PS_PAGE_FRONTIER_MAGIC 0x46504750U /* "PGPF" */
#define PS_PAGE_FRONTIER_VERSION 3
#define PS_PAGE_FRONTIER_SLOTS 2
typedef struct PsPageFrontierEntry
{
	uint64_t	incarnation;
	PsPruneFence fence;
} PsPageFrontierEntry;
typedef struct PsPageFrontierState
{
	uint32_t	magic;
	uint32_t	version;
	PsPageFrontierEntry entries[1024][PS_PAGE_FRONTIER_SLOTS];
	uint32_t	crc;
} PsPageFrontierState;
typedef struct PsPageFrontierStateV2
{
	uint32_t	magic;
	uint32_t	version;
	PsPruneFence frontiers[1024];
	uint32_t	crc;
} PsPageFrontierStateV2;
static char page_frontier_path[4096];
static char page_frontier_dir[4096];
static PsPageFrontierEntry page_reclaimed_frontier[1024][PS_PAGE_FRONTIER_SLOTS];
static int page_frontier_load(const char *store_dir);
static int page_frontier_advance(uint32_t timeline, uint64_t floor,
									uint64_t admission_seq);

#define PS_WALIDX_FRONTIER_MAGIC 0x46584957U /* "WIXF" */
#define PS_WALIDX_FRONTIER_VERSION 2
typedef struct PsWalIdxFrontierEntry
{
	uint64_t	incarnation;
	uint64_t	frontier;
} PsWalIdxFrontierEntry;
typedef struct PsWalIdxFrontierState
{
	uint32_t	magic;
	uint32_t	version;
	PsWalIdxFrontierEntry entries[1024][PS_PAGE_FRONTIER_SLOTS];
	uint32_t	crc;
} PsWalIdxFrontierState;
typedef struct PsWalIdxFrontierStateV1
{
	uint32_t	magic;
	uint32_t	version;
	uint64_t	frontiers[1024];
	uint32_t	crc;
} PsWalIdxFrontierStateV1;
static char walidx_frontier_path[4096];
static char walidx_frontier_dir[4096];
static PsWalIdxFrontierEntry walidx_reclaimed_frontier[1024][PS_PAGE_FRONTIER_SLOTS];
static int walidx_frontier_load(const char *store_dir);
static int walidx_frontier_advance(uint32_t timeline, uint64_t frontier);
static int walidx_frontier_ancestry_allows(uint32_t reader_timeline,
									   uint64_t read_lsn);
static int walidx_frontier_publication_pending(uint32_t timeline);
static int walidx_frontier_ancestry_pending(uint32_t reader_timeline);

static uint64_t
admission_seq_alloc(void)
{
	uint64_t	next = __atomic_load_n(&next_admission_seq, __ATOMIC_RELAXED);

	for (;;)
	{
		if (next == 0 || next == UINT64_MAX)
			return 0;
		if (__atomic_compare_exchange_n(&next_admission_seq, &next, next + 1,
								false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
			return next;
	}
}

static void
admission_seq_observe(uint64_t seq)
{
	uint64_t	next = __atomic_load_n(&next_admission_seq, __ATOMIC_RELAXED);

	while (next <= seq && next != UINT64_MAX &&
		   !__atomic_compare_exchange_n(&next_admission_seq, &next,
								 seq == UINT64_MAX ? UINT64_MAX : seq + 1,
									 false, __ATOMIC_RELAXED,
									 __ATOMIC_RELAXED))
		;
}

void
ps_lifecycle_read_lock(void)
{
	int rc;

	pthread_mutex_lock(&lifecycle_turnstile);
	if (lifecycle_read_queued_test_hook != NULL)
		lifecycle_read_queued_test_hook(lifecycle_read_queued_test_hook_arg);
	while (lifecycle_waiting_writers != 0 || lifecycle_writer_active)
		pthread_cond_wait(&lifecycle_turnstile_cond, &lifecycle_turnstile);
	lifecycle_active_readers++;
	pthread_mutex_unlock(&lifecycle_turnstile);

	rc = pthread_rwlock_rdlock(&lifecycle_lock);
	if (rc != 0)
	{
		pthread_mutex_lock(&lifecycle_turnstile);
		lifecycle_active_readers--;
		pthread_cond_broadcast(&lifecycle_turnstile_cond);
		pthread_mutex_unlock(&lifecycle_turnstile);
		/* A void lock API cannot safely report this to callers: continuing
		 * would execute an unprotected request and later unlock a lock that was
		 * never acquired.  Lock exhaustion or corruption is fatal instead. */
		abort();
	}
	if (lifecycle_read_test_hook != NULL)
		lifecycle_read_test_hook(lifecycle_read_test_hook_arg);
}

void
ps_lifecycle_read_unlock(void)
{
	pthread_rwlock_unlock(&lifecycle_lock);
	pthread_mutex_lock(&lifecycle_turnstile);
	if (lifecycle_active_readers == 0)
		abort();
	lifecycle_active_readers--;
	if (lifecycle_active_readers == 0)
		pthread_cond_broadcast(&lifecycle_turnstile_cond);
	pthread_mutex_unlock(&lifecycle_turnstile);
}

static void
ps_lifecycle_read_reserve(void)
{
	/* The caller must already own lifecycle-rd.  The reservation is a
	 * turnstile token, not another pthread rwlock read ownership. */
	pthread_mutex_lock(&lifecycle_turnstile);
	if (lifecycle_active_readers == UINT32_MAX)
	{
		pthread_mutex_unlock(&lifecycle_turnstile);
		abort();
	}
	lifecycle_active_readers++;
	pthread_mutex_unlock(&lifecycle_turnstile);
}

static void
ps_lifecycle_read_adopt_reserved(void)
{
	/* The parent already counted this reader before pthread_create().  The
	 * parent holds the actual pthread rwlock until the maintenance call
	 * returns; after that, lifecycle_active_readers is the authoritative
	 * reservation which keeps a writer out until this worker is done.  Taking
	 * another pthread rwlock read here would deadlock if a writer queued between
	 * pthread_create() and worker startup: the writer waits for this reservation
	 * while the worker waits for the writer. */
}

static void
lifecycle_drop_reserved(void)
{
	pthread_mutex_lock(&lifecycle_turnstile);
	if (lifecycle_active_readers == 0)
	{
		pthread_mutex_unlock(&lifecycle_turnstile);
		abort();
	}
	lifecycle_active_readers--;
	if (lifecycle_active_readers == 0)
		pthread_cond_broadcast(&lifecycle_turnstile_cond);
	pthread_mutex_unlock(&lifecycle_turnstile);
}

static void
ps_lifecycle_read_release_reserved(void)
{
	lifecycle_drop_reserved();
}

static void
ps_lifecycle_read_cancel_reservation(void)
{
	/* Called by the parent when pthread_create() failed; no rwlock ownership
	 * exists for a reservation that was never handed to a worker. */
	lifecycle_drop_reserved();
}

static int
lifecycle_write_lock_interruptible(const volatile sig_atomic_t *stop_flag)
{
	int rc;

	pthread_mutex_lock(&lifecycle_turnstile);
	if (stop_flag != NULL && *stop_flag)
	{
		pthread_mutex_unlock(&lifecycle_turnstile);
		return ECANCELED;
	}
	lifecycle_waiting_writers++;
	if (lifecycle_write_queued_test_hook != NULL)
		lifecycle_write_queued_test_hook(lifecycle_write_queued_test_hook_arg);
	while (lifecycle_writer_active || lifecycle_active_readers != 0)
	{
		struct timespec deadline;

		if (stop_flag != NULL && *stop_flag)
		{
			lifecycle_waiting_writers--;
			pthread_cond_broadcast(&lifecycle_turnstile_cond);
			pthread_mutex_unlock(&lifecycle_turnstile);
			return ECANCELED;
		}
		/* A signal handler only stores stop_requested.  The waiter notices it
		 * without requiring the handler to touch a pthread mutex or condvar. */
		if (clock_gettime(CLOCK_REALTIME, &deadline) != 0)
		{
			lifecycle_waiting_writers--;
			pthread_cond_broadcast(&lifecycle_turnstile_cond);
			pthread_mutex_unlock(&lifecycle_turnstile);
			return errno;
		}
		deadline.tv_nsec += 10000000L; /* 10ms stop-aware polling bound */
		if (deadline.tv_nsec >= 1000000000L)
		{
			deadline.tv_sec++;
			deadline.tv_nsec -= 1000000000L;
		}
		rc = pthread_cond_timedwait(&lifecycle_turnstile_cond,
										&lifecycle_turnstile, &deadline);
		if (rc != 0 && rc != ETIMEDOUT)
		{
			lifecycle_waiting_writers--;
			pthread_cond_broadcast(&lifecycle_turnstile_cond);
			pthread_mutex_unlock(&lifecycle_turnstile);
			return rc;
		}
	}
	lifecycle_waiting_writers--;
	lifecycle_writer_active = 1;
	pthread_mutex_unlock(&lifecycle_turnstile);

	if (lifecycle_write_lock_test_hook != NULL)
		rc = lifecycle_write_lock_test_hook(&lifecycle_lock,
											 lifecycle_write_lock_test_hook_arg);
	else
		rc = pthread_rwlock_wrlock(&lifecycle_lock);
	if (rc != 0)
	{
		pthread_mutex_lock(&lifecycle_turnstile);
		lifecycle_writer_active = 0;
		pthread_cond_broadcast(&lifecycle_turnstile_cond);
		pthread_mutex_unlock(&lifecycle_turnstile);
	}
	return rc;
}

static int
lifecycle_write_lock(void)
{
	return lifecycle_write_lock_interruptible(NULL);
}

int
ps_lifecycle_write_lock(void)
{
	return lifecycle_write_lock();
}

int
ps_lifecycle_write_lock_interruptible(const volatile sig_atomic_t *stop_flag)
{
	return lifecycle_write_lock_interruptible(stop_flag);
}

void
ps_lifecycle_write_unlock(void)
{
	pthread_rwlock_unlock(&lifecycle_lock);
	pthread_mutex_lock(&lifecycle_turnstile);
	lifecycle_writer_active = 0;
	pthread_cond_broadcast(&lifecycle_turnstile_cond);
	pthread_mutex_unlock(&lifecycle_turnstile);
}

void
ps_admission_read_lock(void)
{
	int rc;

	pthread_mutex_lock(&admission_turnstile);
	while (admission_waiting_writers != 0 || admission_writer_active)
		pthread_cond_wait(&admission_turnstile_cond, &admission_turnstile);
	if (admission_active_readers == UINT32_MAX)
	{
		pthread_mutex_unlock(&admission_turnstile);
		abort();
	}
	/* Reserve the reader before dropping the turnstile.  A writer which queues
	 * between this point and pthread_rwlock_rdlock() must still wait for us. */
	admission_active_readers++;
	pthread_mutex_unlock(&admission_turnstile);
	rc = pthread_rwlock_rdlock(&admission_lock);
	if (rc != 0)
	{
		pthread_mutex_lock(&admission_turnstile);
		admission_active_readers--;
		if (admission_active_readers == 0)
			pthread_cond_broadcast(&admission_turnstile_cond);
		pthread_mutex_unlock(&admission_turnstile);
		abort();
	}
	if (admission_read_test_hook != NULL)
		admission_read_test_hook(admission_read_test_hook_arg);
}

void
ps_admission_read_unlock(void)
{
	pthread_rwlock_unlock(&admission_lock);
	pthread_mutex_lock(&admission_turnstile);
	if (admission_active_readers == 0)
	{
		pthread_mutex_unlock(&admission_turnstile);
		abort();
	}
	admission_active_readers--;
	if (admission_active_readers == 0)
		pthread_cond_broadcast(&admission_turnstile_cond);
	pthread_mutex_unlock(&admission_turnstile);
}

static int
backpressure_stop_observed(const volatile sig_atomic_t *stop_flag)
{
	if (__atomic_load_n(&backpressure_shutdown_requested, __ATOMIC_ACQUIRE) ||
		(stop_flag != NULL && *stop_flag != 0))
		return 1;
	/* The signal handler only publishes STOPPING in shared memory.  The worker
	 * admission probe observes it without calling pthread APIs from the handler. */
	return metrics_header != NULL &&
		ps_load_acquire(&metrics_header->startup_state) == PS_SHM_STOPPING;
}

static void
backpressure_publish_locked(void)
{
	PsShmHeader *hdr = metrics_header;
	uint32_t gate_mask = 0;

	if (page_backpressure.high_water_bytes != 0)
		gate_mask |= PS_BACKPRESSURE_GATE_PAGE_ENABLED;
	if (wal_backpressure.high_water_bytes != 0)
		gate_mask |= PS_BACKPRESSURE_GATE_WAL_ENABLED;
	if (walidx_backpressure.high_water_bytes != 0)
		gate_mask |= PS_BACKPRESSURE_GATE_WALIDX_ENABLED;
	if (page_backpressure.throttled)
		gate_mask |= PS_BACKPRESSURE_GATE_PAGE_THROTTLED;
	if (wal_backpressure.throttled)
		gate_mask |= PS_BACKPRESSURE_GATE_WAL_THROTTLED;
	if (walidx_backpressure.throttled)
		gate_mask |= PS_BACKPRESSURE_GATE_WALIDX_THROTTLED;
	if (forkmeta_backpressure.high_water_bytes != 0)
		gate_mask |= PS_BACKPRESSURE_GATE_FORKMETA_ENABLED;
	if (forkmeta_backpressure.throttled)
		gate_mask |= PS_BACKPRESSURE_GATE_FORKMETA_THROTTLED;
	/* Publish controller state before advertising the corresponding fast-path
	 * mask.  Slow-path callers recheck authoritative state under the mutex. */
	__atomic_store_n(&backpressure_gate_mask, gate_mask, __ATOMIC_RELEASE);

	if (hdr == NULL)
		return;
	ps_fetch_add_u64(&hdr->backpressure_metrics_seq, 1);
	ps_store_release_u64(&hdr->page_backpressure.lag_bytes,
						 page_backpressure.lag_bytes);
	ps_store_release_u64(&hdr->page_backpressure.high_water_bytes,
						 page_backpressure.high_water_bytes);
	ps_store_release_u64(&hdr->page_backpressure.catchup_bytes,
						 page_backpressure.catchup_bytes);
	ps_store_release(&hdr->page_backpressure.throttled,
					 page_backpressure.throttled != 0);
	ps_store_release_u64(&hdr->page_backpressure.throttle_enters,
						 page_backpressure.throttle_enters);
	ps_store_release_u64(&hdr->page_backpressure.throttle_exits,
						 page_backpressure.throttle_exits);
	ps_store_release_u64(&hdr->page_backpressure.foreground_wait_ns,
						 page_backpressure.foreground_wait_ns);
	ps_store_release_u64(&hdr->wal_backpressure.lag_bytes,
						 wal_backpressure.lag_bytes);
	ps_store_release_u64(&hdr->wal_backpressure.high_water_bytes,
						 wal_backpressure.high_water_bytes);
	ps_store_release_u64(&hdr->wal_backpressure.catchup_bytes,
						 wal_backpressure.catchup_bytes);
	ps_store_release(&hdr->wal_backpressure.throttled,
					 wal_backpressure.throttled != 0);
	ps_store_release_u64(&hdr->wal_backpressure.throttle_enters,
						 wal_backpressure.throttle_enters);
	ps_store_release_u64(&hdr->wal_backpressure.throttle_exits,
						 wal_backpressure.throttle_exits);
	ps_store_release_u64(&hdr->wal_backpressure.foreground_wait_ns,
						 wal_backpressure.foreground_wait_ns);
	ps_store_release_u64(&hdr->walidx_backpressure.lag_bytes,
						 walidx_backpressure.lag_bytes);
	ps_store_release_u64(&hdr->walidx_backpressure.high_water_bytes,
						 walidx_backpressure.high_water_bytes);
	ps_store_release_u64(&hdr->walidx_backpressure.catchup_bytes,
						 walidx_backpressure.catchup_bytes);
	ps_store_release(&hdr->walidx_backpressure.throttled,
						 walidx_backpressure.throttled != 0);
	ps_store_release_u64(&hdr->walidx_backpressure.throttle_enters,
						 walidx_backpressure.throttle_enters);
	ps_store_release_u64(&hdr->walidx_backpressure.throttle_exits,
						 walidx_backpressure.throttle_exits);
	ps_store_release_u64(&hdr->walidx_backpressure.foreground_wait_ns,
						 walidx_backpressure.foreground_wait_ns);
	ps_store_release_u64(&hdr->forkmeta_backpressure.lag_bytes,
						 forkmeta_backpressure.lag_bytes);
	ps_store_release_u64(&hdr->forkmeta_backpressure.high_water_bytes,
						 forkmeta_backpressure.high_water_bytes);
	ps_store_release_u64(&hdr->forkmeta_backpressure.catchup_bytes,
						 forkmeta_backpressure.catchup_bytes);
	ps_store_release(&hdr->forkmeta_backpressure.throttled,
						 forkmeta_backpressure.throttled != 0);
	ps_store_release_u64(&hdr->forkmeta_backpressure.throttle_enters,
						 forkmeta_backpressure.throttle_enters);
	ps_store_release_u64(&hdr->forkmeta_backpressure.throttle_exits,
						 forkmeta_backpressure.throttle_exits);
	ps_store_release_u64(&hdr->forkmeta_backpressure.foreground_wait_ns,
						 forkmeta_backpressure.foreground_wait_ns);
	ps_fetch_add_u64(&hdr->backpressure_metrics_seq, 1);
}

int
ps_backpressure_configure(uint64_t page_high_water,
						  uint64_t page_catchup,
						  uint64_t wal_high_water,
						  uint64_t wal_catchup)

{
	return ps_backpressure_configure_all(page_high_water, page_catchup,
									 wal_high_water, wal_catchup, 0, 0);
}

int
ps_backpressure_configure_all(uint64_t page_high_water,
							  uint64_t page_catchup,
							  uint64_t wal_high_water,
							  uint64_t wal_catchup,
								  uint64_t walidx_high_water,
								  uint64_t walidx_catchup)
{
	return ps_backpressure_configure_all_with_forkmeta(page_high_water,
		page_catchup, wal_high_water, wal_catchup, walidx_high_water,
		walidx_catchup, 0, 0);
}

int
ps_backpressure_configure_all_with_forkmeta(uint64_t page_high_water,
										uint64_t page_catchup,
										uint64_t wal_high_water,
										uint64_t wal_catchup,
										uint64_t walidx_high_water,
										uint64_t walidx_catchup,
										uint64_t forkmeta_high_water,
										uint64_t forkmeta_catchup)
{
	if ((page_high_water == 0 && page_catchup != 0) ||
		(page_high_water != 0 && page_catchup >= page_high_water) ||
		(page_high_water != 0 && !segment_gc_enabled) ||
		(wal_high_water == 0 && wal_catchup != 0) ||
		(wal_high_water != 0 && wal_catchup >= wal_high_water) ||
		(walidx_high_water == 0 && walidx_catchup != 0) ||
		(walidx_high_water != 0 && walidx_catchup >= walidx_high_water) ||
		(forkmeta_high_water == 0 && forkmeta_catchup != 0) ||
		(forkmeta_high_water != 0 && forkmeta_catchup >= forkmeta_high_water))
	{
		errno = EINVAL;
		return -1;
	}
	page_reclaim_high_water_bytes = page_high_water;
	page_reclaim_catchup_bytes = page_catchup;
	wal_reclaim_high_water_bytes = wal_high_water;
	wal_reclaim_catchup_bytes = wal_catchup;
	walidx_reclaim_high_water_bytes = walidx_high_water;
	walidx_reclaim_catchup_bytes = walidx_catchup;
	forkmeta_reclaim_high_water_bytes = forkmeta_high_water;
	forkmeta_reclaim_catchup_bytes = forkmeta_catchup;
	__atomic_store_n(&forkmeta_observation_next_ns, 0, __ATOMIC_RELEASE);
	__atomic_store_n(&forkmeta_observation_count, 0, __ATOMIC_RELEASE);
	pthread_mutex_lock(&backpressure_lock);
	memset(&page_backpressure, 0, sizeof(page_backpressure));
	memset(&wal_backpressure, 0, sizeof(wal_backpressure));
	memset(&walidx_backpressure, 0, sizeof(walidx_backpressure));
	memset(&forkmeta_backpressure, 0, sizeof(forkmeta_backpressure));
	page_backpressure.high_water_bytes = page_high_water;
	page_backpressure.catchup_bytes = page_catchup;
	wal_backpressure.high_water_bytes = wal_high_water;
	wal_backpressure.catchup_bytes = wal_catchup;
	walidx_backpressure.high_water_bytes = walidx_high_water;
	walidx_backpressure.catchup_bytes = walidx_catchup;
	forkmeta_backpressure.high_water_bytes = forkmeta_high_water;
	forkmeta_backpressure.catchup_bytes = forkmeta_catchup;
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		__atomic_store_n(&walidx_snapshot_force_due[tl], 0, __ATOMIC_RELEASE);
		__atomic_store_n(&walidx_snapshot_gc_force_due[tl], 0, __ATOMIC_RELEASE);
	}
	__atomic_store_n(&walidx_observation_next_ns, 0, __ATOMIC_RELEASE);
	__atomic_store_n(&backpressure_shutdown_requested, 0, __ATOMIC_RELEASE);
	backpressure_publish_locked();
	pthread_mutex_unlock(&backpressure_lock);
	return 0;
}

int
ps_backpressure_try_admit_mask(const volatile sig_atomic_t *stop_flag,
								 uint32_t controller_mask,
								 uint32_t *cause_mask)
{
	uint32_t causes = 0;
	uint32_t gate_mask;

	/* The disabled/no-throttle path must stay out of the process-wide mutex.
	 * STOPPING is checked first so disabling the controllers cannot hide
	 * shutdown from a deferred foreground request. */
	if (backpressure_stop_observed(stop_flag))
	{
		if (cause_mask != NULL)
			*cause_mask = 0;
		return -1;
	}
	gate_mask = __atomic_load_n(&backpressure_gate_mask, __ATOMIC_ACQUIRE);
	if ((gate_mask & (PS_BACKPRESSURE_GATE_THROTTLED_MASK &
						  (controller_mask == PS_BACKPRESSURE_ALL ? UINT32_MAX :
						   ((controller_mask & PS_BACKPRESSURE_PAGE) ?
							PS_BACKPRESSURE_GATE_PAGE_THROTTLED : 0) |
						   ((controller_mask & PS_BACKPRESSURE_WAL) ?
							PS_BACKPRESSURE_GATE_WAL_THROTTLED : 0) |
						   ((controller_mask & PS_BACKPRESSURE_WALIDX) ?
							PS_BACKPRESSURE_GATE_WALIDX_THROTTLED : 0) |
						   ((controller_mask & PS_BACKPRESSURE_FORKMETA) ?
							PS_BACKPRESSURE_GATE_FORKMETA_THROTTLED : 0)))) == 0)
	{
		if (cause_mask != NULL)
			*cause_mask = 0;
		return 1;
	}
	if (backpressure_slow_path_test_hook != NULL)
		backpressure_slow_path_test_hook(backpressure_slow_path_test_hook_arg);

	pthread_mutex_lock(&backpressure_lock);
	if (backpressure_stop_observed(stop_flag))
	{
		pthread_mutex_unlock(&backpressure_lock);
		if (cause_mask != NULL)
			*cause_mask = 0;
		return -1;
	}
	if (page_backpressure.throttled)
		causes |= PS_BACKPRESSURE_PAGE;
	if (wal_backpressure.throttled)
		causes |= PS_BACKPRESSURE_WAL;
	if (walidx_backpressure.throttled)
		causes |= PS_BACKPRESSURE_WALIDX;
	if (forkmeta_backpressure.throttled)
		causes |= PS_BACKPRESSURE_FORKMETA;
	causes &= controller_mask;
	pthread_mutex_unlock(&backpressure_lock);
	if (cause_mask != NULL)
		*cause_mask = causes;
	return causes == 0 ? 1 : 0;
}

int
ps_backpressure_try_admit(const volatile sig_atomic_t *stop_flag,
						  uint32_t *cause_mask)
{
	return ps_backpressure_try_admit_mask(stop_flag, PS_BACKPRESSURE_ALL,
										  cause_mask);
}

void
ps_backpressure_record_wait(uint64_t page_wait_ns, uint64_t wal_wait_ns)
{
	ps_backpressure_record_wait4(page_wait_ns, wal_wait_ns, 0, 0);
}

void
ps_backpressure_record_wait3(uint64_t page_wait_ns, uint64_t wal_wait_ns,
								 uint64_t walidx_wait_ns)
{
	ps_backpressure_record_wait4(page_wait_ns, wal_wait_ns, walidx_wait_ns, 0);
}

void
ps_backpressure_record_wait4(uint64_t page_wait_ns, uint64_t wal_wait_ns,
							 uint64_t walidx_wait_ns, uint64_t forkmeta_wait_ns)
{
	pthread_mutex_lock(&backpressure_lock);
	if (page_wait_ns != 0)
		page_backpressure.foreground_wait_ns =
			UINT64_MAX - page_backpressure.foreground_wait_ns < page_wait_ns ?
			UINT64_MAX : page_backpressure.foreground_wait_ns + page_wait_ns;
	if (wal_wait_ns != 0)
		wal_backpressure.foreground_wait_ns =
			UINT64_MAX - wal_backpressure.foreground_wait_ns < wal_wait_ns ?
			UINT64_MAX : wal_backpressure.foreground_wait_ns + wal_wait_ns;
	if (walidx_wait_ns != 0)
		walidx_backpressure.foreground_wait_ns =
			UINT64_MAX - walidx_backpressure.foreground_wait_ns < walidx_wait_ns ?
			UINT64_MAX : walidx_backpressure.foreground_wait_ns + walidx_wait_ns;
	if (forkmeta_wait_ns != 0)
		forkmeta_backpressure.foreground_wait_ns =
			UINT64_MAX - forkmeta_backpressure.foreground_wait_ns < forkmeta_wait_ns ?
			UINT64_MAX : forkmeta_backpressure.foreground_wait_ns + forkmeta_wait_ns;
	if (page_wait_ns != 0 || wal_wait_ns != 0 || walidx_wait_ns != 0 ||
		forkmeta_wait_ns != 0)
		backpressure_publish_locked();
	pthread_mutex_unlock(&backpressure_lock);
}

void
ps_backpressure_shutdown(void)
{
	pthread_mutex_lock(&backpressure_lock);
	__atomic_store_n(&backpressure_shutdown_requested, 1, __ATOMIC_RELEASE);
	pthread_mutex_unlock(&backpressure_lock);
}

void
ps_test_backpressure_set_lag(uint64_t page_lag, uint64_t wal_lag)
{
	pthread_mutex_lock(&backpressure_lock);
	backpressure_update_locked(&page_backpressure, page_lag,
							   page_reclaim_high_water_bytes,
							   page_reclaim_catchup_bytes);
	backpressure_update_locked(&wal_backpressure, wal_lag,
							   wal_reclaim_high_water_bytes,
							   wal_reclaim_catchup_bytes);
	backpressure_publish_locked();
	pthread_mutex_unlock(&backpressure_lock);
}

void
ps_test_backpressure_set_walidx_lag(uint64_t walidx_lag)
{
	pthread_mutex_lock(&backpressure_lock);
	backpressure_update_locked(&walidx_backpressure, walidx_lag,
							   walidx_reclaim_high_water_bytes,
							   walidx_reclaim_catchup_bytes);
	backpressure_publish_locked();
	pthread_mutex_unlock(&backpressure_lock);
}

void
ps_test_backpressure_set_forkmeta_lag(uint64_t forkmeta_lag)
{
	pthread_mutex_lock(&backpressure_lock);
	backpressure_update_locked(&forkmeta_backpressure, forkmeta_lag,
							   forkmeta_reclaim_high_water_bytes,
							   forkmeta_reclaim_catchup_bytes);
	backpressure_publish_locked();
	pthread_mutex_unlock(&backpressure_lock);
}

int
ps_test_forkmeta_force_due(void)
{
	return fork_meta_backpressure_throttled();
}

int
ps_test_walidx_force_due(uint32_t timeline)
{
	return timeline < MAX_TIMELINES ?
		__atomic_load_n(&walidx_snapshot_force_due[timeline], __ATOMIC_ACQUIRE) : 0;
}

int
ps_test_walidx_gc_force_due(uint32_t timeline)
{
	return timeline < MAX_TIMELINES ?
		__atomic_load_n(&walidx_snapshot_gc_force_due[timeline],
							__ATOMIC_ACQUIRE) : 0;
}

uint64_t
ps_test_backpressure_walidx_observation_count(void)
{
	return __atomic_load_n(&walidx_observation_count, __ATOMIC_ACQUIRE);
}

uint64_t
ps_test_backpressure_forkmeta_observation_count(void)
{
	return __atomic_load_n(&forkmeta_observation_count, __ATOMIC_ACQUIRE);
}

void
ps_test_set_backpressure_slow_path_hook(PsBackpressureSlowPathTestHook hook,
									 void *arg)
{
	backpressure_slow_path_test_hook = hook;
	backpressure_slow_path_test_hook_arg = arg;
}

void
ps_test_set_forkmeta_cutover_hook(PsForkmetaCutoverTestHook hook, void *arg)
{
	forkmeta_cutover_test_hook = hook;
	forkmeta_cutover_test_hook_arg = arg;
}

void
ps_test_set_forkmeta_baseline_init_hook(PsForkmetaBaselineInitTestHook hook,
											 void *arg)
{
	forkmeta_baseline_init_test_hook = hook;
	forkmeta_baseline_init_test_hook_arg = arg;
}

void
ps_test_set_admission_read_hook(PsAdmissionReadTestHook hook, void *arg)
{
	admission_read_test_hook = hook;
	admission_read_test_hook_arg = arg;
}

void
ps_test_set_lifecycle_read_hook(PsLifecycleReadTestHook hook, void *arg)
{
	lifecycle_read_test_hook = hook;
	lifecycle_read_test_hook_arg = arg;
}

void
ps_test_set_tier_upload_before_publish_hook(
	PsTierUploadBeforePublishTestHook hook, void *arg)
{
	tier_upload_before_publish_test_hook = hook;
	tier_upload_before_publish_test_hook_arg = arg;
}

void
ps_test_set_lifecycle_read_queued_hook(PsLifecycleReadQueuedTestHook hook,
									   void *arg)
{
	lifecycle_read_queued_test_hook = hook;
	lifecycle_read_queued_test_hook_arg = arg;
}

void
ps_test_set_lifecycle_write_queued_hook(PsLifecycleWriteQueuedTestHook hook,
									void *arg)
{
	lifecycle_write_queued_test_hook = hook;
	lifecycle_write_queued_test_hook_arg = arg;
}

void
ps_test_set_admission_write_lock_hook(PsAdmissionWriteLockTestHook hook,
									  void *arg)
{
	admission_write_lock_test_hook = hook;
	admission_write_lock_test_hook_arg = arg;
}

void
ps_test_set_admission_write_queued_hook(PsAdmissionWriteQueuedTestHook hook,
										void *arg)
{
	admission_write_queued_test_hook = hook;
	admission_write_queued_test_hook_arg = arg;
}

void
ps_test_set_lifecycle_write_lock_hook(PsLifecycleWriteLockTestHook hook,
									  void *arg)
{
	lifecycle_write_lock_test_hook = hook;
	lifecycle_write_lock_test_hook_arg = arg;
}

void
ps_test_set_wal_reclaim_attempt_hook(PsWalReclaimAttemptTestHook hook,
									 void *arg)
{
	wal_reclaim_attempt_test_hook = hook;
	wal_reclaim_attempt_test_hook_arg = arg;
}

void
ps_test_set_wal_reclaim_before_floor_hook(
	PsWalReclaimBeforeFloorTestHook hook, void *arg)
{
	wal_reclaim_before_floor_test_hook = hook;
	wal_reclaim_before_floor_test_hook_arg = arg;
}

void
ps_test_set_wal_read_before_lock_hook(PsWalReadBeforeLockTestHook hook,
									  void *arg)
{
	wal_read_before_lock_test_hook = hook;
	wal_read_before_lock_test_hook_arg = arg;
}

void
ps_test_set_walidx_observation_error_hook(
	PsWalIdxObservationErrorTestHook hook, void *arg)
{
	walidx_observation_error_test_hook = hook;
	walidx_observation_error_test_hook_arg = arg;
}

static int
admission_write_lock(void)
{
	int rc;

	pthread_mutex_lock(&admission_turnstile);
	admission_waiting_writers++;
	if (admission_write_queued_test_hook != NULL)
		admission_write_queued_test_hook(admission_write_queued_test_hook_arg);
	if (admission_write_lock_test_hook != NULL)
	{
		/* Preserve the hook's historical meaning: it replaces the blocking
		 * pthread rwlock call, so tests can observe that call while readers are
		 * still active.  Mark the writer active before dropping the turnstile;
		 * otherwise a new reader could pass while the hook is blocked. */
		while (admission_writer_active)
			pthread_cond_wait(&admission_turnstile_cond, &admission_turnstile);
		admission_waiting_writers--;
		admission_writer_active = 1;
		pthread_mutex_unlock(&admission_turnstile);
		rc = admission_write_lock_test_hook(&admission_lock,
										 admission_write_lock_test_hook_arg);
	}
	else
	{
		while (admission_writer_active || admission_active_readers != 0)
			pthread_cond_wait(&admission_turnstile_cond, &admission_turnstile);
		admission_waiting_writers--;
		admission_writer_active = 1;
		pthread_mutex_unlock(&admission_turnstile);
		rc = pthread_rwlock_wrlock(&admission_lock);
	}
	if (rc != 0)
	{
		pthread_mutex_lock(&admission_turnstile);
		admission_writer_active = 0;
		pthread_cond_broadcast(&admission_turnstile_cond);
		pthread_mutex_unlock(&admission_turnstile);
	}
	return rc;
}

int
ps_admission_write_lock(void)
{
	return admission_write_lock();
}

void
ps_admission_write_unlock(void)
{
	pthread_rwlock_unlock(&admission_lock);
	pthread_mutex_lock(&admission_turnstile);
	if (!admission_writer_active)
	{
		pthread_mutex_unlock(&admission_turnstile);
		abort();
	}
	admission_writer_active = 0;
	pthread_cond_broadcast(&admission_turnstile_cond);
	pthread_mutex_unlock(&admission_turnstile);
}

uint64_t
ps_admission_barrier(void)
{
	uint64_t	seq;

	if (admission_write_lock() != 0)
		return 0;
	seq = admission_seq_alloc();
	if (seq != 0 && ps_retention_reserve_admission_seq(seq) != 0)
		seq = 0;
	ps_admission_write_unlock();
	return seq;
}

static uint64_t
segment_order_id_alloc(void)
{
	return __atomic_fetch_add(&next_segment_order_id, 1, __ATOMIC_RELAXED);
}

static void
segment_order_id_observe(uint64_t order_id)
{
	uint64_t	next = __atomic_load_n(&next_segment_order_id, __ATOMIC_RELAXED);

	while (next <= order_id &&
		   !__atomic_compare_exchange_n(&next_segment_order_id, &next,
										 order_id + 1, false,
										 __ATOMIC_RELAXED, __ATOMIC_RELAXED))
		;
}

/*
 * Per-shard state (step 4 target in practice): one thread per shard owns index
 * / staging / cache lock-free; the shard is chosen from the logical key only
 * (block- and timeline-independent), so a key's blocks and all its timelines
 * stay on one shard.
 */
#define IDX_BUCKETS		(1 << 16)
#define IDX_MASK		(IDX_BUCKETS - 1)

struct PageEnt;
struct ForkEnt;
struct WalIdxEnt;

#define MAX_SHARDS		PS_MAX_CHANNELS
#define LAYER_ID_SHARD_BITS	16
#define LAYER_ID_LOCAL_BITS	(64 - LAYER_ID_SHARD_BITS)
#define LAYER_ID_SHARD_MASK	((uint64_t) (((uint64_t) 1 << LAYER_ID_SHARD_BITS) - 1) << LAYER_ID_LOCAL_BITS)
#define LAYER_ID_LOCAL_MASK	((uint64_t) ((1ULL << LAYER_ID_LOCAL_BITS) - 1))

typedef struct Shard
{
	struct PageEnt *page_idx[IDX_BUCKETS];	/* (timeline,key,block) -> versions */
	struct ForkEnt *fork_idx[IDX_BUCKETS];	/* (timeline,key) -> fork size */
	struct WalIdxEnt *walidx[IDX_BUCKETS];	/* (timeline,key,block) -> WAL lsns */
	PsMemtable *memtable;		/* staging -> image layers */
	uint32_t	id;					/* shard id [0..ps_nshards) this state belongs to */
	int			cur_seg;			/* segment id for append cursor */
	uint64_t	cur_off;			/* append cursor byte offset within cur_seg */
	PsFlushWatermark flush_watermark;
	uint32_t	gc_next_seg;		/* oldest segment not yet reclaimed */
	uint64_t	gc_debt_segments;	/* existing, nonempty covered segments */
	uint32_t	gc_pending_remove_seg;	/* victim whose remove result is ambiguous */
	int			gc_pending_remove;
	int			gc_storage_error;	/* sticky fail-closed storage observation */
	int			flush_watermark_valid;
	int			coverage_broken;	/* a record was not staged; do not advance */
	uint64_t	next_layer_id;		/* next layer-local id for this shard */
	uint64_t	rr_mem,			/* read-source counters */
				rr_layer,
				rr_seg;
} Shard;

static Shard g_shards[MAX_SHARDS];

static uint64_t
backpressure_saturating_add(uint64_t left, uint64_t right)
{
	return UINT64_MAX - left < right ? UINT64_MAX : left + right;
}

/* Settle one physical PAGE-debt unit.  A pending remove identifies the only
 * victim whose result is ambiguous; matching it here also clears that state,
 * so a later remove/ENOENT observation cannot settle the same (shard, seg)
 * twice.  Callers without a pending remove must explicitly prove that the
 * segment was counted before asking to settle it. */
static void
page_gc_debt_settle(Shard *s, uint32_t seg, int known_counted)
{
	int pending = s->gc_pending_remove &&
		s->gc_pending_remove_seg == seg;

	if (!pending && !known_counted)
		return;
	if (s->gc_debt_segments != 0)
		s->gc_debt_segments--;
	if (pending)
		s->gc_pending_remove = 0;
}

/* Only complete segments already covered by a durable flush watermark are
 * debt.  Live versions/retained bytes in the boundary segment are not. */
static uint64_t
page_reclaim_lag_bytes(void)
{
	uint64_t lag = 0;

	for (uint32_t shard = 0; shard < core_shards(); shard++)
	{
		uint64_t segments;

		ps_lock_shard_rd(shard);
		/* The count is rebuilt once at startup and maintained by successful GC;
		 * controller refresh therefore remains O(number of shards), independent
		 * of the historical segment-id span.  Any runtime storage error is sticky
		 * and reports fail-closed lag rather than clearing an active throttle. */
		if (g_shards[shard].gc_storage_error)
		{
			ps_unlock_shard(shard);
			return UINT64_MAX;
		}
		segments = g_shards[shard].gc_debt_segments;
		ps_unlock_shard(shard);
		if (segment_size != 0 && segments > UINT64_MAX / segment_size)
			return UINT64_MAX;
		lag = backpressure_saturating_add(lag, segments * segment_size);
	}
	return lag;
}

/* Rebuild the process-local GC cursor and the incremental debt count from the
 * durable watermark.  This is a startup-only scan; refreshes use the count.
 * Zero-length files are present cursor entries but do not represent debt. */
static int
rebuild_page_gc_state(Shard *s)
{
	uint32_t boundary;
	int found = 0;

	s->gc_next_seg = 0;
	s->gc_debt_segments = 0;
	s->gc_pending_remove_seg = 0;
	s->gc_pending_remove = 0;
	s->gc_storage_error = 0;
	if (!s->flush_watermark_valid || ps_storage == NULL ||
		ps_storage->seg_size == NULL)
		return 0;
	boundary = s->flush_watermark.seg_id;
	s->gc_next_seg = boundary;
	for (uint32_t seg = 0; seg < boundary; seg++)
	{
		int64_t bytes;

		errno = 0;
		bytes = ps_storage->seg_size(s->id, (int) seg);
		if (bytes < 0)
		{
			if (errno == ENOENT)
				continue;
			if (errno == 0)
				errno = EIO;
			return -1;
		}
		if (!found)
		{
			s->gc_next_seg = seg;
			found = 1;
		}
		if (bytes > 0)
			s->gc_debt_segments = backpressure_saturating_add(
				s->gc_debt_segments, 1);
	}
	return 0;
}

/* Add only physical segments newly covered by a durable watermark.  The
 * watermark is an id boundary, not proof that every id below it has a file:
 * POSIX stores may be sparse and an empty file is not reclaimable debt.  This
 * is incremental (newly covered ids only); startup is the only historical
 * scan, and refresh remains O(number of shards). */
static void
account_page_gc_coverage(Shard *s, uint32_t old_boundary,
						 uint32_t new_boundary)
{
	if (ps_storage == NULL || ps_storage->seg_size == NULL)
		return;
	for (uint32_t seg = old_boundary; seg < new_boundary; seg++)
	{
		int64_t bytes;

		errno = 0;
		bytes = ps_storage->seg_size(s->id, (int) seg);
		if (bytes < 0)
		{
			if (errno == ENOENT)
				continue;
			if (errno == 0)
				errno = EIO;
			/* The manifest watermark is already durable.  Do not invent a
			 * count after an uncertain size observation; the sticky error
			 * makes the controller fail closed until the next open. */
			s->gc_storage_error = 1;
			return;
		}
		if (bytes > 0)
			s->gc_debt_segments = backpressure_saturating_add(
				s->gc_debt_segments, 1);
	}
}

/*
 * Concurrency.  A per-shard rwlock guards each shard's in-memory state
 * (g_shards[i]: the page/fork/walidx indexes, memtable, append cursor and
 * layer-id cursor).  A single map_lock guards the cross-shard state: the global
 * ps_layer_map and the timelines[] array.  Runtime paths add the lifecycle
 * gate and admission fence outside this existing order: lifecycle -> admission
 * -> shard/page/walidx -> map.  Lock order is otherwise always shard
 * (ascending shard id when taking more than one) then map (inner), never the
 * reverse, so there is no deadlock.
 *
 * Each daemon worker owns exactly one shard and only ever touches its own
 * g_shards[]; the maintenance controller takes one shard for compaction or all
 * shards for physical-segment rebinding, then map_lock.  Reads take shard-rd +
 * map-rd; ordinary writes take only shard-wr, escalating to a brief map-wr
 * inside append_page when a flush mutates the map; branch
 * creation takes map-wr alone.
 */
static pthread_rwlock_t shard_locks[MAX_SHARDS];
static pthread_rwlock_t map_lock = PTHREAD_RWLOCK_INITIALIZER;

void
ps_lock_shard_rd(uint32_t shard)
{
	pthread_rwlock_rdlock(&shard_locks[shard]);
}

void
ps_lock_shard_wr(uint32_t shard)
{
	pthread_rwlock_wrlock(&shard_locks[shard]);
}

void
ps_unlock_shard(uint32_t shard)
{
	pthread_rwlock_unlock(&shard_locks[shard]);
}

void
ps_lock_map_rd(void)
{
	pthread_rwlock_rdlock(&map_lock);
}

void
ps_lock_map_wr(void)
{
	pthread_rwlock_wrlock(&map_lock);
}

void
ps_unlock_map(void)
{
	pthread_rwlock_unlock(&map_lock);
}

static uint32_t
core_shards(void)
{
	if (ps_nshards == 0)
		return 1;
	return ps_nshards > PS_MAX_CHANNELS ? PS_MAX_CHANNELS : ps_nshards;
}

static uint32_t
layer_shard_from_id(uint64_t layer_id)
{
	return (uint32_t) ((layer_id & LAYER_ID_SHARD_MASK) >>
					   LAYER_ID_LOCAL_BITS);
}

static uint64_t
layer_local_id(uint64_t layer_id)
{
	return layer_id & LAYER_ID_LOCAL_MASK;
}

static int
layer_matches_read_shard(const PsLayerDesc *layer, uint32_t shard)
{
	uint32_t	layer_shard = layer_shard_from_id(layer->layer_id);

	return layer_shard == shard ||
		(layer->legacy_shard_zero && layer_shard == 0 && shard != 0);
}

static int
store_shard_count_path(const char *store_dir, char *path, size_t path_len)
{
	int			n;

	n = snprintf(path, path_len, "%s/.pagestore-nshards", store_dir);
	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

static int
fsync_dir_path(const char *path)
{
	int			fd = open(path, O_RDONLY | O_DIRECTORY);
	int			rc = 0;

	if (fd < 0)
		return -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

static int
publish_store_shard_count(const char *store_dir)
{
	char		path[4096];
	char		tmp[4096];
	FILE	   *f;
	uint32_t	current = core_shards();
	int			n;

	if (store_shard_count_path(store_dir, path, sizeof(path)) != 0)
		return -1;
	n = snprintf(tmp, sizeof(tmp), "%s.tmp.%ld", path, (long) getpid());
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;
	f = fopen(tmp, "w");
	if (f == NULL)
		return -1;
	if (fprintf(f, "%u\n", current) < 0 || fflush(f) != 0 ||
		fsync(fileno(f)) != 0)
	{
		fclose(f);
		unlink(tmp);
		return -1;
	}
	if (fclose(f) != 0)
	{
		unlink(tmp);
		return -1;
	}
	if (rename(tmp, path) != 0)
	{
		unlink(tmp);
		return -1;
	}
	if (fsync_dir_path(store_dir) != 0)
		return -1;
	return 0;
}

static int
parse_segment_file_shard(const char *name, uint32_t *shard)
{
	const char *p;
	char	   *end;
	unsigned long first;

	if (strncmp(name, "seg_", 4) != 0)
		return 0;
	p = name + 4;
	errno = 0;
	first = strtoul(p, &end, 10);
	if (end == p || errno != 0 || first > UINT32_MAX)
		return 0;
	if (*end == '\0')
	{
		*shard = 0;
		return 1;
	}
	if (*end != '_')
		return 0;
	p = end + 1;
	errno = 0;
	(void) strtoul(p, &end, 10);
	if (end == p || errno != 0 || *end != '\0')
		return 0;
	*shard = (uint32_t) first;
	return 1;
}

static int
infer_segment_shard_count(const char *store_dir, uint32_t *inferred)
{
	DIR		   *dir;
	struct dirent *ent;
	uint32_t	found = *inferred;

	dir = opendir(store_dir);
	if (dir == NULL)
		return -1;
	while ((ent = readdir(dir)) != NULL)
	{
		uint32_t	shard;

		if (parse_segment_file_shard(ent->d_name, &shard))
		{
			if (shard >= PS_MAX_CHANNELS)
				found = PS_MAX_CHANNELS + 1;
			else if (shard + 1 > found)
				found = shard + 1;
		}
	}
	if (closedir(dir) != 0)
		return -1;
	*inferred = found;
	return 0;
}

static int
validate_store_shard_count(const char *store_dir, int *publish_needed)
{
	char		path[4096];
	FILE	   *f;
	uint32_t	current = core_shards();
	uint32_t	persisted = 0;
	uint32_t	inferred = 1;
	int			have_persisted = 0;

	*publish_needed = 0;
	if (store_shard_count_path(store_dir, path, sizeof(path)) != 0)
		return -1;
	f = fopen(path, "r");
	if (f != NULL)
	{
		if (fscanf(f, "%u", &persisted) == 1 && persisted > 0 &&
			persisted <= PS_MAX_CHANNELS)
			have_persisted = 1;
		fclose(f);
		if (!have_persisted)
			return -1;
	}
	else if (errno != ENOENT)
		return -1;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		uint32_t	shard = layer_shard_from_id(ps_layer_map.layers[i].layer_id);

		if (shard + 1 > inferred)
			inferred = shard + 1;
	}
	for (uint32_t shard = 0; shard < PS_MAX_CHANNELS; shard++)
	{
		PsFlushWatermark watermark;

		if (ps_manifest_get_flush_watermark(shard, &watermark) &&
			shard + 1 > inferred)
			inferred = shard + 1;
	}
	if (!have_persisted && infer_segment_shard_count(store_dir, &inferred) != 0)
		return -1;
	if (have_persisted && persisted != 1 && persisted != current)
		return -1;
	if (!have_persisted && inferred != 1 && inferred != current)
		return -1;
	if (!have_persisted || persisted != current)
		*publish_needed = 1;
	return 0;
}

static uint64_t
layer_id(uint32_t shard, uint64_t local_id)
{
	return ((uint64_t) shard << LAYER_ID_LOCAL_BITS) | (local_id & LAYER_ID_LOCAL_MASK);
}

static Shard *
shard_for(const PsKey *key)
{
	uint32_t ns = core_shards();

	if (ns == 1 || !key)
		return &g_shards[0];
	return &g_shards[ps_key_shard(key, ns)];
}

/*
 * Shard index that will actually be touched for 'key' (klass-aware), so the
 * frontend can take the matching per-shard lock from the FINAL request key
 * rather than trusting a client-supplied channel shard.
 */
uint32_t
ps_shard_of(const PsKey *key)
{
	uint32_t	ns = core_shards();

	if (ns == 1 || !key)
		return 0;
	return ps_key_shard(key, ns);
}

uint32_t
ps_core_layer_count(void)
{
	return ps_layer_map.nlayers;
}

/* read-path source counters (memtable / image layer / segment fallback),
 * summed across shards */
void
ps_core_read_stats(uint64_t *mem, uint64_t *layer, uint64_t *seg)
{
	uint64_t	m = 0,
				l = 0,
				s = 0;
	uint32_t	ns = core_shards();

	for (uint32_t i = 0; i < ns; i++)
	{
		m += __atomic_load_n(&g_shards[i].rr_mem, __ATOMIC_RELAXED);
		l += __atomic_load_n(&g_shards[i].rr_layer, __ATOMIC_RELAXED);
		s += __atomic_load_n(&g_shards[i].rr_seg, __ATOMIC_RELAXED);
	}
	if (mem)
		*mem = m;
	if (layer)
		*layer = l;
	if (seg)
		*seg = s;
}

static uint64_t
alloc_layer_id(void *ctx)
{
	Shard *s = (Shard *) ctx;
	uint64_t	id;
	int			exists;

	if (!s)
		s = &g_shards[0];
	do
	{
		id = layer_id(s->id, s->next_layer_id++);
		exists = ps_layer_store->layer_exists_local ?
			ps_layer_store->layer_exists_local(id) : 0;
	} while (exists > 0);
	return id;
}

static int
record_layer(void *ctx, const PsLayerDesc *desc)
{
	(void) ctx;
	/* ps_manifest_add_layer persists the ADD event *and* adds it to the layer
	 * map (idempotently); do not add to the map a second time. */
	return ps_manifest_add_layer(desc);
}

static int
mark_legacy_shard_zero_layers(void)
{
	if (core_shards() <= 1)
		return 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *layer = &ps_layer_map.layers[i];
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;
		int			legacy = 0;

		if (layer->kind != PS_LAYER_IMAGE || layer->deleting ||
			layer_shard_from_id(layer->layer_id) != 0)
			continue;
		if (read_image_index_refreshing(layer, &idx, &nidx) != 0)
			return -1;
		for (uint32_t j = 0; j < nidx; j++)
			if (ps_key_shard(&idx[j].key, core_shards()) != 0)
			{
				legacy = 1;
				break;
			}
		free(idx);
		layer->legacy_shard_zero = legacy != 0;
	}
	return 0;
}

static int
flush_memtable(Shard *s, uint32_t seg_id, uint64_t seg_off)
{
	int			rc;
	uint32_t	old_boundary = s->flush_watermark_valid ?
		s->flush_watermark.seg_id : 0;

	if (!s->memtable || ps_memtable_count(s->memtable) == 0)
		return 0;
	rc = ps_memtable_flush(s->memtable, alloc_layer_id, record_layer, s);
	if (rc != 0)
	{
		s->coverage_broken = 1;
		return -1;
	}
	if (s->coverage_broken)
		return 0;
	if (ps_manifest_set_flush_watermark(s->id, seg_id, seg_off) != 0)
	{
		s->coverage_broken = 1;
		return -1;
	}
	/* Keep the disabled controller off the extra per-segment metadata path as
	 * well.  A later enabled open rebuilds the durable prefix exactly once. */
	if (page_reclaim_high_water_bytes != 0 && seg_id > old_boundary)
		account_page_gc_coverage(s, old_boundary, seg_id);
	s->flush_watermark.shard = s->id;
	s->flush_watermark.seg_id = seg_id;
	s->flush_watermark.seg_off = seg_off;
	s->flush_watermark_valid = 1;
	return 0;
}

/* ===================== compaction & GC (LSM phase 3) =================== */

static uint32_t
count_image_layers(uint32_t timeline, uint32_t shard)
{
	uint32_t	c = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting && d->timeline == timeline &&
			layer_shard_from_id(d->layer_id) == shard)
			c++;
	}
	return c;
}

static const PsLayerLocation *tier_remote_location(const PsLayerDesc *layer);
static const PsLayerLocation *tier_local_location(const PsLayerDesc *layer);

/*
 * Finish any GC that a crash interrupted: every layer still marked 'deleting' in
 * the manifest has its local and remote files removed (idempotently) and a
 * REMOVE_LAYER event recorded.  Reads already skip 'deleting' layers, so this
 * only reclaims space.
 */
static int __attribute__((unused))
gc_resume(void)
{
	PsLayerDesc *dead;
	uint32_t	m = 0;
	int		did = 0;
	int		uploading;
	uint64_t	uploading_id;

	uploading = __atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1;
	uploading_id = tier_upload_candidate.layer_id;
	if (map_locks_ready)
		ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].deleting &&
			!(uploading && ps_layer_map.layers[i].layer_id == uploading_id) &&
			__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
						__ATOMIC_ACQUIRE) == 0)
			m++;
	if (m == 0)
	{
		if (map_locks_ready)
			ps_unlock_map();
		return 0;
	}
	dead = malloc((size_t) m * sizeof(PsLayerDesc));
	if (!dead)
	{
		if (map_locks_ready)
			ps_unlock_map();
		return 0;
	}
	m = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].deleting &&
			!(uploading && ps_layer_map.layers[i].layer_id == uploading_id) &&
			__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
						__ATOMIC_ACQUIRE) == 0)
			dead[m++] = ps_layer_map.layers[i];
	if (map_locks_ready)
		ps_unlock_map();
	for (uint32_t k = 0; k < m; k++)
	{
		int		remote_failed = 0;
		PsLayerDesc remote = dead[k];
		/*
		 * Drop the manifest entry only after the file is gone (a missing file
		 * is ENOENT == success in delete_local_layer, so this is idempotent and
		 * a partially-deleted layer still completes).  A real unlink error keeps
		 * the layer "deleting" so the next start retries it.  A REMOVE_LAYER
		 * write error may have torn the manifest tail; stop so that record stays
		 * the recoverable tail instead of becoming interior corruption, and the
		 * next start retries from the last valid manifest state.
	 */
		if (tier_remote_location(&remote) == NULL &&
			ps_layer_store->remote_uri != NULL &&
			remote.location_count < PS_LAYER_MAX_LOCATIONS &&
			ps_layer_store->remote_uri(remote.layer_id,
				remote.locations[remote.location_count].uri,
				sizeof(remote.locations[remote.location_count].uri)) == 0)
		{
			remote.locations[remote.location_count].tier = PS_LAYER_TIER_REMOTE_OBJECT;
			remote.locations[remote.location_count].available = true;
			remote.location_count++;
		}
		if (tier_remote_location(&remote) != NULL &&
			ps_layer_store->delete_remote_layer(&remote) != 0)
			remote_failed = 1;
		if (ps_layer_store->delete_local_layer(&dead[k]) != 0 || remote_failed)
			continue;
		if (map_locks_ready)
			ps_lock_map_wr();
	if (map_locks_ready)
		{
		int still_deleting = 0;

		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == dead[k].layer_id &&
				ps_layer_map.layers[i].deleting)
				still_deleting = 1;
		if (!still_deleting)
		{
			ps_unlock_map();
			continue;
		}
		}
		if (ps_manifest_remove_layer(dead[k].layer_id) != 0)
		{
			if (map_locks_ready)
				ps_unlock_map();
			break;
		}
		if (map_locks_ready)
			ps_unlock_map();
		did = 1;
	}
	free(dead);
	return did;
}

static void
lifecycle_worker_cleanup(void *arg)
{
	(void) arg;
	/* This handler runs both on normal return and deferred cancellation.  The
	 * reservation token was counted by its parent before pthread_create(). */
	ps_lifecycle_read_release_reserved();
}

static void *
gc_remote_worker(void *arg)
{
	PsLayerDesc *layer = arg;
	int old_state;
	int rc;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	ps_lifecycle_read_adopt_reserved();
	pthread_cleanup_push(lifecycle_worker_cleanup, NULL);
	rc = ps_layer_store->delete_remote_layer(layer);
	/* The remote delete and its local/manifest publication are one lifecycle
	 * reservation.  A cancellation may interrupt the provider call, but once
	 * it returns successfully the publication must run to completion. */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_state);
	if (rc == 0)
		rc = gc_finish_local(layer->layer_id, 1) ? 0 : -1;
	__atomic_store_n(&gc_remote_state, rc == 0 ? 2 : 3, __ATOMIC_RELEASE);
	pthread_setcancelstate(old_state, NULL);
	pthread_cleanup_pop(1);
	return NULL;
}

/*
 * Finish the local half of GC while holding the map lock that serializes the
 * descriptor lookup and REMOVE_LAYER append.  remote_done is process-local:
 * after a crash, retrying the provider delete is required and must remain
 * idempotent, but during this process a failed unlink must not issue it twice.
 */
static int
gc_finish_local(uint64_t layer_id, int remote_done)
{
	PsLayerDesc *layer = NULL;

	ps_lock_map_wr();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == layer_id &&
			ps_layer_map.layers[i].deleting)
		{
			PsTimelineState state;

			if (!ps_timeline_state(ps_layer_map.layers[i].timeline, &state, NULL) ||
				(state != PS_TIMELINE_LIVE && state != PS_TIMELINE_DELETING))
				break;
			layer = &ps_layer_map.layers[i];
			break;
		}
	if (layer == NULL)
	{
		ps_unlock_map();
		return 1;
	}
	if (remote_done)
		layer->remote_cleanup_done = true;
	if (ps_layer_store->delete_local_layer(layer) != 0 ||
		ps_manifest_remove_layer(layer_id) != 0)
	{
		ps_unlock_map();
		return 0;
	}
	ps_unlock_map();
	return 1;
}

/*
 * Durably mark one layer owned by a timeline whose lifecycle state is
 * DELETING.  This is deliberately separate from ordinary compaction GC:
 * deleting a timeline is the only path allowed to select a layer on a
 * DELETING timeline, and it never touches segments or fork metadata shared by
 * other timelines.
 *
 * The existing asynchronous GC worker performs the physical deletion and
 * REMOVE_LAYER append after this function returns.  Thus MARK_DELETE remains
 * the durable discovery boundary for restart recovery.
 */
static int
timeline_delete_mark_one(void)
{
	PsLayerDesc candidate;
	PsLayerDesc *current;
	int			found = 0;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *layer = &ps_layer_map.layers[i];
		PsTimelineState state;

		if (!ps_timeline_state(layer->timeline, &state, NULL) ||
			state != PS_TIMELINE_DELETING ||
			__atomic_load_n(&layer->cache_readers, __ATOMIC_ACQUIRE) != 0)
			continue;
		candidate = *layer;
		found = 1;
		break;
	}
	ps_unlock_map();
	if (!found)
		return 0;

	/* Establish the durable per-layer tombstone before unlinking anything. */
	if (!candidate.deleting)
	{
		ps_lock_map_wr();
		current = NULL;
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == candidate.layer_id)
			{
				PsTimelineState state;

				if (ps_timeline_state(ps_layer_map.layers[i].timeline, &state, NULL) &&
					state == PS_TIMELINE_DELETING &&
					!ps_layer_map.layers[i].deleting)
					current = &ps_layer_map.layers[i];
				break;
			}
		if (current == NULL || ps_manifest_mark_delete(candidate.layer_id) != 0)
		{
			ps_unlock_map();
			return 0;
		}
		ps_unlock_map();
		return 1;
	}
	return 0;
}

/* Run at most one remote-GC operation without blocking the maintenance loop. */
static void
gc_remote_backoff(const struct timespec *now)
{
	gc_remote_retry_at = *now;
	gc_remote_retry_at.tv_sec++;
}

static int
gc_remote_one(void)
{
	int state = __atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE);
	struct timespec now;

	clock_gettime(CLOCK_MONOTONIC, &now);
	if (now.tv_sec < gc_remote_retry_at.tv_sec ||
		(now.tv_sec == gc_remote_retry_at.tv_sec && now.tv_nsec < gc_remote_retry_at.tv_nsec))
		return 0;

	if (state == 1)
		return 0;
	if (state == 2 || state == 3)
	{
		pthread_join(gc_remote_thread, NULL);
		if (state != 2)
		{
			__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
			gc_remote_backoff(&now);
			return 0;
		}
		__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
		return 1;
	}
	ps_lock_map_rd();
	for (uint32_t pass = 0; pass < ps_layer_map.nlayers; pass++)
	{
		uint32_t i = (gc_remote_map_cursor + pass) % ps_layer_map.nlayers;
		{
			PsTimelineState timeline_state;

			if (ps_layer_map.layers[i].deleting &&
				ps_timeline_state(ps_layer_map.layers[i].timeline,
								  &timeline_state, NULL) &&
				(timeline_state == PS_TIMELINE_LIVE ||
				 timeline_state == PS_TIMELINE_DELETING) &&
				!(__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1 &&
				  ps_layer_map.layers[i].layer_id == tier_upload_candidate.layer_id))
			{
				gc_remote_candidate = ps_layer_map.layers[i];
				gc_remote_layer_cursor = gc_remote_candidate.layer_id;
				gc_remote_map_cursor = (i + 1) % ps_layer_map.nlayers;
				ps_unlock_map();
				if (gc_remote_candidate.remote_cleanup_done)
				{
					int finished = gc_finish_local(gc_remote_candidate.layer_id, 0);

					if (!finished)
						gc_remote_backoff(&now);
					return finished;
				}
				if (tier_remote_location(&gc_remote_candidate) == NULL)
				{
					PsLayerLocation *remote;
					int		uri_errno = 0;

					errno = 0;
					if (ps_layer_store->remote_uri != NULL &&
						gc_remote_candidate.location_count < PS_LAYER_MAX_LOCATIONS &&
						ps_layer_store->remote_uri(gc_remote_candidate.layer_id,
							gc_remote_candidate.locations[gc_remote_candidate.location_count].uri,
							sizeof(gc_remote_candidate.locations[gc_remote_candidate.location_count].uri)) == 0)
					{
						remote = &gc_remote_candidate.locations[gc_remote_candidate.location_count++];
						remote->tier = PS_LAYER_TIER_REMOTE_OBJECT;
						remote->available = true;
					}
					else
					{
						uri_errno = errno;
						/* ENOTSUP means this provider has no object tier.  A
						 * local-only layer can finish immediately; a layer whose
						 * upload was durably recorded must retain its tombstone
						 * until the remote URI is available again. */
						if (!gc_remote_candidate.remote_durable &&
							(ps_layer_store->remote_uri == NULL || uri_errno == ENOTSUP))
						{
							int finished = gc_finish_local(gc_remote_candidate.layer_id, 0);

							if (!finished)
								gc_remote_backoff(&now);
							return finished;
						}
						gc_remote_backoff(&now);
						return 0;
					}
				}
				ps_lifecycle_read_reserve();
				__atomic_store_n(&gc_remote_state, 1, __ATOMIC_RELEASE);
				if (pthread_create(&gc_remote_thread, NULL, gc_remote_worker,
							   &gc_remote_candidate) != 0)
				{
					ps_lifecycle_read_cancel_reservation();
					__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
					gc_remote_backoff(&now);
					return 0;
				}
				return 1;
			}
		}
	}
	ps_unlock_map();
	return 0;
}

/*
 * Merge all of a timeline's image layers into one fresh layer (bounding the
 * layer count and the per-read layer scan), then GC the merged-away layers.
 * Install-new-before-delete-old: the new layer is written and recorded durably
 * before any old layer is marked for deletion, so a crash at any point leaves
 * the data readable and GC resumable.  Each version lives in exactly one
 * source layer; page-history pruning keeps the newest version below the
 * effective floor plus every version at or above it.
 */
typedef struct CompactOrder
{
	PsKey		key;
	uint32_t	block;
	PsPruneVersion version;
	uint32_t	source;
} CompactOrder;

static int
compact_order_cmp(const void *va, const void *vb)
{
	const CompactOrder *a = va;
	const CompactOrder *b = vb;

#define CMP_KEY_FIELD(field) \
	if (a->key.field != b->key.field) \
		return a->key.field < b->key.field ? -1 : 1
	CMP_KEY_FIELD(spcOid);
	CMP_KEY_FIELD(dbOid);
	CMP_KEY_FIELD(relNumber);
	CMP_KEY_FIELD(forkNum);
	CMP_KEY_FIELD(klass);
#undef CMP_KEY_FIELD
	if (a->block != b->block)
		return a->block < b->block ? -1 : 1;
	if (a->version.lsn != b->version.lsn)
		return a->version.lsn < b->version.lsn ? -1 : 1;
	if (a->version.admission_seq != b->version.admission_seq)
		return a->version.admission_seq < b->version.admission_seq ? -1 : 1;
	return a->source < b->source ? -1 : (a->source > b->source ? 1 : 0);
}

static int
compact_same_page(const CompactOrder *a, const CompactOrder *b)
{
	return a->block == b->block &&
		a->key.spcOid == b->key.spcOid && a->key.dbOid == b->key.dbOid &&
		a->key.relNumber == b->key.relNumber &&
		a->key.forkNum == b->key.forkNum && a->key.klass == b->key.klass;
}

static int
prune_compaction_records(uint32_t timeline, PsImgRec *recs, uint32_t *nrec,
						 uint64_t floor,
						 PsImgRec **dropped_out, uint32_t *ndropped_out)
{
	CompactOrder *order;
	PsPruneVersion *versions;
	unsigned char *keep;
	PsImgRec   *selected;
	PsImgRec   *dropped;
	uint32_t	out = 0;
	uint32_t	ndropped = 0;
	PsPruneFence *fences = NULL;
	uint32_t	nfences = 0;

	if (page_prune_fences(timeline, &fences, &nfences) != 0)
		return -1;

	order = malloc((size_t) *nrec * sizeof(*order));
	versions = malloc((size_t) *nrec * sizeof(*versions));
	keep = malloc(*nrec);
	selected = malloc((size_t) *nrec * sizeof(*selected));
	dropped = malloc((size_t) *nrec * sizeof(*dropped));
	if (!order || !versions || !keep || !selected || !dropped)
	{
		free(order);
		free(versions);
		free(keep);
		free(selected);
		free(dropped);
		free(fences);
		return -1;
	}
	for (uint32_t i = 0; i < *nrec; i++)
	{
		order[i].key = recs[i].key;
		order[i].block = recs[i].block;
		order[i].version.lsn = recs[i].lsn;
		order[i].version.admission_seq = recs[i].admission_seq;
		order[i].source = i;
	}
	qsort(order, *nrec, sizeof(*order), compact_order_cmp);
	for (uint32_t first = 0; first < *nrec;)
	{
		uint32_t end = first + 1;

		while (end < *nrec && compact_same_page(&order[first], &order[end]))
			end++;
		/* Relation page history is the only history governed by the page-prune
		 * frontier.  Control and SLRU pages are reader-artifact authority: an
		 * exact checkpoint can require an older SLRU base even when its ordinary
		 * relation pages have advanced.  Keep all non-relation versions until
		 * their dedicated retention protocols prove them reclaimable. */
		if (order[first].key.klass != PS_KLASS_RELATION)
		{
			for (uint32_t i = first; i < end; i++)
				selected[out++] = recs[order[i].source];
			first = end;
			continue;
		}
		for (uint32_t i = first; i < end; i++)
			versions[i - first] = order[i].version;
		if (floor == 0)
			memset(keep, 1, end - first);
		else if (ps_page_prune_plan(versions, end - first,
								(PsPruneFence) {floor, UINT64_MAX}, fences,
									 nfences, keep) < 0)
		{
			free(order);
			free(versions);
			free(keep);
			free(selected);
			free(dropped);
			free(fences);
			return -1;
		}
		for (uint32_t i = first; i < end;)
		{
			uint32_t next = i + 1;
			int kept_source = -1;

			while (next < end &&
				   order[next].version.lsn == order[i].version.lsn &&
				   order[next].version.admission_seq ==
				   order[i].version.admission_seq)
				next++;
			for (uint32_t j = i; j < next; j++)
				if (keep[j - first])
				{
					kept_source = (int) order[j].source;
					break;
				}
			/* Sorted equal identities are one logical version.  Retain one
			 * physical copy, or remove the identity from page_idx exactly once. */
			if (kept_source >= 0)
				selected[out++] = recs[kept_source];
			else
				dropped[ndropped++] = recs[order[i].source];
			i = next;
		}
		first = end;
	}
	memcpy(recs, selected, (size_t) out * sizeof(*recs));
	free(order);
	free(versions);
	free(keep);
	free(selected);
	free(fences);
	*nrec = out;
	*dropped_out = dropped;
	*ndropped_out = ndropped;
	return 0;
}

static int
compact_timeline(uint32_t timeline, uint32_t shard, uint64_t page_floor)
{
	PsLayerDesc *old;
	uint32_t	nold = count_image_layers(timeline, shard);
	PsImgRec   *recs = NULL;
	unsigned char **pages = NULL;
	uint32_t	nrec = 0,
				cap = 0,
				npages = 0,
				scanned = 0;
	uint64_t	nid;
	PsLayerDesc newdesc;
	PsLayerLocation remote;
	PsImgRec   *dropped = NULL;
	uint32_t	ndropped = 0;
	uint64_t	frontier_seq;
	int			rc = -1;

	if (!ps_timeline_live(timeline))
		return 0;
	if (nold == 0)
		return 0;				/* nothing worth merging */

	/*
	 * Never compact a poisoned manifest: the new layer could not be recorded, so
	 * we would just write an unreferenced file and the old layers would stay live
	 * -- maintenance would keep retrying and leaking files.  Bail (the daemon is
	 * already rejecting writes; a restart recovers the manifest).
	 */
	if (ps_manifest_poisoned())
		return -1;

	old = malloc((size_t) nold * sizeof(PsLayerDesc));
	if (!old)
		return -1;
	nold = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];

		if (ps_timeline_live(timeline) && d->kind == PS_LAYER_IMAGE &&
			!d->deleting &&
			d->timeline == timeline &&
			layer_shard_from_id(d->layer_id) == shard)
			old[nold++] = *d;
	}
	/* A read snapshots and pins the complete timeline layer set before doing
	 * remote I/O.  Do not publish a partial compaction while any source is
	 * pinned: that leaves the source count above the threshold and makes idle
	 * maintenance repeatedly create larger overlapping replacements. */
	for (uint32_t k = 0; k < nold; k++)
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == old[k].layer_id &&
				__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
								__ATOMIC_ACQUIRE) != 0)
			{
				free(old);
				return 0;
			}

	/* gather every version (page bytes) from the old layers */
	for (uint32_t k = 0; k < nold; k++)
	{
		PsImgIndexEnt *idx;
		uint32_t	n;

		if (read_image_index_refreshing(&old[k], &idx, &n) != 0)
			goto cleanup;
		if (verify_image_layer_refreshing(&old[k]) != 0)
		{
			free(idx);
			goto cleanup;
		}
		for (uint32_t j = 0; j < n; j++)
		{
			unsigned char *pg;

			if (nrec == cap)
			{
				uint32_t	nc = cap ? cap * 2 : 256;
				PsImgRec   *nr = realloc(recs, (size_t) nc * sizeof(PsImgRec));
				unsigned char **np = realloc(pages, (size_t) nc * sizeof(*pages));

				if (!nr || !np)
				{
					free(nr ? nr : recs);
					free(np ? np : pages);
					recs = NULL;
					pages = NULL;
					free(idx);
					goto cleanup;
				}
				recs = nr;
				pages = np;
				cap = nc;
			}
			pg = malloc(page_size);
			if (!pg || read_layer_block_refreshing(&old[k], idx[j].data_off,
												   pg, page_size) != 0)
			{
				free(pg);
				free(idx);
				goto cleanup;
			}
			recs[nrec].key = idx[j].key;
			recs[nrec].block = idx[j].block;
			recs[nrec].lsn = idx[j].lsn;
			recs[nrec].admission_seq = idx[j].admission_seq;
			recs[nrec].page = pg;
			recs[nrec].growth_lsn = idx[j].growth_lsn;
			recs[nrec].order_id = idx[j].order_id;
			recs[nrec].seg_off = idx[j].seg_off;
			recs[nrec].seg_id = idx[j].seg_id;
			recs[nrec].flags = idx[j].flags;
			pages[nrec] = pg;
			nrec++;
			npages = nrec;
		}
		free(idx);
	}
	if (nrec == 0)
		goto cleanup;
	scanned = nrec;
	if (prune_compaction_records(timeline, recs, &nrec, page_floor,
								 &dropped, &ndropped) != 0 || nrec == 0)
		goto cleanup;
	frontier_seq = __atomic_load_n(&next_admission_seq, __ATOMIC_ACQUIRE);
	if (frontier_seq != 0)
		frontier_seq--;

	/* install the new merged layer durably, THEN delete the old ones */
	nid = alloc_layer_id(&g_shards[shard]);
	if (ps_image_layer_write(nid, timeline, recs, nrec, page_size,
							 &newdesc) != 0)
		goto cleanup;
	for (uint32_t k = 0; k < nold; k++)
		if (old[k].legacy_shard_zero)
			newdesc.legacy_shard_zero = true;
	/* A remote-durable source may be the only copy surviving loss of the local
	 * store.  Publish and verify the replacement in that same durability tier
	 * before its ADD can make any source eligible for deletion. */
	for (uint32_t k = 0; k < nold; k++)
		if (old[k].remote_durable)
		{
			const PsLayerLocation *local = tier_local_location(&newdesc);

			if (local == NULL || ps_layer_store->upload_layer == NULL ||
				ps_layer_store->remote_uri == NULL ||
				newdesc.location_count >= PS_LAYER_MAX_LOCATIONS)
			{
				(void) ps_layer_store->delete_local_layer(&newdesc);
				goto cleanup;
			}
			if (ps_layer_store->upload_layer(&newdesc) != 0)
			{
				(void) ps_layer_store->delete_local_layer(&newdesc);
				goto cleanup;
			}
			memset(&remote, 0, sizeof(remote));
			remote.tier = PS_LAYER_TIER_REMOTE_OBJECT;
			remote.size = local->size;
			remote.available = true;
			if (ps_layer_store->remote_uri(newdesc.layer_id, remote.uri,
										 sizeof(remote.uri)) != 0)
			{
				(void) ps_layer_store->delete_local_layer(&newdesc);
				goto cleanup;
			}
			newdesc.locations[newdesc.location_count++] = remote;
			newdesc.remote_durable = true;
			newdesc.remote_uploaded_lsn = newdesc.lsn_end;
			break;
		}
	/* Reject later pins/branches below this cutoff before the pruned layer can
	 * become durable and visible.  Advancing conservatively when publication
	 * later fails is safe; admitting already-reclaimed history is not. */
	if (ndropped != 0 &&
		page_frontier_advance(timeline, page_floor, frontier_seq) != 0)
	{
		(void) ps_layer_store->delete_local_layer(&newdesc);
		goto cleanup;
	}
	if (ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) != 0)
		goto cleanup;
	if (record_layer(NULL, &newdesc) != 0)
	{
		(void) ps_layer_store->delete_local_layer(&newdesc);
		goto cleanup;
	}
	/* The replacement is published before any source is retired.  Keep this
	 * distinct crash boundary so recovery covers both live sources and the
	 * replacement together. */
	if (ps_fault_probe(PS_FAULT_POINT_PAGE_COMPACTION_AFTER_PUBLISH) != 0)
		goto cleanup;
	/* The durable replacement no longer contains these versions.  Drop their
	 * in-memory index entries at the same publication point; otherwise a live
	 * read can select a pruned PageVer and then fail because no layer can serve
	 * the advertised bytes.  Recovery already derives the same index from the
	 * surviving layer set. */
	page_remove_compacted_versions(timeline, dropped, ndropped);
	if (metrics_header != NULL)
	{
		ps_fetch_add_u64(&metrics_header->page_prune_metrics_seq, 1);
		ps_fetch_add_u64(&metrics_header->page_prune_compactions, 1);
		ps_fetch_add_u64(&metrics_header->page_prune_versions_scanned, scanned);
		ps_fetch_add_u64(&metrics_header->page_prune_versions_kept, nrec);
		ps_fetch_add_u64(&metrics_header->page_prune_versions_deleted,
						 scanned - nrec);
		ps_fetch_add_u64(&metrics_header->page_prune_metrics_seq, 1);
	}
	for (uint32_t k = 0; k < nold; k++)
	{
		/*
		 * Fail safe at every step.  Only delete the file once the layer is
		 * durably marked deleting, and only drop it from the manifest once the
		 * file is gone -- so a failed step leaves a readable layer (its data is
		 * also in the new layer) that gc_resume() retries on the next start,
		 * never a manifest entry pointing at a deleted file.
		 *
		 * A manifest write error may have left a torn record at the tail; STOP
		 * before unlinking or appending anything more, so that torn record stays
		 * the recoverable tail rather than becoming interior corruption that
		 * fails replay (and so we never unlink a file whose later delete mark is
		 * not durable).  A failed unlink is not a manifest error: the layer is
		 * durably deleting, so we can move on and let gc_resume() retry it.
		 */
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == old[k].layer_id &&
				__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
							 __ATOMIC_ACQUIRE) != 0)
				goto next_old;
		if (ps_manifest_mark_delete(old[k].layer_id) != 0)
			goto cleanup;		/* incomplete: old layers stay live, count not cut */
		/* The replacement is visible and this source is now durably retired. */
		if (ps_fault_probe(PS_FAULT_POINT_PAGE_GC_AFTER_MARK_DELETE) != 0)
			goto cleanup;
		if (ps_layer_store->delete_local_layer(&old[k]) != 0)
			continue;			/* still "deleting"; gc_resume() will retry */
		/*
		 * Remote object deletion may block on an object mount.  Keep the
		 * durable deleting record and let the idle maintenance path run
		 * gc_resume() after releasing the shard/map write locks.
		 */
		if (tier_remote_location(&old[k]) != NULL ||
			(__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) != 0 &&
			 tier_upload_candidate.layer_id == old[k].layer_id))
			continue;
		if (ps_manifest_remove_layer(old[k].layer_id) != 0)
			goto cleanup;		/* incomplete */
	next_old:
		;
	}
	rc = 1;

cleanup:
	for (uint32_t j = 0; j < npages; j++)
		free(pages[j]);
	free(recs);
	free(pages);
	free(dropped);
	free(old);
	return rc;
}

static int
materialize_compaction_inputs(uint32_t timeline, uint32_t shard)
{
	PsLayerDesc *layers = NULL;
	uint32_t	nlayers = 0;
	int			rc = -1;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];

		if (ps_timeline_live(timeline) && d->kind == PS_LAYER_IMAGE &&
			!d->deleting &&
			d->timeline == timeline && layer_shard_from_id(d->layer_id) == shard)
		{
			PsLayerDesc *nlayers_ptr;

			nlayers_ptr = realloc(layers, (size_t) (nlayers + 1) * sizeof(*layers));
			if (nlayers_ptr == NULL)
			{
				ps_unlock_map();
				goto out;
			}
			layers = nlayers_ptr;
			layers[nlayers++] = *d;
			__atomic_add_fetch(&d->cache_readers, 1, __ATOMIC_ACQ_REL);
		}
	}
	ps_unlock_map();
	if (nlayers == 0)
	{
		free(layers);
		return 0;
	}

	for (uint32_t i = 0; i < nlayers; i++)
	{
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;

		if (read_image_index_refreshing(&layers[i], &idx, &nidx) != 0 ||
			verify_image_layer_refreshing(&layers[i]) != 0)
		{
			free(idx);
			goto out;
		}
		free(idx);
	}
	rc = 0;

out:
	ps_lock_map_wr();
	for (uint32_t i = 0; i < nlayers; i++)
		for (uint32_t j = 0; j < ps_layer_map.nlayers; j++)
			if (ps_layer_map.layers[j].layer_id == layers[i].layer_id)
			{
				__atomic_sub_fetch(&ps_layer_map.layers[j].cache_readers, 1,
								   __ATOMIC_ACQ_REL);
				if (layers[i].data_verified)
					ps_layer_map.layers[j].data_verified = true;
				if (tier_local_location(&ps_layer_map.layers[j]) == NULL &&
					ps_layer_store->layer_exists_local != NULL &&
					ps_layer_store->layer_exists_local(layers[i].layer_id) == 1)
					ps_layer_map.layers[j].cache_resident = true;
				break;
			}
	ps_unlock_map();
	free(layers);
	return rc;
}

/* ===================== segment storage (log-structured) ================= */

#define SEG_MAGIC		 0x53454732 /* "SEG2": v2 record (PsKey gained klass) */
#define SEG_WALLESS_MAGIC 0x53454730 /* "SEG0": zero-version record + growth floor */
#define SEG_WALLESS_ORDERED_MAGIC 0x53454731 /* "SEG1": SEG0 + required order marker */
#define SEG_CLAMPED_ORDERED_MAGIC 0x53454733 /* "SEG3": clamped version + marker */
#define SEG_WALLESS_BOUND_MAGIC 0x53454734 /* "SEG4": SEG1 + marker identity */
#define SEG_CLAMPED_BOUND_MAGIC 0x53454735 /* "SEG5": SEG3 + marker identity */
#define SEG_ADMISSION_MAGIC 0x53454736 /* "SEG6": SEG2 + admission sequence */
#define SEG_WALLESS_ADMISSION_MAGIC 0x53454737 /* "SEG7": SEG4 + admission */
#define SEG_CLAMPED_ADMISSION_MAGIC 0x53454738 /* "SEG8": SEG5 + admission */

/*
 * On-disk layout of one appended page version: this header immediately
 * followed by 'len' page bytes.  The header is self-describing (carries the
 * full key/block/lsn), which is what lets recover() rebuild the entire
 * in-memory index by scanning segments sequentially -- no separate index file
 * to keep in sync.
 */
typedef struct SegRecHdr
{
	uint32_t	magic;			/* one of the SEG*_MAGIC values above */
	uint32_t	timeline;		/* timeline the version belongs to */
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;			/* version LSN, or WAL-less fork-growth floor */
	uint32_t	len;			/* page bytes following the header */
} SegRecHdr;

typedef struct SegRecHdrBound
{
	SegRecHdr	hdr;
	uint64_t	order_id;
} SegRecHdrBound;

typedef struct SegRecHdrAdmission
{
	SegRecHdr	hdr;
	uint64_t	admission_seq;
} SegRecHdrAdmission;

typedef struct SegRecHdrBoundAdmission
{
	SegRecHdr	hdr;
	uint64_t	order_id;
	uint64_t	admission_seq;
} SegRecHdrBoundAdmission;

typedef struct SegmentReloc
{
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint64_t	old_off;
	uint64_t	new_off;
} SegmentReloc;

static int
segment_record_shape(uint32_t magic, uint64_t *header_size, int *wal_less,
					 int *bound, int *admission)
{
	*wal_less = magic == SEG_WALLESS_MAGIC ||
		magic == SEG_WALLESS_ORDERED_MAGIC ||
		magic == SEG_WALLESS_BOUND_MAGIC ||
		magic == SEG_WALLESS_ADMISSION_MAGIC;
	*bound = magic == SEG_WALLESS_BOUND_MAGIC ||
		magic == SEG_CLAMPED_BOUND_MAGIC ||
		magic == SEG_WALLESS_ADMISSION_MAGIC ||
		magic == SEG_CLAMPED_ADMISSION_MAGIC;
	*admission = magic == SEG_ADMISSION_MAGIC ||
		magic == SEG_WALLESS_ADMISSION_MAGIC ||
		magic == SEG_CLAMPED_ADMISSION_MAGIC;
	if (magic != SEG_MAGIC && magic != SEG_WALLESS_MAGIC &&
		magic != SEG_WALLESS_ORDERED_MAGIC && magic != SEG_CLAMPED_ORDERED_MAGIC &&
		magic != SEG_WALLESS_BOUND_MAGIC && magic != SEG_CLAMPED_BOUND_MAGIC &&
		magic != SEG_ADMISSION_MAGIC && magic != SEG_WALLESS_ADMISSION_MAGIC &&
		magic != SEG_CLAMPED_ADMISSION_MAGIC)
		return -1;
	*header_size = sizeof(SegRecHdr) +
		(*bound ? sizeof(uint64_t) : 0) +
		(*admission ? sizeof(uint64_t) : 0);
	return 0;
}

/*
 * Segments are addressed by (id, byte offset); how they are stored is the
 * storage backend's business (see pagestore_storage.h).  Here we keep only the
 * append cursor (cur_seg, cur_off) marking where the next record goes.
 */
/* append cursors are kept per-shard in g_shards[].cur_seg / cur_off */

/* ===================== in-memory indexes =============================== */

/*
 * Two chained hash tables form the indirection map that lets a single logical
 * page be located inside the large append-only segments:
 *
 *	 page_idx: (timeline, key, block) -> chain of versions {lsn, seg, off}
 *	 fork_idx: (timeline, key)        -> size of the fork on that timeline
 *
 * Entries are keyed by timeline so a branch's writes are isolated; reads that
 * miss on a timeline fall through to its parent (see read_through()).  Both
 * tables are in-memory state, rebuilt from the segments by recover().
 * (Prototype: no GC/compaction, so the version chain only grows.)
 */

/* PageVer (one stored version's location) is defined in pagestore_core.h. */

/* Hash entry: all versions of one (timeline, key, block), in arrival order. */
typedef struct PageEnt
{
	struct PageEnt *next;		/* bucket chain */
	struct PageEnt *fork_next;	/* pages belonging to the same fork */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	PageVer    *vers;			/* dynamic array, length nver, capacity cap */
	int			nver;
	int			cap;
} PageEnt;

/* Hash entry: the block count of one fork on one timeline. */
/*
 * Fork-size history event.  GROW events come from page appends (the block's
 * pd_lsn -- exact: a block is readable as of a horizon iff it has a version
 * at/below it) and zero-extends (the backend's stamped WAL position); SET
 * events from create (0) and truncate (the new size); DEAD from unlink.
 * SET/DEAD are definitive: they end an as-of resolution at their timeline
 * hop, where plain growth still combines with ancestor sizes (a branch that
 * wrote only some blocks inherits the rest by read-through).
 */
typedef struct ForkEvent
{
	uint64_t	lsn;
	uint64_t	admission_seq;	/* global mutation order; 0 = legacy */
	uint64_t	order_id;		/* bound segment marker identity, else zero */
	uint32_t	nblocks;
	uint32_t	cached_nblocks;	/* hop result through this sorted event */
	uint32_t	cached_fence_nblocks; /* smallest inherited block boundary */
	uint8_t		kind;
	uint8_t		marker_kind;	/* durable ordered marker kind, even after activation */
	uint8_t		cached_state;
} ForkEvent;

#define FEV_GROW	0
#define FEV_SET		1
#define FEV_DEAD	2
#define FEV_MIGRATED 3			/* log marker: legacy lsn-0 migration completed */
#define FEV_MIGRATING 4			/* log marker: legacy migration started */
#define FEV_SEG_GROW 5			/* ordering placeholder, activated by segment replay */
#define FEV_SEG_COMMIT 6		/* ordered segment commit that does not change size */
#define FEV_SEG_GROW_BOUND 7	/* FEV_SEG_GROW paired with a segment identity */
#define FEV_SEG_COMMIT_BOUND 8 /* FEV_SEG_COMMIT paired with a segment identity */
#define FEV_SEG_ID 9			/* second record carrying a bound marker's identity */
#define FEV_SNAPSHOT_BASE 10	/* source-log epoch marker after snapshot cutover */

typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	nblocks;		/* newest size (cache of the event history) */
	ForkEvent  *ev;				/* lsn-ordered size history */
	uint32_t	nev;
	uint32_t	evcap;
	uint32_t   *def_idx;		/* indexes of SET/DEAD events only */
	uint32_t	ndef;
	uint32_t	defcap;
	PageEnt    *pages;			/* local pages belonging to this fork */
	uint64_t	last_def_lsn;	/* newest SET/DEAD lsn (growth-clamp floor) */
	uint64_t	last_page_lsn;	/* newest durable local page tuple */
	uint64_t	last_page_seq;
	int			has_wal_less;	/* at least one page version has lsn 0 */
} ForkEnt;

static void
free_page_fork_indexes(void)
{
	for (uint32_t sh = 0; sh < MAX_SHARDS; sh++)
	{
		Shard *s = &g_shards[sh];

		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		{
			PageEnt *page = s->page_idx[bucket];
			ForkEnt *fork = s->fork_idx[bucket];

			while (page != NULL)
			{
				PageEnt *next = page->next;

				free(page->vers);
				free(page);
				page = next;
			}
			while (fork != NULL)
			{
				ForkEnt *next = fork->next;

				free(fork->ev);
				free(fork->def_idx);
				free(fork);
				fork = next;
			}
			s->page_idx[bucket] = NULL;
			s->fork_idx[bucket] = NULL;
		}
	}
}

/*
 * Timeline metadata.  Timeline 0 is the root (no parent).  A branch records its
 * parent and the LSN at which it forked; reads of pages the branch never wrote
 * fall through to the parent as-of that branch LSN, so the branch is a stable
 * copy-on-write snapshot.
 */
/* Per-timeline immutable WAL stores are declared before the admission helpers
 * because the same retained-base fence is used by reads and branch admission. */
static PsWalStore wal_segment_stores[MAX_TIMELINES];
static unsigned char wal_segment_store_opened[MAX_TIMELINES];
typedef struct TimelineMeta
{
	int			defined;		/* 1 if this timeline exists */
	int			parent;			/* parent timeline id, or -1 for the root */
	uint64_t	branch_lsn;		/* parent LSN this timeline forked at */
	uint32_t	state;			/* PsTimelineState; published after durable append */
	uint64_t	incarnation;		/* nonzero fencing generation */
	uint64_t	parent_incarnation;	/* immutable generation of parent, or 1 for root */
} TimelineMeta;

static TimelineMeta timelines[MAX_TIMELINES];
/* A metadata append failure is ambiguous: the lower layer may have made the
 * record durable before reporting an error.  Refuse all timeline services
 * until the process reopens and replays the log. */
static volatile int timeline_meta_poisoned;

static inline int
timeline_meta_poisoned_load(void)
{
	return __atomic_load_n(&timeline_meta_poisoned, __ATOMIC_ACQUIRE);
}

static inline void
timeline_meta_poison(void)
{
	__atomic_store_n(&timeline_meta_poisoned, 1, __ATOMIC_RELEASE);
}
/* Retention changes can make projected ancestor history reclaimable even when
 * no new layer arrives.  Maintenance rewrites every marked nonempty shard and
 * clears its mark only after publishing at the new effective floor. */
static unsigned char page_prune_due[MAX_TIMELINES][PS_MAX_CHANNELS];

static void
page_prune_mark_all_due(void)
{
	uint32_t	ns = core_shards();

	ps_lock_map_rd();
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined)
			for (uint32_t sh = 0; sh < ns; sh++)
				__atomic_store_n(&page_prune_due[tl][sh], 1, __ATOMIC_RELEASE);
	ps_unlock_map();
}

/*
 * Branch-local usage marker: set once a timeline acquires any local state (a
 * page version, fork entry, WAL-index entry, or shipped WAL).  An
 * exact-match duplicate CREATE_BRANCH is accepted only while the timeline is
 * still unused: that keeps a prepare retry idempotent (retries happen before
 * a compute ever boots on the branch), while reusing the id of a live branch
 * is refused -- read_through() resolves timeline-local versions before the
 * parent snapshot, so a "fresh" branch recreated over a written timeline
 * would silently serve the previous branch's pages.
 */
static int timeline_used[MAX_TIMELINES];
/* Set only after the POSIX private WAL cleanup and the matching runtime purge
 * have both completed.  DELETING remains the durable terminal state for this
 * slice; this bit is only a maintenance retry guard. */
static unsigned char timeline_wal_cleanup_done[MAX_TIMELINES];
static unsigned char timeline_page_cleanup_done[MAX_TIMELINES];
static uint32_t timeline_page_cleanup_cursor;

static void timeline_reset_reuse_runtime(uint32_t timeline);

static inline void
timeline_mark_used(uint32_t timeline)
{
	if (timeline < MAX_TIMELINES)
		__atomic_store_n(&timeline_used[timeline], 1, __ATOMIC_RELEASE);
}

static inline int
timeline_is_used(uint32_t timeline)
{
	if (timeline >= MAX_TIMELINES)
		return 0;
	return __atomic_load_n(&timeline_used[timeline], __ATOMIC_ACQUIRE);
}

/* highest end LSN (start+len) of shipped WAL received per timeline */
static uint64_t wal_end[MAX_TIMELINES];
static uint64_t wal_start[MAX_TIMELINES];
static int		wal_start_valid[MAX_TIMELINES];
static uint64_t wal_covered[MAX_TIMELINES];
static uint64_t wal_covered_off[MAX_TIMELINES];
static int		wal_covered_valid[MAX_TIMELINES];
/* Flat-log offsets are replaced independently per timeline.  Readers retain
 * that timeline's matching offset map until their physical reads complete. */
static pthread_rwlock_t wal_log_locks[MAX_TIMELINES];
static pthread_once_t wal_log_locks_once = PTHREAD_ONCE_INIT;
static int wal_log_locks_failed;

static void
wal_log_locks_init(void)
{
	uint32_t initialized = 0;

	for (; initialized < MAX_TIMELINES; initialized++)
		if (pthread_rwlock_init(&wal_log_locks[initialized], NULL) != 0)
			break;
	if (initialized != MAX_TIMELINES)
	{
		while (initialized > 0)
			pthread_rwlock_destroy(&wal_log_locks[--initialized]);
		wal_log_locks_failed = 1;
	}
}

static pthread_rwlock_t *
wal_log_lock_for(uint32_t timeline)
{
	if (timeline >= MAX_TIMELINES ||
		pthread_once(&wal_log_locks_once, wal_log_locks_init) != 0 ||
		wal_log_locks_failed)
		return NULL;
	return &wal_log_locks[timeline];
}

static inline uint64_t
wal_end_read(uint32_t timeline)
{
	if (timeline >= MAX_TIMELINES)
		return 0;
	return __atomic_load_n(&wal_end[timeline], __ATOMIC_ACQUIRE);
}

static inline void
wal_end_advance(uint32_t timeline, uint64_t end_lsn)
{
	uint64_t	old_end;

	if (timeline >= MAX_TIMELINES)
		return;
	old_end = __atomic_load_n(&wal_end[timeline], __ATOMIC_RELAXED);
	while (end_lsn > old_end &&
		   !__atomic_compare_exchange_n(&wal_end[timeline], &old_end,
										end_lsn, false,
										__ATOMIC_RELEASE,
										__ATOMIC_RELAXED))
		;
}

static inline void
wal_start_observe(uint32_t timeline, uint64_t start_lsn)
{
	if (timeline >= MAX_TIMELINES)
		return;
	if (!__atomic_load_n(&wal_start_valid[timeline], __ATOMIC_ACQUIRE) ||
		start_lsn < __atomic_load_n(&wal_start[timeline], __ATOMIC_RELAXED))
	{
		__atomic_store_n(&wal_start[timeline], start_lsn, __ATOMIC_RELAXED);
		__atomic_store_n(&wal_start_valid[timeline], 1, __ATOMIC_RELEASE);
	}
}

/* FNV-1a hash over a byte range (used to hash keys into buckets). */

static uint32_t
fnv(const void *p, size_t n)
{
	const unsigned char *b = p;
	uint32_t	h = 2166136261u;

	for (size_t i = 0; i < n; i++)
	{
		h ^= b[i];
		h *= 16777619u;
	}
	return h;
}

static uint32_t
page_frontier_crc(const PsPageFrontierState *state)
{
	return fnv(state, offsetof(PsPageFrontierState, crc));
}

static int
page_frontier_publish(void)
{
	PsPageFrontierState state;
	char		tmp[4096];
	int			fd = -1;
	int			n;
	int			rc = -1;

	memset(&state, 0, sizeof(state));
	state.magic = PS_PAGE_FRONTIER_MAGIC;
	state.version = PS_PAGE_FRONTIER_VERSION;
	memcpy(state.entries, page_reclaimed_frontier, sizeof(state.entries));
	state.crc = page_frontier_crc(&state);
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", page_frontier_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
		fsync(fd) != 0)
		goto done;
	if (close(fd) != 0)
	{
		fd = -1;
		goto done;
	}
	fd = -1;
	if (rename(tmp, page_frontier_path) != 0 ||
		fsync_dir_path(page_frontier_dir) != 0)
		goto done;
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		unlink(tmp);
	return rc;
}

static int
page_frontier_load(const char *store_dir)
{
	PsPageFrontierState state;
	PsPageFrontierStateV2 legacy;
	struct stat st;
	int			fd;
	int			n;

	memset(page_reclaimed_frontier, 0, sizeof(page_reclaimed_frontier));
	n = snprintf(page_frontier_dir, sizeof(page_frontier_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(page_frontier_dir))
		return -1;
	n = snprintf(page_frontier_path, sizeof(page_frontier_path),
				 "%s/page-prune.frontiers", store_dir);
	if (n < 0 || (size_t) n >= sizeof(page_frontier_path))
		return -1;
	fd = open(page_frontier_path, O_RDONLY);
	if (fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (fstat(fd, &st) != 0)
	{
		close(fd);
		return -1;
	}
	if (st.st_size == (off_t) sizeof(state))
	{
		if (read(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
			state.magic != PS_PAGE_FRONTIER_MAGIC ||
			state.version != PS_PAGE_FRONTIER_VERSION ||
			state.crc != page_frontier_crc(&state))
		{
			close(fd);
			errno = EILSEQ;
			return -1;
		}
		memcpy(page_reclaimed_frontier, state.entries, sizeof(state.entries));
	}
	else if (st.st_size == (off_t) sizeof(legacy))
	{
		if (read(fd, &legacy, sizeof(legacy)) != (ssize_t) sizeof(legacy) ||
			legacy.magic != PS_PAGE_FRONTIER_MAGIC || legacy.version != 2 ||
			legacy.crc != fnv(&legacy, offsetof(PsPageFrontierStateV2, crc)))
		{
			close(fd);
			errno = EILSEQ;
			return -1;
		}
		/* The old format had no identity.  It is safe to use only for the
		 * original incarnation; treating it as a reused incarnation would turn
		 * an ambiguous numeric-ID value into a false rejection. */
		for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		{
			page_reclaimed_frontier[tl][0].incarnation = 1;
			page_reclaimed_frontier[tl][0].fence = legacy.frontiers[tl];
		}
	}
	else
	{
		close(fd);
		errno = EILSEQ;
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	return 0;
}

static PsPruneFence
page_frontier_current(uint32_t timeline)
{
	PsPruneFence zero = {0, 0};
	uint64_t incarnation;

	if (timeline >= MAX_TIMELINES)
		return zero;
	incarnation = __atomic_load_n(&timelines[timeline].incarnation,
									 __ATOMIC_ACQUIRE);
	if (incarnation == 0)
		return zero;
	for (uint32_t slot = 0; slot < PS_PAGE_FRONTIER_SLOTS; slot++)
		if (page_reclaimed_frontier[timeline][slot].incarnation == incarnation)
			return page_reclaimed_frontier[timeline][slot].fence;
	return zero;
}

static PsPageFrontierEntry *
page_frontier_slot(uint32_t timeline, uint64_t incarnation, int create)
{
	PsPageFrontierEntry *oldest = NULL;

	for (uint32_t slot = 0; slot < PS_PAGE_FRONTIER_SLOTS; slot++)
	{
		PsPageFrontierEntry *entry = &page_reclaimed_frontier[timeline][slot];

		if (entry->incarnation == incarnation)
			return entry;
		if (entry->incarnation == 0 || oldest == NULL ||
			entry->incarnation < oldest->incarnation)
			oldest = entry;
	}
	if (!create || oldest == NULL)
		return NULL;
	oldest->incarnation = incarnation;
	oldest->fence = (PsPruneFence) {0, 0};
	return oldest;
}

static int
page_frontier_advance(uint32_t timeline, uint64_t floor,
					  uint64_t admission_seq)
{
	PsPageFrontierEntry old;
	PsPageFrontierEntry *entry;
	PsPruneFence next;
	uint64_t incarnation;

	if (timeline >= MAX_TIMELINES || floor == 0 ||
		(incarnation = __atomic_load_n(&timelines[timeline].incarnation,
												 __ATOMIC_ACQUIRE)) == 0)
		return -1;
	entry = page_frontier_slot(timeline, incarnation, 1);
	if (entry == NULL)
		return -1;
	old = *entry;
	if (old.incarnation != incarnation)
		return -1;
	old.fence = entry->fence;
	next.lsn = floor;
	next.admission_seq = admission_seq;
	if (next.lsn < old.fence.lsn ||
		(next.lsn == old.fence.lsn && next.admission_seq <= old.fence.admission_seq))
		return 0;
	entry->fence = next;
	if (page_frontier_publish() != 0)
	{
		*entry = old;
		return -1;
	}
	return 0;
}

/* A reader pin on a descendant is projected while compaction runs on an
 * ancestor.  Match that same projection here: the ancestor's frontier may
 * have advanced past the branch point while the projected page version is
 * still deliberately retained.  Caller holds map_lock. */
static int
page_frontier_projected_fence_active(uint32_t reader_timeline,
								 uint32_t timeline, uint64_t lsn,
								 uint64_t admission_seq)
{
	PsRetentionPin *pins = NULL;
	uint32_t npins = 0;
	int active = 0;

	if (ps_retention_snapshot_alloc(&pins, &npins) != 0)
		return 0;
	for (uint32_t i = 0; i < npins; i++)
	{
		uint64_t projected;

		if ((pins[i].resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) == 0 ||
			pins[i].timeline != reader_timeline)
			continue;
		projected = pins[i].lsn;
		if (retention_project_lsn(reader_timeline, timeline, &projected) &&
			projected == lsn && pins[i].admission_seq == admission_seq)
		{
			active = 1;
			break;
		}
	}
	free(pins);
	return active;
}

/* Every live child is a structural page-prune fence at its branch point,
 * independent of whether the child currently has an explicit owner pin.
 * page_prune_fences() retains that inherited version, so an as-of child read
 * must be allowed to reach it even if the parent's global frontier moved on. */
static int
page_frontier_structural_fence_active(uint32_t reader_timeline,
									  uint32_t timeline, uint64_t lsn)
{
	uint32_t current = reader_timeline;
	uint32_t hops = 0;

	while (current != timeline)
	{
		if (current >= MAX_TIMELINES || !timelines[current].defined ||
			!timeline_has_parent(current) || ++hops > MAX_TIMELINES)
			return 0;
		if (timelines[current].branch_lsn == lsn)
			return 1;
		current = (uint32_t) timelines[current].parent;
	}
	return 0;
}

static int
page_frontier_allows(uint32_t timeline, uint32_t reader_timeline,
					 uint64_t lsn, uint64_t admission_seq)
{
	PsPruneFence frontier;

	if (timeline >= MAX_TIMELINES)
		return 0;
	frontier = page_frontier_current(timeline);
	if (lsn < frontier.lsn &&
		!((admission_seq == 0 &&
		   page_frontier_structural_fence_active(reader_timeline, timeline, lsn)) ||
		  (admission_seq != 0 &&
		   (ps_retention_page_fence_active(timeline, lsn, admission_seq) ||
			page_frontier_projected_fence_active(reader_timeline, timeline,
											lsn, admission_seq)))))
		return 0;
	/* Sequence zero is the established uncapped/latest-visible fence. */
	if (admission_seq != 0 &&
		lsn == frontier.lsn &&
		admission_seq < frontier.admission_seq &&
		!ps_retention_page_fence_active(timeline, lsn, admission_seq) &&
		!page_frontier_projected_fence_active(reader_timeline, timeline,
									  lsn, admission_seq))
		return 0;
	return 1;
}

static uint32_t
walidx_frontier_crc(const PsWalIdxFrontierState *state)
{
	return fnv(state, offsetof(PsWalIdxFrontierState, crc));
}

static int
walidx_frontier_publish(void)
{
	PsWalIdxFrontierState state;
	char		tmp[4096];
	int			fd = -1;
	int			n;
	int			rc = -1;

	memset(&state, 0, sizeof(state));
	state.magic = PS_WALIDX_FRONTIER_MAGIC;
	state.version = PS_WALIDX_FRONTIER_VERSION;
	memcpy(state.entries, walidx_reclaimed_frontier, sizeof(state.entries));
	state.crc = walidx_frontier_crc(&state);
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", walidx_frontier_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
		fsync(fd) != 0)
		goto done;
	if (close(fd) != 0)
	{
		fd = -1;
		goto done;
	}
	fd = -1;
	if (rename(tmp, walidx_frontier_path) != 0 ||
		fsync_dir_path(walidx_frontier_dir) != 0)
		goto done;
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		unlink(tmp);
	return rc;
}

static int
walidx_frontier_load(const char *store_dir)
{
	PsWalIdxFrontierState state;
	PsWalIdxFrontierStateV1 legacy;
	struct stat st;
	int			fd;
	int			n;

	memset(walidx_reclaimed_frontier, 0,
		   sizeof(walidx_reclaimed_frontier));
	n = snprintf(walidx_frontier_dir, sizeof(walidx_frontier_dir), "%s",
				 store_dir);
	if (n < 0 || (size_t) n >= sizeof(walidx_frontier_dir))
		return -1;
	n = snprintf(walidx_frontier_path, sizeof(walidx_frontier_path),
				 "%s/walidx-prune.frontiers", store_dir);
	if (n < 0 || (size_t) n >= sizeof(walidx_frontier_path))
		return -1;
	fd = open(walidx_frontier_path, O_RDONLY);
	if (fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (fstat(fd, &st) != 0)
	{
		close(fd);
		return -1;
	}
	if (st.st_size == (off_t) sizeof(state))
	{
		if (read(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
			state.magic != PS_WALIDX_FRONTIER_MAGIC ||
			state.version != PS_WALIDX_FRONTIER_VERSION ||
			state.crc != walidx_frontier_crc(&state))
		{
			close(fd);
			errno = EILSEQ;
			return -1;
		}
		memcpy(walidx_reclaimed_frontier, state.entries, sizeof(state.entries));
	}
	else if (st.st_size == (off_t) sizeof(legacy))
	{
		if (read(fd, &legacy, sizeof(legacy)) != (ssize_t) sizeof(legacy) ||
			legacy.magic != PS_WALIDX_FRONTIER_MAGIC || legacy.version != 1 ||
			legacy.crc != fnv(&legacy, offsetof(PsWalIdxFrontierStateV1, crc)))
		{
			close(fd);
			errno = EILSEQ;
			return -1;
		}
		/* As with page frontiers, an unkeyed legacy value belongs only to
		 * incarnation one.  Reused IDs must rebuild from their own WAL. */
		for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		{
			walidx_reclaimed_frontier[tl][0].incarnation = 1;
			walidx_reclaimed_frontier[tl][0].frontier = legacy.frontiers[tl];
		}
	}
	else
	{
		close(fd);
		errno = EILSEQ;
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	return 0;
}

static uint64_t
walidx_frontier_current(uint32_t timeline)
{
	uint64_t incarnation;

	if (timeline >= MAX_TIMELINES)
		return 0;
	incarnation = __atomic_load_n(&timelines[timeline].incarnation,
									 __ATOMIC_ACQUIRE);
	if (incarnation == 0)
		return 0;
	for (uint32_t slot = 0; slot < PS_PAGE_FRONTIER_SLOTS; slot++)
		if (walidx_reclaimed_frontier[timeline][slot].incarnation == incarnation)
			return walidx_reclaimed_frontier[timeline][slot].frontier;
	return 0;
}

static PsWalIdxFrontierEntry *
walidx_frontier_slot(uint32_t timeline, uint64_t incarnation, int create)
{
	PsWalIdxFrontierEntry *oldest = NULL;

	for (uint32_t slot = 0; slot < PS_PAGE_FRONTIER_SLOTS; slot++)
	{
		PsWalIdxFrontierEntry *entry = &walidx_reclaimed_frontier[timeline][slot];

		if (entry->incarnation == incarnation)
			return entry;
		if (entry->incarnation == 0 || oldest == NULL ||
			entry->incarnation < oldest->incarnation)
			oldest = entry;
	}
	if (!create || oldest == NULL)
		return NULL;
	oldest->incarnation = incarnation;
	oldest->frontier = 0;
	return oldest;
}

static int
walidx_frontier_advance(uint32_t timeline, uint64_t frontier)
{
	PsWalIdxFrontierEntry old;
	PsWalIdxFrontierEntry *entry;
	uint64_t incarnation;

	if (timeline >= MAX_TIMELINES || frontier == 0 ||
		(incarnation = __atomic_load_n(&timelines[timeline].incarnation,
												 __ATOMIC_ACQUIRE)) == 0)
		return -1;
	entry = walidx_frontier_slot(timeline, incarnation, 1);
	if (entry == NULL)
		return -1;
	old = *entry;
	if (frontier <= old.frontier)
		return 0;
	entry->frontier = frontier;
	if (walidx_frontier_publish() != 0)
	{
		*entry = old;
		return -1;
	}
	return 0;
}

/* Caller holds map_lock.  Every active WAL-index pin and every descendant
 * branch point is an exact exception represented in a compacted snapshot. */
static int
walidx_frontier_exception_active(uint32_t timeline, uint64_t lsn)
{
	PsRetentionPin *pins = NULL;
	uint32_t npins = 0;
	int active = 0;

	if (ps_retention_snapshot_alloc(&pins, &npins) != 0)
		return 0;
	for (uint32_t i = 0; i < npins; i++)
		if ((pins[i].resources & PS_RETENTION_RESOURCE_WAL_INDEX) != 0)
		{
			uint64_t projected = pins[i].lsn;

			if (retention_project_lsn(pins[i].timeline, timeline, &projected) &&
				projected == lsn)
			{
				active = 1;
				break;
			}
		}
	free(pins);
	if (active)
		return 1;
	for (uint32_t candidate = 0; candidate < MAX_TIMELINES; candidate++)
	{
		uint64_t projected = UINT64_MAX;
		PsTimelineState state;

		if (candidate != timeline && timelines[candidate].defined &&
			ps_timeline_state(candidate, &state, NULL) &&
			state != PS_TIMELINE_DELETED &&
			retention_project_lsn(candidate, timeline, &projected) &&
			projected == lsn)
			return 1;
	}
	return 0;
}

static int
walidx_frontier_allows(uint32_t timeline, uint64_t lsn)
{
	if (timeline >= MAX_TIMELINES)
		return 0;
	return lsn >= walidx_frontier_current(timeline) ||
		walidx_frontier_exception_active(timeline, lsn);
}

static int
key_eq(const PsKey *a, const PsKey *b)
{
	return a->spcOid == b->spcOid && a->dbOid == b->dbOid &&
		a->relNumber == b->relNumber && a->forkNum == b->forkNum &&
		a->klass == b->klass;
}

/* --- page index (keyed by timeline, key, block) --- */

static uint32_t
page_hash(uint32_t timeline, const PsKey *key, uint32_t block)
{
	return fnv(key, sizeof(*key)) ^ (block * 2654435761u) ^ (timeline * 40503u);
}

static ForkEnt *fork_get_or_create(uint32_t timeline, const PsKey *key);
static ForkEnt *fork_find(uint32_t timeline, const PsKey *key);

static PageEnt *
page_find(uint32_t timeline, const PsKey *key, uint32_t block)
{
	uint32_t	h = page_hash(timeline, key, block);
	Shard	   *s = shard_for(key);
	PageEnt    *e;

	for (e = s->page_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && e->block == block && key_eq(&e->key, key))
			return e;
	return NULL;
}

/* Rewrite one POSIX segment after validating every record in it.  The raw
 * record bytes are copied unchanged for survivors; only their physical
 * offsets move.  Caller holds every shard write lock and map write lock. */
static int
page_cleanup_rewrite_segment(Shard *s, int seg, uint32_t target)
{
	int64_t bytes;
	unsigned char *replacement = NULL;
	SegmentReloc *relocs = NULL;
	uint32_t nrelocs = 0, reloc_cap = 0;
	uint64_t off = 0, out_off = 0;
	uint64_t watermark_new = 0, cursor_new = 0;
	int watermark_seen = 0, cursor_seen = 0, cursor_retired = 0, found = 0;
	int counted_debt = 0;
	PsFlushWatermark watermark = {0};
	int have_watermark = s->flush_watermark_valid &&
		s->flush_watermark.seg_id == (uint32_t) seg;

	errno = 0;
	bytes = ps_storage->seg_size(s->id, seg);
	if (bytes < 0 || (uint64_t) bytes > segment_size ||
		(uint64_t) bytes > SIZE_MAX ||
		(uint64_t) bytes > (uint64_t) LLONG_MAX)
		return -1;
	/* PAGE debt is an aggregate of nonempty, covered segments in the
	 * reclaimable prefix.  A successful deletion rewrite can turn one such
	 * segment into an empty file before segment GC sees it. */
	counted_debt = page_reclaim_high_water_bytes != 0 && bytes > 0 &&
		s->flush_watermark_valid && (uint32_t) seg < s->flush_watermark.seg_id &&
		(uint32_t) seg >= s->gc_next_seg && s->gc_debt_segments != 0;
	if (have_watermark)
	{
		watermark = s->flush_watermark;
		if (watermark.seg_off > (uint64_t) bytes)
			return -1;
		watermark_seen = watermark.seg_off == 0;
		watermark_new = 0;
	}
	if (s->cur_seg == seg)
	{
		if (s->cur_off > (uint64_t) bytes)
		{
			/* Recovery uses segment_size as a retirement sentinel when an
			 * ordered record is physically present but its forkmeta marker is
			 * missing.  It is not an append cursor into this short segment:
			 * cleanup may rewrite the physical bytes, but must preserve the
			 * sentinel so the next append rolls to a new segment. */
			if (s->cur_off != segment_size ||
				(uint64_t) bytes >= segment_size)
				return -1;
			cursor_retired = 1;
		}
		else
			cursor_seen = s->cur_off == 0;
	}
	replacement = malloc((size_t) bytes ? (size_t) bytes : 1);
	if (!replacement)
		return -1;
	while (off < (uint64_t) bytes)
	{
		SegRecHdr hdr;
		uint64_t header_size, rec_len, end;
		uint64_t order_id = 0, admission_seq = 0;
		int wal_less, bound, admission;
		unsigned char *raw;

		if ((uint64_t) bytes - off < sizeof(hdr) ||
			ps_storage->seg_read(s->id, seg, off, &hdr, sizeof(hdr)) != 0 ||
			segment_record_shape(hdr.magic, &header_size, &wal_less,
								 &bound, &admission) != 0 ||
			hdr.timeline >= MAX_TIMELINES || hdr.len != page_size ||
			header_size > UINT64_MAX - hdr.len)
			goto fail;
		rec_len = header_size + hdr.len;
		if (rec_len > UINT32_MAX || rec_len > (uint64_t) bytes - off)
			goto fail;
		end = off + rec_len;
		if (bound &&
			(ps_storage->seg_read(s->id, seg, off + sizeof(hdr), &order_id,
							   sizeof(order_id)) != 0 || order_id == 0))
			goto fail;
		if (admission &&
			(ps_storage->seg_read(s->id, seg,
							 off + header_size - sizeof(admission_seq),
							 &admission_seq, sizeof(admission_seq)) != 0 ||
			 admission_seq == 0))
			goto fail;
		raw = malloc((size_t) rec_len);
		if (!raw || ps_storage->seg_read(s->id, seg, off, raw, (uint32_t) rec_len) != 0)
		{
			free(raw);
			goto fail;
		}
		if (hdr.timeline == target)
		{
			found = 1;
			free(raw);
		}
		else
		{
			uint64_t new_data_off = out_off + header_size;

			if (out_off > (uint64_t) bytes - rec_len)
			{
				free(raw);
				goto fail;
			}
			memcpy(replacement + out_off, raw, (size_t) rec_len);
			if (nrelocs == reloc_cap)
			{
				uint32_t new_cap;
				size_t alloc_size;
				SegmentReloc *nr;

				if (reloc_cap != 0 && reloc_cap > UINT32_MAX / 2)
				{
					free(raw);
					goto fail;
				}
				new_cap = reloc_cap ? reloc_cap * 2 : 128;
				alloc_size = (size_t) new_cap * sizeof(*relocs);
				if (new_cap != 0 &&
					alloc_size / sizeof(*relocs) != (size_t) new_cap)
				{
					free(raw);
					goto fail;
				}
				nr = realloc(relocs, alloc_size);

				if (!nr)
				{
					free(raw);
					goto fail;
				}
				relocs = nr;
				reloc_cap = new_cap;
			}
			relocs[nrelocs++] = (SegmentReloc) {
				.timeline = hdr.timeline,
				.key = hdr.key,
				.block = hdr.block,
				.lsn = wal_less ? 0 : hdr.lsn,
				.admission_seq = admission_seq,
				.old_off = off + header_size,
				.new_off = new_data_off
			};
			out_off += rec_len;
			free(raw);
		}
		if (have_watermark && end == watermark.seg_off)
		{
			watermark_seen = 1;
			watermark_new = out_off;
		}
		if (s->cur_seg == seg && end == s->cur_off)
		{
			cursor_seen = 1;
			cursor_new = out_off;
		}
		off = end;
	}
	if (have_watermark && !watermark_seen)
		goto fail;
	if (s->cur_seg == seg && !cursor_seen && !cursor_retired)
		goto fail;
	if (!found)
		goto no_rewrite;
	if (ps_storage->seg_rewrite == NULL)
		goto fail;
	/* Retreat the recovery boundary before replacement.  This is conservative
	 * when a crash occurs between the two operations: either old or new bytes
	 * are scanned, and durable layers still cover the original prefix. */
	if (have_watermark && watermark_new < watermark.seg_off)
	{
		/* Image indexes retain source-segment offsets.  Once bytes inside the
		 * covered prefix move, an index entry after the removed record can no
		 * longer be used as a recovery hint.  Rewind the whole segment boundary
		 * so recovery rebuilds those entries from the replacement bytes instead
		 * of consulting stale layer offsets. */
		watermark_new = 0;
		if (ps_manifest_rebase_flush_watermark(s->id, (uint32_t) seg,
											watermark_new) != 0)
			goto fail;
		s->flush_watermark.seg_off = watermark_new;
	}
	if (ps_storage->seg_rewrite(s->id, seg, replacement, out_off) != 0)
		goto fail;
	for (uint32_t i = 0; i < nrelocs; i++)
	{
		SegmentReloc *r = &relocs[i];

		for (uint32_t sh = 0; sh < core_shards(); sh++)
			for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
				for (PageEnt *e = g_shards[sh].page_idx[bucket]; e; e = e->next)
					if (e->timeline == r->timeline && e->block == r->block &&
						key_eq(&e->key, &r->key))
						for (int v = 0; v < e->nver; v++)
							if (e->vers[v].shard == s->id &&
								e->vers[v].seg == seg &&
								e->vers[v].off == r->old_off &&
								e->vers[v].lsn == r->lsn &&
								e->vers[v].admission_seq == r->admission_seq)
								e->vers[v].off = r->new_off;
		ps_memtable_rewrite_segment(s->memtable, (uint32_t) seg,
								r->old_off, r->new_off);
	}
	if (s->cur_seg == seg)
		s->cur_off = cursor_retired ? segment_size : cursor_new;
	if (out_off == 0 && counted_debt)
		page_gc_debt_settle(s, (uint32_t) seg, 1);
	free(relocs);
	free(replacement);
	return 1;

no_rewrite:
	free(relocs);
	free(replacement);
	return 0;
fail:
	free(relocs);
	free(replacement);
	return -1;
}

/* Remove target-owned in-memory page/fork entries after all physical segments
 * have been successfully filtered.  A deleting timeline is not readable, so
 * no historical PageVer is needed after this point. */
static void
page_cleanup_purge_timeline_locked(uint32_t timeline)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		{
			PageEnt **link = &g_shards[sh].page_idx[bucket];

			while (*link)
			{
				PageEnt *e = *link;

				if (e->timeline != timeline)
				{
					link = &e->next;
					continue;
				}
				*link = e->next;
				free(e->vers);
				free(e);
			}
		}
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		{
			ForkEnt **link = &g_shards[sh].fork_idx[bucket];

			while (*link)
			{
				ForkEnt *e = *link;

				if (e->timeline != timeline)
				{
					link = &e->next;
					continue;
				}
				*link = e->next;
				free(e->ev);
				free(e->def_idx);
				free(e);
			}
		}
}

static int
page_cleanup_has_index_entries_locked(uint32_t timeline)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		{
			for (PageEnt *p = g_shards[sh].page_idx[bucket]; p; p = p->next)
				if (p->timeline == timeline)
					return 1;
			for (ForkEnt *f = g_shards[sh].fork_idx[bucket]; f; f = f->next)
				if (f->timeline == timeline)
					return 1;
		}
	return 0;
}

/* Caller holds every shard write lock and map write lock.  One successful
 * replacement is enough work for a maintenance turn; the next turn rescans,
 * which makes the operation restartable without a durable done bit. */
static int
page_cleanup_scan_timeline_locked(uint32_t timeline)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
	{
		Shard *s = &g_shards[sh];

		/* GC may remove a prefix segment while leaving later segments.  The
		 * append cursor is the authoritative finite scan bound; a missing
		 * segment inside that range is a hole, not end-of-log. */
		for (int seg = 0; seg <= s->cur_seg; seg++)
		{
			int64_t size;

			errno = 0;
			size = ps_storage->seg_size(sh, seg);
			if (size < 0)
			{
				if (errno == ENOENT)
					continue;
				return -1;
			}
			{
				int rc = page_cleanup_rewrite_segment(s, seg, timeline);

				if (rc < 0)
					return -1;
				if (rc > 0)
					return 1;
			}
		}
	}
	return 0;
}

/* Forget versions omitted from a durably published compacted layer.  The
 * caller holds this shard's write lock and map write lock, so page chains
 * cannot change while the replacement layer and index become consistent. */
static void
page_remove_compacted_versions(uint32_t timeline, const PsImgRec *recs,
							   uint32_t nrec)
{
	for (uint32_t r = 0; r < nrec;)
	{
		uint32_t	end = r + 1;
		PageEnt    *e = page_find(timeline, &recs[r].key, recs[r].block);
		int			out = 0;

		while (end < nrec && recs[end].block == recs[r].block &&
			   key_eq(&recs[end].key, &recs[r].key))
			end++;

		if (e == NULL)
		{
			r = end;
			continue;
		}
		/* dropped identities are sorted by (LSN, admission sequence).  Compact
		 * this page's live version array once; binary lookup avoids repeatedly
		 * shifting a hot page while the shard write lock is held. */
		for (int i = 0; i < e->nver; i++)
		{
			PageVer    *v = &e->vers[i];
			uint32_t	lo = r;
			uint32_t	hi = end;
			int			remove = 0;

			while (lo < hi)
			{
				uint32_t mid = lo + (hi - lo) / 2;

				if (recs[mid].lsn < v->lsn ||
					(recs[mid].lsn == v->lsn &&
					 recs[mid].admission_seq < v->admission_seq))
					lo = mid + 1;
				else
					hi = mid;
			}
			if (lo < end && recs[lo].lsn == v->lsn &&
				recs[lo].admission_seq == v->admission_seq)
				remove = 1;
			if (!remove)
				e->vers[out++] = *v;
		}
		e->nver = out;
		r = end;
	}
	for (uint32_t r = 0; r < nrec; r++)
		if (recs[r].lsn == 0)
		{
			ForkEnt    *fork = fork_find(timeline, &recs[r].key);
			Shard	   *shard = shard_for(&recs[r].key);
			int			found = 0;

			for (uint32_t b = 0; b < IDX_BUCKETS && !found; b++)
				for (PageEnt *e = shard->page_idx[b]; e && !found; e = e->next)
					if (e->timeline == timeline && key_eq(&e->key, &recs[r].key))
						for (int i = 0; i < e->nver; i++)
							if (e->vers[i].lsn == 0)
							{
								found = 1;
								break;
							}
			if (fork != NULL)
				fork->has_wal_less = found;
		}
}

/*
 * Record a new version of (timeline, key, block).  Only ever appends to the
 * version chain -- existing versions are never dropped -- which is what makes
 * the store copy-on-write.  The version is tagged with the writing timeline, so
 * a branch's writes never disturb its parent.  Called from append_page and from
 * recover() while replaying segments.
 */
static void
page_add_version(uint32_t timeline, const PsKey *key, uint32_t block,
				 uint64_t lsn, uint64_t admission_seq, uint32_t shard,
				 int seg, uint64_t off)
{
	uint32_t	h = page_hash(timeline, key, block);
	Shard	   *s = shard_for(key);
	PageEnt    *e = page_find(timeline, key, block);
	ForkEnt    *fork = fork_get_or_create(timeline, key);

	timeline_mark_used(timeline);
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->block = block;
		e->fork_next = fork->pages;
		fork->pages = e;
		e->next = s->page_idx[h & IDX_MASK];
		s->page_idx[h & IDX_MASK] = e;
	}
	if (e->nver == e->cap)		/* grow the version array geometrically */
	{
		e->cap = e->cap ? e->cap * 2 : 2;
		e->vers = realloc(e->vers, (size_t) e->cap * sizeof(PageVer));
	}
	e->vers[e->nver].shard = shard;
	e->vers[e->nver].lsn = lsn;
	e->vers[e->nver].admission_seq = admission_seq;
	e->vers[e->nver].seg = seg;
	e->vers[e->nver].off = off;
	e->nver++;
	if (lsn > fork->last_page_lsn ||
		(lsn == fork->last_page_lsn && admission_seq > fork->last_page_seq))
	{
		fork->last_page_lsn = lsn;
		fork->last_page_seq = admission_seq;
	}
	if (lsn == 0)
		fork->has_wal_less = 1;
}

/* Newest version on this entry with lsn <= read_lsn, or NULL if none. */
static PageVer *
page_visible(PageEnt *e, uint64_t read_lsn, uint64_t read_seq)
{
	PageVer    *best = NULL;

	for (int i = 0; i < e->nver; i++)
	{
		PageVer    *v = &e->vers[i];

		if (v->lsn <= read_lsn &&
			(v->lsn < read_lsn || read_seq == 0 || v->admission_seq == 0 ||
			 v->admission_seq <= read_seq) &&
			(!best || v->lsn > best->lsn ||
			 (v->lsn == best->lsn &&
			  v->admission_seq >= best->admission_seq)))
			best = v;
	}
	return best;
}

/* --- fork size index (keyed by timeline, key) --- */

static int fork_meta_persist(uint32_t timeline, const PsKey *key, uint64_t lsn,
							 uint64_t admission_seq, uint32_t nblocks,
							 uint8_t kind);

static ForkEnt *
fork_find(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	Shard	   *s = shard_for(key);
	ForkEnt    *e;

	for (e = s->fork_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && key_eq(&e->key, key))
			return e;
	return NULL;
}

static ForkEnt *
fork_get_or_create(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	Shard	   *s = shard_for(key);
	ForkEnt    *e = fork_find(timeline, key);

	timeline_mark_used(timeline);
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->nblocks = 0;
		e->next = s->fork_idx[h & IDX_MASK];
		s->fork_idx[h & IDX_MASK] = e;
	}
	return e;
}

/*
 * Resolve one timeline hop's contribution to an as-of size/existence query.
 * Scans the event history newest-first below the cap: the newest SET/DEAD is
 * definitive for this hop (with any GROW above it still counted -- writes to
 * a dead or truncated fork re-extend it); bare GROWs are a lower bound that
 * still combines with ancestor hops.
 */
#define FORK_HOP_NONE	0		/* no events at/below the cap */
#define FORK_HOP_GROW	1		/* growth only: combine with ancestors */
#define FORK_HOP_DEF	2		/* definitive size (SET, or DEAD then regrown) */
#define FORK_HOP_DEAD	3		/* definitively unlinked at the cap */

static int
fork_asof_hop(const ForkEnt *e, uint64_t cap, uint64_t seq_cap,
			  uint32_t *nb_out)
{
	*nb_out = 0;
	if (seq_cap == 0)
	{
		uint32_t lo = 0;
		uint32_t hi = e->nev;

		while (lo < hi)
		{
			uint32_t mid = lo + (hi - lo) / 2;

			if (e->ev[mid].lsn <= cap)
				lo = mid + 1;
			else
				hi = mid;
		}
		if (lo == 0)
			return FORK_HOP_NONE;
		*nb_out = e->ev[lo - 1].cached_nblocks;
		return e->ev[lo - 1].cached_state;
	}
	{
		uint32_t	first = 0;
		uint32_t	end = e->nev;
		uint8_t		state;
		uint32_t	nb;

		/* Everything below cap is visible regardless of admission sequence.
		 * Reuse its cached fold, then inspect only the equal-cap run. */
		while (first < end)
		{
			uint32_t mid = first + (end - first) / 2;

			if (e->ev[mid].lsn < cap)
				first = mid + 1;
			else
				end = mid;
		}
		state = first == 0 ? FORK_HOP_NONE : e->ev[first - 1].cached_state;
		nb = first == 0 ? 0 : e->ev[first - 1].cached_nblocks;
		for (uint32_t i = first; i < e->nev && e->ev[i].lsn == cap; i++)
		{
			const ForkEvent *v = &e->ev[i];

			if (v->admission_seq != 0 && v->admission_seq > seq_cap)
				continue;
			if (v->kind == FEV_GROW)
			{
				if (v->nblocks > nb)
					nb = v->nblocks;
				state = (state == FORK_HOP_NONE || state == FORK_HOP_GROW) ?
					FORK_HOP_GROW : FORK_HOP_DEF;
			}
			else if (v->kind == FEV_SET)
			{
				nb = v->nblocks;
				state = FORK_HOP_DEF;
			}
			else if (v->kind == FEV_DEAD)
			{
				nb = 0;
				state = FORK_HOP_DEAD;
			}
		}
		*nb_out = nb;
		return state;
	}
}

static void
fork_event_cache_from(ForkEnt *e, uint32_t start)
{
	uint8_t state = start == 0 ? FORK_HOP_NONE : e->ev[start - 1].cached_state;
	uint32_t nb = start == 0 ? 0 : e->ev[start - 1].cached_nblocks;
	uint32_t fence = start == 0 ? UINT32_MAX :
		e->ev[start - 1].cached_fence_nblocks;

	for (uint32_t i = start; i < e->nev; i++)
	{
		ForkEvent *v = &e->ev[i];

		if (v->kind == FEV_GROW)
		{
			if (v->nblocks > nb)
				nb = v->nblocks;
			state = (state == FORK_HOP_NONE || state == FORK_HOP_GROW) ?
				FORK_HOP_GROW : FORK_HOP_DEF;
		}
		else if (v->kind == FEV_SET)
		{
			nb = v->nblocks;
			state = FORK_HOP_DEF;
			if (v->nblocks < fence)
				fence = v->nblocks;
		}
		else if (v->kind == FEV_DEAD)
		{
			nb = 0;
			state = FORK_HOP_DEAD;
			fence = 0;
		}
		v->cached_nblocks = nb;
		v->cached_state = state;
		v->cached_fence_nblocks = fence;
	}
}

/* Keep a compact index over definitive lifecycle events.  Inserts can shift
 * existing event offsets, but their cost is proportional to the number of
 * truncates/unlinks rather than the usually much larger number of GROWs. */
static void
fork_def_index_insert(ForkEnt *e, uint32_t event_idx, int definitive)
{
	uint32_t	pos = 0;

	while (pos < e->ndef && e->def_idx[pos] < event_idx)
		pos++;
	for (uint32_t i = pos; i < e->ndef; i++)
		e->def_idx[i]++;
	if (!definitive)
		return;
	if (e->ndef == e->defcap)
	{
		e->defcap = e->defcap ? e->defcap * 2 : 4;
		e->def_idx = realloc(e->def_idx,
			(size_t) e->defcap * sizeof(*e->def_idx));
	}
	memmove(&e->def_idx[pos + 1], &e->def_idx[pos],
		(size_t) (e->ndef - pos) * sizeof(*e->def_idx));
	e->def_idx[pos] = event_idx;
	e->ndef++;
}

/* Size of e as of cap, hop-local (for the GROW-dedup below). */
static uint32_t
fork_size_asof_hop(const ForkEnt *e, uint64_t cap, uint64_t seq_cap)
{
	uint32_t	nb;

	(void) fork_asof_hop(e, cap, seq_cap, &nb);
	return nb;
}

/* A later truncate/drop invalidates old page bytes even if subsequent growth
 * makes the block addressable again.  The current fast path avoids touching
 * event history unless a definitive event is newer than the selected page. */
static int
fork_page_invalidated(const ForkEnt *e, uint32_t block, const PageVer *page,
					  uint64_t cap, uint64_t seq_cap)
{
	if (e == NULL || page == NULL || e->last_def_lsn < page->lsn)
		return 0;
	for (int i = (int) e->ndef - 1; i >= 0; i--)
	{
		const ForkEvent *v = &e->ev[e->def_idx[i]];

		if (v->lsn > cap ||
			(seq_cap != 0 && v->lsn == cap && v->admission_seq != 0 &&
			 v->admission_seq > seq_cap) ||
			(v->kind != FEV_SET && v->kind != FEV_DEAD))
			continue;
		/* Nonzero LSN is the primary order, including across legacy and
		 * sequenced records.  WAL-less records have no LSN order, so retain
		 * their established admission-order semantics. */
		if (v->lsn != 0 && page->lsn != 0)
		{
			if (v->lsn < page->lsn)
				break;
			if (v->lsn == page->lsn &&
				v->admission_seq <= page->admission_seq)
				continue;
		}
		else if (v->admission_seq <= page->admission_seq)
			continue;
		if (v->kind == FEV_DEAD || block >= v->nblocks)
			return 1;
	}
	return 0;
}

static int
fork_inheritance_fenced(const ForkEnt *e, uint32_t block,
						uint64_t cap, uint64_t seq_cap)
{
	if (e == NULL)
		return 0;
	if (seq_cap == 0)
	{
		uint32_t lo = 0;
		uint32_t hi = e->nev;

		while (lo < hi)
		{
			uint32_t mid = lo + (hi - lo) / 2;

			if (e->ev[mid].lsn <= cap)
				lo = mid + 1;
			else
				hi = mid;
		}
		return lo != 0 && e->ev[lo - 1].cached_fence_nblocks != UINT32_MAX &&
			block >= e->ev[lo - 1].cached_fence_nblocks;
	}
	{
		uint32_t first = 0;
		uint32_t end = e->nev;
		uint32_t fence;

		while (first < end)
		{
			uint32_t mid = first + (end - first) / 2;

			if (e->ev[mid].lsn < cap)
				first = mid + 1;
			else
				end = mid;
		}
		fence = first == 0 ? UINT32_MAX :
			e->ev[first - 1].cached_fence_nblocks;
		for (uint32_t i = first; i < e->nev && e->ev[i].lsn == cap; i++)
		{
			const ForkEvent *v = &e->ev[i];

			if (v->admission_seq != 0 && v->admission_seq > seq_cap)
				continue;
			if (v->kind == FEV_DEAD)
				fence = 0;
			else if (v->kind == FEV_SET && v->nblocks < fence)
				fence = v->nblocks;
		}
		return fence != UINT32_MAX && block >= fence;
	}
}

/* Markerless SEG0 spans an intermediate format transition: some stores already
 * persisted the same growth in forkmeta, while later ones relied on SEG0 alone.
 * Detect the former without re-evaluating equal-LSN definitive-event order. */
static int
fork_has_growth_at(const ForkEnt *e, uint64_t lsn, uint32_t nblocks)
{
	for (uint32_t i = 0; i < e->nev; i++)
		if (e->ev[i].kind == FEV_GROW && e->ev[i].lsn == lsn &&
			e->ev[i].nblocks >= nblocks)
			return 1;
	return 0;
}

/* A retry can follow page growth at the CREATE's LSN.  The last lifecycle
 * boundary, not the last event, determines whether that CREATE already made
 * an empty generation. */
static int
fork_has_create_at(const ForkEnt *e, uint64_t lsn)
{
	const ForkEvent *last_def = NULL;

	for (uint32_t i = 0; i < e->nev; i++)
		if (e->ev[i].lsn == lsn &&
			(e->ev[i].kind == FEV_SET || e->ev[i].kind == FEV_DEAD))
			last_def = &e->ev[i];
	return last_def != NULL && last_def->kind == FEV_SET &&
		last_def->nblocks == 0;
}

/*
 * Record a fork-size event, keeping the history lsn-ordered (equal LSNs keep
 * arrival order, so a later definitive event at the same LSN wins a
 * newest-first scan).  GROW events that do not raise the size visible at
 * their own LSN are dropped: steady-state rewrites of existing blocks at ever
 * newer pd_lsns add nothing, so the history stays O(distinct sizes).
*/
static _Thread_local int fork_event_cache_defer = 0;

static void
fork_event_add(ForkEnt *e, uint64_t lsn, uint64_t admission_seq,
			   uint32_t nblocks, uint8_t kind)
{
	uint32_t	i;

	if (!fork_event_cache_defer && kind == FEV_GROW &&
		fork_size_asof_hop(e, lsn, admission_seq) >= nblocks)
		return;
	if (kind != FEV_GROW && lsn > e->last_def_lsn)
		e->last_def_lsn = lsn;
	if (e->nev == e->evcap)
	{
		e->evcap = e->evcap ? e->evcap * 2 : 4;
		e->ev = realloc(e->ev, e->evcap * sizeof(ForkEvent));
	}
	i = e->nev;
	while (i > 0 &&
		   (e->ev[i - 1].lsn > lsn ||
			(e->ev[i - 1].lsn == lsn && admission_seq != 0 &&
			 e->ev[i - 1].admission_seq != 0 &&
			 e->ev[i - 1].admission_seq > admission_seq)))
	{
		e->ev[i] = e->ev[i - 1];
		i--;
	}
	e->ev[i].lsn = lsn;
	e->ev[i].admission_seq = admission_seq;
	e->ev[i].order_id = 0;
	e->ev[i].nblocks = nblocks;
	e->ev[i].kind = kind;
	e->ev[i].marker_kind = 0;
	e->nev++;
	fork_def_index_insert(e, i, kind == FEV_SET || kind == FEV_DEAD);
	if (fork_event_cache_defer)
		return;
	fork_event_cache_from(e, i);

	/*
	 * Maintain the newest-size scalar.  A tail insert governs directly.  A
	 * non-tail insert (an out-of-order page flush, or segment replay behind
	 * the pre-loaded fork-meta log) only matters if no definitive event lies
	 * above it: a GROW then raises the newest size; a non-tail SET/DEAD is
	 * not produced by any live path (mutations stamp at/after the newest
	 * event), so just recompute -- rare, correctness first.
	 */
	if (i == e->nev - 1)
	{
		if (kind == FEV_GROW)
		{
			if (nblocks > e->nblocks)
				e->nblocks = nblocks;
		}
		else
			e->nblocks = (kind == FEV_SET) ? nblocks : 0;
	}
	else if (kind == FEV_GROW)
	{
		for (uint32_t j = e->nev - 1; j > i; j--)
			if (e->ev[j].kind == FEV_SET || e->ev[j].kind == FEV_DEAD)
				return;			/* covered by a newer definitive event */
		if (nblocks > e->nblocks)
			e->nblocks = nblocks;
	}
	else
		e->nblocks = fork_size_asof_hop(e, UINT64_MAX, 0);
}

/*
 * Preserve a segment growth's position among equal-LSN fork-meta events without
 * making the marker itself a size event.  Recovery activates the placeholder
 * only after validating the matching segment header and complete page body.
 */
static void
fork_event_add_seg_marker(ForkEnt *e, uint64_t lsn, uint32_t nblocks,
						  uint8_t kind, uint64_t order_id,
						  uint64_t admission_seq)
{
	uint32_t	i;

	if (e->nev == e->evcap)
	{
		e->evcap = e->evcap ? e->evcap * 2 : 4;
		e->ev = realloc(e->ev, e->evcap * sizeof(ForkEvent));
	}
	i = e->nev;
	while (i > 0 &&
		   (e->ev[i - 1].lsn > lsn ||
			(e->ev[i - 1].lsn == lsn && admission_seq != 0 &&
			 e->ev[i - 1].admission_seq != 0 &&
			 e->ev[i - 1].admission_seq > admission_seq)))
	{
		e->ev[i] = e->ev[i - 1];
		i--;
	}
	e->ev[i].lsn = lsn;
	e->ev[i].admission_seq = admission_seq;
	e->ev[i].order_id = order_id;
	e->ev[i].nblocks = nblocks;
	e->ev[i].kind = kind;
	e->ev[i].marker_kind = kind;
	e->nev++;
	fork_def_index_insert(e, i, 0);
	fork_event_cache_from(e, i);
}

static int
fork_event_activate_seg(ForkEnt *e, uint64_t lsn, uint32_t nblocks,
						uint64_t order_id, uint64_t admission_seq)
{
	for (uint32_t i = 0; i < e->nev; i++)
	{
		ForkEvent  *v = &e->ev[i];

		if ((v->kind == FEV_SEG_GROW || v->kind == FEV_SEG_COMMIT ||
			 v->kind == FEV_SEG_GROW_BOUND ||
			 v->kind == FEV_SEG_COMMIT_BOUND) &&
			v->lsn == lsn &&
			v->nblocks == nblocks && v->order_id == order_id &&
			v->admission_seq == admission_seq)
		{
			if (v->kind == FEV_SEG_GROW || v->kind == FEV_SEG_GROW_BOUND)
			{
				v->kind = FEV_GROW;
				fork_event_cache_from(e, i);
				e->nblocks = fork_size_asof_hop(e, UINT64_MAX, 0);
			}
			else
			{
			}
			return 1;
		}
	}
	return 0;
}

static int
fork_grow_with_seq(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
				   uint64_t lsn, uint64_t admission_seq)
{
	ForkEnt    *e = fork_get_or_create(timeline, key);

	/*
	 * Zeroextend has no page record from which recovery can reconstruct its
	 * size.  Clamp first, then persist exactly the event applied in memory.
	 */
	if (lsn < e->last_def_lsn || lsn == 0)
		lsn = e->last_def_lsn;
	if (fork_size_asof_hop(e, lsn, admission_seq) < to_nblocks &&
		fork_meta_persist(timeline, key, lsn, admission_seq, to_nblocks,
						  FEV_GROW) != 0)
		return -1;			/* not durable: do not apply in memory */
	fork_event_add(e, lsn, admission_seq, to_nblocks, FEV_GROW);
	return 0;
}

int
fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
		  uint64_t lsn)
{
	uint64_t admission_seq = admission_seq_alloc();

	if (admission_seq == 0)
		return -1;
	return fork_grow_with_seq(timeline, key, to_nblocks, lsn, admission_seq);
}

/* Apply growth whose durability is already represented by metadata/segment. */
static void
fork_grow_apply(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
				uint64_t lsn, uint64_t admission_seq)
{
	fork_event_add(fork_get_or_create(timeline, key), lsn, admission_seq, to_nblocks,
				   FEV_GROW);
}

static int
fork_event_precedes_known_state(const ForkEnt *e, uint64_t lsn,
							uint64_t admission_seq)
{
	const ForkEvent *tail;

	if (e == NULL || e->nev == 0)
		return 0;
	tail = &e->ev[e->nev - 1];
	return tail->lsn > lsn ||
		(tail->lsn == lsn && tail->admission_seq > admission_seq) ||
		e->last_page_lsn > lsn ||
		(e->last_page_lsn == lsn && e->last_page_seq > admission_seq);
}

/* A definitive event can arrive after pages whose WAL positions are newer.
 * Those page records did not need GROW events when admitted, but the delayed
 * truncate/drop can make them growth retroactively.  Reconstruct the transient
 * events now; segment recovery derives the same events durably after restart. */
typedef struct DeferredGrow
{
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint32_t	nblocks;
} DeferredGrow;

static int
fork_deferred_grow_cmp(const void *left, const void *right)
{
	const DeferredGrow *a = left;
	const DeferredGrow *b = right;

	if (a->lsn != b->lsn)
		return a->lsn < b->lsn ? -1 : 1;
	if (a->admission_seq != b->admission_seq)
		return a->admission_seq < b->admission_seq ? -1 : 1;
	return 0;
}

static void
fork_restore_later_page_growth(uint32_t timeline, const PsKey *key,
								   uint64_t lsn, uint64_t admission_seq)
{
	ForkEnt    *e = fork_get_or_create(timeline, key);
	DeferredGrow *grows = NULL;
	uint32_t	ngrows = 0;
	uint32_t	cap = 0;

	/* The per-fork page chain is block-descending.  Collect its later page
	 * versions first so a delayed lifecycle event does not rebuild the suffix
	 * once per block while holding the shard lock. */

	for (PageEnt *page = e->pages; page; page = page->fork_next)
	{
		for (int i = 0; i < page->nver; i++)
		{
			PageVer    *version = &page->vers[i];

			if (version->lsn != 0 &&
				(version->lsn > lsn ||
				 (version->lsn == lsn &&
				  version->admission_seq > admission_seq)))
			{
				if (ngrows == cap)
				{
					cap = cap ? cap * 2 : 16;
					grows = realloc(grows, (size_t) cap * sizeof(*grows));
					if (grows == NULL)
						return;
				}
				grows[ngrows++] = (DeferredGrow)
					{version->lsn, version->admission_seq, page->block + 1};
			}
		}
	}
	/* Sort once and coalesce adjacent equal page-version tuples.  In deferred
	 * mode fork_event_add deliberately bypasses its cache-based deduplication:
	 * the cache is rebuilt only after the entire batch, so it cannot suppress a
	 * necessary later growth using stale values. */
	qsort(grows, ngrows, sizeof(*grows), fork_deferred_grow_cmp);
	{
		uint32_t out = 0;

		for (uint32_t i = 0; i < ngrows; i++)
		{
			if (out != 0 && grows[out - 1].lsn == grows[i].lsn &&
				grows[out - 1].admission_seq == grows[i].admission_seq)
			{
				if (grows[i].nblocks > grows[out - 1].nblocks)
					grows[out - 1].nblocks = grows[i].nblocks;
			}
			else
				grows[out++] = grows[i];
		}
		ngrows = out;
	}
	fork_event_cache_defer++;
	{
		uint32_t existing = 0;

		for (uint32_t i = 0; i < ngrows; i++)
		{
			int present = 0;

			while (existing < e->nev &&
				(e->ev[existing].lsn < grows[i].lsn ||
				 (e->ev[existing].lsn == grows[i].lsn &&
				  e->ev[existing].admission_seq < grows[i].admission_seq)))
				existing++;
			for (uint32_t j = existing; j < e->nev &&
				 e->ev[j].lsn == grows[i].lsn &&
				 e->ev[j].admission_seq == grows[i].admission_seq; j++)
				if (e->ev[j].kind == FEV_GROW &&
					e->ev[j].nblocks >= grows[i].nblocks)
				{
					present = 1;
					break;
				}
			if (!present)
				fork_event_add(e, grows[i].lsn, grows[i].admission_seq,
							   grows[i].nblocks, FEV_GROW);
		}
	}
	fork_event_cache_defer--;
	if (ngrows != 0)
	{
		fork_event_cache_from(e, 0);
		e->nblocks = fork_size_asof_hop(e, UINT64_MAX, 0);
	}
	free(grows);
}

/*
 * Segment-log replay variant: insert the record's growth verbatim.  Raw
 * nonzero LSNs below a definitive event are REAL pre-truncate history here
 * (the meta log is fully preloaded, so the floor visible now can postdate
 * the record's live order); they stay in place, covered by the later SET.
 * LSN-0 records are skipped by the caller in the normal case -- their live
 * clamped position was persisted -- except in legacy mode (see recover).
 */
static void
fork_grow_replay(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
				 uint64_t lsn, uint64_t admission_seq)
{
	fork_event_add(fork_get_or_create(timeline, key), lsn, admission_seq, to_nblocks,
				   FEV_GROW);
}

/* --- timeline metadata + read-through --- */

static void
timeline_define_incarnation(uint32_t id, int parent, uint64_t branch_lsn,
							uint64_t incarnation, uint64_t parent_incarnation)
{
	if (id >= MAX_TIMELINES || incarnation == 0 || parent_incarnation == 0)
		return;
	timelines[id].parent = parent;
	timelines[id].branch_lsn = branch_lsn;
	timelines[id].parent_incarnation = parent_incarnation;
	__atomic_store_n(&timelines[id].incarnation, incarnation, __ATOMIC_RELEASE);
	__atomic_store_n(&timelines[id].state, PS_TIMELINE_LIVE, __ATOMIC_RELEASE);
	__atomic_store_n(&timeline_used[id], 0, __ATOMIC_RELEASE);
	/* Publish the complete definition last.  Readers outside map_lock use the
	 * acquire load below and can never observe a half-defined branch. */
	__atomic_store_n(&timelines[id].defined, 1, __ATOMIC_RELEASE);
}

static void
timeline_define(uint32_t id, int parent, uint64_t branch_lsn)
{
	timeline_define_incarnation(id, parent, branch_lsn, 1, 1);
}

static int
timeline_has_parent(uint32_t timeline)
{
	return timeline < MAX_TIMELINES && timelines[timeline].defined &&
		timelines[timeline].parent >= 0;
}

/* The durable retained-base metadata is the only WAL history fence.  The
 * physical directory start is deliberately not a process-local authority:
 * reclaim advances it, so remembering it in PsWalStore would reopen the old
 * prefix after a restart. */
static int
wal_reclaim_frontier_one_allows(uint32_t timeline, uint64_t lsn)
{
	uint64_t base;

	if (timeline >= MAX_TIMELINES || !wal_segment_store_opened[timeline])
		return 1;
	if (ps_wal_store_retained_base(&wal_segment_stores[timeline], &base) != 0)
		return 0;
	return lsn >= base;
}

/* A defined child may read the part of its visible history at or before its
 * fork from the parent even when the child's own store starts at the aligned
 * fork and therefore has a higher retained base.  This exception is local to
 * a child level: roots, undefined timelines, and child-local post-fork WAL
 * still have to pass their own retained-base fence. */
static int
wal_reclaim_frontier_level_allows(uint32_t timeline, uint64_t lsn)
{
	if (wal_reclaim_frontier_one_allows(timeline, lsn))
		return 1;
	return timeline < MAX_TIMELINES && timelines[timeline].defined &&
		timelines[timeline].parent >= 0 &&
		lsn <= timelines[timeline].branch_lsn;
}

/* Check every local history level, applying the same branch cap used by
 * read-through.  This is intentionally a contiguous frontier check: R3b-3
 * has no sparse exception protocol for a fixed reader or branch base. */
static int
wal_reclaim_frontier_ancestry_allows(uint32_t timeline, uint64_t lsn)
{
	uint32_t current = timeline;
	uint64_t cap = lsn;

	for (uint32_t hops = 0; hops <= MAX_TIMELINES; hops++)
	{
		if (current >= MAX_TIMELINES ||
			!wal_reclaim_frontier_level_allows(current, cap))
			return 0;
		/* A shipped WAL timeline can legitimately precede its ancestry
		 * metadata.  Its local retained-base fence is still authoritative,
		 * but there is no ancestry to walk until metadata is published. */
		if (!timelines[current].defined)
			return 1;
		if (timelines[current].parent < 0)
			return 1;
		/* A defined timeline with an invalid or not-yet-defined parent is a
		 * malformed ancestry chain, not a legacy pre-metadata read. */
		if (timelines[current].parent >= MAX_TIMELINES ||
			!timelines[timelines[current].parent].defined)
			return 0;
		if (timelines[current].branch_lsn < cap)
			cap = timelines[current].branch_lsn;
		current = (uint32_t) timelines[current].parent;
	}
	return 0;
}

int
ps_timeline_defined(uint32_t timeline)
{
	return !timeline_meta_poisoned_load() && timeline < MAX_TIMELINES &&
		__atomic_load_n(&timelines[timeline].defined, __ATOMIC_ACQUIRE);
}

int
ps_timeline_state(uint32_t timeline, PsTimelineState *state,
					 uint64_t *incarnation)
{
	if (timeline_meta_poisoned_load() || timeline >= MAX_TIMELINES ||
		!__atomic_load_n(&timelines[timeline].defined, __ATOMIC_ACQUIRE))
		return 0;
	if (state != NULL)
		*state = (PsTimelineState) __atomic_load_n(&timelines[timeline].state,
																__ATOMIC_ACQUIRE);
	if (incarnation != NULL)
		*incarnation = __atomic_load_n(&timelines[timeline].incarnation,
																__ATOMIC_ACQUIRE);
	return 1;
}

int
ps_timeline_live(uint32_t timeline)
{
	PsTimelineState state;

	return ps_timeline_state(timeline, &state, NULL) &&
		state == PS_TIMELINE_LIVE;
}

int
ps_timeline_request_allowed(uint32_t timeline, uint64_t expected_incarnation)
{
	uint64_t current;

	if (timeline_meta_poisoned_load() || timeline >= MAX_TIMELINES)
		return 0;
	if (!__atomic_load_n(&timelines[timeline].defined, __ATOMIC_ACQUIRE))
		return expected_incarnation == 0; /* legacy pre-metadata import */
	current = __atomic_load_n(&timelines[timeline].incarnation, __ATOMIC_ACQUIRE);
	return current != 0 &&
		(expected_incarnation == current ||
		 (expected_incarnation == 0 && current == 1));
}

static int
timeline_delete_active(void)
{
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		PsTimelineState state;

		if (ps_timeline_state(tl, &state, NULL) &&
			state == PS_TIMELINE_DELETING)
			return 1;
	}
	return 0;
}

static int
timeline_delete_recovery_skip(void)
{
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		PsTimelineState state;

		if (ps_timeline_state(tl, &state, NULL) &&
			(state == PS_TIMELINE_DELETING || state == PS_TIMELINE_DELETED))
			return 1;
	}
	return 0;
}

/* Legacy clients may write page data before creating timeline metadata.  Keep
 * that compatibility path recoverable; only an explicit durable lifecycle
 * state other than LIVE suppresses page/layer reconstruction. */
static int
timeline_recovery_allowed(uint32_t timeline)
{
	PsTimelineState state;

	return !ps_timeline_state(timeline, &state, NULL) ||
		state == PS_TIMELINE_LIVE;
}

/*
 * Ancestry iterator.  A read on a branch resolves against the branch and then
 * each ancestor, with read_lsn frozen at each branch point so the branch sees a
 * snapshot of the parent as of the fork.  Several walks (read_through,
 * read_resolve, walidx_get, the fork-size/exists walks) repeated this loop; this
 * captures it once.  Usage:
 *
 *		TlWalk w = tl_walk_first(timeline, read_lsn);
 *		do {
 *			... use w.tl and w.lsn ...
 *		} while (tl_walk_next(&w));
 *
 * Size/existence walks that don't care about LSN pass any read_lsn and ignore
 * w.lsn; the capping is harmless to them.
 */
typedef struct TlWalk
{
	uint32_t	tl;				/* current ancestry level */
	uint64_t	lsn;			/* read_lsn capped to this level's fork point */
} TlWalk;

static inline TlWalk
tl_walk_first(uint32_t timeline, uint64_t read_lsn)
{
	TlWalk		w = {timeline, read_lsn};

	return w;
}

/* Advance to the parent, capping lsn at the branch point; 0 at the root. */
static inline int
tl_walk_next(TlWalk *w)
{
	if (!timeline_has_parent(w->tl))
		return 0;
	if (timelines[w->tl].branch_lsn < w->lsn)
		w->lsn = timelines[w->tl].branch_lsn;
	w->tl = (uint32_t) timelines[w->tl].parent;
	return 1;
}

/* Caller holds map_lock for reading.  A pin on a child must be admissible at
 * every ancestor position reached by its capped read, not merely at the
 * child's own frontier. */
static int
page_frontier_ancestry_allows(uint32_t reader_timeline, uint64_t read_lsn,
						  uint64_t read_seq)
{
	TlWalk		w = tl_walk_first(reader_timeline, read_lsn);

	for (;;)
	{
		uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;

		if (!page_frontier_allows(w.tl, reader_timeline, w.lsn, seq_cap))
			return 0;
		if (!tl_walk_next(&w))
			return 1;
	}
}

/* Caller holds map_lock. */
static int
walidx_frontier_ancestry_allows(uint32_t reader_timeline, uint64_t read_lsn)
{
	TlWalk		w = tl_walk_first(reader_timeline, read_lsn);

	for (;;)
	{
		if (!walidx_frontier_allows(w.tl, w.lsn))
			return 0;
		if (!tl_walk_next(&w))
			return 1;
	}
}

/* Caller holds map_lock.  Descendant pins participate in every ancestor's
 * prune fence, so a prepared cutover freezes mutations across that ancestry. */
static int
walidx_frontier_ancestry_pending(uint32_t reader_timeline)
{
	TlWalk		w = tl_walk_first(reader_timeline, UINT64_MAX);

	for (;;)
	{
		if (walidx_frontier_publication_pending(w.tl))
			return 1;
		if (!tl_walk_next(&w))
			return 0;
	}
}

/* A capped relation read cannot prove completeness for WAL-less pages.  Check
 * the whole ancestry before EXISTS/NBLOCKS can turn a hidden LSN-0 version
 * into an apparently valid empty relation. */
static int
fork_has_wal_less_page(uint32_t timeline, const PsKey *key)
{
	TlWalk		w = tl_walk_first(timeline, UINT64_MAX);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e && e->has_wal_less)
			return 1;
	} while (tl_walk_next(&w));
	return 0;
}

/*
 * Validate a branch-creation request before it is recorded.  read_through() and
 * the fork-size walks follow the parent chain assuming it is finite and well
 * formed, so a bad CREATE_BRANCH must be rejected rather than persisted.  Refuse:
 *	- a new id that is out of range, or an already-defined id with mismatched
 *	  ancestry metadata (an exact match is an idempotent retry, but only while
 *	  the timeline is still unused -- see timeline_used[]);
 *	- a parent that is out of range or not yet defined (the requested parent must
 *	  actually exist, else the branch silently inherits from nothing);
 *	- a parent whose ancestry already reaches the new id, which would turn the
 *	  parent walk into an infinite loop (e.g. new == parent, or A->B->A).
 * Returns 1 if (new_tl, parent, branch_lsn) can be used for CREATE_BRANCH.
 */
static int
branch_parent_chain_ok(uint32_t new_tl, int parent)
{
	/* Bound the walk as well as checking the requested id.  This makes replay
	 * fail closed if a corrupt metadata record has already introduced a cycle. */
	for (uint32_t steps = 0; steps < MAX_TIMELINES && parent >= 0; steps++)
	{
		if (parent >= MAX_TIMELINES || (uint32_t) parent == new_tl ||
			!timelines[parent].defined)
			return 0;
		parent = timelines[parent].parent;
	}
	return parent < 0;
}

static int
branch_request_ok(uint32_t new_tl, int parent, uint64_t branch_lsn)
{
	/*
	 * Exact matches to an existing definition are idempotent retries -- but
	 * only while the timeline has no branch-local state yet.  Once it has
	 * pages, forks or shipped WAL, the duplicate is timeline-id reuse, not a
	 * retry, and accepting it would hand the caller the old branch's data.
	 */
	if (new_tl < MAX_TIMELINES && timelines[new_tl].defined)
		return timelines[new_tl].parent == parent &&
			timelines[new_tl].branch_lsn == branch_lsn &&
			__atomic_load_n(&timelines[new_tl].state, __ATOMIC_ACQUIRE) ==
			PS_TIMELINE_LIVE &&
			!timeline_is_used(new_tl) && wal_end_read(new_tl) == 0;

	if (new_tl == 0 || new_tl >= MAX_TIMELINES ||
		timeline_is_used(new_tl) || wal_end_read(new_tl) != 0 || parent < 0 ||
		parent >= MAX_TIMELINES || !timelines[parent].defined ||
		__atomic_load_n(&timelines[parent].state, __ATOMIC_ACQUIRE) !=
		PS_TIMELINE_LIVE)
		return 0;

	return branch_parent_chain_ok(new_tl, parent);
}

static int
branch_parent_token_ok(int parent, uint64_t expected_incarnation)
{
	if (parent < 0 || parent >= MAX_TIMELINES || !timelines[parent].defined ||
		__atomic_load_n(&timelines[parent].state, __ATOMIC_ACQUIRE) !=
		PS_TIMELINE_LIVE)
		return 0;
	return ps_timeline_request_allowed((uint32_t) parent,
									 expected_incarnation);
}

/* Validate both sides of CREATE_BRANCH.  The target token is deliberately
 * separate from req_seq: req_seq is already the parent token for this op,
 * while all other requests use it for admission/read fencing. */
static int
branch_create_request_ok(uint32_t new_tl, int parent, uint64_t branch_lsn,
						 uint64_t target_incarnation,
						 uint64_t parent_incarnation,
						 uint64_t *new_incarnation)
{
	uint64_t current;
	uint64_t parent_current;

	if (!branch_parent_token_ok(parent, parent_incarnation))
		return 0;
	parent_current = __atomic_load_n(&timelines[parent].incarnation,
								  __ATOMIC_ACQUIRE);
	if (parent_current == 0)
		return 0;
	if (new_tl >= MAX_TIMELINES || new_tl == 0)
		return 0;
	if (!branch_parent_chain_ok(new_tl, parent))
		return 0;
	if (!timelines[new_tl].defined)
	{
		if (target_incarnation != 0 && target_incarnation != 1)
			return 0;
		if (!branch_request_ok(new_tl, parent, branch_lsn) ||
			!branch_frontiers_allow(parent, branch_lsn))
			return 0;
		if (new_incarnation)
			*new_incarnation = 1;
		return 1;
	}
	current = __atomic_load_n(&timelines[new_tl].incarnation, __ATOMIC_ACQUIRE);
	if (current == 0)
		return 0;
	if (__atomic_load_n(&timelines[new_tl].state, __ATOMIC_ACQUIRE) ==
		PS_TIMELINE_LIVE)
	{
		/* Exact metadata retries are the explicitly idempotent case. */
		if (timelines[new_tl].parent != parent ||
			timelines[new_tl].branch_lsn != branch_lsn ||
			timelines[new_tl].parent_incarnation != parent_current ||
			timeline_is_used(new_tl) || wal_end_read(new_tl) != 0 ||
			((current > 1 && target_incarnation != current) ||
			 (current == 1 && target_incarnation != 0 &&
			  target_incarnation != current)))
			return 0;
		if (new_incarnation)
			*new_incarnation = current;
		return 1;
	}
	if (__atomic_load_n(&timelines[new_tl].state, __ATOMIC_ACQUIRE) !=
		PS_TIMELINE_DELETED || current == UINT64_MAX ||
		target_incarnation == 0 || target_incarnation != current + 1 ||
		!branch_frontiers_allow(parent, branch_lsn))
		return 0;
	if (new_incarnation)
		*new_incarnation = target_incarnation;
	return 1;
}

/* Project a requested branch horizon through every ancestor cap and reject
 * any ancestor whose durable reclamation frontier has already passed it. */
static int
branch_frontiers_allow(int parent, uint64_t branch_lsn)
{
	uint64_t cap = branch_lsn;

	for (int t = parent; t >= 0 && t < MAX_TIMELINES; t = timelines[t].parent)
	{
		if (!timelines[t].defined ||
			!wal_reclaim_frontier_ancestry_allows((uint32_t) t, cap) ||
			walidx_frontier_publication_pending((uint32_t) t) ||
			cap < page_frontier_current((uint32_t) t).lsn ||
			(cap < walidx_frontier_current((uint32_t) t) &&
			 !walidx_frontier_exception_active((uint32_t) t, cap)))
			return 0;
		if (timelines[t].parent >= 0 && cap > timelines[t].branch_lsn)
			cap = timelines[t].branch_lsn;
	}
	return 1;
}

static int
branch_exists_with_metadata(uint32_t tl, int parent, uint64_t branch_lsn)
{
	return tl < MAX_TIMELINES &&
		timelines[tl].defined &&
		timelines[tl].parent == parent &&
		timelines[tl].branch_lsn == branch_lsn;
}

/*
 * Resolve a read by walking the timeline ancestry: return the newest version of
 * (key, block) visible at read_lsn on 'timeline'; if the timeline never wrote
 * the page (or only after read_lsn), descend to the parent, capping read_lsn at
 * the branch LSN so the branch sees a frozen snapshot of the parent.  Returns
 * the chosen PageVer, or NULL if no ancestor has the page.
 */
PageVer *
read_through(uint32_t timeline, const PsKey *key, uint32_t block,
			 uint64_t read_lsn, uint64_t read_seq)
{
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *fe = fork_find(w.tl, key);
		uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;
		uint32_t	nb = 0;
		int			fork_state = fe ? fork_asof_hop(fe, w.lsn, seq_cap, &nb) :
			FORK_HOP_NONE;
		PageEnt    *e = page_find(w.tl, key, block);
		PageVer    *v = e ? page_visible(e, w.lsn, seq_cap) : NULL;

		if (v)
		{
			if (!fork_page_invalidated(fe, block, v, w.lsn, seq_cap))
				return v;
			return NULL;
		}
		if (fork_state == FORK_HOP_DEAD ||
			(fork_state == FORK_HOP_DEF && block >= nb))
			return NULL;
		if (fork_state == FORK_HOP_DEF &&
			fork_inheritance_fenced(fe, block, w.lsn, seq_cap))
			return NULL;
	} while (tl_walk_next(&w));
	return NULL;
}

/*
 * Fork size visible on 'timeline' as of read_lsn (UINT64_MAX = newest): walk
 * the ancestry, capping the horizon at each branch point exactly like page
 * reads do, and resolve each hop against its size history.  A definitive hop
 * (truncate/create/unlink) ends the walk -- a branch that truncated must not
 * re-inherit the parent's larger size; bare growth combines by max, because a
 * branch that wrote only some blocks inherits the rest by read-through.  The
 * per-hop horizon capping also fixes the old unversioned behavior where a
 * parent growing a fork after the branch point leaked the larger size into
 * the branch.
 */
static uint32_t
fork_nblocks_through(uint32_t timeline, const PsKey *key, uint64_t read_lsn,
					 uint64_t read_seq)
{
	uint32_t	maxnb = 0;
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e)
		{
			uint32_t	nb;
			uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;
			int			r = fork_asof_hop(e, w.lsn, seq_cap, &nb);

			if (r == FORK_HOP_DEAD)
				return maxnb;
			if (r == FORK_HOP_DEF)
				return nb > maxnb ? nb : maxnb;
			if (r == FORK_HOP_GROW && nb > maxnb)
				maxnb = nb;
		}
	} while (tl_walk_next(&w));
	return maxnb;
}

/* Redo must reserve every block that can be reached by WAL already accepted
 * into this store.  A later CREATE/TRUNCATE is a logical visibility boundary,
 * not permission to reject an earlier FPI as beyond EOF. */
static uint32_t
fork_nblocks_recovery(uint32_t timeline, const PsKey *key, uint64_t read_lsn)
{
	uint32_t maxnb = 0;
	TlWalk w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt *e = fork_find(w.tl, key);

		if (e != NULL)
			for (uint32_t i = 0; i < e->nev; i++)
				if (e->ev[i].lsn <= w.lsn &&
					(e->ev[i].kind == FEV_GROW || e->ev[i].kind == FEV_SET) &&
					e->ev[i].nblocks > maxnb)
					maxnb = e->ev[i].nblocks;
	} while (tl_walk_next(&w));
	return maxnb;
}

/* Does the fork exist on 'timeline' or any ancestor, as of read_lsn? */
static int
fork_exists_through(uint32_t timeline, const PsKey *key, uint64_t read_lsn,
					uint64_t read_seq)
{
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e)
		{
			uint32_t	nb;
			uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;
			int			r = fork_asof_hop(e, w.lsn, seq_cap, &nb);

			if (r == FORK_HOP_DEAD)
				return 0;
			if (r != FORK_HOP_NONE)
				return 1;
		}
	} while (tl_walk_next(&w));
	return 0;
}

/* Caller holds the key's shard lock and map_lock for reading.  Find the newest
 * fork/page LSN reachable through the child's ancestry, respecting every
 * branch cap.  Page versions require a per-entry lookup when a local fork's
 * cached newest page lies above an ancestor cap. */
static uint64_t
fork_newest_visible_lsn_through(uint32_t timeline, const PsKey *key)
{
	uint64_t	newest = 0;
	TlWalk		w = tl_walk_first(timeline, UINT64_MAX);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e == NULL)
			continue;
		if (e->nev != 0)
		{
			uint32_t lo = 0;
			uint32_t hi = e->nev;

			while (lo < hi)
			{
				uint32_t mid = lo + (hi - lo) / 2;

				if (e->ev[mid].lsn <= w.lsn)
					lo = mid + 1;
				else
					hi = mid;
			}
			if (lo != 0 && e->ev[lo - 1].lsn > newest)
				newest = e->ev[lo - 1].lsn;
		}
		if (e->last_page_lsn <= w.lsn)
		{
			if (e->last_page_lsn > newest)
				newest = e->last_page_lsn;
		}
		else
		{
			for (PageEnt *page = e->pages; page; page = page->fork_next)
			{
				PageVer    *version = page_visible(page, w.lsn, 0);

				if (version != NULL && version->lsn > newest)
					newest = version->lsn;
			}
		}
	} while (tl_walk_next(&w));
	return newest;
}

/*
 * Timeline metadata is persisted as an append-only log of fixed records in
 * "<store>/timelines", so branches survive a daemon restart.  (The page data
 * itself is already durable in the segments.)
 */
typedef struct TimelineRec
{
	uint32_t	id;
	int32_t		parent;
	uint64_t	branch_lsn;
} TimelineRec;

#define TIMELINE_META_V2_MAGIC 0x324d4c54U /* "TLM2" */
typedef struct TimelineRecV2
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t id;
	int32_t parent;
	uint64_t branch_lsn;
	uint32_t crc;
	uint32_t reserved;
} TimelineRecV2;

/* The V2 create record remains readable forever.  Lifecycle records use the
 * same log and magic, but are self-sized so V2 creates and events can be mixed
 * after a legacy-only log has been migrated. */
#define TIMELINE_META_EVENT_CREATE 1U
#define TIMELINE_META_EVENT_STATE  2U
typedef struct TimelineRecEvent
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t kind;
	uint32_t id;
	int32_t	 parent;
	uint32_t state;
	uint64_t branch_lsn;
	uint64_t incarnation;
	uint64_t parent_incarnation;
	uint32_t crc;
	uint32_t reserved;
} TimelineRecEvent;

/* Event records written before parent incarnation was part of the durable
 * timeline identity.  They remain readable as generation-one ancestry only;
 * a reused parent must be recreated with the new record shape. */
typedef struct TimelineRecEventV1
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t kind;
	uint32_t id;
	int32_t	 parent;
	uint32_t state;
	uint64_t branch_lsn;
	uint64_t incarnation;
	uint32_t crc;
	uint32_t reserved;
} TimelineRecEventV1;

static uint32_t
timeline_rec_crc(TimelineRecV2 *rec)
{
	uint32_t save = rec->crc;
	uint32_t crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static uint32_t
timeline_event_crc(TimelineRecEvent *rec)
{
	uint32_t save = rec->crc;
	uint32_t crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static uint32_t
timeline_event_v1_crc(TimelineRecEventV1 *rec)
{
	uint32_t save = rec->crc;
	uint32_t crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static int
timeline_meta_append(const void *data, uint32_t len)
{
	int rc = ps_storage->meta_append(data, len);

	if (rc != 0)
		timeline_meta_poison();
	return rc;
}

static int
timeline_persist_create(uint32_t id, int parent, uint64_t branch_lsn,
						uint64_t incarnation, uint64_t parent_incarnation)
{
	TimelineRecEvent rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = TIMELINE_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.kind = TIMELINE_META_EVENT_CREATE;
	rec.id = id;
	rec.parent = (int32_t) parent;
	rec.state = PS_TIMELINE_LIVE;
	rec.branch_lsn = branch_lsn;
	rec.incarnation = incarnation;
	rec.parent_incarnation = parent_incarnation;
	rec.crc = timeline_event_crc(&rec);

	return timeline_meta_append(&rec, sizeof(rec));
}

static int
timeline_persist_state(uint32_t id, PsTimelineState state,
						   uint64_t incarnation)
{
	TimelineRecEvent rec;

	if (id >= MAX_TIMELINES || state < PS_TIMELINE_LIVE ||
		state > PS_TIMELINE_DELETED || incarnation == 0)
		return -1;
	memset(&rec, 0, sizeof(rec));
	rec.magic = TIMELINE_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.kind = TIMELINE_META_EVENT_STATE;
	rec.id = id;
	rec.parent = timelines[id].parent;
	rec.state = state;
	rec.branch_lsn = timelines[id].branch_lsn;
	rec.incarnation = incarnation;
	rec.parent_incarnation = timelines[id].parent_incarnation;
	rec.crc = timeline_event_crc(&rec);

	return timeline_meta_append(&rec, sizeof(rec));
}

/*
 * Fork-size events the segment log cannot reproduce -- create, truncate,
 * unlink, zero-extend -- are persisted here (the segment records themselves
 * re-derive every page-append GROW on recovery).  V2 records are self-sized so
 * they can coexist with the legacy fixed records in one append-only log.
 */
typedef struct ForkMetaRecV1
{
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} ForkMetaRecV1;

#define FORK_META_V2_MAGIC 0x324d4b46 /* "FKM2" */
#define FORK_META_SNAPSHOT_PAYLOAD_MAGIC 0x31534d46 /* "FMS1" */
#define FORK_META_SNAPSHOT_PAYLOAD_VERSION 1
#define FORK_META_SNAPSHOT_CHECKPOINT 0
#define FORK_META_SNAPSHOT_TAIL 1

typedef struct ForkMetaRecV2
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
} ForkMetaRecV2;

typedef struct ForkMetaSnapshotPayloadHeader
{
	uint32_t	magic;
	uint16_t	version;
	uint16_t	header_bytes;
	uint32_t	part;
	uint32_t	record_bytes;
	uint64_t	generation;
	uint64_t	cutoff_lsn;
	uint64_t	cutoff_admission_seq;
	uint64_t	freeze_admission_seq;
	uint64_t	checkpoint_records;
	uint64_t	tail_records;
	uint64_t	checkpoint_bytes;
	uint64_t	tail_bytes;
} ForkMetaSnapshotPayloadHeader;

static uint64_t fork_meta_snapshot_generation;
static uint64_t fork_meta_snapshot_cutoff_lsn;
static uint64_t fork_meta_snapshot_cutoff_seq;
static uint64_t fork_meta_snapshot_freeze_seq;
/* The selected source is a compacted baseline, not controller debt.  Only
 * bytes appended after this baseline are charged.  The value is rebuilt after
 * recovery and advanced only after a durable source rewrite. */
static uint64_t fork_meta_reclaim_baseline_bytes;
static int fork_meta_reclaim_baseline_valid;
static uint64_t fork_meta_irreducible_prefix_bytes;
static int fork_meta_event_future(uint64_t lsn, uint64_t admission_seq,
							  uint64_t cutoff_lsn, uint64_t cutoff_seq);
static int fork_meta_migration_marker_valid(const ForkMetaRecV2 *rec);
static int fork_meta_snapshot_marker_matches(const ForkMetaRecV2 *rec);
static int fork_meta_selected_suffix_valid(const ForkMetaRecV2 *rec);
static int fork_meta_source_cutoff_provable(void);

static int
fork_meta_mutation_future(uint64_t lsn, uint64_t admission_seq)
{
	return fork_meta_snapshot_generation == 0 ||
		fork_meta_event_future(lsn, admission_seq,
						   fork_meta_snapshot_cutoff_lsn,
						   fork_meta_snapshot_cutoff_seq);
}

static int
fork_meta_persist(uint32_t timeline, const PsKey *key, uint64_t lsn,
				  uint64_t admission_seq, uint32_t nblocks, uint8_t kind)
{
	ForkMetaRecV2 rec;
	int rc;

	if (fork_meta_poisoned_load())
		return -1;
	if (kind <= FEV_DEAD && !fork_meta_mutation_future(lsn, admission_seq))
		return -1;

	memset(&rec, 0, sizeof(rec));
	rec.magic = FORK_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.timeline = timeline;
	rec.key = *key;
	rec.lsn = lsn;
	rec.admission_seq = admission_seq;
	rec.nblocks = nblocks;
	rec.kind = kind;
	rc = ps_storage->fork_meta_append(&rec, sizeof(rec));
	if (rc == 0)
		fork_meta_bytes_add(sizeof(rec));
	return rc;
}

/* Persist a bound segment marker and its 64-bit identity in one self-sized
 * append.  The loader also accepts the legacy two-record representation. */
static int
fork_meta_persist_segment(uint32_t timeline, const PsKey *key, uint64_t lsn,
							  uint32_t nblocks, uint8_t kind, uint64_t order_id,
							  uint64_t admission_seq)
{
	ForkMetaRecV2 rec;
	int rc;

	if (fork_meta_poisoned_load())
		return -1;

	memset(&rec, 0, sizeof(rec));
	rec.magic = FORK_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.timeline = timeline;
	rec.key = *key;
	rec.lsn = lsn;
	rec.admission_seq = admission_seq;
	rec.order_id = order_id;
	rec.nblocks = nblocks;
	rec.kind = kind == FEV_SEG_GROW ? FEV_SEG_GROW_BOUND :
		FEV_SEG_COMMIT_BOUND;
	rc = ps_storage->fork_meta_append(&rec, sizeof(rec));
	if (rc == 0)
		fork_meta_bytes_add(sizeof(rec));
	else
		/* The ordered segment body may already be complete.  Until restart,
		 * poison all forkmeta mutation and snapshot maintenance so no later
		 * freeze highwater can authorize that uncommitted body without marker. */
		fork_meta_poisoned_store(1);
	return rc;
}

/*
 * Replay the fork-meta log before the segment scan.  Preloading definitive
 * events lets segment growth dedup and clamp detection see the complete size
 * history.  Segment-growth ordering placeholders retain their exact position
 * among equal-LSN metadata events and are activated only by a matching
 * complete segment record.  Invalid records are skipped, mirroring
 * load_timelines().
 */
static int fork_meta_migrating = 0;	/* the log carries the migration-start marker */
static int fork_meta_migrated = 0;	/* the log carries the migration-done marker */
static int fork_meta_legacy = 0;	/* replay lsn-0 records during a known migration */
static int fork_meta_migrate_failed = 0;	/* a migration persist failed this run */
static uint64_t fork_meta_snapshot_bytes;
static PsForkmetaSnapshotPart fork_meta_snapshot_checkpoint_meta;
static PsForkmetaSnapshotPart fork_meta_snapshot_tail_meta;
static int fork_meta_snapshot_gc_pending;
/* A successful deletion-filtered cutover is sufficient for this process.  The
 * selected snapshot/source pair remains authoritative after restart, while
 * this transient fence prevents an idle maintenance loop from publishing the
 * same filtered generation repeatedly before a reopen. */
static unsigned char fork_meta_deletion_cutover_done[MAX_TIMELINES];
/* A failed directory fsync after unlink leaves the next successful empty GC
 * as the operation that closes the durability ambiguity. */
static int fork_meta_snapshot_gc_ambiguous;
static struct timespec fork_meta_snapshot_retry_at;
static char fork_meta_snapshot_dir[4096];

/* The baseline is deliberately conservative across restart.  Before the first
 * selected snapshot only the strictly validated migration-marker prefix is
 * irreducible.  Once a selected snapshot exists, its source epoch marker is
 * the only persisted compacted baseline; every suffix record is conservatively
 * reclaimable source debt until a durable rewrite advances that baseline. */
static int
fork_meta_reclaim_baseline_init(void)
{
	ForkMetaRecV2 marker;
	struct stat st;
	int directory_fd;
	int source_present = 0;
	int rc = 0;

	memset(&st, 0, sizeof(st));
	if (forkmeta_baseline_init_test_hook != NULL)
		forkmeta_baseline_init_test_hook(forkmeta_baseline_init_test_hook_arg);
	directory_fd = open(wal_segment_root,
						O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (directory_fd < 0)
		return -1;
	if (fstatat(directory_fd, "forkmeta", &st, AT_SYMLINK_NOFOLLOW) != 0)
	{
		if (errno != ENOENT)
			rc = -1;
	}
	else if (!S_ISREG(st.st_mode) || st.st_size < 0)
		rc = -1;
	else
		source_present = 1;
	if (close(directory_fd) != 0)
		rc = -1;
	if (rc != 0)
		return -1;
	if (fork_meta_snapshot_generation != 0)
	{
		if (!source_present || st.st_size < (off_t) sizeof(marker) ||
			ps_storage->fork_meta_read == NULL ||
			ps_storage->fork_meta_read(0, &marker, sizeof(marker)) !=
			(int) sizeof(marker) || !fork_meta_snapshot_marker_matches(&marker))
			return -1;
		fork_meta_reclaim_baseline_bytes = sizeof(marker);
	}
	else
	{
		if (!source_present && fork_meta_irreducible_prefix_bytes != 0)
			return -1;
		fork_meta_reclaim_baseline_bytes = fork_meta_irreducible_prefix_bytes;
	}
	fork_meta_reclaim_baseline_valid = 1;
	return 0;
}

static uint64_t
forkmeta_reclaim_lag_bytes(void)
{
	PsForkmetaSnapshotExpected expected;
	uint64_t debt;

	if (ps_storage == NULL || ps_storage->name == NULL ||
		strcmp(ps_storage->name, "posix") != 0 ||
		!fork_meta_reclaim_baseline_valid)
		return UINT64_MAX;
	memset(&expected, 0, sizeof(expected));
	expected.generation = fork_meta_snapshot_generation;
	expected.cutoff_lsn = fork_meta_snapshot_cutoff_lsn;
	expected.cutoff_admission_seq = fork_meta_snapshot_cutoff_seq;
	expected.checkpoint = fork_meta_snapshot_checkpoint_meta;
	expected.tail = fork_meta_snapshot_tail_meta;
	if (ps_forkmeta_snapshot_reclaim_bytes(fork_meta_snapshot_dir,
										wal_segment_root,
										fork_meta_reclaim_baseline_bytes,
										fork_meta_source_cutoff_provable(),
										&expected, &debt) != 0)
		return UINT64_MAX;
	return debt;
}


void
ps_test_forkmeta_snapshot_gc_retry_now(void)
{
	memset(&fork_meta_snapshot_retry_at, 0, sizeof(fork_meta_snapshot_retry_at));
}

typedef struct ForkMetaByteVec
{
	unsigned char *data;
	size_t len;
	size_t cap;
} ForkMetaByteVec;

static int fork_meta_snapshot_load(const char *directory);
static int fork_meta_snapshot_reconcile_source(void);
static int fork_meta_snapshot_maintenance(void);
static int fork_meta_snapshot_due(void);
static int fork_meta_snapshot_due_locked(void);

static int
fork_meta_vec_append(ForkMetaByteVec *vec, const void *data, size_t len)
{
	size_t needed;

	if (len == 0)
		return 0;
	if (len > SIZE_MAX - vec->len)
		return -1;
	needed = vec->len + len;
	if (needed > vec->cap)
	{
		size_t cap = vec->cap ? vec->cap : 4096;
		unsigned char *grown;

		while (cap < needed)
		{
			if (cap > SIZE_MAX / 2)
				return -1;
			cap *= 2;
		}
		grown = realloc(vec->data, cap);
		if (grown == NULL)
			return -1;
		vec->data = grown;
		vec->cap = cap;
	}
	memcpy(vec->data + vec->len, data, len);
	vec->len = needed;
	return 0;
}

static int
fork_meta_vec_record(ForkMetaByteVec *vec, uint32_t timeline,
					 const PsKey *key, uint64_t lsn, uint64_t admission_seq,
					 uint64_t order_id, uint32_t nblocks, uint8_t kind)
{
	ForkMetaRecV2 rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = FORK_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.timeline = timeline;
	rec.key = *key;
	rec.lsn = lsn;
	rec.admission_seq = admission_seq;
	rec.order_id = order_id;
	rec.nblocks = nblocks;
	rec.kind = kind;
	return fork_meta_vec_append(vec, &rec, sizeof(rec));
}

static int
fork_meta_event_future(uint64_t lsn, uint64_t admission_seq,
					   uint64_t cutoff_lsn, uint64_t cutoff_seq)
{
	return lsn > cutoff_lsn ||
		(lsn == cutoff_lsn && admission_seq != 0 && admission_seq > cutoff_seq);
}

static int
fork_meta_snapshot_manifest_exists(const char *directory)
{
	char path[4096];
	int n;

	n = snprintf(path, sizeof(path), "%s/forkmeta_manifest_v1", directory);
	if (n < 0 || (size_t) n >= sizeof(path))
		return -1;
	if (access(path, F_OK) == 0)
		return 1;
	return errno == ENOENT ? 0 : -1;
}

static int
fork_meta_ordered_marker_valid(const ForkMetaRecV2 *rec,
							   int allow_zero_bound_seq)
{
	int bound = rec->kind == FEV_SEG_GROW_BOUND ||
		rec->kind == FEV_SEG_COMMIT_BOUND;
	int unbound = rec->kind == FEV_SEG_GROW || rec->kind == FEV_SEG_COMMIT;

	return (bound || unbound) && rec->magic == FORK_META_V2_MAGIC &&
		rec->rec_len == sizeof(*rec) && rec->timeline < MAX_TIMELINES &&
		rec->key.klass <= PS_KLASS_READER_SNAPSHOT &&
		rec->nblocks != 0 &&
		((bound && rec->order_id != 0 &&
		  (allow_zero_bound_seq || rec->admission_seq != 0)) ||
		 (unbound && rec->order_id == 0 && rec->admission_seq == 0)) &&
		rec->pad[0] == 0 && rec->pad[1] == 0 && rec->pad[2] == 0;
}

static int
fork_meta_bound_marker_valid(const ForkMetaRecV2 *rec, int allow_zero_seq)
{
	return (rec->kind == FEV_SEG_GROW_BOUND ||
			rec->kind == FEV_SEG_COMMIT_BOUND) &&
		fork_meta_ordered_marker_valid(rec, allow_zero_seq);
}

static int
fork_meta_snapshot_record_valid(const ForkMetaRecV2 *records, uint64_t index,
								unsigned int part, PsPruneFence cutoff)
{
	const ForkMetaRecV2 *rec = &records[index];
	int ordered_marker = rec->kind >= FEV_SEG_GROW &&
		rec->kind <= FEV_SEG_COMMIT_BOUND;
	int future;

	if (rec->magic != FORK_META_V2_MAGIC || rec->rec_len != sizeof(*rec) ||
		rec->timeline >= MAX_TIMELINES ||
		rec->key.klass > PS_KLASS_READER_SNAPSHOT ||
		(!ordered_marker && (rec->kind > FEV_DEAD || rec->order_id != 0)) ||
		(ordered_marker && !fork_meta_ordered_marker_valid(rec, 1)) ||
		rec->pad[0] != 0 || rec->pad[1] != 0 ||
		rec->pad[2] != 0 || (rec->kind == FEV_DEAD && rec->nblocks != 0))
	{
		fprintf(stderr, "pagestore: invalid forkmeta snapshot record part=%u index=%llu kind=%u timeline=%u\n",
				part, (unsigned long long) index, rec->kind, rec->timeline);
		return 0;
	}
	future = fork_meta_event_future(rec->lsn, rec->admission_seq,
									  cutoff.lsn, cutoff.admission_seq);
	if ((part == FORK_META_SNAPSHOT_CHECKPOINT && future) ||
		(part == FORK_META_SNAPSHOT_TAIL && !future))
	{
		fprintf(stderr, "pagestore: forkmeta snapshot partition violation part=%u index=%llu lsn=%llu seq=%llu\n",
				part, (unsigned long long) index,
				(unsigned long long) rec->lsn,
				(unsigned long long) rec->admission_seq);
		return 0;
	}
	return 1;
}

typedef struct ForkMetaSnapshotOrderSlot
{
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint64_t	admission_seq;
	int		used;
} ForkMetaSnapshotOrderSlot;

static int
fork_meta_snapshot_order_slots_init(uint64_t nrecords,
								 ForkMetaSnapshotOrderSlot **slots_out,
								 size_t *capacity_out)
{
	size_t capacity = 16;

	if (nrecords > SIZE_MAX / 2 || (size_t) nrecords > SIZE_MAX / 2)
		return -1;
	while (capacity < (size_t) nrecords * 2)
	{
		if (capacity > SIZE_MAX / 2)
			return -1;
		capacity *= 2;
	}
	*slots_out = calloc(capacity, sizeof(**slots_out));
	if (*slots_out == NULL)
		return -1;
	*capacity_out = capacity;
	return 0;
}

static int
fork_meta_snapshot_order_check(ForkMetaSnapshotOrderSlot *slots,
							   size_t capacity, const ForkMetaRecV2 *rec,
							   unsigned int part, uint64_t index)
{
	size_t pos = (fnv(&rec->key, sizeof(rec->key)) ^
					 (rec->timeline * 40503u)) & (capacity - 1);

	for (;;)
	{
		ForkMetaSnapshotOrderSlot *slot = &slots[pos];

		if (!slot->used)
		{
			slot->used = 1;
			slot->timeline = rec->timeline;
			slot->key = rec->key;
			slot->lsn = rec->lsn;
			slot->admission_seq = rec->admission_seq;
			return 1;
		}
		if (slot->timeline == rec->timeline && key_eq(&slot->key, &rec->key))
		{
			if (slot->lsn > rec->lsn ||
				(slot->lsn == rec->lsn && slot->admission_seq != 0 &&
				 rec->admission_seq != 0 &&
				 slot->admission_seq > rec->admission_seq))
			{
				fprintf(stderr, "pagestore: forkmeta snapshot order violation part=%u index=%llu prev=(%llu,%llu) current=(%llu,%llu)\n",
						part, (unsigned long long) index,
						(unsigned long long) slot->lsn,
						(unsigned long long) slot->admission_seq,
						(unsigned long long) rec->lsn,
						(unsigned long long) rec->admission_seq);
				return 0;
			}
			slot->lsn = rec->lsn;
			slot->admission_seq = rec->admission_seq;
			return 1;
		}
		pos = (pos + 1) & (capacity - 1);
	}
}

static int
fork_meta_snapshot_load(const char *directory)
{
	PsForkmetaSnapshot snapshot;
	unsigned char *data[2] = {NULL, NULL};
	uint64_t lengths[2];
	ForkMetaSnapshotPayloadHeader headers[2];
	PsPruneFence cutoff;

	if (ps_forkmeta_snapshot_open(&snapshot, directory) != 0)
		return -1;
	lengths[0] = snapshot.checkpoint.len;
	lengths[1] = snapshot.tail.len;
	cutoff.lsn = snapshot.cutoff_lsn;
	cutoff.admission_seq = snapshot.cutoff_admission_seq;
	for (unsigned int part = 0; part < 2; part++)
	{
		uint64_t nrecords;
		ForkMetaRecV2 *records;
		ForkMetaSnapshotOrderSlot *order_slots = NULL;
		size_t order_capacity = 0;

		if (lengths[part] > SIZE_MAX ||
			lengths[part] < sizeof(ForkMetaSnapshotPayloadHeader))
			goto fail;
		if ((data[part] = malloc((size_t) lengths[part])) == NULL)
			goto fail;
		if (ps_forkmeta_snapshot_read(&snapshot, part, 0, data[part],
								 lengths[part]) != 0)
			goto fail;
		memcpy(&headers[part], data[part], sizeof(headers[part]));
		if (headers[part].magic != FORK_META_SNAPSHOT_PAYLOAD_MAGIC ||
			headers[part].version != FORK_META_SNAPSHOT_PAYLOAD_VERSION ||
			headers[part].header_bytes != sizeof(headers[part]) ||
			headers[part].part != part ||
			headers[part].record_bytes != sizeof(ForkMetaRecV2) ||
			headers[part].generation != snapshot.generation ||
			headers[part].cutoff_lsn != snapshot.cutoff_lsn ||
			headers[part].cutoff_admission_seq != snapshot.cutoff_admission_seq ||
			headers[part].freeze_admission_seq == 0)
			goto fail;
		if (part == FORK_META_SNAPSHOT_CHECKPOINT)
			nrecords = headers[part].checkpoint_records;
		else
			nrecords = headers[part].tail_records;
		if (headers[part].checkpoint_records >
			UINT64_MAX / sizeof(ForkMetaRecV2) ||
			headers[part].tail_records > UINT64_MAX / sizeof(ForkMetaRecV2) ||
			nrecords > (UINT64_MAX - sizeof(headers[part])) / sizeof(ForkMetaRecV2) ||
			lengths[part] != sizeof(headers[part]) + nrecords * sizeof(ForkMetaRecV2) ||
			headers[part].checkpoint_bytes !=
				headers[part].checkpoint_records * sizeof(ForkMetaRecV2) ||
				headers[part].tail_bytes !=
					headers[part].tail_records * sizeof(ForkMetaRecV2))
			goto fail;
		records = (ForkMetaRecV2 *) (data[part] + sizeof(headers[part]));
		if (fork_meta_snapshot_order_slots_init(nrecords, &order_slots,
												&order_capacity) != 0)
			goto fail;
		for (uint64_t i = 0; i < nrecords; i++)
		{
			if (!fork_meta_snapshot_record_valid(records, i, part, cutoff) ||
				!fork_meta_snapshot_order_check(order_slots, order_capacity,
												&records[i], part, i))
			{
				free(order_slots);
				goto fail;
			}
		}
		free(order_slots);
	}
	if (memcmp(&headers[0].generation, &headers[1].generation,
			   sizeof(headers[0]) - offsetof(ForkMetaSnapshotPayloadHeader,
											 generation)) != 0)
		goto fail;
	admission_seq_observe(headers[0].freeze_admission_seq);
	for (unsigned int part = 0; part < 2; part++)
	{
		uint64_t nrecords = part == FORK_META_SNAPSHOT_CHECKPOINT ?
			headers[part].checkpoint_records : headers[part].tail_records;
		ForkMetaRecV2 *records = (ForkMetaRecV2 *)
			(data[part] + sizeof(headers[part]));

		for (uint64_t i = 0; i < nrecords; i++)
		{
			admission_seq_observe(records[i].admission_seq);
			if (records[i].kind >= FEV_SEG_GROW &&
				records[i].kind <= FEV_SEG_COMMIT_BOUND)
				fork_event_add_seg_marker(
					fork_get_or_create(records[i].timeline, &records[i].key),
					records[i].lsn, records[i].nblocks, records[i].kind,
					records[i].order_id, records[i].admission_seq);
			else
				fork_event_add(fork_get_or_create(records[i].timeline, &records[i].key),
							   records[i].lsn, records[i].admission_seq,
							   records[i].nblocks, records[i].kind);
		}
	}
	fork_meta_snapshot_generation = snapshot.generation;
	fork_meta_snapshot_cutoff_lsn = snapshot.cutoff_lsn;
	fork_meta_snapshot_cutoff_seq = snapshot.cutoff_admission_seq;
	fork_meta_snapshot_freeze_seq = headers[0].freeze_admission_seq;
	fork_meta_snapshot_bytes = snapshot.checkpoint.len + snapshot.tail.len;
	fork_meta_snapshot_checkpoint_meta = snapshot.checkpoint;
	fork_meta_snapshot_tail_meta = snapshot.tail;
	ps_forkmeta_snapshot_close(&snapshot);
	free(data[0]);
	free(data[1]);
	return 0;

fail:
	ps_forkmeta_snapshot_close(&snapshot);
	free(data[0]);
	free(data[1]);
	return -1;
}

static int
fork_meta_snapshot_marker_matches(const ForkMetaRecV2 *rec)
{
	PsKey zero_key;

	memset(&zero_key, 0, sizeof(zero_key));
	return rec->magic == FORK_META_V2_MAGIC && rec->rec_len == sizeof(*rec) &&
		rec->timeline == 0 && key_eq(&rec->key, &zero_key) &&
		rec->lsn == fork_meta_snapshot_cutoff_lsn &&
		rec->admission_seq == fork_meta_snapshot_cutoff_seq &&
		rec->order_id == fork_meta_snapshot_generation && rec->nblocks == 0 &&
		rec->kind == FEV_SNAPSHOT_BASE && rec->pad[0] == 0 &&
		rec->pad[1] == 0 && rec->pad[2] == 0;
}

static int
fork_meta_selected_suffix_valid(const ForkMetaRecV2 *rec)
{
	if (rec->magic != FORK_META_V2_MAGIC || rec->rec_len != sizeof(*rec) ||
		rec->timeline >= MAX_TIMELINES ||
		rec->key.klass > PS_KLASS_READER_SNAPSHOT ||
		rec->admission_seq == 0 || rec->pad[0] != 0 || rec->pad[1] != 0 ||
		rec->pad[2] != 0 ||
		!fork_meta_event_future(rec->lsn, rec->admission_seq,
								fork_meta_snapshot_cutoff_lsn,
								fork_meta_snapshot_cutoff_seq))
		return 0;
	switch (rec->kind)
	{
		case FEV_GROW:
			return rec->order_id == 0 && rec->nblocks != 0;
		case FEV_SET:
			return rec->order_id == 0;
		case FEV_DEAD:
			return rec->order_id == 0 && rec->nblocks == 0;
		case FEV_SEG_GROW_BOUND:
		case FEV_SEG_COMMIT_BOUND:
			return fork_meta_bound_marker_valid(rec, 0);
		default:
			/* Migration, legacy/unbound segment, SEG_ID, and epoch markers are
			 * never valid records after a selected current-epoch marker. */
			return 0;
	}
}

/* The selected snapshot owns the entire old epoch, including its captured
 * future tail.  Preserve a matching new epoch byte-for-byte; otherwise replace
 * the whole source with a marker-only epoch. */
static int
fork_meta_snapshot_reconcile_source(void)
{
	ForkMetaByteVec rewritten = {0};
	PsKey zero_key;
	ForkMetaRecV2 first;
	int nread;

	memset(&zero_key, 0, sizeof(zero_key));
	if (fork_meta_vec_record(&rewritten, 0, &zero_key,
						 fork_meta_snapshot_cutoff_lsn,
						 fork_meta_snapshot_cutoff_seq,
						 fork_meta_snapshot_generation, 0, FEV_SNAPSHOT_BASE) != 0)
		goto fail;
	nread = ps_storage->fork_meta_read(0, &first, sizeof(first));
	if (nread == (int) sizeof(first) && fork_meta_snapshot_marker_matches(&first))
	{
		uint64_t off = sizeof(first);

		for (;;)
		{
			ForkMetaRecV2 rec;

			nread = ps_storage->fork_meta_read(off, &rec, sizeof(rec));
			if (nread == 0)
				break;
			if (nread < 0)
				goto fail;
			if (nread != (int) sizeof(rec))
			{
				/* The marker and every complete suffix record are acknowledged.
				 * Discard only the unacknowledged crash tail, matching ordinary
				 * pre-snapshot log recovery. */
				if (ps_storage->fork_meta_truncate == NULL ||
					ps_storage->fork_meta_truncate(off) != 0)
					goto fail;
				break;
			}
			if (!fork_meta_selected_suffix_valid(&rec))
				goto fail;
			off += sizeof(rec);
		}
		fork_meta_bytes_store(off);
		free(rewritten.data);
		return 0;
	}
	if (rewritten.len > UINT32_MAX || ps_storage->fork_meta_rewrite == NULL ||
		ps_storage->fork_meta_rewrite(rewritten.data, (uint32_t) rewritten.len) != 0)
		goto fail;
	fork_meta_bytes_store(rewritten.len);
	free(rewritten.data);
	return 0;

fail:
	free(rewritten.data);
	return -1;
}

static int
load_fork_meta(void)
{
	uint64_t	off = 0;
	int			have_records = 0;
	int			nread = 0;
	uint64_t	record_number = 0;
	uint64_t	migration_prefix_bytes = 0;
	int			migration_prefix_valid = 1;

	for (;;)
	{
		ForkMetaRecV2 rec;
		uint32_t	first;
		uint64_t	rec_size;
		int			ordered_marker_valid = 0;

		nread = ps_storage->fork_meta_read(off, &first, sizeof(first));
		if (nread != (int) sizeof(first))
			break;
		memset(&rec, 0, sizeof(rec));
		if (first == FORK_META_V2_MAGIC)
		{
			nread = ps_storage->fork_meta_read(off, &rec, sizeof(rec));
			if (nread != (int) sizeof(rec) || rec.rec_len != sizeof(rec))
				break;
			rec_size = sizeof(rec);
			if (rec.kind >= FEV_SEG_GROW &&
				rec.kind <= FEV_SEG_COMMIT_BOUND)
				ordered_marker_valid =
					fork_meta_ordered_marker_valid(&rec, 0);
		}
		else
		{
			ForkMetaRecV1 old;

			nread = ps_storage->fork_meta_read(off, &old, sizeof(old));
			if (nread != (int) sizeof(old))
				break;
			rec.timeline = old.timeline;
			rec.key = old.key;
			rec.lsn = old.lsn;
			rec.nblocks = old.nblocks;
			rec.kind = old.kind;
			rec.magic = FORK_META_V2_MAGIC;
			rec.rec_len = sizeof(rec);
			memcpy(rec.pad, old.pad, sizeof(rec.pad));
			rec_size = sizeof(old);
			if (old.kind == FEV_SEG_GROW || old.kind == FEV_SEG_COMMIT)
				ordered_marker_valid =
					fork_meta_ordered_marker_valid(&rec, 1);
			if (old.kind == FEV_SEG_GROW_BOUND ||
				old.kind == FEV_SEG_COMMIT_BOUND)
			{
				ForkMetaRecV1 idrec;
				int			idread;

				idread = ps_storage->fork_meta_read(off + sizeof(old), &idrec,
										   sizeof(idrec));
				if (idread != (int) sizeof(idrec))
					break;
				rec_size += sizeof(idrec);
				if (idrec.kind == FEV_SEG_ID && idrec.timeline == old.timeline &&
					key_eq(&idrec.key, &old.key) && idrec.nblocks == old.nblocks &&
					idrec.lsn != 0 && idrec.pad[0] == 0 && idrec.pad[1] == 0 &&
					idrec.pad[2] == 0)
				{
					rec.order_id = idrec.lsn;
					ordered_marker_valid =
						fork_meta_bound_marker_valid(&rec, 1);
				}
			}
		}
		if (migration_prefix_valid &&
			fork_meta_migration_marker_valid(&rec))
			migration_prefix_bytes += rec_size;
		else
			migration_prefix_valid = 0;
		have_records = 1;
		if (fork_meta_snapshot_generation != 0)
			if ((record_number == 0 && !fork_meta_snapshot_marker_matches(&rec)) ||
				(record_number != 0 && !fork_meta_selected_suffix_valid(&rec)))
				return -1;
		if (rec.admission_seq != 0)
			admission_seq_observe(rec.admission_seq);
		if (rec.order_id != 0 && rec.kind != FEV_SNAPSHOT_BASE &&
			ordered_marker_valid)
			segment_order_id_observe(rec.order_id);
		if (rec.kind == FEV_MIGRATED)
			fork_meta_migrated = 1;
		else if (rec.kind == FEV_MIGRATING)
			fork_meta_migrating = 1;
		else if (rec.kind == FEV_SNAPSHOT_BASE)
		{
			if (record_number != 0 || fork_meta_snapshot_generation == 0 ||
				!fork_meta_snapshot_marker_matches(&rec))
				return -1;
			/* The selected snapshot has already been loaded.  The marker is
			 * an epoch boundary, not a fork event. */
		}
		else if (ordered_marker_valid &&
				 rec.timeline < MAX_TIMELINES)
			fork_event_add_seg_marker(
				fork_get_or_create(rec.timeline, &rec.key),
				rec.lsn, rec.nblocks, rec.kind, rec.order_id,
				rec.admission_seq);
		else if (rec.kind <= FEV_DEAD && rec.timeline < MAX_TIMELINES)
		{
			fork_event_add(fork_get_or_create(rec.timeline, &rec.key),
						   rec.lsn, rec.admission_seq, rec.nblocks, rec.kind);
		}
		else
			fprintf(stderr, "pagestore: skipping invalid fork-meta record "
					"(timeline=%u kind=%u)\n", rec.timeline, rec.kind);
		off += rec_size;
		record_number++;
	}
	/* A short tail is not a record and must not become a prefix of the first
	 * migration marker (or any later append). */
	if (fork_meta_snapshot_generation != 0 && nread != 0)
		return -1;
	if (nread > 0 && ps_storage->fork_meta_truncate(off) != 0)
		return -1;
	if (nread < 0 && off != 0)
		return -1;
	fork_meta_bytes_store(off);
	/* Keep the proof accumulated before the first ordinary record.  A later
	 * MIGRATED seal is not part of that proof unless it was itself contiguous
	 * with the strictly validated marker prefix. */
	fork_meta_irreducible_prefix_bytes = migration_prefix_bytes;
	/*
	 * Only an absent/empty log is unambiguously a pre-fork-events store.  A
	 * nonempty log without either marker was written by the immediately
	 * preceding format: its definitive SET/DEAD history is authoritative and
	 * replaying raw lsn-0 pages against it could resurrect a truncated fork.
	 *
	 * Stamp an empty log before scanning segments.  The start marker lets a
	 * later boot distinguish an interrupted migration (continue legacy replay)
	 * from that older, already-event-aware format (normal replay).  The daemon
	 * must not become writable until this marker and the final seal are durable.
	 */
	if (!have_records)
	{
		PsKey		zk;

		memset(&zk, 0, sizeof(zk));
		if (fork_meta_persist(0, &zk, 0, 0, 0, FEV_MIGRATING) != 0)
		{
			fprintf(stderr, "pagestore: could not start the fork-meta migration\n");
			return -1;
		}
		else
		{
			fork_meta_migrating = 1;
			fork_meta_irreducible_prefix_bytes = sizeof(ForkMetaRecV2);
		}
		fork_meta_legacy = 1;
	}
	else
		fork_meta_legacy = fork_meta_migrating && !fork_meta_migrated;
	return 0;
}

static int
fork_meta_timeline_is_deleting(uint32_t timeline)
{
	PsTimelineState state;

	return ps_timeline_state(timeline, &state, NULL) &&
		state == PS_TIMELINE_DELETING;
}

/* This lock-free outer probe consults only atomically published lifecycle
 * state.  The fork-index scan itself must wait for the cutover's admission and
 * shard write locks. */
static int
fork_meta_deletion_probe_due(void)
{
	for (uint32_t timeline = 0; timeline < MAX_TIMELINES; timeline++)
		if (!__atomic_load_n(&fork_meta_deletion_cutover_done[timeline],
								 __ATOMIC_ACQUIRE) &&
			fork_meta_timeline_is_deleting(timeline))
			return 1;
	return 0;
}

/* Caller holds admission-write, every shard-write lock, and map-write. */
static int
fork_meta_deletion_records_present_locked(void)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (ForkEnt *e = g_shards[sh].fork_idx[bucket]; e; e = e->next)
				if (e->timeline < MAX_TIMELINES && e->nev != 0 &&
					fork_meta_timeline_is_deleting(e->timeline))
					return 1;
	return 0;
}

static int
fork_meta_timeline_records_present_locked(uint32_t target)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (ForkEnt *e = g_shards[sh].fork_idx[bucket]; e; e = e->next)
				if (e->timeline == target && e->nev != 0)
					return 1;
	return 0;
}

/* Caller holds admission-write, every shard-write lock, and map-write. */
static int
fork_meta_deletion_cutover_due_locked(void)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (ForkEnt *e = g_shards[sh].fork_idx[bucket]; e; e = e->next)
			{
				if (e->timeline >= MAX_TIMELINES ||
					__atomic_load_n(&fork_meta_deletion_cutover_done[e->timeline],
									__ATOMIC_ACQUIRE) ||
					!fork_meta_timeline_is_deleting(e->timeline))
					continue;
				if (e->nev != 0)
					return 1;
			}
	return 0;
}

static void
fork_meta_mark_deletion_cutover_done_locked(void)
{
	for (uint32_t timeline = 0; timeline < MAX_TIMELINES; timeline++)
		if (fork_meta_timeline_is_deleting(timeline))
			__atomic_store_n(&fork_meta_deletion_cutover_done[timeline], 1,
							 __ATOMIC_RELEASE);
}

/* The cutoff is the lexicographic minimum of the durable page-reclaimed
 * frontier for every timeline that owns fork metadata.  Retention owners are
 * deliberately not consulted here: they are admission fences, not proof that
 * the source fork history has been durably replaced.  A deletion-filtered
 * cutover may use the selected cutoff, or (for a store without one) the small
 * conservative operational floor. */
static int
fork_meta_snapshot_cutoff(PsPruneFence *cutoff_out, int filter_deleting,
						  int preserve_survivors)
{
	PsPruneFence cutoff = {0, 0};
	int have = 0;
	int missing = 0;

	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (ForkEnt *e = g_shards[sh].fork_idx[bucket]; e; e = e->next)
			{
				int owns = 0;

				if (filter_deleting &&
					fork_meta_timeline_is_deleting(e->timeline))
					continue;

				for (uint32_t i = 0; i < e->nev; i++)
					if (e->ev[i].kind <= FEV_DEAD)
					{
						owns = 1;
						break;
					}
				if (!owns)
					continue;
				{
					PsPruneFence frontier = page_frontier_current(e->timeline);

					if (e->timeline >= MAX_TIMELINES ||
						frontier.lsn == 0 || frontier.admission_seq == 0)
				{
					missing = 1;
					continue;
				}
					if (!have || frontier.lsn < cutoff.lsn ||
						(frontier.lsn == cutoff.lsn &&
						 frontier.admission_seq < cutoff.admission_seq))
					{
						cutoff = frontier;
						have = 1;
					}
				}
			}
	if (missing || !have)
	{
		if (!filter_deleting || (missing && !preserve_survivors))
			return -1;
		if (fork_meta_snapshot_generation != 0 &&
			fork_meta_snapshot_cutoff_lsn != 0 &&
			fork_meta_snapshot_cutoff_seq != 0)
		{
			cutoff.lsn = fork_meta_snapshot_cutoff_lsn;
			cutoff.admission_seq = fork_meta_snapshot_cutoff_seq;
		}
		else
		{
			/* (1,1) is the first valid post-snapshot operational position.
			 * fork_op_lsn() promotes unstamped metadata mutations to this
			 * position, while explicit future WAL positions remain ordered after
			 * it. */
			cutoff.lsn = 1;
			cutoff.admission_seq = 1;
		}
	}
	if (fork_meta_snapshot_generation != 0 &&
		(cutoff.lsn < fork_meta_snapshot_cutoff_lsn ||
		 (cutoff.lsn == fork_meta_snapshot_cutoff_lsn &&
		  cutoff.admission_seq < fork_meta_snapshot_cutoff_seq)))
	{
		/* A deletion-forced generation retains every surviving record, so it
		 * can safely keep the already-selected coverage tuple.  Ordinary pruning
		 * must fail closed if its durable frontiers somehow regress. */
		if (!preserve_survivors)
			return -1;
		cutoff.lsn = fork_meta_snapshot_cutoff_lsn;
		cutoff.admission_seq = fork_meta_snapshot_cutoff_seq;
	}
	*cutoff_out = cutoff;
	return 0;
}

/* Source growth is reclaimable only when the current compactor can name an
 * operational page/forkmeta cutoff.  A migration marker prefix alone is not
 * such a cutoff and must never make metadata churn throttle. */
static int
fork_meta_source_cutoff_provable(void)
{
	PsPruneFence cutoff;
	int rc;

	if (!map_locks_ready)
		return 0;
	if (fork_meta_snapshot_generation != 0 &&
		fork_meta_snapshot_cutoff_lsn != 0 &&
		fork_meta_snapshot_cutoff_seq != 0)
		return 1;
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		ps_lock_shard_rd(sh);
	ps_lock_map_rd();
	rc = fork_meta_snapshot_cutoff(&cutoff, 0, 0);
	ps_unlock_map();
	for (uint32_t sh = core_shards(); sh > 0; sh--)
		ps_unlock_shard(sh - 1);
	return rc == 0;
}

static int
fork_meta_snapshot_marker_present(const ForkMetaRecV2 *rec)
{
	ForkEnt *e = fork_find(rec->timeline, &rec->key);

	if (e == NULL)
		return 0;
	for (uint32_t i = 0; i < e->nev; i++)
		if (e->ev[i].lsn == rec->lsn &&
			e->ev[i].admission_seq == rec->admission_seq &&
			e->ev[i].order_id == rec->order_id &&
			e->ev[i].nblocks == rec->nblocks &&
			e->ev[i].marker_kind == rec->kind)
			return 1;
	return 0;
}

/* A non-future ordered marker is useful only while its exact page admission
 * remains recoverable.  Modern admissions are identified by their sequence;
 * legacy sequence-zero SEG1/SEG3 and V1 bound records additionally rely on
 * block and page/growth LSN.  WAL-less pages retain page LSN zero while their
 * marker carries the fork growth floor. */
static int
fork_meta_snapshot_marker_page_retained(uint32_t timeline, const PsKey *key,
										uint64_t marker_lsn,
										uint64_t admission_seq,
										uint32_t nblocks)
{
	ForkEnt *fork = fork_find(timeline, key);
	uint32_t block;

	if (fork == NULL || nblocks == 0)
		return 0;
	block = nblocks - 1;
	for (PageEnt *page = fork->pages; page != NULL; page = page->fork_next)
		if (page->block == block)
			for (int i = 0; i < page->nver; i++)
				if (page->vers[i].admission_seq == admission_seq &&
					(page->vers[i].lsn == marker_lsn || page->vers[i].lsn == 0))
					return 1;
	return 0;
}

static int
fork_meta_snapshot_append_source_markers(ForkMetaByteVec *checkpoint,
										ForkMetaByteVec *tail,
										PsPruneFence cutoff,
										int filter_deleting,
										int preserve_survivors)
{
	uint64_t off = 0;

	for (;;)
	{
		ForkMetaRecV2 rec;
		uint32_t magic;
		int nread = ps_storage->fork_meta_read(off, &magic, sizeof(magic));

		if (nread == 0)
			return 0;
		if (nread != (int) sizeof(magic))
			return -1;
		if (magic == FORK_META_V2_MAGIC)
		{
			nread = ps_storage->fork_meta_read(off, &rec, sizeof(rec));
			if (nread != (int) sizeof(rec) || rec.rec_len != sizeof(rec))
				return -1;
			if (fork_meta_ordered_marker_valid(&rec, 0) &&
				(!filter_deleting ||
				 !fork_meta_timeline_is_deleting(rec.timeline)) &&
				(preserve_survivors ||
				 (fork_meta_event_future(rec.lsn, rec.admission_seq,
										 cutoff.lsn, cutoff.admission_seq) ||
				  fork_meta_snapshot_marker_page_retained(rec.timeline, &rec.key,
															  rec.lsn, rec.admission_seq,
															  rec.nblocks))) &&
				!fork_meta_snapshot_marker_present(&rec))
			{
				ForkMetaByteVec *part = fork_meta_event_future(
					rec.lsn, rec.admission_seq, cutoff.lsn,
					cutoff.admission_seq) ? tail : checkpoint;

				if (fork_meta_vec_record(part, rec.timeline, &rec.key,
						rec.lsn, rec.admission_seq, rec.order_id,
						rec.nblocks, rec.kind) != 0)
					return -1;
			}
			off += sizeof(rec);
		}
		else
		{
			ForkMetaRecV1 old;
			ForkMetaRecV1 idrec;

			nread = ps_storage->fork_meta_read(off, &old, sizeof(old));
			if (nread != (int) sizeof(old))
				return -1;
			off += sizeof(old);
			memset(&rec, 0, sizeof(rec));
			rec.magic = FORK_META_V2_MAGIC;
			rec.rec_len = sizeof(rec);
			rec.timeline = old.timeline;
			rec.key = old.key;
			rec.lsn = old.lsn;
			rec.nblocks = old.nblocks;
			rec.kind = old.kind;
			memcpy(rec.pad, old.pad, sizeof(rec.pad));
			if (old.kind == FEV_SEG_GROW_BOUND ||
				old.kind == FEV_SEG_COMMIT_BOUND)
			{
				memset(&idrec, 0, sizeof(idrec));
				nread = ps_storage->fork_meta_read(off, &idrec, sizeof(idrec));
				if (nread != (int) sizeof(idrec))
					return -1;
				off += sizeof(idrec);
				rec.order_id = idrec.lsn;
			}
			if ((old.kind == FEV_SEG_GROW || old.kind == FEV_SEG_COMMIT ||
					 old.kind == FEV_SEG_GROW_BOUND ||
					 old.kind == FEV_SEG_COMMIT_BOUND) &&
				(old.kind < FEV_SEG_GROW_BOUND ||
				 (idrec.kind == FEV_SEG_ID && idrec.timeline == old.timeline &&
				  key_eq(&idrec.key, &old.key) && idrec.nblocks == old.nblocks &&
				  idrec.lsn != 0 && idrec.pad[0] == 0 && idrec.pad[1] == 0 &&
				  idrec.pad[2] == 0)) &&
				fork_meta_ordered_marker_valid(&rec, 1) &&
				(!filter_deleting ||
				 !fork_meta_timeline_is_deleting(rec.timeline)) &&
				(preserve_survivors ||
				 (fork_meta_event_future(rec.lsn, 0, cutoff.lsn,
										 cutoff.admission_seq) ||
				  fork_meta_snapshot_marker_page_retained(rec.timeline, &rec.key,
															  rec.lsn, 0, rec.nblocks))) &&
				!fork_meta_snapshot_marker_present(&rec))
			{
				ForkMetaByteVec *part = fork_meta_event_future(
					rec.lsn, 0, cutoff.lsn, cutoff.admission_seq) ?
					tail : checkpoint;

				if (fork_meta_vec_record(part, rec.timeline, &rec.key,
						rec.lsn, 0, rec.order_id, rec.nblocks,
						rec.kind) != 0)
					return -1;
			}
		}
	}
}

static int
fork_meta_migration_marker_valid(const ForkMetaRecV2 *rec)
{
	PsKey zero_key;

	memset(&zero_key, 0, sizeof(zero_key));
	return rec->magic == FORK_META_V2_MAGIC &&
		rec->rec_len == sizeof(*rec) && rec->timeline == 0 &&
		key_eq(&rec->key, &zero_key) && rec->lsn == 0 &&
		rec->admission_seq == 0 && rec->order_id == 0 && rec->nblocks == 0 &&
		(rec->kind == FEV_MIGRATING || rec->kind == FEV_MIGRATED) &&
		rec->pad[0] == 0 && rec->pad[1] == 0 && rec->pad[2] == 0;
}

static int
fork_meta_snapshot_build(ForkMetaByteVec *checkpoint, ForkMetaByteVec *tail,
						  ForkMetaByteVec *source, PsPruneFence cutoff,
						  uint64_t generation, uint64_t freeze_seq,
						  int filter_deleting, int preserve_survivors)
{
	PsKey zero_key;

	memset(&zero_key, 0, sizeof(zero_key));
	if (fork_meta_vec_record(source, 0, &zero_key, cutoff.lsn,
						 cutoff.admission_seq, generation,
						 0, FEV_SNAPSHOT_BASE) != 0)
		return -1;
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (ForkEnt *e = g_shards[sh].fork_idx[bucket]; e; e = e->next)
			{
				PsForkMetaEvent *events = NULL;
				unsigned char *keep = NULL;
				uint32_t *indices = NULL;
				PsPruneFence *raw_fences = NULL;
				PsForkMetaFence *fences = NULL;
				uint32_t nitems = 0, nfences = 0;
				int planned;
				int deleting = filter_deleting &&
					fork_meta_timeline_is_deleting(e->timeline);

				/* A deletion-filtered generation must not carry any lifecycle or
				 * ordered record owned by an explicit DELETING timeline. */
				if (deleting)
					continue;

				for (uint32_t i = 0; i < e->nev; i++)
					if (e->ev[i].kind <= FEV_DEAD)
						nitems++;
				events = nitems == 0 ? NULL :
					malloc((size_t) nitems * sizeof(*events));
				indices = nitems == 0 ? NULL :
					malloc((size_t) nitems * sizeof(*indices));
				keep = calloc(e->nev, 1);
				if ((nitems != 0 && (!events || !indices)) || !keep)
					goto fail_entry;
				for (uint32_t i = 0, j = 0; i < e->nev; i++)
					if (e->ev[i].kind <= FEV_DEAD)
					{
						events[j].lsn = e->ev[i].lsn;
						events[j].admission_seq = e->ev[i].admission_seq;
						events[j].nblocks = e->ev[i].nblocks;
						events[j].kind = e->ev[i].kind;
						indices[j++] = i;
					}
				if (!preserve_survivors)
				{
					if (page_prune_fences(e->timeline, &raw_fences,
										  &nfences) != 0)
						goto fail_entry;
					fences = malloc((size_t) nfences * sizeof(*fences));
					if (nfences != 0 && fences == NULL)
						goto fail_entry;
					{
						uint32_t out = 0;

						for (uint32_t i = 0; i < nfences; i++)
							if (raw_fences[i].lsn < cutoff.lsn ||
								(raw_fences[i].lsn == cutoff.lsn &&
								 raw_fences[i].admission_seq != 0 &&
								 raw_fences[i].admission_seq <= cutoff.admission_seq))
							{
								fences[out].lsn = raw_fences[i].lsn;
								fences[out].admission_seq = raw_fences[i].admission_seq;
								out++;
							}
						nfences = out;
					}
				}
				if (nitems != 0 && !preserve_survivors)
				{
					unsigned char *planned_keep = malloc(nitems);

					if (planned_keep == NULL)
						goto fail_entry;
					planned = ps_forkmeta_prune_plan(events, nitems,
						(PsForkMetaFence) {cutoff.lsn, cutoff.admission_seq},
						fences, nfences, planned_keep);
					if (planned < 0)
					{
						free(planned_keep);
						goto fail_entry;
					}
					for (uint32_t j = 0; j < nitems; j++)
						keep[indices[j]] = planned_keep[j] ||
							fork_meta_event_future(events[j].lsn,
								events[j].admission_seq, cutoff.lsn,
								cutoff.admission_seq);
					free(planned_keep);
				}
				/* Serialize the retained lifecycle and ordered-admission records in
				 * their original per-fork order.  Legacy sequence-zero markers rely
				 * on this physical order at equal LSN.  The forced deletion path
				 * deliberately retains every record for surviving owners. */
				for (uint32_t i = 0; i < e->nev; i++)
				{
					ForkEvent *event = &e->ev[i];
					int future = fork_meta_event_future(event->lsn,
						 event->admission_seq, cutoff.lsn, cutoff.admission_seq);
					uint8_t kind;
					uint64_t order_id;

					if (preserve_survivors)
					{
						kind = event->marker_kind != 0 ? event->marker_kind :
							event->kind;
						order_id = event->marker_kind != 0 ? event->order_id : 0;
					}
					else if (event->marker_kind != 0 &&
						(future || fork_meta_snapshot_marker_page_retained(
							e->timeline, &e->key, event->lsn,
							event->admission_seq, event->nblocks)))
					{
						kind = event->marker_kind;
						order_id = event->order_id;
					}
					else if (event->kind <= FEV_DEAD && keep[i])
					{
						kind = event->kind;
						order_id = 0;
					}
					else
						continue;

					if (fork_meta_vec_record(future ? tail : checkpoint,
							e->timeline, &e->key, event->lsn,
							event->admission_seq, order_id,
							event->nblocks, kind) != 0)
						goto fail_entry;
				}
				free(events);
				free(indices);
				free(keep);
				free(raw_fences);
				free(fences);
				continue;

fail_entry:
				free(events);
				free(indices);
				free(keep);
				free(raw_fences);
				free(fences);
				return -1;
			}
	if (fork_meta_snapshot_append_source_markers(checkpoint, tail, cutoff,
											 filter_deleting,
											 preserve_survivors) != 0)
		return -1;
	{
		ForkMetaSnapshotPayloadHeader headers[2];
		ForkMetaByteVec wrapped[2] = {{0}, {0}};
		ForkMetaByteVec *parts[2] = {checkpoint, tail};

		memset(headers, 0, sizeof(headers));
		for (unsigned int part = 0; part < 2; part++)
		{
			headers[part].magic = FORK_META_SNAPSHOT_PAYLOAD_MAGIC;
			headers[part].version = FORK_META_SNAPSHOT_PAYLOAD_VERSION;
			headers[part].header_bytes = sizeof(headers[part]);
			headers[part].part = part;
			headers[part].record_bytes = sizeof(ForkMetaRecV2);
			headers[part].generation = generation;
			headers[part].cutoff_lsn = cutoff.lsn;
			headers[part].cutoff_admission_seq = cutoff.admission_seq;
			headers[part].freeze_admission_seq = freeze_seq;
			headers[part].checkpoint_records =
				checkpoint->len / sizeof(ForkMetaRecV2);
			headers[part].tail_records = tail->len / sizeof(ForkMetaRecV2);
			headers[part].checkpoint_bytes = checkpoint->len;
			headers[part].tail_bytes = tail->len;
			if (fork_meta_vec_append(&wrapped[part], &headers[part],
								 sizeof(headers[part])) != 0 ||
				fork_meta_vec_append(&wrapped[part], parts[part]->data,
								 parts[part]->len) != 0)
			{
				free(wrapped[0].data);
				free(wrapped[1].data);
				return -1;
			}
		}
		free(checkpoint->data);
		free(tail->data);
		*checkpoint = wrapped[0];
		*tail = wrapped[1];
	}
	if (source->len > UINT32_MAX)
		return -1;
	return 0;
}

static int
fork_meta_snapshot_retry_due(void)
{
	struct timespec now;

	clock_gettime(CLOCK_MONOTONIC, &now);
	return now.tv_sec > fork_meta_snapshot_retry_at.tv_sec ||
		(now.tv_sec == fork_meta_snapshot_retry_at.tv_sec &&
		 now.tv_nsec >= fork_meta_snapshot_retry_at.tv_nsec);
}

static int
fork_meta_snapshot_gc_due(void)
{
	return fork_meta_snapshot_gc_pending && fork_meta_snapshot_retry_due();
}

static int
fork_meta_backpressure_throttled(void)
{
	return (__atomic_load_n(&backpressure_gate_mask, __ATOMIC_ACQUIRE) &
			PS_BACKPRESSURE_GATE_FORKMETA_THROTTLED) != 0;
}

static int
fork_meta_snapshot_due_locked(void)
{
	const char *value = getenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
	uint64_t threshold = value ? strtoull(value, NULL, 10) : 65536;

	if (threshold < 1024)
		threshold = 1024;
	if (value == NULL && fork_meta_snapshot_generation != 0 &&
		fork_meta_snapshot_bytes <= UINT64_MAX / 2 &&
		fork_meta_snapshot_bytes * 2 > threshold)
		threshold = fork_meta_snapshot_bytes * 2;
	if (!fork_meta_snapshot_retry_due())
		return 0;
	return !fork_meta_poisoned_load() && !fork_meta_snapshot_gc_pending &&
		(fork_meta_backpressure_throttled() || fork_meta_bytes_load() >= threshold ||
		 fork_meta_deletion_cutover_due_locked());
}

static int
fork_meta_snapshot_due(void)
{
	const char *value = getenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
	uint64_t threshold = value ? strtoull(value, NULL, 10) : 65536;

	if (threshold < 1024)
		threshold = 1024;
	if (value == NULL && fork_meta_snapshot_generation != 0 &&
		fork_meta_snapshot_bytes <= UINT64_MAX / 2 &&
		fork_meta_snapshot_bytes * 2 > threshold)
		threshold = fork_meta_snapshot_bytes * 2;
	if (!fork_meta_snapshot_retry_due())
		return 0;
	return !fork_meta_poisoned_load() && !fork_meta_snapshot_gc_pending &&
		(fork_meta_backpressure_throttled() || fork_meta_bytes_load() >= threshold ||
		 fork_meta_deletion_probe_due());
}

static int
fork_meta_prepared_matches_selected(const PsForkmetaSnapshotPrepared *pending,
									const PsForkmetaSnapshot *selected)
{
	return pending->generation == selected->generation &&
		pending->cutoff_lsn == selected->cutoff_lsn &&
		pending->cutoff_admission_seq == selected->cutoff_admission_seq &&
		pending->checkpoint.len == selected->checkpoint.len &&
		pending->checkpoint.crc == selected->checkpoint.crc &&
		pending->tail.len == selected->tail.len &&
		pending->tail.crc == selected->tail.crc;
}

static int
fork_meta_snapshot_maintenance(void)
{
	PsPruneFence cutoff;
	ForkMetaByteVec checkpoint = {0}, tail = {0}, source = {0};
	PsForkmetaSnapshotInput cp, tl;
	PsForkmetaSnapshotPrepared prepared;
	uint64_t generation;
	uint64_t freeze_seq;
	int force_deleting;
	int filter_deleting;
	int rc = 0;

	if (fork_meta_snapshot_gc_pending)
	{
		int gc;

		if (!fork_meta_snapshot_retry_due())
			return 0;
		gc = ps_forkmeta_snapshot_gc(fork_meta_snapshot_dir);

		if (gc >= 0)
		{
			/* The unlink set and its directory fsync are complete.  A crash
			 * here must reopen the selected generation with no dependence on
			 * the retired generation. */
			if (gc > 0 || fork_meta_snapshot_gc_ambiguous)
				(void) ps_fault_probe(PS_FAULT_POINT_FORKMETA_AFTER_SNAPSHOT_GC);
			fork_meta_snapshot_gc_pending = 0;
			fork_meta_snapshot_gc_ambiguous = 0;
			memset(&fork_meta_snapshot_retry_at, 0,
				   sizeof(fork_meta_snapshot_retry_at));
			/* Let the outer maintenance cycle refresh backpressure before it
			 * considers another forced snapshot. */
			return 1;
		}
		if (gc < 0)
		{
			if (gc == PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS)
				fork_meta_snapshot_gc_ambiguous = 1;
			clock_gettime(CLOCK_MONOTONIC, &fork_meta_snapshot_retry_at);
			fork_meta_snapshot_retry_at.tv_sec++;
			return 0;
		}
	}
	force_deleting = fork_meta_deletion_cutover_due_locked();
	filter_deleting = fork_meta_deletion_records_present_locked();
	/* A restart after a completed filtered cutover has DELETING lifecycle state
	 * but no matching fork entries.  Record that one locked probe as complete
	 * instead of publishing an identical generation. */
	if (!filter_deleting && fork_meta_deletion_probe_due())
		fork_meta_mark_deletion_cutover_done_locked();
	if ((!fork_meta_snapshot_due_locked() && !force_deleting) ||
		fork_meta_snapshot_cutoff(&cutoff, filter_deleting,
							  force_deleting) != 0)
	{
		if (fork_meta_bytes_load() != 0)
		{
			clock_gettime(CLOCK_MONOTONIC, &fork_meta_snapshot_retry_at);
			fork_meta_snapshot_retry_at.tv_sec++;
		}
		return 0;
	}
	{
		PsForkmetaSnapshotPrepared pending;
		int pending_rc = ps_forkmeta_snapshot_read_prepared(
			fork_meta_snapshot_dir, &pending);

		if (pending_rc < 0)
			goto retry;
		if (pending_rc == 1)
		{
			if (fork_meta_snapshot_generation != 0)
			{
				PsForkmetaSnapshot selected;

				if (ps_forkmeta_snapshot_open(&selected,
									 fork_meta_snapshot_dir) != 0)
					goto retry;
				if (fork_meta_prepared_matches_selected(&pending, &selected))
				{
					ps_forkmeta_snapshot_close(&selected);
					if (ps_forkmeta_snapshot_commit(&pending) != 0)
						goto retry;
				}
				else
				{
					ps_forkmeta_snapshot_close(&selected);
					if (ps_forkmeta_snapshot_abort(&pending) != 0)
						goto retry;
				}
			}
			else if (ps_forkmeta_snapshot_abort(&pending) != 0)
				goto retry;
		}
		if (ps_forkmeta_snapshot_next_generation(fork_meta_snapshot_dir,
										 fork_meta_snapshot_generation, &generation) != 0)
			goto retry;
	}
	/* A legacy-only source has no admission sequence to observe during replay.
	 * The deletion fallback cutoff is nevertheless (1,1), so advance the
	 * allocator through that selected position before freezing the snapshot. */
	if (force_deleting)
		admission_seq_observe(cutoff.admission_seq);
	freeze_seq = __atomic_load_n(&next_admission_seq, __ATOMIC_ACQUIRE);
	if (freeze_seq <= 1)
		goto retry;
	freeze_seq--;
	if (fork_meta_snapshot_build(&checkpoint, &tail, &source, cutoff,
								 generation, freeze_seq, filter_deleting,
								 force_deleting) != 0)
		goto retry_done;
	cp.data = checkpoint.data;
	cp.len = checkpoint.len;
	cp.produce = NULL;
	cp.produce_arg = NULL;
	tl.data = tail.data;
	tl.len = tail.len;
	tl.produce = NULL;
	tl.produce_arg = NULL;
	if (ps_forkmeta_snapshot_prepare(&prepared, fork_meta_snapshot_dir,
								 generation, cutoff.lsn, cutoff.admission_seq,
								 &cp, &tl) != 0)
		goto retry_done;
	/* Prepare has fsynced both immutable parts and the durable intent.  The
	 * manifest is still the selected authority at this boundary. */
	(void) ps_fault_probe(PS_FAULT_POINT_FORKMETA_AFTER_PREPARE);
	if (ps_forkmeta_snapshot_commit(&prepared) != 0)
	{
		fork_meta_poisoned_store(1);
		goto done;
	}
	/* The manifest replacement and its directory fsync are durable.  The
	 * source epoch still names the pre-cutover log at this point. */
	(void) ps_fault_probe(PS_FAULT_POINT_FORKMETA_AFTER_MANIFEST_COMMIT);
	/* The manifest is now authoritative.  Any failure here poisons mutation;
	 * continuing to append to the old epoch would make source identity unknown. */
	if (ps_storage->fork_meta_rewrite == NULL ||
		ps_storage->fork_meta_rewrite(source.data, (uint32_t) source.len) != 0)
	{
		fork_meta_poisoned_store(1);
		goto done;
	}
	/* fork_meta_rewrite returns only after the replacement file and containing
	 * directory have been fsynced. */
	(void) ps_fault_probe(PS_FAULT_POINT_FORKMETA_AFTER_SOURCE_REWRITE);
	fork_meta_bytes_store(source.len);
	fork_meta_reclaim_baseline_bytes = source.len;
	fork_meta_reclaim_baseline_valid = 1;
	fork_meta_snapshot_generation = generation;
	fork_meta_snapshot_cutoff_lsn = cutoff.lsn;
	fork_meta_snapshot_cutoff_seq = cutoff.admission_seq;
	fork_meta_snapshot_freeze_seq = freeze_seq;
	fork_meta_snapshot_bytes = checkpoint.len + tail.len;
	fork_meta_snapshot_checkpoint_meta = prepared.checkpoint;
	fork_meta_snapshot_tail_meta = prepared.tail;
	fork_meta_snapshot_gc_pending = 1;
	memset(&fork_meta_snapshot_retry_at, 0, sizeof(fork_meta_snapshot_retry_at));
	if (filter_deleting)
		fork_meta_mark_deletion_cutover_done_locked();
	/* Keep the in-memory chain conservative until the next restart.  All
	 * acknowledged events remain available to readers; the durable checkpoint
	 * is the source of truth for the next boot. */
	rc = 1;

done:
	free(checkpoint.data);
	free(tail.data);
	free(source.data);
	return rc;

retry_done:
	clock_gettime(CLOCK_MONOTONIC, &fork_meta_snapshot_retry_at);
	fork_meta_snapshot_retry_at.tv_sec++;
	goto done;

retry:
	clock_gettime(CLOCK_MONOTONIC, &fork_meta_snapshot_retry_at);
	fork_meta_snapshot_retry_at.tv_sec++;
	return 0;
}

static int
load_timelines(void)
{
	uint64_t off = 0;
	uint32_t header[2];
	uint32_t magic;
	int n;

	/* The first record selects the grammar for the entire file.  Legacy is a
	 * fixed-record migration input; a TLM2 file is V2/event mixed only. */
	n = ps_storage->meta_read(0, header, sizeof(header));
	if (n < 0 && errno == ENOENT)
		return 0;
	if (n == 0)
		return 0;
	if (n != (int) sizeof(header))
	{
		if (n > 0 && ps_storage->meta_truncate &&
			ps_storage->meta_truncate(0) == 0)
			return 0;
		return -1;
	}
	memcpy(&magic, &header[0], sizeof(magic));

	if (magic == TIMELINE_META_V2_MAGIC)
	{
		for (;;)
		{
			uint32_t rec_len;

			n = ps_storage->meta_read(off, header, sizeof(header));
			if (n == 0)
				break;
			if (n != (int) sizeof(header))
			{
				if (n > 0 && ps_storage->meta_truncate &&
					ps_storage->meta_truncate(off) == 0)
					break;
				return -1;
			}
			memcpy(&magic, &header[0], sizeof(magic));
			memcpy(&rec_len, &header[1], sizeof(rec_len));
			if (magic != TIMELINE_META_V2_MAGIC ||
				(rec_len != sizeof(TimelineRecV2) &&
				 rec_len != sizeof(TimelineRecEventV1) &&
				 rec_len != sizeof(TimelineRecEvent)))
				return -1;
			if (rec_len == sizeof(TimelineRecV2))
			{
				TimelineRecV2 rec;

				n = ps_storage->meta_read(off, &rec, sizeof(rec));
				if (n != (int) sizeof(rec))
				{
					if (n >= 0 && ps_storage->meta_truncate &&
						ps_storage->meta_truncate(off) == 0)
						break;
					return -1;
				}
				if (rec.magic != TIMELINE_META_V2_MAGIC ||
					rec.rec_len != sizeof(rec) || rec.reserved != 0 ||
					rec.crc != timeline_rec_crc(&rec) ||
					rec.id >= MAX_TIMELINES || timelines[rec.id].defined ||
					!branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
					return -1;
				timeline_define(rec.id, rec.parent, rec.branch_lsn);
				off += sizeof(rec);
			}
			else if (rec_len == sizeof(TimelineRecEventV1))
			{
				TimelineRecEventV1 rec;

				n = ps_storage->meta_read(off, &rec, sizeof(rec));
				if (n != (int) sizeof(rec))
				{
					if (n >= 0 && ps_storage->meta_truncate &&
						ps_storage->meta_truncate(off) == 0)
						break;
					return -1;
				}
				if (rec.magic != TIMELINE_META_V2_MAGIC ||
					rec.rec_len != sizeof(rec) || rec.reserved != 0 ||
					rec.crc != timeline_event_v1_crc(&rec) ||
					rec.id >= MAX_TIMELINES || rec.incarnation == 0 ||
					rec.state < PS_TIMELINE_LIVE ||
					rec.state > PS_TIMELINE_DELETED)
					return -1;
				if (rec.kind == TIMELINE_META_EVENT_CREATE)
				{
					uint64_t parent_incarnation;

					/* Old events predate reusable-parent fencing, so they can
					 * only inherit the parent's incarnation at replay position. */
					if (rec.state != PS_TIMELINE_LIVE)
						return -1;
					if (rec.parent < 0 || rec.parent >= MAX_TIMELINES ||
						!timelines[rec.parent].defined)
						return -1;
					parent_incarnation = __atomic_load_n(
						&timelines[rec.parent].incarnation, __ATOMIC_ACQUIRE);
					if (!branch_parent_token_ok(rec.parent, parent_incarnation))
						return -1;
					if (!timelines[rec.id].defined)
					{
						if (rec.incarnation != 1 ||
							!branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
							return -1;
					}
					else if (__atomic_load_n(&timelines[rec.id].state,
											 __ATOMIC_ACQUIRE) == PS_TIMELINE_DELETED)
					{
						uint64_t old_incarnation =
							__atomic_load_n(&timelines[rec.id].incarnation,
											__ATOMIC_ACQUIRE);

						if (old_incarnation == UINT64_MAX ||
							rec.incarnation != old_incarnation + 1 ||
							!branch_parent_chain_ok(rec.id, rec.parent))
							return -1;
					}
					else
						return -1;
					timeline_define_incarnation(rec.id, rec.parent,
											rec.branch_lsn, rec.incarnation,
											parent_incarnation);
				}
				else if (rec.kind == TIMELINE_META_EVENT_STATE)
				{
					uint32_t old_state;

					if (!timelines[rec.id].defined ||
						timelines[rec.id].parent != rec.parent ||
						timelines[rec.id].branch_lsn != rec.branch_lsn ||
						__atomic_load_n(&timelines[rec.id].incarnation,
											 __ATOMIC_ACQUIRE) != rec.incarnation)
						return -1;
					old_state = __atomic_load_n(&timelines[rec.id].state,
											__ATOMIC_ACQUIRE);
					if (!((old_state == PS_TIMELINE_LIVE &&
							 rec.state == PS_TIMELINE_DELETING) ||
							(old_state == PS_TIMELINE_DELETING &&
							 (rec.state == PS_TIMELINE_DELETING ||
							  rec.state == PS_TIMELINE_DELETED))))
						return -1;
					__atomic_store_n(&timelines[rec.id].state, rec.state,
											 __ATOMIC_RELEASE);
				}
				else
					return -1;
				off += sizeof(rec);
			}
			else
			{
				TimelineRecEvent rec;
				n = ps_storage->meta_read(off, &rec, sizeof(rec));
				if (n != (int) sizeof(rec))
				{
					if (n >= 0 && ps_storage->meta_truncate &&
						ps_storage->meta_truncate(off) == 0)
						break;
					return -1;
				}
				if (rec.magic != TIMELINE_META_V2_MAGIC ||
					rec.rec_len != sizeof(rec) || rec.reserved != 0 ||
					rec.crc != timeline_event_crc(&rec) ||
					rec.id >= MAX_TIMELINES || rec.incarnation == 0 ||
					rec.state < PS_TIMELINE_LIVE ||
					rec.state > PS_TIMELINE_DELETED)
					return -1;
				if (rec.kind == TIMELINE_META_EVENT_CREATE)
				{
					if (rec.state != PS_TIMELINE_LIVE ||
						rec.incarnation == 0 || rec.parent_incarnation == 0)
						return -1;
					if (!timelines[rec.id].defined)
					{
						if (rec.incarnation != 1 ||
							!branch_parent_token_ok(rec.parent,
												 rec.parent_incarnation) ||
							!branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
							return -1;
						timeline_define_incarnation(rec.id, rec.parent,
											rec.branch_lsn, rec.incarnation,
											rec.parent_incarnation);
					}
					else if (__atomic_load_n(&timelines[rec.id].state,
													__ATOMIC_ACQUIRE) == PS_TIMELINE_DELETED)
					{
						uint64_t old_incarnation =
							__atomic_load_n(&timelines[rec.id].incarnation,
													__ATOMIC_ACQUIRE);

						if (old_incarnation == UINT64_MAX ||
							rec.incarnation != old_incarnation + 1 ||
							!branch_parent_token_ok(rec.parent,
												 rec.parent_incarnation))
							return -1;
						if (!branch_parent_chain_ok(rec.id, rec.parent))
							return -1;
						timeline_define_incarnation(rec.id, rec.parent,
											rec.branch_lsn, rec.incarnation,
											rec.parent_incarnation);
					}
					else
						return -1;
				}
				else if (rec.kind == TIMELINE_META_EVENT_STATE)
				{
					uint32_t old_state;

					if (!timelines[rec.id].defined ||
						timelines[rec.id].parent != rec.parent ||
						timelines[rec.id].branch_lsn != rec.branch_lsn ||
						timelines[rec.id].parent_incarnation !=
							rec.parent_incarnation ||
						__atomic_load_n(&timelines[rec.id].incarnation,
																							__ATOMIC_ACQUIRE) != rec.incarnation)
						return -1;
					old_state = __atomic_load_n(&timelines[rec.id].state,
																								__ATOMIC_ACQUIRE);
					if (!((old_state == PS_TIMELINE_LIVE &&
								 rec.state == PS_TIMELINE_DELETING) ||
							(old_state == PS_TIMELINE_DELETING &&
							 (rec.state == PS_TIMELINE_DELETING ||
							  rec.state == PS_TIMELINE_DELETED))))
						return -1;
					__atomic_store_n(&timelines[rec.id].state, rec.state,
																								__ATOMIC_RELEASE);
				}
				else
					return -1;
				off += sizeof(rec);
			}
		}
		return 0;
	}

	/* A non-TLM2 first record must be a complete legacy-only file. */
	{
		TimelineRec legacy[MAX_TIMELINES];
		uint32_t nlegacy = 0;

		for (;;)
		{
			TimelineRec rec;

			n = ps_storage->meta_read(off, &rec, sizeof(rec));
			if (n == 0)
				break;
			if (n != (int) sizeof(rec))
			{
				if (n > 0 && ps_storage->meta_truncate &&
					ps_storage->meta_truncate(off) == 0)
					break;
				return -1;
			}
			if (nlegacy == MAX_TIMELINES || rec.id >= MAX_TIMELINES ||
				timelines[rec.id].defined ||
				!branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
				return -1;
			legacy[nlegacy++] = rec;
			timeline_define(rec.id, rec.parent, rec.branch_lsn);
			off += sizeof(rec);
		}
		if (nlegacy != 0)
		{
			TimelineRecV2 out[MAX_TIMELINES];

			for (uint32_t i = 0; i < nlegacy; i++)
			{
				memset(&out[i], 0, sizeof(out[i]));
				out[i].magic = TIMELINE_META_V2_MAGIC;
				out[i].rec_len = sizeof(out[i]);
				out[i].id = legacy[i].id;
				out[i].parent = legacy[i].parent;
				out[i].branch_lsn = legacy[i].branch_lsn;
				out[i].crc = timeline_rec_crc(&out[i]);
			}
			if (!ps_storage->meta_rewrite ||
				ps_storage->meta_rewrite(out, nlegacy * sizeof(out[0])) != 0)
				return -1;
		}
	}
	return 0;
}

/* ===================== shipped WAL log (per timeline) ================== */

static void publish_wal_index_metrics(void);

/*
 * Each timeline has an append-only WAL log "wal_<tl>" of self-describing
 * records [WalRecHdr | bytes].  This is the durability/transport half of WAL
 * shipping: the compute ships its WAL stream here so it is persisted by the
 * store, per timeline.  (Replaying these records to materialize pages -- redo
 * -- is a later milestone; it would reuse PostgreSQL's rmgr redo.)
 */
#define WAL_MAGIC	0x57414c52	/* "WALR" */

typedef struct WalRecHdr
{
	uint32_t	magic;
	uint32_t	len;			/* WAL bytes following the header */
	uint64_t	start_lsn;		/* LSN of the first byte */
} WalRecHdr;

typedef struct WalChunkRef
{
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	uint64_t	payload_off;
} WalChunkRef;

static WalChunkRef *wal_chunks[MAX_TIMELINES];
static uint32_t wal_chunks_n[MAX_TIMELINES];
static uint32_t wal_chunks_cap[MAX_TIMELINES];
static uint64_t wal_log_bytes[MAX_TIMELINES];
#define WAL_IMMUTABLE_SEGMENT_BYTES PS_WAL_SEGMENT_MIN_BYTES
static struct timespec wal_reclaim_retry_at[MAX_TIMELINES];
static uint32_t wal_reclaim_cursor;

static void walidx_progress_init(uint32_t tl, uint64_t first_lsn);
static int wal_segment_sync(uint32_t tl);
static int wal_flat_reclaim(uint32_t tl);

static int
wal_chunk_reserve(uint32_t tl)
{
	WalChunkRef *grown;
	uint32_t	newcap;

	if (wal_chunks_n[tl] < wal_chunks_cap[tl])
		return 0;
	newcap = wal_chunks_cap[tl] ? wal_chunks_cap[tl] * 2 : 64;
	grown = realloc(wal_chunks[tl], (size_t) newcap * sizeof(*grown));
	if (!grown)
		return -1;
	wal_chunks[tl] = grown;
	wal_chunks_cap[tl] = newcap;
	return 0;
}

static void
wal_chunk_add(uint32_t tl, uint64_t record_off, const WalRecHdr *h)
{
	WalChunkRef *ref = &wal_chunks[tl][wal_chunks_n[tl]++];

	ref->start_lsn = h->start_lsn;
	ref->end_lsn = h->start_lsn + h->len;
	ref->payload_off = record_off + sizeof(*h);
	wal_log_bytes[tl] = ref->payload_off + h->len;
	wal_start_observe(tl, h->start_lsn);
}

static uint32_t
wal_chunk_lower_bound(uint32_t tl, uint64_t lsn)
{
	uint32_t	lo = 0;
	uint32_t	hi = wal_chunks_n[tl];

	/* First chunk whose end is after lsn. */
	while (lo < hi)
	{
		uint32_t	mid = lo + (hi - lo) / 2;

		if (wal_chunks[tl][mid].end_lsn <= lsn)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

/*
 * Overlap policy for shipped WAL: identical bytes are an idempotent re-ship
 * (an archiver retry after a partial failure) -- accepted, and skipped
 * entirely when the range is already fully covered, so no duplicate chunk
 * inflates the log or the distinct-byte accounting.  DIFFERING bytes for an
 * already-covered position are two histories claiming the same LSN range --
 * a divergent compute (a pinned reader's private WAL shipped after
 * unpinning, or a second writer on the timeline) trying to overwrite the
 * recorded one -- and are refused: later-chunks-win read semantics would
 * otherwise let the divergent copy silently rewrite history.  Returns -1 on
 * divergence or read failure; otherwise 0 with *covered_prefix set to the
 * length of the already-covered prefix, and *prefix_only set when the
 * coverage is exactly that prefix (no covered bytes beyond it) -- the shape
 * a retry of a partially shipped chunk has, and the only shape the caller
 * can trim to a clean uncovered suffix.
 */
static int
wal_overlap_check(uint32_t tl, uint64_t start_lsn,
				  const unsigned char *data, uint32_t len,
				  uint32_t *covered_prefix, int *prefix_only)
{
	uint64_t	we = start_lsn + len;
	unsigned char *tmp = NULL;
	unsigned char *mask = NULL;
	uint32_t	covered = 0;

	*covered_prefix = 0;
	*prefix_only = 1;
	/* A reclaimed flat prefix remains authoritative in immutable segments.
	 * Include it in retry/divergence detection before consulting the tail map. */
	if (wal_segment_store_opened[tl])
	{
		PsWalStore *store = &wal_segment_stores[tl];
		uint64_t os = start_lsn > store->start_lsn ?
			start_lsn : store->start_lsn;
		uint64_t oe = we < store->end_lsn ? we : store->end_lsn;

		if (os < oe)
		{
			uint32_t n = (uint32_t) (oe - os);
			uint32_t base = (uint32_t) (os - start_lsn);

			tmp = malloc(len);
			mask = calloc(1, len);
			if (!tmp || !mask ||
				ps_wal_store_read(store, os, tmp, n) != 0 ||
				memcmp(tmp, data + base, n) != 0)
			{
				fprintf(stderr, "pagestore: refusing divergent WAL overlap on "
						"timeline %u at %llu (+%u): bytes differ from the "
						"immutable shipped history\n",
						tl, (unsigned long long) os, n);
				free(tmp);
				free(mask);
				return -1;
			}
			for (uint32_t j = 0; j < n; j++)
			{
				mask[base + j] = 1;
				covered++;
			}
		}
	}
	for (uint32_t i = wal_chunk_lower_bound(tl, start_lsn);
		 i < wal_chunks_n[tl] && wal_chunks[tl][i].start_lsn < we; i++)
	{
		WalChunkRef *ref = &wal_chunks[tl][i];
		uint64_t	rs = ref->start_lsn;
		uint64_t	re = ref->end_lsn;
		uint64_t	os = rs > start_lsn ? rs : start_lsn;
		uint64_t	oe = re < we ? re : we;

		if (os < oe)
		{
			uint64_t	src = ref->payload_off + (os - rs);
			uint32_t	n = (uint32_t) (oe - os);
			uint32_t	base = (uint32_t) (os - start_lsn);

			if (!tmp)
			{
				tmp = malloc(len);
				mask = calloc(1, len);
				if (!tmp || !mask)
				{
					free(tmp);
					free(mask);
					return -1;
				}
			}
			if (ps_storage->wal_read(tl, src, tmp, n) != (int) n ||
				memcmp(tmp, data + base, n) != 0)
			{
				fprintf(stderr, "pagestore: refusing divergent WAL overlap on "
						"timeline %u at %llu (+%u): bytes differ from the "
						"already-shipped history\n",
						tl, (unsigned long long) os, n);
				free(tmp);
				free(mask);
				return -1;
			}
			for (uint32_t j = 0; j < n; j++)
				if (!mask[base + j])
				{
					mask[base + j] = 1;
					covered++;
				}
		}
	}
	if (mask)
	{
		uint32_t	i = 0;

		while (i < len && mask[i])
			i++;
		*covered_prefix = i;
		*prefix_only = (covered == i);
	}
	free(tmp);
	free(mask);
	return 0;
}

static int
wal_append_locked(uint32_t tl, uint64_t start_lsn,
				  const unsigned char *data, uint32_t len)
{
	WalRecHdr	h;

	if (tl >= MAX_TIMELINES ||
		!wal_reclaim_frontier_one_allows(tl, start_lsn))
		return -1;

	/*
	 * Appends normally land strictly at/after the shipped end; only a
	 * re-ship (or a divergent history) reaches back below it, so the
	 * byte-compare scan runs only then.  An identical re-ship is trimmed
	 * to its uncovered suffix (fully covered = nothing to do), so no
	 * duplicate chunk ever lands and the distinct-byte accounting the read
	 * paths rely on stays exact.  Coverage that is not a clean prefix has
	 * no trimmable shape; the contiguous log never produces it, so refuse
	 * rather than distort the counts.
	 */
	if (len > 0 && start_lsn < wal_end_read(tl))
	{
		uint32_t	covered_prefix;
		int			prefix_only;

		if (wal_overlap_check(tl, start_lsn, data, len,
							  &covered_prefix, &prefix_only) != 0)
			return -1;
		if (covered_prefix == len)
			return wal_segment_sync(tl);
		if (!prefix_only)
		{
			fprintf(stderr, "pagestore: refusing WAL re-ship with non-prefix "
					"overlap on timeline %u at %llu (+%u)\n",
					tl, (unsigned long long) start_lsn, len);
			return -1;
		}
		start_lsn += covered_prefix;
		data += covered_prefix;
		len -= covered_prefix;
	}

	timeline_mark_used(tl);
	h.magic = WAL_MAGIC;
	h.len = len;
	h.start_lsn = start_lsn;
	if (wal_chunk_reserve(tl) != 0)
		return -1;
	if (ps_storage->wal_append(tl, &h, sizeof(h), data, len) != 0)
		return -1;

	wal_chunk_add(tl, wal_log_bytes[tl], &h);
	wal_end_advance(tl, start_lsn + len);
	if (len > 0)
		walidx_progress_init(tl, start_lsn);
	publish_wal_index_metrics();
	return wal_segment_sync(tl);
}

static int
wal_append(uint32_t tl, uint64_t start_lsn, const unsigned char *data,
		   uint32_t len)
{
	pthread_rwlock_t *lock = wal_log_lock_for(tl);
	int rc;

	if (lock == NULL)
		return -1;
	pthread_rwlock_wrlock(lock);
	rc = wal_append_locked(tl, start_lsn, data, len);
	pthread_rwlock_unlock(lock);
	return rc;
}

/* Fill 'out' from ONE timeline's log: the overlap of [start, start+len) with
 * [.., cap) and with each shipped chunk.  Bytes not covered are left as-is. */
static int64_t
wal_read_flat_one(uint32_t tl, uint64_t start, uint32_t len, uint64_t cap,
				  unsigned char *out)
{
	uint32_t	filled = 0;
	uint64_t	we = start + len;

	if (we > cap)
		we = cap;
	if (we <= start)
		return 0;

	for (uint32_t i = wal_chunk_lower_bound(tl, start);
		 i < wal_chunks_n[tl] && wal_chunks[tl][i].start_lsn < we; i++)
	{
		WalChunkRef *ref = &wal_chunks[tl][i];
		uint64_t	rs = ref->start_lsn;
		uint64_t	re = ref->end_lsn;
		uint64_t	os = rs > start ? rs : start;	/* overlap start */
		uint64_t	oe = re < we ? re : we; /* overlap end */

		if (os < oe)
		{
			uint64_t	src = ref->payload_off + (os - rs);
			int			n = ps_storage->wal_read(tl, src, out + (os - start),
												 (uint32_t) (oe - os));

			if (n < 0)
				return -1;
			if (n > 0)
				filled += (uint32_t) n;
		}
	}
	return filled;
}

/* Prefer validated immutable segments for their sealed prefix.  The flat log
 * remains the authoritative staging/tail representation until prefix
 * reclamation publishes a durable retained base. */
static int64_t
wal_read_one(uint32_t tl, uint64_t start, uint32_t len, uint64_t cap,
			 unsigned char *out)
{
	uint64_t end = start + len;
	uint64_t sealed_start;
	uint64_t sealed_end;
	uint32_t filled = 0;

	if (end < start)
		return 0;
	if (end > cap)
		end = cap;
	if (end <= start)
		return 0;
	if (tl >= MAX_TIMELINES || !wal_segment_store_opened[tl])
		return wal_read_flat_one(tl, start, (uint32_t) (end - start), end, out);
	sealed_start = wal_segment_stores[tl].start_lsn;
	sealed_end = wal_segment_stores[tl].end_lsn;
	if (start < sealed_start)
	{
		uint64_t left_end = end < sealed_start ? end : sealed_start;
		int64_t n;

		n = wal_read_flat_one(tl, start,
						  (uint32_t) (left_end - start), left_end, out);
		if (n < 0)
			return -1;
		filled += (uint32_t) n;
	}
	if (start < sealed_end && end > sealed_start)
	{
		uint64_t segment_start = start > sealed_start ? start : sealed_start;
		uint64_t segment_end = end < sealed_end ? end : sealed_end;

		if (ps_wal_store_read(&wal_segment_stores[tl], segment_start,
						  out + (segment_start - start),
						  (uint32_t) (segment_end - segment_start)) != 0)
			return -1;
		filled += (uint32_t) (segment_end - segment_start);
	}
	if (end > sealed_end)
	{
		uint64_t tail_start = start > sealed_end ? start : sealed_end;
		int64_t n;

		n = wal_read_flat_one(tl, tail_start,
						  (uint32_t) (end - tail_start), end,
						  out + (tail_start - start));
		if (n < 0)
			return -1;
		filled += (uint32_t) n;
	}
	return filled;
}

/* The LSN where a timeline's shipped log begins (its first chunk's start),
 * or UINT64_MAX for an empty log.  The log is contiguous from there: the
 * archiver ships completed segments strictly in order. */
static uint64_t
wal_log_start(uint32_t tl)
{
	if (tl < MAX_TIMELINES &&
		__atomic_load_n(&wal_start_valid[tl], __ATOMIC_ACQUIRE))
		return __atomic_load_n(&wal_start[tl], __ATOMIC_RELAXED);
	return UINT64_MAX;
}

static int
wal_payload_readable(uint32_t tl, uint64_t payload_off, uint32_t len)
{
	unsigned char byte;
	uint64_t	last;

	if (len == 0)
		return 1;
	last = payload_off + len - 1;
	if (last < payload_off)
		return 0;
	return ps_storage->wal_read(tl, last, &byte, 1) == 1;
}

static int
wal_coverage_advance(uint32_t tl, uint64_t start_lsn, uint64_t end_lsn)
{
	uint64_t	covered;
	uint64_t	off;
	int		immutable_prefix = 0;

	if (tl >= MAX_TIMELINES)
		return 0;
	if (wal_segment_store_opened[tl] &&
		start_lsn < wal_segment_stores[tl].start_lsn)
	{
		/* A durable retained base proves that the removed prefix was already
		 * covered by the WAL-index/snapshot contract.  Progress replay may still
		 * contain a marker whose range starts below that base; validate the
		 * surviving suffix against the immutable store instead of consulting the
		 * intentionally reclaimed flat log. */
		if (end_lsn <= wal_segment_stores[tl].start_lsn)
			return 1;
		start_lsn = wal_segment_stores[tl].start_lsn;
		immutable_prefix = 1;
	}
	if (wal_segment_store_opened[tl] &&
		start_lsn >= wal_segment_stores[tl].start_lsn &&
		start_lsn <= wal_segment_stores[tl].end_lsn)
	{
		if (end_lsn <= wal_segment_stores[tl].end_lsn)
			return 1;
		start_lsn = wal_segment_stores[tl].end_lsn;
		immutable_prefix = 1;
	}
	if (!wal_covered_valid[tl])
	{
		WalRecHdr	h;

		if (ps_storage->wal_read(tl, 0, &h, sizeof(h)) != (int) sizeof(h) ||
			h.magic != WAL_MAGIC)
			return start_lsn == end_lsn;
		if (immutable_prefix && h.start_lsn > start_lsn)
			return 0;
		wal_covered[tl] = h.start_lsn;
		wal_covered_off[tl] = 0;
		wal_covered_valid[tl] = 1;
	}
	covered = wal_covered[tl];
	off = wal_covered_off[tl];
	/* A crossing flat record may begin before the immutable end.  Its header is
	 * retained, while the immutable prefix proves continuity up to start_lsn. */
	if (immutable_prefix && covered <= start_lsn)
	{
		covered = start_lsn;
		wal_covered[tl] = covered;
	}
	if (start_lsn > covered)
		return 0;
	while (covered < end_lsn)
	{
		WalRecHdr	h;
		int			n;
		int			advanced = 0;

		while ((n = ps_storage->wal_read(tl, off, &h, sizeof(h))) ==
			   (int) sizeof(h))
		{
			uint64_t	payload_off;
			uint64_t	rec_end;
			uint64_t	next_off;

			if (h.magic != WAL_MAGIC)
				return 0;
			payload_off = off + sizeof(h);
			next_off = payload_off + h.len;
			if (payload_off < off || next_off < payload_off)
				return 0;
			rec_end = h.start_lsn + h.len;
			if (rec_end < h.start_lsn)
				return 0;
			if (!wal_payload_readable(tl, payload_off, h.len))
				return 0;
			off = next_off;
			if (h.start_lsn <= covered)
			{
				if (rec_end > covered)
					covered = rec_end;
				advanced = 1;
				wal_covered[tl] = covered;
				wal_covered_off[tl] = off;
				if (covered >= end_lsn)
					return 1;
			}
			else
			{
				wal_covered[tl] = covered;
				wal_covered_off[tl] = off - sizeof(h) - h.len;
				return 0;
			}
		}
		if (n < 0 || !advanced)
			return 0;
	}
	return 1;
}

static int
wal_segment_open_one(uint32_t tl)
{
	char path[4096];
	unsigned char *segment_buf = NULL;
	unsigned char *flat_buf = NULL;
	uint64_t flat_start;
	uint64_t compare_start;
	struct stat st;
	int n;

	if (tl >= MAX_TIMELINES || wal_segment_store_opened[tl])
		return tl < MAX_TIMELINES ? 0 : -1;
	flat_start = wal_chunks_n[tl] == 0 ? UINT64_MAX :
		wal_chunks[tl][0].start_lsn;
	n = snprintf(path, sizeof(path), "%s/wal_segments_%u",
				 wal_segment_root, tl);
	if (n < 0 || (size_t) n >= sizeof(path))
		return -1;
	if (lstat(path, &st) == 0)
	{
		if (!S_ISDIR(st.st_mode) ||
			(ps_wal_store_open_existing(&wal_segment_stores[tl], path, tl + 1,
									WAL_IMMUTABLE_SEGMENT_BYTES) != 0 &&
			 (flat_start == UINT64_MAX ||
			  flat_start % WAL_IMMUTABLE_SEGMENT_BYTES != 0 ||
			  ps_wal_store_open(&wal_segment_stores[tl], path, tl + 1,
							 flat_start, WAL_IMMUTABLE_SEGMENT_BYTES) != 0)))
			return -1;
	}
	else if (errno == ENOENT)
	{
		if (flat_start == UINT64_MAX ||
			flat_start % WAL_IMMUTABLE_SEGMENT_BYTES != 0)
			return 0;
		if (ps_wal_store_create(&wal_segment_stores[tl], path, tl + 1,
								flat_start, WAL_IMMUTABLE_SEGMENT_BYTES) != 0)
			return -1;
	}
	else
		return -1;
	/* The flat log is still authoritative during this migration.  A valid but
	 * divergent immutable file must never silently replace its bytes. */
	segment_buf = malloc(PS_WAL_STORE_VERIFY_CHUNK_BYTES);
	flat_buf = malloc(PS_WAL_STORE_VERIFY_CHUNK_BYTES);
	if (segment_buf == NULL || flat_buf == NULL)
		goto fail;
	compare_start = flat_start > wal_segment_stores[tl].start_lsn ?
		flat_start : wal_segment_stores[tl].start_lsn;
	for (uint64_t pos = compare_start;
		 pos < wal_segment_stores[tl].end_lsn;)
	{
		uint32_t amount = wal_segment_stores[tl].end_lsn - pos <
			PS_WAL_STORE_VERIFY_CHUNK_BYTES ?
			(uint32_t) (wal_segment_stores[tl].end_lsn - pos) :
			PS_WAL_STORE_VERIFY_CHUNK_BYTES;

		if (!wal_coverage_advance(tl, pos, pos + amount) ||
			ps_wal_store_read(&wal_segment_stores[tl], pos,
						  segment_buf, amount) != 0 ||
			wal_read_flat_one(tl, pos, amount, pos + amount,
							  flat_buf) != amount ||
			memcmp(segment_buf, flat_buf, amount) != 0)
			goto fail;
		pos += amount;
	}
	free(segment_buf);
	free(flat_buf);
	wal_segment_store_opened[tl] = 1;
	if (wal_segment_stores[tl].nentries != 0)
		timeline_mark_used(tl);
	wal_start_observe(tl, wal_segment_stores[tl].start_lsn);
	wal_end_advance(tl, wal_segment_stores[tl].end_lsn);
	return 0;

fail:
	free(segment_buf);
	free(flat_buf);
	ps_wal_store_close(&wal_segment_stores[tl]);
	return -1;
}

/* Immutable WAL can outlive both its reclaimed flat prefix and branch
 * metadata (for example, an archiver may have shipped a timeline before its
 * branch declaration arrives).  Discover canonical segment directories before
 * selecting timelines for recovery so those ids remain occupied after restart. */
static int
wal_segment_discover_used(void)
{
	const char prefix[] = "wal_segments_";
	struct dirent *de;
	DIR *dir = NULL;
	int root_fd = -1;
	int scan_fd = -1;
	int rc = -1;

	root_fd = open(wal_segment_root,
				   O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (root_fd < 0 || (scan_fd = dup(root_fd)) < 0 ||
		(dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	errno = 0;
	while ((de = readdir(dir)) != NULL)
	{
		char expected[64];
		char *end = NULL;
		unsigned long parsed;
		struct stat st;
		int n;

		if (strncmp(de->d_name, prefix, sizeof(prefix) - 1) != 0)
			continue;
		errno = 0;
		parsed = strtoul(de->d_name + sizeof(prefix) - 1, &end, 10);
		n = snprintf(expected, sizeof(expected), "%s%lu", prefix, parsed);
		if (errno != 0 || end == de->d_name + sizeof(prefix) - 1 ||
			*end != '\0' || n < 0 || (size_t) n >= sizeof(expected) ||
			strcmp(expected, de->d_name) != 0 || parsed >= MAX_TIMELINES ||
			fstatat(root_fd, de->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0 ||
			!S_ISDIR(st.st_mode))
			goto cleanup;
		timeline_mark_used((uint32_t) parsed);
		errno = 0;
	}
	if (errno != 0 || closedir(dir) != 0)
	{
		dir = NULL;
		goto cleanup;
	}
	dir = NULL;
	rc = 0;

cleanup:
	if (dir != NULL)
		closedir(dir);
	if (scan_fd >= 0)
		close(scan_fd);
	if (root_fd >= 0)
		close(root_fd);
	return rc;
}

/* Remove only flat-log records whose complete logical range is already held
 * by the validated immutable store.  A boundary-crossing record stays whole;
 * this keeps the flat suffix self-describing for restart.  The caller holds
 * wal_log_lock exclusively (or startup is still single-threaded). */
static int
wal_flat_reclaim(uint32_t tl)
{
	uint32_t drop = 0;
	uint64_t keep_off;

	if (tl >= MAX_TIMELINES || !wal_segment_store_opened[tl] ||
		wal_segment_stores[tl].nentries == 0 ||
		ps_storage->wal_rewrite_prefix == NULL)
		return 0;
	while (drop < wal_chunks_n[tl] &&
		   wal_chunks[tl][drop].end_lsn <= wal_segment_stores[tl].end_lsn)
		drop++;
	if (drop == 0)
		return 0;
	keep_off = drop < wal_chunks_n[tl] ?
		wal_chunks[tl][drop].payload_off - sizeof(WalRecHdr) :
		wal_log_bytes[tl];
	if (keep_off == 0 || ps_storage->wal_rewrite_prefix(tl, keep_off) != 0)
		return -1;
	for (uint32_t i = drop; i < wal_chunks_n[tl]; i++)
	{
		wal_chunks[tl][i - drop] = wal_chunks[tl][i];
		wal_chunks[tl][i - drop].payload_off -= keep_off;
	}
	wal_chunks_n[tl] -= drop;
	wal_log_bytes[tl] -= keep_off;
	wal_covered[tl] = 0;
	wal_covered_off[tl] = 0;
	wal_covered_valid[tl] = 0;
	return 1;
}

static int
wal_segment_sync(uint32_t tl)
{
	unsigned char *buf;
	PsWalStore *store;
	int rc = 0;

	if (tl >= MAX_TIMELINES || wal_segment_open_one(tl) != 0)
		return -1;
	if (!wal_segment_store_opened[tl])
		return 0;
	store = &wal_segment_stores[tl];
	if (store->end_lsn > wal_end_read(tl))
		return -1;
	if (store->end_lsn > UINT64_MAX - WAL_IMMUTABLE_SEGMENT_BYTES ||
		store->end_lsn + WAL_IMMUTABLE_SEGMENT_BYTES > wal_end_read(tl))
		return wal_flat_reclaim(tl) < 0 ? -1 : 0;
	buf = malloc(WAL_IMMUTABLE_SEGMENT_BYTES);
	if (buf == NULL)
		return -1;
	while (store->end_lsn <= UINT64_MAX - WAL_IMMUTABLE_SEGMENT_BYTES &&
		   store->end_lsn + WAL_IMMUTABLE_SEGMENT_BYTES <= wal_end_read(tl) &&
		   wal_coverage_advance(tl, store->end_lsn,
							store->end_lsn + WAL_IMMUTABLE_SEGMENT_BYTES))
	{
		uint64_t segment_start = store->end_lsn;

		if (wal_read_flat_one(tl, segment_start,
							  WAL_IMMUTABLE_SEGMENT_BYTES,
							  segment_start + WAL_IMMUTABLE_SEGMENT_BYTES,
							  buf) != WAL_IMMUTABLE_SEGMENT_BYTES ||
			ps_wal_store_append(store, segment_start, buf,
							WAL_IMMUTABLE_SEGMENT_BYTES) != 0)
		{
			rc = -1;
			break;
		}
	}
	free(buf);
	if (rc == 0 && wal_flat_reclaim(tl) < 0)
		rc = -1;
	return rc;
}

/*
 * Read up to 'len' WAL bytes starting at WAL position 'start' from a
 * timeline's HISTORY into 'out'; returns the number of DISTINCT bytes
 * filled.  Bytes not covered by any shipped record are left as-is.  This is
 * what a redo worker (and the store-backed SLRU appliers) use to pull WAL
 * for replay.
 *
 * Read-through: a branch's history below its fork point lives in its
 * ancestors' logs -- but a branch's OWN first shipped segment can span the
 * fork (PostgreSQL copies the partial segment at the switch, and archiving
 * ships whole segments), so its log legitimately carries a pre-fork prefix
 * the parent may not have shipped yet.  Each hop therefore serves from its
 * own contiguous log coverage [log_start, ...) up to 'cap', and the next
 * (ancestor) hop's cap becomes min(cap, fork LSN, this hop's log_start):
 * the fork bound keeps ancestor-future records out of the branch's history,
 * and the log_start bound keeps the byte count exact -- whatever the child
 * already served below the fork, the parent must not serve again.  Timeline
 * metadata is write-once after definition (see timeline_define), so the
 * walk needs no lock, matching tl_walk.
 */
static int64_t
wal_read_locked(uint32_t tl, uint64_t start, uint32_t len,
				unsigned char *out)
{
	uint32_t	filled = 0;
	uint64_t	cap = UINT64_MAX;
	int			hops = 0;

	if (tl >= MAX_TIMELINES || start + len < start ||
		!wal_reclaim_frontier_ancestry_allows(tl, start))
		return -1;

	for (;;)
	{
		pthread_rwlock_t *lock = wal_log_lock_for(tl);
		uint64_t	ls;
		uint64_t	frontier = start < cap ? start : cap;

		if (lock == NULL)
			return -1;
		if (wal_read_before_lock_test_hook != NULL)
			wal_read_before_lock_test_hook(tl,
									   wal_read_before_lock_test_hook_arg);
		pthread_rwlock_rdlock(lock);
		/* The optimistic ancestry check above can race frontier publication.
		 * Recheck each visited history level while its WAL lock excludes reclaim;
		 * otherwise a read that queued just before publication could return a
		 * successful partial/empty result from an already removed prefix. */
		/* This level is checked while its own WAL lock excludes reclaim.  The
		 * next parent is checked again under the parent's lock on the next loop;
		 * walking the complete ancestry here would reintroduce a TOCTOU gap. */
		if (!wal_reclaim_frontier_level_allows(tl, frontier))
		{
			pthread_rwlock_unlock(lock);
			return -1;
		}
		ls = wal_log_start(tl);

		if (ls != UINT64_MAX && start + len > ls && start < cap)
		{
			uint64_t	ws = start > ls ? start : ls;
			int64_t		n;

			n = wal_read_one(tl, ws, (uint32_t) (start + len - ws),
						 cap, out + (ws - start));
			if (n < 0)
			{
				pthread_rwlock_unlock(lock);
				return -1;
			}
			filled += (uint32_t) n;
		}
		pthread_rwlock_unlock(lock);

		/* everything below min(cap, fork, own coverage) is the parent's */
		if (!timeline_has_parent(tl))
			break;
		if (timelines[tl].branch_lsn < cap)
			cap = timelines[tl].branch_lsn;
		if (ls < cap)
			cap = ls;
		if (start >= cap)
			break;				/* window fully served at/above the bound */
		if (++hops > MAX_TIMELINES)
			break;				/* defensive: malformed chain */
		tl = (uint32_t) timelines[tl].parent;
	}
	return filled;
}

static int64_t
wal_read(uint32_t tl, uint64_t start, uint32_t len, unsigned char *out)
{
	return wal_read_locked(tl, start, len, out);
}

/* Rebuild wal_end[tl] by scanning the timeline's WAL log at startup. */
static int
wal_recover_one(uint32_t tl)
{
	uint64_t	off = 0;
	uint64_t	good_off = 0;
	WalRecHdr	h;
	unsigned char byte;
	int			truncate_needed = 0;
	int			nread;

	if (tl >= MAX_TIMELINES)
		return -1;
	free(wal_chunks[tl]);
	wal_chunks[tl] = NULL;
	wal_chunks_n[tl] = 0;
	wal_chunks_cap[tl] = 0;
	wal_log_bytes[tl] = 0;
	__atomic_store_n(&wal_start_valid[tl], 0, __ATOMIC_RELEASE);
	__atomic_store_n(&wal_end[tl], 0, __ATOMIC_RELEASE);
	wal_covered[tl] = 0;
	wal_covered_off[tl] = 0;
	wal_covered_valid[tl] = 0;
	while ((nread = ps_storage->wal_read(tl, off, &h, sizeof(h))) ==
		   (int) sizeof(h))
	{
		if (h.magic != WAL_MAGIC)
			break;
		if (h.start_lsn + h.len < h.start_lsn)
		{
			truncate_needed = 1;
			break;
		}
		if (h.len > 0 &&
			ps_storage->wal_read(tl, off + sizeof(h) + h.len - 1,
								 &byte, 1) != 1)
		{
			truncate_needed = 1;
			break;
		}
		if (wal_chunk_reserve(tl) != 0)
			return -1;
		wal_chunk_add(tl, off, &h);
		if (h.start_lsn + h.len > wal_end_read(tl))
		{
			timeline_mark_used(tl);
			wal_end_advance(tl, h.start_lsn + h.len);
		}
		off += sizeof(h) + h.len;
		good_off = off;
	}
	if (nread > 0 && nread < (int) sizeof(h))
		truncate_needed = 1;
	if (truncate_needed && ps_storage->wal_truncate)
		(void) ps_storage->wal_truncate(tl, good_off);
	return 0;
}

/* ===================== per-page WAL index ============================== */

/*
 * Maps (timeline, key, block) -> the LSNs of WAL records that modify that page,
 * in ascending order.  This is the lookup single-page materialization needs: to
 * rebuild page P as-of LSN L, take P's newest stored image and replay the WAL
 * records whose LSNs fall after it and <= L.  Populated by decoding shipped WAL
 * (next milestone); queried via PS_OP_WAL_INDEX_GET.
 *
 * Each successful index addition is also appended to a per-timeline durable
 * log.  Restart replays that log; a later indexer can therefore resume from a
 * durable boundary instead of treating a daemon restart as an empty index.
 */
#define WALIDX_MAGIC	0x57494458	/* "WIDX" */
#define WALIDX_PROGRESS_MAGIC	0x57495047	/* "WIPG" */
#define WALIDX_SNAPSHOT_PAYLOAD_MAGIC UINT32_C(0x44534957) /* "WISD" */
#define WALIDX_SNAPSHOT_PAYLOAD_VERSION_V1 1
#define WALIDX_SNAPSHOT_PAYLOAD_VERSION_V2 2
#define WALIDX_SNAPSHOT_PAYLOAD_BYTES_V1 64
#define WALIDX_SNAPSHOT_PAYLOAD_VERSION 3
#define WALIDX_SNAPSHOT_PAYLOAD_BYTES 72
#define WALIDX_SNAPSHOT_DEFAULT_TRIGGER (1024u * 1024u)

typedef struct WalIdxLogHdr
{
	uint32_t	magic;
	uint32_t	rec_len;
} WalIdxLogHdr;

typedef struct WalIdxRecV1
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	crc;
	uint32_t	reserved;
	uint32_t	timeline;
	uint32_t	block;
	uint64_t	lsn;
	PsKey		key;
} WalIdxRecV1;

typedef struct WalIdxRec
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	crc;
	uint32_t	flags;			/* PS_WAL_INDEX_FLAG_* */
	uint32_t	timeline;
	uint32_t	block;
	uint64_t	lsn;
	uint64_t	end_lsn;
	PsKey		key;
} WalIdxRec;

_Static_assert(sizeof(WalIdxRecV1) == 56,
			   "legacy WAL-index record format must remain readable");
_Static_assert(sizeof(WalIdxRec) == 64,
			   "WAL-index record format must remain stable");
_Static_assert(offsetof(WalIdxRec, flags) == 12,
			   "WAL-index flags must reuse the legacy reserved field");

typedef struct WalIdxProgressRec
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	crc;
	uint32_t	pad;
	uint32_t	timeline;
	uint32_t	pad2;
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	uint64_t	shard_mask[2];
	uint64_t	shard_offsets[PS_MAX_CHANNELS];
} WalIdxProgressRec;

static uint64_t walidx_progress[MAX_TIMELINES];
static unsigned char walidx_progress_valid[MAX_TIMELINES];
/* progress_valid also describes the provisional first WAL position before a
 * durable progress marker exists; reclaim policy must use this stricter bit. */
static unsigned char walidx_progress_durable[MAX_TIMELINES];
static uint64_t walidx_shards_seen[MAX_TIMELINES][2];
static uint64_t walidx_shards_required[MAX_TIMELINES][2];
static uint64_t walidx_shard_offsets_seen[MAX_TIMELINES][PS_MAX_CHANNELS];
static uint64_t walidx_shard_offsets_required[MAX_TIMELINES][PS_MAX_CHANNELS];
static uint64_t walidx_snapshot_generation[MAX_TIMELINES];
static uint64_t walidx_snapshot_start[MAX_TIMELINES];
static uint64_t walidx_snapshot_end[MAX_TIMELINES];
static uint64_t walidx_snapshot_offsets[MAX_TIMELINES][PS_MAX_CHANNELS];
static uint64_t walidx_snapshot_bytes[MAX_TIMELINES];
static unsigned char walidx_snapshot_reshard_pending[MAX_TIMELINES];
static struct timespec walidx_snapshot_retry_at[MAX_TIMELINES];
static uint32_t walidx_snapshot_cursor;
static uint64_t walidx_log_epoch[MAX_TIMELINES][PS_MAX_CHANNELS];
static unsigned char walidx_snapshot_gc_pending[MAX_TIMELINES];
static uint32_t walidx_snapshot_gc_cursor;
static struct timespec walidx_snapshot_gc_retry_at[MAX_TIMELINES];
static PsWalIdxSnapshotPrepared walidx_snapshot_cleanup[MAX_TIMELINES];
static int walidx_snapshot_cleanup_pending[MAX_TIMELINES];
static struct timespec walidx_snapshot_cleanup_retry_at[MAX_TIMELINES];
static pthread_mutex_t walidx_meta_lock = PTHREAD_MUTEX_INITIALIZER;
typedef struct WalIdxPublishLock
{
	pthread_mutex_t mutex;
	pthread_cond_t readers_ready;
	pthread_cond_t writers_ready;
	uint32_t readers;
	uint32_t writers_waiting;
	int writer;
} WalIdxPublishLock;

static WalIdxPublishLock walidx_publish_lock = {
	PTHREAD_MUTEX_INITIALIZER,
	PTHREAD_COND_INITIALIZER,
	PTHREAD_COND_INITIALIZER,
	0, 0, 0
};

static void
walidx_publish_rdlock(void)
{
	pthread_mutex_lock(&walidx_publish_lock.mutex);
	while (walidx_publish_lock.writer || walidx_publish_lock.writers_waiting != 0)
		pthread_cond_wait(&walidx_publish_lock.readers_ready,
						  &walidx_publish_lock.mutex);
	walidx_publish_lock.readers++;
	pthread_mutex_unlock(&walidx_publish_lock.mutex);
}

static void
walidx_publish_rdunlock(void)
{
	pthread_mutex_lock(&walidx_publish_lock.mutex);
	walidx_publish_lock.readers--;
	if (walidx_publish_lock.readers == 0 &&
		walidx_publish_lock.writers_waiting != 0)
		pthread_cond_signal(&walidx_publish_lock.writers_ready);
	pthread_mutex_unlock(&walidx_publish_lock.mutex);
}

static void
walidx_publish_wrlock(void)
{
	pthread_mutex_lock(&walidx_publish_lock.mutex);
	walidx_publish_lock.writers_waiting++;
	while (walidx_publish_lock.writer || walidx_publish_lock.readers != 0)
		pthread_cond_wait(&walidx_publish_lock.writers_ready,
						  &walidx_publish_lock.mutex);
	walidx_publish_lock.writers_waiting--;
	walidx_publish_lock.writer = 1;
	pthread_mutex_unlock(&walidx_publish_lock.mutex);
}

static void
walidx_publish_wrunlock(void)
{
	pthread_mutex_lock(&walidx_publish_lock.mutex);
	walidx_publish_lock.writer = 0;
	if (walidx_publish_lock.writers_waiting != 0)
		pthread_cond_signal(&walidx_publish_lock.writers_ready);
	else
		pthread_cond_broadcast(&walidx_publish_lock.readers_ready);
	pthread_mutex_unlock(&walidx_publish_lock.mutex);
}

static int
walidx_frontier_publication_pending(uint32_t timeline)
{
	uint64_t snapshot_end;

	if (timeline >= MAX_TIMELINES)
		return 1;
	if (__atomic_load_n(&walidx_snapshot_cleanup_pending[timeline],
						__ATOMIC_ACQUIRE))
		return 1;
	pthread_mutex_lock(&walidx_meta_lock);
	snapshot_end = walidx_snapshot_end[timeline];
	pthread_mutex_unlock(&walidx_meta_lock);
	return snapshot_end < walidx_frontier_current(timeline);
}

static void
publish_wal_index_metrics(void)
{
	uint64_t	pending = 0;
	uint32_t	lagging = 0;

	if (metrics_header == NULL)
		return;
	pthread_mutex_lock(&walidx_meta_lock);
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		uint64_t shipped = wal_end_read(tl);
		uint64_t indexed = walidx_progress[tl];

		/* Unused timelines have neither lag nor a WAL log to probe. */
		if (shipped == 0)
			continue;
		if (!walidx_progress_valid[tl])
		{
			uint64_t first = wal_log_start(tl);

			indexed = first == UINT64_MAX ? shipped : first;
		}
		if (shipped > indexed)
		{
			uint64_t delta = shipped - indexed;

			pending = UINT64_MAX - pending < delta ? UINT64_MAX : pending + delta;
			lagging++;
		}
	}
	pthread_mutex_unlock(&walidx_meta_lock);
	ps_store_release_u64(&metrics_header->wal_index_pending_bytes, pending);
	ps_store_release(&metrics_header->wal_index_lagging_timelines, lagging);
}

void
ps_core_set_metrics_header(PsShmHeader *hdr)
{
	metrics_header = hdr;
	publish_wal_index_metrics();
	if (hdr != NULL)
		ps_backpressure_refresh();
}

static void
walidx_progress_init(uint32_t tl, uint64_t first_lsn)
{
	if (tl >= MAX_TIMELINES || first_lsn == UINT64_MAX)
		return;
	pthread_mutex_lock(&walidx_meta_lock);
	if (!walidx_progress_valid[tl])
	{
		walidx_progress[tl] = first_lsn;
		walidx_progress_valid[tl] = 1;
	}
	pthread_mutex_unlock(&walidx_meta_lock);
}

static uint32_t
walidx_rec_crc(WalIdxRec *rec)
{
	uint32_t	save = rec->crc;
	uint32_t	crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static uint32_t
walidx_rec_v1_crc(WalIdxRecV1 *rec)
{
	uint32_t	save = rec->crc;
	uint32_t	crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static uint32_t
walidx_progress_crc(WalIdxProgressRec *rec)
{
	uint32_t	save = rec->crc;
	uint32_t	crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static void
walidx_mark_shard(uint64_t mask[2], uint32_t shard)
{
	if (shard < PS_MAX_CHANNELS)
		mask[shard / 64] |= UINT64_C(1) << (shard % 64);
}

static int
walidx_shard_marked(const uint64_t mask[2], uint32_t shard)
{
	return shard < PS_MAX_CHANNELS &&
		(mask[shard / 64] & (UINT64_C(1) << (shard % 64))) != 0;
}

static int
walidx_mask_valid_for_shards(const uint64_t mask[2])
{
	uint32_t	ns = core_shards();

	for (uint32_t shard = ns; shard < PS_MAX_CHANNELS; shard++)
		if (walidx_shard_marked(mask, shard))
			return 0;
	return 1;
}

static int
walidx_offsets_valid_for_shards(const uint64_t offsets[PS_MAX_CHANNELS])
{
	uint32_t	ns = core_shards();

	for (uint32_t shard = ns; shard < PS_MAX_CHANNELS; shard++)
		if (offsets[shard] != 0)
			return 0;
	return 1;
}

typedef struct WalIdxEnt
{
	struct WalIdxEnt *next;
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	struct WalIdxItem *items;	/* ascending by LSN */
	int			n;
	int			cap;
} WalIdxEnt;

typedef struct WalIdxItem
{
	uint64_t	lsn;
	uint64_t	end_lsn;
	uint32_t	flags;
} WalIdxItem;

/* Caller holds all shard write locks and map-rd.  The scan only touches the
 * in-memory WAL-index; it deliberately performs no I/O while map-rd is held. */
static int
wal_reclaim_raw_dependency_floor(uint32_t timeline, uint64_t store_start,
								 uint64_t *floor_out)
{
	uint64_t floor = 0;

	for (uint32_t shard = 0; shard < core_shards(); shard++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (WalIdxEnt *entry = g_shards[shard].walidx[bucket];
				 entry != NULL; entry = entry->next)
				if (entry->n != 0)
				{
					for (int i = 0; i < entry->n; i++)
					{
						WalIdxItem *item = &entry->items[i];
						uint64_t projected = item->lsn;

						/* Only an ancestry-visible child item consumes raw WAL on
						 * this timeline.  A child-local item after its fork point
						 * remains on the child's own WAL store. */
						if (!retention_project_lsn(entry->timeline, timeline,
												  &projected) || projected != item->lsn)
							continue;
						/* A zero/legacy LSN cannot identify a safe raw-WAL
						 * dependency.  Unknown metadata is retained at its LSN. */
						if (item->lsn == 0 || item->lsn < store_start)
							return -1;
						if ((item->flags & PS_WAL_INDEX_FLAG_KNOWN) != 0 &&
							 item->end_lsn <= item->lsn)
							return -1;
						if (floor == 0 || item->lsn < floor)
							floor = item->lsn;
					}
				}
	*floor_out = floor;
	return 0;
}

/* Caller holds walidx_meta_lock.  A progress value initialized from the first
 * append is not durable and is intentionally rejected here. */
static int
wal_reclaim_walidx_state_valid(uint32_t timeline, uint64_t *progress_out)
{
	uint64_t progress;

	if (timeline >= MAX_TIMELINES || !walidx_progress_valid[timeline] ||
		!walidx_progress_durable[timeline] ||
		(progress = walidx_progress[timeline]) == 0 ||
		walidx_snapshot_reshard_pending[timeline] ||
		walidx_snapshot_gc_pending[timeline] ||
		__atomic_load_n(&walidx_snapshot_cleanup_pending[timeline],
						__ATOMIC_ACQUIRE) ||
		((walidx_snapshot_generation[timeline] != 0 &&
		  walidx_snapshot_start[timeline] > walidx_snapshot_end[timeline])) ||
		walidx_snapshot_end[timeline] > progress ||
		walidx_snapshot_end[timeline] < walidx_frontier_current(timeline))
		return 0;
	for (uint32_t word = 0; word < 2; word++)
		if ((walidx_shards_required[timeline][word] &
			 ~walidx_shards_seen[timeline][word]) != 0)
			return 0;
	for (uint32_t shard = 0; shard < core_shards(); shard++)
		if (walidx_shard_offsets_seen[timeline][shard] <
			walidx_shard_offsets_required[timeline][shard])
			return 0;
	*progress_out = progress;
	return 1;
}

static void
wal_reclaim_backoff(uint32_t timeline, const struct timespec *now)
{
	wal_reclaim_retry_at[timeline] = *now;
	if (wal_reclaim_retry_at[timeline].tv_sec < LONG_MAX)
		wal_reclaim_retry_at[timeline].tv_sec++;
}

/* Read only stable per-timeline state while the WAL lock excludes append,
 * segment sync and reclaim.  This is deliberately weaker than a safety
 * decision: the caller must repeat the complete validation after admission and
 * the WAL-index gates have drained.  Observation ignores retry_at so a failed
 * reclaim cannot make real physical debt disappear; maintenance passes
 * honor_retry_at to avoid repeatedly draining admission for the same failure. */
static int
wal_reclaim_preselected(struct timespec *now_out, int honor_retry_at,
						int *observation_error)
{
	if (observation_error != NULL)
		*observation_error = 0;
	if (clock_gettime(CLOCK_MONOTONIC, now_out) != 0)
		return 0;
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		pthread_rwlock_t *wal_lock;
		uint64_t residual_target = 0;
		int residual_pending;
		int residual_status;
		int eligible;

		if ((honor_retry_at &&
			 (now_out->tv_sec < wal_reclaim_retry_at[tl].tv_sec ||
			  (now_out->tv_sec == wal_reclaim_retry_at[tl].tv_sec &&
			   now_out->tv_nsec < wal_reclaim_retry_at[tl].tv_nsec))) ||
			!ps_timeline_live(tl))
			continue;
		wal_lock = wal_log_lock_for(tl);
		if (wal_lock == NULL)
			continue;
		pthread_rwlock_rdlock(wal_lock);
		residual_status = wal_segment_store_opened[tl] ?
			ps_wal_store_residual_prefix_pending(&wal_segment_stores[tl],
											 &residual_target) : 0;
		residual_pending = residual_status > 0;
		if (residual_status < 0 && observation_error != NULL)
			*observation_error = 1;
		/* A residual/storage error is an observation failure, not a
		 * maintenance candidate: draining admission cannot repair it. */
		eligible = wal_segment_store_opened[tl] && residual_status >= 0 &&
			(residual_pending ||
			 (wal_segment_stores[tl].nentries != 0 &&
			  wal_segment_stores[tl].start_lsn <= UINT64_MAX -
			  wal_segment_stores[tl].segment_size &&
			  wal_segment_stores[tl].start_lsn +
			  wal_segment_stores[tl].segment_size <=
			  wal_segment_stores[tl].end_lsn));
		pthread_rwlock_unlock(wal_lock);
		if (eligible)
			return 1;
	}
	return 0;
}

/*
 * R3b-3 conservative core integration.  The caller already owns the
 * lifecycle read side.  A cheap WAL-lock-only preselection keeps the idle
 * maintenance path from draining admission when no timeline can possibly
 * reclaim a complete segment.  Each selected timeline is then revalidated in
 * full under admission -> all shards -> walidx_prune -> walidx publish -> WAL;
 * all-shard locks cover only the in-memory WAL-index snapshot and are released
 * before retention/control I/O, layer I/O, or unlink.
 */
static int
wal_segment_reclaim_one(void)
{
	struct timespec now;
	uint32_t nshards = core_shards();
	int did = 0;

	if (ps_storage == NULL || ps_storage->name == NULL ||
		strcmp(ps_storage->name, "posix") != 0)
		return 0; /* SPDK and unknown providers have no safe R3b policy. */
	if (!wal_reclaim_preselected(&now, 1, NULL))
		return 0;
	if (admission_write_lock() != 0)
		return 0;
	for (uint32_t pass = 0; pass < MAX_TIMELINES; pass++)
	{
		uint32_t tl = (wal_reclaim_cursor + pass) % MAX_TIMELINES;
		PsWalStore *store;
		pthread_rwlock_t *wal_lock;
		uint64_t retention_floor = 0;
		uint64_t progress = 0;
		uint64_t raw_floor = 0;
		uint64_t candidate;
		uint64_t target;
		uint64_t residual_target = 0;
		int attempt = 0;
		int rc;
		int walidx_valid;
		int residual_pending;
		int residual_status;
		int shards_locked = 0;

		if (now.tv_sec < wal_reclaim_retry_at[tl].tv_sec ||
			(now.tv_sec == wal_reclaim_retry_at[tl].tv_sec &&
			 now.tv_nsec < wal_reclaim_retry_at[tl].tv_nsec))
			continue;
		if (!ps_timeline_live(tl))
			continue;
		wal_lock = wal_log_lock_for(tl);
		if (wal_lock == NULL)
			continue;
		/* Avoid taking the global drain for stale preselection results. */
		pthread_rwlock_rdlock(wal_lock);
		store = &wal_segment_stores[tl];
		residual_status = wal_segment_store_opened[tl] ?
			ps_wal_store_residual_prefix_pending(store, &residual_target) : 0;
		residual_pending = residual_status > 0;
		if (!wal_segment_store_opened[tl] ||
			residual_status < 0 ||
			(!residual_pending &&
			 (store->nentries == 0 || store->start_lsn > UINT64_MAX -
														 store->segment_size ||
			  store->start_lsn + store->segment_size > store->end_lsn)))
		{
			pthread_rwlock_unlock(wal_lock);
			continue;
		}
		pthread_rwlock_unlock(wal_lock);

		/* WAL-index writers take shard-wr before the publish read gate. */
		for (uint32_t shard = 0; shard < nshards; shard++)
			ps_lock_shard_wr(shard);
		shards_locked = 1;
		pthread_rwlock_wrlock(&walidx_prune_lock);
		walidx_publish_wrlock();
		pthread_rwlock_wrlock(wal_lock);
		store = &wal_segment_stores[tl];
		residual_status = wal_segment_store_opened[tl] ?
			ps_wal_store_residual_prefix_pending(store, &residual_target) : 0;
		residual_pending = residual_status > 0;
		/* Full revalidation after all global gates. */
		if (!ps_timeline_live(tl) ||
			!wal_segment_store_opened[tl] ||
			residual_status < 0 ||
			(!residual_pending &&
			 (store->nentries == 0 || store->start_lsn > UINT64_MAX -
																		 store->segment_size ||
																		 store->start_lsn + store->segment_size > store->end_lsn)))
			goto unlock_timeline;
		attempt = 1;
		if (residual_pending)
		{
			/* The durable frontier is already published.  The residual retry is
			 * independent of WAL-index proof and may be the only work left after
			 * a restart, including an empty logical catalog. */
			for (uint32_t shard = nshards; shard > 0; shard--)
				ps_unlock_shard(shard - 1);
			shards_locked = 0;
			if (residual_target < store->start_lsn ||
				residual_target > store->end_lsn ||
				residual_target % store->segment_size != 0)
				goto retry_timeline;
			if (wal_reclaim_attempt_test_hook != NULL)
				wal_reclaim_attempt_test_hook(tl, wal_reclaim_attempt_test_hook_arg);
			rc = ps_wal_store_reclaim_prefix(store, residual_target);
			if (rc == 0)
			{
				memset(&wal_reclaim_retry_at[tl], 0,
					   sizeof(wal_reclaim_retry_at[tl]));
				did = 1;
				goto selected_done;
			}
			goto retry_timeline;
		}
		pthread_mutex_lock(&walidx_meta_lock);
		walidx_valid = wal_reclaim_walidx_state_valid(tl, &progress);
		pthread_mutex_unlock(&walidx_meta_lock);
		/* This is the only section that needs all shard locks.  It scans stable
		 * in-memory entries while the publish/prune gates freeze index mutation.
		 * The map lock is nested according to the established shard -> map order.
		 */
		ps_lock_map_rd();
		rc = 0;
		if (walidx_valid)
			rc = wal_reclaim_raw_dependency_floor(tl, store->start_lsn,
										 &raw_floor);
		ps_unlock_map();
		for (uint32_t shard = nshards; shard > 0; shard--)
			ps_unlock_shard(shard - 1);
		shards_locked = 0;
		/* Do not hold reader-facing WAL/WAL-index gates while the effective-floor
		 * scan may refresh a layer under map-wr.  WAL_READ and WAL_INDEX_GET can
		 * hold map-rd before taking those gates, so retaining them here would form
		 * a map lock cycle.  admission-wr keeps append and WAL-index mutation frozen
		 * across the unlocked interval. */
		pthread_rwlock_unlock(wal_lock);
		walidx_publish_wrunlock();
		pthread_rwlock_unlock(&walidx_prune_lock);
		if (wal_reclaim_before_floor_test_hook != NULL)
			wal_reclaim_before_floor_test_hook(tl,
										 wal_reclaim_before_floor_test_hook_arg);
		rc = rc != 0 ? rc : retention_effective_floor(tl,
											 PS_RETENTION_RESOURCE_WAL,
											 &retention_floor);
		pthread_rwlock_wrlock(&walidx_prune_lock);
		walidx_publish_wrlock();
		pthread_rwlock_wrlock(wal_lock);
		store = &wal_segment_stores[tl];
		/* Revalidate the physical store after readers admitted during the floor
		 * scan have drained.  No writer could pass admission-wr in the interval. */
		if (!ps_timeline_live(tl) || !wal_segment_store_opened[tl] ||
			store->metadata_fenced ||
			!walidx_valid || rc != 0 ||
			retention_floor == 0)
		{
			goto retry_timeline;
		}
		candidate = retention_floor < progress ? retention_floor : progress;
		if (raw_floor != 0 && raw_floor < candidate)
			candidate = raw_floor;
		if (timeline_has_parent(tl) && timelines[tl].branch_lsn < candidate)
			candidate = timelines[tl].branch_lsn;
		if (candidate > store->end_lsn)
			candidate = store->end_lsn;
		target = candidate - candidate % store->segment_size;
		if (target <= store->start_lsn)
		{
			/* A complete segment exists, but the proven floor is still in the
			 * current boundary.  Avoid repeating the global drain every idle tick;
			 * the cheap due-time preselection will retry after the bounded delay. */
			wal_reclaim_backoff(tl, &now);
			goto selected_done;
		}
		if (target > store->end_lsn)
			goto retry_timeline;
		if (wal_reclaim_attempt_test_hook != NULL)
			wal_reclaim_attempt_test_hook(tl, wal_reclaim_attempt_test_hook_arg);
		rc = ps_wal_store_reclaim_prefix(store, target);
		if (rc == 0)
		{
			memset(&wal_reclaim_retry_at[tl], 0,
				   sizeof(wal_reclaim_retry_at[tl]));
			did = 1;
			goto selected_done;
		}

retry_timeline:
		if (attempt)
			wal_reclaim_backoff(tl, &now);

		/* Advance after every selected candidate, including fail-closed or failed
		 * attempts, so it cannot starve later timelines on subsequent ticks. */
selected_done:
		wal_reclaim_cursor = (tl + 1) % MAX_TIMELINES;

unlock_timeline:
		pthread_rwlock_unlock(wal_lock);
		walidx_publish_wrunlock();
		pthread_rwlock_unlock(&walidx_prune_lock);
		if (shards_locked)
			for (uint32_t shard = nshards; shard > 0; shard--)
				ps_unlock_shard(shard - 1);
		/* At most one selected LIVE timeline per maintenance call. */
		if (attempt)
			break;
	}
	ps_admission_write_unlock();
	return did;
}

int
ps_test_wal_reclaim_maintenance(void)
{
	return wal_segment_reclaim_one();
}

int
ps_test_wal_retained_base(uint32_t timeline, uint64_t *base_out)
{
	pthread_rwlock_t *wal_lock;
	int rc;

	if (base_out == NULL || timeline >= MAX_TIMELINES)
		return -1;
	wal_lock = wal_log_lock_for(timeline);
	if (wal_lock == NULL)
		return -1;
	pthread_rwlock_rdlock(wal_lock);
	rc = wal_segment_store_opened[timeline] ?
		ps_wal_store_retained_base(&wal_segment_stores[timeline], base_out) : -1;
	pthread_rwlock_unlock(wal_lock);
	return rc;
}

int
ps_test_walidx_frontier_exception_active(uint32_t timeline, uint64_t lsn)
{
	int active;

	ps_lock_map_rd();
	active = walidx_frontier_exception_active(timeline, lsn);
	ps_unlock_map();
	return active;
}

static void
free_walidx_indexes(void)
{
	for (uint32_t sh = 0; sh < MAX_SHARDS; sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		{
			WalIdxEnt *entry = g_shards[sh].walidx[bucket];

			while (entry != NULL)
			{
				WalIdxEnt *next = entry->next;

				free(entry->items);
				free(entry);
				entry = next;
			}
			g_shards[sh].walidx[bucket] = NULL;
		}
}

static void
walidx_purge_timeline(uint32_t tl)
{
	for (uint32_t sh = 0; sh < MAX_SHARDS; sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		{
			WalIdxEnt **link = &g_shards[sh].walidx[bucket];

			while (*link != NULL)
			{
				WalIdxEnt *entry = *link;

				if (entry->timeline != tl)
				{
					link = &entry->next;
					continue;
				}
				*link = entry->next;
				free(entry->items);
				free(entry);
			}
		}
	pthread_mutex_lock(&walidx_meta_lock);
	walidx_progress[tl] = 0;
	walidx_progress_valid[tl] = 0;
	walidx_progress_durable[tl] = 0;
	memset(walidx_shards_seen[tl], 0, sizeof(walidx_shards_seen[tl]));
	memset(walidx_shards_required[tl], 0,
		   sizeof(walidx_shards_required[tl]));
	memset(walidx_shard_offsets_seen[tl], 0,
		   sizeof(walidx_shard_offsets_seen[tl]));
	memset(walidx_shard_offsets_required[tl], 0,
		   sizeof(walidx_shard_offsets_required[tl]));
	walidx_snapshot_generation[tl] = 0;
	walidx_snapshot_start[tl] = 0;
	walidx_snapshot_end[tl] = 0;
	memset(walidx_snapshot_offsets[tl], 0,
		   sizeof(walidx_snapshot_offsets[tl]));
	walidx_snapshot_bytes[tl] = 0;
	walidx_snapshot_reshard_pending[tl] = 0;
	memset(&walidx_snapshot_retry_at[tl], 0,
		   sizeof(walidx_snapshot_retry_at[tl]));
	memset(walidx_log_epoch[tl], 0, sizeof(walidx_log_epoch[tl]));
	walidx_snapshot_gc_pending[tl] = 0;
	__atomic_store_n(&walidx_snapshot_force_due[tl], 0, __ATOMIC_RELEASE);
	__atomic_store_n(&walidx_snapshot_gc_force_due[tl], 0, __ATOMIC_RELEASE);
	memset(&walidx_snapshot_gc_retry_at[tl], 0,
		   sizeof(walidx_snapshot_gc_retry_at[tl]));
	memset(&walidx_snapshot_cleanup[tl], 0,
		   sizeof(walidx_snapshot_cleanup[tl]));
	walidx_snapshot_cleanup_pending[tl] = 0;
	memset(&walidx_snapshot_cleanup_retry_at[tl], 0,
		   sizeof(walidx_snapshot_cleanup_retry_at[tl]));
	/* The frontier slots are keyed by incarnation.  Private-artifact cleanup
	 * resets runtime state only; the old durable slot remains available until a
	 * later incarnation legitimately takes the ID. */
	pthread_mutex_unlock(&walidx_meta_lock);
}

static void
wal_runtime_purge(uint32_t tl)
{
	if (wal_segment_store_opened[tl])
	{
		ps_wal_store_close(&wal_segment_stores[tl]);
		wal_segment_store_opened[tl] = 0;
	}
	free(wal_chunks[tl]);
	wal_chunks[tl] = NULL;
	wal_chunks_n[tl] = 0;
	wal_chunks_cap[tl] = 0;
	wal_log_bytes[tl] = 0;
	__atomic_store_n(&wal_start[tl], 0, __ATOMIC_RELAXED);
	__atomic_store_n(&wal_start_valid[tl], 0, __ATOMIC_RELEASE);
	__atomic_store_n(&wal_end[tl], 0, __ATOMIC_RELEASE);
	wal_covered[tl] = 0;
	wal_covered_off[tl] = 0;
	wal_covered_valid[tl] = 0;
	walidx_purge_timeline(tl);
}

/* The durable DELETED event is the proof that all old-incarnation consumers
 * have drained and have been removed.  Reinitialize only process-local state
 * here; the page/WAL reclaimed frontiers remain durable fences for old
 * horizons and are intentionally not reset on reuse. */
static void
timeline_reset_reuse_runtime(uint32_t timeline)
{
	page_cleanup_purge_timeline_locked(timeline);
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		if (g_shards[sh].memtable != NULL)
			ps_memtable_discard_timeline(g_shards[sh].memtable, timeline);
	ps_pgcache_invalidate_timeline(timeline);
	wal_runtime_purge(timeline);
	memset(page_prune_due[timeline], 0, sizeof(page_prune_due[timeline]));
	__atomic_store_n(&timeline_used[timeline], 0, __ATOMIC_RELEASE);
	__atomic_store_n(&timeline_wal_cleanup_done[timeline], 0, __ATOMIC_RELEASE);
	__atomic_store_n(&timeline_page_cleanup_done[timeline], 0, __ATOMIC_RELEASE);
	__atomic_store_n(&fork_meta_deletion_cutover_done[timeline], 0,
						 __ATOMIC_RELEASE);
}

/* The lifecycle writer has already drained all ordinary requests before the
 * timeline became DELETING.  This maintenance operation therefore owns the
 * target's WAL lock, every runtime WAL-index shard, the prune fence and the
 * snapshot publication gate while it drops the private artifacts. */
static int
timeline_delete_wal_cleanup_one(void)
{
	for (uint32_t tl = 1; tl < MAX_TIMELINES; tl++)
	{
		PsTimelineState state;
		pthread_rwlock_t *wal_lock;
		int rc;

		if (__atomic_load_n(&timeline_wal_cleanup_done[tl], __ATOMIC_ACQUIRE) ||
			!ps_timeline_state(tl, &state, NULL) ||
			state != PS_TIMELINE_DELETING)
			continue;
		/* A backend without an owner-scoped implementation must not guess how
		 * to remove filesystem/device state.  The tombstone remains retryable. */
		if (ps_storage->timeline_wal_cleanup == NULL)
			continue;
		for (uint32_t sh = 0; sh < core_shards(); sh++)
			ps_lock_shard_wr(sh);
		pthread_rwlock_wrlock(&walidx_prune_lock);
		walidx_publish_wrlock();
		wal_lock = wal_log_lock_for(tl);
		if (wal_lock == NULL)
		{
			walidx_publish_wrunlock();
			pthread_rwlock_unlock(&walidx_prune_lock);
			for (uint32_t sh = core_shards(); sh > 0; sh--)
				ps_unlock_shard(sh - 1);
			/* A missing per-timeline lock is a failed attempt, not a reason
			 * to starve later DELETING timelines. */
			continue;
		}
		pthread_rwlock_wrlock(wal_lock);
		/* Close the immutable store before rmdir so no cleanup retry depends on
		 * an unlinked directory fd.  The in-memory catalog is purged only after
		 * the complete physical validation/deletion succeeds. */
		if (wal_segment_store_opened[tl])
		{
			ps_wal_store_close(&wal_segment_stores[tl]);
			wal_segment_store_opened[tl] = 0;
		}
		rc = ps_storage->timeline_wal_cleanup(tl);
		if (rc == 0)
		{
			wal_runtime_purge(tl);
			/* Purging a timeline removes its contribution from the aggregate
			 * pending/lagging view.  Publish while the WAL-index write gate is
			 * still held so readers cannot observe a half-purged runtime state. */
			publish_wal_index_metrics();
			__atomic_store_n(&timeline_wal_cleanup_done[tl], 1,
							 __ATOMIC_RELEASE);
		}
		pthread_rwlock_unlock(wal_lock);
		walidx_publish_wrunlock();
		pthread_rwlock_unlock(&walidx_prune_lock);
		for (uint32_t sh = core_shards(); sh > 0; sh--)
			ps_unlock_shard(sh - 1);
		if (rc == 0)
			return 1;
		/* Keep the failed tombstone retryable, but do not let it starve a
		 * later DELETING timeline on this maintenance tick. */
	}
	return 0;
}

static int
timeline_delete_page_cleanup_one(void)
{
	if (ps_storage->seg_rewrite == NULL)
		return 0; /* SPDK has no safe same-id replacement primitive. */
	for (uint32_t pass = 0; pass < MAX_TIMELINES; pass++)
	{
		uint32_t tl = 1 + (timeline_page_cleanup_cursor + pass) % (MAX_TIMELINES - 1);
		PsTimelineState state;
		int had_entries;
		int rc;

		if (__atomic_load_n(&timeline_page_cleanup_done[tl], __ATOMIC_ACQUIRE) ||
			!__atomic_load_n(&timeline_wal_cleanup_done[tl], __ATOMIC_ACQUIRE) ||
			!ps_timeline_state(tl, &state, NULL) ||
			state != PS_TIMELINE_DELETING)
			continue;
		for (uint32_t sh = 0; sh < core_shards(); sh++)
			ps_lock_shard_wr(sh);
		ps_lock_map_wr();
		had_entries = page_cleanup_has_index_entries_locked(tl);
		rc = page_cleanup_scan_timeline_locked(tl);
		if (rc == 0 && __atomic_load_n(&fork_meta_deletion_cutover_done[tl],
											__ATOMIC_ACQUIRE))
		{
			page_cleanup_purge_timeline_locked(tl);
			for (uint32_t sh = 0; sh < core_shards(); sh++)
				ps_memtable_discard_timeline(g_shards[sh].memtable, tl);
			ps_pgcache_invalidate_timeline(tl);
			__atomic_store_n(&timeline_page_cleanup_done[tl], 1,
							 __ATOMIC_RELEASE);
			rc = had_entries ? 1 : 0;
		}
		ps_unlock_map();
		for (uint32_t sh = core_shards(); sh > 0; sh--)
			ps_unlock_shard(sh - 1);
		timeline_page_cleanup_cursor = (tl - 1) % (MAX_TIMELINES - 1);
		if (rc > 0)
			return 1;
		/* A malformed target segment must not prevent another deleting timeline
		 * from being attempted on the same maintenance tick. */
	}
	return 0;
}

/*
 * The process-local cleanup bits above are scheduling hints only.  A crash can
 * erase them, and a test/provider can make an artifact reappear after a bit is
 * set.  DELETED therefore uses the durable state of every consumer as its
 * completion proof and re-runs the idempotent physical checks while all
 * lifecycle/admission/map fences are held.
 */
static int
fork_meta_source_record_valid(const ForkMetaRecV2 *rec)
{
	PsKey zero_key;
	int ordered_marker;

	if (rec->magic != FORK_META_V2_MAGIC || rec->rec_len != sizeof(*rec) ||
		rec->timeline >= MAX_TIMELINES ||
		rec->key.klass > PS_KLASS_READER_SNAPSHOT ||
		rec->pad[0] != 0 || rec->pad[1] != 0 || rec->pad[2] != 0)
		return 0;
	ordered_marker = rec->kind >= FEV_SEG_GROW &&
		rec->kind <= FEV_SEG_COMMIT_BOUND;
	if (ordered_marker)
		return fork_meta_ordered_marker_valid(rec, 1);
	if (rec->kind <= FEV_DEAD)
		return rec->order_id == 0 &&
			(rec->kind != FEV_DEAD || rec->nblocks == 0);
	if (rec->kind == FEV_MIGRATING || rec->kind == FEV_MIGRATED)
	{
		memset(&zero_key, 0, sizeof(zero_key));
		return rec->timeline == 0 && key_eq(&rec->key, &zero_key) &&
			rec->lsn == 0 && rec->admission_seq == 0 && rec->order_id == 0 &&
			rec->nblocks == 0;
	}
	if (rec->kind == FEV_SNAPSHOT_BASE)
		return fork_meta_snapshot_marker_matches(rec);
	return 0;
}

static int
fork_meta_source_has_timeline(uint32_t target)
{
	uint64_t off = 0;

	for (;;)
	{
		uint32_t first;
		int nread;

		nread = ps_storage->fork_meta_read(off, &first, sizeof(first));
		if (nread == 0)
			return 0;
		if (nread != (int) sizeof(first))
			return -1;
		if (first == FORK_META_V2_MAGIC)
		{
			ForkMetaRecV2 rec;

			nread = ps_storage->fork_meta_read(off, &rec, sizeof(rec));
			if (nread != (int) sizeof(rec) ||
				!fork_meta_source_record_valid(&rec))
				return -1;
			if (rec.timeline == target)
				return 1;
			off += sizeof(rec);
		}
		else
		{
			ForkMetaRecV1 rec;

			nread = ps_storage->fork_meta_read(off, &rec, sizeof(rec));
			if (nread != (int) sizeof(rec) || rec.timeline >= MAX_TIMELINES ||
				rec.key.klass > PS_KLASS_READER_SNAPSHOT ||
				rec.kind > FEV_DEAD ||
				(rec.kind == FEV_DEAD && rec.nblocks != 0) ||
				rec.pad[0] != 0 || rec.pad[1] != 0 || rec.pad[2] != 0)
				return -1;
			if (rec.timeline == target)
				return 1;
			off += sizeof(rec);
		}
	}
}

static int
fork_meta_snapshot_has_timeline(uint32_t target)
{
	PsForkmetaSnapshot snapshot;

	if (fork_meta_snapshot_generation == 0)
		return 0;
	if (ps_forkmeta_snapshot_open(&snapshot, fork_meta_snapshot_dir) != 0)
		return -1;
	for (unsigned int part = 0; part < 2; part++)
	{
		ForkMetaSnapshotPayloadHeader header;
		uint64_t nrecords;
		uint64_t record_bytes;
		uint64_t expected;

		if (ps_forkmeta_snapshot_read(&snapshot, part, 0, &header,
									 sizeof(header)) != 0 ||
				header.magic != FORK_META_SNAPSHOT_PAYLOAD_MAGIC ||
				header.version != FORK_META_SNAPSHOT_PAYLOAD_VERSION ||
				header.header_bytes != sizeof(header) || header.part != part ||
				header.record_bytes != sizeof(ForkMetaRecV2) ||
				header.generation != snapshot.generation ||
				header.cutoff_lsn != snapshot.cutoff_lsn ||
				header.cutoff_admission_seq != snapshot.cutoff_admission_seq ||
				header.freeze_admission_seq == 0)
			goto fail;
		nrecords = part == FORK_META_SNAPSHOT_CHECKPOINT ?
			header.checkpoint_records : header.tail_records;
		if (nrecords > UINT64_MAX / sizeof(ForkMetaRecV2))
			goto fail;
		record_bytes = nrecords * sizeof(ForkMetaRecV2);
		if (header.checkpoint_records > UINT64_MAX / sizeof(ForkMetaRecV2) ||
			header.tail_records > UINT64_MAX / sizeof(ForkMetaRecV2) ||
			header.checkpoint_bytes !=
				header.checkpoint_records * sizeof(ForkMetaRecV2) ||
			header.tail_bytes != header.tail_records * sizeof(ForkMetaRecV2) ||
			record_bytes > UINT64_MAX - sizeof(header))
			goto fail;
		expected = sizeof(header) + record_bytes;
		if (snapshot.checkpoint.len != expected &&
			part == FORK_META_SNAPSHOT_CHECKPOINT)
			goto fail;
		if (snapshot.tail.len != expected && part == FORK_META_SNAPSHOT_TAIL)
			goto fail;
		for (uint64_t i = 0; i < nrecords; i++)
		{
			ForkMetaRecV2 rec;

			if (ps_forkmeta_snapshot_read(&snapshot, part,
									 sizeof(header) + i * sizeof(rec), &rec,
									 sizeof(rec)) != 0 ||
				!fork_meta_source_record_valid(&rec) ||
				!fork_meta_snapshot_record_valid(&rec, 0, part,
					(PsPruneFence) {snapshot.cutoff_lsn,
					 snapshot.cutoff_admission_seq}))
				goto fail;
			if (rec.timeline == target)
			{
				ps_forkmeta_snapshot_close(&snapshot);
				return 1;
			}
		}
	}
	ps_forkmeta_snapshot_close(&snapshot);
	return 0;

fail:
	ps_forkmeta_snapshot_close(&snapshot);
	return -1;
}

static int
fork_meta_deletion_durable_complete(uint32_t target)
{
	int rc;

	rc = fork_meta_source_has_timeline(target);
	if (rc != 0)
		return 0;
	rc = fork_meta_snapshot_has_timeline(target);
	return rc == 0;
}

static int
walidx_runtime_has_timeline(uint32_t target)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (WalIdxEnt *entry = g_shards[sh].walidx[bucket]; entry;
				 entry = entry->next)
				if (entry->timeline == target)
					return 1;
	return 0;
}

/* Caller holds lifecycle-write, admission-write, every shard-write, both
 * pruning fences, the WAL-index publication gate, the target WAL lock, and
 * map-write. */
static int
timeline_delete_publish_ready(uint32_t timeline)
{
	PsTimelineState state;
	int page_rc;

	if (!ps_timeline_state(timeline, &state, NULL) ||
		state != PS_TIMELINE_DELETING || timeline_meta_poisoned_load() ||
		ps_manifest_poisoned() || fork_meta_poisoned_load())
		return 0;
	/* A missing capability is never interpreted as an empty consumer.  In
	 * particular this keeps SPDK's NULL same-id rewrite fail-closed. */
	if (ps_storage->meta_append == NULL || ps_storage->fork_meta_read == NULL ||
		ps_storage->fork_meta_rewrite == NULL ||
		ps_storage->timeline_wal_cleanup == NULL ||
		ps_storage->seg_read == NULL || ps_storage->seg_size == NULL ||
		ps_storage->seg_rewrite == NULL ||
		(use_layers && (ps_layer_store == NULL ||
			ps_layer_store->layer_exists_local == NULL ||
			ps_layer_store->delete_local_layer == NULL ||
			ps_layer_store->delete_remote_layer == NULL ||
			ps_layer_store->remote_uri == NULL ||
			(ps_layer_store->verify_remote_layer == NULL &&
			 ps_layer_store->layer_exists_remote == NULL))))
		return 0;
	/* No asynchronous layer publication or retry may still own a copied target
	 * descriptor when the lifecycle state becomes terminal. */
	if (__atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE) != 0 ||
		__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) != 0 ||
		__atomic_load_n(&evict_local_state, __ATOMIC_ACQUIRE) != 0 ||
		fork_meta_snapshot_gc_pending)
		return 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].timeline == timeline)
			return 0;
	/* Revalidate physical private WAL state even when its process-local done bit
	 * says it was already handled.  The callback is idempotent and fail-closed. */
	if (ps_storage->timeline_wal_cleanup(timeline) != 0 ||
		wal_segment_store_opened[timeline] || wal_chunks[timeline] != NULL ||
		wal_chunks_n[timeline] != 0 || wal_chunks_cap[timeline] != 0 ||
		wal_log_bytes[timeline] != 0 || wal_end_read(timeline) != 0 ||
		wal_start_valid[timeline] || wal_covered_valid[timeline] ||
		walidx_runtime_has_timeline(timeline))
		return 0;
	if (__atomic_load_n(&walidx_snapshot_cleanup_pending[timeline],
							__ATOMIC_ACQUIRE) || walidx_snapshot_gc_pending[timeline] ||
		walidx_progress_valid[timeline] || walidx_progress[timeline] != 0 ||
		walidx_snapshot_generation[timeline] != 0 ||
		walidx_snapshot_start[timeline] != 0 ||
		walidx_snapshot_end[timeline] != 0 || walidx_snapshot_bytes[timeline] != 0)
		return 0;
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		if (walidx_log_epoch[timeline][sh] != 0 ||
			walidx_snapshot_offsets[timeline][sh] != 0 ||
			walidx_shard_offsets_seen[timeline][sh] != 0 ||
			walidx_shard_offsets_required[timeline][sh] != 0)
			return 0;
	/* The filtered rewrite is itself the durable shared-segment predicate.  If a
	 * late/recovered target record is found, schedule another retry and do not
	 * trust the old done bit. */
	page_rc = page_cleanup_scan_timeline_locked(timeline);
	if (page_rc != 0)
	{
		__atomic_store_n(&timeline_page_cleanup_done[timeline], 0,
						 __ATOMIC_RELEASE);
		return 0;
	}
	/* This is a mandatory durable-consumer gate.  Runtime state may already be
	 * empty after a restart, but that cannot substitute for proving that neither
	 * the current forkmeta source nor its selected snapshot can resurrect the
	 * target incarnation. */
	if (!fork_meta_deletion_durable_complete(timeline))
		return 0;
	/* A restart can lose the cutover bit after the durable forkmeta source and
	 * selected snapshot have already been filtered.  The remaining page/fork
	 * indexes, memtables, and cache entries are runtime state, so remove them
	 * under the same fences rather than treating the lost bit as proof that the
	 * durable consumer is incomplete. */
	if (page_cleanup_has_index_entries_locked(timeline) ||
		 fork_meta_timeline_records_present_locked(timeline) ||
		 ps_pgcache_has_timeline(timeline))
	{
		page_cleanup_purge_timeline_locked(timeline);
		for (uint32_t sh = 0; sh < core_shards(); sh++)
			ps_memtable_discard_timeline(g_shards[sh].memtable, timeline);
		ps_pgcache_invalidate_timeline(timeline);
		__atomic_store_n(&timeline_page_cleanup_done[timeline], 1,
						 __ATOMIC_RELEASE);
	}
	if (page_cleanup_has_index_entries_locked(timeline) ||
		fork_meta_timeline_records_present_locked(timeline) ||
		ps_pgcache_has_timeline(timeline))
		return 0;
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		if (ps_memtable_has_timeline(g_shards[sh].memtable, timeline))
			return 0;
	return 1;
}

static int
timeline_delete_publish_one(void)
{
	int did = 0;

	/* A maintenance call may have just handed work to an asynchronous worker.
	 * Do not queue the lifecycle writer from that same foreground call: the
	 * worker still owns a lifecycle-read reservation and needs the caller to
	 * return so it can finish.  The next maintenance pass joins/reaps it and
	 * retries publication. */
	if (__atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE) == 1 ||
		__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1 ||
		__atomic_load_n(&evict_local_state, __ATOMIC_ACQUIRE) == 1)
		return 0;

	if (ps_lifecycle_write_lock() != 0)
		return 0;
	if (ps_admission_write_lock() != 0)
	{
		ps_lifecycle_write_unlock();
		return 0;
	}
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		ps_lock_shard_wr(sh);
	pthread_rwlock_wrlock(&page_prune_lock);
	pthread_rwlock_wrlock(&walidx_prune_lock);
	walidx_publish_wrlock();
	for (uint32_t tl = 1; tl < MAX_TIMELINES && !did; tl++)
	{
		pthread_rwlock_t *wal_lock;
		uint64_t incarnation;

		if (!ps_timeline_state(tl, NULL, &incarnation))
			continue;
		wal_lock = wal_log_lock_for(tl);
		if (wal_lock == NULL)
			continue;
		pthread_rwlock_wrlock(wal_lock);
		ps_lock_map_wr();
		if (timeline_delete_publish_ready(tl) &&
			timeline_persist_state(tl, PS_TIMELINE_DELETED, incarnation) == 0)
		{
			/* The append is fsync-durable before this release publication. */
			__atomic_store_n(&timelines[tl].state, PS_TIMELINE_DELETED,
							 __ATOMIC_RELEASE);
			did = 1;
		}
		ps_unlock_map();
		pthread_rwlock_unlock(wal_lock);
	}
	walidx_publish_wrunlock();
	pthread_rwlock_unlock(&walidx_prune_lock);
	pthread_rwlock_unlock(&page_prune_lock);
	for (uint32_t sh = core_shards(); sh > 0; sh--)
		ps_unlock_shard(sh - 1);
	ps_admission_write_unlock();
	ps_lifecycle_write_unlock();
	return did;
}

/* Keep each hash chain canonical so a fixed-size heap can merge the chains
 * into the same key/block/LSN order as the former whole-shard qsort. */
static int
walidx_entry_compare(const WalIdxEnt *a, const WalIdxEnt *b)
{
#define CMP_FIELD(field) \
	do { if (a->field < b->field) return -1; if (a->field > b->field) return 1; } while (0)
	CMP_FIELD(key.spcOid);
	CMP_FIELD(key.dbOid);
	CMP_FIELD(key.relNumber);
	CMP_FIELD(key.forkNum);
	CMP_FIELD(key.klass);
	CMP_FIELD(block);
	CMP_FIELD(timeline);
#undef CMP_FIELD
	return 0;
}

static WalIdxEnt *
walidx_find(uint32_t tl, const PsKey *key, uint32_t block)
{
	uint32_t	h = page_hash(tl, key, block);
	Shard	   *s = shard_for(key);
	WalIdxEnt  *e;

	for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == tl && e->block == block && key_eq(&e->key, key))
			return e;
	return NULL;
}

static int
walidx_lower_bound(const WalIdxEnt *e, uint64_t lsn)
{
	int		lo = 0;
	int		hi = e ? e->n : 0;

	while (lo < hi)
	{
		int mid = lo + (hi - lo) / 2;

		if (e->items[mid].lsn < lsn)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

static int
walidx_metadata_valid(uint32_t flags, uint64_t lsn, uint64_t end_lsn)
{
	return (flags & ~PS_WAL_INDEX_FLAG_MASK) == 0 &&
		((flags & PS_WAL_INDEX_FLAG_FPI) == 0 ||
		 (flags & PS_WAL_INDEX_FLAG_KNOWN) != 0) &&
		(((flags & PS_WAL_INDEX_FLAG_KNOWN) != 0 && end_lsn > lsn) ||
		 (flags == 0 && end_lsn == 0));
}

static int
walidx_add_memory(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn,
				  uint64_t end_lsn, uint32_t flags)
{
	uint32_t	h = page_hash(tl, key, block);
	Shard	   *s = shard_for(key);
	WalIdxEnt  *e;

	timeline_mark_used(tl);

	e = walidx_find(tl, key, block);
	if (!e)
	{
		WalIdxEnt **link = &s->walidx[h & IDX_MASK];

		e = calloc(1, sizeof(*e));
		if (e == NULL)
			return -1;
		e->timeline = tl;
		e->key = *key;
		e->block = block;
		while (*link != NULL && walidx_entry_compare(*link, e) < 0)
			link = &(*link)->next;
		e->next = *link;
		*link = e;
	}
	if (e->n == e->cap)
	{
		int newcap;
		WalIdxItem *grown;

		if (e->cap > INT_MAX / 2)
			return -1;
		newcap = e->cap ? e->cap * 2 : 4;
		grown = realloc(e->items, (size_t) newcap * sizeof(*e->items));
		if (grown == NULL)
			return -1;
		e->items = grown;
		e->cap = newcap;
	}
	{
		int i = walidx_lower_bound(e, lsn);

		if (i < e->n && e->items[i].lsn == lsn)
		{
			e->items[i].flags |= flags;
			if (end_lsn != 0)
				e->items[i].end_lsn = end_lsn;
			return 0;
		}
		memmove(&e->items[i + 1], &e->items[i],
				(size_t) (e->n - i) * sizeof(*e->items));
		e->items[i].lsn = lsn;
		e->items[i].end_lsn = end_lsn;
		e->items[i].flags = flags;
		e->n++;
	}
	return 0;
}

static uint32_t
walidx_get_le32(const unsigned char *p)
{
	return (uint32_t) p[0] | (uint32_t) p[1] << 8 |
		(uint32_t) p[2] << 16 | (uint32_t) p[3] << 24;
}

static uint64_t
walidx_get_le64(const unsigned char *p)
{
	return (uint64_t) walidx_get_le32(p) |
		(uint64_t) walidx_get_le32(p + 4) << 32;
}

static void
walidx_put_le32(unsigned char *p, uint32_t value)
{
	for (unsigned int i = 0; i < 4; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static void
walidx_put_le64(unsigned char *p, uint64_t value)
{
	for (unsigned int i = 0; i < 8; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static void
walidx_snapshot_encode_header(unsigned char out[WALIDX_SNAPSHOT_PAYLOAD_BYTES],
							  uint32_t tl, uint32_t shard,
							  uint64_t nrecords, uint64_t generation,
							  uint64_t start_lsn, uint64_t end_lsn,
							  uint64_t source_offset, uint64_t log_epoch)
{
	uint32_t crc;

	memset(out, 0, WALIDX_SNAPSHOT_PAYLOAD_BYTES);
	walidx_put_le32(out + 0, WALIDX_SNAPSHOT_PAYLOAD_MAGIC);
	walidx_put_le32(out + 4, WALIDX_SNAPSHOT_PAYLOAD_VERSION);
	walidx_put_le32(out + 8, WALIDX_SNAPSHOT_PAYLOAD_BYTES);
	walidx_put_le32(out + 12, tl);
	walidx_put_le32(out + 16, shard);
	walidx_put_le32(out + 20, (uint32_t) nrecords);
	walidx_put_le64(out + 24, generation);
	walidx_put_le64(out + 32, start_lsn);
	walidx_put_le64(out + 40, end_lsn);
	walidx_put_le64(out + 48, source_offset);
	walidx_put_le64(out + 56, log_epoch);
	walidx_put_le32(out + 68, (uint32_t) (nrecords >> 32));
	crc = fnv(out, WALIDX_SNAPSHOT_PAYLOAD_BYTES);
	walidx_put_le32(out + 64, crc);
}

static int
walidx_snapshot_decode_header(const unsigned char *header, uint64_t available,
							  uint32_t tl,
							  uint32_t shard, uint64_t generation,
							  uint64_t start_lsn, uint64_t end_lsn,
							  uint32_t *header_bytes, uint32_t *record_bytes,
							  uint64_t *nrecords,
							  uint64_t *source_offset, uint64_t *log_epoch)
{
	unsigned char copy[WALIDX_SNAPSHOT_PAYLOAD_BYTES];
	uint32_t version;
	uint32_t bytes;
	uint32_t crc_offset;
	uint32_t stored_crc;

	if (available < WALIDX_SNAPSHOT_PAYLOAD_BYTES_V1 ||
		walidx_get_le32(header + 0) != WALIDX_SNAPSHOT_PAYLOAD_MAGIC)
		return -1;
	version = walidx_get_le32(header + 4);
	bytes = walidx_get_le32(header + 8);
	if (version == WALIDX_SNAPSHOT_PAYLOAD_VERSION_V1 &&
		bytes == WALIDX_SNAPSHOT_PAYLOAD_BYTES_V1)
		crc_offset = 56;
	else if (version == WALIDX_SNAPSHOT_PAYLOAD_VERSION_V2 &&
			 bytes == WALIDX_SNAPSHOT_PAYLOAD_BYTES_V1)
		crc_offset = 56;
	else if (version == WALIDX_SNAPSHOT_PAYLOAD_VERSION &&
			 bytes == WALIDX_SNAPSHOT_PAYLOAD_BYTES)
		crc_offset = 64;
	else
		return -1;
	if (available < bytes)
		return -1;
	memset(copy, 0, sizeof(copy));
	memcpy(copy, header, bytes);
	stored_crc = walidx_get_le32(copy + crc_offset);
	walidx_put_le32(copy + crc_offset, 0);
	if (walidx_get_le32(copy + 0) != WALIDX_SNAPSHOT_PAYLOAD_MAGIC ||
		walidx_get_le32(copy + 12) != tl ||
		walidx_get_le32(copy + 16) != shard ||
		walidx_get_le64(copy + 24) != generation ||
		walidx_get_le64(copy + 32) != start_lsn ||
		walidx_get_le64(copy + 40) != end_lsn ||
		(version == WALIDX_SNAPSHOT_PAYLOAD_VERSION_V1 &&
		 walidx_get_le32(copy + 60) != 0) ||
		fnv(copy, bytes) != stored_crc)
		return -1;
	*header_bytes = bytes;
	*record_bytes = version == WALIDX_SNAPSHOT_PAYLOAD_VERSION ?
		sizeof(WalIdxRec) : sizeof(WalIdxRecV1);
	*nrecords = walidx_get_le32(copy + 20);
	if (version == WALIDX_SNAPSHOT_PAYLOAD_VERSION_V2)
		*nrecords |= (uint64_t) walidx_get_le32(copy + 60) << 32;
	else if (version == WALIDX_SNAPSHOT_PAYLOAD_VERSION)
		*nrecords |= (uint64_t) walidx_get_le32(copy + 68) << 32;
	*source_offset = walidx_get_le64(copy + 48);
	*log_epoch = version >= WALIDX_SNAPSHOT_PAYLOAD_VERSION_V2 ?
		walidx_get_le64(copy + 56) : 0;
	if (version >= WALIDX_SNAPSHOT_PAYLOAD_VERSION_V2 &&
		(*log_epoch != generation || *source_offset != 0))
		return -1;
	return 0;
}

static int
walidx_snapshot_path(uint32_t tl, char *path, size_t path_len)
{
	int n = snprintf(path, path_len, "%s/walidx_snapshots_%u",
				 wal_segment_root, tl);

	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

typedef struct WalIdxDebtSnapshot
{
	uint64_t generation;
	uint64_t epochs[PS_MAX_CHANNELS];
	uint64_t covered_offsets[PS_MAX_CHANNELS];
	uint64_t observed_offsets[PS_MAX_CHANNELS];
	char directory[4096];
	int cleanup_pending;
	PsWalIdxSnapshotPrepared cleanup;
	int valid;
} WalIdxDebtSnapshot;

static int
walidx_prepared_identity_equal(const PsWalIdxSnapshotPrepared *a,
							   const PsWalIdxSnapshotPrepared *b)
{
	if (strcmp(a->directory, b->directory) != 0 ||
		a->timeline != b->timeline || a->nshards != b->nshards ||
		a->generation != b->generation || a->start_lsn != b->start_lsn ||
		a->end_lsn != b->end_lsn)
		return 0;
	for (uint32_t shard = 0; shard < a->nshards; shard++)
		if (a->shards[shard].len != b->shards[shard].len ||
			a->shards[shard].crc != b->shards[shard].crc)
			return 0;
	return 1;
}

/* Capture only a short, coherent logical identity.  Physical inspection is
 * deliberately performed after all of these locks are released. */
static int
walidx_debt_snapshot(uint32_t tl, WalIdxDebtSnapshot *snapshot)
{
	uint32_t ns = core_shards();
	int rc = 0;

	memset(snapshot, 0, sizeof(*snapshot));
	pthread_rwlock_rdlock(&walidx_prune_lock);
	ps_lock_map_rd();
	walidx_publish_wrlock();
	pthread_mutex_lock(&walidx_meta_lock);
	if (ps_timeline_live(tl))
	{
		snapshot->generation = walidx_snapshot_generation[tl];
		for (uint32_t shard = 0; shard < ns; shard++)
		{
			snapshot->epochs[shard] = walidx_log_epoch[tl][shard];
			snapshot->covered_offsets[shard] =
				walidx_snapshot_offsets[tl][shard];
			snapshot->observed_offsets[shard] =
				walidx_shard_offsets_seen[tl][shard];
		}
		snapshot->cleanup_pending =
			__atomic_load_n(&walidx_snapshot_cleanup_pending[tl], __ATOMIC_ACQUIRE);
		if (snapshot->cleanup_pending)
			snapshot->cleanup = walidx_snapshot_cleanup[tl];
		rc = walidx_snapshot_path(tl, snapshot->directory,
								  sizeof(snapshot->directory));
		if (rc == 0)
			snapshot->valid = 1;
	}
	pthread_mutex_unlock(&walidx_meta_lock);
	walidx_publish_wrunlock();
	ps_unlock_map();
	pthread_rwlock_unlock(&walidx_prune_lock);
	return rc;
}

static int
walidx_debt_snapshot_unchanged(uint32_t tl,
							   const WalIdxDebtSnapshot *snapshot)
{
	uint32_t ns = core_shards();
	int unchanged = 0;

	pthread_rwlock_rdlock(&walidx_prune_lock);
	ps_lock_map_rd();
	walidx_publish_wrlock();
	pthread_mutex_lock(&walidx_meta_lock);
	if (ps_timeline_live(tl) &&
		walidx_snapshot_generation[tl] == snapshot->generation)
	{
		unchanged = 1;
		if (snapshot->cleanup_pending !=
			__atomic_load_n(&walidx_snapshot_cleanup_pending[tl],
							__ATOMIC_ACQUIRE) ||
			(snapshot->cleanup_pending &&
			 !walidx_prepared_identity_equal(&snapshot->cleanup,
										  &walidx_snapshot_cleanup[tl])))
			unchanged = 0;
		for (uint32_t shard = 0; shard < ns; shard++)
			if (walidx_log_epoch[tl][shard] != snapshot->epochs[shard] ||
				walidx_snapshot_offsets[tl][shard] !=
				 snapshot->covered_offsets[shard] ||
				walidx_shard_offsets_seen[tl][shard] !=
				 snapshot->observed_offsets[shard])
			{
				unchanged = 0;
				break;
			}
	}
	pthread_mutex_unlock(&walidx_meta_lock);
	walidx_publish_wrunlock();
	ps_unlock_map();
	pthread_rwlock_unlock(&walidx_prune_lock);
	return unchanged;
}

static int
walidx_entry_prune_plan(const WalIdxEnt *e, uint64_t cutoff,
						const uint64_t *horizons, uint32_t nhorizons,
						unsigned char *keep)
{
	PsWalIdxPruneItem *items;
	int rc;

	if (e->n == 0)
		return 0;
	items = malloc((size_t) e->n * sizeof(*items));
	if (items == NULL)
		return -1;
	for (int i = 0; i < e->n; i++)
	{
		items[i].lsn = e->items[i].lsn;
		items[i].end_lsn = e->items[i].end_lsn;
		items[i].known =
			(e->items[i].flags & PS_WAL_INDEX_FLAG_KNOWN) != 0;
		items[i].fpi =
			(e->items[i].flags & PS_WAL_INDEX_FLAG_FPI) != 0;
	}
	rc = ps_walidx_prune_plan(items, (uint32_t) e->n, cutoff,
							  horizons, nhorizons, keep);
	free(items);
	return rc;
}

/* Prove every page before writing any compacted shard.  One unprovable page
 * keeps the whole timeline on its full snapshot generation, so the single
 * timeline frontier can never mask a lagging shard. */
static int
walidx_snapshot_compaction_plan(uint32_t tl, uint64_t cutoff,
								const uint64_t *horizons,
								uint32_t nhorizons, uint64_t *dropped_out)
{
	uint64_t dropped = 0;

	for (uint32_t shard = 0; shard < core_shards(); shard++)
	{
		Shard *s = &g_shards[shard];

		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (WalIdxEnt *e = s->walidx[bucket]; e; e = e->next)
				if (e->timeline == tl && e->n != 0)
				{
					unsigned char *keep = malloc((size_t) e->n);
					int kept;

					if (keep == NULL)
						return 0;
					kept = walidx_entry_prune_plan(e, cutoff, horizons,
											  nhorizons, keep);
					free(keep);
					if (kept < 0)
						return 0;
					dropped += (uint64_t) e->n - (uint64_t) kept;
				}
	}
	*dropped_out = dropped;
	return 1;
}

typedef struct WalIdxSnapshotProduceCtx
{
	uint32_t tl;
	uint32_t shard;
	uint64_t generation;
	uint64_t start_lsn;
	uint64_t end_lsn;
	uint64_t source_offset;
	uint64_t log_epoch;
	uint64_t nrecords;
	int compact;
	const uint64_t *horizons;
	uint32_t nhorizons;
} WalIdxSnapshotProduceCtx;

static int
walidx_snapshot_produce(void *arg, PsWalIdxSnapshotConsume consume,
						void *consume_arg)
{
	WalIdxSnapshotProduceCtx *ctx = arg;
	Shard *s = &g_shards[ctx->shard];
	unsigned char header[WALIDX_SNAPSHOT_PAYLOAD_BYTES];
	WalIdxRec records[1024];
	WalIdxEnt **heap;
	uint32_t heap_size = 0;
	uint64_t emitted = 0;
	size_t used = 0;

	walidx_snapshot_encode_header(header, ctx->tl, ctx->shard, ctx->nrecords,
							  ctx->generation, ctx->start_lsn, ctx->end_lsn,
							  ctx->source_offset, ctx->log_epoch);
	if (consume(consume_arg, header, sizeof(header)) != 0)
		return -1;
	/* One cursor per fixed hash bucket bounds serialization memory regardless
	 * of the number of live WAL-index records. */
	heap = malloc(IDX_BUCKETS * sizeof(*heap));
	if (heap == NULL)
		return -1;
	for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
	{
		WalIdxEnt *e = s->walidx[bucket];
		uint32_t pos;

		while (e != NULL && e->timeline != ctx->tl)
			e = e->next;
		if (e == NULL)
			continue;
		pos = heap_size++;
		while (pos != 0)
		{
			uint32_t parent = (pos - 1) / 2;

			if (walidx_entry_compare(heap[parent], e) <= 0)
				break;
			heap[pos] = heap[parent];
			pos = parent;
		}
		heap[pos] = e;
	}
	while (heap_size != 0)
	{
		WalIdxEnt *e = heap[0];
		WalIdxEnt *next = e->next;
		unsigned char *keep = NULL;

		while (next != NULL && next->timeline != ctx->tl)
			next = next->next;
		if (next == NULL)
			heap[0] = heap[--heap_size];
		else
			heap[0] = next;
		if (heap_size != 0)
		{
			uint32_t pos = 0;

			for (;;)
			{
				uint32_t left = pos * 2 + 1;
				uint32_t right = left + 1;
				uint32_t child;
				WalIdxEnt *value;

				if (left >= heap_size)
					break;
				child = right < heap_size &&
					walidx_entry_compare(heap[right], heap[left]) < 0 ?
					right : left;
				if (walidx_entry_compare(heap[pos], heap[child]) <= 0)
					break;
				value = heap[pos];
				heap[pos] = heap[child];
				heap[child] = value;
				pos = child;
			}
		}
		if (ctx->compact && e->n != 0)
		{
			int kept;

			keep = malloc((size_t) e->n);
			if (keep == NULL)
				goto fail;
			kept = walidx_entry_prune_plan(e, ctx->end_lsn,
										  ctx->horizons, ctx->nhorizons, keep);
			if (kept < 0)
			{
				free(keep);
				goto fail;
			}
		}
		for (int i = 0; i < e->n; i++)
			if (!ctx->compact || keep[i])
			{
				WalIdxRec *rec = &records[used++];

				memset(rec, 0, sizeof(*rec));
				rec->magic = WALIDX_MAGIC;
				rec->rec_len = sizeof(*rec);
				rec->timeline = ctx->tl;
				rec->block = e->block;
				rec->lsn = e->items[i].lsn;
				rec->end_lsn = e->items[i].end_lsn;
				rec->flags = e->items[i].flags;
				rec->key = e->key;
				rec->crc = walidx_rec_crc(rec);
				emitted++;
				if (used == sizeof(records) / sizeof(records[0]))
				{
					if (consume(consume_arg, records, sizeof(records)) != 0)
					{
						free(keep);
						goto fail;
					}
					used = 0;
				}
			}
		free(keep);
	}
	if (emitted != ctx->nrecords ||
		(used != 0 && consume(consume_arg, records,
								 used * sizeof(records[0])) != 0))
	{
		free(heap);
		return -1;
	}
	free(heap);
	return 0;

fail:
	free(heap);
	return -1;
}

static int
walidx_snapshot_prepare_shard(uint32_t tl, uint32_t shard,
							 uint64_t generation, uint64_t start_lsn,
								 uint64_t end_lsn, uint64_t source_offset,
								 uint64_t log_epoch,
								 int compact, const uint64_t *horizons,
								 uint32_t nhorizons,
								 WalIdxSnapshotProduceCtx *ctx,
							 PsWalIdxSnapshotInput *input)
{
	Shard *s = &g_shards[shard];
	uint64_t nrecords = 0;

	for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
		for (WalIdxEnt *e = s->walidx[bucket]; e; e = e->next)
			if (e->timeline == tl)
			{
				uint64_t kept = (uint64_t) e->n;

				if (compact && e->n != 0)
				{
					unsigned char *keep = malloc((size_t) e->n);
					int n;

					if (keep == NULL)
						return -1;
					n = walidx_entry_prune_plan(e, end_lsn, horizons,
													  nhorizons, keep);
					free(keep);
					if (n < 0)
						return -1;
					kept = (uint64_t) n;
				}
				if (UINT64_MAX - nrecords < kept)
					return -1;
				nrecords += kept;
			}
	if (nrecords > (UINT64_MAX - WALIDX_SNAPSHOT_PAYLOAD_BYTES) /
		sizeof(WalIdxRec) ||
		WALIDX_SNAPSHOT_PAYLOAD_BYTES + nrecords * sizeof(WalIdxRec) > INT64_MAX)
		return -1;
	*ctx = (WalIdxSnapshotProduceCtx) {
		tl, shard, generation, start_lsn, end_lsn, source_offset, log_epoch,
		nrecords, compact, horizons, nhorizons
	};
	*input = (PsWalIdxSnapshotInput) {
		NULL,
		WALIDX_SNAPSHOT_PAYLOAD_BYTES + nrecords * sizeof(WalIdxRec),
		walidx_snapshot_produce,
		ctx
	};
	return 0;
}

static void
walidx_prune_memory(uint32_t tl, uint64_t cutoff, const uint64_t *horizons,
					uint32_t nhorizons)
{
	for (uint32_t shard = 0; shard < core_shards(); shard++)
	{
		Shard *s = &g_shards[shard];

		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (WalIdxEnt *e = s->walidx[bucket]; e; e = e->next)
				if (e->timeline == tl && e->n != 0)
				{
					unsigned char *keep = malloc((size_t) e->n);
					int out = 0;

					if (keep == NULL ||
						walidx_entry_prune_plan(e, cutoff, horizons,
											 nhorizons, keep) < 0)
					{
						free(keep);
						continue;
					}
					for (int i = 0; i < e->n; i++)
						if (keep[i])
							e->items[out++] = e->items[i];
					e->n = out;
					free(keep);
				}
	}
}

static int
walidx_add_batch_locked(uint32_t tl, const PsWalIndexEntry *entries,
						uint32_t nentries)
{
	WalIdxRec  *records;
	uint32_t	nrecords = 0;
	uint32_t	shard;

	if (tl >= MAX_TIMELINES || nentries == 0)
		return -1;
	if (walidx_frontier_publication_pending(tl))
		return -1;
	shard = ps_shard_of(&entries[0].key);
	records = malloc((size_t) nentries * sizeof(*records));
	if (!records)
		return -1;
	for (uint32_t i = 0; i < nentries; i++)
	{
		WalIdxEnt  *e;
		WalIdxRec  *rec;
		int			pos;

		/* The caller holds this shard write lock and the WAL-index publish
		 * read gate.  Reclaim needs every shard write lock before its publish
		 * write gate, so the complete ancestry frontier cannot advance between
		 * this admission check and the durable batch append. */
		if (ps_shard_of(&entries[i].key) != shard ||
			!walidx_metadata_valid(entries[i].flags, entries[i].lsn,
								 entries[i].end_lsn) ||
			!wal_reclaim_frontier_ancestry_allows(tl, entries[i].lsn))
		{
			free(records);
			return -1;
		}
		e = walidx_find(tl, &entries[i].key, entries[i].block);
		if (e)
		{
			pos = walidx_lower_bound(e, entries[i].lsn);
			if (pos < e->n && e->items[pos].lsn == entries[i].lsn &&
				e->items[pos].end_lsn != 0 && entries[i].end_lsn != 0 &&
				e->items[pos].end_lsn != entries[i].end_lsn)
			{
				free(records);
				return -1;
			}
			if (pos < e->n && e->items[pos].lsn == entries[i].lsn &&
				(e->items[pos].flags | entries[i].flags) == e->items[pos].flags &&
				(e->items[pos].end_lsn != 0 || entries[i].end_lsn == 0))
				continue;
		}
		rec = &records[nrecords++];
		memset(rec, 0, sizeof(*rec));
		rec->magic = WALIDX_MAGIC;
		rec->rec_len = sizeof(*rec);
		rec->crc = 0;
		rec->flags = entries[i].flags;
		rec->timeline = tl;
		rec->block = entries[i].block;
		rec->lsn = entries[i].lsn;
		rec->end_lsn = entries[i].end_lsn;
		rec->key = entries[i].key;
		rec->crc = walidx_rec_crc(rec);
	}
	if (nrecords == 0)
	{
		free(records);
		return 0;
	}
	if (ps_storage->walidx_append(tl, shard, walidx_log_epoch[tl][shard], records,
								(uint32_t) (nrecords * sizeof(*records))) != 0)
	{
		free(records);
		return -1;
	}
	pthread_mutex_lock(&walidx_meta_lock);
	walidx_shard_offsets_seen[tl][shard] += nrecords * sizeof(*records);
	walidx_mark_shard(walidx_shards_seen[tl], shard);
	pthread_mutex_unlock(&walidx_meta_lock);
	for (uint32_t i = 0; i < nrecords; i++)
		if (walidx_add_memory(tl, &records[i].key, records[i].block,
							  records[i].lsn, records[i].end_lsn,
							  records[i].flags) != 0)
		{
			free(records);
			return -1;
		}
	free(records);
	return 0;
}

static int
walidx_add(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	PsWalIndexEntry entry;
	int			rc;

	entry.key = *key;
	entry.block = block;
	entry.flags = 0;
	entry.lsn = lsn;
	entry.end_lsn = 0;
	walidx_publish_rdlock();
	rc = walidx_add_batch_locked(tl, &entry, 1);
	walidx_publish_rdunlock();
	return rc;
}

static int
walidx_snapshot_recover(uint32_t tl)
{
	PsWalIdxSnapshot snapshot;
	char directory[4096];
	char manifest[4096];
	struct stat st;
	uint64_t coverage_start;
	uint64_t first;
	uint64_t retained_base = 0;
	int reshard;
	int n;

	if (walidx_snapshot_path(tl, directory, sizeof(directory)) != 0)
		return -1;
	n = snprintf(manifest, sizeof(manifest), "%s/walidx_manifest_v1", directory);
	if (n < 0 || (size_t) n >= sizeof(manifest))
		return -1;
	if (lstat(manifest, &st) != 0)
		return errno == ENOENT ? 0 : -1;
	if (ps_walidx_snapshot_open(&snapshot, directory, tl) != 0)
		return -1;
	first = wal_log_start(tl);
	coverage_start = snapshot.start_lsn;
	if (wal_segment_store_opened[tl])
	{
		if (ps_wal_store_retained_base(&wal_segment_stores[tl],
										&retained_base) != 0)
			goto fail;
		if (coverage_start < retained_base)
			coverage_start = retained_base;
	}
	reshard = snapshot.nshards == 1 && core_shards() > 1;
	if ((snapshot.nshards != core_shards() && !reshard) ||
		(snapshot.start_lsn != first &&
		 (!wal_segment_store_opened[tl] ||
		  snapshot.start_lsn >= retained_base ||
		  first < snapshot.start_lsn || first > retained_base)) ||
		snapshot.end_lsn > wal_end_read(tl) ||
		(coverage_start < snapshot.end_lsn &&
		 !wal_coverage_advance(tl, coverage_start, snapshot.end_lsn)))
		goto fail;
	for (uint32_t shard = 0; shard < snapshot.nshards; shard++)
	{
		unsigned char header[WALIDX_SNAPSHOT_PAYLOAD_BYTES];
		unsigned char records[1024 * sizeof(WalIdxRec)];
		uint32_t header_bytes;
		uint32_t record_bytes;
		uint64_t nrecords;
		uint64_t source_offset;
		uint64_t log_epoch;
		uint64_t expected_len;
		uint64_t done = 0;
		uint32_t header_read = snapshot.shards[shard].len < sizeof(header) ?
			(uint32_t) snapshot.shards[shard].len : (uint32_t) sizeof(header);

		memset(header, 0, sizeof(header));
		if (snapshot.shards[shard].len < WALIDX_SNAPSHOT_PAYLOAD_BYTES_V1 ||
			ps_walidx_snapshot_read(&snapshot, shard, 0, header,
								 header_read) != 0 ||
			walidx_snapshot_decode_header(header, snapshot.shards[shard].len,
								 tl, shard, snapshot.generation,
								 snapshot.start_lsn, snapshot.end_lsn,
								 &header_bytes, &record_bytes, &nrecords,
								 &source_offset,
								 &log_epoch) != 0 ||
			nrecords > (UINT64_MAX - header_bytes) / record_bytes)
			goto fail;
		expected_len = header_bytes +
			 nrecords * record_bytes;
		if (expected_len != snapshot.shards[shard].len)
			goto fail;
		while (done < nrecords)
		{
			uint32_t amount = nrecords - done <
				(sizeof(records) / record_bytes) ?
				(uint32_t) (nrecords - done) :
				(uint32_t) (sizeof(records) / record_bytes);

			if (ps_walidx_snapshot_read(&snapshot, shard,
					header_bytes + done * record_bytes,
					records, amount * record_bytes) != 0)
				goto fail;
			for (uint32_t i = 0; i < amount; i++)
			{
				const unsigned char *raw = records + (size_t) i * record_bytes;

				if (record_bytes == sizeof(WalIdxRec))
				{
					WalIdxRec rec;

					memcpy(&rec, raw, sizeof(rec));
					if (rec.magic != WALIDX_MAGIC || rec.rec_len != sizeof(rec) ||
						rec.timeline != tl || rec.crc != walidx_rec_crc(&rec) ||
						(!reshard && ps_shard_of(&rec.key) != shard) ||
						!walidx_metadata_valid(rec.flags, rec.lsn, rec.end_lsn) ||
						walidx_add_memory(tl, &rec.key, rec.block, rec.lsn,
										  rec.end_lsn, rec.flags) != 0)
						goto fail;
				}
				else
				{
					WalIdxRecV1 rec;

					memcpy(&rec, raw, sizeof(rec));
					if (rec.magic != WALIDX_MAGIC || rec.rec_len != sizeof(rec) ||
						rec.reserved != 0 || rec.timeline != tl ||
						rec.crc != walidx_rec_v1_crc(&rec) ||
						(!reshard && ps_shard_of(&rec.key) != shard) ||
						walidx_add_memory(tl, &rec.key, rec.block, rec.lsn,
										  0, 0) != 0)
						goto fail;
				}
			}
			done += amount;
		}
		walidx_log_epoch[tl][shard] = log_epoch;
		walidx_snapshot_offsets[tl][shard] = source_offset;
		walidx_snapshot_bytes[tl] =
			UINT64_MAX - walidx_snapshot_bytes[tl] < snapshot.shards[shard].len ?
			UINT64_MAX : walidx_snapshot_bytes[tl] + snapshot.shards[shard].len;
		walidx_shard_offsets_seen[tl][shard] = source_offset;
		walidx_shard_offsets_required[tl][shard] = source_offset;
		if (source_offset != 0)
		{
			walidx_mark_shard(walidx_shards_seen[tl], shard);
			walidx_mark_shard(walidx_shards_required[tl], shard);
		}
	}
	walidx_snapshot_generation[tl] = snapshot.generation;
	walidx_snapshot_start[tl] = snapshot.start_lsn;
	walidx_snapshot_end[tl] = snapshot.end_lsn;
	walidx_snapshot_reshard_pending[tl] = (unsigned char) reshard;
	walidx_snapshot_gc_pending[tl] = 1;
	walidx_progress[tl] = snapshot.end_lsn;
	walidx_progress_valid[tl] = 1;
	walidx_progress_durable[tl] = 1;
	ps_walidx_snapshot_close(&snapshot);
	return 0;

fail:
	ps_walidx_snapshot_close(&snapshot);
	return -1;
}

static uint64_t
walidx_snapshot_trigger_bytes(void)
{
	const char *value = getenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES");
	unsigned long long parsed;
	char *end = NULL;

	if (value == NULL)
		return WALIDX_SNAPSHOT_DEFAULT_TRIGGER;
	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' || parsed == 0)
		return WALIDX_SNAPSHOT_DEFAULT_TRIGGER;
	return (uint64_t) parsed;
}

static uint64_t
walidx_snapshot_test_max_generation(void)
{
	const char *value = getenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_MAX_GENERATION");
	unsigned long long parsed;
	char *end = NULL;

	if (value == NULL)
		return UINT64_MAX;
	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0')
		return UINT64_MAX;
	return (uint64_t) parsed;
}

static int
walidx_snapshot_publish_one(void)
{
	PsWalIdxSnapshotInput inputs[PS_MAX_CHANNELS];
	WalIdxSnapshotProduceCtx producers[PS_MAX_CHANNELS];
	PsWalIdxSnapshotPrepared prepared;
	uint64_t *fences = NULL;
	uint32_t nfences = 0;
	uint64_t trigger = walidx_snapshot_trigger_bytes();
	uint32_t ns = core_shards();
	struct timespec now;
	int candidate = -1;
	int retry = 0;
	int rc = 0;
	clock_gettime(CLOCK_MONOTONIC, &now);
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (__atomic_load_n(&walidx_snapshot_cleanup_pending[tl],
							__ATOMIC_ACQUIRE) &&
			(now.tv_sec > walidx_snapshot_cleanup_retry_at[tl].tv_sec ||
			 (now.tv_sec == walidx_snapshot_cleanup_retry_at[tl].tv_sec &&
			  now.tv_nsec >= walidx_snapshot_cleanup_retry_at[tl].tv_nsec)))
		{
			if (ps_walidx_snapshot_abort(&walidx_snapshot_cleanup[tl]) == 0)
			{
				memset(&walidx_snapshot_cleanup[tl], 0,
					   sizeof(walidx_snapshot_cleanup[tl]));
				memset(&walidx_snapshot_cleanup_retry_at[tl], 0,
					   sizeof(walidx_snapshot_cleanup_retry_at[tl]));
				__atomic_store_n(&walidx_snapshot_cleanup_pending[tl], 0,
								 __ATOMIC_RELEASE);
				return 1;
			}
			walidx_snapshot_cleanup_retry_at[tl] = now;
			walidx_snapshot_cleanup_retry_at[tl].tv_sec++;
		}
	pthread_mutex_lock(&walidx_meta_lock);
	for (uint32_t step = 0; step < MAX_TIMELINES; step++)
	{
		uint32_t tl = (walidx_snapshot_cursor + step) % MAX_TIMELINES;
		struct timespec retry_at = walidx_snapshot_retry_at[tl];
		int force_due = __atomic_load_n(&walidx_snapshot_force_due[tl],
											 __ATOMIC_ACQUIRE);

		if (ps_timeline_live(tl) &&
				(now.tv_sec > retry_at.tv_sec ||
			 (now.tv_sec == retry_at.tv_sec && now.tv_nsec >= retry_at.tv_nsec)) &&
			!__atomic_load_n(&walidx_snapshot_cleanup_pending[tl],
							 __ATOMIC_ACQUIRE) &&
				(walidx_snapshot_reshard_pending[tl] ||
				 walidx_progress[tl] > walidx_snapshot_end[tl] || force_due))
		{
			uint64_t tail = 0;
			uint64_t threshold =
				UINT64_MAX - trigger < walidx_snapshot_bytes[tl] ?
				UINT64_MAX : trigger + walidx_snapshot_bytes[tl];
			int invalid = 0;

			for (uint32_t shard = 0; shard < ns; shard++)
			{
				uint64_t seen = walidx_shard_offsets_seen[tl][shard];
				uint64_t snap = walidx_snapshot_offsets[tl][shard];

				if (seen < snap)
				{
					invalid = 1;
					break;
				}
				tail = UINT64_MAX - tail < seen - snap ?
					UINT64_MAX : tail + seen - snap;
			}
			/* Full snapshots grow geometrically with the already snapshotted
			 * log, bounding retained generations and total rewrite I/O. */
			if (!invalid && (walidx_snapshot_end[tl] < walidx_frontier_current(tl) ||
						 walidx_snapshot_reshard_pending[tl] || force_due ||
						 tail >= threshold))
			{
				candidate = (int) tl;
				walidx_snapshot_cursor = (tl + 1) % MAX_TIMELINES;
				break;
			}
		}
	}
	pthread_mutex_unlock(&walidx_meta_lock);
	if (candidate < 0)
		return 0;

	pthread_rwlock_rdlock(&walidx_prune_lock);
	ps_lock_map_rd();
	/* The first scan is a scheduling hint.  Recheck the timeline state while
	 * holding map-rd before any snapshot publication is prepared. */
	if (!ps_timeline_live((uint32_t) candidate))
	{
		ps_unlock_map();
		pthread_rwlock_unlock(&walidx_prune_lock);
		return 0;
	}
	walidx_publish_wrlock();
	{
		uint32_t tl = (uint32_t) candidate;
		uint64_t generation;
		uint64_t start_lsn;
		uint64_t end_lsn;
		uint64_t previous_end;
		uint64_t snapshot_bytes = 0;
		uint64_t dropped = 0;
		char directory[4096];
		PsWalIdxSnapshotPrepared staged;
		int compact = 0;
		int frontier_pending;
		int prepared_generation;
		int force_due = __atomic_load_n(&walidx_snapshot_force_due[tl],
											 __ATOMIC_ACQUIRE);

		pthread_mutex_lock(&walidx_meta_lock);
		start_lsn = walidx_snapshot_generation[tl] != 0 ?
			walidx_snapshot_start[tl] : wal_log_start(tl);
		end_lsn = walidx_progress[tl];
		previous_end = walidx_snapshot_end[tl];
		frontier_pending = previous_end < walidx_frontier_current(tl);
		pthread_mutex_unlock(&walidx_meta_lock);
		if (start_lsn == UINT64_MAX ||
			(!walidx_snapshot_reshard_pending[tl] &&
				 (end_lsn < previous_end ||
				  (end_lsn == previous_end && !force_due))) ||
			(walidx_snapshot_reshard_pending[tl] && end_lsn < previous_end))
			goto publish_done;
		if (walidx_snapshot_path(tl, directory, sizeof(directory)) != 0)
		{
			retry = 1;
			goto publish_done;
		}
		/* Reconcile an intent left by a failed prepare before allocating a new
		 * generation.  A frontier-covered intent remains the authoritative retry
		 * input; do not rebuild it with a changed shard layout. */
		prepared_generation =
			ps_walidx_snapshot_read_prepared(directory, tl, &staged);
		if (prepared_generation < 0)
		{
			if (ps_walidx_snapshot_recover_prepared(directory, tl,
											 walidx_frontier_current(tl)) != 0)
			{
				retry = 1;
				goto publish_done;
			}
		}
		else if (prepared_generation == 1)
		{
			if (!frontier_pending)
			{
				retry = ps_walidx_snapshot_abort(&staged) != 0;
				goto publish_done;
			}
			if (staged.nshards != ns)
			{
				retry = 1;
				goto publish_done;
			}
		}
		if (frontier_pending)
		{
			prepared_generation =
				ps_walidx_snapshot_prepared_generation(directory, tl,
											 &generation);
			if (prepared_generation < 0 ||
				(prepared_generation == 0 &&
				 walidx_snapshot_generation[tl] == UINT64_MAX))
			{
				retry = 1;
				goto publish_done;
			}
			if (prepared_generation == 0)
				generation = walidx_snapshot_generation[tl] + 1;
		}
		else if (ps_walidx_snapshot_next_generation(directory,
					walidx_snapshot_generation[tl], &generation) != 0)
		{
			retry = 1;
			goto publish_done;
		}
		if (generation > walidx_snapshot_test_max_generation())
			goto publish_done;
		if (walidx_prune_fences(tl, &fences, &nfences) != 0)
			goto publish_done;
		compact = walidx_snapshot_compaction_plan(tl, end_lsn, fences,
											nfences, &dropped) && dropped != 0;
		for (uint32_t shard = 0; shard < ns; shard++)
		{
			if (walidx_snapshot_prepare_shard(tl, shard, generation,
											start_lsn, end_lsn, 0, generation,
											compact, fences, nfences,
											&producers[shard], &inputs[shard]) != 0)
			{
				retry = 1;
				goto publish_done;
			}
			snapshot_bytes = UINT64_MAX - snapshot_bytes < inputs[shard].len ?
				UINT64_MAX : snapshot_bytes + inputs[shard].len;
		}
		if (ps_storage->walidx_epoch_create == NULL)
		{
			retry = 1;
			goto publish_done;
		}
		for (uint32_t shard = 0; shard < ns; shard++)
			if (ps_storage->walidx_epoch_create(tl, shard, generation) != 0)
			{
				retry = 1;
				goto publish_done;
			}
		if (compact)
		{
			if (ps_walidx_snapshot_prepare(&prepared, directory, tl, generation,
													start_lsn, end_lsn, inputs, ns) != 0)
			{
				retry = 1;
				goto publish_done;
			}
			if (walidx_frontier_advance(tl, end_lsn) != 0)
			{
				walidx_snapshot_cleanup[tl] = prepared;
				__atomic_store_n(&walidx_snapshot_cleanup_pending[tl], 1,
								 __ATOMIC_RELEASE);
				if (ps_walidx_snapshot_abort(&walidx_snapshot_cleanup[tl]) == 0)
				{
					memset(&walidx_snapshot_cleanup[tl], 0,
						   sizeof(walidx_snapshot_cleanup[tl]));
					__atomic_store_n(&walidx_snapshot_cleanup_pending[tl], 0,
									 __ATOMIC_RELEASE);
				}
				retry = 1;
				goto publish_done;
			}
			if (ps_fault_probe(PS_FAULT_POINT_WAL_INDEX_AFTER_FRONTIER) != 0)
				goto publish_done;
			if (ps_walidx_snapshot_commit(&prepared) != 0)
			{
				retry = 1;
				goto publish_done;
			}
			walidx_prune_memory(tl, end_lsn, fences, nfences);
		}
		else if (ps_walidx_snapshot_publish(directory, tl, generation,
											start_lsn, end_lsn, inputs, ns) != 0)
		{
			int discard = ps_walidx_snapshot_discard_generation(directory, tl,
															generation, ns);

			if (discard != 1)
			{
				retry = 1;
				goto publish_done;
			}
		}
		pthread_mutex_lock(&walidx_meta_lock);
		walidx_snapshot_generation[tl] = generation;
		walidx_snapshot_start[tl] = start_lsn;
		walidx_snapshot_end[tl] = end_lsn;
		walidx_snapshot_bytes[tl] = snapshot_bytes;
		walidx_snapshot_reshard_pending[tl] = 0;
		memset(&walidx_snapshot_retry_at[tl], 0,
			   sizeof(walidx_snapshot_retry_at[tl]));
		walidx_shards_seen[tl][0] = 0;
		walidx_shards_seen[tl][1] = 0;
		walidx_shards_required[tl][0] = 0;
		walidx_shards_required[tl][1] = 0;
		for (uint32_t shard = 0; shard < ns; shard++)
		{
			walidx_log_epoch[tl][shard] = generation;
			walidx_snapshot_offsets[tl][shard] = 0;
			walidx_shard_offsets_seen[tl][shard] = 0;
			walidx_shard_offsets_required[tl][shard] = 0;
		}
		walidx_snapshot_gc_pending[tl] = 1;
		pthread_mutex_unlock(&walidx_meta_lock);
		/* A forced publication consumed the observed append-tail frontier.
		 * Clear only after the durable metadata update succeeds; a failed
		 * publication must remain eligible for retry.  Request an immediate
		 * post-maintenance observation so the new physical debt is measured. */
		__atomic_store_n(&walidx_snapshot_force_due[tl], 0, __ATOMIC_RELEASE);
		__atomic_store_n(&walidx_observation_next_ns, 0, __ATOMIC_RELEASE);
		rc = 1;
	}

publish_done:
	if (retry && candidate >= 0)
	{
		clock_gettime(CLOCK_MONOTONIC, &now);
		walidx_snapshot_retry_at[candidate] = now;
		walidx_snapshot_retry_at[candidate].tv_sec++;
	}
	walidx_publish_wrunlock();
	free(fences);
	ps_unlock_map();
	pthread_rwlock_unlock(&walidx_prune_lock);
	return rc;
}

static int
walidx_snapshot_gc_one(void)
{
	int candidate = -1;
	int did = 0;
	int rc;
	char directory[4096];
	struct timespec now;

	clock_gettime(CLOCK_MONOTONIC, &now);
	ps_lock_map_rd();
	for (uint32_t step = 0; step < MAX_TIMELINES; step++)
	{
		uint32_t tl = (walidx_snapshot_gc_cursor + step) % MAX_TIMELINES;
		struct timespec retry = walidx_snapshot_gc_retry_at[tl];

		if (ps_timeline_live(tl) &&
			(walidx_snapshot_gc_pending[tl] ||
			 __atomic_load_n(&walidx_snapshot_gc_force_due[tl], __ATOMIC_ACQUIRE)) &&
			(now.tv_sec > retry.tv_sec ||
			 (now.tv_sec == retry.tv_sec && now.tv_nsec >= retry.tv_nsec)))
		{
			candidate = (int) tl;
			walidx_snapshot_gc_cursor = (tl + 1) % MAX_TIMELINES;
			break;
		}
	}
	ps_unlock_map();
	if (candidate < 0)
		return 0;
	if (walidx_snapshot_path((uint32_t) candidate, directory,
							 sizeof(directory)) != 0)
		rc = -1;
	else
		rc = ps_walidx_snapshot_gc(directory, (uint32_t) candidate);
	if (rc > 0)
		did = 1;
	if (rc >= 0 && ps_storage->walidx_epoch_gc != NULL)
	{
		rc = ps_storage->walidx_epoch_gc((uint32_t) candidate,
								walidx_log_epoch[candidate], core_shards());
		if (rc > 0)
			did = 1;
	}
	if (rc >= 0)
	{
		walidx_snapshot_gc_pending[candidate] = 0;
		__atomic_store_n(&walidx_snapshot_gc_force_due[candidate], 0,
						  __ATOMIC_RELEASE);
		/* GC changed the physical debt identity.  Re-evaluate it on the
		 * maintenance return path even when the normal WAL-index observation
		 * interval has not elapsed. */
		__atomic_store_n(&walidx_observation_next_ns, 0, __ATOMIC_RELEASE);
		memset(&walidx_snapshot_gc_retry_at[candidate], 0,
			   sizeof(walidx_snapshot_gc_retry_at[candidate]));
	}
	else
	{
		walidx_snapshot_gc_retry_at[candidate] = now;
		walidx_snapshot_gc_retry_at[candidate].tv_sec++;
	}
	return did;
}

static int
walidx_recover_one(uint32_t tl, uint32_t shard)
{
	uint64_t	read_off;
	uint64_t	good_off;
	unsigned char buf[PS_IO_UNIT];
	int			used = 0;
	int			torn = 0;

	if (tl >= MAX_TIMELINES || shard >= PS_MAX_CHANNELS ||
		shard >= core_shards())
		return -1;
	read_off = walidx_snapshot_offsets[tl][shard];
	good_off = read_off;
	if (read_off != 0)
	{
		unsigned char byte;

		if (ps_storage->walidx_read(tl, shard, walidx_log_epoch[tl][shard],
									read_off - 1, &byte, 1) != 1)
			return -1;
	}
	for (;;)
	{
		int			n;
		int			want = (int) sizeof(buf) - used;
		int			pos = 0;

		n = ps_storage->walidx_read(tl, shard, walidx_log_epoch[tl][shard],
									read_off, buf + used,
									(uint32_t) want);
		if (n == 0)
		{
			if (used != 0)
				torn = 1;
			break;
		}
		if (n < 0)
		{
			/* Epoch zero is the legacy lazy-created log.  A selected nonzero
			 * epoch was durably prepared before its manifest, so even an empty
			 * epoch must exist; accepting ENOENT could silently lose a shard-0
			 * progress tail before it tells us which other shards are required. */
			if (errno == ENOENT && walidx_log_epoch[tl][shard] == 0 &&
				!walidx_shard_marked(walidx_shards_required[tl], shard))
				return 0;
			return -1;
		}
		read_off += (uint64_t) n;
		used += n;

		while (used - pos >= (int) sizeof(WalIdxLogHdr))
		{
			WalIdxLogHdr hdr;
			uint32_t	rec_len;

			memcpy(&hdr, buf + pos, sizeof(hdr));
			if (hdr.magic == WALIDX_MAGIC &&
				(hdr.rec_len == sizeof(WalIdxRec) ||
				 hdr.rec_len == sizeof(WalIdxRecV1)))
				rec_len = hdr.rec_len;
			else if (shard == 0 && hdr.magic == WALIDX_PROGRESS_MAGIC &&
					 hdr.rec_len == sizeof(WalIdxProgressRec))
				rec_len = sizeof(WalIdxProgressRec);
			else
				return -1;
			if (used - pos < (int) rec_len)
				break;

			if (hdr.magic == WALIDX_MAGIC)
			{
				if (rec_len == sizeof(WalIdxRec))
				{
					WalIdxRec rec;

					memcpy(&rec, buf + pos, sizeof(rec));
					if (rec.magic != WALIDX_MAGIC || rec.rec_len != sizeof(rec) ||
						rec.timeline != tl || rec.crc != walidx_rec_crc(&rec) ||
						!walidx_metadata_valid(rec.flags, rec.lsn, rec.end_lsn))
						return -1;
					if (walidx_add_memory(tl, &rec.key, rec.block, rec.lsn,
										  rec.end_lsn, rec.flags) != 0)
						return -1;
				}
				else
				{
					WalIdxRecV1 rec;

					memcpy(&rec, buf + pos, sizeof(rec));
					if (rec.magic != WALIDX_MAGIC || rec.rec_len != sizeof(rec) ||
						rec.reserved != 0 || rec.timeline != tl ||
						rec.crc != walidx_rec_v1_crc(&rec))
						return -1;
					if (walidx_add_memory(tl, &rec.key, rec.block, rec.lsn, 0, 0) != 0)
						return -1;
				}
				walidx_mark_shard(walidx_shards_seen[tl], shard);
			}
			else
			{
				WalIdxProgressRec rec;
				uint64_t	first;

				memcpy(&rec, buf + pos, sizeof(rec));
				first = wal_log_start(tl);
				if (!walidx_progress_valid[tl] && first != UINT64_MAX)
				{
					walidx_progress[tl] = first == 0 ? rec.start_lsn : first;
					walidx_progress_valid[tl] = 1;
				}
				/* A progress marker can begin in a flat-WAL prefix that was
				 * already reclaimed before this restart.  walidx_progress_init()
				 * necessarily seeded the process-local value from the surviving
				 * physical start, so let the first durable marker restore its true
				 * historical start before validating the record. */
				if (!walidx_progress_durable[tl] &&
					wal_segment_store_opened[tl] &&
					rec.start_lsn < wal_segment_stores[tl].start_lsn &&
					(walidx_progress[tl] == wal_segment_stores[tl].start_lsn ||
					 (wal_chunks_n[tl] != 0 &&
					  wal_chunks[tl][0].start_lsn == walidx_progress[tl] &&
					  wal_chunks[tl][0].start_lsn <
						wal_segment_stores[tl].start_lsn &&
					  wal_chunks[tl][0].end_lsn >
						wal_segment_stores[tl].start_lsn)))
					walidx_progress[tl] = rec.start_lsn;
				if (rec.magic != WALIDX_PROGRESS_MAGIC ||
					rec.rec_len != sizeof(rec) || rec.timeline != tl ||
					rec.crc != walidx_progress_crc(&rec) ||
					!walidx_progress_valid[tl] ||
					rec.start_lsn != walidx_progress[tl] ||
					rec.end_lsn < rec.start_lsn ||
					rec.end_lsn > wal_end_read(tl) ||
					!walidx_mask_valid_for_shards(rec.shard_mask) ||
					!walidx_offsets_valid_for_shards(rec.shard_offsets) ||
					!wal_coverage_advance(tl, rec.start_lsn, rec.end_lsn))
					return -1;
				walidx_shards_required[tl][0] |= rec.shard_mask[0];
				walidx_shards_required[tl][1] |= rec.shard_mask[1];
				for (uint32_t i = 0; i < core_shards(); i++)
					if (rec.shard_offsets[i] >
						walidx_shard_offsets_required[tl][i])
						walidx_shard_offsets_required[tl][i] =
							rec.shard_offsets[i];
				walidx_progress[tl] = rec.end_lsn;
				walidx_progress_valid[tl] = 1;
				walidx_progress_durable[tl] = 1;
			}
			pos += (int) rec_len;
			good_off += rec_len;
		}
		if (pos != 0)
		{
			used -= pos;
			if (used != 0)
				memmove(buf, buf + pos, (size_t) used);
		}
		if (n < want)
		{
			if (used != 0)
				torn = 1;
			break;
		}
	}
	if (good_off < walidx_shard_offsets_required[tl][shard])
		return -1;
	walidx_shard_offsets_seen[tl][shard] = good_off;
	/* Do not let a torn/corrupt suffix become a permanent replay barrier. */
	if (torn && ps_storage->walidx_truncate(tl, shard,
									walidx_log_epoch[tl][shard], good_off) != 0)
		return -1;
	return 0;
}

static int
walidx_commit(uint32_t tl, uint64_t start_lsn, uint64_t end_lsn)
{
	WalIdxProgressRec rec;
	pthread_rwlock_t *wal_lock;
	uint64_t	current;
	uint64_t	first;
	int			rc = -1;
	int			append_progress = 0;
	int			current_valid;

	/* A durable marker must name a contiguous prefix of shipped WAL. */
	if (tl >= MAX_TIMELINES)
		return -1;
	walidx_publish_wrlock();
	/* Compaction may have published a frontier while this request waited. */
	if (walidx_frontier_publication_pending(tl))
	{
		walidx_publish_wrunlock();
		return -1;
	}
	wal_lock = wal_log_lock_for(tl);
	if (wal_lock == NULL)
	{
		walidx_publish_wrunlock();
		return -1;
	}
	pthread_rwlock_rdlock(wal_lock);
	pthread_mutex_lock(&walidx_meta_lock);
	current = walidx_progress[tl];
	current_valid = walidx_progress_valid[tl];
	if (!current_valid)
	{
		first = wal_log_start(tl);
		if (first != UINT64_MAX)
		{
			current = first;
			current_valid = 1;
		}
	}
	if (!current_valid || start_lsn != current || end_lsn < start_lsn ||
		end_lsn > wal_end_read(tl) ||
		!wal_coverage_advance(tl, start_lsn, end_lsn))
		goto out;
	memset(&rec, 0, sizeof(rec));
	rec.magic = WALIDX_PROGRESS_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.crc = 0;
	rec.timeline = tl;
	rec.start_lsn = current;
	rec.end_lsn = end_lsn;
	rec.shard_mask[0] = walidx_shards_seen[tl][0];
	rec.shard_mask[1] = walidx_shards_seen[tl][1];
	for (uint32_t shard = 0; shard < core_shards(); shard++)
		rec.shard_offsets[shard] = walidx_shard_offsets_seen[tl][shard];
	rec.crc = walidx_progress_crc(&rec);
	append_progress = 1;
out:
	pthread_mutex_unlock(&walidx_meta_lock);
	if (!append_progress)
	{
		pthread_rwlock_unlock(wal_lock);
		walidx_publish_wrunlock();
		return -1;
	}
	if (ps_storage->walidx_append(tl, 0, walidx_log_epoch[tl][0],
								&rec, sizeof(rec)) != 0)
	{
		pthread_rwlock_unlock(wal_lock);
		walidx_publish_wrunlock();
		return -1;
	}
	pthread_mutex_lock(&walidx_meta_lock);
	current = walidx_progress[tl];
	current_valid = walidx_progress_valid[tl];
	if (!current_valid)
	{
		first = wal_log_start(tl);
		if (first != UINT64_MAX)
		{
			current = first;
			current_valid = 1;
		}
	}
	if (!current_valid || current != rec.start_lsn)
		goto out_update;
	walidx_shard_offsets_seen[tl][0] += sizeof(rec);
	walidx_shards_required[tl][0] |= rec.shard_mask[0];
	walidx_shards_required[tl][1] |= rec.shard_mask[1];
	for (uint32_t shard = 0; shard < core_shards(); shard++)
		if (rec.shard_offsets[shard] > walidx_shard_offsets_required[tl][shard])
			walidx_shard_offsets_required[tl][shard] = rec.shard_offsets[shard];
	walidx_progress[tl] = rec.end_lsn;
	walidx_progress_valid[tl] = 1;
	walidx_progress_durable[tl] = 1;
	rc = 0;
out_update:
	pthread_mutex_unlock(&walidx_meta_lock);
	pthread_rwlock_unlock(wal_lock);
	walidx_publish_wrunlock();
	publish_wal_index_metrics();
	return rc;
}

static uint64_t
walidx_progress_read(uint32_t tl)
{
	uint64_t	progress;

	pthread_mutex_lock(&walidx_meta_lock);
	progress = walidx_progress[tl];
	pthread_mutex_unlock(&walidx_meta_lock);
	return progress;
}

static int
walidx_upper_bound(WalIdxEnt *e, uint64_t lsn)
{
	int			lo = 0;
	int			hi = e->n;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (e->items[mid].lsn <= lsn)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

/*
 * Merge the already-sorted per-timeline arrays directly into one bounded
 * response page.  A cursor request neither allocates nor scans the remaining
 * history beyond the next max_out records.
 */
static int
walidx_get(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn_max,
		   int have_cursor, uint64_t cursor_lsn, uint32_t cursor_timeline,
		   PsWalRec *out, int max_out)
{
	typedef struct WalIdxSource
	{
		WalIdxEnt  *entry;
		uint32_t	timeline;
		int			pos;
		int			end;
	} WalIdxSource;
	WalIdxSource sources[MAX_TIMELINES];
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	int			nsources = 0;
	int			nout = 0;
	TlWalk		w = tl_walk_first(tl, lsn_max);

	if (!walidx_frontier_ancestry_allows(tl, lsn_max))
		return -1;

	do
	{
		uint32_t	h = page_hash(w.tl, key, block);
		uint64_t	visible_end = walidx_progress_read(w.tl);
		WalIdxEnt  *e;

		/* Entries become queryable only with their durable progress marker. */
		if (visible_end == 0)
			continue;
		for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
			if (e->timeline == w.tl && e->block == block && key_eq(&e->key, key))
			{
				int			pos = have_cursor ?
					walidx_lower_bound(e, cursor_lsn) : 0;
				uint64_t	cap_lsn = w.lsn < visible_end - 1 ?
					w.lsn : visible_end - 1;
				int			end = walidx_upper_bound(e, cap_lsn);

				if (have_cursor && pos < end && e->items[pos].lsn == cursor_lsn &&
					w.tl <= cursor_timeline)
					pos++;
				if (pos < end)
				{
					sources[nsources].entry = e;
					sources[nsources].timeline = w.tl;
					sources[nsources].pos = pos;
					sources[nsources].end = end;
					nsources++;
				}
				break;
			}
	} while (tl_walk_next(&w));

	while (nout < max_out)
	{
		int			best = -1;

		for (int i = 0; i < nsources; i++)
		{
			uint64_t	lsn;
			uint64_t	best_lsn;

			if (sources[i].pos >= sources[i].end)
				continue;
			lsn = sources[i].entry->items[sources[i].pos].lsn;
			if (best < 0)
			{
				best = i;
				continue;
			}
			best_lsn = sources[best].entry->items[sources[best].pos].lsn;
			if (lsn < best_lsn ||
				(lsn == best_lsn &&
				 sources[i].timeline < sources[best].timeline))
				best = i;
		}
		if (best < 0)
			break;
		out[nout] = (PsWalRec) {
			.lsn = sources[best].entry->items[sources[best].pos].lsn,
			.end_lsn = sources[best].entry->items[sources[best].pos].end_lsn,
			.timeline = sources[best].timeline,
			.flags = sources[best].entry->items[sources[best].pos].flags
		};
		sources[best].pos++;
		nout++;
	}
	return nout;
}

/* ===================== write / read primitives ========================= */

/* The page's own pd_lsn lives in its first 8 bytes (xlogid, xrecoff). */
static uint64_t
page_lsn(const unsigned char *page)
{
	uint32_t	xlogid,
				xrecoff;

	memcpy(&xlogid, page, 4);
	memcpy(&xrecoff, page + 4, 4);
	return ((uint64_t) xlogid << 32) | xrecoff;
}

/*
 * Append one page version at the log head and record it in the index.  Because
 * every write lands at the moving append cursor, physical writes are large and
 * sequential even though each logical page is small -- the property we want for
 * NVMe/SPDK and network transports.
 */
int
append_page(uint32_t timeline, const PsKey *key, uint32_t block,
			const unsigned char *page, uint64_t version,
			uint64_t *out_admission_seq)
{
	SegRecHdr	hdr;
	SegRecHdrAdmission admission_hdr;
	SegRecHdrBoundAdmission bound_hdr;
	uint64_t	header_size = sizeof(SegRecHdrAdmission);
	uint64_t	reclen;
	uint64_t	data_off;
	uint64_t	hdr_grow_lsn = 0;
	uint64_t	order_id = 0;
	uint64_t	page_version;
	uint64_t	admission_seq = admission_seq_alloc();
	int			clamped = 0;
	int			ordered_record = 0;
	int			segment_grows = 0;
	int			zero_version = 0;
	Shard	   *s = shard_for(key);
	ForkEnt    *fe = fork_find(timeline, key);
	uint64_t	branch_floor = 0;
	uint64_t	growth_floor = fe ? fe->last_def_lsn : 0;

	if (admission_seq == 0)
		return -1;

	/* A branch-local version written after the branch snapshot must not become
	 * visible AT that snapshot merely because copied bytes retain an older
	 * source LSN.  The first representable local position is branch_lsn + 1. */
	ps_lock_map_rd();
	if (timeline_has_parent(timeline))
	{
		branch_floor = timelines[timeline].branch_lsn;
		if (branch_floor < UINT64_MAX)
			branch_floor++;
		if (branch_floor > growth_floor)
			growth_floor = branch_floor;
	}
	ps_unlock_map();

	hdr.magic = SEG_ADMISSION_MAGIC;
	hdr.timeline = timeline;
	hdr.key = *key;
	hdr.block = block;
	/*
	 * Version key.  A relation page carries a real monotonic pd_lsn.  An SLRU or
	 * control object is versioned by the caller-supplied 'version' -- the
	 * dirtying/cutoff/update WAL LSN -- stored verbatim so it stays directly
	 * comparable to a branch's as-of cutoff (the SLRU seed path keys a snapshot
	 * by its proven cutoff C and reads it as-of L>=C; a control image is keyed
	 * by the LSN of the update that caused the write, so a branch restores the
	 * control state as of its fork point -- PGCONTROL_ON_STORE_DESIGN.md); a
	 * daemon counter would not be comparable.  Any other non-relation object
	 * carries no LSN in its bytes, so versioning it from page_lsn() could make
	 * an overwrite compare lower and silently lose (and poison the pgcache for
	 * that pseudo-LSN); derive a monotonic latest-wins version from the chain.
	 */
	if (key->klass == PS_KLASS_RELATION)
		hdr.lsn = page_lsn(page);
	else if (key->klass == PS_KLASS_SLRU || key->klass == PS_KLASS_CONTROL ||
			 key->klass == PS_KLASS_SLRU_LIVE || key->klass == PS_KLASS_SLRU_TOMB ||
			 key->klass == PS_KLASS_SLRU_WM ||
			 key->klass == PS_KLASS_READER_SNAPSHOT)
		hdr.lsn = version;
	else
	{
		PageVer    *cur;

		/*
		 * Deriving an object's monotonic version walks the cross-shard timeline
		 * ancestry (read_through), which PS_OP_CREATE_BRANCH mutates under map_wr.
		 * The caller holds only this shard's write lock, so take map_rd for the
		 * walk -- otherwise an object write on a branch can race branch creation
		 * and read a partially updated parent/branch_lsn chain.  Drop it before
		 * the flush below re-takes map_wr; shard -> map order is preserved.
		 */
		ps_lock_map_rd();
		cur = read_through(timeline, key, block, UINT64_MAX, 0);
		hdr.lsn = cur ? cur->lsn + 1 : 1;
		ps_unlock_map();
	}
	hdr.len = page_size;

	/*
	 * Below-floor growth cannot be ordered by the page's raw LSN.  Two
	 * shapes, two treatments:
	 *
	 * - A copied relation page with a NONZERO source pd_lsn below the
	 *   fork/branch floor (skip-WAL rewrites) has its RECORD stamped
	 *   at the floor: version visibility, the growth event and recovery
	 *   (which re-derives from the record) then all agree -- an as-of read
	 *   below the fork's creation sees neither the size nor the bytes, and
	 *   nothing needs a separate durable event.
	 *
	 * - A zero-version record must KEEP version 0 -- capped reads refuse
	 *   LSN-0 versions by design.  Its header stores the growth floor while
	 *   the in-memory page/object version remains zero.  Every below-floor or
	 *   zero-version record uses an ordered format and requires its inert
	 *   fork-meta commit marker at recovery, even when it rewrites an existing
	 *   block: a later same-LSN truncate/unlink must stay ordered after it.
	 */
	if (key->klass == PS_KLASS_RELATION)
	{
		if (hdr.lsn != 0 && hdr.lsn < growth_floor)
		{
			/* Every below-branch-point local copy is later than the snapshot,
			 * even when it rewrites an inherited block.  Definitive-event clamps
			 * retain the narrower grow/nonexistence test so old retained-block
			 * flushes keep their real pre-truncate LSN. */
			if (branch_floor != 0 && hdr.lsn < branch_floor)
				clamped = 1;
			else
			{
				uint32_t	visible;
				int			existed_before;

				ps_lock_map_rd();
				visible = fork_nblocks_through(timeline, key, growth_floor, 0);
				existed_before = growth_floor > 0 &&
					fork_exists_through(timeline, key, growth_floor - 1, 0);
				ps_unlock_map();
				clamped = visible < block + 1 || !existed_before;
			}
			if (clamped)
				hdr.lsn = growth_floor;
		}
	}
	if (hdr.lsn == 0)
	{
		hdr.magic = SEG_WALLESS_ADMISSION_MAGIC;
		hdr.lsn = growth_floor;
		zero_version = 1;
	}
	hdr_grow_lsn = hdr.lsn;
	page_version = zero_version ? 0 : hdr.lsn;
	ordered_record = zero_version || clamped;
	segment_grows = (!fe ||
		fork_size_asof_hop(fe, hdr_grow_lsn, admission_seq) < block + 1);
	/* An ordered body is acknowledged only together with its bound marker.
	 * Reject an inadmissible tuple before either header or page bytes reach the
	 * segment, even when this is a non-growth commit marker. */
	if ((ordered_record || segment_grows) &&
		!fork_meta_mutation_future(hdr_grow_lsn, admission_seq))
		return -1;
	if (ordered_record)
	{
		order_id = segment_order_id_alloc();
		header_size = sizeof(SegRecHdrBoundAdmission);
		hdr.magic = zero_version ? SEG_WALLESS_ADMISSION_MAGIC :
			SEG_CLAMPED_ADMISSION_MAGIC;
	}
	reclen = header_size + page_size;

	/* roll over to a fresh segment when the current one would overflow */
	if (s->cur_seg < 0 || s->cur_off + reclen > segment_size)
	{
		s->cur_seg = (s->cur_seg < 0) ? 0 : s->cur_seg + 1;
		s->cur_off = 0;
	}

	/* write header then page bytes contiguously at the append cursor */
	if (ordered_record)
	{
		bound_hdr.hdr = hdr;
		bound_hdr.order_id = order_id;
		bound_hdr.admission_seq = admission_seq;
		if (ps_storage->seg_write(s->id, s->cur_seg, s->cur_off,
								  &bound_hdr, sizeof(bound_hdr)) != 0)
			return -1;
	}
	else
	{
		admission_hdr.hdr = hdr;
		admission_hdr.admission_seq = admission_seq;
		if (ps_storage->seg_write(s->id, s->cur_seg, s->cur_off,
								  &admission_hdr, sizeof(admission_hdr)) != 0)
			return -1;
	}
	data_off = s->cur_off + header_size;
	if (ps_storage->seg_write(s->id, s->cur_seg, data_off, page, page_size) != 0)
		return -1;

	/*
	 * The segment record is the growth's durability; this metadata marker only
	 * records its position among equal-LSN definitive events.  Recovery ignores
	 * an unmatched marker, so a torn/missing segment cannot manufacture size.
	 */
	if (ordered_record &&
		fork_meta_persist_segment(timeline, key, hdr_grow_lsn, block + 1,
								  segment_grows ? FEV_SEG_GROW : FEV_SEG_COMMIT,
								  order_id, admission_seq) != 0)
	{
		/* The complete body is not committed without its marker.  Retire this
		 * segment so a later torn header cannot reuse that stale body. */
		s->cur_off = segment_size;
		return -1;
	}

	/* index points at the page bytes (data_off), so reads skip the header */
	page_add_version(timeline, key, block, page_version, admission_seq,
					 s->id, s->cur_seg, data_off);

	/* Drop a partial cache insertion for this exact durable version, if any. */
	ps_pgcache_invalidate(timeline, key, block, page_version, admission_seq);
	s->cur_off += reclen;

	/*
	 * Stage the version for the LSM memtable and flush to an image layer when full
	 * (additive in phase 2 -- the segment write above is still authoritative).
	 *
	 * Skip all of this once the manifest is poisoned: record_layer() can no longer
	 * record a layer, so staging pages we can never flush would grow the memtable
	 * without bound (turning a metadata error into an OOM), and flushing would seal
	 * unreferenced layer files.  The page is durable in the segment log, which
	 * recovery scans, so the write still succeeds; reads fall back to the segment.
	 */
	if (s->memtable && !ps_manifest_poisoned())
	{
		uint32_t	flags = PS_IMG_REC_SEG_VALID;

		if (ordered_record)
			flags |= PS_IMG_REC_ORDERED;
		if (zero_version)
			flags |= PS_IMG_REC_WALLESS;
		if (ps_memtable_put(s->memtable, timeline, key, block, page_version,
							page, admission_seq, hdr_grow_lsn, order_id,
							(uint32_t) s->cur_seg,
							data_off, flags) != 0)
			s->coverage_broken = 1;
		if (ps_memtable_full(s->memtable))
		{
			/* A flush mutates the cross-shard
			 * ps_layer_map, so take map_lock here -- only on the rare flush, not
			 * on every write.  The caller already holds this shard's write lock,
			 * preserving the shard -> map order. */
			ps_lock_map_wr();
			flush_memtable(s, (uint32_t) s->cur_seg, s->cur_off);
			ps_unlock_map();
		}
	}
	else if (s->memtable)
		s->coverage_broken = 1;

	/*
	 * Grow the fork's size history with this page's exact version LSN: a
	 * block is readable as of a horizon iff it has a version at/below it,
	 * so keying the GROW event by hdr.lsn makes as-of NBLOCKS agree with
	 * as-of page reads block for block.  (This replaces the callers'
	 * former one-shot fork_grow after a batch.)  The WAL-less format stores a
	 * zero-version page/object's growth floor in the same record while its
	 * version stays 0.
	 */
	fork_grow_apply(timeline, key, block + 1, hdr_grow_lsn, admission_seq);
	if (out_admission_seq)
		*out_admission_seq = admission_seq;
	return 0;
}

/* Read a specific version's page bytes into out (page_size bytes). */
int
read_version(const PageVer *v, unsigned char *out)
{
	if (v->seg < 0)				/* layer-origin version (no segment copy) */
		return -1;
	if (ps_storage->seg_read(v->shard, v->seg, v->off, out, page_size) != 0)
		return -1;
	return 0;
}

/*
 * Newest image-layer version of (timeline, key, block) with lsn <= read_lsn on
 * this exact timeline (ancestry is the caller's job).  Tries every image layer
 * of that timeline (key-range/bloom pruning is a later optimization).
 */
static int
layer_map_lookup(uint32_t timeline, const PsKey *key, uint32_t block,
				 uint64_t read_lsn, uint64_t read_seq, uint64_t expected_lsn,
				 uint64_t *out_lsn,
				 uint64_t *out_seq, unsigned char *out)
{
	unsigned char *tmp = malloc(page_size);
	PsLayerDesc *layers;
	uint32_t nlayers;
	int			error = 0;
	int			found = 0;
	uint64_t	best = 0;
	uint64_t	best_seq = 0;
	uint64_t	best_layer = 0;
	uint32_t	shard = ps_shard_of(key);

	if (!tmp)
		return 0;
	ps_lock_map_rd();
	nlayers = ps_layer_map.nlayers;
	layers = nlayers ? malloc((size_t) nlayers * sizeof(*layers)) : NULL;
	if (layers != NULL)
	{
		memcpy(layers, ps_layer_map.layers, (size_t) nlayers * sizeof(*layers));
		for (uint32_t i = 0; i < nlayers; i++)
			if (ps_layer_map.layers[i].kind == PS_LAYER_IMAGE &&
				ps_layer_map.layers[i].timeline == timeline &&
				layer_matches_read_shard(&ps_layer_map.layers[i], shard) &&
				!ps_layer_map.layers[i].deleting)
				__atomic_add_fetch(&ps_layer_map.layers[i].cache_readers, 1,
							   __ATOMIC_ACQ_REL);
	}
	ps_unlock_map();
	if (nlayers == 0)
	{
		free(tmp);
		return 0;
	}
	if (layers == NULL)
	{
		free(tmp);
		return 0;
	}
	for (uint32_t i = 0; i < nlayers; i++)
	{
		const PsLayerDesc *d = &layers[i];
		uint64_t	l,
					a;
		int			lookup;

		if (d->kind != PS_LAYER_IMAGE || d->timeline != timeline ||
			!layer_matches_read_shard(d, shard) || d->deleting)
			continue;
		if (expected_lsn != 0 &&
			(expected_lsn < d->lsn_start || expected_lsn > d->lsn_end))
			continue;
		lookup = ps_image_layer_lookup(d, key, block, read_lsn, read_seq, tmp,
									   page_size, &l, &a);

		if (lookup < 0)
		{
			error = 1;
			break;
		}
		if (lookup == 1 &&
			(!found || l > best || (l == best && a > best_seq) ||
			 (l == best && a == best_seq && d->layer_id > best_layer)))
		{
			best = l;
			best_seq = a;
			best_layer = d->layer_id;
			memcpy(out, tmp, page_size);
			found = 1;
		}
	}
	/* Release the snapshot pins only after all cache I/O has completed. */
	ps_lock_map_wr();
	for (uint32_t i = 0; i < nlayers; i++)
		if (layers[i].kind == PS_LAYER_IMAGE && layers[i].timeline == timeline &&
			layer_matches_read_shard(&layers[i], shard) && !layers[i].deleting)
			for (uint32_t j = 0; j < ps_layer_map.nlayers; j++)
				if (ps_layer_map.layers[j].layer_id == layers[i].layer_id)
				{
					__atomic_sub_fetch(&ps_layer_map.layers[j].cache_readers, 1,
								   __ATOMIC_ACQ_REL);
					if (layers[i].data_verified)
						ps_layer_map.layers[j].data_verified = true;
					if (tier_local_location(&ps_layer_map.layers[j]) == NULL &&
						ps_layer_store->layer_exists_local != NULL &&
						ps_layer_store->layer_exists_local(layers[i].layer_id) == 1)
						ps_layer_map.layers[j].cache_resident = true;
					break;
				}
	free(layers);
	free(tmp);
	if (found && out_lsn)
		*out_lsn = best;
	if (found && out_seq)
		*out_seq = best_seq;
	if (found)
	{
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == best_layer &&
				tier_local_location(&ps_layer_map.layers[i]) == NULL)
			{
				ps_layer_map.layers[i].cache_resident = true;
				break;
			}
	}
	ps_unlock_map();
	return error ? -1 : found;
}

/*
 * Resolve a read into out (page_size bytes): walk the timeline ancestry as
 * read_through() does, but serve the bytes from the memtable or an image layer
 * when they hold the authoritative version, falling back to the segment.  The
 * page index (page_visible) still selects the authoritative version at each
 * level, so the result matches the segment-only read; layers/memtable just serve
 * the bytes without touching the segment.  Returns 1 if a version was found and
 * out filled, 0 if the page is unwritten (caller zero-fills), and -1 if an
 * authoritative stored version cannot be read.
 */
int
read_resolve(uint32_t timeline, const PsKey *key, uint32_t block,
			 uint64_t read_lsn, uint64_t read_seq, unsigned char *out,
			 uint64_t *out_ver)
{
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	TlWalk		walk[MAX_TIMELINES];
	uint32_t	levels = 0;
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	/*
	 * A durable compaction frontier makes older page history unavailable even
	 * while a crash-recovery pass still has its source layers to clean up.
	 * Current reads remain valid: their UINT64_MAX horizon is always newer
	 * than the frontier.
	 */
	if (read_lsn != UINT64_MAX && key->klass == PS_KLASS_RELATION)
	{
		int		frontier_allows;

		/* page_reclaimed_frontier is published by compaction while holding the
		 * map write lock.  Sample the two-word fence under the same lock. */
		ps_lock_map_rd();
		frontier_allows = page_frontier_allows(timeline, timeline, read_lsn, read_seq);
		ps_unlock_map();
		if (!frontier_allows)
			return -2;
	}

	/* Copy ancestry while CREATE_BRANCH is excluded, then release map_lock
	 * before a remote layer read can block. */
	ps_lock_map_rd();
	do
		walk[levels++] = w;
	while (levels < MAX_TIMELINES && tl_walk_next(&w));
	ps_unlock_map();

	for (uint32_t level = 0; level < levels; level++)
	{
		w = walk[level];
		{
			uint32_t	tl = w.tl;
			uint64_t	rl = w.lsn;
			uint64_t	seq_cap = rl == read_lsn ? read_seq : 0;
			ForkEnt    *fe;
			uint32_t	nb = 0;
			int			fork_state;
			PageEnt    *e;
			PageVer    *pv;

			/* A child can inherit this page from a parent.  Check every
			 * traversed timeline so a parent frontier cannot be bypassed merely
			 * because the child has not compacted locally. */
			if (read_lsn != UINT64_MAX && key->klass == PS_KLASS_RELATION)
			{
				int	frontier_allows;

				/* Compaction publishes the two-word frontier under map_lock. */
				ps_lock_map_rd();
				frontier_allows = page_frontier_allows(tl, timeline, rl, seq_cap);
				ps_unlock_map();
				if (!frontier_allows)
					return -2;
			}
			fe = fork_find(tl, key);
			fork_state = fe ? fork_asof_hop(fe, rl, seq_cap, &nb) :
				FORK_HOP_NONE;
			e = page_find(tl, key, block);
			pv = e ? page_visible(e, rl, seq_cap) : NULL;

		if (pv)
		{
			uint64_t	l,
						a;
			int			served;
			int			poisoned;

			if (fork_page_invalidated(fe, block, pv, rl, seq_cap))
				return 0;

			/*
			 * The materialized-page cache and the memtable are safe read sources
			 * only while they stay in lock-step with the segment log.  Once the
			 * manifest is poisoned, append_page() stops staging and the memtable
			 * can lag the segment-backed page index.  Bypass transient sources in
			 * that state; reclaimed versions still use their durable layer.
			 */
			poisoned = ps_manifest_poisoned();

			/* the resolved version (newest <= read_lsn); a caller that needs an
			 * exact-cutoff match -- e.g. an SLRU snapshot read -- compares it. */
			if (out_ver)
				*out_ver = pv->lsn;

			/* fast path: materialized-page cache, keyed by the resolved version */
			if (!poisoned && ps_pgcache_lookup(tl, key, block, pv->lsn,
											pv->admission_seq, out))
				return 1;

			if (s->memtable && !poisoned &&
				ps_memtable_lookup(s->memtable, tl, key, block, rl, seq_cap,
								   &l, &a, out) &&
				l == pv->lsn && a == pv->admission_seq)
			{
				__atomic_fetch_add(&s->rr_mem, 1, __ATOMIC_RELAXED);
				served = 1;		/* served from the memtable */
			}
			else if (pv->seg < 0)
			{
				int		layer_result = layer_map_lookup(tl, key, block, rl,
															seq_cap, pv->lsn, &l, &a, out);

				if (layer_result < 0)
					return -1;
				if (layer_result == 0 || l != pv->lsn || a != pv->admission_seq)
					return -1;
				/*
				 * Serve from a layer only for a layer-origin version (no segment
				 * copy).  A segment-backed version must come from its segment; layers
				 * are only authoritative after recovery or segment reclamation changes
				 * the page index entry to a layer origin.
				 */
				__atomic_fetch_add(&s->rr_layer, 1, __ATOMIC_RELAXED);
				served = 1;		/* served from an image layer */
			}
			else
			{
				__atomic_fetch_add(&s->rr_seg, 1, __ATOMIC_RELAXED);
				served = (read_version(pv, out) == 0);	/* segment fallback */
			}
			if (served)
				ps_pgcache_insert(tl, key, block, pv->lsn,
								  pv->admission_seq, out);
			return served ? 1 : 0;
		}
		if (fork_state == FORK_HOP_DEAD ||
			(fork_state == FORK_HOP_DEF && block >= nb))
			return 0;
		if (fork_state == FORK_HOP_DEF &&
			fork_inheritance_fenced(fe, block, rl, seq_cap))
			return 0;
		}
	}
	return 0;
}

/*
 * Durable WAL retention floor for a timeline (PGCONTROL_ON_STORE_DESIGN.md).
 *
 * Every mirrored pg_control image is preceded by an 8-byte "floor note" --
 * the image's checkpoint redo pointer -- written as block 1 of the control
 * object at the same version LSN.  A control image is only restorable if the
 * WAL from its redo pointer onward still exists, so the retention floor for a
 * timeline is the minimum redo over every control image restorable on its
 * ancestry: all block-1 note versions, capped per ancestry level at the
 * branch point exactly as an as-of restore would be.  The notes live in the
 * ordinary segment log, so the floor survives a daemon restart via normal
 * recovery -- it is the durable authority the design requires, independent of
 * any transient compute-side state.
 *
 * Returns 0 when no control image exists (nothing constrains WAL yet).  Any
 * future shipped-WAL GC must refuse to drop WAL at or above this floor.
 */
typedef struct ControlLsnSlot
{
	uint64_t	lsn;
	unsigned char used;
} ControlLsnSlot;

/* Return 1 when every image through cap has a same-LSN note, 0 when one is
 * missing, and -1 when coverage cannot be proved.  Version chains are in
 * arrival rather than LSN order, so use a one-pass hash set instead of a
 * quadratic nested scan. */
static int
control_images_covered(PageEnt *notes, PageEnt *images, uint64_t cap)
{
	ControlLsnSlot *slots;
	size_t		nslots = 1;

	if (!images)
		return 1;
	if (!notes)
	{
		for (int i = 0; i < images->nver; i++)
			if (images->vers[i].lsn <= cap)
				return 0;
		return 1;
	}
	while (nslots < (size_t) notes->nver * 2 + 1)
	{
		if (nslots > SIZE_MAX / 2)
			return -1;
		nslots *= 2;
	}
	slots = calloc(nslots, sizeof(*slots));
	if (!slots)
		return -1;
	for (int i = 0; i < notes->nver; i++)
	{
		uint64_t lsn = notes->vers[i].lsn;
		size_t pos;

		if (lsn > cap)
			continue;
		pos = (size_t) ((lsn ^ (lsn >> 33)) * 0xff51afd7ed558ccdULL) &
			(nslots - 1);
		while (slots[pos].used && slots[pos].lsn != lsn)
			pos = (pos + 1) & (nslots - 1);
		slots[pos].used = 1;
		slots[pos].lsn = lsn;
	}
	for (int i = 0; i < images->nver; i++)
	{
		uint64_t lsn = images->vers[i].lsn;
		size_t pos;

		if (lsn > cap)
			continue;
		pos = (size_t) ((lsn ^ (lsn >> 33)) * 0xff51afd7ed558ccdULL) &
			(nslots - 1);
		while (slots[pos].used && slots[pos].lsn != lsn)
			pos = (pos + 1) & (nslots - 1);
		if (!slots[pos].used)
		{
			free(slots);
			return 0;
		}
	}
	free(slots);
	return 1;
}

int
wal_retain_floor(uint32_t timeline, uint64_t *floor_out)
{
	PsKey		key;
	TlWalk		ancestry[MAX_TIMELINES];
	uint32_t	ancestry_n = 0;
	uint32_t	tl = timeline;
	uint64_t	lsn = UINT64_MAX;
	uint64_t	floor = 0;
	unsigned char *tmp = malloc(page_size);
	int			rc = 0;
	bool		complete = false;

	if (!tmp)
		return -1;				/* cannot prove a floor: fail closed */
	ps_lock_map_rd();
	for (; ancestry_n < MAX_TIMELINES; ancestry_n++)
	{
		ancestry[ancestry_n].tl = tl;
		ancestry[ancestry_n].lsn = lsn;
		if (!timeline_has_parent(tl))
		{
			complete = true;
			break;
		}
		if (timelines[tl].branch_lsn < lsn)
			lsn = timelines[tl].branch_lsn;
		tl = (uint32_t) timelines[tl].parent;
	}
	ps_unlock_map();
	if (!complete)
	{
		free(tmp);
		return -1;				/* malformed ancestry: fail closed */
	}
	ancestry_n++;
	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;

	for (uint32_t level = 0; level < ancestry_n; level++)
	{
		TlWalk		w = ancestry[level];
		PageEnt    *notes = page_find(w.tl, &key, 1);
		PageEnt    *images = page_find(w.tl, &key, 0);

		if (notes)
		{
			for (int i = 0; i < notes->nver; i++)
			{
				PageVer    *v = &notes->vers[i];
				uint64_t	redo;

				/* only images restorable at this ancestry level count */
				if (v->lsn > w.lsn)
					continue;

				/*
				 * An unreadable note must fail the query, not be skipped: a
				 * WAL-GC caller acting on a floor that silently ignored a
				 * note could drop WAL a restorable image still needs.
				 */
				if (v->seg >= 0)
				{
					if (read_version(v, tmp) != 0)
					{
						rc = -1;
						goto done;
					}
				}
				else
				{
					uint64_t	layer_lsn;

				if (layer_map_lookup(w.tl, &key, 1, v->lsn, 0, v->lsn,
									 &layer_lsn, NULL, tmp) != 1 ||
						layer_lsn != v->lsn)
					{
						rc = -1;
						goto done;
					}
				}
				memcpy(&redo, tmp, sizeof(redo));

				/*
				 * A zero redo means a torn/corrupt note (the mirror never
				 * ships one: every control image carries a real redo
				 * pointer).  Its image's requirement is unknowable, so the
				 * floor collapses to "retain everything" -- returning a
				 * higher floor because the same-LSN coverage check was
				 * satisfied by a garbage note would under-retain.
				 */
				if (redo == 0)
				{
					floor = 1;
					goto done;
				}
				if (floor == 0 || redo < floor)
					floor = redo;
			}
		}

		/*
		 * Every restorable control image (block 0) must be covered by a
		 * note at the same version: an image without one (mirrored before
		 * the note format existed) has an unknowable redo pointer.  Old
		 * versions never leave the chain, so failing the query would brick
		 * the floor FOREVER on upgraded stores; instead collapse to the
		 * most conservative provable answer -- retain everything (floor =
		 * the lowest valid LSN) -- until version-level GC (M5) prunes the
		 * unnoted images away.
		 */
		if (images)
		{
			int covered = control_images_covered(notes, images, w.lsn);

			if (covered < 0)
			{
				rc = -1;
				goto done;
			}
			if (!covered)
			{
				floor = 1;
				goto done;
			}
		}
	}

done:
	free(tmp);
	if (rc == 0)
		*floor_out = floor;
	return rc;
}

/* Scan one timeline's local control versions through cap.  This is used by
 * the batched effective-floor path so each descendant is read exactly once. */
static int
wal_retain_floor_level(uint32_t timeline, uint64_t cap, unsigned char *tmp,
					   uint64_t *floor)
{
	PsKey		key;
	PageEnt    *notes;
	PageEnt    *images;

	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;
	notes = page_find(timeline, &key, 1);
	images = page_find(timeline, &key, 0);
	if (notes)
	{
		for (int i = 0; i < notes->nver; i++)
		{
			PageVer *v = &notes->vers[i];
			uint64_t redo;

			if (v->lsn > cap)
				continue;
			if (v->seg >= 0)
			{
				if (read_version(v, tmp) != 0)
					return -1;
			}
			else
			{
				uint64_t layer_lsn;

				if (layer_map_lookup(timeline, &key, 1, v->lsn, 0, v->lsn,
								 &layer_lsn, NULL, tmp) != 1 ||
					layer_lsn != v->lsn)
					return -1;
			}
			memcpy(&redo, tmp, sizeof(redo));
			if (redo == 0)
			{
				*floor = 1;
				return 0;
			}
			if (*floor == 0 || redo < *floor)
				*floor = redo;
		}
	}
	if (images)
	{
		int covered = control_images_covered(notes, images, cap);

		if (covered < 0)
			return -1;
		if (!covered)
		{
			*floor = 1;
			return 0;
		}
	}
	return 0;
}

static void
retention_floor_add(uint64_t candidate, uint64_t *floor)
{
	/* LSN zero is a real branch cap but the public zero result means "no
	 * constraint".  Floor 1 is the established retain-everything sentinel. */
	if (candidate == 0)
		candidate = 1;
	if (*floor == 0 || candidate < *floor)
		*floor = candidate;
}

/* Project one descendant LSN onto target's physical history.  Caller holds
 * map-rd.  Return 1 if target is an ancestor (including self), else 0. */
static int
retention_project_lsn(uint32_t descendant, uint32_t target, uint64_t *lsn)
{
	uint32_t	current = descendant;
	uint32_t	hops = 0;

	while (current != target)
	{
		if (current >= MAX_TIMELINES || !timelines[current].defined ||
			!timeline_has_parent(current) || ++hops > MAX_TIMELINES)
			return 0;
		if (timelines[current].branch_lsn < *lsn)
			*lsn = timelines[current].branch_lsn;
		current = (uint32_t) timelines[current].parent;
	}
	return 1;
}

/* Caller holds map-wr and the page-prune read fence. */
static int
page_prune_fences(uint32_t timeline, PsPruneFence **fences_out,
				  uint32_t *nfences_out)
{
	PsRetentionPin *pins = NULL;
	uint32_t npins = 0;
	PsPruneFence *fences;
	uint32_t nfences = 0;

	if (ps_retention_snapshot_alloc(&pins, &npins) != 0)
		return -1;
	fences = malloc((size_t) (npins + MAX_TIMELINES) * sizeof(*fences));
	if (fences == NULL)
	{
		free(pins);
		return -1;
	}
	for (uint32_t i = 0; i < npins; i++)
		if ((pins[i].resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
		{
			uint64_t projected = pins[i].lsn;

			if (retention_project_lsn(pins[i].timeline, timeline, &projected))
			{
				fences[nfences].lsn = projected;
				fences[nfences].admission_seq = projected == pins[i].lsn &&
					pins[i].admission_seq != 0 ? pins[i].admission_seq : UINT64_MAX;
				nfences++;
			}
		}
	for (uint32_t candidate = 0; candidate < MAX_TIMELINES; candidate++)
	{
		uint64_t cap = UINT64_MAX;
		PsTimelineState state;

		if (candidate != timeline && timelines[candidate].defined &&
			ps_timeline_state(candidate, &state, NULL) &&
			state != PS_TIMELINE_DELETED &&
			retention_project_lsn(candidate, timeline, &cap))
		{
			fences[nfences].lsn = cap;
			fences[nfences].admission_seq = UINT64_MAX;
			nfences++;
		}
	}
	free(pins);
	*fences_out = fences;
	*nfences_out = nfences;
	return 0;
}

/* Caller holds map-rd and the WAL-index prune read fence. */
static int
walidx_prune_fences(uint32_t timeline, uint64_t **fences_out,
					uint32_t *nfences_out)
{
	PsRetentionPin *pins = NULL;
	uint32_t npins = 0;
	uint64_t *fences;
	uint32_t nfences = 0;

	if (ps_retention_snapshot_alloc(&pins, &npins) != 0)
		return -1;
	fences = malloc((size_t) (npins + MAX_TIMELINES) * sizeof(*fences));
	if (fences == NULL)
	{
		free(pins);
		return -1;
	}
	for (uint32_t i = 0; i < npins; i++)
		if ((pins[i].resources & PS_RETENTION_RESOURCE_WAL_INDEX) != 0)
		{
			uint64_t projected = pins[i].lsn;

			if (retention_project_lsn(pins[i].timeline, timeline, &projected))
				fences[nfences++] = projected;
		}
	for (uint32_t candidate = 0; candidate < MAX_TIMELINES; candidate++)
	{
		uint64_t projected = UINT64_MAX;
		PsTimelineState state;

		if (candidate != timeline && timelines[candidate].defined &&
			ps_timeline_state(candidate, &state, NULL) &&
			state != PS_TIMELINE_DELETED &&
			retention_project_lsn(candidate, timeline, &projected))
			fences[nfences++] = projected;
	}
	free(pins);
	*fences_out = fences;
	*nfences_out = nfences;
	return 0;
}

/*
 * One conservative floor for one resource on one timeline.  Explicit pins on
 * descendants are projected through every branch cap; a live direct child is
 * itself a structural pin.  WAL additionally includes every restorable control
 * image on the target and descendants.  Thus page pruning, WAL GC and WAL-index
 * GC can share this authority instead of each inventing a partial horizon.
 */
static int
retention_effective_floor_internal(uint32_t timeline, uint32_t resource,
							   uint64_t *floor_out, int map_locked)
{
	typedef struct RetentionControlProjection
	{
		uint32_t	timeline;
		uint64_t	cap;
	} RetentionControlProjection;
	RetentionControlProjection controls[MAX_TIMELINES];
	TlWalk		ancestors[MAX_TIMELINES];
	uint32_t	ncontrols = 0;
	uint32_t	nancestors = 0;
	uint32_t	npins = 0;
	PsRetentionPin *pins = NULL;
	uint64_t	floor = 0;

	if (resource != PS_RETENTION_RESOURCE_PAGE_HISTORY &&
		resource != PS_RETENTION_RESOURCE_WAL &&
		resource != PS_RETENTION_RESOURCE_WAL_INDEX)
		return -1;

	if (!map_locked)
		ps_lock_map_rd();
	if (timeline >= MAX_TIMELINES || !timelines[timeline].defined ||
		ps_retention_snapshot_alloc(&pins, &npins) != 0)
	{
		if (!map_locked)
			ps_unlock_map();
		free(pins);
		return -1;
	}
	for (uint32_t i = 0; i < npins; i++)
	{
		PsRetentionPin pin = pins[i];
		uint64_t	projected;
		int			found;

		found = 1;
		if (found != 1 || pin.timeline >= MAX_TIMELINES ||
			!timelines[pin.timeline].defined)
		{
			if (!map_locked)
				ps_unlock_map();
			free(pins);
			return -1;
		}
		if ((pin.resources & resource) == 0)
			continue;
		projected = pin.lsn;
		if (retention_project_lsn(pin.timeline, timeline, &projected))
			retention_floor_add(projected, &floor);
	}
	free(pins);

	/* Every descendant can be started or read again later even with no active
	 * owner pin.  Project all of their branch caps, not just direct children: a
	 * nested branch may fork below its parent's own fork point.  The same walk
	 * snapshots the descendants whose visible control images constrain WAL. */
	for (uint32_t candidate = 0; candidate < MAX_TIMELINES; candidate++)
	{
		uint64_t	cap = UINT64_MAX;
		PsTimelineState state;

		if (!timelines[candidate].defined ||
			!ps_timeline_state(candidate, &state, NULL) ||
			state == PS_TIMELINE_DELETED ||
			!retention_project_lsn(candidate, timeline, &cap))
			continue;
		if (candidate != timeline && resource != PS_RETENTION_RESOURCE_PAGE_HISTORY)
			retention_floor_add(cap, &floor);
		if (resource == PS_RETENTION_RESOURCE_WAL)
		{
			controls[ncontrols].timeline = candidate;
			controls[ncontrols].cap = cap;
			ncontrols++;
		}
	}
	if (resource == PS_RETENTION_RESOURCE_WAL)
	{
		uint32_t current = timeline;
		uint64_t cap = UINT64_MAX;

		while (timeline_has_parent(current))
		{
			if (nancestors >= MAX_TIMELINES)
			{
				if (!map_locked)
					ps_unlock_map();
				return -1;
			}
			if (timelines[current].branch_lsn < cap)
				cap = timelines[current].branch_lsn;
			current = (uint32_t) timelines[current].parent;
			ancestors[nancestors].tl = current;
			ancestors[nancestors].lsn = cap;
			nancestors++;
		}
	}
	if (!map_locked)
		ps_unlock_map();

	if (resource == PS_RETENTION_RESOURCE_WAL)
	{
		unsigned char *tmp = malloc(page_size);

		if (!tmp)
			return -1;
		for (uint32_t i = 0; i < ncontrols && floor != 1; i++)
			if (wal_retain_floor_level(controls[i].timeline,
								   controls[i].cap, tmp, &floor) != 0)
			{
				free(tmp);
				return -1;
			}
		for (uint32_t i = 0; i < nancestors && floor != 1; i++)
			if (wal_retain_floor_level(ancestors[i].tl, ancestors[i].lsn,
								   tmp, &floor) != 0)
			{
				free(tmp);
				return -1;
			}
		free(tmp);
	}
	*floor_out = floor;
	return 0;
}

static int
retention_effective_floor(uint32_t timeline, uint32_t resource,
						  uint64_t *floor_out)
{
	return retention_effective_floor_internal(timeline, resource, floor_out, 0);
}

/* Return only immutable WAL bytes which the existing R3b proof would permit
 * this maintenance pass to reclaim.  In particular, this never treats the
 * current boundary or an unproven end-start interval as lag. */
static uint64_t
wal_reclaim_lag_bytes(void)
{
	struct timespec now;
	uint64_t lag = 0;
	int observation_error = 0;

	if (ps_storage == NULL || ps_storage->name == NULL ||
		strcmp(ps_storage->name, "posix") != 0)
		return 0;
	if (!wal_reclaim_preselected(&now, 0, &observation_error))
		return observation_error ? UINT64_MAX : 0;
	if (observation_error)
		return UINT64_MAX;
	if (admission_write_lock() != 0)
		return 0;
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		pthread_rwlock_t *wal_lock;
		PsWalStore *store;
		uint64_t start;
		uint64_t end;
		uint64_t progress = 0;
		uint64_t raw_floor = 0;
		uint64_t retention_floor = 0;
		uint64_t target = 0;
		uint64_t residual_bytes = 0;
		uint64_t residual_target = 0;
		int residual_status;
		int residual_pending;
		int suffix_candidate;
		int walidx_valid;
		int proof_rc = 0;

		if (!ps_timeline_live(tl) || (wal_lock = wal_log_lock_for(tl)) == NULL)
			continue;
		for (uint32_t shard = 0; shard < core_shards(); shard++)
			ps_lock_shard_wr(shard);
		pthread_rwlock_wrlock(&walidx_prune_lock);
		walidx_publish_wrlock();
		pthread_rwlock_wrlock(wal_lock);
		store = &wal_segment_stores[tl];
		residual_status = wal_segment_store_opened[tl] ?
			ps_wal_store_residual_prefix_pending(store, &residual_target) : 0;
		residual_pending = residual_status > 0;
		if (residual_status > 0 &&
			ps_wal_store_residual_prefix_bytes(store, &residual_bytes) != 0)
			residual_status = -1;
		if (!wal_segment_store_opened[tl] || residual_status < 0 ||
			store->metadata_fenced)
		{
			if (residual_status < 0 || store->metadata_fenced)
				lag = UINT64_MAX;
			goto wal_lag_unlock;
		}
		start = store->start_lsn;
		end = store->end_lsn;
		if (residual_pending)
		{
			/* Recovery publishes start_lsn at the logical frontier while the
			 * residual marker describes only the older physical prefix.  The
			 * two ranges are therefore disjoint, but only when the marker's
			 * target agrees with that frontier. */
			if (residual_target != start)
			{
				lag = UINT64_MAX;
				goto wal_lag_unlock;
			}
			lag = backpressure_saturating_add(lag, residual_bytes);
		}
		suffix_candidate = store->nentries != 0 && store->segment_size != 0 &&
			start <= UINT64_MAX - store->segment_size &&
			start + store->segment_size <= end;
		if (!suffix_candidate)
			goto wal_lag_unlock;
		pthread_mutex_lock(&walidx_meta_lock);
		walidx_valid = wal_reclaim_walidx_state_valid(tl, &progress);
		pthread_mutex_unlock(&walidx_meta_lock);
		ps_lock_map_rd();
		if (walidx_valid)
			proof_rc = wal_reclaim_raw_dependency_floor(tl, start, &raw_floor);
		ps_unlock_map();
		for (uint32_t shard = core_shards(); shard > 0; shard--)
			ps_unlock_shard(shard - 1);
		walidx_publish_wrunlock();
		pthread_rwlock_unlock(&walidx_prune_lock);
		pthread_rwlock_unlock(wal_lock);
		if (!walidx_valid || proof_rc != 0 ||
			retention_effective_floor(tl, PS_RETENTION_RESOURCE_WAL,
									 &retention_floor) != 0 || retention_floor == 0)
		{
			/* If a residual was observed and a complete suffix candidate also
			 * exists, returning only the residual would incorrectly release a
			 * throttle.  The physical prefix remains evidence of debt, so an
			 * unprovable suffix is deliberately fail-closed. */
			if (residual_pending)
				lag = UINT64_MAX;
			continue;
		}
		for (uint32_t shard = 0; shard < core_shards(); shard++)
			ps_lock_shard_wr(shard);
		pthread_rwlock_wrlock(&walidx_prune_lock);
		walidx_publish_wrlock();
		pthread_rwlock_wrlock(wal_lock);
		store = &wal_segment_stores[tl];
		if (!wal_segment_store_opened[tl] || store->metadata_fenced ||
			store->start_lsn != start || store->end_lsn != end)
		{
			if (residual_pending)
				lag = UINT64_MAX;
			goto wal_lag_unlock;
		}
		target = retention_floor < progress ? retention_floor : progress;
		if (raw_floor != 0 && raw_floor < target)
			target = raw_floor;
		if (timeline_has_parent(tl) && timelines[tl].branch_lsn < target)
			target = timelines[tl].branch_lsn;
		if (target > end)
			target = end;
		target -= target % store->segment_size;
		if (target > start)
			lag = backpressure_saturating_add(lag, target - start);

wal_lag_unlock:
		pthread_rwlock_unlock(wal_lock);
		walidx_publish_wrunlock();
		pthread_rwlock_unlock(&walidx_prune_lock);
		for (uint32_t shard = core_shards(); shard > 0; shard--)
			ps_unlock_shard(shard - 1);
	}
	ps_admission_write_unlock();
	return lag;
}

/* Observe one timeline without holding runtime locks across filesystem I/O.
 * The publication writer lock makes the captured offsets a coherent logical
 * identity; the second short critical section rejects an observation that
 * raced a publish/append/recovery transition. */
static uint64_t
walidx_reclaim_lag_bytes(unsigned char *tail_candidates,
						 unsigned char *gc_candidates)
{
	uint64_t lag = 0;
	const uint32_t max_retries = 3;

	if (tail_candidates != NULL)
		memset(tail_candidates, 0, MAX_TIMELINES);
	if (gc_candidates != NULL)
		memset(gc_candidates, 0, MAX_TIMELINES);

	if (ps_storage == NULL || ps_storage->name == NULL ||
		strcmp(ps_storage->name, "posix") != 0 ||
		ps_storage->walidx_reclaim_bytes == NULL)
		return UINT64_MAX;
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		WalIdxDebtSnapshot snapshot;
		uint64_t tail = 0;
		uint64_t obsolete_epoch = 0;
		uint64_t obsolete_snapshot = 0;
		uint64_t total;
		int stable = 0;

		for (uint32_t attempt = 0; attempt < max_retries; attempt++)
		{
			/* Every physical scan owns its outputs for this attempt.  In
			 * particular, never let a failed scan reuse a prior attempt's
			 * candidate bytes. */
			tail = 0;
			obsolete_epoch = 0;
			obsolete_snapshot = 0;
			total = 0;
			if (walidx_debt_snapshot(tl, &snapshot) != 0)
				return UINT64_MAX;
			if (!snapshot.valid)
			{
				stable = 1;
				break;
			}
			if (ps_storage->walidx_reclaim_bytes(tl, snapshot.epochs,
											 snapshot.covered_offsets,
											 snapshot.observed_offsets,
											 core_shards(), &tail,
											 &obsolete_epoch) != 0 ||
								ps_walidx_snapshot_reclaim_bytes(snapshot.directory, tl,
														 snapshot.generation,
														 &obsolete_snapshot) != 0)
			{
				/* A failed physical observation is retryable when the logical
				 * identity moved while it was in flight.  If it did not move,
				 * fail closed rather than turning an I/O/corruption error into
				 * zero debt. */
				if (walidx_observation_error_test_hook != NULL)
					walidx_observation_error_test_hook(tl,
											 walidx_observation_error_test_hook_arg);
				if (walidx_debt_snapshot_unchanged(tl, &snapshot))
					return UINT64_MAX;
				continue;
			}
			total = backpressure_saturating_add(tail, obsolete_epoch);
			total = backpressure_saturating_add(total, obsolete_snapshot);
			if (walidx_debt_snapshot_unchanged(tl, &snapshot))
			{
				lag = backpressure_saturating_add(lag, total);
				if (tail_candidates != NULL && tail != 0)
					tail_candidates[tl] = 1;
				if (gc_candidates != NULL &&
					(obsolete_epoch != 0 || obsolete_snapshot != 0))
					gc_candidates[tl] = 1;
				stable = 1;
				break;
			}
		}
		if (!stable)
			return UINT64_MAX;
	}
	return lag;
}

static void
backpressure_update_locked(PsBackpressureController *controller,
						   uint64_t lag, uint64_t high, uint64_t catchup)
{
	controller->lag_bytes = lag;
	controller->high_water_bytes = high;
	controller->catchup_bytes = catchup;
	if (high == 0)
	{
		if (controller->throttled)
		{
			controller->throttled = 0;
			controller->throttle_exits++;
		}
	}
	else if (!controller->throttled && lag >= high)
	{
		controller->throttled = 1;
		controller->throttle_enters++;
	}
	else if (controller->throttled && lag <= catchup)
	{
		controller->throttled = 0;
		controller->throttle_exits++;
	}
}

static uint64_t
backpressure_monotonic_ns(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
		(uint64_t) now.tv_sec > (UINT64_MAX - (uint64_t) now.tv_nsec) /
		UINT64_C(1000000000))
		return UINT64_MAX;
	return (uint64_t) now.tv_sec * UINT64_C(1000000000) +
		(uint64_t) now.tv_nsec;
}

static int
walidx_auto_observation_claim(void)
{
	uint64_t now = backpressure_monotonic_ns();
	uint64_t next;

	if (now == UINT64_MAX)
		return 1;
	for (;;)
	{
		next = __atomic_load_n(&walidx_observation_next_ns, __ATOMIC_ACQUIRE);
		if (next == UINT64_MAX || now < next)
			return 0;
		/* Reserve the scan itself.  Arming from the completion timestamp is
		 * important when the bounded physical scan takes >=100ms. */
		if (__atomic_compare_exchange_n(&walidx_observation_next_ns, &next,
										UINT64_MAX,
										0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
			return 1;
	}
}

static void
walidx_arm_observation_timer(void)
{
	uint64_t now = backpressure_monotonic_ns();
	uint64_t due;

	if (now == UINT64_MAX)
	{
		/* Do not leave the in-progress sentinel armed forever if the clock
		 * cannot be sampled.  The next automatic call may retry safely. */
		__atomic_store_n(&walidx_observation_next_ns, 0, __ATOMIC_RELEASE);
		return;
	}
	due = UINT64_MAX - now < WALIDX_AUTO_OBSERVATION_INTERVAL_NS ?
		UINT64_MAX - 1 : now + WALIDX_AUTO_OBSERVATION_INTERVAL_NS;
	__atomic_store_n(&walidx_observation_next_ns, due, __ATOMIC_RELEASE);
}

static int
forkmeta_auto_observation_claim(void)
{
	uint64_t now = backpressure_monotonic_ns();
	uint64_t next;

	if (now == UINT64_MAX)
		return 1;
	for (;;)
	{
		next = __atomic_load_n(&forkmeta_observation_next_ns, __ATOMIC_ACQUIRE);
		if (next == UINT64_MAX || now < next)
			return 0;
		if (__atomic_compare_exchange_n(&forkmeta_observation_next_ns, &next,
										UINT64_MAX, 0, __ATOMIC_ACQ_REL,
										__ATOMIC_ACQUIRE))
			return 1;
	}
}

static void
forkmeta_arm_observation_timer(void)
{
	uint64_t now = backpressure_monotonic_ns();
	uint64_t due;

	if (now == UINT64_MAX)
	{
		__atomic_store_n(&forkmeta_observation_next_ns, 0, __ATOMIC_RELEASE);
		return;
	}
	due = UINT64_MAX - now < FORKMETA_AUTO_OBSERVATION_INTERVAL_NS ?
		UINT64_MAX - 1 : now + FORKMETA_AUTO_OBSERVATION_INTERVAL_NS;
	__atomic_store_n(&forkmeta_observation_next_ns, due, __ATOMIC_RELEASE);
}

static void
ps_backpressure_refresh_internal(int automatic)
{
	unsigned char walidx_tail_candidates[MAX_TIMELINES] = {0};
	unsigned char walidx_gc_candidates[MAX_TIMELINES] = {0};
	int observe_walidx = walidx_reclaim_high_water_bytes != 0 &&
		(!automatic || walidx_auto_observation_claim());
	int observe_forkmeta = forkmeta_reclaim_high_water_bytes != 0 &&
		(!automatic || forkmeta_auto_observation_claim());
	uint64_t page_lag = page_reclaim_high_water_bytes != 0 ?
		page_reclaim_lag_bytes() : 0;
	uint64_t wal_lag = wal_reclaim_high_water_bytes != 0 ?
		wal_reclaim_lag_bytes() : 0;
	uint64_t walidx_lag = observe_walidx ?
		walidx_reclaim_lag_bytes(walidx_tail_candidates,
									 walidx_gc_candidates) : 0;
	uint64_t forkmeta_lag = observe_forkmeta ?
		forkmeta_reclaim_lag_bytes() : 0;

	if (observe_walidx)
	{
		__atomic_fetch_add(&walidx_observation_count, 1, __ATOMIC_RELAXED);
		walidx_arm_observation_timer();
	}
	if (observe_forkmeta)
	{
		__atomic_fetch_add(&forkmeta_observation_count, 1, __ATOMIC_RELAXED);
		forkmeta_arm_observation_timer();
	}

	pthread_mutex_lock(&backpressure_lock);
	backpressure_update_locked(&page_backpressure, page_lag,
							   page_reclaim_high_water_bytes,
							   page_reclaim_catchup_bytes);
	backpressure_update_locked(&wal_backpressure, wal_lag,
							   wal_reclaim_high_water_bytes,
							   wal_reclaim_catchup_bytes);
	if (observe_walidx)
	{
		backpressure_update_locked(&walidx_backpressure, walidx_lag,
											   walidx_reclaim_high_water_bytes,
											   walidx_reclaim_catchup_bytes);
		for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		{
			__atomic_store_n(&walidx_snapshot_force_due[tl],
				walidx_backpressure.throttled && walidx_tail_candidates[tl],
				__ATOMIC_RELEASE);
			__atomic_store_n(&walidx_snapshot_gc_force_due[tl],
				walidx_gc_candidates[tl], __ATOMIC_RELEASE);
		}
	}
	if (observe_forkmeta)
		backpressure_update_locked(&forkmeta_backpressure, forkmeta_lag,
										 forkmeta_reclaim_high_water_bytes,
										 forkmeta_reclaim_catchup_bytes);
	backpressure_publish_locked();
	pthread_mutex_unlock(&backpressure_lock);
}

void
ps_backpressure_refresh(void)
{
	ps_backpressure_refresh_internal(0);
}

static void
ps_backpressure_refresh_automatic(void)
{
	ps_backpressure_refresh_internal(1);
}

/* ===================== recovery (layers + segment tail) =============== */

typedef struct LayerRecoverRec
{
	uint32_t	timeline;
	uint64_t	layer_id;
	PsImgIndexEnt ent;
} LayerRecoverRec;

static int
layer_recover_cmp(const void *pa, const void *pb)
{
	const LayerRecoverRec *a = pa;
	const LayerRecoverRec *b = pb;

	if (a->ent.seg_id != b->ent.seg_id)
		return a->ent.seg_id < b->ent.seg_id ? -1 : 1;
	if (a->ent.seg_off != b->ent.seg_off)
		return a->ent.seg_off < b->ent.seg_off ? -1 : 1;
	return a->layer_id < b->layer_id ? -1 :
		(a->layer_id > b->layer_id ? 1 : 0);
}

/* Replay the index and fork-growth effects shared by layer and segment input. */
static int
replay_page_record(uint32_t timeline, const PsKey *key, uint32_t block,
				   uint64_t page_lsn, uint64_t admission_seq,
				   uint64_t growth_lsn, uint64_t order_id,
				   uint32_t flags, uint32_t shard, int seg, uint64_t off)
{
	int			ordered = (flags & PS_IMG_REC_ORDERED) != 0;
	int			wal_less = (flags & PS_IMG_REC_WALLESS) != 0;

	if (order_id != 0)
		segment_order_id_observe(order_id);
	if (admission_seq != 0)
		admission_seq_observe(admission_seq);
	if (ordered)
	{
		ForkEnt    *fe = fork_find(timeline, key);

		if ((!fe || !fork_event_activate_seg(fe, growth_lsn, block + 1,
												 order_id, admission_seq)) &&
			!fork_meta_legacy)
			return 0;
	}

	page_add_version(timeline, key, block, page_lsn, admission_seq,
					 shard, seg, off);
	if (fork_meta_legacy)
	{
		ForkEnt    *fe = fork_get_or_create(timeline, key);
		uint64_t	l = growth_lsn ? growth_lsn : fe->last_def_lsn;

		if (fork_size_asof_hop(fe, l, admission_seq) < block + 1)
		{
			if (!fork_meta_migrate_failed &&
				fork_meta_persist(timeline, key, l, admission_seq,
							  block + 1, FEV_GROW) != 0)
				fork_meta_migrate_failed = 1;
			fork_event_add(fe, l, admission_seq, block + 1, FEV_GROW);
		}
	}
	else if (!ordered && wal_less)
	{
		ForkEnt    *fe = fork_get_or_create(timeline, key);

		if (!fork_has_growth_at(fe, growth_lsn, block + 1))
			fork_grow_replay(timeline, key, block + 1, growth_lsn,
							 admission_seq);
	}
	else if (!ordered && growth_lsn != 0)
		fork_grow_replay(timeline, key, block + 1, growth_lsn,
						 admission_seq);
	return 1;
}

static int
recover_layer_prefix(uint32_t shard)
{
	Shard	   *s = &g_shards[shard];
	LayerRecoverRec *recs = NULL;
	uint32_t	nrec = 0,
				cap = 0;

	if (!s->flush_watermark_valid)
		return 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];
		PsImgIndexEnt *idx;
		uint32_t	n;
		int			first_covered = -1;

		if (d->kind != PS_LAYER_IMAGE || d->deleting ||
			!timeline_recovery_allowed(d->timeline) ||
			layer_shard_from_id(d->layer_id) != shard)
			continue;
		if (read_image_index_refreshing(d, &idx, &n) != 0)
			goto fail;
		for (uint32_t j = 0; j < n; j++)
		{
			int			covered;

			if (!(idx[j].flags & PS_IMG_REC_SEG_VALID))
				continue;
			covered = idx[j].seg_id < s->flush_watermark.seg_id ||
				(idx[j].seg_id == s->flush_watermark.seg_id &&
				 idx[j].seg_off <= s->flush_watermark.seg_off &&
				 page_size <= s->flush_watermark.seg_off - idx[j].seg_off);
			if (!covered)
				continue;
			if (first_covered < 0)
				first_covered = (int) j;
			if (nrec == cap)
			{
				uint32_t	nc = cap ? cap * 2 : 256;
				LayerRecoverRec *nr = realloc(recs, (size_t) nc * sizeof(*nr));

				if (!nr)
				{
					free(idx);
					goto fail;
				}
				recs = nr;
				cap = nc;
			}
			recs[nrec].timeline = d->timeline;
			recs[nrec].layer_id = d->layer_id;
			recs[nrec].ent = idx[j];
			nrec++;
		}
		if (first_covered >= 0 && !d->data_verified)
		{
			unsigned char *verify = malloc(page_size);
			uint64_t	verified_lsn;

			if (!verify ||
				ps_image_layer_lookup(d, &idx[first_covered].key,
								  idx[first_covered].block,
								  idx[first_covered].lsn, 0, verify, page_size,
								  &verified_lsn, NULL) != 1)
			{
				free(verify);
				free(idx);
				goto fail;
			}
			free(verify);
		}
		free(idx);
		/*
		 * Recovery needs only the index entries already copied above.  Reclaim a
		 * remote-only layer's materialized cache only after revalidating the
		 * object: if the object disappeared or rotted while the daemon was down,
		 * the canonical cache may be the only readable copy left.
		 */
		if (tier_local_location(d) == NULL &&
			ps_layer_store->verify_remote_layer != NULL &&
			ps_layer_store->verify_remote_layer(d) == 0 &&
			ps_layer_store->delete_local_layer(d) != 0)
			goto fail;
	}

	/* A deleting timeline can be the sole former owner of this shard's covered
	 * prefix.  Its layers are intentionally absent from recovery, while shared
	 * segments stay fenced from reclamation until filtered rewrite exists. */
	if (nrec == 0)
	{
		int has_recovery_layer = 0;

		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].kind == PS_LAYER_IMAGE &&
				!ps_layer_map.layers[i].deleting &&
				timeline_recovery_allowed(ps_layer_map.layers[i].timeline) &&
				layer_shard_from_id(ps_layer_map.layers[i].layer_id) == shard)
			{
				has_recovery_layer = 1;
				break;
			}
		/* A reused id may be LIVE with no image layer at all: its old layer was
		 * durably removed before DELETED, while the shard watermark remains a
		 * global fence that must continue to skip the old segment prefix. */
		if (!has_recovery_layer)
		{
			free(recs);
			return 0;
		}
		if (timeline_delete_recovery_skip())
		{
			free(recs);
			return 0;
		}
		goto fail;
	}
	qsort(recs, nrec, sizeof(*recs), layer_recover_cmp);
	for (uint32_t i = 0; i < nrec; i++)
	{
		PsImgIndexEnt *e = &recs[i].ent;

		/* Partial flush retries and compaction can duplicate a source record. */
		if (i + 1 < nrec && e->seg_id == recs[i + 1].ent.seg_id &&
			e->seg_off == recs[i + 1].ent.seg_off)
			continue;
		if (!replay_page_record(recs[i].timeline, &e->key, e->block, e->lsn,
								e->admission_seq, e->growth_lsn, e->order_id,
								e->flags,
								shard, -1, 0))
			goto fail;
	}
	free(recs);
	return 0;

fail:
	free(recs);
	return -1;
}

/* Scan only the segment suffix not covered by the durable layer watermark. */
static int
recover(uint32_t shard)
{
	Shard	   *s = &g_shards[shard];
	int			first = s->flush_watermark_valid ?
		(int) s->flush_watermark.seg_id : 0;
	unsigned char *page = NULL;

	s->cur_seg = s->flush_watermark_valid ? first : -1;
	s->cur_off = s->flush_watermark_valid ? s->flush_watermark.seg_off : 0;
	if (s->memtable)
	{
		page = malloc(page_size);
		if (!page)
			return -1;
	}

	for (int id = first;; id++)
	{
		uint64_t	off = (id == first && s->flush_watermark_valid) ?
			s->flush_watermark.seg_off : 0;
		int64_t		seg_bytes;
		int			retire_segment = 0;

		errno = 0;
		seg_bytes = ps_storage->seg_size(shard, id);
		if (seg_bytes < 0)
			break;
		/* A negative size is the storage contract's end-of-log sentinel.
		 * In particular, SPDK intentionally does not set errno for this case.
		 * PAGE debt observation has separate POSIX-specific error handling. */
		if ((uint64_t) seg_bytes < off)
			goto fail;
		for (;;)
		{
			SegRecHdr	hdr;
			uint64_t	header_size = sizeof(SegRecHdr);
			uint64_t	order_id = 0;
			uint64_t	admission_seq = 0;
			uint64_t	data_off;
			uint64_t	page_version;
			uint32_t	flags = PS_IMG_REC_SEG_VALID;
			int			bound;
			int			has_admission;
			int			ordered;
			int			wal_less;

			if (ps_storage->seg_read(shard, id, off, &hdr, sizeof(hdr)) != 0 ||
				hdr.magic == 0)
				break;
			wal_less = hdr.magic == SEG_WALLESS_MAGIC ||
				hdr.magic == SEG_WALLESS_ORDERED_MAGIC ||
				hdr.magic == SEG_WALLESS_BOUND_MAGIC ||
				hdr.magic == SEG_WALLESS_ADMISSION_MAGIC;
			bound = hdr.magic == SEG_WALLESS_BOUND_MAGIC ||
				hdr.magic == SEG_CLAMPED_BOUND_MAGIC ||
				hdr.magic == SEG_WALLESS_ADMISSION_MAGIC ||
				hdr.magic == SEG_CLAMPED_ADMISSION_MAGIC;
			has_admission = hdr.magic == SEG_ADMISSION_MAGIC ||
				hdr.magic == SEG_WALLESS_ADMISSION_MAGIC ||
				hdr.magic == SEG_CLAMPED_ADMISSION_MAGIC;
			ordered = hdr.magic == SEG_WALLESS_ORDERED_MAGIC ||
				hdr.magic == SEG_CLAMPED_ORDERED_MAGIC || bound;
			if (hdr.magic != SEG_MAGIC && hdr.magic != SEG_ADMISSION_MAGIC &&
				!wal_less && !ordered)
			{
				fprintf(stderr, "pagestore_daemon: shard %u segment %d: incompatible "
						"record magic %#x at offset %llu\n", shard, id, hdr.magic,
						(unsigned long long) off);
				goto fail;
			}
			if (hdr.len != page_size)
				break;
			if (bound)
			{
				header_size = has_admission ? sizeof(SegRecHdrBoundAdmission) :
					sizeof(SegRecHdrBound);
				if (ps_storage->seg_read(shard, id, off + sizeof(hdr),
										 &order_id, sizeof(order_id)) != 0 ||
					order_id == 0)
					break;
			}
			else if (has_admission)
				header_size = sizeof(SegRecHdrAdmission);
			if (has_admission &&
				ps_storage->seg_read(shard, id,
									 off + header_size - sizeof(admission_seq),
									 &admission_seq,
									 sizeof(admission_seq)) != 0)
				break;
			if (has_admission && admission_seq == 0)
				break;
			if (seg_bytes < (int64_t) (off + header_size + hdr.len))
				break;
			data_off = off + header_size;
			if (ordered)
				flags |= PS_IMG_REC_ORDERED;
			if (wal_less)
				flags |= PS_IMG_REC_WALLESS;
			/* A legacy scan durably converts missing marker semantics into
			 * ordinary fork-growth events before sealing the migration. */
			if (fork_meta_legacy)
				flags &= ~PS_IMG_REC_ORDERED;
			page_version = wal_less ? 0 : hdr.lsn;
			if (timeline_recovery_allowed(hdr.timeline) &&
				!replay_page_record(hdr.timeline, &hdr.key, hdr.block,
								page_version, admission_seq, hdr.lsn, order_id,
								flags,
								shard, id, data_off))
			{
				retire_segment = 1;
				break;
			}
			if (s->memtable && timeline_recovery_allowed(hdr.timeline))
			{
				if (ps_storage->seg_read(shard, id, data_off, page, page_size) != 0 ||
					ps_memtable_put(s->memtable, hdr.timeline, &hdr.key, hdr.block,
								page_version, page, admission_seq, hdr.lsn,
								order_id,
								(uint32_t) id, data_off, flags) != 0)
					goto fail;
			}
			off += header_size + hdr.len;
			if (s->memtable && ps_memtable_full(s->memtable) &&
				flush_memtable(s, (uint32_t) id, off) != 0)
				goto fail;
		}
		s->cur_seg = id;
		s->cur_off = retire_segment ? segment_size : off;
	}
	if (s->memtable && ps_memtable_count(s->memtable) > 0 &&
		flush_memtable(s, (uint32_t) s->cur_seg, s->cur_off) != 0)
		goto fail;
	free(page);
	if (s->cur_seg >= 0)
		fprintf(stderr, "pagestore_daemon: recovered shard %u through segment %d (off %llu)\n",
				shard, s->cur_seg, (unsigned long long) s->cur_off);
	return 0;

fail:
	free(page);
	return -1;
}

/* ===================== request handling (non-I/O ops) ================== */

/*
 * Handle every request that needs no page byte I/O and return 1.  The four
 * byte-I/O ops (EXTEND/WRITEV/READV/READ_AT) -- and any unknown op -- return 0,
 * for the frontend to handle (synchronously for POSIX, async for SPDK).  The
 * frontend sets ch->status = OK and ch->result = 0 before calling.
 */
/*
 * Event LSN for a fork-mutating op.  The backend stamps req_lsn with its WAL
 * position; a legacy/test caller sending 0 gets the fork's newest known event
 * LSN, so the mutation orders after everything already recorded (visible to
 * newest reads, invisible to strictly older horizons -- fail-safe for
 * unstamped callers).
 */
static uint64_t
fork_op_lsn(uint32_t timeline, const PsKey *key, uint64_t req_lsn)
{
	uint64_t	newest;

	if (req_lsn != 0)
		return req_lsn;
	ps_lock_map_rd();
	newest = fork_newest_visible_lsn_through(timeline, key);
	ps_unlock_map();
	/* A WAL-less mutation admitted after snapshot cutover has no historical
	 * position to preserve.  Place an otherwise-empty fork at the selected
	 * cutoff; its freshly allocated admission sequence orders it strictly after
	 * the snapshot.  Explicit nonzero LSNs still pass through unchanged and are
	 * rejected below the cutoff by fork_meta_mutation_future(). */
	if (fork_meta_snapshot_generation != 0 &&
		newest < fork_meta_snapshot_cutoff_lsn)
		return fork_meta_snapshot_cutoff_lsn;
	return newest == UINT64_MAX ? UINT64_MAX : newest + 1;
}

/* Caller holds map_lock for writing and the global admission write lock. */
static int
timeline_has_live_descendant(uint32_t ancestor)
{
	for (uint32_t candidate = 1; candidate < MAX_TIMELINES; candidate++)
	{
		int current;
		uint32_t state;

		if (!timelines[candidate].defined || candidate == ancestor)
			continue;
		state = __atomic_load_n(&timelines[candidate].state, __ATOMIC_ACQUIRE);
		if (state != PS_TIMELINE_LIVE && state != PS_TIMELINE_DELETING)
			continue;
		current = timelines[candidate].parent;
		for (uint32_t hops = 0; current >= 0 && current < MAX_TIMELINES &&
					hops < MAX_TIMELINES; hops++)
		{
			if ((uint32_t) current == ancestor)
				return 1;
			if (!timelines[current].defined)
				break;
			current = timelines[current].parent;
		}
	}
	return 0;
}

static int
timeline_has_active_owner(uint32_t timeline)
{
	PsRetentionPin *pins = NULL;
	uint32_t npins = 0;
	int found = 0;

	/* One immutable snapshot makes the veto independent of owner churn after
	 * admission write lock is acquired.  A poisoned registry fails closed. */
	if (ps_retention_snapshot_alloc(&pins, &npins) != 0)
		return 1;
	for (uint32_t i = 0; i < npins; i++)
		if (pins[i].timeline == timeline)
		{
			found = 1;
			break;
		}
	free(pins);
	return found;
}

static int
timeline_begin_delete(uint32_t timeline, PsChannel *ch)
{
	uint32_t state;
	uint64_t incarnation;

	if (timeline >= MAX_TIMELINES || !timelines[timeline].defined ||
		timeline == 0)
		return -1;
	state = __atomic_load_n(&timelines[timeline].state, __ATOMIC_ACQUIRE);
	incarnation = __atomic_load_n(&timelines[timeline].incarnation,
																						__ATOMIC_ACQUIRE);
	/* req_seq is the caller's fencing token.  An old retry may not turn a
	 * different incarnation into an apparently idempotent success. */
	if (ch->req_seq == 0 || ch->req_seq != incarnation ||
		incarnation == 0 || state == PS_TIMELINE_DELETED)
		return -1;
	if (state == PS_TIMELINE_DELETING)
	{
		ch->result = state;
		ch->req_seq = incarnation;
		return 0;
	}
	if (state != PS_TIMELINE_LIVE || timeline_has_live_descendant(timeline) ||
		timeline_has_active_owner(timeline))
		return -1;
	if (timeline_persist_state(timeline, PS_TIMELINE_DELETING,
										incarnation) != 0)
		return -1;
	/* All writers are drained by the caller's lifecycle/admission fences.  Drop
	 * their staged pages now so shutdown cannot publish a fresh manifest layer
	 * after deletion cleanup has already passed this timeline. */
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		ps_memtable_discard_timeline(g_shards[sh].memtable, timeline);
	/* Durable append precedes this publication.  The POSIX lifecycle gate and
	 * mutation-admission barrier drain complete requests and maintenance before
	 * this point, so idle maintenance may start owner-scoped layer cleanup after
	 * observing DELETING.  SPDK BEGIN_DELETE remains fail-closed until its async
	 * request drain exists. */
	__atomic_store_n(&timelines[timeline].state, PS_TIMELINE_DELETING,
																__ATOMIC_RELEASE);
	ch->result = PS_TIMELINE_DELETING;
	ch->req_seq = incarnation;
	return 0;
}

static int
timeline_op_allowed(uint32_t timeline, PsOpcode opcode,
					uint64_t expected_incarnation)
{
	PsTimelineState state;

	if (timeline_meta_poisoned_load())
		return 0;

	/* Lifecycle control and diagnostics remain available while a timeline is
	 * deleting.  Branch validation is different: a new target is undefined and
	 * may be checked, but an existing target must still be LIVE. */
	if (opcode == PS_OP_BEGIN_DELETE || opcode == PS_OP_TIMELINE_STATE)
		return 1;
	if (opcode == PS_OP_TIMELINE_INFO)
	{
		uint64_t incarnation;

		if (timeline >= MAX_TIMELINES || !timelines[timeline].defined ||
			__atomic_load_n(&timelines[timeline].state, __ATOMIC_ACQUIRE) !=
			PS_TIMELINE_LIVE || expected_incarnation == 0)
			return 0;
		incarnation = __atomic_load_n(&timelines[timeline].incarnation,
										 __ATOMIC_ACQUIRE);
		return incarnation != 0 && expected_incarnation == incarnation;
	}
	if (opcode == PS_OP_CREATE_BRANCH || opcode == PS_OP_CHECK_BRANCH)
	{
		/* The target may be undefined, LIVE (an exact idempotent retry), or
		 * DELETED (the only reusable state).  branch_create_request_ok() fences
		 * the target and parent tokens in the operation-specific manner. */
		return timeline < MAX_TIMELINES;
	}
	if (opcode == PS_OP_REQUIRE_BRANCH)
		return ps_timeline_live(timeline) &&
			ps_timeline_request_allowed(timeline, expected_incarnation);
	if (timeline >= MAX_TIMELINES)
		return 0;
	/* Before lifecycle state existed, shipped WAL and page records could arrive
	 * before ancestry metadata was defined.  Preserve that recovery/import
	 * behavior: only a durably defined non-LIVE timeline is fenced here. */
	if (!ps_timeline_request_allowed(timeline, expected_incarnation))
		return 0;
	if (!__atomic_load_n(&timelines[timeline].defined, __ATOMIC_ACQUIRE))
		return 1;
	state = (PsTimelineState) __atomic_load_n(&timelines[timeline].state,
														__ATOMIC_ACQUIRE);
	return state == PS_TIMELINE_LIVE;
}

int
ps_handle_meta(PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	if (!timeline_op_allowed(tl, (PsOpcode) ch->opcode, ch->incarnation))
	{
		ch->status = PS_STATUS_ERROR;
		return 1;
	}

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE:
			/*
			 * Definitive existence from ch->req_lsn on (the backend stamps
			 * its WAL position; see fork_op_lsn for a legacy 0).  Keep the
			 * empty-generation boundary even if an older live generation still
			 * appears current: its UNLINK may arrive later during replay.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn;
				uint64_t	seq;
				int			delayed;

				/* An unstamped CREATE is an ensure when this fork's generation is
				 * already live.  This includes normal-backend relation opens as well
				 * as object writers; neither may manufacture an empty generation.
				 * A missing/dead fork falls through as a real durable lifecycle
				 * CREATE, ordered by fork_op_lsn() and the admission sequence. */
				if (ch->req_lsn == 0)
				{
					int live;

					ps_lock_map_rd();
					live = fork_exists_through(tl, &ch->key, UINT64_MAX, 0);
					ps_unlock_map();
					if (live)
						break;
				}

				lsn = fork_op_lsn(tl, &ch->key, ch->req_lsn);
				/* XLogReadBufferExtended() asks smgr to ensure the relation fork
				 * exists before applying every redo record.  That call has the
				 * record's LSN but is not a relation-creation record: if the fork
				 * already exists at this replay position, recording FEV_SET would
				 * manufacture a zero-block generation and hide older pages. */
				if (ch->is_redo == 2 && ch->key.klass == PS_KLASS_RELATION)
				{
					int live;

					ps_lock_map_rd();
					live = fork_exists_through(tl, &ch->key, lsn, 0);
					ps_unlock_map();
					if (live)
						break;
				}
				seq = admission_seq_alloc();
				delayed = fork_event_precedes_known_state(e, lsn, seq);

				if (seq == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (!fork_has_create_at(e, lsn))
				{
					if (fork_meta_persist(tl, &ch->key, lsn, seq, 0, FEV_SET) != 0)
						ch->status = PS_STATUS_ERROR;
					else
					{
						fork_event_add(e, lsn, seq, 0, FEV_SET);
						if (delayed)
							fork_restore_later_page_growth(tl, &ch->key, lsn, seq);
						ch->req_seq = seq;
					}
				}
			}
			break;

		case PS_OP_EXISTS:
			/* req_lsn caps the horizon; 0 = newest (the writer path) */
			if (ch->req_lsn != 0 &&
				!page_frontier_ancestry_allows(tl, ch->req_lsn, ch->req_seq))
			{
				ch->status = PS_STATUS_ERROR;
				break;
			}
			if (ch->req_seq != 0 && ch->key.klass == PS_KLASS_RELATION &&
				fork_has_wal_less_page(tl, &ch->key))
			{
				ch->status = PS_STATUS_ERROR;
				break;
			}
			ch->result = fork_exists_through(tl, &ch->key,
										 ch->req_lsn ? ch->req_lsn : UINT64_MAX,
										 ch->req_seq) ? 1 : 0;
			break;

		case PS_OP_UNLINK:
			/*
			 * COW unlink: a durable DEAD event.  The entry and its history
			 * stay -- an as-of read below the unlink LSN must still see the
			 * fork -- so nothing is freed here.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn = fork_op_lsn(tl, &ch->key, ch->req_lsn);
				uint64_t	seq = admission_seq_alloc();
				int			delayed = fork_event_precedes_known_state(e, lsn, seq);

				if (seq == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (fork_meta_persist(tl, &ch->key, lsn, seq, 0, FEV_DEAD) != 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					fork_event_add(e, lsn, seq, 0, FEV_DEAD);
					if (delayed)
						fork_restore_later_page_growth(tl, &ch->key, lsn, seq);
					ch->req_seq = seq;
				}
			}
			break;

		case PS_OP_NBLOCKS:
			/* req_lsn caps the horizon; 0 = newest (the writer path) */
			if (ch->req_lsn != 0 &&
				!page_frontier_ancestry_allows(tl, ch->req_lsn, ch->req_seq))
			{
				ch->status = PS_STATUS_ERROR;
				break;
			}
			if (ch->req_seq != 0 && ch->key.klass == PS_KLASS_RELATION &&
				fork_has_wal_less_page(tl, &ch->key))
			{
				ch->status = PS_STATUS_ERROR;
				break;
			}
			ch->result = ch->is_redo ?
				fork_nblocks_recovery(tl, &ch->key,
					ch->req_lsn ? ch->req_lsn : UINT64_MAX) :
				fork_nblocks_through(tl, &ch->key,
										  ch->req_lsn ? ch->req_lsn : UINT64_MAX,
										  ch->req_seq);
			break;

		case PS_OP_TRUNCATE:
			/*
			 * COW truncate: a durable SET event at the backend's stamped WAL
			 * position.  Historical versions of the trimmed blocks stay in
			 * the log; as-of reads below the truncate LSN still see the old
			 * size, at/above it the new one.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn = fork_op_lsn(tl, &ch->key, ch->req_lsn);
				uint64_t	seq = admission_seq_alloc();
				int			delayed = fork_event_precedes_known_state(e, lsn, seq);

				if (seq == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (fork_meta_persist(tl, &ch->key, lsn, seq, ch->nblocks,
								  FEV_SET) != 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					fork_event_add(e, lsn, seq, ch->nblocks, FEV_SET);
					if (delayed)
						fork_restore_later_page_growth(tl, &ch->key, lsn, seq);
					ch->req_seq = seq;
				}
			}
			break;

		case PS_OP_ZEROEXTEND:
			/*
			 * Allocation only: grow size, no page data stored (reads -> 0).
			 * The segment log has no record of it, so the GROW event must be
			 * persisted -- but only when it actually raises the size at its
			 * horizon, keeping the log as sparse as the in-memory dedup.
			 */
			{
				uint64_t	lsn = fork_op_lsn(tl, &ch->key, ch->req_lsn);
				uint64_t	seq = admission_seq_alloc();
				uint32_t	to = ch->blocknum + ch->nblocks;

				/* Validate the caller's explicit tuple before fork_grow_with_seq()
				 * can clamp its effective LSN to a newer definitive event. */
				if (seq == 0 ||
					(ch->req_lsn != 0 &&
					 !fork_meta_mutation_future(ch->req_lsn, seq)) ||
					fork_grow_with_seq(tl, &ch->key, to, lsn, seq) != 0)
					ch->status = PS_STATUS_ERROR;
				else
					ch->req_seq = seq;
			}
			break;

		case PS_OP_CREATE_BRANCH:
			/*
			 * Instant clone: just record metadata.  Timeline ch->timeline forks
			 * from ch->parent_timeline at LSN ch->req_lsn.  No page data is
			 * copied -- the branch shares the parent's pages by read-through
			 * until it writes (copy-on-write).
			 */
			{
				uint64_t new_incarnation;
				uint64_t parent_incarnation;

				if (!branch_create_request_ok(ch->timeline,
										(int) ch->parent_timeline, ch->req_lsn,
										ch->incarnation, ch->req_seq,
										&new_incarnation))
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				parent_incarnation = __atomic_load_n(
					&timelines[ch->parent_timeline].incarnation,
					__ATOMIC_ACQUIRE);
				if (parent_incarnation == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (timelines[ch->timeline].defined &&
					__atomic_load_n(&timelines[ch->timeline].state,
												__ATOMIC_ACQUIRE) == PS_TIMELINE_LIVE)
				{
					ch->incarnation = new_incarnation;
					break; /* explicitly idempotent exact retry */
				}
				if (timeline_persist_create(ch->timeline,
										(int) ch->parent_timeline, ch->req_lsn,
										new_incarnation, parent_incarnation) == 0)
				{
					if (__atomic_load_n(&timelines[ch->timeline].state,
												__ATOMIC_ACQUIRE) == PS_TIMELINE_DELETED)
						timeline_reset_reuse_runtime(ch->timeline);
					timeline_define_incarnation(ch->timeline,
											(int) ch->parent_timeline, ch->req_lsn,
											new_incarnation, parent_incarnation);
					ch->incarnation = new_incarnation;
				}
				else
					ch->status = PS_STATUS_ERROR;
			}
			break;
		case PS_OP_CHECK_BRANCH:
			/*
			 * Validate a branch request without mutating timeline metadata.
			 * This keeps prepare/retry paths deterministic: invalid requests are
			 * rejected in-place before any SLRU directory mutation.
			 */
			if (branch_create_request_ok(ch->timeline,
										(int) ch->parent_timeline, ch->req_lsn,
										ch->incarnation, ch->req_seq, NULL))
			{
				/* valid */
			}
			else
				ch->status = PS_STATUS_ERROR;
			break;
		case PS_OP_REQUIRE_BRANCH:
			/*
			 * Startup-time manifest validation: require the timeline to already
			 * exist with exactly the manifest ancestry metadata.  This is stricter
			 * than CHECK_BRANCH, which also accepts a request that would be legal
			 * to create.
			 */
			if (!branch_parent_token_ok((int) ch->parent_timeline, ch->req_seq) ||
				!ps_timeline_request_allowed(ch->timeline, ch->incarnation) ||
				!branch_exists_with_metadata(ch->timeline,
													 (int) ch->parent_timeline,
											 ch->req_lsn))
				ch->status = PS_STATUS_ERROR;
			break;
		case PS_OP_TIMELINE_INFO:
			if (tl >= MAX_TIMELINES || !timelines[tl].defined)
				ch->status = PS_STATUS_ERROR;
			else if (timeline_has_parent(tl))
			{
				ch->result = 1;
				ch->parent_timeline = (uint32_t) timelines[tl].parent;
				ch->req_lsn = timelines[tl].branch_lsn;
				ch->req_seq = timelines[tl].parent_incarnation;
			}
			break;
		case PS_OP_BEGIN_DELETE:
			if (timeline_begin_delete(tl, ch) != 0)
				ch->status = PS_STATUS_ERROR;
			break;
		case PS_OP_TIMELINE_STATE:
			{
				PsTimelineState state;
				uint64_t incarnation;

				if (!ps_timeline_state(tl, &state, &incarnation))
					ch->status = PS_STATUS_ERROR;
				else
				{
					ch->result = state;
					ch->req_seq = incarnation;
				}
			}
			break;

		case PS_OP_WAL_APPEND:
			if (wal_append(tl, ch->req_lsn, ch->data, ch->datalen) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_SIZE:
			ch->req_lsn = wal_end_read(tl);	/* output: end LSN of this timeline's WAL */
			break;

		case PS_OP_WAL_READ:
			{
				int64_t n = wal_read(tl, ch->req_lsn, ch->datalen, ch->data);

				if (n < 0)
					ch->status = PS_STATUS_ERROR;
				else
					ch->result = (uint32_t) n;
			}
			break;

		case PS_OP_WAL_INDEX_ADD:
			if (walidx_add(tl, &ch->key, ch->blocknum, ch->req_lsn) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_INDEX_ADD_BATCH:
			if (ch->nblocks == 0 ||
				ch->datalen % sizeof(PsWalIndexEntry) != 0 ||
				ch->nblocks != ch->datalen / sizeof(PsWalIndexEntry) ||
				ch->datalen > PS_IO_UNIT)
				ch->status = PS_STATUS_ERROR;
			else
			{
				PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;
				uint32_t shard = ps_shard_of(&ch->key);

				walidx_publish_rdlock();
				if (ps_shard_of(&entries[0].key) != shard ||
					walidx_add_batch_locked(tl, entries, ch->nblocks) != 0)
					ch->status = PS_STATUS_ERROR;
				walidx_publish_rdunlock();
			}
			break;

		case PS_OP_WAL_INDEX_GET:
			{
				int max_out = (int) (PS_IO_UNIT / sizeof(PsWalRec));
				int n;

				if (ch->nblocks > 0 && ch->nblocks < (uint32_t) max_out)
					max_out = (int) ch->nblocks;
				walidx_publish_rdlock();
				n = walidx_get(tl, &ch->key, ch->blocknum, ch->req_lsn,
								   ch->pad1 != 0, ch->req_seq, ch->parent_timeline,
								   (PsWalRec *) ch->data, max_out);
				walidx_publish_rdunlock();

				if (n < 0)
					ch->status = PS_STATUS_ERROR;
				else
					ch->result = (uint32_t) n;
			}
			break;

		case PS_OP_WAL_INDEX_PROGRESS:
			if (tl >= MAX_TIMELINES)
				ch->status = PS_STATUS_ERROR;
			else if (ch->req_lsn == 0 && ch->req_seq == 0)
			{
				uint64_t progress = walidx_progress_read(tl);
				uint64_t first = progress == 0 ? wal_log_start(tl) : progress;

				ch->req_lsn = first == UINT64_MAX ? 0 : first;
			}
			else if (walidx_commit(tl, ch->req_lsn, ch->req_seq) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_RETAIN_FLOOR:
			/*
			 * Durable WAL retention floor for this timeline's ancestry.  A
			 * floor that cannot be PROVEN (unreadable note, or a control
			 * image predating the note format) is an error, never a lower
			 * bound: a GC caller must retain everything in that case.
			 */
			{
				uint64_t	floor = 0;

				if (wal_retain_floor(tl, &floor) != 0)
					ch->status = PS_STATUS_ERROR;
				ch->req_lsn = floor;
				ch->result = (floor != 0);
			}
			break;

		case PS_OP_RETENTION_PIN_SET:
			{
				PsRetentionPin pin;
				PsRetentionPin old_pin;
				int			ret;
				int			old_found;
				int			timeline_defined;
				int			timeline_live;
				int			page_history_allowed;
				int			wal_allowed;
				int			wal_index_allowed;
				int			wal_index_pending;

				memset(&pin, 0, sizeof(pin));
				pin.timeline = tl;
				pin.owner_kind = ch->blocknum;
				pin.resources = ch->parent_timeline;
				pin.generation = ch->old_nblocks;
				pin.owner_id = ch->req_seq;
				pin.lsn = ch->req_lsn;
				pin.admission_seq = (uint64_t) ch->nblocks |
					(uint64_t) ch->pad1 << 32;
				/* Generation zero exists only for replaying pre-v27 retention
				 * records.  It is never valid on the current IPC boundary. */
				if (ch->old_nblocks == 0 || tl >= MAX_TIMELINES)
					ret = PS_RETENTION_ERROR;
				else
				{
					/* A retried controller fence may be newer than this
					 * process's recovered allocator.  Serialize its durable SET
					 * with mutations and advance allocation before admitting more. */
					if (admission_write_lock() != 0)
						ret = PS_RETENTION_ERROR;
					else
					{
					pthread_rwlock_wrlock(&page_prune_lock);
					pthread_rwlock_wrlock(&walidx_prune_lock);
					old_found = ps_retention_lookup(tl, pin.owner_kind,
						pin.owner_id, &old_pin);
					ps_lock_map_rd();
					timeline_defined = __atomic_load_n(&timelines[tl].defined,
																						__ATOMIC_ACQUIRE);
					timeline_live = timeline_defined &&
						__atomic_load_n(&timelines[tl].state, __ATOMIC_ACQUIRE) ==
						PS_TIMELINE_LIVE;
					/* Recheck LIVE under map_lock while admission write is held;
					 * this serializes the owner update with BEGIN_DELETE. */
					page_history_allowed = timeline_live &&
						page_frontier_ancestry_allows(tl, pin.lsn,
							pin.admission_seq);
					wal_allowed = timeline_live &&
						wal_reclaim_frontier_ancestry_allows(tl, pin.lsn);
					wal_index_allowed = timeline_live &&
						walidx_frontier_ancestry_allows(tl, pin.lsn);
					wal_index_pending = timeline_live &&
						walidx_frontier_ancestry_pending(tl);
					ps_unlock_map();
					wal_index_pending = wal_index_pending &&
						((((old_found == 1 ? old_pin.resources : 0) |
						   pin.resources) & PS_RETENTION_RESOURCE_WAL_INDEX) != 0) &&
						!(old_found == 1 &&
						  memcmp(&old_pin, &pin, sizeof(pin)) == 0);
					/* Page history permits an active owner to advance from its old
					 * protected image.  WAL-index history is sparse after compaction,
					 * so its new point must independently name a retained chain. */
					ret = ((((pin.resources &
							  PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0 &&
							 !page_history_allowed &&
							 !(old_found == 1 &&
							   old_pin.generation == pin.generation &&
							   old_pin.resources == pin.resources &&
							   (old_pin.lsn < pin.lsn ||
								/* An exact retry cannot expose a new fence. */
								(old_pin.lsn == pin.lsn &&
								 old_pin.admission_seq == pin.admission_seq)))) ||
							((pin.resources & PS_RETENTION_RESOURCE_WAL) != 0 &&
							 !wal_allowed) ||
							((pin.resources & PS_RETENTION_RESOURCE_WAL_INDEX) != 0 &&
							 !wal_index_allowed) || wal_index_pending) ||
							!timeline_live) ?
						PS_RETENTION_ERROR : ps_retention_set(&pin);
					if (ret == PS_RETENTION_OK)
					{
						admission_seq_observe(pin.admission_seq);
						if ((old_found != 1 ||
							 memcmp(&old_pin, &pin, sizeof(pin)) != 0) &&
							(((old_found == 1 ? old_pin.resources : 0) |
							  pin.resources) &
							 PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
							page_prune_mark_all_due();
					}
					pthread_rwlock_unlock(&walidx_prune_lock);
					pthread_rwlock_unlock(&page_prune_lock);
					ps_admission_write_unlock();
					}
				}
				if (ret == PS_RETENTION_STALE)
					ch->status = PS_STATUS_STALE;
				else if (ret != PS_RETENTION_OK)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_RETENTION_PIN_RESERVE:
			{
				PsRetentionPin pin;
				PsRetentionPin old_pin;
				uint64_t	seq;
				int			ret = PS_RETENTION_ERROR;
				int			old_found = 0;
				int			timeline_defined = 0;
				int			timeline_live = 0;
				int			page_history_allowed = 0;
				int			wal_allowed = 0;
				int			wal_index_allowed = 0;
				int			wal_index_pending = 0;

				memset(&pin, 0, sizeof(pin));
				pin.timeline = tl;
				pin.owner_kind = ch->blocknum;
				pin.resources = ch->parent_timeline;
				pin.generation = ch->old_nblocks;
				pin.owner_id = ch->req_seq;
				pin.lsn = ch->req_lsn;
				if (admission_write_lock() == 0)
				{
				seq = admission_seq_alloc();
				pin.admission_seq = seq;
				pthread_rwlock_wrlock(&page_prune_lock);
				pthread_rwlock_wrlock(&walidx_prune_lock);
				if (seq != 0 && ch->old_nblocks != 0 && tl < MAX_TIMELINES)
				{
					old_found = ps_retention_lookup(tl, pin.owner_kind,
						pin.owner_id, &old_pin);
					ps_lock_map_rd();
					timeline_defined = __atomic_load_n(&timelines[tl].defined,
																						__ATOMIC_ACQUIRE);
					timeline_live = timeline_defined &&
						__atomic_load_n(&timelines[tl].state, __ATOMIC_ACQUIRE) ==
						PS_TIMELINE_LIVE;
					/* Recheck LIVE under map_lock after admission write, not merely
					 * defined, so BEGIN_DELETE cannot race this owner publication. */
					page_history_allowed = timeline_live &&
						page_frontier_ancestry_allows(tl, pin.lsn,
							pin.admission_seq);
					wal_allowed = timeline_live &&
						wal_reclaim_frontier_ancestry_allows(tl, pin.lsn);
					wal_index_allowed = timeline_live &&
						walidx_frontier_ancestry_allows(tl, pin.lsn);
					wal_index_pending = timeline_live &&
						walidx_frontier_ancestry_pending(tl);
					ps_unlock_map();
					wal_index_pending = wal_index_pending &&
						((((old_found == 1 ? old_pin.resources : 0) |
						   pin.resources) & PS_RETENTION_RESOURCE_WAL_INDEX) != 0);
					if (timeline_live &&
						(((pin.resources &
						   PS_RETENTION_RESOURCE_PAGE_HISTORY) == 0) ||
						 page_history_allowed) &&
						(((pin.resources & PS_RETENTION_RESOURCE_WAL) == 0) ||
						 wal_allowed) &&
						(((pin.resources & PS_RETENTION_RESOURCE_WAL_INDEX) == 0) ||
						 wal_index_allowed) && !wal_index_pending)
						ret = ps_retention_reserve_and_set(&pin);
				}
				if (ret == PS_RETENTION_OK)
				{
					memcpy(ch->data, &seq, sizeof(seq));
					ch->datalen = sizeof(seq);
					if ((old_found != 1 ||
						 memcmp(&old_pin, &pin, sizeof(pin)) != 0) &&
						(((old_found == 1 ? old_pin.resources : 0) |
						  pin.resources) &
						 PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
						page_prune_mark_all_due();
				}
				pthread_rwlock_unlock(&walidx_prune_lock);
				pthread_rwlock_unlock(&page_prune_lock);
				ps_admission_write_unlock();
				}
				if (ret == PS_RETENTION_STALE)
					ch->status = PS_STATUS_STALE;
				else if (ret != PS_RETENTION_OK)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_RETENTION_PIN_DROP:
			{
				PsRetentionPin old_pin;
				int			ret;
				int			old_found;
				int			timeline_defined;
				int			wal_index_pending;

				pthread_rwlock_wrlock(&page_prune_lock);
				pthread_rwlock_wrlock(&walidx_prune_lock);
				old_found = ps_retention_lookup(tl, ch->blocknum,
											ch->req_seq, &old_pin);
				ps_lock_map_rd();
				timeline_defined = tl < MAX_TIMELINES && timelines[tl].defined;
				wal_index_pending = timeline_defined && old_found == 1 &&
					(old_pin.resources & PS_RETENTION_RESOURCE_WAL_INDEX) != 0 &&
					walidx_frontier_ancestry_pending(tl);
				ps_unlock_map();
				ret = (ch->old_nblocks == 0 || tl >= MAX_TIMELINES ||
					   !timeline_defined || wal_index_pending) ?
					PS_RETENTION_ERROR :
					ps_retention_drop(tl, ch->blocknum, ch->req_seq,
									  ch->old_nblocks);
				if (ret == PS_RETENTION_OK && old_found == 1 &&
					(old_pin.resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
					page_prune_mark_all_due();
				pthread_rwlock_unlock(&walidx_prune_lock);
				pthread_rwlock_unlock(&page_prune_lock);
				if (ret == PS_RETENTION_STALE)
					ch->status = PS_STATUS_STALE;
				else if (ret != PS_RETENTION_OK)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_RETENTION_PIN_GET:
			{
				PsRetentionPin pin;
				PsRetentionGetResult result;
				uint32_t	count = 0;
				uint64_t	epoch = ch->req_lsn;
				int			found = ps_retention_get_consistent(ch->blocknum,
														   &epoch, &pin, &count);

				if (found == PS_RETENTION_STALE)
				{
					ch->status = PS_STATUS_STALE;
					ch->req_lsn = epoch;
				}
				else if (found < 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					ch->nblocks = count;
					memset(&result, 0, sizeof(result));
					result.mutation_epoch = epoch;
					if (found)
					{
						ch->result = 1;
						ch->timeline = pin.timeline;
						ch->blocknum = pin.owner_kind;
						ch->parent_timeline = pin.resources;
						ch->old_nblocks = pin.generation;
						ch->req_seq = pin.owner_id;
						ch->req_lsn = pin.lsn;
						result.admission_seq = pin.admission_seq;
					}
					memcpy(ch->data, &result, sizeof(result));
					ch->datalen = sizeof(result);
				}
			}
			break;

		case PS_OP_RETENTION_PIN_LOOKUP:
			{
				PsRetentionPin pin;
				int			found = ps_retention_lookup(tl, ch->blocknum,
												 ch->req_seq, &pin);

				if (found < 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					ch->result = found != 0;
					if (found)
					{
						ch->timeline = pin.timeline;
						ch->blocknum = pin.owner_kind;
						ch->parent_timeline = pin.resources;
						ch->old_nblocks = pin.generation;
						ch->req_seq = pin.owner_id;
						ch->req_lsn = pin.lsn;
						memcpy(ch->data, &pin.admission_seq,
							   sizeof(pin.admission_seq));
						ch->datalen = sizeof(pin.admission_seq);
					}
				}
			}
			break;

		case PS_OP_RETENTION_FLOOR:
			{
				uint64_t	floor = 0;

				if (retention_effective_floor(tl, ch->parent_timeline,
										  &floor) != 0)
					ch->status = PS_STATUS_ERROR;
				ch->req_lsn = floor;
				ch->result = (floor != 0);
			}
			break;

		case PS_OP_IMMEDSYNC:
			/*
			 * Surface the failure: callers (the pg_control mirror) pop their
			 * retry queues only after a successful sync, and an ignored
			 * ENOSPC/EIO here would let them treat pwrite-only images as
			 * durable.
			 */
			if (ps_storage->sync() != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		default:
			return 0;			/* a byte-I/O op (or unknown): frontend handles */
	}
	return 1;
}

/* ===================== lifecycle ====================================== */

/* Flush the memtable, commit its coverage watermark, and close the manifest. */
void
ps_core_close(void)
{
	uint32_t	ns = core_shards();
	int		join_gc = 0;
	int		join_upload = 0;
	int		join_evict = 0;
	int		evict_state;
	struct timespec deadline;
	int		join_rc;

	if (__atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE) != 0)
	{
		if (__atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE) == 1)
			pthread_cancel(gc_remote_thread);
		join_gc = 1;
	}
	if (__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) != 0)
	{
		/* Remote tiering is optional: cancel only the provider I/O.  The worker
		 * defers cancellation during publication, and every nonzero state is
		 * joined before core state is torn down. */
		if (__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1)
			pthread_cancel(tier_upload_thread);
		join_upload = 1;
	}
	evict_state = __atomic_load_n(&evict_local_state, __ATOMIC_ACQUIRE);
	if (evict_state != 0)
	{
		if (evict_state == 1)
			pthread_cancel(evict_local_thread);
		join_evict = 1;
	}

	/*
	 * The uncovered segment tail must be durable before shutdown (writes between
	 * checkpoints are otherwise only in the OS page cache, and would be lost to a
	 * power failure after the daemon exits even though the write was acknowledged).
	 *
	 * Sync before flushing below so a failed layer/manifest commit still leaves a
	 * durable segment fallback.  A sync error means acknowledged tail writes may
	 * not be durable, so abort before destroying the memtables.
	 */
	if (ps_storage->sync && ps_storage->sync() != 0)
	{
		fprintf(stderr, "pagestore_daemon: FATAL: segment sync failed on shutdown "
				"(%s); aborting before teardown -- recently acknowledged writes "
				"may not be durable\n", strerror(errno));
		_exit(EXIT_FAILURE);
	}
	if (join_gc)
	{
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec++;
		if (pthread_timedjoin_np(gc_remote_thread, NULL, &deadline) != 0)
		{
			fprintf(stderr, "pagestore_daemon: FATAL: remote GC did not stop during shutdown\n");
			_exit(EXIT_FAILURE);
		}
		__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
	}
	if (join_upload)
	{
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec++;
		join_rc = pthread_timedjoin_np(tier_upload_thread, NULL, &deadline);
		if (join_rc != 0)
		{
			/* Do not detach an upload that still owns core/provider state.  The
			 * local store is synced below; terminate the process so the kernel
			 * reclaims the stuck worker rather than permitting a concurrent reopen. */
			fprintf(stderr, "pagestore_daemon: FATAL: tier upload did not stop during shutdown\n");
			_exit(EXIT_FAILURE);
		}
		__atomic_store_n(&tier_upload_state, 0, __ATOMIC_RELEASE);
	}
	if (join_evict)
	{
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec++;
		join_rc = pthread_timedjoin_np(evict_local_thread, NULL, &deadline);
		if (join_rc != 0)
		{
			fprintf(stderr, "pagestore_daemon: FATAL: local eviction verifier did not stop during shutdown\n");
			_exit(EXIT_FAILURE);
		}
		__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
	}

	for (uint32_t i = 0; i < ns; i++)
	{
		Shard	   *s = &g_shards[i];

		if (s->memtable)
		{
			flush_memtable(s, (uint32_t) s->cur_seg, s->cur_off);
			ps_memtable_destroy(s->memtable);
			s->memtable = NULL;
		}
	}

	ps_pgcache_free();
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (wal_segment_store_opened[tl])
		{
			ps_wal_store_close(&wal_segment_stores[tl]);
			wal_segment_store_opened[tl] = 0;
		}
	ps_retention_close();
	ps_manifest_close();
	free_page_fork_indexes();
	free_walidx_indexes();
}

static int
segment_has_references(uint32_t source_shard, uint32_t victim)
{
	uint32_t	ns = core_shards();

	for (uint32_t sh = 0; sh < ns; sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (PageEnt *e = g_shards[sh].page_idx[bucket]; e; e = e->next)
				for (int i = 0; i < e->nver; i++)
					if (e->vers[i].shard == source_shard &&
						e->vers[i].seg == (int) victim)
						return 1;
	return 0;
}

static int
prepare_segment_layers(uint32_t source_shard, uint32_t victim)
{
	PsLayerDesc *layers = NULL;
	uint32_t	nlayers = 0;
	int			rc = 0;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];

		if (ps_timeline_live(d->timeline) &&
			d->kind == PS_LAYER_IMAGE && !d->deleting &&
			layer_shard_from_id(d->layer_id) == source_shard)
			nlayers++;
	}
	if (nlayers != 0)
	{
		layers = malloc((size_t) nlayers * sizeof(*layers));
		if (layers == NULL)
		{
			ps_unlock_map();
			return -1;
		}
		nlayers = 0;
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		{
			PsLayerDesc *d = &ps_layer_map.layers[i];

			if (ps_timeline_live(d->timeline) &&
				d->kind == PS_LAYER_IMAGE && !d->deleting &&
				layer_shard_from_id(d->layer_id) == source_shard)
				layers[nlayers++] = *d;
		}
	}
	ps_unlock_map();

	for (uint32_t i = 0; i < nlayers; i++)
	{
		PsImgIndexEnt *idx;
		uint32_t	n;
		int			covers = 0;

		if (read_image_index_refreshing(&layers[i], &idx, &n) != 0)
		{
			rc = -1;
			break;
		}
		for (uint32_t j = 0; j < n; j++)
			if ((idx[j].flags & PS_IMG_REC_SEG_VALID) &&
				idx[j].seg_id == victim)
			{
				covers = 1;
				break;
			}
		free(idx);
		if (covers && verify_image_layer_refreshing(&layers[i]) != 0)
		{
			rc = -1;
			break;
		}
	}
	free(layers);
	return rc;
}

static int
verify_segment_layers_locked(uint32_t source_shard, uint32_t victim, int need_layer)
{
	int			found = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];
		PsImgIndexEnt *idx;
		uint32_t	n;
		int			covers = 0;

		if (!ps_timeline_live(d->timeline) ||
			d->kind != PS_LAYER_IMAGE || d->deleting ||
			layer_shard_from_id(d->layer_id) != source_shard)
			continue;
		if (ps_image_layer_read_index(d, &idx, &n) != 0)
			return -1;
		for (uint32_t j = 0; j < n; j++)
			if ((idx[j].flags & PS_IMG_REC_SEG_VALID) &&
				idx[j].seg_id == victim)
			{
				covers = 1;
				break;
			}
		free(idx);
		if (covers)
		{
			found = 1;
			/* Always re-read and checksum now: a prior read's cached verification
			 * may predate corruption that occurred before this unlink. */
			if (ps_image_layer_verify_data(d, page_size) != 0)
				return -1;
		}
		/* Keep any remote-only cache materialized while examining this segment.
		 * Segment GC advances one victim at a time, and dropping it here makes
		 * every later victim download the same verified layer again while all
		 * shard write locks are held.  Idle eviction owns eventual cache cleanup. */
	}
	return need_layer && !found ? -1 : 0;
}

static int
reclaim_one_segment(Shard *s)
{
	uint32_t	victim;
	uint32_t	ns = core_shards();
	int64_t		seg_bytes;
	int			refs;

	if (!s->flush_watermark_valid || !ps_storage->seg_remove ||
		s->gc_next_seg >= s->flush_watermark.seg_id)
		return 0;
	victim = s->gc_next_seg;
	if (ps_storage->seg_size != NULL)
	{
		errno = 0;
		seg_bytes = ps_storage->seg_size(s->id, (int) victim);
		if (seg_bytes < 0)
		{
			if (errno == ENOENT)
			{
				/* A sparse hole is not debt.  A prior remove may have
				 * unlinked it before reporting an ambiguous directory fsync;
				 * settle that already-counted victim exactly once when its
				 * absence is confirmed. */
				page_gc_debt_settle(s, victim, 0);
				s->gc_next_seg++;
				return 1;
			}
			if (errno == 0)
				errno = EIO;
			s->gc_storage_error = 1;
			return 0;
		}
	}
	else
		seg_bytes = (int64_t) segment_size;
	refs = segment_has_references(s->id, victim);
	if (verify_segment_layers_locked(s->id, victim, refs) != 0)
		return 0;
	for (uint32_t sh = 0; sh < ns; sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (PageEnt *e = g_shards[sh].page_idx[bucket]; e; e = e->next)
				for (int i = 0; i < e->nver; i++)
					if (e->vers[i].shard == s->id &&
						e->vers[i].seg == (int) victim)
					{
						e->vers[i].seg = -1;
						e->vers[i].off = 0;
					}
	/* Once a positive-size covered victim is known to be counted, remember it
	 * before remove().  POSIX may unlink it and then fail the directory fsync;
	 * the next ENOENT observation must settle the same debt unit. */
	if (!s->gc_pending_remove && seg_bytes > 0 &&
		s->gc_debt_segments != 0)
	{
		s->gc_pending_remove_seg = victim;
		s->gc_pending_remove = 1;
	}
	if (ps_storage->seg_remove(s->id, (int) victim) != 0)
		return 0;
	page_gc_debt_settle(s, victim, 0);
	s->gc_next_seg++;
	return 1;
}

static const PsLayerLocation *
tier_local_location(const PsLayerDesc *layer)
{
	for (uint32_t i = 0; i < layer->location_count; i++)
		if ((layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			 layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD) &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

static const PsLayerLocation *
tier_remote_location(const PsLayerDesc *layer)
{
	for (uint32_t i = 0; i < layer->location_count; i++)
		if (layer->locations[i].tier == PS_LAYER_TIER_REMOTE_OBJECT &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

static int
refresh_remote_only_layer(const PsLayerDesc *layer)
{
	if (tier_local_location(layer) != NULL ||
		tier_remote_location(layer) == NULL ||
		ps_layer_store->refresh_layer_cache == NULL)
		return -1;
	return ps_layer_store->refresh_layer_cache(layer);
}

static int
read_image_index_refreshing(const PsLayerDesc *layer, PsImgIndexEnt **idx,
							uint32_t *n)
{
	if (ps_image_layer_read_index(layer, idx, n) == 0)
		return 0;
	if (refresh_remote_only_layer(layer) != 0)
		return -1;
	return ps_image_layer_read_index(layer, idx, n);
}

static int
read_layer_block_refreshing(const PsLayerDesc *layer, uint64_t off,
							void *buf, uint32_t len)
{
	if (ps_layer_store->read_layer_block(layer, off, buf, len) == 0)
		return 0;
	if (refresh_remote_only_layer(layer) != 0)
		return -1;
	return ps_layer_store->read_layer_block(layer, off, buf, len);
}

static int
verify_image_layer_refreshing(const PsLayerDesc *layer)
{
	if (ps_image_layer_verify_data(layer, page_size) == 0)
		return 0;
	if (refresh_remote_only_layer(layer) != 0)
		return -1;
	return ps_image_layer_verify_data(layer, page_size);
}

/* Publish the remote location and durability marker for an uploaded layer.
 * The caller must have deferred cancellation disabled and hold the worker's
 * lifecycle reservation for the entire operation. */
static int
finish_upload(const PsLayerDesc *candidate)
{
	PsLayerDesc *current = NULL;
	PsLayerLocation remote;
	const PsLayerLocation *local;

	ps_lock_map_wr();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == candidate->layer_id)
		{
			current = &ps_layer_map.layers[i];
			break;
		}
	if (current == NULL || current->deleting ||
		!ps_timeline_live(current->timeline))
	{
		ps_unlock_map();
		return 1;
	}
	if (current->remote_durable)
	{
		ps_unlock_map();
		return 1;
	}
	if (tier_remote_location(current) == NULL)
	{
		local = tier_local_location(current);
		if (local == NULL)
		{
			ps_unlock_map();
			return 0;
		}
		memset(&remote, 0, sizeof(remote));
		remote.tier = PS_LAYER_TIER_REMOTE_OBJECT;
		remote.size = local->size;
		remote.available = true;
		if (ps_layer_store->remote_uri(current->layer_id, remote.uri,
										 sizeof(remote.uri)) != 0 ||
			ps_manifest_set_remote_location(current->layer_id, &remote) != 0)
		{
			ps_unlock_map();
			return 0;
		}
	}
	if (ps_manifest_set_remote_durable(current->layer_id, current->lsn_end) != 0)
	{
		ps_unlock_map();
		return 0;
	}
	ps_unlock_map();
	return 1;
}

static void *
tier_upload_worker(void *arg)
{
	PsLayerDesc *layer = arg;
	int old_state;
	int			rc;

	/* Shutdown cancels optional object copies rather than waiting for a slow
	 * object mount.  The cleanup handler releases its lifecycle reservation. */
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	ps_lifecycle_read_adopt_reserved();
	pthread_cleanup_push(lifecycle_worker_cleanup, NULL);
	rc = ps_layer_store->upload_layer(layer);
	if (tier_upload_before_publish_test_hook != NULL)
		tier_upload_before_publish_test_hook(
			tier_upload_before_publish_test_hook_arg);
	/* Keep the reservation through map/manifest publication.  In particular,
	 * do not let shutdown cancellation split a successful remote upload from
	 * its local durable publication. */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_state);
	if (rc == 0)
		rc = finish_upload(layer) ? 0 : -1;
	__atomic_store_n(&tier_upload_state, rc == 0 ? 2 : 3, __ATOMIC_RELEASE);
	pthread_setcancelstate(old_state, NULL);
	pthread_cleanup_pop(1);
	return NULL;
}

/* Start or finish one immutable-layer upload without blocking a shard worker. */
static int
tier_one_layer(void)
{
	PsLayerDesc candidate;
	PsLayerLocation remote;
	struct timespec now;
	int			state;
	int			found = 0;

	if (ps_layer_store->remote_uri == NULL || ps_layer_store->upload_layer == NULL)
		return 0;
	/* The local provider is always installed, but exposes remote callbacks even
	 * when PAGESTORE_OBJECT_DIR is disabled.  Probe configuration before
	 * scheduling a worker so local-only stores do not spin on ENOTSUP uploads. */
	if (ps_layer_store->remote_uri(0, remote.uri, sizeof(remote.uri)) != 0)
		return 0;
	clock_gettime(CLOCK_MONOTONIC, &now);
	if (tier_upload_retry_at.tv_sec != 0 &&
		(now.tv_sec < tier_upload_retry_at.tv_sec ||
		 (now.tv_sec == tier_upload_retry_at.tv_sec &&
		  now.tv_nsec < tier_upload_retry_at.tv_nsec)))
		return 0;
	state = __atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE);
	if (state == 1)
		return 0;
	if (state != 0)
	{
		pthread_join(tier_upload_thread, NULL);
		__atomic_store_n(&tier_upload_state, 0, __ATOMIC_RELEASE);
		if (state != 2)
		{
			clock_gettime(CLOCK_MONOTONIC, &tier_upload_retry_at);
			tier_upload_retry_at.tv_sec++;
			return 0;
		}
		memset(&tier_upload_retry_at, 0, sizeof(tier_upload_retry_at));
		return 1;
	}
	ps_lock_map_rd();
	for (uint32_t pass = 0; pass < core_shards() && !found; pass++)
	{
		uint32_t shard = (tier_upload_shard_cursor + pass) % core_shards();

		for (uint32_t phase = 0; phase < 2 && !found; phase++)
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		{
			PsLayerDesc *layer = &ps_layer_map.layers[i];

			if (ps_timeline_live(layer->timeline) && !layer->deleting &&
				!layer->remote_durable &&
				tier_local_location(layer) != NULL &&
				layer_shard_from_id(layer->layer_id) == shard &&
				(tier_remote_location(layer) == NULL ||
				 (ps_layer_store->remote_uri(layer->layer_id, remote.uri,
										 sizeof(remote.uri)) == 0 &&
				  strcmp(tier_remote_location(layer)->uri, remote.uri) == 0)) &&
				((phase == 0 && layer->layer_id > tier_upload_layer_cursor[shard]) ||
				 (phase == 1 && layer->layer_id <= tier_upload_layer_cursor[shard])))
			{
				candidate = *layer;
				found = 1;
				break;
			}
		}
	}
	ps_unlock_map();
	if (!found)
		return 0;

	tier_upload_shard_cursor = (layer_shard_from_id(candidate.layer_id) + 1) % core_shards();
	tier_upload_layer_cursor[layer_shard_from_id(candidate.layer_id)] = candidate.layer_id;
	tier_upload_candidate = candidate;
	ps_lifecycle_read_reserve();
	__atomic_store_n(&tier_upload_state, 1, __ATOMIC_RELEASE);
	if (pthread_create(&tier_upload_thread, NULL, tier_upload_worker,
					   &tier_upload_candidate) != 0)
	{
		ps_lifecycle_read_cancel_reservation();
		__atomic_store_n(&tier_upload_state, 0, __ATOMIC_RELEASE);
		return 0;
	}
	return 1;
}

/* Complete local eviction after remote verification, while the worker still
 * owns its lifecycle reservation. */
static int
finish_evict(const PsLayerDesc *candidate)
{
	int found = 0;

	ps_lock_map_wr();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == candidate->layer_id)
		{
			PsLayerDesc *layer = &ps_layer_map.layers[i];

			if (!ps_timeline_live(layer->timeline) || layer->deleting ||
				!layer->remote_durable || layer->local_pinned ||
				__atomic_load_n(&layer->cache_readers, __ATOMIC_ACQUIRE) != 0 ||
				(!layer->local_cleanup_pending &&
				 ps_layer_store->layer_exists_local(layer->layer_id) != 1))
			{
				ps_unlock_map();
				return 0;
			}
			if (tier_local_location(layer) != NULL &&
				ps_manifest_drop_local(candidate->layer_id) != 0)
			{
				ps_unlock_map();
				return 0;
			}
			/* A later cache refill installs different physical bytes; require
			 * the image data checksum to be verified again before serving it. */
			layer->data_verified = false;
			layer->cache_resident = false;
			layer->local_cleanup_pending = true;
			found = 1;
			break;
		}
	if (!found)
	{
		ps_unlock_map();
		return 0;
	}
	/* Keep the write lock through unlink: layer reads hold the matching read
	 * lock while downloading/opening their cache file. */
	found = (ps_layer_store->delete_local_layer(candidate) == 0);
	if (found)
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == candidate->layer_id)
			{
				ps_layer_map.layers[i].local_cleanup_pending = false;
				break;
			}
	ps_unlock_map();
	return found;
}

static void *
evict_local_worker(void *arg)
{
	PsLayerDesc *layer = arg;
	volatile int rc = -1;
	int old_state;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	ps_lifecycle_read_adopt_reserved();
	pthread_cleanup_push(lifecycle_worker_cleanup, NULL);
	if (ps_layer_store->verify_remote_layer != NULL)
		rc = ps_layer_store->verify_remote_layer(layer);
	else if (ps_layer_store->layer_exists_remote != NULL &&
			 ps_layer_store->layer_exists_remote(layer) == 1)
		rc = 0;
	/* Verification and manifest-drop/unlink publication share this lifecycle
	 * reservation.  Do not allow deferred cancellation while publication holds
	 * map-wr or performs manifest/filesystem I/O. */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_state);
	if (rc == 0)
		rc = finish_evict(layer) ? 0 : -1;
	__atomic_store_n(&evict_local_state, rc == 0 ? 2 : 3, __ATOMIC_RELEASE);
	pthread_setcancelstate(old_state, NULL);
	pthread_cleanup_pop(1);
	return NULL;
}

/* Evict at most one remote-durable local cache file. */
static int
evict_one_layer(void)
{
	PsLayerDesc candidate;
	int			state;
	int			found = 0;
	uint32_t	found_idx = 0;
	uint32_t	map_nlayers = 0;

	if (ps_layer_store->layer_exists_local == NULL ||
		ps_layer_store->delete_local_layer == NULL ||
		(ps_layer_store->verify_remote_layer == NULL &&
		 ps_layer_store->layer_exists_remote == NULL))
		return 0;
	state = __atomic_load_n(&evict_local_state, __ATOMIC_ACQUIRE);
	if (state == 1)
		return 0;
	if (state != 0)
	{
		pthread_join(evict_local_thread, NULL);
		__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
		return 1;
	}

	ps_lock_map_rd();
	map_nlayers = ps_layer_map.nlayers;
	for (uint32_t pass = 0; pass < map_nlayers; pass++)
	{
		uint32_t	i = (evict_local_map_cursor + pass) % map_nlayers;
		PsLayerDesc *layer = &ps_layer_map.layers[i];

		if (ps_timeline_live(layer->timeline) && !layer->deleting &&
			layer->remote_durable && !layer->local_pinned &&
			__atomic_load_n(&layer->cache_readers, __ATOMIC_ACQUIRE) == 0 &&
			(layer->local_cleanup_pending ||
			 ps_layer_store->layer_exists_local(layer->layer_id) == 1))
		{
			candidate = *layer;
			found_idx = i;
			found = 1;
			break;
		}
	}
	ps_unlock_map();
	if (!found)
		return 0;
	evict_local_map_cursor = (found_idx + 1) % map_nlayers;
	evict_local_candidate = candidate;
	ps_lifecycle_read_reserve();
	__atomic_store_n(&evict_local_state, 1, __ATOMIC_RELEASE);
	if (pthread_create(&evict_local_thread, NULL, evict_local_worker,
					   &evict_local_candidate) != 0)
	{
		ps_lifecycle_read_cancel_reservation();
		__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
		return 0;
	}
	return 1;
}

/*
 * Off-the-write-path background maintenance: compact one timeline whose image
 * layer count exceeds the (low-water) threshold.  The maintenance controller
 * calls this repeatedly; doing at most one compaction per call lets other
 * maintenance classes make progress.  Returns 1 if it did work (the caller
 * should run another tick), 0 if nothing was due.
 */
static int
ps_core_maintenance_impl(void)
{
	uint32_t	ns;
	uint32_t	ftl = 0,
				fsh = 0;
	uint64_t	page_floor = 0;
	int			found = 0;
	int			did = 0;
	int			legacy_compaction = 0;

	/* A failed timeline metadata append may already be durable.  Until reopen
	 * resolves that ambiguity, background work must fail closed alongside the
	 * request path instead of publishing more timeline-derived metadata. */
	if (timeline_meta_poisoned_load())
		return 0;

	/* Pin churn is independent of the layer read path (SPDK uses the same host
	 * metadata), so bound this log before considering LSM-only work. */
	if (ps_retention_should_compact())
		return ps_retention_compact() == 0;
	if (walidx_snapshot_gc_one())
		return 1;
	if (walidx_snapshot_publish_one())
		return 1;
	if (fork_meta_snapshot_gc_due() || fork_meta_snapshot_due())
	{
		int snapshot_rc;

		/* The cutover is a single run-to-completion operation.  Admission write
		 * drains acknowledged writers, then shard locks precede page/wal-index
		 * fences in the existing shard-to-page ordering. */
		if (forkmeta_cutover_test_hook != NULL)
			forkmeta_cutover_test_hook(forkmeta_cutover_test_hook_arg);
		if (admission_write_lock() != 0)
			return 0;
		for (uint32_t sh = 0; sh < core_shards(); sh++)
			ps_lock_shard_wr(sh);
		pthread_rwlock_wrlock(&page_prune_lock);
		pthread_rwlock_wrlock(&walidx_prune_lock);
		ps_lock_map_wr();
		snapshot_rc = fork_meta_snapshot_maintenance();
		ps_unlock_map();
		pthread_rwlock_unlock(&walidx_prune_lock);
		pthread_rwlock_unlock(&page_prune_lock);
		for (uint32_t sh = core_shards(); sh > 0; sh--)
			ps_unlock_shard(sh - 1);
		ps_admission_write_unlock();
		if (snapshot_rc)
			return 1;
	}
	if (!use_layers)
	{
		if (timeline_delete_wal_cleanup_one())
			return 1;
		if (timeline_delete_page_cleanup_one())
			return 1;
		return 0;
	}

	/*
	 * Back off all maintenance once the manifest is poisoned: compaction cannot
	 * record its replacement layer, so compact_timeline() returns immediately and
	 * reporting "did work" would spin the idle worker on the same timeline until
	 * restart.  Returning 0 lets it sleep until the manifest is recovered.
	 */
	if (ps_manifest_poisoned())
		return 0;
	/* Establish the durable layer tombstone before any owner-scoped WAL
	 * cleanup.  Keeping this first preserves the existing restart discovery
	 * boundary while the two cleanup classes remain independent. */
	if (timeline_delete_mark_one())
		return 1;
	if (timeline_delete_wal_cleanup_one())
		return 1;
	if (timeline_delete_page_cleanup_one())
		return 1;
	ns = core_shards();
	/* Timeline deletion is an explicit owner-scoped cleanup path.  Establish each
	 * layer tombstone before handing it to the existing asynchronous remote GC;
	 * normal tiering, segment GC, and compaction remain LIVE-only selectors. */
	/* WAL reclaim is finite, one-timeline work.  Schedule it before potentially
	 * continuous tier/remote-GC streams so those classes cannot starve it. */
	if (wal_segment_reclaim_one())
		return 1;
	if (gc_remote_one())
		return 1;
	if (tier_one_layer())
		return 1;

	/* Reclaim at most one complete segment.  The boundary segment containing
	 * the watermark stays present because its suffix may not be in a layer.
	 * Shared segments can contain records from several timelines, so retain all
	 * of them while any deletion awaits its filtered-rewrite phase. */
	if (segment_gc_enabled && !timeline_delete_active() && ps_storage->seg_remove)
	{
		for (uint32_t sh = 0; sh < ns; sh++)
		{
			uint32_t	victim = 0;
			int			due;

			/* flush_memtable() publishes the coverage watermark while its
			 * foreground worker holds shard-wr.  Snapshot the candidate under
			 * shard-rd, then release it before layer materialization can perform
			 * remote I/O.  A newer watermark only makes this victim safer. */
			ps_lock_shard_rd(sh);
			due = g_shards[sh].flush_watermark_valid &&
				g_shards[sh].gc_next_seg <
				g_shards[sh].flush_watermark.seg_id;
			if (due)
				victim = g_shards[sh].gc_next_seg;
			ps_unlock_shard(sh);
			if (due)
				prepare_segment_layers(sh, victim);
		}
		for (uint32_t sh = 0; sh < ns; sh++)
			ps_lock_shard_wr(sh);
		ps_lock_map_rd();
		for (uint32_t sh = 0; sh < ns && !found; sh++)
			found = reclaim_one_segment(&g_shards[sh]);
		ps_unlock_map();
		for (uint32_t sh = ns; sh > 0; sh--)
			ps_unlock_shard(sh - 1);
		if (found)
			return 1;
	}
	/*
	 * Phase 1: scan under map read-lock to pick a timeline+shard whose image
	 * layers are due for compaction.  A shared lock here lets reads proceed.
	 */
	ps_lock_map_rd();
	for (uint32_t tl = 0; tl < MAX_TIMELINES && !found; tl++)
		for (uint32_t sh = 0; sh < ns; sh++)
			if (ps_timeline_live(tl) &&
				(count_image_layers(tl, sh) > (uint32_t) compact_layers ||
				 (__atomic_load_n(&page_prune_due[tl][sh], __ATOMIC_ACQUIRE) != 0 &&
				  count_image_layers(tl, sh) > 0)))
			{
				ftl = tl;
				fsh = sh;
				found = 1;
				for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
					if (ps_layer_map.layers[i].kind == PS_LAYER_IMAGE &&
						!ps_layer_map.layers[i].deleting &&
						ps_layer_map.layers[i].timeline == tl &&
						layer_shard_from_id(ps_layer_map.layers[i].layer_id) == sh &&
						ps_layer_map.layers[i].legacy_shard_zero)
						legacy_compaction = 1;
				break;
			}
	ps_unlock_map();

	/*
	 * Phase 2: compact the chosen shard under shard-wr + map-wr (the order other
	 * paths use), which excludes that shard's worker and other map mutators.
	 * Re-check under the write lock since the count may have changed.
	 */
	if (found)
	{
		if (materialize_compaction_inputs(ftl, fsh) == 0)
		{
			if (legacy_compaction)
				for (uint32_t sh = 0; sh < ns; sh++)
					ps_lock_shard_wr(sh);
			else
				ps_lock_shard_wr(fsh);
			pthread_rwlock_rdlock(&page_prune_lock);
			ps_lock_map_wr();
			if (ps_timeline_live(ftl) &&
				(count_image_layers(ftl, fsh) > (uint32_t) compact_layers ||
				 (__atomic_load_n(&page_prune_due[ftl][fsh], __ATOMIC_ACQUIRE) != 0 &&
				  count_image_layers(ftl, fsh) > 0)) &&
				retention_effective_floor_internal(ftl,
					PS_RETENTION_RESOURCE_PAGE_HISTORY, &page_floor, 1) == 0)
			{
				/* Zero disables pruning but still permits a safe layer merge. */
				did = compact_timeline(ftl, fsh, page_floor) > 0;
				if (did)
					__atomic_store_n(&page_prune_due[ftl][fsh], 0,
									 __ATOMIC_RELEASE);
			}
			ps_unlock_map();
			pthread_rwlock_unlock(&page_prune_lock);
			if (legacy_compaction)
				for (uint32_t sh = ns; sh > 0; sh--)
					ps_unlock_shard(sh - 1);
			else
				ps_unlock_shard(fsh);
		}
	}
	/* Keep compaction inputs resident until due compaction has run.  Evicting
	 * first turns routine compaction into remote I/O under map/shard write
	 * locks; segment GC above likewise consumes its caches before this point. */
	if (!found && !did && evict_one_layer())
		return 1;

	/*
	 * Phase 3: rewrite the manifest log if add/seal/delete churn has grown it
	 * well past the live layer count, bounding replay time.  Independent of layer
	 * compaction; map-wr excludes the manifest appends a concurrent flush makes.
	 */
	ps_lock_map_rd();
	found = ps_manifest_should_compact();
	ps_unlock_map();
	if (found)
	{
		int			compacted = 0;

		ps_lock_map_wr();
		if (ps_manifest_should_compact())
			compacted = (ps_manifest_compact() == 0);
		ps_unlock_map();

		/*
		 * Only count a *successful* rewrite as work done.  A failed compaction
		 * leaves should_compact() true, so reporting "did work" would make the
		 * idle worker re-run maintenance immediately and busy-loop on the failing
		 * compaction; returning false here lets it sleep and retry on the next
		 * tick instead.  (An I/O failure also poisons the manifest, after which
		 * should_compact() returns false and the retries stop entirely.)
		 */
		if (compacted)
			did = 1;
	}

	return did;
}

int
ps_core_maintenance(void)
{
	int did;

	/* Keep lifecycle-rd across the complete synchronous call.  Any asynchronous
	 * worker started within it reserves an additional reader before create and
	 * releases that reservation from its thread cleanup handler. */
	ps_lifecycle_read_lock();
	did = ps_core_maintenance_impl();
	ps_lifecycle_read_unlock();
	/* A terminal lifecycle transition cannot upgrade the read section held by
	 * the maintenance body.  Retry readiness under the exclusive lifecycle
	 * writer fence after all ordinary/background work has drained. */
	if (timeline_delete_publish_one())
		did = 1;
	/* Disabled-by-default controllers must not perturb the maintenance hot
	 * path (or its scheduling) merely to republish an unchanged zero snapshot. */
	if (page_reclaim_high_water_bytes != 0 ||
		wal_reclaim_high_water_bytes != 0 ||
		walidx_reclaim_high_water_bytes != 0 ||
		forkmeta_reclaim_high_water_bytes != 0)
		ps_backpressure_refresh_automatic();
	return did;
}

/*
 * Open the store and rebuild all in-memory state from it: define the root
 * timeline, load persisted branches, rebuild the page/fork indexes from the
 * image layers (falling back to a segment scan only for a store that has no
 * layers yet -- e.g. a pre-LSM store being migrated), and recompute each
 * timeline's shipped-WAL end LSN.  The frontend must set page_size,
 * segment_size and ps_storage beforehand.
 */
int
ps_core_open(const char *store_dir)
{
	uint32_t	ns = core_shards();
	int			publish_shard_count = 0;

	__atomic_store_n(&walidx_observation_next_ns, 0, __ATOMIC_RELEASE);
	/* A test or embedding process may reopen without a fresh daemon.  Drop
	 * every hash entry before recovery repopulates the indexes. */
	free_page_fork_indexes();
	free_walidx_indexes();
	__atomic_store_n(&next_segment_order_id, 1, __ATOMIC_RELAXED);
	__atomic_store_n(&next_admission_seq, 1, __ATOMIC_RELAXED);
	/* A close/open cycle may switch to a store with different timelines.  Drop
	 * every in-memory flat-WAL catalog before metadata replay selects which
	 * timelines to recover; resetting only wal_end would leave stale offsets and
	 * chunk references available for a newly reused timeline id. */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		if (wal_segment_store_opened[tl])
		{
			ps_wal_store_close(&wal_segment_stores[tl]);
			wal_segment_store_opened[tl] = 0;
		}
		free(wal_chunks[tl]);
		wal_chunks[tl] = NULL;
		wal_chunks_n[tl] = 0;
		wal_chunks_cap[tl] = 0;
	}
	memset(wal_log_bytes, 0, sizeof(wal_log_bytes));
	memset(wal_start, 0, sizeof(wal_start));
	memset(wal_start_valid, 0, sizeof(wal_start_valid));
	memset(wal_end, 0, sizeof(wal_end));
	memset(wal_covered, 0, sizeof(wal_covered));
	memset(wal_covered_off, 0, sizeof(wal_covered_off));
	memset(wal_covered_valid, 0, sizeof(wal_covered_valid));
	/* Metadata is rebuilt below; a close/open cycle must not retain branches. */
	memset(timelines, 0, sizeof(timelines));
	__atomic_store_n(&timeline_meta_poisoned, 0, __ATOMIC_RELEASE);
	memset(timeline_used, 0, sizeof(timeline_used));
	memset(timeline_wal_cleanup_done, 0, sizeof(timeline_wal_cleanup_done));
	memset(timeline_page_cleanup_done, 0, sizeof(timeline_page_cleanup_done));
	timeline_page_cleanup_cursor = 0;
	fork_meta_poisoned_store(0);
	fork_meta_bytes_store(0);
	fork_meta_snapshot_generation = 0;
	fork_meta_snapshot_cutoff_lsn = 0;
	fork_meta_snapshot_cutoff_seq = 0;
	fork_meta_snapshot_freeze_seq = 0;
	memset(&fork_meta_snapshot_checkpoint_meta, 0,
		   sizeof(fork_meta_snapshot_checkpoint_meta));
	memset(&fork_meta_snapshot_tail_meta, 0,
		   sizeof(fork_meta_snapshot_tail_meta));
	fork_meta_reclaim_baseline_bytes = 0;
	fork_meta_reclaim_baseline_valid = 0;
	fork_meta_irreducible_prefix_bytes = 0;
	fork_meta_snapshot_bytes = 0;
	fork_meta_snapshot_gc_pending = 0;
	fork_meta_snapshot_gc_ambiguous = 0;
	memset(fork_meta_deletion_cutover_done, 0,
		   sizeof(fork_meta_deletion_cutover_done));
	memset(&fork_meta_snapshot_retry_at, 0,
		   sizeof(fork_meta_snapshot_retry_at));
	fork_meta_migrating = 0;
	fork_meta_migrated = 0;
	fork_meta_legacy = 0;
	fork_meta_migrate_failed = 0;
	map_locks_ready = 0;
	memset(&tier_upload_retry_at, 0, sizeof(tier_upload_retry_at));
	memset(tier_upload_layer_cursor, 0, sizeof(tier_upload_layer_cursor));
	__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
	gc_remote_layer_cursor = 0;
	if (snprintf(wal_segment_root, sizeof(wal_segment_root), "%s", store_dir) < 0 ||
		strlen(store_dir) >= sizeof(wal_segment_root))
		return -1;
	if (snprintf(fork_meta_snapshot_dir, sizeof(fork_meta_snapshot_dir),
				 "%s/forkmeta_snapshots", store_dir) < 0 ||
		strlen(fork_meta_snapshot_dir) >= sizeof(fork_meta_snapshot_dir))
		return -1;
	memset(wal_segment_store_opened, 0, sizeof(wal_segment_store_opened));
	memset(walidx_progress, 0, sizeof(walidx_progress));
	memset(walidx_progress_valid, 0, sizeof(walidx_progress_valid));
	memset(walidx_progress_durable, 0, sizeof(walidx_progress_durable));
	memset(walidx_shards_seen, 0, sizeof(walidx_shards_seen));
	memset(walidx_shards_required, 0, sizeof(walidx_shards_required));
	memset(walidx_shard_offsets_seen, 0, sizeof(walidx_shard_offsets_seen));
	memset(walidx_shard_offsets_required, 0,
		   sizeof(walidx_shard_offsets_required));
	memset(walidx_snapshot_generation, 0, sizeof(walidx_snapshot_generation));
	memset(walidx_snapshot_start, 0, sizeof(walidx_snapshot_start));
	memset(walidx_snapshot_end, 0, sizeof(walidx_snapshot_end));
	memset(walidx_snapshot_offsets, 0, sizeof(walidx_snapshot_offsets));
	memset(walidx_snapshot_bytes, 0, sizeof(walidx_snapshot_bytes));
	memset(walidx_snapshot_reshard_pending, 0,
		   sizeof(walidx_snapshot_reshard_pending));
	memset(walidx_snapshot_retry_at, 0, sizeof(walidx_snapshot_retry_at));
	walidx_snapshot_cursor = 0;
	memset(walidx_log_epoch, 0, sizeof(walidx_log_epoch));
	memset(walidx_snapshot_gc_pending, 0,
		   sizeof(walidx_snapshot_gc_pending));
	walidx_snapshot_gc_cursor = 0;
	memset(walidx_snapshot_gc_retry_at, 0,
		   sizeof(walidx_snapshot_gc_retry_at));
	memset(walidx_snapshot_cleanup, 0, sizeof(walidx_snapshot_cleanup));
	memset(walidx_snapshot_cleanup_pending, 0,
		   sizeof(walidx_snapshot_cleanup_pending));
	memset(walidx_snapshot_cleanup_retry_at, 0,
		   sizeof(walidx_snapshot_cleanup_retry_at));
	memset(wal_reclaim_retry_at, 0, sizeof(wal_reclaim_retry_at));
	wal_reclaim_cursor = 0;
	__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
	evict_local_map_cursor = 0;

	if (ps_storage->open(store_dir, segment_size) != 0)
		return -1;
	ps_layer_store_set_page_size(page_size);
	if (ps_layer_store->open(store_dir) != 0)
		return -1;
	if (ps_manifest_open(store_dir) != 0)
		return -1;
	if (ps_manifest_replay(&ps_layer_map) != 0)
		return -1;
	if (validate_store_shard_count(store_dir, &publish_shard_count) != 0)
		return -1;
	/* Leave deleting layers for asynchronous maintenance: recovery must not
	 * block on an unavailable remote object that is already excluded from reads. */

	/* initialize per-shard state, locks and layer-id cursors */
	for (uint32_t i = 0; i < ns; i++)
	{
		PsFlushWatermark watermark;

		g_shards[i].id = i;
		g_shards[i].cur_seg = -1;
		g_shards[i].cur_off = 0;
		g_shards[i].gc_next_seg = 0;
		g_shards[i].gc_debt_segments = 0;
		g_shards[i].gc_pending_remove_seg = 0;
		g_shards[i].gc_pending_remove = 0;
		g_shards[i].gc_storage_error = 0;
		g_shards[i].coverage_broken = 0;
		g_shards[i].flush_watermark_valid = 0;
		if (use_layers && ps_manifest_get_flush_watermark(i, &watermark))
		{
			g_shards[i].flush_watermark = watermark;
			g_shards[i].flush_watermark_valid = 1;
			/* Rebuild the oldest present covered segment and the incremental
			 * positive-size debt count.  Non-ENOENT storage errors fail startup
			 * closed, preserving errno and never advancing the cursor.  Keep
			 * the disabled controller off this path: it must not add a startup
			 * storage scan to the default or SPDK behavior. */
			if (page_reclaim_high_water_bytes != 0 &&
				rebuild_page_gc_state(&g_shards[i]) != 0)
				return -1;
		}
		g_shards[i].next_layer_id = 1;
		pthread_rwlock_init(&shard_locks[i], NULL);
	}
	map_locks_ready = 1;
	/* layer ids continue past the highest one restored per shard */
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		uint32_t	sh = layer_shard_from_id(ps_layer_map.layers[i].layer_id);
		uint64_t	lid = layer_local_id(ps_layer_map.layers[i].layer_id);

		if (sh < ns && lid + 1 > g_shards[sh].next_layer_id)
			g_shards[sh].next_layer_id = lid + 1;
	}
	if (use_layers && mark_legacy_shard_zero_layers() != 0)
		return -1;

	/* the LSM write side (memtable/flush/compaction) runs only when layers are
	 * the read path; the SPDK daemon stays on the segment path for now.  One
	 * memtable per shard. */
	if (use_layers)
		for (uint32_t i = 0; i < ns; i++)
		{
			g_shards[i].memtable = ps_memtable_create(page_size,
													  (uint32_t) flush_pages);
			if (!g_shards[i].memtable)
				return -1;
		}
	/* the materialized-page cache helps both read paths (read_resolve and the
	 * SPDK async path), so it is not gated on use_layers */
	ps_pgcache_init((uint32_t) cache_pages, page_size);
	fprintf(stderr, "pagestore_core: %u image layer(s) in map after manifest replay\n",
			ps_layer_map.nlayers);

	/* timeline 0 is the root; load any persisted branches, then rebuild data */
	timeline_define(0, -1, 0);
	if (load_timelines() != 0)
	{
		fprintf(stderr, "pagestore_core: refusing to open corrupt timelines metadata\n");
		return -1;
	}
	/* Load durable branch definitions before immutable-only ids are marked used:
	 * metadata replay must be allowed to reconstruct a legitimate branch, while
	 * later CREATE_BRANCH requests must not reuse any discovered id. */
	if (wal_segment_discover_used() != 0)
		return -1;
	if (ps_retention_open(store_dir) != 0)
		return -1;
	if (page_frontier_load(store_dir) != 0)
	{
		fprintf(stderr, "pagestore: refusing to open corrupt page reclamation frontiers\n");
		return -1;
	}
	if (walidx_frontier_load(store_dir) != 0)
	{
		fprintf(stderr, "pagestore: refusing to open corrupt WAL-index "
				"reclamation frontiers\n");
		return -1;
	}
	{
		uint64_t	admission_highwater;
		uint32_t	npins = 0;

		if (ps_retention_admission_highwater(&admission_highwater) != 0)
			return -1;
		admission_seq_observe(admission_highwater);
		if (ps_retention_count(&npins) != 0)
			return -1;
		for (uint32_t i = 0; i < npins; i++)
		{
			PsRetentionPin pin;

			if (ps_retention_get(i, &pin, NULL) != 1 ||
				pin.timeline >= MAX_TIMELINES ||
				!timelines[pin.timeline].defined)
			{
				fprintf(stderr, "pagestore: retention pin references an undefined timeline\n");
				errno = EILSEQ;
				return -1;
			}
			if (pin.admission_seq != 0)
				admission_seq_observe(pin.admission_seq);
		}
	}
	/* Reconcile the snapshot intent before loading either source epoch.  Only a
	 * selected manifest transfers ownership away from the old source epoch. */
	{
		int manifest_exists = fork_meta_snapshot_manifest_exists(
			fork_meta_snapshot_dir);
		if (manifest_exists < 0)
			return -1;
		if (manifest_exists)
		{
			PsForkmetaSnapshot selected = {.directory_fd = -1,
				.checkpoint_fd = -1, .tail_fd = -1};
			PsForkmetaSnapshotPrepared pending;
			int have_pending;

			if (ps_forkmeta_snapshot_open(&selected, fork_meta_snapshot_dir) != 0)
				return -1;
			have_pending = ps_forkmeta_snapshot_read_prepared(
				fork_meta_snapshot_dir, &pending);
			if (have_pending < 0 ||
				(have_pending == 1 &&
				 (fork_meta_prepared_matches_selected(&pending, &selected) ?
				  ps_forkmeta_snapshot_commit(&pending) :
				  ps_forkmeta_snapshot_abort(&pending)) != 0))
			{
				fprintf(stderr, "pagestore: forkmeta prepared-intent reconcile failed\n");
				ps_forkmeta_snapshot_close(&selected);
				return -1;
			}
			ps_forkmeta_snapshot_close(&selected);
			if (fork_meta_snapshot_load(fork_meta_snapshot_dir) != 0)
			{
				fprintf(stderr, "pagestore: selected forkmeta snapshot is invalid\n");
				return -1;
			}
			if (fork_meta_snapshot_reconcile_source() != 0)
			{
				fprintf(stderr, "pagestore: forkmeta source epoch reconcile failed\n");
				return -1;
			}
			/* Startup has selected the authoritative generation.  Reclaim older
			 * generations asynchronously; recovery must not leave them behind just
			 * because the in-memory pending bit was reset for this process. */
			fork_meta_snapshot_gc_pending = 1;
			memset(&fork_meta_snapshot_retry_at, 0,
				   sizeof(fork_meta_snapshot_retry_at));
		}
		else
		{
			PsForkmetaSnapshotPrepared prepared;
			int have_prepared = ps_forkmeta_snapshot_read_prepared(
				fork_meta_snapshot_dir, &prepared);

			if (have_prepared < 0)
				return -1;
			if (have_prepared == 1 &&
				ps_forkmeta_snapshot_abort(&prepared) != 0)
				return -1;
		}
	}

	/*
	 * Definitive fork-size events (create/truncate/unlink/zero-extend) load
	 * before page recovery: the GROW dedup in fork_event_add compares a
	 * grow against the size visible at its own LSN, and with the definitive
	 * events already in place layer/segment replay makes exactly the decisions
	 * the live path made (a regrow after a truncate must be kept even when
	 * it does not exceed the pre-truncate envelope).
	 *
	 * A durable per-shard watermark divides recovery: v3 image-layer metadata
	 * reconstructs the covered prefix in source-segment order, then recover()
	 * scans and materializes only the segment suffix.  Without a watermark (an
	 * old store or SPDK), recover() starts at segment zero.
	 */
	if (load_fork_meta() != 0)
		return -1;

	for (uint32_t sh = 0; sh < ns; sh++)
	{
		if (use_layers && recover_layer_prefix(sh) != 0)
			return -1;
		if (recover(sh) != 0)
			return -1;
	}
	/* Retention mutations may have committed immediately before shutdown.
	 * Conservatively revisit every nonempty layer set after recovery. */
	page_prune_mark_all_due();
	if (use_layers && mark_legacy_shard_zero_layers() != 0)
		return -1;

	/*
	 * Seal the legacy migration before the daemon becomes writable.  Starting
	 * with a missing marker, or accepting writes after a partial/unsealed scan,
	 * could create a markerless log or replay old LSN-0 pages above a newly
	 * persisted truncate/unlink on the next boot.  Fail startup instead; replay
	 * is idempotent and the next process retries the migration.
	 */
	if (fork_meta_legacy)
	{
		PsKey		zk;

		if (fork_meta_migrate_failed)
		{
			fprintf(stderr, "pagestore: fork-meta migration incomplete\n");
			return -1;
		}
		memset(&zk, 0, sizeof(zk));
		if (fork_meta_persist(0, &zk, 0, 0, 0, FEV_MIGRATED) != 0)
		{
			fprintf(stderr, "pagestore: could not seal the fork-meta migration\n");
			return -1;
		}
		fork_meta_irreducible_prefix_bytes += sizeof(ForkMetaRecV2);
	}

	/* rebuild each timeline's shipped-WAL end LSN from its log */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined || timeline_is_used(tl))
		{
			PsTimelineState timeline_state;

			/* A crash can leave any prefix of a DELETING timeline's private
			 * WAL set behind.  Do not parse or reopen those artifacts: the
			 * maintenance cleanup owns the retry and the tombstone remains
			 * DELETING throughout. */
			if (ps_timeline_state(tl, &timeline_state, NULL) &&
				(timeline_state == PS_TIMELINE_DELETING ||
				 timeline_state == PS_TIMELINE_DELETED))
				continue;
			if (wal_recover_one(tl) != 0)
				return -1;
			if (wal_segment_sync(tl) != 0)
			{
				fprintf(stderr, "pagestore: refusing invalid immutable WAL segments "
						"for timeline %u\n", tl);
				return -1;
			}
			walidx_progress_init(tl, wal_log_start(tl));
			{
				char directory[4096];

				if (walidx_snapshot_path(tl, directory, sizeof(directory)) != 0 ||
					ps_walidx_snapshot_recover_prepared(directory, tl,
										walidx_frontier_current(tl)) != 0)
					return -1;
			}
			if (walidx_snapshot_recover(tl) != 0)
				return -1;
			for (uint32_t shard = 0; shard < core_shards(); shard++)
				if (walidx_recover_one(tl, shard) != 0)
					return -1;
		}

	if (publish_shard_count && publish_store_shard_count(store_dir) != 0)
		return -1;
	if (forkmeta_reclaim_high_water_bytes != 0 &&
		fork_meta_reclaim_baseline_init() != 0)
		return -1;

	return 0;
}
