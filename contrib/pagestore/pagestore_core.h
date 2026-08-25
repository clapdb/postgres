/*-------------------------------------------------------------------------
 *
 * pagestore_core.h
 *	  Shared "brain" of the page-store daemon.
 *
 * The in-memory indexes, copy-on-write read-through, timelines, per-page WAL
 * index, shipped-WAL metadata and recovery live here and are compiled into both
 * the POSIX daemon and the (optional) SPDK daemon, so the store's core logic is
 * single-sourced.  Each frontend supplies only its request loop and the page
 * byte I/O -- synchronous for POSIX, callback-driven for SPDK -- which goes
 * through the PsStorage interface.
 *
 * Seam: ps_handle_meta() handles every request that is not page byte I/O; the
 * four byte-I/O ops are done by each frontend using read_through()/read_version()
 * (reads) and append_page()/fork_grow() (writes).
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_CORE_H
#define PAGESTORE_CORE_H

#include <pthread.h>
#include <stdint.h>

#include "pagestore_ipc.h"
#include "pagestore_storage.h"

/* One stored version of a page: its LSN and where the bytes live in the store. */
typedef struct PageVer
{
	uint32_t	shard;			/* segment shard that stores this version */
	uint64_t	lsn;			/* the page's pd_lsn when it was written */
	uint64_t	admission_seq;	/* global append order; 0 = legacy pre-fence */
	int			seg;			/* segment id holding the bytes */
	uint64_t	off;			/* byte offset of the page within that segment */
} PageVer;

/* Configuration shared with the frontend; set by the frontend before open. */
extern uint32_t page_size;
extern uint64_t segment_size;
extern int	flush_pages;		/* memtable flush threshold in pages */
extern int	compact_layers;		/* compact a timeline past this many image layers */
extern int	segment_gc_enabled;	/* reclaim layer-covered POSIX segments */
extern int	cache_pages;		/* materialized-page cache size (pages; 0=off) */
extern int	use_layers;			/* rebuild read state from layers (vs segments) */
extern const PsStorage *ps_storage;
extern uint32_t	ps_nshards;		/* logical shards configured for this daemon */

/* Open the store and rebuild all in-memory state (timelines, indexes, WAL). */
extern int	ps_core_open(const char *store_dir);

/* Clean-shutdown: flush the memtable into a layer and close the manifest. */
extern void ps_core_close(void);

/* Off-the-write-path tiering, GC, and compaction.  The maintenance controller
 * calls this repeatedly; returns 1 if it did work, 0 if nothing was due. */
extern int	ps_core_maintenance(void);

/* Number of image layers currently in the layer map (for stats/diagnostics). */
extern uint32_t ps_core_layer_count(void);
extern void ps_core_set_metrics_header(PsShmHeader *hdr);

/* Runtime lifecycle gate.  POSIX frontend requests and complete maintenance
 * work take the read side; destructive lifecycle transitions take the write
 * side.  The lock order is lifecycle -> admission -> shard/page/walidx -> map. */
extern void ps_lifecycle_read_lock(void);
extern void ps_lifecycle_read_unlock(void);
extern int ps_lifecycle_write_lock(void);
extern void ps_lifecycle_write_unlock(void);

/* Assign a fence sequence only after all prior mutation bodies have left. */
extern void ps_admission_read_lock(void);
extern void ps_admission_read_unlock(void);
extern int ps_admission_write_lock(void);
extern void ps_admission_write_unlock(void);
extern uint64_t ps_admission_barrier(void);

/* Test-only observability for deterministic admission/cutover overlap.  The
 * admission callback runs after a test operation acquires admission-rd.
 * Production frontends never install these hooks. */
typedef void (*PsForkmetaCutoverTestHook)(void *arg);
typedef void (*PsAdmissionReadTestHook)(void *arg);
typedef void (*PsLifecycleReadTestHook)(void *arg);
typedef void (*PsTierUploadBeforePublishTestHook)(void *arg);
/* Called after the lifecycle turnstile mutex is acquired, before the reader
 * tests whether a writer is queued.  The callback must not take lifecycle
 * locks. */
typedef void (*PsLifecycleReadQueuedTestHook)(void *arg);
/* Called after lifecycle_waiting_writers is incremented, before the writer
 * waits for active readers.  The callback must not take lifecycle locks. */
typedef void (*PsLifecycleWriteQueuedTestHook)(void *arg);
/* Test-only replacement for the exact blocking admission-wr call.  The
 * production path invokes pthread_rwlock_wrlock directly; when installed,
 * the hook is called in its place and must call pthread_rwlock_wrlock(lock)
 * itself.  This lets a test observe entry to the real blocking call without
 * adding a separate try-lock probe to production maintenance. */
typedef int (*PsAdmissionWriteLockTestHook)(pthread_rwlock_t *lock, void *arg);
typedef int (*PsLifecycleWriteLockTestHook)(pthread_rwlock_t *lock, void *arg);
extern void ps_test_set_forkmeta_cutover_hook(
	PsForkmetaCutoverTestHook hook, void *arg);
extern void ps_test_set_admission_read_hook(PsAdmissionReadTestHook hook,
	void *arg);
extern void ps_test_set_lifecycle_read_hook(PsLifecycleReadTestHook hook,
	void *arg);
extern void ps_test_set_tier_upload_before_publish_hook(
	PsTierUploadBeforePublishTestHook hook, void *arg);
extern void ps_test_set_lifecycle_read_queued_hook(
	PsLifecycleReadQueuedTestHook hook, void *arg);
extern void ps_test_set_lifecycle_write_queued_hook(
	PsLifecycleWriteQueuedTestHook hook, void *arg);
extern void ps_test_set_admission_write_lock_hook(
	PsAdmissionWriteLockTestHook hook, void *arg);
extern void ps_test_set_lifecycle_write_lock_hook(
	PsLifecycleWriteLockTestHook hook, void *arg);
/* Test-only scheduling seam for a pending forkmeta snapshot GC retry. */
extern void ps_test_forkmeta_snapshot_gc_retry_now(void);

/* Read-path source counts: served from memtable / image layer / segment. */
extern void ps_core_read_stats(uint64_t *mem, uint64_t *layer, uint64_t *seg);

/*
 * Handle every request that is NOT page byte I/O and return 1.  The four
 * byte-I/O ops (EXTEND/WRITEV/READV/READ_AT) and unknown ops return 0 for the
 * frontend to handle.  Sets ch->status/ch->result as appropriate.
 */
extern int	ps_handle_meta(PsChannel *ch);

/*
 * Page byte-I/O helpers used by the frontends' byte-op handlers.  'version' is the
 * caller-supplied version LSN for an SLRU-class write (the dirtying/cutoff WAL LSN,
 * stored verbatim so it stays comparable to a branch cutoff); it is ignored for
 * relation pages (versioned by pd_lsn) and other non-relation objects (versioned by
 * a monotonic latest-wins counter).
 */
extern int	append_page(uint32_t timeline, const PsKey *key, uint32_t block,
						const unsigned char *page, uint64_t version,
						uint64_t *out_admission_seq);
extern PageVer *read_through(uint32_t timeline, const PsKey *key, uint32_t block,
							 uint64_t read_lsn, uint64_t read_seq);
extern int	read_version(const PageVer *v, unsigned char *out);
extern int	wal_retain_floor(uint32_t timeline, uint64_t *floor_out);

/*
 * Resolve a read into out (page_size bytes), serving from memtable / image
 * layers with a segment fallback.  Returns 1 if found (out filled), 0 if the
 * page is unwritten, -1 if an authoritative stored version cannot be read, and
 * -2 when the requested capped horizon has been reclaimed.
 */
extern int	read_resolve(uint32_t timeline, const PsKey *key, uint32_t block,
						 uint64_t read_lsn, uint64_t read_seq,
						 unsigned char *out, uint64_t *out_ver);
extern int	fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
					  uint64_t lsn);

/*
 * Concurrency locks (defined in pagestore_core.c).  A per-shard rwlock guards
 * each shard's in-memory state; a single map_lock guards the cross-shard
 * ps_layer_map + timelines[].  Callers MUST take them in the order shard
 * (outer) -> map (inner), never the reverse.
 */
extern void ps_lock_shard_rd(uint32_t shard);
extern void ps_lock_shard_wr(uint32_t shard);
extern void ps_unlock_shard(uint32_t shard);
extern void ps_lock_map_rd(void);
extern void ps_lock_map_wr(void);
extern void ps_unlock_map(void);
/* Caller holds map_lock.  Definitions are append-only during an open. */
extern int ps_timeline_defined(uint32_t timeline);
extern int ps_timeline_state(uint32_t timeline, PsTimelineState *state,
							 uint64_t *incarnation);
extern int ps_timeline_live(uint32_t timeline);

/* Shard index that will be touched for 'key' (klass-aware); the frontend takes
 * the per-shard lock from the final request key, not a client-supplied shard. */
extern uint32_t ps_shard_of(const PsKey *key);

#endif							/* PAGESTORE_CORE_H */
