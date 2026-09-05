/*-------------------------------------------------------------------------
 *
 * pagestore_storage.h
 *	  Byte-log storage abstraction for the page-store daemon.
 *
 * The daemon's indexes, append cursor, timeline metadata and request handling
 * are all storage-agnostic in-memory logic; only raw byte movement and
 * enumeration go through this interface.  Making the storage layer pluggable
 * lets a portable POSIX backend (the default -- libc only, runs anywhere) and
 * an optional, higher-performance SPDK backend coexist behind one interface,
 * WITHOUT changing the IPC ABI: a machine that cannot run SPDK simply uses the
 * POSIX backend, transparently to the compute side.
 *
 * The store is three append-only logs:
 *	 - segments: page-version records, addressed by (segment id, byte offset);
 *	 - per-timeline shipped WAL, one log per timeline;
 *	 - timeline metadata, a single log.
 * A backend maps these onto its medium (files for POSIX; device regions or
 * blobs for SPDK).
 *
 * Conventions:
 *	 - read ops return the number of bytes read (>= 0, possibly short at EOF)
 *	   or -1 on error;
 *	 - write/append/sync/open ops return 0 on success or -1 on error;
 *	 - seg_size returns a segment's byte length, or -1 if it does not exist.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_STORAGE_H
#define PAGESTORE_STORAGE_H

#include <stddef.h>
#include <stdint.h>

typedef struct PsStorage
{
	const char *name;

	/* lifecycle */
	int			(*open) (const char *path, uint64_t segment_size);
	void		(*close) (void);
	int			(*sync) (void);

	/*
	 * If nonzero, sync() flushes in-memory write buffers shared with seg_write
	 * (e.g. SPDK's per-shard curbuf) and is NOT internally serialized against
	 * concurrent writes, so the daemon must hold every shard's write lock around
	 * IMMEDSYNC.  POSIX leaves this 0: its sync() only fsyncs fds under its own
	 * lock, so the lock-free IMMEDSYNC path is safe.
	 */
	int			sync_needs_write_lock;

	/* segment log (page-version data); seg_write creates the segment lazily */
	int			(*seg_write) (uint32_t shard, int seg, uint64_t off,
						 const void *buf, uint32_t len);
	int			(*seg_read) (uint32_t shard, int seg, uint64_t off, void *buf,
						uint32_t len);
	int64_t		(*seg_size) (uint32_t shard, int seg);
	/* Optional: remove a no-longer-referenced segment (ENOENT is success). */
	int			(*seg_remove) (uint32_t shard, int seg);
	/* Crash-safe same-id replacement: fsync temp, rename, fsync parent dir. */
	int			(*seg_rewrite) (uint32_t shard, int seg, const void *buf,
							 uint64_t len);

	/*
	 * Per-timeline shipped-WAL log.  wal_append takes the record header (a) and
	 * its payload (b) so the backend can land them contiguously in one append;
	 * b may be NULL with blen 0.
	 */
	int			(*wal_append) (uint32_t tl, const void *a, uint32_t alen,
							   const void *b, uint32_t blen);
	int			(*wal_read) (uint32_t tl, uint64_t off, void *buf, uint32_t len);
	int			(*wal_truncate) (uint32_t tl, uint64_t len);
	/*
	 * Crash-atomically replace a flat WAL log with the byte suffix beginning at
	 * keep_off.  The caller must freeze logical WAL catalog mutation across the
	 * call and rebuild its file-offset references before admitting an append.
	 * An error after publication is ambiguous: the backend must reject further
	 * WAL access for that timeline, and the caller must reopen storage before
	 * retrying with rebuilt physical offsets.
	 */
	int			(*wal_rewrite_prefix) (uint32_t tl, uint64_t keep_off);

	/* Per-(timeline, shard) durable per-page WAL index records. */
	int			(*walidx_append) (uint32_t tl, uint32_t shard, uint64_t epoch,
							 const void *buf, uint32_t len);
	int			(*walidx_read) (uint32_t tl, uint32_t shard, uint64_t epoch,
							 uint64_t off, void *buf, uint32_t len);
	int			(*walidx_truncate) (uint32_t tl, uint32_t shard, uint64_t epoch,
							   uint64_t len);
	/* Prepare a durable empty epoch before a snapshot manifest selects it, then
	 * retire only epochs older than the selected one.  GC returns 1 if it
	 * removed files, 0 if already clean, and -1 on error. */
	int			(*walidx_epoch_create) (uint32_t tl, uint32_t shard,
								 uint64_t epoch);
	int			(*walidx_epoch_gc) (uint32_t tl, const uint64_t *keep_epochs,
								 uint32_t nshards);
	/* Return append-tail (physical bytes after covered_offsets) and obsolete
	 * physical WAL-index bytes for one previously captured identity;
	 * observed_offsets are the current logical end used for consistency checks.
	 * Implementations must scan
	 * without depending on core runtime locks; the caller revalidates identity
	 * after the scan. */
	int			(*walidx_reclaim_bytes) (uint32_t tl,
								 const uint64_t *keep_epochs,
								 const uint64_t *covered_offsets,
								 const uint64_t *observed_offsets,
								 uint32_t nshards,
								 uint64_t *tail_bytes,
								 uint64_t *obsolete_bytes);
	/* Remove only the deleting timeline's private WAL/WAL-index artifacts.
	 * The backend must validate the complete target set before unlinking any
	 * entry, fsync every changed directory, and return an error for unknown or
	 * non-regular entries.  A backend without a safe implementation leaves this
	 * NULL (fail closed). */
	int			(*timeline_wal_cleanup) (uint32_t tl);

	/* timeline metadata log */
	int			(*meta_append) (const void *buf, uint32_t len);
	int			(*meta_read) (uint64_t off, void *buf, uint32_t len);
	int			(*meta_truncate) (uint64_t len);
	int			(*meta_rewrite) (const void *buf, uint32_t len);

	/*
	 * Fork metadata log: the durable record of fork-size events the segment
	 * log cannot reproduce (create/truncate/unlink and zero-extends, which
	 * write no page records), plus inert segment-growth ordering markers correlated
	 * during recovery.  Same append-only fixed-record discipline as the
	 * timeline log.
	 */
	int			(*fork_meta_append) (const void *buf, uint32_t len);
	int			(*fork_meta_read) (uint64_t off, void *buf, uint32_t len);
	int			(*fork_meta_truncate) (uint64_t len);
	/*
	 * Atomically replace the fixed fork-meta log via durable temporary file,
	 * rename, and parent-directory fsync.  The core externally serializes this
	 * operation against append/read/truncate.  A failure after rename is
	 * intentionally ambiguous: callers must poison the live process and reopen
	 * to reconcile the selected snapshot with whichever name survived.
	 */
	int			(*fork_meta_rewrite) (const void *buf, uint32_t len);
} PsStorage;

/* the active backend, selected at startup */
extern const PsStorage *ps_storage;

/* always available: portable, libc-only */
extern const PsStorage PsStoragePosix;

#ifdef PAGESTORE_SPDK
/* optional: userspace NVMe via SPDK, compiled in only when enabled */
extern const PsStorage PsStorageSpdk;
#endif

#endif							/* PAGESTORE_STORAGE_H */
