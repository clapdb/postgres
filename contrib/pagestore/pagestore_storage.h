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

	/* segment log (page-version data); seg_write creates the segment lazily */
	int			(*seg_write) (uint32_t shard, int seg, uint64_t off,
						 const void *buf, uint32_t len);
	int			(*seg_read) (uint32_t shard, int seg, uint64_t off, void *buf,
						uint32_t len);
	int64_t		(*seg_size) (uint32_t shard, int seg);

	/*
	 * Per-timeline shipped-WAL log.  wal_append takes the record header (a) and
	 * its payload (b) so the backend can land them contiguously in one append;
	 * b may be NULL with blen 0.
	 */
	int			(*wal_append) (uint32_t tl, const void *a, uint32_t alen,
							   const void *b, uint32_t blen);
	int			(*wal_read) (uint32_t tl, uint64_t off, void *buf, uint32_t len);

	/* timeline metadata log */
	int			(*meta_append) (const void *buf, uint32_t len);
	int			(*meta_read) (uint64_t off, void *buf, uint32_t len);
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
