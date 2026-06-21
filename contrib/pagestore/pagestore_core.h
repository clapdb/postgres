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
 * through the PsStorage interface.  The IPC ABI is unaffected by any of this.
 *
 * Seam: ps_handle_meta() handles every request that is not page byte I/O; the
 * four byte-I/O ops are done by each frontend using read_through()/read_version()
 * (reads) and append_page()/fork_grow() (writes).
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_CORE_H
#define PAGESTORE_CORE_H

#include <stdint.h>

#include "pagestore_ipc.h"
#include "pagestore_storage.h"
#include "pagestore_pgcache.h"

/* One stored version of a page: its LSN and where the bytes live in the store. */
typedef struct PageVer
{
	uint64_t	lsn;			/* the page's pd_lsn when it was written */
	int			seg;			/* segment id holding the bytes */
	uint64_t	off;			/* byte offset of the page within that segment */
	uint32_t	crc;			/* CRC32C of the page bytes (verified on read) */
} PageVer;

/* Configuration shared with the frontend; set by the frontend before open. */
extern uint32_t page_size;
extern uint64_t segment_size;
extern int	flush_pages;		/* memtable flush threshold in pages */
extern int	compact_layers;		/* compact a timeline past this many image layers */
extern int	cache_pages;		/* materialized-page cache size (pages; 0=off) */
extern int	use_layers;			/* rebuild read state from layers (vs segments) */
extern int	ps_object_tier;		/* maintenance uploads sealed layers to the object store */
extern uint32_t ps_nshards;		/* live shard count (1..PS_MAX_SHARDS); set before open */
extern const PsStorage *ps_storage;

/* Open the store and rebuild all in-memory state (timelines, indexes, WAL). */
extern int	ps_core_open(const char *store_dir);

/* Clean-shutdown: flush the memtable into a layer and close the manifest. */
extern void ps_core_close(void);

/* Off-the-write-path maintenance (compaction).  Call when idle; returns 1 if it
 * did work (caller should not sleep), 0 if nothing was due.  The (void) form
 * sweeps every shard (single-threaded frontends); the per-shard form is for a
 * worker thread that owns just its shard. */
extern int	ps_core_maintenance(void);
extern int	ps_core_maintenance_shard(uint32_t shard);

/* Number of image layers currently in the layer map (for stats/diagnostics). */
extern uint32_t ps_core_layer_count(void);

/* Read-path source counts: served from memtable / image layer / segment. */
extern void ps_core_read_stats(uint64_t *mem, uint64_t *layer, uint64_t *seg);

/* The shard that must serve a request; PS_ANY_SHARD if any worker may.  A
 * per-shard worker rejects a request whose shard isn't its own (guards the
 * single-owner invariant against a client that posts on the wrong channel). */
#define PS_ANY_SHARD	UINT32_MAX
extern uint32_t ps_request_shard(const PsChannel *ch);

/* The per-shard materialized-page cache that owns 'key' (for a frontend caching
 * pages outside read_resolve, e.g. the SPDK async path). */
extern PsPgcache *ps_core_pgcache_for(const PsKey *key);

/* Materialized-page cache hit/miss/eviction counts, summed across shards. */
extern void ps_core_pgcache_stats(uint64_t *hits, uint64_t *misses,
								  uint64_t *evictions);

/*
 * Handle every request that is NOT page byte I/O and return 1.  The four
 * byte-I/O ops (EXTEND/WRITEV/READV/READ_AT) and unknown ops return 0 for the
 * frontend to handle.  Sets ch->status/ch->result as appropriate.
 */
extern int	ps_handle_meta(PsChannel *ch);

/* Page byte-I/O helpers used by the frontends' byte-op handlers. */
extern int	append_page(uint32_t timeline, const PsKey *key, uint32_t block,
						const unsigned char *page);
extern PageVer *read_through(uint32_t timeline, const PsKey *key, uint32_t block,
							 uint64_t read_lsn);
/*
 * read_through() variant for lsn-keyed page caches: also reports the timeline the
 * version was found on (*src_tl, the correct cache key under COW inheritance) and
 * whether its page LSN is ambiguous (*ambiguous, i.e. unsafe to cache by lsn).
 */
extern PageVer *read_through_cacheable(uint32_t timeline, const PsKey *key,
									   uint32_t block, uint64_t read_lsn,
									   uint32_t *src_tl, int *ambiguous);
extern int	read_version(const PageVer *v, unsigned char *out);

/* CRC32C of a buffer (page-integrity checks shared with the SPDK read path). */
extern uint32_t ps_crc32c(const void *buf, size_t len);

/*
 * Resolve a read into out (page_size bytes), serving from memtable / image
 * layers with a segment fallback.  Returns 1 if found (out filled), 0 if the
 * page is unwritten.
 */
extern int	read_resolve(uint32_t timeline, const PsKey *key, uint32_t block,
						 uint64_t read_lsn, unsigned char *out);
extern void fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks);

#endif							/* PAGESTORE_CORE_H */
