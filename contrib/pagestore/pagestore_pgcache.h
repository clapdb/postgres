/*-------------------------------------------------------------------------
 *
 * pagestore_pgcache.h
 *	  Materialized-page cache for the page-store daemon (LSM phase 8).
 *
 * A database-adapted cache (NOT a generic fs/block LRU): it caches *materialized
 * page versions* keyed by the semantic identity (timeline, key, block,
 * version_lsn, admission_seq), sitting in front of the layer/segment read so a
 * hot read is a RAM hit instead of a layer pread + layer-map scan (and, once delta replay
 * exists, instead of a redo).  Eviction is scan-resistant CLOCK: a new entry is
 * inserted unreferenced, so a one-shot scan's pages are evicted first and only a
 * re-referenced page survives.
 *
 * The version_lsn and admission_seq are the resolved authoritative version,
 * not the requested caps, so one cached version serves every read that resolves
 * to it.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_PGCACHE_H
#define PAGESTORE_PGCACHE_H

#include <stdint.h>

#include "pagestore_ipc.h"

/* Initialize with a capacity in pages (0 disables the cache). */
extern void ps_pgcache_init(uint32_t max_pages, uint32_t page_size);
extern void ps_pgcache_free(void);

/* Look up the full semantic key; on hit copy page_size bytes into out and
 * return 1, else 0. */
extern int	ps_pgcache_lookup(uint32_t timeline, const PsKey *key,
							  uint32_t block, uint64_t version_lsn,
							  uint64_t admission_seq, void *out);

/* Insert/refresh the full semantic key -> page. */
extern void ps_pgcache_invalidate(uint32_t timeline, const PsKey *key,
								  uint32_t block, uint64_t version_lsn,
								  uint64_t admission_seq);
extern void ps_pgcache_invalidate_timeline(uint32_t timeline);
extern int	ps_pgcache_has_timeline(uint32_t timeline);
extern void ps_pgcache_insert(uint32_t timeline, const PsKey *key,
							  uint32_t block, uint64_t version_lsn,
							  uint64_t admission_seq, const void *page);

extern void ps_pgcache_stats(uint64_t *hits, uint64_t *misses,
							 uint64_t *evictions);

#endif							/* PAGESTORE_PGCACHE_H */
