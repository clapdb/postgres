/*-------------------------------------------------------------------------
 *
 * pagestore_pgcache.h
 *	  Materialized-page cache for the page-store daemon (LSM phase 8).
 *
 * A database-adapted cache (NOT a generic fs/block LRU): it caches *materialized
 * page versions* keyed by the semantic identity (timeline, key, block,
 * version_lsn), sitting in front of the layer/segment read so a hot read is a
 * RAM hit instead of a layer pread + layer-map scan (and, once delta replay
 * exists, instead of a redo).  Eviction is scan-resistant CLOCK: a new entry is
 * inserted unreferenced, so a one-shot scan's pages are evicted first and only a
 * re-referenced page survives.
 *
 * The version_lsn is the *resolved* authoritative version (page_visible's lsn),
 * not the read_lsn -- so one cached version serves every read_lsn that resolves
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

/* Look up (timeline, key, block, version_lsn); on hit copy page_size bytes into
 * out and return 1, else 0. */
extern int	ps_pgcache_lookup(uint32_t timeline, const PsKey *key,
							  uint32_t block, uint64_t version_lsn, void *out);

/* Insert/refresh (timeline, key, block, version_lsn) -> page. */
extern void ps_pgcache_insert(uint32_t timeline, const PsKey *key,
							  uint32_t block, uint64_t version_lsn,
							  const void *page);

extern void ps_pgcache_stats(uint64_t *hits, uint64_t *misses,
							 uint64_t *evictions);

#endif							/* PAGESTORE_PGCACHE_H */
