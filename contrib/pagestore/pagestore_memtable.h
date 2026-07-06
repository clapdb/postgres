/*-------------------------------------------------------------------------
 *
 * pagestore_memtable.h
 *	  Mutable in-memory staging of recent page versions, flushed into immutable
 *	  image layers (LSM phase 2).
 *
 * The memtable holds full-page versions (with their bytes) as they are written.
 * When it fills, flush groups the staged versions by timeline and writes one
 * image layer per timeline via ps_image_layer_write(); the caller supplies
 * layer-id allocation and a per-layer callback (to record the layer in the
 * manifest and layer map), so the memtable does not depend on those modules.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_MEMTABLE_H
#define PAGESTORE_MEMTABLE_H

#include <stdint.h>

#include "pagestore_ipc.h"
#include "pagestore_layer.h"

typedef struct PsMemtable PsMemtable;

/* allocate the next durable layer id */
typedef uint64_t (*PsAllocLayerId) (void *ctx);

/* record a freshly written+sealed layer (e.g. manifest add + layer map add) */
typedef int (*PsOnLayer) (void *ctx, const PsLayerDesc *desc);

extern PsMemtable *ps_memtable_create(uint32_t page_size, uint32_t threshold);
extern void ps_memtable_destroy(PsMemtable *mt);

/* Stage one page version (copies the page bytes).  Returns 0 on success. */
extern int	ps_memtable_put(PsMemtable *mt, uint32_t timeline, const PsKey *key,
							uint32_t block, uint64_t lsn, const void *page);

extern uint32_t ps_memtable_count(const PsMemtable *mt);
extern int	ps_memtable_full(const PsMemtable *mt);	/* count >= threshold */

/*
 * Newest staged version of (timeline, key, block) with lsn <= read_lsn (this
 * timeline only -- ancestry is the caller's job).  On a hit copies page_size
 * bytes into out, stores its lsn in *out_lsn, and returns 1; else 0.
 */
extern int	ps_memtable_lookup(const PsMemtable *mt, uint32_t timeline,
							   const PsKey *key, uint32_t block,
							   uint64_t read_lsn, uint64_t *out_lsn, void *out);

/*
 * Flush all staged versions: group by timeline, write one image layer per
 * timeline, invoke on_layer() for each, then empty the memtable.  Returns 0 on
 * success (the memtable is emptied even on a partial failure path is avoided --
 * a failed layer leaves the memtable intact for retry).
 */
extern int	ps_memtable_flush(PsMemtable *mt, PsAllocLayerId alloc,
							  PsOnLayer on_layer, void *ctx);

#endif							/* PAGESTORE_MEMTABLE_H */
