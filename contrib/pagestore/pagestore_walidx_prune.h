/* Pure replacement-base selection for per-page WAL-index compaction. */
#ifndef PAGESTORE_WALIDX_PRUNE_H
#define PAGESTORE_WALIDX_PRUNE_H

#include <stdint.h>

typedef struct PsWalIdxPruneItem
{
	uint64_t	lsn;
	uint64_t	end_lsn;
	unsigned char known;
	unsigned char fpi;
} PsWalIdxPruneItem;

/*
 * Select the union of reconstruction chains required at cutoff and at every
 * discrete retention horizon.  Items must be ordered by WAL start LSN.  A
 * chain begins at the newest FPI whose complete record is visible at the
 * horizon and ends at the last complete visible record.  Records completing
 * after cutoff are always retained for future horizons.
 *
 * Unknown legacy metadata at or below cutoff, or a visible chain without an
 * FPI, fails closed.  keep receives one byte per input item.  Returns the
 * number of retained items, or -1 when safe compaction cannot be proven.
 */
extern int ps_walidx_prune_plan(const PsWalIdxPruneItem *items, uint32_t n,
								uint64_t cutoff, const uint64_t *horizons,
								uint32_t nhorizons, unsigned char *keep);

/*
 * Like ps_walidx_prune_plan, with durable replacement page bases: ascending,
 * nonzero page-version LSNs whose complete page image is durably stored and
 * retained at every horizon that can see it.  At a horizon the chain may
 * start at the newest visible base instead of an FPI: every record completing
 * at or below that base is covered by the stored image, so a page whose
 * newest visible record is covered needs no index entry at that horizon and a
 * visible chain without an FPI no longer fails closed when a base precedes
 * its uncovered records.  Unsorted or zero bases fail closed.
 */
extern int ps_walidx_prune_plan_bases(const PsWalIdxPruneItem *items,
									  uint32_t n, const uint64_t *bases,
									  uint32_t nbases, uint64_t cutoff,
									  const uint64_t *horizons,
									  uint32_t nhorizons, unsigned char *keep);

#endif
