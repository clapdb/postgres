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
 * Like ps_walidx_prune_plan, with replacement bases.  `bases` are ascending,
 * nonzero page-version LSNs whose complete page image is durably stored;
 * `deaths` are ascending, nonzero LSNs at or after which the block is
 * definitively outside its relation (unlink, truncate, or an absence proven
 * at a planning horizon), so nothing at all needs reconstructing there.  At
 * a horizon the chain may start at the newest visible base instead of an
 * FPI: every record completing at or below that base is covered, a page
 * whose newest visible record is covered needs no entry at that horizon, and
 * a visible chain without an FPI no longer fails closed when a base precedes
 * its uncovered records.
 *
 * A stored base is only a promise while the page-history retention that
 * keeps it also protects the horizon.  `cutoff_bases_ok` and the per-horizon
 * `horizon_bases_ok` flags say which horizons are protected that way; an
 * unprotected horizon ignores stored bases and keeps its FPI-led chain.
 * Death bases hold at every horizon.  Unsorted or zero bases fail closed.
 */
extern int ps_walidx_prune_plan_bases(const PsWalIdxPruneItem *items,
									  uint32_t n, const uint64_t *bases,
									  uint32_t nbases, const uint64_t *deaths,
									  uint32_t ndeaths, uint64_t cutoff,
									  int cutoff_bases_ok,
									  const uint64_t *horizons,
									  uint32_t nhorizons,
									  const unsigned char *horizon_bases_ok,
									  unsigned char *keep);
#endif
