#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_admissible.h"
#include "pagestore_prune.h"

/*
 * Is versions[idx] the minimum-admission_seq version at its own lsn among
 * every input version (design doc S1.3's s_min(p))?  Legacy admission_seq
 * == 0 is already the numeric minimum, so no special case is needed.
 */
static int
version_is_smin(const PsPruneVersion *versions, uint32_t n, uint32_t idx)
{
	for (uint32_t j = 0; j < n; j++)
		if (j != idx && versions[j].lsn == versions[idx].lsn &&
			versions[j].admission_seq < versions[idx].admission_seq)
			return 0;
	return 1;
}

/* The index of the minimum-admission_seq version at this lsn, or n if none
 * exists (the caller only calls this for a position it already knows is
 * occupied). */
static uint32_t
position_min_seq_index(const PsPruneVersion *versions, uint32_t n, uint64_t lsn)
{
	uint32_t	best = n;

	for (uint32_t j = 0; j < n; j++)
		if (versions[j].lsn == lsn &&
			(best == n || versions[j].admission_seq < versions[best].admission_seq))
			best = j;
	return best;
}

int
ps_page_prune_plan_capped(const PsPruneVersion *versions, uint32_t n,
						  PsPruneFence floor, const PsViewFence *fences,
						  uint32_t nfences, uint64_t inherited_below,
						  int has_inherited_below, unsigned char *keep,
						  unsigned char *closure_protect)
{
	int			base = -1;
	int			kept = 0;

	if ((n != 0 && (versions == NULL || keep == NULL)) ||
		(nfences != 0 && fences == NULL))
		return -1;
	if (n == 0)
		return 0;
	memset(keep, 0, n);
	if (closure_protect != NULL)
		memset(closure_protect, 0, n);
	for (uint32_t i = 1; i < n; i++)
		if (versions[i].lsn < versions[i - 1].lsn ||
			(versions[i].lsn == versions[i - 1].lsn &&
			 versions[i].admission_seq < versions[i - 1].admission_seq))
			return -1;

	if (floor.lsn == 0 || floor.admission_seq == 0)
		return -1;

	/*
	 * Step 1 (design doc S3.5 item 1): the operational floor base and the
	 * future tail.  Unchanged from the pre-P2 ps_page_prune_plan(): the
	 * floor stays positional (S = infinity), never gains an escape.
	 */
	for (uint32_t i = 0; i < n; i++)
	{
		/* An exact tuple has one authoritative value: the last append.
		 * This remains true above the prune frontier, where a future fence
		 * at this tuple also resolves to that last value. */
		if (i + 1 < n && versions[i].lsn == versions[i + 1].lsn &&
			versions[i].admission_seq == versions[i + 1].admission_seq)
			continue;
		if (versions[i].lsn <= floor.lsn &&
			(versions[i].lsn < floor.lsn || floor.admission_seq == 0 ||
			 versions[i].admission_seq == 0 ||
			 versions[i].admission_seq <= floor.admission_seq))
			base = (int) i;
		/* Every distinct tuple outside the admitted frontier remains a
		 * legal future fence. */
		else
		{
			keep[i] = 1;
			kept++;
		}
	}
	if (base >= 0 && !keep[base])
	{
		keep[base] = 1;
		kept++;
	}

	/*
	 * Step 2 (design doc S3.5 item 2): each fence's selection under S1.3 --
	 * one version, the greatest (lsn, seq) admissible under that fence's
	 * cap, exactly page_select()'s rule (shared via pagestore_admissible.h,
	 * P2 requirement 2).
	 */
	for (uint32_t f = 0; f < nfences; f++)
	{
		PsAdmitCap	ac;
		int			best = -1;

		ac.lsn = fences[f].lsn;
		ac.seq = fences[f].seq;
		ac.strict_seq = fences[f].strict_seq;
		for (uint32_t i = 0; i < n; i++)
		{
			int			is_smin;

			if (versions[i].lsn > fences[f].lsn)
				break;			/* sorted ascending: nothing further can pass */
			/* version_is_smin() is O(n); only worth computing when the
			 * plain seq <= S disjunct could fail and the escape might be
			 * needed -- unreachable whenever fences[f].seq stays
			 * PS_PRUNE_SEQ_UNBOUNDED (every P1/pre-P2 production fence). */
			is_smin = (versions[i].admission_seq != 0 &&
					   fences[f].seq != PS_PRUNE_SEQ_UNBOUNDED &&
					   versions[i].admission_seq > fences[f].seq) ?
				version_is_smin(versions, n, i) : 0;
			if (!ps_version_admissible(versions[i].lsn,
									   versions[i].admission_seq,
									   is_smin, &ac, inherited_below,
									   has_inherited_below != 0))
				continue;
			if (best < 0 || versions[i].lsn > versions[best].lsn ||
				(versions[i].lsn == versions[best].lsn &&
				 versions[i].admission_seq >= versions[best].admission_seq))
				best = (int) i;
		}
		if (best >= 0 && !keep[best])
		{
			keep[best] = 1;
			kept++;
		}
	}

	/*
	 * Step 3 (design doc S3.5 item 3): position closure.  Computed once
	 * from steps 1-2's kept set and the fence set supplied to *this* call,
	 * not transitively over closure's own additions.
	 */
	for (uint32_t f = 0; f < nfences; f++)
	{
		if (fences[f].seq == PS_PRUNE_SEQ_UNBOUNDED)
			continue;			/* positional fence: closure never applies */
		for (uint32_t i = 0; i < n; i++)
		{
			uint32_t	minidx;

			if (!keep[i])
				continue;
			if (versions[i].lsn > fences[f].lsn)
				continue;
			if (has_inherited_below && versions[i].lsn <= inherited_below)
				continue;
			/* i + 1 < n duplicate-tuple entries above were never marked
			 * kept directly, so every kept i here is a real occupied
			 * position (or the sole/last representative of one). */
			minidx = position_min_seq_index(versions, n, versions[i].lsn);
			if (minidx == n)
				continue;
			if (!keep[minidx])
			{
				keep[minidx] = 1;
				kept++;
			}
			if (closure_protect != NULL)
				closure_protect[minidx] = 1;
		}
	}
	return kept;
}

/*
 * Pre-P2 entry point, kept as a thin conversion wrapper (design doc S3.5:
 * "ps_page_prune_plan() is now a thin wrapper").  Every fence stays
 * positional (S = PS_PRUNE_SEQ_UNBOUNDED), at the root (no inherited
 * range), which provably makes ps_page_prune_plan_capped()'s closure step a
 * no-op and its per-fence selection identical to the pre-P2 algorithm's
 * "newest visible tuple at/below the fence" walk -- see the P1-vs-P2
 * differential test.  fences[i].admission_seq == 0 was this legacy type's
 * own "uncapped" convention (distinct from PsViewFence's
 * PS_PRUNE_SEQ_UNBOUNDED sentinel); both it and UINT64_MAX (already used by
 * several existing call sites, e.g. compaction's control/SLRU floor
 * construction) convert to PS_PRUNE_SEQ_UNBOUNDED here.
 */
int
ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
				   PsPruneFence floor, const PsPruneFence *fences,
				   uint32_t nfences, unsigned char *keep)
{
	PsViewFence local_fences[8] = {{0, 0, 0}};
	PsViewFence *vf = local_fences;
	int			rc;

	if (nfences != 0 && fences == NULL)
		return -1;
	if (nfences > 8)
	{
		vf = malloc((size_t) nfences * sizeof(*vf));
		if (vf == NULL)
			return -1;
	}
	for (uint32_t i = 0; i < nfences; i++)
		vf[i] = ps_prune_fence_to_view(fences[i]);
	rc = ps_page_prune_plan_capped(versions, n, floor, vf, nfences, 0, 0,
									keep, NULL);
	if (vf != local_fences)
		free(vf);
	return rc;
}
