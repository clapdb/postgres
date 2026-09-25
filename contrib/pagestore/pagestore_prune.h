/* Pure page-version retention policy used by image compaction. */
#ifndef PAGESTORE_PRUNE_H
#define PAGESTORE_PRUNE_H

#include <stdint.h>

typedef struct PsPruneVersion
{
	uint64_t	lsn;
	uint64_t	admission_seq;
} PsPruneVersion;

typedef PsPruneVersion PsPruneFence;

/*
 * versions must be sorted by (lsn, admission_seq), preserving source append
 * order for exact tuple ties; the last tied element is authoritative.  keep
 * receives one byte per input version.  A floor with either component zero has
 * no proven reclamation meaning and is rejected; the caller must supply an
 * exact durable materialization cutoff.  fences
 * are exact reader-admission or structural branch requirements and do not
 * lower the moving operational floor.
 * Returns the number of kept versions, or -1 for invalid input.
 */
extern int ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
							  PsPruneFence floor, const PsPruneFence *fences,
							  uint32_t nfences, unsigned char *keep);

/*
 * P2: BRANCH_SNAPSHOT_SEQ_CAP.md S3.5.  PS_PRUNE_SEQ_UNBOUNDED (== UINT64_MAX,
 * mirroring pagestore_core.c's PS_SEQ_UNBOUNDED / pagestore_admissible.h's
 * PS_ADM_SEQ_UNBOUNDED) is the *only* "no bound" sentinel for a PsViewFence's
 * seq/strict_seq -- unlike the legacy PsPruneFence.admission_seq field above,
 * where both 0 and UINT64_MAX happen to behave as "uncapped" (S3.5's
 * ps_page_prune_plan() wrapper below converts between the two conventions
 * so its own callers see no change).
 */
#define PS_PRUNE_SEQ_UNBOUNDED	((uint64_t) -1)

typedef struct PsViewFence
{
	uint64_t	lsn;			/* L */
	uint64_t	seq;			/* S; PS_PRUNE_SEQ_UNBOUNDED = infinity.  A
								 * finite S makes this fence's position
								 * closure (below) apply. */
	uint64_t	strict_seq;		/* X: extra bound exactly at position == lsn */
} PsViewFence;

/*
 * Convert a legacy PsPruneFence (X-only, 0-or-UINT64_MAX-is-uncapped) into a
 * positional PsViewFence (S = PS_PRUNE_SEQ_UNBOUNDED).  Used by
 * ps_page_prune_plan()'s own wrapper and by callers that want to route a
 * legacy fence array through ps_page_prune_plan_capped() directly (for
 * example to get its closure_protect output) without duplicating the
 * conversion.
 */
static inline PsViewFence
ps_prune_fence_to_view(PsPruneFence f)
{
	PsViewFence	v;

	v.lsn = f.lsn;
	v.seq = PS_PRUNE_SEQ_UNBOUNDED;
	v.strict_seq = (f.admission_seq == 0 || f.admission_seq == PS_PRUNE_SEQ_UNBOUNDED) ?
		PS_PRUNE_SEQ_UNBOUNDED : f.admission_seq;
	return v;
}

/*
 * The design doc S1.3/S3.5 planner: extends ps_page_prune_plan() with
 * per-fence selection under the full admissibility rule (S1.3's escape,
 * i.e. a fence with finite seq may have to fall back to an *earlier*
 * position's minimum-seq version rather than the newest version at/below
 * its lsn -- ps_page_prune_plan()'s pure "newest admission_seq <=
 * strict_seq" walk cannot do this) and with S3.5 item 3's position
 * closure: at every position where the floor base or a fence's own
 * selection keeps a version, if that position lies at or below some
 * finite-seq fence's lsn and is escape-eligible
 * (inherited_below < lsn, or has_inherited_below is false -- design doc
 * S1.5's B_k, "-infinity" at the root), the minimum-admission_seq version
 * at that position (S1.3's s_min(p); a legacy admission_seq == 0 is
 * already the numeric minimum, so it needs no special case) is also kept,
 * because it may be the s_min(p) fact a future or concurrently-registered
 * finite-seq fence's own escape depends on.  Closure is computed once from
 * the fences supplied to *this* call, not transitively (design doc S3.5:
 * "closure depends on the current fence set").
 *
 * closure_protect, if non-NULL, receives one byte per input version, set to
 * 1 for every version position closure required.  The caller (P2 checklist
 * item 8) must never let prune_version_needed()'s forkmeta-invalidation
 * check drop a version with closure_protect[i] == 1: invalidation hides
 * bytes at a position, it never erases the first-arrival/s_min(p) fact
 * retention depends on.
 *
 * A fence whose seq is PS_PRUNE_SEQ_UNBOUNDED never triggers closure and is
 * selected exactly as ps_page_prune_plan()'s fence loop already does; with
 * *every* fence's seq unbounded, this function's keep[] output is
 * identical to ps_page_prune_plan()'s (see the P1-vs-P2 differential test,
 * pagestore_prune_capped_test.c) -- ps_page_prune_plan() is now a thin
 * wrapper around this function.
 *
 * Returns the number of kept versions, or -1 for invalid input (same rules
 * as ps_page_prune_plan()).
 */
extern int ps_page_prune_plan_capped(const PsPruneVersion *versions, uint32_t n,
									 PsPruneFence floor,
									 const PsViewFence *fences, uint32_t nfences,
									 uint64_t inherited_below,
									 int has_inherited_below,
									 unsigned char *keep,
									 unsigned char *closure_protect);

#endif
