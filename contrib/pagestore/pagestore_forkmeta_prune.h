/* Pure fork-metadata event retention policy used by forkmeta compaction. */
#ifndef PAGESTORE_FORKMETA_PRUNE_H
#define PAGESTORE_FORKMETA_PRUNE_H

#include <stdint.h>

typedef enum PsForkMetaEventKind
{
	PS_FORKMETA_GROW = 0,
	PS_FORKMETA_SET = 1,
	PS_FORKMETA_DEAD = 2
} PsForkMetaEventKind;

/*
 * P2 (BRANCH_SNAPSHOT_SEQ_CAP.md S3.7): flags classifies the event for the
 * S1.3/S3.2 admissibility rule, using the exact bit numbering that
 * pagestore_admissible.h's PS_ADM_F_* and pagestore_core.c's
 * ForkEvent.flags use, so a caller building this array from a ForkEnt's
 * ForkEvents can pass
 * its flags byte straight through.  SET/DEAD are always META regardless of
 * this field (see event_admit_flags() in the .c file) -- flags only needs
 * to distinguish a ZEROEXTEND-origin GROW (META) from a page-append-origin
 * GROW (PAGE-class), and to carry META_FIRST/UNSTAMPED.  flags == 0 (every
 * pre-P2 caller, via the ps_forkmeta_prune_plan()/_required() wrappers,
 * which zero-fill it) is a plain PAGE-class GROW / non-UNSTAMPED SET/DEAD,
 * preserving today's behaviour exactly.
 */
#define PS_FORKMETA_F_META			0x02
#define PS_FORKMETA_F_META_FIRST	0x04
#define PS_FORKMETA_F_UNSTAMPED		0x08

typedef struct PsForkMetaEvent
{
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint32_t	nblocks;
	unsigned char kind;
	unsigned char flags;		/* P2: PS_FORKMETA_F_*, default 0 */
} PsForkMetaEvent;

typedef struct PsForkMetaFence
{
	uint64_t	lsn;
	uint64_t	admission_seq;
} PsForkMetaFence;

/*
 * P2: a full BRANCH_SNAPSHOT_SEQ_CAP.md S1.3 view cap (L, S, X), for the
 * capped planner below.  PS_FORKMETA_SEQ_UNBOUNDED (== UINT64_MAX, mirrors
 * pagestore_admissible.h's PS_ADM_SEQ_UNBOUNDED) is the sentinel for "no
 * bound" on seq/strict_seq -- unlike PsForkMetaFence.admission_seq above,
 * where 0 is this legacy type's own "uncapped" convention.
 */
#define PS_FORKMETA_SEQ_UNBOUNDED	((uint64_t) -1)

typedef struct PsForkMetaViewFence
{
	uint64_t	lsn;			/* L */
	uint64_t	seq;			/* S; PS_FORKMETA_SEQ_UNBOUNDED = infinity */
	uint64_t	strict_seq;		/* X: extra bound exactly at position == lsn */
} PsForkMetaViewFence;

/*
 * Plan retention for one fork's append-ordered event stream.  Events must be
 * nondecreasing in LSN.  Within one LSN, nonzero admission sequences must be
 * nondecreasing even across intervening sequence-zero legacy events; legacy
 * events retain their source order and are visible at every exact sequence
 * fence.
 *
 * The operational cutoff is an exact durable tuple and both its LSN and
 * admission sequence must be nonzero.  Discrete fences may use sequence zero
 * for wildcard same-LSN visibility, but every fence must have a nonzero LSN
 * and lie within the cutoff's visibility domain.  Fences are deliberately
 * accepted in arbitrary order and duplicates are harmless.
 *
 * The planner marks every event not visible at the operational cutoff as the
 * future tail.  For the cutoff and each supplied fence it retains the base
 * the reader resolves there: the latest visible SET/DEAD, the maximum visible
 * GROW strictly after it (or the maximum visible GROW in the whole prefix
 * when no definitive event is visible), and the visible SET/DEAD with the
 * smallest size (latest on a tie), which is the inheritance fence a branch
 * applies to its ancestors.  `required` marks events the caller must keep
 * regardless: a definitive event still needed to invalidate a retained page
 * version that predates it.  The keep mask is the union across all horizons
 * and required events.  keep receives one byte per input event.  The planner
 * uses no storage proportional to the input.  nitems above INT_MAX are
 * rejected because the return type is int.  The planner returns the number
 * kept, or -1 when the input cannot be proven valid and safe.
 */
extern int ps_forkmeta_prune_plan(const PsForkMetaEvent *events,
						  uint32_t nitems,
						  PsForkMetaFence cutoff,
						  const PsForkMetaFence *fences,
						  uint32_t nfences,
						  unsigned char *keep);
extern int ps_forkmeta_prune_plan_required(const PsForkMetaEvent *events,
										   uint32_t nitems,
										   PsForkMetaFence cutoff,
										   const PsForkMetaFence *fences,
										   uint32_t nfences,
										   const unsigned char *required,
										   unsigned char *keep);

/*
 * P2 (design doc S3.7, the highest-risk planner): the full-admissibility
 * planner.
 *
 * S3.7 item 1: every mask (the operational cutoff and every fence) uses the
 * exact S1.3/S3.2 admissibility rule -- pagestore_admissible.h's
 * ps_event_hidden(), the same predicate pagestore_core.c's read path uses
 * (P2 requirement 2) -- rather than the plain positional
 * "lsn <= fence.lsn" test.  This lets a finite-seq mask hide a same-position
 * event admitted after a view froze, while a PAGE-class GROW in the
 * inherited range (inherited_below/has_inherited_below, design doc S1.5's
 * B_k) still loses its escape exactly as fork_event_hidden() does.
 *
 * S3.7 item 2: closure.  A retained META event (SET/DEAD, or a
 * ZEROEXTEND-origin GROW) at an escape-eligible position (has_inherited_below
 * is false, or lsn > inherited_below) implies retaining that position's
 * META_FIRST event too -- the fact that it *is* the first arrival must
 * survive even if some mask's fold currently hides it.  A hidden UNSTAMPED
 * event is retained whenever a later event (strictly greater admission_seq,
 * nonzero) at its exact LSN is retained -- S1.6 gives UNSTAMPED no escape of
 * its own, but a later same-LSN event's own retention already pins that
 * position, so keeping the UNSTAMPED event alongside it costs nothing and
 * preserves the position's full history for a cap between the two.
 *
 * S3.7 item 3: GROW retention is unchanged from
 * ps_forkmeta_prune_plan_required() -- retain_horizon() keeps the largest
 * *admissible* PAGE GROW after each mask's latest *admissible* visible
 * definitive event, now computed under the full mask instead of the plain
 * positional test.
 *
 * `required` keeps its ps_forkmeta_prune_plan_required() meaning.  Returns
 * the number kept, or -1 for invalid input (same validation as
 * ps_forkmeta_prune_plan_required(), plus rejecting a fence lsn outside the
 * cutoff's admissibility domain exactly as before).
 */
extern int ps_forkmeta_prune_plan_capped(const PsForkMetaEvent *events,
										 uint32_t nitems,
										 PsForkMetaFence cutoff,
										 const PsForkMetaViewFence *fences,
										 uint32_t nfences,
										 uint64_t inherited_below,
										 int has_inherited_below,
										 const unsigned char *required,
										 unsigned char *keep);

/*
 * S3.7 item 5: derived fences.  For every live view (l, s, x) with
 * l >= cutoff.lsn and s < cutoff.admission_seq, a forkmeta cutover that
 * folds everything at or below the cutoff into one snapshot base would
 * silently lose the s1.3 distinction that view's own finite S still needs
 * *at the cutoff position itself* (its own L may be far beyond the cutoff,
 * so its own fence, above, does not reach back to protect the cutoff-time
 * mask).  Emit one extra fence (cutoff.lsn, s, strict = cutoff.admission_seq)
 * per such view, so ps_forkmeta_prune_plan_capped()'s masks at the cutoff
 * still honour it.  out must have room for nviews entries; returns the
 * number written (<= nviews).
 */
extern uint32_t ps_forkmeta_derive_fences(const PsForkMetaViewFence *views,
										  uint32_t nviews,
										  PsForkMetaFence cutoff,
										  PsForkMetaViewFence *out);
#endif
