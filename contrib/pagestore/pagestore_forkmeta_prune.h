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

typedef struct PsForkMetaEvent
{
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint32_t	nblocks;
	unsigned char kind;
} PsForkMetaEvent;

typedef struct PsForkMetaFence
{
	uint64_t	lsn;
	uint64_t	admission_seq;
} PsForkMetaFence;

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
#endif
