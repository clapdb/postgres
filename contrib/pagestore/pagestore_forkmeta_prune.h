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
 * future tail.  For the cutoff and each supplied fence it retains every
 * visible SET/DEAD boundary, plus the maximum visible GROW strictly after the
 * latest visible definitive event (latest source event wins a tie).  If no
 * definitive event is visible, the maximum visible GROW in the whole prefix
 * is retained instead.  The keep mask is the union across all horizons.
 * keep receives one byte per input event.  The planner uses no storage
 * proportional to the input.  nitems above INT_MAX are rejected because the
 * return type is int.  The planner returns the number kept, or -1 when the
 * input cannot be proven valid and safe.
 */
extern int ps_forkmeta_prune_plan(const PsForkMetaEvent *events,
						  uint32_t nitems,
						  PsForkMetaFence cutoff,
						  const PsForkMetaFence *fences,
						  uint32_t nfences,
						  unsigned char *keep);

#endif
