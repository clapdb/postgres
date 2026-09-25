/*
 * pagestore_admissible.h -- the single S1.3/S3.2 admissibility predicate,
 * shared bit-for-bit between the read path (pagestore_core.c's
 * fork_event_hidden()/page_select()) and the P2 retention planners
 * (pagestore_prune.c, pagestore_forkmeta_prune.c).
 *
 * BRANCH_SNAPSHOT_SEQ_CAP.md requirement 2 for P2: "the planner must use
 * exactly the same admissibility function as the read path
 * (fork_event_hidden / page_select's admissibility predicate from P1);
 * don't reimplement it."  pagestore_prune.c and pagestore_forkmeta_prune.c
 * are deliberately freestanding (no PostgreSQL/pagestore_core.c type
 * dependencies) so they stay unit-testable in isolation; this header gives
 * them the exact predicate without pulling in pagestore_core.c.
 * pagestore_core.c's fork_event_hidden()/page_select() are refactored (this
 * phase, P2) to delegate their boolean logic to the functions here, so
 * there is exactly one place either rule is spelled out.
 *
 * Design doc S1.3:
 *
 *   admissible(v) <=> p <= L
 *                   && (p < L || v.seq <= X)
 *                   && (v.seq <= S || (p > B_k && v.seq == s_min(p)))
 *
 * "hidden" (used for fork/forkmeta events, S3.2) is the negation, with the
 * escape further restricted by event class (PAGE-class GROW: p > B_k is
 * enough; META/ZEROEXTEND-GROW: also requires META_FIRST; UNSTAMPED: no
 * escape at all, S1.6).
 */
#ifndef PAGESTORE_ADMISSIBLE_H
#define PAGESTORE_ADMISSIBLE_H

#include <stdbool.h>
#include <stdint.h>

#define PS_ADM_SEQ_UNBOUNDED	UINT64_MAX	/* mirrors PS_SEQ_UNBOUNDED */

/* Event/version classification bits.  Mirrors ForkEvent.flags's FEV_F_*
 * (pagestore_core.c) bit for bit, so a ForkEvent's raw flags byte can be
 * passed straight through with no translation; pagestore_forkmeta_prune.c's
 * PsForkMetaEvent.flags reuses the same numbering. */
#define PS_ADM_F_META			0x02	/* SET/DEAD, or a ZEROEXTEND-origin GROW */
#define PS_ADM_F_META_FIRST	0x04	/* the min-seq META event at its own lsn */
#define PS_ADM_F_UNSTAMPED		0x08	/* a WAL-less (req_lsn == 0) op's event */

/* The composed view cap (L, S, X); mirrors pagestore_core.c's ViewCap minus
 * the `legacy` bookkeeping flag, which never affects admissibility. */
typedef struct PsAdmitCap
{
	uint64_t	lsn;			/* L */
	uint64_t	seq;			/* S; PS_ADM_SEQ_UNBOUNDED = infinity */
	uint64_t	strict_seq;		/* X; PS_ADM_SEQ_UNBOUNDED = infinity */
} PsAdmitCap;

/*
 * Shared boundary test: the (p <= L) && (p < L || v.seq <= X) conjuncts,
 * common to every class (design doc S1.3).  Legacy seq 0 always passes the
 * X test (S3.8's "legacy seq 0 is <= S for every S" extends to X the same
 * way -- pre-P1 code already treated a bare admission_seq of 0 as "no
 * cap").
 */
static inline bool
ps_admit_boundary_ok(uint64_t p, uint64_t seq, const PsAdmitCap *c)
{
	if (p > c->lsn)
		return false;
	if (p == c->lsn && c->strict_seq != PS_ADM_SEQ_UNBOUNDED &&
		seq != 0 && seq > c->strict_seq)
		return false;
	return true;
}

/*
 * Relation-page (or object-version) admissibility: true iff v is admissible
 * under cap c at inherited-range boundary B/has_B, given whether v is the
 * minimum-seq version at its own position (s_min(p), design doc S1.3/S1.5).
 * This is page_select()'s per-version `ok` test, factored out so the P2
 * planner's per-fence selection and position-closure logic can use exactly
 * the same rule pagestore_core.c's read path uses.
 */
static inline bool
ps_version_admissible(uint64_t p, uint64_t seq, bool is_smin,
					   const PsAdmitCap *c, uint64_t B, bool has_B)
{
	if (!ps_admit_boundary_ok(p, seq, c))
		return false;
	if (seq == 0 || c->seq == PS_ADM_SEQ_UNBOUNDED || seq <= c->seq)
		return true;
	if (!has_B || p > B)
		return is_smin;
	return false;
}

/*
 * Fork/forkmeta event hidden predicate (design doc S3.2): true iff the event
 * at position p with sequence seq and classification flags is NOT
 * admissible under cap c at inherited-range boundary B/has_B.  flags uses
 * the PS_ADM_F_* bits above; an event with none of them set is PAGE-class
 * GROW, whose escape needs only p > B_k (no META_FIRST requirement).
 */
static inline bool
ps_event_hidden(uint64_t p, uint64_t seq, uint8_t flags,
				 const PsAdmitCap *c, uint64_t B, bool has_B)
{
	if (!ps_admit_boundary_ok(p, seq, c))
		return true;
	if (seq == 0 || c->seq == PS_ADM_SEQ_UNBOUNDED || seq <= c->seq)
		return false;

	/* seq > S: only a class-appropriate escape can still admit it. */
	if (flags & PS_ADM_F_UNSTAMPED)
		return true;
	if (has_B && p <= B)
		return true;			/* inherited range: no escape (S1.5) */
	if (flags & PS_ADM_F_META)
		return !(flags & PS_ADM_F_META_FIRST);
	return false;				/* PAGE-class GROW: p > B_k is enough */
}

#endif							/* PAGESTORE_ADMISSIBLE_H */
