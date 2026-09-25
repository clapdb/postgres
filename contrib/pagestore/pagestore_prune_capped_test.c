/*
 * P2 (BRANCH_SNAPSHOT_SEQ_CAP.md S3.5, S8.2) property tests for the page
 * planner, ps_page_prune_plan_capped().  These are merge blockers per the
 * design doc.
 *
 *  1. A frozen, verbatim copy of the pre-P2 ps_page_prune_plan() body
 *     (copied from pagestore_prune.c before this phase's changes), fuzzed
 *     against the current ps_page_prune_plan() wrapper: with every fence
 *     positional, the two must be bit-identical.
 *  2. Read/prune consistency: for random histories and random finite-S
 *     views, the version ps_version_admissible() (the shared S1.3
 *     predicate, pagestore_admissible.h -- the same one page_select() uses)
 *     selects over the *full* history equals what it selects over the
 *     *kept-only* subset, including after appending random future
 *     arrivals, and across repeated (multi-round) prune/compaction cycles.
 *  3. Named cases from S8.2.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_admissible.h"
#include "pagestore_prune.h"

static int checks;
static int failed;

static void
check(int ok, const char *name)
{
	checks++;
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failed++;
	}
}

/* --- 1. Frozen pre-P2 reference (verbatim from pagestore_prune.c, the P1
 * baseline this phase started from) --- */
static int
ref_ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
					   PsPruneFence floor, const PsPruneFence *fences,
					   uint32_t nfences, unsigned char *keep)
{
	int			base = -1;
	int			kept = 0;

	if ((n != 0 && (versions == NULL || keep == NULL)) ||
		(nfences != 0 && fences == NULL))
		return -1;
	if (n == 0)
		return 0;
	memset(keep, 0, n);
	for (uint32_t i = 1; i < n; i++)
		if (versions[i].lsn < versions[i - 1].lsn ||
			(versions[i].lsn == versions[i - 1].lsn &&
			 versions[i].admission_seq < versions[i - 1].admission_seq))
			return -1;

	if (floor.lsn == 0 || floor.admission_seq == 0)
		return -1;

	for (uint32_t i = 0; i < n; i++)
	{
		if (i + 1 < n && versions[i].lsn == versions[i + 1].lsn &&
			versions[i].admission_seq == versions[i + 1].admission_seq)
			continue;
		if (versions[i].lsn <= floor.lsn &&
			(versions[i].lsn < floor.lsn || floor.admission_seq == 0 ||
			 versions[i].admission_seq == 0 ||
			 versions[i].admission_seq <= floor.admission_seq))
			base = (int) i;
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
	for (uint32_t f = 0; f < nfences; f++)
	{
		int visible = -1;

		for (uint32_t i = 0; i < n; i++)
			if (versions[i].lsn <= fences[f].lsn &&
				(versions[i].lsn < fences[f].lsn ||
				 fences[f].admission_seq == 0 ||
				 versions[i].admission_seq == 0 ||
				 versions[i].admission_seq <= fences[f].admission_seq))
				visible = (int) i;
			else if (versions[i].lsn > fences[f].lsn)
				break;
		if (visible >= 0 && !keep[visible])
		{
			keep[visible] = 1;
			kept++;
		}
	}
	return kept;
}

static uint64_t
xorshift(uint64_t *s)
{
	*s ^= *s << 13;
	*s ^= *s >> 7;
	*s ^= *s << 17;
	return *s;
}

#define MAXN 24

static void
differential_fuzz(uint64_t seed, uint32_t niter)
{
	uint64_t	s = seed ? seed : 1;
	uint32_t	mismatches = 0;

	for (uint32_t iter = 0; iter < niter; iter++)
	{
		PsPruneVersion versions[MAXN];
		PsPruneFence fences[6];
		unsigned char keep_ref[MAXN];
		unsigned char keep_new[MAXN];
		uint32_t	n = 1 + (uint32_t) (xorshift(&s) % MAXN);
		uint32_t	nfences = (uint32_t) (xorshift(&s) % 6);
		uint64_t	lsn = 0;
		uint64_t	seq = 0;
		PsPruneFence floor;
		int			rc_ref,
					rc_new;

		for (uint32_t i = 0; i < n; i++)
		{
			if (xorshift(&s) % 4 != 0)
				lsn += 1 + xorshift(&s) % 20;
			if (xorshift(&s) % 5 == 0)
				seq = 0;			/* legacy */
			else
				seq += 1 + xorshift(&s) % 20;
			versions[i].lsn = lsn ? lsn : 1;
			versions[i].admission_seq = seq;
		}
		floor.lsn = 1 + xorshift(&s) % (lsn + 5);
		floor.admission_seq = 1 + xorshift(&s) % (seq + 5);
		for (uint32_t f = 0; f < nfences; f++)
		{
			fences[f].lsn = 1 + xorshift(&s) % (lsn + 5);
			switch (xorshift(&s) % 3)
			{
				case 0:
					fences[f].admission_seq = 0;
					break;
				case 1:
					fences[f].admission_seq = UINT64_MAX;
					break;
				default:
					fences[f].admission_seq = 1 + xorshift(&s) % (seq + 5);
			}
		}
		rc_ref = ref_ps_page_prune_plan(versions, n, floor, fences, nfences,
										keep_ref);
		rc_new = ps_page_prune_plan(versions, n, floor, fences, nfences,
									keep_new);
		if (rc_ref != rc_new ||
			(rc_ref >= 0 && memcmp(keep_ref, keep_new, n) != 0))
			mismatches++;
	}
	{
		char		name[128];

		snprintf(name, sizeof(name),
				 "P1-vs-P2 differential (seed=%llu, niter=%u, mismatches=%u)",
				 (unsigned long long) seed, niter, mismatches);
		check(mismatches == 0, name);
	}
}

/* --- 2. Read/prune consistency (S8.2 merge blocker) --- */

/* Reference selection: the greatest (lsn, seq) version admissible under
 * cap over `versions`, using the exact shared S1.3 predicate page_select()
 * uses (pagestore_admissible.h). */
static int
select_ref(const PsPruneVersion *versions, uint32_t n, const PsAdmitCap *cap,
		  uint64_t B, int has_B)
{
	int			best = -1;

	for (uint32_t i = 0; i < n; i++)
	{
		int			is_smin = 1;

		for (uint32_t j = 0; j < n; j++)
			if (j != i && versions[j].lsn == versions[i].lsn &&
				versions[j].admission_seq < versions[i].admission_seq)
			{
				is_smin = 0;
				break;
			}
		if (!ps_version_admissible(versions[i].lsn, versions[i].admission_seq,
								   is_smin, cap, B, has_B != 0))
			continue;
		if (best < 0 || versions[i].lsn > versions[best].lsn ||
			(versions[i].lsn == versions[best].lsn &&
			 versions[i].admission_seq >= versions[best].admission_seq))
			best = (int) i;
	}
	return best;
}

static void
consistency_fuzz(uint64_t seed, uint32_t niter)
{
	uint64_t	s = seed ? seed : 2;
	uint32_t	violations = 0;
	uint32_t	future_violations = 0;

	for (uint32_t iter = 0; iter < niter; iter++)
	{
		PsPruneVersion versions[MAXN];
		PsViewFence views[6];
		unsigned char keep[MAXN];
		unsigned char closure[MAXN];
		uint32_t	n = 2 + (uint32_t) (xorshift(&s) % (MAXN - 2));
		uint32_t	nviews = 1 + (uint32_t) (xorshift(&s) % 5);
		uint64_t	lsn = 0;
		uint64_t	seq = 0;
		PsPruneFence floor;
		int			rc;
		PsPruneVersion kept_only[MAXN];
		uint32_t	nkept = 0;

		for (uint32_t i = 0; i < n; i++)
		{
			if (xorshift(&s) % 3 != 0)
				lsn += 1 + xorshift(&s) % 15;
			seq += (xorshift(&s) % 6 == 0) ? 0 : 1 + xorshift(&s) % 15;
			versions[i].lsn = lsn ? lsn : 1;
			versions[i].admission_seq = seq;
		}
		floor.lsn = 1 + xorshift(&s) % (lsn + 5);
		floor.admission_seq = 1 + xorshift(&s) % (seq + 5);
		for (uint32_t v = 0; v < nviews; v++)
		{
			views[v].lsn = 1 + xorshift(&s) % (lsn + 5);
			/* At least one finite-S "registered view" every iteration --
			 * S8.2: "finite caps are reachable only from tests". */
			views[v].seq = 1 + xorshift(&s) % (seq + 5);
			views[v].strict_seq = (xorshift(&s) % 2) ?
				PS_PRUNE_SEQ_UNBOUNDED : 1 + xorshift(&s) % (seq + 5);
		}
		rc = ps_page_prune_plan_capped(versions, n, floor, views, nviews,
									   0, 0, keep, closure);
		check(rc >= 0, "capped plan accepts a well-formed random history");
		if (rc < 0)
			continue;
		for (uint32_t i = 0; i < n; i++)
			if (keep[i])
				kept_only[nkept++] = versions[i];

		for (uint32_t v = 0; v < nviews; v++)
		{
			PsAdmitCap	ac = {views[v].lsn, views[v].seq, views[v].strict_seq};
			int			before = select_ref(versions, n, &ac, 0, 0);
			int			after = select_ref(kept_only, nkept, &ac, 0, 0);
			int			same = (before < 0 && after < 0) ||
				(before >= 0 && after >= 0 &&
				 versions[before].lsn == kept_only[after].lsn &&
				 versions[before].admission_seq == kept_only[after].admission_seq);

			if (!same)
				violations++;

			/* Append a random future arrival (lsn strictly above the whole
			 * history) and re-check: a fence never reaches into the future,
			 * so the answer must be unaffected. */
			{
				PsPruneVersion future_kept[MAXN + 1];
				uint32_t	nf = nkept;
				PsPruneVersion extra;
				int			after_future;

				memcpy(future_kept, kept_only, nkept * sizeof(*kept_only));
				extra.lsn = lsn + 100 + xorshift(&s) % 50;
				extra.admission_seq = seq + 100 + xorshift(&s) % 50;
				future_kept[nf++] = extra;
				after_future = select_ref(future_kept, nf, &ac, 0, 0);
				same = (before < 0 && after_future < 0) ||
					(before >= 0 && after_future >= 0 &&
					 versions[before].lsn == future_kept[after_future].lsn &&
					 versions[before].admission_seq ==
					 future_kept[after_future].admission_seq);
				if (!same)
					future_violations++;
			}
		}
	}
	{
		char		name[160];

		snprintf(name, sizeof(name),
				 "read/prune consistency (seed=%llu, niter=%u, "
				 "violations=%u, post-future-arrival violations=%u)",
				 (unsigned long long) seed, niter, violations,
				 future_violations);
		check(violations == 0 && future_violations == 0, name);
	}
}

/* Multi-round: prune, then plan again over the survivors with a fresh
 * (larger) fence set, as repeated compaction/checkpoint cycles do.
 * Consistency must hold at every round. */
static void
multiround_fuzz(uint64_t seed, uint32_t nrounds, uint32_t niter)
{
	uint64_t	s = seed ? seed : 3;
	uint32_t	violations = 0;

	for (uint32_t iter = 0; iter < niter; iter++)
	{
		PsPruneVersion cur[MAXN];
		uint32_t	n = 2 + (uint32_t) (xorshift(&s) % (MAXN - 2));
		uint64_t	lsn = 0,
					seq = 0;
		/* Views registered before round 0 stay live across every round
		 * (S3.5: "a view registered later has S' >= every existing seq"). */
		PsViewFence views[4];
		uint32_t	nviews = 1 + (uint32_t) (xorshift(&s) % 4);

		for (uint32_t i = 0; i < n; i++)
		{
			if (xorshift(&s) % 3 != 0)
				lsn += 1 + xorshift(&s) % 10;
			seq += (xorshift(&s) % 6 == 0) ? 0 : 1 + xorshift(&s) % 10;
			cur[i].lsn = lsn ? lsn : 1;
			cur[i].admission_seq = seq;
		}
		for (uint32_t v = 0; v < nviews; v++)
		{
			views[v].lsn = 1 + xorshift(&s) % (lsn + 5);
			views[v].seq = 1 + xorshift(&s) % (seq + 5);
			views[v].strict_seq = PS_PRUNE_SEQ_UNBOUNDED;
		}
		for (uint32_t round = 0; round < nrounds; round++)
		{
			unsigned char keep[MAXN];
			unsigned char closure[MAXN];
			PsPruneFence floor = {1 + xorshift(&s) % (lsn + 5),
								   1 + xorshift(&s) % (seq + 5)};
			PsPruneVersion next[MAXN];
			uint32_t	nnext = 0;
			int			rc = ps_page_prune_plan_capped(cur, n, floor, views,
														  nviews, 0, 0, keep,
														  closure);

			if (rc < 0)
				break;
			for (uint32_t v = 0; v < nviews; v++)
			{
				PsAdmitCap	ac = {views[v].lsn, views[v].seq,
									views[v].strict_seq};
				int			before = select_ref(cur, n, &ac, 0, 0);
				PsPruneVersion tmp[MAXN];
				uint32_t	ntmp = 0;
				int			after;

				for (uint32_t i = 0; i < n; i++)
					if (keep[i])
						tmp[ntmp++] = cur[i];
				after = select_ref(tmp, ntmp, &ac, 0, 0);
				if (!((before < 0 && after < 0) ||
					  (before >= 0 && after >= 0 &&
					   cur[before].lsn == tmp[after].lsn &&
					   cur[before].admission_seq == tmp[after].admission_seq)))
					violations++;
			}
			for (uint32_t i = 0; i < n; i++)
				if (keep[i])
					next[nnext++] = cur[i];
			memcpy(cur, next, nnext * sizeof(*next));
			n = nnext;
			if (n == 0)
				break;
		}
	}
	{
		char		name[128];

		snprintf(name, sizeof(name),
				 "multi-round prune consistency (seed=%llu, rounds=%u, "
				 "iter=%u, violations=%u)",
				 (unsigned long long) seed, nrounds, niter, violations);
		check(violations == 0, name);
	}
}

int
main(void)
{
	static const uint64_t seeds[] =
	{1, 2, 42, 1337, 424242, 99991, 7, 8675309};
	unsigned char keep[8];
	unsigned char closure[8];

	for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); i++)
	{
		differential_fuzz(seeds[i], 4000);
		consistency_fuzz(seeds[i], 1500);
	}
	for (size_t i = 0; i < 4; i++)
		multiround_fuzz(seeds[i], 5, 500);

	/* --- Named cases (S8.2) --- */
	{
		/* (20,1)/(20,5) under S=3: neither passes S, so the escape decides;
		 * (20,1) is s_min(20), so it is the one closure protects. */
		PsPruneVersion v[] = {{20, 1}, {20, 5}};
		PsViewFence f[] = {{20, 3, PS_PRUNE_SEQ_UNBOUNDED}};
		int rc = ps_page_prune_plan_capped(v, 2, (PsPruneFence) {20, 5}, f, 1,
										   0, 0, keep, closure);

		check(rc >= 0 && keep[0] && closure[0],
			  "(20,1)/(20,5) under S=3: s_min(20,1) is closure-protected");
	}
	{
		/* post-S-only keeps the min: every version at the position is
		 * post-S under the fence; only the min-seq one is admissible (the
		 * escape).  The floor is set above lsn 20 (30) so its own base
		 * (also (20,9), the newest tuple at/below 30) coincides with the
		 * loser here -- it is kept too, for a separate, floor-shaped
		 * reason, not because the fence selected it. */
		PsPruneVersion v[] = {{20, 4}, {20, 6}, {20, 9}};
		PsViewFence f[] = {{20, 3, PS_PRUNE_SEQ_UNBOUNDED}};
		int rc = ps_page_prune_plan_capped(v, 3, (PsPruneFence) {30, 1}, f, 1,
										   0, 0, keep, closure);

		check(rc >= 0 && keep[0] && !keep[1] && keep[2] && closure[0] &&
			  !closure[1],
			  "post-S-only fence escape selects/protects only the "
			  "min-seq version; the floor's own base survives separately");
	}
	{
		/* A fence above the operational floor (P): still selected/kept. */
		PsPruneVersion v[] = {{10, 1}, {30, 1}};
		PsViewFence f[] = {{30, PS_PRUNE_SEQ_UNBOUNDED, PS_PRUNE_SEQ_UNBOUNDED}};
		int rc = ps_page_prune_plan_capped(v, 2, (PsPruneFence) {10, 1}, f, 1,
										   0, 0, keep, closure);

		check(rc == 2 && keep[0] && keep[1], "a fence above P is honoured");
	}
	{
		/* Collectable after release: while a fence names (10,1) as its
		 * exact selection it survives; once that fence is gone (released),
		 * a floor at or above 30 leaves only the newest tuple. */
		PsPruneVersion v[] = {{10, 1}, {30, 1}};
		PsViewFence f[] = {{10, PS_PRUNE_SEQ_UNBOUNDED, PS_PRUNE_SEQ_UNBOUNDED}};
		int rc1 = ps_page_prune_plan_capped(v, 2, (PsPruneFence) {30, 1}, f, 1,
											0, 0, keep, closure);
		int rc2;

		check(rc1 == 2 && keep[0] && keep[1],
			  "collectable case, with the fence still registered: both survive");
		rc2 = ps_page_prune_plan_capped(v, 2, (PsPruneFence) {30, 1}, NULL, 0,
										0, 0, keep, closure);
		check(rc2 == 1 && !keep[0] && keep[1],
			  "collectable after the fence's release: only the floor base "
			  "survives");
	}
	{
		/* An invalidated first arrival still defines s_min (checklist item
		 * 8): at position 20, a finite-S fence's own greatest-admissible
		 * selection picks the higher-seq sibling (2) directly (no escape
		 * needed); closure nonetheless also keeps and marks
		 * closure_protect on the lower-seq sibling (1, s_min(20)), which
		 * neither the floor nor the fence's own selection would otherwise
		 * retain -- it must survive so a *different*, smaller-S view's
		 * escape still has it available, independent of whatever a
		 * forkmeta-level invalidation decision later does to its bytes. */
		PsPruneVersion v[] = {{20, 1}, {20, 2}};
		PsViewFence f[] = {{20, 10, PS_PRUNE_SEQ_UNBOUNDED}};
		int rc = ps_page_prune_plan_capped(v, 2, (PsPruneFence) {20, 2}, f, 1,
										   0, 0, keep, closure);

		check(rc == 2 && keep[0] && keep[1] && closure[0] && !closure[1],
			  "closure keeps and protects s_min(20) beyond the fence's own "
			  "(non-escape) selection of the newer sibling");
	}

	printf("pagestore_prune_capped_test: %d checks, %d failed\n", checks,
		   failed);
	return failed != 0;
}
