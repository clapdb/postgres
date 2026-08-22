#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "pagestore_forkmeta_prune.h"

static int run;
static int failed;

static void
check(int ok, const char *name)
{
	run++;
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failed++;
	}
}

static int
oracle_visible(const PsForkMetaEvent *event, PsForkMetaFence fence)
{
	if (event->lsn != fence.lsn)
		return event->lsn < fence.lsn;
	if (event->admission_seq == 0)
		return 1;
	if (fence.admission_seq == 0)
		return 1;
	return event->admission_seq <= fence.admission_seq;
}

enum RefState
{
	REF_NONE,
	REF_GROW,
	REF_DEF,
	REF_DEAD
};

typedef struct RefValue
{
	enum RefState state;
	uint32_t nblocks;
} RefValue;

static RefValue
fold(const PsForkMetaEvent *events, uint32_t nitems,
	 const unsigned char *keep, PsForkMetaFence fence)
{
	RefValue result = {REF_NONE, 0};

	for (uint32_t i = 0; i < nitems; i++)
	{
		if (keep != NULL && !keep[i])
			continue;
		if (!oracle_visible(&events[i], fence))
			continue;
		switch (events[i].kind)
		{
			case PS_FORKMETA_GROW:
				if (events[i].nblocks > result.nblocks)
					result.nblocks = events[i].nblocks;
				result.state = (result.state == REF_NONE ||
							result.state == REF_GROW) ? REF_GROW : REF_DEF;
				break;
			case PS_FORKMETA_SET:
				result.state = REF_DEF;
				result.nblocks = events[i].nblocks;
				break;
			case PS_FORKMETA_DEAD:
				result.state = REF_DEAD;
				result.nblocks = 0;
				break;
			default:
				break;
		}
	}
	return result;
}

static int
same_value(RefValue a, RefValue b)
{
	return a.state == b.state && a.nblocks == b.nblocks;
}

static void
check_model(const PsForkMetaEvent *events, uint32_t nitems,
			PsForkMetaFence cutoff, const PsForkMetaFence *fences,
			uint32_t nfences, const unsigned char *keep, const char *name)
{
	unsigned char all[32];
	memset(all, 1, sizeof(all));
	check(same_value(fold(events, nitems, all, cutoff),
					 fold(events, nitems, keep, cutoff)), name);
	for (uint32_t f = 0; f < nfences; f++)
		check(same_value(fold(events, nitems, all, fences[f]),
					 fold(events, nitems, keep, fences[f])), name);
}

static uint32_t
rng_next(uint32_t *state)
{
	*state = *state * 1664525U + 1013904223U;
	return *state;
}

static void
property_test(void)
{
	for (uint32_t trial = 0; trial < 2000; trial++)
	{
		PsForkMetaEvent events[16];
		PsForkMetaFence fences[4];
		unsigned char keep[16];
		uint32_t seed = 0x9e3779b9U ^ trial;
		uint64_t lsn = 1;
		uint64_t cutoff_lsn;
		uint64_t cutoff_seq;
		uint32_t nitems = 1 + rng_next(&seed) % 16;
		uint32_t nfences = rng_next(&seed) % 4;

		for (uint32_t i = 0; i < nitems; i++)
		{
			if (i != 0 && (rng_next(&seed) % 3) != 0)
				lsn += 1 + rng_next(&seed) % 3;
			events[i].lsn = lsn;
			events[i].admission_seq = (rng_next(&seed) % 5 == 0) ? 0 :
				(uint64_t) i + 1;
			events[i].nblocks = rng_next(&seed) % 32;
			events[i].kind = (unsigned char) (rng_next(&seed) % 3);
			if (events[i].kind == PS_FORKMETA_DEAD)
				events[i].nblocks = 0;
		}
		cutoff_lsn = lsn + rng_next(&seed) % 4;
		cutoff_seq = 1 + rng_next(&seed) % 10;
		for (uint32_t i = 0; i < nfences; i++)
		{
			fences[i].lsn = 1 + rng_next(&seed) % cutoff_lsn;
			fences[i].admission_seq = rng_next(&seed) % 10;
			if (fences[i].lsn == cutoff_lsn)
				fences[i].admission_seq = 1 + rng_next(&seed) % cutoff_seq;
		}
		check(ps_forkmeta_prune_plan(events, nitems,
							 (PsForkMetaFence) {cutoff_lsn, cutoff_seq},
							 fences, nfences, keep) >= 0,
				  "reference property input is accepted");
		if (ps_forkmeta_prune_plan(events, nitems,
							(PsForkMetaFence) {cutoff_lsn, cutoff_seq},
							fences, nfences, keep) >= 0)
			check_model(events, nitems,
					(PsForkMetaFence) {cutoff_lsn, cutoff_seq}, fences,
					nfences, keep, "reference model preserves every target fence");
	}
}

int
main(void)
{
	PsForkMetaEvent lifecycle[] = {
		{10, 1, 5, PS_FORKMETA_GROW},
		{20, 2, 8, PS_FORKMETA_SET},
		{20, 3, 12, PS_FORKMETA_GROW},
		{30, 4, 20, PS_FORKMETA_GROW},
		{40, 5, 4, PS_FORKMETA_SET}
	};
	PsForkMetaEvent sparse[] = {
		{10, 1, 2, PS_FORKMETA_GROW},
		{20, 2, 7, PS_FORKMETA_SET},
		{30, 3, 9, PS_FORKMETA_GROW},
		{50, 4, 0, PS_FORKMETA_DEAD},
		{60, 5, 3, PS_FORKMETA_SET}
	};
	PsForkMetaEvent generations[] = {
		{10, 1, 2, PS_FORKMETA_SET},
		{20, 2, 8, PS_FORKMETA_GROW},
		{30, 3, 0, PS_FORKMETA_DEAD},
		{40, 4, 5, PS_FORKMETA_SET},
		{50, 5, 11, PS_FORKMETA_GROW},
		{60, 6, 0, PS_FORKMETA_DEAD},
		{70, 7, 13, PS_FORKMETA_GROW}
	};
	PsForkMetaEvent same_lsn[] = {
		{20, 0, 9, PS_FORKMETA_GROW},
		{20, 2, 3, PS_FORKMETA_SET},
		{20, 3, 8, PS_FORKMETA_GROW},
		{30, 4, 12, PS_FORKMETA_GROW}
	};
	PsForkMetaEvent growth[] = {
		{10, 1, 3, PS_FORKMETA_GROW},
		{20, 2, 8, PS_FORKMETA_GROW},
		{30, 3, 8, PS_FORKMETA_GROW},
		{40, 4, 6, PS_FORKMETA_GROW}
	};
	PsForkMetaEvent bad_lsn[] = {{20, 1, 1, PS_FORKMETA_GROW},
		{10, 2, 2, PS_FORKMETA_GROW}};
	PsForkMetaEvent bad_seq[] = {{20, 3, 1, PS_FORKMETA_GROW},
		{20, 2, 2, PS_FORKMETA_GROW}};
	PsForkMetaEvent legacy_order[] = {{20, 4, 1, PS_FORKMETA_GROW},
		{20, 0, 2, PS_FORKMETA_GROW},
		{20, 5, 3, PS_FORKMETA_GROW}};
	PsForkMetaEvent hidden_seq_regression[] = {
		{20, 2, 1, PS_FORKMETA_GROW},
		{20, 0, 2, PS_FORKMETA_GROW},
		{20, 1, 3, PS_FORKMETA_GROW}
	};
	PsForkMetaEvent dead_then_grow[] = {
		{10, 1, 9, PS_FORKMETA_SET},
		{20, 2, 0, PS_FORKMETA_DEAD},
		{30, 3, 5, PS_FORKMETA_GROW}
	};
	PsForkMetaEvent wildcard_fence[] = {
		{10, 1, 1, PS_FORKMETA_SET},
		{20, 2, 3, PS_FORKMETA_SET},
		{20, 3, 8, PS_FORKMETA_GROW},
		{30, 4, 4, PS_FORKMETA_SET}
	};
	PsForkMetaEvent bad_kind[] = {{20, 1, 1, 9}};
	PsForkMetaEvent bad_dead[] = {{20, 1, 1, PS_FORKMETA_DEAD}};
	unsigned char mask[8];

	check(ps_forkmeta_prune_plan(NULL, 0, (PsForkMetaFence) {1, 1},
							 NULL, 0, NULL) == 0, "empty input");
	check(ps_forkmeta_prune_plan(NULL, 0, (PsForkMetaFence) {0, 0},
							 NULL, 0, NULL) == -1, "zero cutoff LSN rejected");
	check(ps_forkmeta_prune_plan(NULL, 0, (PsForkMetaFence) {1, 0},
							 NULL, 0, NULL) == -1,
				 "zero cutoff sequence rejected for empty input");
	check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 0},
							 NULL, 0, mask) == -1,
				 "zero cutoff sequence rejected for nonempty input");
	check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 4},
							 NULL, 0, mask) == 4 && !mask[0] && mask[1] &&
				 mask[2] && mask[3] && mask[4], "operational base and future tail");
	check(ps_forkmeta_prune_plan(sparse, 5, (PsForkMetaFence) {50, 4},
							 (PsForkMetaFence[]) {{15, 0}, {35, 3}, {15, 0}}, 3,
							 mask) == 5 && mask[0] && mask[1] && mask[2] && mask[3] &&
				 mask[4], "sparse duplicate fences are a union");
	check(ps_forkmeta_prune_plan(generations, 7, (PsForkMetaFence) {60, 6},
							 (PsForkMetaFence[]) {{25, 2}, {45, 4}}, 2, mask) == 5 &&
				 mask[0] && mask[1] && !mask[2] && mask[3] && !mask[4] && mask[5] &&
				 mask[6], "multiple lifecycle generations");
	check(ps_forkmeta_prune_plan(same_lsn, 4, (PsForkMetaFence) {20, 2},
							 NULL, 0, mask) == 3 && !mask[0] && mask[1] && mask[2] &&
				 mask[3], "same-LSN sequence cutoff");
	check(ps_forkmeta_prune_plan(same_lsn, 4, (PsForkMetaFence) {20, 1},
							 (PsForkMetaFence[]) {{20, 1}}, 1, mask) == 4 && mask[0] &&
				 mask[1] && mask[2] && mask[3], "legacy sequence zero is visible");
	check(ps_forkmeta_prune_plan(growth, 4, (PsForkMetaFence) {40, 4},
							 NULL, 0, mask) == 1 && !mask[0] && !mask[1] && mask[2] &&
				 !mask[3], "growth-only maximum uses latest tie");
	check(ps_forkmeta_prune_plan(growth, 4, (PsForkMetaFence) {40, 4},
							 (PsForkMetaFence[]) {{5, 0}}, 1, mask) == 1 && mask[2],
				 "fence with no visible event needs no base");
	check(ps_forkmeta_prune_plan(legacy_order, 3, (PsForkMetaFence) {20, 5},
							 NULL, 0, mask) == 1 && !mask[0] && !mask[1] && mask[2],
				 "legacy order is preserved without false sorting rejection");
	check(ps_forkmeta_prune_plan(hidden_seq_regression, 3,
							 (PsForkMetaFence) {20, 2}, NULL, 0, mask) == -1,
				 "legacy entry cannot hide decreasing same-LSN sequence");
	check(ps_forkmeta_prune_plan(dead_then_grow, 3,
							 (PsForkMetaFence) {30, 3}, NULL, 0, mask) == 2 &&
				 !mask[0] && mask[1] && mask[2] &&
				 fold(dead_then_grow, 3, NULL,
					  (PsForkMetaFence) {30, 3}).state == REF_DEF &&
				 fold(dead_then_grow, 3, NULL,
					  (PsForkMetaFence) {30, 3}).nblocks == 5 &&
				 same_value(fold(dead_then_grow, 3, NULL,
							   (PsForkMetaFence) {30, 3}),
						fold(dead_then_grow, 3, mask,
							 (PsForkMetaFence) {30, 3})),
				 "DEAD followed by GROW becomes definitive and raises size");
	check(ps_forkmeta_prune_plan(wildcard_fence, 4,
							 (PsForkMetaFence) {30, 4},
							 (PsForkMetaFence[]) {{20, 0}}, 1, mask) == 3 &&
				 !mask[0] && mask[1] && mask[2] && mask[3],
				 "lower discrete sequence-zero fence has wildcard visibility");
	check(ps_forkmeta_prune_plan(bad_lsn, 2, (PsForkMetaFence) {30, 1},
							 NULL, 0, mask) == -1, "decreasing LSN rejected");
	check(ps_forkmeta_prune_plan(bad_seq, 2, (PsForkMetaFence) {30, 1},
							 NULL, 0, mask) == -1, "decreasing same-LSN sequence rejected");
	check(ps_forkmeta_prune_plan(bad_kind, 1, (PsForkMetaFence) {30, 1},
							 NULL, 0, mask) == -1, "unknown kind rejected");
	check(ps_forkmeta_prune_plan(bad_dead, 1, (PsForkMetaFence) {30, 1},
							 NULL, 0, mask) == -1, "nonzero DEAD size rejected");
	check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 4},
							 (PsForkMetaFence[]) {{31, 1}}, 1, mask) == -1,
				 "fence above cutoff rejected");
	check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 4},
							 (PsForkMetaFence[]) {{30, 0}}, 1, mask) == -1,
				 "uncapped same-LSN fence above precise cutoff rejected");
	check(ps_forkmeta_prune_plan(NULL, 1, (PsForkMetaFence) {30, 1},
							 NULL, 0, mask) == -1, "missing event array rejected");
	check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 1},
							 NULL, 1, mask) == -1, "missing fence array rejected");
	check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 1},
							 (PsForkMetaFence[]) {{0, 1}}, 1, mask) == -1,
				 "zero fence LSN rejected");
	check(ps_forkmeta_prune_plan(NULL, 0, (PsForkMetaFence) {30, 1},
							 (PsForkMetaFence[]) {{0, 1}}, 1, NULL) == -1,
				 "empty input still validates zero fence LSN");
	check(ps_forkmeta_prune_plan(NULL, 0, (PsForkMetaFence) {30, 1},
							 (PsForkMetaFence[]) {{31, 1}}, 1, NULL) == -1,
				 "empty input still validates fence above cutoff");
#if UINT32_MAX > INT_MAX
	check(ps_forkmeta_prune_plan(lifecycle, (uint32_t) INT_MAX + 1U,
							 (PsForkMetaFence) {30, 1}, NULL, 0, mask) == -1,
				 "item count above INT_MAX rejected before access");
#endif

		/* Exercise the reference model on a hand-written case as well as random
		 * valid streams. */
		check(ps_forkmeta_prune_plan(lifecycle, 5, (PsForkMetaFence) {30, 4},
							 (PsForkMetaFence[]) {{15, 0}, {25, 2}}, 2, mask) >= 0,
				 "reference model setup");
		check_model(lifecycle, 5, (PsForkMetaFence) {30, 4},
						(PsForkMetaFence[]) {{15, 0}, {25, 2}}, 2, mask,
						"hand-written reference model");
		property_test();
	printf("pagestore_forkmeta_prune_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
