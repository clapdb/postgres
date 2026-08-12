#include <stdint.h>
#include <stdio.h>

#include "pagestore_prune.h"

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

int
main(void)
{
	PsPruneVersion versions[] = {
		{10, 1}, {20, 1}, {20, 2}, {30, 1}, {40, 1}
	};
	PsPruneVersion unsorted[] = {{20, 2}, {20, 1}};
	PsPruneVersion lower_lsn[] = {{10, 1}, {10, 3}, {20, 2}, {20, 3}};
	PsPruneVersion legacy_ties[] = {{10, 0}, {10, 0}, {20, 1}};
	PsPruneFence fence[] = {{20, 1}};
	PsPruneFence zero_fence[] = {{20, 0}};
	PsPruneFence later_fence[] = {{25, 1}};
	unsigned char keep[5];

	check(ps_page_prune_plan(NULL, 0, (PsPruneFence) {0, 0}, NULL, 0, NULL) == 0, "empty chain");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {0, 0}, NULL, 0, keep) == -1,
		  "zero floor fails closed without durable cutoff");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {25, 2}, fence, 1, keep) == 4 &&
		  !keep[0] && keep[1] && keep[2] && keep[3] && keep[4],
		  "an admission fence keeps only its visible same-LSN base");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {20, 1}, NULL, 0, keep) == 4 &&
		  !keep[0] && keep[1] && keep[2] && keep[3] && keep[4],
		  "operational tuple keeps its exact visible same-LSN base");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {20, 2}, NULL, 0, keep) == 3 &&
		  !keep[0] && !keep[1] && keep[2] && keep[3] && keep[4],
		  "operational tuple collapses versions through its admission sequence");
	check(ps_page_prune_plan(lower_lsn, 4, (PsPruneFence) {15, 1}, NULL, 0,
							 keep) == 4 && keep[0] && keep[1] && keep[2] && keep[3],
		  "frontier admission sequence caps lower-LSN base visibility");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {10, 1}, NULL, 0, keep) == 5,
		  "floor at oldest version preserves every tuple above it");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {50, 10}, zero_fence, 1, keep) == 2 &&
		  keep[2] && keep[4],
		  "a zero-sequence fence has uncapped admission visibility");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {50, 10}, later_fence, 1, keep) == 2 &&
		  keep[1] && keep[4],
		  "a nonzero admission fence constrains versions below its LSN");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {50, 10}, NULL, 0, keep) == 1 && keep[4],
		  "floor above newest keeps newest base");
	check(ps_page_prune_plan(legacy_ties, 3, (PsPruneFence) {20, 1}, NULL, 0, keep) == 1 &&
		  !keep[0] && !keep[1] && keep[2],
		  "legacy exact ties use stable source append order");
	check(ps_page_prune_plan(unsorted, 2, (PsPruneFence) {20, 2}, NULL, 0, keep) == -1,
		  "unsorted admission sequence is rejected");
	check(ps_page_prune_plan(NULL, 1, (PsPruneFence) {20, 1}, NULL, 0, keep) == -1,
		  "missing input is rejected");
	check(ps_page_prune_plan(versions, 5, (PsPruneFence) {20, 0}, NULL, 0, keep) == -1,
		  "sequence-zero operational floor fails closed");

	printf("pagestore_prune_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
