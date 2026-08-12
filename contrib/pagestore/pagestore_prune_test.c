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
	PsPruneFence fence[] = {{20, 1}};
	PsPruneFence zero_fence[] = {{20, 0}};
	PsPruneFence later_fence[] = {{25, 1}};
	unsigned char keep[5];

	check(ps_page_prune_plan(NULL, 0, 0, NULL, 0, NULL) == 0, "empty chain");
	check(ps_page_prune_plan(versions, 5, 0, NULL, 0, keep) == -1,
		  "zero floor fails closed without durable cutoff");
	check(ps_page_prune_plan(versions, 5, 25, fence, 1, keep) == 4 &&
		  !keep[0] && keep[1] && keep[2] && keep[3] && keep[4],
		  "an admission fence keeps only its visible same-LSN base");
	check(ps_page_prune_plan(versions, 5, 20, NULL, 0, keep) == 4 &&
		  keep[0] && !keep[1] && keep[2] && keep[3] && keep[4],
		  "operational history collapses redundant same-LSN admissions");
	check(ps_page_prune_plan(versions, 5, 10, NULL, 0, keep) == 4,
		  "floor at oldest version keeps the newest admission per LSN");
	check(ps_page_prune_plan(versions, 5, 50, zero_fence, 1, keep) == 2 &&
		  keep[2] && keep[4],
		  "a zero-sequence fence has uncapped admission visibility");
	check(ps_page_prune_plan(versions, 5, 50, later_fence, 1, keep) == 2 &&
		  keep[1] && keep[4],
		  "a nonzero admission fence constrains versions below its LSN");
	check(ps_page_prune_plan(versions, 5, 50, NULL, 0, keep) == 1 && keep[4],
		  "floor above newest keeps newest base");
	check(ps_page_prune_plan(unsorted, 2, 20, NULL, 0, keep) == -1,
		  "unsorted admission sequence is rejected");
	check(ps_page_prune_plan(NULL, 1, 20, NULL, 0, keep) == -1,
		  "missing input is rejected");

	printf("pagestore_prune_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
