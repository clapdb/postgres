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
	unsigned char keep[5];

	check(ps_page_prune_plan(NULL, 0, 0, NULL) == 0, "empty chain");
	check(ps_page_prune_plan(versions, 5, 0, keep) == -1,
		  "zero floor fails closed without durable cutoff");
	check(ps_page_prune_plan(versions, 5, 25, keep) == 4 &&
		  !keep[0] && keep[1] && keep[2] && keep[3] && keep[4],
		  "floor between versions keeps every base admission variant");
	check(ps_page_prune_plan(versions, 5, 20, keep) == 5 &&
		  keep[0] && keep[1] && keep[2] && keep[3] && keep[4],
		  "exact floor keeps older base and every same-LSN admission variant");
	check(ps_page_prune_plan(versions, 5, 10, keep) == 5,
		  "floor at oldest version keeps complete retained history");
	check(ps_page_prune_plan(versions, 5, 50, keep) == 1 && keep[4],
		  "floor above newest keeps newest base");
	check(ps_page_prune_plan(unsorted, 2, 20, keep) == -1,
		  "unsorted admission sequence is rejected");
	check(ps_page_prune_plan(NULL, 1, 20, keep) == -1,
		  "missing input is rejected");

	printf("pagestore_prune_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
