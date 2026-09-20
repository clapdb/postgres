#include <stdint.h>
#include <stdio.h>

#include "pagestore_walidx_prune.h"

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
	PsWalIdxPruneItem items[] = {
		{10, 20, 1, 1}, {30, 40, 1, 0}, {50, 60, 1, 1},
		{70, 80, 1, 0}, {90, 100, 1, 1}, {110, 120, 1, 0}
	};
	PsWalIdxPruneItem no_base[] = {{10, 20, 1, 0}, {30, 40, 1, 0}};
	PsWalIdxPruneItem legacy[] = {{10, 0, 0, 0}, {30, 40, 1, 1}};
	PsWalIdxPruneItem bad_fpi[] = {{10, 0, 0, 1}};
	PsWalIdxPruneItem bad_end[] = {{10, 10, 1, 1}};
	PsWalIdxPruneItem unsorted[] = {{30, 40, 1, 1}, {10, 20, 1, 1}};
	PsWalIdxPruneItem unsorted_end[] = {{10, 40, 1, 1}, {30, 35, 1, 1}};
	unsigned char keep[6];

	check(ps_walidx_prune_plan(NULL, 0, 100, NULL, 0, NULL) == 0,
		  "empty page needs no replacement base");
	check(ps_walidx_prune_plan(items, 6, 0, NULL, 0, keep) == -1,
		  "zero cutoff fails closed");
	check(ps_walidx_prune_plan(items, 6, 80, NULL, 0, keep) == 4 &&
		  !keep[0] && !keep[1] && keep[2] && keep[3] && keep[4] && keep[5],
		  "operational cutoff keeps its newest FPI chain and future tail");
	check(ps_walidx_prune_plan(items, 6, 100, (uint64_t[]) {40}, 1,
								 keep) == 4 &&
		  keep[0] && keep[1] && !keep[2] && !keep[3] && keep[4] && keep[5],
		  "an older discrete horizon adds its replacement chain");
	check(ps_walidx_prune_plan(items, 6, 100,
								 (uint64_t[]) {40, 80}, 2, keep) == 6 &&
		  keep[0] && keep[1] && keep[2] && keep[3] && keep[4] && keep[5],
		  "overlapping discrete chains are retained as a union");
	check(ps_walidx_prune_plan(items, 6, 80, (uint64_t[]) {1000}, 1,
								 keep) == 4 && keep[2] && keep[5],
		  "a future horizon is capped at the proven cutoff");
	check(ps_walidx_prune_plan(items, 6, 80, (uint64_t[]) {5}, 1,
								 keep) == 4 && !keep[0],
		  "a horizon before the page existed needs no chain");
	check(ps_walidx_prune_plan(items, 6, 45, NULL, 0, keep) == 6,
		  "a cutoff between records retains the visible chain and future tail");
	check(ps_walidx_prune_plan(items, 6, 15, NULL, 0, keep) == 6,
		  "record visibility is governed by end LSN, not start LSN");
	check(ps_walidx_prune_plan(no_base, 2, 40, NULL, 0, keep) == -1,
		  "a visible delta chain without an FPI fails closed");
	check(ps_walidx_prune_plan(legacy, 2, 40, NULL, 0, keep) == -1,
		  "legacy metadata at the cutoff fails closed");
	check(ps_walidx_prune_plan(legacy, 2, 20, NULL, 0, keep) == -1,
		  "legacy metadata is conservatively unprunable");
	check(ps_walidx_prune_plan(bad_fpi, 1, 20, NULL, 0, keep) == -1,
		  "an unknown FPI marker is rejected");
	check(ps_walidx_prune_plan(bad_end, 1, 20, NULL, 0, keep) == -1,
		  "known metadata requires record end after start");
	check(ps_walidx_prune_plan(unsorted, 2, 40, NULL, 0, keep) == -1,
		  "out-of-order records are rejected");
	check(ps_walidx_prune_plan(unsorted_end, 2, 40, NULL, 0, keep) == -1,
		  "out-of-order record ends are rejected");
	check(ps_walidx_prune_plan(items, 6, 80, NULL, 1, keep) == -1,
		  "missing horizon array is rejected");
	check(ps_walidx_prune_plan(NULL, 1, 80, NULL, 0, keep) == -1,
		  "missing item array is rejected");

	{
		/* Durable replacement page bases supersede the records they cover. */
		uint64_t base60[] = {60};
		uint64_t base80[] = {80};
		uint64_t base20[] = {20};
		uint64_t base_future[] = {100};
		uint64_t unsorted_bases[] = {60, 40};
		uint64_t zero_base[] = {0};
		unsigned char ok1[] = {1};
		unsigned char no1[] = {0};

		check(ps_walidx_prune_plan_bases(items, 6, base60, 1, NULL, 0, 80, 1,
										 NULL, 0, NULL, keep) == 3 &&
			  !keep[0] && !keep[1] && !keep[2] && keep[3] && keep[4] && keep[5],
			  "a stored image covers the FPI and every earlier record");
		check(ps_walidx_prune_plan_bases(items, 6, base80, 1, NULL, 0, 80, 1,
										 NULL, 0, NULL, keep) == 2 &&
			  !keep[3] && keep[4] && keep[5],
			  "an image at the cutoff leaves only the future tail");
		check(ps_walidx_prune_plan_bases(no_base, 2, base20, 1, NULL, 0, 40, 1,
										 NULL, 0, NULL, keep) == 1 &&
			  !keep[0] && keep[1],
			  "a page without any FPI compacts from a stored image");
		check(ps_walidx_prune_plan_bases(items, 6, base_future, 1, NULL, 0, 80,
										 1, NULL, 0, NULL, keep) == 4 && keep[2],
			  "an image after the cutoff is not yet visible");
		check(ps_walidx_prune_plan_bases(items, 6, base60, 1, NULL, 0, 100, 1,
										 (uint64_t[]) {40}, 1, ok1, keep) == 4 &&
			  keep[0] && keep[1] && !keep[2] && !keep[3] && keep[4] && keep[5],
			  "an older horizon below the image still keeps its FPI chain");
		check(ps_walidx_prune_plan_bases(items, 6, base60, 1, NULL, 0, 100, 1,
										 (uint64_t[]) {80}, 1, ok1, keep) == 3 &&
			  !keep[2] && keep[3] && keep[4] && keep[5],
			  "a horizon above the image starts its chain after the image");
		check(ps_walidx_prune_plan_bases(items, 6, base60, 1, NULL, 0, 80, 0,
										 NULL, 0, NULL, keep) == 4 && keep[2],
			  "an unprotected cutoff ignores stored images and keeps its FPI");
		check(ps_walidx_prune_plan_bases(items, 6, base60, 1, NULL, 0, 100, 1,
										 (uint64_t[]) {80}, 1, no1, keep) == 4 &&
			  keep[2] && keep[3] && keep[4] && keep[5],
			  "an unprotected horizon keeps its FPI chain despite the image");
		check(ps_walidx_prune_plan_bases(items, 6, NULL, 0, base60, 1, 80, 0,
										 (uint64_t[]) {80}, 1, no1, keep) == 3 &&
			  !keep[2] && keep[3],
			  "a death base holds even at unprotected horizons");
		check(ps_walidx_prune_plan_bases(items, 6, unsorted_bases, 2, NULL, 0,
										 80, 1, NULL, 0, NULL, keep) == -1,
			  "unsorted bases fail closed");
		check(ps_walidx_prune_plan_bases(items, 6, zero_base, 1, NULL, 0, 80, 1,
										 NULL, 0, NULL, keep) == -1,
			  "a zero base fails closed");
		check(ps_walidx_prune_plan_bases(items, 6, NULL, 0, NULL, 0, 80, 1,
										 NULL, 0, NULL, keep) == 4,
			  "no bases behaves exactly like the FPI-only planner");
	}
	printf("pagestore_walidx_prune_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
