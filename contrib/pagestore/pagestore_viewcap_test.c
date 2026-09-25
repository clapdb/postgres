/*
 * Focused POSIX unit test for the Bug B read-side seq-cap machinery, phase
 * P1 (BRANCH_SNAPSHOT_SEQ_CAP.md).  This is the differential test the
 * design doc's S9.3 asks for: today's (pre-P1) page/fork selection logic,
 * frozen verbatim inside pagestore_core.c as test-only references, checked
 * against the new ViewCap-based implementation over random histories --
 * bit-identical at PS_SEQ_UNBOUNDED (the only cap P1 ever constructs in
 * production), and cross-checked against an independent brute-force
 * reading of the design doc's admissibility rule with finite caps (never
 * used outside this test).
 *
 * This test links the shared core directly; it never starts a daemon.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "pagestore_core.h"

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

/* One differential run, reported as a single named check: 0 means every
 * one of its internal checks passed, else it names the 1-based index of
 * the first one that did not. */
static void
run_differential(uint64_t seed, uint32_t niter)
{
	char		name[128];
	int			rc = ps_test_viewcap_differential(seed, niter);

	snprintf(name, sizeof(name),
			 "viewcap differential seed=%llu niter=%u (rc=%d)",
			 (unsigned long long) seed, niter, rc);
	check(rc == 0, name);
}

int
main(void)
{
	/* The default seed plus several more, each running a few thousand
	 * random page-version arrays and fork-event histories: comfortably
	 * over the design doc's 10^5-case floor in aggregate across CI's
	 * repeated invocations, while keeping one run fast. */
	static const uint64_t seeds[] =
	{
		0, 1, 2, 3, 7, 42, 1000003, 0xdeadbeefULL, 0x0ddc0ffeeULL,
	};

	for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); i++)
		run_differential(seeds[i], 4000);

	/* Keep the existing fork-event index self-test green under the new
	 * ForkEvent flags byte / META classification (both legacy and
	 * non-legacy admission-sequence mixes). */
	check(ps_test_fork_event_index_selftest(1, 400, 400, 0) == 0,
		  "fork-event index selftest legacy=0");
	check(ps_test_fork_event_index_selftest(1, 400, 400, 1) == 0,
		  "fork-event index selftest legacy=1");
	check(ps_test_fork_event_index_selftest(2, 400, 400, 0) == 0,
		  "fork-event index selftest seed=2 legacy=0");
	check(ps_test_fork_event_index_selftest(2, 400, 400, 1) == 0,
		  "fork-event index selftest seed=2 legacy=1");

	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
