/* Focused deterministic tests for the unified page/WAL lag controller. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

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

static void
reset_controller(PsShmHeader *metrics)
{
	/* The previous test's stack header may no longer exist. */
	ps_core_set_metrics_header(NULL);
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
		  "disabled controller configuration is accepted");
	memset(metrics, 0, sizeof(*metrics));
	page_size = 8192;
	segment_size = 1024 * 1024;
	segment_gc_enabled = 1;
	ps_nshards = 1;
	ps_core_set_metrics_header(metrics);
}

static void
test_validation_and_hysteresis(void)
{
	PsShmHeader metrics;

	reset_controller(&metrics);
	check(ps_backpressure_configure(100, 100, 0, 0) != 0,
		  "page catch-up equal to high-water is rejected");
	check(ps_backpressure_configure(100, 101, 0, 0) != 0,
		  "page catch-up above high-water is rejected");
	check(ps_backpressure_configure(0, 1, 0, 0) != 0,
		  "page catch-up without an enabled high-water is rejected");
	check(ps_backpressure_configure(0, 0, 200, 200) != 0,
		  "WAL catch-up equal to high-water is rejected");
	check(ps_backpressure_configure(100, 40, 200, 80) == 0,
		  "independent page and WAL thresholds are accepted");
	ps_test_backpressure_set_lag(99, 199);
	check(metrics.page_backpressure.throttled == 0 &&
		  metrics.wal_backpressure.throttled == 0,
		  "lag below both thresholds does not throttle");
	ps_test_backpressure_set_lag(100, 200);
	check(metrics.page_backpressure.throttled == 1 &&
		  metrics.wal_backpressure.throttled == 1 &&
		  metrics.page_backpressure.throttle_enters == 1 &&
		  metrics.wal_backpressure.throttle_enters == 1,
		  "each category enters at its own high-water");
	ps_test_backpressure_set_lag(70, 100);
	check(metrics.page_backpressure.throttled == 1 &&
		  metrics.wal_backpressure.throttled == 1,
		  "hysteresis keeps both categories throttled above catch-up");
	ps_test_backpressure_set_lag(40, 79);
	check(metrics.page_backpressure.throttled == 0 &&
		  metrics.wal_backpressure.throttled == 0 &&
		  metrics.page_backpressure.throttle_exits == 1 &&
		  metrics.wal_backpressure.throttle_exits == 1,
		  "each category releases at its catch-up target");
	check(metrics.page_backpressure.lag_bytes == 40 &&
		  metrics.page_backpressure.high_water_bytes == 100 &&
		  metrics.page_backpressure.catchup_bytes == 40 &&
		  metrics.wal_backpressure.lag_bytes == 79 &&
		  metrics.wal_backpressure.high_water_bytes == 200 &&
		  metrics.wal_backpressure.catchup_bytes == 80,
		  "metrics expose independent lag and configured thresholds");
	segment_gc_enabled = 0;
	check(ps_backpressure_configure(100, 40, 0, 0) != 0,
		  "page backpressure is rejected when segment GC is disabled");
}

static void
test_nonblocking_admission_and_shutdown(void)
{
	PsShmHeader metrics;
	volatile sig_atomic_t stop = 0;
	uint32_t causes = 0;

	reset_controller(&metrics);
	check(ps_backpressure_configure(100, 20, 0, 0) == 0,
		  "page controller enables for admission test");
	ps_test_backpressure_set_lag(100, 0);
	check(ps_backpressure_try_admit(&stop, &causes) == 0 &&
		  causes == PS_BACKPRESSURE_PAGE,
		  "throttled foreground mutation is deferred without blocking");
	ps_test_backpressure_set_lag(20, 0);
	check(ps_backpressure_try_admit(&stop, &causes) == 1 && causes == 0 &&
		  ps_load_acquire(&metrics.page_backpressure.throttled) == 0,
		  "deferred mutation is admitted after maintenance catch-up");
	ps_backpressure_record_wait(1234, 5678);
	check(ps_load_acquire_u64(&metrics.page_backpressure.foreground_wait_ns) == 1234 &&
		  ps_load_acquire_u64(&metrics.wal_backpressure.foreground_wait_ns) == 5678,
		  "deferred channel wait is charged to each blocking category separately");

	ps_test_backpressure_set_lag(100, 0);
	stop = 1;
	ps_backpressure_shutdown();
	check(ps_backpressure_try_admit(&stop, &causes) == -1,
		  "shutdown cancels a deferred mutation without a stranded waiter");

	ps_core_set_metrics_header(NULL);
}

int
main(void)
{
	test_validation_and_hysteresis();
	test_nonblocking_admission_and_shutdown();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
