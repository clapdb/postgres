/* Focused deterministic tests for the unified page/WAL lag controller. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_core.h"

static int checks;
static int failed;

static void
remove_tree(const char *path)
{
	char command[512];

	if (snprintf(command, sizeof(command), "rm -rf -- '%s'", path) > 0)
		(void) system(command);
}

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

typedef struct AdmissionFairnessState
{
	volatile int reader1_ready;
	volatile int release_reader1;
	volatile int writer_queued;
	volatile int writer_acquired;
	volatile int release_writer;
	volatile int reader2_acquired;
} AdmissionFairnessState;

static void
wait_flag(volatile int *flag)
{
	for (int i = 0; i < 2000 && !__atomic_load_n(flag, __ATOMIC_ACQUIRE); i++)
		usleep(1000);
}

static void
admission_writer_queued(void *arg)
{
	AdmissionFairnessState *state = arg;

	__atomic_store_n(&state->writer_queued, 1, __ATOMIC_RELEASE);
}

static void *
admission_reader_one(void *arg)
{
	AdmissionFairnessState *state = arg;

	ps_admission_read_lock();
	__atomic_store_n(&state->reader1_ready, 1, __ATOMIC_RELEASE);
	while (!__atomic_load_n(&state->release_reader1, __ATOMIC_ACQUIRE))
		sched_yield();
	ps_admission_read_unlock();
	return NULL;
}

static void *
admission_writer(void *arg)
{
	AdmissionFairnessState *state = arg;

	if (ps_admission_write_lock() != 0)
		return NULL;
	__atomic_store_n(&state->writer_acquired, 1, __ATOMIC_RELEASE);
	while (!__atomic_load_n(&state->release_writer, __ATOMIC_ACQUIRE))
		sched_yield();
	ps_admission_write_unlock();
	return NULL;
}

static void *
admission_reader_two(void *arg)
{
	AdmissionFairnessState *state = arg;

	ps_admission_read_lock();
	__atomic_store_n(&state->reader2_acquired, 1, __ATOMIC_RELEASE);
	ps_admission_read_unlock();
	return NULL;
}

static void
test_admission_writer_preference(void)
{
	AdmissionFairnessState state;
	pthread_t reader1;
	pthread_t writer;
	pthread_t reader2;
	int have_reader1 = 0;
	int have_writer = 0;
	int have_reader2 = 0;
	int rc;

	memset(&state, 0, sizeof(state));
	ps_test_set_admission_write_queued_hook(admission_writer_queued, &state);
	rc = pthread_create(&reader1, NULL, admission_reader_one, &state);
	have_reader1 = rc == 0;
	check(rc == 0,
		  "start the admission fairness reader");
	wait_flag(&state.reader1_ready);
	check(__atomic_load_n(&state.reader1_ready, __ATOMIC_ACQUIRE) != 0,
		  "first admission reader holds the lock");
	rc = pthread_create(&writer, NULL, admission_writer, &state);
	have_writer = rc == 0;
	check(rc == 0,
		  "queue an admission writer behind the reader");
	wait_flag(&state.writer_queued);
	check(__atomic_load_n(&state.writer_queued, __ATOMIC_ACQUIRE) != 0,
		  "admission writer is recorded at the turnstile");
	rc = pthread_create(&reader2, NULL, admission_reader_two, &state);
	have_reader2 = rc == 0;
	check(rc == 0,
		  "start a reader after the writer queues");
	usleep(20000);
	check(__atomic_load_n(&state.reader2_acquired, __ATOMIC_ACQUIRE) == 0,
		  "new admission reader cannot pass a queued writer");
	__atomic_store_n(&state.release_reader1, 1, __ATOMIC_RELEASE);
	wait_flag(&state.writer_acquired);
	check(__atomic_load_n(&state.writer_acquired, __ATOMIC_ACQUIRE) != 0,
		  "queued admission writer acquires after the old reader releases");
	check(__atomic_load_n(&state.reader2_acquired, __ATOMIC_ACQUIRE) == 0,
		  "new admission reader waits while writer is active");
	__atomic_store_n(&state.release_writer, 1, __ATOMIC_RELEASE);
	wait_flag(&state.reader2_acquired);
	check(__atomic_load_n(&state.reader2_acquired, __ATOMIC_ACQUIRE) != 0,
		  "new admission reader acquires after the writer releases");
	if (have_reader1)
		pthread_join(reader1, NULL);
	if (have_writer)
		pthread_join(writer, NULL);
	if (have_reader2)
		pthread_join(reader2, NULL);
	ps_test_set_admission_write_queued_hook(NULL, NULL);
}

static void
fill_page(unsigned char *page, unsigned char tag)
{
	uint64_t lsn = 1000 + tag;
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;

	memset(page, tag, page_size);
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
}

static void
configure_page_core(void)
{
	page_size = 8192;
	segment_size = 32768;
	flush_pages = 1;
	compact_layers = 1000000;
	segment_gc_enabled = 1;
	cache_pages = 0;
	use_layers = 1;
	ps_nshards = 1;
	ps_storage = &PsStoragePosix;
}

static int
make_page_debt_store(char *store)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};
	unsigned char page[8192];

	if (mkdtemp(store) == NULL || ps_core_open(store) != 0)
		return 0;
	for (uint32_t block = 0; block < 5; block++)
	{
		fill_page(page, (unsigned char) (10 + block));
		ps_lock_shard_wr(0);
		if (append_page(0, &key, block, page, 0, NULL) != 0)
		{
			ps_unlock_shard(0);
			return 0;
		}
		ps_unlock_shard(0);
	}
	return ps_storage->sync() == 0;
}

static int64_t
fail_segment_size(uint32_t shard, int seg)
{
	(void) shard;
	(void) seg;
	errno = EIO;
	return -1;
}

static int segment_size_calls;

static int64_t
counting_segment_size(uint32_t shard, int seg)
{
	segment_size_calls++;
	return PsStoragePosix.seg_size(shard, seg);
}

/* Simulate SPDK's EOF sentinel: a missing segment is reported as -1 without
 * setting errno.  The generic recovery scan must accept this contract. */
static int64_t
sentinel_segment_size(uint32_t shard, int seg)
{
	int64_t bytes;

	errno = 0;
	bytes = PsStoragePosix.seg_size(shard, seg);
	if (bytes < 0)
		errno = 0;
	return bytes;
}

static void
test_page_storage_fail_closed(void)
{
	char store[] = "/tmp/pagestore-page-backpressure-eio-XXXXXX";
	PsShmHeader metrics;
	PsStorage failing_storage;

	configure_page_core();
	check(ps_backpressure_configure(32768, 1, 0, 0) == 0,
		  "page controller is configured before standalone core open");
	if (!make_page_debt_store(store))
	{
		check(0, "construct covered page segment for storage-error tests");
		remove_tree(store);
		return;
	}
	check(1,
		  "construct covered page segment for storage-error tests");
	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	check(ps_backpressure_configure(32768, 1, 0, 0) == 0 &&
		  (ps_backpressure_refresh(),
		   metrics.page_backpressure.throttled != 0 &&
		   metrics.page_backpressure.lag_bytes >= 32768),
		  "positive covered page segment enters backpressure");
	failing_storage = PsStoragePosix;
	failing_storage.seg_size = fail_segment_size;
	ps_storage = &failing_storage;
	(void) ps_core_maintenance();
	ps_backpressure_refresh();
	check(metrics.page_backpressure.throttled != 0 &&
		  metrics.page_backpressure.lag_bytes == UINT64_MAX,
		  "runtime non-ENOENT segment-size failure keeps page throttle fail closed");
	ps_storage = &PsStoragePosix;
	ps_backpressure_configure(0, 0, 0, 0);
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();

	{
		PsStorage sentinel_storage = PsStoragePosix;

		sentinel_storage.seg_size = sentinel_segment_size;
		ps_storage = &sentinel_storage;
		ps_backpressure_configure(0, 0, 0, 0);
		check(ps_core_open(store) == 0,
			  "generic recovery accepts a backend EOF sentinel without errno");
		ps_core_close();
		ps_storage->close();
	}
	{
		PsStorage counting_storage = PsStoragePosix;
		int disabled_calls;
		int enabled_calls;

		counting_storage.seg_size = counting_segment_size;
		ps_storage = &counting_storage;
		ps_backpressure_configure(0, 0, 0, 0);
		segment_size_calls = 0;
		check(ps_core_open(store) == 0,
			  "disabled page controller reopens the standalone store");
		disabled_calls = segment_size_calls;
		ps_core_close();
		ps_storage->close();

		ps_backpressure_configure(32768, 1, 0, 0);
		segment_size_calls = 0;
		check(ps_core_open(store) == 0,
			  "enabled page controller reopens the standalone store");
		enabled_calls = segment_size_calls;
		check(enabled_calls > disabled_calls,
			  "disabled page controller avoids the startup PAGE debt scan");
		ps_core_close();
		ps_storage->close();
	}
	{
		pid_t pid = fork();
		int status = 0;

		if (pid == 0)
		{
			int rc;

			setenv("PAGESTORE_TEST_FAIL_SEG_SIZE", "1", 1);
			configure_page_core();
			(void) ps_backpressure_configure(32768, 1, 0, 0);
			errno = 0;
			rc = ps_core_open(store);
			_exit(rc != 0 && errno == EIO ? 0 : 1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 0,
			  "enabled page controller fails startup on non-ENOENT segment-size error");
	}
	remove_tree(store);
}

int
main(void)
{
	test_validation_and_hysteresis();
	test_nonblocking_admission_and_shutdown();
	test_admission_writer_preference();
	test_page_storage_fail_closed();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
