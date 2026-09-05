/* Focused deterministic tests for the unified page/WAL lag controller. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_forkmeta_snapshot.h"
#include "pagestore_retention.h"

static int checks;
static int failed;

static void configure_page_core(void);
static void fill_page(unsigned char *page, unsigned char tag);
static void fill_page_lsn(unsigned char *page, unsigned char tag, uint64_t lsn);
static int append_test_walidx_tail(uint32_t timeline, uint64_t progress);
static int append_test_walidx_identity(uint32_t timeline);
static int test_path_suffix(char *path, size_t path_size, const char *base,
						const char *suffix);

typedef struct WalIdxObservationRetryTest
{
	char manifest[1024];
	char saved_manifest[1024];
	int calls;
	int repaired;
} WalIdxObservationRetryTest;

static void
walidx_observation_error_repair(uint32_t timeline, void *arg)
{
	WalIdxObservationRetryTest *test = arg;

	test->calls++;
	if (test->calls != 1 ||
		rename(test->saved_manifest, test->manifest) != 0 ||
		!append_test_walidx_identity(timeline))
		return;
	test->repaired = 1;
}

typedef struct BackpressureSlowPathCounter
{
	unsigned int calls;
} BackpressureSlowPathCounter;

typedef struct ForkmetaProofRace
{
	PsChannel channel;
	PsKey key;
	pthread_t worker;
	unsigned int calls;
	volatile int started;
	volatile int go;
	volatile int attempting_read_lock;
	volatile int hook_returned;
	volatile int acquired;
	volatile int acquired_before_hook_returned;
	int worker_created;
	int failed;
} ForkmetaProofRace;

static void *
forkmeta_proof_race_worker(void *arg)
{
	ForkmetaProofRace *race = arg;

	__atomic_store_n(&race->started, 1, __ATOMIC_RELEASE);
	while (!__atomic_load_n(&race->go, __ATOMIC_ACQUIRE))
		sched_yield();
	/* This is the final handshake before entering the admission fence.  The
	 * observation hook waits for it before it is allowed to return. */
	__atomic_store_n(&race->attempting_read_lock, 1, __ATOMIC_RELEASE);
	ps_admission_read_lock();
	if (!__atomic_load_n(&race->hook_returned, __ATOMIC_ACQUIRE))
		__atomic_store_n(&race->acquired_before_hook_returned, 1,
						 __ATOMIC_RELEASE);
	__atomic_store_n(&race->acquired, 1, __ATOMIC_RELEASE);
	memset(&race->channel, 0, sizeof(race->channel));
	race->channel.timeline = 1;
	race->channel.opcode = PS_OP_CREATE;
	race->channel.key = race->key;
	race->channel.req_lsn = 1050;
	if (ps_handle_meta(&race->channel) != 1 ||
		race->channel.status != PS_STATUS_OK)
		race->failed = 1;
	ps_admission_read_unlock();
	return NULL;
}

static void
forkmeta_proof_race(unsigned int attempt, void *arg)
{
	ForkmetaProofRace *race = arg;

	/* Mutate only the first observation.  A source identity change should make
	 * the observer retry, while the post-scan owner proof must still reject the
	 * stale source-debt authorization. */
	if (attempt != 0 || race->calls != 0)
		return;
	race->calls++;
	if (pthread_create(&race->worker, NULL, forkmeta_proof_race_worker,
					   race) != 0)
	{
		race->failed = 1;
		return;
	}
	race->worker_created = 1;
	while (!__atomic_load_n(&race->started, __ATOMIC_ACQUIRE))
		sched_yield();
	__atomic_store_n(&race->go, 1, __ATOMIC_RELEASE);
	/* Do not use a timeout as evidence of blocking.  Wait until the worker has
	 * reached the instruction immediately before admission-rd; if the
	 * observation were unfenced, that worker is then allowed to proceed and the
	 * post-join assertion would catch an early acquisition. */
	while (!__atomic_load_n(&race->attempting_read_lock, __ATOMIC_ACQUIRE))
		sched_yield();
	__atomic_store_n(&race->hook_returned, 1, __ATOMIC_RELEASE);
}

static void
count_backpressure_slow_path(void *arg)
{
	BackpressureSlowPathCounter *counter = arg;

	counter->calls++;
}

static void
remove_forkmeta_source(void *arg)
{
	(void) unlink((const char *) arg);
}

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

typedef struct TestWalIdxWatermark
{
	uint64_t magic;
	uint64_t length;
	uint32_t crc;
	uint32_t reserved;
} TestWalIdxWatermark;

static uint32_t
test_walidx_watermark_crc(TestWalIdxWatermark *watermark)
{
	unsigned char *bytes = (unsigned char *) watermark;
	uint32_t saved = watermark->crc;
	uint32_t hash = 2166136261u;

	watermark->crc = 0;
	for (size_t i = 0; i < sizeof(*watermark); i++)
	{
		hash ^= bytes[i];
		hash *= 16777619u;
	}
	watermark->crc = saved;
	return hash;
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
	check(ps_backpressure_configure_all(100, 40, 200, 80, 300, 120) == 0,
		  "the independent WAL-index thresholds are accepted");
	check(ps_backpressure_configure_all(0, 0, 0, 0, 300, 301) != 0 &&
		  ps_backpressure_configure_all(0, 0, 0, 0, 0, 1) != 0,
		  "WAL-index catch-up validation rejects invalid configurations");
	ps_test_backpressure_set_walidx_lag(299);
	check(metrics.walidx_backpressure.throttled == 0,
		  "WAL-index lag below high-water does not throttle");
	ps_test_backpressure_set_walidx_lag(300);
	check(metrics.walidx_backpressure.throttled == 1 &&
		  metrics.walidx_backpressure.lag_bytes == 300,
		  "WAL-index controller enters independently");
	ps_test_backpressure_set_walidx_lag(120);
	check(metrics.walidx_backpressure.throttled == 0 &&
		  metrics.walidx_backpressure.throttle_exits == 1,
		  "WAL-index controller releases at its catch-up target");
	check(ps_backpressure_configure_all_with_forkmeta(0, 0, 0, 0, 0, 0,
										 300, 120) == 0,
		  "independent forkmeta thresholds are accepted");
	ps_test_backpressure_set_forkmeta_lag(299);
	check(metrics.forkmeta_backpressure.throttled == 0,
		  "forkmeta lag below high-water does not throttle");
	ps_test_backpressure_set_forkmeta_lag(300);
	check(metrics.forkmeta_backpressure.throttled == 1 &&
		  metrics.forkmeta_backpressure.lag_bytes == 300,
		  "forkmeta controller enters independently");
	ps_test_backpressure_set_forkmeta_lag(120);
	check(metrics.forkmeta_backpressure.throttled == 0 &&
		  metrics.forkmeta_backpressure.throttle_exits == 1,
		  "forkmeta controller releases at catch-up");
	check(ps_backpressure_configure(100, 40, 200, 80) == 0,
		  "restore page/WAL controller configuration for shared checks");
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
	BackpressureSlowPathCounter slow_path = {0};
	volatile sig_atomic_t stop = 0;
	uint32_t causes = 0;

	reset_controller(&metrics);
	ps_test_set_backpressure_slow_path_hook(count_backpressure_slow_path,
										&slow_path);
	check(ps_backpressure_try_admit(&stop, &causes) == 1 &&
		  slow_path.calls == 0,
		  "disabled controller admits without entering its slow path");
	ps_backpressure_shutdown();
	check(ps_backpressure_try_admit(&stop, &causes) == -1 &&
		  slow_path.calls == 0,
		  "disabled controller still observes shutdown without its slow path");
	check(ps_backpressure_configure(100, 20, 0, 0) == 0,
		  "page controller enables for admission test");
	check(ps_backpressure_try_admit(&stop, &causes) == 1 &&
		  slow_path.calls == 0,
		  "enabled but unthrottled controller keeps the lock-free path");
	ps_test_backpressure_set_lag(100, 0);
	check(ps_backpressure_configure_all(0, 0, 0, 0, 100, 20) == 0,
		  "WAL-index controller enables for cause accounting");
	ps_test_backpressure_set_walidx_lag(100);
	check(ps_backpressure_try_admit(&stop, &causes) == 0 &&
		  causes == PS_BACKPRESSURE_WALIDX,
		  "WAL-index throttle reports only its own admission cause");
	ps_backpressure_record_wait3(0, 0, 4321);
	check(ps_load_acquire_u64(&metrics.walidx_backpressure.foreground_wait_ns) == 4321,
		  "WAL-index deferred wait is charged independently");
	check(ps_backpressure_configure(100, 20, 0, 0) == 0,
		  "restore page controller for shutdown admission test");
	slow_path.calls = 0;
	ps_test_backpressure_set_lag(100, 0);
	check(ps_backpressure_try_admit(&stop, &causes) == 0 &&
		  causes == PS_BACKPRESSURE_PAGE && slow_path.calls == 1,
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

	ps_test_set_backpressure_slow_path_hook(NULL, NULL);
	ps_core_set_metrics_header(NULL);
}

static void
test_walidx_append_tail_restart(void)
{
	char store[] = "/tmp/pagestore-walidx-backpressure-XXXXXX";
	PsShmHeader metrics;
	PsChannel channel;

	configure_page_core();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_backpressure_configure_all(0, 0, 0, 0, 1, 0) == 0,
		  "configure WAL-index backpressure for append-tail coverage");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open a store for WAL-index append-tail coverage");
	ps_core_set_metrics_header(&metrics);
	memset(&channel, 0, sizeof(channel));
	channel.timeline = 0;
	ps_backpressure_refresh();
	check(metrics.walidx_backpressure.lag_bytes == 0 &&
		  metrics.walidx_backpressure.throttled == 0,
		  "legacy epoch-zero lazy absence is clean for an untouched shard");
	channel.key = (PsKey) {1, 1, 1, 0, PS_KLASS_RELATION};
	channel.opcode = PS_OP_WAL_APPEND;
	channel.datalen = 128;
	memset(channel.data, 0xa5, channel.datalen);
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
		  "append shipped WAL backing the WAL-index frontier");
	memset(&channel, 0, sizeof(channel));
	channel.timeline = 0;
	channel.opcode = PS_OP_WAL_INDEX_ADD;
	channel.blocknum = 0;
	channel.req_lsn = 100;
	channel.key = (PsKey) {1, 1, 1, 0, PS_KLASS_RELATION};
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
		  "append one durable WAL-index record");
	memset(&channel, 0, sizeof(channel));
	channel.timeline = 0;
	channel.opcode = PS_OP_WAL_INDEX_PROGRESS;
	channel.req_lsn = 0;
	channel.req_seq = 128;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
		  "publish the durable WAL-index frontier");
	ps_backpressure_refresh();
	check(metrics.walidx_backpressure.lag_bytes >= sizeof(uint64_t) * 8 &&
		  metrics.walidx_backpressure.throttled != 0,
		  "WAL-index append tail enters the independent controller");
	{
		char log_path[2048];
		char saved_path[2048];

		snprintf(log_path, sizeof(log_path), "%s/walidx_0_0", store);
		check(snprintf(saved_path, sizeof(saved_path), "%s/walidx_0_0.saved", store) >= 0,
			  "build the saved WAL-index path");
		check(rename(log_path, saved_path) == 0,
			  "locate the active legacy epoch-zero WAL-index file");
		ps_backpressure_refresh();
		check(metrics.walidx_backpressure.lag_bytes == UINT64_MAX &&
			  metrics.walidx_backpressure.throttled != 0,
			  "missing current WAL-index file fails closed");
		check(rename(saved_path, log_path) == 0,
			  "restore the active legacy WAL-index file");
		ps_backpressure_refresh();
	}
	{
		char obsolete_log[2048];
		char obsolete_marker[2048];
		unsigned char obsolete_data[7] = {0, 1, 2, 3, 4, 5, 6};
		TestWalIdxWatermark watermark;
		uint64_t baseline;
		int fd;
		int created = 0;

		/* The recorded length is deliberately different from the physical
		 * log size: obsolete debt must charge log bytes plus marker bytes. */
		snprintf(obsolete_log, sizeof(obsolete_log),
				 "%s/walidx_0_0_e%020llu", store, 1ULL);
		check(test_path_suffix(obsolete_marker, sizeof(obsolete_marker),
						   obsolete_log, ".size") == 0,
			  "build the obsolete watermark path");
		memset(&watermark, 0, sizeof(watermark));
		watermark.magic = UINT64_C(0x31524b4d58444957);
		watermark.length = 123;
		watermark.crc = test_walidx_watermark_crc(&watermark);
		baseline = metrics.walidx_backpressure.lag_bytes;
		fd = open(obsolete_log, O_CREAT | O_EXCL | O_WRONLY, 0600);
		if (fd >= 0 && write(fd, obsolete_data, sizeof(obsolete_data)) ==
			(ssize_t) sizeof(obsolete_data) && fsync(fd) == 0 && close(fd) == 0)
			created = 1;
		else if (fd >= 0)
			close(fd);
		fd = open(obsolete_marker, O_CREAT | O_EXCL | O_WRONLY, 0600);
		if (created && fd >= 0 && write(fd, &watermark, sizeof(watermark)) ==
			(ssize_t) sizeof(watermark) && fsync(fd) == 0 && close(fd) == 0)
			created = 1;
		else
		{
			created = 0;
			if (fd >= 0)
				close(fd);
		}
		check(created, "create a physical obsolete epoch and watermark");
		if (created)
		{
			ps_backpressure_refresh();
			check(metrics.walidx_backpressure.lag_bytes == baseline +
				  sizeof(obsolete_data) + sizeof(watermark),
				  "obsolete debt charges exact physical epoch plus marker bytes");
			check(unlink(obsolete_marker) == 0 && unlink(obsolete_log) == 0,
				  "remove the synthetic obsolete epoch artifacts");
			ps_backpressure_refresh();
		}
		else
		{
			(void) unlink(obsolete_marker);
			(void) unlink(obsolete_log);
		}
	}
	{
		char current_log[2048];
		char current_marker[2048];
		char watermark_temp[4096];
		char malformed_temp[4096];
		int fd;

		snprintf(current_log, sizeof(current_log),
				 "%s/walidx_0_0_e%020llu", store, 1ULL);
		check(test_path_suffix(current_marker, sizeof(current_marker),
						   current_log, ".size") == 0,
			  "build the current watermark path");
		check(snprintf(watermark_temp, sizeof(watermark_temp),
						 "%s.tmp.%ld.%u", current_marker, (long) getpid(), 0U) >= 0,
			  "build the watermark residue path");
		fd = open(watermark_temp, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(fd >= 0 && write(fd, "watermark", 9) == 9 &&
				fsync(fd) == 0 && close(fd) == 0,
				"create canonical watermark publication residue");
		ps_backpressure_refresh();
		check(metrics.walidx_backpressure.lag_bytes >= 9 &&
				metrics.walidx_backpressure.throttled != 0,
				"watermark publication residue is counted as WAL-index debt");
		check(ps_test_walidx_gc_force_due(0) != 0,
				"watermark publication residue schedules epoch GC");
		check(ps_core_maintenance() == 1 && access(watermark_temp, F_OK) != 0,
				"epoch GC removes canonical watermark residue");
		for (int i = 0; i < 8 && metrics.walidx_backpressure.throttled != 0; i++)
		{
			(void) ps_core_maintenance();
			ps_backpressure_refresh();
		}
		check(metrics.walidx_backpressure.lag_bytes == 0 &&
				metrics.walidx_backpressure.throttled == 0,
				"watermark residue cleanup releases WAL-index debt");
		{
			uint64_t keep_epochs[1] = {1};
			int gc_symlink_ok;
			int gc_directory_ok;

			check(snprintf(watermark_temp, sizeof(watermark_temp),
						 "%s.tmp.%ld.%u", current_marker, (long) getpid(), 2U) >= 0,
				  "build the symlink watermark path");
			gc_symlink_ok = symlink(current_marker, watermark_temp) == 0;
			check(gc_symlink_ok && ps_storage->walidx_epoch_gc(0, keep_epochs, 1) < 0 &&
					access(watermark_temp, F_OK) == 0,
					"epoch GC rejects exact watermark temporary symlink and preserves it");
			unlink(watermark_temp);

			gc_directory_ok = mkdir(watermark_temp, 0700) == 0;
			check(gc_directory_ok && ps_storage->walidx_epoch_gc(0, keep_epochs, 1) < 0 &&
					access(watermark_temp, F_OK) == 0,
					"epoch GC rejects exact watermark temporary directory and preserves it");
			rmdir(watermark_temp);
		}

		check(snprintf(malformed_temp, sizeof(malformed_temp),
					 "%s.tmp.%ld.%u", current_marker, (long) getpid(), 128U) >= 0,
			  "build the malformed watermark path");
		fd = open(malformed_temp, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(fd >= 0 && close(fd) == 0,
				"create a near-miss watermark temporary name");
		ps_backpressure_refresh();
		check(metrics.walidx_backpressure.lag_bytes == UINT64_MAX &&
				ps_core_maintenance() != 1 && access(malformed_temp, F_OK) == 0,
				"near-miss watermark residue fails closed and is not deleted");
		unlink(malformed_temp);
	}
	for (int i = 0; i < 16 && metrics.walidx_backpressure.throttled != 0; i++)
	{
		(void) ps_core_maintenance();
		ps_backpressure_refresh();
	}
	check(metrics.walidx_backpressure.lag_bytes == 0 &&
		  metrics.walidx_backpressure.throttled == 0,
		  "below-trigger WAL-index debt is snapshotted, GC'd, and released");
	{
		char current_log[2048];
		char current_marker[2048];
		char saved_path[2048];
		TestWalIdxWatermark original;
		int fd;
		int watermark_ok = 0;

		/* Publication rotates to epoch one.  Its empty log still has a
		 * required CRC-protected active watermark. */
		snprintf(current_log, sizeof(current_log),
				 "%s/walidx_0_0_e%020llu", store, 1ULL);
		check(test_path_suffix(current_marker, sizeof(current_marker),
						   current_log, ".size") == 0 &&
			  test_path_suffix(saved_path, sizeof(saved_path), current_marker,
							 ".saved") == 0,
			  "build the selected watermark paths");
		fd = open(current_marker, O_RDONLY);
		if (fd >= 0 && read(fd, &original, sizeof(original)) ==
			(ssize_t) sizeof(original) && close(fd) == 0)
			watermark_ok = 1;
		fd = -1;
		check(access(current_log, F_OK) == 0 && watermark_ok,
			  "snapshot publication leaves the selected epoch watermark");
		if (access(current_log, F_OK) == 0 && access(current_marker, F_OK) == 0)
		{
			check(rename(current_log, saved_path) == 0,
				  "rename the selected epoch file for a presence fault");
			ps_backpressure_refresh();
			check(metrics.walidx_backpressure.lag_bytes == UINT64_MAX,
				  "missing selected epoch file fails closed");
			check(rename(saved_path, current_log) == 0,
				  "restore the selected epoch file");

			check(rename(current_marker, saved_path) == 0,
				  "rename the selected active watermark for a presence fault");
			ps_backpressure_refresh();
			check(metrics.walidx_backpressure.lag_bytes == UINT64_MAX,
				  "missing selected active watermark fails closed");
			check(rename(saved_path, current_marker) == 0,
				  "restore the selected active watermark");

			fd = open(current_marker, O_WRONLY);
			check(fd >= 0 && pwrite(fd, "x", 1, 0) == 1 && fsync(fd) == 0 &&
				  close(fd) == 0,
				  "corrupt the selected active watermark");
			ps_backpressure_refresh();
			check(metrics.walidx_backpressure.lag_bytes == UINT64_MAX,
				  "corrupt selected active watermark fails closed");
			fd = open(current_marker, O_WRONLY);
			check(fd >= 0 && pwrite(fd, &original, sizeof(original), 0) ==
				  (ssize_t) sizeof(original) && fsync(fd) == 0 && close(fd) == 0,
				  "restore the selected active watermark after corruption");

			/* A validly encoded but mismatched recorded length is unsafe too. */
		original.length++;
		original.crc = test_walidx_watermark_crc(&original);
		fd = open(current_marker, O_WRONLY);
		check(fd >= 0 && pwrite(fd, &original, sizeof(original), 0) ==
				  (ssize_t) sizeof(original) && fsync(fd) == 0 && close(fd) == 0,
				  "write a mismatched selected watermark length");
		ps_backpressure_refresh();
		check(metrics.walidx_backpressure.lag_bytes == UINT64_MAX,
			  "mismatched selected active watermark fails closed");
		original.length--;
		original.crc = test_walidx_watermark_crc(&original);
		fd = open(current_marker, O_WRONLY);
		check(fd >= 0 && pwrite(fd, &original, sizeof(original), 0) ==
				  (ssize_t) sizeof(original) && fsync(fd) == 0 && close(fd) == 0,
				  "restore the selected watermark after mismatch");
		}
	}
	/* The published WAL-index frontier can have no progress delta while the
	 * active epoch still accumulates an append tail.  Force eligibility must
	 * select this timeline despite the normal geometric snapshot trigger. */
	memset(&channel, 0, sizeof(channel));
	channel.timeline = 0;
	channel.opcode = PS_OP_WAL_INDEX_ADD;
	channel.blocknum = 1;
	channel.req_lsn = 100;
	channel.key = (PsKey) {1, 1, 1, 0, PS_KLASS_RELATION};
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
		  "append WAL-index tail without advancing the published frontier");
	ps_backpressure_refresh();
	check(metrics.walidx_backpressure.throttled != 0 &&
		  ps_test_walidx_force_due(0) != 0,
		  "tail-only debt throttles and marks its timeline force-eligible");
	for (int i = 0; i < 16 && metrics.walidx_backpressure.throttled != 0; i++)
	{
		(void) ps_core_maintenance();
		ps_backpressure_refresh();
	}
	check(metrics.walidx_backpressure.lag_bytes == 0 &&
		  metrics.walidx_backpressure.throttled == 0 &&
		  ps_test_walidx_force_due(0) == 0,
		  "forced same-frontier publication clears tail-only debt");
	{
		WalIdxObservationRetryTest retry_test;
		int hidden;

		memset(&retry_test, 0, sizeof(retry_test));
		snprintf(retry_test.manifest, sizeof(retry_test.manifest),
				 "%s/walidx_snapshots_0/walidx_manifest_v1", store);
		snprintf(retry_test.saved_manifest, sizeof(retry_test.saved_manifest),
				 "%s/walidx_snapshots_0/walidx_manifest_v1.saved", store);
		hidden = rename(retry_test.manifest, retry_test.saved_manifest) == 0;
		check(hidden, "hide the selected manifest for observation retry setup");
		if (hidden)
		{
			ps_test_set_walidx_observation_error_hook(
				walidx_observation_error_repair, &retry_test);
			ps_backpressure_refresh();
			ps_test_set_walidx_observation_error_hook(NULL, NULL);
			check(retry_test.calls == 1 && retry_test.repaired &&
				  metrics.walidx_backpressure.lag_bytes != UINT64_MAX,
				  "a moving identity retries a failed physical observation");
		}
		else
			ps_test_set_walidx_observation_error_hook(NULL, NULL);
		for (int i = 0; i < 16 &&
			 metrics.walidx_backpressure.throttled != 0; i++)
		{
			(void) ps_core_maintenance();
			ps_backpressure_refresh();
		}
		check(!hidden || metrics.walidx_backpressure.lag_bytes == 0,
			  "observation retry debt remains serviceable");
	}
	{
		char orphan[1024];
		int fd;

		snprintf(orphan, sizeof(orphan),
				 "%s/walidx_snapshots_0/walidxg1_%020llu_%03u",
				 store, 99ULL, 0U);
		fd = open(orphan, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(fd >= 0 && write(fd, "orphan", 6) == 6 &&
			  fsync(fd) == 0 && close(fd) == 0,
			  "create an orphan newer WAL-index snapshot shard");
		ps_backpressure_refresh();
		check(metrics.walidx_backpressure.throttled != 0 &&
			  ps_test_walidx_gc_force_due(0) != 0,
			  "orphan snapshot debt marks a WAL-index GC candidate");
		check(ps_core_maintenance() == 1 && access(orphan, F_OK) != 0 &&
			  errno == ENOENT,
			  "GC-force maintenance removes orphan snapshot debt");
		ps_backpressure_refresh();
		check(metrics.walidx_backpressure.lag_bytes == 0 &&
			  metrics.walidx_backpressure.throttled == 0 &&
			  ps_test_walidx_gc_force_due(0) == 0,
			  "orphan-only debt releases the WAL-index throttle");
	}
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_core_open(store) == 0,
		  "restart WAL-index append-tail store");
	ps_core_set_metrics_header(&metrics);
	check(metrics.walidx_backpressure.lag_bytes == 0 &&
		  metrics.walidx_backpressure.throttled == 0,
		  "restart sees no debt after the append tail was reclaimed");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
		  "disable WAL-index backpressure after append-tail test");
	remove_tree(store);
}

static int
create_test_branch(uint32_t timeline, uint32_t parent, uint64_t branch_lsn)
{
	PsChannel ch;
	uint64_t parent_incarnation;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_TIMELINE_STATE;
	ch.timeline = parent;
	ch.status = PS_STATUS_OK;
	if (ps_handle_meta(&ch) != 1 || ch.status != PS_STATUS_OK)
		return 0;
	parent_incarnation = ch.req_seq;
	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_CREATE_BRANCH;
	ch.timeline = timeline;
	ch.parent_timeline = parent;
	ch.req_lsn = branch_lsn;
	ch.req_seq = parent_incarnation;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_shard_wr(0);
	ps_lock_map_wr();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_unlock_shard(0);
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
append_test_walidx_tail(uint32_t timeline, uint64_t progress)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.timeline = timeline;
	ch.opcode = PS_OP_WAL_APPEND;
	ch.datalen = 128;
	memset(ch.data, 0xa5, ch.datalen);
	if (ps_handle_meta(&ch) != 1 || ch.status != PS_STATUS_OK)
		return 0;
	memset(&ch, 0, sizeof(ch));
	ch.timeline = timeline;
	ch.opcode = PS_OP_WAL_INDEX_ADD;
	ch.req_lsn = 100;
	ch.key = (PsKey) {1, 1, 1, 0, PS_KLASS_RELATION};
	if (ps_handle_meta(&ch) != 1 || ch.status != PS_STATUS_OK)
		return 0;
	memset(&ch, 0, sizeof(ch));
	ch.timeline = timeline;
	ch.opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch.req_lsn = 0;
	ch.req_seq = progress;
	return ps_handle_meta(&ch) == 1 && ch.status == PS_STATUS_OK;
}

/* Keep the observation retry test's logical identity transition independent
 * of WAL_INDEX_PROGRESS.  The latter is a monotonic frontier publication and
 * is not needed to make walidx_shard_offsets_seen change. */
static int
append_test_walidx_identity(uint32_t timeline)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.timeline = timeline;
	ch.opcode = PS_OP_WAL_INDEX_ADD;
	ch.blocknum = 2;
	ch.req_lsn = 200;
	ch.key = (PsKey) {2, 2, 2, 0, PS_KLASS_RELATION};
	return ps_handle_meta(&ch) == 1 && ch.status == PS_STATUS_OK;
}

static int
write_test_file(const char *path, size_t len)
{
	unsigned char bytes[32];
	int fd;

	if (len > sizeof(bytes))
		return 0;
	memset(bytes, 0xa5, sizeof(bytes));
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	if (fd < 0 || write(fd, bytes, len) != (ssize_t) len || fsync(fd) != 0)
	{
		if (fd >= 0)
			(void) close(fd);
		return 0;
	}
	return close(fd) == 0;
}

static int
test_path_suffix(char *path, size_t path_size, const char *base,
				 const char *suffix)
{
	size_t base_len = strlen(base);
	size_t suffix_len = strlen(suffix);

	if (base_len >= path_size || suffix_len > path_size - 1 - base_len)
		return -1;
	memcpy(path, base, base_len);
	memcpy(path + base_len, suffix, suffix_len + 1);
	return 0;
}

static void
test_forkmeta_backpressure_observer(void)
{
	char store[] = "/tmp/pagestore-forkmeta-backpressure-XXXXXX";
	char snapshots[1024];
	char old_checkpoint[1200];
	char old_tail[1200];
	char temporary[1200];
	char malformed[1200];
	char malformed_temp[1200];
	char source[1024];
	struct stat before;
	struct stat after;
	PsShmHeader metrics;
	PsChannel channel;
	volatile sig_atomic_t stop = 0;
	uint32_t causes = 0;
	BackpressureSlowPathCounter cutover_attempts = {0};

	configure_page_core();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_backpressure_configure_all_with_forkmeta(0, 0, 0, 0, 0, 0,
																				128, 20) == 0 &&
			mkdtemp(store) != NULL && ps_core_open(store) == 0,
				"open a store for forkmeta backpressure observation");
	ps_core_set_metrics_header(&metrics);
	check(snprintf(source, sizeof(source), "%s/forkmeta", store) >= 0 &&
			stat(source, &before) == 0,
			"locate the stable forkmeta source baseline");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0,
			"migration marker prefix is not reclaimable source debt");

	memset(&channel, 0, sizeof(channel));
	channel.opcode = PS_OP_CREATE;
	channel.timeline = 0;
	channel.key = (PsKey) {11, 11, 11, 0, PS_KLASS_RELATION};
	channel.req_lsn = 100;
		check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK &&
			stat(source, &after) == 0,
			"append a forkmeta growth event after the baseline");
	channel.opcode = PS_OP_CREATE;
	channel.key = (PsKey) {12, 12, 12, 0, PS_KLASS_RELATION};
	channel.req_lsn = 150;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"metadata-only create churn is admitted without a frontier");
	channel.opcode = PS_OP_ZEROEXTEND;
	channel.blocknum = 0;
	channel.nblocks = 1;
	channel.req_lsn = 200;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"metadata-only zero-extend churn is admitted without a frontier");
	channel.opcode = PS_OP_UNLINK;
	channel.key = (PsKey) {11, 11, 11, 0, PS_KLASS_RELATION};
	channel.req_lsn = 300;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"metadata-only unlink churn is admitted without a frontier");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0,
			"metadata-only churn does not deadlock without a safe frontier");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_core_open(store) == 0,
			"restart before the first selected snapshot");
	ps_core_set_metrics_header(&metrics);
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0,
			"restart does not charge source history without a safe frontier");
	check(ps_backpressure_configure_all_with_forkmeta(0, 0, 0, 0, 0, 0,
																				20, 10) == 0,
			"lower forkmeta threshold for physical-debris admission test");
	check(snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store) >= 0 &&
			mkdir(snapshots, 0700) == 0 &&
		snprintf(old_checkpoint, sizeof(old_checkpoint),
				 "%s/forkmeta_checkpoint_v1_00000000000000000001", snapshots) >= 0 &&
		snprintf(old_tail, sizeof(old_tail),
				 "%s/forkmeta_tail_v1_00000000000000000001", snapshots) >= 0 &&
		snprintf(temporary, sizeof(temporary), "%s/forkmeta_tail_v1_00000000000000000002.tmp.1.1",
				 snapshots) >= 0 && write_test_file(old_checkpoint, 11) &&
			write_test_file(old_tail, 13) && write_test_file(temporary, 7),
			"create obsolete forkmeta generations and temporary debris");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 7 &&
			metrics.forkmeta_backpressure.throttled == 0 &&
			ps_test_forkmeta_serviceable_work_due() != 0,
			"only immediately GC-serviceable debris counts without a cutoff");
	ps_test_set_forkmeta_cutover_hook(count_backpressure_slow_path,
									 &cutover_attempts);
	check(ps_core_maintenance() == 1 && access(temporary, F_OK) != 0,
				"temp-only GC runs without forcing a below-threshold cutover");
	ps_test_set_forkmeta_cutover_hook(NULL, NULL);
	ps_backpressure_refresh();
	check(cutover_attempts.calls == 0,
				"below-threshold serviceable lag does not force snapshot publication");
	check(metrics.forkmeta_backpressure.lag_bytes == 0,
				"bounded temp GC clears serviceable lag below the snapshot threshold");
	check(write_test_file(temporary, 7),
				"recreate temporary debris for GC fsync ambiguity");
	ps_backpressure_refresh();
	check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC", "1", 1) == 0 &&
			ps_core_maintenance() == 0 && access(temporary, F_OK) != 0 &&
			unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") == 0,
				"temp GC keeps a pending retry after post-unlink fsync ambiguity");
	ps_test_forkmeta_snapshot_gc_retry_now();
	check(ps_core_maintenance() == 0,
				"temp GC retry reconciles the empty directory after ambiguity");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0,
				"temp GC retry clears reconciled serviceable debt");
	check(ps_backpressure_try_admit_mask(&stop, PS_BACKPRESSURE_FORKMETA,
										 &causes) == 1 && causes == 0,
							"cutoff-dependent debris does not defer admission");

	check(snprintf(malformed, sizeof(malformed), "%s/unexpected", snapshots) >= 0 &&
			write_test_file(malformed, 3),
			"create an unrecognized snapshot entry");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == UINT64_MAX &&
			metrics.forkmeta_backpressure.throttled != 0,
			"unsafe snapshot observation fails closed");
	check(ps_test_forkmeta_force_due() != 0 &&
			ps_test_forkmeta_serviceable_work_due() == 0,
			"observation error throttles but does not force snapshot work");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
			"lower snapshot trigger for persistent observation error test");
	{
		int churn_ok = 1;

		for (unsigned int i = 0; i < 32; i++)
		{
			memset(&channel, 0, sizeof(channel));
			channel.opcode = PS_OP_CREATE;
			channel.timeline = 0;
			channel.key = (PsKey) {100 + i, 100 + i, 100 + i, 0,
				PS_KLASS_RELATION};
			channel.req_lsn = 400 + i;
			if (ps_handle_meta(&channel) != 1 ||
				channel.status != PS_STATUS_OK)
				churn_ok = 0;
		}
		check(churn_ok, "grow source while the snapshot observation is invalid");
	}
	ps_backpressure_refresh();
	ps_test_set_forkmeta_cutover_hook(count_backpressure_slow_path,
										  &cutover_attempts);
	for (int i = 0; i < 3; i++)
	{
		ps_test_forkmeta_snapshot_gc_retry_now();
		(void) ps_core_maintenance();
	}
	ps_test_set_forkmeta_cutover_hook(NULL, NULL);
	check(cutover_attempts.calls == 0,
			"persistent observation error does not retry snapshot cutover");
	check(unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES") == 0,
			"restore snapshot trigger after persistent observation test");
	(void) unlink(malformed);
	check(snprintf(malformed_temp, sizeof(malformed_temp),
					 "%s/forkmeta_checkpoint_v1_bad.tmp.1.1", snapshots) >= 0 &&
			write_test_file(malformed_temp, 5),
			"create a malformed owned temporary snapshot name");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == UINT64_MAX,
			"malformed owned snapshot names fail closed");
	(void) unlink(malformed_temp);
	check(metrics.forkmeta_backpressure.throttle_enters == 1,
			"forkmeta controller records one throttle transition");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
			"disable forkmeta backpressure after observer test");
	remove_tree(store);
}

static void
test_forkmeta_self_recovery(void)
{
	char store[] = "/tmp/pagestore-forkmeta-self-recovery-XXXXXX";
	char manifest[1200];
	char snapshots[1200];
	char old_checkpoint[1200];
	char old_tail[1200];
	char source[1200];
	struct stat source_before;
	struct stat source_after;
	PsShmHeader metrics;
	PsChannel channel;
	PsRetentionPin pin;
	PsKey key = {11, 11, 11, 0, PS_KLASS_RELATION};
	ForkmetaProofRace race;
	PsKey branch_page_key = {22, 22, 22, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t page_admission_seq = 0;
	uint64_t branch_page_admission_seq = 0;
	uint64_t race_throttle_enters;
	uint64_t branch_throttle_enters;
	int did = 0;
	int reopen_rc;
	uint64_t selected_generation = 0;

	configure_page_core();
	compact_layers = 0;
	check(ps_backpressure_configure_all_with_forkmeta(0, 0, 0, 0, 0, 0,
			32, 20) == 0 &&
			mkdtemp(store) != NULL && ps_core_open(store) == 0,
			"open a store for forkmeta self-recovery");
	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	for (unsigned char tag = 1; tag <= 2; tag++)
	{
		fill_page(page, tag);
		ps_admission_read_lock();
		ps_lock_shard_wr(0);
		check(append_page(0, &key, 0, page, 0,
				tag == 2 ? &page_admission_seq : NULL) == 0,
				"seed replaceable page versions for a safe snapshot cutoff");
		ps_unlock_shard(0);
		ps_admission_read_unlock();
	}
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = PS_RETENTION_OWNER_CONFIGURED;
	pin.owner_id = 19001;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 1002;
	pin.admission_seq = page_admission_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK,
			"pin the newest page version as the safe compaction cutoff");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(ps_core_open(store) == 0,
			"reopen with flushed page layers before maintenance recovery");
	ps_core_set_metrics_header(&metrics);
	check(snprintf(source, sizeof(source), "%s/forkmeta", store) >= 0 &&
			stat(source, &source_before) == 0,
			"locate migration-marker baseline after restart");
	for (int i = 0; i < 8; i++)
		(void) ps_core_maintenance();
	memset(&channel, 0, sizeof(channel));
	channel.opcode = PS_OP_CREATE;
	channel.timeline = 0;
	channel.key = key;
	channel.req_lsn = 100;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"seed forkmeta lifecycle history after a safe page frontier exists");
	ps_backpressure_refresh();
	check(stat(source, &source_after) == 0 &&
			source_after.st_size > source_before.st_size &&
			metrics.forkmeta_backpressure.lag_bytes ==
			(uint64_t) (source_after.st_size - source_before.st_size) &&
			metrics.forkmeta_backpressure.throttled != 0,
			"restart excludes migration prefix but charges frontier-covered history");
	check(snprintf(manifest, sizeof(manifest), "%s/forkmeta_snapshots/forkmeta_manifest_v1",
				store) >= 0,
			"build the forkmeta self-recovery manifest path");
	for (int i = 0; i < 64; i++)
	{
		if (ps_core_maintenance())
			did = 1;
		ps_backpressure_refresh();
		if (access(manifest, F_OK) == 0 &&
			metrics.forkmeta_backpressure.lag_bytes <= 20 &&
			metrics.forkmeta_backpressure.throttled == 0)
			break;
	}
	check(did && access(manifest, F_OK) == 0 &&
		metrics.forkmeta_backpressure.lag_bytes <= 20 &&
		metrics.forkmeta_backpressure.throttled == 0,
		"maintenance publishes snapshot/GC and clears forkmeta throttle");
	/* The selected generation covers timeline 0, but not a new owner created
	 * after that cutover.  Metadata-only churn on the new timeline must remain
	 * admissible until that timeline has a real page-reclamation frontier. */
	check(create_test_branch(1, 0, 1002),
			"create a new timeline after selecting the forkmeta snapshot");
	memset(&race, 0, sizeof(race));
	race.key = (PsKey) {33, 33, 33, 0, PS_KLASS_RELATION};
	race_throttle_enters = metrics.forkmeta_backpressure.throttle_enters;
	ps_test_set_forkmeta_snapshot_observation_hook(forkmeta_proof_race, &race);
	ps_backpressure_refresh();
	ps_test_set_forkmeta_snapshot_observation_hook(NULL, NULL);
	if (race.worker_created)
		pthread_join(race.worker, NULL);
	check(race.calls == 1 && race.worker_created && !race.failed &&
			!__atomic_load_n(&race.acquired_before_hook_returned,
						 __ATOMIC_ACQUIRE) &&
			__atomic_load_n(&race.acquired, __ATOMIC_ACQUIRE),
			"foreground metadata mutation waits for the observation fence");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0 &&
			metrics.forkmeta_backpressure.throttle_enters ==
			race_throttle_enters,
			"next refresh rejects source debt for the newly admitted owner");
	check(race.calls == 1 && !race.failed &&
			metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0,
			"source debt is dropped when a new owner appears during observation");
	memset(&channel, 0, sizeof(channel));
	channel.timeline = 1;
	channel.opcode = PS_OP_CREATE;
	channel.key = key;
	channel.req_lsn = 1100;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"append CREATE metadata on the new timeline without a frontier");
	channel.opcode = PS_OP_ZEROEXTEND;
	channel.blocknum = 0;
	channel.nblocks = 1;
	channel.req_lsn = 1150;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"append ZEROEXTEND metadata on the new timeline without a frontier");
	channel.opcode = PS_OP_UNLINK;
	channel.req_lsn = 1200;
	check(ps_handle_meta(&channel) == 1 && channel.status == PS_STATUS_OK,
			"append UNLINK metadata on the new timeline without a frontier");
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0,
			"selected snapshot does not charge an uncovered metadata-only owner");
	branch_throttle_enters = metrics.forkmeta_backpressure.throttle_enters;
	/* Establish progress on that same owner.  Once its durable page frontier is
	 * published, source growth becomes eligible and the controller can cut over
	 * instead of deadlocking behind its own forkmeta gate. */
	for (unsigned char tag = 1; tag <= 2; tag++)
	{
		fill_page_lsn(page, tag, 2000 + tag);
		ps_admission_read_lock();
		ps_lock_shard_wr(0);
		check(append_page(1, &branch_page_key, 0, page, 0,
				tag == 2 ? &branch_page_admission_seq : NULL) == 0,
				"write page history for the new timeline frontier");
		ps_unlock_shard(0);
		ps_admission_read_unlock();
	}
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 1;
	pin.owner_kind = PS_RETENTION_OWNER_CONFIGURED;
	pin.owner_id = 19002;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 2002;
	pin.admission_seq = branch_page_admission_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK,
			"pin the new timeline page frontier");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(ps_core_open(store) == 0,
			"reopen the new timeline page history for frontier maintenance");
	ps_core_set_metrics_header(&metrics);
	ps_backpressure_refresh();
	check(metrics.forkmeta_backpressure.lag_bytes == 0 &&
			metrics.forkmeta_backpressure.throttled == 0,
			"reopen observes no source debt before the new timeline frontier");
	/* The initial recovery fixture uses zero to force root compaction.  Keep the
	 * single root layer out of this phase so the two new timeline layers can
	 * publish their frontier and expose the post-maintenance refresh. */
	compact_layers = 1;
	segment_gc_enabled = 0;
	for (int i = 0; i < 64; i++)
	{
		ps_test_forkmeta_snapshot_gc_retry_now();
		(void) ps_core_maintenance();
		ps_backpressure_refresh();
		if (metrics.forkmeta_backpressure.throttle_enters > branch_throttle_enters)
			break;
	}
	check(metrics.forkmeta_backpressure.throttle_enters > branch_throttle_enters,
			"new timeline source debt becomes eligible after its frontier");
	did = 0;
	for (int i = 0; i < 64; i++)
	{
		if (ps_core_maintenance())
			did = 1;
		ps_backpressure_refresh();
		if (metrics.forkmeta_backpressure.throttled == 0)
			break;
	}
	check(did && metrics.forkmeta_backpressure.throttled == 0,
			"new timeline forkmeta debt remains serviceable after frontier cutover");
	{
		PsForkmetaSnapshot selected;
		int selected_open = -1;

		check(snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots",
					   store) >= 0 &&
				(selected_open = ps_forkmeta_snapshot_open(&selected, snapshots)) == 0 &&
				selected.generation > 1,
				"open the selected snapshot for canonical GC ambiguity");
		if (selected_open == 0)
		{
			selected_generation = selected.generation;
			ps_forkmeta_snapshot_close(&selected);
		}
	}
	if (selected_generation > 1)
	{
		uint64_t old_generation = selected_generation - 1;

		check(snprintf(old_checkpoint, sizeof(old_checkpoint),
					   "%s/forkmeta_checkpoint_v1_%020llu", snapshots,
					   (unsigned long long) old_generation) >= 0 &&
				  snprintf(old_tail, sizeof(old_tail),
					   "%s/forkmeta_tail_v1_%020llu", snapshots,
					   (unsigned long long) old_generation) >= 0 &&
				  write_test_file(old_checkpoint, 11) &&
				  write_test_file(old_tail, 13),
				  "create canonical generation debris for ambiguity retry");
		ps_backpressure_refresh();
		check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC", "1", 1) == 0 &&
				  ps_core_maintenance() == 0 &&
				  access(old_checkpoint, F_OK) != 0 &&
				  access(old_tail, F_OK) != 0 &&
				  ps_test_forkmeta_canonical_gc_ambiguous() != 0 &&
				  unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") == 0,
				  "canonical GC retains pending ambiguity after post-unlink fsync fault");
		ps_test_forkmeta_snapshot_gc_retry_now();
		check(ps_core_maintenance() == 0 &&
				  ps_test_forkmeta_canonical_gc_ambiguous() == 0,
				  "canonical GC empty retry closes directory-fsync ambiguity");
		ps_backpressure_refresh();
		check(metrics.forkmeta_backpressure.lag_bytes == 0,
				  "canonical GC retry reconciles physical debt");
	}
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(unlink(source) == 0,
			"remove selected snapshot source for baseline-init fixture");
	ps_test_set_forkmeta_baseline_init_hook(remove_forkmeta_source, source);
	reopen_rc = ps_core_open(store);
	ps_test_set_forkmeta_baseline_init_hook(NULL, NULL);
	check(reopen_rc != 0,
			"selected snapshot with missing source fails closed on restart");
	ps_storage->close();
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
			"disable forkmeta backpressure after self-recovery test");
	remove_tree(store);
}

static void
test_walidx_aggregate_force(void)
{
	char store[] = "/tmp/pagestore-walidx-aggregate-XXXXXX";
	PsShmHeader metrics;

	configure_page_core();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_backpressure_configure_all(0, 0, 0, 0, 100, 0) == 0 &&
		  mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open a store for aggregate WAL-index force scheduling");
	ps_core_set_metrics_header(&metrics);
	check(append_test_walidx_tail(0, 128),
		  "seed the root timeline WAL-index tail");
	check(create_test_branch(1, 0, 0),
		  "create a second timeline for aggregate WAL-index debt");
	check(append_test_walidx_tail(1, 128),
		  "seed the second timeline WAL-index tail");
	ps_backpressure_refresh();
	check(metrics.walidx_backpressure.lag_bytes >= 100 &&
		  metrics.walidx_backpressure.throttled != 0 &&
		  ps_test_walidx_force_due(0) != 0 &&
		  ps_test_walidx_force_due(1) != 0,
		  "aggregate WAL-index debt forces every timeline with append tail");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
		  "disable aggregate WAL-index backpressure");
	remove_tree(store);
}

static void
test_walidx_automatic_observation_rate(void)
{
	char store[] = "/tmp/pagestore-walidx-observation-XXXXXX";
	PsShmHeader metrics;
	uint64_t before;
	uint64_t after;

	configure_page_core();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_backpressure_configure_all(0, 0, 0, 0, 100, 0) == 0 &&
		  mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open a store for automatic WAL-index observation pacing");
	ps_core_set_metrics_header(&metrics);
	before = ps_test_backpressure_walidx_observation_count();
	ps_backpressure_refresh();
	after = ps_test_backpressure_walidx_observation_count();
	check(after > before,
		  "explicit WAL-index observation runs immediately");
	(void) ps_core_maintenance();
	check(ps_test_backpressure_walidx_observation_count() == after,
		  "automatic WAL-index observation is rate-limited while idle");
	usleep(120000);
	(void) ps_core_maintenance();
	check(ps_test_backpressure_walidx_observation_count() > after,
		  "automatic WAL-index observation resumes after its interval");
	ps_core_set_metrics_header(NULL);
	ps_core_close();
	ps_storage->close();
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
		  "disable WAL-index backpressure after observation pacing test");
	remove_tree(store);
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

	fill_page_lsn(page, tag, lsn);
}

static void
fill_page_lsn(unsigned char *page, unsigned char tag, uint64_t lsn)
{
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

static void
test_ambiguous_page_segment_remove(void)
{
	char store[] = "/tmp/pagestore-page-backpressure-remove-XXXXXX";
	PsShmHeader metrics;
	int64_t removed_size;

	configure_page_core();
	check(ps_backpressure_configure(32768, 1, 0, 0) == 0,
		  "configure PAGE debt for ambiguous segment removal");
	if (!make_page_debt_store(store))
	{
		check(0, "construct PAGE debt for ambiguous segment removal");
		remove_tree(store);
		return;
	}
	check(1, "construct PAGE debt for ambiguous segment removal");
	ps_core_close();
	ps_storage->close();

	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	check(setenv("PAGESTORE_TEST_FAIL_SEG_REMOVE_DIR_FSYNC", "1", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "reopen PAGE debt store with post-unlink directory-fsync fault");
	ps_backpressure_refresh();
	check(metrics.page_backpressure.lag_bytes == segment_size &&
		  metrics.page_backpressure.throttled != 0,
		  "covered segment debt is throttled before ambiguous removal");
	(void) ps_core_maintenance();
	errno = 0;
	removed_size = ps_storage->seg_size(0, 0);
	check(removed_size < 0 && errno == ENOENT,
		  "post-unlink directory-fsync failure physically unlinks the segment");
	ps_backpressure_refresh();
	check(metrics.page_backpressure.lag_bytes == segment_size &&
		  metrics.page_backpressure.throttled != 0,
		  "lag remains while the ambiguous counted removal is pending");

	unsetenv("PAGESTORE_TEST_FAIL_SEG_REMOVE_DIR_FSYNC");
	check(ps_core_maintenance() == 1,
		  "confirmed ENOENT settles the pending removal");
	ps_backpressure_refresh();
	check(metrics.page_backpressure.lag_bytes == 0 &&
		  metrics.page_backpressure.throttled == 0 &&
		  metrics.page_backpressure.throttle_exits == 1,
		  "confirmed removal reaches catch-up without double decrement");
	for (int i = 0; i < 8; i++)
		(void) ps_core_maintenance();
	ps_backpressure_refresh();
	check(metrics.page_backpressure.lag_bytes == 0,
		  "repeated maintenance does not underflow or resurrect PAGE debt");

	ps_core_close();
	ps_storage->close();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_core_open(store) == 0,
		  "restart after ambiguous segment removal");
	ps_backpressure_refresh();
	check(metrics.page_backpressure.lag_bytes == 0 &&
		  metrics.page_backpressure.throttled == 0,
		  "restart rebuild agrees that the removed segment has no debt");
	ps_core_close();
	ps_storage->close();
	ps_core_set_metrics_header(NULL);
	check(ps_backpressure_configure(0, 0, 0, 0) == 0,
		  "disable PAGE debt after ambiguous segment removal test");
	remove_tree(store);
}

int
main(void)
{
	test_validation_and_hysteresis();
	test_nonblocking_admission_and_shutdown();
	test_forkmeta_backpressure_observer();
	test_forkmeta_self_recovery();
	test_walidx_append_tail_restart();
	test_walidx_aggregate_force();
	test_walidx_automatic_observation_rate();
	test_admission_writer_preference();
	test_page_storage_fail_closed();
	test_ambiguous_page_segment_remove();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
