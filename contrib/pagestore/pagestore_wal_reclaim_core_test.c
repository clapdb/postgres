/*
 * Focused POSIX/core tests for the conservative R3b-3 WAL reclaim policy.
 * This test deliberately uses the shared core directly: it does not start a
 * daemon and it does not pretend to cover SPDK callback scheduling.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "pagestore_forkmeta_snapshot.h"
#include "pagestore_walidx_snapshot.h"

#include "pagestore_core.h"
#include "pagestore_retention.h"

#define WAL_SEGMENT (1024u * 1024u)
#define WAL_SEGMENTS 3u
#define WAL_TOTAL ((uint64_t) WAL_SEGMENT * WAL_SEGMENTS)

static int checks;
static int failed;

typedef struct AdmissionCallCounter
{
	unsigned int calls;
} AdmissionCallCounter;

static int
count_admission_call(pthread_rwlock_t *lock, void *arg)
{
	AdmissionCallCounter *counter = arg;

	counter->calls++;
	return pthread_rwlock_wrlock(lock);
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
remove_tree(const char *path)
{
	char command[512];

	/* best effort: a store left behind in /tmp is a nuisance, not a failure
	 * (and the compiler's warn_unused_result on system() is not silenced by
	 * a void cast) */
	if (snprintf(command, sizeof(command), "rm -rf -- '%s'", path) > 0 &&
		system(command) != 0)
		fprintf(stderr, "note: could not remove %s\n", path);
}

static void
configure_core(void)
{
	page_size = 8192;
	segment_size = WAL_SEGMENT;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 0;
	cache_pages = 0;
	use_layers = 1;
	ps_nshards = 1;
	ps_storage = &PsStoragePosix;
}

static int
append_wal_bytes(uint32_t timeline, uint64_t start, uint32_t len)
{
	unsigned char data[64 * 1024];

	memset(data, (int) (timeline + start / WAL_SEGMENT), sizeof(data));
	while (len != 0)
	{
		uint32_t amount = len < sizeof(data) ? len : (uint32_t) sizeof(data);
		PsChannel ch;

		memset(&ch, 0, sizeof(ch));
		ch.opcode = PS_OP_WAL_APPEND;
		ch.timeline = timeline;
		ch.req_lsn = start;
		ch.datalen = amount;
		memcpy(ch.data, data, amount);
		ch.status = PS_STATUS_OK;
		ps_lifecycle_read_lock();
		(void) ps_handle_meta(&ch);
		ps_lifecycle_read_unlock();
		if (ch.status != PS_STATUS_OK)
			return 0;
		start += amount;
		len -= amount;
	}
	return 1;
}

static int
wal_index_add_record(uint32_t timeline, uint64_t lsn, uint32_t block, int fpi)
{
	PsChannel ch;
	PsWalIndexEntry entry;
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	memset(&ch, 0, sizeof(ch));
	memset(&entry, 0, sizeof(entry));
	entry.key = key;
	entry.block = block;
	entry.lsn = lsn;
	entry.end_lsn = lsn + 50;
	entry.flags = PS_WAL_INDEX_FLAG_KNOWN | (fpi ? PS_WAL_INDEX_FLAG_FPI : 0);
	ch.opcode = PS_OP_WAL_INDEX_ADD_BATCH;
	ch.timeline = timeline;
	ch.key = key;
	ch.nblocks = 1;
	ch.datalen = sizeof(entry);
	memcpy(ch.data, &entry, sizeof(entry));
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_shard_wr(ps_shard_of(&key));
	(void) ps_handle_meta(&ch);
	ps_unlock_shard(ps_shard_of(&key));
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
wal_index_add(uint32_t timeline, uint64_t lsn)
{
	return wal_index_add_record(timeline, lsn, 0, 1);
}

/* Number of indexed records for (relation, block) at or below lsn_max. */
static int
wal_index_count(uint32_t timeline, uint32_t block, uint64_t lsn_max)
{
	PsChannel ch;
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_INDEX_GET;
	ch.timeline = timeline;
	ch.key = key;
	ch.blocknum = block;
	ch.req_lsn = lsn_max;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK ? (int) ch.result : -1;
}

static int
wal_index_progress(uint32_t timeline, uint64_t start, uint64_t end)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch.timeline = timeline;
	ch.req_lsn = start;
	ch.req_seq = end;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static uint64_t
wal_floor(uint32_t timeline)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_RETAIN_FLOOR;
	ch.timeline = timeline;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK ? ch.req_lsn : 0;
}

static uint64_t
effective_floor(uint32_t timeline, uint32_t resource)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_FLOOR;
	ch.timeline = timeline;
	ch.parent_timeline = resource;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK ? ch.req_lsn : 0;
}

static int
write_control(uint32_t timeline, uint64_t version, uint64_t redo)
{
	PsKey key;
	unsigned char page[8192];

	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;
	memset(page, 0xC3, sizeof(page));
	ps_lock_shard_wr(ps_shard_of(&key));
	if (append_page(timeline, &key, 0, page, version, NULL) != 0)
	{
		ps_unlock_shard(ps_shard_of(&key));
		return 0;
	}
	memset(page, 0, sizeof(page));
	memcpy(page, &redo, sizeof(redo));
	if (append_page(timeline, &key, 1, page, version, NULL) != 0)
	{
		ps_unlock_shard(ps_shard_of(&key));
		return 0;
	}
	ps_unlock_shard(ps_shard_of(&key));
	return ps_storage->sync() == 0;
}

static int
set_wal_pin_generation(uint32_t timeline, uint64_t owner_id, uint32_t generation,
					   uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_SET;
	ch.timeline = timeline;
	ch.blocknum = PS_RETENTION_OWNER_READER;
	ch.parent_timeline = PS_RETENTION_RESOURCE_WAL;
	ch.old_nblocks = generation;
	ch.req_seq = owner_id;
	ch.req_lsn = lsn;
	ch.nblocks = 1;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
set_wal_pin(uint32_t timeline, uint64_t owner_id, uint64_t lsn)
{
	return set_wal_pin_generation(timeline, owner_id, 1, lsn);
}

static int
write_keyed_page(uint32_t timeline, const PsKey *keyp, uint32_t block,
				 uint64_t lsn)
{
	PsKey key = *keyp;
	unsigned char page[8192];
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;
	int rc;

	memset(page, 0x5A, sizeof(page));
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	ps_lock_shard_wr(ps_shard_of(&key));
	rc = append_page(timeline, &key, block, page, lsn, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc == 0 && ps_storage->sync() == 0;
}

static int
write_page_key(uint32_t timeline, uint32_t rel, uint32_t block, uint64_t lsn)
{
	PsKey key = {1, 1, rel, 0, PS_KLASS_RELATION};

	return write_keyed_page(timeline, &key, block, lsn);
}

static int
write_relation_page(uint32_t timeline, uint32_t block, uint64_t lsn)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	return write_keyed_page(timeline, &key, block, lsn);
}


static int
unlink_relation(uint32_t timeline, uint64_t lsn)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_UNLINK;
	ch.timeline = timeline;
	ch.key = key;
	ch.req_lsn = lsn;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_shard_wr(ps_shard_of(&key));
	(void) ps_handle_meta(&ch);
	ps_unlock_shard(ps_shard_of(&key));
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK && ps_storage->sync() == 0;
}

static int
fork_op_keyed(uint32_t timeline, const PsKey *keyp, PsOpcode opcode,
			  uint64_t lsn, uint32_t nblocks, uint32_t block)
{
	PsKey key = *keyp;
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.timeline = timeline;
	ch.key = key;
	ch.req_lsn = lsn;
	ch.nblocks = nblocks;
	ch.blocknum = block;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(&key));
	(void) ps_handle_meta(&ch);
	ps_unlock_shard(ps_shard_of(&key));
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK && ps_storage->sync() == 0;
}

static int
truncate_relation(uint32_t timeline, uint64_t lsn, uint32_t nblocks)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	return fork_op_keyed(timeline, &key, PS_OP_TRUNCATE, lsn, nblocks, 0);
}

/* Create or regrow the relation to one zero block, as a truncate-then-extend
 * writer would, then store a page for block 0. */
static int
regrow_relation(uint32_t timeline, uint64_t lsn)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	return fork_op_keyed(timeline, &key, PS_OP_ZEROEXTEND, lsn, 1, 0) &&
		write_relation_page(timeline, 0, lsn + 10);
}

/* Enough fork-lifecycle bytes on other relations to reach the forkmeta
 * snapshot trigger without touching the relation under test. */
static int
churn_fork_bytes(uint32_t timeline, uint64_t lsn)
{
	for (uint32_t i = 0; i < 48; i++)
	{
		PsKey key = {1, 1, 100 + i, 0, PS_KLASS_RELATION};

		if (!fork_op_keyed(timeline, &key, PS_OP_CREATE, lsn + 2 * i, 0, 0) ||
			!fork_op_keyed(timeline, &key, PS_OP_ZEROEXTEND, lsn + 2 * i + 1,
						   1, 0))
			return 0;
	}
	return 1;
}

static int
reserve_pin(uint32_t timeline, uint32_t kind, uint64_t owner_id,
			uint32_t generation, uint32_t resources, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch.timeline = timeline;
	ch.blocknum = kind;
	ch.parent_timeline = resources;
	ch.old_nblocks = generation;
	ch.req_seq = owner_id;
	ch.req_lsn = lsn;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
drop_wal_pin(uint32_t timeline, uint64_t owner_id, uint32_t generation)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_DROP;
	ch.timeline = timeline;
	ch.blocknum = PS_RETENTION_OWNER_READER;
	ch.old_nblocks = generation;
	ch.req_seq = owner_id;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
create_branch(uint32_t timeline, uint32_t parent, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_CREATE_BRANCH;
	ch.timeline = timeline;
	ch.parent_timeline = parent;
	ch.req_lsn = lsn;
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
timeline_state(uint32_t timeline, PsTimelineState *state)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_TIMELINE_STATE;
	ch.timeline = timeline;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_lifecycle_read_unlock();
	if (ch.status != PS_STATUS_OK)
		return 0;
	*state = (PsTimelineState) ch.result;
	return 1;
}

static int
begin_delete(uint32_t timeline)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_BEGIN_DELETE;
	ch.timeline = timeline;
	ch.req_seq = 1;
	ch.status = PS_STATUS_OK;
	if (ps_lifecycle_write_lock() != 0)
		return 0;
	if (ps_admission_write_lock() != 0)
	{
		ps_lifecycle_write_unlock();
		return 0;
	}
	ps_lock_map_wr();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_admission_write_unlock();
	ps_lifecycle_write_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
wal_read_status(uint32_t timeline, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_READ;
	ch.timeline = timeline;
	ch.req_lsn = lsn;
	ch.datalen = 1;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status;
}

static unsigned int
segment_count(const char *store, uint32_t timeline)
{
	char path[512];
	char prefix[64];
	DIR *dir;
	struct dirent *entry;
	unsigned int count = 0;

	if (snprintf(path, sizeof(path), "%s/wal_segments_%u", store, timeline) < 0)
		return 0;
	if (snprintf(prefix, sizeof(prefix), "walv1_%u_", timeline + 1) < 0 ||
		(dir = opendir(path)) == NULL)
		return 0;
	while ((entry = readdir(dir)) != NULL)
		if (strncmp(entry->d_name, prefix, strlen(prefix)) == 0)
			count++;
	closedir(dir);
	return count;
}

static int
prepare_store(const char *store, uint64_t control_redo, uint64_t pin_lsn,
			  uint64_t index_lsn, int make_progress)
{
	if (mkdtemp((char *) store) == NULL || ps_core_open(store) != 0 ||
		!append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL))
		return 0;
	if (control_redo != 0 && !write_control(0, WAL_TOTAL, control_redo))
		return 0;
	if (pin_lsn != 0 && !set_wal_pin(0, 100, pin_lsn))
		return 0;
	if (index_lsn != 0 && !wal_index_add(0, index_lsn))
		return 0;
	if (make_progress && !wal_index_progress(0, 0, WAL_TOTAL))
		return 0;
	return 1;
}

static void
close_store(void)
{
	ps_core_close();
	if (ps_storage->close != NULL)
		ps_storage->close();
}

static int
maintenance_until_count(const char *store, uint32_t timeline,
						unsigned int wanted)
{
	/* The WAL reclaimer's no-progress backoff is now rate-limited to one
	 * re-evaluation per WAL_RECLAIM_REARM_MIN_NS (20 ms) regardless of epoch
	 * changes, so a caller driving maintenance right after an arm needs real
	 * wall-clock time to elapse before the next attempt is honored, not just
	 * more tight-loop iterations or a bounded iteration count: a store with
	 * other work pending (e.g. an unrelated forkmeta cutover step) can report
	 * work done on every single pass without ever idling, so an idle-only
	 * sleep never triggers and a fixed iteration budget's real wall-clock
	 * cost is scenario-dependent (observed both ~350 busy passes and, in a
	 * smaller store, still under 20 ms after 2048).  Bound this on wall-clock
	 * time directly instead: keep driving maintenance (sleeping briefly on an
	 * idle pass to make that time pass efficiently) until either the count is
	 * reached or a full second -- 50x the rate-limit floor -- has elapsed.
	 * The common case (no arm to wait out) still converges in well under a
	 * millisecond, since the loop exits as soon as the count is reached. */
	struct timespec t0, t1;

	clock_gettime(CLOCK_MONOTONIC, &t0);
	while (segment_count(store, timeline) > wanted)
	{
		if (!ps_core_maintenance())
			usleep(1000);
		clock_gettime(CLOCK_MONOTONIC, &t1);
		if ((double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
			(double) (t1.tv_nsec - t0.tv_nsec) / 1e6 >= 1000.0)
			break;
	}
	return segment_count(store, timeline) == wanted;
}

typedef struct ReclaimAttemptCounter
{
	unsigned int attempts;
} ReclaimAttemptCounter;

static void count_reclaim_attempt(uint32_t timeline, void *arg);
static void *maintenance_thread(void *arg);

static void
test_no_floor_or_progress(void)
{
	char store[] = "/tmp/pagestore-wal-policy-no-proof-XXXXXX";

	configure_core();
	check(prepare_store(store, 0, 0, 0, 0),
		  "construct WAL without control floor or durable progress");
	check(segment_count(store, 0) == WAL_SEGMENTS && ps_core_maintenance() == 0 &&
		  segment_count(store, 0) == WAL_SEGMENTS,
		  "no floor/progress does not delete a complete segment");
	close_store();
	remove_tree(store);
}

static void
test_preselection_skips_empty_reclaim(void)
{
	char store[] = "/tmp/pagestore-wal-policy-preselection-XXXXXX";
	AdmissionCallCounter counter = {0};

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, WAL_SEGMENT / 2),
		  "construct a WAL tail without a complete immutable segment");
	ps_test_set_admission_write_lock_hook(count_admission_call, &counter);
	check(ps_test_wal_reclaim_maintenance() == 0 && counter.calls == 0,
		  "cheap preselection skips the global drain when no segment is eligible");
	check(append_wal_bytes(0, WAL_SEGMENT / 2,
						 (uint32_t) (WAL_TOTAL - WAL_SEGMENT / 2)) &&
		  write_control(0, WAL_TOTAL, WAL_SEGMENT / 2) &&
		  wal_index_progress(0, 0, WAL_TOTAL) &&
		  ps_test_wal_reclaim_maintenance() == 0 && counter.calls == 1,
		  "a boundary-segment floor performs one full validation");
	check(ps_test_wal_reclaim_maintenance() == 0 && counter.calls == 1,
		  "boundary no-progress backoff skips repeated global drains");
	ps_test_set_admission_write_lock_hook(NULL, NULL);
	close_store();
	remove_tree(store);
}

/* The no-progress backoff exists to avoid repeating the global drain while
 * the candidate is genuinely unchanged; it must not make a proven segment
 * wait out its full second once an owner WAL pin -- a retention-registry
 * change -- releases the boundary.  It also must not be honored inside the
 * WAL_RECLAIM_REARM_MIN_NS (20 ms) rate-limit floor, which exists so a
 * drop-heavy client pinned on one stuck segment cannot turn every drop into a
 * full drain: the call immediately after the pin advance still returns 0,
 * and only a later call (bounded well under the 1 s clock, proving the
 * epoch-keyed cancellation actually fired rather than the timer) reclaims.
 * Without proof-keyed cancellation (Task 2) this needs sleep(2) before that
 * later call, exactly like test_failure_backoff. */
static void
test_no_progress_backoff_follows_proof(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-proof-backoff-XXXXXX";
	AdmissionCallCounter counter = {0};
	struct timespec t_arm, t0, t1;
	int reclaimed;
	double elapsed_ms = 0.0;
	double since_arm_ms;

	configure_core();
	check(prepare_store(store, WAL_TOTAL, limited, 0, 1),
		  "construct a WAL prefix whose control floor sits past the owner WAL pin");
	check(ps_test_wal_reclaim_maintenance() == 1 && segment_count(store, 0) == 2,
		  "reclaim advances to the aligned owner-pin boundary");
	ps_test_set_admission_write_lock_hook(count_admission_call, &counter);
	check(ps_test_wal_reclaim_maintenance() == 0 && counter.calls == 1 &&
		  segment_count(store, 0) == 2,
		  "the pin boundary is still current: the no-progress backoff arms");
	clock_gettime(CLOCK_MONOTONIC, &t_arm);
	check(ps_test_wal_reclaim_maintenance() == 0 && counter.calls == 1,
		  "an unexpired backoff with no proof-input change skips the global drain");
	ps_test_set_admission_write_lock_hook(NULL, NULL);
	check(set_wal_pin(0, 100, WAL_TOTAL),
		  "release the owner WAL pin to the end of the shipped log");
	/* set_wal_pin's two fsyncs (control-image plus retention publication) can
	 * themselves cost close to the 20 ms rate-limit floor on a loaded host,
	 * so only assert "not yet honored" when this check is still inside the
	 * floor measured from the arm above; a host slow enough to cross it
	 * before reaching here would make the assertion flake on timing that is
	 * not what this test is about (mirrors the fast/slow-host branch in
	 * test_epoch_retries_are_rate_limited). */
	clock_gettime(CLOCK_MONOTONIC, &t1);
	since_arm_ms = (double) (t1.tv_sec - t_arm.tv_sec) * 1000.0 +
		(double) (t1.tv_nsec - t_arm.tv_nsec) / 1e6;
	if (since_arm_ms < 20.0)
		check(ps_test_wal_reclaim_maintenance() == 0,
			  "fast host (< 20 ms since arm): the epoch change is not honored"
			  " before the rate-limit floor");
	else
		check(1,
			  "slow host (>= 20 ms since arm): skipping the not-yet-honored"
			  " assertion, the bounded poll below still proves the epoch"
			  " cancellation fired");
	reclaimed = 0;
	clock_gettime(CLOCK_MONOTONIC, &t0);
	for (;;)
	{
		if (ps_test_wal_reclaim_maintenance() == 1)
			reclaimed = 1;
		clock_gettime(CLOCK_MONOTONIC, &t1);
		elapsed_ms = (double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
			(double) (t1.tv_nsec - t0.tv_nsec) / 1e6;
		if (reclaimed || elapsed_ms >= 300.0)
			break;
		usleep(1000);
	}
	check(reclaimed && segment_count(store, 0) == 0 && elapsed_ms < 300.0,
		  "once the rate-limit floor passes, the epoch-cancelled retry"
		  " reclaims well under the 1 s clock");
	close_store();
	remove_tree(store);
}

/* WAL_RECLAIM_REARM_MIN_NS caps re-evaluation at one full R3b-3 drain per
 * 20 ms per timeline, regardless of how many proof-relevant events land in
 * that window: every re-evaluation is the whole drain (admission write lock,
 * all shard write locks, the raw-floor scan, the control-image floor read),
 * and PS_OP_RETENTION_PIN_DROP is dispatched without the admission lock, so
 * a drop-heavy client pinned on one stuck segment must not turn every drop
 * into a drain. ps_test_wal_reclaim_proof_changed() bumps the epoch directly
 * (as a real proof event would) without changing any proof input, so the
 * pin still blocks every attempt and only the drain count is under test. */
static void
test_epoch_retries_are_rate_limited(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-epoch-rate-XXXXXX";
	AdmissionCallCounter counter = {0};
	struct timespec t0, t1;
	double loop_ms;
	unsigned int calls_before;

	configure_core();
	check(prepare_store(store, WAL_TOTAL, limited, 0, 1),
		  "construct a WAL prefix reclaimable only up to the owner pin");
	check(ps_test_wal_reclaim_maintenance() == 1 && segment_count(store, 0) == 2,
		  "reclaim advances to the aligned owner-pin boundary");
	check(ps_test_wal_reclaim_maintenance() == 0 && segment_count(store, 0) == 2,
		  "the pin boundary is still current: the no-progress backoff arms");
	ps_test_set_admission_write_lock_hook(count_admission_call, &counter);
	clock_gettime(CLOCK_MONOTONIC, &t0);
	for (int i = 0; i < 20; i++)
	{
		ps_test_wal_reclaim_proof_changed();
		(void) ps_test_wal_reclaim_maintenance();
		check(counter.calls <= 1,
			  "the rate-limit floor admits at most one drain across repeated epoch bumps");
	}
	clock_gettime(CLOCK_MONOTONIC, &t1);
	loop_ms = (double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
		(double) (t1.tv_nsec - t0.tv_nsec) / 1e6;
	if (loop_ms < 20.0)
		check(counter.calls == 0,
			  "fast host (loop < 20 ms): the rate-limit floor admits zero drains");
	else
		check(1,
			  "slow host (loop >= 20 ms): skipping the zero-drain assertion, "
			  "the <= 1 invariant above still held every iteration");
	/* Read the count after the loop rather than assume it is 0: on a slow
	 * host the loop itself may already have crossed the floor and drained
	 * once (still consistent with the <= 1 invariant above).  Bump the
	 * epoch once more explicitly so there is always a fresh, unconsumed
	 * change for the floor-past call below to react to, and assert the
	 * drain count advances by exactly one, not that it lands on an
	 * absolute value. */
	calls_before = counter.calls;
	ps_test_wal_reclaim_proof_changed();
	usleep(25000);
	check(ps_test_wal_reclaim_maintenance() == 0 &&
		  counter.calls == calls_before + 1 &&
		  segment_count(store, 0) == 2,
		  "the pending epoch change is honored exactly once past the"
		  " rate-limit floor, but the pin still blocks the reclaim");
	ps_test_set_admission_write_lock_hook(NULL, NULL);
	close_store();
	remove_tree(store);
}

/* A complete segment blocked only by the stale raw WAL-index dependency
 * requests one compacted WAL-index publication (Task 1), instead of waiting
 * for the WAL-index controller's own tail trigger or high water.  This reuses
 * the setup of "a durable stored page base releases the raw WAL-index
 * dependency" above, but without PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES: the
 * default 1 MiB tail trigger is nowhere near reached by one record, so
 * nothing publishes on its own before Task 1. */
static void
test_stale_wal_index_requests_compaction(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-reclaim-due-XXXXXX";

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct a stored page base blocked only by the raw WAL-index dependency");
	check(ps_test_wal_reclaim_maintenance() == 1 && segment_count(store, 0) == 2,
		  "reclaim advances to the aligned raw-dependency boundary");
	check(ps_test_wal_reclaim_maintenance() == 0 &&
		  ps_test_walidx_reclaim_due(0) == 1,
		  "the stale raw dependency is the only thing blocking the segment: a compacted publication is requested");
	check(maintenance_until_count(store, 0, 0),
		  "the requested publication retires the raw dependency and the segment reclaims");
	check(ps_test_walidx_reclaim_due(0) == 0,
		  "the request is cleared once the publication it caused succeeds");
	close_store();
	remove_tree(store);
}

/* The sibling negative case: a stored page with no page-history owner cannot
 * authorize a replacement base, so the raw dependency is genuinely
 * unreplaceable and the segment stays blocked forever.  The request is
 * still self-limiting (walidx_snapshot_end[tl] < progress): it fires once
 * and does not storm every idle tick while durable progress does not
 * advance again. */
static void
test_unreplaceable_dependency_requests_once(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-reclaim-once-XXXXXX";
	char directory[512];
	uint64_t next_generation = 0;

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100),
		  "construct a stored page with no page-history owner to authorize replacement");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "the unreplaceable raw dependency still blocks the segment after 64 passes");
	check(ps_test_walidx_reclaim_due(0) == 0,
		  "the one-shot compaction request does not stay armed once its publication completes");
	check(snprintf(directory, sizeof(directory), "%s/walidx_snapshots_0", store) > 0 &&
		  ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
		  next_generation == 2,
		  "no publication storm: exactly one compacted publication while progress does not advance");
	/* Durable WAL-index progress is published once per indexing batch in
	 * production (continuously while WAL ships), so repeating the old
	 * progress-keyed request on every advance would be a sustained
	 * non-compacting rewrite for as long as this dependency stays
	 * unreplaceable.  A progress advance alone also can never retire the
	 * raw item (walidx_entry_prune_plan needs a durable base and no
	 * retained horizon; a higher cutoff changes neither), so the
	 * fence-keyed request must not re-fire here: fails on a branch that
	 * still gates on progress alone (generation grows to ~7). */
	{
		uint64_t end = WAL_TOTAL;

		for (int round = 0; round < 5; round++)
		{
			check(append_wal_bytes(0, end, 8192) &&
				  wal_index_progress(0, end, end + 8192),
				  "append and advance durable progress past the unreplaceable dependency");
			end += 8192;
			for (int pass = 0; pass < 8; pass++)
				(void) ps_core_maintenance();
		}
		check(segment_count(store, 0) == 2,
			  "repeated progress advances alone do not retire the unreplaceable raw dependency");
		check(ps_test_walidx_reclaim_due(0) == 0,
			  "no re-request while the raw floor and retention floor are both unchanged");
		check(ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
			  next_generation <= 2,
			  "progress advances alone produce no further compacted publication");
		check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
						  PS_RETENTION_RESOURCE_ALL, end) &&
			  maintenance_until_count(store, 0, 0),
			  "a fence change (the materializer pin, which also authorizes the"
			  " stored page as a base) re-issues the request and retires the chain");
		check(ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
			  next_generation <= 3,
			  "the fence-triggered request adds exactly one more compacted publication");
	}
	close_store();
	remove_tree(store);
}

/*
 * Residual 1a: a replacement base that becomes durable with no fence change
 * at all.  The reclaim-due request is served once, drops nothing (the base
 * is not yet durable), and becomes fruitless-suppressed; the watch this
 * fixes for arms on that same evaluation and fires once flush_memtable makes
 * the base durable, re-issuing the request without waiting for a fence
 * change or the WAL-index controller's own 1 MiB-tail trigger.  See
 * plan-reclaim-residuals.md section 1.  Before the fix (git stash the core
 * change): the negative-bound writes and the real base both leave the store
 * stuck at 2 segments for the whole 1 s maintenance_until_count window.
 */
static void
test_late_durable_base_requests_compaction(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-late-base-XXXXXX";
	char directory[512];
	uint64_t next_generation = 0;
	uint64_t before_negative;

	configure_core();
	flush_pages = 4;
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct a raw dependency whose horizon a materializer pin protects");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "segment 0 goes; segment 1 is blocked, and the request is served fruitless");
	check(ps_test_walidx_reclaim_due(0) == 0,
		  "the served publication dropped nothing and is fruitless-suppressed");
	check(snprintf(directory, sizeof(directory), "%s/walidx_snapshots_0", store) > 0 &&
		  ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0,
		  "read the snapshot generation before the watch is exercised");
	before_negative = next_generation;

	/* Negative bound first, while the store is still stuck at 2 segments and
	 * the watch is live: a version written and flushed durable *above* the
	 * watch's hi bound must not fire it.  Uses blocks other than 0-3, which
	 * the positive case below still needs untouched. */
	check(write_relation_page(0, 0, WAL_TOTAL + 4096),
		  "write block 0's base above the watched horizon");
	check(write_relation_page(0, 90, WAL_TOTAL + 5000) &&
		  write_relation_page(0, 91, WAL_TOTAL + 5100) &&
		  write_relation_page(0, 92, WAL_TOTAL + 5200),
		  "three more writes fill the memtable (flush_pages = 4) and flush it");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "a base above the horizon does not fire the watch");
	check(ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
		  next_generation == before_negative,
		  "no publication from a flush the watch does not care about");

	/* The real base for the blocking raw item, but memtable-resident: the
	 * fix must not fire before it is durable. */
	check(write_relation_page(0, 0, limited + 100),
		  "write the base that would retire the blocking raw item");
	{
		struct timespec t0, t1;
		double elapsed_ms;

		clock_gettime(CLOCK_MONOTONIC, &t0);
		do
		{
			(void) ps_core_maintenance();
			clock_gettime(CLOCK_MONOTONIC, &t1);
			elapsed_ms = (double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
				(double) (t1.tv_nsec - t0.tv_nsec) / 1e6;
		} while (elapsed_ms < 100.0);
	}
	check(segment_count(store, 0) == 2,
		  "a non-durable version is not a base; the fix must not fire early");
	check(ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
		  next_generation == before_negative,
		  "still no publication: the base has not reached a layer yet");

	/* Three more writes on other blocks fill the memtable and flush it,
	 * making the real base durable: the watch fires. */
	check(write_relation_page(0, 1, limited + 200) &&
		  write_relation_page(0, 2, limited + 300) &&
		  write_relation_page(0, 3, limited + 400),
		  "fill the memtable so flush_memtable runs");
	check(maintenance_until_count(store, 0, 0),
		  "the watch fired, the request was re-issued, and the segment reclaimed");
	check(ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
		  next_generation == before_negative + 1,
		  "exactly one more compacted publication from the watch firing");
	close_store();
	remove_tree(store);
}

/*
 * Residual 1b: the same shape for an unprotected horizon (a WAL_INDEX-only
 * pin, no page history), where the event that can shorten the chain is a
 * newer full-page-image item instead of a durable base.
 */
static void
test_late_fpi_requests_compaction(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-late-fpi-XXXXXX";
	char directory[512];
	uint64_t next_generation = 0;
	uint64_t before_fire;

	configure_core();
	flush_pages = 4;
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1) &&
		  wal_index_add_record(0, limited, 0, /* fpi */ 0) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 301, 1,
					  PS_RETENTION_RESOURCE_WAL_INDEX, WAL_TOTAL),
		  "construct a non-FPI raw item whose only horizon is an unprotected"
		  " WAL_INDEX-only pin");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "segment 0 goes; segment 1 is blocked, and the request is served fruitless");
	check(ps_test_walidx_reclaim_due(0) == 0,
		  "the served publication dropped nothing and is fruitless-suppressed");
	check(wal_index_count(0, 0, WAL_TOTAL) == 1,
		  "one raw item (the non-FPI known record) indexes the block so far");
	check(snprintf(directory, sizeof(directory), "%s/walidx_snapshots_0", store) > 0 &&
		  ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0,
		  "read the snapshot generation before the watch is exercised");
	before_fire = next_generation;

	check(wal_index_add_record(0, limited + 4096, 0, /* fpi */ 1),
		  "a newer FPI item for the same block arrives within the watched window");
	/* The reader's WAL_INDEX-only pin still needs *some* record to
	 * reconstruct the page as of its horizon, so the newer FPI item itself
	 * remains indexed (it is now the chain's own anchor) -- the segment does
	 * not reclaim further on this alone.  What the fix buys back is that the
	 * now-superseded older record is retired *immediately* on the FPI's
	 * arrival, via the watch's own re-issued request, instead of waiting
	 * for the WAL-index controller's unrelated 1 MiB-tail trigger.  Drive
	 * for the full no-progress window (bounded, not open-ended: the request
	 * is still one-shot per event, matching test_unreplaceable_dependency's
	 * own bound) and check the retirement and the single extra publication
	 * directly, since segment_count alone cannot observe it here. */
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "the reader's pin still blocks the segment; expected, not the fix's job");
	check(wal_index_count(0, 0, WAL_TOTAL) == 1,
		  "the watch fired: the superseded non-FPI record was retired,"
		  " leaving only the new FPI item; before the fix, still 2");
	check(ps_walidx_snapshot_next_generation(directory, 0, &next_generation) == 0 &&
		  next_generation >= before_fire + 1,
		  "at least one more compacted publication from the watch firing"
		  " (the controller may also publish on its own schedule during the"
		  " 1 s drive; the fix's contribution is the retirement asserted"
		  " above, which does not happen at all without it)");
	close_store();
	remove_tree(store);
}

/*
 * Residual 2: a superseded control note invisible to compaction while
 * memtable-resident.  wal_retain_floor_level (part of retention_floor's WAL
 * computation) counts every retained note, durable or not; compaction only
 * reads image layers.  When retention_floor's note term alone holds a
 * segment boundary and the note that sets it is superseded (not the newest
 * note at or below any live fence) but still memtable-resident, the
 * reclaimer requests a flush of the control shard so the very next
 * compaction pass can prune it.  Also the twin-rule regression guard: a
 * note that *is* required by a live fence must never be flushed or pruned
 * on the reclaimer's behalf.  See plan-reclaim-residuals.md section 2.
 */
static void
test_superseded_note_in_memtable_is_pruned(void)
{
	char store[] = "/tmp/pagestore-wal-policy-note-flush-XXXXXX";
	char store2[] = "/tmp/pagestore-wal-policy-note-required-XXXXXX";
	PsKey control_key;

	memset(&control_key, 0, sizeof(control_key));
	control_key.klass = PS_KLASS_CONTROL;

	configure_core();
	flush_pages = 64;
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_SEGMENT + 4096, WAL_SEGMENT + 4096) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 100, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_SEGMENT + 8192) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  wal_index_progress(0, 0, WAL_TOTAL),
		  "note A (redo in segment 1), a fence between A and B, then note B");
	check(maintenance_until_count(store, 0, 2),
		  "segment 0 goes; segment 1 is held by the reader and by note A"
		  " (the newest note at or below its fence)");
	check(drop_wal_pin(0, 100, 1), "drop the reader pin: note A is now superseded");
	check(maintenance_until_count(store, 0, 0),
		  "the requested flush lands, compaction prunes A, and the floor"
		  " becomes B's redo; before the fix, stuck at 2 for the whole 1 s"
		  " window (A stays memtable-resident: flush_pages = 64 and only 4"
		  " control pages were ever written)");
	close_store();
	remove_tree(store);

	/* Twin-rule regression guard, same construction, reader pin left live:
	 * note A is still required (the newest note at or below the reader's
	 * fence) and must not be pruned or flushed on the reclaimer's behalf. */
	configure_core();
	flush_pages = 64;
	check(mkdtemp(store2) != NULL && ps_core_open(store2) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_SEGMENT + 4096, WAL_SEGMENT + 4096) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 100, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_SEGMENT + 8192) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  wal_index_progress(0, 0, WAL_TOTAL),
		  "the same construction, reader pin left live this time");
	check(maintenance_until_count(store2, 0, 2),
		  "segment 0 goes; segment 1 is held by the reader and by note A");
	check(!maintenance_until_count(store2, 0, 0) && segment_count(store2, 0) == 2,
		  "note A is required by the live reader fence and must stay put");
	check(ps_test_page_version_count(0, &control_key, PS_REDO_NOTE_BLOCK) == 2,
		  "both notes survive: the reclaimer's request never touches a"
		  " required note");
	close_store();
	remove_tree(store2);
}

/*
 * Review HIGH-1, safety-net half 1 (probe review_gap_base_durable_between_
 * evaluations): the watch armed at a fruitless evaluation fires from the
 * fire site the write passes through even when the base and the writes that
 * fill its memtable happen back to back, with no maintenance call at all in
 * between (so the watch cannot be re-armed a second time before the flush
 * lands) -- the arm from the *first* fruitless evaluation must still be the
 * one that fires.  Bounded to the 20 ms fire path, not the 1 s fallback.
 */
static void
test_base_durable_between_evaluations(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-base-between-XXXXXX";
	struct timespec t0,
				t1;
	double		elapsed_ms;

	configure_core();
	flush_pages = 4;
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct a raw dependency whose horizon a materializer pin protects");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "segment 0 goes; segment 1 is blocked, and the request is served fruitless");
	check(ps_test_walidx_reclaim_due(0) == 0,
		  "the served publication dropped nothing and is fruitless-suppressed");
	check(write_relation_page(0, 0, limited + 100) &&
		  write_relation_page(0, 1, limited + 200) &&
		  write_relation_page(0, 2, limited + 300) &&
		  write_relation_page(0, 3, limited + 400),
		  "the base and three fillers, written and flushed durable in one go,"
		  " with no maintenance call in between");
	clock_gettime(CLOCK_MONOTONIC, &t0);
	check(maintenance_until_count(store, 0, 0),
		  "[design goal] the watch armed at the earlier fruitless evaluation"
		  " fires on the flush the fillers triggered, reclaiming the segment");
	clock_gettime(CLOCK_MONOTONIC, &t1);
	elapsed_ms = (double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
		(double) (t1.tv_nsec - t0.tv_nsec) / 1e6;
	check(elapsed_ms < 700.0,
		  "the 20 ms fire path reclaims well under the 1 s no-progress"
		  " fallback bound (before the fix: stuck for the whole window)");
	check(maintenance_until_count(store, 0, 0), "... stays reclaimed");
	close_store();
	remove_tree(store);
}

/*
 * Review HIGH-1, safety-net half 2 (probe review_gap_base_durable_at_write):
 * flush_pages == 1, so the base is durable at the write itself and the fire
 * site runs inside that same write's flush.  The second half proves the
 * *evaluation's own* retirement-evidence recomputation -- not any fire -- is
 * what actually closes the gap: with both fire sites suppressed via the
 * test hook, the base still gets picked up, just by the next fruitless
 * evaluation deriving fresh evidence from scratch, at the latest after the
 * 1 s no-progress fallback (there is no fire left to shorten it to 20 ms).
 */
static void
test_base_durable_at_write(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-base-at-write-XXXXXX";
	char store2[] = "/tmp/pagestore-wal-policy-base-at-write2-XXXXXX";

	configure_core();
	/* flush_pages stays at configure_core()'s default of 1. */
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct a raw dependency whose horizon a materializer pin protects");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "segment 0 goes; segment 1 is blocked, and the request is served fruitless");
	check(write_relation_page(0, 0, limited + 100),
		  "the base is durable at the write itself (flush_pages == 1)");
	check(maintenance_until_count(store, 0, 0),
		  "the fire site inside the write's own flush reclaims within the"
		  " first 1 s window");
	close_store();
	remove_tree(store);

	configure_core();
	check(prepare_store(store2, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct the same scenario again for the safety-net half");
	check(!maintenance_until_count(store2, 0, 0) && segment_count(store2, 0) == 2,
		  "served fruitless again");
	ps_test_set_wal_reclaim_watch_fire_hook(1);
	check(write_relation_page(0, 0, limited + 100),
		  "the base is durable at the write itself, but the fire site is suppressed");
	check(maintenance_until_count(store2, 0, 0),
		  "[design goal] the evaluation's own evidence recomputation still"
		  " reclaims, with no fire at all");
	ps_test_set_wal_reclaim_watch_fire_hook(0);
	close_store();
	remove_tree(store2);
}

/*
 * Review MEDIUM/cost probe (review_cost_unrelated_shard_flush): with the
 * watch armed (the base is memtable-resident, observed by an evaluation), a
 * flush of an UNRELATED shard must not fire it or cause a publication.
 * Kills mutation 1 (any-flush fire: matching the watched entry's shard and
 * durability dropped from the BASE-kind check).
 */
static void
test_watch_ignores_unrelated_shard_flush(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-cost-XXXXXX";
	char directory[512];
	uint64_t	gen0 = 0,
				gen1 = 0;
	PsKey		base_key = {1, 1, 1, 0, PS_KLASS_RELATION};
	uint32_t	base_shard,
				other_rel = 0;

	configure_core();
	ps_nshards = 4;
	flush_pages = 4;
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct (4 shards)");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "served fruitless, stuck at 2");
	base_shard = ps_shard_of(&base_key);
	for (uint32_t r = 2; r < 64; r++)
	{
		PsKey		k = {1, 1, r, 0, PS_KLASS_RELATION};

		if (ps_shard_of(&k) != base_shard)
		{
			other_rel = r;
			break;
		}
	}
	check(other_rel != 0, "found a relation hashing to another shard");
	check(write_relation_page(0, 0, limited + 100), "base in memtable");
	/* let an evaluation observe the pending base and arm the watch */
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "still stuck (base not durable)");
	check(ps_test_wal_reclaim_watch_count(0) == 1, "watch armed with one entry");
	check(snprintf(directory, sizeof(directory), "%s/walidx_snapshots_0", store) > 0 &&
		  ps_walidx_snapshot_next_generation(directory, 0, &gen0) == 0, "gen0");
	for (uint32_t i = 0; i < 16; i++)
		check(write_page_key(0, other_rel, 10 + i, WAL_TOTAL + 8192 * (i + 1)),
			  "unrelated write");
	/* The unrelated-shard writes above cross flush_pages (4) several times,
	 * calling wal_reclaim_watch_fire_flush() synchronously and immediately --
	 * before any maintenance/evaluation call runs and could re-arm a watch a
	 * bug clears.  Check the watch count right here, in that narrow window,
	 * so a shard/version check dropped from the BASE-kind match (matching on
	 * kind alone) is caught even though the fruitless-evaluation safety net
	 * would otherwise re-arm it and hide the bug by test end. */
	check(ps_test_wal_reclaim_watch_count(0) == 1,
		  "[bound] watch untouched immediately after unrelated-shard flushes,"
		  " before any evaluation could re-arm it");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "still stuck after unrelated flushes");
	check(ps_walidx_snapshot_next_generation(directory, 0, &gen1) == 0 && gen1 == gen0,
		  "[bound] no publication from unrelated-shard flushes");
	check(ps_test_wal_reclaim_watch_count(0) == 1, "watch still armed");
	close_store();
	remove_tree(store);
}

/*
 * Review HIGH-2 (review_churn_twin_note): a control-note churn guard.  Two
 * checkpoints share a twin relationship (the second note's redo points back
 * at the first note's own version), and a live fence keeps the second
 * checkpoint required; the first checkpoint's note, considered alone by the
 * fence-based superseded test, looks superseded (it is not the newest note
 * at or below the fence -- the second checkpoint is), the exact case the
 * plan accepts not mirroring the twin rule for.  This must cost at most one
 * requested flush and one compaction pass over an idle window, not a
 * compaction every evaluation: the (note identity, fence epoch) key in
 * walidx_reclaim_control_request must suppress every repeat.  Kills
 * mutation 2 (page_prune_due set unkeyed, every evaluation).
 */
static void
test_control_flush_not_repeated(void)
{
	char		store[] = "/tmp/pagestore-wal-policy-twin-churn-XXXXXX";
	uint64_t	compact_before;
	uint64_t	compact_after;
	uint64_t	stored_before;
	uint64_t	stored_after;
	uint64_t	wanted_before;
	uint64_t	wanted_after;
	PsKey		control_key;
	uint32_t	control_shard;
	struct timespec t0,
				t1;
	double		elapsed_ms;

	memset(&control_key, 0, sizeof(control_key));
	control_key.klass = PS_KLASS_CONTROL;

	configure_core();
	control_shard = ps_shard_of(&control_key);
	flush_pages = 64;
	compact_layers = 8;
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  /* A: checkpoint 1, self-redo -- this is the note the fence-based
		   * superseded check alone will judge superseded (A2, not A, is the
		   * newest note at or below the fence below), even though compaction
		   * keeps it as A2's exact-redo twin. */
		  write_control(0, WAL_SEGMENT + 4096, WAL_SEGMENT + 4096) &&
		  /* A2: a second checkpoint shortly after A, redo points back at A --
		   * A2 is the newest note at or below the fence and is required by
		   * the ordinary rule; its redo makes A a twin. */
		  write_control(0, WAL_SEGMENT + 6000, WAL_SEGMENT + 4096) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 100, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_SEGMENT + 8192) &&
		  /* B: checkpoint 2, self-redo, above the fence. */
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  wal_index_progress(0, 0, WAL_TOTAL),
		  "checkpoint A (self-redo), checkpoint A2 shortly after (redo = A,"
		  " a twin of A), a fence between A2 and B, checkpoint B (self-redo)");
	compact_before = ps_test_compaction_count();
	stored_before = ps_test_control_flush_stored();
	wanted_before = ps_test_control_flush_wanted();
	/* One explicit call: the first evaluation to see the twin-churn state
	 * finds the note superseded-per-fence and not yet durable, stores the
	 * request (a fresh key), and sets page_flush_requested together with
	 * page_prune_due for the control shard -- before compaction, which
	 * consumes page_prune_due, gets a separate later call of its own. */
	(void) ps_core_maintenance();
	check(ps_test_page_prune_due(0, control_shard) != 0,
		  "the stored request marks the control shard's page-prune-due flag"
		  " together with it, per the LOW that ties the two");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "segment 0 goes; segment 1 is held by the reader's fence, and the"
		  " control-flush decision has had a full window to run at least once");
	clock_gettime(CLOCK_MONOTONIC, &t0);
	do
	{
		(void) ps_core_maintenance();
		clock_gettime(CLOCK_MONOTONIC, &t1);
		elapsed_ms = (double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
			(double) (t1.tv_nsec - t0.tv_nsec) / 1e6;
	} while (elapsed_ms < 5000.0);
	compact_after = ps_test_compaction_count();
	stored_after = ps_test_control_flush_stored();
	wanted_after = ps_test_control_flush_wanted();
	check(wanted_after - wanted_before >= stored_after - stored_before &&
		  stored_after - stored_before >= 1,
		  "the predicate was found true (wanted) at least as often as it was"
		  " actually stored, and stored at least once");
	check(compact_after - compact_before == 1,
		  "[bound] the control-flush request and the compaction it enables"
		  " fire exactly once over 5 s of idle maintenance -- not zero (the"
		  " fix must still request the flush for a note the fence-based"
		  " check alone judges superseded) and not repeatedly (the (note"
		  " identity, fence epoch) key must suppress every further request"
		  " for the same unchanged state; before the fix: unkeyed, once per"
		  " NOPROGRESS evaluation)");
	/* Precise dedup-key proof, independent of compaction's own coalescing:
	 * the predicate may be evaluated true many times before the note lands
	 * durably, but the (note lsn, admission_seq, fence_epoch) key must have
	 * let only one of those evaluations actually store the request. */
	check(stored_after - stored_before == 1,
		  "[bound] the control-flush request is stored exactly once for the"
		  " unchanged (note lsn, admission_seq, fence_epoch) key -- an"
		  " unkeyed store would re-store it on every evaluation the"
		  " predicate holds for");
	close_store();
	remove_tree(store);
}

/*
 * Residual 1, protected-horizon FPI variant (test 5 in the review's list):
 * the same late-FPI shape as test_late_fpi_requests_compaction, but with a
 * materializer pin (a protected horizon) instead of a WAL_INDEX-only pin.
 * A protected horizon's watch kind is BASE|FPI (ps_walidx_prune_plan_bases
 * takes start = max(first record above the base, newest FPI)), so a newer
 * FPI must still retire the chain even though a durable base could also
 * have done it -- the case the review flagged as "BASE armed where an FPI
 * is needed" before MEDIUM-1 unified the protected-set construction.
 */
static void
test_late_fpi_requests_compaction_protected_horizon(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char		store[] = "/tmp/pagestore-wal-policy-late-fpi-prot-XXXXXX";

	configure_core();
	flush_pages = 4;
	/* Progress is set past WAL_TOTAL (a durable WAL-index frontier beyond
	 * the sealed prefix is a supported case) precisely so it does not equal
	 * the materializer pin's LSN: walidx_protected_horizons_build's
	 * standing-horizon exception excludes a materializer grant whenever its
	 * LSN coincides with the shipper's progress (or the durable frontier),
	 * which would otherwise make this horizon unprotected instead -- see
	 * test_late_fpi_requests_compaction for that (correct) unprotected
	 * case. */
	check(prepare_store(store, WAL_TOTAL, 0, 0, 0) &&
		  wal_index_add_record(0, limited, 0, /* fpi */ 0) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL) &&
		  append_wal_bytes(0, WAL_TOTAL, 8192) &&
		  wal_index_progress(0, 0, WAL_TOTAL + 8192),
		  "construct a non-FPI raw item whose horizon a materializer pin protects");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "segment 0 goes; segment 1 is blocked, and the request is served fruitless");
	check(wal_index_count(0, 0, WAL_TOTAL + 8192) == 1,
		  "one raw item (the non-FPI known record) indexes the block so far");
	check(wal_index_add_record(0, limited + 4096, 0, /* fpi */ 1),
		  "a newer FPI item for the same block arrives within the watched window");
	/* The materializer pin still needs *some* record to reconstruct the page
	 * as of its horizon, so the newer FPI item itself remains indexed and
	 * the segment does not reclaim further on this alone (same alignment
	 * shape as the unprotected case above): what the fix buys back is that
	 * the watch, armed BASE|FPI for this protected horizon, retires the
	 * superseded older record immediately on the FPI's arrival instead of
	 * waiting for the WAL-index controller's own trigger. */
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "the materializer pin still blocks the segment; expected, not the fix's job");
	check(wal_index_count(0, 0, WAL_TOTAL + 8192) == 1,
		  "[design goal] a protected horizon's watch is BASE|FPI: the FPI"
		  " arrival retired the superseded older record; before the fix,"
		  " still 2 (a durable-base-only watch never matches an FPI arrival)");
	close_store();
	remove_tree(store);
}

/* Round-2 review, NEW-HIGH-A: a protected page-history/walidx fence F below
 * the unprotected durable-progress horizon (the production shape -- most
 * pins sit well below progress).  retain_chain applies per horizon: the
 * item retires iff (base or FPI in [lo,F]) AND (FPI in [lo,progress]).  A
 * durable base at write (flush_pages == 1) satisfies F immediately; the
 * newer FPI that arrives strictly between F and progress satisfies
 * progress.  An earlier version of the evidence/watch computed a single
 * nearest-horizon window (h_cap = min(progress, nearest fence)) and used it
 * for both base and FPI evidence: since F is nearer than progress, h_cap
 * landed on F, and the FPI arriving above F matched no window at all --
 * the item was in fact retirable (a control case with the same state but
 * the FPI already present before the first evaluation proves the plan
 * retires it), but the watch/evidence mechanism never saw it, leaving the
 * segment to wait for the WAL-index controller's own unrelated trigger,
 * exactly the pre-PR gap this residual was meant to close. */
static void
test_fpi_between_protected_and_progress_retires(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	const uint64_t F = limited + 8192;
	char		store[] = "/tmp/pagestore-wal-policy-r2a-XXXXXX";

	configure_core();			/* flush_pages == 1: a base is durable at write */
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 300, 1,
					  PS_RETENTION_RESOURCE_PAGE_HISTORY |
					  PS_RETENTION_RESOURCE_WAL_INDEX, F) &&
		  write_relation_page(0, 0, limited + 100),
		  "a known WAL-index item at `limited`, a page-history+walidx pin at"
		  " F (protected, below progress), a durable base below F");
	check(!maintenance_until_count(store, 0, 1) && segment_count(store, 0) == 2,
		  "stuck at 2: F is served by the durable base, but the unprotected"
		  " progress horizon still needs an FPI, so segment 1 cannot go"
		  " either (it holds the item this reader pin still needs)");
	check(ps_test_walidx_reclaim_due(0) == 0, "served fruitless");
	check(wal_index_add_record(0, 2 * WAL_SEGMENT + 4096, 0, /* fpi */ 1),
		  "a newer FPI for the same block arrives strictly between F and"
		  " progress -- not in [lo,F], but exactly in (F,progress]");
	check(maintenance_until_count(store, 0, 1),
		  "[design goal] both windows are now satisfied (base in [lo,F], FPI"
		  " in [lo,progress]): segment 1 must reclaim within the window --"
		  " before the fix, an FPI landing between the nearest protected"
		  " fence and progress matched neither window, and the segment"
		  " waited for the WAL-index controller's own unrelated trigger"
		  " instead");
	close_store();
	remove_tree(store);
}

/* Round-2 review, NEW-HIGH-B: the arm is unconditional, but a fire site
 * that matches ANY qualifying version/item in [lo,hi] -- including one
 * already reflected in the evidence at arm time -- refires on every later,
 * unrelated flush of the watched page's shard: the v1 per-flush-drain
 * pathology this design was meant to avoid.  A base already durable in
 * [lo,F] (as in the previous test, before the FPI arrives) cannot be
 * improved by a later flush of some other page that happens to hash to the
 * same shard, so a BASE-kind watch must not still match those flushes.
 * Count full reclaimer evaluations (the before-floor hook fires once per
 * timeline actually reached) across 30 unrelated same-shard flushes spread
 * over ~750 ms: unbounded re-firing costs one evaluation per flush (30);
 * with re-firing correctly suppressed once evidence is satisfied, only the
 * ordinary rate-limited/idle-fallback schedule applies. */
static void
count_eval(uint32_t timeline, void *arg)
{
	(void) timeline;
	(*(unsigned int *) arg)++;
}

static void
test_watch_does_not_refire_on_satisfied_base_evidence(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	const uint64_t F = limited + 8192;
	char		store[] = "/tmp/pagestore-wal-policy-r2b-XXXXXX";
	unsigned int evals = 0;

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 300, 1,
					  PS_RETENTION_RESOURCE_PAGE_HISTORY |
					  PS_RETENTION_RESOURCE_WAL_INDEX, F) &&
		  write_relation_page(0, 0, limited + 100),
		  "same construction as the previous test: base durable in [lo,F],"
		  " item still stuck on progress's own FPI requirement");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "stuck at 2, request served fruitless");
	check(ps_test_wal_reclaim_watch_count(0) == 1, "watch armed (1 entry)");
	ps_test_set_wal_reclaim_before_floor_hook(count_eval, &evals);
	for (uint32_t i = 0; i < 30; i++)
	{
		struct timespec t0, t1;

		check(write_page_key(0, 7, 100 + i, WAL_TOTAL + 8192 * (i + 1)),
			  "unrelated write to a different key, own flush (flush_pages == 1)");
		clock_gettime(CLOCK_MONOTONIC, &t0);
		do
		{
			if (!ps_core_maintenance())
				usleep(1000);
			clock_gettime(CLOCK_MONOTONIC, &t1);
		} while ((double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
				 (double) (t1.tv_nsec - t0.tv_nsec) / 1e6 < 25.0);
	}
	ps_test_set_wal_reclaim_before_floor_hook(NULL, NULL);
	check(evals <= 2,
		  "[bound] 30 unrelated same-shard flushes over ~750 ms must not"
		  " drain the reclaimer once per flush -- a base already found in"
		  " the watched window cannot be improved by an unrelated flush, so"
		  " re-arming BASE for it is pure cost with no effect on the"
		  " answer; before the fix, every flush fired the watch and"
		  " cancelled the no-progress backoff, so evals tracked the flush"
		  " count instead of the reclaimer's own rate limit");
	close_store();
	remove_tree(store);
}

/* A WAL_INDEX-only pin is not caught by page_prune_mark_all_due's
 * (PAGE_HISTORY|WAL) test, but walidx_prune_fences still fences the
 * compaction plan on every WAL_INDEX pin: dropping or moving a WAL_INDEX-only
 * pin can turn an unreplaceable chain into a replaceable one exactly like a
 * PAGE_HISTORY/WAL pin change does, so it must also bump
 * walidx_reclaim_fence_epoch (and the backoff's proof epoch), or the
 * reclaimer stays fruitless-suppressed until the WAL-index controller's own
 * trigger notices independently. */
static void
test_walidx_only_pin_drop_requests_compaction(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-walidx-pin-XXXXXX";

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 301, 1,
					  PS_RETENTION_RESOURCE_WAL_INDEX, limited + 200) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL),
		  "construct a stored page base whose chain a WAL_INDEX-only reader still fences");
	check(ps_test_wal_reclaim_maintenance() == 1 && segment_count(store, 0) == 2,
		  "reclaim advances to the aligned raw-dependency boundary");
	check(ps_test_wal_reclaim_maintenance() == 0 &&
		  ps_test_walidx_reclaim_due(0) == 1,
		  "the stale raw dependency looks compactable, so a publication is requested");
	check(!maintenance_until_count(store, 0, 0) && segment_count(store, 0) == 2,
		  "the WAL_INDEX-only reader still fences the plan after that publication");
	check(ps_test_walidx_reclaim_due(0) == 0,
		  "the one-shot request does not stay armed once its fruitless publication completes");
	check(drop_wal_pin(0, 301, 1),
		  "drop the WAL_INDEX-only reader pin, with no durable-progress op at all");
	check(maintenance_until_count(store, 0, 0),
		  "the dropped WAL_INDEX pin re-issues the request and the chain now retires");
	close_store();
	remove_tree(store);
}

/* Runs synchronously inside wal_segment_reclaim_one, after the attempt's own
 * progress was already captured (wal_reclaim_walidx_state_valid, before this
 * hook point) but before wal_reclaim_backoff is called.  Advancing durable
 * progress here, and so bumping the proof epoch, while the in-flight
 * attempt's own decision is still based on the older value reproduces the
 * ordering the lost-wakeup fix protects against. */
static void
advance_progress_before_floor(uint32_t timeline, void *arg)
{
	const uint64_t *end = arg;

	(void) timeline;
	check(wal_index_progress(0, WAL_SEGMENT, *end),
		  "advance durable progress from inside the floor-scan hook");
}

/* wal_reclaim_backoff must record the proof epoch as it stood before this
 * attempt read any proof input, not a fresh read at arm time: a fresh read
 * can race with a proof-relevant event that lands after the stale input read
 * but before the backoff is armed (here, forced synchronously via the
 * floor-scan hook; in production, a PS_OP_RETENTION_PIN_DROP dispatched
 * without the admission lock while this attempt has released
 * walidx_prune_lock/wal_lock for the retention_effective_floor scan).  If the
 * epoch is read fresh, it already reflects that event, so the next attempt
 * finds no mismatch and waits out the full one-second backoff instead of
 * being due as soon as the WAL_RECLAIM_REARM_MIN_NS rate-limit floor passes
 * -- the store would stay at 2 segments through the bounded poll below
 * instead of reclaiming to 0 well inside it. */
static void
test_backoff_epoch_predates_attempt_inputs(void)
{
	char store[] = "/tmp/pagestore-wal-policy-epoch-order-XXXXXX";
	uint64_t end = WAL_TOTAL;
	struct timespec t0, t1;
	int reclaimed;
	double elapsed_ms = 0.0;

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, 0, 0) &&
		  wal_index_progress(0, 0, WAL_SEGMENT),
		  "construct a WAL prefix whose durable progress covers only the first segment");
	check(ps_test_wal_reclaim_maintenance() == 1 && segment_count(store, 0) == 2,
		  "reclaim advances to the aligned progress boundary");
	ps_test_set_wal_reclaim_before_floor_hook(advance_progress_before_floor, &end);
	check(ps_test_wal_reclaim_maintenance() == 0 && segment_count(store, 0) == 2,
		  "this attempt's own progress read predates the hook's later advance");
	ps_test_set_wal_reclaim_before_floor_hook(NULL, NULL);
	reclaimed = 0;
	clock_gettime(CLOCK_MONOTONIC, &t0);
	for (;;)
	{
		if (ps_test_wal_reclaim_maintenance() == 1)
			reclaimed = 1;
		clock_gettime(CLOCK_MONOTONIC, &t1);
		elapsed_ms = (double) (t1.tv_sec - t0.tv_sec) * 1000.0 +
			(double) (t1.tv_nsec - t0.tv_nsec) / 1e6;
		if (reclaimed || elapsed_ms >= 300.0)
			break;
		usleep(1000);
	}
	check(reclaimed && segment_count(store, 0) == 0 && elapsed_ms < 300.0,
		  "the epoch snapshotted before this attempt's inputs makes the advance"
		  " due once the rate-limit floor passes, well under the 1 s clock");
	close_store();
	remove_tree(store);
}

static void
test_dependency_cutoffs(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-dependency-XXXXXX";

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1) &&
		  wal_floor(0) == WAL_TOTAL && maintenance_until_count(store, 0, 0),
		  "all proof at the end permits reclaim of the complete prefix");
	close_store();
	remove_tree(store);

	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(prepare_store(store, limited, 0, 0, 1) &&
		  wal_floor(0) == limited && maintenance_until_count(store, 0, 2),
		  "control floor limits reclaim to the aligned safe prefix");
	close_store();
	remove_tree(store);

	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(prepare_store(store, WAL_TOTAL, limited, 0, 1) &&
		  effective_floor(0, PS_RETENTION_RESOURCE_WAL) == limited &&
		  maintenance_until_count(store, 0, 2),
		  "owner WAL pin limits reclaim to its aligned dependency");
	close_store();
	remove_tree(store);

	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  create_branch(1, 0, limited) &&
		  wal_index_progress(0, 0, WAL_TOTAL) &&
		  maintenance_until_count(store, 0, 2),
		  "branch cap limits reclaim without treating a discrete base as a range");
	close_store();
	remove_tree(store);

	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(prepare_store(store, WAL_TOTAL, WAL_TOTAL, limited, 1) &&
		  wal_floor(0) == WAL_TOTAL && maintenance_until_count(store, 0, 2),
		  "WAL-index raw dependency limits reclaim");
	close_store();
	remove_tree(store);

	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  create_branch(1, 0, limited) &&
		  write_control(1, WAL_TOTAL, 1) &&
		  wal_index_progress(0, 0, WAL_TOTAL) &&
		  maintenance_until_count(store, 0, 2),
		  "child-local control history above its branch cap does not pin parent WAL");
	close_store();
	remove_tree(store);

	/* The same raw dependency is released once the indexed page has a durable
	 * stored version at or after the record and the materializer's cutoff
	 * lets WAL-index compaction replace the FPI chain with that base. */
	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL) &&
		  maintenance_until_count(store, 0, 0),
		  "a durable stored page base releases the raw WAL-index dependency");
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0,
		  "clear the WAL-index snapshot trigger override");
	close_store();
	remove_tree(store);

	/* With no page-history owner nothing protects a stored version for a
	 * later owner's horizon, so the FPI chain is kept and the dependency
	 * stays. */
	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  !maintenance_until_count(store, 0, 0) &&
		  segment_count(store, 0) == 2,
		  "a stored page without any page-history owner is not a base");
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0,
		  "clear the WAL-index snapshot trigger override again");
	close_store();
	remove_tree(store);

	/* A WAL-index-only owner is not protected by page-history retention, so
	 * its horizon keeps the FPI-led chain even though a stored image exists;
	 * the raw dependency therefore stays. */
	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 301, 1,
					  PS_RETENTION_RESOURCE_WAL_INDEX, limited + 200) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL) &&
		  !maintenance_until_count(store, 0, 0) &&
		  segment_count(store, 0) == 2,
		  "a WAL-index-only owner keeps its FPI chain despite a stored image");
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0,
		  "clear the WAL-index snapshot trigger override after the WAL-index owner");
	close_store();
	remove_tree(store);

	/* A materializer pins only WAL resources, but its pin LSN (the redo of its
	 * last durable restartpoint) is the operational page-history cutoff, so
	 * the stored base at its own horizon is retained by that same pin and
	 * may authorize the replacement -- except where the pin coincides with a
	 * standing horizon: the shipper's progress admits a new WAL-index-only
	 * owner at exactly its LSN, which would arrive after the materializer
	 * advanced and the base was retired, so a materializer pinned there
	 * keeps its FPI-led chain.  (This harness indexes through the whole
	 * shipped WAL, so the only materializer pin that could free every
	 * segment is one at the progress; the exception's positive effect is
	 * not representable here.) */
	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, WAL_TOTAL) &&
		  !maintenance_until_count(store, 0, 0) &&
		  segment_count(store, 0) == 2,
		  "a materializer pinned at the shipper's progress keeps its FPI-led chain");
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0,
		  "clear the WAL-index snapshot trigger override after the WAL-only pin");
	close_store();
	remove_tree(store);

	/* Protection belongs to the horizon's own owner: a page-history owner at
	 * the same LSN as a WAL-index-only owner may advance or drop its pin
	 * first, so it does not authorize the base for the other owner's
	 * horizon and the FPI chain stays. */
	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  write_relation_page(0, 0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 301, 1,
					  PS_RETENTION_RESOURCE_WAL_INDEX, WAL_TOTAL) &&
		  !maintenance_until_count(store, 0, 0) &&
		  segment_count(store, 0) == 2,
		  "another owner's page fence at the same LSN does not authorize the base");
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0,
		  "clear the WAL-index snapshot trigger override after the shared LSN");
	close_store();
	remove_tree(store);

	/* A block outside the relation after a durable unlink has nothing left
	 * to reconstruct; the unlink itself releases its raw dependency. */
	configure_core();
	strcpy(store, "/tmp/pagestore-wal-policy-dependency-XXXXXX");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  prepare_store(store, WAL_TOTAL, 0, limited, 1) &&
		  unlink_relation(0, limited + 100) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_ALL, WAL_TOTAL) &&
		  maintenance_until_count(store, 0, 0),
		  "a durable unlink releases the raw WAL-index dependency of its blocks");
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0,
		  "clear the WAL-index snapshot trigger override after unlink");
	close_store();
	remove_tree(store);
}

/*
 * A chain retired against a fork death depends on that death as its
 * zero-page base.  Once the pre-death FPI is gone the truncate is neither the
 * latest nor the smallest definitive event, so forkmeta compaction would
 * forget it and a WAL-index-only horizon between the two truncates would be
 * left with neither an FPI nor a base: the chain fails closed and every later
 * record accumulates.  The death must stay required for the chain's lifetime.
 */
/* The selected forkmeta snapshot generation, zero before the first one. */
static uint64_t
selected_forkmeta_generation(const char *store)
{
	char directory[1024];
	PsForkmetaSnapshot selected;
	uint64_t generation = 0;

	memset(&selected, 0, sizeof(selected));
	selected.directory_fd = selected.checkpoint_fd = selected.tail_fd = -1;
	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots",
				 store) > 0 &&
		ps_forkmeta_snapshot_open(&selected, directory) == 0)
	{
		generation = selected.generation;
		ps_forkmeta_snapshot_close(&selected);
	}
	return generation;
}

/* Drive maintenance until the block's index holds `wanted` records. */
static int
maintenance_until_index_count(uint32_t block, int wanted)
{
	for (int i = 0; i < 150; i++)
	{
		if (wal_index_count(0, block, WAL_TOTAL) == wanted)
			return 1;
		(void) ps_core_maintenance();
		usleep(20000);
	}
	return wal_index_count(0, block, WAL_TOTAL) == wanted;
}

/* Drive maintenance until forkmeta compaction publishes a generation newer
 * than `previous`, then a few more rounds so WAL-index compaction replans
 * against the compacted fork history. */
static int
maintenance_until_forkmeta_generation(const char *store, uint64_t previous)
{
	int published = 0;

	for (int i = 0; i < 200 && !published; i++)
	{
		(void) ps_test_forkmeta_force_due();
		(void) ps_core_maintenance();
		published = selected_forkmeta_generation(store) > previous;
		if (!published)
			usleep(20000);
	}
	for (int i = 0; i < 8; i++)
		(void) ps_core_maintenance();
	return published;
}

static void
test_death_base_survives_prefix_prune(void)
{
	const uint64_t limited = WAL_SEGMENT + WAL_SEGMENT / 2;
	const uint64_t first_death = limited + 100;
	const uint64_t second_death = limited + 300;
	char store[] = "/tmp/pagestore-wal-policy-death-base-XXXXXX";
	PsKey rel = {1, 1, 1, 0, PS_KLASS_RELATION};
	int count = -1;

	configure_core();
	/* Forkmeta compaction is a segment-GC maintenance class. */
	segment_gc_enabled = 1;
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm the WAL-index and forkmeta compaction triggers");
	check(prepare_store(store, WAL_TOTAL, 0, 0, 0) &&
		  fork_op_keyed(0, &rel, PS_OP_CREATE, 50, 0, 0) &&
		  regrow_relation(0, 90) &&
		  wal_index_add_record(0, 1000, 0, 1) &&
		  truncate_relation(0, first_death, 0) &&
		  regrow_relation(0, limited + 140) &&
		  wal_index_add_record(0, limited + 200, 0, 0) &&
		  truncate_relation(0, second_death, 0) &&
		  regrow_relation(0, limited + 340) &&
		  wal_index_add_record(0, 2 * WAL_SEGMENT + 100, 0, 0) &&
		  wal_index_add_record(0, 2 * WAL_SEGMENT + 600, 0, 1) &&
		  churn_fork_bytes(0, limited + 400) &&
		  wal_index_progress(0, 0, 2 * WAL_SEGMENT + 650),
		  "build a regrown block whose old chain is retired against a truncate");
	/* A WAL-index-only owner between the two truncates, and a page-history
	 * owner at the end so page compaction drops the versions the truncates
	 * invalidated and nothing else keeps the first truncate required.  The
	 * shipper's progress is the only other WAL-index horizon. */
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 300, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, limited + 250) &&
		  reserve_pin(0, PS_RETENTION_OWNER_READER, 301, 1,
					  PS_RETENTION_RESOURCE_PAGE_HISTORY |
					  PS_RETENTION_RESOURCE_WAL, WAL_TOTAL),
		  "pin a horizon between the truncates and one at the end");
	check(maintenance_until_forkmeta_generation(store, 0),
		  "forkmeta compaction publishes once the cutoff is provable");
	count = wal_index_count(0, 0, WAL_TOTAL);
	check(count == 2, "the old chain is retired against the first truncate");
	if (count != 2)
		fprintf(stderr, "  retained records after the first compaction: %d\n",
				count);
	/* More lifecycle bytes elsewhere force another generation now that no
	 * page version and no pre-truncate record keeps the first truncate. */
	check(churn_fork_bytes(0, WAL_TOTAL + 1000) &&
		  maintenance_until_forkmeta_generation(
			  store, selected_forkmeta_generation(store)),
		  "a second forkmeta generation compacts the relation's history");
	/* New records and the progress that covers them schedule the next
	 * WAL-index compaction, as shipping does in production. */
	check(wal_index_add_record(0, 2 * WAL_SEGMENT + 700, 0, 0) &&
		  wal_index_add_record(0, 2 * WAL_SEGMENT + 800, 0, 1) &&
		  wal_index_progress(0, 2 * WAL_SEGMENT + 650, 2 * WAL_SEGMENT + 850),
		  "extend the chain with a record and a newer FPI");
	check(maintenance_until_index_count(0, 2), "the chain still compacts against the retained death");
	count = wal_index_count(0, 0, WAL_TOTAL);
	if (count != 2)
		fprintf(stderr, "  retained records after the second compaction: %d\n",
				count);
	close_store();
	configure_core();
	segment_gc_enabled = 1;
	check(ps_core_open(store) == 0, "reopen the store after compaction");
	check(wal_index_add_record(0, 2 * WAL_SEGMENT + 900, 0, 0) &&
		  wal_index_add_record(0, 2 * WAL_SEGMENT + 1000, 0, 1) &&
		  wal_index_progress(0, 2 * WAL_SEGMENT + 850, 2 * WAL_SEGMENT + 1050),
		  "extend the chain again after the restart");
	check(maintenance_until_index_count(0, 2), "the retained death still bases the chain after restart");
	count = wal_index_count(0, 0, WAL_TOTAL);
	if (count != 2)
		fprintf(stderr, "  retained records after restart: %d\n", count);
	check(unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES") == 0 &&
		  unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES") == 0,
		  "clear the compaction trigger overrides");
	close_store();
	remove_tree(store);
}

static void
test_natural_nonzero_start(void)
{
	const uint64_t start = WAL_SEGMENT;
	const uint64_t end = start + WAL_TOTAL;
	char store[] = "/tmp/pagestore-wal-policy-nonzero-start-XXXXXX";

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, start, (uint32_t) WAL_TOTAL) &&
		  write_control(0, end, end) &&
		  wal_index_progress(0, start, end),
		  "construct WAL whose physical and retained bases naturally start nonzero");
	check(segment_count(store, 0) == WAL_SEGMENTS &&
		  wal_read_status(0, start - 1) == PS_STATUS_ERROR,
		  "natural nonzero retained base rejects the prefix below the base");
	check(maintenance_until_count(store, 0, 0) &&
		  wal_read_status(0, start) == PS_STATUS_ERROR &&
		  wal_read_status(0, end) == PS_STATUS_OK,
		  "natural nonzero reclaim advances the fence without a directory hint");
	close_store();
	remove_tree(store);
}

static void
test_progress_beyond_immutable_end(void)
{
	const uint64_t tail = WAL_TOTAL + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-progress-tail-XXXXXX";

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) tail) &&
		  write_control(0, tail, tail) &&
		  wal_index_progress(0, 0, tail) &&
		  segment_count(store, 0) == WAL_SEGMENTS &&
		  maintenance_until_count(store, 0, 0),
		  "durable WAL-index progress beyond the sealed store end still reclaims the sealed prefix");
	close_store();
	remove_tree(store);
}

static void
test_child_branch_cap(void)
{
	const uint64_t branch_lsn = WAL_SEGMENT + WAL_SEGMENT / 2;
	const uint64_t child_start = branch_lsn - branch_lsn % WAL_SEGMENT;
	const uint64_t child_end = child_start + 2 * WAL_SEGMENT;
	char store[] = "/tmp/pagestore-wal-policy-branch-cap-XXXXXX";
	uint64_t retained_base = 0;

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  create_branch(1, 0, branch_lsn) &&
		  append_wal_bytes(1, child_start, (uint32_t) (child_end - child_start)) &&
		  write_control(1, child_end, child_end) &&
		  wal_index_progress(0, 0, WAL_TOTAL) &&
		  wal_index_progress(1, child_start, child_end) &&
			  segment_count(store, 1) == 2,
			  "construct a child whose copied WAL segment starts at its aligned fork");
	check(ps_test_wal_retained_base(1, &retained_base) == 0 &&
			  retained_base == child_start &&
			  wal_read_status(1, 0) == PS_STATUS_OK &&
			  wal_read_status(1, child_start) == PS_STATUS_OK,
			  "a natural nonzero child retained base falls through to retained parent WAL");
	check(maintenance_until_count(store, 0, 2) &&
		  segment_count(store, 1) == 2 &&
		  wal_read_status(1, 0) == PS_STATUS_ERROR &&
		  wal_read_status(1, child_start) == PS_STATUS_OK,
		  "child reclaim stops at its fork and parent frontier still fences inherited WAL");
	close_store();
	remove_tree(store);
}

static void
test_residual_prefix_retry_after_reopen(void)
{
	char store[] = "/tmp/pagestore-wal-policy-residual-XXXXXX";
	PsShmHeader metrics;

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct reclaimable WAL for a residual-prefix retry");
	/* Crash the child-owned recovery instance, not a fork-inherited provider.
	 * Ownership fencing deliberately forbids mutation through the latter. */
	close_store();
	{
		pid_t pid;
		int status = 0;

		check(setenv("PAGESTORE_TEST_WAL_RECLAIM_CRASH_BEFORE_UNLINK", "1", 1) == 0,
			  "enable the crash point after WAL frontier publication");
		pid = fork();
		if (pid == 0)
		{
			if (ps_core_open(store) != 0)
				_exit(2);
			(void) ps_test_wal_reclaim_maintenance();
			_exit(1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 91 &&
			  segment_count(store, 0) == WAL_SEGMENTS,
			  "crash leaves the authorized residual WAL prefix on disk");
		unsetenv("PAGESTORE_TEST_WAL_RECLAIM_CRASH_BEFORE_UNLINK");
	}
	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	check(ps_backpressure_configure(0, 0, WAL_SEGMENT, WAL_SEGMENT / 2) == 0 &&
		  ps_core_open(store) == 0 && segment_count(store, 0) == WAL_SEGMENTS &&
		  (ps_backpressure_refresh(),
		   metrics.wal_backpressure.throttled != 0 &&
		   metrics.wal_backpressure.lag_bytes >= WAL_TOTAL),
		  "reopen reports physical residual WAL debt and keeps throttle active");
	check(ps_test_wal_reclaim_maintenance() == 1 &&
		  segment_count(store, 0) == 0 &&
		  (ps_backpressure_refresh(),
		   metrics.wal_backpressure.throttled == 0),
		  "successful residual cleanup releases WAL backpressure");
	ps_core_set_metrics_header(NULL);
	close_store();
	remove_tree(store);
}

static void
test_residual_prefix_and_suffix_debt(void)
{
	const uint32_t nsegments = 8;
	const uint64_t total = (uint64_t) WAL_SEGMENT * nsegments;
	const uint64_t residual = WAL_SEGMENT;
	const uint64_t suffix = 3 * (uint64_t) WAL_SEGMENT;
	char store[] = "/tmp/pagestore-wal-policy-residual-suffix-XXXXXX";
	PsShmHeader metrics;
	int opened = 0;

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  (opened = 1) && append_wal_bytes(0, 0, (uint32_t) total) &&
		  write_control(0, total, total) &&
		  wal_index_progress(0, 0, total) &&
		  set_wal_pin(0, 100, 5 * (uint64_t) WAL_SEGMENT) &&
		  effective_floor(0, PS_RETENTION_RESOURCE_WAL) ==
			5 * (uint64_t) WAL_SEGMENT,
		  "construct one residual plus multiple suffix WAL segments");
	if (!opened)
	{
		remove_tree(store);
		return;
	}
	close_store();
	opened = 0;
	{
		pid_t pid;
		int status = 0;

		check(setenv("PAGESTORE_TEST_WAL_RECLAIM_CRASH_AFTER_UNLINK_SEGMENT_NO",
					 "3", 1) == 0,
			  "enable the crash point after three WAL prefix unlinks");
		pid = fork();
		if (pid == 0)
		{
			if (ps_core_open(store) != 0)
				_exit(2);
			(void) ps_test_wal_reclaim_maintenance();
			_exit(1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 92 &&
			  segment_count(store, 0) == nsegments - 4,
			  "crash leaves one residual and three post-frontier segments");
		unsetenv("PAGESTORE_TEST_WAL_RECLAIM_CRASH_AFTER_UNLINK_SEGMENT_NO");
	}
	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	check(ps_backpressure_configure(0, 0, 3 * (uint64_t) WAL_SEGMENT,
									WAL_SEGMENT / 2) == 0 &&
		  ps_core_open(store) == 0 && (opened = 1) &&
		  segment_count(store, 0) == nsegments - 4,
		  "reopen the residual and suffix WAL layout");
	check(drop_wal_pin(0, 100, 1),
		  "remove the temporary floor so the suffix is independently eligible");
	ps_backpressure_refresh();
	check(metrics.wal_backpressure.throttled != 0 &&
		  metrics.wal_backpressure.lag_bytes == residual + suffix,
		  "residual and post-frontier suffix debt are summed without overlap");
	check(set_wal_pin_generation(0, 100, 2, 5 * (uint64_t) WAL_SEGMENT),
		  "restore the floor to clean only the residual prefix");
	check(ps_test_wal_reclaim_maintenance() == 1 &&
		  segment_count(store, 0) == nsegments - 5,
		  "residual cleanup leaves every suffix segment physically present");
	check(drop_wal_pin(0, 100, 2),
		  "remove the temporary floor after residual cleanup");
	ps_backpressure_refresh();
	check(metrics.wal_backpressure.throttled != 0 &&
		  metrics.wal_backpressure.lag_bytes == suffix,
		  "suffix debt remains after residual cleanup");
	check(maintenance_until_count(store, 0, 0) &&
		  (ps_backpressure_refresh(),
		   metrics.wal_backpressure.lag_bytes == 0 &&
		   metrics.wal_backpressure.throttled == 0),
		  "suffix cleanup eventually releases WAL backpressure");
	if (opened)
	{
		close_store();
		opened = 0;
	}
	check(ps_core_open(store) == 0 && (opened = 1) &&
		  segment_count(store, 0) == 0,
		  "restart after combined residual cleanup preserves zero debt");
	ps_core_set_metrics_header(NULL);
	if (opened)
		close_store();
	ps_backpressure_configure(0, 0, 0, 0);
	remove_tree(store);
}

static void
test_restart_with_crossing_flat_tail(void)
{
	const uint64_t crossing_start = WAL_SEGMENT - 32 * 1024;
	const uint64_t tail_end = WAL_SEGMENT + 32 * 1024;
	char store[] = "/tmp/pagestore-wal-policy-crossing-tail-XXXXXX";

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) crossing_start) &&
		  append_wal_bytes(0, crossing_start, 64 * 1024) &&
		  write_control(0, tail_end, tail_end) &&
		  wal_index_progress(0, 0, tail_end) &&
		  segment_count(store, 0) == 1,
		  "construct immutable WAL with one flat record crossing its end");
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0 &&
		  ps_core_maintenance() == 1 && segment_count(store, 0) == 1,
		  "publish a WAL-index snapshot before reclaiming its historical start");
	unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES");
	check(maintenance_until_count(store, 0, 0) &&
		  !append_wal_bytes(0, 0, 32 * 1024) &&
		  !wal_index_add(0, 0),
		  "reclaim rejects WAL and WAL-index re-ships below its retained base");
	close_store();
	check(ps_core_open(store) == 0,
		  "restart restores durable progress through a crossing flat tail");
	check(wal_read_status(0, WAL_SEGMENT) == PS_STATUS_OK,
		  "the surviving crossing flat tail remains readable after restart");
	close_store();
	remove_tree(store);
}

static void
test_deleted_descendant_floor(void)
{
	const uint64_t branch_lsn = WAL_SEGMENT + WAL_SEGMENT / 2;
	char store[] = "/tmp/pagestore-wal-policy-deleted-floor-XXXXXX";
	PsTimelineState state = PS_TIMELINE_LIVE;

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  create_branch(1, 0, branch_lsn) &&
		  ps_test_walidx_frontier_exception_active(0, branch_lsn) &&
		  effective_floor(0, PS_RETENTION_RESOURCE_WAL) == branch_lsn,
		  "a LIVE descendant structurally pins the parent WAL floor");
	check(begin_delete(1) && timeline_state(1, &state) &&
		  state == PS_TIMELINE_DELETING &&
		  ps_test_walidx_frontier_exception_active(0, branch_lsn) &&
		  effective_floor(0, PS_RETENTION_RESOURCE_WAL) == branch_lsn,
		  "a DELETING descendant keeps its structural WAL pin");
	for (int i = 0; i < 128 &&
		 (!timeline_state(1, &state) || state != PS_TIMELINE_DELETED); i++)
		(void) ps_core_maintenance();
	check(timeline_state(1, &state) && state == PS_TIMELINE_DELETED &&
		  !ps_test_walidx_frontier_exception_active(0, branch_lsn) &&
		  effective_floor(0, PS_RETENTION_RESOURCE_WAL) == WAL_TOTAL,
		  "a durably DELETED descendant no longer pins the parent WAL floor");
	close_store();
	remove_tree(store);
}

static void
test_fenced_residual_query_stops_retries(void)
{
	char store[] = "/tmp/pagestore-wal-policy-fenced-residual-XXXXXX";
	ReclaimAttemptCounter counter = {0};
	AdmissionCallCounter admission = {0};
	PsShmHeader metrics;

	configure_core();
	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	check(ps_backpressure_configure(0, 0, WAL_SEGMENT, WAL_SEGMENT / 2) == 0,
		  "enable WAL backpressure for fenced residual observation");
	check(prepare_store(store, WAL_SEGMENT, 0, 0, 1),
		  "construct a one-segment reclaim candidate for directory-fsync fencing");
	ps_test_set_wal_reclaim_attempt_hook(count_reclaim_attempt, &counter);
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_DIR_FSYNC", "1", 1) == 0,
		  "enable ambiguous reclaim directory-fsync failure");
	for (int i = 0; i < 16 && counter.attempts == 0; i++)
		(void) ps_core_maintenance();
	check(counter.attempts == 1 && segment_count(store, 0) == WAL_SEGMENTS - 1,
		  "directory-fsync failure fences after publishing and unlinking the prefix");
	/* A fenced residual is an observation error, not a maintenance candidate:
	 * keep the controller fail-closed without repeatedly draining admission. */
	admission.calls = 0;
	ps_test_set_admission_write_lock_hook(count_admission_call, &admission);
	ps_backpressure_refresh();
	check(metrics.wal_backpressure.lag_bytes == UINT64_MAX &&
		  metrics.wal_backpressure.throttled != 0 && admission.calls == 0,
		  "fenced residual keeps WAL throttle without an admission drain");
	check(ps_test_wal_reclaim_maintenance() == 0 && admission.calls == 0,
		  "fenced residual is not a maintenance candidate");
	ps_test_set_admission_write_lock_hook(NULL, NULL);
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_DIR_FSYNC");
	(void) sleep(2);
	check(ps_test_wal_reclaim_maintenance() == 0 && counter.attempts == 1,
		  "fenced residual-query failure suppresses futile retries until reopen");
	ps_test_set_wal_reclaim_attempt_hook(NULL, NULL);
	ps_backpressure_configure(0, 0, 0, 0);
	ps_core_set_metrics_header(NULL);
	close_store();
	remove_tree(store);
}

static void
test_undefined_timeline_read(void)
{
	char store[] = "/tmp/pagestore-wal-policy-undefined-read-XXXXXX";

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(2, 0, WAL_SEGMENT / 2) &&
		  !ps_timeline_defined(2) && wal_read_status(2, 0) == PS_STATUS_OK,
		  "undefined shipped timeline keeps its local pre-metadata WAL readable");
	close_store();
	remove_tree(store);
}

static void
test_missing_proof_does_not_starve_later_timeline(void)
{
	char store[] = "/tmp/pagestore-wal-policy-fairness-XXXXXX";

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  create_branch(1, 0, WAL_TOTAL) &&
		  append_wal_bytes(1, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(1, WAL_TOTAL, WAL_TOTAL) &&
		  wal_index_progress(1, 0, WAL_TOTAL),
		  "construct an unproven root before a reclaimable child timeline");
	check(maintenance_until_count(store, 1, 0) &&
		  segment_count(store, 0) == WAL_SEGMENTS,
		  "a timeline missing proof does not starve a later safe candidate");
	close_store();
	remove_tree(store);
}

static void
test_pending_durable_proof(void)
{
	char store[] = "/tmp/pagestore-wal-policy-pending-proof-XXXXXX";
	ReclaimAttemptCounter counter = {0};

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct WAL with a durable progress proof candidate");
	ps_test_set_wal_reclaim_attempt_hook(count_reclaim_attempt, &counter);
	check(setenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES", "1", 1) == 0,
		  "force a WAL-index snapshot before reclaim");
	check(ps_core_maintenance() == 1 && counter.attempts == 0 &&
		  segment_count(store, 0) == WAL_SEGMENTS,
		  "new WAL-index snapshot remains pending before reclaim admission");
	check(setenv("PAGESTORE_TEST_FAIL_WALIDX_GC_FSYNC", "1", 1) == 0,
		  "enable WAL-index pending-proof cleanup fault");
	(void) ps_core_maintenance();
	check(counter.attempts == 0 &&
		  segment_count(store, 0) == WAL_SEGMENTS,
		  "failed pending-proof cleanup keeps reclaim fail closed");
	ps_test_set_wal_reclaim_attempt_hook(NULL, NULL);
	unsetenv("PAGESTORE_TEST_FAIL_WALIDX_GC_FSYNC");
	unsetenv("PAGESTORE_TEST_WALIDX_SNAPSHOT_BYTES");
	(void) sleep(2);
	check(maintenance_until_count(store, 0, 0),
		  "reclaim proceeds only after pending durable-proof state clears");
	close_store();
	remove_tree(store);
}

static void
test_safe_delete_admission_restart_and_isolation(void)
{
	char store[] = "/tmp/pagestore-wal-policy-safe-XXXXXX";
	unsigned int child_before;
	ReclaimAttemptCounter counter = {0};

	configure_core();
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0 &&
		  append_wal_bytes(0, 0, (uint32_t) WAL_TOTAL) &&
		  write_control(0, WAL_TOTAL, WAL_TOTAL) &&
		  create_branch(1, 0, WAL_TOTAL) &&
		  append_wal_bytes(1, 0, WAL_SEGMENT) &&
		  wal_index_progress(0, 0, WAL_TOTAL),
		  "construct independent root and child WAL stores");
	child_before = segment_count(store, 1);
	ps_test_set_wal_reclaim_attempt_hook(count_reclaim_attempt, &counter);
	check(child_before == 1 && maintenance_until_count(store, 0, 0) &&
		  segment_count(store, 1) == child_before,
		  "all proven root prefix is deleted and sibling timeline is untouched");
	ps_test_set_wal_reclaim_attempt_hook(NULL, NULL);
	check(!set_wal_pin(0, 200, 0) && !create_branch(2, 0, 0) &&
		  wal_read_status(0, 0) == PS_STATUS_ERROR &&
		  wal_read_status(0, WAL_TOTAL) == PS_STATUS_OK,
		  "new pin/branch/read below the retained root history fail closed");
	close_store();
	{
		int open_rc = ps_core_open(store);
		int root_read = wal_read_status(0, 0);
		unsigned int root_segments = segment_count(store, 0);
		unsigned int child_segments = segment_count(store, 1);

		check(open_rc == 0 && root_segments == 0 &&
			  root_read == PS_STATUS_ERROR && child_segments == child_before,
		  "restart preserves the retained frontier and timeline isolation");
	}
	close_store();
	remove_tree(store);
}

static void
count_reclaim_attempt(uint32_t timeline, void *arg)
{
	ReclaimAttemptCounter *counter = arg;

	(void) timeline;
	counter->attempts++;
}

typedef struct ReadFrontierGate
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int entered;
	int release;
	int status;
} ReadFrontierGate;

typedef struct ReclaimFloorGate
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int entered;
	int release;
	int reader_done;
	int wal_status;
	int walidx_status;
} ReclaimFloorGate;

static void
gate_wal_read_before_lock(uint32_t timeline, void *arg)
{
	ReadFrontierGate *gate = arg;

	(void) timeline;
	pthread_mutex_lock(&gate->mutex);
	gate->entered = 1;
	pthread_cond_broadcast(&gate->cond);
	while (!gate->release)
		pthread_cond_wait(&gate->cond, &gate->mutex);
	pthread_mutex_unlock(&gate->mutex);
}

static void *
frontier_read_thread(void *arg)
{
	ReadFrontierGate *gate = arg;

	gate->status = wal_read_status(0, 0);
	return NULL;
}

static void
gate_reclaim_before_floor(uint32_t timeline, void *arg)
{
	ReclaimFloorGate *gate = arg;

	(void) timeline;
	pthread_mutex_lock(&gate->mutex);
	gate->entered = 1;
	pthread_cond_broadcast(&gate->cond);
	while (!gate->release)
		pthread_cond_wait(&gate->cond, &gate->mutex);
	pthread_mutex_unlock(&gate->mutex);
}

static void *
map_first_wal_readers_thread(void *arg)
{
	ReclaimFloorGate *gate = arg;
	PsChannel ch;
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_READ;
	ch.timeline = 0;
	ch.datalen = 1;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_shard_rd(0);
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_unlock_shard(0);
	ps_lifecycle_read_unlock();
	gate->wal_status = ch.status;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_INDEX_GET;
	ch.timeline = 0;
	ch.key = key;
	ch.nblocks = 1;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_shard_rd(0);
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_unlock_shard(0);
	ps_lifecycle_read_unlock();
	gate->walidx_status = ch.status;

	pthread_mutex_lock(&gate->mutex);
	gate->reader_done = 1;
	pthread_cond_broadcast(&gate->cond);
	pthread_mutex_unlock(&gate->mutex);
	return NULL;
}

static void
test_floor_scan_does_not_hold_reader_gates(void)
{
	char store[] = "/tmp/pagestore-wal-policy-floor-locks-XXXXXX";
	ReclaimFloorGate gate;
	pthread_t maintenance;
	pthread_t reader;

	configure_core();
	memset(&gate, 0, sizeof(gate));
	pthread_mutex_init(&gate.mutex, NULL);
	pthread_cond_init(&gate.cond, NULL);
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct reclaimable WAL for the floor-scan lock test");
	ps_test_set_wal_reclaim_before_floor_hook(gate_reclaim_before_floor, &gate);
	check(pthread_create(&maintenance, NULL, maintenance_thread, NULL) == 0,
		  "pause reclaim after releasing reader-facing gates");
	pthread_mutex_lock(&gate.mutex);
	while (!gate.entered)
		pthread_cond_wait(&gate.cond, &gate.mutex);
	pthread_mutex_unlock(&gate.mutex);
	check(pthread_create(&reader, NULL, map_first_wal_readers_thread, &gate) == 0,
		  "start map-first WAL readers during the floor scan");
	pthread_mutex_lock(&gate.mutex);
	while (!gate.reader_done)
		pthread_cond_wait(&gate.cond, &gate.mutex);
	gate.release = 1;
	pthread_cond_broadcast(&gate.cond);
	pthread_mutex_unlock(&gate.mutex);
	pthread_join(reader, NULL);
	pthread_join(maintenance, NULL);
	check(gate.wal_status == PS_STATUS_OK &&
		  gate.walidx_status == PS_STATUS_OK,
		  "map-first WAL and WAL-index reads do not deadlock with the floor scan");
	ps_test_set_wal_reclaim_before_floor_hook(NULL, NULL);
	pthread_cond_destroy(&gate.cond);
	pthread_mutex_destroy(&gate.mutex);
	close_store();
	remove_tree(store);
}

static void
test_read_rechecks_frontier_under_wal_lock(void)
{
	char store[] = "/tmp/pagestore-wal-policy-read-race-XXXXXX";
	ReadFrontierGate gate;
	pthread_t reader;
	pthread_t maintenance;

	configure_core();
	memset(&gate, 0, sizeof(gate));
	pthread_mutex_init(&gate.mutex, NULL);
	pthread_cond_init(&gate.cond, NULL);
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct reclaimable WAL for the read/frontier race");
	ps_test_set_wal_read_before_lock_hook(gate_wal_read_before_lock, &gate);
	check(pthread_create(&reader, NULL, frontier_read_thread, &gate) == 0,
		  "pause a WAL read after its optimistic frontier check");
	pthread_mutex_lock(&gate.mutex);
	while (!gate.entered)
		pthread_cond_wait(&gate.cond, &gate.mutex);
	pthread_mutex_unlock(&gate.mutex);
	check(pthread_create(&maintenance, NULL, maintenance_thread, NULL) == 0,
		  "start reclaim while the read is paused before WAL-rd");
	for (int i = 0; i < 1000 && segment_count(store, 0) != 0; i++)
		usleep(1000);
	check(segment_count(store, 0) == 0,
		  "publish and reclaim the prefix before the paused read takes WAL-rd");
	pthread_mutex_lock(&gate.mutex);
	gate.release = 1;
	pthread_cond_broadcast(&gate.cond);
	pthread_mutex_unlock(&gate.mutex);
	pthread_join(reader, NULL);
	pthread_join(maintenance, NULL);
	check(gate.status == PS_STATUS_ERROR,
		  "the WAL-lock recheck rejects a read that lost the frontier race");
	ps_test_set_wal_read_before_lock_hook(NULL, NULL);
	pthread_cond_destroy(&gate.cond);
	pthread_mutex_destroy(&gate.mutex);
	close_store();
	remove_tree(store);
}

static void
test_failure_backoff(void)
{
	char store[] = "/tmp/pagestore-wal-policy-failure-XXXXXX";
	ReclaimAttemptCounter counter = {0};
	PsShmHeader metrics;

	configure_core();
	memset(&metrics, 0, sizeof(metrics));
	ps_core_set_metrics_header(&metrics);
	check(ps_backpressure_configure(0, 0, WAL_SEGMENT, WAL_SEGMENT / 2) == 0,
		  "enable WAL controller for failure-backoff observation");
	/* Keep the control pages in the memtable; otherwise page-prune work can
	 * legitimately make the aggregate maintenance call return 1 after the WAL
	 * reclaim attempt fails, obscuring the policy result under test. */
	flush_pages = 1000000;
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct a reclaimable WAL prefix for fault injection");
	/* No unrelated layer-compaction result should mask the failed WAL reclaim
	 * result in this policy test. */
	compact_layers = 1000000;
	ps_test_set_wal_reclaim_attempt_hook(count_reclaim_attempt, &counter);
	{
		int did = 1;

		check(setenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME", "1", 1) == 0,
			  "enable WAL metadata publication fault");
		/* A first tick may publish an unrelated WAL-index snapshot.  The
		 * hook identifies the tick that actually reached reclaim, which must
		 * report no work after metadata publication fails. */
		for (int i = 0; i < 16 && counter.attempts == 0; i++)
			did = ps_core_maintenance();
		check(counter.attempts == 1 && did == 0 &&
			  segment_count(store, 0) == WAL_SEGMENTS,
			  "metadata publication failure does not report reclaim work");
		ps_backpressure_refresh();
		check(metrics.wal_backpressure.throttled != 0 &&
			  metrics.wal_backpressure.lag_bytes >= WAL_SEGMENT,
			  "failed reclaim remains WAL backpressure debt during retry backoff");
	}
	{
		int did = ps_core_maintenance();

		check(did == 0 && counter.attempts == 1 &&
			  segment_count(store, 0) == WAL_SEGMENTS,
		  "failed reclaim backs off instead of retrying in a busy loop");
	}
	ps_test_set_wal_reclaim_attempt_hook(NULL, NULL);
	unsetenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME");
	(void) sleep(2);
	check(maintenance_until_count(store, 0, 0),
		  "failed reclaim retries after backoff and then deletes safely");
	ps_backpressure_configure(0, 0, 0, 0);
	ps_core_set_metrics_header(NULL);
	close_store();
	remove_tree(store);
}

typedef struct AdmissionGate
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int acquired;
	int release;
	int pin_done;
} AdmissionGate;

static int
gate_admission_write(pthread_rwlock_t *lock, void *arg)
{
	AdmissionGate *gate = arg;

	if (pthread_rwlock_wrlock(lock) != 0)
		return -1;
	pthread_mutex_lock(&gate->mutex);
	gate->acquired = 1;
	pthread_cond_broadcast(&gate->cond);
	while (!gate->release)
		pthread_cond_wait(&gate->cond, &gate->mutex);
	pthread_mutex_unlock(&gate->mutex);
	return 0;
}

static void *
maintenance_thread(void *arg)
{
	(void) arg;
	(void) ps_core_maintenance();
	return NULL;
}

static void *
admission_pin_thread(void *arg)
{
	AdmissionGate *gate = arg;
	int done;

	done = set_wal_pin(0, 300, WAL_TOTAL);
	pthread_mutex_lock(&gate->mutex);
	gate->pin_done = done;
	pthread_cond_broadcast(&gate->cond);
	pthread_mutex_unlock(&gate->mutex);
	return NULL;
}

static void
test_concurrent_admission(void)
{
	char store[] = "/tmp/pagestore-wal-policy-admission-XXXXXX";
	AdmissionGate gate;
	pthread_t maintenance;
	pthread_t pin;

	configure_core();
	memset(&gate, 0, sizeof(gate));
	pthread_mutex_init(&gate.mutex, NULL);
	pthread_cond_init(&gate.cond, NULL);
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct concurrent-admission fixture");
	ps_test_set_admission_write_lock_hook(gate_admission_write, &gate);
	check(pthread_create(&maintenance, NULL, maintenance_thread, NULL) == 0,
		  "start maintenance under admission gate");
	pthread_mutex_lock(&gate.mutex);
	while (!gate.acquired)
		pthread_cond_wait(&gate.cond, &gate.mutex);
	pthread_mutex_unlock(&gate.mutex);
	check(pthread_create(&pin, NULL, admission_pin_thread, &gate) == 0,
		  "start admission mutation while maintenance owns drain");
	usleep(20000);
	pthread_mutex_lock(&gate.mutex);
	check(!gate.pin_done,
		  "concurrent WAL pin cannot pass the maintenance admission drain");
	gate.release = 1;
	pthread_cond_broadcast(&gate.cond);
	pthread_mutex_unlock(&gate.mutex);
	pthread_join(maintenance, NULL);
	pthread_join(pin, NULL);
	check(gate.pin_done && segment_count(store, 0) == 0,
		  "admission mutation resumes only after the safe reclaim cutover");
	ps_test_set_admission_write_lock_hook(NULL, NULL);
	pthread_cond_destroy(&gate.cond);
	pthread_mutex_destroy(&gate.mutex);
	close_store();
	remove_tree(store);
}

typedef struct AdmissionTraffic
{
	volatile int stop;
	volatile int reader_started;
	volatile int refresh_done;
} AdmissionTraffic;

static void *
admission_traffic_reader(void *arg)
{
	AdmissionTraffic *traffic = arg;

	while (!__atomic_load_n(&traffic->stop, __ATOMIC_ACQUIRE))
	{
		ps_admission_read_lock();
		__atomic_store_n(&traffic->reader_started, 1, __ATOMIC_RELEASE);
		ps_admission_read_unlock();
	}
	return NULL;
}

static void *
admission_traffic_refresh(void *arg)
{
	AdmissionTraffic *traffic = arg;

	ps_backpressure_refresh();
	__atomic_store_n(&traffic->refresh_done, 1, __ATOMIC_RELEASE);
	return NULL;
}

static void
test_wal_observation_beats_reader_traffic(void)
{
	char store[] = "/tmp/pagestore-wal-policy-reader-traffic-XXXXXX";
	PsShmHeader metrics;
	AdmissionTraffic traffic;
	pthread_t readers[4];
	pthread_t refresh;
	int nreaders = 0;
	int refresh_started = 0;

	configure_core();
	memset(&traffic, 0, sizeof(traffic));
	memset(&metrics, 0, sizeof(metrics));
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct WAL debt for reader-preference observation");
	ps_core_set_metrics_header(&metrics);
	check(ps_backpressure_configure(0, 0, WAL_SEGMENT, WAL_SEGMENT / 2) == 0,
		  "enable WAL backpressure for reader-preference observation");
	for (size_t i = 0; i < sizeof(readers) / sizeof(readers[0]); i++)
	{
		if (pthread_create(&readers[nreaders], NULL, admission_traffic_reader,
						   &traffic) != 0)
			break;
		nreaders++;
	}
	for (int i = 0; i < 2000 &&
			!__atomic_load_n(&traffic.reader_started, __ATOMIC_ACQUIRE); i++)
		usleep(1000);
	check(nreaders == 4 &&
			__atomic_load_n(&traffic.reader_started, __ATOMIC_ACQUIRE) != 0,
		  "sustained admission readers are active before WAL observation");
	if (pthread_create(&refresh, NULL, admission_traffic_refresh, &traffic) == 0)
		refresh_started = 1;
	for (int i = 0; refresh_started && i < 2000 &&
			!__atomic_load_n(&traffic.refresh_done, __ATOMIC_ACQUIRE); i++)
		usleep(1000);
	check(refresh_started &&
			__atomic_load_n(&traffic.refresh_done, __ATOMIC_ACQUIRE) != 0 &&
			ps_load_acquire(&metrics.wal_backpressure.throttled) != 0 &&
			ps_load_acquire_u64(&metrics.wal_backpressure.lag_bytes) >= WAL_SEGMENT,
		  "WAL lag observation eventually activates under reader traffic");
	__atomic_store_n(&traffic.stop, 1, __ATOMIC_RELEASE);
	for (int i = 0; i < nreaders; i++)
		pthread_join(readers[i], NULL);
	if (refresh_started)
		pthread_join(refresh, NULL);
	ps_backpressure_configure(0, 0, 0, 0);
	ps_core_set_metrics_header(NULL);
	close_store();
	remove_tree(store);
}

int
main(void)
{
	test_no_floor_or_progress();
	test_preselection_skips_empty_reclaim();
	test_no_progress_backoff_follows_proof();
	test_epoch_retries_are_rate_limited();
	test_backoff_epoch_predates_attempt_inputs();
	test_stale_wal_index_requests_compaction();
	test_unreplaceable_dependency_requests_once();
	test_late_durable_base_requests_compaction();
	test_late_fpi_requests_compaction();
	test_superseded_note_in_memtable_is_pruned();
	test_base_durable_between_evaluations();
	test_base_durable_at_write();
	test_watch_ignores_unrelated_shard_flush();
	test_control_flush_not_repeated();
	test_late_fpi_requests_compaction_protected_horizon();
	test_fpi_between_protected_and_progress_retires();
	test_watch_does_not_refire_on_satisfied_base_evidence();
	test_walidx_only_pin_drop_requests_compaction();
	test_dependency_cutoffs();
	test_death_base_survives_prefix_prune();
	test_natural_nonzero_start();
	test_progress_beyond_immutable_end();
	test_child_branch_cap();
	test_residual_prefix_retry_after_reopen();
	test_residual_prefix_and_suffix_debt();
	test_restart_with_crossing_flat_tail();
	test_deleted_descendant_floor();
	test_fenced_residual_query_stops_retries();
	test_undefined_timeline_read();
	test_missing_proof_does_not_starve_later_timeline();
	test_pending_durable_proof();
	test_safe_delete_admission_restart_and_isolation();
	test_read_rechecks_frontier_under_wal_lock();
	test_floor_scan_does_not_hold_reader_gates();
	test_failure_backoff();
	test_concurrent_admission();
	test_wal_observation_beats_reader_traffic();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
