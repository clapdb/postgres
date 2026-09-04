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
#include <unistd.h>

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

	if (snprintf(command, sizeof(command), "rm -rf -- '%s'", path) > 0)
		(void) system(command);
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
wal_index_add(uint32_t timeline, uint64_t lsn)
{
	PsChannel ch;
	PsWalIndexEntry entry;
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	memset(&ch, 0, sizeof(ch));
	memset(&entry, 0, sizeof(entry));
	entry.key = key;
	entry.block = 0;
	entry.lsn = lsn;
	entry.end_lsn = lsn + 50;
	entry.flags = PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI;
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
set_wal_pin(uint32_t timeline, uint64_t owner_id, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_SET;
	ch.timeline = timeline;
	ch.blocknum = PS_RETENTION_OWNER_READER;
	ch.parent_timeline = PS_RETENTION_RESOURCE_WAL;
	ch.old_nblocks = 1;
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
	for (int i = 0; i < 64 && segment_count(store, timeline) > wanted; i++)
		(void) ps_core_maintenance();
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

	configure_core();
	check(prepare_store(store, WAL_TOTAL, 0, 0, 1),
		  "construct reclaimable WAL for a residual-prefix retry");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_BEFORE_UNLINK", "1", 1) == 0 &&
		  ps_test_wal_reclaim_maintenance() == 0 &&
		  segment_count(store, 0) == WAL_SEGMENTS,
		  "publish the durable frontier but leave every authorized file residual");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_BEFORE_UNLINK");
	close_store();
	check(ps_core_open(store) == 0 && segment_count(store, 0) == WAL_SEGMENTS &&
		  ps_test_wal_reclaim_maintenance() == 1 &&
		  segment_count(store, 0) == 0,
		  "reopen retries residual unlink at the already-published frontier");
	close_store();
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

	configure_core();
	check(prepare_store(store, WAL_SEGMENT, 0, 0, 1),
		  "construct a one-segment reclaim candidate for directory-fsync fencing");
	ps_test_set_wal_reclaim_attempt_hook(count_reclaim_attempt, &counter);
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_DIR_FSYNC", "1", 1) == 0,
		  "enable ambiguous reclaim directory-fsync failure");
	for (int i = 0; i < 16 && counter.attempts == 0; i++)
		(void) ps_core_maintenance();
	check(counter.attempts == 1 && segment_count(store, 0) == WAL_SEGMENTS - 1,
		  "directory-fsync failure fences after publishing and unlinking the prefix");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_DIR_FSYNC");
	(void) sleep(2);
	check(ps_test_wal_reclaim_maintenance() == 0 && counter.attempts == 1,
		  "fenced residual-query failure suppresses futile retries until reopen");
	ps_test_set_wal_reclaim_attempt_hook(NULL, NULL);
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

int
main(void)
{
	test_no_floor_or_progress();
	test_preselection_skips_empty_reclaim();
	test_dependency_cutoffs();
	test_natural_nonzero_start();
	test_progress_beyond_immutable_end();
	test_child_branch_cap();
	test_residual_prefix_retry_after_reopen();
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
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
