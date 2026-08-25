/*
 * Focused POSIX unit test for the timeline lifecycle foundation.  This test
 * links the shared core directly; it never starts a daemon and never touches
 * the optional SPDK frontend.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_retention.h"

#define TEST_TIMELINE_MAGIC 0x324d4c54U

typedef struct TestTimelineV2
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t id;
	int32_t	 parent;
	uint64_t branch_lsn;
	uint32_t crc;
	uint32_t reserved;
} TestTimelineV2;

typedef struct TestTimelineLegacy
{
	uint32_t id;
	int32_t parent;
	uint64_t branch_lsn;
} TestTimelineLegacy;

typedef struct TestTimelineEvent
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t kind;
	uint32_t id;
	int32_t	 parent;
	uint32_t state;
	uint64_t branch_lsn;
	uint64_t incarnation;
	uint32_t crc;
	uint32_t reserved;
} TestTimelineEvent;

static int checks;
static int failed;
static PsStorage timeline_test_storage;
enum TimelineAppendMode
{
	TIMELINE_APPEND_NORMAL,
	TIMELINE_APPEND_FAIL_BEFORE_WRITE,
	TIMELINE_APPEND_AMBIGUOUS
};
static enum TimelineAppendMode timeline_append_mode;

static int
test_meta_append(const void *buf, uint32_t len)
{
	if (timeline_append_mode == TIMELINE_APPEND_FAIL_BEFORE_WRITE)
	{
		errno = EIO;
		return -1;
	}
	if (PsStoragePosix.meta_append(buf, len) != 0)
		return -1;
	if (timeline_append_mode == TIMELINE_APPEND_AMBIGUOUS)
	{
		errno = EIO;
		return -1;
	}
	return 0;
}

static uint32_t
fnv(const void *data, size_t len)
{
	const unsigned char *p = data;
	uint32_t h = 2166136261U;

	for (size_t i = 0; i < len; i++)
	{
		h ^= p[i];
		h *= 16777619U;
	}
	return h;
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
close_store(void)
{
	ps_core_close();
	if (ps_storage->close != NULL)
		ps_storage->close();
}

static int
write_bytes(const char *path, const void *data, size_t len)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	ssize_t n;

	if (fd < 0)
		return -1;
	n = write(fd, data, len);
	if (n == (ssize_t) len && fsync(fd) == 0 && close(fd) == 0)
		return 0;
	(void) close(fd);
	return -1;
}

static int
append_bytes(const char *path, const void *data, size_t len)
{
	int fd = open(path, O_WRONLY | O_APPEND);
	ssize_t n;

	if (fd < 0)
		return -1;
	n = write(fd, data, len);
	if (n == (ssize_t) len && fsync(fd) == 0 && close(fd) == 0)
		return 0;
	(void) close(fd);
	return -1;
}

static int
state_of(uint32_t timeline, PsTimelineState *state, uint64_t *incarnation)
{
	PsChannel ch;
	int ok;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_TIMELINE_STATE;
	ch.timeline = timeline;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_lifecycle_read_unlock();
	ok = ch.status == PS_STATUS_OK;
	if (ok && state != NULL)
		*state = (PsTimelineState) ch.result;
	if (ok && incarnation != NULL)
		*incarnation = ch.req_seq;
	return ok;
}

static int
create_branch(uint32_t timeline, uint32_t parent, uint64_t branch_lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_CREATE_BRANCH;
	ch.timeline = timeline;
	ch.parent_timeline = parent;
	ch.req_lsn = branch_lsn;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_map_wr();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
begin_delete(uint32_t timeline, uint64_t expected_incarnation,
			 PsChannel *out)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_BEGIN_DELETE;
	ch.timeline = timeline;
	ch.req_seq = expected_incarnation;
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
	if (out != NULL)
		*out = ch;
	return ch.status == PS_STATUS_OK;
}

static int
branch_request(PsOpcode opcode, uint32_t timeline, uint32_t parent,
			   uint64_t branch_lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.timeline = timeline;
	ch.parent_timeline = parent;
	ch.req_lsn = branch_lsn;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_map_wr();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

typedef struct AdmissionReader
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint32_t timeline;
	int ready;
	int release;
} AdmissionReader;

typedef struct LockObserver
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int entered;
	int acquired;
	int release;
} LockObserver;

static void
observer_init(LockObserver *observer)
{
	memset(observer, 0, sizeof(*observer));
	pthread_mutex_init(&observer->mutex, NULL);
	pthread_cond_init(&observer->cond, NULL);
}

static void
observer_destroy(LockObserver *observer)
{
	pthread_cond_destroy(&observer->cond);
	pthread_mutex_destroy(&observer->mutex);
}

static void
observer_wait(LockObserver *observer, int acquired)
{
	pthread_mutex_lock(&observer->mutex);
	while (!(acquired ? observer->acquired : observer->entered))
		pthread_cond_wait(&observer->cond, &observer->mutex);
	pthread_mutex_unlock(&observer->mutex);
}

static int
observe_write_lock(pthread_rwlock_t *lock, void *arg)
{
	LockObserver *observer = arg;

	pthread_mutex_lock(&observer->mutex);
	observer->entered = 1;
	pthread_cond_broadcast(&observer->cond);
	pthread_mutex_unlock(&observer->mutex);
	if (pthread_rwlock_wrlock(lock) != 0)
		return -1;
	pthread_mutex_lock(&observer->mutex);
	observer->acquired = 1;
	pthread_cond_broadcast(&observer->cond);
	while (!observer->release)
		pthread_cond_wait(&observer->cond, &observer->mutex);
	pthread_mutex_unlock(&observer->mutex);
	return 0;
}

static void
observe_writer_queued(void *arg)
{
	LockObserver *observer = arg;

	pthread_mutex_lock(&observer->mutex);
	observer->entered = 1;
	pthread_cond_broadcast(&observer->cond);
	pthread_mutex_unlock(&observer->mutex);
}

typedef struct DeleteThread
{
	uint32_t timeline;
	volatile int done;
	int ok;
} DeleteThread;

static void *
admission_reader_main(void *arg)
{
	AdmissionReader *reader = arg;

	/* Only the mutation-admission reader section is modeled here; this is not
	 * an ordinary read or an SPDK async request. */
	ps_admission_read_lock();
	pthread_mutex_lock(&reader->mutex);
	reader->ready = 1;
	pthread_cond_broadcast(&reader->cond);
	while (!reader->release)
		pthread_cond_wait(&reader->cond, &reader->mutex);
	pthread_mutex_unlock(&reader->mutex);
	ps_admission_read_unlock();
	return NULL;
}

static void *
lifecycle_reader_main(void *arg)
{
	AdmissionReader *reader = arg;
	PsChannel ch;

	ps_lifecycle_read_lock();
	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_TIMELINE_STATE;
	ch.timeline = reader->timeline;
	ch.status = PS_STATUS_OK;
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	pthread_mutex_lock(&reader->mutex);
	reader->ready = 1;
	pthread_cond_broadcast(&reader->cond);
	while (!reader->release)
		pthread_cond_wait(&reader->cond, &reader->mutex);
	pthread_mutex_unlock(&reader->mutex);
	ps_lifecycle_read_unlock();
	return NULL;
}

typedef struct ReadProbe
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int started;
	int entered;
	int done;
} ReadProbe;

static void
observe_reader_queued(void *arg)
{
	ReadProbe *probe = arg;

	pthread_mutex_lock(&probe->mutex);
	probe->started = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->mutex);
}

static void
read_probe_init(ReadProbe *probe)
{
	memset(probe, 0, sizeof(*probe));
	pthread_mutex_init(&probe->mutex, NULL);
	pthread_cond_init(&probe->cond, NULL);
}

static void
read_probe_destroy(ReadProbe *probe)
{
	pthread_cond_destroy(&probe->cond);
	pthread_mutex_destroy(&probe->mutex);
}

static void *
read_probe_main(void *arg)
{
	ReadProbe *probe = arg;

	ps_lifecycle_read_lock();
	pthread_mutex_lock(&probe->mutex);
	probe->entered = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->mutex);
	ps_lifecycle_read_unlock();
	pthread_mutex_lock(&probe->mutex);
	probe->done = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->mutex);
	return NULL;
}

static void
read_probe_wait_started(ReadProbe *probe)
{
	pthread_mutex_lock(&probe->mutex);
	while (!probe->started)
		pthread_cond_wait(&probe->cond, &probe->mutex);
	pthread_mutex_unlock(&probe->mutex);
}

static void *
delete_thread_main(void *arg)
{
	DeleteThread *request = arg;

	request->ok = begin_delete(request->timeline, 1, NULL);
	__atomic_store_n(&request->done, 1, __ATOMIC_RELEASE);
	return NULL;
}

static int
test_admission_drain(uint32_t timeline)
{
	AdmissionReader reader;
	LockObserver observer;
	DeleteThread request;
	pthread_t reader_thread;
	pthread_t delete_thread;

	memset(&reader, 0, sizeof(reader));
	memset(&request, 0, sizeof(request));
	observer_init(&observer);
	pthread_mutex_init(&reader.mutex, NULL);
	pthread_cond_init(&reader.cond, NULL);
	request.timeline = timeline;
	if (pthread_create(&reader_thread, NULL, admission_reader_main, &reader) != 0)
	{
		pthread_cond_destroy(&reader.cond);
		pthread_mutex_destroy(&reader.mutex);
		observer_destroy(&observer);
		return 0;
	}
	pthread_mutex_lock(&reader.mutex);
	while (!reader.ready)
		pthread_cond_wait(&reader.cond, &reader.mutex);
	pthread_mutex_unlock(&reader.mutex);
	ps_test_set_admission_write_lock_hook(observe_write_lock, &observer);
	if (pthread_create(&delete_thread, NULL, delete_thread_main, &request) != 0)
	{
		pthread_mutex_lock(&reader.mutex);
		reader.release = 1;
		pthread_cond_broadcast(&reader.cond);
		pthread_mutex_unlock(&reader.mutex);
		pthread_join(reader_thread, NULL);
		ps_test_set_admission_write_lock_hook(NULL, NULL);
		pthread_cond_destroy(&reader.cond);
		pthread_mutex_destroy(&reader.mutex);
		observer_destroy(&observer);
		return 0;
	}
	observer_wait(&observer, 0);
	/* The admission writer is now known to be queued behind the reader. */
	pthread_mutex_lock(&reader.mutex);
	reader.release = 1;
	pthread_cond_broadcast(&reader.cond);
	pthread_mutex_unlock(&reader.mutex);
	observer_wait(&observer, 1);
	pthread_mutex_lock(&observer.mutex);
	observer.release = 1;
	pthread_cond_broadcast(&observer.cond);
	pthread_mutex_unlock(&observer.mutex);
	pthread_join(reader_thread, NULL);
	pthread_join(delete_thread, NULL);
	ps_test_set_admission_write_lock_hook(NULL, NULL);
	pthread_cond_destroy(&reader.cond);
	pthread_mutex_destroy(&reader.mutex);
	observer_destroy(&observer);
	return request.ok;
}

static int
test_lifecycle_drain(uint32_t timeline)
{
	AdmissionReader reader;
	LockObserver observer;
	ReadProbe late_reader;
	DeleteThread request;
	pthread_t reader_thread;
	pthread_t delete_thread;
	pthread_t late_thread;
	int late_entered;

	memset(&reader, 0, sizeof(reader));
	memset(&request, 0, sizeof(request));
	observer_init(&observer);
	read_probe_init(&late_reader);
	pthread_mutex_init(&reader.mutex, NULL);
	pthread_cond_init(&reader.cond, NULL);
	reader.timeline = timeline;
	request.timeline = timeline;
	if (pthread_create(&reader_thread, NULL, lifecycle_reader_main, &reader) != 0)
		goto fail;
	pthread_mutex_lock(&reader.mutex);
	while (!reader.ready)
		pthread_cond_wait(&reader.cond, &reader.mutex);
	pthread_mutex_unlock(&reader.mutex);
	ps_test_set_lifecycle_write_queued_hook(observe_writer_queued, &observer);
	ps_test_set_lifecycle_write_lock_hook(observe_write_lock, &observer);
	if (pthread_create(&delete_thread, NULL, delete_thread_main, &request) != 0)
	{
		pthread_mutex_lock(&reader.mutex);
		reader.release = 1;
		pthread_cond_broadcast(&reader.cond);
		pthread_mutex_unlock(&reader.mutex);
		pthread_join(reader_thread, NULL);
		goto fail_hooks;
	}
	observer_wait(&observer, 0);
	/* The delete writer is queued behind the complete ordinary request. */
	ps_test_set_lifecycle_read_queued_hook(observe_reader_queued, &late_reader);
	if (pthread_create(&late_thread, NULL, read_probe_main, &late_reader) != 0)
	{
		pthread_mutex_lock(&reader.mutex);
		reader.release = 1;
		pthread_cond_broadcast(&reader.cond);
		pthread_mutex_unlock(&reader.mutex);
		pthread_join(reader_thread, NULL);
		pthread_mutex_lock(&observer.mutex);
		observer.release = 1;
		pthread_cond_broadcast(&observer.cond);
		pthread_mutex_unlock(&observer.mutex);
		pthread_join(delete_thread, NULL);
		goto fail_hooks;
	}
	read_probe_wait_started(&late_reader);
	/* The queued writer closes the turnstile before this late reader can enter. */
	pthread_mutex_lock(&late_reader.mutex);
	late_entered = late_reader.entered;
	pthread_mutex_unlock(&late_reader.mutex);

	pthread_mutex_lock(&reader.mutex);
	reader.release = 1;
	pthread_cond_broadcast(&reader.cond);
	pthread_mutex_unlock(&reader.mutex);
	observer_wait(&observer, 1);

	pthread_mutex_lock(&observer.mutex);
	observer.release = 1;
	pthread_cond_broadcast(&observer.cond);
	pthread_mutex_unlock(&observer.mutex);
	pthread_join(reader_thread, NULL);
	pthread_join(delete_thread, NULL);
	pthread_join(late_thread, NULL);
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_lock_hook(NULL, NULL);
	ps_test_set_lifecycle_read_queued_hook(NULL, NULL);
	pthread_cond_destroy(&reader.cond);
	pthread_mutex_destroy(&reader.mutex);
	observer_destroy(&observer);
	read_probe_destroy(&late_reader);
	return !late_entered && request.ok;

fail_hooks:
	ps_test_set_lifecycle_read_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_lock_hook(NULL, NULL);
fail:
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_lock_hook(NULL, NULL);
	pthread_cond_destroy(&reader.cond);
	pthread_mutex_destroy(&reader.mutex);
	observer_destroy(&observer);
	read_probe_destroy(&late_reader);
	return 0;
}

static void
lifecycle_read_hold_hook(void *arg)
{
	AdmissionReader *reader = arg;

	pthread_mutex_lock(&reader->mutex);
	reader->ready = 1;
	pthread_cond_broadcast(&reader->cond);
	while (!reader->release)
		pthread_cond_wait(&reader->cond, &reader->mutex);
	pthread_mutex_unlock(&reader->mutex);
}

static void *
maintenance_main(void *arg)
{
	int *done = arg;

	(void) ps_core_maintenance();
	*done = 1;
	return NULL;
}

static int
test_maintenance_drain(uint32_t timeline)
{
	AdmissionReader gate;
	LockObserver observer;
	DeleteThread request;
	pthread_t maintenance_thread;
	pthread_t delete_thread;
	int maintenance_done = 0;

	memset(&gate, 0, sizeof(gate));
	memset(&request, 0, sizeof(request));
	observer_init(&observer);
	pthread_mutex_init(&gate.mutex, NULL);
	pthread_cond_init(&gate.cond, NULL);
	request.timeline = timeline;
	ps_test_set_lifecycle_read_hook(lifecycle_read_hold_hook, &gate);
	if (pthread_create(&maintenance_thread, NULL, maintenance_main,
						   &maintenance_done) != 0)
		goto fail;
	pthread_mutex_lock(&gate.mutex);
	while (!gate.ready)
		pthread_cond_wait(&gate.cond, &gate.mutex);
	pthread_mutex_unlock(&gate.mutex);
	ps_test_set_lifecycle_write_queued_hook(observe_writer_queued, &observer);
	ps_test_set_lifecycle_write_lock_hook(observe_write_lock, &observer);
	if (pthread_create(&delete_thread, NULL, delete_thread_main, &request) != 0)
	{
		pthread_mutex_lock(&gate.mutex);
		gate.release = 1;
		pthread_cond_broadcast(&gate.cond);
		pthread_mutex_unlock(&gate.mutex);
		pthread_join(maintenance_thread, NULL);
		goto fail_hooks;
	}
	observer_wait(&observer, 0);
	pthread_mutex_lock(&gate.mutex);
	gate.release = 1;
	pthread_cond_broadcast(&gate.cond);
	pthread_mutex_unlock(&gate.mutex);
	observer_wait(&observer, 1);
	pthread_mutex_lock(&observer.mutex);
	observer.release = 1;
	pthread_cond_broadcast(&observer.cond);
	pthread_mutex_unlock(&observer.mutex);
	pthread_join(maintenance_thread, NULL);
	pthread_join(delete_thread, NULL);
	ps_test_set_lifecycle_read_hook(NULL, NULL);
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_lock_hook(NULL, NULL);
	pthread_cond_destroy(&gate.cond);
	pthread_mutex_destroy(&gate.mutex);
	observer_destroy(&observer);
	return maintenance_done == 1 && request.ok;

fail_hooks:
	ps_test_set_lifecycle_read_hook(NULL, NULL);
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_lock_hook(NULL, NULL);
fail:
	ps_test_set_lifecycle_read_hook(NULL, NULL);
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	ps_test_set_lifecycle_write_lock_hook(NULL, NULL);
	pthread_cond_destroy(&gate.cond);
	pthread_mutex_destroy(&gate.mutex);
	observer_destroy(&observer);
	return 0;
}

static int
meta_exists(uint32_t timeline)
{
	PsChannel ch;
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_EXISTS;
	ch.timeline = timeline;
	ch.key = key;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_shard_rd(ps_shard_of(&key));
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_unlock_shard(ps_shard_of(&key));
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
timeline_info(uint32_t timeline)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_TIMELINE_INFO;
	ch.timeline = timeline;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
wal_size_allowed(uint32_t timeline)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_SIZE;
	ch.timeline = timeline;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_shard_rd(0);
	ps_lock_map_rd();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_unlock_shard(0);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
expect_open_failure(const char *store)
{
	pid_t pid = fork();
	int status;

	if (pid == 0)
	{
		int rc = ps_core_open(store);

		if (rc == 0)
		{
			close_store();
			_exit(1);
		}
		_exit(0);
	}
	if (pid < 0 || waitpid(pid, &status, 0) != pid)
		return 0;
	return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static void
remove_tree(const char *path)
{
	char command[512];

	if (snprintf(command, sizeof(command), "rm -rf -- '%s'", path) > 0)
		(void) system(command);
}

static void
configure_timeline_core(void)
{
	page_size = 8192;
	segment_size = 1024 * 1024;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 0;
	cache_pages = 0;
	ps_nshards = 1;
	use_layers = 1;
	ps_storage = &PsStoragePosix;
}

static void
test_legacy_migration_and_parser_fail_closed(void)
{
	char store[] = "/tmp/pagestore-timeline-parser-XXXXXX";
	char path[512];
	TestTimelineLegacy legacy;
	TestTimelineV2 v2;
	uint32_t bad_header[2];
	int fd;

	configure_timeline_core();
	check(mkdtemp(store) != NULL, "create parser test store");
	check(snprintf(path, sizeof(path), "%s/timelines", store) > 0,
		  "build parser test path");
	memset(&legacy, 0, sizeof(legacy));
	legacy.id = 7;
	legacy.parent = 0;
	legacy.branch_lsn = 100;
	check(write_bytes(path, &legacy, sizeof(legacy)) == 0,
		  "write legacy-only fixed record");
	check(ps_core_open(store) == 0, "replay legacy-only metadata");
	close_store();
	fd = open(path, O_RDONLY);
	memset(&v2, 0, sizeof(v2));
	check(fd >= 0 && read(fd, &v2, sizeof(v2)) == (ssize_t) sizeof(v2) &&
		  v2.magic == TEST_TIMELINE_MAGIC && v2.rec_len == sizeof(v2),
		  "legacy replay atomically migrates to V2");
	if (fd >= 0)
		(void) close(fd);
	remove_tree(store);

	strcpy(store, "/tmp/pagestore-timeline-parser-XXXXXX");
	check(mkdtemp(store) != NULL, "create magic corruption store");
	check(snprintf(path, sizeof(path), "%s/timelines", store) > 0,
		  "build magic corruption path");
	memset(&v2, 0, sizeof(v2));
	v2.magic = TEST_TIMELINE_MAGIC;
	v2.rec_len = sizeof(v2);
	v2.id = 7;
	v2.parent = 0;
	v2.branch_lsn = 100;
	v2.crc = fnv(&v2, sizeof(v2));
	check(write_bytes(path, &v2, sizeof(v2)) == 0, "write modern record");
	bad_header[0] = 0xdeadbeefU;
	bad_header[1] = sizeof(v2);
	check(append_bytes(path, bad_header, sizeof(bad_header)) == 0,
		  "append corrupt modern metadata magic");
	check(expect_open_failure(store),
		  "corrupt modern magic fails closed");
	remove_tree(store);

	strcpy(store, "/tmp/pagestore-timeline-parser-XXXXXX");
	check(mkdtemp(store) != NULL, "create unknown length store");
	check(snprintf(path, sizeof(path), "%s/timelines", store) > 0,
		  "build unknown length path");
	memset(&v2, 0, sizeof(v2));
	v2.magic = TEST_TIMELINE_MAGIC;
	v2.rec_len = sizeof(v2);
	v2.id = 7;
	v2.parent = 0;
	v2.branch_lsn = 100;
	v2.crc = fnv(&v2, sizeof(v2));
	bad_header[0] = TEST_TIMELINE_MAGIC;
	bad_header[1] = 999;
	check(write_bytes(path, &v2, sizeof(v2)) == 0 &&
		  append_bytes(path, bad_header, sizeof(bad_header)) == 0,
		  "append unknown modern record length");
	check(expect_open_failure(store), "unknown modern length fails closed");
	remove_tree(store);
}

static void
test_v2_and_mixed_lifecycle(void)
{
	char store[] = "/tmp/pagestore-timeline-test-XXXXXX";
	char timelines_path[512];
	TestTimelineV2 old;
	PsChannel result;
	PsTimelineState state;
	uint64_t incarnation;
	PsRetentionPin pin;
	unsigned char bad;

	check(mkdtemp(store) != NULL, "create timeline test store");
	page_size = 8192;
	segment_size = 1024 * 1024;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 0;
	cache_pages = 0;
	ps_nshards = 1;
	use_layers = 1;
	check(snprintf(timelines_path, sizeof(timelines_path), "%s/timelines",
					store) > 0, "build timeline log path");

	memset(&old, 0, sizeof(old));
	old.magic = TEST_TIMELINE_MAGIC;
	old.rec_len = sizeof(old);
	old.id = 7;
	old.parent = 0;
	old.branch_lsn = 100;
	old.crc = 0;
	old.crc = fnv(&old, sizeof(old));
	check(write_bytes(timelines_path, &old, sizeof(old)) == 0,
		  "write legacy V2 create record");
	check(ps_core_open(store) == 0, "replay legacy V2 create record");
	check(state_of(0, &state, &incarnation) && state == PS_TIMELINE_LIVE &&
		  incarnation == 1, "root defaults to LIVE incarnation 1");
	check(state_of(7, &state, &incarnation) && state == PS_TIMELINE_LIVE &&
		  incarnation == 1, "legacy V2 branch defaults to LIVE incarnation 1");
	check(wal_size_allowed(99),
		  "undefined timeline keeps pre-metadata shipped-WAL compatibility");
	check(create_branch(10, 0, 200), "append new mixed-format create event");
	check(state_of(10, &state, &incarnation) && state == PS_TIMELINE_LIVE &&
		  incarnation == 1, "new branch starts LIVE incarnation 1");

	/* Descendant veto is evaluated before any lifecycle record is appended. */
	check(create_branch(11, 10, 210), "create descendant for veto test");
	check(!begin_delete(10, 1, NULL) && state_of(10, &state, NULL) &&
		  state == PS_TIMELINE_LIVE, "live descendant vetoes deletion");

	/* A consistent retention snapshot vetoes the leaf transition. */
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 7;
	pin.owner_kind = PS_RETENTION_OWNER_READER;
	pin.owner_id = 7001;
	pin.generation = 1;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.lsn = 100;
	pin.admission_seq = 1;
	check(ps_retention_set(&pin) == PS_RETENTION_OK,
		  "install active retention owner");
	check(!begin_delete(7, 1, NULL) && state_of(7, &state, NULL) &&
		  state == PS_TIMELINE_LIVE, "retention owner vetoes deletion");
	check(ps_retention_drop(pin.timeline, pin.owner_kind, pin.owner_id,
									 pin.generation) == PS_RETENTION_OK,
		  "drop retention owner");
	check(!begin_delete(7, 0, NULL) && !begin_delete(7, 2, NULL) &&
		state_of(7, &state, &incarnation) && state == PS_TIMELINE_LIVE &&
		incarnation == 1, "wrong or zero deletion token is rejected");
	check(begin_delete(7, 1, &result) && result.result == PS_TIMELINE_DELETING &&
		  result.req_seq == 1, "durably begin leaf deletion");
	check(!begin_delete(7, 2, NULL) && begin_delete(7, 1, &result),
		  "deleting idempotency requires the same incarnation");
	check(begin_delete(7, 1, &result) && result.result == PS_TIMELINE_DELETING,
		  "BEGIN_DELETE is idempotent while deleting");
	check(!begin_delete(0, 1, NULL), "root deletion is rejected");
	check(!meta_exists(7), "core rejects ordinary metadata after deleting");
	check(!branch_request(PS_OP_CREATE_BRANCH, 7, 0, 100) &&
		  !branch_request(PS_OP_CHECK_BRANCH, 7, 0, 100) &&
		  !branch_request(PS_OP_REQUIRE_BRANCH, 7, 0, 100),
		  "deleting target rejects exact branch retries");

	check(create_branch(12, 0, 300), "create leaf for append failure test");
	check(create_branch(13, 0, 310), "create leaf for ambiguous append test");
	{
		struct stat before;
		struct stat after;

		check(stat(timelines_path, &before) == 0,
			  "stat timeline log before append failure");
		timeline_test_storage = PsStoragePosix;
		timeline_test_storage.meta_append = test_meta_append;
		timeline_append_mode = TIMELINE_APPEND_FAIL_BEFORE_WRITE;
		ps_storage = &timeline_test_storage;
		check(!begin_delete(12, 1, NULL) && !state_of(12, &state, NULL),
			  "append-before-write failure poisons timeline services");
		timeline_append_mode = TIMELINE_APPEND_NORMAL;
		ps_storage = &PsStoragePosix;
		check(stat(timelines_path, &after) == 0 &&
			  after.st_size == before.st_size,
			  "failed deletion append leaves log unchanged");
	}

	close_store();
	check(ps_core_open(store) == 0, "reopen mixed lifecycle log");
	check(state_of(7, &state, &incarnation) &&
		  state == PS_TIMELINE_DELETING && incarnation == 1,
		  "deleting state survives restart");
	check(state_of(12, &state, &incarnation) && state == PS_TIMELINE_LIVE &&
		  incarnation == 1, "failed deletion remains LIVE after restart");
	{
		timeline_test_storage = PsStoragePosix;
		timeline_test_storage.meta_append = test_meta_append;
		timeline_append_mode = TIMELINE_APPEND_AMBIGUOUS;
		ps_storage = &timeline_test_storage;
		check(!begin_delete(13, 1, NULL) && !state_of(13, &state, NULL) &&
			  !timeline_info(13) && !meta_exists(13) &&
			  !begin_delete(13, 1, NULL),
			  "ambiguous append poisons every timeline service");
		timeline_append_mode = TIMELINE_APPEND_NORMAL;
		ps_storage = &PsStoragePosix;
	}
	close_store();
	check(ps_core_open(store) == 0, "reopen after ambiguous append");
		check(state_of(13, &state, &incarnation) &&
			  state == PS_TIMELINE_DELETING && incarnation == 1,
			  "ambiguous append replays the durable deleting event");
	check(create_branch(14, 0, 320), "create lifecycle drain test timeline");
	check(test_lifecycle_drain(14),
		  "BEGIN_DELETE drains ordinary lifecycle readers and queues fairly");
	check(state_of(14, &state, NULL) && state == PS_TIMELINE_DELETING,
		  "ordinary-reader drain leaves timeline deleting");
	check(create_branch(15, 0, 330), "create maintenance drain test timeline");
	check(test_maintenance_drain(15),
		  "BEGIN_DELETE waits for complete maintenance invocation");
	check(state_of(15, &state, NULL) && state == PS_TIMELINE_DELETING,
		  "maintenance drain leaves timeline deleting");
	check(test_admission_drain(12) && state_of(12, &state, NULL) &&
		  state == PS_TIMELINE_DELETING,
		  "BEGIN_DELETE waits for mutation admission section to drain");
	close_store();

	/* A partial final event is discarded, while the valid prefix remains. */
	bad = 0xA5;
	check(append_bytes(timelines_path, &bad, 1) == 0,
		  "append simulated short lifecycle tail");
	check(ps_core_open(store) == 0 && access(timelines_path, F_OK) == 0,
		  "short lifecycle tail is truncated on replay");
	close_store();

	/* A complete record with a bad CRC is not repairable. */
	{
		int fd = open(timelines_path, O_RDWR);
		TestTimelineEvent event;

		check(fd >= 0 && pread(fd, &event, sizeof(event), sizeof(old)) ==
			  (ssize_t) sizeof(event), "read complete lifecycle event");
		if (fd >= 0)
		{
			event.crc ^= 1U;
			check(pwrite(fd, &event, sizeof(event), sizeof(old)) ==
				  (ssize_t) sizeof(event) && fsync(fd) == 0,
				  "corrupt complete lifecycle CRC");
			(void) close(fd);
		}
	}
	check(expect_open_failure(store), "complete lifecycle CRC fails closed");
	remove_tree(store);
}

int
main(void)
{
	test_legacy_migration_and_parser_fail_closed();
	test_v2_and_mixed_lifecycle();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
