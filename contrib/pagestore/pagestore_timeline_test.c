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
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_layer_store.h"
#include "pagestore_manifest.h"
#include "pagestore_retention.h"

#define TEST_TIMELINE_MAGIC 0x324d4c54U
#define TEST_FORK_META_V2_MAGIC 0x324d4b46U
#define TEST_FEV_SEG_GROW_BOUND 7
#define TEST_FEV_SEG_COMMIT_BOUND 8
#define TEST_SEG_CLAMPED_ADMISSION_MAGIC 0x53454738U

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

typedef struct TestForkMetaRecV2
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t timeline;
	PsKey	 key;
	uint64_t lsn;
	uint64_t admission_seq;
	uint64_t order_id;
	uint32_t nblocks;
	uint8_t	 kind;
	uint8_t	 pad[3];
} TestForkMetaRecV2;

typedef struct TestSegRecHdr
{
	uint32_t magic;
	uint32_t timeline;
	PsKey	 key;
	uint32_t block;
	uint64_t lsn;
	uint32_t len;
} TestSegRecHdr;

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
static PsLayerStore maintenance_store;
static uint32_t watched_maintenance_timeline;
static int watched_uploads;
static char cleanup_remote_dir[512];
static int cleanup_local_deletes;
static int cleanup_remote_deletes;
static int cleanup_remote_fail;

static int
counted_upload(const PsLayerDesc *layer)
{
	if (layer->timeline == watched_maintenance_timeline)
		watched_uploads++;
	return -1;
}

static int
cleanup_remote_uri(uint64_t layer_id, char *uri, uint32_t uri_len)
{
	int n = snprintf(uri, uri_len, "%s/layer-%llu", cleanup_remote_dir,
					 (unsigned long long) layer_id);

	return n < 0 || (uint32_t) n >= uri_len ? -1 : 0;
}

static int
cleanup_delete_local(const PsLayerDesc *layer)
{
	cleanup_local_deletes++;
	return PsLayerStoreLocal.delete_local_layer(layer);
}

static int
cleanup_delete_remote(const PsLayerDesc *layer)
{
	int rc = 0;

	cleanup_remote_deletes++;
	if (cleanup_remote_fail)
		return -1;
	for (uint32_t i = 0; i < layer->location_count; i++)
		if (layer->locations[i].tier == PS_LAYER_TIER_REMOTE_OBJECT &&
			layer->locations[i].available &&
			unlink(layer->locations[i].uri) != 0 && errno != ENOENT)
			rc = -1;
	return rc;
}
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
fixture_path(char *path, size_t path_len, const char *store,
			 const char *name)
{
	int n = snprintf(path, path_len, "%s/%s", store, name);

	return n >= 0 && (size_t) n < path_len;
}

static int
fixture_file(const char *store, const char *name)
{
	char path[1024];
	unsigned char byte = 0;

	return fixture_path(path, sizeof(path), store, name) &&
		write_bytes(path, &byte, sizeof(byte)) == 0;
}

static int
fixture_dir(const char *store, const char *name)
{
	char path[1024];

	return fixture_path(path, sizeof(path), store, name) &&
		(mkdir(path, 0700) == 0 || errno == EEXIST);
}

static int
fixture_exists(const char *store, const char *name)
{
	char path[1024];

	return fixture_path(path, sizeof(path), store, name) &&
		access(path, F_OK) == 0;
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
write_timeline_layer(uint32_t timeline, uint32_t block, uint32_t lsn_lo)
{
	unsigned char page[8192];
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};
	uint32_t lsn_hi = 0;
	int rc;

	memset(page, 0x5A, sizeof(page));
	memcpy(page, &lsn_hi, sizeof(lsn_hi));
	memcpy(page + sizeof(lsn_hi), &lsn_lo, sizeof(lsn_lo));
	ps_lock_shard_wr(ps_shard_of(&key));
	rc = append_page(timeline, &key, block, page, 0, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc;
}

static int
read_test_page(uint32_t timeline, uint32_t block, unsigned char *page)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};
	int rc;

	rc = read_resolve(timeline, &key, block, UINT64_MAX, 0, page, NULL);
	return rc;
}

static uint32_t
timeline_layer_fingerprint(uint32_t timeline, uint64_t *fingerprint,
						   int *remote_durable)
{
	uint32_t count = 0;
	uint64_t ids = 0;
	int remote = 0;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].timeline == timeline)
		{
			ids ^= ps_layer_map.layers[i].layer_id;
			remote |= ps_layer_map.layers[i].remote_durable;
			count++;
		}
	ps_unlock_map();
	*fingerprint = ids;
	*remote_durable = remote;
	return count;
}

static int
first_timeline_layer(uint32_t timeline)
{
	uint64_t layer_id = 0;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].timeline == timeline)
		{
			layer_id = ps_layer_map.layers[i].layer_id;
			break;
		}
	ps_unlock_map();
	return layer_id;
}

static int
timeline_has_deleting_layer(uint32_t timeline)
{
	int found = 0;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].timeline == timeline &&
			ps_layer_map.layers[i].deleting)
		{
			found = 1;
			break;
		}
	ps_unlock_map();
	return found;
}

static int
test_deleting_timeline_cleanup(const char *store, uint32_t timeline)
{
	uint64_t after_ids;
	uint64_t saved_segment_size = segment_size;
	uint64_t layer_id;
	uint32_t before_count;
	uint32_t after_count;
	uint32_t sibling = timeline + 1;
	int before_remote;
	int after_remote;
	int ok;
	int reopened;
	char orphan[512];
	int old_compact_layers = compact_layers;
	int old_segment_gc = segment_gc_enabled;

	/* Two page records cannot share a 16KiB segment, making segment 0 a due
	 * reclamation victim once both flushes publish their layer watermarks. */
	segment_size = 16384;
	compact_layers = 100;
	if (!create_branch(sibling, 0, 350) ||
		write_timeline_layer(0, 2, 50) != 0 ||
		write_timeline_layer(sibling, 3, 350) != 0 ||
		write_timeline_layer(timeline, 0, 100) != 0 ||
		write_timeline_layer(timeline, 1, 200) != 0)
	{
		segment_size = saved_segment_size;
		compact_layers = old_compact_layers;
		return 0;
	}
	before_count = timeline_layer_fingerprint(timeline, &after_ids,
										 &before_remote);
	if (before_count == 0 || ps_storage->seg_size(0, 0) <= 0 ||
		ps_storage->seg_size(0, 1) <= 0 || !begin_delete(timeline, 1, NULL))
	{
		segment_size = saved_segment_size;
		compact_layers = old_compact_layers;
		return 0;
	}
	layer_id = first_timeline_layer(timeline);
	if (snprintf(cleanup_remote_dir, sizeof(cleanup_remote_dir),
				 "%s/delete-objects", store) < 0 ||
		mkdir(cleanup_remote_dir, 0700) != 0 || layer_id == 0 ||
		cleanup_remote_uri(layer_id, orphan, sizeof(orphan)) < 0 ||
		write_bytes(orphan, "orphan", sizeof("orphan") - 1) != 0)
	{
		segment_size = saved_segment_size;
		compact_layers = old_compact_layers;
		return 0;
	}
	maintenance_store = PsLayerStoreLocal;
	maintenance_store.remote_uri = cleanup_remote_uri;
	maintenance_store.upload_layer = counted_upload;
	maintenance_store.delete_local_layer = cleanup_delete_local;
	maintenance_store.delete_remote_layer = cleanup_delete_remote;
	ps_layer_store = &maintenance_store;
	watched_maintenance_timeline = timeline;
	watched_uploads = 0;
	cleanup_local_deletes = 0;
	cleanup_remote_deletes = 0;
	segment_gc_enabled = 1;
	check(ps_core_maintenance() == 1 && timeline_has_deleting_layer(timeline),
		  "MARK_DELETE is the durable cleanup discovery boundary");
	ps_layer_store = &PsLayerStoreLocal;
	close_store();
	reopened = ps_core_open(store);
	check(reopened == 0 && timeline_has_deleting_layer(timeline),
		  "restart resumes a layer left after MARK_DELETE");
	ps_layer_store = &maintenance_store;
	for (int i = 0; i < 100; i++)
		(void) ps_core_maintenance();
	for (int i = 0; i < 100 && timeline_has_deleting_layer(timeline); i++)
	{
		(void) ps_core_maintenance();
		usleep(1000);
	}
	after_count = timeline_layer_fingerprint(timeline, &after_ids,
									  &after_remote);
	/* Explicit deletion cleanup removes only the target timeline's layers.  The
	 * sibling and parent remain discoverable, and shared segments remain intact. */
	{
		uint64_t ignored;
		int ignored_remote;
		uint32_t sibling_count = timeline_layer_fingerprint(sibling, &ignored,
											 &ignored_remote);
		uint32_t parent_count = timeline_layer_fingerprint(0, &ignored,
											&ignored_remote);
		int local_before = cleanup_local_deletes;
		int remote_before = cleanup_remote_deletes;

		ok = watched_uploads == 0 && cleanup_local_deletes > 0 &&
			cleanup_remote_deletes > 0 && access(orphan, F_OK) != 0 &&
			ps_storage->seg_size(0, 0) > 0 && after_count == 0 && after_ids == 0 &&
			!before_remote && !after_remote && sibling_count > 0 && parent_count > 0;
		(void) ps_core_maintenance();
		(void) ps_core_maintenance();
		check(cleanup_local_deletes == local_before &&
			  cleanup_remote_deletes == remote_before &&
			  !timeline_has_deleting_layer(timeline) &&
			  ps_storage->seg_size(0, 0) > 0,
			  "completed cleanup is idempotent and shared segment GC stays fenced");
	}
	/* Recovery must not rebuild the deleted owner's layers from retained shared
	 * segments, and the lifecycle tombstone must continue to reject ID reuse. */
	ps_layer_store = &PsLayerStoreLocal;
	close_store();
	reopened = ps_core_open(store);
	ps_layer_store = &maintenance_store;
	{
		PsTimelineState state;
		uint64_t ids;
		int remote;

		check(reopened == 0 &&
			  timeline_layer_fingerprint(timeline, &ids, &remote) == 0 &&
			  state_of(timeline, &state, NULL) && state == PS_TIMELINE_DELETING &&
			  ps_storage->seg_size(0, 0) > 0 &&
			  !create_branch(timeline, 0, 370),
			  "restart keeps cleanup complete, shared data, and ID-reuse fence");
	}
	/* A provider failure leaves the durable tombstone in place and backs off the
	 * asynchronous retry.  Clear the failure and let the same worker finish it. */
	check(create_branch(sibling + 1, 0, 360) &&
		  write_timeline_layer(sibling + 1, 4, 360) == 0 &&
		  begin_delete(sibling + 1, 1, NULL),
		  "create a timeline for remote-delete failure recovery");
	cleanup_remote_fail = 1;
	check(ps_core_maintenance() == 1, "mark failed remote cleanup layer deleting");
	(void) ps_core_maintenance();
	usleep(10000);
	(void) ps_core_maintenance();
	{
		int failed_remote = cleanup_remote_deletes;

		(void) ps_core_maintenance();
		check(cleanup_remote_deletes == failed_remote &&
			  timeline_has_deleting_layer(sibling + 1),
			  "remote cleanup failure backs off without busy-looping");
	}
	cleanup_remote_fail = 0;
	usleep(1100000);
	for (int i = 0; i < 100 && timeline_has_deleting_layer(sibling + 1); i++)
	{
		(void) ps_core_maintenance();
		usleep(1000);
	}
	check(!timeline_has_deleting_layer(sibling + 1),
		  "remote cleanup retry resumes after backoff");
	ps_layer_store = &PsLayerStoreLocal;
	segment_gc_enabled = old_segment_gc;
	segment_size = saved_segment_size;
	compact_layers = old_compact_layers;
	return ok;
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

typedef struct InterruptibleWriter
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	volatile sig_atomic_t stop;
	int done;
	int rc;
} InterruptibleWriter;

static void *lifecycle_reader_main(void *arg);

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

static void *
interruptible_writer_main(void *arg)
{
	InterruptibleWriter *writer = arg;
	int rc = ps_lifecycle_write_lock_interruptible(&writer->stop);

	if (rc == 0)
		ps_lifecycle_write_unlock();
	pthread_mutex_lock(&writer->mutex);
	writer->rc = rc;
	writer->done = 1;
	pthread_cond_broadcast(&writer->cond);
	pthread_mutex_unlock(&writer->mutex);
	return NULL;
}

static int
test_interruptible_lifecycle_writer(uint32_t timeline)
{
	AdmissionReader reader;
	LockObserver observer;
	InterruptibleWriter writer;
	pthread_t reader_thread;
	pthread_t writer_thread;
	int ok = 0;

	memset(&reader, 0, sizeof(reader));
	memset(&writer, 0, sizeof(writer));
	observer_init(&observer);
	pthread_mutex_init(&reader.mutex, NULL);
	pthread_cond_init(&reader.cond, NULL);
	pthread_mutex_init(&writer.mutex, NULL);
	pthread_cond_init(&writer.cond, NULL);
	reader.timeline = timeline;
	if (pthread_create(&reader_thread, NULL, lifecycle_reader_main, &reader) != 0)
		goto out;
	pthread_mutex_lock(&reader.mutex);
	while (!reader.ready)
		pthread_cond_wait(&reader.cond, &reader.mutex);
	pthread_mutex_unlock(&reader.mutex);
	ps_test_set_lifecycle_write_queued_hook(observe_writer_queued, &observer);
	if (pthread_create(&writer_thread, NULL, interruptible_writer_main, &writer) != 0)
	{
		pthread_mutex_lock(&reader.mutex);
		reader.release = 1;
		pthread_cond_broadcast(&reader.cond);
		pthread_mutex_unlock(&reader.mutex);
		pthread_join(reader_thread, NULL);
		goto clear_hook;
	}
	observer_wait(&observer, 0);
	writer.stop = 1;
	pthread_mutex_lock(&writer.mutex);
	while (!writer.done)
		pthread_cond_wait(&writer.cond, &writer.mutex);
	pthread_mutex_unlock(&writer.mutex);
	pthread_join(writer_thread, NULL);
	check(writer.rc == ECANCELED,
		  "stop-aware lifecycle writer withdraws from the queued writer set");
	pthread_mutex_lock(&reader.mutex);
	reader.release = 1;
	pthread_cond_broadcast(&reader.cond);
	pthread_mutex_unlock(&reader.mutex);
	pthread_join(reader_thread, NULL);
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	check(ps_lifecycle_write_lock() == 0,
		  "a canceled lifecycle writer leaves the normal writer path usable");
	ps_lifecycle_write_unlock();
	ps_lifecycle_read_lock();
	ps_lifecycle_read_unlock();
	ok = 1;
	goto out;

clear_hook:
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
out:
	ps_test_set_lifecycle_write_queued_hook(NULL, NULL);
	pthread_cond_destroy(&writer.cond);
	pthread_mutex_destroy(&writer.mutex);
	pthread_cond_destroy(&reader.cond);
	pthread_mutex_destroy(&reader.mutex);
	observer_destroy(&observer);
	return ok;
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
append_wal(uint32_t timeline, uint64_t start_lsn, const unsigned char *data,
		   uint32_t len)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_APPEND;
	ch.timeline = timeline;
	ch.req_lsn = start_lsn;
	ch.datalen = len;
	memcpy(ch.data, data, len);
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
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

/* Remove one target's ordered segment markers while preserving the rest of the
 * fixed forkmeta log.  This models a crash/loss of the marker after the page
 * body reached the segment, without changing the physical segment bytes. */
static int
strip_ordered_markers(const char *store, uint32_t timeline)
{
	char path[512];
	TestForkMetaRecV2 rec;
	off_t in = 0;
	off_t out = 0;
	int removed = 0;
	int fd;

	if (snprintf(path, sizeof(path), "%s/forkmeta", store) < 0)
		return -1;
	fd = open(path, O_RDWR);
	if (fd < 0)
		return -1;
	for (;;)
	{
		ssize_t n = pread(fd, &rec, sizeof(rec), in);

		if (n == 0)
			break;
		if (n != (ssize_t) sizeof(rec) ||
			rec.magic != TEST_FORK_META_V2_MAGIC ||
			rec.rec_len != sizeof(rec))
		{
			(void) close(fd);
			return -1;
		}
		in += (off_t) sizeof(rec);
		if ((rec.kind == TEST_FEV_SEG_GROW_BOUND ||
			 rec.kind == TEST_FEV_SEG_COMMIT_BOUND) &&
			rec.timeline == timeline)
		{
			removed++;
			continue;
		}
		if (pwrite(fd, &rec, sizeof(rec), out) != (ssize_t) sizeof(rec))
		{
			(void) close(fd);
			return -1;
		}
		out += (off_t) sizeof(rec);
	}
	if (ftruncate(fd, out) != 0 || fsync(fd) != 0 || close(fd) != 0)
		return -1;
	return removed == 1 ? 0 : -1;
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
test_delete_discards_unflushed_memtable(void)
{
	char store[] = "/tmp/pagestore-timeline-memtable-XXXXXX";
	uint64_t ids;
	int remote;

	check(mkdtemp(store) != NULL, "create unflushed-delete test store");
	page_size = 8192;
	segment_size = 1024 * 1024;
	flush_pages = 100;
	compact_layers = 100;
	segment_gc_enabled = 1;
	cache_pages = 0;
	ps_nshards = 1;
	use_layers = 1;
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100),
		  "open store for unflushed deletion");
	check(write_timeline_layer(1, 0, 100) == 0 &&
		  timeline_layer_fingerprint(1, &ids, &remote) == 0,
		  "leave deleting timeline page staged below flush threshold");
	check(begin_delete(1, 1, NULL),
		  "begin deletion after draining the staged writer");
	close_store();
	check(ps_core_open(store) == 0 &&
		  timeline_layer_fingerprint(1, &ids, &remote) == 0,
		  "shutdown and recovery do not recreate a deleting timeline layer");
	close_store();
	remove_tree(store);
}

static void
test_deleting_timeline_wal_cleanup(void)
{
	char store[] = "/tmp/pagestore-timeline-wal-cleanup-XXXXXX";
	char path[1024];
	PsShmHeader metrics;
	unsigned char wal_data[] = {0x11, 0x22, 0x33, 0x44};
	PsTimelineState state;
	int did;

	check(mkdtemp(store) != NULL, "create timeline WAL-cleanup store");
	configure_timeline_core();
	memset(&metrics, 0, sizeof(metrics));
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100) &&
		  create_branch(10, 0, 100),
		  "open WAL-cleanup store with target and prefix sibling");
	ps_core_set_metrics_header(&metrics);
	check(append_wal(1, 100, wal_data, sizeof(wal_data)) &&
		  metrics.wal_index_pending_bytes == sizeof(wal_data) &&
		  metrics.wal_index_lagging_timelines == 1,
		  "publish aggregate WAL-index metrics for target timeline");
	check(begin_delete(1, 1, NULL), "begin WAL-cleanup target deletion");
	check(fixture_file(store, "wal_1.rewrite.tmp") &&
		  fixture_file(store, "walidx_1_0") &&
		  fixture_file(store, "walidx_1_0_e00000000000000000001") &&
		  fixture_file(store, "walidx_1_0_e00000000000000000001.size") &&
		  fixture_dir(store, "wal_segments_1") &&
		  fixture_file(store, "wal_segments_1/wal_store_identity_v1") &&
		  fixture_file(store, "wal_segments_1/walv1_2_00000000000000000000") &&
		  fixture_dir(store, "walidx_snapshots_1") &&
		  fixture_file(store, "walidx_snapshots_1/walidx_manifest_v1") &&
		  fixture_file(store,
				   "walidx_snapshots_1/walidxg1_00000000000000000001_000") &&
		  fixture_file(store, "wal_10") && fixture_file(store, "walidx_10_0"),
		  "install target private WAL families and sibling artifacts");
	check(ps_core_maintenance() == 1, "private WAL cleanup reports progress");
	check(!fixture_exists(store, "wal_1"), "cleanup removes target flat WAL");
	check(!fixture_exists(store, "wal_1.rewrite.tmp"),
		  "cleanup removes target flat-WAL rewrite temporary");
	check(!fixture_exists(store, "walidx_1_0"),
		  "cleanup removes target WAL-index log");
	check(!fixture_exists(store, "wal_segments_1") &&
		  !fixture_exists(store, "walidx_snapshots_1"),
		  "cleanup removes target immutable WAL and WAL-index snapshots");
	check(fixture_exists(store, "wal_10") && fixture_exists(store, "walidx_10_0"),
		  "cleanup preserves prefix-matching sibling artifacts");
	check(metrics.wal_index_pending_bytes == 0 &&
		  metrics.wal_index_lagging_timelines == 0,
		  "purging target timeline refreshes aggregate WAL-index metrics");
	check(state_of(1, &state, NULL) && state == PS_TIMELINE_DELETING,
		  "private WAL cleanup does not publish DELETED early");

	check(create_branch(2, 0, 200) && begin_delete(2, 1, NULL) &&
		  fixture_file(store, "wal_2") && fixture_dir(store, "wal_segments_2") &&
		  fixture_file(store, "wal_segments_2/foreign") &&
		  fixture_file(store,
					   "walidx_2_00_e00000000000000000001.size.tmp.123.0") &&
		  fixture_file(store, "walidx_2_999"),
		  "install non-canonical base/shard artifact");
	check(ps_core_maintenance() == 0 && fixture_exists(store, "wal_2") &&
		  fixture_exists(store, "wal_segments_2/foreign") &&
		  fixture_exists(store,
					 "walidx_2_00_e00000000000000000001.size.tmp.123.0") &&
		  fixture_exists(store, "walidx_2_999"),
		  "non-canonical base/shard artifact fails closed before partial cleanup");
	check(fixture_path(path, sizeof(path), store, "wal_segments_2/foreign") &&
		  unlink(path) == 0 && ps_core_maintenance() == 0 &&
		  fixture_exists(store, "wal_2") &&
		  fixture_exists(store,
					 "walidx_2_00_e00000000000000000001.size.tmp.123.0") &&
		  fixture_exists(store, "walidx_2_999"),
		  "non-canonical base/shard artifact remains fail-closed");
	check(create_branch(4, 0, 300) && begin_delete(4, 1, NULL) &&
		  fixture_file(store, "wal_4") && ps_core_maintenance() == 1 &&
		  !fixture_exists(store, "wal_4") && fixture_exists(store, "wal_2") &&
		  fixture_exists(store,
					 "walidx_2_00_e00000000000000000001.size.tmp.123.0"),
		  "a blocked timeline does not starve a later cleanup");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_2_00_e00000000000000000001.size.tmp.123.0") &&
		  unlink(path) == 0 &&
		  fixture_path(path, sizeof(path), store, "walidx_2_999") &&
		  unlink(path) == 0,
		  "remove non-canonical WAL-index shard fixtures");
	check(fixture_file(store,
					   "walidx_2_0_e00000000000000000001.size.tmp.01.999") &&
		  ps_core_maintenance() == 0 && fixture_exists(store, "wal_2"),
		  "non-canonical watermark temporary fails closed");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_2_0_e00000000000000000001.size.tmp.01.999") &&
		  unlink(path) == 0 &&
		  fixture_file(store,
					   "walidx_2_0_e00000000000000000001.size.tmp.123.128") &&
		  ps_core_maintenance() == 0 && fixture_exists(store, "wal_2"),
		  "out-of-range watermark attempt fails closed");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_2_0_e00000000000000000001.size.tmp.123.128") &&
		  unlink(path) == 0 &&
		  fixture_file(store,
					   "walidx_2_0_e00000000000000000001.size.tmp.999999999999999999999999.0") &&
		  ps_core_maintenance() == 0 && fixture_exists(store, "wal_2"),
		  "overflowed watermark PID fails closed");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_2_0_e00000000000000000001.size.tmp.999999999999999999999999.0") &&
		  unlink(path) == 0 &&
		  fixture_file(store,
					   "walidx_2_0_e00000000000000000001.size.tmp.123.127"),
		  "maximum watermark attempt is canonical");
	check(ps_core_maintenance() == 1 && !fixture_exists(store, "wal_2"),
		  "canonical watermark temporary is removable");
	check(!fixture_exists(store, "wal_segments_2"),
		  "target-private cleanup leaves no immutable directory");
	check(create_branch(8, 0, 500) && begin_delete(8, 1, NULL) &&
		  fixture_file(store, "wal_8") &&
		  fixture_dir(store, "walidx_snapshots_8") &&
		  fixture_file(store,
					   "walidx_snapshots_8/walidx_manifest_v1.tmp.00000000000000000001.00000000000000000001.0"),
		  "install non-canonical manifest PID temporary");
	check(ps_core_maintenance() == 0 && fixture_exists(store, "wal_8") &&
		  fixture_exists(store,
					 "walidx_snapshots_8/walidx_manifest_v1.tmp.00000000000000000001.00000000000000000001.0"),
		  "manifest PID with leading zero fails closed");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_snapshots_8/walidx_manifest_v1.tmp.00000000000000000001.00000000000000000001.0") &&
		  unlink(path) == 0 &&
		  fixture_file(store,
					   "walidx_snapshots_8/walidx_prepared_v1.tmp.123.01"),
		  "install non-canonical prepared attempt temporary");
	check(ps_core_maintenance() == 0 && fixture_exists(store, "wal_8") &&
		  fixture_exists(store,
					 "walidx_snapshots_8/walidx_prepared_v1.tmp.123.01"),
		  "prepared attempt with leading zero fails closed");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_snapshots_8/walidx_prepared_v1.tmp.123.01") &&
		  unlink(path) == 0 &&
		  fixture_file(store,
					   "walidx_snapshots_8/walidxg1_00000000000000000001_000.tmp.123.128"),
		  "install out-of-range snapshot attempt temporary");
	check(ps_core_maintenance() == 0 && fixture_exists(store, "wal_8") &&
		  fixture_exists(store,
					 "walidx_snapshots_8/walidxg1_00000000000000000001_000.tmp.123.128"),
		  "snapshot attempt above 127 fails closed");
	check(fixture_path(path, sizeof(path), store,
					   "walidx_snapshots_8/walidxg1_00000000000000000001_000.tmp.123.128") &&
		  unlink(path) == 0 &&
		  fixture_file(store,
					   "walidx_snapshots_8/walidx_manifest_v1.tmp.00000000000000000001.123.0") &&
		  fixture_file(store,
					   "walidx_snapshots_8/walidx_prepared_v1.tmp.123.0") &&
		  fixture_file(store,
					   "walidx_snapshots_8/walidxg1_00000000000000000001_000.tmp.123.127"),
		  "install canonical snapshot temporaries");
	check(ps_core_maintenance() == 1 && !fixture_exists(store, "wal_8") &&
		  !fixture_exists(store, "walidx_snapshots_8"),
		  "canonical manifest, prepared, and shard temporaries are removable");
	check(create_branch(9, 0, 550) && begin_delete(9, 1, NULL) &&
		  fixture_file(store, "wal_9") &&
		  setenv("PAGESTORE_TEST_REMOVE_TIMELINE_CLEANUP_TARGET_BEFORE_UNLINK",
				 "1", 1) == 0,
		  "install disappearing shared-root target fixture");
	did = ps_core_maintenance();
	check(unsetenv("PAGESTORE_TEST_REMOVE_TIMELINE_CLEANUP_TARGET_BEFORE_UNLINK") == 0,
		  "disarm disappearing shared-root target hook");
	check(did == 1 && !fixture_exists(store, "wal_9"),
		  "ENOENT during delete scan does not become a false readdir failure");
	check(create_branch(5, 0, 400) && begin_delete(5, 1, NULL) &&
		  fixture_dir(store, "wal_segments_5") &&
		  fixture_file(store, "wal_segments_5/wal_store_identity_v1") &&
		  setenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_PRIVATE_DIR_FSYNC", "1", 1) == 0,
		  "install private-directory fsync failure fixture");
	ps_core_set_metrics_header(NULL);
	close_store();
	check(ps_core_open(store) == 0 &&
		  unsetenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_PRIVATE_DIR_FSYNC") == 0,
		  "reopen with private-directory fsync fault armed");
	did = ps_core_maintenance();
	check(did == 1 && fixture_exists(store, "wal_segments_5"),
		  "private-directory fsync failure leaves cleanup retryable");
	for (int i = 0; i < 4 && fixture_exists(store, "wal_segments_5"); i++)
		did = ps_core_maintenance();
	check(!fixture_exists(store, "wal_segments_5"),
		  "private-directory cleanup retry succeeds after fsync failure");

	check(create_branch(7, 0, 600) && begin_delete(7, 1, NULL) &&
		  fixture_file(store, "wal_7") &&
		  setenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_SHARED_SCAN", "1", 1) == 0,
		  "install shared-root readdir fault fixture");
	ps_core_set_metrics_header(NULL);
	close_store();
	check(ps_core_open(store) == 0 &&
		  unsetenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_SHARED_SCAN") == 0,
		  "reopen with shared-root readdir fault armed");
	did = ps_core_maintenance();
	check(did == 1 && fixture_exists(store, "wal_7"),
		  "shared-root readdir failure leaves cleanup retryable");
	for (int i = 0; i < 4 && fixture_exists(store, "wal_7"); i++)
		did = ps_core_maintenance();
	check(!fixture_exists(store, "wal_7"),
		  "shared-root scan retry closes its directory before retrying");

	check(create_branch(3, 0, 300) && begin_delete(3, 1, NULL) &&
		  fixture_file(store, "wal_3") && fixture_dir(store, "wal_segments_3") &&
		  fixture_file(store, "wal_segments_3/wal_store_identity_v1") &&
		  fixture_file(store, "wal_segments_3/walv1_4_00000000000000000000") &&
		  setenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_AFTER_ROOT", "1", 1) == 0,
		  "install interrupted WAL-cleanup fixture");
	check(ps_core_maintenance() == 0 && !fixture_exists(store, "wal_3") &&
		  fixture_exists(store, "wal_segments_3"),
		  "cleanup interruption leaves a restartable private-directory suffix");
	unsetenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_AFTER_ROOT");
	close_store();
	check(ps_core_open(store) == 0, "reopen after interrupted WAL cleanup");
	for (int i = 0; i < 4 && fixture_exists(store, "wal_segments_3"); i++)
		(void) ps_core_maintenance();
	check(
		  !fixture_exists(store, "wal_segments_3") &&
		  fixture_exists(store, "wal_10") &&
		  state_of(3, &state, NULL) && state == PS_TIMELINE_DELETING,
		  "restart skips partial target recovery and resumes owner-scoped cleanup");
	ps_core_set_metrics_header(NULL);
	close_store();
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
	check(test_interruptible_lifecycle_writer(14),
		  "SIGTERM-aware BEGIN_DELETE writer exits before shutdown joins");
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
	check(create_branch(16, 0, 340),
		  "create deleting-timeline maintenance test timeline");
	check(test_deleting_timeline_cleanup(store, 16),
		  "DELETING timeline cleanup is owner-scoped and restartable");
	close_store();

	/* A partial final event is discarded, while the valid prefix remains. */
	bad = 0xA5;
	check(append_bytes(timelines_path, &bad, 1) == 0,
		  "append simulated short lifecycle tail");
	{
		int reopened = ps_core_open(store);

		check(reopened == 0, "short lifecycle tail reopens successfully");
		check(access(timelines_path, F_OK) == 0,
			  "short lifecycle tail is truncated on replay");
	}
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

static void
test_deleting_timeline_page_cleanup(void)
{
	char store[] = "/tmp/pagestore-timeline-page-cleanup-XXXXXX";
	unsigned char page[8192];
	PsTimelineState state;
	int64_t before, after;

	configure_timeline_core();
	segment_size = 1024 * 1024;
	flush_pages = 1; /* force a watermark in the mixed source segment */
	cache_pages = 8;
	check(mkdtemp(store) != NULL, "create shared page-cleanup store");
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100) &&
			create_branch(10, 0, 100),
		  "open shared page-cleanup store with 1/10 siblings");
	check(write_timeline_layer(0, 2, 50) == 0 &&
			write_timeline_layer(1, 0, 100) == 0 &&
			write_timeline_layer(10, 1, 200) == 0,
		  "write parent, target, and sibling into one segment");
		memset(page, 0, sizeof(page));
		check(read_test_page(1, 0, page) == 1 && page[100] == 0x5A,
			  "populate target page cache before deletion");
	before = ps_storage->seg_size(0, 0);
	check(before > 0 && begin_delete(1, 1, NULL),
		  "begin shared page-segment deletion");
	for (int i = 0; i < 32; i++)
		(void) ps_core_maintenance();
	after = ps_storage->seg_size(0, 0);
	check(after > 0 && after < before,
		  "mixed segment is atomically filtered without unlinking it");
	memset(page, 0, sizeof(page));
	check(read_test_page(1, 0, page) == 0,
		  "target page and cache reference are gone");
	memset(page, 0, sizeof(page));
	check(read_test_page(10, 1, page) == 1 && page[100] == 0x5A,
		  "timeline 10 sibling bytes survive byte-for-byte");
	check(state_of(1, &state, NULL) && state == PS_TIMELINE_DELETING,
		  "page cleanup does not publish DELETED");
	check(!create_branch(1, 0, 300), "page cleanup preserves ID-reuse fence");
	close_store();
	check(ps_core_open(store) == 0,
			"restart resumes from the filtered shared segment");
	memset(page, 0, sizeof(page));
	check(state_of(1, &state, NULL) && state == PS_TIMELINE_DELETING,
			"restart preserves deleting state during page cleanup");
	check(read_test_page(10, 1, page) == 1 && page[100] == 0x5A,
			"restart keeps sibling after page cleanup");
	memset(page, 0, sizeof(page));
	check(read_test_page(1, 0, page) == 0,
			"restart does not resurrect target page");
	close_store();
	remove_tree(store);

	/* A segment containing only the target becomes an empty canonical segment;
	 * its identity remains so recovery and the append cursor stay unambiguous. */
	strcpy(store, "/tmp/pagestore-timeline-page-cleanup-XXXXXX");
	check(mkdtemp(store) != NULL, "create pure-target page-cleanup store");
	configure_timeline_core();
	segment_size = 16384;
	flush_pages = 100;
	check(ps_core_open(store) == 0 && create_branch(2, 0, 100) &&
			write_timeline_layer(2, 0, 100) == 0 && begin_delete(2, 1, NULL),
		  "write pure-target segment");
	for (int i = 0; i < 32; i++)
		(void) ps_core_maintenance();
	check(ps_storage->seg_size(0, 0) == 0,
		  "pure-target segment is replaced by an empty segment");
	close_store();
	remove_tree(store);
}

static void
test_deleting_timeline_page_cleanup_fail_closed(void)
{
	char store[] = "/tmp/pagestore-timeline-page-malformed-XXXXXX";
	unsigned char page[8192];
	uint32_t bad = 0xdeadbeefU;
	int64_t valid_size;

	configure_timeline_core();
	segment_size = 1024 * 1024;
	flush_pages = 100;
	check(mkdtemp(store) != NULL, "create malformed page-cleanup store");
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100) &&
			create_branch(10, 0, 100) &&
			write_timeline_layer(1, 0, 100) == 0 &&
			write_timeline_layer(10, 1, 200) == 0,
		  "write target and sibling before malformed tail");
	valid_size = ps_storage->seg_size(0, 0);
	check(valid_size > 0 &&
			ps_storage->seg_write(0, 0, (uint64_t) valid_size, &bad,
								 sizeof(bad)) == 0 && ps_storage->sync() == 0 &&
			begin_delete(1, 1, NULL),
		  "install a truncated record tail after BEGIN_DELETE target");
	for (int i = 0; i < 16; i++)
		(void) ps_core_maintenance();
	check(ps_storage->seg_size(0, 0) == valid_size + (int64_t) sizeof(bad),
		  "malformed segment cleanup stays fail-closed and retryable");
	memset(page, 0, sizeof(page));
	check(read_test_page(1, 0, page) == 1,
		  "failed cleanup retains the target until repair");
	close_store();
	remove_tree(store);
}

static void
test_deleting_timeline_page_cleanup_oversized(void)
{
	char store[] = "/tmp/pagestore-timeline-page-oversized-XXXXXX";
	unsigned char page[8192];
	unsigned char sparse_tail = 0;
	PsTimelineState state;
	int64_t oversized;

	configure_timeline_core();
	segment_size = 32768;
	flush_pages = 100;
	cache_pages = 0;
	check(mkdtemp(store) != NULL, "create oversized page-cleanup store");
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100) &&
			create_branch(10, 0, 100) &&
			write_timeline_layer(1, 0, 100) == 0 &&
			write_timeline_layer(10, 1, 200) == 0,
			"write target and sibling before oversized sparse tail");
	check(ps_storage->seg_write(0, 0, segment_size, &sparse_tail,
								 sizeof(sparse_tail)) == 0 &&
			ps_storage->sync() == 0 && begin_delete(1, 1, NULL),
			"extend mixed segment beyond configured size before cleanup");
	oversized = ps_storage->seg_size(0, 0);
	check(oversized == (int64_t) segment_size + 1,
			"oversized sparse segment is present");
	for (int i = 0; i < 16; i++)
		(void) ps_core_maintenance();
	check(ps_storage->seg_size(0, 0) == oversized,
			"oversized segment fails closed without partial replacement");
	memset(page, 0, sizeof(page));
	check(read_test_page(1, 0, page) == 1 && page[100] == 0x5A,
			"oversized cleanup failure retains target page");
	memset(page, 0, sizeof(page));
	check(read_test_page(10, 1, page) == 1 && page[100] == 0x5A,
			"oversized cleanup failure leaves sibling undamaged");
	check(state_of(1, &state, NULL) && state == PS_TIMELINE_DELETING,
			"oversized cleanup failure remains retryable in DELETING");
	close_store();
	remove_tree(store);
}

static void
test_deleting_timeline_page_cleanup_prefix_hole(void)
{
	char store[] = "/tmp/pagestore-timeline-page-hole-XXXXXX";
	unsigned char page[8192];
	int64_t before;

	/* Two records fill the first segment; the target and sibling then land in
	 * segment 1.  Removing segment 0 models prefix GC leaving a later mixed
	 * segment that cleanup must still visit. */
	configure_timeline_core();
	segment_size = 20000;
	flush_pages = 100;
	cache_pages = 0;
	check(mkdtemp(store) != NULL, "create prefix-hole page-cleanup store");
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100) &&
			create_branch(10, 0, 100) &&
			write_timeline_layer(0, 2, 50) == 0 &&
			write_timeline_layer(0, 3, 60) == 0 &&
			write_timeline_layer(1, 0, 100) == 0 &&
			write_timeline_layer(10, 1, 200) == 0,
			"write prefix filler and later target/sibling records");
	before = ps_storage->seg_size(0, 1);
	check(before > 0 && ps_storage->seg_remove(0, 0) == 0 &&
			begin_delete(1, 1, NULL),
			"remove prefix segment before deleting later target");
	for (int i = 0; i < 32; i++)
		(void) ps_core_maintenance();
	check(ps_storage->seg_size(0, 1) > 0 &&
			ps_storage->seg_size(0, 1) < before,
			"cleanup crosses prefix hole and filters later mixed segment");
	memset(page, 0, sizeof(page));
	check(read_test_page(1, 0, page) == 0,
			"prefix-hole cleanup removes target page");
	memset(page, 0, sizeof(page));
	check(read_test_page(10, 1, page) == 1 && page[100] == 0x5A,
			"prefix-hole cleanup preserves later sibling page");
	close_store();
	remove_tree(store);
}

static void
test_deleting_timeline_page_cleanup_retired_short_segment(void)
{
	char store[] = "/tmp/pagestore-timeline-retired-short-XXXXXX";
	unsigned char page[8192];
	PsTimelineState state;
	TestSegRecHdr sibling_hdr;
	int64_t before;
	int64_t after;

	/* A missing ordered forkmeta marker for the still-live sibling makes
	 * recovery retire the physical segment at cur_off == segment_size even
	 * though the file is short.  The deleting target follows it physically. */
	configure_timeline_core();
	segment_size = 32768;
	flush_pages = 100;
	use_layers = 0; /* keep the recovery case on the physical segment path */
	check(mkdtemp(store) != NULL, "create retired-short page-cleanup store");
	check(ps_core_open(store) == 0 && create_branch(1, 0, 100) &&
			create_branch(10, 0, 100) &&
			/* The below-branch-floor LSN forces the sibling into the ordered
			 * SEG8 shape whose forkmeta marker recovery must validate. */
			write_timeline_layer(10, 1, 50) == 0 &&
			write_timeline_layer(1, 0, 100) == 0,
			"write target and sibling into one short segment");
	before = ps_storage->seg_size(0, 0);
	check(before > 0 && before < (int64_t) segment_size &&
			begin_delete(1, 1, NULL) && ps_storage->sync() == 0,
			"begin deletion before simulating a missing ordered marker");
	close_store();
	check(strip_ordered_markers(store, 10) == 0,
			"remove only the live sibling ordered forkmeta marker");

	check(ps_core_open(store) == 0,
			"restart accepts a short segment with a retired recovery cursor");
	for (int i = 0; i < 32; i++)
		(void) ps_core_maintenance();
	after = ps_storage->seg_size(0, 0);
	check(after > 0 && after < before,
			"DELETING cleanup filters a retired short segment");
	check(state_of(1, &state, NULL) && state == PS_TIMELINE_DELETING,
			"retired-segment cleanup keeps the target DELETING");
	memset(page, 0, sizeof(page));
	check(ps_storage->seg_read(0, 0, 0, &sibling_hdr,
						 sizeof(sibling_hdr)) == 0 &&
			sibling_hdr.magic == TEST_SEG_CLAMPED_ADMISSION_MAGIC &&
			sibling_hdr.timeline == 10 && sibling_hdr.block == 1 &&
			ps_storage->seg_read(0, 0,
						 sizeof(sibling_hdr) + 2 * sizeof(uint64_t),
						 page, page_size) == 0 && page[100] == 0x5A,
			"filtered retired segment preserves sibling bytes");

	/* The sentinel must survive the rewrite.  A subsequent live write therefore
	 * starts segment 1 instead of appending to the compacted physical tail. */
	check(write_timeline_layer(10, 2, 300) == 0 &&
			ps_storage->seg_size(0, 0) == after &&
			ps_storage->seg_size(0, 1) > 0,
			"next write rolls past the retired compacted segment");
	close_store();
	check(ps_core_open(store) == 0,
			"restart after retired-segment cleanup and rolled append");
	memset(page, 0, sizeof(page));
	check(read_test_page(10, 2, page) == 1 && page[100] == 0x5A,
			"rolled append survives retired-segment recovery");
	check(read_test_page(1, 0, page) == 0,
			"target page stays absent after retired-segment recovery");
	close_store();
	remove_tree(store);
}

int
main(void)
{
	test_legacy_migration_and_parser_fail_closed();
	test_delete_discards_unflushed_memtable();
	test_deleting_timeline_wal_cleanup();
	test_deleting_timeline_page_cleanup();
	test_deleting_timeline_page_cleanup_fail_closed();
	test_deleting_timeline_page_cleanup_oversized();
	test_deleting_timeline_page_cleanup_prefix_hole();
	test_deleting_timeline_page_cleanup_retired_short_segment();
	test_v2_and_mixed_lifecycle();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
