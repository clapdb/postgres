/*-------------------------------------------------------------------------
 *
 * pagestore_inspect.c
 *    Read-only inspection client for a running pagestore daemon.
 *
 * Aggregate diagnostics are mapped read-only.  The relation operation maps
 * the private shared-memory object writable only to publish its dedicated
 * read-only request mailbox; it never claims an ordinary I/O channel.  This
 * is for the harness and diagnostics, not a production management API.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"

static void
usage(const char *prog)
{
	fprintf(stderr, "usage: %s --shm NAME health|timeline ID|manifest|gc|"
			"owners|backpressure|pruning|relation TIMELINE INCARNATION "
			"SPC DB REL LSN\n",
			prog);
}

static int
parse_decimal_u64(const char *text, uint64_t *value)
{
	uint64_t parsed = 0;

	if (text == NULL || *text == '\0')
		return -1;
	for (const unsigned char *p = (const unsigned char *) text; *p; p++)
	{
		uint32_t digit;

		if (*p < '0' || *p > '9')
			return -1;
		digit = (uint32_t) (*p - '0');
		if (parsed > (UINT64_MAX - digit) / 10)
			return -1;
		parsed = parsed * 10 + digit;
	}
	*value = parsed;
	return 0;
}

static int
parse_decimal_u32(const char *text, uint32_t *value)
{
	uint64_t parsed;

	if (parse_decimal_u64(text, &parsed) != 0 || parsed > UINT32_MAX)
		return -1;
	*value = (uint32_t) parsed;
	return 0;
}

static uint64_t
inspect_monotonic_ns(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
		(uint64_t) now.tv_sec > UINT64_MAX / UINT64_C(1000000000))
		return 0;
	return (uint64_t) now.tv_sec * UINT64_C(1000000000) +
		(uint64_t) now.tv_nsec;
}

#define INSPECTION_TIMEOUT_NS UINT64_C(5000000000)

static int
set_inspection_lock(int fd, off_t byte, short type)
{
	struct flock lock;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = type;
	lock.l_whence = SEEK_SET;
	lock.l_start = byte;
	lock.l_len = 1;
	return fcntl(fd, F_SETLK, &lock);
}

static int
lock_relation_shm(int fd)
{
	if (set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE, F_WRLCK) == 0)
		return 0;
	if (errno == EACCES || errno == EAGAIN)
	{
		fprintf(stderr, "pagestore_inspect: relation inspection mailbox is busy\n");
		return 1;
	}
	perror("pagestore_inspect: fcntl relation mailbox lock");
	return -1;
}

static void
unlock_relation_shm(int fd)
{
	if (set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE, F_UNLCK) != 0)
		perror("pagestore_inspect: fcntl relation mailbox unlock");
}

/* Return 1 when no daemon owns byte one, 0 when a daemon still owns it, and
 * -1 for an unexpected error.  The caller holds byte zero, so a new daemon
 * cannot pass initialization while this probe is in flight. */
static int
daemon_lease_is_free(int fd)
{
	if (set_inspection_lock(fd, PS_INSPECTION_DAEMON_LOCK_BYTE, F_WRLCK) == 0)
	{
		if (set_inspection_lock(fd, PS_INSPECTION_DAEMON_LOCK_BYTE,
								F_UNLCK) != 0)
		{
			perror("pagestore_inspect: fcntl daemon lease unlock");
			return -1;
		}
		return 1;
	}
	if (errno == EACCES || errno == EAGAIN)
		return 0;
	perror("pagestore_inspect: fcntl daemon lease probe");
	return -1;
}

/* The request slot is protected by byte zero against other inspectors and
 * daemon initialization.  Only after the bounded wait and a successful
 * byte-one probe may REQUEST/BUSY be reclaimed: the old daemon's process lock
 * has then been released by exit, and byte zero prevents a new daemon from
 * entering initialization concurrently. */
static int
recover_relation_mailbox(int fd, PsInspectionRequest *request)
{
	uint64_t now;
	uint64_t deadline;

	now = inspect_monotonic_ns();
	if (now == 0 || UINT64_MAX - now < INSPECTION_TIMEOUT_NS)
		return -1;
	deadline = now + INSPECTION_TIMEOUT_NS;
	for (;;)
	{
		uint32_t state = ps_load_acquire(&request->state);

		if (state == PS_INSPECTION_STATE_IDLE)
			return 0;
		if (state == PS_INSPECTION_STATE_DONE)
		{
			if (ps_cas(&request->state, PS_INSPECTION_STATE_DONE,
					   PS_INSPECTION_STATE_IDLE))
				return 0;
			continue;
		}
		if (state != PS_INSPECTION_STATE_REQUEST &&
			state != PS_INSPECTION_STATE_BUSY)
		{
			fprintf(stderr, "pagestore_inspect: invalid relation mailbox state\n");
			return -1;
		}
		now = inspect_monotonic_ns();
		if (now == 0 || now >= deadline)
		{
			int lease_free = daemon_lease_is_free(fd);

			if (lease_free != 1)
			{
				fprintf(stderr, "pagestore_inspect: abandoned relation request did not converge\n");
				return -1;
			}
			if (ps_cas(&request->state, state, PS_INSPECTION_STATE_IDLE))
				return 0;
		}
		usleep(1000);
	}
}

static int
valid_relation_result(const PsInspectionRelationResult *result)
{
	int has_main_fork = 0;

	if (result->exists > 1 ||
		result->fork_count > PS_INSPECTION_RELATION_MAX_FORKS ||
		result->selected_version_available != 0 || result->reserved != 0)
		return 0;
	for (uint32_t i = 0; i < result->fork_count; i++)
	{
		if (result->forks[i].fork_num < 0 ||
			result->forks[i].fork_num >= PS_INSPECTION_RELATION_MAX_FORKS ||
			(i != 0 && result->forks[i - 1].fork_num >=
				result->forks[i].fork_num))
			return 0;
		if (result->forks[i].fork_num == 0)
			has_main_fork = 1;
	}
	/* The core defines relation existence by the main fork.  Auxiliary forks
	 * may be present independently, but the aggregate flag must agree with
	 * the presence of fork 0. */
	return (result->exists != 0) == has_main_fork;
}

static int
header_valid(const PsShmHeader *hdr)
{
	return hdr->magic == PS_SHM_MAGIC &&
		hdr->version == PS_SHM_VERSION &&
		__atomic_load_n(&hdr->startup_state, __ATOMIC_ACQUIRE) == PS_SHM_READY &&
		hdr->page_size != 0 && hdr->page_size <= PS_IO_UNIT &&
		hdr->io_unit == PS_IO_UNIT &&
		hdr->nchannels != 0 && hdr->nchannels <= PS_MAX_CHANNELS &&
		hdr->nshards != 0 && hdr->nshards <= hdr->nchannels &&
		hdr->channel_stride == PS_CHANNEL_STRIDE &&
		hdr->channels_off == PS_CHANNELS_OFF;
}

static void
print_health(PsShmHeader *hdr)
{
	printf("{\"protocol_version\":%u,\"page_size\":%u,\"io_unit\":%u,"
		   "\"nchannels\":%u,\"nshards\":%u,\"admission_fence_epoch\":%llu,"
		   "\"admission_pending_epoch\":%llu,\"admission_pending_lsn\":%llu}\n",
		   hdr->version, hdr->page_size, hdr->io_unit, hdr->nchannels,
		   hdr->nshards,
		   (unsigned long long) ps_load_acquire_u64(&hdr->admission_fence_epoch),
		   (unsigned long long) ps_load_acquire_u64(&hdr->admission_pending_epoch),
		   (unsigned long long) ps_load_acquire_u64(&hdr->admission_pending_lsn));
}

static void
read_backpressure_metrics(PsShmHeader *hdr, PsBackpressureMetrics *page,
						   PsBackpressureMetrics *wal,
						   PsBackpressureMetrics *walidx,
						   PsBackpressureMetrics *forkmeta)
{
	const unsigned int max_attempts = 10000;
	uint64_t before;
	uint64_t after;

	for (unsigned int attempt = 0; attempt < max_attempts; attempt++)
	{
		before = ps_load_acquire_u64(&hdr->backpressure_metrics_seq);
		if (before & 1)
		{
			usleep(100);
			continue;
		}
		page->lag_bytes = ps_load_acquire_u64(&hdr->page_backpressure.lag_bytes);
		page->high_water_bytes =
			ps_load_acquire_u64(&hdr->page_backpressure.high_water_bytes);
		page->catchup_bytes =
			ps_load_acquire_u64(&hdr->page_backpressure.catchup_bytes);
		page->throttled = ps_load_acquire(&hdr->page_backpressure.throttled);
		page->throttle_enters =
			ps_load_acquire_u64(&hdr->page_backpressure.throttle_enters);
		page->throttle_exits =
			ps_load_acquire_u64(&hdr->page_backpressure.throttle_exits);
		page->foreground_wait_ns =
			ps_load_acquire_u64(&hdr->page_backpressure.foreground_wait_ns);
		*wal = *page;
		wal->lag_bytes = ps_load_acquire_u64(&hdr->wal_backpressure.lag_bytes);
		wal->high_water_bytes =
			ps_load_acquire_u64(&hdr->wal_backpressure.high_water_bytes);
		wal->catchup_bytes =
			ps_load_acquire_u64(&hdr->wal_backpressure.catchup_bytes);
		wal->throttled = ps_load_acquire(&hdr->wal_backpressure.throttled);
		wal->throttle_enters =
			ps_load_acquire_u64(&hdr->wal_backpressure.throttle_enters);
		wal->throttle_exits =
			ps_load_acquire_u64(&hdr->wal_backpressure.throttle_exits);
		wal->foreground_wait_ns =
			ps_load_acquire_u64(&hdr->wal_backpressure.foreground_wait_ns);
		*walidx = *page;
		walidx->lag_bytes = ps_load_acquire_u64(&hdr->walidx_backpressure.lag_bytes);
		walidx->high_water_bytes = ps_load_acquire_u64(&hdr->walidx_backpressure.high_water_bytes);
		walidx->catchup_bytes = ps_load_acquire_u64(&hdr->walidx_backpressure.catchup_bytes);
		walidx->throttled = ps_load_acquire(&hdr->walidx_backpressure.throttled);
		walidx->throttle_enters = ps_load_acquire_u64(&hdr->walidx_backpressure.throttle_enters);
		walidx->throttle_exits = ps_load_acquire_u64(&hdr->walidx_backpressure.throttle_exits);
		walidx->foreground_wait_ns = ps_load_acquire_u64(&hdr->walidx_backpressure.foreground_wait_ns);
		*forkmeta = *page;
		forkmeta->lag_bytes = ps_load_acquire_u64(&hdr->forkmeta_backpressure.lag_bytes);
		forkmeta->high_water_bytes = ps_load_acquire_u64(&hdr->forkmeta_backpressure.high_water_bytes);
		forkmeta->catchup_bytes = ps_load_acquire_u64(&hdr->forkmeta_backpressure.catchup_bytes);
		forkmeta->throttled = ps_load_acquire(&hdr->forkmeta_backpressure.throttled);
		forkmeta->throttle_enters = ps_load_acquire_u64(&hdr->forkmeta_backpressure.throttle_enters);
		forkmeta->throttle_exits = ps_load_acquire_u64(&hdr->forkmeta_backpressure.throttle_exits);
		forkmeta->foreground_wait_ns = ps_load_acquire_u64(&hdr->forkmeta_backpressure.foreground_wait_ns);
		after = ps_load_acquire_u64(&hdr->backpressure_metrics_seq);
		if (before == after && !(after & 1))
			return;
		usleep(100);
	}
	fprintf(stderr, "pagestore_inspect: backpressure metrics stayed unstable\n");
	exit(1);
}

static void
print_backpressure(void *shm, PsShmHeader *hdr)
{
	uint32_t idle = 0;
	uint32_t claimed = 0;
	uint32_t request = 0;
	uint32_t done = 0;
	PsBackpressureMetrics page;
	PsBackpressureMetrics wal;
	PsBackpressureMetrics walidx;
	PsBackpressureMetrics forkmeta;

	for (uint32_t i = 0; i < hdr->nchannels; i++)
	{
		PsChannel  *ch = ps_channel(shm, i);

		if (ps_load_acquire(&ch->claimed) != 0)
			claimed++;
		switch (ps_load_acquire(&ch->state))
		{
			case PS_STATE_IDLE:
				idle++;
				break;
			case PS_STATE_REQUEST:
				request++;
				break;
			case PS_STATE_DONE:
				done++;
				break;
			default:
				fprintf(stderr, "pagestore_inspect: invalid channel state\n");
				exit(1);
		}
	}
	read_backpressure_metrics(hdr, &page, &wal, &walidx, &forkmeta);
	printf("{\"idle\":%u,\"claimed\":%u,\"request\":%u,"
		   "\"done\":%u,\"shards\":%u,\"wal_index_pending_bytes\":%llu,"
		   "\"wal_index_lagging_timelines\":%u,"
		   "\"page_lag_bytes\":%llu,\"page_high_water_bytes\":%llu,"
		   "\"page_catchup_bytes\":%llu,\"page_throttled\":%u,"
		   "\"page_throttle_enters\":%llu,\"page_throttle_exits\":%llu,"
		   "\"page_foreground_wait_ns\":%llu,"
		   "\"wal_lag_bytes\":%llu,\"wal_high_water_bytes\":%llu,"
		   "\"wal_catchup_bytes\":%llu,\"wal_throttled\":%u,"
		   "\"wal_throttle_enters\":%llu,\"wal_throttle_exits\":%llu,"
		   "\"wal_foreground_wait_ns\":%llu,"
		   "\"walidx_lag_bytes\":%llu,\"walidx_high_water_bytes\":%llu,"
		   "\"walidx_catchup_bytes\":%llu,\"walidx_throttled\":%u,"
		   "\"walidx_throttle_enters\":%llu,\"walidx_throttle_exits\":%llu,"
		   "\"walidx_foreground_wait_ns\":%llu,"
		   "\"forkmeta_lag_bytes\":%llu,\"forkmeta_high_water_bytes\":%llu,"
		   "\"forkmeta_catchup_bytes\":%llu,\"forkmeta_throttled\":%u,"
		   "\"forkmeta_throttle_enters\":%llu,\"forkmeta_throttle_exits\":%llu,"
		   "\"forkmeta_foreground_wait_ns\":%llu}\n",
		   idle, claimed, request, done, hdr->nshards,
		   (unsigned long long) ps_load_acquire_u64(&hdr->wal_index_pending_bytes),
		   ps_load_acquire(&hdr->wal_index_lagging_timelines),
		   (unsigned long long) page.lag_bytes,
		   (unsigned long long) page.high_water_bytes,
		   (unsigned long long) page.catchup_bytes, page.throttled,
		   (unsigned long long) page.throttle_enters,
		   (unsigned long long) page.throttle_exits,
		   (unsigned long long) page.foreground_wait_ns,
		   (unsigned long long) wal.lag_bytes,
		   (unsigned long long) wal.high_water_bytes,
		   (unsigned long long) wal.catchup_bytes, wal.throttled,
		   (unsigned long long) wal.throttle_enters,
		   (unsigned long long) wal.throttle_exits,
		   (unsigned long long) wal.foreground_wait_ns,
		   (unsigned long long) walidx.lag_bytes,
		   (unsigned long long) walidx.high_water_bytes,
		   (unsigned long long) walidx.catchup_bytes, walidx.throttled,
		   (unsigned long long) walidx.throttle_enters,
		   (unsigned long long) walidx.throttle_exits,
		   (unsigned long long) walidx.foreground_wait_ns,
		   (unsigned long long) forkmeta.lag_bytes,
		   (unsigned long long) forkmeta.high_water_bytes,
		   (unsigned long long) forkmeta.catchup_bytes, forkmeta.throttled,
		   (unsigned long long) forkmeta.throttle_enters,
		   (unsigned long long) forkmeta.throttle_exits,
		   (unsigned long long) forkmeta.foreground_wait_ns);
}

static void
print_pruning(PsShmHeader *hdr)
{
	const unsigned int max_attempts = 10000;
	uint64_t seq_before,
			 seq_after,
			 compactions,
			 scanned,
			 kept,
			 deleted;
	unsigned int attempt;

	for (attempt = 0; attempt < max_attempts; attempt++)
	{
		seq_before = ps_load_acquire_u64(&hdr->page_prune_metrics_seq);
		if (seq_before & 1)
		{
			usleep(100);
			continue;
		}
		compactions = ps_load_acquire_u64(&hdr->page_prune_compactions);
		scanned = ps_load_acquire_u64(&hdr->page_prune_versions_scanned);
		kept = ps_load_acquire_u64(&hdr->page_prune_versions_kept);
		deleted = ps_load_acquire_u64(&hdr->page_prune_versions_deleted);
		seq_after = ps_load_acquire_u64(&hdr->page_prune_metrics_seq);
		if (seq_before == seq_after && !(seq_after & 1))
			break;
		usleep(100);
	}
	if (attempt == max_attempts)
	{
		fprintf(stderr,
				"pagestore_inspect: pruning metrics snapshot stayed unstable\n");
		exit(1);
	}
	printf("{\"compactions\":%llu,\"versions_scanned\":%llu,"
		   "\"versions_kept\":%llu,\"versions_deleted\":%llu}\n",
		   (unsigned long long) compactions,
		   (unsigned long long) scanned,
		   (unsigned long long) kept,
		   (unsigned long long) deleted);
}

static void
read_inspection_metrics(PsShmHeader *hdr, PsInspectionMetrics *metrics)
{
	const unsigned int max_attempts = 10000;
	uint64_t before;
	uint64_t after;

	for (unsigned int attempt = 0; attempt < max_attempts; attempt++)
	{
		before = ps_load_acquire_u64(&hdr->inspection_metrics_seq);
		if (before & 1)
		{
			usleep(100);
			continue;
		}
		metrics->timeline_count =
			ps_load_acquire_u64(&hdr->inspection.timeline_count);
		metrics->live_timelines =
			ps_load_acquire_u64(&hdr->inspection.live_timelines);
		metrics->deleting_timelines =
			ps_load_acquire_u64(&hdr->inspection.deleting_timelines);
		metrics->deleted_timelines =
			ps_load_acquire_u64(&hdr->inspection.deleted_timelines);
		metrics->metadata_poisoned =
			ps_load_acquire(&hdr->inspection.metadata_poisoned);
		metrics->layer_count =
			ps_load_acquire_u64(&hdr->inspection.layer_count);
		metrics->deleting_layers =
			ps_load_acquire_u64(&hdr->inspection.deleting_layers);
		metrics->local_layers =
			ps_load_acquire_u64(&hdr->inspection.local_layers);
		metrics->remote_durable_layers =
			ps_load_acquire_u64(&hdr->inspection.remote_durable_layers);
		metrics->manifest_poisoned =
			ps_load_acquire(&hdr->inspection.manifest_poisoned);
		metrics->page_debt_segments =
			ps_load_acquire_u64(&hdr->inspection.page_debt_segments);
		metrics->page_debt_unavailable =
			ps_load_acquire(&hdr->inspection.page_debt_unavailable);
		metrics->gc_deleting_layers =
			ps_load_acquire_u64(&hdr->inspection.gc_deleting_layers);
		metrics->remote_cleanup_pending =
			ps_load_acquire_u64(&hdr->inspection.remote_cleanup_pending);
		metrics->forkmeta_pending =
			ps_load_acquire(&hdr->inspection.forkmeta_pending);
		metrics->owner_count =
			ps_load_acquire_u64(&hdr->inspection.owner_count);
		metrics->page_history_owners =
			ps_load_acquire_u64(&hdr->inspection.page_history_owners);
		metrics->wal_owners =
			ps_load_acquire_u64(&hdr->inspection.wal_owners);
		metrics->wal_index_owners =
			ps_load_acquire_u64(&hdr->inspection.wal_index_owners);
		metrics->max_generation =
			ps_load_acquire_u64(&hdr->inspection.max_generation);
		metrics->retention_poisoned =
			ps_load_acquire(&hdr->inspection.retention_poisoned);
		metrics->forkmeta_poisoned =
			ps_load_acquire(&hdr->inspection.forkmeta_poisoned);
		for (uint32_t tl = 0; tl < PS_INSPECTION_MAX_TIMELINES; tl++)
		{
			PsInspectionTimeline *entry = &metrics->timeline_entries[tl];

			entry->parent_timeline = __atomic_load_n(
				&hdr->inspection.timeline_entries[tl].parent_timeline,
				__ATOMIC_ACQUIRE);
			entry->fork_lsn = ps_load_acquire_u64(
				&hdr->inspection.timeline_entries[tl].fork_lsn);
			entry->retained_horizon = ps_load_acquire_u64(
				&hdr->inspection.timeline_entries[tl].retained_horizon);
			entry->defined = ps_load_acquire(
				&hdr->inspection.timeline_entries[tl].defined);
		}
		after = ps_load_acquire_u64(&hdr->inspection_metrics_seq);
		if (before == after && !(after & 1))
			return;
		usleep(100);
	}
	fprintf(stderr,
			"pagestore_inspect: inspection metrics snapshot stayed unstable\n");
	exit(1);
}

static int
print_timeline(PsShmHeader *hdr, uint32_t timeline)
{
	PsInspectionMetrics metrics;
	PsInspectionTimeline *entry;

	read_inspection_metrics(hdr, &metrics);
	if (metrics.metadata_poisoned)
	{
		fprintf(stderr, "pagestore_inspect: timeline metadata is poisoned\n");
		return 1;
	}
	if (timeline >= PS_INSPECTION_MAX_TIMELINES ||
		!metrics.timeline_entries[timeline].defined)
	{
		fprintf(stderr, "pagestore_inspect: timeline %u does not exist\n",
				timeline);
		return 1;
	}
	if (metrics.retention_poisoned)
	{
		fprintf(stderr, "pagestore_inspect: retention metadata is poisoned\n");
		return 1;
	}
	entry = &metrics.timeline_entries[timeline];
	printf("{\"parent_timeline\":%lld,\"fork_lsn\":%llu,"
		   "\"retained_horizon\":%llu}\n",
		   (long long) entry->parent_timeline,
		   (unsigned long long) entry->fork_lsn,
		   (unsigned long long) entry->retained_horizon);
	return 0;
}

static void
print_manifest(PsShmHeader *hdr)
{
	PsInspectionMetrics metrics;

	read_inspection_metrics(hdr, &metrics);
	printf("{\"layer_count\":%llu,\"deleting_layers\":%llu,"
		   "\"local_layers\":%llu,\"remote_durable_layers\":%llu,"
		   "\"manifest_poisoned\":%s}\n",
		   (unsigned long long) metrics.layer_count,
		   (unsigned long long) metrics.deleting_layers,
		   (unsigned long long) metrics.local_layers,
		   (unsigned long long) metrics.remote_durable_layers,
		   metrics.manifest_poisoned ? "true" : "false");
}

static void
print_gc(PsShmHeader *hdr)
{
	PsInspectionMetrics metrics;

	read_inspection_metrics(hdr, &metrics);
	printf("{\"page_debt_segments\":%llu,\"page_debt_unavailable\":%s,"
		   "\"deleting_layers\":%llu,"
		   "\"remote_cleanup_pending\":%llu,\"forkmeta_pending\":%s,"
		   "\"forkmeta_poisoned\":%s}\n",
		   (unsigned long long) metrics.page_debt_segments,
		   metrics.page_debt_unavailable ? "true" : "false",
		   (unsigned long long) metrics.gc_deleting_layers,
		   (unsigned long long) metrics.remote_cleanup_pending,
		   metrics.forkmeta_pending ? "true" : "false",
		   metrics.forkmeta_poisoned ? "true" : "false");
}

static void
print_owners(PsShmHeader *hdr)
{
	PsInspectionMetrics metrics;

	read_inspection_metrics(hdr, &metrics);
	printf("{\"owner_count\":%llu,\"page_history_owners\":%llu,"
		   "\"wal_owners\":%llu,\"wal_index_owners\":%llu,"
		   "\"max_generation\":%llu,\"retention_poisoned\":%s}\n",
		   (unsigned long long) metrics.owner_count,
		   (unsigned long long) metrics.page_history_owners,
		   (unsigned long long) metrics.wal_owners,
		   (unsigned long long) metrics.wal_index_owners,
			(unsigned long long) metrics.max_generation,
			metrics.retention_poisoned ? "true" : "false");
}

static int
print_relation(int fd, PsShmHeader *hdr, uint32_t timeline,
				   uint64_t expected_incarnation, const PsKey *key,
				   uint64_t lsn)
{
	PsInspectionRequest *request = &hdr->inspection_request;
	PsInspectionRelationResult result;
	uint64_t instance;
	uint64_t generation;
	uint64_t now;
	uint64_t deadline;

	if ((hdr->frontend_capabilities & PS_FRONTEND_CAP_RELATION_INSPECTION) == 0)
	{
		fprintf(stderr,
				"pagestore_inspect: relation inspection is unavailable for this frontend\n");
		return 1;
	}
	instance = ps_load_acquire_u64(&hdr->daemon_instance);
	if (instance == 0 || recover_relation_mailbox(fd, request) != 0)
		return 1;
	/* Recovery can legitimately clear a stale slot after the old daemon has
	 * exited.  Before writing any new request field, prove that byte one is
	 * still owned by a daemon.  Byte zero remains held, so a new daemon cannot
	 * start between this probe and publication. */
	if (daemon_lease_is_free(fd) != 0)
	{
		fprintf(stderr, "pagestore_inspect: relation inspection daemon unavailable\n");
		return 1;
	}
	generation = request->request_generation + 1;
	if (generation == 0)
		generation = 1;
	now = inspect_monotonic_ns();
	if (now == 0 || UINT64_MAX - now < INSPECTION_TIMEOUT_NS)
	{
		fprintf(stderr, "pagestore_inspect: monotonic clock unavailable\n");
		return 1;
	}
	deadline = now + INSPECTION_TIMEOUT_NS;
	memset(&result, 0, sizeof(result));
	request->protocol_version = PS_SHM_VERSION;
	request->opcode = PS_INSPECT_RELATION;
	request->status = PS_STATUS_ERROR;
	request->request_generation = generation;
	request->daemon_instance = instance;
	request->deadline_ns = deadline;
	request->timeline = timeline;
	request->reserved = 0;
	request->expected_incarnation = expected_incarnation;
	request->lsn = lsn;
	request->key = *key;
	request->relation = result;
	ps_store_release(&request->state, PS_INSPECTION_STATE_REQUEST);

	for (;;)
	{
		uint32_t state = ps_load_acquire(&request->state);

		if (state == PS_INSPECTION_STATE_DONE)
			break;
		if (hdr->magic != PS_SHM_MAGIC || hdr->version != PS_SHM_VERSION ||
			__atomic_load_n(&hdr->startup_state, __ATOMIC_ACQUIRE) != PS_SHM_READY ||
			ps_load_acquire_u64(&hdr->daemon_instance) != instance)
		{
			fprintf(stderr, "pagestore_inspect: daemon restarted or stopped\n");
			return 1;
		}
		if ((now = inspect_monotonic_ns()) == 0 || now >= deadline)
		{
			fprintf(stderr, "pagestore_inspect: relation inspection timed out\n");
			return 1;
		}
		usleep(1000);
	}
	if (request->request_generation != generation ||
		request->daemon_instance != instance || request->status != PS_STATUS_OK)
	{
		fprintf(stderr, "pagestore_inspect: relation inspection unavailable\n");
		(void) ps_cas(&request->state, PS_INSPECTION_STATE_DONE,
					  PS_INSPECTION_STATE_IDLE);
		return 1;
	}
	result = request->relation;
	if (!valid_relation_result(&result))
	{
		fprintf(stderr, "pagestore_inspect: invalid relation response\n");
		(void) ps_cas(&request->state, PS_INSPECTION_STATE_DONE,
					  PS_INSPECTION_STATE_IDLE);
		return 1;
	}
	printf("{\"exists\":%s,\"forks\":[",
		   result.exists ? "true" : "false");
	for (uint32_t i = 0; i < result.fork_count; i++)
	{
		if (i != 0)
			putchar(',');
		printf("{\"fork\":%d,\"nblocks\":%u}",
			   result.forks[i].fork_num, result.forks[i].nblocks);
	}
	printf("],\"selected_version\":null}\n");
	if (!ps_cas(&request->state, PS_INSPECTION_STATE_DONE,
				PS_INSPECTION_STATE_IDLE))
	{
		fprintf(stderr, "pagestore_inspect: relation response ownership lost\n");
		return 1;
	}
	return 0;
}

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	const char *operation = NULL;
	int		fd;
	void	   *shm;
	PsShmHeader *hdr;
	int		relation_operation = 0;

	uint32_t timeline_id = 0;
	uint64_t expected_incarnation = 0;
	int timeline_status = 0;
	PsKey relation_key;
	uint64_t relation_lsn = 0;

	memset(&relation_key, 0, sizeof(relation_key));

	if ((argc >= 4) && strcmp(argv[1], "--shm") == 0)
	{
		shm_name = argv[2];
		operation = argv[3];
		if (strcmp(operation, "relation") == 0)
		{
			uint32_t spc_oid;
			uint32_t db_oid;
			uint32_t rel_number;

			if (argc != 10 ||
				parse_decimal_u32(argv[4], &timeline_id) != 0 ||
				parse_decimal_u64(argv[5], &expected_incarnation) != 0 ||
				expected_incarnation == 0 ||
				parse_decimal_u32(argv[6], &spc_oid) != 0 ||
				parse_decimal_u32(argv[7], &db_oid) != 0 ||
				parse_decimal_u32(argv[8], &rel_number) != 0 ||
				parse_decimal_u64(argv[9], &relation_lsn) != 0)
				operation = NULL;
			else
			{
				relation_operation = 1;
				relation_key.spcOid = spc_oid;
				relation_key.dbOid = db_oid;
				relation_key.relNumber = rel_number;
				relation_key.forkNum = 0;
				relation_key.klass = PS_KLASS_RELATION;
			}
		}
		else if (strcmp(operation, "timeline") == 0)
		{
			char *end = NULL;
			unsigned long long parsed;

			if (argc != 5 || argv[4][0] == '-')
				operation = NULL;
			else
			{
				errno = 0;
				parsed = strtoull(argv[4], &end, 10);
				if (errno == ERANGE || end == argv[4] || *end != '\0' ||
					parsed >= PS_INSPECTION_MAX_TIMELINES)
					operation = NULL;
				else
					timeline_id = (uint32_t) parsed;
			}
		}
		else if (argc != 4)
			operation = NULL;
	}
	if (shm_name == NULL || operation == NULL ||
		(strcmp(operation, "health") != 0 &&
		 strcmp(operation, "timeline") != 0 &&
		 strcmp(operation, "manifest") != 0 &&
		 strcmp(operation, "gc") != 0 &&
		 strcmp(operation, "owners") != 0 &&
		 strcmp(operation, "backpressure") != 0 &&
		 strcmp(operation, "pruning") != 0 &&
		 strcmp(operation, "relation") != 0))
	{
		usage(argv[0]);
		return 2;
	}
	fd = shm_open(shm_name, relation_operation ? O_RDWR : O_RDONLY, 0);
	if (fd < 0)
	{
		perror("pagestore_inspect: shm_open");
		return 1;
	}
	if (relation_operation)
	{
		int lock_status = lock_relation_shm(fd);

		if (lock_status != 0)
		{
			close(fd);
			return 1;
		}
	}
	shm = mmap(NULL, PS_SHM_SIZE,
			  relation_operation ? (PROT_READ | PROT_WRITE) : PROT_READ,
			  MAP_SHARED, fd, 0);
	if (shm == MAP_FAILED)
	{
		perror("pagestore_inspect: mmap");
		if (relation_operation)
			unlock_relation_shm(fd);
		close(fd);
		return 1;
	}
	hdr = (PsShmHeader *) shm;
	if (!header_valid(hdr))
	{
		fprintf(stderr, "pagestore_inspect: invalid or incompatible shared memory\n");
		munmap(shm, PS_SHM_SIZE);
		if (relation_operation)
			unlock_relation_shm(fd);
		close(fd);
		return 1;
	}
	if (strcmp(operation, "health") == 0)
		print_health(hdr);
	else if (strcmp(operation, "timeline") == 0)
		timeline_status = print_timeline(hdr, timeline_id);
	else if (strcmp(operation, "relation") == 0)
		timeline_status = print_relation(fd, hdr, timeline_id,
							 expected_incarnation, &relation_key, relation_lsn);
	else if (strcmp(operation, "manifest") == 0)
		print_manifest(hdr);
	else if (strcmp(operation, "gc") == 0)
		print_gc(hdr);
	else if (strcmp(operation, "owners") == 0)
		print_owners(hdr);
	else if (strcmp(operation, "backpressure") == 0)
		print_backpressure(shm, hdr);
	else
		print_pruning(hdr);
	munmap(shm, PS_SHM_SIZE);
	if (relation_operation)
		unlock_relation_shm(fd);
	close(fd);
	return timeline_status;
}
