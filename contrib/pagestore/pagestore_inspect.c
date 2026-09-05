/*-------------------------------------------------------------------------
 *
 * pagestore_inspect.c
 *    Read-only inspection client for a running pagestore daemon.
 *
 * This intentionally maps the private shared-memory IPC read-only and never
 * claims a channel.  It is for the harness and diagnostics, not a production
 * management API.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "pagestore_ipc.h"

static void
usage(const char *prog)
{
	fprintf(stderr, "usage: %s --shm NAME health|backpressure|pruning\n", prog);
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

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	const char *operation = NULL;
	int		fd;
	void	   *shm;
	PsShmHeader *hdr;

	if (argc == 4 && strcmp(argv[1], "--shm") == 0)
	{
		shm_name = argv[2];
		operation = argv[3];
	}
	if (shm_name == NULL ||
		(strcmp(operation, "health") != 0 &&
		 strcmp(operation, "backpressure") != 0 &&
		 strcmp(operation, "pruning") != 0))
	{
		usage(argv[0]);
		return 2;
	}
	fd = shm_open(shm_name, O_RDONLY, 0);
	if (fd < 0)
	{
		perror("pagestore_inspect: shm_open");
		return 1;
	}
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ, MAP_SHARED, fd, 0);
	close(fd);
	if (shm == MAP_FAILED)
	{
		perror("pagestore_inspect: mmap");
		return 1;
	}
	hdr = (PsShmHeader *) shm;
	if (!header_valid(hdr))
	{
		fprintf(stderr, "pagestore_inspect: invalid or incompatible shared memory\n");
		munmap(shm, PS_SHM_SIZE);
		return 1;
	}
	if (strcmp(operation, "health") == 0)
		print_health(hdr);
	else if (strcmp(operation, "backpressure") == 0)
		print_backpressure(shm, hdr);
	else
		print_pruning(hdr);
	munmap(shm, PS_SHM_SIZE);
	return 0;
}
