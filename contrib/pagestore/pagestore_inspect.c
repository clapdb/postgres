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
print_backpressure(void *shm, PsShmHeader *hdr)
{
	uint32_t idle = 0;
	uint32_t claimed = 0;
	uint32_t request = 0;
	uint32_t done = 0;

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
	printf("{\"idle\":%u,\"claimed\":%u,\"request\":%u,"
		   "\"done\":%u,\"shards\":%u,\"wal_index_pending_bytes\":%llu,"
		   "\"wal_index_lagging_timelines\":%u}\n",
		   idle, claimed, request, done, hdr->nshards,
		   (unsigned long long) ps_load_acquire_u64(&hdr->wal_index_pending_bytes),
		   ps_load_acquire(&hdr->wal_index_lagging_timelines));
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
