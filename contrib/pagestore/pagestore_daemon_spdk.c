/*-------------------------------------------------------------------------
 *
 * pagestore_daemon_spdk.c
 *	  SPDK frontend for the page-store daemon (optional, higher performance).
 *
 * Reuses the shared brain pagestore_core.c verbatim; this file supplies the
 * SPDK-specific bring-up and an *asynchronous, cross-channel* request loop.
 * SPDK is used in library mode (we own the loop).  The loop scans the channels
 * and begins each ready request without blocking: metadata and (buffered) write
 * ops complete synchronously, while read ops submit their page reads to the NVMe
 * queue and the channel's reply is published from the read completions.  So many
 * requests are in flight at once -- effective queue depth is no longer one
 * request's worth.  The portable POSIX daemon is unaffected and remains the
 * default; this binary is built separately (spdk_build.sh) and links SPDK.
 *
 * Argument-compatible with pagestore_daemon so the standalone test harness can
 * drive it: --shm/--store/--page-size/--segment-size; the control disk's PCI
 * address is --pci or $PS_SPDK_PCI.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"
#include "pagestore_pgcache.h"
#include "storage_spdk.h"

/* most page reads a single request can carry (nblocks * page_size <= io_unit) */
#define MAX_BLOCKS	128

static volatile sig_atomic_t stop_requested = 0;
static pthread_rwlock_t core_rwlock = PTHREAD_RWLOCK_INITIALIZER;

typedef struct WorkerArgs
{
	uint32_t	shard;
	void	   *shm;
	uint32_t	nchannels;
	uint32_t	nshards;
} WorkerArgs;

static void
on_signal(int sig)
{
	(void) sig;
	stop_requested = 1;
}

/* per-block context for one async page read: lets the completion populate the
 * materialized-page cache and find its parent request */
typedef struct BlkCtx
{
	struct ReqState *rs;
	uint32_t	tl;
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	unsigned char *dst;
} BlkCtx;

/* per-channel in-flight read request */
typedef struct ReqState
{
	PsChannel  *ch;
	int			active;			/* a read request is in flight on this channel */
	int			pending;		/* page reads not yet completed */
	BlkCtx		blk[MAX_BLOCKS];	/* one per submitted (cache-missed) read */
} ReqState;

static ReqState reqstate[PS_MAX_CHANNELS];

/* one page read finished: cache the page, and when the last of a request lands
 * publish the reply */
static void
read_done(void *arg, int ok)
{
	BlkCtx	   *bc = arg;
	ReqState   *rs = bc->rs;

	if (ok)						/* the engine delivered the page into bc->dst */
	{
		ps_pgcache_insert(bc->tl, &bc->key, bc->block, bc->lsn, bc->dst);
		/* a READ_AT only counts as found once its page has actually landed */
		if (rs->ch->opcode == PS_OP_READ_AT)
			rs->ch->result = 1;
	}
	if (--rs->pending == 0)
	{
		rs->active = 0;			/* clear before publishing DONE */
		ps_store_release(&rs->ch->state, PS_STATE_DONE);
	}
}

static int
request_is_write(PsOpcode opcode)
{
	switch (opcode)
	{
		case PS_OP_CREATE:
		case PS_OP_UNLINK:
		case PS_OP_TRUNCATE:
		case PS_OP_ZEROEXTEND:
		case PS_OP_CREATE_BRANCH:
		case PS_OP_EXTEND:
		case PS_OP_WRITEV:
		case PS_OP_WAL_APPEND:
		case PS_OP_WAL_INDEX_ADD:
		case PS_OP_IMMEDSYNC:
			return 1;
		case PS_OP_EXISTS:
		case PS_OP_NBLOCKS:
		case PS_OP_READV:
		case PS_OP_READ_AT:
		case PS_OP_WAL_SIZE:
		case PS_OP_WAL_READ:
		case PS_OP_WAL_INDEX_GET:
			return 0;
		default:
			return 1;
	}
}

/*
 * Begin serving the request on channel 'i'.  Synchronous ops (metadata, buffered
 * writes) finish and publish DONE here; read ops submit their page reads and
 * return, leaving DONE to read_done().  Index pointers from read_through() are
 * dereferenced (seg/off taken) synchronously here, never held across the async
 * wait, so a concurrent write reallocating a version array cannot dangle them.
 */
static void
begin(uint32_t i, PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	ch->status = PS_STATUS_OK;
	ch->result = 0;

	if (ps_handle_meta(ch))
	{
		ps_store_release(&ch->state, PS_STATE_DONE);
		return;
	}

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_EXTEND:
			if (append_page(tl, &ch->key, ch->blocknum, ch->data, ch->req_lsn) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(tl, &ch->key, ch->blocknum + 1);
			ps_store_release(&ch->state, PS_STATE_DONE);
			return;

		case PS_OP_WRITEV:
			for (uint32_t b = 0; b < ch->nblocks; b++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + b,
								 ch->data + (size_t) b * page_size,
								 ch->req_lsn) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
			if (ch->status == PS_STATUS_OK)
				fork_grow(tl, &ch->key, ch->blocknum + ch->nblocks);
			ps_store_release(&ch->state, PS_STATE_DONE);
			return;

		case PS_OP_READV:
			{
				uint32_t	nb = ch->nblocks;
				ReqState   *rs = &reqstate[i];

				if (nb > MAX_BLOCKS || (uint64_t) nb * page_size > PS_IO_UNIT)
				{
					ch->status = PS_STATUS_ERROR;
					ps_store_release(&ch->state, PS_STATE_DONE);
					return;
				}
				rs->ch = ch;
				rs->pending = 0;
				rs->active = 1;
				/* completions only fire from ps_spdk_poll() (the main loop), not
				 * during submit, so incrementing pending as we go is safe */
				for (uint32_t b = 0; b < nb; b++)
				{
					unsigned char *dst = ch->data + (size_t) b * page_size;
					uint32_t	blk = ch->blocknum + b;
					PageVer    *v = read_through(tl, &ch->key, blk, UINT64_MAX);
					BlkCtx	   *bc;

					if (!v)
					{
						memset(dst, 0, page_size);	/* unwritten -> zeros */
						continue;
					}
					if (ps_pgcache_lookup(tl, &ch->key, blk, v->lsn, dst))
						continue;	/* RAM hit -> no device read */
					bc = &rs->blk[rs->pending++];
					bc->rs = rs;
					bc->tl = tl;
					bc->key = ch->key;
					bc->block = blk;
					bc->lsn = v->lsn;
					bc->dst = dst;
					ps_spdk_read_async(v->shard, v->seg, v->off, dst, page_size,
									   read_done, bc);
				}
				if (rs->pending == 0)	/* all cached or unwritten */
				{
					rs->active = 0;
					ps_store_release(&ch->state, PS_STATE_DONE);
				}
				return;			/* DONE published by read_done otherwise */
			}

		case PS_OP_READ_AT:
			{
				PageVer    *v = read_through(tl, &ch->key, ch->blocknum,
											 ch->req_lsn);
				ReqState   *rs = &reqstate[i];
				BlkCtx	   *bc;

				if (!v)
				{
					memset(ch->data, 0, page_size);		/* not found: result 0 */
					ps_store_release(&ch->state, PS_STATE_DONE);
					return;
				}
				/* report the resolved version for an exact-cutoff SLRU read; defer
				 * found-ness (ch->result) until the page actually lands, so a failed
				 * async read does not advertise a zero-filled page as found */
				ch->req_lsn = v->lsn;
				if (ps_pgcache_lookup(tl, &ch->key, ch->blocknum, v->lsn,
									  ch->data))
				{
					ch->result = 1;			/* served from RAM: page is present */
					ps_store_release(&ch->state, PS_STATE_DONE);	/* RAM hit */
					return;
				}
				rs->ch = ch;
				rs->pending = 1;
				rs->active = 1;
				bc = &rs->blk[0];
				bc->rs = rs;
				bc->tl = tl;
				bc->key = ch->key;
				bc->block = ch->blocknum;
				bc->lsn = v->lsn;
				bc->dst = ch->data;
				ps_spdk_read_async(v->shard, v->seg, v->off, ch->data, page_size,
								   read_done, bc);
				return;
			}

		default:
			ch->status = PS_STATUS_ERROR;
			ps_store_release(&ch->state, PS_STATE_DONE);
			return;
	}
}

static void
run_request(uint32_t i, PsChannel *ch)
{
	int			is_write = request_is_write((PsOpcode) ch->opcode);

	if (is_write)
		pthread_rwlock_wrlock(&core_rwlock);
	else
		pthread_rwlock_rdlock(&core_rwlock);
	begin(i, ch);
	pthread_rwlock_unlock(&core_rwlock);
}

static void *
shard_worker(void *arg)
{
	WorkerArgs *wa = (WorkerArgs *) arg;
	uint32_t	shard = wa->shard;
	void	   *shm = wa->shm;
	uint32_t	nchannels = wa->nchannels;
	uint32_t	nshards = wa->nshards;

	if (ps_spdk_thread_init(shard) != 0)
	{
		fprintf(stderr, "pagestore_daemon_spdk: failed to initialize shard-%u\n", shard);
		return NULL;
	}

	while (!stop_requested)
	{
		int			did_work = 0;

		for (uint32_t i = shard; i < nchannels; i += nshards)
		{
			PsChannel  *ch = ps_channel(shm, i);

			if (reqstate[i].active || ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;

			run_request(i, ch);
			did_work = 1;
		}

		if (ps_spdk_poll(shard) > 0)
			did_work = 1;

		if (!did_work && shard == 0)
		{
			int			do_maint = 0;

			pthread_rwlock_wrlock(&core_rwlock);
			do_maint = ps_core_maintenance();
			pthread_rwlock_unlock(&core_rwlock);
			if (!do_maint)
			{
				struct timespec ts = {0, 20000};	/* 20us */

				nanosleep(&ts, NULL);
			}
		}
		else if (!did_work)
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}
	ps_spdk_thread_close(shard);
	return NULL;
}

int
main(int argc, char **argv)
{
	int			fd;
	void	   *shm;
	PsShmHeader *hdr;
	struct sigaction sa;
	const char *store_dir = NULL;
	const char *shm_name = NULL;
	const char *pci_addr = NULL;
	uint32_t	nshards = 1;

	ps_storage = &PsStorageSpdk;
	use_layers = 0;				/* SPDK reads serve by segment offset for now */

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--store") == 0 && i + 1 < argc)
			store_dir = argv[++i];
		else if (strcmp(argv[i], "--pci") == 0 && i + 1 < argc)
			pci_addr = argv[++i];
		else if (strcmp(argv[i], "--page-size") == 0 && i + 1 < argc)
			page_size = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--segment-size") == 0 && i + 1 < argc)
			segment_size = strtoull(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--flush-pages") == 0 && i + 1 < argc)
			flush_pages = atoi(argv[++i]);
		else if (strcmp(argv[i], "--compact-layers") == 0 && i + 1 < argc)
			compact_layers = atoi(argv[++i]);
		else if (strcmp(argv[i], "--nshards") == 0 && i + 1 < argc)
			nshards = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--cache-pages") == 0 && i + 1 < argc)
			cache_pages = atoi(argv[++i]);
		else
		{
			fprintf(stderr, "usage: %s --shm NAME --store DIR --pci ADDR "
					"[--page-size N] [--segment-size N] [--nshards N]\n", argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT ||
		nshards == 0 || nshards > PS_MAX_CHANNELS)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR --pci ADDR "
				"[--page-size N] [--segment-size N] [--nshards N]\n", argv[0]);
		return 2;
	}
	ps_nshards = nshards;
	if (pci_addr)
		setenv("PS_SPDK_PCI", pci_addr, 1);

	if (ps_core_open(store_dir) != 0)
	{
		fprintf(stderr, "pagestore_daemon_spdk: bring-up failed\n");
		return 1;
	}

	fd = shm_open(shm_name, O_CREAT | O_RDWR, 0600);
	if (fd < 0)
	{
		perror("shm_open");
		return 1;
	}
	if (ftruncate(fd, PS_SHM_SIZE) != 0)
	{
		perror("ftruncate shm");
		return 1;
	}
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (shm == MAP_FAILED)
	{
		perror("mmap");
		return 1;
	}
	close(fd);

	hdr = (PsShmHeader *) shm;
	memset(shm, 0, PS_SHM_SIZE);
	hdr->magic = PS_SHM_MAGIC;
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = page_size;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->nshards = nshards;
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	fprintf(stderr, "pagestore_daemon_spdk: shm=%s store=%s storage=%s "
			"page_size=%u io_unit=%u channels=%u nshards=%u ready\n",
			shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
			PS_MAX_CHANNELS, hdr->nshards);

	{
		WorkerArgs *workers = malloc((size_t) hdr->nshards * sizeof(WorkerArgs));
		pthread_t  *threads = malloc((size_t) hdr->nshards * sizeof(pthread_t));
		uint32_t	started = 0;

		if (!workers || !threads)
		{
			fprintf(stderr, "pagestore_daemon_spdk: cannot allocate worker slots\n");
			free(workers);
			free(threads);
			munmap(shm, PS_SHM_SIZE);
			return 1;
		}

		for (uint32_t shard = 0; shard < hdr->nshards; shard++)
		{
			workers[shard].shard = shard;
			workers[shard].shm = shm;
			workers[shard].nchannels = hdr->nchannels;
			workers[shard].nshards = hdr->nshards;
			if (pthread_create(&threads[shard], NULL, shard_worker, &workers[shard]) != 0)
			{
				fprintf(stderr, "pagestore_daemon_spdk: failed to start worker %u\n", shard);
				stop_requested = 1;
				break;
			}
			started++;
		}

		for (uint32_t shard = 0; shard < started; shard++)
			pthread_join(threads[shard], NULL);

		free(workers);
		free(threads);
	}

	{
		uint64_t	ch,
					cm,
					ce;

		ps_pgcache_stats(&ch, &cm, &ce);
		fprintf(stderr, "pagestore_daemon_spdk: shutting down (pgcache hit=%llu "
				"miss=%llu evict=%llu)\n", (unsigned long long) ch,
				(unsigned long long) cm, (unsigned long long) ce);
	}
	ps_core_close();			/* flush the memtable into a layer before detaching */
	ps_storage->close();
	munmap(shm, PS_SHM_SIZE);
	return 0;
}
