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
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static void
on_signal(int sig)
{
	(void) sig;
	__atomic_store_n(&stop_requested, 1, __ATOMIC_RELAXED);	/* seen by all workers */
}

/* per-block context for one async page read: lets the completion populate the
 * materialized-page cache and find its parent request */
typedef struct BlkCtx
{
	struct ReqState *rs;
	uint32_t	tl;				/* resolved source timeline (the cache key) */
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	uint32_t	crc;			/* expected page CRC32C (from the version index) */
	int			cacheable;		/* 0 if the version's lsn is ambiguous */
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

/*
 * Cross-shard IMMEDSYNC coordination.  A shard's curbuf/qpair may only be touched
 * by its own worker, so a global durability barrier can't flush every shard from
 * one thread.  Instead the worker that receives IMMEDSYNC flushes its own shard
 * and sets g_flush_req[s] for every other shard; each worker flushes its own
 * curbuf when it sees its flag set.  The whole barrier runs under g_barrier_mtx
 * (below), so only one coordinator owns these per-shard slots at a time.
 */
static volatile int g_flush_req[PS_MAX_SHARDS];		/* 1 = flush requested */
static volatile int g_flush_rc[PS_MAX_SHARDS];		/* 0 ok / -1 failed flush */
static volatile uint32_t g_flush_snap[PS_MAX_SHARDS];	/* seg count at flush */

/*
 * Serializes the whole IMMEDSYNC barrier so two concurrent syncs can't share the
 * per-shard flush slots above (one resetting another's result/snapshot mid-flight
 * and turning a failed flush into a false success).  The worker TRYLOCKs this
 * rather than blocking: a worker that can't start its own barrier returns to its
 * loop and keeps servicing the active barrier's flush requests, which is what
 * prevents a deadlock between a coordinator and a would-be coordinator.
 */
static pthread_mutex_t g_barrier_mtx = PTHREAD_MUTEX_INITIALIZER;

/* Flush this shard's own buffer, recording the result + post-flush count where
 * the barrier coordinator collects them. */
static void
flush_self(uint32_t shard)
{
	uint32_t	cnt = 0;
	int			rc = ps_spdk_flush(shard, &cnt);

	g_flush_snap[shard] = cnt;
	g_flush_rc[shard] = rc;
}

/*
 * Durability barrier for one IMMEDSYNC, run by the receiving worker for 'shard'
 * while holding g_barrier_mtx (so it owns the flush slots exclusively).  Each
 * shard can only flush its own qpair, so request every other shard to flush
 * itself and wait.  The superblock is persisted from each shard's post-flush
 * count snapshot, and ONLY when every shard flushed: a failed flush leaves a
 * count covering data that never reached the device, which recover() must not
 * trust.  Returns 0 if all shards flushed durably, -1 if any failed or shutdown
 * aborted the wait.
 */
static int
immedsync_barrier(uint32_t shard)
{
	uint32_t	counts[PS_MAX_SHARDS];
	int			rc = 0;

	flush_self(shard);			/* this worker's own shard */
	for (uint32_t s = 0; s < ps_nshards; s++)
		if (s != shard)
		{
			g_flush_rc[s] = 0;
			__atomic_store_n(&g_flush_req[s], 1, __ATOMIC_RELEASE);
		}
	for (;;)
	{
		int			pending = 0;

		if (__atomic_load_n(&stop_requested, __ATOMIC_RELAXED))
			return -1;			/* shutdown: abort rather than spin forever */
		for (uint32_t s = 0; s < ps_nshards; s++)
			if (s != shard && __atomic_load_n(&g_flush_req[s], __ATOMIC_ACQUIRE))
				pending = 1;
		if (!pending)
			break;
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}
	/* collect each shard's flush result + post-flush count snapshot */
	for (uint32_t s = 0; s < ps_nshards; s++)
	{
		if (g_flush_rc[s] != 0)
			rc = -1;
		counts[s] = g_flush_snap[s];
	}
	if (rc == 0)				/* never advance the super past a failed flush */
		ps_spdk_super_write_counts(counts);
	return rc;
}

/* one page read finished: cache the page, and when the last of a request lands
 * publish the reply */
static void
read_done(void *arg, int ok)
{
	BlkCtx	   *bc = arg;
	ReqState   *rs = bc->rs;

	/* verify the delivered page against the version's CRC: device bit rot or a
	 * misread must not be served as valid or cached.  Zero-fill and fail the
	 * request rather than hand the client corrupt bytes. */
	if (ok && ps_crc32c(bc->dst, page_size) != bc->crc)
	{
		memset(bc->dst, 0, page_size);
		rs->ch->status = PS_STATUS_ERROR;
		ok = 0;
	}
	if (ok && bc->cacheable)	/* the engine delivered the page into bc->dst */
		ps_pgcache_insert(ps_core_pgcache_for(&bc->key), bc->tl, &bc->key,
						  bc->block, bc->lsn, bc->dst);
	if (--rs->pending == 0)
	{
		rs->active = 0;			/* clear before publishing DONE */
		ps_store_release(&rs->ch->state, PS_STATE_DONE);
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
			if (append_page(tl, &ch->key, ch->blocknum, ch->data) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(tl, &ch->key, ch->blocknum + 1);
			ps_store_release(&ch->state, PS_STATE_DONE);
			return;

		case PS_OP_WRITEV:
			for (uint32_t b = 0; b < ch->nblocks; b++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + b,
								 ch->data + (size_t) b * page_size) != 0)
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
				uint32_t	nsub = 0;

				if (nb > MAX_BLOCKS || (uint64_t) nb * page_size > PS_IO_UNIT)
				{
					ch->status = PS_STATUS_ERROR;
					ps_store_release(&ch->state, PS_STATE_DONE);
					return;
				}
				rs->ch = ch;
				/* start pending at 1 as a submit guard: ps_spdk_read_async() can
				 * invoke read_done inline (the active append segment, error
				 * paths), so without this the request could publish DONE / clear
				 * active mid-loop while later blocks are still being submitted */
				rs->pending = 1;
				rs->active = 1;
				for (uint32_t b = 0; b < nb; b++)
				{
					unsigned char *dst = ch->data + (size_t) b * page_size;
					uint32_t	blk = ch->blocknum + b;
					uint32_t	stl;
					int			ambig;
					PageVer    *v = read_through_cacheable(tl, &ch->key, blk,
														  UINT64_MAX, &stl,
														  &ambig);
					BlkCtx	   *bc;

					if (!v)
					{
						memset(dst, 0, page_size);	/* unwritten -> zeros */
						continue;
					}
					/* cache by the resolved source timeline; bypass the cache for
					 * same-lsn-ambiguous versions (the lsn key can't tell them
					 * apart), matching read_resolve() on the POSIX path */
					if (!ambig &&
						ps_pgcache_lookup(ps_core_pgcache_for(&ch->key), stl,
										  &ch->key, blk, v->lsn, dst))
						continue;	/* RAM hit -> no device read */
					bc = &rs->blk[nsub++];
					bc->rs = rs;
					bc->tl = stl;
					bc->key = ch->key;
					bc->block = blk;
					bc->lsn = v->lsn;
					bc->crc = v->crc;
					bc->cacheable = !ambig;
					bc->dst = dst;
					rs->pending++;
					ps_spdk_read_async(v->seg, v->off, dst, page_size,
									   read_done, bc);
				}
				/* release the submit guard; if every read already completed
				 * (inline) or there were none, this publishes DONE now */
				if (--rs->pending == 0)
				{
					rs->active = 0;
					ps_store_release(&ch->state, PS_STATE_DONE);
				}
				return;			/* DONE published by read_done otherwise */
			}

		case PS_OP_READ_AT:
			{
				uint32_t	stl;
				int			ambig;
				PageVer    *v = read_through_cacheable(tl, &ch->key,
													  ch->blocknum, ch->req_lsn,
													  &stl, &ambig);
				ReqState   *rs = &reqstate[i];
				BlkCtx	   *bc;

				if (!v)
				{
					memset(ch->data, 0, page_size);
					ps_store_release(&ch->state, PS_STATE_DONE);
					return;
				}
				/* cache by the resolved source timeline; bypass for ambiguous
				 * same-lsn versions (see READV above) */
				if (!ambig &&
					ps_pgcache_lookup(ps_core_pgcache_for(&ch->key), stl,
									  &ch->key, ch->blocknum, v->lsn, ch->data))
				{
					ps_store_release(&ch->state, PS_STATE_DONE);	/* RAM hit */
					return;
				}
				rs->ch = ch;
				rs->pending = 1;
				rs->active = 1;
				bc = &rs->blk[0];
				bc->rs = rs;
				bc->tl = stl;
				bc->key = ch->key;
				bc->block = ch->blocknum;
				bc->lsn = v->lsn;
				bc->crc = v->crc;
				bc->cacheable = !ambig;
				bc->dst = ch->data;
				ps_spdk_read_async(v->seg, v->off, ch->data, page_size,
								   read_done, bc);
				return;
			}

		default:
			ch->status = PS_STATUS_ERROR;
			ps_store_release(&ch->state, PS_STATE_DONE);
			return;
	}
}

/* one per-shard worker thread (sharding step 5b): its own NVMe qpair + buffers */
typedef struct Worker
{
	pthread_t	tid;
	void	   *shm;
	uint32_t	shard;			/* this worker's shard index */
	uint32_t	first;			/* its channel pool: [first, first+count) */
	uint32_t	count;
} Worker;

/*
 * Async serve loop for one shard: scan only this shard's channel pool, begin each
 * ready request (reads submit to this shard's qpair and stay 'active' until their
 * completions publish DONE), and drive this shard's qpair completions.  Two
 * workers never touch the same channel or the same qpair, so shards run the
 * device concurrently with no shared mutable state.
 */
static void *
spdk_worker_main(void *arg)
{
	Worker	   *w = arg;

	while (!__atomic_load_n(&stop_requested, __ATOMIC_RELAXED))
	{
		int			work = 0;

		for (uint32_t i = w->first; i < w->first + w->count; i++)
		{
			PsChannel  *ch = ps_channel(w->shm, i);
			uint32_t	owner;

			if (reqstate[i].active ||
				ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;
			/* single-owner guard (as the POSIX daemon): a keyed request always
			 * belongs to this shard's pool; reject a misrouted one rather than
			 * drive another shard's state/qpair from this thread.  PS_ANY_SHARD
			 * ops (e.g. IMMEDSYNC) run anywhere. */
			owner = ps_request_shard(ch);
			if (ch->opcode == PS_OP_IMMEDSYNC)
			{
				/* global durability barrier: each shard flushes its own qpair
				 * (begin()->spdk_sync would flush every shard from this one
				 * thread, racing the other shards' single-owner qpairs).  Trylock
				 * to serialize barriers; if another barrier holds it, leave this
				 * request pending and retry next iteration -- staying in the loop so
				 * we keep servicing that barrier's flush request below (blocking
				 * here would deadlock it).  Report a failed/aborted barrier so the
				 * client doesn't believe the data is durable. */
				if (pthread_mutex_trylock(&g_barrier_mtx) == 0)
				{
					int			brc = immedsync_barrier(w->shard);

					pthread_mutex_unlock(&g_barrier_mtx);
					ch->status = brc == 0 ? PS_STATUS_OK : PS_STATUS_ERROR;
					ps_store_release(&ch->state, PS_STATE_DONE);
				}
			}
			else if (owner != PS_ANY_SHARD && owner != w->shard)
			{
				ch->status = PS_STATUS_ERROR;
				ps_store_release(&ch->state, PS_STATE_DONE);
			}
			else
				begin(i, ch);	/* publishes DONE (sync) or via read_done (async) */
			work = 1;
		}

		/* flush our own shard if another shard's IMMEDSYNC barrier requested it
		 * (records the result + post-flush count the coordinator collects) */
		if (__atomic_load_n(&g_flush_req[w->shard], __ATOMIC_ACQUIRE))
		{
			flush_self(w->shard);
			__atomic_store_n(&g_flush_req[w->shard], 0, __ATOMIC_RELEASE);
			work = 1;
		}

		if (ps_spdk_poll(w->shard) > 0)
			work = 1;

		if (!work)
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}
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
		else if (strcmp(argv[i], "--cache-pages") == 0 && i + 1 < argc)
			cache_pages = atoi(argv[++i]);
		else if (strcmp(argv[i], "--nshards") == 0 && i + 1 < argc)
			ps_nshards = (uint32_t) strtoul(argv[++i], NULL, 10);
		else
		{
			fprintf(stderr, "usage: %s --shm NAME --store DIR --pci ADDR "
					"[--page-size N] [--segment-size N] [--nshards N]\n", argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR --pci ADDR "
				"[--page-size N] [--segment-size N] [--nshards N]\n", argv[0]);
		return 2;
	}
	if (ps_nshards < 1 || ps_nshards > PS_MAX_SHARDS)
	{
		fprintf(stderr, "--nshards must be in [1, %d] (got %u)\n",
				PS_MAX_SHARDS, ps_nshards);
		return 2;
	}
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
	/* Fill every descriptive field first, then publish 'magic' last as the
	 * readiness sentinel (wait_ready / clients gate on it), so a client that
	 * observes the daemon ready also sees nshards and the channel geometry. */
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = page_size;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->nshards = ps_nshards;	/* clients route to the same shard pools */
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	/*
	 * One worker thread per shard, each with its own NVMe qpair (allocated by
	 * spdk_open) and channel pool, running the async cross-channel loop on its own
	 * shard.  Magic is published only after every worker exists, so a client that
	 * sees the shm ready knows all shards are being served.
	 */
	{
		Worker	   *workers = calloc(ps_nshards, sizeof(Worker));

		if (!workers)
		{
			perror("calloc workers");
			return 1;
		}
		for (uint32_t s = 0; s < ps_nshards; s++)
		{
			workers[s].shm = shm;
			workers[s].shard = s;
			ps_shard_channel_range(s, ps_nshards, PS_MAX_CHANNELS,
								   &workers[s].first, &workers[s].count);
			if (pthread_create(&workers[s].tid, NULL, spdk_worker_main,
							   &workers[s]) != 0)
			{
				perror("pthread_create");
				/* magic not yet published, so no client treats the shm as ready;
				 * stop the workers already started and bail */
				__atomic_store_n(&stop_requested, 1, __ATOMIC_RELAXED);
				for (uint32_t j = 0; j < s; j++)
					pthread_join(workers[j].tid, NULL);
				free(workers);
				return 1;
			}
		}

		/* all workers exist: publish magic with a release store (readers' acquire
		 * pairs with it), making the shm ready to serve */
		ps_store_release(&hdr->magic, PS_SHM_MAGIC);
		fprintf(stderr, "pagestore_daemon_spdk: shm=%s store=%s storage=%s "
				"page_size=%u io_unit=%u channels=%d nshards=%u ready\n",
				shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
				PS_MAX_CHANNELS, ps_nshards);

		for (uint32_t s = 0; s < ps_nshards; s++)
			pthread_join(workers[s].tid, NULL);
		free(workers);
	}

	{
		uint64_t	ch,
					cm,
					ce;

		ps_core_pgcache_stats(&ch, &cm, &ce);
		fprintf(stderr, "pagestore_daemon_spdk: shutting down (pgcache hit=%llu "
				"miss=%llu evict=%llu)\n", (unsigned long long) ch,
				(unsigned long long) cm, (unsigned long long) ce);
	}
	ps_core_close();			/* flush the memtable into a layer before detaching */
	ps_storage->close();
	munmap(shm, PS_SHM_SIZE);
	return 0;
}
