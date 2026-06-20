/*-------------------------------------------------------------------------
 *
 * pagestore_daemon.c
 *	  POSIX frontend for the page-store daemon.
 *
 * The store's logic (indexes, COW, timelines, WAL, recovery) lives in the
 * shared brain pagestore_core.c; this frontend is just the synchronous request
 * loop over the shared-memory channels plus the page byte I/O, served through
 * the POSIX storage backend (storage_posix.c).  It is libc-only and depends on
 * nothing else -- the portable default daemon.  The SPDK daemon is a separate
 * binary that reuses the same brain with an asynchronous loop.
 *
 * Includes only pagestore_ipc.h / pagestore_core.h and libc -- never
 * PostgreSQL headers.
 *
 * Usage: pagestore_daemon --shm NAME --store DIR
 *                         [--page-size N] [--segment-size N] [--storage NAME]
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"
#include "pagestore_layer_store.h"
#include "pagestore_pgcache.h"

/* Set by the signal handler and read by every worker thread, so accesses are
 * atomic: volatile sig_atomic_t only covers the signal-handler/main case, not
 * inter-thread visibility.  RELAXED is enough -- it only gates loop exit, and the
 * channel state machine carries the real ordering. */
static volatile sig_atomic_t stop_requested = 0;

static void
on_signal(int sig)
{
	(void) sig;
	__atomic_store_n(&stop_requested, 1, __ATOMIC_RELAXED);
}

static void handle_request(PsChannel *ch);

/* one per-shard worker thread (sharding step 4c-iv) */
typedef struct Worker
{
	pthread_t	tid;
	void	   *shm;
	uint32_t	shard;			/* this worker's shard index */
	uint32_t	first;			/* its channel pool: [first, first+count) */
	uint32_t	count;
} Worker;

/*
 * Serve loop for one shard: poll only this shard's channel pool, so two workers
 * never touch the same channel.  Each request self-routes by key to this shard's
 * state, and idle time runs this shard's own compaction.
 */
static void *
worker_main(void *arg)
{
	Worker	   *w = arg;

	while (!__atomic_load_n(&stop_requested, __ATOMIC_RELAXED))
	{
		int			did_work = 0;

		for (uint32_t i = w->first; i < w->first + w->count; i++)
		{
			PsChannel  *ch = ps_channel(w->shm, i);
			uint32_t	owner;

			if (ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;
			/* Single-owner guard: only serve a request that belongs to this
			 * shard.  A correctly-routing client always posts on the owning
			 * shard's pool; a non-routing client (or a stale one) that lands a
			 * keyed request here is rejected rather than allowed to mutate another
			 * shard's state from this thread.  PS_ANY_SHARD ops run anywhere. */
			owner = ps_request_shard(ch);
			if (owner != PS_ANY_SHARD && owner != w->shard)
				ch->status = PS_STATUS_ERROR;
			else
				handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			did_work = 1;
		}

		if (!did_work)
		{
			/* idle: one unit of this shard's background compaction, else sleep */
			if (!ps_core_maintenance_shard(w->shard))
			{
				struct timespec ts = {0, 20000};	/* 20us */

				nanosleep(&ts, NULL);
			}
		}
	}
	return NULL;
}

/*
 * Serve one request.  The metadata ops are handled by the shared brain; the
 * four byte-I/O ops are done here synchronously via the storage backend.
 */
static void
handle_request(PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	ch->status = PS_STATUS_OK;
	ch->result = 0;

	if (ps_handle_meta(ch))
		return;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_EXTEND:
			if (append_page(tl, &ch->key, ch->blocknum, ch->data) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(tl, &ch->key, ch->blocknum + 1);
			break;

		case PS_OP_WRITEV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + i,
								 ch->data + (size_t) i * page_size) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
			if (ch->status == PS_STATUS_OK)
				fork_grow(tl, &ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_READV:
			/* "current" read on this timeline: resolve at max LSN, serving from
			 * memtable / image layers with a segment fallback */
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				unsigned char *dst = ch->data + (size_t) i * page_size;

				if (!read_resolve(tl, &ch->key, ch->blocknum + i, UINT64_MAX, dst))
					memset(dst, 0, page_size);	/* unwritten -> zeros */
			}
			break;

		case PS_OP_READ_AT:
			/* as-of read on this timeline, honoring branch ancestry */
			if (!read_resolve(tl, &ch->key, ch->blocknum, ch->req_lsn, ch->data))
				memset(ch->data, 0, page_size);
			break;

		default:
			ch->status = PS_STATUS_ERROR;
			break;
	}
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

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--store") == 0 && i + 1 < argc)
			store_dir = argv[++i];
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
		else if (strcmp(argv[i], "--object-dir") == 0 && i + 1 < argc)
		{
			/* enable the object tier: maintenance uploads sealed layers here */
			const char *od = argv[++i];

			if (ps_layer_store_set_object_dir(od) != 0)
			{
				fprintf(stderr, "--object-dir path too long\n");
				return 2;
			}
			/* Fail fast on an unusable directory rather than start, acknowledge
			 * writes, and have every background upload silently fail (leaving the
			 * requested remote durability absent).  Create it if missing, then
			 * require it to be an actual writable directory -- an existing regular
			 * file would pass mkdir(EEXIST)+access but make every obj_* create
			 * fail with ENOTDIR. */
			struct stat odst;

			if (mkdir(od, 0700) != 0 && errno != EEXIST)
			{
				fprintf(stderr, "--object-dir %s: %s\n", od, strerror(errno));
				return 2;
			}
			if (stat(od, &odst) != 0 || !S_ISDIR(odst.st_mode))
			{
				fprintf(stderr, "--object-dir %s is not a directory\n", od);
				return 2;
			}
			if (access(od, W_OK | X_OK) != 0)
			{
				fprintf(stderr, "--object-dir %s not writable: %s\n",
						od, strerror(errno));
				return 2;
			}
			ps_object_tier = 1;
		}
		else if (strcmp(argv[i], "--storage") == 0 && i + 1 < argc)
		{
			const char *name = argv[++i];

			if (strcmp(name, "posix") == 0)
				ps_storage = &PsStoragePosix;
#ifdef PAGESTORE_SPDK
			else if (strcmp(name, "spdk") == 0)
				ps_storage = &PsStorageSpdk;
#endif
			else
			{
				fprintf(stderr, "unknown --storage backend '%s'\n", name);
				return 2;
			}
		}
		else
		{
			fprintf(stderr, "usage: %s --shm NAME --store DIR "
					"[--page-size N] [--segment-size N] [--storage NAME]\n",
					argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR "
				"[--page-size N] [--segment-size N] [--nshards N] [--storage NAME]\n",
				argv[0]);
		return 2;
	}
	if (ps_nshards < 1 || ps_nshards > PS_MAX_SHARDS)
	{
		fprintf(stderr, "--nshards must be in [1, %d] (got %u)\n",
				PS_MAX_SHARDS, ps_nshards);
		return 2;
	}
	/*
	 * The SPDK backend keeps a single qpair + current-segment buffer, so multiple
	 * per-shard worker threads driving it would race/corrupt that state.  Multi-
	 * shard SPDK needs per-thread qpairs (SHARDING.md step 5); until then run a
	 * single shard whenever this daemon uses the SPDK storage (the dedicated
	 * pagestore_daemon_spdk clamps the same way).
	 */
	if (strcmp(ps_storage->name, "spdk") == 0 && ps_nshards != 1)
	{
		fprintf(stderr, "pagestore_daemon: --storage spdk does not support "
				"--nshards > 1 yet (SHARDING.md step 5); using --nshards 1\n");
		ps_nshards = 1;
	}

	if (ps_core_open(store_dir) != 0)
	{
		perror("storage open");
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

	/*
	 * Initialize the shared region.  NB: this zeroes the whole segment, so the
	 * daemon must be (re)started while no engine is attached -- restarting it
	 * against a live shm would wipe in-flight channel state.  A production
	 * version would attach without re-initializing when the header is already
	 * valid.
	 */
	hdr = (PsShmHeader *) shm;
	memset(shm, 0, PS_SHM_SIZE);
	/* Fill every descriptive field now, but publish 'magic' (the readiness
	 * sentinel clients gate on) only after all workers exist -- otherwise a client
	 * could attach and post into a pool no thread is serving yet. */
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
	 * One worker thread per shard, each serving only its channel pool (sharding
	 * step 4c-iv).  Per-shard state is single-owner; the small shared state is
	 * synchronized (segment fd cache mutex, timeline 'defined' atomics, shard-0
	 * branch/WAL routing).  At nshards == 1 this is a single worker == the old
	 * single-threaded loop.  The main thread waits for a signal, then joins.
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
			if (pthread_create(&workers[s].tid, NULL, worker_main,
							   &workers[s]) != 0)
			{
				perror("pthread_create");
				/* never published magic, so no client treats the shm as ready;
				 * stop the workers already started and bail */
				__atomic_store_n(&stop_requested, 1, __ATOMIC_RELAXED);
				for (uint32_t j = 0; j < s; j++)
					pthread_join(workers[j].tid, NULL);
				free(workers);
				return 1;
			}
		}

		/* all workers exist: publish magic with a release store (the readers'
		 * acquire-load of it pairs with this), making the shm ready to serve */
		ps_store_release(&hdr->magic, PS_SHM_MAGIC);
		fprintf(stderr, "pagestore_daemon: shm=%s store=%s storage=%s page_size=%u "
				"io_unit=%u channels=%d nshards=%u ready\n",
				shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
				PS_MAX_CHANNELS, ps_nshards);

		for (uint32_t s = 0; s < ps_nshards; s++)
			pthread_join(workers[s].tid, NULL);
		free(workers);
	}

	{
		uint64_t	rm,
					rl,
					rs,
					ch,
					cm,
					ce;

		ps_core_read_stats(&rm, &rl, &rs);
		ps_core_pgcache_stats(&ch, &cm, &ce);
		fprintf(stderr, "pagestore_daemon: shutting down (%u image layers; reads "
				"mem=%llu layer=%llu seg=%llu; pgcache hit=%llu miss=%llu evict=%llu)\n",
				ps_core_layer_count(),
				(unsigned long long) rm, (unsigned long long) rl,
				(unsigned long long) rs, (unsigned long long) ch,
				(unsigned long long) cm, (unsigned long long) ce);
	}
	ps_core_close();			/* flush the memtable so restart rebuilds from layers */
	munmap(shm, PS_SHM_SIZE);
	return 0;
}
