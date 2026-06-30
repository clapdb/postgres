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

static volatile sig_atomic_t stop_requested = 0;

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
			if (append_page(tl, &ch->key, ch->blocknum, ch->data,
							ch->req_lsn) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(tl, &ch->key, ch->blocknum + 1);
			break;

		case PS_OP_WRITEV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + i,
								ch->data + (size_t) i * page_size,
								ch->req_lsn) != 0)
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
			/* as-of read on this timeline, honoring branch ancestry.  Report
			 * found-ness in ch->result so callers can distinguish "this block has
			 * a stored version" from "zero-filled because it is unwritten" (a block
			 * below the fork length but never written must not look present). */
			if (read_resolve(tl, &ch->key, ch->blocknum, ch->req_lsn, ch->data))
				ch->result = 1;
			else
			{
				memset(ch->data, 0, page_size);
				ch->result = 0;
			}
			break;

		default:
			ch->status = PS_STATUS_ERROR;
			break;
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

static void
run_request(PsChannel *ch)
{
	PsOpcode	op = (PsOpcode) ch->opcode;

	/*
	 * Branch creation mutates only the cross-shard timelines[]; take map_lock
	 * alone (no shard lock), so it serializes against map readers/writers but
	 * not against per-shard work on unrelated shards.
	 */
	if (op == PS_OP_CREATE_BRANCH)
	{
		ps_lock_map_wr();
		handle_request(ch);
		ps_store_release(&ch->state, PS_STATE_DONE);
		ps_unlock_map();
		return;
	}

	/*
	 * IMMEDSYNC fsyncs the shared segment-fd cache; the POSIX backend serializes
	 * that internally (seg_fds_lock), so no per-shard lock is needed (and a single
	 * shard lock would not have excluded the other shards' fd-cache mutations).
	 *
	 * SPDK's sync(), however, flushes every shard's in-memory curbuf, which a
	 * concurrent shard write mutates -- with per-shard locking there is no single
	 * write lock to exclude that, so for such backends hold every shard's write
	 * lock (ascending, the established shard order) around the sync.
	 */
	if (op == PS_OP_IMMEDSYNC)
	{
		if (ps_storage->sync_needs_write_lock)
		{
			for (uint32_t s = 0; s < ps_nshards; s++)
				ps_lock_shard_wr(s);
			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			for (uint32_t s = ps_nshards; s-- > 0;)
				ps_unlock_shard(s);
		}
		else
		{
			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
		}
		return;
	}

	{
		uint32_t	shard;

		/*
		 * Derive the shard from the FINAL request key (klass-aware), not a
		 * client-supplied ch->shard: object I/O claims its channel before setting
		 * ch->key.klass, and freestanding IPC clients don't populate ch->shard at
		 * all -- trusting it would lock one shard while handle_request() mutates
		 * shard_for(&ch->key).  The shipped-WAL byte ops (append/size/read) are not
		 * keyed (they touch the per-timeline WAL log), so serialize them on shard 0.
		 */
		if (op == PS_OP_WAL_APPEND || op == PS_OP_WAL_SIZE || op == PS_OP_WAL_READ)
			shard = 0;
		else
			shard = ps_shard_of(&ch->key);

		if (request_is_write(op))
		{
			/*
			 * Writes touch only this shard's state; append_page escalates to a
			 * brief map_wr itself when a flush/compaction mutates the map.
			 */
			ps_lock_shard_wr(shard);
			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			ps_unlock_shard(shard);
		}
		else
		{
			/* Reads consult this shard's indexes plus the cross-shard map/timelines. */
			ps_lock_shard_rd(shard);
			ps_lock_map_rd();
			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			ps_unlock_map();
			ps_unlock_shard(shard);
		}
	}
}

static void *
shard_worker(void *arg)
{
	WorkerArgs *wa = (WorkerArgs *) arg;
	uint32_t	shard = wa->shard;
	void	   *shm = wa->shm;
	uint32_t	nchannels = wa->nchannels;
	uint32_t	nshards = wa->nshards;

	while (!stop_requested)
	{
		int			did_work = 0;

		for (uint32_t i = shard; i < nchannels; i += nshards)
		{
			PsChannel  *ch = ps_channel(shm, i);

			if (ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;

			run_request(ch);
			did_work = 1;
		}

		if (!did_work && shard == 0)
		{
			/* maintenance takes the shard + map locks it needs internally */
			int			do_maint = ps_core_maintenance();

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
	uint32_t	nshards = 1;

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
		else if (strcmp(argv[i], "--nshards") == 0 && i + 1 < argc)
			nshards = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--cache-pages") == 0 && i + 1 < argc)
			cache_pages = atoi(argv[++i]);
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
					"[--page-size N] [--segment-size N] [--nshards N] [--storage NAME]\n",
					argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT ||
		nshards == 0 || nshards > PS_MAX_CHANNELS)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR "
				"[--page-size N] [--segment-size N] [--nshards N] [--storage NAME]\n",
				argv[0]);
		return 2;
	}
	ps_nshards = nshards;

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

	fprintf(stderr, "pagestore_daemon: shm=%s store=%s storage=%s page_size=%u "
			"io_unit=%u channels=%u nshards=%u ready\n",
			shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
			PS_MAX_CHANNELS, hdr->nshards);

	{
		WorkerArgs *workers = malloc((size_t) hdr->nshards * sizeof(WorkerArgs));
		pthread_t  *threads = malloc((size_t) hdr->nshards * sizeof(pthread_t));
		uint32_t	started = 0;

		if (!workers || !threads)
		{
			fprintf(stderr, "pagestore_daemon: cannot allocate worker slots\n");
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
				fprintf(stderr, "pagestore_daemon: failed to start worker %u\n", shard);
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
		uint64_t	rm,
					rl,
					rs,
					ch,
					cm,
					ce;

		ps_core_read_stats(&rm, &rl, &rs);
		ps_pgcache_stats(&ch, &cm, &ce);
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
