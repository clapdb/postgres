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
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"
#include "pagestore_pgcache.h"

static volatile sig_atomic_t stop_requested = 0;

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
				"[--page-size N] [--segment-size N] [--storage NAME]\n",
				argv[0]);
		return 2;
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
	/* Fill every descriptive field first, then publish 'magic' last as the
	 * readiness sentinel (wait_ready / clients gate on it), so a client that
	 * observes the daemon ready also sees nshards and the channel geometry. */
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = page_size;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->nshards = PS_NSHARDS;	/* clients route to the same shard pools */
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;
	__atomic_thread_fence(__ATOMIC_RELEASE);
	hdr->magic = PS_SHM_MAGIC;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	fprintf(stderr, "pagestore_daemon: shm=%s store=%s storage=%s page_size=%u "
			"io_unit=%u channels=%d ready\n",
			shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
			PS_MAX_CHANNELS);

	while (!stop_requested)
	{
		int			did_work = 0;

		for (uint32_t i = 0; i < PS_MAX_CHANNELS; i++)
		{
			PsChannel  *ch = ps_channel(shm, i);

			if (ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;

			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			did_work = 1;
		}

		if (!did_work)
		{
			/* idle: do one unit of background compaction before sleeping */
			if (!ps_core_maintenance())
			{
				struct timespec ts = {0, 20000};	/* 20us */

				nanosleep(&ts, NULL);
			}
		}
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
