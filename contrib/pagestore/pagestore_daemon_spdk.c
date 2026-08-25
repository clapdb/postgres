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
#include <errno.h>
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
#include "pagestore_retention.h"
#include "pagestore_fault.h"
#include "storage_spdk.h"

/* most page reads a single request can carry (nblocks * page_size <= io_unit) */
#define MAX_BLOCKS	128
#define MAINTENANCE_CHECK_NS	100000000L	/* 100ms */

static volatile sig_atomic_t stop_requested = 0;
static pthread_rwlock_t core_rwlock = PTHREAD_RWLOCK_INITIALIZER;
static PsShmHeader *daemon_hdr = NULL;

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
	uint64_t	admission_seq;
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
		ps_pgcache_insert(bc->tl, &bc->key, bc->block, bc->lsn,
						  bc->admission_seq, bc->dst);
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

/*
 * Release the submission hold taken before queuing a request's reads.
 * ps_spdk_read_async() completes reads served from the in-memory append
 * segment (or absent segments) SYNCHRONOUSLY, so without a hold an early
 * completion could zero 'pending' and publish DONE while later blocks of the
 * same request are still being queued -- the client would read a partially
 * filled channel.  The hold keeps pending >= 1 until every block is queued;
 * this release publishes DONE itself if all completions already fired.
 */
static void
submit_done(ReqState *rs)
{
	if (--rs->pending == 0)
	{
		rs->active = 0;
		ps_store_release(&rs->ch->state, PS_STATE_DONE);
	}
}

static int
request_is_write(PsOpcode opcode)
{
	switch (opcode)
	{
		case PS_OP_BEGIN_DELETE:
			/* The shared core lifecycle path needs a map lock and a complete
			 * async-I/O drain.  This frontend does not provide that drain yet;
			 * fail closed until the lifecycle-specific SPDK barrier lands. */
			return 0;
		case PS_OP_CREATE:
		case PS_OP_UNLINK:
		case PS_OP_TRUNCATE:
		case PS_OP_ZEROEXTEND:
		case PS_OP_CREATE_BRANCH:
		case PS_OP_CHECK_BRANCH:
		case PS_OP_REQUIRE_BRANCH:
		case PS_OP_EXTEND:
		case PS_OP_WRITEV:
		case PS_OP_WAL_APPEND:
		case PS_OP_WAL_INDEX_ADD:
		case PS_OP_WAL_INDEX_ADD_BATCH:
		case PS_OP_IMMEDSYNC:
			return 1;
		case PS_OP_EXISTS:
		case PS_OP_NBLOCKS:
		case PS_OP_READV:
		case PS_OP_READ_AT:
		case PS_OP_WAL_SIZE:
		case PS_OP_WAL_READ:
		case PS_OP_WAL_INDEX_GET:
		case PS_OP_WAL_RETAIN_FLOOR:
		case PS_OP_RETENTION_PIN_GET:
		case PS_OP_RETENTION_PIN_LOOKUP:
		case PS_OP_RETENTION_PIN_SET:
		case PS_OP_RETENTION_PIN_DROP:
		case PS_OP_RETENTION_FLOOR:
		case PS_OP_ADMISSION_BARRIER:
		case PS_OP_TIMELINE_STATE:
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
			/* append_page grows the fork with the page's exact LSN */
			if (append_page(tl, &ch->key, ch->blocknum, ch->data,
							ch->req_lsn, &ch->req_seq) != 0)
				ch->status = PS_STATUS_ERROR;
			ps_store_release(&ch->state, PS_STATE_DONE);
			return;

		case PS_OP_WRITEV:
			for (uint32_t b = 0; b < ch->nblocks; b++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + b,
								 ch->data + (size_t) b * page_size,
								 ch->req_lsn, &ch->req_seq) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
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
				rs->pending = 1;	/* submission hold: see submit_done() */
				rs->active = 1;
				for (uint32_t b = 0; b < nb; b++)
				{
					unsigned char *dst = ch->data + (size_t) b * page_size;
					uint32_t	blk = ch->blocknum + b;
					/* req_lsn nonzero = a pinned reader's horizon cap;
					 * 0 keeps the newest (writer) semantics */
					PageVer    *v = read_through(tl, &ch->key, blk,
											 ch->req_lsn ? ch->req_lsn
											 : UINT64_MAX,
											 ch->req_seq);
					BlkCtx	   *bc;

					if (!v)
					{
						memset(dst, 0, page_size);	/* unwritten -> zeros */
						continue;
					}
					if (ch->req_seq != 0 && v->lsn == 0)
					{
						/* WAL-less content is not as-of-resolvable; see the
						 * POSIX daemon's capped-read refusal */
						ch->status = PS_STATUS_ERROR;
						break;
					}
					if (ps_pgcache_lookup(tl, &ch->key, blk, v->lsn,
										  v->admission_seq, dst))
						continue;	/* RAM hit -> no device read */
					bc = &rs->blk[rs->pending - 1];	/* slot 0.. behind the hold */
					rs->pending++;
					bc->rs = rs;
					bc->tl = tl;
					bc->key = ch->key;
					bc->block = blk;
					bc->lsn = v->lsn;
					bc->admission_seq = v->admission_seq;
					bc->dst = dst;
					ps_spdk_read_async(v->shard, v->seg, v->off, dst, page_size,
									   read_done, bc);
				}
				submit_done(rs);	/* releases the hold; publishes if all landed */
				return;			/* DONE published by read_done/submit_done */
			}

		case PS_OP_READ_AT:
			{
				uint64_t	read_lsn = ch->req_lsn;
				PageVer    *v = read_through(tl, &ch->key, ch->blocknum,
										 read_lsn, ch->req_seq);
				ReqState   *rs = &reqstate[i];
				BlkCtx	   *bc;

				if (!v)
				{
					memset(ch->data, 0, page_size);		/* not found: result 0 */
					ps_store_release(&ch->state, PS_STATE_DONE);
					return;
				}
				if (read_lsn != UINT64_MAX && v->lsn == 0)
				{
					memset(ch->data, 0, page_size);
					ch->status = PS_STATUS_ERROR;
					ps_store_release(&ch->state, PS_STATE_DONE);
					return;
				}
				/* report the resolved version for an exact-cutoff SLRU read; defer
				 * found-ness (ch->result) until the page actually lands, so a failed
				 * async read does not advertise a zero-filled page as found */
				ch->req_lsn = v->lsn;
				if (ps_pgcache_lookup(tl, &ch->key, ch->blocknum, v->lsn,
									  v->admission_seq,
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
				bc->admission_seq = v->admission_seq;
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

static uint64_t
relation_request_lsn(const PsChannel *ch)
{
	PsOpcode	op = (PsOpcode) ch->opcode;

	if (ch->key.klass != PS_KLASS_RELATION)
		return UINT64_MAX;
	if (op == PS_OP_EXTEND || op == PS_OP_WRITEV)
	{
		uint32_t	npages = op == PS_OP_EXTEND ? 1 : ch->nblocks;
		uint64_t	lowest = UINT64_MAX;

		for (uint32_t i = 0; i < npages; i++)
		{
			uint32_t	hi,
						lo;
			uint64_t	lsn;

			memcpy(&hi, ch->data + (size_t) i * page_size, sizeof(hi));
			memcpy(&lo, ch->data + (size_t) i * page_size + sizeof(hi), sizeof(lo));
			lsn = ((uint64_t) hi << 32) | lo;
			if (lsn < lowest)
				lowest = lsn;
		}
		return lowest;
	}
	if (op == PS_OP_CREATE || op == PS_OP_UNLINK || op == PS_OP_TRUNCATE ||
		op == PS_OP_ZEROEXTEND)
		return ch->req_lsn;
	return UINT64_MAX;
}

static uint64_t
active_fence_epoch(void)
{
	uint64_t	epoch = ps_load_acquire_u64(&daemon_hdr->admission_pending_epoch);
	uint32_t	owner;

	if (epoch == 0)
		return 0;
	owner = ps_load_acquire(&daemon_hdr->admission_fence_owner);
	if (owner != 0 && kill((pid_t) owner, 0) != 0 && errno == ESRCH &&
		ps_cas_u64(&daemon_hdr->admission_pending_epoch, epoch, 0))
	{
		ps_store_release_u64(&daemon_hdr->admission_pending_lsn, 0);
		ps_store_release(&daemon_hdr->admission_fence_owner, 0);
		return 0;
	}
	return epoch;
}

static int
run_request(uint32_t i, PsChannel *ch)
{
	PsOpcode	op = (PsOpcode) ch->opcode;
	int			is_write = request_is_write(op);
	uint64_t	epoch;

	if (op == PS_OP_BEGIN_DELETE)
	{
		ch->status = PS_STATUS_ERROR;
		/* No SPDK lifecycle drain exists in this slice.  Complete the request
		 * explicitly rather than entering begin()/the shared core without its
		 * required map/admission synchronization. */
		ps_store_release(&ch->state, PS_STATE_DONE);
		return 1;
	}

	/* Retention registry operations own their own mutex; mutations can also
	 * fsync host metadata.  Do not nest the global core lock around either. */
	if (op == PS_OP_RETENTION_PIN_LOOKUP ||
		op == PS_OP_RETENTION_PIN_SET || op == PS_OP_RETENTION_PIN_DROP ||
		op == PS_OP_RETENTION_PIN_RESERVE)
	{
		int timeline_ok;

		pthread_rwlock_rdlock(&core_rwlock);
		timeline_ok = ps_timeline_defined(ch->timeline);
		pthread_rwlock_unlock(&core_rwlock);
		if (timeline_ok)
			begin(i, ch);
		else
		{
			ch->status = PS_STATUS_ERROR;
			ps_store_release(&ch->state, PS_STATE_DONE);
		}
		return 1;
	}

	if (op == PS_OP_ADMISSION_BARRIER)
	{
		pthread_rwlock_wrlock(&core_rwlock);
		ch->status = PS_STATUS_OK;
		ch->result = 0;
		ch->req_seq = ps_admission_barrier();
		if (ch->req_seq == 0)
			ch->status = PS_STATUS_ERROR;
		ps_store_release(&ch->state, PS_STATE_DONE);
		pthread_rwlock_unlock(&core_rwlock);
		return 1;
	}

	if (is_write)
	{
		epoch = active_fence_epoch();
		pthread_rwlock_wrlock(&core_rwlock);
		ps_admission_read_lock();
		if (epoch != active_fence_epoch() ||
			(epoch != 0 && relation_request_lsn(ch) <=
			 ps_load_acquire_u64(&daemon_hdr->admission_pending_lsn)))
		{
			ps_admission_read_unlock();
			pthread_rwlock_unlock(&core_rwlock);
			return 0;
		}
	}
	else
		pthread_rwlock_rdlock(&core_rwlock);
	if (op == PS_OP_CREATE_BRANCH || op == PS_OP_CHECK_BRANCH ||
		op == PS_OP_REQUIRE_BRANCH)
		ps_lock_map_wr();
	begin(i, ch);
	if (op == PS_OP_CREATE_BRANCH || op == PS_OP_CHECK_BRANCH ||
		op == PS_OP_REQUIRE_BRANCH)
		ps_unlock_map();
	if (is_write)
		ps_admission_read_unlock();
	pthread_rwlock_unlock(&core_rwlock);
	return 1;
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

			if (run_request(i, ch))
				did_work = 1;
		}

		if (ps_spdk_poll(shard) > 0)
			did_work = 1;

		if (!did_work)
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}
	ps_spdk_thread_close(shard);
	return NULL;
}

static void *
maintenance_worker(void *arg)
{
	(void) arg;

	while (!stop_requested)
	{
		if (!ps_core_maintenance())
		{
			struct timespec ts = {0, MAINTENANCE_CHECK_NS};

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
	if (ps_fault_init(store_dir) != 0)
	{
		fprintf(stderr, "pagestore_daemon_spdk: invalid fault configuration\n");
		return 1;
	}

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
	ps_core_set_metrics_header(hdr);
	daemon_hdr = hdr;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	fprintf(stderr, "pagestore_daemon_spdk: shm=%s store=%s storage=%s "
			"page_size=%u io_unit=%u channels=%u nshards=%u ready\n",
			shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
			PS_MAX_CHANNELS, hdr->nshards);
	if (ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY) != 0)
	{
		fprintf(stderr, "pagestore_daemon_spdk: fault probe daemon.after_ready failed\n");
		ps_core_close();
		ps_storage->close();
		munmap(shm, PS_SHM_SIZE);
		return 1;
	}

	{
		WorkerArgs *workers = malloc((size_t) hdr->nshards * sizeof(WorkerArgs));
		pthread_t  *threads = malloc((size_t) hdr->nshards * sizeof(pthread_t));
		pthread_t	maintenance;
		uint32_t	started = 0;
		int			maintenance_started = 0;

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
		if (started == hdr->nshards)
		{
			if (pthread_create(&maintenance, NULL, maintenance_worker, NULL) != 0)
			{
				fprintf(stderr, "pagestore_daemon_spdk: failed to start maintenance worker\n");
				stop_requested = 1;
			}
			else
				maintenance_started = 1;
		}

		for (uint32_t shard = 0; shard < started; shard++)
			pthread_join(threads[shard], NULL);
		if (maintenance_started)
			pthread_join(maintenance, NULL);

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
