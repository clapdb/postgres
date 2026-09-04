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
#include <errno.h>
#include <limits.h>
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
#include "pagestore_fault.h"

static volatile sig_atomic_t stop_requested = 0;
static PsShmHeader *daemon_hdr = NULL;
static const char *maintenance_pause_file = NULL;
static const char *shutdown_cancel_pause_file = NULL;
static int shutdown_cancel_ready_fd = -1;

typedef struct WorkerArgs
{
	uint32_t	shard;
	void	   *shm;
	uint32_t	nchannels;
	uint32_t	nshards;
} WorkerArgs;

typedef struct BackpressureWaitSlot
{
	uint64_t	request_generation;
	uint32_t	cause_mask;
	uint64_t	last_probe_ns;
	uint64_t	page_wait_ns;
	uint64_t	wal_wait_ns;
	int		active;
} BackpressureWaitSlot;

/* A channel belongs to exactly one shard worker, so these slots need no
 * additional synchronization.  Keeping them per channel avoids charging a
 * later request for time spent by an earlier deferred request. */
static BackpressureWaitSlot backpressure_wait_slots[PS_MAX_CHANNELS];

static uint64_t
monotonic_ns(void)
{
	struct timespec now;
	uint64_t sec;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0)
		return 0;
	sec = (uint64_t) now.tv_sec;
	if (sec > UINT64_MAX / UINT64_C(1000000000))
		return UINT64_MAX;
	sec *= UINT64_C(1000000000);
	if ((uint64_t) now.tv_nsec > UINT64_MAX - sec)
		return UINT64_MAX;
	return sec + (uint64_t) now.tv_nsec;
}

static void
add_wait_ns(uint64_t *total, uint64_t elapsed)
{
	*total = UINT64_MAX - *total < elapsed ? UINT64_MAX : *total + elapsed;
}

static void
accrue_backpressure_interval(BackpressureWaitSlot *slot, uint64_t now)
{
	uint64_t elapsed;

	if (!slot->active || now < slot->last_probe_ns)
		return;
	elapsed = now - slot->last_probe_ns;
	if ((slot->cause_mask & PS_BACKPRESSURE_PAGE) != 0)
		add_wait_ns(&slot->page_wait_ns, elapsed);
	if ((slot->cause_mask & PS_BACKPRESSURE_WAL) != 0)
		add_wait_ns(&slot->wal_wait_ns, elapsed);
}

static void
capture_backpressure_request(BackpressureWaitSlot *slot,
							 const PsChannel *ch)
{
	slot->request_generation =
		__atomic_load_n(&ch->request_generation, __ATOMIC_ACQUIRE);
}

static int
backpressure_request_matches(const BackpressureWaitSlot *slot,
							 const PsChannel *ch)
{
	return slot->request_generation ==
		__atomic_load_n(&ch->request_generation, __ATOMIC_ACQUIRE);
}

static void finish_backpressure_wait(uint32_t channel);

static void
record_backpressure_probe(uint32_t channel, PsChannel *ch,
						  uint32_t cause_mask)
{
	BackpressureWaitSlot *slot = &backpressure_wait_slots[channel];
	uint64_t now = monotonic_ns();

	if (slot->active)
	{
		/* A channel is not normally mutable while it is REQUEST, but if a
		 * client abandoned and replaced it, close the old accounting interval
		 * before tracking the new request. */
		if (!backpressure_request_matches(slot, ch))
		{
			finish_backpressure_wait(channel);
			capture_backpressure_request(slot, ch);
			slot->active = 1;
		}
		accrue_backpressure_interval(slot, now);
	}
	else
	{
		capture_backpressure_request(slot, ch);
		slot->active = 1;
	}
	slot->cause_mask = cause_mask;
	slot->last_probe_ns = now;
}

static void
finish_backpressure_wait(uint32_t channel)
{
	BackpressureWaitSlot *slot = &backpressure_wait_slots[channel];
	uint64_t now;

	if (!slot->active)
		return;
	now = monotonic_ns();
	accrue_backpressure_interval(slot, now);
	ps_backpressure_record_wait(slot->page_wait_ns, slot->wal_wait_ns);
	memset(slot, 0, sizeof(*slot));
}

static void
on_signal(int sig)
{
	(void) sig;
	if (daemon_hdr != NULL)
	{
		/* Invalidate readiness before recovery/flush shutdown can block. */
		__atomic_store_n(&daemon_hdr->startup_state, PS_SHM_STOPPING,
						 __ATOMIC_RELEASE);
		__atomic_store_n(&daemon_hdr->magic, 0, __ATOMIC_RELEASE);
	}
	stop_requested = 1;
}

static void
shm_mark_starting(PsShmHeader *hdr)
{
	/* Magic is the compatibility gate used by older inspectors as well. */
	__atomic_store_n(&hdr->magic, 0, __ATOMIC_RELEASE);
	__atomic_store_n(&hdr->startup_state, PS_SHM_STARTING, __ATOMIC_RELEASE);
}

static void
shm_mark_stopping(PsShmHeader *hdr)
{
	__atomic_store_n(&hdr->startup_state, PS_SHM_STOPPING, __ATOMIC_RELEASE);
	__atomic_store_n(&hdr->magic, 0, __ATOMIC_RELEASE);
}

#define SHUTDOWN_CANCEL_READY_BYTE 0x5a

/* Test-only pipe publication point.  The descriptor is inherited across
 * exec, so the byte cannot be confused with a stale filesystem artifact. */
static int
publish_shutdown_cancel_ready(void)
{
	unsigned char byte = SHUTDOWN_CANCEL_READY_BYTE;
	int fd = shutdown_cancel_ready_fd;

	shutdown_cancel_ready_fd = -1;
	if (fd < 0)
		return 0;
	for (;;)
	{
		ssize_t n = write(fd, &byte, 1);

		if (n < 0 && errno == EINTR)
			continue;
		if (n != 1)
		{
			(void) close(fd);
			return -1;
		}
		break;
	}
	/* The byte is the publication point.  Closing the inherited descriptor is
	 * cleanup only; a close error must not make the already-published handshake
	 * look like a failed publication or skip the test pause. */
	(void) close(fd);
	return 0;
}

static int
shm_publish_ready(PsShmHeader *hdr)
{
	uint32_t	expected = PS_SHM_STARTING;

	/* Publish state first, then magic last after every header field is final. */
	if (!__atomic_compare_exchange_n(&hdr->startup_state, &expected,
									 PS_SHM_READY, 0, __ATOMIC_ACQ_REL,
									 __ATOMIC_ACQUIRE))
		return 0;
	__atomic_store_n(&hdr->magic, PS_SHM_MAGIC, __ATOMIC_RELEASE);
	return 1;
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
	if ((ch->opcode == PS_OP_EXTEND || ch->opcode == PS_OP_WRITEV ||
		 ch->opcode == PS_OP_READV || ch->opcode == PS_OP_READ_AT) &&
		!ps_timeline_request_allowed(tl, ch->incarnation))
	{
		ch->status = PS_STATUS_ERROR;
		return;
	}

	if (ps_handle_meta(ch))
		return;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_EXTEND:
			/* append_page grows the fork with the page's exact LSN */
			if (append_page(tl, &ch->key, ch->blocknum, ch->data,
							ch->req_lsn, &ch->req_seq) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WRITEV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + i,
								ch->data + (size_t) i * page_size,
								 ch->req_lsn, &ch->req_seq) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
			break;

		case PS_OP_READV:
			/* "current" read on this timeline: resolve at max LSN, serving from
			 * memtable / image layers with a segment fallback.  A pinned
			 * reader (pagestore.read_lsn) stamps req_lsn to cap the resolve
			 * at its horizon; 0 keeps the newest semantics, so writer
			 * computes are untouched.  Blocks with no version at/below the
			 * cap read as zeros: they were created after the horizon, and
			 * an as-of traversal never reaches them (PageIsNew for heap;
			 * index pointers as-of the horizon only reference as-of
			 * structure). */
			{
				uint64_t	rl = ch->req_lsn ? ch->req_lsn : UINT64_MAX;

				for (uint32_t i = 0; i < ch->nblocks; i++)
				{
					unsigned char *dst = ch->data + (size_t) i * page_size;
					uint64_t	resolved = 0;
					int			read_result;

					read_result = read_resolve(tl, &ch->key, ch->blocknum + i, rl,
												   ch->req_seq, dst, &resolved);
					if (read_result < 0)
					{
						ch->status = PS_STATUS_ERROR;
						break;
					}
					if (read_result == 0)
						memset(dst, 0, page_size);	/* unwritten -> zeros */
					else if (ch->req_seq != 0 && resolved == 0)
					{
						/*
						 * A stored version with LSN 0 is WAL-less content
						 * (an unlogged relation, or a skip-WAL build): it
						 * is not LSN-ordered, so a checkpoint read with
						 * a durable admission cap cannot
						 * honestly serve it "as of" anything.  Fail closed rather
						 * than hand a pinned reader whatever bytes the
						 * writer most recently flushed.
						 */
						ch->status = PS_STATUS_ERROR;
						break;
					}
				}
			}
			break;

		case PS_OP_READ_AT:
			/* as-of read on this timeline, honoring branch ancestry.  Report
			 * found-ness in ch->result, and the resolved version back in ch->req_lsn,
			 * so a caller can tell a real all-zero page from one that has no version
			 * <= req_lsn, and an SLRU snapshot reader can require an exact-cutoff hit
			 * (resolved == requested) rather than an older newest-<= image. */
			{
				uint64_t	read_lsn = ch->req_lsn;
				uint64_t	resolved = 0;
				int			read_result;

				read_result = read_resolve(tl, &ch->key, ch->blocknum, read_lsn,
												  ch->req_seq, ch->data, &resolved);
				/* READ_AT is a diagnostic found-ness probe: reclaimed history is
				 * reported as absent.  Capped READV above fails closed instead. */
				if (read_result == -2)
				{
					memset(ch->data, 0, page_size);
					ch->result = 0;
				}
				else if (read_result < 0)
					ch->status = PS_STATUS_ERROR;
				else if (read_result > 0)
				{
					if (read_lsn != UINT64_MAX && resolved == 0)
					{
						memset(ch->data, 0, page_size);
						ch->status = PS_STATUS_ERROR;
					}
					else
					{
						ch->result = 1;
						ch->req_lsn = resolved;
					}
				}
				else
					memset(ch->data, 0, page_size);	/* not found: result stays 0 */
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
		case PS_OP_BEGIN_DELETE:
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
		case PS_OP_REQUIRE_BRANCH:
		case PS_OP_WAL_SIZE:
		case PS_OP_WAL_READ:
		case PS_OP_WAL_INDEX_GET:
		case PS_OP_WAL_RETAIN_FLOOR:
		case PS_OP_RETENTION_PIN_GET:
		case PS_OP_RETENTION_PIN_LOOKUP:
		case PS_OP_RETENTION_PIN_RESERVE:
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

/* Controller gating covers only operations which are known to mutate state.
 * Keep this separate from request_is_write(): that function intentionally
 * defaults unknown opcodes to the existing write/admission behavior, while a
 * controller must never accidentally throttle a new or metadata-read opcode.
 * Retention pin state changes intentionally remain outside request_is_write()'s
 * admission-rd behavior. */
static int
request_is_mutation(const PsChannel *ch)
{
	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE:
		case PS_OP_UNLINK:
		case PS_OP_TRUNCATE:
		case PS_OP_ZEROEXTEND:
		case PS_OP_CREATE_BRANCH:
		case PS_OP_BEGIN_DELETE:
		case PS_OP_EXTEND:
		case PS_OP_WRITEV:
		case PS_OP_WAL_APPEND:
		case PS_OP_WAL_INDEX_ADD:
		case PS_OP_WAL_INDEX_ADD_BATCH:
		case PS_OP_IMMEDSYNC:
		case PS_OP_RETENTION_PIN_RESERVE:
		case PS_OP_RETENTION_PIN_SET:
		case PS_OP_RETENTION_PIN_DROP:
			return 1;
		case PS_OP_WAL_INDEX_PROGRESS:
			/* 0/0 is the read-current-progress form. */
			return ch->req_lsn != 0 || ch->req_seq != 0;
		default:
			return 0;
	}
}

static void
run_request_admitted(PsChannel *ch)
{
	PsOpcode	op = (PsOpcode) ch->opcode;

	/*
	 * Branch creation normally mutates only timelines[].  Reuse additionally
	 * purges every per-shard incarnation-local index/cache, so exclude all
	 * shard workers before taking map_lock (the established shard->map order).
	 */
	if (op == PS_OP_RETENTION_PIN_LOOKUP || op == PS_OP_RETENTION_PIN_RESERVE ||
		op == PS_OP_RETENTION_PIN_SET || op == PS_OP_RETENTION_PIN_DROP)
	{
		int		timeline_ok;

		/*
		 * Pin operations own the retention mutex, and mutations fsync their log.
		 * Timeline definitions are append-only for a daemon lifetime, so validate
		 * the timeline under the map lock and release it before the core takes any
		 * admission/page/WAL-index locks for the durable mutation.
		 */
		ps_lock_map_rd();
		timeline_ok = ps_timeline_defined(ch->timeline);
		ps_unlock_map();
		if (timeline_ok)
			handle_request(ch);
		else
			ch->status = PS_STATUS_ERROR;
		ps_store_release(&ch->state, PS_STATE_DONE);
		return;
	}

	if (op == PS_OP_CREATE_BRANCH)
	{
		for (uint32_t s = 0; s < ps_nshards; s++)
			ps_lock_shard_wr(s);
		ps_lock_map_wr();
		handle_request(ch);
		ps_store_release(&ch->state, PS_STATE_DONE);
		ps_unlock_map();
		for (uint32_t s = ps_nshards; s-- > 0;)
			ps_unlock_shard(s);
		return;
	}

	if (op == PS_OP_CHECK_BRANCH || op == PS_OP_REQUIRE_BRANCH)
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
		if (op == PS_OP_WAL_APPEND || op == PS_OP_WAL_SIZE || op == PS_OP_WAL_READ ||
			op == PS_OP_WAL_INDEX_PROGRESS)
			shard = 0;
		else if (op == PS_OP_WAL_RETAIN_FLOOR ||
				 op == PS_OP_RETENTION_FLOOR)
		{
			/*
			 * The floor query always scans the fixed control object; derive
			 * the shard from that key rather than trusting the caller to
			 * have pre-filled ch->key (a freestanding client may not).
			 */
			PsKey		ctlkey;

			memset(&ctlkey, 0, sizeof(ctlkey));
			ctlkey.klass = PS_KLASS_CONTROL;
			shard = ps_shard_of(&ctlkey);
		}
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
			/* READV/READ_AT snapshot ancestry internally before potentially blocking
			 * remote-layer I/O.  Other timeline readers complete under map_rd. */
			ps_lock_shard_rd(shard);
			if (op == PS_OP_READV || op == PS_OP_READ_AT ||
				op == PS_OP_WAL_RETAIN_FLOOR || op == PS_OP_RETENTION_FLOOR)
				handle_request(ch);
			else
			{
				ps_lock_map_rd();
				handle_request(ch);
				ps_unlock_map();
			}
			ps_store_release(&ch->state, PS_STATE_DONE);
			ps_unlock_shard(shard);
		}
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

/* Returns zero when a relation mutation is intentionally left in REQUEST
 * until the checkpoint owner publishes and syncs its admission fence. */
static int
run_request(uint32_t channel, PsChannel *ch)
{
	PsOpcode	op = (PsOpcode) ch->opcode;

	/* Backpressure is checked before any lifecycle/admission/shard/map lock.
	 * A throttled mutation remains in REQUEST so this worker can scan the next
	 * channel; reads and maintenance never enter controller gating. */
	if (request_is_mutation(ch))
	{
		uint32_t causes = 0;
		int admit = ps_backpressure_try_admit(&stop_requested, &causes);

		if (admit < 0)
		{
			finish_backpressure_wait(channel);
			ch->status = PS_STATUS_ERROR;
			ps_store_release(&ch->state, PS_STATE_DONE);
			return 1;
		}
		if (admit == 0)
		{
			record_backpressure_probe(channel, ch, causes);
			return 0;
		}
		/* Charge the complete final interval, including the probe which first
		 * observed the controller catch-up transition. */
		finish_backpressure_wait(channel);
	}

	if (op == PS_OP_BEGIN_DELETE)
	{
		/* lifecycle-wr drains every complete POSIX request, including ordinary
		 * reads and reserve operations, before admission/map state changes. */
		if (ps_lifecycle_write_lock_interruptible(&stop_requested) != 0)
			ch->status = PS_STATUS_ERROR;
		else if (ps_admission_write_lock() == 0)
		{
			ps_lock_map_wr();
			handle_request(ch);
			ps_unlock_map();
			ps_admission_write_unlock();
			ps_lifecycle_write_unlock();
		}
		else
		{
			ch->status = PS_STATUS_ERROR;
			ps_lifecycle_write_unlock();
		}
		ps_store_release(&ch->state, PS_STATE_DONE);
		return 1;
	}

	if (op == PS_OP_ADMISSION_BARRIER)
	{
		/* The barrier is an ordinary complete request: lifecycle-rd is held
		 * while its inner admission-wr establishes the sequence.  BEGIN_DELETE
		 * is the sole lifecycle-wr exception. */
		ps_lifecycle_read_lock();
		ch->status = PS_STATUS_OK;
		ch->result = 0;
		ch->req_seq = ps_admission_barrier();
		if (ch->req_seq == 0)
			ch->status = PS_STATUS_ERROR;
		ps_store_release(&ch->state, PS_STATE_DONE);
		ps_lifecycle_read_unlock();
		return 1;
	}

	/* Keep the lifecycle read side across every retry and every request
	 * completion.  The only early return below releases both gates first. */
	ps_lifecycle_read_lock();
	if (op == PS_OP_RETENTION_PIN_RESERVE)
	{
		run_request_admitted(ch);
		ps_lifecycle_read_unlock();
		return 1;
	}
	if (!request_is_write(op))
	{
		run_request_admitted(ch);
		ps_lifecycle_read_unlock();
		return 1;
	}

	for (;;)
	{
		uint64_t	epoch = active_fence_epoch();

		ps_admission_read_lock();
		if (epoch != active_fence_epoch())
		{
			ps_admission_read_unlock();
			continue;
		}
		if (epoch != 0 && relation_request_lsn(ch) <=
			ps_load_acquire_u64(&daemon_hdr->admission_pending_lsn))
		{
			ps_admission_read_unlock();
			ps_lifecycle_read_unlock();
			return 0;
		}
		run_request_admitted(ch);
		ps_admission_read_unlock();
		ps_lifecycle_read_unlock();
		return 1;
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

			if (run_request(i, ch))
				did_work = 1;
		}

		if (!did_work)
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}

	return NULL;
}

/* All request workers are joined before this pass, so no daemon thread can be
 * executing or advancing a channel while it runs.  REQUEST -> CANCELLING is
 * the daemon-owned handoff: a client cannot validly abandon/reclaim and publish
 * a new generation after this CAS.  The generation recheck handles the small
 * probe/CAS race by restoring REQUEST without touching the newer request. */
static void
cancel_deferred_channels(void *shm, uint32_t nchannels)
{
	for (uint32_t i = 0; i < nchannels; i++)
	{
		BackpressureWaitSlot *slot = &backpressure_wait_slots[i];
		PsChannel *ch;
		uint64_t generation;

		if (!slot->active)
			continue;
		ch = ps_channel(shm, i);
		generation = slot->request_generation;
		if (ps_load_acquire(&ch->claimed) == 0 ||
			!backpressure_request_matches(slot, ch) ||
			!ps_cas(&ch->state, PS_STATE_REQUEST, PS_STATE_CANCELLING))
		{
			finish_backpressure_wait(i);
			continue;
		}
		/* The state CAS is the ownership transfer.  A valid client publishes
		 * generation before REQUEST and cannot mutate it while CANCELLING. */
		if (__atomic_load_n(&ch->request_generation, __ATOMIC_ACQUIRE) !=
			generation)
		{
			finish_backpressure_wait(i);
			/* The CAS may have raced a client that changed REQUEST between
			 * the cheap generation probe and the ownership transfer.  Do not
			 * write status or publish DONE for that newer generation; return
			 * the state only if we still own CANCELLING. */
			(void) ps_cas(&ch->state, PS_STATE_CANCELLING,
						  PS_STATE_REQUEST);
			continue;
		}
		finish_backpressure_wait(i);
		ch->status = PS_STATUS_ERROR;
		ps_store_release(&ch->state, PS_STATE_DONE);
	}
}

/*
 * Keep potentially long tiering, GC, and compaction work off every shard's
 * foreground serve thread.  ps_core_maintenance() takes the shard and map
 * locks needed by each job; once compaction is due, that lock contention is
 * the bounded backpressure seen by writers instead of making an arbitrary
 * write request execute the merge itself.
 */
static void *
maintenance_worker(void *arg)
{
	(void) arg;

	while (!stop_requested)
	{
		/* Test-only deterministic seam: keep the real maintenance thread alive
		 * and refreshing controller state, while withholding reclaim progress. */
		if (maintenance_pause_file != NULL &&
			access(maintenance_pause_file, F_OK) == 0)
		{
			ps_backpressure_refresh();
			{
				struct timespec ts = {0, 1000000};

				nanosleep(&ts, NULL);
			}
			continue;
		}
		if (!ps_core_maintenance())
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}

	return NULL;
}

static int
parse_u64_option(const char *text, uint64_t *value)
{
	char *end = NULL;
	unsigned long long parsed;

	if (text == NULL || *text == '\0' || text[0] == '-')
		return -1;
	errno = 0;
	parsed = strtoull(text, &end, 10);
	if (errno == ERANGE || end == text || *end != '\0')
		return -1;
	*value = (uint64_t) parsed;
	return 0;
}

static int
parse_fd_option(const char *text, int *value)
{
	uint64_t parsed;

	if (parse_u64_option(text, &parsed) != 0 || parsed > INT_MAX)
		return -1;
	*value = (int) parsed;
	return 0;
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
		else if (strcmp(argv[i], "--segment-gc") == 0 && i + 1 < argc)
			segment_gc_enabled = atoi(argv[++i]) != 0;
		else if (strcmp(argv[i], "--nshards") == 0 && i + 1 < argc)
			nshards = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--cache-pages") == 0 && i + 1 < argc)
			cache_pages = atoi(argv[++i]);
		else if (strcmp(argv[i], "--page-high-water-bytes") == 0 && i + 1 < argc)
		{
			if (parse_u64_option(argv[++i], &page_reclaim_high_water_bytes) != 0)
				return 2;
		}
		else if (strcmp(argv[i], "--page-catch-up-bytes") == 0 && i + 1 < argc)
		{
			if (parse_u64_option(argv[++i], &page_reclaim_catchup_bytes) != 0)
				return 2;
		}
		else if (strcmp(argv[i], "--wal-high-water-bytes") == 0 && i + 1 < argc)
		{
			if (parse_u64_option(argv[++i], &wal_reclaim_high_water_bytes) != 0)
				return 2;
		}
		else if (strcmp(argv[i], "--wal-catch-up-bytes") == 0 && i + 1 < argc)
		{
			if (parse_u64_option(argv[++i], &wal_reclaim_catchup_bytes) != 0)
				return 2;
		}
		else if (strcmp(argv[i], "--test-maintenance-pause-file") == 0 &&
				 i + 1 < argc)
			maintenance_pause_file = argv[++i];
		else if (strcmp(argv[i], "--test-shutdown-cancel-pause-file") == 0 &&
				 i + 1 < argc)
			shutdown_cancel_pause_file = argv[++i];
		else if (strcmp(argv[i], "--test-shutdown-cancel-ready-fd") == 0 &&
				 i + 1 < argc)
		{
			if (parse_fd_option(argv[++i], &shutdown_cancel_ready_fd) != 0)
				return 2;
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
					"[--page-size N] [--segment-size N] [--segment-gc 0|1] "
					"[--nshards N] [--storage NAME] "
					"[--page-high-water-bytes N --page-catch-up-bytes N] "
					"[--wal-high-water-bytes N --wal-catch-up-bytes N] "
					"[--test-maintenance-pause-file PATH] "
					"[--test-shutdown-cancel-pause-file PATH] "
					"[--test-shutdown-cancel-ready-fd N]\n",
					argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT ||
		nshards == 0 || nshards > PS_MAX_CHANNELS)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR "
				"[--page-size N] [--segment-size N] [--segment-gc 0|1] "
				"[--nshards N] [--storage NAME] "
				"[--page-high-water-bytes N --page-catch-up-bytes N] "
				"[--wal-high-water-bytes N --wal-catch-up-bytes N] "
				"[--test-maintenance-pause-file PATH] "
				"[--test-shutdown-cancel-pause-file PATH] "
				"[--test-shutdown-cancel-ready-fd N]\n",
				argv[0]);
		return 2;
	}
	if (ps_backpressure_configure(page_reclaim_high_water_bytes,
								  page_reclaim_catchup_bytes,
								  wal_reclaim_high_water_bytes,
								  wal_reclaim_catchup_bytes) != 0)
	{
		if (page_reclaim_high_water_bytes != 0 && !segment_gc_enabled)
			fprintf(stderr, "pagestore_daemon: page backpressure requires "
					"--segment-gc 1 when page high-water is enabled\n");
		else
			fprintf(stderr, "pagestore_daemon: catch-up threshold must be less "
					"than the enabled high-water threshold\n");
		return 2;
	}
	ps_nshards = nshards;
	/* Validate explicit test fault configuration before opening any store
	 * state.  A malformed configuration must never silently disable a probe. */
	if (ps_fault_init(store_dir) != 0)
	{
		fprintf(stderr, "pagestore_daemon: invalid fault configuration\n");
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
	daemon_hdr = hdr;
	/* Invalidate a previous daemon's header before store recovery begins. */
	shm_mark_starting(hdr);

	if (ps_core_open(store_dir) != 0)
	{
		perror("storage open");
		munmap(shm, PS_SHM_SIZE);
		return 1;
	}

	/*
	 * Initialize the shared region.  NB: this zeroes the whole segment, so the
	 * daemon must be (re)started while no engine is attached -- restarting it
	 * against a live shm would wipe in-flight channel state.  A production
	 * version would attach without re-initializing when the header is already
	 * valid.
	 */
	memset(shm, 0, PS_SHM_SIZE);
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = page_size;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->nshards = nshards;
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;
	daemon_hdr = hdr;
	ps_core_set_metrics_header(hdr);

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	{
		WorkerArgs *workers = malloc((size_t) hdr->nshards * sizeof(WorkerArgs));
		pthread_t  *threads = malloc((size_t) hdr->nshards * sizeof(pthread_t));
		pthread_t	maintenance;
		uint32_t	started = 0;
		int			maintenance_started = 0;

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
		if (started == hdr->nshards)
		{
			if (pthread_create(&maintenance, NULL, maintenance_worker, NULL) != 0)
			{
				fprintf(stderr, "pagestore_daemon: failed to start maintenance worker\n");
				stop_requested = 1;
			}
			else
				maintenance_started = 1;
		}
		if (started == hdr->nshards && maintenance_started && !stop_requested &&
			shm_publish_ready(hdr))
		{
			fprintf(stderr, "pagestore_daemon: shm=%s store=%s storage=%s page_size=%u "
					"io_unit=%u channels=%u nshards=%u ready\n",
					shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
					PS_MAX_CHANNELS, hdr->nshards);
			if (ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY) != 0)
			{
				fprintf(stderr, "pagestore_daemon: fault probe daemon.after_ready failed\n");
				shm_mark_stopping(hdr);
				ps_core_close();
				munmap(shm, PS_SHM_SIZE);
				return 1;
			}
		}

		for (uint32_t shard = 0; shard < started; shard++)
			pthread_join(threads[shard], NULL);
		if (maintenance_started)
			pthread_join(maintenance, NULL);

		free(workers);
		free(threads);
	}
	/* The signal handler invalidates readiness immediately.  Once every daemon
	 * worker has stopped, finish the requests that were deliberately retained
	 * in REQUEST by backpressure before core/shm teardown. */
	shm_mark_stopping(hdr);
	/* Test-only seam: workers are already joined, so a client can deterministically
	 * abandon/reuse a deferred channel before the generation-checked cancellation
	 * pass.  This is never configured by normal daemon users. */
	if (shutdown_cancel_ready_fd >= 0 &&
		publish_shutdown_cancel_ready() != 0)
		fprintf(stderr, "pagestore_daemon: could not publish shutdown cancellation byte; "
				"skipping test pause\n");
	else
	{
		while (shutdown_cancel_pause_file != NULL &&
			   access(shutdown_cancel_pause_file, F_OK) == 0)
		{
			struct timespec ts = {0, 1000000};

			nanosleep(&ts, NULL);
		}
	}
	cancel_deferred_channels(shm, hdr->nchannels);

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
