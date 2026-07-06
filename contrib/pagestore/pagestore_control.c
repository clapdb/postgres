/*-------------------------------------------------------------------------
 *
 * pagestore_control.c
 *	  Mirror pg_control to the page store (PGCONTROL_ON_STORE_DESIGN.md).
 *
 * Consumes the core control_file_write_hook / control_file_flush_hook seams:
 * every UpdateControlFile() hands us the just-written ControlFileData image
 * plus the LSN of the update that caused it, and we mirror it to the store as
 * a PS_KLASS_CONTROL object versioned by that LSN, so a branch cut at LSN L
 * can later restore the control image "as of L".
 *
 * The write hook can run inside a checkpoint/shutdown critical section, where
 * store I/O must never run: a daemon error there would PANIC the checkpoint.
 * So the hook only records intent into a small pre-reserved in-process queue
 * (allocation-free, never blocks, never errors), and the actual obj_write of
 * each queued image runs at the post-critical ship points -- the flush hook
 * that core invokes right after the critical sections that write pg_control,
 * or the next control write that happens outside one.  The queue is ordered
 * and keeps every image (not a single replaceable slot): two control updates
 * can occur before a drain, and a branch point between their LSNs must
 * restore the earlier image, not the coalesced latest.
 *
 * Two images CAN legitimately carry the same update LSN (end-of-recovery and
 * a promotion that inserted no WAL in between).  The store keeps both, and
 * as-of reads resolve equal versions latest-append-wins: for any branch cut
 * L both images are valid at L (their WAL requirement is identical), and the
 * later write is the later state at that same position -- exactly what a
 * compute booted at L converges to -- so serving it is correct, not a loss.
 *
 * The mirror is an ordered follower of the local file: local pg_control is
 * always written (and fsync'd) first by UpdateControlFile(); the store image
 * follows at the next ship point.  The daemon's WAL retention horizon for the
 * mirrored image (never GC WAL under the store control image's redo pointer)
 * and the bootstrap restore tool are follow-up increments; see the design's
 * sequencing.
 *
 * src/../contrib/pagestore/pagestore_control.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "pagestore_backend.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "varatt.h"

/*
 * Queue capacity.  In healthy operation, control writes between two ship
 * points are a small bounded set (a checkpoint's state flip + completion
 * write, or an end-of-recovery update followed by a promotion update), and
 * every hook call outside a critical section drains first.  The capacity is
 * sized far beyond that -- 64 -- for the MIRROR-OUTAGE case: while the
 * daemon is unavailable every drain fails and images accumulate, and each
 * dropped image is the only version for branch cuts in its LSN interval.
 * Sixty-four slots ride out an outage spanning dozens of checkpoints; past
 * that, overflow drops the OLDEST image (the newest state must survive) and
 * is counted + reported at the next drain, so it cannot pass silently.
 */
#define PS_CONTROL_QUEUE_CAPACITY	64

/* bounded wait per mailbox op while shipping; a wedged daemon must not hang
 * the checkpointer/startup at a post-critical ship point */
#define PS_CONTROL_SHIP_TIMEOUT_MS	10000

/*
 * Wall-clock budget for one drain as a whole.  Each mailbox op is bounded by
 * the ship timeout, but a deep backlog shipped through a slow-but-responsive
 * daemon could otherwise hold a ship point (checkpointer, startup) for
 * queue-depth * ops * timeout.  When the budget runs out the drain syncs and
 * pops what it shipped and leaves the rest for the next ship point.
 */
#define PS_CONTROL_DRAIN_BUDGET_MS	30000

typedef struct PsControlPending
{
	ControlFileData image;
	XLogRecPtr	update_lsn;
	bool		posted;			/* a drain has posted this image at least once */
} PsControlPending;

static PsControlPending ps_control_queue[PS_CONTROL_QUEUE_CAPACITY];
static int	ps_control_queue_head = 0;	/* oldest entry */
static int	ps_control_queue_count = 0;
static uint64 ps_control_dropped = 0;

static control_file_write_hook_type prev_control_file_write_hook = NULL;
static control_file_flush_hook_type prev_control_file_flush_hook = NULL;

/* set once at hook install; mirroring requires the localsvc backend */
static bool ps_control_mirror_enabled = false;

/*
 * Exit-drain registration is PER PROCESS: pagestore loads via
 * shared_preload_libraries, so anything registered in the postmaster is
 * wiped in forked children by InitPostmasterChild()'s on_exit_reset().
 * Register lazily from the first drain (always outside critical sections,
 * where before_shmem_exit may ereport).
 */
static bool ps_control_exit_registered = false;

static void ps_control_exit_drain(int code, Datum arg);

/*
 * Ship every queued control image to the store, in order.  Must only run
 * outside critical sections: obj_write can ERROR (daemon down, no channel),
 * which is recoverable here but would PANIC a critical section.
 */
static void
ps_control_drain(void)
{
	bool		shipped = false;
	MemoryContext drain_cxt = CurrentMemoryContext;

	Assert(CritSectionCount == 0);

	if (!ps_control_exit_registered)
	{
		before_shmem_exit(ps_control_exit_drain, (Datum) 0);
		ps_control_exit_registered = true;
	}

	if (ps_control_dropped > 0)
	{
		ereport(WARNING,
				(errmsg("pagestore: dropped %llu queued pg_control mirror image(s)",
						(unsigned long long) ps_control_dropped),
				 errdetail("More control-file writes occurred inside one critical section than the mirror queue holds.")));
		ps_control_dropped = 0;
	}

	/*
	 * Ship + sync + pop, in that order, and never let a store failure
	 * escape: images are consumed from the queue only after the daemon has
	 * SYNCED them (local pg_control is already fsync'd, so an image popped
	 * on mere pwrite acceptance would be lost to a host crash before the
	 * next sync -- and a branch restore would miss control state the local
	 * file advertises).  On any failure -- including a wedged daemon, which
	 * the bounded waits turn into an error -- everything unpopped stays
	 * queued for the next ship point and the caller continues: a mirror
	 * outage must not abort checkpoints, recovery steps, or the checksum
	 * state machines that host the ship points.  Re-shipping an image the
	 * daemon already accepted appends a same-version duplicate, which is
	 * latest-wins and invalidates the page cache, so retry is idempotent.
	 */
	PG_TRY();
	{
		int			nqueued = ps_control_queue_count;
		int			ndone = 0;
		TimestampTz drain_start = GetCurrentTimestamp();
		int			i;

		for (i = 0; i < nqueued; i++)
		{
			PsControlPending *p = &ps_control_queue[(ps_control_queue_head + i)
													% PS_CONTROL_QUEUE_CAPACITY];
			PageStoreRelKey key = {0};
			char		page[BLCKSZ];
			BlockNumber nb;

			/*
			 * Bound the drain as a whole, not just each mailbox op: a deep
			 * backlog shipped through a daemon that responds just under the
			 * per-op timeout could otherwise hold this ship point for
			 * queue-depth * ops * timeout.  Always attempt the first image
			 * so every drain makes progress; the rest stays queued for the
			 * next ship point.
			 */
			if (i > 0 &&
				TimestampDifferenceExceeds(drain_start, GetCurrentTimestamp(),
										   PS_CONTROL_DRAIN_BUDGET_MS))
			{
				ereport(WARNING,
						(errmsg("pagestore: control mirror drain ran out of time, deferring %d queued image(s) to the next ship point",
								nqueued - i)));
				break;
			}

			/*
			 * An image without a valid update LSN cannot be restored "as of"
			 * a branch point and would shadow every as-of read at version 0;
			 * no call site passes one today, so treat it as a bug, not data.
			 */
			if (XLogRecPtrIsInvalid(p->update_lsn))
			{
				ereport(WARNING,
						(errmsg("pagestore: skipping pg_control mirror image with no update LSN")));
				ndone++;
				continue;
			}

			elog(DEBUG1, "pagestore: shipping pg_control mirror image, update_lsn=%X/%08X state=%d",
				 LSN_FORMAT_ARGS(p->update_lsn), (int) p->image.state);

			/*
			 * The preliminary CREATE and NBLOCKS carry no image bytes, so a
			 * timeout there cannot result in this image being applied late;
			 * the slot stays rewritable by a later same-LSN update.  Mark
			 * posted only right before the first WRITE puts slot-derived
			 * bytes in flight (the daemon can complete a timed-out WRITE
			 * late) -- from then on the slot's bytes must never change
			 * again; see the dedup path.  The floor note below is derived
			 * from the slot too, so it is inside the frozen window.
			 */
			nb = pagestore_localsvc_obj_write_prepare_timeout(PS_KLASS_CONTROL,
															  &key,
															  PS_CONTROL_SHIP_TIMEOUT_MS);
			p->posted = true;

			/*
			 * Ship the retention "floor note" first: block 1 of the control
			 * object carries this image's checkpoint redo pointer at the
			 * same version LSN.  The daemon derives its durable WAL
			 * retention floor from these notes (wal_retain_floor); writing
			 * the note before the image keeps the invariant conservative --
			 * every restorable image has a note, and a crash between the
			 * two leaves only a note, which merely retains WAL a little
			 * longer.
			 */
			memset(page, 0, sizeof(page));
			memcpy(page, &p->image.checkPointCopy.redo, sizeof(XLogRecPtr));
			pagestore_localsvc_obj_write_post_timeout(PS_KLASS_CONTROL, &key,
													  1, page,
													  (uint64) p->update_lsn,
													  nb,
													  PS_CONTROL_SHIP_TIMEOUT_MS);

			/*
			 * The object image is the on-disk pg_control representation:
			 * the ControlFileData bytes (CRC already computed by
			 * update_controlfile) zero-padded -- to PG_CONTROL_FILE_SIZE on
			 * disk, to BLCKSZ here.  The restore tool writes back exactly
			 * the first PG_CONTROL_FILE_SIZE bytes.  Re-fetch the block
			 * count: the note write above may have extended the object.
			 */
			memset(page, 0, sizeof(page));
			memcpy(page, &p->image, sizeof(ControlFileData));

			nb = pagestore_localsvc_obj_write_prepare_timeout(PS_KLASS_CONTROL,
															  &key,
															  PS_CONTROL_SHIP_TIMEOUT_MS);
			pagestore_localsvc_obj_write_post_timeout(PS_KLASS_CONTROL, &key,
													  0, page,
													  (uint64) p->update_lsn,
													  nb,
													  PS_CONTROL_SHIP_TIMEOUT_MS);
			shipped = true;
			ndone++;
		}

		/*
		 * Durability contract: the mirrored images only count once the
		 * daemon has synced them; pop only after that succeeded, and only
		 * the entries this drain actually processed.
		 */
		if (shipped)
			pagestore_localsvc_store_sync_timeout(PS_CONTROL_SHIP_TIMEOUT_MS);

		ps_control_queue_head = (ps_control_queue_head + ndone)
			% PS_CONTROL_QUEUE_CAPACITY;
		ps_control_queue_count -= ndone;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext ecxt = MemoryContextSwitchTo(drain_cxt);

		edata = CopyErrorData();
		MemoryContextSwitchTo(ecxt);

		/*
		 * A user cancellation or backend termination raised from inside the
		 * mailbox wait (CHECK_FOR_INTERRUPTS in ls_exec) is not a mirror
		 * failure: swallowing it would make CHECKPOINT and the checksum
		 * state machines ignore interrupts.  Let those propagate; the queue
		 * was not advanced, so nothing is lost.
		 */
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		{
			FreeErrorData(edata);
			PG_RE_THROW();
		}
		FreeErrorData(edata);

		/*
		 * Swallow store failures: the queue was not advanced, so every image
		 * is retried at the next ship point.  No locks are held here and a
		 * timed-out op marked its channel abandoned, so downgrading to a
		 * WARNING is safe -- and required: ship points live inside
		 * checkpoints, recovery steps, and the checksum state machines,
		 * none of which a mirror outage may abort.
		 */
		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore: pg_control mirror ship failed; %d image(s) remain queued",
						ps_control_queue_count)));
	}
	PG_END_TRY();
}

/*
 * control_file_write_hook: record the just-written control image.  May run
 * inside a critical section, so it only copies into a pre-reserved slot --
 * no allocation, no store I/O, no error.  Outside a critical section it
 * ships immediately.
 */
static void
ps_control_write_hook(const struct ControlFileData *control,
					  XLogRecPtr update_lsn)
{
	if (prev_control_file_write_hook)
		(*prev_control_file_write_hook) (control, update_lsn);

	if (!ps_control_mirror_enabled)
		return;

	/*
	 * Same-LSN dedup: two control writes can legitimately carry one update
	 * LSN (end-of-recovery + a recordless promotion).  Keep only the NEWER
	 * image for a given LSN -- the collapse is correct (see the file
	 * header), and it makes every shipped append for one LSN byte-identical,
	 * so a timed-out write completing LATE after a retry cannot reorder
	 * distinct same-LSN images (latest-append-wins then picks identical
	 * bytes either way).
	 */
	for (int i = 0; i < ps_control_queue_count; i++)
	{
		PsControlPending *q = &ps_control_queue[(ps_control_queue_head + i)
												% PS_CONTROL_QUEUE_CAPACITY];

		if (q->update_lsn == update_lsn)
		{
			/*
			 * Replace only while no write for this LSN can be in flight: a
			 * drain may have posted the older bytes and timed out, and the
			 * abandoned request can still append LATE -- if a retry had
			 * shipped different (newer) bytes first, that late older append
			 * would win latest-append-wins and restore pre-transition
			 * state.  Keeping the first-posted bytes for an LSN forever
			 * makes every append for it byte-identical; the dropped newer
			 * same-LSN image is a valid state at that LSN (the compute
			 * re-derives the transition on boot).
			 */
			if (!q->posted)
				memcpy(&q->image, control, sizeof(ControlFileData));
			if (CritSectionCount == 0 && !LWLockHeldByMe(ControlFileLock))
				ps_control_drain();
			return;
		}
	}

	if (ps_control_queue_count == PS_CONTROL_QUEUE_CAPACITY &&
		CritSectionCount == 0 && !LWLockHeldByMe(ControlFileLock))
	{
		/*
		 * Full after a daemon outage, but it is safe to ship right now:
		 * try that before sacrificing an image -- the daemon may be back.
		 */
		ps_control_drain();
	}
	if (ps_control_queue_count == PS_CONTROL_QUEUE_CAPACITY)
	{
		/*
		 * Still full (inside a critical section, or the daemon is still
		 * down).  Drop the oldest image so the newest -- the one local
		 * pg_control now holds -- survives; the loss is counted and
		 * reported at the next drain.
		 */
		ps_control_queue_head = (ps_control_queue_head + 1) % PS_CONTROL_QUEUE_CAPACITY;
		ps_control_queue_count--;
		ps_control_dropped++;
	}

	{
		int			slot = (ps_control_queue_head + ps_control_queue_count)
			% PS_CONTROL_QUEUE_CAPACITY;

		memcpy(&ps_control_queue[slot].image, control, sizeof(ControlFileData));
		ps_control_queue[slot].update_lsn = update_lsn;
		ps_control_queue[slot].posted = false;
		ps_control_queue_count++;
	}

	/*
	 * Drain only when it is safe to perform (and possibly fail) store IPC:
	 * outside critical sections AND not holding ControlFileLock -- several
	 * callers (UpdateMinRecoveryPoint, the recovery/promotion paths) call
	 * UpdateControlFile() with the lock held, and an IPC wait or ERROR
	 * there would stall or break every other control-file user.  Core
	 * invokes the flush hook right after those locks are released.
	 */
	if (CritSectionCount == 0 && !LWLockHeldByMe(ControlFileLock))
		ps_control_drain();
}

/*
 * Final ship attempt at process exit: control writes made by short-lived
 * processes (the data-checksum worker, a shutting-down checkpointer) must
 * not vanish with the process while the queue holds them.  Runs via
 * before_shmem_exit, so the localsvc channel is still attached; bounded
 * waits + the swallowing catch make it exit-safe.  A process CRASH can
 * still lose queued images -- bounded staleness until the next control
 * write re-mirrors, same class as the documented overflow drop.
 */
static void
ps_control_exit_drain(int code, Datum arg)
{
	if (ps_control_mirror_enabled && ps_control_queue_count > 0 &&
		CritSectionCount == 0)
		ps_control_drain();
}

/* control_file_flush_hook: the post-critical ship point */
static void
ps_control_flush_hook(void)
{
	if (prev_control_file_flush_hook)
		(*prev_control_file_flush_hook) ();

	if (!ps_control_mirror_enabled)
		return;

	ps_control_drain();
}

/*
 * Install the mirror hooks.  Called from the module's _PG_init() once the
 * backend GUCs exist; mirroring engages only under the localsvc backend.
 */
void
pagestore_control_mirror_init(bool localsvc_active)
{
	/*
	 * The control object holds the full on-disk control image in one block;
	 * see the design's block-size requirement.  Refuse (skip) rather than
	 * silently truncate on --with-blocksize builds smaller than the control
	 * file.
	 */
	if (BLCKSZ < PG_CONTROL_FILE_SIZE)
	{
		if (localsvc_active)
			ereport(WARNING,
					(errmsg("pagestore: pg_control mirroring disabled: BLCKSZ (%d) is smaller than the control file (%d)",
							BLCKSZ, PG_CONTROL_FILE_SIZE)));
		ps_control_mirror_enabled = false;
	}
	else
		ps_control_mirror_enabled = localsvc_active;

	prev_control_file_write_hook = control_file_write_hook;
	control_file_write_hook = ps_control_write_hook;
	prev_control_file_flush_hook = control_file_flush_hook;
	control_file_flush_hook = ps_control_flush_hook;

}

/*
 * pagestore_control_image_asof(lsn pg_lsn) returns bytea
 *
 * Test/inspection helper: read the store's mirrored control image as of the
 * given LSN (the newest image whose update LSN is <= lsn) on this compute's
 * timeline.  Returns NULL if no image at/below that LSN exists.
 */
/*
 * pagestore_wal_retain_floor() returns pg_lsn
 *
 * The store's durable WAL retention floor for this compute's timeline
 * ancestry: shipped WAL at/above it must be kept for the mirrored pg_control
 * images to stay restorable.  NULL if no control image constrains WAL yet.
 */
PG_FUNCTION_INFO_V1(pagestore_wal_retain_floor);
Datum
pagestore_wal_retain_floor(PG_FUNCTION_ARGS)
{
	uint64		floor;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to read the WAL retention floor")));

	floor = pagestore_localsvc_wal_retain_floor();
	if (floor == 0)
		PG_RETURN_NULL();
	PG_RETURN_LSN((XLogRecPtr) floor);
}

PG_FUNCTION_INFO_V1(pagestore_control_image_asof);
Datum
pagestore_control_image_asof(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn = PG_GETARG_LSN(0);
	PageStoreRelKey key = {0};
	char		page[BLCKSZ];
	uint64		resolved = 0;
	bytea	   *out;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to read the mirrored control image")));

	/*
	 * Same size gate as mirroring: on a BLCKSZ < PG_CONTROL_FILE_SIZE build
	 * the local page buffer cannot hold a full control image, and copying
	 * PG_CONTROL_FILE_SIZE below would read past it.
	 */
	if (BLCKSZ < PG_CONTROL_FILE_SIZE)
		ereport(ERROR,
				(errmsg("pagestore: control images are not mirrored on BLCKSZ (%d) < %d builds",
						BLCKSZ, PG_CONTROL_FILE_SIZE)));

	if (!pagestore_localsvc_obj_read_at(PS_KLASS_CONTROL, &key, 0,
										(uint64) lsn, page, &resolved))
		PG_RETURN_NULL();

	out = (bytea *) palloc(VARHDRSZ + PG_CONTROL_FILE_SIZE);
	SET_VARSIZE(out, VARHDRSZ + PG_CONTROL_FILE_SIZE);
	memcpy(VARDATA(out), page, PG_CONTROL_FILE_SIZE);
	PG_RETURN_BYTEA_P(out);
}
