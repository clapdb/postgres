/*-------------------------------------------------------------------------
 *
 * pagestore_slru.c
 *	  Live SLRU page mirror: write-side capture and ship.
 *
 * Consumes the core SLRU seams (slru_page_write_hook) to mirror the
 * WAL-logged SLRUs' flushed page images to the page store, so that other
 * computes on the same timeline can later observe this compute's
 * transaction status (SLRU_ON_STORE_DESIGN.md, "Deferred: live
 * multi-compute SLRU read-sharing").  This file is the write side only:
 * capture, staging, WAL-fence gating, and the drain that ships.  The
 * visibility watermark, truncation tombstones, and the read-side consumer
 * are follow-up increments and build on what is staged here.
 *
 * Contract inherited from the hook (slru.h): it fires inside
 * SlruInternalWritePage() while the bank lock is still held, so the page
 * bytes are exactly what the just-cleared dirty bit published; the hook
 * must be infallible (no error, no lock, no allocation).  So, like the
 * pg_control mirror, this file only copies the image into a pre-reserved
 * in-process queue and ships at post-critical drain points.
 *
 * Versioning: an image is versioned by its WAL fence -- the page's largest
 * group commit LSN, which upper-bounds every status bit the bytes contain.
 * The drain calls XLogFlush(fence) before shipping (the same WAL-before-data
 * order the local SlruPhysicalWritePage() enforces), so a mirrored image
 * never advertises a commit whose WAL is not durable.  When the hook cannot
 * supply a fence (SLRUs without group LSNs, or redo-driven writes), the
 * capture stamps the current insert/replay position instead -- at capture
 * time, not at drain time: the page can be dirtied again between the flush
 * and the drain, and a drain-time position would falsely cover those later
 * changes.
 *
 * These live images are keyed PS_KLASS_SLRU_LIVE, deliberately NOT
 * PS_KLASS_SLRU: seed snapshots (pagestore_ship_slru_snapshot) carry a
 * proven clean-as-of-cutoff guarantee that flushed page images do not have
 * (SLRU_ON_STORE_DESIGN.md's round-3 coalescing analysis).  Sharing the
 * keyspace would let branch seeding resolve a live image as its base and
 * silently inherit post-cutoff status.  Live images mean only "the page's
 * newest flushed bytes, contents bounded by the version"; readers take the
 * newest image, never an exact as-of.
 *
 * No-drop overflow: a staged image may not be silently dropped -- a reader
 * gating on the (follow-up) watermark would then trust a mirror that is
 * missing status.  If the image queue is full, the page's identity goes to
 * a recapture table instead, and the drain re-snapshots the page's current
 * bytes under its bank lock (or from the local segment file if evicted)
 * with a freshly computed fence -- a later capture event, not the lost
 * bytes served under their old version.  Only if the recapture table also
 * overflows is coverage lost, and that is counted (ps_slru_lost) and
 * warned about so the watermark side can fail conservative.
 *
 * Post-then-sync, frozen once posted: an entry is popped only after the
 * daemon has durably synced it; until then it stays staged.  And once its
 * WRITE has been posted at a version, the entry's bytes are frozen at that
 * version -- a timed-out request may still be sitting in the daemon's
 * pipeline, and the store resolves same-version appends by arrival order,
 * so a retry must be byte-identical to be order-independent.  Newer bytes
 * for a frozen page go through the recapture table and re-ship under a
 * fence strictly above the posted one.
 *
 * src/../contrib/pagestore/pagestore_slru.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/commit_ts.h"
#include "access/multixact_internal.h"
#include "access/slru.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "pagestore_backend.h"
#include "varatt.h"

/* GUC: engage the live SLRU mirror (requires the localsvc backend) */
static bool pagestore_slru_mirror = false;

static bool ps_slru_mirror_enabled = false;
static slru_page_write_hook_type prev_slru_page_write_hook = NULL;

/* Per-op mailbox timeout and whole-drain budget; see the control mirror. */
#define PS_SLRU_SHIP_TIMEOUT_MS		10000
#define PS_SLRU_DRAIN_BUDGET_MS		30000

/*
 * The staging queue.  Sized for a burst of SimpleLruWriteAll() flushes
 * between two drain points; entries are deduplicated by page (keep-newest),
 * so capacity bounds distinct dirty pages, not write events.
 */
#define PS_SLRU_QUEUE_CAPACITY		256

typedef struct PsSlruPending
{
	bool		used;
	bool		shipped;		/* posted this drain; popped only after the
								 * store has synced */
	uint32		obj;			/* slru_klass_id of the SLRU directory */
	uint32		pageno;
	XLogRecPtr	fence_lsn;		/* what the drain must XLogFlush(): the
								 * page's largest group commit LSN (every
								 * not-yet-durable bit has one) */
	XLogRecPtr	bound;			/* the image's version base: the WAL position
								 * at capture time, an upper bound of every
								 * bit the bytes contain (group fences alone
								 * understate it -- synchronous commits do
								 * not update group LSNs); sampled under the
								 * bank lock, so version order is capture
								 * order */
	XLogRecPtr	posted_fence;	/* version of a post that may have reached
								 * the daemon; freezes the bytes (see
								 * ps_slru_write_hook) */
	char		image[BLCKSZ];
} PsSlruPending;

static PsSlruPending ps_slru_queue[PS_SLRU_QUEUE_CAPACITY];
static int	ps_slru_queue_count = 0;

/*
 * Overflow recapture table: page identities whose image could not be staged.
 * The drain re-snapshots them.  Holds the SlruDesc pointer -- SLRU descs are
 * process-lifetime statics in their owning modules (clog.c etc.), so the
 * pointer stays valid.
 */
#define PS_SLRU_RECAP_CAPACITY		128

typedef struct PsSlruRecapture
{
	bool		used;
	SlruDesc   *ctl;
	uint32		obj;
	uint32		pageno;
	XLogRecPtr	fence_floor;	/* recaptured image must be versioned
								 * strictly above this (a same-version post
								 * with different bytes may still be in
								 * flight); Invalid = no constraint */
} PsSlruRecapture;

static PsSlruRecapture ps_slru_recap[PS_SLRU_RECAP_CAPACITY];
static int	ps_slru_recap_count = 0;

/*
 * Pages whose capture was lost outright (queue AND recapture table full, or
 * an un-keyable page number).  The per-process counter drives this
 * process's WARNING cadence; the shared counter is what
 * pagestore_slru_mirror_stats() reports -- most SLRU flushing happens in
 * the checkpointer, so a private counter would read as zero from any SQL
 * backend.  The watermark follow-up builds its own (persistent, debt-
 * seeded) gating counter on top.
 */
static uint64 ps_slru_lost = 0;
static uint64 ps_slru_lost_reported = 0;

static bool ps_slru_exit_registered = false;

/*
 * True while draining from a place where an ERROR must not escape: the
 * transaction-end callback (the transaction is already committed/prepared;
 * rethrowing a cancel there would report failure for a durable commit) and
 * the exit callback.  A swallowed interrupt is re-armed instead, so it
 * fires at the next CHECK_FOR_INTERRUPTS().
 */
static bool ps_slru_no_rethrow = false;

static void ps_slru_exit_drain(int code, Datum arg);
static void ps_slru_xact_drain(XactEvent event, void *arg);
static XLogRecPtr ps_slru_now_lsn(void);
static XLogRecPtr ps_slru_flush_pos(XLogRecPtr ptr);
static bool ps_slru_service_recaptures(TimestampTz drain_start, bool *budget_out);
static void ps_slru_debt_persist(void);
static void ps_slru_wm_note_lost(void);

/*
 * Bump every ledger a loss touches: the per-process counter (WARNING
 * cadence), the shared stats counter (SQL observability), and the
 * watermark's gating counter (freeze + persistent debt).  Infallible.
 */
static void
ps_slru_note_lost(void)
{
	ps_slru_lost++;
	ps_slru_wm_note_lost();
}

/*
 * The visibility watermark (mirrored_status_lsn): an LSN W such that every
 * in-scope SLRU status change with WAL position <= W is durably mirrored.
 * A reader on another compute may trust the live mirror for status up to W
 * and no further.  W advances only over a contiguous durable prefix:
 *
 * - The *candidate* for W is the redo pointer of the last completed
 *   checkpoint whose pg_control image has durably shipped (the control
 *   mirror reports it).  A completed checkpoint has flushed every dirty
 *   SLRU page, and the mirror captures every flush since boot, so all
 *   status <= redo has been staged by then -- by this instance, or by a
 *   pre-crash instance whose ship already made it durable.
 * - Every process publishes whether it holds images staged but not yet
 *   durably shipped (pending_min, 0 = none).  W advances to the candidate
 *   only while NOTHING is pending anywhere: a staged image carries every
 *   status change on its page since that page's previous durable image,
 *   and that interval's low end is unknown -- so any in-flight image may
 *   carry status arbitrarily far below its fence, and no partial bound
 *   (fence-1 or otherwise) is safe.  A high LSN being durable never
 *   implies lower ones are.
 * - A lost capture freezes W for good.  A lost image is a hole the mirror
 *   cannot prove it ever re-covers: the page is clean locally after the
 *   flush that captured it, so no later checkpoint is guaranteed to flush
 *   (and thus re-capture) it.  W stays wherever it was -- everything at or
 *   below it was proven durable before the loss -- and never advances
 *   again.  Losses are also persistent: they survive restarts via a debt
 *   marker file, and an unclean shutdown (anything but DB_SHUTDOWNED in
 *   pg_control) is itself a loss, because a dying process may have held
 *   staged images whose pages are clean on disk and will never be flushed
 *   again.  Recovery is an operator action (re-prime the mirror, then
 *   pagestore_slru_mirror_reset_debt()), not something a later checkpoint
 *   can silently declare.
 *
 * Image versions are based on the capture-time BOUND, the insert position
 * sampled while the page's bank lock is held, then reserved through a small
 * shared per-page table so equal sampled positions are lifted into strict
 * capture order.  A group-LSN fence alone cannot promise that (recomputed
 * fences shrink on eviction/reload, and understate contents anyway), which is
 * why the fence is only the flush obligation and never the version.
 *
 * The local commit is never held back -- only its visibility to other
 * computes waits for the mirror.
 */
#define PS_SLRU_VERSION_SLOTS	4096

typedef struct PsSlruVersionSlot
{
	pg_atomic_uint64 version;
} PsSlruVersionSlot;

typedef struct PsSlruWatermarkShm
{
	pg_atomic_uint64 watermark;
	pg_atomic_uint64 candidate;
	pg_atomic_uint64 total_lost;
	pg_atomic_uint64 stats_lost;	/* raw loss events, for observability --
									 * unlike total_lost it is not seeded
									 * with boot debt and never reset */
	pg_atomic_uint32 debt_unpersisted;	/* a loss awaits the marker file */
	PsSlruVersionSlot version_slot[PS_SLRU_VERSION_SLOTS];

	/*
	 * Per-process pending floor + owner pid.  A nonzero floor whose owner
	 * is dead is a coverage loss a crashed backend never accounted for;
	 * the sweep in ps_slru_wm_advance() converts it.
	 */
	struct
	{
		pg_atomic_uint64 floor;
		pg_atomic_uint64 pid;
	}			pending[FLEXIBLE_ARRAY_MEMBER];	/* per ProcNumber */
} PsSlruWatermarkShm;

/*
 * Debt marker: created (and fsynced) the moment a loss is observed, checked
 * at every boot.  Without it a loss followed by a clean shutdown would be
 * forgotten with the shared memory.  Lives in the data directory; removed
 * only by pagestore_slru_mirror_reset_debt().
 */
#define PS_SLRU_DEBT_FILE	"pagestore.slru_mirror_debt"

/*
 * Primed marker: proof that an operator has ever declared this mirror
 * whole (pagestore_slru_mirror_reset_debt()).  Without it, enabling the
 * mirror on a cluster with pre-existing SLRU history would let the first
 * checkpoint publish a watermark over pages the write hook never saw --
 * clean local segments are never flushed again, so they are captured only
 * by an explicit priming (seeding) step.  Absent marker = boot debt.
 */
#define PS_SLRU_PRIMED_FILE	"pagestore.slru_mirror_primed"

static PsSlruWatermarkShm *ps_slru_wm = NULL;
static int	ps_slru_wm_nprocs = 0;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_slru_shmem_startup_hook = NULL;

/*
 * The in-scope SLRU directories (the WAL-logged, uint32-page ones; see
 * SLRU_ON_STORE_DESIGN.md's scope) and their stable object ids.  The ids
 * must equal pagestore.c's slru_klass_id() so live images and seed
 * snapshots address the same logical SLRU (they differ by klass).
 */
typedef struct PsSlruDirMap
{
	const char *dir;
	uint32		obj;
} PsSlruDirMap;

static PsSlruDirMap ps_slru_dirmap[] = {
	{"pg_xact", 0},
	{"pg_multixact/offsets", 0},
	{"pg_multixact/members", 0},
	{"pg_commit_ts", 0},
};

#define PS_SLRU_CLOG_XACTS_PER_PAGE	(BLCKSZ * 4)

typedef struct PsSlruCommitTimestampEntry
{
	TimestampTz time;
	ReplOriginId nodeid;
} PsSlruCommitTimestampEntry;

#define PS_SLRU_COMMIT_TS_XACTS_PER_PAGE \
	(BLCKSZ / (offsetof(PsSlruCommitTimestampEntry, nodeid) + \
			   sizeof(ReplOriginId)))

static int64
ps_slru_xid_page(TransactionId xid)
{
	return xid / (int64) PS_SLRU_CLOG_XACTS_PER_PAGE;
}

static int64
ps_slru_commit_ts_page(TransactionId xid)
{
	return xid / (int64) PS_SLRU_COMMIT_TS_XACTS_PER_PAGE;
}

static MultiXactId
ps_slru_previous_multixact_id(MultiXactId multi)
{
	return multi == FirstMultiXactId ? MaxMultiXactId : multi - 1;
}

static bool
ps_slru_tomb_horizon_cutoff(int idx, int64 *cutoff)
{
	const char *dir = ps_slru_dirmap[idx].dir;

	if (strcmp(dir, "pg_xact") == 0)
	{
		*cutoff = ps_slru_xid_page(TransamVariables->oldestXid);
		return true;
	}
	if (strcmp(dir, "pg_commit_ts") == 0)
	{
		if (!TransactionIdIsValid(TransamVariables->oldestCommitTsXid))
			*cutoff = PG_INT64_MAX;
		else
			*cutoff = ps_slru_commit_ts_page(TransamVariables->oldestCommitTsXid);
		return true;
	}
	if (strcmp(dir, "pg_multixact/offsets") == 0 ||
		strcmp(dir, "pg_multixact/members") == 0)
	{
		uint32		multixacts;
		MultiXactOffset nextOffset;
		MultiXactId oldestMulti;
		MultiXactOffset oldestOffset;

		GetMultiXactInfo(&multixacts, &nextOffset, &oldestMulti,
						 &oldestOffset);
		if (strcmp(dir, "pg_multixact/offsets") == 0)
			*cutoff = MultiXactIdToOffsetPage(
				ps_slru_previous_multixact_id(oldestMulti));
		else
			*cutoff = MXOffsetToMemberPage(oldestOffset);
		return true;
	}
	return false;
}

/* Stable per-SLRU object id from its directory name (FNV-1a; libc-only). */
uint32
pagestore_slru_klass_id(const char *name)
{
	uint32		h = 2166136261u;
	const unsigned char *p;

	for (p = (const unsigned char *) name; *p != '\0'; p++)
	{
		h ^= *p;
		h *= 16777619u;
	}
	return h;
}

/* Index of an SLRU dir in the scope table; -1 = out of scope. */
static int
ps_slru_dir_index(const char *dir)
{
	for (int i = 0; i < (int) lengthof(ps_slru_dirmap); i++)
	{
		if (strcmp(ps_slru_dirmap[i].dir, dir) == 0)
			return i;
	}
	return -1;
}

/* Map an SLRU dir to its object id; false = out of scope, do not mirror. */
static bool
ps_slru_dir_obj(const char *dir, uint32 *obj)
{
	int			idx = ps_slru_dir_index(dir);

	if (idx < 0)
		return false;
	*obj = ps_slru_dirmap[idx].obj;
	return true;
}

static void
ps_slru_obj_key(PageStoreRelKey *key, uint32 obj)
{
	key->spcOid = 0;
	key->dbOid = 0;
	key->relNumber = (RelFileNumber) obj;
	key->forkNum = 0;
}

static uint32
ps_slru_page_hash(uint32 obj, uint32 pageno)
{
	uint32		h = obj;

	h ^= pageno + 0x9e3779b9 + (h << 6) + (h >> 2);
	return h;
}

static XLogRecPtr
ps_slru_reserve_version(uint32 obj, uint32 pageno, XLogRecPtr bound)
{
	PsSlruVersionSlot *slot;
	uint64		cur;
	uint64		next;

	if (ps_slru_wm == NULL || XLogRecPtrIsInvalid(bound))
		return bound;

	slot = &ps_slru_wm->version_slot[ps_slru_page_hash(obj, pageno) %
									 PS_SLRU_VERSION_SLOTS];
	cur = pg_atomic_read_u64(&slot->version);
	for (;;)
	{
		next = Max((uint64) bound, cur + 1);
		if (pg_atomic_compare_exchange_u64(&slot->version, &cur, next))
			return (XLogRecPtr) next;
	}
}

/* ---- watermark shared memory ---- */

static Size
ps_slru_wm_size(void)
{
	int			nprocs = MaxBackends + NUM_AUXILIARY_PROCS;

	return offsetof(PsSlruWatermarkShm, pending) +
		mul_size(sizeof(ps_slru_wm->pending[0]), nprocs);
}

static void
ps_slru_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(ps_slru_wm_size());
}

/*
 * Boot-time debt: was the previous life of this cluster provably clean?
 * Anything but a DB_SHUTDOWNED pg_control is not -- a crash (or a
 * postmaster reinit after a backend crash: state is DB_IN_PRODUCTION
 * then) means processes died that may have held staged-but-unsynced
 * images, of pages that are clean on local disk and will never be flushed
 * (and thus re-captured) again.  A pre-existing debt marker is a loss
 * remembered from a previous life.
 */
static bool
ps_slru_boot_debt(void)
{
	struct stat st;
	ControlFileData *cf;
	bool		crc_ok;
	bool		debt;

	if (stat(PS_SLRU_DEBT_FILE, &st) == 0)
		return true;
	if (stat(PS_SLRU_PRIMED_FILE, &st) != 0)
		return true;			/* never primed: pre-enable history unproven */

	cf = get_controlfile(DataDir, &crc_ok);
	debt = !crc_ok || cf->state != DB_SHUTDOWNED;
	pfree(cf);
	return debt;
}

static void
ps_slru_shmem_startup(void)
{
	bool		found;

	if (prev_slru_shmem_startup_hook)
		prev_slru_shmem_startup_hook();

	ps_slru_wm_nprocs = MaxBackends + NUM_AUXILIARY_PROCS;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ps_slru_wm = ShmemInitStruct("pagestore slru mirror watermark",
								 ps_slru_wm_size(), &found);
	if (!found)
	{
		bool		debt = ps_slru_boot_debt();

		pg_atomic_init_u64(&ps_slru_wm->watermark, 0);
		pg_atomic_init_u64(&ps_slru_wm->candidate, 0);
		pg_atomic_init_u64(&ps_slru_wm->total_lost, debt ? 1 : 0);
		pg_atomic_init_u64(&ps_slru_wm->stats_lost, 0);
		pg_atomic_init_u32(&ps_slru_wm->debt_unpersisted, debt ? 1 : 0);
		for (int i = 0; i < PS_SLRU_VERSION_SLOTS; i++)
			pg_atomic_init_u64(&ps_slru_wm->version_slot[i].version, 0);
		for (int i = 0; i < ps_slru_wm_nprocs; i++)
		{
			pg_atomic_init_u64(&ps_slru_wm->pending[i].floor, 0);
			pg_atomic_init_u64(&ps_slru_wm->pending[i].pid, 0);
		}

		/*
		 * Boot debt must not wait for a drain to be persisted: a clean
		 * shutdown before any drain would otherwise forget it.
		 */
		if (debt)
			ps_slru_debt_persist();
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Publish that this process holds a staged-but-not-durable image at
 * 'fence'.  Called under the bank lock: a single atomic store to our own
 * slot, infallible.  Invalid fences publish as 1 (a floor below any real
 * LSN) -- the drain re-stamps them, but until it does the watermark must
 * not move at all.
 */
static void
ps_slru_wm_note_pending(XLogRecPtr fence)
{
	uint64		f = XLogRecPtrIsInvalid(fence) ? 1 : (uint64) fence;
	pg_atomic_uint64 *slot;
	uint64		cur;
	uint64		owner;

	if (ps_slru_wm == NULL || MyProcNumber == INVALID_PROC_NUMBER ||
		MyProcNumber >= ps_slru_wm_nprocs)
		return;

	/*
	 * A leftover floor under a different pid means the slot's previous
	 * owner died holding staged images and nobody accounted for them:
	 * convert to a loss before taking the slot over, or reusing the
	 * ProcNumber would silently erase the hole.
	 */
	owner = pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].pid);
	if (owner != (uint64) MyProcPid)
	{
		if (owner != 0 &&
			pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].floor) != 0)
			ps_slru_wm_note_lost();
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].floor, 0);
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].pid,
							(uint64) MyProcPid);
	}

	slot = &ps_slru_wm->pending[MyProcNumber].floor;
	cur = pg_atomic_read_u64(slot);
	if (cur == 0 || f < cur)
		pg_atomic_write_u64(slot, f);
}

static void
ps_slru_wm_note_lost(void)
{
	if (ps_slru_wm != NULL)
	{
		pg_atomic_fetch_add_u64(&ps_slru_wm->total_lost, 1);
		pg_atomic_fetch_add_u64(&ps_slru_wm->stats_lost, 1);
		pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
	}
}

/*
 * Make an observed loss survive restarts: create the debt marker.  Called
 * from drain points (never in a critical section); libc-only and
 * best-effort -- on failure the flag stays set and the next drain retries,
 * and until the file exists the loss is still enforced by the in-memory
 * counter.
 */
static void
ps_slru_debt_persist(void)
{
	int			fd;

	if (ps_slru_wm == NULL ||
		pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) == 0)
		return;

	fd = open(PS_SLRU_DEBT_FILE, O_WRONLY | O_CREAT | PG_BINARY, pg_file_create_mode);
	if (fd >= 0)
	{
		bool		ok = (fsync(fd) == 0);

		close(fd);

		/*
		 * The new directory entry must be durable too, or a host crash
		 * after a clean shutdown could forget the marker (and with it the
		 * loss).  Libc-only: this also runs from exit callbacks.
		 */
		if (ok)
		{
			int			dfd = open(".", O_RDONLY);

			ok = (dfd >= 0 && fsync(dfd) == 0);
			if (dfd >= 0)
				close(dfd);
		}
		if (ok)
			pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 0);
	}
	if (pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) != 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("pagestore: could not persist SLRU mirror debt marker \"%s\": %m",
						PS_SLRU_DEBT_FILE)));
}

/*
 * Republish this process's pending floor after a drain: 0 when everything
 * staged has durably shipped, else the minimum fence still held.
 */
static void
ps_slru_wm_republish_pending(void)
{
	uint64		f = 0;
	uint64		owner;

	if (ps_slru_wm == NULL || MyProcNumber == INVALID_PROC_NUMBER ||
		MyProcNumber >= ps_slru_wm_nprocs)
		return;

	/*
	 * This can be the first post-drain touch of a ProcNumber slot inherited
	 * from a dead backend.  Account for the previous owner's outstanding
	 * floor before overwriting it with this backend's idle state.
	 */
	owner = pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].pid);
	if (owner != (uint64) MyProcPid)
	{
		if (owner != 0 &&
			pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].floor) != 0)
			ps_slru_wm_note_lost();
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].floor, 0);
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].pid,
							(uint64) MyProcPid);
	}

	for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY; i++)
	{
		PsSlruPending *p = &ps_slru_queue[i];
		uint64		pf;

		if (!p->used)
			continue;
		pf = XLogRecPtrIsInvalid(p->fence_lsn) ? 1 : (uint64) p->fence_lsn;
		if (f == 0 || pf < f)
			f = pf;
	}
	if (ps_slru_recap_count > 0)
		f = 1;					/* identities without fences: freeze */

	pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].floor, f);
}

/*
 * Try to advance the watermark to the candidate.  Only legal while no
 * process holds a pending image: a staged image carries all status on its
 * page since the page's previous durable image, so its uncovered low end
 * is unknown and no partial bound is safe.  Cheap and lock-free; called at
 * the end of every drain (where the common case is: everything shipped,
 * nothing pending).
 */
static void
ps_slru_wm_advance(void)
{
	uint64		cand;

	if (ps_slru_wm == NULL)
		return;
	cand = pg_atomic_read_u64(&ps_slru_wm->candidate);
	if (cand == 0)
		return;

	/*
	 * Any loss, ever -- including the boot debt of an unclean previous life
	 * -- freezes the watermark for good: the missing image's page is clean
	 * locally, so no later checkpoint provably re-covers it.  Whatever W
	 * already vouched for stays valid (it was proven before the loss); it
	 * just never grows until an operator re-primes the mirror and resets
	 * the debt.
	 */
	if (pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0)
		return;

	for (int i = 0; i < ps_slru_wm_nprocs; i++)
	{
		PGPROC	   *proc;

		if (pg_atomic_read_u64(&ps_slru_wm->pending[i].floor) == 0)
			continue;

		/*
		 * A floor whose owner is gone is a coverage loss no exit path
		 * accounted for (e.g. a backend killed after staging but before
		 * its first drain registered the exit callback).  Ownership is
		 * checked against the ProcNumber's PGPROC pid, not the OS pid (a
		 * recycled OS pid would report the dead owner alive).  Convert it
		 * -- the loss freezes the watermark for good, so stop here rather
		 * than let this very pass advance over the fresh hole -- and
		 * clear the slot so it does not read as pending forever.
		 */
		proc = GetPGProcByNumber(i);
		if ((uint64) proc->pid != pg_atomic_read_u64(&ps_slru_wm->pending[i].pid))
		{
			ps_slru_wm_note_lost();
			pg_atomic_write_u64(&ps_slru_wm->pending[i].floor, 0);
			pg_atomic_write_u64(&ps_slru_wm->pending[i].pid, 0);

			/*
			 * This runs after the drain's own persist point; make the
			 * fresh debt durable now rather than hoping for a later drain
			 * (we are in a post-critical drain tail, file I/O is fine).
			 */
			ps_slru_debt_persist();
		}
		return;
	}

	for (;;)
	{
		uint64		cur = pg_atomic_read_u64(&ps_slru_wm->watermark);

		if (cand <= cur)
			break;
		if (pg_atomic_compare_exchange_u64(&ps_slru_wm->watermark, &cur, cand))
			break;
	}
}

/*
 * The control mirror reports the redo pointer of a completed checkpoint
 * whose pg_control image has durably shipped; that redo is the new
 * watermark candidate.  Whether it may ever be published is decided at
 * advance time (no pending images, zero losses ever).
 */
void
pagestore_slru_note_checkpoint_redo(XLogRecPtr redo)
{
	if (ps_slru_wm == NULL || XLogRecPtrIsInvalid(redo))
		return;

	for (;;)
	{
		uint64		cur = pg_atomic_read_u64(&ps_slru_wm->candidate);

		if ((uint64) redo <= cur)
			return;
		if (pg_atomic_compare_exchange_u64(&ps_slru_wm->candidate, &cur,
										   (uint64) redo))
			break;
	}
}

/*
 * Schedule a page identity for recapture at drain time.  Infallible (called
 * under the bank lock from the write hook): fixed storage, counts a loss if
 * the table is full.
 */
static void
ps_slru_note_recapture(SlruDesc *ctl, uint32 obj, uint32 pageno,
					   XLogRecPtr fence_floor)
{
	for (int i = 0; i < PS_SLRU_RECAP_CAPACITY; i++)
	{
		PsSlruRecapture *r = &ps_slru_recap[i];

		if (r->used && r->obj == obj && r->pageno == pageno)
		{
			if (r->fence_floor < fence_floor)
				r->fence_floor = fence_floor;
			return;				/* already scheduled */
		}
	}
	for (int i = 0; i < PS_SLRU_RECAP_CAPACITY; i++)
	{
		PsSlruRecapture *r = &ps_slru_recap[i];

		if (!r->used)
		{
			r->used = true;
			r->ctl = ctl;
			r->obj = obj;
			r->pageno = pageno;
			r->fence_floor = fence_floor;
			ps_slru_recap_count++;
			ps_slru_wm_note_pending(InvalidXLogRecPtr);
			return;
		}
	}

	/* Table full: coverage lost; the watermark side must fail conservative. */
	ps_slru_note_lost();
}

/*
 * Ship a truncation tombstone for 'obj' at 'cutoff_page' and make it
 * durable, versioned by the current WAL position (at/after the truncation
 * record that caused this -- or slightly before it for the multixact
 * pre-barrier, which is the safe direction: a somewhat-early tombstone only
 * hides pages the truncation decision already declared unneeded, while a
 * late one would let as-of readers below it see pages the WAL has
 * truncated).  The truncation WAL record is flushed first: TruncateCommitTs
 * inserts its record without flushing it, and a tombstone must never be
 * durable in the store while the record that justifies it can still be
 * lost to a crash.  ERRORs on store failure -- the caller decides whether
 * that aborts the truncation (the barrier) or degrades.
 */
static void
ps_slru_ship_tombstone(uint32 obj, int64 cutoff_page, XLogRecPtr version)
{
	PageStoreRelKey key = {0};
	char		page[BLCKSZ];

	if (!RecoveryInProgress())
	{
		XLogRecPtr	flushto = ps_slru_flush_pos(version);

		if (GetFlushRecPtr(NULL) < flushto)
			XLogFlush(flushto);
	}

	memset(page, 0, sizeof(page));
	memcpy(page, &cutoff_page, sizeof(int64));

	ps_slru_obj_key(&key, obj);
	pagestore_localsvc_obj_write_timeout(PS_KLASS_SLRU_TOMB, &key, 0, page,
										 (uint64) version,
										 PS_SLRU_SHIP_TIMEOUT_MS);
	pagestore_localsvc_store_sync_timeout(PS_SLRU_SHIP_TIMEOUT_MS);
}

/*
 * The last tombstone cutoff this process durably shipped per SLRU.  This is
 * only a critical-section duplicate shield (the multixact in-critical-section
 * calls after their pre-barrier, SimpleLruTruncate after an exact-LSN
 * pre-barrier): outside critical sections, ship equal cutoffs again so their
 * newer version can supersede stale store state.  Exact match only: several
 * in-scope page spaces wrap (and the commit-ts reset uses PG_INT64_MAX), so
 * "lower than covered" does not mean "already dead" -- a numerically smaller
 * later cutoff still ships, and its newer version supersedes.
 */
static int64 ps_slru_tomb_covered[lengthof(ps_slru_dirmap)];
static bool ps_slru_tomb_covered_set[lengthof(ps_slru_dirmap)];

/*
 * Re-derive tombstones from local truth.  Part of priming
 * (pagestore_slru_mirror_reset_debt): the multixact pre-barrier ships its
 * tombstone before the truncation's WAL record can exist, so a crash in
 * that window leaves a durable tombstone for a truncation that never
 * happened -- pages that are live locally read as dead in the mirror.
 * Every such crash is boot debt, and priming is its mandated repair: for
 * each in-scope SLRU, publish its current horizon page.  That preserves
 * page-granular cutoffs when the first retained segment is only partially
 * live.  The directory scan is still useful as an error check (an unreadable
 * directory must abort priming) and as a fallback for any future in-scope
 * SLRU without a known horizon.
 */
static void
ps_slru_tomb_rederive(void)
{
	volatile bool commit_ts_locked = false;

	/*
	 * Serialize against concurrent truncations, or a scan could observe a
	 * deletion in progress and publish its (higher) cutoff at a version
	 * OLDER than the truncation's own tombstone -- an over-wide cutoff at
	 * an early LSN.  WrapLimitsVacuumLock serializes the vacuum-side
	 * clog/commit-ts truncations; MultiXactTruncationLock the multixact
	 * ones.  Priming is rare and the scans are tiny.
	 */
	LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);
	LWLockAcquire(MultiXactTruncationLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		for (int i = 0; i < (int) lengthof(ps_slru_dirmap); i++)
		{
			DIR		   *dir;
			struct dirent *de;
			int64		minseg = -1;
			int64		cutoff;
			bool		commit_ts = strcmp(ps_slru_dirmap[i].dir,
										   "pg_commit_ts") == 0;

			if (commit_ts)
			{
				LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
				commit_ts_locked = true;
			}

			/*
			 * Scan failures must abort the priming (ReadDir raises), not
			 * read as an empty directory: an empty scan publishes an
			 * everything-dead cutoff.
			 */
			dir = AllocateDir(ps_slru_dirmap[i].dir);
			while ((de = ReadDir(dir, ps_slru_dirmap[i].dir)) != NULL)
			{
				size_t		len = strlen(de->d_name);
				int64		seg;

				if (len < 4 || len > 15 ||
					strspn(de->d_name, "0123456789ABCDEF") != len)
					continue;
				seg = (int64) strtoull(de->d_name, NULL, 16);
				if (minseg < 0 || seg < minseg)
					minseg = seg;
			}
			FreeDir(dir);

			if (!ps_slru_tomb_horizon_cutoff(i, &cutoff))
				cutoff = (minseg < 0) ? PG_INT64_MAX
					: minseg * SLRU_PAGES_PER_SEGMENT;
			ps_slru_tomb_covered_set[i] = false;
			ps_slru_ship_tombstone(ps_slru_dirmap[i].obj, cutoff,
								   ps_slru_now_lsn());
			if (commit_ts)
			{
				LWLockRelease(CommitTsLock);
				commit_ts_locked = false;
			}
		}
	}
	PG_CATCH();
	{
		if (commit_ts_locked)
			LWLockRelease(CommitTsLock);
		LWLockRelease(MultiXactTruncationLock);
		LWLockRelease(WrapLimitsVacuumLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(MultiXactTruncationLock);
	LWLockRelease(WrapLimitsVacuumLock);
}

/*
 * slru_truncate_hook consumer: the synchronous truncate barrier.  Called
 * before any local segment deletion.
 *
 * Normal running: the tombstone must be durable BEFORE the local files go
 * away, or the mirror would keep serving pages the SLRU no longer has.  A
 * store failure first freezes the visibility watermark (the caller has
 * typically already WAL-logged the truncation, so other computes will
 * replay it; the mirror must not vouch past a record whose tombstone is
 * missing) and then raises, abandoning the local truncation (retried by
 * the next vacuum/checkpoint cycle, like any other truncate failure).
 *
 * Critical sections: multixact truncation reaches SimpleLruTruncate()
 * critical; TruncateMultiXact() runs this hook as a pre-barrier first, so
 * the critical calls find their cutoff covered and return immediately.  If
 * an uncovered barrier ever does fire critical, it must neither block on
 * store I/O nor error -- degrade to a coverage loss.
 *
 * Recovery and the commit-ts delete-all reset: failing the barrier cannot
 * abandon anything (replay must go on, a parameter change is a fact), so
 * degrade to a loss there too -- but interrupts still propagate; they are
 * not store failures.
 */
static void
ps_slru_truncate_hook(SlruDesc *ctl, int64 cutoffPage, XLogRecPtr lsn)
{
	MemoryContext hook_cxt = CurrentMemoryContext;
	int			idx;
	uint32		obj;
	bool		may_abandon;
	XLogRecPtr	version;
	volatile bool shipped = false;

	idx = ps_slru_dir_index(ctl->options.Dir);
	if (idx < 0)
		return;					/* out-of-scope SLRU: not mirrored */
	obj = ps_slru_dirmap[idx].obj;

	if (ps_slru_tomb_covered_set[idx] && cutoffPage == ps_slru_tomb_covered[idx])
	{
		if (CritSectionCount > 0)
			return;				/* pre-barrier already shipped this cutoff */
		ps_slru_tomb_covered_set[idx] = false;
	}

	if (CritSectionCount > 0)
	{
		/*
		 * Cannot error, cannot block on the store.  The pre-barriers are
		 * supposed to make this unreachable; if a path was missed, eat the
		 * miss as a coverage loss so the watermark freezes rather than
		 * advancing over an untombstoned truncation.  Persistence of the
		 * debt is picked up by the next drain (no file I/O here).
		 */
		ps_slru_lost++;
		ps_slru_wm_note_lost();
		return;
	}

	may_abandon = !RecoveryInProgress() && cutoffPage != PG_INT64_MAX;

	/*
	 * Version by the exact truncation record when the caller supplied it
	 * (the TruncateCLOG/TruncateCommitTs pre-barriers); sample otherwise.
	 */
	version = XLogRecPtrIsInvalid(lsn) ? ps_slru_now_lsn() : lsn;

	/*
	 * The synchronous ship below is not staged in the queue, so it must
	 * publish its own pending floor: without one, another backend's
	 * checkpoint could advance the watermark past the truncation record
	 * while this tombstone is still in flight, and a reader would trust
	 * the mirror for an LSN whose truncation it cannot see yet.
	 */
	ps_slru_wm_note_pending(version);

	PG_TRY();
	{
		ps_slru_ship_tombstone(obj, cutoffPage, version);
		shipped = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext ecxt;
		bool		rethrow;

		/*
		 * Whether or not the caller can abandon the truncation, the mirror
		 * now has a range whose tombstone may be missing while its WAL
		 * record may already be out: freeze the watermark, durably.
		 */
		ps_slru_lost++;
		ps_slru_wm_note_lost();
		ps_slru_wm_republish_pending();
		ps_slru_debt_persist();

		if (may_abandon)
			PG_RE_THROW();

		/* interrupts are not store failures; see the drain */
		ecxt = MemoryContextSwitchTo(hook_cxt);
		edata = CopyErrorData();
		MemoryContextSwitchTo(ecxt);
		rethrow = (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
				   edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN);
		FreeErrorData(edata);
		if (rethrow)
			PG_RE_THROW();

		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore: could not ship an SLRU truncation tombstone; the live mirror watermark is frozen")));
	}
	PG_END_TRY();

	ps_slru_wm_republish_pending();

	if (shipped && cutoffPage != PG_INT64_MAX)
	{
		/*
		 * Never cache the delete-all cutoff: every commit-ts deactivation
		 * uses the same PG_INT64_MAX, and skipping a later one would leave
		 * the images a reactivation shipped in between alive forever.
		 */
		ps_slru_tomb_covered[idx] = cutoffPage;
		ps_slru_tomb_covered_set[idx] = true;
	}
}

/*
 * Stage an image (fixed pre-reserved storage only; infallible; callable
 * under a bank lock).  'fence' is the flush obligation, 'bound' the
 * version -- both must have been taken under the page's bank lock, which
 * is what makes version order equal capture order.
 */
static void
ps_slru_stage(SlruDesc *ctl, uint32 obj, uint32 pageno, const char *page,
			  XLogRecPtr fence_lsn, XLogRecPtr bound)
{
	int			free_slot = -1;

	{
		for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY; i++)
		{
			PsSlruPending *p = &ps_slru_queue[i];

			if (p->used)
			{
				if (p->obj == obj && p->pageno == pageno)
				{
					/*
					 * Same page staged again before a drain.  If a post of
					 * this entry may have reached the daemon (a previous
					 * drain timed out or failed after obj_write), the bytes
					 * must stay exactly what was posted: the store resolves
					 * same-version appends by arrival order, and an
					 * abandoned request can land after our retry --
					 * byte-identical duplicates make that order irrelevant.
					 * The newer bytes become a recapture instead,
					 * re-snapshotted at drain time and shipped strictly
					 * above the posted version (the last-shipped floor
					 * guarantees it).
					 */
					if (!XLogRecPtrIsInvalid(p->posted_fence))
					{
						ps_slru_note_recapture(ctl, obj, pageno,
											   p->posted_fence);
						return;
					}

					/*
					 * Keep the newest bytes.  The fence and the bound only
					 * ever need to grow -- a smaller recomputed fence
					 * (group LSNs reset on eviction/reload) must not
					 * un-fence bits the older image already carried.
					 */
					memcpy(p->image, page, BLCKSZ);
					ps_slru_wm_note_pending(fence_lsn);
					if (p->fence_lsn < fence_lsn)
						p->fence_lsn = fence_lsn;
					if (p->bound < bound)
						p->bound = bound;
					return;
				}
			}
			else if (free_slot < 0)
				free_slot = i;
		}

		if (free_slot >= 0)
		{
			PsSlruPending *p = &ps_slru_queue[free_slot];

			ps_slru_wm_note_pending(fence_lsn);
			p->used = true;
			p->shipped = false;
			p->obj = obj;
			p->pageno = pageno;
			p->fence_lsn = fence_lsn;
			p->bound = bound;
			p->posted_fence = InvalidXLogRecPtr;
			memcpy(p->image, page, BLCKSZ);
			ps_slru_queue_count++;
			return;
		}
	}

	/* Queue full: record the page identity for recapture at drain time. */
	ps_slru_note_recapture(ctl, obj, pageno, InvalidXLogRecPtr);
}

/*
 * slru_page_write_hook consumer.  Bank lock held; must be infallible: fixed
 * pre-reserved storage only, no locks, no allocation, no elog.
 */
static void
ps_slru_write_hook(SlruDesc *ctl, int64 pageno, const char *page,
				   XLogRecPtr fence_lsn)
{
	uint32		obj;
	XLogRecPtr	now;

	if (prev_slru_page_write_hook)
		prev_slru_page_write_hook(ctl, pageno, page, fence_lsn);

	if (!ps_slru_dir_obj(ctl->options.Dir, &obj))
		return;					/* out-of-scope SLRU: excluded, not mirrored */

	if (pageno < 0 || pageno > (int64) PG_UINT32_MAX)
	{
		/* cannot be keyed (store block numbers are uint32); count the loss */
		ps_slru_note_lost();
		return;
	}

	/*
	 * Bound the image at CAPTURE time, under the bank lock we are called
	 * with: the bytes certainly contain nothing past the current WAL
	 * position, and nothing later can honestly claim less -- a group-LSN
	 * fence alone understates the contents (synchronous commits and aborts
	 * do not update group LSNs; only async commits do), and a later
	 * position would falsely cover changes applied to the page afterwards.
	 * The fence is kept separately as the flush obligation: every
	 * not-yet-durable bit has a group LSN by the SLRU's own WAL-before-data
	 * rule, so flushing the fence is sufficient and cheaper.  (Reading the
	 * positions is atomics/spinlock only, fine under the bank lock.)
	 */
	now = ps_slru_now_lsn();
	if (XLogRecPtrIsInvalid(fence_lsn))
		fence_lsn = now;
	now = ps_slru_reserve_version(obj, (uint32) pageno, now);

	ps_slru_stage(ctl, obj, (uint32) pageno, page, fence_lsn, now);
}

/*
 * Normalize a sampled insert position into something XLogFlush() accepts:
 * when the last record ended exactly on a WAL page boundary, the reserve
 * pointer points just past the next page's header, where no record ends,
 * and flushing there is "past the end of generated WAL".  Only header
 * bytes separate such a pointer from its boundary, so clamping loses
 * nothing.
 */
static XLogRecPtr
ps_slru_flush_pos(XLogRecPtr ptr)
{
	if (XLogSegmentOffset(ptr, wal_segment_size) == SizeOfXLogLongPHD)
		ptr -= SizeOfXLogLongPHD;
	else if (ptr % XLOG_BLCKSZ == SizeOfXLogShortPHD)
		ptr -= SizeOfXLogShortPHD;
	return ptr;
}

/*
 * A conservative "now" to version images that carry no fence and to stamp
 * truncation tombstones: nothing this process has locally observed lies
 * past it.  In recovery that must be the END of the record currently being
 * replayed (GetCurrentReplayRecPtr), not the last fully-replayed position
 * -- the latter still points BEFORE the record whose redo routine is
 * running, and a truncation tombstone versioned there would become visible
 * to as-of readers below the truncation record itself, hiding pages that
 * were still live at those LSNs.  (For page images a too-low bound is
 * harmless -- the last-shipped floor lifts them -- but there is no reason
 * to use a different clock.)
 */
static XLogRecPtr
ps_slru_now_lsn(void)
{
	if (RecoveryInProgress())
	{
		XLogRecPtr	p = GetCurrentReplayRecPtr(NULL);

		/*
		 * The END of the record being replayed: a redo routine's SLRU
		 * write carries that record's effects, which the last-replayed
		 * position (still pointing before the record) would understate.
		 * Early in recovery the replay pointers may not be set yet; fall
		 * back down the chain -- a too-low bound is safe: an unclean boot
		 * carries boot debt, so the watermark is frozen until the mirror
		 * is re-primed.  (Tombstones are never shipped that early; their
		 * versions stay at/after their record.)
		 */
		if (XLogRecPtrIsInvalid(p))
			p = GetXLogReplayRecPtr(NULL);
		if (XLogRecPtrIsInvalid(p))
			p = GetRedoRecPtr();
		if (XLogRecPtrIsInvalid(p))
			p = (XLogRecPtr) 1;
		return p;
	}
	return GetXLogInsertRecPtr();
}

/*
 * Re-snapshot a page whose staged image was lost to queue overflow.  Fast
 * path: the page is still resident, copy it under its bank lock with a
 * fresh fence.  Slow path (evicted): the local flush that evicted it has
 * already written the bytes, read them back from the segment file and stamp
 * a conservative fence.  Returns false if the page cannot be recaptured
 * right now (I/O error, slot busy) -- the entry stays for the next drain.
 */
static bool
ps_slru_recapture_page(PsSlruRecapture *r, char *image, XLogRecPtr *fence)
{
	SlruDesc   *ctl = r->ctl;
	SlruShared	shared = ctl->shared;
	LWLock	   *banklock = SimpleLruGetBankLock(ctl, (int64) r->pageno);
	int			slots_per_bank = shared->num_slots / ctl->nbanks;
	int			bankstart = ((int) (r->pageno % ctl->nbanks)) * slots_per_bank;
	bool		found = false;

	LWLockAcquire(banklock, LW_SHARED);
	for (int slotno = bankstart; slotno < bankstart + slots_per_bank; slotno++)
	{
		if (shared->page_number[slotno] == (int64) r->pageno &&
			(shared->page_status[slotno] == SLRU_PAGE_VALID ||
			 shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS))
		{
			XLogRecPtr	f = InvalidXLogRecPtr;

			memcpy(image, shared->page_buffer[slotno], BLCKSZ);
			if (shared->group_lsn != NULL)
			{
				int			lsnindex = slotno * shared->lsn_groups_per_page;

				for (int off = 0; off < shared->lsn_groups_per_page; off++)
					if (f < shared->group_lsn[lsnindex + off])
						f = shared->group_lsn[lsnindex + off];
			}
			/*
			 * The bound must be sampled UNDER the bank lock: version order
			 * across captures of one page is exactly their bank-lock
			 * serialization order, and a bound taken after the release
			 * could inflate past a concurrent (newer) capture's.  The copy
			 * may race write-OK status setters admitted under LW_SHARED,
			 * so the group-LSN fence is not trusted as the bound; the
			 * in-lock "now" covers whatever the copy saw.
			 */
			*fence = ps_slru_now_lsn();
			found = true;
			break;
		}
	}
	LWLockRelease(banklock);

	if (found)
		return true;

	/*
	 * Evicted: the bytes were flushed locally; read the segment file --
	 * with the bank lock HELD across the residency check, the read, and
	 * the bound sample.  Loading (and thus rewriting) the page requires
	 * the exclusive bank lock, so non-residency under our shared hold
	 * pins the file bytes for the duration: no load-modify-flush-evict
	 * cycle can slip between the check and the read and leave us shipping
	 * pre-cycle bytes under a post-cycle bound.  The hold spans one 8KB
	 * pread on a rare overflow path.
	 */
	{
		char		path[MAXPGPATH];
		int64		segno = (int64) r->pageno / SLRU_PAGES_PER_SEGMENT;
		int			rpageno = (int) ((int64) r->pageno % SLRU_PAGES_PER_SEGMENT);
		off_t		offset = (off_t) rpageno * BLCKSZ;
		int			fd;
		ssize_t		n = -1;
		bool		resident = false;

		if (ctl->options.long_segment_names)
			snprintf(path, MAXPGPATH, "%s/%015" PRIX64, ctl->options.Dir, segno);
		else
			snprintf(path, MAXPGPATH, "%s/%04X", ctl->options.Dir,
					 (unsigned int) segno);

		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			return false;

		LWLockAcquire(banklock, LW_SHARED);
		for (int slotno = bankstart; slotno < bankstart + slots_per_bank; slotno++)
		{
			if (shared->page_number[slotno] == (int64) r->pageno &&
				shared->page_status[slotno] != SLRU_PAGE_EMPTY)
			{
				resident = true;
				break;
			}
		}
		if (!resident)
		{
			n = pg_pread(fd, image, BLCKSZ, offset);
			if (n == BLCKSZ)
				*fence = ps_slru_now_lsn();
		}
		LWLockRelease(banklock);
		CloseTransientFile(fd);

		return !resident && n == BLCKSZ;
	}
}

/* Is this page identity held by a posted-but-unsynced queue entry? */
static bool
ps_slru_queue_holds_posted(uint32 obj, uint32 pageno)
{
	for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY; i++)
	{
		PsSlruPending *p = &ps_slru_queue[i];

		if (p->used && p->obj == obj && p->pageno == pageno &&
			!XLogRecPtrIsInvalid(p->posted_fence))
			return true;
	}
	return false;
}

static bool
ps_slru_service_recaptures(TimestampTz drain_start, bool *budget_out)
{
	bool		staged = false;

	for (int i = 0; i < PS_SLRU_RECAP_CAPACITY && ps_slru_recap_count > 0; i++)
	{
		PsSlruRecapture *r = &ps_slru_recap[i];
		char		image[BLCKSZ];
		XLogRecPtr	fence;
		XLogRecPtr	bound;

		if (!r->used)
			continue;
		if (TimestampDifferenceExceeds(drain_start, GetCurrentTimestamp(),
									   PS_SLRU_DRAIN_BUDGET_MS))
		{
			*budget_out = true;
			break;
		}
		if (ps_slru_queue_count >= PS_SLRU_QUEUE_CAPACITY)
			break;				/* queue refilled; retry next drain */

		/*
		 * While the identity's posted entry is still awaiting its sync, the
		 * recapture must wait.  This helper is called again after synced
		 * entries pop, so exit drains get a same-drain retry before leftovers
		 * are declared lost.
		 */
		if (ps_slru_queue_holds_posted(r->obj, r->pageno))
			continue;
		if (!ps_slru_recapture_page(r, image, &fence))
			continue;			/* keep for the next drain */

		/*
		 * A fence at or below the floor cannot disambiguate the new bytes
		 * from a same-version post that may still be in flight; wait for WAL
		 * to advance.
		 */
		if (!XLogRecPtrIsInvalid(r->fence_floor) && fence <= r->fence_floor)
			continue;

		/*
		 * Re-stage with the snapshot's own in-lock bound -- routing through
		 * the write hook would resample a later position and could version
		 * these (possibly stale) bytes past a newer concurrent capture's.
		 */
		bound = ps_slru_reserve_version(r->obj, r->pageno, fence);
		ps_slru_stage(r->ctl, r->obj, r->pageno, image, fence, bound);
		r->used = false;
		ps_slru_recap_count--;
		staged = true;
	}
	return staged;
}

/*
 * Ship every staged image, in any order (entries are independent objects;
 * per-page ordering is by version).  Must only run outside critical
 * sections: obj_write can ERROR, recoverable here but fatal in one.  Same
 * failure policy as the control mirror: two-phase, post then sync -- an
 * entry is popped only after the daemon has durably synced it; store
 * failures downgrade to WARNING and everything unsynced stays queued (bytes
 * frozen at their posted version) for the next drain point.
 */
static void
ps_slru_drain(void)
{
	MemoryContext drain_cxt = CurrentMemoryContext;

	Assert(CritSectionCount == 0);

	if (!ps_slru_exit_registered)
	{
		before_shmem_exit(ps_slru_exit_drain, (Datum) 0);
		ps_slru_exit_registered = true;
	}

	ps_slru_debt_persist();

	if (ps_slru_lost > ps_slru_lost_reported)
	{
		ereport(WARNING,
				(errmsg("pagestore: lost capture of %llu SLRU page image(s); the live mirror is incomplete",
						(unsigned long long) (ps_slru_lost - ps_slru_lost_reported)),
				 errdetail("More distinct SLRU pages were flushed between drains than the staging queue holds.")));
		ps_slru_lost_reported = ps_slru_lost;
	}

	if (ps_slru_queue_count == 0 && ps_slru_recap_count == 0)
	{
		/*
		 * Nothing staged here, but the watermark tail below must still run:
		 * this very flush cycle may have set a new candidate (a checkpoint
		 * with no dirty SLRU pages, or whose images drained earlier), and
		 * if every process is idle no other drain will come to publish it.
		 */
		ps_slru_wm_republish_pending();
		ps_slru_wm_advance();
		return;
	}

	PG_TRY();
	{
		TimestampTz drain_start = GetCurrentTimestamp();
		bool		posted;
		bool		budget_out = false;

		/*
		 * Service recaptures first: they re-enter the queue as fresh
		 * captures so the shipping loop below handles them uniformly.
		 */
		(void) ps_slru_service_recaptures(drain_start, &budget_out);

post_again:
		posted = false;

		/* Phase one: post.  Entries stay staged until the store syncs. */
		for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY; i++)
		{
			PsSlruPending *p = &ps_slru_queue[i];
			PageStoreRelKey key = {0};
			XLogRecPtr	version;

			if (!p->used)
				continue;

			if (TimestampDifferenceExceeds(drain_start, GetCurrentTimestamp(),
										   PS_SLRU_DRAIN_BUDGET_MS))
			{
				budget_out = true;
				break;
			}

			/*
			 * A retry of an already-posted entry must reuse the posted
			 * version verbatim (the bytes are frozen to match).  A fresh
			 * post is versioned by its capture-time bound after shared
			 * reservation, so same-page equal samples become strictly
			 * ordered before they reach the store.
			 */
			version = p->posted_fence;
			if (XLogRecPtrIsInvalid(version))
			{
				version = p->bound;
				Assert(!XLogRecPtrIsInvalid(version));
			}

			/*
			 * WAL-before-data: never let another compute observe a status
			 * bit whose WAL is not durable.  Same order the local write
			 * path enforces (SlruPhysicalWritePage's XLogFlush): flushing
			 * the fence covers every not-yet-durable bit (async commits
			 * set group LSNs; synchronous ones flushed at commit).  The
			 * version may exceed the flushed position -- it is a key, the
			 * fence is the durability obligation.  (In recovery the
			 * replayed WAL is already durable.)
			 */
			if (!RecoveryInProgress())
			{
				XLogRecPtr	flushto = ps_slru_flush_pos(p->fence_lsn);

				if (GetFlushRecPtr(NULL) < flushto)
					XLogFlush(flushto);
			}

			/*
			 * Freeze BEFORE the bytes can reach the daemon: if obj_write
			 * times out or errors mid-flight, the request may still be
			 * applied later, and only an already-set posted_fence keeps
			 * the write hook from mutating the entry into a same-version,
			 * different-bytes retry.
			 */
			p->posted_fence = version;

			ps_slru_obj_key(&key, p->obj);
			pagestore_localsvc_obj_write_timeout(PS_KLASS_SLRU_LIVE, &key,
												 (BlockNumber) p->pageno,
												 p->image,
												 (uint64) version,
												 PS_SLRU_SHIP_TIMEOUT_MS);
			p->shipped = true;
			posted = true;
		}

		/*
		 * Phase two: durability barrier, then pop.  The images only count
		 * once the daemon has synced them; nothing was consumed above, so
		 * any failure (including the sync itself) leaves every entry
		 * staged -- bytes frozen at their posted version -- for the next
		 * drain point (the watermark's per-process pending floor is derived
		 * from what is still staged, so it cannot advance over a commit the
		 * store could still lose).  A delayed image is a visibility delay,
		 * never a wrong answer.
		 */
		if (posted)
			pagestore_localsvc_store_sync_timeout(PS_SLRU_SHIP_TIMEOUT_MS);

		for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY && ps_slru_queue_count > 0; i++)
		{
			PsSlruPending *p = &ps_slru_queue[i];

			if (!p->used || !p->shipped)
				continue;
			p->used = false;
			p->shipped = false;
			p->posted_fence = InvalidXLogRecPtr;
			ps_slru_queue_count--;
		}

		/*
		 * Recaptures blocked behind posted entries get one same-drain retry
		 * after those entries sync and pop.  This matters most for exit
		 * drains: if the daemon is reachable again, do not count a recapture
		 * as lost merely because it was blocked at the start of the drain.
		 */
		if (!budget_out && ps_slru_recap_count > 0 &&
			ps_slru_queue_count < PS_SLRU_QUEUE_CAPACITY)
		{
			if (ps_slru_service_recaptures(drain_start, &budget_out))
				goto post_again;
		}

		if (budget_out)
			ereport(WARNING,
					(errmsg("pagestore: SLRU mirror drain ran out of time, deferring %d staged image(s) to the next ship point",
							ps_slru_queue_count)));
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext ecxt = MemoryContextSwitchTo(drain_cxt);

		/*
		 * Whatever failed, entries posted in this drain were not confirmed
		 * synced: clear the shipped marks (on every exit path, including
		 * the interrupt rethrow) so no later drain pops them off the back
		 * of a sync that did not cover a repost.  posted_fence stays --
		 * the post may have reached the daemon, so the bytes remain
		 * frozen.
		 */
		for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY; i++)
			ps_slru_queue[i].shipped = false;

		edata = CopyErrorData();
		MemoryContextSwitchTo(ecxt);

		/*
		 * Interrupts are not mirror failures; propagate them -- except in
		 * post-commit/exit context, where an escaping ERROR would turn an
		 * already-durable COMMIT (or the exit path) into a reported
		 * failure.  There the interrupt is re-armed for the next
		 * CHECK_FOR_INTERRUPTS() instead of being lost with the error.
		 */
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		{
			if (!ps_slru_no_rethrow)
			{
				FreeErrorData(edata);
				PG_RE_THROW();
			}
			if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
				QueryCancelPending = true;
			else
				ProcDiePending = true;
			InterruptPending = true;
		}
		FreeErrorData(edata);

		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore: SLRU mirror drain failed; staged images kept for the next ship point")));
	}
	PG_END_TRY();

	/*
	 * Whatever happened above, republish this process's pending floor from
	 * the queue's current contents and try to move the watermark.
	 */
	ps_slru_wm_republish_pending();
	ps_slru_wm_advance();
}

/* Exposed drain point: the control mirror's flush hook chains into this. */
void
pagestore_slru_mirror_drain(void)
{
	if (!ps_slru_mirror_enabled)
		return;
	if (CritSectionCount > 0)
		return;
	ps_slru_drain();
}

/* before_shmem_exit: last chance to ship what this process staged. */
static void
ps_slru_exit_drain(int code, Datum arg)
{
	if (code == 0)
	{
		ps_slru_no_rethrow = true;
		pagestore_slru_mirror_drain();
		ps_slru_no_rethrow = false;
	}

	/*
	 * Whatever the drain could not ship dies with this process; the pages
	 * are clean locally, so no later flush will re-stage them.  That is a
	 * coverage loss, not a deferral: count it (durably) so the watermark
	 * freezes rather than advancing over status the mirror will never
	 * carry, and clear our pending slot (a dead process must not hold the
	 * floor forever -- the loss accounting is what keeps this honest).
	 */
	if (ps_slru_queue_count > 0 || ps_slru_recap_count > 0)
	{
		int			n = ps_slru_queue_count + ps_slru_recap_count;

		ps_slru_lost += n;
		if (ps_slru_wm != NULL)
		{
			pg_atomic_fetch_add_u64(&ps_slru_wm->total_lost, n);
			pg_atomic_fetch_add_u64(&ps_slru_wm->stats_lost, n);
			pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
		}
		ereport(WARNING,
				(errmsg("pagestore: exiting with %d unshipped SLRU image(s); the live mirror is missing them",
						n)));
	}
	ps_slru_debt_persist();
	if (ps_slru_wm != NULL && MyProcNumber != INVALID_PROC_NUMBER &&
		MyProcNumber < ps_slru_wm_nprocs)
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].floor, 0);
}

/*
 * Transaction-end drain: an ordinary backend that evicted (and thus wrote
 * and staged) SLRU pages mid-transaction has no other ship point.  Fires
 * outside critical sections.
 */
static void
ps_slru_xact_drain(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PREPARE)
		return;					/* PREPARE ends the transaction too; the
								 * COMMIT/ROLLBACK PREPARED may run in a
								 * different backend, so ship now */
	ps_slru_no_rethrow = true;
	pagestore_slru_mirror_drain();
	ps_slru_no_rethrow = false;
}

/*
 * pagestore_slru_mirror_stats() returns (staged int, recapture int, lost bigint)
 *
 * Observability for tests and the watermark follow-up: what this backend
 * currently holds staged and whether it ever lost coverage.
 */
PG_FUNCTION_INFO_V1(pagestore_slru_mirror_stats);
Datum
pagestore_slru_mirror_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int32GetDatum(ps_slru_queue_count);
	values[1] = Int32GetDatum(ps_slru_recap_count);
	values[2] = Int64GetDatum(ps_slru_wm != NULL
							  ? (int64) pg_atomic_read_u64(&ps_slru_wm->stats_lost)
							  : (int64) ps_slru_lost);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * pagestore_slru_tombstone_asof(slru text, lsn pg_lsn) returns bigint
 *
 * The newest truncation cutoff page published at/below 'lsn' for an SLRU,
 * or NULL if none: pages below the cutoff are dead as of that LSN and a
 * live-mirror reader must not serve them, whatever images exist.
 */
PG_FUNCTION_INFO_V1(pagestore_slru_tombstone_asof);
Datum
pagestore_slru_tombstone_asof(PG_FUNCTION_ARGS)
{
	char	   *slru = text_to_cstring(PG_GETARG_TEXT_PP(0));
	XLogRecPtr	lsn = PG_GETARG_LSN(1);
	PageStoreRelKey key;
	uint32		obj;
	char		page[BLCKSZ];
	int64		cutoff;

	if (!ps_slru_dir_obj(slru, &obj))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not an in-scope SLRU directory", slru)));

	ps_slru_obj_key(&key, obj);
	if (!pagestore_localsvc_obj_read_at(PS_KLASS_SLRU_TOMB, &key, 0,
										(uint64) lsn, page, NULL))
		PG_RETURN_NULL();
	memcpy(&cutoff, page, sizeof(int64));
	PG_RETURN_INT64(cutoff);
}

/*
 * pagestore_slru_mirror_truncate(slru text, cutoff bigint) returns void
 *
 * Operations/test entry point: publish a durable truncation tombstone
 * through the same path the slru_truncate_hook consumer uses.  Superuser
 * only -- a bogus cutoff makes readers treat live pages as dead.
 */
PG_FUNCTION_INFO_V1(pagestore_slru_mirror_truncate);
Datum
pagestore_slru_mirror_truncate(PG_FUNCTION_ARGS)
{
	char	   *slru = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int64		cutoff = PG_GETARG_INT64(1);
	uint32		obj;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to publish an SLRU tombstone")));
	if (!ps_slru_dir_obj(slru, &obj))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not an in-scope SLRU directory", slru)));
	if (cutoff < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cutoff page must be non-negative")));

	{
		XLogRecPtr	version = ps_slru_now_lsn();

		ps_slru_wm_note_pending(version);
		PG_TRY();
		{
			ps_slru_ship_tombstone(obj, cutoff, version);
		}
		PG_FINALLY();
		{
			ps_slru_wm_republish_pending();
		}
		PG_END_TRY();
	}
	PG_RETURN_VOID();
}

/*
 * pagestore_slru_mirror_watermark() returns pg_lsn
 *
 * The mirrored_status_lsn: every in-scope SLRU status change at/below it is
 * durably mirrored.  NULL while no completed checkpoint's flush cycle has
 * durably shipped yet (or while a coverage loss keeps it frozen at zero).
 */
PG_FUNCTION_INFO_V1(pagestore_slru_mirror_watermark);
Datum
pagestore_slru_mirror_watermark(PG_FUNCTION_ARGS)
{
	uint64		w;

	if (ps_slru_wm == NULL)
		PG_RETURN_NULL();
	w = pg_atomic_read_u64(&ps_slru_wm->watermark);
	if (w == 0)
		PG_RETURN_NULL();
	PG_RETURN_LSN((XLogRecPtr) w);
}

/*
 * pagestore_slru_mirror_reset_debt() returns bigint
 *
 * Operator escape hatch: declare the mirror re-primed after a coverage
 * loss.  Removes the debt marker and zeroes the loss counter, letting the
 * watermark advance again from the next completed checkpoint on.  Only
 * legitimate after the mirror has actually been made whole (e.g. re-seeded
 * from a fresh snapshot of this compute); resetting over a real hole makes
 * other computes trust status the store never received.  Superuser only.
 * Returns the number of losses that were outstanding.
 */
PG_FUNCTION_INFO_V1(pagestore_slru_mirror_reset_debt);
Datum
pagestore_slru_mirror_reset_debt(PG_FUNCTION_ARGS)
{
	uint64		lost;

	int			fd;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to reset the SLRU mirror debt")));
	if (ps_slru_wm == NULL)
		ereport(ERROR,
				(errmsg("the SLRU mirror is not active")));

	lost = pg_atomic_read_u64(&ps_slru_wm->total_lost);

	/*
	 * Priming and forgiving are one act: the operator declares the mirror
	 * whole as of now.  Make the primed marker durable first, then drop
	 * the debt marker, then retire the counted losses -- with a CAS, so a
	 * loss racing this reset survives it (the CAS fails and the debt
	 * marker is re-created by the next drain via debt_unpersisted).
	 */
	fd = OpenTransientFile(PS_SLRU_PRIMED_FILE,
						   O_WRONLY | O_CREAT | PG_BINARY);
	if (fd < 0 || pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create primed marker \"%s\": %m",
						PS_SLRU_PRIMED_FILE)));
	CloseTransientFile(fd);

	/*
	 * Priming repairs tombstone state too: republish each SLRU's cutoff
	 * from local truth, superseding any tombstone whose truncation never
	 * reached the WAL (see ps_slru_tomb_rederive).  Failures abort the
	 * reset before any debt is forgiven.
	 */
	ps_slru_tomb_rederive();

	if (unlink(PS_SLRU_DEBT_FILE) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove debt marker \"%s\": %m",
						PS_SLRU_DEBT_FILE)));
	PG_TRY();
	{
		fsync_fname(".", true);
	}
	PG_CATCH();
	{
		/*
		 * The unlink may have reached storage before directory fsync failed.
		 * Recreate the marker immediately before propagating the error.
		 */
		pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
		ps_slru_debt_persist();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Discard the standing candidate before unfreezing debt: it may stem
	 * from a checkpoint that completed while the mirror was frozen and
	 * unprimed, and a concurrent drain must not publish it after the CAS.
	 */
	pg_atomic_write_u64(&ps_slru_wm->candidate, 0);

	/*
	 * The flag is cleared BEFORE the CAS: a loss that lands after the CAS
	 * sets it again (its ordering is add-then-set), whereas clearing after
	 * the CAS could erase that racing loss's flag and leave its debt
	 * unpersisted.  If the CAS itself fails the flag is restored.
	 */
	pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 0);
	if (!pg_atomic_compare_exchange_u64(&ps_slru_wm->total_lost, &lost, 0))
	{
		/*
		 * A loss raced the reset and the debt marker is already unlinked:
		 * re-create it here and now, not merely re-flag it -- this backend
		 * may never drain again before a clean shutdown.
		 */
		pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
		ps_slru_debt_persist();
		ereport(ERROR,
				(errmsg("a new SLRU mirror loss arrived during the reset"),
				 errhint("Re-prime the mirror and retry.")));
	}
	PG_RETURN_INT64((int64) lost);
}

/*
 * pagestore_slru_live_read_at(slru text, pageno int, lsn pg_lsn) returns bytea
 *
 * Read the newest live-mirrored image of an SLRU page with version <= lsn,
 * or NULL if none.  Distinct from pagestore_slru_read_at, which reads the
 * PS_KLASS_SLRU seed-snapshot keyspace.
 */
PG_FUNCTION_INFO_V1(pagestore_slru_live_read_at);
Datum
pagestore_slru_live_read_at(PG_FUNCTION_ARGS)
{
	char	   *slru = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		pageno = PG_GETARG_INT32(1);
	XLogRecPtr	lsn = PG_GETARG_LSN(2);
	PageStoreRelKey key;
	uint32		obj;
	char	   *out = palloc(BLCKSZ);
	bytea	   *result;

	if (!ps_slru_mirror_enabled)
		ereport(ERROR,
				(errmsg("the SLRU live mirror is not active"),
				 errhint("Set pagestore.slru_mirror = on (with the localsvc backend).")));
	if (!ps_slru_dir_obj(slru, &obj))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not an in-scope SLRU directory", slru)));
	if (pageno < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("page number must be non-negative")));

	ps_slru_obj_key(&key, obj);
	if (!pagestore_localsvc_obj_read_at(PS_KLASS_SLRU_LIVE, &key,
										(BlockNumber) pageno,
										(uint64) lsn, out, NULL))
		PG_RETURN_NULL();

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), out, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * Install the write-side capture.  Called from the module's _PG_init();
 * engages only under the localsvc backend and the GUC.
 */
void
pagestore_slru_mirror_init(bool localsvc_active)
{
	DefineCustomBoolVariable("pagestore.slru_mirror",
							 "Mirror flushed SLRU page images to the page store.",
							 "Write-side capture for live multi-compute SLRU sharing; "
							 "requires the localsvc backend.",
							 &pagestore_slru_mirror,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	for (int i = 0; i < (int) lengthof(ps_slru_dirmap); i++)
		ps_slru_dirmap[i].obj = pagestore_slru_klass_id(ps_slru_dirmap[i].dir);

	if (!pagestore_slru_mirror)
		return;
	if (!localsvc_active)
		ereport(ERROR,
				(errmsg("pagestore.slru_mirror requires pagestore.backend = 'localsvc'")));

	ps_slru_mirror_enabled = true;
	prev_slru_page_write_hook = slru_page_write_hook;
	slru_page_write_hook = ps_slru_write_hook;
	slru_truncate_hook = ps_slru_truncate_hook;
	RegisterXactCallback(ps_slru_xact_drain, NULL);

	/* watermark, floors, and shared stats all live in shared memory */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ps_slru_shmem_request;
	prev_slru_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ps_slru_shmem_startup;
}
