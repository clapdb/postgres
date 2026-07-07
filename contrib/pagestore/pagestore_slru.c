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

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/slru.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "access/xact.h"
#include "access/xlog.h"
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
	XLogRecPtr	fence_lsn;		/* upper bound of the image's contents; the
								 * version floor and what the drain must
								 * XLogFlush() */
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
} PsSlruRecapture;

static PsSlruRecapture ps_slru_recap[PS_SLRU_RECAP_CAPACITY];
static int	ps_slru_recap_count = 0;

/*
 * Pages whose capture was lost outright (queue AND recapture table full, or
 * an un-keyable page number).  Cumulative; the watermark follow-up reads
 * this to refuse advancing over an incomplete mirror.
 */
static uint64 ps_slru_lost = 0;
static uint64 ps_slru_lost_reported = 0;

static bool ps_slru_exit_registered = false;

static void ps_slru_exit_drain(int code, Datum arg);
static void ps_slru_xact_drain(XactEvent event, void *arg);
static XLogRecPtr ps_slru_now_lsn(void);
static void ps_slru_debt_persist(void);
static void ps_slru_wm_note_lost(void);

#define PS_SLRU_SHIPPED_SLOTS	1024	/* power of two */

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
 * Image versions are real WAL positions -- the image's capture-time fence,
 * lifted at ship time above every version the page has ever shipped at
 * (last_shipped, a shared CAS-max table).  Two images of the same page
 * never carry the same version, so the later capture -- whose bytes are a
 * superset, SLRU pages being accretive -- always outranks the earlier one,
 * across processes too.  A group-LSN fence alone cannot promise that
 * (recomputed fences can shrink on eviction/reload, and captures in
 * different processes can race), and a newer image shadowed by an older
 * one at a higher version would un-mirror status the watermark already
 * vouched for.  Lifting never exceeds the current WAL position: when the
 * floor has caught up with the insert pointer, the entry is DEFERRED to a
 * later drain (its pending floor keeps the watermark honest meanwhile, and
 * any WAL insertion -- at latest the next checkpoint record -- unblocks
 * it).  Versions therefore stay comparable to LSNs (as-of reads, tombstone
 * ordering) and stay ordered across restarts without any persistent
 * allocator state: WAL positions only grow.
 *
 * The local commit is never held back -- only its visibility to other
 * computes waits for the mirror.
 */
typedef struct PsSlruWatermarkShm
{
	pg_atomic_uint64 watermark;
	pg_atomic_uint64 candidate;
	pg_atomic_uint64 total_lost;
	pg_atomic_uint32 debt_unpersisted;	/* a loss awaits the marker file */

	/*
	 * Highest version ever shipped, per page-hash slot (conservative on
	 * collisions: a too-high floor only defers a ship, never a wrong
	 * version).  CAS-max before every post.
	 */
	pg_atomic_uint64 last_shipped[PS_SLRU_SHIPPED_SLOTS];

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

/* Map an SLRU dir to its object id; false = out of scope, do not mirror. */
static bool
ps_slru_dir_obj(const char *dir, uint32 *obj)
{
	for (int i = 0; i < (int) lengthof(ps_slru_dirmap); i++)
	{
		if (strcmp(ps_slru_dirmap[i].dir, dir) == 0)
		{
			*obj = ps_slru_dirmap[i].obj;
			return true;
		}
	}
	return false;
}

static void
ps_slru_obj_key(PageStoreRelKey *key, uint32 obj)
{
	key->spcOid = 0;
	key->dbOid = 0;
	key->relNumber = (RelFileNumber) obj;
	key->forkNum = 0;
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
		pg_atomic_init_u32(&ps_slru_wm->debt_unpersisted, debt ? 1 : 0);
		for (int i = 0; i < PS_SLRU_SHIPPED_SLOTS; i++)
			pg_atomic_init_u64(&ps_slru_wm->last_shipped[i], 0);
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
 * The shipped-version floor for a page: the highest version any image of
 * any page hashing to its slot was ever posted at.  Conservative on
 * collisions -- a too-high floor only defers a ship, never mis-versions
 * one.
 */
static inline int
ps_slru_shipped_slot(uint32 obj, uint32 pageno)
{
	uint32		h = obj ^ (pageno * 2654435761u);

	return (int) (h & (PS_SLRU_SHIPPED_SLOTS - 1));
}

static uint64
ps_slru_shipped_floor(uint32 obj, uint32 pageno)
{
	if (ps_slru_wm == NULL)
		return 0;
	return pg_atomic_read_u64(&ps_slru_wm->last_shipped[ps_slru_shipped_slot(obj, pageno)]);
}

static void
ps_slru_shipped_note(uint32 obj, uint32 pageno, uint64 version)
{
	pg_atomic_uint64 *slot;

	if (ps_slru_wm == NULL)
		return;
	slot = &ps_slru_wm->last_shipped[ps_slru_shipped_slot(obj, pageno)];
	for (;;)
	{
		uint64		cur = pg_atomic_read_u64(slot);

		if (version <= cur || pg_atomic_compare_exchange_u64(slot, &cur, version))
			break;
	}
}

/*
 * Republish this process's pending floor after a drain: 0 when everything
 * staged has durably shipped, else the minimum fence still held.
 */
static void
ps_slru_wm_republish_pending(void)
{
	uint64		f = 0;

	if (ps_slru_wm == NULL || MyProcNumber == INVALID_PROC_NUMBER ||
		MyProcNumber >= ps_slru_wm_nprocs)
		return;

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
		if (pg_atomic_read_u64(&ps_slru_wm->pending[i].floor) == 0)
			continue;

		/*
		 * A floor whose owner is gone is a coverage loss no exit path
		 * accounted for (e.g. a backend killed after staging but before
		 * its first drain registered the exit callback).  Convert it --
		 * the loss freezes the watermark -- and clear the slot so it does
		 * not read as pending forever.
		 */
		if (kill((pid_t) pg_atomic_read_u64(&ps_slru_wm->pending[i].pid), 0) != 0 &&
			errno == ESRCH)
		{
			ps_slru_wm_note_lost();
			pg_atomic_write_u64(&ps_slru_wm->pending[i].floor, 0);
			pg_atomic_write_u64(&ps_slru_wm->pending[i].pid, 0);
			continue;
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
ps_slru_note_recapture(SlruDesc *ctl, uint32 obj, uint32 pageno)
{
	for (int i = 0; i < PS_SLRU_RECAP_CAPACITY; i++)
	{
		PsSlruRecapture *r = &ps_slru_recap[i];

		if (r->used && r->obj == obj && r->pageno == pageno)
			return;				/* already scheduled */
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
			ps_slru_recap_count++;
			ps_slru_wm_note_pending(InvalidXLogRecPtr);
			return;
		}
	}

	/* Table full: coverage lost; the watermark side must fail conservative. */
	ps_slru_lost++;
	ps_slru_wm_note_lost();
}

/*
 * slru_page_write_hook consumer.  Bank lock held; must be infallible: fixed
 * pre-reserved storage only, no locks, no allocation, no elog (atomics are
 * fine, which is all the fence/stamp computation needs).
 */
static void
ps_slru_write_hook(SlruDesc *ctl, int64 pageno, const char *page,
				   XLogRecPtr fence_lsn)
{
	uint32		obj;
	int			free_slot = -1;

	if (!ps_slru_dir_obj(ctl->options.Dir, &obj))
		return;					/* out-of-scope SLRU: excluded, not mirrored */

	if (pageno < 0 || pageno > (int64) PG_UINT32_MAX)
	{
		/* cannot be keyed (store block numbers are uint32); count the loss */
		ps_slru_lost++;
		ps_slru_wm_note_lost();
		return;
	}

	/*
	 * A write that carries no fence (SLRUs without group LSNs, redo-driven
	 * writes) is bounded at CAPTURE time: the bytes certainly contain
	 * nothing past the current WAL position.  Stamping at drain time
	 * instead would be wrong -- the page can be dirtied again between this
	 * flush and the drain, and a drain-time position would falsely cover
	 * those later changes.  (Reading the insert position is atomics/
	 * spinlock only, fine under the bank lock.)
	 */
	if (XLogRecPtrIsInvalid(fence_lsn))
		fence_lsn = ps_slru_now_lsn();

	for (int i = 0; i < PS_SLRU_QUEUE_CAPACITY; i++)
	{
		PsSlruPending *p = &ps_slru_queue[i];

		if (p->used)
		{
			if (p->obj == obj && p->pageno == (uint32) pageno)
			{
				/*
				 * Same page staged again before a drain.  If a post of this
				 * entry may have reached the daemon (a previous drain timed
				 * out or failed after obj_write), the bytes must stay
				 * exactly what was posted: the store resolves same-version
				 * appends by arrival order, and an abandoned request can
				 * land after our retry -- byte-identical duplicates make
				 * that order irrelevant.  The newer bytes become a
				 * recapture instead, re-snapshotted at drain time and
				 * shipped strictly above the posted version (the
				 * last-shipped floor guarantees it).
				 */
				if (!XLogRecPtrIsInvalid(p->posted_fence))
				{
					ps_slru_note_recapture(ctl, obj, (uint32) pageno);
					return;
				}

				/*
				 * Keep the newest bytes.  The fence only ever needs to
				 * grow -- a smaller recomputed fence (group LSNs reset on
				 * eviction/reload) must not un-fence bits the older image
				 * already carried.
				 */
				memcpy(p->image, page, BLCKSZ);
				if (p->fence_lsn < fence_lsn)
					p->fence_lsn = fence_lsn;
				ps_slru_wm_note_pending(p->fence_lsn);
				return;
			}
		}
		else if (free_slot < 0)
			free_slot = i;
	}

	if (free_slot >= 0)
	{
		PsSlruPending *p = &ps_slru_queue[free_slot];

		p->used = true;
		p->shipped = false;
		p->obj = obj;
		p->pageno = (uint32) pageno;
		p->fence_lsn = fence_lsn;
		p->posted_fence = InvalidXLogRecPtr;
		memcpy(p->image, page, BLCKSZ);
		ps_slru_queue_count++;
		ps_slru_wm_note_pending(fence_lsn);
		return;
	}

	/* Queue full: record the page identity for recapture at drain time. */
	ps_slru_note_recapture(ctl, obj, (uint32) pageno);
}

/*
 * A conservative "now" to version images that carry no fence: their bytes
 * certainly contain nothing past the current WAL position.
 */
static XLogRecPtr
ps_slru_now_lsn(void)
{
	if (RecoveryInProgress())
	{
		XLogRecPtr	p = GetXLogReplayRecPtr(NULL);

		/*
		 * Early in recovery (before the first record is applied) the
		 * replay pointer may not be set yet; fall back down the chain.  A
		 * too-low bound here is safe: an unclean boot carries boot debt,
		 * so the watermark is frozen until the mirror is re-primed, and a
		 * low-versioned image can at worst be outranked by an existing
		 * store image whose own claims still hold.
		 */
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
			*fence = f;
			found = true;
			break;
		}
	}
	LWLockRelease(banklock);

	if (found)
	{
		/*
		 * The copy raced concurrent status updates (LW_SHARED admits
		 * write-OK setters under some SLRUs' protocols), so the group-LSN
		 * fence read afterwards may not cover the newest bit.  Stamp "now"
		 * instead of trusting it; visibility is only delayed.
		 */
		*fence = ps_slru_now_lsn();
		return true;
	}

	/* Evicted: the bytes were flushed locally; read the segment file. */
	{
		char		path[MAXPGPATH];
		int64		segno = (int64) r->pageno / SLRU_PAGES_PER_SEGMENT;
		int			rpageno = (int) ((int64) r->pageno % SLRU_PAGES_PER_SEGMENT);
		off_t		offset = (off_t) rpageno * BLCKSZ;
		int			fd;
		ssize_t		n;

		if (ctl->options.long_segment_names)
			snprintf(path, MAXPGPATH, "%s/%015" PRIX64, ctl->options.Dir, segno);
		else
			snprintf(path, MAXPGPATH, "%s/%04X", ctl->options.Dir,
					 (unsigned int) segno);

		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			return false;
		n = pg_pread(fd, image, BLCKSZ, offset);
		CloseTransientFile(fd);
		if (n != BLCKSZ)
			return false;
		*fence = ps_slru_now_lsn();
		return true;
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
		bool		posted = false;
		bool		budget_out = false;

		/*
		 * Service recaptures first: they re-enter the queue as fresh
		 * captures so the shipping loop below handles them uniformly.
		 */
		for (int i = 0; i < PS_SLRU_RECAP_CAPACITY && ps_slru_recap_count > 0; i++)
		{
			PsSlruRecapture *r = &ps_slru_recap[i];
			char		image[BLCKSZ];
			XLogRecPtr	fence;

			if (!r->used)
				continue;
			if (ps_slru_queue_count >= PS_SLRU_QUEUE_CAPACITY)
				break;			/* queue refilled; retry next drain */

			/*
			 * While the identity's posted entry is still awaiting its sync,
			 * the recapture must wait: the write hook would route it right
			 * back here.  The entry pops this drain (or a later one) and
			 * the recapture proceeds at the next.
			 */
			if (ps_slru_queue_holds_posted(r->obj, r->pageno))
				continue;
			if (!ps_slru_recapture_page(r, image, &fence))
				continue;		/* keep for the next drain */

			/*
			 * Re-entering through the hook re-stages the page as a fresh
			 * capture; the last-shipped floor at post time lifts it
			 * strictly above any version this page was ever posted under,
			 * so the recaptured bytes can never be shadowed by an
			 * abandoned same-version request still in the daemon's
			 * pipeline.
			 */
			ps_slru_write_hook(r->ctl, (int64) r->pageno, image, fence);
			r->used = false;
			ps_slru_recap_count--;
		}

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
			 * post is versioned by its capture-time fence, lifted above
			 * every version its page ever shipped at (last_shipped): two
			 * images of one page must never carry the same version, or
			 * the store's arrival order would decide which bytes win.
			 * The lift must not exceed the current WAL position, though
			 * -- versions stay LSN-comparable for as-of readers and
			 * tombstone ordering -- so when the floor has caught up with
			 * the insert pointer the entry is DEFERRED: its pending floor
			 * keeps the watermark honest, and any WAL insertion (at
			 * latest the next checkpoint record) unblocks it.
			 */
			version = p->posted_fence;
			if (XLogRecPtrIsInvalid(version))
			{
				uint64		floor = ps_slru_shipped_floor(p->obj, p->pageno);

				version = p->fence_lsn;
				Assert(!XLogRecPtrIsInvalid(version));
				if ((uint64) version <= floor)
				{
					XLogRecPtr	now = ps_slru_now_lsn();

					if (floor >= (uint64) now)
						continue;	/* deferred until the WAL advances */
					version = (XLogRecPtr) (floor + 1);
				}
			}

			/*
			 * WAL-before-data: never let another compute observe a status
			 * bit whose WAL is not durable.  Same order the local write
			 * path enforces (SlruPhysicalWritePage's XLogFlush).  The
			 * fence is the content bound; the lifted version never exceeds
			 * the insert position, so flushing to Max(fence, version) is
			 * always a real, reachable WAL position.  (In recovery the
			 * replayed WAL is already durable.)
			 */
			if (!RecoveryInProgress() && GetFlushRecPtr(NULL) < version)
				XLogFlush(version);

			/*
			 * Freeze BEFORE the bytes can reach the daemon: if obj_write
			 * times out or errors mid-flight, the request may still be
			 * applied later, and only an already-set posted_fence keeps
			 * the write hook from mutating the entry into a same-version,
			 * different-bytes retry.  The last-shipped floor is raised at
			 * the same point, for the same reason.
			 */
			p->posted_fence = version;
			ps_slru_shipped_note(p->obj, p->pageno, (uint64) version);

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

		/* interrupts are not mirror failures; see the control mirror */
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		{
			FreeErrorData(edata);
			PG_RE_THROW();
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
		pagestore_slru_mirror_drain();

	/*
	 * Whatever is still staged dies with this process; the pages are clean
	 * locally, so no later flush will re-stage them.  That is a coverage
	 * loss, not a deferral: count it so the watermark freezes rather than
	 * advancing over status the mirror will never carry, and clear our
	 * pending slot (a dead process must not hold the floor forever -- the
	 * loss accounting is what keeps this honest).
	 */
	if (ps_slru_queue_count > 0 || ps_slru_recap_count > 0)
	{
		ps_slru_lost += ps_slru_queue_count + ps_slru_recap_count;
		if (ps_slru_wm != NULL)
		{
			pg_atomic_fetch_add_u64(&ps_slru_wm->total_lost,
									ps_slru_queue_count + ps_slru_recap_count);
			pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
		}
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
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT)
		return;
	pagestore_slru_mirror_drain();
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
	values[2] = Int64GetDatum((int64) ps_slru_lost);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
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

	if (unlink(PS_SLRU_DEBT_FILE) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove debt marker \"%s\": %m",
						PS_SLRU_DEBT_FILE)));
	fsync_fname(".", true);

	if (!pg_atomic_compare_exchange_u64(&ps_slru_wm->total_lost, &lost, 0))
		ereport(ERROR,
				(errmsg("a new SLRU mirror loss arrived during the reset"),
				 errhint("Re-prime the mirror and retry.")));
	pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 0);
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
	slru_page_write_hook = ps_slru_write_hook;
	RegisterXactCallback(ps_slru_xact_drain, NULL);

	/* the visibility watermark lives in shared memory */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ps_slru_shmem_request;
	prev_slru_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ps_slru_shmem_startup;
}
