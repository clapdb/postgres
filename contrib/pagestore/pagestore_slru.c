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
 * Versioning: an image is versioned by a real WAL/replay position sampled at
 * capture time under the page's bank lock.  The drain separately calls
 * XLogFlush(fence) before shipping (the same WAL-before-data order the local
 * SlruPhysicalWritePage() enforces), so a mirrored image never advertises a
 * commit whose WAL is not durable.  We never invent versions above WAL: if a
 * page already has a capture at the sampled position (including a high-water
 * mark recovered from the store after restart), the newer bytes are deferred
 * to recapture until WAL advances.
 *
 * These live images are keyed PS_KLASS_SLRU_LIVE, timeline-scoped, and
 * deliberately NOT
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
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/message.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "pagestore_backend.h"
#include "varatt.h"

/* GUC: engage the live SLRU mirror (requires the localsvc backend) */
static bool pagestore_slru_mirror = false;

/* GUC: serve SLRU reads from another compute's live mirror */
static bool pagestore_slru_live_reads = false;

static bool ps_slru_mirror_enabled = false;
static bool ps_slru_live_reads_enabled = false;
static slru_page_write_hook_type prev_slru_page_write_hook = NULL;
static slru_truncate_hook_type prev_slru_truncate_hook = NULL;
static slru_page_read_hook_type prev_slru_page_read_hook = NULL;
static slru_page_exists_hook_type prev_slru_page_exists_hook = NULL;
static slru_page_revalidate_hook_type prev_slru_page_revalidate_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

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
	SlruDesc   *ctl;
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
	XLogRecPtr	fence_floor;	/* recaptured image must be versioned by a
								 * real WAL/replay position strictly above
								 * this (a same-version post with different
								 * bytes may still be in flight); Invalid =
								 * no constraint */
	bool		loss_counted;
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

/*
 * PID that registered this process's exit drain.  Keyed by PID, not a bool:
 * _PG_init runs in the postmaster, whose forked children inherit the static
 * but get their before_shmem_exit list cleared, so a bool would make every
 * child skip registration forever.
 */
static int	ps_slru_exit_registered_pid = 0;

/*
 * True while draining from a place where an ERROR must not escape: the
 * transaction-end callback (the transaction is already committed/prepared;
 * rethrowing a cancel there would report failure for a durable commit) and
 * the exit callback.  A swallowed interrupt is re-armed instead, so it
 * fires at the next CHECK_FOR_INTERRUPTS().
 */
static bool ps_slru_no_rethrow = false;

static void ps_slru_exit_drain(int code, Datum arg);
static XLogRecPtr ps_slru_now_lsn(void);
static XLogRecPtr ps_slru_flush_pos(XLogRecPtr ptr);
static void ps_slru_rearm_interrupt(void);
static bool ps_slru_service_recaptures(TimestampTz drain_start, bool *budget_out);
static void ps_slru_debt_persist(void);
static void ps_slru_wm_note_lost(void);
static void ps_slru_wm_note_lost_count(uint64 n, bool new_loss);
static bool ps_slru_wm_claim_pending_slot(void);

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
 * Reader-side state (pagestore.slru_live_reads).
 *
 * The writer's watermark is published to the store (PS_KLASS_SLRU_WM) so a
 * reader on another compute can fetch it; the fetch is IPC, so it happens
 * only at the read/exists hooks (physical-read misses) and at transaction
 * boundaries, TTL-bounded, never under a bank lock.  The fetched watermark,
 * the newest known tombstone per SLRU, and the served-page epochs all live
 * in shared memory: SLRU buffers are shared, so a page one backend served
 * from the mirror is every backend's cache hit, and the revalidation state
 * that governs it must be shared too.
 *
 * Every page the read path DECIDED at watermark E (served from the mirror
 * or deliberately left to the local file) is remembered with that epoch;
 * the revalidate hook (bank lock held, memory-only) declares a cached slot
 * stale once the fetched watermark -- or the tombstone coverage of the
 * page's range -- has moved past its epoch, forcing one physical re-read
 * per page per epoch.  An unknown page (table churn) is treated as stale
 * -- one redundant re-read, never a stale answer.
 *
 * The live mirror has exactly ONE writer per branch timeline (the branch's
 * primary compute runs pagestore.slru_mirror; live-read computes do not).
 * Newest-image reads depend on that: each shipped image is one compute's
 * whole-page local view, and with two writers the newer stamp could lack
 * bits only the other writer's view carried.
 */
#define PS_SLRU_READER_WM_TTL_MS	1000

/*
 * How long cached pages may keep revalidating against the last successfully
 * fetched watermark/tombstones while fresh fetches keep failing.  Past this,
 * the revalidator calls every cached page stale, forcing physical re-reads
 * through the read hook's fail-closed paths -- a reader must not serve
 * possibly-truncated pages on old metadata indefinitely just because the
 * store is unreachable.
 */
#define PS_SLRU_READER_WM_STALE_MS	(3 * PS_SLRU_READER_WM_TTL_MS)
#define PS_SLRU_SERVED_CAPACITY 1024	/* power of two */

/* stats for tests/observability */
static uint64 ps_slru_read_served = 0;
static uint64 ps_slru_read_fallback = 0;

static inline int
ps_slru_served_slot(uint32 obj, uint32 pageno)
{
	uint32		h = obj ^ (pageno * 2654435761u);

	return (int) (h & (PS_SLRU_SERVED_CAPACITY - 1));
}

static inline uint64
ps_slru_served_tag(uint32 obj, uint32 pageno)
{
	return ((uint64) obj << 32) | (uint64) pageno;
}

/*
 * The visibility watermark (mirrored_status_lsn): an LSN W such that every
 * in-scope SLRU status change with WAL position <= W is durably mirrored.
 * A reader on another compute may trust the live mirror for status up to W
 * and no further.
 *
 * W is a completeness floor, NOT a read position: a status change <= W may
 * live only in an image whose version exceeds W (the image's version is its
 * capture-time WAL bound, and a checkpoint's flush cycle captures pages
 * while WAL keeps advancing past its redo).  A reader gates on W and then
 * takes the page's NEWEST image (read at max) -- SLRU page bytes are
 * cumulative, so the newest image carries every bit any older image had.
 * Reading AT W would silently miss bits carried only by a higher-versioned
 * image.
 *
 * W advances only over a contiguous durable prefix:
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
/* number of entries in ps_slru_dirmap; static-asserted below it */
#define PS_SLRU_SCOPE_COUNT		4

/*
 * A shared served-page decision: which epoch (fetched watermark) the last
 * physical read of an SLRU page was decided at.  Shared because the SLRU
 * buffers themselves are shared -- one backend's mirror-served page becomes
 * every backend's cache hit, so the revalidation epoch must be visible to
 * all of them, not just the reader that did the I/O.  Updated tag-last
 * (tag cleared first) with fully-barriered exchanges so a torn read can
 * only look like a mismatch, which counts as stale -- never a wrong epoch.
 */
typedef struct PsSlruServedShm
{
	pg_atomic_uint64 seq;		/* seqlock: even = stable, odd = writer in
								 * flight; advances on every update, so slot
								 * reuse (A -> B -> A) can never satisfy a
								 * reader's before/after check */
	pg_atomic_uint64 tag;		/* obj<<32 | pageno; 0 = empty */
	pg_atomic_uint64 epoch;
} PsSlruServedShm;

#define PS_SLRU_VERSION_SLOTS	4096
#define PS_SLRU_VERSION_PROBES	8

typedef struct PsSlruVersionSlot
{
	slock_t		mutex;
	bool		valid;
	uint32		obj;
	uint32		pageno;
	XLogRecPtr	version;
} PsSlruVersionSlot;

typedef struct PsSlruWatermarkShm
{
	pg_atomic_uint64 watermark;
	pg_atomic_uint64 candidate;
	pg_atomic_uint64 candidate_floor;	/* reject candidates at/below this:
										 * set by reset_debt so only
										 * checkpoints started after the
										 * reset can seed the watermark */
	pg_atomic_uint64 total_lost;
	pg_atomic_uint64 stats_lost;	/* raw loss events, for observability --
									 * unlike total_lost it is not seeded
									 * with boot debt and never reset */
	pg_atomic_uint64 loss_generation;
	pg_atomic_uint64 debt_generation;
	pg_atomic_uint64 pending_owner_next;
	pg_atomic_uint32 floors_set;	/* count of nonzero pending floors: lets
									 * the per-query drain tail skip the
									 * O(MaxBackends) slot scans when nothing
									 * is pending anywhere */
	pg_atomic_uint32 sweeps_active; /* dead-floor sweeps in flight: a sweep
									 * clears the floor BEFORE counting the
									 * loss (single-count CAS claim), so the
									 * advance path must not publish while
									 * one is between the two */
	pg_atomic_uint32 debt_unpersisted;	/* a loss awaits the marker file */
	pg_atomic_uint32 primed_revoked;	/* the primed marker's removal (or
										 * absence) is DURABLE: the clean-exit
										 * backstop may stand down */
	pg_atomic_uint64 version_evict_floor;	/* all evicted slots' versions
											 * are at/below this */
	PsSlruVersionSlot version_slot[PS_SLRU_VERSION_SLOTS];

	/*
	 * Reader-side shared state (pagestore.slru_live_reads): the last
	 * watermark fetched from the store and when, the newest known
	 * truncation tombstone per in-scope SLRU (refreshed together with the
	 * watermark), and the served-page epochs.
	 */
	pg_atomic_uint64 reader_wm;
	pg_atomic_uint64 reader_wm_at;	/* TimestampTz of the last fetch */
	pg_atomic_uint64 reader_wm_ok_at;	/* ... of the last SUCCESSFUL fetch:
										 * failures re-arm the TTL (backoff)
										 * but must not let cached pages
										 * revalidate against stale
										 * tombstones forever */
	pg_atomic_uint64 read_served;	/* live reads served from the mirror --
									 * shared, so stats read from any backend
									 * see the whole cluster's counts */
	pg_atomic_uint64 read_fallback; /* live reads deferred to local files */
	pg_atomic_uint64 tomb_cutoff[PS_SLRU_SCOPE_COUNT];	/* int64 cutoff + 1;
														 * 0 = none known */
	pg_atomic_uint64 tomb_version[PS_SLRU_SCOPE_COUNT];
	PsSlruServedShm served[PS_SLRU_SERVED_CAPACITY];

	/*
	 * Per-process pending floor + owner pid.  A nonzero floor whose owner
	 * is dead is a coverage loss a crashed backend never accounted for;
	 * the sweep in ps_slru_wm_advance() converts it.
	 */
	struct
	{
		pg_atomic_uint64 floor;
		pg_atomic_uint64 pid;
		pg_atomic_uint64 owner_gen;
		pg_atomic_uint64 live_gen;
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
static uint64 ps_slru_my_pending_gen = 0;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_slru_shmem_startup_hook = NULL;

static void
ps_slru_register_exit_drain(void)
{
	if (ps_slru_exit_registered_pid == MyProcPid)
		return;
	before_shmem_exit(ps_slru_exit_drain, (Datum) 0);
	ps_slru_exit_registered_pid = MyProcPid;
}

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

StaticAssertDecl(lengthof(ps_slru_dirmap) == PS_SLRU_SCOPE_COUNT,
				 "PS_SLRU_SCOPE_COUNT must match ps_slru_dirmap");

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

static bool
ps_slru_tomb_horizon_cutoff(int idx, int64 *cutoff)
{
	const char *dir = ps_slru_dirmap[idx].dir;

	if (strcmp(dir, "pg_xact") == 0)
	{
		*cutoff = ps_slru_xid_page(TransamVariables->oldestClogXid);
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
		{
			MultiXactId prev = (oldestMulti == FirstMultiXactId)
				? MaxMultiXactId : oldestMulti - 1;

			/*
			 * Same expression as the truncation path's
			 * PreviousMultiXactId(oldestMulti): after multixact wraparound
			 * the previous id -- and thus the cutoff page -- is at the top
			 * of the id space.  Tombstone coverage is MODULAR (the reader
			 * decides death by PagePrecedes; see ps_slru_ship_tombstone),
			 * so the high cutoff is correct on a fresh cluster too: the low
			 * pages a fresh cluster uses do not precede it, while a wrapped
			 * cluster's retired high pages do.  Publishing it here keeps a
			 * wrapped truncation's tombstone reconstructible by the reset
			 * repair path instead of being skipped.
			 */
			*cutoff = MultiXactIdToOffsetPage(prev);
		}
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
	/*
	 * Live mirror objects are strictly timeline-local: the store's as-of
	 * reads walk timeline ancestry, and a branch consuming its parent's
	 * live SLRU images (or watermark) would inherit status past its fork
	 * point instead of its reconstructed-at-fork truth.  Branding the key
	 * with the compute's own timeline makes ancestor objects simply not
	 * resolve.
	 */
	key->spcOid = (Oid) pagestore_localsvc_timeline();
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

static void
ps_slru_observe_version(uint32 obj, uint32 pageno, XLogRecPtr version)
{
	PsSlruVersionSlot *slot;
	uint32		h;

	if (ps_slru_wm == NULL || XLogRecPtrIsInvalid(version))
		return;

	h = ps_slru_page_hash(obj, pageno);
	for (int probe = 0; probe < PS_SLRU_VERSION_PROBES; probe++)
	{
		slot = &ps_slru_wm->version_slot[(h + probe) %
										 PS_SLRU_VERSION_SLOTS];
		SpinLockAcquire(&slot->mutex);
		if (!slot->valid)
		{
			slot->valid = true;
			slot->obj = obj;
			slot->pageno = pageno;
			slot->version = version;
			SpinLockRelease(&slot->mutex);
			return;
		}
		if (slot->obj == obj && slot->pageno == pageno)
		{
			if (slot->version < version)
				slot->version = version;
			SpinLockRelease(&slot->mutex);
			return;
		}
		SpinLockRelease(&slot->mutex);
	}
}

/*
 * Fold 'version' into the global eviction floor (monotonic max).  Any page
 * whose slot was evicted had its versions at/below the floor, so requiring
 * every slotless reservation to be strictly above it preserves the
 * strict-order guarantee across evictions.
 */
static void
ps_slru_version_floor_raise(XLogRecPtr version)
{
	for (;;)
	{
		uint64		cur = pg_atomic_read_u64(&ps_slru_wm->version_evict_floor);

		if ((uint64) version <= cur ||
			pg_atomic_compare_exchange_u64(&ps_slru_wm->version_evict_floor,
										   &cur, (uint64) version))
			break;
	}
}

/*
 * Reserve 'bound' as the version of the next ship of (obj, pageno): succeeds
 * only if it is strictly above every version already reserved for the page,
 * so equal capture-time samples are lifted into strict capture order.  A
 * failure is always retryable: either the page's slot holds an equal/higher
 * version (a later capture samples a higher bound), or the bound is not
 * provably above the eviction floor (and WAL advances past any floor).
 *
 * A page with no slot must reserve strictly above the eviction floor: its
 * slot may have been evicted, and the forgotten per-page floor is at or
 * below the global one by construction.  When every probed slot belongs to
 * some other page, the smallest-version one is evicted (its version folded
 * into the floor first), so a saturated table degrades to transient
 * deferrals instead of permanent failure.
 */
static bool
ps_slru_try_reserve_version(uint32 obj, uint32 pageno, XLogRecPtr bound)
{
	PsSlruVersionSlot *slot;
	PsSlruVersionSlot *victim = NULL;
	XLogRecPtr	victim_version = InvalidXLogRecPtr;
	uint32		h;

	if (XLogRecPtrIsInvalid(bound))
		return false;
	if (ps_slru_wm == NULL)
		return true;

	h = ps_slru_page_hash(obj, pageno);
	for (int probe = 0; probe < PS_SLRU_VERSION_PROBES; probe++)
	{
		slot = &ps_slru_wm->version_slot[(h + probe) %
										 PS_SLRU_VERSION_SLOTS];
		SpinLockAcquire(&slot->mutex);
		if (!slot->valid)
		{
			if ((uint64) bound <=
				pg_atomic_read_u64(&ps_slru_wm->version_evict_floor))
			{
				SpinLockRelease(&slot->mutex);
				return false;	/* possibly-evicted page: floor applies */
			}
			slot->valid = true;
			slot->obj = obj;
			slot->pageno = pageno;
			slot->version = bound;
			SpinLockRelease(&slot->mutex);
			return true;
		}
		if (slot->obj == obj && slot->pageno == pageno)
		{
			if (bound > slot->version)
			{
				slot->version = bound;
				SpinLockRelease(&slot->mutex);
				return true;
			}
			SpinLockRelease(&slot->mutex);
			return false;
		}
		if (victim == NULL || slot->version < victim_version)
		{
			victim = slot;
			victim_version = slot->version;
		}
		SpinLockRelease(&slot->mutex);
	}

	/*
	 * Every probed slot belongs to another page: evict the smallest-version
	 * one.  Re-check under the lock -- the slot may have been re-pointed at
	 * our page (or refilled) since the probe pass.
	 */
	SpinLockAcquire(&victim->mutex);
	if (victim->valid && victim->obj == obj && victim->pageno == pageno)
	{
		if (bound > victim->version)
		{
			victim->version = bound;
			SpinLockRelease(&victim->mutex);
			return true;
		}
		SpinLockRelease(&victim->mutex);
		return false;
	}
	if (victim->valid)
		ps_slru_version_floor_raise(victim->version);
	if ((uint64) bound <=
		pg_atomic_read_u64(&ps_slru_wm->version_evict_floor))
	{
		SpinLockRelease(&victim->mutex);
		return false;			/* the fold covers this bound too: defer */
	}
	victim->valid = true;
	victim->obj = obj;
	victim->pageno = pageno;
	victim->version = bound;
	SpinLockRelease(&victim->mutex);
	return true;
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
 * Anything but a clean pg_control shutdown is not -- a crash (or a
 * postmaster reinit after a backend crash: state is DB_IN_PRODUCTION then)
 * means processes died that may have held staged-but-unsynced images, of
 * pages that are clean on local disk and will never be flushed (and thus
 * re-captured) again.  A pre-existing debt marker is a loss remembered from
 * a previous life.
 */
static bool
ps_slru_boot_debt(void)
{
	struct stat st;
	ControlFileData *cf;
	bool		crc_ok;
	bool		debt;
	int			fd;
	uint64		stamped = 0;
	ssize_t		n = -1;

	if (stat(PS_SLRU_DEBT_FILE, &st) == 0)
		return true;
	if (stat(PS_SLRU_PRIMED_FILE, &st) != 0)
		return true;			/* never primed: pre-enable history unproven */

	/*
	 * The primed marker is stamped with the redo pointer of the newest
	 * checkpoint the mirror durably shipped (ps_slru_primed_refresh); a
	 * clean shutdown with the mirror active leaves it at the shutdown
	 * checkpoint's redo.  A marker stamped BELOW pg_control's checkpoint
	 * proves discontinuity: the cluster ran (and checkpointed) with the
	 * mirror off, so that run's SLRU writes were never captured, and
	 * "primed + clean shutdown" must not read as debt-free.  An old
	 * stampless marker reads as 0 = always discontinuous, which only
	 * costs one operator re-prime.
	 */
	fd = open(PS_SLRU_PRIMED_FILE, O_RDONLY | PG_BINARY);
	if (fd >= 0)
	{
		n = read(fd, &stamped, sizeof(stamped));
		close(fd);
	}

	cf = get_controlfile(DataDir, &crc_ok);
	debt = !crc_ok ||
		(cf->state != DB_SHUTDOWNED &&
		 cf->state != DB_SHUTDOWNED_IN_RECOVERY) ||
		n != (ssize_t) sizeof(stamped) ||
		stamped < (uint64) cf->checkPointCopy.redo;
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
		pg_atomic_init_u64(&ps_slru_wm->candidate_floor, 0);
		pg_atomic_init_u64(&ps_slru_wm->total_lost, debt ? 1 : 0);
		pg_atomic_init_u64(&ps_slru_wm->stats_lost, 0);
		pg_atomic_init_u64(&ps_slru_wm->loss_generation, debt ? 1 : 0);
		pg_atomic_init_u64(&ps_slru_wm->debt_generation, 1);
		pg_atomic_init_u64(&ps_slru_wm->pending_owner_next, 1);
		pg_atomic_init_u32(&ps_slru_wm->floors_set, 0);
		pg_atomic_init_u32(&ps_slru_wm->sweeps_active, 0);
		pg_atomic_init_u32(&ps_slru_wm->debt_unpersisted, debt ? 1 : 0);
		pg_atomic_init_u32(&ps_slru_wm->primed_revoked, 0);
		pg_atomic_init_u64(&ps_slru_wm->version_evict_floor, 0);
		for (int i = 0; i < PS_SLRU_VERSION_SLOTS; i++)
		{
			PsSlruVersionSlot *slot = &ps_slru_wm->version_slot[i];

			SpinLockInit(&slot->mutex);
			slot->valid = false;
			slot->obj = 0;
			slot->pageno = 0;
			slot->version = InvalidXLogRecPtr;
		}
		pg_atomic_init_u64(&ps_slru_wm->reader_wm, 0);
		pg_atomic_init_u64(&ps_slru_wm->reader_wm_at, 0);
		pg_atomic_init_u64(&ps_slru_wm->reader_wm_ok_at, 0);
		pg_atomic_init_u64(&ps_slru_wm->read_served, 0);
		pg_atomic_init_u64(&ps_slru_wm->read_fallback, 0);
		for (int i = 0; i < PS_SLRU_SCOPE_COUNT; i++)
		{
			pg_atomic_init_u64(&ps_slru_wm->tomb_cutoff[i], 0);
			pg_atomic_init_u64(&ps_slru_wm->tomb_version[i], 0);
		}
		for (int i = 0; i < PS_SLRU_SERVED_CAPACITY; i++)
		{
			pg_atomic_init_u64(&ps_slru_wm->served[i].seq, 0);
			pg_atomic_init_u64(&ps_slru_wm->served[i].tag, 0);
			pg_atomic_init_u64(&ps_slru_wm->served[i].epoch, 0);
		}
		for (int i = 0; i < ps_slru_wm_nprocs; i++)
		{
			pg_atomic_init_u64(&ps_slru_wm->pending[i].floor, 0);
			pg_atomic_init_u64(&ps_slru_wm->pending[i].pid, 0);
			pg_atomic_init_u64(&ps_slru_wm->pending[i].owner_gen, 0);
			pg_atomic_init_u64(&ps_slru_wm->pending[i].live_gen, 0);
		}

		/*
		 * Boot debt must not wait for a drain to be persisted: a clean
		 * shutdown before any drain would otherwise forget it.  And it must
		 * not start up unpersisted at all -- if the marker cannot be made
		 * durable now, later attempts will likely keep failing too, and a
		 * clean shutdown would then boot the next life with a DB_SHUTDOWNED
		 * pg_control, no debt marker, and an existing primed marker: the
		 * loss forgotten and the watermark free to advance over it.  Fail
		 * closed instead.
		 */
		if (debt)
		{
			ps_slru_debt_persist();
			if (pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) != 0)
				ereport(ERROR,
						(errmsg("pagestore: could not persist SLRU mirror debt marker \"%s\"",
								PS_SLRU_DEBT_FILE),
						 errdetail("Boot-time mirror debt must be durable before startup can continue, or a clean shutdown could forget it."),
						 errhint("Fix data directory permissions or disable pagestore.slru_mirror.")));
		}
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Remember the epoch a page's last physical-read decision was made at.
 * Seqlock discipline: the slot is claimed by advancing seq to odd (losers
 * skip; their page reads as unknown = stale and the next physical read
 * re-notes it), the pair is written, and seq lands even.  A reader accepts
 * the pair only when seq is even and UNCHANGED across the reads -- a plain
 * tag re-check would be fooled by slot reuse (page A evicted by B and
 * re-noted as A between the reader's two tag reads pairs B's or a newer
 * epoch with A's tag); seq advances on every update, so any intervening
 * write fails the before/after comparison.
 */
static void
ps_slru_served_note(uint32 obj, uint32 pageno, uint64 epoch)
{
	PsSlruServedShm *e;
	uint64		seq;

	if (ps_slru_wm == NULL)
		return;
	e = &ps_slru_wm->served[ps_slru_served_slot(obj, pageno)];

	seq = pg_atomic_read_u64(&e->seq);
	if ((seq & 1) != 0 ||
		!pg_atomic_compare_exchange_u64(&e->seq, &seq, seq + 1))
		return;					/* another writer mid-update: skip */
	(void) pg_atomic_exchange_u64(&e->epoch, epoch);
	(void) pg_atomic_exchange_u64(&e->tag, ps_slru_served_tag(obj, pageno));
	(void) pg_atomic_exchange_u64(&e->seq, seq + 2);
}

/* The remembered epoch for a page, or false if unknown/mid-update. */
static bool
ps_slru_served_epoch(uint32 obj, uint32 pageno, uint64 *epoch)
{
	PsSlruServedShm *e;
	uint64		tag = ps_slru_served_tag(obj, pageno);
	uint64		seq;

	if (ps_slru_wm == NULL)
		return false;
	e = &ps_slru_wm->served[ps_slru_served_slot(obj, pageno)];
	seq = pg_atomic_read_u64(&e->seq);
	if ((seq & 1) != 0)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u64(&e->tag) != tag)
		return false;
	*epoch = pg_atomic_read_u64(&e->epoch);
	pg_read_barrier();
	return pg_atomic_read_u64(&e->seq) == seq;
}

/*
 * Record the newest known tombstone for a scope slot.  The NEWEST-versioned
 * tombstone wins outright -- cutoffs are NOT numerically monotone (several
 * in-scope page spaces wrap, and the commit-ts reset uses PG_INT64_MAX), so
 * the cutoff follows its version rather than being max'ed.  Version-gated,
 * version written last with barriered exchanges: a torn read pairs a newer
 * cutoff with an older version, which at worst makes the revalidator call a
 * slot stale one extra time.  Cutoff is stored as cutoff+1 so 0 means "none
 * known".
 */
static void
ps_slru_tomb_note(int idx, int64 cutoff, uint64 version)
{
	uint64		cur;

	if (ps_slru_wm == NULL)
		return;

	/*
	 * Claim the pair by parking the version at UINT64_MAX (the revalidator
	 * reads that as "everything below the cutoff is stale" -- conservative
	 * while the update is in flight), write the cutoff, then land the real
	 * version.  A concurrent updater skips; the next TTL fetch re-notes.
	 */
	for (;;)
	{
		cur = pg_atomic_read_u64(&ps_slru_wm->tomb_version[idx]);
		if (cur != PG_UINT64_MAX)
		{
			/*
			 * STRICTLY newer only.  Tombstone versions are strictly
			 * monotone at the source (the natural truncate paths carry
			 * their own WAL records between truncations, and the ops
			 * helper inserts a no-op record for the same guarantee), so
			 * two fetch responses at one version saw the same store state
			 * and re-noting adds nothing -- while allowing equality would
			 * let a DELAYED stale response overwrite a newer same-version
			 * cutoff and regress the cache until the next fetch.
			 */
			if (version <= cur)
				return;
			if (pg_atomic_compare_exchange_u64(&ps_slru_wm->tomb_version[idx],
											   &cur, PG_UINT64_MAX))
				break;
		}

		/*
		 * Another updater is mid-flight.  Wait it out rather than skip:
		 * the shared fetch TTL has already been advanced, so a skipped
		 * newer cutoff would go unnoticed for a whole TTL.  The window is
		 * two exchanges wide.
		 */
		pg_spin_delay();
	}
	(void) pg_atomic_exchange_u64(&ps_slru_wm->tomb_cutoff[idx],
								  (uint64) cutoff + 1);
	(void) pg_atomic_exchange_u64(&ps_slru_wm->tomb_version[idx], version);
}

/*
 * Every write of a pending floor goes through here to keep the aggregate
 * floors_set count exact: it is what lets the drain tail on every query
 * skip the full per-ProcNumber scans when nothing is pending anywhere.
 * Owner writes are process-serial; concurrent sweepers clear floors only
 * through a CAS and decrement on success, so each 0<->nonzero transition is
 * counted exactly once.
 */
static void
ps_slru_wm_floor_write(pg_atomic_uint64 *slot, uint64 newval)
{
	uint64		old = pg_atomic_exchange_u64(slot, newval);

	if (old == 0 && newval != 0)
		pg_atomic_fetch_add_u32(&ps_slru_wm->floors_set, 1);
	else if (old != 0 && newval == 0)
		pg_atomic_fetch_sub_u32(&ps_slru_wm->floors_set, 1);
}

/*
 * Publish that this process holds a staged-but-not-durable image at
 * 'fence'.  Called under the bank lock: claim the ProcNumber slot with this
 * process's non-reused owner generation, then lower its pending floor.
 * Invalid fences publish as 1 (a floor below any real LSN) -- the drain
 * re-stamps them, but until it does the watermark must not move at all.
 */
static void
ps_slru_wm_note_pending(XLogRecPtr fence)
{
	uint64		f = XLogRecPtrIsInvalid(fence) ? 1 : (uint64) fence;
	pg_atomic_uint64 *slot;
	uint64		cur;

	if (!ps_slru_wm_claim_pending_slot())
		return;

	slot = &ps_slru_wm->pending[MyProcNumber].floor;
	cur = pg_atomic_read_u64(slot);
	if (cur == 0 || f < cur)
		ps_slru_wm_floor_write(slot, f);
}

static void
ps_slru_wm_note_lost_count(uint64 n, bool new_loss)
{
	if (ps_slru_wm != NULL)
	{
		if (new_loss)
			pg_atomic_fetch_add_u64(&ps_slru_wm->loss_generation, 1);
		pg_atomic_fetch_add_u64(&ps_slru_wm->total_lost, n);
		pg_atomic_fetch_add_u64(&ps_slru_wm->stats_lost, n);
		pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
	}
}

static void
ps_slru_wm_note_lost(void)
{
	ps_slru_wm_note_lost_count(1, true);
}

static bool
ps_slru_wm_pending_owner_matches(int procno, uint64 pid, uint64 gen)
{
	PGPROC	   *proc;

	if (pid == 0 || gen == 0)
		return false;
	proc = GetPGProcByNumber(procno);
	return (uint64) proc->pid == pid &&
		pg_atomic_read_u64(&ps_slru_wm->pending[procno].live_gen) == gen;
}

static bool
ps_slru_wm_claim_pending_slot(void)
{
	uint64		pid;
	uint64		gen;
	uint64		owner_pid;
	uint64		owner_gen;
	uint64		floor;

	if (ps_slru_wm == NULL || MyProcNumber == INVALID_PROC_NUMBER ||
		MyProcNumber >= ps_slru_wm_nprocs)
		return false;

	if (ps_slru_my_pending_gen == 0)
	{
		ps_slru_my_pending_gen =
			pg_atomic_fetch_add_u64(&ps_slru_wm->pending_owner_next, 1) + 1;
		if (ps_slru_my_pending_gen == 0)
			ps_slru_my_pending_gen =
				pg_atomic_fetch_add_u64(&ps_slru_wm->pending_owner_next, 1) + 1;
	}

	pid = (uint64) MyProcPid;
	gen = ps_slru_my_pending_gen;
	owner_pid = pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].pid);
	owner_gen = pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].owner_gen);
	floor = pg_atomic_read_u64(&ps_slru_wm->pending[MyProcNumber].floor);

	/*
	 * The slot owner is a process-local generation, not just a PID.  A new
	 * backend reusing both the ProcNumber and a recycled OS pid gets a new
	 * generation before it can publish or clear the inherited floor.
	 */
	if (owner_pid == pid && owner_gen == gen)
	{
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].live_gen, gen);
		return true;
	}

	if (floor != 0 && owner_pid != 0 && owner_gen != 0)
		ps_slru_wm_note_lost();

	ps_slru_wm_floor_write(&ps_slru_wm->pending[MyProcNumber].floor, 0);
	pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].pid, pid);
	pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].owner_gen, gen);
	pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].live_gen, gen);
	return true;
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
	uint64		generation;

	if (ps_slru_wm == NULL ||
		pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) == 0)
		return;

	generation = pg_atomic_read_u64(&ps_slru_wm->debt_generation);
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
		{
			if (pg_atomic_read_u64(&ps_slru_wm->total_lost) == 0)
			{
				bool		removed = false;

				/*
				 * The marker this call just (re)created is stale -- the
				 * losses were reset concurrently.  Clear the retry flag
				 * only once the removal is durably done: dropping it after
				 * a failed unlink/fsync would strand the stale marker with
				 * no retry scheduled, freezing the next boot despite a
				 * successful reset.
				 */
				if (unlink(PS_SLRU_DEBT_FILE) == 0 || errno == ENOENT)
				{
					int			dfd = open(".", O_RDONLY);

					removed = (dfd >= 0 && fsync(dfd) == 0);
					if (dfd >= 0)
						close(dfd);
				}
				if (removed)
				{
					pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 0);
					if (pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0)
						pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
				}
			}
			else if (generation == pg_atomic_read_u64(&ps_slru_wm->debt_generation))
				pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 0);
		}
	}
	if (pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) != 0)
	{
		int			saved_errno = errno;

		/*
		 * Marker creation failed.  A clean shutdown must not forget the
		 * loss (next boot would see DB_SHUTDOWNED + primed + no debt marker
		 * = clean), so fail closed the other way: revoke the primed marker.
		 * With neither marker present every future boot carries boot debt,
		 * whatever the shutdown looked like; the operator reset that heals
		 * the mirror re-creates the primed marker anyway.  Skip the
		 * revocation if a reset ran concurrently (it owns the marker state
		 * now; its own generation guards re-persist any racing loss).  The
		 * retry flag stays set either way -- the debt marker remains the
		 * first choice.
		 */
		if (pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0 &&
			generation == pg_atomic_read_u64(&ps_slru_wm->debt_generation))
		{
			if (unlink(PS_SLRU_PRIMED_FILE) == 0 || errno == ENOENT)
			{
				int			dfd = open(".", O_RDONLY);

				/*
				 * Only a DURABLY fsynced removal (or durable absence) stands
				 * the clean-exit backstop down: an unlink that never reached
				 * the directory can resurrect the primed marker after a host
				 * crash, exactly the boot state that forgets the loss.
				 */
				if (dfd >= 0 && fsync(dfd) == 0)
					pg_atomic_write_u32(&ps_slru_wm->primed_revoked, 1);
				if (dfd >= 0)
					close(dfd);
			}
		}
		errno = saved_errno;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("pagestore: could not persist SLRU mirror debt marker \"%s\": %m",
						PS_SLRU_DEBT_FILE)));
	}
}

/*
 * Stamp the primed marker with the redo pointer of the newest checkpoint
 * whose control image durably shipped.  The stamp is the mirror's proof of
 * CONTINUITY: a cluster that runs with the mirror disabled still writes
 * checkpoints, so its shutdown leaves pg_control ahead of the stamp, and
 * the next mirror-enabled boot reads the gap as debt (ps_slru_boot_debt)
 * instead of trusting "primed + clean shutdown" over uncaptured status.
 * Best-effort (a stale stamp only costs a conservative re-prime); written
 * via rename so a crash never leaves a torn stamp; skipped while any debt
 * is outstanding so it cannot resurrect a marker the debt-persist fallback
 * just revoked.
 */
static void
ps_slru_primed_refresh(XLogRecPtr redo)
{
	static uint64 last_stamp = 0;
	char		tmppath[MAXPGPATH];
	struct stat st;
	uint64		stamp = (uint64) redo;
	int			fd;
	bool		ok;

	if (ps_slru_wm == NULL || stamp == 0 || stamp == last_stamp)
		return;
	if (CritSectionCount > 0)
		return;
	if (pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0 ||
		pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) != 0)
		return;
	if (stat(PS_SLRU_PRIMED_FILE, &st) != 0)
		return;					/* not primed: nothing to keep alive */

	snprintf(tmppath, sizeof(tmppath), "%s.tmp", PS_SLRU_PRIMED_FILE);
	fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
			  pg_file_create_mode);
	if (fd < 0)
		goto fail;
	ok = (write(fd, &stamp, sizeof(stamp)) == (ssize_t) sizeof(stamp) &&
		  fsync(fd) == 0);
	close(fd);
	if (!ok || rename(tmppath, PS_SLRU_PRIMED_FILE) != 0)
		goto fail;
	{
		int			dfd = open(".", O_RDONLY);

		if (dfd >= 0)
		{
			(void) fsync(dfd);
			close(dfd);
		}
	}
	last_stamp = stamp;
	return;

fail:
	ereport(WARNING,
			(errcode_for_file_access(),
			 errmsg("pagestore: could not refresh the SLRU mirror primed marker \"%s\": %m",
					PS_SLRU_PRIMED_FILE)));
}

static bool
ps_slru_wm_sweep_dead_pending(bool persist, bool new_loss)
{
	bool		found = false;

	if (ps_slru_wm == NULL)
		return false;

	/*
	 * Announce the sweep before the first clear: the CAS claim removes a
	 * floor BEFORE its loss is counted (single-count discipline), and an
	 * advance pass scanning in that gap would see neither the floor nor
	 * the loss.  ps_slru_wm_advance() refuses to publish while any sweep
	 * is in flight and re-checks total_lost afterwards, which closes the
	 * window from both sides.
	 */
	pg_atomic_fetch_add_u32(&ps_slru_wm->sweeps_active, 1);

	for (int i = 0; i < ps_slru_wm_nprocs; i++)
	{
		uint64		floor;
		uint64		pid;
		uint64		gen;

		floor = pg_atomic_read_u64(&ps_slru_wm->pending[i].floor);
		if (floor == 0)
			continue;

		/*
		 * Order the owner reads after the floor read.  The claiming side
		 * publishes pid/gen before the floor becomes nonzero (the floor
		 * write is a full-barrier exchange), but without a read barrier
		 * here a weakly ordered CPU could pair the fresh floor with stale
		 * owner fields and misread a LIVE backend as dead.
		 */
		pg_read_barrier();

		pid = pg_atomic_read_u64(&ps_slru_wm->pending[i].pid);
		gen = pg_atomic_read_u64(&ps_slru_wm->pending[i].owner_gen);
		if (ps_slru_wm_pending_owner_matches(i, pid, gen))
			continue;

		/*
		 * Claim the stale floor with a CAS before counting it: between the
		 * reads above and here, a new backend can reuse this ProcNumber,
		 * zero the inherited floor (accounting for it itself -- see
		 * ps_slru_wm_claim_pending_slot), and publish a fresh floor for its
		 * own staged image.  An unconditional clear would wipe that LIVE
		 * floor and let the watermark advance over an unsynced image.  The
		 * owner fields are cleared only if still the observed dead pair.
		 */
		if (!pg_atomic_compare_exchange_u64(&ps_slru_wm->pending[i].floor,
											&floor, 0))
			continue;
		pg_atomic_fetch_sub_u32(&ps_slru_wm->floors_set, 1);
		ps_slru_wm_note_lost_count(1, new_loss);
		(void) pg_atomic_compare_exchange_u64(&ps_slru_wm->pending[i].pid,
											  &pid, 0);
		(void) pg_atomic_compare_exchange_u64(&ps_slru_wm->pending[i].owner_gen,
											  &gen, 0);
		found = true;
	}

	pg_atomic_fetch_sub_u32(&ps_slru_wm->sweeps_active, 1);

	if (found && persist)
		ps_slru_debt_persist();
	return found;
}

/*
 * Republish this process's pending floor after a drain: 0 when everything
 * staged has durably shipped, else the minimum fence still held.
 */
static void
ps_slru_wm_republish_pending(void)
{
	uint64		f = 0;

	if (!ps_slru_wm_claim_pending_slot())
		return;
	if (pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) != 0)
		ps_slru_debt_persist();

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

	ps_slru_wm_floor_write(&ps_slru_wm->pending[MyProcNumber].floor, f);
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

	/*
	 * Sweep inherited dead-owner floors before any early return.  A dead
	 * pending floor is a real coverage loss even while no checkpoint candidate
	 * is available yet, and must become persistent debt before a clean shutdown
	 * can forget it.  Gated on the aggregate count: this tail runs at every
	 * executor/xact-end drain, and an unconditional sweep would charge every
	 * simple statement O(MaxBackends) atomic reads for nothing.
	 */
	if (pg_atomic_read_u32(&ps_slru_wm->floors_set) != 0)
		ps_slru_wm_sweep_dead_pending(true, true);

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

	/*
	 * Read the candidate and its floor only after observing zero losses:
	 * reset_debt orders floor-raise, candidate-clear, then the total_lost
	 * CAS (a full barrier), so a zero read above means both writes are
	 * visible below.  A candidate at or below the floor stems from a
	 * checkpoint that started before the last reset (its redo predates the
	 * reset's WAL sample) and must never seed the watermark -- the reset's
	 * clear can race a control-mirror drain re-posting exactly such a redo.
	 */
	pg_read_barrier();
	cand = pg_atomic_read_u64(&ps_slru_wm->candidate);
	if (cand == 0 ||
		cand <= pg_atomic_read_u64(&ps_slru_wm->candidate_floor))
		return;

	for (int i = 0;
		 pg_atomic_read_u32(&ps_slru_wm->floors_set) != 0 &&
		 i < ps_slru_wm_nprocs;
		 i++)
	{
		uint64		floor;
		uint64		pid;
		uint64		gen;

		floor = pg_atomic_read_u64(&ps_slru_wm->pending[i].floor);
		if (floor == 0)
			continue;

		pg_read_barrier();		/* owner reads after the floor read; see
								 * the sweep */

		/*
		 * A floor whose owner is gone is a coverage loss no exit path
		 * accounted for (e.g. a backend killed after staging but before
		 * its first drain registered the exit callback).  Ownership is a
		 * per-ProcNumber generation as well as a PID: a recycled OS PID in
		 * a reused ProcNumber must not make an inherited floor look live.
		 * Convert it -- the loss freezes the watermark for good, so stop
		 * here rather than let this very pass advance over the fresh hole
		 * -- and clear the slot so it does not read as pending forever.
		 * The clears are CAS-guarded like the sweep's: a new owner reusing
		 * this ProcNumber may have published a live floor since our reads,
		 * and wiping that would advance the watermark over its image.
		 */
		pid = pg_atomic_read_u64(&ps_slru_wm->pending[i].pid);
		gen = pg_atomic_read_u64(&ps_slru_wm->pending[i].owner_gen);
		if (!ps_slru_wm_pending_owner_matches(i, pid, gen))
		{
			/* announce like the sweep: the CAS clears before the count */
			pg_atomic_fetch_add_u32(&ps_slru_wm->sweeps_active, 1);
			if (pg_atomic_compare_exchange_u64(&ps_slru_wm->pending[i].floor,
											   &floor, 0))
			{
				pg_atomic_fetch_sub_u32(&ps_slru_wm->floors_set, 1);
				ps_slru_wm_note_lost_count(1, true);
				(void) pg_atomic_compare_exchange_u64(&ps_slru_wm->pending[i].pid,
													  &pid, 0);
				(void) pg_atomic_compare_exchange_u64(&ps_slru_wm->pending[i].owner_gen,
													  &gen, 0);
				pg_atomic_fetch_sub_u32(&ps_slru_wm->sweeps_active, 1);

				/*
				 * This runs after the drain's own persist point; make the
				 * fresh debt durable now rather than hoping for a later drain
				 * (we are in a post-critical drain tail, file I/O is fine).
				 */
				ps_slru_debt_persist();
			}
			else
				pg_atomic_fetch_sub_u32(&ps_slru_wm->sweeps_active, 1);
		}
		return;
	}

	/*
	 * Final gate before publishing.  Every loss path makes the loss visible
	 * before -- or announces itself across -- the removal of its pending
	 * floor: exit drains and slot reclaims count first and clear after,
	 * while the CAS-claiming sweeps bracket their clear-then-count with
	 * sweeps_active.  So a scan that saw no floors either ran before the
	 * floor vanished, or the loss is already in total_lost, or the sweep is
	 * still announced; re-checking both here means a candidate can never
	 * publish over an image that was lost mid-scan.
	 */
	if (pg_atomic_read_u32(&ps_slru_wm->sweeps_active) != 0 ||
		pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0)
		return;

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

	/*
	 * A redo at or below the candidate floor is from a checkpoint that
	 * started before the last debt reset; it may vouch for flush cycles
	 * that ran while the mirror was frozen and unprimed.  Only checkpoints
	 * started after the reset produce candidates above the floor.  (This
	 * check can race the reset's floor-raise; ps_slru_wm_advance() enforces
	 * the floor again before every publish, which closes that window.)
	 */
	if ((uint64) redo <= pg_atomic_read_u64(&ps_slru_wm->candidate_floor))
		return;

	for (;;)
	{
		uint64		cur = pg_atomic_read_u64(&ps_slru_wm->candidate);

		if ((uint64) redo <= cur)
		{
			ps_slru_primed_refresh(redo);
			return;
		}
		if (pg_atomic_compare_exchange_u64(&ps_slru_wm->candidate, &cur,
										   (uint64) redo))
			break;
	}

	/*
	 * Every durably mirrored checkpoint also renews the primed marker's
	 * continuity stamp; the shutdown checkpoint's renewal is what lets the
	 * next boot trust a clean shutdown (see ps_slru_boot_debt).
	 */
	ps_slru_primed_refresh(redo);
}

/*
 * Schedule a page identity for recapture at drain time.  Infallible (called
 * under the bank lock from the write hook): fixed storage, counts a loss if
 * the table is full.
 */
static void
ps_slru_note_recapture(SlruDesc *ctl, uint32 obj, uint32 pageno,
					   XLogRecPtr fence_floor, bool loss_counted)
{
	for (int i = 0; i < PS_SLRU_RECAP_CAPACITY; i++)
	{
		PsSlruRecapture *r = &ps_slru_recap[i];

		if (r->used && r->obj == obj && r->pageno == pageno)
		{
			if (r->fence_floor < fence_floor)
				r->fence_floor = fence_floor;
			r->loss_counted |= loss_counted;
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
			r->loss_counted = loss_counted;
			ps_slru_recap_count++;
			ps_slru_wm_note_pending(InvalidXLogRecPtr);
			return;
		}
	}

	/* Table full: coverage lost; the watermark side must fail conservative. */
	if (!loss_counted)
		ps_slru_note_lost();
}

static void
ps_slru_recapture_lost(PsSlruRecapture *r)
{
	if (!r->used)
		return;
	if (!r->loss_counted)
		ps_slru_note_lost();
	r->used = false;
	ps_slru_recap_count--;
}

/*
 * Publish the watermark to the store so readers on other computes can
 * fetch it.  Losing this write only leaves readers on an older watermark
 * (conservative), so no sync and failures are swallowed with a WARNING.
 * Called after a drain, never in a critical section.
 */
static void
ps_slru_wm_publish(void)
{
	static uint64 ps_slru_wm_published = 0;
	uint64		w;
	MemoryContext cxt = CurrentMemoryContext;

	if (ps_slru_wm == NULL)
		return;
	w = pg_atomic_read_u64(&ps_slru_wm->watermark);
	if (w == 0 || w <= ps_slru_wm_published)
		return;

	PG_TRY();
	{
		PageStoreRelKey key = {0};
		char		page[BLCKSZ];

		memset(page, 0, sizeof(page));
		memcpy(page, &w, sizeof(uint64));
		ps_slru_obj_key(&key, 0);
		pagestore_localsvc_obj_write_timeout(PS_KLASS_SLRU_WM, &key, 0, page,
											 w, PS_SLRU_SHIP_TIMEOUT_MS);
		ps_slru_wm_published = w;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		bool		rethrow;

		MemoryContextSwitchTo(cxt);

		/* interrupts are not mirror failures; see the drain */
		edata = CopyErrorData();
		rethrow = (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
				   edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN);
		FreeErrorData(edata);
		if (rethrow && !ps_slru_no_rethrow)
			PG_RE_THROW();
		if (rethrow)
			ps_slru_rearm_interrupt();

		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore: could not publish the SLRU mirror watermark; readers stay on the previous one")));
	}
	PG_END_TRY();
}

/*
 * Read the cached (shared) tombstone pair for a scope slot.  Returns true
 * with a STABLE answer (-1/0 = genuinely none known); an update in flight
 * is waited out rather than misread as "no tombstone" -- the old cutoff is
 * still the last known truth, and a w == 0 reader that saw none would fall
 * back to a stale local seed page the standing tombstone had retired.  The
 * writer's window is two exchanges wide, so the wait is normally a few
 * spins; the bound only trips if the updater was killed between them
 * (shared memory is torn down by the ensuing crash-restart anyway), and
 * then the answer is UNKNOWN: returns false, and callers must treat the
 * page as undecidable (fail closed on a consumer), never as tombstone-free.
 */
static bool
ps_slru_tomb_cached(int idx, int64 *cutoff, uint64 *version)
{
	uint64		c;
	uint64		v;
	int			spins = 0;

	*cutoff = -1;
	*version = 0;
	if (ps_slru_wm == NULL)
		return true;
	for (;;)
	{
		uint64		v2;

		v = pg_atomic_read_u64(&ps_slru_wm->tomb_version[idx]);
		if (v == 0)
			return true;		/* genuinely none known */
		if (v == PG_UINT64_MAX)
		{
			if (++spins > 10000)
				return false;	/* updater died mid-window */
			pg_spin_delay();
			continue;
		}
		c = pg_atomic_read_u64(&ps_slru_wm->tomb_cutoff[idx]);
		pg_memory_barrier();
		v2 = pg_atomic_read_u64(&ps_slru_wm->tomb_version[idx]);
		if (v == v2)
			break;
		if (++spins > 10000)
			return false;
	}
	if (c == 0)
		return true;
	*cutoff = (int64) (c - 1);
	*version = v;
	return true;
}

/*
 * A catch block on the no-throw read path must not rethrow (the SLRU slot
 * is mid-I/O; an escape would leave it READ_IN_PROGRESS), but it must not
 * eat an interrupt either: ProcessInterrupts() already cleared the pending
 * flag when it raised, so losing the error here would lose the cancel.
 * Re-arm the flag so the next CHECK_FOR_INTERRUPTS() outside the hook
 * fires again.  Call in a sane memory context, before FlushErrorState().
 */
static void
ps_slru_rearm_interrupt(void)
{
	ErrorData  *edata = CopyErrorData();

	if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
	{
		QueryCancelPending = true;
		InterruptPending = true;
	}
	else if (edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
	{
		ProcDiePending = true;
		InterruptPending = true;
	}
	FreeErrorData(edata);
}

/*
 * Fetch the newest published watermark -- and each in-scope SLRU's newest
 * tombstone, which can advance independently of it (truncations publish
 * between checkpoints) -- from the store.  TTL-bounded across all backends
 * (the fetch timestamp is shared).  IPC: never called under a bank lock.
 * On any failure keeps the previous values; readers just stay conservative.
 */
static uint64
ps_slru_reader_fetch_wm(void)
{
	TimestampTz now = GetCurrentTimestamp();
	MemoryContext cxt = CurrentMemoryContext;
	uint64		at;

	if (ps_slru_wm == NULL)
		return 0;

	at = pg_atomic_read_u64(&ps_slru_wm->reader_wm_at);
	if (at != 0 &&
		!TimestampDifferenceExceeds((TimestampTz) at, now,
									PS_SLRU_READER_WM_TTL_MS))
		return pg_atomic_read_u64(&ps_slru_wm->reader_wm);

	PG_TRY();
	{
		PageStoreRelKey key = {0};
		char		page[BLCKSZ];
		uint64		w = 0;
		bool		have_w;

		ps_slru_obj_key(&key, 0);
		have_w =
			pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_WM, &key, 0,
												   PG_UINT64_MAX, page, NULL,
												   PS_SLRU_SHIP_TIMEOUT_MS);
		if (have_w)
			memcpy(&w, page, sizeof(uint64));

		for (int i = 0; i < PS_SLRU_SCOPE_COUNT; i++)
		{
			uint64		resolved = 0;
			int64		cutoff;

			ps_slru_obj_key(&key, ps_slru_dirmap[i].obj);
			if (pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_TOMB, &key,
													   0, PG_UINT64_MAX, page,
													   &resolved,
													   PS_SLRU_SHIP_TIMEOUT_MS))
			{
				memcpy(&cutoff, page, sizeof(int64));
				ps_slru_tomb_note(i, cutoff, resolved);
			}
		}

		/*
		 * Publish the watermark only after every tombstone fetch above has
		 * completed: raising it first would let a mid-cycle failure leave
		 * cache-hit revalidation comparing pages against the NEW watermark
		 * but the OLD tombstones for a whole backoff TTL, serving pages the
		 * writer already truncated.  A failed cycle now leaves the pair
		 * consistently old instead.
		 */
		if (have_w)
		{
			for (;;)
			{
				uint64		cur = pg_atomic_read_u64(&ps_slru_wm->reader_wm);

				if (w <= cur ||
					pg_atomic_compare_exchange_u64(&ps_slru_wm->reader_wm,
												   &cur, w))
					break;
			}
		}

		/*
		 * Stamp with a POST-wait timestamp: the IPC above can take multiples
		 * of the 1s TTL (each wait is bounded by a 10s ship timeout), and
		 * stamping the pre-wait time would leave the TTL already expired,
		 * turning the intended backoff into an immediate retry storm.
		 */
		now = GetCurrentTimestamp();
		pg_atomic_write_u64(&ps_slru_wm->reader_wm_at, (uint64) now);
		pg_atomic_write_u64(&ps_slru_wm->reader_wm_ok_at, (uint64) now);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(cxt);
		ps_slru_rearm_interrupt();
		FlushErrorState();

		/*
		 * Back off for a TTL, keep the old values -- but only reader_wm_at:
		 * reader_wm_ok_at stays put, so if failures persist past
		 * PS_SLRU_READER_WM_STALE_MS the revalidator stops trusting cached
		 * pages instead of serving them against stale tombstones forever.
		 * Post-wait timestamp for the same reason as the success path.
		 */
		pg_atomic_write_u64(&ps_slru_wm->reader_wm_at,
							(uint64) GetCurrentTimestamp());
	}
	PG_END_TRY();

	return pg_atomic_read_u64(&ps_slru_wm->reader_wm);
}

/*
 * Was the last SUCCESSFUL watermark/tombstone fetch recent enough to trust?
 * Distinguishes "the store is reachable and simply has no watermark yet"
 * (a consumer may serve its seed-baseline local files) from "the store is
 * unreachable" (a consumer must fail closed: the writer may have published
 * newer status or truncations it cannot see).
 */
static bool
ps_slru_reader_fetch_fresh(void)
{
	uint64		ok_at = pg_atomic_read_u64(&ps_slru_wm->reader_wm_ok_at);

	return ok_at != 0 &&
		!TimestampDifferenceExceeds((TimestampTz) ok_at,
									GetCurrentTimestamp(),
									PS_SLRU_READER_WM_STALE_MS);
}

/*
 * Does a tombstone at 'cutoff' cover 'pageno'?  Coverage is MODULAR, by the
 * same PagePrecedes relation local truncation deletes with: a numeric
 * pageno < cutoff test would stop covering pre-wrap high pages the moment a
 * post-wrap truncation publishes a low cutoff, resurrecting wrapped-away
 * status on live-read computes.  cutoff < 0 means no tombstone known;
 * PG_INT64_MAX is the commit-ts delete-all.
 */
static bool
ps_slru_tomb_covers(SlruDesc *ctl, int64 pageno, int64 cutoff)
{
	if (cutoff < 0)
		return false;
	if (cutoff == PG_INT64_MAX)
		return true;
	return ctl->options.PagePrecedes(pageno, cutoff);
}

/*
 * Does the local segment file holding 'pageno' exist?  On the mirror's own
 * writer, a present local segment is always at least as new as anything
 * the mirror holds (the mirror is fed FROM these files), so live reads
 * must not shadow it with a possibly-lagging store image; the mirror
 * serves the writer only for segments that are locally gone.
 */
static bool
ps_slru_local_segment_exists(SlruDesc *ctl, int64 pageno)
{
	char		path[MAXPGPATH];
	int64		segno = pageno / SLRU_PAGES_PER_SEGMENT;
	struct stat st;

	if (ctl->options.long_segment_names)
		snprintf(path, MAXPGPATH, "%s/%015" PRIX64, ctl->options.Dir, segno);
	else
		snprintf(path, MAXPGPATH, "%s/%04X", ctl->options.Dir,
				 (unsigned int) segno);
	return stat(path, &st) == 0;
}

static bool
ps_slru_local_page_exists(SlruDesc *ctl, int64 pageno)
{
	char		path[MAXPGPATH];
	int64		segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = (int) (pageno % SLRU_PAGES_PER_SEGMENT);
	struct stat st;

	if (ctl->options.long_segment_names)
		snprintf(path, MAXPGPATH, "%s/%015" PRIX64, ctl->options.Dir, segno);
	else
		snprintf(path, MAXPGPATH, "%s/%04X", ctl->options.Dir,
				 (unsigned int) segno);
	return stat(path, &st) == 0 && st.st_size >= (off_t) (rpageno + 1) * BLCKSZ;
}

/*
 * slru_page_read_hook consumer: serve a physical SLRU page read from the
 * live mirror, gated on the published watermark.  Runs inside the
 * SLRU_PAGE_READ_IN_PROGRESS window: it must not throw, so every store
 * error degrades to FALLBACK -- the local file (the reader's own truth)
 * decides, and a missing local page fails through the ordinary
 * SlruReportIOError machinery.
 */
static SlruReadHookResult
ps_slru_read_hook(SlruDesc *ctl, int64 pageno, char *page)
{
	int			idx;
	uint32		obj;
	uint64		w;
	int64		cached_cut;
	uint64		cached_ver;
	bool		cached_known;
	MemoryContext cxt = CurrentMemoryContext;
	SlruReadHookResult res = SLRU_READ_HOOK_FALLBACK;

	/* chain: a previously installed hook's definitive answer wins */
	if (prev_slru_page_read_hook)
	{
		res = (*prev_slru_page_read_hook) (ctl, pageno, page);
		if (res != SLRU_READ_HOOK_FALLBACK)
			return res;
		res = SLRU_READ_HOOK_FALLBACK;
	}

	idx = ps_slru_dir_index(ctl->options.Dir);
	if (idx < 0)
		return SLRU_READ_HOOK_FALLBACK;
	obj = ps_slru_dirmap[idx].obj;
	if (pageno < 0 || pageno > (int64) PG_UINT32_MAX)
		return SLRU_READ_HOOK_FALLBACK;
	if (pagestore_localsvc_read_lsn() != 0 &&
		pagestore_localsvc_read_epoch() == 1)
		return SLRU_READ_HOOK_FALLBACK;

	/*
	 * A pinned reader needs transaction status as of its effective relation
	 * horizon, not the writer's newest tombstone.  Live images and tombstones
	 * are versioned, so resolve both at R; a seeded local page is the baseline
	 * when no live image had yet been published.
	 */
	if (pagestore_localsvc_read_lsn() != 0)
	{
		PageStoreRelKey key = {0};
		char		tomb_page[BLCKSZ];
		uint64		horizon = pagestore_localsvc_read_lsn();
		uint64		image_version = 0;
		uint64		tomb_version = 0;
		int64		cutoff = -1;
		bool		have_image;
		bool		have_tomb;

		PG_TRY();
		{
			ps_slru_obj_key(&key, obj);
			have_tomb = pagestore_localsvc_obj_read_at_timeout(
				PS_KLASS_SLRU_TOMB, &key, 0, horizon, tomb_page,
				&tomb_version, PS_SLRU_SHIP_TIMEOUT_MS);
			if (have_tomb)
				memcpy(&cutoff, tomb_page, sizeof(cutoff));
			have_image = pagestore_localsvc_obj_read_at_timeout(
				PS_KLASS_SLRU_LIVE, &key, (BlockNumber) pageno, horizon,
				page, &image_version, PS_SLRU_SHIP_TIMEOUT_MS);
			if (have_tomb && ps_slru_tomb_covers(ctl, pageno, cutoff) &&
				(!have_image || image_version <= tomb_version))
				res = SLRU_READ_HOOK_FAILED;
			else if (have_image)
				res = SLRU_READ_HOOK_SERVED;
			else if (!ps_slru_local_page_exists(ctl, pageno))
				res = SLRU_READ_HOOK_FAILED;
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(cxt);
			ps_slru_rearm_interrupt();
			FlushErrorState();
			res = SLRU_READ_HOOK_FAILED;
		}
		PG_END_TRY();

		if (res == SLRU_READ_HOOK_SERVED)
		{
			ps_slru_read_served++;
			if (ps_slru_wm != NULL)
				pg_atomic_fetch_add_u64(&ps_slru_wm->read_served, 1);
		}
		else if (res == SLRU_READ_HOOK_FALLBACK)
		{
			ps_slru_read_fallback++;
			if (ps_slru_wm != NULL)
				pg_atomic_fetch_add_u64(&ps_slru_wm->read_fallback, 1);
		}
		return res;
	}

	/* the writer's own local files outrank its (lagging) mirror */
	if (ps_slru_mirror_enabled && ps_slru_local_segment_exists(ctl, pageno))
	{
		/*
		 * Local truth is unconditionally fresh: record a maximal epoch so
		 * the revalidator does not treat the cached page as unknown (=
		 * stale) and force a physical re-read on every cache hit.  A local
		 * truncation discards the cached slots itself, so nothing can go
		 * stale under this epoch on the writer.
		 */
		ps_slru_served_note(obj, (uint32) pageno, PG_UINT64_MAX);
		return SLRU_READ_HOOK_FALLBACK;
	}

	/*
	 * The shared tombstone cache applies whatever the store IPC below
	 * manages to do (memory only): a page a previous fetch already saw
	 * retired must never fall back to a stale local segment, even when
	 * this read's own store lookups time out.
	 */
	cached_known = ps_slru_tomb_cached(idx, &cached_cut, &cached_ver);

	PG_TRY();
	{
		int64		cut;
		uint64		ver;
		bool		known;

		/*
		 * The watermark is the completeness floor, not a cap: any bit in
		 * any shipped image is a durable commit (the writer ships only
		 * after XLogFlush(fence)), so the NEWEST image is always served --
		 * an image's version can exceed W (a checkpoint's flush includes
		 * commits after its redo), and capping the read at W would hide
		 * status <= W that only that image carries.  W gates whether the
		 * mirror has ever completed a cycle at all, and provides the
		 * revalidation epoch.
		 */
		w = ps_slru_reader_fetch_wm();

		/*
		 * The fetch may have loaded a newer tombstone into the shared cache
		 * even when it returns no watermark (the writer can truncate while
		 * still frozen/unprimed), so re-read the cache: the first read of a
		 * just-retired page must not slip past the pre-fetch cutoff to a
		 * stale local segment.  Fresh locals, deliberately: cached_cut must
		 * stay the untouched pre-setjmp snapshot the catch path can trust.
		 * An in-flight concurrent update reads as none; keep the snapshot
		 * as the floor then.
		 */
		known = ps_slru_tomb_cached(idx, &cut, &ver);
		if (ver == 0)
		{
			cut = cached_cut;
			ver = cached_ver;
		}

		if (!known && !cached_known && !ps_slru_mirror_enabled)
		{
			/*
			 * No trustworthy tombstone state at all (both reads found a
			 * torn update whose writer died): the page is undecidable on a
			 * consumer -- neither servable nor safely local.
			 */
			res = SLRU_READ_HOOK_FAILED;
		}
		else if (w == 0)
		{
			/*
			 * No watermark -- never published, or the fetch failed.  A
			 * live-read compute in recovery with no local segment must
			 * still fail closed: the InRecovery local fallback fabricates
			 * an all-zero page out of ENOENT, and "the mirror is required
			 * but unavailable" must not read as empty status.  Tombstones
			 * advance independently of the watermark, so a cached one
			 * still applies (memory only; no IPC here).
			 */
			if (ps_slru_tomb_covers(ctl, pageno, cut))
			{
				/*
				 * Covered -- but possibly recreated after the tombstone
				 * (commit-ts reset + reactivation under a frozen writer):
				 * a consumer with a fresh fetch probes for a newer image
				 * and serves it under the same outranking rule as the
				 * w != 0 path, else recreated pages stay unreadable until
				 * a watermark is ever published.
				 */
				if (!ps_slru_mirror_enabled && ps_slru_reader_fetch_fresh())
				{
					PageStoreRelKey key = {0};
					uint64		iv = 0;

					ps_slru_obj_key(&key, obj);
					if (pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_LIVE,
															   &key,
															   (BlockNumber) pageno,
															   PG_UINT64_MAX,
															   page, &iv,
															   PS_SLRU_SHIP_TIMEOUT_MS) &&
						iv > ver)
					{
						ps_slru_served_note(obj, (uint32) pageno,
											Max(w, iv));
						res = SLRU_READ_HOOK_SERVED;
					}
					else
						res = SLRU_READ_HOOK_FAILED;
				}
				else
					res = SLRU_READ_HOOK_FAILED;
			}
			else if (!ps_slru_mirror_enabled &&
					 (!ps_slru_reader_fetch_fresh() ||
					  !ps_slru_local_page_exists(ctl, pageno)))
			{
				/*
				 * On a pure consumer, w == 0 is only safe to serve from the
				 * seed-baseline local files when a RECENT successful fetch
				 * proves the store reachable and genuinely watermark-less;
				 * a fetch outage read as "no watermark" would otherwise
				 * quietly downgrade live reads to stale local status.
				 */
				res = SLRU_READ_HOOK_FAILED;
			}
			else if (cut >= 0)
				ps_slru_served_note(obj, (uint32) pageno, 0);
		}
		else
		{
			PageStoreRelKey key = {0};
			char		tpage[BLCKSZ];
			int64		cutoff;
			uint64		tombv;

			/*
			 * Pages below the newest tombstone are dead, whatever exists.
			 * The shared cache is the floor; the per-read lookup can only
			 * improve on it -- if the lookup fails, a tombstone another
			 * backend already saw must still apply.
			 */
			cutoff = cut;
			tombv = ver;
			ps_slru_obj_key(&key, obj);
			if (pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_TOMB,
													   &key, 0, PG_UINT64_MAX,
													   tpage, &tombv,
													   PS_SLRU_SHIP_TIMEOUT_MS))
			{
				memcpy(&cutoff, tpage, sizeof(int64));
				ps_slru_tomb_note(idx, cutoff, tombv);
			}

			{
				uint64		iv = 0;
				bool		have_image;

				ps_slru_obj_key(&key, obj);
				have_image =
					pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_LIVE,
														   &key,
														   (BlockNumber) pageno,
														   PG_UINT64_MAX, page,
														   &iv,
														   PS_SLRU_SHIP_TIMEOUT_MS);

				/*
				 * A tombstone kills only what predates it: an image shipped
				 * after the truncation (a page legitimately recreated --
				 * pg_commit_ts is reset and re-activated this way) outranks
				 * it, newest-wins like everything else.
				 */
				if (ps_slru_tomb_covers(ctl, pageno, cutoff) &&
					(!have_image || iv <= tombv))
				{
					/*
					 * Tombstoned: the store has durably declared this page
					 * dead.  On a pure live-read compute, fail closed
					 * rather than falling back -- a stale local segment
					 * must not resurrect status the truncation retired
					 * (the failure surfaces through the ordinary SLRU I/O
					 * error machinery, exactly like reading a truncated
					 * local segment).  On the mirror's own writer, though,
					 * the local files ARE the truth the mirror lags
					 * behind, so defer to them.
					 */
					if (!ps_slru_mirror_enabled)
						res = SLRU_READ_HOOK_FAILED;
				}
				else
				{
					if (have_image)
						res = SLRU_READ_HOOK_SERVED;
					else if (!ps_slru_mirror_enabled &&
							 RecoveryInProgress() &&
							 !ps_slru_local_page_exists(ctl, pageno))
					{
						/*
						 * No image AND no local segment while in recovery
						 * ON A CONSUMER: the local fallback would fabricate
						 * an all-zero page (SlruPhysicalReadPage treats
						 * ENOENT as zeros under InRecovery), silently
						 * erasing status the mirror is the authority for.
						 * The mirror's own WRITER must keep the fallback:
						 * its crash recovery legitimately synthesizes
						 * zeros for pages whose segments it truncated, and
						 * failing here would block startup.
						 */
						res = SLRU_READ_HOOK_FAILED;
					}

					/*
					 * Remember the decision epoch either way (a FALLBACK
					 * page cached from the local file was also judged
					 * against this watermark and tombstone).  The
					 * tombstone's version folds in only when it covers
					 * this page (or the served image survived it):
					 * inflating an unrelated page's epoch toward tombv
					 * would let it skip the re-read it owes a later
					 * watermark advance.  See the revalidator.
					 */
					if (res != SLRU_READ_HOOK_FAILED)
					{
						uint64		epoch = w;

						if (ps_slru_tomb_covers(ctl, pageno, cutoff))
						{
							/*
							 * A survivor (image outranking the tombstone)
							 * is noted at its image version -- strictly
							 * above tombv, so the revalidator's covered
							 * <= tombv staleness test keeps it fresh
							 * without a re-read per hit.
							 */
							if (have_image && iv > tombv)
								epoch = Max(w, iv);
							else
								epoch = Max(w, tombv);
						}
						ps_slru_served_note(obj, (uint32) pageno, epoch);
					}
				}
			}
		}
	}
	PG_CATCH();
	{
		int64		cut;
		uint64		ver;

		MemoryContextSwitchTo(cxt);
		ps_slru_rearm_interrupt();
		FlushErrorState();

		/*
		 * A store failure mid-read must not resurrect a page the cached
		 * tombstone already retired: fail closed rather than fall back to
		 * a stale local segment.  (The newer-image override needs working
		 * IPC by definition; without it, dead is dead.)  The failed attempt
		 * may itself have fetched and published a newer tombstone before
		 * the lookup that threw, so re-read the shared cache and honor what
		 * this very read learned; an in-flight concurrent update reads as
		 * none, and the pre-fetch snapshot is the floor then.
		 *
		 * On a pure live-read consumer the failure is terminal either way:
		 * its local segments are only the seed baseline, superseded by
		 * whatever the writer has mirrored since, and with the store
		 * unreachable there is no way to tell whether a newer image or
		 * truncation exists.  Serving the local bytes would hand out stale
		 * transaction status; only the mirror's own writer may fall back
		 * (its local files ARE the truth the mirror lags behind).
		 */
		(void) ps_slru_tomb_cached(idx, &cut, &ver);
		if (ver == 0)
			cut = cached_cut;

		if (!ps_slru_mirror_enabled ||
			ps_slru_tomb_covers(ctl, pageno, cut))
			res = SLRU_READ_HOOK_FAILED;
		else
			res = SLRU_READ_HOOK_FALLBACK;
	}
	PG_END_TRY();

	if (res == SLRU_READ_HOOK_SERVED)
	{
		ps_slru_read_served++;
		if (ps_slru_wm != NULL)
			pg_atomic_fetch_add_u64(&ps_slru_wm->read_served, 1);
	}
	else if (res == SLRU_READ_HOOK_FALLBACK)
	{
		ps_slru_read_fallback++;
		if (ps_slru_wm != NULL)
			pg_atomic_fetch_add_u64(&ps_slru_wm->read_fallback, 1);
	}
	return res;
}

/*
 * slru_page_exists_hook consumer: same gating as the read hook.  A page the
 * mirror holds at/below the watermark exists; a tombstoned page does not;
 * anything else defers to the local file.
 */
static SlruReadHookResult
ps_slru_exists_hook(SlruDesc *ctl, int64 pageno, bool *exists)
{
	int			idx;
	uint32		obj;
	uint64		w;
	int64		cached_cut;
	uint64		cached_ver;
	bool		cached_known;
	MemoryContext cxt = CurrentMemoryContext;
	SlruReadHookResult res = SLRU_READ_HOOK_FALLBACK;

	/* chain: a previously installed hook's definitive answer wins */
	if (prev_slru_page_exists_hook)
	{
		res = (*prev_slru_page_exists_hook) (ctl, pageno, exists);
		if (res != SLRU_READ_HOOK_FALLBACK)
			return res;
		res = SLRU_READ_HOOK_FALLBACK;
	}

	idx = ps_slru_dir_index(ctl->options.Dir);
	if (idx < 0)
		return SLRU_READ_HOOK_FALLBACK;
	obj = ps_slru_dirmap[idx].obj;
	if (pageno < 0 || pageno > (int64) PG_UINT32_MAX)
		return SLRU_READ_HOOK_FALLBACK;
	if (pagestore_localsvc_read_lsn() != 0 &&
		pagestore_localsvc_read_epoch() == 1)
		return SLRU_READ_HOOK_FALLBACK;

	/* the writer's own local files outrank its (lagging) mirror */
	if (ps_slru_mirror_enabled && ps_slru_local_segment_exists(ctl, pageno))
		return SLRU_READ_HOOK_FALLBACK;

	/* see the read hook: the cache applies even if the IPC below fails */
	cached_known = ps_slru_tomb_cached(idx, &cached_cut, &cached_ver);

	PG_TRY();
	{
		int64		cut;
		uint64		ver;
		bool		known;

		/* same newest-wins rule as the read hook; W is only the enable gate */
		w = ps_slru_reader_fetch_wm();

		/*
		 * See the read hook: the fetch may have advanced the tombstone
		 * cache.  Fresh locals so cached_cut stays the pre-setjmp snapshot
		 * for the catch path; an in-flight update reads as none, keep the
		 * snapshot then.
		 */
		known = ps_slru_tomb_cached(idx, &cut, &ver);
		if (ver == 0)
		{
			cut = cached_cut;
			ver = cached_ver;
		}

		if (!known && !cached_known && !ps_slru_mirror_enabled)
		{
			/* see the read hook: undecidable tombstone state fails closed */
			res = SLRU_READ_HOOK_FAILED;
		}
		else if (w == 0)
		{
			/*
			 * Same fail-closed rules as the read hook: existence callers
			 * (ActivateCommitTs's zero-create, find_multixact_start) must
			 * not mistake "mirror required but unavailable" for "page does
			 * not exist" -- and on a pure consumer that holds outside
			 * recovery too, where a false answer would zero-create over
			 * state the mirror holds.  A cached tombstone answers
			 * definitively.
			 */
			if (ps_slru_tomb_covers(ctl, pageno, cut))
			{
				/*
				 * The cached tombstone answers definitively only while a
				 * recent successful fetch proves it current.  On a stale
				 * fetch the page may have been recreated since (a newer
				 * image would outrank the tombstone, and the lookup that
				 * would say so is exactly what keeps failing): a false
				 * answer would let existence callers zero-create over
				 * mirror-only state, so fail closed instead.  And even a
				 * FRESH fetch only proves the tombstone current, not the
				 * absence of a newer image -- a frozen/unprimed writer can
				 * reset commit-ts (tombstone) and reactivate it (image)
				 * with the watermark still zero -- so probe for one before
				 * a definitive negative, same outranking rule as the
				 * w != 0 path.
				 */
				if (!ps_slru_mirror_enabled)
				{
					PageStoreRelKey key = {0};
					char		tpage[BLCKSZ];
					uint64		iv = 0;

					if (!ps_slru_reader_fetch_fresh())
						res = SLRU_READ_HOOK_FAILED;
					else
					{
						ps_slru_obj_key(&key, obj);
						if (pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_LIVE,
																   &key,
																   (BlockNumber) pageno,
																   PG_UINT64_MAX,
																   tpage, &iv,
																   PS_SLRU_SHIP_TIMEOUT_MS) &&
							iv > ver)
							*exists = true;
						else
							*exists = false;
						res = SLRU_READ_HOOK_SERVED;
					}
				}
				else
				{
					*exists = false;
					res = SLRU_READ_HOOK_SERVED;
				}
			}
			else if (!ps_slru_mirror_enabled &&
					 (!ps_slru_reader_fetch_fresh() ||
					  !ps_slru_local_page_exists(ctl, pageno)))
				res = SLRU_READ_HOOK_FAILED;	/* see the read hook */
		}
		else
		{
			PageStoreRelKey key = {0};
			char		tpage[BLCKSZ];
			int64		cutoff;
			uint64		tombv;

			cutoff = cut;
			tombv = ver;
			ps_slru_obj_key(&key, obj);
			if (pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_TOMB,
													   &key, 0, PG_UINT64_MAX,
													   tpage, &tombv,
													   PS_SLRU_SHIP_TIMEOUT_MS))
			{
				memcpy(&cutoff, tpage, sizeof(int64));
				ps_slru_tomb_note(idx, cutoff, tombv);
			}

			{
				uint64		iv = 0;
				bool		have_image;

				ps_slru_obj_key(&key, obj);
				have_image =
					pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_LIVE,
														   &key,
														   (BlockNumber) pageno,
														   PG_UINT64_MAX, tpage,
														   &iv,
														   PS_SLRU_SHIP_TIMEOUT_MS);

				/* same tombstone-vs-newer-image rule as the read hook */
				if (ps_slru_tomb_covers(ctl, pageno, cutoff) &&
					(!have_image || iv <= tombv))
				{
					if (!ps_slru_mirror_enabled)
					{
						*exists = false;
						res = SLRU_READ_HOOK_SERVED;
					}
				}
				else if (have_image)
				{
					*exists = true;
					res = SLRU_READ_HOOK_SERVED;
				}
			}
		}
	}
	PG_CATCH();
	{
		int64		cut;
		uint64		ver;

		MemoryContextSwitchTo(cxt);
		ps_slru_rearm_interrupt();
		FlushErrorState();

		/*
		 * See the read hook: a cached tombstone outlives a store failure,
		 * including one this very attempt fetched before the lookup that
		 * threw -- re-read the shared cache, falling back to the pre-fetch
		 * snapshot while an update is in flight.
		 */
		(void) ps_slru_tomb_cached(idx, &cut, &ver);
		if (ver == 0)
			cut = cached_cut;

		if (ps_slru_tomb_covers(ctl, pageno, cut))
		{
			/*
			 * Unlike the no-IPC path above, a definitive "does not exist"
			 * is NOT safe here: the page may have been recreated after the
			 * tombstone (a newer image outranks it -- the lookup that just
			 * failed is exactly what would have told us), and existence
			 * callers zero-create over false.  Fail closed until the store
			 * answers.
			 */
			res = SLRU_READ_HOOK_FAILED;
		}
		else if (!ps_slru_mirror_enabled &&
				 !ps_slru_local_page_exists(ctl, pageno))
		{
			/*
			 * Same fail-closed rule as the no-watermark path above: on a
			 * pure consumer with no local page, a store failure must not
			 * degrade into "does not exist" -- the false answer would let
			 * existence callers zero-create over state that lives only in
			 * the mirror.
			 */
			res = SLRU_READ_HOOK_FAILED;
		}
		else
			res = SLRU_READ_HOOK_FALLBACK;
	}
	PG_END_TRY();

	return res;
}

/*
 * slru_page_revalidate_hook consumer.  Bank lock held (shared or
 * exclusive): shared-memory reads only.  A cached slot is fresh while the
 * fetched watermark still equals the epoch its page was last decided at
 * AND no newer tombstone has covered the page's range since; once either
 * moves, one physical re-read per page picks up whatever the mirror now
 * has (or its death).  Unknown pages (table churn, another backend's
 * mid-update) count as stale -- a redundant re-read, never a stale answer.
 * All state is shared: the page one backend served from the mirror is
 * every backend's cache hit.
 */
static bool
ps_slru_revalidate_hook(SlruDesc *ctl, int64 pageno)
{
	int			idx;
	uint32		obj;
	uint64		w;
	uint64		epoch;
	uint64		tombc;
	uint64		tombv;

	/* chain: a previously installed hook calling the slot stale wins */
	if (prev_slru_page_revalidate_hook &&
		!(*prev_slru_page_revalidate_hook) (ctl, pageno))
		return false;

	idx = ps_slru_dir_index(ctl->options.Dir);
	if (idx < 0)
		return true;
	if (pageno < 0 || pageno > (int64) PG_UINT32_MAX)
		return true;
	if (pagestore_localsvc_read_lsn() != 0 &&
		pagestore_localsvc_read_epoch() == 1)
		return true;
	if (ps_slru_wm == NULL)
		return true;
	obj = ps_slru_dirmap[idx].obj;

	/*
	 * Tombstones advance independently of the watermark and apply even
	 * while it is zero (an unprimed/frozen writer still ships truncation
	 * tombstones); only when NEITHER has ever been fetched is every
	 * cached page local truth.
	 *
	 * The cutoff/version pair must be read STABLY (same discipline as
	 * ps_slru_tomb_cached): reading them independently can pair the old
	 * cutoff with a just-landed newer version, and a page the new cutoff
	 * retires would keep revalidating -- a cache hit never runs the read
	 * hook, so nothing else would catch it.  An in-flight update counts as
	 * stale: one redundant re-read, never a stale answer.
	 */
	w = pg_atomic_read_u64(&ps_slru_wm->reader_wm);
	for (;;)
	{
		uint64		v2;

		tombv = pg_atomic_read_u64(&ps_slru_wm->tomb_version[idx]);
		if (tombv == PG_UINT64_MAX)
			return false;		/* update in flight: treat as stale */
		tombc = pg_atomic_read_u64(&ps_slru_wm->tomb_cutoff[idx]);
		pg_memory_barrier();
		v2 = pg_atomic_read_u64(&ps_slru_wm->tomb_version[idx]);
		if (tombv == v2)
			break;
		if (v2 == PG_UINT64_MAX)
			return false;
	}
	if (w == 0 && tombc == 0)
	{
		/*
		 * Nothing fetched, so nothing was mirror-served -- but on a pure
		 * consumer that is only trustworthy while fetches keep SUCCEEDING:
		 * a consumer that once saw "no watermark yet" and cached seed
		 * pages must not keep serving them through a later store outage
		 * (the writer may have published status or truncations since).
		 * Stale fetches force re-reads through the fail-closed read path.
		 */
		if (!ps_slru_mirror_enabled && !ps_slru_reader_fetch_fresh())
			return false;
		return true;
	}

	/*
	 * The watermark and tombstones above are only as fresh as the last
	 * SUCCESSFUL fetch.  If fetches have been failing past the staleness
	 * bound, stop trusting cached pages: the writer may have truncated
	 * since, and serving cache hits against old tombstones would resurrect
	 * retired pages for as long as the store stays unreachable.
	 */
	{
		uint64		ok_at = pg_atomic_read_u64(&ps_slru_wm->reader_wm_ok_at);

		if (ok_at == 0 ||
			TimestampDifferenceExceeds((TimestampTz) ok_at,
									   GetCurrentTimestamp(),
									   PS_SLRU_READER_WM_STALE_MS))
			return false;
	}

	if (!ps_slru_served_epoch(obj, (uint32) pageno, &epoch))
		return false;			/* unknown: one redundant re-read */
	if (epoch < w)
		return false;
	/*
	 * Covered pages go stale at epoch <= tombv, not <: an EQUAL-version
	 * tombstone rewrite (accepted by ps_slru_tomb_note; the store resolves
	 * same-position writes by arrival order) can widen the cutoff over a
	 * page cached at exactly that epoch.  Pages that legitimately outrank
	 * the tombstone are noted at their image's version, which is strictly
	 * above it, so they stay fresh.
	 */
	if (tombc != 0 &&
		ps_slru_tomb_covers(ctl, pageno, (int64) (tombc - 1)) &&
		epoch <= tombv)
		return false;

	return true;
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
 *
 * A tombstone is a single cutoff page, newest version wins.  Coverage is
 * MODULAR, exactly like local truncation: readers decide death with the
 * SLRU's PagePrecedes relation (cutoff PG_INT64_MAX = everything), not a
 * numeric pageno < cutoff.  That is what keeps a single cutoff correct
 * across page-space wraparound -- a post-wrap low cutoff still covers the
 * pre-wrap high pages, because they precede it modularly, the same
 * relation SimpleLruTruncate() used to delete them locally.
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
 * a one-shot duplicate shield (the multixact in-critical-section calls after
 * their pre-barrier, SimpleLruTruncate after an exact-LSN pre-barrier).  Exact
 * match only: several in-scope page spaces wrap (and the commit-ts reset uses
 * PG_INT64_MAX), so "lower than covered" does not mean "already dead" -- a
 * numerically smaller later cutoff still ships, and its newer version
 * supersedes.
 */
static int64 ps_slru_tomb_covered[lengthof(ps_slru_dirmap)];
static bool ps_slru_tomb_covered_set[lengthof(ps_slru_dirmap)];

/*
 * Re-derive tombstones from local truth.  Part of priming
 * (pagestore_slru_mirror_reset_debt): a coverage loss may include a lost or
 * stale tombstone (an unclean death between a truncation's WAL record and
 * its barrier, or a barrier failure eaten as a loss), so pages that are
 * dead locally could still read as live in the mirror, or vice versa.
 * Priming is the mandated repair: for each in-scope SLRU, publish its
 * current horizon page.  That preserves page-granular cutoffs when the
 * first retained segment is only partially live.  The directory scan is
 * still useful as an error check (an unreadable directory must abort
 * priming) and as a fallback for any future in-scope SLRU without a known
 * horizon.
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
	 * clog/commit-ts truncations; XactTruncationLock protects
	 * oldestClogXid; MultiXactTruncationLock the multixact ones.  Priming
	 * is rare and the scans are tiny.
	 */
	LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);
	LWLockAcquire(XactTruncationLock, LW_EXCLUSIVE);
	LWLockAcquire(MultiXactTruncationLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		for (int i = 0; i < (int) lengthof(ps_slru_dirmap); i++)
		{
			DIR		   *dir;
			struct dirent *de;
			int64		minseg = -1;
			int64		cutoff;
			XLogRecPtr	version;
			bool		commit_ts = strcmp(ps_slru_dirmap[i].dir,
										   "pg_commit_ts") == 0;

			if (commit_ts)
			{
				LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
				commit_ts_locked = true;
			}

			/*
			 * Scan failures must abort the priming (ReadDir raises), not read
			 * as an empty directory: an empty scan publishes an everything-dead
			 * cutoff.
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

			/*
			 * Same pending-floor discipline as the truncate hook: when the
			 * reset runs with no outstanding loss (a plain re-prime), the
			 * watermark is not frozen, and a concurrent checkpoint drain
			 * must not publish a candidate past a tombstone still in
			 * flight.  The floor stays pinned across all the ships and is
			 * republished once below.
			 */
			version = ps_slru_now_lsn();
			ps_slru_wm_note_pending(version);
			ps_slru_tomb_covered_set[i] = false;
			ps_slru_ship_tombstone(ps_slru_dirmap[i].obj, cutoff, version);
			if (commit_ts)
			{
				LWLockRelease(CommitTsLock);
				commit_ts_locked = false;
			}
		}
	}
	PG_CATCH();
	{
		ps_slru_wm_republish_pending();
		if (commit_ts_locked)
			LWLockRelease(CommitTsLock);
		LWLockRelease(MultiXactTruncationLock);
		LWLockRelease(XactTruncationLock);
		LWLockRelease(WrapLimitsVacuumLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ps_slru_wm_republish_pending();
	LWLockRelease(MultiXactTruncationLock);
	LWLockRelease(XactTruncationLock);
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

	if (prev_slru_truncate_hook)
		(*prev_slru_truncate_hook) (ctl, cutoffPage, lsn);

	idx = ps_slru_dir_index(ctl->options.Dir);
	if (idx < 0)
		return;					/* out-of-scope SLRU: not mirrored */
	obj = ps_slru_dirmap[idx].obj;

	if (ps_slru_tomb_covered_set[idx] && cutoffPage == ps_slru_tomb_covered[idx])
	{
		ps_slru_tomb_covered_set[idx] = false;
		return;					/* pre-barrier already shipped this cutoff */
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
	 * (the TruncateCLOG/TruncateCommitTs/TruncateMultiXact pre-barriers);
	 * sample otherwise.
	 */
	version = XLogRecPtrIsInvalid(lsn) ? ps_slru_now_lsn() : lsn;

	/*
	 * The synchronous ship below is not staged in the queue, so it must
	 * publish its own pending floor: without one, another backend's
	 * checkpoint could advance the watermark past the truncation record
	 * while this tombstone is still in flight, and a reader would trust
	 * the mirror for an LSN whose truncation it cannot see yet.  On
	 * success the floor is deliberately NOT republished here: one truncate
	 * record can require several tombstones (multixact covers members and
	 * offsets with two barrier calls at the same trunc_lsn), and clearing
	 * the floor after the first would let another backend publish the
	 * record's redo while the second is still unshipped.  The floor stays
	 * pinned until this process's next drain recomputes it -- a short
	 * watermark delay, never a wrong answer.
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

		/*
		 * A failed barrier aborts the caller's whole pre-barrier sequence
		 * (TruncateMultiXact ships members, then offsets): a cutoff an
		 * EARLIER call in the sequence armed will never meet its
		 * in-critical consumer now, and a retried sequence would consume
		 * that stale flag pre-critically, leaving the retry's in-critical
		 * call to read as an uncovered loss.  Drop every one-shot shield;
		 * the retry re-ships those tombstones byte-identically at a newer
		 * version, which newest-wins absorbs.
		 */
		memset(ps_slru_tomb_covered_set, 0, sizeof(ps_slru_tomb_covered_set));

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

	if (shipped && cutoffPage != PG_INT64_MAX && !XLogRecPtrIsInvalid(lsn))
	{
		/*
		 * Arm the one-shot duplicate shield only for pre-barrier calls
		 * (identified by their exact record LSN): only those have an
		 * in-truncate consumer coming.  A direct SimpleLruTruncate()/
		 * SlruDeleteSegment() ship (recovery replay, hookless callers) must
		 * not leave a dangling shield -- a later same-cutoff truncation
		 * would take the early return above, skip shipping the NEWER
		 * tombstone, and let images shipped between the two truncations
		 * outrank the old one.  Never cache the delete-all cutoff either:
		 * every commit-ts deactivation uses the same PG_INT64_MAX, and
		 * skipping a later one would leave the images a reactivation
		 * shipped in between alive forever.
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
ps_slru_stage_internal(SlruDesc *ctl, uint32 obj, uint32 pageno,
					   const char *page, XLogRecPtr fence_lsn,
					   XLogRecPtr bound, bool version_reserved)
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
											   p->posted_fence, false);
						return;
					}

					/*
					 * Keep the newest bytes.  The fence and the bound only
					 * ever need to grow -- a smaller recomputed fence
					 * (group LSNs reset on eviction/reload) must not
					 * un-fence bits the older image already carried.
					 */
					if (!version_reserved &&
						!ps_slru_try_reserve_version(obj, pageno, bound))
					{
						ps_slru_note_recapture(ctl, obj, pageno, bound, false);
						return;
					}
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

			if (!version_reserved &&
				!ps_slru_try_reserve_version(obj, pageno, bound))
			{
				ps_slru_note_recapture(ctl, obj, pageno, bound, false);
				return;
			}
			ps_slru_wm_note_pending(fence_lsn);
			p->used = true;
			p->shipped = false;
			p->ctl = ctl;
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

	/*
	 * Queue full: this image cannot be staged, but the page's identity goes
	 * to the recapture table and the drain re-snapshots its current bytes --
	 * which carry every status bit this image had (SLRU page bytes are
	 * cumulative), so a successful recapture restores coverage without any
	 * debt.  Until it ships, the recapture pins the pending floor at 1, so
	 * the watermark cannot advance over the gap; a loss is counted only if
	 * the recapture table also overflows (inside the helper) or the
	 * recapture later fails for good (ps_slru_recapture_lost, exit drain,
	 * dead-owner sweep).  The capture-time bound rides along as the fence
	 * floor: it both keeps the re-ship strictly above this capture's
	 * version and marks a post-tombstone recreation as newer than the
	 * tombstone, so the drain's retirement pass cannot drop the only image
	 * of a recreated page.
	 */
	ps_slru_note_recapture(ctl, obj, pageno, bound, false);
}

static void
ps_slru_stage_reserved(SlruDesc *ctl, uint32 obj, uint32 pageno,
					   const char *page, XLogRecPtr fence_lsn,
					   XLogRecPtr bound)
{
	ps_slru_stage_internal(ctl, obj, pageno, page, fence_lsn, bound, true);
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

	/*
	 * The version is reserved inside the staging helper, at the two points
	 * that actually consume it -- NOT up front here.  A reservation made
	 * before the queue-full check would be burned by an overflow: the
	 * recapture's immediate drain re-snapshot samples the same position on
	 * an idle system, the burned slot rejects the equal bound, and the
	 * pending floor sits at 1 (a clean exit even counts a false loss) until
	 * unrelated WAL happens to advance.
	 */
	ps_slru_stage_internal(ctl, obj, (uint32) pageno, page, fence_lsn, now,
						   false);
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
ps_slru_recapture_page(PsSlruRecapture *r, char *image, XLogRecPtr *fence,
					   XLogRecPtr *bound)
{
	SlruDesc   *ctl = r->ctl;
	SlruShared	shared = ctl->shared;
	LWLock	   *banklock = SimpleLruGetBankLock(ctl, (int64) r->pageno);
	int			slots_per_bank = shared->num_slots / ctl->nbanks;
	int			bankstart = ((int) (r->pageno % ctl->nbanks)) * slots_per_bank;
	bool		found = false;
	bool		reserved = false;

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
			*fence = *bound = ps_slru_now_lsn();
			reserved = (XLogRecPtrIsInvalid(r->fence_floor) ||
						*bound > r->fence_floor) &&
				ps_slru_try_reserve_version(r->obj, r->pageno, *bound);
			found = true;
			break;
		}
	}
	LWLockRelease(banklock);

	if (found)
		return reserved;

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
		{
			if (errno == ENOENT)
				ps_slru_recapture_lost(r);
			return false;
		}

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
			{
				*fence = *bound = ps_slru_now_lsn();
				reserved = (XLogRecPtrIsInvalid(r->fence_floor) ||
							*bound > r->fence_floor) &&
					ps_slru_try_reserve_version(r->obj, r->pageno, *bound);
			}
		}
		LWLockRelease(banklock);
		CloseTransientFile(fd);

		return !resident && n == BLCKSZ && reserved;
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
ps_slru_defer_below_store_high(PsSlruPending *p, int timeout_ms)
{
	PageStoreRelKey key;
	char		tmp[BLCKSZ];
	uint64		resolved;

	if (!XLogRecPtrIsInvalid(p->posted_fence))
		return false;			/* retry the byte-identical posted version */

	/*
	 * The probe timeout is capped by the caller to the drain budget's
	 * remainder: with a slow or unreachable daemon, a full per-page
	 * PS_SLRU_SHIP_TIMEOUT_MS here would multiply by queue depth and blow
	 * far past PS_SLRU_DRAIN_BUDGET_MS before the caller's budget check
	 * runs.
	 */
	ps_slru_obj_key(&key, p->obj);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_LIVE, &key,
												(BlockNumber) p->pageno,
												PG_UINT64_MAX, tmp, &resolved,
												timeout_ms))
		return false;

	ps_slru_observe_version(p->obj, p->pageno, (XLogRecPtr) resolved);
	if (resolved < (uint64) p->bound)
		return false;

	ps_slru_note_recapture(p->ctl, p->obj, p->pageno, (XLogRecPtr) resolved,
						   false);
	p->used = false;
	p->shipped = false;
	p->posted_fence = InvalidXLogRecPtr;
	ps_slru_queue_count--;
	return true;
}

static bool
ps_slru_service_recaptures(TimestampTz drain_start, bool *budget_out)
{
	bool		staged = false;
	bool		tomb_known[lengthof(ps_slru_dirmap)] = {false};
	bool		tomb_valid[lengthof(ps_slru_dirmap)] = {false};
	int64		tomb_cut[lengthof(ps_slru_dirmap)] = {0};
	uint64		tomb_ver[lengthof(ps_slru_dirmap)] = {0};
	volatile bool commit_ts_locked = false;

	if (ps_slru_recap_count <= 0)
		return false;

	/*
	 * Serialize the whole pass against truncation barriers.  A re-snapshot
	 * samples its version at 'now', so an image staged while a truncation
	 * is between its WAL record and its tombstone sync would outrank that
	 * tombstone and resurrect the truncated page -- and no LSN comparison
	 * can catch it, because the retirement probe above simply has not seen
	 * the in-flight tombstone yet.  Under these locks (shared; the vacuum
	 * truncation paths hold them exclusive across record + barrier), every
	 * truncation either completed -- its tombstone is durable and the probe
	 * retires the entry -- or starts after our samples, versioning its
	 * tombstone above every image we stage here.  CommitTsLock is left
	 * alone (holding it stalls commits); the commit-ts delete-all reset
	 * runs under it only, but deactivation also deletes every segment, so
	 * a racing recapture degrades to retire-as-lost, never resurrection.
	 */
	LWLockAcquire(WrapLimitsVacuumLock, LW_SHARED);
	LWLockAcquire(MultiXactTruncationLock, LW_SHARED);
	PG_TRY();
	{

	for (int i = 0; i < PS_SLRU_RECAP_CAPACITY && ps_slru_recap_count > 0; i++)
	{
		PsSlruRecapture *r = &ps_slru_recap[i];
		char		image[BLCKSZ];
		XLogRecPtr	fence;
		XLogRecPtr	bound;
		int			idx;
		bool		commit_ts_entry;

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
		 * A recapture the store has since tombstoned must be RETIRED, not
		 * re-shipped: when the cutoff falls inside a segment the local file
		 * survives the truncation, so the re-snapshot below would read the
		 * pre-truncation bytes and ship them at a fresh position ABOVE the
		 * tombstone's -- and the newer-image-outranks-tombstone rule would
		 * resurrect a page the truncation declared dead.  The exception is
		 * a page recreated after the truncation: its recapture carries a
		 * fence floor above the tombstone version (the recreating capture
		 * folded it in) and must still ship.  Retiring is not a loss -- the
		 * tombstone IS the page's mirrored truth -- and this also spares
		 * the segment-gone recapture path from counting needless debt.
		 * One store lookup per SLRU per pass; a failed lookup just defers
		 * the decision to the next drain.
		 */
		idx = ps_slru_dir_index(r->ctl->options.Dir);

		/*
		 * pg_commit_ts recaptures additionally hold CommitTsLock (shared)
		 * across the probe and re-snapshot: DeactivateCommitTs() runs under
		 * it exclusively, and without the interlock a recapture could probe
		 * before its delete-all tombstone lands, then read the not-yet-
		 * unlinked segment bytes and ship them at a post-tombstone version,
		 * resurrecting reset timestamps.  The probe under this lock uses a
		 * SHORT timeout -- commits update xidLastCommit under the same lock,
		 * and a full ship timeout here would stall them; on a miss the
		 * entry just waits for the next drain.
		 */
		commit_ts_entry = (idx >= 0 &&
						   strcmp(ps_slru_dirmap[idx].dir,
								  "pg_commit_ts") == 0);
		if (commit_ts_entry)
		{
			LWLockAcquire(CommitTsLock, LW_SHARED);
			commit_ts_locked = true;
		}

		if (idx >= 0 && (!tomb_known[idx] || commit_ts_entry))
		{
			PageStoreRelKey key = {0};
			char		tpage[BLCKSZ];
			uint64		resolved = 0;

			tomb_known[idx] = true;
			tomb_valid[idx] = false;
			ps_slru_obj_key(&key, r->obj);
			if (pagestore_localsvc_obj_read_at_timeout(PS_KLASS_SLRU_TOMB,
													   &key, 0, PG_UINT64_MAX,
													   tpage, &resolved,
													   commit_ts_entry
													   ? 2000
													   : PS_SLRU_SHIP_TIMEOUT_MS))
			{
				memcpy(&tomb_cut[idx], tpage, sizeof(int64));
				tomb_ver[idx] = resolved;
				tomb_valid[idx] = true;
			}
			else if (commit_ts_entry)
			{
				/* no fresh in-lock probe: do not ship this entry now */
				LWLockRelease(CommitTsLock);
				commit_ts_locked = false;
				continue;
			}
		}
		if (idx >= 0 && tomb_valid[idx] &&
			ps_slru_tomb_covers(r->ctl, (int64) r->pageno, tomb_cut[idx]) &&
			!XLogRecPtrIsInvalid(r->fence_floor) &&
			(uint64) r->fence_floor <= tomb_ver[idx])
		{
			/*
			 * An unknown floor is NOT retired: every creation path stamps a
			 * real capture-time floor now, and an entry we cannot order
			 * against the tombstone might be the only image of a page
			 * recreated after it.
			 */
			r->used = false;
			ps_slru_recap_count--;
			if (commit_ts_entry)
			{
				LWLockRelease(CommitTsLock);
				commit_ts_locked = false;
			}
			continue;
		}

		/*
		 * While the identity's posted entry is still awaiting its sync, the
		 * recapture must wait.  This helper is called again after synced
		 * entries pop, so exit drains get a same-drain retry before leftovers
		 * are declared lost.
		 */
		if (ps_slru_queue_holds_posted(r->obj, r->pageno))
		{
			if (commit_ts_entry)
			{
				LWLockRelease(CommitTsLock);
				commit_ts_locked = false;
			}
			continue;
		}
		if (!ps_slru_recapture_page(r, image, &fence, &bound))
		{
			if (commit_ts_entry)
			{
				LWLockRelease(CommitTsLock);
				commit_ts_locked = false;
			}
			continue;			/* keep for the next drain */
		}

		/*
		 * Re-stage with the snapshot's own in-lock bound -- routing through
		 * the write hook would resample a later position and could version
		 * these (possibly stale) bytes past a newer concurrent capture's.
		 */
		ps_slru_stage_reserved(r->ctl, r->obj, r->pageno, image, fence, bound);
		if (commit_ts_entry)
		{
			LWLockRelease(CommitTsLock);
			commit_ts_locked = false;
		}
		r->used = false;
		ps_slru_recap_count--;
		staged = true;
	}

	}
	PG_CATCH();
	{
		/*
		 * The drain's own catch swallows the error and keeps running, so
		 * these must be released here rather than left to abort cleanup.
		 */
		if (commit_ts_locked)
			LWLockRelease(CommitTsLock);
		LWLockRelease(MultiXactTruncationLock);
		LWLockRelease(WrapLimitsVacuumLock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(MultiXactTruncationLock);
	LWLockRelease(WrapLimitsVacuumLock);

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

	ps_slru_register_exit_drain();

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
		ps_slru_wm_publish();
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
			long		elapsed_ms;

			if (!p->used)
				continue;

			elapsed_ms = TimestampDifferenceMilliseconds(drain_start,
														 GetCurrentTimestamp());
			if (elapsed_ms >= PS_SLRU_DRAIN_BUDGET_MS)
			{
				budget_out = true;
				break;
			}

			if (ps_slru_defer_below_store_high(p,
											   (int) Min(PS_SLRU_SHIP_TIMEOUT_MS,
														 PS_SLRU_DRAIN_BUDGET_MS - elapsed_ms)))
				continue;

			/*
			 * The probe above may have eaten what was left of the budget (a
			 * miss can take its whole capped timeout): re-check before
			 * posting, or the write+sync below would overshoot the budget
			 * by another full ship timeout.
			 */
			elapsed_ms = TimestampDifferenceMilliseconds(drain_start,
														 GetCurrentTimestamp());
			if (elapsed_ms >= PS_SLRU_DRAIN_BUDGET_MS)
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
	 * the queue's current contents, try to move the watermark, and let
	 * readers on other computes see where it now stands.
	 */
	ps_slru_wm_republish_pending();
	ps_slru_wm_advance();
	ps_slru_wm_publish();
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

/*
 * Backend drain points.  Regular backends stage images too (any dirty-slot
 * eviction in SlruSelectLRUPage() runs the write hook), and only the
 * checkpointer reliably reaches the control mirror's flush hook -- without
 * these a long-lived session would sit on its pending floor until
 * disconnect and stall the watermark across otherwise complete checkpoints.
 */
static void
ps_slru_executor_end(QueryDesc *queryDesc)
{
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
	pagestore_slru_mirror_drain();
}

static void
ps_slru_process_utility(PlannedStmt *pstmt, const char *queryString,
						bool readOnlyTree, ProcessUtilityContext context,
						ParamListInfo params, QueryEnvironment *queryEnv,
						DestReceiver *dest, QueryCompletion *qc)
{
	if (prev_ProcessUtility_hook)
		prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context,
								 params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
	pagestore_slru_mirror_drain();
}

/*
 * Transaction-end ship point.  ExecutorEnd has already run before
 * CommitTransactionCommand() updates pg_xact/commit_ts, so DML commit can
 * stage SLRU bytes after the executor drain.  PREPARE also ends the
 * originating transaction; COMMIT PREPARED may run elsewhere.
 */
static void
ps_slru_xact_drain(XactEvent event, void *arg)
{
	bool		old_no_rethrow;

	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PREPARE)
		return;
	if (CritSectionCount > 0)
		return;

	old_no_rethrow = ps_slru_no_rethrow;
	ps_slru_no_rethrow = true;
	PG_TRY();
	{
		pagestore_slru_mirror_drain();
	}
	PG_CATCH();
	{
		ps_slru_no_rethrow = old_no_rethrow;
		PG_RE_THROW();
	}
	PG_END_TRY();
	ps_slru_no_rethrow = old_no_rethrow;
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
	 * On nonzero exit, store I/O is unsafe, but the in-memory/shared loss
	 * counter is still the only truthful handoff.
	 */
	if (ps_slru_queue_count > 0 || ps_slru_recap_count > 0)
	{
		int			n = ps_slru_queue_count + ps_slru_recap_count;

		ps_slru_lost += n;
		if (ps_slru_wm != NULL)
			ps_slru_wm_note_lost_count(n, true);
		ereport(WARNING,
				(errmsg("pagestore: exiting with %d unshipped SLRU image(s); the live mirror is missing them",
						n)));
	}
	ps_slru_debt_persist();

	/*
	 * Last resort: exiting cleanly while a loss is neither marked on disk
	 * nor protected by a revoked primed marker would let a clean shutdown
	 * forget the debt (see ps_slru_debt_persist).  Force crash recovery
	 * instead -- an unclean boot re-derives the debt from pg_control.
	 */
	if (code == 0 && ps_slru_wm != NULL &&
		pg_atomic_read_u32(&ps_slru_wm->debt_unpersisted) != 0 &&
		pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0 &&
		pg_atomic_read_u32(&ps_slru_wm->primed_revoked) == 0)
	{
		/*
		 * The flag (set only after a durably fsynced revocation, including
		 * durable absence) is the arbiter here, NOT a stat(): a primed
		 * marker unlinked but never fsynced is invisible to stat yet can
		 * resurrect after a host crash -- the exact boot state that would
		 * forget the loss.
		 */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("pagestore: could not make the SLRU mirror coverage loss durable"),
				 errdetail("Neither the debt marker \"%s\" could be created nor the primed marker \"%s\" durably removed; a clean shutdown would forget the loss.",
						   PS_SLRU_DEBT_FILE, PS_SLRU_PRIMED_FILE)));
	}
	if (ps_slru_wm != NULL && MyProcNumber != INVALID_PROC_NUMBER &&
		MyProcNumber < ps_slru_wm_nprocs)
	{
		ps_slru_wm_floor_write(&ps_slru_wm->pending[MyProcNumber].floor, 0);
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].pid, 0);
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].owner_gen, 0);
		pg_atomic_write_u64(&ps_slru_wm->pending[MyProcNumber].live_gen, 0);
	}
}

/*
 * Reader side: refresh the fetched watermark at transaction boundaries
 * (TTL-bounded IPC, outside all locks) so the bank-lock revalidator -- which
 * may only do memory checks -- has a moving epoch to compare against even on
 * a pure cache-hit workload.  This deliberately does not drain staged mirror
 * images: xact callbacks run before PostgreSQL releases user locks.
 */
static void
ps_slru_xact_refresh(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PREPARE)
		return;
	if (ps_slru_live_reads_enabled && CritSectionCount == 0)
		(void) ps_slru_reader_fetch_wm();
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
	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int32GetDatum(ps_slru_queue_count);
	values[1] = Int32GetDatum(ps_slru_recap_count);
	values[2] = Int64GetDatum(ps_slru_wm != NULL
							  ? (int64) pg_atomic_read_u64(&ps_slru_wm->stats_lost)
							  : (int64) ps_slru_lost);
	values[3] = Int64GetDatum(ps_slru_wm != NULL
							  ? (int64) pg_atomic_read_u64(&ps_slru_wm->read_served)
							  : (int64) ps_slru_read_served);
	values[4] = Int64GetDatum(ps_slru_wm != NULL
							  ? (int64) pg_atomic_read_u64(&ps_slru_wm->read_fallback)
							  : (int64) ps_slru_read_fallback);

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
		XLogRecPtr	version;

		/*
		 * The version must be STRICTLY above every tombstone ever shipped
		 * for this SLRU: the reader-side shared cache orders by version
		 * alone, and a same-version rewrite would let a delayed stale
		 * fetch response overwrite the newer cutoff (the natural truncate
		 * paths always have their own WAL records between successive
		 * truncations; this helper on an idle system would not).  A no-op
		 * WAL record makes the sampled position strictly advance -- and
		 * doubles as the record the tombstone answers to, like everywhere
		 * else.
		 */
		if (RecoveryInProgress())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot publish an SLRU tombstone during recovery")));
		{
			char		dummy = 0;

			XLogBeginInsert();
			XLogRegisterData(&dummy, 1);
			version = XLogInsert(RM_XLOG_ID, XLOG_NOOP);
		}

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
	uint64		loss_generation;

	int			fd;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to reset the SLRU mirror debt")));
	if (ps_slru_wm == NULL)
		ereport(ERROR,
				(errmsg("the SLRU mirror is not active")));
	if (!ps_slru_mirror_enabled)
		ereport(ERROR,
				(errmsg("priming is the mirror writer's operation"),
				 errdetail("This compute only consumes the mirror (pagestore.slru_live_reads); its local SLRU state must not be published as truth.")));

	loss_generation = pg_atomic_read_u64(&ps_slru_wm->loss_generation);

	/*
	 * Convert any dead-owner pending floors to losses before taking the debt
	 * snapshot -- as NEW losses, advancing loss_generation.  A dead floor
	 * found here cannot be dated: it may be long-stale (safely covered by
	 * the operator's re-prime snapshot) or from a backend that died AFTER
	 * that snapshot, whose lost image the snapshot does not cover.  Counting
	 * it as new makes the generation guard below abort this reset; the
	 * retry, made after a fresh re-prime, has a snapshot that provably
	 * postdates the loss and forgives it soundly.
	 */
	(void) ps_slru_wm_sweep_dead_pending(false, true);
	lost = pg_atomic_read_u64(&ps_slru_wm->total_lost);

	/*
	 * Priming and forgiving are one act: the operator declares the mirror
	 * whole as of now.  Make the primed marker durable first, retire the
	 * counted losses, and only THEN drop the debt marker: the marker must
	 * outlive every window in which total_lost is still nonzero, or a
	 * cancel/kill landing mid-reset (after an early unlink, before the CAS)
	 * would leave a clean shutdown free to forget an outstanding loss.  The
	 * debt generation tells any already-running persist that it raced a
	 * reset; the loss generation catches a new loss that arrived after this
	 * reset's snapshot even if the final total_lost CAS would miss it.
	 */
	{
		uint64		stamp = (uint64) GetRedoRecPtr();

		fd = OpenTransientFile(PS_SLRU_PRIMED_FILE,
							   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0 ||
			write(fd, &stamp, sizeof(stamp)) != (ssize_t) sizeof(stamp) ||
			pg_fsync(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create primed marker \"%s\": %m",
							PS_SLRU_PRIMED_FILE)));
		CloseTransientFile(fd);
	}
	pg_atomic_write_u32(&ps_slru_wm->primed_revoked, 0);

	/*
	 * Priming repairs tombstone state too: republish each SLRU's cutoff
	 * from local truth, superseding any tombstone whose truncation never
	 * reached the WAL (see ps_slru_tomb_rederive).  Failures abort the
	 * reset before any debt is forgiven.
	 */
	ps_slru_tomb_rederive();

	pg_atomic_fetch_add_u64(&ps_slru_wm->debt_generation, 1);

	/*
	 * Discard the standing candidate before unfreezing debt: it may stem
	 * from a checkpoint that completed while the mirror was frozen and
	 * unprimed, and a concurrent drain must not publish it after the CAS.
	 * Clearing alone is racy -- a control-mirror drain can re-post such a
	 * redo right after the clear -- so first raise the candidate floor to
	 * the current WAL position: any checkpoint that started before this
	 * reset has its redo at or below the sample, and both the noter and
	 * the advance path reject candidates at or below the floor.
	 */
	pg_atomic_write_u64(&ps_slru_wm->candidate_floor,
						(uint64) ps_slru_now_lsn());
	pg_atomic_write_u64(&ps_slru_wm->candidate, 0);

	/*
	 * Nudge WAL past the floor.  A checkpoint issued right after this reset
	 * on an otherwise idle system would have its redo exactly AT the floor
	 * and be rejected -- equality cannot be admitted (a checkpoint already
	 * in flight during the reset samples the same position, and it may
	 * vouch for pre-priming losses), so make the very next checkpoint start
	 * strictly above the floor instead of waiting for organic traffic.  An
	 * ERROR or cancel escaping here simply aborts the reset; the debt
	 * marker is still on disk, so nothing can be forgotten.
	 */
	if (!RecoveryInProgress())
		(void) LogLogicalMessage("pagestore_slru_mirror_reset_debt", "", 0,
								 false, false);

	if (pg_atomic_read_u64(&ps_slru_wm->loss_generation) != loss_generation ||
		!pg_atomic_compare_exchange_u64(&ps_slru_wm->total_lost, &lost, 0))
		ereport(ERROR,
				(errmsg("a new SLRU mirror loss arrived during the reset"),
				 errhint("Re-prime the mirror and retry.")));

	/*
	 * The losses are retired; only now may the marker go.  If anything
	 * below fails, the flag re-arms and ps_slru_debt_persist()'s
	 * total_lost == 0 cleanup keeps retrying the removal at every drain
	 * until it is durable, so a failure here never freezes a healed mirror
	 * for good -- and never forgets anything either, since a loss racing
	 * in after the CAS bumps the loss generation and is caught below.
	 */
	if (unlink(PS_SLRU_DEBT_FILE) != 0 && errno != ENOENT)
	{
		pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove debt marker \"%s\": %m",
						PS_SLRU_DEBT_FILE)));
	}
	PG_TRY();
	{
		fsync_fname(".", true);
	}
	PG_CATCH();
	{
		pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 1);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Clear the retry flag only after the unlink is durable.  If a new loss
	 * raced the reset and set the flag just before this store, the
	 * generation/total check below restores and persists it.
	 */
	pg_atomic_write_u32(&ps_slru_wm->debt_unpersisted, 0);
	if (pg_atomic_read_u64(&ps_slru_wm->loss_generation) != loss_generation ||
		pg_atomic_read_u64(&ps_slru_wm->total_lost) != 0)
	{
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
 *
 * Raw store inspection, not the consumer read path.  In particular, passing
 * the watermark as 'lsn' does NOT return everything the watermark vouches
 * for: W is a completeness floor, and a status change <= W may be carried
 * only by an image versioned above W.  A consumer gates on the watermark
 * and reads the newest image (lsn = FF/FFFFFFFF), as the live-read hooks
 * do; an explicit lower 'lsn' is only meaningful for debugging what an
 * as-of-capture prefix of the mirror contained.
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

	if (!ps_slru_mirror_enabled && !ps_slru_live_reads_enabled)
		ereport(ERROR,
				(errmsg("the SLRU live mirror is not active"),
				 errhint("Set pagestore.slru_mirror or pagestore.slru_live_reads = on (with the localsvc backend).")));
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

	DefineCustomBoolVariable("pagestore.slru_live_reads",
							 "Serve SLRU page reads from the store's live mirror.",
							 "For a compute consuming another compute's transaction "
							 "status: physical SLRU reads are answered from the "
							 "mirror, gated on the published visibility watermark, "
							 "and cached pages are revalidated when it advances.",
							 &pagestore_slru_live_reads,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	for (int i = 0; i < (int) lengthof(ps_slru_dirmap); i++)
		ps_slru_dirmap[i].obj = pagestore_slru_klass_id(ps_slru_dirmap[i].dir);

	if (!pagestore_slru_mirror && !pagestore_slru_live_reads &&
		pagestore_localsvc_read_lsn() == 0)
		return;
	if (!localsvc_active)
		ereport(ERROR,
				(errmsg("pagestore.slru_mirror and pagestore.slru_live_reads require pagestore.backend = 'localsvc'")));

	/*
	 * A pinned reader publishes nothing: its SLRU pages are the writer's, and
	 * the mirror's ship paths would only run into pinned-write refusals.  It
	 * does consume the writer's newest status mirror.  The exact-R running-XID
	 * snapshot keeps transactions that committed after R invisible, while
	 * committed transactions outside that set need the newest status bits.
	 */
	if (pagestore_localsvc_read_lsn() != 0)
	{
		pagestore_slru_mirror = false;
		pagestore_slru_live_reads = true;
		ereport(LOG,
				(errmsg("pagestore: SLRU mirroring disabled and live reads enabled on a pinned reader (pagestore.read_lsn)")));
	}
	if (!pagestore_slru_mirror && !pagestore_slru_live_reads)
		return;

	if (pagestore_slru_mirror)
	{
		ps_slru_mirror_enabled = true;
		ps_slru_register_exit_drain();
		prev_slru_page_write_hook = slru_page_write_hook;
		slru_page_write_hook = ps_slru_write_hook;
		prev_slru_truncate_hook = slru_truncate_hook;
		slru_truncate_hook = ps_slru_truncate_hook;
		prev_ExecutorEnd_hook = ExecutorEnd_hook;
		ExecutorEnd_hook = ps_slru_executor_end;
		prev_ProcessUtility_hook = ProcessUtility_hook;
		ProcessUtility_hook = ps_slru_process_utility;
		RegisterXactCallback(ps_slru_xact_drain, NULL);
	}
	if (pagestore_slru_live_reads)
	{
		ps_slru_live_reads_enabled = true;
		prev_slru_page_read_hook = slru_page_read_hook;
		slru_page_read_hook = ps_slru_read_hook;
		prev_slru_page_exists_hook = slru_page_exists_hook;
		slru_page_exists_hook = ps_slru_exists_hook;
		prev_slru_page_revalidate_hook = slru_page_revalidate_hook;
		slru_page_revalidate_hook = ps_slru_revalidate_hook;
		RegisterXactCallback(ps_slru_xact_refresh, NULL);
	}

	/* watermark, floors, and shared stats all live in shared memory */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ps_slru_shmem_request;
	prev_slru_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ps_slru_shmem_startup;
}
