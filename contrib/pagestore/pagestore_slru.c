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

#include <fcntl.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/slru.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
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
	SlruDesc   *ctl;
	uint32		obj;			/* slru_klass_id of the SLRU directory */
	uint32		pageno;
	XLogRecPtr	fence_lsn;		/* what the drain must XLogFlush(): the
								 * page's largest group commit LSN (every
								 * not-yet-durable bit has one) */
	XLogRecPtr	bound;			/* the image's version: the WAL position at
								 * capture time, an upper bound of every bit
								 * the bytes contain (group fences alone
								 * understate it -- synchronous commits do
								 * not update group LSNs) */
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

#define PS_SLRU_VERSION_SLOTS	4096

typedef struct PsSlruVersionSlot
{
	pg_atomic_uint64 version;
} PsSlruVersionSlot;

typedef struct PsSlruStatsShm
{
	pg_atomic_uint64 lost;
	PsSlruVersionSlot version_slot[PS_SLRU_VERSION_SLOTS];
} PsSlruStatsShm;

static PsSlruStatsShm *ps_slru_stats = NULL;

static shmem_request_hook_type prev_slru_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_slru_stats_startup_hook = NULL;

static void
ps_slru_note_lost(void)
{
	ps_slru_lost++;
	if (ps_slru_stats != NULL)
		pg_atomic_fetch_add_u64(&ps_slru_stats->lost, 1);
}

static void
ps_slru_stats_shmem_request(void)
{
	if (prev_slru_shmem_request_hook)
		prev_slru_shmem_request_hook();
	RequestAddinShmemSpace(sizeof(PsSlruStatsShm));
}

static void
ps_slru_stats_shmem_startup(void)
{
	bool		found;

	if (prev_slru_stats_startup_hook)
		prev_slru_stats_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ps_slru_stats = ShmemInitStruct("pagestore slru mirror stats",
									sizeof(PsSlruStatsShm), &found);
	if (!found)
	{
		pg_atomic_init_u64(&ps_slru_stats->lost, 0);
		for (int i = 0; i < PS_SLRU_VERSION_SLOTS; i++)
			pg_atomic_init_u64(&ps_slru_stats->version_slot[i].version, 0);
	}
	LWLockRelease(AddinShmemInitLock);
}

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
static bool ps_slru_service_recaptures(TimestampTz drain_start, bool *budget_out);

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
	uint64		cur;

	if (ps_slru_stats == NULL || XLogRecPtrIsInvalid(version))
		return;

	slot = &ps_slru_stats->version_slot[ps_slru_page_hash(obj, pageno) %
										PS_SLRU_VERSION_SLOTS];
	cur = pg_atomic_read_u64(&slot->version);
	while (cur < (uint64) version &&
		   !pg_atomic_compare_exchange_u64(&slot->version, &cur,
										   (uint64) version))
		;
}

static bool
ps_slru_try_reserve_version(uint32 obj, uint32 pageno, XLogRecPtr bound)
{
	PsSlruVersionSlot *slot;
	uint64		cur;

	if (XLogRecPtrIsInvalid(bound))
		return false;
	if (ps_slru_stats == NULL)
		return true;

	slot = &ps_slru_stats->version_slot[ps_slru_page_hash(obj, pageno) %
										PS_SLRU_VERSION_SLOTS];
	cur = pg_atomic_read_u64(&slot->version);
	for (;;)
	{
		if ((uint64) bound <= cur)
			return false;
		if (pg_atomic_compare_exchange_u64(&slot->version, &cur,
										   (uint64) bound))
			return true;
	}
}

/*
 * Schedule a page identity for recapture at drain time.  Infallible (called
 * under the bank lock from the write hook): fixed storage, counts a loss if
 * the table is full.  fence_floor, when valid, forbids re-shipping the page
 * at or below that version (a post with different bytes may still be in
 * flight there).
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
			return;
		}
	}

	/* Table full: coverage lost; the watermark side must fail conservative. */
	ps_slru_note_lost();
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
					 * re-snapshotted at drain time under a fence strictly
					 * above the posted version.
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

	/* Queue full: record the page identity for recapture at drain time. */
	ps_slru_note_recapture(ctl, obj, pageno, bound);
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
	if (!ps_slru_try_reserve_version(obj, (uint32) pageno, now))
	{
		ps_slru_note_recapture(ctl, obj, (uint32) pageno, now);
		return;
	}

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
 * A conservative "now" to version images that carry no fence: their bytes
 * certainly contain nothing past the current WAL position.
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
		 * back down the chain -- a low bound is conservative for
		 * everything this file uses it for.
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
ps_slru_defer_below_store_high(PsSlruPending *p)
{
	PageStoreRelKey key;
	char		tmp[BLCKSZ];
	uint64		resolved;

	if (!XLogRecPtrIsInvalid(p->posted_fence))
		return false;			/* retry the byte-identical posted version */

	ps_slru_obj_key(&key, p->obj);
	if (!pagestore_localsvc_obj_read_at(PS_KLASS_SLRU_LIVE, &key,
										(BlockNumber) p->pageno,
										PG_UINT64_MAX, tmp, &resolved))
		return false;

	ps_slru_observe_version(p->obj, p->pageno, (XLogRecPtr) resolved);
	if (resolved < (uint64) p->bound)
		return false;

	ps_slru_note_recapture(p->ctl, p->obj, p->pageno, (XLogRecPtr) resolved);
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
		if (!ps_slru_recapture_page(r, image, &fence, &bound))
			continue;			/* keep for the next drain */

		/*
		 * Re-stage with the snapshot's own in-lock bound -- routing through
		 * the write hook would resample a later position and could version
		 * these (possibly stale) bytes past a newer concurrent capture's.
		 */
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

	if (ps_slru_lost > ps_slru_lost_reported)
	{
		ereport(WARNING,
				(errmsg("pagestore: lost capture of %llu SLRU page image(s); the live mirror is incomplete",
						(unsigned long long) (ps_slru_lost - ps_slru_lost_reported)),
				 errdetail("More distinct SLRU pages were flushed between drains than the staging queue holds.")));
		ps_slru_lost_reported = ps_slru_lost;
	}

	if (ps_slru_queue_count == 0 && ps_slru_recap_count == 0)
		return;

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

			if (ps_slru_defer_below_store_high(p))
				continue;

			if (TimestampDifferenceExceeds(drain_start, GetCurrentTimestamp(),
										   PS_SLRU_DRAIN_BUDGET_MS))
			{
				budget_out = true;
				break;
			}

			/*
			 * A retry of an already-posted entry must reuse the posted
			 * version verbatim (the bytes are frozen to match); otherwise
			 * version the image by its capture-time bound.
			 */
			version = p->posted_fence;
			if (XLogRecPtrIsInvalid(version))
				version = p->bound;
			Assert(!XLogRecPtrIsInvalid(version));

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
		 * drain point.  A delayed image is a visibility delay, never a
		 * wrong answer.
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
	if (code != 0)
		return;					/* unclean exit: do not touch the store */
	ps_slru_no_rethrow = true;
	pagestore_slru_mirror_drain();
	ps_slru_no_rethrow = false;

	/*
	 * Whatever the drain could not ship dies with this process: the pages
	 * are clean locally and will not be flushed (captured) again.  Say so
	 * honestly -- the visibility watermark built on top of this counts it
	 * as a coverage loss and freezes rather than advancing over the hole.
	 */
	if (ps_slru_queue_count > 0 || ps_slru_recap_count > 0)
	{
		int			n = ps_slru_queue_count + ps_slru_recap_count;

		ps_slru_lost += n;
		if (ps_slru_stats != NULL)
			pg_atomic_fetch_add_u64(&ps_slru_stats->lost, n);
		ereport(WARNING,
				(errmsg("pagestore: exiting with %d unshipped SLRU image(s); the live mirror is missing them",
						n)));
	}
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
	values[2] = Int64GetDatum(ps_slru_stats != NULL
							  ? (int64) pg_atomic_read_u64(&ps_slru_stats->lost)
							  : (int64) ps_slru_lost);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
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
	RegisterXactCallback(ps_slru_xact_drain, NULL);

	/* the loss counter is shared: most SLRU flushing is the checkpointer's */
	prev_slru_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ps_slru_stats_shmem_request;
	prev_slru_stats_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ps_slru_stats_shmem_startup;
}
