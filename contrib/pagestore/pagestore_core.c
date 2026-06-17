/*-------------------------------------------------------------------------
 *
 * pagestore_core.c
 *	  Shared "brain" of the page-store daemon (see pagestore_core.h).
 *
 * Design (see also pagestore_ipc.h):
 *
 *	- Page-size agnostic.  The logical page size is configured with --page-size
 *	  (8192 for PostgreSQL, 16384 for InnoDB, ...) and published in the shm
 *	  header; nothing about the on-disk format assumes a particular value.
 *
 *	- Log-structured storage.  Every page write is appended to a growing
 *	  segment as a self-describing record [SegRecHdr | page bytes].  Writes are
 *	  therefore large and sequential regardless of how small individual logical
 *	  pages are.  Old versions are never overwritten, so the log is also the COW
 *	  history.  How the segments are physically stored is the storage backend's
 *	  business (pagestore_storage.h): files for POSIX, device regions for SPDK.
 *
 *	- Indirection map.  An in-memory index maps (timeline, key, block) -> a
 *	  chain of versions {lsn, segment, offset}.  This lets a single small
 *	  logical page be addressed inside a large physical segment (ranged read).
 *	  The index is rebuilt by scanning segments at startup (recover()).
 *
 * This file holds everything backend- and loop-agnostic; each frontend (the
 * POSIX daemon, the SPDK daemon) supplies its own request loop and page byte
 * I/O.  Includes only pagestore_ipc.h/pagestore_storage.h and libc.
 *
 *-------------------------------------------------------------------------
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_core.h"

/* configuration, set by the frontend before ps_core_open() */
uint32_t	page_size = PS_DEFAULT_PAGE_SIZE;
uint64_t	segment_size = 8 * 1024 * 1024;

/* the active storage backend (POSIX by default; the frontend may override) */
const PsStorage *ps_storage = &PsStoragePosix;

/* ===================== segment storage (log-structured) ================= */

#define SEG_MAGIC	0x53454752	/* "SEGR" */

/*
 * On-disk layout of one appended page version: this header immediately
 * followed by 'len' page bytes.  The header is self-describing (carries the
 * full key/block/lsn), which is what lets recover() rebuild the entire
 * in-memory index by scanning segments sequentially -- no separate index file
 * to keep in sync.
 */
typedef struct SegRecHdr
{
	uint32_t	magic;			/* SEG_MAGIC; also the end-of-log sentinel */
	uint32_t	timeline;		/* timeline the version belongs to */
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;			/* the page's pd_lsn at write time */
	uint32_t	len;			/* page bytes following the header */
} SegRecHdr;

/*
 * Segments are addressed by (id, byte offset); how they are stored is the
 * storage backend's business (see pagestore_storage.h).  Here we keep only the
 * append cursor (cur_seg, cur_off) marking where the next record goes.
 */
static int	cur_seg = -1;		/* segment currently being appended (-1: none yet) */
static uint64_t cur_off;		/* append cursor within cur_seg */

/* ===================== in-memory indexes =============================== */

/*
 * Two chained hash tables form the indirection map that lets a single logical
 * page be located inside the large append-only segments:
 *
 *	 page_idx: (timeline, key, block) -> chain of versions {lsn, seg, off}
 *	 fork_idx: (timeline, key)        -> size of the fork on that timeline
 *
 * Entries are keyed by timeline so a branch's writes are isolated; reads that
 * miss on a timeline fall through to its parent (see read_through()).  Both
 * tables are in-memory state, rebuilt from the segments by recover().
 * (Prototype: no GC/compaction, so the version chain only grows.)
 */
#define IDX_BUCKETS		(1 << 16)
#define IDX_MASK		(IDX_BUCKETS - 1)

/* PageVer (one stored version's location) is defined in pagestore_core.h. */

/* Hash entry: all versions of one (timeline, key, block), in arrival order. */
typedef struct PageEnt
{
	struct PageEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	PageVer    *vers;			/* dynamic array, length nver, capacity cap */
	int			nver;
	int			cap;
} PageEnt;

/* Hash entry: the block count of one fork on one timeline. */
typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	nblocks;
} ForkEnt;

static PageEnt *page_idx[IDX_BUCKETS];
static ForkEnt *fork_idx[IDX_BUCKETS];

/*
 * Timeline metadata.  Timeline 0 is the root (no parent).  A branch records its
 * parent and the LSN at which it forked; reads of pages the branch never wrote
 * fall through to the parent as-of that branch LSN, so the branch is a stable
 * copy-on-write snapshot.
 */
#define MAX_TIMELINES	1024
typedef struct TimelineMeta
{
	int			defined;		/* 1 if this timeline exists */
	int			parent;			/* parent timeline id, or -1 for the root */
	uint64_t	branch_lsn;		/* parent LSN this timeline forked at */
} TimelineMeta;

static TimelineMeta timelines[MAX_TIMELINES];

/* FNV-1a hash over a byte range (used to hash keys into buckets). */

static uint32_t
fnv(const void *p, size_t n)
{
	const unsigned char *b = p;
	uint32_t	h = 2166136261u;

	for (size_t i = 0; i < n; i++)
	{
		h ^= b[i];
		h *= 16777619u;
	}
	return h;
}

static int
key_eq(const PsKey *a, const PsKey *b)
{
	return a->spcOid == b->spcOid && a->dbOid == b->dbOid &&
		a->relNumber == b->relNumber && a->forkNum == b->forkNum;
}

/* --- page index (keyed by timeline, key, block) --- */

static uint32_t
page_hash(uint32_t timeline, const PsKey *key, uint32_t block)
{
	return fnv(key, sizeof(*key)) ^ (block * 2654435761u) ^ (timeline * 40503u);
}

static PageEnt *
page_find(uint32_t timeline, const PsKey *key, uint32_t block)
{
	uint32_t	h = page_hash(timeline, key, block);
	PageEnt    *e;

	for (e = page_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && e->block == block && key_eq(&e->key, key))
			return e;
	return NULL;
}

/*
 * Record a new version of (timeline, key, block).  Only ever appends to the
 * version chain -- existing versions are never dropped -- which is what makes
 * the store copy-on-write.  The version is tagged with the writing timeline, so
 * a branch's writes never disturb its parent.  Called from append_page and from
 * recover() while replaying segments.
 */
static void
page_add_version(uint32_t timeline, const PsKey *key, uint32_t block,
				 uint64_t lsn, int seg, uint64_t off)
{
	uint32_t	h = page_hash(timeline, key, block);
	PageEnt    *e = page_find(timeline, key, block);

	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->block = block;
		e->next = page_idx[h & IDX_MASK];
		page_idx[h & IDX_MASK] = e;
	}
	if (e->nver == e->cap)		/* grow the version array geometrically */
	{
		e->cap = e->cap ? e->cap * 2 : 2;
		e->vers = realloc(e->vers, (size_t) e->cap * sizeof(PageVer));
	}
	e->vers[e->nver].lsn = lsn;
	e->vers[e->nver].seg = seg;
	e->vers[e->nver].off = off;
	e->nver++;
}

/* Newest version on this entry with lsn <= read_lsn, or NULL if none. */
static PageVer *
page_visible(PageEnt *e, uint64_t read_lsn)
{
	PageVer    *best = NULL;

	for (int i = 0; i < e->nver; i++)
	{
		PageVer    *v = &e->vers[i];

		if (v->lsn <= read_lsn && (!best || v->lsn >= best->lsn))
			best = v;
	}
	return best;
}

/* --- fork size index (keyed by timeline, key) --- */

static ForkEnt *
fork_find(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	ForkEnt    *e;

	for (e = fork_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && key_eq(&e->key, key))
			return e;
	return NULL;
}

static ForkEnt *
fork_get_or_create(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	ForkEnt    *e = fork_find(timeline, key);

	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->nblocks = 0;
		e->next = fork_idx[h & IDX_MASK];
		fork_idx[h & IDX_MASK] = e;
	}
	return e;
}

void
fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks)
{
	ForkEnt    *e = fork_get_or_create(timeline, key);

	if (to_nblocks > e->nblocks)
		e->nblocks = to_nblocks;
}

static void
fork_remove(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	ForkEnt   **pp = &fork_idx[h & IDX_MASK];

	while (*pp)
	{
		if ((*pp)->timeline == timeline && key_eq(&(*pp)->key, key))
		{
			ForkEnt    *dead = *pp;

			*pp = dead->next;
			free(dead);
			return;
		}
		pp = &(*pp)->next;
	}
}

/* --- timeline metadata + read-through --- */

static void
timeline_define(uint32_t id, int parent, uint64_t branch_lsn)
{
	if (id >= MAX_TIMELINES)
		return;
	timelines[id].defined = 1;
	timelines[id].parent = parent;
	timelines[id].branch_lsn = branch_lsn;
}

static int
timeline_has_parent(uint32_t timeline)
{
	return timeline < MAX_TIMELINES && timelines[timeline].defined &&
		timelines[timeline].parent >= 0;
}

/*
 * Validate a branch-creation request before it is recorded.  read_through() and
 * the fork-size walks follow the parent chain assuming it is finite and well
 * formed, so a bad CREATE_BRANCH must be rejected rather than persisted.  Refuse:
 *	- a new id that is out of range, the root (0), or already defined (otherwise
 *	  re-creating an id silently rewrites an existing branch's ancestry);
 *	- a parent that is out of range or not yet defined (the requested parent must
 *	  actually exist, else the branch silently inherits from nothing);
 *	- a parent whose ancestry already reaches the new id, which would turn the
 *	  parent walk into an infinite loop (e.g. new == parent, or A->B->A).
 * Returns 1 if (new_tl, parent) is safe to define.
 */
static int
branch_request_ok(uint32_t new_tl, int parent)
{
	if (new_tl == 0 || new_tl >= MAX_TIMELINES || timelines[new_tl].defined)
		return 0;
	if (parent < 0 || parent >= MAX_TIMELINES || !timelines[parent].defined)
		return 0;
	for (int t = parent; t >= 0 && t < MAX_TIMELINES; t = timelines[t].parent)
	{
		if ((uint32_t) t == new_tl)
			return 0;			/* cycle */
		if (!timelines[t].defined)
			return 0;			/* broken chain: refuse rather than risk a loop */
	}
	return 1;
}

/*
 * Resolve a read by walking the timeline ancestry: return the newest version of
 * (key, block) visible at read_lsn on 'timeline'; if the timeline never wrote
 * the page (or only after read_lsn), descend to the parent, capping read_lsn at
 * the branch LSN so the branch sees a frozen snapshot of the parent.  Returns
 * the chosen PageVer, or NULL if no ancestor has the page.
 */
PageVer *
read_through(uint32_t timeline, const PsKey *key, uint32_t block,
			 uint64_t read_lsn)
{
	for (;;)
	{
		PageEnt    *e = page_find(timeline, key, block);
		PageVer    *v = e ? page_visible(e, read_lsn) : NULL;

		if (v)
			return v;
		if (!timeline_has_parent(timeline))
			return NULL;
		if (timelines[timeline].branch_lsn < read_lsn)
			read_lsn = timelines[timeline].branch_lsn;
		timeline = (uint32_t) timelines[timeline].parent;
	}
}

/*
 * Fork size visible on 'timeline': the maximum block count across the timeline
 * and all its ancestors.  A branch that has written only some blocks of a fork
 * still inherits the parent's full size (the unwritten blocks are served by
 * read_through), so we must take the max rather than the first entry found --
 * otherwise the branch would under-report the size and reads of inherited
 * blocks would look out of range.
 *
 * (Prototype limitation: nblocks is not itself versioned, so if the parent
 * *grows* a fork after the branch point the branch sees the larger size; for a
 * quiescent parent -- the usual branch case -- this is correct.)
 */
static uint32_t
fork_nblocks_through(uint32_t timeline, const PsKey *key)
{
	uint32_t	maxnb = 0;

	for (;;)
	{
		ForkEnt    *e = fork_find(timeline, key);

		if (e && e->nblocks > maxnb)
			maxnb = e->nblocks;
		if (!timeline_has_parent(timeline))
			return maxnb;
		timeline = (uint32_t) timelines[timeline].parent;
	}
}

/* Does the fork exist on 'timeline' or any ancestor? */
static int
fork_exists_through(uint32_t timeline, const PsKey *key)
{
	for (;;)
	{
		if (fork_find(timeline, key))
			return 1;
		if (!timeline_has_parent(timeline))
			return 0;
		timeline = (uint32_t) timelines[timeline].parent;
	}
}

/*
 * Timeline metadata is persisted as an append-only log of fixed records in
 * "<store>/timelines", so branches survive a daemon restart.  (The page data
 * itself is already durable in the segments.)
 */
typedef struct TimelineRec
{
	uint32_t	id;
	int32_t		parent;
	uint64_t	branch_lsn;
} TimelineRec;

static void
timeline_persist(uint32_t id, int parent, uint64_t branch_lsn)
{
	TimelineRec rec = {id, (int32_t) parent, branch_lsn};

	ps_storage->meta_append(&rec, sizeof(rec));		/* best-effort */
}

static void
load_timelines(void)
{
	TimelineRec rec;
	uint64_t	off = 0;

	while (ps_storage->meta_read(off, &rec, sizeof(rec)) == (int) sizeof(rec))
	{
		timeline_define(rec.id, rec.parent, rec.branch_lsn);
		off += sizeof(rec);
	}
}

/* ===================== shipped WAL log (per timeline) ================== */

/*
 * Each timeline has an append-only WAL log "wal_<tl>" of self-describing
 * records [WalRecHdr | bytes].  This is the durability/transport half of WAL
 * shipping: the compute ships its WAL stream here so it is persisted by the
 * store, per timeline.  (Replaying these records to materialize pages -- redo
 * -- is a later milestone; it would reuse PostgreSQL's rmgr redo.)
 */
#define WAL_MAGIC	0x57414c52	/* "WALR" */

typedef struct WalRecHdr
{
	uint32_t	magic;
	uint32_t	len;			/* WAL bytes following the header */
	uint64_t	start_lsn;		/* LSN of the first byte */
} WalRecHdr;

/* highest end LSN (start+len) received per timeline */
static uint64_t wal_end[MAX_TIMELINES];

static int
wal_append(uint32_t tl, uint64_t start_lsn, const unsigned char *data,
		   uint32_t len)
{
	WalRecHdr	h;

	if (tl >= MAX_TIMELINES)
		return -1;

	h.magic = WAL_MAGIC;
	h.len = len;
	h.start_lsn = start_lsn;
	if (ps_storage->wal_append(tl, &h, sizeof(h), data, len) != 0)
		return -1;

	if (start_lsn + len > wal_end[tl])
		wal_end[tl] = start_lsn + len;
	return 0;
}

/*
 * Read up to 'len' WAL bytes starting at WAL position 'start' from a timeline's
 * log into 'out'; returns the number of bytes filled.  Bytes not covered by any
 * record are left as-is.  This is what a redo worker uses to pull WAL from the
 * store for replay.  (Single timeline for now; reading a branch's WAL across
 * its fork point is a refinement.)
 */
static uint32_t
wal_read(uint32_t tl, uint64_t start, uint32_t len, unsigned char *out)
{
	uint64_t	off = 0;
	WalRecHdr	h;
	uint32_t	filled = 0;

	if (tl >= MAX_TIMELINES)
		return 0;

	while (ps_storage->wal_read(tl, off, &h, sizeof(h)) == (int) sizeof(h) &&
		   h.magic == WAL_MAGIC)
	{
		uint64_t	rs = h.start_lsn;
		uint64_t	re = rs + h.len;
		uint64_t	ws = start;
		uint64_t	we = start + len;
		uint64_t	os = rs > ws ? rs : ws;	/* overlap start */
		uint64_t	oe = re < we ? re : we;	/* overlap end */

		if (os < oe)
		{
			uint64_t	src = off + sizeof(h) + (os - rs);
			int			n = ps_storage->wal_read(tl, src, out + (os - start),
												 (uint32_t) (oe - os));

			if (n > 0)
				filled += (uint32_t) n;
		}
		off += sizeof(h) + h.len;
	}
	return filled;
}

/* Rebuild wal_end[tl] by scanning the timeline's WAL log at startup. */
static void
wal_recover_one(uint32_t tl)
{
	uint64_t	off = 0;
	WalRecHdr	h;

	if (tl >= MAX_TIMELINES)
		return;
	while (ps_storage->wal_read(tl, off, &h, sizeof(h)) == (int) sizeof(h) &&
		   h.magic == WAL_MAGIC)
	{
		if (h.start_lsn + h.len > wal_end[tl])
			wal_end[tl] = h.start_lsn + h.len;
		off += sizeof(h) + h.len;
	}
}

/* ===================== per-page WAL index ============================== */

/*
 * Maps (timeline, key, block) -> the LSNs of WAL records that modify that page,
 * in ascending order.  This is the lookup single-page materialization needs: to
 * rebuild page P as-of LSN L, take P's newest stored image and replay the WAL
 * records whose LSNs fall after it and <= L.  Populated by decoding shipped WAL
 * (next milestone); queried via PS_OP_WAL_INDEX_GET.
 *
 * In-memory only for now (rebuilt by re-decoding WAL after a restart).
 */
typedef struct WalIdxEnt
{
	struct WalIdxEnt *next;
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	uint64_t   *lsns;			/* ascending */
	int			n;
	int			cap;
} WalIdxEnt;

static WalIdxEnt *walidx[IDX_BUCKETS];

static void
walidx_add(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	uint32_t	h = page_hash(tl, key, block);
	WalIdxEnt  *e;

	for (e = walidx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == tl && e->block == block && key_eq(&e->key, key))
			break;
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = tl;
		e->key = *key;
		e->block = block;
		e->next = walidx[h & IDX_MASK];
		walidx[h & IDX_MASK] = e;
	}
	if (e->n == e->cap)
	{
		e->cap = e->cap ? e->cap * 2 : 4;
		e->lsns = realloc(e->lsns, (size_t) e->cap * sizeof(uint64_t));
	}
	/* keep ascending; WAL is appended in LSN order, so usually just append */
	if (e->n == 0 || lsn >= e->lsns[e->n - 1])
		e->lsns[e->n++] = lsn;
	else
	{
		int			i = e->n;

		while (i > 0 && e->lsns[i - 1] > lsn)
		{
			e->lsns[i] = e->lsns[i - 1];
			i--;
		}
		e->lsns[i] = lsn;
		e->n++;
	}
}

/* Copy the record LSNs for (tl,key,block) that are <= lsn_max into out (cap
 * max_out); return how many.  Walks the timeline ancestry. */
static int
walidx_get(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn_max,
		   uint64_t *out, int max_out)
{
	int			got = 0;

	for (;;)
	{
		uint32_t	h = page_hash(tl, key, block);
		WalIdxEnt  *e;

		for (e = walidx[h & IDX_MASK]; e; e = e->next)
			if (e->timeline == tl && e->block == block && key_eq(&e->key, key))
			{
				for (int i = 0; i < e->n && got < max_out; i++)
					if (e->lsns[i] <= lsn_max)
						out[got++] = e->lsns[i];
				break;
			}
		if (!timeline_has_parent(tl))
			break;
		if (timelines[tl].branch_lsn < lsn_max)
			lsn_max = timelines[tl].branch_lsn;
		tl = (uint32_t) timelines[tl].parent;
	}
	return got;
}

/* ===================== write / read primitives ========================= */

/* The page's own pd_lsn lives in its first 8 bytes (xlogid, xrecoff). */
static uint64_t
page_lsn(const unsigned char *page)
{
	uint32_t	xlogid,
				xrecoff;

	memcpy(&xlogid, page, 4);
	memcpy(&xrecoff, page + 4, 4);
	return ((uint64_t) xlogid << 32) | xrecoff;
}

/*
 * Append one page version at the log head and record it in the index.  Because
 * every write lands at the moving append cursor, physical writes are large and
 * sequential even though each logical page is small -- the property we want for
 * NVMe/SPDK and network transports.
 */
int
append_page(uint32_t timeline, const PsKey *key, uint32_t block,
			const unsigned char *page)
{
	SegRecHdr	hdr;
	uint64_t	reclen = sizeof(SegRecHdr) + page_size;
	uint64_t	data_off;

	/* roll over to a fresh segment when the current one would overflow */
	if (cur_seg < 0 || cur_off + reclen > segment_size)
	{
		cur_seg = (cur_seg < 0) ? 0 : cur_seg + 1;
		cur_off = 0;
	}

	hdr.magic = SEG_MAGIC;
	hdr.timeline = timeline;
	hdr.key = *key;
	hdr.block = block;
	hdr.lsn = page_lsn(page);	/* version key taken from the page itself */
	hdr.len = page_size;

	/* write header then page bytes contiguously at the append cursor */
	if (ps_storage->seg_write(cur_seg, cur_off, &hdr, sizeof(hdr)) != 0)
		return -1;
	data_off = cur_off + sizeof(hdr);
	if (ps_storage->seg_write(cur_seg, data_off, page, page_size) != 0)
		return -1;

	/* index points at the page bytes (data_off), so reads skip the header */
	page_add_version(timeline, key, block, hdr.lsn, cur_seg, data_off);
	cur_off += reclen;
	return 0;
}

/* Read a specific version's page bytes into out (page_size bytes). */
int
read_version(const PageVer *v, unsigned char *out)
{
	if (ps_storage->seg_read(v->seg, v->off, out, page_size) != 0)
		return -1;
	return 0;
}

/* ===================== recovery (rebuild index from segments) ========== */

/*
 * Rebuild the entire in-memory index at startup by replaying the segments in
 * order.  Each record is self-describing, so replaying append_page's effect
 * (page_add_version + fork_grow) for every record reconstructs both indexes and
 * leaves the append cursor positioned just past the last valid record.
 *
 * Caveat (prototype): TRUNCATE and UNLINK are not logged as records, so their
 * effects are not reproduced on restart.  Also a partial/torn trailing record
 * is simply treated as end-of-log (the magic/len check below stops the scan).
 */
static void
recover(void)
{
	for (int id = 0;; id++)
	{
		uint64_t	off = 0;

		if (ps_storage->seg_size(id) < 0)
			break;				/* no more segments -> done */

		/* replay records until one fails to validate (end of log) */
		for (;;)
		{
			SegRecHdr	hdr;

			if (ps_storage->seg_read(id, off, &hdr, sizeof(hdr)) != 0 ||
				hdr.magic != SEG_MAGIC || hdr.len != page_size)
				break;

			page_add_version(hdr.timeline, &hdr.key, hdr.block, hdr.lsn, id,
							 off + sizeof(hdr));
			fork_grow(hdr.timeline, &hdr.key, hdr.block + 1);
			off += sizeof(hdr) + hdr.len;
		}

		/* this segment is the newest seen so far; append continues after it */
		cur_seg = id;
		cur_off = off;
	}
	if (cur_seg >= 0)
		fprintf(stderr, "pagestore_daemon: recovered through segment %d (off %llu)\n",
				cur_seg, (unsigned long long) cur_off);
}

/* ===================== request handling (non-I/O ops) ================== */

/*
 * Handle every request that needs no page byte I/O and return 1.  The four
 * byte-I/O ops (EXTEND/WRITEV/READV/READ_AT) -- and any unknown op -- return 0,
 * for the frontend to handle (synchronously for POSIX, async for SPDK).  The
 * frontend sets ch->status = OK and ch->result = 0 before calling.
 */
int
ps_handle_meta(PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE:
			fork_get_or_create(tl, &ch->key);
			break;

		case PS_OP_EXISTS:
			ch->result = fork_exists_through(tl, &ch->key) ? 1 : 0;
			break;

		case PS_OP_UNLINK:
			fork_remove(tl, &ch->key);
			break;

		case PS_OP_NBLOCKS:
			ch->result = fork_nblocks_through(tl, &ch->key);
			break;

		case PS_OP_TRUNCATE:
			fork_get_or_create(tl, &ch->key)->nblocks = ch->nblocks;
			break;

		case PS_OP_ZEROEXTEND:
			/* allocation only: grow size, no page data stored (reads -> 0) */
			fork_grow(tl, &ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_CREATE_BRANCH:
			/*
			 * Instant clone: just record metadata.  Timeline ch->timeline forks
			 * from ch->parent_timeline at LSN ch->req_lsn.  No page data is
			 * copied -- the branch shares the parent's pages by read-through
			 * until it writes (copy-on-write).
			 */
			if (branch_request_ok(ch->timeline, (int) ch->parent_timeline))
			{
				timeline_define(ch->timeline, (int) ch->parent_timeline,
								ch->req_lsn);
				timeline_persist(ch->timeline, (int) ch->parent_timeline,
								 ch->req_lsn);
			}
			else
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_APPEND:
			if (wal_append(tl, ch->req_lsn, ch->data, ch->datalen) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_SIZE:
			ch->req_lsn = wal_end[tl];	/* output: end LSN of this timeline's WAL */
			break;

		case PS_OP_WAL_READ:
			ch->result = wal_read(tl, ch->req_lsn, ch->datalen, ch->data);
			break;

		case PS_OP_WAL_INDEX_ADD:
			walidx_add(tl, &ch->key, ch->blocknum, ch->req_lsn);
			break;

		case PS_OP_WAL_INDEX_GET:
			ch->result = (uint32_t) walidx_get(tl, &ch->key, ch->blocknum,
											   ch->req_lsn, (uint64_t *) ch->data,
											   (int) (PS_IO_UNIT / sizeof(uint64_t)));
			break;

		case PS_OP_IMMEDSYNC:
			ps_storage->sync();
			break;

		default:
			return 0;			/* a byte-I/O op (or unknown): frontend handles */
	}
	return 1;
}

/* ===================== lifecycle ====================================== */

/*
 * Open the store and rebuild all in-memory state from it: define the root
 * timeline, load persisted branches, replay the segments to rebuild the page
 * and fork indexes, and recompute each timeline's shipped-WAL end LSN.  The
 * frontend must set page_size, segment_size and ps_storage beforehand.
 */
int
ps_core_open(const char *store_dir)
{
	if (ps_storage->open(store_dir, segment_size) != 0)
		return -1;

	/* timeline 0 is the root; load any persisted branches, then replay data */
	timeline_define(0, -1, 0);
	load_timelines();
	recover();

	/* rebuild each timeline's shipped-WAL end LSN from its log */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined)
			wal_recover_one(tl);

	return 0;
}
