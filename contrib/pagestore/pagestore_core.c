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
#include "pagestore_layer_store.h"
#include "pagestore_manifest.h"
#include "pagestore_memtable.h"

/* configuration, set by the frontend before ps_core_open() */
uint32_t	page_size = PS_DEFAULT_PAGE_SIZE;
uint64_t	segment_size = 8 * 1024 * 1024;
int			flush_pages = 256;	/* memtable flush threshold (pages) */
int			compact_layers = 8;	/* compact a timeline past this many image layers */
/*
 * Use the LSM read path: rebuild the index from image layers on restart and
 * (in the frontend) serve reads via read_resolve.  The POSIX daemon enables it;
 * the SPDK daemon leaves it off for now because its async read path serves pages
 * by segment offset (async layer reads are a later step), so it must keep the
 * segment-scan recovery that gives versions real segment locations.
 */
int			use_layers = 1;

/* the active storage backend (POSIX by default; the frontend may override) */
const PsStorage *ps_storage = &PsStoragePosix;

/*
 * LSM phase 2: a mutable memtable stages recent page versions and flushes them
 * into immutable image layers.  Writes still also go to the segment log (the
 * source of truth until reads + restart move onto layers in 2c/2d), so this is
 * additive.
 */
static PsMemtable *g_memtable;
static uint64_t g_next_layer_id = 1;

uint32_t
ps_core_layer_count(void)
{
	return ps_layer_map.nlayers;
}

/* read-path source counters (memtable / image layer / segment fallback) */
static uint64_t rr_mem,
			rr_layer,
			rr_seg;

void
ps_core_read_stats(uint64_t *mem, uint64_t *layer, uint64_t *seg)
{
	if (mem)
		*mem = rr_mem;
	if (layer)
		*layer = rr_layer;
	if (seg)
		*seg = rr_seg;
}

static uint64_t
alloc_layer_id(void *ctx)
{
	(void) ctx;
	return g_next_layer_id++;
}

static int
record_layer(void *ctx, const PsLayerDesc *desc)
{
	(void) ctx;
	/* ps_manifest_add_layer persists the ADD event *and* adds it to the layer
	 * map (idempotently); do not add to the map a second time. */
	return ps_manifest_add_layer(desc);
}

/* ===================== compaction & GC (LSM phase 3) =================== */

static uint32_t
count_image_layers(uint32_t timeline)
{
	uint32_t	c = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting && d->timeline == timeline)
			c++;
	}
	return c;
}

/*
 * Finish any GC that a crash interrupted: every layer still marked 'deleting' in
 * the manifest has its local file removed (idempotent) and a REMOVE_LAYER event
 * recorded.  Reads already skip 'deleting' layers, so this only reclaims space.
 */
static void
gc_resume(void)
{
	PsLayerDesc *dead;
	uint32_t	m = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].deleting)
			m++;
	if (m == 0)
		return;
	dead = malloc((size_t) m * sizeof(PsLayerDesc));
	if (!dead)
		return;
	m = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].deleting)
			dead[m++] = ps_layer_map.layers[i];
	for (uint32_t k = 0; k < m; k++)
	{
		ps_layer_store->delete_local_layer(&dead[k]);
		ps_manifest_remove_layer(dead[k].layer_id);
	}
	free(dead);
}

/*
 * Merge all of a timeline's image layers into one fresh layer (bounding the
 * layer count and the per-read layer scan), then GC the merged-away layers.
 * Install-new-before-delete-old: the new layer is written and recorded durably
 * before any old layer is marked for deletion, so a crash at any point leaves
 * the data readable and GC resumable.  Keeps every version (dedup-free: each
 * version lives in exactly one source layer); version-level GC by retained-LSN
 * horizon is a later step.
 */
static int
compact_timeline(uint32_t timeline)
{
	PsLayerDesc *old;
	uint32_t	nold = count_image_layers(timeline);
	PsImgRec   *recs = NULL;
	unsigned char **pages = NULL;
	uint32_t	nrec = 0,
				cap = 0;
	uint64_t	nid;
	PsLayerDesc newdesc;
	int			rc = -1;

	if (nold < 2)
		return 0;				/* nothing worth merging */

	old = malloc((size_t) nold * sizeof(PsLayerDesc));
	if (!old)
		return -1;
	nold = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting && d->timeline == timeline)
			old[nold++] = *d;
	}

	/* gather every version (page bytes) from the old layers */
	for (uint32_t k = 0; k < nold; k++)
	{
		PsImgIndexEnt *idx;
		uint32_t	n;

		if (ps_image_layer_read_index(&old[k], &idx, &n) != 0)
			goto cleanup;
		for (uint32_t j = 0; j < n; j++)
		{
			unsigned char *pg;

			if (nrec == cap)
			{
				uint32_t	nc = cap ? cap * 2 : 256;
				PsImgRec   *nr;
				unsigned char **np;

				/* Grow one buffer at a time, committing each on success, so
				 * 'cleanup' always sees valid recs/pages.  A failed realloc
				 * leaves the original allocation intact -- never free or NULL it
				 * here, or cleanup would double-free / deref NULL while leaking
				 * the page allocations. */
				nr = realloc(recs, (size_t) nc * sizeof(PsImgRec));
				if (!nr)
				{
					free(idx);
					goto cleanup;
				}
				recs = nr;
				np = realloc(pages, (size_t) nc * sizeof(*pages));
				if (!np)
				{
					free(idx);
					goto cleanup;
				}
				pages = np;
				cap = nc;
			}
			pg = malloc(page_size);
			if (!pg || ps_layer_store->read_layer_block(&old[k], idx[j].data_off,
														pg, page_size) != 0)
			{
				free(pg);
				free(idx);
				goto cleanup;
			}
			recs[nrec].key = idx[j].key;
			recs[nrec].block = idx[j].block;
			recs[nrec].lsn = idx[j].lsn;
			recs[nrec].page = pg;
			pages[nrec] = pg;
			nrec++;
		}
		free(idx);
	}
	if (nrec == 0)
		goto cleanup;

	/* install the new merged layer durably, THEN delete the old ones */
	nid = alloc_layer_id(NULL);
	if (ps_image_layer_write(nid, timeline, recs, nrec, page_size,
							 &newdesc) != 0 || record_layer(NULL, &newdesc) != 0)
		goto cleanup;
	for (uint32_t k = 0; k < nold; k++)
	{
		/* Only unlink the local file once the layer is durably marked deleting;
		 * otherwise a crash could leave a "live" manifest entry whose file is
		 * gone, so later reads/compactions hit a missing file.  If REMOVE_LAYER
		 * then fails the layer stays 'deleting' -- reads skip it and gc_resume()
		 * finishes the removal on the next start. */
		if (ps_manifest_mark_delete(old[k].layer_id) != 0)
			continue;
		ps_layer_store->delete_local_layer(&old[k]);
		ps_manifest_remove_layer(old[k].layer_id);
	}
	rc = 0;

cleanup:
	for (uint32_t j = 0; j < nrec; j++)
		free(pages[j]);
	free(recs);
	free(pages);
	free(old);
	return rc;
}

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

/*
 * Is 'lsn' ambiguous on this entry -- more than one stored version at this exact
 * lsn?  PostgreSQL can rewrite a page without advancing its page LSN (e.g. hint
 * bits), so an lsn does not uniquely identify a version.  When it is ambiguous a
 * same-lsn image-layer hit may be an older version than the authoritative latest
 * append, so the read must come from the segment instead.
 */
static int
page_lsn_ambiguous(const PageEnt *e, uint64_t lsn)
{
	int			seen = 0;

	for (int i = 0; i < e->nver; i++)
		if (e->vers[i].lsn == lsn && ++seen > 1)
			return 1;
	return 0;
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

	/*
	 * Records are appended in creation order, so a record's parent is defined by
	 * an earlier record (or is the root).  Re-validate each with the same check
	 * used at creation, so a corrupt, truncated, or duplicated persisted record
	 * cannot reintroduce an undefined parent or a cycle that the creation path
	 * rejects -- which would otherwise hang read_through()'s ancestry walk after
	 * a restart.  Invalid records are skipped, not applied.
	 */
	while (ps_storage->meta_read(off, &rec, sizeof(rec)) == (int) sizeof(rec))
	{
		if (branch_request_ok(rec.id, rec.parent))
			timeline_define(rec.id, rec.parent, rec.branch_lsn);
		else
			fprintf(stderr, "pagestore: skipping invalid timeline record "
					"(id=%u parent=%d)\n", rec.id, rec.parent);
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

	/* stage the version for the LSM memtable; flush to an image layer when full
	 * (additive in phase 2 -- the segment write above is still authoritative) */
	if (g_memtable)
	{
		ps_memtable_put(g_memtable, timeline, key, block, hdr.lsn, page);
		if (ps_memtable_full(g_memtable))
		{
			ps_memtable_flush(g_memtable, alloc_layer_id, record_layer, NULL);
			/* a flush groups by timeline and can emit a layer for several of
			 * them, so compact every timeline now over the threshold -- not only
			 * the one this write belongs to (a colder timeline could otherwise
			 * grow unbounded until it happens to cross the flush threshold) */
			for (uint32_t ct = 0; ct < MAX_TIMELINES; ct++)
				if ((ct == 0 || timelines[ct].defined) &&
					count_image_layers(ct) > (uint32_t) compact_layers)
					compact_timeline(ct);
		}
	}
	return 0;
}

/* Read a specific version's page bytes into out (page_size bytes). */
int
read_version(const PageVer *v, unsigned char *out)
{
	if (v->seg < 0)				/* layer-origin version (no segment copy) */
		return -1;
	if (ps_storage->seg_read(v->seg, v->off, out, page_size) != 0)
		return -1;
	return 0;
}

/*
 * Newest image-layer version of (timeline, key, block) with lsn <= read_lsn on
 * this exact timeline (ancestry is the caller's job).  Tries every image layer
 * of that timeline (key-range/bloom pruning is a later optimization).
 */
static int
layer_map_lookup(uint32_t timeline, const PsKey *key, uint32_t block,
				 uint64_t read_lsn, uint64_t *out_lsn, unsigned char *out)
{
	unsigned char *tmp = malloc(page_size);
	int			found = 0;
	uint64_t	best = 0;

	if (!tmp)
		return 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];
		uint64_t	l;

		if (d->kind != PS_LAYER_IMAGE || d->timeline != timeline || d->deleting)
			continue;
		if (ps_image_layer_lookup(d, key, block, read_lsn, tmp, page_size,
								  &l) == 1 && (!found || l > best))
		{
			best = l;
			memcpy(out, tmp, page_size);
			found = 1;
		}
	}
	free(tmp);
	if (found && out_lsn)
		*out_lsn = best;
	return found;
}

/*
 * Resolve a read into out (page_size bytes): walk the timeline ancestry as
 * read_through() does, but serve the bytes from the memtable or an image layer
 * when they hold the authoritative version, falling back to the segment.  The
 * page index (page_visible) still selects the authoritative version at each
 * level, so the result matches the segment-only read; layers/memtable just serve
 * the bytes without touching the segment.  Returns 1 if a version was found and
 * out filled, 0 if the page is unwritten (caller zero-fills).
 */
int
read_resolve(uint32_t timeline, const PsKey *key, uint32_t block,
			 uint64_t read_lsn, unsigned char *out)
{
	uint32_t	tl = timeline;
	uint64_t	rl = read_lsn;

	for (;;)
	{
		PageEnt    *e = page_find(tl, key, block);
		PageVer    *pv = e ? page_visible(e, rl) : NULL;

		if (pv)
		{
			uint64_t	l;

			/* The memtable / image-layer fast paths identify a version only by
			 * lsn; if that lsn is ambiguous (a same-lsn rewrite, e.g. hint bits),
			 * a same-lsn hit may be an older version than the authoritative latest
			 * append, so serve from the segment (pv's exact location) instead. */
			if (!page_lsn_ambiguous(e, pv->lsn))
			{
				if (g_memtable &&
					ps_memtable_lookup(g_memtable, tl, key, block, rl, &l, out) &&
					l == pv->lsn)
				{
					rr_mem++;
					return 1;	/* served from the memtable */
				}
				if (layer_map_lookup(tl, key, block, rl, &l, out) && l == pv->lsn)
				{
					rr_layer++;
					return 1;	/* served from an image layer */
				}
			}
			rr_seg++;
			return read_version(pv, out) == 0 ? 1 : 0;	/* segment fallback */
		}
		if (!timeline_has_parent(tl))
			return 0;
		if (timelines[tl].branch_lsn < rl)
			rl = timelines[tl].branch_lsn;
		tl = (uint32_t) timelines[tl].parent;
	}
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

/* Flush the memtable and close the manifest on a clean shutdown, so a restart
 * rebuilds the full state from layers without scanning segments. */
void
ps_core_close(void)
{
	if (g_memtable)
	{
		ps_memtable_flush(g_memtable, alloc_layer_id, record_layer, NULL);
		ps_memtable_destroy(g_memtable);
		g_memtable = NULL;
	}
	ps_manifest_close();
}

/*
 * Open the store and rebuild all in-memory state from it: define the root
 * timeline, load persisted branches, rebuild the page/fork indexes from the
 * image layers (falling back to a segment scan only for a store that has no
 * layers yet -- e.g. a pre-LSM store being migrated), and recompute each
 * timeline's shipped-WAL end LSN.  The frontend must set page_size,
 * segment_size and ps_storage beforehand.
 */
int
ps_core_open(const char *store_dir)
{
	if (ps_storage->open(store_dir, segment_size) != 0)
		return -1;
	if (ps_layer_store->open(store_dir) != 0)
		return -1;
	if (ps_manifest_open(store_dir) != 0)
		return -1;
	if (ps_manifest_replay(&ps_layer_map) != 0)
		return -1;
	if (use_layers)
		gc_resume();			/* finish any GC interrupted by a crash */

	/* layer ids continue past the highest one the manifest restored */
	g_next_layer_id = 1;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id >= g_next_layer_id)
			g_next_layer_id = ps_layer_map.layers[i].layer_id + 1;

	/* the LSM write side (memtable/flush/compaction) runs only when layers are
	 * the read path; the SPDK daemon stays on the segment path for now */
	if (use_layers)
	{
		g_memtable = ps_memtable_create(page_size, (uint32_t) flush_pages);
		if (!g_memtable)
			return -1;
	}
	fprintf(stderr, "pagestore_core: %u image layer(s) in map after manifest "
			"replay (next layer id %llu)\n", ps_layer_map.nlayers,
			(unsigned long long) g_next_layer_id);

	/* timeline 0 is the root; load any persisted branches, then rebuild data */
	timeline_define(0, -1, 0);
	load_timelines();
	/*
	 * Rebuild the authoritative page index from the complete segment log.  The
	 * segment log is the source of truth: every acknowledged write is appended
	 * there before it enters the memtable, and the memtable is only flushed at a
	 * threshold or on clean shutdown.  A crash -- or a failed shutdown flush --
	 * can therefore leave durable segment records that no image layer covers; if
	 * we rebuilt only from the layer indexes those newer versions would be
	 * dropped and acknowledged writes lost.  Scanning the segments recovers them.
	 * (The manifest-replayed layer map above still serves reads; skipping the
	 * already-flushed segment prefix via a persisted flush watermark is a
	 * deferred optimization.)
	 */
	recover();

	/* rebuild each timeline's shipped-WAL end LSN from its log */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined)
			wal_recover_one(tl);

	return 0;
}
