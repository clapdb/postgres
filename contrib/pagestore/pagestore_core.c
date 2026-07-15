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
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_layer_store.h"
#include "pagestore_manifest.h"
#include "pagestore_memtable.h"
#include "pagestore_pgcache.h"

/* configuration, set by the frontend before ps_core_open() */
uint32_t	page_size = PS_DEFAULT_PAGE_SIZE;
uint64_t	segment_size = 8 * 1024 * 1024;
int			flush_pages = 256;	/* memtable flush threshold (pages) */
int			compact_layers = 8;	/* compact a timeline past this many image layers */
int			cache_pages = 1024;	/* materialized-page cache size (pages; 0=off) */
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

/* configured logical shards for this daemon (set by frontend main before open()) */
uint32_t	ps_nshards = 1;

/*
 * Per-shard state (step 4 target in practice): one thread per shard owns index
 * / staging / cache lock-free; the shard is chosen from the logical key only
 * (block- and timeline-independent), so a key's blocks and all its timelines
 * stay on one shard.
 */
#define IDX_BUCKETS		(1 << 16)
#define IDX_MASK		(IDX_BUCKETS - 1)

struct PageEnt;
struct ForkEnt;
struct WalIdxEnt;

#define MAX_SHARDS		PS_MAX_CHANNELS
#define LAYER_ID_SHARD_BITS	16
#define LAYER_ID_LOCAL_BITS	(64 - LAYER_ID_SHARD_BITS)
#define LAYER_ID_SHARD_MASK	((uint64_t) (((uint64_t) 1 << LAYER_ID_SHARD_BITS) - 1) << LAYER_ID_LOCAL_BITS)
#define LAYER_ID_LOCAL_MASK	((uint64_t) ((1ULL << LAYER_ID_LOCAL_BITS) - 1))

typedef struct Shard
{
	struct PageEnt *page_idx[IDX_BUCKETS];	/* (timeline,key,block) -> versions */
	struct ForkEnt *fork_idx[IDX_BUCKETS];	/* (timeline,key) -> fork size */
	struct WalIdxEnt *walidx[IDX_BUCKETS];	/* (timeline,key,block) -> WAL lsns */
	PsMemtable *memtable;		/* staging -> image layers */
	uint32_t	id;					/* shard id [0..ps_nshards) this state belongs to */
	int			cur_seg;			/* segment id for append cursor */
	uint64_t	cur_off;			/* append cursor byte offset within cur_seg */
	uint64_t	next_layer_id;		/* next layer-local id for this shard */
	uint64_t	rr_mem,			/* read-source counters */
				rr_layer,
				rr_seg;
} Shard;

static Shard g_shards[MAX_SHARDS];

/*
 * Concurrency.  A per-shard rwlock guards each shard's in-memory state
 * (g_shards[i]: the page/fork/walidx indexes, memtable, append cursor and
 * layer-id cursor).  A single map_lock guards the cross-shard state: the global
 * ps_layer_map and the timelines[] array.  Lock order is always shard (outer)
 * then map (inner), never the reverse, so there is no deadlock.
 *
 * Each daemon worker owns exactly one shard and only ever touches its own
 * g_shards[]; the sole cross-worker accessor of another shard is maintenance
 * (worker 0), which takes the target shard's lock plus map_lock before
 * compacting it.  Reads take shard-rd + map-rd; ordinary writes take only
 * shard-wr, escalating to a brief map-wr inside append_page when a flush or
 * inline compaction mutates the map; branch creation takes map-wr alone.
 */
static pthread_rwlock_t shard_locks[MAX_SHARDS];
static pthread_rwlock_t map_lock = PTHREAD_RWLOCK_INITIALIZER;

void
ps_lock_shard_rd(uint32_t shard)
{
	pthread_rwlock_rdlock(&shard_locks[shard]);
}

void
ps_lock_shard_wr(uint32_t shard)
{
	pthread_rwlock_wrlock(&shard_locks[shard]);
}

void
ps_unlock_shard(uint32_t shard)
{
	pthread_rwlock_unlock(&shard_locks[shard]);
}

void
ps_lock_map_rd(void)
{
	pthread_rwlock_rdlock(&map_lock);
}

void
ps_lock_map_wr(void)
{
	pthread_rwlock_wrlock(&map_lock);
}

void
ps_unlock_map(void)
{
	pthread_rwlock_unlock(&map_lock);
}

static uint32_t
core_shards(void)
{
	if (ps_nshards == 0)
		return 1;
	return ps_nshards > PS_MAX_CHANNELS ? PS_MAX_CHANNELS : ps_nshards;
}

static uint32_t
layer_shard_from_id(uint64_t layer_id)
{
	return (uint32_t) ((layer_id & LAYER_ID_SHARD_MASK) >>
					   LAYER_ID_LOCAL_BITS);
}

static uint64_t
layer_local_id(uint64_t layer_id)
{
	return layer_id & LAYER_ID_LOCAL_MASK;
}

static uint64_t
layer_id(uint32_t shard, uint64_t local_id)
{
	return ((uint64_t) shard << LAYER_ID_LOCAL_BITS) | (local_id & LAYER_ID_LOCAL_MASK);
}

static Shard *
shard_for(const PsKey *key)
{
	uint32_t ns = core_shards();

	if (ns == 1 || !key)
		return &g_shards[0];
	return &g_shards[ps_key_shard(key, ns)];
}

/*
 * Shard index that will actually be touched for 'key' (klass-aware), so the
 * frontend can take the matching per-shard lock from the FINAL request key
 * rather than trusting a client-supplied channel shard.
 */
uint32_t
ps_shard_of(const PsKey *key)
{
	uint32_t	ns = core_shards();

	if (ns == 1 || !key)
		return 0;
	return ps_key_shard(key, ns);
}

uint32_t
ps_core_layer_count(void)
{
	return ps_layer_map.nlayers;
}

/* read-path source counters (memtable / image layer / segment fallback),
 * summed across shards */
void
ps_core_read_stats(uint64_t *mem, uint64_t *layer, uint64_t *seg)
{
	uint64_t	m = 0,
				l = 0,
				s = 0;
	uint32_t	ns = core_shards();

	for (uint32_t i = 0; i < ns; i++)
	{
		m += __atomic_load_n(&g_shards[i].rr_mem, __ATOMIC_RELAXED);
		l += __atomic_load_n(&g_shards[i].rr_layer, __ATOMIC_RELAXED);
		s += __atomic_load_n(&g_shards[i].rr_seg, __ATOMIC_RELAXED);
	}
	if (mem)
		*mem = m;
	if (layer)
		*layer = l;
	if (seg)
		*seg = s;
}

static uint64_t
alloc_layer_id(void *ctx)
{
	Shard *s = (Shard *) ctx;

	if (!s)
		s = &g_shards[0];
	return layer_id(s->id, s->next_layer_id++);
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
count_image_layers(uint32_t timeline, uint32_t shard)
{
	uint32_t	c = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting && d->timeline == timeline &&
			layer_shard_from_id(d->layer_id) == shard)
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
		/*
		 * Drop the manifest entry only after the file is gone (a missing file
		 * is ENOENT == success in delete_local_layer, so this is idempotent and
		 * a partially-deleted layer still completes).  A real unlink error keeps
		 * the layer "deleting" so the next start retries it.  A REMOVE_LAYER
		 * write error may have torn the manifest tail; stop so that record stays
		 * the recoverable tail instead of becoming interior corruption, and the
		 * next start retries from the last valid manifest state.
	 */
		if (ps_layer_store->delete_local_layer(&dead[k]) != 0)
			continue;
		if (ps_manifest_remove_layer(dead[k].layer_id) != 0)
			break;
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
compact_timeline(uint32_t timeline, uint32_t shard)
{
	PsLayerDesc *old;
	uint32_t	nold = count_image_layers(timeline, shard);
	PsImgRec   *recs = NULL;
	unsigned char **pages = NULL;
	uint32_t	nrec = 0,
				cap = 0;
	uint64_t	nid;
	PsLayerDesc newdesc;
	int			rc = -1;

	if (nold < 2)
		return 0;				/* nothing worth merging */

	/*
	 * Never compact a poisoned manifest: the new layer could not be recorded, so
	 * we would just write an unreferenced file and the old layers would stay live
	 * -- maintenance would keep retrying and leaking files.  Bail (the daemon is
	 * already rejecting writes; a restart recovers the manifest).
	 */
	if (ps_manifest_poisoned())
		return -1;

	old = malloc((size_t) nold * sizeof(PsLayerDesc));
	if (!old)
		return -1;
	nold = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		const PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting &&
			d->timeline == timeline &&
			layer_shard_from_id(d->layer_id) == shard)
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
				PsImgRec   *nr = realloc(recs, (size_t) nc * sizeof(PsImgRec));
				unsigned char **np = realloc(pages, (size_t) nc * sizeof(*pages));

				if (!nr || !np)
				{
					free(nr ? nr : recs);
					free(np ? np : pages);
					recs = NULL;
					pages = NULL;
					free(idx);
					goto cleanup;
				}
				recs = nr;
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
	nid = alloc_layer_id(&g_shards[shard]);
	if (ps_image_layer_write(nid, timeline, recs, nrec, page_size,
							 &newdesc) != 0 || record_layer(NULL, &newdesc) != 0)
		goto cleanup;
	for (uint32_t k = 0; k < nold; k++)
	{
		/*
		 * Fail safe at every step.  Only delete the file once the layer is
		 * durably marked deleting, and only drop it from the manifest once the
		 * file is gone -- so a failed step leaves a readable layer (its data is
		 * also in the new layer) that gc_resume() retries on the next start,
		 * never a manifest entry pointing at a deleted file.
		 *
		 * A manifest write error may have left a torn record at the tail; STOP
		 * before unlinking or appending anything more, so that torn record stays
		 * the recoverable tail rather than becoming interior corruption that
		 * fails replay (and so we never unlink a file whose later delete mark is
		 * not durable).  A failed unlink is not a manifest error: the layer is
		 * durably deleting, so we can move on and let gc_resume() retry it.
		 */
		if (ps_manifest_mark_delete(old[k].layer_id) != 0)
			goto cleanup;		/* incomplete: old layers stay live, count not cut */
		if (ps_layer_store->delete_local_layer(&old[k]) != 0)
			continue;			/* still "deleting"; gc_resume() will retry */
		if (ps_manifest_remove_layer(old[k].layer_id) != 0)
			goto cleanup;		/* incomplete */
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

#define SEG_MAGIC		 0x53454732 /* "SEG2": v2 record (PsKey gained klass) */
#define SEG_WALLESS_MAGIC 0x53454730 /* "SEG0": zero-version record + growth floor */
#define SEG_WALLESS_ORDERED_MAGIC 0x53454731 /* "SEG1": SEG0 + required order marker */
#define SEG_CLAMPED_ORDERED_MAGIC 0x53454733 /* "SEG3": clamped version + marker */

/*
 * On-disk layout of one appended page version: this header immediately
 * followed by 'len' page bytes.  The header is self-describing (carries the
 * full key/block/lsn), which is what lets recover() rebuild the entire
 * in-memory index by scanning segments sequentially -- no separate index file
 * to keep in sync.
 */
typedef struct SegRecHdr
{
	uint32_t	magic;			/* one of the SEG*_MAGIC values above */
	uint32_t	timeline;		/* timeline the version belongs to */
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;			/* version LSN, or SEG0's fork-growth floor */
	uint32_t	len;			/* page bytes following the header */
} SegRecHdr;

/*
 * Segments are addressed by (id, byte offset); how they are stored is the
 * storage backend's business (see pagestore_storage.h).  Here we keep only the
 * append cursor (cur_seg, cur_off) marking where the next record goes.
 */
/* append cursors are kept per-shard in g_shards[].cur_seg / cur_off */

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
/*
 * Fork-size history event.  GROW events come from page appends (the block's
 * pd_lsn -- exact: a block is readable as of a horizon iff it has a version
 * at/below it) and zero-extends (the backend's stamped WAL position); SET
 * events from create (0) and truncate (the new size); DEAD from unlink.
 * SET/DEAD are definitive: they end an as-of resolution at their timeline
 * hop, where plain growth still combines with ancestor sizes (a branch that
 * wrote only some blocks inherits the rest by read-through).
 */
typedef struct ForkEvent
{
	uint64_t	lsn;
	uint32_t	nblocks;
	uint8_t		kind;
} ForkEvent;

#define FEV_GROW	0
#define FEV_SET		1
#define FEV_DEAD	2
#define FEV_MIGRATED 3			/* log marker: legacy lsn-0 migration completed */
#define FEV_MIGRATING 4			/* log marker: legacy migration started */
#define FEV_SEG_GROW 5			/* ordering placeholder, activated by segment replay */
#define FEV_SEG_COMMIT 6		/* ordered segment commit that does not change size */

typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	nblocks;		/* newest size (cache of the event history) */
	ForkEvent  *ev;				/* lsn-ordered size history */
	uint32_t	nev;
	uint32_t	evcap;
	uint64_t	last_def_lsn;	/* newest SET/DEAD lsn (growth-clamp floor) */
} ForkEnt;

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

/*
 * Branch-local usage marker: set once a timeline acquires any local state (a
 * page version, fork entry, WAL-index entry, or shipped WAL).  An
 * exact-match duplicate CREATE_BRANCH is accepted only while the timeline is
 * still unused: that keeps a prepare retry idempotent (retries happen before
 * a compute ever boots on the branch), while reusing the id of a live branch
 * is refused -- read_through() resolves timeline-local versions before the
 * parent snapshot, so a "fresh" branch recreated over a written timeline
 * would silently serve the previous branch's pages.
 */
static int timeline_used[MAX_TIMELINES];

static inline void
timeline_mark_used(uint32_t timeline)
{
	if (timeline < MAX_TIMELINES)
		__atomic_store_n(&timeline_used[timeline], 1, __ATOMIC_RELEASE);
}

static inline int
timeline_is_used(uint32_t timeline)
{
	if (timeline >= MAX_TIMELINES)
		return 0;
	return __atomic_load_n(&timeline_used[timeline], __ATOMIC_ACQUIRE);
}

/* highest end LSN (start+len) of shipped WAL received per timeline */
static uint64_t wal_end[MAX_TIMELINES];

static inline uint64_t
wal_end_read(uint32_t timeline)
{
	if (timeline >= MAX_TIMELINES)
		return 0;
	return __atomic_load_n(&wal_end[timeline], __ATOMIC_ACQUIRE);
}

static inline void
wal_end_advance(uint32_t timeline, uint64_t end_lsn)
{
	uint64_t	old_end;

	if (timeline >= MAX_TIMELINES)
		return;
	old_end = __atomic_load_n(&wal_end[timeline], __ATOMIC_RELAXED);
	while (end_lsn > old_end &&
		   !__atomic_compare_exchange_n(&wal_end[timeline], &old_end,
										end_lsn, false,
										__ATOMIC_RELEASE,
										__ATOMIC_RELAXED))
		;
}

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
		a->relNumber == b->relNumber && a->forkNum == b->forkNum &&
		a->klass == b->klass;
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
	Shard	   *s = shard_for(key);
	PageEnt    *e;

	for (e = s->page_idx[h & IDX_MASK]; e; e = e->next)
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
				 uint64_t lsn, uint32_t shard, int seg, uint64_t off)
{
	uint32_t	h = page_hash(timeline, key, block);
	Shard	   *s = shard_for(key);
	PageEnt    *e = page_find(timeline, key, block);

	timeline_mark_used(timeline);
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->block = block;
		e->next = s->page_idx[h & IDX_MASK];
		s->page_idx[h & IDX_MASK] = e;
	}
	if (e->nver == e->cap)		/* grow the version array geometrically */
	{
		e->cap = e->cap ? e->cap * 2 : 2;
		e->vers = realloc(e->vers, (size_t) e->cap * sizeof(PageVer));
	}
	e->vers[e->nver].shard = shard;
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

static int fork_meta_persist(uint32_t timeline, const PsKey *key, uint64_t lsn,
							 uint32_t nblocks, uint8_t kind);

static ForkEnt *
fork_find(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	Shard	   *s = shard_for(key);
	ForkEnt    *e;

	for (e = s->fork_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && key_eq(&e->key, key))
			return e;
	return NULL;
}

static ForkEnt *
fork_get_or_create(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	Shard	   *s = shard_for(key);
	ForkEnt    *e = fork_find(timeline, key);

	timeline_mark_used(timeline);
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->nblocks = 0;
		e->next = s->fork_idx[h & IDX_MASK];
		s->fork_idx[h & IDX_MASK] = e;
	}
	return e;
}

/*
 * Resolve one timeline hop's contribution to an as-of size/existence query.
 * Scans the event history newest-first below the cap: the newest SET/DEAD is
 * definitive for this hop (with any GROW above it still counted -- writes to
 * a dead or truncated fork re-extend it); bare GROWs are a lower bound that
 * still combines with ancestor hops.
 */
#define FORK_HOP_NONE	0		/* no events at/below the cap */
#define FORK_HOP_GROW	1		/* growth only: combine with ancestors */
#define FORK_HOP_DEF	2		/* definitive size (SET, or DEAD then regrown) */
#define FORK_HOP_DEAD	3		/* definitively unlinked at the cap */

static int
fork_asof_hop(const ForkEnt *e, uint64_t cap, uint32_t *nb_out)
{
	uint32_t	grow = 0;
	int			have_grow = 0;

	*nb_out = 0;
	for (int i = (int) e->nev - 1; i >= 0; i--)
	{
		const ForkEvent *v = &e->ev[i];

		if (v->lsn > cap)
			continue;
		if (v->kind == FEV_SEG_GROW || v->kind == FEV_SEG_COMMIT)
			continue;			/* marker alone never changes fork size */
		if (v->kind == FEV_GROW)
		{
			if (v->nblocks > grow)
				grow = v->nblocks;
			have_grow = 1;
			continue;
		}
		if (v->kind == FEV_DEAD)
		{
			if (!have_grow)
				return FORK_HOP_DEAD;
			*nb_out = grow;
			return FORK_HOP_DEF;
		}
		/* FEV_SET */
		*nb_out = v->nblocks > grow ? v->nblocks : grow;
		return FORK_HOP_DEF;
	}
	if (have_grow)
	{
		*nb_out = grow;
		return FORK_HOP_GROW;
	}
	return FORK_HOP_NONE;
}

/* Size of e as of cap, hop-local (for the GROW-dedup below). */
static uint32_t
fork_size_asof_hop(const ForkEnt *e, uint64_t cap)
{
	uint32_t	nb;

	(void) fork_asof_hop(e, cap, &nb);
	return nb;
}

/*
 * Record a fork-size event, keeping the history lsn-ordered (equal LSNs keep
 * arrival order, so a later definitive event at the same LSN wins a
 * newest-first scan).  GROW events that do not raise the size visible at
 * their own LSN are dropped: steady-state rewrites of existing blocks at ever
 * newer pd_lsns add nothing, so the history stays O(distinct sizes).
 */
static void
fork_event_add(ForkEnt *e, uint64_t lsn, uint32_t nblocks, uint8_t kind)
{
	uint32_t	i;

	if (kind == FEV_GROW && fork_size_asof_hop(e, lsn) >= nblocks)
		return;
	if (kind != FEV_GROW && lsn > e->last_def_lsn)
		e->last_def_lsn = lsn;
	if (e->nev == e->evcap)
	{
		e->evcap = e->evcap ? e->evcap * 2 : 4;
		e->ev = realloc(e->ev, e->evcap * sizeof(ForkEvent));
	}
	i = e->nev;
	while (i > 0 && e->ev[i - 1].lsn > lsn)
	{
		e->ev[i] = e->ev[i - 1];
		i--;
	}
	e->ev[i].lsn = lsn;
	e->ev[i].nblocks = nblocks;
	e->ev[i].kind = kind;
	e->nev++;

	/*
	 * Maintain the newest-size scalar.  A tail insert governs directly.  A
	 * non-tail insert (an out-of-order page flush, or segment replay behind
	 * the pre-loaded fork-meta log) only matters if no definitive event lies
	 * above it: a GROW then raises the newest size; a non-tail SET/DEAD is
	 * not produced by any live path (mutations stamp at/after the newest
	 * event), so just recompute -- rare, correctness first.
	 */
	if (i == e->nev - 1)
	{
		if (kind == FEV_GROW)
		{
			if (nblocks > e->nblocks)
				e->nblocks = nblocks;
		}
		else
			e->nblocks = (kind == FEV_SET) ? nblocks : 0;
	}
	else if (kind == FEV_GROW)
	{
		for (uint32_t j = e->nev - 1; j > i; j--)
			if (e->ev[j].kind == FEV_SET || e->ev[j].kind == FEV_DEAD)
				return;			/* covered by a newer definitive event */
		if (nblocks > e->nblocks)
			e->nblocks = nblocks;
	}
	else
		e->nblocks = fork_size_asof_hop(e, UINT64_MAX);
}

/*
 * Preserve a segment growth's position among equal-LSN fork-meta events without
 * making the marker itself a size event.  Recovery activates the placeholder
 * only after validating the matching segment header and complete page body.
 */
static void
fork_event_add_seg_marker(ForkEnt *e, uint64_t lsn, uint32_t nblocks,
						  uint8_t kind)
{
	uint32_t	i;

	if (e->nev == e->evcap)
	{
		e->evcap = e->evcap ? e->evcap * 2 : 4;
		e->ev = realloc(e->ev, e->evcap * sizeof(ForkEvent));
	}
	i = e->nev;
	while (i > 0 && e->ev[i - 1].lsn > lsn)
	{
		e->ev[i] = e->ev[i - 1];
		i--;
	}
	e->ev[i].lsn = lsn;
	e->ev[i].nblocks = nblocks;
	e->ev[i].kind = kind;
	e->nev++;
}

static int
fork_event_activate_seg(ForkEnt *e, uint64_t lsn, uint32_t nblocks)
{
	for (uint32_t i = 0; i < e->nev; i++)
	{
		ForkEvent  *v = &e->ev[i];

		if ((v->kind == FEV_SEG_GROW || v->kind == FEV_SEG_COMMIT) &&
			v->lsn == lsn &&
			v->nblocks == nblocks)
		{
			if (v->kind == FEV_SEG_GROW)
			{
				v->kind = FEV_GROW;
				e->nblocks = fork_size_asof_hop(e, UINT64_MAX);
			}
			else
			{
				memmove(v, v + 1, (e->nev - i - 1) * sizeof(*v));
				e->nev--;
			}
			return 1;
		}
	}
	return 0;
}

int
fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
		  uint64_t lsn)
{
	ForkEnt    *e = fork_get_or_create(timeline, key);

	/*
	 * Zeroextend has no page record from which recovery can reconstruct its
	 * size.  Clamp first, then persist exactly the event applied in memory.
	 */
	if (lsn < e->last_def_lsn || lsn == 0)
		lsn = e->last_def_lsn;
	if (fork_size_asof_hop(e, lsn) < to_nblocks &&
		fork_meta_persist(timeline, key, lsn, to_nblocks, FEV_GROW) != 0)
		return -1;			/* not durable: do not apply in memory */
	fork_event_add(e, lsn, to_nblocks, FEV_GROW);
	return 0;
}

/* Apply growth whose durability is already represented by metadata/segment. */
static void
fork_grow_apply(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
				uint64_t lsn)
{
	fork_event_add(fork_get_or_create(timeline, key), lsn, to_nblocks,
				   FEV_GROW);
}

/*
 * Segment-log replay variant: insert the record's growth verbatim.  Raw
 * nonzero LSNs below a definitive event are REAL pre-truncate history here
 * (the meta log is fully preloaded, so the floor visible now can postdate
 * the record's live order); they stay in place, covered by the later SET.
 * LSN-0 records are skipped by the caller in the normal case -- their live
 * clamped position was persisted -- except in legacy mode (see recover).
 */
static void
fork_grow_replay(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
				 uint64_t lsn)
{
	fork_event_add(fork_get_or_create(timeline, key), lsn, to_nblocks,
				   FEV_GROW);
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
 * Ancestry iterator.  A read on a branch resolves against the branch and then
 * each ancestor, with read_lsn frozen at each branch point so the branch sees a
 * snapshot of the parent as of the fork.  Several walks (read_through,
 * read_resolve, walidx_get, the fork-size/exists walks) repeated this loop; this
 * captures it once.  Usage:
 *
 *		TlWalk w = tl_walk_first(timeline, read_lsn);
 *		do {
 *			... use w.tl and w.lsn ...
 *		} while (tl_walk_next(&w));
 *
 * Size/existence walks that don't care about LSN pass any read_lsn and ignore
 * w.lsn; the capping is harmless to them.
 */
typedef struct TlWalk
{
	uint32_t	tl;				/* current ancestry level */
	uint64_t	lsn;			/* read_lsn capped to this level's fork point */
} TlWalk;

static inline TlWalk
tl_walk_first(uint32_t timeline, uint64_t read_lsn)
{
	TlWalk		w = {timeline, read_lsn};

	return w;
}

/* Advance to the parent, capping lsn at the branch point; 0 at the root. */
static inline int
tl_walk_next(TlWalk *w)
{
	if (!timeline_has_parent(w->tl))
		return 0;
	if (timelines[w->tl].branch_lsn < w->lsn)
		w->lsn = timelines[w->tl].branch_lsn;
	w->tl = (uint32_t) timelines[w->tl].parent;
	return 1;
}

/*
 * Validate a branch-creation request before it is recorded.  read_through() and
 * the fork-size walks follow the parent chain assuming it is finite and well
 * formed, so a bad CREATE_BRANCH must be rejected rather than persisted.  Refuse:
 *	- a new id that is out of range, or an already-defined id with mismatched
 *	  ancestry metadata (an exact match is an idempotent retry, but only while
 *	  the timeline is still unused -- see timeline_used[]);
 *	- a parent that is out of range or not yet defined (the requested parent must
 *	  actually exist, else the branch silently inherits from nothing);
 *	- a parent whose ancestry already reaches the new id, which would turn the
 *	  parent walk into an infinite loop (e.g. new == parent, or A->B->A).
 * Returns 1 if (new_tl, parent, branch_lsn) can be used for CREATE_BRANCH.
 */
static int
branch_request_ok(uint32_t new_tl, int parent, uint64_t branch_lsn)
{
	/*
	 * Exact matches to an existing definition are idempotent retries -- but
	 * only while the timeline has no branch-local state yet.  Once it has
	 * pages, forks or shipped WAL, the duplicate is timeline-id reuse, not a
	 * retry, and accepting it would hand the caller the old branch's data.
	 */
	if (new_tl < MAX_TIMELINES && timelines[new_tl].defined)
		return timelines[new_tl].parent == parent &&
			timelines[new_tl].branch_lsn == branch_lsn &&
			!timeline_is_used(new_tl) && wal_end_read(new_tl) == 0;

	if (new_tl == 0 || new_tl >= MAX_TIMELINES || parent < 0 ||
		parent >= MAX_TIMELINES || !timelines[parent].defined)
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

static int
branch_exists_with_metadata(uint32_t tl, int parent, uint64_t branch_lsn)
{
	return tl < MAX_TIMELINES &&
		timelines[tl].defined &&
		timelines[tl].parent == parent &&
		timelines[tl].branch_lsn == branch_lsn;
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
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		PageEnt    *e = page_find(w.tl, key, block);
		PageVer    *v = e ? page_visible(e, w.lsn) : NULL;

		if (v)
			return v;
	} while (tl_walk_next(&w));
	return NULL;
}

/*
 * Fork size visible on 'timeline' as of read_lsn (UINT64_MAX = newest): walk
 * the ancestry, capping the horizon at each branch point exactly like page
 * reads do, and resolve each hop against its size history.  A definitive hop
 * (truncate/create/unlink) ends the walk -- a branch that truncated must not
 * re-inherit the parent's larger size; bare growth combines by max, because a
 * branch that wrote only some blocks inherits the rest by read-through.  The
 * per-hop horizon capping also fixes the old unversioned behavior where a
 * parent growing a fork after the branch point leaked the larger size into
 * the branch.
 */
static uint32_t
fork_nblocks_through(uint32_t timeline, const PsKey *key, uint64_t read_lsn)
{
	uint32_t	maxnb = 0;
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e)
		{
			uint32_t	nb;
			int			r = fork_asof_hop(e, w.lsn, &nb);

			if (r == FORK_HOP_DEAD)
				return maxnb;
			if (r == FORK_HOP_DEF)
				return nb > maxnb ? nb : maxnb;
			if (r == FORK_HOP_GROW && nb > maxnb)
				maxnb = nb;
		}
	} while (tl_walk_next(&w));
	return maxnb;
}

/* Does the fork exist on 'timeline' or any ancestor, as of read_lsn? */
static int
fork_exists_through(uint32_t timeline, const PsKey *key, uint64_t read_lsn)
{
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e)
		{
			uint32_t	nb;
			int			r = fork_asof_hop(e, w.lsn, &nb);

			if (r == FORK_HOP_DEAD)
				return 0;
			if (r != FORK_HOP_NONE)
				return 1;
		}
	} while (tl_walk_next(&w));
	return 0;
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

static int
timeline_persist(uint32_t id, int parent, uint64_t branch_lsn)
{
	TimelineRec rec = {id, (int32_t) parent, branch_lsn};

	return ps_storage->meta_append(&rec, sizeof(rec));
}

/*
 * Fork-size events the segment log cannot reproduce -- create, truncate,
 * unlink, zero-extend -- are persisted here (the segment records themselves
 * re-derive every page-append GROW on recovery).  Same fixed-record
 * append-only discipline as the timeline log.
 */
typedef struct ForkMetaRec
{
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} ForkMetaRec;

static int
fork_meta_persist(uint32_t timeline, const PsKey *key, uint64_t lsn,
				  uint32_t nblocks, uint8_t kind)
{
	ForkMetaRec rec;

	memset(&rec, 0, sizeof(rec));
	rec.timeline = timeline;
	rec.key = *key;
	rec.lsn = lsn;
	rec.nblocks = nblocks;
	rec.kind = kind;
	return ps_storage->fork_meta_append(&rec, sizeof(rec));
}

/*
 * Replay the fork-meta log before the segment scan.  Preloading definitive
 * events lets segment growth dedup and clamp detection see the complete size
 * history.  Segment-growth ordering placeholders retain their exact position
 * among equal-LSN metadata events and are activated only by a matching
 * complete segment record.  Invalid records are skipped, mirroring
 * load_timelines().
 */
static int fork_meta_migrating = 0;	/* the log carries the migration-start marker */
static int fork_meta_migrated = 0;	/* the log carries the migration-done marker */
static int fork_meta_legacy = 0;	/* replay lsn-0 records during a known migration */
static int fork_meta_migrate_failed = 0;	/* a migration persist failed this run */

static int
load_fork_meta(void)
{
	ForkMetaRec rec;
	uint64_t	off = 0;
	int			have_records = 0;

	while (ps_storage->fork_meta_read(off, &rec, sizeof(rec)) == (int) sizeof(rec))
	{
		have_records = 1;
		if (rec.kind == FEV_MIGRATED)
			fork_meta_migrated = 1;
		else if (rec.kind == FEV_MIGRATING)
			fork_meta_migrating = 1;
		else if ((rec.kind == FEV_SEG_GROW || rec.kind == FEV_SEG_COMMIT) &&
				 rec.timeline < MAX_TIMELINES)
			fork_event_add_seg_marker(
				fork_get_or_create(rec.timeline, &rec.key),
				rec.lsn, rec.nblocks, rec.kind);
		else if (rec.kind <= FEV_DEAD && rec.timeline < MAX_TIMELINES)
		{
			fork_event_add(fork_get_or_create(rec.timeline, &rec.key),
						   rec.lsn, rec.nblocks, rec.kind);
		}
		else
			fprintf(stderr, "pagestore: skipping invalid fork-meta record "
					"(timeline=%u kind=%u)\n", rec.timeline, rec.kind);
		off += sizeof(rec);
	}
	/*
	 * Only an absent/empty log is unambiguously a pre-fork-events store.  A
	 * nonempty log without either marker was written by the immediately
	 * preceding format: its definitive SET/DEAD history is authoritative and
	 * replaying raw lsn-0 pages against it could resurrect a truncated fork.
	 *
	 * Stamp an empty log before scanning segments.  The start marker lets a
	 * later boot distinguish an interrupted migration (continue legacy replay)
	 * from that older, already-event-aware format (normal replay).  The daemon
	 * must not become writable until this marker and the final seal are durable.
	 */
	if (!have_records)
	{
		PsKey		zk;

		memset(&zk, 0, sizeof(zk));
		if (fork_meta_persist(0, &zk, 0, 0, FEV_MIGRATING) != 0)
		{
			fprintf(stderr, "pagestore: could not start the fork-meta migration\n");
			return -1;
		}
		else
			fork_meta_migrating = 1;
		fork_meta_legacy = 1;
	}
	else
		fork_meta_legacy = fork_meta_migrating && !fork_meta_migrated;
	return 0;
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
		if (branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
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

/*
 * Overlap policy for shipped WAL: identical bytes are an idempotent re-ship
 * (an archiver retry after a partial failure) -- accepted, and skipped
 * entirely when the range is already fully covered, so no duplicate chunk
 * inflates the log or the distinct-byte accounting.  DIFFERING bytes for an
 * already-covered position are two histories claiming the same LSN range --
 * a divergent compute (a pinned reader's private WAL shipped after
 * unpinning, or a second writer on the timeline) trying to overwrite the
 * recorded one -- and are refused: later-chunks-win read semantics would
 * otherwise let the divergent copy silently rewrite history.  Returns -1 on
 * divergence or read failure; otherwise 0 with *covered_prefix set to the
 * length of the already-covered prefix, and *prefix_only set when the
 * coverage is exactly that prefix (no covered bytes beyond it) -- the shape
 * a retry of a partially shipped chunk has, and the only shape the caller
 * can trim to a clean uncovered suffix.
 */
static int
wal_overlap_check(uint32_t tl, uint64_t start_lsn,
				  const unsigned char *data, uint32_t len,
				  uint32_t *covered_prefix, int *prefix_only)
{
	uint64_t	off = 0;
	WalRecHdr	h;
	uint64_t	we = start_lsn + len;
	unsigned char *tmp = NULL;
	unsigned char *mask = NULL;
	uint32_t	covered = 0;

	*covered_prefix = 0;
	*prefix_only = 1;
	while (ps_storage->wal_read(tl, off, &h, sizeof(h)) == (int) sizeof(h) &&
		   h.magic == WAL_MAGIC)
	{
		uint64_t	rs = h.start_lsn;
		uint64_t	re = rs + h.len;
		uint64_t	os = rs > start_lsn ? rs : start_lsn;
		uint64_t	oe = re < we ? re : we;

		if (os < oe)
		{
			uint64_t	src = off + sizeof(h) + (os - rs);
			uint32_t	n = (uint32_t) (oe - os);
			uint32_t	base = (uint32_t) (os - start_lsn);

			if (!tmp)
			{
				tmp = malloc(len);
				mask = calloc(1, len);
				if (!tmp || !mask)
				{
					free(tmp);
					free(mask);
					return -1;
				}
			}
			if (ps_storage->wal_read(tl, src, tmp, n) != (int) n ||
				memcmp(tmp, data + base, n) != 0)
			{
				fprintf(stderr, "pagestore: refusing divergent WAL overlap on "
						"timeline %u at %llu (+%u): bytes differ from the "
						"already-shipped history\n",
						tl, (unsigned long long) os, n);
				free(tmp);
				free(mask);
				return -1;
			}
			for (uint32_t i = 0; i < n; i++)
				if (!mask[base + i])
				{
					mask[base + i] = 1;
					covered++;
				}
		}
		off += sizeof(h) + h.len;
	}
	if (mask)
	{
		uint32_t	i = 0;

		while (i < len && mask[i])
			i++;
		*covered_prefix = i;
		*prefix_only = (covered == i);
	}
	free(tmp);
	free(mask);
	return 0;
}

static int
wal_append(uint32_t tl, uint64_t start_lsn, const unsigned char *data,
		   uint32_t len)
{
	WalRecHdr	h;

	if (tl >= MAX_TIMELINES)
		return -1;

	/*
	 * Appends normally land strictly at/after the shipped end; only a
	 * re-ship (or a divergent history) reaches back below it, so the
	 * byte-compare scan runs only then.  An identical re-ship is trimmed
	 * to its uncovered suffix (fully covered = nothing to do), so no
	 * duplicate chunk ever lands and the distinct-byte accounting the read
	 * paths rely on stays exact.  Coverage that is not a clean prefix has
	 * no trimmable shape; the contiguous log never produces it, so refuse
	 * rather than distort the counts.
	 */
	if (len > 0 && start_lsn < wal_end_read(tl))
	{
		uint32_t	covered_prefix;
		int			prefix_only;

		if (wal_overlap_check(tl, start_lsn, data, len,
							  &covered_prefix, &prefix_only) != 0)
			return -1;
		if (covered_prefix == len)
			return 0;
		if (!prefix_only)
		{
			fprintf(stderr, "pagestore: refusing WAL re-ship with non-prefix "
					"overlap on timeline %u at %llu (+%u)\n",
					tl, (unsigned long long) start_lsn, len);
			return -1;
		}
		start_lsn += covered_prefix;
		data += covered_prefix;
		len -= covered_prefix;
	}

	timeline_mark_used(tl);
	h.magic = WAL_MAGIC;
	h.len = len;
	h.start_lsn = start_lsn;
	if (ps_storage->wal_append(tl, &h, sizeof(h), data, len) != 0)
		return -1;

	wal_end_advance(tl, start_lsn + len);
	return 0;
}

/* Fill 'out' from ONE timeline's log: the overlap of [start, start+len) with
 * [.., cap) and with each shipped chunk.  Bytes not covered are left as-is. */
static uint32_t
wal_read_one(uint32_t tl, uint64_t start, uint32_t len, uint64_t cap,
			 unsigned char *out)
{
	uint64_t	off = 0;
	WalRecHdr	h;
	uint32_t	filled = 0;
	uint64_t	we = start + len;

	if (we > cap)
		we = cap;
	if (we <= start)
		return 0;

	while (ps_storage->wal_read(tl, off, &h, sizeof(h)) == (int) sizeof(h) &&
		   h.magic == WAL_MAGIC)
	{
		uint64_t	rs = h.start_lsn;
		uint64_t	re = rs + h.len;
		uint64_t	os = rs > start ? rs : start;	/* overlap start */
		uint64_t	oe = re < we ? re : we; /* overlap end */

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

/* The LSN where a timeline's shipped log begins (its first chunk's start),
 * or UINT64_MAX for an empty log.  The log is contiguous from there: the
 * archiver ships completed segments strictly in order. */
static uint64_t
wal_log_start(uint32_t tl)
{
	WalRecHdr	h;

	if (ps_storage->wal_read(tl, 0, &h, sizeof(h)) == (int) sizeof(h) &&
		h.magic == WAL_MAGIC)
		return h.start_lsn;
	return UINT64_MAX;
}

/*
 * Read up to 'len' WAL bytes starting at WAL position 'start' from a
 * timeline's HISTORY into 'out'; returns the number of DISTINCT bytes
 * filled.  Bytes not covered by any shipped record are left as-is.  This is
 * what a redo worker (and the store-backed SLRU appliers) use to pull WAL
 * for replay.
 *
 * Read-through: a branch's history below its fork point lives in its
 * ancestors' logs -- but a branch's OWN first shipped segment can span the
 * fork (PostgreSQL copies the partial segment at the switch, and archiving
 * ships whole segments), so its log legitimately carries a pre-fork prefix
 * the parent may not have shipped yet.  Each hop therefore serves from its
 * own contiguous log coverage [log_start, ...) up to 'cap', and the next
 * (ancestor) hop's cap becomes min(cap, fork LSN, this hop's log_start):
 * the fork bound keeps ancestor-future records out of the branch's history,
 * and the log_start bound keeps the byte count exact -- whatever the child
 * already served below the fork, the parent must not serve again.  Timeline
 * metadata is write-once after definition (see timeline_define), so the
 * walk needs no lock, matching tl_walk.
 */
static uint32_t
wal_read(uint32_t tl, uint64_t start, uint32_t len, unsigned char *out)
{
	uint32_t	filled = 0;
	uint64_t	cap = UINT64_MAX;
	int			hops = 0;

	if (tl >= MAX_TIMELINES)
		return 0;

	for (;;)
	{
		uint64_t	ls = wal_log_start(tl);

		if (ls != UINT64_MAX && start + len > ls && start < cap)
		{
			uint64_t	ws = start > ls ? start : ls;

			filled += wal_read_one(tl, ws, (uint32_t) (start + len - ws),
								   cap, out + (ws - start));
		}

		/* everything below min(cap, fork, own coverage) is the parent's */
		if (!timeline_has_parent(tl))
			break;
		if (timelines[tl].branch_lsn < cap)
			cap = timelines[tl].branch_lsn;
		if (ls < cap)
			cap = ls;
		if (start >= cap)
			break;				/* window fully served at/above the bound */
		if (++hops > MAX_TIMELINES)
			break;				/* defensive: malformed chain */
		tl = (uint32_t) timelines[tl].parent;
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
		if (h.start_lsn + h.len > wal_end_read(tl))
		{
			timeline_mark_used(tl);
			wal_end_advance(tl, h.start_lsn + h.len);
		}
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

static void
walidx_add(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	uint32_t	h = page_hash(tl, key, block);
	Shard	   *s = shard_for(key);
	WalIdxEnt  *e;

	timeline_mark_used(tl);

	for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == tl && e->block == block && key_eq(&e->key, key))
			break;
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = tl;
		e->key = *key;
		e->block = block;
		e->next = s->walidx[h & IDX_MASK];
		s->walidx[h & IDX_MASK] = e;
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
walrec_cmp(const void *a, const void *b)
{
	uint64_t	la = ((const PsWalRec *) a)->lsn;
	uint64_t	lb = ((const PsWalRec *) b)->lsn;

	return (la > lb) - (la < lb);
}

static int
walidx_get(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn_max,
		   PsWalRec *out, int max_out)
{
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	int			got = 0;
	TlWalk		w = tl_walk_first(tl, lsn_max);

	do
	{
		uint32_t	h = page_hash(w.tl, key, block);
		WalIdxEnt  *e;

		for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
			if (e->timeline == w.tl && e->block == block && key_eq(&e->key, key))
			{
				/* tag each record with the timeline it lives on (w.tl) */
				for (int i = 0; i < e->n && got < max_out; i++)
					if (e->lsns[i] <= w.lsn)
					{
						out[got].lsn = e->lsns[i];
						out[got].timeline = w.tl;
						got++;
					}
				break;
			}
	} while (tl_walk_next(&w));

	/* gathered newest-timeline-first across the ancestry; redo (and the base
	 * search) need them in ascending LSN order */
	qsort(out, (size_t) got, sizeof(PsWalRec), walrec_cmp);
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
			const unsigned char *page, uint64_t version)
{
	SegRecHdr	hdr;
	uint64_t	reclen = sizeof(SegRecHdr) + page_size;
	uint64_t	data_off;
	uint64_t	hdr_grow_lsn = 0;
	uint64_t	page_version;
	int			clamped = 0;
	int			ordered_record = 0;
	int			segment_grows = 0;
	int			zero_version = 0;
	Shard	   *s = shard_for(key);
	ForkEnt    *fe = fork_find(timeline, key);
	uint64_t	branch_floor = 0;
	uint64_t	growth_floor = fe ? fe->last_def_lsn : 0;

	/* A branch-local version written after the branch snapshot must not become
	 * visible AT that snapshot merely because copied bytes retain an older
	 * source LSN.  The first representable local position is branch_lsn + 1. */
	ps_lock_map_rd();
	if (timeline_has_parent(timeline))
	{
		branch_floor = timelines[timeline].branch_lsn;
		if (branch_floor < UINT64_MAX)
			branch_floor++;
		if (branch_floor > growth_floor)
			growth_floor = branch_floor;
	}
	ps_unlock_map();

	/* roll over to a fresh segment when the current one would overflow */
	if (s->cur_seg < 0 || s->cur_off + reclen > segment_size)
	{
		s->cur_seg = (s->cur_seg < 0) ? 0 : s->cur_seg + 1;
		s->cur_off = 0;
	}

	hdr.magic = SEG_MAGIC;
	hdr.timeline = timeline;
	hdr.key = *key;
	hdr.block = block;
	/*
	 * Version key.  A relation page carries a real monotonic pd_lsn.  An SLRU or
	 * control object is versioned by the caller-supplied 'version' -- the
	 * dirtying/cutoff/update WAL LSN -- stored verbatim so it stays directly
	 * comparable to a branch's as-of cutoff (the SLRU seed path keys a snapshot
	 * by its proven cutoff C and reads it as-of L>=C; a control image is keyed
	 * by the LSN of the update that caused the write, so a branch restores the
	 * control state as of its fork point -- PGCONTROL_ON_STORE_DESIGN.md); a
	 * daemon counter would not be comparable.  Any other non-relation object
	 * carries no LSN in its bytes, so versioning it from page_lsn() could make
	 * an overwrite compare lower and silently lose (and poison the pgcache for
	 * that pseudo-LSN); derive a monotonic latest-wins version from the chain.
	 */
	if (key->klass == PS_KLASS_RELATION)
		hdr.lsn = page_lsn(page);
	else if (key->klass == PS_KLASS_SLRU || key->klass == PS_KLASS_CONTROL ||
			 key->klass == PS_KLASS_SLRU_LIVE || key->klass == PS_KLASS_SLRU_TOMB ||
			 key->klass == PS_KLASS_SLRU_WM)
		hdr.lsn = version;
	else
	{
		PageVer    *cur;

		/*
		 * Deriving an object's monotonic version walks the cross-shard timeline
		 * ancestry (read_through), which PS_OP_CREATE_BRANCH mutates under map_wr.
		 * The caller holds only this shard's write lock, so take map_rd for the
		 * walk -- otherwise an object write on a branch can race branch creation
		 * and read a partially updated parent/branch_lsn chain.  Drop it before
		 * the flush below re-takes map_wr; shard -> map order is preserved.
		 */
		ps_lock_map_rd();
		cur = read_through(timeline, key, block, UINT64_MAX);
		hdr.lsn = cur ? cur->lsn + 1 : 1;
		ps_unlock_map();
	}
	hdr.len = page_size;

	/*
	 * Below-floor growth cannot be ordered by the page's raw LSN.  Two
	 * shapes, two treatments:
	 *
	 * - A copied relation page with a NONZERO source pd_lsn below the
	 *   fork/branch floor (skip-WAL rewrites) has its RECORD stamped
	 *   at the floor: version visibility, the growth event and recovery
	 *   (which re-derives from the record) then all agree -- an as-of read
	 *   below the fork's creation sees neither the size nor the bytes, and
	 *   nothing needs a separate durable event.
	 *
	 * - A zero-version record must KEEP version 0 -- capped reads refuse
	 *   LSN-0 versions by design.  Its header stores the growth floor while
	 *   the in-memory page/object version remains zero.  Every below-floor or
	 *   zero-version record uses an ordered format and requires its inert
	 *   fork-meta commit marker at recovery, even when it rewrites an existing
	 *   block: a later same-LSN truncate/unlink must stay ordered after it.
	 */
	if (key->klass == PS_KLASS_RELATION)
	{
		if (hdr.lsn != 0 && hdr.lsn < growth_floor)
		{
			/* Every below-branch-point local copy is later than the snapshot,
			 * even when it rewrites an inherited block.  Definitive-event clamps
			 * retain the narrower grow/nonexistence test so old retained-block
			 * flushes keep their real pre-truncate LSN. */
			if (branch_floor != 0 && hdr.lsn < branch_floor)
				clamped = 1;
			else
			{
				uint32_t	visible;
				int			existed_before;

				ps_lock_map_rd();
				visible = fork_nblocks_through(timeline, key, growth_floor);
				existed_before = growth_floor > 0 &&
					fork_exists_through(timeline, key, growth_floor - 1);
				ps_unlock_map();
				clamped = visible < block + 1 || !existed_before;
			}
			if (clamped)
				hdr.lsn = growth_floor;
		}
	}
	if (hdr.lsn == 0)
	{
		hdr.magic = SEG_WALLESS_MAGIC;
		hdr.lsn = growth_floor;
		zero_version = 1;
	}
	hdr_grow_lsn = hdr.lsn;
	page_version = zero_version ? 0 : hdr.lsn;
	ordered_record = zero_version || clamped;
	segment_grows = (!fe ||
		fork_size_asof_hop(fe, hdr_grow_lsn) < block + 1);
	if (ordered_record)
		hdr.magic = zero_version ? SEG_WALLESS_ORDERED_MAGIC :
			SEG_CLAMPED_ORDERED_MAGIC;

	/* write header then page bytes contiguously at the append cursor */
	if (ps_storage->seg_write(s->id, s->cur_seg, s->cur_off, &hdr, sizeof(hdr)) != 0)
		return -1;
	data_off = s->cur_off + sizeof(hdr);
	if (ps_storage->seg_write(s->id, s->cur_seg, data_off, page, page_size) != 0)
		return -1;

	/*
	 * The segment record is the growth's durability; this metadata marker only
	 * records its position among equal-LSN definitive events.  Recovery ignores
	 * an unmatched marker, so a torn/missing segment cannot manufacture size.
	 */
	if (ordered_record &&
		fork_meta_persist(timeline, key, hdr_grow_lsn, block + 1,
						  segment_grows ? FEV_SEG_GROW : FEV_SEG_COMMIT) != 0)
		return -1;

	/* index points at the page bytes (data_off), so reads skip the header */
	page_add_version(timeline, key, block, page_version,
					 s->id, s->cur_seg, data_off);

	/*
	 * A same-LSN rewrite (latest-wins in the version chain) changes the
	 * authoritative bytes under an unchanged cache key; drop any cached copy
	 * so reads do not keep serving the pre-rewrite image.
	 */
	ps_pgcache_invalidate(timeline, key, block, page_version);
	s->cur_off += reclen;

	/*
	 * Stage the version for the LSM memtable and flush to an image layer when full
	 * (additive in phase 2 -- the segment write above is still authoritative).
	 *
	 * Skip all of this once the manifest is poisoned: record_layer() can no longer
	 * record a layer, so staging pages we can never flush would grow the memtable
	 * without bound (turning a metadata error into an OOM), and flushing would seal
	 * unreferenced layer files.  The page is durable in the segment log, which
	 * recovery scans, so the write still succeeds; reads fall back to the segment.
	 */
	if (s->memtable && !ps_manifest_poisoned())
	{
		ps_memtable_put(s->memtable, timeline, key, block, page_version, page);
		if (ps_memtable_full(s->memtable))
		{
			/* A flush (and any inline compaction) mutates the cross-shard
			 * ps_layer_map, so take map_lock here -- only on the rare flush, not
			 * on every write.  The caller already holds this shard's write lock,
			 * preserving the shard -> map order. */
			ps_lock_map_wr();
			ps_memtable_flush(s->memtable, alloc_layer_id, record_layer, s);
			/* backpressure only: normal compaction runs off the write path in
			 * ps_core_maintenance() when the daemon is idle.  Compact inline
			 * here only if the layer count is running away far past the
			 * threshold (writes outpacing background compaction). */
			if (count_image_layers(timeline, s->id) > (uint32_t) compact_layers * 4)
				compact_timeline(timeline, s->id);
			ps_unlock_map();
		}
	}

	/*
	 * Grow the fork's size history with this page's exact version LSN: a
	 * block is readable as of a horizon iff it has a version at/below it,
	 * so keying the GROW event by hdr.lsn makes as-of NBLOCKS agree with
	 * as-of page reads block for block.  (This replaces the callers'
	 * former one-shot fork_grow after a batch.)  SEG0 stores a zero-version
	 * page/object's growth floor in the same record while its version stays 0.
	 */
	fork_grow_apply(timeline, key, block + 1, hdr_grow_lsn);
	return 0;
}

/* Read a specific version's page bytes into out (page_size bytes). */
int
read_version(const PageVer *v, unsigned char *out)
{
	if (v->seg < 0)				/* layer-origin version (no segment copy) */
		return -1;
	if (ps_storage->seg_read(v->shard, v->seg, v->off, out, page_size) != 0)
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
			 uint64_t read_lsn, unsigned char *out, uint64_t *out_ver)
{
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		uint32_t	tl = w.tl;
		uint64_t	rl = w.lsn;
		PageEnt    *e = page_find(tl, key, block);
		PageVer    *pv = e ? page_visible(e, rl) : NULL;

		if (pv)
		{
			uint64_t	l;
			int			served;

			/*
			 * The materialized-page cache and the memtable are safe read sources
			 * only while they stay in lock-step with the segment log.  Once the
			 * manifest is poisoned, append_page() stops staging and a later
			 * same-pd_lsn rewrite lands in the segment only; both the cache (keyed
			 * by (tl,key,block,lsn)) and the memtable would then return the older
			 * bytes under the same LSN as the segment-backed pv.  When poisoned,
			 * bypass both and read the authoritative segment.
			 */
			int			poisoned = ps_manifest_poisoned();

			/* the resolved version (newest <= read_lsn); a caller that needs an
			 * exact-cutoff match -- e.g. an SLRU snapshot read -- compares it. */
			if (out_ver)
				*out_ver = pv->lsn;

			/* fast path: materialized-page cache, keyed by the resolved version */
			if (!poisoned && ps_pgcache_lookup(tl, key, block, pv->lsn, out))
				return 1;

			if (s->memtable && !poisoned &&
				ps_memtable_lookup(s->memtable, tl, key, block, rl, &l, out) &&
				l == pv->lsn)
			{
				__atomic_fetch_add(&s->rr_mem, 1, __ATOMIC_RELAXED);
				served = 1;		/* served from the memtable */
			}
			else if (pv->seg < 0 &&
					 layer_map_lookup(tl, key, block, rl, &l, out) &&
					 l == pv->lsn)
			{
				/*
				 * Serve from a layer only for a layer-origin version (no segment
				 * copy).  A segment-backed version must come from its segment: a
				 * layer match is keyed by LSN alone, so a same-pd_lsn rewrite in
				 * the segment would otherwise be masked by the older layer image.
				 */
				__atomic_fetch_add(&s->rr_layer, 1, __ATOMIC_RELAXED);
				served = 1;		/* served from an image layer */
			}
			else
			{
				__atomic_fetch_add(&s->rr_seg, 1, __ATOMIC_RELAXED);
				served = (read_version(pv, out) == 0);	/* segment fallback */
			}
			if (served)
				ps_pgcache_insert(tl, key, block, pv->lsn, out);
			return served ? 1 : 0;
		}
	} while (tl_walk_next(&w));
	return 0;
}

/*
 * Durable WAL retention floor for a timeline (PGCONTROL_ON_STORE_DESIGN.md).
 *
 * Every mirrored pg_control image is preceded by an 8-byte "floor note" --
 * the image's checkpoint redo pointer -- written as block 1 of the control
 * object at the same version LSN.  A control image is only restorable if the
 * WAL from its redo pointer onward still exists, so the retention floor for a
 * timeline is the minimum redo over every control image restorable on its
 * ancestry: all block-1 note versions, capped per ancestry level at the
 * branch point exactly as an as-of restore would be.  The notes live in the
 * ordinary segment log, so the floor survives a daemon restart via normal
 * recovery -- it is the durable authority the design requires, independent of
 * any transient compute-side state.
 *
 * Returns 0 when no control image exists (nothing constrains WAL yet).  Any
 * future shipped-WAL GC must refuse to drop WAL at or above this floor.
 */
int
wal_retain_floor(uint32_t timeline, uint64_t *floor_out)
{
	PsKey		key;
	TlWalk		w = tl_walk_first(timeline, UINT64_MAX);
	uint64_t	floor = 0;
	unsigned char *tmp = malloc(page_size);
	int			rc = 0;

	if (!tmp)
		return -1;				/* cannot prove a floor: fail closed */
	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;

	do
	{
		PageEnt    *notes = page_find(w.tl, &key, 1);
		PageEnt    *images = page_find(w.tl, &key, 0);

		if (notes)
		{
			for (int i = 0; i < notes->nver; i++)
			{
				PageVer    *v = &notes->vers[i];
				uint64_t	redo;

				/* only images restorable at this ancestry level count */
				if (v->lsn > w.lsn)
					continue;

				/*
				 * An unreadable note must fail the query, not be skipped: a
				 * WAL-GC caller acting on a floor that silently ignored a
				 * note could drop WAL a restorable image still needs.
				 */
				if (read_version(v, tmp) != 0)
				{
					rc = -1;
					goto done;
				}
				memcpy(&redo, tmp, sizeof(redo));

				/*
				 * A zero redo means a torn/corrupt note (the mirror never
				 * ships one: every control image carries a real redo
				 * pointer).  Its image's requirement is unknowable, so the
				 * floor collapses to "retain everything" -- returning a
				 * higher floor because the same-LSN coverage check was
				 * satisfied by a garbage note would under-retain.
				 */
				if (redo == 0)
				{
					floor = 1;
					goto done;
				}
				if (floor == 0 || redo < floor)
					floor = redo;
			}
		}

		/*
		 * Every restorable control image (block 0) must be covered by a
		 * note at the same version: an image without one (mirrored before
		 * the note format existed) has an unknowable redo pointer.  Old
		 * versions never leave the chain, so failing the query would brick
		 * the floor FOREVER on upgraded stores; instead collapse to the
		 * most conservative provable answer -- retain everything (floor =
		 * the lowest valid LSN) -- until version-level GC (M5) prunes the
		 * unnoted images away.
		 */
		if (images)
		{
			for (int i = 0; i < images->nver; i++)
			{
				PageVer    *v = &images->vers[i];
				int			covered = 0;

				if (v->lsn > w.lsn)
					continue;
				if (notes)
					for (int j = 0; j < notes->nver; j++)
						if (notes->vers[j].lsn == v->lsn)
						{
							covered = 1;
							break;
						}
				if (!covered)
				{
					floor = 1;
					goto done;
				}
			}
		}
	} while (tl_walk_next(&w));

done:
	free(tmp);
	if (rc == 0)
		*floor_out = floor;
	return rc;
}

/* ===================== recovery (rebuild index from segments) ========== */

/*
 * Rebuild the entire in-memory index at startup by replaying the segments in
 * order.  Each record is self-describing, so replaying append_page's effect
 * (page_add_version + fork_grow) for every record reconstructs both indexes and
 * leaves the append cursor positioned just past the last valid record.
 * CREATE/TRUNCATE/UNLINK and zero-extends leave no segment records; their
 * size events replay from the fork-meta log afterwards (load_fork_meta).
 * A partial/torn trailing record is simply treated as end-of-log (the
 * magic/len check below stops the scan).
 */
/*
 * Scan a shard's segment log and rebuild its version index, leaving the append
 * cursor just past the last valid record.  The segment log is append-only and
 * never deleted, so it is the complete, authoritative store: it holds every
 * version (including repeated writes at the same pd_lsn, in order) and any write a
 * layer flush did not record.  Recovery rebuilds the exact chain from it; image
 * layers are a future fast-recovery optimization that needs a durable flush
 * watermark before they can replace this scan.
 */
static void
recover(uint32_t shard)
{
	Shard	   *s = &g_shards[shard];

	s->cur_seg = -1;
	s->cur_off = 0;

	for (int id = 0;; id++)
	{
		uint64_t	off = 0;
		int64_t		seg_bytes = ps_storage->seg_size(shard, id);

		if (seg_bytes < 0)
			break;				/* no more segments -> done */

		/* replay records until one fails to validate (end of log).  seg_bytes is
		 * cached once here: the segment is not being written during recovery, and
		 * the POSIX backend's seg_size() is a stat() we must not pay per record. */
		for (;;)
		{
			SegRecHdr	hdr;
			int			ordered;
			int			order_activated = 0;
			int			wal_less;
			uint64_t	page_version;

			if (ps_storage->seg_read(shard, id, off, &hdr, sizeof(hdr)) != 0)
				break;			/* short read -> end of this segment's data */
			if (hdr.magic == 0)
				break;			/* zeroed slot -> normal end-of-log sentinel */
			wal_less = hdr.magic == SEG_WALLESS_MAGIC ||
				hdr.magic == SEG_WALLESS_ORDERED_MAGIC;
			ordered = hdr.magic == SEG_WALLESS_ORDERED_MAGIC ||
				hdr.magic == SEG_CLAMPED_ORDERED_MAGIC;
			if (hdr.magic != SEG_MAGIC && !wal_less && !ordered)
			{
				/*
				 * A non-zero magic that isn't ours is a record written by an
				 * incompatible (pre-klass) on-disk format.  Fail fast instead of
				 * treating it as end-of-log and overwriting it -- that would
				 * silently lose the existing store.  It must be recreated (or
				 * migrated offline) under the new format.
				 */
				fprintf(stderr, "pagestore_daemon: shard %u segment %d: incompatible "
						"record magic %#x at offset %llu; this store "
						"predates the current on-disk format and must be recreated\n",
						shard, id, hdr.magic,
						(unsigned long long) off);
				exit(1);
			}
			if (hdr.len != page_size)
				break;			/* our magic but a torn/short tail record */

			/*
			 * append_page() writes the header and the page body in separate
			 * writes, so after a crash a tail record can have a valid header while
			 * the body is missing or short.  Index a record only once the segment
			 * is large enough to hold its body; a torn body ends the log here so we
			 * never index a version whose bytes cannot be read (which would mask a
			 * good older version and zero-fill on read).
			 */
			if (seg_bytes < (int64_t) (off + sizeof(hdr) + hdr.len))
				break;

			/* Ordered records are committed by their matching inert fork-meta
			 * placeholder.  A complete body without that marker is the tail of a
			 * failed/unacknowledged append: leave the cursor here so it is
			 * overwritten, and publish neither its bytes nor its growth. */
			if (ordered)
			{
				order_activated = fork_event_activate_seg(
					fork_get_or_create(hdr.timeline, &hdr.key),
					hdr.lsn, hdr.block + 1);
				/* A deliberately synthesized/real pre-events store has no
				 * definitive metadata to misorder against.  Its segment records
				 * are the migration source even if their newer magic normally
				 * requires a marker that disappeared with the old metadata log. */
				if (!order_activated && !fork_meta_legacy)
					break;
			}

			page_version = wal_less ? 0 : hdr.lsn;
			page_add_version(hdr.timeline, &hdr.key, hdr.block, page_version,
							 shard, id, off + sizeof(hdr));
			/* Ordered SEG1/SEG3 growth was activated at its fork-meta position,
			 * while a non-growing commit marker was consumed without changing size.
			 * Legacy SEG0 carries zero-version growth at its stored floor,
			 * while ordinary SEG2 re-derives growth at its stored LSN.
			 * Markerless formats carry no reliable per-record correlation for
			 * deciding that a later GROW was a clamp rather than a real regrow,
			 * so guessing here would discard legitimate pre-truncate history.
			 *
			 * Legacy stores (an absent/empty fork-meta log, from before
			 * events were persisted) have no definitive events to misorder
			 * against; their lsn-0 growth replays through the PERSISTING
			 * path, which write-through-migrates it into the meta log --
			 * otherwise the flag would flip on the first new metadata
			 * append and the second restart would bring every unlogged
			 * fork back empty. */
			if (fork_meta_legacy)
			{
				ForkEnt    *fe = fork_get_or_create(hdr.timeline, &hdr.key);
				uint64_t	l = hdr.lsn ? hdr.lsn : fe->last_def_lsn;

				/* migrate (write-through) but NEVER drop the in-memory
				 * growth: a failed persist degrades to retry-next-boot
				 * (no marker), not to invisible blocks this run */
				if (fork_size_asof_hop(fe, l) < hdr.block + 1)
				{
					if (!fork_meta_migrate_failed &&
						fork_meta_persist(hdr.timeline, &hdr.key, l,
										   hdr.block + 1, FEV_GROW) != 0)
						fork_meta_migrate_failed = 1;
					fork_event_add(fe, l, hdr.block + 1, FEV_GROW);
				}
			}
			else if (!ordered && wal_less)
			{
				/* Markerless SEG0 predates ordered segment records. */
				fork_grow_replay(hdr.timeline, &hdr.key, hdr.block + 1,
								 hdr.lsn);
			}
			else if (!ordered && hdr.lsn != 0)
				fork_grow_replay(hdr.timeline, &hdr.key, hdr.block + 1,
								 hdr.lsn);
			off += sizeof(hdr) + hdr.len;
		}

		/* this segment is the newest seen so far; append continues after it */
		s->cur_seg = id;
		s->cur_off = off;
	}
	if (s->cur_seg >= 0)
		fprintf(stderr, "pagestore_daemon: recovered shard %u through segment %d (off %llu)\n",
				shard, s->cur_seg, (unsigned long long) s->cur_off);
}

/* ===================== request handling (non-I/O ops) ================== */

/*
 * Handle every request that needs no page byte I/O and return 1.  The four
 * byte-I/O ops (EXTEND/WRITEV/READV/READ_AT) -- and any unknown op -- return 0,
 * for the frontend to handle (synchronously for POSIX, async for SPDK).  The
 * frontend sets ch->status = OK and ch->result = 0 before calling.
 */
/*
 * Event LSN for a fork-mutating op.  The backend stamps req_lsn with its WAL
 * position; a legacy/test caller sending 0 gets the fork's newest known event
 * LSN, so the mutation orders after everything already recorded (visible to
 * newest reads, invisible to strictly older horizons -- fail-safe for
 * unstamped callers).
 */
static uint64_t
fork_op_lsn(const ForkEnt *e, uint64_t req_lsn)
{
	if (req_lsn != 0)
		return req_lsn;
	return e->nev ? e->ev[e->nev - 1].lsn + 1 : 1;
}

int
ps_handle_meta(PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE:
			/*
			 * Definitive existence from ch->req_lsn on (the backend stamps
			 * its WAL position; see fork_op_lsn for a legacy 0).  Re-creating
			 * a fork that already exists at that horizon is an idempotent
			 * retry or a redo replay: record nothing, so the event history
			 * is not polluted and no walk gets cut short by a stray SET 0.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn = fork_op_lsn(e, ch->req_lsn);
				uint32_t	nb;
				int			r = fork_asof_hop(e, lsn, &nb);

				if (r == FORK_HOP_NONE || r == FORK_HOP_DEAD)
				{
					if (fork_meta_persist(tl, &ch->key, lsn, 0, FEV_SET) != 0)
						ch->status = PS_STATUS_ERROR;
					else
						fork_event_add(e, lsn, 0, FEV_SET);
				}
			}
			break;

		case PS_OP_EXISTS:
			/* req_lsn caps the horizon; 0 = newest (the writer path) */
			ch->result = fork_exists_through(tl, &ch->key,
											 ch->req_lsn ? ch->req_lsn : UINT64_MAX) ? 1 : 0;
			break;

		case PS_OP_UNLINK:
			/*
			 * COW unlink: a durable DEAD event.  The entry and its history
			 * stay -- an as-of read below the unlink LSN must still see the
			 * fork -- so nothing is freed here.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn = fork_op_lsn(e, ch->req_lsn);

				if (fork_meta_persist(tl, &ch->key, lsn, 0, FEV_DEAD) != 0)
					ch->status = PS_STATUS_ERROR;
				else
					fork_event_add(e, lsn, 0, FEV_DEAD);
			}
			break;

		case PS_OP_NBLOCKS:
			/* req_lsn caps the horizon; 0 = newest (the writer path) */
			ch->result = fork_nblocks_through(tl, &ch->key,
											  ch->req_lsn ? ch->req_lsn : UINT64_MAX);
			break;

		case PS_OP_TRUNCATE:
			/*
			 * COW truncate: a durable SET event at the backend's stamped WAL
			 * position.  Historical versions of the trimmed blocks stay in
			 * the log; as-of reads below the truncate LSN still see the old
			 * size, at/above it the new one.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn = fork_op_lsn(e, ch->req_lsn);

				if (fork_meta_persist(tl, &ch->key, lsn, ch->nblocks, FEV_SET) != 0)
					ch->status = PS_STATUS_ERROR;
				else
					fork_event_add(e, lsn, ch->nblocks, FEV_SET);
			}
			break;

		case PS_OP_ZEROEXTEND:
			/*
			 * Allocation only: grow size, no page data stored (reads -> 0).
			 * The segment log has no record of it, so the GROW event must be
			 * persisted -- but only when it actually raises the size at its
			 * horizon, keeping the log as sparse as the in-memory dedup.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn = fork_op_lsn(e, ch->req_lsn);
				uint32_t	to = ch->blocknum + ch->nblocks;

				if (fork_grow(tl, &ch->key, to, lsn) != 0)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_CREATE_BRANCH:
			/*
			 * Instant clone: just record metadata.  Timeline ch->timeline forks
			 * from ch->parent_timeline at LSN ch->req_lsn.  No page data is
			 * copied -- the branch shares the parent's pages by read-through
			 * until it writes (copy-on-write).
			 */
			if (branch_request_ok(ch->timeline, (int) ch->parent_timeline,
								 ch->req_lsn))
			{
				if (timelines[ch->timeline].defined)
					break;
				if (timeline_persist(ch->timeline, (int) ch->parent_timeline,
								 ch->req_lsn) == 0)
					timeline_define(ch->timeline, (int) ch->parent_timeline,
									ch->req_lsn);
				else
					ch->status = PS_STATUS_ERROR;
			}
			else
				ch->status = PS_STATUS_ERROR;
			break;
		case PS_OP_CHECK_BRANCH:
			/*
			 * Validate a branch request without mutating timeline metadata.
			 * This keeps prepare/retry paths deterministic: invalid requests are
			 * rejected in-place before any SLRU directory mutation.
			 */
			if (branch_request_ok(ch->timeline, (int) ch->parent_timeline,
								 ch->req_lsn))
			{
				/* valid */
			}
			else
				ch->status = PS_STATUS_ERROR;
			break;
		case PS_OP_REQUIRE_BRANCH:
			/*
			 * Startup-time manifest validation: require the timeline to already
			 * exist with exactly the manifest ancestry metadata.  This is stricter
			 * than CHECK_BRANCH, which also accepts a request that would be legal
			 * to create.
			 */
			if (!branch_exists_with_metadata(ch->timeline,
											 (int) ch->parent_timeline,
											 ch->req_lsn))
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_APPEND:
			if (wal_append(tl, ch->req_lsn, ch->data, ch->datalen) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_SIZE:
			ch->req_lsn = wal_end_read(tl);	/* output: end LSN of this timeline's WAL */
			break;

		case PS_OP_WAL_READ:
			ch->result = wal_read(tl, ch->req_lsn, ch->datalen, ch->data);
			break;

		case PS_OP_WAL_INDEX_ADD:
			walidx_add(tl, &ch->key, ch->blocknum, ch->req_lsn);
			break;

		case PS_OP_WAL_INDEX_GET:
			ch->result = (uint32_t) walidx_get(tl, &ch->key, ch->blocknum,
											   ch->req_lsn, (PsWalRec *) ch->data,
											   (int) (PS_IO_UNIT / sizeof(PsWalRec)));
			break;

		case PS_OP_WAL_RETAIN_FLOOR:
			/*
			 * Durable WAL retention floor for this timeline's ancestry.  A
			 * floor that cannot be PROVEN (unreadable note, or a control
			 * image predating the note format) is an error, never a lower
			 * bound: a GC caller must retain everything in that case.
			 */
			{
				uint64_t	floor = 0;

				if (wal_retain_floor(tl, &floor) != 0)
					ch->status = PS_STATUS_ERROR;
				ch->req_lsn = floor;
				ch->result = (floor != 0);
			}
			break;

		case PS_OP_IMMEDSYNC:
			/*
			 * Surface the failure: callers (the pg_control mirror) pop their
			 * retry queues only after a successful sync, and an ignored
			 * ENOSPC/EIO here would let them treat pwrite-only images as
			 * durable.
			 */
			if (ps_storage->sync() != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		default:
			return 0;			/* a byte-I/O op (or unknown): frontend handles */
	}
	return 1;
}

/* ===================== lifecycle ====================================== */

/* Flush the memtable and close the manifest on a clean shutdown.  (The segment
 * log is authoritative, so a restart recovers from it regardless.) */
void
ps_core_close(void)
{
	uint32_t	ns = core_shards();

	/*
	 * Recovery rebuilds the index from the segment log, so the segments must be
	 * durable before we rely on them: fsync them on clean shutdown (writes between
	 * checkpoints are otherwise only in the OS page cache, and would be lost to a
	 * power failure after the daemon exits even though the write was acknowledged).
	 *
	 * Sync *before* flushing the memtable into image layers below.  Otherwise a
	 * power loss in the shutdown window could leave a just-committed layer as the
	 * only durable copy of pages whose segment bytes were still in the page cache,
	 * which segment-only recovery would miss.  Syncing first makes the recovery
	 * source durable before any layer is committed on top of it.
	 *
	 * If the sync fails (EIO/ENOSPC/...), the segment log -- the sole source
	 * segment-only recovery reads -- is not durable, so the about-to-be-destroyed
	 * memtable may be the only good copy of recent writes.  We cannot make it
	 * durable from here, so do not proceed to a clean-looking teardown that would
	 * mask the loss: report it and abort with a failure status before destroying
	 * the memtables, leaving the operator a clear signal to investigate.
	 */
	if (ps_storage->sync && ps_storage->sync() != 0)
	{
		fprintf(stderr, "pagestore_daemon: FATAL: segment sync failed on shutdown "
				"(%s); aborting before teardown -- recently acknowledged writes "
				"may not be durable\n", strerror(errno));
		_exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < ns; i++)
	{
		Shard	   *s = &g_shards[i];

		if (s->memtable)
		{
			ps_memtable_flush(s->memtable, alloc_layer_id, record_layer, s);
			ps_memtable_destroy(s->memtable);
			s->memtable = NULL;
		}
	}

	ps_pgcache_free();
	ps_manifest_close();
}

/*
 * Off-the-write-path background maintenance: compact one timeline whose image
 * layer count exceeds the (low-water) threshold.  The daemon calls this when it
 * is otherwise idle; doing at most one compaction per call keeps the serve loop
 * responsive.  Returns 1 if it compacted (the caller should not sleep), 0 if
 * nothing was due.
 */
int
ps_core_maintenance(void)
{
	uint32_t	ns;
	uint32_t	ftl = 0,
				fsh = 0;
	int			found = 0;
	int			did = 0;

	if (!use_layers)
		return 0;

	/*
	 * Back off all maintenance once the manifest is poisoned: compaction cannot
	 * record its replacement layer, so compact_timeline() returns immediately and
	 * reporting "did work" would spin the idle worker on the same timeline until
	 * restart.  Returning 0 lets it sleep until the manifest is recovered.
	 */
	if (ps_manifest_poisoned())
		return 0;
	ns = core_shards();

	/*
	 * Phase 1: scan under map read-lock to pick a timeline+shard whose image
	 * layers are due for compaction.  A shared lock here lets reads proceed.
	 */
	ps_lock_map_rd();
	for (uint32_t tl = 0; tl < MAX_TIMELINES && !found; tl++)
		for (uint32_t sh = 0; sh < ns; sh++)
			if ((tl == 0 || timelines[tl].defined) &&
				count_image_layers(tl, sh) > (uint32_t) compact_layers)
			{
				ftl = tl;
				fsh = sh;
				found = 1;
				break;
			}
	ps_unlock_map();

	/*
	 * Phase 2: compact the chosen shard under shard-wr + map-wr (the order other
	 * paths use), which excludes that shard's worker and other map mutators.
	 * Re-check under the write lock since the count may have changed.
	 */
	if (found)
	{
		ps_lock_shard_wr(fsh);
		ps_lock_map_wr();
		if (count_image_layers(ftl, fsh) > (uint32_t) compact_layers)
			compact_timeline(ftl, fsh);
		ps_unlock_map();
		ps_unlock_shard(fsh);
		did = 1;
	}

	/*
	 * Phase 3: rewrite the manifest log if add/seal/delete churn has grown it
	 * well past the live layer count, bounding replay time.  Independent of layer
	 * compaction; map-wr excludes the manifest appends a concurrent flush makes.
	 */
	ps_lock_map_rd();
	found = ps_manifest_should_compact();
	ps_unlock_map();
	if (found)
	{
		int			compacted = 0;

		ps_lock_map_wr();
		if (ps_manifest_should_compact())
			compacted = (ps_manifest_compact() == 0);
		ps_unlock_map();

		/*
		 * Only count a *successful* rewrite as work done.  A failed compaction
		 * leaves should_compact() true, so reporting "did work" would make the
		 * idle worker re-run maintenance immediately and busy-loop on the failing
		 * compaction; returning false here lets it sleep and retry on the next
		 * tick instead.  (An I/O failure also poisons the manifest, after which
		 * should_compact() returns false and the retries stop entirely.)
		 */
		if (compacted)
			did = 1;
	}

	return did;
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
	uint32_t	ns = core_shards();

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

	/* initialize per-shard state, locks and layer-id cursors */
	for (uint32_t i = 0; i < ns; i++)
	{
		g_shards[i].id = i;
		g_shards[i].cur_seg = -1;
		g_shards[i].cur_off = 0;
		g_shards[i].next_layer_id = 1;
		pthread_rwlock_init(&shard_locks[i], NULL);
	}
	/* layer ids continue past the highest one restored per shard */
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		uint32_t	sh = layer_shard_from_id(ps_layer_map.layers[i].layer_id);
		uint64_t	lid = layer_local_id(ps_layer_map.layers[i].layer_id);

		if (sh < ns && lid + 1 > g_shards[sh].next_layer_id)
			g_shards[sh].next_layer_id = lid + 1;
	}

	/* the LSM write side (memtable/flush/compaction) runs only when layers are
	 * the read path; the SPDK daemon stays on the segment path for now.  One
	 * memtable per shard. */
	if (use_layers)
		for (uint32_t i = 0; i < ns; i++)
		{
			g_shards[i].memtable = ps_memtable_create(page_size,
													  (uint32_t) flush_pages);
			if (!g_shards[i].memtable)
				return -1;
		}
	/* the materialized-page cache helps both read paths (read_resolve and the
	 * SPDK async path), so it is not gated on use_layers */
	ps_pgcache_init((uint32_t) cache_pages, page_size);
	fprintf(stderr, "pagestore_core: %u image layer(s) in map after manifest replay\n",
			ps_layer_map.nlayers);

	/* timeline 0 is the root; load any persisted branches, then rebuild data */
	timeline_define(0, -1, 0);
	load_timelines();

	/*
	 * Rebuild the version index from the segment log, the complete authoritative
	 * store (never deleted).  This recovers every acknowledged write -- including
	 * a tail the memtable flush never recorded in a layer (a crash, or a poisoned
	 * manifest) -- and repeated same-pd_lsn writes in order, which a layer rebuild
	 * keyed only by LSN could not.  Image layers are still loaded into the layer
	 * map (for compaction/GC) but are not used to rebuild the read index here.
	 */
	/*
	 * Definitive fork-size events (create/truncate/unlink/zero-extend) load
	 * BEFORE the segment scan: the GROW dedup in fork_event_add compares a
	 * grow against the size visible at its own LSN, and with the definitive
	 * events already in place the segment replay makes exactly the decisions
	 * the live path made (a regrow after a truncate must be kept even when
	 * it does not exceed the pre-truncate envelope).
	 */
	if (load_fork_meta() != 0)
		return -1;

	for (uint32_t sh = 0; sh < ns; sh++)
		recover(sh);

	/*
	 * Seal the legacy migration before the daemon becomes writable.  Starting
	 * with a missing marker, or accepting writes after a partial/unsealed scan,
	 * could create a markerless log or replay old LSN-0 pages above a newly
	 * persisted truncate/unlink on the next boot.  Fail startup instead; replay
	 * is idempotent and the next process retries the migration.
	 */
	if (fork_meta_legacy)
	{
		PsKey		zk;

		if (fork_meta_migrate_failed)
		{
			fprintf(stderr, "pagestore: fork-meta migration incomplete\n");
			return -1;
		}
		memset(&zk, 0, sizeof(zk));
		if (fork_meta_persist(0, &zk, 0, 0, FEV_MIGRATED) != 0)
		{
			fprintf(stderr, "pagestore: could not seal the fork-meta migration\n");
			return -1;
		}
	}

	/* rebuild each timeline's shipped-WAL end LSN from its log */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined)
			wal_recover_one(tl);

	return 0;
}
