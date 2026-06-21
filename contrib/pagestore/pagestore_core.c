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
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
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

/* Object tier (LSM phase 4): when set, off-path maintenance uploads sealed
 * layers to the configured object store and marks them remote_durable.  The
 * frontend sets it (with ps_layer_store_set_object_dir) when --object-dir is
 * given; 0 keeps the daemon local-only. */
int			ps_object_tier = 0;

/* the active storage backend (POSIX by default; the frontend may override) */
const PsStorage *ps_storage = &PsStoragePosix;

/*
 * Segment-record integrity (no filesystem under the raw NVMe backend, so we carry
 * our own checks).  CRC32C (Castagnoli) detects bit rot / torn writes; a per-store
 * generation distinguishes our records from stale device bytes (a reused disk, or
 * a previous store) that happen to carry a SEG_MAGIC, which a CRC alone can't.
 */
static uint32_t crc32c_tab[256];

static void
crc32c_init(void)
{
	for (uint32_t i = 0; i < 256; i++)
	{
		uint32_t	c = i;

		for (int k = 0; k < 8; k++)
			c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : (c >> 1);
		crc32c_tab[i] = c;
	}
}

static uint32_t
crc32c_upd(uint32_t crc, const void *buf, size_t len)
{
	const unsigned char *p = buf;

	while (len--)
		crc = crc32c_tab[(crc ^ *p++) & 0xff] ^ (crc >> 8);
	return crc;
}

/* page-only CRC32C, stored per version and verified on the read path */
uint32_t
ps_crc32c(const void *buf, size_t len)
{
	return crc32c_upd(0xFFFFFFFFu, buf, len) ^ 0xFFFFFFFFu;
}

/* per-store generation, stamped into every segment record (0 = uninitialized) */
static uint64_t g_store_gen;

/* store directory, stashed at open for metadata writes after open (sync watermark) */
static char g_store_dir[4000];

/*
 * Load the store's generation from <store_dir>/store_gen, creating a durable
 * random one on first open.  Stamped into each SegRecHdr so recover() can reject
 * stale records left on a reused raw device (different/zero generation) instead of
 * replaying them as live data.  Returns 0 on success, -1 if a fresh generation
 * cannot be made durable -- the caller must then fail the open, since records
 * stamped with a generation that a crash could lose would be rejected (i.e. lost)
 * by the next recover().
 */
static int
load_store_gen(const char *store_dir)
{
	char		path[2200];
	int			fd;
	uint64_t	g = 0;
	int			existed = 0;

	snprintf(path, sizeof(path), "%s/store_gen", store_dir);
	fd = open(path, O_RDONLY);
	if (fd >= 0)
	{
		ssize_t		r = read(fd, &g, sizeof(g));

		close(fd);
		existed = 1;
		if (r == (ssize_t) sizeof(g) && g != 0)
		{
			g_store_gen = g;
			return 0;			/* already durable from a prior open */
		}
		g = 0;
	}

	/*
	 * No usable generation on disk.  Only mint one for a genuinely fresh store: if
	 * segment data already exists, a missing or short store_gen means lost or
	 * mismatched metadata (a restored/copied store, a deleted file), and minting a
	 * new generation would make recover() treat every existing record as stale and
	 * silently discard the whole log.  Fail open so the loss is visible.  Check
	 * every shard's first segment (seg id == shard for local index 0), since with
	 * nshards > 1 a store may have written only to a nonzero shard, leaving seg 0
	 * absent while later shard-local segments exist.
	 */
	for (uint32_t s = 0; s < ps_nshards; s++)
		if (ps_storage->seg_size((int) s) >= 0)
			return -1;

	/*
	 * Fresh store: a leftover short/zero file (e.g. a crash mid-create) cannot have
	 * stamped any record yet, so remove and recreate it rather than wedging every
	 * future open on the EEXIST path below.
	 */
	if (existed)
		unlink(path);

	fd = open("/dev/urandom", O_RDONLY);
	if (fd >= 0)
	{
		if (read(fd, &g, sizeof(g)) != (ssize_t) sizeof(g))
			g = 0;
		close(fd);
	}
	if (g == 0)
		return -1;				/* no entropy: refuse rather than derive a
								 * predictable generation -- a path hash would repeat
								 * for a store recreated at the same path on a reused
								 * raw device, defeating the stale-bytes guard */

	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
	{
		/* lost a create race: adopt the peer's now-durable generation */
		if (errno == EEXIST && (fd = open(path, O_RDONLY)) >= 0)
		{
			ssize_t		r = read(fd, &g, sizeof(g));

			close(fd);
			if (r == (ssize_t) sizeof(g) && g != 0)
			{
				g_store_gen = g;
				return 0;
			}
		}
		return -1;
	}
	if (write(fd, &g, sizeof(g)) != (ssize_t) sizeof(g) || fsync(fd) != 0)
	{
		close(fd);
		unlink(path);
		return -1;				/* not durable: caller fails the open */
	}
	close(fd);
	fd = open(store_dir, O_RDONLY);		/* make the directory entry durable */
	if (fd < 0 || fsync(fd) != 0)
	{
		if (fd >= 0)
			close(fd);
		unlink(path);			/* dir entry not durable: don't let a retry adopt
								 * this file as "already durable" */
		return -1;
	}
	close(fd);
	g_store_gen = g;
	return 0;
}

/*
 * Per-shard state (LSM phase 9).  One thread per shard will own its index /
 * staging / cache lock-free; the shard is chosen from the logical key only
 * (block- and timeline-independent), so a key's blocks and all its timelines live
 * on one shard.  The live count ps_nshards is chosen at startup (--nshards) and
 * shard_for() hashes the key into it; at ps_nshards == 1 behavior is identical to
 * the old single global state.  Each shard has its own manifest + layer map (step
 * 4a) and a shard-namespaced layer-id allocator.  Step 4c gives each its own
 * thread; for now a single serve loop drives every shard's channel pool.
 */
#define IDX_BUCKETS		(1 << 16)
#define IDX_MASK		(IDX_BUCKETS - 1)

struct PageEnt;
struct ForkEnt;
struct WalIdxEnt;

/*
 * Timeline tree.  Timeline 0 is the root (no parent).  A branch records its
 * parent and the LSN at which it forked; reads of pages the branch never wrote
 * fall through to the parent as-of that branch LSN, so the branch is a stable
 * copy-on-write snapshot.  The tree is read on every ancestry walk, so each
 * shard holds a *replicated* copy (s->timelines) -- a shard reads its own copy
 * lock-free; branch-create (rare) writes every shard's copy (sharding step
 * 4c-iii).  The tree is append-only: a slot only goes undefined -> defined and a
 * defined slot's fields never change.
 */
#define MAX_TIMELINES	1024
typedef struct TimelineMeta
{
	int			defined;		/* 1 if this timeline exists */
	int			parent;			/* parent timeline id, or -1 for the root */
	uint64_t	branch_lsn;		/* parent LSN this timeline forked at */
} TimelineMeta;

typedef struct Shard
{
	struct PageEnt *page_idx[IDX_BUCKETS];	/* (timeline,key,block) -> versions */
	struct ForkEnt *fork_idx[IDX_BUCKETS];	/* (timeline,key) -> fork size */
	struct WalIdxEnt *walidx[IDX_BUCKETS];	/* (timeline,key,block) -> WAL lsns */
	PsMemtable *memtable;		/* staging -> image layers */
	uint32_t	index;			/* this shard's id (0 .. ps_nshards-1) */
	PsManifest	manifest;		/* per-shard durable layer log + layer map */
	uint64_t	next_local_id;	/* next layer id within this shard (low bits) */
	int			cur_seg;		/* segment being appended (-1: none yet) */
	uint64_t	cur_off;		/* append cursor within cur_seg */
	/* Last committed durable position packed as (seg << 32 | off), updated only
	 * after a record's bytes are written, read atomically by the sync-watermark
	 * capture.  Unlike the cur_seg/cur_off append cursor, this is never the bare
	 * (new_seg, 0) a rollover publishes before any record exists there, so a
	 * concurrent capture can't pair a fresh segment with a stale/zero offset. */
	uint64_t	cur_cursor;
	PsPgcache	pgcache;		/* per-shard materialized-page cache */
	PsLayerBloom bloom;			/* per-shard per-layer (key,block) bloom cache */
	time_t		maint_cooldown;	/* skip compaction attempts until this wall time */
	time_t		upload_cooldown;	/* skip object-tier uploads until this wall time */
	TimelineMeta timelines[MAX_TIMELINES];	/* replicated timeline tree */
	uint64_t	rr_mem,			/* read-source counters */
				rr_layer,
				rr_seg;
} Shard;

/* live shard count (1..PS_MAX_SHARDS); the frontend sets it before ps_core_open.
 * The array is sized to the compile-time cap; only [0, ps_nshards) are used. */
uint32_t	ps_nshards = 1;
static Shard g_shards[PS_MAX_SHARDS];

static Shard *
shard_for(const PsKey *key)
{
	/* same hash clients route with, so a request always lands on the shard that
	 * owns the key (identity at ps_nshards == 1) */
	return &g_shards[ps_shard_for_key(key, ps_nshards)];
}

/*
 * Which shard must serve a request, so a per-shard worker can reject one that was
 * posted on the wrong channel pool (a non-routing client would otherwise have
 * worker A mutate shard B's single-owner state).  Keyed ops go to their key's
 * shard; timeline/shipped-WAL ops are shard-0 global state; a global sync or a
 * no-op may run on any worker (returns PS_ANY_SHARD).  At ps_nshards == 1 every
 * request maps to shard 0, so nothing is ever rejected.
 */
#define PS_ANY_SHARD	UINT32_MAX

uint32_t
ps_request_shard(const PsChannel *ch)
{
	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE_BRANCH:
		case PS_OP_WAL_APPEND:
		case PS_OP_WAL_SIZE:
		case PS_OP_WAL_READ:
			return 0;			/* timeline / shipped-WAL: shard-0 global state */
		case PS_OP_IMMEDSYNC:
		case PS_OP_NONE:
			return PS_ANY_SHARD;	/* global sync / no-op: safe on any worker */
		default:
			return ps_shard_for_key(&ch->key, ps_nshards);
	}
}

/*
 * layer_id namespacing: the high LAYER_ID_SHARD_SHIFT bits hold the shard id and
 * the low bits a per-shard counter, so ids (and the layer files named by them)
 * never collide across shards while each shard allocates independently.  At
 * ps_nshards == 1, shard 0's ids are 1, 2, 3, ... -- identical to the old global
 * allocator.
 */
#define LAYER_ID_SHARD_SHIFT	48
#define LAYER_ID_LOCAL_MASK		(((uint64_t) 1 << LAYER_ID_SHARD_SHIFT) - 1)

/*
 * Per-shard segment namespace by interleaving: shard s owns segment ids
 * s, s + nshards, s + 2*nshards, ...  Each shard thus appends to its own segment
 * files with no shared append cursor, while the id space stays *dense* (ids run
 * 0..~total with no large gaps) -- which the SPDK backend requires, since it maps
 * a segment id linearly to a device offset (seg * segment_size).  shard = id %
 * nshards, local index = id / nshards.  At ps_nshards == 1 this is the identity
 * 0, 1, 2, ... -- byte-for-byte the old single-stream layout.  (The mapping is
 * fixed for a store's life by its nshards, like the per-shard manifests.)
 */
static int
seg_id(uint32_t shard, uint32_t local)
{
	return (int) (shard + local * ps_nshards);
}

static uint32_t
seg_local(int seg)
{
	return (uint32_t) seg / ps_nshards;
}

/*
 * Durable per-shard sync watermark.
 *
 * Appends are not synced per record, so on recovery a record that fails to verify
 * may be an unsynced torn tail (expected after a crash -- truncate it) or
 * corruption of data an IMMEDSYNC already made durable (fail loudly).  The
 * watermark is the (segment local index, byte offset) each shard was synced
 * through; it is persisted on every IMMEDSYNC and on clean close, after the data
 * itself is durable.  recover() compares the position it actually rebuilt each
 * shard to its watermark to tell the two apart.
 */
#define SYNC_WM_MAGIC	0x53574d31u		/* "SWM1" */

typedef struct SyncWmEnt
{
	uint32_t	local;			/* local index of the synced segment */
	uint32_t	_pad;
	uint64_t	off;			/* synced byte offset within it */
} SyncWmEnt;

typedef struct SyncWmFile
{
	uint32_t	magic;
	uint32_t	nshards;
	uint32_t	crc;			/* ps_crc32c over the struct with crc == 0 */
	uint32_t	_pad;
	SyncWmEnt	ent[PS_MAX_SHARDS];
} SyncWmFile;

static SyncWmEnt g_wm[PS_MAX_SHARDS];	/* loaded watermark, per shard */
static int	g_wm_valid;					/* a trustworthy watermark was loaded */
static SyncWmEnt g_wm_pending[PS_MAX_SHARDS];	/* snapshot staged for the next commit */
/* serializes the POSIX IMMEDSYNC capture+commit so concurrent syncs on different
 * worker threads don't race on g_wm_pending or the sync_wm.tmp file (the SPDK path
 * is already serialized by its barrier mutex) */
static pthread_mutex_t g_wm_mtx = PTHREAD_MUTEX_INITIALIZER;

/* Load the persisted watermark into g_wm; g_wm_valid stays 0 (no fail-open) when
 * absent or untrustworthy, so such a store keeps the safe truncate-only behavior. */
static void
load_sync_watermark(void)
{
	char		path[4100];
	int			fd;
	SyncWmFile	f;
	uint32_t	stored;

	g_wm_valid = 0;
	memset(g_wm, 0, sizeof(g_wm));
	snprintf(path, sizeof(path), "%s/sync_wm", g_store_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;
	if (read(fd, &f, sizeof(f)) != (ssize_t) sizeof(f))
	{
		close(fd);
		return;
	}
	close(fd);
	if (f.magic != SYNC_WM_MAGIC || f.nshards != ps_nshards)
		return;
	stored = f.crc;
	f.crc = 0;
	if (ps_crc32c(&f, sizeof(f)) != stored)
		return;					/* torn/partial watermark write: ignore */
	for (uint32_t i = 0; i < ps_nshards; i++)
		g_wm[i] = f.ent[i];
	g_wm_valid = 1;
}

/*
 * Snapshot one shard's current append cursor into the pending watermark.  The
 * watermark must never exceed what the impending sync makes durable, so this is
 * called either by the shard's own worker right after it flushes (SPDK), or by the
 * sync coordinator just before the sync (POSIX) / on clean close -- in every case
 * the captured position is <= what becomes durable, and a slightly stale read only
 * understates (safe: a too-small watermark just treats more of the tail as
 * unsynced, never raises a false "corrupt acknowledged data").  Atomic loads make
 * the cross-thread read on the POSIX path well-defined.
 */
void
ps_core_wm_capture(uint32_t shard)
{
	Shard	   *s = &g_shards[shard];
	/* one atomic load of the packed (seg, off) so the pair is always consistent --
	 * never a freshly-rolled segment with a stale offset (cur_cursor is 0 until a
	 * record is committed, and only ever holds a position whose bytes are written) */
	uint64_t	c = __atomic_load_n(&s->cur_cursor, __ATOMIC_ACQUIRE);
	int			seg = (int) (uint32_t) (c >> 32);
	uint64_t	off = (uint32_t) c;

	if (c == 0)					/* no committed record yet */
	{
		g_wm_pending[shard].local = 0;
		g_wm_pending[shard].off = 0;
		return;
	}
	g_wm_pending[shard].local = seg_local(seg);
	g_wm_pending[shard].off = off;
}

/*
 * Persist the pending watermark snapshot (captured via ps_core_wm_capture) for
 * every shard.  Call only after the segment data through those positions is durable
 * (post IMMEDSYNC / clean close); written via a temp file + rename so a crash never
 * leaves a half-updated watermark.  Returns 0 on success, -1 if it could not be
 * made durable.
 */
int
ps_core_write_sync_watermark(void)
{
	char		path[4100];
	char		tmp[4120];
	SyncWmFile	f;
	int			fd;

	memset(&f, 0, sizeof(f));
	f.magic = SYNC_WM_MAGIC;
	f.nshards = ps_nshards;
	for (uint32_t i = 0; i < ps_nshards; i++)
		f.ent[i] = g_wm_pending[i];	/* the snapshot captured at the sync, not live */
	f.crc = ps_crc32c(&f, sizeof(f));

	snprintf(path, sizeof(path), "%s/sync_wm", g_store_dir);
	snprintf(tmp, sizeof(tmp), "%s/sync_wm.tmp", g_store_dir);
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	if (write(fd, &f, sizeof(f)) != (ssize_t) sizeof(f) || fsync(fd) != 0)
	{
		close(fd);
		unlink(tmp);
		return -1;
	}
	close(fd);
	if (rename(tmp, path) != 0)
	{
		unlink(tmp);
		return -1;
	}
	/* the rename itself must be durable: a failure here means a crash could lose the
	 * watermark, so report it rather than acknowledge a non-durable sync */
	fd = open(g_store_dir, O_RDONLY);
	if (fd < 0 || fsync(fd) != 0)
	{
		if (fd >= 0)
			close(fd);
		return -1;
	}
	close(fd);
	return 0;
}

/*
 * One IMMEDSYNC / clean-shutdown durability step: capture every shard's cursor,
 * sync the segment data, then commit the watermark -- all under g_wm_mtx so two
 * concurrent syncs (the POSIX daemon serves IMMEDSYNC on any worker) can't race on
 * the shared snapshot or the sync_wm temp file.  Capturing before the sync keeps
 * the watermark from ever exceeding what is made durable.  Returns 0 on success.
 */
int
ps_core_sync_and_watermark(void)
{
	int			rc;

	pthread_mutex_lock(&g_wm_mtx);
	for (uint32_t s = 0; s < ps_nshards; s++)
		ps_core_wm_capture(s);
	rc = ps_storage->sync();
	if (rc == 0)
		rc = ps_core_write_sync_watermark();
	pthread_mutex_unlock(&g_wm_mtx);
	return rc;
}

uint32_t
ps_core_layer_count(void)
{
	uint32_t	n = 0;

	for (uint32_t i = 0; i < ps_nshards; i++)
		n += g_shards[i].manifest.map.nlayers;
	return n;
}

/* read-path source counters (memtable / image layer / segment fallback),
 * summed across shards */
void
ps_core_read_stats(uint64_t *mem, uint64_t *layer, uint64_t *seg)
{
	uint64_t	m = 0,
				l = 0,
				s = 0;

	for (uint32_t i = 0; i < ps_nshards; i++)
	{
		m += g_shards[i].rr_mem;
		l += g_shards[i].rr_layer;
		s += g_shards[i].rr_seg;
	}
	if (mem)
		*mem = m;
	if (layer)
		*layer = l;
	if (seg)
		*seg = s;
}

/*
 * The materialized-page cache for 'key' -- i.e. its shard's.  A frontend (the
 * SPDK async path) that caches pages outside read_resolve uses this to reach the
 * right per-shard cache; routing by the key matches read_resolve.
 */
PsPgcache *
ps_core_pgcache_for(const PsKey *key)
{
	return &shard_for(key)->pgcache;
}

/* materialized-page cache counters, summed across shards */
void
ps_core_pgcache_stats(uint64_t *hits, uint64_t *misses, uint64_t *evictions)
{
	uint64_t	h = 0,
				m = 0,
				e = 0;

	for (uint32_t i = 0; i < ps_nshards; i++)
	{
		uint64_t	sh,
					sm,
					se;

		ps_pgcache_stats(&g_shards[i].pgcache, &sh, &sm, &se);
		h += sh;
		m += sm;
		e += se;
	}
	if (hits)
		*hits = h;
	if (misses)
		*misses = m;
	if (evictions)
		*evictions = e;
}

/* ctx is the owning Shard* (passed through the memtable flush / compaction
 * callbacks), so id allocation and the manifest write stay on that shard. */
static uint64_t
alloc_layer_id(void *ctx)
{
	Shard	   *s = ctx;

	return ((uint64_t) s->index << LAYER_ID_SHARD_SHIFT) | s->next_local_id++;
}

static int
record_layer(void *ctx, const PsLayerDesc *desc)
{
	Shard	   *s = ctx;
	int			rc;

	/* ps_manifest_add_layer persists the ADD event *and* adds it to the layer
	 * map (idempotently); do not add to the map a second time. */
	rc = ps_manifest_add_layer(&s->manifest, desc);
	if (rc == 0)
		s->upload_cooldown = 0;	/* a fresh layer to upload ends any idle backoff */
	return rc;
}

/* ===================== compaction & GC (LSM phase 3) =================== */

/* Max image layers merged in one compact_timeline() call, so a single call does
 * bounded I/O (a "nonblocking slice") instead of rewriting a whole timeline and
 * stalling the serve loop; successive idle calls finish the rest. */
#define COMPACT_BATCH	8

/* Seconds an idle daemon waits before re-attempting a compaction that made no
 * progress (corrupt layer / ENOSPC), so it doesn't spin re-reading failing
 * layers; a flush/compaction that changes layer state clears the backoff early. */
#define MAINT_COOLDOWN_SECS 5

/* Size-tiered compaction: only merge image layers whose sizes are within this
 * factor of each other, so a large layer isn't rewritten just because small ones
 * accumulated -- this bounds write/space amplification (à la RocksDB/ScyllaDB
 * size-tiered) instead of the old "merge every layer in the timeline".  A single
 * merge still does at most COMPACT_BATCH layers. */
#define COMPACT_TIER_RATIO	4

/* qsort: order image layers by on-disk size ascending (for tier selection). */
static int
layer_size_cmp(const void *a, const void *b)
{
	uint64_t	sa = ((const PsLayerDesc *) a)->locations[0].size;
	uint64_t	sb = ((const PsLayerDesc *) b)->locations[0].size;

	return (sa > sb) - (sa < sb);
}

static uint32_t
count_image_layers(Shard *s, uint32_t timeline)
{
	const PsLayerMap *map = &s->manifest.map;
	uint32_t	c = 0;

	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		const PsLayerDesc *d = &map->layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting && d->timeline == timeline)
			c++;
	}
	return c;
}

/*
 * Finish removing a 'deleting' layer: drop its local file, then record the
 * manifest removal -- only once the file is gone, so a failed unlink leaves the
 * layer 'deleting' for the next gc_resume rather than orphaning a file whose
 * manifest entry is dropped.
 *
 * Exception: a remote-durable layer compacted away keeps both its object copy AND
 * its manifest entry, as a tombstone (still marked 'deleting', so reads skip it).
 * Its merged replacement is not yet remote-durable, so removing it now would drop
 * the only recorded remote protection for that key range -- and dropping just the
 * entry would leave the object undiscoverable.  A phase-6 remote-aware GC removes
 * the tombstone (entry + object) once the replacement range is durably remote.
 * The stale local location on the tombstone is harmless (deleting layers are never
 * read); its local file is still reclaimed here.
 *
 * The decision keys off the persisted remote_durable flag, NOT ps_object_tier:
 * a store with uploaded layers reopened without --object-dir must still keep the
 * tombstone, or the object becomes undiscoverable if the tier is re-enabled later.
 */
static void
gc_remove_layer(Shard *s, const PsLayerDesc *d)
{
	if (ps_layer_store->delete_local_layer(d) != 0)
		return;
	if (d->remote_durable)
		return;					/* keep the entry as a remote tombstone */
	/* Not remote-durable, but an upload may have copied the object before the
	 * daemon crashed without persisting SET_REMOTE_DURABLE.  With the tier
	 * configured, delete that orphan before dropping the entry (keep the entry if
	 * the delete fails, so a later gc_resume retries).  With the tier disabled we
	 * cannot check or delete it, so if this store ever used the object tier keep
	 * the entry until a tier-enabled run can prove/clean it -- dropping it now
	 * would leak an object no later run could discover. */
	if (ps_object_tier)
	{
		if (ps_layer_store->layer_exists_remote(d) == 1 &&
			ps_layer_store->delete_remote_layer(d) != 0)
			return;
	}
	else if (ps_layer_store_object_used())
		return;
	ps_manifest_remove_layer(&s->manifest, d->layer_id);
}

/*
 * Finish any GC that a crash interrupted: every layer still marked 'deleting' in
 * the manifest has its local file removed (idempotent) and a REMOVE_LAYER event
 * recorded.  Reads already skip 'deleting' layers, so this only reclaims space.
 */
static void
gc_resume(Shard *s)
{
	const PsLayerMap *map = &s->manifest.map;
	PsLayerDesc *dead;
	uint32_t	m = 0;

	for (uint32_t i = 0; i < map->nlayers; i++)
		if (map->layers[i].deleting)
			m++;
	if (m == 0)
		return;
	dead = malloc((size_t) m * sizeof(PsLayerDesc));
	if (!dead)
		return;
	m = 0;
	for (uint32_t i = 0; i < map->nlayers; i++)
		if (map->layers[i].deleting)
			dead[m++] = map->layers[i];
	for (uint32_t k = 0; k < m; k++)
		gc_remove_layer(s, &dead[k]);
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
compact_timeline(Shard *s, uint32_t timeline)
{
	const PsLayerMap *map = &s->manifest.map;
	PsLayerDesc *old;
	uint32_t	nold = count_image_layers(s, timeline);
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
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		const PsLayerDesc *d = &map->layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting && d->timeline == timeline)
			old[nold++] = *d;
	}

	/*
	 * Size-tiered selection: sort by size and merge the longest run of layers
	 * that all fall within COMPACT_TIER_RATIO of the run's smallest, capped at
	 * COMPACT_BATCH.  Merging a same-size tier (rather than the whole timeline)
	 * keeps write amplification bounded -- a freshly merged, larger layer climbs
	 * to a higher tier and isn't rewritten again just because new small layers
	 * appear.  The per-call cap also keeps the work a nonblocking slice so the
	 * serve loop that triggers maintenance isn't stalled; an over-threshold
	 * timeline converges across successive idle calls (ps_core_maintenance
	 * reports more-to-do).  If no two layers are within the ratio, fall back to
	 * the two smallest so progress is still guaranteed.
	 */
	qsort(old, nold, sizeof(PsLayerDesc), layer_size_cmp);
	{
		uint32_t	best_i = 0,
					best_len = 0;

		for (uint32_t i = 0; i < nold; i++)
		{
			uint64_t	base = old[i].locations[0].size;
			uint32_t	j = i;

			while (j < nold && j - i < COMPACT_BATCH &&
				   old[j].locations[0].size <= base * COMPACT_TIER_RATIO)
				j++;
			if (j - i > best_len)
			{
				best_len = j - i;
				best_i = i;
			}
		}
		if (best_len < 2)		/* no same-size tier: merge the two smallest */
			best_i = 0, best_len = 2;
		if (best_i > 0)
			memmove(old, old + best_i, (size_t) best_len * sizeof(PsLayerDesc));
		nold = best_len;
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
			/* verify against the per-page index crc -- compaction reads bytes
			 * directly (bypassing ps_image_layer_lookup's check), so a corrupt
			 * page must abort the merge, not be laundered into the new layer with
			 * a fresh checksum */
			if (ps_image_page_crc(pg, page_size) != idx[j].crc)
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
	nid = alloc_layer_id(s);
	if (ps_image_layer_write(nid, timeline, recs, nrec, page_size, &newdesc) != 0)
		goto cleanup;			/* write failed: it removed its own file */
	if (record_layer(s, &newdesc) != 0)
	{
		/* written + sealed but not recorded in the manifest: delete it so its
		 * reused layer id can't later wedge create_local_layer's O_EXCL (the same
		 * orphan-cleanup the memtable flush path does) */
		ps_layer_store->delete_local_layer(&newdesc);
		goto cleanup;
	}
	for (uint32_t k = 0; k < nold; k++)
	{
		/* Only unlink the local file once the layer is durably marked deleting;
		 * otherwise a crash could leave a "live" manifest entry whose file is
		 * gone, so later reads/compactions hit a missing file.  If REMOVE_LAYER
		 * then fails the layer stays 'deleting' -- reads skip it and gc_resume()
		 * finishes the removal on the next start. */
		if (ps_manifest_mark_delete(&s->manifest, old[k].layer_id) != 0)
			continue;
		/* drop local file (+ object copy if uploaded), then the manifest entry;
		 * a failure leaves the layer 'deleting' so gc_resume() retries it instead
		 * of orphaning a file/object with a dropped manifest entry */
		gc_remove_layer(s, &old[k]);
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
	uint32_t	crc;			/* CRC32C over (pos, header[crc=0], page bytes) */
	uint64_t	gen;			/* per-store generation (rejects stale device bytes) */
} SegRecHdr;

/*
 * CRC32C covering the record's position (segment id + byte offset, to catch a
 * misdirected write that landed elsewhere), the header with its own crc field
 * zeroed, and the page bytes.  Computed at write, checked at recover.
 */
static uint32_t
seg_rec_crc(int seg, uint64_t recoff, const SegRecHdr *h,
			const unsigned char *page)
{
	SegRecHdr	tmp = *h;
	uint32_t	crc = 0xFFFFFFFFu;

	tmp.crc = 0;
	crc = crc32c_upd(crc, &seg, sizeof(seg));
	crc = crc32c_upd(crc, &recoff, sizeof(recoff));
	crc = crc32c_upd(crc, &tmp, sizeof(tmp));
	crc = crc32c_upd(crc, page, page_size);
	return crc ^ 0xFFFFFFFFu;
}

/*
 * Segments are addressed by (id, byte offset); how they are stored is the
 * storage backend's business (see pagestore_storage.h).  The append cursor
 * (cur_seg, cur_off) marking where the next record goes is per shard (Shard),
 * and segment ids are shard-namespaced (seg_id), so shards append to disjoint
 * segment files with no shared cursor.
 */

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

/* One LSN-versioned size change of a fork: its block count became 'nblocks' as
 * of WAL record 'lsn' (an extension grows it; a truncation shrinks it). */
typedef struct ForkSz
{
	uint64_t	lsn;
	uint32_t	nblocks;
} ForkSz;

/* Hash entry: the block count of one fork on one timeline. */
typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	nblocks;		/* current count (unversioned; used by NBLOCKS) */
	ForkSz	   *sz;				/* LSN-versioned size history, ascending by lsn */
	int			nsz;
	int			szcap;
} ForkEnt;

/*
 * Timeline metadata.  Timeline 0 is the root (no parent).  A branch records its
 * parent and the LSN at which it forked; reads of pages the branch never wrote
 * fall through to the parent as-of that branch LSN, so the branch is a stable
 * copy-on-write snapshot.
 */
/* TimelineMeta and the timeline tree are defined above the Shard struct (each
 * shard holds a replicated copy in s->timelines); see there. */

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
				 uint64_t lsn, int seg, uint64_t off, uint32_t crc)
{
	uint32_t	h = page_hash(timeline, key, block);
	Shard	   *s = shard_for(key);
	PageEnt    *e = page_find(timeline, key, block);

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
	e->vers[e->nver].lsn = lsn;
	e->vers[e->nver].seg = seg;
	e->vers[e->nver].off = off;
	e->vers[e->nver].crc = crc;
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
	Shard	   *s = shard_for(key);
	ForkEnt   **pp = &s->fork_idx[h & IDX_MASK];

	while (*pp)
	{
		if ((*pp)->timeline == timeline && key_eq(&(*pp)->key, key))
		{
			ForkEnt    *dead = *pp;

			*pp = dead->next;
			free(dead->sz);
			free(dead);
			return;
		}
		pp = &(*pp)->next;
	}
}

/* --- timeline metadata + read-through --- */

/*
 * Define timeline 'id' in every shard's replicated copy.  Branch-create is rare
 * and the tree is append-only, so a broadcast keeps every shard's lock-free read
 * copy in sync; at ps_nshards == 1 it writes the single copy.
 *
 * The broadcast is synchronous (the CREATE_BRANCH worker writes every shard's copy
 * before publishing DONE), which is the coordination contract clients rely on: a
 * client that observes its branch-create complete can immediately write the branch
 * on any shard and find the timeline defined there -- without a round of acks.  An
 * asynchronous per-shard delivery queue would reopen exactly that visibility gap.
 * The cross-thread write is data-race-free: each fields-then-'defined' publish is a
 * release store (below) and every reader gates on tl_defined()'s acquire load, so
 * parent/branch_lsn are visible once 'defined' reads 1; the slot only flips 0->1.
 */
static void
timeline_define(uint32_t id, int parent, uint64_t branch_lsn)
{
	if (id >= MAX_TIMELINES)
		return;
	for (uint32_t i = 0; i < ps_nshards; i++)
	{
		TimelineMeta *t = &g_shards[i].timelines[id];

		t->parent = parent;
		t->branch_lsn = branch_lsn;
		/* publish 'defined' last with a release store: branch-create (shard 0)
		 * writes peer shards' copies while their threads read them, so the
		 * acquire-load in tl_defined() pairs with this to make parent/branch_lsn
		 * visible.  The tree is append-only, so a slot only ever flips 0 -> 1. */
		__atomic_store_n(&t->defined, 1, __ATOMIC_RELEASE);
	}
}

/* acquire-load of a timeline slot's 'defined' flag (pairs with timeline_define's
 * release store; the slot's other fields are valid once this reads 1) */
static int
tl_defined(const Shard *s, uint32_t timeline)
{
	return timeline < MAX_TIMELINES &&
		__atomic_load_n(&s->timelines[timeline].defined, __ATOMIC_ACQUIRE);
}

static int
timeline_has_parent(const Shard *s, uint32_t timeline)
{
	return tl_defined(s, timeline) && s->timelines[timeline].parent >= 0;
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
branch_request_ok(const Shard *s, uint32_t new_tl, int parent)
{
	if (new_tl == 0 || new_tl >= MAX_TIMELINES || tl_defined(s, new_tl))
		return 0;
	if (parent < 0 || parent >= MAX_TIMELINES || !tl_defined(s, (uint32_t) parent))
		return 0;
	for (int t = parent; t >= 0 && t < MAX_TIMELINES; t = s->timelines[t].parent)
	{
		if ((uint32_t) t == new_tl)
			return 0;			/* cycle */
		if (!tl_defined(s, (uint32_t) t))
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
	const Shard *s = shard_for(key);	/* its replicated timeline copy */

	for (;;)
	{
		PageEnt    *e = page_find(timeline, key, block);
		PageVer    *v = e ? page_visible(e, read_lsn) : NULL;

		if (v)
			return v;
		if (!timeline_has_parent(s, timeline))
			return NULL;
		if (s->timelines[timeline].branch_lsn < read_lsn)
			read_lsn = s->timelines[timeline].branch_lsn;
		timeline = (uint32_t) s->timelines[timeline].parent;
	}
}

/*
 * Like read_through(), but for frontends that cache pages keyed by
 * (timeline,key,block,lsn).  It also reports the timeline the version was found
 * on (*src_tl): a read falls through to ancestor timelines, so an inherited page
 * must be cached under its source timeline, not the requested one -- otherwise a
 * branch that later writes its own same-lsn copy of the block would keep hitting
 * the inherited bytes.  And it reports whether that version's page LSN is
 * ambiguous (*ambiguous): a same-lsn rewrite (e.g. hint bits) means the lsn does
 * not uniquely identify the version, so it is not safe to serve from or store in
 * an lsn-keyed cache.  Returns the chosen PageVer, or NULL (with *src_tl set to
 * the requested timeline and *ambiguous cleared).
 */
PageVer *
read_through_cacheable(uint32_t timeline, const PsKey *key, uint32_t block,
					   uint64_t read_lsn, uint32_t *src_tl, int *ambiguous)
{
	const Shard *s = shard_for(key);	/* its replicated timeline copy */

	for (;;)
	{
		PageEnt    *e = page_find(timeline, key, block);
		PageVer    *v = e ? page_visible(e, read_lsn) : NULL;

		if (v)
		{
			*src_tl = timeline;
			*ambiguous = page_lsn_ambiguous(e, v->lsn);
			return v;
		}
		if (!timeline_has_parent(s, timeline))
		{
			*src_tl = timeline;
			*ambiguous = 0;
			return NULL;
		}
		if (s->timelines[timeline].branch_lsn < read_lsn)
			read_lsn = s->timelines[timeline].branch_lsn;
		timeline = (uint32_t) s->timelines[timeline].parent;
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
	const Shard *s = shard_for(key);
	uint32_t	maxnb = 0;

	for (;;)
	{
		ForkEnt    *e = fork_find(timeline, key);

		if (e && e->nblocks > maxnb)
			maxnb = e->nblocks;
		if (!timeline_has_parent(s, timeline))
			return maxnb;
		timeline = (uint32_t) s->timelines[timeline].parent;
	}
}

/*
 * The fork's block count as of 'lsn': the nblocks of the newest size event at or
 * below 'lsn', walking the timeline ancestry (a branch inherits the parent's size
 * as of its fork LSN until its own first size event).  Returns -1 if no size
 * event is known at/below 'lsn' (caller decides how to treat "unknown").
 */
static int
fork_nblocks_at(uint32_t timeline, const PsKey *key, uint64_t lsn)
{
	const Shard *s = shard_for(key);

	for (;;)
	{
		ForkEnt    *e = fork_find(timeline, key);

		if (e)
			for (int i = e->nsz - 1; i >= 0; i--)
				if (e->sz[i].lsn <= lsn)
					return (int) e->sz[i].nblocks;
		if (!timeline_has_parent(s, timeline))
			return -1;
		if (s->timelines[timeline].branch_lsn < lsn)
			lsn = s->timelines[timeline].branch_lsn;
		timeline = (uint32_t) s->timelines[timeline].parent;
	}
}

/*
 * Record an LSN-versioned size change for the redo "is this block live as-of LSN?"
 * check.  is_trunc distinguishes the two sources, which prune differently:
 *   - a truncation (is_trunc) sets the exact new size; recorded unless the size at
 *     'lsn' already equals it;
 *   - an extension (!is_trunc) only grows the fork, so it is recorded only when
 *     'nblocks' exceeds the size already in effect at 'lsn' -- a WAL reference to a
 *     low block of a larger fork must not look like a shrink.
 * Events store the exact size as of their LSN, so fork_nblocks_at() (latest <= lsn)
 * reads them directly.  Kept ascending by lsn (usually an append).
 */
void
fork_size_add(uint32_t timeline, const PsKey *key, uint64_t lsn,
			  uint32_t nblocks, bool is_trunc)
{
	int			cur = fork_nblocks_at(timeline, key, lsn);
	ForkEnt    *e;
	int			i;

	if (is_trunc ? (cur == (int) nblocks) : (cur >= (int) nblocks))
		return;					/* no effect on the size in force at 'lsn' */

	e = fork_get_or_create(timeline, key);
	/* an event already at exactly this lsn: overwrite (same record, corrected) */
	for (i = e->nsz - 1; i >= 0; i--)
		if (e->sz[i].lsn == lsn)
		{
			e->sz[i].nblocks = nblocks;
			return;
		}
		else if (e->sz[i].lsn < lsn)
			break;

	if (e->nsz == e->szcap)
	{
		e->szcap = e->szcap ? e->szcap * 2 : 4;
		e->sz = realloc(e->sz, (size_t) e->szcap * sizeof(ForkSz));
	}
	i = e->nsz;
	while (i > 0 && e->sz[i - 1].lsn > lsn)
	{
		e->sz[i] = e->sz[i - 1];
		i--;
	}
	e->sz[i].lsn = lsn;
	e->sz[i].nblocks = nblocks;
	e->nsz++;
}

/* Does the fork exist on 'timeline' or any ancestor? */
static int
fork_exists_through(uint32_t timeline, const PsKey *key)
{
	const Shard *s = shard_for(key);

	for (;;)
	{
		if (fork_find(timeline, key))
			return 1;
		if (!timeline_has_parent(s, timeline))
			return 0;
		timeline = (uint32_t) s->timelines[timeline].parent;
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

static int
timeline_persist(uint32_t id, int parent, uint64_t branch_lsn)
{
	TimelineRec rec = {id, (int32_t) parent, branch_lsn};

	return ps_storage->meta_append(&rec, sizeof(rec));
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
		/* startup, single-threaded: validate against shard 0's copy (all copies
		 * are identical), then define broadcasts to every shard */
		if (branch_request_ok(&g_shards[0], rec.id, rec.parent))
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

static void
walidx_add(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	uint32_t	h = page_hash(tl, key, block);
	Shard	   *s = shard_for(key);
	WalIdxEnt  *e;

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
 * max_out), and -- when out_tl != NULL -- the source timeline of each LSN into
 * out_tl (the ancestry level it came from, which the caller needs to fetch each
 * record from the right per-timeline WAL).  Writes at most max_out pairs but
 * returns the TRUE total number of matches, so a caller that only needs the count
 * (PS_OP_WAL_INDEX_GET with no buffer pressure) gets it uncapped and a caller that
 * reads the pairs can tell the chain was truncated (total > written).  Walks the
 * timeline ancestry. */
static int
walidx_get(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn_max,
		   uint64_t *out, uint32_t *out_tl, int max_out)
{
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	int			total = 0;

	for (;;)
	{
		uint32_t	h = page_hash(tl, key, block);
		WalIdxEnt  *e;

		for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
			if (e->timeline == tl && e->block == block && key_eq(&e->key, key))
			{
				for (int i = 0; i < e->n; i++)
					if (e->lsns[i] <= lsn_max)
					{
						if (total < max_out)
						{
							out[total] = e->lsns[i];
							if (out_tl)
								out_tl[total] = tl;	/* source timeline of this LSN */
						}
						total++;	/* counts all matches, even past max_out */
					}
				break;
			}
		if (!timeline_has_parent(s, tl))
			break;
		if (s->timelines[tl].branch_lsn < lsn_max)
			lsn_max = s->timelines[tl].branch_lsn;
		tl = (uint32_t) s->timelines[tl].parent;
	}
	return total;
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
	Shard	   *s = shard_for(key);

	/* roll over to a fresh segment in this shard's namespace when the current one
	 * would overflow (local index 0, 1, 2, ... within the shard) */
	if (s->cur_seg < 0 || s->cur_off + reclen > segment_size)
	{
		uint32_t	local = (s->cur_seg < 0) ? 0 : seg_local(s->cur_seg) + 1;

		/* atomic so a concurrent sync-watermark capture on another thread reads a
		 * consistent cursor; cur_off is published only after the record's bytes are
		 * written (below), so a captured offset is always already-written data */
		__atomic_store_n(&s->cur_seg, seg_id(s->index, local), __ATOMIC_RELEASE);
		__atomic_store_n(&s->cur_off, (uint64_t) 0, __ATOMIC_RELEASE);
	}

	memset(&hdr, 0, sizeof(hdr));	/* deterministic padding: the crc covers it */
	hdr.magic = SEG_MAGIC;
	hdr.timeline = timeline;
	hdr.key = *key;
	hdr.block = block;
	hdr.lsn = page_lsn(page);	/* version key taken from the page itself */
	hdr.len = page_size;
	hdr.gen = g_store_gen;
	hdr.crc = seg_rec_crc(s->cur_seg, s->cur_off, &hdr, page);

	/* write header then page bytes contiguously at the append cursor.  (Ordering
	 * page-before-header would not make a torn tail unambiguous without an fsync
	 * barrier between the two writes, which we don't pay per append; recover()
	 * instead treats any unverifiable record as end-of-log -- see there.) */
	if (ps_storage->seg_write(s->cur_seg, s->cur_off, &hdr, sizeof(hdr)) != 0)
		return -1;
	data_off = s->cur_off + sizeof(hdr);
	if (ps_storage->seg_write(s->cur_seg, data_off, page, page_size) != 0)
		return -1;

	/* index points at the page bytes (data_off), so reads skip the header; carry
	 * the page CRC so the read path can verify the bytes it later fetches */
	page_add_version(timeline, key, block, hdr.lsn, s->cur_seg, data_off,
					 ps_crc32c(page, page_size));
	/* publish the advanced cursor only now, after the bytes are written */
	__atomic_store_n(&s->cur_off, s->cur_off + reclen, __ATOMIC_RELEASE);
	/* publish the committed (seg, off) as one atomic word for the watermark capture,
	 * only here -- never the bare (rolled-seg, 0) above -- so a capture always sees a
	 * position whose record bytes are written */
	__atomic_store_n(&s->cur_cursor,
					 ((uint64_t) (uint32_t) s->cur_seg << 32) | (uint32_t) s->cur_off,
					 __ATOMIC_RELEASE);

	/* stage the version for the LSM memtable; flush to an image layer when full
	 * (additive in phase 2 -- the segment write above is still authoritative) */
	if (s->memtable)
	{
		ps_memtable_put(s->memtable, timeline, key, block, hdr.lsn, page);
		if (ps_memtable_full(s->memtable))
		{
			/* The page is already durable in the segment log (above), which is
			 * what recover() rebuilds from, so a failed flush must not let the
			 * memtable grow without bound across retries: drop the staging (it is
			 * reconstructable) instead of keeping it at/over the threshold. */
			if (ps_memtable_flush(s->memtable, alloc_layer_id, record_layer,
								  s) != 0)
			{
				fprintf(stderr, "pagestore_core: memtable flush failed; dropping "
						"staged pages (still durable in the segment log)\n");
				ps_memtable_reset(s->memtable);
			}
			/* Normal compaction runs off the write path in ps_core_maintenance()
			 * when the daemon is idle.  As a backpressure guard, compact inline
			 * here any timeline whose layer count is running away far past the
			 * threshold (a flush groups by timeline and can emit layers for
			 * several, so check them all, not just this write's).  This shard only:
			 * the flush added layers to this shard's map. */
			for (uint32_t ct = 0; ct < MAX_TIMELINES; ct++)
				if ((ct == 0 || tl_defined(s, ct)) &&
					count_image_layers(s, ct) > (uint32_t) compact_layers * 4)
					compact_timeline(s, ct);
		}
	}
	return 0;
}

/*
 * Read a specific version's page bytes into out (page_size bytes).  Returns 0 on
 * success, -1 if the version has no segment copy (layer-origin), and -2 on an
 * integrity error -- a device read failure or a CRC mismatch (bit rot / misread)
 * -- which the caller must surface rather than treat as an unwritten page.
 */
int
read_version(const PageVer *v, unsigned char *out)
{
	if (v->seg < 0)				/* layer-origin version (no segment copy) */
		return -1;
	if (ps_storage->seg_read(v->seg, v->off, out, page_size) != 0)
		return -2;
	/* verify the bytes against the version's stored CRC: catches bit rot or a
	 * misread that happened since the index was built */
	if (ps_crc32c(out, page_size) != v->crc)
		return -2;
	return 0;
}

/*
 * Newest image-layer version of (timeline, key, block) with lsn <= read_lsn on
 * this exact timeline (ancestry is the caller's job).  Tries every image layer
 * of that timeline (key-range/bloom pruning is a later optimization).
 */
static int
layer_map_lookup(Shard *s, uint32_t timeline, const PsKey *key, uint32_t block,
				 uint64_t read_lsn, uint64_t *out_lsn, unsigned char *out)
{
	const PsLayerMap *map = &s->manifest.map;
	unsigned char *tmp = malloc(page_size);
	int			found = 0;
	uint64_t	best = 0;

	if (!tmp)
		return 0;
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		const PsLayerDesc *d = &map->layers[i];
		uint64_t	l;

		if (d->kind != PS_LAYER_IMAGE || d->timeline != timeline || d->deleting)
			continue;
		if (ps_image_layer_lookup(d, key, block, read_lsn, tmp, page_size,
								  &l, NULL, &s->bloom) == 1 && (!found || l > best))
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
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */

	for (;;)
	{
		PageEnt    *e = page_find(tl, key, block);
		PageVer    *pv = e ? page_visible(e, rl) : NULL;

		if (pv)
		{
			uint64_t	l;
			int			served;
			/* If pv->lsn is ambiguous (a same-lsn rewrite, e.g. hint bits) the
			 * lsn does not uniquely identify a version, so every lsn-keyed fast
			 * path -- the materialized-page cache, the memtable and the image
			 * layers -- may return an older version than the authoritative latest
			 * append.  Bypass all of them (including caching the result) and serve
			 * from the segment (pv's exact location). */
			int			ambig = page_lsn_ambiguous(e, pv->lsn);

			if (!ambig && ps_pgcache_lookup(&s->pgcache, tl, key, block,
											pv->lsn, out))
				return 1;

			if (!ambig && s->memtable &&
				ps_memtable_lookup(s->memtable, tl, key, block, rl, &l, out) &&
				l == pv->lsn)
			{
				s->rr_mem++;
				served = 1;		/* served from the memtable */
			}
			else if (!ambig &&
					 layer_map_lookup(s, tl, key, block, rl, &l, out) &&
					 l == pv->lsn)
			{
				s->rr_layer++;
				served = 1;		/* served from an image layer */
			}
			else
			{
				int			rv;

				s->rr_seg++;
				rv = read_version(pv, out);		/* segment fallback */
				if (rv == -2)
					return -1;	/* corrupt stored page: surface, don't zero-fill */
				served = (rv == 0);
			}
			if (served && !ambig)
				ps_pgcache_insert(&s->pgcache, tl, key, block, pv->lsn, out);
			return served ? 1 : 0;
		}
		if (!timeline_has_parent(s, tl))
			return 0;
		if (s->timelines[tl].branch_lsn < rl)
			rl = s->timelines[tl].branch_lsn;
		tl = (uint32_t) s->timelines[tl].parent;
	}
}

/* ===================== recovery (rebuild index from segments) ========== */

/*
 * Physically clear the written bytes from (seg, off) forward in a shard's
 * segments, so a truncated tail cannot be resurrected: the old records past a
 * truncation point keep valid CRCs at their own offsets, so after the daemon
 * overwrites only some of them and crashes again, the next recover would otherwise
 * replay the untouched ones.
 *
 * Idempotent and self-bounding: it reads each chunk (only up to the segment's
 * written size) and rewrites zeros ONLY where it finds non-zero bytes.  So a clean
 * tail (cursor already at the segment's end) scans nothing, an already-zeroed tail
 * costs reads but no writes/sync, and -- crucially -- a recovery interrupted partway
 * through zeroing is finished by the next one (it scans the whole tail rather than
 * trusting a single probe).  Returns 1 if it wrote anything (caller must sync), 0
 * if nothing, -1 on error.
 */
static int
zero_shard_forward(uint32_t shard, int seg, uint64_t off)
{
	uint32_t	local = seg_local(seg);
	unsigned char *zbuf = calloc(1, page_size);
	unsigned char *rb = malloc(page_size);
	int			wrote = 0;
	int			err = 0;

	if (!zbuf || !rb)
	{
		free(zbuf);
		free(rb);
		return -1;
	}
	for (;; local++, off = 0)
	{
		int			id = seg_id(shard, local);
		int64_t		sz = ps_storage->seg_size(id);

		if (sz < 0)
			break;				/* no more segments in this shard */
		for (uint64_t p = off; p < (uint64_t) sz && !err;)
		{
			uint32_t	n = ((uint64_t) sz - p > page_size)
				? page_size : (uint32_t) ((uint64_t) sz - p);
			int			nz = 0;

			if (ps_storage->seg_read(id, p, rb, n) != 0)
			{
				err = 1;
				break;
			}
			for (uint32_t k = 0; k < n; k++)
				if (rb[k])
				{
					nz = 1;
					break;
				}
			if (nz)
			{
				if (ps_storage->seg_write(id, p, zbuf, n) != 0)
				{
					err = 1;
					break;
				}
				wrote = 1;
			}
			p += n;
		}
		if (err)
			break;
	}
	free(zbuf);
	free(rb);
	return err ? -1 : wrote;
}

/*
 * Rebuild the entire in-memory index at startup by replaying the segments in
 * order.  Each record is self-describing, so replaying append_page's effect
 * (page_add_version + fork_grow) for every record reconstructs both indexes and
 * leaves the append cursor positioned just past the last valid record.
 *
 * The scan stops at the first record that fails its magic/gen/len/CRC check.  Each
 * shard ends at its first non-full segment (a full one always rolled, so it is a
 * clean natural boundary; continuing past a partial one would index a later
 * segment while leaving the cursor mid-this-one).  Whether the stop is an expected
 * unsynced torn tail or corruption of already-synced data is then decided against
 * the durable sync watermark: recovering short of it means committed records are
 * missing -> fail open; stopping at/after it is the normal tail (valid unsynced
 * records past it are still replayed, honoring ack-durability).  Returns 0 on
 * success, -1 on such corruption or on allocation failure (so ps_core_open() does
 * not open with an empty/short index over a real store).
 *
 * Caveat (prototype): TRUNCATE and UNLINK are not logged as records, so their
 * effects are not reproduced on restart.
 */
static int
recover(void)
{
	unsigned char *pg = malloc(page_size);	/* scratch to read+verify each page */
	int			need_sync = 0;	/* a tail was zeroed and must be made durable */

	if (!pg)
		return -1;				/* don't open with an empty index over a real store */
	load_sync_watermark();		/* tells synced-data corruption from an unsynced tail */
	/* Each shard owns a disjoint segment-id namespace, so scan and replay each
	 * shard's segments and position its own append cursor.  page_add_version()
	 * self-routes by key, so a record always lands on the shard that wrote it. */
	for (uint32_t si = 0; si < ps_nshards; si++)
	{
		Shard	   *s = &g_shards[si];

		uint64_t	reclen = sizeof(SegRecHdr) + page_size;

		for (uint32_t local = 0;; local++)
		{
			int			id = seg_id(s->index, local);
			uint64_t	off = 0;
			int64_t		sz = ps_storage->seg_size(id);
			int			full = 0;	/* segment filled (rolled): a clean natural end */

			if (sz < 0)
				break;			/* no more segments in this shard -> done */

			/* replay records until one fails to validate (end of log) */
			for (;;)
			{
				SegRecHdr	hdr;

				/* A full segment (no room for another record) is a clean natural
				 * boundary -- mark it and let the shard continue to the next. */
				if (off + reclen > segment_size)
				{
					full = 1;
					break;
				}
				/*
				 * Otherwise stop at the first record that does not verify -- a
				 * torn/unsynced trailing append, a never-written tail, or stale
				 * bytes.  Whether that is an expected unsynced tail or corruption of
				 * already-synced data is decided after the shard scan against the
				 * sync watermark; zero_shard_forward() below physically discards it.
				 *
				 * But distinguish an expected short tail from a real backend read
				 * error: a full record needs sizeof(hdr)+page_size bytes present, so
				 * bytes past the segment's written size are the tail (break), while a
				 * seg_read failure WITHIN that size is an actual I/O error -- fail the
				 * open rather than truncate, which could otherwise zero live data on a
				 * transient read fault.  (On SPDK seg_size() reports a present segment
				 * as full, so its tail is detected by the magic/gen/CRC check on stale
				 * bytes and only a real do_io failure reaches the -1 paths.)
				 */
				if (off + reclen > (uint64_t) sz)
					break;
				if (ps_storage->seg_read(id, off, &hdr, sizeof(hdr)) != 0)
				{
					fprintf(stderr, "pagestore: shard %u segment %d off %llu: header "
							"read failed; refusing to recover\n",
							s->index, id, (unsigned long long) off);
					free(pg);
					return -1;
				}
				if (hdr.magic != SEG_MAGIC || hdr.len != page_size ||
					hdr.gen != g_store_gen)
					break;
				if (ps_storage->seg_read(id, off + sizeof(hdr), pg, page_size) != 0)
				{
					fprintf(stderr, "pagestore: shard %u segment %d off %llu: page "
							"read failed; refusing to recover\n",
							s->index, id, (unsigned long long) off);
					free(pg);
					return -1;
				}
				if (seg_rec_crc(id, off, &hdr, pg) != hdr.crc)
				{
					fprintf(stderr, "pagestore: shard %u segment %d off %llu: record "
							"failed CRC\n", s->index, id, (unsigned long long) off);
					break;
				}

				page_add_version(hdr.timeline, &hdr.key, hdr.block, hdr.lsn, id,
								 off + sizeof(hdr), ps_crc32c(pg, page_size));
				fork_grow(hdr.timeline, &hdr.key, hdr.block + 1);
				off += sizeof(hdr) + hdr.len;
			}

			/* An empty, not-full segment is this shard's end-of-log. */
			if (off == 0 && !full)
				break;
			/* newest segment seen for this shard; its append continues after it */
			s->cur_seg = id;
			s->cur_off = off;
			/* seed the packed cursor so a watermark capture before the first new
			 * append still reflects the recovered position */
			s->cur_cursor = ((uint64_t) (uint32_t) id << 32) | (uint32_t) off;
			/* A segment that stopped with room left (not full) is the shard's tail or
			 * a mid-log corruption -- the shard ends here, never continue past it
			 * (which would index a later segment yet leave the cursor mid-this-one). */
			if (!full)
				break;
		}

		/*
		 * Did we rebuild the whole durable log?  If a trustworthy watermark says
		 * this shard was synced through (local, off) but recovery stopped short of
		 * it, the missing records were synced -- this is corruption of acknowledged
		 * data, not an unsynced tail.  Fail open so it is visible rather than
		 * silently dropping committed writes.  (Stopping at/after the watermark is
		 * the normal case: any valid records past it are unsynced and still
		 * replayed; a failure past it is just the expected torn tail.)
		 */
		if (s->cur_seg >= 0 && g_wm_valid)
		{
			uint32_t	fl = seg_local(s->cur_seg);

			if (fl < g_wm[si].local ||
				(fl == g_wm[si].local && s->cur_off < g_wm[si].off))
			{
				fprintf(stderr, "pagestore: shard %u recovered only through segment "
						"%d off %llu but was synced through local %u off %llu: "
						"corrupt acknowledged data; refusing to recover\n",
						s->index, s->cur_seg, (unsigned long long) s->cur_off,
						g_wm[si].local, (unsigned long long) g_wm[si].off);
				free(pg);
				return -1;
			}
		}
		if (s->cur_seg >= 0)
			fprintf(stderr, "pagestore_daemon: shard %u recovered through segment "
					"%d (off %llu)\n", s->index, s->cur_seg,
					(unsigned long long) s->cur_off);

		if (s->cur_seg < 0)
		{
			int			first = seg_id(s->index, 0);

			if (g_wm_valid)
			{
				/*
				 * The watermark is authoritative.  If it says this shard was synced
				 * through real data but recovery rebuilt nothing, that committed data
				 * is gone (a missing/truncated/all-zero first segment, or lost SPDK
				 * counts) -- fail rather than silently open empty.
				 */
				if (g_wm[si].local > 0 || g_wm[si].off > 0)
				{
					fprintf(stderr, "pagestore: shard %u: watermark says synced "
							"through local %u off %llu but recovery found no records; "
							"refusing to recover\n", s->index, g_wm[si].local,
							(unsigned long long) g_wm[si].off);
					free(pg);
					return -1;
				}
				/*
				 * Watermark is exactly (0,0): the shard was synced while empty, so any
				 * bytes now in the first segment are an unsynced torn append past the
				 * watermark.  Discard them (don't mistake them for corruption) so a
				 * clean post-crash tail opens instead of bricking.
				 */
				if (ps_storage->seg_size(first) >= 0)
				{
					int			z = zero_shard_forward(s->index, first, 0);

					if (z < 0)
					{
						free(pg);
						return -1;
					}
					if (z)
						need_sync = 1;
				}
			}
			else
			{
				/*
				 * No trustworthy watermark: if the first segment holds non-zero data
				 * we could not replay it -- a wrong --page-size, a corrupt/foreign
				 * store_gen, or corruption of the very first record.  That is
				 * indistinguishable from stale raw-device bytes, and zeroing it would
				 * erase a valid store merely opened with the wrong config, so fail open
				 * instead.  A fresh store has no first segment (or an all-zero one from
				 * a prior truncation) and opens normally.
				 */
				SegRecHdr	probe;

				if (ps_storage->seg_size(first) >= 0 &&
					ps_storage->seg_read(first, 0, &probe, sizeof(probe)) == 0)
					for (size_t k = 0; k < sizeof(probe); k++)
						if (((unsigned char *) &probe)[k])
						{
							fprintf(stderr, "pagestore: shard %u: existing segment "
									"data could not be recovered (wrong --page-size, or "
									"a corrupt/foreign store generation); refusing to "
									"recover\n", s->index);
							free(pg);
							return -1;
						}
			}
		}
		else
		{
			/*
			 * Physically clear any written bytes past the append cursor so a
			 * torn/corrupt/stale tail can't resurrect after a partial overwrite +
			 * crash.  zero_shard_forward() is idempotent and self-bounding: a clean
			 * tail (cursor at the segment's written end) scans nothing, an already-
			 * zeroed tail writes nothing, and a previously-interrupted zeroing is
			 * finished here -- so it is safe to run unconditionally.
			 */
			int			z = zero_shard_forward(s->index, s->cur_seg, s->cur_off);

			if (z < 0)
			{
				free(pg);
				return -1;
			}
			if (z)
				need_sync = 1;
		}
	}
	if (need_sync && ps_storage->sync() != 0)
	{
		/* the truncation is only in memory; opening now would let a later crash
		 * replay the stale records we meant to drop -- fail the open instead */
		fprintf(stderr, "pagestore: could not durably zero a truncated tail; "
				"refusing to recover\n");
		free(pg);
		return -1;
	}
	free(pg);
	return 0;
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

		case PS_OP_FORK_SIZE_ADD:
			/* blocknum carries is_trunc (1 = truncation, 0 = extension) */
			fork_size_add(tl, &ch->key, ch->req_lsn, ch->nblocks,
						  ch->blocknum != 0);
			break;

		case PS_OP_FORK_SIZE_AT:
		{
			int			nb = fork_nblocks_at(tl, &ch->key, ch->req_lsn);

			ch->result = (nb < 0) ? PS_FORKSIZE_UNKNOWN : (uint32_t) nb;
			break;
		}

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
			/* branch-create is routed to shard 0, so validate against shard 0's
			 * copy.  Persist the branch durably BEFORE timeline_define publishes
			 * its 'defined' flag to every shard: under per-shard threads a peer
			 * shard that observes 'defined' could acknowledge writes on the branch,
			 * which must not outlive a crash before the metadata is durable. */
			if (!branch_request_ok(&g_shards[0], ch->timeline,
								   (int) ch->parent_timeline))
				ch->status = PS_STATUS_ERROR;
			else if (timeline_persist(ch->timeline, (int) ch->parent_timeline,
									  ch->req_lsn) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				timeline_define(ch->timeline, (int) ch->parent_timeline,
								ch->req_lsn);
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
		{
			uint64_t   *lsns = (uint64_t *) ch->data;
			uint32_t   *tls = (uint32_t *) (ch->data +
											(size_t) PS_WALIDX_CAP * sizeof(uint64_t));
			int			total = walidx_get(tl, &ch->key, ch->blocknum,
										   ch->req_lsn, lsns, tls, PS_WALIDX_CAP);

			/* result is the true match count (so the count path is uncapped);
			 * the payload holds only the first min(total, PS_WALIDX_CAP) pairs */
			ch->result = (uint32_t) total;
			ch->result_flags = (total > PS_WALIDX_CAP) ? PS_WALIDX_OVERFLOW : 0;
			break;
		}

		case PS_OP_IMMEDSYNC:
			/* capture-before-sync + commit, serialized against concurrent syncs */
			if (ps_core_sync_and_watermark() != 0)
				ch->status = PS_STATUS_ERROR;	/* report a failed durable sync */
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
	/* clean shutdown is a durability point: capture each shard's cursor, sync the
	 * segment data, and commit the watermark, so the next recover() trusts the full
	 * log as synced */
	ps_core_sync_and_watermark();
	for (uint32_t i = 0; i < ps_nshards; i++)
	{
		Shard	   *s = &g_shards[i];

		if (s->memtable)
		{
			ps_memtable_flush(s->memtable, alloc_layer_id, record_layer, s);
			ps_memtable_destroy(s->memtable);
			s->memtable = NULL;
		}
	}
	/* the shutdown flush can push a timeline over the compaction threshold;
	 * compact it back under so repeated short-lived runs don't accumulate image
	 * layers (each of which every read would then scan).  compaction is capped at
	 * COMPACT_BATCH per call, so loop per (shard, timeline) until it is under the
	 * threshold or stops making progress (corrupt layer / ENOSPC).  Only in the
	 * layer mode: a segment-path-only daemon (use_layers == 0, e.g. SPDK) has no
	 * memtable and must stay layer-free, like ps_core_maintenance(). */
	if (use_layers)
		for (uint32_t i = 0; i < ps_nshards; i++)
		{
			Shard	   *s = &g_shards[i];

			for (uint32_t ct = 0; ct < MAX_TIMELINES; ct++)
			{
				if (ct != 0 && !tl_defined(s, ct))
					continue;
				while (count_image_layers(s, ct) > (uint32_t) compact_layers)
				{
					uint32_t	before = count_image_layers(s, ct);

					if (compact_timeline(s, ct) != 0 ||
						count_image_layers(s, ct) >= before)
						break;	/* failed or no progress: stop draining this one */
				}
			}
		}
	for (uint32_t i = 0; i < ps_nshards; i++)
	{
		ps_pgcache_free(&g_shards[i].pgcache);
		ps_manifest_close(&g_shards[i].manifest);
	}
}

/*
 * Upload one sealed layer that isn't yet remote-durable to the object tier and
 * mark it durable (LSM phase 4).  Runs in maintenance (only when the worker found
 * no pending request), one layer per call, between channel-poll cycles -- the
 * same model as compact_timeline().  For the local-directory object store the
 * copy is fast; with a slow remote provider a single layer's upload would still
 * block this shard's polling for its duration, so when a real remote backend
 * lands this moves to a background uploader.  Restart re-attempts any still-not-
 * durable layer and upload is idempotent (keyed by layer id).  Returns 1 if it
 * uploaded one, 0 if none were pending, -1 if a layer was pending but failed.
 */
static int
upload_one(Shard *s)
{
	PsLayerMap *map = &s->manifest.map;

	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		PsLayerDesc *d = &map->layers[i];

		if (d->deleting)
			continue;
		/* Skip only layers whose object is actually present in the configured
		 * tier.  The persisted remote_durable flag alone is not proof: a store
		 * reopened against a different or freshly-recreated --object-dir replays
		 * the flag but the object is absent in this run's tier, so re-upload it
		 * rather than leave remote durability silently missing. */
		if (d->remote_durable && ps_layer_store->layer_exists_remote(d) == 1)
			continue;
		/* upload, then confirm it is really there before marking durable, so a
		 * local copy is never treated as evictable on an unverified upload.  A
		 * failure is reported distinctly from "nothing pending" so the caller can
		 * back off instead of hot-looping on an unwritable/unavailable store. */
		if (ps_layer_store->upload_layer(d) != 0 ||
			ps_layer_store->layer_exists_remote(d) != 1)
			return -1;			/* a layer was pending but the upload failed */
		if (ps_manifest_set_remote_durable(&s->manifest, d->layer_id,
										   d->lsn_end) != 0)
			return -1;
		return 1;				/* uploaded one */
	}
	return 0;					/* nothing pending */
}

/*
 * One unit of off-the-write-path background maintenance for a single shard:
 * compact one of its timelines past the (low-water) threshold, else upload one
 * not-yet-remote-durable layer to the object tier.  Per shard so each worker
 * thread (sharding step 4c) runs its own maintenance with no shared state.
 * Returns 1 if it did work (caller should not sleep), 0 if nothing was due.
 */
static int
maintain_shard(Shard *s)
{
	int			attempted = 0;

	if (!use_layers)
		return 0;
	/*
	 * Compaction backs off on its own cooldown after a no-progress attempt (a
	 * corrupt layer, or repeated ENOSPC during the merge), so it doesn't re-read
	 * the same failing layers every idle tick.
	 */
	if (s->maint_cooldown == 0 || time(NULL) >= s->maint_cooldown)
	{
		for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
			if ((tl == 0 || tl_defined(s, tl)) &&
				count_image_layers(s, tl) > (uint32_t) compact_layers)
			{
				uint32_t	before = count_image_layers(s, tl);

				/* progress only if the layer count actually dropped; a timeline
				 * that can't make progress must not keep the daemon spinning */
				attempted = 1;
				if (compact_timeline(s, tl) == 0 &&
					count_image_layers(s, tl) < before)
				{
					s->maint_cooldown = 0;	/* progress: clear any backoff */
					return 1;
				}
			}
		if (attempted)
			s->maint_cooldown = time(NULL) + MAINT_COOLDOWN_SECS;
	}
	/*
	 * Object-tier upload backs off on a SEPARATE cooldown: a temporarily
	 * unwritable/full object store must not suppress local compaction (which still
	 * reduces read amplification), and a failed upload must not become a 20us retry
	 * storm.  Compaction above already runs regardless of this cooldown.
	 */
	if (ps_object_tier &&
		(s->upload_cooldown == 0 || time(NULL) >= s->upload_cooldown))
	{
		int			u = upload_one(s);

		if (u > 0)
		{
			s->upload_cooldown = 0;	/* drained one; retry next tick for the rest */
			return 1;
		}
		/* u == 0 (nothing pending) or u < 0 (failed): back off either way, so a
		 * quiescent daemon doesn't re-scan the manifest -- stat()-ing every durable
		 * layer -- on every 20us idle tick.  A newly sealed layer waits at most one
		 * cooldown before its upload, which is fine off the write path. */
		s->upload_cooldown = time(NULL) + MAINT_COOLDOWN_SECS;
	}
	return 0;
}

/* Maintenance for one shard by index (the per-shard worker threads call this). */
int
ps_core_maintenance_shard(uint32_t shard)
{
	if (shard >= ps_nshards)
		return 0;
	return maintain_shard(&g_shards[shard]);
}

/*
 * Maintenance across all shards (the single-threaded frontends, e.g. SPDK, call
 * this).  Returns 1 if any shard compacted.
 */
int
ps_core_maintenance(void)
{
	for (uint32_t i = 0; i < ps_nshards; i++)
		if (maintain_shard(&g_shards[i]))
			return 1;
	return 0;
}

/* fsync a store directory so a freshly created metadata file's entry is durable */
static void
fsync_store_dir(const char *store_dir)
{
	int			fd = open(store_dir, O_RDONLY);

	if (fd >= 0)
	{
		fsync(fd);
		close(fd);
	}
}

/*
 * The store records the shard count it was created with.  Per-shard segment and
 * manifest namespaces -- and how recover() partitions segments -- are derived
 * from ps_nshards, so reopening with a different count would mis-route or skip
 * live data; reject the mismatch.  A pre-sharding store (segment data present but
 * no shard-count file) is only reopenable at nshards == 1, since recovery would
 * otherwise replay its single global segment stream out of order.  Returns 0 on
 * match / first-time record, -1 on mismatch or I/O error.
 */
static int
validate_store_nshards(const char *store_dir)
{
	char		path[4096];
	char		buf[32];
	int			fd;
	ssize_t		n;

	if (snprintf(path, sizeof(path), "%s/nshards", store_dir) >= (int) sizeof(path))
		return -1;

	fd = open(path, O_RDONLY);
	if (fd >= 0)
	{
		uint32_t	stored;

		n = read(fd, buf, sizeof(buf) - 1);
		close(fd);
		if (n <= 0)
			return -1;
		buf[n] = '\0';
		stored = (uint32_t) strtoul(buf, NULL, 10);
		if (stored != ps_nshards)
		{
			fprintf(stderr, "pagestore_core: store was created with %u shard(s); "
					"reopen with --nshards %u\n", stored, stored);
			return -1;
		}
		return 0;
	}

	/* no shard-count file: a pre-sharding store (already holds segment data) is
	 * only valid at one shard */
	if (ps_storage->seg_size(0) >= 0 && ps_nshards != 1)
	{
		fprintf(stderr, "pagestore_core: pre-sharding store; reopen with "
				"--nshards 1\n");
		return -1;
	}

	/* first open of this store: record the shard count durably */
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	n = snprintf(buf, sizeof(buf), "%u\n", ps_nshards);
	if (write(fd, buf, (size_t) n) != n || fsync(fd) != 0)
	{
		close(fd);
		return -1;
	}
	close(fd);
	fsync_store_dir(store_dir);	/* make the new file's directory entry durable */
	return 0;
}

/*
 * Persist and validate the store's segment_size.  recover() interprets segment
 * boundaries (where a segment is "full" and rolls) from segment_size, so reopening
 * a POSIX store with a different --segment-size would mis-locate the log tail and
 * could truncate/zero live later segments.  (SPDK records segment_size in its
 * superblock; this gives POSIX -- and any backend -- the same protection.)
 * Returns 0 on match / first-time record, -1 on mismatch or I/O error.
 */
static int
validate_store_segment_size(const char *store_dir)
{
	char		path[4096];
	char		buf[64];
	int			fd;
	ssize_t		n;

	if (snprintf(path, sizeof(path), "%s/segment_size", store_dir) >=
		(int) sizeof(path))
		return -1;

	fd = open(path, O_RDONLY);
	if (fd >= 0)
	{
		uint64_t	stored;

		n = read(fd, buf, sizeof(buf) - 1);
		close(fd);
		if (n <= 0)
			return -1;
		buf[n] = '\0';
		stored = strtoull(buf, NULL, 10);
		if (stored != segment_size)
		{
			fprintf(stderr, "pagestore_core: store was created with segment_size "
					"%llu; reopen with --segment-size %llu\n",
					(unsigned long long) stored, (unsigned long long) stored);
			return -1;
		}
		return 0;
	}

	/* first open of this store: record segment_size durably */
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	n = snprintf(buf, sizeof(buf), "%llu\n", (unsigned long long) segment_size);
	if (write(fd, buf, (size_t) n) != n || fsync(fd) != 0)
	{
		close(fd);
		return -1;
	}
	close(fd);
	fsync_store_dir(store_dir);	/* make the new file's directory entry durable */
	return 0;
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
	crc32c_init();				/* before recover() and any worker thread */
	snprintf(g_store_dir, sizeof(g_store_dir), "%s", store_dir);
	if (ps_storage->open(store_dir, segment_size) != 0)
		return -1;
	if (ps_layer_store->open(store_dir) != 0)
		return -1;
	/* Validate --nshards against the stored value BEFORE minting a generation: the
	 * freshness check below probes the live shards' first segments, so a wrong
	 * nshards could misjudge an existing store as fresh and durably write a new
	 * store_gen before this rejected the mismatch -- leaving the real records'
	 * generations stale on the next correct-nshards open. */
	if (validate_store_nshards(store_dir) != 0)
		return -1;
	if (validate_store_segment_size(store_dir) != 0)
		return -1;
	if (load_store_gen(store_dir) != 0)	/* generation stamped into each record */
	{
		fprintf(stderr, "pagestore_core: could not durably establish store "
				"generation in %s\n", store_dir);
		return -1;
	}

	/* the LSM write side (memtable/flush/compaction) runs only when layers are
	 * the read path; the SPDK daemon stays on the segment path for now.  Reject
	 * non-positive thresholds before their unsigned casts: a negative
	 * --flush-pages would become a huge flush threshold (memtable never flushes
	 * -> unbounded RAM), and a negative --compact-layers a huge compaction
	 * threshold (compaction never runs -> layers grow without bound). */
	if (use_layers)
	{
		if (flush_pages <= 0)
		{
			fprintf(stderr, "pagestore_core: --flush-pages must be positive "
					"(got %d)\n", flush_pages);
			return -1;
		}
		if (compact_layers <= 0)
		{
			fprintf(stderr, "pagestore_core: --compact-layers must be positive "
					"(got %d)\n", compact_layers);
			return -1;
		}
	}
	/* reject a negative --cache-pages before the unsigned cast below (it would
	 * become a huge per-shard capacity and make ps_pgcache_init attempt an
	 * enormous allocation); 0 disables the cache */
	if (cache_pages < 0)
	{
		fprintf(stderr, "pagestore_core: --cache-pages must be >= 0 (got %d)\n",
				cache_pages);
		return -1;
	}
	/* a segment record (header + one page) must fit in a segment; otherwise a
	 * grossly-wrong --page-size would make recover()'s first record overflow the
	 * segment and be mis-handled */
	if (sizeof(SegRecHdr) + page_size > segment_size)
	{
		fprintf(stderr, "pagestore_core: page_size %u + record header exceeds "
				"segment_size %llu\n", page_size,
				(unsigned long long) segment_size);
		return -1;
	}
	/* the sync-watermark capture packs (seg, off) into one 64-bit word with the
	 * offset in the low 32 bits, so a segment must be addressable in 32 bits */
	if (segment_size > 0xFFFFFFFFULL)
	{
		fprintf(stderr, "pagestore_core: segment_size %llu exceeds 4 GiB\n",
				(unsigned long long) segment_size);
		return -1;
	}

	/* per-shard: open + replay its manifest, finish any interrupted GC, continue
	 * layer ids past the highest the manifest restored (low bits only -- the
	 * shard id is OR'd in at allocation), and create the shard's memtable */
	for (uint32_t i = 0; i < ps_nshards; i++)
	{
		Shard	   *s = &g_shards[i];

		s->index = i;
		s->cur_seg = -1;		/* recover() positions the append cursor */
		s->cur_off = 0;
		s->cur_cursor = 0;		/* clear any packed cursor from a prior open of
								 * this process, so a capture before recover/append
								 * doesn't read a stale watermark position */
		/* layer_ids are unique only within a store; drop blooms cached from a
		 * previously-opened store so a reused id can't return a stale "absent" */
		ps_image_bloom_reset(&s->bloom);
		if (ps_manifest_open(&s->manifest, store_dir, i) != 0)
			return -1;
		if (ps_manifest_replay(&s->manifest) != 0)
			return -1;
		if (use_layers)
			gc_resume(s);
		s->next_local_id = 1;
		for (uint32_t j = 0; j < s->manifest.map.nlayers; j++)
		{
			uint64_t	local = s->manifest.map.layers[j].layer_id &
				LAYER_ID_LOCAL_MASK;

			if (local >= s->next_local_id)
				s->next_local_id = local + 1;
		}
		if (use_layers)
		{
			s->memtable = ps_memtable_create(page_size, (uint32_t) flush_pages);
			if (!s->memtable)
				return -1;
		}
		/* The materialized-page cache helps both read paths (read_resolve and the
		 * SPDK async path), so it is not gated on use_layers.  --cache-pages is the
		 * total budget across shards; split it so total RAM is independent of
		 * nshards (each shard's cache stays single-owner).  Hand the division
		 * remainder to the first shards rather than flooring it away, so a small
		 * positive budget (e.g. cache_pages < nshards) still caches its pages
		 * instead of disabling every shard's cache.  At nshards == 1 the shard gets
		 * the whole budget -- identical to before. */
		{
			uint32_t	budget = (uint32_t) cache_pages;
			uint32_t	per = budget / ps_nshards + (i < budget % ps_nshards ? 1 : 0);

			ps_pgcache_init(&s->pgcache, per, page_size);
		}
	}
	fprintf(stderr, "pagestore_core: %u image layer(s) across %u shard(s) after "
			"manifest replay\n", ps_core_layer_count(), ps_nshards);

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
	if (recover() != 0)			/* corruption or OOM: don't open over a real store */
		return -1;

	/* rebuild each timeline's shipped-WAL end LSN from its log (WAL is shard-0,
	 * single-threaded here; all shards' timeline copies are identical) */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || tl_defined(&g_shards[0], tl))
			wal_recover_one(tl);

	return 0;
}
