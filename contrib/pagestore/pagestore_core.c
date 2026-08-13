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
 *	  Startup rebuilds it from the durable image-layer prefix plus the uncovered
 *	  segment tail; SPDK, which does not yet use layers, scans all segments.
 *
 * This file holds everything backend- and loop-agnostic; each frontend (the
 * POSIX daemon, the SPDK daemon) supplies its own request loop and page byte
 * I/O.  Includes only pagestore_ipc.h/pagestore_storage.h and libc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
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
#include "pagestore_prune.h"
#include "pagestore_retention.h"

/* configuration, set by the frontend before ps_core_open() */
uint32_t	page_size = PS_DEFAULT_PAGE_SIZE;
uint64_t	segment_size = 8 * 1024 * 1024;
int			flush_pages = 256;	/* memtable flush threshold (pages) */
int			compact_layers = 8;	/* compact a timeline past this many image layers */
int			segment_gc_enabled = 1;
int			cache_pages = 1024;	/* materialized-page cache size (pages; 0=off) */
/*
 * Use the LSM read path: rebuild the index from image layers on restart and
 * (in the frontend) serve reads via read_resolve.  The POSIX daemon enables it;
 * the SPDK daemon leaves it off for now because its async read path serves pages
 * by segment offset (async layer reads are a later step), so it must keep the
 * segment-scan recovery that gives versions real segment locations.
 */
int			use_layers = 1;

static pthread_t tier_upload_thread;
static PsLayerDesc tier_upload_candidate;
static volatile int tier_upload_state; /* 0 idle, 1 running, 2 success, 3 failed */
static int tier_upload_joined;
static uint32_t tier_upload_shard_cursor;
static struct timespec tier_upload_retry_at;
static uint64_t tier_upload_layer_cursor[PS_MAX_CHANNELS];
static int tier_one_layer(void);
static int map_locks_ready;
static const PsLayerLocation *tier_local_location(const PsLayerDesc *layer);
static int refresh_remote_only_layer(const PsLayerDesc *layer);
static int read_image_index_refreshing(const PsLayerDesc *layer,
									   PsImgIndexEnt **idx, uint32_t *n);
static int read_layer_block_refreshing(const PsLayerDesc *layer, uint64_t off,
									   void *buf, uint32_t len);
static int verify_image_layer_refreshing(const PsLayerDesc *layer);
static pthread_t gc_remote_thread;
static PsLayerDesc gc_remote_candidate;
static volatile int gc_remote_state; /* 0 idle, 1 running, 2 remote success, 3 failed */
static uint64_t gc_remote_layer_cursor;
static uint32_t gc_remote_map_cursor;
static struct timespec gc_remote_retry_at;
static pthread_t evict_local_thread;
static PsLayerDesc evict_local_candidate;
static volatile int evict_local_state; /* 0 idle, 1 verifying, 2 verified, 3 failed */
static uint32_t evict_local_map_cursor;
static void page_remove_compacted_versions(uint32_t timeline,
										   const PsImgRec *recs, uint32_t nrec);
static int retention_project_lsn(uint32_t descendant, uint32_t target,
								 uint64_t *lsn);
static int page_prune_fences(uint32_t timeline, PsPruneFence **fences_out,
								 uint32_t *nfences_out);

/* the active storage backend (POSIX by default; the frontend may override) */
const PsStorage *ps_storage = &PsStoragePosix;

/* configured logical shards for this daemon (set by frontend main before open()) */
uint32_t	ps_nshards = 1;

/* Durable identity for newly bound ordered segment records.  Recovery observes
 * every persisted identity before the daemon accepts writes, so allocation
 * continues above both committed and uncommitted records after a restart. */
static uint64_t next_segment_order_id = 1;
static uint64_t next_admission_seq = 1;
static pthread_rwlock_t admission_lock = PTHREAD_RWLOCK_INITIALIZER;
/* A page-history pin must not change between a compaction floor snapshot and
 * publication of the pruned replacement layer. */
static pthread_rwlock_t page_prune_lock = PTHREAD_RWLOCK_INITIALIZER;

#define PS_PAGE_FRONTIER_MAGIC 0x46504750U /* "PGPF" */
#define PS_PAGE_FRONTIER_VERSION 2
typedef struct PsPageFrontierState
{
	uint32_t	magic;
	uint32_t	version;
	PsPruneFence frontiers[1024];
	uint32_t	crc;
} PsPageFrontierState;
static char page_frontier_path[4096];
static char page_frontier_dir[4096];
static PsPruneFence page_reclaimed_frontier[1024];
static int page_frontier_load(const char *store_dir);
static int page_frontier_advance(uint32_t timeline, uint64_t floor,
								 uint64_t admission_seq);

static uint64_t
admission_seq_alloc(void)
{
	uint64_t	next = __atomic_load_n(&next_admission_seq, __ATOMIC_RELAXED);

	for (;;)
	{
		if (next == 0 || next == UINT64_MAX)
			return 0;
		if (__atomic_compare_exchange_n(&next_admission_seq, &next, next + 1,
								false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
			return next;
	}
}

static void
admission_seq_observe(uint64_t seq)
{
	uint64_t	next = __atomic_load_n(&next_admission_seq, __ATOMIC_RELAXED);

	while (next <= seq && next != UINT64_MAX &&
		   !__atomic_compare_exchange_n(&next_admission_seq, &next,
								 seq == UINT64_MAX ? UINT64_MAX : seq + 1,
									 false, __ATOMIC_RELAXED,
									 __ATOMIC_RELAXED))
		;
}

void
ps_admission_read_lock(void)
{
	pthread_rwlock_rdlock(&admission_lock);
}

void
ps_admission_read_unlock(void)
{
	pthread_rwlock_unlock(&admission_lock);
}

uint64_t
ps_admission_barrier(void)
{
	uint64_t	seq;

	pthread_rwlock_wrlock(&admission_lock);
	seq = admission_seq_alloc();
	if (seq != 0 && ps_retention_reserve_admission_seq(seq) != 0)
		seq = 0;
	pthread_rwlock_unlock(&admission_lock);
	return seq;
}

static uint64_t
segment_order_id_alloc(void)
{
	return __atomic_fetch_add(&next_segment_order_id, 1, __ATOMIC_RELAXED);
}

static void
segment_order_id_observe(uint64_t order_id)
{
	uint64_t	next = __atomic_load_n(&next_segment_order_id, __ATOMIC_RELAXED);

	while (next <= order_id &&
		   !__atomic_compare_exchange_n(&next_segment_order_id, &next,
										 order_id + 1, false,
										 __ATOMIC_RELAXED, __ATOMIC_RELAXED))
		;
}

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
	PsFlushWatermark flush_watermark;
	uint32_t	gc_next_seg;		/* oldest segment not yet reclaimed */
	int			flush_watermark_valid;
	int			coverage_broken;	/* a record was not staged; do not advance */
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
 * ps_layer_map and the timelines[] array.  Lock order is always shard (outer,
 * ascending shard id when taking more than one) then map (inner), never the
 * reverse, so there is no deadlock.
 *
 * Each daemon worker owns exactly one shard and only ever touches its own
 * g_shards[]; the maintenance controller takes one shard for compaction or all
 * shards for physical-segment rebinding, then map_lock.  Reads take shard-rd +
 * map-rd; ordinary writes take only shard-wr, escalating to a brief map-wr
 * inside append_page when a flush mutates the map; branch
 * creation takes map-wr alone.
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

static int
layer_matches_read_shard(const PsLayerDesc *layer, uint32_t shard)
{
	uint32_t	layer_shard = layer_shard_from_id(layer->layer_id);

	return layer_shard == shard ||
		(layer->legacy_shard_zero && layer_shard == 0 && shard != 0);
}

static int
store_shard_count_path(const char *store_dir, char *path, size_t path_len)
{
	int			n;

	n = snprintf(path, path_len, "%s/.pagestore-nshards", store_dir);
	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

static int
fsync_dir_path(const char *path)
{
	int			fd = open(path, O_RDONLY | O_DIRECTORY);
	int			rc = 0;

	if (fd < 0)
		return -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

static int
publish_store_shard_count(const char *store_dir)
{
	char		path[4096];
	char		tmp[4096];
	FILE	   *f;
	uint32_t	current = core_shards();
	int			n;

	if (store_shard_count_path(store_dir, path, sizeof(path)) != 0)
		return -1;
	n = snprintf(tmp, sizeof(tmp), "%s.tmp.%ld", path, (long) getpid());
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;
	f = fopen(tmp, "w");
	if (f == NULL)
		return -1;
	if (fprintf(f, "%u\n", current) < 0 || fflush(f) != 0 ||
		fsync(fileno(f)) != 0)
	{
		fclose(f);
		unlink(tmp);
		return -1;
	}
	if (fclose(f) != 0)
	{
		unlink(tmp);
		return -1;
	}
	if (rename(tmp, path) != 0)
	{
		unlink(tmp);
		return -1;
	}
	if (fsync_dir_path(store_dir) != 0)
		return -1;
	return 0;
}

static int
parse_segment_file_shard(const char *name, uint32_t *shard)
{
	const char *p;
	char	   *end;
	unsigned long first;

	if (strncmp(name, "seg_", 4) != 0)
		return 0;
	p = name + 4;
	errno = 0;
	first = strtoul(p, &end, 10);
	if (end == p || errno != 0 || first > UINT32_MAX)
		return 0;
	if (*end == '\0')
	{
		*shard = 0;
		return 1;
	}
	if (*end != '_')
		return 0;
	p = end + 1;
	errno = 0;
	(void) strtoul(p, &end, 10);
	if (end == p || errno != 0 || *end != '\0')
		return 0;
	*shard = (uint32_t) first;
	return 1;
}

static int
infer_segment_shard_count(const char *store_dir, uint32_t *inferred)
{
	DIR		   *dir;
	struct dirent *ent;
	uint32_t	found = *inferred;

	dir = opendir(store_dir);
	if (dir == NULL)
		return -1;
	while ((ent = readdir(dir)) != NULL)
	{
		uint32_t	shard;

		if (parse_segment_file_shard(ent->d_name, &shard))
		{
			if (shard >= PS_MAX_CHANNELS)
				found = PS_MAX_CHANNELS + 1;
			else if (shard + 1 > found)
				found = shard + 1;
		}
	}
	if (closedir(dir) != 0)
		return -1;
	*inferred = found;
	return 0;
}

static int
validate_store_shard_count(const char *store_dir, int *publish_needed)
{
	char		path[4096];
	FILE	   *f;
	uint32_t	current = core_shards();
	uint32_t	persisted = 0;
	uint32_t	inferred = 1;
	int			have_persisted = 0;

	*publish_needed = 0;
	if (store_shard_count_path(store_dir, path, sizeof(path)) != 0)
		return -1;
	f = fopen(path, "r");
	if (f != NULL)
	{
		if (fscanf(f, "%u", &persisted) == 1 && persisted > 0 &&
			persisted <= PS_MAX_CHANNELS)
			have_persisted = 1;
		fclose(f);
		if (!have_persisted)
			return -1;
	}
	else if (errno != ENOENT)
		return -1;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		uint32_t	shard = layer_shard_from_id(ps_layer_map.layers[i].layer_id);

		if (shard + 1 > inferred)
			inferred = shard + 1;
	}
	for (uint32_t shard = 0; shard < PS_MAX_CHANNELS; shard++)
	{
		PsFlushWatermark watermark;

		if (ps_manifest_get_flush_watermark(shard, &watermark) &&
			shard + 1 > inferred)
			inferred = shard + 1;
	}
	if (!have_persisted && infer_segment_shard_count(store_dir, &inferred) != 0)
		return -1;
	if (have_persisted && persisted != 1 && persisted != current)
		return -1;
	if (!have_persisted && inferred != 1 && inferred != current)
		return -1;
	if (!have_persisted || persisted != current)
		*publish_needed = 1;
	return 0;
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
	uint64_t	id;
	int			exists;

	if (!s)
		s = &g_shards[0];
	do
	{
		id = layer_id(s->id, s->next_layer_id++);
		exists = ps_layer_store->layer_exists_local ?
			ps_layer_store->layer_exists_local(id) : 0;
	} while (exists > 0);
	return id;
}

static int
record_layer(void *ctx, const PsLayerDesc *desc)
{
	(void) ctx;
	/* ps_manifest_add_layer persists the ADD event *and* adds it to the layer
	 * map (idempotently); do not add to the map a second time. */
	return ps_manifest_add_layer(desc);
}

static int
mark_legacy_shard_zero_layers(void)
{
	if (core_shards() <= 1)
		return 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *layer = &ps_layer_map.layers[i];
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;
		int			legacy = 0;

		if (layer->kind != PS_LAYER_IMAGE || layer->deleting ||
			layer_shard_from_id(layer->layer_id) != 0)
			continue;
		if (read_image_index_refreshing(layer, &idx, &nidx) != 0)
			return -1;
		for (uint32_t j = 0; j < nidx; j++)
			if (ps_key_shard(&idx[j].key, core_shards()) != 0)
			{
				legacy = 1;
				break;
			}
		free(idx);
		layer->legacy_shard_zero = legacy != 0;
	}
	return 0;
}

static int
flush_memtable(Shard *s, uint32_t seg_id, uint64_t seg_off)
{
	int			rc;

	if (!s->memtable || ps_memtable_count(s->memtable) == 0)
		return 0;
	rc = ps_memtable_flush(s->memtable, alloc_layer_id, record_layer, s);
	if (rc != 0)
	{
		s->coverage_broken = 1;
		return -1;
	}
	if (s->coverage_broken)
		return 0;
	if (ps_manifest_set_flush_watermark(s->id, seg_id, seg_off) != 0)
	{
		s->coverage_broken = 1;
		return -1;
	}
	s->flush_watermark.shard = s->id;
	s->flush_watermark.seg_id = seg_id;
	s->flush_watermark.seg_off = seg_off;
	s->flush_watermark_valid = 1;
	return 0;
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

static const PsLayerLocation *tier_remote_location(const PsLayerDesc *layer);
static const PsLayerLocation *tier_local_location(const PsLayerDesc *layer);

/*
 * Finish any GC that a crash interrupted: every layer still marked 'deleting' in
 * the manifest has its local and remote files removed (idempotently) and a
 * REMOVE_LAYER event recorded.  Reads already skip 'deleting' layers, so this
 * only reclaims space.
 */
static int __attribute__((unused))
gc_resume(void)
{
	PsLayerDesc *dead;
	uint32_t	m = 0;
	int		did = 0;
	int		uploading;
	uint64_t	uploading_id;

	uploading = __atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1;
	uploading_id = tier_upload_candidate.layer_id;
	if (map_locks_ready)
		ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].deleting &&
			!(uploading && ps_layer_map.layers[i].layer_id == uploading_id) &&
			__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
						__ATOMIC_ACQUIRE) == 0)
			m++;
	if (m == 0)
	{
		if (map_locks_ready)
			ps_unlock_map();
		return 0;
	}
	dead = malloc((size_t) m * sizeof(PsLayerDesc));
	if (!dead)
	{
		if (map_locks_ready)
			ps_unlock_map();
		return 0;
	}
	m = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].deleting &&
			!(uploading && ps_layer_map.layers[i].layer_id == uploading_id) &&
			__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
						__ATOMIC_ACQUIRE) == 0)
			dead[m++] = ps_layer_map.layers[i];
	if (map_locks_ready)
		ps_unlock_map();
	for (uint32_t k = 0; k < m; k++)
	{
		int		remote_failed = 0;
		PsLayerDesc remote = dead[k];
		/*
		 * Drop the manifest entry only after the file is gone (a missing file
		 * is ENOENT == success in delete_local_layer, so this is idempotent and
		 * a partially-deleted layer still completes).  A real unlink error keeps
		 * the layer "deleting" so the next start retries it.  A REMOVE_LAYER
		 * write error may have torn the manifest tail; stop so that record stays
		 * the recoverable tail instead of becoming interior corruption, and the
		 * next start retries from the last valid manifest state.
	 */
		if (tier_remote_location(&remote) == NULL &&
			ps_layer_store->remote_uri != NULL &&
			remote.location_count < PS_LAYER_MAX_LOCATIONS &&
			ps_layer_store->remote_uri(remote.layer_id,
				remote.locations[remote.location_count].uri,
				sizeof(remote.locations[remote.location_count].uri)) == 0)
		{
			remote.locations[remote.location_count].tier = PS_LAYER_TIER_REMOTE_OBJECT;
			remote.locations[remote.location_count].available = true;
			remote.location_count++;
		}
		if (tier_remote_location(&remote) != NULL &&
			ps_layer_store->delete_remote_layer(&remote) != 0)
			remote_failed = 1;
		if (ps_layer_store->delete_local_layer(&dead[k]) != 0 || remote_failed)
			continue;
		if (map_locks_ready)
			ps_lock_map_wr();
	if (map_locks_ready)
		{
		int still_deleting = 0;

		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == dead[k].layer_id &&
				ps_layer_map.layers[i].deleting)
				still_deleting = 1;
		if (!still_deleting)
		{
			ps_unlock_map();
			continue;
		}
		}
		if (ps_manifest_remove_layer(dead[k].layer_id) != 0)
		{
			if (map_locks_ready)
				ps_unlock_map();
			break;
		}
		if (map_locks_ready)
			ps_unlock_map();
		did = 1;
	}
	free(dead);
	return did;
}

static void *
gc_remote_worker(void *arg)
{
	PsLayerDesc *layer = arg;
	int old_state;
	int rc;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_state);
	rc = ps_layer_store->delete_remote_layer(layer);
	pthread_setcancelstate(old_state, NULL);
	pthread_testcancel();

	__atomic_store_n(&gc_remote_state, rc == 0 ? 2 : 3, __ATOMIC_RELEASE);
	return NULL;
}

/*
 * Finish the local half of GC while holding the map lock that serializes the
 * descriptor lookup and REMOVE_LAYER append.  remote_done is process-local:
 * after a crash, retrying the provider delete is required and must remain
 * idempotent, but during this process a failed unlink must not issue it twice.
 */
static int
gc_finish_local(uint64_t layer_id, int remote_done)
{
	PsLayerDesc *layer = NULL;

	ps_lock_map_wr();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == layer_id &&
			ps_layer_map.layers[i].deleting)
		{
			layer = &ps_layer_map.layers[i];
			break;
		}
	if (layer == NULL)
	{
		ps_unlock_map();
		return 1;
	}
	if (remote_done)
		layer->remote_cleanup_done = true;
	if (ps_layer_store->delete_local_layer(layer) != 0 ||
		ps_manifest_remove_layer(layer_id) != 0)
	{
		ps_unlock_map();
		return 0;
	}
	ps_unlock_map();
	return 1;
}

/* Run at most one remote-GC operation without blocking the maintenance loop. */
static int
gc_remote_one(void)
{
	int state = __atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE);
	struct timespec now;

	clock_gettime(CLOCK_MONOTONIC, &now);
	if (now.tv_sec < gc_remote_retry_at.tv_sec ||
		(now.tv_sec == gc_remote_retry_at.tv_sec && now.tv_nsec < gc_remote_retry_at.tv_nsec))
		return 0;

	if (state == 1)
		return 0;
	if (state == 2 || state == 3)
	{
		pthread_join(gc_remote_thread, NULL);
		if (state != 2)
		{
			__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
			gc_remote_retry_at = now;
			gc_remote_retry_at.tv_sec++;
			return 0;
		}
		__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
		if (!gc_finish_local(gc_remote_candidate.layer_id, 1))
		{
			gc_remote_retry_at = now;
			gc_remote_retry_at.tv_sec++;
			return 0;
		}
		return 1;
	}
	ps_lock_map_rd();
	for (uint32_t pass = 0; pass < ps_layer_map.nlayers; pass++)
	{
		uint32_t i = (gc_remote_map_cursor + pass) % ps_layer_map.nlayers;
		{
			if (ps_layer_map.layers[i].deleting &&
				!(__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1 &&
				  ps_layer_map.layers[i].layer_id == tier_upload_candidate.layer_id))
			{
				gc_remote_candidate = ps_layer_map.layers[i];
				gc_remote_layer_cursor = gc_remote_candidate.layer_id;
				gc_remote_map_cursor = (i + 1) % ps_layer_map.nlayers;
				ps_unlock_map();
				if (gc_remote_candidate.remote_cleanup_done)
					return gc_finish_local(gc_remote_candidate.layer_id, 0);
				if (tier_remote_location(&gc_remote_candidate) == NULL)
				{
					PsLayerLocation *remote;
					int		uri_errno = 0;

					errno = 0;
					if (ps_layer_store->remote_uri != NULL &&
						gc_remote_candidate.location_count < PS_LAYER_MAX_LOCATIONS &&
						ps_layer_store->remote_uri(gc_remote_candidate.layer_id,
							gc_remote_candidate.locations[gc_remote_candidate.location_count].uri,
							sizeof(gc_remote_candidate.locations[gc_remote_candidate.location_count].uri)) == 0)
					{
						remote = &gc_remote_candidate.locations[gc_remote_candidate.location_count++];
						remote->tier = PS_LAYER_TIER_REMOTE_OBJECT;
						remote->available = true;
					}
					else
					{
						uri_errno = errno;
						/* ENOTSUP means this provider has no object tier.  A
						 * local-only layer can finish immediately; a layer whose
						 * upload was durably recorded must retain its tombstone
						 * until the remote URI is available again. */
						if (!gc_remote_candidate.remote_durable &&
							(ps_layer_store->remote_uri == NULL || uri_errno == ENOTSUP))
							return gc_finish_local(gc_remote_candidate.layer_id, 0);
						return 0;
					}
				}
				__atomic_store_n(&gc_remote_state, 1, __ATOMIC_RELEASE);
				if (pthread_create(&gc_remote_thread, NULL, gc_remote_worker,
								   &gc_remote_candidate) != 0)
				{
					__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
					return 0;
				}
				return 1;
			}
		}
	}
	ps_unlock_map();
	return 0;
}

/*
 * Merge all of a timeline's image layers into one fresh layer (bounding the
 * layer count and the per-read layer scan), then GC the merged-away layers.
 * Install-new-before-delete-old: the new layer is written and recorded durably
 * before any old layer is marked for deletion, so a crash at any point leaves
 * the data readable and GC resumable.  Each version lives in exactly one
 * source layer; page-history pruning keeps the newest version below the
 * effective floor plus every version at or above it.
 */
typedef struct CompactOrder
{
	PsKey		key;
	uint32_t	block;
	PsPruneVersion version;
	uint32_t	source;
} CompactOrder;

static int
compact_order_cmp(const void *va, const void *vb)
{
	const CompactOrder *a = va;
	const CompactOrder *b = vb;

#define CMP_KEY_FIELD(field) \
	if (a->key.field != b->key.field) \
		return a->key.field < b->key.field ? -1 : 1
	CMP_KEY_FIELD(spcOid);
	CMP_KEY_FIELD(dbOid);
	CMP_KEY_FIELD(relNumber);
	CMP_KEY_FIELD(forkNum);
	CMP_KEY_FIELD(klass);
#undef CMP_KEY_FIELD
	if (a->block != b->block)
		return a->block < b->block ? -1 : 1;
	if (a->version.lsn != b->version.lsn)
		return a->version.lsn < b->version.lsn ? -1 : 1;
	if (a->version.admission_seq != b->version.admission_seq)
		return a->version.admission_seq < b->version.admission_seq ? -1 : 1;
	return a->source < b->source ? -1 : (a->source > b->source ? 1 : 0);
}

static int
compact_same_page(const CompactOrder *a, const CompactOrder *b)
{
	return a->block == b->block &&
		a->key.spcOid == b->key.spcOid && a->key.dbOid == b->key.dbOid &&
		a->key.relNumber == b->key.relNumber &&
		a->key.forkNum == b->key.forkNum && a->key.klass == b->key.klass;
}

static int
prune_compaction_records(uint32_t timeline, PsImgRec *recs, uint32_t *nrec,
						 uint64_t floor,
						 PsImgRec **dropped_out, uint32_t *ndropped_out)
{
	CompactOrder *order;
	PsPruneVersion *versions;
	unsigned char *keep;
	PsImgRec   *selected;
	PsImgRec   *dropped;
	uint32_t	out = 0;
	uint32_t	ndropped = 0;
	PsPruneFence *fences = NULL;
	uint32_t	nfences = 0;

	if (page_prune_fences(timeline, &fences, &nfences) != 0)
		return -1;

	order = malloc((size_t) *nrec * sizeof(*order));
	versions = malloc((size_t) *nrec * sizeof(*versions));
	keep = malloc(*nrec);
	selected = malloc((size_t) *nrec * sizeof(*selected));
	dropped = malloc((size_t) *nrec * sizeof(*dropped));
	if (!order || !versions || !keep || !selected || !dropped)
	{
		free(order);
		free(versions);
		free(keep);
		free(selected);
		free(dropped);
		free(fences);
		return -1;
	}
	for (uint32_t i = 0; i < *nrec; i++)
	{
		order[i].key = recs[i].key;
		order[i].block = recs[i].block;
		order[i].version.lsn = recs[i].lsn;
		order[i].version.admission_seq = recs[i].admission_seq;
		order[i].source = i;
	}
	qsort(order, *nrec, sizeof(*order), compact_order_cmp);
	for (uint32_t first = 0; first < *nrec;)
	{
		uint32_t end = first + 1;

		while (end < *nrec && compact_same_page(&order[first], &order[end]))
			end++;
		/* pg_control images and their same-version floor notes are the durable
		 * authority from which WAL retention is computed.  A page-history floor
		 * cannot safely prune that authority (doing so would be circular); WAL GC
		 * will eventually retire control checkpoints under its own proof. */
		if (order[first].key.klass == PS_KLASS_CONTROL)
		{
			for (uint32_t i = first; i < end; i++)
				selected[out++] = recs[order[i].source];
			first = end;
			continue;
		}
		for (uint32_t i = first; i < end; i++)
			versions[i - first] = order[i].version;
		if (floor == 0)
			memset(keep, 1, end - first);
		else if (ps_page_prune_plan(versions, end - first,
								(PsPruneFence) {floor, UINT64_MAX}, fences,
									 nfences, keep) < 0)
		{
			free(order);
			free(versions);
			free(keep);
			free(selected);
			free(dropped);
			free(fences);
			return -1;
		}
		for (uint32_t i = first; i < end;)
		{
			uint32_t next = i + 1;
			int kept_source = -1;

			while (next < end &&
				   order[next].version.lsn == order[i].version.lsn &&
				   order[next].version.admission_seq ==
				   order[i].version.admission_seq)
				next++;
			for (uint32_t j = i; j < next; j++)
				if (keep[j - first])
				{
					kept_source = (int) order[j].source;
					break;
				}
			/* Sorted equal identities are one logical version.  Retain one
			 * physical copy, or remove the identity from page_idx exactly once. */
			if (kept_source >= 0)
				selected[out++] = recs[kept_source];
			else
				dropped[ndropped++] = recs[order[i].source];
			i = next;
		}
		first = end;
	}
	memcpy(recs, selected, (size_t) out * sizeof(*recs));
	free(order);
	free(versions);
	free(keep);
	free(selected);
	free(fences);
	*nrec = out;
	*dropped_out = dropped;
	*ndropped_out = ndropped;
	return 0;
}

static int
compact_timeline(uint32_t timeline, uint32_t shard, uint64_t page_floor)
{
	PsLayerDesc *old;
	uint32_t	nold = count_image_layers(timeline, shard);
	PsImgRec   *recs = NULL;
	unsigned char **pages = NULL;
	uint32_t	nrec = 0,
				cap = 0,
				npages = 0;
	uint64_t	nid;
	PsLayerDesc newdesc;
	PsLayerLocation remote;
	PsImgRec   *dropped = NULL;
	uint32_t	ndropped = 0;
	uint64_t	frontier_seq;
	int			rc = -1;

	if (nold == 0)
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
	/* A read snapshots and pins the complete timeline layer set before doing
	 * remote I/O.  Do not publish a partial compaction while any source is
	 * pinned: that leaves the source count above the threshold and makes idle
	 * maintenance repeatedly create larger overlapping replacements. */
	for (uint32_t k = 0; k < nold; k++)
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == old[k].layer_id &&
				__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
								__ATOMIC_ACQUIRE) != 0)
			{
				free(old);
				return 0;
			}

	/* gather every version (page bytes) from the old layers */
	for (uint32_t k = 0; k < nold; k++)
	{
		PsImgIndexEnt *idx;
		uint32_t	n;

		if (read_image_index_refreshing(&old[k], &idx, &n) != 0)
			goto cleanup;
		if (verify_image_layer_refreshing(&old[k]) != 0)
		{
			free(idx);
			goto cleanup;
		}
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
			if (!pg || read_layer_block_refreshing(&old[k], idx[j].data_off,
												   pg, page_size) != 0)
			{
				free(pg);
				free(idx);
				goto cleanup;
			}
			recs[nrec].key = idx[j].key;
			recs[nrec].block = idx[j].block;
			recs[nrec].lsn = idx[j].lsn;
			recs[nrec].admission_seq = idx[j].admission_seq;
			recs[nrec].page = pg;
			recs[nrec].growth_lsn = idx[j].growth_lsn;
			recs[nrec].order_id = idx[j].order_id;
			recs[nrec].seg_off = idx[j].seg_off;
			recs[nrec].seg_id = idx[j].seg_id;
			recs[nrec].flags = idx[j].flags;
			pages[nrec] = pg;
			nrec++;
			npages = nrec;
		}
		free(idx);
	}
	if (nrec == 0)
		goto cleanup;
	if (prune_compaction_records(timeline, recs, &nrec, page_floor,
								 &dropped, &ndropped) != 0 || nrec == 0)
		goto cleanup;
	frontier_seq = __atomic_load_n(&next_admission_seq, __ATOMIC_ACQUIRE);
	if (frontier_seq != 0)
		frontier_seq--;

	/* install the new merged layer durably, THEN delete the old ones */
	nid = alloc_layer_id(&g_shards[shard]);
	if (ps_image_layer_write(nid, timeline, recs, nrec, page_size,
							 &newdesc) != 0)
		goto cleanup;
	for (uint32_t k = 0; k < nold; k++)
		if (old[k].legacy_shard_zero)
			newdesc.legacy_shard_zero = true;
	/* A remote-durable source may be the only copy surviving loss of the local
	 * store.  Publish and verify the replacement in that same durability tier
	 * before its ADD can make any source eligible for deletion. */
	for (uint32_t k = 0; k < nold; k++)
		if (old[k].remote_durable)
		{
			const PsLayerLocation *local = tier_local_location(&newdesc);

			if (local == NULL || ps_layer_store->upload_layer == NULL ||
				ps_layer_store->remote_uri == NULL ||
				newdesc.location_count >= PS_LAYER_MAX_LOCATIONS)
			{
				(void) ps_layer_store->delete_local_layer(&newdesc);
				goto cleanup;
			}
			if (ps_layer_store->upload_layer(&newdesc) != 0)
			{
				(void) ps_layer_store->delete_local_layer(&newdesc);
				goto cleanup;
			}
			memset(&remote, 0, sizeof(remote));
			remote.tier = PS_LAYER_TIER_REMOTE_OBJECT;
			remote.size = local->size;
			remote.available = true;
			if (ps_layer_store->remote_uri(newdesc.layer_id, remote.uri,
										 sizeof(remote.uri)) != 0)
			{
				(void) ps_layer_store->delete_local_layer(&newdesc);
				goto cleanup;
			}
			newdesc.locations[newdesc.location_count++] = remote;
			newdesc.remote_durable = true;
			newdesc.remote_uploaded_lsn = newdesc.lsn_end;
			break;
		}
	/* Reject later pins/branches below this cutoff before the pruned layer can
	 * become durable and visible.  Advancing conservatively when publication
	 * later fails is safe; admitting already-reclaimed history is not. */
	if ((ndropped != 0 &&
		 page_frontier_advance(timeline, page_floor, frontier_seq) != 0) ||
		record_layer(NULL, &newdesc) != 0)
	{
		(void) ps_layer_store->delete_local_layer(&newdesc);
		goto cleanup;
	}
	/* The durable replacement no longer contains these versions.  Drop their
	 * in-memory index entries at the same publication point; otherwise a live
	 * read can select a pruned PageVer and then fail because no layer can serve
	 * the advertised bytes.  Recovery already derives the same index from the
	 * surviving layer set. */
	page_remove_compacted_versions(timeline, dropped, ndropped);
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
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == old[k].layer_id &&
				__atomic_load_n(&ps_layer_map.layers[i].cache_readers,
							 __ATOMIC_ACQUIRE) != 0)
				goto next_old;
		if (ps_manifest_mark_delete(old[k].layer_id) != 0)
			goto cleanup;		/* incomplete: old layers stay live, count not cut */
		if (ps_layer_store->delete_local_layer(&old[k]) != 0)
			continue;			/* still "deleting"; gc_resume() will retry */
		/*
		 * Remote object deletion may block on an object mount.  Keep the
		 * durable deleting record and let the idle maintenance path run
		 * gc_resume() after releasing the shard/map write locks.
		 */
		if (tier_remote_location(&old[k]) != NULL ||
			(__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) != 0 &&
			 tier_upload_candidate.layer_id == old[k].layer_id))
			continue;
		if (ps_manifest_remove_layer(old[k].layer_id) != 0)
			goto cleanup;		/* incomplete */
	next_old:
		;
	}
	rc = 1;

cleanup:
	for (uint32_t j = 0; j < npages; j++)
		free(pages[j]);
	free(recs);
	free(pages);
	free(dropped);
	free(old);
	return rc;
}

static int
materialize_compaction_inputs(uint32_t timeline, uint32_t shard)
{
	PsLayerDesc *layers = NULL;
	uint32_t	nlayers = 0;
	int			rc = -1;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting &&
			d->timeline == timeline && layer_shard_from_id(d->layer_id) == shard)
		{
			PsLayerDesc *nlayers_ptr;

			nlayers_ptr = realloc(layers, (size_t) (nlayers + 1) * sizeof(*layers));
			if (nlayers_ptr == NULL)
			{
				ps_unlock_map();
				goto out;
			}
			layers = nlayers_ptr;
			layers[nlayers++] = *d;
			__atomic_add_fetch(&d->cache_readers, 1, __ATOMIC_ACQ_REL);
		}
	}
	ps_unlock_map();
	if (nlayers == 0)
	{
		free(layers);
		return 0;
	}

	for (uint32_t i = 0; i < nlayers; i++)
	{
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;

		if (read_image_index_refreshing(&layers[i], &idx, &nidx) != 0 ||
			verify_image_layer_refreshing(&layers[i]) != 0)
		{
			free(idx);
			goto out;
		}
		free(idx);
	}
	rc = 0;

out:
	ps_lock_map_wr();
	for (uint32_t i = 0; i < nlayers; i++)
		for (uint32_t j = 0; j < ps_layer_map.nlayers; j++)
			if (ps_layer_map.layers[j].layer_id == layers[i].layer_id)
			{
				__atomic_sub_fetch(&ps_layer_map.layers[j].cache_readers, 1,
								   __ATOMIC_ACQ_REL);
				if (layers[i].data_verified)
					ps_layer_map.layers[j].data_verified = true;
				if (tier_local_location(&ps_layer_map.layers[j]) == NULL &&
					ps_layer_store->layer_exists_local != NULL &&
					ps_layer_store->layer_exists_local(layers[i].layer_id) == 1)
					ps_layer_map.layers[j].cache_resident = true;
				break;
			}
	ps_unlock_map();
	free(layers);
	return rc;
}

/* ===================== segment storage (log-structured) ================= */

#define SEG_MAGIC		 0x53454732 /* "SEG2": v2 record (PsKey gained klass) */
#define SEG_WALLESS_MAGIC 0x53454730 /* "SEG0": zero-version record + growth floor */
#define SEG_WALLESS_ORDERED_MAGIC 0x53454731 /* "SEG1": SEG0 + required order marker */
#define SEG_CLAMPED_ORDERED_MAGIC 0x53454733 /* "SEG3": clamped version + marker */
#define SEG_WALLESS_BOUND_MAGIC 0x53454734 /* "SEG4": SEG1 + marker identity */
#define SEG_CLAMPED_BOUND_MAGIC 0x53454735 /* "SEG5": SEG3 + marker identity */
#define SEG_ADMISSION_MAGIC 0x53454736 /* "SEG6": SEG2 + admission sequence */
#define SEG_WALLESS_ADMISSION_MAGIC 0x53454737 /* "SEG7": SEG4 + admission */
#define SEG_CLAMPED_ADMISSION_MAGIC 0x53454738 /* "SEG8": SEG5 + admission */

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
	uint64_t	lsn;			/* version LSN, or WAL-less fork-growth floor */
	uint32_t	len;			/* page bytes following the header */
} SegRecHdr;

typedef struct SegRecHdrBound
{
	SegRecHdr	hdr;
	uint64_t	order_id;
} SegRecHdrBound;

typedef struct SegRecHdrAdmission
{
	SegRecHdr	hdr;
	uint64_t	admission_seq;
} SegRecHdrAdmission;

typedef struct SegRecHdrBoundAdmission
{
	SegRecHdr	hdr;
	uint64_t	order_id;
	uint64_t	admission_seq;
} SegRecHdrBoundAdmission;

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
	struct PageEnt *fork_next;	/* pages belonging to the same fork */
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
	uint64_t	admission_seq;	/* global mutation order; 0 = legacy */
	uint64_t	order_id;		/* bound segment marker identity, else zero */
	uint32_t	nblocks;
	uint32_t	cached_nblocks;	/* hop result through this sorted event */
	uint32_t	cached_fence_nblocks; /* smallest inherited block boundary */
	uint8_t		kind;
	uint8_t		cached_state;
} ForkEvent;

#define FEV_GROW	0
#define FEV_SET		1
#define FEV_DEAD	2
#define FEV_MIGRATED 3			/* log marker: legacy lsn-0 migration completed */
#define FEV_MIGRATING 4			/* log marker: legacy migration started */
#define FEV_SEG_GROW 5			/* ordering placeholder, activated by segment replay */
#define FEV_SEG_COMMIT 6		/* ordered segment commit that does not change size */
#define FEV_SEG_GROW_BOUND 7	/* FEV_SEG_GROW paired with a segment identity */
#define FEV_SEG_COMMIT_BOUND 8 /* FEV_SEG_COMMIT paired with a segment identity */
#define FEV_SEG_ID 9			/* second record carrying a bound marker's identity */

typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	nblocks;		/* newest size (cache of the event history) */
	ForkEvent  *ev;				/* lsn-ordered size history */
	uint32_t	nev;
	uint32_t	evcap;
	uint32_t   *def_idx;		/* indexes of SET/DEAD events only */
	uint32_t	ndef;
	uint32_t	defcap;
	PageEnt    *pages;			/* local pages belonging to this fork */
	uint64_t	last_def_lsn;	/* newest SET/DEAD lsn (growth-clamp floor) */
	uint64_t	last_page_lsn;	/* newest durable local page tuple */
	uint64_t	last_page_seq;
	int			has_wal_less;	/* at least one page version has lsn 0 */
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
/* Retention changes can make projected ancestor history reclaimable even when
 * no new layer arrives.  Maintenance rewrites every marked nonempty shard and
 * clears its mark only after publishing at the new effective floor. */
static unsigned char page_prune_due[MAX_TIMELINES][PS_MAX_CHANNELS];

static void
page_prune_mark_all_due(void)
{
	uint32_t	ns = core_shards();

	ps_lock_map_rd();
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined)
			for (uint32_t sh = 0; sh < ns; sh++)
				__atomic_store_n(&page_prune_due[tl][sh], 1, __ATOMIC_RELEASE);
	ps_unlock_map();
}

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
static uint64_t wal_covered[MAX_TIMELINES];
static uint64_t wal_covered_off[MAX_TIMELINES];
static int		wal_covered_valid[MAX_TIMELINES];

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

static uint32_t
page_frontier_crc(const PsPageFrontierState *state)
{
	return fnv(state, offsetof(PsPageFrontierState, crc));
}

static int
page_frontier_publish(void)
{
	PsPageFrontierState state;
	char		tmp[4096];
	int			fd = -1;
	int			n;
	int			rc = -1;

	memset(&state, 0, sizeof(state));
	state.magic = PS_PAGE_FRONTIER_MAGIC;
	state.version = PS_PAGE_FRONTIER_VERSION;
	memcpy(state.frontiers, page_reclaimed_frontier,
		   sizeof(state.frontiers));
	state.crc = page_frontier_crc(&state);
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", page_frontier_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
		fsync(fd) != 0)
		goto done;
	if (close(fd) != 0)
	{
		fd = -1;
		goto done;
	}
	fd = -1;
	if (rename(tmp, page_frontier_path) != 0 ||
		fsync_dir_path(page_frontier_dir) != 0)
		goto done;
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		unlink(tmp);
	return rc;
}

static int
page_frontier_load(const char *store_dir)
{
	PsPageFrontierState state;
	unsigned char extra;
	int			fd;
	int			n;

	memset(page_reclaimed_frontier, 0, sizeof(page_reclaimed_frontier));
	n = snprintf(page_frontier_dir, sizeof(page_frontier_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(page_frontier_dir))
		return -1;
	n = snprintf(page_frontier_path, sizeof(page_frontier_path),
				 "%s/page-prune.frontiers", store_dir);
	if (n < 0 || (size_t) n >= sizeof(page_frontier_path))
		return -1;
	fd = open(page_frontier_path, O_RDONLY);
	if (fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (read(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
		read(fd, &extra, 1) != 0 ||
		state.magic != PS_PAGE_FRONTIER_MAGIC ||
		state.version != PS_PAGE_FRONTIER_VERSION ||
		state.crc != page_frontier_crc(&state))
	{
		close(fd);
		errno = EILSEQ;
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	memcpy(page_reclaimed_frontier, state.frontiers,
		   sizeof(page_reclaimed_frontier));
	return 0;
}

static int
page_frontier_advance(uint32_t timeline, uint64_t floor,
					  uint64_t admission_seq)
{
	PsPruneFence old;
	PsPruneFence next;

	if (timeline >= MAX_TIMELINES || floor == 0)
		return -1;
	old = page_reclaimed_frontier[timeline];
	next.lsn = floor;
	next.admission_seq = admission_seq;
	if (next.lsn < old.lsn ||
		(next.lsn == old.lsn && next.admission_seq <= old.admission_seq))
		return 0;
	page_reclaimed_frontier[timeline] = next;
	if (page_frontier_publish() != 0)
	{
		page_reclaimed_frontier[timeline] = old;
		return -1;
	}
	return 0;
}

static int
page_frontier_allows(uint32_t timeline, uint64_t lsn, uint64_t admission_seq)
{
	PsPruneFence frontier;

	if (timeline >= MAX_TIMELINES)
		return 0;
	frontier = page_reclaimed_frontier[timeline];
	if (lsn < frontier.lsn)
		return 0;
	/* Sequence zero is the established uncapped/latest-visible fence. */
	if (admission_seq != 0 &&
		lsn == frontier.lsn &&
		admission_seq < frontier.admission_seq)
		return 0;
	return 1;
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

static ForkEnt *fork_get_or_create(uint32_t timeline, const PsKey *key);
static ForkEnt *fork_find(uint32_t timeline, const PsKey *key);

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

/* Forget versions omitted from a durably published compacted layer.  The
 * caller holds this shard's write lock and map write lock, so page chains
 * cannot change while the replacement layer and index become consistent. */
static void
page_remove_compacted_versions(uint32_t timeline, const PsImgRec *recs,
							   uint32_t nrec)
{
	for (uint32_t r = 0; r < nrec;)
	{
		uint32_t	end = r + 1;
		PageEnt    *e = page_find(timeline, &recs[r].key, recs[r].block);
		int			out = 0;

		while (end < nrec && recs[end].block == recs[r].block &&
			   key_eq(&recs[end].key, &recs[r].key))
			end++;

		if (e == NULL)
		{
			r = end;
			continue;
		}
		/* dropped identities are sorted by (LSN, admission sequence).  Compact
		 * this page's live version array once; binary lookup avoids repeatedly
		 * shifting a hot page while the shard write lock is held. */
		for (int i = 0; i < e->nver; i++)
		{
			PageVer    *v = &e->vers[i];
			uint32_t	lo = r;
			uint32_t	hi = end;
			int			remove = 0;

			while (lo < hi)
			{
				uint32_t mid = lo + (hi - lo) / 2;

				if (recs[mid].lsn < v->lsn ||
					(recs[mid].lsn == v->lsn &&
					 recs[mid].admission_seq < v->admission_seq))
					lo = mid + 1;
				else
					hi = mid;
			}
			if (lo < end && recs[lo].lsn == v->lsn &&
				recs[lo].admission_seq == v->admission_seq)
				remove = 1;
			if (!remove)
				e->vers[out++] = *v;
		}
		e->nver = out;
		r = end;
	}
	for (uint32_t r = 0; r < nrec; r++)
		if (recs[r].lsn == 0)
		{
			ForkEnt    *fork = fork_find(timeline, &recs[r].key);
			Shard	   *shard = shard_for(&recs[r].key);
			int			found = 0;

			for (uint32_t b = 0; b < IDX_BUCKETS && !found; b++)
				for (PageEnt *e = shard->page_idx[b]; e && !found; e = e->next)
					if (e->timeline == timeline && key_eq(&e->key, &recs[r].key))
						for (int i = 0; i < e->nver; i++)
							if (e->vers[i].lsn == 0)
							{
								found = 1;
								break;
							}
			if (fork != NULL)
				fork->has_wal_less = found;
		}
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
				 uint64_t lsn, uint64_t admission_seq, uint32_t shard,
				 int seg, uint64_t off)
{
	uint32_t	h = page_hash(timeline, key, block);
	Shard	   *s = shard_for(key);
	PageEnt    *e = page_find(timeline, key, block);
	ForkEnt    *fork = fork_get_or_create(timeline, key);

	timeline_mark_used(timeline);
	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->block = block;
		e->fork_next = fork->pages;
		fork->pages = e;
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
	e->vers[e->nver].admission_seq = admission_seq;
	e->vers[e->nver].seg = seg;
	e->vers[e->nver].off = off;
	e->nver++;
	if (lsn > fork->last_page_lsn ||
		(lsn == fork->last_page_lsn && admission_seq > fork->last_page_seq))
	{
		fork->last_page_lsn = lsn;
		fork->last_page_seq = admission_seq;
	}
	if (lsn == 0)
		fork->has_wal_less = 1;
}

/* Newest version on this entry with lsn <= read_lsn, or NULL if none. */
static PageVer *
page_visible(PageEnt *e, uint64_t read_lsn, uint64_t read_seq)
{
	PageVer    *best = NULL;

	for (int i = 0; i < e->nver; i++)
	{
		PageVer    *v = &e->vers[i];

		if (v->lsn <= read_lsn &&
			(v->lsn < read_lsn || read_seq == 0 || v->admission_seq == 0 ||
			 v->admission_seq <= read_seq) &&
			(!best || v->lsn > best->lsn ||
			 (v->lsn == best->lsn &&
			  v->admission_seq >= best->admission_seq)))
			best = v;
	}
	return best;
}

/* --- fork size index (keyed by timeline, key) --- */

static int fork_meta_persist(uint32_t timeline, const PsKey *key, uint64_t lsn,
							 uint64_t admission_seq, uint32_t nblocks,
							 uint8_t kind);

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
fork_asof_hop(const ForkEnt *e, uint64_t cap, uint64_t seq_cap,
			  uint32_t *nb_out)
{
	*nb_out = 0;
	if (seq_cap == 0)
	{
		uint32_t lo = 0;
		uint32_t hi = e->nev;

		while (lo < hi)
		{
			uint32_t mid = lo + (hi - lo) / 2;

			if (e->ev[mid].lsn <= cap)
				lo = mid + 1;
			else
				hi = mid;
		}
		if (lo == 0)
			return FORK_HOP_NONE;
		*nb_out = e->ev[lo - 1].cached_nblocks;
		return e->ev[lo - 1].cached_state;
	}
	{
		uint32_t	first = 0;
		uint32_t	end = e->nev;
		uint8_t		state;
		uint32_t	nb;

		/* Everything below cap is visible regardless of admission sequence.
		 * Reuse its cached fold, then inspect only the equal-cap run. */
		while (first < end)
		{
			uint32_t mid = first + (end - first) / 2;

			if (e->ev[mid].lsn < cap)
				first = mid + 1;
			else
				end = mid;
		}
		state = first == 0 ? FORK_HOP_NONE : e->ev[first - 1].cached_state;
		nb = first == 0 ? 0 : e->ev[first - 1].cached_nblocks;
		for (uint32_t i = first; i < e->nev && e->ev[i].lsn == cap; i++)
		{
			const ForkEvent *v = &e->ev[i];

			if (v->admission_seq != 0 && v->admission_seq > seq_cap)
				continue;
			if (v->kind == FEV_GROW)
			{
				if (v->nblocks > nb)
					nb = v->nblocks;
				state = (state == FORK_HOP_NONE || state == FORK_HOP_GROW) ?
					FORK_HOP_GROW : FORK_HOP_DEF;
			}
			else if (v->kind == FEV_SET)
			{
				nb = v->nblocks;
				state = FORK_HOP_DEF;
			}
			else if (v->kind == FEV_DEAD)
			{
				nb = 0;
				state = FORK_HOP_DEAD;
			}
		}
		*nb_out = nb;
		return state;
	}
}

static void
fork_event_cache_from(ForkEnt *e, uint32_t start)
{
	uint8_t state = start == 0 ? FORK_HOP_NONE : e->ev[start - 1].cached_state;
	uint32_t nb = start == 0 ? 0 : e->ev[start - 1].cached_nblocks;
	uint32_t fence = start == 0 ? UINT32_MAX :
		e->ev[start - 1].cached_fence_nblocks;

	for (uint32_t i = start; i < e->nev; i++)
	{
		ForkEvent *v = &e->ev[i];

		if (v->kind == FEV_GROW)
		{
			if (v->nblocks > nb)
				nb = v->nblocks;
			state = (state == FORK_HOP_NONE || state == FORK_HOP_GROW) ?
				FORK_HOP_GROW : FORK_HOP_DEF;
		}
		else if (v->kind == FEV_SET)
		{
			nb = v->nblocks;
			state = FORK_HOP_DEF;
			if (v->nblocks < fence)
				fence = v->nblocks;
		}
		else if (v->kind == FEV_DEAD)
		{
			nb = 0;
			state = FORK_HOP_DEAD;
			fence = 0;
		}
		v->cached_nblocks = nb;
		v->cached_state = state;
		v->cached_fence_nblocks = fence;
	}
}

/* Keep a compact index over definitive lifecycle events.  Inserts can shift
 * existing event offsets, but their cost is proportional to the number of
 * truncates/unlinks rather than the usually much larger number of GROWs. */
static void
fork_def_index_insert(ForkEnt *e, uint32_t event_idx, int definitive)
{
	uint32_t	pos = 0;

	while (pos < e->ndef && e->def_idx[pos] < event_idx)
		pos++;
	for (uint32_t i = pos; i < e->ndef; i++)
		e->def_idx[i]++;
	if (!definitive)
		return;
	if (e->ndef == e->defcap)
	{
		e->defcap = e->defcap ? e->defcap * 2 : 4;
		e->def_idx = realloc(e->def_idx,
			(size_t) e->defcap * sizeof(*e->def_idx));
	}
	memmove(&e->def_idx[pos + 1], &e->def_idx[pos],
		(size_t) (e->ndef - pos) * sizeof(*e->def_idx));
	e->def_idx[pos] = event_idx;
	e->ndef++;
}

static void
fork_def_index_remove_nondef(ForkEnt *e, uint32_t event_idx)
{
	for (uint32_t i = 0; i < e->ndef; i++)
		if (e->def_idx[i] > event_idx)
			e->def_idx[i]--;
}

/* Size of e as of cap, hop-local (for the GROW-dedup below). */
static uint32_t
fork_size_asof_hop(const ForkEnt *e, uint64_t cap, uint64_t seq_cap)
{
	uint32_t	nb;

	(void) fork_asof_hop(e, cap, seq_cap, &nb);
	return nb;
}

/* A later truncate/drop invalidates old page bytes even if subsequent growth
 * makes the block addressable again.  The current fast path avoids touching
 * event history unless a definitive event is newer than the selected page. */
static int
fork_page_invalidated(const ForkEnt *e, uint32_t block, const PageVer *page,
					  uint64_t cap, uint64_t seq_cap)
{
	if (e == NULL || page == NULL || e->last_def_lsn < page->lsn)
		return 0;
	for (int i = (int) e->ndef - 1; i >= 0; i--)
	{
		const ForkEvent *v = &e->ev[e->def_idx[i]];

		if (v->lsn > cap ||
			(seq_cap != 0 && v->lsn == cap && v->admission_seq != 0 &&
			 v->admission_seq > seq_cap) ||
			(v->kind != FEV_SET && v->kind != FEV_DEAD))
			continue;
		/* Nonzero LSN is the primary order, including across legacy and
		 * sequenced records.  WAL-less records have no LSN order, so retain
		 * their established admission-order semantics. */
		if (v->lsn != 0 && page->lsn != 0)
		{
			if (v->lsn < page->lsn)
				break;
			if (v->lsn == page->lsn &&
				v->admission_seq <= page->admission_seq)
				continue;
		}
		else if (v->admission_seq <= page->admission_seq)
			continue;
		if (v->kind == FEV_DEAD || block >= v->nblocks)
			return 1;
	}
	return 0;
}

static int
fork_inheritance_fenced(const ForkEnt *e, uint32_t block,
						uint64_t cap, uint64_t seq_cap)
{
	if (e == NULL)
		return 0;
	if (seq_cap == 0)
	{
		uint32_t lo = 0;
		uint32_t hi = e->nev;

		while (lo < hi)
		{
			uint32_t mid = lo + (hi - lo) / 2;

			if (e->ev[mid].lsn <= cap)
				lo = mid + 1;
			else
				hi = mid;
		}
		return lo != 0 && e->ev[lo - 1].cached_fence_nblocks != UINT32_MAX &&
			block >= e->ev[lo - 1].cached_fence_nblocks;
	}
	{
		uint32_t first = 0;
		uint32_t end = e->nev;
		uint32_t fence;

		while (first < end)
		{
			uint32_t mid = first + (end - first) / 2;

			if (e->ev[mid].lsn < cap)
				first = mid + 1;
			else
				end = mid;
		}
		fence = first == 0 ? UINT32_MAX :
			e->ev[first - 1].cached_fence_nblocks;
		for (uint32_t i = first; i < e->nev && e->ev[i].lsn == cap; i++)
		{
			const ForkEvent *v = &e->ev[i];

			if (v->admission_seq != 0 && v->admission_seq > seq_cap)
				continue;
			if (v->kind == FEV_DEAD)
				fence = 0;
			else if (v->kind == FEV_SET && v->nblocks < fence)
				fence = v->nblocks;
		}
		return fence != UINT32_MAX && block >= fence;
	}
}

/* Markerless SEG0 spans an intermediate format transition: some stores already
 * persisted the same growth in forkmeta, while later ones relied on SEG0 alone.
 * Detect the former without re-evaluating equal-LSN definitive-event order. */
static int
fork_has_growth_at(const ForkEnt *e, uint64_t lsn, uint32_t nblocks)
{
	for (uint32_t i = 0; i < e->nev; i++)
		if (e->ev[i].kind == FEV_GROW && e->ev[i].lsn == lsn &&
			e->ev[i].nblocks >= nblocks)
			return 1;
	return 0;
}

/* A retry can follow page growth at the CREATE's LSN.  The last lifecycle
 * boundary, not the last event, determines whether that CREATE already made
 * an empty generation. */
static int
fork_has_create_at(const ForkEnt *e, uint64_t lsn)
{
	const ForkEvent *last_def = NULL;

	for (uint32_t i = 0; i < e->nev; i++)
		if (e->ev[i].lsn == lsn &&
			(e->ev[i].kind == FEV_SET || e->ev[i].kind == FEV_DEAD))
			last_def = &e->ev[i];
	return last_def != NULL && last_def->kind == FEV_SET &&
		last_def->nblocks == 0;
}

/*
 * Record a fork-size event, keeping the history lsn-ordered (equal LSNs keep
 * arrival order, so a later definitive event at the same LSN wins a
 * newest-first scan).  GROW events that do not raise the size visible at
 * their own LSN are dropped: steady-state rewrites of existing blocks at ever
 * newer pd_lsns add nothing, so the history stays O(distinct sizes).
*/
static _Thread_local int fork_event_cache_defer = 0;

static void
fork_event_add(ForkEnt *e, uint64_t lsn, uint64_t admission_seq,
			   uint32_t nblocks, uint8_t kind)
{
	uint32_t	i;

	if (!fork_event_cache_defer && kind == FEV_GROW &&
		fork_size_asof_hop(e, lsn, admission_seq) >= nblocks)
		return;
	if (kind != FEV_GROW && lsn > e->last_def_lsn)
		e->last_def_lsn = lsn;
	if (e->nev == e->evcap)
	{
		e->evcap = e->evcap ? e->evcap * 2 : 4;
		e->ev = realloc(e->ev, e->evcap * sizeof(ForkEvent));
	}
	i = e->nev;
	while (i > 0 &&
		   (e->ev[i - 1].lsn > lsn ||
			(e->ev[i - 1].lsn == lsn && admission_seq != 0 &&
			 e->ev[i - 1].admission_seq != 0 &&
			 e->ev[i - 1].admission_seq > admission_seq)))
	{
		e->ev[i] = e->ev[i - 1];
		i--;
	}
	e->ev[i].lsn = lsn;
	e->ev[i].admission_seq = admission_seq;
	e->ev[i].order_id = 0;
	e->ev[i].nblocks = nblocks;
	e->ev[i].kind = kind;
	e->nev++;
	fork_def_index_insert(e, i, kind == FEV_SET || kind == FEV_DEAD);
	if (fork_event_cache_defer)
		return;
	fork_event_cache_from(e, i);

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
		e->nblocks = fork_size_asof_hop(e, UINT64_MAX, 0);
}

/*
 * Preserve a segment growth's position among equal-LSN fork-meta events without
 * making the marker itself a size event.  Recovery activates the placeholder
 * only after validating the matching segment header and complete page body.
 */
static void
fork_event_add_seg_marker(ForkEnt *e, uint64_t lsn, uint32_t nblocks,
						  uint8_t kind, uint64_t order_id,
						  uint64_t admission_seq)
{
	uint32_t	i;

	if (e->nev == e->evcap)
	{
		e->evcap = e->evcap ? e->evcap * 2 : 4;
		e->ev = realloc(e->ev, e->evcap * sizeof(ForkEvent));
	}
	i = e->nev;
	while (i > 0 &&
		   (e->ev[i - 1].lsn > lsn ||
			(e->ev[i - 1].lsn == lsn && admission_seq != 0 &&
			 e->ev[i - 1].admission_seq != 0 &&
			 e->ev[i - 1].admission_seq > admission_seq)))
	{
		e->ev[i] = e->ev[i - 1];
		i--;
	}
	e->ev[i].lsn = lsn;
	e->ev[i].admission_seq = admission_seq;
	e->ev[i].order_id = order_id;
	e->ev[i].nblocks = nblocks;
	e->ev[i].kind = kind;
	e->nev++;
	fork_def_index_insert(e, i, 0);
	fork_event_cache_from(e, i);
}

static int
fork_event_activate_seg(ForkEnt *e, uint64_t lsn, uint32_t nblocks,
						uint64_t order_id, uint64_t admission_seq)
{
	for (uint32_t i = 0; i < e->nev; i++)
	{
		ForkEvent  *v = &e->ev[i];

		if ((v->kind == FEV_SEG_GROW || v->kind == FEV_SEG_COMMIT ||
			 v->kind == FEV_SEG_GROW_BOUND ||
			 v->kind == FEV_SEG_COMMIT_BOUND) &&
			v->lsn == lsn &&
			v->nblocks == nblocks && v->order_id == order_id &&
			v->admission_seq == admission_seq)
		{
			if (v->kind == FEV_SEG_GROW || v->kind == FEV_SEG_GROW_BOUND)
			{
				v->kind = FEV_GROW;
				fork_event_cache_from(e, i);
				e->nblocks = fork_size_asof_hop(e, UINT64_MAX, 0);
			}
			else
			{
				memmove(v, v + 1, (e->nev - i - 1) * sizeof(*v));
				e->nev--;
				fork_def_index_remove_nondef(e, i);
				fork_event_cache_from(e, i);
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
	uint64_t	admission_seq = admission_seq_alloc();

	if (admission_seq == 0)
		return -1;

	/*
	 * Zeroextend has no page record from which recovery can reconstruct its
	 * size.  Clamp first, then persist exactly the event applied in memory.
	 */
	if (lsn < e->last_def_lsn || lsn == 0)
		lsn = e->last_def_lsn;
	if (fork_size_asof_hop(e, lsn, admission_seq) < to_nblocks &&
		fork_meta_persist(timeline, key, lsn, admission_seq, to_nblocks,
						  FEV_GROW) != 0)
		return -1;			/* not durable: do not apply in memory */
	fork_event_add(e, lsn, admission_seq, to_nblocks, FEV_GROW);
	return 0;
}

/* Apply growth whose durability is already represented by metadata/segment. */
static void
fork_grow_apply(uint32_t timeline, const PsKey *key, uint32_t to_nblocks,
				uint64_t lsn, uint64_t admission_seq)
{
	fork_event_add(fork_get_or_create(timeline, key), lsn, admission_seq, to_nblocks,
				   FEV_GROW);
}

static int
fork_event_precedes_known_state(const ForkEnt *e, uint64_t lsn,
							uint64_t admission_seq)
{
	const ForkEvent *tail;

	if (e == NULL || e->nev == 0)
		return 0;
	tail = &e->ev[e->nev - 1];
	return tail->lsn > lsn ||
		(tail->lsn == lsn && tail->admission_seq > admission_seq) ||
		e->last_page_lsn > lsn ||
		(e->last_page_lsn == lsn && e->last_page_seq > admission_seq);
}

/* A definitive event can arrive after pages whose WAL positions are newer.
 * Those page records did not need GROW events when admitted, but the delayed
 * truncate/drop can make them growth retroactively.  Reconstruct the transient
 * events now; segment recovery derives the same events durably after restart. */
typedef struct DeferredGrow
{
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint32_t	nblocks;
} DeferredGrow;

static int
fork_deferred_grow_cmp(const void *left, const void *right)
{
	const DeferredGrow *a = left;
	const DeferredGrow *b = right;

	if (a->lsn != b->lsn)
		return a->lsn < b->lsn ? -1 : 1;
	if (a->admission_seq != b->admission_seq)
		return a->admission_seq < b->admission_seq ? -1 : 1;
	return 0;
}

static void
fork_restore_later_page_growth(uint32_t timeline, const PsKey *key,
								   uint64_t lsn, uint64_t admission_seq)
{
	ForkEnt    *e = fork_get_or_create(timeline, key);
	DeferredGrow *grows = NULL;
	uint32_t	ngrows = 0;
	uint32_t	cap = 0;

	/* The per-fork page chain is block-descending.  Collect its later page
	 * versions first so a delayed lifecycle event does not rebuild the suffix
	 * once per block while holding the shard lock. */

	for (PageEnt *page = e->pages; page; page = page->fork_next)
	{
		for (int i = 0; i < page->nver; i++)
		{
			PageVer    *version = &page->vers[i];

			if (version->lsn != 0 &&
				(version->lsn > lsn ||
				 (version->lsn == lsn &&
				  version->admission_seq > admission_seq)))
			{
				if (ngrows == cap)
				{
					cap = cap ? cap * 2 : 16;
					grows = realloc(grows, (size_t) cap * sizeof(*grows));
					if (grows == NULL)
						return;
				}
				grows[ngrows++] = (DeferredGrow)
					{version->lsn, version->admission_seq, page->block + 1};
			}
		}
	}
	/* Sort once and coalesce adjacent equal page-version tuples.  In deferred
	 * mode fork_event_add deliberately bypasses its cache-based deduplication:
	 * the cache is rebuilt only after the entire batch, so it cannot suppress a
	 * necessary later growth using stale values. */
	qsort(grows, ngrows, sizeof(*grows), fork_deferred_grow_cmp);
	{
		uint32_t out = 0;

		for (uint32_t i = 0; i < ngrows; i++)
		{
			if (out != 0 && grows[out - 1].lsn == grows[i].lsn &&
				grows[out - 1].admission_seq == grows[i].admission_seq)
			{
				if (grows[i].nblocks > grows[out - 1].nblocks)
					grows[out - 1].nblocks = grows[i].nblocks;
			}
			else
				grows[out++] = grows[i];
		}
		ngrows = out;
	}
	fork_event_cache_defer++;
	{
		uint32_t existing = 0;

		for (uint32_t i = 0; i < ngrows; i++)
		{
			int present = 0;

			while (existing < e->nev &&
				(e->ev[existing].lsn < grows[i].lsn ||
				 (e->ev[existing].lsn == grows[i].lsn &&
				  e->ev[existing].admission_seq < grows[i].admission_seq)))
				existing++;
			for (uint32_t j = existing; j < e->nev &&
				 e->ev[j].lsn == grows[i].lsn &&
				 e->ev[j].admission_seq == grows[i].admission_seq; j++)
				if (e->ev[j].kind == FEV_GROW &&
					e->ev[j].nblocks >= grows[i].nblocks)
				{
					present = 1;
					break;
				}
			if (!present)
				fork_event_add(e, grows[i].lsn, grows[i].admission_seq,
							   grows[i].nblocks, FEV_GROW);
		}
	}
	fork_event_cache_defer--;
	if (ngrows != 0)
	{
		fork_event_cache_from(e, 0);
		e->nblocks = fork_size_asof_hop(e, UINT64_MAX, 0);
	}
	free(grows);
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
				 uint64_t lsn, uint64_t admission_seq)
{
	fork_event_add(fork_get_or_create(timeline, key), lsn, admission_seq, to_nblocks,
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

int
ps_timeline_defined(uint32_t timeline)
{
	return timeline < MAX_TIMELINES && timelines[timeline].defined;
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

/* A capped relation read cannot prove completeness for WAL-less pages.  Check
 * the whole ancestry before EXISTS/NBLOCKS can turn a hidden LSN-0 version
 * into an apparently valid empty relation. */
static int
fork_has_wal_less_page(uint32_t timeline, const PsKey *key)
{
	TlWalk		w = tl_walk_first(timeline, UINT64_MAX);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e && e->has_wal_less)
			return 1;
	} while (tl_walk_next(&w));
	return 0;
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

/* Project a requested branch horizon through every ancestor cap and reject
 * any ancestor whose durable reclamation frontier has already passed it. */
static int
branch_frontiers_allow(int parent, uint64_t branch_lsn)
{
	uint64_t cap = branch_lsn;

	for (int t = parent; t >= 0 && t < MAX_TIMELINES; t = timelines[t].parent)
	{
		if (!timelines[t].defined ||
			cap < page_reclaimed_frontier[t].lsn)
			return 0;
		if (timelines[t].parent >= 0 && cap > timelines[t].branch_lsn)
			cap = timelines[t].branch_lsn;
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
			 uint64_t read_lsn, uint64_t read_seq)
{
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *fe = fork_find(w.tl, key);
		uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;
		uint32_t	nb = 0;
		int			fork_state = fe ? fork_asof_hop(fe, w.lsn, seq_cap, &nb) :
			FORK_HOP_NONE;
		PageEnt    *e = page_find(w.tl, key, block);
		PageVer    *v = e ? page_visible(e, w.lsn, seq_cap) : NULL;

		if (v)
		{
			if (!fork_page_invalidated(fe, block, v, w.lsn, seq_cap))
				return v;
			return NULL;
		}
		if (fork_state == FORK_HOP_DEAD ||
			(fork_state == FORK_HOP_DEF && block >= nb))
			return NULL;
		if (fork_state == FORK_HOP_DEF &&
			fork_inheritance_fenced(fe, block, w.lsn, seq_cap))
			return NULL;
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
fork_nblocks_through(uint32_t timeline, const PsKey *key, uint64_t read_lsn,
					 uint64_t read_seq)
{
	uint32_t	maxnb = 0;
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e)
		{
			uint32_t	nb;
			uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;
			int			r = fork_asof_hop(e, w.lsn, seq_cap, &nb);

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

/* Redo must reserve every block that can be reached by WAL already accepted
 * into this store.  A later CREATE/TRUNCATE is a logical visibility boundary,
 * not permission to reject an earlier FPI as beyond EOF. */
static uint32_t
fork_nblocks_recovery(uint32_t timeline, const PsKey *key, uint64_t read_lsn)
{
	uint32_t maxnb = 0;
	TlWalk w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt *e = fork_find(w.tl, key);

		if (e != NULL)
			for (uint32_t i = 0; i < e->nev; i++)
				if (e->ev[i].lsn <= w.lsn && e->ev[i].kind != FEV_DEAD &&
					e->ev[i].nblocks > maxnb)
					maxnb = e->ev[i].nblocks;
	} while (tl_walk_next(&w));
	return maxnb;
}

/* Does the fork exist on 'timeline' or any ancestor, as of read_lsn? */
static int
fork_exists_through(uint32_t timeline, const PsKey *key, uint64_t read_lsn,
					uint64_t read_seq)
{
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	do
	{
		ForkEnt    *e = fork_find(w.tl, key);

		if (e)
		{
			uint32_t	nb;
			uint64_t	seq_cap = w.lsn == read_lsn ? read_seq : 0;
			int			r = fork_asof_hop(e, w.lsn, seq_cap, &nb);

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

#define TIMELINE_META_V2_MAGIC 0x324d4c54U /* "TLM2" */
typedef struct TimelineRecV2
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t id;
	int32_t parent;
	uint64_t branch_lsn;
	uint32_t crc;
	uint32_t reserved;
} TimelineRecV2;

static uint32_t
timeline_rec_crc(TimelineRecV2 *rec)
{
	uint32_t save = rec->crc;
	uint32_t crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static int
timeline_persist(uint32_t id, int parent, uint64_t branch_lsn)
{
	TimelineRecV2 rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = TIMELINE_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.id = id;
	rec.parent = (int32_t) parent;
	rec.branch_lsn = branch_lsn;
	rec.crc = timeline_rec_crc(&rec);

	return ps_storage->meta_append(&rec, sizeof(rec));
}

/*
 * Fork-size events the segment log cannot reproduce -- create, truncate,
 * unlink, zero-extend -- are persisted here (the segment records themselves
 * re-derive every page-append GROW on recovery).  V2 records are self-sized so
 * they can coexist with the legacy fixed records in one append-only log.
 */
typedef struct ForkMetaRecV1
{
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} ForkMetaRecV1;

#define FORK_META_V2_MAGIC 0x324d4b46 /* "FKM2" */

typedef struct ForkMetaRecV2
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint64_t	order_id;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} ForkMetaRecV2;

static int
fork_meta_persist(uint32_t timeline, const PsKey *key, uint64_t lsn,
				  uint64_t admission_seq, uint32_t nblocks, uint8_t kind)
{
	ForkMetaRecV2 rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = FORK_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.timeline = timeline;
	rec.key = *key;
	rec.lsn = lsn;
	rec.admission_seq = admission_seq;
	rec.nblocks = nblocks;
	rec.kind = kind;
	return ps_storage->fork_meta_append(&rec, sizeof(rec));
}

/* Persist a bound segment marker and its 64-bit identity in one self-sized
 * append.  The loader also accepts the legacy two-record representation. */
static int
fork_meta_persist_segment(uint32_t timeline, const PsKey *key, uint64_t lsn,
							  uint32_t nblocks, uint8_t kind, uint64_t order_id,
							  uint64_t admission_seq)
{
	ForkMetaRecV2 rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = FORK_META_V2_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.timeline = timeline;
	rec.key = *key;
	rec.lsn = lsn;
	rec.admission_seq = admission_seq;
	rec.order_id = order_id;
	rec.nblocks = nblocks;
	rec.kind = kind == FEV_SEG_GROW ? FEV_SEG_GROW_BOUND :
		FEV_SEG_COMMIT_BOUND;
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
	uint64_t	off = 0;
	int			have_records = 0;
	int			nread = 0;

	for (;;)
	{
		ForkMetaRecV2 rec;
		uint32_t	first;
		uint64_t	rec_size;

		nread = ps_storage->fork_meta_read(off, &first, sizeof(first));
		if (nread != (int) sizeof(first))
			break;
		memset(&rec, 0, sizeof(rec));
		if (first == FORK_META_V2_MAGIC)
		{
			nread = ps_storage->fork_meta_read(off, &rec, sizeof(rec));
			if (nread != (int) sizeof(rec) || rec.rec_len != sizeof(rec))
				break;
			rec_size = sizeof(rec);
		}
		else
		{
			ForkMetaRecV1 old;

			nread = ps_storage->fork_meta_read(off, &old, sizeof(old));
			if (nread != (int) sizeof(old))
				break;
			rec.timeline = old.timeline;
			rec.key = old.key;
			rec.lsn = old.lsn;
			rec.nblocks = old.nblocks;
			rec.kind = old.kind;
			rec_size = sizeof(old);
			if (old.kind == FEV_SEG_GROW_BOUND ||
				old.kind == FEV_SEG_COMMIT_BOUND)
			{
				ForkMetaRecV1 idrec;
				int			idread;

				idread = ps_storage->fork_meta_read(off + sizeof(old), &idrec,
										   sizeof(idrec));
				if (idread != (int) sizeof(idrec))
					break;
				rec_size += sizeof(idrec);
				if (idrec.kind == FEV_SEG_ID && idrec.timeline == old.timeline &&
					key_eq(&idrec.key, &old.key) && idrec.nblocks == old.nblocks)
					rec.order_id = idrec.lsn;
			}
		}
		have_records = 1;
		if (rec.admission_seq != 0)
			admission_seq_observe(rec.admission_seq);
		if (rec.order_id != 0)
			segment_order_id_observe(rec.order_id);
		if (rec.kind == FEV_MIGRATED)
			fork_meta_migrated = 1;
		else if (rec.kind == FEV_MIGRATING)
			fork_meta_migrating = 1;
		else if ((rec.kind == FEV_SEG_GROW || rec.kind == FEV_SEG_COMMIT ||
				  rec.kind == FEV_SEG_GROW_BOUND ||
				  rec.kind == FEV_SEG_COMMIT_BOUND) &&
				 rec.timeline < MAX_TIMELINES &&
				 ((rec.kind == FEV_SEG_GROW || rec.kind == FEV_SEG_COMMIT) ||
				  rec.order_id != 0))
			fork_event_add_seg_marker(
				fork_get_or_create(rec.timeline, &rec.key),
				rec.lsn, rec.nblocks, rec.kind, rec.order_id,
				rec.admission_seq);
		else if (rec.kind <= FEV_DEAD && rec.timeline < MAX_TIMELINES)
		{
			fork_event_add(fork_get_or_create(rec.timeline, &rec.key),
						   rec.lsn, rec.admission_seq, rec.nblocks, rec.kind);
		}
		else
			fprintf(stderr, "pagestore: skipping invalid fork-meta record "
					"(timeline=%u kind=%u)\n", rec.timeline, rec.kind);
		off += rec_size;
	}
	/* A short tail is not a record and must not become a prefix of the first
	 * migration marker (or any later append). */
	if (nread > 0 && ps_storage->fork_meta_truncate(off) != 0)
		return -1;
	if (nread < 0 && off != 0)
		return -1;
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
		if (fork_meta_persist(0, &zk, 0, 0, 0, FEV_MIGRATING) != 0)
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

static int
load_timelines(void)
{
	uint32_t magic;
	uint64_t off = 0;
	TimelineRec legacy[ MAX_TIMELINES ];
	uint32_t nlegacy = 0;
	int n;

	n = ps_storage->meta_read(0, &magic, sizeof(magic));
	if (n < 0 && errno == ENOENT)
		return 0;
	if (n == 0)
		return 0;
	if (n != (int) sizeof(magic))
	{
		if (n > 0 && ps_storage->meta_truncate &&
			ps_storage->meta_truncate(0) == 0)
			return 0;
		return -1;
	}
	if (magic == TIMELINE_META_V2_MAGIC)
	{
		TimelineRecV2 rec;
		for (;;)
		{
			n = ps_storage->meta_read(off, &rec, sizeof(rec));
			if (n == 0)
				return 0;
			if (n != (int) sizeof(rec))
			{
				if (n > 0 && ps_storage->meta_truncate &&
					ps_storage->meta_truncate(off) == 0)
					return 0;
				return -1;
			}
			if (rec.magic != TIMELINE_META_V2_MAGIC ||
				rec.rec_len != sizeof(rec) || rec.reserved != 0 ||
				rec.crc != timeline_rec_crc(&rec) ||
				rec.id >= MAX_TIMELINES || timelines[rec.id].defined ||
				!branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
				return -1;
			timeline_define(rec.id, rec.parent, rec.branch_lsn);
			off += sizeof(rec);
		}
	}
	/* Legacy records are accepted only when complete and valid, then atomically
	 * rewritten in the checksummed format before the daemon becomes writable. */
	off = 0;
	for (;;)
	{
		TimelineRec rec;
		n = ps_storage->meta_read(off, &rec, sizeof(rec));
		if (n == 0)
			break;
		if (n != (int) sizeof(rec))
		{
			if (n < 0 || !ps_storage->meta_truncate ||
				ps_storage->meta_truncate(off) != 0)
				return -1;
			break;
		}
		if (nlegacy == MAX_TIMELINES ||
			rec.id >= MAX_TIMELINES || timelines[rec.id].defined ||
			!branch_request_ok(rec.id, rec.parent, rec.branch_lsn))
			return -1;
		legacy[nlegacy++] = rec;
		timeline_define(rec.id, rec.parent, rec.branch_lsn);
		off += sizeof(rec);
	}
	if (nlegacy > 0)
	{
		TimelineRecV2 out[MAX_TIMELINES];
		for (uint32_t i = 0; i < nlegacy; i++)
		{
			memset(&out[i], 0, sizeof(out[i]));
			out[i].magic = TIMELINE_META_V2_MAGIC;
			out[i].rec_len = sizeof(out[i]);
			out[i].id = legacy[i].id;
			out[i].parent = legacy[i].parent;
			out[i].branch_lsn = legacy[i].branch_lsn;
			out[i].crc = timeline_rec_crc(&out[i]);
		}
		if (!ps_storage->meta_rewrite ||
			ps_storage->meta_rewrite(out, nlegacy * sizeof(out[0])) != 0)
			return -1;
	}
	return 0;
}

/* ===================== shipped WAL log (per timeline) ================== */

static void publish_wal_index_metrics(void);

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

typedef struct WalChunkRef
{
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	uint64_t	payload_off;
} WalChunkRef;

static WalChunkRef *wal_chunks[MAX_TIMELINES];
static uint32_t wal_chunks_n[MAX_TIMELINES];
static uint32_t wal_chunks_cap[MAX_TIMELINES];
static uint64_t wal_log_bytes[MAX_TIMELINES];

static void walidx_progress_init(uint32_t tl, uint64_t first_lsn);

static int
wal_chunk_reserve(uint32_t tl)
{
	WalChunkRef *grown;
	uint32_t	newcap;

	if (wal_chunks_n[tl] < wal_chunks_cap[tl])
		return 0;
	newcap = wal_chunks_cap[tl] ? wal_chunks_cap[tl] * 2 : 64;
	grown = realloc(wal_chunks[tl], (size_t) newcap * sizeof(*grown));
	if (!grown)
		return -1;
	wal_chunks[tl] = grown;
	wal_chunks_cap[tl] = newcap;
	return 0;
}

static void
wal_chunk_add(uint32_t tl, uint64_t record_off, const WalRecHdr *h)
{
	WalChunkRef *ref = &wal_chunks[tl][wal_chunks_n[tl]++];

	ref->start_lsn = h->start_lsn;
	ref->end_lsn = h->start_lsn + h->len;
	ref->payload_off = record_off + sizeof(*h);
	wal_log_bytes[tl] = ref->payload_off + h->len;
}

static uint32_t
wal_chunk_lower_bound(uint32_t tl, uint64_t lsn)
{
	uint32_t	lo = 0;
	uint32_t	hi = wal_chunks_n[tl];

	/* First chunk whose end is after lsn. */
	while (lo < hi)
	{
		uint32_t	mid = lo + (hi - lo) / 2;

		if (wal_chunks[tl][mid].end_lsn <= lsn)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

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
	uint64_t	we = start_lsn + len;
	unsigned char *tmp = NULL;
	unsigned char *mask = NULL;
	uint32_t	covered = 0;

	*covered_prefix = 0;
	*prefix_only = 1;
	for (uint32_t i = wal_chunk_lower_bound(tl, start_lsn);
		 i < wal_chunks_n[tl] && wal_chunks[tl][i].start_lsn < we; i++)
	{
		WalChunkRef *ref = &wal_chunks[tl][i];
		uint64_t	rs = ref->start_lsn;
		uint64_t	re = ref->end_lsn;
		uint64_t	os = rs > start_lsn ? rs : start_lsn;
		uint64_t	oe = re < we ? re : we;

		if (os < oe)
		{
			uint64_t	src = ref->payload_off + (os - rs);
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
			for (uint32_t j = 0; j < n; j++)
				if (!mask[base + j])
				{
					mask[base + j] = 1;
					covered++;
				}
		}
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
	if (wal_chunk_reserve(tl) != 0)
		return -1;
	if (ps_storage->wal_append(tl, &h, sizeof(h), data, len) != 0)
		return -1;

	wal_chunk_add(tl, wal_log_bytes[tl], &h);
	wal_end_advance(tl, start_lsn + len);
	if (len > 0)
		walidx_progress_init(tl, start_lsn);
	publish_wal_index_metrics();
	return 0;
}

/* Fill 'out' from ONE timeline's log: the overlap of [start, start+len) with
 * [.., cap) and with each shipped chunk.  Bytes not covered are left as-is. */
static uint32_t
wal_read_one(uint32_t tl, uint64_t start, uint32_t len, uint64_t cap,
			 unsigned char *out)
{
	uint32_t	filled = 0;
	uint64_t	we = start + len;

	if (we > cap)
		we = cap;
	if (we <= start)
		return 0;

	for (uint32_t i = wal_chunk_lower_bound(tl, start);
		 i < wal_chunks_n[tl] && wal_chunks[tl][i].start_lsn < we; i++)
	{
		WalChunkRef *ref = &wal_chunks[tl][i];
		uint64_t	rs = ref->start_lsn;
		uint64_t	re = ref->end_lsn;
		uint64_t	os = rs > start ? rs : start;	/* overlap start */
		uint64_t	oe = re < we ? re : we; /* overlap end */

		if (os < oe)
		{
			uint64_t	src = ref->payload_off + (os - rs);
			int			n = ps_storage->wal_read(tl, src, out + (os - start),
												 (uint32_t) (oe - os));

			if (n > 0)
				filled += (uint32_t) n;
		}
	}
	return filled;
}

/* The LSN where a timeline's shipped log begins (its first chunk's start),
 * or UINT64_MAX for an empty log.  The log is contiguous from there: the
 * archiver ships completed segments strictly in order. */
static uint64_t
wal_log_start(uint32_t tl)
{
	if (tl < MAX_TIMELINES && wal_chunks_n[tl] != 0)
		return wal_chunks[tl][0].start_lsn;
	return UINT64_MAX;
}

static int
wal_payload_readable(uint32_t tl, uint64_t payload_off, uint32_t len)
{
	unsigned char byte;
	uint64_t	last;

	if (len == 0)
		return 1;
	last = payload_off + len - 1;
	if (last < payload_off)
		return 0;
	return ps_storage->wal_read(tl, last, &byte, 1) == 1;
}

static int
wal_coverage_advance(uint32_t tl, uint64_t start_lsn, uint64_t end_lsn)
{
	uint64_t	covered;
	uint64_t	off;

	if (tl >= MAX_TIMELINES)
		return 0;
	if (!wal_covered_valid[tl])
	{
		WalRecHdr	h;

		if (ps_storage->wal_read(tl, 0, &h, sizeof(h)) != (int) sizeof(h) ||
			h.magic != WAL_MAGIC)
			return start_lsn == end_lsn;
		wal_covered[tl] = h.start_lsn;
		wal_covered_off[tl] = 0;
		wal_covered_valid[tl] = 1;
	}
	covered = wal_covered[tl];
	off = wal_covered_off[tl];
	if (start_lsn > covered)
		return 0;
	while (covered < end_lsn)
	{
		WalRecHdr	h;
		int			n;
		int			advanced = 0;

		while ((n = ps_storage->wal_read(tl, off, &h, sizeof(h))) ==
			   (int) sizeof(h))
		{
			uint64_t	payload_off;
			uint64_t	rec_end;
			uint64_t	next_off;

			if (h.magic != WAL_MAGIC)
				return 0;
			payload_off = off + sizeof(h);
			next_off = payload_off + h.len;
			if (payload_off < off || next_off < payload_off)
				return 0;
			rec_end = h.start_lsn + h.len;
			if (rec_end < h.start_lsn)
				return 0;
			if (!wal_payload_readable(tl, payload_off, h.len))
				return 0;
			off = next_off;
			if (h.start_lsn <= covered)
			{
				if (rec_end > covered)
					covered = rec_end;
				advanced = 1;
				wal_covered[tl] = covered;
				wal_covered_off[tl] = off;
				if (covered >= end_lsn)
					return 1;
			}
			else
			{
				wal_covered[tl] = covered;
				wal_covered_off[tl] = off - sizeof(h) - h.len;
				return 0;
			}
		}
		if (n < 0 || !advanced)
			return 0;
	}
	return 1;
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
static int
wal_recover_one(uint32_t tl)
{
	uint64_t	off = 0;
	uint64_t	good_off = 0;
	WalRecHdr	h;
	unsigned char byte;
	int			truncate_needed = 0;
	int			nread;

	if (tl >= MAX_TIMELINES)
		return -1;
	free(wal_chunks[tl]);
	wal_chunks[tl] = NULL;
	wal_chunks_n[tl] = 0;
	wal_chunks_cap[tl] = 0;
	wal_log_bytes[tl] = 0;
	while ((nread = ps_storage->wal_read(tl, off, &h, sizeof(h))) ==
		   (int) sizeof(h))
	{
		if (h.magic != WAL_MAGIC)
			break;
		if (h.start_lsn + h.len < h.start_lsn)
		{
			truncate_needed = 1;
			break;
		}
		if (h.len > 0 &&
			ps_storage->wal_read(tl, off + sizeof(h) + h.len - 1,
								 &byte, 1) != 1)
		{
			truncate_needed = 1;
			break;
		}
		if (wal_chunk_reserve(tl) != 0)
			return -1;
		wal_chunk_add(tl, off, &h);
		if (h.start_lsn + h.len > wal_end_read(tl))
		{
			timeline_mark_used(tl);
			wal_end_advance(tl, h.start_lsn + h.len);
		}
		off += sizeof(h) + h.len;
		good_off = off;
	}
	if (nread > 0 && nread < (int) sizeof(h))
		truncate_needed = 1;
	if (truncate_needed && ps_storage->wal_truncate)
		(void) ps_storage->wal_truncate(tl, good_off);
	return 0;
}

/* ===================== per-page WAL index ============================== */

/*
 * Maps (timeline, key, block) -> the LSNs of WAL records that modify that page,
 * in ascending order.  This is the lookup single-page materialization needs: to
 * rebuild page P as-of LSN L, take P's newest stored image and replay the WAL
 * records whose LSNs fall after it and <= L.  Populated by decoding shipped WAL
 * (next milestone); queried via PS_OP_WAL_INDEX_GET.
 *
 * Each successful index addition is also appended to a per-timeline durable
 * log.  Restart replays that log; a later indexer can therefore resume from a
 * durable boundary instead of treating a daemon restart as an empty index.
 */
#define WALIDX_MAGIC	0x57494458	/* "WIDX" */
#define WALIDX_PROGRESS_MAGIC	0x57495047	/* "WIPG" */

typedef struct WalIdxLogHdr
{
	uint32_t	magic;
	uint32_t	rec_len;
} WalIdxLogHdr;

typedef struct WalIdxRec
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	crc;
	uint32_t	pad;
	uint32_t	timeline;
	uint32_t	block;
	uint64_t	lsn;
	PsKey		key;
} WalIdxRec;

typedef struct WalIdxProgressRec
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	crc;
	uint32_t	pad;
	uint32_t	timeline;
	uint32_t	pad2;
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	uint64_t	shard_mask[2];
	uint64_t	shard_offsets[PS_MAX_CHANNELS];
} WalIdxProgressRec;

static uint64_t walidx_progress[MAX_TIMELINES];
static uint64_t walidx_shards_seen[MAX_TIMELINES][2];
static uint64_t walidx_shards_required[MAX_TIMELINES][2];
static uint64_t walidx_shard_offsets_seen[MAX_TIMELINES][PS_MAX_CHANNELS];
static uint64_t walidx_shard_offsets_required[MAX_TIMELINES][PS_MAX_CHANNELS];
static pthread_mutex_t walidx_meta_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_rwlock_t walidx_publish_lock = PTHREAD_RWLOCK_INITIALIZER;
static PsShmHeader *metrics_header;

static void
publish_wal_index_metrics(void)
{
	uint64_t	pending = 0;
	uint32_t	lagging = 0;

	if (metrics_header == NULL)
		return;
	pthread_mutex_lock(&walidx_meta_lock);
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
	{
		uint64_t shipped = wal_end_read(tl);
		uint64_t indexed = walidx_progress[tl];

		/* Unused timelines have neither lag nor a WAL log to probe. */
		if (shipped == 0)
			continue;
		if (indexed == 0)
		{
			uint64_t first = wal_log_start(tl);

			indexed = first == UINT64_MAX ? shipped : first;
		}
		if (shipped > indexed)
		{
			uint64_t delta = shipped - indexed;

			pending = UINT64_MAX - pending < delta ? UINT64_MAX : pending + delta;
			lagging++;
		}
	}
	pthread_mutex_unlock(&walidx_meta_lock);
	ps_store_release_u64(&metrics_header->wal_index_pending_bytes, pending);
	ps_store_release(&metrics_header->wal_index_lagging_timelines, lagging);
}

void
ps_core_set_metrics_header(PsShmHeader *hdr)
{
	metrics_header = hdr;
	publish_wal_index_metrics();
}

static void
walidx_progress_init(uint32_t tl, uint64_t first_lsn)
{
	if (tl >= MAX_TIMELINES || first_lsn == UINT64_MAX)
		return;
	pthread_mutex_lock(&walidx_meta_lock);
	if (walidx_progress[tl] == 0)
		walidx_progress[tl] = first_lsn;
	pthread_mutex_unlock(&walidx_meta_lock);
}

static uint32_t
walidx_rec_crc(WalIdxRec *rec)
{
	uint32_t	save = rec->crc;
	uint32_t	crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static uint32_t
walidx_progress_crc(WalIdxProgressRec *rec)
{
	uint32_t	save = rec->crc;
	uint32_t	crc;

	rec->crc = 0;
	crc = fnv(rec, sizeof(*rec));
	rec->crc = save;
	return crc;
}

static void
walidx_mark_shard(uint64_t mask[2], uint32_t shard)
{
	if (shard < PS_MAX_CHANNELS)
		mask[shard / 64] |= UINT64_C(1) << (shard % 64);
}

static int
walidx_shard_marked(const uint64_t mask[2], uint32_t shard)
{
	return shard < PS_MAX_CHANNELS &&
		(mask[shard / 64] & (UINT64_C(1) << (shard % 64))) != 0;
}

static int
walidx_mask_valid_for_shards(const uint64_t mask[2])
{
	uint32_t	ns = core_shards();

	for (uint32_t shard = ns; shard < PS_MAX_CHANNELS; shard++)
		if (walidx_shard_marked(mask, shard))
			return 0;
	return 1;
}

static int
walidx_offsets_valid_for_shards(const uint64_t offsets[PS_MAX_CHANNELS])
{
	uint32_t	ns = core_shards();

	for (uint32_t shard = ns; shard < PS_MAX_CHANNELS; shard++)
		if (offsets[shard] != 0)
			return 0;
	return 1;
}

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

static WalIdxEnt *
walidx_find(uint32_t tl, const PsKey *key, uint32_t block)
{
	uint32_t	h = page_hash(tl, key, block);
	Shard	   *s = shard_for(key);
	WalIdxEnt  *e;

	for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == tl && e->block == block && key_eq(&e->key, key))
			return e;
	return NULL;
}

static int
walidx_lower_bound(const WalIdxEnt *e, uint64_t lsn)
{
	int		lo = 0;
	int		hi = e ? e->n : 0;

	while (lo < hi)
	{
		int mid = lo + (hi - lo) / 2;

		if (e->lsns[mid] < lsn)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

static void
walidx_add_memory(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	uint32_t	h = page_hash(tl, key, block);
	Shard	   *s = shard_for(key);
	WalIdxEnt  *e;

	timeline_mark_used(tl);

	e = walidx_find(tl, key, block);
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
	{
		int i = walidx_lower_bound(e, lsn);

		if (i < e->n && e->lsns[i] == lsn)
			return;
		memmove(&e->lsns[i + 1], &e->lsns[i],
				(size_t) (e->n - i) * sizeof(uint64_t));
		e->lsns[i] = lsn;
		e->n++;
	}
}

static int
walidx_add_batch_locked(uint32_t tl, const PsWalIndexEntry *entries,
						uint32_t nentries)
{
	WalIdxRec  *records;
	uint32_t	nrecords = 0;
	uint32_t	shard;

	if (tl >= MAX_TIMELINES || nentries == 0)
		return -1;
	shard = ps_shard_of(&entries[0].key);
	records = malloc((size_t) nentries * sizeof(*records));
	if (!records)
		return -1;
	for (uint32_t i = 0; i < nentries; i++)
	{
		WalIdxEnt  *e;
		WalIdxRec  *rec;
		int			pos;

		if (ps_shard_of(&entries[i].key) != shard)
		{
			free(records);
			return -1;
		}
		e = walidx_find(tl, &entries[i].key, entries[i].block);
		if (e)
		{
			pos = walidx_lower_bound(e, entries[i].lsn);
			if (pos < e->n && e->lsns[pos] == entries[i].lsn)
				continue;
		}
		rec = &records[nrecords++];
		rec->magic = WALIDX_MAGIC;
		rec->rec_len = sizeof(*rec);
		rec->crc = 0;
		rec->pad = 0;
		rec->timeline = tl;
		rec->block = entries[i].block;
		rec->lsn = entries[i].lsn;
		rec->key = entries[i].key;
		rec->crc = walidx_rec_crc(rec);
	}
	if (nrecords == 0)
	{
		free(records);
		return 0;
	}
	if (ps_storage->walidx_append(tl, shard, records,
								(uint32_t) (nrecords * sizeof(*records))) != 0)
	{
		free(records);
		return -1;
	}
	pthread_mutex_lock(&walidx_meta_lock);
	walidx_shard_offsets_seen[tl][shard] += nrecords * sizeof(*records);
	walidx_mark_shard(walidx_shards_seen[tl], shard);
	pthread_mutex_unlock(&walidx_meta_lock);
	for (uint32_t i = 0; i < nrecords; i++)
		walidx_add_memory(tl, &records[i].key, records[i].block,
						  records[i].lsn);
	free(records);
	return 0;
}

static int
walidx_add(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	PsWalIndexEntry entry;
	int			rc;

	entry.key = *key;
	entry.block = block;
	entry.pad = 0;
	entry.lsn = lsn;
	pthread_rwlock_rdlock(&walidx_publish_lock);
	rc = walidx_add_batch_locked(tl, &entry, 1);
	pthread_rwlock_unlock(&walidx_publish_lock);
	return rc;
}

static int
walidx_recover_one(uint32_t tl, uint32_t shard)
{
	uint64_t	read_off = 0;
	uint64_t	good_off = 0;
	unsigned char buf[PS_IO_UNIT];
	int			used = 0;
	int			torn = 0;

	if (tl >= MAX_TIMELINES || shard >= PS_MAX_CHANNELS ||
		shard >= core_shards())
		return -1;
	for (;;)
	{
		int			n;
		int			want = (int) sizeof(buf) - used;
		int			pos = 0;

		n = ps_storage->walidx_read(tl, shard, read_off, buf + used,
									(uint32_t) want);
		if (n == 0)
		{
			if (used != 0)
				torn = 1;
			break;
		}
		if (n < 0)
		{
			if (errno == ENOENT &&
				!walidx_shard_marked(walidx_shards_required[tl], shard))
				return 0;
			return -1;
		}
		read_off += (uint64_t) n;
		used += n;

		while (used - pos >= (int) sizeof(WalIdxLogHdr))
		{
			WalIdxLogHdr hdr;
			uint32_t	rec_len;

			memcpy(&hdr, buf + pos, sizeof(hdr));
			if (hdr.magic == WALIDX_MAGIC && hdr.rec_len == sizeof(WalIdxRec))
				rec_len = sizeof(WalIdxRec);
			else if (shard == 0 && hdr.magic == WALIDX_PROGRESS_MAGIC &&
					 hdr.rec_len == sizeof(WalIdxProgressRec))
				rec_len = sizeof(WalIdxProgressRec);
			else
				return -1;
			if (used - pos < (int) rec_len)
				break;

			if (hdr.magic == WALIDX_MAGIC)
			{
				WalIdxRec rec;

				memcpy(&rec, buf + pos, sizeof(rec));
				if (rec.magic != WALIDX_MAGIC || rec.rec_len != sizeof(rec) ||
					rec.timeline != tl || rec.crc != walidx_rec_crc(&rec))
					return -1;
				walidx_mark_shard(walidx_shards_seen[tl], shard);
				walidx_add_memory(tl, &rec.key, rec.block, rec.lsn);
			}
			else
			{
				WalIdxProgressRec rec;
				uint64_t	first;

				memcpy(&rec, buf + pos, sizeof(rec));
				first = wal_log_start(tl);
				if (walidx_progress[tl] == 0 && first != UINT64_MAX)
					walidx_progress[tl] = first;
				if (rec.magic != WALIDX_PROGRESS_MAGIC ||
					rec.rec_len != sizeof(rec) || rec.timeline != tl ||
					rec.crc != walidx_progress_crc(&rec) ||
					rec.start_lsn != walidx_progress[tl] ||
					rec.end_lsn < rec.start_lsn ||
					rec.end_lsn > wal_end_read(tl) ||
					!walidx_mask_valid_for_shards(rec.shard_mask) ||
					!walidx_offsets_valid_for_shards(rec.shard_offsets) ||
					!wal_coverage_advance(tl, rec.start_lsn, rec.end_lsn))
					return -1;
				walidx_shards_required[tl][0] |= rec.shard_mask[0];
				walidx_shards_required[tl][1] |= rec.shard_mask[1];
				for (uint32_t i = 0; i < core_shards(); i++)
					if (rec.shard_offsets[i] >
						walidx_shard_offsets_required[tl][i])
						walidx_shard_offsets_required[tl][i] =
							rec.shard_offsets[i];
				walidx_progress[tl] = rec.end_lsn;
			}
			pos += (int) rec_len;
			good_off += rec_len;
		}
		if (pos != 0)
		{
			used -= pos;
			if (used != 0)
				memmove(buf, buf + pos, (size_t) used);
		}
		if (n < want)
		{
			if (used != 0)
				torn = 1;
			break;
		}
	}
	if (good_off < walidx_shard_offsets_required[tl][shard])
		return -1;
	walidx_shard_offsets_seen[tl][shard] = good_off;
	/* Do not let a torn/corrupt suffix become a permanent replay barrier. */
	if (torn && ps_storage->walidx_truncate(tl, shard, good_off) != 0)
		return -1;
	return 0;
}

static int
walidx_commit(uint32_t tl, uint64_t start_lsn, uint64_t end_lsn)
{
	WalIdxProgressRec rec;
	uint64_t	current;
	uint64_t	first;
	int			rc = -1;
	int			append_progress = 0;

	/* A durable marker must name a contiguous prefix of shipped WAL. */
	if (tl >= MAX_TIMELINES)
		return -1;
	pthread_rwlock_wrlock(&walidx_publish_lock);
	pthread_mutex_lock(&walidx_meta_lock);
	current = walidx_progress[tl];
	if (current == 0)
	{
		first = wal_log_start(tl);
		if (first != UINT64_MAX)
			current = first;
	}
	if (start_lsn != current || end_lsn < start_lsn ||
		end_lsn > wal_end_read(tl) ||
		!wal_coverage_advance(tl, start_lsn, end_lsn))
		goto out;
	memset(&rec, 0, sizeof(rec));
	rec.magic = WALIDX_PROGRESS_MAGIC;
	rec.rec_len = sizeof(rec);
	rec.crc = 0;
	rec.timeline = tl;
	rec.start_lsn = current;
	rec.end_lsn = end_lsn;
	rec.shard_mask[0] = walidx_shards_seen[tl][0];
	rec.shard_mask[1] = walidx_shards_seen[tl][1];
	for (uint32_t shard = 0; shard < core_shards(); shard++)
		rec.shard_offsets[shard] = walidx_shard_offsets_seen[tl][shard];
	rec.crc = walidx_progress_crc(&rec);
	append_progress = 1;
out:
	pthread_mutex_unlock(&walidx_meta_lock);
	if (!append_progress)
	{
		pthread_rwlock_unlock(&walidx_publish_lock);
		return -1;
	}
	if (ps_storage->walidx_append(tl, 0, &rec, sizeof(rec)) != 0)
	{
		pthread_rwlock_unlock(&walidx_publish_lock);
		return -1;
	}
	pthread_mutex_lock(&walidx_meta_lock);
	current = walidx_progress[tl];
	if (current == 0)
	{
		first = wal_log_start(tl);
		if (first != UINT64_MAX)
			current = first;
	}
	if (current != rec.start_lsn)
		goto out_update;
	walidx_shard_offsets_seen[tl][0] += sizeof(rec);
	walidx_shards_required[tl][0] |= rec.shard_mask[0];
	walidx_shards_required[tl][1] |= rec.shard_mask[1];
	for (uint32_t shard = 0; shard < core_shards(); shard++)
		if (rec.shard_offsets[shard] > walidx_shard_offsets_required[tl][shard])
			walidx_shard_offsets_required[tl][shard] = rec.shard_offsets[shard];
	walidx_progress[tl] = rec.end_lsn;
	rc = 0;
out_update:
	pthread_mutex_unlock(&walidx_meta_lock);
	pthread_rwlock_unlock(&walidx_publish_lock);
	publish_wal_index_metrics();
	return rc;
}

static uint64_t
walidx_progress_read(uint32_t tl)
{
	uint64_t	progress;

	pthread_mutex_lock(&walidx_meta_lock);
	progress = walidx_progress[tl];
	pthread_mutex_unlock(&walidx_meta_lock);
	return progress;
}

static int
walidx_upper_bound(WalIdxEnt *e, uint64_t lsn)
{
	int			lo = 0;
	int			hi = e->n;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (e->lsns[mid] <= lsn)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

/*
 * Merge the already-sorted per-timeline arrays directly into one bounded
 * response page.  A cursor request neither allocates nor scans the remaining
 * history beyond the next max_out records.
 */
static int
walidx_get(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn_max,
		   int have_cursor, uint64_t cursor_lsn, uint32_t cursor_timeline,
		   PsWalRec *out, int max_out)
{
	typedef struct WalIdxSource
	{
		WalIdxEnt  *entry;
		uint32_t	timeline;
		int			pos;
		int			end;
	} WalIdxSource;
	WalIdxSource sources[MAX_TIMELINES];
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	int			nsources = 0;
	int			nout = 0;
	TlWalk		w = tl_walk_first(tl, lsn_max);

	do
	{
		uint32_t	h = page_hash(w.tl, key, block);
		uint64_t	visible_end = walidx_progress_read(w.tl);
		WalIdxEnt  *e;

		/* Entries become queryable only with their durable progress marker. */
		if (visible_end == 0)
			continue;
		for (e = s->walidx[h & IDX_MASK]; e; e = e->next)
			if (e->timeline == w.tl && e->block == block && key_eq(&e->key, key))
			{
				int			pos = have_cursor ?
					walidx_lower_bound(e, cursor_lsn) : 0;
				uint64_t	cap_lsn = w.lsn < visible_end - 1 ?
					w.lsn : visible_end - 1;
				int			end = walidx_upper_bound(e, cap_lsn);

				if (have_cursor && pos < end && e->lsns[pos] == cursor_lsn &&
					w.tl <= cursor_timeline)
					pos++;
				if (pos < end)
				{
					sources[nsources].entry = e;
					sources[nsources].timeline = w.tl;
					sources[nsources].pos = pos;
					sources[nsources].end = end;
					nsources++;
				}
				break;
			}
	} while (tl_walk_next(&w));

	while (nout < max_out)
	{
		int			best = -1;

		for (int i = 0; i < nsources; i++)
		{
			uint64_t	lsn;
			uint64_t	best_lsn;

			if (sources[i].pos >= sources[i].end)
				continue;
			lsn = sources[i].entry->lsns[sources[i].pos];
			if (best < 0)
			{
				best = i;
				continue;
			}
			best_lsn = sources[best].entry->lsns[sources[best].pos];
			if (lsn < best_lsn ||
				(lsn == best_lsn &&
				 sources[i].timeline < sources[best].timeline))
				best = i;
		}
		if (best < 0)
			break;
		out[nout] = (PsWalRec) {
			.lsn = sources[best].entry->lsns[sources[best].pos++],
			.timeline = sources[best].timeline
		};
		nout++;
	}
	return nout;
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
			const unsigned char *page, uint64_t version,
			uint64_t *out_admission_seq)
{
	SegRecHdr	hdr;
	SegRecHdrAdmission admission_hdr;
	SegRecHdrBoundAdmission bound_hdr;
	uint64_t	header_size = sizeof(SegRecHdrAdmission);
	uint64_t	reclen;
	uint64_t	data_off;
	uint64_t	hdr_grow_lsn = 0;
	uint64_t	order_id = 0;
	uint64_t	page_version;
	uint64_t	admission_seq = admission_seq_alloc();
	int			clamped = 0;
	int			ordered_record = 0;
	int			segment_grows = 0;
	int			zero_version = 0;
	Shard	   *s = shard_for(key);
	ForkEnt    *fe = fork_find(timeline, key);
	uint64_t	branch_floor = 0;
	uint64_t	growth_floor = fe ? fe->last_def_lsn : 0;

	if (admission_seq == 0)
		return -1;

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

	hdr.magic = SEG_ADMISSION_MAGIC;
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
			 key->klass == PS_KLASS_SLRU_WM ||
			 key->klass == PS_KLASS_READER_SNAPSHOT)
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
		cur = read_through(timeline, key, block, UINT64_MAX, 0);
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
				visible = fork_nblocks_through(timeline, key, growth_floor, 0);
				existed_before = growth_floor > 0 &&
					fork_exists_through(timeline, key, growth_floor - 1, 0);
				ps_unlock_map();
				clamped = visible < block + 1 || !existed_before;
			}
			if (clamped)
				hdr.lsn = growth_floor;
		}
	}
	if (hdr.lsn == 0)
	{
		hdr.magic = SEG_WALLESS_ADMISSION_MAGIC;
		hdr.lsn = growth_floor;
		zero_version = 1;
	}
	hdr_grow_lsn = hdr.lsn;
	page_version = zero_version ? 0 : hdr.lsn;
	ordered_record = zero_version || clamped;
	segment_grows = (!fe ||
		fork_size_asof_hop(fe, hdr_grow_lsn, admission_seq) < block + 1);
	if (ordered_record)
	{
		order_id = segment_order_id_alloc();
		header_size = sizeof(SegRecHdrBoundAdmission);
		hdr.magic = zero_version ? SEG_WALLESS_ADMISSION_MAGIC :
			SEG_CLAMPED_ADMISSION_MAGIC;
	}
	reclen = header_size + page_size;

	/* roll over to a fresh segment when the current one would overflow */
	if (s->cur_seg < 0 || s->cur_off + reclen > segment_size)
	{
		s->cur_seg = (s->cur_seg < 0) ? 0 : s->cur_seg + 1;
		s->cur_off = 0;
	}

	/* write header then page bytes contiguously at the append cursor */
	if (ordered_record)
	{
		bound_hdr.hdr = hdr;
		bound_hdr.order_id = order_id;
		bound_hdr.admission_seq = admission_seq;
		if (ps_storage->seg_write(s->id, s->cur_seg, s->cur_off,
								  &bound_hdr, sizeof(bound_hdr)) != 0)
			return -1;
	}
	else
	{
		admission_hdr.hdr = hdr;
		admission_hdr.admission_seq = admission_seq;
		if (ps_storage->seg_write(s->id, s->cur_seg, s->cur_off,
								  &admission_hdr, sizeof(admission_hdr)) != 0)
			return -1;
	}
	data_off = s->cur_off + header_size;
	if (ps_storage->seg_write(s->id, s->cur_seg, data_off, page, page_size) != 0)
		return -1;

	/*
	 * The segment record is the growth's durability; this metadata marker only
	 * records its position among equal-LSN definitive events.  Recovery ignores
	 * an unmatched marker, so a torn/missing segment cannot manufacture size.
	 */
	if (ordered_record &&
		fork_meta_persist_segment(timeline, key, hdr_grow_lsn, block + 1,
								  segment_grows ? FEV_SEG_GROW : FEV_SEG_COMMIT,
								  order_id, admission_seq) != 0)
	{
		/* The complete body is not committed without its marker.  Retire this
		 * segment so a later torn header cannot reuse that stale body. */
		s->cur_off = segment_size;
		return -1;
	}

	/* index points at the page bytes (data_off), so reads skip the header */
	page_add_version(timeline, key, block, page_version, admission_seq,
					 s->id, s->cur_seg, data_off);

	/* Drop a partial cache insertion for this exact durable version, if any. */
	ps_pgcache_invalidate(timeline, key, block, page_version, admission_seq);
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
		uint32_t	flags = PS_IMG_REC_SEG_VALID;

		if (ordered_record)
			flags |= PS_IMG_REC_ORDERED;
		if (zero_version)
			flags |= PS_IMG_REC_WALLESS;
		if (ps_memtable_put(s->memtable, timeline, key, block, page_version,
							page, admission_seq, hdr_grow_lsn, order_id,
							(uint32_t) s->cur_seg,
							data_off, flags) != 0)
			s->coverage_broken = 1;
		if (ps_memtable_full(s->memtable))
		{
			/* A flush mutates the cross-shard
			 * ps_layer_map, so take map_lock here -- only on the rare flush, not
			 * on every write.  The caller already holds this shard's write lock,
			 * preserving the shard -> map order. */
			ps_lock_map_wr();
			flush_memtable(s, (uint32_t) s->cur_seg, s->cur_off);
			ps_unlock_map();
		}
	}
	else if (s->memtable)
		s->coverage_broken = 1;

	/*
	 * Grow the fork's size history with this page's exact version LSN: a
	 * block is readable as of a horizon iff it has a version at/below it,
	 * so keying the GROW event by hdr.lsn makes as-of NBLOCKS agree with
	 * as-of page reads block for block.  (This replaces the callers'
	 * former one-shot fork_grow after a batch.)  The WAL-less format stores a
	 * zero-version page/object's growth floor in the same record while its
	 * version stays 0.
	 */
	fork_grow_apply(timeline, key, block + 1, hdr_grow_lsn, admission_seq);
	if (out_admission_seq)
		*out_admission_seq = admission_seq;
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
				 uint64_t read_lsn, uint64_t read_seq, uint64_t expected_lsn,
				 uint64_t *out_lsn,
				 uint64_t *out_seq, unsigned char *out)
{
	unsigned char *tmp = malloc(page_size);
	PsLayerDesc *layers;
	uint32_t nlayers;
	int			error = 0;
	int			found = 0;
	uint64_t	best = 0;
	uint64_t	best_seq = 0;
	uint64_t	best_layer = 0;
	uint32_t	shard = ps_shard_of(key);

	if (!tmp)
		return 0;
	ps_lock_map_rd();
	nlayers = ps_layer_map.nlayers;
	layers = nlayers ? malloc((size_t) nlayers * sizeof(*layers)) : NULL;
	if (layers != NULL)
	{
		memcpy(layers, ps_layer_map.layers, (size_t) nlayers * sizeof(*layers));
		for (uint32_t i = 0; i < nlayers; i++)
			if (ps_layer_map.layers[i].kind == PS_LAYER_IMAGE &&
				ps_layer_map.layers[i].timeline == timeline &&
				layer_matches_read_shard(&ps_layer_map.layers[i], shard) &&
				!ps_layer_map.layers[i].deleting)
				__atomic_add_fetch(&ps_layer_map.layers[i].cache_readers, 1,
							   __ATOMIC_ACQ_REL);
	}
	ps_unlock_map();
	if (nlayers == 0)
	{
		free(tmp);
		return 0;
	}
	if (layers == NULL)
	{
		free(tmp);
		return 0;
	}
	for (uint32_t i = 0; i < nlayers; i++)
	{
		const PsLayerDesc *d = &layers[i];
		uint64_t	l,
					a;
		int			lookup;

		if (d->kind != PS_LAYER_IMAGE || d->timeline != timeline ||
			!layer_matches_read_shard(d, shard) || d->deleting)
			continue;
		if (expected_lsn != 0 &&
			(expected_lsn < d->lsn_start || expected_lsn > d->lsn_end))
			continue;
		lookup = ps_image_layer_lookup(d, key, block, read_lsn, read_seq, tmp,
									   page_size, &l, &a);

		if (lookup < 0)
		{
			error = 1;
			break;
		}
		if (lookup == 1 &&
			(!found || l > best || (l == best && a > best_seq) ||
			 (l == best && a == best_seq && d->layer_id > best_layer)))
		{
			best = l;
			best_seq = a;
			best_layer = d->layer_id;
			memcpy(out, tmp, page_size);
			found = 1;
		}
	}
	/* Release the snapshot pins only after all cache I/O has completed. */
	ps_lock_map_wr();
	for (uint32_t i = 0; i < nlayers; i++)
		if (layers[i].kind == PS_LAYER_IMAGE && layers[i].timeline == timeline &&
			layer_matches_read_shard(&layers[i], shard) && !layers[i].deleting)
			for (uint32_t j = 0; j < ps_layer_map.nlayers; j++)
				if (ps_layer_map.layers[j].layer_id == layers[i].layer_id)
				{
					__atomic_sub_fetch(&ps_layer_map.layers[j].cache_readers, 1,
								   __ATOMIC_ACQ_REL);
					if (layers[i].data_verified)
						ps_layer_map.layers[j].data_verified = true;
					if (tier_local_location(&ps_layer_map.layers[j]) == NULL &&
						ps_layer_store->layer_exists_local != NULL &&
						ps_layer_store->layer_exists_local(layers[i].layer_id) == 1)
						ps_layer_map.layers[j].cache_resident = true;
					break;
				}
	free(layers);
	free(tmp);
	if (found && out_lsn)
		*out_lsn = best;
	if (found && out_seq)
		*out_seq = best_seq;
	if (found)
	{
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == best_layer &&
				tier_local_location(&ps_layer_map.layers[i]) == NULL)
			{
				ps_layer_map.layers[i].cache_resident = true;
				break;
			}
	}
	ps_unlock_map();
	return error ? -1 : found;
}

/*
 * Resolve a read into out (page_size bytes): walk the timeline ancestry as
 * read_through() does, but serve the bytes from the memtable or an image layer
 * when they hold the authoritative version, falling back to the segment.  The
 * page index (page_visible) still selects the authoritative version at each
 * level, so the result matches the segment-only read; layers/memtable just serve
 * the bytes without touching the segment.  Returns 1 if a version was found and
 * out filled, 0 if the page is unwritten (caller zero-fills), and -1 if an
 * authoritative stored version cannot be read.
 */
int
read_resolve(uint32_t timeline, const PsKey *key, uint32_t block,
			 uint64_t read_lsn, uint64_t read_seq, unsigned char *out,
			 uint64_t *out_ver)
{
	Shard	   *s = shard_for(key);	/* same shard across the ancestry walk */
	TlWalk		walk[MAX_TIMELINES];
	uint32_t	levels = 0;
	TlWalk		w = tl_walk_first(timeline, read_lsn);

	/* Copy ancestry while CREATE_BRANCH is excluded, then release map_lock
	 * before a remote layer read can block. */
	ps_lock_map_rd();
	do
		walk[levels++] = w;
	while (levels < MAX_TIMELINES && tl_walk_next(&w));
	ps_unlock_map();

	for (uint32_t level = 0; level < levels; level++)
	{
		w = walk[level];
		{
			uint32_t	tl = w.tl;
			uint64_t	rl = w.lsn;
			uint64_t	seq_cap = rl == read_lsn ? read_seq : 0;
			ForkEnt    *fe = fork_find(tl, key);
			uint32_t	nb = 0;
			int			fork_state = fe ? fork_asof_hop(fe, rl, seq_cap, &nb) :
				FORK_HOP_NONE;
			PageEnt    *e = page_find(tl, key, block);
			PageVer    *pv = e ? page_visible(e, rl, seq_cap) : NULL;

		if (pv)
		{
			uint64_t	l,
						a;
			int			served;
			int			poisoned;

			if (fork_page_invalidated(fe, block, pv, rl, seq_cap))
				return 0;

			/*
			 * The materialized-page cache and the memtable are safe read sources
			 * only while they stay in lock-step with the segment log.  Once the
			 * manifest is poisoned, append_page() stops staging and the memtable
			 * can lag the segment-backed page index.  Bypass transient sources in
			 * that state; reclaimed versions still use their durable layer.
			 */
			poisoned = ps_manifest_poisoned();

			/* the resolved version (newest <= read_lsn); a caller that needs an
			 * exact-cutoff match -- e.g. an SLRU snapshot read -- compares it. */
			if (out_ver)
				*out_ver = pv->lsn;

			/* fast path: materialized-page cache, keyed by the resolved version */
			if (!poisoned && ps_pgcache_lookup(tl, key, block, pv->lsn,
											pv->admission_seq, out))
				return 1;

			if (s->memtable && !poisoned &&
				ps_memtable_lookup(s->memtable, tl, key, block, rl, seq_cap,
								   &l, &a, out) &&
				l == pv->lsn && a == pv->admission_seq)
			{
				__atomic_fetch_add(&s->rr_mem, 1, __ATOMIC_RELAXED);
				served = 1;		/* served from the memtable */
			}
			else if (pv->seg < 0)
			{
				int		layer_result = layer_map_lookup(tl, key, block, rl,
															seq_cap, pv->lsn, &l, &a, out);

				if (layer_result < 0)
					return -1;
				if (layer_result == 0 || l != pv->lsn || a != pv->admission_seq)
					return -1;
				/*
				 * Serve from a layer only for a layer-origin version (no segment
				 * copy).  A segment-backed version must come from its segment; layers
				 * are only authoritative after recovery or segment reclamation changes
				 * the page index entry to a layer origin.
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
				ps_pgcache_insert(tl, key, block, pv->lsn,
								  pv->admission_seq, out);
			return served ? 1 : 0;
		}
		if (fork_state == FORK_HOP_DEAD ||
			(fork_state == FORK_HOP_DEF && block >= nb))
			return 0;
		if (fork_state == FORK_HOP_DEF &&
			fork_inheritance_fenced(fe, block, rl, seq_cap))
			return 0;
		}
	}
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
typedef struct ControlLsnSlot
{
	uint64_t	lsn;
	unsigned char used;
} ControlLsnSlot;

/* Return 1 when every image through cap has a same-LSN note, 0 when one is
 * missing, and -1 when coverage cannot be proved.  Version chains are in
 * arrival rather than LSN order, so use a one-pass hash set instead of a
 * quadratic nested scan. */
static int
control_images_covered(PageEnt *notes, PageEnt *images, uint64_t cap)
{
	ControlLsnSlot *slots;
	size_t		nslots = 1;

	if (!images)
		return 1;
	if (!notes)
	{
		for (int i = 0; i < images->nver; i++)
			if (images->vers[i].lsn <= cap)
				return 0;
		return 1;
	}
	while (nslots < (size_t) notes->nver * 2 + 1)
	{
		if (nslots > SIZE_MAX / 2)
			return -1;
		nslots *= 2;
	}
	slots = calloc(nslots, sizeof(*slots));
	if (!slots)
		return -1;
	for (int i = 0; i < notes->nver; i++)
	{
		uint64_t lsn = notes->vers[i].lsn;
		size_t pos;

		if (lsn > cap)
			continue;
		pos = (size_t) ((lsn ^ (lsn >> 33)) * 0xff51afd7ed558ccdULL) &
			(nslots - 1);
		while (slots[pos].used && slots[pos].lsn != lsn)
			pos = (pos + 1) & (nslots - 1);
		slots[pos].used = 1;
		slots[pos].lsn = lsn;
	}
	for (int i = 0; i < images->nver; i++)
	{
		uint64_t lsn = images->vers[i].lsn;
		size_t pos;

		if (lsn > cap)
			continue;
		pos = (size_t) ((lsn ^ (lsn >> 33)) * 0xff51afd7ed558ccdULL) &
			(nslots - 1);
		while (slots[pos].used && slots[pos].lsn != lsn)
			pos = (pos + 1) & (nslots - 1);
		if (!slots[pos].used)
		{
			free(slots);
			return 0;
		}
	}
	free(slots);
	return 1;
}

int
wal_retain_floor(uint32_t timeline, uint64_t *floor_out)
{
	PsKey		key;
	TlWalk		ancestry[MAX_TIMELINES];
	uint32_t	ancestry_n = 0;
	uint32_t	tl = timeline;
	uint64_t	lsn = UINT64_MAX;
	uint64_t	floor = 0;
	unsigned char *tmp = malloc(page_size);
	int			rc = 0;
	bool		complete = false;

	if (!tmp)
		return -1;				/* cannot prove a floor: fail closed */
	ps_lock_map_rd();
	for (; ancestry_n < MAX_TIMELINES; ancestry_n++)
	{
		ancestry[ancestry_n].tl = tl;
		ancestry[ancestry_n].lsn = lsn;
		if (!timeline_has_parent(tl))
		{
			complete = true;
			break;
		}
		if (timelines[tl].branch_lsn < lsn)
			lsn = timelines[tl].branch_lsn;
		tl = (uint32_t) timelines[tl].parent;
	}
	ps_unlock_map();
	if (!complete)
	{
		free(tmp);
		return -1;				/* malformed ancestry: fail closed */
	}
	ancestry_n++;
	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;

	for (uint32_t level = 0; level < ancestry_n; level++)
	{
		TlWalk		w = ancestry[level];
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
				if (v->seg >= 0)
				{
					if (read_version(v, tmp) != 0)
					{
						rc = -1;
						goto done;
					}
				}
				else
				{
					uint64_t	layer_lsn;

				if (layer_map_lookup(w.tl, &key, 1, v->lsn, 0, v->lsn,
									 &layer_lsn, NULL, tmp) != 1 ||
						layer_lsn != v->lsn)
					{
						rc = -1;
						goto done;
					}
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
			int covered = control_images_covered(notes, images, w.lsn);

			if (covered < 0)
			{
				rc = -1;
				goto done;
			}
			if (!covered)
			{
				floor = 1;
				goto done;
			}
		}
	}

done:
	free(tmp);
	if (rc == 0)
		*floor_out = floor;
	return rc;
}

/* Scan one timeline's local control versions through cap.  This is used by
 * the batched effective-floor path so each descendant is read exactly once. */
static int
wal_retain_floor_level(uint32_t timeline, uint64_t cap, unsigned char *tmp,
					   uint64_t *floor)
{
	PsKey		key;
	PageEnt    *notes;
	PageEnt    *images;

	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;
	notes = page_find(timeline, &key, 1);
	images = page_find(timeline, &key, 0);
	if (notes)
	{
		for (int i = 0; i < notes->nver; i++)
		{
			PageVer *v = &notes->vers[i];
			uint64_t redo;

			if (v->lsn > cap)
				continue;
			if (v->seg >= 0)
			{
				if (read_version(v, tmp) != 0)
					return -1;
			}
			else
			{
				uint64_t layer_lsn;

				if (layer_map_lookup(timeline, &key, 1, v->lsn, 0, v->lsn,
								 &layer_lsn, NULL, tmp) != 1 ||
					layer_lsn != v->lsn)
					return -1;
			}
			memcpy(&redo, tmp, sizeof(redo));
			if (redo == 0)
			{
				*floor = 1;
				return 0;
			}
			if (*floor == 0 || redo < *floor)
				*floor = redo;
		}
	}
	if (images)
	{
		int covered = control_images_covered(notes, images, cap);

		if (covered < 0)
			return -1;
		if (!covered)
		{
			*floor = 1;
			return 0;
		}
	}
	return 0;
}

static void
retention_floor_add(uint64_t candidate, uint64_t *floor)
{
	/* LSN zero is a real branch cap but the public zero result means "no
	 * constraint".  Floor 1 is the established retain-everything sentinel. */
	if (candidate == 0)
		candidate = 1;
	if (*floor == 0 || candidate < *floor)
		*floor = candidate;
}

/* Project one descendant LSN onto target's physical history.  Caller holds
 * map-rd.  Return 1 if target is an ancestor (including self), else 0. */
static int
retention_project_lsn(uint32_t descendant, uint32_t target, uint64_t *lsn)
{
	uint32_t	current = descendant;
	uint32_t	hops = 0;

	while (current != target)
	{
		if (current >= MAX_TIMELINES || !timelines[current].defined ||
			!timeline_has_parent(current) || ++hops > MAX_TIMELINES)
			return 0;
		if (timelines[current].branch_lsn < *lsn)
			*lsn = timelines[current].branch_lsn;
		current = (uint32_t) timelines[current].parent;
	}
	return 1;
}

/* Caller holds map-wr and the page-prune read fence. */
static int
page_prune_fences(uint32_t timeline, PsPruneFence **fences_out,
				  uint32_t *nfences_out)
{
	PsRetentionPin *pins = NULL;
	uint32_t npins = 0;
	PsPruneFence *fences;
	uint32_t nfences = 0;

	if (ps_retention_snapshot_alloc(&pins, &npins) != 0)
		return -1;
	fences = malloc((size_t) (npins + MAX_TIMELINES) * sizeof(*fences));
	if (fences == NULL)
	{
		free(pins);
		return -1;
	}
	for (uint32_t i = 0; i < npins; i++)
		if ((pins[i].resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
		{
			uint64_t projected = pins[i].lsn;

			if (retention_project_lsn(pins[i].timeline, timeline, &projected))
			{
				fences[nfences].lsn = projected;
				fences[nfences].admission_seq = projected == pins[i].lsn &&
					pins[i].admission_seq != 0 ? pins[i].admission_seq : UINT64_MAX;
				nfences++;
			}
		}
	for (uint32_t candidate = 0; candidate < MAX_TIMELINES; candidate++)
	{
		uint64_t cap = UINT64_MAX;

		if (candidate != timeline && timelines[candidate].defined &&
			retention_project_lsn(candidate, timeline, &cap))
		{
			fences[nfences].lsn = cap;
			fences[nfences].admission_seq = UINT64_MAX;
			nfences++;
		}
	}
	free(pins);
	*fences_out = fences;
	*nfences_out = nfences;
	return 0;
}

/*
 * One conservative floor for one resource on one timeline.  Explicit pins on
 * descendants are projected through every branch cap; a live direct child is
 * itself a structural pin.  WAL additionally includes every restorable control
 * image on the target and descendants.  Thus page pruning, WAL GC and WAL-index
 * GC can share this authority instead of each inventing a partial horizon.
 */
static int
retention_effective_floor_internal(uint32_t timeline, uint32_t resource,
							   uint64_t *floor_out, int map_locked)
{
	typedef struct RetentionControlProjection
	{
		uint32_t	timeline;
		uint64_t	cap;
	} RetentionControlProjection;
	RetentionControlProjection controls[MAX_TIMELINES];
	TlWalk		ancestors[MAX_TIMELINES];
	uint32_t	ncontrols = 0;
	uint32_t	nancestors = 0;
	uint32_t	npins = 0;
	PsRetentionPin *pins = NULL;
	uint64_t	floor = 0;

	if (resource != PS_RETENTION_RESOURCE_PAGE_HISTORY &&
		resource != PS_RETENTION_RESOURCE_WAL &&
		resource != PS_RETENTION_RESOURCE_WAL_INDEX)
		return -1;

	if (!map_locked)
		ps_lock_map_rd();
	if (timeline >= MAX_TIMELINES || !timelines[timeline].defined ||
		ps_retention_snapshot_alloc(&pins, &npins) != 0)
	{
		if (!map_locked)
			ps_unlock_map();
		free(pins);
		return -1;
	}
	for (uint32_t i = 0; i < npins; i++)
	{
		PsRetentionPin pin = pins[i];
		uint64_t	projected;
		int			found;

		found = 1;
		if (found != 1 || pin.timeline >= MAX_TIMELINES ||
			!timelines[pin.timeline].defined)
		{
			if (!map_locked)
				ps_unlock_map();
			free(pins);
			return -1;
		}
		if ((pin.resources & resource) == 0)
			continue;
		projected = pin.lsn;
		if (retention_project_lsn(pin.timeline, timeline, &projected))
			retention_floor_add(projected, &floor);
	}
	free(pins);

	/* Every descendant can be started or read again later even with no active
	 * owner pin.  Project all of their branch caps, not just direct children: a
	 * nested branch may fork below its parent's own fork point.  The same walk
	 * snapshots the descendants whose visible control images constrain WAL. */
	for (uint32_t candidate = 0; candidate < MAX_TIMELINES; candidate++)
	{
		uint64_t	cap = UINT64_MAX;

		if (!timelines[candidate].defined ||
			!retention_project_lsn(candidate, timeline, &cap))
			continue;
		if (candidate != timeline && resource != PS_RETENTION_RESOURCE_PAGE_HISTORY)
			retention_floor_add(cap, &floor);
		if (resource == PS_RETENTION_RESOURCE_WAL)
		{
			controls[ncontrols].timeline = candidate;
			controls[ncontrols].cap = UINT64_MAX;
			ncontrols++;
		}
	}
	if (resource == PS_RETENTION_RESOURCE_WAL)
	{
		uint32_t current = timeline;
		uint64_t cap = UINT64_MAX;

		while (timeline_has_parent(current))
		{
			if (nancestors >= MAX_TIMELINES)
			{
				if (!map_locked)
					ps_unlock_map();
				return -1;
			}
			if (timelines[current].branch_lsn < cap)
				cap = timelines[current].branch_lsn;
			current = (uint32_t) timelines[current].parent;
			ancestors[nancestors].tl = current;
			ancestors[nancestors].lsn = cap;
			nancestors++;
		}
	}
	if (!map_locked)
		ps_unlock_map();

	if (resource == PS_RETENTION_RESOURCE_WAL)
	{
		unsigned char *tmp = malloc(page_size);

		if (!tmp)
			return -1;
		for (uint32_t i = 0; i < ncontrols && floor != 1; i++)
			if (wal_retain_floor_level(controls[i].timeline,
								   controls[i].cap, tmp, &floor) != 0)
			{
				free(tmp);
				return -1;
			}
		for (uint32_t i = 0; i < nancestors && floor != 1; i++)
			if (wal_retain_floor_level(ancestors[i].tl, ancestors[i].lsn,
								   tmp, &floor) != 0)
			{
				free(tmp);
				return -1;
			}
		free(tmp);
	}
	*floor_out = floor;
	return 0;
}

static int
retention_effective_floor(uint32_t timeline, uint32_t resource,
						  uint64_t *floor_out)
{
	return retention_effective_floor_internal(timeline, resource, floor_out, 0);
}

/* ===================== recovery (layers + segment tail) =============== */

typedef struct LayerRecoverRec
{
	uint32_t	timeline;
	uint64_t	layer_id;
	PsImgIndexEnt ent;
} LayerRecoverRec;

static int
layer_recover_cmp(const void *pa, const void *pb)
{
	const LayerRecoverRec *a = pa;
	const LayerRecoverRec *b = pb;

	if (a->ent.seg_id != b->ent.seg_id)
		return a->ent.seg_id < b->ent.seg_id ? -1 : 1;
	if (a->ent.seg_off != b->ent.seg_off)
		return a->ent.seg_off < b->ent.seg_off ? -1 : 1;
	return a->layer_id < b->layer_id ? -1 :
		(a->layer_id > b->layer_id ? 1 : 0);
}

/* Replay the index and fork-growth effects shared by layer and segment input. */
static int
replay_page_record(uint32_t timeline, const PsKey *key, uint32_t block,
				   uint64_t page_lsn, uint64_t admission_seq,
				   uint64_t growth_lsn, uint64_t order_id,
				   uint32_t flags, uint32_t shard, int seg, uint64_t off)
{
	int			ordered = (flags & PS_IMG_REC_ORDERED) != 0;
	int			wal_less = (flags & PS_IMG_REC_WALLESS) != 0;

	if (order_id != 0)
		segment_order_id_observe(order_id);
	if (admission_seq != 0)
		admission_seq_observe(admission_seq);
	if (ordered)
	{
		ForkEnt    *fe = fork_find(timeline, key);

		if ((!fe || !fork_event_activate_seg(fe, growth_lsn, block + 1,
										order_id, admission_seq)) &&
			!fork_meta_legacy)
			return 0;
	}

	page_add_version(timeline, key, block, page_lsn, admission_seq,
					 shard, seg, off);
	if (fork_meta_legacy)
	{
		ForkEnt    *fe = fork_get_or_create(timeline, key);
		uint64_t	l = growth_lsn ? growth_lsn : fe->last_def_lsn;

		if (fork_size_asof_hop(fe, l, admission_seq) < block + 1)
		{
			if (!fork_meta_migrate_failed &&
				fork_meta_persist(timeline, key, l, admission_seq,
							  block + 1, FEV_GROW) != 0)
				fork_meta_migrate_failed = 1;
			fork_event_add(fe, l, admission_seq, block + 1, FEV_GROW);
		}
	}
	else if (!ordered && wal_less)
	{
		ForkEnt    *fe = fork_get_or_create(timeline, key);

		if (!fork_has_growth_at(fe, growth_lsn, block + 1))
			fork_grow_replay(timeline, key, block + 1, growth_lsn,
							 admission_seq);
	}
	else if (!ordered && growth_lsn != 0)
		fork_grow_replay(timeline, key, block + 1, growth_lsn,
						 admission_seq);
	return 1;
}

static int
recover_layer_prefix(uint32_t shard)
{
	Shard	   *s = &g_shards[shard];
	LayerRecoverRec *recs = NULL;
	uint32_t	nrec = 0,
				cap = 0;

	if (!s->flush_watermark_valid)
		return 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];
		PsImgIndexEnt *idx;
		uint32_t	n;
		int			first_covered = -1;

		if (d->kind != PS_LAYER_IMAGE || d->deleting ||
			layer_shard_from_id(d->layer_id) != shard)
			continue;
		if (read_image_index_refreshing(d, &idx, &n) != 0)
			goto fail;
		for (uint32_t j = 0; j < n; j++)
		{
			int			covered;

			if (!(idx[j].flags & PS_IMG_REC_SEG_VALID))
				continue;
			covered = idx[j].seg_id < s->flush_watermark.seg_id ||
				(idx[j].seg_id == s->flush_watermark.seg_id &&
				 idx[j].seg_off <= s->flush_watermark.seg_off &&
				 page_size <= s->flush_watermark.seg_off - idx[j].seg_off);
			if (!covered)
				continue;
			if (first_covered < 0)
				first_covered = (int) j;
			if (nrec == cap)
			{
				uint32_t	nc = cap ? cap * 2 : 256;
				LayerRecoverRec *nr = realloc(recs, (size_t) nc * sizeof(*nr));

				if (!nr)
				{
					free(idx);
					goto fail;
				}
				recs = nr;
				cap = nc;
			}
			recs[nrec].timeline = d->timeline;
			recs[nrec].layer_id = d->layer_id;
			recs[nrec].ent = idx[j];
			nrec++;
		}
		if (first_covered >= 0 && !d->data_verified)
		{
			unsigned char *verify = malloc(page_size);
			uint64_t	verified_lsn;

			if (!verify ||
				ps_image_layer_lookup(d, &idx[first_covered].key,
								  idx[first_covered].block,
								  idx[first_covered].lsn, 0, verify, page_size,
								  &verified_lsn, NULL) != 1)
			{
				free(verify);
				free(idx);
				goto fail;
			}
			free(verify);
		}
		free(idx);
		/*
		 * Recovery needs only the index entries already copied above.  Reclaim a
		 * remote-only layer's materialized cache only after revalidating the
		 * object: if the object disappeared or rotted while the daemon was down,
		 * the canonical cache may be the only readable copy left.
		 */
		if (tier_local_location(d) == NULL &&
			ps_layer_store->verify_remote_layer != NULL &&
			ps_layer_store->verify_remote_layer(d) == 0 &&
			ps_layer_store->delete_local_layer(d) != 0)
			goto fail;
	}

	if (nrec == 0)
		goto fail;
	qsort(recs, nrec, sizeof(*recs), layer_recover_cmp);
	for (uint32_t i = 0; i < nrec; i++)
	{
		PsImgIndexEnt *e = &recs[i].ent;

		/* Partial flush retries and compaction can duplicate a source record. */
		if (i + 1 < nrec && e->seg_id == recs[i + 1].ent.seg_id &&
			e->seg_off == recs[i + 1].ent.seg_off)
			continue;
		if (!replay_page_record(recs[i].timeline, &e->key, e->block, e->lsn,
								e->admission_seq, e->growth_lsn, e->order_id,
								e->flags,
								shard, -1, 0))
			goto fail;
	}
	free(recs);
	return 0;

fail:
	free(recs);
	return -1;
}

/* Scan only the segment suffix not covered by the durable layer watermark. */
static int
recover(uint32_t shard)
{
	Shard	   *s = &g_shards[shard];
	int			first = s->flush_watermark_valid ?
		(int) s->flush_watermark.seg_id : 0;
	unsigned char *page = NULL;

	s->cur_seg = s->flush_watermark_valid ? first : -1;
	s->cur_off = s->flush_watermark_valid ? s->flush_watermark.seg_off : 0;
	if (s->memtable)
	{
		page = malloc(page_size);
		if (!page)
			return -1;
	}

	for (int id = first;; id++)
	{
		uint64_t	off = (id == first && s->flush_watermark_valid) ?
			s->flush_watermark.seg_off : 0;
		int64_t		seg_bytes = ps_storage->seg_size(shard, id);
		int			retire_segment = 0;

		if (seg_bytes < 0)
			break;
		if ((uint64_t) seg_bytes < off)
			goto fail;
		for (;;)
		{
			SegRecHdr	hdr;
			uint64_t	header_size = sizeof(SegRecHdr);
			uint64_t	order_id = 0;
			uint64_t	admission_seq = 0;
			uint64_t	data_off;
			uint64_t	page_version;
			uint32_t	flags = PS_IMG_REC_SEG_VALID;
			int			bound;
			int			has_admission;
			int			ordered;
			int			wal_less;

			if (ps_storage->seg_read(shard, id, off, &hdr, sizeof(hdr)) != 0 ||
				hdr.magic == 0)
				break;
			wal_less = hdr.magic == SEG_WALLESS_MAGIC ||
				hdr.magic == SEG_WALLESS_ORDERED_MAGIC ||
				hdr.magic == SEG_WALLESS_BOUND_MAGIC ||
				hdr.magic == SEG_WALLESS_ADMISSION_MAGIC;
			bound = hdr.magic == SEG_WALLESS_BOUND_MAGIC ||
				hdr.magic == SEG_CLAMPED_BOUND_MAGIC ||
				hdr.magic == SEG_WALLESS_ADMISSION_MAGIC ||
				hdr.magic == SEG_CLAMPED_ADMISSION_MAGIC;
			has_admission = hdr.magic == SEG_ADMISSION_MAGIC ||
				hdr.magic == SEG_WALLESS_ADMISSION_MAGIC ||
				hdr.magic == SEG_CLAMPED_ADMISSION_MAGIC;
			ordered = hdr.magic == SEG_WALLESS_ORDERED_MAGIC ||
				hdr.magic == SEG_CLAMPED_ORDERED_MAGIC || bound;
			if (hdr.magic != SEG_MAGIC && hdr.magic != SEG_ADMISSION_MAGIC &&
				!wal_less && !ordered)
			{
				fprintf(stderr, "pagestore_daemon: shard %u segment %d: incompatible "
						"record magic %#x at offset %llu\n", shard, id, hdr.magic,
						(unsigned long long) off);
				goto fail;
			}
			if (hdr.len != page_size)
				break;
			if (bound)
			{
				header_size = has_admission ? sizeof(SegRecHdrBoundAdmission) :
					sizeof(SegRecHdrBound);
				if (ps_storage->seg_read(shard, id, off + sizeof(hdr),
										 &order_id, sizeof(order_id)) != 0 ||
					order_id == 0)
					break;
			}
			else if (has_admission)
				header_size = sizeof(SegRecHdrAdmission);
			if (has_admission &&
				ps_storage->seg_read(shard, id,
									 off + header_size - sizeof(admission_seq),
									 &admission_seq,
									 sizeof(admission_seq)) != 0)
				break;
			if (has_admission && admission_seq == 0)
				break;
			if (seg_bytes < (int64_t) (off + header_size + hdr.len))
				break;
			data_off = off + header_size;
			if (ordered)
				flags |= PS_IMG_REC_ORDERED;
			if (wal_less)
				flags |= PS_IMG_REC_WALLESS;
			/* A legacy scan durably converts missing marker semantics into
			 * ordinary fork-growth events before sealing the migration. */
			if (fork_meta_legacy)
				flags &= ~PS_IMG_REC_ORDERED;
			page_version = wal_less ? 0 : hdr.lsn;
			if (!replay_page_record(hdr.timeline, &hdr.key, hdr.block,
								page_version, admission_seq, hdr.lsn, order_id,
								flags,
								shard, id, data_off))
			{
				retire_segment = 1;
				break;
			}
			if (s->memtable)
			{
				if (ps_storage->seg_read(shard, id, data_off, page, page_size) != 0 ||
					ps_memtable_put(s->memtable, hdr.timeline, &hdr.key, hdr.block,
								page_version, page, admission_seq, hdr.lsn,
								order_id,
								(uint32_t) id, data_off, flags) != 0)
					goto fail;
			}
			off += header_size + hdr.len;
			if (s->memtable && ps_memtable_full(s->memtable) &&
				flush_memtable(s, (uint32_t) id, off) != 0)
				goto fail;
		}
		s->cur_seg = id;
		s->cur_off = retire_segment ? segment_size : off;
	}
	if (s->memtable && ps_memtable_count(s->memtable) > 0 &&
		flush_memtable(s, (uint32_t) s->cur_seg, s->cur_off) != 0)
		goto fail;
	free(page);
	if (s->cur_seg >= 0)
		fprintf(stderr, "pagestore_daemon: recovered shard %u through segment %d (off %llu)\n",
				shard, s->cur_seg, (unsigned long long) s->cur_off);
	return 0;

fail:
	free(page);
	return -1;
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
	uint64_t	newest;

	if (req_lsn != 0)
		return req_lsn;
	newest = e->nev ? e->ev[e->nev - 1].lsn : 0;
	if (e->last_page_lsn > newest)
		newest = e->last_page_lsn;
	return newest == UINT64_MAX ? UINT64_MAX : newest + 1;
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
			 * its WAL position; see fork_op_lsn for a legacy 0).  Keep the
			 * empty-generation boundary even if an older live generation still
			 * appears current: its UNLINK may arrive later during replay.
			 */
			{
				ForkEnt    *e = fork_get_or_create(tl, &ch->key);
				uint64_t	lsn;
				uint64_t	seq;
				int			delayed;

				/* Object writers issue CREATE as a preparatory operation before
				 * every write.  Unlike a relation WAL CREATE, an unstamped retry
				 * must preserve the existing fork and its block count. */
				if (ch->req_lsn == 0 && ch->key.klass != PS_KLASS_RELATION &&
					fork_exists_through(tl, &ch->key, UINT64_MAX, 0))
					break;

				lsn = fork_op_lsn(e, ch->req_lsn);
				/* XLogReadBufferExtended() asks smgr to ensure the relation fork
				 * exists before applying every redo record.  That call has the
				 * record's LSN but is not a relation-creation record: if the fork
				 * already exists at this replay position, recording FEV_SET would
				 * manufacture a zero-block generation and hide older pages. */
				if (ch->is_redo == 2 && ch->key.klass == PS_KLASS_RELATION &&
					fork_exists_through(tl, &ch->key, lsn, 0))
					break;
				seq = admission_seq_alloc();
				delayed = fork_event_precedes_known_state(e, lsn, seq);

				if (seq == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (!fork_has_create_at(e, lsn))
				{
					if (fork_meta_persist(tl, &ch->key, lsn, seq, 0, FEV_SET) != 0)
						ch->status = PS_STATUS_ERROR;
					else
					{
						fork_event_add(e, lsn, seq, 0, FEV_SET);
						if (delayed)
							fork_restore_later_page_growth(tl, &ch->key, lsn, seq);
						ch->req_seq = seq;
					}
				}
			}
			break;

		case PS_OP_EXISTS:
			/* req_lsn caps the horizon; 0 = newest (the writer path) */
			if (ch->req_seq != 0 && ch->key.klass == PS_KLASS_RELATION &&
				fork_has_wal_less_page(tl, &ch->key))
			{
				ch->status = PS_STATUS_ERROR;
				break;
			}
			ch->result = fork_exists_through(tl, &ch->key,
										 ch->req_lsn ? ch->req_lsn : UINT64_MAX,
										 ch->req_seq) ? 1 : 0;
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
				uint64_t	seq = admission_seq_alloc();
				int			delayed = fork_event_precedes_known_state(e, lsn, seq);

				if (seq == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (fork_meta_persist(tl, &ch->key, lsn, seq, 0, FEV_DEAD) != 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					fork_event_add(e, lsn, seq, 0, FEV_DEAD);
					if (delayed)
						fork_restore_later_page_growth(tl, &ch->key, lsn, seq);
					ch->req_seq = seq;
				}
			}
			break;

		case PS_OP_NBLOCKS:
			/* req_lsn caps the horizon; 0 = newest (the writer path) */
			if (ch->req_seq != 0 && ch->key.klass == PS_KLASS_RELATION &&
				fork_has_wal_less_page(tl, &ch->key))
			{
				ch->status = PS_STATUS_ERROR;
				break;
			}
			ch->result = ch->is_redo ?
				fork_nblocks_recovery(tl, &ch->key,
					ch->req_lsn ? ch->req_lsn : UINT64_MAX) :
				fork_nblocks_through(tl, &ch->key,
										  ch->req_lsn ? ch->req_lsn : UINT64_MAX,
										  ch->req_seq);
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
				uint64_t	seq = admission_seq_alloc();
				int			delayed = fork_event_precedes_known_state(e, lsn, seq);

				if (seq == 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
				if (fork_meta_persist(tl, &ch->key, lsn, seq, ch->nblocks,
								  FEV_SET) != 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					fork_event_add(e, lsn, seq, ch->nblocks, FEV_SET);
					if (delayed)
						fork_restore_later_page_growth(tl, &ch->key, lsn, seq);
					ch->req_seq = seq;
				}
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
								 ch->req_lsn) &&
				(timelines[ch->timeline].defined ||
				 branch_frontiers_allow((int) ch->parent_timeline,
								ch->req_lsn)))
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
								 ch->req_lsn) &&
				(timelines[ch->timeline].defined ||
				 branch_frontiers_allow((int) ch->parent_timeline,
								ch->req_lsn)))
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
		case PS_OP_TIMELINE_INFO:
			if (tl >= MAX_TIMELINES || !timelines[tl].defined)
				ch->status = PS_STATUS_ERROR;
			else if (timeline_has_parent(tl))
			{
				ch->result = 1;
				ch->parent_timeline = (uint32_t) timelines[tl].parent;
				ch->req_lsn = timelines[tl].branch_lsn;
			}
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
			if (walidx_add(tl, &ch->key, ch->blocknum, ch->req_lsn) != 0)
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_WAL_INDEX_ADD_BATCH:
			if (ch->nblocks == 0 ||
				ch->datalen % sizeof(PsWalIndexEntry) != 0 ||
				ch->nblocks != ch->datalen / sizeof(PsWalIndexEntry) ||
				ch->datalen > PS_IO_UNIT)
				ch->status = PS_STATUS_ERROR;
			else
			{
				PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;
				uint32_t shard = ps_shard_of(&ch->key);

				pthread_rwlock_rdlock(&walidx_publish_lock);
				if (ps_shard_of(&entries[0].key) != shard ||
					walidx_add_batch_locked(tl, entries, ch->nblocks) != 0)
					ch->status = PS_STATUS_ERROR;
				pthread_rwlock_unlock(&walidx_publish_lock);
			}
			break;

		case PS_OP_WAL_INDEX_GET:
			{
				int max_out = (int) (PS_IO_UNIT / sizeof(PsWalRec));
				int n;

				if (ch->nblocks > 0 && ch->nblocks < (uint32_t) max_out)
					max_out = (int) ch->nblocks;
				n = walidx_get(tl, &ch->key, ch->blocknum, ch->req_lsn,
								   ch->pad1 != 0, ch->req_seq, ch->parent_timeline,
								   (PsWalRec *) ch->data, max_out);

				if (n < 0)
					ch->status = PS_STATUS_ERROR;
				else
					ch->result = (uint32_t) n;
			}
			break;

		case PS_OP_WAL_INDEX_PROGRESS:
			if (tl >= MAX_TIMELINES)
				ch->status = PS_STATUS_ERROR;
			else if (ch->req_lsn == 0 && ch->req_seq == 0)
			{
				uint64_t progress = walidx_progress_read(tl);
				uint64_t first = progress == 0 ? wal_log_start(tl) : progress;

				ch->req_lsn = first == UINT64_MAX ? 0 : first;
			}
			else if (walidx_commit(tl, ch->req_lsn, ch->req_seq) != 0)
				ch->status = PS_STATUS_ERROR;
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

		case PS_OP_RETENTION_PIN_SET:
			{
				PsRetentionPin pin;
				PsRetentionPin old_pin;
				int			ret;
				int			old_found;
				int			timeline_defined;

				memset(&pin, 0, sizeof(pin));
				pin.timeline = tl;
				pin.owner_kind = ch->blocknum;
				pin.resources = ch->parent_timeline;
				pin.generation = ch->old_nblocks;
				pin.owner_id = ch->req_seq;
				pin.lsn = ch->req_lsn;
				pin.admission_seq = (uint64_t) ch->nblocks |
					(uint64_t) ch->pad1 << 32;
				/* Generation zero exists only for replaying pre-v27 retention
				 * records.  It is never valid on the current IPC boundary. */
				if (ch->old_nblocks == 0 || tl >= MAX_TIMELINES)
					ret = PS_RETENTION_ERROR;
				else
				{
					/* A retried controller fence may be newer than this
					 * process's recovered allocator.  Serialize its durable SET
					 * with mutations and advance allocation before admitting more. */
					pthread_rwlock_wrlock(&admission_lock);
					pthread_rwlock_wrlock(&page_prune_lock);
					old_found = ps_retention_lookup(tl, pin.owner_kind,
						pin.owner_id, &old_pin);
					ps_lock_map_rd();
					timeline_defined = timelines[tl].defined;
					ps_unlock_map();
					/* An active owner can only advance its own horizon.  Its old
					 * pin already protected all history needed by the newer view, so
					 * permit the atomic handoff even if a previously published global
					 * frontier is stricter than the new point. */
					ret = (((pin.resources &
							  PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0 &&
							 !page_frontier_allows(tl, pin.lsn,
												  pin.admission_seq) &&
							 !(old_found == 1 &&
							   old_pin.generation == pin.generation &&
							   old_pin.resources == pin.resources &&
							   (old_pin.lsn < pin.lsn ||
								/* An exact retry cannot expose a new fence. */
								(old_pin.lsn == pin.lsn &&
								 old_pin.admission_seq == pin.admission_seq)))) ||
							!timeline_defined) ?
						PS_RETENTION_ERROR : ps_retention_set(&pin);
					if (ret == PS_RETENTION_OK)
					{
						admission_seq_observe(pin.admission_seq);
						if ((old_found != 1 ||
							 memcmp(&old_pin, &pin, sizeof(pin)) != 0) &&
							(((old_found == 1 ? old_pin.resources : 0) |
							  pin.resources) &
							 PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
							page_prune_mark_all_due();
					}
					pthread_rwlock_unlock(&page_prune_lock);
					pthread_rwlock_unlock(&admission_lock);
				}
				if (ret == PS_RETENTION_STALE)
					ch->status = PS_STATUS_STALE;
				else if (ret != PS_RETENTION_OK)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_RETENTION_PIN_RESERVE:
			{
				PsRetentionPin pin;
				PsRetentionPin old_pin;
				uint64_t	seq;
				int			ret = PS_RETENTION_ERROR;
				int			old_found = 0;
				int			timeline_defined = 0;

				memset(&pin, 0, sizeof(pin));
				pin.timeline = tl;
				pin.owner_kind = ch->blocknum;
				pin.resources = ch->parent_timeline;
				pin.generation = ch->old_nblocks;
				pin.owner_id = ch->req_seq;
				pin.lsn = ch->req_lsn;
				pthread_rwlock_wrlock(&admission_lock);
				seq = admission_seq_alloc();
				pin.admission_seq = seq;
				pthread_rwlock_wrlock(&page_prune_lock);
				if (seq != 0 && ch->old_nblocks != 0 && tl < MAX_TIMELINES)
				{
					old_found = ps_retention_lookup(tl, pin.owner_kind,
						pin.owner_id, &old_pin);
					ps_lock_map_rd();
					timeline_defined = timelines[tl].defined;
					ps_unlock_map();
					if (timeline_defined &&
						(((pin.resources &
						   PS_RETENTION_RESOURCE_PAGE_HISTORY) == 0) ||
						 page_frontier_allows(tl, pin.lsn, pin.admission_seq)))
						ret = ps_retention_reserve_and_set(&pin);
				}
				if (ret == PS_RETENTION_OK)
				{
					memcpy(ch->data, &seq, sizeof(seq));
					ch->datalen = sizeof(seq);
					if ((old_found != 1 ||
						 memcmp(&old_pin, &pin, sizeof(pin)) != 0) &&
						(((old_found == 1 ? old_pin.resources : 0) |
						  pin.resources) &
						 PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
						page_prune_mark_all_due();
				}
				pthread_rwlock_unlock(&page_prune_lock);
				pthread_rwlock_unlock(&admission_lock);
				if (ret == PS_RETENTION_STALE)
					ch->status = PS_STATUS_STALE;
				else if (ret != PS_RETENTION_OK)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_RETENTION_PIN_DROP:
			{
				PsRetentionPin old_pin;
				int			ret;
				int			old_found;
				int			timeline_defined;

				pthread_rwlock_wrlock(&page_prune_lock);
				old_found = ps_retention_lookup(tl, ch->blocknum,
											ch->req_seq, &old_pin);
				ps_lock_map_rd();
				timeline_defined = tl < MAX_TIMELINES && timelines[tl].defined;
				ps_unlock_map();
				ret = (ch->old_nblocks == 0 || tl >= MAX_TIMELINES ||
					   !timeline_defined) ?
					PS_RETENTION_ERROR :
					ps_retention_drop(tl, ch->blocknum, ch->req_seq,
									  ch->old_nblocks);
				if (ret == PS_RETENTION_OK && old_found == 1 &&
					(old_pin.resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0)
					page_prune_mark_all_due();
				pthread_rwlock_unlock(&page_prune_lock);
				if (ret == PS_RETENTION_STALE)
					ch->status = PS_STATUS_STALE;
				else if (ret != PS_RETENTION_OK)
					ch->status = PS_STATUS_ERROR;
			}
			break;

		case PS_OP_RETENTION_PIN_GET:
			{
				PsRetentionPin pin;
				PsRetentionGetResult result;
				uint32_t	count = 0;
				uint64_t	epoch = ch->req_lsn;
				int			found = ps_retention_get_consistent(ch->blocknum,
														   &epoch, &pin, &count);

				if (found == PS_RETENTION_STALE)
				{
					ch->status = PS_STATUS_STALE;
					ch->req_lsn = epoch;
				}
				else if (found < 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					ch->nblocks = count;
					memset(&result, 0, sizeof(result));
					result.mutation_epoch = epoch;
					if (found)
					{
						ch->result = 1;
						ch->timeline = pin.timeline;
						ch->blocknum = pin.owner_kind;
						ch->parent_timeline = pin.resources;
						ch->old_nblocks = pin.generation;
						ch->req_seq = pin.owner_id;
						ch->req_lsn = pin.lsn;
						result.admission_seq = pin.admission_seq;
					}
					memcpy(ch->data, &result, sizeof(result));
					ch->datalen = sizeof(result);
				}
			}
			break;

		case PS_OP_RETENTION_PIN_LOOKUP:
			{
				PsRetentionPin pin;
				int			found = ps_retention_lookup(tl, ch->blocknum,
												 ch->req_seq, &pin);

				if (found < 0)
					ch->status = PS_STATUS_ERROR;
				else
				{
					ch->result = found != 0;
					if (found)
					{
						ch->timeline = pin.timeline;
						ch->blocknum = pin.owner_kind;
						ch->parent_timeline = pin.resources;
						ch->old_nblocks = pin.generation;
						ch->req_seq = pin.owner_id;
						ch->req_lsn = pin.lsn;
						memcpy(ch->data, &pin.admission_seq,
							   sizeof(pin.admission_seq));
						ch->datalen = sizeof(pin.admission_seq);
					}
				}
			}
			break;

		case PS_OP_RETENTION_FLOOR:
			{
				uint64_t	floor = 0;

				if (retention_effective_floor(tl, ch->parent_timeline,
										  &floor) != 0)
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

/* Flush the memtable, commit its coverage watermark, and close the manifest. */
void
ps_core_close(void)
{
	uint32_t	ns = core_shards();
	int		join_gc = 0;
	int		cancel_upload = 0;
	int		join_evict = 0;
	int		evict_state;
	struct timespec deadline;
	int		join_rc;

	if (__atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE) != 0)
	{
		if (__atomic_load_n(&gc_remote_state, __ATOMIC_ACQUIRE) == 1)
			pthread_cancel(gc_remote_thread);
		join_gc = 1;
	}
	if (__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 1)
	{
		/* Remote tiering is optional: do not let a stalled object store delay
		 * shutdown of the locally durable store.  The interrupted copy is
		 * crash-safe and a later open reclaims its temporary file. */
		pthread_cancel(tier_upload_thread);
		cancel_upload = 1;
	}
	else if (__atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE) == 2)
	{
		pthread_join(tier_upload_thread, NULL);
		tier_upload_joined = 1;
		/* Persist a completed upload before closing its manifest. */
		tier_one_layer();
	}
	evict_state = __atomic_load_n(&evict_local_state, __ATOMIC_ACQUIRE);
	if (evict_state != 0)
	{
		if (evict_state == 1)
			pthread_cancel(evict_local_thread);
		join_evict = 1;
	}

	/*
	 * The uncovered segment tail must be durable before shutdown (writes between
	 * checkpoints are otherwise only in the OS page cache, and would be lost to a
	 * power failure after the daemon exits even though the write was acknowledged).
	 *
	 * Sync before flushing below so a failed layer/manifest commit still leaves a
	 * durable segment fallback.  A sync error means acknowledged tail writes may
	 * not be durable, so abort before destroying the memtables.
	 */
	if (ps_storage->sync && ps_storage->sync() != 0)
	{
		fprintf(stderr, "pagestore_daemon: FATAL: segment sync failed on shutdown "
				"(%s); aborting before teardown -- recently acknowledged writes "
				"may not be durable\n", strerror(errno));
		_exit(EXIT_FAILURE);
	}
	if (join_gc)
	{
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec++;
		if (pthread_timedjoin_np(gc_remote_thread, NULL, &deadline) != 0)
		{
			fprintf(stderr, "pagestore_daemon: FATAL: remote GC did not stop during shutdown\n");
			_exit(EXIT_FAILURE);
		}
		__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
	}

	for (uint32_t i = 0; i < ns; i++)
	{
		Shard	   *s = &g_shards[i];

		if (s->memtable)
		{
			flush_memtable(s, (uint32_t) s->cur_seg, s->cur_off);
			ps_memtable_destroy(s->memtable);
			s->memtable = NULL;
		}
	}

	ps_pgcache_free();
	if (cancel_upload)
	{
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec++;
		join_rc = pthread_timedjoin_np(tier_upload_thread, NULL, &deadline);
		if (join_rc != 0)
		{
			/* Do not detach an upload that still owns core/provider state.  The
			 * local store is synced above; terminate the process so the kernel
			 * reclaims the stuck worker rather than permitting a concurrent reopen. */
			fprintf(stderr, "pagestore_daemon: FATAL: tier upload did not stop during shutdown\n");
			_exit(EXIT_FAILURE);
		}
		__atomic_store_n(&tier_upload_state, 0, __ATOMIC_RELEASE);
	}
	if (join_evict)
	{
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec++;
		join_rc = pthread_timedjoin_np(evict_local_thread, NULL, &deadline);
		if (join_rc != 0)
		{
			fprintf(stderr, "pagestore_daemon: FATAL: local eviction verifier did not stop during shutdown\n");
			_exit(EXIT_FAILURE);
		}
	}
	__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
	ps_retention_close();
	ps_manifest_close();
}

static int
segment_has_references(uint32_t source_shard, uint32_t victim)
{
	uint32_t	ns = core_shards();

	for (uint32_t sh = 0; sh < ns; sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (PageEnt *e = g_shards[sh].page_idx[bucket]; e; e = e->next)
				for (int i = 0; i < e->nver; i++)
					if (e->vers[i].shard == source_shard &&
						e->vers[i].seg == (int) victim)
						return 1;
	return 0;
}

static int
prepare_segment_layers(uint32_t source_shard, uint32_t victim)
{
	PsLayerDesc *layers = NULL;
	uint32_t	nlayers = 0;
	int			rc = 0;

	ps_lock_map_rd();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];

		if (d->kind == PS_LAYER_IMAGE && !d->deleting &&
			layer_shard_from_id(d->layer_id) == source_shard)
			nlayers++;
	}
	if (nlayers != 0)
	{
		layers = malloc((size_t) nlayers * sizeof(*layers));
		if (layers == NULL)
		{
			ps_unlock_map();
			return -1;
		}
		nlayers = 0;
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		{
			PsLayerDesc *d = &ps_layer_map.layers[i];

			if (d->kind == PS_LAYER_IMAGE && !d->deleting &&
				layer_shard_from_id(d->layer_id) == source_shard)
				layers[nlayers++] = *d;
		}
	}
	ps_unlock_map();

	for (uint32_t i = 0; i < nlayers; i++)
	{
		PsImgIndexEnt *idx;
		uint32_t	n;
		int			covers = 0;

		if (read_image_index_refreshing(&layers[i], &idx, &n) != 0)
		{
			rc = -1;
			break;
		}
		for (uint32_t j = 0; j < n; j++)
			if ((idx[j].flags & PS_IMG_REC_SEG_VALID) &&
				idx[j].seg_id == victim)
			{
				covers = 1;
				break;
			}
		free(idx);
		if (covers && verify_image_layer_refreshing(&layers[i]) != 0)
		{
			rc = -1;
			break;
		}
	}
	free(layers);
	return rc;
}

static int
verify_segment_layers_locked(uint32_t source_shard, uint32_t victim, int need_layer)
{
	int			found = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		PsLayerDesc *d = &ps_layer_map.layers[i];
		PsImgIndexEnt *idx;
		uint32_t	n;
		int			covers = 0;

		if (d->kind != PS_LAYER_IMAGE || d->deleting ||
			layer_shard_from_id(d->layer_id) != source_shard)
			continue;
		if (ps_image_layer_read_index(d, &idx, &n) != 0)
			return -1;
		for (uint32_t j = 0; j < n; j++)
			if ((idx[j].flags & PS_IMG_REC_SEG_VALID) &&
				idx[j].seg_id == victim)
			{
				covers = 1;
				break;
			}
		free(idx);
		if (covers)
		{
			found = 1;
			/* Always re-read and checksum now: a prior read's cached verification
			 * may predate corruption that occurred before this unlink. */
			if (ps_image_layer_verify_data(d, page_size) != 0)
				return -1;
		}
		/* Keep any remote-only cache materialized while examining this segment.
		 * Segment GC advances one victim at a time, and dropping it here makes
		 * every later victim download the same verified layer again while all
		 * shard write locks are held.  Idle eviction owns eventual cache cleanup. */
	}
	return need_layer && !found ? -1 : 0;
}

static int
reclaim_one_segment(Shard *s)
{
	uint32_t	victim;
	uint32_t	ns = core_shards();
	int			refs;

	if (!s->flush_watermark_valid || !ps_storage->seg_remove ||
		s->gc_next_seg >= s->flush_watermark.seg_id)
		return 0;
	victim = s->gc_next_seg;
	refs = segment_has_references(s->id, victim);
	if (verify_segment_layers_locked(s->id, victim, refs) != 0)
		return 0;
	for (uint32_t sh = 0; sh < ns; sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (PageEnt *e = g_shards[sh].page_idx[bucket]; e; e = e->next)
				for (int i = 0; i < e->nver; i++)
					if (e->vers[i].shard == s->id &&
						e->vers[i].seg == (int) victim)
					{
						e->vers[i].seg = -1;
						e->vers[i].off = 0;
					}
	if (ps_storage->seg_remove(s->id, (int) victim) != 0)
		return 0;
	s->gc_next_seg++;
	return 1;
}

static const PsLayerLocation *
tier_local_location(const PsLayerDesc *layer)
{
	for (uint32_t i = 0; i < layer->location_count; i++)
		if ((layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			 layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD) &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

static const PsLayerLocation *
tier_remote_location(const PsLayerDesc *layer)
{
	for (uint32_t i = 0; i < layer->location_count; i++)
		if (layer->locations[i].tier == PS_LAYER_TIER_REMOTE_OBJECT &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

static int
refresh_remote_only_layer(const PsLayerDesc *layer)
{
	if (tier_local_location(layer) != NULL ||
		tier_remote_location(layer) == NULL ||
		ps_layer_store->refresh_layer_cache == NULL)
		return -1;
	return ps_layer_store->refresh_layer_cache(layer);
}

static int
read_image_index_refreshing(const PsLayerDesc *layer, PsImgIndexEnt **idx,
							uint32_t *n)
{
	if (ps_image_layer_read_index(layer, idx, n) == 0)
		return 0;
	if (refresh_remote_only_layer(layer) != 0)
		return -1;
	return ps_image_layer_read_index(layer, idx, n);
}

static int
read_layer_block_refreshing(const PsLayerDesc *layer, uint64_t off,
							void *buf, uint32_t len)
{
	if (ps_layer_store->read_layer_block(layer, off, buf, len) == 0)
		return 0;
	if (refresh_remote_only_layer(layer) != 0)
		return -1;
	return ps_layer_store->read_layer_block(layer, off, buf, len);
}

static int
verify_image_layer_refreshing(const PsLayerDesc *layer)
{
	if (ps_image_layer_verify_data(layer, page_size) == 0)
		return 0;
	if (refresh_remote_only_layer(layer) != 0)
		return -1;
	return ps_image_layer_verify_data(layer, page_size);
}

static void *
tier_upload_worker(void *arg)
{
	PsLayerDesc *layer = arg;
	int			rc;

	/* Shutdown cancels optional object copies rather than waiting for a slow
	 * object mount.  This worker owns no locks or shared allocations. */
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	rc = ps_layer_store->upload_layer(layer);

	__atomic_store_n(&tier_upload_state, rc == 0 ? 2 : 3, __ATOMIC_RELEASE);
	return NULL;
}

/* Start or finish one immutable-layer upload without blocking a shard worker. */
static int
tier_one_layer(void)
{
	PsLayerDesc candidate;
	PsLayerDesc *current = NULL;
	PsLayerLocation remote;
	const PsLayerLocation *local;
	struct timespec now;
	int			state;
	int			found = 0;

	if (ps_layer_store->remote_uri == NULL || ps_layer_store->upload_layer == NULL)
		return 0;
	/* The local provider is always installed, but exposes remote callbacks even
	 * when PAGESTORE_OBJECT_DIR is disabled.  Probe configuration before
	 * scheduling a worker so local-only stores do not spin on ENOTSUP uploads. */
	if (ps_layer_store->remote_uri(0, remote.uri, sizeof(remote.uri)) != 0)
		return 0;
	clock_gettime(CLOCK_MONOTONIC, &now);
	if (tier_upload_retry_at.tv_sec != 0 &&
		(now.tv_sec < tier_upload_retry_at.tv_sec ||
		 (now.tv_sec == tier_upload_retry_at.tv_sec &&
		  now.tv_nsec < tier_upload_retry_at.tv_nsec)))
		return 0;
	state = __atomic_load_n(&tier_upload_state, __ATOMIC_ACQUIRE);
	if (state == 1)
		return 0;
	if (state != 0)
	{
		if (!tier_upload_joined)
			pthread_join(tier_upload_thread, NULL);
		tier_upload_joined = 0;
		__atomic_store_n(&tier_upload_state, 0, __ATOMIC_RELEASE);
		if (state != 2)
		{
			clock_gettime(CLOCK_MONOTONIC, &tier_upload_retry_at);
			tier_upload_retry_at.tv_sec++;
			return 0;
		}
		memset(&tier_upload_retry_at, 0, sizeof(tier_upload_retry_at));
		candidate = tier_upload_candidate;
		goto finish_upload;
	}
	ps_lock_map_rd();
	for (uint32_t pass = 0; pass < core_shards() && !found; pass++)
	{
		uint32_t shard = (tier_upload_shard_cursor + pass) % core_shards();

		for (uint32_t phase = 0; phase < 2 && !found; phase++)
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		{
			PsLayerDesc *layer = &ps_layer_map.layers[i];

			if (!layer->deleting && !layer->remote_durable &&
				tier_local_location(layer) != NULL &&
				layer_shard_from_id(layer->layer_id) == shard &&
				(tier_remote_location(layer) == NULL ||
				 (ps_layer_store->remote_uri(layer->layer_id, remote.uri,
										 sizeof(remote.uri)) == 0 &&
				  strcmp(tier_remote_location(layer)->uri, remote.uri) == 0)) &&
				((phase == 0 && layer->layer_id > tier_upload_layer_cursor[shard]) ||
				 (phase == 1 && layer->layer_id <= tier_upload_layer_cursor[shard])))
			{
				candidate = *layer;
				found = 1;
				break;
			}
		}
	}
	ps_unlock_map();
	if (!found)
		return 0;

	tier_upload_shard_cursor = (layer_shard_from_id(candidate.layer_id) + 1) % core_shards();
	tier_upload_layer_cursor[layer_shard_from_id(candidate.layer_id)] = candidate.layer_id;
	tier_upload_candidate = candidate;
	__atomic_store_n(&tier_upload_state, 1, __ATOMIC_RELEASE);
	if (pthread_create(&tier_upload_thread, NULL, tier_upload_worker,
					   &tier_upload_candidate) != 0)
	{
		__atomic_store_n(&tier_upload_state, 0, __ATOMIC_RELEASE);
		return 0;
	}
	return 1;


finish_upload:
	ps_lock_map_wr();
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == candidate.layer_id)
		{
			current = &ps_layer_map.layers[i];
			break;
		}
	if (current == NULL || current->deleting)
	{
		ps_unlock_map();
		return 1;
	}
	if (current->remote_durable)
	{
		ps_unlock_map();
		return 1;
	}
	if (tier_remote_location(current) == NULL)
	{
		local = tier_local_location(current);
		memset(&remote, 0, sizeof(remote));
		remote.tier = PS_LAYER_TIER_REMOTE_OBJECT;
		remote.size = local->size;
		remote.available = true;
		if (ps_layer_store->remote_uri(current->layer_id, remote.uri,
										 sizeof(remote.uri)) != 0 ||
			ps_manifest_set_remote_location(current->layer_id, &remote) != 0)
		{
			ps_unlock_map();
			return 0;
		}
	}
	if (ps_manifest_set_remote_durable(current->layer_id, current->lsn_end) != 0)
	{
		ps_unlock_map();
		return 0;
	}
	ps_unlock_map();
	return 1;
}

static void *
evict_local_worker(void *arg)
{
	PsLayerDesc *layer = arg;
	int			rc = -1;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	if (ps_layer_store->verify_remote_layer != NULL)
		rc = ps_layer_store->verify_remote_layer(layer);
	else if (ps_layer_store->layer_exists_remote != NULL &&
			 ps_layer_store->layer_exists_remote(layer) == 1)
		rc = 0;
	__atomic_store_n(&evict_local_state, rc == 0 ? 2 : 3, __ATOMIC_RELEASE);
	return NULL;
}

/* Evict at most one remote-durable local cache file. */
static int
evict_one_layer(void)
{
	PsLayerDesc candidate;
	int			state;
	int			found = 0;
	uint32_t	found_idx = 0;
	uint32_t	map_nlayers = 0;

	if (ps_layer_store->layer_exists_local == NULL ||
		ps_layer_store->delete_local_layer == NULL ||
		(ps_layer_store->verify_remote_layer == NULL &&
		 ps_layer_store->layer_exists_remote == NULL))
		return 0;
	state = __atomic_load_n(&evict_local_state, __ATOMIC_ACQUIRE);
	if (state == 1)
		return 0;
	if (state != 0)
	{
		pthread_join(evict_local_thread, NULL);
		__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
		if (state != 2)
			return 1;
		candidate = evict_local_candidate;
		goto finish_evict;
	}

	ps_lock_map_rd();
	map_nlayers = ps_layer_map.nlayers;
	for (uint32_t pass = 0; pass < map_nlayers; pass++)
	{
		uint32_t	i = (evict_local_map_cursor + pass) % map_nlayers;
		PsLayerDesc *layer = &ps_layer_map.layers[i];

		if (!layer->deleting && layer->remote_durable && !layer->local_pinned &&
			__atomic_load_n(&layer->cache_readers, __ATOMIC_ACQUIRE) == 0 &&
			(layer->local_cleanup_pending ||
			 ps_layer_store->layer_exists_local(layer->layer_id) == 1))
		{
			candidate = *layer;
			found_idx = i;
			found = 1;
			break;
		}
	}
	ps_unlock_map();
	if (!found)
		return 0;
	evict_local_map_cursor = (found_idx + 1) % map_nlayers;
	evict_local_candidate = candidate;
	__atomic_store_n(&evict_local_state, 1, __ATOMIC_RELEASE);
	if (pthread_create(&evict_local_thread, NULL, evict_local_worker,
					   &evict_local_candidate) != 0)
	{
		__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
		return 0;
	}
	return 1;

finish_evict:
	ps_lock_map_wr();
	found = 0;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == candidate.layer_id)
		{
			PsLayerDesc *layer = &ps_layer_map.layers[i];

			if (layer->deleting || !layer->remote_durable || layer->local_pinned ||
				__atomic_load_n(&layer->cache_readers, __ATOMIC_ACQUIRE) != 0 ||
				(!layer->local_cleanup_pending &&
				 ps_layer_store->layer_exists_local(layer->layer_id) != 1))
			{
				ps_unlock_map();
				return 0;
			}
			if (tier_local_location(&ps_layer_map.layers[i]) != NULL &&
				ps_manifest_drop_local(candidate.layer_id) != 0)
			{
				ps_unlock_map();
				return 0;
			}
			/* A later cache refill installs different physical bytes; require
			 * the image data checksum to be verified again before serving it. */
			ps_layer_map.layers[i].data_verified = false;
			ps_layer_map.layers[i].cache_resident = false;
			ps_layer_map.layers[i].local_cleanup_pending = true;
			found = 1;
			break;
		}
	if (!found)
	{
		ps_unlock_map();
		return 0;
	}
	/* Keep the write lock through unlink: layer reads hold the matching read
	 * lock while downloading/opening their cache file. */
	found = (ps_layer_store->delete_local_layer(&candidate) == 0);
	if (found)
		for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
			if (ps_layer_map.layers[i].layer_id == candidate.layer_id)
			{
				ps_layer_map.layers[i].local_cleanup_pending = false;
				break;
			}
	ps_unlock_map();
	return found;
}

/*
 * Off-the-write-path background maintenance: compact one timeline whose image
 * layer count exceeds the (low-water) threshold.  The maintenance controller
 * calls this repeatedly; doing at most one compaction per call lets other
 * maintenance classes make progress.  Returns 1 if it did work (the caller
 * should run another tick), 0 if nothing was due.
 */
int
ps_core_maintenance(void)
{
	uint32_t	ns;
	uint32_t	ftl = 0,
				fsh = 0;
	uint64_t	page_floor = 0;
	int			found = 0;
	int			did = 0;
	int			legacy_compaction = 0;

	/* Pin churn is independent of the layer read path (SPDK uses the same host
	 * metadata), so bound this log before considering LSM-only work. */
	if (ps_retention_should_compact())
		return ps_retention_compact() == 0;

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
	if (gc_remote_one())
		return 1;
	if (tier_one_layer())
		return 1;

	/* Reclaim at most one complete segment.  The boundary segment containing
	 * the watermark stays present because its suffix may not be in a layer. */
	if (segment_gc_enabled && ps_storage->seg_remove)
	{
		for (uint32_t sh = 0; sh < ns; sh++)
		{
			uint32_t	victim = 0;
			int			due;

			/* flush_memtable() publishes the coverage watermark while its
			 * foreground worker holds shard-wr.  Snapshot the candidate under
			 * shard-rd, then release it before layer materialization can perform
			 * remote I/O.  A newer watermark only makes this victim safer. */
			ps_lock_shard_rd(sh);
			due = g_shards[sh].flush_watermark_valid &&
				g_shards[sh].gc_next_seg <
				g_shards[sh].flush_watermark.seg_id;
			if (due)
				victim = g_shards[sh].gc_next_seg;
			ps_unlock_shard(sh);
			if (due)
				prepare_segment_layers(sh, victim);
		}
		for (uint32_t sh = 0; sh < ns; sh++)
			ps_lock_shard_wr(sh);
		ps_lock_map_rd();
		for (uint32_t sh = 0; sh < ns && !found; sh++)
			found = reclaim_one_segment(&g_shards[sh]);
		ps_unlock_map();
		for (uint32_t sh = ns; sh > 0; sh--)
			ps_unlock_shard(sh - 1);
		if (found)
			return 1;
	}
	/*
	 * Phase 1: scan under map read-lock to pick a timeline+shard whose image
	 * layers are due for compaction.  A shared lock here lets reads proceed.
	 */
	ps_lock_map_rd();
	for (uint32_t tl = 0; tl < MAX_TIMELINES && !found; tl++)
		for (uint32_t sh = 0; sh < ns; sh++)
			if ((tl == 0 || timelines[tl].defined) &&
				(count_image_layers(tl, sh) > (uint32_t) compact_layers ||
				 (__atomic_load_n(&page_prune_due[tl][sh], __ATOMIC_ACQUIRE) != 0 &&
				  count_image_layers(tl, sh) > 0)))
			{
				ftl = tl;
				fsh = sh;
				found = 1;
				for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
					if (ps_layer_map.layers[i].kind == PS_LAYER_IMAGE &&
						!ps_layer_map.layers[i].deleting &&
						ps_layer_map.layers[i].timeline == tl &&
						layer_shard_from_id(ps_layer_map.layers[i].layer_id) == sh &&
						ps_layer_map.layers[i].legacy_shard_zero)
						legacy_compaction = 1;
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
		if (materialize_compaction_inputs(ftl, fsh) == 0)
		{
			if (legacy_compaction)
				for (uint32_t sh = 0; sh < ns; sh++)
					ps_lock_shard_wr(sh);
			else
				ps_lock_shard_wr(fsh);
			pthread_rwlock_rdlock(&page_prune_lock);
			ps_lock_map_wr();
			if ((count_image_layers(ftl, fsh) > (uint32_t) compact_layers ||
				 (__atomic_load_n(&page_prune_due[ftl][fsh], __ATOMIC_ACQUIRE) != 0 &&
				  count_image_layers(ftl, fsh) > 0)) &&
				retention_effective_floor_internal(ftl,
					PS_RETENTION_RESOURCE_PAGE_HISTORY, &page_floor, 1) == 0)
			{
				/* Zero disables pruning but still permits a safe layer merge. */
				did = compact_timeline(ftl, fsh, page_floor) > 0;
				if (did)
					__atomic_store_n(&page_prune_due[ftl][fsh], 0,
									 __ATOMIC_RELEASE);
			}
			ps_unlock_map();
			pthread_rwlock_unlock(&page_prune_lock);
			if (legacy_compaction)
				for (uint32_t sh = ns; sh > 0; sh--)
					ps_unlock_shard(sh - 1);
			else
				ps_unlock_shard(fsh);
		}
	}
	/* Keep compaction inputs resident until due compaction has run.  Evicting
	 * first turns routine compaction into remote I/O under map/shard write
	 * locks; segment GC above likewise consumes its caches before this point. */
	if (!found && !did && evict_one_layer())
		return 1;

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
	int			publish_shard_count = 0;

	__atomic_store_n(&next_segment_order_id, 1, __ATOMIC_RELAXED);
	__atomic_store_n(&next_admission_seq, 1, __ATOMIC_RELAXED);
	/* Metadata is rebuilt below; a close/open cycle must not retain branches. */
	memset(timelines, 0, sizeof(timelines));
	map_locks_ready = 0;
	tier_upload_joined = 0;
	memset(&tier_upload_retry_at, 0, sizeof(tier_upload_retry_at));
	memset(tier_upload_layer_cursor, 0, sizeof(tier_upload_layer_cursor));
	__atomic_store_n(&gc_remote_state, 0, __ATOMIC_RELEASE);
	gc_remote_layer_cursor = 0;
	__atomic_store_n(&evict_local_state, 0, __ATOMIC_RELEASE);
	evict_local_map_cursor = 0;

	if (ps_storage->open(store_dir, segment_size) != 0)
		return -1;
	ps_layer_store_set_page_size(page_size);
	if (ps_layer_store->open(store_dir) != 0)
		return -1;
	if (ps_manifest_open(store_dir) != 0)
		return -1;
	if (ps_manifest_replay(&ps_layer_map) != 0)
		return -1;
	if (validate_store_shard_count(store_dir, &publish_shard_count) != 0)
		return -1;
	/* Leave deleting layers for asynchronous maintenance: recovery must not
	 * block on an unavailable remote object that is already excluded from reads. */

	/* initialize per-shard state, locks and layer-id cursors */
	for (uint32_t i = 0; i < ns; i++)
	{
		PsFlushWatermark watermark;

		g_shards[i].id = i;
		g_shards[i].cur_seg = -1;
		g_shards[i].cur_off = 0;
		g_shards[i].gc_next_seg = 0;
		g_shards[i].coverage_broken = 0;
		g_shards[i].flush_watermark_valid = 0;
		if (use_layers && ps_manifest_get_flush_watermark(i, &watermark))
		{
			g_shards[i].flush_watermark = watermark;
			g_shards[i].flush_watermark_valid = 1;
		}
		g_shards[i].next_layer_id = 1;
		pthread_rwlock_init(&shard_locks[i], NULL);
	}
	map_locks_ready = 1;
	/* layer ids continue past the highest one restored per shard */
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
	{
		uint32_t	sh = layer_shard_from_id(ps_layer_map.layers[i].layer_id);
		uint64_t	lid = layer_local_id(ps_layer_map.layers[i].layer_id);

		if (sh < ns && lid + 1 > g_shards[sh].next_layer_id)
			g_shards[sh].next_layer_id = lid + 1;
	}
	if (use_layers && mark_legacy_shard_zero_layers() != 0)
		return -1;

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
	if (load_timelines() != 0)
	{
		fprintf(stderr, "pagestore_core: refusing to open corrupt timelines metadata\n");
		return -1;
	}
	if (ps_retention_open(store_dir) != 0)
		return -1;
	if (page_frontier_load(store_dir) != 0)
	{
		fprintf(stderr, "pagestore: refusing to open corrupt page reclamation frontiers\n");
		return -1;
	}
	{
		uint64_t	admission_highwater;
		uint32_t	npins = 0;

		if (ps_retention_admission_highwater(&admission_highwater) != 0)
			return -1;
		admission_seq_observe(admission_highwater);
		if (ps_retention_count(&npins) != 0)
			return -1;
		for (uint32_t i = 0; i < npins; i++)
		{
			PsRetentionPin pin;

			if (ps_retention_get(i, &pin, NULL) != 1 ||
				pin.timeline >= MAX_TIMELINES ||
				!timelines[pin.timeline].defined)
			{
				fprintf(stderr, "pagestore: retention pin references an undefined timeline\n");
				errno = EILSEQ;
				return -1;
			}
			if (pin.admission_seq != 0)
				admission_seq_observe(pin.admission_seq);
		}
	}

	/*
	 * Definitive fork-size events (create/truncate/unlink/zero-extend) load
	 * before page recovery: the GROW dedup in fork_event_add compares a
	 * grow against the size visible at its own LSN, and with the definitive
	 * events already in place layer/segment replay makes exactly the decisions
	 * the live path made (a regrow after a truncate must be kept even when
	 * it does not exceed the pre-truncate envelope).
	 *
	 * A durable per-shard watermark divides recovery: v3 image-layer metadata
	 * reconstructs the covered prefix in source-segment order, then recover()
	 * scans and materializes only the segment suffix.  Without a watermark (an
	 * old store or SPDK), recover() starts at segment zero.
	 */
	if (load_fork_meta() != 0)
		return -1;

	for (uint32_t sh = 0; sh < ns; sh++)
	{
		if (use_layers && recover_layer_prefix(sh) != 0)
			return -1;
		if (recover(sh) != 0)
			return -1;
	}
	/* Retention mutations may have committed immediately before shutdown.
	 * Conservatively revisit every nonempty layer set after recovery. */
	page_prune_mark_all_due();
	if (use_layers && mark_legacy_shard_zero_layers() != 0)
		return -1;

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
		if (fork_meta_persist(0, &zk, 0, 0, 0, FEV_MIGRATED) != 0)
		{
			fprintf(stderr, "pagestore: could not seal the fork-meta migration\n");
			return -1;
		}
	}

	/* rebuild each timeline's shipped-WAL end LSN from its log */
	for (uint32_t tl = 0; tl < MAX_TIMELINES; tl++)
		if (tl == 0 || timelines[tl].defined)
		{
			if (wal_recover_one(tl) != 0)
				return -1;
			walidx_progress_init(tl, wal_log_start(tl));
			for (uint32_t shard = 0; shard < core_shards(); shard++)
				if (walidx_recover_one(tl, shard) != 0)
					return -1;
		}

	if (publish_shard_count && publish_store_shard_count(store_dir) != 0)
		return -1;

	return 0;
}
