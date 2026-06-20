/*-------------------------------------------------------------------------
 *
 * pagestore_layer.c
 *	  LSM-like immutable layer metadata helpers.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_layer.h"
#include "pagestore_layer_store.h"

void
ps_layer_map_init(PsLayerMap *map)
{
	memset(map, 0, sizeof(*map));
}

void
ps_layer_map_free(PsLayerMap *map)
{
	free(map->layers);
	memset(map, 0, sizeof(*map));
}

int
ps_layer_map_reserve(PsLayerMap *map, uint32_t capacity)
{
	if (capacity > map->capacity)
	{
		uint32_t	newcap = map->capacity ? map->capacity * 2 : 64;
		PsLayerDesc *newlayers;

		while (newcap < capacity)
			newcap *= 2;
		newlayers = realloc(map->layers, (size_t) newcap * sizeof(PsLayerDesc));
		if (newlayers == NULL)
			return -1;
		map->layers = newlayers;
		map->capacity = newcap;
	}
	return 0;
}

int
ps_layer_map_add(PsLayerMap *map, const PsLayerDesc *desc)
{
	if (ps_layer_map_reserve(map, map->nlayers + 1) != 0)
		return -1;
	map->layers[map->nlayers++] = *desc;
	return 0;
}

uint32_t
ps_layer_map_count(const PsLayerMap *map)
{
	return map->nlayers;
}

/* ===================== image layer file format ========================= */

/* FNV-1a checksum for layer integrity (not cryptographic). */
static uint32_t
img_crc(const void *p, size_t n)
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

/* Public wrapper so callers that read page bytes directly (compaction) can
 * verify a page against its per-page index crc, matching what the writer
 * stored. */
uint32_t
ps_image_page_crc(const void *page, uint32_t len)
{
	return img_crc(page, len);
}

static int
key_cmp(const PsKey *a, const PsKey *b)
{
	if (a->spcOid != b->spcOid)
		return a->spcOid < b->spcOid ? -1 : 1;
	if (a->dbOid != b->dbOid)
		return a->dbOid < b->dbOid ? -1 : 1;
	if (a->relNumber != b->relNumber)
		return a->relNumber < b->relNumber ? -1 : 1;
	if (a->forkNum != b->forkNum)
		return a->forkNum < b->forkNum ? -1 : 1;
	return 0;
}

/* ---- per-layer (key,block) bloom filter, in-memory read cache --------------
 * The read planner and layer_map_lookup() probe every image layer of a timeline.
 * A bloom of each layer's (key,block) pairs lets a read answer "definitely
 * absent" for a layer without re-reading its index from disk.  The filter is
 * built from the index the first time a layer is read -- so there is no new
 * on-disk format and no manifest change -- and cached here, direct-mapped by
 * layer_id (ids are monotonic and never reused).  A cache miss costs only the
 * index read we would have done anyway; a hit on a negative skips it.
 *
 * Single-threaded with the daemon's serve loop; per-shard worker threads (a
 * later sharding step) would shard this cache or guard it with a lock.
 */
#define PS_LAYER_BLOOM_BYTES	512			/* 4096 bits per layer */
#define PS_LAYER_BLOOM_BITS		(PS_LAYER_BLOOM_BYTES * 8)
#define PS_LAYER_BLOOM_SLOTS	1024		/* direct-mapped by layer_id */

typedef struct LayerBloom
{
	uint64_t	layer_id;
	int			valid;
	uint8_t		bits[PS_LAYER_BLOOM_BYTES];
} LayerBloom;

static LayerBloom layer_bloom_cache[PS_LAYER_BLOOM_SLOTS];

/* two independent hashes of (key,block) -> bit positions (FNV-1a variants) */
static void
bloom_bits(const PsKey *key, uint32_t block, uint32_t *b1, uint32_t *b2)
{
	uint32_t	v[5];
	uint32_t	h1 = 2166136261u;
	uint32_t	h2 = 2166136261u;

	v[0] = key->spcOid;
	v[1] = key->dbOid;
	v[2] = key->relNumber;
	v[3] = (uint32_t) key->forkNum;
	v[4] = block;
	for (unsigned i = 0; i < 5; i++)
	{
		h1 = (h1 ^ v[i]) * 16777619u;
		h2 = (h2 ^ v[i]) * 2654435761u;
	}
	*b1 = h1 % PS_LAYER_BLOOM_BITS;
	*b2 = h2 % PS_LAYER_BLOOM_BITS;
}

static void
bloom_set(uint8_t *bits, const PsKey *key, uint32_t block)
{
	uint32_t	b1,
				b2;

	bloom_bits(key, block, &b1, &b2);
	bits[b1 >> 3] |= (uint8_t) (1u << (b1 & 7));
	bits[b2 >> 3] |= (uint8_t) (1u << (b2 & 7));
}

/* 1 if (key,block) might be present, 0 if it is definitely absent */
static int
bloom_test(const uint8_t *bits, const PsKey *key, uint32_t block)
{
	uint32_t	b1,
				b2;

	bloom_bits(key, block, &b1, &b2);
	return (bits[b1 >> 3] & (1u << (b1 & 7))) &&
		(bits[b2 >> 3] & (1u << (b2 & 7)));
}

/* the direct-mapped cache slot for a layer; caller checks ->valid && ->layer_id
 * to know whether it already holds THIS layer's bloom */
static LayerBloom *
bloom_slot(uint64_t layer_id)
{
	return &layer_bloom_cache[layer_id % PS_LAYER_BLOOM_SLOTS];
}

/*
 * Drop every cached bloom.  layer_ids are only unique within a single store, so a
 * process that closes one store and opens another (ps_core_close/open) could
 * otherwise hit a slot still holding the previous store's bloom for the same
 * layer_id and wrongly skip a layer.  The open path calls this so each store
 * starts with an empty cache.
 */
void
ps_image_bloom_reset(void)
{
	memset(layer_bloom_cache, 0, sizeof(layer_bloom_cache));
}

/* internal sort record: on-disk index entry plus its source page index */
typedef struct ImgSort
{
	PsImgIndexEnt ent;
	uint32_t	src;
} ImgSort;

static int
img_sort_cmp(const void *pa, const void *pb)
{
	const ImgSort *a = pa;
	const ImgSort *b = pb;
	int			c = key_cmp(&a->ent.key, &b->ent.key);

	if (c)
		return c;
	if (a->ent.block != b->ent.block)
		return a->ent.block < b->ent.block ? -1 : 1;
	if (a->ent.lsn != b->ent.lsn)
		return a->ent.lsn < b->ent.lsn ? -1 : 1;
	return 0;
}

/*
 * Necessary-condition prune: can layer 'd' possibly hold (key, block)?  Uses the
 * layer's key range and global block range (min/max across all its entries).
 * A 0 means definitely-absent, so the caller can skip the layer without reading
 * its footer/index/data.  (block range is coarse -- it is the global min/max,
 * not per-key -- but a block outside it is still definitely absent.)
 */
static int
layer_covers(const PsLayerDesc *d, const PsKey *key, uint32_t block)
{
	if (key_cmp(key, &d->start_key) < 0 || key_cmp(key, &d->end_key) > 0)
		return 0;
	if (block < d->start_block || block > d->end_block)
		return 0;
	return 1;
}

static const PsLayerLocation *
img_local_loc(const PsLayerDesc *layer)
{
	uint32_t	nlocs = layer->location_count;

	if (nlocs > PS_LAYER_MAX_LOCATIONS)
		nlocs = PS_LAYER_MAX_LOCATIONS;
	for (uint32_t i = 0; i < nlocs; i++)
		if ((layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			 layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD) &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

int
ps_image_layer_write(uint64_t layer_id, uint32_t timeline,
					 const PsImgRec *recs, uint32_t n, uint32_t page_size,
					 PsLayerDesc *out)
{
	ImgSort    *sorted;
	char	   *filebuf;
	char	   *idxsec;
	PsImgFooter foot;
	uint64_t	data_bytes = (uint64_t) n * page_size;
	uint64_t	idx_bytes = (uint64_t) n * sizeof(PsImgIndexEnt);
	uint64_t	total = data_bytes + idx_bytes + sizeof(PsImgFooter);
	char		uri[PS_LAYER_URI_MAX];
	int			rc = -1;

	if (n == 0)
		return -1;
	sorted = malloc((size_t) n * sizeof(ImgSort));
	filebuf = malloc((size_t) total);
	if (!sorted || !filebuf)
		goto out;
	/* zero so struct padding isn't persisted (no heap leak to disk/objects, and
	 * the on-disk index bytes don't depend on compiler padding) */
	memset(sorted, 0, (size_t) n * sizeof(ImgSort));

	for (uint32_t i = 0; i < n; i++)
	{
		sorted[i].ent.key = recs[i].key;
		sorted[i].ent.block = recs[i].block;
		sorted[i].ent.lsn = recs[i].lsn;
		sorted[i].ent.data_off = 0;
		sorted[i].src = i;
	}
	qsort(sorted, n, sizeof(ImgSort), img_sort_cmp);

	/* page data, in sorted order, back to back */
	for (uint32_t i = 0; i < n; i++)
	{
		sorted[i].ent.data_off = (uint64_t) i * page_size;
		memcpy(filebuf + sorted[i].ent.data_off, recs[sorted[i].src].page,
			   page_size);
		sorted[i].ent.crc = img_crc(recs[sorted[i].src].page, page_size);
	}
	/* index section */
	idxsec = filebuf + data_bytes;
	for (uint32_t i = 0; i < n; i++)
		memcpy(idxsec + (size_t) i * sizeof(PsImgIndexEnt), &sorted[i].ent,
			   sizeof(PsImgIndexEnt));
	/* footer */
	memset(&foot, 0, sizeof(foot));
	foot.magic = PS_IMG_MAGIC;
	foot.version = PS_IMG_VERSION;
	foot.page_size = page_size;
	foot.nrecs = n;
	foot.index_off = data_bytes;
	foot.data_crc = img_crc(filebuf, (size_t) data_bytes);
	foot.index_crc = img_crc(idxsec, (size_t) idx_bytes);
	memcpy(filebuf + data_bytes + idx_bytes, &foot, sizeof(foot));

	if (ps_layer_store->create_local_layer(layer_id, uri, sizeof(uri)) != 0)
		goto out;
	if (ps_layer_store->write_local_layer(layer_id, filebuf, total) != 0 ||
		ps_layer_store->seal_local_layer(layer_id) != 0)
	{
		/* The file exists but is unwritten/unsealed and was never recorded in the
		 * manifest.  Remove it: g_next_layer_id is rebuilt from the manifest, so a
		 * left-behind id would be reused and create_local_layer's O_EXCL would
		 * then fail every retry, stranding the layer path. */
		PsLayerDesc orphan;

		memset(&orphan, 0, sizeof(orphan));
		orphan.layer_id = layer_id;
		orphan.location_count = 1;
		orphan.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
		orphan.locations[0].available = true;
		snprintf(orphan.locations[0].uri, sizeof(orphan.locations[0].uri),
				 "%s", uri);
		ps_layer_store->delete_local_layer(&orphan);
		goto out;
	}

	memset(out, 0, sizeof(*out));
	out->layer_id = layer_id;
	out->kind = PS_LAYER_IMAGE;
	out->timeline = timeline;
	out->start_key = sorted[0].ent.key;
	out->end_key = sorted[n - 1].ent.key;
	out->start_block = sorted[0].ent.block;
	out->end_block = sorted[n - 1].ent.block;
	out->lsn_start = sorted[0].ent.lsn;
	out->lsn_end = sorted[0].ent.lsn;
	for (uint32_t i = 0; i < n; i++)
	{
		if (sorted[i].ent.lsn < out->lsn_start)
			out->lsn_start = sorted[i].ent.lsn;
		if (sorted[i].ent.lsn > out->lsn_end)
			out->lsn_end = sorted[i].ent.lsn;
		if (sorted[i].ent.block < out->start_block)
			out->start_block = sorted[i].ent.block;
		if (sorted[i].ent.block > out->end_block)
			out->end_block = sorted[i].ent.block;
	}
	out->created_at_lsn = out->lsn_end;
	out->location_count = 1;
	out->locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
	out->locations[0].size = total;
	out->locations[0].available = true;
	snprintf(out->locations[0].uri, sizeof(out->locations[0].uri), "%s", uri);
	rc = 0;

out:
	free(sorted);
	free(filebuf);
	return rc;
}

/* ===================== delta layer file format ========================= */

typedef struct DeltaSort
{
	PsDeltaIndexEnt ent;
	uint32_t	src;
} DeltaSort;

static int
delta_sort_cmp(const void *pa, const void *pb)
{
	const DeltaSort *a = pa;
	const DeltaSort *b = pb;
	int			c = key_cmp(&a->ent.key, &b->ent.key);

	if (c)
		return c;
	if (a->ent.block != b->ent.block)
		return a->ent.block < b->ent.block ? -1 : 1;
	if (a->ent.lsn != b->ent.lsn)
		return a->ent.lsn < b->ent.lsn ? -1 : 1;
	return 0;
}

int
ps_delta_layer_write(uint64_t layer_id, uint32_t timeline,
					 const PsDeltaRec *recs, uint32_t n, PsLayerDesc *out)
{
	DeltaSort  *sorted;
	char	   *filebuf;
	char	   *idxsec;
	PsDeltaFooter foot;
	uint64_t	data_bytes = 0;
	uint64_t	idx_bytes = (uint64_t) n * sizeof(PsDeltaIndexEnt);
	uint64_t	total;
	uint64_t	off;
	char		uri[PS_LAYER_URI_MAX];
	int			rc = -1;

	if (n == 0)
		return -1;
	for (uint32_t i = 0; i < n; i++)
		data_bytes += recs[i].delta_len;
	total = data_bytes + idx_bytes + sizeof(PsDeltaFooter);

	sorted = malloc((size_t) n * sizeof(DeltaSort));
	filebuf = malloc((size_t) total);
	if (!sorted || !filebuf)
		goto out;
	/* zero so struct padding isn't persisted (no heap leak to disk/objects, and
	 * the on-disk index bytes don't depend on compiler padding) */
	memset(sorted, 0, (size_t) n * sizeof(DeltaSort));

	for (uint32_t i = 0; i < n; i++)
	{
		sorted[i].ent.key = recs[i].key;
		sorted[i].ent.block = recs[i].block;
		sorted[i].ent.lsn = recs[i].lsn;
		sorted[i].ent.data_len = recs[i].delta_len;
		sorted[i].src = i;
	}
	qsort(sorted, n, sizeof(DeltaSort), delta_sort_cmp);

	off = 0;
	for (uint32_t i = 0; i < n; i++)
	{
		sorted[i].ent.data_off = off;
		memcpy(filebuf + off, recs[sorted[i].src].delta, sorted[i].ent.data_len);
		sorted[i].ent.crc = img_crc(recs[sorted[i].src].delta,
								   sorted[i].ent.data_len);
		off += sorted[i].ent.data_len;
	}
	idxsec = filebuf + data_bytes;
	for (uint32_t i = 0; i < n; i++)
		memcpy(idxsec + (size_t) i * sizeof(PsDeltaIndexEnt), &sorted[i].ent,
			   sizeof(PsDeltaIndexEnt));
	memset(&foot, 0, sizeof(foot));
	foot.magic = PS_DELTA_MAGIC;
	foot.version = PS_DELTA_VERSION;
	foot.nrecs = n;
	foot.index_off = data_bytes;
	foot.data_crc = img_crc(filebuf, (size_t) data_bytes);
	foot.index_crc = img_crc(idxsec, (size_t) idx_bytes);
	memcpy(filebuf + data_bytes + idx_bytes, &foot, sizeof(foot));

	if (ps_layer_store->create_local_layer(layer_id, uri, sizeof(uri)) != 0)
		goto out;
	if (ps_layer_store->write_local_layer(layer_id, filebuf, total) != 0 ||
		ps_layer_store->seal_local_layer(layer_id) != 0)
	{
		/* remove the unmanifested file so its reused layer_id can't dead-lock
		 * create_local_layer's O_EXCL on a later flush (see ps_image_layer_write) */
		PsLayerDesc orphan;

		memset(&orphan, 0, sizeof(orphan));
		orphan.layer_id = layer_id;
		orphan.location_count = 1;
		orphan.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
		orphan.locations[0].available = true;
		snprintf(orphan.locations[0].uri, sizeof(orphan.locations[0].uri),
				 "%s", uri);
		ps_layer_store->delete_local_layer(&orphan);
		goto out;
	}

	memset(out, 0, sizeof(*out));
	out->layer_id = layer_id;
	out->kind = PS_LAYER_DELTA;
	out->timeline = timeline;
	out->start_key = sorted[0].ent.key;
	out->end_key = sorted[n - 1].ent.key;
	out->start_block = sorted[0].ent.block;
	out->end_block = sorted[n - 1].ent.block;
	out->lsn_start = sorted[0].ent.lsn;
	out->lsn_end = sorted[0].ent.lsn;
	for (uint32_t i = 0; i < n; i++)
	{
		if (sorted[i].ent.lsn < out->lsn_start)
			out->lsn_start = sorted[i].ent.lsn;
		if (sorted[i].ent.lsn > out->lsn_end)
			out->lsn_end = sorted[i].ent.lsn;
		if (sorted[i].ent.block < out->start_block)
			out->start_block = sorted[i].ent.block;
		if (sorted[i].ent.block > out->end_block)
			out->end_block = sorted[i].ent.block;
	}
	out->created_at_lsn = out->lsn_end;
	out->location_count = 1;
	out->locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
	out->locations[0].size = total;
	out->locations[0].available = true;
	snprintf(out->locations[0].uri, sizeof(out->locations[0].uri), "%s", uri);
	rc = 0;

out:
	free(sorted);
	free(filebuf);
	return rc;
}

int
ps_delta_layer_collect(const PsLayerDesc *layer, const PsKey *key,
					   uint32_t block, uint64_t lo_lsn, uint64_t hi_lsn,
					   PsDeltaOut **outs, uint32_t *cap, uint32_t *n)
{
	const PsLayerLocation *loc = img_local_loc(layer);
	PsDeltaFooter foot;
	PsDeltaIndexEnt *idx;
	uint64_t	idx_bytes;
	int			rc = -1;

	/* prune: skip without any IO if this layer cannot hold (key,block) or its
	 * LSN range does not overlap the requested half-open [lo_lsn, hi_lsn).  Use
	 * '<'/'>=' so a layer whose newest record is exactly lo_lsn (a delta starting
	 * at the base LSN, which the scan includes) is not dropped, and one starting
	 * exactly at hi_lsn (== read_lsn, excluded) is. */
	if (!layer_covers(layer, key, block) ||
		layer->lsn_end < lo_lsn || layer->lsn_start >= hi_lsn)
		return 0;
	if (!loc || loc->size < sizeof(PsDeltaFooter))
		return -1;
	if (ps_layer_store->read_layer_block(layer, loc->size - sizeof(foot),
										 &foot, sizeof(foot)) != 0)
		return -1;
	if (foot.magic != PS_DELTA_MAGIC || foot.version != PS_DELTA_VERSION ||
		foot.nrecs == 0)
		return -1;
	idx_bytes = (uint64_t) foot.nrecs * sizeof(PsDeltaIndexEnt);
	/* reject a footer whose index section would not fit in the file (guards a
	 * corrupt/forged nrecs/index_off from driving a huge allocation or an OOB
	 * read), matching the image-layer reader */
	if (foot.index_off + idx_bytes + sizeof(foot) > loc->size)
		return -1;
	idx = malloc((size_t) idx_bytes);
	if (!idx)
		return -1;
	if (ps_layer_store->read_layer_block(layer, foot.index_off, idx,
										 (uint32_t) idx_bytes) != 0 ||
		img_crc(idx, (size_t) idx_bytes) != foot.index_crc)
		goto out;

	/* index is sorted by (key, block, lsn): matches are contiguous + ascending */
	for (uint32_t i = 0; i < foot.nrecs; i++)
	{
		if (idx[i].block != block || key_cmp(&idx[i].key, key) != 0)
			continue;
		/* half-open [lo_lsn, hi_lsn): delta keys are WAL record *start* LSNs while
		 * an image/page LSN is the previous record's end == the next record's
		 * start.  So a base at lo_lsn needs the record whose start == lo_lsn
		 * applied, and a record starting exactly at hi_lsn (== read_lsn) is past
		 * the requested state and must be excluded. */
		if (idx[i].lsn < lo_lsn || idx[i].lsn >= hi_lsn)
			continue;
		/* the payload must lie wholly inside the payload section [0, index_off);
		 * a corrupt offset/len (even with a matching index crc) must fail the
		 * layer, not expose index/footer bytes as a WAL payload (overflow-safe) */
		if (idx[i].data_off > foot.index_off ||
			idx[i].data_len > foot.index_off - idx[i].data_off)
			goto out;
		/* grow the output as needed -- no fixed per-layer cap, since a hot page
		 * can legitimately have many deltas in one sealed layer and the caller's
		 * plan->deltas is itself growable */
		if (*n == *cap)
		{
			uint32_t	nc = *cap ? *cap * 2 : 16;
			PsDeltaOut *no = realloc(*outs, (size_t) nc * sizeof(PsDeltaOut));

			if (!no)
				goto out;
			*outs = no;
			*cap = nc;
		}
		(*outs)[*n].lsn = idx[i].lsn;
		(*outs)[*n].data_off = idx[i].data_off;
		(*outs)[*n].data_len = idx[i].data_len;
		(*outs)[*n].crc = idx[i].crc;
		(*n)++;
	}
	rc = 0;

out:
	free(idx);
	return rc;
}

/* ===================== read plan (phase 7b) ============================ */

static int
plan_delta_cmp(const void *pa, const void *pb)
{
	uint64_t	a = ((const PsPlanDelta *) pa)->lsn;
	uint64_t	b = ((const PsPlanDelta *) pb)->lsn;

	return a < b ? -1 : (a > b ? 1 : 0);
}

/* order image-base candidates newest-first by their layer LSN range (lsn_end). */
static int
img_cand_newest_first(const void *pa, const void *pb)
{
	const PsLayerDesc *a = *(const PsLayerDesc *const *) pa;
	const PsLayerDesc *b = *(const PsLayerDesc *const *) pb;

	if (a->lsn_end != b->lsn_end)
		return a->lsn_end > b->lsn_end ? -1 : 1;
	return 0;
}

void
ps_read_plan_free(PsReadPlan *plan)
{
	if (!plan)
		return;
	free(plan->base);
	for (uint32_t i = 0; i < plan->ndelta; i++)
		free(plan->deltas[i].bytes);
	free(plan->deltas);
	plan->base = NULL;
	plan->deltas = NULL;
	plan->ndelta = 0;
	plan->has_base = 0;
}

int
ps_read_plan_build(const PsLayerMap *map, uint32_t timeline, const PsKey *key,
				   uint32_t block, uint64_t read_lsn, uint32_t page_size,
				   PsReadPlan *plan)
{
	unsigned char *tmp = malloc(page_size);
	uint64_t	lo;
	uint32_t	cap = 0;
	PsDeltaOut *douts = NULL;	/* growable collect buffer, reused per layer */
	uint32_t	dcap = 0;
	int			rc = -1;

	memset(plan, 0, sizeof(*plan));
	if (!tmp)
		return -1;

	/* 1. base = newest unambiguous image-layer version <= read_lsn.  Probe
	 * candidates newest-first (by layer lsn_end) so the freshest base is chosen
	 * before older layers: a clearly-superseded layer (lsn_end below the base
	 * found) is then skipped without being read, while a covering layer that could
	 * still be the base but is corrupt/unavailable fails the plan rather than
	 * silently selecting an older base (there may be no delta chain that recreates
	 * the skipped image). */
	{
		const PsLayerDesc **cand = NULL;
		uint32_t	ncand = 0;
		int			base_ok = 1;
		int			base_ambiguous = 0;

		if (map->nlayers)
		{
			cand = malloc((size_t) map->nlayers * sizeof(*cand));
			if (!cand)
				goto out;
		}
		for (uint32_t i = 0; i < map->nlayers; i++)
		{
			const PsLayerDesc *d = &map->layers[i];

			if (d->kind != PS_LAYER_IMAGE || d->deleting ||
				d->timeline != timeline)
				continue;
			/* skip a layer whose metadata cannot hold a version of (key,block)
			 * at or below read_lsn (never read it) */
			if (key_cmp(key, &d->start_key) < 0 || key_cmp(key, &d->end_key) > 0 ||
				block < d->start_block || block > d->end_block ||
				d->lsn_start > read_lsn)
				continue;
			cand[ncand++] = d;
		}
		qsort(cand, ncand, sizeof(*cand), img_cand_newest_first);
		for (uint32_t j = 0; j < ncand; j++)
		{
			const PsLayerDesc *d = cand[j];
			uint64_t	l;
			int			amb;
			int			r;

			/* sorted newest-first: once a candidate's whole range is below the
			 * base already found it cannot beat or tie it, and neither can the
			 * rest -- stop without reading them */
			if (plan->has_base && d->lsn_end < plan->base_lsn)
				break;
			r = ps_image_layer_lookup(d, key, block, read_lsn, tmp, page_size,
									  &l, &amb);
			if (r < 0)
			{
				base_ok = 0;	/* a covering candidate is corrupt/unavailable */
				break;
			}
			if (r != 1)
				continue;
			if (amb)			/* several versions share this lsn in one layer */
			{
				base_ambiguous = 1;
				break;
			}
			if (!plan->has_base || l > plan->base_lsn)
			{
				if (!plan->base)
					plan->base = malloc(page_size);
				if (!plan->base)
				{
					base_ok = 0;
					break;
				}
				memcpy(plan->base, tmp, page_size);
				plan->base_lsn = l;
				plan->has_base = 1;
			}
			else if (l == plan->base_lsn)
			{
				/* same lsn in two layers: tolerate an identical-bytes duplicate --
				 * crash-safe compaction (install-new-before-delete-old) briefly
				 * leaves the old and new image layers both live with the same
				 * version -- but a genuine same-lsn rewrite (different bytes, e.g.
				 * hint bits) is ambiguous and must fail to the segment path */
				if (memcmp(plan->base, tmp, page_size) != 0)
				{
					base_ambiguous = 1;
					break;
				}
				/* identical duplicate base -> keep the one already chosen */
			}
		}
		free(cand);
		/* an ambiguous base (same-lsn rewrite) or a corrupt covering layer cannot
		 * be materialized safely; fail so the caller serves from the authoritative
		 * segment, as read_resolve() does via page_lsn_ambiguous() */
		if (!base_ok || base_ambiguous)
			goto out;
	}
	lo = plan->has_base ? plan->base_lsn : 0;

	/* 2. deltas in [lo, read_lsn) across this timeline's delta layers.  When the
	 * base already reaches read_lsn the interval is empty -- skip the scan
	 * entirely so an irrelevant remote-only/corrupt delta layer at that boundary
	 * cannot fail a read the base image alone already satisfies. */
	for (uint32_t i = 0; lo < read_lsn && i < map->nlayers; i++)
	{
		const PsLayerDesc *d = &map->layers[i];
		uint32_t	tn = 0;

		if (d->kind != PS_LAYER_DELTA || d->deleting || d->timeline != timeline)
			continue;
		/* skip a layer whose key/block/LSN range cannot overlap [lo, read_lsn):
		 * its metadata already proves it is irrelevant, so a remote-only/evicted/
		 * corrupt unrelated layer must not be touched (let alone fail the read) */
		if (key_cmp(key, &d->start_key) < 0 || key_cmp(key, &d->end_key) > 0 ||
			block < d->start_block || block > d->end_block ||
			d->lsn_end < lo || d->lsn_start >= read_lsn)
			continue;
		if (ps_delta_layer_collect(d, key, block, lo, read_lsn, &douts,
								   &dcap, &tn) != 0)
			goto out;
		for (uint32_t j = 0; j < tn; j++)
		{
			unsigned char *bytes;

			if (plan->ndelta == cap)
			{
				uint32_t	nc = cap ? cap * 2 : 16;
				PsPlanDelta *nd = realloc(plan->deltas,
										  (size_t) nc * sizeof(PsPlanDelta));

				if (!nd)
					goto out;
				plan->deltas = nd;
				cap = nc;
			}
			bytes = malloc(douts[j].data_len);
			if (!bytes ||
				ps_layer_store->read_layer_block(d, douts[j].data_off, bytes,
												 douts[j].data_len) != 0 ||
				img_crc(bytes, douts[j].data_len) != douts[j].crc)
			{
				/* a payload that fails its per-record crc is corrupt: reject the
				 * read instead of feeding bad bytes to the redo helper */
				free(bytes);
				goto out;
			}
			plan->deltas[plan->ndelta].lsn = douts[j].lsn;
			plan->deltas[plan->ndelta].len = douts[j].data_len;
			plan->deltas[plan->ndelta].bytes = bytes;
			plan->ndelta++;
		}
	}

	/* 3. order the chain by ascending LSN (it spans multiple layers) */
	if (plan->ndelta > 1)
		qsort(plan->deltas, plan->ndelta, sizeof(PsPlanDelta), plan_delta_cmp);

	/* 4. drop duplicate records: overlapping delta layers (e.g. a replacement
	 * layer added before the old ones are marked deleting in the install-new-
	 * before-delete-old compaction flow) can expose the same WAL record (same
	 * record LSN) twice; applying it twice in rm_redo would corrupt the page.
	 * Sorted by LSN above, so duplicates are adjacent.  Only drop a duplicate that
	 * is byte-identical -- two same-LSN records with *different* payloads (a stale
	 * replacement or a corrupt-but-self-consistent layer) are a real conflict that
	 * qsort would resolve arbitrarily, so fail the plan instead of guessing. */
	for (uint32_t i = 1; i < plan->ndelta; i++)
		if (plan->deltas[i].lsn == plan->deltas[i - 1].lsn)
		{
			if (plan->deltas[i].len != plan->deltas[i - 1].len ||
				memcmp(plan->deltas[i].bytes, plan->deltas[i - 1].bytes,
					   plan->deltas[i].len) != 0)
				goto out;		/* conflicting duplicate record -> reject */
			free(plan->deltas[i].bytes);
			memmove(&plan->deltas[i], &plan->deltas[i + 1],
					(size_t) (plan->ndelta - i - 1) * sizeof(PsPlanDelta));
			plan->ndelta--;
			i--;				/* re-check the new entry now at i */
		}
	rc = 0;

out:
	free(tmp);
	free(douts);
	if (rc != 0)
		ps_read_plan_free(plan);
	return rc;
}

int
ps_image_layer_read_index(const PsLayerDesc *layer, PsImgIndexEnt **out,
						  uint32_t *n)
{
	const PsLayerLocation *loc = img_local_loc(layer);
	PsImgFooter foot;
	PsImgIndexEnt *idx;
	uint64_t	idx_bytes;

	*out = NULL;
	*n = 0;
	if (!loc || loc->size < sizeof(PsImgFooter))
		return -1;
	if (ps_layer_store->read_layer_block(layer, loc->size - sizeof(foot),
										 &foot, sizeof(foot)) != 0)
		return -1;
	if (foot.magic != PS_IMG_MAGIC || foot.version != PS_IMG_VERSION ||
		foot.nrecs == 0)
		return -1;
	idx_bytes = (uint64_t) foot.nrecs * sizeof(PsImgIndexEnt);
	/* reject a footer whose index section would not fit in the file (guards a
	 * corrupt/forged nrecs from driving a huge allocation) */
	if (foot.index_off + idx_bytes + sizeof(foot) > loc->size)
		return -1;
	idx = malloc((size_t) idx_bytes);
	if (!idx)
		return -1;
	if (ps_layer_store->read_layer_block(layer, foot.index_off, idx,
										 (uint32_t) idx_bytes) != 0 ||
		img_crc(idx, (size_t) idx_bytes) != foot.index_crc)
	{
		free(idx);
		return -1;
	}
	*out = idx;
	*n = foot.nrecs;
	return 0;
}

int
ps_image_layer_lookup(const PsLayerDesc *layer, const PsKey *key,
					  uint32_t block, uint64_t read_lsn,
					  void *out, uint32_t page_size, uint64_t *out_lsn,
					  int *out_ambiguous)
{
	const PsLayerLocation *loc = img_local_loc(layer);
	PsImgFooter foot;
	PsImgIndexEnt *idx;
	uint64_t	idx_bytes;
	uint64_t	best_off = 0;
	uint64_t	best_lsn = 0;
	uint32_t	best_crc = 0;
	int			found = 0;
	int			rc = -1;

	LayerBloom *be = bloom_slot(layer->layer_id);
	int			cached = (be->valid && be->layer_id == layer->layer_id);

	/* prune: skip without any IO if this layer cannot hold (key,block) or has no
	 * version at/below read_lsn */
	if (!layer_covers(layer, key, block) || read_lsn < layer->lsn_start)
		return 0;
	/* finer prune: a cached bloom can rule (key,block) out with no index read */
	if (cached && !bloom_test(be->bits, key, block))
		return 0;
	if (!loc || loc->size < sizeof(PsImgFooter))
		return -1;
	if (ps_layer_store->read_layer_block(layer, loc->size - sizeof(foot),
										 &foot, sizeof(foot)) != 0)
		return -1;
	if (foot.magic != PS_IMG_MAGIC || foot.version != PS_IMG_VERSION ||
		foot.page_size != page_size || foot.nrecs == 0)
		return -1;

	idx_bytes = (uint64_t) foot.nrecs * sizeof(PsImgIndexEnt);
	/* reject a footer whose index section would not fit in the file (guards a
	 * corrupt/forged nrecs from driving a huge allocation) */
	if (foot.index_off + idx_bytes + sizeof(foot) > loc->size)
		return -1;
	idx = malloc((size_t) idx_bytes);
	if (!idx)
		return -1;
	if (ps_layer_store->read_layer_block(layer, foot.index_off, idx,
										 (uint32_t) idx_bytes) != 0)
		goto out;
	if (img_crc(idx, (size_t) idx_bytes) != foot.index_crc)
		goto out;				/* corrupt index */

	/* first read of this layer: build its bloom from the verified index so later
	 * reads can skip it without this index read */
	if (!cached)
	{
		memset(be->bits, 0, sizeof(be->bits));
		for (uint32_t i = 0; i < foot.nrecs; i++)
			bloom_set(be->bits, &idx[i].key, idx[i].block);
		be->layer_id = layer->layer_id;
		be->valid = 1;
	}

	/* newest version of (key, block) with lsn <= read_lsn */
	for (uint32_t i = 0; i < foot.nrecs; i++)
	{
		if (idx[i].block != block || key_cmp(&idx[i].key, key) != 0)
			continue;
		if (idx[i].lsn <= read_lsn && (!found || idx[i].lsn >= best_lsn))
		{
			best_lsn = idx[i].lsn;
			best_off = idx[i].data_off;
			best_crc = idx[i].crc;
			found = 1;
		}
	}
	if (out_ambiguous)
		*out_ambiguous = 0;
	if (!found)
	{
		rc = 0;
		goto out;
	}
	/* report whether the chosen version's page LSN is ambiguous within this layer:
	 * more than one version of (key,block) shares best_lsn *with differing bytes*
	 * (a genuine same-lsn rewrite, e.g. hint bits), which lsn alone can't identify.
	 * Identical-byte duplicates are benign -- compaction copies the versions of
	 * crash-overlapped layers (install-new-before-delete-old) into one new layer,
	 * so the same (key,block,lsn) can legitimately appear twice with equal bytes;
	 * compare the per-page crc rather than just counting. */
	if (out_ambiguous)
	{
		*out_ambiguous = 0;
		for (uint32_t i = 0; i < foot.nrecs; i++)
			if (idx[i].block == block && key_cmp(&idx[i].key, key) == 0 &&
				idx[i].lsn == best_lsn && idx[i].crc != best_crc)
			{
				*out_ambiguous = 1;
				break;
			}
	}
	if (ps_layer_store->read_layer_block(layer, best_off, out, page_size) != 0)
		goto out;
	if (img_crc(out, page_size) != best_crc)
		goto out;				/* corrupt page bytes -- rc stays -1, caller falls
								 * back to the segment copy */
	if (out_lsn)
		*out_lsn = best_lsn;
	rc = 1;

out:
	free(idx);
	return rc;
}
