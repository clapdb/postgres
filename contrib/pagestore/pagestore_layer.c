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
	if (a->klass != b->klass)
		return a->klass < b->klass ? -1 : 1;
	return 0;
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
		goto out;

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
		goto out;

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
					   PsDeltaOut *outs, uint32_t cap, uint32_t *n)
{
	const PsLayerLocation *loc = img_local_loc(layer);
	PsDeltaFooter foot;
	PsDeltaIndexEnt *idx;
	uint64_t	idx_bytes;
	int			rc = -1;

	/* prune: skip without any IO if this layer cannot hold (key,block) or its
	 * LSN range does not overlap the requested (lo_lsn, hi_lsn] */
	if (!layer_covers(layer, key, block) ||
		layer->lsn_end <= lo_lsn || layer->lsn_start > hi_lsn)
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
	idx = malloc((size_t) idx_bytes);
	if (!idx)
		return -1;
	if (ps_layer_store->read_layer_block(layer, foot.index_off, idx,
										 (uint32_t) idx_bytes) != 0 ||
		img_crc(idx, (size_t) idx_bytes) != foot.index_crc)
		goto out;

	/* index is sorted by (key, block, lsn): matches are contiguous + ascending */
	for (uint32_t i = 0; i < foot.nrecs && *n < cap; i++)
	{
		if (idx[i].block != block || key_cmp(&idx[i].key, key) != 0)
			continue;
		if (idx[i].lsn <= lo_lsn || idx[i].lsn > hi_lsn)
			continue;
		outs[*n].lsn = idx[i].lsn;
		outs[*n].data_off = (uint32_t) idx[i].data_off;
		outs[*n].data_len = idx[i].data_len;
		(*n)++;
	}
	rc = 0;

out:
	free(idx);
	return rc;
}

/* ===================== read plan (phase 7b) ============================ */

#define PS_PLAN_MAX_CHAIN	1024	/* per-layer delta cap (chains are bounded
									 * by compaction policy) */

static int
plan_delta_cmp(const void *pa, const void *pb)
{
	uint64_t	a = ((const PsPlanDelta *) pa)->lsn;
	uint64_t	b = ((const PsPlanDelta *) pb)->lsn;

	return a < b ? -1 : (a > b ? 1 : 0);
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
	int			rc = -1;

	memset(plan, 0, sizeof(*plan));
	if (!tmp)
		return -1;

	/* 1. base = newest image-layer version <= read_lsn on this timeline */
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		const PsLayerDesc *d = &map->layers[i];
		uint64_t	l;

		if (d->kind != PS_LAYER_IMAGE || d->deleting || d->timeline != timeline)
			continue;
		if (ps_image_layer_lookup(d, key, block, read_lsn, tmp, page_size,
								  &l) == 1 && (!plan->has_base || l > plan->base_lsn))
		{
			if (!plan->base)
				plan->base = malloc(page_size);
			if (!plan->base)
				goto out;
			memcpy(plan->base, tmp, page_size);
			plan->base_lsn = l;
			plan->has_base = 1;
		}
	}
	lo = plan->has_base ? plan->base_lsn : 0;

	/* 2. deltas in (lo, read_lsn] across this timeline's delta layers */
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		const PsLayerDesc *d = &map->layers[i];
		PsDeltaOut	outs[PS_PLAN_MAX_CHAIN];
		uint32_t	tn = 0;

		if (d->kind != PS_LAYER_DELTA || d->deleting || d->timeline != timeline)
			continue;
		if (ps_delta_layer_collect(d, key, block, lo, read_lsn, outs,
								   PS_PLAN_MAX_CHAIN, &tn) != 0)
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
			bytes = malloc(outs[j].data_len);
			if (!bytes ||
				ps_layer_store->read_layer_block(d, outs[j].data_off, bytes,
												 outs[j].data_len) != 0)
			{
				free(bytes);
				goto out;
			}
			plan->deltas[plan->ndelta].lsn = outs[j].lsn;
			plan->deltas[plan->ndelta].len = outs[j].data_len;
			plan->deltas[plan->ndelta].bytes = bytes;
			plan->ndelta++;
		}
	}

	/* 3. order the chain by ascending LSN (it spans multiple layers) */
	if (plan->ndelta > 1)
		qsort(plan->deltas, plan->ndelta, sizeof(PsPlanDelta), plan_delta_cmp);
	rc = 0;

out:
	free(tmp);
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
					  void *out, uint32_t page_size, uint64_t *out_lsn)
{
	const PsLayerLocation *loc = img_local_loc(layer);
	PsImgFooter foot;
	PsImgIndexEnt *idx;
	uint64_t	idx_bytes;
	uint64_t	best_off = 0;
	uint64_t	best_lsn = 0;
	int			found = 0;
	int			rc = -1;

	/* prune: skip without any IO if this layer cannot hold (key,block) or has no
	 * version at/below read_lsn */
	if (!layer_covers(layer, key, block) || read_lsn < layer->lsn_start)
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
	idx = malloc((size_t) idx_bytes);
	if (!idx)
		return -1;
	if (ps_layer_store->read_layer_block(layer, foot.index_off, idx,
										 (uint32_t) idx_bytes) != 0)
		goto out;
	if (img_crc(idx, (size_t) idx_bytes) != foot.index_crc)
		goto out;				/* corrupt index */

	/* newest version of (key, block) with lsn <= read_lsn */
	for (uint32_t i = 0; i < foot.nrecs; i++)
	{
		if (idx[i].block != block || key_cmp(&idx[i].key, key) != 0)
			continue;
		if (idx[i].lsn <= read_lsn && (!found || idx[i].lsn >= best_lsn))
		{
			best_lsn = idx[i].lsn;
			best_off = idx[i].data_off;
			found = 1;
		}
	}
	if (!found)
	{
		rc = 0;
		goto out;
	}
	if (ps_layer_store->read_layer_block(layer, best_off, out, page_size) != 0)
		goto out;
	if (out_lsn)
		*out_lsn = best_lsn;
	rc = 1;

out:
	free(idx);
	return rc;
}
