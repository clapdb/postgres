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
					  void *out, uint32_t page_size, uint64_t *out_lsn)
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
	if (!found)
	{
		rc = 0;
		goto out;
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
