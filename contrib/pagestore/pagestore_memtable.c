/*-------------------------------------------------------------------------
 *
 * pagestore_memtable.c
 *	  Mutable staging of recent page versions, flushed into image layers.
 *	  See pagestore_memtable.h.
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <string.h>

#include "pagestore_memtable.h"

typedef struct MemEnt
{
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	uint64_t	admission_seq;
	unsigned char *page;		/* owned copy, page_size bytes */
	uint64_t	growth_lsn;
	uint64_t	order_id;
	uint64_t	seg_off;
	uint64_t	append_seq;
	uint32_t	seg_id;
	uint32_t	flags;
} MemEnt;

struct PsMemtable
{
	uint32_t	page_size;
	uint32_t	threshold;
	MemEnt	   *ents;
	uint32_t	n;
	uint32_t	cap;
	uint64_t	next_append_seq;
};

PsMemtable *
ps_memtable_create(uint32_t page_size, uint32_t threshold)
{
	PsMemtable *mt = calloc(1, sizeof(*mt));

	if (!mt)
		return NULL;
	mt->page_size = page_size;
	mt->threshold = threshold ? threshold : 1;
	return mt;
}

static void
free_pages(PsMemtable *mt)
{
	for (uint32_t i = 0; i < mt->n; i++)
		free(mt->ents[i].page);
	mt->n = 0;
}

void
ps_memtable_destroy(PsMemtable *mt)
{
	if (!mt)
		return;
	free_pages(mt);
	free(mt->ents);
	free(mt);
}

int
ps_memtable_put(PsMemtable *mt, uint32_t timeline, const PsKey *key,
				uint32_t block, uint64_t lsn, const void *page,
				uint64_t admission_seq,
				uint64_t growth_lsn, uint64_t order_id,
				uint32_t seg_id, uint64_t seg_off, uint32_t flags)
{
	MemEnt	   *e;
	unsigned char *copy;

	if (mt->n == mt->cap)
	{
		uint32_t	newcap = mt->cap ? mt->cap * 2 : 64;
		MemEnt	   *ne = realloc(mt->ents, (size_t) newcap * sizeof(MemEnt));

		if (!ne)
			return -1;
		mt->ents = ne;
		mt->cap = newcap;
	}
	copy = malloc(mt->page_size);
	if (!copy)
		return -1;
	memcpy(copy, page, mt->page_size);

	e = &mt->ents[mt->n++];
	e->timeline = timeline;
	e->key = *key;
	e->block = block;
	e->lsn = lsn;
	e->admission_seq = admission_seq;
	e->page = copy;
	e->growth_lsn = growth_lsn;
	e->order_id = order_id;
	e->seg_off = seg_off;
	e->append_seq = mt->next_append_seq++;
	e->seg_id = seg_id;
	e->flags = flags;
	return 0;
}

uint32_t
ps_memtable_count(const PsMemtable *mt)
{
	return mt->n;
}

int
ps_memtable_full(const PsMemtable *mt)
{
	return mt->n >= mt->threshold;
}

int
ps_memtable_lookup(const PsMemtable *mt, uint32_t timeline, const PsKey *key,
				   uint32_t block, uint64_t read_lsn, uint64_t read_seq,
				   uint64_t *out_lsn, uint64_t *out_seq, void *out)
{
	const MemEnt *best = NULL;

	for (uint32_t i = 0; i < mt->n; i++)
	{
		const MemEnt *e = &mt->ents[i];

		if (e->timeline != timeline || e->block != block)
			continue;
		if (e->key.spcOid != key->spcOid || e->key.dbOid != key->dbOid ||
			e->key.relNumber != key->relNumber || e->key.forkNum != key->forkNum ||
			e->key.klass != key->klass)
			continue;
		if (e->lsn <= read_lsn &&
			(e->lsn < read_lsn || read_seq == 0 || e->admission_seq == 0 ||
			 e->admission_seq <= read_seq) &&
			(!best || e->lsn > best->lsn ||
			 (e->lsn == best->lsn && e->admission_seq >= best->admission_seq)))
			best = e;
	}
	if (!best)
		return 0;
	memcpy(out, best->page, mt->page_size);
	if (out_lsn)
		*out_lsn = best->lsn;
	if (out_seq)
		*out_seq = best->admission_seq;
	return 1;
}

static int
ent_timeline_cmp(const void *pa, const void *pb)
{
	uint32_t	a = ((const MemEnt *) pa)->timeline;
	uint32_t	b = ((const MemEnt *) pb)->timeline;

	if (a != b)
		return a < b ? -1 : 1;
	return ((const MemEnt *) pa)->append_seq < ((const MemEnt *) pb)->append_seq ? -1 :
		(((const MemEnt *) pa)->append_seq > ((const MemEnt *) pb)->append_seq ? 1 : 0);
}

int
ps_memtable_flush(PsMemtable *mt, PsAllocLayerId alloc, PsOnLayer on_layer,
				  void *ctx)
{
	uint32_t	i = 0;

	if (mt->n == 0)
		return 0;

	/* group by timeline: sort, then emit one image layer per run */
	qsort(mt->ents, mt->n, sizeof(MemEnt), ent_timeline_cmp);

	while (i < mt->n)
	{
		uint32_t	tl = mt->ents[i].timeline;
		uint32_t	j = i;
		uint32_t	m;
		PsImgRec   *recs;
		PsLayerDesc desc;
		uint64_t	layer_id;

		while (j < mt->n && mt->ents[j].timeline == tl)
			j++;
		m = j - i;

		recs = malloc((size_t) m * sizeof(PsImgRec));
		if (!recs)
			return -1;			/* leave memtable intact for retry */
		for (uint32_t r = 0; r < m; r++)
		{
			recs[r].key = mt->ents[i + r].key;
			recs[r].block = mt->ents[i + r].block;
			recs[r].lsn = mt->ents[i + r].lsn;
			recs[r].admission_seq = mt->ents[i + r].admission_seq;
			recs[r].page = mt->ents[i + r].page;
			recs[r].growth_lsn = mt->ents[i + r].growth_lsn;
			recs[r].order_id = mt->ents[i + r].order_id;
			recs[r].seg_off = mt->ents[i + r].seg_off;
			recs[r].seg_id = mt->ents[i + r].seg_id;
			recs[r].flags = mt->ents[i + r].flags;
		}

		layer_id = alloc(ctx);
		if (ps_image_layer_write(layer_id, tl, recs, m, mt->page_size,
								 &desc) != 0 ||
			on_layer(ctx, &desc) != 0)
		{
			free(recs);
			return -1;			/* partial flush: memtable kept for retry */
		}
		free(recs);
		i = j;
	}

	free_pages(mt);
	return 0;
}
