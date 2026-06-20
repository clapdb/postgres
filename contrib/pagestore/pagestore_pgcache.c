/*-------------------------------------------------------------------------
 *
 * pagestore_pgcache.c
 *	  Scan-resistant CLOCK materialized-page cache.  See pagestore_pgcache.h.
 *
 * One instance per shard (PsPgcache), so a shard's cache is single-owner and
 * lock-free; all state lives in the handle, no file-scope globals.
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <string.h>

#include "pagestore_pgcache.h"

typedef struct PgKey
{
	uint32_t	timeline;
	uint32_t	block;
	uint64_t	lsn;
	PsKey		key;
} PgKey;

struct PgcSlot
{
	int			valid;
	uint8_t		ref;			/* CLOCK reference bit */
	PgKey		k;
	int			hnext;			/* next slot in its hash bucket, or -1 */
};

static uint32_t
key_hash(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn)
{
	uint64_t	h = 1469598103934665603ULL;	/* FNV-1a over the fields */
	const uint64_t pr = 1099511628211ULL;
	uint64_t	parts[6];

	parts[0] = tl;
	parts[1] = key->spcOid;
	parts[2] = ((uint64_t) key->dbOid << 32) | key->relNumber;
	parts[3] = key->forkNum;
	parts[4] = block;
	parts[5] = lsn;
	for (int i = 0; i < 6; i++)
		h = (h ^ parts[i]) * pr;
	return (uint32_t) (h ^ (h >> 32));
}

static int
key_eq(const PgKey *a, uint32_t tl, const PsKey *key, uint32_t block,
	   uint64_t lsn)
{
	return a->timeline == tl && a->block == block && a->lsn == lsn &&
		a->key.spcOid == key->spcOid && a->key.dbOid == key->dbOid &&
		a->key.relNumber == key->relNumber && a->key.forkNum == key->forkNum;
}

void
ps_pgcache_init(PsPgcache *c, uint32_t max_pages, uint32_t page_size)
{
	memset(c, 0, sizeof(*c));
	c->max = max_pages;
	c->psz = page_size;
	if (max_pages == 0)
		return;
	c->nbuckets = max_pages * 2 + 1;
	c->slots = calloc(max_pages, sizeof(struct PgcSlot));
	c->pages = malloc((size_t) max_pages * page_size);
	c->buckets = malloc((size_t) c->nbuckets * sizeof(int));
	if (!c->slots || !c->pages || !c->buckets)
	{
		ps_pgcache_free(c);
		return;
	}
	for (uint32_t i = 0; i < c->nbuckets; i++)
		c->buckets[i] = -1;
}

void
ps_pgcache_free(PsPgcache *c)
{
	free(c->slots);
	free(c->pages);
	free(c->buckets);
	c->slots = NULL;
	c->pages = NULL;
	c->buckets = NULL;
	c->max = 0;
}

static int
find_slot(PsPgcache *c, uint32_t tl, const PsKey *key, uint32_t block,
		  uint64_t lsn, uint32_t *out_bucket)
{
	uint32_t	b = key_hash(tl, key, block, lsn) % c->nbuckets;

	if (out_bucket)
		*out_bucket = b;
	for (int i = c->buckets[b]; i >= 0; i = c->slots[i].hnext)
		if (c->slots[i].valid && key_eq(&c->slots[i].k, tl, key, block, lsn))
			return i;
	return -1;
}

int
ps_pgcache_lookup(PsPgcache *c, uint32_t tl, const PsKey *key, uint32_t block,
				  uint64_t version_lsn, void *out)
{
	int			s;

	if (c->max == 0)
		return 0;
	s = find_slot(c, tl, key, block, version_lsn, NULL);
	if (s < 0)
	{
		c->misses++;
		return 0;
	}
	c->slots[s].ref = 1;		/* re-reference: survives the next sweep */
	memcpy(out, c->pages + (size_t) s * c->psz, c->psz);
	c->hits++;
	return 1;
}

/* unlink slot s from its hash bucket chain */
static void
bucket_remove(PsPgcache *c, int s)
{
	uint32_t	b = key_hash(c->slots[s].k.timeline, &c->slots[s].k.key,
							 c->slots[s].k.block, c->slots[s].k.lsn) % c->nbuckets;
	int		   *pp = &c->buckets[b];

	while (*pp >= 0 && *pp != s)
		pp = &c->slots[*pp].hnext;
	if (*pp == s)
		*pp = c->slots[s].hnext;
}

void
ps_pgcache_insert(PsPgcache *c, uint32_t tl, const PsKey *key, uint32_t block,
				  uint64_t version_lsn, const void *page)
{
	uint32_t	b;
	int			s;

	if (c->max == 0)
		return;
	s = find_slot(c, tl, key, block, version_lsn, &b);
	if (s >= 0)
	{							/* already present: refresh bytes + reference */
		memcpy(c->pages + (size_t) s * c->psz, page, c->psz);
		c->slots[s].ref = 1;
		return;
	}

	if (c->nused < c->max)
		s = (int) c->nused++;	/* still filling */
	else
	{
		/* CLOCK: advance over referenced slots (clearing), evict first ref=0 */
		while (c->slots[c->hand].ref)
		{
			c->slots[c->hand].ref = 0;
			c->hand = (c->hand + 1) % c->max;
		}
		s = (int) c->hand;
		c->hand = (c->hand + 1) % c->max;
		bucket_remove(c, s);
		c->evict++;
	}

	c->slots[s].valid = 1;
	c->slots[s].ref = 0;		/* scan-resistant: new entries start unreferenced */
	c->slots[s].k.timeline = tl;
	c->slots[s].k.key = *key;
	c->slots[s].k.block = block;
	c->slots[s].k.lsn = version_lsn;
	memcpy(c->pages + (size_t) s * c->psz, page, c->psz);
	c->slots[s].hnext = c->buckets[b];
	c->buckets[b] = s;
}

void
ps_pgcache_stats(const PsPgcache *c, uint64_t *hits, uint64_t *misses,
				 uint64_t *evictions)
{
	if (hits)
		*hits = c->hits;
	if (misses)
		*misses = c->misses;
	if (evictions)
		*evictions = c->evict;
}
