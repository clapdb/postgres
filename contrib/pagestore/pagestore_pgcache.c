/*-------------------------------------------------------------------------
 *
 * pagestore_pgcache.c
 *	  Scan-resistant CLOCK materialized-page cache.  See pagestore_pgcache.h.
 *
 *-------------------------------------------------------------------------
 */
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_pgcache.h"

/*
 * Reads run concurrently across shard workers (under a shared read lock) and
 * both lookup and insert mutate the CLOCK state (ref bits, hand, buckets,
 * counters), so serialize cache access with this lock.  Writers hold the
 * daemon's exclusive lock and SPDK completions insert here too; all go through
 * these functions, so a single cache lock covers every mutator.
 */
static pthread_mutex_t cache_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct PgKey
{
	uint32_t	timeline;
	uint32_t	block;
	uint64_t	lsn;
	uint64_t	admission_seq;
	PsKey		key;
} PgKey;

typedef struct Slot
{
	int			valid;
	uint8_t		ref;			/* CLOCK reference bit */
	PgKey		k;
	int			hnext;			/* next slot in its hash bucket, or -1 */
} Slot;

static uint32_t cache_max;		/* capacity in pages (0 = disabled) */
static uint32_t cache_psz;
static Slot *slots;
static unsigned char *pages;	/* cache_max * cache_psz, slot i at i*psz */
static int *buckets;			/* nbuckets heads (slot index or -1) */
static uint32_t nbuckets;
static uint32_t nused;			/* slots populated so far (<= cache_max) */
static uint32_t hand;			/* CLOCK hand */
static uint64_t st_hits,
			st_misses,
			st_evict;

static uint32_t
key_hash(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn,
		 uint64_t admission_seq)
{
	uint64_t	h = 1469598103934665603ULL;	/* FNV-1a over the fields */
	const uint64_t pr = 1099511628211ULL;
	uint64_t	parts[7];

	parts[0] = tl;
	parts[1] = key->spcOid;
	parts[2] = ((uint64_t) key->dbOid << 32) | key->relNumber;
	parts[3] = key->forkNum;
	parts[4] = block;
	parts[5] = lsn;
	parts[6] = admission_seq;
	for (int i = 0; i < 7; i++)
		h = (h ^ parts[i]) * pr;
	return (uint32_t) (h ^ (h >> 32));
}

static int
key_eq(const PgKey *a, uint32_t tl, const PsKey *key, uint32_t block,
	   uint64_t lsn, uint64_t admission_seq)
{
	return a->timeline == tl && a->block == block && a->lsn == lsn &&
		a->admission_seq == admission_seq &&
		a->key.spcOid == key->spcOid && a->key.dbOid == key->dbOid &&
		a->key.relNumber == key->relNumber && a->key.forkNum == key->forkNum &&
		a->key.klass == key->klass;
}

void
ps_pgcache_init(uint32_t max_pages, uint32_t page_size)
{
	cache_max = max_pages;
	cache_psz = page_size;
	st_hits = st_misses = st_evict = 0;
	nused = hand = 0;
	if (max_pages == 0)
		return;
	nbuckets = max_pages * 2 + 1;
	slots = calloc(max_pages, sizeof(Slot));
	pages = malloc((size_t) max_pages * page_size);
	buckets = malloc((size_t) nbuckets * sizeof(int));
	if (!slots || !pages || !buckets)
	{
		ps_pgcache_free();
		cache_max = 0;
		return;
	}
	for (uint32_t i = 0; i < nbuckets; i++)
		buckets[i] = -1;
}

void
ps_pgcache_free(void)
{
	free(slots);
	free(pages);
	free(buckets);
	slots = NULL;
	pages = NULL;
	buckets = NULL;
	cache_max = 0;
}

static int
find_slot(uint32_t tl, const PsKey *key, uint32_t block, uint64_t lsn,
		  uint64_t admission_seq, uint32_t *out_bucket)
{
	uint32_t	b = key_hash(tl, key, block, lsn, admission_seq) % nbuckets;

	if (out_bucket)
		*out_bucket = b;
	for (int i = buckets[b]; i >= 0; i = slots[i].hnext)
		if (slots[i].valid && key_eq(&slots[i].k, tl, key, block, lsn,
								 admission_seq))
			return i;
	return -1;
}

int
ps_pgcache_lookup(uint32_t tl, const PsKey *key, uint32_t block,
				  uint64_t version_lsn, uint64_t admission_seq, void *out)
{
	int			s;

	if (cache_max == 0)
		return 0;
	pthread_mutex_lock(&cache_lock);
	s = find_slot(tl, key, block, version_lsn, admission_seq, NULL);
	if (s < 0)
	{
		st_misses++;
		pthread_mutex_unlock(&cache_lock);
		return 0;
	}
	slots[s].ref = 1;			/* re-reference: survives the next sweep */
	memcpy(out, pages + (size_t) s * cache_psz, cache_psz);
	st_hits++;
	pthread_mutex_unlock(&cache_lock);
	return 1;
}

static void bucket_remove(int s);

/*
 * Drop a cached page, if present.  A same-LSN rewrite (e.g. a hint-bit-only
 * relation write that does not advance pd_lsn) replaces the authoritative
 * bytes under an unchanged (tl, key, block, lsn) cache key, so the writer
 * must invalidate the entry or later reads keep serving the stale image.
 */
void
ps_pgcache_invalidate(uint32_t tl, const PsKey *key, uint32_t block,
					  uint64_t version_lsn, uint64_t admission_seq)
{
	int			s;

	if (cache_max == 0)
		return;
	pthread_mutex_lock(&cache_lock);
	s = find_slot(tl, key, block, version_lsn, admission_seq, NULL);
	if (s >= 0)
	{
		bucket_remove(s);
		slots[s].valid = 0;
		slots[s].ref = 0;
	}
	pthread_mutex_unlock(&cache_lock);
}

void
ps_pgcache_invalidate_timeline(uint32_t timeline)
{
	if (cache_max == 0)
		return;
	pthread_mutex_lock(&cache_lock);
	for (uint32_t i = 0; i < cache_max; i++)
		if (slots[i].valid && slots[i].k.timeline == timeline)
		{
			bucket_remove((int) i);
			slots[i].valid = 0;
			slots[i].ref = 0;
		}
	pthread_mutex_unlock(&cache_lock);
}

/* unlink slot s from its hash bucket chain */
static void
bucket_remove(int s)
{
	uint32_t	b = key_hash(slots[s].k.timeline, &slots[s].k.key,
							 slots[s].k.block, slots[s].k.lsn,
							 slots[s].k.admission_seq) % nbuckets;
	int		   *pp = &buckets[b];

	while (*pp >= 0 && *pp != s)
		pp = &slots[*pp].hnext;
	if (*pp == s)
		*pp = slots[s].hnext;
}

void
ps_pgcache_insert(uint32_t tl, const PsKey *key, uint32_t block,
				  uint64_t version_lsn, uint64_t admission_seq,
				  const void *page)
{
	uint32_t	b;
	int			s;

	if (cache_max == 0)
		return;
	pthread_mutex_lock(&cache_lock);
	s = find_slot(tl, key, block, version_lsn, admission_seq, &b);
	if (s >= 0)
	{							/* already present: refresh bytes + reference */
		memcpy(pages + (size_t) s * cache_psz, page, cache_psz);
		slots[s].ref = 1;
		pthread_mutex_unlock(&cache_lock);
		return;
	}

	if (nused < cache_max)
		s = (int) nused++;		/* still filling */
	else
	{
		/* CLOCK: advance over referenced slots (clearing), evict first ref=0 */
		while (slots[hand].ref)
		{
			slots[hand].ref = 0;
			hand = (hand + 1) % cache_max;
		}
		s = (int) hand;
		hand = (hand + 1) % cache_max;
		bucket_remove(s);
		st_evict++;
	}

	slots[s].valid = 1;
	slots[s].ref = 0;			/* scan-resistant: new entries start unreferenced */
	slots[s].k.timeline = tl;
	slots[s].k.key = *key;
	slots[s].k.block = block;
	slots[s].k.lsn = version_lsn;
	slots[s].k.admission_seq = admission_seq;
	memcpy(pages + (size_t) s * cache_psz, page, cache_psz);
	slots[s].hnext = buckets[b];
	buckets[b] = s;
	pthread_mutex_unlock(&cache_lock);
}

void
ps_pgcache_stats(uint64_t *hits, uint64_t *misses, uint64_t *evictions)
{
	if (hits)
		*hits = st_hits;
	if (misses)
		*misses = st_misses;
	if (evictions)
		*evictions = st_evict;
}
