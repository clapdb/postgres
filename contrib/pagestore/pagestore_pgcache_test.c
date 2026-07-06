/*-------------------------------------------------------------------------
 *
 * pagestore_pgcache_test.c
 *	  Standalone unit test for the scan-resistant materialized-page cache.
 *	  No daemon, no PostgreSQL.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <string.h>

#include "pagestore_pgcache.h"

static int	run = 0,
			failed = 0;

static void
check(int cond, const char *msg)
{
	run++;
	if (!cond)
	{
		failed++;
		fprintf(stderr, "  FAIL: %s\n", msg);
	}
}

#define PSZ 64

static void
fill(unsigned char *p, unsigned char v)
{
	memset(p, v, PSZ);
}

int
main(void)
{
	PsKey		k = {1, 1, 5, 0, PS_KLASS_RELATION};
	unsigned char in[PSZ],
				out[PSZ];
	uint64_t	hits,
				misses,
				evict;

	ps_pgcache_init(4, PSZ);

	/* basic insert + hit */
	fill(in, 0xAA);
	ps_pgcache_insert(0, &k, 7, 100, in);
	check(ps_pgcache_lookup(0, &k, 7, 100, out) == 1 && out[0] == 0xAA,
		  "insert then hit");

	/* miss on absent block / lsn */
	check(ps_pgcache_lookup(0, &k, 8, 100, out) == 0, "miss on other block");
	check(ps_pgcache_lookup(0, &k, 7, 200, out) == 0, "miss on other version_lsn");

	/* version_lsn keying: two versions of the same (key,block) coexist */
	fill(in, 0xBB);
	ps_pgcache_insert(0, &k, 7, 200, in);
	check(ps_pgcache_lookup(0, &k, 7, 100, out) == 1 && out[0] == 0xAA &&
		  ps_pgcache_lookup(0, &k, 7, 200, out) == 1 && out[0] == 0xBB,
		  "two versions keyed by lsn");

	/* --- scan resistance: a hot page re-referenced each round survives a
	 * stream of one-shot scan pages; the scan pages churn through the rest --- */
	ps_pgcache_free();
	ps_pgcache_init(4, PSZ);
	fill(in, 0x11);
	ps_pgcache_insert(0, &k, 1, 1, in);	/* hot page H = (block 1, lsn 1) */
	ps_pgcache_lookup(0, &k, 1, 1, out);	/* reference it */
	for (uint32_t i = 0; i < 3; i++)	/* fillers to capacity */
	{
		fill(in, (unsigned char) (0x20 + i));
		ps_pgcache_insert(0, &k, 100 + i, 1, in);
	}
	for (uint32_t i = 0; i < 16; i++)	/* long one-shot scan, re-touching H */
	{
		fill(in, 0x80);
		ps_pgcache_insert(0, &k, 1000 + i, 1, in);
		ps_pgcache_lookup(0, &k, 1, 1, out);	/* H stays hot */
	}
	check(ps_pgcache_lookup(0, &k, 1, 1, out) == 1 && out[0] == 0x11,
		  "hot page survives a long scan (scan-resistant)");
	check(ps_pgcache_lookup(0, &k, 1000, 1, out) == 0,
		  "early scan page evicted");

	ps_pgcache_stats(&hits, &misses, &evict);
	check(evict > 0, "evictions occurred under pressure");

	ps_pgcache_free();
	fprintf(stderr, "%d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
