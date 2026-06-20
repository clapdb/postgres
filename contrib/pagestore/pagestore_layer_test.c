/*-------------------------------------------------------------------------
 *
 * pagestore_layer_test.c
 *	  Standalone unit test for the immutable image-layer file format
 *	  (pagestore_layer.c writer/reader over the local layer store).  Runs with
 *	  no daemon and no PostgreSQL.
 *
 * Usage: pagestore_layer_test
 * Exit status: 0 = all checks passed, 1 = one or more failed.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_layer.h"
#include "pagestore_layer_store.h"

static int	run = 0,
			failed = 0;

static PsLayerBloom tb;		/* the test's bloom cache (mirrors a shard's) */

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

#define PSZ 8192

int
main(void)
{
	char		dir[] = "/tmp/pslayertestXXXXXX";
	uint32_t	psz = PSZ;
	static unsigned char pg[5][PSZ];
	PsKey		k5 = {1, 1, 5, 0};	/* relation 5, block 0 */
	PsKey		k6 = {1, 1, 6, 0};	/* relation 6, block 0 */
	PsKey		k9 = {1, 1, 9, 0};	/* absent */
	PsLayerDesc d;
	unsigned char out[PSZ];
	int			r;

	if (!mkdtemp(dir) || ps_layer_store->open(dir) != 0)
	{
		fprintf(stderr, "setup failed\n");
		return 2;
	}

	/* versions (out of insertion order on purpose):
	 *   (5,0)@100=0xA1  (5,0)@200=0xA2  (5,0)@300=0xA3
	 *   (5,1)@150=0xB1  (6,0)@250=0xC1 */
	memset(pg[0], 0xA1, psz);
	memset(pg[1], 0xA2, psz);
	memset(pg[2], 0xA3, psz);
	memset(pg[3], 0xB1, psz);
	memset(pg[4], 0xC1, psz);
	{
		PsImgRec	recs[5] = {
			{k5, 0, 100, pg[0]}, {k5, 0, 300, pg[2]}, {k5, 0, 200, pg[1]},
			{k5, 1, 150, pg[3]}, {k6, 0, 250, pg[4]},
		};

		check(ps_image_layer_write(7, 0, recs, 5, psz, &d) == 0,
			  "write image layer");
	}

	check(d.start_key.relNumber == 5 && d.end_key.relNumber == 6,
		  "layer key range");
	check(d.lsn_start == 100 && d.lsn_end == 300, "layer lsn range");

	/* newest version <= read_lsn */
	r = ps_image_layer_lookup(&d, &k5, 0, 250, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xA2, "(5,0)@<=250 -> version 200");
	r = ps_image_layer_lookup(&d, &k5, 0, 1000, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xA3, "(5,0)@<=1000 -> version 300");
	r = ps_image_layer_lookup(&d, &k5, 0, 100, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xA1, "(5,0)@<=100 -> version 100 (exact)");
	r = ps_image_layer_lookup(&d, &k5, 0, 50, out, psz, NULL, NULL, &tb);
	check(r == 0, "(5,0)@<=50 -> no version (older than oldest)");
	r = ps_image_layer_lookup(&d, &k5, 1, 1000, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xB1, "(5,1) -> version 150");
	r = ps_image_layer_lookup(&d, &k6, 0, 1000, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xC1, "(6,0) -> version 250");
	r = ps_image_layer_lookup(&d, &k9, 0, 1000, out, psz, NULL, NULL, &tb);
	check(r == 0, "absent key -> no version");

	/* bloom read cache: the lookups above built this layer's bloom from its
	 * index; present keys must still be found (no false negative) and absent keys
	 * still rejected via the cached bloom */
	r = ps_image_layer_lookup(&d, &k5, 0, 1000, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xA3, "bloom-cached: (5,0) still found");
	r = ps_image_layer_lookup(&d, &k6, 0, 1000, out, psz, NULL, NULL, &tb);
	check(r == 1 && out[0] == 0xC1, "bloom-cached: (6,0) still found");
	r = ps_image_layer_lookup(&d, &k9, 0, 1000, out, psz, NULL, NULL, &tb);
	check(r == 0, "bloom-cached: absent key still rejected");
	{
		int			ok = 1;

		for (uint32_t rel = 100; rel < 400; rel++)	/* many absent keys */
		{
			PsKey		ka = {1, 1, rel, 0};

			if (ps_image_layer_lookup(&d, &ka, 0, 1000, out, psz, NULL, NULL, &tb) != 0)
				ok = 0;
		}
		check(ok, "bloom-cached: 300 absent keys all rejected");
	}

	/* bloom cache reset: layer_ids are unique only within a store, so a process
	 * that opens a second store can see a new layer reuse an id whose slot still
	 * holds the old store's bloom.  ps_image_bloom_reset(&tb) (called by ps_core_open)
	 * must clear it.  Simulate: build the bloom for d's id (holds rel 5/6), then a
	 * different layer that reuses that id but holds rel 7 -- after a reset its
	 * absent-from-the-old-bloom key must still be found, not masked. */
	{
		PsLayerDesc reuse;
		PsKey		k7 = {1, 1, 7, 0};	/* not in d's bloom */
		PsImgRec	rr[1] = {{k7, 0, 100, pg[2]}};	/* 0xA3 */

		r = ps_image_layer_lookup(&d, &k5, 0, 1000, out, psz, NULL, NULL, &tb);
		check(r == 1, "bloom-reset: prime old bloom for the id");
		check(ps_image_layer_write(21, 0, rr, 1, psz, &reuse) == 0,
			  "bloom-reset: write reuse layer");
		reuse.layer_id = d.layer_id;	/* force the id (slot) collision */
		ps_image_bloom_reset(&tb);
		r = ps_image_layer_lookup(&reuse, &k7, 0, 1000, out, psz, NULL, NULL, &tb);
		check(r == 1 && out[0] == 0xA3,
			  "bloom-reset: reused-id layer found after reset (not masked)");
		/* that lookup cached reuse's bloom under d's id; clear it so the checks
		 * below that read d again rebuild d's own bloom */
		ps_image_bloom_reset(&tb);
	}

	/* same-lsn duplicate versions of one (key,block): ambiguous only when the
	 * bytes differ (a genuine rewrite).  Identical-byte duplicates within a layer
	 * are benign (compaction merges crash-overlapped layers into one). */
	{
		PsLayerDesc da,
					di;
		PsImgRec	dup[2] = {{k5, 0, 400, pg[0]}, {k5, 0, 400, pg[1]}};	/* differ */
		PsImgRec	idup[2] = {{k5, 0, 400, pg[2]}, {k5, 0, 400, pg[2]}};	/* same */
		int			amb = -1;

		check(ps_image_layer_write(11, 0, dup, 2, psz, &da) == 0,
			  "write same-lsn differing-bytes layer");
		r = ps_image_layer_lookup(&da, &k5, 0, 400, out, psz, NULL, &amb, &tb);
		check(r == 1 && amb == 1, "same-lsn differing bytes -> ambiguous");
		amb = -1;
		check(ps_image_layer_write(19, 0, idup, 2, psz, &di) == 0,
			  "write same-lsn identical-bytes layer");
		r = ps_image_layer_lookup(&di, &k5, 0, 400, out, psz, NULL, &amb, &tb);
		check(r == 1 && amb == 0 && out[0] == 0xA3,
			  "same-lsn identical bytes -> not ambiguous (compaction overlap)");
		amb = -1;
		r = ps_image_layer_lookup(&d, &k5, 0, 250, out, psz, NULL, &amb, &tb);
		check(r == 1 && amb == 0, "distinct lsn -> not ambiguous");
	}

	/* corrupt a page byte on disk -> lookup must reject it (per-page crc),
	 * not hand back bad bytes.  (5,0)@100 sorts first, so its page is at off 0. */
	{
		int			fd = open(d.locations[0].uri, O_WRONLY);
		unsigned char bad = 0xFF;

		check(fd >= 0 && pwrite(fd, &bad, 1, 0) == 1, "corrupt a page byte");
		if (fd >= 0)
			close(fd);
		r = ps_image_layer_lookup(&d, &k5, 0, 100, out, psz, NULL, NULL, &tb);
		check(r == -1, "corrupted page -> lookup rejects (data crc)");
		r = ps_image_layer_lookup(&d, &k5, 0, 250, out, psz, NULL, NULL, &tb);
		check(r == 1 && out[0] == 0xA2, "uncorrupted page still served");
	}

	/* --- delta layer: ordered collect in an LSN range --- */
	{
		PsLayerDesc dd;
		PsDeltaRec	drecs[4] = {
			{k5, 0, 300, "D300", 4}, {k5, 0, 100, "D100", 4},
			{k5, 0, 200, "DD200", 5}, {k5, 1, 150, "E150", 4},
		};
		PsDeltaOut *outs = NULL;	/* growable: no fixed per-layer chain cap */
		uint32_t	ocap = 0;
		uint32_t	dn = 0;
		unsigned char dbuf[16];

		check(ps_delta_layer_write(8, 0, drecs, 4, &dd) == 0, "write delta layer");
		check(dd.kind == PS_LAYER_DELTA && dd.lsn_start == 100 &&
			  dd.lsn_end == 300, "delta layer kind + lsn range");

		/* (5,0) in [100, 300): half-open -> 100 then 200, ascending; 300 excluded */
		r = ps_delta_layer_collect(&dd, &k5, 0, 100, 300, &outs, &ocap, &dn);
		check(r == 0 && dn == 2, "delta collect [100,300) -> 2 records");
		check(outs[0].lsn == 100 && outs[1].lsn == 200, "deltas in ascending LSN");
		check(ps_layer_store->read_layer_block(&dd, outs[1].data_off, dbuf,
											   outs[1].data_len) == 0 &&
			  outs[1].data_len == 5 && memcmp(dbuf, "DD200", 5) == 0,
			  "delta payload readable (200)");

		dn = 0;
		r = ps_delta_layer_collect(&dd, &k5, 0, 0, 1000, &outs, &ocap, &dn);
		check(r == 0 && dn == 3, "delta collect [0,1000) -> all 3 of (5,0)");
		dn = 0;
		r = ps_delta_layer_collect(&dd, &k5, 1, 0, 1000, &outs, &ocap, &dn);
		check(r == 0 && dn == 1 && outs[0].lsn == 150, "delta collect other block");

		/* boundary: lo == the layer's newest record (300).  [300,400) must still
		 * return that record -- the early layer-level prune must use lsn_end < lo,
		 * not <= lo, or a delta starting exactly at the base LSN is dropped. */
		dn = 0;
		r = ps_delta_layer_collect(&dd, &k5, 0, 300, 400, &outs, &ocap, &dn);
		check(r == 0 && dn == 1 && outs[0].lsn == 300,
			  "delta collect [300,400) keeps record at lo (lsn_end==lo)");

		/* a small starting buffer must grow to hold all matches (no fixed cap):
		 * collect into a fresh NULL/0 buffer and expect all 3, not an overflow */
		{
			PsDeltaOut *g = NULL;
			uint32_t	gcap = 0,
						gn = 0;

			r = ps_delta_layer_collect(&dd, &k5, 0, 0, 1000, &g, &gcap, &gn);
			check(r == 0 && gn == 3, "delta collect grows buffer -> all 3 (no cap)");
			free(g);
		}
		free(outs);
	}

	/* --- read plan: base image + ordered delta chain --- */
	{
		PsLayerDesc img,
					dl;
		PsLayerMap	map;
		PsReadPlan	plan;
		PsImgRec	irecs[2] = {{k5, 0, 100, pg[0]}, {k5, 0, 200, pg[1]}};
		PsDeltaRec	drecs[3] = {
			{k5, 0, 150, "x150", 4}, {k5, 0, 250, "x250", 4}, {k5, 0, 300, "x300", 4},
		};

		check(ps_image_layer_write(9, 0, irecs, 2, psz, &img) == 0, "plan: image");
		check(ps_delta_layer_write(10, 0, drecs, 3, &dl) == 0, "plan: delta");
		ps_layer_map_init(&map);
		ps_layer_map_add(&map, &img);
		ps_layer_map_add(&map, &dl);

		/* @250: base=image@200 (0xA2); deltas in [200,250) = {} (250 excluded) */
		check(ps_read_plan_build(&map, 0, &k5, 0, 250, psz, &plan) == 0, "plan@250");
		check(plan.has_base && plan.base_lsn == 200 && plan.base[0] == 0xA2,
			  "plan@250 base = image@200");
		check(plan.ndelta == 0, "plan@250 no deltas (250 is exclusive upper bound)");
		ps_read_plan_free(&plan);

		/* @300: base=image@200; deltas in [200,300) = {250} (300 excluded) */
		check(ps_read_plan_build(&map, 0, &k5, 0, 300, psz, &plan) == 0, "plan@300");
		check(plan.base_lsn == 200 && plan.ndelta == 1 &&
			  plan.deltas[0].lsn == 250 &&
			  memcmp(plan.deltas[0].bytes, "x250", 4) == 0, "plan@300 chain {250}");
		ps_read_plan_free(&plan);

		/* @180: base=image@100 (0xA1); deltas in [100,180) = {150} */
		check(ps_read_plan_build(&map, 0, &k5, 0, 180, psz, &plan) == 0, "plan@180");
		check(plan.base_lsn == 100 && plan.base[0] == 0xA1 && plan.ndelta == 1 &&
			  plan.deltas[0].lsn == 150, "plan@180 base@100 + delta150");
		ps_read_plan_free(&plan);

		/* corrupt a delta payload byte on disk -> the read plan must reject it
		 * (per-record crc), not feed bad WAL bytes to redo.  x150 sorts first, so
		 * its payload is at offset 0. */
		{
			int			fd = open(dl.locations[0].uri, O_WRONLY);
			unsigned char bad = 0xFF;

			check(fd >= 0 && pwrite(fd, &bad, 1, 0) == 1, "corrupt a delta byte");
			if (fd >= 0)
				close(fd);
			check(ps_read_plan_build(&map, 0, &k5, 0, 180, psz, &plan) == -1,
				  "corrupted delta payload -> read plan rejects (crc)");
		}

		ps_layer_map_free(&map);
	}

	/* --- read plan: cross-layer ambiguity + corrupt base both fail the plan --- */
	{
		PsLayerDesc a,
					b;
		PsLayerMap	map;
		PsReadPlan	plan;
		PsImgRec	ra[1] = {{k5, 0, 500, pg[0]}};
		PsImgRec	rb[1] = {{k5, 0, 500, pg[1]}};	/* same (key,block,lsn) */

		check(ps_image_layer_write(12, 0, ra, 1, psz, &a) == 0, "amb: layer A@500");
		check(ps_image_layer_write(13, 0, rb, 1, psz, &b) == 0, "amb: layer B@500");
		ps_layer_map_init(&map);
		ps_layer_map_add(&map, &a);
		ps_layer_map_add(&map, &b);
		/* same page LSN in two image layers with *different* bytes (a genuine
		 * same-lsn rewrite) -> ambiguous base -> reject (segment serves it) */
		check(ps_read_plan_build(&map, 0, &k5, 0, 600, psz, &plan) == -1,
			  "cross-layer same-lsn base, differing bytes -> plan rejects");

		/* a covering image layer that is corrupt must fail the plan, not fall back
		 * to an older base: corrupt B's page on disk, read at 500 */
		{
			int			fd = open(b.locations[0].uri, O_WRONLY);
			unsigned char bad = 0xFF;

			check(fd >= 0 && pwrite(fd, &bad, 1, 0) == 1, "amb: corrupt B page");
			if (fd >= 0)
				close(fd);
			check(ps_read_plan_build(&map, 0, &k5, 0, 600, psz, &plan) == -1,
				  "corrupt covering base layer -> plan fails (no stale fallback)");
		}
		ps_layer_map_free(&map);
	}

	/* --- read plan: overlapping delta layers expose a record only once --- */
	{
		PsLayerDesc img,
					d1,
					d2;
		PsLayerMap	map;
		PsReadPlan	plan;
		PsImgRec	irecs[1] = {{k5, 0, 100, pg[0]}};
		/* two delta layers both carrying the (5,0)@200 record (e.g. a replacement
		 * layer added before the old one is marked deleting in compaction) */
		PsDeltaRec	da[1] = {{k5, 0, 200, "y200", 4}};
		PsDeltaRec	db[1] = {{k5, 0, 200, "y200", 4}};

		check(ps_image_layer_write(14, 0, irecs, 1, psz, &img) == 0, "dedup: image");
		check(ps_delta_layer_write(15, 0, da, 1, &d1) == 0, "dedup: delta1");
		check(ps_delta_layer_write(16, 0, db, 1, &d2) == 0, "dedup: delta2");
		ps_layer_map_init(&map);
		ps_layer_map_add(&map, &img);
		ps_layer_map_add(&map, &d1);
		ps_layer_map_add(&map, &d2);
		check(ps_read_plan_build(&map, 0, &k5, 0, 300, psz, &plan) == 0,
			  "dedup: plan built");
		check(plan.ndelta == 1 && plan.deltas[0].lsn == 200,
			  "overlapping layers expose record @200 once (deduped)");
		ps_read_plan_free(&plan);
		ps_layer_map_free(&map);

		/* but two same-LSN delta records with *different* payloads are a real
		 * conflict (qsort order is arbitrary) -> fail the plan, don't guess */
		{
			PsLayerDesc d3;
			PsDeltaRec	dc[1] = {{k5, 0, 200, "zZZZ", 4}};	/* same lsn, diff bytes */
			PsLayerMap	m2;
			PsReadPlan	p2;

			check(ps_delta_layer_write(20, 0, dc, 1, &d3) == 0, "conflict: delta3");
			ps_layer_map_init(&m2);
			ps_layer_map_add(&m2, &img);
			ps_layer_map_add(&m2, &d1);		/* y200 */
			ps_layer_map_add(&m2, &d3);		/* zZZZ @200 */
			check(ps_read_plan_build(&m2, 0, &k5, 0, 300, psz, &p2) == -1,
				  "conflicting same-lsn delta payloads -> plan rejects");
			ps_layer_map_free(&m2);
		}
	}

	/* --- read plan: identical-bytes duplicate base is tolerated, not rejected --- */
	{
		PsLayerDesc a,
					b;
		PsLayerMap	map;
		PsReadPlan	plan;
		/* two image layers with the SAME (key,block,lsn) AND identical bytes --
		 * the crash-safe compaction overlap (install-new-before-delete-old) */
		PsImgRec	ra[1] = {{k5, 0, 700, pg[2]}};
		PsImgRec	rb[1] = {{k5, 0, 700, pg[2]}};

		check(ps_image_layer_write(17, 0, ra, 1, psz, &a) == 0, "dupbase: layer A");
		check(ps_image_layer_write(18, 0, rb, 1, psz, &b) == 0, "dupbase: layer B");
		ps_layer_map_init(&map);
		ps_layer_map_add(&map, &a);
		ps_layer_map_add(&map, &b);
		check(ps_read_plan_build(&map, 0, &k5, 0, 800, psz, &plan) == 0 &&
			  plan.has_base && plan.base_lsn == 700 && plan.base[0] == 0xA3,
			  "identical duplicate base across layers -> tolerated (not ambiguous)");
		ps_read_plan_free(&plan);
		ps_layer_map_free(&map);
	}

	fprintf(stderr, "%d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
