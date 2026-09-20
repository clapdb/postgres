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
	PsKey		k5 = {1, 1, 5, 0, PS_KLASS_RELATION};	/* relation 5, block 0 */
	PsKey		k6 = {1, 1, 6, 0, PS_KLASS_RELATION};	/* relation 6, block 0 */
	PsKey		k9 = {1, 1, 9, 0, PS_KLASS_RELATION};	/* absent */
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
			{.key = k5, .block = 0, .lsn = 100, .page = pg[0]},
			{.key = k5, .block = 0, .lsn = 300, .page = pg[2]},
			{.key = k5, .block = 0, .lsn = 200, .page = pg[1]},
			{.key = k5, .block = 1, .lsn = 150, .page = pg[3]},
			{.key = k6, .block = 0, .lsn = 250, .page = pg[4]},
		};
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;

		recs[1].growth_lsn = 275;
		recs[1].order_id = 44;
		recs[1].seg_id = 3;
		recs[1].seg_off = 1234;
		recs[1].flags = PS_IMG_REC_SEG_VALID | PS_IMG_REC_ORDERED;

		check(ps_image_layer_write(7, 0, recs, 5, psz, &d) == 0,
			  "write image layer");
		check(ps_image_layer_read_index(&d, &idx, &nidx) == 0 && nidx == 5,
			  "read image v3 index");
		if (idx && nidx == 5)
		{
			PsImgIndexEnt *e = NULL;

			for (uint32_t i = 0; i < nidx; i++)
				if (idx[i].lsn == 300)
					e = &idx[i];
			check(e && e->growth_lsn == 275 && e->order_id == 44 &&
				  e->seg_id == 3 && e->seg_off == 1234 &&
				  e->flags == (PS_IMG_REC_SEG_VALID | PS_IMG_REC_ORDERED),
				  "image v3 recovery metadata round-trips");
		}
		free(idx);
	}

	check(d.start_key.relNumber == 5 && d.end_key.relNumber == 6,
		  "layer key range");
	check(d.lsn_start == 100 && d.lsn_end == 300, "layer lsn range");

	/* newest version <= read_lsn */
	r = ps_image_layer_lookup(&d, &k5, 0, 250, 0, out, psz, NULL, NULL);
	check(r == 1 && out[0] == 0xA2, "(5,0)@<=250 -> version 200");
	r = ps_image_layer_lookup(&d, &k5, 0, 1000, 0, out, psz, NULL, NULL);
	check(r == 1 && out[0] == 0xA3, "(5,0)@<=1000 -> version 300");
	r = ps_image_layer_lookup(&d, &k5, 0, 100, 0, out, psz, NULL, NULL);
	check(r == 1 && out[0] == 0xA1, "(5,0)@<=100 -> version 100 (exact)");
	r = ps_image_layer_lookup(&d, &k5, 0, 50, 0, out, psz, NULL, NULL);
	check(r == 0, "(5,0)@<=50 -> no version (older than oldest)");
	r = ps_image_layer_lookup(&d, &k5, 1, 1000, 0, out, psz, NULL, NULL);
	check(r == 1 && out[0] == 0xB1, "(5,1) -> version 150");
	r = ps_image_layer_lookup(&d, &k6, 0, 1000, 0, out, psz, NULL, NULL);
	check(r == 1 && out[0] == 0xC1, "(6,0) -> version 250");
	r = ps_image_layer_lookup(&d, &k9, 0, 1000, 0, out, psz, NULL, NULL);
	check(r == 0, "absent key -> no version");

	/* GC must force a fresh checksum even if an earlier lookup cached success. */
	{
		unsigned char bad = 0,
					  good = 0xA1;
		int			fd = open(d.locations[0].uri, O_WRONLY);

		check(fd >= 0 && pwrite(fd, &bad, 1, 0) == 1 && close(fd) == 0,
			  "corrupt image bytes after cached verification");
		check(ps_image_layer_verify_data(&d, psz) != 0,
			  "forced data verification detects post-read corruption");
		check(!d.data_verified, "failed forced verification clears cached success");
		fd = open(d.locations[0].uri, O_WRONLY);
		check(fd >= 0 && pwrite(fd, &good, 1, 0) == 1 && close(fd) == 0 &&
			  ps_image_layer_verify_data(&d, psz) == 0,
			  "restored image bytes pass forced verification");
	}

	/* --- delta layer: ordered collect in an LSN range --- */
	{
		PsLayerDesc dd;
		PsDeltaRec	drecs[4] = {
			{k5, 0, 300, "D300", 4}, {k5, 0, 100, "D100", 4},
			{k5, 0, 200, "DD200", 5}, {k5, 1, 150, "E150", 4},
		};
		PsDeltaOut	outs[8];
		uint32_t	dn = 0;
		unsigned char dbuf[16];

		check(ps_delta_layer_write(8, 0, drecs, 4, &dd) == 0, "write delta layer");
		check(dd.kind == PS_LAYER_DELTA && dd.lsn_start == 100 &&
			  dd.lsn_end == 300, "delta layer kind + lsn range");

		/* (5,0) in (100, 300]: expect 200 then 300, ascending; 100 excluded */
		r = ps_delta_layer_collect(&dd, &k5, 0, 100, 300, outs, 8, &dn);
		check(r == 0 && dn == 2, "delta collect (100,300] -> 2 records");
		check(outs[0].lsn == 200 && outs[1].lsn == 300, "deltas in ascending LSN");
		check(ps_layer_store->read_layer_block(&dd, outs[0].data_off, dbuf,
											   outs[0].data_len) == 0 &&
			  outs[0].data_len == 5 && memcmp(dbuf, "DD200", 5) == 0,
			  "delta payload readable (200)");

		dn = 0;
		r = ps_delta_layer_collect(&dd, &k5, 0, 0, 1000, outs, 8, &dn);
		check(r == 0 && dn == 3, "delta collect (0,1000] -> all 3 of (5,0)");
		dn = 0;
		r = ps_delta_layer_collect(&dd, &k5, 1, 0, 1000, outs, 8, &dn);
		check(r == 0 && dn == 1 && outs[0].lsn == 150, "delta collect other block");
	}

	{
		PsLayerDesc huge;
		PsDeltaFooter foot;
		char		uri[PS_LAYER_URI_MAX];
		uint64_t	size = (uint64_t) UINT32_MAX + 1 + sizeof(foot);
		int			fd;

		memset(&huge, 0, sizeof(huge));
		memset(&foot, 0, sizeof(foot));
		foot.magic = PS_DELTA_MAGIC;
		foot.version = PS_DELTA_VERSION;
		foot.index_off = (uint64_t) UINT32_MAX + 1;
		check(ps_layer_store->create_local_layer(11, uri, sizeof(uri)) == 0,
			  "create oversized delta layer shell");
		fd = open(uri, O_WRONLY);
		check(fd >= 0 &&
			  pwrite(fd, &foot, sizeof(foot),
					 (off_t) (size - sizeof(foot))) == (ssize_t) sizeof(foot) &&
			  close(fd) == 0,
			  "write sparse oversized delta footer");
		huge.layer_id = 11;
		huge.kind = PS_LAYER_DELTA;
		huge.location_count = 1;
		huge.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
		huge.locations[0].available = true;
		huge.locations[0].size = size;
		snprintf(huge.locations[0].uri, sizeof(huge.locations[0].uri), "%s", uri);
		check(ps_delta_layer_verify_data(&huge) != 0,
			  "delta verifier rejects sections above the 32-bit read limit");
		unlink(uri);
	}

	/* --- read plan: base image + ordered delta chain --- */
	{
		PsLayerDesc img,
					dl;
		PsLayerMap	map;
		PsReadPlan	plan;
		PsImgRec	irecs[2] = {
			{.key = k5, .block = 0, .lsn = 100, .page = pg[0]},
			{.key = k5, .block = 0, .lsn = 200, .page = pg[1]},
		};
		PsDeltaRec	drecs[3] = {
			{k5, 0, 150, "x150", 4}, {k5, 0, 250, "x250", 4}, {k5, 0, 300, "x300", 4},
		};

		check(ps_image_layer_write(9, 0, irecs, 2, psz, &img) == 0, "plan: image");
		check(ps_delta_layer_write(10, 0, drecs, 3, &dl) == 0, "plan: delta");
		ps_layer_map_init(&map);
		ps_layer_map_add(&map, &img);
		ps_layer_map_add(&map, &dl);

		/* @250: base=image@200 (0xA2); deltas in (200,250] = {250} */
		check(ps_read_plan_build(&map, 0, &k5, 0, 250, psz, &plan) == 0, "plan@250");
		check(plan.has_base && plan.base_lsn == 200 && plan.base[0] == 0xA2,
			  "plan@250 base = image@200");
		check(plan.ndelta == 1 && plan.deltas[0].lsn == 250 &&
			  memcmp(plan.deltas[0].bytes, "x250", 4) == 0, "plan@250 one delta");
		ps_read_plan_free(&plan);

		/* @300: base=image@200; deltas in (200,300] = {250,300} ascending */
		check(ps_read_plan_build(&map, 0, &k5, 0, 300, psz, &plan) == 0, "plan@300");
		check(plan.base_lsn == 200 && plan.ndelta == 2 &&
			  plan.deltas[0].lsn == 250 && plan.deltas[1].lsn == 300,
			  "plan@300 chain {250,300}");
		ps_read_plan_free(&plan);

		/* @180: base=image@100 (0xA1); deltas in (100,180] = {150} */
		check(ps_read_plan_build(&map, 0, &k5, 0, 180, psz, &plan) == 0, "plan@180");
		check(plan.base_lsn == 100 && plan.base[0] == 0xA1 && plan.ndelta == 1 &&
			  plan.deltas[0].lsn == 150, "plan@180 base@100 + delta150");
		ps_read_plan_free(&plan);

		ps_layer_map_free(&map);
	}

	fprintf(stderr, "%d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
