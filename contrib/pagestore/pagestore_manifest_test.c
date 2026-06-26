/*-------------------------------------------------------------------------
 *
 * pagestore_manifest_test.c
 *	  Standalone unit test for the durable layer manifest (pagestore_manifest.c):
 *	  add layers, replay the log into a fresh map, and verify the decoded
 *	  descriptors round-trip.  Runs with no daemon and no PostgreSQL.
 *
 * Regression coverage: manifest_decode_key() must restore PsKey.klass.  Before
 * the fix, encode wrote klass to disk (manifest v2) but decode dropped it, so a
 * replayed non-relation (SLRU/control) layer key came back as PS_KLASS_RELATION
 * and its lookups were pruned after a daemon restart.
 *
 * Usage: pagestore_manifest_test
 * Exit status: 0 = all checks passed, 1 = one or more failed.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_manifest.h"
#include "pagestore_layer.h"

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

/* A sealed image layer covering an SLRU-class key range. */
static PsLayerDesc
make_slru_layer(uint64_t id)
{
	PsLayerDesc d;

	memset(&d, 0, sizeof(d));
	d.layer_id = id;
	d.kind = PS_LAYER_IMAGE;
	d.timeline = 1;
	/* non-relation key range -- the field the bug dropped */
	d.start_key = (PsKey) {0, 0, 1, 0, PS_KLASS_SLRU};
	d.end_key = (PsKey) {0, 0, 1, 0, PS_KLASS_SLRU};
	d.start_block = 0;
	d.end_block = 7;
	d.lsn_start = 0x1000;
	d.lsn_end = 0x2000;
	d.location_count = 1;
	d.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
	snprintf(d.locations[0].uri, sizeof(d.locations[0].uri), "layer-%llu",
			 (unsigned long long) id);
	d.locations[0].size = 8192;
	d.locations[0].available = true;
	d.created_at_lsn = 0x1800;
	return d;
}

int
main(void)
{
	char		dir[] = "/tmp/psmanifesttestXXXXXX";
	char		mpath[4096];
	PsLayerDesc rel,
				slru;
	PsLayerDesc *got;

	if (!mkdtemp(dir))
	{
		fprintf(stderr, "setup failed\n");
		return 2;
	}

	/* --- write side: persist one relation-class and one SLRU-class layer --- */
	if (ps_manifest_open(dir) != 0)
	{
		fprintf(stderr, "manifest open failed\n");
		return 2;
	}
	rel = make_slru_layer(1);
	rel.start_key.klass = PS_KLASS_RELATION;	/* a relation layer for contrast */
	rel.end_key.klass = PS_KLASS_RELATION;
	slru = make_slru_layer(2);
	check(ps_manifest_add_layer(&rel) == 0, "add relation layer");
	check(ps_manifest_add_layer(&slru) == 0, "add SLRU layer");
	ps_manifest_close();

	/* --- restart: re-open and replay the manifest into a fresh map --------- */
	/* ps_manifest_close() already freed the in-memory map; reopen re-inits it. */
	if (ps_manifest_open(dir) != 0)
	{
		fprintf(stderr, "manifest reopen failed\n");
		return 2;
	}
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay succeeds");
	check(ps_layer_map_count(&ps_layer_map) == 2, "both layers replayed");

	/* layer 1: relation class survives */
	got = NULL;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == 1)
			got = &ps_layer_map.layers[i];
	check(got != NULL, "relation layer present after replay");
	if (got)
	{
		check(got->start_key.klass == PS_KLASS_RELATION,
			  "relation layer start_key.klass == RELATION");
		check(got->end_key.klass == PS_KLASS_RELATION,
			  "relation layer end_key.klass == RELATION");
	}

	/* layer 2: SLRU class must survive replay (the regression) */
	got = NULL;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == 2)
			got = &ps_layer_map.layers[i];
	check(got != NULL, "SLRU layer present after replay");
	if (got)
	{
		check(got->start_key.klass == PS_KLASS_SLRU,
			  "SLRU layer start_key.klass survives replay (decode restores klass)");
		check(got->end_key.klass == PS_KLASS_SLRU,
			  "SLRU layer end_key.klass survives replay (decode restores klass)");
		/* a couple of other fields, to confirm the whole descriptor round-trips */
		check(got->kind == PS_LAYER_IMAGE, "kind round-trips");
		check(got->timeline == 1, "timeline round-trips");
		check(got->lsn_start == 0x1000 && got->lsn_end == 0x2000,
			  "lsn range round-trips");
		check(got->location_count == 1, "location round-trips");
	}

	ps_manifest_close();		/* frees the in-memory map */

	/* best-effort cleanup */
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
	unlink(mpath);
	rmdir(dir);

	printf("pagestore_manifest_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
