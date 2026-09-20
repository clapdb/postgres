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
#include <stddef.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "pagestore_manifest.h"
#include "pagestore_layer.h"

static int	run = 0,
			failed = 0;

#define TEST_MANIFEST_SET_REMOTE_LOCATION 7
#define TEST_MANIFEST_REBASE_FLUSH_WATERMARK 8
#define TEST_MANIFEST_MAGIC 0x504d414eU
#define TEST_MANIFEST_VERSION 3U

typedef struct TestManifestRecord
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	type;
	uint32_t	len;
	uint32_t	crc;
} TestManifestRecord;

static uint32_t
test_manifest_fnv1a(uint32_t h, const void *data, size_t len)
{
	const unsigned char *p = data;

	for (size_t i = 0; i < len; i++)
	{
		h ^= p[i];
		h *= 16777619u;
	}
	return h;
}

static uint32_t
test_manifest_crc(const TestManifestRecord *record, const void *payload,
				  uint32_t len)
{
	uint32_t crc = test_manifest_fnv1a(2166136261u, record,
								  offsetof(TestManifestRecord, crc));

	return len == 0 ? crc : test_manifest_fnv1a(crc, payload, len);
}

static int
append_manifest_record(const char *path, uint32_t type, const void *payload,
					   uint32_t len)
{
	TestManifestRecord record = {
		.magic = TEST_MANIFEST_MAGIC,
		.version = TEST_MANIFEST_VERSION,
		.type = type,
		.len = len
	};
	int fd;

	record.crc = test_manifest_crc(&record, payload, len);
	fd = open(path, O_WRONLY | O_APPEND);
	if (fd < 0)
		return -1;
	if (write(fd, &record, sizeof(record)) != (ssize_t) sizeof(record) ||
		(len > 0 && write(fd, payload, len) != (ssize_t) len) ||
		fsync(fd) != 0)
	{
		close(fd);
		return -1;
	}
	return close(fd);
}

/* Write a legacy-shaped manifest by removing only the new location event. */
static int
copy_without_remote_location(const char *src, const char *dst)
{
	int		in = open(src, O_RDONLY);
	int		out = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	TestManifestRecord rec;
	char	   *payload = NULL;
	int		ok = 0;

	if (in < 0 || out < 0)
		goto done;
	while (read(in, &rec, sizeof(rec)) == (ssize_t) sizeof(rec))
	{
		payload = malloc(rec.len);
		if ((rec.len > 0 && payload == NULL) ||
			(rec.len > 0 && read(in, payload, rec.len) != (ssize_t) rec.len))
			goto done;
		if (rec.type != TEST_MANIFEST_SET_REMOTE_LOCATION &&
			(write(out, &rec, sizeof(rec)) != (ssize_t) sizeof(rec) ||
			 (rec.len > 0 && write(out, payload, rec.len) != (ssize_t) rec.len)))
			goto done;
		free(payload);
		payload = NULL;
	}
	ok = fsync(out) == 0;
done:
	free(payload);
	if (in >= 0)
		close(in);
	if (out >= 0)
		close(out);
	return ok ? 0 : -1;
}

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
	char		legacy_dir[] = "/tmp/psmanifestlegacyXXXXXX";
	char		mpath[4096];
	char		legacy_path[4096];
	PsLayerDesc rel,
				slru;
	PsLayerDesc *got;
	PsFlushWatermark watermark;
	PsLayerLocation remote;

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
	memset(&remote, 0, sizeof(remote));
	remote.tier = PS_LAYER_TIER_REMOTE_OBJECT;
	snprintf(remote.uri, sizeof(remote.uri), "objects/layer-%llu",
			 (unsigned long long) slru.layer_id);
	remote.size = slru.locations[0].size;
	remote.generation = 1;
	remote.available = true;
	check(ps_manifest_set_remote_durable(slru.layer_id, 0x2000) != 0,
		  "remote durability requires a persisted remote location");
	check(ps_manifest_set_remote_location(slru.layer_id, &remote) == 0,
		  "persist remote location");
	check(ps_manifest_set_remote_location(slru.layer_id, &remote) != 0,
		  "reject a second remote location for an immutable layer");
	check(ps_manifest_set_remote_durable(slru.layer_id, 0x2000) == 0,
		  "mark uploaded remote location durable");
	check(ps_manifest_set_flush_watermark(3, 7, 12345) == 0,
		  "persist flush watermark");
	check(ps_manifest_rebase_flush_watermark(3, 7, 12344) == 0,
		  "persist a conservative flush watermark rebase");
	check(ps_manifest_set_flush_watermark(3, 7, 12343) != 0,
		  "reject regressing flush watermark");
	ps_manifest_close();
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
	if (!mkdtemp(legacy_dir))
	{
		fprintf(stderr, "legacy setup failed\n");
		return 2;
	}
	snprintf(legacy_path, sizeof(legacy_path), "%s/layers.manifest", legacy_dir);
	check(copy_without_remote_location(mpath, legacy_path) == 0,
		  "construct legacy remote-durable manifest without URI");

	/* --- restart: re-open and replay the manifest into a fresh map --------- */
	/* ps_manifest_close() already freed the in-memory map; reopen re-inits it. */
	if (ps_manifest_open(dir) != 0)
	{
		fprintf(stderr, "manifest reopen failed\n");
		return 2;
	}
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay succeeds");
	check(ps_layer_map_count(&ps_layer_map) == 2, "both layers replayed");
	check(ps_manifest_get_flush_watermark(3, &watermark) == 1 &&
		  watermark.seg_id == 7 && watermark.seg_off == 12344,
		  "flush watermark replays");

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
		check(got->location_count == 2 &&
			  got->locations[0].tier == PS_LAYER_TIER_LOCAL_HOT,
			  "local location round-trips");
		check(got->remote_durable && got->remote_uploaded_lsn == 0x2000,
			  "remote durability round-trips");
		check(got->location_count == 2 &&
			  got->locations[1].tier == PS_LAYER_TIER_REMOTE_OBJECT &&
			  strcmp(got->locations[1].uri, remote.uri) == 0 &&
			  got->locations[1].available,
			  "remote location round-trips");
	}

	check(ps_manifest_compact() == 0, "manifest compaction succeeds");
	ps_manifest_close();
	check(ps_manifest_open(dir) == 0, "open compacted manifest");
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay compacted manifest");
	check(ps_manifest_get_flush_watermark(3, &watermark) == 1 &&
		  watermark.seg_id == 7 && watermark.seg_off == 12344,
		  "manifest compaction preserves flush watermark");
	got = NULL;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == slru.layer_id)
			got = &ps_layer_map.layers[i];
	check(got != NULL && got->remote_durable && got->location_count == 2 &&
		  got->locations[1].tier == PS_LAYER_TIER_REMOTE_OBJECT,
		  "manifest compaction preserves remote location and durability");

	ps_manifest_close();		/* frees the in-memory map */
	{
		PsFlushWatermark invalid = {.shard = 3, .seg_id = 8, .seg_off = 0};

		check(append_manifest_record(mpath,
								TEST_MANIFEST_REBASE_FLUSH_WATERMARK,
								&invalid, sizeof(invalid)) == 0,
				  "append an invalid watermark-rebase event");
		check(ps_manifest_open(dir) == 0 &&
			  ps_manifest_replay(&ps_layer_map) != 0,
			  "replay rejects a rebase for the wrong segment");
		ps_manifest_close();
		unlink(mpath);
	}
	check(ps_manifest_open(legacy_dir) == 0, "open legacy remote-durable manifest");
	check(ps_manifest_replay(&ps_layer_map) == 0,
		  "legacy remote-durable manifest replays without a URI");
	got = NULL;
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == slru.layer_id)
			got = &ps_layer_map.layers[i];
	check(got != NULL && !got->remote_durable && got->location_count == 1,
		  "legacy remote durability is downgraded for re-upload");
	check(ps_manifest_set_remote_location(slru.layer_id, &remote) == 0 &&
		  ps_manifest_set_remote_durable(slru.layer_id, 0x3000) == 0,
		  "legacy layer accepts a new location and re-upload");
	ps_manifest_close();

	/* best-effort cleanup */
	unlink(mpath);
	rmdir(dir);
	unlink(legacy_path);
	rmdir(legacy_dir);

	printf("pagestore_manifest_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
