/*-------------------------------------------------------------------------
 *
 * pagestore_gc_test.c
 *	  Standalone unit test for crash-safe compaction/GC recovery: a layer left
 *	  "deleting" by a crash (marked in the manifest but not yet removed) must be
 *	  recoverable, the merged survivor must stay readable across the crash
 *	  window, and resume must be durable and idempotent.  No daemon, no PG.
 *
 * This exercises the manifest + local layer store with real on-disk layer files
 * and replicates gc_resume()'s loop (which is static in pagestore_core.c):
 * delete the file, then drop the manifest entry only on success.
 *
 * Usage: pagestore_gc_test
 * Exit status: 0 = all checks passed, 1 = one or more failed.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_manifest.h"
#include "pagestore_layer.h"
#include "pagestore_layer_store.h"

#define PSZ 8192

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

static int
file_exists(const char *path)
{
	struct stat st;

	return stat(path, &st) == 0;
}

static PsLayerDesc *
find_layer(uint64_t id)
{
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == id)
			return &ps_layer_map.layers[i];
	return NULL;
}

/*
 * Replica of pagestore_core.c's gc_resume() (static there): for each layer the
 * manifest still marks "deleting", delete the local file, then drop the manifest
 * entry only once the file is gone.  Snapshot the deleting set first since
 * ps_manifest_remove_layer() mutates the map.
 */
static void
gc_resume_like(void)
{
	PsLayerDesc dead[16];
	uint32_t	n = 0;

	for (uint32_t i = 0; i < ps_layer_map.nlayers && n < 16; i++)
		if (ps_layer_map.layers[i].deleting)
			dead[n++] = ps_layer_map.layers[i];
	for (uint32_t k = 0; k < n; k++)
		if (ps_layer_store->delete_local_layer(&dead[k]) == 0)
			ps_manifest_remove_layer(dead[k].layer_id);
}

static PsLayerDesc
write_layer(uint64_t id, unsigned char fill)
{
	static unsigned char pg[PSZ];
	PsKey		k = {1, 1, 5, 0, PS_KLASS_RELATION};
	PsImgRec	rec;
	PsLayerDesc d;

	memset(pg, fill, PSZ);
	rec = (PsImgRec) {k, 0, 100, pg};
	memset(&d, 0, sizeof(d));
	if (ps_image_layer_write(id, 0, &rec, 1, PSZ, &d) != 0)
	{
		fprintf(stderr, "write_layer %llu failed\n", (unsigned long long) id);
		exit(2);
	}
	return d;
}

int
main(void)
{
	char		dir[] = "/tmp/psgctestXXXXXX";
	PsLayerDesc a,
				b;
	char		a_uri[PS_LAYER_URI_MAX],
				b_uri[PS_LAYER_URI_MAX];
	PsKey		k = {1, 1, 5, 0, PS_KLASS_RELATION};
	unsigned char out[PSZ];
	PsLayerDesc *pa,
			   *pb;

	if (!mkdtemp(dir) || ps_layer_store->open(dir) != 0 ||
		ps_manifest_open(dir) != 0)
	{
		fprintf(stderr, "setup failed\n");
		return 2;
	}

	/*
	 * Compaction wrote a merged layer B and recorded it, then started removing
	 * the old layer A -- install-new-before-delete-old.  Simulate a crash right
	 * after A is marked deleting but before it is removed.
	 */
	a = write_layer(1, 0xA1);
	check(ps_manifest_add_layer(&a) == 0, "manifest add old layer A");
	b = write_layer(2, 0xB2);
	check(ps_manifest_add_layer(&b) == 0, "manifest add merged layer B");
	snprintf(a_uri, sizeof(a_uri), "%s", a.locations[0].uri);
	snprintf(b_uri, sizeof(b_uri), "%s", b.locations[0].uri);
	check(ps_manifest_mark_delete(a.layer_id) == 0, "mark A deleting");
	ps_manifest_close();		/* simulate process exit; files remain on disk */

	/* --- restart: replay the manifest --- */
	check(ps_manifest_open(dir) == 0, "reopen manifest");
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay after crash");
	check(ps_layer_map_count(&ps_layer_map) == 2, "both layers present after replay");

	pa = find_layer(1);
	pb = find_layer(2);
	check(pa != NULL && pa->deleting,
		  "A replays as deleting (interrupted GC is recoverable, not lost)");
	check(pb != NULL && !pb->deleting, "B replays live");
	check(file_exists(a_uri),
		  "A's file is still present (install-before-delete kept the data safe)");
	check(file_exists(b_uri) && pb != NULL &&
		  ps_image_layer_lookup(pb, &k, 0, 1000, out, PSZ, NULL) == 1 &&
		  out[0] == 0xB2,
		  "merged layer B is readable through the entire crash window");

	/* --- gc_resume finishes the interrupted deletion --- */
	gc_resume_like();
	check(!file_exists(a_uri), "resume removed A's local file");
	check(ps_layer_map_count(&ps_layer_map) == 1, "resume removed A from the map");

	/* --- restart again: the removal is durable --- */
	ps_manifest_close();
	check(ps_manifest_open(dir) == 0, "reopen manifest after resume");
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay after resume");
	check(ps_layer_map_count(&ps_layer_map) == 1, "only B survives (durable)");
	check(find_layer(2) != NULL && find_layer(1) == NULL, "B durable, A gone");

	/* --- resume is idempotent: nothing deleting, nothing to do --- */
	gc_resume_like();
	check(ps_layer_map_count(&ps_layer_map) == 1, "second resume is a no-op");

	ps_manifest_close();

	/* best-effort cleanup */
	unlink(b_uri);
	{
		char		mpath[4096];

		snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
		unlink(mpath);
	}
	rmdir(dir);

	printf("pagestore_gc_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
