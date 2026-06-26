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
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
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

/* A layer descriptor that needs no on-disk file (only a manifest record). */
static PsLayerDesc
synth_layer(uint64_t id, const char *dir)
{
	PsLayerDesc d;

	memset(&d, 0, sizeof(d));
	d.layer_id = id;
	d.kind = PS_LAYER_IMAGE;
	d.location_count = 1;
	d.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
	d.locations[0].available = true;
	snprintf(d.locations[0].uri, sizeof(d.locations[0].uri), "%s/l%llu",
			 dir, (unsigned long long) id);
	return d;
}

/*
 * Torn-tail poisoning: force a manifest append to fail mid-write (RLIMIT_FSIZE),
 * then verify every later append is refused (so nothing lands after the torn
 * tail) and that a restart's replay truncates the torn record, keeping the
 * pre-failure layer.
 */
static void
test_torn_tail_poison(void)
{
	char		dir[] = "/tmp/pspoisontestXXXXXX";
	char		mpath[4096];
	PsLayerDesc d1,
				d2;
	struct rlimit save,
				lim;
	struct stat st;

	if (!mkdtemp(dir) || ps_manifest_open(dir) != 0)
	{
		check(0, "poison test setup");
		return;
	}
	d1 = synth_layer(1, dir);
	d2 = synth_layer(2, dir);
	check(ps_manifest_add_layer(&d1) == 0, "add layer 1 before the torn write");

	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
	if (stat(mpath, &st) != 0)
	{
		check(0, "stat manifest");
		return;
	}

	/* cap the file just past its current size so the next record's write tears */
	signal(SIGXFSZ, SIG_IGN);	/* exceed -> EFBIG/short write, not a signal */
	getrlimit(RLIMIT_FSIZE, &save);
	lim = save;
	lim.rlim_cur = (rlim_t) (st.st_size + 8);
	setrlimit(RLIMIT_FSIZE, &lim);

	check(ps_manifest_add_layer(&d2) != 0, "torn manifest write fails");

	/*
	 * Lift the size cap *before* the next append: writes would now succeed, so
	 * the only thing that can refuse this is the poison flag.  Without poisoning
	 * this append would land a MARK_DELETE record *after* the torn tail, turning
	 * it into interior corruption that fails the replay below.
	 */
	setrlimit(RLIMIT_FSIZE, &save);
	check(ps_manifest_mark_delete(1) != 0,
		  "manifest is poisoned: later appends refused even though writes would succeed");

	ps_manifest_close();

	/* restart: replay truncates the torn tail; only the pre-failure layer remains */
	check(ps_manifest_open(dir) == 0, "reopen after poison");
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay recovers the torn tail");
	check(ps_layer_map_count(&ps_layer_map) == 1, "only the pre-failure layer survives");
	check(find_layer(1) != NULL && find_layer(2) == NULL,
		  "layer 1 durable, the torn layer-2 record is gone");
	ps_manifest_close();

	unlink(mpath);
	rmdir(dir);
}

/*
 * Manifest compaction: add/delete churn grows the append-only log past the live
 * set; ps_manifest_compact() rewrites it to one record per live layer, preserving
 * every layer's state (including the deleting flag), and a stale .tmp left by a
 * crashed compaction is never treated as authority.
 */
static void
test_manifest_compaction(void)
{
	char		dir[] = "/tmp/psmcomptestXXXXXX";
	char		mpath[4096],
				tmppath[4096];
	struct stat before,
				after;
	PsLayerDesc *l39,
			   *l40;
	int			fd;

	if (!mkdtemp(dir) || ps_manifest_open(dir) != 0)
	{
		check(0, "manifest-compaction setup");
		return;
	}
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
	snprintf(tmppath, sizeof(tmppath), "%s/layers.manifest.tmp", dir);

	/* churn: add 40 layers, delete 1..38 (mark+remove), mark 39 deleting */
	for (uint64_t i = 1; i <= 40; i++)
	{
		PsLayerDesc d = synth_layer(i, dir);

		if (ps_manifest_add_layer(&d) != 0)
		{
			check(0, "add during churn");
			return;
		}
	}
	for (uint64_t i = 1; i <= 38; i++)
		if (ps_manifest_mark_delete(i) != 0 || ps_manifest_remove_layer(i) != 0)
		{
			check(0, "delete during churn");
			return;
		}
	check(ps_manifest_mark_delete(39) == 0, "mark layer 39 deleting");
	check(ps_layer_map_count(&ps_layer_map) == 2, "2 layers live after churn (39,40)");
	check(ps_manifest_should_compact(),
		  "log is due for compaction after add/delete churn");
	if (stat(mpath, &before) != 0)
	{
		check(0, "stat before");
		return;
	}

	check(ps_manifest_compact() == 0, "compact succeeds");
	check(!ps_manifest_should_compact(), "no longer due right after compaction");
	check(stat(mpath, &after) == 0 && after.st_size < before.st_size,
		  "compacted log is smaller than the churned log");
	check(stat(tmppath, &after) != 0, "no .tmp file left behind");

	/* restart: the compacted log replays to the same live set, flags intact */
	ps_manifest_close();
	check(ps_manifest_open(dir) == 0, "reopen after compaction");
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay the compacted log");
	check(ps_layer_map_count(&ps_layer_map) == 2, "live set preserved across compaction");
	l39 = find_layer(39);
	l40 = find_layer(40);
	check(l39 != NULL && l39->deleting, "deleting flag preserved through compaction");
	check(l40 != NULL && !l40->deleting, "live layer preserved through compaction");

	/* crash-safety: a stale .tmp (crash before rename) is not authority */
	fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd >= 0)
	{
		ssize_t		w = write(fd, "garbage", 7);

		(void) w;
		close(fd);
	}
	ps_manifest_close();
	check(ps_manifest_open(dir) == 0, "reopen with a stale .tmp present");
	check(ps_manifest_replay(&ps_layer_map) == 0, "replay ignores the stale .tmp");
	check(ps_layer_map_count(&ps_layer_map) == 2, "live set intact despite stale .tmp");
	ps_manifest_close();

	unlink(mpath);
	unlink(tmppath);
	rmdir(dir);
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

	test_torn_tail_poison();
	test_manifest_compaction();

	printf("pagestore_gc_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
