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

/* Flip every bit of one byte at 'off' in a file. */
static void
corrupt_byte(const char *path, off_t off)
{
	int			fd = open(path, O_RDWR);
	unsigned char b;

	if (fd < 0)
		return;
	if (pread(fd, &b, 1, off) == 1)
	{
		b ^= 0xFF;
		if (pwrite(fd, &b, 1, off) != 1)
			perror("pwrite");
	}
	close(fd);
}

/* Set one byte at 'off' to an exact value (to corrupt a specific header field). */
static void
poke_byte(const char *path, off_t off, unsigned char val)
{
	int			fd = open(path, O_RDWR);

	if (fd < 0)
		return;
	if (pwrite(fd, &val, 1, off) != 1)
		perror("pwrite");
	close(fd);
}

/* manifest record header field offsets (magic, version, type, len, crc) */
#define MR_MAGIC_OFF	0
#define MR_VERSION_OFF	4
#define MR_TYPE_OFF		8
#define MT_ADD_LAYER	1		/* PsManifestEventType values (private to manifest.c) */
#define MT_MARK_DELETE	4

/* Build a fresh manifest with ADD(1) then ADD(2); return the first ADD's on-disk
 * size (so a caller can locate the second record), or -1 on failure. */
static off_t
build_two_adds(char *dir, char *mpath, size_t mpath_cap)
{
	PsLayerDesc d1,
				d2;
	struct stat st;
	off_t		add1;

	if (!mkdtemp(dir) || ps_manifest_open(dir) != 0)
		return -1;
	snprintf(mpath, mpath_cap, "%s/layers.manifest", dir);
	d1 = synth_layer(1, dir);
	if (ps_manifest_add_layer(&d1) != 0 || stat(mpath, &st) != 0)
	{
		ps_manifest_close();
		return -1;
	}
	add1 = st.st_size;
	d2 = synth_layer(2, dir);
	if (ps_manifest_add_layer(&d2) != 0)
	{
		ps_manifest_close();
		return -1;
	}
	ps_manifest_close();
	return add1;
}

/*
 * Replay must not trust a corrupted header word (version/type/magic) to size a
 * record: corruption confined to the physically last record is a recoverable torn
 * tail, but corruption that leaves a valid record after it is interior corruption
 * and must fail -- never silently truncating later records or bypassing the CRC.
 */
static void
test_manifest_replay_robustness(void)
{
	char		dir[] = "/tmp/psrobXXXXXX";
	char		mpath[4096];
	off_t		add1;

	/* (a) last record's TYPE corrupted -> torn tail, the earlier ADD survives */
	add1 = build_two_adds(dir, mpath, sizeof(mpath));
	check(add1 > 0, "robust: build (last-type)");
	poke_byte(mpath, add1 + MR_TYPE_OFF, MT_MARK_DELETE);	/* ADD2.type -> a smaller type */
	check(ps_manifest_open(dir) == 0 &&
		  ps_manifest_replay(&ps_layer_map) == 0,
		  "last-record type corruption recovers as a torn tail");
	check(ps_layer_map_count(&ps_layer_map) == 1 && find_layer(1) && !find_layer(2),
		  "  ... the corrupt last record is dropped, the earlier ADD survives");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir);

	/* (b) last record's VERSION flipped 3->2 must NOT bypass the CRC */
	memcpy(dir, "/tmp/psrobXXXXXX", sizeof(dir));
	add1 = build_two_adds(dir, mpath, sizeof(mpath));
	check(add1 > 0, "robust: build (version-downgrade)");
	poke_byte(mpath, add1 + MR_VERSION_OFF, 2);	/* ADD2.version 3 -> 2 */
	check(ps_manifest_open(dir) == 0 &&
		  ps_manifest_replay(&ps_layer_map) == 0,
		  "version-downgraded last record recovers as a torn tail");
	check(ps_layer_map_count(&ps_layer_map) == 1 && !find_layer(2),
		  "  ... the downgraded record is not installed (CRC not bypassed)");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir);

	/* (c) last record's MAGIC corrupted -> torn tail, earlier ADD survives */
	memcpy(dir, "/tmp/psrobXXXXXX", sizeof(dir));
	add1 = build_two_adds(dir, mpath, sizeof(mpath));
	check(add1 > 0, "robust: build (magic)");
	poke_byte(mpath, add1 + MR_MAGIC_OFF, 0x00);	/* ADD2.magic low byte */
	check(ps_manifest_open(dir) == 0 &&
		  ps_manifest_replay(&ps_layer_map) == 0 &&
		  ps_layer_map_count(&ps_layer_map) == 1,
		  "last-record magic corruption recovers as a torn tail");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir);

	/* (d) interior record's TYPE corrupted to ADD must FAIL, not truncate the
	 * following REMOVE (which would resurrect the removed layer) */
	memcpy(dir, "/tmp/psrobXXXXXX", sizeof(dir));
	if (mkdtemp(dir) && ps_manifest_open(dir) == 0)
	{
		PsLayerDesc d1 = synth_layer(1, dir);
		struct stat st;

		snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
		check(ps_manifest_add_layer(&d1) == 0 && stat(mpath, &st) == 0,
			  "robust: build (interior ADD)");
		add1 = st.st_size;
		check(ps_manifest_mark_delete(1) == 0 && ps_manifest_remove_layer(1) == 0,
			  "robust: MARK_DELETE + REMOVE");
		ps_manifest_close();
		poke_byte(mpath, add1 + MR_TYPE_OFF, MT_ADD_LAYER);	/* MARK_DELETE.type -> ADD */
		check(ps_manifest_open(dir) == 0 &&
			  ps_manifest_replay(&ps_layer_map) != 0,
			  "interior type corruption fails replay (REMOVE not truncated away)");
		ps_manifest_close();
		unlink(mpath);
		rmdir(dir);
	}
	else
		check(0, "robust: build (interior ADD)");
}

/*
 * Per-record CRC: a bit-flip inside a record (not the tail) is detected and fails
 * replay rather than loading a wrong layer; the same flip in the last record is a
 * torn tail and is recovered.
 */
static void
test_manifest_crc(void)
{
	char		dir[] = "/tmp/pscrcintXXXXXX";
	char		dir2[] = "/tmp/pscrctailXXXXXX";
	char		mpath[4096];
	PsLayerDesc d1,
				d2;
	struct stat st;

	/* --- interior corruption must fail replay --- */
	if (!mkdtemp(dir) || ps_manifest_open(dir) != 0)
	{
		check(0, "crc test setup");
		return;
	}
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
	d1 = synth_layer(1, dir);
	d2 = synth_layer(2, dir);
	check(ps_manifest_add_layer(&d1) == 0 && ps_manifest_add_layer(&d2) == 0,
		  "two records written");
	ps_manifest_close();
	corrupt_byte(mpath, 24);		/* inside record 1's payload (header is 20 bytes) */
	check(ps_manifest_open(dir) == 0, "reopen (interior corruption)");
	check(ps_manifest_replay(&ps_layer_map) != 0,
		  "interior CRC corruption fails replay (not loaded as a wrong layer)");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir);

	/* --- corruption of the last record is a recoverable torn tail --- */
	if (!mkdtemp(dir2) || ps_manifest_open(dir2) != 0)
	{
		check(0, "crc tail setup");
		return;
	}
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir2);
	d1 = synth_layer(1, dir);
	d2 = synth_layer(2, dir);
	check(ps_manifest_add_layer(&d1) == 0 && ps_manifest_add_layer(&d2) == 0,
		  "two records written (tail case)");
	ps_manifest_close();
	if (stat(mpath, &st) == 0)
		corrupt_byte(mpath, st.st_size - 1);	/* last byte = record 2's payload */
	check(ps_manifest_open(dir2) == 0, "reopen (tail corruption)");
	check(ps_manifest_replay(&ps_layer_map) == 0,
		  "tail CRC corruption is recovered (torn-tail truncate)");
	check(ps_layer_map_count(&ps_layer_map) == 1,
		  "record 1 survives, the corrupt last record is dropped");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir2);
}

/*
 * The len field is not trusted: a corrupted length on an interior record is
 * detected as corruption (it is not used to size the read, so it cannot consume
 * the following record), while a corrupted length on the physically last record
 * is a recoverable torn tail.
 */
static void
test_manifest_len_corruption(void)
{
	char		dir[] = "/tmp/pslenintXXXXXX";
	char		dir2[] = "/tmp/pslentailXXXXXX";
	char		mpath[4096];
	PsLayerDesc d1;
	struct stat st;
	off_t		add_size;
	PsLayerDesc *l1;

	/*
	 * Interior small record whose len is flipped upward: a length-trusting reader
	 * would consume the following REMOVE record and hit EOF, mis-recovering the
	 * MARK_DELETE as a torn tail and resurrecting the removed layer.  Sizing from
	 * the type instead, this is detected as interior corruption and fails replay.
	 * Build [ADD, MARK_DELETE, REMOVE]; measure the ADD's size to locate the
	 * MARK_DELETE header that follows it.
	 */
	if (!mkdtemp(dir) || ps_manifest_open(dir) != 0)
	{
		check(0, "len test setup");
		return;
	}
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir);
	d1 = synth_layer(1, dir);
	check(ps_manifest_add_layer(&d1) == 0, "ADD written");
	check(stat(mpath, &st) == 0, "measure ADD size");
	add_size = st.st_size;
	check(ps_manifest_mark_delete(1) == 0 && ps_manifest_remove_layer(1) == 0,
		  "MARK_DELETE + REMOVE written");
	ps_manifest_close();
	corrupt_byte(mpath, add_size + 12); /* the MARK_DELETE header's len field */
	check(ps_manifest_open(dir) == 0, "reopen (interior len)");
	check(ps_manifest_replay(&ps_layer_map) != 0,
		  "interior len corruption fails replay (not a misread torn tail)");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir);

	/* --- last record's len corrupted -> recoverable torn tail --- */
	if (!mkdtemp(dir2) || ps_manifest_open(dir2) != 0)
	{
		check(0, "len tail setup");
		return;
	}
	snprintf(mpath, sizeof(mpath), "%s/layers.manifest", dir2);
	d1 = synth_layer(1, dir2);
	check(ps_manifest_add_layer(&d1) == 0, "ADD written");
	check(ps_manifest_mark_delete(1) == 0, "MARK_DELETE written (last record)");
	ps_manifest_close();
	/* the trailing MARK_DELETE is 20-byte header + 16-byte event = 36 bytes; its
	 * len field sits at file_size - 36 + 12 = file_size - 24 */
	if (stat(mpath, &st) == 0)
		corrupt_byte(mpath, st.st_size - 24);
	check(ps_manifest_open(dir2) == 0, "reopen (last-record len)");
	check(ps_manifest_replay(&ps_layer_map) == 0,
		  "last-record len corruption is a recoverable torn tail");
	l1 = find_layer(1);
	check(ps_layer_map_count(&ps_layer_map) == 1 && l1 != NULL && !l1->deleting,
		  "the ADD survives and the truncated MARK_DELETE did not apply");
	ps_manifest_close();
	unlink(mpath);
	rmdir(dir2);
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
	test_manifest_crc();
	test_manifest_len_corruption();
	test_manifest_replay_robustness();

	printf("pagestore_gc_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
