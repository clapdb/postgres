/*-------------------------------------------------------------------------
 *
 * pagestore_recover_test.c
 *	  In-process unit tests for segment recovery (no IPC daemon).
 *
 * Links the core + POSIX storage directly.  Recovery must run with the same fresh
 * process state a real restart has (a zeroed in-memory index and segment fd cache),
 * so each phase runs in a forked child of a parent that never opens the store:
 * a "setup" child writes/syncs/optionally crashes, the parent injects on-media
 * corruption between phases, and a "recover" child re-opens and checks.  The child
 * returns 0 if its expectations held; the parent counts one check per phase.
 *
 * Built segment-mode (use_layers == 0): recovery rebuilds purely from the segment
 * log -- the path the CRC + sync-watermark changes touch.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"

static int	checks = 0;
static int	fails = 0;

#define check(cond, msg) \
	do { \
		checks++; \
		if (!(cond)) { fails++; printf("FAIL: %s\n", msg); } \
	} while (0)

static const PsKey key = {1, 2, 3, 0};

static void
fill_page(unsigned char *buf, uint32_t ps, uint64_t lsn, unsigned char tag)
{
	uint32_t	xlogid = (uint32_t) (lsn >> 32);
	uint32_t	xrecoff = (uint32_t) (lsn & 0xFFFFFFFF);

	memcpy(buf, &xlogid, 4);
	memcpy(buf + 4, &xrecoff, 4);
	for (uint32_t i = 8; i < ps; i++)
		buf[i] = (unsigned char) (tag ^ (i & 0xFF));
}

static int
page_has_tag(const unsigned char *buf, uint32_t ps, unsigned char tag)
{
	for (uint32_t i = 8; i < ps; i++)
		if (buf[i] != (unsigned char) (tag ^ (i & 0xFF)))
			return 0;
	return 1;
}

static void
corrupt_byte(const char *store, int seg, uint64_t off)
{
	char		path[4200];
	int			fd;
	unsigned char b = 0;

	snprintf(path, sizeof(path), "%s/seg_%08d", store, seg);
	fd = open(path, O_RDWR);
	if (fd < 0 || pread(fd, &b, 1, (off_t) off) != 1)
	{
		printf("FAIL: corrupt_byte open/read %s\n", path);
		if (fd >= 0)
			close(fd);
		return;
	}
	b ^= 0xFF;
	if (pwrite(fd, &b, 1, (off_t) off) != 1)
		printf("FAIL: corrupt_byte write\n");
	fsync(fd);
	close(fd);
}

/* run fn in a fresh child (clean index + fd cache, like a real restart) */
static int
phase(int (*fn)(const char *store), const char *store)
{
	pid_t		pid = fork();
	int			st;

	if (pid == 0)
	{
		fflush(stdout);
		_exit(fn(store));		/* 0 == expectations held */
	}
	if (pid < 0 || waitpid(pid, &st, 0) != pid)
		return 99;
	return WIFEXITED(st) ? WEXITSTATUS(st) : 98;
}

static void
append_blocks(const char *store, int from, int to, unsigned char tagbase)
{
	unsigned char pg[8192];

	(void) store;
	for (int b = from; b < to; b++)
	{
		fill_page(pg, page_size, 1000 + b, (unsigned char) (tagbase + b));
		if (append_page(0, &key, (uint32_t) b, pg) != 0)
		{
			printf("FAIL: append block %d\n", b);
			_exit(1);
		}
		fork_grow(0, &key, (uint32_t) b + 1);
	}
}

/* setup children */
static int
setup_synced(const char *store)		/* append 5, watermark all, clean close */
{
	if (ps_core_open(store) != 0)
		return 1;
	append_blocks(store, 0, 5, 20);
	ps_core_wm_capture(0);
	if (ps_core_write_sync_watermark() != 0)
		return 1;
	ps_core_close();
	return 0;
}

static int
setup_crash_tail(const char *store)	/* watermark first 3, append 2 more, no close */
{
	if (ps_core_open(store) != 0)
		return 1;
	append_blocks(store, 0, 3, 30);
	ps_core_wm_capture(0);
	if (ps_core_write_sync_watermark() != 0)
		return 1;
	append_blocks(store, 3, 5, 30);
	fflush(stdout);
	_exit(0);					/* simulate crash: durable data, no clean close */
}

/* recover children */
static int
recover_expect_fail(const char *store)	/* corrupt synced data -> must refuse */
{
	return (ps_core_open(store) == -1) ? 0 : 1;
}

static int
recover_read_all5_synced(const char *store)	/* all 5 present (tag base 20) */
{
	unsigned char rb[8192];

	if (ps_core_open(store) != 0)
		return 1;
	for (int b = 0; b < 5; b++)
		if (read_resolve(0, &key, (uint32_t) b, ~0ull, rb) != 1 ||
			!page_has_tag(rb, page_size, (unsigned char) (20 + b)))
			return 1;
	return 0;
}

static int
recover_read_all5(const char *store)	/* all 5 present after recover */
{
	unsigned char rb[8192];

	if (ps_core_open(store) != 0)
		return 1;
	for (int b = 0; b < 5; b++)
		if (read_resolve(0, &key, (uint32_t) b, ~0ull, rb) != 1 ||
			!page_has_tag(rb, page_size, (unsigned char) (30 + b)))
			return 1;
	return 0;
}

static int
recover_truncated_tail(const char *store)	/* 0..3 survive, 4 dropped */
{
	unsigned char rb[8192];

	if (ps_core_open(store) != 0)
		return 1;				/* must NOT brick on a torn unsynced tail */
	for (int b = 0; b < 4; b++)
		if (read_resolve(0, &key, (uint32_t) b, ~0ull, rb) != 1 ||
			!page_has_tag(rb, page_size, (unsigned char) (40 + b)))
			return 1;
	if (read_resolve(0, &key, 4, ~0ull, rb) != 0)
		return 1;				/* torn block must be dropped, not served */
	return 0;
}

static int
setup_crash_tail40(const char *store)	/* like setup_crash_tail but tag base 40 */
{
	if (ps_core_open(store) != 0)
		return 1;
	append_blocks(store, 0, 3, 40);
	ps_core_wm_capture(0);
	if (ps_core_write_sync_watermark() != 0)
		return 1;
	append_blocks(store, 3, 5, 40);
	fflush(stdout);
	_exit(0);
}

static int
setup_resurrect(const char *store)	/* 0-2 synced, 3-5 unsynced, crash */
{
	if (ps_core_open(store) != 0)
		return 1;
	append_blocks(store, 0, 3, 50);
	ps_core_wm_capture(0);
	if (ps_core_write_sync_watermark() != 0)
		return 1;
	append_blocks(store, 3, 6, 50);		/* blocks 3,4,5 in segment 1, unsynced */
	fflush(stdout);
	_exit(0);
}

static int
recover_overwrite(const char *store)	/* recover (zeroes torn tail), reappend, crash */
{
	unsigned char pg[8192];

	if (ps_core_open(store) != 0)
		return 1;
	fill_page(pg, page_size, 9000, 99);	/* a fresh block 3 over the zeroed tail */
	if (append_page(0, &key, 3, pg) != 0)
		return 1;
	fork_grow(0, &key, 4);
	fflush(stdout);
	_exit(0);					/* crash before syncing: blocks 4,5 must stay gone */
}

static int
recover_no_resurrect(const char *store)	/* new block 3 only; old 4,5 not resurrected */
{
	unsigned char rb[8192];

	if (ps_core_open(store) != 0)
		return 1;
	if (read_resolve(0, &key, 3, ~0ull, rb) != 1 || !page_has_tag(rb, page_size, 99))
		return 1;
	if (read_resolve(0, &key, 4, ~0ull, rb) != 0)
		return 1;				/* old block 4 must NOT come back */
	if (read_resolve(0, &key, 5, ~0ull, rb) != 0)
		return 1;
	return 0;
}

/* per-run scenario directory under the test's private base, so concurrent test
 * runs (separate build dirs / overlapping CI jobs) never share a store path */
static const char *g_base;

static const char *
sdir(const char *name)
{
	static char buf[4096];

	snprintf(buf, sizeof(buf), "%s/%s", g_base, name);
	if (mkdir(buf, 0700) != 0)
	{
		printf("FAIL: mkdir %s\n", buf);
		exit(2);
	}
	return buf;
}

int
main(void)
{
	char		base[] = "/tmp/ps_recover.XXXXXX";
	const char *dir;

	if (!(g_base = mkdtemp(base)))
		return 2;

	page_size = 8192;
	segment_size = 32768;		/* ~3 records/segment -> multi-segment shards */
	ps_nshards = 1;
	use_layers = 0;
	cache_pages = 0;
	flush_pages = 1 << 20;
	compact_layers = 1 << 20;

	/* 1: clean write -> close -> recover -> read */
	dir = sdir("A");
	check(phase(setup_synced, dir) == 0, "clean: setup (write + watermark)");
	check(phase(recover_read_all5_synced, dir) == 0,
		  "clean: recover reads all records");

	/* 2: corruption of SYNCED data (below watermark) refuses to open */
	dir = sdir("B");
	check(phase(setup_synced, dir) == 0, "synced-corrupt: setup");
	corrupt_byte(dir, 0, 9000);	/* inside block 1, segment 0 */
	check(phase(recover_expect_fail, dir) == 0,
		  "synced-corrupt: recovery refuses (acknowledged data corrupt)");

	/* 3: a valid UNSYNCED tail past the watermark is still replayed */
	dir = sdir("C");
	check(phase(setup_crash_tail, dir) == 0, "unsynced-tail: setup (crash)");
	check(phase(recover_read_all5, dir) == 0,
		  "unsynced-tail: valid tail past watermark recovered");

	/* 4: a corrupt UNSYNCED tail truncates, does not brick */
	dir = sdir("D");
	check(phase(setup_crash_tail40, dir) == 0, "torn-tail: setup (crash)");
	corrupt_byte(dir, 1, 9000);	/* block 4 (segment 1), above watermark */
	check(phase(recover_truncated_tail, dir) == 0,
		  "torn-tail: truncates (not brick); torn block dropped");

	/* 5: a truncated tail is physically discarded, not resurrected by a partial
	 * overwrite + second crash */
	dir = sdir("E");
	check(phase(setup_resurrect, dir) == 0, "resurrect: setup (crash)");
	corrupt_byte(dir, 1, 100);	/* corrupt block 3 (first unsynced record) */
	check(phase(recover_overwrite, dir) == 0,
		  "resurrect: recover zeroes tail, reappends block 3, crashes");
	check(phase(recover_no_resurrect, dir) == 0,
		  "resurrect: old blocks 4,5 stay gone (no resurrection)");

	/* 6: the watermark says this shard had synced data, but the segment is gone --
	 * recovery must refuse, not silently open an empty store */
	dir = sdir("F");
	check(phase(setup_synced, dir) == 0, "wm-vs-empty: setup");
	{
		char		seg[4200];
		int			fd;

		snprintf(seg, sizeof(seg), "%s/seg_00000000", dir);
		fd = open(seg, O_RDWR);
		if (fd < 0 || ftruncate(fd, 0) != 0)	/* segment lost after the watermark */
			check(0, "wm-vs-empty: truncate segment");
		if (fd >= 0)
			close(fd);
	}
	check(phase(recover_expect_fail, dir) == 0,
		  "wm-vs-empty: refuse to open when committed data is gone");

	{
		char		cmd[4200];

		snprintf(cmd, sizeof(cmd), "rm -rf %s", g_base);
		if (system(cmd) != 0)
			printf("warning: could not clean up %s\n", g_base);
	}
	printf("%d checks, %d failed\n", checks, fails);
	return fails ? 1 : 0;
}
