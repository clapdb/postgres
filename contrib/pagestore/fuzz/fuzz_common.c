/*-------------------------------------------------------------------------
 *
 * fuzz_common.c
 *	  Copy-template-mutate-open-close driver shared by every persisted-
 *	  format fuzz target.  See fuzz_common.h and fuzz/build.sh.
 *
 * Each iteration: copy a fresh scratch instance of the posix-mvp-baseline
 * fixture store, wholesale-replace one file (selected by target name, or by
 * the first input byte in "all" mode) with the fuzz input, ps_core_open()
 * it, do a small bounded set of reads/inspections if it opened, ps_core_
 * close() it, then remove the scratch copy.  Product code
 * (pagestore_core.c and friends) is never modified by this harness.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "fuzz_common.h"

#ifndef PS_FUZZ_FIXTURE_TGZ
#error "PS_FUZZ_FIXTURE_TGZ must be defined at compile time (see fuzz/build.sh)"
#endif

/*
 * Every persisted-format family in fixtures/posix-mvp-baseline/format.json
 * that lives directly under the store directory, mapped to a representative
 * file.  Sibling files of the same family/magic (e.g. seg_00000000 ..
 * seg_00000004, layer_0_...0a/0b) are covered by seeding the same target's
 * corpus with each of them -- the reader code path is identical, only the
 * bytes differ.  "control" (the pg_control admission-fence mirror) is
 * PGDATA-side state written by backend_localsvc.c, not a file ps_core_open()
 * reads from the store directory, so it is out of scope for this layer.
 */
const PsFuzzTarget ps_fuzz_targets[] = {
	{"manifest", "layers.manifest"},
	{"forkmeta", "forkmeta"},
	{"forkmeta_snapshot_manifest", "forkmeta_snapshots/forkmeta_manifest_v1"},
	{"forkmeta_snapshot_checkpoint", "forkmeta_snapshots/forkmeta_checkpoint_v1_00000000000000000001"},
	{"forkmeta_snapshot_tail", "forkmeta_snapshots/forkmeta_tail_v1_00000000000000000001"},
	{"image_layer", "layer_0_000000000000000a"},
	{"page_frontier", "page-prune.frontiers"},
	{"page_segment", "seg_00000004"},
	{"retention_meta", "retention.meta"},
	{"retention_state", "retention.state"},
	{"store_config", ".pagestore-nshards"},
	{"timelines", "timelines"},
	{"wal_log", "wal_1"},
	{"wal_store_identity", "wal_segments_0/wal_store_identity_v1"},
	{"wal_segment", "wal_segments_0/walv1_1_00000000000000000000"},
	{"walidx_frontier", "walidx-prune.frontiers"},
	{"walidx_log_epoch", "walidx_0_0_e00000000000000000001"},
	{"walidx_log_legacy", "walidx_1_0"},
	{"walidx_watermark", "walidx_0_0_e00000000000000000001.size"},
	{"walidx_snapshot_manifest", "walidx_snapshots_0/walidx_manifest_v1"},
	{"walidx_snapshot_shard", "walidx_snapshots_0/walidxg1_00000000000000000001_000"},
};
const int	ps_fuzz_target_count =
	(int) (sizeof(ps_fuzz_targets) / sizeof(ps_fuzz_targets[0]));

static char template_dir[PATH_MAX];
static int	template_ready = 0;
static const char *scratch_root;

/* nftw() callbacks take no user-data pointer, so the destination of the
 * current copy/remove lives in these statics.  Every ps_fuzz_run_one() call
 * is a single mkdtemp -> nftw copy -> mutate -> open/close -> nftw remove
 * sequence with no concurrency within a process, matching how libFuzzer (one
 * iteration at a time per worker) and the standalone replay driver both
 * call this. */
static const char *copy_dest_root;
static const char *copy_src_root;
static size_t copy_src_root_len;

static int
copy_entry(const char *fpath, const struct stat *sb, int typeflag,
		   struct FTW *ftwbuf)
{
	const char *rel = fpath + copy_src_root_len;
	char		dest[PATH_MAX];

	(void) sb;
	(void) ftwbuf;
	while (*rel == '/')
		rel++;
	if (*rel == '\0')
		return 0;				/* the root itself: dest already exists */
	if (snprintf(dest, sizeof(dest), "%s/%s", copy_dest_root, rel) >=
		(int) sizeof(dest))
		return 0;

	if (typeflag == FTW_D || typeflag == FTW_DP)
		(void) mkdir(dest, 0700);
	else if (typeflag == FTW_F)
	{
		int			in = open(fpath, O_RDONLY);
		int			out;

		if (in < 0)
			return 0;
		out = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
		if (out >= 0)
		{
			char		buf[65536];
			ssize_t		n;

			while ((n = read(in, buf, sizeof(buf))) > 0)
			{
				ssize_t		off = 0;

				while (off < n)
				{
					ssize_t		w = write(out, buf + off, (size_t) (n - off));

					if (w <= 0)
						break;
					off += w;
				}
			}
			close(out);
		}
		close(in);
	}
	return 0;
}

static int
remove_entry(const char *fpath, const struct stat *sb, int typeflag,
			 struct FTW *ftwbuf)
{
	(void) sb;
	(void) ftwbuf;
	if (typeflag == FTW_DP)
		rmdir(fpath);
	else
		unlink(fpath);
	return 0;
}

static void
recursive_copy(const char *src, const char *dest)
{
	copy_dest_root = dest;
	copy_src_root = src;
	copy_src_root_len = strlen(src);
	nftw(src, copy_entry, 32, FTW_PHYS);
}

static void
recursive_remove(const char *path)
{
	nftw(path, remove_entry, 32, FTW_DEPTH | FTW_PHYS);
}

/* Extract the packaged posix-mvp-baseline fixture (PS_FUZZ_FIXTURE_TGZ) into
 * a durable template directory once.  Shelling out to tar here -- once per
 * process, not per iteration -- is simpler and less fuzz-harness-bug-prone
 * than a bundled gzip/tar reader. */
void
ps_fuzz_global_init(void)
{
	char		cmd[PATH_MAX * 2];
	int			rc;

	if (template_ready)
		return;

	scratch_root = getenv("TMPDIR");
	if (scratch_root == NULL || scratch_root[0] == '\0')
		scratch_root = "/tmp";

	if (snprintf(template_dir, sizeof(template_dir),
				 "%s/psfuzz-template-XXXXXX", scratch_root) >=
		(int) sizeof(template_dir))
	{
		fprintf(stderr, "ps_fuzz_global_init: TMPDIR path too long\n");
		abort();
	}
	if (mkdtemp(template_dir) == NULL)
	{
		perror("ps_fuzz_global_init: mkdtemp");
		abort();
	}
	if (snprintf(cmd, sizeof(cmd), "tar xzf '%s' -C '%s'",
				 PS_FUZZ_FIXTURE_TGZ, template_dir) >= (int) sizeof(cmd))
	{
		fprintf(stderr, "ps_fuzz_global_init: command too long\n");
		abort();
	}
	rc = system(cmd);
	if (rc != 0)
	{
		fprintf(stderr,
				"ps_fuzz_global_init: failed to extract fixture "
				"(rc=%d): %s\n", rc, cmd);
		abort();
	}

	/* Matches fixtures/posix-mvp-baseline/fixture.json's daemon_env. */
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1);

	template_ready = 1;
}

/* A small, bounded set of read-side calls exercised after a successful
 * open, mirroring "read a few pages, NBLOCKS, timeline info" from the task:
 * layer count, each defined timeline's state/liveness, and a handful of
 * page reads per timeline.  Every loop bound is a compile-time constant, so
 * this cannot itself hang or allocate unboundedly. */
static void
bounded_reads(void)
{
	static unsigned char buf[8192];
	uint32_t	tl;

	(void) ps_core_layer_count();

	for (tl = 0; tl < 2; tl++)
	{
		PsTimelineState state;
		uint64_t	incarnation;
		int			defined;
		uint32_t	rel;

		ps_lock_map_rd();
		defined = ps_timeline_defined(tl);
		if (defined)
			(void) ps_timeline_state(tl, &state, &incarnation);
		ps_unlock_map();
		if (!defined)
			continue;
		(void) ps_timeline_live(tl);

		for (rel = 1; rel <= 6; rel++)
		{
			PsKey		key = {1, 1, rel, 0, PS_KLASS_RELATION};
			uint32_t	shard = ps_shard_of(&key);
			uint32_t	blk;

			ps_lock_shard_rd(shard);
			for (blk = 0; blk < 4; blk++)
			{
				uint64_t	ver = 0;

				(void) read_resolve(tl, &key, blk, UINT64_MAX, UINT64_MAX,
									 buf, &ver);
			}
			ps_unlock_shard(shard);
		}
	}
}

/*
 * pagestore_core.c logs routine recovery/maintenance progress to stderr with
 * plain fprintf() (e.g. "recovered shard N through segment M", "K image
 * layer(s) in map after manifest replay").  That is by design for the
 * daemon, but at fuzzing rates it is most of each iteration's wall time
 * (unbuffered small writes) and clutters the log without adding signal.
 * Muting stderr/stdout for the open/read/maintenance/close window -- never
 * touching the product code that writes to them -- is what makes this run
 * fast enough to fuzz with; libFuzzer's own stats/crash output goes through
 * its own fd captured before the first mute and is unaffected.
 */
static int	saved_stdout = -1;
static int	saved_stderr = -1;

/*
 * Muting works by dup2()'ing fd 1/2 to /dev/null, which -- for the brief
 * window where a crash can happen -- would also swallow ASan/UBSan/
 * libFuzzer's own crash report if it fires mid-iteration.  Two ways out:
 * (1) ASAN_OPTIONS=log_path=... (set by run_fuzz.sh) makes the sanitizer
 * write its report to a real file via a raw fd, independent of what fd 2
 * currently points at, so exploration runs never lose a report even though
 * they mute; (2) PS_FUZZ_NO_MUTE=1 disables muting outright, which is what
 * a one-shot crash-triage replay should set to see everything live.
 */
static int
muting_disabled(void)
{
	static int	checked = 0;
	static int	disabled = 0;

	if (!checked)
	{
		const char *v = getenv("PS_FUZZ_NO_MUTE");

		disabled = (v != NULL && v[0] != '\0' && strcmp(v, "0") != 0);
		checked = 1;
	}
	return disabled;
}

static void
mute_output(void)
{
	int			devnull;

	if (muting_disabled())
		return;
	devnull = open("/dev/null", O_WRONLY);
	if (devnull < 0)
		return;
	fflush(stdout);
	fflush(stderr);
	if (saved_stdout < 0)
		saved_stdout = dup(1);
	if (saved_stderr < 0)
		saved_stderr = dup(2);
	dup2(devnull, 1);
	dup2(devnull, 2);
	close(devnull);
}

static void
unmute_output(void)
{
	if (muting_disabled())
		return;
	fflush(stdout);
	fflush(stderr);
	if (saved_stdout >= 0)
		dup2(saved_stdout, 1);
	if (saved_stderr >= 0)
		dup2(saved_stderr, 2);
}

void
ps_fuzz_run_one(const char *target_name, const uint8_t *data, size_t size)
{
	char		store_dir[PATH_MAX];
	char		target_path[PATH_MAX];
	const char *relpath = NULL;
	const uint8_t *content = data;
	size_t		content_len = size;
	int			fd;

	if (!template_ready)
		return;

	if (target_name != NULL && target_name[0] != '\0' &&
		strcmp(target_name, "all") != 0)
	{
		int			i;

		for (i = 0; i < ps_fuzz_target_count; i++)
		{
			if (strcmp(ps_fuzz_targets[i].name, target_name) == 0)
			{
				relpath = ps_fuzz_targets[i].relpath;
				break;
			}
		}
		if (relpath == NULL)
			return;				/* unknown PS_FUZZ_TARGET: nothing to do */
	}
	else
	{
		/* "all" mode: the first input byte picks the file kind. */
		if (size == 0)
			return;
		relpath = ps_fuzz_targets[data[0] % ps_fuzz_target_count].relpath;
		content = data + 1;
		content_len = size - 1;
	}

	if (snprintf(store_dir, sizeof(store_dir), "%s/psfuzz-run-XXXXXX",
				 scratch_root) >= (int) sizeof(store_dir))
		return;
	if (mkdtemp(store_dir) == NULL)
		return;

	recursive_copy(template_dir, store_dir);

	if (snprintf(target_path, sizeof(target_path), "%s/%s", store_dir,
				 relpath) >= (int) sizeof(target_path))
		goto cleanup;

	fd = open(target_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd >= 0)
	{
		size_t		off = 0;

		while (off < content_len)
		{
			ssize_t		w = write(fd, content + off, content_len - off);

			if (w <= 0)
				break;
			off += (size_t) w;
		}
		close(fd);
	}

	/* Matches fixtures/posix-mvp-baseline/fixture.json's daemon_args. */
	page_size = 8192;
	segment_size = 65536;
	flush_pages = 8;
	segment_gc_enabled = 0;
	cache_pages = 0;
	use_layers = 1;
	ps_nshards = 1;
	wal_reclaim_high_water_bytes = 8388608;
	wal_reclaim_catchup_bytes = 1;
	walidx_snapshot_trigger_option_bytes = 1048576;
	page_reclaim_high_water_bytes = 0;
	page_reclaim_catchup_bytes = 0;
	walidx_reclaim_high_water_bytes = 0;
	walidx_reclaim_catchup_bytes = 0;
	forkmeta_reclaim_high_water_bytes = 0;
	forkmeta_reclaim_catchup_bytes = 0;
	ps_storage = &PsStoragePosix;

	mute_output();
	if (ps_core_open(store_dir) == 0)
	{
		bounded_reads();
		(void) ps_core_maintenance();
		ps_core_close();
	}
	unmute_output();

cleanup:
	recursive_remove(store_dir);
}
