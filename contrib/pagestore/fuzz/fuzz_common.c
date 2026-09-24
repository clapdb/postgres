/*-------------------------------------------------------------------------
 *
 * fuzz_common.c
 *	  Persistent-work-directory driver shared by every persisted-format
 *	  fuzz target.  See fuzz_common.h and fuzz/build.sh.
 *
 * Round 1 mkdtemp'd and fully recursive-copied a fresh store directory
 * every iteration; that plus real fsync() calls in the product code held
 * throughput to 1-2 exec/s.  Round 2 (see the coordinator's review):
 *   - The whole template fixture store is read into memory once
 *     (template_files[]) and written out to one persistent work_dir once,
 *     at ps_fuzz_global_init() time.
 *   - Each iteration only: (1) write the mutated bytes to the target file,
 *     (2) ps_core_open()/bounded reads/ps_core_maintenance()/
 *     ps_core_close(), (3) reset_work_dir() -- rewrite every cached
 *     file's original bytes back (cheap in-place O_TRUNC writes, no new
 *     inodes) and delete/recreate anything open()/maintenance() added or
 *     removed, so the *next* iteration starts from the same pristine state
 *     without a fresh mkdtemp/mkdir/rmdir per iteration.
 *   - fsync()/fdatasync()/sync_file_range() are neutralized for the
 *     instrumented binary only, via linker --wrap (fuzz_nosync.c); this
 *     file never calls or overrides them itself.
 *   - stdout/stderr are muted for the open/close window using fds opened
 *     once at init, not per iteration (see mute_output()).
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
#include <unistd.h>

#include "pagestore_core.h"
#include "fuzz_common.h"
#include "fuzz_crc_fixup.h"

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

/* ---- in-memory template cache + persistent work directory -------------- */

typedef struct TemplateEntry
{
	char	   *relpath;		/* "" for the store root itself */
	uint8_t    *data;			/* NULL for a directory */
	size_t		len;
	int			is_dir;
} TemplateEntry;

static TemplateEntry *template_entries;
static int	template_entry_count;
static int	template_ready = 0;
static const char *scratch_root;
static char work_dir[PATH_MAX];

static const char *
skip_root(const char *fpath, size_t root_len)
{
	const char *rel = fpath + root_len;

	while (*rel == '/')
		rel++;
	return rel;
}

/* ---- building the template cache from the extracted fixture ------------ */

static const char *g_walk_root;
static size_t g_walk_root_len;

static int
cache_entry(const char *fpath, const struct stat *sb, int typeflag,
			struct FTW *ftwbuf)
{
	const char *rel = skip_root(fpath, g_walk_root_len);
	TemplateEntry *e;

	(void) ftwbuf;
	if (typeflag != FTW_D && typeflag != FTW_F)
		return 0;

	template_entries = realloc(template_entries,
		(size_t) (template_entry_count + 1) * sizeof(TemplateEntry));
	if (template_entries == NULL)
	{
		fprintf(stderr, "ps_fuzz_global_init: out of memory caching template\n");
		abort();
	}
	e = &template_entries[template_entry_count++];
	e->relpath = strdup(rel);
	e->is_dir = (typeflag == FTW_D);
	e->data = NULL;
	e->len = 0;
	if (typeflag == FTW_F)
	{
		e->len = (size_t) sb->st_size;
		e->data = e->len > 0 ? malloc(e->len) : malloc(1);
		if (e->data == NULL)
		{
			fprintf(stderr, "ps_fuzz_global_init: out of memory reading %s\n",
					fpath);
			abort();
		}
		if (e->len > 0)
		{
			int			fd = open(fpath, O_RDONLY);
			size_t		off = 0;

			if (fd < 0)
			{
				fprintf(stderr, "ps_fuzz_global_init: open %s: %s\n", fpath,
						strerror(errno));
				abort();
			}
			while (off < e->len)
			{
				ssize_t		n = read(fd, e->data + off, e->len - off);

				if (n <= 0)
				{
					fprintf(stderr, "ps_fuzz_global_init: read %s failed\n",
							fpath);
					abort();
				}
				off += (size_t) n;
			}
			close(fd);
		}
	}
	return 0;
}

static const TemplateEntry *
template_find(const char *relpath)
{
	for (int i = 0; i < template_entry_count; i++)
		if (strcmp(template_entries[i].relpath, relpath) == 0)
			return &template_entries[i];
	return NULL;
}

const uint8_t *
ps_fuzz_template_lookup(const char *relpath, size_t *len_out)
{
	const TemplateEntry *e = template_find(relpath);

	if (e == NULL || e->is_dir)
		return NULL;
	if (len_out != NULL)
		*len_out = e->len;
	return e->data;
}

static void
write_file(const char *path, const uint8_t *data, size_t len)
{
	int			fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	size_t		off = 0;

	if (fd < 0)
	{
		fprintf(stderr, "ps_fuzz: open %s: %s\n", path, strerror(errno));
		abort();
	}
	while (off < len)
	{
		ssize_t		n = write(fd, data + off, len - off);

		if (n <= 0)
		{
			fprintf(stderr, "ps_fuzz: write %s failed\n", path);
			abort();
		}
		off += (size_t) n;
	}
	close(fd);
}

/* One-time population of the persistent work_dir from the template cache
 * (mirrors the order entries were discovered, i.e. directories before the
 * files inside them, since nftw() visits a directory before its
 * children). */
static void
populate_work_dir(void)
{
	for (int i = 0; i < template_entry_count; i++)
	{
		TemplateEntry *e = &template_entries[i];
		char		path[PATH_MAX];

		if (snprintf(path, sizeof(path), "%s/%s", work_dir, e->relpath) >=
			(int) sizeof(path))
			continue;
		if (e->is_dir)
			(void) mkdir(path, 0700);
		else
			write_file(path, e->data, e->len);
	}
}

/* ---- resetting the persistent work directory between iterations -------- */

static int
reset_entry(const char *fpath, const struct stat *sb, int typeflag,
			struct FTW *ftwbuf)
{
	const char *rel;
	const TemplateEntry *e;

	(void) sb;
	(void) ftwbuf;
	rel = skip_root(fpath, strlen(work_dir));
	if (*rel == '\0')
		return 0;				/* the work_dir root itself */
	e = template_find(rel);
	if (typeflag == FTW_F)
	{
		if (e != NULL && !e->is_dir)
			write_file(fpath, e->data, e->len);
		else
			unlink(fpath);		/* a file open()/maintenance() created */
	}
	else if (typeflag == FTW_DP)
	{
		if (e == NULL || !e->is_dir)
			rmdir(fpath);		/* a directory open()/maintenance() created */
	}
	return 0;
}

static void
reset_work_dir(void)
{
	nftw(work_dir, reset_entry, 32, FTW_DEPTH | FTW_PHYS);
	/* Recreate anything product code deleted outright (renamed away,
	 * unlinked, ...): the walk above only rewrites/removes what it found on
	 * disk, so a template entry that no longer exists needs a second pass. */
	for (int i = 0; i < template_entry_count; i++)
	{
		TemplateEntry *e = &template_entries[i];
		char		path[PATH_MAX];
		struct stat st;

		if (snprintf(path, sizeof(path), "%s/%s", work_dir, e->relpath) >=
			(int) sizeof(path))
			continue;
		if (stat(path, &st) == 0)
			continue;
		if (e->is_dir)
			(void) mkdir(path, 0700);
		else
			write_file(path, e->data, e->len);
	}
}

/* ---- one-time global setup ---------------------------------------------- */

void
ps_fuzz_global_init(void)
{
	char		cmd[PATH_MAX * 2];
	char		extract_dir[PATH_MAX];
	int			rc;

	if (template_ready)
		return;

	scratch_root = getenv("TMPDIR");
	if (scratch_root == NULL || scratch_root[0] == '\0')
		scratch_root = "/tmp";

	if (snprintf(extract_dir, sizeof(extract_dir),
				 "%s/psfuzz-extract-XXXXXX", scratch_root) >=
			(int) sizeof(extract_dir) ||
		mkdtemp(extract_dir) == NULL)
	{
		fprintf(stderr, "ps_fuzz_global_init: mkdtemp (extract) failed\n");
		abort();
	}
	if (snprintf(cmd, sizeof(cmd), "tar xzf '%s' -C '%s'",
				 PS_FUZZ_FIXTURE_TGZ, extract_dir) >= (int) sizeof(cmd))
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

	g_walk_root = extract_dir;
	g_walk_root_len = strlen(extract_dir);
	nftw(extract_dir, cache_entry, 32, FTW_PHYS);

	if (snprintf(work_dir, sizeof(work_dir), "%s/psfuzz-work-XXXXXX",
				 scratch_root) >= (int) sizeof(work_dir) ||
		mkdtemp(work_dir) == NULL)
	{
		fprintf(stderr, "ps_fuzz_global_init: mkdtemp (work_dir) failed\n");
		abort();
	}
	populate_work_dir();

	/* The extracted template tree is no longer needed once cached and
	 * copied into work_dir; free the disk space, especially with many
	 * parallel target processes sharing /tmp. */
	{
		char		rm_cmd[PATH_MAX + 16];

		if (snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf '%s'", extract_dir) <
			(int) sizeof(rm_cmd))
			(void) system(rm_cmd);
	}

	/* Matches fixtures/posix-mvp-baseline/fixture.json's daemon_env. */
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1);

	/* Matches fixtures/posix-mvp-baseline/fixture.json's daemon_args; set
	 * once here rather than every ps_fuzz_run_one() call -- these globals
	 * never change between iterations. */
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

	template_ready = 1;
}

/* ---- bounded post-open reads --------------------------------------------- */

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

/* ---- output muting (fds opened once, not per iteration) ----------------- */

static int	saved_stdout = -1;
static int	saved_stderr = -1;
static int	devnull_fd = -1;

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
	if (muting_disabled())
		return;
	if (devnull_fd < 0)
		devnull_fd = open("/dev/null", O_WRONLY);
	if (devnull_fd < 0)
		return;
	fflush(stdout);
	fflush(stderr);
	if (saved_stdout < 0)
		saved_stdout = dup(1);
	if (saved_stderr < 0)
		saved_stderr = dup(2);
	dup2(devnull_fd, 1);
	dup2(devnull_fd, 2);
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

/* ---- CRC-fixup sampling --------------------------------------------------
 * PS_FUZZ_CRC_FIXUP unset/"0": never fix up (round-1 behavior, the "cov/ft
 * without fixup" baseline).  "always"/"1": every iteration.  Anything else
 * (including the default when the variable is set but not recognized) and
 * the documented default once the feature is on at all: alternate
 * deterministically so exactly half of iterations get the fixed-up
 * checksum and half stay pure mutation -- reproducible corpus minimization
 * and a mix of "past the gate" and "at the gate" inputs in the same run. */
typedef enum FixupMode
{
	FIXUP_OFF,
	FIXUP_ALWAYS,
	FIXUP_HALF,
} FixupMode;

static FixupMode
fixup_mode(void)
{
	static int	checked = 0;
	static FixupMode mode = FIXUP_OFF;

	if (!checked)
	{
		const char *v = getenv("PS_FUZZ_CRC_FIXUP");

		if (v == NULL || v[0] == '\0' || strcmp(v, "0") == 0)
			mode = FIXUP_OFF;
		else if (strcmp(v, "always") == 0)
			mode = FIXUP_ALWAYS;
		else
			mode = FIXUP_HALF;
		checked = 1;
	}
	return mode;
}

static int
should_fixup_this_iteration(void)
{
	static uint64_t counter = 0;

	switch (fixup_mode())
	{
		case FIXUP_OFF:
			return 0;
		case FIXUP_ALWAYS:
			return 1;
		case FIXUP_HALF:
		default:
			return (counter++ & 1) == 0;
	}
}

/* ---- the per-iteration driver -------------------------------------------- */

void
ps_fuzz_run_one(const char *target_name, const uint8_t *data, size_t size)
{
	char		target_path[PATH_MAX];
	const char *relpath = NULL;
	const char *resolved_target_name = target_name;
	uint8_t    *content;
	size_t		content_len = size;

	if (!template_ready)
		return;

	if (target_name != NULL && target_name[0] != '\0' &&
		strcmp(target_name, "all") != 0)
	{
		int			i;

		relpath = NULL;
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
		int			idx;

		if (size == 0)
			return;
		idx = data[0] % ps_fuzz_target_count;
		relpath = ps_fuzz_targets[idx].relpath;
		resolved_target_name = ps_fuzz_targets[idx].name;
		data += 1;
		size -= 1;
		content_len = size;
	}

	content = content_len > 0 ? malloc(content_len) : malloc(1);
	if (content == NULL)
		return;
	if (content_len > 0)
		memcpy(content, data, content_len);

	if (should_fixup_this_iteration())
		ps_fuzz_crc_fixup(resolved_target_name, work_dir, content, content_len);

	if (snprintf(target_path, sizeof(target_path), "%s/%s", work_dir,
				 relpath) >= (int) sizeof(target_path))
	{
		free(content);
		return;
	}
	write_file(target_path, content, content_len);
	free(content);

	mute_output();
	if (ps_core_open(work_dir) == 0)
	{
		bounded_reads();
		(void) ps_core_maintenance();
		ps_core_close();
	}
	unmute_output();

	reset_work_dir();
}
