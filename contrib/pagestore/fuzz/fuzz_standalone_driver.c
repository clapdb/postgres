/*-------------------------------------------------------------------------
 *
 * fuzz_standalone_driver.c
 *	  Non-instrumented replay driver: feeds a directory of corpus files
 *	  through ps_fuzz_run_one() (the same LLVMFuzzerTestOneInput path the
 *	  libFuzzer binary uses) without linking libFuzzer, ASan or UBSan.
 *
 * This is what the meson test suite runs (pagestore_format_fuzz_replay,
 * see meson.build): a corpus regression pass that works with the project's
 * ordinary compiler, so a clang+libFuzzer toolchain is not a CI
 * requirement.  It only catches hard crashes (SIGSEGV/SIGABRT, including
 * PAGESTORE_ASSERT_CHECKING assertions in a cassert build); it does not
 * catch the ASan/UBSan findings the instrumented binary does, which is why
 * it is a regression check on known-good corpus files, not a fuzz run.
 *
 * Usage: pagestore_format_fuzz_replay <target-name> <corpus-dir> [<corpus-dir> ...]
 * <target-name> is "all" or one of the names in fuzz_common.c's
 * ps_fuzz_targets table.  Each <corpus-dir> is scanned non-recursively for
 * regular files; a directory or file named "known-crashes" is skipped, since
 * those are confirmed findings replayed separately, not a regression gate
 * that would block CI.
 *
 *-------------------------------------------------------------------------
 */
#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "fuzz_common.h"

static long total_files = 0;

static void
replay_file(const char *target, const char *path)
{
	FILE	   *f = fopen(path, "rb");
	long		len;
	uint8_t    *buf;

	if (f == NULL)
		return;
	if (fseek(f, 0, SEEK_END) != 0)
	{
		fclose(f);
		return;
	}
	len = ftell(f);
	if (len < 0)
	{
		fclose(f);
		return;
	}
	rewind(f);
	buf = malloc((size_t) len > 0 ? (size_t) len : 1);
	if (buf == NULL)
	{
		fclose(f);
		return;
	}
	if (len > 0 && fread(buf, 1, (size_t) len, f) != (size_t) len)
	{
		free(buf);
		fclose(f);
		return;
	}
	fclose(f);

	fprintf(stderr, "replay: %s (%ld bytes)\n", path, len);
	ps_fuzz_run_one(target, buf, (size_t) len);
	free(buf);
	total_files++;
}

static void
replay_dir(const char *target, const char *dirpath)
{
	DIR		   *d = opendir(dirpath);
	struct dirent *ent;

	if (d == NULL)
		return;
	while ((ent = readdir(d)) != NULL)
	{
		char		path[4096];
		struct stat st;

		if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
			continue;
		if (strcmp(ent->d_name, "known-crashes") == 0)
			continue;
		if (snprintf(path, sizeof(path), "%s/%s", dirpath, ent->d_name) >=
			(int) sizeof(path))
			continue;
		if (stat(path, &st) != 0 || !S_ISREG(st.st_mode))
			continue;
		replay_file(target, path);
	}
	closedir(d);
}

int
main(int argc, char **argv)
{
	const char *target;
	int			i;

	if (argc < 3)
	{
		fprintf(stderr,
				"usage: %s <target-name|all> <corpus-dir> [<corpus-dir> ...]\n",
				argv[0]);
		return 2;
	}
	target = argv[1];

	ps_fuzz_global_init();
	for (i = 2; i < argc; i++)
		replay_dir(target, argv[i]);

	fprintf(stderr, "replayed %ld corpus file(s) for target '%s'\n",
			total_files, target);
	return 0;
}
