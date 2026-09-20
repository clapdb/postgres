#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_storage.h"

static int run;
static int failed;

static void
check(int condition, const char *name)
{
	run++;
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failed++;
	}
}

static int
read_is(uint32_t timeline, const char *expected, uint32_t len)
{
	char buf[64];

	memset(buf, 0, sizeof(buf));
	return len <= sizeof(buf) &&
		PsStoragePosix.wal_read(timeline, 0, buf, sizeof(buf)) == (int) len &&
		memcmp(buf, expected, len) == 0;
}

static int
fork_read_is(const char *expected, uint32_t len)
{
	char buf[64];

	memset(buf, 0, sizeof(buf));
	return len <= sizeof(buf) &&
		PsStoragePosix.fork_meta_read(0, buf, sizeof(buf)) == (int) len &&
		memcmp(buf, expected, len) == 0;
}

static int
segment_read_is(uint32_t shard, int segment, const char *expected, uint32_t len)
{
	char buf[64];

	memset(buf, 0, sizeof(buf));
	return len <= sizeof(buf) &&
		PsStoragePosix.seg_read(shard, segment, 0, buf, len) == 0 &&
		memcmp(buf, expected, len) == 0;
}

int
main(void)
{
	char directory[] = "/tmp/psflatwalXXXXXX";
	char temporary[1024];
	int fd;

	check(mkdtemp(directory) != NULL, "create flat-WAL test directory");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_REWRITE_BEFORE_RENAME", "1", 1) == 0 &&
		  PsStoragePosix.open(directory, 0) == 0,
		  "open POSIX store with pre-publication fault injection");
	check(PsStoragePosix.wal_append(7, "abc", 3, "def", 3) == 0 &&
		  PsStoragePosix.wal_append(7, "ghi", 3, "j", 1) == 0 &&
		  read_is(7, "abcdefghij", 10),
		  "append the original flat WAL bytes");
	check(PsStoragePosix.wal_rewrite_prefix(7, 11) != 0 &&
		  read_is(7, "abcdefghij", 10),
		  "reject an offset beyond EOF without changing the log");
	check(PsStoragePosix.wal_rewrite_prefix(7, 3) != 0 &&
		  read_is(7, "abcdefghij", 10),
		  "a failure before rename leaves the old log authoritative");
	snprintf(temporary, sizeof(temporary), "%s/wal_7.rewrite.tmp", directory);
	check(access(temporary, F_OK) != 0 && errno == ENOENT,
		  "a reported pre-publication failure removes its staging file");
	PsStoragePosix.close();
	unsetenv("PAGESTORE_TEST_FAIL_WAL_REWRITE_BEFORE_RENAME");

	/* Model a process death after the durable temporary file but before rename. */
	fd = open(temporary, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	check(fd >= 0 && write(fd, "stale", 5) == 5 && fsync(fd) == 0 &&
		  close(fd) == 0,
		  "leave a durable pre-publication staging orphan");
	check(PsStoragePosix.open(directory, 0) == 0 &&
		  PsStoragePosix.wal_rewrite_prefix(7, 3) == 0 &&
		  read_is(7, "defghij", 7),
		  "retry overwrites an orphan and publishes exactly the retained suffix");
	check(access(temporary, F_OK) != 0 && errno == ENOENT,
		  "successful publication consumes the staging file");
	check(PsStoragePosix.wal_append(7, "kl", 2, NULL, 0) == 0 &&
		  read_is(7, "defghijkl", 9),
		  "append continues at the replacement tail");
	PsStoragePosix.close();
	check(PsStoragePosix.open(directory, 0) == 0 &&
		  read_is(7, "defghijkl", 9),
		  "restart observes the published replacement and appended tail");
	check(PsStoragePosix.wal_rewrite_prefix(7, 9) == 0 &&
		  PsStoragePosix.wal_read(7, 0, temporary, 1) == 0 &&
		  PsStoragePosix.wal_append(7, "z", 1, NULL, 0) == 0 &&
		  read_is(7, "z", 1),
		  "an empty retained suffix remains appendable");
	PsStoragePosix.close();

	check(setenv("PAGESTORE_TEST_FAIL_WAL_REWRITE_DIR_FSYNC", "1", 1) == 0 &&
		  PsStoragePosix.open(directory, 0) == 0 &&
		  PsStoragePosix.wal_append(7, "xy", 2, NULL, 0) == 0,
		  "prepare a post-rename directory-sync failure");
	check(PsStoragePosix.wal_rewrite_prefix(7, 1) != 0 &&
		  PsStoragePosix.wal_append(7, "lost", 4, NULL, 0) != 0 &&
		  PsStoragePosix.wal_read(7, 0, temporary, 1) < 0,
		  "an ambiguous rename poisons WAL access until reopen");
	PsStoragePosix.close();
	unsetenv("PAGESTORE_TEST_FAIL_WAL_REWRITE_DIR_FSYNC");
	check(PsStoragePosix.open(directory, 0) == 0 && read_is(7, "xy", 2) &&
		  PsStoragePosix.wal_append(7, "q", 1, NULL, 0) == 0 &&
		  read_is(7, "xyq", 3),
		  "reopen reconciles the visible replacement before new appends");
	PsStoragePosix.close();

	check(setenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME", "1", 1) == 0 &&
		PsStoragePosix.open(directory, 0) == 0 &&
		PsStoragePosix.fork_meta_append("old", 3) == 0 &&
		PsStoragePosix.fork_meta_rewrite("new", 3) != 0,
		"forkmeta rewrite failure before rename preserves the old log");
	check(fork_read_is("old", 3), "forkmeta old epoch remains readable");
	PsStoragePosix.close();
	unsetenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME");
	check(PsStoragePosix.open(directory, 0) == 0 &&
		PsStoragePosix.fork_meta_rewrite("new", 3) == 0 &&
		fork_read_is("new", 3),
		"forkmeta rewrite publishes a bounded replacement");
	PsStoragePosix.close();

	check(PsStoragePosix.open(directory, 0) == 0 &&
		PsStoragePosix.seg_write(0, 0, 0, "zero", 4) == 0 &&
		PsStoragePosix.seg_write(2, 3, 0, "shard-two", 9) == 0,
		"populate per-shard segment fd caches");
	check(PsStoragePosix.open(directory, 0) == 0 &&
		segment_read_is(0, 0, "zero", 4) &&
		segment_read_is(2, 3, "shard-two", 9),
		"open-again abandons and rebuilds stale segment fd caches");
	PsStoragePosix.close();
	for (int reopen = 0; reopen < 3; reopen++)
	{
		check(PsStoragePosix.open(directory, 0) == 0 &&
			segment_read_is(0, 0, "zero", 4) &&
			segment_read_is(2, 3, "shard-two", 9),
			"repeated storage close/reopen rebuilds segment fd caches");
		PsStoragePosix.close();
	}

	snprintf(temporary, sizeof(temporary), "%s/wal_7", directory);
	unlink(temporary);
	snprintf(temporary, sizeof(temporary), "%s/seg_00000000", directory);
	unlink(temporary);
	snprintf(temporary, sizeof(temporary), "%s/seg_2_00000003", directory);
	unlink(temporary);
	rmdir(directory);
	printf("storage_posix_wal_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
