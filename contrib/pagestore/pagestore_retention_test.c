/* Durable retention-registry log unit test (no daemon, no PostgreSQL). */
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_retention.h"

static int failed;

#define TEST_RETENTION_MAGIC 0x4e544552u
#define TEST_RETENTION_STATE_MAGIC 0x53544552u
#define TEST_FNV_INIT 2166136261u

typedef struct TestRetentionPinV1
{
	uint32_t timeline, owner_kind, resources, generation;
	uint64_t owner_id, lsn;
} TestRetentionPinV1;

typedef struct TestRetentionRecordV1
{
	uint32_t magic, version, type, len;
	TestRetentionPinV1 pin;
	uint32_t crc, pad;
} TestRetentionRecordV1;

typedef struct TestRetentionState
{
	uint32_t magic, version;
	uint64_t nrecords;
	uint32_t log_hash, crc;
} TestRetentionState;

static uint32_t
test_fnv1a(uint32_t h, const void *data, size_t len)
{
	const unsigned char *p = data;

	for (size_t i = 0; i < len; i++)
	{
		h ^= p[i];
		h *= 16777619u;
	}
	return h;
}

static int
write_v1_registry(const char *dir)
{
	char path[512];
	TestRetentionRecordV1 rec = {0};
	TestRetentionState state = {0};
	int fd;

	rec.magic = TEST_RETENTION_MAGIC;
	rec.version = 1;
	rec.type = 1;
	rec.len = sizeof(rec);
	rec.pin.timeline = 7;
	rec.pin.owner_kind = PS_RETENTION_OWNER_READER;
	rec.pin.resources = PS_RETENTION_RESOURCE_ALL;
	rec.pin.generation = 3;
	rec.pin.owner_id = 9001;
	rec.pin.lsn = 456;
	rec.crc = test_fnv1a(TEST_FNV_INIT, &rec,
						 offsetof(TestRetentionRecordV1, crc));
	snprintf(path, sizeof(path), "%s/retention.meta", dir);
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec) ||
		fsync(fd) != 0 || close(fd) != 0)
		return -1;
	state.magic = TEST_RETENTION_STATE_MAGIC;
	state.version = 1;
	state.nrecords = 1;
	state.log_hash = test_fnv1a(TEST_FNV_INIT, &rec, sizeof(rec));
	state.crc = test_fnv1a(TEST_FNV_INIT, &state,
						   offsetof(TestRetentionState, crc));
	snprintf(path, sizeof(path), "%s/retention.state", dir);
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
		fsync(fd) != 0 || close(fd) != 0)
		return -1;
	snprintf(path, sizeof(path), "%s/retention.initialized", dir);
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	return fd >= 0 && close(fd) == 0 ? 0 : -1;
}

static void
check(int condition, const char *message)
{
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", message);
		failed = 1;
	}
}

static int
copy_file(const char *src, const char *dst)
{
	unsigned char buf[4096];
	int in = open(src, O_RDONLY);
	int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	ssize_t n;
	int rc = -1;

	if (in < 0 || out < 0)
		goto done;
	while ((n = read(in, buf, sizeof(buf))) > 0)
		if (write(out, buf, (size_t) n) != n)
			goto done;
	if (n == 0 && fsync(out) == 0)
		rc = 0;
done:
	if (in >= 0)
		close(in);
	if (out >= 0)
		close(out);
	return rc;
}

int
main(void)
{
	char		dir[] = "/tmp/psretentionXXXXXX";
	char		faildir[] = "/tmp/psretentionfailXXXXXX";
	char		legacydir[] = "/tmp/psretentionlegacyXXXXXX";
	char		migratedir[] = "/tmp/psretentionmigrateXXXXXX";
	char		path[512];
	char		tmp[520];
	char		backup[520];
	char		current[520];
	PsRetentionPin pin = {0};
	PsRetentionPin legacy = {0};
	PsRetentionPin fenced = {0};
	PsRetentionPin got;
	uint32_t	count = 0;
	struct stat before,
				after;
	int			fd;
	unsigned char byte;
	uint32_t	format_version;

	if (mkdtemp(dir) == NULL)
	{
		perror("mkdtemp");
		return 2;
	}
	check(mkdtemp(migratedir) != NULL, "create v1 migration directory");
	check(write_v1_registry(migratedir) == 0, "write committed v1 registry");
	check(ps_retention_open(migratedir) == 0, "migrate committed v1 registry");
	check(ps_retention_get(0, &got, &count) == 1 && count == 1 &&
		  got.owner_id == 9001 && got.generation == 3 && got.lsn == 456 &&
		  got.admission_seq == 0,
		  "v1 pin replays with conservative admission sequence");
	ps_retention_close();
	snprintf(path, sizeof(path), "%s/retention.meta", migratedir);
	fd = open(path, O_RDONLY);
	check(fd >= 0 && pread(fd, &format_version, sizeof(format_version),
						 sizeof(uint32_t)) == (ssize_t) sizeof(format_version) &&
		  format_version == 2,
		  "v1 migration publishes the current record format");
	if (fd >= 0)
		close(fd);
	check(ps_retention_open(migratedir) == 0,
		  "reopen registry after v1 migration");
	ps_retention_close();
	snprintf(path, sizeof(path), "%s/retention.meta", dir);
	snprintf(tmp, sizeof(tmp), "%s.tmp", path);
	snprintf(backup, sizeof(backup), "%s.backup", path);
	snprintf(current, sizeof(current), "%s.current", path);
	check(ps_retention_open(dir) == 0, "open an empty registry");
	legacy.timeline = 7;
	legacy.owner_kind = PS_RETENTION_OWNER_READER;
	legacy.resources = PS_RETENTION_RESOURCE_ALL;
	legacy.owner_id = 41;
	legacy.lsn = 80;
	check(ps_retention_set(&legacy) == PS_RETENTION_OK &&
		  ps_retention_drop(legacy.timeline, legacy.owner_kind, legacy.owner_id,
							legacy.generation) == PS_RETENTION_OK,
		  "generation zero can replay a legacy SET/DROP sequence");
	legacy.lsn++;
	check(ps_retention_set(&legacy) == PS_RETENTION_OK &&
		  ps_retention_drop(legacy.timeline, legacy.owner_kind, legacy.owner_id,
							legacy.generation) == PS_RETENTION_OK,
		  "legacy generation zero may reuse an owner key after DROP");
	fenced = legacy;
	fenced.owner_id = 43;
	fenced.generation = 5;
	fenced.lsn = 90;
	check(ps_retention_set(&fenced) == PS_RETENTION_OK &&
		  ps_retention_drop(fenced.timeline, fenced.owner_kind, fenced.owner_id,
							fenced.generation) == PS_RETENTION_OK,
		  "a current generation is durably fenced on release");
	pin.timeline = 7;
	pin.owner_kind = PS_RETENTION_OWNER_READER;
	pin.resources = PS_RETENTION_RESOURCE_ALL;
	pin.owner_id = 42;
	pin.generation = 1;
	pin.admission_seq = 77;
	pin.lsn = 99;
	pin.admission_seq = UINT64_MAX;
	check(ps_retention_set(&pin) == PS_RETENTION_ERROR,
		  "unrecoverable maximum admission sequence is rejected");
	pin.admission_seq = 77;
	for (uint64_t i = 0; i < 70; i++)
	{
		pin.lsn = 100 + i;
		check(ps_retention_set(&pin) == 0, "append a replacement SET");
	}
	check(ps_retention_should_compact(), "owner churn makes compaction due");
	check(stat(path, &before) == 0, "stat registry before compaction");
	check(ps_retention_compact() == 0, "compact registry atomically");
	check(stat(path, &after) == 0 && after.st_size < before.st_size,
		  "compaction rewrites only the live owner state");
	check(!ps_retention_should_compact(), "compacted registry is below threshold");
	ps_retention_close();

	check(ps_retention_open(dir) == 0, "reopen compacted registry");
	check(ps_retention_get(0, &got, &count) == 1 && count == 1 &&
		  got.timeline == 7 && got.owner_id == 42 && got.generation == 1 &&
		  got.lsn == 169 && got.admission_seq == 77,
		  "compacted owner state survives replay");
	check(ps_retention_set(&fenced) == PS_RETENTION_STALE,
		  "compaction preserves a released generation tombstone");
	check(ps_retention_drop(pin.timeline, pin.owner_kind, pin.owner_id,
							pin.generation) == PS_RETENTION_OK,
		  "release writes a durable generation tombstone");
	ps_retention_close();
	check(ps_retention_open(dir) == 0, "reopen a released owner tombstone");
	check(ps_retention_count(&count) == 0 && count == 0,
		  "generation tombstones are not live retention pins");
	pin.lsn = 170;
	check(ps_retention_set(&pin) == PS_RETENTION_STALE,
		  "a released generation cannot resurrect after restart");
	pin.generation = 2;
	pin.lsn = 169;
	check(ps_retention_set(&pin) == PS_RETENTION_OK,
		  "a newer generation can replace a durable tombstone");
	check(copy_file(path, backup) == 0, "save a same-sized valid old registry");
	for (uint64_t i = 0; i < 70; i++)
	{
		pin.lsn = 170 + i;
		check(ps_retention_set(&pin) == 0, "append another replacement SET");
	}
	check(ps_retention_compact() == 0, "compact replacement generation");
	check(copy_file(path, current) == 0, "save the current registry generation");
	check(rename(backup, path) == 0,
		  "atomically restore an older same-sized registry");
	check(ps_retention_set(&pin) != 0,
		  "exact SET rejects a same-sized registry with a different identity");
	ps_retention_close();
	check(ps_retention_open(dir) != 0,
		  "committed-prefix hash rejects the replaced registry after restart");
	ps_retention_close();
	check(rename(current, path) == 0,
		  "restore the committed registry generation after mismatch test");

	fd = open(path, O_WRONLY | O_APPEND);
	check(fd >= 0 && write(fd, "short", 5) == 5 && fsync(fd) == 0,
		  "append an incomplete tail");
	if (fd >= 0)
		close(fd);
	check(ps_retention_open(dir) == 0, "short final record is recoverable");
	check(ps_retention_get(0, &got, &count) == 1 && count == 1 && got.lsn == 239,
		  "short-tail recovery preserves the committed SET");
	ps_retention_close();
	check(copy_file(path, current) == 0,
		  "save registry before complete-prefix truncation");
	fd = open(path, O_WRONLY);
	check(fd >= 0 && ftruncate(fd, 0) == 0 && fsync(fd) == 0,
		  "truncate registry at a complete-record boundary");
	if (fd >= 0)
		close(fd);
	check(ps_retention_open(dir) != 0,
		  "committed-prefix state rejects complete-record loss after restart");
	ps_retention_close();
	check(rename(current, path) == 0,
		  "restore registry after complete-prefix truncation test");

	fd = open(path, O_RDWR);
	check(fd >= 0 && pread(fd, &byte, 1, 0) == 1,
		  "open a complete record for corruption");
	if (fd >= 0)
	{
		byte ^= 0x40;
		check(pwrite(fd, &byte, 1, 0) == 1 && fsync(fd) == 0,
			  "corrupt a complete record");
		close(fd);
	}
	check(ps_retention_open(dir) != 0,
		  "a complete corrupt record fails closed instead of dropping a pin");
	ps_retention_close();

	check(unlink(path) == 0, "remove an initialized retention registry");
	check(ps_retention_open(dir) != 0,
		  "an initialized store fails closed when its registry is missing");
	ps_retention_close();

	check(mkdtemp(faildir) != NULL, "create rollback-failure test directory");
	check(ps_retention_open(faildir) == 0,
		  "open registry for rollback-failure test");
	pin.lsn = 500;
	check(ps_retention_set(&pin) == 0,
		  "persist pin before rollback-failure test");
	ps_retention_close();
	check(setenv("PS_TEST_FAIL_RETENTION_APPEND_AFTER_WRITE", "1", 1) == 0,
		  "enable append fault injection");
	check(ps_retention_open(faildir) == 0,
		  "reopen registry with append fault injection");
	check(ps_retention_drop(pin.timeline, pin.owner_kind, pin.owner_id,
							pin.generation) != 0,
		  "failed DROP is not acknowledged after a proven rollback");
	check(ps_retention_get(0, &got, &count) == 1 && count == 1 &&
		  got.lsn == pin.lsn,
		  "proven rollback keeps the registry usable");
	pin.lsn++;
	check(ps_retention_set(&pin) == 0,
		  "a new mutation succeeds after the proven rollback");
	ps_retention_close();
	unsetenv("PS_TEST_FAIL_RETENTION_APPEND_AFTER_WRITE");
	check(setenv("PS_TEST_FAIL_RETENTION_APPEND_AFTER_WRITE", "1", 1) == 0 &&
		  setenv("PS_TEST_FAIL_RETENTION_ROLLBACK", "1", 1) == 0,
		  "enable retention append and rollback fault injection");
	check(ps_retention_open(faildir) == 0,
		  "reopen registry with rollback fault injection");
	check(ps_retention_drop(pin.timeline, pin.owner_kind, pin.owner_id,
							pin.generation) != 0,
		  "failed full DROP with failed rollback is not acknowledged");
	ps_retention_close();
	unsetenv("PS_TEST_FAIL_RETENTION_APPEND_AFTER_WRITE");
	unsetenv("PS_TEST_FAIL_RETENTION_ROLLBACK");
	snprintf(path, sizeof(path), "%s/retention.pending", faildir);
	check(stat(path, &before) == 0,
		  "failed rollback leaves the pre-durable fail-closed guard");
	check(ps_retention_open(faildir) != 0,
		  "restart rejects a registry with an uncertain full DROP");
	ps_retention_close();

	check(mkdtemp(legacydir) != NULL, "create legacy-guard test directory");
	check(ps_retention_open(legacydir) == 0,
		  "open registry for legacy-guard test");
	check(ps_retention_set(&pin) == 0,
		  "persist pin before legacy-guard test");
	ps_retention_close();
	snprintf(path, sizeof(path), "%s/retention.failed", legacydir);
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	check(fd >= 0 && fsync(fd) == 0,
		  "install a legacy uncertain-rollback guard");
	if (fd >= 0)
		close(fd);
	check(ps_retention_open(legacydir) != 0,
		  "migration honors the legacy fail-closed guard");
	ps_retention_close();

	unlink(tmp);
	unlink(backup);
	unlink(current);
	unlink(path);
	snprintf(tmp, sizeof(tmp), "%s/retention.initialized", dir);
	unlink(tmp);
	snprintf(tmp, sizeof(tmp), "%s/retention.state", dir);
	unlink(tmp);
	snprintf(tmp, sizeof(tmp), "%s/retention.state.tmp", dir);
	unlink(tmp);
	snprintf(tmp, sizeof(tmp), "%s/retention.pending", dir);
	unlink(tmp);
	rmdir(dir);
	snprintf(path, sizeof(path), "%s/retention.meta", faildir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.initialized", faildir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.pending", faildir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.state", faildir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.state.tmp", faildir);
	unlink(path);
	rmdir(faildir);
	snprintf(path, sizeof(path), "%s/retention.meta", legacydir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.initialized", legacydir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.state", legacydir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.failed", legacydir);
	unlink(path);
	rmdir(legacydir);
	snprintf(path, sizeof(path), "%s/retention.meta", migratedir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.initialized", migratedir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.state", migratedir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.state.tmp", migratedir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/retention.pending", migratedir);
	unlink(path);
	rmdir(migratedir);
	if (!failed)
		fprintf(stderr, "retention registry test: PASS\n");
	return failed;
}
