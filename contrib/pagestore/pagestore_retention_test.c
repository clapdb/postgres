/* Durable retention-registry log unit test (no daemon, no PostgreSQL). */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_retention.h"

static int failed;

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
	char		path[512];
	char		tmp[520];
	char		backup[520];
	PsRetentionPin pin = {0};
	PsRetentionPin got;
	uint32_t	count = 0;
	struct stat before,
				after;
	int			fd;
	unsigned char byte;

	if (mkdtemp(dir) == NULL)
	{
		perror("mkdtemp");
		return 2;
	}
	snprintf(path, sizeof(path), "%s/retention.meta", dir);
	snprintf(tmp, sizeof(tmp), "%s.tmp", path);
	snprintf(backup, sizeof(backup), "%s.backup", path);
	check(ps_retention_open(dir) == 0, "open an empty registry");
	pin.timeline = 7;
	pin.owner_kind = PS_RETENTION_OWNER_READER;
	pin.resources = PS_RETENTION_RESOURCE_ALL;
	pin.owner_id = 42;
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
		  got.timeline == 7 && got.owner_id == 42 && got.lsn == 169,
		  "compacted owner state survives replay");
	check(copy_file(path, backup) == 0, "save a same-sized valid old registry");
	for (uint64_t i = 0; i < 70; i++)
	{
		pin.lsn = 170 + i;
		check(ps_retention_set(&pin) == 0, "append another replacement SET");
	}
	check(ps_retention_compact() == 0, "compact replacement generation");
	check(rename(backup, path) == 0,
		  "atomically restore an older same-sized registry");
	check(ps_retention_set(&pin) != 0,
		  "exact SET rejects a same-sized registry with a different identity");
	ps_retention_close();

	fd = open(path, O_WRONLY | O_APPEND);
	check(fd >= 0 && write(fd, "short", 5) == 5 && fsync(fd) == 0,
		  "append an incomplete tail");
	if (fd >= 0)
		close(fd);
	check(ps_retention_open(dir) == 0, "short final record is recoverable");
	check(ps_retention_get(0, &got, &count) == 1 && count == 1 && got.lsn == 169,
		  "short-tail recovery preserves the committed SET");
	ps_retention_close();

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

	unlink(tmp);
	unlink(backup);
	unlink(path);
	snprintf(tmp, sizeof(tmp), "%s/retention.initialized", dir);
	unlink(tmp);
	rmdir(dir);
	if (!failed)
		fprintf(stderr, "retention registry test: PASS\n");
	return failed;
}
