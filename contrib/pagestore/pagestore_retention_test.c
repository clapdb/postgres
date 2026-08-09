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

int
main(void)
{
	char		dir[] = "/tmp/psretentionXXXXXX";
	char		path[512];
	char		tmp[520];
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

	unlink(tmp);
	unlink(path);
	rmdir(dir);
	if (!failed)
		fprintf(stderr, "retention registry test: PASS\n");
	return failed;
}
