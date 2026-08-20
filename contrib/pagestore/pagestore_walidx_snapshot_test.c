#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_walidx_snapshot.h"

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
pwrite_byte(const char *path, off_t offset, unsigned char value)
{
	int fd = open(path, O_RDWR);
	int rc = -1;

	if (fd >= 0 && pwrite(fd, &value, 1, offset) == 1 && fsync(fd) == 0)
		rc = 0;
	if (fd >= 0)
		close(fd);
	return rc;
}

int
main(void)
{
	char root[] = "/tmp/pswalidxsnapXXXXXX";
	char directory[1024];
	char path[1200];
	unsigned char shard0[257];
	unsigned char shard1[513];
	unsigned char newer0[257];
	unsigned char out[64];
	PsWalIdxSnapshotInput first[3];
	PsWalIdxSnapshotInput second[3];
	PsWalIdxSnapshot snapshot;
	PsWalIdxSnapshot wrong;

	check(mkdtemp(root) != NULL, "create snapshot test root");
	for (size_t i = 0; i < sizeof(shard0); i++)
		shard0[i] = (unsigned char) (i * 3u + 1u);
	for (size_t i = 0; i < sizeof(shard1); i++)
		shard1[i] = (unsigned char) (i * 5u + 2u);
	memcpy(newer0, shard0, sizeof(newer0));
	newer0[100] ^= 0x5a;
	first[0] = (PsWalIdxSnapshotInput) {shard0, sizeof(shard0)};
	first[1] = (PsWalIdxSnapshotInput) {shard1, sizeof(shard1)};
	first[2] = (PsWalIdxSnapshotInput) {NULL, 0};
	memcpy(second, first, sizeof(second));
	second[0].data = newer0;
	snprintf(directory, sizeof(directory), "%s/timeline_0", root);

	check(ps_walidx_snapshot_publish(directory, 0, 0, 100, 500,
								 first, 3) != 0 && access(directory, F_OK) != 0,
		  "invalid generation is rejected before filesystem changes");
	check(ps_walidx_snapshot_publish(directory, 0, 1, 100, 500,
								 first, 3) == 0,
		  "publish the first complete shard generation");
	check(ps_walidx_snapshot_open(&snapshot, directory, 0) == 0 &&
		  snapshot.generation == 1 && snapshot.nshards == 3 &&
		  snapshot.start_lsn == 100 && snapshot.end_lsn == 500 &&
		  snapshot.shards[2].len == 0,
		  "reopen root timeline generation including an empty shard");
	check(ps_walidx_snapshot_read(&snapshot, 1, sizeof(shard1) - 10,
								out, sizeof(out)) != 0,
		  "range reads reject a request beyond the immutable shard");
	check(ps_walidx_snapshot_open(&wrong, directory, 1) != 0,
		  "recovery rejects a manifest for a different timeline");
	check(ps_walidx_snapshot_read(&snapshot, 1, 240, out, sizeof(out)) == 0 &&
		  memcmp(out, shard1 + 240, sizeof(out)) == 0,
		  "read a bounded range from a validated immutable shard");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_publish(directory, 0, 1, 100, 500,
								 first, 3) == 0,
		  "an identical publication retry is idempotent");
	check(ps_walidx_snapshot_publish(directory, 0, 1, 100, 500,
								 second, 3) != 0,
		  "a divergent retry cannot replace an immutable generation");

	check(setenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD", "0", 1) == 0 &&
		  ps_walidx_snapshot_publish(directory, 0, 2, 500, 900,
								 second, 3) != 0,
		  "fault before manifest publication leaves generation one selected");
	unsetenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD");
	{
		uint64_t next_generation = 0;

		check(ps_walidx_snapshot_next_generation(directory, 1,
										  &next_generation) == 0 &&
			  next_generation == 3,
			  "generation allocation skips immutable publication debris");
	}
	check(ps_walidx_snapshot_open(&snapshot, directory, 0) == 0 &&
		  snapshot.generation == 1,
		  "recovery ignores an incomplete unpublished generation");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_publish(directory, 0, 2, 500, 900,
								 second, 3) == 0 &&
		  ps_walidx_snapshot_open(&snapshot, directory, 0) == 0 &&
		  snapshot.generation == 2 && snapshot.start_lsn == 500 &&
		  snapshot.end_lsn == 900,
		  "retry completes and atomically selects the next generation");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_publish(directory, 0, 1, 100, 500,
								 first, 3) != 0,
		  "publication cannot move the durable generation backward");
	check(ps_walidx_snapshot_publish(directory, 0, 3, 400, 1000,
								 second, 3) != 0,
		  "a newer generation cannot move its retained frontier backward");

	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 directory, 2ULL, 1U);
	check(pwrite_byte(path, 17, shard1[17] ^ 0xff) == 0,
		  "corrupt one selected shard byte");
	check(ps_walidx_snapshot_open(&snapshot, directory, 0) != 0,
		  "recovery rejects a corrupt shard in the selected generation");
	check(pwrite_byte(path, 17, shard1[17]) == 0 &&
		  ps_walidx_snapshot_open(&snapshot, directory, 0) == 0,
		  "recovery succeeds after the shard is restored");
	ps_walidx_snapshot_close(&snapshot);

	snprintf(path, sizeof(path), "%s/walidx_manifest_v1", directory);
	check(pwrite_byte(path, 32, 0xff) == 0,
		  "corrupt the selected manifest checksum domain");
	check(ps_walidx_snapshot_open(&snapshot, directory, 0) != 0,
		  "recovery fails closed on a corrupt generation manifest");

	/* The test owns every path in this private directory. */
	for (uint32_t shard = 0; shard < 3; shard++)
	{
		snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
				 directory, 1ULL, shard);
		unlink(path);
		snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
				 directory, 2ULL, shard);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/walidx_manifest_v1", directory);
	unlink(path);
	rmdir(directory);
	rmdir(root);

	printf("pagestore_walidx_snapshot_test: %d checks, %d failed\n",
		   run, failed);
	return failed != 0;
}
