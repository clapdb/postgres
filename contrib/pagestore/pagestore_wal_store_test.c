#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_wal_store.h"

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

int
main(void)
{
	char directory[] = "/tmp/pswalstoreXXXXXX";
	char path[1024];
	char retry_directory[512];
	uint32_t first_len = 2 * PS_WAL_SEGMENT_PAYLOAD_BYTES;
	uint32_t retry_len = 2 * PS_WAL_SEGMENT_PAYLOAD_BYTES;
	unsigned char *input = malloc((size_t) first_len +
								  PS_WAL_SEGMENT_PAYLOAD_BYTES);
	unsigned char window[256];
	PsWalSegmentHeader header;
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	PsWalStore store;
	PsWalStore retry_store;
	int fd;

	check(mkdtemp(directory) != NULL, "create WAL segment test directory");
	for (uint32_t i = 0; i < first_len + PS_WAL_SEGMENT_PAYLOAD_BYTES; i++)
		input[i] = (unsigned char) (i * 31u + 7u);
	check(ps_wal_store_create(&store, directory, 7, 0) == 0,
		  "create an empty timeline WAL segment store");
	check(ps_wal_store_append(&store, 0, input, first_len) == 0 &&
		  store.nentries == 2 && store.end_lsn == first_len,
		  "one append splits at the immutable segment payload boundary");
	check(ps_wal_store_read(&store,
						PS_WAL_SEGMENT_PAYLOAD_BYTES - 64,
						window, 128) == 0 &&
		  memcmp(window, input + PS_WAL_SEGMENT_PAYLOAD_BYTES - 64, 128) == 0,
		  "one read crosses two immutable segment files");
	check(ps_wal_store_append(&store, store.end_lsn, input + first_len, 211) != 0 &&
		  store.nentries == 2 && store.end_lsn == first_len,
		  "partial append is rejected without stranding an immutable segment");
	check(ps_wal_store_append(&store, store.end_lsn, input + first_len,
								 PS_WAL_SEGMENT_PAYLOAD_BYTES) == 0 &&
		  store.nentries == 3,
		  "a later complete append publishes the next segment identity");
	check(ps_wal_store_read(&store, first_len - 32, window, 96) == 0 &&
		  memcmp(window, input + first_len - 32, 96) == 0,
		  "read spans segment files created by separate appends");
	check(ps_wal_store_append(&store, store.end_lsn + 1, input, 1) != 0,
		  "append rejects a gap in shipped WAL");
	check(ps_wal_store_read(&store, UINT64_MAX, window, 1) != 0 &&
		  ps_wal_store_read(&store, store.end_lsn, window, 1) != 0,
		  "read rejects bytes outside the contiguous retained range");
	check(ps_wal_store_create(&retry_store, path, 8, 1) != 0,
		  "store creation rejects a noncanonical segment start");

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	fd = open(path, O_RDONLY);
	check(fd >= 0 && read(fd, encoded, sizeof(encoded)) == sizeof(encoded) &&
		  ps_wal_segment_decode(&header, encoded, sizeof(encoded)) == 0 &&
		  header.timeline == 7 && header.segment_no == 0 &&
		  header.payload_len == PS_WAL_SEGMENT_PAYLOAD_BYTES,
		  "published file carries its durable segment identity");
	if (fd >= 0)
		close(fd);

	snprintf(retry_directory, sizeof(retry_directory), "%s/retry", directory);
	check(ps_wal_store_create(&retry_store, retry_directory, 8, 0) == 0,
		  "create retryable split-append store");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_SEGMENT_NO", "1", 1) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input, retry_len) != 0 &&
		  retry_store.nentries == 1 &&
		  retry_store.end_lsn == PS_WAL_SEGMENT_PAYLOAD_BYTES,
		  "split append reports a failure after its durable prefix");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_SEGMENT_NO");
	check(ps_wal_store_append(&retry_store, 0, input, retry_len) == 0 &&
		  retry_store.nentries == 2 &&
		  retry_store.end_lsn == retry_len,
		  "identical retry validates its prefix and resumes publication");
	ps_wal_store_close(&retry_store);
	for (uint64_t segment = 0; segment < 2; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_8_%020llu", retry_directory,
				 (unsigned long long) segment);
		unlink(path);
	}
	rmdir(retry_directory);

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char byte;

		check(pread(fd, &byte, 1, PS_WAL_SEGMENT_HEADER_BYTES + 10) == 1 &&
			  pwrite(fd, (unsigned char[]) {byte ^ 0xff}, 1,
					 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 1,
			  "corrupt a published payload without changing its size");
		close(fd);
	}
	check(fd >= 0 && ps_wal_store_read(&store, 10, window, 1) != 0,
		  "read validates payload checksum before returning WAL bytes");
	ps_wal_store_close(&store);
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory,
				 (unsigned long long) segment);
		unlink(path);
	}
	rmdir(directory);
	free(input);

	printf("pagestore_wal_store_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
