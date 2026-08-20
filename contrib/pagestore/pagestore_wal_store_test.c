#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_wal_store.h"

#define TEST_SEGMENT_BYTES (16u * 1024u * 1024u)

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
pwrite_all(int fd, const void *data, size_t len, off_t offset)
{
	const unsigned char *bytes = data;
	size_t done = 0;

	while (done < len)
	{
		ssize_t amount = pwrite(fd, bytes + done, len - done,
							 offset + (off_t) done);

		if (amount <= 0)
			return -1;
		done += (size_t) amount;
	}
	return 0;
}

int
main(void)
{
	char directory[] = "/tmp/pswalstoreXXXXXX";
	char path[1024];
	char temporary_path[1024];
	char retry_directory[512];
	char create_retry_directory[512];
	char discover_directory[512];
	uint32_t first_len = 2 * TEST_SEGMENT_BYTES;
	uint32_t retry_len = 2 * TEST_SEGMENT_BYTES;
	unsigned char *input = malloc((size_t) first_len +
								  TEST_SEGMENT_BYTES);
	unsigned char window[256];
	PsWalSegmentHeader header;
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	PsWalStore store;
	PsWalStore retry_store;
	PsWalStore discover_store;
	int fd;

	check(mkdtemp(directory) != NULL, "create WAL segment test directory");
	for (uint32_t i = 0; i < first_len + TEST_SEGMENT_BYTES; i++)
		input[i] = (unsigned char) (i * 31u + 7u);
	snprintf(path, sizeof(path), "%s/zero", directory);
	check(ps_wal_store_create(&store, path, 0, 0, TEST_SEGMENT_BYTES) != 0 &&
		  access(path, F_OK) != 0,
		  "store creation rejects an unset timeline before filesystem changes");
	check(ps_wal_store_create(&store, directory, 7, 0, TEST_SEGMENT_BYTES) == 0,
		  "create an empty timeline WAL segment store");
	check((fcntl(store.directory_fd, F_GETFD) & FD_CLOEXEC) != 0,
		  "the WAL store directory descriptor is close-on-exec");
	check(ps_wal_store_append(&store, 0, input, first_len) == 0 &&
		  store.nentries == 2 && store.end_lsn == first_len,
		  "one append splits at the immutable segment payload boundary");
	check(ps_wal_store_read(&store,
						TEST_SEGMENT_BYTES - 64,
						window, 128) == 0 &&
		  memcmp(window, input + TEST_SEGMENT_BYTES - 64, 128) == 0,
		  "one read crosses two immutable segment files");
	check(ps_wal_store_append(&store, store.end_lsn, input + first_len, 211) != 0 &&
		  store.nentries == 2 && store.end_lsn == first_len,
		  "partial append is rejected without stranding an immutable segment");
	check(ps_wal_store_append(&store, store.end_lsn, input + first_len,
								 TEST_SEGMENT_BYTES) == 0 &&
		  store.nentries == 3,
		  "a later complete append publishes the next segment identity");
	check(ps_wal_store_read(&store, first_len - 32, window, 96) == 0 &&
		  memcmp(window, input + first_len - 32, 96) == 0,
		  "read spans segment files created by separate appends");
	ps_wal_store_close(&store);
	snprintf(temporary_path, sizeof(temporary_path),
			 "%s/walv1_7_%020llu.tmp.Ab12Z9", directory, 3ULL);
	fd = open(temporary_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(fd >= 0 && write(fd, "partial", 7) == 7 && close(fd) == 0,
		  "leave a recognized pre-publication staging file");
	check(ps_wal_store_open(&store, directory, 7, 0,
						 TEST_SEGMENT_BYTES) == 0 &&
		  store.nentries == 3 && store.end_lsn == first_len + TEST_SEGMENT_BYTES,
		  "restart rebuilds a contiguous validated segment catalog");
	check(access(temporary_path, F_OK) != 0 && errno == ENOENT,
		  "restart durably removes a recognized staging orphan");
	check(ps_wal_store_read(&store, first_len - 32, window, 96) == 0 &&
		  memcmp(window, input + first_len - 32, 96) == 0,
		  "reopened store serves a cross-segment read");
	ps_wal_store_close(&store);
	snprintf(path, sizeof(path), "%s/%s", directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	check(unlink(path) == 0 &&
		  ps_wal_store_open(&store, directory, 7, 0,
						 TEST_SEGMENT_BYTES) == 0,
		  "explicit-start open validates a legacy store before identity backfill");
	ps_wal_store_close(&store);
	check(access(path, F_OK) == 0 &&
		  ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0,
		  "legacy identity backfill survives loss of flat-log start metadata");
	ps_wal_store_close(&store);
	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 1ULL);
	snprintf(temporary_path, sizeof(temporary_path), "%s/held_segment", directory);
	check(rename(path, temporary_path) == 0,
		  "temporarily remove the middle segment from the catalog");
	check(ps_wal_store_open(&store, directory, 7, 0,
						 TEST_SEGMENT_BYTES) != 0,
		  "restart rejects a gap in immutable segment identities");
	check(rename(temporary_path, path) == 0 &&
		  ps_wal_store_open(&store, directory, 7, 0,
						 TEST_SEGMENT_BYTES) == 0,
		  "restart succeeds after the contiguous segment is restored");
	check(ps_wal_store_append(&store, store.end_lsn + 1, input, 1) != 0,
		  "append rejects a gap in shipped WAL");
	check(ps_wal_store_read(&store, UINT64_MAX, window, 1) != 0 &&
		  ps_wal_store_read(&store, store.end_lsn, window, 1) != 0,
		  "read rejects bytes outside the contiguous retained range");
	check(ps_wal_store_create(&retry_store, path, 8, 1, TEST_SEGMENT_BYTES) != 0,
		  "store creation rejects a noncanonical segment start");
	snprintf(create_retry_directory, sizeof(create_retry_directory),
			 "%s/create_retry", directory);
	check(setenv("PAGESTORE_TEST_FAIL_WAL_PARENT_FSYNC", "1", 1) == 0 &&
		  ps_wal_store_create(&retry_store, create_retry_directory, 9, 0,
							  TEST_SEGMENT_BYTES) != 0,
		  "store creation can fail after mkdir but before parent durability");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_PARENT_FSYNC");
	check(ps_wal_store_create(&retry_store, create_retry_directory, 9, 0,
							 TEST_SEGMENT_BYTES) == 0,
		  "an EEXIST retry still makes the directory entry durable");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/%s", create_retry_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(create_retry_directory);

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	fd = open(path, O_RDONLY);
	check(fd >= 0 && read(fd, encoded, sizeof(encoded)) == sizeof(encoded) &&
		  ps_wal_segment_decode(&header, encoded, sizeof(encoded)) == 0 &&
		  header.timeline == 7 && header.segment_no == 0 &&
		  header.payload_len == TEST_SEGMENT_BYTES,
		  "published file carries its durable segment identity");
	if (fd >= 0)
		close(fd);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char damaged = input[10] ^ 0xff;

		check(pwrite_all(fd, &damaged, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "corrupt one byte in a verification chunk");
		check(ps_wal_store_read(&store, 10, window, 1) != 0,
			  "range read rejects corruption in its verification chunk");
		check(pwrite_all(fd, input + 10, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "restore the verification chunk after corruption test");
		close(fd);
	}
	else
		check(0, "open WAL segment for verification-chunk corruption");

	snprintf(retry_directory, sizeof(retry_directory), "%s/retry", directory);
	check(ps_wal_store_create(&retry_store, retry_directory, 8, 0,
							 TEST_SEGMENT_BYTES) == 0,
		  "create retryable split-append store");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_SEGMENT_NO", "1", 1) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input, retry_len) != 0 &&
		  retry_store.nentries == 1 &&
		  retry_store.end_lsn == TEST_SEGMENT_BYTES,
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
	snprintf(path, sizeof(path), "%s/%s", retry_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(retry_directory);

	snprintf(discover_directory, sizeof(discover_directory), "%s/discover",
			 directory);
	check(ps_wal_store_create(&discover_store, discover_directory, 10,
								3ULL * TEST_SEGMENT_BYTES,
								TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&discover_store, 3ULL * TEST_SEGMENT_BYTES,
						  input, 2 * TEST_SEGMENT_BYTES) == 0,
		  "create a nonzero-start immutable WAL store");
	ps_wal_store_close(&discover_store);
	check(ps_wal_store_open_existing(&discover_store, discover_directory, 10,
								 TEST_SEGMENT_BYTES) == 0 &&
		  discover_store.start_lsn == 3ULL * TEST_SEGMENT_BYTES &&
		  discover_store.end_lsn == 5ULL * TEST_SEGMENT_BYTES,
		  "reopen discovers the immutable start without flat-log metadata");
	ps_wal_store_close(&discover_store);
	snprintf(path, sizeof(path), "%s/walv1_10_%020llu", discover_directory,
			 3ULL);
	check(unlink(path) == 0 &&
		  ps_wal_store_open_existing(&discover_store, discover_directory, 10,
								 TEST_SEGMENT_BYTES) != 0,
		  "durable identity rejects a shifted store when its first segment is missing");
	snprintf(path, sizeof(path), "%s/walv1_10_%020llu", discover_directory,
			 4ULL);
	unlink(path);
	snprintf(path, sizeof(path), "%s/%s", discover_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(discover_directory);

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		PsWalSegmentHeader mismatched;

		check(ps_wal_segment_seal(&mismatched, 7, 0, 0,
								  2 * TEST_SEGMENT_BYTES, input,
								  TEST_SEGMENT_BYTES) == 0 &&
			  ps_wal_segment_encode(&mismatched, encoded) == 0 &&
			  pwrite_all(fd, encoded, sizeof(encoded), 0) == 0,
			  "replace a segment header with a mismatched segment size");
		close(fd);
	}
	check(fd >= 0 && ps_wal_store_read(&store, 10, window, 1) != 0,
		  "read rejects a persisted segment-size mismatch");
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		PsWalSegmentHeader restored;

		check(ps_wal_segment_seal(&restored, 7, 0, 0,
								  TEST_SEGMENT_BYTES, input,
								  TEST_SEGMENT_BYTES) == 0 &&
			  ps_wal_segment_encode(&restored, encoded) == 0 &&
			  pwrite_all(fd, encoded, sizeof(encoded), 0) == 0,
			  "restore the canonical segment header after mismatch test");
		close(fd);
	}
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		PsWalSegmentHeader replacement;
		unsigned char *different = malloc(TEST_SEGMENT_BYTES);

		if (different != NULL)
		{
			memcpy(different, input, TEST_SEGMENT_BYTES);
			different[10] ^= 0xff;
		}
		check(different != NULL &&
			  ps_wal_segment_seal(&replacement, 7, 0, 0,
								  TEST_SEGMENT_BYTES, different,
								  TEST_SEGMENT_BYTES) == 0 &&
			  ps_wal_segment_encode(&replacement, encoded) == 0 &&
			  pwrite_all(fd, encoded, sizeof(encoded), 0) == 0 &&
			  pwrite_all(fd, different, TEST_SEGMENT_BYTES,
						 PS_WAL_SEGMENT_HEADER_BYTES) == 0,
			  "replace a segment with different internally valid WAL");
		free(different);
		close(fd);
	}
	check(fd >= 0 && ps_wal_store_read(&store, 10, window, 1) != 0,
		  "read compares the persisted checksum with its published header");
	ps_wal_store_close(&store);
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory,
				 (unsigned long long) segment);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/%s", directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(directory);
	free(input);

	printf("pagestore_wal_store_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
