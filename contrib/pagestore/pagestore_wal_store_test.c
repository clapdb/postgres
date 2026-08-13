#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
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
	char retry_directory[512];
	char create_retry_directory[512];
	uint32_t first_len = 2 * TEST_SEGMENT_BYTES;
	uint32_t retry_len = 2 * TEST_SEGMENT_BYTES;
	unsigned char *input = malloc((size_t) first_len +
								  TEST_SEGMENT_BYTES);
	unsigned char window[256];
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	unsigned char saved_header[PS_WAL_SEGMENT_HEADER_BYTES];
	unsigned char saved_byte;
	PsWalSegmentHeader header;
	PsWalStore store;
	PsWalStore retry_store;
	int fd;

	check(mkdtemp(directory) != NULL, "create WAL segment test directory");
	for (uint32_t i = 0; i < first_len + TEST_SEGMENT_BYTES; i++)
		input[i] = (unsigned char) (i * 31u + 7u);
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
	rmdir(retry_directory);

	/* Reads validate the complete immutable file before returning any bytes. */
	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	fd = open(path, O_RDWR);
	check(fd >= 0 &&
		  pread(fd, &saved_byte, 1, PS_WAL_SEGMENT_HEADER_BYTES + 10) == 1 &&
		  pwrite(fd, (unsigned char[]) {saved_byte ^ 0xff}, 1,
				 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 1 && fsync(fd) == 0,
		  "corrupt a published payload without changing its size");
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
	fd = open(path, O_RDWR);
	check(fd >= 0 && ps_wal_segment_encode(&header, encoded) == 0 &&
		  pwrite_all(fd, encoded, sizeof(encoded), 0) == 0 &&
		  pwrite_all(fd, input, PS_WAL_SEGMENT_PAYLOAD_BYTES,
					 PS_WAL_SEGMENT_HEADER_BYTES) == 0 && fsync(fd) == 0,
		  "restore the published segment after replacement test");
	if (fd >= 0)
		close(fd);
	{
		char held[1024];

		check(ps_wal_store_read(&store, 0, window, 1) == 0,
			  "populate the validated segment cache");
		snprintf(held, sizeof(held), "%s/held_cached_segment", directory);
		check(rename(path, held) == 0 && symlink("held_cached_segment", path) == 0,
			  "replace a cached canonical segment with a symlink");
		check(ps_wal_store_read(&store, 0, window, 1) != 0,
			  "cached read rejects a symlink at the canonical segment path");
		check(unlink(path) == 0 && rename(held, path) == 0,
			  "restore the canonical cached segment path");
	}
	ps_wal_store_close(&store);

	/* Only a canonical mkstemp orphan belongs to recovery cleanup. */
	snprintf(path, sizeof(path), "%s/walv1_7_%020llu.tmp.A1b2C3",
			 directory, 3ULL);
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(fd >= 0, "create a canonical orphan temporary segment");
	if (fd >= 0)
		close(fd);
	check(ps_wal_store_open(&store, directory, 7) == 0 &&
		  access(path, F_OK) != 0 && store.nentries == 3 &&
		  store.start_lsn == 0 &&
		  store.end_lsn == first_len + PS_WAL_SEGMENT_PAYLOAD_BYTES,
		  "reopen reclaims a canonical orphan and reconstructs the WAL range");
	check(ps_wal_store_read(&store,
						PS_WAL_SEGMENT_PAYLOAD_BYTES - 64,
						window, 128) == 0 &&
		  memcmp(window, input + PS_WAL_SEGMENT_PAYLOAD_BYTES - 64, 128) == 0,
		  "reopened store reads across a segment boundary");
	ps_wal_store_close(&store);

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu.tmp.bad",
			 directory, 3ULL);
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(fd >= 0, "create a noncanonical reserved-name file");
	if (fd >= 0)
		close(fd);
	check(ps_wal_store_open(&store, directory, 7) != 0 &&
		  access(path, F_OK) == 0,
		  "reopen rejects and preserves a noncanonical reserved-name file");
	unlink(path);

	/* Every published file is authoritative: corruption, missing identities and
	 * short tails fail closed rather than silently changing retained history. */
	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 1ULL);
	fd = open(path, O_RDWR);
	check(fd >= 0 &&
		  pread(fd, &saved_byte, 1, PS_WAL_SEGMENT_HEADER_BYTES + 3) == 1 &&
		  pwrite(fd, (unsigned char[]) {saved_byte ^ 0xff}, 1,
				 PS_WAL_SEGMENT_HEADER_BYTES + 3) == 1 && fsync(fd) == 0,
		  "inject payload corruption into a published segment");
	if (fd >= 0)
		close(fd);
	check(ps_wal_store_open(&store, directory, 7) != 0,
		  "reopen rejects a payload checksum mismatch");
	fd = open(path, O_RDWR);
	check(fd >= 0 && pwrite(fd, &saved_byte, 1,
						PS_WAL_SEGMENT_HEADER_BYTES + 3) == 1 && fsync(fd) == 0,
		  "restore payload after corruption test");
	if (fd >= 0)
		close(fd);

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	fd = open(path, O_RDWR);
	check(fd >= 0 && pread(fd, saved_header, sizeof(saved_header), 0) ==
		  sizeof(saved_header), "read header for corruption injection");
	encoded[0] = saved_header[0] ^ 0xff;
	check(fd >= 0 && pwrite(fd, encoded, 1, 0) == 1 && fsync(fd) == 0,
		  "inject header corruption into a published segment");
	if (fd >= 0)
		close(fd);
	check(ps_wal_store_open(&store, directory, 7) != 0,
		  "reopen rejects a header checksum mismatch");
	fd = open(path, O_RDWR);
	check(fd >= 0 && pwrite(fd, saved_header, sizeof(saved_header), 0) ==
		  sizeof(saved_header) && fsync(fd) == 0,
		  "restore header after corruption test");
	if (fd >= 0)
		close(fd);

	{
		char missing[1024];

		snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 1ULL);
		snprintf(missing, sizeof(missing), "%s/held_segment", directory);
		check(rename(path, missing) == 0, "inject a segment-number gap");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a missing interior segment");
		check(rename(missing, path) == 0, "restore the missing segment");
	}
	{
		char held[1024];

		snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 1ULL);
		snprintf(held, sizeof(held), "%s/held_special", directory);
		check(rename(path, held) == 0 && symlink("held_special", path) == 0,
			  "replace a canonical segment with a symlink");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a symlink at a canonical segment path");
		check(unlink(path) == 0 && mkfifo(path, 0600) == 0,
			  "replace a canonical segment with a FIFO");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a FIFO without blocking");
		check(unlink(path) == 0 && rename(held, path) == 0,
			  "restore the regular segment after special-file tests");
	}
	{
		char noncanonical[1024];

		snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 2ULL);
		snprintf(noncanonical, sizeof(noncanonical),
				 "%s/walv1_07_%020llu", directory, 2ULL);
		check(rename(path, noncanonical) == 0,
			  "inject a noncanonical spelling of the requested timeline");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a numerically equivalent timeline spelling");
		check(rename(noncanonical, path) == 0,
			  "restore the canonical timeline spelling");
		snprintf(noncanonical, sizeof(noncanonical),
				 "%s/walv1_+7_%020llu", directory, 2ULL);
		check(rename(path, noncanonical) == 0,
			  "inject a signed spelling of the requested timeline");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a signed equivalent timeline spelling");
		check(rename(noncanonical, path) == 0,
			  "restore the canonical timeline after signed spelling test");
		snprintf(noncanonical, sizeof(noncanonical),
				 "%s/walv1_-7_%020llu", directory, 2ULL);
		check(rename(path, noncanonical) == 0,
			  "inject a negative spelling of the requested timeline");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a negative equivalent timeline spelling");
		check(rename(noncanonical, path) == 0,
			  "restore the canonical timeline after negative spelling test");
	}
	{
		char malformed[512];

		snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 2ULL);
		snprintf(malformed, sizeof(malformed), "%s/walv1_7", directory);
		check(rename(path, malformed) == 0,
			  "inject a current-timeline name without a suffix separator");
		check(ps_wal_store_open(&store, directory, 7) != 0,
			  "reopen rejects a current-timeline name without a suffix separator");
		check(rename(malformed, path) == 0,
			  "restore the final segment after missing-separator test");
		for (const char *alias = "walv1_+7"; alias != NULL;
			 alias = strcmp(alias, "walv1_+7") == 0 ? "walv1_07" : NULL)
		{
			snprintf(malformed, sizeof(malformed), "%s/%s", directory, alias);
			check(rename(path, malformed) == 0,
				  "inject an incomplete equivalent timeline alias");
			check(ps_wal_store_open(&store, directory, 7) != 0,
				  "reopen rejects an incomplete equivalent timeline alias");
			check(rename(malformed, path) == 0,
				  "restore the final segment after incomplete alias test");
		}
	}

	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 2ULL);
	fd = open(path, O_RDWR);
	check(fd >= 0 &&
		  ftruncate(fd, (off_t) (PS_WAL_SEGMENT_HEADER_BYTES + 210)) == 0 &&
		  fsync(fd) == 0, "inject a truncated final segment");
	if (fd >= 0)
		close(fd);
	check(ps_wal_store_open(&store, directory, 7) != 0,
		  "reopen rejects a truncated final segment");

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
