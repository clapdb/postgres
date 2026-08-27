#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

#include "pagestore_wal_store.h"

#define TEST_SEGMENT_BYTES (16u * 1024u * 1024u)

static int run;
static int failed;

typedef struct ConcurrentWalArgs
{
	PsWalStore *store;
	const unsigned char *append_data;
	uint64_t append_start;
	int append_rc;
	int advance_rc;
	int read_failures;
} ConcurrentWalArgs;

static void *
concurrent_append(void *arg)
{
	ConcurrentWalArgs *args = arg;

	args->append_rc = ps_wal_store_append(args->store, args->append_start,
									 args->append_data, TEST_SEGMENT_BYTES);
	return NULL;
}

static void *
concurrent_advance(void *arg)
{
	ConcurrentWalArgs *args = arg;

	args->advance_rc = -1;
	for (int i = 0; i < 100; i++)
		if (ps_wal_store_advance_retained_base(args->store,
										 TEST_SEGMENT_BYTES) == 0)
			args->advance_rc = 0;
	return NULL;
}

static void *
concurrent_read(void *arg)
{
	ConcurrentWalArgs *args = arg;
	unsigned char window[256];

	args->read_failures = 0;
	for (int i = 0; i < 100; i++)
		if (ps_wal_store_read(args->store, TEST_SEGMENT_BYTES,
							 window, sizeof(window)) != 0)
			args->read_failures++;
	return NULL;
}

static uint32_t
test_metadata_hash(const unsigned char *data, size_t len)
{
	uint32_t hash = 2166136261u;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= data[i];
		hash *= 16777619u;
	}
	return hash;
}

static void
test_put_le64(unsigned char *p, uint64_t value)
{
	for (unsigned int i = 0; i < 8; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static void
test_put_le32(unsigned char *p, uint32_t value)
{
	for (unsigned int i = 0; i < 4; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static uint32_t
test_metadata_crc(unsigned char *metadata)
{
	unsigned char copy[64];

	memcpy(copy, metadata, sizeof(copy));
	memset(copy + 48, 0, 4);
	return test_metadata_hash(copy, sizeof(copy));
}

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

static int
restore_metadata_atomically(const char *directory,
							const unsigned char *metadata)
{
	char temporary[1024];
	char identity[1024];
	int fd;
	int directory_fd;

	if (snprintf(temporary, sizeof(temporary), "%s/%s.recovery.tmp",
				 directory, PS_WAL_STORE_IDENTITY_FILE) < 0 ||
		snprintf(identity, sizeof(identity), "%s/%s", directory,
					 PS_WAL_STORE_IDENTITY_FILE) < 0)
		return -1;
	fd = open(temporary, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd < 0 || pwrite_all(fd, metadata, 64, 0) != 0 || fsync(fd) != 0)
	{
		if (fd >= 0)
			close(fd);
		unlink(temporary);
		return -1;
	}
	if (close(fd) != 0 || rename(temporary, identity) != 0)
	{
		unlink(temporary);
		return -1;
	}
	directory_fd = open(directory, O_RDONLY | O_DIRECTORY);
	if (directory_fd < 0 || fsync(directory_fd) != 0)
	{
		if (directory_fd >= 0)
			close(directory_fd);
		return -1;
	}
	return close(directory_fd);
}

int
main(void)
{
	char directory[] = "/tmp/pswalstoreXXXXXX";
	char path[1024];
	char temporary_path[2048];
	char retry_directory[512];
	char append_retry_directory[512];
	char append_ambiguous_directory[512];
	char frontier_directory[512];
	char reconcile_directory[512];
	char concurrent_directory[512];
	char create_retry_directory[512];
	char discover_directory[512];
	uint32_t first_len = 2 * TEST_SEGMENT_BYTES;
	uint32_t retry_len = 2 * TEST_SEGMENT_BYTES;
	uint64_t retained_base = UINT64_MAX;
	unsigned char *input = malloc((size_t) first_len +
								  TEST_SEGMENT_BYTES);
	unsigned char window[256];
	PsWalSegmentHeader header;
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	PsWalStore store;
	PsWalStore retry_store;
	PsWalStore reconcile_store;
	PsWalStore concurrent_store;
	PsWalStore discover_store;
	PsWalStore unopened = {0};
	int fd;

	check(mkdtemp(directory) != NULL, "create WAL segment test directory");
	check(ps_wal_store_retained_base(NULL, &retained_base) != 0 &&
		  retained_base == UINT64_MAX &&
		  ps_wal_store_retained_base(&unopened, &retained_base) != 0 &&
		  retained_base == UINT64_MAX &&
		  ps_wal_store_retained_base(&unopened, NULL) != 0,
		  "retained-base getter rejects null, unopened, and invalid outputs");
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
	check(ps_wal_store_retained_base(&store, NULL) != 0,
		  "opened retained-base getter rejects a null output without a value");
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
	check(ps_wal_store_retained_base(&store, &retained_base) == 0 &&
		  retained_base == 0 &&
		  ps_wal_store_advance_retained_base(&store, TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
		  retained_base == TEST_SEGMENT_BYTES,
		  "retained base advances atomically at a segment boundary");
	check(ps_wal_store_read(&store, 0, window, 1) != 0 &&
		  ps_wal_store_advance_retained_base(&store, 0) != 0,
		  "retained base rejects reads and monotonic rollback");
	ps_wal_store_close(&store);
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
		  retained_base == TEST_SEGMENT_BYTES &&
		  store.start_lsn == TEST_SEGMENT_BYTES && store.nentries == 2,
		  "reopen preserves the frontier and ignores the retained old prefix");
	ps_wal_store_close(&store);
	snprintf(path, sizeof(path), "%s/%s", directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	check(unlink(path) == 0 &&
		  (fd = open(path, O_CREAT | O_WRONLY, 0600)) >= 0 &&
		  dprintf(fd, "PSWALSTORE1 7 %u %020llu\n", TEST_SEGMENT_BYTES,
				  0ULL) > 0 && close(fd) == 0 &&
		  ps_wal_store_open(&store, directory, 7, 0,
						 TEST_SEGMENT_BYTES) == 0,
		  "legacy v1 identity migrates only after segment validation");
	if (fd >= 0)
		close(fd);
	ps_wal_store_close(&store);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char damaged;

		check(pread(fd, &damaged, 1, 48) == 1 &&
			  pwrite_all(fd, &((unsigned char) { (unsigned char) (damaged ^ 0xff) }),
						 1, 48) == 0,
			  "corrupt the retained-base metadata checksum");
		close(fd);
		check(ps_wal_store_open_existing(&store, directory, 7,
									 TEST_SEGMENT_BYTES) != 0,
					  "reopen rejects checksummed retained-base metadata corruption");
		fd = open(path, O_RDWR);
		check(fd >= 0 && pwrite_all(fd, &damaged, 1, 48) == 0,
			  "restore the retained-base metadata checksum");
		if (fd >= 0)
			close(fd);
	}
	else
		check(0, "open retained-base metadata for checksum corruption");
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0,
		  "reopen succeeds after retained-base metadata repair");
	ps_wal_store_close(&store);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char metadata[64];
		unsigned char original[64];

		check(pread(fd, metadata, sizeof(metadata), 0) == sizeof(metadata),
			  "read metadata before testing an advanced end frontier");
		memcpy(original, metadata, sizeof(original));
		test_put_le64(metadata + 40, 4 * (uint64_t) TEST_SEGMENT_BYTES);
		test_put_le32(metadata + 48, test_metadata_crc(metadata));
		check(pwrite_all(fd, metadata, sizeof(metadata), 0) == 0 &&
			  fsync(fd) == 0,
			  "install a checksummed metadata end ahead of the directory");
		close(fd);
		check(ps_wal_store_open_existing(&store, directory, 7,
									 TEST_SEGMENT_BYTES) != 0,
			  "reopen rejects metadata end ahead of the validated directory");
		fd = open(path, O_RDWR);
		check(fd >= 0 && pwrite_all(fd, original, sizeof(original), 0) == 0 &&
			  fsync(fd) == 0,
			  "restore metadata after the end-ahead rejection");
		if (fd >= 0)
			close(fd);
	}
	else
		check(0, "open metadata for end-ahead validation");
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0,
		  "reopen succeeds after restoring end metadata");
	snprintf(temporary_path, sizeof(temporary_path), "%s.tmp.Ab12Z9", path);
	fd = open(temporary_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(fd >= 0 && close(fd) == 0,
		  "leave a recognized retained-base metadata staging orphan");
	ps_wal_store_close(&store);
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0 &&
		  access(temporary_path, F_OK) != 0 && errno == ENOENT,
		  "reopen removes only the recognized metadata staging orphan");
	ps_wal_store_close(&store);
	check(setenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME", "1", 1) == 0,
		  "arm pre-rename metadata publication failure");
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0,
		  "open store while pre-rename metadata failure is armed");
	check(ps_wal_store_advance_retained_base(&store, 2 * TEST_SEGMENT_BYTES) != 0 &&
		  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
		  retained_base == 0,
		  "metadata publication failure leaves the in-memory retained base unchanged");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME");
	ps_wal_store_close(&store);
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
		  retained_base == 0,
		  "failed pre-publication advance leaves the durable retained base unchanged");
	{
		unsigned char old_metadata[64];

		fd = open(path, O_RDONLY);
		check(fd >= 0 && pread(fd, old_metadata, sizeof(old_metadata), 0) ==
			  sizeof(old_metadata),
			  "save the old metadata image before an ambiguous rename");
		if (fd >= 0)
			close(fd);
		check(setenv("PAGESTORE_TEST_FAIL_WAL_METADATA_DIR_FSYNC", "1", 1) == 0 &&
			  ps_wal_store_advance_retained_base(&store, 2 * TEST_SEGMENT_BYTES) != 0 &&
			  ps_wal_store_retained_base(&store, &retained_base) != 0 &&
			  ps_wal_store_read(&store, 0, window, 1) != 0 &&
			  ps_wal_store_append(&store, store.end_lsn, input,
							  TEST_SEGMENT_BYTES) != 0 &&
			  ps_wal_store_advance_retained_base(&store, 2 * TEST_SEGMENT_BYTES) != 0,
			  "directory-fsync failure fences every stateful API in this instance");
		unsetenv("PAGESTORE_TEST_FAIL_WAL_METADATA_DIR_FSYNC");
		ps_wal_store_close(&store);
		check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0 &&
			  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
			  retained_base == 2 * TEST_SEGMENT_BYTES,
			  "reopen observes the candidate metadata when it survives the crash");
		ps_wal_store_close(&store);
		check(restore_metadata_atomically(directory, old_metadata) == 0,
			  "atomically restore the old metadata image for crash recovery");
		check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0 &&
			  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
			  retained_base == 0 &&
			  ps_wal_store_read(&store, 0, window, 1) == 0 &&
			  window[0] == input[0],
			  "reopen recovers the old base and old data after crash rollback");
		ps_wal_store_close(&store);
	}

	snprintf(frontier_directory, sizeof(frontier_directory), "%s/frontier",
			 directory);
	check(ps_wal_store_create(&retry_store, frontier_directory, 15, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input, 2 * TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_advance_retained_base(&retry_store, TEST_SEGMENT_BYTES) == 0,
		  "publish a retained frontier before prefix unlink");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/walv1_15_%020llu", frontier_directory,
			 0ULL);
	check(unlink(path) == 0,
		  "unlink the authorized immutable prefix after successful advance");
	check(ps_wal_store_open_existing(&retry_store, frontier_directory, 15,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.start_lsn == TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 1 &&
		  ps_wal_store_read(&retry_store, TEST_SEGMENT_BYTES, window,
								  sizeof(window)) == 0 &&
		  memcmp(window, input + TEST_SEGMENT_BYTES, sizeof(window)) == 0,
		  "reopen validates the retained physical suffix after prefix unlink");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/walv1_15_%020llu", frontier_directory,
			 1ULL);
	unlink(path);
	snprintf(path, sizeof(path), "%s/%s", frontier_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(frontier_directory);

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
	input[0] ^= 0xff;
	check(ps_wal_store_append(&retry_store, 0, input, retry_len) != 0 &&
		  retry_store.nentries == 1 &&
		  retry_store.end_lsn == TEST_SEGMENT_BYTES,
		  "divergent retry rejects without leaving the WAL-store lock held");
	input[0] ^= 0xff;
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

	snprintf(append_retry_directory, sizeof(append_retry_directory),
			 "%s/append_retry", directory);
	check(ps_wal_store_create(&retry_store, append_retry_directory, 13, 0,
							 TEST_SEGMENT_BYTES) == 0,
		  "create an append metadata retry store");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME", "1", 1) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input, TEST_SEGMENT_BYTES) != 0 &&
		  retry_store.nentries == 0 && retry_store.end_lsn == 0,
		  "pre-rename append failure leaves only a retryable durable segment");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME");
	check(ps_wal_store_append(&retry_store, 0, input, TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.nentries == 1 && retry_store.end_lsn == TEST_SEGMENT_BYTES,
		  "same-process append retry adopts an existing durable segment");
	ps_wal_store_close(&retry_store);
	for (uint64_t segment = 0; segment < 1; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_13_%020llu",
				 append_retry_directory, (unsigned long long) segment);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/%s", append_retry_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(append_retry_directory);

	snprintf(append_ambiguous_directory, sizeof(append_ambiguous_directory),
			 "%s/append_ambiguous", directory);
	check(ps_wal_store_create(&retry_store, append_ambiguous_directory, 14, 0,
							 TEST_SEGMENT_BYTES) == 0,
		  "create an ambiguous append store");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_METADATA_DIR_FSYNC", "1", 1) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input, TEST_SEGMENT_BYTES) != 0 &&
		  retry_store.nentries == 0 && retry_store.end_lsn == 0 &&
		  retry_store.metadata_fenced,
		  "ambiguous append fences without treating candidate metadata as committed");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_METADATA_DIR_FSYNC");
	check(ps_wal_store_read(&retry_store, 0, window, 1) != 0 &&
		  ps_wal_store_retained_base(&retry_store, &retained_base) != 0 &&
		  ps_wal_store_append(&retry_store, 0, input, TEST_SEGMENT_BYTES) != 0 &&
		  ps_wal_store_advance_retained_base(&retry_store, TEST_SEGMENT_BYTES) != 0,
		  "fenced append rejects read, append, and advance in the same instance");
	ps_wal_store_close(&retry_store);
	check(ps_wal_store_open_existing(&retry_store, append_ambiguous_directory, 14,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.nentries == 1 && retry_store.end_lsn == TEST_SEGMENT_BYTES,
		  "reopen adopts candidate append metadata only after the fenced instance closes");
	check(ps_wal_store_append(&retry_store, TEST_SEGMENT_BYTES,
						  input + TEST_SEGMENT_BYTES, TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.nentries == 2 && retry_store.end_lsn == 2 * (uint64_t) TEST_SEGMENT_BYTES,
		  "reopened candidate state can be advanced normally");
	ps_wal_store_close(&retry_store);
	check(ps_wal_store_open_existing(&retry_store, append_ambiguous_directory, 14,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.nentries == 2 && retry_store.end_lsn == 2 * (uint64_t) TEST_SEGMENT_BYTES,
		  "reopen preserves the append end after fenced recovery");
	ps_wal_store_close(&retry_store);
	for (uint64_t segment = 0; segment < 2; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_14_%020llu",
				 append_ambiguous_directory, (unsigned long long) segment);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/%s", append_ambiguous_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(append_ambiguous_directory);

	snprintf(reconcile_directory, sizeof(reconcile_directory), "%s/reconcile",
			 directory);
	check(ps_wal_store_create(&reconcile_store, reconcile_directory, 11, 0,
							 TEST_SEGMENT_BYTES) == 0,
		  "create a store for stale end metadata recovery");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME", "1", 1) == 0 &&
		  ps_wal_store_append(&reconcile_store, 0, input,
							  TEST_SEGMENT_BYTES) != 0 &&
		  reconcile_store.nentries == 0 && reconcile_store.end_lsn == 0,
		  "leave a durable segment with a stale metadata end frontier");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME");
	ps_wal_store_close(&reconcile_store);
	check(ps_wal_store_open_existing(&reconcile_store, reconcile_directory, 11,
								 TEST_SEGMENT_BYTES) == 0 &&
		  reconcile_store.nentries == 1 &&
		  reconcile_store.end_lsn == TEST_SEGMENT_BYTES,
		  "reopen accepts a contiguous legal suffix and atomically repairs its end frontier");
	ps_wal_store_close(&reconcile_store);
	snprintf(path, sizeof(path), "%s/walv1_11_%020llu", reconcile_directory,
				 0ULL);
	unlink(path);
	snprintf(path, sizeof(path), "%s/%s", reconcile_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(reconcile_directory);

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
			 4ULL);
	snprintf(temporary_path, sizeof(temporary_path), "%s/walv1_10_%020llu",
			 discover_directory, 6ULL);
	check(link(path, temporary_path) == 0 &&
		  ps_wal_store_open_existing(&discover_store, discover_directory, 10,
									 TEST_SEGMENT_BYTES) != 0,
		  "restart rejects a valid but unexpected immutable suffix");
	unlink(temporary_path);
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

	snprintf(concurrent_directory, sizeof(concurrent_directory), "%s/concurrent",
			 directory);
	if (ps_wal_store_create(&concurrent_store, concurrent_directory, 12, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		ps_wal_store_append(&concurrent_store, 0, input,
							 2 * TEST_SEGMENT_BYTES) == 0)
	{
		ConcurrentWalArgs args = {
			.store = &concurrent_store,
			.append_data = input + 2 * TEST_SEGMENT_BYTES,
			.append_start = 2 * TEST_SEGMENT_BYTES,
			.append_rc = -1,
			.advance_rc = -1,
			.read_failures = 0,
		};
		pthread_t append_thread;
		pthread_t advance_thread;
		pthread_t read_thread;
		int append_created;
		int advance_created;
		int read_created;

		append_created = pthread_create(&append_thread, NULL,
								   concurrent_append, &args) == 0;
		advance_created = pthread_create(&advance_thread, NULL,
									 concurrent_advance, &args) == 0;
		read_created = pthread_create(&read_thread, NULL,
									 concurrent_read, &args) == 0;
		check(append_created && advance_created && read_created,
			  "start concurrent WAL append, advance, and read workers");
		if (append_created)
			pthread_join(append_thread, NULL);
		if (advance_created)
			pthread_join(advance_thread, NULL);
		if (read_created)
			pthread_join(read_thread, NULL);
		check(args.append_rc == 0 && args.advance_rc == 0 &&
			  args.read_failures == 0 &&
			  concurrent_store.retained_base_lsn == TEST_SEGMENT_BYTES &&
			  concurrent_store.end_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES,
			  "concurrent WAL operations preserve one monotonic frontier and end");
		ps_wal_store_close(&concurrent_store);
	}
	else
		check(0, "create and seed concurrent WAL store");
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_12_%020llu",
				 concurrent_directory, (unsigned long long) segment);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/%s", concurrent_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(concurrent_directory);

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
