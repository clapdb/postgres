#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

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

typedef struct ReclaimBarrierArgs
{
	PsWalStore *store;
	const unsigned char *data;
	int read_rc;
	int reclaim_rc;
	atomic_int read_started;
	atomic_int read_done;
	atomic_int reclaim_started;
	atomic_int reclaim_done;
} ReclaimBarrierArgs;

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

static void *
barrier_read(void *arg)
{
	ReclaimBarrierArgs *args = arg;
	unsigned char window[256];

	atomic_store(&args->read_started, 1);
	args->read_rc = ps_wal_store_read(args->store, TEST_SEGMENT_BYTES,
								 window, sizeof(window));
	atomic_store(&args->read_done, 1);
	return NULL;
}

static void *
barrier_reclaim(void *arg)
{
	ReclaimBarrierArgs *args = arg;

	atomic_store(&args->reclaim_started, 1);
	args->reclaim_rc = ps_wal_store_reclaim_prefix(args->store,
									  TEST_SEGMENT_BYTES);
	atomic_store(&args->reclaim_done, 1);
	return NULL;
}

static int
run_reclaim_crash_child(const char *directory, uint32_t timeline,
						uint64_t target_lsn, const char *hook,
						const char *value, int expected_status)
	{
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0)
		return -1;
	if (pid == 0)
	{
		PsWalStore child_store;

		if (setenv(hook, value, 1) != 0 ||
			ps_wal_store_open_existing(&child_store, directory, timeline,
									 TEST_SEGMENT_BYTES) != 0)
			_exit(120);
		(void) ps_wal_store_reclaim_prefix(&child_store, target_lsn);
		_exit(121);
	}
	if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status))
		return -1;
	return WEXITSTATUS(status) == expected_status ? 0 : -1;
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
	char partial_reclaim_directory[512];
	char fsync_reclaim_directory[512];
	char enumerate_reclaim_directory[512];
	char crash_before_directory[512];
	char crash_partial_directory[512];
	char crash_fsync_directory[512];
	char barrier_directory[512];
	char barrier_gate_path[1024];
	char barrier_release_path[1024];
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
	PsWalStore partial_reclaim_store;
	PsWalStore fsync_reclaim_store;
	PsWalStore enumerate_reclaim_store;
	PsWalStore barrier_store;
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
		  retained_base == TEST_SEGMENT_BYTES && store.start_lsn == 0 &&
		  store.nentries == 3,
		  "logical retained base advances without changing the physical catalog");
	snprintf(path, sizeof(path), "%s/walv1_7_%020llu", directory, 0ULL);
	check(ps_wal_store_read(&store, 0, window, 1) != 0 &&
		  access(path, F_OK) == 0 &&
		  ps_wal_store_advance_retained_base(&store, 0) != 0,
		  "logical advance does not delete files or permit rollback");
	ps_wal_store_close(&store);
	check(ps_wal_store_open_existing(&store, directory, 7,
								 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_retained_base(&store, &retained_base) == 0 &&
		  retained_base == TEST_SEGMENT_BYTES &&
		  store.start_lsn == 0 && store.nentries == 3,
		  "reopen preserves a logical frontier without assuming physical deletion");
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
		  ps_wal_store_append(&retry_store, 0, input, 2 * TEST_SEGMENT_BYTES) == 0,
		  "create a store for crash-safe prefix reclamation");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_BEFORE_UNLINK", "1", 1) == 0 &&
		  ps_wal_store_reclaim_prefix(&retry_store, TEST_SEGMENT_BYTES) != 0 &&
		  ps_wal_store_retained_base(&retry_store, &retained_base) == 0 &&
		  retained_base == TEST_SEGMENT_BYTES,
		  "publish the reclaim frontier before a pre-unlink stop");
	check(ps_wal_store_advance_retained_base(&retry_store,
										 2 * TEST_SEGMENT_BYTES) != 0 &&
		  ps_wal_store_append(&retry_store, 2 * TEST_SEGMENT_BYTES, input,
							  TEST_SEGMENT_BYTES) != 0 &&
		  ps_wal_store_retained_base(&retry_store, &retained_base) == 0 &&
		  retained_base == TEST_SEGMENT_BYTES &&
		  retry_store.end_lsn == 2 * TEST_SEGMENT_BYTES,
		  "pending physical reclaim blocks independent advance and append");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_BEFORE_UNLINK");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/walv1_15_%020llu", frontier_directory,
				 0ULL);
	check(ps_wal_store_open_existing(&retry_store, frontier_directory, 15,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.start_lsn == TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 1 && access(path, F_OK) == 0,
		  "restart retains an old prefix file outside the logical catalog");
	check(ps_wal_store_read(&retry_store, 0, window, 1) != 0 &&
		  ps_wal_store_reclaim_prefix(&retry_store, TEST_SEGMENT_BYTES) == 0 &&
		  access(path, F_OK) != 0 && retry_store.start_lsn == TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 1,
		  "reclaim unlinks only the authorized prefix and drains its catalog");
	check(ps_wal_store_reclaim_prefix(&retry_store, TEST_SEGMENT_BYTES) == 0 &&
		  access(path, F_OK) != 0,
		  "prefix reclaim is idempotent after the target is already installed");
	ps_wal_store_close(&retry_store);
	check(ps_wal_store_open_existing(&retry_store, frontier_directory, 15,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.start_lsn == TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 1 &&
		  ps_wal_store_read(&retry_store, TEST_SEGMENT_BYTES, window,
								 sizeof(window)) == 0 &&
		  memcmp(window, input + TEST_SEGMENT_BYTES, sizeof(window)) == 0,
		  "reopen validates the retained physical suffix after prefix unlink");
	ps_wal_store_close(&retry_store);

	/* Validate catalog entries again after open: reclaim must not trust the
	 * hashes and header captured by the initial catalog load. */
	snprintf(partial_reclaim_directory, sizeof(partial_reclaim_directory),
			 "%s/catalog_reclaim", directory);
	check(ps_wal_store_create(&partial_reclaim_store,
							 partial_reclaim_directory, 16,
							 0, TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&partial_reclaim_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0,
		  "create a catalog-validation reclaim store");
	ps_wal_store_close(&partial_reclaim_store);
	check(ps_wal_store_open_existing(&partial_reclaim_store,
								 partial_reclaim_directory, 16,
								 TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.nentries == 3,
		  "open all catalog segments before in-process corruption");
	snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
			 partial_reclaim_directory, 0ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char damaged = input[10] ^ 0xff;

		check(pwrite_all(fd, &damaged, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "corrupt catalog segment zero after open");
		close(fd);
	}
	else
		check(0, "open catalog segment zero for corruption");
	check(ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0 &&
		  partial_reclaim_store.start_lsn == 0 &&
		  partial_reclaim_store.nentries == 3,
		  "corrupt catalog segment zero prevents every unlink");
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
				 partial_reclaim_directory, (unsigned long long) segment);
		check(access(path, F_OK) == 0,
			  "low catalog corruption leaves all segments present");
	}
	/* Recompute the name because the loop above leaves path at segment two. */
	snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
			 partial_reclaim_directory, 0ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		check(pwrite_all(fd, input + 10, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "restore catalog segment zero after failed reclaim");
		close(fd);
	}
	else
		check(0, "open catalog segment zero for repair");
	check(ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 0,
		  "repairing low catalog corruption permits retry");
	ps_wal_store_close(&partial_reclaim_store);
	snprintf(path, sizeof(path), "%s/%s", partial_reclaim_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(partial_reclaim_directory);

	snprintf(partial_reclaim_directory, sizeof(partial_reclaim_directory),
			 "%s/catalog_middle_reclaim", directory);
	check(ps_wal_store_create(&partial_reclaim_store,
							 partial_reclaim_directory, 23,
							 0, TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&partial_reclaim_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0,
		  "create a middle-catalog-validation reclaim store");
	ps_wal_store_close(&partial_reclaim_store);
	check(ps_wal_store_open_existing(&partial_reclaim_store,
								 partial_reclaim_directory, 23,
								 TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.nentries == 3,
		  "reopen the middle-catalog-validation store");
	snprintf(path, sizeof(path), "%s/walv1_23_%020llu",
			 partial_reclaim_directory, 1ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char damaged = input[TEST_SEGMENT_BYTES + 10] ^ 0xff;

		check(pwrite_all(fd, &damaged, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "corrupt middle catalog segment after open");
		close(fd);
	}
	else
		check(0, "open middle catalog segment for corruption");
	check(ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0 &&
		  partial_reclaim_store.start_lsn == TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 2,
		  "middle catalog corruption stops after only lower prefix unlink");
	snprintf(path, sizeof(path), "%s/walv1_23_%020llu",
			 partial_reclaim_directory, 0ULL);
	snprintf(temporary_path, sizeof(temporary_path), "%s/walv1_23_%020llu",
			 partial_reclaim_directory, 1ULL);
	check(access(path, F_OK) != 0 && access(temporary_path, F_OK) == 0,
		  "middle catalog corruption preserves the failed segment");
	snprintf(path, sizeof(path), "%s/walv1_23_%020llu",
			 partial_reclaim_directory, 2ULL);
	check(access(path, F_OK) == 0,
		  "middle catalog corruption preserves every higher segment");
	snprintf(temporary_path, sizeof(temporary_path), "%s/walv1_23_%020llu",
			 partial_reclaim_directory, 1ULL);
	fd = open(temporary_path, O_RDWR);
	if (fd >= 0)
	{
		check(pwrite_all(fd, input + TEST_SEGMENT_BYTES + 10, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "restore middle catalog segment after failed reclaim");
		close(fd);
	}
	else
		check(0, "open middle catalog segment for repair");
	check(ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 0,
		  "repairing middle catalog corruption permits retry");
	ps_wal_store_close(&partial_reclaim_store);
	check(ps_wal_store_open_existing(&partial_reclaim_store,
								 partial_reclaim_directory, 23,
								 TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 0,
		  "reopen succeeds after repairing middle catalog corruption");
	ps_wal_store_close(&partial_reclaim_store);
	snprintf(path, sizeof(path), "%s/%s", partial_reclaim_directory,
			 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(partial_reclaim_directory);

	snprintf(partial_reclaim_directory, sizeof(partial_reclaim_directory),
			 "%s/partial_reclaim", directory);
	check(ps_wal_store_create(&partial_reclaim_store,
							 partial_reclaim_directory, 16, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&partial_reclaim_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0,
		  "create a three-segment store for partial reclaim");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_UNLINK_SEGMENT_NO", "1", 1) == 0 &&
		  ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0 &&
		  partial_reclaim_store.retained_base_lsn ==
							 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.start_lsn == TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 2,
		  "partial unlink stops before the failed segment and keeps catalog parity");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_UNLINK_SEGMENT_NO");
	snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
			 partial_reclaim_directory, 0ULL);
	check(access(path, F_OK) != 0,
		  "partial reclaim never leaves a successfully unlinked prefix in catalog");
	snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
			 partial_reclaim_directory, 1ULL);
	snprintf(temporary_path, sizeof(temporary_path), "%s/walv1_16_%020llu",
			 partial_reclaim_directory, 2ULL);
	check(access(path, F_OK) == 0 && access(temporary_path, F_OK) == 0,
		  "failed segment leaves only the contiguous residual suffix");
	ps_wal_store_close(&partial_reclaim_store);
	check(ps_wal_store_open_existing(&partial_reclaim_store,
								 partial_reclaim_directory, 16,
								 TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 0,
		  "reopen accepts the residual suffix after a partial unlink");
	snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
			 partial_reclaim_directory, 1ULL);
	check(access(path, F_OK) == 0 &&
		  ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 0 && access(path, F_OK) != 0,
		  "reclaim retry completes a partial prefix without crossing the target");
	check(ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES + 1) != 0 &&
		  ps_wal_store_reclaim_prefix(&partial_reclaim_store,
								 4 * (uint64_t) TEST_SEGMENT_BYTES) != 0,
		  "reclaim is idempotent and rejects unaligned or beyond-end targets");
	ps_wal_store_close(&partial_reclaim_store);
	check(ps_wal_store_open_existing(&partial_reclaim_store,
								 partial_reclaim_directory, 16,
								 TEST_SEGMENT_BYTES) == 0 &&
		  partial_reclaim_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  partial_reclaim_store.nentries == 0 &&
		  ps_wal_store_read(&partial_reclaim_store,
								 2 * (uint64_t) TEST_SEGMENT_BYTES,
								 window, sizeof(window)) != 0,
		  "restart preserves the completed frontier after partial retry");
	ps_wal_store_close(&partial_reclaim_store);
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_16_%020llu",
				 partial_reclaim_directory, (unsigned long long) segment);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/%s", partial_reclaim_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(partial_reclaim_directory);

	snprintf(enumerate_reclaim_directory, sizeof(enumerate_reclaim_directory),
			 "%s/enumerate_reclaim", directory);
	check(ps_wal_store_create(&enumerate_reclaim_store,
							 enumerate_reclaim_directory, 18, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&enumerate_reclaim_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0 &&
		  setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_BEFORE_UNLINK", "1", 1) == 0 &&
		  ps_wal_store_reclaim_prefix(&enumerate_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0,
		  "publish the full enumeration test frontier before stopping");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_BEFORE_UNLINK");
	ps_wal_store_close(&enumerate_reclaim_store);
	/* Reinsert the three names in reverse order so a scan which deletes while
	 * iterating would encounter a high segment before the failing segment 0. */
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		snprintf(temporary_path, sizeof(temporary_path), "%s/reorder_%llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		check(rename(path, temporary_path) == 0,
			  "stage immutable files for reverse directory enumeration");
	}
	for (uint64_t segment = 3; segment-- > 0;)
	{
		snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		snprintf(temporary_path, sizeof(temporary_path), "%s/reorder_%llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		check(rename(temporary_path, path) == 0,
			  "restore immutable files in reverse directory order");
	}
	check(ps_wal_store_open_existing(&enumerate_reclaim_store,
								 enumerate_reclaim_directory, 18,
								 TEST_SEGMENT_BYTES) == 0,
		  "open the reverse-enumerated authorized prefix");
	check(setenv("PAGESTORE_TEST_WAL_RECLAIM_SCAN_ERROR_AFTER_CANDIDATES", "1", 1) == 0 &&
		  ps_wal_store_reclaim_prefix(&enumerate_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0,
		  "scan error after a candidate fails before any residual unlink");
	unsetenv("PAGESTORE_TEST_WAL_RECLAIM_SCAN_ERROR_AFTER_CANDIDATES");
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		check(access(path, F_OK) == 0,
			  "scan error leaves every residual candidate intact");
	}
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_UNLINK_SEGMENT_NO", "0", 1) == 0 &&
		  ps_wal_store_reclaim_prefix(&enumerate_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0,
		  "candidate collection completes before a low-segment unlink failure");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_UNLINK_SEGMENT_NO");
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		check(access(path, F_OK) == 0,
			  "a failed lowest candidate leaves every higher candidate intact");
	}
	snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
			 enumerate_reclaim_directory, 0ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char damaged = input[10] ^ 0xff;

		check(pwrite_all(fd, &damaged, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "corrupt the lowest residual candidate after open");
		close(fd);
	}
	else
		check(0, "open the lowest residual candidate for corruption");
	check(ps_wal_store_reclaim_prefix(&enumerate_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0,
		  "corrupt lowest residual candidate stops reclaim before any unlink");
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
				 enumerate_reclaim_directory, (unsigned long long) segment);
		check(access(path, F_OK) == 0,
			  "low residual corruption preserves all candidates for diagnosis");
	}
	ps_wal_store_close(&enumerate_reclaim_store);
	check(ps_wal_store_open_existing(&enumerate_reclaim_store,
								 enumerate_reclaim_directory, 18,
								 TEST_SEGMENT_BYTES) != 0,
		  "reopen fails closed when a low residual segment is corrupt");
	snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
			 enumerate_reclaim_directory, 0ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		check(pwrite_all(fd, input + 10, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "restore the lowest residual candidate after fail-closed reopen");
		close(fd);
	}
	else
		check(0, "open the lowest residual candidate for repair");
	check(ps_wal_store_open_existing(&enumerate_reclaim_store,
								 enumerate_reclaim_directory, 18,
								 TEST_SEGMENT_BYTES) == 0,
		  "reopen succeeds after repairing the low residual candidate");
	snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
			 enumerate_reclaim_directory, 1ULL);
	fd = open(path, O_RDWR);
	if (fd >= 0)
	{
		unsigned char damaged = input[TEST_SEGMENT_BYTES + 10] ^ 0xff;

		check(pwrite_all(fd, &damaged, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "corrupt a middle residual candidate after open");
		close(fd);
	}
	else
		check(0, "open the middle residual candidate for corruption");
	check(ps_wal_store_reclaim_prefix(&enumerate_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) != 0,
		  "middle residual corruption stops before the middle unlink");
	snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
			 enumerate_reclaim_directory, 0ULL);
	snprintf(temporary_path, sizeof(temporary_path), "%s/walv1_18_%020llu",
			 enumerate_reclaim_directory, 1ULL);
	check(access(path, F_OK) != 0 && access(temporary_path, F_OK) == 0,
		  "middle corruption leaves the failed and higher residual candidates");
	snprintf(path, sizeof(path), "%s/walv1_18_%020llu",
			 enumerate_reclaim_directory, 2ULL);
	check(access(path, F_OK) == 0,
		  "middle corruption does not cross into the higher candidate");
	ps_wal_store_close(&enumerate_reclaim_store);
	check(ps_wal_store_open_existing(&enumerate_reclaim_store,
								 enumerate_reclaim_directory, 18,
								 TEST_SEGMENT_BYTES) != 0,
		  "reopen fails closed while the middle residual segment is corrupt");
	fd = open(temporary_path, O_RDWR);
	if (fd >= 0)
	{
		check(pwrite_all(fd, input + TEST_SEGMENT_BYTES + 10, 1,
						 PS_WAL_SEGMENT_HEADER_BYTES + 10) == 0,
			  "restore the middle residual candidate after fail-closed reopen");
		close(fd);
	}
	else
		check(0, "open the middle residual candidate for repair");
	check(ps_wal_store_open_existing(&enumerate_reclaim_store,
								 enumerate_reclaim_directory, 18,
								 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_reclaim_prefix(&enumerate_reclaim_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0,
		  "repairing the middle residual permits complete retry");
	ps_wal_store_close(&enumerate_reclaim_store);
	snprintf(path, sizeof(path), "%s/%s", enumerate_reclaim_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(enumerate_reclaim_directory);

	snprintf(fsync_reclaim_directory, sizeof(fsync_reclaim_directory),
			 "%s/fsync_reclaim", directory);
	check(ps_wal_store_create(&fsync_reclaim_store, fsync_reclaim_directory, 17, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&fsync_reclaim_store, 0, input,
							 2 * TEST_SEGMENT_BYTES) == 0,
		  "create a store for ambiguous reclaim directory fsync");
	check(setenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_DIR_FSYNC", "1", 1) == 0 &&
		  ps_wal_store_reclaim_prefix(&fsync_reclaim_store, TEST_SEGMENT_BYTES) != 0 &&
		  fsync_reclaim_store.metadata_fenced &&
		  ps_wal_store_retained_base(&fsync_reclaim_store, &retained_base) != 0,
		  "ambiguous reclaim directory fsync fences before further unlink");
	unsetenv("PAGESTORE_TEST_FAIL_WAL_RECLAIM_DIR_FSYNC");
	ps_wal_store_close(&fsync_reclaim_store);
	check(ps_wal_store_open_existing(&fsync_reclaim_store,
								 fsync_reclaim_directory, 17,
								 TEST_SEGMENT_BYTES) == 0 &&
		  fsync_reclaim_store.start_lsn == TEST_SEGMENT_BYTES &&
		  fsync_reclaim_store.nentries == 1 &&
		  ps_wal_store_reclaim_prefix(&fsync_reclaim_store,
								 2 * (uint64_t) TEST_SEGMENT_BYTES) == 0 &&
		  fsync_reclaim_store.nentries == 0,
		  "restart clears the fenced reclaim and permits an idempotent retry");
	ps_wal_store_close(&fsync_reclaim_store);
	snprintf(path, sizeof(path), "%s/%s", fsync_reclaim_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(fsync_reclaim_directory);

	snprintf(crash_before_directory, sizeof(crash_before_directory),
			 "%s/crash_before_unlink", directory);
	check(ps_wal_store_create(&retry_store, crash_before_directory, 19, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0,
		  "seed the real pre-unlink crash store");
	ps_wal_store_close(&retry_store);
	check(run_reclaim_crash_child(crash_before_directory, 19,
								 3 * (uint64_t) TEST_SEGMENT_BYTES,
								 "PAGESTORE_TEST_WAL_RECLAIM_CRASH_BEFORE_UNLINK",
								 "1", 91) == 0,
		  "child exits after durable frontier and before first unlink");
	check(ps_wal_store_open_existing(&retry_store, crash_before_directory, 19,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 0,
		  "parent reopens the pre-unlink crash state");
	for (uint64_t segment = 0; segment < 3; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_19_%020llu",
				 crash_before_directory, (unsigned long long) segment);
		check(access(path, F_OK) == 0,
			  "pre-unlink crash leaves every authorized prefix file");
	}
	check(ps_wal_store_reclaim_prefix(&retry_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0,
		  "parent retries the complete residual prefix");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/%s", crash_before_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(crash_before_directory);

	snprintf(crash_partial_directory, sizeof(crash_partial_directory),
			 "%s/crash_partial_unlink", directory);
	check(ps_wal_store_create(&retry_store, crash_partial_directory, 20, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0,
		  "seed the real partial-unlink crash store");
	ps_wal_store_close(&retry_store);
	check(run_reclaim_crash_child(crash_partial_directory, 20,
								 3 * (uint64_t) TEST_SEGMENT_BYTES,
								 "PAGESTORE_TEST_WAL_RECLAIM_CRASH_AFTER_UNLINK_SEGMENT_NO",
								 "0", 92) == 0,
		  "child exits after the first successful unlink");
	check(ps_wal_store_open_existing(&retry_store, crash_partial_directory, 20,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 0,
		  "parent reopens the partial-unlink crash state");
	snprintf(path, sizeof(path), "%s/walv1_20_%020llu",
			 crash_partial_directory, 0ULL);
	snprintf(temporary_path, sizeof(temporary_path), "%s/walv1_20_%020llu",
			 crash_partial_directory, 1ULL);
	check(access(path, F_OK) != 0 && access(temporary_path, F_OK) == 0,
		  "partial crash removes only segment zero before stopping");
	snprintf(path, sizeof(path), "%s/walv1_20_%020llu",
			 crash_partial_directory, 2ULL);
	check(access(path, F_OK) == 0 &&
		  ps_wal_store_reclaim_prefix(&retry_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0 &&
		  access(temporary_path, F_OK) != 0 && access(path, F_OK) != 0,
		  "parent retries the contiguous residual segments one and two");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/%s", crash_partial_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(crash_partial_directory);

	snprintf(crash_fsync_directory, sizeof(crash_fsync_directory),
			 "%s/crash_before_dir_fsync", directory);
	check(ps_wal_store_create(&retry_store, crash_fsync_directory, 21, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&retry_store, 0, input,
							 3 * TEST_SEGMENT_BYTES) == 0,
		  "seed the real pre-directory-fsync crash store");
	ps_wal_store_close(&retry_store);
	check(run_reclaim_crash_child(crash_fsync_directory, 21,
								 3 * (uint64_t) TEST_SEGMENT_BYTES,
								 "PAGESTORE_TEST_WAL_RECLAIM_CRASH_BEFORE_DIR_FSYNC",
								 "1", 93) == 0,
		  "child exits after all unlink calls and before directory fsync");
	check(ps_wal_store_open_existing(&retry_store, crash_fsync_directory, 21,
								 TEST_SEGMENT_BYTES) == 0 &&
		  retry_store.start_lsn == 3 * (uint64_t) TEST_SEGMENT_BYTES &&
		  retry_store.nentries == 0 &&
		  ps_wal_store_reclaim_prefix(&retry_store,
								 3 * (uint64_t) TEST_SEGMENT_BYTES) == 0,
		  "parent reopens and idempotently accepts the fully unlinked state");
	ps_wal_store_close(&retry_store);
	snprintf(path, sizeof(path), "%s/%s", crash_fsync_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	rmdir(crash_fsync_directory);
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

	snprintf(barrier_directory, sizeof(barrier_directory), "%s/barrier",
			 directory);
	check(ps_wal_store_create(&barrier_store, barrier_directory, 22, 0,
							 TEST_SEGMENT_BYTES) == 0 &&
		  ps_wal_store_append(&barrier_store, 0, input,
							 2 * TEST_SEGMENT_BYTES) == 0,
		  "seed the reclaim reader-barrier store");
	snprintf(barrier_gate_path, sizeof(barrier_gate_path), "%s/read.gate",
			 barrier_directory);
	snprintf(barrier_release_path, sizeof(barrier_release_path), "%s/read.release",
			 barrier_directory);
	check(setenv("PAGESTORE_TEST_WAL_READ_GATE", barrier_gate_path, 1) == 0 &&
		  setenv("PAGESTORE_TEST_WAL_READ_RELEASE", barrier_release_path, 1) == 0,
		  "arm the test-only reader gate");
	{
		ReclaimBarrierArgs args;
		pthread_t reader_thread;
		pthread_t reclaimer_thread;
		int reader_created;
		int reclaimer_created;
		int gate_seen = 0;
		int release_fd;

		memset(&args, 0, sizeof(args));
		args.store = &barrier_store;
		atomic_init(&args.read_started, 0);
		atomic_init(&args.read_done, 0);
		atomic_init(&args.reclaim_started, 0);
		atomic_init(&args.reclaim_done, 0);
		reader_created = pthread_create(&reader_thread, NULL, barrier_read, &args) == 0;
		for (int i = 0; reader_created && i < 5000; i++)
		{
			if (access(barrier_gate_path, F_OK) == 0)
			{
				gate_seen = 1;
				break;
			}
			usleep(1000);
		}
		reclaimer_created = gate_seen &&
			pthread_create(&reclaimer_thread, NULL, barrier_reclaim, &args) == 0;
		for (int i = 0; reclaimer_created && i < 5000; i++)
		{
			if (atomic_load(&args.reclaim_started))
				break;
			usleep(1000);
		}
		check(reader_created && reclaimer_created && gate_seen &&
			  !atomic_load(&args.read_done) && !atomic_load(&args.reclaim_done),
			  "reclaimer waits while an already-held reader owns the WAL mutex");
		release_fd = open(barrier_release_path, O_CREAT | O_WRONLY | O_TRUNC, 0600);
		if (release_fd >= 0)
			close(release_fd);
		if (reader_created)
			pthread_join(reader_thread, NULL);
		if (reclaimer_created)
			pthread_join(reclaimer_thread, NULL);
		check(args.read_rc == 0 && args.reclaim_rc == 0 &&
			  atomic_load(&args.read_done) && atomic_load(&args.reclaim_done),
			  "reader completes before the waiting reclaim proceeds");
	}
	unsetenv("PAGESTORE_TEST_WAL_READ_GATE");
	unsetenv("PAGESTORE_TEST_WAL_READ_RELEASE");
	snprintf(path, sizeof(path), "%s/walv1_22_%020llu", barrier_directory, 0ULL);
	check(access(path, F_OK) != 0 && ps_wal_store_read(&barrier_store, 0,
								 window, 1) != 0,
		  "below-frontier reads fail after the reader barrier and reclaim");
	ps_wal_store_close(&barrier_store);
	for (uint64_t segment = 0; segment < 2; segment++)
	{
		snprintf(path, sizeof(path), "%s/walv1_22_%020llu", barrier_directory,
				 (unsigned long long) segment);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/%s", barrier_directory,
				 PS_WAL_STORE_IDENTITY_FILE);
	unlink(path);
	unlink(barrier_gate_path);
	unlink(barrier_release_path);
	rmdir(barrier_directory);

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
