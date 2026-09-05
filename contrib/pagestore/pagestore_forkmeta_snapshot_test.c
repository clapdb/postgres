#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_forkmeta_snapshot.h"

static int runs;
static int failures;

typedef struct Producer
{
	const unsigned char *data;
	size_t len;
	size_t fragment;
	int extra;
} Producer;

typedef struct FlakyProducer
{
	const unsigned char *data;
	size_t len;
	unsigned int calls;
	unsigned int fail_call;
} FlakyProducer;

static void
check(int condition, const char *name)
{
	runs++;
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failures++;
	}
}

static int
produce(void *arg, PsForkmetaSnapshotConsume consume, void *consume_arg)
{
	Producer *producer = arg;
	size_t offset = 0;

	while (offset < producer->len)
	{
		size_t amount = producer->len - offset < producer->fragment ?
			producer->len - offset : producer->fragment;

		if (consume(consume_arg, producer->data + offset, amount) != 0)
			return -1;
		offset += amount;
	}
	if (producer->extra != 0 && consume(consume_arg, "!", 1) != 0)
		return -1;
	return 0;
}

static int
produce_flaky(void *arg, PsForkmetaSnapshotConsume consume, void *consume_arg)
{
	FlakyProducer *producer = arg;

	producer->calls++;
	if (producer->calls == producer->fail_call)
		return -1;
	return consume(consume_arg, producer->data, producer->len);
}

static int
make_dir(const char *root, const char *name, char *directory, size_t len)
{
	int n = snprintf(directory, len, "%s/%s", root, name);

	return n < 0 || (size_t) n >= len || mkdir(directory, 0700) != 0 ? -1 : 0;
}

static int
write_file(const char *path, const void *data, size_t len, int append)
{
	int fd = open(path, O_WRONLY | O_CREAT | (append ? O_APPEND : O_TRUNC),
				 0600);
	int rc = -1;

	if (fd >= 0 && write(fd, data, len) == (ssize_t) len && fsync(fd) == 0)
		rc = 0;
	if (fd >= 0)
		(void) close(fd);
	return rc;
}

static int
part_path(const char *directory, const char *prefix, uint64_t generation,
		  char *path, size_t path_len)
{
	int n = snprintf(path, path_len, "%s/%s%020llu", directory, prefix,
				 (unsigned long long) generation);

	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

static int
manifest_path(const char *directory, char *path, size_t path_len)
{
	int n = snprintf(path, path_len, "%s/forkmeta_manifest_v1", directory);

	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

static int
prepared_path(const char *directory, char *path, size_t path_len)
{
	int n = snprintf(path, path_len, "%s/forkmeta_prepared_v1", directory);

	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

static int
path_suffix(const char *base, const char *suffix, char *path, size_t path_len)
{
	size_t base_len = strlen(base);
	size_t suffix_len = strlen(suffix);

	if (base_len >= path_len || suffix_len > path_len - base_len - 1)
		return -1;
	memcpy(path, base, base_len);
	memcpy(path + base_len, suffix, suffix_len + 1);
	return 0;
}

typedef struct ObservationRetryTest
{
	char debris[1200];
	unsigned int calls;
} ObservationRetryTest;

typedef struct GcInspectionTest
{
	unsigned int calls;
} GcInspectionTest;

typedef struct GenerationScanTest
{
	unsigned int calls;
	unsigned int recognized;
	unsigned int unrecognized;
	uint64_t maximum_generation;
	uint64_t last_highest;
	int monotonic;
} GenerationScanTest;

static void
gc_inspection_hook(void *arg)
{
	GcInspectionTest *test = arg;

	test->calls++;
}

static void
generation_scan_hook(const char *name, uint64_t generation,
					 uint64_t highest, void *arg)
{
	GenerationScanTest *test = arg;

	(void) name;
	test->calls++;
	if (generation == 0)
		test->unrecognized++;
	else
	{
		test->recognized++;
		if (generation > test->maximum_generation)
			test->maximum_generation = generation;
	}
	if (highest < test->last_highest)
		test->monotonic = 0;
	test->last_highest = highest;
}

static void
observation_retry_hook(unsigned int attempt, void *arg)
{
	ObservationRetryTest *test = arg;

	if (attempt == 0)
	{
		test->calls++;
		(void) write_file(test->debris, "retry-debris", 12, 0);
	}
}

static int
sync_directory(const char *directory)
{
	int fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	int rc = -1;

	if (fd >= 0 && fsync(fd) == 0)
		rc = 0;
	if (fd >= 0)
		(void) close(fd);
	return rc;
}

static int
remove_tree(const char *path)
{
	struct dirent *entry;
	DIR *dir = opendir(path);
	int rc = 0;

	if (dir == NULL)
		return unlink(path);
	while ((entry = readdir(dir)) != NULL)
	{
		char child[1400];
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) < 0 ||
			lstat(child, &st) != 0)
		{
			rc = -1;
			continue;
		}
		if (S_ISDIR(st.st_mode))
		{
			if (remove_tree(child) != 0)
				rc = -1;
		}
		else if (unlink(child) != 0)
			rc = -1;
	}
	if (closedir(dir) != 0 || rmdir(path) != 0)
		rc = -1;
	return rc;
}

static int
prepare_two(PsForkmetaSnapshotPrepared *prepared, const char *directory,
			uint64_t generation, uint64_t lsn, uint64_t seq,
			const unsigned char *checkpoint, size_t checkpoint_len,
			const unsigned char *tail, size_t tail_len)
{
	PsForkmetaSnapshotInput cp = {
		.data = checkpoint, .len = checkpoint_len
	};
	PsForkmetaSnapshotInput tl = {.data = tail, .len = tail_len};

	return ps_forkmeta_snapshot_prepare(prepared, directory, generation, lsn,
									seq, &cp, &tl);
}

int
main(void)
{
	char root[] = "/tmp/psforkmetasnapXXXXXX";
	char directory[1024];
	char path[1200];
	unsigned char checkpoint[] = "checkpoint-v1";
	unsigned char tail[] = "captured-tail-v1-with-more-bytes";
	unsigned char replacement[] = "checkpoint-v2";
	unsigned char output[64];
	PsForkmetaSnapshot snapshot;
	PsForkmetaSnapshotPrepared prepared;
	PsForkmetaSnapshotPrepared recovered;
	Producer streamed = {tail, sizeof(tail) - 1, 3, 0};
	PsForkmetaSnapshotInput stream_input = {
		.len = sizeof(tail) - 1, .produce = produce, .produce_arg = &streamed
	};
	PsForkmetaSnapshotInput cp_input = {
		.data = checkpoint, .len = sizeof(checkpoint) - 1
	};
	PsForkmetaSnapshotInput empty_input = {.data = NULL, .len = 0};

	check(mkdtemp(root) != NULL, "create snapshot test root");
	check(make_dir(root, "main", directory, sizeof(directory)) == 0,
		  "create main directory");
	check(ps_forkmeta_snapshot_publish(directory, 1, 100, 7, &cp_input,
								  &stream_input) == 0,
		  "initial publish with fragmented producer");
	check(ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  snapshot.generation == 1 && snapshot.cutoff_lsn == 100 &&
		  snapshot.cutoff_admission_seq == 7 &&
		  snapshot.checkpoint.len == sizeof(checkpoint) - 1 &&
		  snapshot.tail.len == sizeof(tail) - 1,
		  "open selected checkpoint and captured tail");
	check(ps_forkmeta_snapshot_read_checkpoint(&snapshot, 2, output, 5) == 0 &&
		  memcmp(output, checkpoint + 2, 5) == 0 &&
		  ps_forkmeta_snapshot_read_tail(&snapshot, 4, output, 9) == 0 &&
		  memcmp(output, tail + 4, 9) == 0,
		  "bounded reads cover both parts");
	check(ps_forkmeta_snapshot_read_tail(&snapshot, sizeof(tail), output, 1) != 0,
		  "bounded read rejects overrun");
	ps_forkmeta_snapshot_close(&snapshot);

	/* Generation allocation must account for every recognized final and
	 * part-temporary entry.  The test hook makes that traversal observable to
	 * the admission-fence test without changing core. */
	check(make_dir(root, "next_generation_scan", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 110, 2, &cp_input,
								  &stream_input) == 0,
		  "create generation-allocation scan fixture");
	{
		char entry_path[1200];
		GenerationScanTest scan = {.monotonic = 1};
		uint64_t next_generation = 0;

		check(part_path(directory, "forkmeta_checkpoint_v1_", 17, entry_path,
					 sizeof(entry_path)) == 0 &&
			  write_file(entry_path, "checkpoint", 10, 0) == 0 &&
			  part_path(directory, "forkmeta_tail_v1_", 23, entry_path,
					 sizeof(entry_path)) == 0 &&
			  write_file(entry_path, "tail", 4, 0) == 0 &&
			  snprintf(entry_path, sizeof(entry_path),
						   "%s/forkmeta_tail_v1_%020u.tmp.7.1", directory, 29u) >= 0 &&
			  write_file(entry_path, "temporary", 9, 0) == 0 &&
			  snprintf(entry_path, sizeof(entry_path), "%s/unrelated", directory) >= 0 &&
			  write_file(entry_path, "other", 5, 0) == 0,
			  "create recognized and unrelated generation entries");
		ps_test_set_forkmeta_snapshot_generation_scan_hook(
				generation_scan_hook, &scan);
		check(ps_forkmeta_snapshot_next_generation(directory, 1,
										   &next_generation) == 0 &&
			  next_generation == 30 && scan.calls == 6 &&
			  scan.recognized == 5 && scan.unrecognized == 1 &&
			  scan.maximum_generation == 29 && scan.last_highest == 29 &&
			  scan.monotonic,
			  "generation scan hook reports traversal and highest generation");
		ps_test_set_forkmeta_snapshot_generation_scan_hook(NULL, NULL);
	}

	check(make_dir(root, "stable_read", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 110, 2, &cp_input,
								  &stream_input) == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  part_path(directory, "forkmeta_checkpoint_v1_", 1, path,
					 sizeof(path)) == 0 && unlink(path) == 0 &&
		  write_file(path, "replacement!", sizeof(checkpoint) - 1, 0) == 0,
		  "replace checkpoint path after validated open");
	check(ps_forkmeta_snapshot_read_checkpoint(&snapshot, 0, output,
										 sizeof(checkpoint) - 1) == 0 &&
		  memcmp(output, checkpoint, sizeof(checkpoint) - 1) == 0,
		  "open snapshot keeps validated checkpoint identity");
	{
		PsForkmetaSnapshot replacement_open;

		check(ps_forkmeta_snapshot_open(&replacement_open, directory) != 0,
			  "new open rejects path replacement against selected manifest");
	}
	ps_forkmeta_snapshot_close(&snapshot);

	/* A mutation after the first scan must retry from a fresh directory stream,
	 * not a dup'd descriptor whose readdir offset is already at EOF. */
	check(make_dir(root, "observation_retry", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 120, 3, &cp_input,
								  &stream_input) == 0,
		  "create observation retry fixture");
	{
		ObservationRetryTest retry;
		PsForkmetaSnapshotExpected expected;
		uint64_t debt = 0;
		int reclaim_rc;

		memset(&retry, 0, sizeof(retry));
		check(path_suffix(directory,
					  "/forkmeta_tail_v1_00000000000000000002.tmp.1.1",
					  retry.debris, sizeof(retry.debris)) == 0 &&
			  ps_forkmeta_snapshot_open(&snapshot, directory) == 0,
			  "prepare observation retry paths");
		memset(&expected, 0, sizeof(expected));
		expected.generation = snapshot.generation;
		expected.cutoff_lsn = snapshot.cutoff_lsn;
		expected.cutoff_admission_seq = snapshot.cutoff_admission_seq;
		expected.checkpoint = snapshot.checkpoint;
		expected.tail = snapshot.tail;
		ps_forkmeta_snapshot_close(&snapshot);
		ps_test_set_forkmeta_snapshot_observation_hook(observation_retry_hook,
											 &retry);
		reclaim_rc = ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 0,
												 &expected, &debt);
		ps_test_set_forkmeta_snapshot_observation_hook(NULL, NULL);
		check(reclaim_rc == 0 && retry.calls == 1 && debt == 12,
			  "observation retry reopens directory and counts new finite debt");
		check(ps_forkmeta_snapshot_reclaim_bytes(directory, root, 1, 0,
												 &expected, &debt) != 0,
			  "missing source fails closed even when source debt is ineligible");
		check(unlink(retry.debris) == 0 && remove_tree(directory) == 0,
			  "remove observation retry fixture");
	}

	check(make_dir(root, "empty", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 101, 1, &empty_input,
								  &empty_input) == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  snapshot.checkpoint.len == 0 && snapshot.tail.len == 0 &&
		  ps_forkmeta_snapshot_read(&snapshot, PS_FORKMETA_SNAPSHOT_CHECKPOINT,
									 0, NULL, 0) == 0,
		  "empty checkpoint and tail are durable");
	ps_forkmeta_snapshot_close(&snapshot);

		/* A producer that emits more than its declared length is rejected. */
	streamed.extra = 1;
	check(make_dir(root, "mismatch", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) != 0,
		  "producer length mismatch is rejected");
	streamed.extra = 0;
	{
		FlakyProducer flaky = {tail, sizeof(tail) - 1, 0, 2};
		PsForkmetaSnapshotInput flaky_input = {
			.len = sizeof(tail) - 1,
			.produce = produce_flaky,
			.produce_arg = &flaky
		};
		char checkpoint_path[1200];
		char tail_path[1200];
		char intent_path[1200];

		check(make_dir(root, "write_fail", directory, sizeof(directory)) == 0 &&
			  part_path(directory, "forkmeta_checkpoint_v1_", 1,
						checkpoint_path, sizeof(checkpoint_path)) == 0 &&
			  part_path(directory, "forkmeta_tail_v1_", 1, tail_path,
						sizeof(tail_path)) == 0 &&
			  prepared_path(directory, intent_path, sizeof(intent_path)) == 0,
			  "create failed-write fixture");
		check(ps_forkmeta_snapshot_prepare(&prepared, directory, 1, 100, 1,
									 &cp_input, &flaky_input) != 0 &&
			  flaky.calls == 2 && access(checkpoint_path, F_OK) != 0 &&
			  access(tail_path, F_OK) != 0 && access(intent_path, F_OK) != 0,
			  "write-phase producer failure leaves no generation or intent debris");
	}
	if (UINT64_MAX > (uint64_t) SIZE_MAX)
	{
		PsForkmetaSnapshotInput too_large = {
			.data = checkpoint, .len = (uint64_t) SIZE_MAX + 1
		};

		check(ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &too_large,
									  &empty_input) != 0,
			  "direct input above SIZE_MAX is rejected");
	}
	if (sizeof(off_t) <= sizeof(uint64_t) && (off_t) -1 < 0)
	{
		unsigned int off_bits = (unsigned int) (sizeof(off_t) * CHAR_BIT);
		uint64_t off_max = off_bits == 64 ? UINT64_MAX >> 1 :
			(UINT64_C(1) << (off_bits - 1)) - 1;
		PsForkmetaSnapshotInput beyond_off_t = {
			.data = checkpoint, .len = off_max + 1
		};

		check(ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &beyond_off_t,
									  &empty_input) != 0,
			  "input end beyond off_t is rejected");
	}

	check(make_dir(root, "staged", directory, sizeof(directory)) == 0 &&
		  prepare_two(&prepared, directory, 2, 200, 8, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) != 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1,
		  "prepare records durable intent without selecting it");
	check(ps_forkmeta_snapshot_commit(&prepared) == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  snapshot.generation == 2 && snapshot.cutoff_lsn == 200,
		  "commit selects staged generation");
	ps_forkmeta_snapshot_close(&snapshot);
	check(ps_forkmeta_snapshot_commit(&prepared) == 0,
		  "identical commit retry succeeds after intent clear");
	{
		PsForkmetaSnapshotInput divergent = {
			.data = "DIFFERENT", .len = sizeof("DIFFERENT") - 1
		};

		check(ps_forkmeta_snapshot_prepare(&recovered, directory, 2, 200, 8,
									 &divergent, &stream_input) != 0,
			  "divergent same-generation retry is rejected");
	}
	check(ps_forkmeta_snapshot_publish(directory, 1, 300, 1, &cp_input,
								  &stream_input) != 0 &&
		  ps_forkmeta_snapshot_publish(directory, 3, 199, 9, &cp_input,
								  &stream_input) != 0,
		  "backward generation and cutoff are rejected");
	check(ps_forkmeta_snapshot_publish(directory, 3, 200, 9, &cp_input,
								  &stream_input) == 0,
		  "same-LSN higher admission sequence advances exact cutoff");

	check(make_dir(root, "intent_isolation", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  prepare_two(&prepared, directory, 3, 300, 3, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0,
		  "create unrelated durable prepare intent");
	{
		PsForkmetaSnapshotPrepared mismatched = prepared;
		char generation_two[1200];

		part_path(directory, "forkmeta_checkpoint_v1_", 2, generation_two,
				  sizeof(generation_two));
		check(prepare_two(&recovered, directory, 2, 200, 2, checkpoint,
						   sizeof(checkpoint) - 1, tail, sizeof(tail) - 1) != 0 &&
			  access(generation_two, F_OK) != 0 &&
			  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1 &&
			  recovered.generation == 3,
			  "failed prepare removes only its files and preserves unrelated intent");
		mismatched.generation = 2;
		check(ps_forkmeta_snapshot_abort(&mismatched) != 0 &&
			  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1 &&
			  recovered.generation == 3,
			  "abort rejects another durable prepared generation");
		mismatched = prepared;
		mismatched.tail.crc ^= 1;
		check(ps_forkmeta_snapshot_abort(&mismatched) != 0 &&
			  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1 &&
			  recovered.tail.crc == prepared.tail.crc,
			  "abort rejects mismatched prepared metadata");
		check(write_file(generation_two, "debris", 6, 0) == 0 &&
			  ps_forkmeta_snapshot_discard_generation(directory, 2) == 0 &&
			  access(generation_two, F_OK) != 0 &&
			  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1 &&
			  recovered.generation == 3,
			  "discard of another generation preserves durable intent");
	}
	check(ps_forkmeta_snapshot_abort(&prepared) == 0,
		  "controller aborts an uncovered durable intent");

	/* Frontier recovery below, exactly at, and above the prepared tuple. */
	check(make_dir(root, "recovery", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  prepare_two(&prepared, directory, 2, 200, 2, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0 &&
		  ps_forkmeta_snapshot_recover_prepared(directory, 199, 9) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0,
		  "recovery aborts below prepared cutoff");
	check(prepare_two(&prepared, directory, 2, 200, 2, replacement,
					 sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0 &&
		  ps_forkmeta_snapshot_recover_prepared(directory, 200, 2) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1,
		  "recovery retains at exact prepared cutoff");
	check(part_path(directory, "forkmeta_checkpoint_v1_", 2, path,
					 sizeof(path)) == 0 && ps_forkmeta_snapshot_gc(directory) == 0 &&
		  access(path, F_OK) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1,
		  "GC preserves durable prepared generation");
	check(ps_forkmeta_snapshot_commit(&prepared) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0,
		  "covered exact-frontier intent is committed, not aborted");
	check(make_dir(root, "recovery_above", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  prepare_two(&prepared, directory, 2, 200, 2, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0 &&
		  ps_forkmeta_snapshot_recover_prepared(directory, 201, 1) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1,
		  "recovery retains above prepared cutoff");
	check(ps_forkmeta_snapshot_commit(&prepared) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0,
		  "covered above-frontier intent is committed, not discarded");

	check(make_dir(root, "partial_abort_one", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  prepare_two(&prepared, directory, 2, 200, 2, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0 &&
		  part_path(directory, "forkmeta_checkpoint_v1_", 2, path,
					 sizeof(path)) == 0 && unlink(path) == 0 &&
		  sync_directory(directory) == 0,
		  "create one-file partial abort");
	check(ps_forkmeta_snapshot_recover_prepared(directory, 199, 9) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0 &&
		  part_path(directory, "forkmeta_tail_v1_", 2, path, sizeof(path)) == 0 &&
		  access(path, F_OK) != 0,
		  "recovery completes uncovered one-file partial abort");

	check(make_dir(root, "partial_abort_both", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  prepare_two(&prepared, directory, 2, 200, 2, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0,
		  "create two-file partial abort");
	{
		char checkpoint_two[1200];
		char tail_two[1200];
		uint64_t next_generation = 0;

		part_path(directory, "forkmeta_checkpoint_v1_", 2, checkpoint_two,
				  sizeof(checkpoint_two));
		part_path(directory, "forkmeta_tail_v1_", 2, tail_two,
				  sizeof(tail_two));
		check(unlink(checkpoint_two) == 0 && unlink(tail_two) == 0 &&
			  sync_directory(directory) == 0 &&
			  ps_forkmeta_snapshot_next_generation(directory, 1,
										  &next_generation) == 0 &&
			  next_generation == 3 &&
			  ps_forkmeta_snapshot_recover_prepared(directory, 199, 9) == 0 &&
			  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0,
			  "recovery completes uncovered two-file partial abort");
	}

	check(make_dir(root, "zero_frontier", directory, sizeof(directory)) == 0 &&
		  prepare_two(&prepared, directory, 1, 200, 2, replacement,
					   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0 &&
		  ps_forkmeta_snapshot_recover_prepared(directory, 0, 2) != 0 &&
		  ps_forkmeta_snapshot_recover_prepared(directory, 200, 0) != 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1,
		  "zero durable frontier is rejected without mutation");
	check(ps_forkmeta_snapshot_abort(&prepared) == 0,
		  "clean zero-frontier fixture");

	/* GC removes old files and recognized temporary debris, but not newer work. */
	check(make_dir(root, "gc", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
								  &stream_input) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 3, 300, 1, &cp_input,
								  &stream_input) == 0,
		  "publish selected and newer GC generations");
	check(part_path(directory, "forkmeta_checkpoint_v1_", 2, path,
					 sizeof(path)) == 0 && access(path, F_OK) == 0,
		  "old generation exists before GC");
	{
		char old_checkpoint[1200];
		char old_tail[1200];
		char temp[1200];
		char prepared_temp[1200];
		char newer_tail[1200];
		char unrelated[1200];
		char older_fixture[1200];
		char prepared_intent_path[1200];
		char fixture_intent_path[1200];
		struct stat old_checkpoint_stat;
		struct stat old_tail_stat;
		PsForkmetaSnapshotExpected expected;
		PsForkmetaSnapshotReclaimObservation observation;
		uint64_t debt = 0;
		uint64_t baseline;

		part_path(directory, "forkmeta_checkpoint_v1_", 2, old_checkpoint,
				  sizeof(old_checkpoint));
		part_path(directory, "forkmeta_tail_v1_", 2, old_tail,
				  sizeof(old_tail));
		snprintf(temp, sizeof(temp), "%s/forkmeta_tail_v1_00000000000000000003.tmp.1.1",
				 directory);
		part_path(directory, "forkmeta_checkpoint_v1_", 4, path,
				  sizeof(path));
		part_path(directory, "forkmeta_tail_v1_", 4, newer_tail,
				  sizeof(newer_tail));
		check(write_file(path, "newer", 5, 0) == 0,
			  "create newer checkpoint debris");
		check(write_file(newer_tail, "newer-tail", 10, 0) == 0,
			  "create newer tail debris");
		check(write_file(temp, "debris", 6, 0) == 0,
			  "create recognized temp debris");
		check(ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
			  snapshot.generation == 3,
			  "unselected debris is ignored by open");
		memset(&expected, 0, sizeof(expected));
		expected.generation = snapshot.generation;
		expected.cutoff_lsn = snapshot.cutoff_lsn;
		expected.cutoff_admission_seq = snapshot.cutoff_admission_seq;
		expected.checkpoint = snapshot.checkpoint;
		expected.tail = snapshot.tail;
		ps_forkmeta_snapshot_close(&snapshot);
		check(ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &debt) == 0 &&
				  debt >= 6,
				  "recognized temporary snapshot residue is counted as physical debt");
		check(ps_forkmeta_snapshot_reclaim_observation(directory, root, 0, 1,
												 &expected, &observation) == 0,
				  "safe cutoff observation succeeds with canonical residue");
		check(observation.gc_canonical_bytes == 45,
				  "safe cutoff observation classifies old canonical GC residue");
		check(observation.cutoff_dependent_bytes == 15,
				  "safe cutoff observation includes newer canonical residue");
		baseline = debt;
		snprintf(prepared_temp, sizeof(prepared_temp),
				 "%s/forkmeta_prepared_v1.tmp.1.1", directory);
		check(write_file(prepared_temp, "prepared", 8, 0) == 0 &&
			  ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &debt) == 0 &&
				  debt == baseline + 8,
				  "orphaned prepared intent temp is counted as GC debris");
		baseline = debt;
		snprintf(older_fixture, sizeof(older_fixture), "%s/prepared_older", root);
		check(stat(old_checkpoint, &old_checkpoint_stat) == 0 &&
			  stat(old_tail, &old_tail_stat) == 0 &&
			  mkdir(older_fixture, 0700) == 0 &&
			  prepared_path(directory, prepared_intent_path,
							 sizeof(prepared_intent_path)) == 0 &&
			  prepared_path(older_fixture, fixture_intent_path,
							 sizeof(fixture_intent_path)) == 0 &&
			  prepare_two(&prepared, older_fixture, 2, 200, 1, checkpoint,
						   sizeof(checkpoint) - 1, tail, sizeof(tail) - 1) == 0 &&
			  link(fixture_intent_path, prepared_intent_path) == 0,
				  "prepare an older generation alongside the selected snapshot");
		snprintf(unrelated, sizeof(unrelated),
				 "%s/forkmeta_tail_v1_00000000000000000009.tmp.1.1", directory);
		check(write_file(unrelated, "unrelated", 9, 0) == 0 &&
			  ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &debt) == 0 &&
			  debt == baseline - (uint64_t) old_checkpoint_stat.st_size -
				  (uint64_t) old_tail_stat.st_size + 9,
				  "older prepared intent and parts are excluded while obsolete debris counts");
		check(unlink(prepared_intent_path) == 0 && unlink(unrelated) == 0 &&
			  remove_tree(older_fixture) == 0,
				  "remove the older prepared generation fixture");
		check(ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &baseline) == 0,
				  "restore the debt baseline after older prepared fixture");
		check(prepare_two(&prepared, directory, 5, 500, 1, replacement,
						   sizeof(replacement) - 1, tail, sizeof(tail) - 1) == 0,
				  "prepare a newer generation alongside the selected snapshot");
		snprintf(unrelated, sizeof(unrelated),
				 "%s/forkmeta_tail_v1_00000000000000000010.tmp.1.1", directory);
		check(write_file(unrelated, "unrelated", 9, 0) == 0 &&
			  ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &debt) == 0 && debt == baseline + 9,
				  "newer prepared intent and parts are excluded while obsolete debris counts");
		check(ps_forkmeta_snapshot_abort(&prepared) == 0 && unlink(unrelated) == 0,
				  "remove the newer prepared generation fixture");
		check(ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &baseline) == 0,
				  "restore the debt baseline after newer prepared fixture");
		check(prepare_two(&prepared, directory, 3, 300, 1, checkpoint,
						   sizeof(checkpoint) - 1, tail, sizeof(tail) - 1) == 0,
				  "prepare a generation equal to the selected snapshot");
		snprintf(unrelated, sizeof(unrelated),
				 "%s/forkmeta_tail_v1_00000000000000000011.tmp.1.1", directory);
		check(write_file(unrelated, "unrelated", 9, 0) == 0 &&
			  ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &debt) == 0 && debt == baseline + 9,
				  "equal prepared generation is excluded while obsolete debris counts");
		check(ps_forkmeta_snapshot_commit(&prepared) == 0 && unlink(unrelated) == 0,
				  "commit the equal prepared generation fixture");
		check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR", "1", 1) == 0 &&
			  ps_forkmeta_snapshot_reclaim_bytes(directory, root, 0, 1,
										 &expected, &debt) != 0 &&
			  unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR") == 0,
				  "closedir failure does not double-close the consumed scan stream");
		check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR", "1", 1) == 0 &&
			  ps_forkmeta_snapshot_gc(directory) < 0 &&
			  unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR") == 0,
				  "GC closedir failure does not reuse the consumed stream");
		check(ps_forkmeta_snapshot_gc(directory) == 1 &&
				  access(old_checkpoint, F_OK) != 0 &&
				  access(temp, F_OK) != 0 && access(prepared_temp, F_OK) != 0 &&
				  access(newer_tail, F_OK) == 0,
			  "GC removes old generation and recognized temp debris");
	}
	check(part_path(directory, "forkmeta_checkpoint_v1_", 4, path,
					 sizeof(path)) == 0 && access(path, F_OK) == 0,
				  "GC preserves newer unpublished generation");

	/* Full GC has the same bounded cursor contract as temporary GC.  Keep a
	 * large, entirely reclaimable residue set so every batch makes progress
	 * without allocating a directory-sized name list. */
	check(make_dir(root, "gc_bounded", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1000, 100000, 1,
									   &cp_input, &stream_input) == 0,
			  "create bounded full-GC fixture");
	{
		char residue[1200];
		char selected[1200];
		GcInspectionTest inspection = {0};
		int gc_rc;
		int created = 1;

		for (uint64_t generation = 1; generation <= 300; generation++)
		{
			for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL;
				 part++)
			{
				const char *prefix = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
					"forkmeta_checkpoint_v1_" : "forkmeta_tail_v1_";

				if (part_path(directory, prefix, generation, residue,
							 sizeof(residue)) != 0 ||
					write_file(residue, "old", 3, 0) != 0)
				{
					created = 0;
					break;
				}
			}
			if (!created)
				break;
		}
		check(part_path(directory, "forkmeta_checkpoint_v1_", 1, residue,
					 sizeof(residue)) == 0 &&
			  part_path(directory, "forkmeta_checkpoint_v1_", 1000, selected,
					 sizeof(selected)) == 0,
			  "locate bounded full-GC entries");
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(
				gc_inspection_hook, &inspection);
		gc_rc = ps_forkmeta_snapshot_gc(directory);
		check(created && gc_rc ==
				  PS_FORKMETA_SNAPSHOT_GC_REMOVED_SCAN_INCOMPLETE &&
				  inspection.calls == PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				  "full GC reports removal with more bounded work");
		for (unsigned int pass = 0; pass < 64 && gc_rc > 0; pass++)
		{
			inspection.calls = 0;
			gc_rc = ps_forkmeta_snapshot_gc(directory);
			check(inspection.calls <= PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				  "full GC keeps every repeated batch bounded");
		}
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(NULL, NULL);
		check(gc_rc == PS_FORKMETA_SNAPSHOT_GC_NO_WORK &&
			  access(residue, F_OK) != 0 && access(selected, F_OK) == 0,
			  "repeated full-GC batches drain residue and preserve current");
	}

	/* A scan containing only newer canonical entries must retain its cursor and
	 * report SCAN_INCOMPLETE, rather than looking like a mutation or spinning
	 * through the complete directory in one maintenance tick. */
	check(make_dir(root, "gc_scan_incomplete", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0,
			  "create no-removal full-GC fixture");
	{
		char newer[1200];
		GcInspectionTest inspection = {0};
		int gc_rc;
		int created = 1;

		for (uint64_t generation = 2; generation <= 300; generation++)
		{
			for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL;
				 part++)
			{
				const char *prefix = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
					"forkmeta_checkpoint_v1_" : "forkmeta_tail_v1_";

				if (part_path(directory, prefix, generation, newer,
							 sizeof(newer)) != 0 ||
					write_file(newer, "new", 3, 0) != 0)
				{
					created = 0;
					break;
				}
			}
			if (!created)
				break;
		}
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(
				gc_inspection_hook, &inspection);
		gc_rc = ps_forkmeta_snapshot_gc(directory);
		check(created && gc_rc == PS_FORKMETA_SNAPSHOT_GC_SCAN_INCOMPLETE &&
				  inspection.calls == PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				  "full GC reports an incomplete no-removal batch");
		for (unsigned int pass = 0; pass < 64 && gc_rc ==
				 PS_FORKMETA_SNAPSHOT_GC_SCAN_INCOMPLETE; pass++)
		{
			inspection.calls = 0;
			gc_rc = ps_forkmeta_snapshot_gc(directory);
			check(inspection.calls <= PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				  "incomplete full-GC cursor remains bounded");
		}
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(NULL, NULL);
		check(gc_rc == PS_FORKMETA_SNAPSHOT_GC_NO_WORK,
			  "incomplete full-GC scan reaches EOF without mutation");
	}
	check(make_dir(root, "temp_only", directory, sizeof(directory)) == 0,
			  "create a temp-only directory without a selected manifest");
	{
		char temp_only[1200];
		char manifest[1200];
		PsForkmetaSnapshotExpected expected;
		PsForkmetaSnapshotReclaimObservation observation;
		GcInspectionTest inspection = {0};
		int created = 1;
		int gc_rc;
		int saw_removed_scan_incomplete = 0;
		int saw_removed_at_eof = 0;

		snprintf(temp_only, sizeof(temp_only),
				 "%s/forkmeta_tail_v1_00000000000000000001.tmp.1.1",
				 directory);
		snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1",
				 directory);
		check(write_file(temp_only, "temp", 4, 0) == 0 &&
				  access(manifest, F_OK) != 0 &&
				  ps_forkmeta_snapshot_gc_temporary(directory) == 1 &&
				  access(temp_only, F_OK) != 0,
				  "temp-only GC works before the first manifest publication");

		/* Overflow is a stable but incomplete observation.  It must remain
		 * fail-closed to admission while still exposing validated temporary
		 * cleanup work.  Cleanup is deliberately bounded and can be repeated. */
		for (unsigned int i = 0; i < 4097; i++)
		{
			int n = snprintf(temp_only, sizeof(temp_only),
						 "%s/forkmeta_tail_v1_%020u.tmp.1.1", directory,
						 i + 1);

			if (n < 0 || (size_t) n >= sizeof(temp_only) ||
				write_file(temp_only, "x", 1, 0) != 0)
			{
				created = 0;
				break;
			}
		}
		memset(&expected, 0, sizeof(expected));
		check(created &&
				  ps_forkmeta_snapshot_reclaim_observation(directory, root, 0, 0,
															 &expected, &observation) ==
				  PS_FORKMETA_SNAPSHOT_RECLAIM_OBSERVATION_OVERFLOW &&
				  observation.gc_serviceable_overflow != 0 &&
				  observation.gc_temp_gc_due != 0 &&
				  observation.gc_serviceable_bytes != 0,
				  "temporary overflow is incomplete but remains executable GC work");
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(
				gc_inspection_hook, &inspection);
		gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
		check(gc_rc == PS_FORKMETA_SNAPSHOT_GC_REMOVED_SCAN_INCOMPLETE &&
				  inspection.calls == PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				  "temporary GC reports removal with more bounded work");
		for (unsigned int pass = 0; pass < 64 && gc_rc > 0; pass++)
		{
			inspection.calls = 0;
			gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
			if (gc_rc == PS_FORKMETA_SNAPSHOT_GC_REMOVED_SCAN_INCOMPLETE)
				saw_removed_scan_incomplete = 1;
			if (gc_rc == PS_FORKMETA_SNAPSHOT_GC_REMOVED)
				saw_removed_at_eof = 1;
			check(inspection.calls <= PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				  "temporary GC keeps repeated removal batches bounded");
		}
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(NULL, NULL);
		check(gc_rc == PS_FORKMETA_SNAPSHOT_GC_NO_WORK &&
			  saw_removed_scan_incomplete && saw_removed_at_eof,
			  "temporary GC distinguishes suffix work through complete drain");
		{
			char unknown[1200];
			int observation_rc;

			/* Selected parts and an unknown entry must not be silently skipped
			 * when the cursor crosses the bound.  The result is incomplete and
			 * therefore cannot authorize admission; after temp cleanup the
			 * complete scan must expose the unknown entry. */
			for (unsigned int i = 0;
				 i < PS_FORKMETA_SNAPSHOT_RECLAIM_MAX_ENTRIES; i++)
			{
				int n = snprintf(temp_only, sizeof(temp_only),
							 "%s/forkmeta_tail_v1_%020u.tmp.2.1", directory,
							 i + 10000);

				if (n < 0 || (size_t) n >= sizeof(temp_only) ||
					write_file(temp_only, "x", 1, 0) != 0)
				{
					created = 0;
					break;
				}
			}
			snprintf(unknown, sizeof(unknown), "%s/not-a-forkmeta-entry", directory);
			check(created && write_file(unknown, "unknown", 7, 0) == 0 &&
					  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
													  &stream_input) == 0 &&
					  ps_forkmeta_snapshot_open(&snapshot, directory) == 0,
					  "create selected parts beyond an overflow cursor");
			if (access(unknown, F_OK) == 0 && snapshot.generation != 0)
			{
				expected.generation = snapshot.generation;
				expected.cutoff_lsn = snapshot.cutoff_lsn;
				expected.cutoff_admission_seq = snapshot.cutoff_admission_seq;
				expected.checkpoint = snapshot.checkpoint;
				expected.tail = snapshot.tail;
				ps_forkmeta_snapshot_close(&snapshot);
				observation_rc = ps_forkmeta_snapshot_reclaim_observation(
					directory, root, 0, 0, &expected, &observation);
				check(observation_rc != 0 &&
						(observation_rc !=
						 PS_FORKMETA_SNAPSHOT_RECLAIM_OBSERVATION_OVERFLOW ||
						 observation.gc_temp_gc_due != 0),
						  "overflow does not claim complete selected/unknown accounting");
				for (unsigned int pass = 0; pass < 64; pass++)
					if (ps_forkmeta_snapshot_gc_temporary(directory) == 0)
						break;
				check(ps_forkmeta_snapshot_reclaim_observation(
						 directory, root, 0, 0, &expected, &observation) != 0,
						"complete scan exposes unknown entry after bounded cleanup");
			}
		}
	}
	{
		char overflow_symlink[1200];
		char temp_only[1200];
		PsForkmetaSnapshotExpected expected;
		PsForkmetaSnapshotReclaimObservation observation;
		int created = 1;

		check(make_dir(root, "overflow_symlink", directory,
					   sizeof(directory)) == 0,
				  "create overflow symlink fixture");
		for (unsigned int i = 0;
			 i < PS_FORKMETA_SNAPSHOT_RECLAIM_MAX_ENTRIES; i++)
		{
			int n = snprintf(temp_only, sizeof(temp_only),
						 "%s/forkmeta_tail_v1_%020u.tmp.3.1", directory,
						 i + 20000);

			if (n < 0 || (size_t) n >= sizeof(temp_only) ||
				write_file(temp_only, "x", 1, 0) != 0)
			{
				created = 0;
				break;
			}
		}
		snprintf(overflow_symlink, sizeof(overflow_symlink),
				 "%s/forkmeta_tail_v1_99999999999999999999.tmp.3.1", directory);
		memset(&expected, 0, sizeof(expected));
		check(created && symlink("/dev/null", overflow_symlink) == 0 &&
				  ps_forkmeta_snapshot_reclaim_observation(directory, root, 0, 0,
														 &expected, &observation) != 0 &&
				  access(overflow_symlink, F_OK) == 0,
				  "overflow never treats an exact temporary symlink as validated cleanup");
		{
			int gc_rc = 1;

			for (unsigned int pass = 0; pass < 64 && gc_rc > 0; pass++)
				gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
			check(gc_rc < 0 && access(overflow_symlink, F_OK) == 0,
				  "temporary GC eventually preserves the overflow symlink");
		}
	}
	{
		char canonical[1200];
		char temp_later[1200];
		PsForkmetaSnapshotExpected expected;
		PsForkmetaSnapshotReclaimObservation observation;
		GcInspectionTest inspection = {0};
		int created = 1;

		check(make_dir(root, "overflow_canonical_cursor", directory,
					   sizeof(directory)) == 0,
				  "create canonical overflow cursor fixture");
		for (unsigned int i = 0; i < 2048; i++)
		{
			for (unsigned int part = 0; part < 2; part++)
			{
				const char *prefix = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
					"forkmeta_checkpoint_v1_" : "forkmeta_tail_v1_";
				int n = snprintf(canonical, sizeof(canonical), "%s/%s%020u",
							 directory, prefix, i + 30000);

				if (n < 0 || (size_t) n >= sizeof(canonical) ||
					write_file(canonical, "x", 1, 0) != 0)
				{
					created = 0;
					break;
				}
			}
			if (!created)
				break;
		}
		check(created &&
				  snprintf(canonical, sizeof(canonical),
						   "%s/forkmeta_checkpoint_v1_00000000000000009999",
						   directory) >= 0 && write_file(canonical, "x", 1, 0) == 0 &&
				  snprintf(temp_later, sizeof(temp_later),
						   "%s/forkmeta_tail_v1_00000000000000009998.tmp.4.1",
						   directory) >= 0 && write_file(temp_later, "later", 5, 0) == 0,
				  "create temp after the canonical overflow cursor");
		memset(&expected, 0, sizeof(expected));
		check(created &&
			  ps_forkmeta_snapshot_reclaim_observation(directory, root, 0, 0,
														 &expected, &observation) ==
			  PS_FORKMETA_SNAPSHOT_RECLAIM_OBSERVATION_OVERFLOW &&
			  observation.gc_temp_gc_due != 0,
			  "canonical overflow schedules an external temporary GC probe");
		{
			int gc_rc = 0;
			unsigned int calls = 0;

			while (access(temp_later, F_OK) == 0 && calls < 64)
			{
				inspection.calls = 0;
				ps_test_set_forkmeta_snapshot_gc_inspection_hook(
						gc_inspection_hook, &inspection);
				gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
				ps_test_set_forkmeta_snapshot_gc_inspection_hook(NULL, NULL);
				check(inspection.calls <= PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
						"temporary GC bounds each cursor inspection batch");
				calls++;
				if (gc_rc < 0)
					break;
			}
			check(access(temp_later, F_OK) != 0 && gc_rc > 0,
					"external temporary GC traverses canonical prefix to valid temp");
		}
		check(ps_forkmeta_snapshot_reclaim_observation(directory, root, 0, 0,
														 &expected, &observation) != 0,
				  "canonical overflow remains fail-closed until below the bound");
	}
	{
		char canonical[1200];
		char temp[1200];
		GcInspectionTest inspection = {0};
		int created = 1;
		int gc_rc = PS_FORKMETA_SNAPSHOT_GC_SCAN_INCOMPLETE;
		unsigned int calls = 0;

		check(make_dir(root, "cursor_no_temp", directory,
					   sizeof(directory)) == 0,
				  "create no-temp cursor fixture");
		for (unsigned int i = 0; i < 300; i++)
		{
			int n = snprintf(canonical, sizeof(canonical),
						 "%s/forkmeta_checkpoint_v1_%020u", directory,
						 i + 50000);

			if (n < 0 || (size_t) n >= sizeof(canonical) ||
				write_file(canonical, "x", 1, 0) != 0)
			{
				created = 0;
				break;
			}
		}
		while (created && gc_rc == PS_FORKMETA_SNAPSHOT_GC_SCAN_INCOMPLETE &&
			   calls < 8)
		{
			inspection.calls = 0;
			ps_test_set_forkmeta_snapshot_gc_inspection_hook(
					gc_inspection_hook, &inspection);
			gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
			ps_test_set_forkmeta_snapshot_gc_inspection_hook(NULL, NULL);
			check(inspection.calls <= PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
					"no-temp traversal remains bounded per call");
			calls++;
		}
		check(created && gc_rc == PS_FORKMETA_SNAPSHOT_GC_NO_WORK && calls >= 3,
				"no-temp cursor reaches EOF and resets");
		check(snprintf(temp, sizeof(temp),
					   "%s/forkmeta_tail_v1_00000000000000000001.tmp.5.1",
					   directory) >= 0 && write_file(temp, "x", 1, 0) == 0,
				  "add temp after no-temp cursor EOF");
		gc_rc = PS_FORKMETA_SNAPSHOT_GC_NO_WORK;
		for (calls = 0; access(temp, F_OK) == 0 && calls < 8; calls++)
			gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
		check(access(temp, F_OK) != 0 &&
			  (gc_rc == PS_FORKMETA_SNAPSHOT_GC_REMOVED ||
			   gc_rc == PS_FORKMETA_SNAPSHOT_GC_REMOVED_SCAN_INCOMPLETE),
			  "EOF reset makes later temp reachable");
		while (gc_rc > 0 && calls < 16)
		{
			gc_rc = ps_forkmeta_snapshot_gc_temporary(directory);
			calls++;
		}
		check(gc_rc == PS_FORKMETA_SNAPSHOT_GC_NO_WORK,
			  "later temporary deletion completes its cursor drain");
	}
	{
		char replacement[1200];
		char old_directory[1200];
		char canonical[1200];
		char temp[1200];
		GcInspectionTest inspection = {0};
		int created = 1;
		int gc_rc;

		check(make_dir(root, "cursor_replacement", replacement,
					   sizeof(replacement)) == 0,
				  "create cursor identity fixture");
		for (unsigned int i = 0; i < 256; i++)
		{
			int n = snprintf(canonical, sizeof(canonical),
						 "%s/forkmeta_checkpoint_v1_%020u", replacement,
						 i + 60000);

			if (n < 0 || (size_t) n >= sizeof(canonical) ||
				write_file(canonical, "x", 1, 0) != 0)
			{
				created = 0;
				break;
			}
		}
		inspection.calls = 0;
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(
				gc_inspection_hook, &inspection);
		gc_rc = ps_forkmeta_snapshot_gc_temporary(replacement);
		ps_test_set_forkmeta_snapshot_gc_inspection_hook(NULL, NULL);
		check(created && gc_rc == PS_FORKMETA_SNAPSHOT_GC_SCAN_INCOMPLETE &&
				inspection.calls == PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH,
				"cursor identity fixture advances one bounded prefix");
		check(snprintf(old_directory, sizeof(old_directory), "%s.old", replacement) >= 0 &&
				rename(replacement, old_directory) == 0 &&
				mkdir(replacement, 0700) == 0 &&
				snprintf(temp, sizeof(temp),
						 "%s/forkmeta_tail_v1_00000000000000000002.tmp.6.1",
						 replacement) >= 0 && write_file(temp, "x", 1, 0) == 0,
				"replace cursor path with a new directory identity");
		gc_rc = ps_forkmeta_snapshot_gc_temporary(replacement);
		check(gc_rc == PS_FORKMETA_SNAPSHOT_GC_REMOVED && access(temp, F_OK) != 0,
				"directory identity change resets the cursor");
		check(remove_tree(old_directory) == 0,
				"remove old cursor identity fixture");
	}
	{
		char temp[1200];
		char near_final[1200];

		snprintf(temp, sizeof(temp),
				 "%s/forkmeta_tail_v1_00000000000000000004.tmp.1.2",
				 directory);
		check(symlink(path, temp) == 0 &&
			  ps_forkmeta_snapshot_gc(directory) < 0 && access(temp, F_OK) == 0,
			  "GC rejects an exact snapshot temporary symlink without unlinking it");
		(void) unlink(temp);
		snprintf(temp, sizeof(temp),
				 "%s/forkmeta_tail_v1_00000000000000000004.tmp.1.128",
				 directory);
		check(write_file(temp, "bad", 3, 0) == 0 &&
			  ps_forkmeta_snapshot_gc(directory) < 0 && access(temp, F_OK) == 0,
			  "GC fails closed on a near-miss temporary name");
		(void) unlink(temp);
		snprintf(near_final, sizeof(near_final), "%s/forkmeta_tail_v1_1",
				 directory);
		check(write_file(near_final, "bad-final", 9, 0) == 0 &&
			  ps_forkmeta_snapshot_gc(directory) < 0 &&
			  access(near_final, F_OK) == 0,
			  "GC fails closed on a non-canonical final generation name");
		(void) unlink(near_final);
	}

	check(make_dir(root, "gc_fsync_retry", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
								  &stream_input) == 0 &&
		  part_path(directory, "forkmeta_checkpoint_v1_", 1, path,
					 sizeof(path)) == 0,
		  "create GC fsync retry fixture");
	check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC", "1", 1) == 0 &&
		  ps_forkmeta_snapshot_gc(directory) ==
		  PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS &&
		  access(path, F_OK) != 0,
		  "GC fsync fault leaves an empty unlink retry");
	check(unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") == 0 &&
		  ps_forkmeta_snapshot_gc(directory) == 0,
		  "empty GC retry fsyncs successfully");

	/* Prepare failure must preserve identical final parts it did not create. */
	check(make_dir(root, "prepared_fault", directory, sizeof(directory)) == 0,
		  "create prepared-intent fault fixture");
	{
		char checkpoint_two[1200];
		char tail_two[1200];
		uint64_t next_generation = 0;

		check(part_path(directory, "forkmeta_checkpoint_v1_", 2, checkpoint_two,
						 sizeof(checkpoint_two)) == 0 &&
			  part_path(directory, "forkmeta_tail_v1_", 2, tail_two,
						 sizeof(tail_two)) == 0 &&
			  write_file(checkpoint_two, checkpoint, sizeof(checkpoint) - 1, 0) == 0 &&
			  write_file(tail_two, tail, sizeof(tail) - 1, 0) == 0 &&
			  sync_directory(directory) == 0,
			  "create pre-existing identical generation parts");
		check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_PREPARED_BEFORE_CREATE", "1", 1) == 0 &&
			  ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
									  &stream_input) != 0 &&
			  unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_PREPARED_BEFORE_CREATE") == 0 &&
			  access(checkpoint_two, F_OK) == 0 && access(tail_two, F_OK) == 0 &&
			  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0 &&
			  ps_forkmeta_snapshot_next_generation(directory, 0,
										&next_generation) == 0 &&
			  next_generation == 3,
			  "intent fault preserves prior parts and generation allocation");
	}

	check(make_dir(root, "manifest_before", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  setenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_BEFORE_RENAME", "1", 1) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
								  &stream_input) != 0 &&
		  unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_BEFORE_RENAME") == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  snapshot.generation == 1,
		  "manifest fault before rename keeps old selection");
	ps_forkmeta_snapshot_close(&snapshot);
	check(ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 1 &&
		  recovered.generation == 2,
		  "commit failure preserves durable prepared retry");
	check(ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
								  &stream_input) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  snapshot.generation == 2,
		  "before-rename commit failure retries from durable intent and parts");
	ps_forkmeta_snapshot_close(&snapshot);

	check(make_dir(root, "manifest_after", directory, sizeof(directory)) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
								  &stream_input) == 0 &&
		  setenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_AFTER_RENAME", "1", 1) == 0 &&
		  ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
								  &stream_input) != 0 &&
		  unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_AFTER_RENAME") == 0 &&
		  ps_forkmeta_snapshot_open(&snapshot, directory) == 0 &&
		  snapshot.generation == 2 &&
		  ps_forkmeta_snapshot_read_checkpoint(&snapshot, 0, output,
										 sizeof(checkpoint) - 1) == 0 &&
		  memcmp(output, checkpoint, sizeof(checkpoint) - 1) == 0,
		  "after-rename ambiguity keeps complete selected files");
	ps_forkmeta_snapshot_close(&snapshot);
	check(ps_forkmeta_snapshot_publish(directory, 2, 200, 1, &cp_input,
								  &stream_input) == 0 &&
		  ps_forkmeta_snapshot_read_prepared(directory, &recovered) == 0,
		  "after-rename ambiguity retries idempotently");

	/* Selected manifest and part corruption, truncation, extension, and symlink. */
	for (unsigned int case_no = 0; case_no < 5; case_no++)
	{
		char name[32];
		char cp_path[1200];
		char manifest[1200];

		snprintf(name, sizeof(name), "bad%u", case_no);
		check(make_dir(root, name, directory, sizeof(directory)) == 0 &&
			  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
									  &stream_input) == 0,
			  "create corruption fixture");
		check(part_path(directory, "forkmeta_checkpoint_v1_", 1, cp_path,
						 sizeof(cp_path)) == 0 &&
			  manifest_path(directory, manifest, sizeof(manifest)) == 0,
			  "locate corruption fixture");
		if (case_no == 0)
			check(write_file(cp_path, "X", 1, 0) == 0,
				  "corrupt generation bytes");
		else if (case_no == 1)
			check(truncate(cp_path, 1) == 0, "short generation file");
		else if (case_no == 2)
			check(write_file(cp_path, "X", 1, 1) == 0, "extended generation file");
		else if (case_no == 3)
			check(truncate(manifest, 1) == 0, "short manifest");
		else
			check(write_file(manifest, "X", 1, 1) == 0, "extended manifest");
		check(ps_forkmeta_snapshot_open(&snapshot, directory) != 0 &&
			  ps_forkmeta_snapshot_gc(directory) != 0,
			  "corrupt or wrong-size selected state fails closed");
	}
	check(make_dir(root, "symlink", directory, sizeof(directory)) == 0,
		  "create symlink fixture");
	{
		char symlink_path[1200];

		part_path(directory, "forkmeta_checkpoint_v1_", 1, symlink_path,
				  sizeof(symlink_path));
		check(symlink("/dev/null", symlink_path) == 0 &&
			  ps_forkmeta_snapshot_publish(directory, 1, 100, 1, &cp_input,
									   &stream_input) != 0,
			  "publication rejects symlink final part");
	}
	check(remove_tree(root) == 0, "remove snapshot test tree");

	if (failures != 0)
		fprintf(stderr, "%d/%d checks failed\n", failures, runs);
	else
		printf("ok - %d checks\n", runs);
	return failures == 0 ? 0 : 1;
}
