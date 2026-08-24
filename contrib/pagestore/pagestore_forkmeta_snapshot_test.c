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
		char temp[1200];
		char newer_tail[1200];

		part_path(directory, "forkmeta_checkpoint_v1_", 2, old_checkpoint,
				  sizeof(old_checkpoint));
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
		ps_forkmeta_snapshot_close(&snapshot);
		check(ps_forkmeta_snapshot_gc(directory) == 1 &&
			  access(old_checkpoint, F_OK) != 0 &&
			  access(temp, F_OK) != 0 && access(newer_tail, F_OK) == 0,
			  "GC removes old generation and recognized temp debris");
	}
	check(part_path(directory, "forkmeta_checkpoint_v1_", 4, path,
					 sizeof(path)) == 0 && access(path, F_OK) == 0,
		  "GC preserves newer unpublished generation");

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
