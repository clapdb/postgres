#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "pagestore_walidx_snapshot.h"

static int run;
static int failed;

typedef struct ProduceCtx
{
	const unsigned char *data;
	size_t len;
} ProduceCtx;

static int
produce_fragmented(void *arg, PsWalIdxSnapshotConsume consume,
				   void *consume_arg)
{
	ProduceCtx *ctx = arg;

	for (size_t off = 0; off < ctx->len;)
	{
		size_t amount = ctx->len - off < 17 ? ctx->len - off : 17;

		if (consume(consume_arg, ctx->data + off, amount) != 0)
			return -1;
		off += amount;
	}
	return 0;
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
	char recovery_directory[1024];
	char reshard_directory[1024];
	char unselected_directory[1024];
	char path[1200];
	unsigned char shard0[257];
	unsigned char shard1[513];
	unsigned char newer0[257];
	unsigned char out[64];
	PsWalIdxSnapshotInput first[3];
	PsWalIdxSnapshotInput second[3];
	PsWalIdxSnapshot snapshot;
	PsWalIdxSnapshot wrong;
	ProduceCtx streamed;
	PsWalIdxSnapshotPrepared prepared;
	uint64_t prepared_obsolete = 0;

	check(mkdtemp(root) != NULL, "create snapshot test root");
	for (size_t i = 0; i < sizeof(shard0); i++)
		shard0[i] = (unsigned char) (i * 3u + 1u);
	for (size_t i = 0; i < sizeof(shard1); i++)
		shard1[i] = (unsigned char) (i * 5u + 2u);
	memcpy(newer0, shard0, sizeof(newer0));
	newer0[100] ^= 0x5a;
	first[0] = (PsWalIdxSnapshotInput) {.data = shard0, .len = sizeof(shard0)};
	streamed = (ProduceCtx) {shard1, sizeof(shard1)};
	first[1] = (PsWalIdxSnapshotInput) {
		.len = sizeof(shard1),
		.produce = produce_fragmented,
		.produce_arg = &streamed
	};
	first[2] = (PsWalIdxSnapshotInput) {.data = NULL, .len = 0};
	memcpy(second, first, sizeof(second));
	second[0].data = newer0;
	snprintf(directory, sizeof(directory), "%s/timeline_0", root);
	snprintf(recovery_directory, sizeof(recovery_directory), "%s/timeline_1", root);
	snprintf(reshard_directory, sizeof(reshard_directory), "%s/timeline_2", root);
	snprintf(unselected_directory, sizeof(unselected_directory),
			 "%s/timeline_unselected", root);
	{
		uint64_t obsolete = UINT64_MAX;

		check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 0, &obsolete) == 0 &&
			  obsolete == 0,
			  "an absent never-selected snapshot directory has no reclaim debt");
		check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 1, &obsolete) != 0,
				"an absent selected snapshot directory fails closed");
	}
	{
		char manifest_temp[1200];
		char prepared_temp[1200];
		uint64_t obsolete = 0;
		int manifest_fd;
		int prepared_fd;

		check(mkdir(unselected_directory, 0700) == 0,
				"create an empty unselected snapshot directory");
		snprintf(manifest_temp, sizeof(manifest_temp),
				 "%s/walidx_manifest_v1.tmp.%020llu.%ld.%u",
				 unselected_directory, 7ULL, (long) getpid(), 0U);
		snprintf(prepared_temp, sizeof(prepared_temp),
				 "%s/walidx_prepared_v1.tmp.%ld.%u",
				 unselected_directory, (long) getpid(), 1U);
		manifest_fd = open(manifest_temp, O_CREAT | O_EXCL | O_WRONLY, 0600);
		prepared_fd = open(prepared_temp, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(manifest_fd >= 0 && prepared_fd >= 0 &&
				write(manifest_fd, "manifest", 8) == 8 &&
				write(prepared_fd, "prepared", 8) == 8 &&
				close(manifest_fd) == 0 && close(prepared_fd) == 0,
				"create exact residue without a selected snapshot");
		check(ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
										 &obsolete) == 0 && obsolete == 16,
				"unselected snapshot residue is counted as debt");
		check(ps_walidx_snapshot_gc(unselected_directory, 0) == 1 &&
				access(manifest_temp, F_OK) != 0 &&
				access(prepared_temp, F_OK) != 0 &&
				ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
										 &obsolete) == 0 && obsolete == 0,
				"unselected snapshot GC removes residue and clears debt");
		{
			char orphan_shard[1200];
			char prepared_path[1200];
			char extra_shard[1200];
			int fd;

			snprintf(orphan_shard, sizeof(orphan_shard),
					 "%s/walidxg1_%020llu_%03u", unselected_directory,
					 1ULL, 0U);
			fd = open(orphan_shard, O_CREAT | O_EXCL | O_WRONLY, 0600);
			check(fd >= 0 && write(fd, "orphan", 6) == 6 && close(fd) == 0 &&
				  ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
											 &obsolete) == 0 && obsolete == 6,
				  "an orphan canonical shard without a manifest is physical debt");
			check(ps_walidx_snapshot_gc(unselected_directory, 0) == 1 &&
				  access(orphan_shard, F_OK) != 0 &&
				  ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
											 &obsolete) == 0 && obsolete == 0,
				  "no-manifest GC removes an orphan canonical shard");

			snprintf(prepared_path, sizeof(prepared_path), "%s/%s",
					 unselected_directory, "walidx_prepared_v1");
			check(ps_walidx_snapshot_prepare(&prepared, unselected_directory, 0,
									 2, 500, 900, first, 3) == 0 &&
				  ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
											 &obsolete) == 0 &&
				  obsolete == 112 + sizeof(shard0) + sizeof(shard1),
				  "a complete no-manifest prepared generation is counted");
			check(ps_walidx_snapshot_gc(unselected_directory, 0) == 0 &&
				  access(prepared_path, F_OK) == 0 &&
				  ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
											 &obsolete) == 0 && obsolete != 0,
				  "no-manifest GC preserves a complete prepared generation");
			snprintf(extra_shard, sizeof(extra_shard),
					 "%s/walidxg1_%020llu_%03u", unselected_directory,
					 2ULL, 3U);
			fd = open(extra_shard, O_CREAT | O_EXCL | O_WRONLY, 0600);
			check(fd >= 0 && close(fd) == 0 &&
				  ps_walidx_snapshot_reclaim_bytes(unselected_directory, 0, 0,
											 &obsolete) != 0 &&
				  ps_walidx_snapshot_gc(unselected_directory, 0) != 0 &&
				  access(extra_shard, F_OK) == 0,
				  "an extra shard in a prepared no-manifest generation fails closed");
			check(unlink(extra_shard) == 0 &&
				  ps_walidx_snapshot_abort(&prepared) == 0,
				  "abort the preserved no-manifest prepared generation");
		}
	}

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
		check(ps_walidx_snapshot_discard_generation(directory, 0, 2, 3) == 0 &&
			  ps_walidx_snapshot_next_generation(directory, 1,
											  &next_generation) == 0 &&
			  next_generation == 2,
			  "failed publication cleanup bounds immutable debris");
	}
	check(ps_walidx_snapshot_open(&snapshot, directory, 0) == 0 &&
		  snapshot.generation == 1,
		  "recovery ignores an incomplete unpublished generation");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_prepare(&prepared, directory, 0, 2, 500, 900,
								 second, 3) == 0 &&
		  ps_walidx_snapshot_open(&snapshot, directory, 0) == 0 &&
		  snapshot.generation == 1,
		  "prepare makes every replacement shard durable without selecting it");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_commit(&prepared) == 0 &&
		  ps_walidx_snapshot_open(&snapshot, directory, 0) == 0 &&
		  snapshot.generation == 2 && snapshot.start_lsn == 500 &&
		  snapshot.end_lsn == 900,
		  "commit atomically selects the prepared generation");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_commit(&prepared) == 0,
		  "an identical staged commit retry is idempotent");
	check(ps_walidx_snapshot_prepare(&prepared, directory, 0, 3, 900, 1000,
								 second, 3) == 0,
		  "prepare another complete unpublished generation");
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 directory, 3ULL, 0U);
	check(pwrite_byte(path, 17, newer0[17] ^ 0xff) == 0 &&
		  ps_walidx_snapshot_commit(&prepared) != 0,
		  "commit validates prepared shard durability before cutover");
	check(pwrite_byte(path, 17, newer0[17]) == 0,
		  "restore the unpublished generation after validation coverage");
	check(ps_walidx_snapshot_abort(&prepared) == 0 &&
		  access(path, F_OK) != 0 && errno == ENOENT &&
		  ps_walidx_snapshot_commit(&prepared) != 0,
		  "abort removes a prepared generation before input can change");
	check(ps_walidx_snapshot_publish(directory, 0, 1, 100, 500,
								 first, 3) != 0,
		  "publication cannot move the durable generation backward");
	check(ps_walidx_snapshot_publish(directory, 0, 3, 400, 1000,
								 second, 3) != 0,
		  "a newer generation cannot move its retained frontier backward");
	{
		uint64_t baseline_obsolete = 0;
		uint64_t orphan_obsolete = 0;

		check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 2,
										  &baseline_obsolete) == 0,
			  "observe snapshot debt before an orphan newer generation");
		check(setenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD", "0", 1) == 0 &&
		  ps_walidx_snapshot_publish(directory, 0, 3, 900, 1000,
								 second, 3) != 0,
			  "leave an orphan newer shard after interrupted publication");
		unsetenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD");

		check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 2,
										  &orphan_obsolete) == 0 &&
			  orphan_obsolete == baseline_obsolete + sizeof(newer0),
			  "physical observation counts an orphan newer generation");
	}
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u.tmp.%u.%u",
			 directory, 4ULL, 0U, 123U, 0U);
	{
		int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);

		check(fd >= 0 && close(fd) == 0,
			  "create crash-left snapshot temporary file");
	}
	check(setenv("PAGESTORE_TEST_FAIL_WALIDX_GC_FSYNC", "1", 1) == 0 &&
		  ps_walidx_snapshot_gc(directory, 0) != 0,
		  "generation GC reports a directory sync failure after unlinking");
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 directory, 1ULL, 0U);
	check(access(path, F_OK) != 0 && errno == ENOENT,
		  "generation GC removes the old selected generation");
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 directory, 2ULL, 0U);
	check(access(path, F_OK) == 0,
		  "generation GC preserves the currently selected generation");
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 directory, 3ULL, 0U);
	check(access(path, F_OK) != 0 && errno == ENOENT,
		  "generation GC removes an orphan newer retry state");
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u.tmp.%u.%u",
			 directory, 4ULL, 0U, 123U, 0U);
	check(access(path, F_OK) != 0 && errno == ENOENT,
		  "generation GC removes crash-left snapshot temporaries");
	check(ps_walidx_snapshot_gc(directory, 0) != 0,
		  "generation GC retry still syncs after prior unlinks disappeared");
	unsetenv("PAGESTORE_TEST_FAIL_WALIDX_GC_FSYNC");
	check(ps_walidx_snapshot_gc(directory, 0) == 0,
		  "generation GC durably completes an ambiguous sync retry");
	{
		char prepared_path[1200];
		char prepared_shard[1200];

		snprintf(prepared_path, sizeof(prepared_path), "%s/walidx_prepared_v1",
				 directory);
		snprintf(prepared_shard, sizeof(prepared_shard),
				 "%s/walidxg1_%020llu_%03u", directory, 4ULL, 0U);
		check(ps_walidx_snapshot_prepare(&prepared, directory, 0, 4,
									 900, 1000, second, 3) == 0 &&
			  ps_walidx_snapshot_gc(directory, 0) == 0 &&
			  access(prepared_path, F_OK) == 0 &&
			  access(prepared_shard, F_OK) == 0,
			  "generation GC preserves a valid newer prepared generation");
		check(ps_walidx_snapshot_abort(&prepared) == 0,
			  "abort the preserved newer prepared generation");
	}
	{
		char prepared_path[1200];
		char selected_shard[1200];

		snprintf(prepared_path, sizeof(prepared_path), "%s/walidx_prepared_v1",
				 directory);
		snprintf(selected_shard, sizeof(selected_shard),
				 "%s/walidxg1_%020llu_%03u", directory, 2ULL, 0U);
		check(ps_walidx_snapshot_prepare(&prepared, directory, 0, 2,
									 500, 900, second, 3) == 0 &&
				  ps_walidx_snapshot_gc(directory, 0) == 1 &&
				  access(prepared_path, F_OK) != 0 &&
				  access(selected_shard, F_OK) == 0,
				  "generation GC clears selected prepared residue without deleting shards");
	}
	{
		char manifest_temp[1200];
		char prepared_temp[1200];
		char malformed_manifest[1200];
		char malformed_prepared[1200];
		uint64_t obsolete = 0;
		int manifest_fd;
		int prepared_fd;
		int malformed_manifest_fd;
		int malformed_prepared_fd;

		snprintf(manifest_temp, sizeof(manifest_temp),
				 "%s/walidx_manifest_v1.tmp.%020llu.%ld.%u", directory,
				 5ULL, (long) getpid(), 0U);
		snprintf(prepared_temp, sizeof(prepared_temp),
				 "%s/walidx_prepared_v1.tmp.%ld.%u", directory,
				 (long) getpid(), 1U);
		manifest_fd = open(manifest_temp, O_CREAT | O_EXCL | O_WRONLY, 0600);
		prepared_fd = open(prepared_temp, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(manifest_fd >= 0 && prepared_fd >= 0 &&
				write(manifest_fd, "manifest residue", 16) == 16 &&
				write(prepared_fd, "prepared residue", 16) == 16 &&
				close(manifest_fd) == 0 && close(prepared_fd) == 0,
				"create canonical snapshot publication residue");
		check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 2, &obsolete) == 0 &&
				obsolete == 32,
				"snapshot publication residue is counted as physical debt");
		check(ps_walidx_snapshot_gc(directory, 0) == 1 &&
				access(manifest_temp, F_OK) != 0 &&
				access(prepared_temp, F_OK) != 0 &&
				ps_walidx_snapshot_reclaim_bytes(directory, 0, 2, &obsolete) == 0 &&
				obsolete == 0,
				"snapshot GC removes publication residue and releases debt");

		snprintf(malformed_manifest, sizeof(malformed_manifest),
				 "%s/walidx_manifest_v1.tmp.%020llu.%ld.%u", directory,
				 6ULL, (long) getpid(), 128U);
		snprintf(malformed_prepared, sizeof(malformed_prepared),
				 "%s/walidx_prepared_v1.tmp.%ld.%u", directory,
				 (long) getpid(), 128U);
		malformed_manifest_fd = open(malformed_manifest, O_CREAT | O_EXCL | O_WRONLY, 0600);
		malformed_prepared_fd = open(malformed_prepared, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(malformed_manifest_fd >= 0 && malformed_prepared_fd >= 0 &&
				close(malformed_manifest_fd) == 0 && close(malformed_prepared_fd) == 0,
				"create near-miss snapshot temporary names");
		check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 2, &obsolete) != 0 &&
				ps_walidx_snapshot_gc(directory, 0) != 0 &&
				access(malformed_manifest, F_OK) == 0 &&
				access(malformed_prepared, F_OK) == 0,
				"near-miss snapshot residue fails closed and is not deleted");
		unlink(malformed_manifest);
		unlink(malformed_prepared);
		if (sizeof(pid_t) < sizeof(long))
		{
			char malformed_pid[1200];
			int malformed_pid_fd;

			snprintf(malformed_pid, sizeof(malformed_pid),
					 "%s/walidx_prepared_v1.tmp.%ld.%u", directory,
					 LONG_MAX, 0U);
			malformed_pid_fd = open(malformed_pid, O_CREAT | O_EXCL | O_WRONLY, 0600);
			check(malformed_pid_fd >= 0 && close(malformed_pid_fd) == 0 &&
					ps_walidx_snapshot_reclaim_bytes(directory, 0, 2, &obsolete) != 0 &&
					ps_walidx_snapshot_gc(directory, 0) != 0 &&
					access(malformed_pid, F_OK) == 0,
					"a non-round-trippable pid residue fails closed and is not deleted");
			unlink(malformed_pid);
		}
	}
	{
		char symlink_temp[1200];
		char directory_temp[1200];
		struct stat symlink_st;
		int gc_symlink_ok;
		int gc_directory_ok;

		snprintf(symlink_temp, sizeof(symlink_temp),
				 "%s/walidx_manifest_v1.tmp.%020llu.%ld.%u", directory,
				 8ULL, (long) getpid(), 0U);
		snprintf(directory_temp, sizeof(directory_temp),
				 "%s/walidx_prepared_v1.tmp.%ld.%u", directory,
				 (long) getpid(), 2U);
		gc_symlink_ok = symlink("walidx_manifest_v1", symlink_temp) == 0;
		gc_directory_ok = mkdir(directory_temp, 0700) == 0;
		check(gc_symlink_ok && gc_directory_ok &&
				ps_walidx_snapshot_gc(directory, 0) != 0 &&
				lstat(symlink_temp, &symlink_st) == 0 &&
				S_ISLNK(symlink_st.st_mode) &&
				access(directory_temp, F_OK) == 0,
				"snapshot GC rejects exact temporary symlink and preserves it");
		unlink(symlink_temp);
		check(gc_directory_ok && ps_walidx_snapshot_gc(directory, 0) != 0 &&
				access(directory_temp, F_OK) == 0,
				"snapshot GC rejects exact temporary directory and preserves it");
		rmdir(directory_temp);
	}
	{
		uint64_t obsolete = 0;
		char older_shard[1200];
		char selected_shard[1200];
		char selected_saved[sizeof(selected_shard) + sizeof(".saved")];
		int fd;

		snprintf(older_shard, sizeof(older_shard),
				 "%s/walidxg1_%020llu_%03u", directory, 1ULL, 0U);
		fd = open(older_shard, O_CREAT | O_EXCL | O_WRONLY, 0600);
		check(fd >= 0 && write(fd, "old", 3) == 3 && close(fd) == 0 &&
			  ps_walidx_snapshot_reclaim_bytes(directory, 0, 2,
										   &obsolete) == 0 && obsolete == 3,
			  "physical observation counts only older generations");
		check(unlink(older_shard) == 0 &&
			  ps_walidx_snapshot_reclaim_bytes(directory, 0, 2,
										   &obsolete) == 0 && obsolete == 0,
			  "selected and newer retry generations are excluded from debt");
		snprintf(selected_shard, sizeof(selected_shard),
				 "%s/walidxg1_%020llu_%03u", directory, 2ULL, 0U);
		snprintf(selected_saved, sizeof(selected_saved), "%s.saved", selected_shard);
		check(rename(selected_shard, selected_saved) == 0 &&
			  ps_walidx_snapshot_reclaim_bytes(directory, 0, 2, &obsolete) != 0 &&
			  rename(selected_saved, selected_shard) == 0,
			  "physical observation rejects an incomplete selected generation");
		snprintf(selected_shard, sizeof(selected_shard),
				 "%s/walidxg1_%020llu_%03u", directory, 2ULL, 3U);
		{
			int extra_fd = open(selected_shard, O_CREAT | O_EXCL | O_WRONLY, 0600);

			check(extra_fd >= 0 && write(extra_fd, "x", 1) == 1 &&
				  close(extra_fd) == 0 &&
				  ps_walidx_snapshot_reclaim_bytes(directory, 0, 2, &obsolete) != 0,
				  "physical observation rejects an unexpected selected-generation shard");
			unlink(selected_shard);
		}
	}

	check(ps_walidx_snapshot_publish(recovery_directory, 1, 1, 100, 500,
								 first, 3) == 0 &&
			  ps_walidx_snapshot_prepare(&prepared, recovery_directory, 1, 2,
								 500, 900, second, 3) == 0 &&
			  ps_walidx_snapshot_reclaim_bytes(recovery_directory, 1, 1,
										  &prepared_obsolete) == 0 &&
			  prepared_obsolete > 0 &&
			  ps_walidx_snapshot_recover_prepared(recovery_directory, 1, 499) == 0 &&
			  ps_walidx_snapshot_open(&snapshot, recovery_directory, 1) == 0 &&
			  snapshot.generation == 1,
			  "prepared artifacts are counted until restart aborts them behind the frontier");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_publish(reshard_directory, 2, 1, 100, 500,
								 first, 1) == 0 &&
			  ps_walidx_snapshot_prepare(&prepared, reshard_directory, 2, 2,
								 500, 500, second, 3) == 0 &&
			  ps_walidx_snapshot_recover_prepared(reshard_directory, 2, 500) == 0,
			  "restart aborts an uncommitted reshard when the frontier only covers the selected snapshot");
	snprintf(path, sizeof(path), "%s/walidx_prepared_v1", reshard_directory);
	check(access(path, F_OK) != 0 && errno == ENOENT,
		  "uncommitted reshard intent is removed at the selected frontier");
	check(ps_walidx_snapshot_open(&snapshot, reshard_directory, 2) == 0 &&
		  snapshot.generation == 1 && snapshot.nshards == 1,
		  "selected one-shard snapshot survives an aborted reshard");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_prepare(&prepared, recovery_directory, 1, 3,
								 900, 1000, second, 3) == 0,
		  "prepare a generation for partial abort recovery");
	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 recovery_directory, 3ULL, 1U);
	check(unlink(path) == 0 &&
		  ps_walidx_snapshot_recover_prepared(recovery_directory, 1, 899) == 0,
		  "restart completes abort after one prepared shard is already missing");
	snprintf(path, sizeof(path), "%s/walidx_prepared_v1", recovery_directory);
	check(access(path, F_OK) != 0 && errno == ENOENT,
		  "partial abort recovery clears the durable prepare intent");
	check(ps_walidx_snapshot_prepare(&prepared, recovery_directory, 1, 2,
								 500, 900, second, 3) == 0 &&
		  ps_walidx_snapshot_recover_prepared(recovery_directory, 1, 900) == 0 &&
		  ps_walidx_snapshot_open(&snapshot, recovery_directory, 1) == 0 &&
		  snapshot.generation == 1,
		  "restart retains a durable prepare intent covered by the frontier");
	ps_walidx_snapshot_close(&snapshot);
	check(ps_walidx_snapshot_prepare(&prepared, recovery_directory, 1, 2,
								 500, 900, second, 3) == 0 &&
		  ps_walidx_snapshot_commit(&prepared) == 0 &&
		  ps_walidx_snapshot_open(&snapshot, recovery_directory, 1) == 0 &&
		  snapshot.generation == 2 && snapshot.end_lsn == 900,
		  "publisher retries the retained prepare intent idempotently");
	ps_walidx_snapshot_close(&snapshot);

	snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
			 directory, 2ULL, 1U);
	check(pwrite_byte(path, 17, shard1[17] ^ 0xff) == 0,
		  "corrupt one selected shard byte");
	check(ps_walidx_snapshot_open(&snapshot, directory, 0) != 0,
			  "recovery rejects a corrupt shard in the selected generation");
	check(ps_walidx_snapshot_reclaim_bytes(directory, 0, 2,
										  &prepared_obsolete) == 0 &&
			  prepared_obsolete == 0,
			  "metadata observation excludes newer unpublished retry shards");
	check(ps_walidx_snapshot_gc(directory, 0) != 0,
		  "generation GC fails closed when the selected generation is corrupt");
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
		snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
				 directory, 3ULL, shard);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/walidx_manifest_v1", directory);
	unlink(path);
	for (uint32_t generation = 1; generation <= 2; generation++)
		for (uint32_t shard = 0; shard < 3; shard++)
		{
			snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
					 recovery_directory, (unsigned long long) generation, shard);
			unlink(path);
		}
	snprintf(path, sizeof(path), "%s/walidx_manifest_v1", recovery_directory);
	unlink(path);
	for (uint32_t shard = 0; shard < 3; shard++)
	{
		snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
				 reshard_directory, 1ULL, shard);
		unlink(path);
		snprintf(path, sizeof(path), "%s/walidxg1_%020llu_%03u",
				 reshard_directory, 2ULL, shard);
		unlink(path);
	}
	snprintf(path, sizeof(path), "%s/walidx_manifest_v1", reshard_directory);
	unlink(path);
	rmdir(recovery_directory);
	rmdir(reshard_directory);
	rmdir(unselected_directory);
	rmdir(directory);
	rmdir(root);

	printf("pagestore_walidx_snapshot_test: %d checks, %d failed\n",
		   run, failed);
	return failed != 0;
}
