/* Deterministic publication/drop tests; no stress workload or PostgreSQL. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>
#include "pagestore_core.h"
#include "pagestore_artifact_format.h"
static int	failures,
			checks;
static PsKey key = {.klass = PS_KLASS_SLRU, .relNumber = 1};
static int	fail_sync_at,
			sync_calls;
static int
fault_sync(void)
{
	if (++sync_calls == fail_sync_at)
	{
		errno = EIO;
		return -1;
	}
	return PsStoragePosix.sync();
}
static void
check(int ok, const char *what)
{
	checks++;
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", what);
		failures++;
	}
}
static void
configure(void)
{
	page_size = 8192;
	segment_size = 65536;
	flush_pages = 1;
	/* One merged layer must stop being due so every shard gets a turn. */
	compact_layers = 1;
	segment_gc_enabled = 1;
	cache_pages = 0;
	use_layers = 1;
	ps_nshards = 3;
	ps_storage = &PsStoragePosix;
	if (getenv("PAGESTORE_ARTIFACT_TEST_READER"))
		key.klass = PS_KLASS_READER_SNAPSHOT;
}
static uint64_t
begin(uint64_t lsn)
{
	uint64_t	token = 0;

	ps_lock_shard_wr(ps_shard_of(&key));
	int			rc = ps_artifact_begin(0, &key, lsn, &token);

	ps_unlock_shard(ps_shard_of(&key));
	check(rc == 0 && token != 0, "begin publication");
	return token;
}
static int
write_page(uint64_t lsn, uint64_t token, uint32_t block, int value)
{
	unsigned char page[8192];

	memset(page, value, sizeof(page));
	ps_lock_shard_wr(ps_shard_of(&key));
	int			rc = ps_artifact_write(0, &key, block, page, lsn, token, NULL);

	ps_unlock_shard(ps_shard_of(&key));
	return rc;
}
static int
commit(uint64_t lsn, uint64_t token, uint64_t count)
{
	ps_lock_shard_wr(ps_shard_of(&key));
	int			rc = ps_artifact_commit(0, &key, lsn, token, count);

	ps_unlock_shard(ps_shard_of(&key));
	return rc;
}
static int
drop(uint64_t lsn)
{
	ps_lock_shard_wr(ps_shard_of(&key));
	int			rc = ps_artifact_drop(0, &key, lsn);

	ps_unlock_shard(ps_shard_of(&key));
	return rc;
}
static int
read_value(uint32_t tl, uint64_t horizon, uint32_t block, int expected)
{
	unsigned char page[8192];
	uint64_t	lsn = 0;

	ps_lock_shard_rd(ps_shard_of(&key));
	int			rc = read_resolve(tl, &key, block, horizon, 0, page, &lsn);

	ps_unlock_shard(ps_shard_of(&key));
	if (expected < 0)
		return rc == 0;
	if (rc != 1)
		return 0;
	for (size_t i = 0; i < sizeof(page); i++)
		if (page[i] != expected)
			return 0;
	return 1;
}
static void
meta(PsChannel *ch)
{
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(ch);
	ps_lifecycle_read_unlock();
	check(ch->status == PS_STATUS_OK, "metadata operation");
}
static int
metadata_matches(uint32_t tl, uint64_t lsn, uint64_t seq,
				 int exists, uint32_t nblocks)
{
	PsChannel	ch = {.opcode = PS_OP_EXISTS, .timeline = tl, .key = key,
	.req_lsn = lsn, .req_seq = seq};
	int			ok;

	ps_lock_shard_rd(ps_shard_of(&key));
	(void) ps_handle_meta(&ch);
	ok = ch.status == PS_STATUS_OK && ch.result == (uint32_t) exists;
	ch.opcode = PS_OP_NBLOCKS;
	(void) ps_handle_meta(&ch);
	ok = ok && ch.status == PS_STATUS_OK && ch.result == nblocks;
	ps_unlock_shard(ps_shard_of(&key));
	return ok;
}

static void
maintain(void)
{
	for (int i = 0; i < 48; i++)
	{
		(void) ps_core_maintenance();
		usleep(1000);
	}
}
int
main(int argc, char **argv)
{
	configure();
	if (argc == 3)
	{
		if (strncmp(argv[1], "sync-fail-", 10) == 0)
			flush_pages = 64;
		if (ps_core_open(argv[2]) != 0)
			return 10;
		if (strncmp(argv[1], "sync-fail-", 10) == 0)
		{
			PsStorage	fault = PsStoragePosix;
			uint64_t	token = begin(650);

			if (write_page(650, token, 0, 0x65) != 0)
				return 15;
			fault.sync = fault_sync;
			fail_sync_at = atoi(argv[1] + 10);
			sync_calls = 0;
			ps_storage = &fault;
			int			rc = commit(650, token, 1);

			ps_storage = &PsStoragePosix;
			_exit(rc != 0 && sync_calls >= fail_sync_at ? 0 : 16);
		}
		if (strcmp(argv[1], "corrupt-crash") == 0)
		{
			PsKey		meta = key;
			unsigned char page[8192] = {0};
			PsArtifactLifecycle record = {.magic = PS_ARTIFACT_LIFECYCLE_MAGIC,
				.version = PS_ARTIFACT_LIFECYCLE_VERSION, .state = PS_ARTIFACT_DROPPED,
				.generation = 700, .incarnation = 1, .klass = key.klass,
			.spcOid = key.spcOid, .dbOid = key.dbOid, .relNumber = key.relNumber};

			record.crc = (~ps_crc32c_update(UINT32_MAX, &record,
											offsetof(PsArtifactLifecycle, crc))) ^ 1;

			meta.forkNum = key.klass;
			meta.klass = PS_KLASS_ARTIFACT;
			memcpy(page, &record, sizeof(record));
			ps_lock_shard_wr(ps_shard_of(&meta));
			int			rc = append_page(0, &meta, 1, page, 700, NULL);

			ps_unlock_shard(ps_shard_of(&meta));
			_exit(rc == 0 && ps_storage->sync() == 0 ? 0 : 14);
		}
		uint64_t	token = begin(300);

		if (write_page(300, token, 0, 0x33) != 0 || ps_storage->sync() != 0)
			return 11;
		if (strcmp(argv[1], "commit-crash") == 0 && commit(300, token, 1) != 0)
			return 12;
		_exit(failures ? 13 : 0);
	}
	char		store[] = "/tmp/pagestore-artifact-XXXXXX";

	check(mkdtemp(store) != NULL && ps_core_open(store) == 0, "open three-shard store");
	key.relNumber = 2;
	uint64_t	empty = begin(100);

	check(write_page(100, empty, 8, 0x18) == 0 &&
		  metadata_matches(0, 0, 0, 0, 0), "first pending attempt has no visible metadata");
	empty = begin(100);
	check(commit(100, empty, 0) == 0 && metadata_matches(0, 0, 0, 1, 0),
		  "publish empty generation with empty metadata");
	empty = begin(100);
	check(commit(100, empty, 0) == 0 && read_value(0, 100, 0, -1),
		  "retry empty generation at the same cutoff");
	key.relNumber = 1;
	check(write_page(100, 0, 0, 0x11) == 0 && write_page(100, 0, 1, 0x12) == 0, "legacy generation");
	uint64_t	token = begin(200);

	check(write_page(200, token, 7, 0x21) == 0, "stage first page");
	check(read_value(0, 200, 0, 0x11) && read_value(0, 200, 1, 0x12), "partial generation preserves complete legacy base");
	check(metadata_matches(0, 0, 0, 1, 2), "pending high block preserves legacy size");
	check(commit(200, token, 2) != 0, "commit refuses missing page");
	uint64_t	retry = begin(200);

	check(write_page(200, token, 1, 0x22) != 0, "superseded attempt cannot append");
	check(write_page(200, retry, 1, 0x23) == 0 && commit(200, retry, 2) != 0, "retry cannot count previous attempt's pages");
	check(write_page(200, retry, 1, 0x23) == 0 && commit(200, retry, 1) == 0,
		  "overwriting a block counts once in sparse replacement");
	check(metadata_matches(0, 0, 0, 1, 2), "sparse size uses maximum block, not page count");
	check(read_value(0, 200, 0, -1) && read_value(0, 200, 1, 0x23), "absent blocks never inherit older pages");
	check(write_page(200, retry, 1, 0x24) != 0 && write_page(200, 0, 1, 0x24) != 0, "committed interval immutable; legacy bypass refused");
	token = begin(200);
	check(metadata_matches(0, 0, 0, 1, 2) && metadata_matches(0, 200, token, 1, 2),
		  "same-LSN retry retains completed metadata");
	check(write_page(200, token, 1, 0x25) != 0 && read_value(0, 200, 1, 0x23), "same-LSN retry rejects changes to completed bytes");
	check(write_page(200, token, 1, 0x23) == 0 && commit(200, token, 1) == 0 &&
		  read_value(0, 200, 1, 0x23), "identical same-LSN retry is idempotent");
	ps_core_close();
	for (int committed = 0; committed < 2; committed++)
	{
		pid_t		child = fork();

		if (child == 0)
		{
			execl(argv[0], argv[0], committed ? "commit-crash" : "stage-crash", store, NULL);
			_exit(127);
		}
		int			status = 0;

		check(child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "crash subprocess reached intended boundary");
		check(ps_core_open(store) == 0, "recover after process exit without shutdown");
		check(read_value(0, 300, committed ? 0 : 1, committed ? 0x33 : 0x23), "recover complete generation or previous base");
		ps_core_close();
	}
	check(ps_core_open(store) == 0, "second restart");
	check(metadata_matches(0, 0, 0, 1, 1), "recovery restores committed size");
	key.relNumber = 2;
	check(metadata_matches(0, 0, 0, 1, 0), "empty metadata survives restart");
	key.relNumber = 1;
	PsChannel	branch = {.opcode = PS_OP_CREATE_BRANCH, .timeline = 1, .req_lsn = 300};

	meta(&branch);
	token = begin(300);
	check(write_page(300, token, 0, 0x34) != 0 &&
		  write_page(300, token, 0, 0x33) == 0 && commit(300, token, 1) == 0 &&
		  read_value(1, UINT64_MAX, 0, 0x33), "same-LSN retry cannot change branch bytes after restart");
	check(drop(400) == 0 && drop(400) == 0, "durable drop is idempotent");
	check(metadata_matches(0, 0, 0, 0, 0) && metadata_matches(0, 300, 0, 1, 1) &&
		  metadata_matches(1, 0, 0, 1, 1), "drop metadata respects history and ancestry");
	check(read_value(0, 400, 0, -1) && read_value(0, 300, 0, 0x33) && read_value(1, UINT64_MAX, 0, 0x33), "drop preserves retained history and branch ancestry");
	check(write_page(300, 0, 0, 0x44) != 0, "delayed writer cannot resurrect dropped object");
	PsChannel	pin = {.opcode = PS_OP_RETENTION_PIN_RESERVE, .blocknum = PS_RETENTION_OWNER_MATERIALIZER,
	.parent_timeline = PS_RETENTION_RESOURCE_ALL, .old_nblocks = 1, .req_seq = 1, .req_lsn = 500};

	meta(&pin);
	maintain();
	check(read_value(1, UINT64_MAX, 0, 0x33), "compaction preserves descendant generation");
	ps_core_close();
	check(ps_core_open(store) == 0 && read_value(0, 500, 0, -1) && read_value(1, UINT64_MAX, 0, 0x33), "drop survives compacted restart");
	check(metadata_matches(0, 0, 0, 0, 0) && metadata_matches(1, 0, 0, 1, 1),
		  "compacted restart preserves drop and branch metadata");
	PsChannel	deleting = {.opcode = PS_OP_BEGIN_DELETE, .timeline = 1, .req_seq = 1};

	meta(&deleting);
	maintain();
	check(ps_test_page_version_count(0, &key, 0) == 0 && ps_test_page_version_count(0, &key, 1) == 0, "last generation reclaimed after dependency deletion");
	check(ps_test_artifact_fence_count(0) == 0, "retired data releases every artifact control-era fence");
	token = begin(600);
	check(write_page(600, token, 0, 0x66) == 0 && commit(600, token, 1) == 0 && read_value(0, 600, 0, 0x66), "recreate after drop");
	ps_core_close();
	for (int phase = 1; phase <= 2; phase++)
	{
		pid_t		child = fork();
		int			status = 0;

		if (child == 0)
		{
			execl(argv[0], argv[0], phase == 1 ? "sync-fail-1" : "sync-fail-2", store, NULL);
			_exit(127);
		}
		check(child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
			  "publication reports injected data or marker sync failure");
		check(ps_core_open(store) == 0, "recover ambiguous sync outcome");
		check(phase == 1 ? read_value(0, 650, 0, 0x66) :
			  (read_value(0, 650, 0, 0x66) || read_value(0, 650, 0, 0x65)),
			  "sync failure recovers old or complete new generation");
		ps_core_close();
	}
	pid_t		child = fork();

	if (child == 0)
	{
		execl(argv[0], argv[0], "corrupt-crash", store, NULL);
		_exit(127);
	}
	int			status = 0;

	check(child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "persist corrupt lifecycle record");
	check(ps_core_open(store) != 0, "recovery rejects corrupt completion metadata");
	char		cmd[512];

	snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", store);
	if (system(cmd) != 0)
		failures++;
	printf("%d checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
