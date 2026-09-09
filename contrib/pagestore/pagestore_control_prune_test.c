/*
 * Focused core test for control-object retention during image compaction.
 *
 * pg_control images (block 0) and their same-version redo-floor notes (block
 * 1) are pruned by the operational page floor like relation pages, but they
 * are fenced by every retained WAL boundary: the newest image/note pair at or
 * below each owner pin and each live branch cap survives, and the WAL
 * retention floor derived from the surviving notes advances accordingly.  The
 * pair is planned over the complete version chain so a flush boundary between
 * the note and its image can never leave an unnoted image behind.
 *
 * Uses the shared core directly; no daemon, no PostgreSQL.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_core.h"

static int checks;
static int failed;

static void
check(int ok, const char *name)
{
	checks++;
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failed++;
	}
}

static void
remove_tree(const char *path)
{
	char command[512];

	if (snprintf(command, sizeof(command), "rm -rf -- '%s'", path) > 0 &&
		system(command) != 0)
		fprintf(stderr, "warning: could not remove %s\n", path);
}

static void
configure_core(int flush_every_page)
{
	page_size = 8192;
	segment_size = 65536;
	flush_pages = flush_every_page ? 1 : 64;
	compact_layers = 0;
	segment_gc_enabled = 1;
	cache_pages = 0;
	use_layers = 1;
	ps_nshards = 1;
	ps_storage = &PsStoragePosix;
}

static PsKey
control_key(void)
{
	PsKey key;

	memset(&key, 0, sizeof(key));
	key.klass = PS_KLASS_CONTROL;
	return key;
}

static int
write_control(uint32_t timeline, uint64_t version, uint64_t redo)
{
	PsKey key = control_key();
	unsigned char page[8192];

	ps_lock_shard_wr(ps_shard_of(&key));
	memset(page, 0, sizeof(page));
	memcpy(page, &redo, sizeof(redo));
	if (append_page(timeline, &key, 1, page, version, NULL) != 0)
	{
		ps_unlock_shard(ps_shard_of(&key));
		return 0;
	}
	memset(page, 0xC3, sizeof(page));
	memcpy(page, &version, sizeof(version));
	if (append_page(timeline, &key, 0, page, version, NULL) != 0)
	{
		ps_unlock_shard(ps_shard_of(&key));
		return 0;
	}
	ps_unlock_shard(ps_shard_of(&key));
	return ps_storage->sync() == 0;
}

static int
write_relation(uint32_t timeline, uint32_t block, uint64_t lsn)
{
	PsKey key = {1, 1, 1, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;
	int rc;

	memset(page, 0x5A, sizeof(page));
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	ps_lock_shard_wr(ps_shard_of(&key));
	rc = append_page(timeline, &key, block, page, lsn, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc == 0;
}

static uint64_t
wal_floor(uint32_t timeline)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_WAL_RETAIN_FLOOR;
	ch.timeline = timeline;
	ch.key = control_key();
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK ? ch.req_lsn : UINT64_MAX;
}

static int
reserve_pin(uint32_t timeline, uint32_t kind, uint64_t owner_id,
			uint32_t generation, uint32_t resources, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch.timeline = timeline;
	ch.blocknum = kind;
	ch.parent_timeline = resources;
	ch.old_nblocks = generation;
	ch.req_seq = owner_id;
	ch.req_lsn = lsn;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
drop_pin(uint32_t timeline, uint32_t kind, uint64_t owner_id, uint32_t generation)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_DROP;
	ch.timeline = timeline;
	ch.blocknum = kind;
	ch.old_nblocks = generation;
	ch.req_seq = owner_id;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
create_branch(uint32_t timeline, uint32_t parent, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_CREATE_BRANCH;
	ch.timeline = timeline;
	ch.parent_timeline = parent;
	ch.req_lsn = lsn;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_shard_wr(0);
	ps_lock_map_wr();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_unlock_shard(0);
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
read_control_at(uint32_t timeline, uint32_t block, uint64_t lsn,
				uint64_t *version_out)
{
	PsKey key = control_key();
	unsigned char page[8192];
	int rc;

	ps_lifecycle_read_lock();
	ps_lock_shard_rd(ps_shard_of(&key));
	rc = read_resolve(timeline, &key, block, lsn, 0, page, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	ps_lifecycle_read_unlock();
	if (rc != 1)
		return 0;
	memcpy(version_out, page, sizeof(*version_out));
	return 1;
}

static void
run_maintenance(int rounds)
{
	for (int i = 0; i < rounds; i++)
		(void) ps_core_maintenance();
}

static void
close_store(void)
{
	ps_core_close();
	if (ps_storage->close != NULL)
		ps_storage->close();
}

/* Three checkpoints; the materializer floor above all of them keeps only the
 * newest pair, and the WAL floor follows its redo. */
static void
test_floor_retires_older_checkpoints(void)
{
	char store[] = "/tmp/pagestore-control-prune-floor-XXXXXX";

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for floor test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints with relation churn between them");
	check(wal_floor(0) == 800, "WAL floor is the oldest checkpoint redo before pruning");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer publishes a cutoff above every checkpoint");
	run_maintenance(64);
	check(wal_floor(0) == 2800,
		  "WAL floor advances to the newest checkpoint at or below the cutoff");
	{
		uint64_t version = 0;

		check(read_control_at(0, 0, 3500, &version) && version == 3000,
			  "newest control image still restores at the cutoff");
		check(read_control_at(0, 1, 3500, &version) && version == 2800,
			  "its same-version note still resolves");
	}
	close_store();

	/* The surviving pair must be exactly what recovery rebuilds. */
	configure_core(1);
	check(ps_core_open(store) == 0, "reopen store after pruning");
	check(wal_floor(0) == 2800, "WAL floor survives restart after pruning");
	close_store();
	remove_tree(store);
}

/* A reader pinned between two checkpoints keeps the older pair it restores. */
static void
test_reader_pin_retains_its_checkpoint(void)
{
	char store[] = "/tmp/pagestore-control-prune-reader-XXXXXX";

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for reader test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints for the reader test");
	check(reserve_pin(0, PS_RETENTION_OWNER_READER, 7, 1,
					  PS_RETENTION_RESOURCE_ALL, 2500),
		  "reader pins between the second and third checkpoint");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above every checkpoint");
	run_maintenance(64);
	check(wal_floor(0) == 1800,
		  "WAL floor stops at the checkpoint the pinned reader restores");
	{
		uint64_t version = 0;

		check(read_control_at(0, 0, 2500, &version) && version == 2000,
			  "reader still restores the second control image");
		check(read_control_at(0, 1, 2500, &version) && version == 1800,
			  "reader still resolves the second note");
	}
	check(drop_pin(0, PS_RETENTION_OWNER_READER, 7, 1), "reader drops its pin");
	run_maintenance(64);
	check(wal_floor(0) == 2800, "WAL floor advances once the reader leaves");
	close_store();
	remove_tree(store);
}

/* A live branch cap is a structural fence for the pair it forked from. */
static void
test_branch_cap_retains_its_checkpoint(void)
{
	char store[] = "/tmp/pagestore-control-prune-branch-XXXXXX";

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for branch test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  create_branch(1, 0, 2200) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "branch between the second and third checkpoint");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above every checkpoint");
	run_maintenance(64);
	check(wal_floor(0) == 1800,
		  "WAL floor stops at the checkpoint the live branch restores");
	check(wal_floor(1) == 1800, "the branch itself reports the same floor");
	{
		uint64_t version = 0;

		check(read_control_at(1, 0, 2200, &version) && version == 2000,
			  "branch restores its fork-point control image through read-through");
	}
	close_store();
	remove_tree(store);
}

/* A flush boundary between the note and its image must not strand an
 * unnoted image: both blocks are planned over their complete chains. */
static void
test_split_pair_is_never_unnoted(void)
{
	char store[] = "/tmp/pagestore-control-prune-split-XXXXXX";
	PsKey key = control_key();
	unsigned char page[8192];

	/* flush_pages=2: the note of checkpoint 3 completes a flush window while
	 * its image stays in the page log. */
	configure_core(0);
	flush_pages = 2;
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for split-pair test");
	check(write_control(0, 1000, 800) && write_control(0, 2000, 1800),
		  "two complete checkpoints flushed into layers");
	ps_lock_shard_wr(ps_shard_of(&key));
	memset(page, 0, sizeof(page));
	{
		uint64_t redo = 2800;

		memcpy(page, &redo, sizeof(redo));
	}
	check(append_page(0, &key, 1, page, 3000, NULL) == 0,
		  "third note lands in the flushed window");
	ps_unlock_shard(ps_shard_of(&key));
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above the split checkpoint");
	run_maintenance(64);
	/* Note 3000 exists without image 3000 yet; the newest complete pair at or
	 * below the cutoff for the image block is 2000, so its note must survive
	 * and the floor must stay provable. */
	check(wal_floor(0) == 1800,
		  "floor stays provable while the third image is still pending");
	ps_lock_shard_wr(ps_shard_of(&key));
	memset(page, 0xC3, sizeof(page));
	check(append_page(0, &key, 0, page, 3000, NULL) == 0,
		  "third image completes the pair");
	ps_unlock_shard(ps_shard_of(&key));
	check(ps_storage->sync() == 0, "sync the completed pair");
	run_maintenance(64);
	check(wal_floor(0) == 2800, "floor advances once the pair is complete");
	close_store();
	remove_tree(store);
}

/* A mirror retry appends the same control bytes again under a new admission
 * sequence.  Only the newest copy of a retained version survives. */
static void
test_retry_copies_collapse(void)
{
	char store[] = "/tmp/pagestore-control-prune-retry-XXXXXX";
	PsKey key = control_key();

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for retry test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_control(0, 1000, 800) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_control(0, 2000, 1800),
		  "write checkpoints with retried control appends");
	check(ps_test_page_version_count(0, &key, 0) == 5 &&
		  ps_test_page_version_count(0, &key, 1) == 5,
		  "retries leave one physical copy per append before compaction");
	check(reserve_pin(0, PS_RETENTION_OWNER_READER, 9, 1,
					  PS_RETENTION_RESOURCE_ALL, 1500) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 2500),
		  "reader below and materializer above the second checkpoint");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &key, 0) == 2 &&
		  ps_test_page_version_count(0, &key, 1) == 2,
		  "compaction keeps exactly one copy per retained version");
	check(wal_floor(0) == 800, "the reader's checkpoint keeps its floor");
	{
		uint64_t version = 0;

		check(read_control_at(0, 0, 1500, &version) && version == 1000 &&
			  read_control_at(0, 1, 1500, &version) && version == 800,
			  "the reader still restores its pair after collapsing retries");
	}
	close_store();
	remove_tree(store);
}

/* A WAL-only pin fences control images.  Dropping it must schedule the
 * control layers for pruning even when no layer-count threshold is crossed. */
static void
test_wal_only_pin_release_reschedules(void)
{
	char store[] = "/tmp/pagestore-control-prune-walpin-XXXXXX";

	configure_core(1);
	compact_layers = 1;
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for WAL-only pin test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints for the WAL-only pin test");
	check(reserve_pin(0, PS_RETENTION_OWNER_READER, 11, 1,
					  PS_RETENTION_RESOURCE_WAL, 2500) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "WAL-only reader below the third checkpoint");
	run_maintenance(64);
	check(wal_floor(0) == 1800,
		  "WAL-only pin keeps the checkpoint it restores");
	check(drop_pin(0, PS_RETENTION_OWNER_READER, 11, 1),
		  "WAL-only reader drops its pin");
	run_maintenance(64);
	check(wal_floor(0) == 2800,
		  "dropping the WAL-only pin reschedules control pruning");
	close_store();
	remove_tree(store);
}

/* An SLRU seed shipped at an older cutoff keeps the control image it
 * resolves its era from, across compaction and restart, while newer
 * unreferenced checkpoints are still retired. */
static void
test_slru_seed_keeps_its_control_image(void)
{
	char store[] = "/tmp/pagestore-control-prune-seed-XXXXXX";
	PsKey seed = {0, 0, 7, 0, PS_KLASS_SLRU};
	unsigned char page[8192];

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the SLRU seed test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints before the seed");
	memset(page, 0x77, sizeof(page));
	ps_lock_shard_wr(ps_shard_of(&seed));
	check(append_page(0, &seed, 0, page, 2000, NULL) == 0,
		  "ship an SLRU seed snapshot at the second checkpoint");
	ps_unlock_shard(ps_shard_of(&seed));
	check(ps_storage->sync() == 0, "sync the seed");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above every checkpoint");
	run_maintenance(64);
	{
		uint64_t version = 0;

		check(read_control_at(0, 0, 2000, &version) && version == 2000,
			  "the seed's control image survives compaction");
		check(wal_floor(0) == 1800,
			  "the WAL floor is bounded by the seed's checkpoint, not older ones");
	}
	close_store();
	configure_core(1);
	check(ps_core_open(store) == 0, "reopen the seeded store");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3600),
		  "advance the cutoff after restart");
	run_maintenance(64);
	{
		uint64_t version = 0;

		check(read_control_at(0, 0, 2000, &version) && version == 2000,
			  "recovery rebuilds the seed fence before pruning again");
	}
	close_store();
	remove_tree(store);
}

/* An artifact shipped at a cutoff that page compaction already passed, with
 * no fence at that cutoff, is refused: the control image it would resolve its
 * era from is gone, so registering it would only pin a dead era.  At or above
 * the frontier it is accepted as before. */
static void
test_late_artifact_below_frontier_is_refused(void)
{
	char store[] = "/tmp/pagestore-control-prune-late-XXXXXX";
	PsKey seed = {0, 0, 7, 0, PS_KLASS_SLRU};
	PsKey snapshot = {0, 0, 9, 0, PS_KLASS_READER_SNAPSHOT};
	unsigned char page[8192];
	int rc_below, rc_snapshot_below, rc_at;

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the late artifact test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above every checkpoint");
	run_maintenance(64);
	{
		uint64_t version = 0;

		check(!read_control_at(0, 0, 2000, &version),
			  "the second checkpoint's control image was retired");
	}
	memset(page, 0x77, sizeof(page));
	ps_lock_shard_wr(ps_shard_of(&seed));
	rc_below = append_page(0, &seed, 0, page, 2000, NULL);
	ps_unlock_shard(ps_shard_of(&seed));
	ps_lock_shard_wr(ps_shard_of(&snapshot));
	rc_snapshot_below = append_page(0, &snapshot, 0, page, 2000, NULL);
	ps_unlock_shard(ps_shard_of(&snapshot));
	check(rc_below != 0 && rc_snapshot_below != 0,
		  "an SLRU seed or reader snapshot at the retired cutoff is refused");
	check(ps_test_artifact_fence_count(0) == 0,
		  "a refused artifact registers no fence");
	ps_lock_shard_wr(ps_shard_of(&seed));
	rc_at = append_page(0, &seed, 0, page, 3500, NULL);
	ps_unlock_shard(ps_shard_of(&seed));
	check(rc_at == 0 && ps_storage->sync() == 0,
		  "an SLRU seed at the cutoff itself is accepted");
	close_store();
	remove_tree(store);
}

/* Independently versioned control blocks (materializer marker, checkpoints)
 * keep their own newest visible version, whether or not their LSNs coincide
 * with image versions the pair plan drops. */
static void
test_independent_blocks_keep_their_newest(void)
{
	char store[] = "/tmp/pagestore-control-prune-marker-XXXXXX";
	PsKey key = control_key();
	unsigned char page[8192];

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the marker test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints for the marker test");
	ps_lock_shard_wr(ps_shard_of(&key));
	for (uint64_t version = 1000; version <= 3000; version += 500)
	{
		memset(page, 0, sizeof(page));
		memcpy(page, &version, sizeof(version));
		check(append_page(0, &key, 3, page, version, NULL) == 0,
			  "write a materializer marker version");
	}
	ps_unlock_shard(ps_shard_of(&key));
	check(ps_storage->sync() == 0, "sync the markers");
	check(ps_test_page_version_count(0, &key, 3) == 5,
		  "five marker versions before compaction");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above every marker");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &key, 3) == 1,
		  "only the newest marker survives the cutoff");
	{
		uint64_t version = 0;

		check(read_control_at(0, 3, 3500, &version) && version == 3000,
			  "the newest marker is the one retained");
		check(read_control_at(0, 0, 3500, &version) && version == 3000,
			  "the image plan is unaffected by the marker block");
	}
	close_store();
	remove_tree(store);
}

int
main(void)
{
	test_floor_retires_older_checkpoints();
	test_reader_pin_retains_its_checkpoint();
	test_branch_cap_retains_its_checkpoint();
	test_split_pair_is_never_unnoted();
	test_retry_copies_collapse();
	test_wal_only_pin_release_reschedules();
	test_slru_seed_keeps_its_control_image();
	test_late_artifact_below_frontier_is_refused();
	test_independent_blocks_keep_their_newest();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
