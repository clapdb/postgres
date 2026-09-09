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

/* The redo-floor note alone: a mirror that timed out before its image. */
static int
write_note(uint32_t timeline, uint64_t version, uint64_t redo)
{
	PsKey key = control_key();
	unsigned char page[8192];
	int rc;

	ps_lock_shard_wr(ps_shard_of(&key));
	memset(page, 0, sizeof(page));
	memcpy(page, &redo, sizeof(redo));
	rc = append_page(timeline, &key, 1, page, version, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc == 0 && ps_storage->sync() == 0;
}

/* One checkpoint-completing control publication as the compute mirrors it:
 * the exact-redo pair (note, image) at the checkpoint redo, the admission
 * fence at the redo, then the pair and the fence again at the update LSN. */
static int
write_checkpoint(uint32_t timeline, uint64_t redo, uint64_t update)
{
	PsKey key = control_key();
	unsigned char page[8192];
	PsAdmissionFence fence;
	int rc = 0;

	memset(&fence, 0, sizeof(fence));
	fence.magic = 0x50534146;
	fence.version = 1;
	fence.redo_lsn = redo;
	fence.admission_seq = 1;
	ps_lock_shard_wr(ps_shard_of(&key));
	memset(page, 0, sizeof(page));
	memcpy(page, &redo, sizeof(redo));
	rc |= append_page(timeline, &key, 1, page, redo, NULL);
	memset(page, 0xC3, sizeof(page));
	memcpy(page, &redo, sizeof(redo));
	rc |= append_page(timeline, &key, 0, page, redo, NULL);
	memset(page, 0, sizeof(page));
	memcpy(page, &fence, sizeof(fence));
	rc |= append_page(timeline, &key, 2, page, redo, NULL);
	memset(page, 0, sizeof(page));
	memcpy(page, &redo, sizeof(redo));
	rc |= append_page(timeline, &key, 1, page, update, NULL);
	memset(page, 0xC3, sizeof(page));
	memcpy(page, &update, sizeof(update));
	rc |= append_page(timeline, &key, 0, page, update, NULL);
	memset(page, 0, sizeof(page));
	memcpy(page, &fence, sizeof(fence));
	rc |= append_page(timeline, &key, 2, page, update, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc == 0 && ps_storage->sync() == 0;
}

/* The materializer's durable progress marker (control block 3). */
static int
write_marker(uint32_t timeline, uint64_t lsn)
{
	PsKey key = control_key();
	unsigned char page[8192];
	int rc;

	memset(page, 0x3d, sizeof(page));
	memcpy(page, &lsn, sizeof(lsn));
	ps_lock_shard_wr(ps_shard_of(&key));
	rc = append_page(timeline, &key, 3, page, lsn, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc == 0 && ps_storage->sync() == 0;
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

/* A mirror that keeps timing out after its note but before its image
 * appends the same note again under a new sequence each time.  Compaction
 * keeps exactly one copy of that in-flight note while waiting for the image,
 * so repeated failures cannot grow the control layers without bound. */
static void
test_in_flight_note_retries_collapse(void)
{
	char store[] = "/tmp/pagestore-control-prune-inflight-XXXXXX";
	PsKey key = control_key();

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the in-flight note test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_note(0, 3000, 2800) && write_note(0, 3000, 2800) &&
		  write_note(0, 3000, 2800),
		  "two checkpoints, then a note retried three times without its image");
	check(ps_test_page_version_count(0, &key, 0) == 2 &&
		  ps_test_page_version_count(0, &key, 1) == 5,
		  "every retried note copy exists before compaction");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 2500),
		  "materializer cutoff above the second checkpoint");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &key, 1) == 2,
		  "one copy of the in-flight note survives with the retained pair's note");
	{
		uint64_t version = 0;

		check(read_control_at(0, 1, 3000, &version) && version == 2800,
			  "the surviving in-flight note is still readable");
	}
	check(write_control(0, 3000, 2800) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "the image finally arrives and the cutoff moves past it");
	run_maintenance(64);
	{
		uint64_t version = 0;

		check(read_control_at(0, 0, 3000, &version) && version == 3000 &&
			  read_control_at(0, 1, 3000, &version) && version == 2800,
			  "the completed pair restores at its checkpoint");
		check(wal_floor(0) == 2800, "the WAL floor follows the completed pair");
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
	/* The consumer that captures a seed pins its cutoff first; the seed
	 * itself is retained by that pin and fences the control image. */
	check(reserve_pin(0, PS_RETENTION_OWNER_READER, 7, 1,
					  PS_RETENTION_RESOURCE_ALL, 2000),
		  "the seed's consumer pins the second checkpoint");
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

/* An artifact admitted below the frontier reserves its fence before the
 * bytes are written.  If the append then fails, the reservation is released
 * and no fence remains, so an abandoned cutoff does not pin a control era
 * until restart; a later successful append at the same cutoff fences it. */
static void
test_failed_artifact_append_releases_its_fence(void)
{
	char store[] = "/tmp/pagestore-control-prune-release-XXXXXX";
	PsKey seed = {0, 0, 7, 0, PS_KLASS_SLRU};
	unsigned char page[8192];
	int rc;

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the released fence test");
	check(write_relation(0, 0, 900) && write_control(0, 1000, 800) &&
		  write_relation(0, 0, 1900) && write_control(0, 2000, 1800) &&
		  write_relation(0, 0, 2900) && write_control(0, 3000, 2800),
		  "write three checkpoints for the released fence test");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_ALL, 3500),
		  "materializer cutoff above every checkpoint for the released fence test");
	run_maintenance(64);
	close_store();
	/* the next segment write fails: the seed is admitted but never lands */
	check(setenv("PAGESTORE_TEST_FAIL_SEG_WRITES", "1", 1) == 0,
		  "arm one failing segment write");
	configure_core(1);
	check(ps_core_open(store) == 0, "reopen with the failing write armed");
	memset(page, 0x77, sizeof(page));
	ps_lock_shard_wr(ps_shard_of(&seed));
	rc = append_page(0, &seed, 0, page, 3500, NULL);
	ps_unlock_shard(ps_shard_of(&seed));
	check(rc != 0, "the admitted seed append fails at the segment write");
	check(ps_test_artifact_fence_count(0) == 0,
		  "a failed artifact append leaves no fence behind");
	check(unsetenv("PAGESTORE_TEST_FAIL_SEG_WRITES") == 0,
		  "disarm the failing segment write");
	ps_lock_shard_wr(ps_shard_of(&seed));
	rc = append_page(0, &seed, 0, page, 3500, NULL);
	ps_unlock_shard(ps_shard_of(&seed));
	check(rc == 0 && ps_storage->sync() == 0 &&
		  ps_test_artifact_fence_count(0) == 1,
		  "the retried seed append lands and fences its cutoff");
	close_store();
	remove_tree(store);
}

/* A materializer pins WAL and the WAL index but no page history, at the redo
 * of its last durable restartpoint.  That pin is the operational page-history
 * cutoff: relation history and control checkpoints below it are retired with
 * no page-history owner at all, the newest checkpoint keeps its exact-redo
 * twin, later pins are refused below the frontier, and the cutoff survives a
 * restart. */
static void
test_materializer_pin_is_the_page_cutoff(void)
{
	char store[] = "/tmp/pagestore-control-prune-marker-XXXXXX";
	PsKey rel = {1, 1, 1, 0, PS_KLASS_RELATION};
	uint64_t version = 0;

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the materializer cutoff test");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 500),
		  "the materializer registers at its first restart redo");
	check(write_relation(0, 0, 900) && write_checkpoint(0, 800, 1000) &&
		  write_relation(0, 0, 1900) && write_checkpoint(0, 1800, 2000) &&
		  write_relation(0, 0, 2900) && write_checkpoint(0, 2800, 3000),
		  "three checkpoints with exact-redo twins and relation history");
	run_maintenance(16);
	check(ps_test_page_version_count(0, &rel, 0) == 3 &&
		  read_control_at(0, 0, 1000, &version) && version == 1000,
		  "nothing is pruned while the materializer's restart redo is below it");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 3400) &&
		  write_marker(0, 3450),
		  "the materializer advances its pin to a newer restart redo");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &rel, 0) == 1,
		  "relation history below the pin is retired without a page owner");
	check(read_control_at(0, 0, 3000, &version) && version == 3000 &&
		  read_control_at(0, 0, 2800, &version) && version == 2800,
		  "the newest checkpoint keeps both its update image and its exact-redo twin");
	check(!read_control_at(0, 0, 2000, &version),
		  "older checkpoints are retired");
	check(wal_floor(0) == 2800, "the WAL floor follows the retained checkpoint");
	check(!reserve_pin(0, PS_RETENTION_OWNER_READER, 9, 1,
					   PS_RETENTION_RESOURCE_ALL, 1500),
		  "a page-history pin below the derived frontier is refused");
	check(reserve_pin(0, PS_RETENTION_OWNER_READER, 9, 2,
					  PS_RETENTION_RESOURCE_ALL, 3450),
		  "a page-history pin above the derived frontier is admitted");
	close_store();
	configure_core(1);
	check(ps_core_open(store) == 0, "reopen the store");
	run_maintenance(16);
	check(read_control_at(0, 0, 2800, &version) && version == 2800 &&
		  !read_control_at(0, 0, 2000, &version) && wal_floor(0) == 2800,
		  "the derived cutoff and the retained twin survive restart");
	close_store();
	remove_tree(store);
}

/* A direct-write compute pins nothing: the redo of its newest durable
 * checkpoint note is the cutoff, and a writer's notes are ignored while a
 * materializer owns the timeline. */
static void
test_checkpoint_note_is_the_page_cutoff(void)
{
	char store[] = "/tmp/pagestore-control-prune-note-XXXXXX";
	PsKey rel = {1, 1, 1, 0, PS_KLASS_RELATION};
	uint64_t version = 0;

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the note cutoff test");
	check(write_relation(0, 0, 900) && write_checkpoint(0, 800, 1000) &&
		  write_relation(0, 0, 1900) && write_checkpoint(0, 1800, 2000) &&
		  write_relation(0, 0, 2900) && write_checkpoint(0, 2800, 3000),
		  "three checkpoints from a direct-write compute");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &rel, 0) == 2,
		  "history below the newest checkpoint redo is retired, the rest kept");
	check(read_control_at(0, 0, 2800, &version) && version == 2800 &&
		  read_control_at(0, 0, 3000, &version) && version == 3000 &&
		  !read_control_at(0, 0, 2000, &version),
		  "the newest checkpoint and its twin survive, older ones are retired");
	check(wal_floor(0) == 2800, "the WAL floor follows the newest checkpoint");
	close_store();
	remove_tree(store);

	/* With a materializer owner present, the writer's newer checkpoint notes
	 * do not move the cutoff ahead of materialization. */
	configure_core(1);
	strcpy(store, "/tmp/pagestore-control-prune-note-XXXXXX");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the materializer-owned note test");
	check(write_relation(0, 0, 900) && write_checkpoint(0, 800, 1000) &&
		  write_relation(0, 0, 1900) && write_checkpoint(0, 1800, 2000) &&
		  reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 1500),
		  "a materializer owns the timeline while the writer checkpoints ahead");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &rel, 0) == 2 &&
		  read_control_at(0, 0, 1000, &version) && version == 1000,
		  "writer checkpoint notes establish no cutoff on a materializer-fed timeline");
	close_store();
	remove_tree(store);
}

/* An object of any class, written as the backend's obj_write would. */
static int
write_object(uint32_t timeline, uint32_t klass, uint32_t object,
			 uint32_t block, uint64_t version, unsigned char tag)
{
	PsKey key = {0, 0, object, 0, klass};
	unsigned char page[8192];
	int rc;

	memset(page, tag, sizeof(page));
	memcpy(page, &version, sizeof(version));
	ps_lock_shard_wr(ps_shard_of(&key));
	rc = append_page(timeline, &key, block, page, version, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	return rc == 0 && ps_storage->sync() == 0;
}

/* Resolved version of an object at or below lsn, or 0 when none is left. */
static uint64_t
object_version_at(uint32_t timeline, uint32_t klass, uint32_t object,
				  uint32_t block, uint64_t lsn)
{
	PsKey key = {0, 0, object, 0, klass};
	unsigned char page[8192];
	uint64_t version = 0;
	int rc;

	ps_lifecycle_read_lock();
	ps_lock_shard_rd(ps_shard_of(&key));
	rc = read_resolve(timeline, &key, block, lsn, 0, page, NULL);
	ps_unlock_shard(ps_shard_of(&key));
	ps_lifecycle_read_unlock();
	if (rc != 1)
		return 0;
	memcpy(&version, page, sizeof(version));
	return version;
}

/* SLRU seeds are replay bases and reader snapshots exact-horizon artifacts,
 * consumed as-of a horizon their consumer pinned or a branch forked at.  Below
 * the floor they survive as the newest version at or below such a fence; a
 * retired artifact also releases the control era it fenced, and a dropped pin
 * retires the artifacts only it protected. */
static void
test_stale_artifacts_are_retired(void)
{
	char store[] = "/tmp/pagestore-control-prune-artifact-XXXXXX";
	PsKey rel = {1, 1, 1, 0, PS_KLASS_RELATION};
	PsKey seed7 = {0, 0, 7, 0, PS_KLASS_SLRU};

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the artifact retention test");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 500),
		  "a materializer owns the timeline");
	check(write_relation(0, 0, 900) && write_checkpoint(0, 800, 1000) &&
		  write_relation(0, 0, 1900) && write_checkpoint(0, 1800, 2000) &&
		  write_relation(0, 0, 2900) && write_checkpoint(0, 2800, 3000),
		  "three checkpoints for the artifact retention test");
	check(reserve_pin(0, PS_RETENTION_OWNER_READER, 9, 1,
					  PS_RETENTION_RESOURCE_ALL, 1500),
		  "a reader pins the cutoff it captures its artifacts at");
	check(write_object(0, PS_KLASS_SLRU, 7, 0, 1500, 0x71) &&
		  write_object(0, PS_KLASS_SLRU, 7, 1, 1500, 0x72) &&
		  write_object(0, PS_KLASS_READER_SNAPSHOT, 9, 0, 1500, 0x73) &&
		  write_object(0, PS_KLASS_SLRU, 7, 0, 2500, 0x74) &&
		  write_object(0, PS_KLASS_SLRU, 7, 0, 2500, 0x75) &&
		  write_object(0, PS_KLASS_READER_SNAPSHOT, 9, 0, 2500, 0x76),
		  "artifacts at the reader's cutoff and at a branch base (with a retry)");
	check(create_branch(1, 0, 2500), "a branch forks at the second cutoff");
	check(ps_test_artifact_fence_count(0) == 2,
		  "both artifact cutoffs fence control images before compaction");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 3400),
		  "the materializer moves its restart redo above every artifact");
	run_maintenance(64);
	/* The reader's pin is the floor, so everything at or above it stays,
	 * retried copies included. */
	check(object_version_at(0, PS_KLASS_SLRU, 7, 0, 1500) == 1500 &&
		  object_version_at(0, PS_KLASS_READER_SNAPSHOT, 9, 0, 1500) == 1500 &&
		  object_version_at(0, PS_KLASS_SLRU, 7, 0, 2500) == 2500 &&
		  ps_test_page_version_count(0, &seed7, 0) == 3 &&
		  ps_test_artifact_fence_count(0) == 2,
		  "artifacts at and above the reader's pin survive");
	check(drop_pin(0, PS_RETENTION_OWNER_READER, 9, 1),
		  "the reader releases its pin");
	run_maintenance(64);
	/* Only the branch's fork point remains below the floor: the seed page
	 * that has a newer version at the fork point and the reader snapshot are
	 * retired at 1500, while the seed page whose newest base at or below the
	 * fork point is still the 1500 one survives as that base. */
	check(object_version_at(0, PS_KLASS_SLRU, 7, 0, 1500) == 0 &&
		  object_version_at(0, PS_KLASS_READER_SNAPSHOT, 9, 0, 1500) == 0,
		  "a dropped pin retires the artifacts it protected below the floor");
	check(object_version_at(0, PS_KLASS_SLRU, 7, 1, 1500) == 1500,
		  "a seed page stays as the newest base below the branch's fork point");
	check(object_version_at(0, PS_KLASS_SLRU, 7, 0, 2500) == 2500 &&
		  object_version_at(0, PS_KLASS_READER_SNAPSHOT, 9, 0, 2500) == 2500 &&
		  ps_test_page_version_count(0, &seed7, 0) == 1,
		  "artifacts at a live branch's fork point survive, one copy per cutoff");
	check(ps_test_artifact_fence_count(0) == 2,
		  "each cutoff with a surviving artifact still fences control images");
	check(ps_test_page_version_count(0, &rel, 0) == 2,
		  "relation history keeps the branch's base and the newest version");
	close_store();
	configure_core(1);
	check(ps_core_open(store) == 0, "reopen the store after artifact pruning");
	check(object_version_at(0, PS_KLASS_SLRU, 7, 0, 1500) == 0 &&
		  object_version_at(0, PS_KLASS_SLRU, 7, 0, 2500) == 2500 &&
		  object_version_at(0, PS_KLASS_SLRU, 7, 1, 1500) == 1500 &&
		  ps_test_artifact_fence_count(0) == 2,
		  "retired artifacts stay retired and the surviving bases persist across restart");
	close_store();
	remove_tree(store);
}

/* The live SLRU mirror, its truncation tombstones, and the visibility
 * watermark are read as-of a horizon like relation pages, so they keep the
 * newest version at or below every fence and the floor, and everything above
 * the floor. */
static void
test_live_slru_mirror_follows_page_history(void)
{
	char store[] = "/tmp/pagestore-control-prune-slrulive-XXXXXX";
	PsKey live = {0, 0, 7, 0, PS_KLASS_SLRU_LIVE};
	PsKey tomb = {0, 0, 7, 0, PS_KLASS_SLRU_TOMB};
	PsKey wm = {0, 0, 0, 0, PS_KLASS_SLRU_WM};

	configure_core(1);
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the live SLRU retention test");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 500),
		  "a materializer owns the live-mirror timeline");
	check(write_object(0, PS_KLASS_SLRU_LIVE, 7, 0, 1000, 0x11) &&
		  write_object(0, PS_KLASS_SLRU_WM, 0, 0, 1100, 0x12) &&
		  write_object(0, PS_KLASS_SLRU_TOMB, 7, 0, 1200, 0x13) &&
		  write_object(0, PS_KLASS_SLRU_LIVE, 7, 0, 2000, 0x14) &&
		  write_object(0, PS_KLASS_SLRU_WM, 0, 0, 2100, 0x15) &&
		  write_object(0, PS_KLASS_SLRU_TOMB, 7, 0, 2200, 0x16) &&
		  write_object(0, PS_KLASS_SLRU_LIVE, 7, 0, 3000, 0x17) &&
		  write_object(0, PS_KLASS_SLRU_WM, 0, 0, 3100, 0x18),
		  "three generations of live mirror, tombstone, and watermark");
	check(create_branch(1, 0, 1500), "a branch forks inside the history");
	check(reserve_pin(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
					  PS_RETENTION_RESOURCE_WAL |
					  PS_RETENTION_RESOURCE_WAL_INDEX, 3500),
		  "the materializer's restart redo passes the whole history");
	run_maintenance(64);
	check(ps_test_page_version_count(0, &live, 0) == 2 &&
		  object_version_at(0, PS_KLASS_SLRU_LIVE, 7, 0, 1500) == 1000 &&
		  object_version_at(0, PS_KLASS_SLRU_LIVE, 7, 0, 3500) == 3000,
		  "the live mirror keeps the branch's version and the newest one");
	check(ps_test_page_version_count(0, &tomb, 0) == 2 &&
		  object_version_at(0, PS_KLASS_SLRU_TOMB, 7, 0, 1500) == 1200 &&
		  object_version_at(0, PS_KLASS_SLRU_TOMB, 7, 0, 3500) == 2200,
		  "tombstones keep the branch's version and the newest one");
	check(ps_test_page_version_count(0, &wm, 0) == 2 &&
		  object_version_at(0, PS_KLASS_SLRU_WM, 0, 0, 1500) == 1100 &&
		  object_version_at(0, PS_KLASS_SLRU_WM, 0, 0, 3500) == 3100,
		  "the watermark keeps the branch's version and the newest one");
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
	test_in_flight_note_retries_collapse();
	test_wal_only_pin_release_reschedules();
	test_slru_seed_keeps_its_control_image();
	test_late_artifact_below_frontier_is_refused();
	test_failed_artifact_append_releases_its_fence();
	test_materializer_pin_is_the_page_cutoff();
	test_checkpoint_note_is_the_page_cutoff();
	test_stale_artifacts_are_retired();
	test_live_slru_mirror_follows_page_history();
	test_independent_blocks_keep_their_newest();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
