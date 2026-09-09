/*
 * Focused core test for bounded fork-lifecycle history.
 *
 * Repeated write/truncate/regrow and drop/recreate churn must not grow the
 * retained state without bound: image compaction drops page versions that a
 * later truncate or drop invalidates at every horizon they serve, and
 * forkmeta compaction then keeps only the base, the inheritance fence, and
 * the growth per horizon instead of every historical boundary.  A definitive
 * event stays while a retained page version, or a still-indexed WAL record,
 * of a block it kills predates it, and a pinned reader keeps the history it
 * can see.
 *
 * Uses the shared core directly; no daemon, no PostgreSQL.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_forkmeta_snapshot.h"

#define TEST_SNAPSHOT_MAGIC 0x31534d46U

typedef struct TestSnapshotHeader
{
	uint32_t	magic;
	uint16_t	version;
	uint16_t	header_bytes;
	uint32_t	part;
	uint32_t	record_bytes;
	uint64_t	generation;
	uint64_t	cutoff_lsn;
	uint64_t	cutoff_admission_seq;
	uint64_t	freeze_admission_seq;
	uint64_t	checkpoint_records;
	uint64_t	tail_records;
	uint64_t	checkpoint_bytes;
	uint64_t	tail_bytes;
} TestSnapshotHeader;

typedef struct TestForkMetaRecV2
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint64_t	order_id;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} TestForkMetaRecV2;

static int checks;
static int failed;
static const PsKey rel_key = {1, 1, 7, 0, PS_KLASS_RELATION};

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
	char		command[512];

	if (snprintf(command, sizeof(command), "rm -rf -- '%s'", path) > 0 &&
		system(command) != 0)
		fprintf(stderr, "warning: could not remove %s\n", path);
}

static void
configure_core(void)
{
	page_size = 8192;
	segment_size = 65536;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 1;
	cache_pages = 0;
	use_layers = 1;
	ps_nshards = 1;
	ps_storage = &PsStoragePosix;
}

static int
write_page(uint32_t block, uint64_t lsn, unsigned char tag)
{
	unsigned char page[8192];
	uint32_t	hi = (uint32_t) (lsn >> 32);
	uint32_t	lo = (uint32_t) lsn;
	int			rc;

	memset(page, tag, sizeof(page));
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(&rel_key));
	rc = append_page(0, &rel_key, block, page, lsn, NULL);
	ps_unlock_shard(ps_shard_of(&rel_key));
	ps_admission_read_unlock();
	return rc == 0 && ps_storage->sync() == 0;
}

static int
fork_op(PsOpcode opcode, uint64_t lsn, uint32_t nblocks, uint32_t block)
{
	PsChannel	ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.key = rel_key;
	ch.req_lsn = lsn;
	ch.nblocks = nblocks;
	ch.blocknum = block;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(&rel_key));
	(void) ps_handle_meta(&ch);
	ps_unlock_shard(ps_shard_of(&rel_key));
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
reserve_pin(uint32_t kind, uint64_t owner_id, uint32_t generation,
			uint64_t lsn)
{
	PsChannel	ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch.blocknum = kind;
	ch.parent_timeline = PS_RETENTION_RESOURCE_ALL;
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
drop_pin(uint32_t kind, uint64_t owner_id, uint32_t generation)
{
	PsChannel	ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_RETENTION_PIN_DROP;
	ch.blocknum = kind;
	ch.old_nblocks = generation;
	ch.req_seq = owner_id;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	(void) ps_handle_meta(&ch);
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

/* 1 = a page with this tag is visible, 0 = no content, -1 = error. */
static int
read_tag_at(uint32_t block, uint64_t lsn, unsigned char *tag_out)
{
	unsigned char page[8192];
	int			rc;

	ps_lifecycle_read_lock();
	ps_lock_shard_rd(ps_shard_of(&rel_key));
	rc = read_resolve(0, &rel_key, block, lsn, 0, page, NULL);
	ps_unlock_shard(ps_shard_of(&rel_key));
	ps_lifecycle_read_unlock();
	if (rc == 1)
		*tag_out = page[100];
	return rc;
}

/* Lifecycle churn on unrelated relations, so the byte-triggered snapshot
 * publishes without touching the relation under test. */
static void
churn_other_relations(uint64_t *lsn)
{
	for (uint32_t i = 0; i < 12; i++)
	{
		PsChannel	ch;
		PsKey		key = rel_key;

		key.relNumber = 100 + i;
		memset(&ch, 0, sizeof(ch));
		ch.opcode = PS_OP_CREATE;
		ch.key = key;
		ch.req_lsn = *lsn += 10;
		ch.status = PS_STATUS_OK;
		ps_lifecycle_read_lock();
		ps_admission_read_lock();
		ps_lock_shard_wr(ps_shard_of(&key));
		(void) ps_handle_meta(&ch);
		ch.opcode = PS_OP_UNLINK;
		ch.req_lsn = *lsn += 10;
		ch.status = PS_STATUS_OK;
		(void) ps_handle_meta(&ch);
		ps_unlock_shard(ps_shard_of(&key));
		ps_admission_read_unlock();
		ps_lifecycle_read_unlock();
	}
}

static uint64_t
selected_generation(const char *store)
{
	char		directory[1024];
	PsForkmetaSnapshot selected;
	uint64_t	generation = 0;

	memset(&selected, 0, sizeof(selected));
	selected.directory_fd = selected.checkpoint_fd = selected.tail_fd = -1;
	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots",
				 store) > 0 &&
		ps_forkmeta_snapshot_open(&selected, directory) == 0)
	{
		generation = selected.generation;
		ps_forkmeta_snapshot_close(&selected);
	}
	return generation;
}

/* Drive maintenance until a newer forkmeta generation is selected.  A cutoff
 * that is not provable yet backs the snapshot off by one second, so the loop
 * spans that retry. */
static int
run_maintenance_until_generation(const char *store, uint64_t previous)
{
	for (int i = 0; i < 160; i++)
	{
		(void) ps_core_maintenance();
		if (selected_generation(store) > previous)
			return 1;
		usleep(20000);
	}
	return selected_generation(store) > previous;
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

/* Lifecycle records (GROW/SET/DEAD) retained for the relation in the selected
 * forkmeta snapshot, or -1. */
static int
snapshot_lifecycle_records(const char *store)
{
	char		directory[1024];
	PsForkmetaSnapshot selected;
	int			found = 0;

	memset(&selected, 0, sizeof(selected));
	selected.directory_fd = selected.checkpoint_fd = selected.tail_fd = -1;
	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots",
				 store) < 0 ||
		ps_forkmeta_snapshot_open(&selected, directory) != 0)
		return -1;
	for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		TestSnapshotHeader header;
		uint64_t	records;

		if (ps_forkmeta_snapshot_read(&selected, part, 0, &header,
									  sizeof(header)) != 0 ||
			header.magic != TEST_SNAPSHOT_MAGIC)
		{
			found = -1;
			break;
		}
		records = part == 0 ? header.checkpoint_records : header.tail_records;
		for (uint64_t i = 0; i < records; i++)
		{
			TestForkMetaRecV2 rec;

			if (ps_forkmeta_snapshot_read(&selected, part,
									  sizeof(header) + i * sizeof(rec), &rec,
									  sizeof(rec)) != 0)
			{
				found = -1;
				break;
			}
			if (rec.timeline == 0 && rec.kind <= 2 &&
				memcmp(&rec.key, &rel_key, sizeof(rel_key)) == 0)
			{
				found++;
				if (getenv("PAGESTORE_LIFECYCLE_TRACE") != NULL)
					fprintf(stderr, "  rec part=%u kind=%u lsn=%llu seq=%llu nblocks=%u\n",
							part, rec.kind, (unsigned long long) rec.lsn,
							(unsigned long long) rec.admission_seq, rec.nblocks);
			}
		}
		if (found < 0)
			break;
	}
	ps_forkmeta_snapshot_close(&selected);
	return found;
}

static uint64_t
directory_bytes(const char *path)
{
	DIR		   *dir = opendir(path);
	struct dirent *ent;
	uint64_t	total = 0;

	if (dir == NULL)
		return 0;
	while ((ent = readdir(dir)) != NULL)
	{
		char		child[2048];
		struct stat st;

		if (ent->d_name[0] == '.')
			continue;
		snprintf(child, sizeof(child), "%s/%s", path, ent->d_name);
		if (stat(child, &st) == 0 && S_ISREG(st.st_mode))
			total += (uint64_t) st.st_size;
	}
	closedir(dir);
	return total;
}

/* Twelve write/truncate/regrow cycles: the retained lifecycle history and the
 * snapshot bytes stay flat, invalidated versions leave the image layers, and
 * every read answers exactly as before. */
static void
test_truncate_churn_is_bounded(void)
{
	char		store[] = "/tmp/pagestore-lifecycle-churn-XXXXXX";
	char		snapshots[1100];
	uint64_t	lsn = 1000;
	unsigned char tag = 0;
	int			records_after_six = -1;
	int			records_after_twelve = -1;
	uint64_t	bytes_after_six = 0;
	uint64_t	bytes_after_twelve = 0;

	configure_core();
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm a small forkmeta snapshot trigger");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for truncate churn");
	snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(fork_op(PS_OP_CREATE, lsn, 0, 0), "create the relation");
	for (int cycle = 1; cycle <= 12; cycle++)
	{
		uint64_t	generation = selected_generation(store);

		/* grow to four blocks, write them, then truncate to one */
		check(fork_op(PS_OP_ZEROEXTEND, lsn += 10, 4, 0),
			  "regrow the relation to four blocks");
		for (uint32_t b = 0; b < 4; b++)
			check(write_page(b, lsn += 10, (unsigned char) (0x10 + cycle)),
				  "write a block in the cycle");
		check(fork_op(PS_OP_TRUNCATE, lsn += 10, 1, 0),
			  "truncate the relation to one block");
		check(fork_op(PS_OP_ZEROEXTEND, lsn += 10, 3, 1),
			  "regrow the truncated blocks as zero pages");
		churn_other_relations(&lsn);
		/* the materializer's cutoff follows every cycle */
		check(reserve_pin(PS_RETENTION_OWNER_MATERIALIZER, 1, 1, lsn += 10),
			  "advance the materializer cutoff");
		run_maintenance(8);
		/* The first cycles have no durable page frontier yet; once compaction
		 * has dropped a version the cutoff is provable every cycle. */
		if (cycle >= 3)
			check(run_maintenance_until_generation(store, generation),
				  "each cycle publishes a compacted forkmeta generation");
		if (cycle == 6 || cycle == 12)
		{
			int			records = snapshot_lifecycle_records(store);
			uint64_t	bytes = directory_bytes(snapshots);

			if (getenv("PAGESTORE_LIFECYCLE_TRACE") != NULL)
				fprintf(stderr, "cycle %d: records=%d bytes=%llu\n", cycle, records,
						(unsigned long long) bytes);
			if (cycle == 6)
			{
				records_after_six = records;
				bytes_after_six = bytes;
			}
			else
			{
				records_after_twelve = records;
				bytes_after_twelve = bytes;
			}
		}
		/* Reads: block 0 keeps the newest write, blocks 1..3 are zero pages
		 * after the regrow even though older versions were written. */
		check(read_tag_at(0, lsn, &tag) == 1 && tag == (unsigned char) (0x10 + cycle),
			  "block 0 reads its newest version after the cycle");
		for (uint32_t b = 1; b < 4; b++)
			check(read_tag_at(b, lsn, &tag) == 0,
				  "regrown block has no content at the cutoff");
	}
	check(records_after_six > 0 && records_after_twelve > 0 &&
		  records_after_twelve <= records_after_six &&
		  records_after_twelve <= 6,
		  "retained lifecycle records do not grow with truncate churn");
	check(bytes_after_six > 0 && bytes_after_twelve <= bytes_after_six + 1024,
		  "forkmeta snapshot bytes stay flat across truncate churn");
	/* The daemon's own restart must see the same answers. */
	close_store();
	configure_core();
	check(ps_core_open(store) == 0, "reopen the churned store");
	check(read_tag_at(0, lsn, &tag) == 1 && tag == 0x10 + 12,
		  "block 0 survives restart with its newest version");
	for (uint32_t b = 1; b < 4; b++)
		check(read_tag_at(b, lsn, &tag) == 0,
			  "regrown block still has no content after restart");
	close_store();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

/* A reader pinned before a truncate keeps the pre-truncate version and its
 * invalidation fence; dropping the pin releases both. */
static void
test_reader_pin_keeps_invalidated_history(void)
{
	char		store[] = "/tmp/pagestore-lifecycle-reader-XXXXXX";
	unsigned char tag = 0;
	int			records_pinned;
	int			records_released;

	configure_core();
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm a small forkmeta snapshot trigger for the reader test");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for reader retention");
	/* Two versions of block 0 give image compaction something to drop, which
	 * publishes the durable page frontier the forkmeta cutoff needs. */
	check(fork_op(PS_OP_CREATE, 1000, 0, 0) &&
		  fork_op(PS_OP_ZEROEXTEND, 1010, 2, 0) &&
		  write_page(0, 1012, 0xB0) && write_page(0, 1016, 0xB1) &&
		  write_page(1, 1020, 0xA1),
		  "write block 1 before the reader pins");
	check(reserve_pin(PS_RETENTION_OWNER_READER, 7, 1, 1500),
		  "reader pins after the write and before the truncate");
	check(fork_op(PS_OP_TRUNCATE, 2000, 1, 0) &&
		  fork_op(PS_OP_ZEROEXTEND, 2010, 1, 1) &&
		  fork_op(PS_OP_TRUNCATE, 2100, 1, 0) &&
		  fork_op(PS_OP_ZEROEXTEND, 2110, 1, 1),
		  "truncate and regrow twice above the pinned reader");
	{
		uint64_t	lsn = 2200;

		churn_other_relations(&lsn);
	}
	{
		uint64_t	generation = selected_generation(store);

		check(reserve_pin(PS_RETENTION_OWNER_MATERIALIZER, 1, 1, 3000),
			  "publish the cutoff above the churn");
		run_maintenance(8);
		check(run_maintenance_until_generation(store, generation),
			  "a compacted generation publishes while the reader is pinned");
	}
	check(read_tag_at(1, 1500, &tag) == 1 && tag == 0xA1,
		  "the pinned reader still sees its pre-truncate version");
	check(read_tag_at(1, 3000, &tag) == 0,
		  "the cutoff sees the regrown zero page");
	records_pinned = snapshot_lifecycle_records(store);
	check(drop_pin(PS_RETENTION_OWNER_READER, 7, 1), "reader drops its pin");
	{
		uint64_t	lsn = 3000;

		churn_other_relations(&lsn);
	}
	{
		uint64_t	generation = selected_generation(store);
		uint64_t	lsn = 3300;

		check(reserve_pin(PS_RETENTION_OWNER_MATERIALIZER, 1, 1, 3400),
			  "the cutoff advances after the reader leaves");
		run_maintenance(8);
		check(run_maintenance_until_generation(store, generation),
			  "a generation publishes after the reader leaves");
		/* Image compaction publishes the advanced page frontier on its own
		 * maintenance tick; the next size-triggered generation then compacts
		 * against it. */
		churn_other_relations(&lsn);
		generation = selected_generation(store);
		check(reserve_pin(PS_RETENTION_OWNER_MATERIALIZER, 1, 1, 3600),
			  "the cutoff advances once more");
		run_maintenance(8);
		check(run_maintenance_until_generation(store, generation),
			  "a compacted generation publishes against the advanced frontier");
	}
	records_released = snapshot_lifecycle_records(store);
	check(records_pinned > 0 && records_released > 0 &&
		  records_released < records_pinned,
		  "releasing the reader retires the fence it needed");
	check(read_tag_at(1, 3600, &tag) == 0,
		  "the regrown block still has no content after the fence retired");
	close_store();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

static int
create_branch(uint32_t timeline, uint32_t parent, uint64_t lsn)
{
	PsChannel	ch;

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
branch_fork_op(uint32_t timeline, PsOpcode opcode, uint64_t lsn,
			   uint32_t nblocks, uint32_t block)
{
	PsChannel	ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.timeline = timeline;
	ch.key = rel_key;
	ch.req_lsn = lsn;
	ch.nblocks = nblocks;
	ch.blocknum = block;
	ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(&rel_key));
	(void) ps_handle_meta(&ch);
	ps_unlock_shard(ps_shard_of(&rel_key));
	ps_admission_read_unlock();
	ps_lifecycle_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static uint64_t
selected_cutoff_lsn(const char *store)
{
	char		directory[1024];
	PsForkmetaSnapshot selected;
	uint64_t	cutoff = 0;

	memset(&selected, 0, sizeof(selected));
	selected.directory_fd = selected.checkpoint_fd = selected.tail_fd = -1;
	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots",
				 store) > 0 &&
		ps_forkmeta_snapshot_open(&selected, directory) == 0)
	{
		cutoff = selected.cutoff_lsn;
		ps_forkmeta_snapshot_close(&selected);
	}
	return cutoff;
}

/* A branch without a page frontier keeps every record but caps the cutoff
 * at its fork point, so its own lower-LSN mutations stay admissible while the
 * root keeps compacting. */
static void
test_frontier_less_branch_caps_cutoff(void)
{
	char		store[] = "/tmp/pagestore-lifecycle-branch-XXXXXX";
	uint64_t	lsn = 1000;
	unsigned char tag = 0;
	uint64_t	generation;

	configure_core();
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm a small forkmeta snapshot trigger for the branch test");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open store for the branch cutoff test");
	check(fork_op(PS_OP_CREATE, lsn, 0, 0) &&
		  fork_op(PS_OP_ZEROEXTEND, lsn += 10, 2, 0) &&
		  write_page(0, lsn += 10, 0xC0) && write_page(0, lsn += 10, 0xC1),
		  "write the root relation before forking");
	check(create_branch(1, 0, lsn += 10), "fork a branch at the current LSN");
	{
		uint64_t	fork_lsn = lsn;

		/* Root churn far above the fork point, with a materializer cutoff. */
		for (int cycle = 0; cycle < 6; cycle++)
		{
			generation = selected_generation(store);
			check(write_page(0, lsn += 1000, (unsigned char) (0xD0 + cycle)),
				  "write the root above the fork point");
			churn_other_relations(&lsn);
			check(reserve_pin(PS_RETENTION_OWNER_MATERIALIZER, 1, 1, lsn += 10),
				  "advance the root cutoff above the fork point");
			run_maintenance(8);
			if (cycle >= 2)
				check(run_maintenance_until_generation(store, generation),
					  "root keeps publishing generations with a live branch");
		}
		check(selected_cutoff_lsn(store) != 0 &&
			  selected_cutoff_lsn(store) <= fork_lsn,
			  "the selected cutoff never passes the live branch's fork point");
		/* The branch's own stream starts at its fork point. */
		check(branch_fork_op(1, PS_OP_ZEROEXTEND, fork_lsn + 5, 3, 2),
			  "the frontier-less branch extends at its own low LSN");
		check(branch_fork_op(1, PS_OP_TRUNCATE, fork_lsn + 15, 1, 0),
			  "the frontier-less branch truncates at its own low LSN");
		check(read_tag_at(0, lsn, &tag) == 1,
			  "root reads stay intact beside the branch");
	}
	close_store();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

int
main(void)
{
	test_truncate_churn_is_bounded();
	test_reader_pin_keeps_invalidated_history();
	test_frontier_less_branch_caps_cutoff();
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
