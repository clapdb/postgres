#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_fault.h"
#include "pagestore_forkmeta_snapshot.h"
#include "pagestore_manifest.h"
#include "pagestore_prune.h"
#include "pagestore_retention.h"

static int checks;
static int failed;

#define TEST_FORK_META_V2_MAGIC 0x324d4b46U
#define TEST_FORK_META_V3_MAGIC 0x334d4b46U
#define TEST_FORK_META_SNAPSHOT_PAYLOAD_MAGIC 0x31534d46U
#define TEST_FEV_GROW 0
#define TEST_FEV_SET 1
#define TEST_FEV_DEAD 2
#define TEST_FEV_SEG_GROW 5
#define TEST_FEV_SEG_COMMIT 6
#define TEST_FEV_SEG_GROW_BOUND 7
#define TEST_FEV_SEG_COMMIT_BOUND 8
#define TEST_FEV_SEG_ID 9
#define TEST_FEV_MIGRATED 3
#define TEST_FEV_SNAPSHOT_BASE 10
#define TEST_SEG_WALLESS_ORDERED_MAGIC 0x53454731U
#define TEST_SEG_CLAMPED_ORDERED_MAGIC 0x53454733U
#define TEST_SEG_WALLESS_BOUND_MAGIC 0x53454734U
#define TEST_SEG_MAGIC 0x53454732U
#define TEST_SEG_WALLESS_MAGIC 0x53454730U
#define TEST_SEG_CLAMPED_BOUND_MAGIC 0x53454735U
#define TEST_SEG_ADMISSION_MAGIC 0x53454736U
#define TEST_SEG_WALLESS_ADMISSION_MAGIC 0x53454737U
#define TEST_SEG_CLAMPED_ADMISSION_MAGIC 0x53454738U
#define TEST_SEG_HOLE48_MAGIC 0x53454830U
#define TEST_SEG_HOLE56_MAGIC 0x53454831U
#define TEST_SEG_HOLE64_MAGIC 0x53454832U
#define TEST_MAX_TIMELINES 1024

typedef struct TestForkMetaRecV1
{
	uint32_t timeline;
	PsKey key;
	uint64_t lsn;
	uint32_t nblocks;
	uint8_t kind;
	uint8_t pad[3];
} TestForkMetaRecV1;

typedef struct TestForkMetaRecV2
{
	uint32_t magic;
	uint32_t rec_len;
	uint32_t timeline;
	PsKey key;
	uint64_t lsn;
	uint64_t admission_seq;
	uint64_t order_id;
	uint32_t nblocks;
	uint8_t kind;
	uint8_t pad[3];
} TestForkMetaRecV2;

typedef struct TestSnapshotHeader
{
	uint32_t magic;
	uint16_t version;
	uint16_t header_bytes;
	uint32_t part;
	uint32_t record_bytes;
	uint64_t generation;
	uint64_t cutoff_lsn;
	uint64_t cutoff_admission_seq;
	uint64_t freeze_admission_seq;
	uint64_t checkpoint_records;
	uint64_t tail_records;
	uint64_t checkpoint_bytes;
	uint64_t tail_bytes;
} TestSnapshotHeader;

typedef struct TestSegRecHdr
{
	uint32_t magic;
	uint32_t timeline;
	PsKey key;
	uint32_t block;
	uint64_t lsn;
	uint32_t len;
} TestSegRecHdr;

typedef struct TestSegRecHdrBound
{
	TestSegRecHdr hdr;
	uint64_t order_id;
} TestSegRecHdrBound;

/*
 * Scan a segment end to end, mirroring recover()'s own walk: every record,
 * live or a SEG_HOLE*-magic tombstone (invariant I3, see
 * page_cleanup_tombstone_segment() in pagestore_core.c), is self-describing
 * from its magic alone, so header_size + hdr.len always reaches the next
 * record.  Reports the hole count and, if 'target_timeline' is nonnegative,
 * the count of *non-hole* records still belonging to that timeline (which
 * the Q1 crash-matrix follow-up asserts is zero: every target record must be
 * a hole).  Returns 0 on success, -1 if a record's magic is unrecognized.
 */
static int
scan_segment_holes(int shard, int seg, int64_t size,
					int64_t target_timeline, int64_t *holes_out,
					int64_t *target_live_out)
{
	int64_t		off = 0;
	int64_t		holes = 0;
	int64_t		target_live = 0;

	while (off + (int64_t) sizeof(TestSegRecHdr) <= size)
	{
		TestSegRecHdr hdr;
		uint64_t	header_size;
		int			is_hole;
		int			bound;
		int			admission;

		if (ps_storage->seg_read(shard, seg, (uint64_t) off, &hdr,
								  sizeof(hdr)) != 0)
			return -1;
		is_hole = hdr.magic == TEST_SEG_HOLE48_MAGIC ||
			hdr.magic == TEST_SEG_HOLE56_MAGIC ||
			hdr.magic == TEST_SEG_HOLE64_MAGIC;
		if (is_hole)
		{
			holes++;
			header_size = hdr.magic == TEST_SEG_HOLE48_MAGIC ?
				sizeof(TestSegRecHdr) :
				hdr.magic == TEST_SEG_HOLE56_MAGIC ?
					sizeof(TestSegRecHdr) + sizeof(uint64_t) :
					sizeof(TestSegRecHdr) + 2 * sizeof(uint64_t);
		}
		else
		{
			bound = hdr.magic == TEST_SEG_WALLESS_BOUND_MAGIC ||
				hdr.magic == TEST_SEG_CLAMPED_BOUND_MAGIC ||
				hdr.magic == TEST_SEG_WALLESS_ADMISSION_MAGIC ||
				hdr.magic == TEST_SEG_CLAMPED_ADMISSION_MAGIC;
			admission = hdr.magic == TEST_SEG_ADMISSION_MAGIC ||
				hdr.magic == TEST_SEG_WALLESS_ADMISSION_MAGIC ||
				hdr.magic == TEST_SEG_CLAMPED_ADMISSION_MAGIC;
			if (hdr.magic != TEST_SEG_MAGIC &&
				hdr.magic != TEST_SEG_WALLESS_MAGIC &&
				hdr.magic != TEST_SEG_WALLESS_ORDERED_MAGIC &&
				hdr.magic != TEST_SEG_CLAMPED_ORDERED_MAGIC &&
				hdr.magic != TEST_SEG_WALLESS_BOUND_MAGIC &&
				hdr.magic != TEST_SEG_CLAMPED_BOUND_MAGIC &&
				hdr.magic != TEST_SEG_ADMISSION_MAGIC &&
				hdr.magic != TEST_SEG_WALLESS_ADMISSION_MAGIC &&
				hdr.magic != TEST_SEG_CLAMPED_ADMISSION_MAGIC)
				return -1;
			header_size = sizeof(TestSegRecHdr) +
				(bound ? sizeof(uint64_t) : 0) +
				(admission ? sizeof(uint64_t) : 0);
			if (target_timeline >= 0 &&
				(int64_t) hdr.timeline == target_timeline)
				target_live++;
		}
		off += (int64_t) header_size + (int64_t) hdr.len;
	}
	if (holes_out != NULL)
		*holes_out = holes;
	if (target_live_out != NULL)
		*target_live_out = target_live;
	return 0;
}

static void
remove_tree(const char *path)
{
	DIR *dir = opendir(path);
	struct dirent *entry;

	if (dir == NULL)
	{
		(void) unlink(path);
		return;
	}
	while ((entry = readdir(dir)) != NULL)
	{
		char child[1200];
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) < 0 ||
			lstat(child, &st) != 0)
			continue;
		if (S_ISDIR(st.st_mode))
			remove_tree(child);
		else
			(void) unlink(child);
	}
	closedir(dir);
	(void) rmdir(path);
}

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

static int
meta_request_timeline(uint32_t timeline, PsOpcode opcode, const PsKey *key,
					  uint64_t lsn, uint64_t seq, uint32_t nblocks,
					  uint32_t blocknum, PsChannel *result)
{
	PsChannel ch;
	uint32_t shard = ps_shard_of(key);
	int write_op = opcode == PS_OP_CREATE || opcode == PS_OP_UNLINK ||
		opcode == PS_OP_TRUNCATE || opcode == PS_OP_ZEROEXTEND;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.timeline = timeline;
	ch.key = *key;
	ch.req_lsn = lsn;
	ch.req_seq = seq;
	ch.nblocks = nblocks;
	ch.blocknum = blocknum;
	ch.status = PS_STATUS_OK;
	if (write_op)
		ps_admission_read_lock();
	if (write_op)
		ps_lock_shard_wr(shard);
	else
	{
		ps_lock_shard_rd(shard);
		ps_lock_map_rd();
	}
	(void) ps_handle_meta(&ch);
	if (!write_op)
		ps_unlock_map();
	ps_unlock_shard(shard);
	if (write_op)
		ps_admission_read_unlock();
	if (result)
		*result = ch;
	return ch.status == PS_STATUS_OK;
}

static int
meta_request(PsOpcode opcode, const PsKey *key, uint64_t lsn,
			 uint64_t seq, uint32_t nblocks, uint32_t blocknum,
			 PsChannel *result)
{
	return meta_request_timeline(0, opcode, key, lsn, seq, nblocks,
							 blocknum, result);
}

static int
create_branch_request(uint32_t timeline, uint32_t parent, uint64_t lsn)
{
	PsChannel ch;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = PS_OP_CREATE_BRANCH;
	ch.timeline = timeline;
	ch.parent_timeline = parent;
	ch.req_lsn = lsn;
	ch.status = PS_STATUS_OK;
	ps_admission_read_lock();
	ps_lock_map_wr();
	(void) ps_handle_meta(&ch);
	ps_unlock_map();
	ps_admission_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
append_relation_tag(const PsKey *key, uint32_t block, uint64_t lsn,
					unsigned char *page, unsigned char tag, uint64_t *seq)
{
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;
	int rc;

	memset(page, 0, page_size);
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	page[128] = tag;
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(key));
	rc = append_page(0, key, block, page, 0, seq);
	ps_unlock_shard(ps_shard_of(key));
	ps_admission_read_unlock();
	return rc;
}

static int
append_relation(const PsKey *key, uint32_t block, uint64_t lsn,
				unsigned char *page, uint64_t *seq)
{
	return append_relation_tag(key, block, lsn, page, 0, seq);
}

static int
append_relation_timeline_tag(uint32_t timeline, const PsKey *key, uint32_t block,
							 uint64_t lsn, unsigned char *page, unsigned char tag,
							 uint64_t *seq)
{
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;
	int rc;

	memset(page, 0, page_size);
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	page[128] = tag;
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(key));
	rc = append_page(timeline, key, block, page, 0, seq);
	ps_unlock_shard(ps_shard_of(key));
	ps_admission_read_unlock();
	return rc;
}

static int
append_relation_timeline(uint32_t timeline, const PsKey *key, uint32_t block,
						 uint64_t lsn, unsigned char *page, uint64_t *seq)
{
	return append_relation_timeline_tag(timeline, key, block, lsn, page,
									   (unsigned char) (timeline + 0x20), seq);
}

static off_t
file_size(const char *path)
{
	struct stat st;

	return stat(path, &st) == 0 ? st.st_size : -1;
}

static int
run_maintenance_until(const char *path, int want_exists)
{
	for (unsigned int i = 0; i < 40; i++)
	{
		(void) ps_core_maintenance();
		if ((access(path, F_OK) == 0) == want_exists)
			return 1;
		usleep(100000);
	}
	return 0;
}

static int
snapshot_ordered_marker_count(const char *directory, const PsKey *key,
								  uint64_t admission_seq, int match_seq)
{
	PsForkmetaSnapshot selected = {.directory_fd = -1,
		.checkpoint_fd = -1, .tail_fd = -1};
	int found = 0;

	if (ps_forkmeta_snapshot_open(&selected, directory) != 0)
		return 0;
	for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		TestSnapshotHeader header;
		uint64_t records;

		if (ps_forkmeta_snapshot_read(&selected, part, 0, &header,
									  sizeof(header)) != 0)
			break;
		records = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
			header.checkpoint_records : header.tail_records;
		for (uint64_t i = 0; i < records; i++)
		{
			TestForkMetaRecV2 rec;

			if (ps_forkmeta_snapshot_read(&selected, part,
									  sizeof(header) + i * sizeof(rec), &rec,
									  sizeof(rec)) != 0)
				break;
			if ((rec.kind == TEST_FEV_SEG_GROW ||
				 rec.kind == TEST_FEV_SEG_COMMIT ||
				 rec.kind == TEST_FEV_SEG_GROW_BOUND ||
				 rec.kind == TEST_FEV_SEG_COMMIT_BOUND) &&
				(!match_seq || rec.admission_seq == admission_seq) &&
				memcmp(&rec.key, key, sizeof(*key)) == 0)
				found++;
		}
	}
	ps_forkmeta_snapshot_close(&selected);
	return found;
}

static int
snapshot_has_ordered_marker(const char *directory, const PsKey *key,
								uint64_t admission_seq)
{
	return snapshot_ordered_marker_count(directory, key, admission_seq, 1) != 0;
}

static int
snapshot_has_plain_grow(const char *directory, const PsKey *key)
{
	PsForkmetaSnapshot selected = {.directory_fd = -1,
		.checkpoint_fd = -1, .tail_fd = -1};
	int found = 0;

	if (ps_forkmeta_snapshot_open(&selected, directory) != 0)
		return 0;
	for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL && !found;
		 part++)
	{
		TestSnapshotHeader header;
		uint64_t records;

		if (ps_forkmeta_snapshot_read(&selected, part, 0, &header,
									  sizeof(header)) != 0)
			break;
		records = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
			header.checkpoint_records : header.tail_records;
		for (uint64_t i = 0; i < records; i++)
		{
			TestForkMetaRecV2 rec;

			if (ps_forkmeta_snapshot_read(&selected, part,
									  sizeof(header) + i * sizeof(rec), &rec,
									  sizeof(rec)) != 0)
				break;
			if (rec.kind == TEST_FEV_GROW && rec.order_id == 0 &&
				memcmp(&rec.key, key, sizeof(*key)) == 0)
			{
				found = 1;
				break;
			}
		}
	}
	ps_forkmeta_snapshot_close(&selected);
	return found;
}

static int
read_selected_header(const char *directory, TestSnapshotHeader *header)
{
	PsForkmetaSnapshot selected = {.directory_fd = -1,
		.checkpoint_fd = -1, .tail_fd = -1};
	int rc = -1;

	memset(&selected, 0, sizeof(selected));
	selected.directory_fd = selected.checkpoint_fd = selected.tail_fd = -1;
	if (ps_forkmeta_snapshot_open(&selected, directory) == 0 &&
		ps_forkmeta_snapshot_read(&selected, 0, 0, header,
								  sizeof(*header)) == 0 &&
		header->magic == TEST_FORK_META_SNAPSHOT_PAYLOAD_MAGIC &&
		header->generation == selected.generation &&
		header->cutoff_lsn == selected.cutoff_lsn &&
		header->cutoff_admission_seq == selected.cutoff_admission_seq)
		rc = 0;
	if (selected.directory_fd >= 0)
		ps_forkmeta_snapshot_close(&selected);
	return rc;
}

static int
source_is_marker_only(const char *store, TestForkMetaRecV2 *marker)
{
	char path[1024];
	int fd;
	ssize_t n;

	if (snprintf(path, sizeof(path), "%s/forkmeta", store) < 0 ||
		file_size(path) != (off_t) sizeof(*marker))
		return 0;
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return 0;
	n = read(fd, marker, sizeof(*marker));
	close(fd);
	return n == (ssize_t) sizeof(*marker) &&
		(marker->magic == TEST_FORK_META_V2_MAGIC ||
		 marker->magic == TEST_FORK_META_V3_MAGIC) &&
		marker->rec_len == sizeof(*marker) &&
		marker->kind == TEST_FEV_SNAPSHOT_BASE;
}

static int
append_source_bytes(const char *path, const void *data, size_t len)
{
	int fd = open(path, O_WRONLY | O_APPEND);
	int ok = fd >= 0 && write(fd, data, len) == (ssize_t) len && fsync(fd) == 0;

	if (fd >= 0 && close(fd) != 0)
		ok = 0;
	return ok;
}


/* The daemon seals what it writes as FKM3 (CRC-24 in the pad bytes).  A test
 * that edits a copied record must present it as a legacy FKM2 record, which
 * the loader still accepts, or its checksum would fail before the field it
 * exercises is even examined. */
static void
as_legacy_record(TestForkMetaRecV2 *rec)
{
	rec->magic = TEST_FORK_META_V2_MAGIC;
	memset(rec->pad, 0, sizeof(rec->pad));
}

static int
append_source_record(const char *path, const TestForkMetaRecV2 *record)
{
	return append_source_bytes(path, record, sizeof(*record));
}

static off_t
source_record_count(const char *path)
{
	off_t size = file_size(path);

	return size >= 0 && size % (off_t) sizeof(TestForkMetaRecV2) == 0 ?
		size / (off_t) sizeof(TestForkMetaRecV2) : -1;
}

static int
snapshot_timeline_record_count(const char *directory, uint32_t timeline)
{
	PsForkmetaSnapshot selected = {.directory_fd = -1,
		.checkpoint_fd = -1, .tail_fd = -1};
	int found = 0;

	if (ps_forkmeta_snapshot_open(&selected, directory) != 0)
		return -1;
	for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		TestSnapshotHeader header;
		uint64_t records;

		if (ps_forkmeta_snapshot_read(&selected, part, 0, &header,
									  sizeof(header)) != 0)
		{
			found = -1;
			break;
		}
		records = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
			header.checkpoint_records : header.tail_records;
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
			if (rec.timeline == timeline)
				found++;
		}
		if (found < 0)
			break;
	}
	ps_forkmeta_snapshot_close(&selected);
	return found;
}

static int
source_has_timeline_record(const char *path, uint32_t timeline)
{
	TestForkMetaRecV2 rec;
	int fd = open(path, O_RDONLY);
	int found = 0;

	if (fd < 0)
		return 0;
	while (read(fd, &rec, sizeof(rec)) == (ssize_t) sizeof(rec))
		if (rec.timeline == timeline)
		{
			found = 1;
			break;
		}
	close(fd);
	return found;
}

static int
read_last_source_record(const char *path, TestForkMetaRecV2 *record)
{
	off_t size = file_size(path);
	int fd = size >= (off_t) sizeof(*record) ? open(path, O_RDONLY) : -1;
	int ok = fd >= 0 && pread(fd, record, sizeof(*record),
							 size - (off_t) sizeof(*record)) == (ssize_t) sizeof(*record);

	if (fd >= 0 && close(fd) != 0)
		ok = 0;
	return ok;
}

static int
restore_marker_only(const char *path, size_t marker_size)
{
	int fd = open(path, O_WRONLY);
	int ok = fd >= 0 && ftruncate(fd, (off_t) marker_size) == 0 &&
		fsync(fd) == 0;

	if (fd >= 0 && close(fd) != 0)
		ok = 0;
	return ok;
}

static int
append_growth_batch_timeline(uint32_t timeline, uint32_t rel_base, uint64_t lsn_base)
{
	for (uint32_t i = 0; i < 20; i++)
	{
		PsKey key = {7, 7, rel_base + i, 0, PS_KLASS_RELATION};
		int rc;

		ps_admission_read_lock();
		ps_lock_shard_wr(ps_shard_of(&key));
		rc = fork_grow(timeline, &key, 1, lsn_base + i);
		ps_unlock_shard(ps_shard_of(&key));
		ps_admission_read_unlock();
		if (rc != 0)
			return 0;
	}
	return 1;
}

static int
append_growth_batch(uint32_t rel_base, uint64_t lsn_base)
{
	return append_growth_batch_timeline(0, rel_base, lsn_base);
}

static void
close_runtime(void)
{
	ps_core_close();
	if (ps_storage->close)
		ps_storage->close();
}

static int
begin_delete_timeline(uint32_t timeline)
{
	PsChannel state_ch;
	PsChannel delete_ch;
	int lifecycle_locked;

	memset(&state_ch, 0, sizeof(state_ch));
	state_ch.opcode = PS_OP_TIMELINE_STATE;
	state_ch.timeline = timeline;
	state_ch.status = PS_STATUS_OK;
	ps_lifecycle_read_lock();
	ps_lock_map_rd();
	(void) ps_handle_meta(&state_ch);
	ps_unlock_map();
	ps_lifecycle_read_unlock();
	if (state_ch.status != PS_STATUS_OK)
		return 0;
	memset(&delete_ch, 0, sizeof(delete_ch));
	delete_ch.opcode = PS_OP_BEGIN_DELETE;
	delete_ch.timeline = timeline;
	delete_ch.req_seq = state_ch.req_seq;
	delete_ch.status = PS_STATUS_OK;
	lifecycle_locked = ps_lifecycle_write_lock() == 0;
	if (!lifecycle_locked || ps_admission_write_lock() != 0)
	{
		if (lifecycle_locked)
			ps_lifecycle_write_unlock();
		return 0;
	}
	ps_lock_map_wr();
	(void) ps_handle_meta(&delete_ch);
	ps_unlock_map();
	ps_admission_write_unlock();
	ps_lifecycle_write_unlock();
	return delete_ch.status == PS_STATUS_OK &&
		delete_ch.result == PS_TIMELINE_DELETING;
}

static int
expect_open_failure(const char *store)
{
	pid_t pid = fork();
	int status;

	if (pid == 0)
	{
		int rc = ps_core_open(store);

		if (rc == 0)
			close_runtime();
		_exit(rc != 0 ? 0 : 1);
	}
	return pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static void
test_no_manifest_marker_only_rejected(void)
{
	char store[] = "/tmp/psforkmetanomftXXXXXX";
	TestForkMetaRecV2 marker;

	check(mkdtemp(store) != NULL, "create no-manifest marker-only store");
	memset(&marker, 0, sizeof(marker));
	marker.magic = TEST_FORK_META_V2_MAGIC;
	marker.rec_len = sizeof(marker);
	marker.lsn = 1;
	marker.admission_seq = 1;
	marker.order_id = 1;
	marker.kind = TEST_FEV_SNAPSHOT_BASE;
	check(PsStoragePosix.open(store, segment_size) == 0 &&
		  PsStoragePosix.fork_meta_rewrite(&marker, sizeof(marker)) == 0,
		  "install marker-only source without selected manifest");
	PsStoragePosix.close();
	check(expect_open_failure(store),
		  "no-manifest marker-only source fails startup closed");
	remove_tree(store);
}

static void
test_v1_bound_marker_snapshot(void)
{
	char store[] = "/tmp/psforkmetav1boundXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {5, 5, 5, 0, PS_KLASS_RELATION};
	PsKey lifecycle_key = {5, 5, 7, 0, PS_KLASS_RELATION};
	PsKey seg1_key = {5, 5, 8, 0, PS_KLASS_RELATION};
	PsKey seg3_key = {5, 5, 9, 0, PS_KLASS_RELATION};
	PsKey pin_key = {5, 5, 6, 0, PS_KLASS_RELATION};
	TestForkMetaRecV1 records[6];
	TestSegRecHdrBound bound_bodies[2];
	TestSegRecHdr legacy_bodies[2];
	PsRetentionPin pin;
	PsChannel reply;
	unsigned char page[8192];
	unsigned char legacy_pages[4][8192];
	uint64_t first_seq = 0;
	uint64_t second_seq = 0;
	int64_t seg_off;
	uint64_t offsets[4];
	int n;

	check(mkdtemp(store) != NULL, "create V1 bound-marker store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build V1 snapshot directory path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build V1 snapshot manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build V1 frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "open V1 fixture store before installing legacy body");
	memset(page, 0, sizeof(page));
	check(meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "write page history for V1 fixture frontier");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 77;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(first_seq != 0 && second_seq > first_seq &&
		  ps_retention_set(&pin) == PS_RETENTION_OK,
		  "install page-history pin for V1 snapshot fixture");
	check(run_maintenance_until(frontier, 1),
		  "publish safe frontier for V1 snapshot fixture");
	close_runtime();

	memset(records, 0, sizeof(records));
	for (int i = 0; i < 6; i++)
	{
		records[i].timeline = 0;
		records[i].nblocks = 1;
	}
	records[0].key = key;
	records[0].lsn = 200;
	records[0].kind = TEST_FEV_SEG_GROW_BOUND;
	records[1].key = key;
	records[1].lsn = 77;
	records[1].kind = TEST_FEV_SEG_ID;
	records[2].key = lifecycle_key;
	records[2].lsn = 150;
	records[2].kind = TEST_FEV_SEG_GROW_BOUND;
	records[3].key = lifecycle_key;
	records[3].lsn = 78;
	records[3].kind = TEST_FEV_SEG_ID;
	records[4].key = seg1_key;
	records[4].lsn = 160;
	records[4].kind = TEST_FEV_SEG_GROW;
	records[5].key = seg3_key;
	records[5].lsn = 170;
	records[5].kind = TEST_FEV_SEG_GROW;
	memset(bound_bodies, 0, sizeof(bound_bodies));
	for (int i = 0; i < 2; i++)
	{
		bound_bodies[i].hdr.magic = TEST_SEG_WALLESS_BOUND_MAGIC;
		bound_bodies[i].hdr.timeline = 0;
		bound_bodies[i].hdr.block = 0;
		bound_bodies[i].hdr.len = sizeof(page);
	}
	bound_bodies[0].hdr.key = key;
	bound_bodies[0].hdr.lsn = 200;
	bound_bodies[0].order_id = 77;
	bound_bodies[1].hdr.key = lifecycle_key;
	bound_bodies[1].hdr.lsn = 150;
	bound_bodies[1].order_id = 78;
	memset(legacy_bodies, 0, sizeof(legacy_bodies));
	legacy_bodies[0].magic = TEST_SEG_WALLESS_ORDERED_MAGIC;
	legacy_bodies[0].timeline = 0;
	legacy_bodies[0].key = seg1_key;
	legacy_bodies[0].lsn = 160;
	legacy_bodies[0].len = sizeof(page);
	legacy_bodies[1].magic = TEST_SEG_CLAMPED_ORDERED_MAGIC;
	legacy_bodies[1].timeline = 0;
	legacy_bodies[1].key = seg3_key;
	legacy_bodies[1].lsn = 170;
	legacy_bodies[1].len = sizeof(page);
	memset(legacy_pages, 0, sizeof(legacy_pages));
	legacy_pages[0][128] = 0x6d;
	legacy_pages[1][128] = 0x7d;
	legacy_pages[2][128] = 0x31;
	legacy_pages[3][128] = 0x33;
	check(PsStoragePosix.open(store, segment_size) == 0 &&
		  (seg_off = PsStoragePosix.seg_size(0, 0)) >= 0 &&
		  PsStoragePosix.fork_meta_append(records, sizeof(records)) == 0 &&
		  (offsets[0] = (uint64_t) seg_off, 1) &&
		  (offsets[1] = offsets[0] + sizeof(bound_bodies[0]) + sizeof(page), 1) &&
		  (offsets[2] = offsets[1] + sizeof(bound_bodies[1]) + sizeof(page), 1) &&
		  (offsets[3] = offsets[2] + sizeof(legacy_bodies[0]) + sizeof(page), 1) &&
		  PsStoragePosix.seg_write(0, 0, offsets[0], &bound_bodies[0],
								 sizeof(bound_bodies[0])) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[0] + sizeof(bound_bodies[0]),
								 legacy_pages[0], sizeof(page)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[1], &bound_bodies[1],
								 sizeof(bound_bodies[1])) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[1] + sizeof(bound_bodies[1]),
								 legacy_pages[1], sizeof(page)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[2], &legacy_bodies[0],
								 sizeof(legacy_bodies[0])) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[2] + sizeof(legacy_bodies[0]),
								 legacy_pages[2], sizeof(page)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[3], &legacy_bodies[1],
								 sizeof(legacy_bodies[1])) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[3] + sizeof(legacy_bodies[1]),
								 legacy_pages[3], sizeof(page)) == 0 &&
		  PsStoragePosix.sync() == 0,
		  "install V1 bound and unbound SEG1/SEG3 ordered bodies");
	PsStoragePosix.close();
	check(ps_core_open(store) == 0,
		  "open committed V1 ordered-page fixture");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x6d,
		  "V1 bound marker admits its ordered page before snapshot");
	check(read_resolve(0, &lifecycle_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x7d &&
		  read_resolve(0, &seg1_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x31 &&
		  read_resolve(0, &seg3_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x33,
		  "V1 SEG1/SEG3 and lifecycle bodies admit before snapshot");
	check(meta_request(PS_OP_UNLINK, &lifecycle_key, 150, 0, 0, 0, NULL) &&
		  meta_request(PS_OP_EXISTS, &lifecycle_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 0,
		  "append same-LSN lifecycle event after V1 sequence-zero marker");
	check(append_growth_batch(1800, 1500) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "publish snapshot containing V1 ordered admission");
	check(snapshot_has_ordered_marker(snapshots, &key, 0),
		  "snapshot preserves V1 bound marker with sequence zero");
	check(snapshot_has_ordered_marker(snapshots, &lifecycle_key, 0) &&
		  snapshot_has_ordered_marker(snapshots, &seg1_key, 0) &&
		  snapshot_has_ordered_marker(snapshots, &seg3_key, 0),
		  "snapshot preserves same-LSN and legacy SEG1/SEG3 admissions");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x6d &&
		  read_resolve(0, &seg1_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x31 &&
		  read_resolve(0, &seg3_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x33 &&
		  read_resolve(0, &lifecycle_key, 0, UINT64_MAX, 0, page, NULL) == 0,
		  "V1 bound, SEG1/SEG3, and same-LSN lifecycle survive restart");
	close_runtime();
	remove_tree(store);
}

static void
test_reclaimed_ordered_markers_pruned(void)
{
	char store[] = "/tmp/psforkmetamarkerpruneXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	unsigned char page[8192];
	uint64_t seq = 0;
	int markers;
	int n;

	check(mkdtemp(store) != NULL, "create ordered-marker pruning store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build marker-pruning snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build marker-pruning manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build marker-pruning frontier path");
	flush_pages = 1;
	compact_layers = 2;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &key, 100, 0, 0, 0, NULL),
		  "open marker-pruning fixture and create fork");
	check(append_relation_tag(&key, 0, 50, page, 0x40, &seq) == 0,
		  "write initial ordered growth before reviewer restart");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40,
		  "restart with activated marker-growth as sole durable size event");
	for (int i = 1; i < 12; i++)
		check(append_relation_tag(&key, 0, 50, page,
								  (unsigned char) (0x40 + i), &seq) == 0,
			  "write later ordered COMMIT for the same block");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 88;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = seq;
	check(seq != 0 && ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "compact and durably reclaim old ordered page identities");
	check(append_growth_batch(2200, 500) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "snapshot after ordered page-version reclamation");
	markers = snapshot_ordered_marker_count(snapshots, &key, 0, 0);
	check(markers > 0 && markers < 12 && snapshot_has_plain_grow(snapshots, &key),
		  "reclaimed growth marker becomes retained ordinary GROW while markers bound");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x4b &&
		  snapshot_ordered_marker_count(snapshots, &key, 0, 0) == markers,
		  "bounded ordered-marker snapshot restarts with latest page intact");
	close_runtime();
	remove_tree(store);
	compact_layers = 0;
}

/*
 * Design B: the in-memory event array is bounded per daemon lifetime, not
 * just per restart.  Same fixture shape as test_reclaimed_ordered_markers_pruned()
 * above (a growth marker that gets activated, 11 later inert commit markers
 * on the same block, then a reclaiming cutover), but this test checks the
 * IN-MEMORY counts (ps_test_fork_event_count()) instead of only the durable
 * snapshot.  Before this PR's fork_event_compact_dropped_markers(), the
 * in-memory ninert stays 11 forever in this process (the array only shrinks
 * on the next restart, per the old "keep the in-memory chain conservative"
 * comment) -- so the post-cutover assertion below is the fail-before/
 * pass-after case for this task.
 */
static void
test_inert_markers_compacted_after_cutover(void)
{
	char store[] = "/tmp/psforkmetamarkercompactXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 7, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	unsigned char page[8192];
	uint64_t seq = 0;
	uint32_t nevents_before = 0,
				nmarkers_before = 0,
				ninert_before = 0;
	uint32_t nevents_after = 0,
				nmarkers_after = 0,
				ninert_after = 0;
	uint32_t nevents_reopen = 0,
				nmarkers_reopen = 0,
				ninert_reopen = 0;
	int markers;
	int n;

	check(mkdtemp(store) != NULL, "create inert-marker compaction store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build compaction snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build compaction manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build compaction frontier path");
	flush_pages = 1;
	compact_layers = 2;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &key, 100, 0, 0, 0, NULL),
		  "open compaction fixture and create fork");
	check(append_relation_tag(&key, 0, 50, page, 0x40, &seq) == 0,
		  "write initial ordered growth before compaction restart");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40,
		  "restart with activated marker-growth as sole durable size event");
	for (int i = 1; i < 12; i++)
		check(append_relation_tag(&key, 0, 50, page,
								  (unsigned char) (0x40 + i), &seq) == 0,
			  "write later ordered COMMIT for the same block");
	check(ps_test_fork_event_count(0, &key, &nevents_before, &nmarkers_before,
									&ninert_before) &&
		  nevents_before == 13 && nmarkers_before == 12 &&
		  ninert_before == 11,
		  "the CREATE's SET plus 12 live writes (1 activated GROW, 11 inert "
		  "commit markers) sit in memory pre-cutover");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 88;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = seq;
	check(seq != 0 && ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "compact and durably reclaim old ordered page identities");
	check(append_growth_batch(2300, 600) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "snapshot after ordered page-version reclamation");
	markers = snapshot_ordered_marker_count(snapshots, &key, 0, 0);
	check(markers > 0 && markers < 11 && snapshot_has_plain_grow(snapshots, &key),
		  "reclaimed growth marker becomes retained ordinary GROW while markers bound");
	check(ps_test_fork_event_count(0, &key, &nevents_after, &nmarkers_after,
									&ninert_after) &&
		  ninert_after == (uint32_t) markers &&
		  ninert_after < ninert_before &&
		  nevents_after < nevents_before,
		  "post-cutover compaction drops exactly the durable snapshot's dropped "
		  "inert markers from memory (fail-before: ninert stayed 11 here)");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x4b,
		  "bounded ordered-marker snapshot restarts with latest page intact");
	/*
	 * nmarkers is not compared here: it counts marker_kind != 0, which the
	 * activated GROW keeps set (the durable ordered identity survives
	 * activation, on purpose -- see the ForkEvent.marker_kind comment) for
	 * as long as this daemon lifetime runs, but the checkpoint record for an
	 * activated GROW is an ordinary plain-GROW record (snapshot_has_plain_grow()
	 * above already established this), which the loader reconstructs with
	 * marker_kind == 0.  That asymmetry is pre-existing snapshot-builder
	 * behavior, not something design B's compaction touches; nevents and
	 * ninert are the counts the F5 in-memory/recovery equivalence is about,
	 * and both must match exactly.
	 */
	check(ps_test_fork_event_count(0, &key, &nevents_reopen, &nmarkers_reopen,
									&ninert_reopen) &&
		  nevents_reopen == nevents_after && ninert_reopen == ninert_after,
		  "recovery equivalence: reopen's in-memory nevents/ninert match the "
		  "compacted in-memory counts from before the restart");
	close_runtime();
	remove_tree(store);
	compact_layers = 0;
}

/*
 * Companion case: when no page version has been reclaimed, the builder
 * retains every ordered marker (fork_meta_snapshot_marker_page_retained()
 * is true for all of them), so fork_event_compact_dropped_markers() has
 * nothing flagged to drop.  In-memory counts must be unchanged by the
 * cutover.
 */
static void
test_inert_markers_kept_when_retained(void)
{
	char store[] = "/tmp/psforkmetamarkerretainXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 8, 0, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 21, 1, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t first_seq = 0,
				second_seq = 0,
				seq = 0;
	uint32_t nevents_before = 0,
				nmarkers_before = 0,
				ninert_before = 0;
	uint32_t nevents_after = 0,
				nmarkers_after = 0,
				ninert_after = 0;
	PsRetentionPin pin;
	int n;

	check(mkdtemp(store) != NULL, "create inert-marker retention store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build retention snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build retention manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build retention frontier path");
	flush_pages = 1;
	compact_layers = 0;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open retention fixture and write a pinned page to prove a cutoff");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 89;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a provable frontier without compacting any layer "
		  "(compact_layers == 0, so no page version is reclaimed)");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the fork under test above the published frontier");
	for (int i = 0; i < 12; i++)
		check(append_relation_tag(&key, 0, 50, page, (unsigned char) i, &seq) == 0,
			  "write ordered growth then later ordered COMMITs for the same block");
	check(ps_test_fork_event_count(0, &key, &nevents_before, &nmarkers_before,
									&ninert_before) &&
		  ninert_before == 11,
		  "12 live writes leave 11 inert commit markers before cutover");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "cutover with page versions unreclaimed");
	/*
	 * All 12 markers are retained durably, including the activated growth
	 * marker: fork_meta_snapshot_marker_page_retained() is true for its own
	 * (lsn, admission_seq, nblocks) too, since no page version was
	 * reclaimed, so the per-entry loop takes the marker_kind branch for it
	 * as well instead of falling through to the plain-GROW branch (compare
	 * test_inert_markers_compacted_after_cutover() above, where reclaiming
	 * flips exactly that one event to a plain GROW).
	 */
	check(snapshot_ordered_marker_count(snapshots, &key, 0, 0) == 12,
		  "every marker is retained durably (no version was reclaimed)");
	check(ps_test_fork_event_count(0, &key, &nevents_after, &nmarkers_after,
									&ninert_after) &&
		  nevents_after == nevents_before && nmarkers_after == nmarkers_before &&
		  ninert_after == ninert_before,
		  "every marker is retained (page versions unreclaimed): compaction "
		  "is a no-op");
	close_runtime();
	remove_tree(store);
}

/*
 * preserve_survivors == 1 (a deletion-forced generation with no operational
 * cutoff it can prove): every record is emitted for every surviving fork, so
 * nothing is ever flagged, and compaction must be a no-op even though a
 * cutover happened.  The root timeline (0) fork below deliberately never
 * gets a published page-prune frontier, so fork_meta_snapshot_cutoff()
 * cannot prove an operational cutoff (fork_meta_entry_exempt() only exempts
 * a non-root branch); deleting timeline 1 (with its own fork events) makes
 * the cutover forced.
 */
static void
test_inert_markers_kept_on_preserve_survivors(void)
{
	char store[] = "/tmp/psforkmetamarkerpreserveXXXXXX";
	char manifest[1200];
	PsKey key = {6, 6, 9, 0, PS_KLASS_RELATION};
	PsKey del_key = {6, 6, 10, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t seq = 0,
				del_seq = 0;
	uint32_t nevents_before = 0,
				nmarkers_before = 0,
				ninert_before = 0;
	uint32_t nevents_after = 0,
				nmarkers_after = 0,
				ninert_after = 0;
	int n;

	check(mkdtemp(store) != NULL, "create preserve-survivors compaction store");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_snapshots/forkmeta_manifest_v1",
				 store);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build preserve-survivors manifest path");
	flush_pages = 1;
	compact_layers = 0;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &key, 100, 0, 0, 0, NULL),
		  "open preserve-survivors fixture and create the root-timeline fork");
	for (int i = 0; i < 6; i++)
		check(append_relation_tag(&key, 0, 50, page, (unsigned char) i, &seq) == 0,
			  "write ordered growth then later ordered COMMITs, no frontier published");
	check(ps_test_fork_event_count(0, &key, &nevents_before, &nmarkers_before,
									&ninert_before) &&
		  ninert_before == 5,
		  "6 live writes leave 5 inert commit markers before the forced cutover");
	check(create_branch_request(1, 0, 1) &&
		  meta_request_timeline(1, PS_OP_CREATE, &del_key, 100, 0, 0, 0, NULL) &&
		  append_relation_timeline(1, &del_key, 0, 0, page, &del_seq) == 0,
		  "create a second timeline with its own fork event to force deletion");
	check(begin_delete_timeline(1),
		  "begin deleting the second timeline");
	check(run_maintenance_until(manifest, 1),
		  "deletion-forced cutover publishes without a provable operational cutoff");
	check(ps_test_fork_event_count(0, &key, &nevents_after, &nmarkers_after,
									&ninert_after) &&
		  nevents_after == nevents_before && nmarkers_after == nmarkers_before &&
		  ninert_after == ninert_before,
		  "preserve_survivors retains every record: compaction is a no-op");
	close_runtime();
	remove_tree(store);
}

/*
 * Review finding (Low-1): a fork can reach a later build's
 * "if (deleting) continue;" skip with a STALE snapshot_dropped == 1 from an
 * EARLIER build that flagged it (while its timeline was not yet deleting)
 * and then failed after that per-entry loop ran but before
 * fork_event_compact_dropped_markers() could act on the flags (prepare
 * fails -> fork_meta_snapshot_maintenance() takes the retry_done path,
 * rc stays 0, compaction never runs for that attempt).  If the timeline
 * then enters DELETING before the next successful build, that build skips
 * the fork wholesale and, without explicitly clearing the flag there too,
 * would leave the stale 1 for the compaction pass that follows THIS
 * build's own success to wrongly act on -- removing an inert marker from a
 * fork this generation is supposed to leave untouched.  Reproduces exactly
 * that sequence: write ordered markers on a branch timeline, reclaim some
 * of them so a cutover would flag them, fail that cutover at the prepare
 * stage (PAGESTORE_TEST_FAIL_FORKMETA_PREPARED_BEFORE_CREATE, which fails
 * after fork_meta_snapshot_build()'s per-entry loop already ran but before
 * any compaction), then delete the timeline and let the next cutover
 * succeed; asserts the fork's in-memory counts are unchanged by that
 * second, successful cutover.
 */
static void
test_deleting_fork_flags_cleared_on_failed_build_skip(void)
{
	char store[] = "/tmp/psforkmetaflagclearXXXXXX";
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 12, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	unsigned char page[8192];
	uint64_t seq = 0;
	uint32_t nevents_before = 0,
				nmarkers_before = 0,
				ninert_before = 0;
	uint32_t nevents_after = 0,
				nmarkers_after = 0,
				ninert_after = 0;
	int n;

	check(mkdtemp(store) != NULL, "create flag-clear-on-skip store");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_snapshots/forkmeta_manifest_v1",
				 store);
	check(n > 0 && (size_t) n < sizeof(manifest), "build flag-clear manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build flag-clear frontier path");
	flush_pages = 1;
	compact_layers = 2;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  create_branch_request(1, 0, 1) &&
		  meta_request_timeline(1, PS_OP_CREATE, &key, 100, 0, 0, 0, NULL),
		  "open store, branch timeline 1, create the fork under test on it");
	for (int i = 0; i < 12; i++)
		check(append_relation_timeline(1, &key, 0, 0, page, &seq) == 0,
			  "write ordered growth then later ordered COMMITs on the branch fork");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 1;
	pin.owner_kind = 1;
	pin.owner_id = 91;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = seq;
	check(seq != 0 && ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a frontier for the branch and reclaim old ordered versions");
	check(ps_test_fork_event_count(1, &key, &nevents_before, &nmarkers_before,
									&ninert_before) &&
		  ninert_before == 11,
		  "12 live writes leave 11 inert commit markers before any cutover");
	/*
	 * A cutover now would flag some of those 11 as dropped (their versions
	 * were just reclaimed) but must fail before compaction ever sees the
	 * flags: fork_meta_snapshot_build()'s per-entry loop still runs (it
	 * happens before ps_forkmeta_snapshot_prepare()), so the flags get
	 * stamped; the injected failure is in prepare, which does not poison
	 * the store, so a later cutover in this same process can still succeed.
	 *
	 * The churn to cross the byte trigger is deliberately created on
	 * timeline 1 (not root_timeline via append_growth_batch()): a new
	 * owning fork on the ROOT timeline with no published frontier of its
	 * own would make fork_meta_snapshot_cutoff() fail closed (root is
	 * never exempt), blocking every cutover attempt below, not just this
	 * one -- timeline 1 already has a published frontier from the reclaim
	 * above, so churn there stays covered.
	 */
	check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_PREPARED_BEFORE_CREATE", "1", 1) == 0 &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm a low trigger for the cutover attempt below");
	for (uint32_t i = 0; i < 20; i++)
	{
		PsKey churn_key = key;

		churn_key.relNumber = 100 + i;
		check(meta_request_timeline(1, PS_OP_CREATE, &churn_key, 100, 0, 0, 0, NULL),
			  "churn on the branch timeline to cross the byte trigger");
	}
	for (int tick = 0; tick < 10; tick++)
		(void) ps_core_maintenance();
	check(access(manifest, F_OK) != 0,
		  "the injected prepare failure blocks this cutover from publishing");
	unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_PREPARED_BEFORE_CREATE");
	/*
	 * The still-live retention pin on timeline 1 is an "active owner"
	 * (timeline_has_active_owner()) and blocks PS_OP_BEGIN_DELETE; drop it
	 * through the daemon meta op first, the same way T7's
	 * test_artifact_write_unfenced_after_pin_drop() does above.
	 */
	{
		PsChannel	ch;

		memset(&ch, 0, sizeof(ch));
		ch.opcode = PS_OP_RETENTION_PIN_DROP;
		ch.timeline = 1;
		ch.key = key;
		ch.blocknum = pin.owner_kind;
		ch.req_seq = pin.owner_id;
		ch.old_nblocks = pin.generation;
		ch.status = PS_STATUS_OK;
		ps_lock_shard_rd(ps_shard_of(&key));
		(void) ps_handle_meta(&ch);
		ps_unlock_shard(ps_shard_of(&key));
		check(ch.status == PS_STATUS_OK,
			  "drop the branch's retention pin so it is no longer an active owner");
	}
	check(begin_delete_timeline(1),
		  "begin deleting the branch timeline whose fork was just flagged");
	check(run_maintenance_until(manifest, 1),
		  "a later cutover now publishes, filtering the deleting timeline");
	check(ps_test_fork_event_count(1, &key, &nevents_after, &nmarkers_after,
									&ninert_after) &&
		  nevents_after == nevents_before && nmarkers_after == nmarkers_before &&
		  ninert_after == ninert_before,
		  "the deleting fork's in-memory counts are untouched by the "
		  "successful cutover that skipped it, despite the stale flags "
		  "from the earlier failed cutover's per-entry loop");
	close_runtime();
	remove_tree(store);
}

/* Defined below, near the other two-cutover tests it was written for; also
 * used by the periodic-cutover scaling test just above its definition. */
static int generation_advances_past(const char *directory, uint64_t generation);

/*
 * Scaling assertion (design B): repeated rewrite/cutover PERIODS must keep
 * the in-memory array bounded, not growing with the total number of writes
 * across every period.
 *
 * The fork is created once, far above every round's retention pin (floor
 * 100000 vs pin lsn 100000 itself -- see below), and never truncated: every
 * round's PER_ROUND WAL-less rewrites of the same block therefore clamp to
 * the SAME fixed floor for the whole test (test_reclaimed_ordered_markers_pruned()'s
 * clamped-write shape above), so `existed_before` in the admission check
 * stays false and every one of them keeps generating an inert commit
 * marker, round after round -- not just in round 0.  (A floor that
 * advances between rounds, e.g. via a same-size PS_OP_TRUNCATE, makes the
 * block "already visible" as of the new floor for every write after the
 * first in that round, which silences the ordered/marker path entirely --
 * a dead end this test steered around, not a design B concern.)  Each
 * round's retention pin targets that SAME floor lsn (where the versions
 * actually are) with an advancing admission_seq (a higher generation
 * supersedes its own earlier pin, test_inert_markers_compacted_after_cutover()'s
 * reclaim recipe), so every earlier round's versions become reclaimable;
 * the pin's lsn equals the fork's floor, so the operational cutoff it
 * establishes never exceeds the floor and a fresh admission_seq is always
 * future at equal lsn, so later rounds' writes are never refused
 * (PS_APPEND_REFUSED_FORKMETA_CUTOFF).  Before
 * fork_event_compact_dropped_markers() existed, nothing ever dropped the
 * earlier rounds' now-durably-dropped inert markers from memory, so
 * nevents would grow by roughly PER_ROUND on every round; this is the
 * fail-before case for this task (see the PR description for the measured
 * before/after numbers).
 */
static void
test_fork_event_index_periodic_cutover_bounded(void)
{
	char store[] = "/tmp/psforkmetaperiodicboundXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 11, 0, PS_KLASS_RELATION};
	const uint64_t FLOOR = 100000;
	const uint32_t ROUNDS = 6;
	const uint32_t PER_ROUND = 30;
	unsigned char page[8192];
	uint64_t seq = 0;
	uint32_t max_nevents = 0;
	uint32_t first_nevents = 0;
	TestSnapshotHeader hdr;
	int have_hdr = 0;
	int n;

	check(mkdtemp(store) != NULL, "create periodic-cutover bound store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build periodic snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build periodic manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build periodic frontier path");
	flush_pages = 1;
	compact_layers = 2;
	/*
	 * An earlier test in this binary (test_deletion_filtered_forkmeta_commit_shape())
	 * leaves segment_gc_enabled/cache_pages/segment_size at non-default
	 * values for the rest of the process; restore the defaults this test
	 * was designed and repro'd against.
	 */
	segment_gc_enabled = 1;
	cache_pages = 1024;
	segment_size = 8 * 1024 * 1024;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &key, FLOOR, 0, 0, 0, NULL),
		  "open periodic-cutover store and create fork at the fixed floor");

	for (uint32_t round = 0; round < ROUNDS; round++)
	{
		PsRetentionPin pin;
		uint32_t nevents = 0,
					nmarkers = 0,
					ninert = 0;

		for (uint32_t i = 0; i < PER_ROUND; i++)
			check(append_relation_tag(&key, 0, 50, page,
									  (unsigned char) (round * PER_ROUND + i),
									  &seq) == 0,
				  "periodic-cutover round write");
		memset(&pin, 0, sizeof(pin));
		pin.timeline = 0;
		pin.owner_kind = 1;
		pin.owner_id = 90;
		pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
		pin.generation = round + 1;
		pin.lsn = FLOOR;
		pin.admission_seq = seq;
		check(seq != 0 && ps_retention_set(&pin) == PS_RETENTION_OK,
			  "advance this round's retention pin past every write so far");
		/*
		 * The frontier FILE only needs to exist once (round 0 creates it);
		 * from round 1 on, wait for content by polling the block's own
		 * version count (page-prune's actual effect) down to stability
		 * instead of a blind fixed tick budget, so a round that converges
		 * in a couple of ticks does not pay for the worst case every time.
		 */
		if (round == 0)
			check(run_maintenance_until(frontier, 1),
				  "publish the first frontier and reclaim this round's versions");
		else
		{
			uint32_t	prev = ps_test_page_version_count(0, &key, 0);
			int			stable = 0;

			for (int tick = 0; tick < 300 && stable < 3; tick++)
			{
				uint32_t	cur;

				(void) ps_core_maintenance();
				usleep(5000);
				cur = ps_test_page_version_count(0, &key, 0);
				stable = (cur == prev) ? stable + 1 : 0;
				prev = cur;
			}
		}
		/*
		 * The snapshot trigger stays high while each round's writes land
		 * and its reclaim runs (matching every other reclaim test in this
		 * file); only lowering it right before forcing the cutover avoids
		 * a premature cutover racing the reclaim pass mid-round.  The
		 * churn batch's own floor also stays above FLOOR, for the same
		 * admission reason as the fork under test.
		 */
		check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
			  append_growth_batch(2500 + round * 20, FLOOR + 500 + round * 20),
			  "periodic-cutover round churn to cross the byte trigger");
		if (!have_hdr)
		{
			check(run_maintenance_until(manifest, 1),
				  "first periodic cutover publishes");
			have_hdr = read_selected_header(snapshots, &hdr) == 0;
			check(have_hdr, "read first periodic cutover's header");
		}
		else
		{
			uint64_t before_generation = hdr.generation;

			check(generation_advances_past(snapshots, before_generation),
				  "later periodic cutover publishes a new generation");
			check(read_selected_header(snapshots, &hdr) == 0,
				  "read this round's cutover header");
		}
		check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0,
			  "raise the trigger back before the next round's writes");
		check(ps_test_fork_event_count(0, &key, &nevents, &nmarkers, &ninert),
			  "read in-memory counts after this round's cutover");
		if (round == 0)
			first_nevents = nevents;
		if (nevents > max_nevents)
			max_nevents = nevents;
	}

	/*
	 * Bounded by roughly what round 0 alone left behind, not by
	 * ROUNDS * PER_ROUND: the fail-before number for this fixture is
	 * reported in the PR description (measured against this same test with
	 * fork_event_compact_dropped_markers() reverted).
	 */
	check(max_nevents <= first_nevents + PER_ROUND,
		  "the in-memory array stays bounded across repeated cutovers, not "
		  "growing with the total writes over every round (fail-before: it "
		  "grew toward ROUNDS * PER_ROUND here)");

	close_runtime();
	remove_tree(store);
	compact_layers = 0;
}

static void
test_legacy_only_deletion_filtered_forkmeta(void)
{
	char store[] = "/tmp/psforkmetadeletelegacyXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	PsKey key = {10, 10, 10, 0, PS_KLASS_RELATION};
	TestForkMetaRecV1 legacy;
	TestSnapshotHeader header;
	int n;

	check(mkdtemp(store) != NULL &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 && create_branch_request(1, 0, 1),
		  "create timeline for legacy-only deletion cutover");
	close_runtime();
	memset(&legacy, 0, sizeof(legacy));
	legacy.timeline = 1;
	legacy.key = key;
	legacy.lsn = 1;
	legacy.nblocks = 1;
	legacy.kind = TEST_FEV_GROW;
	check(PsStoragePosix.open(store, segment_size) == 0 &&
		  PsStoragePosix.fork_meta_rewrite(&legacy, sizeof(legacy)) == 0,
		  "install legacy-only sequence-zero forkmeta source");
	PsStoragePosix.close();
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build legacy deletion snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build legacy deletion manifest path");
	check(ps_core_open(store) == 0 && begin_delete_timeline(1) &&
		  run_maintenance_until(manifest, 1),
		  "legacy-only deletion advances admission and publishes cutover");
	check(read_selected_header(snapshots, &header) == 0 &&
		  header.cutoff_lsn == 1 && header.cutoff_admission_seq == 1 &&
		  header.freeze_admission_seq >= 1 &&
		  snapshot_timeline_record_count(snapshots, 1) == 0,
		  "legacy-only deletion snapshot filters target at valid freeze");
	close_runtime();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

/* Forward declarations: defined below with the other orphaned-ordered-record
 * adoption test helpers, but also needed here for the commit-shape variant's
 * stderr-based F3 assertions. */
static int open_capture_stderr(const char *store, char *out, size_t out_cap,
							   int *rc_out);
static int count_occurrences(const char *haystack, const char *needle);

/*
 * `commit_shape`, when true, inserts one extra WAL-less rewrite of the
 * sibling's block 0 before its later same-block write below, so the first
 * write becomes a growth-class ordered record and the second a commit-class
 * one (segment_grows == 0) -- the FSM/VM pattern, and the shape F3's data
 * loss actually affects, since a growth-class orphan's plain GROW alone was
 * already (incidentally) repaired by growth adoption even before F2.
 */
static void
test_deletion_filtered_forkmeta_impl(int commit_shape)
{
	char store[] = "/tmp/psforkmetadeletefilterXXXXXX";
	char failed_store[] = "/tmp/psforkmetadeletefailXXXXXX";
	char snapshots[1024];
	char frontier[1200];
	char manifest[1200];
	char source[1200];
	PsKey target_key = {11, 11, 1, 0, PS_KLASS_RELATION};
	PsKey sibling_key = {11, 11, 2, 0, PS_KLASS_RELATION};
	PsKey sibling_extra_key = {11, 11, 3, 0, PS_KLASS_RELATION};
	PsKey undefined_key = {11, 11, 5, 0, PS_KLASS_RELATION};
	TestForkMetaRecV2 extra;
	TestForkMetaRecV2 target_marker;
	TestForkMetaRecV2 undefined_marker;
	TestSnapshotHeader before;
	TestSnapshotHeader after;
	PsChannel reply;
	unsigned char page[8192];
	uint64_t seq = 0;
	uint64_t generation;
	int advanced = 0;
	int n;

	page_size = sizeof(page);
	segment_size = 1024 * 1024;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 0;
	cache_pages = 0;
	ps_nshards = 1;
	use_layers = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0,
		  "arm deletion-filter snapshot threshold below forced trigger");
	check(mkdtemp(store) != NULL && ps_core_open(store) == 0,
		  "open deletion-filter forkmeta store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build deletion-filter snapshot path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build deletion-filter page frontier path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build deletion-filter manifest path");
	n = snprintf(source, sizeof(source), "%s/forkmeta", store);
	check(n > 0 && (size_t) n < sizeof(source),
		  "build deletion-filter source path");
	check(create_branch_request(1, 0, 1) &&
			  create_branch_request(2, 0, 1) &&
			  create_branch_request(3, 0, 1),
		  "create target, marker-only target, and live sibling timelines");
	check(meta_request_timeline(1, PS_OP_CREATE, &target_key, 100, 0, 0, 0,
								 NULL) &&
			  meta_request_timeline(2, PS_OP_CREATE, &sibling_key, 100, 0, 0, 0,
								 NULL),
		  "create target and sibling fork metadata");
	if (commit_shape)
		check(append_relation_timeline(2, &sibling_key, 0, 0, page, &seq) == 0,
			  "write the sibling's growth-class ordered marker (commit-shape "
			  "variant)");
	check(append_relation_timeline(1, &target_key, 0, 0, page, &seq) == 0 &&
			  append_relation_timeline(2, &sibling_key, 0, 0, page, &seq) == 0 &&
			  append_relation_timeline(2, &sibling_key, 0, 200, page, &seq) == 0,
		  "write target and sibling ordered markers");
	memset(&extra, 0, sizeof(extra));
	extra.magic = TEST_FORK_META_V2_MAGIC;
	extra.rec_len = sizeof(extra);
	extra.timeline = 2;
	extra.key = sibling_extra_key;
	extra.lsn = 200;
	extra.admission_seq = seq + 100;
	extra.order_id = 9001;
	extra.nblocks = 1;
	extra.kind = TEST_FEV_SEG_GROW_BOUND;
	check(append_source_record(source, &extra),
		  "append surviving source marker outside the in-memory index");
	undefined_marker = extra;
	undefined_marker.timeline = 99;
	undefined_marker.key = undefined_key;
	undefined_marker.admission_seq += 2;
	undefined_marker.order_id += 2;
	check(append_source_record(source, &undefined_marker),
		  "append undefined/pre-metadata owner marker");
	target_marker = extra;
	target_marker.timeline = 3;
	target_marker.admission_seq++;
	target_marker.order_id++;
	target_marker.kind = TEST_FEV_SEG_COMMIT_BOUND;
	check(append_source_record(source, &target_marker),
		  "append deleting owner with only an ordered commit marker");
	close_runtime();
	check(ps_core_open(store) == 0,
		  "reopen to load source-only ordered markers before deletion");
	check(begin_delete_timeline(3),
		  "begin marker-only timeline deletion before forkmeta maintenance");
	check(run_maintenance_until(manifest, 1),
		  "marker-only deletion forces cutover below the byte threshold");
	check(read_selected_header(snapshots, &before) == 0 &&
			  snapshot_timeline_record_count(snapshots, 3) == 0 &&
			  snapshot_timeline_record_count(snapshots, 2) > 0 &&
			  snapshot_timeline_record_count(snapshots, 99) > 0,
		  "marker-only target is filtered while live and undefined owners survive");
	generation = before.generation;
	check(begin_delete_timeline(1),
		  "begin a second target deletion in the same process");
	for (int i = 0; i < 40; i++)
	{
		(void) ps_core_maintenance();
		if (read_selected_header(snapshots, &after) == 0 &&
			after.generation > generation)
		{
			advanced = 1;
			break;
		}
		usleep(100000);
	}
	check(advanced && snapshot_timeline_record_count(snapshots, 1) == 0 &&
			  snapshot_timeline_record_count(snapshots, 3) == 0 &&
			  snapshot_timeline_record_count(snapshots, 2) > 0 &&
			  snapshot_timeline_record_count(snapshots, 99) > 0,
		  "later deletion publishes one new generation and preserves survivors");
	check(!source_has_timeline_record(source, 1) &&
			  !source_has_timeline_record(source, 3) &&
			  snapshot_ordered_marker_count(snapshots, &sibling_extra_key,
											extra.admission_seq, 1) == 1,
		  "rewritten source filters target markers while surviving source marker remains selected");
	generation = after.generation;
	for (int i = 0; i < 4; i++)
		(void) ps_core_maintenance();
	check(read_selected_header(snapshots, &after) == 0 &&
			  after.generation == generation,
		  "successful deletion filtering does not repeat in this process");
	{
		PsRetentionPin pin;

		memset(&pin, 0, sizeof(pin));
		pin.timeline = 2;
		pin.owner_kind = PS_RETENTION_OWNER_READER;
		pin.owner_id = 2202;
		pin.generation = 1;
		pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
		pin.lsn = 200;
		pin.admission_seq = seq;
		check(ps_retention_set(&pin) == PS_RETENTION_OK &&
			  run_maintenance_until(frontier, 1),
			  "publish surviving owner frontier for ordinary pruning");
	}
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm an ordinary size-triggered snapshot after deletion filtering");
	for (uint32_t i = 0; i < 20; i++)
	{
		PsKey churn_key = sibling_key;

		churn_key.relNumber = 100 + i;
		check(meta_request_timeline(2, PS_OP_CREATE, &churn_key, 400 + i,
									 0, 0, 0, NULL),
			  "append surviving owner churn after filtered cutover");
	}
	advanced = 0;
	for (int i = 0; i < 40; i++)
	{
		(void) ps_core_maintenance();
		if (read_selected_header(snapshots, &after) == 0 &&
			after.generation > generation)
		{
			advanced = 1;
			break;
		}
		usleep(100000);
	}
	check(advanced, "ordinary threshold publishes a later generation");
	check(snapshot_timeline_record_count(snapshots, 1) == 0 &&
			  snapshot_timeline_record_count(snapshots, 3) == 0,
		  "ordinary threshold snapshot cannot resurrect filtered owners");
	check(snapshot_timeline_record_count(snapshots, 2) > 0,
		  "ordinary threshold snapshot retains live owner");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0,
		  "restore high threshold after ordinary snapshot regression check");
	close_runtime();
	if (commit_shape)
	{
		char		captured[16384];
		int			rc = -1;

		check(open_capture_stderr(store, captured, sizeof(captured), &rc) &&
			  rc == 0, "reopen after ordinary threshold snapshot (commit-shape)");
		check(count_occurrences(captured, "retiring tail at offset") == 0 &&
			  count_occurrences(captured, "refusing unmatched ordered record") == 0,
			  "commit-shape reopen logs no segment-tail retirement and no refusal");
		/* Invariant I3: page_cleanup_tombstone_segment() tombstones a target
		 * record in place instead of relocating survivors, so it never
		 * rebases the flush watermark and never creates a rescan region.  A
		 * pruned survivor's segment record therefore stays below the
		 * watermark, is never rescanned, and reopen needs no adoption at
		 * all -- unlike the pre-fix rewrite, which moved bytes and forced
		 * exactly this to be a mitigation rather than the fix. */
		check(count_occurrences(captured, "adopting orphaned ordered") == 0,
			  "invariant I3: reopen adopts no orphan; the pruned survivor's "
			  "record is never rescanned");
	}
	else
		check(ps_core_open(store) == 0,
			  "reopen after ordinary threshold snapshot");
	check(snapshot_timeline_record_count(snapshots, 1) == 0 &&
			  snapshot_timeline_record_count(snapshots, 3) == 0 &&
			  !source_has_timeline_record(source, 1),
		  "restart does not resurrect filtered targets");
	check(snapshot_timeline_record_count(snapshots, 2) > 0,
		  "restart retains live metadata");
	check(meta_request_timeline(2, PS_OP_NBLOCKS, &sibling_key, 0, 0, 0, 0,
								 &reply) && reply.result == 1,
		  "live sibling metadata resolves after restart");
	/* F3/Q1 (RELEASE_VALIDATION.md, resolved): fork_meta_snapshot_build()
	 * can degrade or drop a marker whose page version was pruned from
	 * memory while its segment record survives.  Before the fix, a later
	 * timeline-delete rewrite (page_cleanup_rewrite_segment()) relocated
	 * survivors and rebased the flush watermark to (seg, 0), so the next
	 * open rescanned the whole segment and met that now-orphaned record
	 * (silently losing it -- read_resolve_version returned rc 0) or,
	 * incidentally, the generalized adoption rule turned it into a logged
	 * pruning reversal.  Invariant I3 (page_cleanup_tombstone_segment()):
	 * segment bytes never move and the watermark never retreats, so this
	 * pruned survivor's record stays below the watermark and is never
	 * rescanned at all -- it is simply still there, unreplayed, exactly as
	 * before the deletion. */
	{
		unsigned char verpage[8192];
		uint64_t	ver = 0,
					rseq = 0;

		check(read_resolve_version(2, &sibling_key, 0, UINT64_MAX, 0, verpage,
								   &ver, &rseq) == 1 && ver == 200,
			  "invariant I3: the pinned sibling version at LSN 200 survives "
			  "the timeline-delete tombstone pass untouched");
	}
	close_runtime();
	remove_tree(store);

	check(mkdtemp(failed_store) != NULL &&
			  setenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME", "1", 1) == 0 &&
			  ps_core_open(failed_store) == 0,
		  "open deletion-filter failure-boundary store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", failed_store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build failure-boundary snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build failure-boundary manifest path");
	n = snprintf(source, sizeof(source), "%s/forkmeta", failed_store);
	check(n > 0 && (size_t) n < sizeof(source),
		  "build failure-boundary source path");
	check(create_branch_request(1, 0, 1) && create_branch_request(2, 0, 1) &&
			  meta_request_timeline(1, PS_OP_CREATE, &target_key, 100, 0, 0, 0,
									 NULL) &&
			  meta_request_timeline(2, PS_OP_CREATE, &sibling_key, 100, 0, 0, 0,
									 NULL) &&
			  append_relation_timeline(1, &target_key, 0, 0, page, &seq) == 0 &&
			  append_relation_timeline(2, &sibling_key, 0, 0, page, &seq) == 0 &&
			  begin_delete_timeline(1),
		  "prepare deletion-filter manifest failure boundary");
	(void) ps_core_maintenance();
	unsetenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME");
	check(access(manifest, F_OK) == 0 && source_has_timeline_record(source, 1),
		  "source remains old while selected filtered manifest is ambiguous");
	close_runtime();
	check(ps_core_open(failed_store) == 0 &&
			  snapshot_timeline_record_count(snapshots, 1) == 0 &&
			  !source_has_timeline_record(source, 1),
		  "restart reconciles selected filtered manifest and removes target source records");
	close_runtime();
	remove_tree(failed_store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

static void
test_deletion_filtered_forkmeta(void)
{
	test_deletion_filtered_forkmeta_impl(0);
}

static void
test_deletion_filtered_forkmeta_commit_shape(void)
{
	test_deletion_filtered_forkmeta_impl(1);
}

/*
 * ---- a live ordered write's in-memory history equals recovery's (PR #262 review finding) ----
 *
 * append_page_impl() durably persists an ordered write's bound marker via
 * fork_meta_persist_segment() regardless of the bug this covers; what was
 * wrong is the in-memory mirror fork_grow_apply() built (a plain GROW with
 * marker_kind = order_id = 0).  A forkmeta snapshot is serialized from that
 * memory, so after a cutover it is the only durable copy of the pre-cutover
 * metadata; a *second* cutover in the same daemon lifetime then finds the
 * marker in neither memory nor the (already rewritten) source log and
 * publishes a plain GROW, after which the record can never activate again.
 * These tests drive exactly that two-cutover sequence and assert the bound
 * marker (not a plain GROW) survives both generations.
 */

static int
generation_advances_past(const char *directory, uint64_t generation)
{
	TestSnapshotHeader after;

	for (int i = 0; i < 40; i++)
	{
		(void) ps_core_maintenance();
		if (read_selected_header(directory, &after) == 0 &&
			after.generation > generation)
			return 1;
		usleep(100000);
	}
	return 0;
}

static void
test_live_ordered_marker_survives_two_cutovers(void)
{
	char store[] = "/tmp/psforkmetalive2cutXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 1, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 20, 0, PS_KLASS_RELATION};
	uint64_t first_seq = 0,
				second_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	uint64_t seq = 0;
	int n;

	check(mkdtemp(store) != NULL, "create live-ordered two-cutover store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build live-ordered snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build live-ordered manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build live-ordered frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 77;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	/* pd_lsn 250 < definitive floor 300 -> clamped ordered record with a bound marker */
	check(append_relation_tag(&key, 0, 250, page, 0x40, &seq) == 0 && seq != 0,
		  "live clamped ordered write below the fork floor");
	check(append_growth_batch(2200, 500) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover after the live ordered write");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 1 header");
	check(snapshot_ordered_marker_count(snapshots, &key, seq, 1) == 1 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 1 keeps the live bound marker with no duplicate plain GROW");
	check(append_growth_batch(2300, 600) &&
		  generation_advances_past(snapshots, hdr.generation),
		  "second cutover in the same daemon lifetime");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 2 header");
	check(snapshot_ordered_marker_count(snapshots, &key, seq, 1) == 1 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 2 still keeps the live bound marker (the bug lost it here)");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0,
		  "store reopens after two cutovers following a live ordered write");
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40, "ordered page readable after reopen");
	close_runtime();
	remove_tree(store);
}

static void
test_live_ordered_commit_marker_survives_two_cutovers(void)
{
	char store[] = "/tmp/psforkmetalive2cutcmtXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 2, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 21, 0, PS_KLASS_RELATION};
	uint64_t first_seq = 0,
				second_seq = 0;
	uint64_t grow_seq = 0,
				commit_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	int n;

	check(mkdtemp(store) != NULL, "create live-ordered commit-marker store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build commit-marker snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build commit-marker manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build commit-marker frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 78;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	/* First write grows the block (SEG_GROW_BOUND); the second, identically
	 * clamped write to the same block does not grow it, so it durably
	 * persists as a SEG_COMMIT_BOUND -- recovery never builds a plain GROW
	 * for that kind, so the pre-fix live path's plain GROW was itself a
	 * divergence for this shape, independent of the two-cutover bug. */
	check(append_relation_tag(&key, 0, 250, page, 0x40, &grow_seq) == 0 &&
		  grow_seq != 0,
		  "live clamped ordered growth write below the fork floor");
	check(append_relation_tag(&key, 0, 250, page, 0x41, &commit_seq) == 0 &&
		  commit_seq != 0 && commit_seq != grow_seq,
		  "live clamped ordered commit rewrite of the same block");
	check(append_growth_batch(2400, 700) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover after the live ordered growth+commit writes");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 1 header");
	check(snapshot_ordered_marker_count(snapshots, &key, 0, 0) == 2 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 1 keeps both bound markers with no duplicate plain GROW");
	check(append_growth_batch(2500, 800) &&
		  generation_advances_past(snapshots, hdr.generation),
		  "second cutover in the same daemon lifetime");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 2 header");
	check(snapshot_ordered_marker_count(snapshots, &key, 0, 0) == 2 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 2 still keeps both bound markers (the bug lost the growth one here)");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0,
		  "store reopens after two cutovers following live ordered growth+commit writes");
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x41, "newest ordered commit tag readable after reopen");
	close_runtime();
	remove_tree(store);
}

static void
test_live_ordered_marker_walless_survives_two_cutovers(void)
{
	char store[] = "/tmp/psforkmetalive2cutwlXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 3, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 22, 0, PS_KLASS_RELATION};
	uint64_t first_seq = 0,
				second_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	uint64_t seq = 0;
	int n;

	check(mkdtemp(store) != NULL, "create WAL-less live-ordered two-cutover store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build WAL-less live-ordered snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build WAL-less live-ordered manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build WAL-less live-ordered frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 79;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	/* pd_lsn 0 -> SEG_WALLESS_ADMISSION_MAGIC, stamped at the growth floor;
	 * ordered via zero_version rather than clamped, same in-memory hazard. */
	check(append_relation_tag(&key, 0, 0, page, 0x50, &seq) == 0 && seq != 0,
		  "live WAL-less ordered write");
	check(append_growth_batch(2600, 900) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover after the live WAL-less ordered write");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 1 header");
	check(snapshot_ordered_marker_count(snapshots, &key, seq, 1) == 1 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 1 keeps the live WAL-less bound marker with no duplicate plain GROW");
	check(append_growth_batch(2700, 1000) &&
		  generation_advances_past(snapshots, hdr.generation),
		  "second cutover in the same daemon lifetime");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 2 header");
	check(snapshot_ordered_marker_count(snapshots, &key, seq, 1) == 1 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 2 still keeps the live WAL-less bound marker (the bug lost it here)");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0,
		  "store reopens after two cutovers following a live WAL-less ordered write");
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x50, "WAL-less ordered page readable after reopen");
	close_runtime();
	remove_tree(store);
}

/* E-6: a long-lived FSM/VM fork must stay writable after the selected
 * forkmeta cutoff passes its definition (or a child's branch floor). */
static void
test_ordered_write_after_cutoff(uint32_t timeline, int walless)
{
	char store[] = "/tmp/psforkmetaaftercutXXXXXX";
	char snapshots[1024], manifest[1200], frontier[1200];
	PsKey key = {6, 6, 31, 1, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 32, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	PsChannel reply;
	unsigned char page[8192];
	uint64_t seq = 0;
	uint64_t lsn = walless ? 0 : 50;

	check(mkdtemp(store) != NULL, "E6 create store");
	snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "E6 open store");
	if (timeline)
		check(create_branch_request(timeline, 0, 100), "E6 create child");
	check(meta_request_timeline(timeline, PS_OP_CREATE, &key,
								 100 + timeline, 0, 0, 0, NULL) &&
		  append_relation_timeline(timeline, &key, 0, lsn, page, NULL) == 0,
		  "E6 initial ordered local write");
	memset(&pin, 0, sizeof(pin));
	pin.owner_kind = 1;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 300;
	{
		uint32_t tl = timeline;

		check(meta_request_timeline(tl, PS_OP_CREATE, &pin_key, 200, 0, 0, 0, NULL) &&
			  append_relation_timeline(tl, &pin_key, 0, 200, page, NULL) == 0 &&
			  append_relation_timeline(tl, &pin_key, 0, 300, page, &seq) == 0,
			  "E6 write history to advance each timeline frontier");
		pin.timeline = tl;
		pin.owner_id = 90 + tl;
		pin.admission_seq = seq;
		check(ps_retention_set(&pin) == PS_RETENTION_OK, "E6 retain current history");
	}
	check(run_maintenance_until(frontier, 1) &&
		  append_growth_batch_timeline(timeline, 2800, 900) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1) &&
		  read_selected_header(snapshots, &hdr) == 0 && hdr.cutoff_lsn == 300,
		  "E6 compact past fork definition and branch floor");
	check(append_relation_timeline_tag(timeline, &key, 0, lsn, page, 0x70, &seq) == 0 &&
		  seq > hdr.freeze_admission_seq,
		  "E6 existing ordered block remains writable after cutoff");
	check(read_resolve(timeline, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x70, "E6 rewrite replaces existing bytes");
	check(append_relation_timeline(timeline, &key, 2, lsn, page, NULL) == 0 &&
		  meta_request_timeline(timeline, PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
		  reply.result == 3,
		  "E6 ordered growth remains writable after cutoff");
	if (!walless)
		check(read_resolve(timeline, &key, 2, hdr.cutoff_lsn,
						   hdr.cutoff_admission_seq, page, NULL) == 0,
			  "E6 new growth is invisible at the retained cutoff tuple");
	check(append_relation_timeline(timeline, &key, 5, 150, page, NULL) != 0 &&
		  meta_request_timeline(timeline, PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
		  reply.result == 3,
		  "E6 explicit historical WAL growth is still refused without changing size");
	/* Recover the promoted source-tail markers without a clean close/flush.
	 * Both a commit-class rewrite and a growth-class append must survive. */
	close_runtime();
	{
		pid_t pid = fork();
		int status = 0;

		if (pid == 0)
		{
			flush_pages = 100000;
			_exit(ps_core_open(store) == 0 &&
				  append_relation_timeline_tag(timeline, &key, 0, lsn, page, 0x71, NULL) == 0 &&
				  append_relation_timeline_tag(timeline, &key, 4, lsn, page, 0x72, NULL) == 0 ? 0 : 1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 0,
			  "E6 append post-cutoff records then exit without flushing layers");
	}
	check(ps_core_open(store) == 0 &&
		  read_resolve(timeline, &key, 0, UINT64_MAX, 0, page, NULL) == 1 && page[128] == 0x71 &&
		  read_resolve(timeline, &key, 4, UINT64_MAX, 0, page, NULL) == 1 && page[128] == 0x72,
		  "E6 source-tail recovery preserves promoted commit and growth markers");
	check(meta_request_timeline(timeline, PS_OP_TRUNCATE, &key,
								 hdr.cutoff_lsn, 0, 1, 0, NULL) &&
		  read_resolve(timeline, &key, 2, UINT64_MAX, 0, page, NULL) == 0,
		  "E6 later same-LSN truncate hides the grown page");
	check(append_relation_timeline(timeline, &key, 2, lsn, page, NULL) == 0,
		  "E6 ordered growth after same-LSN truncate succeeds");
	check(append_growth_batch_timeline(timeline, 2900, 1000) &&
		  generation_advances_past(snapshots, hdr.generation) &&
		  read_selected_header(snapshots, &hdr) == 0 &&
		  append_growth_batch_timeline(timeline, 3000, 1100) &&
		  generation_advances_past(snapshots, hdr.generation),
		  "E6 two more cutovers preserve admitted markers");
	for (int restart = 0; restart < 2; restart++)
	{
		close_runtime();
		check(ps_core_open(store) == 0 &&
			  read_resolve(timeline, &key, 2, UINT64_MAX, 0, page, NULL) == 1 &&
			  page[128] == (unsigned char) (timeline + 0x20) &&
			  meta_request_timeline(timeline, PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
			  reply.result == 3 &&
			  read_resolve(timeline, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
			  page[128] == 0x71 &&
			  read_resolve(timeline, &key, 4, UINT64_MAX, 0, page, NULL) == 0,
			  "E6 ordered bytes and size survive repeated reopen");
		if (timeline)
			check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 0 &&
				  meta_request(PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) && reply.result == 0,
				  "E6 child writes leave the parent empty");
	}
	close_runtime();
	remove_tree(store);
}

/* A late ordered parent write must not change an existing child's snapshot,
 * including a branch newer than the selected forkmeta cutoff. */
static void
test_ordered_parent_after_branch(int walless)
{
	char store[] = "/tmp/psforkmetabranchXXXXXX";
	char snapshots[1024], manifest[1200], frontier[1200];
	PsKey key = {6, 6, 33, 1, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 34, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	PsChannel reply;
	unsigned char page[8192];
	uint64_t seq = 0;
	uint64_t reader_seq[2];
	uint64_t lsn = walless ? 0 : 50;

	check(mkdtemp(store) != NULL, "E6 branch create store");
	snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "E6 branch open store");
	check(meta_request(PS_OP_CREATE, &key, 100, 0, 0, 0, NULL) &&
		  append_relation_timeline_tag(0, &key, 0, lsn, page, 0x61, NULL) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 200, 0, 0, 0, NULL) &&
		  append_relation_timeline(0, &pin_key, 0, 200, page, NULL) == 0 &&
		  append_relation_timeline(0, &pin_key, 0, 300, page, &seq) == 0,
		  "E6 branch seed parent history");
	memset(&pin, 0, sizeof(pin));
	pin.owner_kind = 1;
	pin.owner_id = 95;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 300;
	pin.admission_seq = seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1) &&
		  append_growth_batch_timeline(0, 2800, 900) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1) &&
		  read_selected_header(snapshots, &hdr) == 0 && hdr.cutoff_lsn == 300,
		  "E6 branch select cutoff");
	check(create_branch_request(1, 0, 300) &&
		  create_branch_request(2, 0, 400) &&
		  create_branch_request(3, 2, 500), "E6 branch fork at and beyond cutoff");
	for (int reader = 0; reader < 2; reader++)
	{
		pin.owner_id = 96 + reader;
		pin.lsn = 500 + reader * 100;
		check(append_relation_timeline(0, &pin_key, 0, pin.lsn, page, &reader_seq[reader]) == 0,
			  "E6 reader establish newer admission boundary");
		pin.admission_seq = reader_seq[reader];
		check(ps_retention_set(&pin) == PS_RETENTION_OK,
			  "E6 reader pin above selected cutoff and branch caps");
	}
	for (int restart = 0; restart < 3; restart++)
	{
		check(append_relation_timeline_tag(0, &key, 0, lsn, page, 0x70 + restart, NULL) == 0 &&
			  append_relation_timeline_tag(0, &key, 2, lsn, page, 0x73, NULL) == 0 &&
			  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
			  page[128] == 0x70 + restart,
			  "E6 parent accepts ordered writes after branch");
		if (!walless)
			for (int reader = 0; reader < 2; reader++)
			{
				uint64_t horizon = 500 + reader * 100;

				check(read_resolve(0, &key, 0, horizon, reader_seq[reader], page, NULL) == 1 &&
					  page[128] == 0x61 &&
					  read_resolve(0, &key, 2, horizon, reader_seq[reader], page, NULL) == 0 &&
					  meta_request(PS_OP_NBLOCKS, &key, horizon, reader_seq[reader], 0, 0, &reply) &&
					  reply.result == 1,
					  "E6 retained readers never see later clamped bytes or growth");
			}
		for (uint32_t child = 1; child <= 3; child++)
		{
			int rc = read_resolve(child, &key, 0, UINT64_MAX, 0, page, NULL);

			check(walless ? rc < 0 : rc == 1 && page[128] == 0x61,
				  "E6 child never serves later ordered parent bytes");
			{
				PageVer *v;
				int status = read_through_checked(child, &key, 0, UINT64_MAX, 0, &v);

				check(walless ? status == -1 && v == NULL :
					  status == 1 && v != NULL && v->lsn == 100,
					  "E6 in-memory ancestry lookup obeys the same snapshot boundary");
			}
			check(meta_request_timeline(child, PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
				  reply.result == 1 &&
				  read_resolve(child, &key, 2, UINT64_MAX, 0, page, NULL) <= 0,
				  "E6 child never inherits later ordered parent growth");
		}
		close_runtime();
		check(ps_core_open(store) == 0, "E6 branch reopen ordered history");
	}
	for (int reader = 0; reader < 2; reader++)
		check(ps_retention_drop(0, 1, 96 + reader, 1) == PS_RETENTION_OK,
			  "E6 release newer reader horizons");
	check(append_relation_timeline_tag(0, &key, 0, lsn, page, 0x74, NULL) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 && page[128] == 0x74,
		  "E6 ordered writes remain latest after higher reader pins disappear");
	check(create_branch_request(4, 0, UINT64_MAX) &&
		  append_relation_timeline_tag(0, &key, 0, lsn, page, 0x79, NULL) != 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 && page[128] == 0x74,
		  "E6 unrepresentable post-branch position refuses without replacing bytes");
	close_runtime();
	remove_tree(store);
}

/*
 * F5: randomized cross-check of the (lsn, admission_seq) position index
 * (fork_event_lower_bound()/upper_bound()/identity_range()/insert_pos())
 * against the linear scans it replaces, on a private in-core fork that
 * needs no store.  See ps_test_fork_event_index_selftest()'s header comment
 * in pagestore_core.h for what each run covers.
 */
static void
test_fork_event_index_selftest(void)
{
	static const uint64_t seeds[] = {1, 2, 3};

	for (int legacy = 0; legacy <= 1; legacy++)
		for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); i++)
		{
			int			rc = ps_test_fork_event_index_selftest(seeds[i], 2000,
																4000, legacy);
			char		msg[128];

			snprintf(msg, sizeof(msg),
					 "fork-event index selftest seed=%llu legacy=%d (failed check %d)",
					 (unsigned long long) seeds[i], legacy, rc);
			check(rc == 0, msg);
		}
}

/*
 * F5: deterministic scaling guard.  K WAL-less rewrites of one block (the
 * FSM/VM pattern: one growth-class write, then K-1 inert commit-class
 * markers sharing its LSN) must cost O(log N) bisection steps per write,
 * not the O(N) linear scan the position index replaces -- a thread-local
 * step counter, not a wall clock, so the assertion is reproducible on a
 * loaded CI runner.  Sanity-run with fork_event_index_usable() forced to
 * return 0 (the counted fallback loops are the unmodified O(N) scans, so
 * this reproduces the pre-index cost exactly) confirmed all three fail
 * deterministically at K=5000: writes 25,010,000 steps (~K^2), cutover
 * 12,507,500, reopen 12,507,501 (~K^2/2 each, one triangular pass per
 * phase), all far past the K*128 ceiling (640,000).
 *
 * The K rewrites reopen with a raised flush_pages (see below) so the
 * dominant cost is the index's own O(log N) work, not memtable-flush
 * fsyncs; measured 0.25s on tmpfs and 2.5s on a disk-backed directory
 * (XFS/NVMe) for the whole case.  The wall clock is logged, not
 * asserted, below.
 */
static void
test_fork_event_index_scaling(void)
{
	char		store[] = "/tmp/psforkmetaeventscaleXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 3, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 21, 1, PS_KLASS_RELATION};
	const uint32_t K = 5000;
	uint64_t	first_seq = 0,
				second_seq = 0,
				seq = 0;
	uint64_t	steps_before,
				steps_after;
	PsRetentionPin pin;
	unsigned char page[8192];
	uint32_t	nevents = 0,
				nmarkers = 0,
				ninert = 0;
	struct timespec t0,
				t1;
	int			n;

	check(mkdtemp(store) != NULL, "create fork-event scaling store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build scaling snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build scaling manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build scaling frontier path");
	flush_pages = 1;
	compact_layers = 0;
	clock_gettime(CLOCK_MONOTONIC, &t0);
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open scaling store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 79;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier for the scaling store");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the FSM-like fork at LSN 300");

	/*
	 * flush_pages is captured by ps_memtable_create() at ps_core_open(), so
	 * flush_pages == 1 above (needed to get the frontier published from
	 * just two writes, matching every other test in this file) stays in
	 * effect for the rest of this open -- one memtable flush per K rewrite
	 * below, each with its own fsyncs.  Cheap on tmpfs, 10s-100s of seconds
	 * on a real disk (the standalone CI lane's store).  Close and reopen
	 * with a flush threshold above K: the K rewrites below then flush at
	 * most once (at close), and the fsync cost this test pays scales with
	 * K, not with K times a per-write flush.
	 */
	close_runtime();
	flush_pages = (int) K + 1000;
	check(ps_core_open(store) == 0,
		  "reopen with a flush threshold above K for the scaling rewrites");

	/* K WAL-less rewrites of block 0: the first grows the fork (activated
	 * SEG_GROW_BOUND), every later one is an inert SEG_COMMIT_BOUND at the
	 * same floor LSN -- long equal-LSN runs, the shape the index exists
	 * for. */
	steps_before = ps_test_fork_event_scan_steps();
	for (uint32_t i = 0; i < K; i++)
		check(append_relation_tag(&key, 0, 0, page, (unsigned char) i, &seq) == 0,
			  "WAL-less rewrite of block 0 for the scaling guard");
	steps_after = ps_test_fork_event_scan_steps();
	check(steps_after - steps_before < (uint64_t) K * 128,
		  "K live writes stay sublinear in scan steps (index, not O(N) scan)");

	check(ps_test_fork_event_count(0, &key, &nevents, &nmarkers, &ninert) &&
		  nevents == K + 1 && nmarkers == K && ninert == K - 1,
		  "event/marker/inert counts match the FSM growth+commit shape");

	steps_before = ps_test_fork_event_scan_steps();
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "cutover publishes for the scaling store");
	steps_after = ps_test_fork_event_scan_steps();
	check(steps_after - steps_before < (uint64_t) K * 128,
		  "cutover stays sublinear in scan steps");
	check(snapshot_ordered_marker_count(snapshots, &key, 0, 0) == (int) K,
		  "cutover retains every marker (page versions unreclaimed)");

	close_runtime();
	steps_before = ps_test_fork_event_scan_steps();
	check(ps_core_open(store) == 0, "reopen after cutover for the scaling store");
	steps_after = ps_test_fork_event_scan_steps();
	check(steps_after - steps_before < (uint64_t) K * 128,
		  "reopen replay stays sublinear in scan steps");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == (unsigned char) (K - 1),
		  "newest tag readable after reopen");

	/*
	 * A wall-clock catastrophe check would be redundant with the step-count
	 * assertions above (the real guard) and flaky across CI hardware/disk
	 * speed (M1 review finding); log it instead of asserting it.
	 */
	clock_gettime(CLOCK_MONOTONIC, &t1);
	if (t1.tv_sec - t0.tv_sec >= 60)
		fprintf(stderr,
				"WARN: test_fork_event_index_scaling took %lds (informational only; "
				"the scan-step assertions above are the real guard)\n",
				(long) (t1.tv_sec - t0.tv_sec));

	close_runtime();
	remove_tree(store);
}

/*
 * ---- recovery adopts an orphaned ordered record, with diagnostics ----
 *
 * Recovery adopts an unmatched ordered record only when the selected
 * forkmeta snapshot proves the append completed (admission_seq <= its
 * freeze_seq; see fork_meta_orphan_proven()) -- a NECESSARY filter only, not
 * proof against a torn append still in flight (review finding R2-F1: a
 * refused record's admission_seq is still observed, so a later cutover can
 * freeze past a torn sequence too) -- AND either a plain GROW with the
 * identical admission identity exists (growth class, sound unconditionally
 * on both paths, no further proof needed) or the fork's size at that
 * position already covered the block (commit class, inert marker), PLUS,
 * for the commit class only, a path-specific torn-exclusion proof: residency
 * on the image-layer path (direct adoption, as here), or "at least one
 * complete record follows it in the same segment" on the segment-suffix
 * path (recover()'s deferred look-ahead; see test_torn_commit_append_never_adopted()
 * and test_orphaned_commit_marker_segment_path_last_is_retired() below for
 * that proof's positive and negative cases).  Every other unmatched ordered
 * record is refused (layer prefix) or retires the segment tail (segment
 * suffix), with a logged tuple either way.  A store written entirely by the
 * fixed live path (Task 1 keeps memory and recovery in agreement) exercises
 * this rule only through the pruned-marker rescan path (F3,
 * RELEASE_VALIDATION.md; open follow-up) or an actual crash, not through an
 * ordinary live write.  These tests construct the degraded shapes directly,
 * through the real forkmeta snapshot publication API
 * (republish_degraded_snapshot(), modeling what a pre-fix or F3-affected
 * builder durably published) or, for the fail-closed mismatch case, a raw
 * source-log edit (patch_source_ordered_record()), then exercise
 * open/adopt/reopen.
 */

/*
 * Patch the sole forkmeta source record matching (key, admission_seq) and
 * kind SEG_GROW_BOUND/SEG_COMMIT_BOUND into a plain GROW with order_id = 0,
 * using new_admission_seq/new_nblocks for the degraded record's identity
 * (equal to the original to model the bug faithfully; deliberately different
 * to build a fail-closed mismatch case).  Sealed as a legacy FKM2 record
 * (as_legacy_record()) so the loader accepts the hand-edited bytes without
 * recomputing the daemon's CRC-24.  The store must be closed (close_runtime())
 * before calling this, and reopened by the caller afterward.
 */
static int
patch_source_ordered_record(const char *store, const PsKey *key,
							uint64_t admission_seq, uint64_t new_admission_seq,
							uint32_t new_nblocks)
{
	char path[1024];
	off_t size;
	size_t count,
				i;
	TestForkMetaRecV2 *recs;
	int patched = 0;
	int ok;

	if (snprintf(path, sizeof(path), "%s/forkmeta", store) < 0)
		return 0;
	size = file_size(path);
	if (size < 0 || size % (off_t) sizeof(TestForkMetaRecV2) != 0)
		return 0;
	count = (size_t) (size / (off_t) sizeof(TestForkMetaRecV2));
	recs = malloc(count * sizeof(TestForkMetaRecV2));
	if (recs == NULL)
		return 0;
	ok = PsStoragePosix.open(store, segment_size) == 0;
	/* fork_meta_read() returns the byte count read (like read(2)), not a
	 * 0-on-success status -- unlike fork_meta_rewrite() below. */
	for (i = 0; ok && i < count; i++)
		ok = PsStoragePosix.fork_meta_read(i * sizeof(TestForkMetaRecV2),
										   &recs[i],
										   sizeof(TestForkMetaRecV2)) ==
			(int) sizeof(TestForkMetaRecV2);
	if (ok)
	{
		for (i = 0; i < count; i++)
			if ((recs[i].kind == TEST_FEV_SEG_GROW_BOUND ||
				 recs[i].kind == TEST_FEV_SEG_COMMIT_BOUND) &&
				recs[i].admission_seq == admission_seq &&
				memcmp(&recs[i].key, key, sizeof(*key)) == 0)
			{
				as_legacy_record(&recs[i]);
				recs[i].kind = TEST_FEV_GROW;
				recs[i].order_id = 0;
				recs[i].admission_seq = new_admission_seq;
				recs[i].nblocks = new_nblocks;
				patched = 1;
				break;
			}
		ok = patched &&
			PsStoragePosix.fork_meta_rewrite(recs,
							(uint32_t) (count * sizeof(TestForkMetaRecV2))) == 0;
	}
	PsStoragePosix.close();
	free(recs);
	return ok;
}

/*
 * Revision-2 recipe: fork_event_adopt_orphaned_seg() and its commit-class
 * companion now require fork_meta_orphan_proven() -- a *selected forkmeta
 * generation* whose freeze_admission_seq proves the append completed -- so
 * constructing an orphan by hand-editing the source log with no selected
 * generation (patch_source_ordered_record() above) no longer reaches the
 * adoption rule at all.  Build the orphan through the real publication API
 * instead: read the currently selected generation's two parts, edit them in
 * memory, and publish the result as the next generation, which is exactly
 * the shape a pre-fix (or F3-affected) snapshot builder would have durably
 * published.  `degrade` rewrites the matching record to a plain GROW with
 * order_id = 0 (sealed as legacy FKM2, like patch_source_ordered_record());
 * clear it to drop the record from its part entirely (the shape of a
 * dropped commit marker, or of a growth marker whose page was never
 * admitted).  `freeze_override`, when nonzero, replaces the published
 * generation's freeze_admission_seq, letting a test simulate a sequence
 * that no real generation ever froze (a torn append in flight).
 */
typedef struct SnapshotOrphanEdit
{
	PsKey		key;
	uint64_t	admission_seq;
	int			degrade;
} SnapshotOrphanEdit;

static const SnapshotOrphanEdit *
find_snapshot_edit(const SnapshotOrphanEdit *edits, int nedits,
				   const TestForkMetaRecV2 *rec)
{
	int			i;

	for (i = 0; i < nedits; i++)
		if (rec->admission_seq == edits[i].admission_seq &&
			memcmp(&rec->key, &edits[i].key, sizeof(rec->key)) == 0)
			return &edits[i];
	return NULL;
}

static int
republish_degraded_snapshot(const char *store, const char *snapshots,
							const SnapshotOrphanEdit *edits, int nedits,
							uint64_t freeze_override)
{
	PsForkmetaSnapshot selected = {.directory_fd = -1,
		.checkpoint_fd = -1, .tail_fd = -1};
	TestSnapshotHeader hdr[2];
	unsigned char *in[2] = {NULL, NULL};
	unsigned char *out[2] = {NULL, NULL};
	uint64_t	new_records[2] = {0, 0};
	uint64_t	out_len[2] = {0, 0};
	uint64_t	new_generation;
	PsForkmetaSnapshotPrepared prepared;
	PsForkmetaSnapshotInput input[2];
	TestForkMetaRecV2 epoch;
	char		source[1024];
	unsigned int part;
	int			ok = 0;
	int			opened = 0;

	if (snprintf(source, sizeof(source), "%s/forkmeta", store) < 0)
		return 0;
	if (ps_forkmeta_snapshot_open(&selected, snapshots) != 0)
		return 0;
	new_generation = selected.generation + 1;
	for (part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		uint64_t	in_len;

		if (ps_forkmeta_snapshot_read(&selected, part, 0, &hdr[part],
									 sizeof(hdr[part])) != 0)
			goto done;
		in_len = sizeof(hdr[part]) +
			(part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
				hdr[part].checkpoint_records : hdr[part].tail_records) *
			sizeof(TestForkMetaRecV2);
		if ((in[part] = malloc((size_t) in_len)) == NULL ||
			ps_forkmeta_snapshot_read(&selected, part, 0, in[part],
									 in_len) != 0)
			goto done;
	}
	ps_forkmeta_snapshot_close(&selected);
	opened = 1;
	for (part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		uint64_t	nrec = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
			hdr[part].checkpoint_records : hdr[part].tail_records;
		TestForkMetaRecV2 *recs = (TestForkMetaRecV2 *)
			(in[part] + sizeof(hdr[part]));
		uint64_t	kept = 0;
		uint64_t	i;

		for (i = 0; i < nrec; i++)
		{
			TestForkMetaRecV2 r = recs[i];
			const SnapshotOrphanEdit *edit = find_snapshot_edit(edits, nedits, &r);

			if (edit != NULL && !edit->degrade)
				continue;		/* drop this record from its part entirely */
			if (edit != NULL && edit->degrade)
			{
				as_legacy_record(&r);
				r.kind = TEST_FEV_GROW;
				r.order_id = 0;
			}
			recs[kept++] = r;
		}
		new_records[part] = kept;
	}
	for (part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		TestSnapshotHeader new_hdr = hdr[part];

		new_hdr.generation = new_generation;
		new_hdr.checkpoint_records = new_records[PS_FORKMETA_SNAPSHOT_CHECKPOINT];
		new_hdr.tail_records = new_records[PS_FORKMETA_SNAPSHOT_TAIL];
		new_hdr.checkpoint_bytes =
			new_hdr.checkpoint_records * sizeof(TestForkMetaRecV2);
		new_hdr.tail_bytes = new_hdr.tail_records * sizeof(TestForkMetaRecV2);
		if (freeze_override != 0)
			new_hdr.freeze_admission_seq = freeze_override;
		out_len[part] = sizeof(new_hdr) +
			new_records[part] * sizeof(TestForkMetaRecV2);
		if ((out[part] = malloc((size_t) out_len[part])) == NULL)
			goto done;
		memcpy(out[part], &new_hdr, sizeof(new_hdr));
		memcpy(out[part] + sizeof(new_hdr), in[part] + sizeof(hdr[part]),
			  (size_t) (new_records[part] * sizeof(TestForkMetaRecV2)));
	}
	memset(input, 0, sizeof(input));
	input[PS_FORKMETA_SNAPSHOT_CHECKPOINT].data = out[PS_FORKMETA_SNAPSHOT_CHECKPOINT];
	input[PS_FORKMETA_SNAPSHOT_CHECKPOINT].len = out_len[PS_FORKMETA_SNAPSHOT_CHECKPOINT];
	input[PS_FORKMETA_SNAPSHOT_TAIL].data = out[PS_FORKMETA_SNAPSHOT_TAIL];
	input[PS_FORKMETA_SNAPSHOT_TAIL].len = out_len[PS_FORKMETA_SNAPSHOT_TAIL];
	if (ps_forkmeta_snapshot_prepare(&prepared, snapshots, new_generation,
								 hdr[PS_FORKMETA_SNAPSHOT_CHECKPOINT].cutoff_lsn,
								 hdr[PS_FORKMETA_SNAPSHOT_CHECKPOINT].cutoff_admission_seq,
								 &input[PS_FORKMETA_SNAPSHOT_CHECKPOINT],
								 &input[PS_FORKMETA_SNAPSHOT_TAIL]) != 0)
		goto done;
	if (ps_forkmeta_snapshot_commit(&prepared) != 0)
		goto done;
	/* Collapse the source to the bare epoch record for the new generation --
	 * the same shape fork_meta_snapshot_build() leaves behind after a real
	 * cutover (fork_meta_vec_record(source, 0, &zero_key, cutoff.lsn,
	 * cutoff.admission_seq, generation, 0, FEV_SNAPSHOT_BASE)). */
	memset(&epoch, 0, sizeof(epoch));
	epoch.rec_len = sizeof(epoch);
	epoch.timeline = 0;
	epoch.lsn = hdr[PS_FORKMETA_SNAPSHOT_CHECKPOINT].cutoff_lsn;
	epoch.admission_seq = hdr[PS_FORKMETA_SNAPSHOT_CHECKPOINT].cutoff_admission_seq;
	epoch.order_id = new_generation;
	epoch.nblocks = 0;
	epoch.kind = TEST_FEV_SNAPSHOT_BASE;
	as_legacy_record(&epoch);
	ok = PsStoragePosix.open(store, segment_size) == 0 &&
		PsStoragePosix.fork_meta_rewrite(&epoch, sizeof(epoch)) == 0;
	PsStoragePosix.close();
done:
	if (!opened)
		ps_forkmeta_snapshot_close(&selected);
	free(in[0]);
	free(in[1]);
	free(out[0]);
	free(out[1]);
	return ok;
}

/*
 * Like expect_open_failure(), but also asserts the daemon's diagnostic for a
 * refused ordered record appears on stderr, so the bare, stale-
 * errno "storage open: Invalid argument" this used to surface as cannot come
 * back silently.
 */
static int
expect_open_failure_logs(const char *store, const char *needle)
{
	char logpath[] = "/tmp/psforkmetadiagXXXXXX";
	int logfd = mkstemp(logpath);
	pid_t pid;
	int status;
	int found = 0;

	if (logfd < 0)
		return 0;
	pid = fork();
	if (pid == 0)
	{
		int rc;

		dup2(logfd, STDERR_FILENO);
		rc = ps_core_open(store);
		if (rc == 0)
			close_runtime();
		_exit(rc != 0 ? 0 : 1);
	}
	if (pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == 0)
	{
		char buf[8192];
		ssize_t n;

		if (lseek(logfd, 0, SEEK_SET) == 0 &&
			(n = read(logfd, buf, sizeof(buf) - 1)) > 0)
		{
			buf[n] = 0;
			found = strstr(buf, needle) != NULL;
		}
	}
	close(logfd);
	unlink(logpath);
	return found;
}

/*
 * Like expect_open_failure_logs(), but for an open expected to SUCCEED: it
 * cannot fork (the resulting open runtime must stay live for the caller's
 * subsequent assertions), so it redirects this process's own stderr to a
 * temp file around the ps_core_open() call instead and hands the captured
 * bytes back so the caller can count more than one needle in a single open
 * (count_occurrences() below).  *rc_out receives ps_core_open()'s return
 * value.  Returns 1 on success, 0 if the capture could not be set up (out
 * is left an empty string).
 */
static int
open_capture_stderr(const char *store, char *out, size_t out_cap, int *rc_out)
{
	char		logpath[] = "/tmp/psforkmetaopenlogXXXXXX";
	int			logfd = mkstemp(logpath);
	int			saved_stderr;
	ssize_t		n;

	out[0] = 0;
	if (logfd < 0)
		return 0;
	saved_stderr = dup(STDERR_FILENO);
	if (saved_stderr < 0)
	{
		close(logfd);
		unlink(logpath);
		return 0;
	}
	fflush(stderr);
	dup2(logfd, STDERR_FILENO);
	*rc_out = ps_core_open(store);
	fflush(stderr);
	dup2(saved_stderr, STDERR_FILENO);
	close(saved_stderr);
	if (lseek(logfd, 0, SEEK_SET) == 0 &&
		(n = read(logfd, out, out_cap - 1)) > 0)
		out[n] = 0;
	close(logfd);
	unlink(logpath);
	return 1;
}

static int
count_occurrences(const char *haystack, const char *needle)
{
	int			count = 0;
	const char *p = haystack;

	while (haystack != NULL && (p = strstr(p, needle)) != NULL)
	{
		count++;
		p += strlen(needle);
	}
	return count;
}

static void
test_orphaned_ordered_marker_adopted(void)
{
	char store[] = "/tmp/psforkmetaadoptokXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 4, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 23, 0, PS_KLASS_RELATION};
	uint64_t first_seq = 0,
				second_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	uint64_t seq = 0;
	int n;

	check(mkdtemp(store) != NULL, "create orphaned-marker adoption store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build adoption snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest),
		  "build adoption manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build adoption frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 80;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	check(append_relation_tag(&key, 0, 250, page, 0x40, &seq) == 0 && seq != 0,
		  "live clamped ordered write below the fork floor");
	check(append_growth_batch(2800, 1100) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover publishes the real bound marker (fixed live path)");
	check(read_selected_header(snapshots, &hdr) == 0 &&
		  snapshot_ordered_marker_count(snapshots, &key, seq, 1) == 1 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 1 carries the marker, not a plain GROW");
	close_runtime();
	{
		SnapshotOrphanEdit edit;

		memset(&edit, 0, sizeof(edit));
		edit.key = key;
		edit.admission_seq = seq;
		edit.degrade = 1;
		check(republish_degraded_snapshot(store, snapshots, &edit, 1, 0),
			  "republish a degraded generation modeling the pre-fix serializer output");
	}
	check(ps_core_open(store) == 0,
		  "a store with an orphaned ordered record still opens via adoption");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40, "adopted ordered page readable after open");
	check(read_selected_header(snapshots, &hdr) == 0,
		  "read the degraded generation's header");
	check(append_growth_batch(2900, 1300) &&
		  generation_advances_past(snapshots, hdr.generation),
		  "cutover after adoption");
	check(read_selected_header(snapshots, &hdr) == 0, "read post-cutover header");
	check(snapshot_ordered_marker_count(snapshots, &key, seq, 1) == 1 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "post-adoption cutover re-emits the bound marker, self-healing the store");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40, "healed store reopens and serves the ordered page");
	close_runtime();
	remove_tree(store);
}

static void
test_orphaned_ordered_marker_not_adopted_on_mismatch(void)
{
	char store[] = "/tmp/psforkmetaadoptbadXXXXXX";
	char frontier[1200];
	PsKey key = {6, 6, 6, 5, PS_KLASS_RELATION};
	PsKey pin_key = {6, 6, 24, 0, PS_KLASS_RELATION};
	uint64_t first_seq = 0,
				second_seq = 0;
	PsRetentionPin pin;
	unsigned char page[8192];
	uint64_t seq = 0;
	int n;

	check(mkdtemp(store) != NULL, "create orphaned-marker mismatch store");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build mismatch frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 81;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	check(append_relation_tag(&key, 0, 250, page, 0x40, &seq) == 0 && seq != 0,
		  "live clamped ordered write below the fork floor");
	close_runtime();
	/* A different admission_seq: the tuple no longer identifies the same
	 * append, so the rule must stay fail-closed and refuse the open. */
	check(patch_source_ordered_record(store, &key, seq, seq + 1000, 1),
		  "degrade the durable marker with a mismatched admission sequence");
	check(expect_open_failure_logs(store, "refusing unmatched ordered record"),
		  "a plain GROW at a different admission_seq is not adopted; open "
		  "still fails and logs the refused record's tuple");
	remove_tree(store);
}

/*
 * ---- F2: commit-class orphans (a second below-floor/WAL-less rewrite) ----
 *
 * A growth-class orphan's plain GROW exists in memory to promote because
 * fork_event_add() always inserts a GROW event, even a redundant one, for
 * the live (pre-fix) path.  A commit-class rewrite (segment_grows == 0)
 * never did: fork_event_add()'s early return for a GROW that does not raise
 * fork_size_asof_hop() past its nblocks means no event at that admission
 * identity was ever recorded, live-path bug or not.  So there is nothing to
 * promote by identity; fork_event_adopt_orphaned_commit_seg() instead proves
 * the record safe to admit by size (the fork's size already covered the
 * block) under the same fork_meta_orphan_proven() precondition.  This is
 * exactly the FSM/VM pattern (rewritten at every checkpoint), and it is the
 * shape the real integration store's failure (RELEASE_VALIDATION.md) and
 * the F3 pruned-marker rescan both produce.
 */

static void
test_orphaned_commit_marker_adopted(void)
{
	char		store[] = "/tmp/psforkmetacmtadoptXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 6, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 25, 0, PS_KLASS_RELATION};
	uint64_t	first_seq = 0,
				second_seq = 0;
	uint64_t	grow_seq = 0,
				commit_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	char		captured[16384];
	int			rc = -1;
	int			n;

	check(mkdtemp(store) != NULL, "create commit-orphan adoption store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build commit-adopt snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build commit-adopt manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build commit-adopt frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 82;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	check(append_relation_tag(&key, 0, 250, page, 0x40, &grow_seq) == 0 &&
		  grow_seq != 0,
		  "live clamped ordered growth write below the fork floor");
	check(append_relation_tag(&key, 0, 250, page, 0x41, &commit_seq) == 0 &&
		  commit_seq != 0 && commit_seq != grow_seq,
		  "live clamped ordered commit rewrite of the same block");
	check(append_growth_batch(3000, 1500) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover publishes both real bound markers");
	check(read_selected_header(snapshots, &hdr) == 0 &&
		  snapshot_ordered_marker_count(snapshots, &key, 0, 0) == 2 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "generation 1 carries both markers, no plain GROW");
	close_runtime();
	{
		SnapshotOrphanEdit edits[2];

		memset(edits, 0, sizeof(edits));
		edits[0].key = key;
		edits[0].admission_seq = grow_seq;
		edits[0].degrade = 1;
		edits[1].key = key;
		edits[1].admission_seq = commit_seq;
		edits[1].degrade = 0;
		check(republish_degraded_snapshot(store, snapshots, edits, 2, 0),
			  "republish a generation with the growth marker degraded and the "
			  "commit marker dropped, modeling the pre-fix serializer output");
	}
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "a store with both a growth and a commit orphan still opens via adoption");
	check(count_occurrences(captured,
			  "adopting orphaned ordered record as bound marker") == 1 &&
		  count_occurrences(captured,
			  "adopting orphaned ordered commit record as inert bound marker") == 1,
		  "exactly one growth adoption and one commit adoption line logged");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x41, "newest (commit) tag readable after adoption");
	check(read_selected_header(snapshots, &hdr) == 0,
		  "read the degraded generation's header");
	check(append_growth_batch(3100, 1700) &&
		  generation_advances_past(snapshots, hdr.generation),
		  "cutover after adoption");
	check(read_selected_header(snapshots, &hdr) == 0, "read post-cutover header");
	check(snapshot_ordered_marker_count(snapshots, &key, 0, 0) == 2 &&
		  !snapshot_has_plain_grow(snapshots, &key),
		  "post-adoption cutover re-emits both bound markers, self-healing the store");
	close_runtime();
	memset(page, 0, sizeof(page));
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x41, "healed store reopens and serves the commit tag");
	close_runtime();
	remove_tree(store);
}

/*
 * Build the commit-orphan segment-path scenario without ever cleanly closing
 * the store.  ps_core_close() (via close_runtime()) unconditionally flushes
 * every shard's memtable into a layer on its way out (see MVP_STATUS.md's
 * "page-segment record can never be the only copy" gap), so a store that is
 * built and then closed normally can never leave an ordered record
 * segment-resident for recover() to meet -- recover_layer_prefix() would see
 * it instead, exercising the wrong path entirely.  A forked child builds the
 * store and exits (_exit(), no close_runtime()) instead; the store-owner
 * lock it holds releases with the process, the same property
 * test_torn_commit_append_never_adopted() below relies on for its crash
 * lifetime.  `with_follower` writes one more ordinary page, in the same
 * shard's segment, after the commit-class write and before the cutover; the
 * child hands the two ordered writes' admission sequences back over a pipe
 * so the parent can drive republish_degraded_snapshot().
 */
static int
build_segment_resident_commit_orphan(const char *store, const char *frontier,
									 const char *manifest, const PsKey *pin_key,
									 const PsKey *key, int with_follower,
									 uint64_t *grow_seq_out,
									 uint64_t *commit_seq_out)
{
	int			pipefd[2];
	pid_t		pid;
	int			status;
	uint64_t	seqs[2] = {0, 0};

	if (pipe(pipefd) != 0)
		return 0;
	pid = fork();
	if (pid == 0)
	{
		unsigned char page[8192];
		uint64_t	first_seq = 0,
					second_seq = 0,
					third_seq = 0;
		uint64_t	grow_seq = 0,
					commit_seq = 0;
		PsRetentionPin pin;

		close(pipefd[0]);
		flush_pages = 1;
		if (setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) != 0 ||
			ps_core_open(store) != 0 ||
			!meta_request(PS_OP_CREATE, pin_key, 100, 0, 0, 0, NULL) ||
			append_relation(pin_key, 0, 100, page, &first_seq) != 0 ||
			append_relation(pin_key, 0, 200, page, &second_seq) != 0)
			_exit(1);
		memset(&pin, 0, sizeof(pin));
		pin.timeline = 0;
		pin.owner_kind = 1;
		pin.owner_id = 90;
		pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
		pin.generation = 1;
		pin.lsn = 200;
		pin.admission_seq = second_seq;
		if (ps_retention_set(&pin) != PS_RETENTION_OK ||
			!run_maintenance_until(frontier, 1))
			_exit(2);
		/* flush_pages is read once, into the memtable's fixed threshold,
		 * when ps_core_open() creates it, so reassigning the global
		 * mid-session would not change an already-open memtable's flush
		 * cadence.  Close (the pinned page history is meant to be flushed,
		 * for the frontier above) and reopen with a threshold too high for
		 * the few writes below, so they stay memtable/segment-resident;
		 * this reopen is itself a clean close and is not the one this
		 * scenario avoids. */
		close_runtime();
		flush_pages = 1000;
		if (ps_core_open(store) != 0)
			_exit(9);
		if (!meta_request(PS_OP_CREATE, key, 300, 0, 0, 0, NULL))
			_exit(3);
		if (append_relation_tag(key, 0, 250, page, 0x40, &grow_seq) != 0 ||
			grow_seq == 0)
			_exit(4);
		if (append_relation_tag(key, 0, 250, page, 0x41, &commit_seq) != 0 ||
			commit_seq == 0 || commit_seq == grow_seq)
			_exit(5);
		if (with_follower &&
			append_relation(pin_key, 0, 400, page, &third_seq) != 0)
			_exit(6);
		if (!append_growth_batch(3500, 2500) ||
			setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) != 0 ||
			!run_maintenance_until(manifest, 1))
			_exit(7);
		seqs[0] = grow_seq;
		seqs[1] = commit_seq;
		if (write(pipefd[1], seqs, sizeof(seqs)) != (ssize_t) sizeof(seqs))
			_exit(8);
		close(pipefd[1]);
		/* Deliberately no close_runtime(): a clean close would flush every
		 * record into a layer, which is exactly what this scenario must
		 * avoid. */
		_exit(0);
	}
	close(pipefd[1]);
	if (pid < 0 ||
		read(pipefd[0], seqs, sizeof(seqs)) != (ssize_t) sizeof(seqs) ||
		waitpid(pid, &status, 0) != pid ||
		!WIFEXITED(status) || WEXITSTATUS(status) != 0)
	{
		close(pipefd[0]);
		return 0;
	}
	close(pipefd[0]);
	*grow_seq_out = seqs[0];
	*commit_seq_out = seqs[1];
	return 1;
}

static void
test_orphaned_commit_marker_segment_path(void)
{
	char		store[] = "/tmp/psforkmetacmtsegXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 7, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 26, 0, PS_KLASS_RELATION};
	uint64_t	grow_seq = 0,
				commit_seq = 0;
	PsChannel	reply;
	unsigned char page[8192];
	char		captured[16384];
	int			rc = -1;
	int			n;

	check(mkdtemp(store) != NULL, "create commit-orphan segment-path store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build segment-path snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build segment-path manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build segment-path frontier path");
	check(build_segment_resident_commit_orphan(store, frontier, manifest,
											   &pin_key, &key, 1,
											   &grow_seq, &commit_seq),
		  "build a segment-resident growth+commit pair followed by another "
		  "complete record, without a clean shutdown");
	{
		SnapshotOrphanEdit edits[2];

		memset(edits, 0, sizeof(edits));
		edits[0].key = key;
		edits[0].admission_seq = grow_seq;
		edits[0].degrade = 1;
		edits[1].key = key;
		edits[1].admission_seq = commit_seq;
		edits[1].degrade = 0;
		check(republish_degraded_snapshot(store, snapshots, edits, 2, 0),
			  "republish a degraded generation for the segment-suffix path");
	}
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "a segment-resident growth+commit orphan pair still opens via adoption");
	check(count_occurrences(captured,
			  "adopting orphaned ordered record as bound marker") == 1 &&
		  count_occurrences(captured,
			  "adopting orphaned ordered commit record as inert bound marker") == 1 &&
		  count_occurrences(captured, "followed by a complete record at offset") == 1 &&
		  count_occurrences(captured, "retiring tail at offset") == 0,
		  "both records adopt on the segment path, the commit one logging "
		  "which following complete record proved it; no segment tail is retired");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x41, "newest (commit) tag readable after adoption");
	/* The inert commit marker kept its position among the equal-LSN (300)
	 * events, so a same-LSN truncate ordered after it (a newer admission_seq)
	 * still hides the block -- proving it is a real, ordered fork event and
	 * not just a page-admission side effect. */
	check(meta_request(PS_OP_TRUNCATE, &key, 300, 0, 0, 0, NULL) &&
		  meta_request(PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
		  reply.result == 0,
		  "a same-LSN truncate ordered after the inert marker still hides the block");
	close_runtime();
	remove_tree(store);
}

/*
 * Sibling of the above with nothing written after the commit-class record:
 * this is exactly the shape a torn append below (no marker append, crash) is
 * indistinguishable from at scan time, so it must stay fail-closed -- retired,
 * not adopted -- even though fork_meta_orphan_proven() and the size check
 * both hold.
 */
static void
test_orphaned_commit_marker_segment_path_last_is_retired(void)
{
	char		store[] = "/tmp/psforkmetacmtseglastXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 10, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 29, 0, PS_KLASS_RELATION};
	uint64_t	grow_seq = 0,
				commit_seq = 0;
	unsigned char page[8192];
	char		captured[16384];
	int			rc = -1;
	int			n;

	check(mkdtemp(store) != NULL, "create commit-orphan last-in-segment store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build last-in-segment snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build last-in-segment manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build last-in-segment frontier path");
	check(build_segment_resident_commit_orphan(store, frontier, manifest,
											   &pin_key, &key, 0,
											   &grow_seq, &commit_seq),
		  "build a segment-resident growth+commit pair with nothing following, "
		  "without a clean shutdown");
	{
		SnapshotOrphanEdit edits[2];

		memset(edits, 0, sizeof(edits));
		edits[0].key = key;
		edits[0].admission_seq = grow_seq;
		edits[0].degrade = 1;
		edits[1].key = key;
		edits[1].admission_seq = commit_seq;
		edits[1].degrade = 0;
		check(republish_degraded_snapshot(store, snapshots, edits, 2, 0),
			  "republish the same degraded generation with no follower");
	}
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "open still succeeds: the growth record adopts on its own");
	check(count_occurrences(captured,
			  "adopting orphaned ordered record as bound marker") == 1 &&
		  count_occurrences(captured,
			  "adopting orphaned ordered commit record") == 0 &&
		  count_occurrences(captured,
			  "no complete record follows in this segment") == 1,
		  "the commit record, last in its segment, is retired instead of "
		  "adopted -- torn-indistinguishable, so it must fail closed");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40,
		  "the block still serves the older (growth) tag, not the retired one");
	close_runtime();
	remove_tree(store);
}

static void
test_orphaned_commit_marker_not_proven(void)
{
	char		store[] = "/tmp/psforkmetacmtunprovenXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 8, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 27, 0, PS_KLASS_RELATION};
	uint64_t	first_seq = 0,
				second_seq = 0;
	uint64_t	grow_seq = 0,
				commit_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	int			n;

	check(mkdtemp(store) != NULL, "create commit-orphan not-proven store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build not-proven snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build not-proven manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build not-proven frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 84;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	check(append_relation_tag(&key, 0, 250, page, 0x40, &grow_seq) == 0 &&
		  grow_seq != 0,
		  "live clamped ordered growth write below the fork floor");
	check(append_relation_tag(&key, 0, 250, page, 0x41, &commit_seq) == 0 &&
		  commit_seq != 0 && commit_seq != grow_seq,
		  "live clamped ordered commit rewrite of the same block");
	check(append_growth_batch(3300, 2100) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover publishes both real bound markers");
	check(read_selected_header(snapshots, &hdr) == 0,
		  "read generation 1 header");
	close_runtime();
	{
		SnapshotOrphanEdit edits[2];

		memset(edits, 0, sizeof(edits));
		edits[0].key = key;
		edits[0].admission_seq = grow_seq;
		edits[0].degrade = 1;
		edits[1].key = key;
		edits[1].admission_seq = commit_seq;
		edits[1].degrade = 0;
		/* freeze_admission_seq = grow_seq: the growth orphan is still proven
		 * (grow_seq <= freeze), but the commit orphan is not (commit_seq >
		 * freeze) -- exactly a torn append's signature, which must stay
		 * fail-closed even though its size and identity would otherwise
		 * qualify. */
		check(republish_degraded_snapshot(store, snapshots, edits, 2, grow_seq),
			  "republish a generation whose freeze does not cover the commit orphan");
	}
	check(expect_open_failure_logs(store, "refusing unmatched ordered record"),
		  "a commit orphan above the selected generation's freeze is not "
		  "adopted; open still fails and logs the refused record's tuple");
	remove_tree(store);
}

static void
test_orphaned_commit_marker_size_mismatch(void)
{
	char		store[] = "/tmp/psforkmetacmtsizeXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 9, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 28, 0, PS_KLASS_RELATION};
	uint64_t	first_seq = 0,
				second_seq = 0;
	uint64_t	grow_seq = 0,
				commit_seq = 0;
	PsRetentionPin pin;
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	int			n;

	check(mkdtemp(store) != NULL, "create commit-orphan size-mismatch store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build size-mismatch snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build size-mismatch manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build size-mismatch frontier path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &first_seq) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &second_seq) == 0,
		  "open store and write pinned page history");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 85;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "publish a safe frontier");
	check(meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL),
		  "create the ordered fork at LSN 300");
	check(append_relation_tag(&key, 0, 250, page, 0x40, &grow_seq) == 0 &&
		  grow_seq != 0,
		  "live clamped ordered growth write below the fork floor");
	check(append_relation_tag(&key, 0, 250, page, 0x41, &commit_seq) == 0 &&
		  commit_seq != 0 && commit_seq != grow_seq,
		  "live clamped ordered commit rewrite of the same block");
	check(append_growth_batch(3400, 2300) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "first cutover publishes both real bound markers");
	check(read_selected_header(snapshots, &hdr) == 0,
		  "read generation 1 header");
	close_runtime();
	{
		SnapshotOrphanEdit edits[2];

		memset(edits, 0, sizeof(edits));
		/* Both omitted entirely (not degraded): no GROW event of any kind
		 * survives at grow_seq, so the fork's size at this position is 0
		 * and the commit rule's size check (not just the freeze proof)
		 * must be what refuses the open. */
		edits[0].key = key;
		edits[0].admission_seq = grow_seq;
		edits[0].degrade = 0;
		edits[1].key = key;
		edits[1].admission_seq = commit_seq;
		edits[1].degrade = 0;
		check(republish_degraded_snapshot(store, snapshots, edits, 2, 0),
			  "republish a generation with both markers dropped and no plain GROW");
	}
	check(expect_open_failure_logs(store, "refusing unmatched ordered record"),
		  "no size ever covered the block, so neither orphan rule adopts; "
		  "open still fails and logs the refused record's tuple");
	remove_tree(store);
}

/*
 * ---- torn append vs. commit-class adoption (review finding R2-F1) ----
 *
 * A crash between a commit-class record's segment body write and its marker
 * append (fork_meta_persist_segment()) leaves complete, well-formed bytes on
 * disk with no marker -- exactly what an F3 pruned survivor also looks like
 * to a rescan.  fork_meta_orphan_proven() alone cannot tell them apart: the
 * torn record's admission_seq is observed (segment_order_id_observe()/
 * admission_seq_observe()) on the very recovery pass that refuses it, and a
 * later cutover in that same lifetime freezes past it, so the predicate
 * alone would call it "proven" on any later rescan -- see its header
 * comment.  The real, structural proof recover() uses instead: a torn body
 * is always the last complete record of its segment (append_page_impl()
 * advances the shard cursor only after the marker append succeeded), so it
 * is retired again on every later rescan, not adopted, until a flush or a
 * new segment moves past it.  `./torn_test 8` under `$SP/review-fsm/` is
 * the reviewer's original standalone reproduction of this scenario against
 * `PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES` (storage_posix.c); this is that
 * scenario folded into the suite, plus a check that a later write still
 * lands cleanly past the retired tail.
 */
static void
test_torn_commit_append_never_adopted(void)
{
	char		store[] = "/tmp/psforkmetatorncommitXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 11, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 30, 0, PS_KLASS_RELATION};
	TestSnapshotHeader hdr;
	unsigned char page[8192];
	char		captured[16384];
	pid_t		pid;
	int			status;
	int			rc = -1;
	uint64_t	new_seq = 0;
	int			n;

	check(mkdtemp(store) != NULL, "create torn-commit store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build torn-commit snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build torn-commit manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build torn-commit frontier path");
	flush_pages = 1;
	/* lifetime 1 (child): CREATE@300, an acknowledged clamped write of block
	 * 0 (tag 0x40), then a second clamped rewrite of the same block (tag
	 * 0x41, commit-class) crashed after its 8th successful segment write --
	 * its body, before fork_meta_persist_segment() ever runs. */
	pid = fork();
	if (pid == 0)
	{
		uint64_t	first_seq = 0,
					second_seq = 0,
					grow_seq = 0,
					commit_seq = 0;
		PsRetentionPin pin;

		setenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES", "8", 1);
		setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
		if (ps_core_open(store) != 0)
			_exit(1);
		if (!meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL))
			_exit(2);
		if (append_relation(&pin_key, 0, 100, page, &first_seq) != 0)
			_exit(3);
		if (append_relation(&pin_key, 0, 200, page, &second_seq) != 0)
			_exit(4);
		memset(&pin, 0, sizeof(pin));
		pin.timeline = 0;
		pin.owner_kind = 1;
		pin.owner_id = 91;
		pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
		pin.generation = 1;
		pin.lsn = 200;
		pin.admission_seq = second_seq;
		if (ps_retention_set(&pin) != PS_RETENTION_OK ||
			!run_maintenance_until(frontier, 1))
			_exit(5);
		if (!meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL))
			_exit(6);
		if (append_relation_tag(&key, 0, 250, page, 0x40, &grow_seq) != 0 ||
			grow_seq == 0)
			_exit(7);
		(void) append_relation_tag(&key, 0, 250, page, 0x41, &commit_seq);
		_exit(10);		/* not reached: the crash hook exits 86 first */
	}
	check(waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
		  WEXITSTATUS(status) == 86,
		  "child crashed after the torn commit-class body reached the segment");
	unsetenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES");
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);

	/* lifetime 2: no forkmeta generation is selected yet, so
	 * fork_meta_orphan_proven() is trivially false and the torn record is
	 * refused immediately -- not even stashed as a look-ahead candidate. */
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "lifetime 2 opens");
	check(count_occurrences(captured, "retiring tail at offset") == 1 &&
		  count_occurrences(captured, "no complete record follows") == 0 &&
		  count_occurrences(captured, "adopting orphaned ordered commit record") == 0,
		  "lifetime 2 retires the torn tail once, adopting nothing");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40, "lifetime 2 serves the committed tag, not the torn one");
	check(append_growth_batch(3600, 2700) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "lifetime 2 cutover (its freeze now covers the torn record's sequence)");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 1 header");
	close_runtime();

	/* lifetime 3: fork_meta_orphan_proven() now holds for the torn record --
	 * this is the reviewer's finding -- but the segment path's own proof
	 * does not: nothing follows the retired tail (the crash truncated the
	 * segment exactly there), so it is retired again, not adopted. */
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "lifetime 3 opens");
	check(count_occurrences(captured, "retiring tail at offset") == 1 &&
		  count_occurrences(captured, "no complete record follows in this segment") == 1 &&
		  count_occurrences(captured, "adopting orphaned ordered commit record") == 0,
		  "lifetime 3 retires the torn tail again, with no complete record "
		  "following it in the segment, and still adopts nothing");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x40, "lifetime 3 still serves the committed tag");

	/* A fresh write after the retired tail must still land and be readable:
	 * the retired-segment sentinel (cur_off = segment_size) rolls it into a
	 * new segment rather than reusing the poisoned tail. */
	check(append_relation_tag(&key, 0, 250, page, 0x42, &new_seq) == 0 &&
		  new_seq != 0,
		  "a fresh write past the retired torn segment succeeds");
	close_runtime();
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
	check(ps_core_open(store) == 0, "lifetime 4 opens");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x42, "lifetime 4 serves the fresh write in its new segment");
	close_runtime();
	remove_tree(store);
}

/*
 * Companion to the above: the torn record is a growth-class write instead
 * (block 1, extending the fork past its acknowledged size 1).  The growth
 * rule needs no look-ahead proof -- it is sound by construction on both
 * paths (no durable source can ever hold a degraded GROW at a torn
 * sequence) -- so this never reaches the pending mechanism at all: the
 * commit-rule's own size check (fork_size_asof_hop() < block + 1, since
 * block 1 was never truly grown) refuses it independently of proof state,
 * on every lifetime, so neither retire message ever gains the "no complete
 * record follows" suffix here.
 */
static void
test_torn_growth_append_never_adopted(void)
{
	char		store[] = "/tmp/psforkmetatorngrowthXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		key = {6, 6, 6, 12, PS_KLASS_RELATION};
	PsKey		pin_key = {6, 6, 31, 0, PS_KLASS_RELATION};
	TestSnapshotHeader hdr;
	PsChannel	reply;
	unsigned char page[8192];
	char		captured[16384];
	pid_t		pid;
	int			status;
	int			rc = -1;
	int			n;

	check(mkdtemp(store) != NULL, "create torn-growth store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build torn-growth snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build torn-growth manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build torn-growth frontier path");
	flush_pages = 1;
	pid = fork();
	if (pid == 0)
	{
		uint64_t	first_seq = 0,
					second_seq = 0,
					grow_seq = 0;
		PsRetentionPin pin;

		setenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES", "8", 1);
		setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
		if (ps_core_open(store) != 0)
			_exit(1);
		if (!meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL))
			_exit(2);
		if (append_relation(&pin_key, 0, 100, page, &first_seq) != 0)
			_exit(3);
		if (append_relation(&pin_key, 0, 200, page, &second_seq) != 0)
			_exit(4);
		memset(&pin, 0, sizeof(pin));
		pin.timeline = 0;
		pin.owner_kind = 1;
		pin.owner_id = 92;
		pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
		pin.generation = 1;
		pin.lsn = 200;
		pin.admission_seq = second_seq;
		if (ps_retention_set(&pin) != PS_RETENTION_OK ||
			!run_maintenance_until(frontier, 1))
			_exit(5);
		if (!meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0, NULL))
			_exit(6);
		if (append_relation_tag(&key, 0, 250, page, 0x40, &grow_seq) != 0 ||
			grow_seq == 0)
			_exit(7);
		/* Torn: extends the fork from 1 to 2 blocks; crashes after the
		 * body of block 1's write, before its marker. */
		(void) append_relation_tag(&key, 1, 250, page, 0x50, &grow_seq);
		_exit(10);		/* not reached */
	}
	check(waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
		  WEXITSTATUS(status) == 86,
		  "child crashed after the torn growth-class body reached the segment");
	unsetenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES");
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);

	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "lifetime 2 opens");
	check(count_occurrences(captured, "retiring tail at offset") == 1 &&
		  count_occurrences(captured, "no complete record follows") == 0 &&
		  count_occurrences(captured, "adopting orphaned ordered") == 0,
		  "lifetime 2 retires the torn growth tail, adopting nothing");
	check(meta_request(PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
		  reply.result == 1, "lifetime 2: NBLOCKS stays 1, the torn growth never counted");
	check(append_growth_batch(3700, 2900) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "lifetime 2 cutover");
	check(read_selected_header(snapshots, &hdr) == 0, "read generation 1 header");
	close_runtime();

	/* lifetime 3: fork_meta_orphan_proven() now holds, but the growth rule
	 * still finds no plain GROW to promote and the commit rule's size check
	 * still fails (the fork's size never covered block 1) -- the size proof,
	 * not just the segment look-ahead, keeps this fail-closed regardless of
	 * lifetime, so the message stays the plain, non-look-ahead form. */
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) && rc == 0,
		  "lifetime 3 opens");
	check(count_occurrences(captured, "retiring tail at offset") == 1 &&
		  count_occurrences(captured, "no complete record follows") == 0 &&
		  count_occurrences(captured, "adopting orphaned ordered") == 0,
		  "lifetime 3 retires the torn growth tail again, still adopting nothing");
	check(meta_request(PS_OP_NBLOCKS, &key, 0, 0, 0, 0, &reply) &&
		  reply.result == 1, "lifetime 3: NBLOCKS still 1");
	close_runtime();
	remove_tree(store);
}

/*
 * Small artifact-lifecycle op wrappers, matching pagestore_artifact_
 * lifecycle_test.c's begin()/write_page()/commit()/read_value() but keyed
 * by an explicit PsKey argument (this file has no single shared `key`).
 */
static int
artifact_begin_request(const PsKey *key, uint64_t lsn, uint64_t *token,
					   PsArtifactRefuseReason *reason)
{
	int rc;

	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(key));
	rc = ps_artifact_begin(0, key, lsn, token, reason);
	ps_unlock_shard(ps_shard_of(key));
	ps_admission_read_unlock();
	return rc;
}

static int
artifact_write_request(const PsKey *key, uint32_t block, uint64_t lsn,
					   uint64_t token, int fill, PsArtifactRefuseReason *reason)
{
	unsigned char page[8192];
	int rc;

	memset(page, fill, sizeof(page));
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(key));
	rc = ps_artifact_write(0, key, block, page, lsn, token, NULL, reason);
	ps_unlock_shard(ps_shard_of(key));
	ps_admission_read_unlock();
	return rc;
}

static int
artifact_commit_request(const PsKey *key, uint64_t lsn, uint64_t token,
						uint64_t count, PsArtifactRefuseReason *reason)
{
	int rc;

	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(key));
	rc = ps_artifact_commit(0, key, lsn, token, count, reason);
	ps_unlock_shard(ps_shard_of(key));
	ps_admission_read_unlock();
	return rc;
}

/* 1 = read back and matches fill, 0 = absent, -1 = error or content mismatch. */
static int
artifact_read_at(const PsKey *key, uint64_t horizon, uint32_t block, int fill)
{
	unsigned char page[8192];
	uint64_t lsn = 0;
	int rc;

	ps_lock_shard_rd(ps_shard_of(key));
	rc = read_resolve(0, key, block, horizon, 0, page, &lsn);
	ps_unlock_shard(ps_shard_of(key));
	if (rc != 1)
		return rc;
	for (size_t i = 0; i < sizeof(page); i++)
		if (page[i] != (unsigned char) fill)
			return -1;
	return 1;
}

/*
 * T5: the forkmeta cutoff derivation (2.2/2.4) implies cutoff <= frontier <=
 * floor <= every active same-timeline page-history pin, so a generation at a
 * PINNED LSN is never below the cutoff and publishes cleanly after a
 * cutover; an unpinned LSN just below that same pin is refused, by name, as
 * UNFENCED (not the forkmeta growth check -- the data-page fence already
 * refuses it), and a generation exactly AT the cutoff LSN is future and
 * admitted.  Mirrors repro_b.c scenario 2, plus the BEGIN/WRITE/COMMIT
 * coverage repro_b did not have room for.
 */
static void
test_artifact_generation_vs_cutoff(void)
{
	char		store[] = "/tmp/psforkmetaartifactcutoffXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		pin_key = {6, 6, 900, 0, PS_KLASS_RELATION};
	PsKey		keyA = {0, 0, 901, 0, PS_KLASS_READER_SNAPSHOT};
	PsKey		keyB = {0, 0, 902, 0, PS_KLASS_READER_SNAPSHOT};
	PsKey		keyC = {0, 0, 903, 0, PS_KLASS_READER_SNAPSHOT};
	PsKey		keyD = {0, 0, 904, 0, PS_KLASS_READER_SNAPSHOT};
	PsRetentionPin pin;
	unsigned char page[8192];
	TestSnapshotHeader hdr;
	uint64_t	s100 = 0, s150 = 0, s200 = 0, token = 0;
	uint64_t	f = 0, fs = 0;
	PsArtifactRefuseReason reason;
	int			n;

	check(mkdtemp(store) != NULL, "T5: create artifact-vs-cutoff store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "T5: build snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "T5: build manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "T5: build frontier path");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "T5: open store, defer cutover");
	check(meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &s100) == 0 &&
		  append_relation(&pin_key, 0, 150, page, &s150) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &s200) == 0,
		  "T5: relation history at 100/150/200");

	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = PS_RETENTION_OWNER_READER;
	pin.owner_id = 78;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 150;
	pin.admission_seq = s150;
	check(ps_retention_set(&pin) == PS_RETENTION_OK, "T5: pin@150 (owner 78)");
	pin.owner_id = 77;
	pin.lsn = 200;
	pin.admission_seq = s200;
	check(ps_retention_set(&pin) == PS_RETENTION_OK, "T5: pin@200 (owner 77)");
	check(run_maintenance_until(frontier, 1), "T5: durable frontier published");
	check(ps_test_page_frontier(0, &f, &fs) != 0 && f == 150,
		  "T5: the earlier pin@150 holds the durable frontier down to 150");

	check(append_growth_batch(2200, 500) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1) &&
		  read_selected_header(snapshots, &hdr) == 0,
		  "T5: forkmeta cutover");
	check(hdr.cutoff_lsn == 150,
		  "T5: the cutoff lands exactly at the frontier, which the pin@150 holds at 150");

	/* A new key's generation at the pinned LSN publishes cleanly after the
	 * cutover: it is never below the cutoff. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyA, 150, &token, &reason) == 0 && token != 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE,
		  "T5: BEGIN at the pinned LSN 150 publishes after the cutover");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_write_request(&keyA, 0, 150, token, 0xA0, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T5: keyA WRITE block 0");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_write_request(&keyA, 1, 150, token, 0xA1, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T5: keyA WRITE block 1");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_commit_request(&keyA, 150, token, 2, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T5: keyA COMMIT");
	check(artifact_read_at(&keyA, 150, 0, 0xA0) == 1 &&
		  artifact_read_at(&keyA, 150, 1, 0xA1) == 1,
		  "T5: keyA reads back at exactly 150");

	/* A new key's BEGIN one below the pin is refused by name, not admitted:
	 * the data-page fence (not the forkmeta growth check) is what applies
	 * pre-cutover-derivation to an unpinned LSN; the fence refuses it at
	 * BEGIN time now (B2), so it never poisons. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyB, 149, &token, &reason) != 0 &&
		  reason == PS_ARTIFACT_REFUSE_UNFENCED,
		  "T5: BEGIN one below the pin at 149 is refused as UNFENCED");

	/* Collateral check: the refusal above did not poison anything. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyC, 500, &token, &reason) == 0 && token != 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE,
		  "T5: BEGIN on another key at 500 still succeeds after the refusal above");
	check(artifact_read_at(&keyA, 150, 0, 0xA0) == 1,
		  "T5: keyA's committed generation is still readable after the refusal");

	/* A generation exactly AT the cutoff LSN is future (fresh admission
	 * sequence) and publishes. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyD, hdr.cutoff_lsn, &token, &reason) == 0 &&
		  token != 0 && reason == PS_ARTIFACT_REFUSE_NONE,
		  "T5: BEGIN exactly at the cutoff LSN publishes");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_write_request(&keyD, 0, hdr.cutoff_lsn, token, 0xD0, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T5: keyD WRITE at the cutoff LSN");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_commit_request(&keyD, hdr.cutoff_lsn, token, 1, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T5: keyD COMMIT at the cutoff LSN");

	/* Drop pin 78 from the raw registry to close out this scenario's state.
	 * This is the raw registry drop (ps_retention_drop()), not the daemon's
	 * PS_OP_RETENTION_PIN_DROP meta op: it removes the pin but does not mark
	 * page-prune due, so it does not itself move the frontier, and nothing
	 * here asserts that it does.  T7 (test_artifact_write_unfenced_after_
	 * pin_drop, below) is the test that drops a pin through the daemon op
	 * specifically to observe the frontier advance past it. */
	check(ps_retention_drop(0, PS_RETENTION_OWNER_READER, 78, 1) == PS_RETENTION_OK,
		  "T5: drop pin 78");
	close_runtime();
	remove_tree(store);
}

/*
 * T6 (artificial): the one theoretical gap in 2.2's implication chain -- a
 * live child that already has its own durable frontier while the parent's
 * frontier has passed the branch point -- is not reachable through the
 * daemon's own admission gates (a new same-timeline pin below the durable
 * frontier is refused by page_frontier_ancestry_allows(), core.c:6015).  To
 * exercise PS_ARTIFACT_REFUSE_FORKMETA_CUTOFF at all, this test installs a
 * pin below the frontier through the raw registry write ps_retention_set()
 * directly, bypassing that daemon gate -- not a claim that a real producer
 * can reach this path, only a way to prove the reason is reported correctly
 * and does not poison when the (structural, defence-in-depth) forkmeta
 * growth check is what fires instead of the data-page fence.
 */
static void
test_artifact_forkmeta_cutoff_reason(void)
{
	char		store[] = "/tmp/psforkmetaartifactreasonXXXXXX";
	char		snapshots[1024];
	char		manifest[1200];
	char		frontier[1200];
	PsKey		pin_key = {6, 6, 910, 0, PS_KLASS_RELATION};
	PsKey		keyE = {0, 0, 911, 0, PS_KLASS_READER_SNAPSHOT};
	PsKey		keyF = {0, 0, 912, 0, PS_KLASS_READER_SNAPSHOT};
	PsRetentionPin pin;
	unsigned char page[8192];
	TestSnapshotHeader hdr;
	uint64_t	s100 = 0, s200 = 0, token = 0;
	uint32_t	fence_before;
	PsArtifactRefuseReason reason;
	int			n;

	check(mkdtemp(store) != NULL, "T6: create forkmeta-cutoff-reason store");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "T6: build snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "T6: build manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "T6: build frontier path");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "T6: open store, defer cutover");
	check(meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &s100) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &s200) == 0,
		  "T6: relation history at 100/200");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = PS_RETENTION_OWNER_READER;
	pin.owner_id = 77;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = s200;
	check(ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1), "T6: pin@200, durable frontier published");
	check(append_growth_batch(2300, 600) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1) &&
		  read_selected_header(snapshots, &hdr) == 0,
		  "T6: forkmeta cutover, cutoff == frontier (200)");
	check(hdr.cutoff_lsn == 200, "T6: cutoff lands exactly at the frontier");

	/* Bypass the daemon's own admission gate (see the comment above) to
	 * install a pin below the now-fixed cutoff. */
	pin.owner_id = 78;
	pin.lsn = 150;
	pin.admission_seq = s100;
	check(ps_retention_set(&pin) == PS_RETENTION_OK,
		  "T6: raw registry pin@150, below the cutoff (bypasses the daemon gate)");

	fence_before = ps_test_artifact_fence_count(0);
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyE, 150, &token, &reason) != 0 &&
		  reason == PS_ARTIFACT_REFUSE_FORKMETA_CUTOFF,
		  "T6: the data-page fence admits (pinned), but the forkmeta growth check refuses -- named FORKMETA_CUTOFF");
	check(ps_test_artifact_fence_count(0) == fence_before,
		  "T6: the meta record path never reserves a fence, so the refusal changed nothing");

	/* And, critically, it did not poison: another key's BEGIN still works. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyF, 300, &token, &reason) == 0 && token != 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE,
		  "T6: BEGIN on another key at 300 still succeeds -- FORKMETA_CUTOFF did not poison");

	close_runtime();
	remove_tree(store);
}

/*
 * T7: the ps_artifact_write() poisoning boundary (pagestore_artifact_
 * lifecycle.inc, append_page_raw_outcome() call in ps_artifact_write()).
 * B2's BEGIN-time fence gate means an unfenced *new* generation is now
 * refused at BEGIN, so T1 (pagestore_artifact_lifecycle_test.c) never
 * reaches append_page_raw_outcome()'s UNFENCED outcome on the WRITE side at
 * all -- it is dead in that file after B2.  The one remaining way to reach
 * it is a genuine TOCTOU race B2 cannot close: BEGIN is admitted while an
 * LSN is fenced (here, by an active page-history pin), and the fence is
 * then legitimately withdrawn (the pin is dropped, and the frontier is
 * free to advance past that LSN) before the attempt's data WRITE runs. That
 * WRITE must still be refused UNFENCED and must NOT poison, exactly like a
 * BEGIN-time refusal.  This mirrors the review's repro_c.c scenario
 * (<scratchpad>/review-refusal/repro_c.c): restoring blanket poisoning in
 * ps_artifact_write() (poisoning on any nonzero append_page_raw_outcome()
 * rc, not just PS_APPEND_IO_FAILED) passes every other lifecycle/cutover
 * test unchanged but fails this one.
 */
static void
test_artifact_write_unfenced_after_pin_drop(void)
{
	char		store[] = "/tmp/psforkmetaartifactraceXXXXXX";
	char		frontier[1200];
	PsKey		pin_key = {6, 6, 920, 0, PS_KLASS_RELATION};
	PsKey		keyG = {0, 0, 921, 0, PS_KLASS_READER_SNAPSHOT};
	PsKey		keyH = {0, 0, 922, 0, PS_KLASS_READER_SNAPSHOT};
	PsRetentionPin pin;
	unsigned char page[8192];
	uint64_t	s100 = 0,
				s150 = 0,
				s200 = 0;
	uint64_t	token = 0,
				token2 = 0;
	uint64_t	f = 0,
				fs = 0;
	uint32_t	fence_before;
	PsArtifactRefuseReason reason;
	int			n;

	check(mkdtemp(store) != NULL, "T7: create write-race store");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "T7: build frontier path");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "T7: open store, no cutover in this scenario");
	check(meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0, NULL) &&
		  append_relation(&pin_key, 0, 100, page, &s100) == 0 &&
		  append_relation(&pin_key, 0, 150, page, &s150) == 0 &&
		  append_relation(&pin_key, 0, 200, page, &s200) == 0,
		  "T7: relation history at 100/150/200");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = PS_RETENTION_OWNER_READER;
	pin.owner_id = 78;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 150;
	pin.admission_seq = s150;
	check(ps_retention_set(&pin) == PS_RETENTION_OK, "T7: pin@150 (owner 78)");
	pin.owner_id = 77;
	pin.lsn = 200;
	pin.admission_seq = s200;
	check(ps_retention_set(&pin) == PS_RETENTION_OK, "T7: pin@200 (owner 77)");
	check(run_maintenance_until(frontier, 1), "T7: durable frontier published");
	check(ps_test_page_frontier(0, &f, &fs) != 0 && f == 150,
		  "T7: pin@150 holds the frontier at 150");

	/* BEGIN at the pinned LSN: admitted (the fence holds). */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token = 0;
	check(artifact_begin_request(&keyG, 150, &token, &reason) == 0 && token != 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE,
		  "T7: BEGIN at the pinned LSN 150 is admitted");

	/* Drop the pin through the daemon meta op (PS_OP_RETENTION_PIN_DROP),
	 * not the raw registry ps_retention_drop(): only the daemon op marks
	 * page-prune due, which is what lets maintenance actually move the
	 * frontier past 150 below. */
	{
		PsChannel	ch;

		memset(&ch, 0, sizeof(ch));
		ch.opcode = PS_OP_RETENTION_PIN_DROP;
		ch.timeline = 0;
		ch.key = pin_key;
		ch.blocknum = PS_RETENTION_OWNER_READER;
		ch.req_seq = 78;
		ch.old_nblocks = 1;
		ch.status = PS_STATUS_OK;
		ps_lock_shard_rd(ps_shard_of(&pin_key));
		(void) ps_handle_meta(&ch);
		ps_unlock_shard(ps_shard_of(&pin_key));
		check(ch.status == PS_STATUS_OK, "T7: drop pin 78 via the daemon meta op");
	}
	for (int i = 0; i < 40; i++)
	{
		(void) ps_core_maintenance();
		(void) ps_test_page_frontier(0, &f, &fs);
		if (f > 150)
			break;
		usleep(50000);
	}
	check(f > 150, "T7: the frontier moves past 150 once the pin is dropped");

	/* The attempt's data WRITE now lands after the fence that admitted its
	 * BEGIN has been legitimately withdrawn: refused UNFENCED, not poisoned,
	 * and no fence leaked. */
	fence_before = ps_test_artifact_fence_count(0);
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_write_request(&keyG, 0, 150, token, 0x70, &reason) != 0 &&
		  reason == PS_ARTIFACT_REFUSE_UNFENCED,
		  "T7: the WRITE, not the already-admitted BEGIN, is refused UNFENCED");
	check(ps_test_artifact_fence_count(0) == fence_before,
		  "T7: the refused WRITE leaked no artifact control-era fence");

	/* Not poisoned: another key's BEGIN still succeeds, and keyG's own
	 * earlier-completed sibling state is unaffected (nothing else was ever
	 * published here, so check a fresh unrelated key instead). */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token2 = 0;
	check(artifact_begin_request(&keyH, 400, &token2, &reason) == 0 && token2 != 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE,
		  "T7: BEGIN on another key still succeeds -- the WRITE refusal did not poison");

	/* The open attempt on keyG is recoverable: a fresh BEGIN at a newer,
	 * fenced LSN supersedes it and completes normally. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	token2 = 0;
	check(artifact_begin_request(&keyG, 500, &token2, &reason) == 0 && token2 != 0 &&
		  token2 != token && reason == PS_ARTIFACT_REFUSE_NONE,
		  "T7: a fresh BEGIN at 500 supersedes the refused attempt");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_write_request(&keyG, 0, 500, token2, 0x75, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T7: its WRITE succeeds");
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_commit_request(&keyG, 500, token2, 1, &reason) == 0 &&
		  reason == PS_ARTIFACT_REFUSE_NONE, "T7: its COMMIT succeeds");
	check(artifact_read_at(&keyG, 500, 0, 0x75) == 1,
		  "T7: the superseding generation reads back at 500");

	/* The old, superseded token is simply dead -- not poisoned, not
	 * ambiguous. */
	reason = PS_ARTIFACT_REFUSE_NONE;
	check(artifact_write_request(&keyG, 0, 150, token, 0x70, &reason) != 0 &&
		  reason == PS_ARTIFACT_REFUSE_ATTEMPT_MISMATCH,
		  "T7: the old, superseded token is refused as ATTEMPT_MISMATCH");

	/* Recovery is unaffected: reopen and the completed generation survives. */
	close_runtime();
	check(ps_core_open(store) == 0, "T7: reopen");
	check(artifact_read_at(&keyG, 500, 0, 0x75) == 1,
		  "T7: the completed generation at 500 survives reopen");

	close_runtime();
	remove_tree(store);
}

/*
 * ---- T1: the hole record format and its readers ----
 *
 * A hole is written only by page_cleanup_tombstone_segment(); these tests
 * hand-craft one directly with the low-level storage ops (as
 * test_v1_bound_marker_snapshot() does for legacy bodies above) to test the
 * reader in isolation from the writer: recover() must skip a well-formed
 * hole between two live records without observing anything in it, and must
 * fail closed on one whose len does not match page_size.
 */
static void
test_hole_record_skipped_on_reopen(void)
{
	char		store[] = "/tmp/psforkmetaholeXXXXXX";
	PsKey		before_key = {9, 9, 101, 0, PS_KLASS_RELATION};
	PsKey		hole_key = {9, 9, 102, 0, PS_KLASS_RELATION};
	PsKey		after_key = {9, 9, 103, 0, PS_KLASS_RELATION};
	TestSegRecHdr before_hdr;
	TestSegRecHdr hole_hdr;
	TestSegRecHdr after_hdr;
	unsigned char page[8192];
	unsigned char pages[3][8192];
	char		captured[16384];
	int64_t		seg_off;
	uint64_t	offsets[3];
	int			rc = -1;

	check(mkdtemp(store) != NULL, "create hole-record store");
	check(ps_core_open(store) == 0, "bootstrap hole-record store");
	close_runtime();
	memset(&before_hdr, 0, sizeof(before_hdr));
	before_hdr.magic = TEST_SEG_MAGIC;
	before_hdr.timeline = 0;
	before_hdr.key = before_key;
	before_hdr.len = sizeof(page);
	before_hdr.lsn = 500;
	memset(&hole_hdr, 0, sizeof(hole_hdr));
	hole_hdr.magic = TEST_SEG_HOLE48_MAGIC;
	hole_hdr.timeline = 0;
	hole_hdr.key = hole_key;
	hole_hdr.len = sizeof(page);	/* well-formed: matches page_size */
	hole_hdr.lsn = 999;
	memset(&after_hdr, 0, sizeof(after_hdr));
	after_hdr.magic = TEST_SEG_MAGIC;
	after_hdr.timeline = 0;
	after_hdr.key = after_key;
	after_hdr.len = sizeof(page);
	after_hdr.lsn = 600;
	memset(pages, 0, sizeof(pages));
	pages[0][128] = 0xa1;
	/* pages[1] (the hole's body) stays zero -- a hole's body is never read */
	pages[2][128] = 0xb2;
	check(PsStoragePosix.open(store, segment_size) == 0,
		  "open low-level storage for hand-crafted hole store");
	seg_off = PsStoragePosix.seg_size(0, 0);
	offsets[0] = seg_off >= 0 ? (uint64_t) seg_off : 0;
	check((offsets[1] = offsets[0] + sizeof(before_hdr) + sizeof(page), 1) &&
		  (offsets[2] = offsets[1] + sizeof(hole_hdr) + sizeof(page), 1) &&
		  PsStoragePosix.seg_write(0, 0, offsets[0], &before_hdr,
								 sizeof(before_hdr)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[0] + sizeof(before_hdr),
								 pages[0], sizeof(page)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[1], &hole_hdr,
								 sizeof(hole_hdr)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[1] + sizeof(hole_hdr),
								 pages[1], sizeof(page)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[2], &after_hdr,
								 sizeof(after_hdr)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[2] + sizeof(after_hdr),
								 pages[2], sizeof(page)) == 0 &&
		  PsStoragePosix.sync() == 0,
		  "install a well-formed hole by hand between two live records");
	PsStoragePosix.close();
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) &&
		  rc == 0, "reopen with a hand-crafted hole between two live records");
	check(count_occurrences(captured, "retiring tail at offset") == 0 &&
		  count_occurrences(captured, "refusing unmatched ordered record") == 0 &&
		  count_occurrences(captured, "adopting orphaned ordered") == 0 &&
		  count_occurrences(captured, "incompatible record magic") == 0,
		  "the hole logs no diagnostic and does not interrupt the scan");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &before_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0xa1,
		  "the record before the hole is served");
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &after_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0xb2,
		  "the record after the hole is served");
	check(read_resolve(0, &hole_key, 0, UINT64_MAX, 0, page, NULL) == 0,
		  "the hole itself contributes no version");
	close_runtime();
	remove_tree(store);
}

static void
test_hole_record_bad_len_rejected(void)
{
	char		store[] = "/tmp/psforkmetaholebadlenXXXXXX";
	PsKey		before_key = {9, 9, 111, 0, PS_KLASS_RELATION};
	PsKey		hole_key = {9, 9, 112, 0, PS_KLASS_RELATION};
	TestSegRecHdr before_hdr;
	TestSegRecHdr hole_hdr;
	unsigned char page[8192];
	unsigned char pages[2][8192];
	int64_t		seg_off;
	uint64_t	offsets[2];

	check(mkdtemp(store) != NULL, "create bad-len hole store");
	check(ps_core_open(store) == 0, "bootstrap bad-len hole store");
	close_runtime();
	memset(&before_hdr, 0, sizeof(before_hdr));
	before_hdr.magic = TEST_SEG_MAGIC;
	before_hdr.timeline = 0;
	before_hdr.key = before_key;
	before_hdr.len = sizeof(page);
	before_hdr.lsn = 700;
	memset(&hole_hdr, 0, sizeof(hole_hdr));
	hole_hdr.magic = TEST_SEG_HOLE48_MAGIC;
	hole_hdr.timeline = 0;
	hole_hdr.key = hole_key;
	hole_hdr.len = sizeof(page) - 8;	/* corrupt: must equal page_size */
	hole_hdr.lsn = 999;
	memset(pages, 0, sizeof(pages));
	pages[0][128] = 0xc3;
	check(PsStoragePosix.open(store, segment_size) == 0,
		  "open low-level storage for bad-len hole store");
	seg_off = PsStoragePosix.seg_size(0, 0);
	offsets[0] = seg_off >= 0 ? (uint64_t) seg_off : 0;
	check((offsets[1] = offsets[0] + sizeof(before_hdr) + sizeof(page), 1) &&
		  PsStoragePosix.seg_write(0, 0, offsets[0], &before_hdr,
								 sizeof(before_hdr)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[0] + sizeof(before_hdr),
								 pages[0], sizeof(page)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[1], &hole_hdr,
								 sizeof(hole_hdr)) == 0 &&
		  PsStoragePosix.seg_write(0, 0, offsets[1] + sizeof(hole_hdr),
								 pages[1], sizeof(page)) == 0 &&
		  PsStoragePosix.sync() == 0,
		  "install a hole with a wrong len by hand");
	PsStoragePosix.close();
	check(expect_open_failure(store),
		  "a hole whose len does not match page_size fails startup closed");
	remove_tree(store);
}

/*
 * ---- T2: page_cleanup_tombstone_segment() keeps every survivor's offset ----
 *
 * Folds $SP/f3/q1_test.c's three variants (crash right after the tombstone
 * pass, crash after one more flush, and a clean close after one more flush)
 * into one test.  On the pre-fix rewrite path this reproduces Q1: a survivor
 * flushed before the deletion and appended again afterward becomes
 * unreadable (variants 1 and 2), because the rewrite relocates it and
 * rebases the flush watermark past both its old and new offsets.  Under
 * tombstoning no offset ever moves, so every variant serves every survivor.
 */
static void
test_timeline_delete_keeps_offsets_variant(int variant)
{
	char		store[] = "/tmp/psq1XXXXXX";
	char		seg0[1200];
	char		size_marker[1200];
	char		snapshots[1024];
	char		manifest[1200];
	char		captured[16384];
	PsKey		target_key = {12, 12, 1, 0, PS_KLASS_RELATION};
	PsKey		sibling_key = {12, 12, 2, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t	seq = 0;
	off_t		size_before, size_after_tombstone;
	PsFlushWatermark watermark_before = {0};
	PsFlushWatermark watermark_after = {0};
	int			have_watermark_before;
	pid_t		pid;
	int			status;
	int			rc = -1;
	int			n;

	check(mkdtemp(store) != NULL, "create Q1 offsets-kept store");
	n = snprintf(seg0, sizeof(seg0), "%s/seg_00000000", store);
	check(n > 0 && (size_t) n < sizeof(seg0), "build Q1 seg0 path");
	n = snprintf(size_marker, sizeof(size_marker),
				 "%s/.test_size_after_tombstone", store);
	check(n > 0 && (size_t) n < sizeof(size_marker),
		  "build Q1 size-marker path");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build Q1 snapshot path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build Q1 manifest path");
	flush_pages = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "open Q1 offsets-kept store");
	check(create_branch_request(1, 0, 1) && create_branch_request(2, 0, 1),
		  "create Q1 target and sibling timelines");
	check(meta_request_timeline(1, PS_OP_CREATE, &target_key, 100, 0, 0, 0, NULL) &&
		  meta_request_timeline(2, PS_OP_CREATE, &sibling_key, 100, 0, 0, 0, NULL),
		  "create Q1 target and sibling fork metadata");
	for (uint32_t b = 0; b < 6; b++)
		check(append_relation_timeline(1, &target_key, b, 100 + b, page, &seq) == 0,
			  "write target page into the shared segment");
	/* survivors after the target bytes in the same segment: a plain
	 * versioned page and a WAL-less (ordered) page. */
	check(append_relation_timeline(2, &sibling_key, 0, 200, page, &seq) == 0,
		  "write plain survivor page after the target bytes");
	check(append_relation_timeline(2, &sibling_key, 1, 0, page, &seq) == 0,
		  "write ordered survivor page after the target bytes");
	have_watermark_before =
		ps_manifest_get_flush_watermark(0, &watermark_before);
	size_before = file_size(seg0);
	check(size_before > 0, "shared segment has bytes before deletion");
	close_runtime();

	pid = fork();
	if (pid == 0)
	{
		PsTimelineState state = (PsTimelineState) -1;
		int			deleted = 0;

		flush_pages = 1;
		if (ps_core_open(store) != 0)
			_exit(10);
		if (!begin_delete_timeline(1))
			_exit(11);
		for (int i = 0; i < 400; i++)
		{
			(void) ps_core_maintenance();
			if (ps_timeline_state(1, &state, NULL) && state == PS_TIMELINE_DELETED)
			{
				deleted = 1;
				break;
			}
			usleep(50000);
		}
		if (!deleted)
			_exit(12);
		/* Measure the segment right after the tombstone pass, before any
		 * further legitimate append can legitimately grow it, and hand the
		 * value to the parent (a fork does not share stdout state). */
		{
			off_t		sz = file_size(seg0);
			FILE	   *marker = sz >= 0 ? fopen(size_marker, "w") : NULL;

			if (marker == NULL || fprintf(marker, "%lld", (long long) sz) < 0 ||
				fclose(marker) != 0)
				_exit(14);
		}
		if (variant >= 1)
		{
			/* one more append: with the pre-fix rewrite this is the flush that
			 * moves the watermark past the survivors' new offsets but below
			 * their stale ones -- the Q1 loss window. */
			uint64_t	extra_seq = 0;

			if (append_relation_timeline(2, &sibling_key, 2, 300, page,
										 &extra_seq) != 0)
				_exit(13);
		}
		if (variant == 2)
		{
			close_runtime();
			_exit(0);
		}
		_exit(0);
	}
	check(pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
		  WEXITSTATUS(status) == 0,
		  "Q1 child scenario ran to completion");

	flush_pages = 1;
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) &&
		  rc == 0, "Q1 crash-restart opens");
	check(count_occurrences(captured, "retiring tail") == 0 &&
		  count_occurrences(captured, "refusing unmatched") == 0 &&
		  count_occurrences(captured, "adopting orphaned") == 0,
		  "Q1 reopen needs no retire/refuse/adopt");
	{
		FILE	   *marker = fopen(size_marker, "r");
		long long	value = -1;

		check(marker != NULL && fscanf(marker, "%lld", &value) == 1,
			  "Q1 child reported the post-tombstone segment size");
		if (marker != NULL)
			fclose(marker);
		size_after_tombstone = (off_t) value;
	}
	check(size_before == size_after_tombstone,
		  "the shared segment's byte size right after the tombstone pass is "
		  "unchanged from before the deletion (holes keep bytes, nothing "
		  "was rewritten)");
	check(ps_manifest_get_flush_watermark(0, &watermark_after) ==
			  have_watermark_before &&
		  (!have_watermark_before ||
		   watermark_after.seg_id > watermark_before.seg_id ||
		   (watermark_after.seg_id == watermark_before.seg_id &&
			watermark_after.seg_off >= watermark_before.seg_off)),
		  "the shard flush watermark never retreats");
	{
		uint64_t	ver = 0,
					rseq = 0;

		memset(page, 0, sizeof(page));
		rc = read_resolve_version(2, &sibling_key, 0, UINT64_MAX, 0, page,
								  &ver, &rseq);
		check(rc == 1 && ver == 200,
			  "plain survivor block 0 serves LSN 200 after crash-restart");
		rc = read_resolve_version(2, &sibling_key, 1, UINT64_MAX, 0, page,
								  &ver, &rseq);
		check(rc == 1,
			  "ordered (WAL-less) survivor block 1 readable after crash-restart");
	}
	{
		PsChannel	reply;

		check(meta_request_timeline(2, PS_OP_NBLOCKS, &sibling_key, 0, 0, 0, 0,
									 &reply) &&
			  reply.result == (uint64_t) (variant >= 1 ? 3 : 2),
			  "NBLOCKS intact");
	}
	check(append_growth_batch(3100, 900) &&
		  setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0 &&
		  run_maintenance_until(manifest, 1),
		  "Q1 cutover after crash-restart");
	check(snapshot_ordered_marker_count(snapshots, &sibling_key, 0, 0) == 1,
		  "ordered survivor's marker count is 1 after a forced cutover");
	close_runtime();
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0 &&
		  read_resolve_version(2, &sibling_key, 1, UINT64_MAX, 0, page, NULL,
							   NULL) == 1,
		  "clean reopen still serves the ordered survivor");
	close_runtime();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

static void
test_timeline_delete_keeps_offsets(void)
{
	test_timeline_delete_keeps_offsets_variant(0);
	test_timeline_delete_keeps_offsets_variant(1);
	test_timeline_delete_keeps_offsets_variant(2);
}

/*
 * After the survivors are flushed and the target is fully tombstoned, the
 * segment holding only holes plus covered survivors is reclaimed the same
 * way any other fully-covered segment is: by segment GC once the watermark
 * moves past it.
 */
static void
test_timeline_delete_reclaims_by_segment_gc(void)
{
	char		store[] = "/tmp/psq1gcXXXXXX";
	char		seg0[1200];
	PsKey		target_key = {13, 13, 1, 0, PS_KLASS_RELATION};
	PsKey		sibling_key = {13, 13, 2, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t	seq = 0;
	PsTimelineState state = (PsTimelineState) -1;
	int			n;

	check(mkdtemp(store) != NULL, "create segment-GC reclaim store");
	n = snprintf(seg0, sizeof(seg0), "%s/seg_00000000", store);
	check(n > 0 && (size_t) n < sizeof(seg0), "build segment-GC seg0 path");
	segment_size = 65536;
	flush_pages = 1;
	segment_gc_enabled = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "open segment-GC reclaim store");
	check(create_branch_request(1, 0, 1) && create_branch_request(2, 0, 1),
		  "create target and sibling timelines for segment-GC test");
	check(meta_request_timeline(1, PS_OP_CREATE, &target_key, 100, 0, 0, 0,
								 NULL) &&
		  meta_request_timeline(2, PS_OP_CREATE, &sibling_key, 100, 0, 0, 0,
								 NULL),
		  "create target and sibling fork metadata for segment-GC test");
	for (uint32_t b = 0; b < 3; b++)
		check(append_relation_timeline(1, &target_key, b, 100 + b, page,
									   &seq) == 0,
			  "write target page into segment 0");
	check(append_relation_timeline(2, &sibling_key, 0, 200, page, &seq) == 0,
		  "write surviving sibling page into segment 0");
	check(access(seg0, F_OK) == 0, "segment 0 exists before deletion");
	check(begin_delete_timeline(1), "begin target deletion for segment-GC test");
	for (int i = 0; i < 400; i++)
	{
		(void) ps_core_maintenance();
		if (ps_timeline_state(1, &state, NULL) && state == PS_TIMELINE_DELETED)
			break;
		usleep(50000);
	}
	check(state == PS_TIMELINE_DELETED, "target deletion completes");
	/* Push the append cursor, and the flush watermark behind it, into a
	 * later segment: only once segment 0 is fully below the watermark can
	 * segment GC reclaim it (it never reclaims the boundary segment). */
	for (uint32_t b = 2; b < 40; b++)
		check(append_relation_timeline(2, &sibling_key, b, 400 + b, page,
									   &seq) == 0,
			  "grow the surviving sibling past segment 0");
	check(run_maintenance_until(seg0, 0),
		  "segment 0 (holes and covered survivors alike) is reclaimed by "
		  "segment GC");
	check(read_resolve_version(2, &sibling_key, 0, UINT64_MAX, 0, page, NULL,
							   NULL) == 1,
		  "the covered survivor is still served from its image layer "
		  "after its segment is gone");
	close_runtime();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
	segment_size = 8 * 1024 * 1024;
}

/*
 * ---- T3: a compaction publish never removes a version above the watermark ----
 *
 * page_remove_compacted_versions() runs only for versions a *retention-pin-
 * driven page prune* actually dropped: compaction merging layers alone does
 * not call it (gdb-verified: a breakpoint on page_remove_compacted_versions
 * with nrec > 0 is never hit by compact_layers/flush alone) -- pruning needs
 * a durable retention floor past the superseded revisions and the frontier
 * maintenance publishes once it reaches them, exactly the shape
 * test_reclaimed_ordered_markers_pruned() and the T5 crash-matrix's
 * "survivor A" setup below both use.  This drives that real prune (six
 * same-LSN revisions of one block, a retention pin above them, wait for the
 * durable frontier) and then leaves one more revision unflushed in the
 * memtable, asserting it still resolves -- a cassert build additionally
 * trips the invariant check in page_remove_compacted_versions() itself
 * (~4820) if a pruned version's segment record were ever at or after the
 * watermark, which the prune step above already exercises.
 */
static void
test_compaction_never_prunes_above_watermark(void)
{
	char		store[] = "/tmp/psforkmetawatermarkXXXXXX";
	char		frontier[1200];
	PsKey		key = {22, 22, 1, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	unsigned char page[8192];
	uint64_t	seq = 0;
	int			n;

	check(mkdtemp(store) != NULL, "create I3 watermark-prune store");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build I3 watermark-prune frontier path");
	compact_layers = 2;
	flush_pages = 1;
	segment_gc_enabled = 0;
	check(ps_core_open(store) == 0 &&
		  meta_request(PS_OP_CREATE, &key, 100, 0, 0, 0, NULL),
		  "open I3 watermark-prune store and create fork");
	/* Six same-LSN, flushed revisions: a retention pin past all of them
	 * lets the page-prune frontier durably reclaim the superseded ones,
	 * actually calling page_remove_compacted_versions() with nrec > 0. */
	for (int i = 0; i < 6; i++)
		check(append_relation_tag(&key, 0, 50, page, (unsigned char) (0x10 + i),
								  &seq) == 0,
			  "write prunable revisions of the same block");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 220;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.lsn = 200;
	pin.admission_seq = seq;
	check(seq != 0 && ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "durable page-prune frontier reclaims the superseded revisions");
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x15,
		  "prune leaves the newest flushed revision readable");
	/* Now leave a seventh revision unflushed in the memtable: raise the
	 * threshold so append_page's own "memtable full" flush never fires. */
	flush_pages = 1000000;
	check(append_relation_tag(&key, 0, 50, page, 0x20, &seq) == 0,
		  "write a revision that stays memtable-resident, unflushed");
	/* Give maintenance a few turns: if anything ever tried to treat the
	 * memtable-resident version as prunable, it would either assert
	 * (cassert build) or silently vanish here. */
	for (int i = 0; i < 5; i++)
		(void) ps_core_maintenance();
	memset(page, 0, sizeof(page));
	check(read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x20,
		  "invariant I3: the unflushed memtable-resident revision survives "
		  "in memory, untouched by page-prune pruning");
	close_runtime();
	remove_tree(store);
	compact_layers = 0;
	flush_pages = 1;
	segment_gc_enabled = 0;
}

/*
 * ---- T5: crash matrix around the tombstone probes ----
 *
 * A segment laid out as [survivor A, pruned from memory by a prior
 * compaction] [target records] [survivor B, live] [one more target record],
 * crashed at PS_FAULT_POINT_TIMELINE_DELETE_MID_SEGMENT_TOMBSTONE (after the
 * first hole of the segment, before the rest) and at
 * PS_FAULT_POINT_TIMELINE_DELETE_AFTER_SEGMENT_TOMBSTONE (after the whole
 * segment's holes are synced, before the DELETED transition).  Both must
 * reopen with zero adoption/retire/refuse lines, deletion must resume and
 * reach DELETED, and both survivors must still resolve -- survivor A because
 * its record was always below the watermark and is never rescanned
 * (invariant I3), survivor B because its bytes never moved.
 */
static int
crash_matrix_configure_fault(const char *store, const char *fault_dir,
							 const char *name)
{
	return setenv("PAGESTORE_TEST_FAULT_NAME", name, 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_ACTION", "crash", 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_HIT", "1", 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_DIR", fault_dir, 1) == 0 &&
		ps_fault_init(store) == 0;
}

static int
crash_matrix_arm_fault(const char *fault_dir)
{
	char		path[1600];
	int			fd;

	if (snprintf(path, sizeof(path), "%s/arm", fault_dir) < 0)
		return 0;
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
	if (fd < 0)
		return 0;
	return close(fd) == 0;
}

static void
test_timeline_delete_crash_matrix_case(const char *fault_name)
{
	char		store[] = "/tmp/psforkmetacrashXXXXXX";
	char		fault_dir[] = "/tmp/psforkmetacrashctlXXXXXX";
	char		snapshots[1024];
	char		frontier[1200];
	char		captured[16384];
	PsKey		survivor_a_key = {21, 21, 1, 0, PS_KLASS_RELATION};
	PsKey		survivor_b_key = {21, 21, 2, 0, PS_KLASS_RELATION};
	PsKey		target_key = {21, 21, 3, 0, PS_KLASS_RELATION};
	PsKey		followup_key = {21, 21, 9, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t	seq = 0;
	PsRetentionPin pin;
	pid_t		pid;
	int			status;
	int			rc = -1;
	int			n;

	check(mkdtemp(store) != NULL, "create crash-matrix store");
	check(mkdtemp(fault_dir) != NULL, "create crash-matrix fault control dir");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots),
		  "build crash-matrix snapshot path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier),
		  "build crash-matrix frontier path");
	flush_pages = 1;
	compact_layers = 2;
	segment_gc_enabled = 0;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0 &&
		  ps_core_open(store) == 0, "open crash-matrix store");
	check(create_branch_request(1, 0, 1) && create_branch_request(2, 0, 1),
		  "create crash-matrix target and sibling timelines");
	/* survivor A: several same-LSN ordered revisions of one block, so the
	 * earlier ones become prunable once compaction publishes past them --
	 * the exact shape test_reclaimed_ordered_markers_pruned() uses. */
	for (int i = 0; i < 6; i++)
		check(append_relation_timeline(2, &survivor_a_key, 0, 50, page,
									   &seq) == 0,
			  "write survivor A's prunable ordered revisions");
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 2;
	pin.owner_kind = 1;
	pin.owner_id = 210;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.lsn = 200;
	pin.admission_seq = seq;
	check(seq != 0 && ps_retention_set(&pin) == PS_RETENTION_OK &&
		  run_maintenance_until(frontier, 1),
		  "compact and prune survivor A's earlier revisions from memory");
	/* target records, then survivor B (live, ordered), then one more target
	 * record -- all appended after survivor A in the same segment. */
	for (uint32_t b = 0; b < 4; b++)
		check(append_relation_timeline(1, &target_key, b, 100 + b, page,
									   &seq) == 0,
			  "write target records after the pruned survivor");
	check(append_relation_timeline(2, &survivor_b_key, 0, 0, page, &seq) == 0,
		  "write live survivor B between target records");
	check(append_relation_timeline(1, &target_key, 4, 104, page, &seq) == 0,
		  "write the trailing target record after survivor B");
	close_runtime();

	pid = fork();
	if (pid == 0)
	{
		int			i;

		flush_pages = 1;
		compact_layers = 2;
		segment_gc_enabled = 0;
		if (!crash_matrix_configure_fault(store, fault_dir, fault_name))
			_exit(20);
		if (ps_core_open(store) != 0)
			_exit(21);
		if (!begin_delete_timeline(1))
			_exit(22);
		if (!crash_matrix_arm_fault(fault_dir))
			_exit(23);
		for (i = 0; i < 400; i++)
		{
			(void) ps_core_maintenance();
			usleep(20000);
		}
		/* The fault should have fired (_exit(PS_FAULT_CRASH_EXIT)) well
		 * before this loop exhausts; reaching here means it never did. */
		_exit(24);
	}
	check(pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
		  WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT,
		  "crash-matrix child crashed exactly at the armed tombstone probe");

	flush_pages = 1;
	compact_layers = 2;
	segment_gc_enabled = 0;
	check(open_capture_stderr(store, captured, sizeof(captured), &rc) &&
		  rc == 0, "crash-matrix reopen after the armed probe");
	check(count_occurrences(captured, "retiring tail") == 0 &&
		  count_occurrences(captured, "refusing unmatched") == 0 &&
		  count_occurrences(captured, "adopting orphaned") == 0,
		  "crash-matrix reopen needs no retire/refuse/adopt");
	{
		PsTimelineState state = (PsTimelineState) -1;
		int			deleted = 0;

		for (int i = 0; i < 400 && !deleted; i++)
		{
			(void) ps_core_maintenance();
			if (ps_timeline_state(1, &state, NULL) && state == PS_TIMELINE_DELETED)
				deleted = 1;
			else
				usleep(20000);
		}
		check(deleted, "deletion resumes and reaches DELETED after the crash");
	}
	{
		uint64_t	ver = 0;

		memset(page, 0, sizeof(page));
		check(read_resolve_version(2, &survivor_a_key, 0, UINT64_MAX, 0, page,
								   &ver, NULL) == 1 && ver == 50,
			  "the pruned survivor A resolves after the crash (never rescanned)");
		memset(page, 0, sizeof(page));
		check(read_resolve_version(2, &survivor_b_key, 0, UINT64_MAX, 0, page,
								   &ver, NULL) == 1,
			  "the live survivor B resolves after the crash");
	}
	/*
	 * Q1 follow-up: the data-loss scenario the plan set out to fix was
	 * "drop a branch, write a page, restart" -- a write and a clean restart
	 * *after* deletion completes, which used to rebase the flush watermark
	 * past the rewritten segment.  Force exactly that here: one more write
	 * past DELETED (flush_pages == 1 flushes it immediately, advancing the
	 * watermark), then a second crash-free restart, and assert invariant I3
	 * held throughout -- every byte already on disk when deletion completed
	 * (the tombstoned prefix) is unchanged, and every remaining record
	 * belonging to the deleted target within it is a hole.  The segment's
	 * *total* size is expected to grow: the new write appends past that
	 * prefix, it does not touch it.
	 */
	{
		int64_t		after_crash_size = ps_storage->seg_size(0, 0);
		int64_t		holes = 0;
		int64_t		target_live = 0;
		uint64_t	flush_seq = 0;
		uint64_t	ver = 0;

		check(after_crash_size > 0 &&
				scan_segment_holes(0, 0, after_crash_size, 1, &holes,
									&target_live) == 0 && target_live == 0,
			  "Q1 follow-up: every remaining target-timeline record is a "
			  "hole once deletion resumes and completes after the crash");
		check(append_relation_timeline(2, &followup_key, 0, 10000, page,
									   &flush_seq) == 0,
			  "Q1 follow-up: write and flush past the deleted target");
		close_runtime();
		check(ps_core_open(store) == 0,
			  "Q1 follow-up: second, crash-free restart after the flush");
		check(ps_storage->seg_size(0, 0) >= after_crash_size,
			  "Q1 follow-up: the later write only appends -- the segment "
			  "never shrinks below the size it had when deletion completed");
		check(scan_segment_holes(0, 0, after_crash_size, 1, &holes,
								  &target_live) == 0 && target_live == 0,
			  "Q1 follow-up: invariant I3 -- the tombstoned prefix still "
			  "parses record by record with no live target-timeline record "
			  "in it, after the later write and second restart");
		memset(page, 0, sizeof(page));
		check(read_resolve_version(2, &survivor_a_key, 0, UINT64_MAX, 0, page,
								   &ver, NULL) == 1 && ver == 50,
			  "Q1 follow-up: the pruned survivor A still resolves");
		memset(page, 0, sizeof(page));
		check(read_resolve_version(2, &survivor_b_key, 0, UINT64_MAX, 0, page,
								   &ver, NULL) == 1,
			  "Q1 follow-up: the live survivor B still resolves");
	}
	close_runtime();
	remove_tree(store);
	rmdir(fault_dir);
	unsetenv("PAGESTORE_TEST_FAULT_NAME");
	unsetenv("PAGESTORE_TEST_FAULT_ACTION");
	unsetenv("PAGESTORE_TEST_FAULT_HIT");
	unsetenv("PAGESTORE_TEST_FAULT_DIR");
	ps_fault_reset();
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
	compact_layers = 0;
}

static void
test_timeline_delete_crash_matrix(void)
{
	test_timeline_delete_crash_matrix_case("timeline_delete.mid_segment_tombstone");
	test_timeline_delete_crash_matrix_case("timeline_delete.after_segment_tombstone");
}

/*
 * L6, the real-crash shape: a genuine torn tail from PAGESTORE_TEST_CRASH_
 * AFTER_SEG_WRITES (storage_posix.c), not a synthetic one appended by hand.
 * Lifetime 1 writes two target records and closes cleanly.  Lifetime 2 is
 * armed to crash after exactly its first segment write -- a survivor's own
 * record header -- so the header lands complete on disk but the body never
 * does: a real torn tail past the two flushed target records, produced the
 * same way recover()'s "torn record is always the last complete record of
 * its segment" proof (see test_torn_commit_append_never_adopted()'s header
 * comment) already assumes.  Lifetime 3 (the parent) reopens, reads the
 * recovered cursor off the "recovered shard ... (off N)" log line, deletes
 * the target timeline, and asserts it reaches DELETED with holes only
 * below that cursor and the torn bytes at/after it byte-for-byte untouched
 * -- invariant I4's reachable region, exercised against a real crash
 * instead of a hand-crafted file.
 */
static void
test_timeline_delete_torn_tail_crash_matrix(void)
{
	char		store[] = "/tmp/pstorntailcrashXXXXXX";
	char		seg0[1200];
	char		captured[16384];
	PsKey		target_key = {23, 23, 1, 0, PS_KLASS_RELATION};
	PsKey		survivor_key = {23, 23, 2, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	unsigned char tail_before[4096];
	unsigned char tail_after[4096];
	long long	recovered_off = -1;
	off_t		seg0_size;
	pid_t		pid;
	int			status;
	int			rc = -1;
	int			fd;
	int			n;

	check(mkdtemp(store) != NULL, "create torn-tail crash-matrix store");
	n = snprintf(seg0, sizeof(seg0), "%s/seg_00000000", store);
	check(n > 0 && (size_t) n < sizeof(seg0), "build torn-tail crash seg0 path");
	flush_pages = 1;

	pid = fork();
	if (pid == 0)
	{
		uint64_t	seq = 0;

		setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
		if (ps_core_open(store) != 0)
			_exit(1);
		if (!create_branch_request(1, 0, 1) || !create_branch_request(2, 0, 1))
			_exit(2);
		if (append_relation_timeline(1, &target_key, 0, 100, page, &seq) != 0)
			_exit(3);
		if (append_relation_timeline(1, &target_key, 1, 101, page, &seq) != 0)
			_exit(4);
		close_runtime();
		/* Lifetime 2: a fresh crash budget of 1 fires after this lifetime's
		 * very first segment write, which is the survivor's own header --
		 * its body never gets its turn. */
		setenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES", "1", 1);
		if (ps_core_open(store) != 0)
			_exit(5);
		(void) append_relation_timeline(2, &survivor_key, 0, 200, page, &seq);
		_exit(10);		/* not reached: the crash hook exits 86 first */
	}
	check(pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
		  WEXITSTATUS(status) == 86,
		  "child crashed after the survivor's torn header reached the segment");
	unsetenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES");
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);

	check(open_capture_stderr(store, captured, sizeof(captured), &rc) &&
		  rc == 0, "lifetime 3 reopens after the real crash");
	check(count_occurrences(captured, "retiring tail") == 0 &&
		  count_occurrences(captured, "refusing unmatched") == 0 &&
		  count_occurrences(captured, "adopting orphaned") == 0,
		  "the torn survivor header is plain end of log: no "
		  "retire/refuse/adopt handling applies to it");
	{
		const char *p = strstr(captured,
								"recovered shard 0 through segment 0 (off ");

		check(p != NULL && sscanf(p,
								   "recovered shard 0 through segment 0 (off %lld)",
								   &recovered_off) == 1 && recovered_off > 0,
			  "capture the recovered append cursor from the reopen log");
	}
	seg0_size = file_size(seg0);
	check(seg0_size > recovered_off,
		  "the torn survivor header is physically present past the cursor");
	check((size_t) (seg0_size - recovered_off) <= sizeof(tail_before),
		  "torn tail fits the snapshot buffer");
	fd = open(seg0, O_RDONLY);
	check(fd >= 0 &&
			pread(fd, tail_before, (size_t) (seg0_size - recovered_off),
				  (off_t) recovered_off) ==
				(ssize_t) (seg0_size - recovered_off) &&
			close(fd) == 0,
		  "snapshot the torn survivor header before deletion");

	check(begin_delete_timeline(1), "begin torn-tail crash-matrix target deletion");
	{
		PsTimelineState state = (PsTimelineState) -1;
		int			deleted = 0;

		for (int i = 0; i < 400 && !deleted; i++)
		{
			(void) ps_core_maintenance();
			if (ps_timeline_state(1, &state, NULL) && state == PS_TIMELINE_DELETED)
				deleted = 1;
			else
				usleep(20000);
		}
		check(deleted,
			  "a real crash-produced torn tail does not stall the deletion");
	}
	check(file_size(seg0) == seg0_size,
		  "segment 0 keeps its size after deletion completes");
	{
		int64_t		holes = 0;
		int64_t		target_live = 0;

		check(scan_segment_holes(0, 0, recovered_off, 1, &holes,
								  &target_live) == 0 &&
				holes == 2 && target_live == 0,
			  "exactly the two target records below the recovered cursor "
			  "become holes");
	}
	fd = open(seg0, O_RDONLY);
	check(fd >= 0 &&
			pread(fd, tail_after, (size_t) (seg0_size - recovered_off),
				  (off_t) recovered_off) ==
				(ssize_t) (seg0_size - recovered_off) &&
			close(fd) == 0 &&
			memcmp(tail_before, tail_after,
				   (size_t) (seg0_size - recovered_off)) == 0,
		  "the torn survivor header past the cursor is never written");
	memset(page, 0, sizeof(page));
	check(read_resolve_version(1, &target_key, 0, UINT64_MAX, 0, page, NULL,
							   NULL) == 0,
		  "target block 0 gone after torn-tail crash-matrix cleanup");
	close_runtime();
	remove_tree(store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
}

int
main(void)
{
	char store[] = "/tmp/psforkmetacutoverXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	char source[1200];
	PsKey page_key = {1, 1, 1, 0, PS_KLASS_RELATION};
	PsKey sequence_key = {1, 1, 2, 0, PS_KLASS_RELATION};
	PsKey after_key = {1, 1, 3, 0, PS_KLASS_RELATION};
	PsKey boundary_key = {1, 1, 4, 0, PS_KLASS_RELATION};
	PsKey lazy_fsm_key = {1, 1, 1, 2, PS_KLASS_RELATION};
	PsKey delayed_create_key = {1, 1, 5, 0, PS_KLASS_RELATION};
	PsKey ancestry_key = {1, 1, 6, 0, PS_KLASS_RELATION};
	PsKey round3_reject_key = {1, 1, 7, 0, PS_KLASS_RELATION};
	PsKey invalid_marker_key = {3, 3, 333, 0, PS_KLASS_RELATION};
	PsKey invalid_unbound_key = {3, 3, 334, 0, PS_KLASS_RELATION};
	PsKey torn_tail_key = {3, 3, 335, 0, PS_KLASS_RELATION};
	PsRetentionPin pin;
	unsigned char page[8192];
	PsForkmetaSnapshot selected;
	PsForkmetaSnapshotPrepared stale;
	PsForkmetaSnapshotInput stale_part;
	TestSnapshotHeader header;
	TestForkMetaRecV2 marker;
	PsChannel reply;
	uint64_t first_seq = 0, second_seq = 0, ordered_seq = 0;
	uint64_t invalid_marker_seq = 0;
	uint64_t generation;
	uint64_t poison_generation;
	off_t source_after_append;
	off_t source_before_fault;
	off_t poison_source_size;
	int n;

	/* Needs no store; runs first. */
	test_fork_event_index_selftest();

	check(mkdtemp(store) != NULL, "create runtime cutover store");
	page_size = sizeof(page);
	segment_size = 1024 * 1024;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 0;
	cache_pages = 0;
	ps_nshards = 1;
	use_layers = 1;
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm conservative forkmeta trigger");
	check(ps_core_open(store) == 0, "open runtime cutover store");
	/*
	 * Codex round-3 review finding 4098831579: created early, well before
	 * any cutover is ever selected, so its CREATE becomes real retained
	 * pre-cutover history (nothing else ever touches this key, so
	 * forkmeta compaction has no later event to consolidate it into).
	 * Reused much further below, once a cutover and a descendant fence are
	 * both in place, to prove an explicit mutation below the cutoff that
	 * collides with this retained event is rejected, not rescued by
	 * promotion.
	 */
	check(meta_request(PS_OP_CREATE, &round3_reject_key, 50, 0, 0, 0, NULL),
		  "round 3: create key with real pre-cutover history for the later below-cutoff-collision rejection check");
	check(meta_request(PS_OP_CREATE, &page_key, 100, 0, 0, 0, NULL),
		  "create fork before page history");
	check(append_relation(&page_key, 0, 100, page, &first_seq) == 0,
		  "write first page version");
	check(append_relation(&page_key, 0, 200, page, &second_seq) == 0,
		  "write second page version");
	check(first_seq != 0 && second_seq > first_seq,
		  "page writes receive ordered admission sequences");
	n = snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	check(n > 0 && (size_t) n < sizeof(snapshots), "build snapshot directory path");
	n = snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	check(n > 0 && (size_t) n < sizeof(manifest), "build manifest path");
	n = snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	check(n > 0 && (size_t) n < sizeof(frontier), "build frontier path");
	n = snprintf(source, sizeof(source), "%s/forkmeta", store);
	check(n > 0 && (size_t) n < sizeof(source), "build source path");
	(void) ps_core_maintenance();
	check(access(manifest, F_OK) != 0,
		  "no safe cutoff does not publish a snapshot");
	close_runtime();
	check(ps_core_open(store) == 0 && access(manifest, F_OK) != 0,
		  "legacy no-snapshot store reopens compatibly");

	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 1;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	check(ps_retention_set(&pin) == PS_RETENTION_OK,
		  "install page-history pin for durable frontier");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) == 0,
		  "defer cutover while producing a real frontier");
	check(run_maintenance_until(frontier, 1),
		  "page compaction publishes a real durable frontier");

	/* Leave an ordered page only in the segment/memtable.  Its source marker is
	 * intentionally removed by cutover; freeze_seq authorizes recovery. */
	flush_pages = 1000;
	check(append_relation(&page_key, 1, 0, page, &ordered_seq) == 0,
		  "write pre-cutover ordered unflushed segment page");
	check(meta_request(PS_OP_CREATE, &sequence_key, 150, 0, 0, 0, &reply),
		  "same-LSN create");
	check(meta_request(PS_OP_ZEROEXTEND, &sequence_key, 150, 0, 2, 0, NULL),
		  "same-LSN extend");
	check(meta_request(PS_OP_TRUNCATE, &sequence_key, 150, 0, 1, 0, NULL),
		  "same-LSN truncate");
	check(meta_request(PS_OP_UNLINK, &sequence_key, 150, 0, 0, 0, NULL),
		  "same-LSN drop");
	check(meta_request(PS_OP_CREATE, &sequence_key, 150, 0, 0, 0, NULL),
		  "same-LSN recreate");
	/* This event is future relative to cutoff 200 and belongs only to tail. */
	check(meta_request(PS_OP_CREATE, &after_key, 300, 0, 0, 0, NULL),
		  "capture future snapshot-tail event");
	check(meta_request(PS_OP_CREATE, &boundary_key, 200, 0, 0, 0, NULL),
		  "capture same-cutoff-LSN state above the cutoff sequence");
	for (uint32_t i = 0; i < 40; i++)
	{
		PsKey key = {2, 1, i + 10, 0, PS_KLASS_RELATION};

		ps_admission_read_lock();
		ps_lock_shard_wr(ps_shard_of(&key));
		check(fork_grow(0, &key, 1, 201 + i) == 0,
			  "persist trigger growth event");
		ps_unlock_shard(ps_shard_of(&key));
		ps_admission_read_unlock();
	}
	{
		TestForkMetaRecV2 bad;

		memset(&bad, 0, sizeof(bad));
		bad.magic = TEST_FORK_META_V2_MAGIC;
		bad.rec_len = sizeof(bad);
		bad.timeline = TEST_MAX_TIMELINES;
		bad.key = invalid_marker_key;
		bad.lsn = 500;
		bad.admission_seq = invalid_marker_seq = ordered_seq + 1000;
		bad.order_id = 999;
		bad.nblocks = 1;
		bad.kind = TEST_FEV_SEG_GROW_BOUND;
		check(append_source_record(source, &bad),
			  "append invalid V2 bound marker source fixture");
		bad.timeline = 0;
		bad.key = invalid_unbound_key;
		bad.admission_seq = 0;
		bad.order_id = 999;
		bad.kind = TEST_FEV_SEG_GROW;
		check(append_source_record(source, &bad),
			  "append invalid unbound marker identity fixture");
	}
	stale_part.data = "stale";
	stale_part.len = 5;
	stale_part.produce = NULL;
	stale_part.produce_arg = NULL;
	check(ps_forkmeta_snapshot_next_generation(snapshots, 0, &generation) == 0 &&
		  ps_forkmeta_snapshot_prepare(&stale, snapshots, generation, 1, 1,
									   &stale_part, &stale_part) == 0,
		  "stage an unselected stale prepared intent");
	check(setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) == 0,
		  "arm runtime cutover");
	check(run_maintenance_until(manifest, 1),
		  "stale intent is aborted and runtime snapshot publishes");
	check(access(manifest, F_OK) == 0,
		  "selected forkmeta manifest is durable");
	check(ps_forkmeta_snapshot_open(&selected, snapshots) == 0 &&
		  selected.generation != 0 && selected.cutoff_lsn != 0 &&
		  selected.cutoff_admission_seq != 0,
		  "selected snapshot carries an exact cutoff");
	if (selected.directory_fd >= 0)
		ps_forkmeta_snapshot_close(&selected);
	check(read_selected_header(snapshots, &header) == 0 &&
		  header.freeze_admission_seq >= ordered_seq &&
		  header.checkpoint_records != 0 && header.tail_records != 0,
		  "versioned payload records cutoff, counts, and freeze highwater");
	{
		char stale_gc[1400];
		int fd;

		n = snprintf(stale_gc, sizeof(stale_gc),
					 "%s/forkmeta_manifest_v1.tmp.999.1", snapshots);
		fd = n > 0 && (size_t) n < sizeof(stale_gc) ?
			open(stale_gc, O_WRONLY | O_CREAT | O_TRUNC, 0600) : -1;
		check(fd >= 0 && write(fd, "x", 1) == 1 && close(fd) == 0 &&
			  setenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC", "1", 1) == 0,
			  "install stale generation and arm snapshot GC failure");
		(void) ps_core_maintenance();
		fd = open(stale_gc, O_WRONLY | O_CREAT | O_TRUNC, 0600);
		check(fd >= 0 && write(fd, "x", 1) == 1 && close(fd) == 0,
			  "restore stale generation after failed GC attempt");
		unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC");
		(void) ps_core_maintenance();
		check(access(stale_gc, F_OK) == 0,
			  "snapshot GC failure backs off the immediate maintenance tick");
		usleep(1100000);
		(void) ps_core_maintenance();
		check(access(stale_gc, F_OK) != 0 && errno == ENOENT,
			  "snapshot GC retries after its bounded backoff deadline");
	}
	check(snapshot_has_ordered_marker(snapshots, &page_key, ordered_seq),
		  "snapshot records the committed ordered admission marker");
	check(!snapshot_has_ordered_marker(snapshots, &invalid_marker_key,
									 invalid_marker_seq),
		  "snapshot skips invalid V2 bound marker source record");
	check(!snapshot_has_ordered_marker(snapshots, &invalid_unbound_key, 0),
		  "snapshot skips invalid unbound marker identity");
	check(source_is_marker_only(store, &marker),
		  "new source epoch initially contains only its exact marker");
	check(marker.order_id == header.generation &&
		  marker.lsn == header.cutoff_lsn &&
		  marker.admission_seq == header.cutoff_admission_seq,
		  "source marker matches selected payload identity");
	check(meta_request(PS_OP_TRUNCATE, &after_key, 400, 0, 3, 0, &reply),
		  "append one post-cutover logical mutation");
	source_after_append = file_size(source);
	check(source_after_append == (off_t) (2 * sizeof(TestForkMetaRecV2)),
		  "post-cutover append follows marker exactly once");
	close_runtime();
	check(ps_core_open(store) == 0, "reopen selected forkmeta epoch");
	/* A successful pending-GC resolution reports one maintenance unit and
	 * intentionally does not fall through to a new snapshot in the same tick. */
	(void) ps_core_maintenance();
	check(read_resolve(0, &page_key, 0, UINT64_MAX, 0, page, NULL) == 1,
		  "page state remains readable after snapshot reopen");
	check(read_resolve(0, &page_key, 1, UINT64_MAX, 0, page, NULL) == 1,
		  "freeze highwater recovers ordered unflushed page without old marker");
	check(meta_request(PS_OP_EXISTS, &sequence_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 1 &&
		  meta_request(PS_OP_NBLOCKS, &sequence_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 0,
		  "same-LSN drop/recreate sequence is equivalent after reopen");
	check(meta_request(PS_OP_NBLOCKS, &after_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 3 && file_size(source) == source_after_append,
		  "tail plus preserved suffix replay once without source rewrite");
	check(!meta_request(PS_OP_EXISTS, &page_key, 100, 0, 0, 0, &reply),
		  "capped metadata below reclaimed frontier fails closed");
	{
		PsRetentionPin reopened_pin;
		int found_pin = ps_retention_lookup(0, pin.owner_kind, pin.owner_id,
										&reopened_pin);

		check(found_pin == 1 && reopened_pin.lsn == 200 &&
			  reopened_pin.admission_seq == second_seq,
			  "retained exact page fence survives restart");
	}
	{
		int exact_ok = meta_request(PS_OP_EXISTS, &sequence_key, 200, second_seq,
								0, 0, &reply);

		if (!exact_ok || reply.result != 1)
			fprintf(stderr, "exact fence diagnostic: status=%u result=%u lsn=200 seq=%llu\n",
					reply.status, reply.result, (unsigned long long) second_seq);
		check(exact_ok && reply.result == 1,
			  "retained exact page fence admits capped metadata read");
	}
	check(meta_request(PS_OP_CREATE, &(PsKey) {9, 9, 9, 0,
										 PS_KLASS_RELATION}, 500, 0, 0, 0, &reply) &&
		  reply.req_seq > header.freeze_admission_seq,
		  "restarted admission sequence advances above snapshot freeze");
	{
		off_t before_ensure = source_record_count(source);

		check(meta_request(PS_OP_CREATE, &page_key, 0, 0, 0, 0, NULL) &&
			  meta_request(PS_OP_NBLOCKS, &page_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 2 && source_record_count(source) == before_ensure,
			  "live unstamped CREATE ensure adds no source event and preserves size");
	}
	{
		off_t before_lazy = source_record_count(source);
		off_t after_lazy;

		check(meta_request(PS_OP_CREATE, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
			  reply.req_seq > header.freeze_admission_seq &&
			  source_record_count(source) == before_lazy + 1 &&
			  meta_request(PS_OP_EXISTS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 1 &&
			  meta_request(PS_OP_NBLOCKS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 0,
			  "post-snapshot lazy FSM CREATE writes exactly one durable empty fork event");
		after_lazy = source_record_count(source);
		check(meta_request(PS_OP_CREATE, &lazy_fsm_key, 0, 0, 0, 0, NULL) &&
			  source_record_count(source) == after_lazy,
			  "repeated lazy FSM ensure does not duplicate its durable event");
		close_runtime();
		check(ps_core_open(store) == 0 &&
			  source_record_count(source) == after_lazy &&
			  meta_request(PS_OP_EXISTS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 1 &&
			  meta_request(PS_OP_NBLOCKS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 0,
			  "lazy FSM CREATE replays exactly once with empty-fork semantics");
	}
	check(!meta_request(PS_OP_CREATE, &delayed_create_key,
							 header.cutoff_lsn - 1, 0, 0, 0, NULL),
		  "explicit delayed historical CREATE remains rejected");
	{
		off_t before_delayed_grow = source_record_count(source);

		check(!meta_request(PS_OP_ZEROEXTEND, &lazy_fsm_key,
							  header.cutoff_lsn - 1, 0, 3, 0, NULL) &&
			  source_record_count(source) == before_delayed_grow &&
			  meta_request(PS_OP_NBLOCKS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 0,
			  "explicit below-cutoff ZEROEXTEND is rejected before LSN clamp");
	}
	check(meta_request(PS_OP_ZEROEXTEND, &lazy_fsm_key, 0, 0, 3, 0, NULL) &&
		  meta_request(PS_OP_NBLOCKS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 3,
		  "post-snapshot unstamped ZEROEXTEND uses an operational future position");
	check(meta_request(PS_OP_TRUNCATE, &lazy_fsm_key, 0, 0, 1, 0, NULL) &&
		  meta_request(PS_OP_NBLOCKS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 1,
		  "post-snapshot unstamped TRUNCATE uses an operational future position");
	check(meta_request(PS_OP_UNLINK, &lazy_fsm_key, 0, 0, 0, 0, NULL) &&
		  meta_request(PS_OP_EXISTS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 0 &&
		  meta_request(PS_OP_NBLOCKS, &lazy_fsm_key, 0, 0, 0, 0, &reply) &&
		  reply.result == 0,
		  "post-snapshot unstamped UNLINK durably kills the fork");
	check(!meta_request(PS_OP_TRUNCATE, &after_key, 199, 0, 1, 0, NULL),
		  "fork mutation below selected cutoff is rejected");
	{
		char segment_path[1200];
		off_t before;

		n = snprintf(segment_path, sizeof(segment_path), "%s/seg_00000000", store);
		before = n > 0 && (size_t) n < sizeof(segment_path) ?
			file_size(segment_path) : -1;
		check(before >= 0 &&
			  append_relation(&page_key, 0, 50, page, NULL) == 0 &&
			  file_size(segment_path) > before,
			  "ordered non-growth rewrite advances its operational floor to the cutoff");
	}
	check(append_relation(&boundary_key, 0, 199, page, NULL) == 0 &&
		  append_relation(&boundary_key, 0, 199, page, NULL) == 0,
		  "same-cutoff-LSN ordered growth and non-growth accept future sequences");

	check(append_growth_batch(900, 520) &&
		  read_selected_header(snapshots, &header) == 0,
		  "prepare a due source epoch before ordered-marker failure");
	poison_generation = header.generation;
	poison_source_size = file_size(source);
	close_runtime();
	check(setenv("PAGESTORE_TEST_FAIL_FORK_META_APPEND_AT", "1", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "reopen with ordered marker append failure armed");
	(void) ps_core_maintenance();
	check(append_relation_tag(&boundary_key, 0, 199, page, 0xa5, NULL) != 0,
		  "complete ordered body fails when its bound marker append fails");
	check(append_relation_tag(&page_key, 0, 600, page, 0x33, NULL) == 0,
		  "unrelated non-forkmeta page activity advances admission after failure");
	(void) ps_core_maintenance();
	check(read_selected_header(snapshots, &header) == 0 &&
		  header.generation == poison_generation &&
		  file_size(source) == poison_source_size,
		  "marker failure poison blocks snapshot cutover and source rewrite");
	close_runtime();
	unsetenv("PAGESTORE_TEST_FAIL_FORK_META_APPEND_AT");
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &boundary_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0,
		  "restart skips failed post-cutover ordered body above prior freeze");
	(void) ps_core_maintenance();
	check(append_relation_tag(&boundary_key, 0, 199, page, 0x5a, NULL) == 0,
		  "fresh process resumes ordered marker publication safely");
	close_runtime();
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &boundary_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x5a,
		  "successfully marked ordered page survives the following restart");
	(void) ps_core_maintenance();

	check(append_growth_batch(1000, 600),
		  "append source suffix for manifest-before-rename fault");
	source_before_fault = file_size(source);
	check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_BEFORE_RENAME", "1", 1) == 0,
		  "arm manifest-before-rename failure");
	(void) ps_core_maintenance();
	unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_BEFORE_RENAME");
	check(file_size(source) == source_before_fault,
		  "manifest commit failure never rewrites source");
	check(!append_growth_batch(1100, 700),
		  "manifest ambiguity poisons subsequent forkmeta mutation");
	close_runtime();
	check(ps_core_open(store) == 0,
		  "restart aborts non-surviving manifest intent");
	(void) ps_core_maintenance();
	check(ps_forkmeta_snapshot_read_prepared(snapshots, &stale) == 0,
		  "non-surviving manifest intent is no liveness blocker");

	check(append_growth_batch(1200, 800),
		  "append source suffix for manifest-after-rename fault");
	source_before_fault = file_size(source);
	check(setenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_AFTER_RENAME", "1", 1) == 0,
		  "arm manifest-after-rename ambiguity");
	(void) ps_core_maintenance();
	unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_AFTER_RENAME");
	check(file_size(source) == source_before_fault,
		  "surviving ambiguous manifest still leaves source unchanged in process");
	check(!append_growth_batch(1300, 900),
		  "surviving manifest ambiguity poisons mutation");
	close_runtime();
	check(ps_core_open(store) == 0 && source_is_marker_only(store, &marker),
		  "restart selects survived manifest and replaces the old epoch");
	(void) ps_core_maintenance();
	check(ps_forkmeta_snapshot_read_prepared(snapshots, &stale) == 0,
		  "matching selected intent is finalized on restart");

	close_runtime();
	check(setenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME", "1", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "open with source rewrite before-rename fault armed");
	(void) ps_core_maintenance();
	check(append_growth_batch(1400, 1000),
		  "append source suffix for rewrite-before-rename fault");
	source_before_fault = file_size(source);
	(void) ps_core_maintenance();
	check(file_size(source) == source_before_fault &&
		  !append_growth_batch(1500, 1100),
		  "rewrite-before-rename poisons and preserves old source");
	close_runtime();
	unsetenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME");
	check(ps_core_open(store) == 0 && source_is_marker_only(store, &marker),
		  "restart reconciles selected manifest after pre-rename rewrite failure");
	(void) ps_core_maintenance();

	close_runtime();
	check(setenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_DIR_FSYNC", "1", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "open with source rewrite post-rename fault armed");
	(void) ps_core_maintenance();
	check(append_growth_batch(1600, 1200),
		  "append source suffix for rewrite-post-rename fault");
	(void) ps_core_maintenance();
	check(source_is_marker_only(store, &marker) &&
		  !append_growth_batch(1700, 1300),
		  "post-rename durability ambiguity poisons with visible new epoch");
	close_runtime();
	unsetenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_DIR_FSYNC");
	check(ps_core_open(store) == 0 && source_is_marker_only(store, &marker),
		  "restart reconciles post-rename source ambiguity deterministically");
	(void) ps_core_maintenance();
	close_runtime();
	{
		TestForkMetaRecV2 valid = marker;
		TestForkMetaRecV2 torn = marker;

		valid.key = torn_tail_key;
		valid.lsn++;
		valid.admission_seq++;
		valid.order_id = 0;
		valid.nblocks = 2;
		valid.kind = TEST_FEV_GROW;
		torn = valid;
		torn.lsn++;
		torn.admission_seq++;
		as_legacy_record(&valid);
		as_legacy_record(&torn);
		check(append_source_record(source, &valid) &&
			  append_source_bytes(source, &torn, sizeof(torn) / 2),
			  "append valid selected suffix followed by a torn crash tail");
		check(ps_core_open(store) == 0 &&
			  file_size(source) == (off_t) (2 * sizeof(TestForkMetaRecV2)) &&
			  meta_request(PS_OP_NBLOCKS, &torn_tail_key, 0, 0, 0, 0, &reply) &&
			  reply.result == 2,
			  "startup retains complete selected suffix and truncates only torn tail");
		close_runtime();
	}
	{
		TestForkMetaRecV2 bad;

		check(append_source_record(source, &marker),
			  "append an unexpected later epoch marker fixture");
		check(expect_open_failure(store),
			  "unexpected later epoch marker fails startup closed");
		check(restore_marker_only(source, sizeof(marker)),
			  "restore exact marker-only source after corruption test");

		bad = marker;
		bad.lsn++;
		bad.admission_seq++;
		bad.order_id = 0;
		bad.kind = TEST_FEV_MIGRATED;
		as_legacy_record(&bad);
		check(append_source_record(source, &bad) && expect_open_failure(store),
			  "stale migration marker after matching epoch fails startup closed");
		check(restore_marker_only(source, sizeof(marker)),
			  "restore source after migration-marker corruption");

		bad = marker;
		bad.timeline = TEST_MAX_TIMELINES;
		bad.key.klass = PS_KLASS_RELATION;
		bad.lsn++;
		bad.admission_seq++;
		bad.order_id = 0;
		bad.nblocks = 1;
		bad.kind = TEST_FEV_GROW;
		as_legacy_record(&bad);
		check(append_source_record(source, &bad) && expect_open_failure(store),
			  "malformed current-epoch timeline fails startup closed");
		check(restore_marker_only(source, sizeof(marker)),
			  "restore source after timeline corruption");

		bad.timeline = 0;
		bad.kind = 255;
		as_legacy_record(&bad);
		check(append_source_record(source, &bad) && expect_open_failure(store),
			  "malformed current-epoch kind fails startup closed");
		check(restore_marker_only(source, sizeof(marker)),
			  "restore source after kind corruption");
	}
	check(read_selected_header(snapshots, &header) == 0,
		  "read latest selected payload before corruption test");
	{
		char checkpoint[1400];
		unsigned char byte = 0;
		int fd;

		n = snprintf(checkpoint, sizeof(checkpoint),
					 "%s/forkmeta_checkpoint_v1_%020llu", snapshots,
					 (unsigned long long) header.generation);
		fd = n > 0 && (size_t) n < sizeof(checkpoint) ?
			open(checkpoint, O_RDWR) : -1;
		check(fd >= 0 && pread(fd, &byte, 1, sizeof(TestSnapshotHeader)) == 1,
			  "read selected snapshot byte for corruption fixture");
		byte ^= 0x5a;
		check(fd >= 0 && pwrite(fd, &byte, 1, sizeof(TestSnapshotHeader)) == 1 &&
			  fsync(fd) == 0,
			  "corrupt selected snapshot payload");
		check(expect_open_failure(store),
			  "corrupt selected snapshot fails startup closed");
		byte ^= 0x5a;
		check(fd >= 0 && pwrite(fd, &byte, 1, sizeof(TestSnapshotHeader)) == 1 &&
			  fsync(fd) == 0 && close(fd) == 0,
			  "restore selected snapshot payload");
	}
	check(ps_core_open(store) == 0,
		  "store reopens after restoring corruption fixtures");
	{
		uint64_t create_lsn = header.cutoff_lsn + 100;
		uint64_t grow_lsn = header.cutoff_lsn + 110;
		uint64_t page_lsn = header.cutoff_lsn + 120;
		uint64_t branch_lsn = header.cutoff_lsn + 130;
		TestForkMetaRecV2 last;

		check(meta_request(PS_OP_CREATE, &ancestry_key, create_lsn,
							 0, 0, 0, NULL) &&
			  meta_request(PS_OP_ZEROEXTEND, &ancestry_key, grow_lsn,
							 0, 4, 0, NULL) &&
			  append_relation(&ancestry_key, 0, page_lsn, page, NULL) == 0,
			  "parent publishes newer fork and page state after selected snapshot");
		check(create_branch_request(1, 0, branch_lsn),
			  "create child through durable timeline metadata path");
		check(meta_request_timeline(1, PS_OP_NBLOCKS, &ancestry_key,
								   0, 0, 0, 0, &reply) && reply.result == 4,
			  "local-empty child initially inherits the capped parent size");
		check(meta_request_timeline(1, PS_OP_TRUNCATE, &ancestry_key,
								   0, 0, 2, 0, NULL) &&
			  read_last_source_record(source, &last) && last.timeline == 1 &&
			  last.key.relNumber == ancestry_key.relNumber &&
			  last.lsn == page_lsn + 1 && last.kind == TEST_FEV_SET &&
			  last.nblocks == 2,
			  "child unstamped mutation orders after newest inherited page state");
		check(meta_request_timeline(1, PS_OP_NBLOCKS, &ancestry_key,
								   page_lsn, 0, 0, 0, &reply) && reply.result == 4 &&
			  meta_request_timeline(1, PS_OP_NBLOCKS, &ancestry_key,
								   page_lsn + 1, 0, 0, 0, &reply) && reply.result == 2,
			  "child operational truncate preserves inherited pre-mutation history");
	}
	{
		/*
		 * Codex round-3 review finding 4098831579: an explicit UNLINK or
		 * TRUNCATE below the durable forkmeta snapshot cutoff, colliding
		 * with a retained event (round3_reject_key's real CREATE@50,
		 * created at the very top of this test, well before this cutover
		 * was ever selected), must be REJECTED -- not rescued into
		 * acceptance by fork_meta_lsn_promote() bumping it above a live
		 * descendant fence.  A descendant branch with a cap at/above the
		 * cutoff supplies that fence: without the fix,
		 * fence_promote_lsn() promotes the mutation's lsn comfortably
		 * above the cutoff (to sort after the branch's own frozen view),
		 * and fork_meta_persist() then validates only that PROMOTED
		 * position -- never the caller's real, below-cutoff request.
		 */
		uint64_t	fence_branch_lsn = header.cutoff_lsn + 500;

		check(create_branch_request(9, 0, fence_branch_lsn),
			  "round 3: create a descendant fence at/above the cutoff");
		check(!meta_request(PS_OP_TRUNCATE, &round3_reject_key,
							 50, 0, 3, 0, NULL),
			  "round 3: an explicit TRUNCATE below the cutoff colliding with a retained event is rejected, not rescued by promotion above the descendant fence");
		check(!meta_request(PS_OP_UNLINK, &round3_reject_key,
							 50, 0, 0, 0, NULL),
			  "round 3: an explicit UNLINK below the cutoff colliding with the same retained event is also rejected");
	}
	close_runtime();
	test_legacy_only_deletion_filtered_forkmeta();
	test_deletion_filtered_forkmeta();
	test_deletion_filtered_forkmeta_commit_shape();
	test_reclaimed_ordered_markers_pruned();
	test_inert_markers_compacted_after_cutover();
	test_inert_markers_kept_when_retained();
	test_inert_markers_kept_on_preserve_survivors();
	test_deleting_fork_flags_cleared_on_failed_build_skip();
	test_fork_event_index_periodic_cutover_bounded();
	test_v1_bound_marker_snapshot();
	test_no_manifest_marker_only_rejected();
	test_live_ordered_marker_survives_two_cutovers();
	test_live_ordered_commit_marker_survives_two_cutovers();
	test_live_ordered_marker_walless_survives_two_cutovers();
	for (uint32_t timeline = 0; timeline <= 1; timeline++)
		for (int walless = 0; walless <= 1; walless++)
			test_ordered_write_after_cutoff(timeline, walless);
	for (int walless = 0; walless <= 1; walless++)
		test_ordered_parent_after_branch(walless);
	test_fork_event_index_scaling();
	test_orphaned_ordered_marker_adopted();
	test_orphaned_ordered_marker_not_adopted_on_mismatch();
	test_orphaned_commit_marker_adopted();
	test_orphaned_commit_marker_segment_path();
	test_orphaned_commit_marker_segment_path_last_is_retired();
	test_orphaned_commit_marker_not_proven();
	test_orphaned_commit_marker_size_mismatch();
	test_torn_commit_append_never_adopted();
	test_torn_growth_append_never_adopted();
	test_artifact_generation_vs_cutoff();
	test_artifact_forkmeta_cutoff_reason();
	test_artifact_write_unfenced_after_pin_drop();
	test_hole_record_skipped_on_reopen();
	test_hole_record_bad_len_rejected();
	test_timeline_delete_keeps_offsets();
	test_timeline_delete_reclaims_by_segment_gc();
	test_compaction_never_prunes_above_watermark();
	test_timeline_delete_crash_matrix();
	test_timeline_delete_torn_tail_crash_matrix();
	if (!failed)
		remove_tree(store);
	else
		fprintf(stderr, "failed test store retained at %s\n", store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
	printf("pagestore_forkmeta_cutover_test: %d checks, %d failed\n",
		   checks, failed);
	return failed != 0;
}
