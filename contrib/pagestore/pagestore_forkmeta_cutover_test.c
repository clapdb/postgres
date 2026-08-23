#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_forkmeta_snapshot.h"
#include "pagestore_prune.h"
#include "pagestore_retention.h"

static int checks;
static int failed;

#define TEST_FORK_META_V2_MAGIC 0x324d4b46U
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
		marker->magic == TEST_FORK_META_V2_MAGIC &&
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
append_growth_batch(uint32_t rel_base, uint64_t lsn_base)
{
	for (uint32_t i = 0; i < 20; i++)
	{
		PsKey key = {7, 7, rel_base + i, 0, PS_KLASS_RELATION};
		int rc;

		ps_admission_read_lock();
		ps_lock_shard_wr(ps_shard_of(&key));
		rc = fork_grow(0, &key, 1, lsn_base + i);
		ps_unlock_shard(ps_shard_of(&key));
		ps_admission_read_unlock();
		if (rc != 0)
			return 0;
	}
	return 1;
}

static void
close_runtime(void)
{
	ps_core_close();
	if (ps_storage->close)
		ps_storage->close();
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
			  append_relation(&page_key, 0, 50, page, NULL) != 0 &&
			  file_size(segment_path) == before,
			  "ordered non-growth rewrite below cutoff is rejected before segment write");
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
	check(append_relation_tag(&boundary_key, 0, 199, page, 0x5a, NULL) == 0,
		  "fresh process resumes ordered marker publication safely");
	close_runtime();
	check(ps_core_open(store) == 0 &&
		  read_resolve(0, &boundary_key, 0, UINT64_MAX, 0, page, NULL) == 1 &&
		  page[128] == 0x5a,
		  "successfully marked ordered page survives the following restart");

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
	check(ps_forkmeta_snapshot_read_prepared(snapshots, &stale) == 0,
		  "matching selected intent is finalized on restart");

	close_runtime();
	check(setenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME", "1", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "open with source rewrite before-rename fault armed");
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

	close_runtime();
	check(setenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_DIR_FSYNC", "1", 1) == 0 &&
		  ps_core_open(store) == 0,
		  "open with source rewrite post-rename fault armed");
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
		check(append_source_record(source, &bad) && expect_open_failure(store),
			  "malformed current-epoch timeline fails startup closed");
		check(restore_marker_only(source, sizeof(marker)),
			  "restore source after timeline corruption");

		bad.timeline = 0;
		bad.kind = 255;
		check(append_source_record(source, &bad) && expect_open_failure(store),
			  "malformed current-epoch kind fails startup closed");
		check(restore_marker_only(source, sizeof(marker)),
			  "restore source after kind corruption");
	}
	check(read_selected_header(snapshots, &header) == 0,
		  "read latest selected payload before corruption test");
	{
		char checkpoint[1400];
		unsigned char byte;
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
	close_runtime();
	test_reclaimed_ordered_markers_pruned();
	test_v1_bound_marker_snapshot();
	test_no_manifest_marker_only_rejected();
	if (!failed)
		remove_tree(store);
	else
		fprintf(stderr, "failed test store retained at %s\n", store);
	unsetenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES");
	printf("pagestore_forkmeta_cutover_test: %d checks, %d failed\n",
		   checks, failed);
	return failed != 0;
}
