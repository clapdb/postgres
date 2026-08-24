#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_fault.h"
#include "pagestore_forkmeta_snapshot.h"
#include "pagestore_retention.h"

#define MATRIX_SHARDS 4
#define MATRIX_KEYS 24
#define TEST_FORK_META_V2_MAGIC UINT32_C(0x324d4b46)
#define TEST_MAX_TIMELINES 1024
#define TEST_FEV_SNAPSHOT_BASE 10
#define TEST_FEV_GROW 0
#define TEST_FEV_SET 1
#define TEST_FEV_DEAD 2
#define TEST_FEV_SEG_GROW 5
#define TEST_FEV_SEG_COMMIT 6
#define TEST_FEV_SEG_GROW_BOUND 7
#define TEST_FEV_SEG_COMMIT_BOUND 8
#define ACK_CAPACITY 16

typedef enum CrashCase
{
	CASE_AFTER_PREPARE,
	CASE_AFTER_MANIFEST_COMMIT,
	CASE_AFTER_SOURCE_REWRITE,
	CASE_AFTER_SNAPSHOT_GC,
	CASE_COUNT
} CrashCase;

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

typedef struct ConcurrentAppend
{
	PsKey *keys;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int admission_entered;
	int write_lock_path_entered;
	int write_lock_path_returned;
	int release_writer;
	int operation_acknowledged;
	int operation_failed;
	struct AckLedger *ledger;
	int ok;
} ConcurrentAppend;

typedef struct AckEntry
{
	uint32_t key_index;
	uint32_t nblocks;
} AckEntry;

typedef struct AckLedger
{
	volatile uint32_t count;
	volatile int overlap_observed;
	AckEntry entries[ACK_CAPACITY];
} AckLedger;

typedef struct PreRecoveryEvidence
{
	uint64_t prepared_generation;
	uint64_t selected_generation;
} PreRecoveryEvidence;

static int checks;
static int failures;

static const char *
case_fault_name(CrashCase which)
{
	switch (which)
	{
		case CASE_AFTER_PREPARE:
			return "forkmeta.after_prepare";
		case CASE_AFTER_MANIFEST_COMMIT:
			return "forkmeta.after_manifest_commit";
		case CASE_AFTER_SOURCE_REWRITE:
			return "forkmeta.after_source_rewrite";
		case CASE_AFTER_SNAPSHOT_GC:
			return "forkmeta.after_snapshot_gc";
		default:
			return NULL;
	}
}

static void
check(int ok, const char *name)
{
	checks++;
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failures++;
	}
}

static int
remove_tree(const char *path)
{
	DIR *dir = opendir(path);
	struct dirent *entry;
	int ok = 1;

	if (dir == NULL)
	{
		return unlink(path) == 0 || errno == ENOENT;
	}
	while ((entry = readdir(dir)) != NULL)
	{
		char child[1600];
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) < 0 ||
			lstat(child, &st) != 0)
		{
			ok = 0;
			continue;
		}
		if (S_ISDIR(st.st_mode))
			ok = remove_tree(child) && ok;
		else
			if (unlink(child) != 0 && errno != ENOENT)
				ok = 0;
	}
	if (closedir(dir) != 0)
		ok = 0;
	if (rmdir(path) != 0 && errno != ENOENT)
		ok = 0;
	return ok;
}

static void
close_runtime(void)
{
	ps_core_close();
	if (ps_storage->close != NULL)
		ps_storage->close();
}

static int
meta_request(PsOpcode opcode, const PsKey *key, uint64_t lsn,
				 uint32_t nblocks, PsChannel *result)
{
	PsChannel ch;
	uint32_t shard = ps_shard_of(key);
	int write_op = opcode == PS_OP_CREATE || opcode == PS_OP_UNLINK ||
		opcode == PS_OP_TRUNCATE || opcode == PS_OP_ZEROEXTEND;

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.timeline = 0;
	ch.key = *key;
	ch.req_lsn = lsn;
	ch.nblocks = nblocks;
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
	if (result != NULL)
		*result = ch;
	return ch.status == PS_STATUS_OK;
}

static int
grow_key(const PsKey *key, uint32_t nblocks, uint64_t lsn)
{
	uint32_t shard = ps_shard_of(key);
	int rc;

	ps_admission_read_lock();
	ps_lock_shard_wr(shard);
	rc = fork_grow(0, key, nblocks, lsn);
	ps_unlock_shard(shard);
	ps_admission_read_unlock();
	return rc == 0;
}

static int
append_anchor_page(const PsKey *key, uint64_t lsn, unsigned char tag,
					 uint64_t *seq_out)
{
	unsigned char page[8192];
	uint32_t shard = ps_shard_of(key);
	int rc;

	memset(page, 0, sizeof(page));
	{
		uint32_t hi = (uint32_t) (lsn >> 32);
		uint32_t lo = (uint32_t) lsn;

		memcpy(page, &hi, sizeof(hi));
		memcpy(page + sizeof(hi), &lo, sizeof(lo));
	}
	page[128] = tag;
	ps_admission_read_lock();
	ps_lock_shard_wr(shard);
	rc = append_page(0, key, 0, page, 0, seq_out);
	ps_unlock_shard(shard);
	ps_admission_read_unlock();
	return rc == 0;
}

static int
find_matrix_keys(PsKey keys[MATRIX_KEYS])
{
	int found[MATRIX_SHARDS] = {0, 0, 0, 0};
	uint32_t count = 0;

	memset(keys, 0, MATRIX_KEYS * sizeof(*keys));
	for (uint32_t rel = 1; count < MATRIX_KEYS && rel < 1000000; rel++)
	{
		PsKey key = {17, 29, rel, 0, PS_KLASS_RELATION};
		uint32_t shard = ps_shard_of(&key);

		if (count < MATRIX_SHARDS && !found[shard])
		{
			keys[count++] = key;
			found[shard] = 1;
		}
		else if (count >= MATRIX_SHARDS)
			keys[count++] = key;
	}
	if (count != MATRIX_KEYS)
		return 0;
	for (uint32_t shard = 0; shard < MATRIX_SHARDS; shard++)
		if (!found[shard])
			return 0;
	for (uint32_t i = 0; i < MATRIX_KEYS; i++)
	{
		if (keys[i].spcOid != 17 || keys[i].dbOid != 29 ||
			keys[i].relNumber == 0 || keys[i].klass != PS_KLASS_RELATION ||
			ps_shard_of(&keys[i]) >= MATRIX_SHARDS)
			return 0;
		for (uint32_t j = 0; j < i; j++)
			if (memcmp(&keys[i], &keys[j], sizeof(keys[i])) == 0)
				return 0;
	}
	return 1;
}

static int
drive_until_frontier(const char *store)
{
	char path[1600];

	if (snprintf(path, sizeof(path), "%s/page-prune.frontiers", store) < 0)
		return 0;
	for (unsigned int i = 0; i < 10000; i++)
	{
		(void) ps_core_maintenance();
		if (access(path, F_OK) == 0)
			return 1;
		sched_yield();
	}
	return 0;
}

static int
selected_generation(const char *store, uint64_t *generation)
{
	char directory[1600];
	PsForkmetaSnapshot selected = {
		.directory_fd = -1, .checkpoint_fd = -1, .tail_fd = -1
	};
	int rc;

	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots", store) < 0)
		return -1;
	rc = ps_forkmeta_snapshot_open(&selected, directory);
	if (rc == 0)
	{
		*generation = selected.generation;
		ps_forkmeta_snapshot_close(&selected);
	}
	return rc;
}

static int
parse_matrix_generation_name(const char *name, const char *prefix,
						 uint64_t *generation)
{
	size_t prefix_len = strlen(prefix);
	uint64_t value = 0;

	if (strlen(name) != prefix_len + 20 ||
		strncmp(name, prefix, prefix_len) != 0)
		return -1;
	for (size_t i = prefix_len; i < prefix_len + 20; i++)
	{
		unsigned int digit;

		if (name[i] < '0' || name[i] > '9')
			return -1;
		digit = (unsigned int) (name[i] - '0');
		if (value > (UINT64_MAX - digit) / 10)
			return -1;
		value = value * 10 + digit;
	}
	if (value == 0)
		return -1;
	if (generation != NULL)
		*generation = value;
	return 0;
}

static int
snapshot_temp_name(const char *name, uint64_t *generation)
{
	const char *marker = strstr(name, ".tmp.");
	const char *parts[] = {
		"forkmeta_checkpoint_v1_", "forkmeta_tail_v1_"
	};
	const char *other[] = {"forkmeta_manifest_v1", "forkmeta_prepared_v1"};
	char base[128];
	const char *p;
	size_t base_len;

	if (marker == NULL || strstr(marker + 5, ".tmp.") != NULL)
		return -1;
	base_len = (size_t) (marker - name);
	if (base_len >= sizeof(base))
		return -1;
	memcpy(base, name, base_len);
	base[base_len] = '\0';
	for (size_t i = 0; i < sizeof(parts) / sizeof(parts[0]); i++)
		if (parse_matrix_generation_name(base, parts[i], generation) == 0)
			goto valid_suffix;
	for (size_t i = 0; i < sizeof(other) / sizeof(other[0]); i++)
		if (strcmp(base, other[i]) == 0)
		{
			if (generation != NULL)
				*generation = 0;
			goto valid_suffix;
		}
	return -1;

valid_suffix:
	p = marker + 5;
	if (*p < '0' || *p > '9')
		return -1;
	while (*p >= '0' && *p <= '9')
		p++;
	if (*p++ != '.' || *p < '0' || *p > '9')
		return -1;
	while (*p >= '0' && *p <= '9')
		p++;
	return *p == '\0' ? 0 : -1;
}

static int
generation_artifact_count(const char *store, uint64_t generation)
{
	const char *prefixes[] = {
		"forkmeta_checkpoint_v1_", "forkmeta_tail_v1_"
	};
	char directory[1600];
	DIR *dir;
	struct dirent *entry;
	int count = 0;

	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots", store) < 0)
		return -1;
	dir = opendir(directory);
	if (dir == NULL)
		return -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
		for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++)
		{
			uint64_t found = 0;

			if (parse_matrix_generation_name(entry->d_name, prefixes[i], &found) == 0 &&
				found == generation)
				count++;
			else if (snapshot_temp_name(entry->d_name, &found) == 0 &&
				found == generation &&
				strncmp(entry->d_name, prefixes[i], strlen(prefixes[i])) == 0)
				count++;
		}
	{
		int saved_errno = errno;
		int close_rc = closedir(dir);

		return saved_errno == 0 && close_rc == 0 ? count : -1;
	}
}

static int
snapshot_temp_count(const char *store)
{
	char directory[1600];
	DIR *dir;
	struct dirent *entry;
	int count = 0;

	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots", store) < 0)
		return -1;
	dir = opendir(directory);
	if (dir == NULL)
		return -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
		if (snapshot_temp_name(entry->d_name, NULL) == 0)
			count++;
	{
		int saved_errno = errno;
		int close_rc = closedir(dir);

		return saved_errno == 0 && close_rc == 0 ? count : -1;
	}
}

static int
prepared_parts_complete(const char *store,
						const PsForkmetaSnapshotPrepared *prepared)
{
	const char *prefixes[] = {
		"forkmeta_checkpoint_v1_", "forkmeta_tail_v1_"
	};
	const uint64_t lengths[] = {prepared->checkpoint.len, prepared->tail.len};
	char directory[1600];
	char name[128];
	struct stat st;
	int fd;

	if (snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots", store) < 0)
		return 0;
	for (unsigned int i = 0; i < 2; i++)
	{
		if (snprintf(name, sizeof(name), "%s%020llu", prefixes[i],
					 (unsigned long long) prepared->generation) < 0)
			return 0;
		fd = openat(AT_FDCWD, directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
		if (fd < 0)
			return 0;
		if (fstatat(fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0 ||
			!S_ISREG(st.st_mode) || st.st_size < 0 ||
			(uint64_t) st.st_size != lengths[i])
		{
			(void) close(fd);
			return 0;
		}
		if (close(fd) != 0)
			return 0;
	}
	return 1;
}

static void
verify_pre_recovery(const char *store, CrashCase which,
					PreRecoveryEvidence *evidence)
{
	char directory[1600];
	char manifest[1800];
	PsForkmetaSnapshotPrepared prepared;
	PsForkmetaSnapshot selected = {
		.directory_fd = -1, .checkpoint_fd = -1, .tail_fd = -1
	};
	uint64_t generation = 0;
	int prepared_rc;

	memset(evidence, 0, sizeof(*evidence));
	check(snprintf(directory, sizeof(directory), "%s/forkmeta_snapshots", store) >= 0 &&
		  snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1",
				   directory) >= 0,
		  "build pre-recovery snapshot paths");
	if (which == CASE_AFTER_PREPARE)
	{
		check(access(manifest, F_OK) != 0 && errno == ENOENT,
			  "prepare crash has not selected a manifest");
		check(selected_generation(store, &generation) != 0,
			  "prepare crash has no selected generation");
		prepared_rc = ps_forkmeta_snapshot_read_prepared(directory, &prepared);
		check(prepared_rc == 1 &&
			  prepared.generation != 0 && prepared.checkpoint.len != 0 &&
			  prepared.tail.len != 0 && prepared_parts_complete(store, &prepared),
			  "prepare crash retains a complete checksum-valid intent and both immutable parts");
		if (prepared_rc == 1)
			evidence->prepared_generation = prepared.generation;
	}
	else
	{
		uint64_t expected = which == CASE_AFTER_SNAPSHOT_GC ? 2 : 1;

		check(selected_generation(store, &generation) == 0 && generation == expected,
			  "pre-recovery manifest selects the expected complete generation");
		evidence->selected_generation = generation;
	}
	if (which == CASE_AFTER_SNAPSHOT_GC)
	{
		check(generation_artifact_count(store, 1) == 0,
			  "durable GC removed every generation-one part artifact");
		check(snapshot_temp_count(store) == 0,
			  "durable GC removed every snapshot temporary artifact");
		check(generation_artifact_count(store, 2) == 2 &&
			  ps_forkmeta_snapshot_open(&selected, directory) == 0 &&
			  selected.generation == 2,
			  "durable GC preserves both checksum-valid generation-two parts");
		if (selected.directory_fd >= 0)
			ps_forkmeta_snapshot_close(&selected);
	}
}

static int
drive_until_generation(const char *store, uint64_t want)
{
	uint64_t generation = 0;

	for (unsigned int i = 0; i < 10000; i++)
	{
		(void) ps_core_maintenance();
		if (selected_generation(store, &generation) == 0 && generation >= want)
			return 1;
		sched_yield();
	}
	return 0;
}

static int
arm_fault(const char *fault_dir)
{
	char path[1600];
	int fd;

	if (snprintf(path, sizeof(path), "%s/arm", fault_dir) < 0)
		return 0;
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
	if (fd < 0)
		return 0;
	return close(fd) == 0;
}

static int
configure_fault(const char *store, const char *fault_dir, const char *name)
{
	return setenv("PAGESTORE_TEST_FAULT_NAME", name, 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_ACTION", "crash", 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_HIT", "1", 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_DIR", fault_dir, 1) == 0 &&
		ps_fault_init(store) == 0;
}

static int
populate_store(const char *store, PsKey keys[MATRIX_KEYS])
{
	PsRetentionPin pin;
	uint64_t first_seq = 0;
	uint64_t second_seq = 0;

	if (!find_matrix_keys(keys))
	{
		dprintf(STDERR_FILENO, "matrix key discovery failed\n");
		return 0;
	}
	if (!meta_request(PS_OP_CREATE, &keys[0], 100, 0, NULL) ||
		!grow_key(&keys[0], 1, 100) ||
		!append_anchor_page(&keys[0], 100, 0x11, &first_seq) ||
		!append_anchor_page(&keys[0], 200, 0x22, &second_seq))
	{
		dprintf(STDERR_FILENO, "populate anchor failed\n");
		return 0;
	}
	for (unsigned int i = 1; i < MATRIX_KEYS; i++)
		if (!grow_key(&keys[i], 1, 100 + i))
		{
			dprintf(STDERR_FILENO, "populate key failed i=%u shard=%u\n", i,
					ps_shard_of(&keys[i]));
			return 0;
		}
	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 9001;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = 1;
	pin.lsn = 200;
	pin.admission_seq = second_seq;
	if (first_seq == 0 || second_seq <= first_seq)
	{
		dprintf(STDERR_FILENO, "populate sequence failed first=%llu second=%llu\n",
				(unsigned long long) first_seq, (unsigned long long) second_seq);
		return 0;
	}
	if (ps_retention_set(&pin) != PS_RETENTION_OK)
	{
		dprintf(STDERR_FILENO, "populate retention failed seq=%llu\n",
				(unsigned long long) second_seq);
		return 0;
	}
	if (!drive_until_frontier(store))
	{
		dprintf(STDERR_FILENO, "populate frontier failed\n");
		return 0;
	}
	return 1;
}

static int
source_epoch_valid(const char *store, uint64_t generation, int require_marker)
{
	char path[1600];
	TestForkMetaRecV2 rec;
	struct stat st;
	int fd;
	off_t offset;

	if (snprintf(path, sizeof(path), "%s/forkmeta", store) < 0 ||
		stat(path, &st) != 0 || st.st_size < (off_t) sizeof(rec) ||
		st.st_size % (off_t) sizeof(rec) != 0)
		return 0;
	fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0 || pread(fd, &rec, sizeof(rec), 0) != (ssize_t) sizeof(rec))
	{
		if (fd >= 0)
			(void) close(fd);
		return 0;
	}
	if (require_marker && (rec.magic != TEST_FORK_META_V2_MAGIC ||
			rec.rec_len != sizeof(rec) || rec.kind != TEST_FEV_SNAPSHOT_BASE ||
			rec.order_id != generation || rec.admission_seq == 0))
	{
		(void) close(fd);
		return 0;
	}
	for (offset = 0; offset < st.st_size; offset += (off_t) sizeof(rec))
		if (pread(fd, &rec, sizeof(rec), offset) != (ssize_t) sizeof(rec) ||
			rec.magic != TEST_FORK_META_V2_MAGIC || rec.rec_len != sizeof(rec))
		{
			(void) close(fd);
			return 0;
		}
	(void) close(fd);
	return 1;
}

static int
source_epoch_matches_selected(const char *store, const char *snapshots,
							 uint64_t expected_generation)
{
	PsForkmetaSnapshot selected = {
		.directory_fd = -1, .checkpoint_fd = -1, .tail_fd = -1
	};
	TestForkMetaRecV2 rec;
	PsKey zero_key;
	char path[1600];
	struct stat st;
	uint64_t offset;
	int fd = -1;
	int ok = 0;

	memset(&zero_key, 0, sizeof(zero_key));
	if (ps_forkmeta_snapshot_open(&selected, snapshots) != 0 ||
		selected.generation != expected_generation ||
		snprintf(path, sizeof(path), "%s/forkmeta", store) < 0 ||
		stat(path, &st) != 0 || st.st_size < (off_t) sizeof(rec) ||
		st.st_size % (off_t) sizeof(rec) != 0)
		goto done;
	fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
	if (fd < 0 || pread(fd, &rec, sizeof(rec), 0) != (ssize_t) sizeof(rec) ||
		rec.magic != TEST_FORK_META_V2_MAGIC || rec.rec_len != sizeof(rec) ||
		rec.timeline != 0 || memcmp(&rec.key, &zero_key, sizeof(zero_key)) != 0 ||
		rec.lsn != selected.cutoff_lsn ||
		rec.admission_seq != selected.cutoff_admission_seq ||
		rec.order_id != selected.generation || rec.nblocks != 0 ||
		rec.kind != TEST_FEV_SNAPSHOT_BASE || rec.pad[0] != 0 ||
		rec.pad[1] != 0 || rec.pad[2] != 0)
		goto done;
	for (offset = sizeof(rec); offset < (uint64_t) st.st_size;
			offset += sizeof(rec))
	{
		int ordered_marker;
		int ordered_marker_valid = 0;

		if (pread(fd, &rec, sizeof(rec), (off_t) offset) != (ssize_t) sizeof(rec) ||
			rec.magic != TEST_FORK_META_V2_MAGIC || rec.rec_len != sizeof(rec) ||
			rec.timeline >= TEST_MAX_TIMELINES ||
			rec.key.klass > PS_KLASS_READER_SNAPSHOT ||
			rec.admission_seq == 0 || rec.pad[0] != 0 || rec.pad[1] != 0 ||
			rec.pad[2] != 0 ||
			!(rec.lsn > selected.cutoff_lsn ||
			 (rec.lsn == selected.cutoff_lsn &&
			  rec.admission_seq > selected.cutoff_admission_seq)))
			goto done;
		ordered_marker = rec.kind == TEST_FEV_SEG_GROW_BOUND ||
			rec.kind == TEST_FEV_SEG_COMMIT_BOUND;
		if (ordered_marker)
		{
			ordered_marker_valid =
				rec.timeline < TEST_MAX_TIMELINES &&
				rec.key.klass <= PS_KLASS_READER_SNAPSHOT &&
				rec.nblocks != 0 &&
				rec.order_id != 0 && rec.admission_seq != 0;
			if (!ordered_marker_valid)
				goto done;
		}
		/* Keep this branch in lockstep with fork_meta_selected_suffix_valid()
		 * and fork_meta_ordered_marker_valid() in pagestore_core.c.  In
		 * particular, only bound ordered markers are accepted in a selected
		 * suffix; ordinary records may not carry an order id. */
		switch (rec.kind)
		{
			case TEST_FEV_GROW:
				if (rec.order_id != 0 || rec.nblocks == 0)
					goto done;
				break;
			case TEST_FEV_SET:
				if (rec.order_id != 0)
					goto done;
				break;
			case TEST_FEV_DEAD:
				if (rec.order_id != 0 || rec.nblocks != 0)
					goto done;
				break;
			case TEST_FEV_SEG_GROW_BOUND:
			case TEST_FEV_SEG_COMMIT_BOUND:
				break;
			default:
				goto done;
		}
	}
	ok = 1;

done:
	if (fd >= 0)
		(void) close(fd);
	ps_forkmeta_snapshot_close(&selected);
	return ok;
}

static int
record_ack(AckLedger *ledger, uint32_t key_index, uint32_t nblocks)
{
	uint32_t index = __atomic_load_n(&ledger->count, __ATOMIC_RELAXED);

	if (index >= ACK_CAPACITY)
		return 0;
	ledger->entries[index].key_index = key_index;
	ledger->entries[index].nblocks = nblocks;
	__atomic_store_n(&ledger->count, index + 1, __ATOMIC_RELEASE);
	return 1;
}

/* Runs in the worker after its metadata operation has acquired admission-rd. */
static void
admission_read_hook(void *arg)
{
	ConcurrentAppend *append = arg;

	(void) pthread_mutex_lock(&append->mutex);
	if (!append->admission_entered)
	{
		append->admission_entered = 1;
		(void) pthread_cond_broadcast(&append->cond);
		/* Keep the writer inside admission-rd until a separate coordinator has
		 * observed maintenance enter its real blocking wrlock path. */
		while (!append->release_writer && !append->operation_failed)
			(void) pthread_cond_wait(&append->cond, &append->mutex);
	}
	(void) pthread_mutex_unlock(&append->mutex);
}

/* This is installed as the exact admission-wr call made by maintenance.  It
 * records entry and then immediately calls the real blocking primitive; the
 * coordinator releases the held admission-rd only after this path is entered. */
static int
admission_write_lock_hook(pthread_rwlock_t *lock, void *arg)
{
	ConcurrentAppend *append = arg;
	int try_rc;
	int rc;

	/* This probe is inside the exact replacement for maintenance's wrlock,
	 * not a second observer.  EBUSY proves that this same path encountered the
	 * admitted writer before publishing the handshake below. */
	try_rc = pthread_rwlock_trywrlock(lock);
	if (try_rc != EBUSY)
	{
		if (try_rc == 0)
			(void) pthread_rwlock_unlock(lock);
		(void) pthread_mutex_lock(&append->mutex);
		append->operation_failed = 1;
		append->release_writer = 1;
		(void) pthread_cond_broadcast(&append->cond);
		(void) pthread_mutex_unlock(&append->mutex);
		return try_rc == 0 ? EBUSY : try_rc;
	}
	(void) pthread_mutex_lock(&append->mutex);
	append->write_lock_path_entered = 1;
	__atomic_store_n(&append->ledger->overlap_observed, 1, __ATOMIC_RELEASE);
	(void) pthread_cond_broadcast(&append->cond);
	(void) pthread_mutex_unlock(&append->mutex);

	rc = pthread_rwlock_wrlock(lock);
	(void) pthread_mutex_lock(&append->mutex);
	append->write_lock_path_returned = 1;
	if (rc != 0)
		append->operation_failed = 1;
	(void) pthread_cond_broadcast(&append->cond);
	(void) pthread_mutex_unlock(&append->mutex);
	return rc;
}

/* The coordinator is the only test participant allowed to release the
 * admitted writer.  It waits until maintenance has entered the exact
 * replacement for its blocking wrlock, then lets the worker finish and waits
 * for its explicit durable-operation ack. */
static void *
admission_coordinator(void *arg)
{
	ConcurrentAppend *append = arg;

	(void) pthread_mutex_lock(&append->mutex);
	while (!append->write_lock_path_entered && !append->operation_failed)
		(void) pthread_cond_wait(&append->cond, &append->mutex);
	if (append->write_lock_path_entered)
	{
		append->release_writer = 1;
		(void) pthread_cond_broadcast(&append->cond);
		while (!append->operation_acknowledged && !append->operation_failed)
			(void) pthread_cond_wait(&append->cond, &append->mutex);
	}
	(void) pthread_mutex_unlock(&append->mutex);
	return NULL;
}

static void *
concurrent_appender(void *arg)
{
	ConcurrentAppend *append = arg;

	append->ok = grow_key(&append->keys[0], 2, 350) &&
		record_ack(append->ledger, 0, 2);
	(void) pthread_mutex_lock(&append->mutex);
	if (append->ok)
	{
		append->operation_acknowledged = 1;
	}
	else
		append->operation_failed = 1;
	(void) pthread_cond_broadcast(&append->cond);
	(void) pthread_mutex_unlock(&append->mutex);
	return NULL;
}

static int
run_crashing_child(CrashCase which, const char *store, const char *fault_dir,
				   AckLedger *ledger)
{
	PsKey keys[MATRIX_KEYS];
	char snapshots[1600];
	ConcurrentAppend appender;
	pthread_t thread;
	pthread_t coordinator;
	int thread_started = 0;
	int coordinator_started = 0;

	if (!configure_fault(store, fault_dir, case_fault_name(which)) ||
		ps_core_open(store) != 0 || !populate_store(store, keys))
	{
		dprintf(STDERR_FILENO, "child setup failed case=%s\n", case_fault_name(which));
		_exit(2);
	}
	if (setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1) != 0)
	{
		dprintf(STDERR_FILENO, "child trigger setup failed case=%s\n", case_fault_name(which));
		_exit(2);
	}
	if (which == CASE_AFTER_SNAPSHOT_GC)
	{
		/* Publish generation 1 without arming the fault, then make generation
		 * 2.  The next maintenance tick retires generation 1. */
		if (!drive_until_generation(store, 1))
		{
			dprintf(STDERR_FILENO, "child gen1 failed\n");
			_exit(2);
		}
		for (unsigned int i = 0; i < MATRIX_KEYS; i++)
			if (!grow_key(&keys[i], 2, 600 + i))
				_exit(2);
		/* Generation 1 leaves GC pending.  Publish generation 2, then make the
		 * first GC attempt unlink generation 1 but fail its directory fsync.
		 * The next empty retry must fsync again and is the completion boundary
		 * covered by the named crash probe. */
		if (!drive_until_generation(store, 2) ||
			setenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC", "1", 1) != 0)
		{
			uint64_t observed = 0;
			(void) selected_generation(store, &observed);
			dprintf(STDERR_FILENO, "child gen2/gc fault setup failed observed=%llu\n",
					(unsigned long long) observed);
			_exit(2);
		}
		(void) ps_core_maintenance();
		if (unsetenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") != 0)
			_exit(2);
		ps_test_forkmeta_snapshot_gc_retry_now();
		if (!arm_fault(fault_dir))
			_exit(2);
		(void) ps_core_maintenance();
		_exit(3);
	}
	if (which == CASE_AFTER_SOURCE_REWRITE)
	{
		memset(&appender, 0, sizeof(appender));
		appender.keys = keys;
		appender.ledger = ledger;
		if (pthread_mutex_init(&appender.mutex, NULL) != 0 ||
			pthread_cond_init(&appender.cond, NULL) != 0)
			_exit(2);
		ps_test_set_admission_read_hook(admission_read_hook, &appender);
		ps_test_set_admission_write_lock_hook(admission_write_lock_hook,
										  &appender);
		if (pthread_create(&thread, NULL, concurrent_appender, &appender) != 0)
			_exit(2);
		thread_started = 1;
		if (pthread_create(&coordinator, NULL, admission_coordinator, &appender) != 0)
			_exit(2);
		coordinator_started = 1;
		(void) pthread_mutex_lock(&appender.mutex);
		while (!appender.admission_entered && !appender.operation_failed)
			(void) pthread_cond_wait(&appender.cond, &appender.mutex);
		if (!appender.admission_entered || appender.operation_failed)
		{
			(void) pthread_mutex_unlock(&appender.mutex);
			_exit(2);
		}
		(void) pthread_mutex_unlock(&appender.mutex);
	}
	if (snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store) < 0 ||
		!arm_fault(fault_dir))
	{
		dprintf(STDERR_FILENO, "child arm failed case=%s\n", case_fault_name(which));
		_exit(2);
	}
	(void) ps_core_maintenance();
	if (thread_started)
	{
		(void) pthread_join(thread, NULL);
		if (coordinator_started)
			(void) pthread_join(coordinator, NULL);
		ps_test_set_admission_read_hook(NULL, NULL);
		ps_test_set_admission_write_lock_hook(NULL, NULL);
	}
	dprintf(STDERR_FILENO, "child maintenance returned case=%s\n", case_fault_name(which));
	_exit(3);
}

static int
report_matches(const char *fault_dir, const char *expected, pid_t expected_pid)
{
	char path[1600];
	char expected_line[512];
	char actual_line[512];
	struct stat st;
	int fd = -1;
	int n;
	ssize_t read_count;

	if (snprintf(path, sizeof(path), "%s/report.jsonl", fault_dir) < 0)
		return 0;
	n = snprintf(expected_line, sizeof(expected_line),
			 "{\"schema\":1,\"name\":\"%s\",\"action\":\"crash\","
			 "\"hit\":1,\"pid\":%ld}\n", expected, (long) expected_pid);
	if (n < 0 || (size_t) n >= sizeof(expected_line))
		return 0;
	fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size != n || (ssize_t) n !=
		(read_count = pread(fd, actual_line, (size_t) n, 0)))
	{
		if (fd >= 0)
			(void) close(fd);
		return 0;
	}
	if (close(fd) != 0)
		return 0;
	return memcmp(actual_line, expected_line, (size_t) n) == 0;
}

static int
verify_recovered(const char *store, CrashCase which, PsKey keys[MATRIX_KEYS],
				 const AckLedger *ledger,
				 const PreRecoveryEvidence *evidence)
{
	char snapshots[1600];
	char manifest[1800];
	PsForkmetaSnapshot selected = {
		.directory_fd = -1, .checkpoint_fd = -1, .tail_fd = -1
	};
	PsForkmetaSnapshotPrepared prepared;
	PsChannel reply;
	uint64_t generation = 0;
	int expected_generation = which == CASE_AFTER_PREPARE ? 0 :
		(which == CASE_AFTER_SNAPSHOT_GC ? 2 : 1);
	int selected_ok;

	selected_ok = selected_generation(store, &generation) == 0;
	check(selected_ok == (expected_generation != 0),
		  "recovery selects old-or-new complete snapshot generation");
	if (expected_generation != 0)
	{
		check(selected_ok && generation == (uint64_t) expected_generation,
			  "recovery selected the expected generation");
		if (selected_ok)
		{
			if (snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store) >= 0 &&
				ps_forkmeta_snapshot_open(&selected, snapshots) == 0)
			{
				check(selected.cutoff_lsn != 0 &&
					  selected.cutoff_admission_seq != 0,
					  "selected generation carries an exact cutoff");
				ps_forkmeta_snapshot_close(&selected);
			}
		}
	}
	else
	{
		check(snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store) >= 0 &&
			  access(snapshots, F_OK) == 0,
			  "prepare crash leaves snapshot directory available for recovery");
		check(snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1",
					 snapshots) >= 0 && access(manifest, F_OK) != 0 &&
			  errno == ENOENT,
			  "prepare recovery keeps the manifest unselected");
		check(evidence->prepared_generation != 0 &&
			  ps_forkmeta_snapshot_read_prepared(snapshots, &prepared) == 0 &&
			  ps_forkmeta_snapshot_prepared_generation(snapshots, &generation) == 0 &&
			  generation == 0 &&
			  generation_artifact_count(store,
									evidence->prepared_generation) == 0,
			  "startup safely clears the durable prepared intent and both unselected parts");
	}
	if (snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store) >= 0)
	{
		uint64_t prepared_generation = 0;

		check(ps_forkmeta_snapshot_read_prepared(snapshots, &prepared) == 0 &&
			  ps_forkmeta_snapshot_prepared_generation(snapshots,
												 &prepared_generation) == 0 &&
			  prepared_generation == 0,
			  "fresh reopen leaves no durable prepared intent");
	}
	check(source_epoch_valid(store, generation, expected_generation != 0),
		  expected_generation != 0 ?
		  "recovered source has a durable marker and complete suffix" :
		  "pre-commit recovery retains a complete old source epoch");
	if (which == CASE_AFTER_SNAPSHOT_GC)
	{
		check(generation_artifact_count(store, 1) == 0,
			  "restart leaves every generation-one artifact retired");
		check(generation_artifact_count(store, 2) == 2,
			  "restart keeps both selected generation-two immutable parts");
	}
	for (unsigned int i = 0; i < MATRIX_KEYS; i++)
	{
		check(meta_request(PS_OP_EXISTS, &keys[i], 0, 0, &reply) &&
			  reply.result == 1,
			  "cross-shard relation existence survives restart");
		{
			uint32_t expected_nblocks = which == CASE_AFTER_SNAPSHOT_GC ? 2 : 1;

			if (!meta_request(PS_OP_NBLOCKS, &keys[i], 0, 0, &reply) ||
				(which == CASE_AFTER_SNAPSHOT_GC ?
				 reply.result != expected_nblocks : reply.result < expected_nblocks))
			{
				dprintf(STDERR_FILENO, "size failure case=%d key=%u shard=%u status=%u result=%u expected=%u\n",
						(int) which, i, ps_shard_of(&keys[i]), reply.status,
						reply.result, expected_nblocks);
				check(0, which == CASE_AFTER_SNAPSHOT_GC ?
					  "GC recovery restores every grown relation at exactly two blocks" :
					  "cross-shard relation size does not roll back");
			}
			else
				check(1, which == CASE_AFTER_SNAPSHOT_GC ?
					  "GC recovery restores every grown relation at exactly two blocks" :
					  "cross-shard relation size does not roll back");
		}
	}
	if (which == CASE_AFTER_SOURCE_REWRITE)
	{
		uint32_t ack_count = __atomic_load_n(&ledger->count, __ATOMIC_ACQUIRE);

		check(__atomic_load_n(&ledger->overlap_observed, __ATOMIC_ACQUIRE) == 1,
			  "maintenance entered the real blocking wrlock behind admitted writer");
		check(ack_count != 0 && ack_count <= ACK_CAPACITY,
			  "concurrent appender recorded every acknowledged append");
		for (uint32_t i = 0; i < ack_count && i < ACK_CAPACITY; i++)
		{
			const AckEntry *ack = &ledger->entries[i];

			check(ack->key_index < MATRIX_KEYS &&
				  meta_request(PS_OP_EXISTS, &keys[ack->key_index], 0, 0,
							   &reply) && reply.result == 1,
				  "acknowledged append relation exists after fresh recovery");
			check(ack->key_index < MATRIX_KEYS &&
				  meta_request(PS_OP_NBLOCKS, &keys[ack->key_index], 0, 0,
							   &reply) && reply.result >= ack->nblocks,
				  "acknowledged append size survives fresh recovery");
		}
	}
	close_runtime();
	return 1;
}

static int
run_case(CrashCase which)
{
	char store[] = "/tmp/psforkmetacrashXXXXXX";
	char fault_dir[1600];
	char snapshots[1600];
	PsKey keys[MATRIX_KEYS];
	PreRecoveryEvidence evidence;
	AckLedger *ledger = MAP_FAILED;
	pid_t pid;
	int status;
	int report_ok = 0;
	int ok;

	if (mkdtemp(store) == NULL)
		return 0;
	if (snprintf(fault_dir, sizeof(fault_dir), "%s.fault", store) < 0 ||
		mkdir(fault_dir, 0700) != 0)
	{
		remove_tree(store);
		return 0;
	}
	ledger = mmap(NULL, sizeof(*ledger), PROT_READ | PROT_WRITE,
				  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (ledger == MAP_FAILED)
	{
		remove_tree(fault_dir);
		remove_tree(store);
		return 0;
	}
	memset(ledger, 0, sizeof(*ledger));
	pid = fork();
	if (pid == 0)
		run_crashing_child(which, store, fault_dir, ledger);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == 88;
	check(ok, "fault child exits with process-abort status 88");
	report_ok = report_matches(fault_dir, case_fault_name(which), pid);
	check(report_ok,
		  "fault report has exactly one precise named hit");
	if (ok && report_ok)
	{
		verify_pre_recovery(store, which, &evidence);
		/* Reconstruct the deterministic key set before opening the recovered
		 * process.  The parent never inherits an open core from the child. */
		check(find_matrix_keys(keys),
			  "matrix key discovery initializes 24 unique keys across four shards");
		if (which == CASE_AFTER_SOURCE_REWRITE)
			check(snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots",
					   store) >= 0 &&
				  source_epoch_matches_selected(store, snapshots,
									 evidence.selected_generation),
				  "source marker and future suffix match selected generation before reopen");
		/* Repeated maintenance needed to create two snapshot generations can
		 * independently publish a page-prune layer whose recovery belongs to the
		 * R2 crash matrix.  Keep this focused GC-retirement case on the POSIX
		 * segment read path; the other boundaries still exercise layer recovery. */
		if (which == CASE_AFTER_SNAPSHOT_GC)
			use_layers = 0;
		if (find_matrix_keys(keys) && ps_core_open(store) == 0)
			(void) verify_recovered(store, which, keys, ledger, &evidence);
		else
		{
			dprintf(STDERR_FILENO, "parent reopen failed case=%d store=%s errno=%d\n",
					(int) which, store, errno);
			check(0, "parent reopens the crashed child store");
			remove_tree(store);
		}
		use_layers = 1;
	}
	else
		remove_tree(store);
	if (ledger != MAP_FAILED)
		(void) munmap(ledger, sizeof(*ledger));
	if (access(fault_dir, F_OK) == 0)
		remove_tree(fault_dir);
	if (access(store, F_OK) == 0)
		remove_tree(store);
	return ok;
}

int
main(void)
{
	int ok = 1;

	page_size = 8192;
	segment_size = 1024 * 1024;
	flush_pages = 1;
	compact_layers = 0;
	segment_gc_enabled = 0;
	cache_pages = 0;
	ps_nshards = MATRIX_SHARDS;
	use_layers = 1;
	if (setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1) != 0)
		return 1;
	for (CrashCase which = CASE_AFTER_PREPARE;
		 which < CASE_COUNT; which++)
		if (!run_case(which))
			ok = 0;
	printf("pagestore_forkmeta_crash_matrix_test: %d checks, %d failed\n",
		   checks, failures);
	return !ok || failures != 0;
}
