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

#define FORKMETA_SNAPSHOT_MAGIC UINT32_C(0x4d534946) /* "FISM" */
#define FORKMETA_SNAPSHOT_VERSION 1
#define FORKMETA_SNAPSHOT_HEADER_BYTES 80
#define FORKMETA_SNAPSHOT_MANIFEST "forkmeta_manifest_v1"
#define FORKMETA_SNAPSHOT_PREPARED "forkmeta_prepared_v1"
#define FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX "forkmeta_checkpoint_v1_"
#define FORKMETA_SNAPSHOT_TAIL_PREFIX "forkmeta_tail_v1_"
#define VERIFY_BYTES (64u * 1024u)
#define IO_MAX_BYTES (UINT64_C(1024) * 1024 * 1024)

typedef struct SnapshotRecord
{
	uint64_t generation;
	uint64_t cutoff_lsn;
	uint64_t cutoff_admission_seq;
	PsForkmetaSnapshotPart checkpoint;
	PsForkmetaSnapshotPart tail;
} SnapshotRecord;

typedef struct SnapshotCrcCtx
{
	uint32_t crc;
	uint64_t len;
} SnapshotCrcCtx;

typedef struct SnapshotWriteCtx
{
	int fd;
	uint32_t crc;
	uint64_t len;
} SnapshotWriteCtx;

typedef struct SnapshotCompareCtx
{
	int fd;
	uint32_t crc;
	uint64_t offset;
} SnapshotCompareCtx;

static PsForkmetaSnapshotObservationTestHook observation_test_hook;
static void *observation_test_hook_arg;
static PsForkmetaSnapshotGcInspectionTestHook gc_inspection_test_hook;
static void *gc_inspection_test_hook_arg;

void
ps_test_set_forkmeta_snapshot_observation_hook(
		PsForkmetaSnapshotObservationTestHook hook, void *arg)
{
	observation_test_hook = hook;
	observation_test_hook_arg = arg;
}

void
ps_test_set_forkmeta_snapshot_gc_inspection_hook(
		PsForkmetaSnapshotGcInspectionTestHook hook, void *arg)
{
	gc_inspection_test_hook = hook;
	gc_inspection_test_hook_arg = arg;
}

static size_t
io_amount(uint64_t remaining)
{
	uint64_t limit = IO_MAX_BYTES;

	if ((uint64_t) SSIZE_MAX < limit)
		limit = (uint64_t) SSIZE_MAX;
	return (size_t) (remaining < limit ? remaining : limit);
}

static uint32_t
fnv1a(uint32_t hash, const void *data, size_t len)
{
	const unsigned char *bytes = data;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= bytes[i];
		hash *= UINT32_C(16777619);
	}
	return hash;
}

static uint32_t
get_le32(const unsigned char *p)
{
	return (uint32_t) p[0] | (uint32_t) p[1] << 8 |
		(uint32_t) p[2] << 16 | (uint32_t) p[3] << 24;
}

static uint64_t
get_le64(const unsigned char *p)
{
	return (uint64_t) get_le32(p) | (uint64_t) get_le32(p + 4) << 32;
}

static void
put_le32(unsigned char *p, uint32_t value)
{
	for (unsigned int i = 0; i < 4; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static void
put_le64(unsigned char *p, uint64_t value)
{
	for (unsigned int i = 0; i < 8; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static int
write_all(int fd, const void *data, uint64_t len)
{
	const unsigned char *bytes = data;
	uint64_t done = 0;

	while (done < len)
	{
		size_t amount = io_amount(len - done);
		ssize_t n = write(fd, bytes + done, amount);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		done += (uint64_t) n;
	}
	return 0;
}

static int
uint64_to_off_t(uint64_t value, off_t *result)
{
	off_t converted = (off_t) value;

	if (converted < 0 || (uint64_t) converted != value)
		return -1;
	*result = converted;
	return 0;
}

static int
file_range_valid(uint64_t offset, uint64_t len)
{
	off_t ignored;

	if (len == 0)
		return uint64_to_off_t(offset, &ignored);
	if (offset > UINT64_MAX - len)
		return -1;
	return uint64_to_off_t(offset, &ignored) == 0 &&
		uint64_to_off_t(offset + len, &ignored) == 0 ? 0 : -1;
}

static int
read_all_at(int fd, void *data, size_t len, uint64_t offset)
{
	unsigned char *bytes = data;
	size_t done = 0;

	if (file_range_valid(offset, len) != 0)
		return -1;
	while (done < len)
	{
		off_t file_offset;
		size_t amount = io_amount(len - done);
		ssize_t n;

		if (uint64_to_off_t(offset + done, &file_offset) != 0)
			return -1;
		n = pread(fd, bytes + done, amount, file_offset);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		done += (size_t) n;
	}
	return 0;
}

static int
open_directory(const char *directory, int create)
{
	int directory_fd;
	int parent_fd;

	if (directory == NULL || strlen(directory) >= PS_FORKMETA_SNAPSHOT_PATH_MAX)
	{
		errno = ENAMETOOLONG;
		return -1;
	}
	if (create && mkdir(directory, 0700) != 0 && errno != EEXIST)
		return -1;
	directory_fd = open(directory,
						O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (directory_fd < 0)
		return -1;
	parent_fd = openat(directory_fd, "..", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (parent_fd < 0 || fsync(parent_fd) != 0)
	{
		if (parent_fd >= 0)
			(void) close(parent_fd);
		(void) close(directory_fd);
		return -1;
	}
	if (close(parent_fd) != 0)
	{
		(void) close(directory_fd);
		return -1;
	}
	return directory_fd;
}

static int
input_emit(const PsForkmetaSnapshotInput *input,
		   PsForkmetaSnapshotConsume consume, void *consume_arg)
{
	if (input == NULL || consume == NULL ||
		(input->data != NULL && input->produce != NULL) ||
		(input->data == NULL && input->produce == NULL && input->len != 0) ||
		(input->produce == NULL && input->len > (uint64_t) SIZE_MAX) ||
		file_range_valid(0, input->len) != 0)
		return -1;
	if (input->produce != NULL)
		return input->produce(input->produce_arg, consume, consume_arg);
	return consume(consume_arg, input->data, (size_t) input->len);
}

static int
consume_crc(void *arg, const void *data, size_t len)
{
	SnapshotCrcCtx *ctx = arg;

	if (UINT64_MAX - ctx->len < len)
		return -1;
	ctx->crc = fnv1a(ctx->crc, data, len);
	ctx->len += len;
	return 0;
}

static int
consume_write(void *arg, const void *data, size_t len)
{
	SnapshotWriteCtx *ctx = arg;

	if (UINT64_MAX - ctx->len < len || write_all(ctx->fd, data, len) != 0)
		return -1;
	ctx->crc = fnv1a(ctx->crc, data, len);
	ctx->len += len;
	return 0;
}

static int
consume_compare(void *arg, const void *data, size_t len)
{
	SnapshotCompareCtx *ctx = arg;
	unsigned char buffer[VERIFY_BYTES];
	const unsigned char *expected = data;
	size_t done = 0;

	while (done < len)
	{
		size_t amount = len - done < sizeof(buffer) ? len - done : sizeof(buffer);

		if (read_all_at(ctx->fd, buffer, amount, ctx->offset) != 0 ||
			memcmp(buffer, expected + done, amount) != 0 ||
			UINT64_MAX - ctx->offset < amount)
			return -1;
		ctx->crc = fnv1a(ctx->crc, buffer, amount);
		ctx->offset += amount;
		done += amount;
	}
	return 0;
}

static int
measure_input(const PsForkmetaSnapshotInput *input,
			  PsForkmetaSnapshotPart *part)
{
	SnapshotCrcCtx ctx = {UINT32_C(2166136261), 0};

	if (input_emit(input, consume_crc, &ctx) != 0 || ctx.len != input->len)
		return -1;
	part->len = ctx.len;
	part->crc = ctx.crc;
	return 0;
}

static int
part_name(uint64_t generation, unsigned int part, char *name, size_t name_len)
{
	const char *prefix = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
		FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX : FORKMETA_SNAPSHOT_TAIL_PREFIX;
	int n;

	if (part > PS_FORKMETA_SNAPSHOT_TAIL)
		return -1;
	n = snprintf(name, name_len, "%s%020llu", prefix,
				 (unsigned long long) generation);
	return n < 0 || (size_t) n >= name_len ? -1 : 0;
}

static int
parse_generation_name(const char *name, const char *prefix,
					  uint64_t *generation_out)
{
	const size_t prefix_len = strlen(prefix);
	uint64_t generation = 0;

	if (strlen(name) != prefix_len + 20 ||
		memcmp(name, prefix, prefix_len) != 0)
		return -1;
	for (size_t i = prefix_len; i < prefix_len + 20; i++)
	{
		unsigned int digit;

		if (name[i] < '0' || name[i] > '9')
			return -1;
		digit = (unsigned int) (name[i] - '0');
		if (generation > (UINT64_MAX - digit) / 10)
			return -1;
		generation = generation * 10 + digit;
	}
	if (generation == 0)
		return -1;
	*generation_out = generation;
	return 0;
}

static int
parse_pid_attempt(const char *text)
{
	uint64_t pid_value = 0;
	uint64_t attempt = 0;
	const char *p = text;
	pid_t pid;

	if (*p < '0' || *p > '9')
		return -1;
	for (; *p >= '0' && *p <= '9'; p++)
	{
		unsigned int digit = (unsigned int) (*p - '0');

		if (pid_value > (UINT64_MAX - digit) / 10)
			return -1;
		pid_value = pid_value * 10 + digit;
	}
	if (*p++ != '.' || *p < '0' || *p > '9')
		return -1;
	for (; *p >= '0' && *p <= '9'; p++)
	{
		unsigned int digit = (unsigned int) (*p - '0');

		if (attempt > (UINT64_MAX - digit) / 10)
			return -1;
		attempt = attempt * 10 + digit;
	}
	if (*p != '\0' || pid_value == 0 || attempt >= 128 ||
		pid_value > (uint64_t) LONG_MAX)
		return -1;
	pid = (pid_t) pid_value;
	return pid > 0 && (uint64_t) (long) pid == pid_value ? 0 : -1;
}

static int
parse_temp_name(const char *name)
{
	const char *bases[] = {FORKMETA_SNAPSHOT_MANIFEST,
		FORKMETA_SNAPSHOT_PREPARED};
	const char *marker = strstr(name, ".tmp.");
	const char *part_prefixes[] = {FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
		FORKMETA_SNAPSHOT_TAIL_PREFIX};

	if (marker == NULL || strstr(marker + 5, ".tmp.") != NULL)
		return -1;
	for (size_t i = 0; i < sizeof(part_prefixes) / sizeof(part_prefixes[0]);
		 i++)
	{
		char base[128];
		size_t base_len = (size_t) (marker - name);

		if (base_len >= sizeof(base))
			return -1;
		memcpy(base, name, base_len);
		base[base_len] = '\0';
		if (parse_generation_name(base, part_prefixes[i],
								  &(uint64_t) {0}) == 0)
		{
			return parse_pid_attempt(marker + 5);
		}
	}
	for (size_t i = 0; i < sizeof(bases) / sizeof(bases[0]); i++)
	{
		size_t base_len = strlen(bases[i]);

		if ((size_t) (marker - name) != base_len ||
			memcmp(name, bases[i], base_len) != 0)
			continue;
		return parse_pid_attempt(marker + 5);
	}
	return -1;
}

static int
parse_part_temp_generation(const char *name, uint64_t *generation_out)
{
	const char *marker = strstr(name, ".tmp.");
	const char *prefixes[] = {FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
		FORKMETA_SNAPSHOT_TAIL_PREFIX};

	if (marker == NULL)
		return -1;
	for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++)
	{
		char base[128];
		size_t base_len = (size_t) (marker - name);

		if (base_len >= sizeof(base))
			return -1;
		memcpy(base, name, base_len);
		base[base_len] = '\0';
		if (parse_generation_name(base, prefixes[i], generation_out) == 0)
			return 0;
	}
	return -1;
}

static int
tuple_cmp(uint64_t lsn_a, uint64_t seq_a, uint64_t lsn_b, uint64_t seq_b)
{
	if (lsn_a != lsn_b)
		return lsn_a < lsn_b ? -1 : 1;
	if (seq_a != seq_b)
		return seq_a < seq_b ? -1 : 1;
	return 0;
}

static int
copy_directory(char *destination, const char *directory)
{
	int n = snprintf(destination, PS_FORKMETA_SNAPSHOT_PATH_MAX, "%s",
				 directory);

	return n < 0 || (size_t) n >= PS_FORKMETA_SNAPSHOT_PATH_MAX ? -1 : 0;
}

static void
unlink_temp_and_sync(int directory_fd, const char *temporary)
{
	if (temporary[0] != '\0' && unlinkat(directory_fd, temporary, 0) == 0)
		(void) fsync(directory_fd);
}

static void
record_from_prepared(SnapshotRecord *record,
					 const PsForkmetaSnapshotPrepared *prepared)
{
	record->generation = prepared->generation;
	record->cutoff_lsn = prepared->cutoff_lsn;
	record->cutoff_admission_seq = prepared->cutoff_admission_seq;
	record->checkpoint = prepared->checkpoint;
	record->tail = prepared->tail;
}

static void
encode_record(const SnapshotRecord *record, unsigned char encoded[80])
{
	memset(encoded, 0, FORKMETA_SNAPSHOT_HEADER_BYTES);
	put_le32(encoded + 0, FORKMETA_SNAPSHOT_MAGIC);
	put_le32(encoded + 4, FORKMETA_SNAPSHOT_VERSION);
	put_le32(encoded + 8, FORKMETA_SNAPSHOT_HEADER_BYTES);
	put_le64(encoded + 16, record->generation);
	put_le64(encoded + 24, record->cutoff_lsn);
	put_le64(encoded + 32, record->cutoff_admission_seq);
	put_le64(encoded + 40, record->checkpoint.len);
	put_le32(encoded + 48, record->checkpoint.crc);
	put_le64(encoded + 52, record->tail.len);
	put_le32(encoded + 60, record->tail.crc);
	put_le32(encoded + 64, 0);
	put_le32(encoded + 64, fnv1a(UINT32_C(2166136261), encoded,
									 FORKMETA_SNAPSHOT_HEADER_BYTES));
}

static int
open_validated_part(int directory_fd, unsigned int part, uint64_t generation,
					uint64_t len, uint32_t expected_crc, int *fd_out)
{
	unsigned char buffer[VERIFY_BYTES];
	char name[128];
	struct stat st;
	uint64_t done = 0;
	uint32_t crc = UINT32_C(2166136261);
	int fd = -1;
	int rc = -1;

	if (part_name(generation, part, name, sizeof(name)) != 0 ||
		(fd = openat(directory_fd, name,
					 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size != len)
		goto cleanup;
	while (done < len)
	{
		size_t amount = len - done < sizeof(buffer) ?
			(size_t) (len - done) : sizeof(buffer);

		if (read_all_at(fd, buffer, amount, done) != 0)
			goto cleanup;
		crc = fnv1a(crc, buffer, amount);
		done += amount;
	}
	if (crc == expected_crc)
	{
		if (fd_out != NULL)
		{
			*fd_out = fd;
			fd = -1;
		}
		rc = 0;
	}

cleanup:
	if (fd >= 0 && close(fd) != 0)
		rc = -1;
	return rc;
}

static int
published_part_valid(int directory_fd, unsigned int part, uint64_t generation,
					 uint64_t len, uint32_t expected_crc)
{
	return open_validated_part(directory_fd, part, generation, len,
						   expected_crc, NULL);
}

static int
published_part_matches(int directory_fd, unsigned int part,
					   uint64_t generation,
					   const PsForkmetaSnapshotInput *input,
					   const PsForkmetaSnapshotPart *expected)
{
	SnapshotCompareCtx ctx;
	char name[128];
	struct stat st;
	int fd = -1;
	int rc = -1;

	if (part_name(generation, part, name, sizeof(name)) != 0 ||
		(fd = openat(directory_fd, name,
					 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size != expected->len)
		goto cleanup;
	ctx.fd = fd;
	ctx.offset = 0;
	ctx.crc = UINT32_C(2166136261);
	if (input_emit(input, consume_compare, &ctx) != 0 ||
		ctx.offset != expected->len || ctx.crc != expected->crc)
		goto cleanup;
	rc = 0;

cleanup:
	if (fd >= 0 && close(fd) != 0)
		rc = -1;
	return rc;
}

static int
publish_part(int directory_fd, unsigned int part, uint64_t generation,
			 const PsForkmetaSnapshotInput *input,
			 const PsForkmetaSnapshotPart *expected, int *newly_linked)
{
	char final_name[128];
	char temporary[192] = {0};
	int fd = -1;
	int rc = -1;
	int linked = 0;

	if (newly_linked == NULL ||
		part_name(generation, part, final_name, sizeof(final_name)) != 0)
		return -1;
	*newly_linked = 0;
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%u",
					 final_name, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0)
		return -1;
	{
		SnapshotWriteCtx ctx = {fd, UINT32_C(2166136261), 0};

		if (input_emit(input, consume_write, &ctx) != 0 ||
			ctx.len != expected->len || ctx.crc != expected->crc ||
			fsync(fd) != 0)
			goto cleanup;
	}
	if (close(fd) != 0)
	{
		fd = -1;
		goto cleanup;
	}
	fd = -1;
	if (linkat(directory_fd, temporary, directory_fd, final_name, 0) == 0)
		linked = 1;
	else if (errno != EEXIST ||
			 published_part_matches(directory_fd, part, generation, input,
								expected) != 0)
		goto cleanup;
	if (unlinkat(directory_fd, temporary, 0) != 0 || fsync(directory_fd) != 0)
		goto cleanup;
	*newly_linked = linked;
	rc = 0;

cleanup:
	if (fd >= 0)
		(void) close(fd);
	unlink_temp_and_sync(directory_fd, temporary);
	if (rc != 0)
		*newly_linked = linked;
	return rc;
}

static int
read_record(int directory_fd, const char *directory, const char *filename,
			SnapshotRecord *record, int validate_parts)
{
	unsigned char encoded[FORKMETA_SNAPSHOT_HEADER_BYTES];
	struct stat st;
	uint32_t stored_crc;
	uint32_t actual_crc;
	int fd = -1;
	int rc = -1;

	fd = openat(directory_fd, filename,
				O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size != FORKMETA_SNAPSHOT_HEADER_BYTES ||
		read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
		get_le32(encoded + 0) != FORKMETA_SNAPSHOT_MAGIC ||
		get_le32(encoded + 4) != FORKMETA_SNAPSHOT_VERSION ||
		get_le32(encoded + 8) != FORKMETA_SNAPSHOT_HEADER_BYTES ||
		get_le32(encoded + 12) != 0 || get_le32(encoded + 68) != 0 ||
		get_le64(encoded + 72) != 0)
		goto cleanup;
	record->generation = get_le64(encoded + 16);
	record->cutoff_lsn = get_le64(encoded + 24);
	record->cutoff_admission_seq = get_le64(encoded + 32);
	record->checkpoint.len = get_le64(encoded + 40);
	record->checkpoint.crc = get_le32(encoded + 48);
	record->tail.len = get_le64(encoded + 52);
	record->tail.crc = get_le32(encoded + 60);
	stored_crc = get_le32(encoded + 64);
	put_le32(encoded + 64, 0);
	actual_crc = fnv1a(UINT32_C(2166136261), encoded, sizeof(encoded));
	if (record->generation == 0 || record->cutoff_lsn == 0 ||
		record->cutoff_admission_seq == 0 ||
		file_range_valid(0, record->checkpoint.len) != 0 ||
		file_range_valid(0, record->tail.len) != 0 || stored_crc != actual_crc ||
		directory == NULL || strlen(directory) >= PS_FORKMETA_SNAPSHOT_PATH_MAX)
		goto cleanup;
	if (validate_parts &&
		(published_part_valid(directory_fd, PS_FORKMETA_SNAPSHOT_CHECKPOINT,
						 record->generation, record->checkpoint.len,
						 record->checkpoint.crc) != 0 ||
		 published_part_valid(directory_fd, PS_FORKMETA_SNAPSHOT_TAIL,
						 record->generation, record->tail.len,
						 record->tail.crc) != 0))
		goto cleanup;
	rc = 0;

cleanup:
	if (fd >= 0 && close(fd) != 0)
		rc = -1;
	return rc;
}

static void
prepared_from_record(PsForkmetaSnapshotPrepared *prepared,
					 const char *directory, const SnapshotRecord *record)
{
	memset(prepared, 0, sizeof(*prepared));
	(void) copy_directory(prepared->directory, directory);
	prepared->generation = record->generation;
	prepared->cutoff_lsn = record->cutoff_lsn;
	prepared->cutoff_admission_seq = record->cutoff_admission_seq;
	prepared->checkpoint = record->checkpoint;
	prepared->tail = record->tail;
}

static int
prepared_equal(const PsForkmetaSnapshotPrepared *a,
			   const PsForkmetaSnapshotPrepared *b)
{
	return strcmp(a->directory, b->directory) == 0 &&
		a->generation == b->generation &&
		a->cutoff_lsn == b->cutoff_lsn &&
		a->cutoff_admission_seq == b->cutoff_admission_seq &&
		a->checkpoint.len == b->checkpoint.len &&
		a->checkpoint.crc == b->checkpoint.crc && a->tail.len == b->tail.len &&
		a->tail.crc == b->tail.crc;
}

static int
read_prepared_internal(int directory_fd, const char *directory,
					   PsForkmetaSnapshotPrepared *prepared, int validate_parts)
{
	SnapshotRecord record;

	if (read_record(directory_fd, directory, FORKMETA_SNAPSHOT_PREPARED,
					&record, validate_parts) != 0)
		return -1;
	prepared_from_record(prepared, directory, &record);
	return 0;
}

static int
read_prepared_metadata_if_exists(int directory_fd, const char *directory,
								PsForkmetaSnapshotPrepared *prepared,
								int *exists)
{
	struct stat st;

	*exists = 0;
	if (fstatat(directory_fd, FORKMETA_SNAPSHOT_PREPARED, &st,
				AT_SYMLINK_NOFOLLOW) != 0)
		return errno == ENOENT ? 0 : -1;
	if (!S_ISREG(st.st_mode) ||
		read_prepared_internal(directory_fd, directory, prepared, 0) != 0)
		return -1;
	*exists = 1;
	return 0;
}

static int
clear_prepared(int directory_fd)
{
	if (unlinkat(directory_fd, FORKMETA_SNAPSHOT_PREPARED, 0) != 0 &&
		errno != ENOENT)
		return -1;
	return fsync(directory_fd);
}

static int
publish_prepared(int directory_fd,
				const PsForkmetaSnapshotPrepared *prepared, int *newly_linked)
{
	SnapshotRecord record;
	unsigned char encoded[FORKMETA_SNAPSHOT_HEADER_BYTES];
	char temporary[160] = {0};
	int fd = -1;
	int linked = 0;

	if (newly_linked == NULL)
		return -1;
	*newly_linked = 0;

	if (faccessat(directory_fd, FORKMETA_SNAPSHOT_PREPARED, F_OK, 0) == 0)
	{
		PsForkmetaSnapshotPrepared existing;

		return read_prepared_internal(directory_fd, prepared->directory,
							  &existing, 1) == 0 &&
			prepared_equal(&existing, prepared) ? 0 : -1;
	}
	if (errno != ENOENT)
		return -1;
	record_from_prepared(&record, prepared);
	encode_record(&record, encoded);
	if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_PREPARED_BEFORE_CREATE") != NULL)
	{
		errno = EIO;
		return -1;
	}
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%u",
					 FORKMETA_SNAPSHOT_PREPARED, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0 || write_all(fd, encoded, sizeof(encoded)) != 0 ||
		fsync(fd) != 0)
		goto fail;
	if (close(fd) != 0)
	{
		fd = -1;
		goto fail;
	}
	fd = -1;
	if (linkat(directory_fd, temporary, directory_fd,
			FORKMETA_SNAPSHOT_PREPARED, 0) != 0)
	{
		if (errno != EEXIST)
			goto fail;
		{
			PsForkmetaSnapshotPrepared existing;

			if (read_prepared_internal(directory_fd, prepared->directory,
							  &existing, 1) != 0 ||
				!prepared_equal(&existing, prepared))
				goto fail;
		}
	}
	else
		linked = 1;
	if (unlinkat(directory_fd, temporary, 0) != 0 || fsync(directory_fd) != 0)
		goto fail;
	*newly_linked = linked;
	return 0;

fail:
	if (fd >= 0)
		(void) close(fd);
	unlink_temp_and_sync(directory_fd, temporary);
	*newly_linked = linked;
	return -1;
}

static int
publish_manifest(int directory_fd, const SnapshotRecord *record)
{
	unsigned char encoded[FORKMETA_SNAPSHOT_HEADER_BYTES];
	char temporary[160] = {0};
	struct stat st;
	int fd = -1;

	encode_record(record, encoded);
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%u",
					 FORKMETA_SNAPSHOT_MANIFEST, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0 || write_all(fd, encoded, sizeof(encoded)) != 0 ||
		fsync(fd) != 0)
		goto fail;
	if (close(fd) != 0)
	{
		fd = -1;
		goto fail;
	}
	fd = -1;
	if (fstatat(directory_fd, FORKMETA_SNAPSHOT_MANIFEST, &st,
				AT_SYMLINK_NOFOLLOW) == 0)
	{
		if (!S_ISREG(st.st_mode))
			goto fail;
	}
	else if (errno != ENOENT)
		goto fail;
	if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_BEFORE_RENAME") != NULL)
	{
		errno = EIO;
		goto fail;
	}
	if (renameat(directory_fd, temporary, directory_fd,
				 FORKMETA_SNAPSHOT_MANIFEST) != 0)
		goto fail;
	if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_MANIFEST_AFTER_RENAME") != NULL)
	{
		errno = EIO;
		goto fail;
	}
	if (fsync(directory_fd) != 0)
		goto fail;
	return 0;

fail:
	if (fd >= 0)
		(void) close(fd);
	unlink_temp_and_sync(directory_fd, temporary);
	return -1;
}

int
ps_forkmeta_snapshot_read_prepared(const char *directory,
							PsForkmetaSnapshotPrepared *prepared)
{
	int directory_fd;

	if (directory == NULL || prepared == NULL)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (faccessat(directory_fd, FORKMETA_SNAPSHOT_PREPARED, F_OK, 0) != 0)
	{
		int saved_errno = errno;

		(void) close(directory_fd);
		return saved_errno == ENOENT ? 0 : -1;
	}
	if (read_prepared_internal(directory_fd, directory, prepared, 1) != 0)
	{
		(void) close(directory_fd);
		return -1;
	}
	if (close(directory_fd) != 0)
		return -1;
	return 1;
}

int
ps_forkmeta_snapshot_prepared_generation(const char *directory,
									 uint64_t *generation_out)
{
	PsForkmetaSnapshotPrepared prepared;
	int rc;

	if (generation_out == NULL)
		return -1;
	rc = ps_forkmeta_snapshot_read_prepared(directory, &prepared);
	if (rc == 1)
		*generation_out = prepared.generation;
	return rc;
}

int
ps_forkmeta_snapshot_next_generation(const char *directory,
							 uint64_t selected_generation,
							 uint64_t *generation_out)
{
	struct dirent *entry;
	DIR *dir = NULL;
	int directory_fd;
	int scan_fd = -1;
	int intent_exists;
	PsForkmetaSnapshotPrepared durable_prepared;
	uint64_t highest = selected_generation;

	if (directory == NULL || generation_out == NULL ||
		selected_generation == UINT64_MAX)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
	{
		if (errno != ENOENT)
			return -1;
		*generation_out = selected_generation + 1;
		return 0;
	}
	if (read_prepared_metadata_if_exists(directory_fd, directory,
									 &durable_prepared, &intent_exists) != 0)
	{
		(void) close(directory_fd);
		return -1;
	}
	if (intent_exists && durable_prepared.generation > highest)
		highest = durable_prepared.generation;
	scan_fd = fcntl(directory_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
	{
		if (scan_fd >= 0)
			(void) close(scan_fd);
		(void) close(directory_fd);
		return -1;
	}
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		uint64_t generation;

		if (parse_generation_name(entry->d_name,
							 FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
							 &generation) == 0 ||
			parse_generation_name(entry->d_name, FORKMETA_SNAPSHOT_TAIL_PREFIX,
								  &generation) == 0)
		{
			if (generation > highest)
				highest = generation;
			continue;
		}
		if (parse_temp_name(entry->d_name) == 0 &&
			parse_part_temp_generation(entry->d_name, &generation) == 0 &&
			generation > highest)
			highest = generation;
	}
	{
		int scan_errno = errno;
		int close_rc = closedir(dir);
		int directory_close_rc = close(directory_fd);

		if (scan_errno != 0 || close_rc != 0 || directory_close_rc != 0 ||
			highest == UINT64_MAX)
			return -1;
	}
	*generation_out = highest + 1;
	return 0;
}

int
ps_forkmeta_snapshot_prepare(PsForkmetaSnapshotPrepared *prepared,
							 const char *directory, uint64_t generation,
							 uint64_t cutoff_lsn,
							 uint64_t cutoff_admission_seq,
							 const PsForkmetaSnapshotInput *checkpoint,
							 const PsForkmetaSnapshotInput *tail)
{
	PsForkmetaSnapshotPart parts[2];
	PsForkmetaSnapshot current;
	int directory_fd;
	int created[2] = {0, 0};
	int current_open = 0;
	int intent_created = 0;
	int need_parts = 1;

	if (prepared == NULL || directory == NULL || generation == 0 ||
		cutoff_lsn == 0 || cutoff_admission_seq == 0 || checkpoint == NULL ||
		tail == NULL || copy_directory(prepared->directory, directory) != 0 ||
		measure_input(checkpoint, &parts[0]) != 0 ||
		measure_input(tail, &parts[1]) != 0)
		return -1;
	directory_fd = open_directory(directory, 1);
	if (directory_fd < 0)
		return -1;
	if (faccessat(directory_fd, FORKMETA_SNAPSHOT_MANIFEST, F_OK, 0) == 0)
	{
		if (ps_forkmeta_snapshot_open(&current, directory) != 0)
			goto fail;
		current_open = 1;
		if (generation < current.generation ||
			(generation > current.generation &&
			 tuple_cmp(cutoff_lsn, cutoff_admission_seq, current.cutoff_lsn,
						 current.cutoff_admission_seq) < 0))
			goto fail;
		if (generation == current.generation)
		{
			int identical = current.cutoff_lsn == cutoff_lsn &&
				current.cutoff_admission_seq == cutoff_admission_seq &&
				current.checkpoint.len == parts[0].len &&
				current.checkpoint.crc == parts[0].crc &&
				current.tail.len == parts[1].len && current.tail.crc == parts[1].crc &&
				published_part_matches(current.directory_fd,
									 PS_FORKMETA_SNAPSHOT_CHECKPOINT, generation,
									 checkpoint, &parts[0]) == 0 &&
				published_part_matches(current.directory_fd, PS_FORKMETA_SNAPSHOT_TAIL,
									 generation, tail, &parts[1]) == 0;

			ps_forkmeta_snapshot_close(&current);
			current_open = 0;
			if (!identical)
				goto fail;
			need_parts = 0;
		}
		else
		{
			ps_forkmeta_snapshot_close(&current);
			current_open = 0;
		}
	}
	else if (errno != ENOENT)
		goto fail;
	if (need_parts)
	{
		if (publish_part(directory_fd, PS_FORKMETA_SNAPSHOT_CHECKPOINT,
						generation, checkpoint, &parts[0], &created[0]) != 0 ||
			publish_part(directory_fd, PS_FORKMETA_SNAPSHOT_TAIL, generation, tail,
						 &parts[1], &created[1]) != 0)
			goto fail;
	}
	prepared->generation = generation;
	prepared->cutoff_lsn = cutoff_lsn;
	prepared->cutoff_admission_seq = cutoff_admission_seq;
	prepared->checkpoint = parts[0];
	prepared->tail = parts[1];
	if (publish_prepared(directory_fd, prepared, &intent_created) != 0)
		goto fail;
	if (close(directory_fd) != 0)
		return -1;
	return 0;

fail:
	if (current_open)
		ps_forkmeta_snapshot_close(&current);
	if (intent_created)
		(void) clear_prepared(directory_fd);
	for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		char name[128];

		if (!created[part])
			continue;
		if (part_name(generation, part, name, sizeof(name)) == 0)
			(void) unlinkat(directory_fd, name, 0);
	}
	if (created[0] || created[1])
		(void) fsync(directory_fd);
	(void) close(directory_fd);
	return -1;
}

int
ps_forkmeta_snapshot_commit(const PsForkmetaSnapshotPrepared *prepared)
{
	PsForkmetaSnapshotPrepared durable_prepared;
	PsForkmetaSnapshot current;
	SnapshotRecord record;
	int directory_fd;
	int manifest_exists;

	if (prepared == NULL || prepared->directory[0] == '\0' ||
		prepared->generation == 0 || prepared->cutoff_lsn == 0 ||
		prepared->cutoff_admission_seq == 0)
		return -1;
	directory_fd = open_directory(prepared->directory, 0);
	if (directory_fd < 0)
		return -1;
	if (faccessat(directory_fd, FORKMETA_SNAPSHOT_PREPARED, F_OK, 0) != 0)
	{
		if (errno == ENOENT)
		{
			if (ps_forkmeta_snapshot_open(&current, prepared->directory) == 0)
			{
				int identical = current.generation == prepared->generation &&
					current.cutoff_lsn == prepared->cutoff_lsn &&
					current.cutoff_admission_seq ==
					prepared->cutoff_admission_seq &&
					current.checkpoint.len == prepared->checkpoint.len &&
					current.checkpoint.crc == prepared->checkpoint.crc &&
					current.tail.len == prepared->tail.len &&
					current.tail.crc == prepared->tail.crc;

				ps_forkmeta_snapshot_close(&current);
				if (identical && close(directory_fd) == 0)
					return 0;
			}
		}
		(void) close(directory_fd);
		return -1;
	}
	if (read_prepared_internal(directory_fd, prepared->directory,
							&durable_prepared, 1) != 0 ||
		!prepared_equal(prepared, &durable_prepared))
	{
		(void) close(directory_fd);
		return -1;
	}
	manifest_exists = faccessat(directory_fd, FORKMETA_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
		goto fail;
	if (manifest_exists)
	{
		if (ps_forkmeta_snapshot_open(&current, prepared->directory) != 0)
			goto fail;
		if (prepared->generation < current.generation ||
			(prepared->generation > current.generation &&
			 tuple_cmp(prepared->cutoff_lsn, prepared->cutoff_admission_seq,
						 current.cutoff_lsn,
						 current.cutoff_admission_seq) < 0))
		{
			ps_forkmeta_snapshot_close(&current);
			goto fail;
		}
		if (prepared->generation == current.generation)
		{
			int identical = current.cutoff_lsn == prepared->cutoff_lsn &&
				current.cutoff_admission_seq == prepared->cutoff_admission_seq &&
				current.checkpoint.len == prepared->checkpoint.len &&
				current.checkpoint.crc == prepared->checkpoint.crc &&
				current.tail.len == prepared->tail.len &&
				current.tail.crc == prepared->tail.crc;

			ps_forkmeta_snapshot_close(&current);
			if (!identical || clear_prepared(directory_fd) != 0)
				goto fail;
			(void) close(directory_fd);
			return 0;
		}
		ps_forkmeta_snapshot_close(&current);
	}
	record_from_prepared(&record, prepared);
	if (publish_manifest(directory_fd, &record) != 0 ||
		clear_prepared(directory_fd) != 0)
		goto fail;
	if (close(directory_fd) != 0)
		return -1;
	return 0;

fail:
	(void) close(directory_fd);
	return -1;
}

static int
remove_generation_files(int directory_fd, uint64_t generation)
{
	int rc = 0;

	for (unsigned int part = 0; part <= PS_FORKMETA_SNAPSHOT_TAIL; part++)
	{
		char name[128];

		if (part_name(generation, part, name, sizeof(name)) != 0 ||
			(unlinkat(directory_fd, name, 0) != 0 && errno != ENOENT))
			rc = -1;
	}
	if (fsync(directory_fd) != 0)
		rc = -1;
	return rc;
}

static int
snapshot_gc_entry_regular(int directory_fd, const char *name)
{
	struct stat st;

	if (fstatat(directory_fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0)
		return errno == ENOENT ? 0 : -1;
	return S_ISREG(st.st_mode) ? 1 : -1;
}

int
ps_forkmeta_snapshot_abort(const PsForkmetaSnapshotPrepared *prepared)
{
	PsForkmetaSnapshotPrepared durable_prepared;
	PsForkmetaSnapshot current;
	int directory_fd;
	int intent_exists;
	int manifest_exists;

	if (prepared == NULL || prepared->directory[0] == '\0' ||
		prepared->generation == 0)
		return -1;
	directory_fd = open_directory(prepared->directory, 0);
	if (directory_fd < 0)
		return -1;
	if (read_prepared_metadata_if_exists(directory_fd, prepared->directory,
									 &durable_prepared, &intent_exists) != 0 ||
		(intent_exists && !prepared_equal(prepared, &durable_prepared)))
		goto fail;
	manifest_exists = faccessat(directory_fd, FORKMETA_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
		goto fail;
	if (manifest_exists)
	{
		if (ps_forkmeta_snapshot_open(&current, prepared->directory) != 0)
			goto fail;
		if (current.generation == prepared->generation)
		{
			ps_forkmeta_snapshot_close(&current);
			goto fail;
		}
		ps_forkmeta_snapshot_close(&current);
	}
	if (remove_generation_files(directory_fd, prepared->generation) != 0 ||
		(intent_exists && clear_prepared(directory_fd) != 0))
		goto fail;
	if (close(directory_fd) != 0)
		return -1;
	return 0;

fail:
	(void) close(directory_fd);
	return -1;
}

int
ps_forkmeta_snapshot_recover_prepared(const char *directory,
							  uint64_t durable_lsn,
							  uint64_t durable_admission_seq)
{
	PsForkmetaSnapshotPrepared prepared;
	PsForkmetaSnapshotPrepared validated;
	PsForkmetaSnapshot current;
	int directory_fd;
	int intent_exists;
	int manifest_exists;
	int retain = 0;

	if (directory == NULL || durable_lsn == 0 || durable_admission_seq == 0)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (read_prepared_metadata_if_exists(directory_fd, directory, &prepared,
									 &intent_exists) != 0)
	{
		(void) close(directory_fd);
		return -1;
	}
	if (!intent_exists)
	{
		(void) close(directory_fd);
		return 0;
	}
	manifest_exists = faccessat(directory_fd, FORKMETA_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
	{
		(void) close(directory_fd);
		return -1;
	}
	if (close(directory_fd) != 0)
		return -1;
	if (manifest_exists)
	{
		if (ps_forkmeta_snapshot_open(&current, directory) != 0)
			return -1;
		if (current.generation == prepared.generation)
		{
			int identical = current.cutoff_lsn == prepared.cutoff_lsn &&
				current.cutoff_admission_seq == prepared.cutoff_admission_seq &&
				current.checkpoint.len == prepared.checkpoint.len &&
				current.checkpoint.crc == prepared.checkpoint.crc &&
				current.tail.len == prepared.tail.len &&
				current.tail.crc == prepared.tail.crc;

			ps_forkmeta_snapshot_close(&current);
			return identical ? ps_forkmeta_snapshot_commit(&prepared) : -1;
		}
		if (current.generation > prepared.generation ||
			tuple_cmp(prepared.cutoff_lsn, prepared.cutoff_admission_seq,
					  current.cutoff_lsn, current.cutoff_admission_seq) <= 0)
		{
			ps_forkmeta_snapshot_close(&current);
			return ps_forkmeta_snapshot_abort(&prepared);
		}
		ps_forkmeta_snapshot_close(&current);
	}
	retain = tuple_cmp(durable_lsn, durable_admission_seq,
					   prepared.cutoff_lsn,
					   prepared.cutoff_admission_seq) >= 0;
	if (!retain)
		return ps_forkmeta_snapshot_abort(&prepared);
	return ps_forkmeta_snapshot_read_prepared(directory, &validated) == 1 &&
		prepared_equal(&prepared, &validated) ? 0 : -1;
}

int
ps_forkmeta_snapshot_publish(const char *directory, uint64_t generation,
							 uint64_t cutoff_lsn,
							 uint64_t cutoff_admission_seq,
							 const PsForkmetaSnapshotInput *checkpoint,
							 const PsForkmetaSnapshotInput *tail)
{
	PsForkmetaSnapshotPrepared prepared;

	memset(&prepared, 0, sizeof(prepared));

	if (ps_forkmeta_snapshot_prepare(&prepared, directory, generation,
									 cutoff_lsn, cutoff_admission_seq, checkpoint,
									 tail) != 0)
		return -1;
	return ps_forkmeta_snapshot_commit(&prepared);
}

int
ps_forkmeta_snapshot_discard_generation(const char *directory,
									uint64_t generation)
{
	PsForkmetaSnapshotPrepared durable_prepared;
	PsForkmetaSnapshot current;
	int directory_fd;
	int intent_exists;
	int manifest_exists;

	if (directory == NULL || generation == 0)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (read_prepared_metadata_if_exists(directory_fd, directory,
									 &durable_prepared, &intent_exists) != 0)
		goto fail;
	manifest_exists = faccessat(directory_fd, FORKMETA_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
		goto fail;
	if (manifest_exists)
	{
		if (ps_forkmeta_snapshot_open(&current, directory) != 0)
			goto fail;
		if (current.generation == generation)
		{
			ps_forkmeta_snapshot_close(&current);
			(void) close(directory_fd);
			return 1;
		}
		ps_forkmeta_snapshot_close(&current);
	}
	if (remove_generation_files(directory_fd, generation) != 0 ||
		(intent_exists && durable_prepared.generation == generation &&
		 clear_prepared(directory_fd) != 0))
		goto fail;
	if (close(directory_fd) != 0)
		return -1;
	return 0;

fail:
	(void) close(directory_fd);
	return -1;
}

int
ps_forkmeta_snapshot_open(PsForkmetaSnapshot *snapshot, const char *directory)
{
	SnapshotRecord record;
	int directory_fd;

	if (snapshot == NULL || directory == NULL)
		return -1;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->directory_fd = -1;
	snapshot->checkpoint_fd = -1;
	snapshot->tail_fd = -1;
	if (copy_directory(snapshot->directory, directory) != 0)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return -1;
	if (read_record(directory_fd, directory, FORKMETA_SNAPSHOT_MANIFEST,
					&record, 0) != 0)
	{
		(void) close(directory_fd);
		return -1;
	}
	snapshot->directory_fd = directory_fd;
	snapshot->generation = record.generation;
	snapshot->cutoff_lsn = record.cutoff_lsn;
	snapshot->cutoff_admission_seq = record.cutoff_admission_seq;
	snapshot->checkpoint = record.checkpoint;
	snapshot->tail = record.tail;
	if (open_validated_part(directory_fd, PS_FORKMETA_SNAPSHOT_CHECKPOINT,
						 record.generation, record.checkpoint.len,
						 record.checkpoint.crc, &snapshot->checkpoint_fd) != 0 ||
		open_validated_part(directory_fd, PS_FORKMETA_SNAPSHOT_TAIL,
						 record.generation, record.tail.len, record.tail.crc,
						 &snapshot->tail_fd) != 0)
	{
		ps_forkmeta_snapshot_close(snapshot);
		return -1;
	}
	return 0;
}

int
ps_forkmeta_snapshot_read(const PsForkmetaSnapshot *snapshot, unsigned int part,
						  uint64_t offset, void *data, uint64_t len)
{
	const PsForkmetaSnapshotPart *metadata;
	int fd;

	if (snapshot == NULL || snapshot->directory_fd < 0 ||
		part > PS_FORKMETA_SNAPSHOT_TAIL || (len != 0 && data == NULL))
		return -1;
	metadata = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ? &snapshot->checkpoint :
		&snapshot->tail;
	fd = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ? snapshot->checkpoint_fd :
		snapshot->tail_fd;
	if (offset > metadata->len || len > metadata->len - offset ||
		len > (uint64_t) SIZE_MAX || fd < 0)
		return -1;
	if (len == 0)
		return 0;
	return read_all_at(fd, data, (size_t) len, offset);
}

int
ps_forkmeta_snapshot_read_checkpoint(const PsForkmetaSnapshot *snapshot,
							 uint64_t offset, void *data, uint64_t len)
{
	return ps_forkmeta_snapshot_read(snapshot, PS_FORKMETA_SNAPSHOT_CHECKPOINT,
							 offset, data, len);
}

int
ps_forkmeta_snapshot_read_tail(const PsForkmetaSnapshot *snapshot,
					 uint64_t offset, void *data, uint64_t len)
{
	return ps_forkmeta_snapshot_read(snapshot, PS_FORKMETA_SNAPSHOT_TAIL,
							 offset, data, len);
}

int
ps_forkmeta_snapshot_gc(const char *directory)
{
	PsForkmetaSnapshotPrepared durable_prepared;
	PsForkmetaSnapshot current;
	struct dirent *entry;
	char (*names)[256] = NULL;
	DIR *dir = NULL;
	int scan_fd = -1;
	int rc = -1;
	int intent_exists;
	int unlink_failed = 0;
	int removed = 0;
	size_t count = 0;
	size_t capacity = 0;

	if (ps_forkmeta_snapshot_open(&current, directory) != 0)
		return -1;
	if (read_prepared_metadata_if_exists(current.directory_fd, directory,
									 &durable_prepared, &intent_exists) != 0)
		goto cleanup;
	scan_fd = fcntl(current.directory_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		uint64_t generation;
		int temp_status = parse_temp_name(entry->d_name);
		int recognized = temp_status == 0;

		if (!recognized)
		{
			int checkpoint_status =
				strncmp(entry->d_name, FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
						strlen(FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX)) == 0 ?
				parse_generation_name(entry->d_name,
										 FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
										 &generation) : 1;
			int tail_status =
				strncmp(entry->d_name, FORKMETA_SNAPSHOT_TAIL_PREFIX,
						strlen(FORKMETA_SNAPSHOT_TAIL_PREFIX)) == 0 ?
				parse_generation_name(entry->d_name,
										 FORKMETA_SNAPSHOT_TAIL_PREFIX, &generation) : 1;

			if ((strstr(entry->d_name, ".tmp.") != NULL && temp_status != 0) ||
				checkpoint_status < 0 || tail_status < 0)
				goto cleanup;
			recognized = checkpoint_status == 0 || tail_status == 0;
			if (!recognized || generation >= current.generation ||
				(intent_exists && generation == durable_prepared.generation))
				continue;
		}
		if (count == capacity)
		{
			size_t next = capacity == 0 ? 16 : capacity * 2;
			void *grown;

			if (next < capacity || next > SIZE_MAX / sizeof(*names))
				goto cleanup;
			grown = realloc(names, next * sizeof(*names));
			if (grown == NULL)
				goto cleanup;
			names = grown;
			capacity = next;
		}
		if (strlen(entry->d_name) >= sizeof(names[0]))
			goto cleanup;
		memcpy(names[count++], entry->d_name, strlen(entry->d_name) + 1);
	}
	{
		int scan_errno = errno;
		int close_rc = closedir(dir);

		/* closedir consumes the stream even when it reports an error. */
		dir = NULL;
		/* Test-only fault injection preserves the real closedir call, then
		 * exercises the error cleanup path without leaving a live stream. */
		if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR") != NULL)
			close_rc = -1;
		if (close_rc != 0 || scan_errno != 0)
			goto cleanup;
	}
	for (size_t i = 0; i < count; i++)
	{
		int regular = snapshot_gc_entry_regular(current.directory_fd, names[i]);

		if (regular < 0)
		{
			unlink_failed = 1;
			continue;
		}
		if (regular == 0)
			continue;
		if (unlinkat(current.directory_fd, names[i], 0) != 0)
		{
			if (errno != ENOENT)
				unlink_failed = 1;
		}
		else
			removed = 1;
	}
	if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") != NULL)
	{
		errno = EIO;
		if (removed)
			rc = PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS;
		goto cleanup;
	}
	/* Always sync: an empty retry closes an ambiguous prior unlink fsync. */
	if (fsync(current.directory_fd) != 0)
	{
		unlink_failed = 1;
		if (removed)
			rc = PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS;
	}
	if (!unlink_failed)
		rc = removed ? 1 : 0;

cleanup:
	free(names);
	if (dir != NULL)
		(void) closedir(dir);
	else if (scan_fd >= 0)
		(void) close(scan_fd);
	ps_forkmeta_snapshot_close(&current);
	return rc;
}

#define FORKMETA_OBSERVATION_MAX_ENTRIES \
	PS_FORKMETA_SNAPSHOT_RECLAIM_MAX_ENTRIES
#define FORKMETA_OBSERVATION_RETRIES 3U
#define FORKMETA_TEMP_GC_BATCH PS_FORKMETA_SNAPSHOT_TEMP_GC_BATCH

typedef struct ForkmetaObservationIdentity
{
	int present;
	dev_t dev;
	ino_t ino;
	off_t size;
	time_t mtime_sec;
	long mtime_nsec;
	time_t ctime_sec;
	long ctime_nsec;
} ForkmetaObservationIdentity;

typedef struct ForkmetaTempGcCursor
{
	int valid;
	char directory[PS_FORKMETA_SNAPSHOT_PATH_MAX];
	dev_t dev;
	ino_t ino;
	long offset;
} ForkmetaTempGcCursor;

static ForkmetaTempGcCursor forkmeta_temp_gc_cursor;

static void
forkmeta_temp_gc_cursor_reset(void)
{
	memset(&forkmeta_temp_gc_cursor, 0, sizeof(forkmeta_temp_gc_cursor));
}

static int
forkmeta_temp_gc_cursor_prepare(const char *directory, int directory_fd,
								DIR *dir)
{
	struct stat st;
	int same_directory;

	if (fstat(directory_fd, &st) != 0 || !S_ISDIR(st.st_mode) ||
		strlen(directory) >= sizeof(forkmeta_temp_gc_cursor.directory))
	{
		forkmeta_temp_gc_cursor_reset();
		return -1;
	}
	same_directory = forkmeta_temp_gc_cursor.valid &&
		strcmp(forkmeta_temp_gc_cursor.directory, directory) == 0 &&
		forkmeta_temp_gc_cursor.dev == st.st_dev &&
		forkmeta_temp_gc_cursor.ino == st.st_ino;
	if (!same_directory)
		forkmeta_temp_gc_cursor_reset();
	else
		seekdir(dir, forkmeta_temp_gc_cursor.offset);
	if (!forkmeta_temp_gc_cursor.valid)
	{
		memset(&forkmeta_temp_gc_cursor, 0, sizeof(forkmeta_temp_gc_cursor));
		memcpy(forkmeta_temp_gc_cursor.directory, directory, strlen(directory) + 1);
		forkmeta_temp_gc_cursor.dev = st.st_dev;
		forkmeta_temp_gc_cursor.ino = st.st_ino;
		forkmeta_temp_gc_cursor.valid = 1;
	}
	return 0;
}

static int
forkmeta_identity_equal(const ForkmetaObservationIdentity *left,
						const ForkmetaObservationIdentity *right)
{
	return left->present == right->present &&
		(!left->present ||
		 (left->dev == right->dev && left->ino == right->ino &&
		  left->size == right->size && left->mtime_sec == right->mtime_sec &&
		  left->mtime_nsec == right->mtime_nsec &&
		  left->ctime_sec == right->ctime_sec &&
		  left->ctime_nsec == right->ctime_nsec));
}

static int
forkmeta_identity_at(int directory_fd, const char *name,
					 ForkmetaObservationIdentity *identity)
{
	struct stat st;

	memset(identity, 0, sizeof(*identity));
	if (fstatat(directory_fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0)
		return errno == ENOENT ? 0 : -1;
	if (!S_ISREG(st.st_mode) || st.st_size < 0)
		return -1;
	identity->present = 1;
	identity->dev = st.st_dev;
	identity->ino = st.st_ino;
	identity->size = st.st_size;
	identity->mtime_sec = st.st_mtim.tv_sec;
	identity->mtime_nsec = st.st_mtim.tv_nsec;
	identity->ctime_sec = st.st_ctim.tv_sec;
	identity->ctime_nsec = st.st_ctim.tv_nsec;
	return 0;
}

static int
forkmeta_directory_identity(int directory_fd,
						 ForkmetaObservationIdentity *identity)
{
	struct stat st;

	memset(identity, 0, sizeof(*identity));
	if (fstat(directory_fd, &st) != 0 || !S_ISDIR(st.st_mode))
		return -1;
	identity->present = 1;
	identity->dev = st.st_dev;
	identity->ino = st.st_ino;
	identity->mtime_sec = st.st_mtim.tv_sec;
	identity->mtime_nsec = st.st_mtim.tv_nsec;
	identity->ctime_sec = st.st_ctim.tv_sec;
	identity->ctime_nsec = st.st_ctim.tv_nsec;
	return 0;
}

static int
forkmeta_directory_path_identity(const char *path,
						  ForkmetaObservationIdentity *identity)
{
	int fd;
	int rc;

	memset(identity, 0, sizeof(*identity));
	fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (fd < 0)
		return errno == ENOENT ? 0 : -1;
	rc = forkmeta_directory_identity(fd, identity);
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

static int
forkmeta_record_equal_expected(const SnapshotRecord *record,
						   const PsForkmetaSnapshotExpected *expected)
{
	return record->generation == expected->generation &&
		record->cutoff_lsn == expected->cutoff_lsn &&
		record->cutoff_admission_seq == expected->cutoff_admission_seq &&
		record->checkpoint.len == expected->checkpoint.len &&
		record->checkpoint.crc == expected->checkpoint.crc &&
		record->tail.len == expected->tail.len &&
			record->tail.crc == expected->tail.crc;
}

static int
forkmeta_record_equal(const SnapshotRecord *left, const SnapshotRecord *right)
{
	return left->generation == right->generation &&
		left->cutoff_lsn == right->cutoff_lsn &&
		left->cutoff_admission_seq == right->cutoff_admission_seq &&
		left->checkpoint.len == right->checkpoint.len &&
		left->checkpoint.crc == right->checkpoint.crc &&
		left->tail.len == right->tail.len &&
		left->tail.crc == right->tail.crc;
}

static int
forkmeta_read_record_metadata(int directory_fd, const char *directory,
							 const char *name, SnapshotRecord *record,
							 ForkmetaObservationIdentity *identity)
{
	if (forkmeta_identity_at(directory_fd, name, identity) != 0 ||
		!identity->present || read_record(directory_fd, directory, name,
										 record, 0) != 0)
		return -1;
	return 0;
}

static int
forkmeta_read_optional_record(int directory_fd, const char *directory,
							 const char *name, SnapshotRecord *record,
							 ForkmetaObservationIdentity *identity,
							 int *present)
{
	if (forkmeta_identity_at(directory_fd, name, identity) != 0)
		return -1;
	if (!identity->present)
	{
		*present = 0;
		return 0;
	}
	if (read_record(directory_fd, directory, name, record, 0) != 0)
		return -1;
	*present = 1;
	return 0;
}

static int
forkmeta_snapshot_parts_identity(int directory_fd, const SnapshotRecord *record,
							 ForkmetaObservationIdentity identities[2])
{
	for (unsigned int part = 0; part < 2; part++)
	{
		char name[128];
		uint64_t expected_len = part == PS_FORKMETA_SNAPSHOT_CHECKPOINT ?
			record->checkpoint.len : record->tail.len;

		if (part_name(record->generation, part, name, sizeof(name)) != 0 ||
			forkmeta_identity_at(directory_fd, name, &identities[part]) != 0 ||
			!identities[part].present ||
			(uint64_t) identities[part].size != expected_len)
			return -1;
	}
	return 0;
}

static int
forkmeta_add_bytes(uint64_t *total, uint64_t bytes)
{
	if (UINT64_MAX - *total < bytes)
		*total = UINT64_MAX;
	else
		*total += bytes;
	return 0;
}

static int
forkmeta_snapshot_debt_scan(int directory_fd, uint64_t selected_generation,
												uint64_t prepared_generation,
												uint64_t *gc_debt_out,
											uint64_t *gc_temp_out,
											uint64_t *gc_canonical_out,
											uint64_t *cutoff_debt_out,
											int *gc_overflow_out,
											int *gc_temp_gc_due_out)
{
	struct dirent *entry;
	DIR *dir = NULL;
	int scan_fd = -1;
	uint64_t gc_total = 0;
	uint64_t gc_temp = 0;
	uint64_t gc_canonical = 0;
	uint64_t cutoff_total = 0;
	unsigned int nentries = 0;
	unsigned char selected_seen[2] = {0, 0};
	unsigned char prepared_seen[2] = {0, 0};
	int rc = -1;
	int gc_overflow = 0;
	int gc_temp_gc_due = 0;

	scan_fd = fcntl(directory_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		uint64_t generation = 0;
		uint64_t bytes;
		int checkpoint_status;
		int tail_status;
		int is_canonical = 0;
		int is_temp;
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0 ||
			strcmp(entry->d_name, FORKMETA_SNAPSHOT_MANIFEST) == 0)
			continue;
		if (strcmp(entry->d_name, FORKMETA_SNAPSHOT_PREPARED) == 0)
		{
			if (fstatat(directory_fd, entry->d_name, &st,
							AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(st.st_mode) ||
				st.st_size < 0)
				goto cleanup;
			/* The prepared intent is retained until commit/abort resolves it;
			 * its bytes are not reclaimable debt.  Its identity and content are
			 * still checked by the caller around this scan. */
			continue;
		}
		checkpoint_status = strncmp(entry->d_name,
									FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
									strlen(FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX)) == 0 ?
			parse_generation_name(entry->d_name,
										FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX, &generation) : 1;
		tail_status = strncmp(entry->d_name, FORKMETA_SNAPSHOT_TAIL_PREFIX,
									 strlen(FORKMETA_SNAPSHOT_TAIL_PREFIX)) == 0 ?
			parse_generation_name(entry->d_name,
									 FORKMETA_SNAPSHOT_TAIL_PREFIX, &generation) : 1;
		is_temp = parse_temp_name(entry->d_name) == 0;
		if (checkpoint_status == 0 || tail_status == 0)
			is_canonical = 1;
		else if (!is_temp && (checkpoint_status < 0 || tail_status < 0))
		{
			if (strncmp(entry->d_name, FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
							strlen(FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX)) == 0 ||
				strncmp(entry->d_name, FORKMETA_SNAPSHOT_TAIL_PREFIX,
							strlen(FORKMETA_SNAPSHOT_TAIL_PREFIX)) == 0)
				goto cleanup;
		}
		if (nentries >= FORKMETA_OBSERVATION_MAX_ENTRIES)
		{
			/* Do not hold the admission fence while fstat'ing beyond the
			 * bounded prefix.  The incomplete result stays fail-closed and
			 * schedules an admission-fence-external exact-temp GC probe. */
			gc_overflow = 1;
			break;
		}
		if (!is_canonical && !is_temp)
			goto cleanup;
		nentries++;
		if (fstatat(directory_fd, entry->d_name, &st,
						 AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(st.st_mode) ||
			st.st_size < 0)
			goto cleanup;
		bytes = (uint64_t) st.st_size;
		if (is_temp)
			gc_temp_gc_due = 1;
		if (is_canonical && generation == selected_generation &&
			selected_generation != 0)
		{
			unsigned int part = checkpoint_status == 0 ?
				PS_FORKMETA_SNAPSHOT_CHECKPOINT : PS_FORKMETA_SNAPSHOT_TAIL;

			if (selected_seen[part])
				goto cleanup;
			selected_seen[part] = 1;
			if (prepared_generation == selected_generation)
				prepared_seen[part] = 1;
			continue;
		}
		if (is_canonical && prepared_generation != 0 &&
			generation == prepared_generation)
		{
			unsigned int part = checkpoint_status == 0 ?
				PS_FORKMETA_SNAPSHOT_CHECKPOINT : PS_FORKMETA_SNAPSHOT_TAIL;

			if (prepared_seen[part])
				goto cleanup;
			prepared_seen[part] = 1;
			continue;
		}
		/* Every recognized temp is owned by GC, including an orphaned prepared
		 * intent temp.  Canonical generations older than the selected authority
		 * are also immediately removable; all other canonical residue needs a
		 * newer safe snapshot/cutoff first. */
		if (is_temp)
		{
			forkmeta_add_bytes(&gc_total, bytes);
			forkmeta_add_bytes(&gc_temp, bytes);
		}
		else if (is_canonical && selected_generation != 0 &&
				 generation < selected_generation)
		{
			forkmeta_add_bytes(&gc_total, bytes);
			forkmeta_add_bytes(&gc_canonical, bytes);
		}
		else
			forkmeta_add_bytes(&cutoff_total, bytes);
	}
	{
		int scan_errno = errno;
		int close_rc = closedir(dir);

		/* closedir consumes the stream even when it reports an error. */
		dir = NULL;
		if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR") != NULL)
			close_rc = -1;
		if (scan_errno != 0 || close_rc != 0)
			goto cleanup;
	}
	if (!gc_overflow && selected_generation != 0 &&
		(!selected_seen[0] || !selected_seen[1]))
		goto cleanup;
	if (!gc_overflow && prepared_generation != 0 &&
		(!prepared_seen[0] || !prepared_seen[1]))
		goto cleanup;
	*gc_debt_out = gc_total;
	if (gc_temp_out != NULL)
		*gc_temp_out = gc_temp;
	if (gc_canonical_out != NULL)
		*gc_canonical_out = gc_canonical;
	*cutoff_debt_out = cutoff_total;
	if (gc_overflow_out != NULL)
		*gc_overflow_out = gc_overflow;
	if (gc_temp_gc_due_out != NULL)
		*gc_temp_gc_due_out = gc_temp_gc_due || gc_overflow;
	rc = gc_overflow ? PS_FORKMETA_SNAPSHOT_RECLAIM_OBSERVATION_OVERFLOW : 0;

cleanup:
	if (dir != NULL)
		(void) closedir(dir);
	else if (scan_fd >= 0)
		(void) close(scan_fd);
	return rc;
}

/* Remove only recognized temporary files.  This path is intentionally
 * independent of the selected manifest: an interrupted first publication can
 * leave executable debris before any manifest exists.  It scans at most a
 * bounded prefix per call and resumes from a validated directory cursor, so
 * canonical entries do not hide later temporary debris. */
PsForkmetaSnapshotGcResult
ps_forkmeta_snapshot_gc_temporary(const char *directory)
{
	struct dirent *entry;
	char names[FORKMETA_TEMP_GC_BATCH][256];
	DIR *dir = NULL;
	int directory_fd = -1;
	int scan_fd = -1;
	int removed = 0;
	int unlink_failed = 0;
	int rc = -1;
	int scan_incomplete = 0;
	int keep_cursor = 0;
	unsigned int inspected = 0;
	size_t count = 0;

	if (directory == NULL)
	{
		forkmeta_temp_gc_cursor_reset();
		return -1;
	}
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
	{
		forkmeta_temp_gc_cursor_reset();
		return errno == ENOENT ? 0 : -1;
	}
	scan_fd = fcntl(directory_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	if (forkmeta_temp_gc_cursor_prepare(directory, directory_fd, dir) != 0)
		goto cleanup;
	errno = 0;
	while (inspected < FORKMETA_TEMP_GC_BATCH && (entry = readdir(dir)) != NULL)
	{
		int temp_status;
		long offset;

		offset = telldir(dir);
		if (offset == -1)
			goto cleanup;
		forkmeta_temp_gc_cursor.offset = offset;
		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0 ||
			strcmp(entry->d_name, FORKMETA_SNAPSHOT_MANIFEST) == 0 ||
			strcmp(entry->d_name, FORKMETA_SNAPSHOT_PREPARED) == 0)
			continue;
		inspected++;
		if (gc_inspection_test_hook != NULL)
			gc_inspection_test_hook(gc_inspection_test_hook_arg);
		temp_status = parse_temp_name(entry->d_name);
		if (temp_status != 0)
		{
			/* A near-miss .tmp. name is not safe to ignore.  Canonical final
			 * parts and unrelated names are not owned by this cleanup path. */
			if (strstr(entry->d_name, ".tmp.") != NULL)
				goto cleanup;
			continue;
		}
		if (snapshot_gc_entry_regular(directory_fd, entry->d_name) != 1)
			goto cleanup;
		if (strlen(entry->d_name) >= sizeof(names[0]))
			goto cleanup;
		memcpy(names[count++], entry->d_name,
			   strlen(entry->d_name) + 1);
	}
	scan_incomplete = inspected == FORKMETA_TEMP_GC_BATCH;
	{
		int scan_errno = scan_incomplete ? 0 : errno;
		int close_rc = closedir(dir);

		dir = NULL;
		if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_CLOSEDIR") != NULL)
			close_rc = -1;
		if (scan_errno != 0 || close_rc != 0)
			goto cleanup;
	}
	keep_cursor = scan_incomplete;
	if (!keep_cursor)
		forkmeta_temp_gc_cursor_reset();
	for (size_t i = 0; i < count; i++)
	{
		int regular = snapshot_gc_entry_regular(directory_fd, names[i]);

		if (regular < 0)
		{
			unlink_failed = 1;
			continue;
		}
		if (regular == 0)
			continue;
		if (unlinkat(directory_fd, names[i], 0) != 0)
		{
			if (errno != ENOENT)
				unlink_failed = 1;
		}
		else
			removed = 1;
	}
	if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") != NULL)
	{
		keep_cursor = 0;
		if (removed)
			rc = PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS;
		goto cleanup;
	}
	if (fsync(directory_fd) != 0)
	{
		unlink_failed = 1;
		keep_cursor = 0;
		if (removed)
			rc = PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS;
	}
	if (!unlink_failed)
		rc = removed ? PS_FORKMETA_SNAPSHOT_GC_REMOVED :
		keep_cursor ? PS_FORKMETA_SNAPSHOT_GC_SCAN_INCOMPLETE :
		PS_FORKMETA_SNAPSHOT_GC_NO_WORK;

cleanup:
	if (rc < 0)
		keep_cursor = 0;
	if (dir != NULL)
		(void) closedir(dir);
	else if (scan_fd >= 0)
		(void) close(scan_fd);
	if (directory_fd >= 0)
		(void) close(directory_fd);
	if (!keep_cursor)
		forkmeta_temp_gc_cursor_reset();
	return rc;
}

int
ps_forkmeta_snapshot_reclaim_observation(const char *directory,
										 const char *source_directory,
										 uint64_t source_baseline,
										 int source_debt_enabled,
										 const PsForkmetaSnapshotExpected *expected,
										 PsForkmetaSnapshotReclaimObservation *observation)
{
	SnapshotRecord selected_before;
	SnapshotRecord selected_after;
	SnapshotRecord prepared_before;
	SnapshotRecord prepared_after;
	ForkmetaObservationIdentity source_dir_before, source_dir_after;
	ForkmetaObservationIdentity source_path_before, source_path_after;
	ForkmetaObservationIdentity source_before, source_after;
	ForkmetaObservationIdentity snapshot_dir_before, snapshot_dir_after;
	ForkmetaObservationIdentity snapshot_path_before, snapshot_path_after;
	ForkmetaObservationIdentity manifest_before, manifest_after;
	ForkmetaObservationIdentity prepared_identity_before, prepared_identity_after;
	ForkmetaObservationIdentity selected_parts_before[2], selected_parts_after[2];
	ForkmetaObservationIdentity prepared_parts_before[2], prepared_parts_after[2];
	uint64_t gc_debt;
	uint64_t gc_temp;
	uint64_t gc_canonical;
	uint64_t cutoff_debt;
	uint64_t source_debt;
	int gc_overflow;
	int gc_temp_gc_due;
	int scan_rc;
	int source_fd = -1;
	int snapshot_fd = -1;
	int selected_present_before;
	int selected_present_after;
	int prepared_present_before;
	int prepared_present_after;

	if (directory == NULL || source_directory == NULL || expected == NULL ||
		observation == NULL)
		return -1;
	memset(observation, 0, sizeof(*observation));
	source_fd = open(source_directory,
					 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (source_fd < 0)
		return -1;
	snapshot_fd = open(directory,
					 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (snapshot_fd < 0 && errno != ENOENT)
		goto fail;
	if (snapshot_fd < 0 && expected->generation != 0)
		goto fail;
	memset(&manifest_before, 0, sizeof(manifest_before));
	memset(&manifest_after, 0, sizeof(manifest_after));
	for (unsigned int attempt = 0; attempt < FORKMETA_OBSERVATION_RETRIES;
			 attempt++)
	{
		/* Do not duplicate the previous scan's open file description: its
		 * directory offset is shared by dup'd descriptors.  Reopen the trusted
		 * directory for every retry so a second scan starts at entry zero. */
		if (snapshot_fd >= 0)
		{
			if (close(snapshot_fd) != 0)
				goto fail;
			snapshot_fd = -1;
		}
		if (forkmeta_directory_path_identity(directory, &snapshot_path_before) != 0)
			goto fail;
		if (snapshot_path_before.present)
		{
			snapshot_fd = open(directory,
							 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
			if (snapshot_fd < 0)
				goto fail;
		}
		if (forkmeta_directory_identity(source_fd, &source_dir_before) != 0 ||
			forkmeta_directory_path_identity(source_directory,
												&source_path_before) != 0 ||
			!forkmeta_identity_equal(&source_dir_before, &source_path_before) ||
			forkmeta_identity_at(source_fd, "forkmeta", &source_before) != 0)
			goto fail;
		if (snapshot_fd >= 0)
		{
			if (forkmeta_directory_identity(snapshot_fd, &snapshot_dir_before) != 0 ||
				forkmeta_directory_path_identity(directory,
												 &snapshot_path_before) != 0 ||
				!forkmeta_identity_equal(&snapshot_dir_before,
												 &snapshot_path_before))
				goto fail;
			if (expected->generation == 0)
			{
				if (forkmeta_identity_at(snapshot_fd, FORKMETA_SNAPSHOT_MANIFEST,
													 &manifest_before) != 0 ||
													 manifest_before.present)
					goto fail;
				selected_present_before = 0;
			}
			else
			{
				if (forkmeta_read_record_metadata(snapshot_fd, directory,
													 FORKMETA_SNAPSHOT_MANIFEST,
													 &selected_before, &manifest_before) != 0 ||
													 !forkmeta_record_equal_expected(&selected_before,
																	 expected) ||
													 forkmeta_snapshot_parts_identity(snapshot_fd,
																			 &selected_before,
																			 selected_parts_before) != 0)
					goto fail;
				selected_present_before = 1;
			}
			if (forkmeta_read_optional_record(snapshot_fd, directory,
													 FORKMETA_SNAPSHOT_PREPARED,
													 &prepared_before,
													 &prepared_identity_before,
													 &prepared_present_before) != 0 ||
				(prepared_present_before &&
					forkmeta_snapshot_parts_identity(snapshot_fd, &prepared_before,
																	 prepared_parts_before) != 0))
				goto fail;
			scan_rc = forkmeta_snapshot_debt_scan(snapshot_fd, expected->generation,
																 prepared_present_before ?
																 prepared_before.generation : 0,
																 &gc_debt,
																 &gc_temp,
																 &gc_canonical,
																 &cutoff_debt,
																 &gc_overflow,
																 &gc_temp_gc_due);
			if (scan_rc < 0)
				goto fail;
			if (observation_test_hook != NULL)
				observation_test_hook(attempt, observation_test_hook_arg);
			if (expected->generation == 0)
			{
				if (forkmeta_identity_at(snapshot_fd, FORKMETA_SNAPSHOT_MANIFEST,
													 &manifest_after) != 0 ||
													 manifest_after.present)
				goto fail;
				selected_present_after = 0;
			}
			else
			{
				if (forkmeta_read_record_metadata(snapshot_fd, directory,
													 FORKMETA_SNAPSHOT_MANIFEST,
													 &selected_after, &manifest_after) != 0 ||
													 !forkmeta_record_equal_expected(&selected_after,
																	 expected) ||
													 forkmeta_snapshot_parts_identity(snapshot_fd,
																			 &selected_after,
																			 selected_parts_after) != 0)
				goto fail;
				selected_present_after = 1;
			}
			if (forkmeta_read_optional_record(snapshot_fd, directory,
													 FORKMETA_SNAPSHOT_PREPARED,
													 &prepared_after,
													 &prepared_identity_after,
													 &prepared_present_after) != 0 ||
				(prepared_present_after &&
					forkmeta_snapshot_parts_identity(snapshot_fd, &prepared_after,
															 prepared_parts_after) != 0) ||
				forkmeta_directory_identity(snapshot_fd, &snapshot_dir_after) != 0 ||
				forkmeta_directory_path_identity(directory,
												 &snapshot_path_after) != 0)
				goto fail;
		}
		else
		{
			memset(&snapshot_dir_before, 0, sizeof(snapshot_dir_before));
			memset(&snapshot_path_before, 0, sizeof(snapshot_path_before));
			memset(&snapshot_dir_after, 0, sizeof(snapshot_dir_after));
			if (forkmeta_directory_path_identity(directory,
												 &snapshot_path_after) != 0)
				goto fail;
			selected_present_before = selected_present_after = 0;
			prepared_present_before = prepared_present_after = 0;
			gc_debt = 0;
			gc_temp = 0;
			gc_canonical = 0;
			cutoff_debt = 0;
			gc_overflow = 0;
			gc_temp_gc_due = 0;
			scan_rc = 0;
		}
		if (forkmeta_identity_at(source_fd, "forkmeta", &source_after) != 0 ||
			forkmeta_directory_identity(source_fd, &source_dir_after) != 0 ||
			forkmeta_directory_path_identity(source_directory,
												&source_path_after) != 0 ||
			!forkmeta_identity_equal(&source_dir_after, &source_path_after) ||
			!forkmeta_identity_equal(&source_before, &source_after) ||
			!forkmeta_identity_equal(&source_dir_before, &source_dir_after) ||
			!forkmeta_identity_equal(&source_path_before, &source_path_after) ||
			!forkmeta_identity_equal(&snapshot_dir_before, &snapshot_dir_after) ||
			!forkmeta_identity_equal(&snapshot_path_before, &snapshot_path_after) ||
			selected_present_before != selected_present_after ||
			(prepared_present_before != prepared_present_after) ||
			(selected_present_before &&
			 (!forkmeta_identity_equal(&manifest_before, &manifest_after) ||
			  !forkmeta_identity_equal(&selected_parts_before[0],
													 &selected_parts_after[0]) ||
			  !forkmeta_identity_equal(&selected_parts_before[1],
													 &selected_parts_after[1]))) ||
			(prepared_present_before &&
			 (!forkmeta_identity_equal(&prepared_identity_before,
													&prepared_identity_after) ||
			  !forkmeta_record_equal(&prepared_before, &prepared_after) ||
			  !forkmeta_identity_equal(&prepared_parts_before[0],
													 &prepared_parts_after[0]) ||
			  !forkmeta_identity_equal(&prepared_parts_before[1],
													 &prepared_parts_after[1]))) ||
			(expected->generation == 0 &&
			 !forkmeta_identity_equal(&manifest_before, &manifest_after)))
			continue;
		if (source_after.present)
		{
			uint64_t source_size = (uint64_t) source_after.size;

			if (source_size < source_baseline)
				goto fail;
			source_debt = source_debt_enabled ? source_size - source_baseline : 0;
		}
		else
		{
			if (source_baseline != 0)
				goto fail;
			source_debt = 0;
		}
		observation->source_debt_bytes = source_debt;
		observation->gc_serviceable_bytes = gc_debt;
		observation->gc_temp_bytes = gc_temp;
		observation->gc_canonical_bytes = gc_canonical;
		observation->cutoff_dependent_bytes = cutoff_debt;
		observation->gc_serviceable_overflow = gc_overflow;
		observation->gc_temp_gc_due = gc_temp_gc_due;
		observation->bytes = source_debt;
		forkmeta_add_bytes(&observation->bytes,
						   observation->gc_serviceable_bytes);
		forkmeta_add_bytes(&observation->bytes,
						   observation->cutoff_dependent_bytes);
		(void) close(source_fd);
		if (snapshot_fd >= 0)
			(void) close(snapshot_fd);
		return scan_rc;
	}

fail:
	if (source_fd >= 0)
		(void) close(source_fd);
	if (snapshot_fd >= 0)
		(void) close(snapshot_fd);
	return -1;
}

int
ps_forkmeta_snapshot_reclaim_bytes(const char *directory,
										 const char *source_directory,
										 uint64_t source_baseline,
										 int source_debt_enabled,
										 const PsForkmetaSnapshotExpected *expected,
										 uint64_t *bytes_out)
{
	PsForkmetaSnapshotReclaimObservation observation;

	if (bytes_out == NULL ||
		ps_forkmeta_snapshot_reclaim_observation(directory, source_directory,
											 source_baseline, source_debt_enabled,
											 expected, &observation) != 0)
		return -1;
	*bytes_out = observation.bytes;
	return 0;
}

void
ps_forkmeta_snapshot_close(PsForkmetaSnapshot *snapshot)
{
	if (snapshot == NULL)
		return;
	if (snapshot->checkpoint_fd >= 0)
		(void) close(snapshot->checkpoint_fd);
	if (snapshot->tail_fd >= 0)
		(void) close(snapshot->tail_fd);
	if (snapshot->directory_fd >= 0)
		(void) close(snapshot->directory_fd);
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->directory_fd = -1;
	snapshot->checkpoint_fd = -1;
	snapshot->tail_fd = -1;
}
