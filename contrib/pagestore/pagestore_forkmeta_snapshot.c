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
			const char *p = marker + 5;

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
	}
	for (size_t i = 0; i < sizeof(bases) / sizeof(bases[0]); i++)
	{
		size_t base_len = strlen(bases[i]);
		const char *p;

		if ((size_t) (marker - name) != base_len ||
			memcmp(name, bases[i], base_len) != 0)
			continue;
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
		int recognized = parse_temp_name(entry->d_name) == 0;

		if (!recognized)
		{
			recognized = parse_generation_name(entry->d_name,
									 FORKMETA_SNAPSHOT_CHECKPOINT_PREFIX,
									 &generation) == 0 ||
				parse_generation_name(entry->d_name,
									 FORKMETA_SNAPSHOT_TAIL_PREFIX, &generation) == 0;
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

		if (closedir(dir) != 0 || scan_errno != 0)
			goto cleanup;
		dir = NULL;
	}
	for (size_t i = 0; i < count; i++)
	{
		if (unlinkat(current.directory_fd, names[i], 0) != 0)
			unlink_failed = 1;
		else
			removed = 1;
	}
	if (getenv("PAGESTORE_TEST_FAIL_FORKMETA_GC_FSYNC") != NULL)
	{
		errno = EIO;
		goto cleanup;
	}
	/* Always sync: an empty retry closes an ambiguous prior unlink fsync. */
	if (fsync(current.directory_fd) != 0)
		unlink_failed = 1;
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
