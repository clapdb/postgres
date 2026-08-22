#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_walidx_snapshot.h"

/*
 * A timeline manifest is the only discovery point.  Shards are immutable and
 * durable before rename selects their generation, so recovery observes either
 * the complete old set or the complete new set and ignores publication debris.
 */

#define WALIDX_SNAPSHOT_MAGIC UINT32_C(0x4d534957) /* "WISM" */
#define WALIDX_SNAPSHOT_VERSION 1
#define WALIDX_SNAPSHOT_HEADER_BYTES 64
#define WALIDX_SNAPSHOT_ENTRY_BYTES 16
#define WALIDX_SNAPSHOT_MANIFEST "walidx_manifest_v1"
#define WALIDX_SNAPSHOT_PREPARED "walidx_prepared_v1"
#define VERIFY_BYTES (64u * 1024u)

static int open_directory(const char *directory, int create);
static int read_prepared(int directory_fd, const char *directory,
						 uint32_t timeline,
						 PsWalIdxSnapshotPrepared *prepared,
						 int validate_shards);
static int test_failed_walidx_after_shard_once;

static uint32_t
fnv1a(uint32_t hash, const void *data, size_t len)
{
	const unsigned char *bytes = data;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= bytes[i];
		hash *= 16777619u;
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
		size_t amount = len - done > UINT64_C(1024) * 1024 * 1024 ?
			(size_t) (UINT64_C(1024) * 1024 * 1024) :
			(size_t) (len - done);
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
read_all_at(int fd, void *data, size_t len, uint64_t offset)
{
	unsigned char *bytes = data;
	size_t done = 0;

	if (offset > (uint64_t) INT64_MAX ||
		(uint64_t) len > (uint64_t) INT64_MAX - offset)
		return -1;
	while (done < len)
	{
		ssize_t n = pread(fd, bytes + done, len - done,
						  (off_t) offset + (off_t) done);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		done += (size_t) n;
	}
	return 0;
}

static int
shard_name(uint64_t generation, uint32_t shard, char *name, size_t name_len)
{
	int n = snprintf(name, name_len, "walidxg1_%020llu_%03u",
				 (unsigned long long) generation, shard);

	return n < 0 || (size_t) n >= name_len ? -1 : 0;
}

int
ps_walidx_snapshot_next_generation(const char *directory,
								   uint64_t selected_generation,
								   uint64_t *generation_out)
{
	static const char prefix[] = "walidxg1_";
	struct dirent *entry;
	DIR *dir;
	uint64_t highest = selected_generation;

	if (directory == NULL || generation_out == NULL ||
		selected_generation == UINT64_MAX)
		return -1;
	dir = opendir(directory);
	if (dir == NULL)
	{
		if (errno != ENOENT)
			return -1;
		*generation_out = selected_generation + 1;
		return 0;
	}
	while ((entry = readdir(dir)) != NULL)
	{
		char expected[128];
		char *end = NULL;
		unsigned long long parsed;
		unsigned long shard;

		if (strncmp(entry->d_name, prefix, sizeof(prefix) - 1) != 0)
			continue;
		errno = 0;
		parsed = strtoull(entry->d_name + sizeof(prefix) - 1, &end, 10);
		if (errno != 0 || end == entry->d_name + sizeof(prefix) - 1 ||
			*end != '_')
			continue;
		errno = 0;
		shard = strtoul(end + 1, &end, 10);
		if (errno != 0 || *end != '\0' || shard >= PS_WALIDX_SNAPSHOT_MAX_SHARDS ||
			shard_name((uint64_t) parsed, (uint32_t) shard,
					   expected, sizeof(expected)) != 0 ||
			strcmp(expected, entry->d_name) != 0)
			continue;
		if ((uint64_t) parsed > highest)
			highest = (uint64_t) parsed;
	}
	if (closedir(dir) != 0 || highest == UINT64_MAX)
		return -1;
	*generation_out = highest + 1;
	return 0;
}

int
ps_walidx_snapshot_read_prepared(const char *directory, uint32_t timeline,
									   PsWalIdxSnapshotPrepared *prepared)
{
	int directory_fd;

	if (directory == NULL || prepared == NULL)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (faccessat(directory_fd, WALIDX_SNAPSHOT_PREPARED, F_OK, 0) != 0)
	{
		int saved_errno = errno;

		close(directory_fd);
		return saved_errno == ENOENT ? 0 : -1;
	}
	if (read_prepared(directory_fd, directory, timeline, prepared, 1) != 0)
	{
		close(directory_fd);
		return -1;
	}
	if (close(directory_fd) != 0)
		return -1;
	return 1;
}

int
ps_walidx_snapshot_prepared_generation(const char *directory, uint32_t timeline,
									   uint64_t *generation_out)
{
	PsWalIdxSnapshotPrepared prepared;
	int rc;

	if (generation_out == NULL)
		return -1;
	rc = ps_walidx_snapshot_read_prepared(directory, timeline, &prepared);
	if (rc == 1)
		*generation_out = prepared.generation;
	return rc;
}

int
ps_walidx_snapshot_discard_generation(const char *directory, uint32_t timeline,
									  uint64_t generation, uint32_t nshards)
{
	PsWalIdxSnapshot current;
	int directory_fd;
	int manifest_exists;

	if (directory == NULL || generation == 0 || nshards == 0 ||
		nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS)
		return -1;
	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return errno == ENOENT ? 0 : -1;
	manifest_exists = faccessat(directory_fd, WALIDX_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
		goto fail;
	if (manifest_exists)
	{
		if (ps_walidx_snapshot_open(&current, directory, timeline) != 0)
			goto fail;
		if (current.generation == generation)
		{
			ps_walidx_snapshot_close(&current);
			close(directory_fd);
			return 1;
		}
		ps_walidx_snapshot_close(&current);
	}
	for (uint32_t shard = 0; shard < nshards; shard++)
	{
		char name[128];

		if (shard_name(generation, shard, name, sizeof(name)) != 0 ||
			(unlinkat(directory_fd, name, 0) != 0 && errno != ENOENT))
			goto fail;
	}
	if (fsync(directory_fd) != 0 || close(directory_fd) != 0)
		return -1;
	return 0;

fail:
	close(directory_fd);
	return -1;
}

static int
parse_shard_name(const char *name, uint64_t *generation_out)
{
	static const char prefix[] = "walidxg1_";
	const size_t prefix_len = sizeof(prefix) - 1;
	const size_t generation_end = prefix_len + 20;
	const size_t shard_start = generation_end + 1;
	const size_t name_len = shard_start + 3;
	uint64_t generation = 0;
	uint32_t shard = 0;

	if (strlen(name) != name_len || memcmp(name, prefix, prefix_len) != 0 ||
		name[generation_end] != '_')
		return -1;
	for (size_t i = prefix_len; i < generation_end; i++)
	{
		unsigned int digit;

		if (name[i] < '0' || name[i] > '9')
			return -1;
		digit = (unsigned int) (name[i] - '0');
		if (generation > (UINT64_MAX - digit) / 10)
			return -1;
		generation = generation * 10 + digit;
	}
	for (size_t i = shard_start; i < name_len; i++)
	{
		if (name[i] < '0' || name[i] > '9')
			return -1;
		shard = shard * 10 + (uint32_t) (name[i] - '0');
	}
	if (generation == 0 || shard >= PS_WALIDX_SNAPSHOT_MAX_SHARDS)
		return -1;
	*generation_out = generation;
	return 0;
}

static int
parse_shard_temp_name(const char *name)
{
	const char *marker = strstr(name, ".tmp.");
	char final_name[128];
	uint64_t generation;
	const char *p;
	size_t final_len;

	if (marker == NULL || strstr(marker + 1, ".tmp.") != NULL)
		return -1;
	final_len = (size_t) (marker - name);
	if (final_len == 0 || final_len >= sizeof(final_name))
		return -1;
	memcpy(final_name, name, final_len);
	final_name[final_len] = '\0';
	if (parse_shard_name(final_name, &generation) != 0)
		return -1;
	p = marker + strlen(".tmp.");
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
open_directory(const char *directory, int create)
{
	int directory_fd;
	int parent_fd;

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
			close(parent_fd);
		close(directory_fd);
		return -1;
	}
	if (close(parent_fd) != 0)
	{
		close(directory_fd);
		return -1;
	}
	return directory_fd;
}

static int
input_emit(const PsWalIdxSnapshotInput *input,
		   PsWalIdxSnapshotConsume consume, void *consume_arg)
{
	if (input->produce != NULL)
		return input->data == NULL ?
			input->produce(input->produce_arg, consume, consume_arg) : -1;
	if (input->data == NULL && input->len != 0)
		return -1;
	if (input->len > (uint64_t) SIZE_MAX)
		return -1;
	return consume(consume_arg, input->data, (size_t) input->len);
}

typedef struct SnapshotCrcCtx
{
	uint32_t crc;
	uint64_t len;
} SnapshotCrcCtx;

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
	int fd = *(int *) arg;

	return write_all(fd, data, len);
}

typedef struct SnapshotCompareCtx
{
	int fd;
	uint64_t offset;
	uint32_t crc;
} SnapshotCompareCtx;

static int
consume_compare(void *arg, const void *data, size_t len)
{
	SnapshotCompareCtx *ctx = arg;
	unsigned char buf[VERIFY_BYTES];
	const unsigned char *expected = data;
	size_t done = 0;

	while (done < len)
	{
		size_t amount = len - done < sizeof(buf) ? len - done : sizeof(buf);

		if (read_all_at(ctx->fd, buf, amount, ctx->offset) != 0 ||
			memcmp(buf, expected + done, amount) != 0)
			return -1;
		ctx->crc = fnv1a(ctx->crc, buf, amount);
		ctx->offset += amount;
		done += amount;
	}
	return 0;
}

static int
published_shard_matches(int directory_fd, const char *name,
						const PsWalIdxSnapshotInput *input, uint32_t crc)
{
	SnapshotCompareCtx ctx;
	struct stat st;
	int fd = openat(directory_fd, name,
					O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);

	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size != input->len)
		goto fail;
	ctx.fd = fd;
	ctx.offset = 0;
	ctx.crc = 2166136261u;
	if (input_emit(input, consume_compare, &ctx) != 0 ||
		ctx.offset != input->len || ctx.crc != crc)
		goto fail;
	if (close(fd) != 0)
		return -1;
	return 0;

fail:
	if (fd >= 0)
		close(fd);
	return -1;
}

static int
published_shard_valid(int directory_fd, const char *name, uint64_t len,
					uint32_t expected_crc)
{
	unsigned char buf[VERIFY_BYTES];
	struct stat st;
	uint64_t done = 0;
	uint32_t crc = 2166136261u;
	int fd = openat(directory_fd, name,
					O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);

	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size != len)
		goto fail;
	while (done < len)
	{
		size_t amount = len - done < sizeof(buf) ?
			(size_t) (len - done) : sizeof(buf);

		if (read_all_at(fd, buf, amount, done) != 0)
			goto fail;
		crc = fnv1a(crc, buf, amount);
		done += amount;
	}
	if (close(fd) != 0)
		return -1;
	return crc == expected_crc ? 0 : -1;

fail:
	if (fd >= 0)
		close(fd);
	return -1;
}

static int
publish_shard(int directory_fd, uint64_t generation, uint32_t shard,
			  const PsWalIdxSnapshotInput *input, uint32_t crc)
{
	char final_name[128];
	char temporary[160];
	int fd = -1;
	int rc = -1;

	if (shard_name(generation, shard, final_name, sizeof(final_name)) != 0)
		return -1;
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%u",
						 final_name, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0)
		return -1;
	if (input_emit(input, consume_write, &fd) != 0 || fsync(fd) != 0)
	{
		close(fd);
		fd = -1;
		goto cleanup;
	}
	if (close(fd) != 0)
	{
		fd = -1;
		goto cleanup;
	}
	fd = -1;
	if (linkat(directory_fd, temporary, directory_fd, final_name, 0) != 0)
	{
		if (errno != EEXIST ||
			published_shard_matches(directory_fd, final_name,
								input, crc) != 0)
			goto cleanup;
	}
	if (unlinkat(directory_fd, temporary, 0) != 0 || fsync(directory_fd) != 0)
		goto cleanup;
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	(void) unlinkat(directory_fd, temporary, 0);
	return rc;
}

static unsigned char *
encode_manifest(uint32_t timeline, uint64_t generation, uint64_t start_lsn,
				uint64_t end_lsn, const PsWalIdxSnapshotShard *shards,
				uint32_t nshards, size_t *len_out)
{
	size_t len = WALIDX_SNAPSHOT_HEADER_BYTES +
		(size_t) nshards * WALIDX_SNAPSHOT_ENTRY_BYTES;
	unsigned char *encoded = calloc(1, len);
	uint32_t crc;

	if (encoded == NULL)
		return NULL;
	put_le32(encoded + 0, WALIDX_SNAPSHOT_MAGIC);
	put_le32(encoded + 4, WALIDX_SNAPSHOT_VERSION);
	put_le32(encoded + 8, WALIDX_SNAPSHOT_HEADER_BYTES);
	put_le32(encoded + 12, WALIDX_SNAPSHOT_ENTRY_BYTES);
	put_le32(encoded + 16, timeline);
	put_le32(encoded + 20, nshards);
	put_le64(encoded + 24, generation);
	put_le64(encoded + 32, start_lsn);
	put_le64(encoded + 40, end_lsn);
	for (uint32_t i = 0; i < nshards; i++)
	{
		unsigned char *entry = encoded + WALIDX_SNAPSHOT_HEADER_BYTES +
			(size_t) i * WALIDX_SNAPSHOT_ENTRY_BYTES;

		put_le32(entry + 0, i);
		put_le32(entry + 4, shards[i].crc);
		put_le64(entry + 8, shards[i].len);
	}
	crc = fnv1a(2166136261u, encoded, len);
	put_le32(encoded + 48, crc);
	*len_out = len;
	return encoded;
}

static int
publish_manifest(int directory_fd, const unsigned char *encoded, size_t len,
				 uint64_t generation)
{
	char temporary[128];
	int fd = -1;

	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary),
						 "%s.tmp.%020llu.%ld.%u", WALIDX_SNAPSHOT_MANIFEST,
						 (unsigned long long) generation, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0)
		return -1;
	if (write_all(fd, encoded, len) != 0 || fsync(fd) != 0)
	{
		close(fd);
		(void) unlinkat(directory_fd, temporary, 0);
		return -1;
	}
	if (close(fd) != 0)
	{
		(void) unlinkat(directory_fd, temporary, 0);
		return -1;
	}
	if (renameat(directory_fd, temporary, directory_fd,
				 WALIDX_SNAPSHOT_MANIFEST) != 0 || fsync(directory_fd) != 0)
	{
		(void) unlinkat(directory_fd, temporary, 0);
		return -1;
	}
	return 0;
}

static int
read_prepared(int directory_fd, const char *directory, uint32_t timeline,
			  PsWalIdxSnapshotPrepared *prepared, int validate_shards)
{
	unsigned char header[WALIDX_SNAPSHOT_HEADER_BYTES];
	unsigned char *encoded = NULL;
	struct stat st;
	size_t expected_len;
	uint32_t stored_crc;
	int fd = -1;
	int rc = -1;

	memset(prepared, 0, sizeof(*prepared));
	fd = openat(directory_fd, WALIDX_SNAPSHOT_PREPARED,
				O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size < WALIDX_SNAPSHOT_HEADER_BYTES ||
		read_all_at(fd, header, sizeof(header), 0) != 0 ||
		get_le32(header + 0) != WALIDX_SNAPSHOT_MAGIC ||
		get_le32(header + 4) != WALIDX_SNAPSHOT_VERSION ||
		get_le32(header + 8) != WALIDX_SNAPSHOT_HEADER_BYTES ||
		get_le32(header + 12) != WALIDX_SNAPSHOT_ENTRY_BYTES ||
		get_le32(header + 16) != timeline || get_le32(header + 52) != 0 ||
		get_le64(header + 56) != 0)
		goto cleanup;
	prepared->timeline = timeline;
	prepared->nshards = get_le32(header + 20);
	prepared->generation = get_le64(header + 24);
	prepared->start_lsn = get_le64(header + 32);
	prepared->end_lsn = get_le64(header + 40);
	if (prepared->nshards == 0 ||
		prepared->nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS ||
		prepared->generation == 0 || prepared->end_lsn < prepared->start_lsn)
		goto cleanup;
	expected_len = WALIDX_SNAPSHOT_HEADER_BYTES +
		(size_t) prepared->nshards * WALIDX_SNAPSHOT_ENTRY_BYTES;
	if ((uint64_t) st.st_size != expected_len)
		goto cleanup;
	encoded = malloc(expected_len);
	if (encoded == NULL || read_all_at(fd, encoded, expected_len, 0) != 0)
		goto cleanup;
	stored_crc = get_le32(encoded + 48);
	put_le32(encoded + 48, 0);
	if (stored_crc != fnv1a(2166136261u, encoded, expected_len))
		goto cleanup;
	if (snprintf(prepared->directory, sizeof(prepared->directory), "%s",
				 directory) < 0 || strlen(directory) >= sizeof(prepared->directory))
		goto cleanup;
	for (uint32_t i = 0; i < prepared->nshards; i++)
	{
		const unsigned char *entry = encoded + WALIDX_SNAPSHOT_HEADER_BYTES +
			(size_t) i * WALIDX_SNAPSHOT_ENTRY_BYTES;
		char name[128];

		if (get_le32(entry + 0) != i)
			goto cleanup;
		prepared->shards[i].crc = get_le32(entry + 4);
		prepared->shards[i].len = get_le64(entry + 8);
		if (validate_shards &&
			(shard_name(prepared->generation, i, name, sizeof(name)) != 0 ||
			 published_shard_valid(directory_fd, name, prepared->shards[i].len,
							  prepared->shards[i].crc) != 0))
			goto cleanup;
	}
	rc = 0;

cleanup:
	free(encoded);
	if (fd >= 0)
		close(fd);
	return rc;
}

static int
prepared_equal(const PsWalIdxSnapshotPrepared *a,
			   const PsWalIdxSnapshotPrepared *b)
{
	if (a->timeline != b->timeline || a->nshards != b->nshards ||
		a->generation != b->generation || a->start_lsn != b->start_lsn ||
		a->end_lsn != b->end_lsn)
		return 0;
	for (uint32_t i = 0; i < a->nshards; i++)
		if (a->shards[i].len != b->shards[i].len ||
			a->shards[i].crc != b->shards[i].crc)
			return 0;
	return 1;
}

static int
publish_prepared(int directory_fd, const PsWalIdxSnapshotPrepared *prepared)
{
	PsWalIdxSnapshotPrepared existing;
	unsigned char *encoded;
	size_t len;
	char temporary[128] = {0};
	int fd = -1;

	if (faccessat(directory_fd, WALIDX_SNAPSHOT_PREPARED, F_OK, 0) == 0)
		return read_prepared(directory_fd, prepared->directory,
						 prepared->timeline, &existing, 1) == 0 &&
			prepared_equal(&existing, prepared) ? 0 : -1;
	if (errno != ENOENT)
		return -1;
	encoded = encode_manifest(prepared->timeline, prepared->generation,
							 prepared->start_lsn, prepared->end_lsn,
							 prepared->shards, prepared->nshards, &len);
	if (encoded == NULL)
		return -1;
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%u",
						 WALIDX_SNAPSHOT_PREPARED, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			goto fail;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0 || write_all(fd, encoded, len) != 0 || fsync(fd) != 0)
		goto fail;
	if (close(fd) != 0)
	{
		fd = -1;
		goto fail;
	}
	fd = -1;
	if (renameat(directory_fd, temporary, directory_fd,
				 WALIDX_SNAPSHOT_PREPARED) != 0 || fsync(directory_fd) != 0)
		goto fail;
	free(encoded);
	return 0;

fail:
	if (fd >= 0)
		close(fd);
	if (temporary[0] != '\0')
		(void) unlinkat(directory_fd, temporary, 0);
	free(encoded);
	return -1;
}

static int
clear_prepared(int directory_fd)
{
	if (unlinkat(directory_fd, WALIDX_SNAPSHOT_PREPARED, 0) != 0 &&
		errno != ENOENT)
		return -1;
	return fsync(directory_fd);
}

int
ps_walidx_snapshot_prepare(PsWalIdxSnapshotPrepared *prepared,
					   const char *directory, uint32_t timeline,
					   uint64_t generation, uint64_t start_lsn,
					   uint64_t end_lsn,
					   const PsWalIdxSnapshotInput *inputs,
					   uint32_t nshards)
{
	PsWalIdxSnapshotShard shards[PS_WALIDX_SNAPSHOT_MAX_SHARDS];
	PsWalIdxSnapshot current;
	int directory_fd;
	int rc = -1;
	int n;

	if (prepared == NULL || directory == NULL || generation == 0 ||
		inputs == NULL || nshards == 0 ||
		nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS || end_lsn < start_lsn)
		return -1;
	memset(prepared, 0, sizeof(*prepared));
	for (uint32_t i = 0; i < nshards; i++)
	{
		SnapshotCrcCtx ctx = {2166136261u, 0};

		if ((inputs[i].data != NULL && inputs[i].produce != NULL) ||
			(inputs[i].data == NULL && inputs[i].produce == NULL &&
			 inputs[i].len != 0) || inputs[i].len > (uint64_t) INT64_MAX ||
			input_emit(&inputs[i], consume_crc, &ctx) != 0 ||
			ctx.len != inputs[i].len)
			return -1;
		shards[i].len = inputs[i].len;
		shards[i].crc = ctx.crc;
	}
	directory_fd = open_directory(directory, 1);
	if (directory_fd < 0)
		return -1;
	if (faccessat(directory_fd, WALIDX_SNAPSHOT_MANIFEST, F_OK, 0) == 0)
	{
		int identical = 1;

		if (ps_walidx_snapshot_open(&current, directory, timeline) != 0)
			goto cleanup;
		if (generation < current.generation)
			identical = 0;
		else if (generation > current.generation &&
				 (start_lsn < current.start_lsn || end_lsn < current.end_lsn))
			identical = 0;
		else if (generation == current.generation)
		{
			if (current.start_lsn != start_lsn || current.end_lsn != end_lsn ||
				current.nshards != nshards)
				identical = 0;
			for (uint32_t i = 0; identical && i < nshards; i++)
			{
				char name[128];

				if (current.shards[i].len != shards[i].len ||
					current.shards[i].crc != shards[i].crc ||
					shard_name(generation, i, name, sizeof(name)) != 0 ||
					published_shard_matches(current.directory_fd, name,
										&inputs[i],
										shards[i].crc) != 0)
					identical = 0;
			}
			ps_walidx_snapshot_close(&current);
			if (identical)
				goto prepared_ok;
			goto cleanup;
		}
		ps_walidx_snapshot_close(&current);
		if (!identical)
			goto cleanup;
	}
	else if (errno != ENOENT)
		goto cleanup;
	for (uint32_t i = 0; i < nshards; i++)
	{
		if (publish_shard(directory_fd, generation, i, &inputs[i],
						  shards[i].crc) != 0)
			goto cleanup;
		{
			const char *fail_after = getenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD");
			const char *fail_once =
				getenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD_ONCE");

			if (fail_after != NULL && strtoul(fail_after, NULL, 10) == i)
				goto cleanup;
			if (!test_failed_walidx_after_shard_once && fail_once != NULL &&
				strtoul(fail_once, NULL, 10) == i)
			{
				test_failed_walidx_after_shard_once = 1;
				goto cleanup;
			}
		}
	}

prepared_ok:
	n = snprintf(prepared->directory, sizeof(prepared->directory), "%s", directory);
	if (n < 0 || (size_t) n >= sizeof(prepared->directory))
		goto cleanup;
	prepared->timeline = timeline;
	prepared->nshards = nshards;
	prepared->generation = generation;
	prepared->start_lsn = start_lsn;
	prepared->end_lsn = end_lsn;
	memcpy(prepared->shards, shards, (size_t) nshards * sizeof(shards[0]));
	rc = publish_prepared(directory_fd, prepared);

cleanup:
	close(directory_fd);
	return rc;
}

int
ps_walidx_snapshot_commit(const PsWalIdxSnapshotPrepared *prepared)
{
	PsWalIdxSnapshot current;
	unsigned char *manifest = NULL;
	size_t manifest_len;
	int directory_fd;
	int rc = -1;

	if (prepared == NULL || prepared->directory[0] == '\0' ||
		prepared->generation == 0 || prepared->nshards == 0 ||
		prepared->nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS ||
		prepared->end_lsn < prepared->start_lsn)
		return -1;
	directory_fd = open_directory(prepared->directory, 0);
	if (directory_fd < 0)
		return -1;
	for (uint32_t i = 0; i < prepared->nshards; i++)
	{
		char name[128];

		if (shard_name(prepared->generation, i, name, sizeof(name)) != 0 ||
			published_shard_valid(directory_fd, name, prepared->shards[i].len,
							 prepared->shards[i].crc) != 0)
			goto cleanup;
	}
	if (faccessat(directory_fd, WALIDX_SNAPSHOT_MANIFEST, F_OK, 0) == 0)
	{
		if (ps_walidx_snapshot_open(&current, prepared->directory,
								 prepared->timeline) != 0)
			goto cleanup;
		if (prepared->generation < current.generation ||
			(prepared->generation > current.generation &&
			 (prepared->start_lsn < current.start_lsn ||
			  prepared->end_lsn < current.end_lsn)))
		{
			ps_walidx_snapshot_close(&current);
			goto cleanup;
		}
		if (prepared->generation == current.generation)
		{
			int identical = current.start_lsn == prepared->start_lsn &&
				current.end_lsn == prepared->end_lsn &&
				current.nshards == prepared->nshards;

			for (uint32_t i = 0; identical && i < prepared->nshards; i++)
				if (current.shards[i].len != prepared->shards[i].len ||
					current.shards[i].crc != prepared->shards[i].crc)
					identical = 0;
			ps_walidx_snapshot_close(&current);
			rc = identical && clear_prepared(directory_fd) == 0 ? 0 : -1;
			goto cleanup;
		}
		ps_walidx_snapshot_close(&current);
	}
	else if (errno != ENOENT)
		goto cleanup;
	manifest = encode_manifest(prepared->timeline, prepared->generation,
						   prepared->start_lsn, prepared->end_lsn,
						   prepared->shards, prepared->nshards,
						   &manifest_len);
	if (manifest != NULL &&
		publish_manifest(directory_fd, manifest, manifest_len,
						 prepared->generation) == 0 &&
		clear_prepared(directory_fd) == 0)
		rc = 0;

cleanup:
	free(manifest);
	close(directory_fd);
	return rc;
}

int
ps_walidx_snapshot_abort(const PsWalIdxSnapshotPrepared *prepared)
{
	PsWalIdxSnapshot current;
	int directory_fd;
	int manifest_exists;

	if (prepared == NULL || prepared->directory[0] == '\0' ||
		prepared->generation == 0 || prepared->nshards == 0 ||
		prepared->nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS)
		return -1;
	directory_fd = open_directory(prepared->directory, 0);
	if (directory_fd < 0)
		return -1;
	manifest_exists = faccessat(directory_fd, WALIDX_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
		goto fail;
	if (manifest_exists)
	{
		if (ps_walidx_snapshot_open(&current, prepared->directory,
								 prepared->timeline) != 0)
			goto fail;
		if (current.generation == prepared->generation)
		{
			ps_walidx_snapshot_close(&current);
			goto fail;
		}
		ps_walidx_snapshot_close(&current);
	}
	for (uint32_t shard = 0; shard < prepared->nshards; shard++)
	{
		char name[128];

		if (shard_name(prepared->generation, shard, name, sizeof(name)) != 0 ||
			(unlinkat(directory_fd, name, 0) != 0 && errno != ENOENT))
			goto fail;
	}
	{
		int sync_rc = clear_prepared(directory_fd);
		int close_rc = close(directory_fd);

		if (sync_rc != 0 || close_rc != 0)
			return -1;
	}
	return 0;

fail:
	close(directory_fd);
	return -1;
}

int
ps_walidx_snapshot_recover_prepared(const char *directory, uint32_t timeline,
								uint64_t durable_frontier)
{
	PsWalIdxSnapshotPrepared prepared;
	PsWalIdxSnapshot current;
	int directory_fd;
	int manifest_exists;

	directory_fd = open_directory(directory, 0);
	if (directory_fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (read_prepared(directory_fd, directory, timeline, &prepared, 1) != 0)
	{
		int saved_errno = errno;
		PsWalIdxSnapshotPrepared partial;

		if (faccessat(directory_fd, WALIDX_SNAPSHOT_PREPARED, F_OK, 0) == 0 &&
			read_prepared(directory_fd, directory, timeline, &partial, 0) == 0)
		{
			close(directory_fd);
			return ps_walidx_snapshot_abort(&partial);
		}
		if (errno == ENOENT)
		{
			close(directory_fd);
			return 0;
		}
		close(directory_fd);
		errno = saved_errno != 0 ? saved_errno : EILSEQ;
		return -1;
	}
	manifest_exists = faccessat(directory_fd, WALIDX_SNAPSHOT_MANIFEST,
								 F_OK, 0) == 0;
	if (!manifest_exists && errno != ENOENT)
	{
		close(directory_fd);
		return -1;
	}
	close(directory_fd);
	if (manifest_exists)
	{
		if (ps_walidx_snapshot_open(&current, directory, timeline) != 0)
			return -1;
		if (current.generation == prepared.generation)
		{
			int identical = current.start_lsn == prepared.start_lsn &&
				current.end_lsn == prepared.end_lsn &&
				current.nshards == prepared.nshards;

			for (uint32_t i = 0; identical && i < prepared.nshards; i++)
				if (current.shards[i].len != prepared.shards[i].len ||
					current.shards[i].crc != prepared.shards[i].crc)
					identical = 0;
			ps_walidx_snapshot_close(&current);
			return identical ? ps_walidx_snapshot_commit(&prepared) : -1;
		}
		if (current.generation > prepared.generation)
		{
			ps_walidx_snapshot_close(&current);
			return ps_walidx_snapshot_abort(&prepared);
		}
		/* An equal frontier only covers the selected manifest.  A staged
		 * reshard with the same end LSN has not published a new frontier and
		 * must be discarded after a crash. */
		{
			int frontier_covers_prepare =
				prepared.end_lsn > current.end_lsn &&
				durable_frontier >= prepared.end_lsn;

			ps_walidx_snapshot_close(&current);
			return frontier_covers_prepare ? 0 :
				ps_walidx_snapshot_abort(&prepared);
		}
	}
	/* The durable frontier is the publication decision.  Keep a covered
	 * generation staged so the normal publisher retries the same canonical
	 * bytes; otherwise remove it before admissions reopen. */
	return durable_frontier >= prepared.end_lsn ? 0 :
		ps_walidx_snapshot_abort(&prepared);
}

int
ps_walidx_snapshot_publish(const char *directory, uint32_t timeline,
						   uint64_t generation, uint64_t start_lsn,
						   uint64_t end_lsn,
						   const PsWalIdxSnapshotInput *inputs,
						   uint32_t nshards)
{
	PsWalIdxSnapshotPrepared prepared;

	if (ps_walidx_snapshot_prepare(&prepared, directory, timeline, generation,
								 start_lsn, end_lsn, inputs, nshards) != 0)
		return -1;
	if (ps_walidx_snapshot_commit(&prepared) == 0)
		return 0;
	/* A failed manifest cutover must not leave an ordinary prepared intent
	 * behind.  If the manifest was already selected, abort deliberately fails
	 * and recovery will reconcile the idempotent selected generation. */
	(void) ps_walidx_snapshot_abort(&prepared);
	return -1;
}

static int
validate_shard(PsWalIdxSnapshot *snapshot, uint32_t shard)
{
	unsigned char buf[VERIFY_BYTES];
	struct stat st;
	char name[128];
	uint64_t done = 0;
	uint32_t crc = 2166136261u;
	int fd = -1;
	int rc = -1;

	if (shard_name(snapshot->generation, shard, name, sizeof(name)) != 0 ||
		(fd = openat(snapshot->directory_fd, name,
					 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size != snapshot->shards[shard].len)
		goto cleanup;
	while (done < snapshot->shards[shard].len)
	{
		size_t amount = snapshot->shards[shard].len - done < sizeof(buf) ?
			(size_t) (snapshot->shards[shard].len - done) : sizeof(buf);

		if (read_all_at(fd, buf, amount, done) != 0)
			goto cleanup;
		crc = fnv1a(crc, buf, amount);
		done += amount;
	}
	rc = crc == snapshot->shards[shard].crc ? 0 : -1;

cleanup:
	if (fd >= 0)
		close(fd);
	return rc;
}

int
ps_walidx_snapshot_open(PsWalIdxSnapshot *snapshot, const char *directory,
						uint32_t timeline)
{
	unsigned char header[WALIDX_SNAPSHOT_HEADER_BYTES];
	unsigned char *encoded = NULL;
	struct stat st;
	size_t expected_len;
	uint32_t stored_crc;
	uint32_t actual_crc;
	int fd = -1;
	int rc = -1;

	if (snapshot == NULL || directory == NULL)
		return -1;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->directory_fd = -1;
	if (snprintf(snapshot->directory, sizeof(snapshot->directory), "%s",
				 directory) < 0 || strlen(directory) >= sizeof(snapshot->directory))
		return -1;
	snapshot->directory_fd = open_directory(directory, 0);
	if (snapshot->directory_fd < 0)
		return -1;
	fd = openat(snapshot->directory_fd, WALIDX_SNAPSHOT_MANIFEST,
				O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size < WALIDX_SNAPSHOT_HEADER_BYTES ||
		read_all_at(fd, header, sizeof(header), 0) != 0 ||
		get_le32(header + 0) != WALIDX_SNAPSHOT_MAGIC ||
		get_le32(header + 4) != WALIDX_SNAPSHOT_VERSION ||
		get_le32(header + 8) != WALIDX_SNAPSHOT_HEADER_BYTES ||
		get_le32(header + 12) != WALIDX_SNAPSHOT_ENTRY_BYTES ||
		get_le32(header + 16) != timeline || get_le32(header + 52) != 0 ||
		get_le64(header + 56) != 0)
		goto cleanup;
	snapshot->timeline = timeline;
	snapshot->nshards = get_le32(header + 20);
	snapshot->generation = get_le64(header + 24);
	snapshot->start_lsn = get_le64(header + 32);
	snapshot->end_lsn = get_le64(header + 40);
	if (snapshot->nshards == 0 ||
		snapshot->nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS ||
		snapshot->generation == 0 || snapshot->end_lsn < snapshot->start_lsn)
		goto cleanup;
	expected_len = WALIDX_SNAPSHOT_HEADER_BYTES +
		(size_t) snapshot->nshards * WALIDX_SNAPSHOT_ENTRY_BYTES;
	if ((uint64_t) st.st_size != expected_len)
		goto cleanup;
	encoded = malloc(expected_len);
	if (encoded == NULL || read_all_at(fd, encoded, expected_len, 0) != 0)
		goto cleanup;
	stored_crc = get_le32(encoded + 48);
	put_le32(encoded + 48, 0);
	actual_crc = fnv1a(2166136261u, encoded, expected_len);
	if (stored_crc != actual_crc)
		goto cleanup;
	for (uint32_t i = 0; i < snapshot->nshards; i++)
	{
		const unsigned char *entry = encoded + WALIDX_SNAPSHOT_HEADER_BYTES +
			(size_t) i * WALIDX_SNAPSHOT_ENTRY_BYTES;

		if (get_le32(entry + 0) != i)
			goto cleanup;
		snapshot->shards[i].crc = get_le32(entry + 4);
		snapshot->shards[i].len = get_le64(entry + 8);
		if (validate_shard(snapshot, i) != 0)
			goto cleanup;
	}
	rc = 0;

cleanup:
	free(encoded);
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		ps_walidx_snapshot_close(snapshot);
	return rc;
}

int
ps_walidx_snapshot_read(const PsWalIdxSnapshot *snapshot, uint32_t shard,
						uint64_t offset, void *data, uint32_t len)
{
	char name[128];
	int fd;
	int rc;

	if (snapshot == NULL || snapshot->directory_fd < 0 || data == NULL ||
		shard >= snapshot->nshards || offset + len < offset ||
		offset + len > snapshot->shards[shard].len || len == 0 ||
		shard_name(snapshot->generation, shard, name, sizeof(name)) != 0)
		return -1;
	fd = openat(snapshot->directory_fd, name,
				O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0)
		return -1;
	rc = read_all_at(fd, data, len, offset);
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

int
ps_walidx_snapshot_gc(const char *directory, uint32_t timeline)
{
	PsWalIdxSnapshot current;
	struct dirent *entry;
	char (*names)[128] = NULL;
	DIR *dir = NULL;
	size_t count = 0;
	size_t capacity = 0;
	int scan_fd = -1;
	int rc = -1;

	if (ps_walidx_snapshot_open(&current, directory, timeline) != 0)
		return -1;
	scan_fd = fcntl(current.directory_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		uint64_t generation;
		char (*grown)[128];

		if (parse_shard_temp_name(entry->d_name) != 0 &&
			(parse_shard_name(entry->d_name, &generation) != 0 ||
			 generation >= current.generation))
			continue;
		if (count == capacity)
		{
			size_t next = capacity == 0 ? 16 : capacity * 2;

			if (next < capacity || next > SIZE_MAX / sizeof(*names) ||
				(grown = realloc(names, next * sizeof(*names))) == NULL)
				goto cleanup;
			names = grown;
			capacity = next;
		}
		memcpy(names[count++], entry->d_name, strlen(entry->d_name) + 1);
	}
	{
		int scan_errno = errno;
		int close_rc = closedir(dir);

		dir = NULL;
		if (scan_errno != 0 || close_rc != 0)
			goto cleanup;
	}
	for (size_t i = 0; i < count; i++)
		if (unlinkat(current.directory_fd, names[i], 0) != 0)
			goto cleanup;
	/* Always sync, including a retry after an ambiguous earlier fsync error. */
	if (getenv("PAGESTORE_TEST_FAIL_WALIDX_GC_FSYNC") != NULL)
	{
		errno = EIO;
		goto cleanup;
	}
	if (fsync(current.directory_fd) != 0)
		goto cleanup;
	rc = count != 0;

cleanup:
	free(names);
	if (dir != NULL)
		closedir(dir);
	else if (scan_fd >= 0)
		close(scan_fd);
	ps_walidx_snapshot_close(&current);
	return rc;
}

void
ps_walidx_snapshot_close(PsWalIdxSnapshot *snapshot)
{
	if (snapshot == NULL)
		return;
	if (snapshot->directory_fd >= 0)
		close(snapshot->directory_fd);
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->directory_fd = -1;
}
