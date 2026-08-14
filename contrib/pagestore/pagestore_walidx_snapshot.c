#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
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
#define VERIFY_BYTES (64u * 1024u)

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
published_shard_matches(int directory_fd, const char *name,
						const void *data, uint64_t len, uint32_t crc)
{
	unsigned char buf[VERIFY_BYTES];
	const unsigned char *expected = data;
	struct stat st;
	uint64_t done = 0;
	uint32_t actual_crc = 2166136261u;
	int fd = openat(directory_fd, name,
					O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);

	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size != len)
		goto fail;
	while (done < len)
	{
		size_t amount = len - done < sizeof(buf) ?
			(size_t) (len - done) : sizeof(buf);

		if (read_all_at(fd, buf, amount, done) != 0 ||
			memcmp(buf, expected + done, amount) != 0)
			goto fail;
		actual_crc = fnv1a(actual_crc, buf, amount);
		done += amount;
	}
	if (close(fd) != 0)
		return -1;
	return actual_crc == crc ? 0 : -1;

fail:
	if (fd >= 0)
		close(fd);
	return -1;
}

static int
publish_shard(int directory_fd, uint64_t generation, uint32_t shard,
			  const void *data, uint64_t len, uint32_t crc)
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
	if (write_all(fd, data, len) != 0 || fsync(fd) != 0)
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
								data, len, crc) != 0)
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

int
ps_walidx_snapshot_publish(const char *directory, uint32_t timeline,
						   uint64_t generation, uint64_t start_lsn,
						   uint64_t end_lsn,
						   const PsWalIdxSnapshotInput *inputs,
						   uint32_t nshards)
{
	PsWalIdxSnapshotShard shards[PS_WALIDX_SNAPSHOT_MAX_SHARDS];
	PsWalIdxSnapshot current;
	unsigned char *manifest;
	size_t manifest_len;
	int directory_fd;
	int rc = -1;

	if (directory == NULL || generation == 0 ||
		inputs == NULL || nshards == 0 ||
		nshards > PS_WALIDX_SNAPSHOT_MAX_SHARDS || end_lsn < start_lsn)
		return -1;
	for (uint32_t i = 0; i < nshards; i++)
	{
		if ((inputs[i].data == NULL && inputs[i].len != 0) ||
			inputs[i].len > (uint64_t) SIZE_MAX ||
			inputs[i].len > (uint64_t) INT64_MAX)
			return -1;
		shards[i].len = inputs[i].len;
		shards[i].crc = fnv1a(2166136261u, inputs[i].data,
								(size_t) inputs[i].len);
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

				if (shard_name(generation, i, name, sizeof(name)) != 0 ||
					published_shard_matches(current.directory_fd, name,
										inputs[i].data, inputs[i].len,
										shards[i].crc) != 0)
					identical = 0;
			}
			ps_walidx_snapshot_close(&current);
			if (identical)
			{
				close(directory_fd);
				return 0;
			}
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
		if (publish_shard(directory_fd, generation, i, inputs[i].data,
						  inputs[i].len, shards[i].crc) != 0)
			goto cleanup;
		{
			const char *fail_after = getenv("PAGESTORE_TEST_FAIL_WALIDX_AFTER_SHARD");

			if (fail_after != NULL && strtoul(fail_after, NULL, 10) == i)
				goto cleanup;
		}
	}
	manifest = encode_manifest(timeline, generation, start_lsn, end_lsn,
						   shards, nshards, &manifest_len);
	if (manifest == NULL)
		goto cleanup;
	if (publish_manifest(directory_fd, manifest, manifest_len, generation) == 0)
		rc = 0;
	free(manifest);

cleanup:
	close(directory_fd);
	return rc;
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
