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

#include "pagestore_wal_store.h"

static int
reserve_entry(PsWalStore *store)
{
	PsWalStoreEntry *grown;
	uint32_t capacity;

	if (store->nentries < store->capacity)
		return 0;
	capacity = store->capacity == 0 ? 8 : store->capacity * 2;
	grown = realloc(store->entries, (size_t) capacity * sizeof(*grown));
	if (grown == NULL)
		return -1;
	store->entries = grown;
	store->capacity = capacity;
	return 0;
}

static int
segment_name(const PsWalStore *store, uint64_t segment_no,
			 char *path, size_t path_len)
{
	int n = snprintf(path, path_len, "%s/walv1_%u_%020llu",
					 store->directory, store->timeline,
					 (unsigned long long) segment_no);

	return n < 0 || (size_t) n >= path_len ? -1 : 0;
}

static int
write_all(int fd, const void *data, size_t len)
{
	const unsigned char *bytes = data;
	size_t done = 0;

	while (done < len)
	{
		ssize_t n = write(fd, bytes + done, len - done);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		done += (size_t) n;
	}
	return 0;
}

static int
read_all_at(int fd, void *data, size_t len, off_t offset)
{
	unsigned char *bytes = data;
	size_t done = 0;

	while (done < len)
	{
		ssize_t n = pread(fd, bytes + done, len - done, offset + (off_t) done);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		done += (size_t) n;
	}
	return 0;
}

static int load_validated_segment(const PsWalStore *store,
								  const PsWalSegmentHeader *expected,
								  unsigned char **payload_out);

static int
publish_segment(PsWalStore *store, const PsWalSegmentHeader *header,
				const void *payload)
{
	char final_path[4096];
	char temporary[4096];
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	int fd = -1;
	int rc = -1;
	int n;

	if (segment_name(store, header->segment_no, final_path,
					 sizeof(final_path)) != 0)
		return -1;
	n = snprintf(temporary, sizeof(temporary), "%s.tmp.XXXXXX", final_path);
	if (n < 0 || (size_t) n >= sizeof(temporary))
		return -1;
	if (ps_wal_segment_encode(header, encoded) != 0)
		return -1;
	fd = mkstemp(temporary);
	if (fd < 0)
		return -1;
	if (write_all(fd, encoded, sizeof(encoded)) != 0 ||
		write_all(fd, payload, header->payload_len) != 0 || fsync(fd) != 0)
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
	/* link() is the no-overwrite publication point.  A retry cannot replace an
	 * immutable segment that already owns this timeline/sequence identity. */
	if (link(temporary, final_path) != 0)
		goto cleanup;
	(void) unlink(temporary);
	if (fsync(store->directory_fd) != 0)
	{
		(void) unlink(final_path);
		(void) fsync(store->directory_fd);
		goto cleanup;
	}
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		unlink(temporary);
	return rc;
}

int
ps_wal_store_create(PsWalStore *store, const char *directory,
					uint32_t timeline, uint64_t start_lsn)
{
	char path[4096];
	char temporary[4096];
	int created = 0;
	int parent_fd = -1;
	int n;

	if (store == NULL || directory == NULL)
		return -1;
	if (start_lsn % PS_WAL_SEGMENT_PAYLOAD_BYTES != 0)
		return -1;
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
	n = snprintf(store->directory, sizeof(store->directory), "%s", directory);
	store->timeline = timeline;
	if (n < 0 || (size_t) n >= sizeof(store->directory) ||
		segment_name(store, UINT64_MAX, path, sizeof(path)) != 0)
		return -1;
	n = snprintf(temporary, sizeof(temporary), "%s.tmp.XXXXXX", path);
	if (n < 0 || (size_t) n >= sizeof(temporary))
		return -1;
	if (mkdir(directory, 0700) == 0)
		created = 1;
	else if (errno != EEXIST)
		return -1;
	store->directory_fd = open(directory, O_RDONLY | O_DIRECTORY);
	if (store->directory_fd < 0)
		return -1;
	/* Always sync the parent, including EEXIST retries.  A prior attempt may
	 * have created the directory and failed before making that entry durable. */
	if (created && getenv("PAGESTORE_TEST_FAIL_WAL_PARENT_FSYNC") != NULL)
	{
		close(store->directory_fd);
		store->directory_fd = -1;
		return -1;
	}
	{
		parent_fd = openat(store->directory_fd, "..", O_RDONLY | O_DIRECTORY);
		if (parent_fd < 0 || fsync(parent_fd) != 0)
		{
			if (parent_fd >= 0)
				close(parent_fd);
			close(store->directory_fd);
			store->directory_fd = -1;
			return -1;
		}
		if (close(parent_fd) != 0)
		{
			close(store->directory_fd);
			store->directory_fd = -1;
			return -1;
		}
	}
	store->start_lsn = start_lsn;
	store->end_lsn = start_lsn;
	store->next_segment_no = start_lsn / PS_WAL_SEGMENT_PAYLOAD_BYTES;
	return 0;
}

/* Validate each immutable segment at most once, then compare the complete
 * overlapping range from that in-memory payload. */
static int
validate_committed_prefix(const PsWalStore *store, uint64_t start_lsn,
						  const unsigned char *bytes, uint32_t prefix)
{
	uint32_t done = 0;
	uint64_t end_lsn = start_lsn + prefix;

	for (uint32_t i = 0; i < store->nentries && done < prefix; i++)
	{
		const PsWalSegmentHeader *header = &store->entries[i].header;
		uint64_t segment_end = header->start_lsn + header->payload_len;
		uint64_t overlap_start = start_lsn > header->start_lsn ?
			start_lsn : header->start_lsn;
		uint64_t overlap_end = end_lsn < segment_end ? end_lsn : segment_end;

		if (overlap_start < overlap_end)
		{
			unsigned char *payload = NULL;
			size_t amount = (size_t) (overlap_end - overlap_start);

			if (load_validated_segment(store, header, &payload) != 0)
				return -1;
			if (memcmp(payload + (overlap_start - header->start_lsn),
					   bytes + (overlap_start - start_lsn), amount) != 0)
			{
				free(payload);
				return -1;
			}
			free(payload);
			done += (uint32_t) amount;
		}
	}
	return done == prefix ? 0 : -1;
}

int
ps_wal_store_append(PsWalStore *store, uint64_t start_lsn,
					const void *data, uint32_t len)
{
	const unsigned char *bytes = data;
	uint32_t done = 0;

	if (store == NULL || store->directory_fd < 0 || data == NULL || len == 0 ||
		start_lsn % PS_WAL_SEGMENT_PAYLOAD_BYTES != 0 ||
		len % PS_WAL_SEGMENT_PAYLOAD_BYTES != 0 ||
		start_lsn > store->end_lsn || start_lsn + len < start_lsn ||
		start_lsn + len < store->end_lsn)
		return -1;
	/* A prior split append may have durably published a prefix before a later
	 * segment failed.  Accept an identical retry and resume at end_lsn; reject
	 * divergent bytes so immutable WAL identity remains fail closed. */
	if (start_lsn < store->end_lsn)
	{
		uint32_t prefix = (uint32_t) (store->end_lsn - start_lsn);

		if (validate_committed_prefix(store, start_lsn, bytes, prefix) != 0)
			return -1;
		done = prefix;
	}
	while (done < len)
	{
		PsWalSegmentHeader header;
		uint32_t chunk = PS_WAL_SEGMENT_PAYLOAD_BYTES;
		uint64_t segment_start = start_lsn + done;

		if (segment_start % PS_WAL_SEGMENT_PAYLOAD_BYTES != 0 ||
			segment_start / PS_WAL_SEGMENT_PAYLOAD_BYTES != store->next_segment_no)
			return -1;
		{
			const char *fail_segment = getenv("PAGESTORE_TEST_FAIL_WAL_SEGMENT_NO");

			if (fail_segment != NULL &&
				strtoull(fail_segment, NULL, 10) == store->next_segment_no)
				return -1;
		}
		if (reserve_entry(store) != 0 ||
			ps_wal_segment_seal(&header, store->timeline,
								 store->next_segment_no, start_lsn + done,
								 bytes + done, chunk) != 0 ||
			publish_segment(store, &header, bytes + done) != 0)
			return -1;
		store->entries[store->nentries++].header = header;
		store->next_segment_no++;
		store->end_lsn += chunk;
		done += chunk;
	}
	return 0;
}

static int
load_validated_segment(const PsWalStore *store,
					   const PsWalSegmentHeader *expected,
					   unsigned char **payload_out)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	PsWalSegmentHeader actual;
	struct stat st;
	char path[4096];
	unsigned char *payload = NULL;
	int fd = -1;
	int rc = -1;

	if (segment_name(store, expected->segment_no, path, sizeof(path)) != 0 ||
		(fd = open(path, O_RDONLY)) < 0 || fstat(fd, &st) != 0 ||
		st.st_size != (off_t) (PS_WAL_SEGMENT_HEADER_BYTES +
						 expected->payload_len) ||
		read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
		ps_wal_segment_decode(&actual, encoded, sizeof(encoded)) != 0)
		goto cleanup;
	if (actual.timeline != expected->timeline ||
		actual.segment_no != expected->segment_no ||
		actual.start_lsn != expected->start_lsn ||
		actual.payload_len != expected->payload_len ||
		actual.payload_crc != expected->payload_crc)
		goto cleanup;
	payload = malloc(actual.payload_len);
	if (payload == NULL ||
		read_all_at(fd, payload, actual.payload_len,
					PS_WAL_SEGMENT_HEADER_BYTES) != 0 ||
		ps_wal_segment_validate(&actual, payload, actual.payload_len) != 0)
		goto cleanup;
	*payload_out = payload;
	payload = NULL;
	rc = 0;

cleanup:
	free(payload);
	if (fd >= 0)
		close(fd);
	return rc;
}

int
ps_wal_store_read(const PsWalStore *store, uint64_t start_lsn,
				  void *data, uint32_t len)
{
	unsigned char *out = data;
	uint64_t end_lsn;
	uint32_t done = 0;

	if (store == NULL || data == NULL || start_lsn + len < start_lsn)
		return -1;
	end_lsn = start_lsn + len;
	if (start_lsn < store->start_lsn || end_lsn > store->end_lsn)
		return -1;
	for (uint32_t i = 0; i < store->nentries && done < len; i++)
	{
		const PsWalSegmentHeader *header = &store->entries[i].header;
		uint64_t segment_end = header->start_lsn + header->payload_len;
		uint64_t overlap_start = start_lsn > header->start_lsn ?
			start_lsn : header->start_lsn;
		uint64_t overlap_end = end_lsn < segment_end ? end_lsn : segment_end;

		if (overlap_start < overlap_end)
		{
			unsigned char *payload = NULL;
			size_t amount = (size_t) (overlap_end - overlap_start);

			if (load_validated_segment(store, header, &payload) != 0)
				return -1;
			memcpy(out + (overlap_start - start_lsn),
				   payload + (overlap_start - header->start_lsn), amount);
			free(payload);
			done += (uint32_t) amount;
		}
	}
	return done == len ? 0 : -1;
}

void
ps_wal_store_close(PsWalStore *store)
{
	if (store == NULL)
		return;
	if (store->directory_fd >= 0)
		close(store->directory_fd);
	free(store->entries);
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
}
