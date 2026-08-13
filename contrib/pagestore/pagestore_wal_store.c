#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <dirent.h>
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
			 char *name, size_t name_len)
{
	int n = snprintf(name, name_len, "walv1_%u_%020llu", store->timeline,
					 (unsigned long long) segment_no);

	return n < 0 || (size_t) n >= name_len ? -1 : 0;
}

static int
segment_number_cmp(const void *left, const void *right)
{
	uint64_t a = *(const uint64_t *) left;
	uint64_t b = *(const uint64_t *) right;

	return a < b ? -1 : (a > b ? 1 : 0);
}

static int
list_segments(const PsWalStore *store, uint64_t **numbers_out,
			  uint32_t *count_out)
{
	char prefix[64];
	DIR *directory;
	uint64_t *numbers = NULL;
	uint32_t count = 0;
	uint32_t capacity = 0;
	char **orphans = NULL;
	uint32_t norphans = 0;
	uint32_t orphan_capacity = 0;
	int duplicated_fd;
	int rc = -1;
	int n;

	n = snprintf(prefix, sizeof(prefix), "walv1_%u_", store->timeline);
	if (n < 0 || (size_t) n >= sizeof(prefix))
		return -1;
	duplicated_fd = dup(store->directory_fd);
	if (duplicated_fd < 0)
		return -1;
	directory = fdopendir(duplicated_fd);
	if (directory == NULL)
	{
		close(duplicated_fd);
		return -1;
	}
	for (;;)
	{
		struct dirent *entry;
		const char *suffix;
		size_t suffix_len;
		uint64_t number = 0;

		errno = 0;
		entry = readdir(directory);
		if (entry == NULL)
		{
			if (errno != 0)
				goto cleanup;
			break;
		}

		if (strncmp(entry->d_name, prefix, (size_t) n) != 0)
		{
			/* Do not let a leading-zero spelling of this timeline hide a
			 * boundary segment and make recovery accept a shorter history. */
			if (strncmp(entry->d_name, "walv1_", 6) == 0)
			{
				const char *timeline_text = entry->d_name + 6;
				const char *separator = strchr(timeline_text, '_');
				char canonical[32];
				int canonical_len = snprintf(canonical, sizeof(canonical), "%u",
								 store->timeline);

				if (separator == NULL && canonical_len > 0 &&
					strcmp(timeline_text, canonical) == 0)
					goto cleanup;
				uint64_t parsed = 0;
				int signed_spelling = separator != NULL &&
					(*timeline_text == '+' || *timeline_text == '-');
				const char *digits = signed_spelling ? timeline_text + 1 : timeline_text;
				int valid = separator != NULL && separator != digits;

				for (const char *p = digits; valid && p < separator; p++)
				{
					unsigned int digit;

					if (*p < '0' || *p > '9')
					{
						valid = 0;
						break;
					}
					digit = (unsigned int) (*p - '0');
					if (parsed > (UINT64_MAX - digit) / 10)
					{
						valid = 0;
						break;
					}
					parsed = parsed * 10 + digit;
				}
				if (valid && parsed == store->timeline)
					goto cleanup;
			}
			continue;
		}
		suffix = entry->d_name + n;
		suffix_len = strlen(suffix);
		/* Published identities have exactly 20 decimal digits.  mkstemp orphan
		 * names add exactly `.tmp.` plus six ASCII alphanumerics.  Anything else
		 * in this timeline's reserved namespace is corruption, not debris. */
		if (suffix_len != 20 && suffix_len != 31)
			goto cleanup;
		for (int i = 0; i < 20; i++)
		{
			unsigned int digit;

			if (suffix[i] < '0' || suffix[i] > '9')
				goto cleanup;
			digit = (unsigned int) (suffix[i] - '0');
			if (number > (UINT64_MAX - digit) / 10)
				goto cleanup;
			number = number * 10 + digit;
		}
		if (suffix_len == 31)
		{
			if (strncmp(suffix + 20, ".tmp.", 5) != 0)
				goto cleanup;
			for (int i = 25; i < 31; i++)
				if (!((suffix[i] >= '0' && suffix[i] <= '9') ||
					  (suffix[i] >= 'A' && suffix[i] <= 'Z') ||
					  (suffix[i] >= 'a' && suffix[i] <= 'z')))
					goto cleanup;
			if (norphans == orphan_capacity)
			{
				uint32_t next = orphan_capacity == 0 ? 4 : orphan_capacity * 2;
				char **grown = realloc(orphans, (size_t) next * sizeof(*grown));

				if (grown == NULL)
					goto cleanup;
				orphans = grown;
				orphan_capacity = next;
			}
			orphans[norphans] = strdup(entry->d_name);
			if (orphans[norphans] == NULL)
				goto cleanup;
			norphans++;
			continue;
		}
		if (count == capacity)
		{
			uint32_t next_capacity = capacity == 0 ? 8 : capacity * 2;
			uint64_t *grown = realloc(numbers,
									 (size_t) next_capacity * sizeof(*grown));

			if (grown == NULL)
				goto cleanup;
			numbers = grown;
			capacity = next_capacity;
		}
		numbers[count++] = number;
	}
	if (closedir(directory) != 0)
	{
		directory = NULL;
		goto cleanup;
	}
	directory = NULL;
	for (uint32_t i = 0; i < norphans; i++)
		if (unlinkat(store->directory_fd, orphans[i], 0) != 0)
			goto cleanup;
	/* Also makes a final link durable when its publisher crashed after unlinking
	 * the temporary name but before its directory fsync. */
	if (fsync(store->directory_fd) != 0)
		goto cleanup;
	if (count > 1)
		qsort(numbers, count, sizeof(*numbers), segment_number_cmp);
	*numbers_out = numbers;
	*count_out = count;
	numbers = NULL;
	rc = 0;

cleanup:
	free(numbers);
	for (uint32_t i = 0; i < norphans; i++)
		free(orphans[i]);
	free(orphans);
	if (directory != NULL && closedir(directory) != 0)
		rc = -1;
	return rc;
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

static uint32_t
wal_payload_hash(uint32_t hash, const void *data, size_t len)
{
	const unsigned char *bytes = data;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= bytes[i];
		hash *= 16777619u;
	}
	return hash;
}

static int
build_chunk_hashes(const void *payload, uint32_t payload_len,
				   uint32_t **hashes_out, uint32_t *nchunks_out)
{
	const unsigned char *bytes = payload;
	uint32_t	nchunks;
	uint32_t   *hashes;

	nchunks = (payload_len + PS_WAL_STORE_VERIFY_CHUNK_BYTES - 1) /
		PS_WAL_STORE_VERIFY_CHUNK_BYTES;
	hashes = malloc((size_t) nchunks * sizeof(*hashes));
	if (hashes == NULL)
		return -1;
	for (uint32_t i = 0; i < nchunks; i++)
	{
		uint32_t off = i * PS_WAL_STORE_VERIFY_CHUNK_BYTES;
		uint32_t amount = payload_len - off < PS_WAL_STORE_VERIFY_CHUNK_BYTES ?
			payload_len - off : PS_WAL_STORE_VERIFY_CHUNK_BYTES;

		hashes[i] = wal_payload_hash(2166136261u, bytes + off, amount);
	}
	*hashes_out = hashes;
	*nchunks_out = nchunks;
	return 0;
}

static int
random_suffix(char suffix[7])
{
	static const char alphabet[] =
		"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	unsigned char random_bytes[6];
	size_t done = 0;
	int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);

	if (fd < 0)
		return -1;
	while (done < sizeof(random_bytes))
	{
		ssize_t n = read(fd, random_bytes + done, sizeof(random_bytes) - done);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
		{
			close(fd);
			return -1;
		}
		done += (size_t) n;
	}
	if (close(fd) != 0)
		return -1;
	for (size_t i = 0; i < sizeof(random_bytes); i++)
		suffix[i] = alphabet[random_bytes[i] % (sizeof(alphabet) - 1)];
	suffix[6] = '\0';
	return 0;
}

static int
open_regular_segment_at(const PsWalStore *store, const char *name,
						struct stat *st)
{
	int fd = openat(store->directory_fd, name,
				O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);

	if (fd < 0)
		return -1;
	if (fstat(fd, st) != 0 || !S_ISREG(st->st_mode))
	{
		close(fd);
		return -1;
	}
	return fd;
}

static int read_validated_segment_range(PsWalStore *store,
											const PsWalStoreEntry *entry,
										uint64_t range_off, unsigned char *out,
										const unsigned char *compare,
										size_t range_len);

static int
publish_segment(PsWalStore *store, const PsWalSegmentHeader *header,
				const void *payload)
{
	char final_name[128];
	char temporary[128];
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	int fd = -1;
	int rc = -1;
	int n;

	if (segment_name(store, header->segment_no, final_name,
					 sizeof(final_name)) != 0)
		return -1;
	if (ps_wal_segment_encode(header, encoded) != 0)
		return -1;
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		char suffix[7];

		if (random_suffix(suffix) != 0)
			return -1;
		n = snprintf(temporary, sizeof(temporary), "%s.tmp.%s",
					 final_name, suffix);
		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(store->directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
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
	if (linkat(store->directory_fd, temporary, store->directory_fd,
			   final_name, 0) != 0)
		goto cleanup;
	(void) unlinkat(store->directory_fd, temporary, 0);
	if (fsync(store->directory_fd) != 0)
	{
		(void) unlinkat(store->directory_fd, final_name, 0);
		(void) fsync(store->directory_fd);
		goto cleanup;
	}
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		unlinkat(store->directory_fd, temporary, 0);
	return rc;
}

int
ps_wal_store_create(PsWalStore *store, const char *directory,
					uint32_t timeline, uint64_t start_lsn,
					uint32_t segment_size)
{
	char path[128];
	int created = 0;
	int parent_fd = -1;
	int n;

	if (store == NULL || directory == NULL)
		return -1;
	if (segment_size < PS_WAL_SEGMENT_MIN_BYTES ||
		segment_size > PS_WAL_SEGMENT_MAX_BYTES ||
		(segment_size & (segment_size - 1)) != 0 ||
		start_lsn % segment_size != 0)
		return -1;
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
	n = snprintf(store->directory, sizeof(store->directory), "%s", directory);
	store->timeline = timeline;
	store->segment_size = segment_size;
	if (n < 0 || (size_t) n >= sizeof(store->directory) ||
		segment_name(store, UINT64_MAX, path, sizeof(path)) != 0)
		return -1;
	if (mkdir(directory, 0700) == 0)
		created = 1;
	else if (errno != EEXIST)
		return -1;
	store->directory_fd = open(directory,
					   O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
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
	store->next_segment_no = start_lsn / store->segment_size;
	return 0;
}

/* Start at the first overlapping entry.  Segment validation streams through a
 * bounded buffer; retry comparison never allocates the full payload. */
static int
validate_committed_prefix(PsWalStore *store, uint64_t start_lsn,
						  const unsigned char *bytes, uint32_t prefix)
{
	uint32_t done = 0;
	uint64_t end_lsn = start_lsn + prefix;

	for (uint32_t i = (uint32_t) ((start_lsn - store->start_lsn) /
			 store->segment_size); i < store->nentries && done < prefix; i++)
	{
		const PsWalStoreEntry *entry = &store->entries[i];
		const PsWalSegmentHeader *header = &entry->header;
		uint64_t segment_end = header->start_lsn + header->payload_len;
		uint64_t overlap_start = start_lsn > header->start_lsn ?
			start_lsn : header->start_lsn;
		uint64_t overlap_end = end_lsn < segment_end ? end_lsn : segment_end;

		if (overlap_start < overlap_end)
		{
			size_t amount = (size_t) (overlap_end - overlap_start);

			if (read_validated_segment_range(store, entry,
					overlap_start - header->start_lsn, NULL,
					bytes + (overlap_start - start_lsn), amount) != 0)
				return -1;
			done += (uint32_t) amount;
		}
	}
	return done == prefix ? 0 : -1;
}

int
ps_wal_store_open(PsWalStore *store, const char *directory, uint32_t timeline)
{
	uint64_t *numbers = NULL;
	uint32_t count = 0;
	unsigned char *payload = NULL;
	uint64_t expected_lsn = 0;
	int n;
	int rc = -1;

	if (store == NULL || directory == NULL)
		return -1;
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
	n = snprintf(store->directory, sizeof(store->directory), "%s", directory);
	if (n < 0 || (size_t) n >= sizeof(store->directory))
		return -1;
	store->directory_fd = open(directory, O_RDONLY | O_DIRECTORY);
	if (store->directory_fd < 0)
		return -1;
	store->timeline = timeline;
	if (list_segments(store, &numbers, &count) != 0 || count == 0)
		goto cleanup;
	for (uint32_t i = 0; i < count; i++)
	{
		char name[128];
		unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
		PsWalSegmentHeader header;
		struct stat st;
		int fd = -1;

		if (numbers[i] != numbers[0] + i ||
			segment_name(store, numbers[i], name, sizeof(name)) != 0 ||
			(fd = open_regular_segment_at(store, name, &st)) < 0 ||
			read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
			ps_wal_segment_decode(&header, encoded, sizeof(encoded)) != 0 ||
			header.payload_len != PS_WAL_SEGMENT_PAYLOAD_BYTES ||
			(uint64_t) st.st_size != PS_WAL_SEGMENT_HEADER_BYTES +
				header.payload_len)
		{
			if (fd >= 0)
				close(fd);
			goto cleanup;
		}
		payload = malloc(header.payload_len);
		if (payload == NULL ||
			read_all_at(fd, payload, header.payload_len,
						PS_WAL_SEGMENT_HEADER_BYTES) != 0)
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
		if (ps_wal_segment_validate(&header, payload, header.payload_len) != 0 ||
			header.timeline != timeline || header.segment_no != numbers[i] ||
			(i != 0 && header.start_lsn != expected_lsn) ||
			reserve_entry(store) != 0)
			goto cleanup;
		if (i == 0)
			store->start_lsn = header.start_lsn;
		expected_lsn = header.start_lsn + header.payload_len;
		store->entries[store->nentries++].header = header;
		free(payload);
		payload = NULL;
	}
	store->next_segment_no = numbers[0] + count;
	store->end_lsn = expected_lsn;
	rc = 0;

cleanup:
	free(payload);
	free(numbers);
	if (rc != 0)
		ps_wal_store_close(store);
	return rc;
}

int
ps_wal_store_append(PsWalStore *store, uint64_t start_lsn,
					const void *data, uint32_t len)
{
	const unsigned char *bytes = data;
	uint32_t done = 0;

	if (store == NULL || store->directory_fd < 0 || data == NULL || len == 0 ||
		start_lsn % store->segment_size != 0 ||
		len % store->segment_size != 0 ||
		start_lsn < store->start_lsn ||
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
		uint32_t   *chunk_hashes = NULL;
		uint32_t	nchunks = 0;
		uint32_t chunk = store->segment_size;
		uint64_t segment_start = start_lsn + done;

		if (segment_start % store->segment_size != 0 ||
			segment_start / store->segment_size != store->next_segment_no)
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
								 store->segment_size, bytes + done, chunk) != 0 ||
			build_chunk_hashes(bytes + done, chunk, &chunk_hashes, &nchunks) != 0)
			return -1;
		if (publish_segment(store, &header, bytes + done) != 0)
		{
			free(chunk_hashes);
			return -1;
		}
		store->entries[store->nentries].header = header;
		store->entries[store->nentries].chunk_hashes = chunk_hashes;
		store->entries[store->nentries].nchunks = nchunks;
		store->nentries++;
		store->next_segment_no++;
		store->end_lsn += chunk;
		done += chunk;
	}
	return 0;
}

static int
read_validated_segment_range(PsWalStore *store,
							 const PsWalStoreEntry *entry,
						 uint64_t range_off, unsigned char *out,
						 const unsigned char *compare, size_t range_len)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	unsigned char buf[PS_WAL_STORE_VERIFY_CHUNK_BYTES];
	const PsWalSegmentHeader *expected = &entry->header;
	PsWalSegmentHeader actual;
	struct stat st;
	char name[128];
	uint64_t range_end;
	uint32_t first_chunk;
	uint32_t last_chunk;
	int fd = -1;
	int rc = -1;

	if (segment_name(store, expected->segment_no, name, sizeof(name)) != 0 ||
		(fd = open_regular_segment_at(store, name, &st)) < 0)
		goto cleanup;
	if (st.st_size != (off_t) (PS_WAL_SEGMENT_HEADER_BYTES +
						 expected->payload_len) ||
		read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
		ps_wal_segment_decode(&actual, encoded, sizeof(encoded)) != 0)
		goto cleanup;
	if (actual.timeline != expected->timeline ||
		actual.segment_no != expected->segment_no ||
		actual.start_lsn != expected->start_lsn ||
		actual.payload_len != expected->payload_len ||
		actual.segment_size != expected->segment_size ||
		actual.payload_crc != expected->payload_crc)
		goto cleanup;
	if (range_len == 0 || range_off + range_len < range_off ||
		range_off + range_len > actual.payload_len)
		goto cleanup;
	range_end = range_off + range_len;
	first_chunk = (uint32_t) (range_off / PS_WAL_STORE_VERIFY_CHUNK_BYTES);
	last_chunk = (uint32_t) ((range_end - 1) /
							 PS_WAL_STORE_VERIFY_CHUNK_BYTES);
	if (entry->chunk_hashes == NULL ||
		entry->nchunks != (actual.payload_len + PS_WAL_STORE_VERIFY_CHUNK_BYTES - 1) /
			PS_WAL_STORE_VERIFY_CHUNK_BYTES || last_chunk >= entry->nchunks)
		goto cleanup;
	for (uint32_t chunk_no = first_chunk; chunk_no <= last_chunk; chunk_no++)
	{
		uint64_t chunk_start = (uint64_t) chunk_no *
			PS_WAL_STORE_VERIFY_CHUNK_BYTES;
		size_t amount = actual.payload_len - chunk_start < sizeof(buf) ?
			(size_t) (actual.payload_len - chunk_start) : sizeof(buf);
		uint64_t chunk_end = chunk_start + amount;
		uint64_t copy_start = range_off > chunk_start ? range_off : chunk_start;
		uint64_t copy_end = range_end < chunk_end ? range_end : chunk_end;

		if (read_all_at(fd, buf, amount,
				PS_WAL_SEGMENT_HEADER_BYTES + (off_t) chunk_start) != 0 ||
			wal_payload_hash(2166136261u, buf, amount) !=
				entry->chunk_hashes[chunk_no])
			goto cleanup;
		if (copy_start < copy_end)
		{
			size_t copy_len = (size_t) (copy_end - copy_start);
			const unsigned char *source = buf + (copy_start - chunk_start);
			size_t target_off = (size_t) (copy_start - range_off);

			if (out != NULL)
				memcpy(out + target_off, source, copy_len);
			if (compare != NULL &&
				memcmp(source, compare + target_off, copy_len) != 0)
				goto cleanup;
		}
	}
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	return rc;
}

int
ps_wal_store_read(PsWalStore *store, uint64_t start_lsn,
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
	for (uint32_t i = (uint32_t) ((start_lsn - store->start_lsn) /
			 store->segment_size); i < store->nentries && done < len; i++)
	{
		const PsWalStoreEntry *entry = &store->entries[i];
		const PsWalSegmentHeader *header = &entry->header;
		uint64_t segment_end = header->start_lsn + header->payload_len;
		uint64_t overlap_start = start_lsn > header->start_lsn ?
			start_lsn : header->start_lsn;
		uint64_t overlap_end = end_lsn < segment_end ? end_lsn : segment_end;

		if (overlap_start < overlap_end)
		{
			size_t amount = (size_t) (overlap_end - overlap_start);

			if (read_validated_segment_range(store, entry,
					overlap_start - header->start_lsn,
					out + (overlap_start - start_lsn), NULL, amount) != 0)
				return -1;
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
	for (uint32_t i = 0; i < store->nentries; i++)
		free(store->entries[i].chunk_hashes);
	free(store->entries);
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
}
