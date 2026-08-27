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

static int segment_name(const PsWalStore *store, uint64_t segment_no,
						char *name, size_t name_len);
static int write_all(int fd, const void *data, size_t len);
static int read_all_at(int fd, void *data, size_t len, off_t offset);
static int random_suffix(char suffix[7]);

#define PS_WAL_STORE_IDENTITY_BYTES 128
#define PS_WAL_STORE_METADATA_BYTES 64
#define PS_WAL_STORE_METADATA_MAGIC 0x4d535732u /* "MSW2" */
#define PS_WAL_STORE_METADATA_VERSION 2u

typedef struct PsWalStoreMetadata
{
	uint32_t timeline;
	uint32_t segment_size;
	uint64_t directory_start_lsn;
	uint64_t retained_base_lsn;
	uint64_t end_lsn;
} PsWalStoreMetadata;

static uint32_t
metadata_hash(const unsigned char *data, size_t len)
{
	uint32_t hash = 2166136261u;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= data[i];
		hash *= 16777619u;
	}
	return hash;
}

static uint32_t
metadata_crc(const unsigned char encoded[PS_WAL_STORE_METADATA_BYTES])
{
	unsigned char copy[PS_WAL_STORE_METADATA_BYTES];

	memcpy(copy, encoded, sizeof(copy));
	memset(copy + 48, 0, 4);
	return metadata_hash(copy, sizeof(copy));
}

static void
put_metadata_le32(unsigned char *p, uint32_t value)
{
	for (unsigned int i = 0; i < 4; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static void
put_metadata_le64(unsigned char *p, uint64_t value)
{
	for (unsigned int i = 0; i < 8; i++)
		p[i] = (unsigned char) (value >> (i * 8));
}

static uint32_t
get_metadata_le32(const unsigned char *p)
{
	return (uint32_t) p[0] | (uint32_t) p[1] << 8 |
		(uint32_t) p[2] << 16 | (uint32_t) p[3] << 24;
}

static uint64_t
get_metadata_le64(const unsigned char *p)
{
	return (uint64_t) get_metadata_le32(p) |
		(uint64_t) get_metadata_le32(p + 4) << 32;
}

static void
encode_metadata(const PsWalStoreMetadata *metadata,
				unsigned char encoded[PS_WAL_STORE_METADATA_BYTES])
{
	memset(encoded, 0, PS_WAL_STORE_METADATA_BYTES);
	put_metadata_le32(encoded + 0, PS_WAL_STORE_METADATA_MAGIC);
	put_metadata_le32(encoded + 4, PS_WAL_STORE_METADATA_VERSION);
	put_metadata_le32(encoded + 8, PS_WAL_STORE_METADATA_BYTES);
	put_metadata_le32(encoded + 16, metadata->timeline);
	put_metadata_le32(encoded + 20, metadata->segment_size);
	put_metadata_le64(encoded + 24, metadata->directory_start_lsn);
	put_metadata_le64(encoded + 32, metadata->retained_base_lsn);
	put_metadata_le64(encoded + 40, metadata->end_lsn);
	put_metadata_le32(encoded + 48, metadata_crc(encoded));
}

static int
decode_metadata(PsWalStoreMetadata *metadata,
				const unsigned char encoded[PS_WAL_STORE_METADATA_BYTES])
{
	if (get_metadata_le32(encoded + 0) != PS_WAL_STORE_METADATA_MAGIC ||
		get_metadata_le32(encoded + 4) != PS_WAL_STORE_METADATA_VERSION ||
		get_metadata_le32(encoded + 8) != PS_WAL_STORE_METADATA_BYTES ||
		get_metadata_le32(encoded + 12) != 0 ||
		get_metadata_le32(encoded + 48) != metadata_crc(encoded) ||
		get_metadata_le32(encoded + 52) != 0 ||
		get_metadata_le64(encoded + 56) != 0)
		return -1;
	metadata->timeline = get_metadata_le32(encoded + 16);
	metadata->segment_size = get_metadata_le32(encoded + 20);
	metadata->directory_start_lsn = get_metadata_le64(encoded + 24);
	metadata->retained_base_lsn = get_metadata_le64(encoded + 32);
	metadata->end_lsn = get_metadata_le64(encoded + 40);
	if (metadata->timeline == 0 ||
		metadata->segment_size < PS_WAL_SEGMENT_MIN_BYTES ||
		metadata->segment_size > PS_WAL_SEGMENT_MAX_BYTES ||
		(metadata->segment_size & (metadata->segment_size - 1)) != 0 ||
		metadata->directory_start_lsn % metadata->segment_size != 0 ||
		metadata->retained_base_lsn % metadata->segment_size != 0 ||
		metadata->end_lsn % metadata->segment_size != 0 ||
		metadata->directory_start_lsn > metadata->retained_base_lsn ||
		metadata->retained_base_lsn > metadata->end_lsn ||
		metadata->end_lsn > UINT64_MAX - metadata->segment_size)
		return -1;
	return 0;
}

static int
read_store_metadata_fd(int directory_fd, PsWalStoreMetadata *metadata)
{
	unsigned char encoded[PS_WAL_STORE_METADATA_BYTES];
	struct stat st;
	int fd;

	fd = openat(directory_fd, PS_WAL_STORE_IDENTITY_FILE,
				O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size != (off_t) sizeof(encoded))
	{
		if (fd >= 0)
			close(fd);
		return -1;
	}
	if (read_all_at(fd, encoded, sizeof(encoded), 0) != 0)
	{
		close(fd);
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	return decode_metadata(metadata, encoded);
}

static int
parse_decimal_u64(const char **cursor, uint64_t *value)
{
	const unsigned char *p = (const unsigned char *) *cursor;
	uint64_t parsed = 0;
	int digits = 0;

	while (*p >= '0' && *p <= '9')
	{
		uint64_t digit = (uint64_t) (*p - '0');

		if (parsed > (UINT64_MAX - digit) / 10)
			return -1;
		parsed = parsed * 10 + digit;
		p++;
		digits = 1;
	}
	if (!digits)
		return -1;
	*cursor = (const char *) p;
	*value = parsed;
	return 0;
}

static int
read_store_identity_v1_fd(int directory_fd, uint32_t *timeline,
						  uint64_t *start_lsn, uint32_t *segment_size)
{
	char buf[PS_WAL_STORE_IDENTITY_BYTES];
	struct stat st;
	const char *cursor;
	uint64_t parsed_timeline;
	uint64_t parsed_segment_size;
	uint64_t parsed_start;
	int fd;

	fd = openat(directory_fd, PS_WAL_STORE_IDENTITY_FILE,
				O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size <= 0 || st.st_size >= (off_t) sizeof(buf))
	{
		if (fd >= 0)
			close(fd);
		return -1;
	}
	if (read_all_at(fd, buf, (size_t) st.st_size, 0) != 0)
	{
		close(fd);
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	buf[st.st_size] = '\0';
	cursor = buf;
	if (strncmp(cursor, "PSWALSTORE1 ", 12) != 0)
		return -1;
	cursor += 12;
	if (parse_decimal_u64(&cursor, &parsed_timeline) != 0 ||
		*cursor++ != ' ' || parse_decimal_u64(&cursor, &parsed_segment_size) != 0 ||
		*cursor++ != ' ' || parse_decimal_u64(&cursor, &parsed_start) != 0 ||
		*cursor++ != '\n' || *cursor != '\0' ||
		parsed_timeline == 0 || parsed_timeline > UINT32_MAX ||
		parsed_segment_size > UINT32_MAX ||
		parsed_segment_size < PS_WAL_SEGMENT_MIN_BYTES ||
		parsed_segment_size > PS_WAL_SEGMENT_MAX_BYTES ||
		(parsed_segment_size & (parsed_segment_size - 1)) != 0 ||
		parsed_start % parsed_segment_size != 0 ||
		parsed_start > UINT64_MAX - parsed_segment_size)
		return -1;
	*timeline = (uint32_t) parsed_timeline;
	*segment_size = (uint32_t) parsed_segment_size;
	*start_lsn = parsed_start;
	return 0;
}

static int
metadata_matches(const PsWalStoreMetadata *metadata, uint32_t timeline,
				 uint64_t directory_start_lsn, uint64_t retained_base_lsn,
				 uint64_t end_lsn, uint32_t segment_size)
{
	return metadata->timeline == timeline &&
		metadata->directory_start_lsn == directory_start_lsn &&
		metadata->retained_base_lsn == retained_base_lsn &&
		metadata->end_lsn == end_lsn &&
		metadata->segment_size == segment_size ? 0 : -1;
}

static int
publish_store_metadata(PsWalStore *store, uint64_t directory_start_lsn,
					   uint64_t retained_base_lsn, uint64_t end_lsn)
{
	PsWalStoreMetadata metadata;
	unsigned char encoded[PS_WAL_STORE_METADATA_BYTES];
	char temporary[128] = {0};
	int fd = -1;
	int rc = -1;

	if (directory_start_lsn > retained_base_lsn ||
		retained_base_lsn > end_lsn ||
		directory_start_lsn % store->segment_size != 0 ||
		retained_base_lsn % store->segment_size != 0 ||
		directory_start_lsn > UINT64_MAX - store->segment_size ||
		end_lsn < store->start_lsn ||
		end_lsn % store->segment_size != 0 ||
		end_lsn > UINT64_MAX - store->segment_size)
		return -1;
	metadata.timeline = store->timeline;
	metadata.segment_size = store->segment_size;
	metadata.directory_start_lsn = directory_start_lsn;
	metadata.retained_base_lsn = retained_base_lsn;
	metadata.end_lsn = end_lsn;
	encode_metadata(&metadata, encoded);
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		char suffix[7];
		int n;

		if (random_suffix(suffix) != 0)
			return -1;
		n = snprintf(temporary, sizeof(temporary), "%s.tmp.%s",
					 PS_WAL_STORE_IDENTITY_FILE, suffix);
		if (n < 0 || (size_t) n >= sizeof(temporary))
			return -1;
		fd = openat(store->directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0)
		return -1;
	if (getenv("PAGESTORE_TEST_FAIL_WAL_METADATA_BEFORE_RENAME") != NULL ||
		write_all(fd, encoded, sizeof(encoded)) != 0 || fsync(fd) != 0)
		goto cleanup;
	if (close(fd) != 0)
	{
		fd = -1;
		goto cleanup;
	}
	fd = -1;
	if (renameat(store->directory_fd, temporary, store->directory_fd,
				 PS_WAL_STORE_IDENTITY_FILE) != 0)
		goto cleanup;
	memset(temporary, 0, sizeof(temporary));
	if (getenv("PAGESTORE_TEST_FAIL_WAL_METADATA_DIR_FSYNC") != NULL ||
		fsync(store->directory_fd) != 0)
	{
		/* The rename outcome is intentionally not enough to authorize a
		 * frontier.  The current process must fence every caller until reopen
		 * observes a crash-consistent metadata image. */
		store->metadata_fenced = 1;
		return -1;
	}
	return 0;

cleanup:
	if (fd >= 0)
		close(fd);
	if (temporary[0] != '\0')
		(void) unlinkat(store->directory_fd, temporary, 0);
	return rc;
}

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
initialize_store(PsWalStore *store, const char *directory, uint32_t timeline,
				 uint64_t start_lsn, uint32_t segment_size)
{
	char path[128];
	int n;

	if (store == NULL || directory == NULL || timeline == 0 ||
		segment_size < PS_WAL_SEGMENT_MIN_BYTES ||
		segment_size > PS_WAL_SEGMENT_MAX_BYTES ||
		(segment_size & (segment_size - 1)) != 0 ||
		start_lsn % segment_size != 0 ||
		start_lsn > UINT64_MAX - segment_size)
		return -1;
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
	n = snprintf(store->directory, sizeof(store->directory), "%s", directory);
	store->timeline = timeline;
	store->segment_size = segment_size;
	if (pthread_mutex_init(&store->lock, NULL) != 0)
		return -1;
	store->lock_initialized = 1;
	if (n < 0 || (size_t) n >= sizeof(store->directory) ||
		segment_name(store, UINT64_MAX, path, sizeof(path)) != 0)
	{
		pthread_mutex_destroy(&store->lock);
		store->lock_initialized = 0;
		return -1;
	}
	store->start_lsn = start_lsn;
	store->retained_base_lsn = start_lsn;
	store->end_lsn = start_lsn;
	store->next_segment_no = start_lsn / segment_size;
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
build_chunk_hashes_and_payload_crc(const void *payload, uint32_t payload_len,
									 uint32_t **hashes_out, uint32_t *nchunks_out,
									 uint32_t *payload_crc_out)
{
	const unsigned char *bytes = payload;
	uint32_t	nchunks;
	uint32_t   *hashes;

	nchunks = (payload_len + PS_WAL_STORE_VERIFY_CHUNK_BYTES - 1) /
		PS_WAL_STORE_VERIFY_CHUNK_BYTES;
	hashes = malloc((size_t) nchunks * sizeof(*hashes));
	if (hashes == NULL)
		return -1;
	*payload_crc_out = 2166136261u;
	for (uint32_t i = 0; i < nchunks; i++)
	{
		uint32_t off = i * PS_WAL_STORE_VERIFY_CHUNK_BYTES;
		uint32_t amount = payload_len - off < PS_WAL_STORE_VERIFY_CHUNK_BYTES ?
			payload_len - off : PS_WAL_STORE_VERIFY_CHUNK_BYTES;
		uint32_t chunk_hash = 2166136261u;

		/* Keep the payload and per-chunk FNV states in one pass. */
		for (uint32_t j = 0; j < amount; j++)
		{
			chunk_hash ^= bytes[off + j];
			chunk_hash *= 16777619u;
			*payload_crc_out ^= bytes[off + j];
			*payload_crc_out *= 16777619u;
		}
		hashes[i] = chunk_hash;
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

static int read_validated_segment_range(PsWalStore *store,
																const PsWalStoreEntry *entry,
																uint64_t range_off, unsigned char *out,
																const unsigned char *compare,
																size_t range_len);

/* A directory fsync can fail after linkat() has exposed the final name.  Before
 * reporting failure, determine whether that name is the immutable segment we
 * were publishing, so an otherwise successful retry cannot be wedged by EEXIST. */
static int
published_segment_matches(PsWalStore *store, const PsWalSegmentHeader *expected,
						  const void *payload)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	unsigned char buf[PS_WAL_STORE_VERIFY_CHUNK_BYTES];
	const unsigned char *bytes = payload;
	PsWalSegmentHeader actual;
	struct stat st;
	char name[128];
	uint32_t done = 0;
	int fd = -1;
	int rc = -1;

	if (segment_name(store, expected->segment_no, name, sizeof(name)) != 0 ||
		(fd = openat(store->directory_fd, name,
				 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 ||
		!S_ISREG(st.st_mode) ||
		st.st_size != (off_t) (PS_WAL_SEGMENT_HEADER_BYTES + expected->payload_len) ||
		read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
		ps_wal_segment_decode(&actual, encoded, sizeof(encoded)) != 0 ||
		actual.timeline != expected->timeline ||
		actual.segment_no != expected->segment_no ||
		actual.start_lsn != expected->start_lsn ||
		actual.payload_len != expected->payload_len ||
		actual.segment_size != expected->segment_size ||
		actual.payload_crc != expected->payload_crc)
		goto cleanup;
	while (done < expected->payload_len)
	{
		size_t amount = expected->payload_len - done < sizeof(buf) ?
			expected->payload_len - done : sizeof(buf);

		if (read_all_at(fd, buf, amount,
				PS_WAL_SEGMENT_HEADER_BYTES + (off_t) done) != 0 ||
			memcmp(buf, bytes + done, amount) != 0)
			goto cleanup;
		done += (uint32_t) amount;
	}
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	return rc;
}

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
	{
		/* A previous ambiguous publication can leave our final name behind.
		 * Reconcile it before treating EEXIST as a permanent append failure. */
		if (errno == EEXIST &&
			published_segment_matches(store, header, payload) == 0)
		{
			/* The existing immutable name is ours.  Remove our staging
			 * entry before syncing the directory so its deletion is durable
			 * too; otherwise a crash can resurrect a full-size .tmp file. */
			if (unlinkat(store->directory_fd, temporary, 0) == 0 &&
				fsync(store->directory_fd) == 0)
				rc = 0;
		}
		goto cleanup;
	}
	if (unlinkat(store->directory_fd, temporary, 0) != 0 ||
		fsync(store->directory_fd) != 0)
	{
		if (published_segment_matches(store, header, payload) == 0 &&
			fsync(store->directory_fd) == 0)
		{
			rc = 0;
			goto cleanup;
		}
		/* The published identity is not ours.  Do not unlink an immutable name
		 * that may have become durable despite the failed directory fsync. */
		goto cleanup;
	}
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	/* The final name is immutable once linked.  Whether publication succeeded
	 * directly or was reconciled after EEXIST, our private staging name must
	 * never remain as an orphaned full segment. */
	(void) unlinkat(store->directory_fd, temporary, 0);
	return rc;
}

int
ps_wal_store_create(PsWalStore *store, const char *directory,
					uint32_t timeline, uint64_t start_lsn,
					uint32_t segment_size)
{
	int created = 0;
	int parent_fd = -1;

	if (initialize_store(store, directory, timeline, start_lsn,
					 segment_size) != 0)
		return -1;
	if (mkdir(directory, 0700) == 0)
		created = 1;
	else if (errno != EEXIST)
	{
		ps_wal_store_close(store);
		return -1;
	}
	if (!created)
	{
		/* An EEXIST retry must validate the existing identity and directory
		 * before accepting it.  This is also the safe path for a legacy v1
		 * directory whose first create attempt was interrupted. */
		ps_wal_store_close(store);
		return ps_wal_store_open(store, directory, timeline, start_lsn,
								 segment_size);
	}
	store->directory_fd = open(directory,
							 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (store->directory_fd < 0)
	{
		ps_wal_store_close(store);
		return -1;
	}
	if (publish_store_metadata(store, store->start_lsn,
							   store->start_lsn, store->end_lsn) != 0)
	{
		close(store->directory_fd);
		store->directory_fd = -1;
		pthread_mutex_destroy(&store->lock);
		store->lock_initialized = 0;
		return -1;
	}
	/* Always sync the parent, including EEXIST retries.  A prior attempt may
	 * have created the directory and failed before making that entry durable. */
	if (created && getenv("PAGESTORE_TEST_FAIL_WAL_PARENT_FSYNC") != NULL)
	{
		close(store->directory_fd);
		store->directory_fd = -1;
		pthread_mutex_destroy(&store->lock);
		store->lock_initialized = 0;
		return -1;
	}
	{
		parent_fd = openat(store->directory_fd, "..",
						   O_RDONLY | O_DIRECTORY | O_CLOEXEC);
		if (parent_fd < 0 || fsync(parent_fd) != 0)
		{
			if (parent_fd >= 0)
			close(parent_fd);
			close(store->directory_fd);
			store->directory_fd = -1;
			pthread_mutex_destroy(&store->lock);
			store->lock_initialized = 0;
			return -1;
		}
		if (close(parent_fd) != 0)
		{
			close(store->directory_fd);
			store->directory_fd = -1;
			pthread_mutex_destroy(&store->lock);
			store->lock_initialized = 0;
			return -1;
		}
	}
	return 0;
}

static int
load_segment(PsWalStore *store, uint64_t segment_no)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	unsigned char buf[PS_WAL_STORE_VERIFY_CHUNK_BYTES];
	PsWalSegmentHeader header;
	struct stat st;
	char name[128];
	uint32_t *hashes = NULL;
	uint32_t nchunks;
	uint32_t payload_crc = 2166136261u;
	uint32_t done = 0;
	int fd = -1;
	int rc = -1;

	if (segment_no > UINT64_MAX / store->segment_size)
		return -1;
	if (segment_name(store, segment_no, name, sizeof(name)) != 0 ||
		(fd = openat(store->directory_fd, name,
					 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
		ps_wal_segment_decode(&header, encoded, sizeof(encoded)) != 0 ||
		header.timeline != store->timeline ||
		header.segment_no != segment_no ||
		header.start_lsn != store->end_lsn ||
		header.segment_size != store->segment_size ||
		header.payload_len != store->segment_size ||
		st.st_size != (off_t) (PS_WAL_SEGMENT_HEADER_BYTES +
							 header.payload_len))
		goto cleanup;
	nchunks = (header.payload_len + PS_WAL_STORE_VERIFY_CHUNK_BYTES - 1) /
		PS_WAL_STORE_VERIFY_CHUNK_BYTES;
	hashes = malloc((size_t) nchunks * sizeof(*hashes));
	if (hashes == NULL || reserve_entry(store) != 0)
		goto cleanup;
	for (uint32_t i = 0; i < nchunks; i++)
	{
		uint32_t amount = header.payload_len - done < sizeof(buf) ?
			header.payload_len - done : (uint32_t) sizeof(buf);

		if (read_all_at(fd, buf, amount,
				PS_WAL_SEGMENT_HEADER_BYTES + (off_t) done) != 0)
			goto cleanup;
		hashes[i] = wal_payload_hash(2166136261u, buf, amount);
		payload_crc = wal_payload_hash(payload_crc, buf, amount);
		done += amount;
	}
	if (payload_crc != header.payload_crc)
		goto cleanup;
	store->entries[store->nentries].header = header;
	store->entries[store->nentries].chunk_hashes = hashes;
	store->entries[store->nentries].nchunks = nchunks;
	hashes = NULL;
	store->nentries++;
	store->next_segment_no++;
	store->end_lsn += header.payload_len;
	rc = 0;

cleanup:
	free(hashes);
	if (fd >= 0)
		close(fd);
	return rc;
}

/* Validate an already-authorized prefix segment without adding it to the
 * logical catalog.  The next reclaimer may unlink these files after the
 * frontier metadata is durable, so reopen must tolerate their presence while
 * still refusing an incorrectly named or corrupted prefix.
 *
 * Suffix reconciliation below has the same protocol boundary: this directory
 * is private to one timeline/incarnation cleanup writer, and only
 * publish_segment creates a legal final segment.  A contiguous suffix with
 * valid headers and CRCs is therefore the only evidence accepted for a
 * pre-metadata publication crash; gaps, unexpected names, and corruption
 * remain fail-closed. */
static int
validate_prefix_segment(PsWalStore *store, uint64_t segment_no)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	unsigned char buf[PS_WAL_STORE_VERIFY_CHUNK_BYTES];
	PsWalSegmentHeader header;
	struct stat st;
	char name[128];
	uint32_t done = 0;
	uint32_t payload_crc = 2166136261u;
	int fd = -1;
	int rc = -1;

	if (segment_name(store, segment_no, name, sizeof(name)) != 0 ||
		(fd = openat(store->directory_fd, name,
					 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		read_all_at(fd, encoded, sizeof(encoded), 0) != 0 ||
		ps_wal_segment_decode(&header, encoded, sizeof(encoded)) != 0 ||
		header.timeline != store->timeline ||
		header.segment_no != segment_no ||
		header.start_lsn != segment_no * store->segment_size ||
		header.segment_size != store->segment_size ||
		header.payload_len != store->segment_size ||
		st.st_size != (off_t) (PS_WAL_SEGMENT_HEADER_BYTES +
											 header.payload_len))
		goto cleanup;
	while (done < header.payload_len)
	{
		uint32_t amount = header.payload_len - done < sizeof(buf) ?
			header.payload_len - done : (uint32_t) sizeof(buf);

		if (read_all_at(fd, buf, amount,
				PS_WAL_SEGMENT_HEADER_BYTES + (off_t) done) != 0)
			goto cleanup;
		payload_crc = wal_payload_hash(payload_crc, buf, amount);
		done += amount;
	}
	if (payload_crc != header.payload_crc)
		goto cleanup;
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	return rc;
}

static void
install_retained_frontier(PsWalStore *store, uint64_t retained_base_lsn)
{
	uint64_t drop64;
	uint32_t drop;

	if (retained_base_lsn == store->start_lsn)
	{
		store->retained_base_lsn = retained_base_lsn;
		return;
	}
	drop64 = (retained_base_lsn - store->start_lsn) / store->segment_size;
	drop = drop64 > store->nentries ? store->nentries : (uint32_t) drop64;
	for (uint32_t i = 0; i < drop; i++)
		free(store->entries[i].chunk_hashes);
	if (drop < store->nentries)
		memmove(store->entries, store->entries + drop,
				(size_t) (store->nentries - drop) * sizeof(*store->entries));
	store->nentries -= drop;
	store->start_lsn = retained_base_lsn;
	store->retained_base_lsn = retained_base_lsn;
}

/* Resolve an error after metadata rename.  If the new record is visible, the
 * caller must adopt it rather than restore the old in-memory frontier.  If no
 * coherent old or new record can be observed, fence all operations until
 * reopen makes the durable state authoritative. */
static int
reconcile_metadata_failure(PsWalStore *store, uint64_t old_start_lsn,
							uint64_t old_base_lsn, uint64_t old_end_lsn,
							uint64_t new_start_lsn, uint64_t new_base_lsn,
							uint64_t new_end_lsn)
{
	PsWalStoreMetadata metadata;

	if (store->metadata_fenced)
		return -1;
	if (read_store_metadata_fd(store->directory_fd, &metadata) == 0 &&
		metadata_matches(&metadata, store->timeline, new_start_lsn,
											 new_base_lsn, new_end_lsn, store->segment_size) == 0)
	{
		install_retained_frontier(store, new_base_lsn);
		return 1;
	}
	if (read_store_metadata_fd(store->directory_fd, &metadata) == 0 &&
		metadata_matches(&metadata, store->timeline, old_start_lsn,
											 old_base_lsn, old_end_lsn, store->segment_size) == 0)
		return 0;
	store->metadata_fenced = 1;
	return -1;
}

int
ps_wal_store_open(PsWalStore *store, const char *directory,
				  uint32_t timeline, uint64_t start_lsn,
				  uint32_t segment_size)
{
	struct dirent *de;
	DIR *dir = NULL;
	uint64_t count = 0;
	uint64_t prefix_first = UINT64_MAX;
	uint64_t prefix_count = 0;
	char prefix[64];
	int prefix_len;
	int parent_fd = -1;
	int scan_fd = -1;
	int identity_missing = 0;
	int identity_legacy = 0;
	PsWalStoreMetadata metadata;
	int removed_temporary = 0;
	int rc = -1;

	if (initialize_store(store, directory, timeline, start_lsn,
					 segment_size) != 0)
		return -1;
	store->directory_fd = open(directory,
							 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (store->directory_fd < 0)
	{
		ps_wal_store_close(store);
		return -1;
	}
	/* Explicit-start open remains the safe migration path for legacy stores
	 * that predate the identity file.  Once a v2 identity exists its complete
	 * tuple is authoritative and must match exactly. */
	{
		struct stat identity_st;

		if (fstatat(store->directory_fd, PS_WAL_STORE_IDENTITY_FILE,
					&identity_st, AT_SYMLINK_NOFOLLOW) == 0)
		{
			if (!S_ISREG(identity_st.st_mode))
				goto cleanup;
			if (identity_st.st_size == (off_t) PS_WAL_STORE_METADATA_BYTES)
			{
				if (read_store_metadata_fd(store->directory_fd, &metadata) != 0 ||
					metadata_matches(&metadata, timeline, start_lsn,
									 metadata.retained_base_lsn, metadata.end_lsn,
									 segment_size) != 0)
					goto cleanup;
				store->retained_base_lsn = metadata.retained_base_lsn;
			}
			else
			{
				uint32_t identity_timeline;
				uint32_t identity_segment_size;
				uint64_t identity_start;

				if (read_store_identity_v1_fd(store->directory_fd,
										  &identity_timeline, &identity_start,
										  &identity_segment_size) != 0 ||
					identity_timeline != timeline ||
					identity_start != start_lsn ||
					identity_segment_size != segment_size)
					goto cleanup;
				identity_legacy = 1;
			}
		}
		else if (errno == ENOENT)
			identity_missing = 1;
		else
			goto cleanup;
	}
	/* An earlier create may have exposed the directory and then reported an
	 * ambiguous parent-fsync failure.  Reopen completes that durability step
	 * before accepting or publishing segment contents. */
	parent_fd = openat(store->directory_fd, "..",
					 O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (parent_fd < 0)
		goto cleanup;
	if (fsync(parent_fd) != 0)
	{
		close(parent_fd);
		parent_fd = -1;
		goto cleanup;
	}
	if (close(parent_fd) != 0)
	{
		parent_fd = -1;
		goto cleanup;
	}
	parent_fd = -1;
	/* A matching identity may have become visible before the previous open
	 * reported its child-directory fsync failure.  Re-sync the directory before
	 * allowing recovery to rely on its immutable segments. */
	if (!identity_missing && fsync(store->directory_fd) != 0)
		goto cleanup;
	prefix_len = snprintf(prefix, sizeof(prefix), "walv1_%u_", timeline);
	if (prefix_len < 0 || (size_t) prefix_len >= sizeof(prefix) ||
		(scan_fd = dup(store->directory_fd)) < 0 ||
		(dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	while ((de = readdir(dir)) != NULL)
	{
		char expected[128];
		char *end = NULL;
		unsigned long long parsed;
		const char *suffix;
		size_t identity_len = strlen(PS_WAL_STORE_IDENTITY_FILE);

		if (strncmp(de->d_name, PS_WAL_STORE_IDENTITY_FILE,
					identity_len) == 0 &&
			strcmp(de->d_name, PS_WAL_STORE_IDENTITY_FILE) != 0)
		{
			suffix = de->d_name + identity_len;
			if (strncmp(suffix, ".tmp.", 5) != 0 || strlen(suffix + 5) != 6)
				goto cleanup;
			for (size_t i = 0; i < 6; i++)
				if (!((suffix[5 + i] >= '0' && suffix[5 + i] <= '9') ||
					  (suffix[5 + i] >= 'A' && suffix[5 + i] <= 'Z') ||
					  (suffix[5 + i] >= 'a' && suffix[5 + i] <= 'z')))
					goto cleanup;
			if (unlinkat(store->directory_fd, de->d_name, 0) != 0)
				goto cleanup;
			removed_temporary = 1;
			continue;
		}

		if (strncmp(de->d_name, prefix, (size_t) prefix_len) != 0)
			continue;
		errno = 0;
		parsed = strtoull(de->d_name + prefix_len, &end, 10);
		if (errno == ERANGE || end == de->d_name + prefix_len ||
			segment_name(store, (uint64_t) parsed, expected,
						 sizeof(expected)) != 0)
			goto cleanup;
		if (*end == '\0' && strcmp(expected, de->d_name) == 0)
		{
			if ((uint64_t) parsed < store->next_segment_no)
			{
				if ((uint64_t) parsed < prefix_first)
					prefix_first = (uint64_t) parsed;
				if (prefix_count == UINT64_MAX)
					goto cleanup;
				prefix_count++;
			}
			else
			{
				if (count == UINT64_MAX)
					goto cleanup;
				count++;
			}
			continue;
		}
		/* A crash before publication may leave only the private staging link.
		 * Its strictly generated name is safe to unlink; malformed names fail
		 * closed so startup never guesses about user data. */
		suffix = de->d_name + strlen(expected);
		if (strncmp(de->d_name, expected, strlen(expected)) != 0 ||
			strncmp(suffix, ".tmp.", 5) != 0 || strlen(suffix + 5) != 6)
			goto cleanup;
		for (size_t i = 0; i < 6; i++)
			if (!((suffix[5 + i] >= '0' && suffix[5 + i] <= '9') ||
				  (suffix[5 + i] >= 'A' && suffix[5 + i] <= 'Z') ||
				  (suffix[5 + i] >= 'a' && suffix[5 + i] <= 'z')))
				goto cleanup;
		if (unlinkat(store->directory_fd, de->d_name, 0) != 0)
			goto cleanup;
		removed_temporary = 1;
	}
	if (closedir(dir) != 0)
	{
		dir = NULL;
		goto cleanup;
	}
	dir = NULL;
	if (removed_temporary && fsync(store->directory_fd) != 0)
		goto cleanup;
	if (prefix_first != UINT64_MAX)
	{
		if (prefix_first >= store->next_segment_no ||
			store->next_segment_no - prefix_first != prefix_count)
			goto cleanup;
		for (uint64_t segment_no = prefix_first;
			 segment_no < store->next_segment_no; segment_no++)
			if (validate_prefix_segment(store, segment_no) != 0)
				goto cleanup;
	}
	for (uint64_t i = 0; i < count; i++)
		if (load_segment(store, store->next_segment_no) != 0)
			goto cleanup;
	if (!identity_missing && !identity_legacy)
	{
		if (metadata.end_lsn > store->end_lsn ||
			metadata.retained_base_lsn > store->end_lsn)
			goto cleanup;
		if (metadata.end_lsn < store->end_lsn)
		{
			/* In the private single-writer directory protocol, a complete
			 * validated contiguous suffix can only be a segment durable before
			 * its end frontier publication.  Adopt it and atomically repair end. */
			if (publish_store_metadata(store, store->start_lsn,
								   metadata.retained_base_lsn,
								   store->end_lsn) != 0)
				goto cleanup;
			metadata.end_lsn = store->end_lsn;
		}
		if (metadata_matches(&metadata, timeline, store->start_lsn,
						  store->retained_base_lsn, store->end_lsn,
						  segment_size) != 0)
			goto cleanup;
	}
	/* A legacy store can be opened only with an externally proven start LSN.
	 * Publish v2 after validating every immutable segment, before a later
	 * flat-prefix reclaim removes the final duplicate source of truth. */
	if (identity_missing || identity_legacy)
	{
		if (publish_store_metadata(store, store->start_lsn,
							   store->start_lsn, store->end_lsn) != 0)
			goto cleanup;
	}
	rc = 0;

	cleanup:
	if (dir != NULL)
		closedir(dir);
	if (scan_fd >= 0)
		close(scan_fd);
	if (parent_fd >= 0)
		close(parent_fd);
	if (rc != 0)
		ps_wal_store_close(store);
	return rc;
}

int
ps_wal_store_open_existing(PsWalStore *store, const char *directory,
						   uint32_t timeline, uint32_t segment_size)
{
	PsWalStoreMetadata metadata;
	uint32_t identity_timeline;
	uint32_t identity_segment_size;
	uint64_t identity_start;
	struct stat identity_st;
	int directory_fd;
	int rc;

	memset(&metadata, 0, sizeof(metadata));
	directory_fd = open(directory,
						O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (directory_fd < 0)
		return -1;
	if (fstatat(directory_fd, PS_WAL_STORE_IDENTITY_FILE, &identity_st,
				AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(identity_st.st_mode))
	{
		close(directory_fd);
		return -1;
	}
	if (identity_st.st_size == (off_t) PS_WAL_STORE_METADATA_BYTES)
	{
		rc = read_store_metadata_fd(directory_fd, &metadata);
		identity_timeline = metadata.timeline;
		identity_segment_size = metadata.segment_size;
		identity_start = metadata.directory_start_lsn;
	}
	else
	{
		rc = read_store_identity_v1_fd(directory_fd, &identity_timeline,
								&identity_start, &identity_segment_size);
	}
	if (close(directory_fd) != 0)
		rc = -1;
	if (rc != 0 || identity_timeline != timeline ||
		identity_segment_size != segment_size)
		return -1;
	return ps_wal_store_open(store, directory, timeline, identity_start,
							 segment_size);
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
ps_wal_store_append(PsWalStore *store, uint64_t start_lsn,
					const void *data, uint32_t len)
{
	const unsigned char *bytes = data;
	uint32_t done = 0;
	int rc = -1;

	if (store == NULL || data == NULL || len == 0 ||
		start_lsn + len < start_lsn)
		return -1;
	if (!store->lock_initialized || pthread_mutex_lock(&store->lock) != 0)
		return -1;
	if (store->directory_fd < 0 || store->metadata_fenced ||
		start_lsn % store->segment_size != 0 ||
		len % store->segment_size != 0 ||
		start_lsn < store->start_lsn ||
		start_lsn > store->end_lsn || start_lsn + len < store->end_lsn)
		goto done_append;
	/* A prior split append may have durably published a prefix before a later
	 * segment failed.  Accept an identical retry and resume at end_lsn; reject
	 * divergent bytes so immutable WAL identity remains fail closed. */
	if (start_lsn < store->end_lsn)
	{
		uint32_t prefix = (uint32_t) (store->end_lsn - start_lsn);

		if (validate_committed_prefix(store, start_lsn, bytes, prefix) != 0)
			goto done_append;
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
			goto done_append;
		{
			const char *fail_segment = getenv("PAGESTORE_TEST_FAIL_WAL_SEGMENT_NO");

			if (fail_segment != NULL &&
				strtoull(fail_segment, NULL, 10) == store->next_segment_no)
				goto done_append;
		}
		if (reserve_entry(store) != 0)
			goto done_append;
		if (build_chunk_hashes_and_payload_crc(bytes + done, chunk,
														 &chunk_hashes, &nchunks,
														 &header.payload_crc) != 0)
		{
			free(chunk_hashes);
			goto done_append;
		}
		if (ps_wal_segment_seal_with_crc(&header, store->timeline,
											 store->next_segment_no, start_lsn + done,
											 store->segment_size, bytes + done, chunk,
											 header.payload_crc) != 0)
		{
			free(chunk_hashes);
			goto done_append;
		}
		if (publish_segment(store, &header, bytes + done) != 0)
		{
			free(chunk_hashes);
			goto done_append;
		}
		/* Publish the directory contents first, then atomically publish its
		 * matching end frontier.  A crash between these points is fail-closed
		 * on reopen because metadata end must equal the validated directory end. */
		if (publish_store_metadata(store, store->start_lsn,
								 store->retained_base_lsn,
								 store->end_lsn + chunk) != 0)
		{
			uint64_t old_end_lsn = store->end_lsn;

			(void) reconcile_metadata_failure(store,
													  store->start_lsn,
												  store->retained_base_lsn,
												  old_end_lsn,
												  store->start_lsn,
												  store->retained_base_lsn,
													  old_end_lsn + chunk);
			free(chunk_hashes);
			goto done_append;
		}
		store->entries[store->nentries].header = header;
		store->entries[store->nentries].chunk_hashes = chunk_hashes;
		store->entries[store->nentries].nchunks = nchunks;
		store->nentries++;
		store->next_segment_no++;
		store->end_lsn += chunk;
		done += chunk;
	}
	rc = 0;

done_append:
	pthread_mutex_unlock(&store->lock);
	return rc;
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
		(fd = openat(store->directory_fd, name,
					 O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
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
	if (!store->lock_initialized || pthread_mutex_lock(&store->lock) != 0)
		return -1;
	if (store->directory_fd < 0 || store->metadata_fenced ||
		start_lsn < store->retained_base_lsn || end_lsn > store->end_lsn)
	{
		pthread_mutex_unlock(&store->lock);
		return -1;
	}
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
			{
				pthread_mutex_unlock(&store->lock);
				return -1;
			}
			done += (uint32_t) amount;
		}
	}
	pthread_mutex_unlock(&store->lock);
	return done == len ? 0 : -1;
}

int
ps_wal_store_retained_base(PsWalStore *store, uint64_t *out)
{
	if (store == NULL || out == NULL || !store->lock_initialized)
		return -1;
	if (pthread_mutex_lock(&store->lock) != 0)
		return -1;
	if (store->directory_fd < 0 || store->metadata_fenced)
	{
		pthread_mutex_unlock(&store->lock);
		return -1;
	}
	*out = store->retained_base_lsn;
	pthread_mutex_unlock(&store->lock);
	return 0;
}

int
ps_wal_store_advance_retained_base(PsWalStore *store,
								   uint64_t retained_base_lsn)
{
	int rc = -1;

	if (store == NULL || !store->lock_initialized ||
		pthread_mutex_lock(&store->lock) != 0)
		return -1;
	if (store->directory_fd < 0 || store->metadata_fenced ||
		retained_base_lsn < store->retained_base_lsn ||
		retained_base_lsn > store->end_lsn ||
		retained_base_lsn % store->segment_size != 0)
		goto done_advance;
	if (retained_base_lsn == store->retained_base_lsn)
	{
		rc = 0;
		goto done_advance;
	}
	{
		uint64_t old_start_lsn = store->start_lsn;
		uint64_t old_base_lsn = store->retained_base_lsn;
		uint64_t end_lsn = store->end_lsn;

		if (publish_store_metadata(store, retained_base_lsn,
								   retained_base_lsn, end_lsn) != 0)
		{
			reconcile_metadata_failure(store, old_start_lsn, old_base_lsn, end_lsn,
									 retained_base_lsn, retained_base_lsn, end_lsn);
			goto done_advance;
		}
		install_retained_frontier(store, retained_base_lsn);
	}
	rc = 0;

done_advance:
	pthread_mutex_unlock(&store->lock);
	return rc;
}

void
ps_wal_store_close(PsWalStore *store)
{
	if (store == NULL)
		return;
	if (store->lock_initialized)
		(void) pthread_mutex_lock(&store->lock);
	if (store->directory_fd >= 0)
		close(store->directory_fd);
	for (uint32_t i = 0; i < store->nentries; i++)
		free(store->entries[i].chunk_hashes);
	free(store->entries);
	if (store->lock_initialized)
	{
		(void) pthread_mutex_unlock(&store->lock);
		(void) pthread_mutex_destroy(&store->lock);
	}
	memset(store, 0, sizeof(*store));
	store->directory_fd = -1;
}
