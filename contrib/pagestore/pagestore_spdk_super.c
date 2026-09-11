/*-------------------------------------------------------------------------
 *
 * pagestore_spdk_super.c
 *	  Decode, encode, read and durably publish the SPDK store superblock.
 *	  See pagestore_spdk_super.h for the layouts.
 *
 * This file has no SPDK dependency: it is linked into the SPDK daemon, the
 * format-identity tool, and its own unit test.
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_spdk_super.h"

/* the sharded struct image: {u32, u32 version = 1, u32, pad, u64, u32, u32[128]} */
#define LEGACY_SHARDED_BYTES	544u
#define LEGACY_SHARDED_SECTOR	8u
#define LEGACY_SHARDED_SEGSIZE	16u
#define LEGACY_SHARDED_NSHARDS	24u
#define LEGACY_SHARDED_COUNTS	28u
/* the single-shard struct image: {u32 magic, u32 sector, u64 segsize, u32 count, pad} */
#define LEGACY_SINGLE_BYTES		24u
#define LEGACY_SINGLE_SECTOR	4u
#define LEGACY_SINGLE_SEGSIZE	8u
#define LEGACY_SINGLE_COUNT		16u
/* a version word at or above this is a sector size, i.e. the single-shard image */
#define LEGACY_SINGLE_MIN_SECTOR 512u

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
put_le32(unsigned char *p, uint32_t v)
{
	p[0] = (unsigned char) v;
	p[1] = (unsigned char) (v >> 8);
	p[2] = (unsigned char) (v >> 16);
	p[3] = (unsigned char) (v >> 24);
}

static void
put_le64(unsigned char *p, uint64_t v)
{
	put_le32(p, (uint32_t) v);
	put_le32(p + 4, (uint32_t) (v >> 32));
}

/* the legacy struct images were written in host byte order */
static uint32_t
get_native32(const unsigned char *p)
{
	uint32_t	v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static uint64_t
get_native64(const unsigned char *p)
{
	uint64_t	v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static uint32_t
fnv1a(const unsigned char *data, size_t len)
{
	uint32_t	hash = 2166136261u;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= data[i];
		hash *= 16777619u;
	}
	return hash;
}

const char *
ps_spdk_super_status_name(PsSpdkSuperStatus status)
{
	switch (status)
	{
		case PS_SPDK_SUPER_OK:
			return "ok";
		case PS_SPDK_SUPER_ABSENT:
			return "absent";
		case PS_SPDK_SUPER_TRUNCATED:
			return "truncated";
		case PS_SPDK_SUPER_OVERLONG:
			return "longer than its layout";
		case PS_SPDK_SUPER_BAD_MAGIC:
			return "not a superblock";
		case PS_SPDK_SUPER_NEWER:
			return "newer than this build";
		case PS_SPDK_SUPER_CORRUPT:
			return "checksum or length mismatch";
		case PS_SPDK_SUPER_GEOMETRY:
			return "device geometry mismatch";
		case PS_SPDK_SUPER_SHARDS:
			return "shard count mismatch";
		case PS_SPDK_SUPER_IO:
			return "read error";
	}
	return "unknown";
}

static PsSpdkSuperStatus
check_geometry(uint32_t sector_size, uint64_t segment_size,
			   uint32_t rec_sector, uint64_t rec_segsize)
{
	if (rec_sector != sector_size || rec_segsize != segment_size)
		return PS_SPDK_SUPER_GEOMETRY;
	return PS_SPDK_SUPER_OK;
}

PsSpdkSuperStatus
ps_spdk_super_decode(const unsigned char *buf, size_t len,
					 uint32_t sector_size, uint64_t segment_size,
					 uint32_t nshards, uint32_t *counts, uint32_t *version_out)
{
	uint32_t	version;
	PsSpdkSuperStatus geometry;
	bool		legacy;

	if (nshards == 0 || nshards > PS_SPDK_SUPER_MAX_SHARDS)
		return PS_SPDK_SUPER_SHARDS;
	if (len < 8)
		return PS_SPDK_SUPER_TRUNCATED;

	/*
	 * v2 is little-endian everywhere; the legacy struct images carry the
	 * magic in the byte order of the host that wrote them, which is the
	 * host reading them back (a store is not moved between byte orders).
	 * On a little-endian host the two readings coincide.
	 */
	if (get_le32(buf) == PS_SPDK_SUPER_MAGIC && get_le32(buf + 4) == PS_SPDK_SUPER_VERSION)
		legacy = false;
	else if (get_native32(buf) == PS_SPDK_SUPER_MAGIC)
		legacy = true;
	else
		return PS_SPDK_SUPER_BAD_MAGIC;

	if (!legacy)
	{
		uint32_t	length;
		uint32_t	rec_shards;
		size_t		body;

		version = PS_SPDK_SUPER_VERSION;
		if (len < PS_SPDK_SUPER_FIXED_BYTES)
			return PS_SPDK_SUPER_TRUNCATED;
		length = get_le32(buf + 8);
		rec_shards = get_le32(buf + 24);
		if (rec_shards == 0 || rec_shards > PS_SPDK_SUPER_MAX_SHARDS)
			return PS_SPDK_SUPER_CORRUPT;
		body = PS_SPDK_SUPER_FIXED_BYTES + 4u * rec_shards;
		if (length != body + 4u)
			return PS_SPDK_SUPER_CORRUPT;
		if (len < length)
			return PS_SPDK_SUPER_TRUNCATED;
		if (len > length)
			return PS_SPDK_SUPER_OVERLONG;
		if (get_le32(buf + body) != fnv1a(buf, body))
			return PS_SPDK_SUPER_CORRUPT;
		geometry = check_geometry(sector_size, segment_size,
								  get_le32(buf + 12), get_le64(buf + 16));
		if (geometry != PS_SPDK_SUPER_OK)
			return geometry;
		if (rec_shards != nshards)
			return PS_SPDK_SUPER_SHARDS;
		for (uint32_t i = 0; i < nshards; i++)
			counts[i] = get_le32(buf + PS_SPDK_SUPER_FIXED_BYTES + 4u * i);
		if (version_out)
			*version_out = version;
		return PS_SPDK_SUPER_OK;
	}
	/*
	 * The single-shard image had no version word: its second word is the
	 * sector size, a power of two no smaller than 512, which no version
	 * number reaches.
	 */
	version = get_native32(buf + 4);
	if (version == PS_SPDK_SUPER_LEGACY_SHARDED)
	{
		uint32_t	rec_shards;

		if (len < LEGACY_SHARDED_BYTES)
			return PS_SPDK_SUPER_TRUNCATED;
		if (len > LEGACY_SHARDED_BYTES)
			return PS_SPDK_SUPER_OVERLONG;
		geometry = check_geometry(sector_size, segment_size,
								  get_native32(buf + LEGACY_SHARDED_SECTOR),
								  get_native64(buf + LEGACY_SHARDED_SEGSIZE));
		if (geometry != PS_SPDK_SUPER_OK)
			return geometry;
		rec_shards = get_native32(buf + LEGACY_SHARDED_NSHARDS);
		if (rec_shards != nshards)
			return PS_SPDK_SUPER_SHARDS;
		for (uint32_t i = 0; i < nshards; i++)
			counts[i] = get_native32(buf + LEGACY_SHARDED_COUNTS + 4u * i);
		if (version_out)
			*version_out = version;
		return PS_SPDK_SUPER_OK;
	}
	if (version > PS_SPDK_SUPER_VERSION && version < LEGACY_SINGLE_MIN_SECTOR)
		return PS_SPDK_SUPER_NEWER;
	if (version < LEGACY_SINGLE_MIN_SECTOR)
		return PS_SPDK_SUPER_CORRUPT;	/* version word 0 or a v2 word in the
										 * wrong byte order: neither layout */

	/* single-shard image: the word at 4 is the sector size */
	if (len < LEGACY_SINGLE_BYTES)
		return PS_SPDK_SUPER_TRUNCATED;
	if (len > LEGACY_SINGLE_BYTES)
		return PS_SPDK_SUPER_OVERLONG;
	geometry = check_geometry(sector_size, segment_size,
							  get_native32(buf + LEGACY_SINGLE_SECTOR),
							  get_native64(buf + LEGACY_SINGLE_SEGSIZE));
	if (geometry != PS_SPDK_SUPER_OK)
		return geometry;
	if (nshards != 1)
		return PS_SPDK_SUPER_SHARDS;
	counts[0] = get_native32(buf + LEGACY_SINGLE_COUNT);
	if (version_out)
		*version_out = PS_SPDK_SUPER_LEGACY_SINGLE;
	return PS_SPDK_SUPER_OK;
}

size_t
ps_spdk_super_encode(unsigned char *out, uint32_t sector_size,
					 uint64_t segment_size, uint32_t nshards,
					 const uint32_t *counts)
{
	size_t		body;

	if (nshards == 0 || nshards > PS_SPDK_SUPER_MAX_SHARDS)
		return 0;
	body = PS_SPDK_SUPER_FIXED_BYTES + 4u * nshards;
	put_le32(out, PS_SPDK_SUPER_MAGIC);
	put_le32(out + 4, PS_SPDK_SUPER_VERSION);
	put_le32(out + 8, (uint32_t) (body + 4u));
	put_le32(out + 12, sector_size);
	put_le64(out + 16, segment_size);
	put_le32(out + 24, nshards);
	for (uint32_t i = 0; i < nshards; i++)
		put_le32(out + PS_SPDK_SUPER_FIXED_BYTES + 4u * i, counts[i]);
	put_le32(out + body, fnv1a(out, body));
	return body + 4u;
}

static int
super_path(char *buf, size_t buflen, const char *store_dir, const char *name)
{
	int			n = snprintf(buf, buflen, "%s/%s", store_dir, name);

	if (n < 0 || (size_t) n >= buflen)
	{
		errno = ENAMETOOLONG;
		return -1;
	}
	return 0;
}

PsSpdkSuperStatus
ps_spdk_super_read(const char *store_dir, uint32_t sector_size,
				   uint64_t segment_size, uint32_t nshards,
				   uint32_t *counts, uint32_t *version_out)
{
	char		path[4096];
	/* one byte more than the largest layout, so a longer file is seen as such */
	unsigned char buf[(LEGACY_SHARDED_BYTES > PS_SPDK_SUPER_MAX_BYTES
					   ? LEGACY_SHARDED_BYTES : PS_SPDK_SUPER_MAX_BYTES) + 1];
	int			fd;
	size_t		len = 0;

	if (super_path(path, sizeof(path), store_dir, PS_SPDK_SUPER_FILE) != 0)
		return PS_SPDK_SUPER_IO;
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return errno == ENOENT ? PS_SPDK_SUPER_ABSENT : PS_SPDK_SUPER_IO;
	for (;;)
	{
		ssize_t		n = read(fd, buf + len, sizeof(buf) - len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			close(fd);
			return PS_SPDK_SUPER_IO;
		}
		if (n == 0 || (len += (size_t) n) == sizeof(buf))
			break;
	}
	close(fd);
	return ps_spdk_super_decode(buf, len, sector_size, segment_size, nshards,
								counts, version_out);
}

static int
write_all(int fd, const unsigned char *buf, size_t len)
{
	while (len > 0)
	{
		ssize_t		n = write(fd, buf, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return -1;
		}
		buf += n;
		len -= (size_t) n;
	}
	return 0;
}

int
ps_spdk_super_publish(const char *store_dir, uint32_t sector_size,
					  uint64_t segment_size, uint32_t nshards,
					  const uint32_t *counts)
{
	unsigned char buf[PS_SPDK_SUPER_MAX_BYTES];
	char		tmp[4096];
	char		final[4096];
	size_t		len;
	int			fd;
	int			dirfd;
	int			rc;

	len = ps_spdk_super_encode(buf, sector_size, segment_size, nshards, counts);
	if (len == 0)
		return -EINVAL;
	if (super_path(tmp, sizeof(tmp), store_dir, PS_SPDK_SUPER_FILE ".tmp") != 0 ||
		super_path(final, sizeof(final), store_dir, PS_SPDK_SUPER_FILE) != 0)
		return -errno;

	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0)
		return -errno;
	if (write_all(fd, buf, len) != 0 || fsync(fd) != 0)
	{
		rc = -errno;
		close(fd);
		unlink(tmp);
		return rc;
	}
	if (close(fd) != 0)
	{
		rc = -errno;
		unlink(tmp);
		return rc;
	}
	if (rename(tmp, final) != 0)
	{
		rc = -errno;
		unlink(tmp);
		return rc;
	}
	dirfd = open(store_dir, O_RDONLY | O_DIRECTORY);
	if (dirfd < 0)
		return -errno;
	if (fsync(dirfd) != 0)
	{
		rc = -errno;
		close(dirfd);
		return rc;
	}
	close(dirfd);
	return 0;
}

size_t
ps_storage_spdk_format_identities(const PsFormatIdentity **out)
{
	static const PsFormatIdentity identities[] = {
		{"spdk_container", "spdk_super", PS_SPDK_SUPER_MAGIC,
		 PS_SPDK_SUPER_VERSION},
		{"spdk_container", "spdk_super (accepted legacy sharded struct, version word 1)",
		 PS_SPDK_SUPER_MAGIC, PS_SPDK_SUPER_LEGACY_SHARDED},
		{"spdk_container", "spdk_super (accepted legacy single-shard struct, no version word)",
		 PS_SPDK_SUPER_MAGIC, PS_SPDK_SUPER_LEGACY_SINGLE},
		{"spdk_container", "on-device segment extent layout", 0,
		 PS_SPDK_EXTENT_LAYOUT_VERSION},
	};

	*out = identities;
	return sizeof(identities) / sizeof(identities[0]);
}
