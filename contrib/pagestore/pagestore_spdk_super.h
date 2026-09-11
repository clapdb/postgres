/*-------------------------------------------------------------------------
 *
 * pagestore_spdk_super.h
 *	  The SPDK store's container metadata: the superblock that records how
 *	  many segments each shard has appended to the raw NVMe namespace, and
 *	  the identity of the on-device extent layout those counts address.
 *
 * The SPDK provider keeps only page segments on the device.  Everything a
 * segment holds is the provider-neutral record format the POSIX fixture
 * proves; what is SPDK-specific is the container: where segment S of shard
 * H lives on the device and how far each shard's append position has
 * reached.  That position is the one piece of metadata a later open needs
 * to place new data without overwriting old data, so it is versioned,
 * checksummed, and published durably here, independent of SPDK itself so
 * the format can be unit-tested and reported by pagestore_format_versions
 * on a machine without SPDK.
 *
 * Extent layout v1: segment S of shard H (of N shards) starts at device byte
 * (S * N + H) * segment_size and is written as one sector-aligned,
 * zero-padded extent; a reader stops at the first non-magic record, so the
 * padding needs no length bookkeeping.  The layout depends on N, which is
 * why a superblock recorded for a different shard count refuses the open:
 * reopening with another N would address every extent differently.
 *
 * Superblock v2 (current), little-endian, no padding:
 *   0  u32 magic "SPKS"
 *   4  u32 version = 2
 *   8  u32 length     bytes in the whole superblock, CRC included
 *  12  u32 sector_size
 *  16  u64 segment_size
 *  24  u32 nshards
 *  28  u32 num_segments[nshards]
 *  28 + 4 * nshards  u32 crc   FNV-1a over bytes [0, 28 + 4 * nshards)
 *
 * Accepted legacy layouts (native struct images written by fwrite):
 *   sharded, version word 1:
 *       {u32 magic, u32 version = 1, u32 sector_size, u64 segment_size,
 *        u32 nshards, u32 num_segments[PS_MAX_CHANNELS]}
 *   single-shard, no version word (its second word is the sector size,
 *   a power of two of at least 512, which no version number reaches):
 *       {u32 magic, u32 sector_size, u64 segment_size, u32 num_segments}
 * A legacy superblock is decoded once and replaced by v2 at the next
 * publication.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_SPDK_SUPER_H
#define PAGESTORE_SPDK_SUPER_H

#include <stddef.h>
#include <stdint.h>

#include "pagestore_format.h"

#define PS_SPDK_SUPER_MAGIC			0x53504b53u		/* "SPKS" */
#define PS_SPDK_SUPER_VERSION		2u
#define PS_SPDK_SUPER_LEGACY_SHARDED 1u	/* version word of the sharded struct */
#define PS_SPDK_SUPER_LEGACY_SINGLE	0u	/* the single-shard struct has none */
#define PS_SPDK_SUPER_FILE			"spdk_super"
#define PS_SPDK_SUPER_MAX_SHARDS	128u	/* PS_MAX_CHANNELS */
#define PS_SPDK_SUPER_FIXED_BYTES	28u
#define PS_SPDK_SUPER_MAX_BYTES		(PS_SPDK_SUPER_FIXED_BYTES + 4u * PS_SPDK_SUPER_MAX_SHARDS + 4u)
#define PS_SPDK_EXTENT_LAYOUT_VERSION 1u

typedef enum PsSpdkSuperStatus
{
	PS_SPDK_SUPER_OK = 0,		/* decoded; counts filled */
	PS_SPDK_SUPER_ABSENT,		/* no superblock: a store that never published one */
	PS_SPDK_SUPER_TRUNCATED,	/* shorter than the layout it announces */
	PS_SPDK_SUPER_BAD_MAGIC,	/* not a superblock */
	PS_SPDK_SUPER_NEWER,		/* a version this build does not know */
	PS_SPDK_SUPER_CORRUPT,		/* checksum or length mismatch */
	PS_SPDK_SUPER_GEOMETRY,		/* sector or segment size differ from the device */
	PS_SPDK_SUPER_SHARDS,		/* recorded for a different shard count */
	PS_SPDK_SUPER_IO			/* read failed; errno set */
} PsSpdkSuperStatus;

/*
 * Decode 'len' bytes of a superblock against the device geometry and the
 * configured shard count.  On PS_SPDK_SUPER_OK, counts[0..nshards) hold the
 * recorded append positions and *version_out the layout that carried them.
 */
extern PsSpdkSuperStatus ps_spdk_super_decode(const unsigned char *buf, size_t len,
											  uint32_t sector_size,
											  uint64_t segment_size,
											  uint32_t nshards,
											  uint32_t *counts,
											  uint32_t *version_out);

/*
 * Encode a v2 superblock into 'out' (at least PS_SPDK_SUPER_MAX_BYTES).
 * Returns the encoded length, or 0 when nshards is out of range.
 */
extern size_t ps_spdk_super_encode(unsigned char *out, uint32_t sector_size,
								   uint64_t segment_size, uint32_t nshards,
								   const uint32_t *counts);

/* Read and decode <store_dir>/spdk_super. */
extern PsSpdkSuperStatus ps_spdk_super_read(const char *store_dir,
											uint32_t sector_size,
											uint64_t segment_size,
											uint32_t nshards,
											uint32_t *counts,
											uint32_t *version_out);

/*
 * Publish the counts durably as a v2 superblock: written to a temporary
 * file, fsynced, renamed over the old copy, and the directory fsynced.  A
 * failure at any step leaves the previous superblock in place and returns
 * -errno; the caller must not treat the new counts as persisted.
 */
extern int	ps_spdk_super_publish(const char *store_dir, uint32_t sector_size,
								  uint64_t segment_size, uint32_t nshards,
								  const uint32_t *counts);

extern const char *ps_spdk_super_status_name(PsSpdkSuperStatus status);

/* Container identities of the SPDK provider, for pagestore_format_versions. */
extern size_t ps_storage_spdk_format_identities(const PsFormatIdentity **out);

#endif							/* PAGESTORE_SPDK_SUPER_H */
