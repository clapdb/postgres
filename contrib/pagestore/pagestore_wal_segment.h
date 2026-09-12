#ifndef PAGESTORE_WAL_SEGMENT_H
#define PAGESTORE_WAL_SEGMENT_H

#include <stddef.h>
#include <stdint.h>

/*
 * The immutable shipped-WAL segment envelope.  The payload is a run of
 * PostgreSQL WAL bytes exactly as the writer produced them, starting on a
 * WAL page boundary; the envelope wraps it with the store's own identity of
 * the run (timeline, segment number, start LSN, lengths, checksums).
 *
 * Version 2 also records the payload's own PostgreSQL identity, read from
 * the page header the payload begins with: xlp_magic, which PostgreSQL bumps
 * whenever the WAL format changes (XLOG_PAGE_MAGIC), xlp_info, and, when the
 * payload begins a PostgreSQL WAL segment and so carries a long page header,
 * the cluster's WAL segment size.  The store does not interpret those values
 * beyond copying them; a loader that hands the payload to PostgreSQL compares
 * them with the running build and cluster, and the fixture check reads them
 * from the envelope without parsing WAL.  A version 1 envelope carries zeros
 * there and records no payload identity.  Bytes 56..63 were reserved in
 * version 1, so the layout and every offset are unchanged.  The page header
 * fields are read in the byte order PostgreSQL wrote them -- the host's,
 * since WAL is not moved between byte orders -- while the envelope itself
 * is little-endian on every host.
 */
#define PS_WAL_SEGMENT_MAGIC 0x57534731u /* "WSG1" */
#define PS_WAL_SEGMENT_VERSION 2u
#define PS_WAL_SEGMENT_LEGACY_VERSION 1u
#define PS_WAL_SEGMENT_HEADER_BYTES 64u
#define PS_WAL_SEGMENT_MIN_BYTES (1u * 1024u * 1024u)
#define PS_WAL_SEGMENT_MAX_BYTES (1024u * 1024u * 1024u)
/* the PostgreSQL WAL page header fields the envelope copies */
#define PS_WAL_XLP_LONG_HEADER 0x0002u	/* XLP_LONG_HEADER */
#define PS_WAL_XLP_SHORT_HEADER_BYTES 24u
#define PS_WAL_XLP_LONG_HEADER_BYTES 40u
#define PS_WAL_XLP_SEG_SIZE_OFFSET 32u

typedef struct PsWalSegmentHeader
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	header_len;
	uint32_t	flags;
	uint32_t	timeline;
	uint32_t	payload_len;
	uint64_t	segment_no;
	uint64_t	start_lsn;
	uint32_t	payload_crc;
	uint32_t	header_crc;
	uint64_t	segment_size;
	uint16_t	xlp_magic;		/* payload's XLOG_PAGE_MAGIC; 0 in version 1 */
	uint16_t	xlp_info;		/* payload's first page flags; 0 in version 1 */
	uint32_t	xlp_seg_size;	/* cluster WAL segment size from a long page
								 * header at the payload start, else 0 */
} PsWalSegmentHeader;

/*
 * Read the payload's own identity from the page header it begins with.
 * Returns -1 when the payload is too short to carry a page header.
 */
extern int ps_wal_segment_payload_identity(const void *payload,
											 uint32_t payload_len,
											 uint16_t *xlp_magic,
											 uint16_t *xlp_info,
											 uint32_t *xlp_seg_size);

extern int ps_wal_segment_seal(PsWalSegmentHeader *header, uint32_t timeline,
								   uint64_t segment_no, uint64_t start_lsn,
								   uint32_t segment_size, const void *payload,
								   uint32_t payload_len);
/* Seal a header after a caller has already computed the FNV-1a payload CRC. */
extern int ps_wal_segment_seal_with_crc(PsWalSegmentHeader *header,
											uint32_t timeline, uint64_t segment_no,
											uint64_t start_lsn, uint32_t segment_size,
											const void *payload, uint32_t payload_len,
											uint32_t payload_crc);
extern int ps_wal_segment_validate(const PsWalSegmentHeader *header,
								 const void *payload, uint32_t payload_len);
extern int ps_wal_segment_encode(const PsWalSegmentHeader *header,
								 unsigned char out[PS_WAL_SEGMENT_HEADER_BYTES]);
extern int ps_wal_segment_decode(PsWalSegmentHeader *header,
								 const unsigned char *input, size_t input_len);

#endif
