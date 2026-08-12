#ifndef PAGESTORE_WAL_SEGMENT_H
#define PAGESTORE_WAL_SEGMENT_H

#include <stddef.h>
#include <stdint.h>

#define PS_WAL_SEGMENT_MAGIC 0x57534731u /* "WSG1" */
#define PS_WAL_SEGMENT_VERSION 1u
#define PS_WAL_SEGMENT_HEADER_BYTES 64u
#define PS_WAL_SEGMENT_PAYLOAD_BYTES (16u * 1024u * 1024u)

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
	uint64_t	reserved64[2];
} PsWalSegmentHeader;

extern int ps_wal_segment_seal(PsWalSegmentHeader *header, uint32_t timeline,
							   uint64_t segment_no, uint64_t start_lsn,
							   const void *payload, uint32_t payload_len);
extern int ps_wal_segment_validate(const PsWalSegmentHeader *header,
								 const void *payload, uint32_t payload_len);
extern int ps_wal_segment_encode(const PsWalSegmentHeader *header,
								 unsigned char out[PS_WAL_SEGMENT_HEADER_BYTES]);
extern int ps_wal_segment_decode(PsWalSegmentHeader *header,
								 const unsigned char *input, size_t input_len);

#endif
