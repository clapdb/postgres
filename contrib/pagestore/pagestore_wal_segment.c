#include <stddef.h>
#include <string.h>

#include "pagestore_wal_segment.h"

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
payload_crc(const void *payload, uint32_t payload_len)
{
	return fnv1a(2166136261u, payload, payload_len);
}

static uint16_t
get_le16(const unsigned char *p)
{
	return (uint16_t) ((uint32_t) p[0] | (uint32_t) p[1] << 8);
}

/* the payload's page header fields, in the byte order PostgreSQL wrote them */
static uint16_t
get_native16(const unsigned char *p)
{
	uint16_t	v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static uint32_t
get_native32(const unsigned char *p)
{
	uint32_t	v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static void
put_le16(unsigned char *p, uint16_t v)
{
	p[0] = (unsigned char) v;
	p[1] = (unsigned char) (v >> 8);
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

static void
encode_fields(const PsWalSegmentHeader *header, unsigned char *out,
			  int include_crc)
{
	memset(out, 0, PS_WAL_SEGMENT_HEADER_BYTES);
	put_le32(out + 0, header->magic);
	put_le32(out + 4, header->version);
	put_le32(out + 8, header->header_len);
	put_le32(out + 12, header->flags);
	put_le32(out + 16, header->timeline);
	put_le32(out + 20, header->payload_len);
	put_le64(out + 24, header->segment_no);
	put_le64(out + 32, header->start_lsn);
	put_le32(out + 40, header->payload_crc);
	put_le32(out + 44, include_crc ? header->header_crc : 0);
	put_le64(out + 48, header->segment_size);
	put_le16(out + 56, header->xlp_magic);
	put_le16(out + 58, header->xlp_info);
	put_le32(out + 60, header->xlp_seg_size);
}

/* PostgreSQL's own bounds: a WAL segment is a power of two in [1 MiB, 1 GiB],
 * a WAL block a power of two in [1 KiB, 64 KiB]. */
static int
wal_seg_size_plausible(uint32_t size)
{
	return size >= (1u << 20) && size <= (1u << 30) && (size & (size - 1)) == 0;
}

static int
wal_blcksz_plausible(uint32_t size)
{
	return size >= 1024u && size <= 65536u && (size & (size - 1)) == 0;
}

int
ps_wal_segment_payload_identity(const void *payload, uint32_t payload_len,
								uint16_t *xlp_magic, uint16_t *xlp_info,
								uint32_t *xlp_seg_size)
{
	const unsigned char *bytes = payload;

	if (payload == NULL || payload_len < PS_WAL_XLP_MIN_HEADER_BYTES)
		return -1;
	*xlp_magic = get_native16(bytes);
	*xlp_info = get_native16(bytes + 2);
	*xlp_seg_size = 0;
	if ((*xlp_info & PS_WAL_XLP_LONG_HEADER) != 0 &&
		payload_len >= PS_WAL_XLP_LONG_HEADER_BYTES)
	{
		/* see the header comment: the writer's ABI decides where the
		 * segment size sits, and the bytes say which ABI wrote them */
		uint32_t seg8 = get_native32(bytes + PS_WAL_XLP_SEG_SIZE_OFFSET_ALIGN8);
		uint32_t blk8 = get_native32(bytes + PS_WAL_XLP_SEG_SIZE_OFFSET_ALIGN8 + 4);
		uint32_t seg4 = get_native32(bytes + PS_WAL_XLP_SEG_SIZE_OFFSET_ALIGN4);
		uint32_t blk4 = get_native32(bytes + PS_WAL_XLP_SEG_SIZE_OFFSET_ALIGN4 + 4);
		int		pad8_zero = get_native32(bytes + 20) == 0;

		if (pad8_zero && wal_seg_size_plausible(seg8) && wal_blcksz_plausible(blk8))
			*xlp_seg_size = seg8;
		else if (wal_seg_size_plausible(seg4) && wal_blcksz_plausible(blk4))
			*xlp_seg_size = seg4;
	}
	return 0;
}

/* The envelope's recorded payload identity is what the payload carries. */
static int
payload_identity_matches(const PsWalSegmentHeader *header, const void *payload,
						 uint32_t payload_len)
{
	uint16_t	magic;
	uint16_t	info;
	uint32_t	seg_size;

	if (header->version == PS_WAL_SEGMENT_LEGACY_VERSION)
		return header->xlp_magic == 0 && header->xlp_info == 0 &&
			header->xlp_seg_size == 0;
	if (ps_wal_segment_payload_identity(payload, payload_len, &magic, &info,
										&seg_size) != 0)
		return 0;
	return header->xlp_magic == magic && header->xlp_info == info &&
		header->xlp_seg_size == seg_size;
}

static int
version_known(uint32_t version)
{
	return version == PS_WAL_SEGMENT_VERSION ||
		version == PS_WAL_SEGMENT_LEGACY_VERSION;
}

static uint32_t
header_crc(const PsWalSegmentHeader *header)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];

	encode_fields(header, encoded, 0);
	return fnv1a(2166136261u, encoded, sizeof(encoded));
}

static int
segment_identity_valid(const PsWalSegmentHeader *header)
{
	return header->timeline != 0 &&
		header->segment_size >= PS_WAL_SEGMENT_MIN_BYTES &&
		header->segment_size <= PS_WAL_SEGMENT_MAX_BYTES &&
		(header->segment_size & (header->segment_size - 1)) == 0 &&
		header->start_lsn % header->segment_size == 0 &&
		header->start_lsn / header->segment_size == header->segment_no &&
		header->start_lsn + header->payload_len >= header->start_lsn &&
		header->payload_len <= header->segment_size;
}

int
ps_wal_segment_seal_with_crc(PsWalSegmentHeader *header, uint32_t timeline,
							 uint64_t segment_no, uint64_t start_lsn,
							 uint32_t segment_size, const void *payload,
							 uint32_t payload_len, uint32_t stored_payload_crc)
{
	uintptr_t hstart = (uintptr_t) header;
	uintptr_t pstart = (uintptr_t) payload;

	if (header == NULL || payload_len == 0 || payload == NULL ||
		segment_size == 0 || payload_len > segment_size ||
		pstart > UINTPTR_MAX - payload_len ||
		(pstart < hstart + sizeof(*header) && pstart + payload_len > hstart))
		return -1;
	memset(header, 0, sizeof(*header));
	header->magic = PS_WAL_SEGMENT_MAGIC;
	header->version = PS_WAL_SEGMENT_VERSION;
	header->header_len = PS_WAL_SEGMENT_HEADER_BYTES;
	header->timeline = timeline;
	header->segment_no = segment_no;
	header->start_lsn = start_lsn;
	header->payload_len = payload_len;
	header->segment_size = segment_size;
	if (!segment_identity_valid(header) ||
		ps_wal_segment_payload_identity(payload, payload_len, &header->xlp_magic,
										&header->xlp_info,
										&header->xlp_seg_size) != 0)
		return -1;
	header->payload_crc = stored_payload_crc;
	header->header_crc = header_crc(header);
	return 0;
}

int
ps_wal_segment_seal(PsWalSegmentHeader *header, uint32_t timeline,
					uint64_t segment_no, uint64_t start_lsn,
					uint32_t segment_size, const void *payload,
					uint32_t payload_len)
{
	/* Reject malformed identities before touching the claimed payload.  In
	 * particular, an invalid uint32 length must not turn into a multi-gigabyte
	 * synchronous hash scan on the append shard. */
	if (ps_wal_segment_seal_with_crc(header, timeline, segment_no,
										start_lsn, segment_size, payload, payload_len, 0) != 0)
		return -1;
	return ps_wal_segment_seal_with_crc(header, timeline, segment_no,
										start_lsn, segment_size, payload, payload_len,
										payload_crc(payload, payload_len));
}

int
ps_wal_segment_validate(const PsWalSegmentHeader *header,
						const void *payload, uint32_t payload_len)
{
	if (header == NULL || payload == NULL ||
		header->magic != PS_WAL_SEGMENT_MAGIC ||
		!version_known(header->version) ||
		header->header_len != PS_WAL_SEGMENT_HEADER_BYTES || header->flags != 0 ||
		header->payload_len == 0 ||
		header->payload_len != payload_len ||
		!segment_identity_valid(header) ||
		!payload_identity_matches(header, payload, payload_len) ||
		header->header_crc != header_crc(header))
		return -1;
	return header->payload_crc == payload_crc(payload, payload_len) ? 0 : -1;
}

int
ps_wal_segment_encode(const PsWalSegmentHeader *header,
					  unsigned char out[PS_WAL_SEGMENT_HEADER_BYTES])
{
	PsWalSegmentHeader staged;

	if (header == NULL || out == NULL || header->header_crc != header_crc(header))
		return -1;
	staged = *header;
	encode_fields(&staged, out, 1);
	return 0;
}

int
ps_wal_segment_decode(PsWalSegmentHeader *header, const unsigned char *input,
					  size_t input_len)
{
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	PsWalSegmentHeader decoded;

	if (header == NULL || input == NULL ||
		input_len != PS_WAL_SEGMENT_HEADER_BYTES)
		return -1;
	memmove(encoded, input, sizeof(encoded));
	memset(&decoded, 0, sizeof(decoded));
	decoded.magic = get_le32(encoded + 0);
	decoded.version = get_le32(encoded + 4);
	decoded.header_len = get_le32(encoded + 8);
	decoded.flags = get_le32(encoded + 12);
	decoded.timeline = get_le32(encoded + 16);
	decoded.payload_len = get_le32(encoded + 20);
	decoded.segment_no = get_le64(encoded + 24);
	decoded.start_lsn = get_le64(encoded + 32);
	decoded.payload_crc = get_le32(encoded + 40);
	decoded.header_crc = get_le32(encoded + 44);
	decoded.segment_size = get_le64(encoded + 48);
	decoded.xlp_magic = get_le16(encoded + 56);
	decoded.xlp_info = get_le16(encoded + 58);
	decoded.xlp_seg_size = get_le32(encoded + 60);
	if (decoded.magic != PS_WAL_SEGMENT_MAGIC ||
		!version_known(decoded.version) ||
		decoded.header_len != PS_WAL_SEGMENT_HEADER_BYTES ||
		decoded.flags != 0 || decoded.payload_len == 0 ||
		!segment_identity_valid(&decoded) ||
		(decoded.version == PS_WAL_SEGMENT_LEGACY_VERSION &&
		 (decoded.xlp_magic != 0 || decoded.xlp_info != 0 ||
		  decoded.xlp_seg_size != 0)) ||
		decoded.header_crc != header_crc(&decoded))
		return -1;
	*header = decoded;
	return 0;
}
