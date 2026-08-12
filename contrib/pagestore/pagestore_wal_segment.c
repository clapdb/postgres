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
	put_le64(out + 48, header->reserved64[0]);
	put_le64(out + 56, header->reserved64[1]);
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
	return header->start_lsn % PS_WAL_SEGMENT_PAYLOAD_BYTES == 0 &&
		header->start_lsn / PS_WAL_SEGMENT_PAYLOAD_BYTES == header->segment_no &&
		header->start_lsn + header->payload_len >= header->start_lsn &&
		header->payload_len <= PS_WAL_SEGMENT_PAYLOAD_BYTES;
}

int
ps_wal_segment_seal(PsWalSegmentHeader *header, uint32_t timeline,
					uint64_t segment_no, uint64_t start_lsn,
					const void *payload, uint32_t payload_len)
{
	if (header == NULL || payload_len == 0 || payload == NULL ||
		payload_len > PS_WAL_SEGMENT_PAYLOAD_BYTES)
		return -1;
	memset(header, 0, sizeof(*header));
	header->magic = PS_WAL_SEGMENT_MAGIC;
	header->version = PS_WAL_SEGMENT_VERSION;
	header->header_len = PS_WAL_SEGMENT_HEADER_BYTES;
	header->timeline = timeline;
	header->segment_no = segment_no;
	header->start_lsn = start_lsn;
	header->payload_len = payload_len;
	if (!segment_identity_valid(header))
		return -1;
	header->payload_crc = payload_crc(payload, payload_len);
	header->header_crc = header_crc(header);
	return 0;
}

int
ps_wal_segment_validate(const PsWalSegmentHeader *header,
						const void *payload, uint32_t payload_len)
{
	if (header == NULL || payload == NULL ||
		header->magic != PS_WAL_SEGMENT_MAGIC ||
		header->version != PS_WAL_SEGMENT_VERSION ||
		header->header_len != PS_WAL_SEGMENT_HEADER_BYTES || header->flags != 0 ||
		header->payload_len == 0 ||
		header->payload_len != payload_len ||
		!segment_identity_valid(header) ||
		header->reserved64[0] != 0 ||
		header->reserved64[1] != 0 || header->header_crc != header_crc(header))
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
	decoded.reserved64[0] = get_le64(encoded + 48);
	decoded.reserved64[1] = get_le64(encoded + 56);
	if (decoded.magic != PS_WAL_SEGMENT_MAGIC ||
		decoded.version != PS_WAL_SEGMENT_VERSION ||
		decoded.header_len != PS_WAL_SEGMENT_HEADER_BYTES ||
		decoded.flags != 0 || decoded.payload_len == 0 ||
		!segment_identity_valid(&decoded) ||
		decoded.reserved64[0] != 0 || decoded.reserved64[1] != 0 ||
		decoded.header_crc != header_crc(&decoded))
		return -1;
	*header = decoded;
	return 0;
}
