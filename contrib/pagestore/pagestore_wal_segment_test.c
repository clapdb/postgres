#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "pagestore_wal_segment.h"

static int run;
static int failed;

static void
check(int condition, const char *name)
{
	run++;
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failed++;
	}
}

int
main(void)
{
	static const unsigned char v1_fixture[PS_WAL_SEGMENT_HEADER_BYTES] = {
		0x31, 0x47, 0x53, 0x57, 0x01, 0x00, 0x00, 0x00,
		0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x07, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00,
		0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00,
		0x65, 0xad, 0x13, 0x09, 0xb6, 0x31, 0x29, 0x23,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	};
	/*
	 * A payload that begins like a PostgreSQL WAL segment: a long page header
	 * (xlp_magic 0xD120, xlp_info with XLP_LONG_HEADER, a 16 MiB
	 * xlp_seg_size at offset 32) followed by arbitrary bytes.
	 */
	unsigned char payload[64];
	PsWalSegmentHeader header;
	PsWalSegmentHeader damaged;
	PsWalSegmentHeader decoded;
	unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	union
	{
		PsWalSegmentHeader header;
		unsigned char encoded[PS_WAL_SEGMENT_HEADER_BYTES];
	} alias;

	for (size_t i = 0; i < sizeof(payload); i++)
		payload[i] = (unsigned char) i;
	payload[0] = 0x20;
	payload[1] = 0xd1;			/* xlp_magic 0xD120 */
	payload[2] = 0x02;
	payload[3] = 0x00;			/* xlp_info XLP_LONG_HEADER */
	payload[32] = 0x00;
	payload[33] = 0x00;
	payload[34] = 0x00;
	payload[35] = 0x01;			/* xlp_seg_size 16 MiB */
	check(ps_wal_segment_seal(&header, 7, 11, 0xb000000, 16 * 1024 * 1024, payload,
						  sizeof(payload)) == 0,
		  "seal a complete segment payload");
	check(header.timeline == 7 && header.segment_no == 11 &&
		  header.start_lsn == 0xb000000 &&
		  ps_wal_segment_validate(&header, payload, sizeof(payload)) == 0,
		  "validate persisted identity and checksums");
	check(header.version == PS_WAL_SEGMENT_VERSION &&
		  header.xlp_magic == 0xd120 && header.xlp_info == 0x0002 &&
		  header.xlp_seg_size == 16 * 1024 * 1024,
		  "the envelope records the payload's page magic, flags and segment size");
	check(ps_wal_segment_encode(&header, encoded) == 0 &&
		  ps_wal_segment_decode(&decoded, encoded, sizeof(encoded)) == 0 &&
		  decoded.timeline == 7 && decoded.segment_no == 11 &&
		  decoded.start_lsn == 0xb000000 &&
		  decoded.xlp_magic == 0xd120 && decoded.xlp_info == 0x0002 &&
		  decoded.xlp_seg_size == 16 * 1024 * 1024,
		  "fixed little-endian header round-trips");
	check(encoded[56] == 0x20 && encoded[57] == 0xd1 &&
		  encoded[58] == 0x02 && encoded[59] == 0x00 &&
		  encoded[60] == 0x00 && encoded[63] == 0x01,
		  "the payload identity occupies the bytes version 1 reserved");
	check(ps_wal_segment_decode(&decoded, v1_fixture, sizeof(v1_fixture)) == 0 &&
		  decoded.version == PS_WAL_SEGMENT_LEGACY_VERSION &&
		  decoded.segment_size == 16 * 1024 * 1024 &&
		  decoded.xlp_magic == 0 && decoded.xlp_seg_size == 0,
		  "a version-1 golden header still decodes, with no payload identity");
	{
		unsigned char v1_nonzero[PS_WAL_SEGMENT_HEADER_BYTES];
		PsWalSegmentHeader v1_header;
		unsigned char short_payload[PS_WAL_XLP_SHORT_HEADER_BYTES];

		memcpy(v1_nonzero, v1_fixture, sizeof(v1_nonzero));
		v1_nonzero[56] = 0x20;
		check(ps_wal_segment_decode(&decoded, v1_nonzero, sizeof(v1_nonzero)) != 0,
			  "a version-1 header with bytes in the reserved area is rejected");
		check(ps_wal_segment_decode(&v1_header, v1_fixture, sizeof(v1_fixture)) == 0 &&
			  ps_wal_segment_validate(&v1_header, payload, 32) != 0,
			  "a version-1 header validates its checksums, not a payload identity");
		memcpy(short_payload, payload, sizeof(short_payload));
		short_payload[2] = 0x00;	/* short page header: no segment size */
		check(ps_wal_segment_seal(&damaged, 7, 11, 0xb000000, 16 * 1024 * 1024,
								  short_payload, sizeof(short_payload)) == 0 &&
			  damaged.xlp_magic == 0xd120 && damaged.xlp_info == 0 &&
			  damaged.xlp_seg_size == 0,
			  "a payload starting with a short page header records no segment size");
		check(ps_wal_segment_seal(&damaged, 7, 11, 0xb000000, 16 * 1024 * 1024,
								  short_payload, 8) != 0,
			  "a payload shorter than a page header cannot be sealed");
	}
	check(ps_wal_segment_seal(&damaged, 7, 11, 11 * 1024 * 1024,
						  1024 * 1024, payload, sizeof(payload)) == 0 &&
		  damaged.segment_size == 1024 * 1024,
		  "configured one-megabyte WAL segment identity is preserved");
	alias.header = header;
	check(ps_wal_segment_encode(&alias.header, alias.encoded) == 0 &&
		  ps_wal_segment_decode(&alias.header, alias.encoded,
								sizeof(alias.encoded)) == 0 &&
		  alias.header.segment_no == 11 && alias.header.start_lsn == 0xb000000,
		  "overlapping encode and decode buffers are supported");
	check(encoded[0] == 0x31 && encoded[1] == 0x47 &&
		  encoded[2] == 0x53 && encoded[3] == 0x57 &&
		  encoded[24] == 11 && encoded[25] == 0,
		  "persisted integers use little-endian byte order");
	check(ps_wal_segment_seal(&header, 7, UINT64_C(1) << 39, UINT64_C(1) << 63,
						  16 * 1024 * 1024,
						  payload, sizeof(payload)) == 0 &&
		  header.segment_no == (UINT64_C(1) << 39),
		  "large segment identities are preserved");
	check(ps_wal_segment_seal(&damaged, 7, 11, 0x1000000, 16 * 1024 * 1024, payload,
						  sizeof(payload)) != 0,
		  "mismatched segment number and start LSN are rejected");

	damaged = header;
	damaged.timeline++;
	check(ps_wal_segment_validate(&damaged, payload, sizeof(payload)) != 0,
		  "header checksum rejects identity corruption");
	damaged = header;
	damaged.xlp_magic = 0xd121;
	check(ps_wal_segment_validate(&damaged, payload, sizeof(payload)) != 0,
		  "header checksum rejects a changed payload identity");
	{
		unsigned char swapped[sizeof(payload)];

		memcpy(swapped, payload, sizeof(swapped));
		swapped[1] = 0xd2;
		check(ps_wal_segment_validate(&header, swapped, sizeof(swapped)) != 0,
			  "a payload that no longer matches the envelope is rejected");
	}
	payload[5] ^= 0xff;
	check(ps_wal_segment_validate(&header, payload, sizeof(payload)) != 0,
		  "payload checksum rejects byte corruption");
	payload[5] ^= 0xff;
	check(ps_wal_segment_validate(&header, payload, sizeof(payload) - 1) != 0,
		  "truncated payload is rejected");
	check(ps_wal_segment_validate(&header, payload, sizeof(payload) + 1) != 0,
		  "trailing payload bytes are rejected");
	check(ps_wal_segment_seal(&header, 0, 0, 0, 16 * 1024 * 1024, payload,
						  8) != 0,
		  "timeline zero is rejected");
	check(ps_wal_segment_seal(&header, 1, 0, UINT64_MAX - 3,
						  16 * 1024 * 1024, payload, 8) != 0,
		  "overflowing end LSN is rejected");
	check(ps_wal_segment_seal(&header, 1, 0, 0, 16 * 1024 * 1024, NULL, 1) != 0 &&
		  ps_wal_segment_seal(&header, 1, 0, 0, 16 * 1024 * 1024, payload, 0) != 0,
		  "empty or missing payload is rejected");
	check(ps_wal_segment_seal(&header, 1, 0, 0, 16 * 1024 * 1024, &header,
							 sizeof(header)) != 0,
		  "payload overlapping the output header is rejected");

	printf("pagestore_wal_segment_test: %d checks, %d failed\n", run, failed);
	return failed != 0;
}
