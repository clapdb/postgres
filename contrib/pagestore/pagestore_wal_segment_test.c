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
	unsigned char payload[32];
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
	check(ps_wal_segment_seal(&header, 7, 11, 0xb000000, 16 * 1024 * 1024, payload,
						  sizeof(payload)) == 0,
		  "seal a complete segment payload");
	check(header.timeline == 7 && header.segment_no == 11 &&
		  header.start_lsn == 0xb000000 &&
		  ps_wal_segment_validate(&header, payload, sizeof(payload)) == 0,
		  "validate persisted identity and checksums");
	check(ps_wal_segment_encode(&header, encoded) == 0 &&
		  ps_wal_segment_decode(&decoded, encoded, sizeof(encoded)) == 0 &&
		  decoded.timeline == 7 && decoded.segment_no == 11 &&
		  decoded.start_lsn == 0xb000000,
		  "fixed little-endian header round-trips");
	check(memcmp(encoded, v1_fixture, sizeof(encoded)) == 0 &&
		  ps_wal_segment_decode(&decoded, v1_fixture, sizeof(v1_fixture)) == 0 &&
		  decoded.segment_size == 16 * 1024 * 1024,
		  "version-1 header matches and independently decodes its golden fixture");
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
	damaged.reserved64 = 1;
	damaged.header_crc = header.header_crc;
	check(ps_wal_segment_validate(&damaged, payload, sizeof(payload)) != 0,
		  "reserved format fields must remain zero");
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
