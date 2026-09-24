/*-------------------------------------------------------------------------
 *
 * fuzz_crc_fixup.c
 *	  Per-format checksum recomputation.  See fuzz_crc_fixup.h.
 *
 * Every record layout below is copied from the product source named in its
 * comment (never guessed), the same convention the existing
 * pagestore_*_test.c white-box tests already use for a format whose struct
 * is private to one .c file.  Two generic hash primitives cover every
 * format in this codebase:
 *   - fnv1a_step(): the project's standard FNV-1a (offset basis
 *     2166136261, prime 16777619), identical in pagestore_core.c's fnv(),
 *     pagestore_manifest.c's manifest_fnv1a(), pagestore_retention.c's
 *     retention_fnv1a(), pagestore_layer.c's img_crc(),
 *     pagestore_wal_segment.c's fnv1a(), pagestore_wal_store.c's
 *     metadata_hash(), storage_posix.c's posix_walidx_watermark_crc(), and
 *     pagestore_walidx_snapshot.c/pagestore_forkmeta_snapshot.c's fnv1a().
 *   - ps_crc32c_update()/PS_CRC32C_INIT/PS_CRC32C_FIN(): PostgreSQL's
 *     CRC-32C, from pagestore_artifact_format.h (the one public, shared
 *     implementation -- used directly here, not reimplemented).
 *
 *-------------------------------------------------------------------------
 */
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_prune.h"
#include "fuzz_crc_fixup.h"
#include "fuzz_common.h"

/* ---- generic hash primitives ------------------------------------------ */

static uint32_t
fnv1a_step(uint32_t h, const void *p, size_t n)
{
	const unsigned char *b = p;

	for (size_t i = 0; i < n; i++)
	{
		h ^= b[i];
		h *= 16777619u;
	}
	return h;
}
#define FNV1A_INIT 2166136261u

static uint32_t
get_le32(const unsigned char *p)
{
	return (uint32_t) p[0] | (uint32_t) p[1] << 8 |
		(uint32_t) p[2] << 16 | (uint32_t) p[3] << 24;
}

static void
put_le32(unsigned char *p, uint32_t v)
{
	p[0] = (unsigned char) v;
	p[1] = (unsigned char) (v >> 8);
	p[2] = (unsigned char) (v >> 16);
	p[3] = (unsigned char) (v >> 24);
}

static uint64_t
get_le64(const unsigned char *p)
{
	return (uint64_t) get_le32(p) | (uint64_t) get_le32(p + 4) << 32;
}

/* ---- manifest: layers.manifest (pagestore_manifest.c) -----------------
 * Sequence of [PsManifestRecord header (20 bytes: magic,version,type,len,
 * crc) | 'len' bytes of payload].  crc = FNV-1a over the header's first 16
 * bytes (offsetof(PsManifestRecord, crc)), then the payload
 * (manifest_record_crc()).  Self-sized: len picks where the next record
 * starts, so a fixup must recompute crc using the record's own (possibly
 * fuzzed) len, clamped to what is actually left in the buffer. */
static void
fixup_manifest(uint8_t *buf, size_t len)
{
	size_t		off = 0;

	while (off + 20 <= len)
	{
		uint32_t	declared_len = get_le32(buf + off + 12);
		size_t		payload_len = declared_len;
		uint32_t	crc;

		if (payload_len > len - off - 20)
			payload_len = len - off - 20;
		crc = fnv1a_step(FNV1A_INIT, buf + off, 16);
		if (payload_len > 0)
			crc = fnv1a_step(crc, buf + off + 20, payload_len);
		put_le32(buf + off + 16, crc);
		/* Advance by the record's own declared size (matching how the
		 * reader frames records); a declared len that ran past the buffer
		 * consumed the rest of it as payload above, so the loop ends. */
		if (declared_len > len - off - 20)
			break;
		off += 20 + declared_len;
	}
}

/* ---- forkmeta (pagestore_core.c ForkMetaRecV2) -------------------------
 * Fixed 64-byte records.  FORK_META_V3_MAGIC ("FKM3") records carry a
 * CRC-24 (OpenPGP polynomial) in the 3 low bytes of `pad`, computed over
 * every byte before `pad` (fork_meta_rec_crc24()/fork_meta_rec_seal()).
 * FORK_META_V2_MAGIC ("FKM2") records carry no checksum (left untouched).
 * Mirrors ForkMetaRecV2 exactly (same field order/types as
 * pagestore_core.c), using the shared PsKey type so the layout matches by
 * construction. */
typedef struct FuzzForkMetaRecV2
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	timeline;
	PsKey		key;
	uint64_t	lsn;
	uint64_t	admission_seq;
	uint64_t	order_id;
	uint32_t	nblocks;
	uint8_t		kind;
	uint8_t		pad[3];
} FuzzForkMetaRecV2;

#define FORK_META_V3_MAGIC_LOCAL 0x334d4b46u /* "FKM3" */

static uint32_t
crc24_openpgp(const unsigned char *bytes, size_t n)
{
	uint32_t	crc = 0xB704CEu;

	for (size_t i = 0; i < n; i++)
	{
		crc ^= (uint32_t) bytes[i] << 16;
		for (int bit = 0; bit < 8; bit++)
		{
			crc <<= 1;
			if (crc & 0x1000000u)
				crc ^= 0x1864CFBu;
		}
	}
	return crc & 0xFFFFFFu;
}

static void
fixup_forkmeta(uint8_t *buf, size_t len)
{
	size_t		stride = sizeof(FuzzForkMetaRecV2);
	size_t		crc_off = offsetof(FuzzForkMetaRecV2, pad);

	for (size_t off = 0; off + stride <= len; off += stride)
	{
		if (get_le32(buf + off) == FORK_META_V3_MAGIC_LOCAL)
		{
			uint32_t	crc = crc24_openpgp(buf + off, crc_off);

			buf[off + crc_off + 0] = (uint8_t) (crc >> 16);
			buf[off + crc_off + 1] = (uint8_t) (crc >> 8);
			buf[off + crc_off + 2] = (uint8_t) crc;
		}
	}
}

/* ---- retention.meta (pagestore_retention.c PsRetentionRecord) ---------
 * Fixed-size records: magic,version,type,len (16 bytes) + PsRetentionPin
 * (40 bytes, shared type) + crc(4) + pad(4) = 64 bytes.  crc = FNV-1a over
 * offsetof(crc) = 56 leading bytes (retention_record_crc()). */
typedef struct FuzzRetentionRecord
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	type;
	uint32_t	len;
	PsRetentionPin pin;
	uint32_t	crc;
	uint32_t	pad;
} FuzzRetentionRecord;

static void
fixup_retention_meta(uint8_t *buf, size_t len)
{
	size_t		stride = sizeof(FuzzRetentionRecord);
	size_t		crc_off = offsetof(FuzzRetentionRecord, crc);

	for (size_t off = 0; off + stride <= len; off += stride)
	{
		uint32_t	crc = fnv1a_step(FNV1A_INIT, buf + off, crc_off);

		put_le32(buf + off + crc_off, crc);
	}
}

/* ---- retention.state (pagestore_retention.c PsRetentionState) ---------
 * Single fixed struct: magic,version(8) + nrecords(8) + log_hash(4) +
 * crc(4).  crc = FNV-1a over offsetof(crc) leading bytes
 * (retention_state_crc()). */
typedef struct FuzzRetentionState
{
	uint32_t	magic;
	uint32_t	version;
	uint64_t	nrecords;
	uint32_t	log_hash;
	uint32_t	crc;
} FuzzRetentionState;

static void
fixup_retention_state(uint8_t *buf, size_t len)
{
	size_t		crc_off = offsetof(FuzzRetentionState, crc);

	if (len < sizeof(FuzzRetentionState))
		return;
	put_le32(buf + crc_off, fnv1a_step(FNV1A_INIT, buf, crc_off));
}

/* ---- timelines (pagestore_core.c TimelineRecEvent) ---------------------
 * Fixed 60-byte self-sized records (the V1/legacy shorter shape is not
 * fixed up here -- both stay readable, this just targets the current write
 * shape, still by far the common case in a mutated corpus of V2 seeds).
 * crc = FNV-1a over the whole record with crc temporarily zeroed
 * (timeline_event_crc()), so `reserved` (after crc) is part of the hash
 * input exactly as-is. */
typedef struct FuzzTimelineRecEvent
{
	uint32_t	magic;
	uint32_t	rec_len;
	uint32_t	kind;
	uint32_t	id;
	int32_t		parent;
	uint32_t	state;
	uint64_t	branch_lsn;
	uint64_t	incarnation;
	uint64_t	parent_incarnation;
	uint32_t	crc;
	uint32_t	reserved;
} FuzzTimelineRecEvent;

static void
fixup_timelines(uint8_t *buf, size_t len)
{
	size_t		stride = sizeof(FuzzTimelineRecEvent);
	size_t		crc_off = offsetof(FuzzTimelineRecEvent, crc);

	for (size_t off = 0; off + stride <= len; off += stride)
	{
		uint32_t	saved = get_le32(buf + off + crc_off);
		uint32_t	crc;

		put_le32(buf + off + crc_off, 0);
		crc = fnv1a_step(FNV1A_INIT, buf + off, stride);
		(void) saved;
		put_le32(buf + off + crc_off, crc);
	}
}

/* ---- page-prune.frontiers / walidx-prune.frontiers (pagestore_core.c) -
 * One fixed struct per file: magic,version + a big fixed entries array +
 * crc.  crc = FNV-1a over offsetof(crc) leading bytes
 * (page_frontier_crc()/walidx_frontier_crc()).  Mirrors PsPageFrontierState
 * / PsWalIdxFrontierState using the shared PsPruneFence type so padding
 * matches by construction; sizeof()/offsetof() (not hand-computed byte
 * offsets) do the layout-sensitive work. */
#define PS_FUZZ_FRONTIER_SLOTS 2
typedef struct FuzzPageFrontierEntry
{
	uint64_t	incarnation;
	PsPruneFence fence;
} FuzzPageFrontierEntry;
typedef struct FuzzPageFrontierState
{
	uint32_t	magic;
	uint32_t	version;
	FuzzPageFrontierEntry entries[1024][PS_FUZZ_FRONTIER_SLOTS];
	uint32_t	crc;
} FuzzPageFrontierState;

typedef struct FuzzWalIdxFrontierEntry
{
	uint64_t	incarnation;
	uint64_t	frontier;
} FuzzWalIdxFrontierEntry;
typedef struct FuzzWalIdxFrontierState
{
	uint32_t	magic;
	uint32_t	version;
	FuzzWalIdxFrontierEntry entries[1024][PS_FUZZ_FRONTIER_SLOTS];
	uint32_t	crc;
} FuzzWalIdxFrontierState;

static void
fixup_page_frontier(uint8_t *buf, size_t len)
{
	size_t		crc_off = offsetof(FuzzPageFrontierState, crc);

	if (len < sizeof(FuzzPageFrontierState))
		return;
	put_le32(buf + crc_off, fnv1a_step(FNV1A_INIT, buf, crc_off));
}

static void
fixup_walidx_frontier(uint8_t *buf, size_t len)
{
	size_t		crc_off = offsetof(FuzzWalIdxFrontierState, crc);

	if (len < sizeof(FuzzWalIdxFrontierState))
		return;
	put_le32(buf + crc_off, fnv1a_step(FNV1A_INIT, buf, crc_off));
}

/* ---- walidx_<tl>_<shard>[_e<epoch>] (pagestore_core.c WalIdxRec family) -
 * A log of self-sized records that can be any of three shapes, told apart
 * by magic + declared rec_len exactly as the reader does
 * (WALIDX_MAGIC=="WIDX" with rec_len 64 -> WalIdxRec, rec_len 56 ->
 * legacy WalIdxRecV1; WALIDX_PROGRESS_MAGIC=="WIPG" -> WalIdxProgressRec,
 * fixed 1080 bytes).  Every shape's crc sits at byte offset 8 and is
 * FNV-1a over the whole record with crc zeroed
 * (walidx_rec_crc()/walidx_rec_v1_crc()/walidx_progress_crc()); an
 * unrecognized magic/rec_len combination stops the walk (leaves the rest
 * of the buffer as pure fuzzer output, same as the manifest walk). */
#define WALIDX_MAGIC_LOCAL 0x57494458u			/* "WIDX" */
#define WALIDX_PROGRESS_MAGIC_LOCAL 0x57495047u	/* "WIPG" */
#define WALIDX_REC_BYTES 64u
#define WALIDX_REC_V1_BYTES 56u
#define WALIDX_PROGRESS_BYTES 1080u /* 4*6 + 8+8+16 + PS_MAX_CHANNELS(128)*8 */

static void
fixup_walidx_log(uint8_t *buf, size_t len)
{
	size_t		off = 0;

	while (off + 8 <= len)
	{
		uint32_t	magic = get_le32(buf + off);
		uint32_t	rec_len = get_le32(buf + off + 4);
		size_t		stride;

		if (magic == WALIDX_MAGIC_LOCAL && rec_len == WALIDX_REC_BYTES)
			stride = WALIDX_REC_BYTES;
		else if (magic == WALIDX_MAGIC_LOCAL && rec_len == WALIDX_REC_V1_BYTES)
			stride = WALIDX_REC_V1_BYTES;
		else if (magic == WALIDX_PROGRESS_MAGIC_LOCAL)
			stride = WALIDX_PROGRESS_BYTES;
		else
			break;
		if (off + stride > len)
			break;
		put_le32(buf + off + 8, 0);
		put_le32(buf + off + 8, fnv1a_step(FNV1A_INIT, buf + off, stride));
		off += stride;
	}
}

/* ---- walidx_<tl>_<shard>_e<epoch>.size (storage_posix.c
 * PosixWalIdxWatermark) -- fixed 24 bytes: magic(8),length(8),crc(4),
 * reserved(4).  crc = FNV-1a over the whole struct with crc zeroed
 * (posix_walidx_watermark_crc()). */
typedef struct FuzzWalIdxWatermark
{
	uint64_t	magic;
	uint64_t	length;
	uint32_t	crc;
	uint32_t	reserved;
} FuzzWalIdxWatermark;

static void
fixup_walidx_watermark(uint8_t *buf, size_t len)
{
	size_t		crc_off = offsetof(FuzzWalIdxWatermark, crc);

	if (len < sizeof(FuzzWalIdxWatermark))
		return;
	put_le32(buf + crc_off, 0);
	put_le32(buf + crc_off,
			 fnv1a_step(FNV1A_INIT, buf, sizeof(FuzzWalIdxWatermark)));
}

/* ---- wal_segments_<tl>/wal_store_identity_v1 (pagestore_wal_store.c) --
 * Fixed 64-byte encoding: crc (metadata_crc()) at byte offset 48, FNV-1a
 * over the whole 64 bytes with bytes 48..51 zeroed. */
static void
fixup_wal_store_identity(uint8_t *buf, size_t len)
{
	unsigned char copy[64];

	if (len != 64)
		return;					/* only the exact identity encoding is fixed up */
	memcpy(copy, buf, 64);
	memset(copy + 48, 0, 4);
	put_le32(buf + 48, fnv1a_step(FNV1A_INIT, copy, 64));
}

/* ---- wal_segments_<tl>/walv1_* (pagestore_wal_segment.c
 * PsWalSegmentHeader) -- 64-byte header (offsets from encode_fields():
 * payload_crc@40, header_crc@44) + payload.  payload_crc = FNV-1a over the
 * payload (payload_crc()); header_crc = FNV-1a over the 64-byte header
 * with header_crc zeroed (header_crc()). */
static void
fixup_wal_segment(uint8_t *buf, size_t len)
{
	unsigned char header[64];
	uint32_t	payload_crc;
	uint32_t	header_crc;

	if (len < 64)
		return;
	payload_crc = fnv1a_step(FNV1A_INIT, buf + 64, len - 64);
	put_le32(buf + 40, payload_crc);
	memcpy(header, buf, 64);
	memset(header + 44, 0, 4);
	header_crc = fnv1a_step(FNV1A_INIT, header, 64);
	put_le32(buf + 44, header_crc);
}

/* ---- .pagestore-nshards (pagestore_core.c, "PSS2 %u %08x\n") -----------
 * Text format: "PSS2 <count> <crc32c-of-count-hex>\n".  crc is PostgreSQL
 * CRC-32C of the 4-byte native-endian count
 * (publish_store_shard_count(): ~ps_crc32c_update(UINT32_MAX,&current,4),
 * i.e. PS_CRC32C_FIN() since FIN is XOR 0xffffffff == bitwise NOT here).
 * Only fixed up when the buffer already parses as "PSS2 <digits> <hex>";
 * a mutation that broke the textual shape explores that path instead, same
 * as every other format's "give up cleanly" fallback. */
static void
fixup_store_config(uint8_t *buf, size_t len)
{
	char		text[64];
	unsigned int count;
	unsigned int old_crc;
	int			consumed = 0;
	uint32_t	crc;
	char		out[64];
	int			n;

	if (len == 0 || len >= sizeof(text))
		return;
	memcpy(text, buf, len);
	text[len] = '\0';
	if (sscanf(text, "PSS2 %u %x%n", &count, &old_crc, &consumed) != 2)
		return;
	/* Require the parse to have consumed a sane prefix so we do not rewrite
	 * trailing garbage into something that looks well-formed. */
	if (consumed <= 0 || (size_t) consumed > len)
		return;
	crc = PS_CRC32C_FIN(ps_crc32c_update(PS_CRC32C_INIT, &count, sizeof(count)));
	n = snprintf(out, sizeof(out), "PSS2 %u %08x", count, crc);
	if (n <= 0 || (size_t) n > len)
		return;
	memcpy(buf, out, (size_t) n);
}

/* ---- layer_<shard>_<id> (pagestore_layer.c/.h PsImgFooter) -------------
 * [page data][index][PsImgFooter (32 bytes, trailing)].  data_crc/
 * index_crc = FNV-1a (img_crc()) over the data and index sections; index_
 * off (in the footer, left as whatever the fuzzer produced) is the
 * boundary between them.  Only fixed up when index_off is in range --
 * exactly the "give up, still a valid fuzz input" fallback used
 * everywhere else in this file. */
typedef struct FuzzImgFooter
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	page_size;
	uint32_t	nrecs;
	uint64_t	index_off;
	uint32_t	data_crc;
	uint32_t	index_crc;
} FuzzImgFooter;

static void
fixup_image_layer(uint8_t *buf, size_t len)
{
	size_t		footer_bytes = sizeof(FuzzImgFooter);
	uint8_t    *footer;
	uint64_t	index_off;
	uint32_t	data_crc;
	uint32_t	index_crc;

	if (len < footer_bytes)
		return;
	footer = buf + (len - footer_bytes);
	index_off = get_le64(footer + offsetof(FuzzImgFooter, index_off));
	if (index_off > len - footer_bytes)
		return;					/* out of range: leave fully fuzzer-controlled */
	data_crc = fnv1a_step(FNV1A_INIT, buf, (size_t) index_off);
	index_crc = fnv1a_step(FNV1A_INIT, buf + index_off,
							(len - footer_bytes) - (size_t) index_off);
	put_le32(footer + offsetof(FuzzImgFooter, data_crc), data_crc);
	put_le32(footer + offsetof(FuzzImgFooter, index_crc), index_crc);
}

/* ---- forkmeta_snapshots/forkmeta_manifest_v1 (pagestore_forkmeta_
 * snapshot.c encode_record()) -- fixed 80-byte record.  Byte offsets are
 * the product's own explicit put_le32/put_le64 layout (not a struct), so
 * they are reproduced numerically rather than mirrored: magic@0,
 * version@4, header_bytes@8, generation@16(8), cutoff_lsn@24(8),
 * cutoff_admission_seq@32(8), checkpoint.len@40(8), checkpoint.crc@48(4),
 * tail.len@52(8), tail.crc@60(4), crc@64(4) = FNV-1a over the whole 80
 * bytes with crc zeroed. */
#define FORKMETA_SNAPSHOT_HEADER_BYTES_LOCAL 80u

static void
fixup_forkmeta_snapshot_manifest(uint8_t *buf, size_t len)
{
	if (len != FORKMETA_SNAPSHOT_HEADER_BYTES_LOCAL)
		return;
	put_le32(buf + 64, 0);
	put_le32(buf + 64, fnv1a_step(FNV1A_INIT, buf, len));
}

/*
 * forkmeta_snapshots/forkmeta_checkpoint_v1_* and .../forkmeta_tail_v1_*:
 * these payload files carry no checksum of their own -- open_validated_part()
 * in pagestore_forkmeta_snapshot.c hashes the raw file bytes against the
 * (len, crc) the sibling forkmeta_manifest_v1 record keeps for that part.
 * So the only way a mutated payload here reaches anything past "length or
 * checksum mismatch, part rejected" is to patch the sibling manifest's
 * record to match -- same fixed 80-byte layout as
 * fixup_forkmeta_snapshot_manifest() above, this time read from and
 * written back to the live directory, since the manifest is a different
 * file from the one this target mutates.
 */
static void
fixup_forkmeta_snapshot_part(const char *work_dir, uint8_t *buf, size_t len,
							  int is_tail)
{
	char		path[4096];
	unsigned char manifest[FORKMETA_SNAPSHOT_HEADER_BYTES_LOCAL];
	int			fd;
	uint32_t	part_crc;
	size_t		len_off = is_tail ? 52 : 40;
	size_t		crc_off = is_tail ? 60 : 48;

	if (snprintf(path, sizeof(path),
				 "%s/forkmeta_snapshots/forkmeta_manifest_v1", work_dir) >=
		(int) sizeof(path))
		return;
	fd = open(path, O_RDWR);
	if (fd < 0)
		return;
	if (read(fd, manifest, sizeof(manifest)) != (ssize_t) sizeof(manifest))
	{
		close(fd);
		return;
	}
	part_crc = fnv1a_step(FNV1A_INIT, buf, len);
	/* len is a 64-bit field on disk; this harness's inputs never approach
	 * that range, so the low 32 bits (put_le32) plus zeroed high half
	 * (already true after the read above only if the file already encoded
	 * a small length -- write the full 8 bytes explicitly instead). */
	for (unsigned i = 0; i < 8; i++)
		manifest[len_off + i] = (unsigned char) ((uint64_t) len >> (i * 8));
	put_le32(manifest + crc_off, part_crc);
	put_le32(manifest + 64, 0);
	put_le32(manifest + 64,
			 fnv1a_step(FNV1A_INIT, manifest, sizeof(manifest)));
	if (pwrite(fd, manifest, sizeof(manifest), 0) != (ssize_t) sizeof(manifest))
	{
		/* best-effort: an iteration that fails this write just runs
		 * without the cross-file fixup, same as any other skipped case */
	}
	close(fd);
}

/* ---- walidx_snapshots_<tl>/walidx_manifest_v1 (pagestore_walidx_
 * snapshot.c encode_manifest()) -- header_bytes(64) + nshards*entry_bytes
 * (16) bytes, whatever the buffer's actual (possibly fuzzer-resized)
 * length is; crc@48 = FNV-1a over the whole buffer with crc zeroed
 * (read_prepared() checks stored_crc against fnv1a(...) over the buffer's
 * declared expected_len, but hashing "whatever is actually here" is the
 * only definition that make sense once nshards itself may have been
 * mutated -- see the file's own comment on this). */
static void
fixup_walidx_snapshot_manifest(uint8_t *buf, size_t len)
{
	if (len < 52)
		return;					/* need through the crc field itself */
	put_le32(buf + 48, 0);
	put_le32(buf + 48, fnv1a_step(FNV1A_INIT, buf, len));
}

/*
 * walidx_snapshots_<tl>/walidxg1_<generation>_<shard> (pagestore_core.c
 * walidx_snapshot_encode_header(), WALIDX_SNAPSHOT_PAYLOAD_BYTES == 72):
 * fixed 72-byte header + a WalIdxRec array payload.  decode_header() (the
 * reader) checks the header's crc *and* five identity fields (timeline,
 * shard, generation, start_lsn, end_lsn) against values the caller already
 * trusts from elsewhere (the sibling manifest's generation/start_lsn/
 * end_lsn plus which shard/timeline is being recovered) -- getting only
 * the crc right still means instant rejection on an identity mismatch, so
 * the mutated payload's record bytes (after the header) never get parsed.
 * To actually reach that record-parsing loop, this pins the header's
 * identity fields to the *template* fixture's real values (read from this
 * harness's in-memory template cache -- real captured values, not
 * invented ones) for timeline 0/shard 0, which is what every corpus seed
 * here already is, and leaves nrecords / the record payload entirely
 * fuzzer-controlled before recomputing the header crc. */
static void
fixup_walidx_snapshot_shard(uint8_t *buf, size_t len)
{
	const uint8_t *manifest;
	size_t		manifest_len;
	uint64_t	generation;
	uint64_t	start_lsn;
	uint64_t	end_lsn;

	if (len < 72)
		return;
	manifest = ps_fuzz_template_lookup(
		"walidx_snapshots_0/walidx_manifest_v1", &manifest_len);
	if (manifest == NULL || manifest_len < 48)
		return;
	generation = get_le64(manifest + 24);
	start_lsn = get_le64(manifest + 32);
	end_lsn = get_le64(manifest + 40);

	put_le32(buf + 12, 0);		/* timeline 0 */
	put_le32(buf + 16, 0);		/* shard 0 */
	for (unsigned i = 0; i < 8; i++)
	{
		buf[24 + i] = (unsigned char) (generation >> (i * 8));	/* generation */
		buf[32 + i] = (unsigned char) (start_lsn >> (i * 8));		/* start_lsn */
		buf[40 + i] = (unsigned char) (end_lsn >> (i * 8));		/* end_lsn */
		buf[56 + i] = (unsigned char) (generation >> (i * 8));	/* log_epoch */
	}
	memset(buf + 48, 0, 8);		/* source_offset must be 0 */
	put_le32(buf + 64, 0);
	put_le32(buf + 64, fnv1a_step(FNV1A_INIT, buf, 72));
}

/* ---- dispatch ----------------------------------------------------------
 * wal_log (wal_<tl>, WalRecHdr) and page_segment (seg_*, SegRecHdr) carry
 * no checksum at all in this codebase -- their headers are magic/len/lsn
 * fields only, integrity coming solely from being append-only and
 * self-describing -- so there is nothing for a fixup to recompute; both
 * fall through untouched below. */
void
ps_fuzz_crc_fixup(const char *target_name, const char *work_dir,
				   uint8_t *buf, size_t len)
{
	if (target_name == NULL)
		return;
	if (strcmp(target_name, "manifest") == 0)
		fixup_manifest(buf, len);
	else if (strcmp(target_name, "forkmeta") == 0)
		fixup_forkmeta(buf, len);
	else if (strcmp(target_name, "retention_meta") == 0)
		fixup_retention_meta(buf, len);
	else if (strcmp(target_name, "retention_state") == 0)
		fixup_retention_state(buf, len);
	else if (strcmp(target_name, "timelines") == 0)
		fixup_timelines(buf, len);
	else if (strcmp(target_name, "page_frontier") == 0)
		fixup_page_frontier(buf, len);
	else if (strcmp(target_name, "walidx_frontier") == 0)
		fixup_walidx_frontier(buf, len);
	else if (strcmp(target_name, "walidx_log_epoch") == 0 ||
			 strcmp(target_name, "walidx_log_legacy") == 0)
		fixup_walidx_log(buf, len);
	else if (strcmp(target_name, "walidx_watermark") == 0)
		fixup_walidx_watermark(buf, len);
	else if (strcmp(target_name, "wal_store_identity") == 0)
		fixup_wal_store_identity(buf, len);
	else if (strcmp(target_name, "wal_segment") == 0)
		fixup_wal_segment(buf, len);
	else if (strcmp(target_name, "store_config") == 0)
		fixup_store_config(buf, len);
	else if (strcmp(target_name, "image_layer") == 0)
		fixup_image_layer(buf, len);
	else if (strcmp(target_name, "forkmeta_snapshot_manifest") == 0)
		fixup_forkmeta_snapshot_manifest(buf, len);
	else if (strcmp(target_name, "forkmeta_snapshot_checkpoint") == 0)
		fixup_forkmeta_snapshot_part(work_dir, buf, len, 0);
	else if (strcmp(target_name, "forkmeta_snapshot_tail") == 0)
		fixup_forkmeta_snapshot_part(work_dir, buf, len, 1);
	else if (strcmp(target_name, "walidx_snapshot_manifest") == 0)
		fixup_walidx_snapshot_manifest(buf, len);
	else if (strcmp(target_name, "walidx_snapshot_shard") == 0)
		fixup_walidx_snapshot_shard(buf, len);
	/* wal_log, page_segment, and any other/unknown target: no checksum to
	 * fix up; left untouched. */
}
