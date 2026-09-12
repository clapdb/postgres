#!/usr/bin/env python3
"""Persisted-format fixtures for the pagestore daemon.

``--capture`` builds a store that carries every POSIX persisted family with the
``fixture`` workload of ``pagestore_gc_crash_client``, waits for maintenance to
settle, and writes a deterministic tar of the store next to the compiled format
identities.  ``--check`` reopens the fixture with the current daemon, runs the
workload's verify oracle across a restart, fails when the compiled format
identities differ from the fixture's (a format change without a fixture
update), and then applies each declared mutation to a fresh copy and requires
the documented rejection.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pagestore_harness as harness  # noqa: E402

DAEMON_ARGS = [
    "--page-size", "8192", "--nshards", "1", "--storage", "posix",
    "--segment-size", "65536", "--flush-pages", "8", "--segment-gc", "0",
    "--wal-high-water-bytes", "8388608", "--wal-catch-up-bytes", "1",
    "--walidx-snapshot-bytes", "1",
]
DAEMON_ENV = {"PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES": "1024"}
# The seeding phase snapshots the WAL index as soon as it has bytes, which
# empties the epoch logs; the extension phase raises the trigger so the
# records appended after the cutover stay in the live epoch, and that is the
# configuration the archive is read back with.
SEED_ARGS = DAEMON_ARGS
DAEMON_ARGS = [
    arg if previous != "--walidx-snapshot-bytes" else "1048576"
    for previous, arg in zip([""] + SEED_ARGS, SEED_ARGS)
]
EXCLUDED = {".pagestore.lock"}
MANIFEST_NAME = "layers.manifest"
MANIFEST_MAGIC = 0x504D414E
MANIFEST_HEADER_BYTES = 20
FNV_INIT = 2166136261
# A relocated store rebases a local layer location whose recorded parent
# directory no longer exists, so the archive records the leaves under a path
# that cannot exist rather than under whichever directory captured them.
FIXTURE_LAYER_ROOT = "/nonexistent/pagestore-fixture/layers"
WALIDX_MAGICS = {0x57494458, 0x57495047}      # "WIDX" records, "WIPG" progress
# The shipped-WAL envelope (pagestore_wal_segment.h): a version-2 header
# records the payload's PostgreSQL identity at bytes 56..63 -- xlp_magic u16,
# xlp_info u16, xlp_seg_size u32, little-endian like the rest of the envelope
# -- copied from the WAL page header the payload begins with at byte 64
# (magic at 0, info at 2, and a long header's segment size where the
# writer's ABI put it: at 32 with 8-byte-aligned uint64, at 28 with 4-byte
# alignment; the store tells the layouts apart the way
# ps_wal_segment_payload_identity() does), which PostgreSQL wrote in host
# byte order.  The header CRC at 44 is FNV-1a over the 64 bytes with the CRC
# field zeroed.
WAL_SEGMENT_HEADER_BYTES = 64
WAL_SEGMENT_VERSION = 2
WAL_SEGMENT_IDENTITY_OFFSET = 56
WAL_SEGMENT_HEADER_CRC_OFFSET = 44
XLP_LONG_HEADER = 0x0002


def wal_long_header_sizes(payload: bytes) -> tuple[int, int, int]:
    """The (segment size, offset it sits at, WAL block size) a long WAL page
    header carries, recognizing the writer's ABI by the bytes: zeroed padding
    at 20 with plausible sizes at 32/36 is the 8-byte-aligned layout,
    plausible sizes at 28/32 the 4-byte one; (0, 0, 0) when neither."""
    def plausible_seg(size: int) -> bool:
        return 1 << 20 <= size <= 1 << 30 and size & (size - 1) == 0

    def plausible_blk(size: int) -> bool:
        return 1024 <= size <= 65536 and size & (size - 1) == 0

    pad, = struct.unpack_from("=I", payload, 20)
    seg8, blk8 = struct.unpack_from("=II", payload, 32)
    seg4, blk4 = struct.unpack_from("=II", payload, 28)
    if pad == 0 and plausible_seg(seg8) and plausible_blk(blk8):
        return seg8, 32, blk8
    if plausible_seg(seg4) and plausible_blk(blk4):
        return seg4, 28, blk4
    return 0, 0, 0
WATERMARK_SUFFIX = ".size"
SEG_HEADER_BYTES = {
    0x53454732: 48, 0x53454730: 48, 0x53454731: 48, 0x53454733: 48,
    0x53454734: 56, 0x53454735: 56, 0x53454736: 56,
    0x53454737: 64, 0x53454738: 64,
}
FORKMETA_RECORD_BYTES = 64
# the relation the extension phase creates and grows: addressing its two
# events by position would move with the archive
FORKMETA_TAIL_REL = 8000
FORKMETA_KIND_SET = 0
FORKMETA_KIND_CREATE = 1
INSPECTION_SCHEMA = harness.read_json(Path(__file__).resolve().parent / "inspection_schema.json")
STORE_TAR = "store.tar.gz"
FORMAT_JSON = "format.json"
FIXTURE_JSON = "fixture.json"

# Outcomes a mutated fixture may produce.
OPEN_REJECTED = "open_rejected"      # the daemon refuses to open the store
USE_REJECTED = "use_rejected"        # it opens, but the oracle or inspection fails closed
ACCEPTED = "accepted"                # it opens and the oracle passes
CRASHED = "daemon_crashed"           # the daemon died of a signal or exited under use; never expected


class ForeignPayload(Exception):
    """The fixture's payload was written for another PostgreSQL build: not a
    broken envelope, but not a fixture this build can check either."""


class FixtureError(Exception):
    pass


def bump_le32(path: Path, offset: int) -> None:
    data = bytearray(path.read_bytes())
    value = struct.unpack_from("<I", data, offset)[0]
    struct.pack_into("<I", data, offset, (value + 1) & 0xFFFFFFFF)
    path.write_bytes(data)


def flip_byte(path: Path, offset: int) -> None:
    data = bytearray(path.read_bytes())
    if offset < 0:
        offset += len(data)
    data[offset] ^= 0xFF
    path.write_bytes(data)


def reseal_wal_segment_header(path: Path, mutate: Callable[[bytearray], None]) -> None:
    """Change a shipped-WAL envelope and recompute its header CRC, so the
    change is not caught by the checksum but by what it now claims."""
    data = bytearray(path.read_bytes())
    header = data[:WAL_SEGMENT_HEADER_BYTES]
    mutate(header)
    header[WAL_SEGMENT_HEADER_CRC_OFFSET:WAL_SEGMENT_HEADER_CRC_OFFSET + 4] = b"\0\0\0\0"
    crc = fnv1a(bytes(header))
    header[WAL_SEGMENT_HEADER_CRC_OFFSET:WAL_SEGMENT_HEADER_CRC_OFFSET + 4] = struct.pack("<I", crc)
    data[:WAL_SEGMENT_HEADER_BYTES] = header
    path.write_bytes(bytes(data))


def wal_segment_payload_identity(path: Path) -> dict[str, int] | None:
    """The (recorded, carried) payload identity of one shipped-WAL segment,
    or None for a version-1 envelope, which records none."""
    data = path.read_bytes()
    if len(data) < WAL_SEGMENT_HEADER_BYTES + 40:
        raise FixtureError(f"{path} is shorter than a WAL envelope and page header")
    version = struct.unpack_from("<I", data, 4)[0]
    if version != WAL_SEGMENT_VERSION:
        return None
    magic, info, seg_size = struct.unpack_from("<HHI", data, WAL_SEGMENT_IDENTITY_OFFSET)
    payload = data[WAL_SEGMENT_HEADER_BYTES:]
    carried_magic, carried_info = struct.unpack_from("=HH", payload, 0)
    carried_seg, seg_offset, carried_blk = (
        wal_long_header_sizes(payload) if carried_info & XLP_LONG_HEADER else (0, 0, 0))
    if (magic, info, seg_size) != (carried_magic, carried_info, carried_seg):
        raise FixtureError(
            f"{path.name} records payload identity ({magic:#06x}, {info:#06x}, {seg_size}) "
            f"but its payload carries ({carried_magic:#06x}, {carried_info:#06x}, {carried_seg})"
        )
    return {"xlog_page_magic": magic, "xlp_info": info, "wal_segment_size": seg_size,
            "xlog_long_header_seg_size_offset": seg_offset, "xlog_blcksz": carried_blk}


def archive_payload_identity(store: Path) -> dict[str, int]:
    """The PostgreSQL identity the archive's shipped WAL carries: every
    version-2 envelope agrees on the page magic, and the one that begins a
    PostgreSQL segment names the segment size."""
    magics: set[int] = set()
    seg_sizes: set[int] = set()
    offsets: set[int] = set()
    blcksz: set[int] = set()
    for path in sorted(store.glob("wal_segments_*/walv1_*")):
        identity = wal_segment_payload_identity(path)
        if identity is None:
            continue
        magics.add(identity["xlog_page_magic"])
        if identity["wal_segment_size"]:
            seg_sizes.add(identity["wal_segment_size"])
            offsets.add(identity["xlog_long_header_seg_size_offset"])
            blcksz.add(identity["xlog_blcksz"])
    if len(magics) != 1 or len(seg_sizes) != 1 or len(offsets) != 1 or len(blcksz) != 1:
        raise FixtureError(
            f"the archive's shipped WAL carries page magics {sorted(map(hex, magics))}, "
            f"segment sizes {sorted(seg_sizes)}, long-header layouts {sorted(offsets)} and "
            f"block sizes {sorted(blcksz)}; a fixture names exactly one of each"
        )
    # the layout is the writer's ABI: a build whose XLogLongPageHeaderData
    # puts xlp_seg_size elsewhere reads these bytes differently; the block
    # size is a build option XLogReaderValidatePageHeader() rejects on
    return {"xlog_page_magic": magics.pop(), "wal_segment_size": seg_sizes.pop(),
            "xlog_long_header_seg_size_offset": offsets.pop(), "xlog_blcksz": blcksz.pop()}


def truncate_to(path: Path, size: int) -> None:
    data = path.read_bytes()
    if size < 0:
        size += len(data)
    path.write_bytes(data[:size])


def first_match(store: Path, pattern: str) -> Path:
    matches = sorted(store.glob(pattern))
    if not matches:
        raise FixtureError(f"fixture has no file matching {pattern!r}")
    return matches[0]


def forkmeta_record_offset(path: Path, rel: int, kind: int) -> int:
    """Where one fork-size event sits in the source log, found by the relation
    and the event it records rather than by its position."""
    data = path.read_bytes()
    for offset in range(0, len(data) - FORKMETA_RECORD_BYTES + 1,
                        FORKMETA_RECORD_BYTES):
        record = data[offset:offset + FORKMETA_RECORD_BYTES]
        if struct.unpack_from("=I", record, 20)[0] == rel and record[60] == kind:
            return offset
    raise FixtureError(
        f"{path.name} carries no kind-{kind} record for relation {rel}"
    )


def mutation(name: str, pattern: str, apply: Callable[[Path], None], expect: str) -> dict[str, Any]:
    return {"name": name, "pattern": pattern, "apply": apply, "expect": expect}


# Every persisted family with a startup or read-time validation contract.
# Offsets follow the on-disk layouts in the owning modules.
MUTATIONS = [
    mutation("wal_store.newer_version", "wal_segments_0/wal_store_identity_v1",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("wal_store.crc", "wal_segments_0/wal_store_identity_v1",
             lambda p: flip_byte(p, 24), OPEN_REJECTED),
    mutation("wal_store.truncated", "wal_segments_0/wal_store_identity_v1",
             lambda p: truncate_to(p, 32), OPEN_REJECTED),
    mutation("wal_segment.newer_version", "wal_segments_0/walv1_1_*",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("wal_segment.header_crc", "wal_segments_0/walv1_1_*",
             lambda p: flip_byte(p, 44), OPEN_REJECTED),
    mutation("wal_segment.payload_crc", "wal_segments_0/walv1_1_*",
             lambda p: flip_byte(p, 4096), OPEN_REJECTED),
    mutation("wal_segment.truncated", "wal_segments_0/walv1_1_*",
             lambda p: truncate_to(p, -4096), OPEN_REJECTED),
    # a resealed envelope that names another WAL page magic than its payload
    # carries: the checksums hold, the identity does not, and the read that
    # would hand the bytes on refuses them
    mutation("wal_segment.payload_identity", "wal_segments_0/walv1_1_*",
             lambda p: reseal_wal_segment_header(
                 p, lambda h: h.__setitem__(WAL_SEGMENT_IDENTITY_OFFSET,
                                            h[WAL_SEGMENT_IDENTITY_OFFSET] ^ 0x01)),
             USE_REJECTED),
    mutation("retention.state.newer_version", "retention.state",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("retention.state.crc", "retention.state",
             lambda p: flip_byte(p, 8), OPEN_REJECTED),
    mutation("retention.record.newer_version", "retention.meta",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("retention.record.crc", "retention.meta",
             lambda p: flip_byte(p, 20), OPEN_REJECTED),
    mutation("page_frontier.newer_version", "page-prune.frontiers",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("page_frontier.crc", "page-prune.frontiers",
             lambda p: flip_byte(p, 8), OPEN_REJECTED),
    mutation("page_frontier.truncated", "page-prune.frontiers",
             lambda p: truncate_to(p, -4), OPEN_REJECTED),
    mutation("walidx_frontier.newer_version", "walidx-prune.frontiers",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("walidx_frontier.crc", "walidx-prune.frontiers",
             lambda p: flip_byte(p, 8), OPEN_REJECTED),
    mutation("forkmeta_snapshot.manifest.newer_version", "forkmeta_snapshots/forkmeta_manifest_v1",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("forkmeta_snapshot.manifest.crc", "forkmeta_snapshots/forkmeta_manifest_v1",
             lambda p: flip_byte(p, 16), OPEN_REJECTED),
    mutation("forkmeta_snapshot.checkpoint.crc", "forkmeta_snapshots/forkmeta_checkpoint_v1_*",
             lambda p: flip_byte(p, -1), OPEN_REJECTED),
    mutation("walidx_snapshot.manifest.newer_version", "walidx_snapshots_0/walidx_manifest_v1",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("walidx_snapshot.manifest.crc", "walidx_snapshots_0/walidx_manifest_v1",
             lambda p: flip_byte(p, 16), OPEN_REJECTED),
    mutation("walidx_snapshot.shard.crc", "walidx_snapshots_0/walidxg1_*",
             lambda p: flip_byte(p, -1), OPEN_REJECTED),
    # the epoch watermark is the acknowledged length of its log; a damaged
    # one cannot be told from a lost suffix, so the store fails closed
    # the shard count is a headerless decimal; a value it never wrote is
    # damage, and a store that cannot prove its shard count fails closed
    mutation("store_config.shard_count", ".pagestore-nshards",
             lambda p: flip_byte(p, 0), OPEN_REJECTED),
    mutation("walidx_watermark.crc", "walidx_0_0_e*.size",
             lambda p: flip_byte(p, 8), OPEN_REJECTED),
    mutation("walidx_watermark.truncated", "walidx_0_0_e*.size",
             lambda p: truncate_to(p, 16), OPEN_REJECTED),
    mutation("timelines.unknown_magic", "timelines",
             lambda p: bump_le32(p, 0), OPEN_REJECTED),
    mutation("timelines.crc", "timelines",
             lambda p: flip_byte(p, 12), OPEN_REJECTED),
    # append-only logs repair a torn tail: the partial record is dropped and
    # the interrupted transition is re-derived (deletion cleanup resumes)
    mutation("timelines.torn_tail", "timelines",
             lambda p: truncate_to(p, -8), ACCEPTED),
    # the source epoch is the snapshot-base marker followed by the events
    # appended after the cutover (the fixture extension: a create and a grow)
    mutation("forkmeta.marker.unknown_magic", "forkmeta",
             lambda p: bump_le32(p, 0), OPEN_REJECTED),
    mutation("forkmeta.marker.corrupt_cutoff", "forkmeta",
             lambda p: flip_byte(p, 32), OPEN_REJECTED),
    mutation("forkmeta.tail.unknown_magic", "forkmeta",
             lambda p: bump_le32(p, FORKMETA_RECORD_BYTES), OPEN_REJECTED),
    # FKM3 records carry a CRC-24 in their former pad bytes, so a flipped byte
    # inside one is now rejected at open rather than left to the oracle.  Each
    # record is still addressed by the relation and the event it names: the
    # source also carries the branch's page-growth records, and a position
    # would move with the archive.
    mutation("forkmeta.tail.corrupt_nblocks", "forkmeta",
             lambda p: flip_byte(
                 p,
                 forkmeta_record_offset(p, FORKMETA_TAIL_REL, FORKMETA_KIND_SET) + 56,
             ), OPEN_REJECTED),
    mutation("forkmeta.tail.corrupt_event_lsn", "forkmeta",
             lambda p: flip_byte(
                 p,
                 forkmeta_record_offset(p, FORKMETA_TAIL_REL, FORKMETA_KIND_CREATE) + 32,
             ), OPEN_REJECTED),
    # a torn last record is the unacknowledged crash tail by contract; the
    # oracle notices because the fixture's event was in fact acknowledged
    mutation("forkmeta.tail.torn", "forkmeta",
             lambda p: truncate_to(p, -8), USE_REJECTED),
    # the cutover rewrites the source atomically, so an empty source behind a
    # selected snapshot is damage: accepting it would discard the epoch
    mutation("forkmeta.source.emptied", "forkmeta",
             lambda p: truncate_to(p, 0), OPEN_REJECTED),
    mutation("manifest.newer_version", "layers.manifest",
             lambda p: bump_le32(p, 4), OPEN_REJECTED),
    mutation("manifest.record.crc", "layers.manifest",
             lambda p: flip_byte(p, 8), OPEN_REJECTED),
    # the manifest log repairs a torn tail; a layer whose ADD is lost becomes
    # an orphan and its pages stay served from the segments it was built from
    mutation("manifest.torn_tail", "layers.manifest",
             lambda p: truncate_to(p, -8), ACCEPTED),
    # layer footers, data, and index checksums are validated during manifest
    # replay, so a damaged layer refuses the whole store rather than a read
    mutation("image_layer.newer_version", "layer_0_*",
             lambda p: bump_le32(p, -28), OPEN_REJECTED),
    mutation("image_layer.data_crc", "layer_0_*",
             lambda p: flip_byte(p, 100), OPEN_REJECTED),
    mutation("image_layer.truncated", "layer_0_*",
             lambda p: truncate_to(p, -8), OPEN_REJECTED),
]


def fnv1a(data: bytes, crc: int = FNV_INIT) -> int:
    for byte in data:
        crc ^= byte
        crc = (crc * 16777619) & 0xFFFFFFFF
    return crc


def canonicalize_manifest(store: Path) -> None:
    """Rewrite the absolute layer locations the manifest persists so that the
    archive does not depend on the directory that captured it, recomputing
    each record's checksum over the rewritten payload."""
    path = store / MANIFEST_NAME
    if not path.exists():
        return
    data = path.read_bytes()
    # the daemon persists the resolved spelling of the store path, which
    # differs from the one handed to it when a temporary directory contains a
    # symlink component
    prefix = str(store.resolve()).encode() + b"/"
    out = bytearray()
    offset = 0
    while offset + MANIFEST_HEADER_BYTES <= len(data):
        magic, _version, _type, length, _crc = struct.unpack_from("=IIIII", data, offset)
        if magic != MANIFEST_MAGIC or offset + MANIFEST_HEADER_BYTES + length > len(data):
            break                       # a torn tail travels as it is
        header = bytearray(data[offset:offset + MANIFEST_HEADER_BYTES])
        payload = bytearray(
            data[offset + MANIFEST_HEADER_BYTES:offset + MANIFEST_HEADER_BYTES + length])
        at = payload.find(prefix)
        while at >= 0:
            end = payload.index(b"\0", at)
            replacement = FIXTURE_LAYER_ROOT.encode() + b"/" + payload[at + len(prefix):end]
            if len(replacement) > end - at:
                raise FixtureError(f"canonical layer path does not fit in {payload[at:end]!r}")
            payload[at:end] = replacement + b"\0" * (end - at - len(replacement))
            at = payload.find(prefix, at + 1)
        struct.pack_into("=I", header, 16,
                         fnv1a(bytes(header[:16]) + bytes(payload)))
        out += header + payload
        offset += MANIFEST_HEADER_BYTES + length
    out += data[offset:]
    path.write_bytes(bytes(out))


def segment_magics(store: Path) -> set[int]:
    """Every page-segment record magic present in the store, walking the
    records at their own header sizes."""
    magics: set[int] = set()
    for path in sorted(store.glob("seg_*")):
        data = path.read_bytes()
        offset = 0
        while offset + 48 <= len(data):
            magic = struct.unpack_from("=I", data, offset)[0]
            header = SEG_HEADER_BYTES.get(magic)
            if header is None:
                break
            length = struct.unpack_from("=I", data, offset + 40)[0]
            if offset + header + length > len(data):
                break
            magics.add(magic)
            offset += header + length
    return magics


def walidx_magics(store: Path) -> set[int]:
    """The record magics present in the live WAL-index epoch logs.  Records
    are self-sized, so they are walked rather than searched for."""
    magics: set[int] = set()
    for path in sorted(store.glob("walidx_*")):
        if path.name.endswith(WATERMARK_SUFFIX) or not path.is_file():
            continue
        data = path.read_bytes()
        offset = 0
        while offset + 8 <= len(data):
            magic, rec_len = struct.unpack_from("=II", data, offset)
            if magic not in WALIDX_MAGICS or rec_len < 8 or offset + rec_len > len(data):
                break
            magics.add(magic)
            offset += rec_len
    return magics


# Where a persisted family's identity is readable from the archived bytes:
# the glob that finds a representative, and how to read (magic, version) from
# it.  "u32" is a 32-bit magic followed by a 32-bit version, "u32-u16" a
# 32-bit magic followed by a 16-bit version, and "u32-recordlen" a self-sized
# record log whose magic alone is the identity.
ARCHIVED_IDENTITIES: dict[str, tuple[str, str]] = {
    "page-prune.frontiers": ("page_frontier", "u32"),
    "walidx-prune.frontiers": ("walidx_frontier", "u32"),
    "retention.state": ("retention", "u32"),
    "retention.meta": ("retention", "u32"),
    "timelines": ("timelines", "u32-recordlen"),
    "layers.manifest": ("manifest", "u32"),
    "forkmeta": ("forkmeta", "u32-recordlen"),
    "forkmeta_snapshots/forkmeta_manifest_v1": ("forkmeta_snapshot", "u32"),
    "forkmeta_snapshots/forkmeta_checkpoint_v1_*": ("forkmeta_snapshot", "u32-u16"),
    "walidx_snapshots_0/walidx_manifest_v1": ("walidx_snapshot", "u32"),
    "walidx_snapshots_0/walidxg1_*": ("walidx_snapshot", "u32-u16"),
    "wal_segments_0/wal_store_identity_v1": ("wal_store", "u32"),
    "wal_segments_0/walv1_*": ("wal_segment", "u32"),
}


def archived_identity(store: Path, pattern: str, layout: str) -> tuple[int, int | None] | None:
    """The (magic, version) the archived representative of one family carries,
    or None when the archive has no such file."""
    matches = sorted(store.glob(pattern))
    if not matches:
        return None
    head = matches[0].read_bytes()[:8]
    if len(head) < 8:
        return None
    if layout == "u32":
        magic, version = struct.unpack_from("=II", head, 0)
        return magic, version
    if layout == "u32-u16":
        magic, version = struct.unpack_from("=IH", head, 0)
        return magic, version
    return struct.unpack_from("=I", head, 0)[0], None


def check_archived_identities(store: Path, identities: list[dict[str, Any]]) -> None:
    """The archive's own bytes must carry the identities format.json records.
    Updating the metadata alone would otherwise pass while the archive still
    holds the superseded format, and the reopen would succeed through the very
    reader the new fixture is supposed to retire."""
    advertised: dict[str, set[tuple[int, Any]]] = {}
    for item in identities:
        advertised.setdefault(item["family"], set()).add(
            (int(item["magic"], 16), item["version"])
        )
    for pattern, (family, layout) in ARCHIVED_IDENTITIES.items():
        found = archived_identity(store, pattern, layout)
        if found is None:
            raise FixtureError(f"the fixture has no {family} artifact matching {pattern}")
        magic, version = found
        known = advertised.get(family, set())
        if version is None:
            if not any(magic == item_magic for item_magic, _ in known):
                raise FixtureError(
                    f"{pattern} carries magic {magic:#x}, which no {family} identity "
                    f"advertises ({sorted(hex(m) for m, _ in known)})"
                )
            continue
        if (magic, version) not in known:
            raise FixtureError(
                f"{pattern} carries ({magic:#x}, {version}), which the {family} "
                f"identities do not advertise ({sorted(known)})"
            )


def check_segment_formats(store: Path, identities: list[dict[str, Any]]) -> None:
    """Every page-segment format the identity table advertises must have an
    instance in the fixture, or a regression in its reader cannot be caught."""
    advertised = {
        int(item["magic"], 16) for item in identities
        if item["family"] == "page_segment"
    }
    present = segment_magics(store)
    missing = sorted(advertised - present)
    if missing:
        raise FixtureError(
            "the fixture carries no record of advertised page-segment formats "
            + ", ".join(f"{magic:#x}" for magic in missing)
        )
    advertised_log = {
        int(item["magic"], 16) for item in identities
        if item["family"] == "walidx_log"
    }
    missing_log = sorted(advertised_log - walidx_magics(store))
    if missing_log:
        raise FixtureError(
            "the fixture carries no record of advertised WAL-index log formats "
            + ", ".join(f"{magic:#x}" for magic in missing_log)
        )


def format_identities(tool: Path) -> list[dict[str, Any]]:
    output = subprocess.run([str(tool)], check=True, capture_output=True, text=True).stdout
    return json.loads(output)


def deterministic_tar(store: Path, output: Path) -> list[str]:
    names: list[str] = []
    with gzip.GzipFile(filename="", mode="wb", fileobj=output.open("wb"), mtime=0) as compressed, \
            tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for path in sorted(store.rglob("*")):
            relative = path.relative_to(store).as_posix()
            if path.name in EXCLUDED:
                continue
            info = tar.gettarinfo(str(path), arcname=relative)
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o755 if path.is_dir() else 0o644
            info.pax_headers = {}
            if path.is_file():
                with path.open("rb") as handle:
                    tar.addfile(info, handle)
            else:
                tar.addfile(info)
            names.append(relative)
    return names


def extract(fixture: Path, store: Path) -> None:
    store.mkdir(parents=True)
    with tarfile.open(fixture / STORE_TAR, "r:gz") as tar:
        tar.extractall(store, filter="data")


class Daemon:
    def __init__(self, binary: Path, inspector: Path, store: Path, shm: str, log: Path,
                 daemon_args: list[str] | None = None,
                 daemon_env: dict[str, str] | None = None):
        self.binary, self.inspector, self.store, self.shm, self.log = binary, inspector, store, shm, log
        self.process: subprocess.Popen[str] | None = None
        # the fixture's own recorded configuration, so an archive is always
        # read back under the parameters that produced it
        self.args = list(DAEMON_ARGS if daemon_args is None else daemon_args)
        self.env = harness.private_environment()
        self.env.update(DAEMON_ENV if daemon_env is None else daemon_env)

    def start(self, timeout: float = 10.0) -> str:
        """Return "ready", "exit N" when the daemon refuses the store, or
        "signal N" when it died of a signal (a crash, never a rejection)."""
        harness.remove_shm(self.shm)
        with self.log.open("a", encoding="utf-8") as log:
            self.process = subprocess.Popen(
                [str(self.binary), "--shm", self.shm, "--store", str(self.store), *self.args],
                stdout=log, stderr=subprocess.STDOUT, text=True, env=self.env,
            )
        deadline = time.monotonic() + timeout
        while True:
            if self.process.poll() is not None:
                code = self.process.returncode
                return f"signal {-code}" if code < 0 else f"exit {code}"
            try:
                harness.inspect_store(self.inspector, self.shm, "health", INSPECTION_SCHEMA)
                return "ready"
            except harness.PlanError:
                if time.monotonic() >= deadline:
                    self.stop()
                    raise FixtureError("daemon did not become ready")
                time.sleep(0.05)

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def stop(self) -> int:
        if self.process is None:
            return 0
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        code = self.process.returncode
        self.process = None
        harness.remove_shm(self.shm)
        return code


def run_client(client: Path, shm: str, mode: str, log: Path,
               payload_identity: dict[str, Any] | None = None) -> subprocess.CompletedProcess[str]:
    """Run the fixture workload; ``payload_identity`` (the capturing build's
    on capture, the fixture's own on check) tells it which WAL page magic
    and block size the shipped WAL carries."""
    env = harness.private_environment()
    if payload_identity is not None:
        env["PAGESTORE_FIXTURE_XLOG_MAGIC"] = str(int(payload_identity["xlog_page_magic"]))
        env["PAGESTORE_FIXTURE_XLOG_BLCKSZ"] = str(int(payload_identity["xlog_blcksz"]))
    with log.open("a", encoding="utf-8") as output:
        return subprocess.run(
            [str(client), "--shm", shm, "--mode", mode, "--workload", "fixture"],
            stdout=output, stderr=subprocess.STDOUT, text=True,
            env=env, check=False,
        )


def verify_until(client: Path, shm: str, log: Path, timeout: float, expect_tail: bool = True,
                 payload_identity: dict[str, Any] | None = None) -> None:
    deadline = time.monotonic() + timeout
    while True:
        result = run_client(client, shm, "verify", log, payload_identity)
        if result.returncode == 0:
            return
        if not expect_tail:
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-1:]
            if tail and "appended after the snapshot cutover" in tail[0]:
                return
        if time.monotonic() >= deadline:
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-3:]
            raise FixtureError(
                f"fixture verify did not pass within {timeout:.0f}s; last oracle output: {tail!r}"
            )
        time.sleep(0.2)


def wait_for_forkmeta_cutover(store: Path, timeout: float) -> None:
    """The source epoch is exactly its snapshot-base marker once the first
    generation has been selected and the source rewritten behind it."""
    deadline = time.monotonic() + timeout
    while True:
        manifest = store / "forkmeta_snapshots" / "forkmeta_manifest_v1"
        source = store / "forkmeta"
        if manifest.exists() and source.exists() and source.stat().st_size == FORKMETA_RECORD_BYTES:
            return
        if time.monotonic() >= deadline:
            raise FixtureError("forkmeta snapshot cutover did not settle before the extension")
        time.sleep(0.05)


def capture(args: argparse.Namespace) -> int:
    fixture = args.capture
    fixture.mkdir(parents=True, exist_ok=True)
    # the shipped WAL is stamped with the capturing build's page magic and
    # block size, so the fixture is one that build loads; without a build
    # identity the workload's defaults (the pagestore branch's) apply
    stamp = build_payload_identity(args)
    with tempfile.TemporaryDirectory(prefix="pagestore-fixture-") as temp:
        root = Path(temp)
        # The capture directory is private to this process, so concurrent or
        # stale captures cannot destroy each other's store; the absolute paths
        # layers.manifest persists are canonicalized before archiving instead.
        store = root / "store"
        store.mkdir()
        log = root / "daemon.log"
        shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}"
        daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log, SEED_ARGS)
        try:
            if daemon.start() != "ready":
                raise FixtureError(f"daemon refused a fresh store; see {log}")
            seed = run_client(args.client_binary, shm, "seed", root / "client.log", stamp)
            if seed.returncode != 0:
                tail = (root / "client.log").read_text(encoding="utf-8", errors="replace").splitlines()[-3:]
                daemon_tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-3:]
                raise FixtureError(
                    f"fixture seed failed: {tail!r}; daemon: {daemon_tail!r}"
                )
            # maintenance publishes the frontiers, snapshots, and deletion
            # asynchronously; the seed oracle passes only once every family
            # settled, and the extension then lands in the settled source tail
            verify_until(args.client_binary, shm, root / "client.log", 60.0, expect_tail=False,
                         payload_identity=stamp)
            wait_for_forkmeta_cutover(store, 60.0)
        finally:
            code = daemon.stop()
        if code != 0:
            raise FixtureError(f"seeding daemon did not stop cleanly: status {code}")
        # the extension phase runs under the configuration the fixture records
        shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}_x"
        daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log)
        try:
            if daemon.start() != "ready":
                raise FixtureError(f"daemon refused the seeded store; see {log}")
            extend = run_client(args.client_binary, shm, "extend", root / "client.log", stamp)
            if extend.returncode != 0:
                tail = (root / "client.log").read_text(encoding="utf-8", errors="replace").splitlines()[-3:]
                raise FixtureError(f"fixture extension failed: {tail!r}")
            verify_until(args.client_binary, shm, root / "client.log", 30.0, payload_identity=stamp)
            time.sleep(1.0)
            verify_until(args.client_binary, shm, root / "client.log", 10.0, payload_identity=stamp)
        finally:
            code = daemon.stop()
        if code != 0:
            raise FixtureError(f"daemon did not stop cleanly: status {code}")
        identities = format_identities(args.format_tool)
        check_segment_formats(store, identities)
        check_archived_identities(store, identities)
        payload_identity = archive_payload_identity(store)
        canonicalize_manifest(store)
        names = deterministic_tar(store, fixture / STORE_TAR)
    (fixture / FORMAT_JSON).write_text(json.dumps(identities, indent=2) + "\n", encoding="utf-8")
    (fixture / FIXTURE_JSON).write_text(json.dumps({
        "schema": 1,
        "name": fixture.name,
        "role": "current",
        "workload": "fixture",
        "daemon_args": DAEMON_ARGS,
        "daemon_env": DAEMON_ENV,
        # the PostgreSQL identity of the payloads inside: a build with another
        # WAL page magic needs a fixture captured under it, not this one
        "payload_identity": payload_identity,
        "files": names,
    }, indent=2) + "\n", encoding="utf-8")
    print(f"captured {fixture} ({len(names)} entries)")
    return 0


def build_payload_identity(args: argparse.Namespace) -> dict[str, Any] | None:
    """The checking (or capturing) PostgreSQL build's payload identity, from
    a JSON file or from pagestore_control_restore --payload-identity."""
    if args.postgres_payload_identity is not None:
        return json.loads(args.postgres_payload_identity.read_text(encoding="utf-8"))
    if args.postgres_payload_identity_tool is not None:
        return json.loads(subprocess.run(
            [str(args.postgres_payload_identity_tool), "--payload-identity"],
            check=True, capture_output=True, text=True).stdout)
    return None


def check_payload_identity(args: argparse.Namespace, store: Path,
                           metadata: dict[str, Any]) -> None:
    """The archive's shipped WAL carries the payload identity fixture.json
    records, and, when the checking build's identity is given, that build can
    load it -- otherwise the fixture is for another PostgreSQL, which is a
    different finding from a broken envelope."""
    recorded = metadata.get("payload_identity")
    if not isinstance(recorded, dict):
        raise FixtureError("a current fixture records the payload identity of its shipped WAL")
    carried = archive_payload_identity(store)
    if carried != recorded:
        raise FixtureError(
            f"fixture.json records payload identity {recorded} but the archive carries {carried}"
        )
    print(f"ok   - shipped WAL carries the recorded payload identity "
          f"(XLOG_PAGE_MAGIC {carried['xlog_page_magic']:#06x}, "
          f"segment size {carried['wal_segment_size']})")
    build_identity = build_payload_identity(args)
    if build_identity is None:
        return
    build_magic = int(build_identity["xlog_page_magic"])
    if build_magic != carried["xlog_page_magic"]:
        raise ForeignPayload(
            f"payload needs a PostgreSQL build with XLOG_PAGE_MAGIC "
            f"{carried['xlog_page_magic']:#06x}; the checking build has {build_magic:#06x}"
        )
    build_offset = int(build_identity.get("xlog_long_header_seg_size_offset", 0))
    if build_offset and build_offset != carried["xlog_long_header_seg_size_offset"]:
        raise ForeignPayload(
            f"payload was written by an ABI that puts the WAL segment size at byte "
            f"{carried['xlog_long_header_seg_size_offset']} of the long page header; "
            f"the checking build reads it at byte {build_offset}"
        )
    build_blcksz = int(build_identity.get("xlog_blcksz", 0))
    if build_blcksz and build_blcksz != carried["xlog_blcksz"]:
        raise ForeignPayload(
            f"payload was written with a {carried['xlog_blcksz']}-byte WAL block size; "
            f"the checking build uses {build_blcksz}"
        )
    print(f"ok   - the checking PostgreSQL build loads this payload (XLOG_PAGE_MAGIC "
          f"{build_magic:#06x}, segment size at byte {carried['xlog_long_header_seg_size_offset']}, "
          f"WAL block size {carried['xlog_blcksz']})")


def check_reopen(args: argparse.Namespace, root: Path, fixture: Path,
                 metadata: dict[str, Any],
                 identities: list[dict[str, Any]] | None) -> None:
    store = root / "reopen"
    extract(fixture, store)
    if identities is not None:
        check_segment_formats(store, identities)
        check_archived_identities(store, identities)
        check_payload_identity(args, store, metadata)
    log = root / "reopen-daemon.log"
    for generation in range(2):
        shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}_{generation}"
        daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log,
                        metadata["daemon_args"], metadata["daemon_env"])
        try:
            status = daemon.start()
            if status != "ready":
                tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-4:]
                raise FixtureError(f"fixture reopen {generation} refused: {status}; daemon: {tail!r}")
            result = run_client(args.client_binary, shm, "verify", root / "reopen-client.log",
                                metadata.get("payload_identity"))
            if result.returncode != 0:
                tail = (root / "reopen-client.log").read_text(
                    encoding="utf-8", errors="replace").splitlines()[-3:]
                raise FixtureError(
                    f"fixture oracle failed after reopen {generation}: {tail!r}"
                )
        finally:
            code = daemon.stop()
        if code != 0:
            raise FixtureError(f"fixture daemon {generation} did not stop cleanly: status {code}")
    print("ok   - fixture reopens and its oracle holds across a restart")


def run_mutation(args: argparse.Namespace, root: Path, fixture: Path, case: dict[str, Any],
                 metadata: dict[str, Any]) -> str:
    store = root / "mutations" / case["name"]
    extract(fixture, store)
    case["apply"](first_match(store, case["pattern"]))
    log = root / "mutations" / f"{case['name']}.daemon.log"
    shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}_m"
    daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log,
                    metadata["daemon_args"], metadata["daemon_env"])
    try:
        status = daemon.start()
        if status.startswith("signal"):
            return f"{CRASHED} ({status} at open)"
        if status != "ready":
            return OPEN_REJECTED
        client_log = root / "mutations" / f"{case['name']}.client.log"
        result = run_client(args.client_binary, shm, "verify", client_log,
                            metadata.get("payload_identity"))
        # A mutation the store repairs (a torn append-only tail) resumes the
        # transition it interrupted asynchronously, so the oracle is retried
        # while that can still land; a rejection stays a rejection.
        deadline = time.monotonic() + (30.0 if case["expect"] == ACCEPTED else 0.0)
        while result.returncode != 0 and time.monotonic() < deadline and daemon.alive():
            time.sleep(0.5)
            result = run_client(args.client_binary, shm, "verify", client_log,
                            metadata.get("payload_identity"))
        if not daemon.alive():
            code = daemon.process.returncode if daemon.process else None
            return f"{CRASHED} (daemon exited {code} under use)"
        return ACCEPTED if result.returncode == 0 else USE_REJECTED
    finally:
        daemon.stop()


def fixture_metadata(fixture: Path) -> dict[str, Any]:
    """The archive's own configuration.  A later slice may change the capture
    parameters, and an older archive must still be read back under the ones it
    was captured with rather than under today's defaults."""
    metadata = json.loads((fixture / FIXTURE_JSON).read_text(encoding="utf-8"))
    if metadata.get("schema") != 1 or metadata.get("name") != fixture.name:
        raise FixtureError(f"{fixture / FIXTURE_JSON} does not describe {fixture.name}")
    daemon_args = metadata.get("daemon_args")
    daemon_env = metadata.get("daemon_env")
    if not isinstance(daemon_args, list) or not all(isinstance(a, str) for a in daemon_args) or \
            not isinstance(daemon_env, dict) or \
            not all(isinstance(k, str) and isinstance(v, str) for k, v in daemon_env.items()):
        raise FixtureError(f"{fixture / FIXTURE_JSON} has no usable daemon configuration")
    role = metadata.get("role", "current")
    if role not in ("current", "legacy"):
        raise FixtureError(f"{fixture / FIXTURE_JSON} has an unknown role {role!r}")
    metadata["daemon_args"], metadata["daemon_env"], metadata["role"] = \
        daemon_args, daemon_env, role
    return metadata


def check_one(args: argparse.Namespace, fixture: Path) -> int | None:
    """A "current" fixture pins the compiled identities and takes every
    mutation; a "legacy" fixture records a format the daemon still reads, so
    it only has to reopen with its oracle intact (an upgrade path).  Returns
    None for a current fixture whose payload is for another PostgreSQL build:
    it is skipped, and the caller requires some current fixture to match."""
    metadata = fixture_metadata(fixture)
    role = metadata["role"]
    print(f"--- fixture {fixture.name} ({role})")
    expected = json.loads((fixture / FORMAT_JSON).read_text(encoding="utf-8"))
    current = format_identities(args.format_tool)
    failures = 0
    with tempfile.TemporaryDirectory(prefix="pagestore-fixture-check-") as temp:
        root = Path(temp)
        # Reopening comes first: an archive whose identities the binary has
        # moved past is exactly the upgrade path a fixture exists to prove,
        # and it must still open before the identity table is judged.  Only a
        # current fixture must carry every advertised format; a legacy one
        # records formats the daemon has since moved past.
        try:
            check_reopen(args, root, fixture, metadata,
                         current if role == "current" else None)
        except ForeignPayload as foreign:
            # a release branch carries the fixture captured under its own
            # build; one captured under another build is neither broken
            # nor checkable here
            print(f"skip - {foreign} (a fixture for another PostgreSQL release)")
            return None
        if role == "current":
            if current != expected:
                print("FAIL - compiled persisted-format identities differ from the fixture:")
                for item in current:
                    if item not in expected:
                        print(f"  new or changed: {item}")
                for item in expected:
                    if item not in current:
                        print(f"  missing or changed: {item}")
                print(f"  a persisted-format change must add or update a fixture ({fixture})")
                return 1
            print("ok   - compiled persisted-format identities match the fixture")
        elif current == expected:
            print("FAIL - a legacy fixture records the current identities; mark it current")
            return 1
        else:
            print("ok   - legacy fixture records superseded identities")
            return 0
        (root / "mutations").mkdir()
        for case in MUTATIONS:
            if args.only and case["name"] not in args.only:
                continue
            try:
                outcome = run_mutation(args, root, fixture, case, metadata)
            except FixtureError as error:
                outcome = f"error: {error}"
            if outcome == case["expect"]:
                print(f"ok   - {case['name']}: {outcome}")
            else:
                failures += 1
                print(f"FAIL - {case['name']}: {outcome}, expected {case['expect']}")
                if args.keep_failures:
                    keep = args.keep_failures / case["name"]
                    shutil.copytree(root / "mutations" / case["name"], keep, dirs_exist_ok=True)
                    for suffix in ("daemon.log", "client.log"):
                        source = root / "mutations" / f"{case['name']}.{suffix}"
                        if source.exists():
                            shutil.copy2(source, keep / suffix)
    return 1 if failures else 0


def check(args: argparse.Namespace) -> int:
    status = 0
    binds_build = (args.postgres_payload_identity is not None or
                   args.postgres_payload_identity_tool is not None)
    matched_current = 0
    for fixture in args.check:
        result = check_one(args, fixture)
        if result is None:
            continue
        status |= result
        if fixture_metadata(fixture)["role"] == "current":
            matched_current += 1
    if binds_build and matched_current == 0:
        # A release branch that has just received a format change carries only
        # the fixture captured on `pagestore`; until it captures its own, the
        # envelopes were still checked and only the build binding is missing.
        # The development branch requires the match.
        if args.require_build_match:
            print("FAIL - no current fixture carries a payload this PostgreSQL build loads; "
                  "capture one under this build")
            return 1
        print("WARN - no current fixture carries a payload this PostgreSQL build loads; "
              "capture one under this build (required on pagestore)")
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--capture", type=Path, metavar="FIXTURE_DIR")
    group.add_argument("--check", type=Path, nargs="+", metavar="FIXTURE_DIR")
    parser.add_argument("--daemon-binary", type=Path, required=True)
    parser.add_argument("--client-binary", type=Path, required=True)
    parser.add_argument("--inspect-binary", type=Path, required=True)
    parser.add_argument("--format-tool", type=Path, required=True)
    parser.add_argument("--postgres-payload-identity", type=Path,
                        help="JSON from pagestore_control_restore --payload-identity; "
                             "when given, the fixture's payload must be loadable by that build")
    parser.add_argument("--postgres-payload-identity-tool", type=Path,
                        help="a pagestore_control_restore binary to ask for the same identity")
    parser.add_argument("--require-build-match", action="store_true",
                        help="fail, rather than warn, when no current fixture carries a payload "
                             "the checking build loads")
    parser.add_argument("--only", nargs="*", help="run only these mutation cases")
    parser.add_argument("--keep-failures", type=Path, help="copy failed mutation stores here")
    args = parser.parse_args(argv)
    for name in ("daemon_binary", "client_binary", "inspect_binary", "format_tool",
                 "postgres_payload_identity_tool", "postgres_payload_identity"):
        if getattr(args, name) is not None:
            setattr(args, name, getattr(args, name).resolve())
    try:
        return capture(args) if args.capture else check(args)
    except FixtureError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
