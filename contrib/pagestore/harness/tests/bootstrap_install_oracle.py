#!/usr/bin/env python3
"""Small, process-independent oracle for portable branch installation tests.

The golden shell deliberately owns PostgreSQL and pg_ctl.  This helper only
arms no processes and never starts a server; it records or checks the bytes
which must survive an installer backend crash.

The public command line is intentionally boring so a shell test can use it
from any working directory::

    bootstrap_install_oracle.py snapshot PREPARED TARGET OUT
    bootstrap_install_oracle.py unchanged PREPARED TARGET SNAPSHOT
    bootstrap_install_oracle.py installed PREPARED TARGET
    bootstrap_install_oracle.py snapshot-installed TARGET OUT
    bootstrap_install_oracle.py equal-installed TARGET SNAPSHOT
    bootstrap_install_oracle.py report REPORT NAME OPERATION

``snapshot`` covers the complete prepared directory and only
``TARGET/global/pg_control``.  ``installed`` checks the portable bootstrap
artifact, relation maps encoded in that artifact, and the installed SLRUs.
The installed snapshot intentionally excludes logs, WAL, and pg_control.
Repeated ``snapshot-installed``/``equal-installed`` calls therefore provide
an idempotence check without making normal PostgreSQL startup noise part of
the oracle.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
from pathlib import Path
import struct
import stat
import sys
from typing import Any, Iterable


FORMAT = 1
MANIFEST = "pagestore_branch.manifest"
BOOTSTRAP = "pagestore_branch.bootstrap"
CONTROL = "global/pg_control"
REPORT_SCENARIO = "mvp-golden"
REPORT_SEED = 1

# The C structure is five uint64 fields followed by eight uint32 fields,
# including the CRC and manifest CRC.  It is 72 bytes on native builds.
BOOTSTRAP_HEADER = struct.Struct("@QQQQQIIIIIIII")
BOOTSTRAP_MAP = struct.Struct("@II")
BOOTSTRAP_MAGIC = 0x50534242
BOOTSTRAP_FORMAT = 1


class OracleError(RuntimeError):
    """An acceptance assertion failed."""


def _fail(message: str) -> "NoReturn":
    raise OracleError(message)


def _regular_bytes(path: Path) -> bytes:
    try:
        file_stat = path.lstat()
    except FileNotFoundError:
        _fail(f"missing regular file: {path}")
    if not stat.S_ISREG(file_stat.st_mode):
        _fail(f"expected non-symlink regular file: {path}")
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        _fail(f"could not open {path}: {exc}")
    chunks: list[bytes] = []
    try:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(fd)
    return b"".join(chunks)


def _entry(path: Path) -> dict[str, Any]:
    try:
        file_stat = path.lstat()
    except FileNotFoundError:
        return {"kind": "missing"}
    mode = file_stat.st_mode
    if stat.S_ISREG(mode):
        digest = hashlib.sha256(_regular_bytes(path)).hexdigest()
        return {"kind": "file", "size": file_stat.st_size, "sha256": digest}
    if stat.S_ISDIR(mode):
        return {"kind": "dir"}
    if stat.S_ISLNK(mode):
        return {"kind": "symlink", "target": os.readlink(path)}
    return {"kind": "other", "mode": mode & 0o7777}


def _relative(root: Path, path: Path) -> str:
    value = os.path.relpath(path, root)
    return "" if value == "." else value.replace(os.sep, "/")


def _snapshot_tree(root: Path) -> dict[str, dict[str, Any]]:
    """Snapshot a tree without following symlinks."""

    root = Path(root)
    if not root.exists() and not root.is_symlink():
        _fail(f"missing snapshot root: {root}")
    result: dict[str, dict[str, Any]] = {}

    def visit(path: Path) -> None:
        relative = _relative(root, path)
        item = _entry(path)
        result[relative] = item
        if item["kind"] != "dir":
            return
        try:
            children = sorted(path.iterdir(), key=lambda child: child.name)
        except OSError as exc:
            _fail(f"could not enumerate {path}: {exc}")
        for child in children:
            visit(child)

    visit(root)
    return result


def _snapshot_paths(root: Path, paths: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Snapshot exact relative paths, recursively for directory paths."""

    root = Path(root)
    result: dict[str, dict[str, Any]] = {}
    for relative in sorted(set(paths)):
        path = root / relative
        item = _entry(path)
        result[relative] = item
        if item["kind"] == "dir":
            for child, child_item in _snapshot_tree(path).items():
                if child == "":
                    continue
                result[f"{relative}/{child}"] = child_item
    return result


def _write_json(path: Path, value: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    data = json.dumps(value, sort_keys=True, indent=2).encode("utf-8") + b"\n"
    with open(temporary, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _read_json(path: Path) -> Any:
    try:
        with open(path, "rb") as stream:
            return json.load(stream)
    except (OSError, ValueError) as exc:
        _fail(f"could not read JSON snapshot {path}: {exc}")


def _snapshot_inputs(prepared: Path, target: Path) -> dict[str, Any]:
    control = _snapshot_paths(target, [CONTROL])
    if control[CONTROL].get("kind") != "file":
        _fail(f"target control file is not a regular file: {target / CONTROL}")
    return {
        "format": FORMAT,
        "prepared": _snapshot_tree(prepared),
        "target_pg_control": control,
    }


def _assert_equal(label: str, expected: Any, actual: Any) -> None:
    if expected != actual:
        _fail(f"{label} changed")


def snapshot_inputs(prepared: Path, target: Path, output: Path) -> None:
    _write_json(output, _snapshot_inputs(prepared, target))


def unchanged_inputs(prepared: Path, target: Path, snapshot: Path) -> None:
    saved = _read_json(snapshot)
    if saved.get("format") != FORMAT:
        _fail(f"unsupported snapshot format in {snapshot}")
    _assert_equal("prepared artifacts or target pg_control", saved,
                  _snapshot_inputs(prepared, target))


def _manifest(prepared: Path) -> tuple[bytes, dict[str, Any]]:
    data = _regular_bytes(prepared / MANIFEST)
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        _fail(f"invalid prepared branch manifest: {exc}")
    if not isinstance(value, dict):
        _fail("prepared branch manifest is not an object")
    return data, value


def _commit_ts_required(manifest: dict[str, Any]) -> bool:
    try:
        oldest = int(manifest["oldest_commit_ts_xid"])
        newest = int(manifest["next_commit_ts_xid"])
    except (KeyError, TypeError, ValueError):
        _fail("manifest has invalid commit-ts horizons")
    if not (0 <= oldest <= 0xFFFFFFFF and 0 <= newest <= 0xFFFFFFFF):
        _fail("manifest has out-of-range commit-ts horizons")
    normal_oldest = 3 <= oldest <= 0xFFFFFFFF
    normal_newest = 3 <= newest <= 0xFFFFFFFF
    if not normal_oldest and not normal_newest:
        return False
    if not normal_oldest or not normal_newest:
        _fail("manifest has a half-active commit-ts horizon")
    # Match TransactionIdFollows(oldest, newest), including wraparound.
    follows = oldest != newest and ((oldest - newest) & 0xFFFFFFFF) < 0x80000000
    if follows:
        _fail("manifest has inverted commit-ts horizons")
    return True


def _assert_tree_equal(prepared: Path, target: Path, relative: str) -> None:
    expected = _snapshot_tree(prepared / relative)
    actual = _snapshot_tree(target / relative)
    _assert_equal(relative, expected, actual)


def _parse_bootstrap(data: bytes) -> list[tuple[int, bytes]]:
    if len(data) < BOOTSTRAP_HEADER.size:
        _fail("branch bootstrap artifact is truncated")
    fields = BOOTSTRAP_HEADER.unpack_from(data)
    artifact_size = fields[4]
    magic = fields[5]
    version = fields[6]
    map_count = fields[9]
    if magic != BOOTSTRAP_MAGIC or version != BOOTSTRAP_FORMAT:
        _fail("branch bootstrap artifact has an invalid magic or format")
    if artifact_size != len(data) or map_count < 2:
        _fail("branch bootstrap artifact has an invalid size or map count")
    cursor = BOOTSTRAP_HEADER.size
    maps: list[tuple[int, bytes]] = []
    previous = -1
    for index in range(map_count):
        if cursor + BOOTSTRAP_MAP.size > len(data):
            _fail("branch bootstrap map table is truncated")
        database_oid, size = BOOTSTRAP_MAP.unpack_from(data, cursor)
        cursor += BOOTSTRAP_MAP.size
        if database_oid <= previous or size == 0:
            _fail("branch bootstrap maps are not strictly ordered")
        if index == 0 and database_oid != 0:
            _fail("branch bootstrap has no global relation map")
        if cursor + size > len(data):
            _fail("branch bootstrap relation map is truncated")
        maps.append((database_oid, data[cursor:cursor + size]))
        cursor += size
        previous = database_oid
    if cursor != len(data):
        _fail("branch bootstrap has trailing bytes")
    return maps


def installed(prepared: Path, target: Path) -> None:
    manifest_data, manifest = _manifest(prepared)
    bootstrap_data = _regular_bytes(prepared / BOOTSTRAP)
    target_manifest = _regular_bytes(target / MANIFEST)
    target_bootstrap = _regular_bytes(target / BOOTSTRAP)
    _assert_equal("installed branch manifest", manifest_data, target_manifest)
    _assert_equal("installed branch bootstrap", bootstrap_data, target_bootstrap)

    for database_oid, relation_map in _parse_bootstrap(bootstrap_data):
        if database_oid == 0:
            relative = "global/pg_filenode.map"
        else:
            relative = f"base/{database_oid}/pg_filenode.map"
        _assert_equal(relative, relation_map, _regular_bytes(target / relative))

    for relative in ("pg_xact", "pg_multixact/offsets", "pg_multixact/members"):
        _assert_tree_equal(prepared, target, relative)

    commit_ts_target = target / "pg_commit_ts"
    if _commit_ts_required(manifest):
        _assert_tree_equal(prepared, target, "pg_commit_ts")
    else:
        if not commit_ts_target.is_dir() or commit_ts_target.is_symlink():
            _fail("inactive commit-ts target is not a real directory")
        if any(commit_ts_target.iterdir()):
            _fail("inactive commit-ts target is not empty")


def _installed_paths(target: Path) -> list[str]:
    patterns = [
        MANIFEST,
        BOOTSTRAP,
        "global/pg_filenode.map",
        "pg_xact",
        "pg_commit_ts",
        "pg_multixact",
    ]
    paths: list[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(str(target / pattern)))
        if not matches:
            _fail(f"installed artifact is missing: {target / pattern}")
        paths.extend(_relative(target, Path(match)) for match in matches)
    maps = sorted(glob.glob(str(target / "base" / "*" / "pg_filenode.map")))
    if not maps:
        _fail("installed database relation maps are missing")
    paths.extend(_relative(target, Path(match)) for match in maps)
    return sorted(set(paths))


def snapshot_installed(target: Path, output: Path) -> None:
    paths = _installed_paths(target)
    _write_json(output, {
        "format": FORMAT,
        "paths": paths,
        "snapshot": _snapshot_paths(target, paths),
    })


def equal_installed(target: Path, snapshot: Path) -> None:
    saved = _read_json(snapshot)
    if saved.get("format") != FORMAT or not isinstance(saved.get("paths"), list):
        _fail(f"unsupported installed snapshot format in {snapshot}")
    paths = [str(path) for path in saved["paths"]]
    _assert_equal("installed artifact selection", sorted(paths),
                  _installed_paths(target))
    actual = _snapshot_paths(target, paths)
    _assert_equal("installed artifacts", saved.get("snapshot"), actual)


def check_report(report_path: Path, name: str, operation: str) -> int:
    data = _regular_bytes(report_path)
    try:
        records = [json.loads(line) for line in data.decode("utf-8").splitlines()
                   if line.strip()]
    except (UnicodeDecodeError, ValueError) as exc:
        _fail(f"invalid fault report: {exc}")
    if len(records) != 1 or not isinstance(records[0], dict):
        _fail("fault report does not contain exactly one JSON record")
    record = records[0]
    if (record.get("schema") != 1 or record.get("name") != name or
            record.get("action") != "crash" or record.get("hit") != 1 or
            record.get("scenario") != REPORT_SCENARIO or
            record.get("seed") != REPORT_SEED or
            record.get("operation") != operation):
        _fail("fault report identity does not match the requested install point")
    pid = record.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        _fail("fault report has no positive process PID")
    return pid


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    command = commands.add_parser("snapshot")
    command.add_argument("prepared", type=Path)
    command.add_argument("target", type=Path)
    command.add_argument("output", type=Path)

    command = commands.add_parser("unchanged")
    command.add_argument("prepared", type=Path)
    command.add_argument("target", type=Path)
    command.add_argument("snapshot", type=Path)

    command = commands.add_parser("installed")
    command.add_argument("prepared", type=Path)
    command.add_argument("target", type=Path)

    command = commands.add_parser("snapshot-installed")
    command.add_argument("target", type=Path)
    command.add_argument("output", type=Path)

    command = commands.add_parser("equal-installed")
    command.add_argument("target", type=Path)
    command.add_argument("snapshot", type=Path)

    command = commands.add_parser("report")
    command.add_argument("report", type=Path)
    command.add_argument("name")
    command.add_argument("operation")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "snapshot":
        snapshot_inputs(args.prepared, args.target, args.output)
    elif args.command == "unchanged":
        unchanged_inputs(args.prepared, args.target, args.snapshot)
    elif args.command == "installed":
        installed(args.prepared, args.target)
    elif args.command == "snapshot-installed":
        snapshot_installed(args.target, args.output)
    elif args.command == "equal-installed":
        equal_installed(args.target, args.snapshot)
    elif args.command == "report":
        print(check_report(args.report, args.name, args.operation))
    else:  # pragma: no cover - argparse makes this unreachable
        _fail(f"unknown command: {args.command}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OracleError as exc:
        print(f"bootstrap_install_oracle: {exc}", file=sys.stderr)
        raise SystemExit(1)
