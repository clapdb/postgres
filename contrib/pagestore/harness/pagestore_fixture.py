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
import io
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
EXCLUDED = {".pagestore.lock"}
FORKMETA_RECORD_BYTES = 64
INSPECTION_SCHEMA = harness.read_json(Path(__file__).resolve().parent / "inspection_schema.json")
STORE_TAR = "store.tar.gz"
FORMAT_JSON = "format.json"
FIXTURE_JSON = "fixture.json"

# Outcomes a mutated fixture may produce.
OPEN_REJECTED = "open_rejected"      # the daemon refuses to open the store
USE_REJECTED = "use_rejected"        # it opens, but the oracle or inspection fails closed
ACCEPTED = "accepted"                # it opens and the oracle passes


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
    # KNOWN GAP: forkmeta source records carry no checksum, so a flipped byte
    # inside a record is caught only by the oracle, not by the format
    mutation("forkmeta.tail.corrupt_nblocks", "forkmeta",
             lambda p: flip_byte(p, 2 * FORKMETA_RECORD_BYTES + 56), USE_REJECTED),
    # a torn last record is the unacknowledged crash tail by contract; the
    # oracle notices because the fixture's event was in fact acknowledged
    mutation("forkmeta.tail.torn", "forkmeta",
             lambda p: truncate_to(p, -8), USE_REJECTED),
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
    def __init__(self, binary: Path, inspector: Path, store: Path, shm: str, log: Path):
        self.binary, self.inspector, self.store, self.shm, self.log = binary, inspector, store, shm, log
        self.process: subprocess.Popen[str] | None = None
        self.env = harness.private_environment()
        self.env.update(DAEMON_ENV)

    def start(self, timeout: float = 10.0) -> str:
        """Return "ready" or the daemon's exit status text when it refuses."""
        harness.remove_shm(self.shm)
        with self.log.open("a", encoding="utf-8") as log:
            self.process = subprocess.Popen(
                [str(self.binary), "--shm", self.shm, "--store", str(self.store), *DAEMON_ARGS],
                stdout=log, stderr=subprocess.STDOUT, text=True, env=self.env,
            )
        deadline = time.monotonic() + timeout
        while True:
            if self.process.poll() is not None:
                return f"exit {self.process.returncode}"
            try:
                harness.inspect_store(self.inspector, self.shm, "health", INSPECTION_SCHEMA)
                return "ready"
            except harness.PlanError:
                if time.monotonic() >= deadline:
                    self.stop()
                    raise FixtureError("daemon did not become ready")
                time.sleep(0.05)

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


def run_client(client: Path, shm: str, mode: str, log: Path) -> subprocess.CompletedProcess[str]:
    with log.open("a", encoding="utf-8") as output:
        return subprocess.run(
            [str(client), "--shm", shm, "--mode", mode, "--workload", "fixture"],
            stdout=output, stderr=subprocess.STDOUT, text=True,
            env=harness.private_environment(), check=False,
        )


def verify_until(client: Path, shm: str, log: Path, timeout: float, expect_tail: bool = True) -> None:
    deadline = time.monotonic() + timeout
    while True:
        result = run_client(client, shm, "verify", log)
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
    with tempfile.TemporaryDirectory(prefix="pagestore-fixture-") as temp:
        root = Path(temp)
        store = root / "store"
        store.mkdir()
        log = root / "daemon.log"
        shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}"
        daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log)
        try:
            if daemon.start() != "ready":
                raise FixtureError(f"daemon refused a fresh store; see {log}")
            seed = run_client(args.client_binary, shm, "seed", root / "client.log")
            if seed.returncode != 0:
                tail = (root / "client.log").read_text(encoding="utf-8", errors="replace").splitlines()[-3:]
                daemon_tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-3:]
                raise FixtureError(
                    f"fixture seed failed: {tail!r}; daemon: {daemon_tail!r}"
                )
            # maintenance publishes the frontiers, snapshots, and deletion
            # asynchronously; the seed oracle passes only once every family
            # settled, and the extension then lands in the settled source tail
            verify_until(args.client_binary, shm, root / "client.log", 60.0, expect_tail=False)
            wait_for_forkmeta_cutover(store, 60.0)
            extend = run_client(args.client_binary, shm, "extend", root / "client.log")
            if extend.returncode != 0:
                raise FixtureError(f"fixture extension failed; see {root / 'client.log'}")
            verify_until(args.client_binary, shm, root / "client.log", 30.0)
            time.sleep(1.0)
            verify_until(args.client_binary, shm, root / "client.log", 10.0)
        finally:
            code = daemon.stop()
        if code != 0:
            raise FixtureError(f"daemon did not stop cleanly: status {code}")
        names = deterministic_tar(store, fixture / STORE_TAR)
    identities = format_identities(args.format_tool)
    (fixture / FORMAT_JSON).write_text(json.dumps(identities, indent=2) + "\n", encoding="utf-8")
    (fixture / FIXTURE_JSON).write_text(json.dumps({
        "schema": 1,
        "name": fixture.name,
        "workload": "fixture",
        "daemon_args": DAEMON_ARGS,
        "daemon_env": DAEMON_ENV,
        "files": names,
    }, indent=2) + "\n", encoding="utf-8")
    print(f"captured {fixture} ({len(names)} entries)")
    return 0


def check_reopen(args: argparse.Namespace, root: Path, fixture: Path) -> None:
    store = root / "reopen"
    extract(fixture, store)
    log = root / "reopen-daemon.log"
    for generation in range(2):
        shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}_{generation}"
        daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log)
        try:
            status = daemon.start()
            if status != "ready":
                tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-4:]
                raise FixtureError(f"fixture reopen {generation} refused: {status}; daemon: {tail!r}")
            result = run_client(args.client_binary, shm, "verify", root / "reopen-client.log")
            if result.returncode != 0:
                raise FixtureError(
                    f"fixture oracle failed after reopen {generation}; see {root / 'reopen-client.log'}"
                )
        finally:
            code = daemon.stop()
        if code != 0:
            raise FixtureError(f"fixture daemon {generation} did not stop cleanly: status {code}")
    print("ok   - fixture reopens and its oracle holds across a restart")


def run_mutation(args: argparse.Namespace, root: Path, fixture: Path, case: dict[str, Any]) -> str:
    store = root / "mutations" / case["name"]
    extract(fixture, store)
    case["apply"](first_match(store, case["pattern"]))
    log = root / "mutations" / f"{case['name']}.daemon.log"
    shm = f"/psfixture_{os.getpid()}_{time.monotonic_ns()}_m"
    daemon = Daemon(args.daemon_binary, args.inspect_binary, store, shm, log)
    try:
        status = daemon.start()
        if status != "ready":
            return OPEN_REJECTED
        result = run_client(args.client_binary, shm, "verify", root / "mutations" / f"{case['name']}.client.log")
        return ACCEPTED if result.returncode == 0 else USE_REJECTED
    finally:
        daemon.stop()


def check(args: argparse.Namespace) -> int:
    fixture = args.check
    expected = json.loads((fixture / FORMAT_JSON).read_text(encoding="utf-8"))
    current = format_identities(args.format_tool)
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
    failures = 0
    with tempfile.TemporaryDirectory(prefix="pagestore-fixture-check-") as temp:
        root = Path(temp)
        check_reopen(args, root, fixture)
        (root / "mutations").mkdir()
        for case in MUTATIONS:
            if args.only and case["name"] not in args.only:
                continue
            try:
                outcome = run_mutation(args, root, fixture, case)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--capture", type=Path, metavar="FIXTURE_DIR")
    group.add_argument("--check", type=Path, metavar="FIXTURE_DIR")
    parser.add_argument("--daemon-binary", type=Path, required=True)
    parser.add_argument("--client-binary", type=Path, required=True)
    parser.add_argument("--inspect-binary", type=Path, required=True)
    parser.add_argument("--format-tool", type=Path, required=True)
    parser.add_argument("--only", nargs="*", help="run only these mutation cases")
    parser.add_argument("--keep-failures", type=Path, help="copy failed mutation stores here")
    args = parser.parse_args(argv)
    for name in ("daemon_binary", "client_binary", "inspect_binary", "format_tool"):
        setattr(args, name, getattr(args, name).resolve())
    try:
        return capture(args) if args.capture else check(args)
    except FixtureError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
