#!/usr/bin/env python3
"""Standalone POSIX process-level acceptance tests for pagestore ownership/recovery.

This file intentionally has no unittest test cases.  It is invoked explicitly
with the two standalone binaries, for example:

    python3 contrib/pagestore/harness/tests/test_store_owner_recovery.py \
        --daemon-binary build/contrib/pagestore/pagestore_daemon \
        --inspect-binary build/contrib/pagestore/pagestore_inspect

The scenarios use only the daemon and its read-only inspector; they do not
start PostgreSQL or SPDK.  The temporary root is removed unless --keep is set.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
import stat
from typing import Any


DEFAULT_TIMEOUT = 10.0
PAGE_SIZE = 8192
SHARD_COUNT = 1
MANIFEST_MAGIC = 0x504D414E  # "PMAN"
MANIFEST_VERSION = 3
MANIFEST_REMOVE_LAYER = 5
ORPHAN_ID = 0x0000000000004000


class TestFailure(RuntimeError):
    """An acceptance assertion failed."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TestFailure(message)


def private_environment() -> dict[str, str]:
    """Keep unrelated cluster and pagestore settings out of the subprocesses."""
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PG") and not key.startswith("PAGESTORE_")
    }


def shm_unlink(name: str) -> None:
    """Remove a POSIX shm object without depending on /dev/shm layout."""
    libc = ctypes.CDLL(None, use_errno=True)
    unlink = libc.shm_unlink
    unlink.argtypes = [ctypes.c_char_p]
    unlink.restype = ctypes.c_int
    if unlink(name.encode("utf-8")) != 0:
        error = ctypes.get_errno()
        if error != errno.ENOENT:
            raise OSError(error, os.strerror(error), name)


def next_shm_name() -> str:
    # Linux limits POSIX shm names to NAME_MAX, so keep this deliberately short.
    return f"/psowner_{os.getpid()}_{time.monotonic_ns() & 0xFFFFFFFF:x}"


def daemon_command(daemon: Path, shm: str, store: Path) -> list[str]:
    return [
        str(daemon),
        "--shm",
        shm,
        "--store",
        str(store),
        "--page-size",
        str(PAGE_SIZE),
        "--nshards",
        str(SHARD_COUNT),
        "--storage",
        "posix",
    ]


@dataclass
class DaemonProcess:
    process: subprocess.Popen[str]
    shm: str
    log: Path

    def cleanup(self) -> None:
        if self.process.poll() is None:
            try:
                os.kill(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass
        shm_unlink(self.shm)


def launch_daemon(daemon: Path, store: Path, log: Path) -> DaemonProcess:
    shm = next_shm_name()
    with log.open("ab") as stream:
        process = subprocess.Popen(
            daemon_command(daemon, shm, store),
            stdout=stream,
            stderr=subprocess.STDOUT,
            env=private_environment(),
            start_new_session=True,
        )
    return DaemonProcess(process, shm, log)


def inspect_health(inspector: Path, shm: str, timeout: float) -> dict[str, Any] | None:
    try:
        result = subprocess.run(
            [str(inspector), "--shm", shm, "health"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=private_environment(),
            timeout=max(0.05, timeout),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        value = json.loads(result.stdout)
    except (ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def wait_ready(
    process: DaemonProcess, inspector: Path, timeout: float, label: str
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_health: dict[str, Any] | None = None
    while True:
        returncode = process.process.poll()
        if returncode is not None:
            log_tail = process.log.read_text(encoding="utf-8", errors="replace")[-2000:]
            raise TestFailure(
                f"{label}: daemon exited before health with code {returncode}\n{log_tail}"
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TestFailure(f"{label}: health deadline expired; last={last_health!r}")
        last_health = inspect_health(inspector, process.shm, min(0.5, remaining))
        if last_health is not None:
            require(last_health.get("page_size") == PAGE_SIZE,
                    f"{label}: unexpected page size {last_health!r}")
            require(last_health.get("nshards") == SHARD_COUNT,
                    f"{label}: unexpected shard count {last_health!r}")
            return last_health
        time.sleep(min(0.01, remaining))


def wait_exit(process: DaemonProcess, timeout: float, expected: int, label: str) -> None:
    try:
        result = process.process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        raise TestFailure(f"{label}: did not exit before deadline") from error
    require(result == expected, f"{label}: expected exit code {expected}, got {result}")


def stop_clean(process: DaemonProcess, timeout: float, label: str) -> None:
    if process.process.poll() is not None:
        require(process.process.returncode == 0,
                f"{label}: daemon had unexpected exit {process.process.returncode}")
        return
    os.kill(process.process.pid, signal.SIGTERM)
    wait_exit(process, timeout, 0, label)


def kill_owner(process: DaemonProcess, timeout: float, label: str) -> None:
    os.kill(process.process.pid, signal.SIGKILL)
    wait_exit(process, timeout, -signal.SIGKILL, label)


def assert_regular(path: Path, label: str) -> None:
    require(path.exists() and not path.is_symlink() and path.is_file(),
            f"{label}: expected regular file {path}")


def snapshot_tree(root: Path) -> dict[str, tuple[Any, ...]]:
    """Capture file identity/content without following symlinks."""
    result: dict[str, tuple[Any, ...]] = {}

    def visit(directory: Path) -> None:
        for entry in sorted(os.scandir(directory), key=lambda item: item.name):
            path = Path(entry.path)
            relative = str(path.relative_to(root))
            stat_result = path.lstat()
            mode = stat_result.st_mode
            if path.is_symlink():
                result[relative] = ("symlink", stat_result.st_mode & 0o7777,
                                    os.readlink(path))
            elif path.is_dir():
                result[relative] = ("directory", stat_result.st_mode & 0o7777)
                visit(path)
            elif path.is_file():
                result[relative] = ("file", stat_result.st_mode & 0o7777,
                                    hashlib.sha256(path.read_bytes()).digest(),
                                    stat_result.st_size)
            else:
                result[relative] = ("other", stat_result.st_mode & 0o7777)

    visit(root)
    return result


def assert_tree_unchanged(before: dict[str, tuple[Any, ...]], root: Path, label: str) -> None:
    after = snapshot_tree(root)
    require(after == before, f"{label}: store files changed\nbefore={before!r}\nafter={after!r}")


def layer_path(store: Path, layer_id: int) -> Path:
    shard = (layer_id >> 48) & 0xFFFF
    return store / f"layer_{shard}_{layer_id:016x}"


def seed_store(daemon: Path, inspector: Path, store: Path, log: Path, timeout: float) -> None:
    process = launch_daemon(daemon, store, log)
    try:
        wait_ready(process, inspector, timeout, "seed store")
        lock = store / ".pagestore.lock"
        assert_regular(lock, "seed store")
        stop_clean(process, timeout, "seed store shutdown")
    finally:
        process.cleanup()


def test_duplicate_shm_does_not_touch_store(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "duplicate-store"
    store.mkdir()
    first = launch_daemon(daemon, store, root / "duplicate-first.log")
    second: DaemonProcess | None = None
    try:
        wait_ready(first, inspector, timeout, "duplicate owner")
        lock = store / ".pagestore.lock"
        assert_regular(lock, "duplicate owner")
        before = snapshot_tree(store)

        second = launch_daemon(daemon, store, root / "duplicate-second.log")
        wait_exit(second, timeout, 1, "duplicate owner")
        require(inspect_health(inspector, first.shm, timeout) is not None,
                "duplicate owner: first daemon lost health")
        assert_tree_unchanged(before, store, "duplicate owner")
    finally:
        if second is not None:
            second.cleanup()
        first.cleanup()


def test_sigkill_releases_owner(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "sigkill-store"
    store.mkdir()
    first = launch_daemon(daemon, store, root / "sigkill-first.log")
    second: DaemonProcess | None = None
    try:
        wait_ready(first, inspector, timeout, "SIGKILL owner")
        kill_owner(first, timeout, "SIGKILL owner")
        second = launch_daemon(daemon, store, root / "sigkill-second.log")
        wait_ready(second, inspector, timeout, "post-SIGKILL restart")
        stop_clean(second, timeout, "post-SIGKILL shutdown")
    finally:
        if second is not None:
            second.cleanup()
        first.cleanup()


def test_empty_orphan_is_removed(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "orphan-store"
    store.mkdir()
    seed_store(daemon, inspector, store, root / "orphan-seed.log", timeout)
    establish_clean_manifest(
        daemon, inspector, store, root / "orphan-clean-manifest.log", timeout
    )
    orphan = layer_path(store, ORPHAN_ID)
    unrelated = store / "unrelated-preserved.bin"
    orphan.touch()
    unrelated.write_bytes(b"do not remove\n")

    process = launch_daemon(daemon, store, root / "orphan-restart.log")
    try:
        wait_ready(process, inspector, timeout, "orphan cleanup restart")
        require(not orphan.exists() and not orphan.is_symlink(),
                f"orphan cleanup restart: empty orphan survived: {orphan}")
        require(unrelated.read_bytes() == b"do not remove\n",
                "orphan cleanup restart: unrelated file changed")
        stop_clean(process, timeout, "orphan cleanup shutdown")
    finally:
        process.cleanup()


def test_missing_manifest_skips_sweep(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "missing-manifest-store"
    store.mkdir()
    seed_store(daemon, inspector, store, root / "missing-seed.log", timeout)
    manifest = store / "layers.manifest"
    if manifest.exists() or manifest.is_symlink():
        manifest.unlink()
    orphan = layer_path(store, ORPHAN_ID)
    orphan.touch()

    process = launch_daemon(daemon, store, root / "missing-manifest.log")
    try:
        wait_ready(process, inspector, timeout, "missing manifest")
        require(orphan.exists() and not orphan.is_symlink() and orphan.stat().st_size == 0,
                "missing manifest: orphan was swept")
        stop_clean(process, timeout, "missing manifest shutdown")
    finally:
        process.cleanup()


def test_unsafe_layer_entries_fail_closed(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    for case, make_unsafe in (("malformed", _make_malformed), ("symlink", _make_symlink)):
        store = root / f"unsafe-{case}-store"
        store.mkdir()
        seed_store(daemon, inspector, store, root / f"unsafe-{case}-seed.log", timeout)
        establish_clean_manifest(
            daemon, inspector, store, root / f"unsafe-{case}-clean-manifest.log", timeout
        )
        orphan = layer_path(store, ORPHAN_ID)
        orphan.touch()
        keep = store / "unrelated-preserved.bin"
        keep.write_bytes(b"preserve\n")
        unsafe = make_unsafe(store)
        outside = store.parent / f"{case}-outside-target"
        if outside.exists():
            outside.unlink()
        if case == "symlink":
            outside.write_bytes(b"outside target\n")
            unsafe.symlink_to(outside)

        process = launch_daemon(daemon, store, root / f"unsafe-{case}.log")
        try:
            wait_exit(process, timeout, 1, f"unsafe {case} layer")
            require(orphan.exists() and not orphan.is_symlink() and orphan.stat().st_size == 0,
                    f"unsafe {case} layer: valid orphan was removed")
            require(keep.read_bytes() == b"preserve\n",
                    f"unsafe {case} layer: unrelated file changed")
            require(unsafe.is_symlink() if case == "symlink" else unsafe.exists(),
                    f"unsafe {case} layer: unsafe entry disappeared")
        finally:
            process.cleanup()


def test_unsafe_copy_temporaries_fail_closed(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    exited = subprocess.Popen([sys.executable, "-c", "pass"], env=private_environment())
    require(exited.wait(timeout=timeout) == 0, "prepare exited copy-owner PID")
    for label, pid in (("live", os.getpid()), ("exited", exited.pid)):
        for kind in ("directory", "symlink", "hardlink"):
            case = f"copy-{label}-{kind}"
            store = root / f"{case}-store"
            store.mkdir()
            seed_store(daemon, inspector, store, root / f"{case}-seed.log", timeout)
            establish_clean_manifest(
                daemon, inspector, store, root / f"{case}-manifest.log", timeout
            )
            orphan = layer_path(store, ORPHAN_ID)
            orphan.touch()
            unsafe = store / f"{layer_path(store, ORPHAN_ID + 1).name}.tmp.{pid}.1"
            outside = root / f"{case}-outside"
            outside.write_bytes(b"outside bytes\n")
            if kind == "directory":
                unsafe.mkdir()
            elif kind == "symlink":
                unsafe.symlink_to(outside)
            else:
                os.link(outside, unsafe)
            before = snapshot_tree(store)
            process = launch_daemon(daemon, store, root / f"{case}.log")
            try:
                wait_exit(process, timeout, 1, case)
                assert_tree_unchanged(before, store, case)
                require(outside.read_bytes() == b"outside bytes\n",
                        f"{case}: outside target changed")
            finally:
                process.cleanup()


def test_malformed_inhibition_markers_fail_closed(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    for case in ("symlink", "fifo", "nonempty"):
        store = root / f"malformed-marker-{case}-store"
        store.mkdir()
        seed_store(daemon, inspector, store, root / f"marker-{case}-seed.log", timeout)
        establish_clean_manifest(
            daemon, inspector, store, root / f"marker-{case}-clean-manifest.log", timeout
        )
        orphan = layer_path(store, ORPHAN_ID)
        orphan.touch()
        marker = store / ".pagestore-orphan-sweep-inhibited"
        marker_target = store.parent / f"marker-{case}-target"
        if case == "symlink":
            marker_target.write_bytes(b"marker target\n")
            marker.symlink_to(marker_target)
        elif case == "fifo":
            os.mkfifo(marker, 0o600)
        else:
            marker.write_bytes(b"not an empty marker\n")

        process = launch_daemon(daemon, store, root / f"marker-{case}.log")
        try:
            wait_exit(process, timeout, 1, f"malformed inhibition marker {case}")
            require(orphan.exists() and not orphan.is_symlink() and orphan.stat().st_size == 0,
                    f"malformed inhibition marker {case}: orphan was removed")
            if case == "symlink":
                require(marker.is_symlink() and marker.resolve() == marker_target.resolve(),
                        "malformed inhibition marker symlink: marker changed")
                require(marker_target.read_bytes() == b"marker target\n",
                        "malformed inhibition marker symlink: target changed")
            elif case == "fifo":
                require(stat.S_ISFIFO(marker.lstat().st_mode),
                        "malformed inhibition marker FIFO: marker changed")
            else:
                require(marker.read_bytes() == b"not an empty marker\n",
                        "malformed inhibition marker nonempty: marker changed")
        finally:
            process.cleanup()


def test_manifest_aliases_fail_closed(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    for case in ("symlink", "fifo"):
        store = root / f"manifest-alias-{case}-store"
        store.mkdir()
        seed_store(daemon, inspector, store, root / f"manifest-{case}-seed.log", timeout)
        establish_clean_manifest(
            daemon, inspector, store, root / f"manifest-{case}-clean-manifest.log", timeout
        )
        orphan = layer_path(store, ORPHAN_ID)
        orphan.touch()
        manifest = store / "layers.manifest"
        manifest.unlink()
        if case == "symlink":
            target = store.parent / "manifest-symlink-target"
            target.write_bytes(
                manifest_record(MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0))
            )
            manifest.symlink_to(target)
        else:
            # The daemon must reject this without blocking in FIFO open/read.
            os.mkfifo(manifest, 0o600)

        process = launch_daemon(daemon, store, root / f"manifest-{case}.log")
        try:
            wait_exit(process, timeout, 1, f"manifest {case} alias")
            require(orphan.exists() and not orphan.is_symlink() and orphan.stat().st_size == 0,
                    f"manifest {case} alias: orphan was removed")
            if case == "symlink":
                require(manifest.is_symlink() and manifest.resolve() == target.resolve(),
                        "manifest symlink: manifest path changed")
                require(target.read_bytes() == manifest_record(
                    MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0)
                ), "manifest symlink: target changed")
            else:
                require(stat.S_ISFIFO(manifest.lstat().st_mode),
                        "manifest FIFO: manifest path changed")
        finally:
            process.cleanup()


def _make_malformed(store: Path) -> Path:
    path = store / "layer_0_not-a-hex-id"
    path.write_bytes(b"malformed layer name\n")
    return path


def _make_symlink(store: Path) -> Path:
    return store / "layer_0_0000000000005000"


def fnv1a(data: bytes) -> int:
    value = 2166136261
    for byte in data:
        value = ((value ^ byte) * 16777619) & 0xFFFFFFFF
    return value


def manifest_record(
    event_type: int, payload: bytes, corrupt_crc: bool = False
) -> bytes:
    header_without_crc = struct.pack(
        "<IIII", MANIFEST_MAGIC, MANIFEST_VERSION,
        event_type, len(payload)
    )
    crc = fnv1a(header_without_crc + payload)
    if corrupt_crc:
        crc ^= 1
    return header_without_crc + struct.pack("<I", crc) + payload


def establish_clean_manifest(
    daemon: Path, inspector: Path, store: Path, log: Path, timeout: float
) -> None:
    """Make a valid manifest and prove the daemon can replay it cleanly."""
    manifest = store / "layers.manifest"
    manifest.write_bytes(
        manifest_record(MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0))
    )
    process = launch_daemon(daemon, store, log)
    try:
        wait_ready(process, inspector, timeout, "clean manifest validation")
        stop_clean(process, timeout, "clean manifest validation shutdown")
    finally:
        process.cleanup()


def test_corrupt_manifest_tail_does_not_delete_orphan(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "corrupt-manifest-store"
    store.mkdir()
    seed_store(daemon, inspector, store, root / "corrupt-seed.log", timeout)
    orphan = layer_path(store, ORPHAN_ID)
    orphan.touch()

    # A valid v3 no-op record anchors the file format.  The second complete
    # record has a bad CRC, so replay must truncate only that final record.
    valid = manifest_record(MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0))
    corrupt = manifest_record(
        MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0), corrupt_crc=True
    )
    manifest = store / "layers.manifest"
    manifest.write_bytes(valid + corrupt)
    marker = store / ".pagestore-orphan-sweep-inhibited"

    process = launch_daemon(daemon, store, root / "corrupt-manifest.log")
    try:
        wait_ready(process, inspector, timeout, "corrupt manifest tail")
        require(orphan.exists() and not orphan.is_symlink() and orphan.stat().st_size == 0,
                "corrupt manifest tail: orphan was removed")
        assert_regular(marker, "corrupt manifest tail")
        stop_clean(process, timeout, "corrupt manifest shutdown")
        require(manifest.read_bytes() == valid,
                "corrupt manifest tail: final CRC-corrupt record was not truncated")
    finally:
        process.cleanup()

    second = launch_daemon(daemon, store, root / "corrupt-manifest-second.log")
    try:
        wait_ready(second, inspector, timeout, "inhibited orphan restart")
        require(orphan.exists() and orphan.stat().st_size == 0,
                "inhibited orphan restart: orphan was removed")
        assert_regular(marker, "inhibited orphan restart")
        stop_clean(second, timeout, "inhibited orphan shutdown")
    finally:
        second.cleanup()


def test_interior_manifest_corruption_fails_closed(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "interior-corrupt-manifest-store"
    store.mkdir()
    seed_store(daemon, inspector, store, root / "interior-seed.log", timeout)
    orphan = layer_path(store, ORPHAN_ID)
    orphan.touch()
    valid = manifest_record(MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0))
    corrupt = manifest_record(
        MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0), corrupt_crc=True
    )
    (store / "layers.manifest").write_bytes(valid + corrupt + valid)

    process = launch_daemon(daemon, store, root / "interior-corrupt-manifest.log")
    try:
        wait_exit(process, timeout, 1, "interior manifest corruption")
        require(orphan.exists() and not orphan.is_symlink() and orphan.stat().st_size == 0,
                "interior manifest corruption: orphan was removed")
        require(not (store / ".pagestore-orphan-sweep-inhibited").exists(),
                "interior manifest corruption: terminal-repair marker was created")
    finally:
        process.cleanup()


def test_torn_first_manifest_record_stays_quarantined(
    daemon: Path, inspector: Path, root: Path, timeout: float
) -> None:
    store = root / "torn-first-manifest-store"
    store.mkdir()
    seed_store(daemon, inspector, store, root / "torn-first-seed.log", timeout)
    orphan = layer_path(store, ORPHAN_ID)
    orphan.touch()
    valid = manifest_record(MANIFEST_REMOVE_LAYER, struct.pack("<QQ", 0, 0))
    manifest = store / "layers.manifest"
    manifest.write_bytes(valid[:-1])
    marker = store / ".pagestore-orphan-sweep-inhibited"

    first = launch_daemon(daemon, store, root / "torn-first-manifest.log")
    try:
        wait_ready(first, inspector, timeout, "torn first manifest record")
        require(orphan.exists() and orphan.stat().st_size == 0,
                "torn first manifest record: orphan was removed")
        assert_regular(marker, "torn first manifest record")
        require(manifest.read_bytes() == b"",
                "torn first manifest record: torn record was not truncated")
        stop_clean(first, timeout, "torn first manifest shutdown")
    finally:
        first.cleanup()

    second = launch_daemon(daemon, store, root / "torn-first-manifest-second.log")
    try:
        wait_ready(second, inspector, timeout, "torn first manifest restart")
        require(orphan.exists() and orphan.stat().st_size == 0,
                "torn first manifest restart: orphan was removed")
        assert_regular(marker, "torn first manifest restart")
        require(manifest.read_bytes() == b"",
                "torn first manifest restart: manifest changed unexpectedly")
        stop_clean(second, timeout, "torn first manifest restart shutdown")
    finally:
        second.cleanup()


def run(args: argparse.Namespace) -> None:
    daemon = Path(args.daemon_binary).resolve()
    inspector = Path(args.inspect_binary).resolve()
    require(daemon.is_file() and os.access(daemon, os.X_OK),
            f"daemon binary is not executable: {daemon}")
    require(inspector.is_file() and os.access(inspector, os.X_OK),
            f"inspector binary is not executable: {inspector}")
    require(args.timeout > 0, "--timeout must be positive")

    root = Path(tempfile.mkdtemp(prefix="pagestore-owner-recovery-", dir=args.work_dir))
    completed = False
    try:
        tests = (
            ("duplicate shm ownership", test_duplicate_shm_does_not_touch_store),
            ("SIGKILL owner release", test_sigkill_releases_owner),
            ("empty orphan cleanup", test_empty_orphan_is_removed),
            ("missing manifest skips sweep", test_missing_manifest_skips_sweep),
            ("unsafe layer fail-closed", test_unsafe_layer_entries_fail_closed),
            ("unsafe copy temporaries fail-closed", test_unsafe_copy_temporaries_fail_closed),
            ("malformed inhibition markers fail-closed", test_malformed_inhibition_markers_fail_closed),
            ("manifest aliases fail-closed", test_manifest_aliases_fail_closed),
            ("corrupt manifest tail safety", test_corrupt_manifest_tail_does_not_delete_orphan),
            ("interior manifest fail-closed", test_interior_manifest_corruption_fails_closed),
            ("torn first manifest quarantine", test_torn_first_manifest_record_stays_quarantined),
        )
        for label, test in tests:
            test(daemon, inspector, root, args.timeout)
            print(f"ok - {label}")
        completed = True
    finally:
        if args.keep or not completed:
            destination = "temporary root preserved" if not completed else "temporary root"
            print(f"{destination}: {root}", file=sys.stderr if not completed else sys.stdout)
        else:
            import shutil

            shutil.rmtree(root, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon-binary", required=True,
                        help="path to pagestore_daemon")
    parser.add_argument("--inspect-binary", required=True,
                        help="path to pagestore_inspect")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help=f"per-process deadline in seconds (default: {DEFAULT_TIMEOUT:g})")
    parser.add_argument("--work-dir", type=Path, default=None,
                        help="parent directory for temporary test data")
    parser.add_argument("--keep", action="store_true",
                        help="retain the temporary root after the run")
    args = parser.parse_args(argv)
    try:
        run(args)
    except (OSError, TestFailure) as error:
        print(f"not ok - {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
