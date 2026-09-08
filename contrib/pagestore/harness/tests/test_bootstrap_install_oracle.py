#!/usr/bin/env python3
"""Unit tests for bootstrap_install_oracle.py; no PostgreSQL binaries needed."""

from __future__ import annotations

import contextlib
import io
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


HELPER = Path(__file__).with_name("bootstrap_install_oracle.py")
SPEC = importlib.util.spec_from_file_location("bootstrap_install_oracle", HELPER)
assert SPEC is not None and SPEC.loader is not None
ORACLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ORACLE)


class BootstrapInstallOracleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.prepared = self.root / "prepared"
        self.target = self.root / "target"
        self.prepared.mkdir()
        self.target.mkdir()
        self._make_fixture()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write(self, root: Path, relative: str, data: bytes) -> None:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _make_fixture(self) -> None:
        manifest = (
            b'{"format":2,"oldest_commit_ts_xid":"0",'
            b'"next_commit_ts_xid":"0"}\n'
        )
        maps = [(0, b"global-map\n"), (42, b"database-map\n")]
        header_size = ORACLE.BOOTSTRAP_HEADER.size
        artifact_size = header_size + sum(
            ORACLE.BOOTSTRAP_MAP.size + len(data) for _oid, data in maps
        )
        header = ORACLE.BOOTSTRAP_HEADER.pack(
            1, 2, 3, 4, artifact_size, ORACLE.BOOTSTRAP_MAGIC,
            ORACLE.BOOTSTRAP_FORMAT, 1, 0, len(maps), 0, 0, 0,
        )
        bootstrap = header + b"".join(
            ORACLE.BOOTSTRAP_MAP.pack(oid, len(data)) + data
            for oid, data in maps
        )
        for root in (self.prepared, self.target):
            self._write(root, ORACLE.MANIFEST, manifest)
            self._write(root, ORACLE.BOOTSTRAP, bootstrap)
            self._write(root, "pg_xact/0000", b"xact")
            self._write(root, "pg_multixact/offsets/0000", b"offsets")
            self._write(root, "pg_multixact/members/0000", b"members")
        self._write(self.prepared, "extra/source-only", b"immutable")
        self._write(self.target, ORACLE.CONTROL, b"control")
        self._write(self.target, "global/pg_filenode.map", maps[0][1])
        self._write(self.target, "base/42/pg_filenode.map", maps[1][1])
        (self.target / "pg_commit_ts").mkdir()

    def test_snapshot_and_unchanged_cover_prepared_and_control(self) -> None:
        snapshot = self.root / "inputs.json"
        ORACLE.snapshot_inputs(self.prepared, self.target, snapshot)
        ORACLE.unchanged_inputs(self.prepared, self.target, snapshot)
        (self.prepared / "extra/source-only").write_bytes(b"changed")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.unchanged_inputs(self.prepared, self.target, snapshot)

    def test_target_control_mutation_is_rejected(self) -> None:
        snapshot = self.root / "inputs.json"
        ORACLE.snapshot_inputs(self.prepared, self.target, snapshot)
        (self.target / ORACLE.CONTROL).write_bytes(b"control changed")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.unchanged_inputs(self.prepared, self.target, snapshot)

    def test_installed_checks_bootstrap_maps_slrus_and_inactive_commit_ts(self) -> None:
        ORACLE.installed(self.prepared, self.target)
        (self.target / "base/42/pg_filenode.map").write_bytes(b"wrong")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.installed(self.prepared, self.target)

    def test_missing_or_mutated_slru_is_rejected(self) -> None:
        for relative in (
            "pg_xact/0000",
            "pg_multixact/offsets/0000",
            "pg_multixact/members/0000",
        ):
            with self.subTest(relative=relative):
                path = self.target / relative
                original = path.read_bytes()
                path.unlink()
                with self.assertRaises(ORACLE.OracleError):
                    ORACLE.installed(self.prepared, self.target)
                path.write_bytes(original)
                path.write_bytes(b"mutated")
                with self.assertRaises(ORACLE.OracleError):
                    ORACLE.installed(self.prepared, self.target)
                path.write_bytes(original)

    def test_commit_ts_activity_and_exact_contents_are_checked(self) -> None:
        commit_ts = self.target / "pg_commit_ts"
        self._write(self.target, "pg_commit_ts/unexpected", b"inactive")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.installed(self.prepared, self.target)
        (commit_ts / "unexpected").unlink()

        active_manifest = (
            b'{"format":2,"oldest_commit_ts_xid":"10",'
            b'"next_commit_ts_xid":"20"}\n'
        )
        self._write(self.prepared, ORACLE.MANIFEST, active_manifest)
        self._write(self.target, ORACLE.MANIFEST, active_manifest)
        self._write(self.prepared, "pg_commit_ts/0000", b"commit-ts")
        self._write(self.target, "pg_commit_ts/0000", b"commit-ts")
        ORACLE.installed(self.prepared, self.target)
        (commit_ts / "0000").write_bytes(b"wrong commit-ts")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.installed(self.prepared, self.target)

    def test_inverted_commit_ts_horizon_is_rejected(self) -> None:
        with self.assertRaisesRegex(ORACLE.OracleError, "inverted commit-ts"):
            ORACLE._commit_ts_required({
                "oldest_commit_ts_xid": "20", "next_commit_ts_xid": "10",
            })
        self.assertTrue(ORACLE._commit_ts_required({
            "oldest_commit_ts_xid": "4294967290", "next_commit_ts_xid": "10",
        }))

    def test_installed_snapshot_is_idempotent_and_excludes_control(self) -> None:
        snapshot = self.root / "installed.json"
        ORACLE.snapshot_installed(self.target, snapshot)
        ORACLE.equal_installed(self.target, snapshot)
        self._write(self.target, "server.log", b"normal startup noise")
        self._write(self.target, ORACLE.CONTROL, b"control changed by startup")
        ORACLE.equal_installed(self.target, snapshot)
        (self.target / ORACLE.BOOTSTRAP).write_bytes(b"changed")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.equal_installed(self.target, snapshot)

    def test_added_database_map_is_detected_by_installed_snapshot(self) -> None:
        snapshot = self.root / "installed.json"
        ORACLE.snapshot_installed(self.target, snapshot)
        self._write(self.target, "base/43/pg_filenode.map", b"unexpected map")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.equal_installed(self.target, snapshot)

    def test_report_returns_exact_positive_pid(self) -> None:
        report = self.root / "report.jsonl"
        record = {
            "schema": 1,
            "name": "branch_install.after_maps",
            "action": "crash",
            "scenario": "mvp-golden",
            "seed": 1,
            "operation": "install-after-maps",
            "hit": 1,
            "pid": 12345,
        }
        report.write_text(json.dumps(record) + "\n")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            pid = ORACLE.check_report(
                report, "branch_install.after_maps", "install-after-maps"
            )
        self.assertEqual(pid, 12345)
        self.assertEqual(output.getvalue(), "")

        for field, value in (
            ("name", "branch_install.before_manifest"),
            ("operation", "other-operation"),
            ("hit", 2),
            ("pid", 0),
        ):
            with self.subTest(field=field):
                invalid = dict(record)
                invalid[field] = value
                report.write_text(json.dumps(invalid) + "\n")
                with self.assertRaises(ORACLE.OracleError):
                    ORACLE.check_report(
                        report, "branch_install.after_maps", "install-after-maps"
                    )

        report.write_text(json.dumps(record) + "\n" + json.dumps(record) + "\n")
        with self.assertRaises(ORACLE.OracleError):
            ORACLE.check_report(
                report, "branch_install.after_maps", "install-after-maps"
            )

        with self.assertRaises(ORACLE.OracleError):
            ORACLE._parse_bootstrap(
                b"\0" * (ORACLE.BOOTSTRAP_HEADER.size - 1)
            )


if __name__ == "__main__":
    unittest.main()
