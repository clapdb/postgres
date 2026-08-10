import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


PAGESTORE_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "pagestore_branch_prepare",
    PAGESTORE_ROOT / "pagestore_branch_prepare.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class BranchPrepareTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.writer = self.root / "writer"
        self.materializer = self.root / "materializer"
        self.writer.mkdir()
        self.materializer.mkdir()
        (self.writer / "PG_VERSION").write_text("19\n", encoding="utf-8")
        (self.materializer / "PG_VERSION").write_text("19\n", encoding="utf-8")

    def config_value(self, **overrides):
        value = {
            "schema": 1,
            "pg_ctl": sys.executable,
            "psql": sys.executable,
            "writer_data_dir": str(self.writer),
            "writer_host": "127.0.0.1",
            "writer_port": 5432,
            "writer_log_file": str(self.writer / "writer.log"),
            "private_socket_dir": str(self.root / "private-socket"),
            "private_port": 5433,
            "materializer_data_dir": str(self.materializer),
            "materializer_host": "127.0.0.1",
            "materializer_port": 5434,
            "prepared_dir": str(self.root / "prepared"),
            "new_timeline": 1,
            "parent_timeline": 0,
            "poll_interval_ms": 1,
            "progress_timeout_ms": 10,
            "command_timeout_seconds": 1,
        }
        value.update(overrides)
        return value

    def write_config(self, **overrides):
        path = self.root / "branch.json"
        path.write_text(
            json.dumps(self.config_value(**overrides)) + "\n", encoding="utf-8"
        )
        return path

    def test_config_is_strict_and_prepares_private_paths(self):
        config = MODULE.Config.load(self.write_config())
        self.assertEqual(config.new_timeline, 1)
        self.assertEqual(config.parent_timeline, 0)
        self.assertTrue(config.prepared_dir.is_dir())
        self.assertEqual(config.private_socket_dir.stat().st_mode & 0o777, 0o700)

        with self.assertRaisesRegex(MODULE.ConfigError, "unknown.*typo"):
            MODULE.Config.load(self.write_config(typo=True))
        with self.assertRaisesRegex(MODULE.ConfigError, "must be absolute"):
            MODULE.Config.load(self.write_config(prepared_dir="relative"))
        with self.assertRaisesRegex(MODULE.ConfigError, "outside"):
            MODULE.Config.load(
                self.write_config(prepared_dir=str(self.writer / "prepared"))
            )
        with self.assertRaisesRegex(MODULE.ConfigError, "outside"):
            MODULE.Config.load(self.write_config(prepared_dir=str(self.root)))
        with self.assertRaisesRegex(MODULE.ConfigError, "schema must be 1"):
            MODULE.Config.load(self.write_config(schema=True))
        with self.assertRaisesRegex(MODULE.ConfigError, "exceeds 1023"):
            MODULE.Config.load(self.write_config(new_timeline=1024))

    def test_lsn_sql_and_output_helpers(self):
        self.assertEqual(MODULE.parse_lsn("1/00000002"), (1 << 32) + 2)
        self.assertEqual(MODULE.sql_literal("a'b"), "'a''b'")
        self.assertEqual(MODULE.last_output_line("CHECKPOINT\n0/20|0/30\n"), "0/20|0/30")
        with self.assertRaisesRegex(ValueError, "invalid PostgreSQL LSN"):
            MODULE.parse_lsn("bad")
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "no result"):
            MODULE.last_output_line("\n")

    def test_owner_locks_fence_branch_and_supervisor(self):
        config = MODULE.Config.load(self.write_config())
        for path in (config.lock_file, config.materializer_lock_file):
            first = MODULE.OwnerLock(path, "first owner")
            second = MODULE.OwnerLock(path, "second owner")
            first.acquire()
            try:
                with self.assertRaisesRegex(MODULE.OwnershipError, "another second"):
                    second.acquire()
            finally:
                first.close()

    def test_execute_orders_both_captures_and_restores_services(self):
        config = MODULE.Config.load(self.write_config())

        class FakePreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config):
                super().__init__(branch_config)
                self.events = []

            def preflight(self):
                self.events.append("preflight")

            def pause_and_capture(self, keep_paused):
                self.events.append(f"capture:{keep_paused}")
                return "0/10" if not keep_paused else "0/40"

            def stop_writer(self):
                self.events.append("stop-writer")

            def start_restricted_writer(self):
                self.events.append("start-restricted")

            def select_checkpoint(self):
                self.events.append("checkpoint")
                return "0/20", "0/30"

            def archive_checkpoint(self):
                self.events.append("archive")
                return "0/40"

            def wait_materializer(self, target):
                self.events.append(f"wait:{target}")

            def prepare_branch(self, base, redo, fork):
                self.events.append(f"prepare:{base}:{redo}:{fork}")
                return 7

            def restore_services(self):
                self.events.append("restore")
                return []

        preparer = FakePreparer(config)
        receipt = preparer.execute()
        self.assertEqual(
            preparer.events,
            [
                "preflight",
                "capture:False",
                "stop-writer",
                "start-restricted",
                "checkpoint",
                "archive",
                "wait:0/30",
                "capture:True",
                "prepare:0/10:0/20:0/40",
                "restore",
            ],
        )
        self.assertEqual(receipt["state"], "complete")
        self.assertEqual(receipt["checkpoint_end_lsn"], "0/30")
        self.assertEqual(receipt["seeded_slru_pages"], 7)
        self.assertEqual(
            json.loads(config.receipt_file.read_text(encoding="utf-8"))["state"],
            "complete",
        )

    def test_execute_restores_after_prepare_failure(self):
        config = MODULE.Config.load(self.write_config())

        class FailingPreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config):
                super().__init__(branch_config)
                self.restored = False

            def preflight(self):
                pass

            def pause_and_capture(self, keep_paused):
                return "0/10" if not keep_paused else "0/40"

            def stop_writer(self):
                pass

            def start_restricted_writer(self):
                pass

            def select_checkpoint(self):
                return "0/20", "0/30"

            def archive_checkpoint(self):
                return "0/40"

            def wait_materializer(self, target):
                pass

            def prepare_branch(self, base, redo, fork):
                raise RuntimeError("injected prepare failure")

            def restore_services(self):
                self.restored = True
                return []

        preparer = FailingPreparer(config)
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "injected"):
            preparer.execute()
        self.assertTrue(preparer.restored)
        self.assertFalse(config.receipt_file.exists())

    def test_wait_does_not_swallow_cancellation(self):
        config = MODULE.Config.load(self.write_config())
        preparer = MODULE.BranchPreparer(config)

        def cancel():
            raise MODULE.CancelledError("cancelled")

        with self.assertRaisesRegex(MODULE.CancelledError, "cancelled"):
            preparer.wait_until("cancellation", cancel)

    def test_prepare_branch_reads_wal_from_store(self):
        config = MODULE.Config.load(self.write_config())

        class RecordingPreparer(MODULE.BranchPreparer):
            def writer_sql(self, sql, *, private=False):
                self.sql = sql
                self.private = private
                return "1\n"

        preparer = RecordingPreparer(config)
        self.assertEqual(preparer.prepare_branch("0/1", "0/2", "0/3"), 1)
        self.assertTrue(preparer.private)
        self.assertTrue(preparer.sql.startswith("SET pagestore.redo_wal_from_store = on;"))

    def test_duplicate_branch_main_returns_temporary_failure(self):
        config_path = self.write_config()
        config = MODULE.Config.load(config_path)
        with MODULE.OwnerLock(config.lock_file, "test"):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    MODULE.main(["--config", str(config_path)]), MODULE.EX_TEMPFAIL
                )


if __name__ == "__main__":
    unittest.main()
