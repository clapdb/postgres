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
            "schema": MODULE.CONFIG_SCHEMA,
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
            "retention_authority_dir": str(self.root / "controller-authority"),
            "retention_owner_id": 1,
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
        with self.assertRaisesRegex(
            MODULE.ConfigError, f"schema must be {MODULE.CONFIG_SCHEMA}"
        ):
            MODULE.Config.load(self.write_config(schema=True))
        with self.assertRaisesRegex(MODULE.ConfigError, "exceeds 1023"):
            MODULE.Config.load(self.write_config(new_timeline=1024))

        bad_log = self.root / "bad-log"
        bad_log.mkdir()
        with self.assertRaisesRegex(MODULE.ConfigError, "writer_log_file is not writable"):
            MODULE.Config.load(self.write_config(writer_log_file=str(bad_log)))

        bad_socket = self.root / "bad-socket"
        bad_socket.mkdir(mode=0o700)
        bad_socket.chmod(0o710)
        with self.assertRaisesRegex(MODULE.ConfigError, "mode 0700"):
            MODULE.Config.load(self.write_config(private_socket_dir=str(bad_socket)))

    def test_lsn_sql_and_output_helpers(self):
        self.assertEqual(MODULE.parse_lsn("1/00000002"), (1 << 32) + 2)
        self.assertEqual(MODULE.sql_literal("a'b"), "'a''b'")
        self.assertEqual(MODULE.sql_identifier('Page "Store"'), '"Page ""Store"""')
        self.assertEqual(MODULE.last_output_line("CHECKPOINT\n0/20|0/30\n"), "0/20|0/30")
        with self.assertRaisesRegex(ValueError, "invalid PostgreSQL LSN"):
            MODULE.parse_lsn("bad")
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "no result"):
            MODULE.last_output_line("\n")

    def test_owner_locks_fence_branch_and_supervisor(self):
        config = MODULE.Config.load(self.write_config())
        for path in (
            config.lock_file,
            config.prepared_lock_file,
            config.materializer_lock_file,
        ):
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
        preparer.writer_extension_schema = '"Page Store"'
        self.assertEqual(preparer.prepare_branch("0/1", "0/2", "0/3"), 1)
        self.assertTrue(preparer.private)
        self.assertTrue(preparer.sql.startswith("SET pagestore.redo_wal_from_store = on;"))
        self.assertIn('"Page Store".pagestore_prepare_branch_from_control(', preparer.sql)

    def test_preflight_discovers_and_qualifies_extension_schemas(self):
        config = MODULE.Config.load(self.write_config())
        materializer_stat = config.materializer_data_dir.stat()
        config.retention_authority_file.write_text(
            json.dumps(
                {
                    "retention_generation": 4,
                    "consumer_data_dir": str(config.materializer_data_dir),
                    "consumer_data_dev": materializer_stat.st_dev,
                    "consumer_data_ino": materializer_stat.st_ino,
                    "authority_namespace_dev": config.retention_authority_dir.stat().st_dev,
                    "authority_namespace_ino": config.retention_authority_dir.stat().st_ino,
                }
            ),
            encoding="utf-8",
        )

        class RecordingPreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config):
                super().__init__(branch_config)
                self.writer_queries = []
                self.materializer_queries = []

            def server_running(self, data_dir):
                return True

            def writer_sql(self, sql, private=False):
                self.writer_queries.append(sql)
                if "FROM pg_extension" in sql:
                    return 'Writer "Store"'
                return "t"

            def materializer_sql(self, sql):
                self.materializer_queries.append(sql)
                if "FROM pg_extension" in sql:
                    return "Materializer Store"
                if "pg_get_wal_replay_pause_state" in sql:
                    return "not paused"
                return "t"

        preparer = RecordingPreparer(config)
        preparer.preflight()
        self.assertEqual(preparer.writer_extension_schema, '"Writer ""Store"""')
        self.assertEqual(preparer.materializer_extension_schema, '"Materializer Store"')
        self.assertIn(
            "\"Writer \"\"Store\"\"\".pagestore_branch_checkpoint()",
            preparer.writer_queries[1],
        )
        self.assertIn(
            '"Materializer Store".pagestore_capture_slru_snapshot()',
            preparer.materializer_queries[1],
        )
        self.assertIn("archive_mode') IN ('on', 'always')", preparer.writer_queries[1])
        self.assertIn("FROM pg_tablespace", preparer.writer_queries[1])

    def test_ambiguous_pause_failure_remains_owned_for_cleanup(self):
        config = MODULE.Config.load(self.write_config())

        class AmbiguousPausePreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config):
                super().__init__(branch_config)
                self.owned_at_failure = False
                self.resumed = False

            def preflight(self):
                pass

            def materializer_sql(self, sql):
                if "pg_get_wal_replay_pause_state" in sql:
                    return "not paused"
                if "pg_wal_replay_pause" in sql:
                    self.owned_at_failure = self.pause_owned
                    raise MODULE.BranchPrepareError("ambiguous pause result")
                raise AssertionError(sql)

            def resume_materializer(self):
                self.resumed = True
                self.pause_owned = False

        preparer = AmbiguousPausePreparer(config)
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "ambiguous pause result"):
            preparer.execute()
        self.assertTrue(preparer.owned_at_failure)
        self.assertTrue(preparer.resumed)

    def test_duplicate_branch_main_returns_temporary_failure(self):
        config_path = self.write_config()
        config = MODULE.Config.load(config_path)
        with MODULE.OwnerLock(config.lock_file, "test"):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    MODULE.main(["--config", str(config_path)]), MODULE.EX_TEMPFAIL
                )

        stderr = io.StringIO()
        with MODULE.OwnerLock(config.prepared_lock_file, "test"):
            with contextlib.redirect_stderr(stderr):
                self.assertEqual(
                    MODULE.main(["--config", str(config_path)]), MODULE.EX_TEMPFAIL
                )
        self.assertIn("prepared artifact directory", stderr.getvalue())
        self.assertNotIn("stop the materializer supervisor", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
