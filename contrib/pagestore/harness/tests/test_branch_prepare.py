import contextlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from pathlib import Path


PAGESTORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PAGESTORE_ROOT))
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

        max_int64 = MODULE.Config.load(
            self.write_config(new_incarnation=(1 << 63) - 1)
        )
        self.assertEqual(max_int64.new_incarnation, (1 << 63) - 1)

        too_large_root = self.root / "too-large"
        too_large_root.mkdir()
        too_large = self.write_config(
            writer_log_file=str(too_large_root / "writer.log"),
            private_socket_dir=str(too_large_root / "private-socket"),
            retention_authority_dir=str(too_large_root / "authority"),
            prepared_dir=str(too_large_root / "prepared"),
            new_incarnation=(1 << 63),
        )
        with self.assertRaisesRegex(MODULE.ConfigError, "new_incarnation exceeds int64"):
            MODULE.Config.load(too_large)
        self.assertFalse((too_large_root / "writer.log").exists())
        self.assertFalse((too_large_root / "private-socket").exists())
        self.assertFalse((too_large_root / "authority").exists())
        self.assertFalse((too_large_root / "prepared").exists())

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

    def test_legacy_receipt_is_rejected_without_full_manifest_identity(self):
        config = MODULE.Config.load(self.write_config())
        receipt = {
            "schema": 1,
            "state": "complete",
            "new_timeline": config.new_timeline,
            "parent_timeline": config.parent_timeline,
            "prepared_dir": str(config.prepared_dir),
        }
        config.receipt_file.write_text(json.dumps(receipt), encoding="utf-8")
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "legacy.*full prepared-manifest identity"):
            MODULE.BranchPreparer(config).read_journal()

    def test_normal_writer_observation_uses_public_identity_not_listen_addresses(self):
        config = MODULE.Config.load(self.write_config())
        queries = []

        class UnixSocketWriter(MODULE.BranchPreparer):
            def server_running(self, data_dir):
                return True

            def writer_sql(self, sql, private=False):
                queries.append((sql, private))
                return "t" if not private else "f"

        self.assertEqual(UnixSocketWriter(config).observe_writer_mode(), "normal")
        self.assertFalse(queries[0][1])
        self.assertNotIn("listen_addresses", queries[0][0])
        self.assertIn("data_directory", queries[0][0])

    def test_branch_fault_installed_layout_and_canonical_identity(self):
        install = self.root / "installed-bin"
        install.mkdir()
        shutil.copy2(PAGESTORE_ROOT / "pagestore_branch_fault.py", install / "pagestore_branch_fault.py")
        shutil.copy2(PAGESTORE_ROOT / "pagestore_fault_points.def", install / "pagestore_fault_points.def")
        control = self.root / "fault-control"
        control.mkdir()
        env = {
            "PAGESTORE_TEST_FAULT_NAME": "branch_prepare.before_prepared_receipt",
            "PAGESTORE_TEST_FAULT_ACTION": "crash",
            "PAGESTORE_TEST_FAULT_HIT": "1",
            "PAGESTORE_TEST_FAULT_DIR": str(control),
            "PAGESTORE_TEST_FAULT_SCENARIO": "installed-layout",
            "PAGESTORE_TEST_FAULT_SEED": "7",
            "PAGESTORE_TEST_FAULT_OPERATION": "prepare",
        }
        spec = importlib.util.spec_from_file_location(
            "installed_branch_fault", install / "pagestore_branch_fault.py"
        )
        installed = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        with mock.patch.dict(os.environ, env, clear=False):
            spec.loader.exec_module(installed)
            probe = installed.BranchFaultProbe()
            self.assertEqual(probe.seed, 7)
            self.assertEqual(probe.catalog, install / "pagestore_fault_points.def")
            probe.probe("branch_prepare.before_prepared_receipt")
            self.assertEqual(probe.hit, 0)
            (control / "arm").write_text("arm\n", encoding="utf-8")
            with mock.patch.object(installed.os, "_exit", side_effect=RuntimeError("exit")):
                with self.assertRaisesRegex(RuntimeError, "exit"):
                    probe.probe("branch_prepare.before_prepared_receipt")
            self.assertEqual(probe.hit, 1)
            (control / "arm").unlink()
            probe.probe("branch_prepare.before_prepared_receipt")
            self.assertEqual(probe.hit, 1)

        broken_control = self.root / "broken-fault-control"
        broken_control.mkdir()
        (broken_control / "arm").symlink_to(self.root / "does-not-exist")
        bad_arm_env = dict(env, PAGESTORE_TEST_FAULT_DIR=str(broken_control))
        with mock.patch.dict(os.environ, bad_arm_env, clear=False):
            with self.assertRaises(installed.BranchFaultError):
                installed.BranchFaultProbe()

        for field, value in (
            ("PAGESTORE_TEST_FAULT_SEED", "07"),
            ("PAGESTORE_TEST_FAULT_SEED", "+7"),
            ("PAGESTORE_TEST_FAULT_SEED", str(1 << 63)),
            ("PAGESTORE_TEST_FAULT_WATCHDOG_MS", "1000"),
            ("PAGESTORE_TEST_FAULT_DIR", "relative-control"),
            ("PAGESTORE_TEST_FAULT_SCENARIO", "bad/name"),
        ):
            bad = dict(env)
            bad[field] = value
            with mock.patch.dict(os.environ, bad, clear=False):
                with self.assertRaises(installed.BranchFaultError):
                    installed.BranchFaultProbe()

        for field in ("release", "report.jsonl", "report.tmp"):
            dangling_control = self.root / f"dangling-{field.replace('.', '-') }"
            dangling_control.mkdir()
            (dangling_control / field).symlink_to(self.root / "missing-target")
            bad = dict(env, PAGESTORE_TEST_FAULT_DIR=str(dangling_control))
            with mock.patch.dict(os.environ, bad, clear=False):
                with self.assertRaisesRegex(installed.BranchFaultError, "already contains|invalid"):
                    installed.BranchFaultProbe()

    def test_early_recovery_restores_observed_services_and_does_not_reselect(self):
        config = MODULE.Config.load(self.write_config())
        service = {"retention": True, "paused": True, "writer": "restricted"}
        calls = []

        class EarlyRecovery(MODULE.BranchPreparer):
            def discover_recovery_services(self):
                self.writer_extension_schema = '"writer"'
                self.materializer_extension_schema = '"materializer"'

            def observe_materializer_pause(self):
                return "paused" if service["paused"] else "not paused"

            def observe_writer_mode(self):
                return service["writer"]

            def materializer_sql(self, sql):
                if "pagestore_retention_drop" in sql:
                    service["retention"] = False
                    return "0"
                if "pg_wal_replay_resume" in sql:
                    service["paused"] = False
                    return "t"
                return "not paused" if not service["paused"] else "paused"

            def writer_sql(self, sql, private=False):
                return "t" if ((private and service["writer"] == "restricted") or
                                (not private and service["writer"] == "normal")) else "f"

            def server_running(self, data_dir):
                return service["writer"] != "stopped"

            def pg_ctl(self, data_dir, *arguments, check=True):
                calls.append(arguments)
                service["writer"] = "stopped" if "stop" in arguments else "normal"
                return subprocess.CompletedProcess([], 0, "", "")

            def prepare_branch(self, base, redo, fork):
                raise AssertionError("early recovery must not reselect or prepare a boundary")

        preparer = EarlyRecovery(config)
        preparer.journal = preparer.new_journal()
        preparer.journal.update(
            state="writer_stopped", intent="start_restricted_writer",
            retention_generation=1, retention_owned=True,
            pause_owned=True, writer_owned=True, restricted_writer_running=True,
        )
        preparer.write_journal()
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "no safe exact-boundary"):
            preparer.execute()
        self.assertEqual(service, {"retention": False, "paused": False, "writer": "normal"})
        self.assertTrue(calls)
        self.assertEqual(MODULE.BranchPreparer(config).read_journal()["state"], "writer_stopped")

    def test_recovery_ownership_is_inferred_from_intent_before_action(self):
        config = MODULE.Config.load(self.write_config())
        service = {"paused": False, "writer": "normal"}
        calls = []

        class IntentRecovery(MODULE.BranchPreparer):
            def discover_recovery_services(self):
                self.writer_extension_schema = '"writer"'
                self.materializer_extension_schema = '"materializer"'

            def observe_materializer_pause(self):
                return "paused" if service["paused"] else "not paused"

            def observe_writer_mode(self):
                return service["writer"]

            def materializer_sql(self, sql):
                if "pg_get_wal_replay_pause_state" in sql:
                    return "paused" if service["paused"] else "not paused"
                if "pg_wal_replay_resume" in sql:
                    service["paused"] = False
                    return "t"
                if "pagestore_retention_drop" in sql:
                    return "0"
                raise AssertionError(sql)

            def writer_sql(self, sql, private=False):
                if private:
                    return "t" if service["writer"] == "restricted" else "f"
                return "t" if service["writer"] == "normal" else "f"

            def server_running(self, data_dir):
                return service["writer"] != "stopped"

            def pg_ctl(self, data_dir, *arguments, check=True):
                calls.append(arguments)
                service["writer"] = "stopped" if "stop" in arguments else "normal"
                return subprocess.CompletedProcess([], 0, "", "")

        cases = (
            ("preflight_complete", "capture_base", True, "normal"),
            ("preflight_complete", "install_retention", True, "normal"),
            ("base_captured", "stop_writer", False, "stopped"),
            ("checkpoint_archived", "capture_fork", True, "restricted"),
        )
        for state, intent, paused, writer in cases:
            with self.subTest(state=state, intent=intent):
                service.update(paused=paused, writer=writer)
                preparer = IntentRecovery(config)
                preparer.journal = preparer.new_journal()
                preparer.journal.update(
                    state=state,
                    intent=intent,
                    retention_generation=1,
                    retention_owned=False,
                    pause_owned=False,
                    writer_owned=False,
                    restricted_writer_running=writer == "restricted",
                )
                preparer.write_journal()
                with self.assertRaisesRegex(MODULE.BranchPrepareError, "no safe exact-boundary"):
                    preparer.execute()
                self.assertFalse(service["paused"])
                self.assertEqual(service["writer"], "normal")
        self.assertTrue(calls)

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

    def test_branch_retention_generation_is_durable_and_monotonic(self):
        config = MODULE.Config.load(self.write_config())
        first = MODULE.BranchPreparer(config)
        second = MODULE.BranchPreparer(config)

        first.reserve_branch_retention_generation()
        second.reserve_branch_retention_generation()
        self.assertEqual(first.branch_retention_generation, 1)
        self.assertEqual(second.branch_retention_generation, 2)
        self.assertEqual(
            json.loads(
                config.branch_retention_generation_file.read_text(encoding="utf-8")
            )["generation"],
            2,
        )

    def test_retention_set_lost_response_keeps_exact_drop_candidate(self):
        config = MODULE.Config.load(self.write_config())
        pin = {"present": False}
        drop_failed = {"value": True}

        class AmbiguousRetention(MODULE.BranchPreparer):
            def materializer_sql(self, sql):
                if "pagestore_retention_set" in sql:
                    pin["present"] = True
                    raise TimeoutError("retention set committed but response was lost")
                if "pagestore_retention_drop" in sql:
                    if drop_failed["value"]:
                        raise TimeoutError("retention drop response was lost")
                    pin["present"] = False
                    return "0"
                raise AssertionError(sql)

        preparer = AmbiguousRetention(config)
        preparer.materializer_extension_schema = '"materializer"'
        preparer.branch_retention_generation = 17
        preparer.journal = preparer.new_journal()
        preparer.write_journal()

        with self.assertRaisesRegex(TimeoutError, "response was lost"):
            preparer.install_branch_retention("0/10")
        self.assertTrue(pin["present"])
        self.assertFalse(preparer.branch_retention_owned)
        durable = MODULE.BranchPreparer(config).read_journal()
        self.assertEqual(durable["retention_generation"], 17)
        self.assertTrue(durable["retention_set_attempted"])
        self.assertFalse(durable["retention_owned"])
        self.assertEqual(durable["intent"], "install_retention")

        with self.assertRaisesRegex(TimeoutError, "drop response was lost"):
            preparer.release_branch_retention()
        retained = MODULE.BranchPreparer(config).read_journal()
        self.assertTrue(retained["retention_set_attempted"])
        self.assertTrue(pin["present"])

        drop_failed["value"] = False
        preparer.release_branch_retention()
        self.assertFalse(pin["present"])
        cleaned = MODULE.BranchPreparer(config).read_journal()
        self.assertFalse(cleaned["retention_set_attempted"])
        self.assertFalse(cleaned["retention_owned"])

        # A non-zero status is not success, but the attempted exact generation
        # remains a cleanup candidate for an idempotent drop.
        second = AmbiguousRetention(config)
        second.materializer_extension_schema = '"materializer"'
        second.branch_retention_generation = 18
        second.journal = second.new_journal()
        second.write_journal()
        original = second.materializer_sql

        def bad_status(sql):
            if "pagestore_retention_set" in sql:
                return "5"
            return original(sql)

        second.materializer_sql = bad_status
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "status 5"):
            second.install_branch_retention("0/20")
        self.assertFalse(second.branch_retention_owned)
        self.assertTrue(second.branch_retention_set_attempted)

    def test_execute_orders_both_captures_and_restores_services(self):
        config = MODULE.Config.load(self.write_config())

        class FakePreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config):
                super().__init__(branch_config)
                self.events = []

            def preflight(self):
                self.events.append("preflight")

            def capture_and_pin_base(self):
                self.events.extend(("capture:True", "pin-base", "resume-base"))
                return "0/10"

            def pause_and_capture(self, keep_paused):
                self.events.append(f"capture:{keep_paused}")
                return "0/40"

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
                "capture:True",
                "pin-base",
                "resume-base",
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

    def test_execute_preserves_fence_after_prepare_unknown_result(self):
        config = MODULE.Config.load(self.write_config())

        class FailingPreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config):
                super().__init__(branch_config)
                self.restored = False

            def preflight(self):
                pass

            def capture_and_pin_base(self):
                return "0/10"

            def pause_and_capture(self, keep_paused):
                return "0/40"

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
                raise TimeoutError("psql connection lost after prepare commit")

            def restore_services(self):
                self.restored = True
                return []

        preparer = FailingPreparer(config)
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "connection lost"):
            preparer.execute()
        self.assertFalse(preparer.restored)
        journal = MODULE.BranchPreparer(config).read_journal()
        self.assertEqual(journal["state"], "fork_captured")
        self.assertEqual(journal["intent"], "prepare_branch")

    def test_failed_prepared_journal_write_retries_exact_fenced_boundary(self):
        config = MODULE.Config.load(self.write_config())
        calls = []

        class AmbiguousPreparer(MODULE.BranchPreparer):
            def __init__(self, branch_config, fail_publish=False):
                super().__init__(branch_config)
                self.fail_publish = fail_publish
                self.restored = False

            def preflight(self):
                pass

            def capture_and_pin_base(self):
                self.branch_retention_generation = 7
                self.branch_retention_owned = True
                self.pause_owned = True
                return "0/10"

            def stop_writer(self):
                self.writer_owned = True

            def start_restricted_writer(self):
                self.writer_owned = True
                self.restricted_writer_running = True

            def select_checkpoint(self):
                return "0/20", "0/30"

            def archive_checkpoint(self):
                return "0/40"

            def wait_materializer(self, target):
                pass

            def pause_and_capture(self, keep_paused):
                self.pause_owned = True
                return "0/40"

            def prepare_branch(self, base, redo, fork):
                calls.append((base, redo, fork))
                return 9

            def journal_update(self, state=None, intent=None, **values):
                if self.fail_publish and state == "branch_prepared":
                    raise OSError("journal publication lost after prepare commit")
                return super().journal_update(state, intent, **values)

            def restore_services(self):
                self.restored = True
                return []

            def discover_recovery_services(self):
                self.writer_extension_schema = '"writer"'
                self.materializer_extension_schema = '"materializer"'

            def observe_recovery_ownership(self):
                self.pause_owned = True
                self.writer_owned = True
                self.restricted_writer_running = True
                self.branch_retention_generation = self.journal["retention_generation"]
                self.branch_retention_owned = True
                return "restricted"

        first = AmbiguousPreparer(config, fail_publish=True)
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "journal publication lost"):
            first.execute()
        self.assertFalse(first.restored)
        durable = MODULE.BranchPreparer(config).read_journal()
        self.assertEqual(durable["state"], "fork_captured")
        self.assertEqual(durable["intent"], "prepare_branch")

        second = AmbiguousPreparer(config)
        receipt = second.execute()
        self.assertEqual(receipt["state"], "complete")
        self.assertEqual(calls, [("0/10", "0/20", "0/40"), ("0/10", "0/20", "0/40")])

    def test_journal_rejects_legacy_incomplete_corrupt_and_identity_mismatch(self):
        config = MODULE.Config.load(self.write_config())
        preparer = MODULE.BranchPreparer(config)
        config.receipt_file.write_text(
            json.dumps({"schema": 1, "state": "prepared"}) + "\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "legacy"):
            preparer.read_journal()

        preparer.write_journal(preparer.new_journal())
        value = json.loads(config.receipt_file.read_text(encoding="utf-8"))
        value["state"] = "prepared"
        config.receipt_file.write_text(json.dumps(value) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "CRC"):
            preparer.read_journal()

        preparer.write_journal(preparer.new_journal())
        other_path = self.write_config(
            new_timeline=2, prepared_dir=str(config.prepared_dir)
        )
        other = MODULE.BranchPreparer(MODULE.Config.load(other_path))
        with self.assertRaisesRegex(MODULE.BranchPrepareError, "identity mismatch"):
            other.read_journal()

    def test_branch_fault_points_exit_and_recover_from_operation_journal(self):
        child = textwrap.dedent(
            f"""
            import json
            import subprocess
            import sys
            from pathlib import Path
            sys.path.insert(0, {str(PAGESTORE_ROOT)!r})
            import pagestore_branch_prepare as m

            calls = Path({str(self.root / 'prepare.calls')!r})
            service = Path({str(self.root / 'service-state.json')!r})
            def set_service(**values):
                current = {{
                    "retention_owned": False,
                    "materializer_paused": False,
                    "writer_mode": "normal",
                }}
                if service.exists():
                    current.update(__import__("json").loads(service.read_text()))
                current.update(values)
                service.write_text(__import__("json").dumps(current))
            class Fake(m.BranchPreparer):
                def preflight(self):
                    stat = self.config.materializer_data_dir.stat()
                    self.config.retention_authority_file.write_text(json.dumps({{
                        "retention_generation": 4,
                        "consumer_data_dir": str(self.config.materializer_data_dir),
                        "consumer_data_dev": stat.st_dev,
                        "consumer_data_ino": stat.st_ino,
                        "authority_namespace_dev": self.config.retention_authority_dir.stat().st_dev,
                        "authority_namespace_ino": self.config.retention_authority_dir.stat().st_ino,
                    }}))
                def capture_and_pin_base(self):
                    self.branch_retention_generation = 1
                    self.branch_retention_owned = True
                    set_service(retention_owned=True)
                    return "0/10"
                def stop_writer(self):
                    self.writer_owned = True
                    set_service(writer_mode="stopped")
                def start_restricted_writer(self):
                    self.writer_owned = True
                    self.restricted_writer_running = True
                    set_service(writer_mode="restricted")
                def select_checkpoint(self):
                    return "0/20", "0/30"
                def archive_checkpoint(self):
                    return "0/40"
                def wait_materializer(self, target):
                    pass
                def pause_and_capture(self, keep_paused):
                    self.pause_owned = True
                    set_service(materializer_paused=True)
                    return "0/40"
                def prepare_branch(self, base, redo, fork):
                    with calls.open("a", encoding="utf-8") as stream:
                        stream.write(base + "," + redo + "," + fork + "\\n")
                    return 7
                def release_branch_retention(self):
                    self.branch_retention_owned = False
                    set_service(retention_owned=False)
                def resume_materializer(self):
                    self.pause_owned = False
                    set_service(materializer_paused=False)
                def restore_writer(self):
                    self.writer_owned = False
                    self.restricted_writer_running = False
                    set_service(writer_mode="normal")
                def materializer_sql(self, sql):
                    if "pg_get_wal_replay_pause_state" in sql:
                        state = json.loads(service.read_text()) if service.exists() else {{}}
                        return "paused" if state.get("materializer_paused") else "not paused"
                    raise AssertionError(sql)

            class Recovery(m.BranchPreparer):
                def _state(self):
                    return json.loads((self.config.prepared_dir.parent / "service-state.json").read_text())
                def _save(self, state):
                    (self.config.prepared_dir.parent / "service-state.json").write_text(json.dumps(state))
                def materializer_sql(self, sql):
                    state = self._state()
                    if "FROM pg_extension" in sql:
                        return '"materializer"'
                    if "pg_get_wal_replay_pause_state" in sql:
                        return "paused" if state["materializer_paused"] else "not paused"
                    if "pg_wal_replay_resume" in sql:
                        state["materializer_paused"] = False
                        self._save(state)
                        return "t"
                    if "pagestore_retention_drop" in sql:
                        state["retention_owned"] = False
                        self._save(state)
                        return "0"
                    return "t"
                def writer_sql(self, sql, private=False):
                    state = self._state()
                    if "FROM pg_extension" in sql:
                        return '"writer"'
                    if private:
                        return "t" if state["writer_mode"] == "restricted" else "f"
                    return "t" if state["writer_mode"] == "normal" else "f"
                def server_running(self, data_dir):
                    return self._state()["writer_mode"] != "stopped"
                def pg_ctl(self, data_dir, *arguments, check=True):
                    state = self._state()
                    if "stop" in arguments:
                        state["writer_mode"] = "stopped"
                    elif "start" in arguments:
                        state["writer_mode"] = "normal"
                    self._save(state)
                    return subprocess.CompletedProcess([], 0, "", "")

            preparer = Recovery if len(sys.argv) > 2 else Fake
            preparer(m.Config.load(Path(sys.argv[1]))).execute()
            """
        )
        points = (
            ("branch_prepare.before_prepared_receipt", "branch_prepared"),
            ("branch_prepare.after_prepared_receipt", "prepared"),
            ("branch_prepare.after_materializer_resume", "materializer_resumed"),
            ("branch_prepare.after_writer_restore", "writer_restored"),
        )
        for point, crashed_state in points:
            with self.subTest(point=point):
                prepared_dir = self.root / point.replace(".", "-")
                config_path = self.write_config(prepared_dir=str(prepared_dir))
                config = MODULE.Config.load(config_path)
                control = self.root / (point + ".control")
                control.mkdir()
                (control / "arm").write_text("arm\n", encoding="utf-8")
                env = os.environ.copy()
                for key in (
                    "PAGESTORE_TEST_FAULT_NAME", "PAGESTORE_TEST_FAULT_ACTION",
                    "PAGESTORE_TEST_FAULT_HIT", "PAGESTORE_TEST_FAULT_DIR",
                    "PAGESTORE_TEST_FAULT_SCENARIO", "PAGESTORE_TEST_FAULT_SEED",
                    "PAGESTORE_TEST_FAULT_OPERATION", "PAGESTORE_TEST_FAULT_OPERATION_ID",
                    "PAGESTORE_TEST_FAULT_WATCHDOG_MS",
                ):
                    env.pop(key, None)
                env.update({
                    "PAGESTORE_TEST_FAULT_NAME": point,
                    "PAGESTORE_TEST_FAULT_ACTION": "crash",
                    "PAGESTORE_TEST_FAULT_HIT": "1",
                    "PAGESTORE_TEST_FAULT_DIR": str(control),
                    "PAGESTORE_TEST_FAULT_SCENARIO": "branch-h1",
                    "PAGESTORE_TEST_FAULT_SEED": "1",
                    "PAGESTORE_TEST_FAULT_OPERATION": point,
                })
                result = subprocess.run(
                    [sys.executable, "-c", child, str(config_path)],
                    env=env, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 88)
                report = json.loads((control / "report.jsonl").read_text(encoding="utf-8"))
                self.assertEqual(report["name"], point)
                self.assertEqual(report["operation"], point)
                crashed = MODULE.BranchPreparer(config).read_journal()
                self.assertEqual(crashed["state"], crashed_state)
                self.assertEqual(crashed["base_lsn"], "0/10")
                self.assertEqual(crashed["checkpoint_redo_lsn"], "0/20")
                self.assertEqual(crashed["checkpoint_end_lsn"], "0/30")
                self.assertEqual(crashed["switch_lsn"], "0/40")
                self.assertEqual(crashed["archived_through_lsn"], "0/40")
                self.assertEqual(crashed["fork_lsn"], "0/40")
                self.assertEqual(
                    crashed["materializer_resumed"],
                    crashed_state in {"materializer_resumed", "writer_restored"},
                )

                recovery = subprocess.run(
                    [sys.executable, "-c", child, str(config_path), "recover"],
                    capture_output=True, text=True,
                )
                self.assertEqual(recovery.returncode, 0, recovery.stderr)
                recovered = json.loads(config.receipt_file.read_text(encoding="utf-8"))
                self.assertEqual(recovered["state"], "complete")
                self.assertFalse(recovered["retention_owned"])
                self.assertTrue(recovered["materializer_resumed"])
                self.assertTrue(recovered["writer_restored"])
                service_state = json.loads((self.root / "service-state.json").read_text())
                self.assertEqual(
                    service_state,
                    {"retention_owned": False, "materializer_paused": False, "writer_mode": "normal"},
                )
                self.assertEqual(
                    (self.root / "prepare.calls").read_text(encoding="utf-8").count("0/10,0/20,0/40"),
                    points.index((point, crashed_state)) + 1,
                )

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
