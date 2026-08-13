import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


PAGESTORE_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "pagestore_materializer_supervisor",
    PAGESTORE_ROOT / "pagestore_materializer_supervisor.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.data = self.root / "data"
        self.data.mkdir()
        (self.data / "PG_VERSION").write_text("19\n", encoding="utf-8")

    def config_value(self, **overrides):
        value = {
            "schema": MODULE.CONFIG_SCHEMA,
            "pg_ctl": sys.executable,
            "psql": sys.executable,
            "data_dir": str(self.data),
            "socket_dir": str(self.root / "socket"),
            "port": 5432,
            "log_file": str(self.root / "log" / "materializer.log"),
            "state_dir": str(self.root / "state"),
            "retention_authority_dir": str(self.root / "controller-authority"),
            "retention_owner_id": 7001,
            "controller_instance_id": "controller-test-1",
            "poll_interval_ms": 1,
            "retry_initial_ms": 1,
            "retry_max_ms": 2,
            "max_consecutive_failures": 3,
        }
        value.update(overrides)
        return value

    def write_config(self, **overrides):
        path = self.root / "supervisor.json"
        path.write_text(
            json.dumps(self.config_value(**overrides)) + "\n", encoding="utf-8"
        )
        return path

    def test_config_is_strict_and_prepares_private_state(self):
        config = MODULE.Config.load(self.write_config())
        self.assertEqual(config.port, 5432)
        self.assertTrue(config.socket_dir.is_dir())
        self.assertTrue(config.state_dir.is_dir())
        self.assertTrue(config.retention_authority_dir.is_dir())
        self.assertEqual(config.retention_authority_dir.stat().st_mode & 0o777, 0o700)
        self.assertTrue(config.log_file.parent.is_dir())

        with self.assertRaisesRegex(MODULE.ConfigError, "unknown.*typo"):
            MODULE.Config.load(self.write_config(typo=True))
        with self.assertRaisesRegex(MODULE.ConfigError, "must be absolute"):
            MODULE.Config.load(self.write_config(state_dir="relative"))
        with self.assertRaisesRegex(
            MODULE.ConfigError, f"schema must be {MODULE.CONFIG_SCHEMA}"
        ):
            MODULE.Config.load(self.write_config(schema=True))
        with self.assertRaisesRegex(MODULE.ConfigError, "retention_owner_id"):
            value = self.config_value()
            del value["retention_owner_id"]
            path = self.root / "missing-owner.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            MODULE.Config.load(path)

        unsafe = self.root / "unsafe-authority"
        unsafe.mkdir(mode=0o777)
        unsafe.chmod(0o777)
        with self.assertRaisesRegex(MODULE.ConfigError, "mode 0700"):
            MODULE.Config.load(
                self.write_config(retention_authority_dir=str(unsafe))
            )

    def test_lsn_and_backoff_helpers(self):
        self.assertEqual(MODULE.parse_lsn("1/00000002"), (1 << 32) + 2)
        with self.assertRaisesRegex(ValueError, "invalid PostgreSQL LSN"):
            MODULE.parse_lsn("not-an-lsn")

        config = MODULE.Config.load(self.write_config())
        self.assertEqual(MODULE.retry_delay_ms(config, 1), 1)
        self.assertEqual(MODULE.retry_delay_ms(config, 2), 2)
        self.assertEqual(MODULE.retry_delay_ms(config, 20), 2)

    def test_owner_lock_rejects_a_duplicate(self):
        first_config = MODULE.Config.load(self.write_config())
        second_config = MODULE.Config.load(
            self.write_config(state_dir=str(self.root / "other-state"))
        )
        self.assertEqual(first_config.lock_file, second_config.lock_file)
        first = MODULE.OwnerLock(first_config.lock_file)
        second = MODULE.OwnerLock(second_config.lock_file)
        first.acquire()
        self.addCleanup(first.close)
        with self.assertRaisesRegex(MODULE.OwnershipError, "another.*owns"):
            second.acquire()

    def test_duplicate_main_returns_temporary_failure(self):
        config_path = self.write_config()
        config = MODULE.Config.load(config_path)
        with MODULE.OwnerLock(config.lock_file):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    MODULE.main(["--config", str(config_path)]), MODULE.EX_TEMPFAIL
                )

    def test_controller_authority_fences_a_cloned_pgdata(self):
        first = MODULE.Config.load(self.write_config())
        clone = self.root / "clone"
        clone.mkdir()
        (clone / "PG_VERSION").write_text("19\n", encoding="utf-8")
        second = MODULE.Config.load(self.write_config(
            data_dir=str(clone), state_dir=str(self.root / "clone-state")
        ))
        self.assertNotEqual(first.state_dir, second.state_dir)
        self.assertNotEqual(first.lock_file, second.lock_file)
        self.assertEqual(
            first.retention_authority_lock_file,
            second.retention_authority_lock_file,
        )
        with MODULE.OwnerLock(first.retention_authority_lock_file):
            with self.assertRaisesRegex(MODULE.OwnershipError, "another.*owns"):
                with MODULE.OwnerLock(second.retention_authority_lock_file):
                    self.fail("cloned PGDATA acquired controller authority")

    def test_status_update_is_atomic_and_carries_epochs(self):
        config = MODULE.Config.load(self.write_config())
        MODULE.atomic_write_json(
            config.status_file,
            {
                "owner_epoch": 4,
                "worker_generation": 7,
                "retention_generation": 11,
            },
        )
        MODULE.atomic_write_json(
            config.retention_generation_file,
            {
                "retention_generation": 11,
                "consumer_data_dir": str(config.data_dir),
                "consumer_instance_id": config.controller_instance_id,
                "consumer_data_dev": config.data_dir.stat().st_dev,
                "consumer_data_ino": config.data_dir.stat().st_ino,
                "authority_namespace_dev": config.retention_authority_dir.stat().st_dev,
                "authority_namespace_ino": config.retention_authority_dir.stat().st_ino,
            },
        )
        supervisor = MODULE.Supervisor(config)
        self.assertEqual(supervisor.owner_epoch, 5)
        self.assertEqual(supervisor.generation, 7)
        self.assertEqual(supervisor.retention_generation, 11)
        self.assertEqual(
            json.loads(config.retention_generation_file.read_text(encoding="utf-8"))[
                "retention_generation"
            ],
            11,
        )

        supervisor.publish("running", reason="test")
        status = json.loads(config.status_file.read_text(encoding="utf-8"))
        self.assertEqual(status["schema"], MODULE.STATUS_SCHEMA)
        self.assertEqual(status["state"], "running")
        self.assertEqual(status["reason"], "test")
        self.assertEqual(status["retention_owner_id"], 7001)
        self.assertEqual(status["retention_generation"], 11)

    def test_start_persists_and_passes_a_new_retention_generation(self):
        config = MODULE.Config.load(self.write_config())

        class StartingSupervisor(MODULE.Supervisor):
            def __init__(self, supervisor_config):
                super().__init__(supervisor_config)
                self.command_args = None

            def pg_ctl(self, *arguments, **_kwargs):
                self.command_args = arguments

            def worker_healthy(self):
                return True

        supervisor = StartingSupervisor(config)
        supervisor.start_worker("test")
        self.assertEqual(supervisor.retention_generation, 1)
        command = " ".join(supervisor.command_args)
        self.assertIn("pagestore.retention_owner_id=7001", command)
        self.assertIn(
            "pagestore.retention_owner_generation=1", command
        )
        status = json.loads(config.status_file.read_text(encoding="utf-8"))
        self.assertEqual(status["retention_generation"], 1)
        config.status_file.unlink()
        recovered = MODULE.Supervisor(config)
        self.assertEqual(recovered.retention_generation, 1)

    def test_takeover_quiesces_the_previous_pgdata_before_generation_advance(self):
        first = MODULE.Config.load(self.write_config())
        MODULE.atomic_write_json(
            first.retention_generation_file,
            {
                "retention_generation": 4,
                "consumer_data_dir": str(first.data_dir),
                "consumer_instance_id": first.controller_instance_id,
                "consumer_data_dev": first.data_dir.stat().st_dev,
                "consumer_data_ino": first.data_dir.stat().st_ino,
                "authority_namespace_dev": first.retention_authority_dir.stat().st_dev,
                "authority_namespace_ino": first.retention_authority_dir.stat().st_ino,
            },
        )
        clone = self.root / "clone"
        clone.mkdir()
        (clone / "PG_VERSION").write_text("19\n", encoding="utf-8")
        second = MODULE.Config.load(self.write_config(data_dir=str(clone)))

        class TakeoverSupervisor(MODULE.Supervisor):
            def __init__(self, supervisor_config):
                super().__init__(supervisor_config)
                self.commands = []

            def command(self, arguments, **_kwargs):
                self.commands.append(arguments)
                status = 0 if arguments[-1] == "status" else 0
                return MODULE.subprocess.CompletedProcess(arguments, status, "", "")

            def worker_healthy(self):
                return True

        supervisor = TakeoverSupervisor(second)
        supervisor.start_worker("takeover")
        rendered = [" ".join(command) for command in supervisor.commands]
        self.assertTrue(any(str(first.data_dir) in item and " stop" in item
                            for item in rendered))
        self.assertEqual(supervisor.retention_generation, 5)

    def test_existing_ambiguous_status_fails_closed(self):
        config = MODULE.Config.load(self.write_config())
        MODULE.atomic_write_json(config.status_file, {"state": "running"})
        with self.assertRaisesRegex(MODULE.OwnershipError, "cannot be migrated"):
            MODULE.Supervisor(config)

    def test_malformed_existing_status_fails_closed(self):
        config = MODULE.Config.load(self.write_config())
        config.status_file.write_text("{broken", encoding="utf-8")
        with self.assertRaisesRegex(MODULE.OwnershipError, "status is unreadable"):
            MODULE.Supervisor(config)

    def test_takeover_from_another_controller_instance_fails_closed(self):
        config = MODULE.Config.load(self.write_config())
        MODULE.atomic_write_json(
            config.retention_generation_file,
            {
                "retention_generation": 4,
                "consumer_data_dir": str(config.data_dir),
                "consumer_instance_id": "another-controller",
                "consumer_data_dev": config.data_dir.stat().st_dev,
                "consumer_data_ino": config.data_dir.stat().st_ino,
                "authority_namespace_dev": config.retention_authority_dir.stat().st_dev,
                "authority_namespace_ino": config.retention_authority_dir.stat().st_ino,
            },
        )
        supervisor = MODULE.Supervisor(config)
        with self.assertRaisesRegex(MODULE.OwnershipError, "another controller"):
            supervisor.start_worker("takeover")

    def test_start_failures_retry_to_the_configured_bound(self):
        config = MODULE.Config.load(self.write_config())

        class AlwaysFailingSupervisor(MODULE.Supervisor):
            def __init__(self, supervisor_config):
                super().__init__(supervisor_config)
                self.starts = 0

            def worker_running(self):
                return False

            def start_worker(self, reason):
                self.starts += 1
                raise RuntimeError(f"start {self.starts} failed: {reason}")

        class Owner:
            def publish_owner(self, _pid, _epoch):
                pass

        supervisor = AlwaysFailingSupervisor(config)
        self.assertEqual(supervisor.run(Owner()), 1)
        self.assertEqual(supervisor.starts, config.max_consecutive_failures)
        self.assertFalse(config.status_file.exists())
        self.assertFalse(config.retention_generation_file.exists())
        # A retry remains a genuinely new owner instead of failing on
        # status-only state created by the transient startup failures.
        recovered = MODULE.Supervisor(config)
        self.assertEqual(recovered.retention_generation, 0)

    def test_running_worker_without_authority_fails_closed(self):
        config = MODULE.Config.load(self.write_config())

        class LegacySupervisor(MODULE.Supervisor):
            def worker_running(self):
                return True

        class Owner:
            def publish_owner(self, _pid, _epoch):
                pass

        with self.assertRaisesRegex(MODULE.OwnershipError, "no durable"):
            LegacySupervisor(config).run(Owner())

    def test_restartpoint_without_new_checkpoint_is_retried(self):
        config = MODULE.Config.load(self.write_config())

        class StalledSupervisor(MODULE.Supervisor):
            def read_progress(self):
                return MODULE.Progress("0/2", "0/2", "0/1", 1)

        supervisor = StalledSupervisor(config)
        for failure in range(1, config.max_consecutive_failures + 1):
            supervisor.restart_baseline = 1
            supervisor.restart_deadline = 0
            keep_running = supervisor.observe_progress(float(failure))
            self.assertTrue(keep_running)
        self.assertEqual(supervisor.failures, 0)

    def test_missing_progress_api_reaches_the_configured_bound(self):
        config = MODULE.Config.load(self.write_config(progress_timeout_ms=1))

        class MissingProgressSupervisor(MODULE.Supervisor):
            def read_progress(self):
                return None

        supervisor = MissingProgressSupervisor(config)
        supervisor.progress_api_wait_started = 0
        for failure in range(1, config.max_consecutive_failures + 1):
            supervisor.retry_not_before = 0
            keep_running = supervisor.observe_progress(float(failure))
            self.assertEqual(keep_running, failure < config.max_consecutive_failures)
        self.assertEqual(supervisor.failures, config.max_consecutive_failures)

    def test_missing_progress_api_honors_retry_backoff(self):
        config = MODULE.Config.load(self.write_config(progress_timeout_ms=1))

        class MissingProgressSupervisor(MODULE.Supervisor):
            def read_progress(self):
                return None

        supervisor = MissingProgressSupervisor(config)
        supervisor.progress_api_wait_started = 0
        supervisor.retry_not_before = 2
        self.assertTrue(supervisor.observe_progress(1))
        self.assertEqual(supervisor.failures, 0)

        supervisor.retry_not_before = 0
        self.assertTrue(supervisor.observe_progress(1))
        self.assertEqual(supervisor.failures, 1)

    def test_restartpoint_progress_resets_lag_timer(self):
        config = MODULE.Config.load(self.write_config())

        class RestartedSupervisor(MODULE.Supervisor):
            def read_progress(self):
                return MODULE.Progress("0/4", "0/3", "0/1", 1)

        supervisor = RestartedSupervisor(config)
        supervisor.restart_baseline = 0
        supervisor.lag_started_at = 0
        supervisor.last_replay = 4
        supervisor.observe_progress(10)
        self.assertEqual(supervisor.restart_baseline, None)
        self.assertEqual(supervisor.lag_started_at, 10)


if __name__ == "__main__":
    unittest.main()
