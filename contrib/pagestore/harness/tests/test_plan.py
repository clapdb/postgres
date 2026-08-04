import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pagestore_harness", ROOT / "pagestore_harness.py")
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


CAPABILITIES = {
    "schema": 1,
    "storage": ["posix"],
    "shards": [1],
    "compute": ["writer", "reader"],
    "crash_models": ["power_loss"],
    "operations": ["sql", "checkpoint", "prepare_reader", "assert", "crash"],
    "inspection_operations": ["health", "backpressure"],
    "postgres_major": [13, 14, 15, 16, 17, 18, 19],
    "runtimes": {
        "daemon_smoke": {
            "operations": ["crash"], "protocol_version": 21,
            "page_size": 8192, "io_unit": 262144,
            "constraints": {
                "crash": {
                    "target": ["store"], "model": ["power_loss"],
                    "forbidden_fields": ["fault"],
                },
            },
        },
    },
}


class PlanValidationTests(unittest.TestCase):
    def write_plan(self, records):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "scenario.jsonl"
        path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")
        return path

    def header(self):
        return {
            "schema": 1,
            "scenario": "reader",
            "seed": 7,
            "contracts": ["snapshot_visibility"],
            "case": {"storage": "posix", "shards": 1, "compute": ["writer", "reader"]},
        }

    def test_accepts_declared_horizon(self):
        path = self.write_plan([
            self.header(),
            {"op": "checkpoint", "id": "r0", "target": "writer", "name": "R"},
            {"op": "prepare_reader", "id": "reader", "target": "writer", "base": "$R", "read_lsn": "$R"},
            {"op": "assert", "id": "visible", "target": "reader-R", "oracle": "sql_scalar", "sql": "SELECT 1", "expect": "$R"},
        ])
        MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_rejects_horizon_before_checkpoint(self):
        path = self.write_plan([
            self.header(),
            {"op": "prepare_reader", "id": "reader", "target": "writer", "base": "$R", "read_lsn": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "no completed boundary"):
            MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_rejects_unsupported_crash_model(self):
        path = self.write_plan([
            self.header(),
            {"op": "crash", "id": "crash", "target": "store", "model": "network_partition"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "unsupported crash model"):
            MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_rejects_unknown_action_field(self):
        path = self.write_plan([
            self.header(),
            {"op": "checkpoint", "id": "r0", "target": "writer", "name": "R", "typo": True},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "unknown field"):
            MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_accepts_expected_sql_failure(self):
        path = self.write_plan([
            self.header(),
            {"op": "sql", "id": "write-rejected", "target": "reader-R",
             "sql": "UPDATE t SET v = 1", "expect_error": "read-only"},
        ])
        MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_accepts_expected_sqlstate(self):
        path = self.write_plan([
            self.header(),
            {"op": "sql", "id": "write-rejected", "target": "reader-R",
             "sql": "UPDATE t SET v = 1", "expect_sqlstate": "25006"},
        ])
        MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_sqlstate_is_extracted_from_verbose_psql_error(self):
        output = "ERROR:  25006: cannot execute UPDATE in a read-only transaction\n"
        self.assertEqual(MODULE.sqlstate_from_output(output), "25006")

    def test_private_environment_removes_all_postgres_variables(self):
        with mock.patch.dict(MODULE.os.environ, {
            "PGDATABASE": "outside", "PGOPTIONS": "-c work_mem=1MB",
            "PGSERVICE": "external", "PAGESTORE_TEST_FAULT": "crash",
        }, clear=False):
            environment = MODULE.private_environment()
        self.assertNotIn("PGDATABASE", environment)
        self.assertNotIn("PGOPTIONS", environment)
        self.assertNotIn("PGSERVICE", environment)
        self.assertNotIn("PAGESTORE_TEST_FAULT", environment)

    def test_inspection_schema_is_read_only_and_versioned(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        self.assertEqual(schema["schema"], 1)
        self.assertEqual(schema["transport"], "private-test-ipc")
        self.assertEqual(schema["mutating_operations"], [])
        self.assertEqual(
            set(schema["operations"]),
            {"health", "timeline", "relation", "manifest", "gc", "backpressure"},
        )

    def test_inspector_response_must_match_schema(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        inspector = Path(directory.name) / "inspect"
        inspector.write_text(
            "#!/bin/sh\ncase \"$3\" in\n"
            "health) printf '%s\\n' "
            "'{\"protocol_version\":21,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1}' ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        inspector.chmod(0o755)
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        value = MODULE.inspect_store(inspector, "/unused", "health", schema)
        self.assertEqual(value["page_size"], 8192)

    def test_runtime_rejects_an_operation_the_runner_cannot_execute(self):
        path = self.write_plan([
            self.header(),
            {"op": "sql", "id": "write", "target": "writer", "sql": "SELECT 1"},
        ])
        plan = MODULE.read_plan(path)
        with self.assertRaisesRegex(MODULE.PlanError, "cannot execute operation.*sql"):
            MODULE.validate_runtime_plan(plan, CAPABILITIES, "daemon_smoke")

    def test_runtime_rejects_unsupported_operation_parameters(self):
        path = self.write_plan([
            self.header(),
            {"op": "crash", "id": "crash", "target": "writer", "model": "power_loss"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "does not support target='writer'"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), CAPABILITIES, "daemon_smoke"
            )

    def test_runtime_rejects_unsupported_optional_fields(self):
        path = self.write_plan([
            self.header(),
            {"op": "crash", "id": "crash", "target": "store", "model": "power_loss",
             "fault": "manifest.after_rename"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "does not support field.*fault"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), CAPABILITIES, "daemon_smoke"
            )

    def test_runtime_rejects_forbidden_field_values(self):
        capabilities = json.loads(json.dumps(CAPABILITIES))
        capabilities["runtimes"]["daemon_smoke"]["constraints"]["crash"] = {
            "forbidden_values": {"target": ["writer"]},
        }
        path = self.write_plan([
            self.header(),
            {"op": "crash", "id": "crash", "target": "writer", "model": "power_loss"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "forbids target='writer'"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "daemon_smoke"
            )

    def test_runtime_health_must_match_advertised_protocol(self):
        path = self.write_plan([self.header()])
        health = {
            "protocol_version": 20, "page_size": 8192, "io_unit": 262144,
            "nshards": 1,
        }
        schema = {"implemented_operations": ["health", "backpressure"]}
        with self.assertRaisesRegex(MODULE.PlanError, "protocol_version mismatch"):
            MODULE.validate_runtime_health(
                MODULE.read_plan(path), CAPABILITIES, "daemon_smoke", health, schema
            )

    def test_runtime_requires_advertised_inspection_operations(self):
        path = self.write_plan([self.header()])
        health = {
            "protocol_version": 21, "page_size": 8192, "io_unit": 262144,
            "nshards": 1,
        }
        schema = {"implemented_operations": ["health"]}
        with self.assertRaisesRegex(MODULE.PlanError, "lacks advertised.*backpressure"):
            MODULE.validate_runtime_health(
                MODULE.read_plan(path), CAPABILITIES, "daemon_smoke", health, schema
            )

    def test_runtime_probes_every_advertised_inspection_operation(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        inspector = Path(directory.name) / "inspect"
        inspector.write_text(
            "#!/bin/sh\n"
            "if [ \"$3\" = health ]; then\n"
            "  echo '{\"protocol_version\":21,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}'\n"
            "else\n"
            "  echo 'unsupported operation' >&2; exit 1\n"
            "fi\n",
            encoding="utf-8",
        )
        inspector.chmod(0o755)
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        with self.assertRaisesRegex(MODULE.PlanError, "inspector backpressure failed"):
            MODULE.probe_runtime_inspection(
                inspector, "/unused", CAPABILITIES, schema
            )

    def test_postgres_runtime_major_must_be_advertised(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        postgres = Path(directory.name) / "postgres"
        postgres.write_text("#!/bin/sh\necho 'postgres (PostgreSQL) 20devel'\n", encoding="utf-8")
        postgres.chmod(0o755)
        with self.assertRaisesRegex(MODULE.PlanError, "major 20.*not advertised"):
            MODULE.validate_postgres_runtime(postgres, CAPABILITIES)

    def test_postgres_runtime_accepts_supported_major(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        postgres = Path(directory.name) / "postgres"
        postgres.write_text("#!/bin/sh\necho 'postgres (PostgreSQL) 19beta1'\n", encoding="utf-8")
        postgres.chmod(0o755)
        self.assertEqual(MODULE.validate_postgres_runtime(postgres, CAPABILITIES), 19)

    def test_postgres_runtime_settings_respect_guc_version(self):
        self.assertNotIn("io_method", MODULE.postgres_runtime_settings(17))
        self.assertEqual(MODULE.postgres_runtime_settings(18), "io_method = sync\n")

    def test_postgres_block_size_must_match_runtime_page_size(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        build = Path(directory.name)
        config_header = build / "src" / "include" / "pg_config.h"
        config_header.parent.mkdir(parents=True)
        config_header.write_text("#define BLCKSZ 16384\n", encoding="utf-8")
        with self.assertRaisesRegex(MODULE.PlanError, "block size 16384.*page size 8192"):
            MODULE.validate_postgres_block_size(build, 8192)

    def test_postgres_block_size_reads_generated_build_configuration(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        build = Path(directory.name)
        config_header = build / "src" / "include" / "pg_config.h"
        config_header.parent.mkdir(parents=True)
        config_header.write_text("#define BLCKSZ 8192\n", encoding="utf-8")
        self.assertEqual(MODULE.validate_postgres_block_size(build, 8192), 8192)

    def test_postgres_preflight_runs_before_creating_the_run_root(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        build = Path(directory.name) / "build"
        pg_bin = build / "tmp_install" / "install" / "bin"
        pg_bin.mkdir(parents=True)
        postgres = pg_bin / "postgres"
        (pg_bin / "pg_ctl").touch()
        postgres.write_text("#!/bin/sh\necho 'not postgres'\n", encoding="utf-8")
        postgres.chmod(0o755)
        plan = MODULE.read_plan(self.write_plan([self.header()]))
        with mock.patch.object(MODULE, "run_root") as create_root:
            with self.assertRaisesRegex(MODULE.PlanError, "cannot identify PostgreSQL"):
                MODULE.run_writer_smoke(
                    plan, CAPABILITIES, {}, Path("daemon"), Path("inspect"),
                    build, None, False,
                )
        create_root.assert_not_called()

    def test_daemon_smoke_retains_a_replayable_run_root(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        daemon = base / "daemon"
        inspector = base / "inspect"
        daemon.write_text(
            "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            encoding="utf-8",
        )
        inspector.write_text(
            "#!/bin/sh\ncase \"$3\" in\n"
            "health) printf '%s\\n' "
            "'{\"protocol_version\":21,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1}' ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        daemon.chmod(0o755)
        inspector.chmod(0o755)
        plan = self.write_plan([self.header()])
        root = base / "run"
        with contextlib.redirect_stdout(io.StringIO()):
            status = MODULE.main([
                "--capabilities", str(ROOT / "capabilities.json"),
                "--daemon-smoke", str(plan),
                "--daemon-binary", str(daemon),
                "--inspect-binary", str(inspector),
                "--run-root", str(root),
                "--keep",
            ])
        self.assertEqual(status, 0)
        self.assertTrue((root / "plan.jsonl").is_file())
        self.assertTrue((root / "case.json").is_file())
        events = [json.loads(line) for line in (root / "trace" / "events.jsonl").read_text().splitlines()]
        self.assertEqual([event["event"] for event in events], [
            "run_start", "process_start", "ready", "capture", "run_pass", "process_stop",
        ])
        self.assertFalse((root / "failure.json").exists())

    def test_daemon_smoke_retains_failure_bundle(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        daemon = base / "daemon"
        inspector = base / "inspect"
        daemon.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        inspector.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        daemon.chmod(0o755)
        inspector.chmod(0o755)
        plan = self.write_plan([self.header()])
        root = base / "failure"
        with contextlib.redirect_stderr(io.StringIO()):
            status = MODULE.main([
                "--capabilities", str(ROOT / "capabilities.json"),
                "--daemon-smoke", str(plan),
                "--daemon-binary", str(daemon),
                "--inspect-binary", str(inspector),
                "--run-root", str(root),
            ])
        self.assertEqual(status, 1)
        self.assertTrue((root / "failure.json").is_file())
        events = [json.loads(line) for line in (root / "trace" / "events.jsonl").read_text().splitlines()]
        self.assertEqual(events[-2]["event"], "run_fail")
        self.assertEqual(events[-1]["event"], "process_stop")

    def test_daemon_smoke_recovers_from_power_loss(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        daemon = base / "daemon"
        inspector = base / "inspect"
        daemon.write_text(
            "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            encoding="utf-8",
        )
        inspector.write_text(
            "#!/bin/sh\ncase \"$3\" in\n"
            "health) printf '%s\\n' "
            "'{\"protocol_version\":21,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1}' ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        daemon.chmod(0o755)
        inspector.chmod(0o755)
        root = base / "run"
        with contextlib.redirect_stdout(io.StringIO()):
            status = MODULE.main([
                "--capabilities", str(ROOT / "capabilities.json"),
                "--daemon-smoke", str(ROOT / "scenarios" / "daemon_recovery.jsonl"),
                "--daemon-binary", str(daemon),
                "--inspect-binary", str(inspector),
                "--run-root", str(root), "--keep",
            ])
        self.assertEqual(status, 0)
        events = [json.loads(line) for line in (root / "trace" / "events.jsonl").read_text().splitlines()]
        self.assertEqual([event["event"] for event in events], [
            "run_start", "process_start", "ready", "capture", "crash", "process_start",
            "ready", "recovered", "run_pass", "process_stop",
        ])
        self.assertEqual(events[4]["returncode"], -9)
        self.assertNotEqual(events[1]["argv"][2], events[5]["argv"][2])

    def test_daemon_smoke_rejects_named_fault_before_launch(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        daemon = base / "daemon"
        inspector = base / "inspect"
        daemon.write_text(
            "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            encoding="utf-8",
        )
        inspector.write_text(
            "#!/bin/sh\ncase \"$3\" in\n"
            "health) printf '%s\\n' "
            "'{\"protocol_version\":21,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1}' ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        daemon.chmod(0o755)
        inspector.chmod(0o755)
        plan = self.write_plan([
            {"schema": 1, "scenario": "fault", "seed": 1, "contracts": ["lifecycle"],
             "case": {"storage": "posix", "shards": 1, "compute": ["writer"]}},
            {"op": "crash", "id": "fault", "target": "store", "model": "power_loss",
             "fault": "manifest.after_rename"},
        ])
        root = base / "run"
        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            status = MODULE.main([
                "--capabilities", str(ROOT / "capabilities.json"),
                "--daemon-smoke", str(plan), "--daemon-binary", str(daemon),
                "--inspect-binary", str(inspector), "--run-root", str(root),
            ])
        self.assertEqual(status, 1)
        self.assertIn("does not support field(s): fault", stderr.getvalue())
        self.assertFalse(root.exists())

    def test_daemon_signal_targets_its_process_group(self):
        process = mock.Mock(pid=4321)
        with mock.patch.object(MODULE.os, "killpg") as killpg:
            MODULE.signal_process_group(process, MODULE.signal.SIGKILL)
        killpg.assert_called_once_with(4321, MODULE.signal.SIGKILL)

    def test_writer_smoke_requires_runtime_binaries_and_build_dir(self):
        prerequisites = [
            ("--build-dir", "build"),
            ("--daemon-binary", "daemon"),
            ("--inspect-binary", "inspect"),
        ]
        base_args = [
            "--capabilities", str(ROOT / "capabilities.json"),
            "--writer-smoke", str(ROOT / "scenarios" / "writer_lifecycle.jsonl"),
        ]

        for missing, _ in prerequisites:
            with self.subTest(missing=missing):
                args = list(base_args)
                for option, value in prerequisites:
                    if option != missing:
                        args.extend([option, value])

                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as raised:
                        MODULE.parse_args(args)

                self.assertNotEqual(raised.exception.code, 0)
                self.assertIn("--writer-smoke requires --build-dir, --daemon-binary and --inspect-binary",
                              stderr.getvalue())

    def test_legacy_integration_is_captured_in_a_bundle(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        script = base / "integration"
        script.write_text("#!/bin/sh\necho reader-coverage\n", encoding="utf-8")
        script.chmod(0o755)
        root = base / "run"
        with contextlib.redirect_stdout(io.StringIO()):
            status = MODULE.main([
                "--capabilities", str(ROOT / "capabilities.json"),
                "--legacy-integration", "--build-dir", str(base),
                "--integration-script", str(script), "--run-root", str(root),
            ])
        self.assertEqual(status, 0)
        self.assertEqual((root / "trace" / "integration.log").read_text(), "reader-coverage\n")


if __name__ == "__main__":
    unittest.main()
