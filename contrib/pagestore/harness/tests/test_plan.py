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
            "operations": ["crash"], "protocol_version": 26,
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
            "'{\"protocol_version\":26,\"page_size\":8192,\"io_unit\":262144,"
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

    def test_runtime_rejects_profile_operation_not_implemented_by_runner(self):
        capabilities = json.loads(json.dumps(CAPABILITIES))
        capabilities["runtimes"]["daemon_smoke"]["operations"].append("sql")
        path = self.write_plan([self.header()])
        with self.assertRaisesRegex(MODULE.PlanError, "do not match runner implementation"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "daemon_smoke"
            )

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
        with self.assertRaisesRegex(MODULE.PlanError, "constraints do not match"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "daemon_smoke"
            )

    def test_runtime_rejects_missing_code_owned_constraints(self):
        capabilities = json.loads(json.dumps(CAPABILITIES))
        capabilities["runtimes"]["daemon_smoke"]["constraints"] = {}
        path = self.write_plan([self.header()])
        with self.assertRaisesRegex(MODULE.PlanError, "constraints do not match"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "daemon_smoke"
            )

    def test_writer_runtime_rejects_unavailable_sql_target(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "sql", "id": "read", "target": "reader-R", "sql": "SELECT 1"},
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "0/1"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "target 'reader-R'.*not an available compute"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_accepts_sql_target_after_reader_install(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R"},
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "$R"},
            {"op": "sql", "id": "read", "target": "reader-R", "sql": "SELECT 1"},
        ])
        MODULE.validate_runtime_plan(
            MODULE.read_plan(path), capabilities, "writer_smoke"
        )

    def test_reader_sql_does_not_dirty_writer_capture(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint-1", "target": "writer", "name": "R1"},
            {"op": "capture", "id": "capture-1", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R1", "horizon": "$R1"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R1", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R1"},
            {"op": "install_reader", "id": "install", "target": "reader-R1",
             "prepared": "prepare", "read_lsn": "$R1"},
            {"op": "checkpoint", "id": "checkpoint-2", "target": "writer", "name": "R2"},
            {"op": "sql", "id": "reader-sql", "target": "reader-R1", "sql": "SELECT 1"},
            {"op": "capture", "id": "capture-2", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R2", "horizon": "$R2"},
        ])
        MODULE.validate_runtime_plan(
            MODULE.read_plan(path), capabilities, "writer_smoke"
        )

    def test_writer_assert_dirties_checkpoint_capture(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "assert", "id": "side-effect", "target": "writer",
             "oracle": "sql_scalar", "sql": "SELECT nextval('s')", "expect": "1"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "does not describe the current writer"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_runtime_requires_page_size_capability(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        del capabilities["runtimes"]["writer_smoke"]["page_size"]
        path = self.write_plan([self.header()])
        with self.assertRaisesRegex(MODULE.PlanError, "invalid page_size"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_materializer_runtime_accepts_managed_lifecycle(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer"]
        path = self.write_plan([
            header,
            {"op": "sql", "id": "write", "target": "writer",
             "sql": "SELECT 1"},
            {"op": "checkpoint", "id": "boundary", "target": "writer",
             "name": "R"},
            {"op": "crash", "id": "replace", "target": "materializer",
             "model": "compute"},
            {"op": "assert", "id": "healthy", "target": "materializer",
             "oracle": "sql_scalar", "sql": "SELECT 1", "expect": "1"},
        ])
        MODULE.validate_plan(MODULE.read_plan(path), capabilities)
        MODULE.validate_runtime_plan(
            MODULE.read_plan(path), capabilities, "materializer_smoke"
        )

    def test_materializer_runtime_requires_exact_topology(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([self.header()])
        with self.assertRaisesRegex(
            MODULE.PlanError, "requires exactly writer and materializer"
        ):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "materializer_smoke"
            )

    def test_materializer_runtime_rejects_duplicate_compute_roles(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer", "materializer"]
        path = self.write_plan([header])
        with self.assertRaisesRegex(
            MODULE.PlanError, "requires exactly writer and materializer"
        ):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "materializer_smoke"
            )

    def test_postgresql_conf_string_escapes_path_characters(self):
        self.assertEqual(
            MODULE.postgresql_conf_string("/tmp/alice's\\socket"),
            "'/tmp/alice''s\\\\socket'",
        )

    def test_writer_runtime_requires_reader_artifacts_before_install(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "0/1"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "requires prepared artifact 'prepare'"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_plan_rejects_non_string_target(self):
        path = self.write_plan([
            self.header(),
            {"op": "sql", "id": "read", "target": ["writer"], "sql": "SELECT 1"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "target must be a non-empty string"):
            MODULE.validate_plan(MODULE.read_plan(path), CAPABILITIES)

    def test_plan_rejects_non_string_artifact_identifier(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": ["prepare"], "read_lsn": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "prepared must be a non-empty string"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_plan_rejects_artifact_path_escape(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "../R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "name must be a safe path component"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_plan_validates_optional_sql_failure_expectations(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        invalid_actions = [
            {"op": "sql", "id": "error", "target": "writer", "sql": "SELECT 1",
             "expect_error": ["bad"]},
            {"op": "sql", "id": "state", "target": "writer", "sql": "SELECT 1",
             "expect_sqlstate": "bad"},
        ]
        for action in invalid_actions:
            with self.subTest(action=action["id"]):
                path = self.write_plan([self.header(), action])
                with self.assertRaises(MODULE.PlanError):
                    MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_writer_runtime_rejects_wrong_boundary_kind(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R", "name": "C"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$C"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "capture requires an earlier checkpoint horizon"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_requires_install_horizon_to_match_prepared_reader(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint-1", "target": "writer", "name": "R1"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R1"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R1", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R1"},
            {"op": "checkpoint", "id": "checkpoint-2", "target": "writer", "name": "R2"},
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "$R2"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "does not match prepared artifact horizon"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_requires_install_horizon_to_match_reader_seed(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint-1", "target": "writer", "name": "R1"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R1"},
            {"op": "checkpoint", "id": "checkpoint-2", "target": "writer", "name": "R2"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R2", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R2"},
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "$R2"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "does not match reader seed horizon"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_requires_bootstrap_before_reader_install(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R"},
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "bootstrap before checkpoint"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_requires_bootstrap_before_reader_checkpoint(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R"},
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "install_reader", "id": "install", "target": "reader-R",
             "prepared": "prepare", "read_lsn": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "bootstrap before checkpoint"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_control_restore_is_selected_from_postgres_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            build = Path(temporary)
            program = build / "contrib" / "pagestore" / "pagestore_control_restore"
            program.parent.mkdir(parents=True)
            program.write_text("#!/bin/sh\n", encoding="utf-8")
            program.chmod(0o755)
            self.assertEqual(
                MODULE.pagestore_build_program(build, "pagestore_control_restore"),
                program,
            )

    def test_missing_build_control_restore_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(MODULE.PlanError, "does not provide executable"):
                MODULE.pagestore_build_program(
                    Path(temporary), "pagestore_control_restore"
                )

    def test_importer_command_passes_runtime_page_size(self):
        command = MODULE.pagestore_import_command(
            Path("/build/pagestore_import"), "/harness", Path("/data"), 16384,
        )
        self.assertEqual(command[-2:], ["--page-size", "16384"])

    def test_writer_runtime_rejects_reader_base_newer_than_horizon(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint-1", "target": "writer", "name": "R1"},
            {"op": "checkpoint", "id": "checkpoint-2", "target": "writer", "name": "R2"},
            {"op": "reader_base", "id": "base", "target": "writer",
             "checkpoint": "$R2", "name": "C"},
            {"op": "prepare_reader", "id": "prepare", "target": "writer",
             "base": "$C", "read_lsn": "$R1"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "base '\\$C' is newer"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_rejects_capture_after_writer_mutation(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "sql", "id": "mutate", "target": "writer", "sql": "CREATE TABLE late()"},
            {"op": "capture", "id": "capture", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "does not describe the current writer"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_rejects_duplicate_reader_capture_name(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            self.header(),
            {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
            {"op": "checkpoint", "id": "checkpoint", "target": "writer", "name": "R"},
            {"op": "capture", "id": "capture-1", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
            {"op": "capture", "id": "capture-2", "target": "writer",
             "kind": "reader_datadir", "name": "reader-R", "horizon": "$R"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "capture name 'reader-R' is already used"):
            MODULE.validate_runtime_plan(
                MODULE.read_plan(path), capabilities, "writer_smoke"
            )

    def test_writer_runtime_requires_current_unmodified_reader_base(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        for mutation in (
            {"op": "sql", "id": "mutate", "target": "writer", "sql": "SELECT 1"},
            {"op": "checkpoint", "id": "new-checkpoint", "target": "writer", "name": "R2"},
        ):
            with self.subTest(operation=mutation["op"]):
                path = self.write_plan([
                    self.header(),
                    {"op": "bootstrap", "id": "bootstrap", "target": "writer"},
                    {"op": "checkpoint", "id": "checkpoint", "target": "writer",
                     "name": "R"},
                    mutation,
                    {"op": "reader_base", "id": "base", "target": "writer",
                     "checkpoint": "$R", "name": "C"},
                ])
                with self.assertRaisesRegex(
                    MODULE.PlanError, "does not describe the current unmodified writer"
                ):
                    MODULE.validate_runtime_plan(
                        MODULE.read_plan(path), capabilities, "writer_smoke"
                    )

    def test_inspection_schema_version_must_match_capabilities(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        schema = Path(directory.name) / "inspection_schema.json"
        schema.write_text('{"schema": 999}\n', encoding="utf-8")
        with self.assertRaisesRegex(MODULE.PlanError, "version does not match capabilities"):
            MODULE.read_inspection_schema(schema, MODULE.read_json(ROOT / "capabilities.json"))

    def test_inspection_operations_must_match_runner_before_launch(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        cases = []
        missing = json.loads(json.dumps(schema))
        missing["implemented_operations"].remove("backpressure")
        cases.append((capabilities, missing))
        extra_capability = json.loads(json.dumps(capabilities))
        extra_capability["inspection_operations"].append("timeline")
        extra_schema = json.loads(json.dumps(schema))
        extra_schema["implemented_operations"].append("timeline")
        cases.append((extra_capability, extra_schema))
        for number, (candidate_capabilities, candidate_schema) in enumerate(cases):
            with self.subTest(number=number), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "schema.json"
                path.write_text(json.dumps(candidate_schema), encoding="utf-8")
                with self.assertRaisesRegex(MODULE.PlanError, "do not match runner"):
                    MODULE.read_inspection_schema(path, candidate_capabilities)

    def test_inspection_response_definition_is_validated_before_launch(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        schema["operations"]["backpressure"]["response"] = []
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema.json"
            path.write_text(json.dumps(schema), encoding="utf-8")
            with self.assertRaisesRegex(MODULE.PlanError, "backpressure.*do not match runner"):
                MODULE.read_inspection_schema(path, capabilities)

    def test_inspection_response_fields_must_match_runner(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        schema["operations"]["health"]["response"] = ["protocol_version"]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema.json"
            path.write_text(json.dumps(schema), encoding="utf-8")
            with self.assertRaisesRegex(MODULE.PlanError, "health.*do not match runner"):
                MODULE.read_inspection_schema(path, capabilities)

    def test_runtime_rejects_page_size_larger_than_io_unit(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        capabilities["runtimes"]["daemon_smoke"]["page_size"] = 524288
        plan = MODULE.read_plan(self.write_plan([self.header()]))
        with self.assertRaisesRegex(MODULE.PlanError, "page_size exceeds io_unit"):
            MODULE.validate_runtime_plan(plan, capabilities, "daemon_smoke")

    def test_sql_scalar_assert_accepts_an_empty_expected_value(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        plan = MODULE.read_plan(self.write_plan([
            self.header(),
            {
                "op": "assert", "id": "empty", "target": "writer",
                "oracle": "sql_scalar", "sql": "SELECT NULL", "expect": "",
            },
        ]))
        MODULE.validate_plan(plan, capabilities)
        MODULE.validate_runtime_plan(plan, capabilities, "writer_smoke")

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
            "protocol_version": 26, "page_size": 8192, "io_unit": 262144,
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
            "  echo '{\"protocol_version\":26,\"page_size\":8192,\"io_unit\":262144,"
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

    def test_postgres_relation_segment_size_must_match_importer(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        build = Path(directory.name)
        config_header = build / "src" / "include" / "pg_config.h"
        config_header.parent.mkdir(parents=True)
        config_header.write_text("#define RELSEG_SIZE 65536\n", encoding="utf-8")
        with self.assertRaisesRegex(MODULE.PlanError, "65536 blocks.*131072 blocks"):
            MODULE.validate_postgres_relation_segment_size(build, 8192)

    def test_postgres_relation_segment_size_accepts_one_gibibyte(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        build = Path(directory.name)
        config_header = build / "src" / "include" / "pg_config.h"
        config_header.parent.mkdir(parents=True)
        config_header.write_text("#define RELSEG_SIZE 131072\n", encoding="utf-8")
        self.assertEqual(
            MODULE.validate_postgres_relation_segment_size(build, 8192), 131072
        )

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

    def test_relation_segment_geometry_is_required_only_for_bootstrap(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        for bootstrap, expected_calls in ((False, 0), (True, 1)):
            with self.subTest(bootstrap=bootstrap):
                records = [self.header()]
                if bootstrap:
                    records.append({
                        "op": "bootstrap", "id": "bootstrap", "target": "writer",
                    })
                plan = MODULE.read_plan(self.write_plan(records))
                with (
                    mock.patch.object(MODULE, "find_pg_bin", return_value=Path("/pg/bin")),
                    mock.patch.object(MODULE, "validate_postgres_runtime", return_value=19),
                    mock.patch.object(MODULE, "validate_postgres_block_size"),
                    mock.patch.object(
                        MODULE, "validate_postgres_relation_segment_size"
                    ) as relation_geometry,
                    mock.patch.object(
                        MODULE, "run_root", side_effect=MODULE.PlanError("stop before launch")
                    ) as create_root,
                ):
                    with self.assertRaisesRegex(MODULE.PlanError, "stop before launch"):
                        MODULE.run_writer_smoke(
                            plan, capabilities,
                            MODULE.read_json(ROOT / "inspection_schema.json"),
                            Path("daemon"), Path("inspect"), Path("build"), None, False,
                        )
                self.assertEqual(relation_geometry.call_count, expected_calls)
                create_root.assert_called_once()

    def test_small_page_reader_is_rejected_before_creating_run_root(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        capabilities["runtimes"]["writer_smoke"]["page_size"] = 4096
        plan = MODULE.read_plan(self.write_plan([
            self.header(),
            {
                "op": "install_reader", "id": "install", "target": "reader-R",
                "prepared": "prepare", "read_lsn": "0/1",
            },
        ]))
        with (
            mock.patch.object(MODULE, "find_pg_bin", return_value=Path("/pg/bin")),
            mock.patch.object(MODULE, "validate_postgres_runtime", return_value=19),
            mock.patch.object(MODULE, "validate_postgres_block_size", return_value=4096),
            mock.patch.object(MODULE, "run_root") as create_root,
            mock.patch.object(MODULE, "pagestore_build_program") as build_program,
        ):
            with self.assertRaisesRegex(MODULE.PlanError, "cannot hold.*control file"):
                MODULE.run_writer_smoke(
                    plan, capabilities,
                    MODULE.read_json(ROOT / "inspection_schema.json"),
                    Path("daemon"), Path("inspect"), Path("build"), None, False,
                )
        create_root.assert_not_called()
        build_program.assert_not_called()

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
            "'{\"protocol_version\":26,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1,"
            "\"wal_index_pending_bytes\":0,\"wal_index_lagging_timelines\":0}' ;;\n"
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
        self.assertIn("--page-size", events[1]["argv"])
        self.assertEqual(events[1]["argv"][events[1]["argv"].index("--page-size") + 1], "8192")
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
            "'{\"protocol_version\":26,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1,"
            "\"wal_index_pending_bytes\":0,\"wal_index_lagging_timelines\":0}' ;;\n"
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
            "'{\"protocol_version\":26,\"page_size\":8192,\"io_unit\":262144,"
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

    def test_materializer_smoke_requires_runtime_binaries_and_build_dir(self):
        prerequisites = [
            ("--build-dir", "build"),
            ("--daemon-binary", "daemon"),
            ("--inspect-binary", "inspect"),
            ("--materializer-supervisor", "supervisor"),
        ]
        base_args = [
            "--capabilities", str(ROOT / "capabilities.json"),
            "--materializer-smoke",
            str(ROOT / "scenarios" / "materializer_lifecycle.jsonl"),
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
                self.assertIn(
                    "--materializer-smoke requires --build-dir, "
                    "--daemon-binary, --inspect-binary and "
                    "--materializer-supervisor",
                    stderr.getvalue(),
                )

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
