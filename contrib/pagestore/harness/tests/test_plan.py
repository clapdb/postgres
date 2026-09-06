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
    "inspection_schema": 4,
    "storage": ["posix"],
    "shards": [1],
    "compute": ["writer", "reader"],
    "crash_models": ["power_loss"],
    "operations": ["sql", "checkpoint", "prepare_reader", "assert", "crash"],
    "inspection_operations": [
        "health", "timeline", "relation", "manifest", "gc", "owners", "backpressure", "pruning",
    ],
    "postgres_major": [13, 14, 15, 16, 17, 18, 19],
    "runtimes": {
        "daemon_smoke": {
            "operations": ["crash"], "protocol_version": 45,
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

    def test_fault_failure_classifications_distinguish_timeout_and_unreached(self):
        self.assertEqual(
            MODULE.fault_failure_classification(MODULE.HarnessTimeout("alive")),
            "timeout",
        )
        self.assertEqual(
            MODULE.fault_failure_classification(MODULE.FaultNotReached("report")),
            "fault_not_reached",
        )
        self.assertEqual(
            MODULE.fault_failure_classification(MODULE.UnexpectedExit("status")),
            "unexpected_exit",
        )

    def test_materializer_status_parser_preserves_actual_values_and_nulls(self):
        self.assertEqual(
            MODULE.parse_materializer_status_row("(0/04000000,0/03045670,4096)"),
            {
                "shipped_wal_lsn": "0/04000000",
                "materialized_wal_lsn": "0/03045670",
                "lag_bytes": "4096",
            },
        )
        self.assertEqual(
            MODULE.parse_materializer_status_row("(0/04000000,,)"),
            {
                "shipped_wal_lsn": "0/04000000",
                "materialized_wal_lsn": None,
                "lag_bytes": None,
            },
        )

    def test_materializer_crash_evidence_precedes_recovery(self):
        source = (ROOT / "pagestore_harness.py").read_text(encoding="utf-8")
        self.assertLess(
            source.index('"post_crash_durable_state"'),
            source.index('"materializer-supervisor-recovery.log"'),
        )
        self.assertLess(
            source.index("post_crash_status = read_materializer_status()"),
            source.index("recovered_status = read_materializer_status()"),
        )
        self.assertLess(
            source.index("marker_at_report = sql_scalar"),
            source.index("crash_materializer(action[\"id\"])", source.index("marker_at_report = sql_scalar")),
        )
        self.assertNotIn('"action_skip"', source)
        self.assertIn(
            '"post_recovery" if materializer_recovered else "pre_crash"',
            source,
        )
        self.assertLess(
            source.index("recovered materializer did not replay the fault checkpoint"),
            source.index("materializer_recovered = True"),
        )

    def test_materializer_fault_arms_only_after_replay_and_marker_observation(self):
        source = (ROOT / "pagestore_harness.py").read_text(encoding="utf-8")
        start = source.index('elif action["op"] == "materializer_fault":')
        wait = source.index("materializer did not replay fault checkpoint", start)
        marker = source.index("marker_before_trigger =", start)
        arm = source.index('_atomic_arm_marker(fault_control / "arm")', start)
        trigger = source.index("fault_trigger_proc = subprocess.Popen", start)
        self.assertLess(wait, arm)
        self.assertLess(marker, arm)
        self.assertLess(arm, trigger)

    def test_relation_observation_comparison_is_per_relation(self):
        observations = {}
        for name, key, blocks in (
            ("relation_a", (1, 1, 1), 10),
            ("relation_b", (1, 1, 2), 2),
        ):
            MODULE.record_relation_observation(
                observations, name, "$R1",
                {"relation_key": key, "main_nblocks": blocks}, "$R9",
            )
        self.assertTrue(
            MODULE.record_relation_observation(
                observations, "relation_a", "$R9",
                {"relation_key": (1, 1, 1), "main_nblocks": 11}, "$R9",
            )
        )
        self.assertTrue(
            MODULE.record_relation_observation(
                observations, "relation_b", "$R9",
                {"relation_key": (1, 1, 2), "main_nblocks": 3}, "$R9",
            )
        )
        self.assertEqual(len(observations), 4)

    def test_materializer_status_and_fault_errors_have_specific_classes(self):
        self.assertEqual(
            MODULE.fault_failure_classification(MODULE.OracleMismatch("oracle")),
            "oracle_mismatch",
        )

    def test_materializer_fault_requires_prior_r1_checkpoint(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer"]
        cases = [
            [
                {"op": "materializer_fault", "id": "fault", "target": "materializer",
                 "fault": "materializer.after_marker_sync", "action": "pause",
                 "hit": 1, "timeout": 30, "name": "R2"},
            ],
            [
                {"op": "materializer_fault", "id": "fault", "target": "materializer",
                 "fault": "materializer.after_marker_sync", "action": "pause",
                 "hit": 1, "timeout": 30, "name": "R2"},
                {"op": "checkpoint", "id": "late-r1", "target": "writer", "name": "R1"},
            ],
            [
                {"op": "checkpoint", "id": "wrong", "target": "writer", "name": "OLD"},
                {"op": "materializer_fault", "id": "fault", "target": "materializer",
                 "fault": "materializer.after_marker_sync", "action": "pause",
                 "hit": 1, "timeout": 30, "name": "R2"},
            ],
        ]
        for actions in cases:
            path = self.write_plan([header, *actions])
            plan = MODULE.read_plan(path)
            MODULE.validate_plan(plan, capabilities)
            with self.assertRaisesRegex(MODULE.PlanError, "prior checkpoint named R1"):
                MODULE.validate_runtime_plan(plan, capabilities, "materializer_smoke")

    def test_non_pause_fault_timeout_keeps_old_positive_semantics(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            {
                "schema": 1, "scenario": "daemon-fault-error", "seed": 1,
                "contracts": ["fault_reachability"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {"op": "set_fault", "id": "fault", "target": "store",
             "fault": "daemon.after_ready", "action": "error", "hit": 1,
             "timeout": 301},
        ])
        MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_relation_metadata_sql_quotes_complete_predicate_once(self):
        sql = MODULE.relation_metadata_sql("relation'with-quote")
        self.assertEqual(
            sql,
            "SELECT COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::text "
            "|| '|' || d.oid::text || '|' || pg_relation_filenode(c.oid)::text "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_database d ON d.datname = current_database() "
            "WHERE n.nspname = 'public' AND c.relname = 'relation''with-quote'",
        )
        self.assertEqual(sql.count("AND c.relname ="), 1)
        self.assertNotIn("c.relname = AND", sql)

    def test_fault_evidence_is_preserved_before_safe_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            control = root / "control"
            trace = root / "trace"
            control.mkdir()
            trace.mkdir()
            (control / "report.jsonl").write_text("{partial\n", encoding="utf-8")
            target = root / "target"
            target.write_text("do-not-follow", encoding="utf-8")
            (control / "report-link").symlink_to(target)
            MODULE._preserve_fault_evidence(control, trace)
            evidence = trace / "materializer-fault-control"
            self.assertEqual((evidence / "report.jsonl").read_text(), "{partial\n")
            self.assertEqual((evidence / "report-link.symlink").read_text(), str(target))
            MODULE._cleanup_fault_control(control)
            self.assertFalse(control.exists())
            self.assertEqual((evidence / "report.jsonl").read_text(), "{partial\n")

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

    def test_seed_must_fit_fault_report_integer_contract(self):
        for seed in (0, 2**63 - 1):
            records = [self.header()]
            records[0]["seed"] = seed
            MODULE.validate_plan(MODULE.read_plan(self.write_plan(records)), CAPABILITIES)
        records = [self.header()]
        records[0]["seed"] = 2**63
        with self.assertRaisesRegex(MODULE.PlanError, "bounded non-negative integer"):
            MODULE.validate_plan(MODULE.read_plan(self.write_plan(records)), CAPABILITIES)

    def test_named_fault_catalog_validates_target_model_action_and_hit(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            {
                "schema": 1, "scenario": "daemon-fault-recovery", "seed": 1,
                "contracts": ["fault_reachability"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {
                "op": "crash", "id": "fault", "target": "store",
                "model": "process_abort", "fault": "daemon.after_ready",
                "action": "crash", "hit": 1,
            },
        ])
        plan = MODULE.read_plan(path)
        MODULE.validate_plan(plan, capabilities)
        MODULE.validate_runtime_plan(plan, capabilities, "daemon_fault_smoke")

    def test_named_fault_catalog_rejects_wrong_hit_without_launch(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            {
                "schema": 1, "scenario": "daemon-fault-recovery", "seed": 1,
                "contracts": ["fault_reachability"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {
                "op": "crash", "id": "fault", "target": "store",
                "model": "process_abort", "fault": "daemon.after_ready",
                "action": "crash", "hit": 2,
            },
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "requires hit=1"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_named_fault_rejects_nonfinite_timeout(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            {
                "schema": 1, "scenario": "daemon-fault-recovery", "seed": 1,
                "contracts": ["fault_reachability"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {
                "op": "crash", "id": "fault", "target": "store",
                "model": "process_abort", "fault": "daemon.after_ready",
                "action": "crash", "hit": 1, "timeout": float("inf"),
            },
        ])
        path.write_text(
            path.read_text(encoding="utf-8").replace("Infinity", "1e309"),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(MODULE.PlanError, "finite and positive"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_set_fault_accepts_a_bounded_timeout(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            {
                "schema": 1, "scenario": "daemon-fault-error", "seed": 1,
                "contracts": ["fault_reachability"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {
                "op": "set_fault", "id": "fault", "target": "store",
                "fault": "daemon.after_ready", "action": "error", "hit": 1,
                "timeout": 2.5,
            },
        ])
        MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_pause_timeout_must_fit_registry_watchdog(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        for timeout in (0.0009, 300.001):
            path = self.write_plan([
                {
                    "schema": 1, "scenario": "daemon-fault-pause", "seed": 1,
                    "contracts": ["fault_reachability"],
                    "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
                },
                {
                    "op": "set_fault", "id": "fault", "target": "store",
                    "fault": "daemon.after_ready", "action": "pause", "hit": 1,
                    "timeout": timeout,
                },
                {
                    "op": "release_fault", "id": "release", "target": "store",
                    "fault": "daemon.after_ready",
                },
            ])
            with self.assertRaisesRegex(MODULE.PlanError, "1..300000 milliseconds"):
                MODULE.validate_plan(MODULE.read_plan(path), capabilities)

        crash_path = self.write_plan([
            {
                "schema": 1, "scenario": "daemon-fault-pause", "seed": 1,
                "contracts": ["fault_reachability"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {
                "op": "crash", "id": "fault", "target": "store",
                "model": "process_abort", "fault": "daemon.after_ready",
                "action": "pause", "hit": 1, "timeout": 301,
            },
            {
                "op": "release_fault", "id": "release", "target": "store",
                "fault": "daemon.after_ready",
            },
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "1..300000 milliseconds"):
            MODULE.validate_plan(MODULE.read_plan(crash_path), capabilities)

    def test_named_fault_rejects_unencodable_scenario_identity(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        for scenario in ('bad"scenario', "bad\\scenario", "x" * 129):
            path = self.write_plan([
                {
                    "schema": 1, "scenario": scenario, "seed": 1,
                    "contracts": ["fault_reachability"],
                    "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
                },
                {
                    "op": "set_fault", "id": "fault", "target": "store",
                    "fault": "daemon.after_ready", "action": "error", "hit": 1,
                },
            ])
            with self.assertRaisesRegex(MODULE.PlanError, "fault identity"):
                MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_expected_error_exit_accepts_orderly_status_one(self):
        self.assertTrue(MODULE.expected_error_exit("error", 1))
        self.assertFalse(MODULE.expected_error_exit("error", -11))
        self.assertFalse(MODULE.expected_error_exit("pause", 1))

    def test_fault_marker_is_created_atomically(self):
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "arm"
            MODULE._atomic_arm_marker(marker)
            self.assertEqual(marker.read_text(encoding="utf-8"), "armed\n")
            with self.assertRaises(FileExistsError):
                MODULE._atomic_arm_marker(marker)

    def test_fault_report_requires_name_and_hit(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.json"
            report.write_text(json.dumps({
                "schema": 1, "name": "daemon.after_ready", "action": "crash",
                "hit": 1, "pid": 123,
            }), encoding="utf-8")
            self.assertEqual(
                MODULE._fault_report(report, "daemon.after_ready", 1, 123)["hit"], 1
            )
            report.write_text(json.dumps({"name": "wrong", "hit": 1}), encoding="utf-8")
            with self.assertRaises(MODULE.FaultNotReached):
                MODULE._fault_report(report, "daemon.after_ready", 1, 123)

    def test_catalog_action_list_and_legacy_single_action(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            catalog = root / "faults.def"
            catalog.write_text(
                'PAGESTORE_FAULT_POINT(TEST, "test.fault", "store", '
                '"process_abort", "crash|error|pause", 1, 1)\n'
                'PAGESTORE_FAULT_POINT(OLD, "test.old", "store", '
                '"process_abort", "crash", 1, 1)\n', encoding="utf-8",
            )
            capabilities = MODULE.read_json(ROOT / "capabilities.json")
            capabilities["fault_catalog"] = catalog.name
            entries = MODULE.fault_catalog(capabilities, root / "capabilities.json")
            self.assertEqual(entries["test.fault"]["actions"], ("crash", "error", "pause"))
            self.assertEqual(entries["test.old"]["actions"], ("crash",))

    def test_error_report_requires_reach_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.json"
            report.write_text(json.dumps({
                "schema": 1, "name": "test.fault", "action": "error",
                "hit": 1, "pid": 123,
            }), encoding="utf-8")
            with self.assertRaises(MODULE.FaultNotReached):
                MODULE._fault_report(report, "test.fault", 1, 123, "error")
            report.write_text(json.dumps({
                "schema": 1, "scenario": "s", "seed": 9, "fault": "test.fault",
                "action": "error", "hit": 1, "pid": 123, "operation": "op",
            }), encoding="utf-8")
            self.assertEqual(
                MODULE._fault_report(report, "test.fault", 1, 123, "error", "s", 9, "op")["hit"],
                1,
            )

    def test_pause_release_marker_is_safe_regular_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "release"
            MODULE._atomic_release_marker(marker)
            self.assertTrue(marker.is_file())
            self.assertFalse(marker.is_symlink())
            with self.assertRaises(FileExistsError):
                MODULE._atomic_release_marker(marker)

    def test_pause_timeout_report_is_structured(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.jsonl"
            report.write_text(json.dumps({
                "schema": 1, "name": "test.fault", "action": "pause",
                "scenario": "s", "seed": 9, "hit": 1, "pid": 123,
                "operation": "op", "state": "timeout", "watchdog_ms": 5000,
            }) + "\n", encoding="utf-8")
            value = MODULE._fault_report(
                report, "test.fault", 1, 123, "pause", "s", 9, "op", "timeout",
            )
            self.assertEqual(value["watchdog_ms"], 5000)

    def test_pause_timeout_bundle_contains_actionable_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            control = root / "control"
            control.mkdir()
            diagnostic = MODULE._capture_fault_diagnostics(
                root, control, root / "daemon.log", reason="watchdog",
                scenario="s", seed=9, fault="test.fault", action="pause", hit=2,
                operation="op", process=None,
            )
            value = json.loads(diagnostic.read_text(encoding="utf-8"))
            self.assertEqual(value["fault"], "test.fault")
            self.assertEqual(value["operation_id"], "op")
            self.assertEqual(value["hit_count"], 2)

    def test_fault_runtime_arms_before_popen_and_restarts_twice(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        plan = MODULE.read_plan(ROOT / "scenarios" / "daemon_fault_recovery.jsonl")
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        previous_cwd = MODULE.os.getcwd()
        MODULE.os.chdir(directory.name)
        self.addCleanup(MODULE.os.chdir, previous_cwd)
        root = Path("run")
        health = {
            "protocol_version": 45, "page_size": 8192, "io_unit": 262144,
            "nchannels": 128, "nshards": 1, "admission_fence_epoch": 0,
            "admission_pending_epoch": 0, "admission_pending_lsn": 0,
        }
        calls = []
        inspection_probes = []

        class FakeProcess:
            next_pid = 700

            def __init__(self, env):
                self.pid = FakeProcess.next_pid
                FakeProcess.next_pid += 1
                self.returncode = 88 if "PAGESTORE_TEST_FAULT_NAME" in env else None
                self.env = env
                if self.returncode == 88:
                    (Path(env["PAGESTORE_TEST_FAULT_DIR"]) / "report.jsonl").write_text(
                        json.dumps({"schema": 1, "name": "daemon.after_ready",
                                    "action": "crash", "hit": 1, "pid": self.pid}) + "\n",
                        encoding="utf-8",
                    )

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                if self.returncode is None:
                    self.returncode = 0
                return self.returncode

        def fake_popen(command, **kwargs):
            env = kwargs["env"]
            control = Path(env["PAGESTORE_TEST_FAULT_DIR"]) \
                if "PAGESTORE_TEST_FAULT_DIR" in env else None
            calls.append((
                "popen", "PAGESTORE_TEST_FAULT_NAME" in env,
                (control / "arm").exists() if control is not None else False,
                command, env,
            ))
            return FakeProcess(env)

        with (
            mock.patch.object(MODULE.subprocess, "Popen", side_effect=fake_popen),
            mock.patch.object(MODULE, "inspect_store", return_value=health),
            mock.patch.object(MODULE, "validate_runtime_health"),
            mock.patch.object(
                MODULE, "probe_runtime_inspection",
                side_effect=lambda *args: inspection_probes.append(args),
            ),
            mock.patch.object(MODULE, "signal_process_group"),
            mock.patch.object(MODULE, "remove_shm"),
        ):
            MODULE.run_daemon_fault_recovery(
                plan, capabilities, {}, Path("daemon"), Path("inspect"), root, True,
                ROOT / "capabilities.json",
            )
        self.assertEqual([call[1] for call in calls], [True, False, False])
        self.assertEqual(len(inspection_probes), 2)
        self.assertEqual(
            [probe[0] for probe in inspection_probes],
            [Path("inspect").resolve()] * 2,
        )
        self.assertEqual([probe[2] for probe in inspection_probes], [capabilities] * 2)
        self.assertNotEqual(inspection_probes[0][1], inspection_probes[1][1])
        self.assertTrue(calls[0][2])
        self.assertTrue(Path(calls[0][3][0]).is_absolute())
        self.assertTrue(
            Path(calls[0][3][calls[0][3].index("--store") + 1]).is_absolute()
        )
        self.assertTrue(Path(calls[0][4]["PAGESTORE_TEST_FAULT_DIR"]).is_absolute())
        events = [
            json.loads(line)
            for line in (root / "trace" / "events.jsonl").read_text().splitlines()
        ]
        self.assertTrue(all({"scenario", "seed", "action_id", "generation"} <= set(event)
                            for event in events))
        self.assertEqual(
            [event["event"] for event in events].count("ready"), 0
        )
        self.assertEqual([event["event"] for event in events].count("recovered"), 1)
        self.assertEqual([event["event"] for event in events].count("restarted"), 1)
        crash_exit = next(event for event in events if event["event"] == "crash_exit")
        self.assertEqual(
            {crash_exit[field] for field in ("name", "action", "hit", "returncode")},
            {"daemon.after_ready", "crash", 1, 88},
        )
        first_stop = next(
            event for event in events
            if event["event"] == "process_stop" and event["generation"] == 0
        )
        self.assertEqual(first_stop["returncode"], 88)
        self.assertFalse((root / "fault-control").exists())

    def test_fault_failure_bundle_contains_inspection_schema_for_rerun(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        inspection_schema = MODULE.read_json(ROOT / "inspection_schema.json")
        plan = MODULE.read_plan(ROOT / "scenarios" / "daemon_fault_recovery.jsonl")
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name) / "failure"

        class FakeProcess:
            pid = 701
            returncode = 88

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                return self.returncode

        def fake_popen(command, **kwargs):
            control = Path(kwargs["env"]["PAGESTORE_TEST_FAULT_DIR"])
            (control / "report.jsonl").write_text("{malformed\n", encoding="utf-8")
            return FakeProcess()

        with (
            mock.patch.object(MODULE.subprocess, "Popen", side_effect=fake_popen),
            mock.patch.object(MODULE, "remove_shm"),
        ):
            with self.assertRaisesRegex(MODULE.PlanError, "failure bundle"):
                MODULE.run_daemon_fault_recovery(
                    plan, capabilities, inspection_schema,
                    Path("daemon"), Path("inspect"), root, True,
                    ROOT / "capabilities.json",
                )

        metadata = json.loads((root / "failure.json").read_text(encoding="utf-8"))
        bundled_schema = root / "inspection_schema.json"
        self.assertEqual(
            json.loads(bundled_schema.read_text(encoding="utf-8")), inspection_schema
        )
        schema_index = metadata["command"].index("--inspection-schema")
        self.assertEqual(metadata["command"][schema_index + 1], str(bundled_schema))
        self.assertEqual(metadata["inspection_schema"], str(bundled_schema))
        self.assertEqual(
            (root / "fault-control" / "report.jsonl").read_text(encoding="utf-8"),
            "{malformed\n",
        )
        self.assertFalse((root / "fault-control" / "arm").exists())

    def test_fault_report_rejects_extra_fields_and_multiple_lines(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.jsonl"
            report.write_text(
                "{\"schema\":1,\"name\":\"daemon.after_ready\","
                "\"action\":\"crash\",\"hit\":1,\"pid\":123,\"extra\":true}\n",
                encoding="utf-8",
            )
            with self.assertRaises(MODULE.FaultNotReached):
                MODULE._fault_report(report, "daemon.after_ready", 1, 123)

            report.write_bytes(b"\xff\xfe\xfd\n")
            with self.assertRaises(MODULE.FaultNotReached):
                MODULE._fault_report(report, "daemon.after_ready", 1, 123)
            line = (
                "{\"schema\":1,\"name\":\"daemon.after_ready\","
                "\"action\":\"crash\",\"hit\":1,\"pid\":123}\n"
            )
            report.write_text(line + line, encoding="utf-8")
            with self.assertRaises(MODULE.FaultNotReached):
                MODULE._fault_report(report, "daemon.after_ready", 1, 123)

    def test_fault_control_cleanup_preserves_file_and_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            regular = root / "control-file"
            regular.write_text("diagnostic", encoding="utf-8")
            MODULE._cleanup_fault_control(regular)
            self.assertEqual(regular.read_text(encoding="utf-8"), "diagnostic")
            target = root / "target"
            target.write_text("diagnostic", encoding="utf-8")
            link = root / "control-link"
            link.symlink_to(target)
            MODULE._cleanup_fault_control(link)
            self.assertTrue(link.is_symlink())
            self.assertEqual(target.read_text(encoding="utf-8"), "diagnostic")

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
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        self.assertEqual(schema["schema"], 4)
        self.assertEqual(capabilities["inspection_schema"], 4)
        self.assertEqual(schema["transport"], "private-test-ipc")
        self.assertEqual(schema["mutating_operations"], [])
        self.assertEqual(
            set(schema["operations"]),
            {"health", "timeline", "manifest", "gc", "owners", "backpressure",
             "pruning", "relation"},
        )
        self.assertIn("relation", schema["implemented_operations"])
        self.assertIn("relation", capabilities["inspection_operations"])

    def test_inspection_schema_advertises_exact_response_fields(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        for operation, fields in {
            "timeline": [
                "parent_timeline", "fork_lsn", "retained_horizon",
            ],
            "manifest": [
                "layer_count", "deleting_layers", "local_layers",
                "remote_durable_layers", "manifest_poisoned",
            ],
            "gc": [
                "page_debt_segments", "page_debt_unavailable", "deleting_layers",
                "remote_cleanup_pending",
                "forkmeta_pending", "forkmeta_poisoned",
            ],
            "owners": [
                "owner_count", "page_history_owners", "wal_owners",
                "wal_index_owners", "max_generation", "retention_poisoned",
            ],
            "relation": ["exists", "forks", "selected_version"],
        }.items():
            self.assertEqual(schema["operations"][operation]["response"], fields)

    def test_relation_inspection_validates_nested_read_only_response(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        result = mock.Mock(
            returncode=0,
            stdout=json.dumps({
                "exists": True,
                "forks": [{"fork": 0, "nblocks": 3}],
                "selected_version": None,
            }),
            stderr="",
        )
        with mock.patch.object(MODULE.subprocess, "run", return_value=result) as run:
            value = MODULE.inspect_store(
                Path("/inspect"), "/unused", "relation", schema,
                timeline=7, incarnation=9, relation_key=(1663, 1, 42), lsn=123,
            )
        self.assertTrue(value["exists"])
        self.assertEqual(
            run.call_args.args[0],
            ["/inspect", "--shm", "/unused", "relation", "7", "9", "1663", "1", "42", "123"],
        )

    def test_relation_inspection_requires_positive_incarnation(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        for incarnation in (None, 0, -1, True):
            with self.subTest(incarnation=incarnation), self.assertRaisesRegex(
                MODULE.PlanError, "positive incarnation"
            ):
                MODULE.inspect_store(
                    Path("/inspect"), "/unused", "relation", schema,
                    timeline=7, incarnation=incarnation,
                    relation_key=(1663, 1, 42), lsn=123,
                )

    def test_relation_inspection_validates_native_fork_invariants(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        valid = {
            "exists": True,
            "forks": [{"fork": 0, "nblocks": 3}, {"fork": 2, "nblocks": 1}],
            "selected_version": None,
        }
        invalid_cases = [
            ("too many forks", {"fork": 3, "nblocks": 1}),
            ("fork number out of range", [{"fork": 0, "nblocks": 1}, {"fork": 4, "nblocks": 1}]),
            ("out of order forks", [{"fork": 1, "nblocks": 1}, {"fork": 0, "nblocks": 1}]),
            ("duplicate forks", [{"fork": 0, "nblocks": 1}, {"fork": 0, "nblocks": 2}]),
            ("exists without main fork", {"exists": True, "forks": []}),
            ("main fork without exists", {"exists": False, "forks": [{"fork": 0, "nblocks": 1}]}),
        ]
        for label, change in invalid_cases:
            with self.subTest(label=label):
                response = dict(valid)
                if label == "too many forks":
                    response["forks"] = valid["forks"] + [change, change, change]
                elif label in {"fork number out of range", "out of order forks", "duplicate forks"}:
                    response["forks"] = change
                else:
                    response.update(change)
                result = mock.Mock(
                    returncode=0, stdout=json.dumps(response), stderr=""
                )
                with (
                    mock.patch.object(MODULE.subprocess, "run", return_value=result),
                    self.assertRaisesRegex(MODULE.PlanError, "fork|exists"),
                ):
                    MODULE.inspect_store(
                        Path("/inspect"), "/unused", "relation", schema,
                        timeline=7, incarnation=9,
                        relation_key=(1663, 1, 42), lsn=123,
                    )

    def test_inspector_response_must_match_schema(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        inspector = Path(directory.name) / "inspect"
        inspector.write_text(
            "#!/bin/sh\ncase \"$3\" in\n"
            "health) printf '%s\\n' "
            "'{\"protocol_version\":45,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "*) exit 1 ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        inspector.chmod(0o755)
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        value = MODULE.inspect_store(inspector, "/unused", "health", schema)
        self.assertEqual(value["page_size"], 8192)

    def test_timeline_inspection_passes_a_validated_id(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        result = mock.Mock(
            returncode=0,
            stdout=json.dumps({
                "parent_timeline": 0,
                "fork_lsn": 0,
                "retained_horizon": 0,
            }),
            stderr="",
        )
        with mock.patch.object(MODULE.subprocess, "run", return_value=result) as run:
            value = MODULE.inspect_store(
                Path("/inspect"), "/unused", "timeline", schema, timeline=7
            )
        self.assertEqual(value["parent_timeline"], 0)
        self.assertEqual(run.call_args.args[0], ["/inspect", "--shm", "/unused", "timeline", "7"])

    def test_inspection_request_arguments_are_rejected_for_other_operations(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        for operation in ("health", "gc", "owners"):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                MODULE.PlanError, "does not accept request arguments"
            ):
                MODULE.inspect_store(
                    Path("/inspect"), "/unused", operation, schema, timeline=7
                )

    def test_relation_only_cli_flags_are_rejected_for_other_inspections(self):
        base = [
            "--capabilities", str(ROOT / "capabilities.json"),
            "--inspect", "health", "--inspect-binary", "/inspect", "--shm", "/unused",
        ]
        for flag in ("--incarnation", "--spc-oid", "--db-oid", "--rel-number", "--lsn"):
            with self.subTest(flag=flag):
                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as raised:
                        MODULE.parse_args([*base, flag, "1"])
                self.assertNotEqual(raised.exception.code, 0)
                self.assertIn("only valid with --inspect relation", stderr.getvalue())

    def test_timeline_inspection_requires_a_nonnegative_integer_id(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        for timeline in (None, -1, True):
            with self.subTest(timeline=timeline), self.assertRaisesRegex(
                MODULE.PlanError, "nonnegative timeline ID"
            ):
                MODULE.inspect_store(
                    Path("/inspect"), "/unused", "timeline", schema,
                    timeline=timeline,
                )

    def test_inspector_response_fields_have_strict_json_types(self):
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        all_fields = set().union(*MODULE.INSPECTION_RESPONSES.values())
        self.assertEqual(
            MODULE.INSPECTION_BOOLEAN_FIELDS,
            {
                "manifest_poisoned", "forkmeta_pending",
                "forkmeta_poisoned", "retention_poisoned",
                "page_debt_unavailable",
            },
        )
        self.assertEqual(
            MODULE.INSPECTION_COUNTER_FIELDS
            | MODULE.INSPECTION_SIGNED_FIELDS
            | MODULE.INSPECTION_BOOLEAN_FIELDS,
            all_fields,
        )
        self.assertFalse(
            MODULE.INSPECTION_COUNTER_FIELDS & MODULE.INSPECTION_BOOLEAN_FIELDS
        )

        for operation, fields in MODULE.INSPECTION_RESPONSES.items():
            if operation == "relation":
                continue
            valid = {
                field: (
                    False if field in MODULE.INSPECTION_BOOLEAN_FIELDS
                    else -1 if field in MODULE.INSPECTION_SIGNED_FIELDS
                    else 0
                )
                for field in fields
            }
            invalid_values = {
                field: (
                    (0,) if field in MODULE.INSPECTION_BOOLEAN_FIELDS
                    else (False, -2) if field in MODULE.INSPECTION_SIGNED_FIELDS
                    else (False, -1)
                )
                for field in fields
            }
            for field, values in invalid_values.items():
                for invalid in values:
                    with self.subTest(
                        operation=operation, field=field, invalid=invalid
                    ):
                        response = dict(valid)
                        response[field] = invalid
                        result = mock.Mock(
                            returncode=0, stdout=json.dumps(response), stderr=""
                        )
                        with (
                            mock.patch.object(
                                MODULE.subprocess, "run", return_value=result
                            ),
                            self.assertRaisesRegex(MODULE.PlanError, field),
                        ):
                            MODULE.inspect_store(
                                Path("/inspect"), "/unused", operation, schema,
                                timeline=0 if operation == "timeline" else None,
                            )

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

    def test_materializer_runtime_accepts_named_fault_and_relation_snapshots(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["scenario"] = "materializer-restartpoint-after-relation-sync-before-marker"
        header["case"]["compute"] = ["writer", "materializer"]
        path = self.write_plan([
            header,
            {"op": "checkpoint", "id": "old", "target": "writer", "name": "R1"},
            {"op": "inspect_relation", "id": "inspect-r1", "target": "materializer",
             "relation": "materializer_crash_relation", "lsn": "$R1"},
            {"op": "materializer_fault", "id": "fault", "target": "materializer",
             "fault": "materializer.after_relation_sync", "action": "pause",
             "hit": 1, "timeout": 30, "name": "R9"},
            {"op": "inspect_relation", "id": "inspect-r9", "target": "materializer",
             "relation": "materializer_crash_relation", "lsn": "$R9"},
        ])
        plan = MODULE.read_plan(path)
        MODULE.validate_plan(plan, capabilities)
        MODULE.validate_runtime_plan(plan, capabilities, "materializer_smoke")

    def test_inspect_relation_requires_prior_boundary_reference(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer"]
        for lsn, expected in (
            ("R1", "must reference"),
            ("$R2", "no completed boundary"),
        ):
            path = self.write_plan([
                header,
                {"op": "checkpoint", "id": "old", "target": "writer", "name": "R1"},
                {"op": "inspect_relation", "id": "inspect", "target": "materializer",
                 "relation": "materializer_crash_relation", "lsn": lsn},
                {"op": "materializer_fault", "id": "fault", "target": "materializer",
                 "fault": "materializer.after_marker_sync", "action": "pause",
                 "hit": 1, "timeout": 30, "name": "R2"},
            ])
            with self.assertRaisesRegex(MODULE.PlanError, expected):
                MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_materializer_pause_timeout_has_one_second_floor(self):
        self.assertEqual(MODULE.fault_watchdog_milliseconds(0.5), 500)
        with self.assertRaisesRegex(MODULE.PlanError, "at least 1 second"):
            MODULE.materializer_fault_watchdog_milliseconds(0.5)
        self.assertEqual(MODULE.materializer_fault_watchdog_milliseconds(1), 1000)

    def test_materializer_fault_validates_scenario_identity(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["scenario"] = 'bad"scenario'
        header["case"]["compute"] = ["writer", "materializer"]
        path = self.write_plan([
            header,
            {"op": "checkpoint", "id": "old", "target": "writer", "name": "R1"},
            {"op": "materializer_fault", "id": "fault", "target": "materializer",
             "fault": "materializer.after_marker_sync", "action": "pause",
             "hit": 1, "timeout": 30, "name": "R2"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "scenario.*fault identity"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_materializer_runtime_rejects_non_pause_named_fault(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer"]
        path = self.write_plan([
            header,
            {"op": "checkpoint", "id": "old", "target": "writer", "name": "R1"},
            {"op": "materializer_fault", "id": "fault", "target": "materializer",
             "fault": "materializer.after_marker_sync", "action": "process_abort",
             "hit": 1, "timeout": 30, "name": "R2"},
        ])
        plan = MODULE.read_plan(path)
        with self.assertRaisesRegex(MODULE.PlanError, "allows action.*pause"):
            MODULE.validate_plan(plan, capabilities)

    def test_materializer_fault_rejects_unsafe_name(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer"]
        path = self.write_plan([
            header,
            {"op": "materializer_fault", "id": "fault", "target": "materializer",
             "fault": "materializer.after_marker_sync", "action": "pause",
             "hit": 1, "timeout": 30, "name": "../R2"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "name must be a safe path component"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_materializer_fault_rejects_unbounded_timeout(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        header = self.header()
        header["case"]["compute"] = ["writer", "materializer"]
        path = self.write_plan([
            header,
            {"op": "materializer_fault", "id": "fault", "target": "materializer",
             "fault": "materializer.after_marker_sync", "action": "pause",
             "hit": 1, "timeout": 301, "name": "R2"},
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "1..300000 milliseconds"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

    def test_pause_fault_report_mock_allows_recovery_child_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.jsonl"
            report.write_text(json.dumps({
                "schema": 1,
                "fault": "materializer.after_marker_sync",
                "name": "materializer.after_marker_sync",
                "action": "pause",
                "hit": 1,
                "pid": 43210,
                "scenario": "materializer-restartpoint-after-marker-sync",
                "seed": 1,
                "operation": "fault-after-marker-sync",
                "state": "reached",
                "watchdog_ms": 0,
            }) + "\n", encoding="utf-8")
            value = MODULE._fault_report(
                report, "materializer.after_marker_sync", 1, None, "pause",
                "materializer-restartpoint-after-marker-sync", 1,
                "fault-after-marker-sync",
            )
            self.assertEqual(value["pid"], 43210)

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

    def test_control_restore_command_carries_immutable_incarnation(self):
        self.assertEqual(
            MODULE.pagestore_control_restore_command(
                Path("/build/pagestore_control_restore"),
                "/harness", 0, 1, "0/2", Path("/data"),
            ),
            [
                "/build/pagestore_control_restore", "--shm", "/harness",
                "--timeline", "0", "--incarnation", "1", "--lsn", "0/2",
                "/data",
            ],
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

    def test_legacy_inspection_schema_version_does_not_match_runner(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        capabilities["inspection_schema"] = 1
        schema["schema"] = 1
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema.json"
            path.write_text(json.dumps(schema), encoding="utf-8")
            with self.assertRaisesRegex(MODULE.PlanError, "runner implementation"):
                MODULE.read_inspection_schema(path, capabilities)

    def test_inspection_operations_must_match_runner_before_launch(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        cases = []
        missing = json.loads(json.dumps(schema))
        missing["implemented_operations"].remove("backpressure")
        cases.append((capabilities, missing))
        extra_capability = json.loads(json.dumps(capabilities))
        extra_capability["inspection_operations"].append("relation")
        extra_schema = json.loads(json.dumps(schema))
        extra_schema["implemented_operations"].append("relation")
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

    def test_inspection_schema_rejects_non_read_only_contract_metadata(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        for field, value, message in (
            ("transport", "public-api", "transport"),
            ("mutating_operations", ["delete"], "mutating"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                candidate = json.loads(json.dumps(schema))
                candidate[field] = value
                path = Path(temporary) / "schema.json"
                path.write_text(json.dumps(candidate), encoding="utf-8")
                with self.assertRaisesRegex(MODULE.PlanError, message):
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
            "protocol_version": 45, "page_size": 8192, "io_unit": 262144,
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
            "case \"$3\" in\n"
            "health) printf '%s\\n' '"
            "{\"protocol_version\":45,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "timeline) printf '%s\\n' '"
            "{\"parent_timeline\":0,\"fork_lsn\":0,\"retained_horizon\":0}' ;;\n"
            "manifest) printf '%s\\n' '"
            "{\"layer_count\":0,\"deleting_layers\":0,\"local_layers\":0,"
            "\"remote_durable_layers\":0,\"manifest_poisoned\":false}' ;;\n"
            "gc) printf '%s\\n' '"
            "{\"page_debt_segments\":0,\"page_debt_unavailable\":false,"
            "\"deleting_layers\":0,"
            "\"remote_cleanup_pending\":0,\"forkmeta_pending\":false,"
            "\"forkmeta_poisoned\":false}' ;;\n"
            "owners) printf '%s\\n' '"
            "{\"owner_count\":0,\"page_history_owners\":0,\"wal_owners\":0,"
            "\"wal_index_owners\":0,\"max_generation\":0,"
            "\"retention_poisoned\":false}' ;;\n"
            "backpressure) printf '%s\\n' '"
            "{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1,"
            "\"wal_index_pending_bytes\":0,\"wal_index_lagging_timelines\":0,"
            "\"page_lag_bytes\":0,\"page_high_water_bytes\":0,\"page_catchup_bytes\":0,"
            "\"page_throttled\":0,\"page_throttle_enters\":0,\"page_throttle_exits\":0,"
            "\"page_foreground_wait_ns\":0,\"wal_lag_bytes\":0,\"wal_high_water_bytes\":0,"
            "\"wal_catchup_bytes\":0,\"wal_throttled\":0,\"wal_throttle_enters\":0,"
            "\"wal_throttle_exits\":0,\"wal_foreground_wait_ns\":0,\"walidx_lag_bytes\":0,"
            "\"walidx_high_water_bytes\":0,\"walidx_catchup_bytes\":0,\"walidx_throttled\":0,"
            "\"walidx_throttle_enters\":0,\"walidx_throttle_exits\":0,\"walidx_foreground_wait_ns\":0,"
            "\"forkmeta_lag_bytes\":0,\"forkmeta_high_water_bytes\":0,\"forkmeta_catchup_bytes\":0,"
            "\"forkmeta_throttled\":0,\"forkmeta_throttle_enters\":0,"
            "\"forkmeta_throttle_exits\":0,\"forkmeta_foreground_wait_ns\":0}' ;;\n"
            "pruning) printf '%s\\n' '{\"compactions\":0,\"versions_scanned\":0,"
            "\"versions_kept\":0,\"versions_deleted\":0}' ;;\n"
            "relation) printf '%s\\n' '{\"exists\":false,\"forks\":[],"
            "\"selected_version\":null}' ;;\n"
            "*) echo 'unsupported operation' >&2; exit 1 ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        inspector.chmod(0o755)
        schema = MODULE.read_json(ROOT / "inspection_schema.json")
        observations = MODULE.probe_runtime_inspection(
            inspector, "/unused", CAPABILITIES, schema
        )
        self.assertEqual(set(observations), set(CAPABILITIES["inspection_operations"]))

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
            "'{\"protocol_version\":45,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "timeline) printf '%s\\n' '"
            "{\"parent_timeline\":0,\"fork_lsn\":0,\"retained_horizon\":0}' ;;\n"
            "manifest) printf '%s\\n' '"
            "{\"layer_count\":0,\"deleting_layers\":0,\"local_layers\":0,"
            "\"remote_durable_layers\":0,\"manifest_poisoned\":false}' ;;\n"
            "gc) printf '%s\\n' '"
            "{\"page_debt_segments\":0,\"page_debt_unavailable\":false,"
            "\"deleting_layers\":0,"
            "\"remote_cleanup_pending\":0,\"forkmeta_pending\":false,"
            "\"forkmeta_poisoned\":false}' ;;\n"
            "owners) printf '%s\\n' '"
            "{\"owner_count\":0,\"page_history_owners\":0,\"wal_owners\":0,"
            "\"wal_index_owners\":0,\"max_generation\":0,"
            "\"retention_poisoned\":false}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1,"
            "\"wal_index_pending_bytes\":0,\"wal_index_lagging_timelines\":0,"
            "\"page_lag_bytes\":0,\"page_high_water_bytes\":0,"
            "\"page_catchup_bytes\":0,\"page_throttled\":0,"
            "\"page_throttle_enters\":0,\"page_throttle_exits\":0,"
            "\"page_foreground_wait_ns\":0,\"wal_lag_bytes\":0,"
            "\"wal_high_water_bytes\":0,\"wal_catchup_bytes\":0,"
            "\"wal_throttled\":0,\"wal_throttle_enters\":0,"
            "\"wal_throttle_exits\":0,\"wal_foreground_wait_ns\":0,"
            "\"walidx_lag_bytes\":0,\"walidx_high_water_bytes\":0,"
            "\"walidx_catchup_bytes\":0,\"walidx_throttled\":0,"
            "\"walidx_throttle_enters\":0,\"walidx_throttle_exits\":0,"
            "\"walidx_foreground_wait_ns\":0,\"forkmeta_lag_bytes\":0,"
            "\"forkmeta_high_water_bytes\":0,\"forkmeta_catchup_bytes\":0,"
            "\"forkmeta_throttled\":0,\"forkmeta_throttle_enters\":0,"
            "\"forkmeta_throttle_exits\":0,\"forkmeta_foreground_wait_ns\":0}' ;;\n"
            "pruning) printf '%s\\n' "
            "'{\"compactions\":0,\"versions_scanned\":0,\"versions_kept\":0,"
            "\"versions_deleted\":0}' ;;\n"
            "relation) printf '%s\\n' "
            "'{\"exists\":false,\"forks\":[],\"selected_version\":null}' ;;\n"
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
            "'{\"protocol_version\":45,\"page_size\":8192,\"io_unit\":262144,"
            "\"nchannels\":128,\"nshards\":1,\"admission_fence_epoch\":0,"
            "\"admission_pending_epoch\":0,\"admission_pending_lsn\":0}' ;;\n"
            "timeline) printf '%s\\n' '"
            "{\"parent_timeline\":0,\"fork_lsn\":0,\"retained_horizon\":0}' ;;\n"
            "manifest) printf '%s\\n' '"
            "{\"layer_count\":0,\"deleting_layers\":0,\"local_layers\":0,"
            "\"remote_durable_layers\":0,\"manifest_poisoned\":false}' ;;\n"
            "gc) printf '%s\\n' '"
            "{\"page_debt_segments\":0,\"page_debt_unavailable\":false,"
            "\"deleting_layers\":0,"
            "\"remote_cleanup_pending\":0,\"forkmeta_pending\":false,"
            "\"forkmeta_poisoned\":false}' ;;\n"
            "owners) printf '%s\\n' '"
            "{\"owner_count\":0,\"page_history_owners\":0,\"wal_owners\":0,"
            "\"wal_index_owners\":0,\"max_generation\":0,"
            "\"retention_poisoned\":false}' ;;\n"
            "backpressure) printf '%s\\n' "
            "'{\"idle\":128,\"claimed\":0,\"request\":0,\"done\":0,\"shards\":1,"
            "\"wal_index_pending_bytes\":0,\"wal_index_lagging_timelines\":0,"
            "\"page_lag_bytes\":0,\"page_high_water_bytes\":0,"
            "\"page_catchup_bytes\":0,\"page_throttled\":0,"
            "\"page_throttle_enters\":0,\"page_throttle_exits\":0,"
            "\"page_foreground_wait_ns\":0,\"wal_lag_bytes\":0,"
            "\"wal_high_water_bytes\":0,\"wal_catchup_bytes\":0,"
            "\"wal_throttled\":0,\"wal_throttle_enters\":0,"
            "\"wal_throttle_exits\":0,\"wal_foreground_wait_ns\":0,"
            "\"walidx_lag_bytes\":0,\"walidx_high_water_bytes\":0,"
            "\"walidx_catchup_bytes\":0,\"walidx_throttled\":0,"
            "\"walidx_throttle_enters\":0,\"walidx_throttle_exits\":0,"
            "\"walidx_foreground_wait_ns\":0,\"forkmeta_lag_bytes\":0,"
            "\"forkmeta_high_water_bytes\":0,\"forkmeta_catchup_bytes\":0,"
            "\"forkmeta_throttled\":0,\"forkmeta_throttle_enters\":0,"
            "\"forkmeta_throttle_exits\":0,\"forkmeta_foreground_wait_ns\":0}' ;;\n"
            "pruning) printf '%s\\n' "
            "'{\"compactions\":0,\"versions_scanned\":0,\"versions_kept\":0,"
            "\"versions_deleted\":0}' ;;\n"
            "relation) printf '%s\\n' "
            "'{\"exists\":false,\"forks\":[],\"selected_version\":null}' ;;\n"
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
        inspector.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        daemon.chmod(0o755)
        inspector.chmod(0o755)
        plan = self.write_plan([
            {"schema": 1, "scenario": "fault", "seed": 1, "contracts": ["lifecycle"],
             "case": {"storage": "posix", "shards": 1, "compute": ["writer"]}},
            {"op": "crash", "id": "fault", "target": "store", "model": "process_abort",
             "fault": "daemon.after_ready", "action": "crash", "hit": 1},
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

    def test_named_fault_rejects_unknown_catalog_entry(self):
        capabilities = MODULE.read_json(ROOT / "capabilities.json")
        path = self.write_plan([
            {
                "schema": 1, "scenario": "fault", "seed": 1,
                "contracts": ["lifecycle"],
                "case": {"storage": "posix", "shards": 1, "compute": ["writer"]},
            },
            {
                "op": "crash", "id": "fault", "target": "store",
                "model": "process_abort", "fault": "unknown.after_ready",
                "action": "crash", "hit": 1,
            },
        ])
        with self.assertRaisesRegex(MODULE.PlanError, "unknown fault"):
            MODULE.validate_plan(MODULE.read_plan(path), capabilities)

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
