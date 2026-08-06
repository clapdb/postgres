#!/usr/bin/env python3
"""Validate and inventory deterministic pagestore harness plans.

This is deliberately stdlib-only.  It is phase zero of the harness: plans and
capabilities are made strict before lifecycle/fault execution is introduced.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from xml.sax.saxutils import escape


class PlanError(ValueError):
    """A plan is syntactically valid JSON but has invalid harness semantics."""


class EventLog:
    """Append-only event stream retained as part of every run bundle."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.sequence = 0

    def emit(self, event: str, **fields: Any) -> None:
        self.sequence += 1
        record = {
            "sequence": self.sequence,
            "event": event,
            "monotonic_ns": time.monotonic_ns(),
            "wall_time": time.time(),
            **fields,
        }
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def private_environment() -> dict[str, str]:
    """Keep inherited cluster and pagestore settings out of a harness run."""
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PG") and not key.startswith("PAGESTORE_")
    }


def sqlstate_from_output(output: str) -> str | None:
    """Extract PostgreSQL's SQLSTATE from psql verbose error output."""
    match = re.search(r"(?:ERROR|FATAL):\s+([0-9A-Z]{5}):", output)
    return match.group(1) if match else None


HEADER_FIELDS = {"schema", "scenario", "seed", "contracts", "case", "extra"}
ACTION_FIELDS = {
    "sql": {"op", "id", "target", "sql", "expect_error", "expect_sqlstate", "extra"},
    "checkpoint": {"op", "id", "target", "name", "extra"},
    "prepare_branch": {"op", "id", "target", "fork_lsn", "extra"},
    "install_branch": {"op", "id", "target", "fork_lsn", "extra"},
    "prepare_reader": {"op", "id", "target", "base", "read_lsn", "extra"},
    "reader_base": {"op", "id", "target", "checkpoint", "name", "extra"},
    "bootstrap": {"op", "id", "target", "extra"},
    "install_reader": {"op", "id", "target", "prepared", "read_lsn", "extra"},
    "restart": {"op", "id", "target", "extra"},
    "crash": {"op", "id", "target", "model", "fault", "extra"},
    "advance": {"op", "id", "target", "profile", "steps", "extra"},
    "assert": {"op", "id", "target", "oracle", "sql", "expect", "extra"},
    "parallel": {"op", "id", "lanes", "barrier", "extra"},
    "wait": {"op", "id", "target", "predicate", "timeout", "extra"},
    "sync": {"op", "id", "target", "kind", "extra"},
    "set_fault": {"op", "id", "target", "fault", "action", "hit", "extra"},
    "release_fault": {"op", "id", "target", "fault", "extra"},
    "capture": {"op", "id", "target", "kind", "name", "horizon", "extra"},
    "compare": {"op", "id", "left", "right", "extra"},
    "expect_failure": {"op", "id", "target", "command", "sqlstate", "extra"},
    "cleanup": {"op", "id", "target", "extra"},
}

REQUIRED_FIELDS = {
    "sql": {"target", "sql"},
    "checkpoint": {"target", "name"},
    "prepare_branch": {"target", "fork_lsn"},
    "install_branch": {"target", "fork_lsn"},
    "prepare_reader": {"target", "base", "read_lsn"},
    "reader_base": {"target", "checkpoint", "name"},
    "bootstrap": {"target"},
    "install_reader": {"target", "prepared", "read_lsn"},
    "restart": {"target"},
    "crash": {"target", "model"},
    "advance": {"target", "profile", "steps"},
    "assert": {"target", "oracle", "sql", "expect"},
    "parallel": {"lanes"},
    "wait": {"target", "predicate"},
    "sync": {"target", "kind"},
    "set_fault": {"target", "fault", "action"},
    "release_fault": {"target", "fault"},
    "capture": {"target", "kind", "name", "horizon"},
    "compare": {"left", "right"},
    "expect_failure": {"target", "command"},
    "cleanup": {"target"},
}


@dataclass(frozen=True)
class Plan:
    path: Path
    header: dict[str, Any]
    actions: tuple[dict[str, Any], ...]


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PlanError(f"{path}: cannot read JSON: {error}") from error
    if not isinstance(value, dict):
        raise PlanError(f"{path}: expected a JSON object")
    return value


def read_plan(path: Path) -> Plan:
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise PlanError(f"{path}: cannot read plan: {error}") from error
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise PlanError(f"{path}:{number}: invalid JSON: {error.msg}") from error
        if not isinstance(record, dict):
            raise PlanError(f"{path}:{number}: expected a JSON object")
        records.append(record)
    if not records:
        raise PlanError(f"{path}: empty plan")
    return Plan(path=path, header=records[0], actions=tuple(records[1:]))


def read_inspection_schema(path: Path, capabilities: dict[str, Any]) -> dict[str, Any]:
    schema = read_json(path)
    if schema.get("schema") != capabilities.get("inspection_schema"):
        raise PlanError(f"{path}: inspection schema version does not match capabilities")
    return schema


def capability_values(capabilities: dict[str, Any], name: str) -> set[Any]:
    values = capabilities.get(name)
    if not isinstance(values, list) or not values:
        raise PlanError(f"capabilities: missing non-empty {name} list")
    return set(values)


def runtime_capabilities(capabilities: dict[str, Any], runtime: str) -> dict[str, Any]:
    runtimes = capabilities.get("runtimes")
    if not isinstance(runtimes, dict) or not isinstance(runtimes.get(runtime), dict):
        raise PlanError(f"capabilities: missing runtime profile {runtime!r}")
    return runtimes[runtime]


def validate_runtime_plan(plan: Plan, capabilities: dict[str, Any], runtime: str) -> None:
    profile = runtime_capabilities(capabilities, runtime)
    for field in ("protocol_version", "page_size", "io_unit"):
        value = profile.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise PlanError(f"capabilities: runtime {runtime!r} has invalid {field}")
    operations = profile.get("operations")
    if not isinstance(operations, list) or not all(isinstance(op, str) for op in operations):
        raise PlanError(f"capabilities: runtime {runtime!r} has an invalid operations list")
    unsupported = sorted({action["op"] for action in plan.actions} - set(operations))
    if unsupported:
        raise PlanError(
            f"runtime {runtime!r} cannot execute operation(s): {', '.join(unsupported)}"
        )
    constraints = profile.get("constraints", {})
    if not isinstance(constraints, dict):
        raise PlanError(f"capabilities: runtime {runtime!r} has invalid constraints")
    for action in plan.actions:
        operation_constraints = constraints.get(action["op"], {})
        if not isinstance(operation_constraints, dict):
            raise PlanError(
                f"capabilities: runtime {runtime!r} has invalid {action['op']!r} constraints"
            )
        forbidden = operation_constraints.get("forbidden_fields", [])
        if not isinstance(forbidden, list) or not all(isinstance(field, str) for field in forbidden):
            raise PlanError(
                f"capabilities: runtime {runtime!r} has invalid forbidden_fields"
            )
        present = sorted(set(action) & set(forbidden))
        if present:
            raise PlanError(
                f"runtime {runtime!r} operation {action['op']!r} does not support "
                f"field(s): {', '.join(present)}"
            )
        forbidden_values = operation_constraints.get("forbidden_values", {})
        if not isinstance(forbidden_values, dict):
            raise PlanError(
                f"capabilities: runtime {runtime!r} has invalid forbidden_values"
            )
        for field, values in forbidden_values.items():
            if not isinstance(values, list) or not values:
                raise PlanError(
                    f"capabilities: runtime {runtime!r} has invalid forbidden values "
                    f"for {field!r}"
                )
            if action.get(field) in values:
                raise PlanError(
                    f"runtime {runtime!r} operation {action['op']!r} forbids "
                    f"{field}={action.get(field)!r}"
                )
        for field, allowed in operation_constraints.items():
            if field in ("forbidden_fields", "forbidden_values"):
                continue
            if not isinstance(allowed, list) or not allowed:
                raise PlanError(
                    f"capabilities: runtime {runtime!r} has invalid constraint {field!r}"
                )
            if action.get(field) not in allowed:
                raise PlanError(
                    f"runtime {runtime!r} operation {action['op']!r} does not support "
                    f"{field}={action.get(field)!r}"
                )
    if runtime == "writer_smoke":
        available_clients = {"writer"}
        checkpoints: set[str] = set()
        reader_bases: set[str] = set()
        prepared_readers: dict[str, str] = {}
        reader_seeds: set[str] = set()
        for action in plan.actions:
            if action["op"] in ("sql", "assert") and action["target"] not in available_clients:
                raise PlanError(
                    f"runtime {runtime!r} operation {action['op']!r} target "
                    f"{action['target']!r} is not an available compute"
                )
            if action["op"] == "checkpoint":
                checkpoints.add(action["name"])
            elif action["op"] == "reader_base":
                checkpoint = action["checkpoint"]
                if not checkpoint.startswith("$") or checkpoint[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} reader_base requires an earlier checkpoint"
                    )
                reader_bases.add(action["name"])
            elif action["op"] == "prepare_reader":
                base = action["base"]
                read_lsn = action["read_lsn"]
                if not base.startswith("$") or base[1:] not in reader_bases:
                    raise PlanError(
                        f"runtime {runtime!r} prepare_reader requires an earlier reader_base"
                    )
                if not read_lsn.startswith("$") or read_lsn[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} prepare_reader requires an earlier checkpoint read_lsn"
                    )
                prepared_readers[action["id"]] = read_lsn
            elif action["op"] == "capture":
                horizon = action["horizon"]
                if not horizon.startswith("$") or horizon[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} capture requires an earlier checkpoint horizon"
                    )
                if action["name"] in reader_seeds:
                    raise PlanError(
                        f"runtime {runtime!r} reader_datadir capture name "
                        f"{action['name']!r} is already used"
                    )
                reader_seeds.add(action["name"])
            elif action["op"] == "install_reader":
                if action["target"] in available_clients:
                    raise PlanError(
                        f"runtime {runtime!r} reader target {action['target']!r} "
                        "is already installed"
                    )
                prepared_horizon = prepared_readers.get(action["prepared"])
                if prepared_horizon is None:
                    raise PlanError(
                        f"runtime {runtime!r} reader target {action['target']!r} "
                        f"requires prepared artifact {action['prepared']!r}"
                    )
                if action["target"] not in reader_seeds:
                    raise PlanError(
                        f"runtime {runtime!r} reader target {action['target']!r} "
                        "requires an earlier reader_datadir capture"
                    )
                read_lsn = action["read_lsn"]
                if not read_lsn.startswith("$") or read_lsn[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} install_reader requires an earlier checkpoint read_lsn"
                    )
                if read_lsn != prepared_horizon:
                    raise PlanError(
                        f"runtime {runtime!r} install_reader read_lsn {read_lsn!r} "
                        f"does not match prepared artifact horizon {prepared_horizon!r}"
                    )
                available_clients.add(action["target"])


def validate_runtime_health(
    plan: Plan,
    capabilities: dict[str, Any],
    runtime: str,
    health: dict[str, Any],
    inspection_schema: dict[str, Any],
) -> None:
    profile = runtime_capabilities(capabilities, runtime)
    for field in ("protocol_version", "page_size", "io_unit"):
        expected = profile.get(field)
        if not isinstance(expected, int) or isinstance(expected, bool) or expected <= 0:
            raise PlanError(f"capabilities: runtime {runtime!r} has invalid {field}")
        if health.get(field) != expected:
            raise PlanError(
                f"runtime {runtime!r} {field} mismatch: advertised {expected}, "
                f"observed {health.get(field)!r}"
            )
    expected_shards = plan.header["case"]["shards"]
    if health.get("nshards") != expected_shards:
        raise PlanError(
            f"runtime {runtime!r} shard mismatch: plan requires {expected_shards}, "
            f"observed {health.get('nshards')!r}"
        )
    implemented = inspection_schema.get("implemented_operations")
    if not isinstance(implemented, list):
        raise PlanError("inspection schema: implemented_operations must be a list")
    missing = sorted(capability_values(capabilities, "inspection_operations") - set(implemented))
    if missing:
        raise PlanError(
            f"runtime {runtime!r} lacks advertised inspection operation(s): {', '.join(missing)}"
        )


def validate_postgres_runtime(postgres: Path, capabilities: dict[str, Any]) -> int:
    try:
        result = subprocess.run(
            [str(postgres), "--version"], capture_output=True, check=False,
            encoding="utf-8", env=private_environment(),
        )
    except OSError as error:
        raise PlanError(f"cannot run PostgreSQL binary {postgres}: {error}") from error
    match = re.search(r"PostgreSQL\)?\s+(\d+)", result.stdout)
    if result.returncode != 0 or match is None:
        raise PlanError(
            f"cannot identify PostgreSQL runtime {postgres}: "
            f"{(result.stderr or result.stdout).strip()}"
        )
    major = int(match.group(1))
    if major not in capability_values(capabilities, "postgres_major"):
        raise PlanError(f"PostgreSQL runtime major {major} is not advertised as supported")
    return major


def postgres_runtime_settings(major: int) -> str:
    """Settings whose availability differs across supported PostgreSQL releases."""
    return "io_method = sync\n" if major >= 18 else ""


def validate_postgres_block_size(build: Path, expected: int) -> int:
    config_header = build / "src" / "include" / "pg_config.h"
    try:
        config = config_header.read_text(encoding="utf-8")
    except OSError as error:
        raise PlanError(f"cannot read PostgreSQL configuration {config_header}: {error}") from error
    match = re.search(r"^#define\s+BLCKSZ\s+(\d+)\s*$", config, re.MULTILINE)
    if match is None:
        raise PlanError(f"cannot identify PostgreSQL block size in {config_header}")
    block_size = int(match.group(1))
    if block_size != expected:
        raise PlanError(
            f"PostgreSQL block size {block_size} does not match advertised runtime "
            f"page size {expected}"
        )
    return block_size


def validate_postgres_relation_segment_size(build: Path, page_size: int) -> int:
    config_header = build / "src" / "include" / "pg_config.h"
    try:
        config = config_header.read_text(encoding="utf-8")
    except OSError as error:
        raise PlanError(f"cannot read PostgreSQL configuration {config_header}: {error}") from error
    match = re.search(r"^#define\s+RELSEG_SIZE\s+(\d+)\s*$", config, re.MULTILINE)
    if match is None:
        raise PlanError(f"cannot identify PostgreSQL relation segment size in {config_header}")
    segment_blocks = int(match.group(1))
    expected_blocks = (1024 * 1024 * 1024) // page_size
    if segment_blocks != expected_blocks:
        raise PlanError(
            f"PostgreSQL relation segment size {segment_blocks} blocks does not match "
            f"pagestore importer assumption {expected_blocks} blocks"
        )
    return segment_blocks


def probe_runtime_inspection(
    inspector: Path, shm: str, capabilities: dict[str, Any],
    inspection_schema: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    observations = {}
    for operation in sorted(capability_values(capabilities, "inspection_operations")):
        observations[operation] = inspect_store(
            inspector, shm, operation, inspection_schema
        )
    return observations


def require_string(record: dict[str, Any], field: str, context: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise PlanError(f"{context}: {field} must be a non-empty string")
    return value


def reject_unknown_fields(record: dict[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(record) - allowed)
    if unknown:
        raise PlanError(f"{context}: unknown field(s): {', '.join(unknown)}")
    extra = record.get("extra")
    if extra is not None and not isinstance(extra, dict):
        raise PlanError(f"{context}: extra must be an object")


def validate_plan(plan: Plan, capabilities: dict[str, Any]) -> None:
    header = plan.header
    context = str(plan.path)
    reject_unknown_fields(header, HEADER_FIELDS, context)
    if header.get("schema") != 1:
        raise PlanError(f"{context}: header schema must be 1")
    require_string(header, "scenario", context)
    seed = header.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        raise PlanError(f"{context}: seed must be a non-negative integer")
    contracts = header.get("contracts")
    if not isinstance(contracts, list) or not contracts or not all(
        isinstance(contract, str) and contract for contract in contracts
    ):
        raise PlanError(f"{context}: contracts must be a non-empty string list")

    case = header.get("case")
    if not isinstance(case, dict):
        raise PlanError(f"{context}: case must be an object")
    storage = require_string(case, "storage", f"{context}: case")
    if storage not in capability_values(capabilities, "storage"):
        raise PlanError(f"{context}: unsupported storage backend {storage!r}")
    shards = case.get("shards")
    if not isinstance(shards, int) or isinstance(shards, bool):
        raise PlanError(f"{context}: case.shards must be an integer")
    if shards not in capability_values(capabilities, "shards"):
        raise PlanError(f"{context}: unsupported shard count {shards}")
    computes = case.get("compute")
    if not isinstance(computes, list) or not computes or not all(
        isinstance(compute, str) and compute for compute in computes
    ):
        raise PlanError(f"{context}: case.compute must be a non-empty string list")
    supported_computes = capability_values(capabilities, "compute")
    unknown_computes = sorted(set(computes) - supported_computes)
    if unknown_computes:
        raise PlanError(f"{context}: unsupported compute roles: {', '.join(unknown_computes)}")

    operations = capability_values(capabilities, "operations")
    action_ids: set[str] = set()
    boundaries: set[str] = set()
    for number, action in enumerate(plan.actions, start=2):
        action_context = f"{context}:{number}"
        operation = require_string(action, "op", action_context)
        if operation not in operations:
            raise PlanError(f"{action_context}: unsupported operation {operation!r}")
        allowed_fields = ACTION_FIELDS.get(operation)
        if allowed_fields is None:
            raise PlanError(f"{action_context}: no schema for operation {operation!r}")
        reject_unknown_fields(action, allowed_fields, action_context)
        action_id = require_string(action, "id", action_context)
        if action_id in action_ids:
            raise PlanError(f"{action_context}: duplicate action id {action_id!r}")
        action_ids.add(action_id)
        for field in REQUIRED_FIELDS[operation]:
            if field not in action:
                raise PlanError(f"{action_context}: missing required field {field!r}")
        for field in REQUIRED_FIELDS[operation] - {"lanes", "steps"}:
            require_string(action, field, action_context)
        for value in action.values():
            if isinstance(value, str) and value.startswith("$"):
                boundary = value[1:]
                if boundary not in boundaries:
                    raise PlanError(
                        f"{action_context}: horizon reference {value!r} has no completed boundary"
                    )
        if operation == "checkpoint":
            boundary = require_string(action, "name", action_context)
            if boundary in boundaries:
                raise PlanError(f"{action_context}: duplicate durability boundary {boundary!r}")
            boundaries.add(boundary)
        if operation == "reader_base":
            boundary = require_string(action, "name", action_context)
            if boundary in boundaries:
                raise PlanError(f"{action_context}: duplicate durability boundary {boundary!r}")
            boundaries.add(boundary)
        if operation == "crash":
            model = require_string(action, "model", action_context)
            if model not in capability_values(capabilities, "crash_models"):
                raise PlanError(f"{action_context}: unsupported crash model {model!r}")
        if operation == "advance":
            steps = action["steps"]
            if not isinstance(steps, int) or isinstance(steps, bool) or steps <= 0:
                raise PlanError(f"{action_context}: steps must be a positive integer")


def plan_files(directory: Path) -> Iterable[Path]:
    return sorted(path for path in directory.rglob("*.jsonl") if path.is_file())


def validate_paths(paths: Iterable[Path], capabilities: dict[str, Any]) -> list[PlanError]:
    errors: list[PlanError] = []
    for path in paths:
        try:
            validate_plan(read_plan(path), capabilities)
        except PlanError as error:
            errors.append(error)
    return errors


def write_junit(path: Path, name: str, errors: list[PlanError]) -> None:
    failures = "".join(
        f'<failure message="{escape(str(error))}" />' for error in errors
    )
    body = (
        f'<testsuite name="{escape(name)}" tests="1" failures="{int(bool(errors))}">'
        f'<testcase name="validation">{failures}</testcase></testsuite>'
    )
    path.write_text(f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}\n", encoding="utf-8")


def inspect_store(binary: Path, shm: str, operation: str, schema: dict[str, Any]) -> dict[str, Any]:
    implemented = schema.get("implemented_operations")
    operations = schema.get("operations")
    if not isinstance(implemented, list) or operation not in implemented:
        raise PlanError(f"inspection schema: operation {operation!r} is not implemented")
    if not isinstance(operations, dict) or not isinstance(operations.get(operation), dict):
        raise PlanError(f"inspection schema: missing definition for {operation!r}")
    expected = operations[operation].get("response")
    if not isinstance(expected, list) or not all(isinstance(field, str) for field in expected):
        raise PlanError(f"inspection schema: invalid response definition for {operation!r}")
    try:
        result = subprocess.run(
            [str(binary), "--shm", shm, operation],
            capture_output=True,
            check=False,
            encoding="utf-8",
        )
    except OSError as error:
        raise PlanError(f"cannot run inspector {binary}: {error}") from error
    if result.returncode != 0:
        raise PlanError(f"inspector {operation} failed: {result.stderr.strip()}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise PlanError(f"inspector {operation} returned invalid JSON: {error.msg}") from error
    if not isinstance(value, dict) or set(value) != set(expected):
        raise PlanError(f"inspector {operation} returned a response outside its schema")
    return value


def run_root(path: Path | None) -> tuple[Path, bool]:
    if path is None:
        return Path(tempfile.mkdtemp(prefix="pagestore-harness-")), True
    if path.exists() and any(path.iterdir()):
        raise PlanError(f"run root must be empty: {path}")
    path.mkdir(parents=True, exist_ok=True)
    return path, False


def remove_shm(shm: str) -> None:
    """Release a POSIX shm object without relying on Linux's /dev/shm view."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        unlink = libc.shm_unlink
        unlink.argtypes = [ctypes.c_char_p]
        unlink.restype = ctypes.c_int
        if unlink(shm.encode()) == 0:
            return
        if ctypes.get_errno() == errno.ENOENT:
            return
    except (AttributeError, OSError):
        pass

    # Fallback for platforms that expose POSIX shm only as filesystem entries.
    try:
        (Path("/dev/shm") / shm.removeprefix("/")).unlink()
    except FileNotFoundError:
        pass


def signal_process_group(process: subprocess.Popen[str], sig: signal.Signals) -> None:
    """Signal the daemon and every helper it started in its private session."""
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


def run_daemon_smoke(
    plan: Plan,
    capabilities: dict[str, Any],
    inspection_schema: dict[str, Any],
    daemon: Path,
    inspector: Path,
    requested_root: Path | None,
    keep: bool,
) -> Path:
    """Exercise the coordinator's daemon lifecycle without executing SQL actions."""
    root, temporary = run_root(requested_root)
    trace = root / "trace"
    store = root / "store"
    trace.mkdir()
    store.mkdir()
    shutil.copy2(plan.path, root / "plan.jsonl")
    (root / "case.json").write_text(
        json.dumps(plan.header["case"], indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    events = EventLog(trace / "events.jsonl")
    shm_base = f"/psharness_{os.getpid()}_{time.monotonic_ns()}"
    shm = ""
    shm_names: list[str] = []
    generation = 0
    daemon_log = trace / "daemon.log"
    process: subprocess.Popen[str] | None = None
    failure: Exception | None = None

    def require_daemon_alive(daemon_process: subprocess.Popen[str], context: str) -> None:
        if daemon_process.poll() is not None:
            raise PlanError(
                f"daemon exited {context} with status {daemon_process.returncode}")

    def start_daemon(action_id: str | None = None) -> subprocess.Popen[str]:
        nonlocal generation, process, shm
        shm = f"{shm_base}_{generation}"
        generation += 1
        shm_names.append(shm)
        command = [str(daemon), "--shm", shm, "--store", str(store),
                   "--nshards", str(plan.header["case"]["shards"]),
                   "--storage", plan.header["case"]["storage"]]
        with daemon_log.open("a", encoding="utf-8") as log:
            daemon_process = subprocess.Popen(
                command, stdout=log, stderr=subprocess.STDOUT, text=True,
                env=private_environment(), start_new_session=True)
        process = daemon_process
        events.emit("process_start", target="store", pid=daemon_process.pid,
                    argv=command, action_id=action_id)
        deadline = time.monotonic() + 10.0
        while True:
            if daemon_process.poll() is not None:
                raise PlanError(
                    f"daemon exited before readiness with status {daemon_process.returncode}")
            try:
                health = inspect_store(inspector, shm, "health", inspection_schema)
                require_daemon_alive(daemon_process, "during readiness")
                break
            except PlanError as error:
                if time.monotonic() >= deadline:
                    raise PlanError(f"daemon did not become ready: {error}") from error
                time.sleep(0.05)
        validate_runtime_health(
            plan, capabilities, "daemon_smoke", health, inspection_schema
        )
        probe_runtime_inspection(inspector, shm, capabilities, inspection_schema)
        events.emit("ready", target="store", health=health, action_id=action_id)
        return daemon_process

    try:
        events.emit("run_start", scenario=plan.header["scenario"], seed=plan.header["seed"],
                    shm_base=shm_base)
        daemon_log.touch()
        process = start_daemon()
        backpressure = inspect_store(inspector, shm, "backpressure", inspection_schema)
        events.emit("capture", target="store", kind="backpressure", value=backpressure)
        for action in plan.actions:
            if action["op"] != "crash":
                continue
            if action["target"] != "store" or action["model"] != "power_loss":
                raise PlanError(f"daemon smoke does not execute crash on {action['target']!r}")
            if "fault" in action:
                raise PlanError(
                    f"daemon smoke does not implement named fault {action['fault']!r}")
            require_daemon_alive(process, "before power loss")
            signal_process_group(process, signal.SIGKILL)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired as error:
                raise PlanError(f"crash action {action['id']} did not stop the daemon") from error
            if process.returncode != -signal.SIGKILL:
                raise PlanError(
                    f"crash action {action['id']} did not deliver SIGKILL "
                    f"(status {process.returncode})")
            events.emit("crash", id=action["id"], target="store", model="power_loss",
                        pid=process.pid, returncode=process.returncode)
            remove_shm(shm)
            process = start_daemon(action["id"])
            backpressure = inspect_store(inspector, shm, "backpressure", inspection_schema)
            require_daemon_alive(process, "after recovery")
            events.emit("recovered", id=action["id"], target="store", value=backpressure)
        events.emit("run_pass")
    except Exception as error:
        failure = error
        events.emit("run_fail", error=str(error), error_type=type(error).__name__)
        (root / "failure.json").write_text(
            json.dumps({"classification": "setup", "error": str(error)}, indent=2) + "\n",
            encoding="utf-8",
        )
    finally:
        if process is not None:
            if process.poll() is None:
                signal_process_group(process, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    signal_process_group(process, signal.SIGKILL)
                    process.wait(timeout=5)
            events.emit("process_stop", target="store", pid=process.pid, returncode=process.returncode)
        for name in shm_names:
            remove_shm(name)

    if failure is not None:
        raise PlanError(f"daemon smoke failed; failure bundle: {root}: {failure}") from failure
    if temporary and not keep:
        shutil.rmtree(root)
    return root


def find_pg_bin(build: Path) -> Path:
    matches = sorted(build.glob("tmp_install/**/bin/pg_ctl"))
    if not matches:
        raise PlanError(f"{build}: no tmp_install pg_ctl; run the Meson setup suite first")
    return matches[0].parent


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_writer_smoke(
    plan: Plan, capabilities: dict[str, Any], schema: dict[str, Any],
    daemon: Path, inspector: Path, build: Path, requested_root: Path | None,
    keep: bool,
) -> Path:
    """Start a real localsvc writer and execute SQL/checkpoint plan actions."""
    pg_bin = find_pg_bin(build)
    postgres_major = validate_postgres_runtime(pg_bin / "postgres", capabilities)
    validate_postgres_block_size(
        build,
        runtime_capabilities(capabilities, "writer_smoke")["page_size"],
    )
    validate_postgres_relation_segment_size(
        build,
        runtime_capabilities(capabilities, "writer_smoke")["page_size"],
    )
    root, temporary = run_root(requested_root)
    trace, store, data, tablespace, sockdir = (root / "trace", root / "store", root / "computes" / "writer",
                                                root / "tablespace", root / "socket")
    artifacts = root / "artifacts" / "checkpoints"
    for path in (trace, store, data, tablespace, sockdir, artifacts):
        path.mkdir(parents=True, exist_ok=True)
    shutil.copy2(plan.path, root / "plan.jsonl")
    (root / "case.json").write_text(json.dumps(plan.header["case"], indent=2) + "\n", encoding="utf-8")
    events, shm = EventLog(trace / "events.jsonl"), f"/psharness_{os.getpid()}_{time.monotonic_ns()}"
    port = free_port()
    env = private_environment()
    install = pg_bin.parent
    env["LD_LIBRARY_PATH"] = f"{install / 'lib'}:{install / 'lib64'}"
    dproc = None
    try:
        events.emit("run_start", scenario=plan.header["scenario"], seed=plan.header["seed"],
                    shm=shm, postgres_major=postgres_major)
        with (trace / "daemon.log").open("w", encoding="utf-8") as log:
            dproc = subprocess.Popen([str(daemon), "--shm", shm, "--store", str(store),
                                     "--nshards", str(plan.header["case"]["shards"]),
                                     "--storage", plan.header["case"]["storage"]], stdout=log,
                                     stderr=subprocess.STDOUT, text=True, env=env)
        deadline = time.monotonic() + 10
        while True:
            if dproc.poll() is not None:
                raise PlanError(f"daemon exited before readiness with status {dproc.returncode}")
            try:
                health = inspect_store(inspector, shm, "health", schema)
                break
            except PlanError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.05)
        validate_runtime_health(plan, capabilities, "writer_smoke", health, schema)
        probe_runtime_inspection(inspector, shm, capabilities, schema)
        subprocess.run([str(pg_bin / "initdb"), "-D", str(data), "-U", "postgres", "-A", "trust"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        (data / "postgresql.conf").open("a", encoding="utf-8").write(
            f"shared_preload_libraries = 'pagestore'\npagestore.backend = 'localsvc'\n"
            f"pagestore.localsvc_shm = '{shm}'\npagestore.route_user_tablespaces = on\n"
            "pagestore.slru_mirror = on\n"
            f"{postgres_runtime_settings(postgres_major)}"
            "max_prepared_transactions = 10\n"
            f"listen_addresses = ''\nunix_socket_directories = '{sockdir}'\nport = {port}\n")
        subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-l", str(trace / "writer.log"), "-w", "start"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        events.emit("ready", target="writer", health=health, port=port)
        checkpoints: dict[str, dict[str, str]] = {}
        reader_bases: dict[str, str] = {}
        prepared_readers: dict[str, Path] = {}
        reader_seeds: dict[str, Path] = {}
        reader_clients: dict[str, tuple[Path, int]] = {}
        reader_data_dirs: dict[str, Path] = {}
        for action in plan.actions:
            if action["op"] == "sql":
                sql = action["sql"].replace("${tablespace}", str(tablespace).replace("'", "''"))
            elif action["op"] == "assert":
                if action["oracle"] != "sql_scalar":
                    raise PlanError(f"unsupported writer smoke oracle {action['oracle']}")
                sql = action["sql"]
            elif action["op"] == "checkpoint":
                sql = """CHECKPOINT;
SELECT json_build_object(
  'redo_lsn', redo_lsn::text,
  'next_xid', split_part(next_xid, ':', 2),
  'oldest_xid', oldest_xid::text,
  'next_multixact_id', next_multixact_id::text,
  'next_multi_offset', next_multi_offset::text,
  'oldest_multi_xid', oldest_multi_xid::text,
  'oldest_commit_ts_xid', CASE WHEN oldest_commit_ts_xid::text = '0' THEN '1' ELSE oldest_commit_ts_xid::text END,
  'next_commit_ts_xid', CASE WHEN newest_commit_ts_xid::text = '0' THEN '1' ELSE ((newest_commit_ts_xid::text::bigint + 1) & 4294967295)::text END)
FROM pg_control_checkpoint();"""
            elif action["op"] == "prepare_reader":
                ref = action["read_lsn"]
                base_ref = action["base"]
                if not isinstance(ref, str) or not ref.startswith("$") or ref[1:] not in checkpoints:
                    raise PlanError(f"prepare_reader {action['id']} requires a completed checkpoint reference")
                if not isinstance(base_ref, str) or not base_ref.startswith("$") or base_ref[1:] not in reader_bases:
                    raise PlanError(f"prepare_reader {action['id']} requires a reader_base reference")
                horizon = checkpoints[ref[1:]]
                prepared = root / "artifacts" / "readers" / action["id"]
                prepared.mkdir(parents=True)
                setup = """CREATE OR REPLACE FUNCTION pagestore_prepare_reader(text, int, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint AS 'pagestore','pagestore_prepare_reader' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION pagestore_validate_reader_manifest(text, int, pg_lsn) RETURNS bool AS 'pagestore','pagestore_validate_reader_manifest' LANGUAGE C STRICT;"""
                subprocess.run([str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port), "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", setup], check=True, capture_output=True, encoding="utf-8", env=env)
                sql = f"SELECT pagestore_prepare_reader('{str(prepared).replace(chr(39), chr(39) * 2)}', 0, '{reader_bases[base_ref[1:]]}', '{horizon['redo_lsn']}', '{horizon['oldest_xid']}'::xid, '{horizon['next_xid']}'::xid, '{horizon['oldest_commit_ts_xid']}'::xid, '{horizon['next_commit_ts_xid']}'::xid, '{horizon['oldest_multi_xid']}'::xid, '{horizon['next_multixact_id']}'::xid, 0, {horizon['next_multi_offset']}); SELECT pagestore_validate_reader_manifest('{str(prepared).replace(chr(39), chr(39) * 2)}', 0, '{horizon['redo_lsn']}');"
            elif action["op"] == "reader_base":
                ref = action["checkpoint"]
                if not isinstance(ref, str) or not ref.startswith("$") or ref[1:] not in checkpoints:
                    raise PlanError(f"reader_base {action['id']} requires a completed checkpoint reference")
                base = checkpoints[ref[1:]]["redo_lsn"]
                setup = "CREATE OR REPLACE FUNCTION pagestore_ship_slru_snapshot(text, pg_lsn) RETURNS void AS 'pagestore','pagestore_ship_slru_snapshot' LANGUAGE C STRICT;"
                subprocess.run([str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port), "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", setup], check=True, capture_output=True, encoding="utf-8", env=env)
                sql = "; ".join(f"SELECT pagestore_ship_slru_snapshot('{name}', '{base}')" for name in ("pg_xact", "pg_commit_ts", "pg_multixact/offsets", "pg_multixact/members"))
            elif action["op"] == "bootstrap":
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                importer = daemon.parent / "pagestore_import"
                subprocess.run([str(importer), "--shm", shm, "--pgdata", str(data)], check=True, capture_output=True, encoding="utf-8", env=env)
                (data / "postgresql.conf").open("a", encoding="utf-8").write("pagestore.route_all = on\n")
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-l", str(trace / "writer.log"), "-w", "start"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                events.emit("bootstrap", id=action["id"], target="writer", route_all=True)
                continue
            elif action["op"] == "install_reader":
                ref = action["read_lsn"]
                prepared = prepared_readers.get(action["prepared"])
                if prepared is None or not isinstance(ref, str) or not ref.startswith("$") or ref[1:] not in checkpoints:
                    raise PlanError(f"install_reader {action['id']} requires prepared artifact and checkpoint")
                seed = reader_seeds.get(action["target"])
                if seed is None:
                    raise PlanError(f"install_reader {action['id']} requires reader_datadir capture")
                reader_data = root / "computes" / action["target"]
                reader_socket = root / "socket" / action["target"]
                reader_socket.mkdir(parents=True)
                shutil.copytree(seed, reader_data)
                lsn = checkpoints[ref[1:]]["redo_lsn"]
                restore = daemon.parent / "pagestore_control_restore"
                subprocess.run([str(restore), "--shm", shm, "--timeline", "0", "--lsn", lsn, str(reader_data)], check=True, capture_output=True, encoding="utf-8", env=env)
                setup = """CREATE OR REPLACE FUNCTION pagestore_install_prepared_reader(text, text, int, pg_lsn) RETURNS void AS 'pagestore','pagestore_install_prepared_reader' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION pagestore_mark_reader_catalog_snapshot(text, int, pg_lsn) RETURNS void AS 'pagestore','pagestore_mark_reader_catalog_snapshot' LANGUAGE C STRICT;"""
                subprocess.run([str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port), "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", setup], check=True, capture_output=True, encoding="utf-8", env=env)
                reader_data_sql = str(reader_data).replace(chr(39), chr(39) * 2)
                sql = f"SELECT pagestore_mark_reader_catalog_snapshot('{reader_data_sql}', 0, '{lsn}'); SELECT pagestore_install_prepared_reader('{str(prepared).replace(chr(39), chr(39) * 2)}', '{reader_data_sql}', 0, '{lsn}')"
                result = subprocess.run([str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port), "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql], check=True, capture_output=True, encoding="utf-8", env=env)
                reader_port = free_port()
                (reader_data / "postgresql.conf").open("a", encoding="utf-8").write(f"pagestore.read_lsn = '{lsn}'\npagestore.route_all = on\narchive_mode = off\nlisten_addresses = ''\nunix_socket_directories = '{reader_socket}'\nport = {reader_port}\n")
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(reader_data), "-l", str(trace / "reader.log"), "-w", "start"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                reader_clients[action["target"]] = (reader_socket, reader_port)
                reader_data_dirs[action["target"]] = reader_data
                events.emit("install_reader", id=action["id"], target=action["target"], lsn=lsn, data=str(reader_data), port=reader_port)
                continue
            elif action["op"] == "capture":
                ref = action["horizon"]
                if action["kind"] != "reader_datadir" or not isinstance(ref, str) or not ref.startswith("$") or ref[1:] not in checkpoints:
                    raise PlanError(f"capture {action['id']} requires reader_datadir and checkpoint horizon")
                seed = root / "reader-seeds" / action["name"]
                seed.parent.mkdir(exist_ok=True)
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                shutil.copytree(data, seed)
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-l", str(trace / "writer.log"), "-w", "start"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                reader_seeds[action["name"]] = seed
                events.emit("capture", id=action["id"], kind="reader_datadir", horizon=ref, path=str(seed))
                continue
            else:
                raise PlanError(f"writer smoke does not execute {action['op']}")
            target = action["target"]
            if target == "writer":
                client_socket, client_port = sockdir, port
            elif target in reader_clients:
                client_socket, client_port = reader_clients[target]
            else:
                raise PlanError(f"action {action['id']} targets unavailable compute {target!r}")
            result = subprocess.run([str(pg_bin / "psql"), "-h", str(client_socket), "-p", str(client_port), "-U", "postgres", "-tA", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-c", sql],
                                    check=False, capture_output=True, encoding="utf-8", env=env)
            expected_error = action.get("expect_error")
            expected_sqlstate = action.get("expect_sqlstate")
            if expected_error is not None or expected_sqlstate is not None:
                combined = result.stdout + result.stderr
                actual_sqlstate = sqlstate_from_output(combined)
                if (result.returncode == 0 or
                        (expected_error is not None and expected_error not in combined) or
                        (expected_sqlstate is not None and expected_sqlstate != actual_sqlstate)):
                    raise PlanError(
                        f"sql {action['id']} expected failure with error {expected_error!r} and "
                        f"SQLSTATE {expected_sqlstate!r}, got status {result.returncode}, "
                        f"SQLSTATE {actual_sqlstate!r}: {combined.strip()!r}")
                events.emit("expected_failure", id=action["id"], target=action["target"],
                            error=expected_error, sqlstate=actual_sqlstate)
                continue
            result.check_returncode()
            output = result.stdout.strip()
            if action["op"] == "checkpoint":
                horizon = json.loads(output.splitlines()[-1])
                checkpoints[action["name"]] = horizon
                (artifacts / f"{action['name']}.json").write_text(
                    json.dumps(horizon, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                events.emit("checkpoint", id=action["id"], name=action["name"], horizon=horizon)
            elif action["op"] == "reader_base":
                reader_bases[action["name"]] = checkpoints[action["checkpoint"][1:]]["redo_lsn"]
                events.emit("reader_base", id=action["id"], name=action["name"], lsn=reader_bases[action["name"]])
            elif action["op"] == "prepare_reader":
                prepared_readers[action["id"]] = root / "artifacts" / "readers" / action["id"]
                events.emit("prepare_reader", id=action["id"], result=output,
                            artifact=str(root / "artifacts" / "readers" / action["id"]))
            elif action["op"] == "assert":
                if output != action["expect"]:
                    raise PlanError(f"assert {action['id']} got {output!r}, expected {action['expect']!r}")
                events.emit("assert", id=action["id"], target=action["target"], actual=output)
            else:
                events.emit("action", id=action["id"], op=action["op"], result=output)
        events.emit("run_pass")
    except Exception as error:
        events.emit("run_fail", error=str(error), error_type=type(error).__name__)
        (root / "failure.json").write_text(json.dumps({"classification": "setup", "error": str(error)}, indent=2) + "\n", encoding="utf-8")
        raise PlanError(f"writer smoke failed; failure bundle: {root}: {error}") from error
    finally:
        for reader_data in reader_data_dirs.values():
            if (reader_data / "postmaster.pid").exists():
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(reader_data), "-m", "immediate", "-w", "stop"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        if (data / "postmaster.pid").exists():
            subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "immediate", "-w", "stop"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        if dproc is not None and dproc.poll() is None:
            dproc.terminate(); dproc.wait(timeout=5)
        remove_shm(shm)
    if temporary and not keep:
        shutil.rmtree(root)
    return root


def run_legacy_integration(script: Path, build: Path, requested_root: Path | None, keep: bool) -> Path:
    """Bridge the existing reader/branch integration coverage into a bundle."""
    root, temporary = run_root(requested_root)
    trace = root / "trace"; trace.mkdir()
    events = EventLog(trace / "events.jsonl")
    events.emit("run_start", scenario="legacy-integration", build=str(build))
    with (trace / "integration.log").open("w", encoding="utf-8") as log:
        result = subprocess.run([str(script), str(build)], stdout=log, stderr=subprocess.STDOUT,
                                check=False, text=True, env=private_environment())
    if result.returncode:
        events.emit("run_fail", returncode=result.returncode)
        (root / "failure.json").write_text(json.dumps({"classification": "oracle_mismatch", "returncode": result.returncode}, indent=2) + "\n", encoding="utf-8")
        raise PlanError(f"legacy integration failed; failure bundle: {root}")
    events.emit("run_pass", returncode=0)
    if temporary and not keep: shutil.rmtree(root)
    return root


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capabilities", type=Path, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate", type=Path, metavar="PLAN")
    group.add_argument("--list", type=Path, metavar="SCENARIO_DIR")
    group.add_argument("--inspect", choices=["health", "backpressure"])
    group.add_argument("--daemon-smoke", type=Path, metavar="PLAN")
    group.add_argument("--writer-smoke", type=Path, metavar="PLAN")
    group.add_argument("--legacy-integration", action="store_true")
    parser.add_argument("--daemon-binary", type=Path)
    parser.add_argument("--build-dir", type=Path)
    parser.add_argument("--integration-script", type=Path,
                        default=Path(__file__).resolve().parents[1] / "integration_test.sh")
    parser.add_argument("--inspect-binary", type=Path)
    parser.add_argument("--shm")
    parser.add_argument("--inspection-schema", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--junit", type=Path)
    args = parser.parse_args(argv)
    if args.inspect and (args.inspect_binary is None or not args.shm):
        parser.error("--inspect requires --inspect-binary and --shm")
    if args.daemon_smoke and (args.daemon_binary is None or args.inspect_binary is None):
        parser.error("--daemon-smoke requires --daemon-binary and --inspect-binary")
    if args.writer_smoke and (args.daemon_binary is None or args.inspect_binary is None or args.build_dir is None):
        parser.error("--writer-smoke requires --build-dir, --daemon-binary and --inspect-binary")
    if args.legacy_integration and args.build_dir is None:
        parser.error("--legacy-integration requires --build-dir")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        capabilities = read_json(args.capabilities)
        if capabilities.get("schema") != 1:
            raise PlanError(f"{args.capabilities}: capability schema must be 1")
        if args.inspect:
            if args.inspect not in capability_values(capabilities, "inspection_operations"):
                raise PlanError(f"capabilities: inspection operation {args.inspect!r} is unavailable")
            schema_path = args.inspection_schema or args.capabilities.with_name("inspection_schema.json")
            schema = read_inspection_schema(schema_path, capabilities)
            print(json.dumps(inspect_store(args.inspect_binary, args.shm, args.inspect, schema), sort_keys=True))
            return 0
        if args.daemon_smoke:
            plan = read_plan(args.daemon_smoke)
            validate_plan(plan, capabilities)
            validate_runtime_plan(plan, capabilities, "daemon_smoke")
            schema_path = args.inspection_schema or args.capabilities.with_name("inspection_schema.json")
            schema = read_inspection_schema(schema_path, capabilities)
            root = run_daemon_smoke(plan, capabilities, schema, args.daemon_binary,
                                    args.inspect_binary, args.run_root, args.keep)
            if args.keep or args.run_root:
                print(root)
            return 0
        if args.writer_smoke:
            plan = read_plan(args.writer_smoke); validate_plan(plan, capabilities)
            validate_runtime_plan(plan, capabilities, "writer_smoke")
            schema_path = args.inspection_schema or args.capabilities.with_name("inspection_schema.json")
            schema = read_inspection_schema(schema_path, capabilities)
            root = run_writer_smoke(plan, capabilities, schema, args.daemon_binary, args.inspect_binary,
                                    args.build_dir, args.run_root, args.keep)
            if args.keep or args.run_root: print(root)
            return 0
        if args.legacy_integration:
            root = run_legacy_integration(args.integration_script, args.build_dir, args.run_root, args.keep)
            if args.keep or args.run_root: print(root)
            return 0
        paths = [args.validate] if args.validate else list(plan_files(args.list))
        if not paths:
            raise PlanError("no scenario plans found")
        errors = validate_paths(paths, capabilities)
    except PlanError as error:
        errors = [error]
        paths = []
    if args.list and not errors:
        for path in paths:
            plan = read_plan(path)
            print(f"{plan.header['scenario']}\t{path}\t{','.join(plan.header['contracts'])}")
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if args.junit:
        write_junit(args.junit, "pagestore-harness-plan", errors)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
