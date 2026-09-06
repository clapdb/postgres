#!/usr/bin/env python3
"""Validate and execute deterministic pagestore harness plans.

The runner is deliberately stdlib-only.  Plans and runtime capabilities stay
strict as daemon, writer, reader, and materializer lifecycle coverage grows.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import math
import os
import re
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable
from xml.sax.saxutils import escape


class PlanError(ValueError):
    """A plan is syntactically valid JSON but has invalid harness semantics."""


class OracleMismatch(PlanError):
    """A runtime assertion produced a value different from its oracle."""


class HarnessTimeout(PlanError):
    """A bounded harness operation exceeded its deadline."""


class FaultNotReached(PlanError):
    """The expected named fault did not produce its signed report."""


class UnexpectedExit(PlanError):
    """A faulted process exited for a reason other than the expected abort."""


FAULT_ACTIONS = {"crash", "error", "pause"}
FAULT_REPORT_FIELDS = {
    "schema", "scenario", "seed", "fault", "action", "hit", "pid", "operation",
}
FAULT_REPORT_MAX_BYTES = 4096


def fault_failure_classification(error: Exception) -> str:
    if isinstance(error, FaultNotReached):
        return "fault_not_reached"
    if isinstance(error, HarnessTimeout):
        return "timeout"
    if isinstance(error, UnexpectedExit):
        return "unexpected_exit"
    return "setup"


def expected_error_exit(action: str, returncode: int | None) -> bool:
    return action == "error" and returncode == 1


class EventLog:
    """Append-only event stream retained as part of every run bundle."""

    def __init__(self, path: Path, context: dict[str, Any] | None = None) -> None:
        self.path = path
        self.sequence = 0
        self.context = {
            "scenario": None, "seed": None, "fault": None, "action": None,
            "hit": None, "hit_count": None, "operation": None,
            "operation_id": None, **(context or {}),
        }

    def emit(self, event: str, **fields: Any) -> None:
        self.sequence += 1
        record = {
            "sequence": self.sequence,
            "event": event,
            "monotonic_ns": time.monotonic_ns(),
            "wall_time": time.time(),
            **self.context,
            **fields,
        }
        if record.get("operation_id") is None and record.get("action_id") is not None:
            record["operation_id"] = record["action_id"]
        if record.get("operation") is None and record.get("operation_id") is not None:
            record["operation"] = record["operation_id"]
        if record.get("fault") is None and record.get("name") is not None:
            record["fault"] = record["name"]
        if record.get("hit_count") is None and record.get("hit") is not None:
            record["hit_count"] = record["hit"]
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


def postgresql_conf_string(value: str | Path) -> str:
    """Return value as a safely quoted PostgreSQL configuration string."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


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
    "crash": {"op", "id", "target", "model", "fault", "action", "hit", "timeout", "extra"},
    "advance": {"op", "id", "target", "profile", "steps", "extra"},
    "assert": {"op", "id", "target", "oracle", "sql", "expect", "extra"},
    "parallel": {"op", "id", "lanes", "barrier", "extra"},
    "wait": {"op", "id", "target", "predicate", "timeout", "extra"},
    "sync": {"op", "id", "target", "kind", "extra"},
    "set_fault": {"op", "id", "target", "fault", "action", "hit", "timeout", "extra"},
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

SAFE_COMPONENT_FIELDS = {
    "checkpoint": {"name"},
    "reader_base": {"name"},
    "capture": {"name"},
    "install_reader": {"prepared"},
}


@dataclass(frozen=True)
class Plan:
    path: Path
    header: dict[str, Any]
    actions: tuple[dict[str, Any], ...]


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
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
    advertised = capability_values(capabilities, "inspection_operations")
    if advertised != INSPECTION_OPERATIONS:
        raise PlanError(
            "capabilities: inspection operations do not match runner implementation"
        )
    implemented = schema.get("implemented_operations")
    if (
        not isinstance(implemented, list)
        or not all(isinstance(operation, str) for operation in implemented)
        or len(implemented) != len(set(implemented))
        or set(implemented) != INSPECTION_OPERATIONS
    ):
        raise PlanError(
            f"{path}: implemented inspection operations do not match runner implementation"
        )
    operations = schema.get("operations")
    if not isinstance(operations, dict):
        raise PlanError(f"{path}: inspection operations must be an object")
    for operation, expected_response in sorted(INSPECTION_RESPONSES.items()):
        definition = operations.get(operation)
        response = definition.get("response") if isinstance(definition, dict) else None
        if (
            not isinstance(response, list)
            or not all(isinstance(field, str) and field for field in response)
            or len(response) != len(set(response))
            or set(response) != expected_response
        ):
            raise PlanError(
                f"{path}: inspection operation {operation!r} response fields "
                "do not match runner implementation"
            )
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


FAULT_CATALOG_KEY = "fault_catalog"


def fault_catalog_path(capabilities: dict[str, Any], base_path: Path | None = None) -> Path:
    relative = capabilities.get(FAULT_CATALOG_KEY)
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise PlanError(f"capabilities: {FAULT_CATALOG_KEY} must be a relative path")
    base = (base_path.parent if base_path is not None else Path(__file__).resolve().parent)
    candidate = base / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise PlanError(f"capabilities: fault catalog is not a regular file: {candidate}")
    path = candidate.resolve()
    return path


def fault_catalog(
    capabilities: dict[str, Any], base_path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Parse the shared ``pagestore_fault_points.def`` as the sole catalog."""
    path = fault_catalog_path(capabilities, base_path)
    entries: dict[str, dict[str, Any]] = {}
    try:
        source = path.read_text(encoding="utf-8")
        source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
        lines = source.splitlines()
    except OSError as error:
        raise PlanError(f"cannot read fault catalog {path}: {error}") from error
    pattern = re.compile(
        r'^PAGESTORE_FAULT_POINT\('
        r'([A-Z][A-Z0-9_]*),\s*"([a-z][a-z0-9_.-]*)",\s*'
        r'"([a-z][a-z0-9_-]*)",\s*"([a-z][a-z0-9_-]*)",\s*'
        r'"([a-z]+(?:\s*\|\s*[a-z]+)*)",\s*([1-9][0-9]*),\s*([0-9]+)\)$'
    )
    symbols: set[str] = set()
    for number, raw in enumerate(lines, start=1):
        line = raw.split("#", 1)[0].split("//", 1)[0].strip()
        if not line:
            continue
        match = pattern.fullmatch(line)
        if match is None:
            raise PlanError(f"fault catalog {path}:{number}: invalid record")
        symbol, name, target, model, action_text, hit_text, max_hit_text = match.groups()
        actions = tuple(part.strip() for part in action_text.split("|"))
        configured_actions = capabilities.get("fault_actions", sorted(FAULT_ACTIONS))
        if not isinstance(configured_actions, list) or not all(
            isinstance(item, str) and item in FAULT_ACTIONS for item in configured_actions
        ):
            raise PlanError("capabilities: fault_actions must be a supported action subset")
        if not actions or len(set(actions)) != len(actions) or any(
            item not in set(configured_actions) for item in actions
        ):
            raise PlanError(f"fault catalog {path}:{number}: invalid action list")
        hit = int(hit_text, 10)
        max_hit = int(max_hit_text, 10)
        if hit <= 0 or hit > 2**63 - 1:
            raise PlanError(f"fault catalog {path}:{number}: hit out of range")
        if max_hit > 2**63 - 1 or (max_hit != 0 and hit > max_hit):
            raise PlanError(f"fault catalog {path}:{number}: max_hit out of range")
        models = capabilities.get("crash_models", [])
        if isinstance(models, list) and model not in models:
            raise PlanError(f"fault catalog {path}:{number}: unknown model {model!r}")
        computes = capabilities.get("compute", [])
        if isinstance(computes, list) and target != "store" and target not in computes:
            raise PlanError(f"fault catalog {path}:{number}: unknown target {target!r}")
        if name in entries or symbol in symbols:
            raise PlanError(f"fault catalog {path}:{number}: duplicate fault name or symbol")
        symbols.add(symbol)
        entries[name] = {"name": name, "target": target, "model": model,
                         "action": actions[0] if len(actions) == 1 else action_text,
                         "actions": actions, "hit": hit, "max_hit": max_hit,
                         "symbol": symbol}
    if not entries:
        raise PlanError(f"fault catalog {path}: no fault points")
    return entries


def validate_fault_action(
    action: dict[str, Any], capabilities: dict[str, Any], context: str,
    catalog_path: Path | None = None, *, require_model: bool = True,
) -> None:
    """Validate a crash action against the named fault catalog."""
    name = action.get("fault")
    if not isinstance(name, str) or not name:
        raise PlanError(f"{context}: named fault must be a non-empty string")
    entry = fault_catalog(capabilities, catalog_path).get(name)
    if entry is None:
        raise PlanError(f"{context}: unknown fault {name!r}")
    for field in ("action", "hit"):
        if field not in action:
            raise PlanError(f"{context}: named fault {name!r} requires {field}")
    if not isinstance(action["action"], str) or not action["action"]:
        raise PlanError(f"{context}: named fault action must be a string")
    if (
        not isinstance(action["hit"], int) or isinstance(action["hit"], bool)
        or action["hit"] <= 0 or action["hit"] > 2**63 - 1
    ):
        raise PlanError(f"{context}: named fault hit must be a bounded positive integer")
    for field in ("target",) + (("model",) if require_model else ()):
        if action.get(field) != entry[field]:
            raise PlanError(
                f"{context}: fault {name!r} requires {field}={entry[field]!r}, "
                f"got {action.get(field)!r}"
            )
    action_name = action["action"]
    if action_name not in set(entry["actions"]):
        raise PlanError(
            f"{context}: fault {name!r} allows action(s)="
            f"{'|'.join(entry['actions'])!r}, "
            f"got {action_name!r}"
        )
    hit = action["hit"]
    if entry["max_hit"] == entry["hit"] and hit != entry["hit"]:
        raise PlanError(
            f"{context}: fault {name!r} requires hit={entry['hit']!r}, got {hit!r}"
        )
    if entry["max_hit"] != 0 and hit > entry["max_hit"]:
        raise PlanError(
            f"{context}: fault {name!r} hit {hit!r} exceeds {entry['max_hit']!r}"
        )


INSPECTION_RESPONSES = {
    "health": {
        "protocol_version", "page_size", "io_unit", "nchannels", "nshards",
        "admission_fence_epoch", "admission_pending_epoch", "admission_pending_lsn",
    },
    "backpressure": {
        "idle", "claimed", "request", "done", "shards",
        "wal_index_pending_bytes", "wal_index_lagging_timelines",
        "page_lag_bytes", "page_high_water_bytes", "page_catchup_bytes",
        "page_throttled", "page_throttle_enters", "page_throttle_exits",
        "page_foreground_wait_ns", "wal_lag_bytes", "wal_high_water_bytes",
        "wal_catchup_bytes", "wal_throttled", "wal_throttle_enters",
        "wal_throttle_exits", "wal_foreground_wait_ns",
        "walidx_lag_bytes", "walidx_high_water_bytes", "walidx_catchup_bytes",
        "walidx_throttled", "walidx_throttle_enters", "walidx_throttle_exits",
        "walidx_foreground_wait_ns",
        "forkmeta_lag_bytes", "forkmeta_high_water_bytes", "forkmeta_catchup_bytes",
        "forkmeta_throttled", "forkmeta_throttle_enters", "forkmeta_throttle_exits",
        "forkmeta_foreground_wait_ns",
    },
    "pruning": {
        "compactions", "versions_scanned", "versions_kept", "versions_deleted",
    },
}
INSPECTION_OPERATIONS = set(INSPECTION_RESPONSES)
PG_CONTROL_FILE_SIZE = 8192


RUNTIME_OPERATIONS = {
    "daemon_smoke": {"crash"},
    "daemon_fault_smoke": {"crash", "set_fault", "release_fault"},
    "writer_smoke": {
        "sql", "checkpoint", "prepare_reader", "reader_base", "bootstrap",
        "install_reader", "assert", "capture",
    },
    "materializer_smoke": {"sql", "checkpoint", "crash", "assert"},
}

RUNTIME_CONSTRAINTS = {
    "daemon_smoke": {
        "crash": {
            "target": ["store"], "model": ["power_loss"],
            "forbidden_fields": ["fault"],
        },
    },
    "daemon_fault_smoke": {
        "crash": {},
        "set_fault": {},
        "release_fault": {"target": ["store"]},
    },
    "writer_smoke": {
        "checkpoint": {"target": ["writer"]},
        "prepare_reader": {"target": ["writer"]},
        "reader_base": {"target": ["writer"]},
        "bootstrap": {"target": ["writer"]},
        "install_reader": {"forbidden_values": {"target": ["writer"]}},
        "assert": {"oracle": ["sql_scalar"]},
        "capture": {"target": ["writer"], "kind": ["reader_datadir"]},
    },
    "materializer_smoke": {
        "sql": {
            "target": ["writer"],
            "forbidden_fields": ["expect_error", "expect_sqlstate"],
        },
        "checkpoint": {"target": ["writer"]},
        "crash": {
            "target": ["materializer"], "model": ["compute"],
            "forbidden_fields": ["fault"],
        },
        "assert": {
            "target": ["writer", "materializer"], "oracle": ["sql_scalar"],
        },
    },
}


def pagestore_import_command(
    importer: Path, shm: str, data: Path, page_size: int,
) -> list[str]:
    return [
        str(importer), "--shm", shm, "--pgdata", str(data),
        "--page-size", str(page_size),
    ]


def pagestore_control_restore_command(
    restore: Path, shm: str, timeline: int, incarnation: int,
    read_lsn: str, data: Path,
) -> list[str]:
    return [
        str(restore), "--shm", shm, "--timeline", str(timeline),
        "--incarnation", str(incarnation), "--lsn", read_lsn, str(data),
    ]


def validate_runtime_plan(plan: Plan, capabilities: dict[str, Any], runtime: str) -> None:
    profile = runtime_capabilities(capabilities, runtime)
    for field in ("protocol_version", "page_size", "io_unit"):
        value = profile.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise PlanError(f"capabilities: runtime {runtime!r} has invalid {field}")
    if profile["page_size"] > profile["io_unit"]:
        raise PlanError(
            f"capabilities: runtime {runtime!r} page_size exceeds io_unit"
        )
    operations = profile.get("operations")
    if not isinstance(operations, list) or not all(isinstance(op, str) for op in operations):
        raise PlanError(f"capabilities: runtime {runtime!r} has an invalid operations list")
    implemented = RUNTIME_OPERATIONS.get(runtime)
    if implemented is None or set(operations) != implemented:
        raise PlanError(
            f"capabilities: runtime {runtime!r} operations do not match runner implementation"
        )
    unsupported = sorted({action["op"] for action in plan.actions} - set(operations))
    if unsupported:
        raise PlanError(
            f"runtime {runtime!r} cannot execute operation(s): {', '.join(unsupported)}"
        )
    constraints = profile.get("constraints")
    expected_constraints = RUNTIME_CONSTRAINTS.get(runtime)
    if not isinstance(constraints, dict) or constraints != expected_constraints:
        raise PlanError(
            f"capabilities: runtime {runtime!r} constraints do not match "
            "runner implementation"
        )
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
        checkpoints: dict[str, tuple[int, bool]] = {}
        reader_bases: dict[str, str] = {}
        prepared_readers: dict[str, str] = {}
        reader_seeds: dict[str, str] = {}
        latest_checkpoint: str | None = None
        writer_mutated_since_checkpoint = True
        bootstrapped = False
        for action in plan.actions:
            if action["op"] in ("sql", "assert") and action["target"] not in available_clients:
                raise PlanError(
                    f"runtime {runtime!r} operation {action['op']!r} target "
                    f"{action['target']!r} is not an available compute"
                )
            if action["op"] == "checkpoint":
                checkpoints[action["name"]] = (len(checkpoints), bootstrapped)
                latest_checkpoint = action["name"]
                writer_mutated_since_checkpoint = False
            elif action["op"] == "reader_base":
                checkpoint = action["checkpoint"]
                if not checkpoint.startswith("$") or checkpoint[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} reader_base requires an earlier checkpoint"
                    )
                if not checkpoints[checkpoint[1:]][1]:
                    raise PlanError(
                        f"runtime {runtime!r} reader_base requires bootstrap before "
                        f"checkpoint {checkpoint!r}"
                    )
                if (
                    checkpoint != f"${latest_checkpoint}"
                    or writer_mutated_since_checkpoint
                ):
                    raise PlanError(
                        f"runtime {runtime!r} reader_base checkpoint {checkpoint!r} "
                        "does not describe the current unmodified writer"
                    )
                reader_bases[action["name"]] = checkpoint[1:]
                writer_mutated_since_checkpoint = True
            elif action["op"] == "prepare_reader":
                base = action["base"]
                read_lsn = action["read_lsn"]
                base_checkpoint = reader_bases.get(base[1:]) if base.startswith("$") else None
                if base_checkpoint is None:
                    raise PlanError(
                        f"runtime {runtime!r} prepare_reader requires an earlier reader_base"
                    )
                if not read_lsn.startswith("$") or read_lsn[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} prepare_reader requires an earlier checkpoint read_lsn"
                    )
                if not checkpoints[read_lsn[1:]][1]:
                    raise PlanError(
                        f"runtime {runtime!r} prepare_reader requires bootstrap before "
                        f"checkpoint {read_lsn!r}"
                    )
                if checkpoints[base_checkpoint][0] > checkpoints[read_lsn[1:]][0]:
                    raise PlanError(
                        f"runtime {runtime!r} prepare_reader base {base!r} is newer "
                        f"than read_lsn {read_lsn!r}"
                    )
                prepared_readers[action["id"]] = read_lsn
                writer_mutated_since_checkpoint = True
            elif action["op"] == "capture":
                horizon = action["horizon"]
                if not horizon.startswith("$") or horizon[1:] not in checkpoints:
                    raise PlanError(
                        f"runtime {runtime!r} capture requires an earlier checkpoint horizon"
                    )
                if not checkpoints[horizon[1:]][1]:
                    raise PlanError(
                        f"runtime {runtime!r} capture requires bootstrap before "
                        f"checkpoint {horizon!r}"
                    )
                if horizon != f"${latest_checkpoint}" or writer_mutated_since_checkpoint:
                    raise PlanError(
                        f"runtime {runtime!r} capture horizon {horizon!r} does not "
                        "describe the current writer data directory"
                    )
                if action["name"] in reader_seeds:
                    raise PlanError(
                        f"runtime {runtime!r} reader_datadir capture name "
                        f"{action['name']!r} is already used"
                    )
                reader_seeds[action["name"]] = horizon
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
                seed_horizon = reader_seeds.get(action["target"])
                if seed_horizon is None:
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
                if read_lsn != seed_horizon:
                    raise PlanError(
                        f"runtime {runtime!r} install_reader read_lsn {read_lsn!r} "
                        f"does not match reader seed horizon {seed_horizon!r}"
                    )
                if not bootstrapped:
                    raise PlanError(
                        f"runtime {runtime!r} install_reader requires an earlier bootstrap"
                    )
                available_clients.add(action["target"])
                writer_mutated_since_checkpoint = True
            elif action["op"] == "bootstrap":
                bootstrapped = True
                writer_mutated_since_checkpoint = True
            elif action["op"] in ("sql", "assert") and action["target"] == "writer":
                writer_mutated_since_checkpoint = True
    elif runtime == "materializer_smoke":
        required = {"writer", "materializer"}
        computes = plan.header["case"]["compute"]
        if len(computes) != len(required) or set(computes) != required:
            raise PlanError(
                f"runtime {runtime!r} requires exactly writer and materializer computes"
            )
    elif runtime == "daemon_fault_smoke":
        named = [
            action for action in plan.actions
            if action["op"] in ("crash", "set_fault") and "fault" in action
        ]
        if len(named) != 1 or any(
            action["op"] == "crash" and "fault" not in action
            for action in plan.actions
        ):
            raise PlanError(
                "runtime daemon_fault_smoke requires exactly one named fault action"
            )
        fault = named[0]
        releases = [
            action for action in plan.actions
            if action["op"] == "release_fault" and action["fault"] == fault["fault"]
        ]
        if fault["action"] == "pause" and len(releases) != 1:
            raise PlanError(
                "runtime daemon_fault_smoke requires one release_fault for a pause"
            )
        if fault["action"] != "pause" and releases:
            raise PlanError("runtime daemon_fault_smoke only releases pause faults")


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


def pagestore_build_program(build: Path, name: str) -> Path:
    program = build / "contrib" / "pagestore" / name
    if not program.is_file() or not os.access(program, os.X_OK):
        raise PlanError(
            f"PostgreSQL build {build} does not provide executable {name!r}"
        )
    return program


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


def require_safe_component(record: dict[str, Any], field: str, context: str) -> str:
    value = require_string(record, field, context)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
        raise PlanError(f"{context}: {field} must be a safe path component")
    return value


def reject_unknown_fields(record: dict[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(record) - allowed)
    if unknown:
        raise PlanError(f"{context}: unknown field(s): {', '.join(unknown)}")
    extra = record.get("extra")
    if extra is not None and not isinstance(extra, dict):
        raise PlanError(f"{context}: extra must be an object")


def validate_fault_identity(value: str, field: str, context: str) -> None:
    """Match the bounded, directly JSON-embeddable C fault identity contract."""
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise PlanError(f"{context}: {field} is not valid UTF-8") from error
    if (
        not encoded or len(encoded) > 128 or '"' in value or "\\" in value
        or any(ord(character) < 0x20 for character in value)
    ):
        raise PlanError(
            f"{context}: {field} must be a 1..128-byte fault identity without "
            "quotes, backslashes, or control characters"
        )


def validate_plan(
    plan: Plan, capabilities: dict[str, Any], catalog_path: Path | None = None,
) -> None:
    header = plan.header
    context = str(plan.path)
    reject_unknown_fields(header, HEADER_FIELDS, context)
    if header.get("schema") != 1:
        raise PlanError(f"{context}: header schema must be 1")
    require_string(header, "scenario", context)
    seed = header.get("seed")
    if (
        not isinstance(seed, int) or isinstance(seed, bool)
        or seed < 0 or seed > 2**63 - 1
    ):
        raise PlanError(f"{context}: seed must be a bounded non-negative integer")
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
    armed_fault_actions: dict[str, str] = {}
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
        require_safe_component(action, "id", action_context)
        if action_id in action_ids:
            raise PlanError(f"{action_context}: duplicate action id {action_id!r}")
        action_ids.add(action_id)
        for field in REQUIRED_FIELDS[operation]:
            if field not in action:
                raise PlanError(f"{action_context}: missing required field {field!r}")
        for field in REQUIRED_FIELDS[operation] - {"lanes", "steps", "expect", "hit"}:
            require_string(action, field, action_context)
        if operation == "assert" and not isinstance(action["expect"], str):
            raise PlanError(f"{action_context}: expect must be a string")
        if "target" in action:
            require_safe_component(action, "target", action_context)
        for field in SAFE_COMPONENT_FIELDS.get(operation, set()):
            require_safe_component(action, field, action_context)
        if operation == "sql":
            if "expect_error" in action:
                require_string(action, "expect_error", action_context)
            if "expect_sqlstate" in action:
                sqlstate = require_string(action, "expect_sqlstate", action_context)
                if re.fullmatch(r"[0-9A-Z]{5}", sqlstate) is None:
                    raise PlanError(
                        f"{action_context}: expect_sqlstate must be five characters"
                    )
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
            if "fault" in action:
                validate_fault_identity(header["scenario"], "scenario", context)
                validate_fault_action(action, capabilities, action_context, catalog_path)
                armed_fault_actions[action["fault"]] = action["action"]
            elif "action" in action or "hit" in action:
                raise PlanError(
                    f"{action_context}: crash action/hit fields require a named fault"
                )
        elif operation == "set_fault":
            name = require_string(action, "fault", action_context)
            validate_fault_identity(header["scenario"], "scenario", context)
            validate_fault_action(
                action, capabilities, action_context, catalog_path, require_model=False,
            )
            armed_fault_actions[name] = action["action"]
        elif operation == "release_fault":
            name = require_string(action, "fault", action_context)
            entry = fault_catalog(capabilities, catalog_path).get(name)
            if entry is None:
                raise PlanError(f"{action_context}: unknown fault {name!r}")
            if action.get("target") != entry["target"]:
                raise PlanError(
                    f"{action_context}: fault {name!r} requires target={entry['target']!r}, "
                    f"got {action.get('target')!r}"
                )
            if armed_fault_actions.get(name) != "pause":
                raise PlanError(
                    f"{action_context}: release_fault requires an earlier pause action "
                    f"for fault {name!r}"
                )
        if operation == "advance":
            steps = action["steps"]
            if not isinstance(steps, int) or isinstance(steps, bool) or steps <= 0:
                raise PlanError(f"{action_context}: steps must be a positive integer")
        if operation in ("crash", "set_fault") and "timeout" in action:
            timeout = action["timeout"]
            if (
                not isinstance(timeout, (int, float))
                or isinstance(timeout, bool)
                or not math.isfinite(timeout)
                or timeout <= 0
            ):
                raise PlanError(f"{action_context}: timeout must be finite and positive")


def plan_files(directory: Path) -> Iterable[Path]:
    return sorted(path for path in directory.rglob("*.jsonl") if path.is_file())


def validate_paths(
    paths: Iterable[Path], capabilities: dict[str, Any], catalog_path: Path | None = None,
) -> list[PlanError]:
    errors: list[PlanError] = []
    for path in paths:
        try:
            validate_plan(read_plan(path), capabilities, catalog_path)
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
                   "--page-size", str(runtime_capabilities(
                       capabilities, "daemon_smoke")["page_size"]),
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


def _atomic_arm_marker(path: Path) -> None:
    """Create a fault arm marker exactly once, without a truncate race."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        os.write(fd, b"armed\n")
    finally:
        os.close(fd)


def _atomic_release_marker(path: Path) -> None:
    """Create the regular-file marker used to release a paused fault."""
    _atomic_arm_marker(path)


def _cleanup_fault_control(control: Path) -> None:
    """Remove only a real control directory; preserve other diagnostics."""
    try:
        control_stat = control.lstat()
    except (FileNotFoundError, OSError):
        return
    if not stat.S_ISDIR(control_stat.st_mode):
        return
    for child in list(control.iterdir()):
        try:
            child_stat = child.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISDIR(child_stat.st_mode) and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)
    try:
        control.rmdir()
    except OSError:
        pass


def _disarm_fault_control(control: Path) -> None:
    """Remove only the arm marker while retaining failure diagnostics."""
    try:
        control_stat = control.lstat()
    except (FileNotFoundError, OSError):
        return
    if not stat.S_ISDIR(control_stat.st_mode):
        return
    marker = control / "arm"
    try:
        marker_stat = marker.lstat()
    except (FileNotFoundError, OSError):
        return
    if not stat.S_ISDIR(marker_stat.st_mode):
        try:
            marker.unlink(missing_ok=True)
        except OSError:
            pass


def _fault_report(
    path: Path, expected_name: str, expected_hit: int, expected_pid: int,
    expected_action: str = "crash", expected_scenario: str | None = None,
    expected_seed: int | None = None, expected_operation: str | None = None,
    expected_state: str | None = None,
) -> dict[str, Any]:
    try:
        file_stat = path.lstat()
    except OSError as error:
        raise FaultNotReached(f"fault report {path} is missing: {error}") from error
    if path.is_symlink() or not stat.S_ISREG(file_stat.st_mode):
        raise FaultNotReached(f"fault report {path} is not a regular file")
    try:
        if file_stat.st_size <= 0 or file_stat.st_size > FAULT_REPORT_MAX_BYTES:
            raise ValueError("report size is outside the bounded schema")
        lines = path.read_text(encoding="utf-8").splitlines()
        if len(lines) != 1 or not lines[0].strip():
            raise ValueError("expected exactly one JSON line")
        value = json.loads(lines[0])
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise FaultNotReached(f"fault report {path} is missing or invalid: {error}") from error
    if not isinstance(value, dict):
        raise FaultNotReached("fault report must be an object")
    legacy_keys = {"schema", "name", "action", "hit", "pid"}
    extended_keys = FAULT_REPORT_FIELDS
    extended_name_keys = (extended_keys - {"fault"}) | {"name"}
    extended_both_keys = extended_keys | {"name"}
    pause_suffix = {"state", "watchdog_ms"}
    allowed_key_sets = {
        frozenset(legacy_keys), frozenset(extended_keys),
        frozenset(extended_name_keys), frozenset(extended_both_keys),
    }
    if expected_action == "pause":
        allowed_key_sets |= {frozenset(keys | pause_suffix) for keys in (
            extended_keys, extended_name_keys, extended_both_keys,
        )}
    if frozenset(value) not in allowed_key_sets:
        raise FaultNotReached("fault report has unexpected keys")
    fault_name = value.get("fault", value.get("name"))
    if (
        value["schema"] != 1 or isinstance(value["schema"], bool)
        or fault_name != expected_name or value["action"] != expected_action
        or value["hit"] != expected_hit or isinstance(value["hit"], bool)
        or not isinstance(value["hit"], int) or value["hit"] <= 0
        or value["hit"] > 2**63 - 1
        or not isinstance(value["pid"], int) or isinstance(value["pid"], bool)
        or value["pid"] <= 0 or value["pid"] != expected_pid
        or value["pid"] > 2**31 - 1
    ):
        raise FaultNotReached("fault report fields do not match expected fault")
    is_extended = "scenario" in value
    if is_extended:
        if (
            expected_scenario is not None and value["scenario"] != expected_scenario
            or expected_seed is not None and value["seed"] != expected_seed
            or expected_operation is not None and value["operation"] != expected_operation
        ):
            raise FaultNotReached("fault report metadata does not match expected operation")
        if expected_action == "pause":
            state = expected_state or "reached"
            watchdog_ms = value.get("watchdog_ms")
            if (
                value.get("state") != state or isinstance(watchdog_ms, bool)
                or not isinstance(watchdog_ms, int)
                or (state == "reached" and watchdog_ms != 0)
                or (state == "timeout" and watchdog_ms <= 0)
            ):
                raise FaultNotReached(
                    f"pause fault report does not describe the {state} state"
                )
    elif expected_action != "crash":
        raise FaultNotReached(
            f"fault report for action {expected_action!r} lacks extended metadata"
        )
    return value


def _capture_fault_diagnostics(
    root: Path, control: Path, daemon_log: Path, *, reason: str,
    scenario: str, seed: int, fault: str, action: str, hit: int,
    operation: str, process: subprocess.Popen[str] | None,
) -> Path:
    diagnostics = {
        "schema": 1, "reason": reason, "scenario": scenario, "seed": seed,
        "fault": fault, "action": action, "hit": hit, "hit_count": hit,
        "operation": operation, "operation_id": operation,
        "pid": process.pid if process is not None else None,
        "returncode": process.poll() if process is not None else None,
        "control": str(control), "control_entries": [], "daemon_log": str(daemon_log),
    }
    try:
        entries = []
        if control.is_dir() and not control.is_symlink():
            for child in sorted(control.iterdir()):
                try:
                    child_stat = child.lstat()
                except OSError as error:
                    entries.append({"name": child.name, "error": str(error)})
                    continue
                entries.append({
                    "name": child.name, "mode": stat.S_IFMT(child_stat.st_mode),
                    "size": child_stat.st_size, "regular": stat.S_ISREG(child_stat.st_mode),
                    "symlink": stat.S_ISLNK(child_stat.st_mode),
                })
        diagnostics["control_entries"] = entries
        (root / "fault-diagnostics.json").write_text(
            json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError as error:
        diagnostics["write_error"] = str(error)
        try:
            (root / "fault-diagnostics.json").write_text(
                json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        except OSError:
            pass
    return root / "fault-diagnostics.json"


def run_daemon_fault_recovery(
    plan: Plan,
    capabilities: dict[str, Any],
    inspection_schema: dict[str, Any],
    daemon: Path,
    inspector: Path,
    requested_root: Path | None,
    keep: bool,
    capabilities_path: Path | None = None,
    rerun_command: list[str] | None = None,
    timeout: float = 15.0,
) -> Path:
    """Run one pre-armed named daemon fault and prove recovery is idempotent."""
    daemon = daemon.resolve()
    inspector = inspector.resolve()
    validate_plan(plan, capabilities, capabilities_path)
    validate_runtime_plan(plan, capabilities, "daemon_fault_smoke")
    root, temporary = run_root(requested_root)
    # The C fault registry rejects relative control paths. Resolve before
    # deriving store, trace, bundle, and control paths; ordinary daemon smoke
    # keeps its historical run_root semantics.
    root = root.resolve()
    trace = root / "trace"
    store = root / "store"
    control = root / "fault-control"
    trace.mkdir()
    store.mkdir()
    shutil.copy2(plan.path, root / "plan.jsonl")
    (root / "case.json").write_text(
        json.dumps(plan.header["case"], indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    catalog_path = fault_catalog_path(capabilities, capabilities_path)
    bundle_catalog = root / "catalog" / catalog_path.name
    bundle_catalog.parent.mkdir()
    shutil.copy2(catalog_path, bundle_catalog)
    bundle_capabilities = root / "capabilities.json"
    bundle_value = dict(capabilities)
    bundle_value[FAULT_CATALOG_KEY] = f"catalog/{catalog_path.name}"
    bundle_capabilities.write_text(
        json.dumps(bundle_value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    bundle_inspection_schema = root / "inspection_schema.json"
    bundle_inspection_schema.write_text(
        json.dumps(inspection_schema, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    scenario = plan.header["scenario"]
    seed = plan.header["seed"]
    named_faults = [
        item for item in plan.actions
        if item["op"] in ("crash", "set_fault") and "fault" in item
    ]
    if len(named_faults) != 1 or any(
        item["op"] == "crash" and "fault" not in item for item in plan.actions
    ):
        raise PlanError("daemon fault recovery requires exactly one named fault action")
    action = named_faults[0]
    validate_fault_action(
        action, capabilities, f"{plan.path}:{action['id']}", capabilities_path,
        require_model=action["op"] == "crash",
    )
    fault_name = action["fault"]
    catalog_entry = fault_catalog(capabilities, capabilities_path)[fault_name]
    fault_action = action["action"]
    fault_hit = action["hit"]
    fault_model = action.get("model", catalog_entry["model"])
    release_actions = [
        item for item in plan.actions
        if item["op"] == "release_fault" and item["fault"] == fault_name
    ]
    if fault_action == "pause" and len(release_actions) != 1:
        raise PlanError("daemon fault pause requires exactly one release_fault action")
    if fault_action != "pause" and release_actions:
        raise PlanError("release_fault is only valid for a pause fault")
    events = EventLog(
        trace / "events.jsonl",
        {"scenario": scenario, "seed": seed, "fault": fault_name,
         "action": fault_action, "hit": fault_hit, "hit_count": fault_hit,
         "operation": action["id"], "operation_id": action["id"]},
    )
    (root / "run.json").write_text(
        json.dumps({"schema": 1, "scenario": scenario, "seed": seed,
                    "fault": fault_name, "action": fault_action,
                    "hit": fault_hit, "hit_count": fault_hit,
                    "operation": action["id"], "operation_id": action["id"]},
                   indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    generation = 0  # next generation; fault, recovery, clean restart: 0, 1, 2
    active_generation = 0
    process: subprocess.Popen[str] | None = None
    shm_names: list[str] = []
    shm_base = f"/psharness_{os.getpid()}_{time.monotonic_ns()}"
    shm = ""
    daemon_log = trace / "daemon.log"
    marker = control / "arm"
    report = control / "report.jsonl"
    release = control / "release"
    failure: Exception | None = None
    current_action_id: str | None = None

    def emit(event: str, **fields: Any) -> None:
        fields.setdefault("scenario", scenario)
        fields.setdefault("seed", seed)
        fields.setdefault("action_id", current_action_id)
        fields.setdefault("generation", active_generation)
        fields.setdefault("fault", fault_name)
        fields.setdefault("action", fault_action)
        fields.setdefault("hit", fault_hit)
        fields.setdefault("hit_count", fault_hit)
        fields.setdefault("operation", current_action_id)
        fields.setdefault("operation_id", current_action_id)
        events.emit(event, **fields)

    def start_daemon(inject_fault: bool, action_id: str | None = None) -> subprocess.Popen[str]:
        nonlocal generation, active_generation, process, shm
        this_generation = generation
        generation += 1
        active_generation = this_generation
        shm = f"{shm_base}_{this_generation}"
        shm_names.append(shm)
        command = [
            str(daemon), "--shm", shm, "--store", str(store),
            "--page-size", str(runtime_capabilities(capabilities, "daemon_fault_smoke")["page_size"]),
            "--nshards", str(plan.header["case"]["shards"]),
            "--storage", plan.header["case"]["storage"],
        ]
        env = private_environment()
        if inject_fault:
            # Keep these names local and explicit: inherited PAGESTORE_* values
            # are removed by private_environment before this point.
            env.update({
                "PAGESTORE_TEST_FAULT_NAME": fault_name,
                "PAGESTORE_TEST_FAULT_ACTION": fault_action,
                "PAGESTORE_TEST_FAULT_HIT": str(fault_hit),
                "PAGESTORE_TEST_FAULT_DIR": str(control),
                "PAGESTORE_TEST_FAULT_RELEASE": str(release),
                "PAGESTORE_TEST_FAULT_SCENARIO": scenario,
                "PAGESTORE_TEST_FAULT_SEED": str(seed),
                "PAGESTORE_TEST_FAULT_OPERATION": str(action["id"]),
            })
        with daemon_log.open("a", encoding="utf-8") as log:
            daemon_process = subprocess.Popen(
                command, stdout=log, stderr=subprocess.STDOUT, text=True,
                env=env, start_new_session=True,
            )
        process = daemon_process
        emit("process_start", target="store", pid=daemon_process.pid,
             argv=command, action_id=action_id, generation=this_generation)
        return daemon_process

    def stop_daemon() -> None:
        if process is None or process.poll() is not None:
            return
        signal_process_group(process, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            signal_process_group(process, signal.SIGKILL)
            process.wait(timeout=5)

    def wait_ready(daemon_process: subprocess.Popen[str]) -> dict[str, Any]:
        deadline = time.monotonic() + min(10.0, timeout)
        last_error: PlanError | None = None
        while time.monotonic() < deadline:
            if daemon_process.poll() is not None:
                raise UnexpectedExit(
                    "daemon exited during recovery readiness with status "
                    f"{daemon_process.returncode}"
                )
            try:
                health = inspect_store(inspector, shm, "health", inspection_schema)
                validate_runtime_health(
                    plan, capabilities, "daemon_fault_smoke", health,
                    inspection_schema,
                )
                return health
            except PlanError as error:
                last_error = error
                time.sleep(0.05)
        raise HarnessTimeout(
            f"timeout while waiting for recovered daemon readiness: {last_error}"
        )

    try:
        if control.exists() or control.is_symlink():
            raise PlanError(f"fault control path is stale: {control}")
        control.mkdir(mode=0o700)
        if any(control.iterdir()):
            raise PlanError(f"fault control path is not fresh: {control}")
        _atomic_arm_marker(marker)
        daemon_log.touch()
        emit("run_start", shm_base=shm_base)
        current_action_id = action["id"]
        emit("fault_arm", target="store", name=fault_name, hit=fault_hit)
        process = start_daemon(True, action["id"])
        deadline = time.monotonic() + float(action.get("timeout", timeout))
        if fault_action == "crash":
            while process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            if process.poll() is None:
                raise FaultNotReached(
                    f"deadline waiting for fault {fault_name!r} expired; expected crash was unhit"
                )
            if process.returncode != 88:
                exit_status = process.returncode
                emit("crash_exit", target="store", name=fault_name,
                     returncode=process.returncode)
                emit("process_stop", target="store", pid=process.pid, name=fault_name,
                     returncode=process.returncode)
                process = None
                raise UnexpectedExit(
                    f"fault {fault_name!r} exited with status {exit_status}, expected 88"
                )
            fault_process = process
            process = None
            emit("crash_exit", target="store", name=fault_name,
                 returncode=fault_process.returncode)
            emit("process_stop", target="store", pid=fault_process.pid, name=fault_name,
                 returncode=fault_process.returncode)
            result = _fault_report(
                report, fault_name, fault_hit, fault_process.pid, fault_action,
                scenario, seed, action["id"],
            )
            shutil.copy2(report, trace / "fault-report.jsonl")
            emit("fault", target="store", name=fault_name, model=fault_model,
                 returncode=fault_process.returncode, report=result, reached=True)
            remove_shm(shm)
            shutil.rmtree(control)
        else:
            while time.monotonic() < deadline:
                if report.exists():
                    break
                if process.poll() is not None:
                    raise FaultNotReached(
                        f"fault {fault_name!r} exited before its {fault_action} reached report"
                    )
                time.sleep(0.02)
            if not report.exists():
                _capture_fault_diagnostics(
                    root, control, daemon_log,
                    reason="fault watchdog expired before reached report",
                    scenario=scenario, seed=seed, fault=fault_name,
                    action=fault_action, hit=fault_hit, operation=action["id"],
                    process=process,
                )
                raise FaultNotReached(
                    f"fault {fault_name!r} watchdog expired; expected {fault_action} was unhit"
                )
            if fault_action == "pause" and process.poll() == 90:
                _fault_report(
                    report, fault_name, fault_hit, process.pid, fault_action,
                    scenario, seed, action["id"], "timeout",
                )
                raise HarnessTimeout(
                    f"fault {fault_name!r} pause watchdog expired before release"
                )
            if process.poll() is not None and not expected_error_exit(
                fault_action, process.returncode,
            ):
                raise UnexpectedExit(
                    f"fault {fault_name!r} {fault_action} report arrived after daemon exit"
                )
            result = _fault_report(
                report, fault_name, fault_hit, process.pid, fault_action,
                scenario, seed, action["id"],
            )
            shutil.copy2(report, trace / "fault-report.jsonl")
            emit("fault_reached", target="store", name=fault_name, model=fault_model,
                 report=result, reached=True)
            if fault_action == "error":
                while process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                if process.poll() is None:
                    raise HarnessTimeout(
                        f"fault {fault_name!r} returned an error but daemon did not exit"
                    )
                if process.returncode != 1:
                    raise UnexpectedExit(
                        f"fault {fault_name!r} error exited with status "
                        f"{process.returncode}, expected 1"
                    )
                emit("error_exit", target="store", name=fault_name,
                     returncode=process.returncode)
                emit("process_stop", target="store", pid=process.pid,
                     returncode=process.returncode)
                process = None
            else:
                current_action_id = release_actions[0]["id"]
                _atomic_release_marker(release)
                emit("fault_release", target="store", name=fault_name,
                     release_marker=str(release), released=True)
                release_deadline = min(deadline + 1.0, time.monotonic() + 1.0)
                while process.poll() is None and time.monotonic() < release_deadline:
                    time.sleep(0.02)
                if process.poll() is not None:
                    if process.returncode == 90:
                        _fault_report(
                            report, fault_name, fault_hit, process.pid, fault_action,
                            scenario, seed, action["id"], "timeout",
                        )
                        raise HarnessTimeout(
                            f"fault {fault_name!r} pause watchdog expired during release"
                        )
                    raise UnexpectedExit(
                        f"fault {fault_name!r} exited after release marker with status "
                        f"{process.returncode}"
                    )
                current_action_id = action["id"]
                stop_daemon()
                emit("process_stop", target="store", pid=process.pid,
                     returncode=process.returncode)
                process = None
            remove_shm(shm)
            shutil.rmtree(control)
        # Recovery is intentionally followed by one additional clean restart.
        process = start_daemon(False, action["id"])
        health = wait_ready(process)
        emit("recovered", target="store", health=health)
        stop_daemon()
        emit("process_stop", target="store", pid=process.pid,
             returncode=process.returncode)
        remove_shm(shm)
        process = start_daemon(False, action["id"])
        health = wait_ready(process)
        emit("restarted", target="store", health=health)
        emit("run_pass")
    except Exception as error:
        failure = error
        classification = fault_failure_classification(error)
        if fault_action in ("error", "pause") and not (root / "fault-diagnostics.json").exists():
            _capture_fault_diagnostics(
                root, control, daemon_log, reason=str(error), scenario=scenario,
                seed=seed, fault=fault_name, action=fault_action, hit=fault_hit,
                operation=action["id"], process=process,
            )
        emit("run_fail", error=str(error), error_type=type(error).__name__,
             classification=classification)
        metadata = {
            "schema": 1,
            "classification": classification,
            "error_type": type(error).__name__,
            "error": str(error),
            "scenario": scenario, "seed": seed, "fault": fault_name,
            "action": fault_action, "hit": fault_hit, "hit_count": fault_hit,
            "operation": action["id"], "operation_id": action["id"],
            "run_root": str(root),
            "plan": str(root / "plan.jsonl"),
            "capabilities": str(bundle_capabilities),
            "catalog": str(bundle_catalog),
            "inspection_schema": str(bundle_inspection_schema),
            "binaries": {"daemon": str(daemon), "inspector": str(inspector)},
            "command": [
                sys.executable, str(Path(__file__).resolve()),
                "--capabilities", str(bundle_capabilities),
                "--inspection-schema", str(bundle_inspection_schema),
                "--daemon-fault-recovery", str(root / "plan.jsonl"),
                "--daemon-binary", str(daemon), "--inspect-binary", str(inspector),
                "--run-root", str(root.parent / f"{root.name}.rerun"), "--keep",
            ],
        }
        (root / "failure.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    finally:
        if process is not None:
            stop_daemon()
            emit("process_stop", target="store", pid=process.pid,
                 returncode=process.returncode)
        for name in shm_names:
            remove_shm(name)
        if failure is None:
            _cleanup_fault_control(control)
        else:
            _disarm_fault_control(control)

    if failure is not None:
        raise PlanError(f"daemon fault recovery failed; failure bundle: {root}: {failure}") from failure
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
    if any(action["op"] == "bootstrap" for action in plan.actions):
        validate_postgres_relation_segment_size(
            build,
            runtime_capabilities(capabilities, "writer_smoke")["page_size"],
        )
    if (
        any(action["op"] == "install_reader" for action in plan.actions)
        and runtime_capabilities(capabilities, "writer_smoke")["page_size"]
        < PG_CONTROL_FILE_SIZE
    ):
        raise PlanError(
            "writer runtime page size cannot hold a PostgreSQL control file "
            "required for reader installation"
        )
    control_restore = None
    if any(action["op"] == "install_reader" for action in plan.actions):
        control_restore = pagestore_build_program(build, "pagestore_control_restore")
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
                                     "--page-size", str(runtime_capabilities(
                                         capabilities, "writer_smoke")["page_size"]),
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
        if any(action["op"] == "reader_base" for action in plan.actions):
            setup = (
                "CREATE OR REPLACE FUNCTION "
                "pagestore_ship_slru_snapshot(text, pg_lsn) RETURNS void "
                "AS 'pagestore','pagestore_ship_slru_snapshot' LANGUAGE C STRICT;"
            )
            subprocess.run(
                [str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port),
                 "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", setup],
                check=True, capture_output=True, encoding="utf-8", env=env,
            )
        checkpoints: dict[str, dict[str, str]] = {}
        reader_bases: dict[str, str] = {}
        prepared_readers: dict[str, Path] = {}
        reader_seeds: dict[str, Path] = {}
        reader_clients: dict[str, tuple[Path, int]] = {}
        reader_data_dirs: dict[str, Path] = {}
        reader_owner_ids: dict[str, int] = {}
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
                sql = "; ".join(f"SELECT pagestore_ship_slru_snapshot('{name}', '{base}')" for name in ("pg_xact", "pg_commit_ts", "pg_multixact/offsets", "pg_multixact/members"))
            elif action["op"] == "bootstrap":
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                importer = daemon.parent / "pagestore_import"
                subprocess.run(pagestore_import_command(
                    importer, shm, data,
                    runtime_capabilities(capabilities, "writer_smoke")["page_size"],
                ), check=True, capture_output=True, encoding="utf-8", env=env)
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
                assert control_restore is not None
                # The harness reader fixture is rooted at timeline 0,
                # incarnation 1.  Pass that immutable identity explicitly;
                # control_restore must never infer it from mutable daemon
                # state.
                subprocess.run(
                    pagestore_control_restore_command(
                        control_restore, shm, 0, 1, lsn, reader_data,
                    ),
                    check=True, capture_output=True, encoding="utf-8", env=env,
                )
                setup = """CREATE OR REPLACE FUNCTION pagestore_install_prepared_reader(text, text, int, pg_lsn) RETURNS void AS 'pagestore','pagestore_install_prepared_reader' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION pagestore_mark_reader_catalog_snapshot(text, int, pg_lsn) RETURNS void AS 'pagestore','pagestore_mark_reader_catalog_snapshot' LANGUAGE C STRICT;"""
                subprocess.run([str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port), "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", setup], check=True, capture_output=True, encoding="utf-8", env=env)
                reader_data_sql = str(reader_data).replace(chr(39), chr(39) * 2)
                sql = f"SELECT pagestore_mark_reader_catalog_snapshot('{reader_data_sql}', 0, '{lsn}'); SELECT pagestore_install_prepared_reader('{str(prepared).replace(chr(39), chr(39) * 2)}', '{reader_data_sql}', 0, '{lsn}')"
                result = subprocess.run([str(pg_bin / "psql"), "-h", str(sockdir), "-p", str(port), "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql], check=True, capture_output=True, encoding="utf-8", env=env)
                reader_port = free_port()
                owner_id = reader_owner_ids.setdefault(
                    action["target"], 10000 + len(reader_owner_ids) + 1
                )
                (reader_data / "postgresql.conf").open("a", encoding="utf-8").write(
                    f"pagestore.read_lsn = '{lsn}'\n"
                    "pagestore.retention_owner_generation = '1'\n"
                    f"pagestore.retention_owner_id = '{owner_id}'\n"
                    "pagestore.route_all = on\n"
                    "archive_mode = off\n"
                    "listen_addresses = ''\n"
                    f"unix_socket_directories = '{reader_socket}'\n"
                    f"port = {reader_port}\n"
                )
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
            dproc.terminate()
            dproc.wait(timeout=5)
        remove_shm(shm)
    if temporary and not keep:
        shutil.rmtree(root)
    return root


def run_materializer_smoke(
    plan: Plan, capabilities: dict[str, Any], schema: dict[str, Any],
    daemon: Path, inspector: Path, supervisor: Path, build: Path,
    requested_root: Path | None, keep: bool,
) -> Path:
    """Provision a WAL-only pair and exercise its materializer supervisor."""
    pg_bin = find_pg_bin(build)
    postgres_major = validate_postgres_runtime(pg_bin / "postgres", capabilities)
    if postgres_major < 15:
        raise PlanError(
            "materializer_smoke requires PostgreSQL 15 or newer: "
            "archive_library is unavailable on older releases"
        )
    profile = runtime_capabilities(capabilities, "materializer_smoke")
    recovery_settings = (
        "recovery_prefetch = try\n" if postgres_major >= 15 else ""
    )
    validate_postgres_block_size(build, profile["page_size"])
    validate_postgres_relation_segment_size(build, profile["page_size"])
    walrestore = pagestore_build_program(build, "pagestore_walrestore").resolve()
    importer = pagestore_build_program(build, "pagestore_import")
    root, temporary = run_root(requested_root)
    root = root.resolve()
    trace = root / "trace"
    store = root / "store"
    writer_data = root / "computes" / "writer"
    materializer_data = root / "computes" / "materializer"
    writer_socket = root / "socket" / "writer"
    materializer_socket = root / "socket" / "materializer"
    supervisor_state = root / "control" / "materializer"
    artifacts = root / "artifacts" / "checkpoints"
    for path in (
        trace, store, writer_data, materializer_data, writer_socket,
        materializer_socket, supervisor_state, artifacts,
    ):
        path.mkdir(parents=True, exist_ok=True)
    shutil.copy2(plan.path, root / "plan.jsonl")
    (root / "case.json").write_text(
        json.dumps(plan.header["case"], indent=2) + "\n", encoding="utf-8"
    )
    events = EventLog(trace / "events.jsonl")
    shm = f"/psharness_materializer_{os.getpid()}_{time.monotonic_ns()}"
    writer_port = free_port()
    materializer_port = free_port()
    env = private_environment()
    install = pg_bin.parent
    env["LD_LIBRARY_PATH"] = f"{install / 'lib'}:{install / 'lib64'}"
    dproc: subprocess.Popen[str] | None = None
    supervisor_proc: subprocess.Popen[str] | None = None
    materializer_generation = 0
    materializer_retention_generation = 0

    def sql_result(socket_dir: Path, port: int, sql: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(pg_bin / "psql"), "-h", str(socket_dir), "-p", str(port),
                "-U", "postgres", "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql,
            ],
            check=True, capture_output=True, encoding="utf-8", env=env,
        )

    def sql_scalar(socket_dir: Path, port: int, sql: str) -> str:
        return sql_result(socket_dir, port, sql).stdout.strip()

    def wait_scalar(
        socket_dir: Path, port: int, sql: str, expected: str, context: str,
        timeout: float = 40,
    ) -> str:
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            try:
                last = sql_scalar(socket_dir, port, sql)
            except subprocess.CalledProcessError as error:
                last = (error.stderr or error.stdout or "").strip()
            if last == expected:
                return last
            time.sleep(.1)
        raise PlanError(f"{context}: got {last!r}, expected {expected!r}")

    def read_supervisor_status() -> dict[str, Any]:
        try:
            value = json.loads(
                (supervisor_state / "status.json").read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def wait_supervisor_status(
        predicate: Callable[[dict[str, Any]], bool], context: str,
        timeout: float = 40,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            if supervisor_proc is not None and supervisor_proc.poll() is not None:
                raise PlanError(
                    f"{context}: supervisor exited with status "
                    f"{supervisor_proc.returncode}"
                )
            last = read_supervisor_status()
            if predicate(last):
                return last
            time.sleep(.05)
        raise PlanError(f"{context}: last supervisor status {last!r}")

    def sync_materializer_generation(status: dict[str, Any]) -> int:
        nonlocal materializer_generation, materializer_retention_generation
        generation = status.get("worker_generation")
        retention_generation = status.get("retention_generation")
        if not isinstance(generation, int) or generation <= 0:
            raise PlanError(f"invalid supervisor worker generation: {status!r}")
        if not isinstance(retention_generation, int) or retention_generation <= 0:
            raise PlanError(f"invalid supervisor retention generation: {status!r}")
        materializer_generation = generation
        materializer_retention_generation = retention_generation
        return generation

    def crash_materializer(reason: str) -> None:
        subprocess.run(
            [
                str(pg_bin / "pg_ctl"), "-D", str(materializer_data),
                "-m", "immediate", "-W", "stop",
            ],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env,
        )
        events.emit(
            "process_stop", target="materializer",
            generation=materializer_generation, mode="immediate", reason=reason,
        )

    def archive_current_wal() -> str:
        wal_file = sql_scalar(
            writer_socket, writer_port,
            "SELECT pg_walfile_name(pg_switch_wal() - 1);",
        )
        done = writer_data / "pg_wal" / "archive_status" / f"{wal_file}.done"
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if done.is_file():
                return wal_file
            time.sleep(.1)
        status = sql_scalar(
            writer_socket, writer_port,
            "SELECT row(archived_count, failed_count, last_archived_wal, "
            "last_failed_wal)::text FROM pg_stat_archiver;",
        )
        raise PlanError(
            f"writer did not archive WAL file {wal_file}; archiver status {status}"
        )

    def make_boundary_durable(action: dict[str, Any], checkpoint_lsn: str) -> dict[str, Any]:
        wal_file = archive_current_wal()
        wait_scalar(
            materializer_socket, materializer_port,
            f"SELECT pg_last_wal_replay_lsn() >= '{checkpoint_lsn}'::pg_lsn",
            "t", f"materializer did not replay checkpoint {checkpoint_lsn}",
        )
        wait_scalar(
            materializer_socket, materializer_port,
            f"SELECT pagestore_materialized_wal_lsn() >= "
            f"'{checkpoint_lsn}'::pg_lsn",
            "t", f"materializer marker did not cover checkpoint {checkpoint_lsn}",
        )
        supervisor_status = wait_supervisor_status(
            lambda status: (
                status.get("state") == "running"
                and isinstance(status.get("progress"), dict)
                and status["progress"].get("lag_bytes") == 0
            ),
            f"supervisor did not publish zero lag for {action['name']}",
        )
        sync_materializer_generation(supervisor_status)
        status = sql_scalar(
            writer_socket, writer_port,
            "SELECT row(shipped_wal_lsn::text, materialized_wal_lsn::text, "
            "lag_bytes)::text FROM pagestore_materializer_status();",
        )
        shipped, materialized, lag = status.strip("()").split(",")
        if lag != "0":
            raise PlanError(
                f"materializer boundary {action['name']} retained {lag} bytes of lag"
            )
        horizon = {
            "checkpoint_lsn": checkpoint_lsn,
            "shipped_wal_lsn": shipped,
            "materialized_wal_lsn": materialized,
            "lag_bytes": lag,
            "wal_file": wal_file,
            "materializer_generation": materializer_generation,
        }
        events.emit(
            "materialized_boundary", id=action["id"], name=action["name"],
            horizon=horizon,
        )
        return horizon

    try:
        events.emit(
            "run_start", scenario=plan.header["scenario"], seed=plan.header["seed"],
            shm=shm, postgres_major=postgres_major,
        )
        with (trace / "daemon.log").open("w", encoding="utf-8") as log:
            dproc = subprocess.Popen(
                [
                    str(daemon), "--shm", shm, "--store", str(store),
                    "--page-size", str(profile["page_size"]),
                    "--nshards", str(plan.header["case"]["shards"]),
                    "--storage", plan.header["case"]["storage"],
                ],
                stdout=log, stderr=subprocess.STDOUT, text=True, env=env,
            )
        deadline = time.monotonic() + 10
        while True:
            if dproc.poll() is not None:
                raise PlanError(
                    f"daemon exited before readiness with status {dproc.returncode}"
                )
            try:
                health = inspect_store(inspector, shm, "health", schema)
                break
            except PlanError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.05)
        validate_runtime_health(
            plan, capabilities, "materializer_smoke", health, schema
        )
        probe_runtime_inspection(inspector, shm, capabilities, schema)
        subprocess.run(
            [
                str(pg_bin / "initdb"), "-D", str(writer_data),
                "-U", "postgres", "-A", "trust",
            ],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env,
        )
        subprocess.run(
            pagestore_import_command(importer, shm, writer_data, profile["page_size"]),
            check=True, capture_output=True, encoding="utf-8", env=env,
        )
        with (writer_data / "postgresql.conf").open("a", encoding="utf-8") as config:
            config.write(
                "shared_preload_libraries = 'pagestore'\n"
                "pagestore.backend = 'localsvc'\n"
                f"pagestore.localsvc_shm = {postgresql_conf_string(shm)}\n"
                "pagestore.route_all = off\n"
                "pagestore.timeline = 0\n"
                f"{postgres_runtime_settings(postgres_major)}"
                f"{recovery_settings}"
                "archive_mode = on\n"
                "archive_library = 'pagestore'\n"
                "listen_addresses = ''\n"
                f"unix_socket_directories = {postgresql_conf_string(writer_socket)}\n"
                f"port = {writer_port}\n"
            )
        subprocess.run(
            [
                str(pg_bin / "pg_ctl"), "-D", str(writer_data), "-l",
                str(trace / "writer.log"), "-w", "start",
            ],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env,
        )
        events.emit("ready", target="writer", health=health, port=writer_port)

        subprocess.run(
            [
                str(pg_bin / "pg_basebackup"), "-h", str(writer_socket),
                "-p", str(writer_port), "-U", "postgres", "-D",
                str(materializer_data), "--wal-method=none", "--checkpoint=fast",
            ],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env,
        )
        materializer_data.chmod(0o700)
        archive_current_wal()
        restore_program = str(walrestore).replace("%", "%%")
        restore_shm = str(shm).replace("%", "%%")
        restore_command = (
            f"{shlex.quote(restore_program)} --shm {shlex.quote(restore_shm)} "
            "--timeline 0 --incarnation 1 --segsize 16777216 %f %p"
        )
        restore_command_setting = postgresql_conf_string(restore_command)
        with (materializer_data / "postgresql.conf").open(
            "a", encoding="utf-8"
        ) as config:
            config.write(
                "pagestore.route_all = on\n"
                "pagestore.materializer = on\n"
                "pagestore.retention_owner_id = '1'\n"
                "pagestore.retention_owner_generation = '1'\n"
                "archive_mode = off\n"
                "hot_standby = on\n"
                "listen_addresses = ''\n"
                f"unix_socket_directories = {postgresql_conf_string(materializer_socket)}\n"
                f"port = {materializer_port}\n"
                f"restore_command = {restore_command_setting}\n"
            )
        (materializer_data / "standby.signal").touch()
        for wal_path in (materializer_data / "pg_wal").glob("0000000*"):
            if wal_path.is_file():
                wal_path.unlink()
        supervisor_config = root / "materializer-supervisor.json"
        supervisor_config.write_text(
            json.dumps(
                {
                    "schema": 4,
                    "pg_ctl": str(pg_bin / "pg_ctl"),
                    "psql": str(pg_bin / "psql"),
                    "data_dir": str(materializer_data),
                    "socket_dir": str(materializer_socket),
                    "port": materializer_port,
                    "log_file": str(trace / "materializer.log"),
                    "state_dir": str(supervisor_state),
                    "retention_authority_dir": str(root / "controller-authority"),
                    "retention_owner_id": 1,
                    "controller_instance_id": "harness-controller-1",
                    "poll_interval_ms": 100,
                    "replay_idle_ms": 300,
                    "progress_timeout_ms": 10000,
                    "retry_initial_ms": 50,
                    "retry_max_ms": 1000,
                    "max_consecutive_failures": 5,
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        with (trace / "materializer-supervisor.log").open(
            "w", encoding="utf-8"
        ) as log:
            supervisor_proc = subprocess.Popen(
                [
                    sys.executable, str(supervisor),
                    "--config", str(supervisor_config),
                ],
                stdout=log, stderr=subprocess.STDOUT, text=True, env=env,
            )
        supervisor_status = wait_supervisor_status(
            lambda status: (
                status.get("owner_pid") == supervisor_proc.pid
                and status.get("state")
                in {"running", "waiting_for_progress_api"}
            ),
            "materializer supervisor did not start its worker",
        )
        sync_materializer_generation(supervisor_status)
        wait_scalar(
            materializer_socket, materializer_port,
            "SELECT pg_is_in_recovery() AND "
            "current_setting('pagestore.materializer')::boolean",
            "t", "materializer did not enter its declared recovery role",
        )
        events.emit(
            "process_start", target="materializer",
            generation=materializer_generation, reason="supervisor provisioned",
        )
        events.emit(
            "ready", target="materializer", generation=materializer_generation,
            port=materializer_port,
        )
        initial_owner_pid = supervisor_proc.pid
        initial_owner_epoch = supervisor_status["owner_epoch"]
        initial_retention_generation = materializer_retention_generation
        initial_worker_pid = int(
            (materializer_data / "postmaster.pid")
            .read_text(encoding="utf-8")
            .splitlines()[0]
        )
        supervisor_proc.terminate()
        supervisor_proc.wait(timeout=5)
        if supervisor_proc.returncode != 0:
            raise PlanError(
                "materializer supervisor did not stop cleanly for handoff: "
                f"status {supervisor_proc.returncode}"
            )
        with (trace / "materializer-supervisor-replacement.log").open(
            "w", encoding="utf-8"
        ) as log:
            supervisor_proc = subprocess.Popen(
                [
                    sys.executable, str(supervisor),
                    "--config", str(supervisor_config),
                ],
                stdout=log, stderr=subprocess.STDOUT, text=True, env=env,
            )
        supervisor_status = wait_supervisor_status(
            lambda status: (
                status.get("owner_pid") == supervisor_proc.pid
                and status.get("owner_epoch", 0) > initial_owner_epoch
                and status.get("worker_generation") == materializer_generation
                and status.get("retention_generation")
                    == initial_retention_generation
                and status.get("state")
                in {"running", "waiting_for_progress_api"}
            ),
            "replacement supervisor did not adopt the running worker",
        )
        adopted_worker_pid = int(
            (materializer_data / "postmaster.pid")
            .read_text(encoding="utf-8")
            .splitlines()[0]
        )
        if adopted_worker_pid != initial_worker_pid:
            raise PlanError(
                "supervisor handoff restarted the healthy materializer: "
                f"worker {initial_worker_pid} became {adopted_worker_pid}"
            )
        events.emit(
            "supervisor_replacement", target="materializer-supervisor",
            previous_owner_pid=initial_owner_pid,
            owner_pid=supervisor_proc.pid,
            owner_epoch=supervisor_status["owner_epoch"],
            adopted_generation=materializer_generation,
        )
        duplicate = subprocess.run(
            [
                sys.executable, str(supervisor),
                "--config", str(supervisor_config),
            ],
            check=False, capture_output=True, encoding="utf-8", env=env,
            timeout=10,
        )
        (trace / "duplicate-supervisor.log").write_text(
            duplicate.stdout + duplicate.stderr, encoding="utf-8"
        )
        if duplicate.returncode != 75:
            raise PlanError(
                "duplicate materializer supervisor was not fenced: "
                f"status {duplicate.returncode}"
            )
        events.emit(
            "ownership_fenced", target="materializer-supervisor",
            owner_pid=supervisor_proc.pid, duplicate_status=duplicate.returncode,
        )
        # Install after the worker base is taken: route_all recovery must replay
        # the extension catalog into the store instead of relying on a local
        # catalog copy that the WAL-only topology cannot keep authoritative.
        sql_result(writer_socket, writer_port, "CREATE EXTENSION pagestore;")

        for action in plan.actions:
            events.emit(
                "action_start", id=action["id"], op=action["op"],
                target=action["target"],
            )
            if action["op"] == "sql":
                output = sql_scalar(writer_socket, writer_port, action["sql"])
                events.emit(
                    "action", id=action["id"], op="sql", target="writer",
                    result=output,
                )
            elif action["op"] == "checkpoint":
                output = sql_result(
                    writer_socket, writer_port,
                    "CHECKPOINT; SELECT pg_current_wal_lsn();",
                ).stdout.strip().splitlines()
                checkpoint_lsn = output[-1]
                horizon = make_boundary_durable(action, checkpoint_lsn)
                (artifacts / f"{action['name']}.json").write_text(
                    json.dumps(horizon, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                events.emit(
                    "checkpoint", id=action["id"], name=action["name"],
                    horizon=horizon,
                )
            elif action["op"] == "crash":
                crashed_generation = materializer_generation
                crashed_retention_generation = materializer_retention_generation
                crash_materializer(action["id"])
                events.emit(
                    "crash", id=action["id"], target="materializer",
                    model="compute", generation=crashed_generation,
                )
                supervisor_status = wait_supervisor_status(
                    lambda status: (
                        isinstance(status.get("worker_generation"), int)
                        and status["worker_generation"] > crashed_generation
                        and isinstance(status.get("retention_generation"), int)
                        and status["retention_generation"]
                            > crashed_retention_generation
                        and status.get("state")
                        in {"running", "waiting_for_progress_api"}
                    ),
                    f"supervisor did not replace worker after {action['id']}",
                )
                sync_materializer_generation(supervisor_status)
                wait_scalar(
                    materializer_socket, materializer_port,
                    "SELECT pg_is_in_recovery() AND "
                    "current_setting('pagestore.materializer')::boolean",
                    "t", "replacement materializer did not become healthy",
                )
                events.emit(
                    "replacement", id=action["id"], target="materializer",
                    crashed_generation=crashed_generation,
                    generation=materializer_generation,
                )
            elif action["op"] == "assert":
                if action["target"] == "writer":
                    socket_dir, port = writer_socket, writer_port
                else:
                    socket_dir, port = materializer_socket, materializer_port
                try:
                    output = sql_scalar(socket_dir, port, action["sql"])
                except subprocess.CalledProcessError as error:
                    detail = (error.stderr or error.stdout or str(error)).strip()
                    raise OracleMismatch(
                        f"assert {action['id']} query failed: {detail}"
                    ) from error
                if output != action["expect"]:
                    raise OracleMismatch(
                        f"assert {action['id']} got {output!r}, "
                        f"expected {action['expect']!r}"
                    )
                events.emit(
                    "assert", id=action["id"], target=action["target"],
                    actual=output, generation=(
                        materializer_generation
                        if action["target"] == "materializer" else None
                    ),
                )
            else:
                raise PlanError(
                    f"materializer smoke does not execute {action['op']}"
                )
        events.emit(
            "run_pass", materializer_generation=materializer_generation
        )
    except Exception as error:
        classification = (
            "oracle_mismatch" if isinstance(error, OracleMismatch) else "setup"
        )
        events.emit(
            "run_fail", error=str(error), error_type=type(error).__name__,
            materializer_generation=materializer_generation,
        )
        (root / "failure.json").write_text(
            json.dumps(
                {"classification": classification, "error": str(error)}, indent=2
            ) + "\n",
            encoding="utf-8",
        )
        raise PlanError(
            f"materializer smoke failed; failure bundle: {root}: {error}"
        ) from error
    finally:
        if supervisor_proc is not None and supervisor_proc.poll() is None:
            supervisor_proc.terminate()
            try:
                supervisor_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                supervisor_proc.kill()
                supervisor_proc.wait(timeout=5)
        if (materializer_data / "postmaster.pid").exists():
            subprocess.run(
                [
                    str(pg_bin / "pg_ctl"), "-D", str(materializer_data),
                    "-m", "immediate", "-w", "stop",
                ],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
            )
        if (writer_data / "postmaster.pid").exists():
            subprocess.run(
                [
                    str(pg_bin / "pg_ctl"), "-D", str(writer_data),
                    "-m", "immediate", "-w", "stop",
                ],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
            )
        if dproc is not None and dproc.poll() is None:
            dproc.terminate()
            dproc.wait(timeout=5)
        remove_shm(shm)
    if temporary and not keep:
        shutil.rmtree(root)
    return root


def run_legacy_integration(script: Path, build: Path, requested_root: Path | None, keep: bool) -> Path:
    """Bridge the existing reader/branch integration coverage into a bundle."""
    root, temporary = run_root(requested_root)
    trace = root / "trace"
    trace.mkdir()
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
    if temporary and not keep:
        shutil.rmtree(root)
    return root


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capabilities", type=Path, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate", type=Path, metavar="PLAN")
    group.add_argument("--list", type=Path, metavar="SCENARIO_DIR")
    group.add_argument("--inspect", choices=["health", "backpressure", "pruning"])
    group.add_argument("--daemon-smoke", type=Path, metavar="PLAN")
    group.add_argument(
        "--daemon-fault-smoke", "--daemon-fault-recovery",
        dest="daemon_fault_smoke", type=Path, metavar="PLAN",
    )
    group.add_argument("--writer-smoke", type=Path, metavar="PLAN")
    group.add_argument("--materializer-smoke", type=Path, metavar="PLAN")
    group.add_argument("--legacy-integration", action="store_true")
    parser.add_argument("--daemon-binary", type=Path)
    parser.add_argument("--materializer-supervisor", type=Path)
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
    if args.daemon_fault_smoke and (args.daemon_binary is None or args.inspect_binary is None):
        parser.error(
            "--daemon-fault-smoke requires --daemon-binary and --inspect-binary"
        )
    if args.writer_smoke and (args.daemon_binary is None or args.inspect_binary is None or args.build_dir is None):
        parser.error("--writer-smoke requires --build-dir, --daemon-binary and --inspect-binary")
    if args.materializer_smoke and (
        args.daemon_binary is None
        or args.inspect_binary is None
        or args.materializer_supervisor is None
        or args.build_dir is None
    ):
        parser.error(
            "--materializer-smoke requires --build-dir, --daemon-binary, "
            "--inspect-binary and --materializer-supervisor"
        )
    if args.legacy_integration and args.build_dir is None:
        parser.error("--legacy-integration requires --build-dir")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        capabilities = read_json(args.capabilities)
        if capabilities.get("schema") != 1:
            raise PlanError(f"{args.capabilities}: capability schema must be 1")
        fault_catalog(capabilities, args.capabilities)
        if args.inspect:
            if args.inspect not in capability_values(capabilities, "inspection_operations"):
                raise PlanError(f"capabilities: inspection operation {args.inspect!r} is unavailable")
            schema_path = args.inspection_schema or args.capabilities.with_name("inspection_schema.json")
            schema = read_inspection_schema(schema_path, capabilities)
            print(json.dumps(inspect_store(args.inspect_binary, args.shm, args.inspect, schema), sort_keys=True))
            return 0
        if args.daemon_smoke:
            plan = read_plan(args.daemon_smoke)
            validate_plan(plan, capabilities, args.capabilities)
            validate_runtime_plan(plan, capabilities, "daemon_smoke")
            schema_path = args.inspection_schema or args.capabilities.with_name("inspection_schema.json")
            schema = read_inspection_schema(schema_path, capabilities)
            root = run_daemon_smoke(plan, capabilities, schema, args.daemon_binary,
                                    args.inspect_binary, args.run_root, args.keep)
            if args.keep or args.run_root:
                print(root)
            return 0
        if args.daemon_fault_smoke:
            plan = read_plan(args.daemon_fault_smoke)
            validate_plan(plan, capabilities, args.capabilities)
            validate_runtime_plan(plan, capabilities, "daemon_fault_smoke")
            command = [
                sys.executable, str(Path(__file__).resolve()),
                "--capabilities", str(args.capabilities),
                "--daemon-fault-smoke", str(args.daemon_fault_smoke),
                "--daemon-binary", str(args.daemon_binary),
                "--inspect-binary", str(args.inspect_binary),
            ]
            if args.run_root is not None:
                command.extend(["--run-root", str(args.run_root)])
            root = run_daemon_fault_recovery(
                plan, capabilities,
                read_inspection_schema(
                    args.inspection_schema
                    or args.capabilities.with_name("inspection_schema.json"),
                    capabilities,
                ),
                args.daemon_binary, args.inspect_binary, args.run_root, args.keep,
                args.capabilities, command,
            )
            if args.keep or args.run_root:
                print(root)
            return 0
        if args.writer_smoke:
            plan = read_plan(args.writer_smoke)
            validate_plan(plan, capabilities, args.capabilities)
            validate_runtime_plan(plan, capabilities, "writer_smoke")
            schema_path = args.inspection_schema or args.capabilities.with_name("inspection_schema.json")
            schema = read_inspection_schema(schema_path, capabilities)
            root = run_writer_smoke(plan, capabilities, schema, args.daemon_binary, args.inspect_binary,
                                    args.build_dir, args.run_root, args.keep)
            if args.keep or args.run_root:
                print(root)
            return 0
        if args.materializer_smoke:
            plan = read_plan(args.materializer_smoke)
            validate_plan(plan, capabilities, args.capabilities)
            validate_runtime_plan(plan, capabilities, "materializer_smoke")
            schema_path = (
                args.inspection_schema
                or args.capabilities.with_name("inspection_schema.json")
            )
            schema = read_inspection_schema(schema_path, capabilities)
            root = run_materializer_smoke(
                plan, capabilities, schema, args.daemon_binary,
                args.inspect_binary, args.materializer_supervisor,
                args.build_dir, args.run_root, args.keep,
            )
            if args.keep or args.run_root:
                print(root)
            return 0
        if args.legacy_integration:
            root = run_legacy_integration(args.integration_script, args.build_dir, args.run_root, args.keep)
            if args.keep or args.run_root:
                print(root)
            return 0
        paths = [args.validate] if args.validate else list(plan_files(args.list))
        if not paths:
            raise PlanError("no scenario plans found")
        errors = validate_paths(paths, capabilities, args.capabilities)
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
