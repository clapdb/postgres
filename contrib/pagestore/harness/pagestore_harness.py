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
import select
import shlex
import shutil
import signal
import socket
import stat
import struct
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
MAX_FAULT_TIMEOUT_SECONDS = 300.0
MATERIALIZER_MARKER_TIMEOUT_MS = 10000
MATERIALIZER_FAULT_PHASE_COUNT = 6
MATERIALIZER_FAULT_SCHEDULING_MARGIN_MS = 5000
MATERIALIZER_FAULT_MIN_WATCHDOG_MS = (
    MATERIALIZER_MARKER_TIMEOUT_MS * MATERIALIZER_FAULT_PHASE_COUNT
    + MATERIALIZER_FAULT_SCHEDULING_MARGIN_MS
)
MATERIALIZER_FAULT_MIN_TIMEOUT_SECONDS = MATERIALIZER_FAULT_MIN_WATCHDOG_MS / 1000.0


def fault_failure_classification(error: Exception) -> str:
    if isinstance(error, FaultNotReached):
        return "fault_not_reached"
    if isinstance(error, HarnessTimeout):
        return "timeout"
    if isinstance(error, UnexpectedExit):
        return "unexpected_exit"
    if isinstance(error, OracleMismatch):
        return "oracle_mismatch"
    return "setup"


def parse_materializer_status_row(value: str) -> dict[str, str | None]:
    """Parse the writer-side composite status without inventing progress."""
    text = value.strip()
    if len(text) < 2 or not text.startswith("(") or not text.endswith(")"):
        raise PlanError(f"invalid materializer status row: {value!r}")
    fields = text[1:-1].split(",")
    if len(fields) != 3:
        raise PlanError(f"invalid materializer status row: {value!r}")
    shipped, materialized, lag = (field.strip() or None for field in fields)
    if lag is not None:
        try:
            int(lag)
        except ValueError as error:
            raise PlanError(f"invalid materializer status lag: {value!r}") from error
    return {
        "shipped_wal_lsn": shipped,
        "materialized_wal_lsn": materialized,
        "lag_bytes": lag,
    }


def postgres_string_literal(value: str) -> str:
    """Quote a value for a SQL string literal with standard SQL escaping."""
    return "'" + value.replace("'", "''") + "'"


def relation_metadata_sql(relation: str) -> str:
    return (
        "SELECT COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::text "
        "|| '|' || d.oid::text || '|' || pg_relation_filenode(c.oid)::text "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_database d ON d.datname = current_database() "
        "WHERE n.nspname = 'public' AND c.relname = "
        + postgres_string_literal(relation)
    )


def record_relation_observation(
    observations: dict[tuple[str, str], dict[str, object]],
    relation_name: str,
    boundary_ref: str,
    observation: dict[str, object],
    fault_boundary_ref: str | None,
) -> bool:
    observations[(relation_name, boundary_ref)] = observation
    if fault_boundary_ref is None or boundary_ref != fault_boundary_ref:
        return False
    old_observation = observations.get((relation_name, "$R1"))
    if old_observation is None:
        raise OracleMismatch(
            f"{relation_name} fault-boundary relation inspection has no R1 snapshot"
        )
    if observation["relation_key"] != old_observation["relation_key"]:
        raise OracleMismatch(
            f"{relation_name} relation key changed between the R1 and fault snapshots"
        )
    if observation["main_nblocks"] <= old_observation["main_nblocks"]:
        raise OracleMismatch(
            f"{relation_name} fault-boundary main fork did not grow beyond R1"
        )
    return True


def fault_watchdog_milliseconds(value: Any) -> int:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise PlanError("fault timeout must be finite and positive")
    if value < 0.001 or value > MAX_FAULT_TIMEOUT_SECONDS:
        raise PlanError("fault timeout must map to 1..300000 milliseconds")
    milliseconds = math.ceil(value * 1000.0)
    if milliseconds < 1 or milliseconds > 300000:
        raise PlanError("fault timeout does not fit the watchdog millisecond range")
    return milliseconds


def materializer_fault_watchdog_milliseconds(value: Any) -> int:
    milliseconds = fault_watchdog_milliseconds(value)
    if milliseconds < MATERIALIZER_FAULT_MIN_WATCHDOG_MS:
        raise PlanError(
            "materializer fault timeout must be at least "
            f"{MATERIALIZER_FAULT_MIN_TIMEOUT_SECONDS:g} seconds "
            "(six 10-second mailbox phases plus 5-second scheduling margin: "
            "marker read, initial sync, CREATE, NBLOCKS, WRITE/EXTEND, final sync)"
        )
    return milliseconds


def fault_timeout_seconds(value: Any) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise PlanError("fault timeout must be finite and positive")
    return float(value)


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


def parse_lsn_value(value: Any) -> int:
    if not isinstance(value, str):
        raise PlanError(f"invalid PostgreSQL LSN {value!r}")
    try:
        high, low = value.split("/", 1)
        result = (int(high, 16) << 32) | int(low, 16)
    except (ValueError, TypeError) as error:
        raise PlanError(f"invalid PostgreSQL LSN {value!r}") from error
    if result < 0:
        raise PlanError(f"invalid PostgreSQL LSN {value!r}")
    return result


def postgresql_conf_string(value: str | Path) -> str:
    """Return value as a safely quoted PostgreSQL configuration string."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


HEADER_FIELDS = {"schema", "scenario", "seed", "contracts", "case", "extra"}
ACTION_FIELDS = {
    "sql": {"op", "id", "target", "sql", "expect_error", "expect_sqlstate", "extra"},
    "checkpoint": {"op", "id", "target", "name", "extra"},
    "materializer_fault": {"op", "id", "target", "fault", "action", "hit", "timeout", "name", "extra"},
    "inspect_relation": {"op", "id", "target", "relation", "lsn", "extra"},
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
    "layer_seed": {"op", "id", "target", "extra"},
    "gc_seed": {"op", "id", "target", "workload", "extra"},
    "capture": {"op", "id", "target", "kind", "name", "horizon", "extra"},
    "compare": {"op", "id", "left", "right", "extra"},
    "expect_failure": {"op", "id", "target", "command", "sqlstate", "extra"},
    "cleanup": {"op", "id", "target", "extra"},
}

REQUIRED_FIELDS = {
    "sql": {"target", "sql"},
    "checkpoint": {"target", "name"},
    "materializer_fault": {"target", "fault", "action", "hit", "name", "timeout"},
    "inspect_relation": {"target", "relation", "lsn"},
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
    "layer_seed": {"target"},
    "gc_seed": {"target", "workload"},
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
    "materializer_fault": {"name"},
}

REFERENCE_FIELDS = {
    "prepare_branch": {"fork_lsn"},
    "install_branch": {"fork_lsn"},
    "prepare_reader": {"base", "read_lsn"},
    "reader_base": {"checkpoint"},
    "install_reader": {"read_lsn"},
    "capture": {"horizon"},
    "inspect_relation": {"lsn"},
    "compare": {"left", "right"},
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
    if capabilities.get("inspection_schema") != INSPECTION_SCHEMA_VERSION:
        raise PlanError(
            "capabilities: inspection schema version does not match "
            "runner implementation"
        )
    if schema.get("schema") != capabilities.get("inspection_schema"):
        raise PlanError(f"{path}: inspection schema version does not match capabilities")
    if schema.get("transport") != INSPECTION_TRANSPORT:
        raise PlanError(
            f"{path}: inspection transport must be {INSPECTION_TRANSPORT!r}"
        )
    if schema.get("mutating_operations") != []:
        raise PlanError(f"{path}: inspection schema must not advertise mutating operations")
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
    if not isinstance(operations, dict) or set(operations) != (
        INSPECTION_OPERATIONS | INSPECTION_PLANNED_OPERATIONS
    ):
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
        expected_request = list(INSPECTION_REQUESTS.get(operation, ()))
        request = definition.get("request", []) if isinstance(definition, dict) else None
        if request != expected_request:
            raise PlanError(
                f"{path}: inspection operation {operation!r} request fields "
                "do not match its contract"
            )
        if operation == "relation":
            response_types = definition.get("response_types")
            if response_types != {
                "exists": "boolean",
                "forks": "array",
                "selected_version": "null",
            }:
                raise PlanError(
                    f"{path}: relation inspection response types are invalid"
                )
    for operation, expected_response in INSPECTION_PLANNED_RESPONSES.items():
        definition = operations.get(operation)
        if not isinstance(definition, dict):
            raise PlanError(
                f"{path}: planned inspection operation {operation!r} must be an object"
            )
        if definition.get("request") != list(INSPECTION_REQUESTS[operation]):
            raise PlanError(
                f"{path}: planned inspection operation {operation!r} request fields "
                "do not match its contract"
            )
        if definition.get("response") != list(expected_response):
            raise PlanError(
                f"{path}: planned inspection operation {operation!r} response fields "
                "do not match its contract"
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
    "timeline": {
        "parent_timeline", "fork_lsn", "retained_horizon",
    },
    "manifest": {
        "layer_count", "deleting_layers", "local_layers",
        "remote_durable_layers", "manifest_poisoned",
    },
    "gc": {
        "page_debt_segments", "page_debt_unavailable", "deleting_layers",
        "remote_cleanup_pending",
        "forkmeta_pending", "forkmeta_poisoned",
    },
    "owners": {
        "owner_count", "page_history_owners", "wal_owners",
        "wal_index_owners", "max_generation", "retention_poisoned",
    },
    "pruning": {
        "compactions", "versions_scanned", "versions_kept", "versions_deleted",
    },
    "relation": {"exists", "forks", "selected_version"},
}
INSPECTION_OPERATIONS = set(INSPECTION_RESPONSES)
INSPECTION_PLANNED_RESPONSES = {}
INSPECTION_PLANNED_OPERATIONS = set(INSPECTION_PLANNED_RESPONSES)
INSPECTION_REQUESTS = {
    "timeline": ("timeline",),
    "relation": ("timeline", "incarnation", "key", "lsn"),
}
INSPECTION_BOOLEAN_FIELDS = {
    "retention_poisoned", "manifest_poisoned",
    "forkmeta_pending", "forkmeta_poisoned", "page_debt_unavailable",
}
INSPECTION_SIGNED_FIELDS = {"parent_timeline"}
INSPECTION_COUNTER_FIELDS = (
    set().union(*INSPECTION_RESPONSES.values())
    - INSPECTION_BOOLEAN_FIELDS - INSPECTION_SIGNED_FIELDS
)
INSPECTION_SCHEMA_VERSION = 4
INSPECTION_TRANSPORT = "private-test-ipc"
PG_CONTROL_FILE_SIZE = 8192


RUNTIME_OPERATIONS = {
    "daemon_smoke": {"crash"},
    "daemon_fault_smoke": {"crash", "set_fault", "release_fault", "layer_seed", "gc_seed"},
    "writer_smoke": {
        "sql", "checkpoint", "prepare_reader", "reader_base", "bootstrap",
        "install_reader", "assert", "capture", "restart",
    },
    "materializer_smoke": {
        "sql", "checkpoint", "materializer_fault", "inspect_relation", "crash",
        "assert", "restart",
    },
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
        "layer_seed": {"target": ["store"]},
        "gc_seed": {"target": ["store"], "workload": ["page_prune", "wal_index", "wal_reclaim",
                                                      "timeline_delete", "timeline_delete_abort",
                                                      "manifest_compact", "forkmeta"]},
    },
    "writer_smoke": {
        "checkpoint": {"target": ["writer"]},
        "prepare_reader": {"target": ["writer"]},
        "reader_base": {"target": ["writer"]},
        "bootstrap": {"target": ["writer"]},
        "install_reader": {"forbidden_values": {"target": ["writer"]}},
        "assert": {"oracle": ["sql_scalar"]},
        "capture": {"target": ["writer"], "kind": ["reader_datadir"]},
        "restart": {},
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
        "materializer_fault": {"target": ["materializer"], "action": ["pause"]},
        "inspect_relation": {"target": ["materializer"]},
        "restart": {"target": ["store", "writer", "materializer"]},
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
    validate_materializer_relation_pairing(plan)
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
            if action["op"] in ("sql", "assert", "restart") and action["target"] not in available_clients:
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
            elif action["op"] in ("sql", "assert", "restart") and \
                    action["target"] == "writer":
                # a restart runs recovery and writes its own checkpoint, so the
                # declared checkpoint no longer describes the writer's data
                # directory any more than a statement against it would
                writer_mutated_since_checkpoint = True
    elif runtime == "materializer_smoke":
        required = {"writer", "materializer"}
        computes = plan.header["case"]["compute"]
        if len(computes) != len(required) or set(computes) != required:
            raise PlanError(
                f"runtime {runtime!r} requires exactly writer and materializer computes"
            )
        fault_actions = [
            action for action in plan.actions if action["op"] == "materializer_fault"
        ]
        if len(fault_actions) > 1:
            raise PlanError(
                f"runtime {runtime!r} permits at most one materializer_fault action"
            )
        if fault_actions:
            fault_action = fault_actions[0]
            fault_index = plan.actions.index(fault_action)
            prior_r1 = any(
                earlier["op"] == "checkpoint" and earlier.get("name") == "R1"
                for earlier in plan.actions[:fault_index]
            )
            if not prior_r1:
                raise PlanError(
                    f"runtime {runtime!r} materializer_fault requires a prior checkpoint named R1"
                )
            validate_fault_action(
                fault_action, capabilities, f"{plan.path}:{fault_action['id']}",
                require_model=False,
            )
            if fault_action["action"] != "pause":
                raise PlanError(
                    f"runtime {runtime!r} materializer_fault must use pause"
                )
            try:
                materializer_fault_watchdog_milliseconds(fault_action.get("timeout"))
            except PlanError as error:
                raise PlanError(
                    f"runtime {runtime!r} materializer_fault has invalid timeout: {error}"
                ) from error
        if any(action["op"] == "crash" and "fault" in action for action in plan.actions):
            raise PlanError(
                f"runtime {runtime!r} uses materializer_fault for named process faults"
            )
    elif runtime == "daemon_fault_smoke":
        seed_actions = [
            action for action in plan.actions
            if action["op"] in ("layer_seed", "gc_seed")
        ]
        if len(seed_actions) > 1:
            raise PlanError(
                f"runtime {runtime!r} permits at most one layer_seed or gc_seed action"
            )
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
        if seed_actions and plan.actions.index(seed_actions[0]) > plan.actions.index(named[0]):
            raise PlanError(
                f"runtime daemon_fault_smoke requires {seed_actions[0]['op']} before the named fault"
            )
        fault = named[0]
        # The seeds are the only workloads this runtime runs, so a fault that
        # only their state can reach needs the seed that creates it: without
        # one the plan is accepted and then deterministically expires as
        # FaultNotReached against an empty store.
        layer_faults = {
            "image_layer.after_create", "image_layer.after_write",
            "image_layer.after_seal", "image_layer.after_manifest_add",
        }
        gc_workload = next(
            (name for name, faults in GC_WORKLOAD_FAULTS.items()
             if fault["fault"] in faults),
            None,
        )
        if seed_actions and seed_actions[0]["op"] == "layer_seed" and \
                fault["fault"] not in layer_faults:
            raise PlanError(
                "runtime daemon_fault_smoke layer_seed requires an H1 image-layer fault"
            )
        if seed_actions and seed_actions[0]["op"] == "gc_seed":
            if fault["fault"] not in (
                GC_WORKLOAD_FAULTS.get(seed_actions[0].get("workload"), set())
            ):
                raise PlanError(
                    "runtime daemon_fault_smoke gc_seed requires an H1 fault matching "
                    "its workload"
                )
            reachable = GC_FAULT_MAX_HITS.get(fault["fault"], GC_FAULT_DEFAULT_MAX_HIT)
            if fault.get("hit", 1) > reachable:
                raise PlanError(
                    f"runtime daemon_fault_smoke gc_seed fault {fault['fault']!r} is "
                    f"reachable {reachable} time(s) in workload "
                    f"{seed_actions[0].get('workload')!r}, plan asks for hit "
                    f"{fault.get('hit')}"
                )
        if fault["fault"] in layer_faults and not (
                seed_actions and seed_actions[0]["op"] == "layer_seed"):
            raise PlanError(
                f"runtime daemon_fault_smoke fault {fault['fault']!r} requires a "
                "layer_seed"
            )
        if gc_workload is not None and not (
                seed_actions and seed_actions[0]["op"] == "gc_seed" and
                seed_actions[0].get("workload") == gc_workload):
            raise PlanError(
                f"runtime daemon_fault_smoke fault {fault['fault']!r} requires a "
                f"gc_seed with workload {gc_workload!r}"
            )
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
            inspector, shm, operation, inspection_schema,
            timeline=0 if operation in {"timeline", "relation"} else None,
            incarnation=1 if operation == "relation" else None,
            relation_key=(1, 1, 1) if operation == "relation" else None,
            lsn=0 if operation == "relation" else None,
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


def validate_materializer_relation_pairing(plan: Plan) -> None:
    """Require each fault-boundary relation snapshot to have its own prior R1."""
    r1_relations: set[str] = set()
    fault_r1_relations: dict[str, set[str]] = {}
    for action in plan.actions:
        if action["op"] == "materializer_fault":
            fault_r1_relations[action["name"]] = set(r1_relations)
            continue
        if action["op"] != "inspect_relation":
            continue
        boundary_ref = action["lsn"]
        if boundary_ref == "$R1":
            r1_relations.add(action["relation"])
            continue
        if not boundary_ref.startswith("$"):
            continue
        fault_r1 = fault_r1_relations.get(boundary_ref[1:])
        if fault_r1 is not None and action["relation"] not in fault_r1:
            raise PlanError(
                f"{plan.path}:{action['id']}: inspect_relation {action['relation']!r} "
                f"at {boundary_ref} requires a prior same-relation $R1 inspection "
                "before the materializer fault"
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
        for field in REQUIRED_FIELDS[operation] - {"lanes", "steps", "expect", "hit", "timeout"}:
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
        for field in REFERENCE_FIELDS.get(operation, set()):
            value = action.get(field)
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
        if operation == "materializer_fault":
            boundary = require_string(action, "name", action_context)
            if boundary in boundaries:
                raise PlanError(f"{action_context}: duplicate durability boundary {boundary!r}")
            boundaries.add(boundary)
            validate_fault_identity(header["scenario"], "scenario", context)
            validate_fault_identity(action["fault"], "fault", action_context)
            validate_fault_action(
                action, capabilities, action_context, catalog_path, require_model=False,
            )
            if action.get("action") != "pause":
                raise PlanError(
                    f"{action_context}: materializer fault scenarios must use pause"
                )
        if operation == "inspect_relation":
            lsn = require_string(action, "lsn", action_context)
            if not lsn.startswith("$") or lsn[1:] not in boundaries:
                raise PlanError(
                    f"{action_context}: inspect_relation lsn must reference an earlier "
                    "checkpoint or materializer boundary"
                )
            validate_fault_identity(action["relation"], "relation", action_context)
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
        if operation in ("crash", "set_fault", "materializer_fault") and "timeout" in action:
            try:
                if operation == "materializer_fault":
                    materializer_fault_watchdog_milliseconds(action["timeout"])
                elif action.get("action") == "pause":
                    fault_watchdog_milliseconds(action["timeout"])
                else:
                    fault_timeout_seconds(action["timeout"])
            except PlanError as error:
                raise PlanError(f"{action_context}: {error}") from error
    validate_materializer_relation_pairing(plan)


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


def inspect_store(
    binary: Path,
    shm: str,
    operation: str,
    schema: dict[str, Any],
    timeline: int | None = None,
    incarnation: int | None = None,
    relation_key: tuple[int, int, int] | None = None,
    lsn: int | None = None,
) -> dict[str, Any]:
    implemented = schema.get("implemented_operations")
    operations = schema.get("operations")
    if not isinstance(implemented, list) or operation not in implemented:
        raise PlanError(f"inspection schema: operation {operation!r} is not implemented")
    if not isinstance(operations, dict) or not isinstance(operations.get(operation), dict):
        raise PlanError(f"inspection schema: missing definition for {operation!r}")
    expected = operations[operation].get("response")
    if not isinstance(expected, list) or not all(isinstance(field, str) for field in expected):
        raise PlanError(f"inspection schema: invalid response definition for {operation!r}")
    if operation == "timeline":
        if (
            not isinstance(timeline, int)
            or isinstance(timeline, bool)
            or timeline < 0
        ):
            raise PlanError("inspection operation 'timeline' requires a nonnegative timeline ID")
        request = [str(timeline)]
    elif operation == "relation":
        if not isinstance(timeline, int) or isinstance(timeline, bool) or timeline < 0:
            raise PlanError("inspection operation 'relation' requires a nonnegative timeline ID")
        if (
            not isinstance(incarnation, int)
            or isinstance(incarnation, bool)
            or incarnation <= 0
        ):
            raise PlanError(
                "inspection operation 'relation' requires a positive incarnation"
            )
        if (
            not isinstance(relation_key, tuple)
            or len(relation_key) != 3
            or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in relation_key
            )
            or not isinstance(lsn, int)
            or isinstance(lsn, bool)
            or lsn < 0
        ):
            raise PlanError(
                "inspection operation 'relation' requires a nonnegative key and LSN"
            )
        request = [
            str(timeline), str(incarnation),
            *(str(value) for value in relation_key), str(lsn),
        ]
    elif any(value is not None for value in (timeline, incarnation, relation_key, lsn)):
        raise PlanError(
            f"inspection operation {operation!r} does not accept request arguments"
        )
    try:
        result = subprocess.run(
            [str(binary), "--shm", shm, operation, *request]
            if operation in {"timeline", "relation"}
            else [str(binary), "--shm", shm, operation],
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
    if operation == "relation":
        if not isinstance(value.get("exists"), bool):
            raise PlanError("inspector relation field 'exists' must be a boolean")
        forks = value.get("forks")
        if not isinstance(forks, list):
            raise PlanError("inspector relation field 'forks' must be an array")
        if len(forks) > 4:
            raise PlanError("inspector relation field 'forks' has too many entries")
        has_main_fork = False
        previous_fork = -1
        for fork in forks:
            if (
                not isinstance(fork, dict)
                or set(fork) != {"fork", "nblocks"}
                or not isinstance(fork["fork"], int)
                or isinstance(fork["fork"], bool)
                or not 0 <= fork["fork"] < 4
                or not isinstance(fork["nblocks"], int)
                or isinstance(fork["nblocks"], bool)
                or fork["nblocks"] < 0
            ):
                raise PlanError("inspector relation fork entries have invalid types")
            if fork["fork"] <= previous_fork:
                raise PlanError(
                    "inspector relation fork entries must be strictly increasing"
                )
            previous_fork = fork["fork"]
            has_main_fork = has_main_fork or fork["fork"] == 0
        if value["exists"] != has_main_fork:
            raise PlanError(
                "inspector relation 'exists' must match presence of fork 0"
            )
        if value.get("selected_version") is not None:
            raise PlanError(
                "inspector relation selected_version must be null when unavailable"
            )
        return value
    for field in expected:
        observed = value[field]
        if field in INSPECTION_BOOLEAN_FIELDS:
            if not isinstance(observed, bool):
                raise PlanError(
                    f"inspector {operation} field {field!r} must be a boolean"
                )
        elif field in INSPECTION_SIGNED_FIELDS:
            if (
                not isinstance(observed, int)
                or isinstance(observed, bool)
                or observed < -1
            ):
                raise PlanError(
                    f"inspector {operation} field {field!r} must be -1 or a "
                    "nonnegative integer"
                )
        elif field in INSPECTION_COUNTER_FIELDS:
            if (
                not isinstance(observed, int)
                or isinstance(observed, bool)
                or observed < 0
            ):
                raise PlanError(
                    f"inspector {operation} field {field!r} must be a "
                    "nonnegative integer"
                )
        else:
            raise PlanError(
                f"inspection schema: field {field!r} has no response type"
            )
    return value


def run_root(path: Path | None) -> tuple[Path, bool]:
    if path is None:
        return Path(tempfile.mkdtemp(prefix="pagestore-harness-")), True
    if path.exists() and any(path.iterdir()):
        raise PlanError(f"run root must be empty: {path}")
    path.mkdir(parents=True, exist_ok=True)
    return path, False


def cleanup_temporary_root(
    root: Path, temporary: bool, keep: bool, cleanup_errors: list[str],
) -> None:
    if not temporary or keep or cleanup_errors:
        return
    try:
        shutil.rmtree(root)
    except Exception as error:
        cleanup_errors.append(f"run root cleanup: {error}")


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


@dataclass
class ProcessIdentity:
    """A process identity that remains safe across pidfile replacement."""

    pid: int
    starttime: int | None
    pidfd: int | None = None
    already_exited: bool = False
    status: str = "captured"


@dataclass(frozen=True)
class ProcessStopResult:
    """Evidence for an immediate stop performed against one identity."""

    pid: int
    starttime: int | None
    status: str
    signal_method: str
    wait_method: str


def require_signaled_process_stop(result: ProcessStopResult, target: str) -> None:
    """Require that a requested whole-process stop actually delivered SIGQUIT."""
    if result.status != "signaled":
        raise UnexpectedExit(
            f"{target} immediate stop did not signal the captured process: "
            f"status={result.status} pid={result.pid}"
        )


def read_process_starttime(pid: int, proc_root: Path = Path("/proc")) -> int | None:
    """Read Linux proc stat field 22, parsing comm through its final ')' safely."""
    stat_path = proc_root / str(pid) / "stat"
    try:
        text = stat_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except PermissionError as error:
        raise PlanError(f"cannot read process identity {stat_path}: {error}") from error
    except OSError as error:
        raise PlanError(f"cannot read process identity {stat_path}: {error}") from error

    closing_paren = text.rfind(")")
    if closing_paren < 0:
        raise PlanError(f"malformed process stat for pid {pid}")
    fields = text[closing_paren + 1:].split()
    # fields[0] is state (field 3); starttime is field 22.
    if len(fields) <= 19:
        raise PlanError(f"short process stat for pid {pid}")
    try:
        return int(fields[19])
    except ValueError as error:
        raise PlanError(f"invalid process starttime for pid {pid}") from error


def procfs_available(proc_root: Path = Path("/proc")) -> bool:
    """Return whether this host exposes a usable procfs process view."""
    try:
        (proc_root / "self" / "stat").read_text(encoding="utf-8")
    except PermissionError as error:
        raise PlanError(f"cannot read procfs process identity: {error}") from error
    except OSError:
        return False
    return True


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as error:
        if error.errno == errno.ESRCH:
            return False
        raise PlanError(f"cannot verify whether pid {pid} exists: {error}") from error
    return True


def pidfile_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").splitlines()[0])
    except (OSError, ValueError, IndexError):
        return None


def capture_process_identity(pid: int) -> ProcessIdentity:
    """Capture a stable identity without ever reacquiring a replacement PID."""
    if pid <= 0:
        raise PlanError(f"invalid process pid: {pid}")
    # Always take the first observation before pidfd_open.  If the process
    # disappears during pidfd_open, do not read the PID again: that read could
    # describe a supervisor replacement rather than the old postmaster.
    procfs_is_available = procfs_available()
    first_starttime = read_process_starttime(pid)
    if procfs_is_available and first_starttime is None:
        return ProcessIdentity(
            pid, None, already_exited=True, status="already_exited"
        )
    if first_starttime is None:
        if not process_exists(pid):
            return ProcessIdentity(
                pid, None, already_exited=True, status="already_exited"
            )
        raise PlanError(
            f"cannot capture live pid {pid}: procfs unavailable and identity is unverifiable"
        )
    pidfd_open = getattr(os, "pidfd_open", None)
    pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        if not process_exists(pid):
            return ProcessIdentity(
                pid, first_starttime, already_exited=True, status="already_exited"
            )
        raise PlanError(
            f"cannot capture live pid {pid}: pidfd support is unavailable"
        )

    try:
        pidfd = pidfd_open(pid, 0)
    except ProcessLookupError:
        return ProcessIdentity(
            pid, first_starttime, already_exited=True, status="already_exited"
        )
    except OSError as error:
        if error.errno == errno.ESRCH:
            return ProcessIdentity(
                pid, first_starttime, already_exited=True, status="already_exited"
            )
        if error.errno not in {
            errno.ENOSYS, errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP,
        }:
            raise PlanError(f"cannot capture pidfd for pid {pid}: {error}") from error
        if not process_exists(pid):
            return ProcessIdentity(
                pid, first_starttime, already_exited=True, status="already_exited"
            )
        raise PlanError(
            f"cannot capture live pid {pid}: pidfd support is unavailable"
        )

    second_starttime = read_process_starttime(pid)
    if (
        first_starttime is not None
        and second_starttime is not None
        and first_starttime != second_starttime
    ):
        try:
            os.close(pidfd)
        except OSError as error:
            raise PlanError(
                f"cannot close pidfd {pidfd} after pid {pid} identity changed: {error}"
            ) from error
        return ProcessIdentity(
            pid, first_starttime, already_exited=True, status="already_exited"
        )
    # pidfd is the authoritative identity if /proc is unavailable after the
    # second read; retain the first observation for diagnostics/fallback.
    return ProcessIdentity(pid, first_starttime, pidfd)


def close_process_identity(identity: ProcessIdentity) -> None:
    if identity.pidfd is None:
        return
    pidfd = identity.pidfd
    identity.pidfd = None
    try:
        os.close(pidfd)
    except OSError as error:
        raise PlanError(f"cannot close pidfd {pidfd} for pid {identity.pid}: {error}") from error


def send_process_sigquit(identity: ProcessIdentity) -> tuple[str, str]:
    """Send SIGQUIT only to the captured process identity."""
    if identity.already_exited or identity.status == "already_exited":
        identity.already_exited = True
        identity.status = "already_exited"
        return "already_exited", "already_exited"
    if identity.pidfd is not None:
        try:
            signal.pidfd_send_signal(identity.pidfd, signal.SIGQUIT, None, 0)
            identity.status = "signaled"
            return "pidfd_send_signal", "signaled"
        except ProcessLookupError:
            identity.already_exited = True
            identity.status = "already_exited"
            return "pidfd_send_signal", "already_exited"
        except PermissionError as error:
            raise PlanError(f"permission denied signalling pid {identity.pid}: {error}") from error
        except OSError as error:
            raise PlanError(f"cannot signal pidfd for pid {identity.pid}: {error}") from error

    if identity.starttime is None:
        if not process_exists(identity.pid):
            identity.already_exited = True
            identity.status = "already_exited"
            return "pidfd-unavailable", "already_exited"
        raise PlanError(
            f"cannot safely signal live pid {identity.pid}: no pidfd identity"
        )
    before = read_process_starttime(identity.pid)
    if before is None:
        if process_exists(identity.pid):
            raise PlanError(
                f"cannot verify live pid {identity.pid} before SIGQUIT"
            )
        identity.already_exited = True
        identity.status = "already_exited"
        return "pidfd-unavailable", "already_exited"
    if before != identity.starttime:
        identity.already_exited = True
        identity.status = "already_exited"
        return "pidfd-unavailable", "already_exited"
    raise PlanError(
        f"cannot signal live pid {identity.pid}: pidfd identity is unavailable"
    )


def wait_process_exit(identity: ProcessIdentity, timeout: float) -> str:
    """Wait for the captured identity, never for a replacement pidfile owner."""
    if identity.already_exited or identity.status == "already_exited":
        return "already_exited"
    deadline = time.monotonic() + timeout
    if identity.pidfd is not None:
        poller = select.poll()
        poller.register(identity.pidfd, select.POLLIN | select.POLLHUP | select.POLLERR)
        while time.monotonic() < deadline:
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            if poller.poll(remaining_ms):
                return "pidfd_poll"
        raise HarnessTimeout(f"timed out waiting for pidfd of pid {identity.pid}")

    while time.monotonic() < deadline:
        current = read_process_starttime(identity.pid)
        if current is None:
            if not process_exists(identity.pid):
                return "proc_starttime"
            raise PlanError(
                f"cannot verify pid {identity.pid} while waiting: /proc identity unavailable"
            )
        if identity.starttime is None:
            raise PlanError(f"cannot verify pid {identity.pid} while waiting")
        if current != identity.starttime:
            raise PlanError(
                f"pid {identity.pid} was reused while waiting: "
                f"captured starttime={identity.starttime} current={current}"
            )
        time.sleep(.05)
    raise HarnessTimeout(f"timed out waiting for pid {identity.pid} to exit")


def stop_process_immediately(
    pid: int, timeout: float = 15.0, diagnostic_pidfile: Path | None = None,
) -> ProcessStopResult:
    """SIGQUIT one captured postmaster and wait for that same identity."""
    identity = capture_process_identity(pid)
    try:
        signal_method, status = send_process_sigquit(identity)
        if status == "already_exited":
            wait_method = "already_exited"
        else:
            try:
                wait_method = wait_process_exit(identity, timeout)
            except HarnessTimeout as error:
                diagnostic = ""
                if diagnostic_pidfile is not None:
                    diagnostic = (
                        f" pidfile={diagnostic_pidfile.exists()}"
                        f" pidfile_pid={pidfile_pid(diagnostic_pidfile)}"
                    )
                raise HarnessTimeout(f"{error}; old_pid={pid}{diagnostic}") from error
        result = ProcessStopResult(
            pid, identity.starttime, status, signal_method, wait_method
        )
    except BaseException:
        # Cleanup must not replace the signal/wait/timeout failure with a
        # secondary pidfd close error.
        try:
            close_process_identity(identity)
        except Exception:
            pass
        raise
    close_process_identity(identity)
    return result


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


def _preserve_fault_evidence(control: Path, trace: Path) -> None:
    """Best-effort copy of raw fault-control files before failure cleanup."""
    try:
        control_stat = control.lstat()
        if not stat.S_ISDIR(control_stat.st_mode) or control.is_symlink():
            return
        evidence = trace / "materializer-fault-control"
        evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
        for child in control.iterdir():
            child_stat = child.lstat()
            destination = evidence / child.name
            if stat.S_ISREG(child_stat.st_mode) and not child.is_symlink():
                shutil.copyfile(child, destination, follow_symlinks=False)
            elif child.is_symlink():
                destination.with_name(destination.name + ".symlink").write_text(
                    os.readlink(child), encoding="utf-8"
                )
    except (OSError, UnicodeError):
        return


def _fault_report(
    path: Path, expected_name: str, expected_hit: int, expected_pid: int | None,
    expected_action: str = "crash", expected_scenario: str | None = None,
    expected_seed: int | None = None, expected_operation: str | None = None,
    expected_state: str | None = None, require_replay_lsn: bool = False,
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
    replay_lsn_key_sets = {
        frozenset(keys | {"replay_lsn"}) for keys in (
            extended_keys, extended_name_keys, extended_both_keys,
        )
    }
    pause_suffix = {"state", "watchdog_ms"}
    allowed_key_sets = {
        frozenset(legacy_keys), frozenset(extended_keys),
        frozenset(extended_name_keys), frozenset(extended_both_keys),
        frozenset(legacy_keys | {"replay_lsn"}),
    } | replay_lsn_key_sets
    if expected_action == "pause":
        allowed_key_sets |= {
            frozenset(keys | pause_suffix)
            for keys in (
                extended_keys, extended_name_keys, extended_both_keys,
                *replay_lsn_key_sets,
            )
        }
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
        or value["pid"] <= 0
        or (expected_pid is not None and value["pid"] != expected_pid)
        or value["pid"] > 2**31 - 1
    ):
        raise FaultNotReached("fault report fields do not match expected fault")
    if "replay_lsn" in value:
        try:
            parse_lsn_value(value["replay_lsn"])
        except PlanError as error:
            raise FaultNotReached("fault report replay_lsn is invalid") from error
    elif require_replay_lsn:
        raise FaultNotReached("fault report lacks actual replay_lsn")
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


# The page-pruning H1 slice: history below a configured cutoff is compacted;
# the three boundaries are the replacement layer publication, the retired
# sources' durable mark-delete, and the durable page-prune frontier advance.
# Each gc_seed workload names the H1 faults it can reach, the daemon flags
# that make the publication observable at the workload's scale, and the
# retained page-history horizon recovery must republish (None when the
# workload pins no page history).
# the deleted branch and its live sibling both fork from the root here, so the
# sibling keeps the root's history capped at this LSN after the target is gone
DELETE_FORK_LSN = 1024 * 1024
GC_WORKLOADS: dict[str, dict[str, Any]] = {
    "page_prune": {
        "faults": {
            "page_compaction.after_publish",
            "page_gc.after_mark_delete",
            "page_prune.after_frontier",
        },
        "daemon_args": [
            "--segment-size", "65536", "--flush-pages", "8", "--segment-gc", "0",
            "--wal-high-water-bytes", "1048576", "--wal-catch-up-bytes", "1",
        ],
        "retained_horizon": 3500,
    },
    "wal_index": {
        "faults": {"wal_index.after_frontier"},
        # one metadata-complete interval must make the timeline a snapshot
        # candidate; the geometric default trigger needs a mebibyte of index
        "daemon_args": ["--walidx-snapshot-bytes", "1"],
        "retained_horizon": None,
    },
    "wal_reclaim": {
        "faults": {
            "wal_reclaim.before_unlink",
            "wal_reclaim.after_unlink",
            "wal_reclaim.before_dir_fsync",
        },
        # the sealed prefix is three segments, below the high water so the
        # workload's own appends are never throttled behind the reclaim
        "daemon_args": ["--wal-high-water-bytes", "8388608",
                        "--wal-catch-up-bytes", "1"],
        "retained_horizon": None,
    },
    "timeline_delete": {
        "faults": {
            "timeline_delete.after_deleting",
            "timeline_delete.after_wal_cleanup",
            "timeline_delete.after_segment_rewrite",
            "timeline_delete.after_deleted",
        },
        # small segments and an eager flush give the branch an owner layer
        # and several shared segments for the deletion to filter
        "daemon_args": ["--segment-size", "65536", "--flush-pages", "8",
                        "--segment-gc", "0"],
        "retained_horizon": DELETE_FORK_LSN,
    },
    # the same seed, crashed on the old-state side of the first transition:
    # the request is lost and the branch must survive intact
    "timeline_delete_abort": {
        "faults": {"timeline_delete.before_deleting"},
        "daemon_args": ["--segment-size", "65536", "--flush-pages", "8",
                        "--segment-gc", "0"],
        "retained_horizon": DELETE_FORK_LSN,
    },
    "manifest_compact": {
        "faults": {
            "manifest_compact.after_tmp_sync",
            "manifest_compact.after_rename",
        },
        # eager flushes and a low layer-compaction threshold grow the manifest
        # log past its rewrite trigger; maintenance stays paused (the
        # {pause} file) until the workload has written every page
        "daemon_args": ["--segment-size", "65536", "--flush-pages", "8",
                        "--compact-layers", "2", "--segment-gc", "0",
                        "--test-maintenance-pause-file", "{pause}"],
        "retained_horizon": None,
        "pause_maintenance": True,
    },
    "forkmeta": {
        "faults": {
            "forkmeta.after_prepare",
            "forkmeta.after_manifest_commit",
            "forkmeta.after_source_rewrite",
            "forkmeta.after_snapshot_gc",
        },
        # the page_prune history proves the cutoff through its frontier; the
        # operational snapshot trigger is lowered to its floor so the seeded
        # fork-size events are enough to publish a generation
        "daemon_args": [
            "--segment-size", "65536", "--flush-pages", "8", "--segment-gc", "0",
            "--wal-high-water-bytes", "1048576", "--wal-catch-up-bytes", "1",
        ],
        "daemon_env": {"PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES": "1024"},
        "retained_horizon": 3500,
    },
}
# The supervisor's own pg_ctl stop and start are each bounded by its command
# timeout (never below 60s), and a stop request cannot interrupt one.
SUPERVISOR_COMMAND_TIMEOUT = 60
SUPERVISOR_STOP_TIMEOUT = 2 * SUPERVISOR_COMMAND_TIMEOUT + 15
FORKMETA_SNAPSHOTS = Path("forkmeta_snapshots")
FORKMETA_MANIFEST = FORKMETA_SNAPSHOTS / "forkmeta_manifest_v1"
FORKMETA_PREPARED = FORKMETA_SNAPSHOTS / "forkmeta_prepared_v1"
FORKMETA_V2_MAGIC = 0x324D4B46
FORKMETA_SNAPSHOT_BASE_KIND = 10
DELETE_BRANCH = 1
MANIFEST_TMP = "layers.manifest.tmp"
WAL_RECLAIM_SEGMENTS = 3
WAL_RECLAIM_SEGMENT_BYTES = 1024 * 1024
WAL_RECLAIM_TOTAL = WAL_RECLAIM_SEGMENTS * WAL_RECLAIM_SEGMENT_BYTES
WAL_STORE_METADATA_MAGIC = 0x4D535732        # "MSW2"
WAL_STORE_METADATA_VERSION = 2
WAL_STORE_METADATA_BYTES = 64
WAL_RECLAIM_SEGMENT_DIR = Path("wal_segments_0")
WAL_RECLAIM_SEGMENT_PREFIX = "walv1_1_"
WAL_RECLAIM_IDENTITY = WAL_RECLAIM_SEGMENT_DIR / "wal_store_identity_v1"
GC_WORKLOAD_FAULTS = {name: spec["faults"] for name, spec in GC_WORKLOADS.items()}
# Each gc_seed workload installs its condition once, so its faults are
# reachable exactly once per run; a plan asking for a later hit would wait
# for work the seed never creates again and expire as FaultNotReached.
GC_FAULT_MAX_HITS: dict[str, int] = {
    # the reclaim workload unlinks three sealed segments, and the snapshot
    # oracle reads hit N as the Nth unlink
    "wal_reclaim.after_unlink": 3,
}
GC_FAULT_DEFAULT_MAX_HIT = 1
GC_STAGES = {
    "page_compaction.after_publish": "publish",
    "page_gc.after_mark_delete": "mark_delete",
    "page_prune.after_frontier": "frontier",
    "wal_index.after_frontier": "walidx_frontier",
    "wal_reclaim.before_unlink": "reclaim_before_unlink",
    "wal_reclaim.after_unlink": "reclaim_after_unlink",
    "wal_reclaim.before_dir_fsync": "reclaim_before_dir_fsync",
    "timeline_delete.after_deleting": "delete_deleting",
    "timeline_delete.after_wal_cleanup": "delete_wal_cleanup",
    "timeline_delete.after_segment_rewrite": "delete_segment_rewrite",
    "timeline_delete.after_deleted": "delete_deleted",
    "timeline_delete.before_deleting": "delete_before_deleting",
    "manifest_compact.after_tmp_sync": "manifest_tmp_sync",
    "manifest_compact.after_rename": "manifest_rename",
    "forkmeta.after_prepare": "forkmeta_prepare",
    "forkmeta.after_manifest_commit": "forkmeta_manifest_commit",
    "forkmeta.after_source_rewrite": "forkmeta_source_rewrite",
    "forkmeta.after_snapshot_gc": "forkmeta_snapshot_gc",
}
WALIDX_SNAPSHOT_MAGIC = 0x4D534957          # "WISM"
WALIDX_SNAPSHOT_VERSION = 1
WALIDX_SNAPSHOT_HEADER_BYTES = 64
WALIDX_SNAPSHOT_ENTRY_BYTES = 16
WALIDX_PREPARED = Path("walidx_snapshots_0") / "walidx_prepared_v1"
WALIDX_MANIFEST = Path("walidx_snapshots_0") / "walidx_manifest_v1"


def _gc_fault_stage(fault_name: str) -> str | None:
    return GC_STAGES.get(fault_name)


MANIFEST_MAGIC = 0x504D414E
MANIFEST_VERSION = 3
MANIFEST_ADD_LAYER = 1
MANIFEST_MARK_DELETE = 4
PAGE_FRONTIER_MAGIC = 0x46504750
PAGE_FRONTIER_VERSION = 3
PAGE_FRONTIER_TIMELINES = 1024
PAGE_FRONTIER_SLOTS = 2
PAGE_FRONTIER_ENTRY_BYTES = 24        # incarnation, fence LSN, fence admission sequence


def _fnv1a32(data: bytes) -> int:
    crc = 2166136261
    for byte in data:
        crc ^= byte
        crc = (crc * 16777619) & 0xFFFFFFFF
    return crc


def _page_frontier_fences(store: Path, timeline: int) -> list[tuple[int, int, int]]:
    """(incarnation, fence LSN, fence admission sequence) of every slot the
    durable page-prune frontier file holds for the timeline, after checking
    the file's magic, version, and checksum.  Empty when the file is absent
    or invalid."""
    path = store / "page-prune.frontiers"
    try:
        data = path.read_bytes()
    except OSError:
        return []
    body = 8 + PAGE_FRONTIER_TIMELINES * PAGE_FRONTIER_SLOTS * PAGE_FRONTIER_ENTRY_BYTES
    if len(data) < body + 4:
        return []
    magic, version = struct.unpack_from("=II", data, 0)
    crc = struct.unpack_from("=I", data, body)[0]
    if magic != PAGE_FRONTIER_MAGIC or version != PAGE_FRONTIER_VERSION or \
            _fnv1a32(data[:body]) != crc:
        return []
    fences = []
    for slot in range(PAGE_FRONTIER_SLOTS):
        offset = 8 + (timeline * PAGE_FRONTIER_SLOTS + slot) * PAGE_FRONTIER_ENTRY_BYTES
        fences.append(struct.unpack_from("=QQQ", data, offset))
    return fences
MANIFEST_REMOVE_LAYER = 5


def _wal_segment_files(store: Path) -> list[str]:
    directory = store / WAL_RECLAIM_SEGMENT_DIR
    if not directory.is_dir():
        return []
    return sorted(
        entry.name for entry in directory.iterdir()
        if entry.name.startswith(WAL_RECLAIM_SEGMENT_PREFIX)
    )


def _wal_store_metadata(store: Path) -> dict[str, Any] | None:
    """The shipped-WAL store's durable metadata: which prefix the directory
    starts at, the base it retains, and how far it has been written.  None
    when the record is missing or does not validate."""
    try:
        data = (store / WAL_RECLAIM_IDENTITY).read_bytes()
    except OSError:
        return None
    if len(data) != WAL_STORE_METADATA_BYTES:
        return None
    # the daemon writes this record little-endian, not in host order
    magic, version, record_bytes, reserved = struct.unpack_from("<IIII", data, 0)
    timeline, segment_size = struct.unpack_from("<II", data, 16)
    directory_start, retained_base, end = struct.unpack_from("<QQQ", data, 24)
    crc = struct.unpack_from("<I", data, 48)[0]
    if magic != WAL_STORE_METADATA_MAGIC or \
            version != WAL_STORE_METADATA_VERSION or \
            record_bytes != WAL_STORE_METADATA_BYTES or reserved != 0 or \
            _fnv1a32(data[:48] + b"\x00" * 4 + data[52:]) != crc:
        return None
    return {
        "timeline": timeline, "segment_size": segment_size,
        "directory_start_lsn": directory_start,
        "retained_base_lsn": retained_base, "end_lsn": end,
    }

def _manifest_layers(records: list[tuple[int, bytes]]) -> dict[int, dict[str, Any]]:
    """Replay ADD/MARK_DELETE/REMOVE records to the layers the manifest still
    names, keyed by layer id, with their owning timeline."""
    layers: dict[int, dict[str, Any]] = {}
    for kind, payload in records:
        if kind == MANIFEST_ADD_LAYER and len(payload) >= 16:
            layer_id, _layer_kind, timeline = struct.unpack_from("=QII", payload, 0)
            layers[layer_id] = {"timeline": timeline, "deleting": False}
        elif kind in (MANIFEST_MARK_DELETE, MANIFEST_REMOVE_LAYER) and len(payload) >= 8:
            layer_id = struct.unpack_from("=Q", payload, 0)[0]
            if kind == MANIFEST_REMOVE_LAYER:
                layers.pop(layer_id, None)
            elif layer_id in layers:
                layers[layer_id]["deleting"] = True
    return layers


SEG_HEADER_BYTES = {
    0x53454732: 48, 0x53454730: 48, 0x53454731: 48, 0x53454733: 48,   # SEG2/0/1/3
    0x53454734: 56, 0x53454735: 56, 0x53454736: 56,                   # SEG4/5 bound, SEG6 admission
    0x53454737: 64, 0x53454738: 64,                                   # SEG7/8 bound + admission
}
TIMELINE_META_MAGIC = 0x324D4C54
TIMELINE_EVENT_STATE = 2
FORKMETA_PAYLOAD_MAGIC = 0x31534D46


def _segment_record_timelines(path: Path) -> list[int]:
    """The owning timeline of every complete page record in one segment
    file, decoded in host byte order; the scan stops at the first byte
    pattern that is not a record magic (the unused tail)."""
    data = path.read_bytes()
    timelines: list[int] = []
    offset = 0
    while offset + 48 <= len(data):
        magic, timeline = struct.unpack_from("=II", data, offset)
        header = SEG_HEADER_BYTES.get(magic)
        if header is None:
            break
        length = struct.unpack_from("=I", data, offset + 40)[0]
        if offset + header + length > len(data):
            break
        timelines.append(timeline)
        offset += header + length
    return timelines


def _timeline_events(store: Path) -> list[dict[str, int]]:
    """Every lifecycle record of the timelines log (create records carry no
    state), decoded in host byte order."""
    try:
        data = (store / "timelines").read_bytes()
    except OSError:
        return []
    events: list[dict[str, int]] = []
    offset = 0
    while offset + 8 <= len(data):
        magic, rec_len = struct.unpack_from("=II", data, offset)
        if magic != TIMELINE_META_MAGIC or rec_len < 8 or offset + rec_len > len(data):
            break
        if rec_len == 56:
            kind, ident, _parent, state = struct.unpack_from("=IIiI", data, offset + 8)
            incarnation = struct.unpack_from("=Q", data, offset + 32)[0]
            events.append({"kind": kind, "id": ident, "state": state, "incarnation": incarnation})
        elif rec_len == 32:
            events.append({"kind": 0, "id": struct.unpack_from("=I", data, offset + 8)[0],
                           "state": 0, "incarnation": 0})
        offset += rec_len
    return events


def _forkmeta_timelines(store: Path) -> set[int]:
    """Timelines named by fork-size events in the shared forkmeta source and
    in every snapshot payload part on disk (markers carry no owner)."""
    owners: set[int] = set()

    def scan(data: bytes, offset: int) -> None:
        while offset + 64 <= len(data):
            magic, rec_len, timeline = struct.unpack_from("=III", data, offset)
            if rec_len != 64 or magic & 0x00FFFFFF != 0x4D4B46:   # "FKM?" family
                break
            kind = data[offset + 60]
            if kind < 10:                                        # not a snapshot marker
                owners.add(timeline)
            offset += rec_len

    try:
        scan((store / "forkmeta").read_bytes(), 0)
    except OSError:
        pass
    snapshots = store / "forkmeta_snapshots"
    if snapshots.is_dir():
        for part in snapshots.iterdir():
            if not part.name.startswith(("forkmeta_checkpoint_v1_", "forkmeta_tail_v1_")):
                continue
            data = part.read_bytes()
            if len(data) >= 80 and struct.unpack_from("=I", data, 0)[0] == FORKMETA_PAYLOAD_MAGIC:
                scan(data, 80)
    return owners


def _manifest_removed_layers(records: list[tuple[int, bytes]]) -> dict[int, int]:
    """For every layer id the log removed and did not add again, the timeline
    that owned it when it went.  A file left behind by such a layer is an
    orphan of that timeline, not of whoever reuses the id later."""
    owner: dict[int, int] = {}
    removed: dict[int, int] = {}
    for kind, payload in records:
        if kind == MANIFEST_ADD_LAYER and len(payload) >= 16:
            layer_id, _layer_kind, timeline = struct.unpack_from("=QII", payload, 0)
            owner[layer_id] = timeline
            removed.pop(layer_id, None)
        elif kind == MANIFEST_REMOVE_LAYER and len(payload) >= 8:
            layer_id = struct.unpack_from("=Q", payload, 0)[0]
            if layer_id in owner:
                removed[layer_id] = owner.pop(layer_id)
    return removed

FORKMETA_RECORD_BYTES = 64
FORKMETA_SNAPSHOT_MAGIC = 0x4D534946
FORKMETA_SNAPSHOT_VERSION = 1
FORKMETA_SNAPSHOT_RECORD_BYTES = 80


def _fnv1a(data: bytes, crc: int = 2166136261) -> int:
    for byte in data:
        crc ^= byte
        crc = (crc * 16777619) & 0xFFFFFFFF
    return crc


def _forkmeta_snapshot_record(path: Path) -> dict[str, Any] | None:
    """The selected (manifest) or staged (prepared) generation record: its
    generation, cutoff, and the length and checksum of each immutable part.
    None when the file is absent, malformed, or fails its header checksum."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if len(data) != FORKMETA_SNAPSHOT_RECORD_BYTES:
        return None
    magic, version, header_bytes = struct.unpack_from("<III", data, 0)
    if magic != FORKMETA_SNAPSHOT_MAGIC or version != FORKMETA_SNAPSHOT_VERSION or \
            header_bytes != FORKMETA_SNAPSHOT_RECORD_BYTES:
        return None
    crc = struct.unpack_from("<I", data, 64)[0]
    if _fnv1a(data[:64] + b"\0\0\0\0" + data[68:]) != crc:
        return None
    generation, cutoff_lsn, cutoff_seq, checkpoint_len = struct.unpack_from("<QQQQ", data, 16)
    checkpoint_crc = struct.unpack_from("<I", data, 48)[0]
    tail_len = struct.unpack_from("<Q", data, 52)[0]
    tail_crc = struct.unpack_from("<I", data, 60)[0]
    return {
        "generation": generation, "cutoff_lsn": cutoff_lsn, "cutoff_seq": cutoff_seq,
        "checkpoint": (checkpoint_len, checkpoint_crc), "tail": (tail_len, tail_crc),
    }


def _forkmeta_part_name(generation: int, part: str) -> str:
    return f"forkmeta_{part}_v1_{generation:020d}"


def _forkmeta_parts_valid(store: Path, record: dict[str, Any]) -> list[str]:
    """Names of the record's parts that are missing, mis-sized, or fail the
    checksum the record carries for them."""
    broken = []
    for part in ("checkpoint", "tail"):
        length, crc = record[part]
        path = store / FORKMETA_SNAPSHOTS / _forkmeta_part_name(record["generation"], part)
        try:
            data = path.read_bytes()
        except OSError:
            broken.append(f"{path.name} (absent)")
            continue
        if len(data) != length or _fnv1a(data) != crc:
            broken.append(f"{path.name} (len {len(data)} vs {length}, checksum mismatch)")
    return broken


def _forkmeta_generation_files(store: Path) -> list[str]:
    directory = store / FORKMETA_SNAPSHOTS
    if not directory.is_dir():
        return []
    return sorted(
        entry.name for entry in directory.iterdir()
        if entry.name.startswith(("forkmeta_checkpoint_v1_", "forkmeta_tail_v1_"))
    )


def _forkmeta_source_head(store: Path) -> dict[str, Any] | None:
    """The first record of the shared forkmeta log, decoded in the host's
    byte order (the daemon persists the C struct directly)."""
    source = store / "forkmeta"
    try:
        with source.open("rb") as handle:
            record = handle.read(FORKMETA_RECORD_BYTES)
    except OSError:
        return None
    if len(record) != FORKMETA_RECORD_BYTES:
        return None
    magic, rec_len, timeline = struct.unpack_from("=III", record, 0)
    key = struct.unpack_from("=IIIiI", record, 12)
    lsn, admission_seq, order_id = struct.unpack_from("=QQQ", record, 32)
    nblocks = struct.unpack_from("=I", record, 56)[0]
    kind = record[60]
    pad = record[61:64]
    if magic != FORKMETA_V2_MAGIC or rec_len != FORKMETA_RECORD_BYTES:
        return None
    return {
        "timeline": timeline, "key": key, "lsn": lsn, "admission_seq": admission_seq,
        "order_id": order_id, "nblocks": nblocks, "kind": kind, "pad": pad,
    }


FORKMETA_CUTOFF_LSN = 3500          # the workload's sole proven page frontier
FORKMETA_SEED_FIRST_REL = 5000      # the gc client's oracle relations ...
FORKMETA_SEED_RELS = 32             # ... and their count
FORKMETA_SET_KIND = 1


def _forkmeta_source_records(store: Path, aligned: bool = False) -> list[dict[str, Any]] | None:
    """Every record of the shared forkmeta log, or None when the log is
    absent or not a whole number of well-formed V2 records."""
    try:
        data = (store / "forkmeta").read_bytes()
    except OSError:
        return None
    # The publication boundaries hold the admission write lock and every
    # shard lock, so no foreground append is in flight at those probes and a
    # partial record there is real damage.  The trickle workload does append
    # between them, so a crash image taken elsewhere may end mid-record and
    # recovery drops exactly that unacknowledged tail.
    complete = len(data) - len(data) % FORKMETA_RECORD_BYTES
    if aligned and complete != len(data):
        return None
    records = []
    for offset in range(0, complete, FORKMETA_RECORD_BYTES):
        record = data[offset:offset + FORKMETA_RECORD_BYTES]
        magic, rec_len, timeline = struct.unpack_from("=III", record, 0)
        if magic != FORKMETA_V2_MAGIC or rec_len != FORKMETA_RECORD_BYTES:
            return None
        lsn, admission_seq, order_id = struct.unpack_from("=QQQ", record, 32)
        records.append({
            "timeline": timeline, "key": struct.unpack_from("=IIIiI", record, 12),
            "lsn": lsn, "admission_seq": admission_seq, "order_id": order_id,
            "nblocks": struct.unpack_from("=I", record, 56)[0], "kind": record[60],
        })
    return records


def _forkmeta_seeded_events(above_lsn: int = 0) -> list[tuple[int, int, int, int]]:
    """Every event the gc client's forkmeta seed persists for its oracle
    relations, as (relation, lsn, kind, nblocks), above the given LSN.  The
    seed creates each relation, zero-extends it to four blocks, and truncates
    it below the cutoff for even relations and above it for odd ones."""
    events = []
    for index in range(FORKMETA_SEED_RELS):
        rel = FORKMETA_SEED_FIRST_REL + index
        truncate_lsn = 2500 + index if index % 2 == 0 else 4500 + index
        for lsn, kind, nblocks in (
            (1000 + index, FORKMETA_SET_KIND, 0),
            (2000 + index, FORKMETA_GROW_KIND, 4),
            (truncate_lsn, FORKMETA_SET_KIND, 2 if index % 2 == 0 else 3),
        ):
            if lsn > above_lsn:
                events.append((rel, lsn, kind, nblocks))
    return events


def _forkmeta_seeded_events_missing(records: list[dict[str, Any]],
                                    above_lsn: int) -> list[str]:
    """The seeded events above the given LSN that the log no longer carries
    with the same size and kind."""
    present = {
        (r["key"][2], r["lsn"], r["kind"], r["nblocks"]) for r in records
        if r["timeline"] == 0
    }
    return [
        f"rel {rel} kind {kind} lsn {lsn} nblocks {nblocks}"
        for rel, lsn, kind, nblocks in _forkmeta_seeded_events(above_lsn)
        if (rel, lsn, kind, nblocks) not in present
    ]


def _forkmeta_seeded_truncates_missing(records: list[dict[str, Any]], above_lsn: int) -> list[str]:
    """The seeded truncate events (one per oracle relation, at a known LSN)
    above the given LSN that the log no longer carries."""
    present = {
        (r["key"][2], r["lsn"]) for r in records
        if r["timeline"] == 0 and r["kind"] == FORKMETA_SET_KIND
    }
    missing = []
    for index in range(FORKMETA_SEED_RELS):
        lsn = 2500 + index if index % 2 == 0 else 4500 + index
        rel = FORKMETA_SEED_FIRST_REL + index
        if lsn > above_lsn and (rel, lsn) not in present:
            missing.append(f"rel {rel} truncate@{lsn}")
    return missing


FORKMETA_PAYLOAD_HEADER_BYTES = 80


FORKMETA_MAX_TIMELINES = 1024
FORKMETA_MAX_KLASS = 6                 # PS_KLASS_READER_SNAPSHOT
FORKMETA_DEAD_KIND = 2
FORKMETA_SEG_GROW_KIND = 5
FORKMETA_SEG_COMMIT_KIND = 6
FORKMETA_SEG_GROW_BOUND_KIND = 7
FORKMETA_SEG_COMMIT_BOUND_KIND = 8


def _forkmeta_payload_header(store: Path, record: dict[str, Any],
                             part: str) -> dict[str, Any] | None:
    """The payload header of one part, decoded; None when unreadable."""
    path = store / FORKMETA_SNAPSHOTS / _forkmeta_part_name(record["generation"], part)
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if len(data) < FORKMETA_PAYLOAD_HEADER_BYTES:
        return None
    magic, version, header_bytes = struct.unpack_from("=IHH", data, 0)
    index, record_bytes = struct.unpack_from("=II", data, 8)
    generation, cutoff_lsn, cutoff_seq, freeze_seq = struct.unpack_from("=QQQQ", data, 16)
    checkpoint_records, tail_records, checkpoint_bytes, tail_bytes = \
        struct.unpack_from("=QQQQ", data, 48)
    return {
        "magic": magic, "version": version, "header_bytes": header_bytes,
        "part": index, "record_bytes": record_bytes, "generation": generation,
        "cutoff_lsn": cutoff_lsn, "cutoff_seq": cutoff_seq, "freeze_seq": freeze_seq,
        "checkpoint_records": checkpoint_records, "tail_records": tail_records,
        "checkpoint_bytes": checkpoint_bytes, "tail_bytes": tail_bytes,
        "body": len(data) - FORKMETA_PAYLOAD_HEADER_BYTES,
    }


def _forkmeta_event_future(lsn: int, seq: int, cutoff_lsn: int, cutoff_seq: int) -> bool:
    """The loader's partition test: at the cutoff LSN a nonzero sequence
    above the cutoff sequence still belongs to the tail."""
    return lsn > cutoff_lsn or (lsn == cutoff_lsn and seq != 0 and seq > cutoff_seq)


def _forkmeta_record_invalid(entry: dict[str, Any], part: str,
                             cutoff_lsn: int, cutoff_seq: int) -> str | None:
    """Why one staged record would be refused by the loader: its identity,
    class, kind and order-id rules, its zero pad, and the partition it is
    stored in."""
    kind = entry["kind"]
    ordered = FORKMETA_SEG_GROW_KIND <= kind <= FORKMETA_SEG_COMMIT_BOUND_KIND
    bound = kind in (FORKMETA_SEG_GROW_BOUND_KIND, FORKMETA_SEG_COMMIT_BOUND_KIND)
    if entry["magic"] != FORKMETA_V2_MAGIC or entry["rec_len"] != FORKMETA_RECORD_BYTES:
        return f"record magic {entry['magic']:#x} length {entry['rec_len']}"
    if entry["timeline"] >= FORKMETA_MAX_TIMELINES or entry["key"][4] > FORKMETA_MAX_KLASS:
        return f"record timeline {entry['timeline']} class {entry['key'][4]}"
    if entry["pad"] != b"\x00\x00\x00":
        return f"record pad {entry['pad']!r}"
    if not ordered and (kind > FORKMETA_DEAD_KIND or entry["order_id"] != 0):
        return f"lifecycle record kind {kind} order {entry['order_id']}"
    if ordered and (entry["nblocks"] == 0 or
                    (bound and entry["order_id"] == 0) or
                    (not bound and (entry["order_id"] != 0 or
                                    entry["admission_seq"] != 0))):
        return (f"ordered marker kind {kind} order {entry['order_id']} "
                f"seq {entry['admission_seq']} nblocks {entry['nblocks']}")
    if kind == FORKMETA_DEAD_KIND and entry["nblocks"] != 0:
        return f"death record with {entry['nblocks']} blocks"
    future = _forkmeta_event_future(entry["lsn"], entry["admission_seq"],
                                    cutoff_lsn, cutoff_seq)
    if (part == "checkpoint" and future) or (part == "tail" and not future):
        return (f"{part} record at lsn {entry['lsn']} seq {entry['admission_seq']} "
                f"is on the wrong side of the cutoff")
    return None


def _forkmeta_part_framing(store: Path, record: dict[str, Any], part: str) -> str | None:
    """Why a part does not satisfy the framing invariants the loader applies:
    its payload header must identify the part and the generation, its record
    size must be the record size, and its body must be a whole number of
    records matching the count the header states."""
    path = store / FORKMETA_SNAPSHOTS / _forkmeta_part_name(record["generation"], part)
    try:
        data = path.read_bytes()
    except OSError:
        return f"{path.name} is unreadable"
    if len(data) < FORKMETA_PAYLOAD_HEADER_BYTES:
        return f"{path.name} is shorter than its payload header"
    header = _forkmeta_payload_header(store, record, part)
    other = _forkmeta_payload_header(
        store, record, "tail" if part == "checkpoint" else "checkpoint")
    if header is None:
        return f"{path.name} is shorter than its payload header"
    expected_index = 0 if part == "checkpoint" else 1
    if header["magic"] != FORKMETA_PAYLOAD_MAGIC or header["version"] != 1 or \
            header["header_bytes"] != FORKMETA_PAYLOAD_HEADER_BYTES or \
            header["record_bytes"] != FORKMETA_RECORD_BYTES or \
            header["part"] != expected_index or \
            header["generation"] != record["generation"]:
        return (f"{path.name} has payload header magic {header['magic']:#x} version "
                f"{header['version']} header {header['header_bytes']} record "
                f"{header['record_bytes']} part {header['part']} generation "
                f"{header['generation']}")
    # the cutoff the header states is the one the record was selected with,
    # and the freeze sequence that froze appends must be real
    if header["cutoff_lsn"] != record["cutoff_lsn"] or \
            header["cutoff_seq"] != record["cutoff_seq"] or header["freeze_seq"] == 0:
        return (f"{path.name} states cutoff ({header['cutoff_lsn']}, "
                f"{header['cutoff_seq']}) freeze {header['freeze_seq']}, record says "
                f"({record['cutoff_lsn']}, {record['cutoff_seq']})")
    if other is not None and any(header[field] != other[field] for field in (
            "generation", "cutoff_lsn", "cutoff_seq", "freeze_seq",
            "checkpoint_records", "tail_records", "checkpoint_bytes", "tail_bytes")):
        return f"{path.name} disagrees with the other part's payload header"
    body = header["body"]
    if body % FORKMETA_RECORD_BYTES != 0:
        return f"{path.name} ends with a partial record ({body} body bytes)"
    stated = header["checkpoint_records"] if part == "checkpoint" else header["tail_records"]
    stated_bytes = header["checkpoint_bytes"] if part == "checkpoint" else header["tail_bytes"]
    if stated * FORKMETA_RECORD_BYTES != body or stated_bytes != body:
        return (f"{path.name} holds {body // FORKMETA_RECORD_BYTES} records, "
                f"header states {stated} ({stated_bytes} bytes)")
    # every record, and the per-fork ordering the loader enforces
    seen: dict[tuple[int, tuple[int, ...]], tuple[int, int]] = {}
    for index, entry in enumerate(_forkmeta_part_records(store, record, part)):
        invalid = _forkmeta_record_invalid(entry, part, record["cutoff_lsn"],
                                           record["cutoff_seq"])
        if invalid is not None:
            return f"{path.name} index {index}: {invalid}"
        previous = seen.get((entry["timeline"], entry["key"]))
        position = (entry["lsn"], entry["admission_seq"])
        if previous is not None and (
                previous[0] > position[0] or
                (previous[0] == position[0] and previous[1] != 0 and
                 position[1] != 0 and previous[1] > position[1])):
            return (f"{path.name} index {index} steps back from {previous} to "
                    f"{position} for one fork")
        seen[(entry["timeline"], entry["key"])] = position
    return None


def _forkmeta_part_records(store: Path, record: dict[str, Any], part: str) -> list[dict[str, Any]]:
    """The fork-size records of one immutable snapshot part (after its
    80-byte payload header); empty when the part cannot be read."""
    path = store / FORKMETA_SNAPSHOTS / _forkmeta_part_name(record["generation"], part)
    try:
        data = path.read_bytes()
    except OSError:
        return []
    records = []
    for offset in range(80, len(data) - FORKMETA_RECORD_BYTES + 1, FORKMETA_RECORD_BYTES):
        chunk = data[offset:offset + FORKMETA_RECORD_BYTES]
        magic, rec_len = struct.unpack_from("=II", chunk, 0)
        records.append({
            "magic": magic, "rec_len": rec_len,
            "timeline": struct.unpack_from("=I", chunk, 8)[0],
            "key": struct.unpack_from("=IIIiI", chunk, 12),
            "lsn": struct.unpack_from("=Q", chunk, 32)[0],
            "admission_seq": struct.unpack_from("=Q", chunk, 40)[0],
            "order_id": struct.unpack_from("=Q", chunk, 48)[0],
            "nblocks": struct.unpack_from("=I", chunk, 56)[0], "kind": chunk[60],
            "pad": chunk[61:64],
        })
    return records


FORKMETA_GROW_KIND = 0


def _forkmeta_size_asof(events: list[dict[str, Any]], horizon: int) -> int:
    """The relation size the records reconstruct at one horizon: the newest
    definitive size at or below it, raised by any growth after that."""
    size = 0
    for record in sorted(events, key=lambda r: (r["lsn"], r.get("admission_seq", 0))):
        if record["lsn"] > horizon:
            break
        if record["kind"] == FORKMETA_SET_KIND:
            size = record["nblocks"]
        elif record["kind"] == FORKMETA_GROW_KIND:
            size = max(size, record["nblocks"])
    return size


def _forkmeta_generation_incomplete(store: Path, record: dict[str, Any]) -> str | None:
    """Why a staged or selected generation does not carry the seeded history.
    The seed grows every oracle relation to four blocks and then truncates it,
    below the cutoff for even relations and above it for odd ones, so the
    generation's records must reconstruct four blocks at the cutoff for an odd
    relation and its truncated size for an even one, and the truncated size
    for every relation at the newest horizon.  Sizes are compared rather than
    a record list, because which records survive is the compaction plan's
    business while the sizes are the contract."""
    for part in ("checkpoint", "tail"):
        framing = _forkmeta_part_framing(store, record, part)
        if framing is not None:
            return framing
    missing = _forkmeta_seeded_truncates_missing(
        _forkmeta_part_records(store, record, "tail"), record["cutoff_lsn"])
    if missing:
        return f"tail of generation {record['generation']} lacks {missing!r}"
    events: dict[int, list[dict[str, Any]]] = {}
    for part in ("checkpoint", "tail"):
        for entry in _forkmeta_part_records(store, record, part):
            if entry["timeline"] == 0 and \
                    entry["kind"] in (FORKMETA_SET_KIND, FORKMETA_GROW_KIND):
                events.setdefault(entry["key"][2], []).append(entry)
    wrong = []
    for index in range(FORKMETA_SEED_RELS):
        rel = FORKMETA_SEED_FIRST_REL + index
        truncated = 2 if index % 2 == 0 else 3
        at_cutoff = truncated if index % 2 == 0 else 4
        entries = events.get(rel)
        if not entries:
            wrong.append(f"rel {rel} absent")
            continue
        cutoff_size = _forkmeta_size_asof(entries, record["cutoff_lsn"])
        newest_size = _forkmeta_size_asof(entries, 2**64 - 1)
        if cutoff_size != at_cutoff or newest_size != truncated:
            wrong.append(
                f"rel {rel} reconstructs {cutoff_size} blocks at the cutoff and "
                f"{newest_size} at the newest horizon, expected {at_cutoff} and {truncated}"
            )
    if wrong:
        return f"generation {record['generation']} misstates {wrong!r}"
    return None


def _forkmeta_old_epoch_intact(store: Path, selected: dict[str, Any]) -> str | None:
    """Why the source is not the complete epoch that preceded the selected
    generation: it must be a well-formed log that starts with the previous
    generation's marker (or, for the first generation, with no marker at
    all), names the selected generation nowhere, and still carries every
    seeded truncate event that epoch is responsible for."""
    records = _forkmeta_source_records(store)
    if not records:
        return "source absent, empty or malformed"
    head = records[0]
    previous = selected["generation"] - 1
    if previous == 0:
        if head["kind"] >= FORKMETA_SNAPSHOT_BASE_KIND:
            return f"first generation's source starts with a marker {head!r}"
        floor = 0
    elif head["kind"] != FORKMETA_SNAPSHOT_BASE_KIND or head["order_id"] != previous:
        return f"source does not start with generation {previous}'s marker: {head!r}"
    else:
        floor = head["lsn"]
    if any(r["kind"] >= FORKMETA_SNAPSHOT_BASE_KIND and
           r["order_id"] == selected["generation"] for r in records):
        return "source already names the selected generation"
    # the complete epoch, not only its truncates: a premature rewrite that
    # kept the truncates and dropped the creates and growths would otherwise
    # look intact, because the selected snapshot supplies them after startup
    missing = _forkmeta_seeded_events_missing(records, floor)
    if missing:
        return f"source lost seeded events of the old epoch: {missing!r}"
    return None


def _forkmeta_marker_matches(store: Path, selected: dict[str, Any]) -> bool:
    """True when the source epoch starts with the selected generation's exact
    snapshot-base marker, the same test startup applies before it preserves
    the epoch's suffix."""
    head = _forkmeta_source_head(store)
    return head is not None and head["kind"] == FORKMETA_SNAPSHOT_BASE_KIND and \
        head["timeline"] == 0 and not any(head["key"]) and \
        head["lsn"] == selected["cutoff_lsn"] and \
        head["admission_seq"] == selected["cutoff_seq"] and \
        head["order_id"] == selected["generation"] and head["nblocks"] == 0 and \
        head["pad"] == b"\0\0\0"


def _forkmeta_source_starts_with_marker(store: Path) -> bool:
    selected = _forkmeta_snapshot_record(store / FORKMETA_MANIFEST)
    return selected is not None and _forkmeta_marker_matches(store, selected)


def _forkmeta_cutoff_mismatch(store: Path, record: dict[str, Any]) -> str | None:
    """Why a generation's cutoff is not the durable page frontier: the
    runtime selects the full (lsn, admission sequence) tuple the frontier
    published, so a record that keeps the LSN and misstates the sequence is
    a boundary error even though every functional query still passes."""
    fences = _page_frontier_fences(store, 0)
    proven = [
        (lsn, seq) for incarnation, lsn, seq in fences
        if incarnation == 1 and lsn == FORKMETA_CUTOFF_LSN
    ]
    if not proven:
        return (f"has no durable timeline 0 frontier at {FORKMETA_CUTOFF_LSN}: "
                f"{fences!r}")
    if record["cutoff_lsn"] != FORKMETA_CUTOFF_LSN or \
            all(record["cutoff_seq"] != seq for _lsn, seq in proven):
        return (f"states cutoff ({record['cutoff_lsn']}, {record['cutoff_seq']}), "
                f"expected the proven frontier {proven!r}")
    return None


def _check_forkmeta_crash_snapshot(
    store: Path, stage: str
) -> dict[str, Any] | None:
    """Prepare leaves a staged generation whose two parts are complete and
    checksum-valid, without a selected manifest; commit selects it while the
    source still names the old epoch; the rewrite puts the selected
    generation's exact marker at the head of the source; GC leaves exactly
    the selected second generation's valid pair."""
    prepared = _forkmeta_snapshot_record(store / FORKMETA_PREPARED)
    selected = _forkmeta_snapshot_record(store / FORKMETA_MANIFEST)
    files = _forkmeta_generation_files(store)
    if stage == "forkmeta_prepare":
        if prepared is None or selected is not None:
            raise OracleMismatch(
                f"after_prepare crash left prepared={prepared!r} selected={selected!r}"
            )
        broken = _forkmeta_parts_valid(store, prepared)
        if broken:
            raise OracleMismatch(
                f"after_prepare crash staged generation {prepared['generation']} "
                f"with incomplete parts {broken!r}"
            )
        stale = _forkmeta_cutoff_mismatch(store, prepared)
        if stale is not None:
            raise OracleMismatch(f"after_prepare crash {stale}")
        incomplete = _forkmeta_generation_incomplete(store, prepared)
        if incomplete is not None:
            raise OracleMismatch(f"after_prepare crash: {incomplete}")
        # nothing is selected yet, so recovery is free to publish whatever
        # generation it settles on
        return None
    if selected is None:
        raise OracleMismatch(f"after_{stage} crash left no valid selected forkmeta manifest")
    if prepared is not None or (store / FORKMETA_PREPARED).exists():
        # the commit consumes the intent; a leftover would be re-committed or
        # aborted by startup, which hides an incomplete commit
        raise OracleMismatch(
            f"after_{stage} crash left a prepared intent behind the selected "
            f"generation {selected['generation']}: {prepared!r}"
        )
    broken = _forkmeta_parts_valid(store, selected)
    if broken:
        raise OracleMismatch(
            f"after_{stage} crash selected generation {selected['generation']} "
            f"with invalid parts {broken!r}"
        )
    stale = _forkmeta_cutoff_mismatch(store, selected)
    if stale is not None:
        raise OracleMismatch(f"after_{stage} crash {stale}")
    incomplete = _forkmeta_generation_incomplete(store, selected)
    if incomplete is not None:
        raise OracleMismatch(f"after_{stage} crash: {incomplete}")
    matches = _forkmeta_marker_matches(store, selected)
    if stage == "forkmeta_manifest_commit":
        # the complete old epoch is still the source behind the new manifest
        stale = _forkmeta_old_epoch_intact(store, selected)
        if stale is not None:
            raise OracleMismatch(f"after_manifest_commit crash: {stale}")
    if stage in ("forkmeta_source_rewrite", "forkmeta_snapshot_gc"):
        # the source belongs to the selected generation from the rewrite on,
        # and snapshot GC touches only the retired files, so it must still
        # carry that marker and nothing that contradicts it
        if not matches:
            raise OracleMismatch(
                f"after_{stage} crash left the forkmeta source without the "
                f"selected generation's exact marker: {_forkmeta_source_head(store)!r}"
            )
        # The rewritten source is the marker plus whatever was appended
        # after the freeze; the seeded history lives in the parts, checked
        # above, so the source only has to be well formed and marker-only
        # before the first post-freeze append.  These probes fire with the
        # admission and shard write locks held, so no append is in flight
        # and a partial record is damage rather than a crash tail.
        records = _forkmeta_source_records(store, aligned=True)
        if records is None or any(
                r["kind"] >= FORKMETA_SNAPSHOT_BASE_KIND for r in records[1:]):
            raise OracleMismatch(
                f"after_{stage} crash left a malformed rewritten source "
                f"or a second marker: {records!r}"
            )
    # The seed starts from an empty store, so the commit and the rewrite
    # publish its first generation and nothing else can be on disk, while
    # snapshot GC retires that one and leaves the second.  Requiring the exact
    # pair is what rejects an orphan generation left by a faulty first
    # publication: startup schedules snapshot GC unconditionally, so the
    # recovery oracle would see the leak already swept up.
    expected_generation = 2 if stage == "forkmeta_snapshot_gc" else 1
    expected = sorted(
        _forkmeta_part_name(selected["generation"], part)
        for part in ("checkpoint", "tail")
    )
    if selected["generation"] != expected_generation or files != expected:
        raise OracleMismatch(
            f"after_{stage} crash selected generation {selected['generation']} "
            f"and left {files!r}, expected exactly generation "
            f"{expected_generation}'s {expected!r}"
        )
    return selected


def _check_forkmeta_recovery(
    store: Path, stage: str, timeout: float,
    crash_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Recovery selects one durable generation whose parts are valid, finishes
    any staged or retired file cleanup, and serves the source behind that
    generation's exact marker.  Returns the settled generation record so the
    clean restart can be held to the same one."""
    poll_timeout = max(0.0, min(10.0, timeout))
    deadline = time.monotonic() + poll_timeout
    while True:
        selected = _forkmeta_snapshot_record(store / FORKMETA_MANIFEST)
        files = _forkmeta_generation_files(store)
        prepared = (store / FORKMETA_PREPARED).exists()
        if selected is not None and not prepared and \
                not _forkmeta_parts_valid(store, selected) and \
                files == sorted(
                    _forkmeta_part_name(selected["generation"], part)
                    for part in ("checkpoint", "tail")
                ) and _forkmeta_marker_matches(store, selected):
            stale = _forkmeta_cutoff_mismatch(store, selected)
            if stale is not None:
                raise OracleMismatch(f"after_{stage} recovery {stale}")
            # The crash had already selected a generation, and the probe holds
            # the locks that would let an acknowledged write land, so recovery
            # has nothing new to publish.  Accepting whatever it settles on
            # would let one unnecessary republication through, since the clean
            # restart is then compared with that.
            crash_selected = (crash_state or {}).get("forkmeta_selected")
            if crash_selected is not None and selected != crash_selected:
                raise OracleMismatch(
                    f"after_{stage} recovery settled on {selected!r} instead of "
                    f"the generation the crash had selected, {crash_selected!r}"
                )
            return selected
        now = time.monotonic()
        if now >= deadline:
            raise HarnessTimeout(
                f"after_{stage} recovery did not settle the forkmeta snapshot: "
                f"selected={selected!r} prepared={prepared} files={files!r} "
                f"head={_forkmeta_source_head(store)!r}"
            )
        time.sleep(min(0.05, deadline - now))


def _check_manifest_crash_snapshot(store: Path, stage: str) -> None:
    """Before the rename the live log is intact next to the fsync'd temp
    file; after it the compacted log has replaced the live log."""
    manifest = store / "layers.manifest"
    if not manifest.exists() or manifest.stat().st_size == 0:
        raise OracleMismatch(f"after_{stage} crash left no layers.manifest")
    tmp = store / MANIFEST_TMP
    if stage == "manifest_tmp_sync":
        if not tmp.exists():
            raise OracleMismatch("after_tmp_sync crash left no compacted temp manifest")
        # The boundary is a complete, fsynced rewrite.  A temp file that had
        # only been created and truncated would satisfy a presence check, and
        # recovery would then discard it, replay the intact live log, and
        # satisfy every later check -- the scenario would pass while testing
        # nothing.  Require the compacted log to replay to the live one.
        compacted = _manifest_layers(_manifest_records_at(tmp))
        live = _manifest_layers(_manifest_records(store))
        if not compacted or compacted != live:
            raise OracleMismatch(
                f"after_tmp_sync crash left a temp manifest replaying to "
                f"{compacted!r}, expected the live log's {live!r}"
            )
    if stage == "manifest_rename" and tmp.exists():
        raise OracleMismatch("after_rename crash left the temp manifest after its rename")


def _check_manifest_recovery(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    store: Path,
    stage: str,
    timeout: float,
) -> dict[str, Any] | None:
    """Either log replays to the same layer map: the manifest is sane and
    reconciled with the local layers, and no crashed temp file leaks."""
    poll_timeout = max(0.0, min(10.0, timeout))
    deadline = time.monotonic() + poll_timeout
    while True:
        manifest = inspect_store(inspector, shm, "manifest", inspection_schema)
        if manifest.get("manifest_poisoned") is not False:
            raise OracleMismatch(
                f"after_{stage} recovery reported manifest_poisoned="
                f"{manifest.get('manifest_poisoned')!r}"
            )
        if manifest.get("deleting_layers") == 0 and \
                manifest.get("layer_count") == manifest.get("local_layers") and \
                isinstance(manifest.get("layer_count"), int) and \
                manifest.get("layer_count") > 0:
            break
        now = time.monotonic()
        if now >= deadline:
            raise HarnessTimeout(
                f"after_{stage} recovery did not reconcile the manifest within "
                f"{poll_timeout:.3f}s; last manifest={manifest!r}"
            )
        time.sleep(min(0.05, deadline - now))
    if (store / MANIFEST_TMP).exists():
        raise OracleMismatch(f"after_{stage} recovery kept the crashed temp manifest")
    owners = inspect_store(inspector, shm, "owners", inspection_schema)
    if owners.get("retention_poisoned") is not False or owners.get("owner_count") != 0:
        raise OracleMismatch(
            f"after_{stage} recovery reported owners={owners!r}, expected none"
        )
    # the counts come from the replayed map, so they say nothing about files
    # an interrupted retirement may have left behind
    replayed = set(_manifest_layers(_manifest_records(store)))
    on_disk = {int(path.name.rsplit("_", 1)[1], 16) for path in _canonical_layer_files(store)}
    if replayed != on_disk:
        raise OracleMismatch(
            f"after_{stage} recovery left the manifest naming {sorted(replayed)!r} "
            f"while the store holds {sorted(on_disk)!r}"
        )
    return None


# A sealed segment is named walv1_<store>_<segment number, 20 digits>; the
# publication writes a "...tmp.<suffix>" file first, which is not one.
SEALED_WAL_SEGMENT = re.compile(r"^walv1_[0-9]+_([0-9]{20})$")
DELETE_WAL_SEGMENT_NUMBER = 1          # the branch's WAL starts at 1 MiB


def _branch_sealed_wal_segments(store: Path, timeline: int) -> list[str]:
    """The canonical sealed immutable WAL segment files of one timeline.  The
    directory itself appears on the first aligned append, before any segment
    is sealed, and a staging file is not a sealed segment either, so neither
    says anything about immutable-WAL cleanup on its own."""
    directory = store / f"wal_segments_{timeline}"
    if not directory.is_dir():
        return []
    return sorted(
        entry.name for entry in directory.iterdir()
        if entry.is_file() and SEALED_WAL_SEGMENT.fullmatch(entry.name)
    )


def _branch_seeded_wal_segment(store: Path, timeline: int) -> bool:
    """Whether the segment the seed's aligned WAL fills is sealed under its
    canonical name."""
    return any(
        int(SEALED_WAL_SEGMENT.fullmatch(name).group(1)) == DELETE_WAL_SEGMENT_NUMBER
        for name in _branch_sealed_wal_segments(store, timeline)
    )


def _branch_private_artifacts(store: Path, timeline: int) -> list[str]:
    """Private WAL and WAL-index artifacts of one timeline, plus the layer
    files the manifest still attributes to it.  Layer file names carry the
    shard, not the owner, so ownership comes from the manifest's ADD records.
    """
    names = []
    prefixes = (f"walidx_{timeline}_", f"wal_segments_{timeline}", f"walidx_snapshots_{timeline}")
    for entry in sorted(store.iterdir()):
        name = entry.name
        if name == f"wal_{timeline}" or name.startswith(f"wal_{timeline}.") or \
                any(name.startswith(prefix) for prefix in prefixes):
            names.append(name)
    records = _manifest_records(store)
    owned = {
        layer_id for layer_id, layer in _manifest_layers(records).items()
        if layer["timeline"] == timeline
    }
    on_disk = {int(path.name.rsplit("_", 1)[1], 16) for path in _canonical_layer_files(store)}
    # A layer the manifest still attributes to the owner is an artifact
    # whether or not its file is still there (publishing DELETED before the
    # manifest REMOVE would otherwise look artifact-free after an unlink),
    # and so is a file left behind by a layer the manifest has already
    # removed but whose unlink failed or was skipped.
    orphaned = {
        layer_id for layer_id, layer_timeline in _manifest_removed_layers(records).items()
        if layer_timeline == timeline
    }
    for layer_id in sorted(owned | (orphaned & on_disk)):
        names.append(f"layer_{layer_id:#x} (layer of timeline {timeline}, "
                     f"{'on disk' if layer_id in on_disk else 'file gone'}, "
                     f"{'in the manifest' if layer_id in owned else 'orphaned'})")
    return names


def _check_delete_abort_crash_snapshot(store: Path) -> None:
    """Before the DELETING record is durable nothing of the branch has
    changed: no lifecycle record, and every seeded artifact present."""
    _check_delete_abort_state(store)


def _check_delete_abort_recovery(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    store: Path,
) -> dict[str, Any]:
    """The branch survived the lost request intact, and the root still
    carries the cap both live branches fork at."""
    state = _check_delete_abort_state(store)
    # The client reads only pages each branch owns, so nothing else here would
    # notice a cap recovery dropped -- and dropping it admits pruning of the
    # ancestor history those branches read through.
    timeline = inspect_store(inspector, shm, "timeline", inspection_schema, timeline=0)
    if timeline.get("retained_horizon") != DELETE_FORK_LSN:
        raise OracleMismatch(
            "a deletion that never became durable reported retained_horizon="
            f"{timeline.get('retained_horizon')!r}, expected the branches' fork "
            f"point {DELETE_FORK_LSN}"
        )
    return state


def _check_delete_crash_snapshot(store: Path, stage: str) -> None:
    """DELETING is durable at every stage; the owner's private WAL artifacts
    survive the first boundary and are gone from the second on, and nothing
    of the owner remains once DELETED is durable."""
    artifacts = _branch_private_artifacts(store, DELETE_BRANCH)
    wal = [name for name in artifacts if not name.startswith("layer_")]
    if stage == "delete_deleting":
        # every consumer the later stages claim to clean up must exist now,
        # or their absence afterwards proves nothing
        missing = [
            what for what, present in (
                ("private WAL", f"wal_{DELETE_BRANCH}" in wal),
                ("WAL-index epoch", any(n.startswith(f"walidx_{DELETE_BRANCH}_") for n in wal)),
                ("owner layer", any(n.startswith("layer_") for n in artifacts)),
                ("sealed immutable WAL segment",
                 _branch_seeded_wal_segment(store, DELETE_BRANCH)),
                ("fork metadata", DELETE_BRANCH in _forkmeta_timelines(store)),
            ) if not present
        ]
        if missing:
            raise OracleMismatch(
                f"after_deleting crash found the branch without its seeded {missing!r}: "
                f"{artifacts!r}"
            )
        return
    if wal:
        raise OracleMismatch(
            f"after_{stage} crash left private WAL artifacts {wal!r}"
        )
    if stage == "delete_segment_rewrite":
        # the probe fires after one shared segment was atomically rewritten
        # without the owner's records: some segment must now hold survivors
        # of timeline 0 and nothing of the deleted owner
        rewritten = [
            path.name for path in sorted(store.glob("seg_*"))
            for owners in [_segment_record_timelines(path)]
            if owners and DELETE_BRANCH not in owners
        ]
        if not rewritten:
            raise OracleMismatch(
                "after_segment_rewrite crash left no shared segment rewritten "
                "without the deleted owner's records"
            )
    if stage == "delete_deleted":
        if artifacts:
            raise OracleMismatch(
                f"after_deleted crash left owner artifacts {artifacts!r}"
            )
        events = [e for e in _timeline_events(store) if e["id"] == DELETE_BRANCH]
        if not events or events[-1]["kind"] != TIMELINE_EVENT_STATE or \
                events[-1]["state"] != 3 or events[-1]["incarnation"] != 1:
            raise OracleMismatch(
                "after_deleted crash did not leave a durable DELETED event with the "
                f"seeded incarnation as the branch's latest record: {events[-1:]!r}"
            )


def _check_delete_abort_state(store: Path) -> dict[str, Any]:
    """A deletion whose DELETING record never became durable leaves nothing
    behind: the branch keeps its lifecycle, and every artifact it owned is
    still there (the verify client already read its pages back)."""
    events = [e for e in _timeline_events(store) if e["id"] == DELETE_BRANCH]
    lifecycle = [e for e in events if e["kind"] == TIMELINE_EVENT_STATE]
    if lifecycle:
        raise OracleMismatch(
            "a deletion that never became durable left lifecycle records "
            f"{lifecycle!r}"
        )
    artifacts = _branch_private_artifacts(store, DELETE_BRANCH)
    missing = [
        what for what, present in (
            ("private WAL", f"wal_{DELETE_BRANCH}" in artifacts),
            ("WAL-index epoch",
             any(n.startswith(f"walidx_{DELETE_BRANCH}_") for n in artifacts)),
            ("sealed immutable WAL segment",
             _branch_seeded_wal_segment(store, DELETE_BRANCH)),
            # both halves of the layer must still be there: a manifest entry
            # whose file was unlinked, or a file whose entry was removed, is
            # half a retirement the aborted deletion must not have started
            ("owner layer in the manifest and on disk",
             any(n.startswith("layer_") and "on disk" in n and "in the manifest" in n
                 for n in artifacts)),
            ("fork metadata", DELETE_BRANCH in _forkmeta_timelines(store)),
        ) if not present
    ]
    if missing:
        raise OracleMismatch(
            f"a deletion that never became durable lost {missing!r}: {artifacts!r}"
        )
    # the same settled state the completed deletion reports, so the clean
    # restart is held to what this recovery left behind
    return {
        "events": _timeline_events(store),
        "artifacts": artifacts,
        "forkmeta_owners": sorted(_forkmeta_timelines(store)),
    }


def _check_delete_recovery(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    store: Path,
    stage: str,
    timeout: float,
) -> dict[str, Any]:
    """The verify client already waited for DELETED; the owner's artifacts
    must be gone, the manifest reconciled, the root's history still capped by
    the live sibling's fork point, and no owner registered.  Returns the
    settled deletion state so the clean restart can be held to it."""
    poll_timeout = max(0.0, min(10.0, timeout))
    deadline = time.monotonic() + poll_timeout
    while True:
        artifacts = _branch_private_artifacts(store, DELETE_BRANCH)
        manifest = inspect_store(inspector, shm, "manifest", inspection_schema)
        if manifest.get("manifest_poisoned") is not False:
            raise OracleMismatch(
                f"after_{stage} recovery reported manifest_poisoned="
                f"{manifest.get('manifest_poisoned')!r}"
            )
        owning_segments = [
            path.name for path in sorted(store.glob("seg_*"))
            if DELETE_BRANCH in _segment_record_timelines(path)
        ]
        settled = not artifacts and manifest.get("deleting_layers") == 0 and \
            manifest.get("layer_count") == manifest.get("local_layers") and \
            DELETE_BRANCH not in _forkmeta_timelines(store) and not owning_segments
        if settled:
            break
        now = time.monotonic()
        if now >= deadline:
            raise HarnessTimeout(
                f"after_{stage} recovery left artifacts {artifacts!r} manifest "
                f"{manifest!r} forkmeta owners {sorted(_forkmeta_timelines(store))!r} "
                f"segments still holding the owner's records {owning_segments!r} "
                f"after {poll_timeout:.3f}s"
            )
        time.sleep(min(0.05, deadline - now))
    timeline = inspect_store(inspector, shm, "timeline", inspection_schema, timeline=0)
    if timeline.get("parent_timeline") != -1:
        raise OracleMismatch(f"after_{stage} recovery changed timeline 0: {timeline!r}")
    # The live sibling forks from the root at the same LSN, so deleting its
    # sibling must not lift the root's structural cap: cleanup that dropped it
    # would let the ancestor history the sibling still reads be pruned, and
    # the client only ever reads the parent's latest page.
    if timeline.get("retained_horizon") != DELETE_FORK_LSN:
        raise OracleMismatch(
            f"after_{stage} recovery reported retained_horizon="
            f"{timeline.get('retained_horizon')!r}, expected the live sibling's "
            f"fork point {DELETE_FORK_LSN}"
        )
    owners = inspect_store(inspector, shm, "owners", inspection_schema)
    if owners.get("retention_poisoned") is not False or owners.get("owner_count") != 0:
        raise OracleMismatch(
            f"after_{stage} recovery reported owners={owners!r}, expected none"
        )
    # The settled deletion state: a restart that re-appends a lifecycle event
    # or rewrites the deletion's artifacts reaches the same predicates above
    # while changing what is durable, which is what idempotence is about.
    return {
        "events": _timeline_events(store),
        "artifacts": _branch_private_artifacts(store, DELETE_BRANCH),
        "forkmeta_owners": sorted(_forkmeta_timelines(store)),
    }


def _check_wal_reclaim_crash_snapshot(store: Path, stage: str, hit: int) -> None:
    """The physical frontier is durable at every stage; the number of sealed
    segments still on disk tells the stage apart: none unlinked before the
    first unlink, one per hit after an unlink, none before the directory
    fsync that retires the residual prefix."""
    expected = {
        "reclaim_before_unlink": WAL_RECLAIM_SEGMENTS,
        "reclaim_after_unlink": WAL_RECLAIM_SEGMENTS - hit,
        "reclaim_before_dir_fsync": 0,
    }[stage]
    metadata = _wal_store_metadata(store)
    if metadata is None:
        raise OracleMismatch(
            f"after_{stage} crash left no valid shipped-WAL store metadata"
        )
    # The physical frontier is what these boundaries are about, and before the
    # first unlink it is the only thing that distinguishes the crash image
    # from the state before publication: the files are all still there either
    # way, and restart maintenance would publish the frontier and reclaim them
    # before the recovery checks ran.
    if metadata["directory_start_lsn"] != WAL_RECLAIM_TOTAL or \
            metadata["retained_base_lsn"] != WAL_RECLAIM_TOTAL:
        raise OracleMismatch(
            f"after_{stage} crash published {metadata!r}, expected the "
            f"directory to start and retain at {WAL_RECLAIM_TOTAL}"
        )
    files = _wal_segment_files(store)
    if len(files) != expected:
        raise OracleMismatch(
            f"after_{stage} crash left {len(files)} sealed WAL segments "
            f"{files!r}, expected {expected}"
        )


def _check_wal_reclaim_recovery(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    store: Path,
    stage: str,
    timeout: float,
) -> dict[str, Any] | None:
    """The verify client already waited for the prefix reads to be refused;
    the segment files must be gone, the identity kept, and the reclaimer
    must have released its physical debt."""
    poll_timeout = max(0.0, min(10.0, timeout))
    deadline = time.monotonic() + poll_timeout
    while True:
        files = _wal_segment_files(store)
        if not files:
            break
        now = time.monotonic()
        if now >= deadline:
            raise HarnessTimeout(
                f"after_{stage} recovery left sealed WAL segments {files!r} "
                f"after {poll_timeout:.3f}s"
            )
        time.sleep(min(0.05, deadline - now))
    if not (store / WAL_RECLAIM_IDENTITY).exists():
        raise OracleMismatch(f"after_{stage} recovery lost the shipped-WAL identity")
    backpressure = inspect_store(inspector, shm, "backpressure", inspection_schema)
    if backpressure.get("wal_throttled") not in (0, False) or \
            backpressure.get("wal_lag_bytes") != 0:
        raise OracleMismatch(
            f"after_{stage} recovery kept WAL reclaim debt: "
            f"throttled={backpressure.get('wal_throttled')!r} "
            f"lag={backpressure.get('wal_lag_bytes')!r}"
        )
    owners = inspect_store(inspector, shm, "owners", inspection_schema)
    if owners.get("retention_poisoned") is not False or owners.get("owner_count") != 0:
        raise OracleMismatch(
            f"after_{stage} recovery reported owners={owners!r}, expected none"
        )
    metadata = _wal_store_metadata(store)
    if metadata is None:
        raise OracleMismatch(
            f"after_{stage} recovery left no valid shipped-WAL store metadata"
        )
    # the settled shipped-WAL state, so the clean restart has to adopt what
    # recovery reclaimed instead of moving the directory or its files again
    return {"metadata": metadata, "files": _wal_segment_files(store)}




def _manifest_records_at(manifest: Path) -> list[tuple[int, bytes]]:
    """(type, payload) of every complete record of one manifest log, in
    order."""
    try:
        # compaction replaces the log by rename, so it can be absent for an
        # instant while a polling oracle reads it
        data = manifest.read_bytes()
    except OSError:
        return []
    records: list[tuple[int, bytes]] = []
    offset = 0
    while offset + 20 <= len(data):
        # the daemon persists its C structs directly: decode in host byte order
        magic, version, kind, length, _crc = struct.unpack_from("=IIIII", data, offset)
        if magic != MANIFEST_MAGIC or version != MANIFEST_VERSION:
            break
        if offset + 20 + length > len(data):
            break
        records.append((kind, data[offset + 20:offset + 20 + length]))
        offset += 20 + length
    return records


def _manifest_records(store: Path) -> list[tuple[int, bytes]]:
    return _manifest_records_at(store / "layers.manifest")


def _manifest_record_layer_id(payload: bytes) -> int | None:
    """The layer id a record names: the leading uint64 of every payload that
    carries one.  Matching the raw bytes anywhere in the payload would also
    match an unrelated block range or size that happens to look like it."""
    return struct.unpack_from("=Q", payload, 0)[0] if len(payload) >= 8 else None


def _manifest_names_layer(records: list[tuple[int, bytes]], kind: int, layer_id: int) -> bool:
    return any(k == kind and _manifest_record_layer_id(payload) == layer_id
               for k, payload in records)


def _manifest_marks_after_add(records: list[tuple[int, bytes]], layer_id: int) -> int:
    """Tombstones the log records after the given layer's ADD, whether or not
    their layer files are still on disk."""
    after = False
    marks = 0
    for kind, payload in records:
        if kind == MANIFEST_ADD_LAYER and _manifest_record_layer_id(payload) == layer_id:
            after = True
        elif after and kind == MANIFEST_MARK_DELETE:
            marks += 1
    return marks


def _gc_granted_cutoff_seq(control: Path) -> int | None:
    """The admission sequence the seed's reservation was granted, as the seed
    recorded it; None when the seed has not reported one."""
    try:
        return int((control / "cutoff-seq").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _check_gc_crash_snapshot(
    store: Path, stage: str, control: Path, hit: int = 1
) -> dict[str, Any] | None:
    """Check the stage-specific durable state before recovery mutates the store.

    The replacement layer file is sealed before the frontier advances, its
    manifest ADD lands after the frontier probe, and the sources' tombstones
    after the publication probe.  The newest layer id on disk is the
    replacement; whether the manifest names it, and whether any tombstone
    exists yet, tells the three boundaries apart.  The WAL-index stage follows
    the durable frontier but precedes the generation commit, so the prepared
    generation is still staged.
    """
    if stage.startswith("reclaim_"):
        _check_wal_reclaim_crash_snapshot(store, stage, hit)
        return
    if stage == "delete_before_deleting":
        _check_delete_abort_crash_snapshot(store)
        return
    if stage.startswith("delete_"):
        _check_delete_crash_snapshot(store, stage)
        return
    if stage.startswith("manifest_"):
        _check_manifest_crash_snapshot(store, stage)
        return
    if stage.startswith("forkmeta_"):
        # the cutoff a forkmeta generation carries is only meaningful if the
        # page frontier that proves it is durable in the same crash image
        fences = _page_frontier_fences(store, 0)
        if not any(incarnation == 1 and lsn == FORKMETA_CUTOFF_LSN
                   for incarnation, lsn, _seq in fences):
            raise OracleMismatch(
                f"after_{stage} crash published timeline 0 fences {fences!r}, "
                f"expected the proven frontier {FORKMETA_CUTOFF_LSN} in incarnation 1"
            )
        selected = _check_forkmeta_crash_snapshot(store, stage)
        # the generation the crash had already selected, so recovery can be
        # held to it instead of to whatever it settles on
        return {"forkmeta_selected": selected} if selected is not None else None
    if stage == "walidx_frontier":
        if not (store / "walidx-prune.frontiers").exists():
            raise OracleMismatch(
                "after_walidx_frontier did not leave a durable WAL-index frontier"
            )
        if not (store / WALIDX_PREPARED).exists():
            raise OracleMismatch(
                "after_walidx_frontier did not leave the prepared WAL-index "
                "generation staged for its restart retry"
            )
        if (store / WALIDX_MANIFEST).exists():
            raise OracleMismatch(
                "after_walidx_frontier committed the WAL-index generation "
                "before the probe"
            )
        prepared = _walidx_snapshot_record(store / WALIDX_PREPARED)
        if prepared is None:
            raise OracleMismatch(
                "after_walidx_frontier staged a WAL-index generation the "
                "harness cannot read"
            )
        # the frontier already covers this generation, so recovery has to
        # retry it rather than build an equivalent one under a new number
        return {"walidx_prepared": prepared}
    files = _canonical_layer_files(store)
    if len(files) < 2:
        raise OracleMismatch(
            f"after_{stage} crash left {len(files)} canonical layer files, "
            "expected the replacement next to its retired sources"
        )
    records = _manifest_records(store)
    if not records:
        raise OracleMismatch(f"after_{stage} crash left no published layers.manifest")
    ids = {int(path.name.rsplit("_", 1)[1], 16) for path in files}
    replacement = max(ids)
    published = _manifest_names_layer(records, MANIFEST_ADD_LAYER, replacement)
    # earlier flush-driven passes may have retired layers of their own; only
    # tombstones on the sources still on disk belong to the crashed pass
    tombstones = sum(
        1 for layer_id in ids
        if layer_id != replacement and
        _manifest_names_layer(records, MANIFEST_MARK_DELETE, layer_id)
    )
    # The frontier is the prerequisite of both later boundaries: publishing a
    # replacement or a tombstone before it is durable is an ordering error
    # that recovery would otherwise repair before any later check runs.
    fences = _page_frontier_fences(store, 0)
    if not fences:
        raise OracleMismatch(
            f"after_{stage} did not leave a valid durable page-prune frontier"
        )
    # this store defines timeline 0 with the first incarnation, so the cutoff
    # must be published for that one, not merely for some slot, and at the
    # admission sequence the reservation was granted
    expected = GC_WORKLOADS["page_prune"]["retained_horizon"]
    granted = _gc_granted_cutoff_seq(control)
    if not any(incarnation == 1 and lsn == expected and
               (granted is None or seq == granted)
               for incarnation, lsn, seq in fences):
        raise OracleMismatch(
            f"after_{stage} published timeline 0 fences {fences!r}, expected the "
            f"configured cutoff {expected} in incarnation 1 at "
            f"admission sequence {granted}"
        )
    if stage == "frontier":
        if published:
            raise OracleMismatch(
                "after_frontier crash already published the replacement layer"
            )
        if tombstones:
            raise OracleMismatch(
                f"after_frontier crash already wrote {tombstones} source tombstone(s)"
            )
    elif stage == "publish":
        if not published:
            raise OracleMismatch(
                f"after_publish crash did not publish replacement layer {replacement:#x}"
            )
        # No tombstone of this pass's sources at all, wherever the record
        # sits: retirement ordered ahead of publication is the defect, and
        # counting only records after the ADD would miss it.
        if tombstones:
            raise OracleMismatch(
                f"after_publish crash already wrote {tombstones} source tombstone(s)"
            )
    elif stage == "mark_delete":
        if not published or tombstones == 0:
            raise OracleMismatch(
                "after_mark_delete crash left no source tombstone behind the "
                f"published replacement (published={published}, tombstones={tombstones})"
            )
        if not _manifest_marks_after_add(records, replacement):
            raise OracleMismatch(
                "after_mark_delete crash retired a source before publishing the "
                f"replacement layer {replacement:#x}"
            )
    return None


def _walidx_snapshot_record(path: Path) -> dict[str, Any] | None:
    """The identity of one WAL-index snapshot generation, as the staged and
    the committed file both carry it: the generation, the interval it covers,
    and the shard payloads it names."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if len(data) < WALIDX_SNAPSHOT_HEADER_BYTES:
        return None
    # the daemon writes this header little-endian, not in host order
    magic, version, header_bytes, entry_bytes, timeline, nshards = \
        struct.unpack_from("<IIIIII", data, 0)
    generation, start_lsn, end_lsn = struct.unpack_from("<QQQ", data, 24)
    if magic != WALIDX_SNAPSHOT_MAGIC or version != WALIDX_SNAPSHOT_VERSION or \
            header_bytes != WALIDX_SNAPSHOT_HEADER_BYTES or \
            entry_bytes != WALIDX_SNAPSHOT_ENTRY_BYTES:
        return None
    if len(data) != header_bytes + nshards * entry_bytes:
        return None
    shards = []
    for shard in range(nshards):
        index, crc, length = struct.unpack_from(
            "<IIQ", data, header_bytes + shard * entry_bytes
        )
        if index != shard:
            return None
        shards.append((crc, length))
    return {
        "timeline": timeline, "generation": generation,
        "start_lsn": start_lsn, "end_lsn": end_lsn, "shards": shards,
    }


def _walidx_generation_files(store: Path) -> list[str]:
    """The committed WAL-index snapshot manifest and the shard payloads
    beside it, as the names that identify one generation."""
    directory = store / "walidx_snapshots_0"
    if not directory.is_dir():
        return []
    return sorted(entry.name for entry in directory.iterdir() if entry.is_file())


def _check_walidx_recovery(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    store: Path,
    stage: str,
    crash_state: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """The verify client already waited for the compacted chains, so the
    retried generation is committed: the staged copy is gone, the committed
    manifest exists, and the fixed WAL-index reader survived recovery."""
    if (store / WALIDX_PREPARED).exists():
        raise OracleMismatch(
            f"after_{stage} recovery served the compacted chains but left the "
            "prepared WAL-index generation staged"
        )
    if not (store / WALIDX_MANIFEST).exists():
        raise OracleMismatch(
            f"after_{stage} recovery did not commit the WAL-index generation"
        )
    if not (store / "walidx-prune.frontiers").exists():
        raise OracleMismatch(f"after_{stage} recovery lost the WAL-index frontier")
    committed = _walidx_snapshot_record(store / WALIDX_MANIFEST)
    if committed is None:
        raise OracleMismatch(
            f"after_{stage} recovery committed a WAL-index generation the "
            "harness cannot read"
        )
    # The staged generation was already covered by the durable frontier, so
    # recovery has to retry that one.  Rebuilding an equivalent snapshot under
    # a new generation would satisfy every later check -- the restart
    # comparison starts from whatever this recovery settled on -- while
    # abandoning the generation the frontier was published for.
    prepared = (crash_state or {}).get("walidx_prepared")
    if prepared is not None and committed != prepared:
        raise OracleMismatch(
            f"after_{stage} recovery committed {committed!r} instead of the "
            f"staged generation {prepared!r}"
        )
    owners = inspect_store(inspector, shm, "owners", inspection_schema)
    if owners.get("retention_poisoned") is not False or \
            owners.get("wal_index_owners") != 1 or \
            owners.get("page_history_owners") != 0:
        raise OracleMismatch(
            f"after_{stage} recovery reported owners={owners!r}, expected one "
            "WAL-index owner and no page-history owner"
        )
    # the committed generation, so a restart that republishes it can be told
    # from one that adopts what recovery already committed
    return {
        "manifest": (store / WALIDX_MANIFEST).read_bytes(),
        "files": _walidx_generation_files(store),
    }


def _check_gc_recovery(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    store: Path,
    workload: str,
    stage: str,
    timeout: float,
    crash_state: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """After recovery the manifest is sane, the retired sources are gone once
    cleanup has resumed (only the replacement remains, in the manifest and on
    disk), and the retained horizon is the configured cutoff."""
    if workload == "wal_index":
        return _check_walidx_recovery(inspector, shm, inspection_schema, store,
                                      stage, crash_state)
    if workload == "wal_reclaim":
        return _check_wal_reclaim_recovery(
            inspector, shm, inspection_schema, store, stage, timeout
        )
    if workload == "timeline_delete":
        return _check_delete_recovery(inspector, shm, inspection_schema, store,
                                      stage, timeout)
    if workload == "timeline_delete_abort":
        return _check_delete_abort_recovery(inspector, shm, inspection_schema,
                                            store)
    if workload == "manifest_compact":
        return _check_manifest_recovery(inspector, shm, inspection_schema, store,
                                        stage, timeout)
    poll_timeout = max(0.0, min(10.0, timeout))
    deadline = time.monotonic() + poll_timeout
    while True:
        manifest = inspect_store(inspector, shm, "manifest", inspection_schema)
        if manifest.get("manifest_poisoned") is not False:
            raise OracleMismatch(
                f"after_{stage} recovery reported manifest_poisoned="
                f"{manifest.get('manifest_poisoned')!r}"
            )
        layer_count = manifest.get("layer_count")
        deleting = manifest.get("deleting_layers")
        files = _canonical_layer_files(store)
        if isinstance(layer_count, int) and isinstance(deleting, int) and \
                deleting == 0 and layer_count == manifest.get("local_layers") == 1 and \
                len(files) == 1:
            break
        now = time.monotonic()
        if now >= deadline:
            raise HarnessTimeout(
                f"after_{stage} recovery did not retire the source layers within "
                f"{poll_timeout:.3f}s; last manifest={manifest!r}, files={[f.name for f in files]!r}"
            )
        time.sleep(min(0.05, deadline - now))
    expected_horizon = GC_WORKLOADS[workload]["retained_horizon"]
    timeline = inspect_store(inspector, shm, "timeline", inspection_schema, timeline=0)
    if timeline.get("retained_horizon") != expected_horizon:
        raise OracleMismatch(
            f"after_{stage} recovery reported retained_horizon="
            f"{timeline.get('retained_horizon')!r}, expected {expected_horizon}"
        )
    if workload == "forkmeta":
        return _check_forkmeta_recovery(store, stage, timeout, crash_state)
    return None


def _gc_converged_state(store: Path) -> dict[str, Any]:
    """The durable state a converged store must not change on a further boot.

    Every per-boot oracle is satisfied by a recovery that recompacts the sole
    surviving layer into a fresh replacement each time it starts, so the
    idempotence contract needs the state that such churn moves: which layer
    files exist, which layers the manifest still publishes, and the durable
    page-prune frontiers.
    """
    records = _manifest_records(store)
    live: set[int] = set()
    for kind, payload in records:
        layer_id = _manifest_record_layer_id(payload)
        if layer_id is None:
            continue
        if kind == MANIFEST_ADD_LAYER:
            live.add(layer_id)
        elif kind == MANIFEST_MARK_DELETE:
            live.discard(layer_id)
    return {
        "layer_files": sorted(path.name for path in _canonical_layer_files(store)),
        "published_layers": sorted(live),
        "frontiers": sorted(_page_frontier_fences(store, 0)),
    }


def _check_gc_restart_idempotent(
    store: Path, converged: dict[str, Any], stage: str
) -> None:
    """The extra clean restart must converge on the state the first recovery
    already reached, not merely on a state that passes the same checks."""
    current = _gc_converged_state(store)
    if current != converged:
        differing = sorted(
            key for key in converged if current.get(key) != converged.get(key)
        )
        raise OracleMismatch(
            f"after_{stage} the extra restart changed durable state "
            f"{differing!r}: {converged!r} became {current!r}"
        )


def _layer_fault_stage(fault_name: str) -> str | None:
    stages = {
        "image_layer.after_create": "create",
        "image_layer.after_write": "write",
        "image_layer.after_seal": "seal",
        "image_layer.after_manifest_add": "manifest_add",
    }
    return stages.get(fault_name)


def _canonical_layer_files(store: Path) -> list[Path]:
    files: list[Path] = []
    pattern = re.compile(r"^layer_(?:0|[1-9][0-9]*)_[0-9a-fA-F]{16}$")
    for path in sorted(store.iterdir()):
        if not pattern.fullmatch(path.name):
            continue
        try:
            value = path.lstat()
        except FileNotFoundError:
            # a recovering daemon retires source layers while the oracles
            # poll: a name listed a moment ago may already be unlinked, which
            # is the state this listing is watching for, not an error
            continue
        if stat.S_ISREG(value.st_mode):
            files.append(path)
    return files


def _check_layer_crash_snapshot(store: Path, stage: str) -> None:
    """Check process-abort physical state before recovery mutates the store.

    The after_write point deliberately proves only the write-stage ordering;
    it is not a power-loss or file-fsync durability claim.
    """
    files = _canonical_layer_files(store)
    if len(files) != 1:
        raise OracleMismatch(
            f"H1 {stage} crash left {len(files)} canonical layer files, expected one"
        )
    manifest = store / "layers.manifest"
    manifest_size = manifest.stat().st_size if manifest.exists() else 0
    size = files[0].stat().st_size
    if stage == "create" and size != 0:
        raise OracleMismatch("after_create did not leave an empty canonical layer")
    if stage in {"write", "seal"} and size == 0:
        raise OracleMismatch(f"after_{stage} did not leave written layer bytes")
    if stage != "manifest_add" and manifest_size != 0:
        raise OracleMismatch(f"after_{stage} unexpectedly published layers.manifest")
    if stage == "manifest_add" and manifest_size == 0:
        raise OracleMismatch("after_manifest_add did not leave a durable manifest ADD")


def _check_layer_manifest(
    manifest: dict[str, Any], stage: str, recovery_restart: bool,
) -> None:
    # Before manifest publication, segment replay rebuilds and republishes the
    # interrupted flush.  After ADD publication but before the flush watermark,
    # recovery conservatively retains that layer and republishes segment-backed
    # coverage once.  The intervening clean shutdown compacts that conservative
    # duplicate, so the following restart must converge to one layer.
    expected_states = (
        ((1, 1), (2, 2))
        if stage == "manifest_add" and not recovery_restart
        else ((1, 1),)
    )
    expected_text = "(1, 1) or (2, 2)" if len(expected_states) > 1 else "(1, 1)"
    observed_state = (manifest.get("layer_count"), manifest.get("local_layers"))
    if observed_state not in expected_states:
        raise OracleMismatch(
            f"after_{stage} recovery reported layer_count/local_layers="
            f"{observed_state!r}, expected {expected_text}"
        )
    if manifest.get("manifest_poisoned") is not False:
        raise OracleMismatch(
            f"after_{stage} recovery reported manifest_poisoned="
            f"{manifest.get('manifest_poisoned')!r}"
        )


def _check_layer_manifest_after_restart(
    inspector: Path,
    shm: str,
    inspection_schema: dict[str, Any],
    stage: str,
    timeout: float,
) -> None:
    """Wait for the second restart to compact a manifest_add duplicate."""
    poll_timeout = max(0.0, min(10.0, timeout))
    deadline = time.monotonic() + poll_timeout
    while True:
        manifest = inspect_store(inspector, shm, "manifest", inspection_schema)
        observed_state = (manifest.get("layer_count"), manifest.get("local_layers"))
        if manifest.get("manifest_poisoned") is not False:
            raise OracleMismatch(
                f"after_{stage} recovery reported manifest_poisoned="
                f"{manifest.get('manifest_poisoned')!r}"
            )
        if observed_state == (1, 1):
            _check_layer_manifest(manifest, stage, True)
            return

        # Only the coherent duplicate produced by manifest_add recovery is
        # allowed to remain transient. Poisoned or otherwise inconsistent
        # observations must not be hidden by a later successful poll.
        if (
            stage != "manifest_add"
            or observed_state != (2, 2)
        ):
            _check_layer_manifest(manifest, stage, True)

        now = time.monotonic()
        if now >= deadline:
            raise HarnessTimeout(
                f"after_{stage} recovery manifest did not converge to (1, 1) "
                f"within {poll_timeout:.3f}s; last state={observed_state!r}"
            )
        time.sleep(min(0.05, deadline - now))


def _start_layer_client(
    client: Path, shm: str, mode: str, log: Path, arm_marker: Path | None = None,
    workload: str | None = None, resume_file: Path | None = None,
    cutoff_seq_file: Path | None = None,
) -> subprocess.Popen[str]:
    command = [str(client.resolve()), "--shm", shm, "--mode", mode]
    if workload is not None:
        command.extend(["--workload", workload])
    if arm_marker is not None:
        command.extend(["--arm-marker", str(arm_marker)])
    if resume_file is not None:
        command.extend(["--resume-file", str(resume_file)])
    if cutoff_seq_file is not None:
        command.extend(["--cutoff-seq-file", str(cutoff_seq_file)])
    with log.open("a", encoding="utf-8") as output:
        return subprocess.Popen(
            command,
            stdout=output, stderr=subprocess.STDOUT, text=True,
            env=private_environment(), start_new_session=True,
        )


def _verify_layer_client(
    client: Path, shm: str, log: Path, timeout: float, workload: str | None = None,
) -> None:
    command = [str(client.resolve()), "--shm", shm, "--mode", "verify"]
    if workload is not None:
        command.extend(["--workload", workload])
    with log.open("a", encoding="utf-8") as output:
        result = subprocess.run(
            command,
            stdout=output, stderr=subprocess.STDOUT, text=True,
            env=private_environment(), timeout=max(5.0, timeout), check=False,
        )
    if result.returncode != 0:
        raise OracleMismatch(
            f"layer sentinel client failed after recovery with status {result.returncode}"
        )


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
    layer_client: Path | None = None,
    gc_client: Path | None = None,
) -> Path:
    """Run one pre-armed named daemon fault and prove recovery is idempotent."""
    daemon = daemon.resolve()
    inspector = inspector.resolve()
    layer_client = layer_client.resolve() if layer_client is not None else None
    gc_client = gc_client.resolve() if gc_client is not None else None
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
    pause_file = control / "maintenance-pause"
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
    layer_seed_actions = [item for item in plan.actions if item["op"] == "layer_seed"]
    if layer_seed_actions and layer_client is None:
        raise PlanError("layer_seed requires --layer-client-binary")
    layer_stage = _layer_fault_stage(action["fault"]) if layer_seed_actions else None
    gc_seed_actions = [item for item in plan.actions if item["op"] == "gc_seed"]
    if gc_seed_actions and gc_client is None:
        raise PlanError("gc_seed requires --gc-client-binary")
    gc_stage = _gc_fault_stage(action["fault"]) if gc_seed_actions else None
    crash_state: dict[str, Any] | None = None
    gc_workload = gc_seed_actions[0]["workload"] if gc_seed_actions else None
    # Both seeds drive the same one-client workload protocol; the layer and
    # page-pruning slices differ only in the binary, daemon flags, and oracles.
    seed_client = gc_client if gc_seed_actions else layer_client
    seed_actions = gc_seed_actions or layer_seed_actions
    validate_fault_action(
        action, capabilities, f"{plan.path}:{action['id']}", capabilities_path,
        require_model=action["op"] == "crash",
    )
    fault_name = action["fault"]
    catalog_entry = fault_catalog(capabilities, capabilities_path)[fault_name]
    fault_action = action["action"]
    fault_hit = action["hit"]
    fault_timeout = float(action.get("timeout", timeout))
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
    layer_client_process: subprocess.Popen[str] | None = None
    shm_names: list[str] = []
    shm_base = f"/psharness_{os.getpid()}_{time.monotonic_ns()}"
    shm = ""
    daemon_log = trace / "daemon.log"
    marker = control / "arm"
    report = control / "report.jsonl"
    release = control / "release"
    pause_file = control / "maintenance-pause"
    cutoff_seq_file = control / "cutoff-seq"
    # The seed installs the cutoff that makes pruning due and then arms the
    # fault; maintenance stays paused across both, so no pass can run against
    # the old floor and none can outrun arming either.
    # Every gc workload starts its crash generation paused: the seed installs
    # the cutoff that makes its work due and then arms the fault, and a pass
    # in between would either be planned against the old floor or consume the
    # only due work before the fault is armed.  A workload whose daemon_args
    # name {pause} places the file itself; the rest get the flag added.
    gc_pauses_maintenance = bool(gc_seed_actions)
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
        if layer_seed_actions:
            command.extend(["--segment-size", "65536", "--flush-pages", "2",
                            "--compact-layers", "1000"])
        if gc_seed_actions:
            command.extend(
                arg.replace("{pause}", str(pause_file))
                for arg in GC_WORKLOADS[gc_workload]["daemon_args"]
            )
            # workloads whose own daemon_args do not name the pause file still
            # start paused while the seed installs its cutoff and arms
            if inject_fault and gc_pauses_maintenance and \
                    "--test-maintenance-pause-file" not in \
                    GC_WORKLOADS[gc_workload]["daemon_args"]:
                command.extend(["--test-maintenance-pause-file", str(pause_file)])
        env = private_environment()
        if gc_seed_actions:
            env.update(GC_WORKLOADS[gc_workload].get("daemon_env", {}))
        if inject_fault:
            # Keep these names local and explicit: inherited PAGESTORE_* values
            # are removed by private_environment before this point.
            env.update({
                "PAGESTORE_TEST_FAULT_NAME": fault_name,
                "PAGESTORE_TEST_FAULT_ACTION": fault_action,
                "PAGESTORE_TEST_FAULT_HIT": str(fault_hit),
                "PAGESTORE_TEST_FAULT_DIR": str(control),
                "PAGESTORE_TEST_FAULT_SCENARIO": scenario,
                "PAGESTORE_TEST_FAULT_SEED": str(seed),
                "PAGESTORE_TEST_FAULT_OPERATION": str(action["id"]),
            })
            if fault_action == "pause":
                env["PAGESTORE_TEST_FAULT_WATCHDOG_MS"] = str(
                    math.ceil(fault_timeout * 1000.0)
                )
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
        # The page-pruning workload arms the marker itself, right before it
        # installs the cutoff: the flush-driven compactions that run while
        # its history is written would otherwise trip the probe with nothing
        # to retire.  Every other fault is armed before the daemon starts.
        if not gc_seed_actions:
            _atomic_arm_marker(marker)
        # A workload that needs its writes complete before maintenance runs
        # starts the crash generation paused; the workload removes the file
        # once it has armed the fault.  Restarts never pause.
        if gc_pauses_maintenance:
            pause_file.touch()
        daemon_log.touch()
        emit("run_start", shm_base=shm_base)
        current_action_id = action["id"]
        if gc_seed_actions:
            # the workload creates the marker itself; fault_arm is recorded
            # when that marker is observed, so the event log places the
            # arming after the history writes it must follow
            emit("fault_configured", target="store", name=fault_name, hit=fault_hit,
                 armed_by="workload")
        else:
            emit("fault_arm", target="store", name=fault_name, hit=fault_hit,
                 armed_by="harness")
        workload_armed = not gc_seed_actions
        # the crash generation starts paused; the workload releases it once
        # the cutoff is durable and the fault armed.  Restarts never pause.
        if gc_pauses_maintenance:
            pause_file.touch()
        process = start_daemon(True, action["id"])
        if seed_actions:
            wait_ready(process)
            layer_client_process = _start_layer_client(
                seed_client, shm, "seed", trace / "layer-client.log",
                arm_marker=marker if gc_seed_actions else None,
                workload=gc_workload,
                resume_file=pause_file if gc_pauses_maintenance else None,
                cutoff_seq_file=cutoff_seq_file if gc_seed_actions else None,
            )
        deadline = time.monotonic() + fault_timeout
        if fault_action == "crash":
            while process.poll() is None and time.monotonic() < deadline:
                if not workload_armed and marker.exists():
                    workload_armed = True
                    emit("fault_arm", target="store", name=fault_name, hit=fault_hit,
                         armed_by="workload")
                if layer_client_process is not None and layer_client_process.poll() is not None:
                    if layer_client_process.returncode == 0:
                        raise FaultNotReached(
                            f"layer workload completed before fault {fault_name!r}"
                        )
                    raise UnexpectedExit(
                        f"layer workload exited with status {layer_client_process.returncode}"
                    )
                time.sleep(0.02)
            if not workload_armed and marker.exists():
                # the daemon may have crashed within the same poll interval
                # in which the workload armed it
                workload_armed = True
                emit("fault_arm", target="store", name=fault_name, hit=fault_hit,
                     armed_by="workload")
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
            if layer_stage is not None:
                _check_layer_crash_snapshot(store, layer_stage)
            if gc_stage is not None:
                # what the crash left behind, for the recovery oracles that
                # must prove recovery settled on it rather than on an
                # equivalent state of its own making
                crash_state = _check_gc_crash_snapshot(store, gc_stage, control,
                                                       fault_hit)
            if layer_client_process is not None and layer_client_process.poll() is None:
                layer_client_process.kill()
                layer_client_process.wait(timeout=5)
            layer_client_process = None
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
            try:
                result = _fault_report(
                    report, fault_name, fault_hit, process.pid, fault_action,
                    scenario, seed, action["id"],
                )
            except FaultNotReached as reached_error:
                if fault_action != "pause":
                    raise
                try:
                    _fault_report(
                        report, fault_name, fault_hit, process.pid, fault_action,
                        scenario, seed, action["id"], "timeout",
                    )
                except FaultNotReached:
                    raise reached_error
                raise HarnessTimeout(
                    f"fault {fault_name!r} pause watchdog expired before release"
                ) from reached_error
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
                if process.returncode == 90:
                    _fault_report(
                        report, fault_name, fault_hit, process.pid, fault_action,
                        scenario, seed, action["id"], "timeout",
                    )
                    raise HarnessTimeout(
                        f"fault {fault_name!r} pause watchdog expired after release"
                    )
                if process.returncode != 0:
                    raise UnexpectedExit(
                        f"fault {fault_name!r} did not stop cleanly after release; "
                        f"status {process.returncode}"
                    )
                emit("process_stop", target="store", pid=process.pid,
                     returncode=process.returncode)
                process = None
            remove_shm(shm)
            shutil.rmtree(control)
        # Recovery is intentionally followed by one additional clean restart.
        process = start_daemon(False, action["id"])
        health = wait_ready(process)
        if layer_seed_actions:
            _verify_layer_client(layer_client, shm, trace / "layer-client.log", timeout)
            manifest = inspect_store(inspector, shm, "manifest", inspection_schema)
            _check_layer_manifest(manifest, layer_stage, False)
        recovered_state = None
        if gc_seed_actions:
            _verify_layer_client(gc_client, shm, trace / "layer-client.log", timeout,
                                 workload=gc_workload)
            recovered_state = _check_gc_recovery(inspector, shm, inspection_schema,
                                                 store, gc_workload, gc_stage, timeout,
                                                 crash_state)
        probe_runtime_inspection(inspector, shm, capabilities, inspection_schema)
        emit("recovered", target="store", health=health)
        stop_daemon()
        if process.returncode != 0:
            raise UnexpectedExit(
                "recovered daemon did not stop cleanly before restart; status "
                f"{process.returncode}"
            )
        emit("process_stop", target="store", pid=process.pid,
             returncode=process.returncode)
        remove_shm(shm)
        process = None
        # the first recovery has converged and its daemon is down, so this is
        # the durable state the additional restart has to leave alone
        converged = _gc_converged_state(store) if gc_seed_actions else None
        process = start_daemon(False, action["id"])
        health = wait_ready(process)
        if layer_seed_actions:
            _verify_layer_client(layer_client, shm, trace / "layer-client.log", timeout)
            _check_layer_manifest_after_restart(
                inspector, shm, inspection_schema, layer_stage, timeout
            )
        if gc_seed_actions:
            _verify_layer_client(gc_client, shm, trace / "layer-client.log", timeout,
                                 workload=gc_workload)
            restarted_state = _check_gc_recovery(inspector, shm, inspection_schema,
                                                 store, gc_workload, gc_stage, timeout)
            # nothing mutates the store between the two starts, so a restart
            # that republishes a generation is not idempotent
            if restarted_state != recovered_state:
                # nothing changed between the two starts, so a restart that
                # publishes a new generation is not idempotent
                raise OracleMismatch(
                    f"clean restart changed the settled state from {recovered_state!r} "
                    f"to {restarted_state!r}"
                )
        probe_runtime_inspection(inspector, shm, capabilities, inspection_schema)
        emit("restarted", target="store", health=health)
        if gc_seed_actions:
            # Readiness is published as soon as the maintenance thread is
            # created, so the pass that startup marks due may not have run
            # yet: comparing while the daemon is up can see the state before
            # a recompaction it is about to do.  Stop it first -- the same
            # quiescent point the converged state was taken at.
            stop_daemon()
            if process.returncode != 0:
                raise UnexpectedExit(
                    "restarted daemon did not stop cleanly; status "
                    f"{process.returncode}"
                )
            emit("process_stop", target="store", pid=process.pid,
                 returncode=process.returncode)
            remove_shm(shm)
            process = None
            _check_gc_restart_idempotent(store, converged, gc_stage)
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
                *( ["--layer-client-binary", str(layer_client)]
                   if layer_client is not None else [] ),
                *( ["--gc-client-binary", str(gc_client)]
                   if gc_client is not None else [] ),
                "--run-root", str(root.parent / f"{root.name}.rerun"), "--keep",
            ],
        }
        (root / "failure.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    finally:
        if layer_client_process is not None and layer_client_process.poll() is None:
            layer_client_process.kill()
            layer_client_process.wait(timeout=5)
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
        reader_lsns: dict[str, str] = {}
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
                reader_lsns[action["target"]] = lsn
                events.emit("install_reader", id=action["id"], target=action["target"], lsn=lsn, data=str(reader_data), port=reader_port)
                continue
            elif action["op"] == "restart":
                # A clean compute restart: the writer or an installed pinned
                # reader stops with a fast shutdown and starts from its own
                # data directory against the unchanged store.  A pinned
                # reader's shutdown checkpoint rewrites its pg_control, so its
                # restart follows the documented protocol and restores the
                # boot control image at its immutable identity first.
                restored_control = False
                if action["target"] == "writer":
                    restart_data, restart_log = data, trace / "writer.log"
                elif action["target"] in reader_data_dirs:
                    restart_data, restart_log = reader_data_dirs[action["target"]], trace / "reader.log"
                else:
                    raise PlanError(f"restart {action['id']} targets unavailable compute {action['target']!r}")

                def compute_instance(what: Path) -> dict[str, Any]:
                    """The postmaster this data directory is running, as a
                    PID with its start time: a replacement handed the same PID
                    is a different process, and the trace has to be able to
                    show that the named compute was in fact replaced."""
                    try:
                        pid = int(
                            (what / "postmaster.pid").read_text(encoding="utf-8")
                            .splitlines()[0]
                        )
                    except (OSError, ValueError, IndexError) as error:
                        raise PlanError(
                            f"restart {action['id']} cannot read the postmaster pid "
                            f"of {what}: {error}"
                        ) from error
                    return {"pid": pid, "starttime": read_process_starttime(pid)}

                previous_instance = compute_instance(restart_data)
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(restart_data), "-m", "fast", "-w", "stop"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                if action["target"] in reader_data_dirs:
                    assert control_restore is not None
                    subprocess.run(
                        pagestore_control_restore_command(
                            control_restore, shm, 0, 1, reader_lsns[action["target"]], restart_data,
                        ),
                        check=True, capture_output=True, encoding="utf-8", env=env,
                    )
                    restored_control = True
                subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(restart_data), "-l", str(restart_log), "-w", "start"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                # `pg_ctl -w start` establishes only that the PID file says
                # the server accepts connections.  What the restart owes is
                # that the target came back as itself: the writer out of
                # recovery, a pinned reader in recovery and still at its own
                # horizon.  Ask it here rather than leaving it to an
                # assertion the plan may not have.
                if action["target"] == "writer":
                    health_socket, health_port = sockdir, port
                    health_sql = "SELECT NOT pg_is_in_recovery()"
                else:
                    health_socket, health_port = reader_clients[action["target"]]
                    # A pinned reader is an ordinary instance held at a
                    # horizon by its own GUC, not a standby, so what it owes
                    # after a restart is that horizon.  Compare as LSNs, not
                    # as text: the value is a string GUC and the reader is
                    # free to echo it back in its own spelling.
                    health_sql = (
                        "SELECT NOT pg_is_in_recovery() AND "
                        "current_setting('pagestore.read_lsn')::pg_lsn = '"
                        + reader_lsns[action["target"]].replace("'", "''")
                        + "'::pg_lsn"
                    )
                health_deadline = time.monotonic() + 40
                health = ""
                while True:
                    probe = subprocess.run(
                        [str(pg_bin / "psql"), "-h", str(health_socket),
                         "-p", str(health_port), "-U", "postgres", "-tA",
                         "-v", "ON_ERROR_STOP=1", "-c", health_sql],
                        check=False, capture_output=True, encoding="utf-8", env=env,
                    )
                    health = (probe.stdout or probe.stderr or "").strip()
                    if health == "t":
                        break
                    if time.monotonic() >= health_deadline:
                        raise OracleMismatch(
                            f"restart {action['id']} left {action['target']!r} "
                            f"unhealthy: {health_sql} returned {health!r}"
                        )
                    time.sleep(.1)
                instance = compute_instance(restart_data)
                if instance == previous_instance:
                    raise OracleMismatch(
                        f"restart {action['id']} left the {action['target']} "
                        f"instance {previous_instance!r} in place"
                    )
                events.emit("restart", id=action["id"], target=action["target"], mode="fast", data=str(restart_data), restored_control=restored_control, health=health, previous_instance=previous_instance, instance=instance)
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
    materializer_recovered = False
    materializer_fault_actions = [
        item for item in plan.actions if item["op"] == "materializer_fault"
    ]
    materializer_fault = materializer_fault_actions[0] if materializer_fault_actions else None
    fault_control = root / "control" / "materializer-fault"
    fault_report = fault_control / "report.jsonl"
    fault_trigger_proc: subprocess.Popen[str] | None = None
    checkpoints: dict[str, dict[str, Any]] = {}
    relation_observations: dict[tuple[str, str], dict[str, object]] = {}

    def sql_result(
        socket_dir: Path, port: int, sql: str, timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(pg_bin / "psql"), "-h", str(socket_dir), "-p", str(port),
                "-U", "postgres", "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql,
            ],
            check=True, capture_output=True, encoding="utf-8", env=env,
            timeout=timeout,
        )

    def sql_scalar(
        socket_dir: Path, port: int, sql: str, timeout: float | None = None,
    ) -> str:
        return sql_result(socket_dir, port, sql, timeout=timeout).stdout.strip()

    def read_materializer_status() -> dict[str, str | None]:
        return parse_materializer_status_row(sql_scalar(
            writer_socket, writer_port,
            "SELECT row(shipped_wal_lsn::text, materialized_wal_lsn::text, "
            "lag_bytes::text)::text FROM pagestore_materializer_status();",
        ))

    def read_retention_owner_lsn(generation: int) -> str | None:
        value = sql_scalar(
            writer_socket, writer_port,
            "SELECT pagestore_retention_owner_lsn(0, 2, 1, "
            + str(generation) + ")::text",
        )
        return value or None

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
        pid = postmaster_pid()
        stop = stop_process_immediately(
            pid, diagnostic_pidfile=materializer_data / "postmaster.pid"
        )
        require_signaled_process_stop(stop, "materializer")
        events.emit(
            "process_stop", target="materializer",
            generation=materializer_generation, mode="immediate", reason=reason,
            pid=stop.pid, starttime=stop.starttime,
            signal=stop.signal_method, wait=stop.wait_method,
        )

    def compute_postmaster_pid(data: Path, role: str) -> int:
        try:
            value = int(
                (data / "postmaster.pid").read_text(encoding="utf-8").splitlines()[0]
            )
        except (OSError, ValueError, IndexError) as error:
            raise PlanError(f"{role} postmaster pid is unreadable: {error}") from error
        if value <= 0:
            raise PlanError(f"{role} postmaster pid is invalid: {value}")
        return value

    def postmaster_pid() -> int:
        return compute_postmaster_pid(materializer_data, "materializer")

    def lsn_value(value: str) -> int:
        try:
            high, low = value.split("/", 1)
            result = (int(high, 16) << 32) | int(low, 16)
        except (AttributeError, ValueError) as error:
            raise PlanError(f"invalid PostgreSQL LSN {value!r}") from error
        if result < 0:
            raise PlanError(f"invalid PostgreSQL LSN {value!r}")
        return result

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
        status = read_materializer_status()
        shipped = status["shipped_wal_lsn"]
        materialized = status["materialized_wal_lsn"]
        lag = status["lag_bytes"]
        if shipped is None or materialized is None or lag is None:
            raise PlanError(
                f"materializer boundary {action['name']} returned incomplete status"
            )
        if lag != "0":
            raise PlanError(
                f"materializer boundary {action['name']} retained {lag} bytes of lag"
            )
        retention_owner_lsn = read_retention_owner_lsn(materializer_retention_generation)
        if retention_owner_lsn is None:
            raise PlanError(
                f"materializer boundary {action['name']} has no durable retention owner LSN"
            )
        horizon = {
            "checkpoint_lsn": checkpoint_lsn,
            "shipped_wal_lsn": shipped,
            "materialized_wal_lsn": materialized,
            "lag_bytes": lag,
            "wal_file": wal_file,
            "materializer_generation": materializer_generation,
            "retention_owner_generation": materializer_retention_generation,
            "retention_owner_lsn": retention_owner_lsn,
        }
        events.emit(
            "materialized_boundary", id=action["id"], name=action["name"],
            horizon=horizon,
        )
        return horizon

    def inspect_relation_action(action: dict[str, Any], phase: str) -> None:
        if not action["lsn"].startswith("$"):
            raise PlanError(
                f"inspect_relation {action['id']} requires a boundary LSN reference"
            )
        boundary = checkpoints.get(action["lsn"][1:])
        if boundary is None:
            raise PlanError(
                f"inspect_relation {action['id']} references an unknown boundary"
            )
        metadata = sql_scalar(
            writer_socket, writer_port, relation_metadata_sql(action["relation"]),
        )
        fields = metadata.split("|")
        if len(fields) != 3 or any(not field.isdigit() for field in fields):
            raise OracleMismatch(
                f"could not resolve actual relation key for {action['relation']!r}: {metadata!r}"
            )
        relation_key = tuple(int(field) for field in fields)
        relation = inspect_store(
            inspector, shm, "relation", schema,
            timeline=0, incarnation=1, relation_key=relation_key,
            lsn=lsn_value(boundary["checkpoint_lsn"]),
        )
        main_fork = next(
            (fork for fork in relation["forks"] if fork["fork"] == 0), None
        )
        if not relation["exists"] or main_fork is None or main_fork["nblocks"] <= 0:
            raise OracleMismatch(
                f"relation inspection did not find a nonempty main fork: {relation!r}"
            )
        observation = {
            "relation_key": relation_key,
            "declared_lsn": boundary["checkpoint_lsn"],
            "main_nblocks": main_fork["nblocks"],
            "result": relation,
        }
        fault_boundary_ref = (
            f"${materializer_fault['name']}"
            if materializer_fault is not None else None
        )
        compared_to_r1 = record_relation_observation(
            relation_observations, action["relation"], action["lsn"],
            observation, fault_boundary_ref,
        )
        events.emit(
            "relation_inspection", id=action["id"], target=action["target"],
            relation=action["relation"], timeline=0, incarnation=1,
            relation_key=relation_key, declared_lsn=boundary["checkpoint_lsn"],
            result=relation, main_nblocks=main_fork["nblocks"], phase=phase,
            compared_to_r1=compared_to_r1,
        )

    cleanup_errors: list[str] = []
    run_error: Exception | None = None

    def cleanup_step(label: str, callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception as error:
            cleanup_errors.append(f"{label}: {error}")

    def stop_child(process: subprocess.Popen[str] | None) -> None:
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def stop_materializer_for_cleanup() -> None:
        if not (materializer_data / "postmaster.pid").exists():
            return
        pid = postmaster_pid()
        stop_process_immediately(
            pid, diagnostic_pidfile=materializer_data / "postmaster.pid"
        )

    def stop_writer_for_cleanup() -> None:
        pidfile = writer_data / "postmaster.pid"
        if not pidfile.exists():
            return
        pid = int(pidfile.read_text(encoding="utf-8").splitlines()[0])
        subprocess.run(
            [str(pg_bin / "pg_ctl"), "-D", str(writer_data),
             "-m", "immediate", "-w", "stop"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
        )
        deadline = time.monotonic() + 15.0
        while pidfile.exists() or process_exists(pid):
            if time.monotonic() >= deadline:
                raise PlanError("writer immediate stop did not finish")
            time.sleep(0.05)

    try:
        events.emit(
            "run_start", scenario=plan.header["scenario"], seed=plan.header["seed"],
            shm=shm, postgres_major=postgres_major,
        )
        daemon_command = [
            str(daemon), "--shm", shm, "--store", str(store),
            "--page-size", str(profile["page_size"]),
            "--nshards", str(plan.header["case"]["shards"]),
            "--storage", plan.header["case"]["storage"],
        ]

        def start_store(reason: str) -> dict[str, Any]:
            nonlocal dproc
            with (trace / "daemon.log").open("a", encoding="utf-8") as log:
                dproc = subprocess.Popen(
                    daemon_command,
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
            events.emit("process_start", target="store", pid=dproc.pid, reason=reason)
            return health

        health = start_store("provision")
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
        def start_writer(reason: str) -> None:
            subprocess.run(
                [
                    str(pg_bin / "pg_ctl"), "-D", str(writer_data), "-l",
                    str(trace / "writer.log"), "-w", "start",
                ],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=env,
            )
            wait_scalar(
                writer_socket, writer_port, "SELECT 1", "1",
                f"writer did not accept connections after {reason}",
            )
            events.emit("process_start", target="writer", reason=reason)

        def process_instance(pid: int) -> dict[str, Any]:
            """A process identity a replacement cannot accidentally repeat:
            the PID together with its start time, since the OS is free to
            hand the old PID to the new process."""
            return {"pid": pid, "starttime": read_process_starttime(pid)}

        def restart_instance(target: str) -> dict[str, Any]:
            """What a restart of this target actually replaces: the writer's
            and the store's own process, and the materializer's worker
            generation.  A restart event that reported the materializer's
            generation for every target could not distinguish a writer or
            store that came back from one that never went down."""
            if target == "writer":
                return process_instance(compute_postmaster_pid(writer_data, "writer"))
            if target == "materializer":
                return {"generation": materializer_generation}
            if dproc is None:
                raise PlanError("store restart has no daemon process")
            return process_instance(dproc.pid)

        def stop_writer(reason: str) -> None:
            subprocess.run(
                [
                    str(pg_bin / "pg_ctl"), "-D", str(writer_data),
                    "-m", "fast", "-w", "stop",
                ],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=env,
            )
            events.emit("process_stop", target="writer", mode="fast", reason=reason)

        def stop_materializer_worker(reason: str) -> None:
            subprocess.run(
                [
                    str(pg_bin / "pg_ctl"), "-D", str(materializer_data),
                    "-m", "fast", "-w", "stop",
                ],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=env,
            )
            events.emit(
                "process_stop", target="materializer",
                generation=materializer_generation, mode="fast", reason=reason,
            )

        def stop_supervisor(reason: str) -> None:
            nonlocal supervisor_proc
            if supervisor_proc is None:
                return
            supervisor_proc.terminate()
            # SIGTERM only asks the supervisor to stop; it cannot leave a
            # blocking pg_ctl stop/start, each bounded by its own command
            # timeout, so the wait allows for both plus a margin.  If it still
            # has not exited, escalate and stop the worker it was driving,
            # rather than leaving a postmaster behind.
            try:
                supervisor_proc.wait(timeout=SUPERVISOR_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                supervisor_proc.kill()
                supervisor_proc.wait(timeout=30)
                stop_materializer_worker(f"{reason} (supervisor escalation)")
                raise PlanError(
                    f"materializer supervisor did not stop within "
                    f"{SUPERVISOR_STOP_TIMEOUT}s for {reason}"
                )
            if supervisor_proc.returncode != 0:
                raise PlanError(
                    f"materializer supervisor did not stop cleanly for {reason}: "
                    f"status {supervisor_proc.returncode}"
                )
            events.emit(
                "process_stop", target="materializer-supervisor",
                pid=supervisor_proc.pid, reason=reason,
            )
            supervisor_proc = None

        start_writer("provision")
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
        supervisor_env = env.copy()
        if materializer_fault is not None:
            fault_control.mkdir(mode=0o700, parents=True, exist_ok=False)
            supervisor_env.update(
                {
                    "PAGESTORE_TEST_FAULT_NAME": materializer_fault["fault"],
                    "PAGESTORE_TEST_FAULT_ACTION": materializer_fault["action"],
                    "PAGESTORE_TEST_FAULT_HIT": str(materializer_fault["hit"]),
                    "PAGESTORE_TEST_FAULT_DIR": str(fault_control),
                    "PAGESTORE_TEST_FAULT_SCENARIO": plan.header["scenario"],
                    "PAGESTORE_TEST_FAULT_SEED": str(plan.header["seed"]),
                    "PAGESTORE_TEST_FAULT_OPERATION": materializer_fault["id"],
                    "PAGESTORE_TEST_FAULT_WATCHDOG_MS": str(
                        materializer_fault_watchdog_milliseconds(materializer_fault["timeout"])
                    ),
                }
            )
        supervisor_starts = 0

        def start_supervisor(log_name: str) -> None:
            nonlocal supervisor_proc, supervisor_starts
            supervisor_starts += 1
            # Once the named fault has fired and been recovered its control
            # directory is gone; a supervisor started after that must not
            # inherit the fault configuration, or its worker would refuse the
            # missing directory instead of restarting.
            env = supervisor_env
            if materializer_recovered:
                env = {
                    key: value for key, value in supervisor_env.items()
                    if not key.startswith("PAGESTORE_TEST_FAULT_")
                }
            with (trace / log_name).open("w", encoding="utf-8") as log:
                supervisor_proc = subprocess.Popen(
                    [
                        sys.executable, str(supervisor),
                        "--config", str(supervisor_config),
                    ],
                    stdout=log, stderr=subprocess.STDOUT, text=True, env=env,
                )

        def wait_materializer_role(context: str) -> None:
            wait_scalar(
                materializer_socket, materializer_port,
                "SELECT pg_is_in_recovery() AND "
                "current_setting('pagestore.materializer')::boolean",
                "t", context,
            )

        start_supervisor("materializer-supervisor.log")
        supervisor_status = wait_supervisor_status(
            lambda status: (
                status.get("owner_pid") == supervisor_proc.pid
                and status.get("state")
                in {"running", "waiting_for_progress_api"}
            ),
            "materializer supervisor did not start its worker",
        )
        sync_materializer_generation(supervisor_status)
        wait_materializer_role("materializer did not enter its declared recovery role")
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
        start_supervisor("materializer-supervisor-replacement.log")
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
                checkpoints[action["name"]] = horizon
                (artifacts / f"{action['name']}.json").write_text(
                    json.dumps(horizon, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                events.emit(
                    "checkpoint", id=action["id"], name=action["name"],
                    horizon=horizon,
                )
            elif action["op"] == "materializer_fault":
                if materializer_fault is None or action["id"] != materializer_fault["id"]:
                    raise PlanError(
                        "materializer smoke encountered an unexpected fault action"
                    )
                if supervisor_proc is not None and supervisor_proc.poll() is None:
                    supervisor_proc.terminate()
                    try:
                        supervisor_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        supervisor_proc.kill()
                        supervisor_proc.wait(timeout=5)
                    events.emit(
                        "process_stop", target="materializer-supervisor",
                        pid=supervisor_proc.pid,
                        reason="harness owns the named restartpoint trigger",
                    )
                output = sql_result(
                    writer_socket, writer_port,
                    "CHECKPOINT; SELECT pg_current_wal_lsn();",
                ).stdout.strip().splitlines()
                checkpoint_lsn = output[-1]
                wal_file = archive_current_wal()
                wait_scalar(
                    materializer_socket, materializer_port,
                    f"SELECT pg_last_wal_replay_lsn() >= '{checkpoint_lsn}'::pg_lsn",
                    "t", f"materializer did not replay fault checkpoint {checkpoint_lsn}",
                    timeout=float(action.get("timeout", 30.0)),
                )
                replay_before_trigger = sql_scalar(
                    materializer_socket, materializer_port,
                    "SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0'::pg_lsn)::text",
                )
                marker_before_trigger = sql_scalar(
                    materializer_socket, materializer_port,
                    "SELECT pagestore_materialized_wal_lsn()::text",
                )
                _atomic_arm_marker(fault_control / "arm")
                events.emit(
                    "fault_arm", target="materializer", name=action["fault"],
                    hit=action["hit"], action=action["action"],
                )
                fault_trigger_proc = subprocess.Popen(
                    [
                        str(pg_bin / "pg_ctl"), "-D", str(materializer_data),
                        "-m", "fast", "-w", "stop",
                    ],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
                )
                events.emit(
                    "restartpoint_trigger", target="materializer",
                    pid=fault_trigger_proc.pid, model="fast_shutdown",
                    replay_lsn=replay_before_trigger,
                    marker_lsn=marker_before_trigger,
                )
                deadline = time.monotonic() + float(action.get("timeout", 30.0))
                while not fault_report.exists() and time.monotonic() < deadline:
                    if fault_trigger_proc.poll() is not None and not fault_report.exists():
                        raise PlanError(
                            "materializer fast-stop trigger exited before the named fault report"
                        )
                    time.sleep(0.02)
                if not fault_report.exists():
                    _capture_fault_diagnostics(
                        root, fault_control, trace / "materializer.log",
                        reason="materializer fault watchdog expired before report",
                        scenario=plan.header["scenario"], seed=plan.header["seed"],
                        fault=action["fault"], action=action["action"],
                        hit=action["hit"], operation=action["id"], process=None,
                    )
                    raise FaultNotReached(
                        f"materializer fault {action['fault']!r} watchdog expired"
                    )
                report = _fault_report(
                    fault_report, action["fault"], action["hit"], None,
                    action["action"], plan.header["scenario"],
                    plan.header["seed"], action["id"],
                    require_replay_lsn=True,
                )
                (trace / "materializer-fault-report.jsonl").write_text(
                    fault_report.read_text(encoding="utf-8"), encoding="utf-8"
                )
                old_postmaster_pid = postmaster_pid()
                fault_pid = report["pid"]
                if fault_pid in {old_postmaster_pid, supervisor_proc.pid if supervisor_proc else -1}:
                    raise PlanError(
                        "materializer fault report was not emitted by a recovery child"
                    )
                declared_lsn = lsn_value(checkpoint_lsn)
                replay_at_fault = parse_lsn_value(report["replay_lsn"])
                if replay_at_fault < declared_lsn:
                    raise OracleMismatch(
                        "materializer fault report replay_lsn precedes declared checkpoint"
                    )
                replay_at_fault_text = report["replay_lsn"]
                baseline_marker = checkpoints["R1"]["materialized_wal_lsn"]
                if baseline_marker is None:
                    raise OracleMismatch("R1 has no durable materializer marker")
                baseline_retention_owner_lsn = checkpoints["R1"].get(
                    "retention_owner_lsn"
                )
                if baseline_retention_owner_lsn is None:
                    raise OracleMismatch(
                        "R1 has no durable materializer retention owner LSN"
                    )
                events.emit(
                    "fault_reached", target="materializer", name=action["fault"],
                    report=report, reached=True, fault_pid=fault_pid,
                    postmaster_pid=old_postmaster_pid,
                    replay_lsn=replay_at_fault_text,
                    replay_lsn_source="fault report actual probe replay_lsn",
                    marker_lsn=None,
                    marker_source="post-crash writer status",
                    declared_lsn=checkpoint_lsn,
                    wal_file=wal_file,
                )
                crashed_generation = materializer_generation
                crashed_retention_generation = materializer_retention_generation
                crash_materializer(action["id"])
                if fault_trigger_proc is not None:
                    try:
                        fault_trigger_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        fault_trigger_proc.kill()
                        fault_trigger_proc.wait(timeout=5)
                    fault_trigger_proc = None
                _disarm_fault_control(fault_control)
                _cleanup_fault_control(fault_control)
                if (materializer_data / "postmaster.pid").exists():
                    raise OracleMismatch(
                        "materializer remained up while collecting post-crash evidence"
                    )
                post_crash_status = read_materializer_status()
                post_crash_marker = post_crash_status.get("materialized_wal_lsn")
                if baseline_marker is None or post_crash_marker is None:
                    raise OracleMismatch(
                        "post-crash materializer status did not contain a durable marker"
                    )
                if action["fault"] == "materializer.after_relation_sync":
                    if post_crash_marker != baseline_marker:
                        raise OracleMismatch(
                            "before-marker crash changed the durable marker beyond R1"
                        )
                    if lsn_value(post_crash_marker) >= declared_lsn:
                        raise OracleMismatch(
                            "before-marker crash left a durable marker at or beyond R2"
                        )
                elif lsn_value(post_crash_marker) < declared_lsn:
                    raise OracleMismatch(
                        "after-marker crash did not leave R2's marker durable"
                    )
                checkpoints[action["name"]] = {"checkpoint_lsn": checkpoint_lsn}
                fault_boundary_ref = f"${action['name']}"
                for relation_action in plan.actions:
                    if (
                        relation_action["op"] == "inspect_relation"
                        and relation_action["lsn"] in {"$R1", fault_boundary_ref}
                    ):
                        pre_action = dict(relation_action)
                        pre_action["id"] = (
                            f"pre-recovery-{relation_action['id']}"
                        )
                        inspect_relation_action(
                            pre_action, "post_crash_pre_recovery"
                        )
                events.emit(
                    "post_crash_durable_state", target="materializer",
                    status=post_crash_status, marker_lsn=post_crash_marker,
                    baseline_marker_lsn=baseline_marker,
                    declared_lsn=checkpoint_lsn,
                    materializer_down=True,
                    marker_source="writer status direct store read after report",
                )
                events.emit(
                    "crash", id=action["id"], target="materializer", model="compute",
                    postmaster_pid=old_postmaster_pid, fault_pid=fault_pid,
                    generation=crashed_generation,
                    retention_generation=crashed_retention_generation,
                )
                with (trace / "materializer-supervisor-recovery.log").open(
                    "w", encoding="utf-8"
                ) as log:
                    supervisor_proc = subprocess.Popen(
                        [sys.executable, str(supervisor), "--config", str(supervisor_config)],
                        stdout=log, stderr=subprocess.STDOUT, text=True, env=env,
                    )
                supervisor_status = wait_supervisor_status(
                    lambda status: (
                        status.get("owner_pid") == supervisor_proc.pid
                        and status.get("state")
                        in {"running", "waiting_for_progress_api"}
                    ),
                    "materializer supervisor did not recover the faulted postmaster",
                    timeout=float(action.get("timeout", 30.0)) + 20.0,
                )
                sync_materializer_generation(supervisor_status)
                new_postmaster_pid = postmaster_pid()
                if new_postmaster_pid == old_postmaster_pid:
                    raise OracleMismatch(
                        "materializer recovery reused the faulted postmaster PID"
                    )
                wait_scalar(
                    materializer_socket, materializer_port,
                    "SELECT pg_is_in_recovery() AND "
                    "current_setting('pagestore.materializer')::boolean",
                    "t", "recovered materializer did not become healthy",
                    timeout=float(action.get("timeout", 30.0)) + 20.0,
                )
                wait_scalar(
                    materializer_socket, materializer_port,
                    f"SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0'::pg_lsn) >= "
                    f"'{checkpoint_lsn}'::pg_lsn",
                    "t", "recovered materializer did not replay the fault checkpoint",
                    timeout=float(action.get("timeout", 30.0)) + 20.0,
                )
                recovered_replay_lsn = sql_scalar(
                    materializer_socket, materializer_port,
                    "SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0'::pg_lsn)::text",
                )
                recovery_restartpoint_result = sql_result(
                    materializer_socket, materializer_port, "CHECKPOINT;"
                ).stdout.strip()
                events.emit(
                    "recovery_restartpoint", target="materializer",
                    replay_lsn=recovered_replay_lsn,
                    result=recovery_restartpoint_result,
                )
                wait_scalar(
                    materializer_socket, materializer_port,
                    f"SELECT COALESCE(pagestore_materialized_wal_lsn(), '0/0'::pg_lsn) >= "
                    f"'{checkpoint_lsn}'::pg_lsn",
                    "t", "recovered materializer did not publish the fault marker",
                    timeout=float(action.get("timeout", 30.0)) + 20.0,
                )
                materializer_recovered = True
                recovered_marker_lsn = sql_scalar(
                    materializer_socket, materializer_port,
                    "SELECT pagestore_materialized_wal_lsn()::text",
                )
                recovered_status = read_materializer_status()
                if lsn_value(recovered_marker_lsn) < lsn_value(post_crash_marker):
                    raise OracleMismatch(
                        "recovered materializer marker regressed across process crash"
                    )
                if lsn_value(recovered_marker_lsn) < declared_lsn:
                    raise OracleMismatch(
                        "recovered materializer marker did not cover the declared checkpoint"
                    )
                retention_timeout = float(action.get("timeout", 30.0)) + 20.0
                wait_scalar(
                    writer_socket, writer_port,
                    "SELECT COALESCE(pagestore_retention_owner_lsn(0, 2, 1, "
                    + str(materializer_retention_generation)
                    + "), '0/0'::pg_lsn) > '"
                    + baseline_retention_owner_lsn + "'::pg_lsn",
                    "t", "recovered retention owner did not advance beyond R1",
                    timeout=retention_timeout,
                )
                recovered_retention_owner_lsn = read_retention_owner_lsn(
                    materializer_retention_generation
                )
                if recovered_retention_owner_lsn is None:
                    raise OracleMismatch(
                        "recovered materializer has no durable retention owner LSN"
                    )
                if lsn_value(recovered_retention_owner_lsn) <= lsn_value(
                    baseline_retention_owner_lsn
                ):
                    raise OracleMismatch(
                        "recovered retention owner did not advance beyond the R1 owner LSN"
                    )
                retention_advancement = (
                    lsn_value(recovered_retention_owner_lsn)
                    - lsn_value(baseline_retention_owner_lsn)
                )
                events.emit(
                    "retention_recovered", target="materializer",
                    owner_kind=2, owner_id=1,
                    owner_generation=materializer_retention_generation,
                    r1_owner_lsn=baseline_retention_owner_lsn,
                    recovered_owner_lsn=recovered_retention_owner_lsn,
                    advancement_delta=retention_advancement,
                    declared_lsn=checkpoint_lsn,
                )
                horizon = {
                    "checkpoint_lsn": checkpoint_lsn,
                    "shipped_wal_lsn": post_crash_status["shipped_wal_lsn"],
                    "materialized_wal_lsn": post_crash_status["materialized_wal_lsn"],
                    "lag_bytes": post_crash_status["lag_bytes"],
                    "post_crash_status": post_crash_status,
                    "recovered_status": recovered_status,
                    "wal_file": wal_file,
                    "materializer_generation": materializer_generation,
                    "old_postmaster_pid": old_postmaster_pid,
                    "new_postmaster_pid": new_postmaster_pid,
                    "replay_lsn_at_fault": replay_at_fault_text,
                    "recovered_replay_lsn": recovered_replay_lsn,
                    "marker_before_trigger": marker_before_trigger,
                    "marker_after_report_before_recovery": post_crash_marker,
                    "post_crash_marker_lsn": post_crash_marker,
                    "recovered_marker_lsn": recovered_marker_lsn,
                    "r1_retention_owner_lsn": baseline_retention_owner_lsn,
                    "recovered_retention_owner_lsn": recovered_retention_owner_lsn,
                    "retention_advancement_delta": retention_advancement,
                    "retention_owner_generation": materializer_retention_generation,
                    "fault_pid": fault_pid,
                    "fault_report_lines": 1,
                }
                checkpoints[action["name"]] = horizon
                (artifacts / f"{action['name']}.json").write_text(
                    json.dumps(horizon, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                events.emit(
                    "materialized_boundary", id=action["id"], name=action["name"],
                    horizon=horizon, marker_monotonic=True, fault_replayed_once=True,
                )
            elif action["op"] == "inspect_relation":
                inspect_relation_action(
                    action,
                    "post_recovery" if materializer_recovered else "pre_crash",
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
            elif action["op"] == "restart":
                restarted_generation = materializer_generation
                restarted_retention_generation = materializer_retention_generation
                previous_instance = restart_instance(action["target"])
                if action["target"] == "writer":
                    stop_writer(action["id"])
                    start_writer(action["id"])
                elif action["target"] == "materializer":
                    # The supervisor keeps running and treats a cleanly stopped
                    # worker exactly like a crashed one: a new worker and a new
                    # retention generation, adopted through the same status.
                    stop_materializer_worker(action["id"])
                    supervisor_status = wait_supervisor_status(
                        lambda status: (
                            isinstance(status.get("worker_generation"), int)
                            and status["worker_generation"] > restarted_generation
                            and isinstance(status.get("retention_generation"), int)
                            and status["retention_generation"]
                                > restarted_retention_generation
                            and status.get("state")
                            in {"running", "waiting_for_progress_api"}
                        ),
                        f"supervisor did not replace the stopped worker after {action['id']}",
                    )
                    sync_materializer_generation(supervisor_status)
                    wait_materializer_role(
                        f"restarted materializer did not become healthy after {action['id']}"
                    )
                else:
                    # Every attached compute must be down while the daemon
                    # reinitializes its shared memory; the supervisor stops
                    # first so it cannot replace the worker mid-restart.
                    stop_supervisor(action["id"])
                    stop_materializer_worker(action["id"])
                    stop_writer(action["id"])
                    assert dproc is not None
                    dproc.terminate()
                    # the daemon's own clean-stop bound: draining maintenance
                    # and flushing persistent state is allowed to take this
                    # long, and a slower shutdown is not a failed one
                    dproc.wait(timeout=60)
                    if dproc.returncode != 0:
                        raise UnexpectedExit(
                            f"daemon did not stop cleanly for {action['id']}: "
                            f"status {dproc.returncode}"
                        )
                    events.emit(
                        "process_stop", target="store", pid=dproc.pid,
                        returncode=dproc.returncode, reason=action["id"],
                    )
                    remove_shm(shm)
                    health = start_store(action["id"])
                    start_writer(action["id"])
                    start_supervisor(f"materializer-supervisor-restart-{supervisor_starts}.log")
                    supervisor_status = wait_supervisor_status(
                        lambda status: (
                            status.get("owner_pid") == supervisor_proc.pid
                            and isinstance(status.get("worker_generation"), int)
                            and status["worker_generation"] > restarted_generation
                            and status.get("state")
                            in {"running", "waiting_for_progress_api"}
                        ),
                        f"supervisor did not restart the worker after {action['id']}",
                    )
                    sync_materializer_generation(supervisor_status)
                    wait_materializer_role(
                        f"materializer did not recover after {action['id']}"
                    )
                instance = restart_instance(action["target"])
                if instance == previous_instance:
                    raise OracleMismatch(
                        f"restart {action['id']} left the {action['target']} "
                        f"instance {previous_instance!r} in place"
                    )
                # The materializer's generation is the restarted instance only
                # when the materializer is the target; for a writer or store
                # restart it is unrelated context, and reporting it as the
                # event's generation described a restart that never showed.
                events.emit(
                    "restart", id=action["id"], target=action["target"],
                    previous_instance=previous_instance, instance=instance,
                    previous_materializer_generation=restarted_generation,
                    materializer_generation=materializer_generation,
                    health=health if action["target"] == "store" else None,
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
        run_error = error
        classification = fault_failure_classification(error)
        _preserve_fault_evidence(fault_control, trace)
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
        cleanup_step("fault trigger", lambda: stop_child(fault_trigger_proc))
        cleanup_step("fault disarm", lambda: _disarm_fault_control(fault_control))
        cleanup_step("fault control cleanup", lambda: _cleanup_fault_control(fault_control))
        cleanup_step("materializer supervisor", lambda: stop_child(supervisor_proc))
        cleanup_step("materializer postmaster", stop_materializer_for_cleanup)
        cleanup_step("writer postmaster", stop_writer_for_cleanup)
        cleanup_step("pagestore daemon", lambda: stop_child(dproc))
        cleanup_step("shared memory", lambda: remove_shm(shm))
    cleanup_temporary_root(root, temporary, keep, cleanup_errors)
    if cleanup_errors and run_error is None:
        raise PlanError(
            f"materializer smoke cleanup failed; failure bundle: {root}: "
            + "; ".join(cleanup_errors)
        )
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
    group.add_argument("--inspect", choices=sorted(INSPECTION_OPERATIONS))
    group.add_argument("--daemon-smoke", type=Path, metavar="PLAN")
    group.add_argument(
        "--daemon-fault-smoke", "--daemon-fault-recovery",
        dest="daemon_fault_smoke", type=Path, metavar="PLAN",
    )
    group.add_argument("--writer-smoke", type=Path, metavar="PLAN")
    group.add_argument("--materializer-smoke", type=Path, metavar="PLAN")
    group.add_argument("--legacy-integration", action="store_true")
    parser.add_argument("--daemon-binary", type=Path)
    parser.add_argument("--layer-client-binary", type=Path)
    parser.add_argument("--gc-client-binary", type=Path)
    parser.add_argument("--materializer-supervisor", type=Path)
    parser.add_argument("--build-dir", type=Path)
    parser.add_argument("--integration-script", type=Path,
                        default=Path(__file__).resolve().parents[1] / "integration_test.sh")
    parser.add_argument("--inspect-binary", type=Path)
    parser.add_argument("--shm")
    parser.add_argument("--timeline", type=int, metavar="ID")
    parser.add_argument("--incarnation", type=int, metavar="GENERATION")
    parser.add_argument("--spc-oid", type=int, metavar="OID")
    parser.add_argument("--db-oid", type=int, metavar="OID")
    parser.add_argument("--rel-number", type=int, metavar="OID")
    parser.add_argument("--lsn", type=int, metavar="LSN")
    parser.add_argument("--inspection-schema", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--junit", type=Path)
    args = parser.parse_args(argv)
    if args.inspect and (args.inspect_binary is None or not args.shm):
        parser.error("--inspect requires --inspect-binary and --shm")
    relation_flag_values = (
        args.incarnation, args.spc_oid, args.db_oid, args.rel_number, args.lsn,
    )
    if args.inspect != "relation" and any(value is not None for value in relation_flag_values):
            parser.error(
                "--incarnation, --spc-oid, --db-oid, --rel-number and --lsn are only valid with "
                "--inspect relation"
            )
    if args.inspect == "timeline":
        if args.timeline is None or args.timeline < 0:
            parser.error("--inspect timeline requires a nonnegative --timeline ID")
    elif args.inspect == "relation":
        relation_values = (
            args.timeline, args.spc_oid, args.db_oid, args.rel_number, args.lsn,
        )
        if (
            args.incarnation is None
            or isinstance(args.incarnation, bool)
            or args.incarnation <= 0
            or any(value is None or isinstance(value, bool) or value < 0
                   for value in relation_values)
        ):
            parser.error(
                "--inspect relation requires nonnegative --timeline, --spc-oid, "
                "--db-oid, --rel-number and --lsn plus a positive --incarnation"
            )
    elif args.timeline is not None:
        parser.error("--timeline is only valid with --inspect timeline")
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
            print(json.dumps(
                inspect_store(
                    args.inspect_binary, args.shm, args.inspect, schema,
                    timeline=args.timeline,
                    incarnation=args.incarnation if args.inspect == "relation" else None,
                    relation_key=(args.spc_oid, args.db_oid, args.rel_number)
                    if args.inspect == "relation" else None,
                    lsn=args.lsn if args.inspect == "relation" else None,
                ),
                sort_keys=True,
            ))
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
                layer_client=args.layer_client_binary,
                gc_client=args.gc_client_binary,
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
