#!/usr/bin/env python3
"""The JSON artifacts the branch controller and the materializer supervisor
persist: their schema numbers, checksums and key sets, in one module both
tools and the persisted-format fixture import.

D5 rule 4: these files are page-store envelopes -- a controller's journal
and generation authority, a supervisor's status and retention authority, and
the configuration an operator hands either -- so each carries a ``schema``
member naming its layout, and the ones a later run relies on carry a
``crc32`` over the rest of the object.  A reader accepts the current schema,
the legacy schemas listed for the kind (which may predate the checksum), and
nothing else: an unknown schema, a missing or wrong checksum, or a key set
the layout does not define fails closed.  ``--identities`` prints the table
``harness/pagestore_controller_fixture.py`` pins to its fixture.
"""
from __future__ import annotations

import json
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ArtifactError(ValueError):
    pass


@dataclass(frozen=True)
class ArtifactSpec:
    family: str
    artifact: str
    schema: int
    accepted: frozenset[int | None]  # schemas a reader still loads; None: no member
    refused: frozenset[int]          # schemas a reader names as unsupported
    crc_from: int | None             # the schema from which crc32 is required
    required: frozenset[str]         # keys every current-schema object carries
    closed: bool                     # no keys beyond `required` and `optional`
    optional: frozenset[str] = frozenset()

    def carries_crc(self, schema: int | None) -> bool:
        return self.crc_from is not None and schema is not None and schema >= self.crc_from


BRANCH_CONFIG_FIELDS = frozenset({
    "schema", "pg_ctl", "psql", "writer_data_dir", "writer_host", "writer_port",
    "writer_log_file", "private_socket_dir", "private_port", "materializer_data_dir",
    "materializer_host", "materializer_port", "retention_authority_dir",
    "retention_owner_id", "prepared_dir", "new_timeline", "parent_timeline",
    "new_incarnation", "database", "user", "poll_interval_ms", "progress_timeout_ms",
    "command_timeout_seconds",
})
BRANCH_CONFIG_REQUIRED = frozenset({
    "schema", "pg_ctl", "psql", "writer_data_dir", "writer_host", "writer_port",
    "writer_log_file", "private_socket_dir", "private_port", "materializer_data_dir",
    "materializer_host", "materializer_port", "retention_authority_dir",
    "retention_owner_id", "prepared_dir", "new_timeline", "parent_timeline",
})
BRANCH_JOURNAL_KEYS = frozenset({
    "schema", "operation", "identity", "state", "intent", "base_lsn",
    "checkpoint_redo_lsn", "checkpoint_end_lsn", "switch_lsn",
    "archived_through_lsn", "fork_lsn", "seeded_slru_pages", "retention_generation",
    "pause_owned", "writer_owned", "retention_owned", "retention_set_attempted",
    "restricted_writer_running", "materializer_resumed", "writer_restored",
    "prepared_dir", "crc32",
})
SUPERVISOR_CONFIG_FIELDS = frozenset({
    "schema", "pg_ctl", "psql", "data_dir", "socket_dir", "port", "log_file", "state_dir",
    "retention_authority_dir", "retention_owner_id", "controller_instance_id", "database",
    "user", "poll_interval_ms", "replay_idle_ms", "progress_timeout_ms",
    "retry_initial_ms", "retry_max_ms", "max_consecutive_failures",
    "command_timeout_seconds",
})
SUPERVISOR_CONFIG_REQUIRED = frozenset({
    "schema", "pg_ctl", "psql", "data_dir", "socket_dir", "port", "log_file", "state_dir",
    "retention_authority_dir", "retention_owner_id", "controller_instance_id",
})
SUPERVISOR_STATUS_REQUIRED = frozenset({
    "schema", "state", "owner_pid", "owner_epoch", "worker_generation",
    "retention_owner_id", "retention_generation", "consecutive_failures", "last_error",
    "updated_at", "monotonic_ns", "progress", "crc32",
})
RETENTION_AUTHORITY_REQUIRED = frozenset({
    "schema", "retention_generation", "consumer_data_dir", "consumer_instance_id",
    "consumer_data_dev", "consumer_data_ino", "authority_namespace_dev",
    "authority_namespace_ino", "crc32",
})
BRANCH_RETENTION_GENERATION_REQUIRED = frozenset({
    "schema", "retention_owner_id", "generation", "crc32",
})

ARTIFACTS: dict[str, ArtifactSpec] = {
    # what an operator hands the controller; other schemas are refused
    "branch_config": ArtifactSpec(
        "controller", "branch controller configuration (JSON, schema member)",
        schema=2, accepted=frozenset({2}), refused=frozenset(), crc_from=None,
        required=BRANCH_CONFIG_REQUIRED, closed=True,
        optional=BRANCH_CONFIG_FIELDS - BRANCH_CONFIG_REQUIRED),
    # the controller's crash journal: schema 1 was a receipt without the
    # prepared-manifest identity and is refused by name
    "branch_journal": ArtifactSpec(
        "controller", "pagestore_branch.prepare.json (JSON, schema member, crc32)",
        schema=2, accepted=frozenset({2}), refused=frozenset({1}), crc_from=2,
        required=BRANCH_JOURNAL_KEYS, closed=True),
    # the controller's own retention generation authority; schema 1 had no
    # checksum and is read as legacy
    "branch_retention_generation": ArtifactSpec(
        "controller", "branch-retention-generation-<id>.json (JSON, schema member, crc32)",
        schema=2, accepted=frozenset({1, 2}), refused=frozenset(), crc_from=2,
        required=BRANCH_RETENTION_GENERATION_REQUIRED, closed=True),
    "supervisor_config": ArtifactSpec(
        "supervisor", "materializer supervisor configuration (JSON, schema member)",
        schema=4, accepted=frozenset({4}), refused=frozenset(), crc_from=None,
        required=SUPERVISOR_CONFIG_REQUIRED, closed=True,
        optional=SUPERVISOR_CONFIG_FIELDS - SUPERVISOR_CONFIG_REQUIRED),
    # the supervisor's status: schema 2 had no checksum and is read as legacy,
    # as is a status that predates the schema member; a status carries
    # whatever the publisher adds, so its key set is open
    "supervisor_status": ArtifactSpec(
        "supervisor", "status.json (JSON, schema member, crc32)",
        schema=3, accepted=frozenset({None, 2, 3}), refused=frozenset(), crc_from=3,
        required=SUPERVISOR_STATUS_REQUIRED, closed=False),
    # the materializer's retention generation authority, read by the
    # controller too; the original carried no schema member and is legacy
    "retention_authority": ArtifactSpec(
        "supervisor", "retention-owner-<id>.json (JSON, schema member, crc32)",
        schema=1, accepted=frozenset({None, 1}), refused=frozenset(), crc_from=1,
        required=RETENTION_AUTHORITY_REQUIRED, closed=True),
}


def artifact_crc(value: dict[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key != "crc32"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"{zlib.crc32(encoded) & 0xffffffff:08x}"


def stamp(kind: str, value: dict[str, Any]) -> dict[str, Any]:
    """The object as the current schema writes it: schema member first,
    checksum last."""
    spec = ARTIFACTS[kind]
    stamped = dict(value)
    stamped.pop("crc32", None)
    stamped["schema"] = spec.schema
    if spec.carries_crc(spec.schema):
        stamped["crc32"] = artifact_crc(stamped)
    return stamped


def parse(kind: str, value: Any) -> dict[str, Any]:
    """Judge an already-decoded object against the kind's layout; returns it."""
    spec = ARTIFACTS[kind]
    if not isinstance(value, dict):
        raise ArtifactError(f"{spec.artifact} must be a JSON object")
    schema = value.get("schema")
    if isinstance(schema, bool) or (schema is not None and not isinstance(schema, int)):
        raise ArtifactError(f"{spec.artifact} schema must be {spec.schema}")
    if schema in spec.refused:
        raise ArtifactError(f"{spec.artifact} schema {schema} is unsupported")
    if schema not in spec.accepted:
        raise ArtifactError(f"{spec.artifact} schema must be {spec.schema}")
    if spec.carries_crc(schema):
        crc = value.get("crc32")
        if not isinstance(crc, str) or crc != artifact_crc(value):
            raise ArtifactError(f"{spec.artifact} CRC mismatch")
    elif "crc32" in value:
        raise ArtifactError(f"{spec.artifact} schema {schema} carries no checksum")
    if schema == spec.schema:
        keys = set(value)
        missing = sorted(spec.required - keys)
        if missing:
            raise ArtifactError(f"{spec.artifact} lacks {', '.join(missing)}")
        if spec.closed:
            unknown = sorted(keys - spec.required - spec.optional)
            if unknown:
                raise ArtifactError(f"{spec.artifact} has unknown member(s) {', '.join(unknown)}")
    return value


def load(kind: str, path: Path) -> dict[str, Any]:
    """Read and judge one artifact; FileNotFoundError passes through."""
    spec = ARTIFACTS[kind]
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise
    except (OSError, UnicodeError) as error:
        raise ArtifactError(f"{spec.artifact} is unreadable: {error}") from error
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise ArtifactError(f"{spec.artifact} is not JSON: {error}") from error
    return parse(kind, value)


def identities() -> list[dict[str, Any]]:
    """The persisted-format identity table, in the shape
    pagestore_format_versions prints (a JSON artifact has no magic)."""
    return sorted(
        (
            {"family": spec.family, "artifact": spec.artifact, "magic": "0x00000000",
             "version": spec.schema}
            for spec in ARTIFACTS.values()
        ),
        key=lambda item: (item["family"], item["artifact"]),
    )


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--identities"]:
        print(json.dumps(identities(), indent=2))
        return 0
    print("usage: pagestore_artifact_schema.py --identities", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
