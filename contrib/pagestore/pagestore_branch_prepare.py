#!/usr/bin/env python3
"""Prepare one pagestore branch while serializing its correctness boundary."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pagestore_branch_fault import BranchFaultProbe


EX_TEMPFAIL = 75
EX_CONFIG = 78
CONFIG_SCHEMA = 2
RECEIPT_SCHEMA = 2
LEGACY_RECEIPT_SCHEMA = 1
JOURNAL_OPERATION = "pagestore_branch_prepare"
JOURNAL_STATES = {
    "started", "preflight_complete", "base_captured", "writer_stopped",
    "writer_restricted", "checkpoint_selected", "checkpoint_archived",
    "fork_captured", "branch_prepared", "prepared", "materializer_resumed",
    "writer_restored", "complete",
}
JOURNAL_KEYS = {
    "schema", "operation", "identity", "state", "intent", "base_lsn",
    "checkpoint_redo_lsn", "checkpoint_end_lsn", "switch_lsn",
    "archived_through_lsn", "fork_lsn",
    "seeded_slru_pages", "retention_generation", "pause_owned", "writer_owned",
    "retention_owned",
    "restricted_writer_running", "materializer_resumed", "writer_restored",
    "prepared_dir", "crc32",
}
SAFE_POSTGRES_OPTION_PATH = re.compile(r"^[A-Za-z0-9_./-]+$")
WAL_FILE_NAME = re.compile(r"^[0-9A-F]{24}$")
CONFIG_FIELDS = {
    "schema",
    "pg_ctl",
    "psql",
    "writer_data_dir",
    "writer_host",
    "writer_port",
    "writer_log_file",
    "private_socket_dir",
    "private_port",
    "materializer_data_dir",
    "materializer_host",
    "materializer_port",
    "retention_authority_dir",
    "retention_owner_id",
    "prepared_dir",
    "new_timeline",
    "parent_timeline",
    "new_incarnation",
    "database",
    "user",
    "poll_interval_ms",
    "progress_timeout_ms",
    "command_timeout_seconds",
}
REQUIRED_CONFIG_FIELDS = {
    "schema",
    "pg_ctl",
    "psql",
    "writer_data_dir",
    "writer_host",
    "writer_port",
    "writer_log_file",
    "private_socket_dir",
    "private_port",
    "materializer_data_dir",
    "materializer_host",
    "materializer_port",
    "retention_authority_dir",
    "retention_owner_id",
    "prepared_dir",
    "new_timeline",
    "parent_timeline",
}


class ConfigError(ValueError):
    pass


def validate_authority_path(authority_dir: Path) -> os.stat_result:
    effective_uid = os.geteuid()
    authority_stat = os.lstat(authority_dir)
    if (
        not stat.S_ISDIR(authority_stat.st_mode)
        or authority_stat.st_uid != effective_uid
        or stat.S_IMODE(authority_stat.st_mode) != 0o700
    ):
        raise ConfigError(
            "retention_authority_dir must be owned by this user and mode 0700"
        )
    component = authority_dir.parent
    immediate = True
    while True:
        component_stat = os.lstat(component)
        if not stat.S_ISDIR(component_stat.st_mode):
            raise ConfigError(
                "retention_authority_dir ancestry must contain only directories"
            )
        writable = component_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        sticky = component_stat.st_mode & stat.S_ISVTX
        if immediate and (
            component_stat.st_uid != effective_uid or writable
        ):
            raise ConfigError(
                "retention_authority_dir parent must be owner-controlled and not group/world writable"
            )
        if not immediate and writable and not sticky:
            raise ConfigError(
                "retention_authority_dir ancestry contains a replaceable writable directory"
            )
        if component.parent == component:
            break
        component = component.parent
        immediate = False
    return authority_stat


class OwnershipError(RuntimeError):
    pass


class BranchPrepareError(RuntimeError):
    pass


class CancelledError(BranchPrepareError):
    pass


def parse_lsn(value: str) -> int:
    try:
        high, low = value.split("/", 1)
        return (int(high, 16) << 32) | int(low, 16)
    except (ValueError, AttributeError) as error:
        raise ValueError(f"invalid PostgreSQL LSN {value!r}") from error


def sql_literal(value: str) -> str:
    if "\x00" in value:
        raise ValueError("SQL string contains NUL")
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    if "\x00" in value:
        raise ValueError("SQL identifier contains NUL")
    return '"' + value.replace('"', '""') + '"'


def last_output_line(value: str) -> str:
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if not lines:
        raise BranchPrepareError("PostgreSQL command returned no result")
    return lines[-1]


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class Config:
    pg_ctl: Path
    psql: Path
    writer_data_dir: Path
    writer_host: str
    writer_port: int
    writer_log_file: Path
    private_socket_dir: Path
    private_port: int
    materializer_data_dir: Path
    materializer_host: str
    materializer_port: int
    retention_authority_dir: Path
    retention_owner_id: int
    prepared_dir: Path
    new_timeline: int
    parent_timeline: int
    new_incarnation: int = 1
    database: str = "postgres"
    user: str = "postgres"
    poll_interval_ms: int = 100
    progress_timeout_ms: int = 60000
    command_timeout_seconds: int = 60

    @classmethod
    def load(cls, path: Path) -> Config:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ConfigError(f"cannot read branch config {path}: {error}") from error
        if not isinstance(value, dict):
            raise ConfigError("branch config must be a JSON object")
        unknown = sorted(set(value) - CONFIG_FIELDS)
        missing = sorted(REQUIRED_CONFIG_FIELDS - set(value))
        if unknown:
            raise ConfigError(f"unknown branch config field(s): {', '.join(unknown)}")
        if missing:
            raise ConfigError(f"missing branch config field(s): {', '.join(missing)}")
        if (
            value.get("schema") != CONFIG_SCHEMA
            or isinstance(value.get("schema"), bool)
        ):
            raise ConfigError(f"branch config schema must be {CONFIG_SCHEMA}")

        paths: dict[str, Path] = {}
        for field in (
            "pg_ctl",
            "psql",
            "writer_data_dir",
            "writer_log_file",
            "private_socket_dir",
            "materializer_data_dir",
            "retention_authority_dir",
            "prepared_dir",
        ):
            item = value[field]
            if not isinstance(item, str) or not item:
                raise ConfigError(f"branch config {field} must be a path string")
            paths[field] = Path(item)
            if not paths[field].is_absolute():
                raise ConfigError(f"branch config {field} must be absolute")

        for field in ("pg_ctl", "psql"):
            program = paths[field]
            if not program.is_file() or not os.access(program, os.X_OK):
                raise ConfigError(f"branch config {field} is not executable: {program}")
        for field in ("writer_data_dir", "materializer_data_dir"):
            if not (paths[field] / "PG_VERSION").is_file():
                raise ConfigError(f"branch config {field} is not provisioned: {paths[field]}")
        writer_data = paths["writer_data_dir"].resolve()
        materializer_data = paths["materializer_data_dir"].resolve()
        prepared = paths["prepared_dir"].resolve()
        if writer_data == materializer_data:
            raise ConfigError("writer and materializer data directories must differ")
        for data_dir in (writer_data, materializer_data):
            if (
                prepared == data_dir
                or data_dir in prepared.parents
                or prepared in data_dir.parents
            ):
                raise ConfigError("prepared_dir must be outside both PostgreSQL data directories")
        if not SAFE_POSTGRES_OPTION_PATH.fullmatch(str(paths["private_socket_dir"])):
            raise ConfigError("private_socket_dir contains characters unsafe for pg_ctl -o")

        integers: dict[str, int] = {}
        defaults = {
            "writer_port": None,
            "private_port": None,
            "materializer_port": None,
            "retention_owner_id": None,
            "new_timeline": None,
            "parent_timeline": None,
            "new_incarnation": 1,
            "poll_interval_ms": 100,
            "progress_timeout_ms": 60000,
            "command_timeout_seconds": 60,
        }
        for field, default in defaults.items():
            item = value.get(field, default)
            minimum = 0 if field == "parent_timeline" else 1
            if (
                not isinstance(item, int)
                or isinstance(item, bool)
                or item < minimum
            ):
                qualifier = "non-negative" if minimum == 0 else "positive"
                raise ConfigError(f"branch config {field} must be a {qualifier} integer")
            integers[field] = item
        for field in ("writer_port", "private_port", "materializer_port"):
            if integers[field] > 65535:
                raise ConfigError(f"branch config {field} exceeds 65535")
        if integers["retention_owner_id"] > (1 << 64) - 1:
            raise ConfigError("branch config retention_owner_id exceeds uint64")
        if integers["new_incarnation"] > (1 << 63) - 1:
            raise ConfigError("branch config new_incarnation exceeds int64")
        if integers["new_timeline"] == integers["parent_timeline"]:
            raise ConfigError("new_timeline must differ from parent_timeline")
        for field in ("new_timeline", "parent_timeline"):
            if integers[field] > 1023:
                raise ConfigError(f"branch config {field} exceeds 1023")

        strings: dict[str, str] = {}
        for field, default in (
            ("writer_host", None),
            ("materializer_host", None),
            ("database", "postgres"),
            ("user", "postgres"),
        ):
            item = value.get(field, default)
            if not isinstance(item, str) or not item or "\x00" in item:
                raise ConfigError(f"branch config {field} must be a non-empty string")
            strings[field] = item

        paths["writer_log_file"].parent.mkdir(parents=True, exist_ok=True)
        try:
            log_fd = os.open(
                paths["writer_log_file"],
                os.O_WRONLY | os.O_APPEND | os.O_CREAT,
                0o600,
            )
            os.close(log_fd)
        except OSError as error:
            raise ConfigError(
                f"writer_log_file is not writable: {paths['writer_log_file']}: {error}"
            ) from error
        paths["prepared_dir"].mkdir(parents=True, exist_ok=True)
        authority_dir = paths["retention_authority_dir"]
        authority_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        validate_authority_path(authority_dir)
        private_socket = paths["private_socket_dir"]
        existed = private_socket.exists()
        private_socket.mkdir(mode=0o700, parents=True, exist_ok=True)
        private_stat = private_socket.stat()
        if not stat.S_ISDIR(private_stat.st_mode):
            raise ConfigError("private_socket_dir is not a directory")
        if (
            private_stat.st_uid != os.geteuid()
            or stat.S_IMODE(private_stat.st_mode) != 0o700
        ):
            if not existed:
                private_socket.chmod(0o700)
            else:
                raise ConfigError("private_socket_dir must be owned by this user and mode 0700")
        return cls(**paths, **strings, **integers)

    @property
    def lock_file(self) -> Path:
        return self.writer_data_dir / ".pagestore-branch-prepare.lock"

    @property
    def materializer_lock_file(self) -> Path:
        # Share the supervisor's ownership fence.  The branch operation itself
        # controls pause/resume and must not race a supervisor restartpoint.
        return self.materializer_data_dir / ".pagestore-materializer-supervisor.lock"

    @property
    def retention_authority_lock_file(self) -> Path:
        return self.retention_authority_dir / f"retention-owner-{self.retention_owner_id}.lock"

    @property
    def retention_authority_file(self) -> Path:
        return self.retention_authority_dir / f"retention-owner-{self.retention_owner_id}.json"

    @property
    def branch_retention_generation_file(self) -> Path:
        return self.retention_authority_dir / (
            f"branch-retention-generation-{self.retention_owner_id}.json"
        )

    @property
    def receipt_file(self) -> Path:
        return self.prepared_dir / "pagestore_branch.prepare.json"

    @property
    def prepared_lock_file(self) -> Path:
        return self.prepared_dir / ".pagestore-branch-prepare.lock"


class OwnerLock:
    def __init__(self, path: Path, owner_name: str) -> None:
        self.path = path
        self.owner_name = owner_name
        self.fd = -1

    def acquire(self) -> None:
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(self.fd)
            self.fd = -1
            raise OwnershipError(f"another {self.owner_name} owns {self.path}") from error
        payload = f"pid={os.getpid()}\noperation=branch_prepare\n".encode()
        os.ftruncate(self.fd, 0)
        os.lseek(self.fd, 0, os.SEEK_SET)
        os.write(self.fd, payload)
        os.fsync(self.fd)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self) -> OwnerLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class BranchPreparer:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.pause_owned = False
        self.writer_owned = False
        self.restricted_writer_running = False
        self.writer_extension_schema: str | None = None
        self.materializer_extension_schema: str | None = None
        self.branch_retention_generation: int | None = None
        self.branch_retention_owned = False
        self.journal: dict[str, Any] | None = None
        self.fault_probe = BranchFaultProbe(
            scope_paths=(
                config.writer_data_dir,
                config.materializer_data_dir,
                config.prepared_dir,
                config.retention_authority_dir,
            )
        )

    def config_identity(self) -> dict[str, Any]:
        return {
            "new_timeline": self.config.new_timeline,
            "parent_timeline": self.config.parent_timeline,
            "new_incarnation": self.config.new_incarnation,
            "writer_data_dir": str(self.config.writer_data_dir),
            "materializer_data_dir": str(self.config.materializer_data_dir),
            "prepared_dir": str(self.config.prepared_dir),
            "retention_authority_dir": str(self.config.retention_authority_dir),
            "retention_owner_id": self.config.retention_owner_id,
        }

    @staticmethod
    def journal_crc(value: dict[str, Any]) -> str:
        payload = {key: item for key, item in value.items() if key != "crc32"}
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return f"{zlib.crc32(encoded) & 0xffffffff:08x}"

    def new_journal(self) -> dict[str, Any]:
        return {
            "schema": RECEIPT_SCHEMA,
            "operation": JOURNAL_OPERATION,
            "identity": self.config_identity(),
            "state": "started",
            "intent": "preflight",
            "base_lsn": None,
            "checkpoint_redo_lsn": None,
            "checkpoint_end_lsn": None,
            "switch_lsn": None,
            "archived_through_lsn": None,
            "fork_lsn": None,
            "seeded_slru_pages": None,
            "retention_generation": None,
            "pause_owned": False,
            "writer_owned": False,
            "retention_owned": False,
            "restricted_writer_running": False,
            "materializer_resumed": False,
            "writer_restored": False,
            "prepared_dir": str(self.config.prepared_dir),
        }

    def read_journal(self) -> dict[str, Any] | None:
        path = self.config.receipt_file
        try:
            file_stat = path.lstat()
            if not stat.S_ISREG(file_stat.st_mode) or path.is_symlink():
                raise BranchPrepareError("branch journal is not a regular file")
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise BranchPrepareError("branch journal is unreadable or corrupt") from error
        if not isinstance(value, dict):
            raise BranchPrepareError("branch journal must be a JSON object")
        if value.get("schema") == LEGACY_RECEIPT_SCHEMA:
            if value.get("state") == "complete":
                if (
                    value.get("new_timeline") != self.config.new_timeline
                    or value.get("parent_timeline") != self.config.parent_timeline
                    or value.get("prepared_dir") != str(self.config.prepared_dir)
                ):
                    raise BranchPrepareError(
                        "legacy complete branch receipt identity mismatch"
                    )
                return value
            raise BranchPrepareError(
                "legacy incomplete branch receipt is unsupported; refusing recovery"
            )
        if value.get("schema") != RECEIPT_SCHEMA or set(value) != JOURNAL_KEYS:
            raise BranchPrepareError("branch journal schema or fields are invalid")
        if value.get("operation") != JOURNAL_OPERATION:
            raise BranchPrepareError("branch journal operation is invalid")
        if value.get("identity") != self.config_identity():
            raise BranchPrepareError("branch journal config identity mismatch")
        if value.get("state") not in JOURNAL_STATES:
            raise BranchPrepareError("branch journal state is unknown")
        if value.get("intent") is not None and not isinstance(value.get("intent"), str):
            raise BranchPrepareError("branch journal intent is invalid")
        for field in (
            "pause_owned", "writer_owned", "restricted_writer_running",
            "retention_owned", "materializer_resumed", "writer_restored",
        ):
            if not isinstance(value[field], bool):
                raise BranchPrepareError(f"branch journal {field} is invalid")
        if value["crc32"] != self.journal_crc(value):
            raise BranchPrepareError("branch journal CRC mismatch")
        return value

    def write_journal(self, journal: dict[str, Any] | None = None) -> None:
        value = dict(journal or self.journal or self.new_journal())
        value.pop("crc32", None)
        value["crc32"] = self.journal_crc(value)
        atomic_write_json(self.config.receipt_file, value)
        self.journal = value

    def journal_update(self, state: str | None = None, intent: str | None = None, **values: Any) -> None:
        if self.journal is None:
            self.journal = self.new_journal()
        updated = dict(self.journal)
        if state is not None:
            updated["state"] = state
        updated["intent"] = intent
        updated.update(values)
        self.write_journal(updated)

    def fault(self, name: str) -> None:
        self.fault_probe.probe(name)

    def restore_ownership_from_journal(self) -> None:
        if self.journal is None:
            raise BranchPrepareError("branch journal is not loaded")
        self.branch_retention_generation = self.journal["retention_generation"]
        self.branch_retention_owned = self.journal["retention_owned"]
        self.pause_owned = self.journal["pause_owned"]
        self.writer_owned = self.journal["writer_owned"]
        self.restricted_writer_running = self.journal["restricted_writer_running"]

    def observe_recovery_ownership(self) -> str:
        """Replace ambiguous journal booleans with observations of live services."""
        if self.journal is None:
            raise BranchPrepareError("branch journal is not loaded")
        pause_state = self.observe_materializer_pause()
        if pause_state not in {"paused", "not paused"}:
            raise BranchPrepareError(f"unknown materializer pause state {pause_state}")
        self.pause_owned = pause_state == "paused"
        mode = self.observe_writer_mode()
        if mode == "unknown":
            raise BranchPrepareError("cannot identify the surviving writer process")
        self.restricted_writer_running = mode == "restricted"
        self.writer_owned = mode in {"restricted", "stopped"}
        # A generation is a scoped, idempotent owner identity.  Treat its
        # presence as a cleanup candidate even when an intent update was lost;
        # do not trust a stale false ownership bit.
        self.branch_retention_generation = self.journal["retention_generation"]
        self.branch_retention_owned = self.branch_retention_generation is not None
        return mode

    def restore_ambiguous_services(self, mode: str) -> None:
        """Restore observed services before rejecting an unsafe early journal."""
        errors: list[str] = []
        if self.branch_retention_owned:
            try:
                self.release_branch_retention()
            except Exception as error:
                errors.append(f"could not release branch-base retention: {error}")
        try:
            pause_state = self.observe_materializer_pause()
            if pause_state == "paused":
                self.pause_owned = True
                self.resume_materializer()
            elif pause_state != "not paused":
                errors.append(f"unknown materializer pause state {pause_state}")
        except Exception as error:
            errors.append(f"could not restore materializer: {error}")
        if mode == "restricted":
            self.writer_owned = True
            try:
                self.restore_writer()
            except Exception as error:
                errors.append(f"could not restore normal writer: {error}")
        elif mode == "stopped" and self.journal is not None and self.journal["state"] not in {
            "started", "preflight_complete"
        }:
            # The journal proves this operation stopped the writer after the
            # initial preflight.  Starting the configured normal writer is the
            # only safe restoration; no checkpoint is selected here.
            self.writer_owned = True
            try:
                self.restore_writer()
            except Exception as error:
                errors.append(f"could not restore stopped writer: {error}")
        if errors:
            raise BranchPrepareError("; ".join(errors))

    def command(self, command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                encoding="utf-8",
                timeout=self.config.command_timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise BranchPrepareError(f"command failed: {command[0]}: {error}") from error
        if check and result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise BranchPrepareError(
                f"command failed ({result.returncode}): {' '.join(command)}"
                + (f": {detail}" if detail else "")
            )
        return result

    def pg_ctl(
        self, data_dir: Path, *arguments: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return self.command(
            [str(self.config.pg_ctl), "-D", str(data_dir), *arguments], check=check
        )

    def psql(self, host: str, port: int, sql: str) -> str:
        result = self.command(
            [
                str(self.config.psql),
                "-X",
                "-h",
                host,
                "-p",
                str(port),
                "-U",
                self.config.user,
                "-d",
                self.config.database,
                "-tA",
                "-F",
                "|",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
            ]
        )
        return result.stdout.strip()

    def writer_sql(self, sql: str, private: bool = False) -> str:
        host = str(self.config.private_socket_dir) if private else self.config.writer_host
        port = self.config.private_port if private else self.config.writer_port
        return self.psql(host, port, sql)

    def materializer_sql(self, sql: str) -> str:
        return self.psql(self.config.materializer_host, self.config.materializer_port, sql)

    def server_running(self, data_dir: Path) -> bool:
        return self.pg_ctl(data_dir, "status", check=False).returncode == 0

    def extension_schema(self, query: Any, role: str) -> str:
        try:
            schema = last_output_line(
                query(
                    "SELECT n.nspname FROM pg_extension e "
                    "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    "WHERE e.extname = 'pagestore'"
                )
            )
        except BranchPrepareError as error:
            raise BranchPrepareError(
                f"{role} does not have the pagestore extension installed"
            ) from error
        return sql_identifier(schema)

    def validate_retention_authority_identity(self) -> int:
        try:
            authority = json.loads(
                self.config.retention_authority_file.read_text(encoding="utf-8")
            )
            authority_generation = authority["retention_generation"]
            authority_data_dir = authority["consumer_data_dir"]
            authority_stat = self.config.materializer_data_dir.stat()
            namespace_stat = self.config.retention_authority_dir.stat()
            if (
                not isinstance(authority_generation, int)
                or isinstance(authority_generation, bool)
                or authority_generation <= 0
                or authority_data_dir != str(self.config.materializer_data_dir)
                or authority.get("consumer_data_dev") != authority_stat.st_dev
                or authority.get("consumer_data_ino") != authority_stat.st_ino
                or authority.get("authority_namespace_dev") != namespace_stat.st_dev
                or authority.get("authority_namespace_ino") != namespace_stat.st_ino
            ):
                raise ValueError("authority identity mismatch")
            return authority_generation
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise BranchPrepareError(
                "materializer retention authority does not match the configured consumer"
            ) from error

    def observe_materializer_pause(self) -> str:
        return last_output_line(
            self.materializer_sql("SELECT pg_get_wal_replay_pause_state()")
        )

    def observe_writer_mode(self) -> str:
        if not self.server_running(self.config.writer_data_dir):
            return "stopped"
        try:
            if last_output_line(
                self.writer_sql(
                    "SELECT NOT pg_is_in_recovery()"
                    " AND current_setting('listen_addresses') <> ''"
                )
            ) == "t":
                return "normal"
        except BranchPrepareError:
            pass
        try:
            if last_output_line(
                self.writer_sql(
                    "SELECT NOT pg_is_in_recovery()"
                    " AND current_setting('listen_addresses') = ''"
                    " AND current_setting('unix_socket_directories') = "
                    + sql_literal(str(self.config.private_socket_dir)),
                    private=True,
                )
            ) == "t":
                return "restricted"
        except BranchPrepareError:
            pass
        return "unknown"

    def validate_recovery_materializer(self, authority_generation: int) -> None:
        result = last_output_line(
            self.materializer_sql(
                "SELECT pg_is_in_recovery()"
                " AND current_setting('pagestore.backend') = 'localsvc'"
                " AND current_setting('pagestore.materializer')::boolean"
                " AND current_setting('pagestore.route_all')::boolean"
                " AND current_setting('pagestore.retention_owner_id') = "
                + sql_literal(str(self.config.retention_owner_id))
                + " AND current_setting('pagestore.retention_owner_generation')::bigint = "
                + str(authority_generation)
                + " AND current_setting('data_directory') = "
                + sql_literal(str(self.config.materializer_data_dir))
                + " AND current_setting('pagestore.timeline')::integer = "
                + str(self.config.parent_timeline)
                + " AND COALESCE(NULLIF(current_setting('pagestore.read_lsn'), ''),"
                " '0/0')::pg_lsn = '0/0'::pg_lsn"
            )
        )
        if result != "t":
            raise BranchPrepareError("materializer recovery identity check failed")

    def validate_recovery_writer(self, private: bool) -> None:
        result = last_output_line(
            self.writer_sql(
                "SELECT NOT pg_is_in_recovery()"
                " AND current_setting('pagestore.backend') = 'localsvc'"
                " AND current_setting('pagestore.timeline')::integer = "
                + str(self.config.parent_timeline)
                + " AND COALESCE(NULLIF(current_setting('pagestore.read_lsn'), ''),"
                " '0/0')::pg_lsn = '0/0'::pg_lsn",
                private=private,
            )
        )
        if result != "t":
            raise BranchPrepareError("writer recovery identity check failed")

    def discover_recovery_services(self) -> None:
        """Discover roles from their surviving sockets, not from pre-crash flags."""
        authority_generation = self.validate_retention_authority_identity()
        self.materializer_extension_schema = self.extension_schema(
            self.materializer_sql, "materializer"
        )
        self.validate_recovery_materializer(authority_generation)
        mode = self.observe_writer_mode()
        if (
            mode == "stopped"
            and self.journal is not None
            and self.journal["state"] in {
                "fork_captured", "branch_prepared", "prepared",
                "materializer_resumed", "writer_restored",
            }
            and self.journal["restricted_writer_running"]
        ):
            # The controller may have died after the intent was recorded but
            # before the restricted postmaster survived.  Recreate exactly
            # that persisted service configuration; never choose a boundary.
            self.writer_owned = True
            self.start_restricted_writer()
            mode = "restricted"
        if mode == "restricted":
            self.writer_extension_schema = self.extension_schema(
                lambda sql: self.writer_sql(sql, private=True), "writer"
            )
            self.validate_recovery_writer(private=True)
        elif mode == "normal":
            self.writer_extension_schema = self.extension_schema(self.writer_sql, "writer")
            self.validate_recovery_writer(private=False)
        elif mode != "stopped":
            raise BranchPrepareError("writer role is neither normal, restricted, nor stopped")

    @staticmethod
    def extension_function(schema: str | None, signature: str) -> str:
        if schema is None:
            raise BranchPrepareError("pagestore extension schema is not initialized")
        return f"{schema}.{signature}"

    def preflight(self) -> None:
        if not self.server_running(self.config.writer_data_dir):
            raise BranchPrepareError("writer is not running")
        if not self.server_running(self.config.materializer_data_dir):
            raise BranchPrepareError("materializer is not running")
        authority_generation = self.validate_retention_authority_identity()
        self.writer_extension_schema = self.extension_schema(self.writer_sql, "writer")
        self.materializer_extension_schema = self.extension_schema(
            self.materializer_sql, "materializer"
        )
        prepare_signature = self.extension_function(
            self.writer_extension_schema,
            "pagestore_prepare_branch_from_control(text,integer,integer,pg_lsn,pg_lsn,pg_lsn,bigint)",
        )
        checkpoint_signature = self.extension_function(
            self.writer_extension_schema, "pagestore_branch_checkpoint()"
        )
        capture_signature = self.extension_function(
            self.materializer_extension_schema, "pagestore_capture_slru_snapshot()"
        )
        retention_set_signature = self.extension_function(
            self.materializer_extension_schema,
            "pagestore_retention_set(integer,integer,bigint,bigint,integer,pg_lsn)",
        )
        retention_drop_signature = self.extension_function(
            self.materializer_extension_schema,
            "pagestore_retention_drop(integer,integer,bigint,bigint)",
        )
        writer_ok = last_output_line(
            self.writer_sql(
                "SELECT NOT pg_is_in_recovery()"
                " AND current_setting('pagestore.backend') = 'localsvc'"
                " AND current_setting('pagestore.timeline')::integer = "
                f"{self.config.parent_timeline}"
                " AND COALESCE(NULLIF(current_setting('pagestore.read_lsn'), ''),"
                " '0/0')::pg_lsn = '0/0'::pg_lsn"
                " AND current_setting('archive_mode') IN ('on', 'always')"
                " AND current_setting('archive_library') = 'pagestore'"
                " AND to_regprocedure("
                + sql_literal(prepare_signature)
                + ") IS NOT NULL"
                " AND to_regprocedure("
                + sql_literal(checkpoint_signature)
                + ") IS NOT NULL"
                " AND NOT EXISTS (SELECT 1 FROM pg_tablespace"
                " WHERE spcname NOT IN ('pg_default', 'pg_global'))"
            )
        )
        if writer_ok != "t":
            raise BranchPrepareError("writer failed the pagestore branch-source health check")
        materializer_ok = last_output_line(
            self.materializer_sql(
                "SELECT pg_is_in_recovery()"
                " AND current_setting('pagestore.backend') = 'localsvc'"
                " AND current_setting('pagestore.materializer')::boolean"
                " AND current_setting('pagestore.route_all')::boolean"
                " AND current_setting('pagestore.retention_owner_id') = '"
                + str(self.config.retention_owner_id) + "'"
                " AND current_setting('pagestore.retention_owner_generation')::bigint = "
                + str(authority_generation)
                + " AND current_setting('data_directory') = "
                + sql_literal(str(self.config.materializer_data_dir))
                + " AND current_setting('pagestore.timeline')::integer = "
                f"{self.config.parent_timeline}"
                " AND COALESCE(NULLIF(current_setting('pagestore.read_lsn'), ''),"
                " '0/0')::pg_lsn = '0/0'::pg_lsn"
                " AND to_regprocedure("
                + sql_literal(capture_signature)
                + ") IS NOT NULL"
                " AND to_regprocedure("
                + sql_literal(retention_set_signature)
                + ") IS NOT NULL"
                " AND to_regprocedure("
                + sql_literal(retention_drop_signature)
                + ") IS NOT NULL"
            )
        )
        if materializer_ok != "t":
            raise BranchPrepareError("materializer failed the recovery-role health check")
        self.reserve_branch_retention_generation()
        pause_state = last_output_line(
            self.materializer_sql("SELECT pg_get_wal_replay_pause_state()")
        )
        if pause_state != "not paused":
            raise BranchPrepareError(
                f"materializer pause is already externally owned ({pause_state})"
            )

    def wait_until(self, description: str, predicate: Any) -> None:
        deadline = time.monotonic() + self.config.progress_timeout_ms / 1000
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
                last_error = None
            except CancelledError:
                raise
            except (BranchPrepareError, ValueError) as error:
                last_error = error
            time.sleep(self.config.poll_interval_ms / 1000)
        detail = f": {last_error}" if last_error is not None else ""
        raise BranchPrepareError(f"timed out waiting for {description}{detail}")

    def resume_materializer(self) -> None:
        if not self.pause_owned:
            return
        self.materializer_sql("SELECT pg_wal_replay_resume()")
        self.wait_until(
            "materializer replay resume",
            lambda: last_output_line(
                self.materializer_sql("SELECT pg_get_wal_replay_pause_state()")
            )
            == "not paused",
        )
        self.pause_owned = False

    def reserve_branch_retention_generation(self) -> None:
        path = self.config.branch_retention_generation_file
        generation = 0
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if (
                value.get("schema") != 1
                or value.get("retention_owner_id")
                != self.config.retention_owner_id
                or not isinstance(value.get("generation"), int)
                or isinstance(value.get("generation"), bool)
                or value["generation"] <= 0
            ):
                raise ValueError("branch retention generation identity mismatch")
            generation = value["generation"]
        except FileNotFoundError:
            pass
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise BranchPrepareError(
                "branch retention generation authority is unreadable"
            ) from error
        if generation >= (1 << 32) - 1:
            raise BranchPrepareError("branch retention generation exhausted")
        generation += 1
        atomic_write_json(
            path,
            {
                "schema": 1,
                "retention_owner_id": self.config.retention_owner_id,
                "generation": generation,
            },
        )
        self.branch_retention_generation = generation

    def install_branch_retention(self, base: str) -> None:
        if self.branch_retention_generation is None:
            raise BranchPrepareError("branch retention generation is unavailable")
        owner_id = self.config.retention_owner_id
        if owner_id > (1 << 63) - 1:
            owner_id -= 1 << 64
        status = last_output_line(
            self.materializer_sql(
                "SELECT "
                + self.extension_function(
                    self.materializer_extension_schema,
                    "pagestore_retention_set(",
                )
                + f"{self.config.parent_timeline}, 3, {owner_id}, "
                + f"{self.branch_retention_generation}, 6, "
                + sql_literal(base)
                + "::pg_lsn)"
            )
        )
        if status != "0":
            raise BranchPrepareError(
                f"could not install branch-base retention pin (status {status})"
            )
        self.branch_retention_owned = True

    def release_branch_retention(self) -> None:
        if not self.branch_retention_owned:
            return
        if self.branch_retention_generation is None:
            raise BranchPrepareError("branch retention generation is unavailable")
        owner_id = self.config.retention_owner_id
        if owner_id > (1 << 63) - 1:
            owner_id -= 1 << 64
        status = last_output_line(
            self.materializer_sql(
                "SELECT "
                + self.extension_function(
                    self.materializer_extension_schema,
                    "pagestore_retention_drop(",
                )
                + f"{self.config.parent_timeline}, 3, {owner_id}, "
                + f"{self.branch_retention_generation})"
            )
        )
        if status != "0":
            raise BranchPrepareError(
                f"could not release branch-base retention pin (status {status})"
            )
        self.branch_retention_owned = False

    def capture_and_pin_base(self) -> str:
        base = self.pause_and_capture(keep_paused=True)
        try:
            self.install_branch_retention(base)
        except BaseException:
            self.resume_materializer()
            raise
        self.resume_materializer()
        return base

    def pause_and_capture(self, keep_paused: bool) -> str:
        state = last_output_line(
            self.materializer_sql("SELECT pg_get_wal_replay_pause_state()")
        )
        if state != "not paused":
            raise BranchPrepareError(f"cannot own materializer pause from state {state}")
        self.pause_owned = True
        self.materializer_sql("SELECT pg_wal_replay_pause()")
        try:
            self.wait_until(
                "materializer replay pause",
                lambda: last_output_line(
                    self.materializer_sql("SELECT pg_get_wal_replay_pause_state()")
                )
                == "paused",
            )
            cutoff = last_output_line(
                self.materializer_sql(
                    "SELECT "
                    + self.extension_function(
                        self.materializer_extension_schema,
                        "pagestore_capture_slru_snapshot()",
                    )
                )
            )
            parse_lsn(cutoff)
            return cutoff
        finally:
            if not keep_paused:
                self.resume_materializer()

    def stop_writer(self) -> None:
        self.writer_owned = True
        self.pg_ctl(self.config.writer_data_dir, "-m", "fast", "-w", "stop")
        if self.server_running(self.config.writer_data_dir):
            raise BranchPrepareError("writer remained running after fast stop")

    def start_restricted_writer(self) -> None:
        options = " ".join(
            (
                "-c listen_addresses=",
                f"-c unix_socket_directories={self.config.private_socket_dir}",
                "-c unix_socket_permissions=0700",
                f"-c port={self.config.private_port}",
                "-c autovacuum=off",
                "-c max_logical_replication_workers=0",
                "-c max_wal_senders=0",
                "-c pagestore.auto_reader_artifacts=off",
                "-c pagestore.auto_wal_index=off",
            )
        )
        self.pg_ctl(
            self.config.writer_data_dir,
            "-l",
            str(self.config.writer_log_file),
            "-o",
            options,
            "-w",
            "start",
        )
        self.restricted_writer_running = True
        healthy = last_output_line(
            self.writer_sql(
                "SELECT NOT pg_is_in_recovery()"
                " AND current_setting('listen_addresses') = ''"
                " AND current_setting('unix_socket_directories') = "
                + sql_literal(str(self.config.private_socket_dir)),
                private=True,
            )
        )
        if healthy != "t":
            raise BranchPrepareError("restricted writer failed its isolation health check")

    def select_checkpoint(self) -> tuple[str, str]:
        # The fast stop that established writer ownership already completed a
        # shutdown checkpoint after draining every public client.  The server
        # API verifies its exact store image/fence and resolves the checkpoint
        # record end from WAL; the current insert/flush point may be newer.
        output = last_output_line(
            self.writer_sql(
                "SELECT * FROM "
                + self.extension_function(
                    self.writer_extension_schema, "pagestore_branch_checkpoint()"
                ),
                private=True,
            )
        )
        fields = output.split("|")
        if len(fields) != 2:
            raise BranchPrepareError(f"unexpected checkpoint result: {output}")
        redo, end = fields
        if parse_lsn(redo) > parse_lsn(end):
            raise BranchPrepareError("checkpoint redo follows its flushed record end")
        return redo, end

    def archive_checkpoint(self) -> str:
        output = last_output_line(
            self.writer_sql(
                "SELECT switch_lsn, pg_walfile_name(switch_lsn - 1)"
                " FROM (SELECT pg_switch_wal() AS switch_lsn) switched",
                private=True,
            )
        )
        fields = output.split("|")
        if len(fields) != 2 or not WAL_FILE_NAME.fullmatch(fields[1]):
            raise BranchPrepareError(f"unexpected WAL switch result: {output}")
        switch_lsn, wal_file = fields
        parse_lsn(switch_lsn)
        archive_done = (
            self.config.writer_data_dir
            / "pg_wal"
            / "archive_status"
            / f"{wal_file}.done"
        )
        self.wait_until(f"archive completion for {wal_file}", archive_done.is_file)
        self.wait_until(
            f"durable pagestore WAL through {switch_lsn}",
            lambda: last_output_line(
                self.writer_sql(
                    "SELECT "
                    + self.extension_function(
                        self.writer_extension_schema, "pagestore_shipped_wal_lsn()"
                    )
                    + " >= "
                    + sql_literal(switch_lsn)
                    + "::pg_lsn",
                    private=True,
                )
            )
            == "t",
        )
        return switch_lsn

    def wait_materializer(self, target: str) -> None:
        self.wait_until(
            f"materializer replay through {target}",
            lambda: last_output_line(
                self.materializer_sql(
                    "SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0'::pg_lsn) >= "
                    + sql_literal(target)
                    + "::pg_lsn"
                )
            )
            == "t",
        )

    def prepare_branch(self, base: str, redo: str, fork: str) -> int:
        output = last_output_line(
            self.writer_sql(
                "SET pagestore.redo_wal_from_store = on; "
                "SELECT "
                + self.extension_function(
                    self.writer_extension_schema,
                    "pagestore_prepare_branch_from_control(",
                )
                + sql_literal(str(self.config.prepared_dir))
                + f", {self.config.new_timeline}, {self.config.parent_timeline}, "
                + sql_literal(base)
                + "::pg_lsn, "
                + sql_literal(redo)
                + "::pg_lsn, "
                + sql_literal(fork)
                + "::pg_lsn, "
                + str(self.config.new_incarnation)
                + ")",
                private=True,
            )
        )
        try:
            seeded = int(output)
        except ValueError as error:
            raise BranchPrepareError(f"unexpected branch prepare result: {output}") from error
        if seeded < 0:
            raise BranchPrepareError("branch prepare returned a negative page count")
        return seeded

    def writer_is_normal(self) -> bool:
        if not self.server_running(self.config.writer_data_dir):
            return False
        try:
            return last_output_line(
                self.writer_sql("SELECT current_setting('listen_addresses') <> ''")
            ) == "t"
        except BranchPrepareError:
            return False

    def restore_writer(self) -> None:
        if not self.writer_owned:
            return
        if self.writer_is_normal():
            self.restricted_writer_running = False
            self.writer_owned = False
            return
        if self.server_running(self.config.writer_data_dir):
            self.pg_ctl(self.config.writer_data_dir, "-m", "fast", "-w", "stop")
        self.restricted_writer_running = False
        self.pg_ctl(
            self.config.writer_data_dir,
            "-l",
            str(self.config.writer_log_file),
            "-w",
            "start",
        )
        self.writer_owned = False

    def success_restore(self, run_faults: bool = True) -> None:
        """Complete the prepared boundary with journaled, ordered transitions."""
        # Keep existing test/subclass cleanup hooks compatible; production uses
        # the finer-grained path below so each crash point has a durable state.
        if type(self).restore_services is not BranchPreparer.restore_services:
            errors = self.restore_services()
            if errors:
                raise BranchPrepareError("; ".join(errors))
            self.journal_update(
                "complete", None, pause_owned=False, retention_owned=False,
                materializer_resumed=True, writer_restored=True,
                writer_owned=False, restricted_writer_running=False,
            )
            return
        if self.branch_retention_owned:
            self.journal_update("prepared", "drop_temporary_pin")
            self.release_branch_retention()
            self.journal_update("prepared", None, retention_owned=False)
        if not self.journal["materializer_resumed"] or self.pause_owned:
            self.journal_update("prepared", "resume_materializer")
            state = last_output_line(
                self.materializer_sql("SELECT pg_get_wal_replay_pause_state()")
            )
            if state == "paused":
                self.resume_materializer()
            elif state != "not paused":
                raise BranchPrepareError(
                    f"cannot recover materializer from pause state {state}"
                )
            self.pause_owned = False
            self.journal_update(
                "materializer_resumed", None, pause_owned=False,
                materializer_resumed=True,
            )
        if run_faults:
            self.fault("branch_prepare.after_materializer_resume")
        self.journal_update("materializer_resumed", "restore_writer")
        self.restore_writer()
        self.journal_update(
            "writer_restored", None, writer_owned=False,
            restricted_writer_running=False, writer_restored=True,
        )
        if run_faults:
            self.fault("branch_prepare.after_writer_restore")
        self.journal_update(
            "complete", None, pause_owned=False, retention_owned=False,
            materializer_resumed=True, writer_restored=True,
            writer_owned=False, restricted_writer_running=False,
        )

    def recover_journal(self) -> dict[str, Any]:
        if self.journal is None:
            raise BranchPrepareError("branch journal is not loaded")
        state = self.journal["state"]
        if state == "complete":
            return dict(self.journal)
        self.discover_recovery_services()
        mode = self.observe_recovery_ownership()
        if state in {
            "started", "preflight_complete", "base_captured", "writer_stopped",
            "writer_restricted", "checkpoint_selected", "checkpoint_archived",
        }:
            try:
                self.restore_ambiguous_services(mode)
            finally:
                # Keep the journal as the durable proof of the abandoned
                # operation, including when restoration itself is successful.
                self.journal_update(self.journal["state"], "recovery_failed")
            raise BranchPrepareError(
                f"branch journal state {state!r} has no safe exact-boundary continuation; "
                "services were restored without selecting a new checkpoint"
            )
        if state not in {
            "fork_captured", "branch_prepared", "prepared",
            "materializer_resumed", "writer_restored",
        }:
            raise BranchPrepareError(
                f"branch journal state {state!r} has no safe idempotent recovery path"
            )
        if state == "fork_captured":
            if self.journal.get("intent") not in (None, "prepare_branch"):
                raise BranchPrepareError("branch journal has a contradictory prepare intent")
            seeded = self.prepare_branch(
                self.journal["base_lsn"],
                self.journal["checkpoint_redo_lsn"],
                self.journal["fork_lsn"],
            )
            self.journal_update(
                "branch_prepared", None, seeded_slru_pages=seeded,
                pause_owned=self.pause_owned, writer_owned=self.writer_owned,
                restricted_writer_running=self.restricted_writer_running,
            )
            state = "branch_prepared"
        if state == "branch_prepared":
            if self.journal.get("intent") not in (None, "publish_prepared_receipt"):
                raise BranchPrepareError("branch journal has a contradictory prepared intent")
            self.journal_update("prepared", None)
            state = "prepared"
        self.success_restore(run_faults=False)
        return dict(self.journal or {})

    def restore_services(self) -> list[str]:
        errors: list[str] = []
        if self.branch_retention_owned:
            try:
                self.release_branch_retention()
            except Exception as error:
                errors.append(f"could not release branch-base retention: {error}")
        if self.pause_owned:
            try:
                self.resume_materializer()
            except Exception as error:  # continue restoring the writer
                errors.append(f"could not resume materializer: {error}")
        if self.writer_owned:
            try:
                self.restore_writer()
            except Exception as error:
                errors.append(f"could not restore normal writer: {error}")
        return errors

    def execute(self) -> dict[str, Any]:
        existing = self.read_journal()
        if existing is not None:
            if existing.get("state") == "complete":
                return existing
            self.journal = existing
            self.restore_ownership_from_journal()
            return self.recover_journal()
        self.journal = self.new_journal()
        self.write_journal()
        failure: BaseException | None = None
        try:
            self.journal_update("started", "preflight")
            self.preflight()
            self.journal_update(
                "preflight_complete", None,
                retention_generation=self.branch_retention_generation,
            )
            self.journal_update("preflight_complete", "capture_base")
            base = self.capture_and_pin_base()
            self.journal_update(
                "base_captured", None, base_lsn=base,
                retention_generation=self.branch_retention_generation,
                retention_owned=self.branch_retention_owned,
                materializer_resumed=True,
            )
            self.journal_update("base_captured", "stop_writer")
            self.stop_writer()
            self.journal_update(
                "writer_stopped", None, writer_owned=self.writer_owned,
            )
            self.journal_update("writer_stopped", "start_restricted_writer")
            self.start_restricted_writer()
            self.journal_update(
                "writer_restricted", None,
                writer_owned=self.writer_owned,
                restricted_writer_running=self.restricted_writer_running,
            )
            self.journal_update("writer_restricted", "select_checkpoint")
            redo, checkpoint_end = self.select_checkpoint()
            if parse_lsn(base) > parse_lsn(redo):
                raise BranchPrepareError("proven SLRU base follows the selected checkpoint")
            self.journal_update(
                "checkpoint_selected", None,
                checkpoint_redo_lsn=redo, checkpoint_end_lsn=checkpoint_end,
            )
            self.journal_update("checkpoint_selected", "archive_checkpoint")
            switch_lsn = self.archive_checkpoint()
            if parse_lsn(checkpoint_end) > parse_lsn(switch_lsn):
                raise BranchPrepareError("WAL switch did not cover the selected checkpoint")
            self.journal_update("checkpoint_archived", None, switch_lsn=switch_lsn)
            self.journal_update("checkpoint_archived", "wait_materializer")
            self.wait_materializer(checkpoint_end)
            self.journal_update("checkpoint_archived", None)
            self.journal_update("checkpoint_archived", "capture_fork")
            fork = self.pause_and_capture(keep_paused=True)
            if parse_lsn(fork) < parse_lsn(checkpoint_end):
                raise BranchPrepareError("materialized fork does not cover the checkpoint")
            self.journal_update(
                "fork_captured", None, fork_lsn=fork, pause_owned=self.pause_owned,
            )
            self.journal_update("fork_captured", "prepare_branch")
            seeded = self.prepare_branch(base, redo, fork)
            self.journal_update(
                "branch_prepared", None, base_lsn=base,
                checkpoint_redo_lsn=redo, checkpoint_end_lsn=checkpoint_end,
                switch_lsn=switch_lsn, archived_through_lsn=switch_lsn,
                fork_lsn=fork, seeded_slru_pages=seeded,
                pause_owned=self.pause_owned, writer_owned=self.writer_owned,
                restricted_writer_running=self.restricted_writer_running,
                retention_owned=self.branch_retention_owned,
            )
            self.fault("branch_prepare.before_prepared_receipt")
            self.journal_update("branch_prepared", "publish_prepared_receipt")
            self.journal_update("prepared", None)
            self.fault("branch_prepare.after_prepared_receipt")
            self.success_restore()
        except BaseException as error:
            failure = error

        cleanup_errors = (
            [] if self.journal is not None and self.journal.get("state") == "complete"
            else self.restore_services()
        )
        if failure is not None:
            detail = f"; cleanup also failed: {'; '.join(cleanup_errors)}" if cleanup_errors else ""
            if isinstance(failure, (KeyboardInterrupt, CancelledError)):
                raise failure
            # Once prepare_branch returned, the journal is the recovery proof
            # and must survive even if best-effort cleanup happened to work.
            # Only an operation that never crossed the prepared boundary may
            # remove its temporary journal.
            prepared_boundary = self.journal is not None and self.journal.get(
                "state"
            ) in {
                "branch_prepared", "prepared", "materializer_resumed",
                "writer_restored", "complete",
            }
            if not cleanup_errors and not prepared_boundary:
                try:
                    self.config.receipt_file.unlink()
                except FileNotFoundError:
                    pass
            raise BranchPrepareError(f"{failure}{detail}") from failure
        if cleanup_errors:
            raise BranchPrepareError("; ".join(cleanup_errors))
        if self.journal is None or self.journal.get("state") != "complete":
            raise BranchPrepareError("branch journal did not reach complete state")
        return dict(self.journal)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        config = Config.load(args.config)
        if args.check_config:
            print("ok")
            return 0
        with OwnerLock(config.retention_authority_lock_file, "retention owner authority"):
            with OwnerLock(config.lock_file, "branch prepare"):
                with OwnerLock(config.prepared_lock_file, "prepared artifact directory"):
                    try:
                        with OwnerLock(
                            config.materializer_lock_file,
                            "materializer supervisor or branch prepare",
                        ):
                            preparer = BranchPreparer(config)

                            def cancel(signum: int, _frame: object) -> None:
                                raise CancelledError(f"received signal {signum}")

                            signal.signal(signal.SIGINT, cancel)
                            signal.signal(signal.SIGTERM, cancel)
                            receipt = preparer.execute()
                            print(json.dumps(receipt, sort_keys=True))
                            return 0
                    except OwnershipError as error:
                        raise OwnershipError(
                            f"{error}; stop the materializer supervisor before preparing a branch"
                        ) from error
    except OwnershipError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return EX_TEMPFAIL
    except ConfigError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return EX_CONFIG
    except (BranchPrepareError, OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
