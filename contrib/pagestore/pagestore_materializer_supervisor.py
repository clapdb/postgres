#!/usr/bin/env python3
"""Continuously supervise one local pagestore materializer compute."""

from __future__ import annotations

import argparse
import errno
import fcntl
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EX_TEMPFAIL = 75
EX_CONFIG = 78
CONFIG_SCHEMA = 2
STATUS_SCHEMA = 2
CONFIG_FIELDS = {
    "schema",
    "pg_ctl",
    "psql",
    "data_dir",
    "socket_dir",
    "port",
    "log_file",
    "state_dir",
    "retention_owner_id",
    "database",
    "user",
    "poll_interval_ms",
    "replay_idle_ms",
    "progress_timeout_ms",
    "retry_initial_ms",
    "retry_max_ms",
    "max_consecutive_failures",
    "command_timeout_seconds",
}
REQUIRED_CONFIG_FIELDS = {
    "schema",
    "pg_ctl",
    "psql",
    "data_dir",
    "socket_dir",
    "port",
    "log_file",
    "state_dir",
    "retention_owner_id",
}


class ConfigError(ValueError):
    pass


class OwnershipError(RuntimeError):
    pass


@dataclass(frozen=True)
class Config:
    pg_ctl: Path
    psql: Path
    data_dir: Path
    socket_dir: Path
    port: int
    log_file: Path
    state_dir: Path
    retention_owner_id: int
    database: str = "postgres"
    user: str = "postgres"
    poll_interval_ms: int = 1000
    replay_idle_ms: int = 3000
    progress_timeout_ms: int = 30000
    retry_initial_ms: int = 100
    retry_max_ms: int = 2000
    max_consecutive_failures: int = 5
    command_timeout_seconds: int = 15

    @classmethod
    def load(cls, path: Path) -> Config:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ConfigError(
                f"cannot read supervisor config {path}: {error}"
            ) from error
        if not isinstance(value, dict):
            raise ConfigError("supervisor config must be a JSON object")
        unknown = sorted(set(value) - CONFIG_FIELDS)
        missing = sorted(REQUIRED_CONFIG_FIELDS - set(value))
        if unknown:
            raise ConfigError(
                f"unknown supervisor config field(s): {', '.join(unknown)}"
            )
        if missing:
            raise ConfigError(
                f"missing supervisor config field(s): {', '.join(missing)}"
            )
        if (
            value.get("schema") != CONFIG_SCHEMA
            or isinstance(value.get("schema"), bool)
        ):
            raise ConfigError(
                f"supervisor config schema must be {CONFIG_SCHEMA}"
            )

        paths: dict[str, Path] = {}
        for field in (
            "pg_ctl",
            "psql",
            "data_dir",
            "socket_dir",
            "log_file",
            "state_dir",
        ):
            item = value[field]
            if not isinstance(item, str) or not item:
                raise ConfigError(f"supervisor config {field} must be a path string")
            resolved = Path(item)
            if not resolved.is_absolute():
                raise ConfigError(f"supervisor config {field} must be absolute")
            paths[field] = resolved

        for field in ("pg_ctl", "psql"):
            program = paths[field]
            if not program.is_file() or not os.access(program, os.X_OK):
                raise ConfigError(
                    f"supervisor config {field} is not executable: {program}"
                )
        if not (paths["data_dir"] / "PG_VERSION").is_file():
            raise ConfigError(
                f"materializer data directory is not provisioned: {paths['data_dir']}"
            )

        integers: dict[str, int] = {}
        defaults = {
            "port": None,
            "retention_owner_id": None,
            "poll_interval_ms": 1000,
            "replay_idle_ms": 3000,
            "progress_timeout_ms": 30000,
            "retry_initial_ms": 100,
            "retry_max_ms": 2000,
            "max_consecutive_failures": 5,
            "command_timeout_seconds": 15,
        }
        for field, default in defaults.items():
            item = value.get(field, default)
            if not isinstance(item, int) or isinstance(item, bool) or item <= 0:
                raise ConfigError(
                    f"supervisor config {field} must be a positive integer"
                )
            integers[field] = item
        if integers["port"] > 65535:
            raise ConfigError("supervisor config port exceeds 65535")
        if integers["retention_owner_id"] > (1 << 64) - 1:
            raise ConfigError("supervisor config retention_owner_id exceeds uint64")
        if integers["retry_initial_ms"] > integers["retry_max_ms"]:
            raise ConfigError("retry_initial_ms must not exceed retry_max_ms")

        strings: dict[str, str] = {}
        for field, default in (("database", "postgres"), ("user", "postgres")):
            item = value.get(field, default)
            if not isinstance(item, str) or not item:
                raise ConfigError(
                    f"supervisor config {field} must be a non-empty string"
                )
            strings[field] = item

        try:
            paths["socket_dir"].mkdir(parents=True, exist_ok=True)
            paths["state_dir"].mkdir(parents=True, exist_ok=True)
            paths["log_file"].parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ConfigError(
                f"could not prepare supervisor runtime directories: {error}"
            ) from error
        return cls(
            **paths,
            **integers,
            **strings,
        )

    @property
    def lock_file(self) -> Path:
        # Anchor ownership in PGDATA, not in an operator-selected status path,
        # so different configs cannot supervise the same worker concurrently.
        return self.data_dir / ".pagestore-materializer-supervisor.lock"

    @property
    def status_file(self) -> Path:
        return self.state_dir / "status.json"

    @property
    def retention_generation_file(self) -> Path:
        # Controller state must not be copied with a disposable PGDATA clone.
        return self.state_dir / f"retention-owner-{self.retention_owner_id}.json"

    @property
    def retention_authority_lock_file(self) -> Path:
        # Serialize generation allocation across replacement PGDATA trees.
        return self.state_dir / f"retention-owner-{self.retention_owner_id}.lock"


@dataclass(frozen=True)
class Progress:
    replay: str
    shipped: str
    materialized: str
    lag_bytes: int

    @property
    def replay_int(self) -> int:
        return parse_lsn(self.replay)

    @property
    def shipped_int(self) -> int:
        return parse_lsn(self.shipped)

    @property
    def materialized_int(self) -> int:
        return parse_lsn(self.materialized)

    def as_json(self) -> dict[str, Any]:
        return {
            "replay_lsn": self.replay,
            "shipped_wal_lsn": self.shipped,
            "materialized_wal_lsn": self.materialized,
            "lag_bytes": self.lag_bytes,
        }


def parse_lsn(value: str) -> int:
    try:
        high, low = value.split("/", 1)
        return (int(high, 16) << 32) | int(low, 16)
    except (ValueError, AttributeError) as error:
        raise ValueError(f"invalid PostgreSQL LSN {value!r}") from error


def retry_delay_ms(config: Config, failures: int) -> int:
    delay = config.retry_initial_ms
    for _ in range(max(0, failures - 1)):
        if delay >= config.retry_max_ms:
            return config.retry_max_ms
        delay = min(delay * 2, config.retry_max_ms)
    return delay


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
            try:
                os.fsync(directory_fd)
            except OSError as error:
                if error.errno not in (errno.EBADF, errno.EINVAL):
                    raise
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class OwnerLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd = -1

    def acquire(self) -> None:
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(self.fd)
            self.fd = -1
            raise OwnershipError(
                f"another materializer supervisor owns {self.path}"
            ) from error

    def publish_owner(self, pid: int, epoch: int) -> None:
        payload = f"pid={pid}\nepoch={epoch}\n".encode()
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


def previous_status(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


class Supervisor:
    def __init__(self, config: Config) -> None:
        self.config = config
        old = previous_status(config.status_file)
        old_epoch = old.get("owner_epoch", 0)
        old_generation = old.get("worker_generation", 0)
        old_retention_generation = old.get("retention_generation")
        if (
            not isinstance(old_epoch, int)
            or isinstance(old_epoch, bool)
            or old_epoch < 0
        ):
            old_epoch = 0
        if (
            not isinstance(old_generation, int)
            or isinstance(old_generation, bool)
            or old_generation < 0
        ):
            old_generation = 0
        authority_exists = config.retention_generation_file.exists()
        authority = previous_status(config.retention_generation_file)
        authority_generation = authority.get("retention_generation")
        if authority_exists:
            if (
                not isinstance(authority_generation, int)
                or isinstance(authority_generation, bool)
                or authority_generation < 1
                or authority_generation > (1 << 32) - 1
            ):
                raise OwnershipError(
                    "materializer retention generation authority is unreadable"
                )
            old_retention_generation = authority_generation
        elif old:
            # Upgrade from the status-only format is allowed only with an
            # intact positive generation; ambiguity must not reuse generation 1.
            if (
                not isinstance(old_retention_generation, int)
                or isinstance(old_retention_generation, bool)
                or old_retention_generation < 1
                or old_retention_generation > (1 << 32) - 1
            ):
                raise OwnershipError(
                    "materializer status lacks retention generation authority"
                )
            atomic_write_json(
                config.retention_generation_file,
                {"retention_generation": old_retention_generation},
            )
        else:
            # A genuinely new PGDATA has no prior worker or durable owner.
            old_retention_generation = 0
        self.owner_epoch = old_epoch + 1
        self.generation = old_generation
        self.retention_generation = old_retention_generation
        self.failures = 0
        self.last_error: str | None = None
        self.progress: Progress | None = None
        self.stop_requested = False
        self.last_replay: int | None = None
        self.replay_changed_at = time.monotonic()
        self.restart_baseline: int | None = None
        self.restart_deadline = 0.0
        self.retry_not_before = 0.0
        self.progress_api_ready = False
        self.progress_schema: str | None = None
        self.progress_api_wait_started = time.monotonic()
        self.lag_started_at: float | None = None

    def publish(self, state: str, **fields: Any) -> None:
        status = {
            "schema": STATUS_SCHEMA,
            "state": state,
            "owner_pid": os.getpid(),
            "owner_epoch": self.owner_epoch,
            "worker_generation": self.generation,
            "retention_owner_id": self.config.retention_owner_id,
            "retention_generation": self.retention_generation,
            "consecutive_failures": self.failures,
            "last_error": self.last_error,
            "updated_at": time.time(),
            "monotonic_ns": time.monotonic_ns(),
            "progress": self.progress.as_json() if self.progress else None,
            **fields,
        }
        atomic_write_json(self.config.status_file, status)

    def command(
        self, command: list[str], check: bool = False, timeout: int | None = None
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            check=check,
            capture_output=True,
            encoding="utf-8",
            timeout=self.config.command_timeout_seconds if timeout is None else timeout,
        )

    def pg_ctl(
        self, *arguments: str, check: bool = True, timeout: int | None = None
    ) -> subprocess.CompletedProcess[str]:
        return self.command(
            [str(self.config.pg_ctl), "-D", str(self.config.data_dir), *arguments],
            check=check, timeout=timeout,
        )

    def psql(self, sql: str) -> str:
        result = self.command(
            [
                str(self.config.psql),
                "-X",
                "-h",
                str(self.config.socket_dir),
                "-p",
                str(self.config.port),
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
            ],
            check=True,
        )
        return result.stdout.strip()

    def worker_running(self) -> bool:
        return self.pg_ctl("status", check=False).returncode == 0

    def worker_healthy(self) -> bool:
        try:
            expected_data_dir = str(self.config.data_dir).replace("'", "''")
            return (
                self.psql(
                    "SELECT pg_is_in_recovery() AND "
                    "current_setting('pagestore.materializer')::boolean AND "
                    "current_setting('pagestore.retention_owner_id') = '"
                    + str(self.config.retention_owner_id) + "' AND "
                    "current_setting('pagestore.retention_owner_generation')::bigint = "
                    + str(self.retention_generation) + " AND "
                    "current_setting('data_directory') = '" + expected_data_dir + "'"
                )
                == "t"
            )
        except (subprocess.SubprocessError, OSError):
            return False

    def read_progress(self) -> Progress | None:
        try:
            if not self.progress_api_ready:
                schema = self.psql(
                    "SELECT n.nspname FROM pg_extension e "
                    "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    "WHERE e.extname = 'pagestore'"
                )
                if not schema:
                    return None
                self.progress_schema = '"' + schema.replace('"', '""') + '"'
                regprocedure = (
                    self.progress_schema + ".pagestore_shipped_wal_lsn()"
                ).replace("'", "''")
                self.progress_api_ready = (
                    self.psql(
                        "SELECT to_regprocedure('" + regprocedure + "') "
                        "IS NOT NULL"
                    )
                    == "t"
                )
                if not self.progress_api_ready:
                    return None
            if self.progress_schema is None:
                return None
            output = self.psql(
                "SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0'::pg_lsn), " +
                self.progress_schema + ".pagestore_shipped_wal_lsn(), " +
                self.progress_schema + ".pagestore_materialized_wal_lsn(), " +
                self.progress_schema + ".pagestore_materializer_lag_bytes()"
            )
            replay, shipped, materialized, lag = output.split("|")
            progress = Progress(replay, shipped, materialized, int(lag))
            if progress.lag_bytes < 0:
                raise ValueError("materializer lag is negative")
            _ = (
                progress.replay_int,
                progress.shipped_int,
                progress.materialized_int,
            )
            return progress
        except (subprocess.SubprocessError, OSError, ValueError):
            self.progress_api_ready = False
            self.progress_schema = None
            return None

    def start_worker(self, reason: str) -> None:
        if self.retention_generation >= (1 << 32) - 1:
            raise RuntimeError("materializer retention generation exhausted")
        self.retention_generation += 1
        # Persist authority before the worker can register this generation.
        # Losing status.json after this point cannot cause generation reuse.
        atomic_write_json(
            self.config.retention_generation_file,
            {"retention_generation": self.retention_generation},
        )
        self.publish("starting", reason=reason)
        server_options = (
            "-c pagestore.retention_owner_id="
            + str(self.config.retention_owner_id)
            + " -c pagestore.retention_owner_generation="
            + str(self.retention_generation)
        )
        self.pg_ctl(
            "-l", str(self.config.log_file),
            "-o", server_options,
            "-w", "start",
        )
        self.generation += 1
        if not self.worker_healthy():
            raise RuntimeError("materializer failed its recovery-role health check")
        self.publish("running", reason=reason)

    def stop_worker(self, mode: str, reason: str) -> None:
        self.publish("stopping", mode=mode, reason=reason)
        self.pg_ctl(
            "-m", mode, "-w", "stop",
            timeout=max(self.config.command_timeout_seconds, 60),
        )

    def restart_for_progress(self) -> None:
        assert self.progress is not None
        baseline = self.progress.materialized_int
        self.publish("restartpoint", reason="replay stable with durable lag")
        self.stop_worker("fast", "publish materialized watermark")
        self.start_worker("restartpoint policy")
        self.restart_baseline = baseline
        self.restart_deadline = (
            time.monotonic() + self.config.progress_timeout_ms / 1000
        )

    def record_failure(self, error: BaseException | str) -> bool:
        self.failures += 1
        self.last_error = str(error)
        delay = retry_delay_ms(self.config, self.failures)
        self.retry_not_before = time.monotonic() + delay / 1000
        if self.failures >= self.config.max_consecutive_failures:
            self.publish("failed", retry_in_ms=None)
            return False
        self.publish("degraded", retry_in_ms=delay)
        return True

    def observe_progress(self, now: float) -> bool:
        progress = self.read_progress()
        if progress is None:
            self.progress = None
            if (
                now - self.progress_api_wait_started
                >= self.config.progress_timeout_ms / 1000
            ):
                if now < self.retry_not_before:
                    self.publish("waiting_for_progress_api")
                    return True
                self.progress_api_wait_started = now
                return self.record_failure(
                    "materializer progress API did not become available"
                )
            self.publish("waiting_for_progress_api")
            return True
        self.progress = progress
        self.progress_api_wait_started = now
        if self.last_error == "materializer progress API did not become available":
            self.failures = 0
            self.last_error = None

        if self.restart_baseline is not None:
            if progress.materialized_int > self.restart_baseline:
                self.restart_baseline = None
                self.lag_started_at = now
                self.failures = 0
                self.last_error = None
            elif now >= self.restart_deadline:
                self.restart_baseline = None
                self.lag_started_at = now
                self.replay_changed_at = now
                self.retry_not_before = (
                    now + self.config.replay_idle_ms / 1000
                )
                self.publish("awaiting_checkpoint")
                return True

        replay = progress.replay_int
        if replay != self.last_replay:
            self.last_replay = replay
            self.replay_changed_at = now

        if progress.lag_bytes == 0:
            self.lag_started_at = None
            self.failures = 0
            self.last_error = None
            self.publish("running")
            return True
        if progress.replay_int <= progress.materialized_int:
            if now - self.replay_changed_at >= self.config.progress_timeout_ms / 1000:
                if now < self.retry_not_before:
                    self.publish("catching_up")
                    return True
                self.replay_changed_at = now
                return self.record_failure(
                    "replay did not advance beyond the materialized watermark"
                )
            self.publish("catching_up")
            return True
        if self.lag_started_at is None:
            self.lag_started_at = now
        if self.restart_baseline is not None or now < self.retry_not_before:
            self.publish("awaiting_restartpoint")
            return True
        if (now - self.replay_changed_at >= self.config.replay_idle_ms / 1000 or
                now - self.lag_started_at >= self.config.replay_idle_ms / 1000):
            try:
                self.restart_for_progress()
            except (subprocess.SubprocessError, OSError, RuntimeError) as error:
                return self.record_failure(error)
        else:
            self.publish("replay_settling")
        return True

    def run(self, owner: OwnerLock) -> int:
        owner.publish_owner(os.getpid(), self.owner_epoch)
        self.publish("starting")
        worker_observed = False
        while not self.stop_requested:
            now = time.monotonic()
            try:
                running = self.worker_running()
            except (subprocess.SubprocessError, OSError) as error:
                if now >= self.retry_not_before and not self.record_failure(error):
                    return 1
                self.interruptible_wait(self.config.poll_interval_ms / 1000)
                continue

            if not running:
                if now >= self.retry_not_before:
                    try:
                        reason = (
                            "supervisor startup"
                            if self.generation == 0
                            else "replace stopped worker"
                        )
                        self.start_worker(reason)
                        worker_observed = True
                    except (subprocess.SubprocessError, OSError, RuntimeError) as error:
                        if not self.record_failure(error):
                            return 1
                else:
                    self.publish("degraded")
            elif not self.worker_healthy():
                if now >= self.retry_not_before:
                    try:
                        self.stop_worker("immediate", "failed health check")
                    except (subprocess.SubprocessError, OSError):
                        pass
                    if not self.record_failure("materializer health check failed"):
                        return 1
                else:
                    self.publish("degraded")
            else:
                if not worker_observed:
                    if self.generation == 0:
                        self.generation = 1
                    self.publish("running", reason="adopted")
                    worker_observed = True
                if not self.observe_progress(now):
                    return 1
            self.interruptible_wait(self.config.poll_interval_ms / 1000)
        self.publish("stopped", reason="supervisor signal")
        return 0

    def interruptible_wait(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stop_requested and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.05, remaining))


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
        with OwnerLock(config.retention_authority_lock_file):
            with OwnerLock(config.lock_file) as owner:
                supervisor = Supervisor(config)

                def request_stop(_signum: int, _frame: object) -> None:
                    supervisor.stop_requested = True

                signal.signal(signal.SIGINT, request_stop)
                signal.signal(signal.SIGTERM, request_stop)
                return supervisor.run(owner)
    except OwnershipError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return EX_TEMPFAIL
    except ConfigError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return EX_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
