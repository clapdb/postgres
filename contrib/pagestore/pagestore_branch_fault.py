#!/usr/bin/env python3
"""Small branch-controller adapter for the canonical pagestore fault contract."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from typing import Iterable


CRASH_EXIT = 88
FIELD_MAX = 128
_POINT = re.compile(
    r'^PAGESTORE_FAULT_POINT\([A-Z][A-Z0-9_]*,\s*"([a-z][a-z0-9_.-]*)",\s*'
    r'"([a-z][a-z0-9_-]*)",\s*"([a-z][a-z0-9_-]*)",\s*"([a-z| ]+)",\s*'
    r'([1-9][0-9]*),\s*([0-9]+)\)$'
)


class BranchFaultError(ValueError):
    pass


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class BranchFaultProbe:
    """A process-abort-only probe using the same env/control/report protocol."""

    def __init__(
        self,
        catalog: Path | None = None,
        scope_paths: Iterable[Path] | None = None,
    ) -> None:
        self.point: str | None = None
        self.action: str | None = None
        self.hit = 0
        self.target_hit = 0
        self.control: Path | None = None
        self.scenario: str | None = None
        self.seed: int | None = None
        self.operation: str | None = None
        self.catalog = catalog or Path(__file__).with_name("pagestore_fault_points.def")
        self.scope_paths = tuple(Path(path) for path in (scope_paths or ()))
        self._load()

    @staticmethod
    def _canonical_uint64(value: str, field: str) -> int:
        if not re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
            raise BranchFaultError(f"branch fault {field} must be canonical uint64")
        parsed = int(value, 10)
        if parsed > (1 << 64) - 1:
            raise BranchFaultError(f"branch fault {field} exceeds uint64")
        return parsed

    @staticmethod
    def _overlaps(first: Path, second: Path) -> bool:
        try:
            first = first.resolve(strict=True)
            second = second.resolve(strict=True)
        except OSError as error:
            raise BranchFaultError(f"branch fault scope is unreadable: {error}") from error
        return first == second or first in second.parents or second in first.parents

    @staticmethod
    def _validate_regular(path: Path, description: str) -> None:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = -1
        try:
            fd = os.open(path, flags)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise BranchFaultError(f"{description} must be a regular file")
        except OSError as error:
            raise BranchFaultError(f"{description} is not a regular file: {error}") from error
        finally:
            if fd >= 0:
                os.close(fd)

    @staticmethod
    def _field(name: str) -> str | None:
        return os.environ.get(name)

    def _load(self) -> None:
        names = (
            "PAGESTORE_TEST_FAULT_NAME", "PAGESTORE_TEST_FAULT_ACTION",
            "PAGESTORE_TEST_FAULT_HIT", "PAGESTORE_TEST_FAULT_DIR",
            "PAGESTORE_TEST_FAULT_SCENARIO", "PAGESTORE_TEST_FAULT_SEED",
            "PAGESTORE_TEST_FAULT_OPERATION", "PAGESTORE_TEST_FAULT_OPERATION_ID",
        )
        values = {name: self._field(name) for name in names}
        if not any(value is not None for value in values.values()):
            return
        name = values["PAGESTORE_TEST_FAULT_NAME"]
        action = values["PAGESTORE_TEST_FAULT_ACTION"]
        hit = values["PAGESTORE_TEST_FAULT_HIT"]
        control = values["PAGESTORE_TEST_FAULT_DIR"]
        if not name or action != "crash" or not hit or not control:
            raise BranchFaultError("incomplete branch fault configuration")
        catalog: dict[str, tuple[str, str, int, int]] = {}
        try:
            for number, raw in enumerate(self.catalog.read_text(encoding="utf-8").splitlines(), 1):
                line = raw.split("//", 1)[0].strip()
                if not line or line.startswith("/*") or line.startswith("*"):
                    continue
                match = _POINT.fullmatch(line)
                if match is None:
                    raise BranchFaultError(f"invalid fault catalog record at line {number}")
                point, target, model, actions, min_hit, max_hit = match.groups()
                catalog[point] = (target, model, int(min_hit), int(max_hit))
        except OSError as error:
            raise BranchFaultError(f"cannot read fault catalog: {error}") from error
        if name not in catalog:
            raise BranchFaultError(f"unknown branch fault {name}")
        target, model, min_hit, max_hit = catalog[name]
        if target != "branch" or model != "process_abort" or "crash" not in actions.split("|"):
            raise BranchFaultError(f"fault {name} is not a branch crash point")
        target_hit = self._canonical_uint64(hit, "hit")
        if target_hit < min_hit or (max_hit and target_hit > max_hit):
            raise BranchFaultError("branch fault hit is outside its catalog bounds")
        path = Path(control)
        if not path.is_absolute():
            raise BranchFaultError("branch fault control must be an absolute path")
        try:
            control_stat = path.lstat()
        except OSError as error:
            raise BranchFaultError(f"branch fault control is unreadable: {error}") from error
        if not stat.S_ISDIR(control_stat.st_mode) or stat.S_ISLNK(control_stat.st_mode):
            raise BranchFaultError("branch fault control must be a real directory")
        try:
            path = path.resolve(strict=True)
        except OSError as error:
            raise BranchFaultError(f"branch fault control is not a real directory: {error}") from error
        if any(self._overlaps(path, scope) for scope in self.scope_paths):
            raise BranchFaultError("branch fault control overlaps a protected store/config scope")
        arm = path / "arm"
        self._validate_regular(arm, "branch fault arm")
        if (path / "release").exists():
            raise BranchFaultError("branch fault control has an invalid arm/release state")
        if (path / "report.jsonl").exists() or (path / "report.tmp").exists():
            raise BranchFaultError("branch fault control already contains a report")
        operation = values["PAGESTORE_TEST_FAULT_OPERATION"]
        operation_id = values["PAGESTORE_TEST_FAULT_OPERATION_ID"]
        if operation and operation_id:
            raise BranchFaultError("branch fault operation and operation_id are exclusive")
        metadata = (
            values["PAGESTORE_TEST_FAULT_SCENARIO"], values["PAGESTORE_TEST_FAULT_SEED"],
            operation or operation_id,
        )
        if any(item is not None for item in metadata) and any(item is None for item in metadata):
            raise BranchFaultError("branch fault metadata is incomplete")
        if any(item is not None and (not item or len(item) > FIELD_MAX or "\x00" in item)
               for item in metadata):
            raise BranchFaultError("branch fault metadata is invalid")
        if any(item is not None and not re.fullmatch(r"[A-Za-z0-9_.:-]+", item)
               for item in metadata if item is not None and item != values["PAGESTORE_TEST_FAULT_SEED"]):
            raise BranchFaultError("branch fault metadata contains invalid identity characters")
        seed = None
        if values["PAGESTORE_TEST_FAULT_SEED"] is not None:
            seed = self._canonical_uint64(values["PAGESTORE_TEST_FAULT_SEED"], "seed")
        self.point = name
        self.action = action
        self.target_hit = target_hit
        self.control = path
        self.scenario, _, self.operation = metadata
        self.seed = seed

    def probe(self, name: str) -> None:
        if self.point != name or self.control is None:
            return
        self.hit += 1
        if self.hit != self.target_hit:
            return
        report: dict[str, object] = {
            "schema": 1,
            "name": name,
            "action": "crash",
            "hit": self.target_hit,
            "pid": os.getpid(),
        }
        if self.scenario is not None:
            report.update(
                scenario=self.scenario,
                seed=self.seed,
                operation=self.operation,
            )
        temporary = self.control / "report.tmp"
        final = self.control / "report.jsonl"
        fd = -1
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                fd = -1
                json.dump(report, stream, separators=(",", ":"), sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, final)
            _fsync_directory(self.control)
        except OSError as error:
            if fd >= 0:
                os.close(fd)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            raise BranchFaultError(f"cannot publish branch fault report: {error}") from error
        os._exit(CRASH_EXIT)
