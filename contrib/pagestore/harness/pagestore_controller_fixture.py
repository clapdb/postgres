#!/usr/bin/env python3
"""Persisted-format fixtures for the JSON artifacts of the branch controller
and the materializer supervisor.

``pagestore_branch_prepare`` persists its configuration, a crash journal
(``pagestore_branch.prepare.json``) and a retention generation authority;
``pagestore_materializer_supervisor`` its configuration, ``status.json`` and
the materializer's retention generation authority (``retention-owner-<id>.json``),
which the controller reads too.  Their layouts -- schema numbers, checksums,
key sets -- live in ``pagestore_artifact_schema.py``, which both tools import
and which prints the identity table this fixture pins.

``--capture`` packages the files real runs of the tools left behind
(``mvp_golden_test.sh`` and the managed materializer smoke of
``pagestore_harness.py`` write them into ``$PAGESTORE_CONTROLLER_FIXTURE_CAPTURE``)
next to the identity table.  ``--check`` fails when the compiled identities
differ from the fixture's (a schema change without a fixture update), loads
every artifact through the shared module's reader, and applies each declared
mutation to a copy, requiring the reader to refuse it -- or, for a legacy
layout the tools still read, to accept it.  Needs nothing but Python.
"""
from __future__ import annotations

import argparse
import datetime
import gzip
import json
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pagestore_artifact_schema as artifact_schema  # noqa: E402
from pagestore_artifact_schema import ArtifactError  # noqa: E402

FIXTURE_JSON = "fixture.json"
FORMAT_JSON = "format.json"
ARTIFACTS_TAR = "artifacts.tar.gz"

# fixture artifact -> (kind in pagestore_artifact_schema, path in the archive)
ARTIFACTS: dict[str, tuple[str, str]] = {
    "branch_config": ("branch_config", "controller/branch-prepare.json"),
    "branch_journal": ("branch_journal", "controller/pagestore_branch.prepare.json"),
    "branch_retention_generation": (
        "branch_retention_generation", "controller/branch-retention-generation-1.json"),
    "supervisor_config": ("supervisor_config", "supervisor/materializer-supervisor.json"),
    "supervisor_status": ("supervisor_status", "supervisor/status.json"),
    "retention_authority": ("retention_authority", "supervisor/retention-owner-1.json"),
}


class FixtureError(Exception):
    pass


def deterministic_tar(root: Path, output: Path) -> list[str]:
    names: list[str] = []
    with gzip.GzipFile(filename="", mode="wb", fileobj=output.open("wb"), mtime=0) as compressed, \
            tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for path in sorted(root.rglob("*")):
            relative = path.relative_to(root).as_posix()
            info = tar.gettarinfo(str(path), arcname=relative)
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o755 if path.is_dir() else 0o644
            info.pax_headers = {}
            if path.is_file():
                with path.open("rb") as handle:
                    tar.addfile(info, handle)
            else:
                tar.addfile(info)
            names.append(relative)
    return names


def extract(fixture: Path, target: Path) -> None:
    target.mkdir(parents=True)
    with tarfile.open(fixture / ARTIFACTS_TAR, "r:gz") as tar:
        tar.extractall(target, filter="data")


# ---- mutations -----------------------------------------------------------------

def edit(path: Path, change: Callable[[dict[str, Any]], None], restamp: bool = False) -> None:
    """Apply a change to the decoded object and write it back verbatim --
    with the checksum recomputed only when the mutation says so, since a
    stale checksum is itself what most mutations must be caught by."""
    value = json.loads(path.read_text(encoding="utf-8"))
    change(value)
    if restamp and "crc32" in value:
        value["crc32"] = artifact_schema.artifact_crc(value)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def bump_schema(value: dict[str, Any]) -> None:
    value["schema"] = int(value.get("schema") or 0) + 100


def set_schema(schema: int | None) -> Callable[[dict[str, Any]], None]:
    def change(value: dict[str, Any]) -> None:
        if schema is None:
            value.pop("schema", None)
        else:
            value["schema"] = schema
    return change


def drop(key: str) -> Callable[[dict[str, Any]], None]:
    def change(value: dict[str, Any]) -> None:
        value.pop(key, None)
    return change


def add_member(value: dict[str, Any]) -> None:
    value["unexpected_member"] = 1


def corrupt_crc(value: dict[str, Any]) -> None:
    value["crc32"] = "00000000" if value.get("crc32") != "00000000" else "ffffffff"


def truncate(path: Path) -> None:
    path.write_bytes(path.read_bytes()[:-3])


ACCEPTED = "accepted"
REJECTED = "rejected"

# name, artifact, mutation (a callable on the file path), expectation
MUTATIONS: list[dict[str, Any]] = []


def declare(name: str, artifact: str, expect: str, apply: Callable[[Path], None]) -> None:
    MUTATIONS.append({"name": name, "artifact": artifact, "expect": expect, "apply": apply})


for _artifact, (_kind, _) in ARTIFACTS.items():
    _spec = artifact_schema.ARTIFACTS[_kind]
    declare(f"{_artifact}.schema-unknown", _artifact, REJECTED,
            lambda p: edit(p, bump_schema, restamp=True))
    declare(f"{_artifact}.not-json", _artifact, REJECTED, truncate)
    if _spec.closed:
        declare(f"{_artifact}.unknown-member", _artifact, REJECTED,
                lambda p: edit(p, add_member, restamp=True))
    if _spec.carries_crc(_spec.schema):
        declare(f"{_artifact}.crc", _artifact, REJECTED, lambda p: edit(p, corrupt_crc))
        declare(f"{_artifact}.crc-missing", _artifact, REJECTED, lambda p: edit(p, drop("crc32")))
        # a member changed under a checksum left as it was
        declare(f"{_artifact}.edited", _artifact, REJECTED,
                lambda p: edit(p, lambda v: v.update({next(
                    k for k in ("retention_generation", "generation", "state") if k in v): 999})))
    for _legacy in sorted(_spec.accepted - {_spec.schema}, key=lambda s: -1 if s is None else s):
        # a legacy layout: the schema it names, without the checksum it
        # predates
        declare(f"{_artifact}.legacy-{'none' if _legacy is None else _legacy}", _artifact,
                ACCEPTED,
                lambda p, s=_legacy: edit(p, lambda v: (set_schema(s)(v), v.pop("crc32", None))))
    for _refused in sorted(_spec.refused):
        declare(f"{_artifact}.refused-{_refused}", _artifact, REJECTED,
                lambda p, s=_refused: edit(p, set_schema(s), restamp=True))


# ---- capture -------------------------------------------------------------------

def capture(args: argparse.Namespace) -> int:
    fixture: Path = args.capture
    source: Path = args.source
    missing = [rel for _, rel in ARTIFACTS.values() if not (source / rel).is_file()]
    if missing:
        raise FixtureError(f"{source} lacks {', '.join(missing)}")
    for name, (kind, rel) in ARTIFACTS.items():
        try:
            value = artifact_schema.load(kind, source / rel)
        except ArtifactError as error:
            raise FixtureError(f"{name} does not load: {error}") from error
        if value.get("schema") != artifact_schema.ARTIFACTS[kind].schema:
            raise FixtureError(f"{name} is not at the current schema")
    fixture.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pagestore-controller-capture-") as temp:
        staged = Path(temp) / "artifacts"
        for _, rel in ARTIFACTS.values():
            target = staged / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / rel, target)
        names = deterministic_tar(staged, fixture / ARTIFACTS_TAR)
    (fixture / FORMAT_JSON).write_text(
        json.dumps(artifact_schema.identities(), indent=2) + "\n", encoding="utf-8")
    previous: dict[str, Any] = {}
    if (fixture / FIXTURE_JSON).is_file():
        previous = json.loads((fixture / FIXTURE_JSON).read_text(encoding="utf-8"))
    metadata = {
        "schema": 1,
        "name": fixture.name,
        "role": "current",
        "captured": datetime.date.today().isoformat(),
        "description": previous.get(
            "description",
            "the JSON artifacts real runs of the branch controller and the materializer "
            "supervisor left behind: each tool's configuration, the controller's completed "
            "journal and retention generation authority, the supervisor's status and the "
            "materializer's retention generation authority"),
        "artifacts": {name: rel for name, (_, rel) in ARTIFACTS.items()},
        "files": names,
    }
    (fixture / FIXTURE_JSON).write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"captured {fixture} ({len(names)} entries)")
    return 0


# ---- check ---------------------------------------------------------------------

def fixture_metadata(fixture: Path) -> dict[str, Any]:
    metadata = json.loads((fixture / FIXTURE_JSON).read_text(encoding="utf-8"))
    if metadata.get("schema") != 1 or metadata.get("name") != fixture.name:
        raise FixtureError(f"{fixture / FIXTURE_JSON} does not describe {fixture.name}")
    role = metadata.get("role", "current")
    if role not in ("current", "legacy"):
        raise FixtureError(f"{fixture / FIXTURE_JSON} has an unknown role {role!r}")
    metadata["role"] = role
    return metadata


def load_outcome(kind: str, path: Path) -> tuple[str, str]:
    try:
        value = artifact_schema.load(kind, path)
    except (ArtifactError, OSError) as error:
        return REJECTED, str(error)
    schema = value.get("schema")
    return ACCEPTED, f"schema={'none' if schema is None else schema}"


def check_one(args: argparse.Namespace, fixture: Path) -> int:
    metadata = fixture_metadata(fixture)
    role = metadata["role"]
    print(f"--- fixture {fixture.name} ({role})")
    expected = json.loads((fixture / FORMAT_JSON).read_text(encoding="utf-8"))
    current = artifact_schema.identities()
    if role == "current":
        if current != expected:
            print("FAIL - the tools' persisted-format identities differ from the fixture:")
            for item in current:
                if item not in expected:
                    print(f"  new or changed: {item}")
            for item in expected:
                if item not in current:
                    print(f"  missing or changed: {item}")
            print(f"  a persisted-format change must add or update a fixture ({fixture})")
            return 1
        print("ok   - the tools' persisted-format identities match the fixture")
    elif current == expected:
        print("FAIL - a legacy fixture records the current identities; mark it current")
        return 1
    else:
        print("ok   - legacy fixture records superseded identities")
    failures = 0
    with tempfile.TemporaryDirectory(prefix="pagestore-controller-check-") as temp:
        root = Path(temp)
        artifacts = root / "artifacts"
        extract(fixture, artifacts)
        for name, (kind, rel) in ARTIFACTS.items():
            outcome, report = load_outcome(kind, artifacts / rel)
            spec = artifact_schema.ARTIFACTS[kind]
            if outcome != ACCEPTED:
                failures += 1
                print(f"FAIL - {name} does not load: {report}")
            elif role == "current" and report != f"schema={spec.schema}":
                failures += 1
                print(f"FAIL - {name} loads as {report}, the current layout is schema {spec.schema}")
            else:
                print(f"ok   - {name} loads: {report}")
        if role == "current":
            for case in MUTATIONS:
                if args.only and case["name"] not in args.only:
                    continue
                kind, rel = ARTIFACTS[case["artifact"]]
                copy = root / "mutations" / case["name"] / Path(rel).name
                copy.parent.mkdir(parents=True)
                shutil.copy2(artifacts / rel, copy)
                case["apply"](copy)
                outcome, report = load_outcome(kind, copy)
                if outcome == case["expect"]:
                    print(f"ok   - {case['name']}: {outcome}")
                else:
                    failures += 1
                    print(f"FAIL - {case['name']}: {outcome} ({report}), expected {case['expect']}")
        if failures and args.keep_failures:
            shutil.copytree(root, args.keep_failures, dirs_exist_ok=True)
    return 1 if failures else 0


def check(args: argparse.Namespace) -> int:
    status = 0
    for fixture in args.check:
        status |= check_one(args, fixture)
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--capture", type=Path, metavar="FIXTURE_DIR")
    group.add_argument("--check", type=Path, nargs="+", metavar="FIXTURE_DIR")
    parser.add_argument("--source", type=Path, metavar="CAPTURE_DIR",
                        help="the artifacts the golden test and the managed materializer "
                             "smoke captured")
    parser.add_argument("--only", nargs="*", help="run only these mutation cases")
    parser.add_argument("--keep-failures", type=Path, help="copy a failed check's tree here")
    args = parser.parse_args(argv)
    for name in ("source", "capture", "keep_failures"):
        if getattr(args, name) is not None:
            setattr(args, name, getattr(args, name).resolve())
    if args.capture is not None and args.source is None:
        parser.error("--capture needs --source")
    try:
        return capture(args) if args.capture else check(args)
    except FixtureError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
