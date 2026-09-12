#!/usr/bin/env python3
"""Persisted-format fixtures for the files a compute leaves in a data directory.

The page-store backend writes a prepared branch (``pagestore_branch.manifest``,
``pagestore_branch.bootstrap``), a prepared reader (``pagestore_reader.manifest``,
``pagestore_reader.snapshot``, ``pagestore_reader.catalog``) and two raw-value
markers (the reader's relation-map intent marker and the SLRU mirror's primed
marker) for another compute -- or itself, after a restart -- to load.  Their
identities live in ``pagestore_artifact_format.h`` and are reported by
``pagestore_format_versions`` as the ``pgdata`` family.

``--capture`` packages a set of those artifacts that a real backend produced
(``integration_test.sh`` writes them into ``$PAGESTORE_PGDATA_FIXTURE_CAPTURE``
together with the identity each loader binds them to) next to the compiled
identities.  ``--check`` fails when the compiled identities differ from the
fixture's (a format change without a fixture update), then -- given a
PostgreSQL build -- starts a scratch cluster with the extension loaded, has
``pagestore_pgdata_artifact_check()`` load every artifact through the very
loader a compute uses, and applies each declared mutation to a copy, requiring
the loader to refuse it.  A ``legacy`` fixture must only still load.
"""
from __future__ import annotations

import argparse
import datetime
import gzip
import json
import os
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pagestore_harness as harness  # noqa: E402
from pagestore_fixture import Daemon  # noqa: E402

FIXTURE_JSON = "fixture.json"
FORMAT_JSON = "format.json"
ARTIFACTS_TAR = "artifacts.tar.gz"
IDENTITY_JSON = "identity.json"
FAMILY = "pgdata"

# artifact -> (kind for pagestore_pgdata_artifact_check, relative path)
ARTIFACTS: dict[str, tuple[str, str]] = {
    "branch_manifest": ("branch_manifest", "branch/pagestore_branch.manifest"),
    "branch_bootstrap": ("branch_bootstrap", "branch/pagestore_branch.bootstrap"),
    "reader_manifest": ("reader_manifest", "reader/pagestore_reader.manifest"),
    "reader_snapshot": ("reader_snapshot", "reader/pagestore_reader.snapshot"),
    "reader_catalog": ("reader_catalog", "reader/pagestore_reader.catalog"),
    "reader_map_pending": ("reader_map_pending", "markers/.pagestore-reader-map-pending"),
    "slru_primed": ("slru_primed", "markers/pagestore.slru_mirror_primed"),
}
TRAILER_OFFSET = 8
FOREIGN_TRAILER = struct.pack("<II", 0x41424344, 7)


class FixtureError(Exception):
    pass


def format_identities(tool: Path) -> list[dict[str, Any]]:
    output = subprocess.run([str(tool)], check=True, capture_output=True, text=True).stdout
    return json.loads(output)


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


# ---- the identity each loader binds an artifact to ---------------------------

def identity_arguments(kind: str, identity: dict[str, Any]) -> tuple[int, int, str, str, str, str]:
    """(timeline, parent_timeline, lsn_a, lsn_b, lsn_c, system_identifier) for
    pagestore_pgdata_artifact_check, from the fixture's identity record."""
    zero = "0/0"
    if kind in ("branch_manifest", "branch_bootstrap"):
        branch = identity["branch"]
        return (int(branch["new_timeline"]), int(branch["parent_timeline"]),
                str(branch["checkpoint_redo"]), str(branch["recovery_lsn"]),
                str(branch["fork_lsn"]), "0")
    if kind in ("reader_manifest", "reader_snapshot", "reader_catalog"):
        reader = identity["reader"]
        return (int(reader["timeline"]), 0, str(reader["read_lsn"]), zero, zero,
                str(reader["system_identifier"]))
    return (0, 0, zero, zero, zero, "0")


def check_identity_record(identity: dict[str, Any]) -> None:
    try:
        branch, reader, markers = identity["branch"], identity["reader"], identity["markers"]
        for key in ("new_timeline", "parent_timeline", "checkpoint_redo", "recovery_lsn",
                    "fork_lsn"):
            branch[key]
        for key in ("timeline", "read_lsn", "system_identifier"):
            reader[key]
        for key in ("reader_map_pending_horizon", "slru_primed_stamp"):
            markers[key]
    except (KeyError, TypeError) as error:
        raise FixtureError(f"the fixture identity record is incomplete: {error}") from error


# ---- a scratch cluster with the extension loaded -----------------------------

class Cluster:
    """A throwaway cluster with the pagestore extension loaded against an
    empty page store of its own: the artifact loaders read files, but the
    extension binds its timeline to a daemon at startup."""

    def __init__(self, build: Path, root: Path):
        pgctl = next(build.joinpath("tmp_install").rglob("bin/pg_ctl"), None)
        if pgctl is None:
            raise FixtureError(f"no tmp_install under {build} (run: meson test -C {build} --suite setup)")
        self.bin = pgctl.parent
        self.root = root
        self.data = root / "pgdata"
        self.sock = root / "sock"
        self.log = root / "server.log"
        self.env = harness.private_environment()
        prefix = self.bin.parent
        self.env["LD_LIBRARY_PATH"] = f"{prefix / 'lib'}:{prefix / 'lib64'}"
        self.env["PGCTLTIMEOUT"] = os.environ.get("PGCTLTIMEOUT", "180")
        self.port = 5433
        tools = build / "contrib" / "pagestore"
        self.shm = f"/pspgdata_{os.getpid()}"
        self.daemon = Daemon(tools / "pagestore_daemon", tools / "pagestore_inspect",
                             root / "store", self.shm, root / "daemon.log")

    def start(self) -> None:
        self.sock.mkdir()
        (self.root / "store").mkdir()
        status = self.daemon.start()
        if status != "ready":
            raise FixtureError(f"the scratch page store did not start: {status}")
        subprocess.run([str(self.bin / "initdb"), "-D", str(self.data), "-U", "postgres",
                        "-A", "trust"], check=True, capture_output=True, env=self.env)
        with (self.data / "postgresql.conf").open("a", encoding="utf-8") as conf:
            conf.write(
                "shared_preload_libraries = 'pagestore'\n"
                "pagestore.backend = 'localsvc'\n"
                f"pagestore.localsvc_shm = '{self.shm}'\n"
                "listen_addresses = ''\n"
                f"unix_socket_directories = '{self.sock}'\n"
                f"port = {self.port}\n"
            )
        started = subprocess.run([str(self.bin / "pg_ctl"), "-D", str(self.data), "-l",
                                  str(self.log), "-w", "start"],
                                 capture_output=True, text=True, env=self.env, check=False)
        if started.returncode != 0:
            tail = self.log.read_text(encoding="utf-8", errors="replace").splitlines()[-5:] \
                if self.log.exists() else []
            raise FixtureError(f"the scratch cluster did not start: {started.stderr.strip()} {tail!r}")
        self.sql(
            "CREATE FUNCTION pagestore_pgdata_artifact_check(text, text, int, int, pg_lsn, "
            "pg_lsn, pg_lsn, text) RETURNS text AS 'pagestore', "
            "'pagestore_pgdata_artifact_check' LANGUAGE C STRICT;"
        )

    def stop(self) -> None:
        subprocess.run([str(self.bin / "pg_ctl"), "-D", str(self.data), "-m", "fast", "-w",
                        "stop"], check=False, capture_output=True, env=self.env)
        self.daemon.stop()

    def sql(self, statement: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.bin / "psql"), "-h", str(self.sock), "-p", str(self.port), "-U",
             "postgres", "-d", "postgres", "-tA", "-v", "ON_ERROR_STOP=1", "-c", statement],
            capture_output=True, text=True, env=self.env, check=False,
        )

    def check_artifact(self, kind: str, directory: Path,
                       identity: dict[str, Any]) -> tuple[bool, str]:
        timeline, parent, lsn_a, lsn_b, lsn_c, sysid = identity_arguments(kind, identity)
        result = self.sql(
            f"SELECT pagestore_pgdata_artifact_check('{kind}', '{directory}', {timeline}, "
            f"{parent}, '{lsn_a}', '{lsn_b}', '{lsn_c}', '{sysid}');"
        )
        if result.returncode != 0:
            return False, (result.stderr.strip().splitlines() or ["?"])[-1]
        return True, result.stdout.strip()


# ---- mutations -----------------------------------------------------------------

class Skip(Exception):
    """The mutation does not apply to this capture (a byte past its end)."""


def flip_byte(path: Path, offset: int) -> None:
    data = bytearray(path.read_bytes())
    if offset >= len(data):
        raise Skip(f"{path.name} is only {len(data)} bytes")
    data[offset] ^= 0x01
    path.write_bytes(bytes(data))


def truncate_tail(path: Path) -> None:
    data = path.read_bytes()
    path.write_bytes(data[:-1])


def foreign_trailer(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[TRAILER_OFFSET:TRAILER_OFFSET + 8] = FOREIGN_TRAILER
    path.write_bytes(bytes(data))


def legacy_marker(path: Path) -> None:
    path.write_bytes(path.read_bytes()[:TRAILER_OFFSET])


def zero_trailer(path: Path) -> None:
    """A full-size marker whose trailer is zero: not the legacy layout."""
    data = bytearray(path.read_bytes())
    data[TRAILER_OFFSET:TRAILER_OFFSET + 8] = bytes(8)
    path.write_bytes(bytes(data))


def overlong(path: Path) -> None:
    path.write_bytes(path.read_bytes() + b"\0")


def bump_manifest_format(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    marker = '"format": '
    start = text.index(marker) + len(marker)
    end = start
    while text[end].isdigit():
        end += 1
    path.write_text(text[:start] + str(int(text[start:end]) + 100) + text[end:], encoding="utf-8")


def edit_manifest_field(path: Path, key: str, value: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    for i, line in enumerate(lines):
        if line.lstrip().startswith(f'"{key}":'):
            comma = "," if line.rstrip().endswith(",") else ""
            lines[i] = f'  "{key}": {value}{comma}\n'
            break
    else:
        raise FixtureError(f"{path} has no {key} member")
    path.write_text("".join(lines), encoding="utf-8")


ACCEPTED = "accepted"
REJECTED = "rejected"
SKIPPED = "skipped"

# name, artifact, mutation, expectation.  A rejection is the loader raising.
MUTATIONS: list[dict[str, Any]] = [
    {"name": "bootstrap-header-byte", "artifact": "branch_bootstrap",
     "apply": lambda p: flip_byte(p, 0), "expect": REJECTED},
    {"name": "bootstrap-map-byte", "artifact": "branch_bootstrap",
     "apply": lambda p: flip_byte(p, 72), "expect": REJECTED},
    {"name": "bootstrap-truncated", "artifact": "branch_bootstrap",
     "apply": truncate_tail, "expect": REJECTED},
    {"name": "bootstrap-format-bumped", "artifact": "branch_bootstrap",
     "apply": lambda p: flip_byte(p, 44), "expect": REJECTED},
    {"name": "branch-manifest-format-unknown", "artifact": "branch_manifest",
     "apply": bump_manifest_format, "expect": REJECTED},
    {"name": "branch-manifest-edited", "artifact": "branch_manifest",
     "apply": lambda p: edit_manifest_field(p, "fork_lsn", '"0/1"'), "expect": REJECTED},
    # the bootstrap binds the manifest text by CRC: a manifest edit that the
    # manifest loader itself accepts must still fail the bootstrap
    {"name": "bootstrap-manifest-unbound", "artifact": "branch_bootstrap",
     "apply_to": "branch_manifest",
     "apply": lambda p: edit_manifest_field(p, "seeded_slru_pages", '"9999"'),
     "expect": REJECTED},
    {"name": "reader-manifest-format-unknown", "artifact": "reader_manifest",
     "apply": bump_manifest_format, "expect": REJECTED},
    {"name": "reader-manifest-edited", "artifact": "reader_manifest",
     "apply": lambda p: edit_manifest_field(p, "timeline", "77"), "expect": REJECTED},
    {"name": "snapshot-header-byte", "artifact": "reader_snapshot",
     "apply": lambda p: flip_byte(p, 8), "expect": REJECTED},
    {"name": "snapshot-xid-byte", "artifact": "reader_snapshot",
     "apply": lambda p: flip_byte(p, 40), "expect": REJECTED},
    {"name": "snapshot-truncated", "artifact": "reader_snapshot",
     "apply": truncate_tail, "expect": REJECTED},
    {"name": "catalog-byte", "artifact": "reader_catalog",
     "apply": lambda p: flip_byte(p, 0), "expect": REJECTED},
    {"name": "catalog-format-bumped", "artifact": "reader_catalog",
     "apply": lambda p: flip_byte(p, 20), "expect": REJECTED},
    {"name": "catalog-truncated", "artifact": "reader_catalog",
     "apply": truncate_tail, "expect": REJECTED},
    {"name": "map-pending-foreign-identity", "artifact": "reader_map_pending",
     "apply": foreign_trailer, "expect": REJECTED},
    {"name": "map-pending-legacy", "artifact": "reader_map_pending",
     "apply": legacy_marker, "expect": ACCEPTED},
    {"name": "map-pending-truncated", "artifact": "reader_map_pending",
     "apply": truncate_tail, "expect": REJECTED},
    {"name": "map-pending-zero-trailer", "artifact": "reader_map_pending",
     "apply": zero_trailer, "expect": REJECTED},
    {"name": "map-pending-overlong", "artifact": "reader_map_pending",
     "apply": overlong, "expect": REJECTED},
    {"name": "primed-foreign-identity", "artifact": "slru_primed",
     "apply": foreign_trailer, "expect": REJECTED},
    {"name": "primed-legacy", "artifact": "slru_primed",
     "apply": legacy_marker, "expect": ACCEPTED},
    {"name": "primed-truncated", "artifact": "slru_primed",
     "apply": truncate_tail, "expect": REJECTED},
    {"name": "primed-zero-trailer", "artifact": "slru_primed",
     "apply": zero_trailer, "expect": REJECTED},
    {"name": "primed-overlong", "artifact": "slru_primed",
     "apply": overlong, "expect": REJECTED},
]


# ---- capture -------------------------------------------------------------------

def capture(args: argparse.Namespace) -> int:
    fixture: Path = args.capture
    source: Path = args.source
    identity = json.loads((source / IDENTITY_JSON).read_text(encoding="utf-8"))
    check_identity_record(identity)
    missing = [rel for _, rel in ARTIFACTS.values() if not (source / rel).is_file()]
    if missing:
        raise FixtureError(f"{source} lacks {', '.join(missing)}")
    fixture.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pagestore-pgdata-capture-") as temp:
        staged = Path(temp) / "artifacts"
        for _, rel in ARTIFACTS.values():
            target = staged / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / rel, target)
        names = deterministic_tar(staged, fixture / ARTIFACTS_TAR)
    identities = format_identities(args.format_tool)
    (fixture / FORMAT_JSON).write_text(json.dumps(identities, indent=2) + "\n", encoding="utf-8")
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
            "data-directory artifacts a real backend prepared: a branch (manifest and "
            "bootstrap), a reader (manifest, snapshot and catalog provenance) and the "
            "two raw-value markers with their identity trailers"),
        "identity": identity,
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
    check_identity_record(metadata.get("identity", {}))
    return metadata


def check_loads(cluster: Cluster, root: Path, metadata: dict[str, Any]) -> int:
    """Every artifact loads through its loader and reports the identity the
    fixture advertises for it."""
    failures = 0
    identity = metadata["identity"]
    advertised = {
        item["artifact"].split(" ")[0]: (int(item["magic"], 16), item["version"])
        for item in json.loads((root.parent / "expected-format.json").read_text(encoding="utf-8"))
        if item["family"] == FAMILY
    }
    for name, (kind, rel) in ARTIFACTS.items():
        ok, report = cluster.check_artifact(kind, (root / rel).parent, identity)
        if not ok:
            failures += 1
            print(f"FAIL - {name} does not load: {report}")
            continue
        magic, version = advertised[Path(rel).name]
        if magic != 0 and f"magic={magic:08x} format={version}" not in report:
            failures += 1
            print(f"FAIL - {name} loaded as {report!r}, fixture advertises ({magic:#x}, {version})")
        elif magic == 0 and f"format={version}" not in report and metadata["role"] == "current":
            failures += 1
            print(f"FAIL - {name} loaded as {report!r}, fixture advertises format {version}")
        elif name in ("reader_map_pending", "slru_primed") and metadata["role"] == "current" \
                and "stamped" not in report:
            failures += 1
            print(f"FAIL - {name} loaded as {report!r}; a current fixture's marker carries its identity")
        else:
            print(f"ok   - {name} loads: {report}")
    return failures


def run_mutation(cluster: Cluster, temp: Path, fixture: Path, case: dict[str, Any],
                 metadata: dict[str, Any]) -> str:
    root = temp / "mutations" / case["name"]
    extract(fixture, root)
    target = ARTIFACTS[case.get("apply_to", case["artifact"])][1]
    try:
        case["apply"](root / target)
    except Skip as skip:
        print(f"skip - {case['name']}: {skip}")
        return SKIPPED
    kind, rel = ARTIFACTS[case["artifact"]]
    ok, report = cluster.check_artifact(kind, (root / rel).parent, metadata["identity"])
    return ACCEPTED if ok else REJECTED


def check_one(args: argparse.Namespace, fixture: Path) -> int:
    metadata = fixture_metadata(fixture)
    role = metadata["role"]
    print(f"--- fixture {fixture.name} ({role})")
    expected = json.loads((fixture / FORMAT_JSON).read_text(encoding="utf-8"))
    current = format_identities(args.format_tool)
    if role == "current":
        if current != expected:
            print("FAIL - compiled persisted-format identities differ from the fixture:")
            for item in current:
                if item not in expected:
                    print(f"  new or changed: {item}")
            for item in expected:
                if item not in current:
                    print(f"  missing or changed: {item}")
            print(f"  a persisted-format change must add or update a fixture ({fixture})")
            return 1
        print("ok   - compiled persisted-format identities match the fixture")
    elif current == expected:
        print("FAIL - a legacy fixture records the current identities; mark it current")
        return 1
    else:
        print("ok   - legacy fixture records superseded identities")
    if args.build is None:
        print("skip - no PostgreSQL build given: the loaders were not exercised")
        return 0
    failures = 0
    with tempfile.TemporaryDirectory(prefix="pagestore-pgdata-check-") as temp:
        root = Path(temp)
        (root / "expected-format.json").write_text(json.dumps(expected), encoding="utf-8")
        cluster = Cluster(args.build, root / "cluster")
        (root / "cluster").mkdir()
        try:
            cluster.start()
            artifacts = root / "artifacts"
            extract(fixture, artifacts)
            failures += check_loads(cluster, artifacts, metadata)
            if role == "current":
                (root / "mutations").mkdir()
                for case in MUTATIONS:
                    if args.only and case["name"] not in args.only:
                        continue
                    outcome = run_mutation(cluster, root, fixture, case, metadata)
                    if outcome == SKIPPED:
                        continue
                    if outcome == case["expect"]:
                        print(f"ok   - {case['name']}: {outcome}")
                    else:
                        failures += 1
                        print(f"FAIL - {case['name']}: {outcome}, expected {case['expect']}")
        finally:
            cluster.stop()
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
                        help="the artifacts integration_test.sh captured, with identity.json")
    parser.add_argument("--format-tool", type=Path, required=True)
    parser.add_argument("--build", type=Path,
                        help="a meson build directory with tmp_install; without it only "
                             "the identities are checked")
    parser.add_argument("--only", nargs="*", help="run only these mutation cases")
    parser.add_argument("--keep-failures", type=Path, help="copy a failed check's tree here")
    args = parser.parse_args(argv)
    for name in ("format_tool", "build", "source", "capture", "keep_failures"):
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
