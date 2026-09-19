#!/usr/bin/env python3
"""Guard: every "current" fixture's ``format.json`` must agree with one
snapshot of ``pagestore_format_versions``.

``harness/pagestore_fixture.py`` (the POSIX fixtures) and
``harness/pagestore_pgdata_fixture.py`` (``fixtures/pgdata-artifacts``) each
already compare their own fixtures' ``format.json`` against the compiled
identities -- but each runs ``pagestore_format_versions`` itself, in its own
workflow step, and neither looks at the other's fixture.  PR #262 bumped two
persisted-format identities and regenerated
``fixtures/posix-artifact-lifecycle/format.json`` but not
``fixtures/pgdata-artifacts/format.json``; both files are supposed to carry
the identical, full identity list, so the mismatch was only caught on the
base branch after merge (PR #263 fixed it).  This script buys nothing the two
checks do not already do on their own -- it runs the tool once, up front, and
diffs every current fixture against that one snapshot in a single cheap step,
so two fixtures drifting apart is caught before either fixture check's much
slower daemon (and, for pgdata-artifacts, PostgreSQL) work even starts.

A "legacy" fixture (``fixture.json``'s ``role``) records a format the daemon
still reads and is allowed to differ from the compiled identities, exactly as
the per-fixture checks already treat it: it is skipped here, not compared.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

FIXTURE_JSON = "fixture.json"
FORMAT_JSON = "format.json"


class GuardError(Exception):
    pass


def fixture_metadata(fixture: Path) -> dict[str, Any]:
    path = fixture / FIXTURE_JSON
    metadata = json.loads(path.read_text(encoding="utf-8"))
    if metadata.get("schema") != 1 or metadata.get("name") != fixture.name:
        raise GuardError(f"{path} does not describe {fixture.name}")
    role = metadata.get("role", "current")
    if role not in ("current", "legacy"):
        raise GuardError(f"{path} has an unknown role {role!r}")
    metadata["role"] = role
    return metadata


def format_identities(tool: Path) -> list[dict[str, Any]]:
    output = subprocess.run([str(tool)], check=True, capture_output=True, text=True).stdout
    return json.loads(output)


def check_one(fixture: Path, current: list[dict[str, Any]]) -> tuple[str, bool]:
    """Returns (role, ok)."""
    metadata = fixture_metadata(fixture)
    role = metadata["role"]
    if role != "current":
        print(f"skip - {fixture.name} (legacy): may differ from the compiled identities")
        return role, True
    expected = json.loads((fixture / FORMAT_JSON).read_text(encoding="utf-8"))
    if current == expected:
        print(f"ok   - {fixture.name}: format.json matches pagestore_format_versions")
        return role, True
    print(f"FAIL - {fixture.name}: format.json does not match pagestore_format_versions")
    for item in current:
        if item not in expected:
            print(f"  new or changed:     {item}")
    for item in expected:
        if item not in current:
            print(f"  missing or changed: {item}")
    return role, False


def check(args: argparse.Namespace) -> int:
    current = format_identities(args.format_tool)
    ok = True
    matched_current = 0
    for fixture in args.check:
        role, item_ok = check_one(fixture, current)
        ok = ok and item_ok
        if role == "current":
            matched_current += 1
    if matched_current == 0:
        raise GuardError(
            "none of the given fixtures is a current fixture; nothing was guarded"
        )
    if not ok:
        print(
            "  a persisted-format change must regenerate every current fixture's "
            "format.json, not just one (see the pagestore_fixture.py / "
            "pagestore_pgdata_fixture.py --capture steps)"
        )
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path, nargs="+", required=True, metavar="FIXTURE_DIR")
    parser.add_argument("--format-tool", type=Path, required=True)
    args = parser.parse_args(argv)
    args.format_tool = args.format_tool.resolve()
    args.check = [fixture.resolve() for fixture in args.check]
    try:
        return check(args)
    except GuardError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
