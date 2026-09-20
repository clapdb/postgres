#!/usr/bin/env python3
"""Reader for ``contrib/pagestore/release-branches.json``: the supported-major
manifest.

The manifest is the single place that says which PostgreSQL majors
``branchdb_<N>`` supports (``supported``, each with a ``status`` of
``planned``, ``candidate``, ``released`` or ``released-preview``, and the
``base_tag``/``contrib_sha`` it was last synced at -- both ``null`` until a
sync happens) and which are kept only for reference (``unsupported``, each
with a human-readable ``reason``, e.g. an upstream EOL date).

The manifest is **hand-maintained**: nothing writes ``base_tag``/
``contrib_sha`` automatically. ``scripts/branchdb-sync.sh sync-contrib``
and ``minor`` only *read* them (for ``status``'s "does contrib match the
recorded SHA" report); a release-branch PR that performs a sync updates
the manifest itself, on ``pagestore``, as part of that PR -- a branch's
own byte-identical ``contrib/pagestore`` copy has no way to record its own
commit SHA from inside itself.

Consumers:

* ``scripts/branchdb-sync.sh status`` invokes this module directly (as a
  subprocess, so the shell script stays dependency-free) to look up a
  major's recorded ``contrib_sha``.
* The nightly/release-acceptance workflows (V4 plan, P4) read
  ``candidate_branches()`` to fan their soak matrix out over every branch
  that currently has release evidence riding on it, instead of a hardcoded
  YAML list.

Run this file directly (no arguments) for a self-test: it validates the
manifest's own structure (schema, no duplicate or misnamed majors, every
supported major has a status this reader recognises, 13 and 14 are exactly
the unsupported set) and prints a one-line summary. Run it with
``--major N --field FIELD`` to print one manifest field for major N (the
lookup ``scripts/branchdb-sync.sh status`` uses).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_PATH = Path(__file__).resolve().parent.parent / "release-branches.json"
REQUIRED_SCHEMA = 1
VALID_STATUSES = {"planned", "candidate", "released", "released-preview"}
CANDIDATE_STATUSES = {"candidate", "released", "released-preview"}


class ReleaseBranchesError(Exception):
    pass


def _check_common(path: Path, entry: dict[str, Any], seen_majors: set[int]) -> None:
    major = entry.get("major")
    branch = entry.get("branch")
    if not isinstance(major, int):
        raise ReleaseBranchesError(f"{path}: entry {entry!r} has no integer 'major'")
    if major in seen_majors:
        raise ReleaseBranchesError(f"{path}: major {major} is listed more than once")
    seen_majors.add(major)
    if branch != f"branchdb_{major}":
        raise ReleaseBranchesError(
            f"{path}: major {major}'s branch is {branch!r}, expected 'branchdb_{major}'"
        )


def load(path: Path | None = None) -> dict[str, Any]:
    """Parse and validate release-branches.json.  Raises
    ReleaseBranchesError on any structural problem, so a workflow fans its
    matrix out from a manifest that is at least internally consistent rather
    than failing later with a confusing branch-not-found error."""
    path = path or DEFAULT_PATH
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ReleaseBranchesError(f"cannot read {path}: {error}") from error
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ReleaseBranchesError(f"{path} is not valid JSON: {error}") from error

    if data.get("schema") != REQUIRED_SCHEMA:
        raise ReleaseBranchesError(
            f"{path}: schema must be {REQUIRED_SCHEMA}, got {data.get('schema')!r}"
        )

    supported = data.get("supported")
    unsupported = data.get("unsupported")
    if not isinstance(supported, list) or not supported:
        raise ReleaseBranchesError(f"{path}: 'supported' must be a non-empty list")
    if not isinstance(unsupported, list):
        raise ReleaseBranchesError(f"{path}: 'unsupported' must be a list")

    seen_majors: set[int] = set()
    for entry in supported:
        _check_common(path, entry, seen_majors)
        if entry.get("status") not in VALID_STATUSES:
            raise ReleaseBranchesError(
                f"{path}: supported major {entry.get('major')} has an unknown "
                f"status {entry.get('status')!r} (expected one of {sorted(VALID_STATUSES)})"
            )
        for field in ("base_tag", "contrib_sha"):
            if field not in entry:
                raise ReleaseBranchesError(
                    f"{path}: supported major {entry.get('major')} is missing '{field}' "
                    "(null is fine before it is synced; the key must still be present)"
                )
    for entry in unsupported:
        _check_common(path, entry, seen_majors)
        if not isinstance(entry.get("reason"), str) or not entry["reason"]:
            raise ReleaseBranchesError(
                f"{path}: unsupported major {entry.get('major')} is missing a 'reason'"
            )

    return data


def supported_majors(data: dict[str, Any] | None = None) -> list[int]:
    data = data if data is not None else load()
    return sorted(entry["major"] for entry in data["supported"])


def unsupported_majors(data: dict[str, Any] | None = None) -> list[int]:
    data = data if data is not None else load()
    return sorted(entry["major"] for entry in data["unsupported"])


def candidate_branches(data: dict[str, Any] | None = None) -> list[str]:
    """Branch names whose status is "candidate", "released" or
    "released-preview" -- what a soak/acceptance workflow matrix should
    include; a "planned" major has no branch worth soaking yet."""
    data = data if data is not None else load()
    return [entry["branch"] for entry in data["supported"]
            if entry["status"] in CANDIDATE_STATUSES]


def entry_for_major(major: int, data: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """The supported- or unsupported-list entry for `major`, or None."""
    data = data if data is not None else load()
    for entry in data["supported"] + data["unsupported"]:
        if entry["major"] == major:
            return entry
    return None


def _self_test() -> int:
    data = load()
    majors = supported_majors(data)
    unsupported = unsupported_majors(data)
    assert len(majors) == len(set(majors)), "duplicate majors in 'supported' slipped past validation"
    assert set(majors) >= {15, 16, 17, 18, 19}, f"expected at least 15-19 supported, got {majors}"
    assert unsupported == [13, 14], f"expected exactly 13 and 14 unsupported, got {unsupported}"
    assert not (set(majors) & set(unsupported)), "a major cannot be both supported and unsupported"
    for major in unsupported:
        entry = entry_for_major(major, data)
        assert entry is not None and "EOL" in entry["reason"], \
            f"unsupported major {major} should record an EOL-shaped reason, got {entry}"
    candidates = candidate_branches(data)
    print(f"ok   - {DEFAULT_PATH} is valid: {len(majors)} supported major(s) "
          f"({', '.join(str(m) for m in majors)}), "
          f"{len(unsupported)} unsupported ({', '.join(str(m) for m in unsupported)}), "
          f"candidate/released branch(es): {', '.join(candidates) if candidates else '(none yet)'}")
    return 0


def _print_field(major: int, field: str) -> int:
    """`--major N --field FIELD`: print one field of major N's manifest
    entry (supported or unsupported), or nothing if the field is absent or
    null. Used by scripts/branchdb-sync.sh's `status` as a subprocess call,
    so the shell script does not duplicate the manifest's parsing/lookup
    logic -- this module is the one place that knows the schema."""
    try:
        entry = entry_for_major(major)
    except ReleaseBranchesError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    if entry is None:
        return 0
    value = entry.get(field)
    if value is not None:
        print(value)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--major", type=int, metavar="N",
                        help="print one manifest field for major N instead of self-testing")
    parser.add_argument("--field", metavar="FIELD",
                        help="the field to print (requires --major), e.g. contrib_sha, base_tag")
    args = parser.parse_args(argv)
    if args.major is not None or args.field is not None:
        if args.major is None or args.field is None:
            parser.error("--major and --field must be given together")
        return _print_field(args.major, args.field)
    try:
        return _self_test()
    except (ReleaseBranchesError, AssertionError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
