#!/usr/bin/env python3
"""Read-only pull-request poller for pr-watchdog.

The poller collects one coherent GitHub snapshot per cycle and emits JSON
lines.  It deliberately does not decide whether a PR is ready and never
mutates GitHub; the watchdog/sol layer owns interpretation and actions.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from typing import Any


def gh_json(args: list[str]) -> Any:
    result = subprocess.run(
        ["gh", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def repo_name() -> str:
    value = gh_json(["repo", "view", "--json", "nameWithOwner"])
    return value["nameWithOwner"]


def gh_collection(args: list[str]) -> list[Any]:
    pages = gh_json([*args, "--paginate", "--slurp"])
    if not pages:
        return []
    if all(isinstance(page, list) for page in pages):
        return [item for page in pages for item in page]
    return pages


def snapshot(repo: str, pr: str, cycle: int) -> dict[str, Any]:
    view = gh_json(
        [
            "pr",
            "view",
            pr,
            "--repo",
            repo,
            "--json",
            ",".join(
                [
                    "url",
                    "state",
                    "isDraft",
                    "headRepositoryOwner",
                    "headRefName",
                    "headRefOid",
                    "baseRefName",
                    "baseRefOid",
                    "mergeable",
                    "mergeStateStatus",
                    "reviewDecision",
                    "autoMergeRequest",
                    "statusCheckRollup",
                ]
            ),
        ]
    )
    reviews = gh_collection(["api", f"repos/{repo}/pulls/{pr}/reviews"])
    inline = gh_collection(["api", f"repos/{repo}/pulls/{pr}/comments"])
    comments = gh_collection(["api", f"repos/{repo}/issues/{pr}/comments"])
    return {
        "cycle": cycle,
        "observed_at": int(time.time()),
        "repo": repo,
        "pr": int(pr),
        "pull": view,
        "reviews": reviews,
        "inline_comments": inline,
        "issue_comments": comments,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pr", type=int, help="pull-request number")
    parser.add_argument("--repo", help="OWNER/REPO; defaults to gh's current repo")
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval < 0 or args.cycles < 1:
        parser.error("--interval must be non-negative and --cycles must be positive")

    repo = args.repo or repo_name()
    cycles = 1 if args.once else args.cycles
    for cycle in range(1, cycles + 1):
        try:
            print(json.dumps(snapshot(repo, str(args.pr), cycle), sort_keys=True), flush=True)
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
            print(json.dumps({"cycle": cycle, "error": str(exc)}), flush=True)
            return 1
        if cycle != cycles:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
