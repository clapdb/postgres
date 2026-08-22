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


class PollError(RuntimeError):
    """A poll failed after bounded retries and may be transient."""


def gh_json(args: list[str]) -> Any:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            result = subprocess.run(
                ["gh", *args],
                check=True,
                capture_output=True,
                text=True,
            )
            return json.loads(result.stdout)
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2**attempt)
    raise PollError(str(last_error)) from last_error


def repo_name() -> str:
    value = gh_json(["repo", "view", "--json", "nameWithOwner"])
    return value["nameWithOwner"]


def graphql_json(query: str, variables: dict[str, Any]) -> Any:
    args = ["api", "graphql", "-f", f"query={query}"]
    for name, value in variables.items():
        args.extend(["-F", f"{name}={value}"])
    return gh_json(args)


def review_threads(repo: str, pr: str) -> list[dict[str, Any]]:
    owner, name = repo.split("/", 1)
    query = """
      query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
        repository(owner: $owner, name: $name) {
          pullRequest(number: $number) {
            reviewThreads(first: 100, after: $cursor) {
              nodes {
                id
                isResolved
                comments(first: 100) { nodes { databaseId } }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
      }
    """
    cursor: str | None = None
    threads: list[dict[str, Any]] = []
    while True:
        result = graphql_json(
            query,
            {
                "owner": owner,
                "name": name,
                "number": pr,
                "cursor": cursor or "null",
            },
        )
        page = result["data"]["repository"]["pullRequest"]["reviewThreads"]
        threads.extend(page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            return threads
        cursor = page["pageInfo"]["endCursor"]


def gh_collection(args: list[str]) -> list[Any]:
    pages = gh_json([*args, "--paginate", "--slurp"])
    if not pages:
        return []
    if all(isinstance(page, list) for page in pages):
        return [item for page in pages for item in page]
    return pages


VIEW_FIELDS = ",".join(
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
)


def pull_view(repo: str, pr: str) -> dict[str, Any]:
    view = gh_json(
        [
            "pr",
            "view",
            pr,
            "--repo",
            repo,
            "--json",
            VIEW_FIELDS,
        ]
    )
    return view


def identity(view: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(view.get(name) for name in (
        "state", "isDraft", "headRefOid", "baseRefOid", "baseRefName",
        "headRefName", "headRepositoryOwner",
    ))


def snapshot(repo: str, pr: str, cycle: int) -> dict[str, Any]:
    for attempt in range(3):
        view = pull_view(repo, pr)
        before = identity(view)
        reviews = gh_collection(["api", f"repos/{repo}/pulls/{pr}/reviews"])
        inline = gh_collection(["api", f"repos/{repo}/pulls/{pr}/comments"])
        comments = gh_collection(["api", f"repos/{repo}/issues/{pr}/comments"])
        threads = review_threads(repo, pr)
        after_view = pull_view(repo, pr)
        if before == identity(after_view):
            view = after_view
            break
        if attempt == 2:
            raise PollError("PR identity changed during snapshot collection")
        time.sleep(1)
    return {
        "cycle": cycle,
        "observed_at": int(time.time()),
        "repo": repo,
        "pr": int(pr),
        "pull": view,
        "reviews": reviews,
        "inline_comments": inline,
        "review_threads": threads,
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
        except (PollError, KeyError) as exc:
            print(json.dumps({"cycle": cycle, "error": str(exc)}), flush=True)
            if cycle != cycles:
                time.sleep(args.interval)
                continue
            return 1
        if cycle != cycles:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
