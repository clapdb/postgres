# Rebase conflict playbook

Use this reference when a PR base has advanced through a stacked series or has
rewritten equivalent commits.

## Establish the commit mapping

Record the remote head and base before mutation. In an isolated worktree:

```sh
git log --graph --oneline --decorate <base>..<head>
git range-diff <old-base>..<old-head> <new-base>..<new-head>
git log --cherry-pick --right-only --oneline <base>...<head>
```

Record both complete series before editing: `<old-base>..<old-head>` is the
original PR series, and `<new-base>..<new-head>` is the candidate rebased
series. Use the two-dot ranges in `range-diff`; symmetric three-dot ranges mix
base-only commits with PR commits and can produce a misleading equivalence map.

For suspected duplicates, compare `git show <commit>` with the base history
and stable patch IDs. A different SHA is not enough to call a commit
equivalent: changed validation, locking, persistence, error paths, or tests
must be reviewed as behavior. If an earlier stacked commit is already in base,
replay only the commits after that semantic boundary, then verify the final
base-to-head diff.

Build an explicit mapping before editing:

| Change in the conflict | Default treatment |
| --- | --- |
| Base-only hardening, API, lock, cleanup, or recovery behavior | Preserve base |
| PR-only behavior absent from base | Port into the base API |
| Same behavior implemented differently | Keep the base implementation and port only missing tests/behavior |
| Truly incompatible policy or lifecycle semantics | Stop only after the reconstruction and test steps below fail |

Review follow-up comments against the rewritten range, not their old commit
IDs. A finding already fixed by a later PR commit is evidence to preserve that
behavior, not a reason to replay the old patch again.

## Resolve a code conflict

For each file, inspect the three stages and the patch being replayed:

```sh
git show :1:path       # merge base
git show :2:path       # current base / ours
git show :3:path       # replayed commit / theirs
git show <commit>^..<commit> -- path
```

Resolve the smallest coherent region. In refactored code, port the feature's
observable behavior into the current base API rather than restoring the old
implementation. Explicitly check record formats, lock/unlock pairs, cleanup
paths, retry semantics, overflow handling, and test expectations when those
areas are involved.

## Complex conflict procedure

When a conflict touches a subsystem refactor, do not treat the file as the
unit of resolution. Use this sequence:

1. Identify the subsystem invariants from the base: ownership of each lock,
   legal state transitions, durable markers, cleanup obligations, and the
   success/failure contract of each public helper. Search all callers and
   tests of the affected symbols with `rg` before editing.
2. Partition the PR commit into behavioral units such as data-format changes,
   planning/selection, serialization, publication, recovery, cleanup, and
   tests. Use `git show --function-context <commit>` and targeted path diffs to
   make the partition; do not copy conflict markers wholesale.
3. Keep the base lifecycle and synchronization skeleton. Port the PR unit into
   the base API, adapting names and data flow as needed. For example, if base
   changed from direct buffers to a producer/prepared-input API, retain that
   API and move the PR's filtering or compaction decision into the producer;
   do not restore the old buffered path just to make the patch apply.
4. For every failure path, trace both the normal and crash-restart sequence.
   Check that locks are released, prepared files/descriptors are either
   discarded or durably discoverable, retries are idempotent, and a failed
   operation cannot admit mutations that change the retry input.
5. Stage one coherent unit, inspect `git diff --cached`, and run its narrow
   test or compile target. Only then resolve the next unit. If a unit changes a
   wire or on-disk format, compile and run both legacy-read and new-write tests
   before continuing.

### Reconstruction fallback

If normal `rebase` conflict hunks are too entangled, abort it and reconstruct
the same rebased history in a fresh worktree rooted at the current base:

```sh
git worktree add -b <temporary-rebase-branch> <dir> <base>
git diff <unique-commit>^ <unique-commit> -- <paths>
```

Replay each unique commit in order by applying its parent-to-child changes to
the fresh base, resolving them by behavioral unit, then committing with the
original message while preserving its author metadata, for example with
`git commit -C <source-commit>` after staging the reconstructed change. This is
still a rebase of the PR head: do not merge the base branch and do not copy
already-integrated commits. After every reconstructed commit, compare its diff
with the original commit's intended behavior and run the narrow test.

After the final reconstructed commit passes validation, publish that exact
history as the PR head before removing the temporary worktree. Either move the
local PR-head ref to the temporary branch's tip, or push the temporary branch
directly with the recorded lease and explicit refspec, for example:

```sh
git update-ref refs/heads/<head> <temporary-rebase-branch>
git push --force-with-lease=refs/heads/<head>:<recorded-remote-sha> \
  <head-remote> <temporary-rebase-branch>:refs/heads/<head>
```

Verify the remote head SHA and final diff before cleanup. Remove the temporary
worktree only after the PR head has been published and the tests are recorded.

This fallback is preferred for a large refactor conflict because it separates
base integration from feature replay. It is not permission to redesign the
feature or to silently drop behavior.

After staging a complex file, inspect `git diff --cached` and run the narrowest
faithful test or build. At the end, use `git range-diff` and
`git diff <base>...HEAD` to ensure the resolution did not revert base work or
broaden the PR.

For a subsystem with a state machine, add a temporary verification checklist
before declaring success:

- normal success path;
- failure before durable publication;
- failure after durable publication but before commit;
- restart after each partial cleanup step;
- concurrent admission while publication is pending;
- retry with unchanged input and retry after unrelated activity.

Run existing tests covering each case. If coverage is missing, add the smallest
scoped regression test required by the conflict rather than weakening an
assertion or skipping the path.

## Safe stopping rule

Abort and ask for direction only when the code or repository policy does not
determine the intended behavior, the resolution would redesign the feature,
or the required test/build/infrastructure is unavailable. Before declaring
that condition, complete at least one behavioral-unit attempt and the
reconstruction fallback when the conflict is merely large. Report the exact
conflicted files, competing invariants, attempted resolutions, test results,
and missing decision.
