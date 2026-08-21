---
name: pr-watchdog
description: Monitor a specified GitHub pull request until it is ready by handling actionable reviews, fixing CI failures, rebasing conflicts, pushing scoped changes, and requesting a fresh Codex review after every push. Use when the user explicitly asks to watch or babysit a PR; never merge automatically.
---

# PR Watchdog

Own the specified pull request until it reaches a stable ready state. Keep the
work scoped to that PR and preserve repository instructions, branch policy, and
unrelated user changes.

## Authorization boundary

An explicit request to watch or babysit a PR authorizes these normal operations
on that PR's head branch:

- inspect checks, logs, reviews, comments, mergeability, and branch state;
- implement review or CI fixes within the PR's existing scope;
- run proportionate tests;
- commit and push fixes;
- rebase the PR head onto its current base and use an exact
  `--force-with-lease` after conflicts are resolved;
- post the exact PR comment `@codex, review` after each successful push.

It does not authorize merging or closing the PR, changing its base, rewriting a
different branch, bypassing branch protection, dismissing reviews, changing
secrets, or expanding the feature. Stop and ask before any such action.

If the user only asks for PR status or analysis, remain read-only.

## Establish the watch

1. Resolve the repository and exact PR URL or number. Read applicable
   `AGENTS.md` files and repository-specific contribution instructions.
2. Inspect the PR's open/closed state, head and base repositories/branches,
   current head SHA, mergeability, review decision, review threads, comments,
   and all required checks. Prefer connected GitHub tools when available;
   otherwise use `gh`.
3. Confirm authentication, write access to the head branch, and that the head is
   not a protected/default branch. A fork PR without writable head access is a
   blocker, not permission to push somewhere else.
4. Inspect the local worktree before switching branches. Preserve unrelated
   changes. Do not overwrite, stash, clean, or relocate user work without
   permission. Create a separate worktree if isolation is needed and safe.
5. Record at least: PR identity, base branch, remote head SHA, handled review
   thread IDs, check run identities, and the last head SHA for which a Codex
   review was requested.

## Monitoring cycle

Use the product's monitoring or wait mechanism when available. Otherwise poll
with backoff: start around 30 seconds, increase toward five minutes while state
is unchanged, and reset after a state change or push. Respect API rate limits.
Do not spin or repeatedly emit unchanged status.

For every cycle, take one coherent snapshot and compare it with the previous
state. Process in this order:

1. PR closed or merged: report the terminal state and stop.
2. Remote head changed externally: stop any pending mutation, fetch, inspect the
   new commits, and rebuild the state snapshot before continuing.
3. Merge conflict: run the conflict workflow.
4. New actionable review feedback: run the review workflow.
5. Failed or cancelled required CI: run the CI workflow.
6. Pending CI or review: wait.
7. Ready-state criteria satisfied: report readiness and stop.

After any commit or push, discard conclusions tied to the old SHA and start a
fresh cycle. Never treat checks or reviews from an older head as current proof.

## Handle reviews

- Collect unresolved review threads and review decisions, including inline
  comments. Deduplicate by stable review/thread identity, not comment text.
- Evaluate each finding against the code and repository rules. Do not implement
  a suggestion merely because it exists. Fix valid findings; explain incorrect,
  stale, conflicting, or out-of-scope findings with concrete evidence.
- Keep fixes narrowly connected to the finding. Add the smallest useful
  regression test when a correctness bug is found.
- Reply with the fix commit or concise reasoning when repository permissions and
  conventions permit. Resolve a thread only when the concern is actually
  handled; never dismiss a review automatically.
- Treat a fresh review on the current head as new evidence even if similar text
  appeared earlier.

## Handle CI failures

- Identify the failing job, step, attempt, and exact head SHA. Read the failed
  logs before editing code.
- Classify the failure as code regression, test expectation, environment or
  infrastructure issue, flaky test, timeout/resource pressure, or cancellation.
- Reproduce locally when practical with the narrowest faithful command. Fix the
  root cause without weakening assertions or skipping coverage.
- Retry a job without a code change only when there is evidence of an
  infrastructure or flaky failure. Retry at most once per unchanged failure
  fingerprint; a repeated failure requires diagnosis or escalation.
- Run tests proportionate to the change and repository policy. Do not start
  redundant concurrent builds when one build can cover the affected targets.

## Resolve conflicts

For the detailed stacked-history and three-way conflict procedure, read
[references/rebase-conflicts.md](references/rebase-conflicts.md) before editing.

1. Fetch both base and head. Re-read the remote head SHA immediately before
   rebasing; it must equal the recorded SHA.
2. Require a clean, isolated worktree containing the PR head. Rebase the head
   onto the PR's current remote base. Do not merge the base into the head unless
   repository policy explicitly requires it.
3. Before applying a long stacked series, compare the commit graph and patch
   content. Use `git range-diff`, `git log --cherry-pick`, and stable patch IDs
   to identify commits already represented by the current base under different
   SHAs. Drop only commits whose behavior is demonstrably already in base;
   preserve dependent commits and record the mapping. Never drop a commit
   merely because its subject looks similar.
4. Apply and resolve the remaining commits in dependency order. For each
   conflict, inspect all three versions and the commit's parent-to-child diff.
   Preserve compatible changes from both sides, including newer base locking,
   validation, error handling, and test behavior. Do not resolve a whole file
   with `ours` or `theirs` when executable behavior overlaps. If a conflict
   spans a refactor, follow the complex-conflict procedure in the reference:
   establish the base API and invariants, split the PR change into behavioral
   units, and port each unit into that shape. Keep the resolution limited to
   the PR. Complexity or a large conflict region is not, by itself, a reason
   to abort.
5. After each behavioral unit, inspect the staged diff and run the narrowest
   relevant build/test before continuing. If the next commit no longer applies
   because an earlier equivalent change was absorbed, use `rebase --skip` only
   with evidence that the commit's complete behavior is present.
6. Run the affected tests, inspect the rewritten range with `git range-diff`,
   verify that the final diff against the base still contains the intended
   feature and no accidental base reverts, and push only with an exact lease,
   for example:

   ```sh
   git push --force-with-lease=refs/heads/<head>:<recorded-remote-sha> origin <head>
   ```

7. If the lease fails, do not override it. Fetch the collaborator's update and
   reassess from a new snapshot.

If a conflict cannot be resolved without choosing a new design, changing the
PR's scope, weakening an invariant, or guessing at intended behavior, abort the
rebase and report the exact files, commits, and decision required. Complexity
alone is not a blocker; unresolved semantic ambiguity is. Before stopping for
semantic ambiguity, attempt the reference's reconstruction fallback, which
replays the unique logical commits from a fresh base worktree without merging
the base branch.

## Push and request a fresh Codex review

- Before pushing, verify the diff, tests, intended branch, and current remote
  head. Prefer ordinary push for additive fix commits; use force-with-lease only
  for the authorized rebase workflow.
- After a successful push, read back the new remote head SHA and post exactly:

  ```text
  @codex, review
  ```

  With `gh`, use a literal body so shell interpolation cannot alter it:

  ```sh
  gh pr comment <pr> --body '@codex, review'
  ```

- Request once per pushed head SHA. Do not post duplicate triggers for the same
  SHA. If the integration provides no acknowledgement or review after a
  reasonable wait, verify that Codex review is enabled and retry once; then
  report the integration problem instead of spamming comments.
- Wait for the resulting review and associate it with the current head before
  declaring the PR ready. Handle any new actionable findings through the normal
  review workflow.

## Ready and stopping conditions

The default ready state requires all of the following on one unchanged head SHA:

- the PR is open and mergeable without conflicts;
- every required check has completed successfully;
- no unresolved actionable review thread or changes-requested decision remains;
- the requested Codex review for the current pushed head has completed and its
  actionable findings are handled;
- the local worktree is clean and the local head matches the remote head.

Report the final SHA, checks, review state, fixes pushed, and any non-blocking
warnings. Do not merge the PR.

Stop and report a blocker when authentication or permissions fail, the remote
head repeatedly moves during a mutation, conflict resolution is ambiguous,
required secrets or external infrastructure are unavailable, the same failure
recurs for three cycles without new evidence, repository policy forbids the
needed action, or the user cancels the watch.

