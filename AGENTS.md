# Repository workflow (clapdb fork of PostgreSQL)

## Branching model

- **`master`** tracks the upstream PostgreSQL mainline. Keep it in sync with
  upstream; do **not** put clapdb work here. It is **not** the base of
  `pagestore` (see below).
- **`pagestore`** is the development branch for the clapdb page-store work
  (the disaggregated copy-on-write page store under `contrib/pagestore/`).
  It tracks the newest upstream **stable** branch (`REL_19_STABLE`, and later
  its GA tag and minors) by **merging it in** (`git merge
  upstream/REL_19_STABLE`, one merge commit per sync) — **never a rebase**.
  A rebase across `pagestore`'s ~1600-commit, 300-merge history would orphan
  every open branch and worktree and destroy the blame/PR trail; a merge
  costs one conflict-resolution pass per sync and keeps both. It is the
  repository's **default branch**: GitHub runs scheduled and manually
  dispatched workflows only from the default branch, and `master` must stay
  a pure upstream mirror, so the pagestore nightly soak lives here.
- **`branchdb_13` … `branchdb_19`** are the per-PG-major-version release
  branches. Each has a fixed shape: an upstream release **tag** (pinned, not
  a moving `REL_N_STABLE` head — `minor <N>` moves it to a newer tag on
  request), a small linear series of **core patches** ported from
  `pagestore`'s history, and a **byte-identical copy of `contrib/pagestore`**
  taken from one `pagestore` commit. Never develop directly on a
  `branchdb_N` branch or merge upstream into it; use
  `scripts/branchdb-sync.sh` (below) for every change to one.
  `contrib/pagestore/release-branches.json` is the source of truth for which
  majors are currently supported and each one's status, base tag and synced
  `pagestore` SHA. It is **hand-maintained** (a PR to `pagestore` updates
  it after a sync; `scripts/branchdb-sync.sh` only *reads* `base_tag`/
  `contrib_sha` for `status`, it never writes them — and a branch's own
  byte-identical `contrib/pagestore` copy cannot record its own commit SHA
  from inside itself anyway). `branchdb_13`/`branchdb_14` are kept for
  reference at their June 2026 LSM-foundation state, receive no syncs, and
  carry no release evidence (see the file's `unsupported` list for the
  upstream EOL dates driving that call).

So the flow is: develop on `pagestore` (tracking upstream `REL_19_STABLE` by
merge) → port the core patch series and sync `contrib/pagestore` onto each
supported `branchdb_N` with `scripts/branchdb-sync.sh`.

### `scripts/branchdb-sync.sh` and the `Branchdb-Series` trailer

- Every commit on `pagestore` that is meant to be forwarded to a
  `branchdb_N` core-patch series (as opposed to a `contrib/pagestore`-only
  commit, which travels through `sync-contrib`'s tree copy instead) must
  carry a `Branchdb-Series: C<n>` trailer (`C1`–`C7`; see
  `contrib/pagestore/PG_MAJOR_PORTABILITY.md` for the table of what each
  series covers and its source commits) in its commit message.
  `scripts/branchdb-sync.sh forward <FROM> <TO>` selects commits
  mechanically by this trailer and refuses (rather than silently doing
  nothing, or falling back to the raw commit range) when none carry it —
  which is the case for `pagestore`'s history as of this PR; a later PR
  adds the trailers when the core series is squashed into C1–C7 for the
  first `branchdb_18` sync.
- `contrib/pagestore` must be **byte-identical** between `pagestore` and
  every `branchdb_N`: version differences live inside the tree behind
  `#if PG_VERSION_NUM` guards, landed on `pagestore` first, so a release
  branch's contrib sync is a mechanical tree copy (`scripts/branchdb-sync.sh
  sync-contrib <branch> <pagestore SHA>`), verifiable with
  `scripts/branchdb-sync.sh verify <branch> <SHA>`. Today there is exactly
  one such guard (`pagestore_slru.c`'s 19-only `access/multixact_internal.h`
  include) — **that alone does not make `contrib/pagestore` build on 18**;
  `PG_MAJOR_PORTABILITY.md` tracks the full, compile-verified list of
  version-specific API usage still needing a guard (`ReplOriginId`,
  `XLogFindNextRecord`'s arity, `CHECKPOINT_FAST`, and more), closed by a
  separate "PG 18 compatibility guards" PR built from P2's work, landed on
  `pagestore` before `branchdb_18-rc`'s `sync-contrib` can copy a tree that
  actually compiles.
- A PR against a `branchdb_N` branch may only contain: `Branchdb-Series:`
  cherry-picks from `pagestore` (via `forward`), a `sync-contrib` commit, a
  `minor` rebase, or a `ci:`/`docs:`/`fixtures:`-prefixed commit; merges are
  not allowed above the branch's upstream tag base. `verify` checks all of
  this mechanically and is what a release-branch PR's CI (or reviewer)
  should run.
- See `scripts/branchdb-sync.sh --help` for the full command reference
  (`fetch`, `status`, `minor`, `forward`, `sync-contrib`, `verify`).

## Pull requests

- Open PRs with **base = `pagestore`**.
- When changes depend on each other, use **stacked PRs** (each PR based on the
  previous feature branch), matching the existing history.
- Keep each PR scoped to one coherent change, and keep the standalone pagestore
  test suite green.

## Pagestore

- Code: `contrib/pagestore/`. Design docs there: `LSM_ARCHITECTURE.md`,
  `MATERIALIZATION.md`, `SHARDING.md`, `WAL_REDO.md`, `DESIGN_NOTES.md`.
- Engine principle: **follow ScyllaDB** (share-nothing shard-per-core,
  run-to-completion, controllers + backpressure) and **stay lean** — do not pull
  in RocksDB's breadth (column families, merge operators, transactions, the many
  compaction knobs). Take only the minimal mechanisms a specialized page store
  needs. See `SHARDING.md`.
