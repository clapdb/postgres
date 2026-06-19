# Repository workflow (clapdb fork of PostgreSQL)

## Branching model

- **`master`** tracks the upstream PostgreSQL mainline. Keep it in sync with
  upstream; do **not** put clapdb work here.
- **`pagestore`** is the development branch for the clapdb page-store work
  (the disaggregated copy-on-write page store under `contrib/pagestore/`).
  It is kept rebased on top of `master`.
- **`branchdb_13` … `branchdb_19`** are the per-PG-major-version release
  branches. Land work on a release by **cherry-picking from `pagestore`** to the
  relevant `branchdb_N` (do not develop directly on the release branches).

So the flow is: develop on `pagestore` (atop upstream `master`) → cherry-pick to
each `branchdb_N` release branch.

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
