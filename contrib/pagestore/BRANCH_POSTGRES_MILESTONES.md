# Branch PostgreSQL milestone plan

This document defines the milestone plan for evolving `contrib/pagestore` from a
branchable relation-page prototype into a complete branch-capable PostgreSQL
system.

The plan is intentionally staged.  Relation-page branching should be made
correct first.  Bootable branch computes, SLRU, pg_control, retention, GC, and
production operations should be added only after the core storage and WAL/redo
semantics are stable.

## Current high-level assessment

Current implementation is closest to a branchable relation-storage prototype:

- timeline metadata exists;
- branch reads can fall back through parent timelines;
- relation pages are stored as versioned page images;
- WAL shipping, WAL indexing, and single-page redo are partially implemented;
- object-class keys, locking, SLRU, pg_control, and store-backed WAL work have
  been explored in the stacked PRs.

It is not yet a complete branch PostgreSQL system.

The largest remaining gaps are:

- timeline/WAL/redo correctness across branch boundaries;
- durable or rebuildable WAL indexes;
- SLRU transaction-status lifecycle;
- pg_control bootstrap and restore protocol;
- startup/recovery for a branch compute;
- branch retention, compaction, and garbage collection;
- operational tooling and production hardening.

## Milestone overview

```text
M0  current PR stack cleanup
M1  branchable relation storage MVP
M2  timeline WAL and redo correctness
M3  durable layer and manifest foundation
M4  bootable branch compute MVP
M5  branch lifecycle, retention, and GC
M6  production hardening
```

Recommended commitment boundary:

- commit near-term engineering to M0-M2;
- design M3 in parallel because it affects storage durability;
- do not resume SLRU or pg_control production work until M4 design is explicit.

## M0: current PR stack cleanup

### Goal

Turn the current stacked PRs into a reviewable, mergeable main route.

### Scope

Keep and complete:

- `#46`: read path, timeline walk, wal-redo client, `redo_page_asof`;
- `#47`: object-class discriminator in `PsKey`;
- `#48`: per-shard and map locking;
- `#51`: drop/truncate liveness;
- `#52`: timeline-tagged, LSN-ordered `walidx_get`.

Keep as draft or redesign:

- `#49`: SLRU on the store;
- `#50`: pg_control on the store;
- `#53`: broad store-backed WAL reader.

### Required stack shape

The healthy near-term branch should be:

```text
#46 -> #47 -> #48 -> #51' -> #52'
```

`#51` and `#52` should be rebased or recreated on top of `#48`, not left behind
frozen SLRU and pg_control PRs.

### Non-goals

M0 should not include:

- SLRU read-from-store production semantics;
- pg_control production restore;
- full store-backed WAL reader across all object classes;
- object-storage tiering;
- branch compute bootstrap.

### Completion criteria

- The mergeable PR chain does not depend on draft PRs.
- `#46` handles base FPI selection before redo-chain caps.
- wal-redo helper I/O is interrupt-safe enough not to strand helpers or locks.
- ancestry WAL replay order is correct for relation pages.
- `#48` preserves `IMMEDSYNC` and map-lock invariants.
- `#51/#52` are reviewable as relation-page correctness work.

## M1: branchable relation storage MVP

### Goal

Provide a reliable storage-level branch for relation forks.

This milestone answers: can pagestore maintain branch-isolated relation pages,
without yet booting a separate PostgreSQL compute from that branch?

### Scope

- Metadata-only branch creation.
- Branch-local relation writes.
- Parent fallback reads at the branch point.
- Relation fork support for heap, index, toast, FSM, and VM forks.
- Branch-aware fork size, extend, truncate, and drop behavior.
- Relation-page materialization at a target LSN.
- Durable branch metadata across daemon restart.

### Non-goals

M1 should not include:

- branch compute startup;
- SLRU bootstrapping from the store;
- pg_control restore;
- full PostgreSQL crash recovery on a branch;
- remote object storage;
- production GC.

### Completion criteria

- A child branch can read parent data visible at the fork point.
- Parent and child writes do not contaminate each other.
- Child branch can overwrite, extend, truncate, and drop relation forks without
  corrupting parent visibility.
- Daemon restart preserves branch metadata and relation-page visibility.
- Relation fork reads either return the correct visible page or fail closed.

## M2: timeline WAL and redo correctness

### Goal

Make relation-page materialization correct across timelines and WAL sources.

This milestone answers: can pagestore reconstruct relation pages as of a target
LSN in a branch-aware way?

### Scope

- Durable or reliably rebuildable per-page WAL index.
- Source timeline attached to each WAL index record.
- Replay-safe global order across timeline ancestry.
- Newest usable FPI selection before applying redo-chain caps.
- Drop/truncate liveness scans for relation pages.
- Relation-page store-backed WAL reader only.
- WAL reader cache invalidation when source or timeline changes.
- Fail-closed behavior when WAL is missing, incomplete, or ambiguous.

### Non-goals

M2 should not include:

- SLRU WAL replay or SLRU read-from-store;
- pg_control restore;
- generic object bootstrapping;
- SPDK-specific object semantics beyond contracts used by included code;
- helper pooling or performance optimization except where required for safety.

### Completion criteria

- `redo_page_asof(timeline, relation, fork, block, lsn)` returns the correct page
  or fails closed.
- A hot page with long total history remains readable when a recent FPI bounds
  the replay chain.
- Parent and branch WAL records are replayed in the correct order.
- A truncate or drop between the base image and target LSN is honored.
- Missing local or shipped WAL does not produce a stale page.
- Switching from local WAL to store WAL cannot reuse a stale reader cache page.

## M3: durable layer and manifest foundation

### Goal

Move pagestore from prototype in-memory indexes and append-only segments toward a
durable layer/manifest model that can support long-running branches.

This milestone answers: can pagestore recover its storage metadata and compacted
state safely after restart or crash?

### Scope

- Manifest as the source of truth for layer metadata.
- Image layer descriptor encode/decode.
- Layer map rebuilt from manifest.
- Memtable flush into immutable image layers.
- Manifest state transitions for add, seal, replace, delete, and remove.
- Local compaction replacement protocol.
- Local GC protocol.
- `PsKey.klass` persistence through layer and manifest metadata.
- Crash-safe manifest replay.

### Non-goals

M3 should not include:

- remote object storage as the primary storage tier;
- remote range reads;
- production branch-drop GC across all object classes;
- delta layers unless relation-page redo already requires them in a bounded
  local form.

### Completion criteria

- Daemon restart can rebuild the layer map from durable manifest metadata.
- Compaction can install replacement layers without exposing partial results.
- Interrupted compaction or GC is recoverable.
- Branch-visible parent layers are not removed while still referenced.
- Relation-page reads remain correct through memtable, image layers, and compacted
  layers.

## M4: bootable branch compute MVP

### Goal

Allow a PostgreSQL compute to start on a branch timeline and run normal read/write
workloads.

This milestone answers: can a branch become an independently bootable PostgreSQL
compute, not just a storage view?

### Scope

- Branch compute bootstrap protocol.
- pg_control restore design and implementation.
- SLRU lifecycle design and implementation.
- WAL restore from pagestore.
- Startup/recovery flow for a branch timeline.
- Branch-local transaction status handling.
- Safe inheritance of parent transaction status at the fork point.
- Atomic and durable restore of control files and required bootstrap state.

### Non-goals

M4 should not include:

- multiple active computes sharing the same writable branch;
- production-grade branch deletion and retention;
- remote object storage eviction;
- cross-version upgrade support.

### Completion criteria

- A branch can be created at a consistent LSN.
- A new compute can start on that branch timeline.
- The branch compute can run ordinary table reads and writes.
- Restarting the branch compute preserves consistency.
- pg_control restore is atomic and directory-durable.
- SLRU read, write, truncate, existence, and cache semantics are complete enough
  for normal transaction processing.
- Missing or incomplete bootstrap state fails closed rather than silently
  starting an inconsistent branch.

## M5: branch lifecycle, retention, and GC

### Goal

Support long-lived branches with safe deletion, retention, compaction, and
reclamation.

This milestone answers: can pagestore manage a branch graph over time without
leaking storage or deleting still-visible history?

### Scope

- Persistent branch graph metadata.
- Branch drop.
- Retention horizon calculation.
- Active read tracking.
- WAL retention across branch ancestry.
- SLRU/control retention once M4 exists.
- Layer GC across branches.
- Compaction across branch-visible history.
- Consistency checker for branch graph, manifest, layer, and WAL metadata.

### Non-goals

M5 should not include:

- full multi-tenant product API;
- automatic remote-tier cost optimization;
- cross-cluster replication unless explicitly required by product scope.

### Completion criteria

- Dropping a branch eventually reclaims its exclusive data.
- Parent data referenced by child branches is not reclaimed prematurely.
- GC and compaction preserve historical reads required by retention.
- Crashes during branch drop, GC, or compaction are recoverable.
- Tooling can inspect and validate branch graph, manifest, layers, WAL, and
  object-class metadata.

## M6: production hardening

### Goal

Turn the branch-capable prototype into an operationally usable system.

### Scope

- Performance tuning and benchmarking.
- wal-redo helper pooling or a redo service.
- Observability: metrics, logs, tracing, and debug endpoints.
- Failpoint and crash-injection tests.
- Corruption detection and repair tooling.
- Upgrade and migration story.
- POSIX, SPDK, and object-tier contract parity.
- Object storage tiering and local cache policy.
- Branch create, drop, list, inspect, and repair APIs.

### Completion criteria

- Long-running stress tests are stable.
- Crash/restart/failover tests cover the critical branch lifecycle.
- Performance envelope is known and documented.
- Operational metrics can explain latency, redo work, branch retention, GC,
  compaction, and storage growth.
- Repair tooling can diagnose common inconsistencies without manual data surgery.
- POSIX, SPDK, and object-tier backends provide equivalent correctness contracts.

## Suggested execution strategy

### Near-term engineering focus

Work only on M0-M2 until the relation-page route is stable.

Recommended order:

```text
1. Rebase or recreate #51/#52 on top of #48.
2. Finish #46 review issues.
3. Finish #47/#48 implementation completeness issues.
4. Merge the healthy relation-page correctness chain.
5. Split #53 and keep only relation-page store-backed WAL in the first revival.
```

### Parallel design work

Design M3-M4 in documents before resuming broad implementation:

- M3 layer/manifest durability and compaction protocol;
- M4 SLRU lifecycle;
- M4 pg_control bootstrap and restore protocol.

### Work to avoid for now

Do not keep adding fixes to SLRU, pg_control, and broad store-backed WAL inside
the current stack.  Those paths cross PostgreSQL recovery, transaction-status,
and critical-section invariants; they need explicit protocols before more code is
useful.
