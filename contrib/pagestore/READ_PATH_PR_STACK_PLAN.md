# pagestore read-path PR stack plan

This document records the current plan for the stacked pagestore read-path PRs
`#46` through `#53` after review.  The goal is to preserve the parts of the
route that are structurally sound, stop accumulating semantic debt in the parts
that are not ready, and define clear criteria for bringing the deferred work
back.

## Summary decision

Keep the core read-path direction, but narrow the mergeable scope.

The following direction is worth preserving:

- timeline-aware WAL indexing;
- relation-page `redo_page_asof`;
- object-class key discrimination as a storage seam;
- per-shard locking once its synchronization invariants are complete;
- liveness and timeline ordering fixes that make relation-page redo correct.

The following work should remain draft or experimental until its semantics are
redesigned:

- store-backed SLRU reads and writes;
- pg_control mirroring and restore;
- broad store-backed WAL reading that mixes relation redo, SLRU/control object
  semantics, SPDK foundness, manifest replay, and branch-boundary liveness in a
  single PR.

This is not a rejection of the overall read-path route.  It is a scope boundary:
relation-page redo and timeline-aware WAL lookup should become reliable first;
SLRU, pg_control, and broader object bootstrapping need separate lifecycle
contracts before they become mergeable production paths.

## PR stack state

The current stack is:

```text
#46 feat/pagestore-readpath
  -> #47 feat/pagestore-objclass
  -> #48 feat/pagestore-pershard-lock
  -> #49 feat/pagestore-slru
  -> #50 feat/pagestore-control
  -> #51 feat/pagestore-liveness
  -> #52 feat/pagestore-branchwal
  -> #53 feat/pagestore-storewal
```

Because these PRs are stacked, an unresolved semantic problem in an early or
middle layer expands the review surface of every later PR.  The plan below keeps
that dependency chain short enough for review and correctness reasoning.

## Keep and finish

### PR #46: relation read path and `redo_page_asof`

Keep this PR as the base of the route.

Remaining work should focus on correctness of relation-page materialization:

- locate the newest usable base full-page image before applying any history cap;
- make wal-redo helper writes interruptible, not just reads;
- sort ancestry WAL records by the replay order required by timeline and LSN;
- keep helper lifetime cleanup robust under errors and interrupts.

Mergeability criterion:

- a relation-page read at a target LSN either returns the correct as-of page or
  fails closed;
- hot pages with long history remain usable when a recent FPI bounds the replay
  chain;
- branch ancestry does not change replay order incorrectly;
- query cancel and statement timeout cannot strand helper processes or locks.

### PR #47: object-class discriminator in `PsKey`

Keep this PR.

The review feedback here points to implementation completeness rather than a
wrong direction.  The `klass` seam is necessary if relation pages, SLRU pages,
pg_control, and future object classes share storage infrastructure without key
collisions.

Mergeability criterion:

- `klass` participates in all key comparisons and lookups;
- manifest encode/decode preserves `klass`;
- on-disk and shared-memory format changes are gated or rejected safely;
- non-relation object writes use a real monotonic version, not arbitrary payload
  bytes.

### PR #48: per-shard and map locking

Keep this PR, but do not let it hide synchronization exceptions.

Remaining work:

- object writes that walk or update timeline state must take the map lock;
- `IMMEDSYNC` must remain serialized with shard writes, including SPDK storage;
- shard selection must use the final request key and class.

Mergeability criterion:

- single-shard and multi-shard behavior are equivalent for correctness;
- immediate sync cannot race with shard-local writes;
- object requests are routed and locked by their actual key class.

### PR #51: drop/truncate liveness

Keep this PR.

Current review found no major issues.  This PR is part of making
`redo_page_asof` fail correctly when a relation block was dropped or truncated.

Mergeability criterion:

- liveness checks preserve relation-page as-of semantics;
- unreadable WAL ranges do not silently imply that a block is still live.

### PR #52: timeline-tagged, LSN-ordered `walidx_get`

Keep this PR.

Current review found no major issues.  This PR directly supports the core route:
relation-page redo needs timeline-aware records returned in a replay-safe order.

Mergeability criterion:

- returned WAL records are tagged with their source timeline;
- records across ancestry are ordered for replay, not merely grouped by ancestry;
- callers can distinguish parent and branch records when applying liveness or
  source-specific WAL reads.

## Freeze and redesign

### PR #49: SLRU on the store

Keep as draft.  Do not continue treating this as a mergeable production path
until the SLRU lifecycle is designed explicitly.

Reason:

The review feedback is not concentrated in one function.  It points to missing
SLRU object semantics across the lifecycle:

- write mirroring must not depend on unstable buffers or arbitrary payload LSNs;
- critical-section SLRU writes cannot perform fallible store I/O, but dropping
  the mirror can make the store permanently stale;
- truncation and segment deletion need durable tombstone semantics;
- existence probes such as `SimpleLruDoesPhysicalPageExist()` must consult the
  same source of truth as reads;
- cached local SLRU pages must be revalidated before store-backed reads trust
  old copies;
- hook error paths must not rethrow while SLRU slots or bank locks are in states
  that require core cleanup.

Required design before revival:

- define SLRU object identity and version ordering;
- define write, truncate, and existence semantics together;
- define whether mirroring is synchronous, deferred, or log-shipped;
- define how critical-section writes are queued without losing durability;
- define cache invalidation and stale local page handling;
- define interrupt behavior for hooks called while core SLRU locks or slots are
  in transitional states.

Acceptable near-term scope:

- leave mirror-only or read-from-store code behind an experimental flag;
- keep tests that demonstrate known limitations;
- avoid presenting store-backed SLRU as complete until truncate, existence, and
  critical-section semantics are implemented.

### PR #50: pg_control on the store

Keep as draft.  Do not treat the current hook-based pg_control mirroring as a
mergeable production path.

Reason:

pg_control is on a conservative recovery-critical path.  The remaining feedback
is about production invariants:

- mirroring currently happens inside checkpoint or shutdown critical sections;
- mirrored pg_control durability is not fully specified;
- restore should write atomically using a temporary file and rename, not truncate
  an existing control file in place;
- restore must fsync the containing directory;
- any previously installed control-file hook must be preserved.

Required design before revival:

- move fallible mirror work out of checkpoint/shutdown critical sections;
- define a checkpoint-completion or async shipper handoff;
- define the durability contract for the mirrored control object;
- make restore atomic and directory-durable;
- define hook chaining behavior.

Acceptable near-term scope:

- keep prototype code documented as non-production;
- keep restore tooling only if it fails safely and does not claim durability
  beyond what it implements.

### PR #53: store-backed WAL reader

Keep as draft and split before revival.

Reason:

This PR currently combines too many invariants in one review surface:

- local WAL and store WAL source switching;
- WAL reader cache invalidation across source and timeline changes;
- fail-closed truncate scans when shipped WAL is incomplete;
- branch-boundary liveness semantics;
- manifest `klass` replay for non-relation objects;
- SPDK object-read foundness;
- SLRU read/write hook safety inherited from `#49`;
- pg_control/object concerns inherited from `#50`.

The relation-page part of store-backed WAL is valuable, but it should be reviewed
without SLRU/control/object lifecycle debt attached to it.

Required split:

- relation-page store-backed WAL reader;
- manifest/object foundness fixes;
- SPDK foundness contract;
- SLRU-specific store-backed reads and writes;
- pg_control-specific restore/mirror work.

Mergeability criterion for the relation-page split:

- switching between local and store WAL invalidates all relevant reader caches;
- timeline changes cannot reuse a WAL page from the wrong source;
- truncate scans fail closed if WAL cannot be read through the requested LSN;
- branch-boundary scans respect parent and branch visibility separately;
- no SLRU or pg_control production semantics are included in the same PR.

## Recommended near-term sequence

### Step 1: stabilize the mergeable base

Finish `#46`, `#47`, `#48`, `#51`, and `#52` as the narrow relation-page read
path.

The intended capability at the end of this step is:

```text
relation page + target timeline + target LSN
  -> timeline-aware WAL index
  -> bounded base FPI selection
  -> ordered WAL redo
  -> liveness/drop/truncate check
  -> correct page or fail-closed result
```

Do not include SLRU, pg_control, or broad object bootstrapping in this acceptance
scope.

### Step 2: split `#53`

Extract a small relation-page store-backed WAL reader PR.

This PR should depend only on the stable base and should not contain SLRU or
pg_control changes.  It should prove the source-switching and timeline-switching
contracts for WAL reads first.

### Step 3: write SLRU design before reviving `#49`

Before more SLRU implementation work, write down the object lifecycle:

```text
SLRU write
  -> version assignment
  -> mirror or deferred mirror
  -> truncate tombstone
  -> existence check
  -> read fallback
  -> cache revalidation
```

No single hook should be considered sufficient until the whole chain is defined.

### Step 4: write pg_control design before reviving `#50`

Before more pg_control implementation work, write down the critical-section-safe
mirror and restore protocol:

```text
control file update
  -> critical-section-safe handoff
  -> durable mirror object
  -> atomic restore file creation
  -> directory fsync
  -> hook chaining
```

### Step 5: reintroduce object bootstrapping one object class at a time

After relation-page store-backed WAL is correct, reintroduce object classes in
separate PRs:

- SLRU;
- pg_control;
- future object classes.

Each object class should have its own lifecycle, fallback, existence, truncation,
and durability rules.

## Non-goals for the current mergeable path

The current mergeable path should not attempt to solve:

- complete SLRU bootstrapping from store;
- pg_control production restore;
- object-storage tiering or remote range reads;
- SPDK object-read foundness beyond the contracts required by included code;
- all object classes sharing one generic read hook without per-class lifecycle
  semantics.

## Review interpretation rule

A review thread should be treated as architecture-level feedback, not a local
bug, when fixing it requires defining behavior across more than one of these
boundaries:

- WAL source and timeline;
- object version ordering;
- truncation or tombstone semantics;
- existence checks;
- critical-section behavior;
- PostgreSQL lock or slot cleanup;
- durability or crash recovery;
- local cache freshness;
- SPDK and POSIX backend contract parity.

`#49`, `#50`, and the broad form of `#53` crossed several of these boundaries at
once.  That is why they should remain draft until their scope is split and their
invariants are documented.
