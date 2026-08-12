# Pagestore MVP completion plan

This document is the execution plan for closing the remaining pagestore MVP
gates.  [`MVP_STATUS.md`](MVP_STATUS.md) remains the source of truth for current
status and MVP scope; this document records the ordered work packages,
dependencies, acceptance criteria, and decisions that still need agreement.

Update this plan when a decision is made, a pull request lands, or evidence
changes an acceptance criterion.  Do not mark a work package complete merely
because its implementation PR merged: its listed evidence must pass on the
`pagestore` branch.

## Baseline

Baseline as of 2026-08-12:

- PRs #174 and #175 are merged into `pagestore`.
- The local POSIX golden path is green: WAL-only writer -> continuous
  materializer -> durable fork -> independent branch -> process restarts.
- Managed materializer lifecycle and serialized portable branch bootstrap are
  implemented for the default-tablespace local POSIX topology.
- The durable retention registry and effective per-resource floor are
  implemented, but controllers and reclaimers do not consume them yet.
- Focused crash tests exist, but the composed fault and persisted-format
  compatibility gates remain open.

Three of the five MVP gates are therefore complete.  The two remaining gates
are:

1. retention-driven, bounded space reclamation;
2. composed crash recovery and persisted-format compatibility.

## Completion definition

The MVP is complete when all of the following are true on `pagestore`:

- the existing composed golden scenario remains green;
- every running fixed/advancing reader and materializer protects the exact
  resources and LSNs it may still consume;
- page history, shipped WAL, and WAL-index history are reclaimed without
  crossing the effective retention floor;
- a deleted timeline is durably and idempotently removed with all of its local
  data, subject to ancestry and owner checks;
- a bounded long-running workload reaches a bounded steady-state disk size;
- process crashes at declared materializer, branch, manifest/layer, and GC
  transitions recover to a valid old or new state, never a torn state;
- persisted-format fixtures prove supported reopen/upgrade paths and fail
  closed on unsupported or corrupt formats;
- the complete pagestore CI suite and the new bounded-space/crash/compatibility
  lanes are green.

S3/Lambda, SPDK layer recovery, multi-writer timelines, user-tablespace portable
bootstrap, and production performance targets remain outside this MVP.

## Dependency order

```text
R0 retention localsvc API
  -> R1 controller owner lifecycle
       -> R2 page-version pruning
       -> R3a segmented-WAL format
       -> R4 WAL-index reclamation/replacement bases
            -> R3b shipped-WAL reclamation acceptance
       -> R4b fork-metadata compaction
       -> R5 timeline deletion
            -> R6 bounded-space acceptance

H0 harness fault/inspection primitives
  -> H1 composed crash scenarios
  -> H2 persisted-format compatibility lane

R2-R5, including R4b, feed GC scenarios in H1.  R6 and H2 close the two MVP
gates.
```

R0 and H0 may proceed independently.  Reclaimers must not be enabled before R1
has made every in-scope runtime owner visible to the retention authority.
R3a may land before R4, but R3b and its bounded-WAL acceptance gate require
R4's replacement bases to have removed the oldest raw-WAL dependencies.

## Retention and reclamation work

### R0. Land the localsvc retention-owner API

Status: **not started on `pagestore`**.

The previously reviewed stacked PR #171 did not reach the final `pagestore`
history.  Port its current-state equivalent rather than merging the stale
stacked branch.

Deliverables:

- `pagestore_localsvc_retention_set()` and owner lookup/enumeration with the
  exact `(LSN, admission_sequence)` fence plus explicit owner generation;
- `pagestore_localsvc_retention_drop()` with explicit owner generation;
- backend declarations and protocol-field documentation;
- tests for successful set/drop, idempotent drop, invalid owner/resource input,
  daemon rejection, and reconnect/restart behavior;
- durable, non-enumerable owner tombstones that retain the maximum accepted
  generation after DROP, including across log compaction and restart.
- persisted per-resource reclamation frontiers.  SET admission and reclaimer
  cutoff selection share one synchronization protocol: a SET below any
  requested `(LSN, admission_sequence)` resource frontier is rejected, while
  an accepted SET is visible before a reclaimer can select a conflicting
  cutoff.  The record/recovery format and lifecycle tests preserve the
  sequence through append, enumeration, compaction, and restart.

Acceptance:

- the backend carries the controller-assigned generation and exact admission
  sequence on every durable SET, returns both through lookup, and reports a
  stale-generation rejection distinctly;
- failures are reported without pretending the pin was installed or removed;
- a delayed SET or DROP below the tombstone generation is rejected, and a
  same-generation SET cannot resurrect a dropped owner; tests cover both
  orderings before and after restart/compaction;
- standalone, PostgreSQL integration, and retention recovery tests pass.

Expected scope: one PR.

### R1. Register reader and materializer owner generations

Status: **blocked on R0 and decision D2**.

Deliverables:

- stable owner identity and monotonically replaceable generation for each
  managed materializer and fixed/advancing reader;
- registration before a process can consume retained history;
- atomic advancing-reader handoff: prepare and validate the newer view while
  the old pin remains active, then prevent every old-view request while the
  durable pin advances and the runtime switches views (or use an equivalent
  protocol with the same no-gap property);
- pins survive ordinary process shutdown and restart.  Release happens only
  during authoritative deprovisioning or after a durable handoff to another
  owner that protects an equal-or-older safe horizon;
- supervisor handoff/restart behavior that never creates an unprotected window;
- status/inspection output that identifies active and stale owners.

Safety rule: uncertainty keeps data.  An ambiguous SET/DROP must either leave
the old pin/view pair usable or fail closed until a later authoritative
reconciliation proves which durable owner state won.

Acceptance:

- process start, handoff, advancement, clean stop, crash, daemon restart, and
  duplicate-owner tests cover each lifecycle transition;
- no runtime can serve or redo at LSN `R` unless its required resource masks are
  protected at or below `R`;
- a registration racing page/WAL/index reclamation either installs before
  cutoff selection or is rejected below the already durable frontier; it can
  never report protection for reclaimed history;
- a stale owner can be identified and explicitly reconciled without wall-clock
  expiry changing correctness.

Expected scope: two PRs, materializer then reader.

### R2. Prune page versions during image compaction

Status: **not started; blocked on R1**.

Compaction currently rewrites image layers while retaining every historical
version.  It must consume the page-history effective floor.

An effective floor of zero means that no retention owner constrains page
history; it is not a literal LSN cutoff.  In that case the GC cutoff is the
latest horizon proven durable and materialized for the timeline.  Compaction
must fail closed if no such horizon has been established.  All page-history
pins and reclamation frontiers are ordered `(LSN, admission_sequence)` fences,
not bare LSNs: for every retained fence compaction preserves the newest version
not later than that exact fence.  This keeps the version visible before a
same-LSN hint rewrite as well as the version visible after it.

Branch points are discrete structural base requirements, not moving retention
floors.  A child at fork fence `F` requires the parent base visible at `F`, but
does not pin every later parent version.  The parent's operational GC cutoff
may continue to advance while compaction retains those discrete bases for all
live descendants.

Branch creation participates in the same cutoff-selection fence as owner SET.
It validates the requested `(LSN, admission_sequence)` against the durable
page, WAL, WAL-index, and forkmeta frontiers before publishing the child; a
frontier already beyond any required base rejects the branch.  Control-object
versions are protected independently by the WAL floor: compaction retains the
newest usable control image and redo-floor note at or below every retained WAL
boundary even when the page-history floor is newer.

Deliverables:

- a precise keep/drop rule that retains the newest required base version at or
  below the floor and every version required above it;
- descendant pins projected through each fork cap;
- structural branch ancestry preserved independently of explicit owners;
- install-new-before-delete-old manifest transition;
- durable publication of the full `(LSN, admission_sequence)` page reclamation
  frontier before any pruned source image layer is marked deleting or removed;
  recovery must therefore reject a below-frontier SET or branch even after a
  crash at every replacement/frontier/source-retirement boundary;
- publication of the replacement to every durability tier represented by its
  sources before deletion begins there (or retention of the old remote copies
  until that publication is durable);
- idempotent local and remote deletion retries;
- crash-safe page-log reclamation after the pruned replacement and frontier are
  durable: recovery ignores dropped references, mixed segments are rewritten
  or retained until every live record is covered, and only then are source
  segments removed;
- pruning statistics and inspection output.

Acceptance:

- reads at every retained horizon agree before and after compaction;
- reads below the declared floor may be rejected, but never return a wrong
  version;
- branch divergence, relation truncate/drop/recreate, restart, and injected GC
  failures preserve the rule;
- repeated update/compact cycles stop growing retained page history.

Expected scope: one implementation PR and, if needed, one fault-test PR.

### R3. Reclaim shipped WAL

Status: **design decision required; R3a is blocked on R1 and D3; R3b
acceptance is additionally blocked on the R4 replacement-base milestone**.

The current flat `wal_<timeline>` file does not support simple crash-safe prefix
deletion.  WAL reclamation must first gain a durable physical base LSN and a
layout that can remove an old prefix without rewriting an unbounded file under
the serve path.

Deliverables:

- segmented WAL storage or another agreed crash-safe prefix-reclaim format;
- persisted base/end metadata with checksum and reopen validation;
- append/read across physical segment boundaries and branch ancestry;
- reclamation driven by the WAL effective floor;
- a read-lifetime pin/reference for every selected physical WAL segment, with
  unlink deferred until existing readers drain (or an equivalent epoch/barrier),
  including a concurrent read-versus-reclaim fault test;
- explicit protection for restorable control images and in-progress WAL-index
  scanning;
- migration or fail-closed handling for the existing flat format.

With no owner floor, the WAL cutoff is the newest restart/recovery boundary
whose control image and required WAL are durably published.  It is independent
of discrete ancestor WAL bases required by live branches.  Reclamation fails
closed until that boundary is proven, and persists its resulting per-timeline
frontier so later pins and branches below it are rejected.

The WAL cutoff also includes the oldest raw WAL record referenced by every
surviving WAL-index reconstruction chain.  R3 cannot cross an indexed FPI/base
until R4 has durably published an equivalent replacement page base and removed
that WAL dependency; index progress by itself never authorizes WAL deletion.

Acceptance:

- WAL read/restore results are identical before and after reclaim at every
  retained LSN;
- reclaim never crosses a control, branch, reader, materializer, or indexing
  requirement;
- crash at create, fsync, publish, and unlink boundaries reopens safely;
- a continuous WAL-only workload reaches bounded WAL disk usage.

Expected scope: two or three PRs (format, reclaimer, crash/migration coverage),
coordinated or stacked with R4 where the acceptance criteria overlap.

### R4. Compact and reclaim the WAL index

Status: **not started; blocked on R1**.

Deliverables:

- compacted per-(timeline, shard) durable index representation;
- removal of entries strictly below the WAL-index effective floor while
  retaining the necessary base/read boundary;
- atomic publication and old-log deletion;
- bounded startup replay and compaction scheduling off serve threads.

With no owner floor, the WAL-index cutoff is the latest completely indexed and
durably published WAL horizon for which every retained page also has a durable
reconstruction base.  Index progress alone is not a reclamation proof: for each
page, compaction retains its required FPI/base entry and every redo entry from
that base through each retained horizon.  Discrete branch-point lookup bases
are retained separately.  Compaction fails closed without that proof and
persists the per-(timeline, shard) reclaimed frontier before removing entries,
so a later registration below it cannot be admitted.

Index append and compaction publication share a cutover protocol.  The
compactor freezes an append sequence under the shard append lock, publishes a
replacement through that sequence, then hands off and durably appends any tail
before replacing the old log.  This includes the shard-0 durable progress
record; acknowledged concurrent appends can never be omitted by publication.

Acceptance:

- retained `redo_page_asof` results match before and after compaction;
- incomplete final records, complete corruption, interrupted publication, and
  daemon restart are covered, including concurrent appends at every publication
  crash boundary;
- repeated WAL indexing and compaction reaches bounded index size.

Expected scope: one or two PRs.

### R4b. Compact and reclaim fork metadata

Status: **not started; blocked on R1 and R2**.

The shared append-only `forkmeta` stream reconstructs historical relation
existence and size, so it is retained with page history rather than treated as
current-state-only metadata.

Deliverables:

- compaction against a proven forkmeta GC cutoff: the effective owner fence, or
  (when it is unconstrained) the latest durably materialized timeline fence;
  fail closed when neither is available and persist the reclaimed frontier;
- discrete descendant fork fences retained as required historical bases rather
  than projected as a moving floor that pins all later parent metadata;
- for each relation incarnation and each retained owner/branch fence, the
  definitive create/size/existence base visible at that `(LSN,
  admission_sequence)` fence, plus every event required above the operational
  cutoff;
- preservation of same-LSN admission ordering needed by retained reader
  fences;
- bounded replay from an atomically published checkpoint plus tail, with the
  old log removed only after the replacement and directory entry are durable.
- an append cutover barrier or sequence handoff: publication freezes a precise
  input sequence, and every concurrent create/extend/truncate/drop/recreate is
  either included in the replacement tail or redirected to the new log before
  publication becomes visible; no append may fall between snapshots.

Acceptance:

- relation existence and size at every retained horizon match before and after
  compaction;
- crashes before replacement publication, after publication, and during old
  log removal reopen to either complete old or complete new state;
- concurrent metadata mutations at every publication/crash boundary reopen
  with every acknowledged event exactly once;
- H1 exercises each publication boundary before R6 begins its soak.

Expected scope: one implementation PR and one crash-test PR.

### R5. Delete timelines durably

Status: **not started; blocked on R1-R4b and decision D4**.

Deliverables:

- an atomic durable transition from live to deleting that first fences all new
  owner registrations and page/WAL/timeline operations, then drains every
  already-admitted operation before physical cleanup;
- deletion admission checks for descendants, every non-dropped durable owner
  (whether its process is active or offline), and structural retention
  requirements performed as part of that fenced transition; only an
  authoritative DROP or completed safe handoff removes an owner's veto;
- durable timeline tombstone/state transition carrying a monotonically
  increasing timeline incarnation generation;
- idempotent removal of manifests, layers, page segments, WAL, WAL index,
  control/SLRU metadata, fork metadata, and object-tier copies;
- restart resumes deletion and never resurrects a deleted timeline;
- timeline-ID reuse is permitted only after cleanup is durable and only with a
  higher incarnation generation carried by every request and durable record;
  delayed metadata, retention mutations, and requests from an older
  incarnation are rejected;
- retention owner IDs carry a controller-assigned, monotonically increasing
  owner incarnation (independent of the per-owner generation).  Authoritative
  DROP durably closes that incarnation; after every operation admitted under it
  has drained, its generation tombstone may be folded into a bounded durable
  per-controller allocation frontier even while the timeline remains live.
  Reuse requires a higher owner incarnation, and delayed mutations for a folded
  incarnation are rejected by that frontier.  Deleting a timeline incarnation
  may reclaim all of its owner records while retaining the timeline-incarnation
  fence;
- crash-safe checkpoint/compaction of the shared timeline-state log, retaining
  each slot's current state and maximum incarnation while bounding startup
  replay;
- a timeline-state append cutover lock or frozen-sequence plus tail-handoff
  protocol, so concurrent create/delete/incarnation events acknowledged during
  checkpoint publication are present exactly once in either the replacement or
  its durable tail;
- inspection reports pending and failed cleanup.

Acceptance:

- unsafe deletes fail before mutation;
- every fault boundary leaves the timeline either fully readable or durably
  deleting/deleted;
- repeated delete requests are idempotent;
- repeated delete/recreate cycles reuse the bounded ID space without aliasing
  an older incarnation, including across daemon restart;
- repeated provision/drop churn on one long-lived timeline keeps owner metadata
  bounded and rejects delayed SET/DROP from every folded owner incarnation;
- concurrent timeline transitions at every state-log publication crash boundary
  reopen with no lost state or incarnation event;
- deleting a branch does not affect its parent or siblings.

Expected scope: one or two PRs.

### R6. Prove bounded space

Status: **blocked on R2-R5**.

Add a deterministic soak scenario with bounded live data but repeated updates,
WAL generation, compaction, reader advancement, branch creation/deletion, and
daemon restart.

Acceptance:

- page history, shipped WAL, WAL index, and deleted-timeline debris each remain
  within a declared bound while the workload continues;
- the append-only shared `forkmeta` log is compacted/reclaimed and its physical
  bytes are included in the declared bound;
- retention owner/tombstone metadata and the timeline-state log remain within
  declared bounds across repeated owner and timeline incarnations;
- each reclaimer has a declared maximum lag/catch-up interval, and controller
  backpressure bounds foreground admission when maintenance exceeds it;
- the report includes logical live bytes, physical bytes, write amplification,
  per-category GC lag/catch-up time, backpressure time, and active retention
  owners;
- retained SQL-visible state remains correct throughout the run.

Expected scope: one PR.  Passing it closes the retention MVP gate.

## Crash and compatibility work

### H0. Add common fault and inspection primitives

Status: **partial harness exists**.

Deliverables:

- one test-only named fault registry with crash/error/pause actions and hit
  counts;
- reachability accounting so an unhit expected fault fails the test;
- harness timeouts, replay metadata, and diagnostic bundles;
- read-only inspection for timeline, manifest/layer, retention/GC, and owner
  state needed by recovery assertions.

Acceptance:

- faults do not add durability edges or alter production behavior when disabled;
- every run reports scenario, seed, fault, hit count, and operation identity;
- paused faults have a watchdog and actionable diagnostics.

Expected scope: one or two PRs.

### H1. Compose process-level crash scenarios

Status: **blocked partly on H0; GC cases also depend on R2-R5**.

Required scenario families:

- materializer replay/restartpoint/durable-marker publication;
- branch prepare, receipt publication, bootstrap install, and service restore;
- manifest replacement and image-layer seal/publication;
- page pruning, WAL reclaim, WAL-index compaction, and timeline deletion;
- daemon, writer, materializer, and branch-compute restart combinations.

Acceptance:

- each declared transition is exercised before and after its durability point;
- recovery yields a valid complete old or new state;
- direct and recovered SQL-visible results agree at declared horizons.

Expected scope: two or three focused PRs.

### H2. Add persisted-format fixtures and compatibility CI

Status: **not started; decision D5 required**.

Fixture families:

- timeline metadata and retention registry;
- manifest and image/delta layer headers/indexes;
- page segments, shipped WAL metadata, and WAL index;
- control, SLRU, reader, and branch-bootstrap artifacts.

Acceptance:

- supported old fixtures reopen or upgrade to the documented state;
- unsupported newer/unknown versions fail closed with an actionable error;
- checksum corruption and illegal truncation are rejected;
- every persisted-format change must update or add a fixture.

Expected scope: one or two PRs.  Passing it with H1 closes the crash/compatibility
MVP gate.

## Work that follows the MVP

These items matter, but should not delay the two remaining MVP gates unless
measurement proves they block the acceptance scenarios:

- user-tablespace portable branch bootstrap;
- materializer PGDATA provisioning and service-manager integration;
- object-tier cache budget/residency and orphan reconciliation;
- production S3 provider;
- sparse image indexes and a separate layer-block cache;
- immutable WAL delta sealing and bounded redo-chain compaction;
- cost-aware materialized-page cache admission and proven redo avoidance;
- size-tiered/incremental compaction, key-range pruning, and bloom filters;
- per-shard manifest/layer map/cache and replicated timeline metadata;
- asynchronous POSIX I/O and explicit CPU/IO scheduling;
- SPDK image-layer recovery/GC;
- production backup/restore, observability, alerting, and repair procedures.

## Decisions for discussion

Record the selected answer and rationale here before implementing the dependent
work.

### D1. Owner identity authority

Selected: deployment/controller-assigned stable 64-bit owner ID plus a
monotonic generation stored in controller state and carried on every durable
SET/DROP.  The retention registry stores the maximum generation, including as
a durable non-enumerable tombstone after DROP, and rejects stale or
same-generation resurrection.  Delayed cleanup from an old supervisor therefore
cannot remove or resurrect its replacement's pin.  A replacement supervisor
updates the same owner key rather than adding another logical owner.

Alternative: derive identity from PGDATA or reader artifact.  This is easier to
bootstrap but makes cloning and deliberate replacement ambiguous.

Decision: **accepted**.

### D2. Owner release and stale-owner policy

Selected: ordinary process shutdown retains the durable pin.  Release requires
explicit authoritative deprovisioning or a safe durable horizon handoff;
generation replacement alone must quiesce the old runtime before its pin is
superseded.  Never use wall-clock lease expiry for correctness.  Stale owners
retain space until controller/operator reconciliation proves them dead.

Decision: **accepted**.

### D3. Shipped-WAL physical layout

Recommended: immutable fixed-size logical WAL segment files plus small durable
timeline metadata recording the retained base and append end.  Reclaim deletes
whole old files and retains the boundary file when needed.

Alternative: periodically rewrite the flat file.  It minimizes format count but
causes unbounded copy cost and a larger crash-publication protocol.

Decision: **open**.

### D4. Timeline deletion with descendants

Recommended for MVP: reject deletion while any descendant exists.  Do not add
cascade or ancestry reparenting semantics.

Decision: **open**.

### D5. Persisted-format support window

Recommended: preserve fixtures for every format shipped on `pagestore` after the
MVP format baseline; require explicit migration for supported older versions and
fail closed otherwise.  Release branches can define a narrower cross-major
policy separately.

Decision: **open**.

### D6. MVP deployment boundary

Recommended: keep default-tablespace local POSIX as the MVP boundary.  Treat
user tablespaces, S3, SPDK layers, and service-manager packaging as follow-up
work.

Decision: **open**.

## Proposed PR sequence

The default sequence is:

1. R0 localsvc retention API;
2. R1a materializer owner lifecycle;
3. R1b reader owner lifecycle;
4. R2 page-version pruning;
5. R3a segmented WAL format and compatibility;
6. R4 WAL-index compaction/reclamation and replacement bases;
7. R3b WAL reclaimer, enabled after those raw-WAL dependencies are removed;
8. R4b forkmeta compaction/reclamation and publication crash tests;
9. R5 timeline deletion;
10. H0 fault/inspection primitives (may start in parallel with R0-R1);
11. H1 composed crash scenarios;
12. H2 format fixtures and compatibility CI;
13. R6 bounded-space acceptance and final MVP status update.

Keep each PR independently reviewable and keep the existing standalone and
golden suites green.  If work packages depend on one another before their base
lands, use stacked PRs and finish with an explicit roll-up PR to `pagestore`.

## Progress log

| Date | Change | Evidence |
|---|---|---|
| 2026-08-12 | Established completion plan after PRs #174 and #175 landed | Existing pagestore CI green; remaining gates from `MVP_STATUS.md` |
