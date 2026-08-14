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

Baseline as of 2026-08-14:

- PRs #174-#191 have completed their stacked review flow.  The reclamation
  roll-up lands their aggregate on `pagestore`; this includes retention-owner
  lifecycle, page-version pruning, bounded page-history churn, and immutable
  WAL segment/store primitives.
- The local POSIX golden path is green: WAL-only writer -> continuous
  materializer -> durable fork -> independent branch -> process restarts.
- Managed materializer lifecycle and serialized portable branch bootstrap are
  implemented for the default-tablespace local POSIX topology.
- The durable retention registry is consumed by reader and materializer
  controllers.  Image compaction consumes its exact page-history fences and
  publishes a durable reclamation frontier before retiring sources.
- Repeated update/compact cycles bound retained page history.  The live WAL
  path seals validated 1 MiB immutable segments while retaining its flat log
  as the migration/tail authority.  Shipped WAL, WAL-index history, fork
  metadata, and deleted timelines remain unbounded.
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

Status: **implemented for durable owners and page-history admission; the WAL,
WAL-index, and forkmeta resource frontiers remain coupled to R3/R4/R4b**.

Deliverables:

- `pagestore_localsvc_retention_set()` and owner lookup/enumeration with the
  exact `(LSN, admission_sequence)` fence plus explicit owner generation;
- `pagestore_localsvc_retention_drop()` with explicit owner generation;
- backend declarations and protocol-field documentation;
- tests for successful set/drop, idempotent drop, invalid owner/resource input,
  daemon rejection, and reconnect/restart behavior;
- durable, non-enumerable owner tombstones that retain the maximum accepted
  generation after DROP, including across log compaction and restart.
- persisted per-resource reclamation cutoffs plus exact retained-base
  exceptions.  SET admission and reclaimer
  cutoff selection share one synchronization protocol: a SET below any
  requested `(LSN, admission_sequence)` resource frontier is rejected, while
  an accepted SET is visible before a reclaimer can select a conflicting
  cutoff.  The record/recovery format and lifecycle tests preserve the
  sequence through append, enumeration, compaction, and restart.
- fork metadata shares the page-history reclamation frontier: its compactor
  advances that same full tuple atomically before retiring events, and every
  page-history SET is checked against the maximum of page-image and forkmeta
  progress.  Retention owners declare whether each resource is a point-in-time
  base consumer or a range/replay consumer.  A point-in-time owner may retain
  a sparse fixed-reader base `F` while its
  operational cutoff advances to `C > F`: admission accepts `F` and tuples at
  or above `C`, but rejects reclaimed tuples in `(F, C)`.  The durable cutoff
  and exact exception set are updated crash-atomically and consulted by page,
  WAL, WAL-index, and forkmeta admission; tests cover restart and exception
  removal after the owning reader drains.  Materializers and other range
  consumers must instead declare their full required interval; admission
  rejects them unless that interval is continuously retained and never treats
  equality with a discrete reader base as sufficient proof.
- the R0 wire/API and durable owner record carry a controller allocation-domain
  token distinct from the per-owner generation, plus durable live-token
  reservation.  R1 clients must reserve and transmit it from their first
  deployment; older records are upgraded to explicit live exceptions before
  any R5 allocation-domain frontier may advance.
- recovery resumes the global admission-sequence allocator strictly above the
  maximum sequence in every durable pin, reclamation frontier, branch fence,
  page/forkmeta record, and other sequence-bearing state before admitting a
  mutation; alternatively, allocation advances a durable high-water mark
  before returning a sequence.

Acceptance:

- the backend carries the controller-assigned generation and exact admission
  sequence on every durable SET, returns both through lookup, and reports a
  stale-generation rejection distinctly;
- failures are reported without pretending the pin was installed or removed;
- a delayed SET or DROP below the tombstone generation is rejected, and a
  same-generation SET cannot resurrect a dropped owner; tests cover both
  orderings before and after restart/compaction;
- callers can distinguish `PS_STATUS_STALE` from a general daemon error;
- standalone, PostgreSQL integration, and retention recovery tests pass.

Expected scope: one PR.

### R1. Register reader and materializer owner generations

Status: **implemented for managed materializers and fixed/advancing readers;
decision D2 is accepted below**.

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
- authoritative deprovisioning first fences new operations for that owner and
  drains every already-admitted page, WAL, and index operation (or retains an
  equivalent per-operation pin) before durably dropping the owner pin;
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

Status: **implemented in the reclamation roll-up**.

Image compaction consumes exact page-history fences, retains discrete reader
and descendant bases, publishes the durable frontier before source retirement,
and fails closed below reclaimed history.

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
and fixed-reader pins follow the same discrete-base rule: each fixed fence
retains the state visible at that fence while the operational frontier may
advance past it.  The bounded-space soak keeps a fixed reader alive while its
timeline receives continuing updates and verifies bounded page, forkmeta,
WAL-index, and raw-WAL storage.
The operational frontier may continue to advance while compaction retains
those discrete bases for all live descendants.  Durable timeline metadata stores the complete fork tuple
`(branch_lsn, branch_admission_sequence)`, not a bare LSN; restart, ancestor
reads, page/forkmeta visibility, and compaction all apply that tuple so a
same-LSN post-fork mutation cannot enter the child.

Branch creation participates in the same cutoff-selection fence as owner SET.
It validates the requested `(LSN, admission_sequence)` against the durable
page, WAL, WAL-index, and forkmeta frontiers before publishing the child; a
frontier already beyond any required base rejects the branch.  Control-object
versions are protected independently by the WAL floor: compaction retains the
newest usable control image and redo-floor note at or below every retained WAL
boundary even when the page-history floor is newer.
If SLRU capture yields a replay base `C` earlier than branch fence `L`, branch
preparation validates and temporarily pins both WAL and page history at `C`
before seeding: WAL protects the replay stream, while page history protects the
exact SLRU image that seeding must resolve at `C`.  Both protections remain
until the branch artifact and its structural retention are durable.
Temporary pins belong to a durable branch-preparation operation ID with
`preparing -> committed|aborting -> complete` transitions.  Success converts
them atomically to structural retention; abort first fences the operation and
drains admitted seeding, then authoritatively drops both pins.  Restart resumes
either transition idempotently, so an abandoned preparation cannot leave an
unbounded orphan.  Fault tests stop before and after every state publication,
pin SET/DROP, seeding drain, and structural handoff.

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

Status: **R3a immutable segment/store primitives and live-path integration and
the R3b crash-atomic flat-log prefix-rewrite primitive are implemented; durable
retained-base publication and reclamation policy remain**.

The transition path accepts the existing arbitrary-size archive IPC chunks in
the flat staging log, seals every complete contiguous segment-aligned 1 MiB
logical range into an immutable segment, and prefers validated segments for
reads.  Startup reopens the segment catalog, removes only strictly recognized
staging orphans,
validates headers and bounded payload chunks, compares the sealed prefix with
the still-authoritative flat history, and seals any complete tail missed by a
pre-publication crash.  R3b may make retained-base metadata authoritative and
reclaim flat prefixes only after R4 removes their remaining raw-WAL
dependencies.

Each flat `wal_<timeline>` record is self-describing, so the POSIX backend can
now copy a retained suffix from a complete record boundary, fsync it, and
atomically replace the old log while serializing physical append/truncate
publication.  A crash before rename leaves the old file authoritative and a
retry replaces any staging orphan; after rename, directory fsync makes the new
suffix durable.  The core must still durably publish the logical retained-base
frontier first, freeze its WAL catalog across cutover, translate in-memory file
offsets, and then use this primitive.  This rewrite is an MVP transition path;
bounded steady-state operation should move reclamation onto immutable segment
lifecycle so it does not repeatedly copy an unbounded live suffix.

Deliverables:

- segmented WAL storage or another agreed crash-safe prefix-reclaim format;
- persisted base/end metadata with checksum and reopen validation;
- WAL append and base/end replacement share a cutover lock (or frozen sequence
  plus durable tail handoff), so an append acknowledged during reclamation is
  represented exactly once in the replacement metadata or its tail.  Crash
  tests overlap appends with every metadata publication boundary;
- append/read across physical segment boundaries and branch ancestry;
- reclamation driven by an independently advancing operational WAL cutoff;
  fixed-reader and branch fences retain only their discrete replacement bases
  and do not pin every later WAL record;
- the authoritative per-timeline WAL retained-base frontier is durably
  published before any segment below it is unlinked.  SET and branch admission
  consult that same metadata; crash tests stop between frontier publication and
  each unlink and prove recovery rejects requests below the retained base;
- a read-lifetime pin/reference for every selected physical WAL segment, with
  unlink deferred until existing readers drain (or an equivalent epoch/barrier),
  including a concurrent read-versus-reclaim fault test;
- explicit protection for restorable control images, in-progress WAL-index
  scanning, and the durable WAL-index resume position even while no scan is
  running.  Reclamation cannot cross the undecoded interval after that resume
  point unless durable replacement page coverage proves the entire interval is
  unnecessary;
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

Status: **implemented for the MVP POSIX path**.  New records durably carry
known/FPI metadata and decoded record-end LSNs; legacy records remain unknown
and conservatively unprunable.  Timeline compaction proves every page across
all configured shards, retains the union of the operational FPI-led chain,
discrete owner/branch chains, and the future tail, and falls back to a complete
snapshot if any shard cannot prove a base.  A checksummed durable timeline
frontier rejects unrepresented reads, pins, and branches after restart.

Snapshot publication now also exposes an explicit staged boundary: prepare
writes, checksum-validates, and fsyncs every immutable shard without changing
the selected manifest; commit revalidates those files and atomically selects
the generation.  R4 frontier integration can therefore publish its durable
reclamation fence between prepare and commit, matching the page-compaction
write-replacement-before-frontier-before-retirement ordering.  Live cutover
uses exactly that order, then removes discarded entries from memory only after
the compacted manifest is selected.  If a crash leaves the durable frontier
ahead of the selected snapshot, recovery serves the conservative old snapshot
but backpressures WAL-index append/progress, WAL-index pin mutation, and branch
creation until it retries the already-prepared generation; its immutable
replacement inputs therefore cannot diverge during the recovery window.

R4a publishes every per-shard snapshot as an immutable checksummed file before
atomically replacing one checksummed timeline manifest.  Recovery validates
the complete selected generation and never discovers an unpublished partial
generation by directory scan.  Identical publication retries are idempotent,
divergent retries and generation rollback fail closed, and old generations
remain reachable for reader-drain and later reclamation.  Live maintenance now
freezes append/progress and drains admitted index readers under the existing
publish lock, publishes all shard images, and records each source-log offset.
Recovery restores the selected generation and replays only the tail after those
absolute offsets.  Once a newer manifest is durable, maintenance validates it
and idempotently removes older immutable snapshot shard generations; newer
unpublished retry files are preserved.  New payloads also name one prepared,
durable log epoch per shard.  The manifest atomically selects those empty log
epochs with the snapshot, later appends land only in the selected epochs, and
maintenance removes legacy/older log epochs.  Snapshot entries are now pruned;
raw WAL reclamation remains R3b.

Deliverables:

- compacted per-(timeline, shard) durable index representation;
- removal below an independently advancing operational WAL-index cutoff while
  retaining the necessary reconstruction base at every fixed owner/branch
  fence;
- atomic publication and old-log deletion;
- bounded startup replay and compaction scheduling off serve threads.

With no owner floor, the WAL-index cutoff is the latest completely indexed and
durably published WAL horizon for which every retained page also has a durable
reconstruction base.  Index progress alone is not a reclamation proof: for each
page, compaction retains its required FPI/base entry and every redo entry from
that base through each retained horizon.  Discrete branch-point lookup bases
are retained separately.  Compaction fails closed without that proof and
persists the per-(timeline, shard) reclaimed frontier before removing entries,
so a later registration below it cannot be admitted.  Timeline-level SET and
branch admission atomically aggregate these frontiers and reject below the
maximum reclaimed tuple across every relevant shard; a lagging shard cannot
mask missing history on a more advanced shard while frontiers move.

Index append and compaction publication share a cutover protocol.  The
compactor freezes an append sequence under the shard append lock, publishes a
replacement through that sequence, then hands off and durably appends any tail
before replacing the old log.  All replacement shards and the shard-0 durable
progress record are named by one durable publication generation.  The complete
old generation remains reachable until every replacement shard and its exact
required-byte offsets are durable, after which one atomic generation manifest
makes the new set visible; recovery selects only a complete generation and
never combines old shards with new progress.  Acknowledged concurrent appends
can never be omitted by publication.
Publication also runs on the owning shard's run-to-completion path and drains
all admitted `walidx_get()` readers before retiring the old arrays/log.  An
equivalent epoch/reference scheme is acceptable only if old representations
remain reachable until their final reader exits; tests overlap reads with
cutover and retirement.

Acceptance:

- retained `redo_page_asof` results match before and after compaction;
- incomplete final records, complete corruption, interrupted publication, and
  daemon restart are covered, including concurrent appends at every publication
  crash boundary;
- repeated WAL indexing and compaction reaches bounded index size.

Expected scope: one or two PRs.

### R4b. Compact and reclaim fork metadata

Status: **not started; R1 and R2 prerequisites are complete**.

The shared append-only `forkmeta` stream reconstructs historical relation
existence and size, so it is retained with page history rather than treated as
current-state-only metadata.

Deliverables:

- compaction against an independently advancing operational forkmeta cutoff;
  each owner/branch fence below it retains only the exact visible base tuple.
  With no proven operational cutoff compaction fails closed and does not use a
  fixed owner's minimum as the moving cutoff;
- discrete descendant fork fences retained as required historical bases rather
  than projected as a moving floor that pins all later parent metadata;
- for each relation incarnation and each retained owner/branch fence, the
  definitive create/size/existence base visible at that `(LSN,
  admission_sequence)` fence, plus every event required above the operational
  cutoff;
- preservation of same-LSN admission ordering needed by retained reader
  fences;
- restartpoint/materialization publication captures and durably stores a full
  `(LSN, admission_sequence)` barrier; an LSN-only marker never authorizes a
  page or forkmeta cutoff when same-LSN mutations can exist;
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
  log removal reopen to the complete old state only before the replacement is
  durably published and to the complete new state after publication; no fault
  during source-log removal may roll acknowledged metadata back;
- concurrent metadata mutations at every publication/crash boundary reopen
  with every acknowledged event exactly once;
- H1 exercises each publication boundary before R6 begins its soak.

Expected scope: one implementation PR and one crash-test PR.

### R5. Delete timelines durably

Status: **not started; blocked on R1-R4b and decision D4**.

Deliverables:

- an atomic durable transition from live to deleting that first fences all new
  owner registrations and page/WAL/timeline operations, then drains every
  already-admitted operation and timeline-scoped maintenance task before
  physical cleanup.  The drain includes compaction, tier upload/eviction,
  remote GC, and cache/background publication, so none can recreate an
  artifact after cleanup;
- deletion admission checks for descendants, every non-dropped durable owner
  (whether its process is active or offline), and structural retention
  requirements performed as part of that fenced transition; only an
  authoritative DROP or completed safe handoff removes an owner's veto;
- durable timeline tombstone/state transition carrying a monotonically
  increasing timeline incarnation generation;
- idempotent removal of manifests, layers, page segments, WAL, WAL index,
  control/SLRU metadata, fork metadata, and object-tier copies;
- remote uploads use a durable operation identity or upload-intent record
  published before object creation.  Startup and deletion reconcile every
  completed-but-unpublished upload, so a crash between `upload_layer()` and
  manifest publication cannot leave an undiscoverable object or collide with
  a reused layer identity;
- shard page segments shared with live timelines are reclaimed only by a
  crash-safe filtered rewrite plus durable coverage transition; deletion never
  unlinks a mixed source segment and never permits ID reuse while an
  old-incarnation record remains recoverable from it;
- the shared `forkmeta` log is reclaimed by an R4b-style crash-safe filtered
  checkpoint/tail rewrite that removes only the deleted incarnation; deletion
  never unlinks the shared stream or permits ID reuse until the filtered
  replacement and its coverage transition are durable;
- restart resumes deletion and never resurrects a deleted timeline;
- timeline-ID reuse is permitted only after cleanup is durable and only with a
  higher incarnation generation carried by every request and durable record;
  delayed metadata, retention mutations, and requests from an older
  incarnation are rejected;
- before a numeric timeline ID becomes reusable, deletion purges every
  shard-local page/fork/WAL index and materialized-page cache entry for the old
  incarnation (or those runtime keys include the incarnation); tests reuse the
  ID immediately with identical relation keys and horizons without restarting;
- retention owner IDs carry an incarnation token allocated from one named,
  durable controller allocation domain.  Tokens are globally monotonic within
  that domain, are carried by every SET/DROP and durable owner record, and are
  never inferred from a per-owner generation.  Authoritative
  DROP durably closes that incarnation; after every operation admitted under it
  has drained, its generation tombstone may be folded into a bounded allocation-
  domain frontier even past lower live tokens.  Every live or undrained token at
  or below that frontier remains an explicit durable exception.  Allocation
  must durably reserve that exception before returning a token to its caller,
  or frontier advancement must be capped by a controller-published safe
  allocation watermark.  Each reservation carries a durable, idempotent
  allocation-operation identity before token delivery.  On retry or startup,
  the controller either returns the same token or durably closes an
  undelivered reservation after proving no caller can possess it; fault tests
  cover crashes before reservation, after reservation, and during token
  delivery so interrupted allocations cannot accumulate permanent live
  exceptions.  Exceptions persist a live/closed state: only a live
  exception admits SET/DROP, while a closed exception rejects all new mutations
  but remains until already-admitted operations drain.  Removing it then makes the
  already-advanced frontier reject delayed mutations.  Thus metadata is bounded
  by active/undrained owners rather than historical reader churn.  Reuse
  requires a higher owner incarnation.  Deleting a timeline incarnation may
  reclaim all of its owner records while retaining the timeline-incarnation
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

Status: **blocked on R2-R5 and R5b**.

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

### R5b. Add reclaimer backpressure controllers

Status: **blocked on R2-R5**.

Before the soak test, add one lean lag controller for page, WAL, WAL-index, and
forkmeta reclamation.  Each controller publishes a high-water threshold and
catch-up target, throttles foreground admission when lag exceeds the threshold,
and releases admission only after the target is reached.  Controller state and
wait time are inspectable, shard-local work remains run-to-completion, and
tests prove bounded queues under ingestion faster than maintenance.

Expected scope: one or two PRs.  R6 consumes these controls; it does not
introduce them.

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
- page pruning, WAL reclaim, WAL-index compaction, remote upload intent and
  orphan reconciliation, and timeline deletion;
- daemon, writer, materializer, and branch-compute restart combinations.

Acceptance:

- each declared transition is exercised before and after its durability point;
- recovery yields the complete old state only for crashes before durability
  and the complete new state for crashes after durability; acknowledged
  markers, deletion states, reclamation frontiers, and branch publications are
  monotonic and cannot roll back even when they are not SQL-visible;
- direct and recovered SQL-visible results agree at declared horizons.

Expected scope: two or three focused PRs.

### H2. Add persisted-format fixtures and compatibility CI

Status: **not started; decision D5 required**.

Fixture families:

- timeline metadata and retention registry;
- manifest and image/delta layer headers/indexes;
- page segments, shipped WAL metadata, and WAL index;
- control, SLRU, reader, branch-bootstrap, and fork-metadata artifacts.  The
  forkmeta checkpoint/tail format is checksummed; fixtures cover legacy reopen
  or migration, truncation, and corruption of a structurally complete record;

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
- object-tier cache budget/residency;
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

Decision: **accepted 2026-08-12**.

The authority is a deployment/controller-assigned nonzero 64-bit `owner_id`
plus a monotonically increasing 32-bit `generation`, both durably stored by the
controller.  The key remains `(timeline, owner_kind, owner_id)`.  Generation 0
is reserved for retention records written by the pre-D1 protocol; a controller
starts at generation 1 and must fail rather than wrap.

The store persists the greatest observed generation, including after DROP.
SET/DROP below that generation return `PS_STATUS_STALE`; DROP leaves an
unenumerated tombstone, so a delayed request cannot resurrect or remove state
after restart or compaction.  SET at the current live generation may update the
horizon, and a greater generation atomically takes over the key.  For log
compatibility only, generation-0 SET-after-DROP retains the legacy unfenced
behavior until a generation-1 controller takes over the key.  The existing
materializer supervisor `owner_epoch` and `worker_generation` are local process
coordination fields, not this controller authority.

### D2. Owner release and stale-owner policy

Selected: ordinary process shutdown retains the durable pin.  Release requires
explicit authoritative deprovisioning or a safe durable horizon handoff;
generation replacement alone must quiesce the old runtime before its pin is
superseded.  Never use wall-clock lease expiry for correctness.  Stale owners
retain space until controller/operator reconciliation proves them dead.

Decision: **accepted 2026-08-12**.

Correctness never depends on wall-clock expiry.  A controller replacement
increments the durable generation only after quiescing the old consumer, then
atomically supersedes the old owner.
Crashes, timeouts, ambiguous failures, and ordinary supervisor handoff retain
the last pin.  DROP is reserved for explicit deprovision after consumption has
stopped; if that proof or the DROP acknowledgement is uncertain, the pin stays.

### D3. Shipped-WAL physical layout

Recommended: immutable fixed-size logical WAL segment files plus small durable
timeline metadata recording the retained base and append end.  Reclaim deletes
whole old files and retains the boundary file when needed.

Alternative: periodically rewrite the flat file.  It minimizes format count but
causes unbounded copy cost and a larger crash-publication protocol.

Decision: **accepted 2026-08-14**.  Use immutable fixed-size logical WAL
segments with checksummed metadata and bounded chunk validation.  The segment
store is integrated into daemon append/read/recovery with the flat log retained
as migration/tail authority.  R3b will add retained-base publication, read
pins, segment deletion, and reclamation of the corresponding flat prefix.

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

Decision: **accepted for MVP**.  Default-tablespace local POSIX is the required
deployment boundary.  User tablespaces, S3, SPDK layers, and service-manager
packaging do not block MVP completion.

## Proposed PR sequence

The remaining default sequence is:

1. R3b WAL reclaimer, now enabled by replacement-base compaction;
2. R4b forkmeta compaction/reclamation and publication crash tests;
3. R5 timeline deletion;
4. R5b reclaimer backpressure controllers;
5. H0 fault/inspection primitives;
6. H1 composed crash scenarios;
7. H2 format fixtures and compatibility CI;
8. R6 bounded-space acceptance and final MVP status update.

Keep each PR independently reviewable and keep the existing standalone and
golden suites green.  If work packages depend on one another before their base
lands, use stacked PRs and finish with an explicit roll-up PR to `pagestore`.

## Progress log

| Date | Change | Evidence |
|---|---|---|
| 2026-08-12 | Established completion plan after PRs #174 and #175 landed | Existing pagestore CI green; remaining gates from `MVP_STATUS.md` |
| 2026-08-14 | Completed R0/R1 owner lifecycle and R2 page pruning; added R3a immutable WAL segment/store primitives | Stacked PRs #177-#191, standalone/integration CI, bounded-churn and publication-crash tests |
| 2026-08-14 | Added R4a live snapshot/log-epoch cutover and GC, then persisted known/FPI plus record-end metadata needed for safe replacement-base selection | Stacked PRs #195-#197 plus the replacement-base metadata follow-up; standalone and integration coverage |
| 2026-08-15 | Added the pure R4 replacement-base planner: operational and discrete horizons retain a union of FPI-led redo chains, future records remain intact, and legacy/insufficient metadata fails closed | Dedicated planner unit tests; durable frontier and snapshot cutover remain the next stacked change |
| 2026-08-15 | Split WAL-index snapshot publication into durable shard preparation and atomic manifest commit | Creates the crash-safe insertion point for the R4 reclaimed frontier without changing the existing one-shot API |
| 2026-08-15 | Completed R4 WAL-index entry compaction and durable frontier admission | Multi-shard proof, discrete/operational chain integration, restart/corruption coverage, and a deterministic crash after frontier publication |
