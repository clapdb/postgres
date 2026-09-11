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

### H1 branch-controller prepared-receipt crash slice

Status: **implemented for the prepared-receipt/service-restore slice**.

`pagestore_branch_prepare` publishes a CRC-protected, configuration-bound
operation journal before changing service state. Recovery only continues a
fully recorded branch boundary and converges the temporary retention pin,
materializer pause, and writer ownership through idempotent/read-only checks.
The four process-abort points cover receipt publication and service-restore
edges. Bootstrap installation, page-store layer recovery, and GC are outside
this slice.

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

Status: **R3a immutable segment/store primitives and live-path integration are
implemented.  R3b-1 supplies the durable retained-base foundation and R3b-2
supplies a standalone physical immutable-prefix reclamation primitive; the R3
cutoff policy, core maintenance integration, and complete shipped-WAL gate
remain open**.

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

Each flat `wal_<timeline>` record is self-describing.  The POSIX backend copies
the retained suffix from a complete record boundary, fsyncs it, and atomically
replaces the old log while serializing physical append/truncate publication.
The core holds a WAL catalog cutover lock, translates every surviving physical
offset, and trims only records fully covered by validated immutable segments.
Restart discovers the immutable store's start from its own headers instead of
depending on the removed flat prefix; retries are compared against immutable
bytes, so reclaim cannot reopen a divergent-history window.  Concurrent reads
either finish on the old inode/offset catalog or begin on the new pair.

This bounds the duplicate flat staging tail.  R3b-1 adds a checksummed v2
identity carrying the physical directory start, retained base, and append end,
validates that tuple against the complete contiguous directory on reopen,
migrates the old v1 identity only after validation, and exposes a monotonic
logical retained-base advance.  That low-level advance deliberately never
authorizes deletion.  R3b-2 adds
`ps_wal_store_reclaim_prefix(target_lsn)`: under the WAL mutex it first
atomically publishes matching retained-base and physical-start frontiers, then
unlinks only segments strictly below the aligned target.  The mutex drains an
already-started read and bars new reads below the published frontier.  Partial
unlink updates the in-memory catalog only after each successful unlink;
directory-fsync ambiguity fences the instance, and reopen uses the published
frontier to validate and idempotently retry any residual authorized prefix.
The primitive rejects rollback, unaligned or beyond-end targets and never
selects a retention cutoff.  It does not integrate core maintenance, owner
admission, flat-WAL reclamation, or WAL-index replacement-base policy.  The
compacted WAL index still names FPI records in raw WAL; until those FPIs are
published as independent replacement page bases, their immutable WAL segments
remain reconstruction dependencies.

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

R3b-2 delivers the standalone frontier-before-unlink, mutex reader barrier,
partial/idempotent prefix unlink, and ambiguous directory-fsync fence portions
of these requirements.  Its residual-prefix path fully enumerates and validates
canonical names, sorts candidates, requires a contiguous suffix immediately
below the target, and only then unlinks in ascending order.  The main catalog
path also revalidates each complete segment header, length, and payload CRC
immediately before unlink, so corruption stops at the current segment and
cannot cross it.  The focused tests also use real child `_exit` stops before
unlink, after a partial unlink, and before directory fsync, plus a deterministic
reader/reclaimer mutex ordering.  Scan errors and corrupt candidates cause zero
further deletion or an immediate stop, while a corrupt low residual prefix makes
reopen fail closed until repaired.
Operational cutoff selection, core maintenance wiring, retention-owner
admission, WAL-index dependency removal, and continuous bounded-space
acceptance are now supplied conservatively by R3b-3 on the POSIX path.

R3b-3 is the conservative POSIX/core policy integration for that reclaimer.
It admits at most one LIVE timeline per maintenance tick, drains ordinary
admission before taking the WAL-index prune/publish gates, and takes one
timeline WAL write lock.  All shard locks are released after the in-memory
WAL-index dependency snapshot and before control-image reads, layer I/O,
metadata publication, or unlink.  The candidate is the segment-aligned-down
minimum of the effective WAL retention floor, durable WAL-index progress, and
the oldest surviving raw WAL dependency.  Missing durable proof, pending or
malformed snapshot state, non-POSIX providers, and any publication/unlink
failure do not authorize deletion and retry after one second.  Durable
retained-base metadata, including a naturally nonzero base after restart, is
the sole admission frontier; physical directory start is not a runtime field
or fence.

This R3b-3 slice intentionally does not implement sparse/discrete retained
base crossing or a bounded fixed-reader soak; those remain explicit follow-up
acceptance work.

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

Status: **runtime implementation, the POSIX crash matrix, and the composed
H1 publication scenarios are implemented; the acceptance matrix remains
incomplete on its concurrency clause**.  Per-horizon existence/size
equivalence across compaction is exercised by the R6 soak's reader and
branch verifications, every publication boundary by the composed daemon
scenarios, and crash recovery by the matrix; but acknowledged concurrent
mutations are verified exactly-once only at the source-rewrite boundary
(the matrix's deterministic concurrent appender), while the composed
workload's post-cutoff trickle is untracked by its oracle.  Prepare,
manifest-commit, and snapshot-GC still need a concurrent-append oracle;
SPDK remains outside the claim.

The pure forkmeta keep-planner and exhaustive unit/property coverage now define
the event visibility, exact-fence base retention, legacy sequence handling, and
fail-closed input contract.  Meson and standalone CI run this planner directly.
A durable POSIX checkpoint/captured-tail format foundation now also exists: a
single selected generation is discovered through a checksummed manifest, with
immutable checkpoint and tail files, staged prepare/commit, exact tuple fences,
bounded reads, recovery, and conservative GC.  This runtime slice now wires
that format into daemon/tiering builds and core maintenance: the controller
holds the admission, all-shard, page-prune, WAL-index, and map fences, derives
the cutoff from the lexicographic minimum durable page frontier, commits the
manifest, atomically rewrites the forkmeta epoch with a matching snapshot-base
marker, and leaves the live in-memory arrays conservative for SPDK readers;
restart rebuilds compacted arrays from the selected snapshot.  The versioned
snapshot payload records the exact cutoff and frozen admission highwater.
Startup treats the selected snapshot as authoritative over an old epoch, while
preserving a matching marker's complete post-cutover suffix.  Ambiguous
manifest publication or source rewrite poisons the runtime until restart.  The
first crash-matrix slice now covers real child-process aborts at durable
prepare, manifest-commit, source-rewrite, and completed snapshot-generation GC
boundaries. It reopens each store in a fresh parent, checks the exact named
fault report and exit 88, and covers deterministic concurrent append overlap
plus four configured POSIX shards. This work does not claim coverage of every
internal unlink/fsync instruction, SPDK hardware, or the remaining composed H1
crash scenarios at the time it landed; the composed `forkmeta` daemon
scenarios now cover those boundaries (without a concurrent-append oracle
at prepare, manifest commit, and snapshot GC), and SPDK stays outside the
claim.

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
- H1 exercises each publication boundary before R6 begins its soak
  (composed daemon scenarios now cover prepare, manifest commit, source
  rewrite, and snapshot GC).

Expected scope: one implementation PR and one crash-test PR.

### R5. Delete timelines durably

Status: **foundation, POSIX runtime quiescence, layer/private-WAL cleanup, and forkmeta filtering implemented**.  The first
slice adds legacy-only migration and V2 create/event mixed records,
LIVE/DELETING state plus a reserved DELETED format value with incarnation,
BEGIN_DELETE/STATE IPC, and descendant/retention-owner admission vetoes.  The
POSIX path now holds an independent lifecycle read gate across every complete
ordinary request and maintenance task, including handed-off async workers;
BEGIN_DELETE takes lifecycle
write, then admission write, then map write.  A fair turnstile closes new
readers once a delete writer queues, independent of the platform's default
pthread rwlock reader/writer preference.  SPDK BEGIN_DELETE remains
fail-closed: async request drain is still follow-up work.  DELETING timelines
now durably mark and asynchronously reclaim manifest-owned local and remote
layers, resume after restart, retry provider failures with backoff, and
reconcile deterministic remote objects from completed-but-unpublished uploads.
DELETING owners are also removed from the shared forkmeta checkpoint, tail,
and source epoch through the existing crash-safe R4b publication protocol;
surviving owners are retained losslessly in the deletion-forced generation.
The POSIX deletion worker now also removes the target's flat and immutable WAL,
WAL-index epochs/watermarks, and WAL-index snapshot directory after validating
the complete owner-scoped set.  Partial removal reopens without parsing the
deleting owner's artifacts and resumes idempotently; live sibling artifacts are
untouched.  POSIX maintenance now also parses and atomically filters every
shared page segment containing the deleting owner, preserving survivor record
bytes and rebasing the covered-prefix watermark before same-id replacement.
Restart scans the filtered stream and never rebuilds DELETING owners; SPDK
leaves this callback NULL.  POSIX maintenance now revalidates every durable
consumer, appends and fsyncs a same-incarnation DELETED state event, and only
then publishes DELETED in memory.  DELETED timelines remain defined and reject
  normal operations.  POSIX CREATE_BRANCH reuses an ID only from durable DELETED
  with exactly the next nonzero incarnation, after purging incarnation-local
  runtime state; STATE returns the current token and requests for incarnations
  greater than one must carry it.  SPDK async drain remains fail-closed: its
  daemon rejects reuse CREATE_BRANCH before shared-core entry.  WAL and control
  restore clients receive the immutable token from their startup
  configuration/manifest and propagate it; control restore never queries
  mutable STATE.

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
  CREATE_BRANCH target token exactly one greater than the DELETED token;
  delayed metadata, retention mutations, and requests from an older
  incarnation are rejected.  The existing page, forkmeta, WAL, and layer
  formats remain unchanged: the durable DELETED event is the cutover proof
  that all old-incarnation consumers and writers have drained before reuse;
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

Status: **soak in CI and scheduled nightly; every persisted category the
soak's owner mix exercises is proven bounded on CI-sized and 6000/8000-round
runs, the operational cutoff no longer needs a page-history owner, and
SLRU-class and reader-artifact versions follow the relation plan.  One
documented limitation stays open: the newest artifact generation at or below
the floor is retained even after the object it describes is dropped, because
the publication protocol emits no durable drop event, so churn of artifact
keys (a reader snapshot per dropped database, say) accumulates one surviving
generation per removed object, which the soak's fixed/advancing readers do
not exercise**.

`pagestore_soak_test` is the acceptance harness.  It plays every retention
role over the daemon protocol: a WAL-shipping writer with a bounded live set
(update, extend, truncate, unlink/recreate), a materializer that mirrors
control note/image pairs, publishes durable WAL-index progress, and advances
an exact `(LSN, admission_seq)` owner pin, an advancing and a fixed reader
whose pinned views are verified by as-of reads and sizes, copy-on-write
branches that are written, read for fork-point isolation, durably deleted, and
reused by incarnation, and clean/crash daemon restarts with full-model
re-verification.  Physical bytes are sampled per persisted category against
declared during-run and quiescent bounds derived from the controller
configuration, and the report carries logical/physical bytes, approximate
write amplification, per-controller lag, throttle and wait time, catch-up
time, and active owners.  Bugs it exposed and the fixes that landed with it:
control-image/note pairs are now pruned like relation pages but fenced by every
retained WAL boundary and planned over the complete version chain
(`pagestore_control_prune_test`); WAL-index compaction takes durable stored
page versions and fork-level deaths as replacement bases
(`ps_walidx_prune_plan_bases`, reclaim core cases); and forkmeta snapshot
compaction exempts frontier-less branch timelines instead of failing closed
for every timeline, capping the cutoff at each live branch's fork point so
its own lower-LSN mutations stay admissible, while reclaim-debt accounting
stays strict.  Control-image retention also fences every retained SLRU seed
and reader artifact, keeps one physical copy per retained version across
mirror retries, and is rescheduled whenever a WAL-resource pin or a deleted
branch cap changes; single-page redo starts from the stored replacement base
when the WAL index no longer carries a full-page image.  Longer runs
then exposed unbounded fork-lifecycle history: image compaction now drops
page versions invalidated at every horizon they serve, the forkmeta planner
keeps only the base, inheritance fence, and growth per horizon plus the
definitive events a retained version or a still-indexed WAL record needs, and
deletion-forced generations compact survivors whenever a cutoff is proven
(`pagestore_lifecycle_prune_test`, `ps_forkmeta_prune_plan_required`).

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

The operational cutoff is now derived from what the writing compute already
publishes (the materializer's WAL/WAL-index pin at its restart redo, or the
newest checkpoint note's redo for a direct-write compute), so a materializer-only or
branch-compute topology prunes page history and control images without a
page-history owner; the branch controller's base pin carries page history
while a branch is prepared, and a kept checkpoint image retains its
exact-redo twin.

The materializer's own WAL-index horizon is page-protected by the cutoff
derived from its pin, so stored pages replace its FPI-led chains; nothing
remains before the gate closes beyond keeping the nightly soak green.

The nightly long-run configuration is `.github/workflows/pagestore-nightly.yml`
(three seeds, 8000 rounds each, scheduled daily and dispatchable with chosen
seeds/rounds; reports summarized per job and retained as artifacts).  The
schedule fires only once the workflow file is on the repository's default
branch, which GitHub requires for `schedule` and `workflow_dispatch`.

Expected scope: one PR.  Passing it closes the retention MVP gate.

### R5b. Add reclaimer backpressure controllers

Status: **R5b controllers complete for PAGE, shipped-WAL, WAL-index, and
forkmeta; queue-bound soak/tuning remains R6 work**.

R5b-1 adds lean independent PAGE and shipped-WAL lag controllers.  They publish
high-water/catch-up configuration, hysteretic throttle state, transitions, and
foreground wait time; POSIX foreground mutations wait before entering runtime
locks, while reads and maintenance continue.  PAGE debt is limited to complete
flush-covered segments, and WAL debt uses the existing retention, raw
dependency, and branch proofs.  R5b-2 adds an independent POSIX WAL-index
controller whose debt is the saturating sum of append tails, obsolete
epoch/watermark bytes, and obsolete snapshot generations.  Its bounded
metadata-only refresh validates selected manifest/shard identity while
immutable payload checksums remain a startup/publication responsibility, and
per-timeline tail candidates can force fair snapshot publication below the
geometric trigger.  Forkmeta adds a POSIX-only compacted-source baseline plus
append growth and obsolete snapshot/temp debris, with bounded metadata
observation and forced fair snapshot maintenance.  Queue-bound soak evidence
and controller-specific tuning remain follow-up work in R6; forkmeta does not
include any forkmeta-index or H0 changes.

Expected scope: one or two PRs.  R6 consumes these controls; it does not
introduce them.

## Crash and compatibility work

### H0. Add common fault and inspection primitives

Status: **read-only aggregate, per-timeline, and minimal relation inspection
foundation complete; composed H1 scenarios remain**.

The first H0 slice uses one canonical fault catalog for C and Python, proves a
pre-armed daemon fault was reached, and checks two recovery opens.  Existing
page-compaction, GC mark-delete, page-frontier, and WAL-index-frontier crash
windows use the same registry.  The lock-free `daemon.after_ready` point also
supports error and bounded pause actions, with real crash/error/pause daemon
recovery scenarios.  H0b now supplies bounded seqlock snapshots and the strict,
read-only `private-test-ipc` inspection schema v4 contract for aggregate health,
manifest, GC, owners, backpressure, and pruning observations plus the
per-timeline foundation, including runtime probing of every advertised operation
and strict
boolean/nonnegative-counter response typing.  Runtime profiles now advertise
protocol version 45 and schema version 4.  The minimal H1 relation operation
uses a dedicated request/response slot isolated from ordinary I/O channels,
with daemon-instance/generation/deadline fencing, strict read-only opcode and
parameter validation, expected timeline-incarnation fencing, as-of
existence/fork-size results, and explicit `selected_version: null` when the
core cannot prove one aggregate version.  The relation mailbox is advertised
only by the POSIX frontend; standalone relation assertions are capability-gated
so the SPDK frontend remains runnable without that POSIX-only mailbox.
The first composed H1 materializer slice is now implemented: the two
restartpoint plans pause the checkpointer child after relation-page sync/before
marker write and after marker sync, then stop and recover the whole
materializer.  The prepared-receipt/service-restore branch slice and the POSIX
image-layer create/write/seal/manifest-ADD publication slice are also covered.
Portable bootstrap/install is covered by the golden scenario's installer
crash/retry matrix. Manifest replacement, reclaim, and GC H1 cases remain.

Deliverables:

- one test-only named fault registry with crash/error/pause actions and hit
  counts;
- reachability accounting so an unhit expected fault fails the test;
- harness timeouts, replay metadata, and diagnostic bundles;
- bounded shared-memory snapshots plus a strict read-only `private-test-ipc`
  inspection schema and capability advertisement for timeline, relation,
  manifest/layer, retention/GC, and owner state needed by recovery assertions;
  mutating inspection operations remain empty.  Relation requests use a
  dedicated inspection mailbox and never claim ordinary I/O channels.

Acceptance:

- faults do not add durability edges or alter production behavior when disabled;
- every run reports scenario, seed, fault, hit count, and operation identity;
- paused faults have a watchdog and actionable diagnostics.
- the H0b Python harness tests validate every advertised inspection operation
  and its exact response fields, and reject extra or mutating schema entries.

Expected scope: one or two PRs.

### Local POSIX store ownership and orphan-layer recovery

Store recovery and local provider mutations share an exclusive advisory store
lease. Startup may remove canonical, unreferenced local layer files only after
validating the manifest and the complete candidate namespace. Manifest-owned
IDs, including deleting and remote-only records, remain protected. Unknown
non-layer files and object-tier artifacts are not part of this sweep.

Ambiguous manifest-tail repair must durably inhibit orphan sweeping before
truncating the manifest, and the inhibition persists across restart. Missing
manifest metadata does not authorize deletion. Recovery retries must preserve
referenced data after partial unlink or directory-sync failures.

Acceptance includes competing owners with distinct SHM names, release after
process death, failed-open cleanup, canonical orphan reclamation, namespace
validation before deletion, and continued H1 sentinel recovery. This closes
local orphan cleanup only; the other H1 crash families and R6 space acceptance
remain separate gates.

### H1. Compose process-level crash scenarios

Status: **materializer replay/restartpoint, branch prepared-receipt/service-
restore, portable bootstrap/install, POSIX image-layer publication, POSIX
page-pruning, POSIX WAL-index compaction, POSIX WAL reclaim, POSIX timeline
deletion, POSIX manifest replacement, POSIX fork-metadata publication, and
writer/reader/materializer/store restart-combination slices implemented;
branch-compute restarts remain with the golden scenario**.

Required scenario families:

- materializer replay/restartpoint/durable-marker publication;
- branch prepare, receipt publication, bootstrap install, and service restore;
- manifest replacement and image-layer seal/publication;
- page pruning, WAL reclaim, WAL-index compaction, remote upload intent and
  orphan reconciliation, and timeline deletion;
- daemon, writer, materializer, and branch-compute restart combinations.

The portable install slice in `mvp_golden_test.sh` targets an offline same-build,
default-tablespace skeleton. Four named installer-backend aborts cover maps
installed, the pg_xact remove/rename gap, and both sides of final manifest
publication. Each case checks the exact fault report and backend exit,
unchanged prepared inputs and restored control, startup rejection before
publication, full artifact recovery on retry, and byte-idempotent reinstall.
The resulting branch must pass golden SQL fork-point, parent/child isolation,
and restart checks. Power-loss recovery and concurrent installers/service
managers are outside this process-abort contract.

The materializer slice is split into two focused plans: one pauses after
relation-page store sync and before marker write, and one pauses after marker
store sync and before retention advance.  The pause is reported by the named
fault machinery, while the harness immediately stops the complete materializer
postmaster and lets the supervisor recover it; it does not assume that the
checkpointer child is the supervisor's worker generation.  Each plan records
both R1 and R2 relation metadata and requires the R2 main fork to grow.

The page-pruning slice adds a `gc_seed` runtime operation to the daemon fault
harness.  A dedicated IPC client seeds three generations of relation history
and a newer block, arms the named fault marker, then installs a configured
page-history owner at the cutoff; three process-abort scenarios crash after
the durable page-prune frontier, after the compacted layer's manifest
publication, and after the retired layer's mark-delete.  The snapshot, recovery
read (published newest block, retained history at the cutoff, refused
pre-cutoff version), manifest/local-layer reconciliation, and republished
retained horizon are checked, followed by one idempotent restart.

The WAL-index compaction slice reuses `gc_seed` with a `wal_index` workload:
a fixed WAL-index reader at 40 under three FPI-led chains on one block, one
committed interval made a snapshot candidate by `--walidx-snapshot-bytes 1`,
and a process abort after the durable frontier.  The crash must leave the
frontier plus the staged generation without its commit; recovery must commit
the retried generation, serve the reader's exact chain and the newest chain,
refuse the dropped point below the frontier, and keep the WAL-index owner.

The WAL reclaim slice adds three named store-lock probes to the shipped-WAL
prefix reclaim (`wal_reclaim.before_unlink`, `wal_reclaim.after_unlink`,
`wal_reclaim.before_dir_fsync`) beside the existing environment hooks the
core/store unit tests use, and a `wal_reclaim` gc_seed workload: three sealed
1 MiB segments, a control note at the shipped end, workload-armed fault, and
WAL-index progress through the end.  Snapshots count the sealed segments left
on disk per stage; recovery must finish the unlink retry, refuse prefix reads,
keep the WAL end and retain floor, clear physical reclaim debt, and leave no
owner.

The timeline deletion slice adds four lock-held probes
(`timeline_delete.after_deleting`, `.after_wal_cleanup`,
`.after_segment_rewrite`, `.after_deleted`) around the durable DELETING event,
private WAL/WAL-index removal, shared segment rewrite, and the durable DELETED
event, and a `timeline_delete` gc_seed workload: a branch with private shipped
WAL, a committed WAL-index interval, an owner layer, and shared-segment pages,
then a workload-armed BEGIN_DELETE.  Snapshots check the owner's private
artifacts per stage; recovery must reach DELETED with its incarnation token,
keep the parent readable, reject branch reads, remove every owner artifact,
reconcile the manifest, and register no owner.

The manifest replacement slice adds two map-held probes around the atomic
`layers.manifest` rewrite (`manifest_compact.after_tmp_sync`,
`manifest_compact.after_rename`), removes a crashed compaction's temp log on
manifest open, and adds a `manifest_compact` gc_seed workload that writes 320
pages under a harness-held maintenance pause (`--test-maintenance-pause-file`)
before arming the fault and releasing maintenance.  Snapshots check the temp
file per stage; recovery must replay to a reconciled manifest, serve every
page, and leave no temp log.

The restart-combination slice adds a `restart` operation to the writer and
materializer runtimes.  Writer-runtime targets are the writer and installed
pinned readers (a reader restart restores its boot control image with
`pagestore_control_restore` first); materializer-runtime targets are the
writer, the materializer worker (replaced by the supervisor with a new
generation), and the store (supervisor and computes stopped, daemon
restarted on the same shared memory, computes and supervisor returned).
`writer_reader_restart` and `materializer_restart_combinations` compose
them with horizon, visibility, boundary, and zero-lag assertions.

The fork-metadata slice composes the existing `forkmeta.*` probes on the
daemon through a `forkmeta` gc_seed workload: the page-pruning history and
cutoff pin prove the cutoff, thirty-two relations carry persisted fork-size
events on both sides of it, and a post-cutoff trickle publishes the second
generation that retires the first.  This covers the R4b acceptance item that
H1 exercise each publication boundary in a composed process-crash scenario;
the concurrent-mutation clause stays open at the prepare, manifest-commit,
and snapshot-GC boundaries because the trickle relations are not in the
oracle.

Acceptance:

- each declared transition is exercised before and after its durability point;
- recovery yields the complete old state only for crashes before durability
  and the complete new state for crashes after durability; acknowledged
  markers, deletion states, reclamation frontiers, and branch publications are
  monotonic and cannot roll back even when they are not SQL-visible;
- direct and recovered SQL-visible results agree at declared horizons.

Expected scope: two or three focused PRs.

### H2. Add persisted-format fixtures and compatibility CI

Status: **daemon-side POSIX record formats covered under D5 rule 2,
including a legacy fixture exercised by the first format change;
backend-side artifact fixtures (D5 rule 4), the PostgreSQL payload-version
binding in envelopes (D5 rule 1), and rule 3's container obligations --
the POSIX and SPDK container identities, and a fail-closed SPDK superblock
open -- remain**.

The slice adds `pagestore_format.h` identities reported by every format-owning
module, the `pagestore_format_versions` tool, the `fixture` workload of
`pagestore_gc_crash_client`, `harness/pagestore_fixture.py` (capture and
check), and `fixtures/posix-mvp-baseline`.  The check enforces identity
freshness, reopen with the oracle across a restart, and thirty-six declared
mutations with their documented outcomes.  It surfaced and fixed two defects
(relocated stores refused by absolute layer locations; a damaged forkmeta
marker discarding acknowledged post-cutover events).  The gap it documented
(forkmeta source records had no checksum) is closed by FKM3 records, the
same 64-byte layout with a CRC-24 in the former pad bytes; FKM2 stays
readable and its fixture is the legacy one.

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
as migration/tail authority.  The first R3b slice supplies the durable
retained-base metadata and monotonic publication primitive; later R3b slices
must add read pins, segment deletion, and reclamation of the corresponding
flat prefix.

### D4. Timeline deletion with descendants

Recommended for MVP: reject deletion while any descendant exists.  Do not add
cascade or ancestry reparenting semantics.

Decision: **accepted 2026-08-25**.  Reject deletion while any live or deleting
descendant exists; do not cascade or reparent.  The first lifecycle slice
implements this rule before durable transition publication.

### D5. Persisted-format support window

Decision: **accepted**, as the four rules below.  They separate what the page
store owns (its envelopes and containers) from what PostgreSQL owns (the bytes
inside them), because only the former can have a page-store support window.

1. **PostgreSQL-native payloads are wrapped, never rewritten.**  Relation
   pages (with their `pd_lsn`), the `ControlFileData` image, WAL segment
   bytes, SLRU pages, and `pg_filenode.map` contents are stored as the bytes
   PostgreSQL wrote and handed back to PostgreSQL code to load: pages to the
   buffer manager through smgr, the control image through
   `pagestore_control_restore`, WAL through the restore command, SLRUs and
   relation maps by installing the files.  The page store does not interpret
   a payload beyond the fields it needs to key and fence it (`pd_lsn`, the
   checkpoint redo, the segment start).  The rule binds the *persisted*
   payload: what the store holds is byte-exact.  Where an install step must
   derive a different PostgreSQL object from a stored one, that derivation
   is an explicit, named transformation on PostgreSQL's own definitions and
   never a second persisted format.  Two such steps exist today, and each
   fixture checks the stored input byte-exactly and the installed output
   against the named transformation, not for byte equality with the input:
   - the archive-bootstrap control install: after checking the control
     image's compatibility tuple against the running build,
     `pagestore_control_restore --archive-bootstrap` copies the stored
     `ControlFileData`, sets `minRecoveryPoint`/`minRecoveryPointTLI` to the
     checkpoint redo and its timeline, clears the backup start/end fields,
     recomputes the CRC with PostgreSQL's algorithm, and installs the
     result, so a fresh skeleton enters archive recovery at the stored
     checkpoint instead of trusting a foreign `initdb` state;
   - SLRU seeding for a branch (`pagestore_seed_clog`,
     `pagestore_seed_commit_ts`, `pagestore_seed_multixact`, driven by
     `pagestore_seed_branch_slrus()`): each page over the fork's horizon is
     the stored seed page at the base cutoff `C` with the shipped WAL in
     `(C, target]` applied, written as whole segments under `pg_xact`,
     `pg_commit_ts`, and `pg_multixact`.  The seed pages and the WAL bytes
     are PostgreSQL's, but the appliers are not: `ps_clog_apply_range()`
     and the commit-ts and multixact seeders decode the records and set
     the status bits, timestamps, and member slots themselves, mirroring
     `clog_redo`, `CommitTsRedo`, and `multixact_redo` rather than calling
     them.  That mirror is page-store-owned transformation logic: it is
     versioned with the envelope, bound to the PostgreSQL version whose
     redo it mirrors, and it must be proven equivalent to actual recovery
     by an independent result, not by a fixture that would only compare
     the mirror with itself.  That proof does not exist yet: the golden
     scenario takes its seed base `C` from the materializer's recovery,
     but it installs the seeded SLRUs before the branch starts, so
     recovery never reconstructs `(C, R]` on its own for comparison, and
     afterwards it checks a single `pg_xact` status and no commit-ts or
     multixact content.  H2 therefore requires a comparison of the seeded
     `pg_xact`, `pg_commit_ts`, and `pg_multixact` segments over the fork
     horizon against the same segments produced by normal recovery of the
     same WAL through `R` (the materializer's own files at that
     restartpoint are such a result) before this obligation is treated as
     covered.  Refactoring the appliers onto PostgreSQL's redo routines
     would retire the obligation.  The
     transformation is version checked only where its inputs are resolved
     from a control image: the serialized branch controller path does so;
     the public `pagestore_seed_branch_slrus()` and legacy
     `pagestore_prepare_branch_impl()` entrypoints take caller-supplied
     cutoffs and horizons and check nothing, so H2 requires them to resolve
     and validate the control tuple before interpreting a seed page.
   A payload's version is therefore
   PostgreSQL's, and whether a payload can be loaded by a different
   PostgreSQL build is PostgreSQL's question, not a page-store migration.
   That identity is the full compatibility tuple PostgreSQL itself checks
   when it opens a cluster, not the version constants alone: a build with a
   different `BLCKSZ`, `XLOG_BLCKSZ`, `RELSEG_SIZE`, `SLRU_PAGES_PER_SEGMENT`,
   `MAXALIGN`, `NAMEDATALEN`, `INDEX_MAX_KEYS`, `TOAST_MAX_CHUNK_SIZE`,
   `LOBLKSIZE`, or float format writes incompatible page, WAL, and SLRU
   bytes under the same `PG_CONTROL_VERSION` and `CATALOG_VERSION_NO`.  The
   tuple is therefore the version constants (`PG_CONTROL_VERSION`,
   `CATALOG_VERSION_NO`, `XLOG_PAGE_MAGIC`, `RELMAPPER_FILEMAGIC`,
   `PG_PAGE_LAYOUT_VERSION`) together with the layout parameters
   `ControlFileData` records and `pagestore_control_restore` already
   compares one by one.  Envelopes record that tuple for their payload,
   and a control-image reference covers only what `ControlFileData`
   contains: the control and catalog versions and the layout parameters.
   `XLOG_PAGE_MAGIC`, `RELMAPPER_FILEMAGIC`, and `PG_PAGE_LAYOUT_VERSION`
   live in the native headers of the WAL page, the relation map, and the
   relation page, so the envelope or loader that hands one of those
   payloads to PostgreSQL binds or checks that native identity as well
   (the WAL segment envelope against the first page header it carries, the
   relation-map envelope against the map's own magic, the page path
   against the page header) rather than relying on the control image.  A
   relation block that is still uninitialized is legitimately all zero
   (`PageIsNew()`: `pd_upper == 0`, `pd_pagesize_version == 0`), and the
   zero-extension path serves unwritten blocks that way, so the page-header
   check applies only to initialized pages; a new page carries no native
   identity and is bound by the control tuple alone.
   Loaders compare the tuple with the running build, and a mismatch fails
   closed naming the payload identity rather than the envelope.  Some
   object payloads are page-store-defined rather than PostgreSQL's, and
   those are versioned as envelopes: the reader's running-transaction
   snapshot (PostgreSQL has no stable serialization for it) and, today
   without any magic or version of their own, the raw values consumers
   `memcpy` out of an object -- the checkpoint-redo `XLogRecPtr` in
   control block 1 that the WAL retention floor derives from, the
   `PS_KLASS_SLRU_WM` watermark and the `PS_KLASS_SLRU_TOMB` truncation
   cutoff.  A layout change to one of those would be misread as a floor,
   a watermark, or a cutoff, so H2's store-object slice classifies each,
   gives it an identity, and keeps a fixture or migration for it.
2. **Envelopes -- the daemon's record formats -- keep a fixture for every
   version shipped on `pagestore` after the MVP baseline.**  A supported
   older version is readable or has an explicit migration; anything newer,
   unknown, checksum-corrupt, or illegally truncated fails closed with an
   actionable error.  Every format change updates or adds a fixture, and the
   compiled identities must match the committed fixture.  Release branches
   may define a narrower cross-major window.
3. **Containers are per provider, are registered separately from the
   records they hold, and carry the same support window as records.**  A
   record format is provider-neutral and one fixture proves it for every
   provider; a provider must not fork it.  Each provider registers the
   identity of its own container format -- the POSIX store's file naming
   and directory layout, the SPDK store's `spdk_super` (V1 legacy, V2
   current) and its on-device segment extent layout -- so a container
   change is caught by the identity check even where its fixture cannot
   run in CI, and rule 2 applies to it: a container shipped after the MVP
   baseline stays openable by a later release or migrates explicitly (as
   the V1 superblock already does), a change that would strand an earlier
   post-baseline store is not a compatible change, and the POSIX fixture
   keeps the earlier layout as a legacy fixture rather than being
   recaptured over it.  Registration is not the whole obligation.  A
   container whose metadata is unknown, newer, truncated, or corrupt must
   fail the open, never degrade to a default that can overwrite existing
   data; and the metadata a container depends on to place new data must be
   published durably -- written to a temporary file, fsynced, renamed over
   the old copy, the directory fsynced, with every failure propagated to
   the caller -- because a valid but stale copy that survives a lost write
   or crash reopens to the same overwrite.  The SPDK provider meets
   neither today: `super_read()` leaves every per-shard segment count at
   zero when neither known `spdk_super` layout matches and `spdk_open()`
   proceeds, and `super_write()` overwrites `spdk_super` in place, ignores
   open and write failures, never fsyncs, and `spdk_sync()` reports
   success regardless, so a later append would reuse live extents in
   either case.  Closing both is part of the remaining H2 work, ahead of
   the SPDK fixture.  Neither provider
   registers its container identity yet: `ps_storage_posix_format_identities()`
   reports only the WAL-index watermark record.  The SPDK container fixture
   itself follows the MVP: it needs the device layout split from NVMe I/O
   behind a file-backed shim before it can be captured and checked without
   hardware.
4. **Backend-side artifacts follow rule 2 for their envelopes and rule 1
   for their payloads.**  Reader and branch manifests, the branch bootstrap,
   reader snapshot and catalog files, the reader's published snapshot
   objects, the materializer and writer control blocks, and the durable
   controller artifacts that recovery of an interrupted operation depends
   on -- the CRC-protected branch preparation journal
   (`pagestore_branch.prepare.json`), the branch retention-generation
   authority file that fences owner-generation reuse, the branch
   controller's configuration (loaded, schema-checked, and matched against
   the journal's configuration identity before an interrupted operation
   can be resumed), the reader-map intent marker
   (`.pagestore-reader-map-pending`, whose exact name and raw `XLogRecPtr`
   content restart recognizes to retry a relation-map installation that
   crashed before the pin advanced), the materializer
   supervisor's configuration, status, and its own generation-authority
   file (`retention-owner-<id>.json`, read independently of the status and
   published before a new worker generation is registered), and the SLRU
   mirror continuity
   markers (`pagestore.slru_mirror_debt`, and
   `pagestore.slru_mirror_primed` with its raw eight-byte checkpoint-redo
   stamp, whose presence and stamp decide at boot whether the stored SLRU
   mirror is complete or the cluster carries mirror debt) -- are page-store
   envelopes with
   a fixture and a support window of their own (their readers enforce
   exact schemas, so a schema change without a retained fixture and
   migration could leave a controller unable to resume or clean up, with
   the writer restricted, the materializer paused, or a pin stranded); the
   relation
   maps, SLRU pages, and control image they carry are PostgreSQL payloads
   whose version identity the envelope records and the loader checks.  A
   fixture whose payload names a different PostgreSQL version reports
   "payload needs PostgreSQL <version>" and is not treated as a broken
   envelope.

### D6. MVP deployment boundary

Recommended: keep default-tablespace local POSIX as the MVP boundary.  Treat
user tablespaces, S3, SPDK layers, and service-manager packaging as follow-up
work.

Decision: **accepted for MVP**.  Default-tablespace local POSIX is the required
deployment boundary.  User tablespaces, S3, SPDK layers, and service-manager
packaging do not block MVP completion.

## Proposed PR sequence

The default sequence was:

1. R3b retained-base foundation, then the WAL reclaimer enabled by replacement-base compaction -- done;
2. R4b forkmeta compaction/reclamation and publication crash tests -- done except the concurrency clause;
3. R5 timeline deletion -- done;
4. R5b reclaimer backpressure controllers -- done;
5. H0 fault/inspection primitives -- done;
6. H1 composed crash scenarios -- done;
7. H2 format fixtures and compatibility CI -- daemon-side done; backend-side artifacts remain;
8. R6 bounded-space acceptance and final MVP status update -- soak and nightly lane done; the
   final status update follows the first scheduled nightly runs.

What remains, in order: the H2 slices under the D5 decision -- first the
PostgreSQL payload-identity binding in envelopes (the control tuple plus
the native header identities the control image does not carry), the POSIX
and SPDK container identities, a fail-closed `spdk_super` open (unknown,
newer, truncated, or corrupt metadata refuses the store instead of zeroing
the segment counts) with durable, error-propagating superblock
publication, control-tuple validation in the public and legacy SLRU
seeding entrypoints, and the independent-recovery comparison for the SLRU
appliers; then the store-object backend families, the page-store-defined
raw payloads (control block 1's redo note, the SLRU watermark and
tombstone values) included; then the PGDATA artifacts -- the reader and
branch manifests, branch bootstrap, reader snapshot and catalog files,
the reader-map intent marker, the branch controller's configuration,
journal, and authority files, the materializer supervisor's
configuration, status, and generation-authority file, and the SLRU
mirror continuity markers with their migration semantics -- then the
R4b concurrent-append oracle, then the final MVP status update once the
nightly lane has a run history.

Keep each PR independently reviewable and keep the existing standalone and
golden suites green.  If work packages depend on one another before their base
lands, use stacked PRs and finish with an explicit roll-up PR to `pagestore`.

## Progress log

| Date | Change | Evidence |
|---|---|---|
| 2026-08-12 | Established completion plan after PRs #174 and #175 landed | Existing pagestore CI green; remaining gates from `MVP_STATUS.md` |
| 2026-08-14 | Completed R0/R1 owner lifecycle and R2 page pruning; added R3a immutable WAL segment/store primitives | Stacked PRs #177-#191, standalone/integration CI, bounded-churn and publication-crash tests |
| 2026-08-14 | Added R4a live snapshot/log-epoch cutover and GC, then persisted known/FPI plus record-end metadata needed for safe replacement-base selection | Stacked PRs #195-#197 plus the replacement-base metadata follow-up; standalone and integration coverage |
| 2026-08-23 | Landed the R4b runtime cutover foundation: durable frontier-gated normalized forkmeta snapshots, all-shard run-to-completion maintenance, source-log rewrite/epoch marker, startup reconcile, poison-on-ambiguous rewrite, and daemon/tiering wiring | Strict standalone, ASan/UBSan unit coverage, focused Meson, and existing pagestore suite; publication crash matrix remains a follow-up |
| 2026-08-25 | Added R5 POSIX runtime quiescence: lifecycle read coverage for complete requests and synchronous/asynchronous maintenance, fair queued-writer turnstile, and deterministic ordinary/maintenance/delete-drain tests | Focused POSIX timeline test: 58 checks, 0 failures; tiering worker/publication test: 23 checks, 0 failures; SPDK async drain remains explicitly fail-closed/follow-up |
| 2026-08-26 | Added R5 manifest-owned layer cleanup for DELETING timelines: durable per-layer tombstones, asynchronous local/remote deletion, restart resume, orphan-object reconciliation, and retry backoff | Timeline cleanup/restart/failure coverage; strict focused suites and standalone 1998-check suite |
| 2026-08-26 | Added R5 deletion-filtered forkmeta cutover: explicit DELETING owners are omitted from checkpoint, tail, and rewritten source while live and pre-metadata owners survive | Forced/ordinary generation, marker-only owner, multi-delete, restart, rewrite-failure, and existing crash-matrix coverage |
| 2026-08-26 | Added R5 owner-scoped POSIX WAL cleanup for DELETING timelines: flat/immutable WAL and WAL-index logs/snapshots are validated, durably removed, and purged from runtime state without publishing DELETED | Focused normal/fail-closed/restart/sibling tests plus WAL, snapshot, forkmeta crash, and 1998-check standalone coverage; shared page segments remain |
| 2026-08-26 | Added R5 durable DELETED publication and incarnation-aware numeric-ID reuse: the same-incarnation DELETED event is fsynced after owner-scoped cleanup, and CREATE_BRANCH admits only the exact next token after runtime reset | Focused normal/ASan publication, immediate same-horizon reuse, stale-token/parent fencing, restart, repeated-cycle, sibling-safety, and ambiguous-append coverage; SPDK async drain remains fail-closed |
| 2026-09-05 | Completed R5b forkmeta reclamation backpressure: stable compacted-source baseline debt, bounded fail-closed POSIX metadata observation, forkmeta-specific mutation admission, shared-memory/inspect/daemon metrics, and forced fair snapshot/GC catch-up | Focused POSIX backpressure, daemon, inspect, and forkmeta observer/controller coverage; queue-bound soak and tuning remain R6 |
| 2026-09-06 | Added the minimal H1 relation inspection slice: protocol 45/schema 4, a dedicated private request/response mailbox with daemon-instance, generation, timeout, and concurrent-client fencing, strict relation-only read validation, coherent all-shard as-of existence/fork nblocks, explicit unavailable selected version, and expected timeline-incarnation fencing | POSIX standalone plus Python schema/runtime coverage; no SPDK execution |
| 2026-09-06 | Hardened H1 relation inspection follow-up: protocol 45, POSIX fd ownership lock across the complete inspector transaction, direct release-published REQUEST without CLAIMED, bounded abandoned-slot recovery, and strict main-fork/existence consistency validation | Focused POSIX mailbox coverage plus standalone/Python tests; no SPDK execution |
| 2026-09-06 | Closed H1 relation-mailbox ownership gaps: byte-zero initialization/client gate, byte-one daemon lifetime lease acquired after byte zero and retained on the shm fd through shutdown, lease-gated stale REQUEST/BUSY recovery, and real fork/SIGKILL lock coverage; published the POSIX-only mailbox capability so standalone assertions are skipped for unsupported frontends | POSIX mailbox, standalone, and Python tests; no SPDK execution |
| 2026-09-10 | Sealed forkmeta source and snapshot payload records as FKM3 (FKM2 layout, CRC-24 in the former pad bytes; a complete record with a bad checksum refuses to open instead of being treated as a torn tail); FKM2 stays readable; `fixtures/posix-mvp-baseline` becomes the legacy fixture and `fixtures/posix-forkmeta-crc` the current one; the fixture check distinguishes legacy (reopen/oracle) from current (identity pin plus mutations) fixtures and takes several directories | First format change through the fixture process; cutover, crash-matrix, timeline, lifecycle unit tests; both fixtures checked; standalone and integration lanes |
| 2026-09-10 | Added the first H2 persisted-format fixture slice: per-module format identities and `pagestore_format_versions`, a `fixture` workload covering every daemon-side POSIX family, `harness/pagestore_fixture.py` capture/check with identity freshness, reopen-and-restart oracle, and thirty-six mutation cases, `fixtures/posix-mvp-baseline`, meson and standalone CI checks; fixed relocated-store layer locations and a damaged forkmeta marker discarding post-cutover events; documented the forkmeta record checksum gap | Fixture check locally against the captured baseline; layer-store and forkmeta crash-matrix unit tests; standalone and integration lanes; D5 remains open and is flagged as an assumption |
| 2026-09-10 | Added the H1 fork-metadata publication crash slice: a `forkmeta` gc_seed workload (page-pruning cutoff, thirty-two relations with create/zero-extend/truncate events on both sides of the cutoff, post-cutoff trickle for the second generation) composing the four existing `forkmeta.*` probes, with staged/selected/marker/GC snapshots, settled-generation recovery, current and retained fork sizes, refused below-cutoff queries, and idempotent restart | Harness validation tests; four meson/CI scenarios against the POSIX daemon; no SPDK execution |
| 2026-09-10 | Added the H1 restart-combination slice: a `restart` operation for the writer runtime (writer, installed pinned readers via boot-control restore) and the materializer runtime (writer, supervisor-replaced worker, store with all computes down), plus `writer_reader_restart` and `materializer_restart_combinations` scenarios asserting reader horizon and prepared-xid hiding across restarts and materialized boundaries after writer, worker, and store restarts | Harness validation tests and meson plan checks; both scenarios in the integration CI lane; no SPDK execution |
| 2026-09-10 | Added the H1 manifest replacement crash slice: map-held probes after the fsync'd compacted temp log and after its rename, crashed temp-log removal on manifest open, a `manifest_compact` gc_seed workload (320 pages under a harness-held maintenance pause, workload-armed fault and release), per-stage temp-file snapshots, reconciled-manifest recovery with every page served and no temp log, and idempotent restart | Harness validation tests; two meson/CI scenarios against the POSIX daemon; manifest unit test; no SPDK execution |
| 2026-09-10 | Added the H1 timeline deletion crash slice: lock-held probes after the durable DELETING event, after private WAL/WAL-index removal, after a shared segment rewrite, and after the durable DELETED event; a `timeline_delete` gc_seed workload (branch with private WAL, WAL-index interval, owner layer, shared-segment pages, workload-armed BEGIN_DELETE); per-stage private-artifact snapshots, DELETED-with-token recovery, parent readable, branch reads rejected, owner artifacts gone, manifest reconciled, no owner, and idempotent restart | Harness validation tests; four meson/CI scenarios against the POSIX daemon; no SPDK execution |
| 2026-09-10 | Added the H1 WAL reclaim crash slice: named store-lock probes before the first unlink, after each unlink, and before the residual-prefix directory fsync; a `wal_reclaim` gc_seed workload (three sealed segments, control note at the shipped end, workload-armed fault, WAL-index progress); per-stage sealed-segment snapshot counts, unlink-retry recovery, refused prefix reads, WAL end/retain floor, cleared reclaim debt, no owner, and idempotent restart | Harness validation tests; three meson/CI scenarios against the POSIX daemon; fault registry and WAL-store unit tests; no SPDK execution |
| 2026-09-10 | Added the H1 WAL-index compaction crash slice: a `wal_index` gc_seed workload (fixed WAL-index reader at 40 under three FPI-led chains, one committed interval, workload-armed fault), a `--walidx-snapshot-bytes` daemon trigger override, and a process abort after the durable WAL-index frontier with staged-generation snapshot, retried commit, chain/refusal reads, owner, and idempotent-restart checks | Harness validation tests; meson/CI scenario against the POSIX daemon; no SPDK execution |
| 2026-09-10 | Added the H1 page-pruning crash slice: a `gc_seed` daemon-harness operation backed by `pagestore_gc_crash_client` (three history generations, newer block, workload-armed fault, configured cutoff at 3500), three process aborts after the durable prune frontier, the compacted layer's manifest publication, and the retired layer's mark-delete, with snapshot, recovery-read, manifest reconciliation, retained-horizon, and idempotent-restart checks | Harness validation tests; three meson/CI scenarios against the POSIX daemon; no SPDK execution |
| 2026-09-10 | Made the materializer's WAL-index horizon page-protected by its own derived cutoff (unless another WAL-index-only owner shares the LSN), so stored pages replace its FPI-led chains and the soak's WAL bound returns to two publication intervals | Reclaim-core case (a WAL/WAL-index materializer pin authorizes the base at its horizon; a shared LSN still does not); control/lifecycle/standalone suites; integration lane; 2400/8000-round soaks within the tighter bound |
| 2026-09-10 | Added SLRU-class and reader-artifact retention: seeds (replay bases), reader snapshots, the live SLRU mirror, tombstones, and the watermark follow the relation plan below their consumers' pins and branch fork points, and a retired artifact releases its control-image fence (the registry now counts artifact versions) | Control-prune cases for a pinned reader, a branch fork point, a dropped pin, retry collapse, fence release, and restart; lifecycle/reclaim/standalone suites; integration lane; 2400/8000-round soaks |
| 2026-09-10 | Added the nightly long-run soak workflow: a seed matrix (default three seeds at 8000 rounds) built from the freestanding daemon and harness, scheduled daily and dispatchable with chosen seeds/rounds, with per-job report summaries and 30-day JSON artifacts | Workflow YAML validated; the same soak binary and report format as the pull-request lane |
| 2026-09-10 | Derived the operational page-history cutoff from the writing compute (the materializer's restart-redo pin, or the newest checkpoint note's redo), kept the exact-redo twin of every retained checkpoint image, gave the branch controller's base pin page history, and modeled the real materializer mask plus progress marker in the soak | Control-prune cases for marker and note cutoffs with refusal below the frontier and restart; lifecycle/reclaim/standalone suites; integration lane with pruning active in the golden and branch-boot flows; 2400/8000-round soaks |
| 2026-09-11 | Rolled the merged #238-#248 stack (SLRU/reader retention, materializer horizon, the H1 page-prune/WAL-index/WAL-reclaim/timeline-delete/manifest/restart/forkmeta slices, the first H2 fixture slice, FKM3) onto `pagestore`; the stacked PRs had each merged into the PR below them, so their content had stopped on the top branch | PR #249, ancestry-only merge with the tree of the reviewed #248 head; pagestore CI green |
| 2026-09-11 | Scheduled the nightly soak: GitHub runs schedules only from the default branch and `master` is reserved for the upstream mirror, so `pagestore` became the repository's default branch and the workflow gained a schedule guard (this repository or `PAGESTORE_NIGHTLY_ENABLED=1`; manual dispatch always); the interim copy on `master` (#250) is withdrawn | A 200-round dispatch resolved and checked out `pagestore` at the #249 merge, 1426 checks, 0 failures; after the default-branch change a 100-round dispatch ran from `pagestore` itself (run 34610482840, 978 checks, 0 failures) |
| 2026-09-12 | Decided D5: PostgreSQL-native payloads are wrapped and never rewritten, with their PostgreSQL version identity recorded in the envelope and checked by the loader; daemon envelopes keep a fixture per shipped version with explicit migration or fail-closed; container formats are registered per provider (SPDK `spdk_super` and extent layout included, its fixture after the MVP); backend artifacts follow the same envelope/payload split | `MVP_COMPLETION_PLAN.md` D5; the H2 backend slices are sequenced against these rules |
| 2026-09-09 | Added the R6 bounded-space soak (`pagestore_soak_test`, standalone CI) and closed four retention gaps it exposed: control-object version pruning fenced by retained WAL boundaries, WAL-index replacement bases from durable stored page versions and fork deaths, forkmeta cutoff exemption for frontier-less branch timelines, and bounded fork-lifecycle history (invalidated versions dropped by image compaction, base/fence/growth planner with required invalidation fences, compacting deletion-forced generations) | 2400/6000/8000-round runs (three seeds): every category within bound, WAL reclaimed to the last immutable segment, forkmeta at 5-22 KB; lifecycle (178), control-prune (32), WAL-index planner (27), forkmeta planner (12040), reclaim core (95), timeline (316), backpressure (369), forkmeta cutover/crash (259/245), gc (93), standalone (2074), and backpressure daemon (79) suites green |
| 2026-09-06 | Added the first composed H1 materializer crash slice: pause-only checkpointer-child probes after relation sync/before marker write and after marker sync/before retention advance, whole-postmaster recovery, exact fault reports, marker monotonicity, R1/R2 timeline-0 incarnation-1 relation inspection with main-fork growth, and recovered SQL visibility | Python validation/runtime mocks, focused plan validation, explicit PostgreSQL CI lane; real integration lane is CI-owned; no SPDK execution |
| 2026-08-28 | Added the first R3b retained-base foundation: checksummed identity v2, validated v1 migration, strict base/end reopen validation, monotonic atomic retained-base publication, explicit getter status, append publication-fault recovery, and fail-closed ambiguous directory-fsync handling; immutable segments and retention policy are unchanged | Focused WAL-store coverage for getter validation, reopen, monotonic advance/rollback rejection, metadata corruption, append/advance publication faults, crash recovery, prefix unlink/reopen, unexpected suffix validation, recognized temporary cleanup, and 83 checks with 0 failures |
| 2026-08-28 | Added R3b-2 standalone crash-safe physical immutable-prefix reclamation: `ps_wal_store_reclaim_prefix()` publishes retained/physical frontiers before unlink, uses the WAL mutex as a reader drain/barrier, fully validates/sorts residual candidates before ascending unlink, revalidates every main-catalog candidate immediately before unlink, keeps partial unlink catalog state exact, fences ambiguous directory fsync, and retries residual prefixes after restart; no core maintenance or cutoff policy | Final focused WAL-store test: 167 checks, 0 failures; includes reverse-enumeration candidate ordering, scan-error zero-unlink, low/middle main-catalog corruption and residual corruption, lowest/middle unlink failures, per-candidate header/CRC validation, real fork/`_exit` stops before unlink/after partial unlink/before directory fsync, pending-reclaim advance fencing, deterministic reader-barrier timing, idempotence, boundary rejection, and restart retry |
| 2026-08-28 | Added conservative R3b-3 POSIX/core WAL reclaim policy integration: cheap WAL-lock-only due preselection plus bounded no-progress backoff, durable retained-base admission with per-level WAL-lock read rechecks and inherited-parent fallback, fair one-LIVE-timeline scheduling ahead of continuous tier/remote-GC work, admission drain plus WAL-index freeze and single-timeline WAL locking, target branch/control caps, dependency/retention/progress minimum cutoff (including progress beyond the sealed prefix), residual-prefix retry at an already-published frontier, DELETED-descendant release, fail-closed pending-proof/publication handling, and one-second retry backoff; physical directory start is not a runtime fence and pre-metadata timelines retain local WAL reads | Focused core policy test: 66 checks, 0 failures; covers empty and boundary-floor preselection, ancestry and natural nonzero child fallback, child-local controls and target branch caps, LIVE/DELETING/DELETED structural floors and WAL-index exceptions, timeline isolation, naturally nonzero starts, unaligned and boundary-crossing flat progress tails, snapshot recovery plus WAL/WAL-index re-ship admission after base advancement, pre-metadata reads, restart/residual retry, fenced residual-query suppression, read/frontier publication and floor-scan lock-order races, no-proof candidate fairness, pending durable-proof cleanup failure, metadata publication failure/backoff, and admission concurrency; sparse/discrete base crossing and bounded fixed-reader soak remain out of scope |
| 2026-08-15 | Added the pure R4 replacement-base planner: operational and discrete horizons retain a union of FPI-led redo chains, future records remain intact, and legacy/insufficient metadata fails closed | Dedicated planner unit tests; durable frontier and snapshot cutover remain the next stacked change |
| 2026-08-15 | Split WAL-index snapshot publication into durable shard preparation and atomic manifest commit | Creates the crash-safe insertion point for the R4 reclaimed frontier without changing the existing one-shot API |
| 2026-08-15 | Completed R4 WAL-index entry compaction and durable frontier admission | Multi-shard proof, discrete/operational chain integration, restart/corruption coverage, and a deterministic crash after frontier publication |
