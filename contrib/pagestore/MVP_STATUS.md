# pagestore MVP status

This page is the progress source of truth for the pagestore MVP.  The other
documents in this directory describe subsystem designs and longer-term target
architecture; their future-looking sections do not by themselves define MVP
scope or completion.

The ordered work packages, acceptance criteria, and open decisions for closing
the remaining gates are tracked in
[`MVP_COMPLETION_PLAN.md`](MVP_COMPLETION_PLAN.md).

Status below includes work through managed retention owners, page-history
pruning, and immutable WAL segment/store primitives.

## MVP scope

The MVP is a local POSIX deployment with:

- one read-write compute per timeline;
- one continuous PostgreSQL recovery worker materializing shipped WAL;
- immutable image layers on local storage, with a filesystem-backed object-tier
  provider available for exercising upload, eviction, download, and remote GC;
- fixed or advancing read-only computes;
- copy-on-write branches booted as independent computes;
- one or more logical daemon shards.

The MVP does not require S3/Lambda, SPDK layer recovery, multi-writer timelines,
or production performance targets.  Those remain later deployment/performance
work and must not expand the MVP critical path.

The POSIX daemon's shared-memory readiness is a two-phase handshake.  On every
start it invalidates the previous header before opening/recovering the store,
keeps it invalid while workers are being created, and publishes the magic and
`READY` state only after the request and maintenance workers exist.  Shutdown
invalidates the header before flushing the store.  This preserves the MVP
assumption that no live engine client remains attached while the daemon is
restarted, while preventing stale health checks from admitting requests into a
new daemon's zeroing/recovery window.

## What is implemented

| Area | Status | Current proof |
|---|---|---|
| Page ingest and copy-on-write reads | Implemented | standalone and PostgreSQL integration suites |
| Image-layer path | Functional mechanisms implemented; H1 POSIX publication crash slice covered; phases 2–3 partial | manifest/compaction/segment-GC restart tests plus create/write/seal/manifest-ADD crash recovery scenarios with sentinel/LSN and idempotent restart checks; sparse indexes and layer-block cache invalidation remain |
| Filesystem object tier | Upload done; cache/GC operations partial | download, eviction, refresh, and remote-delete tests; cache policy and orphan reconciliation remain |
| Materialized-page cache | Basic version cache implemented; phase partial | bounded cache/invalidation tests; cost-aware admission and integrated redo avoidance remain |
| WAL shipping and ancestry-aware WAL reads | Immutable 1 MiB segments integrated for sealed prefixes; the flat-log copy of every complete sealed record is reclaimed, while the flat log remains migration/tail authority | chunk assembly, reopen, ancestry, and WAL segment/store tests |
| Per-page WAL index and PostgreSQL `rm_redo` reuse | Live index plus crash-safe replacement-chain compaction, durable timeline frontier, multi-shard snapshot/log-epoch cutover, and old generation/epoch GC implemented | WAL redo demos plus discrete/operational chain pruning, frontier admission/restart/corruption/crash tests, snapshot publication, generation/epoch GC, and tail replay |
| Continuous recovery materializer | Local POSIX supervisor implemented; H1 restartpoint crash slice wired with pre-control relation/marker publication and post-control retention | ownership fencing, bounded restart/restartpoint policy, atomic status, `materializer_smoke` crash replacement, and focused pause-only crashes after relation sync/before marker write and after marker sync; both probes precede local `pg_control` durability and scope remains process-crash recovery |
| Composed MVP data path | Implemented | `mvp_golden_test.sh`: WAL-only writer -> materializer -> durable fork -> independent branch, including restarts |
| `pg_control` and branch SLRU/catalog bootstrap | Serialized portable local path implemented | one-shot lifecycle controller plus fresh-initdb golden boot from CRC-bound maps/SLRUs/control |
| Fixed/advancing readers and handoff | Implemented | integration coverage for reader artifacts and view adoption |
| Retention horizon authority | Reader/materializer owners plus page-history, WAL, WAL-index, and forkmeta reclaim frontiers/controllers implemented; control-image pruning, WAL-index replacement bases, and bounded fork-lifecycle history close the reclamation loop for owned timelines | restart/corruption tests, exact-fence admission, branch projection, page/WAL-index publication crash tests, forkmeta observer/controller tests, bounded page churn, and the `pagestore_soak_test` bounded-space run |
| Logical sharding | Implemented with shared-map locking | multi-shard standalone stress |
| Background maintenance | Implemented for POSIX | dedicated maintenance controller; no foreground inline compaction |

The existing CI proves both focused subsystem paths and the composed contract:

- `integration_test.sh` exercises the PostgreSQL-facing storage, control, SLRU,
  reader, and branch primitives;
- `wal_only_redo_demo.sh` proves non-redundant WAL ingest;
- `continuous_redo_demo.sh` proves a live writer and materializer following new
  archived WAL, publishing durable progress, and applying lag backpressure;
- `materializer_lifecycle.jsonl` proves the installed supervisor exclusively
  owns a provisioned WAL-only worker, turns writer checkpoints into durable
  materialized boundaries, replaces a crashed worker, and continues following
  WAL;
- the two H1 materializer plans exercise the checkpointer-child restartpoint
  boundary with a whole-postmaster stop: they inspect R1 and R2 at timeline 0,
  incarnation 1, require main-fork growth and monotonic markers, and retain
  old/new SQL visibility checks.  Meson validates only these plans; the real
  runs are an explicit PostgreSQL CI lane;
- `mvp_golden_test.sh` composes WAL-only ingest, durable materialization,
  a proven recovery-produced SLRU base, portable branch boot from a fresh
  `initdb` skeleton (no parent PGDATA copy), parent/child isolation, and
  store/materializer/compute restarts in one topology;
- `branch_boot_test.sh` proves an independent branch compute can boot, preserve
  fork-point visibility, and write on its own timeline.

The H1 image-layer crash slice is limited to POSIX local layers and process
abort.  Its four ordered stages cover canonical file creation, file writes
before seal, sealed layer data, and durable `layers.manifest` ADD publication;
the write stage does not claim power-loss durability.  The composed
harness keeps the pre-recovery physical snapshot, then checks a sentinel
page/LSN, the expected manifest state, and one additional restart for
idempotence.  A crash after ADD but before its flush watermark conservatively
retains the durable layer and republishes segment-backed coverage once; the
second restart waits for background maintenance to compact that conservative
duplicate to one layer. Clean shutdown alone does not guarantee compaction.

POSIX store opens now hold an exclusive advisory ownership lease across recovery
and provider teardown. Cooperating storage and local-layer
provider users share the same ownership mechanism. After successful manifest
replay, startup reconciles canonical local layer files against manifest-owned
IDs, preserving referenced layers and removing validated unreferenced files.
Legacy relative, symlinked-directory, and dot-dot local URI spellings are
normalized in the replayed map when their parent resolves to the owned store
and their filename matches the layer ID. Unresolvable or foreign-store paths
still fail closed; this is not an arbitrary store-relocation mechanism.
An invalid layer namespace or unsafe file type fails closed before deletion;
unrelated files and object-tier contents are outside this reconciliation.
Missing manifests do not authorize a sweep. Before accepting an ambiguous
manifest-tail repair, recovery durably records an orphan-sweep inhibition
marker; automatic cleanup remains disabled across subsequent restarts because
the repaired manifest cannot prove that omitted files were never referenced.
The persistent lock file must not be removed while a store is in use. Older
binaries and external tools that do not acquire the lock must remain stopped
during recovery; the lock is advisory, not a fence against arbitrary filesystem
writes. This is local POSIX recovery, not SPDK or power-loss certification.
Child processes cannot mutate through inherited provider handles; a child that
inherits an open core must exec a fresh process before using the core. SPDK
storage retains its original caller-owned teardown contract.

## MVP gates

### 1. One composed golden scenario -- implemented

`mvp_golden_test.sh` now composes the acceptance path:

```text
WAL-only writer
  -> continuous materializer
  -> durable materialized horizon
  -> branch prepare/install
  -> independent branch compute
  -> store/materializer/compute restart
```

The test requires the recovery worker's durable materialized watermark to cover
an explicit workload checkpoint, uses that watermark as the child fork LSN,
then materializes a newer parent page and proves the child cannot see it.  It
also builds the child from a fresh same-build `initdb` skeleton, restores exact
checkpoint control, installs the prepared portable catalog/SLRU artifact,
recovers WAL from the store, promotes, and restarts the POSIX store daemon,
writer, materializer, and branch compute.  No stopped-parent PGDATA copy is in
the golden path.  The scenario is wired into CI and is the stable end-to-end
MVP contract.

### 2. Managed materializer lifecycle -- implemented for local POSIX

`pagestore_materializer_supervisor` is a continuously running, stdlib-only
service process for one provisioned recovery worker.  A nonblocking lock
anchored in the worker PGDATA fences duplicate owners even when they use
different status directories.  The supervisor validates the recovery role,
monitors replay and durable lag, issues a fast restartpoint after replay settles,
replaces a crashed worker, and applies bounded exponential retry before
publishing a terminal failure.  Atomic JSON status carries distinct owner
epochs and worker generations; a replacement supervisor adopts an already
running healthy worker.

`materializer_smoke` is now the acceptance client rather than the lifecycle
implementation.  It proves healthy-worker adoption across supervisor handoff,
duplicate-owner rejection, automatic durable progress, immediate compute-crash
replacement, later WAL materialization, and zero writer-observed lag.
Provisioning the initial PGDATA and registering this foreground process with a
deployment's service manager remain deployment orchestration, not page-store
data-path work.

### 3. Safe automatic branch bootstrap -- implemented for local POSIX

`pagestore_capture_slru_snapshot()` now turns a confirmed recovery pause into
that proven base cutoff.  It requests and waits for a restartpoint, requires
the durable materializer marker to equal the unchanged paused replay LSN, then
stages `pg_xact`, commit-ts, and both multixact SLRUs locally.  A second replay
check prevents a concurrent resume from publishing a mixed image; only then
does it publish and sync every staged page under the returned cutoff.  The
golden scenario exercises both its unpaused fail-closed case and the successful
path, replacing its former writer-side expert snapshot calls.

`pagestore_prepare_branch_from_control` accepts that proven SLRU base cutoff,
an exact checkpoint redo, and the materialized fork boundary which covers that
checkpoint.  It requires the matching durable control admission fence and WAL
checkpoint record, then derives every XID, commit-ts, multixact-ID, and
multixact-member horizon from that one control state.  It reconstructs the
otherwise-unrecorded oldest member offset from the same `(C, R]` window, fails
closed on a missing or inconsistent bound, cuts the store branch at the
separate materialized LSN (avoiding exact-R admission-sequence ties), and reuses
the prepared-manifest/store-branch idempotency protocol.  The legacy expert ABI
remains available for compatibility.

`pagestore_branch_prepare` now owns that control-plane window.  It takes the
materializer supervisor's PGDATA lock, rejects a pre-existing replay pause,
captures the proven base `C`, drains and cleanly stops the public writer, and
restarts it on an owner-only Unix socket with autonomous writers disabled.  The
clean stop's shutdown checkpoint is the serialized horizon boundary;
`pagestore_branch_checkpoint()` admits it only when its exact control image and
admission fence are durable, and resolves the checkpoint record's true end `E`
from WAL.  The controller completes and archives that segment, waits for the
materializer through `E`, pauses it again, captures the durable fork `L`, and
prepares maps, SLRUs, and the store branch before resuming the materializer and
restoring the normal writer.  An atomic JSON receipt records `C/R/E/L`, archive
coverage, seeded page count, and whether service restoration completed.

The H1 branch crash slice evolves that receipt into a CRC-protected,
configuration-bound operation journal written before the first service
mutation. Four process-abort points cover the prepared-receipt and
service-restore edges; recovery drops the temporary pin, resumes the
materializer, restores the normal writer, and advances the journal
monotonically to `complete`. Bootstrap installation, layer recovery, and GC
remain outside this slice.

Portable bootstrap installation has a separate golden-scenario crash slice:
installer-backend aborts after maps, in the pg_xact replacement gap, and on
both sides of final manifest publication. It checks startup rejection while
the manifest is absent, unchanged prepared inputs/control, exact artifact
recovery and idempotent retry, followed by branch SQL visibility and isolation.
The target stays offline under one installer; concurrent installation and
power-loss durability are not claimed by these process-abort tests.

The same prepare now captures every default-tablespace database relation map
plus the global map under `RelationMappingLock` into one CRC-protected
`pagestore_branch.bootstrap`.  Its header binds the system identifier, logical
ancestry, exact checkpoint redo `R`, checkpoint-record end `E`, materialized
fork `L`, topology flags, map count, and the exact prepared SLRU manifest.  After
a fresh same-build `initdb`,
`pagestore_control_restore --archive-bootstrap --incarnation I --lsn R` restores exact control
and forces archive recovery without forging shutdown state or checkpoint WAL.
`pagestore_install_prepared_branch_bootstrap` validates that control against the
artifact, installs maps and SLRUs, and publishes the ordinary branch manifest
last.  With foreign initdb WAL removed, recovery fetches the real checkpoint
record and subsequent WAL from the store through `E`, promotes, and continues
on the already-cut page-store branch at `L`.  The golden scenario proves this
path without reading any artifact from the stopped parent.

Portable bootstrap currently fails explicitly when the source or target has a
user-tablespace topology; encoding those paths is outside the default-
tablespace local MVP format rather than being silently guessed.  The local
controller also assumes it owns the writer service lifecycle for the operation:
an outer service manager must not independently restart the writer, while the
shared materializer lock mechanically excludes its supervisor.  The live SLRU
watermark still cannot substitute for the proven capture API: its newest-image
contract deliberately permits bytes newer than its completeness floor and is
therefore unsafe as an exact branch seed.

### 4. Retention-driven space reclamation -- page history bounded; other consumers remaining

Segment GC removes page-log segments covered by image layers, and image
compaction now bounds retained page-version history.  `retention.meta` is the
durable, CRC-protected owner registry for reader, materializer, and configured
pins.
Each pin carries a resource mask for page history, shipped WAL, and the WAL
index.  Controller-assigned stable owner IDs now carry monotonic generations;
the registry rejects stale SET/DROP requests and retains an unenumerated
generation tombstone after DROP so delayed owners stay fenced across restart
and compaction.  Enumeration is available over IPC, and churn is compacted off
the request path.  Recovery truncates only an incomplete final record and fails
closed on any complete corrupt record or a pin whose timeline is absent.

Managed readers and materializers install and advance durable owner generations
before consuming retained history.  The effective-floor query projects
explicit descendant pins through every branch cap, derives permanent fork-point
pins from timeline metadata rather than duplicating them, and folds every
branch-visible restorable control image into the WAL resource.  Page compaction
consumes exact tuple fences, publishes its durable frontier before source
retirement, and has bounded-churn, relation-lifecycle, descendant, and
publication-crash coverage.  R3b-1 is present in the standalone WAL store: its
v2 identity durably records and checksums the physical directory start, retained
base, and append end; reopen validates those values against a complete
contiguous segment directory; old v1 identities migrate only after that
validation; and callers can monotonically advance the logical retained base
without deletion authority.  R3b-2 adds the standalone
`ps_wal_store_reclaim_prefix()` primitive.  It publishes retained-base and
physical-start frontiers atomically under the WAL mutex before unlinking only
segments below an aligned target; the mutex drains in-flight reads and blocks
new below-frontier reads, partial unlink keeps the memory catalog aligned with
successful unlink calls, and ambiguous directory fsync fences until reopen.
Reopen validates the authorized suffix and can retry residual prefix files
idempotently.  Residual candidates are fully enumerated and validated before
any unlink, sorted in ascending order, and required to form the contiguous
suffix immediately below the target.  The main catalog path likewise validates
each complete header, length, and payload CRC immediately before unlink; a
corrupt low catalog segment deletes nothing, while a corrupt middle segment may
delete only the already validated lower prefix and never the corrupt or higher
segments.  Focused coverage includes real fork/`_exit` restart points,
scan-error zero-unlink behavior, complete per-segment validation, corrupt
catalog/residual fail-closed behavior, repair-and-retry, and a deterministic
read-versus-reclaim mutex barrier.
R3b-3 is the conservative POSIX/core policy integration.  It admits at most
one LIVE timeline per maintenance tick ahead of continuous tier/remote-GC work.  A cheap WAL-lock-only preselection avoids draining admission when no complete prefix exists, and a bounded no-progress backoff suppresses repeated drains while a safe floor remains in the boundary segment; a selected candidate drains ordinary admission before
freezing the WAL index, snapshots raw WAL dependencies under short-lived
shard/map protection, and releases all shard locks before control-image,
layer, metadata, or unlink I/O.  Its deletion candidate is the aligned-down
minimum of the effective WAL retention floor, durable WAL-index progress, and
the oldest surviving raw WAL dependency.  Missing durable proof, malformed or
pending snapshot state, non-POSIX providers, and publication failures all fail
closed with a one-second retry backoff.  Durable retained-base metadata is the
sole restart-stable admission frontier; physical directory start is not a
runtime fence.  WAL reads recheck that fence under each visited timeline's WAL
lock; inherited history may bypass a child's natural local base and is then
rechecked against the parent, while pre-metadata timelines retain local WAL
readability.  Descendant control history and a target child's reclaim candidate
are capped at their branch points, durable index progress beyond the sealed
prefix remains a valid proof, and a durably DELETED descendant no longer pins
its parent (DELETING still does).  Reopen reconstructs residual-prefix work so
an already-published frontier can finish idempotent unlink without a new proof.
Focused core coverage (66 checks) includes idle preselection, ancestry and timeline
isolation, natural nonzero child fallback, child-local controls, target branch
caps, LIVE/DELETING/DELETED floors, naturally nonzero starts, unaligned and
boundary-crossing flat progress tails, snapshot recovery plus WAL/WAL-index
re-ship admission after base advancement, pre-metadata reads, restart/residual
retry, read/frontier publication and floor-scan lock-order races, fenced
residual-query suppression, pending-proof cleanup failure,
metadata publication failure/backoff, and admission concurrency.

This is a conservative R3b-3 policy integration.  It does not include sparse
or discrete retained-base crossing, or a bounded fixed-reader soak.  Still
required for the gate:

- shipped-WAL reclamation without crossing the durable control/WAL floor;
- WAL-index log compaction/reclamation;
- fork-metadata compaction/reclamation;
- timeline deletion and its layer/WAL cleanup.  The first R5 lifecycle slice
  now migrates legacy-only timeline logs to V2 and persists V2 create/event
  records, exposes LIVE/DELETING plus
  a reserved DELETED format value with incarnation, and vetoes BEGIN_DELETE
  for descendants or active retention owners.  The POSIX runtime-quiescence
  slice now drains complete ordinary requests (including reads and reserves)
  and complete maintenance work, including async workers, with a fair lifecycle
  turnstile.  Deleting timelines now reclaim their manifest-owned local and
  remote layer artifacts through restartable MARK_DELETE/REMOVE_LAYER GC, with
  idempotent remote retry and completed-but-unpublished upload reconciliation.
  Shared fork metadata is now filtered through the existing crash-safe
  snapshot/epoch cutover whenever an owner enters DELETING, without pruning
  surviving live or pre-metadata owners in that forced generation.  POSIX
  maintenance now validates and removes each deleting owner's private flat and
  immutable WAL plus WAL-index epochs, watermarks, and snapshots; it also
  atomically filters shared page segments and updates survivor offsets.  Partial
  deletion resumes after restart while sibling artifacts survive.  POSIX
  maintenance now durably revalidates all deletion consumers and fsyncs the
  same-incarnation DELETED state event before publishing it in memory; DELETED
  timelines remain defined, reject normal operations, and expose their current
  incarnation token through STATE.  POSIX CREATE_BRANCH can reuse an ID only
  from durable DELETED with exactly the next nonzero incarnation; delayed
  requests from older incarnations are rejected.  SPDK async drain remains
  explicitly fail-closed: its daemon rejects reuse CREATE_BRANCH requests
  before entering the shared core.  WAL and control restore clients carry the
  immutable token from their startup configuration/manifest on every request;
  control restore never queries mutable STATE.

The `timelines` log uses CRC-protected records, rejects truncated or corrupt
entries, and atomically migrates complete legacy logs before opening the store.
R5b-1 provides disabled-by-default, hysteretic POSIX backpressure for page
segment and shipped-WAL reclaim debt.  R5b-2 adds the same foundation for
WAL-index append tails and physically obsolete snapshot/epoch files, using
bounded lock-free filesystem observation and shared-memory inspection.  R5b
also covers forkmeta with a compacted-source baseline, append growth, and
obsolete snapshot/temp debt; its forkmeta gate is limited to forkmeta-growing
mutations and its snapshot maintenance can be forced below the geometric
trigger.  All three controllers are POSIX-only; forkmeta does not claim the
remaining R6 queue-bound soak/tuning work.

The R6 bounded-space soak (`pagestore_soak_test`, standalone CI) now drives one
real POSIX daemon with a seeded writer over a bounded live set, a materializer
publishing exact cutoffs, fixed and advancing readers, short-lived
copy-on-write branches created and durably deleted, and clean/crash restarts,
measuring every persisted category against declared bounds.  Its first runs
found three retention gaps that are now closed on `pagestore`: control-object
versions were never pruned, so the WAL retention floor never advanced;
WAL-index compaction retained an FPI-led chain per page even when a durable
stored page version (or a fork-level death) already covered it, so cold pages
pinned raw WAL forever; and a branch timeline without a page frontier blocked
forkmeta compaction for every timeline (such a branch now caps the cutoff at
its fork point instead).  With those fixes the CI-sized run
keeps page, layer, WAL, WAL-index, retention, and timeline storage within
bound and reclaims shipped WAL down to the last immutable segment.  A fourth
gap showed only in longer runs: the forkmeta planner retained every visible
SET/DEAD event as an invalidation fence, so truncate/unlink churn grew the
checkpoint linearly, and deletion-forced generations copied every surviving
record.  Now image compaction drops page versions that a later truncate or
drop invalidates at every horizon they serve, the planner keeps only the
base, the inheritance fence, and the growth per horizon plus the definitive
events a retained version or a still-indexed WAL record needs, and a
deletion-forced generation compacts survivors whenever a cutoff is proven
(`pagestore_lifecycle_prune_test`).  Review of those fixes tightened two
rules that the soak's owner mix could not expose: a stored replacement base
is trusted only at a WAL-index horizon whose own owner also holds page
history (another owner's page fence at the same LSN can move first), and
forkmeta compaction treats every WAL-index horizon as a fork-history horizon,
so a WAL-index-only owner between two truncates keeps the death its chain
was retired against and the regrowth after it
(`pagestore_wal_reclaim_core_test`).  With that, 8000-round soaks keep every
category, forkmeta included, within bound.  Still required for the gate:

- no owner establishes the durable operational cutoff that controllers
  respect: the real materializer pins WAL and the WAL index but not page
  history, and a branch compute pins nothing, so in that topology page
  history and control images are never pruned, WAL-index compaction cannot
  substitute stored images for FPI chains (a stored base is only trusted at a
  page-history fence), and shipped WAL stays pinned by cold pages.  The
  checkpoint admission fence those computes already mirror is the intended
  cutoff, but branch/reader preparation must select and pin its horizon
  before that cutoff can pass it;
- SLRU-class object versions are still retained without a dedicated protocol;
- a nightly long-run soak configuration.

### 5. Composed crash and format-compatibility coverage -- partial

The POSIX image-layer publication slice is now covered by the declarative
harness.  Other crash boundaries remain outside this slice.
Before declaring the MVP repeatable, add process-level fault scenarios around
manifest replacement and retention/reclaim/GC, plus
a persisted-format fixture for restart/upgrade compatibility.

## Recommended sequence

Keep the composed WAL-only -> materializer -> branch scenario green as the MVP
acceptance contract.  The implementation sequence for the remaining gates is:

1. Integrate immutable WAL segments, compact the WAL index, then reclaim raw
   WAL without crossing retained reconstruction bases.
2. Compact fork metadata, add durable timeline deletion, and prove total-space
   bounds with reclaimer backpressure.
3. Promote the golden scenario into the declarative crash/compatibility harness
   and add persisted-format fixtures.

Performance refinements such as size-tiered compaction, layer key-range pruning,
bloom filters, per-shard layer maps, asynchronous POSIX I/O, and explicit
CPU/IO scheduling remain important, but they follow the functional and
operational gates above unless measurement shows they block the MVP scenario.
