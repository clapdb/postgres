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

The H1 page-pruning slice reuses that harness with a deterministic IPC
workload (`pagestore_gc_crash_client`): three generations of relation history
plus a newer block, then a configured page-history owner at the cutoff that
lets maintenance retire the older history.  The workload arms the named fault
itself right before it installs the cutoff, because the flush-driven
compactions that run while the history is written have nothing to retire.
Three process aborts cover the durable page-prune frontier, the compacted
layer's manifest publication, and the retired layer's mark-delete step.  Each
crash keeps the physical snapshot (a durable frontier file after the frontier
stage, a non-empty manifest and at least two local layers otherwise), and
recovery must serve the published newest block and the retained history at the
cutoff, refuse the retired pre-cutoff version, reconcile the manifest to the
local layer set with no pending deletions, and republish the configured
horizon; a second restart proves idempotence, and idempotence now means the
durable state itself -- the layer files, the layers the manifest publishes,
and the prune frontiers -- is unchanged by that restart, not merely that it
passes the same checks.  Both snapshots are taken with the daemon stopped:
readiness is published as soon as the maintenance thread exists, so a
comparison made while it runs can precede the pass startup marked due.  Compaction accordingly leaves a converged shard
alone: a single source with nothing to prune is already its own compacted
result, so it is no longer rewritten into a fresh layer by the pruning pass
each startup marks due.

The same workload binary carries a `wal_index` workload for the WAL-index
compaction boundary: one metadata-complete interval on timeline 0 with a
fixed WAL-index reader at 40 below FPI-led chains at 10/30, 50/70, and
90/110, published under a one-byte `--walidx-snapshot-bytes` trigger so the
first committed interval is a compaction candidate.  The process abort after
the durable frontier must leave the frontier file and the staged, uncommitted
generation, whose identity the oracle records; recovery must commit that same
generation -- not an equivalent one rebuilt under a new number, which the
frontier was never published for -- and must serve the reader's exact chain
and the newest chain while refusing the dropped middle point, and keep the WAL-index owner; a second restart proves idempotence.

The `wal_reclaim` workload ships three complete 1 MiB segments on timeline 0,
publishes a control note whose redo is the shipped end, arms the fault, and
commits WAL-index progress through that end so the whole sealed prefix is
reclaimable.  Three named store-lock probes crash after the durable physical
frontier and before the first unlink, after each authorized segment unlink,
and after the last unlink but before the directory fsync that retires the
residual prefix.  The crash snapshot must carry the durable store
metadata with the directory start and retained base already at the shipped
end -- before the first unlink that frontier is the only thing separating the
crash image from the state before publication -- and exactly the expected
number of sealed segments; recovery must finish the unlink retry, refuse
reads below the frontier, keep the WAL end and the retain floor at the
shipped end, clear the reclaimer's physical debt, and leave no retention
owner; a second restart must then reproduce that settled shipped-WAL state,
metadata and files alike.

The `timeline_delete` workload creates a branch of timeline 0 with its own
shipped WAL, a committed WAL-index interval, and enough relation pages for an
owner layer and several shared segments, then arms the fault and issues
BEGIN_DELETE.  Four lock-held probes crash after the fsync'd DELETING event
and before its publication, after the owner's private WAL and WAL-index
artifacts are removed, after a shared page segment is atomically rewritten
without the owner's records, and after the fsync'd DELETED event and before
its publication.  The crash snapshot must keep the private WAL at the first
boundary, have removed it from the second on, and leave no owner artifact
once DELETED is durable; recovery must reach DELETED with the incarnation
token, keep serving the parent's page, reject branch reads, leave no owner
artifact, reconcile the manifest, keep the root's history capped at the live
sibling's fork point, and register no owner; a second restart
proves idempotence.  A fifth probe crashes on the old-state side of the
first transition, where the request is lost: the branch must keep its
lifecycle, its artifacts, and its persisted ancestry -- parent, fork point
and parent token, none of which the pages it serves would reveal -- and the
root must still carry the cap both live branches fork at.

The `manifest_compact` workload writes 320 relation pages while the harness
holds maintenance paused, so the write path flushes layers and their manifest
records but layer compaction and the manifest rewrite wait; it then arms the
fault and releases maintenance.  Two map-held probes crash after the
compacted temp log is fsync'd and before the rename, and after the rename
and before the directory fsync.  The crash snapshot must keep a non-empty
live log with the temp file absent after the rename and, before it, present
and already replaying to the same layers as the live log -- a temp file that
had only been created would satisfy a presence check and then be discarded by
recovery, which replays the intact live log and passes everything after it;
recovery must replay either log to a sane manifest reconciled with the local
layers, serve every page written before the rewrite, remove a crashed temp
log on open, and register no owner; a second restart proves idempotence.

Compute-restart combinations are composed through a `restart` operation in
the writer and materializer runtimes.  The writer runtime restarts the writer
or an installed pinned reader with a fast shutdown and then asks the target
itself whether it came back as itself -- out of recovery, and, for a pinned
reader, still at the horizon its own GUC pins it to -- because `pg_ctl -w`
establishes only that the PID file says connections are accepted; a pinned reader's
shutdown checkpoint rewrites its `pg_control`, so its restart restores the
boot control image at its immutable identity before starting, as the
documented reader protocol requires.  The materializer runtime restarts the
writer, the materializer worker (the supervisor replaces the cleanly stopped
worker with a new generation), or the store, where the supervisor stops
first, both computes shut down, the daemon restarts on the same shared
memory name, and the writer and supervisor return.  The two scenarios
require the pinned reader to keep its horizon and hide the in-flight
prepared transaction across its restart and the writer's -- including
after that transaction commits -- and the materializer to serve the last
durable boundary as soon as its replacement is up, then each boundary
after a writer restart, a worker restart, and a store restart with zero
lag at the end.  Each restart event in both runtimes records the instance the
restart actually replaced -- the writer's, reader's or daemon's process as a
PID with its start time, since the OS may hand the replacement the same PID,
or the materializer's worker generation -- and fails if it is unchanged, and a writer restart
invalidates the declared checkpoint, so a later reader base or capture
must declare a new one.  Remaining outside
the harness: branch-compute restarts, which the golden scenario covers.

The `forkmeta` workload composes the four fork-metadata publication probes
(after the fsync'd prepared generation, after the manifest commit, after the
source-epoch rewrite, and after the retired generation's GC) on the daemon:
the page-pruning history proves the cutoff through its frontier, thirty-two
relations carry create, zero-extend, and truncate events on both sides of
the cutoff, and a trickle of further fork events after the cutoff drives the
second generation that retires the first.  Snapshots check the staged
generation without a selected manifest, and the selected manifest before and
after the source-epoch marker; every crash image must hold exactly the
selected generation's two files -- the first generation's at the commit and
the rewrite, the second's after GC -- because startup schedules snapshot GC
unconditionally, so an orphan generation left by a faulty publication would
be swept away before recovery is inspected;
recovery must settle on the generation the crash had already selected --
these probes hold the locks that would let an acknowledged write land, so
there is nothing new to publish -- behind its marker, and serve
every relation's current size and its retained size history above the
cutoff, refuse size queries below the cutoff, keep the page-pruning
guarantees, and republish the configured horizon.

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
(`pagestore_wal_reclaim_core_test`).  Two consumers were then aligned with
the compacted index: single-page redo asks the daemon for the newest fork
death at or below its horizon (`PS_OP_BLOCK_DEATH`) and starts from a zero
page there whenever that death is newer than the full-page image or stored
version it found, because the index is the union of every horizon's chain
and may still list pre-death records for an older owner; and an SLRU seed or
reader snapshot shipped at a cutoff that page compaction already passed
without a fence is refused, since the control image it would resolve its
era from is gone (`pagestore_lifecycle_prune_test`,
`pagestore_control_prune_test`).  For those consumers to be exact at a
WAL-index-only owner's horizon, forkmeta compaction keeps the size envelope
at every horizon (each definitive event that is the newest death of some
block, not only the latest and the smallest), and the as-of size, existence,
and death queries are admissible at WAL-index horizons below the page
frontier like the WAL-index reads retained for that owner are
(`pagestore_forkmeta_prune_test`, `pagestore_lifecycle_prune_test`).  Those
identities are compared as (LSN, admission sequence) tuples end to end, the
order `fork_page_invalidated()` already applies, so a page clamped to the
LSN of the truncate it was written after keeps its bytes; an admitted
artifact registers its fence under the same lock as the check that admitted
it and releases that fence again if the append fails; a note whose image is
still on its way keeps a single copy across mirror retries; a stored base,
size, or death the daemon cannot answer fails single-page redo closed
(the SPDK frontend reports a failed page read as an error, never as an
absent version); and the soak measures allocated blocks rather than
logical length and bounds the file count.  With that, 8000-round soaks keep every
category, forkmeta included, within bound.  The operational cutoff no longer needs a
page-history owner: the compute that writes a timeline's pages mirrors it
already, and the daemon derives the page-history floor from that mirror
(`control_checkpoint_cutoff`): the materializer's own WAL/WAL-index pin,
which it advances to the redo of each durable restartpoint, while a
materializer owns the timeline (the writer's own checkpoint notes run ahead
of materialization and are ignored there), otherwise the redo of the newest
durable checkpoint note of a direct-write compute.  Explicit pins
still win when lower, later pins are refused below the derived frontier as
before, and the branch controller's temporary base pin now carries page
history so the base is an explicit fence while the branch is prepared.  The
exact-redo twin of a kept checkpoint image is retained with it, which is what
an earlier attempt at this floor had missed
(`pagestore_control_prune_test`; the soak now models the materializer with
its real WAL/WAL-index mask and a progress marker).  Still required for the
gate: none.  The materializer's own WAL-index horizon is page-protected by the
cutoff derived from its pin (the base at that horizon and the horizon itself
are the same pin and move together), so stored pages replace its FPI-led
chains and cold pages no longer pin shipped WAL beyond the reclaimer's
declared bound.  That exception stops where the pin coincides with a standing
horizon: the durable WAL-index frontier and the shipper's progress admit a new
WAL-index-only owner at exactly their LSN, which would arrive after the
materializer advanced and the base was retired, so a pin at either keeps its
FPI-led chain.  The plan is built before publication is excluded and progress
can advance in between, so the standing horizons are rechecked under the
publication lock and an exception that has since become one is withdrawn.
SLRU-class and reader-artifact versions now have their retention protocol.
Seeds and reader snapshots are exact-generation artifacts: a consumer reads
every page of the object at exactly the generation it captured, which is
the newest generation at or below the horizon it pinned or forked at, so a
page copy is kept only when it belongs to the newest generation at or below
the floor or some fence (a copy from an older generation of a page the newer
generation no longer has serves nobody and is retired with its control-era
fence).  Only a seed is a replay base, though: a horizon above the floor is
served by the newest seed at or below it plus the WAL after it, while a
reader snapshot resolves at exactly the horizon it was captured for, so a
snapshot is kept only while a fence names that horizon and no longer pins its
control era once its reader is gone.  The live mirror, tombstones, and watermark are read at the newest
horizon by their consumer and at the fork point by a branch, so they keep
only the newest version and the newest at or below each fence.  Retried
copies collapse to one, and a retired artifact releases the control era it
fenced (`pagestore_control_prune_test`).  One limitation is deliberate and
documented: a generation is defined by the pages that carry its LSN, and
nothing marks a publication complete, so a publication that appends some
pages and then fails is indistinguishable from an object that shrank.  Such a
partial generation supersedes the complete one below it, and a consumer at
that cutoff then fails to reconstruct rather than silently reading a stale
page from the older generation.  Making the newer generation wait for a
durable completion marker is part of the artifact-publication protocol, not
of retention.  The same missing lifecycle shows at object granularity: the
newest generation at or below the floor is the replay base for every horizon
above it, so it is kept even when the object it describes is gone (a reader
snapshot of a dropped database publishes no newer generation of that key).
Retiring it needs a durable drop event for the artifact, which the
publication protocol does not emit yet.

The long-run configuration the gate asks for is the
`pagestore nightly soak` workflow (`.github/workflows/pagestore-nightly.yml`):
three seeds at 8000 rounds on a daily schedule and on demand with chosen
seeds/rounds, one job per seed, with every JSON report summarized in the job
and kept as a 30-day artifact.  Each job takes the time its rounds need
instead of a fixed limit, and a dispatch too large to finish inside the
hosted-runner limit is refused rather than killed before it reports.  GitHub fires scheduled and dispatchable
workflows only from the repository's default branch, so the daily lane starts
once this file reaches it, and a run started there checks the soak's own
branch out explicitly; until then the same configuration is run on demand.
The branch is resolved to a commit once, before the seeds fan out, and every
job checks that commit out, so a run's seeds stay one experiment even when
the branch advances between jobs or a job is rerun later; the resolved
revision is reported in each job summary.  A
run that cannot write its JSON report fails rather than passing with nothing
to compare across nights.

### 5. Composed crash and format-compatibility coverage -- partial

The POSIX image-layer publication, page-pruning, WAL-index compaction, WAL
reclaim, timeline deletion, manifest replacement, fork-metadata publication,
and compute-restart slices are now covered by the declarative harness.  The
deletion slice crashes on both sides of its first transition: before the
DELETING record is durable the request is lost and the branch must survive
intact, and after each later boundary the cleanup must resume.  Its workload
seeds a live sibling branch with the same kind of private state, so cleanup
that reached past its owner would be caught.  Before declaring the MVP
repeatable, add a persisted-format fixture for restart/upgrade
compatibility.

An advancing reader's data directory boots from the checkpoint its manifest
names, and the reader moves its own retention pin above that horizon as it
adopts newer published views.  Nothing then keeps the boot control image
alive, so a restart of that data directory cannot restore it once pruning has
run.  Until an adopted horizon is written back into the reader manifest, the
controller owns that image's lifetime and must hold a page-history horizon at
it; the integration test models exactly that.

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
