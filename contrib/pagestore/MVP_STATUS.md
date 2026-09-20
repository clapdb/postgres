# pagestore MVP status

This page is the progress source of truth for the pagestore MVP.  The other
documents in this directory describe subsystem designs and longer-term target
architecture; their future-looking sections do not by themselves define MVP
scope or completion.

The ordered work packages, acceptance criteria, and open decisions for closing
the remaining gates are tracked in
[`MVP_COMPLETION_PLAN.md`](MVP_COMPLETION_PLAN.md).

The evidence assessment and proposed release-qualification tests are in
[`RELEASE_VALIDATION.md`](RELEASE_VALIDATION.md). They distinguish the accepted
MVP gates from the additional evidence needed for a supported production release.

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
| Fixed/advancing readers and handoff | Implemented | integration coverage for reader artifacts and view adoption; the explicit exact-R reader snapshot now publishes under an owner-scoped key distinct from the automatic checkpoint-driven snapshot's key, after PRs #264/#265/#266 hit a silent same-key collision in CI (RELEASE_VALIDATION.md's "Integration-lane finding") -- `pagestore_artifact_lifecycle_test.c`'s `test_reader_snapshot_owner_key_split` plus a deterministic collision-order check in `integration_test.sh` cover it |
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
selected generation's two files and no publication temporary -- every part,
prepared intent and manifest is renamed into place, and startup's temp GC
would sweep any debris away before recovery could be inspected -- -- the first generation's at the commit and
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

### 4. Retention-driven space reclamation -- implemented for local POSIX; artifact publication and drop are durable

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
one LIVE timeline per maintenance tick ahead of continuous tier/remote-GC work.  A cheap WAL-lock-only preselection avoids draining admission when no complete prefix exists, and a bounded no-progress backoff suppresses repeated drains while a safe floor remains in the boundary segment.  The backoff is proof-keyed, not clock-only: after a 20 ms rate-limit floor (WAL_RECLAIM_REARM_MIN_NS -- every re-evaluation is a full drain, and a retention pin drop is dispatched without the admission lock, so an unrated cancellation would let drop-heavy churn turn every drop into a drain), it ends at the earlier of one second or the next event that can move a proof input (a WAL-index publication or GC, durable WAL-index progress, a retention-registry change, or a timeline reaching DELETED), so a floor advance past that 20 ms floor is not left waiting on the one-second clock.  When a complete segment's retention floor and durable progress have both passed the boundary but its raw WAL-index dependency has not, the reclaimer requests one compacted WAL-index publication on its own behalf instead of waiting for the WAL-index controller's own tail trigger or high water: one publication + GC + reclaim pass after the blocking condition clears, no earlier than the 20 ms floor after the last arm.  The request is fence-keyed, not progress-keyed: it is re-issued only when the oldest raw dependency or a retention-registry fence (a pin reserved/dropped -- including a WAL_INDEX-only pin, which the compaction plan fences exactly like a PAGE_HISTORY/WAL pin -- an artifact fence released/opened, a branch cap released) has changed since the last served request, because a durable WAL-index progress advance alone -- published once per indexing batch by the backend materializer -- can never retire the blocking item, and re-requesting on every advance would be a sustained non-compacting rewrite for as long as an unreplaceable dependency blocks the segment; when the request would be fruitless, the reclaimer arms a watch on the blocking page at every fruitless evaluation and keys the request on retirement evidence computed over two separate windows, not one: retain_chain applies per horizon, so a nearer protected horizon P (a page-history/walidx fence) accepts a durable base or a newer FPI in [item end, P], while the farther unprotected durable-progress horizon U needs a newer FPI in [item end, U] specifically -- collapsing both to whichever horizon is nearest, as an earlier version of this watch did, misses the ordinary case where an FPI lands strictly between P and U and satisfies U without ever being in [item end, P].  A flush or index add that changes either window's evidence wakes the watch within the 20 ms floor (only while that window has not already found what it needs, so a base or FPI already present cannot trigger a further, useless wake from the same watch on every later unrelated flush of the same shard), and the next evaluation (<= 1 s idle) catches any change that arrived without a wake; a request is not re-issued while both windows' evidence and the raw floor and fence epoch are unchanged, though a count of watched items that changes without their content doing so (a partial retirement) costs one extra fruitless re-request as a bounded nit, not a repeat.  Separately, when the retention floor alone holds the boundary and the note that sets it is superseded -- below the PAGE_HISTORY effective floor and not the newest note at or below any control-note prune fence -- but still memtable-resident, the reclaimer requests a flush of the control shard so the next compaction can prune it, keyed on (note lsn, admission_seq, fence_epoch) so an unchanged decision is never re-issued, and marks the shard's page-prune-due flag only together with that flush request; the newest note at or below a live fence is never touched, and a layer-resident superseded note is left to the due mark the fence change already set.  A selected candidate drains ordinary admission before
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
or discrete retained-base crossing, or a bounded fixed-reader soak.  The
consumers the gate still required at that point have since landed:

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
fenced (`pagestore_control_prune_test`).  Exact-generation artifacts now have a durable publication and retirement
protocol: BEGIN fences a write attempt, COMMIT selects only its fully synced
pages, and DROP records absence without crossing reader or branch retention
fences. Incomplete generations no longer supersede complete ones, and removed
databases' manifest/relmap data can be reclaimed after their last dependency
is released. The launcher retires removed keys before replacing its durable
database inventory, making interrupted cleanup retryable. Small lifecycle
metadata tombstones remain to prevent resurrection; this does not promise
bounded metadata for infinitely many distinct keys. Admission refusals
(an unfenced generation LSN, growth ordered before a forkmeta cutover) are
named and leave the store otherwise untouched; only a real storage/sync
failure fails the artifact path closed until reopen, and the cutoff
derivation guarantees a generation at a pinned LSN is admitted after any
forkmeta cutover (`cutoff <= frontier <= floor <= pin`; see
`RELEASE_VALIDATION.md`'s R5-5 writeup). See
[`ARTIFACT_LIFECYCLE.md`](ARTIFACT_LIFECYCLE.md) for retry, recovery, legacy
migration and the minimum-reader store format.

Two nightly dispatches (2026-09-13/14, runs 34747373574 and 34825221247) failed
seed 20260909's `wal` bound by a few KiB at the same sample (4698112 against
4685824), both times between rounds 6400 and 6500.  The failure is explained
and closed.  Physical shipped WAL in this workload is not governed by the WAL
controller's high-water -- its lag counts only proven-reclaimable bytes, so an
unproven interval is deliberately not lag and the controller never throttles
here -- but by how often the WAL reclaimer's raw WAL-index dependency floor
moves, and that floor only retires an item at a compacted WAL-index snapshot
publication.  Nothing requested one on the reclaimer's own behalf, so a fully
proven segment waited on the WAL-index controller's own cadence (~1000 rounds
here, driven by its 128 KiB high water and a 100 ms observation timer), and a
fixed one-second no-progress backoff -- uncancelled by the publication that
unblocked it -- added up to another 237 rounds on the hosted runner.  Both
mechanisms are now fixed (R3b-3 paragraph above): an on-demand, fence-keyed
WAL-index compaction request for a segment blocked only by the stale raw
dependency, and a proof-epoch-keyed backoff, rate limited to a 20 ms floor,
that ends as soon as a proof input actually moves.  Independent review found
two follow-on gaps in the first version of this fix and both are closed
before merge: the backoff's epoch must be the one read before this attempt's
own proof inputs (retention floor, durable progress, raw dependency), not a
fresh read at arm time, or a retention pin dropped without the admission
lock in the window this attempt has released walidx_prune_lock/wal_lock for
the retention floor scan can bump the epoch before the backoff records it, a
lost wakeup; and the request must be re-issued on a fence change (a pin
reserved/dropped, an artifact fence released/opened, a branch cap released),
not on every durable WAL-index progress op, since the backend materializer
publishes progress once per indexing batch and a progress advance alone can
never retire the blocking item.  The soak's `wal` during-bound is restated
from five declared terms instead of a formula that named a term (the
controller high-water) which never engages here: `WAL_HIGH_WATER +
WAL_SEGMENT_BYTES + FENCE_WAL_BYTES_MAX + RECLAIM_REACTION_WAL_BYTES +
BRANCH_WAL_ALLOWANCE = 4667392`, 18 KiB tighter than the bound it replaces,
where `FENCE_WAL_BYTES_MAX` is the workload's longest-lived fence (the fixed
reader's `READER_LIFE + 37` rounds) and `RECLAIM_REACTION_WAL_BYTES` is two
materializer intervals of reclaimer reaction latency -- one WAL-index
publication + GC + reclaim pass after the blocking condition clears (a raw
dependency moving, or a retention-registry fence removing what was blocking
compaction at the same durable progress), no earlier than the 20 ms
rate-limit floor after the last no-progress arm; the allowance is >= 330 ms
on a hosted runner.  A per-sample check ties physical `wal` directly to the
fences the soak itself holds (`model_floor`, the minimum of the materializer
pin, durable progress, every held reader, and a live branch's cap) rather
than to the WAL-index controller's cadence, and reports the observed margin
as `wal_fence_slack_max` in the JSON report; its expected composition is up
to 1 MiB of segment alignment, up to another 1 MiB for one fully proven
segment caught between clearing its boundary and actually reclaiming
(publication + GC + the 20 ms floor + the reclaim pass), and ~130-200 KiB of
raw WAL-index items retained for a held reader or branch below its own
horizon -- so values up to roughly 2.1 MiB are the expected range, not a
regression signal.  A second review round found two more gaps, both closed
before merge: a WAL_INDEX-only retention pin (no PAGE_HISTORY or WAL bit) is
fenced by the compaction plan (`walidx_prune_fences`) exactly like a
PAGE_HISTORY/WAL pin, but the three pin-mutation sites only bumped the
reclaimer's proof and fence epochs when the changed resources included
PAGE_HISTORY or WAL, so dropping or moving a WAL_INDEX-only pin left the
reclaimer fruitless-suppressed until the WAL-index controller's own trigger
happened to notice independently; and the reclaim-due request's
`walidx_snapshot_end[tl] < progress` guard was removed, since a fence change
can make the compaction plan drop more raw WAL-index items at the very same
durable-progress-covered end_lsn a prior publication already reached --
"already covers current progress" was never evidence that nothing more
could be dropped, and the publish path itself already admits a same-end_lsn
republish when reclaim_due is set.  With the fix, seed 20260909 at 8000
rounds peaks at ~1.7-1.8 MiB (over a 2.5x margin under the new bound)
instead of sitting at the old bound, stable across CPU regimes and repeated
runs; the first post-fix nightly dispatch,
[run 35458043758](https://github.com/clapdb/postgres/actions/runs/35458043758)
(manually dispatched 2026-09-19 against `316401d8d4b`, the #264 merge
commit on `pagestore`, with this fix -- #265 -- already merged in its
ancestry), confirms it: seed 20260909 at 8000 rounds passed with 45024 checks,
0 failures, and both bounds satisfied; physical WAL peaked at 1736704 bytes
against the 4667392 bound, with `wal_fence_slack_max` at 1568768 bytes. That
is one dispatched run, not yet a scheduled-run history; the three-seed
scheduled nightlies must still accumulate green runs against this revision
before the final MVP status update.

A later review of the soak's trace window (rounds 4860-4980) found the WAL
floor's apparent stall there was not a pruning lag: `daemon_floor =
16731136` was the redo of the control note written at round 4760, the
newest note at or below the fixed reader's fence (pinned at round 4820, LSN
16822272, between the notes of rounds 4760 and 4800) -- required retention
under the newest-note-at-or-below-each-fence rule (`control_prune_fences`),
not a bug.  When the reader re-pinned at round 4960 the note was released
and the floor moved at round 4980, within one compaction pass.  Two
residuals remained after the fix above, both closed here (2026-09-20): (1)
a replacement base or full-page-image item that becomes durable/arrives
with no retention-registry fence change at all was previously left to the
WAL-index controller's own trigger; the reclaimer now arms a watch on the
specific blocking page's window at every fruitless evaluation and keys the
request on retirement evidence (the newest durable version and newest
full-page-image item inside that window), so a flush or index add that
changes the evidence wakes it within the 20 ms floor and a missed wake is
still caught by the next evaluation.  (2) a superseded control note that is
memtable-resident is invisible to compaction, which reads image layers
only, so the WAL floor it sets stuck around until the control shard's
memtable filled on its own (2 MiB of page writes at the default
`flush_pages`); the reclaimer now requests a flush of the control shard
when the note term alone holds the boundary, keyed on (note lsn,
admission_seq, fence_epoch) so an unchanged decision is never re-issued,
and marks the shard's page-prune-due flag only together with that flush
request -- a layer-resident superseded note is left to the due mark the
fence change already set.  Neither residual is
exercised by the soak's own workload (it flushes every 8 pages and
re-permits the request on every materializer publication anyway), so the
soak's `wal_fence_slack_max` composition and acceptance bound are
unchanged; see the new reclaim-core test cases
(`test_late_durable_base_requests_compaction`,
`test_late_fpi_requests_compaction`,
`test_superseded_note_in_memtable_is_pruned`) and the soak comment for the
closed mechanisms.

An independent review of the two closed residuals above (2026-09-20) found
the branch lock-/data-safe but flagged a design gap in each fix and three
lower-severity issues, closed here with the same branch and reclaim-core
suite: HIGH-1, residual 1's fire-and-epoch design only closed the case
where an evaluation happened to observe the base already durable in the
memtable; a base written and flushed *between* evaluations, or one filling
the memtable to trigger its own flush outside that window, was never
noticed.  Fixed by dropping the `have`-gated arm in favor of arming a
per-timeline watch (`WAL_RECLAIM_WATCH_MAX = 33` entries) at *every*
fruitless evaluation, and by keying re-requests on retirement evidence --
the newest durable version and newest full-page-image item inside
`[lo, h_cap]` -- computed and compared at each evaluation regardless of
whether a fire happened; `walidx_reclaim_base_epoch` is retired to a
wake-up-only role (canceling backoff early, never itself deciding
fruitlessness).  HIGH-2, residual 2's superseded-note predicate (below the
PAGE_HISTORY floor alone) was a superset of what compaction's twin rule
actually drops -- a checkpoint note that is an exact-redo twin of a note
still required by a live fence looks superseded to the reclaimer but is
never dropped by compaction, so the shard was re-marked due on every
NOPROGRESS evaluation.  Fixed by evaluating only the note that actually
sets `retention_floor`, adding the fence check (not the newest note at or
below any `control_prune_fences` fence) alongside the floor check, keying
the request on (note lsn, admission_seq, fence_epoch) so an unchanged
decision is never re-issued, and setting the shard's page-prune-due flag
only together with the flush request. MEDIUM-1, the protected-horizon
membership test had drifted from `walidx_plan_bases_build`'s own logic;
both now call one shared `walidx_protected_horizons_build()`.  MEDIUM-2,
an overflowed timeline's watch fired on every flush regardless of
relevance; overflow now arms no watch at all (the evidence sentinel still
limits it to one re-request per raw value).  MEDIUM-3, the control-note
flush decision's note I/O ran under the write-path locks without the map
lock; the decision (not the dedup-key store) now runs in the unlocked
interval after `retention_effective_floor_internal`, with `ps_lock_map_rd()`
held only around the note read.  Five LOWs: `page_flush_requested` and the
control-request key are reset on bare reopen (matching the other two reset
sites); the maintenance flush-servicing loop skips a poisoned manifest
instead of attempting a flush that cannot record its layer;
`wal_reclaim_watch_timelines_active` uses paired atomic add/sub instead of
non-atomic increments under a lock that does not cover every reader; the
watch-mechanism comment was rewritten for the evidence design; and
`ps_test_compaction_count()`, `ps_test_page_prune_due()`, and
`ps_test_set_wal_reclaim_watch_fire_hook()` were added as test-only hooks
(plus, added during mutation verification below,
`ps_test_control_flush_wanted()`/`ps_test_control_flush_stored()`).  Five
new reclaim-core tests were added (`test_base_durable_between_evaluations`,
`test_base_durable_at_write`, `test_watch_ignores_unrelated_shard_flush`,
`test_control_flush_not_repeated`, and the protected-horizon FPI variant of
the residual-1 test); each fails on 020482f8208 and passes after.  The
review's two mutations were re-verified against the shipped design rather
than applied verbatim (both predate the evidence-keyed/dedup-keyed
mechanisms): the "any-flush fires an armed watch" mutation is killed by
`test_watch_ignores_unrelated_shard_flush`'s bound checked immediately
after the unrelated-shard flushes, before any evaluation could re-arm and
hide it.  The "unkeyed control-flush store" mutation could not be forced
to manifest as a *repeated* store in a single-threaded idle-loop test: this
maintenance dispatch runs the flush-servicing step inline, within the same
`ps_core_maintenance()` call that stores the request, so the note lands
durably before a second evaluation with the same key is possible -- making
the review's "due re-marked forever" shape structurally impossible here
regardless of the key.  The `ps_test_control_flush_wanted()` /
`ps_test_control_flush_stored()` counters added for this verification
confirm the check-and-store code path executes and that the store count
matches the request count exactly once for the unchanged key in
`test_control_flush_not_repeated`; the key remains as keyed defense-in-
depth (it is exactly what closed a real bug found during this work: an
earlier draft updated the key in the unlocked interval before revalidation,
so a discarded/retried attempt permanently suppressed the real request
that never happened).  Full section 5 verification (all standalone suites,
soak seed 20260909 5/5 plain + 3/3 contended, seeds 7 and 4242, and the
2400-round default) was re-run after this revision; see the PR for the
suite and soak tables.  No persisted format change.

A second review pass of the same PR (2026-09-20) found the branch's other
findings closed but two further HIGH issues in the residual-1 mechanism
specifically, both in the single-window (h_cap) approximation the watch
and its evidence used.  NEW-HIGH-A: retain_chain applies per horizon, and
a blocking item generally sits below two horizons of different kinds, not
one -- a nearer protected fence P (page-history/walidx) and the farther
unprotected durable-progress horizon U -- retiring only once a base or FPI
exists in [item end, P] AND an FPI exists in [item end, U].  Collapsing
both to h_cap = min(progress, nearest fence) and using that single window
for both base and FPI evidence meant an FPI landing strictly between P and
U (the ordinary production shape once P's own base already exists)
satisfied neither window: it was not in [item end, P], and h_cap never
reached U at all once a nearer P existed, so the item stayed stuck and the
segment waited for the WAL-index controller's own unrelated trigger, the
exact pre-PR gap this residual exists to close.  Fixed by computing P and
U independently per item (P = the nearest protected horizon at or above
the item's low end; U = the nearest unprotected one, including progress
itself), evidence over [item end, P] for the base and [item end, U] (or P
when U does not exist) for the FPI, and arming/matching each kind against
its own window (`WalReclaimWatchEntry` now carries `hi_base`/`hi_fpi`
instead of one shared `hi`).  NEW-HIGH-B: with the arm unconditional, a
fire site that matches *any* qualifying version already reflected in the
evidence -- not only a new one -- refired on every later, unrelated flush
of the watched page's shard: the exact per-flush-drain cost this design
was meant to avoid, just relocated from "no watch" to "a watch that never
stops mattering."  Fixed by arming a kind only while its own window's
evidence has not already found what it needs (a base or FPI already
present there cannot be improved by a later, unrelated flush landing in
the same closed window); if an item is still stuck with both already
nonzero, something other than this watch holds it, and a fence-epoch
change or the 1 s idle fallback is what moves it next.  Two new
reclaim-core tests were added
(`test_fpi_between_protected_and_progress_retires`,
`test_watch_does_not_refire_on_satisfied_base_evidence`), each failing on
the pre-this-revision commit and passing after; the review's r2mut2
mutation (every flush unconditionally wakes the reclaimer, independent of
the watch) is killed by the second test's evaluation-count bound.  A nit
(a watched-item-count mismatch after a partial retirement forces one extra
fruitless re-request rather than a like-for-like comparison) is now
called out in a comment rather than fixed, since it is already bounded to
one extra request, not a repeat.  `ps_test_page_prune_due` and
`ps_test_control_flush_wanted`, exported but unused after the first
review pass, are now exercised by `test_control_flush_not_repeated`.  No
persisted format change; see the PR for the re-verification results.

The long-run configuration the gate asks for is the
`pagestore nightly soak` workflow (`.github/workflows/pagestore-nightly.yml`):
three seeds at 8000 rounds on a daily schedule and on demand with chosen
seeds/rounds, one job per seed, with every JSON report summarized in the job
and kept as a 30-day artifact.  Each job takes the time its rounds need
instead of a fixed limit, and a dispatch too large to finish inside the
hosted-runner limit is refused rather than killed before it reports.  GitHub fires scheduled and dispatchable
workflows only from the repository's default branch, and `master` is reserved
for the upstream mirror, so `pagestore` is the repository's default branch and
the lane runs from it directly; the schedule fires only in this repository or
in a fork that sets `PAGESTORE_NIGHTLY_ENABLED=1`, while a manual dispatch is
always honoured.  A 200-round dispatch proved the path against the #249
roll-up (1426 checks, 0 failures).
The branch is resolved to a commit once, before the seeds fan out, and every
job checks that commit out, so a run's seeds stay one experiment even when
the branch advances between jobs or a job is rerun later; the resolved
revision is reported in each job summary.  A
run that cannot write its JSON report fails rather than passing with nothing
to compare across nights.

### 5. Composed crash and format-compatibility coverage -- crash coverage composed; format fixtures complete

The POSIX image-layer publication, page-pruning, WAL-index compaction, WAL
reclaim, timeline deletion, manifest replacement, fork-metadata publication,
and compute-restart slices are now covered by the declarative harness.  The
deletion slice crashes on both sides of its first transition: before the
DELETING record is durable the request is lost and the branch must survive
intact, and after each later boundary the cleanup must resume.  Its workload
seeds a live sibling branch with the same kind of private state, so cleanup
that reached past its owner would be caught.

The first persisted-format fixture slice is in place under the D5 policy
(now decided: PostgreSQL-native payloads are wrapped and never rewritten and
carry their PostgreSQL version identity; page-store envelopes keep a fixture
for every format shipped after the MVP baseline, with explicit migration for
supported older versions and fail-closed otherwise; container formats are
registered per provider; backend artifacts follow the same envelope/payload
split).  Every
daemon-side record format reports its compiled magic and version through
`pagestore_format_versions`, the POSIX and SPDK container identities
included; the fixtures (`posix-mvp-baseline` and `posix-forkmeta-crc`, now
legacy, and `posix-wal-payload-identity`, current) each hold a captured
store carrying page history and its cutoff, fork-size events on both sides of
the cutoff plus a post-cutover source tail, a sealed shipped-WAL segment
with a control note inside it, a compacted WAL-index interval with a fixed
reader, a live branch with one record of every page-segment format the daemon
writes (an ordinary versioned record, a below-floor copy clamped to the branch
point, and a zero-version WAL-less record), and a deleted branch; the current
one's shipped WAL begins with a genuine long WAL page header, and its
`fixture.json` records the payload identity (WAL page magic and segment
size) the archive's version-2 envelopes carry, which the check verifies
against the payload bytes and, given the checking build's identity, against
that build.
`harness/pagestore_fixture.py
--check` fails when the compiled identities differ from the fixture (a
format change without a fixture update) and when the archive's own bytes do
not carry the identities its metadata records (metadata edited without a new
capture), reopens the fixture and runs its
oracle across a restart -- including the identities the archive's own
metadata carries: both seeded retention pins are looked up by owner and must
still name that owner, its resources and its horizon; the live branch must
still record the parent and fork point it was created at and serve its own
shipped WAL bytes, and its WAL-index entry must still name the branch as its
source timeline; and the relation the extension phase creates must still
exist exactly above its create event and be grown exactly above its growth
event, none of which is visible in the horizons, the latest sizes or the
pages a read returns -- and applies forty mutations (unknown newer
version, checksum corruption, truncation) across the WAL store identity,
sealed WAL segments, retention state and records, page and WAL-index
frontiers, forkmeta and WAL-index snapshot manifests and payloads, the
WAL-index epoch watermark, the persisted shard count, the timelines log, the
forkmeta source epoch, the layer manifest, and image layers.  Each is rejected at open except the documented torn-tail repairs of
the timelines and layer manifest logs; a daemon that dies of a signal or
exits under use is reported as a crash, never as a rejection.  The identity
table also covers the page-segment (versioned, WAL-less, and clamped),
flat-WAL, WAL-index source-log, POSIX WAL-index watermark, and persisted
shard-count formats the daemon writes, and the check requires the fixture to carry a record of every
advertised page-segment and WAL-index log format; the capture therefore runs
in two phases, seeding under the trigger that publishes a WAL-index snapshot
and then extending under the one the archive records, so the records appended
after the cutover stay in the live epoch; it does not
advertise delta layers, which no maintenance path produces yet.  Captures run
in a private directory and canonicalize the absolute layer locations the
manifest persists, so repeated captures produce byte-identical archives, and
the check starts every daemon with the configuration the archive records
rather than today's defaults.  Forkmeta source and snapshot payload records
are now sealed as FKM3, the FKM2 layout with a CRC-24 in the three former pad
bytes, so a flipped byte inside a record is rejected at open; FKM2 records
stay readable, and `fixtures/posix-mvp-baseline` (FKM2) is kept as the legacy
fixture that must keep reopening while `fixtures/posix-forkmeta-crc` is the
current one that pins the compiled identities and takes every mutation.  This
was the first format change to go through the fixture process.  Six findings
were fixed on the way: a store reopened at a new path was refused because
layer locations recorded their absolute parent directory (a missing parent now
rebases onto the store's own leaf; a foreign existing parent is still
rejected); a damaged forkmeta snapshot marker silently discarded acknowledged
post-cutover events (a source epoch that conflicts with the selected snapshot,
an emptied source included, now refuses to open); a segment record header
persisted its alignment padding uninitialized; a fork-metadata snapshot part
could be published in an order its own loader refuses, because ordered markers
that live only in the source log were appended after the in-memory events of
the same fork -- a below-floor copy followed by a higher-LSN write on the same
relation published a snapshot the daemon then could not open, so each part is
now sorted into per-fork order before it is written; and a torn legacy prefix
stayed repairable only after the unknown-magic check learned to read a legacy
record first; and, found during PR #262 review, a live ordered write's bound
marker existed only in the forkmeta source log -- the in-memory fork history
recorded a plain GROW instead -- so a second forkmeta cutover in the same
daemon lifetime published that plain GROW and the store could not reopen
(`storage open: Invalid argument`).  Live writes now insert the marker-plus-
activation representation recovery itself rebuilds, closing that path outright.
Recovery separately gained a fail-closed adoption rule for an unmatched
ordered record whose *selected forkmeta snapshot's freeze sequence* covers its
admission sequence.  That freeze condition is only a NECESSARY filter, not
proof against a torn append: a refused record's admission sequence is still
observed to prevent identity reuse on retry, so a later cutover can freeze
past a torn record's sequence too (review finding R2-F1, from independent
re-review after F2/F3).  A growth-class orphan (a plain GROW carrying the
record's exact identity) is promoted back to a bound marker -- sound
unconditionally, since a torn growth append never leaves a durable marker or
an in-memory event to begin with.  A commit-class orphan (a second
below-floor/WAL-less rewrite of an already-sized block -- the FSM/VM pattern
-- which never left a plain GROW behind even pre-fix) is proven safe by size
instead and gets an inert marker, but only after an additional,
path-specific torn-exclusion proof: residency on the image-layer path (a
layer-resident record's marker append cannot still be in flight, since
staging happens only after that append returns), or, on the segment-suffix
path, that at least one complete record follows it in the same segment (a
torn body is always the last complete record of its segment, so nothing
can ever follow it there); a commit-class record that is last in its segment
is retired instead, since it cannot be told apart from a torn append at scan
time -- for a pre-fix store that crashed after two same-lifetime cutovers (no
close-time flush) it can instead be that record's own last acknowledged
commit-class write, torn-indistinguishable, so retiring it falls back to
serving the previous version.  Either adoption logs one `adopting orphaned
ordered ...` line and any other case stays refused or retired.  **Resolved**:
a pruned marker's record used to be rescanned after a timeline-delete rewrite
rebased the flush watermark (F3; worse, Q1: the same rewrite silently lost
already-flushed survivors whose stale layer offset sat above a later,
rebased watermark, no crash required -- see `RELEASE_VALIDATION.md`,
"Resolved: pruned ordered marker rescanned after a timeline-delete rewrite").
The fix is invariant I3: `page_cleanup_tombstone_segment()` tombstones a
target timeline's records in place (each overwritten with a hole record of
identical size, magic changed to one of three new `SEG_HOLE48/56/64_MAGIC`
values) instead of rewriting the segment into a relocated replacement, so no
survivor moves, no image-index entry goes stale, and the watermark is never
rebased -- segment bytes are immutable once written.  Space is reclaimed by
segment GC once the whole segment is below the watermark, same as any other
covered segment.  A store written entirely by the fixed daemon now exercises
the adoption rule above **never**: no timeline deletion it performs can ever
create a rescan region.  The rule stays, unchanged, purely as recovery for a
store that deleted a timeline before this fix (see the T7 follow-up below).
`integration_test.sh`
now stops every cluster it started, reopens its own retained store against a
fresh daemon, and asserts the reopen succeeds, that no segment tail was
retired and no record was refused (both unconditionally fatal), and asserts the adoption count is zero as a hard invariant (not merely this run's observation), so any nonzero count fails the script instead of requiring the separate manual check `RELEASE_VALIDATION.md` used to call out.  Follow-up
(task T7, separate PR, not blocking): a store that already underwent a
timeline deletion before this fix may have lost survivors to Q1, and
plainly -- a store that deleted a timeline under the old daemon and was then
flushed (ordinary operation, not a rare condition) has already lost those
versions; nothing here recovers them.  Repairing them is not yet
implemented, and cannot in general rely on the manifest: the old rewrite's
`PS_MANIFEST_REBASE_FLUSH_WATERMARK` record never carried the pre-rebase
watermark, only a differently-tagged copy of the same post-rebase value, and
even that tag does not survive the next routine `ps_manifest_compact()`,
which rewrites every shard's current watermark out as a plain
`SET_FLUSH_WATERMARK` -- so a repair tool has no durable manifest evidence
to work from once a pre-fix-affected store has compacted even once; see
`RELEASE_VALIDATION.md`'s "What is not yet fixed" for the detail.  Resolved
(F5, see `RELEASE_VALIDATION.md`'s "Resolved: linear
event scans over inert commit markers (F5)"): the marker-matching/adoption
scans and the lsn-only bisection's equal-LSN run walk were linear in a
fork's event count, costing O(N) per replayed record on an FSM/VM fork of a
hot table (O(N^2) per fork at recovery and per cutover); a
`(lsn, admission_seq)` position index, gated by a per-fork legacy-event
counter with the old linear code kept as its fallback, now makes every one
of those O(log N)/O(1).  Measured at K = 50,000 commit-class rewrites: live
path 41.7 -> 11.9 us/write, cutover 0.536 s -> 0.143 s, reopen 1.052 s ->
0.451 s.  The in-memory array is now also bounded per daemon lifetime
(design B, same `RELEASE_VALIDATION.md` section): a successful cutover
compacts each fork's array down to exactly the events the durable
checkpoint/tail kept, dropping the inert markers it dropped, right after
the publish succeeds instead of leaving that to the next restart.
Recovery-equivalent by construction (the durable state no longer carries
them either).  Measured with `fev_bench K periodic` (K = 50,000 WAL-less
rewrites split into 10 reclaiming cutover rounds): the in-memory event
count on that fork stays in the low teens after every round with this fix,
versus growing to 50,001 without it.  See `RELEASE_VALIDATION.md` for the
full detail and the two follow-ups left open (design C's version-chain
lookup, `def_idx` for a few remaining newest-first linear scans).  One
gap remains documented in the check: a
page-segment record can never be the only copy of a page, because a cleanly
stopped daemon flushes its memtable into a layer before it exits, so every
archived page is also in a layer and a read resolves there (`reads mem=0
layer=9 seg=0` on a reopened fixture).  The segment readers are still
exercised -- every open replays the uncovered tail to rebuild the index, and a
record whose framing is wrong fails that scan -- but a mutation inside one
cannot be observed through a read while the layer carries the same version.
The backend's own artifacts followed: the store objects it writes (the
redo note, materializer markers, writer checkpoint, SLRU watermark and
tombstone, reader snapshot objects) are in `posix-backend-objects`, and
the files it leaves in a data directory (the prepared branch's manifest
and bootstrap, the prepared reader's manifest, snapshot and catalog
provenance, the reader-map intent marker and the SLRU mirror's primed
marker) are in `fixtures/pgdata-artifacts`, captured from a real backend
by `integration_test.sh` and loaded back through the backend's own loaders
in a scratch cluster by `harness/pagestore_pgdata_fixture.py --check
--build` (twenty-one mutations rejected or accepted as declared).  The
controller's and supervisor's JSON files -- each tool's configuration,
the controller's journal and retention generation authority, the
supervisor's status and the materializer's retention generation authority
-- have their layouts in `pagestore_artifact_schema.py`, which both tools
write and read through, and are in `fixtures/controller-json`, captured
from real runs and checked by `harness/pagestore_controller_fixture.py`
(forty mutations).  Gate 5's format fixtures are complete.

An advancing reader's data directory boots from the checkpoint its manifest
names, and the reader moves its own retention pin above that horizon as it
adopts newer published views.  Nothing then keeps the boot control image
alive, so a restart of that data directory cannot restore it once pruning has
run.  Until an adopted horizon is written back into the reader manifest, the
controller owns that image's lifetime and must hold a page-history horizon at
it; the integration test models exactly that.

## Recommended sequence

Keep the composed WAL-only -> materializer -> branch scenario green as the MVP
acceptance contract.  Gates 1-4 are implemented for the local POSIX
deployment, with the artifact lifecycle described in gate 4;
gate 5 has its crash coverage composed, its concurrency clause closed (the
crash matrix's concurrent appender at every publication boundary, and the
composed forkmeta workload's acknowledged-append ledger), and its format
fixtures complete.  The nightly lane's seed-20260909 `wal`-bound flake is
explained and fixed (#265); the FSM reopen blocker its fix's PRs hit is
fixed (#264); and the artifact-key collision that made #264/#265/#266 all
fail the integration lane identically is fixed (#266) -- see
`RELEASE_VALIDATION.md`'s "Integration-lane finding" for the root cause and
"Open: pruned ordered marker rescanned after a timeline-delete rewrite (F3)"
for the one item that review left open: it is explicitly release-blocking
there, but not an MVP gate, since #264's F2 mitigation already keeps it a
logged pruning reversal rather than silent loss and gate 5's format fixtures
stay complete.  What remains before the MVP is declared complete is:

1. Accumulate a green scheduled-run history on the three-seed nightly lane
   against `316401d8d4b` and later (the fixes above).  A manually dispatched
   run against that revision,
   [35458043758](https://github.com/clapdb/postgres/actions/runs/35458043758)
   (seed 20260909, 8000 rounds, 2026-09-19), confirms the fix -- 45024
   checks, 0 failures, `physical_max.wal` 1736704 bytes against the 4667392
   bound, `wal_fence_slack_max` 1568768 bytes -- but it is one dispatched
   run, not yet the scheduled-run history this gate asks for.

Performance refinements such as size-tiered compaction, layer key-range pruning,
bloom filters, per-shard layer maps, asynchronous POSIX I/O, and explicit
CPU/IO scheduling remain important, but they follow the functional and
operational gates above unless measurement shows they block the MVP scenario.
