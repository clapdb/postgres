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
| Image-layer path | Functional mechanisms implemented; phases 2–3 partial | manifest/compaction/segment-GC restart tests; sparse indexes and layer-block cache invalidation remain |
| Filesystem object tier | Upload done; cache/GC operations partial | download, eviction, refresh, and remote-delete tests; cache policy and orphan reconciliation remain |
| Materialized-page cache | Basic version cache implemented; phase partial | bounded cache/invalidation tests; cost-aware admission and integrated redo avoidance remain |
| WAL shipping and ancestry-aware WAL reads | Immutable 1 MiB segments integrated for sealed prefixes; the flat-log copy of every complete sealed record is reclaimed, while the flat log remains migration/tail authority | chunk assembly, reopen, ancestry, and WAL segment/store tests |
| Per-page WAL index and PostgreSQL `rm_redo` reuse | Live index plus crash-safe replacement-chain compaction, durable timeline frontier, multi-shard snapshot/log-epoch cutover, and old generation/epoch GC implemented | WAL redo demos plus discrete/operational chain pruning, frontier admission/restart/corruption/crash tests, snapshot publication, generation/epoch GC, and tail replay |
| Continuous recovery materializer | Local POSIX supervisor implemented | ownership fencing, bounded restart/restartpoint policy, atomic status, and `materializer_smoke` crash replacement |
| Composed MVP data path | Implemented | `mvp_golden_test.sh`: WAL-only writer -> materializer -> durable fork -> independent branch, including restarts |
| `pg_control` and branch SLRU/catalog bootstrap | Serialized portable local path implemented | one-shot lifecycle controller plus fresh-initdb golden boot from CRC-bound maps/SLRUs/control |
| Fixed/advancing readers and handoff | Implemented | integration coverage for reader artifacts and view adoption |
| Retention horizon authority | Reader/materializer owners plus page-history and WAL-index frontiers implemented; WAL and forkmeta reclaim consumers remain | restart/corruption tests, exact-fence admission, branch projection, page/WAL-index publication crash tests, and bounded page churn |
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
- `mvp_golden_test.sh` composes WAL-only ingest, durable materialization,
  a proven recovery-produced SLRU base, portable branch boot from a fresh
  `initdb` skeleton (no parent PGDATA copy), parent/child isolation, and
  store/materializer/compute restarts in one topology;
- `branch_boot_test.sh` proves an independent branch compute can boot, preserve
  fork-point visibility, and write on its own timeline.

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
lock, while pre-metadata timelines retain local WAL readability.  Descendant
control history and a target child's reclaim candidate are capped at their
branch points, and durable index progress beyond the sealed prefix remains a
valid proof.  Focused core coverage includes idle preselection, ancestry and
timeline isolation, child-local controls, target branch caps, naturally
nonzero starts, unaligned progress tails, pre-metadata reads, restart
admission, a read/frontier publication race, pending-proof cleanup failure,
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
Until the remaining consumers land, correctness demos run and page history is
bounded, but total disk use can still grow without bound.

### 5. Composed crash and format-compatibility coverage -- remaining

Focused crash-safety tests exist, while the declarative harness remains partial.
Before declaring the MVP repeatable, add process-level fault scenarios around
materializer progress, branch prepare/install, manifest/layer publication, and
retention GC, plus a persisted-format fixture for restart/upgrade compatibility.

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
