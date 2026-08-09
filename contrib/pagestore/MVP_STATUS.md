# pagestore MVP status

This page is the progress source of truth for the pagestore MVP.  The other
documents in this directory describe subsystem designs and longer-term target
architecture; their future-looking sections do not by themselves define MVP
scope or completion.

Status below includes work through recovery-materializer SLRU snapshot capture.

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

## What is implemented

| Area | Status | Current proof |
|---|---|---|
| Page ingest and copy-on-write reads | Implemented | standalone and PostgreSQL integration suites |
| Image-layer path | Functional mechanisms implemented; phases 2–3 partial | manifest/compaction/segment-GC restart tests; sparse indexes and layer-block cache invalidation remain |
| Filesystem object tier | Upload done; cache/GC operations partial | download, eviction, refresh, and remote-delete tests; cache policy and orphan reconciliation remain |
| Materialized-page cache | Basic version cache implemented; phase partial | bounded cache/invalidation tests; cost-aware admission and integrated redo avoidance remain |
| WAL shipping and ancestry-aware WAL reads | Implemented | integration and branch tests |
| Per-page WAL index and PostgreSQL `rm_redo` reuse | Implemented | WAL redo and WAL-only demos |
| Continuous recovery materializer | Local POSIX supervisor implemented | ownership fencing, bounded restart/restartpoint policy, atomic status, and `materializer_smoke` crash replacement |
| Composed MVP data path | Implemented | `mvp_golden_test.sh`: WAL-only writer -> materializer -> durable fork -> independent branch, including restarts |
| `pg_control` and branch SLRU bootstrap | Core mechanisms implemented; automatic bootstrap partial | proven recovery snapshot capture, control-derived prepare, prepared install, and fail-closed startup validation |
| Fixed/advancing readers and handoff | Implemented | integration coverage for reader artifacts and view adoption |
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
  a proven recovery-produced SLRU base, prepared branch boot, parent/child
  isolation, and store/materializer/compute restarts in one topology;
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
also restarts the POSIX store daemon, writer, materializer, and branch compute.
The scenario is wired into CI and is the stable end-to-end MVP contract.

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

### 3. Safe automatic branch bootstrap -- partial

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
remains available for compatibility.  The control plane must still keep the
parent free of horizon-changing work between the selected checkpoint and that
materialized boundary.

The current demo still copies a stopped parent data directory.  Completing this
gate requires a portable catalog/control bootstrap artifact plus one serialized
control-plane operation spanning checkpoint selection through the materialized
fork boundary.  The live SLRU watermark cannot substitute for the new capture
API: its newest-image contract deliberately permits bytes newer than its
completeness floor and is therefore unsafe as an exact branch seed.

### 4. Retention-driven space reclamation -- remaining

Segment GC removes page-log segments covered by image layers, but long-running
logical history is not yet bounded.  The MVP needs one retained-horizon model
covering active readers, branch fork points, restorable control images, and the
materializer.  That horizon must drive:

- version pruning during image-layer compaction;
- shipped-WAL reclamation without crossing the durable control/WAL floor;
- WAL-index log compaction/reclamation;
- timeline deletion and its layer/WAL cleanup.

Until this gate lands, correctness demos run but disk use can grow without
bound.

### 5. Composed crash and format-compatibility coverage -- remaining

Focused crash-safety tests exist, while the declarative harness remains partial.
Before declaring the MVP repeatable, add process-level fault scenarios around
materializer progress, branch prepare/install, manifest/layer publication, and
retention GC, plus a persisted-format fixture for restart/upgrade compatibility.

## Recommended sequence

Keep the composed WAL-only -> materializer -> branch scenario green as the MVP
acceptance contract.  The implementation sequence for the remaining gates is:

1. Add the portable catalog/control bootstrap artifact and serialize checkpoint
   selection through the materialized fork, building on proven SLRU capture and
   the control-derived prepare operation.
2. Add the retained-horizon registry, then reclaim image history, WAL, WAL index,
   and deleted timelines.
3. Promote the golden scenario into the declarative crash/compatibility harness.

Performance refinements such as size-tiered compaction, layer key-range pruning,
bloom filters, per-shard layer maps, asynchronous POSIX I/O, and explicit
CPU/IO scheduling remain important, but they follow the functional and
operational gates above unless measurement shows they block the MVP scenario.
