# pagestore MVP status

This page is the progress source of truth for the pagestore MVP.  The other
documents in this directory describe subsystem designs and longer-term target
architecture; their future-looking sections do not by themselves define MVP
scope or completion.

Status below includes work through the composed MVP golden scenario.

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
| Continuous recovery materializer | Topology proven | `continuous_redo_demo.sh`, durable progress and bounded archive-lag tests |
| Composed MVP data path | Implemented | `mvp_golden_test.sh`: WAL-only writer -> materializer -> durable fork -> independent branch, including restarts |
| `pg_control` and branch SLRU bootstrap | Implemented | prepared branch install and fail-closed startup validation |
| Fixed/advancing readers and handoff | Implemented | integration coverage for reader artifacts and view adoption |
| Logical sharding | Implemented with shared-map locking | multi-shard standalone stress |
| Background maintenance | Implemented for POSIX | dedicated maintenance controller; no foreground inline compaction |

The existing CI proves both focused subsystem paths and the composed contract:

- `integration_test.sh` exercises the PostgreSQL-facing storage, control, SLRU,
  reader, and branch primitives;
- `wal_only_redo_demo.sh` proves non-redundant WAL ingest;
- `continuous_redo_demo.sh` proves a live writer and materializer following new
  archived WAL, publishing durable progress, and applying lag backpressure;
- `mvp_golden_test.sh` composes WAL-only ingest, durable materialization,
  prepared branch boot, parent/child isolation, and store/materializer/compute
  restarts in one topology;
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

### 2. Managed materializer lifecycle -- remaining

The data-plane contract exists (`pagestore_materializer_status()`, durable
materialized markers, timeline-bound lag backpressure), but production
provisioning and restartpoint/restart policy are still external manual work.
The MVP control path must create the recovery worker, supervise it, replace it
after failure, and drive progress without test-only restart nudges.

### 3. Safe automatic branch bootstrap -- remaining

The prepared branch protocol is fail-closed, but the current demo still supplies
the SLRU base cutoff and transaction/multixact horizons explicitly and copies a
stopped parent data directory.  The MVP path must obtain a proven snapshot
cutoff, derive bootstrap horizons from the matching control state, and expose an
idempotent control-plane operation rather than a sequence of expert-only SQL
calls and filesystem steps.

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

1. Put materializer provisioning/supervision and restart policy behind the
   harness/control-plane contract.
2. Automate proven-cutoff branch preparation and bootstrap inputs.
3. Add the retained-horizon registry, then reclaim image history, WAL, WAL index,
   and deleted timelines.
4. Promote the golden scenario into the declarative crash/compatibility harness.

Performance refinements such as size-tiered compaction, layer key-range pruning,
bloom filters, per-shard layer maps, asynchronous POSIX I/O, and explicit
CPU/IO scheduling remain important, but they follow the functional and
operational gates above unless measurement shows they block the MVP scenario.
