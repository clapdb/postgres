# pagestore — ingest & materialization abstraction

How changes *enter* the store (ingest) and how a logical "page as of an LSN" is
*produced* (materialize) are abstracted behind narrow interfaces, so that the
**same logical pipeline runs unchanged on POSIX, SPDK and cloud** — only the
backend bindings (and *where* materialization runs) differ.

This doc sits above [`LSM_ARCHITECTURE.md`](LSM_ARCHITECTURE.md) (the layer
model, read planner, compaction/GC), [`WAL_REDO.md`](WAL_REDO.md) (the redo
mechanics) and [`LSM_OBJECT_STORAGE_PLAN.md`](LSM_OBJECT_STORAGE_PLAN.md)
(tiering). It defines the *deployment-agnostic* contract those pieces plug into.

Current MVP progress and the remaining operational gates are tracked in
[`MVP_STATUS.md`](MVP_STATUS.md).

## First principle: WAL is truth, page is a materialized view

A page is never the unit that *must* be durable — the change is. The durable
record can be either the **page** (page-ingest) or the **WAL** (wal-ingest); a
page at a given LSN is then a *view* derived from the nearest base image plus the
WAL deltas after it. Two corollaries drive the whole design:

1. **Redo (WAL→page) is a materialization step, never on the hot read path.**
   Reads return an already-materialized versioned page (from a cache or an image
   layer). Materialization happens at *ingest time*, in the *background*, or for
   *cold* pages — but a hot read pays no redo. (This is the Aurora placement, not
   Neon's read-time redo; it keeps local-NVMe reads fast.)
2. **Pages are cached at multiple tiers; only one tier is authoritative.** The
   compute-local cache (RAM buffer pool + local-NVMe page cache, à la Neon LFC /
   Aurora Optimized Reads) accelerates reads and is *non-authoritative* (rebuilt
   on miss). The store's sealed image layers are the durable authority.

## The two abstract operations

### `ship` — get a change into the store durably

Ingest is a *policy*, expressed over the existing IPC opcodes:

| policy | what crosses to the store | lands in | seals to |
|--------|---------------------------|----------|----------|
| **page-ingest** | full page (`WRITEV`/`EXTEND`) | memtable | image layer |
| **wal-ingest**  | WAL records (`WAL_APPEND`)   | delta staging | delta layer |

Both go through the *same* shape: `ship → mutable staging → sealed immutable
layer → manifest`. page-ingest seals **image** layers (already materialized);
wal-ingest seals **delta** layers (need materialization to become pages).

The choice is per-deployment policy, not a code fork (see the binding table).

### `materialize` — turn (base image + delta chain) into an image-layer page

```
materialize(timeline, key, block, target_lsn)
   = read plan (LSM_ARCHITECTURE read path):
       base = newest image layer version <= target_lsn
       deltas = delta layers covering (base_lsn, target_lsn]   (ascending)
   -> apply deltas onto base via PG rm_redo
   -> new image-layer page version  -> manifest ADD_LAYER
```

This is the **only** place redo runs. It is pluggable in *where* it runs — the
`PsMaterializer` binding:

| binding | runs where | redo engine | writes result to | use |
|---------|-----------|-------------|------------------|-----|
| **inline-none** | n/a | none | — | page-ingest: pages already materialized |
| **local-worker** | same host (sidecar / recovery-worker-style PG in recovery) | `rm_redo` in an isolated context | local image layer + manifest | POSIX / SPDK wal-ingest |
| **serverless** | cloud (Lambda/fn) | `rm_redo` reading layer objects from S3 | new image-layer **object** in S3 + manifest | cloud wal-ingest |

Crucially the *input* (a read plan: base image object + ordered delta payloads)
and the *output* (an image-layer page version, recorded in the manifest) are
identical across bindings. On cloud, S3 only stores immutable layer **bytes**;
the Lambda is the compute that reads base+deltas and produces the page — "S3
materializes" is shorthand for "a serverless function materializes from S3".

## One pipeline, three deployments

```text
              ┌─────────── ship (page | wal) ───────────┐
 PG backend ─►│  staging (memtable | delta staging)     │
              │      │ seal                              │
              │      ▼                                   │
              │  immutable layer (image | delta) ──► manifest
              └──────────────────┬──────────────────────┘
                                 │ (wal-ingest only)
                    materialize (inline | local-worker | serverless)
                                 │  rm_redo: base + deltas -> image layer
                                 ▼
                          image layer ──► manifest
   read: getPage@LSN ─► page cache (compute-local ─► storage) ─► versioned image layer
                         (no redo on this path)
```

The boxes are deployment-invariant. Only the bindings below change.

### Backend binding table

| concern | abstraction | POSIX | SPDK | Cloud |
|---------|-------------|-------|------|-------|
| byte log (seg/wal/meta) | `PsStorage` | posix files | spdk nvme | posix on instance disk |
| layer files (local/hot) | `PsLayerStore` | posix files | spdk/posix | local cache of objects |
| layer object tier (cold) | `PsLayerStore` object ops | none | none | **S3** |
| ingest policy | `ship` | page (now) → wal | page → wal | **wal** |
| materializer | `PsMaterializer` | inline / local-worker | local-worker | **serverless (Lambda)** |
| compute page cache | materialized-page cache | RAM + local NVMe | RAM + local NVMe | RAM + local NVMe (LFC) |
| read | read planner + cache | uniform — versioned image layers | uniform | uniform |

Reading down a column gives a complete, consistent deployment. Reading across a
row shows the *only* thing that varies is the binding — the pipeline is the same.

## Invariants (must hold in every binding)

- **No redo on the hot read path.** Reads resolve to a cached or sealed image
  page; redo is confined to `materialize`.
- **Materialize is off the foreground ack path.** `ship` acks once the change is
  durable (page in staging+seg, or WAL durably appended). Materialization is
  asynchronous/background/on-demand-for-cold, never blocking commit.
- **Install-new-before-delete-old / idempotent GC** carry over unchanged from
  `LSM_ARCHITECTURE.md` regardless of where materialize ran.
- **The materializer's output is a normal image layer.** Whether produced by a
  local worker or a Lambda, it is an ordinary sealed image layer recorded in the
  manifest — the read path cannot tell the difference.
- **Compute-local pages are a cache, the store is authority.** Losing the local
  cache is always recoverable from the store.

## Mapping to current code & status

- `PsStorage` (posix / spdk) — **done** (byte-log backend).
- `PsLayerStore` local (posix) — **done**.  The filesystem-backed object provider
  and durable upload/verification are done for local testing.  Download,
  eviction, and remote deletion mechanisms exist, while cache residency policy
  and remote-orphan reconciliation keep phases 5–6 partial.  A real S3 provider
  is planned.
- `ship` page-ingest → memtable → image layer (+manifest, compaction, GC,
  restart-from-layers) — **functional path done**.  LSM phases 2–3 remain
  partial because image indexes are dense and layer-block cache invalidation is
  not implemented.
- `ship` wal-ingest: WAL shipping (`archive_library`) + per-page WAL index +
  delta-layer format + read plan — **partial** (7a/7b and continuous auto-index
  done; the continuous recovery-worker topology is proven, while its managed
  production lifecycle remains).
- `materialize`:
  - inline-none (page-ingest) — **done** (the page is the image-layer version).
  - local-worker — **continuous topology proven**: `wal_only_redo_demo.sh` runs
    a recovery worker that replays shipped WAL into image layers via smgr, and
    `continuous_redo_demo.sh` keeps a WAL-only writer and a distinct recovery
    worker online together while later archived segments are materialized.
    Restartpoints publish a durable `pagestore_materialized_wal_lsn()` only
    after their buffer flush completes.  The publication order is relation-page
    store sync, marker write, marker store sync, and retention advance.  The
    named H1 fault points pause after relation-page sync/before marker write or
    after marker sync/before retention advance; they are pause-only test
    controls and add no durability edge when disabled.  Because the hook runs
    in PostgreSQL's checkpointer child, the harness records the exact child
    report, stops the complete materializer postmaster, safely removes the
    release/control markers, and lets the supervisor recover without assuming
    that its worker generation changed at the probe.  Supervisors compare that watermark
    with `pagestore_shipped_wal_lsn()`, or read
    `pagestore_materializer_lag_bytes()` directly; the lag API fails closed
    outside recovery so a writer cannot masquerade as a
    caught-up materializer.  Writer-side control planes use the read-only
    `pagestore_materializer_status()` row instead: it reports the store-observed
    marker, lag, and latched release checkpoint without asserting that the
    caller is the worker, and returns NULL progress fields until a valid
    timeline-local marker exists.  A worker must explicitly set
    `pagestore.materializer = on`; startup then requires the localsvc backend,
    full relation routing, and an unpinned read horizon.  Installing the
    `pagestore` extension provisions these declarations persistently in each
    monitored database.  After the first durable marker is established,
    writers may set `pagestore.materializer_max_lag_mb` to pause WAL archiving
    when that marker falls too far behind.  The
    limiter stops only at complete segment boundaries and always includes the
    first writer-owned checkpoint completion observed for the current marker
    (plus room for a crossing record), so recovery cannot be stranded behind a
    partial segment or before its next usable restartpoint.  Writer checkpoint
    state is separate from recovery's pg_control updates.  The release
    checkpoint is durably latched: later checkpoints cannot move the bound
    until materialization advances.  Any already-started segment is allowed to
    finish, including an interrupted bootstrap or a prefix left before the
    limit was enabled.  All progress records carry the logical timeline, so a
    branch cannot inherit its parent's marker or latch.  PostgreSQL's archive
    retry loop resumes shipping when the marker selects the next release
    checkpoint.  `pagestore_materializer_supervisor` now continuously drives
    checkpoint-to-marker progress for one provisioned worker, records owner and
    worker generations, fences duplicate owners, applies bounded retry, and
    replaces a crashed worker.  `materializer_smoke` provisions the pair and
    acts as its acceptance client.
  - serverless (Lambda) — **planned**; same read-plan input, writes an image
    layer object to S3.
- `PsMaterializer` is not yet a named vtable in code — today materialization is
  the recovery worker. Promoting it to an explicit interface (inline /
  local-worker / serverless) is the abstraction this doc commits to.

## Local materializer supervisor

Meson installs the POSIX foreground service as
`pagestore_materializer_supervisor`.  It accepts `--config PATH`; the strict
schema-4 JSON object requires absolute `pg_ctl`, `psql`, `data_dir`,
`socket_dir`, `log_file`, `state_dir`, and controller-owned
`retention_authority_dir` paths, a PostgreSQL `port`, and a
controller-assigned nonzero `retention_owner_id` that remains stable across
replacement workers and is globally unique across every timeline managed by
the authority directory (owner IDs are not merely timeline-local).  It also requires a globally unique
`controller_instance_id`; generation authority records bind the consumer to
that identity, and takeover from another identity fails closed until the old
controller has explicitly quiesced and handed off its worker.
Optional fields select `database`, `user`, polling/replay-idle/progress timeout
intervals, command timeout, exponential retry bounds, and the maximum
consecutive failure count.  Before starting a replacement worker the supervisor
durably increments `retention_generation` and passes both retention values as
postmaster settings.  `--check-config` validates and prepares the runtime
directories without taking ownership.

The per-PGDATA nonblocking owner lock is stored at a fixed name in `data_dir`, so changing
`state_dir` cannot create a second owner for the same worker.  Lock contention
exits with status 75; invalid configuration exits with 78.  The lock contents
are diagnostic only—the held `flock` is authoritative.

Generation allocation and its lock are stored by owner ID in
`retention_authority_dir`.  The controller must supply one shared durable
namespace for every incarnation of an owner; it must not be a per-PGDATA or
per-supervisor status directory.  Consequently replacement PGDATA instances
with different `state_dir` values serialize on the same generation authority.

`state_dir/status.json` is replaced atomically after fsync and reports schema,
state, owner PID/epoch, worker generation, retention owner ID/generation,
consecutive failures, last error, timestamps, and the last
replay/shipped/materialized LSNs and lag.  Normal
service-manager termination releases ownership but deliberately leaves a
healthy PostgreSQL worker running; the next owner increments its epoch and
adopts that worker.  Startup, health, missing progress API, restartpoint, and
replacement failures share bounded exponential retry.  Exhaustion publishes
`failed` and exits nonzero for the outer service manager to handle.

The recovery startup process registers its WAL and WAL-index floor before the
first redo record is applied.  A stale generation or registration failure is a
fatal startup error.  After a restartpoint marker becomes durable, the worker
advances the same pin; a transient failure leaves the older conservative floor,
while a stale response stops the fenced worker.  Supervisor handoff does not
drop the pin because the healthy worker remains live, and crashes retain it
until a higher controller generation takes over.

## Local serialized branch prepare

Meson also installs the one-shot `pagestore_branch_prepare --config PATH`
controller.  Its strict schema-2 JSON names absolute `pg_ctl`, `psql`, writer
and materializer PGDATA, writer log, private socket, and prepared-artifact
paths; public writer/materializer hosts and ports; a private writer port;
logical parent/child timelines; and optional database, user, polling, progress,
and command timeouts.  `--check-config` validates the topology and prepares the
owner-only socket/artifact directories.

The controller serializes the complete branch boundary:

1. Take a writer-PGDATA operation lock and the materializer supervisor's own
   PGDATA lock.  A live supervisor or another branch operation fails with exit
   status 75; an externally owned replay pause also fails closed.
2. Pause the materializer, capture a restartpoint-proven SLRU base `C`, and
   resume it.  Then fast-stop the public writer so existing clients drain.
3. Restart that writer with TCP disabled on a mode-0700 private Unix socket;
   disable autovacuum, WAL senders, logical workers, and optional pagestore
   artifact/index workers.  `pagestore_branch_checkpoint()` verifies the clean
   shutdown checkpoint's exact mirrored control image and admission fence and
   returns its real WAL-record boundary `R/E`.
4. Switch/archive WAL, wait for durable shipping and materializer replay through
   `E`, pause again, and capture restartpoint/materialized fork `L >= E`.
   Prepare the store branch, catalog maps, and SLRUs while both sides remain
   fenced.
5. Atomically publish `pagestore_branch.prepare.json`, resume materialization,
   stop the private writer, restore its normal configuration, and update the
   receipt from `prepared` to `complete`.

Handled failures and termination signals make a best-effort service restore;
the intermediate receipt makes a branch already prepared before a cleanup
failure visible.  A hard process kill may still require manually resuming the
materializer or starting the writer.  The deployment must suspend an external
writer service manager for the command's duration; the materializer side is
mechanically fenced by the shared supervisor lock.  The portable artifact
continues to reject user tablespaces rather than guessing their topology.

## Why this shape (vs Neon read-time redo)

On **local NVMe**, reading a stored page is ~tens of µs; doing redo on the read
path (Neon's lazy model, tuned for S3 cold storage + scale-to-zero) is slower and
pointless. By keeping redo in `materialize` (background/cold) and serving reads
from versioned image layers + a compute-local page cache, local deployments stay
fast **and** the cloud deployment still gets WAL's wins (cheap durable commit,
cheap replication, S3-economical immutable layers) — without changing the
pipeline. The deployment that benefits from WAL-ingest most is "stateless
compute + remote commit durability" and/or "object-storage backing"; everything
else may stay on page-ingest. The abstraction lets both coexist.
