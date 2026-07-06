# pagestore — ingest & materialization abstraction

How changes *enter* the store (ingest) and how a logical "page as of an LSN" is
*produced* (materialize) are abstracted behind narrow interfaces, so that the
**same logical pipeline runs unchanged on POSIX, SPDK and cloud** — only the
backend bindings (and *where* materialization runs) differ.

This doc sits above [`LSM_ARCHITECTURE.md`](LSM_ARCHITECTURE.md) (the layer
model, read planner, compaction/GC), [`WAL_REDO.md`](WAL_REDO.md) (the redo
mechanics) and [`LSM_OBJECT_STORAGE_PLAN.md`](LSM_OBJECT_STORAGE_PLAN.md)
(tiering). It defines the *deployment-agnostic* contract those pieces plug into.

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
- `PsLayerStore` local (posix) — **done**; object/S3 ops — **planned** (tiering,
  `LSM_OBJECT_STORAGE_PLAN.md` phases 4–6).
- `ship` page-ingest → memtable → image layer (+manifest, compaction, GC,
  restart-from-layers) — **done** (LSM phases 2–3).
- `ship` wal-ingest: WAL shipping (`archive_library`) + per-page WAL index +
  delta-layer format + read plan — **partial** (7a/7b done; continuous WAL ship
  + auto-index still a worker).
- `materialize`:
  - inline-none (page-ingest) — **done** (the page is the image-layer version).
  - local-worker — **prototype exists**: `wal_only_redo_demo.sh` runs a
    recovery-worker that replays shipped WAL into image layers via smgr; making
    it a *continuous* applicator is the next step.
  - serverless (Lambda) — **planned**; same read-plan input, writes an image
    layer object to S3.
- `PsMaterializer` is not yet a named vtable in code — today materialization is
  the recovery worker. Promoting it to an explicit interface (inline /
  local-worker / serverless) is the abstraction this doc commits to.

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
