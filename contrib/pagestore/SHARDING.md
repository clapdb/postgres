# pagestore — multi-core sharding design

## Guiding principle: follow ScyllaDB

ScyllaDB is our primary reference for the concurrency/engine model.  Concrete
principles we commit to (adopt incrementally; adapt, don't clone Seastar):

- **Share-nothing shard-per-core.** `hash(key) -> shard`; each core owns its
  data and takes no global lock on the hot path; cross-shard only by message
  passing.
- **Run-to-completion, never block a shard thread.** Per-core async/polled IO
  (SPDK qpair per thread); the synchronous POSIX backend is the explicit slow-path
  exception.
- **Autonomous controllers + backpressure.** Compaction/flush are not merely
  "background" — they are scheduled under CPU/IO shares with backpressure (throttle
  writes only at a high-water mark) so foreground read latency stays bounded.
- **Per-core I/O scheduler with priority classes:** query > flush > compaction;
  background work must not starve foreground reads.
- **Engine-managed memory:** cache + memtable share a per-shard memory budget the
  engine evicts under pressure (à la Scylla's unified cache), not fixed separate
  allocations.
- **Semantic cache** (materialized pages, like Scylla's row cache), client-side
  shard routing (like shard-aware drivers).

Adaptation limits: we are a freestanding daemon talking to PostgreSQL over shared
memory, not a networked DB on Seastar.  Adopt the principles; build a custom
allocator / full scheduler only where it pays.  Redo/materialization is our own
cross-process concern (see `MATERIALIZATION.md`), not a Scylla mechanism.

## Overview

The daemon is single-threaded: one thread polls every channel and owns all state
lock-free.  On fast NVMe/SPDK that single core is the throughput ceiling.  This
doc commits to a shard-by-key model that keeps each shard single-owner and
lock-free (no global locks on the hot path) and lays out an incremental migration
that never breaks the standalone suite.

Expands the sketch in `LSM_ARCHITECTURE.md` (§Sharding model).

## Shard key: `hash(key) mod nshards` — block- and timeline-independent

The shard is chosen from the **logical key only** (spcOid, dbOid, relNumber,
forkNum) — *not* the block number, *not* the timeline.  Two properties make this
the right cut:

1. **A multi-block `READV` stays on one shard.** All blocks of a relation+fork
   hash to the same shard, so a vectored read is served by a single core without
   splitting across shards.
2. **COW read-through stays on one shard.** `read_through` walks a key's parent
   timelines as of the branch LSN; since the shard ignores timeline, every
   timeline's versions of a given key live on the same shard — the ancestry walk
   is local, no cross-core coordination.

Trade-off: a single hot relation maps to one shard (no intra-relation
parallelism).  Acceptable — real workloads spread across many relations/forks,
and keeping per-key state on one core is what makes it lock-free.

## What each shard owns (single-owner, lock-free)

```text
Shard {
  page_idx        // version index for this shard's keys
  fork_idx        // relation sizes for this shard's keys
  memtable        // staging -> image layers (this shard's keys only)
  pgcache         // materialized-page cache (this shard's keys)
  layer_map       // the layers THIS shard produced (cover only its keys)
  next_layer_id   // namespaced per shard (no cross-shard id collision)
  append cursor   // its own segment namespace (seg files tagged by shard)
  SPDK qpair      // per-thread NVMe queue (SPDK is per-thread)
}
```

Because the shard key is block-independent, a sealed image/delta layer produced
by a shard covers **only that shard's keys** — no layer ever spans shards, so the
layer map partitions cleanly and compaction/GC stay per-shard and lock-free.

## Shared state (kept off the hot path)

- **Timelines (branch tree)** — global, read-mostly.  Every shard needs it for
  `read_through` ancestry.  Replicate a read-only copy per shard; on branch
  create (rare) coordinate an update (brief barrier or per-shard apply of a
  broadcast event).  No per-read locking.
- **Manifest** — **per-shard manifest logs** (each shard owns its layers, so its
  ADD/SEAL/DELETE events are independent).  Avoids a shared-manifest lock
  entirely.  Restart replays each shard's manifest into that shard's layer map.
- **Layer store / object store** — shared filesystem (and later S3), but layer
  files are **namespaced by shard** (e.g. `layer_<shard>_<id>`), so concurrent
  shards never collide and a shard's GC only touches its own files.
- **Shipped WAL (`wal_<tl>`)** — a single global ingest stream today
  (archive_library).  The per-page WAL index (walidx) shards by key like
  everything else; raw WAL transport stays global (secondary while page-ingest is
  the default).

## Request routing: client-side over partitioned channel pools

No central router thread (a hop would re-serialize).  Instead:

- The shm channels are partitioned into `nshards` pools; shard *s* polls only
  pool *s*.
- The localsvc **client** computes `s = hash(key) mod nshards` and posts the
  request on a channel from pool *s*.  A backend claims one channel per shard it
  talks to (lazily).
- A `READV` is single-key, so it always lands on one pool — no split.

`nshards` is published in the shm header so the client and daemon agree.

## Threading

`nshards` worker threads, each running the existing serve loop over its channel
pool and owning its `Shard` struct.  POSIX: synchronous serve per shard.  SPDK:
each shard thread opens its own NVMe qpair and runs its own submit/poll loop
(SPDK is designed for per-thread qpairs), so async batching scales per core.

## Crash consistency

Per-shard manifests + per-shard layer namespaces mean the existing rules
(install-new-before-delete, idempotent GC, restart-from-manifest) apply unchanged
*within* a shard.  `layer_id` is namespaced per shard so ids never collide.
Branch creation is the only cross-shard write; it updates the (replicated)
timeline tree under a brief coordination point, not on the read/write hot path.

## Migration plan (each step keeps the standalone suite green)

1. **This design.** ✅
2. **Refactor global state into a `Shard` struct, `nshards = 1`, still one
   thread.** ✅ Pure mechanical refactor (page_idx/fork_idx/memtable/pgcache/
   layer_map/cursor/next_layer_id become `shard->...`); behavior identical.
   De-risks everything below.
3. **Partition channels + client-side routing**, still `nshards = 1` (routing is
   identity) — proves the routing path without parallelism. ✅ The contract lives
   in `pagestore_ipc.h` (`PS_NSHARDS`, `ps_shard_for_key`, `ps_shard_channel_range`),
   the daemon publishes `nshards` in the shm header and routes keys with
   `shard_for` → `ps_shard_for_key`, and the standalone client claims one channel
   per shard pool and routes each op to its key's shard (a unit test checks the
   helpers for `nshards` up to 8).  The in-engine client (`backend_localsvc`)
   keeps its single channel — correct at `nshards = 1` (the sole pool) — and
   adopts per-key routing in step 4 when `nshards > 1` requires it.
4. **`nshards > 1` + one worker thread per shard** (POSIX first), in sub-slices:
   - **4a — per-shard manifest + layer map + id namespace** (still `nshards = 1`,
     single thread). ✅ The manifest is now a `PsManifest` handle (durable log +
     layer map) owned per shard; each shard has its own `layers.<shard>.manifest`
     and its own `next_local_id`, and `layer_id` is shard-namespaced (high bits =
     shard) so the id-named layer files never collide across shards in the one
     store dir.  Compaction/GC/lookup are parameterized by `Shard *`.  Behavior
     identical at one shard (ids 1,2,3…; one manifest) — de-risks the threading
     slice like step 2 did.
   - **4b** — runtime `nshards > 1`, still single-threaded serve loop over all
     pools (proves multi-shard data partitioning end-to-end before threads).
   - **4c** — one POSIX worker thread per shard; `backend_localsvc` per-shard
     channel pool + routing.  This is where real parallelism lands.
5. **SPDK per-thread qpairs** so the async path scales per core.
6. **Branch-create coordination** for the shared timeline tree.

Each step is independently testable; step 2 is the big mechanical change and is
behavior-preserving, so the risky parallelism (step 4) sits on a proven refactor.

## Engine refinements this sharding depends on (informed by ScyllaDB)

ScyllaDB is the reference for share-nothing shard-per-core LSM (Seastar:
`hash(key) -> core`, per-core memtable/SSTable/cache, shard-aware clients,
per-core I/O scheduler).  This design matches it.  But a few engine details must
change *before or with* sharding, or per-shard threads will stall:

1. **Compaction must move off the serve thread (hard prerequisite).** Today
   `maybe_compact()` runs inline in `append_page()` on the write path.  Once each
   shard is a single thread, an inline compaction blocks that shard's reads and
   writes for the whole merge.  Compaction must become a **background per-shard
   task** with **backpressure** (throttle/stall writes only when the layer count
   or memtable backlog crosses a high-water mark), à la ScyllaDB's compaction /
   flush controllers.  Until this lands, sharding would just multiply stalls.
2. **Real compaction strategy, not "merge all".** The current policy rewrites all
   of a timeline's image layers into one whenever the count exceeds a threshold —
   unbounded read/space amplification control.  Adopt **size-tiered** (simplest:
   merge similarly-sized layers) first; if space amplification matters, an
   **incremental / fragmented** scheme (ScyllaDB ICS) bounds the temporary space a
   compaction needs.  Per-shard, so it stays lock-free.
3. **Per-layer key range (min/max) + bloom to skip layers.** `layer_map_lookup`
   scans every image layer of a timeline (O(layers)).  Record each layer's
   min/max key (already have start/end_key in `PsLayerDesc`) and *use it to
   prune*, and add a per-layer bloom filter, so a read touches only layers that
   can hold the key — RocksDB/ScyllaDB both rely on this.  Read amplification
   only gets worse with more layers per shard.

These are tracked as co-requisites: step 4 (real parallelism) should not ship
without (1); (2) and (3) can land incrementally alongside.

## Invariants

- Every version/timeline/block of a given key lives on exactly one shard.
- No layer spans shards; no manifest entry is shared.
- Hot path (read/write/flush/compact/GC of a shard) takes no global lock.
- Only branch-create touches cross-shard (timeline) state.
