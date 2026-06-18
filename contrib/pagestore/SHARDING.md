# pagestore — multi-core sharding design

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

1. **This design.**
2. **Refactor global state into a `Shard` struct, `nshards = 1`, still one
   thread.** Pure mechanical refactor (page_idx/fork_idx/memtable/pgcache/
   layer_map/cursor/next_layer_id become `shard->...`); behavior identical,
   116/116 unchanged.  De-risks everything below.
3. **Partition channels + client-side routing**, still `nshards = 1` (routing is
   identity) — proves the routing path without parallelism.
4. **`nshards > 1` + one worker thread per shard** (POSIX first).  Per-shard
   manifest files + shard-namespaced layer/segment files.  This is where real
   parallelism lands.
5. **SPDK per-thread qpairs** so the async path scales per core.
6. **Branch-create coordination** for the shared timeline tree.

Each step is independently testable; step 2 is the big mechanical change and is
behavior-preserving, so the risky parallelism (step 4) sits on a proven refactor.

## Invariants

- Every version/timeline/block of a given key lives on exactly one shard.
- No layer spans shards; no manifest entry is shared.
- Hot path (read/write/flush/compact/GC of a shard) takes no global lock.
- Only branch-create touches cross-shard (timeline) state.
