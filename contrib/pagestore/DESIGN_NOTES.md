# pagestore — design notes & planned evolution

This module is a working prototype of a disaggregated, copy-on-write page store
for PostgreSQL.  Several pieces are deliberately simplified to get the end-to-end
architecture working and tested; this file records the directions they must take
to become production-grade.

Current MVP progress and scope are tracked in [MVP_STATUS.md](MVP_STATUS.md).
The test direction and rollout plan are in [HARNESS_DESIGN.md](HARNESS_DESIGN.md).

## 1. Storage & indexes should become LSM-like

**Current.**  The POSIX daemon uses the append-only segment log as write-ahead
staging, per-shard memtables, immutable image layers, a durable manifest, and a
layer map.  Flush coverage watermarks let restart rebuild the covered prefix
from layers and scan only the segment tail.  A background maintenance controller
compacts image layers, reclaims covered segments, and manages the optional
filesystem-backed object tier.

Some hot metadata remains in plain in-memory chained hash tables:

- page versions: `(timeline, key, block) -> list of {lsn, segment, offset}`
- fork sizes:    `(timeline, key) -> nblocks`
- per-page WAL index: `(timeline, key, block) -> ascending list of record LSNs`

Page data is still appended to flat segment files before layer flush, and shipped
WAL still has a flat per-timeline migration/tail authority (`wal_<tl>`).  Image
compaction prunes relation-page history behind durable resource frontiers, and
WAL-index snapshot compaction retains only proven FPI-led reconstruction chains
at the moving operational cutoff and discrete reader/branch fences.  Fork
metadata compaction and shipped-WAL reclamation are not implemented yet, so
long-running logical history is not fully bounded.

**Target.**  An LSM-like layered store, along the lines of Neon's pageserver:

- **image layers** — materialized page images at an LSN, over a key range.
- **delta layers** — WAL records / page deltas over a (key range × LSN range).
- a **layer map** indexing layers by (key range, LSN range); a read finds the
  covering image layer plus the delta layers above it and applies redo.
- layers are **on-disk and immutable**, with **compaction** (merge deltas into
  fresh image layers) and **GC** (drop layers/records below the retained-LSN
  horizon, e.g. once a page is materialized past them).

This bounds per-page replay chains and space, makes the index naturally
persistent (it *is* the on-disk layers), and enables tiering (e.g. cold layers
to object storage).

## 2. The daemon must run on multiple CPUs

**Current.**  The POSIX daemon runs one foreground worker per logical shard plus
a dedicated maintenance controller.  Per-shard locks protect page/fork/WAL-index
state and memtables; a shared lock still protects the global layer map and
timeline tree.  This provides real cross-shard concurrency and keeps compaction
off serve threads, but it is not yet the target share-nothing design: synchronous
I/O can block a shard and the shared map remains a coordination point.

**Target.**  The daemon must scale across cores, and the data structures must be
designed for multi-core from the start (not retrofitted):

- **Shard the key space across cores** — one thread (or SPDK reactor) per core
  owns a partition of relations; a backend's channel is routed to the owning
  core.  Each core's indexes/layers stay single-owner, so they remain lock-free
  by construction (the property we rely on today, preserved under sharding).
- Where structures must be shared, use **sharded locks or lock-free layer maps**.
- **Asynchronous / overlapped I/O** (io_uring, or per-core SPDK reactors) so a
  single disk I/O does not stall unrelated requests.

Items 1 and 2 should be **co-designed**: partition the LSM layers by key shard,
keep a per-core layer map, and run compaction/GC per shard.  In other words, the
choice of data structure (item 1) must account for how it is owned and accessed
across cores (item 2).

## 3. The in-memory cache must be database-adapted, not an fs page cache

SPDK bypasses the kernel, so it loses the OS page cache and readahead.  A
benchmark made this concrete: on sequential reads POSIX (kernel readahead +
page cache) does ~1574 MiB/s cold vs SPDK ~756; on *random* reads the kernel's
advantage evaporates and the two tie (~750).  The naive fix -- a userspace
block-address LRU -- is the wrong design: **you still fill it with the same I/O
bandwidth**, so it merely re-implements the kernel cache we just bypassed, and a
single big seqscan thrashes it.  You cannot out-bandwidth a cold/streaming
workload, so don't try; be selective and cache at the level of database
semantics, where the store knows things a block cache cannot:

- **Cache materialized page versions, not blocks.**  Key by
  `(timeline, key, block, LSN)`; the value is the reconstructed page image.  A
  hit then skips both the device read *and* the WAL replay (single-page redo,
  3c-4) that is the genuinely expensive work -- this is the page-server insight
  (Neon's pageserver caches redo outputs).  Branches share a parent's cached
  page by read-through instead of duplicating it.
- **Value-aware admission/eviction, not pure LRU.**  Keep what is expensive to
  reproduce and likely reused -- B-tree interior nodes, catalog/SLRU, pages with
  long redo chains -- ranked by `reproduction-cost x reuse-probability`, not
  recency alone.
- **Scan-resistant.**  A bulk seqscan must not evict the hot set; use a
  ring/probation policy (exactly what PostgreSQL's buffer access strategies,
  e.g. `BAS_BULKREAD`, do).  Detect sequential block runs, or take a hint.
- **Structure-aware prefetch, not blind readahead.**  Prefetch driven by access
  type (index range scan, bitmap heap scan) -- ideally from hints the compute's
  AIO prefetcher already computes -- rather than guessing from offsets.
- **Unified with the LSM image layers (item 1).**  An image layer is a persisted
  materialization of pages at an LSN over a key range; the RAM cache is just the
  hot tier of that same materialization.  So cache, image-layer materialization,
  and compaction are one system with one policy (what to materialize, keep hot,
  persist, evict), not a bolt-on LRU.
- **Per-shard and lock-free (item 2):** each core owns its cache slice.

In short, the cache's job is not "read the disk less" (a block cache does that
and still needs the bandwidth) but "do less of the work the database makes
expensive" -- redo replay and repeated index traversal -- while staying
disciplined about scans.  Items 1, 2 and 3 are co-designed.

## Other known simplifications (smaller)

- Relation and fork indexes are rebuilt on restart.  The per-page WAL index is
  durably appended in per-(timeline, shard) logs; maintenance publishes an
  immutable multi-shard snapshot that atomically selects fresh log epochs, and
  restart replays only the selected epoch tail.  Superseded snapshot and log
  epochs are removed asynchronously.  Newly decoded entries durably distinguish
  known FPI bases from known deltas and carry the record-end LSN used for as-of
  visibility; legacy entries remain explicitly unknown and cannot authorize
  pruning.  Replacement-base selection and pruning old entries from the
  snapshot are still future maintenance work.
- The IPC channel uses busy-polling (no eventfd) and one copy via the channel
  buffer (no zero-copy into shared_buffers); see the performance discussion.
- WAL is parsed by reusing PostgreSQL's reader, never reimplemented in the daemon
  (see WAL_REDO.md); the per-page WAL index population follows that rule.
- A redo worker may use the default `recovery_prefetch=try`.  The localsvc
  backend has no kernel-readahead primitive, so advisory prefetch degrades to
  ordinary reads; forcing `recovery_prefetch=on` remains an error.  Its
  `smgr_startreadv` path is available independently for PostgreSQL AIO reads.
