# pagestore — design notes & planned evolution

This module is a working prototype of a disaggregated, copy-on-write page store
for PostgreSQL.  Several pieces are deliberately simplified to get the end-to-end
architecture working and tested; this file records the directions they must take
to become production-grade.

## 1. Storage & indexes should become LSM-like

**Current.**  The daemon keeps its metadata as plain in-memory chained hash
tables:

- page versions: `(timeline, key, block) -> list of {lsn, segment, offset}`
- fork sizes:    `(timeline, key) -> nblocks`
- per-page WAL index: `(timeline, key, block) -> ascending list of record LSNs`

Page data is appended to flat segment files (`seg_NNNNNNNN`); shipped WAL goes to
flat per-timeline logs (`wal_<tl>`).  There is **no compaction and no GC**: the
version lists and the per-page LSN lists grow without bound, and the indexes are
rebuilt by scanning everything on restart.

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

**Current.**  The daemon is **single-threaded**: one poll loop scans all
channels and processes requests serially, with synchronous blocking I/O
(`pread`/`pwrite`/`open`).  It uses a single core.  This is precisely why the
in-memory indexes above need no locking — there is only one accessor.  It is
also a serialization point and a hard throughput ceiling.

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

## Other known simplifications (smaller)

- Indexes are in-memory and rebuilt on restart; the per-page WAL index is not
  yet persisted at all.
- The IPC channel uses busy-polling (no eventfd) and one copy via the channel
  buffer (no zero-copy into shared_buffers); see the performance discussion.
- WAL is parsed by reusing PostgreSQL's reader, never reimplemented in the daemon
  (see WAL_REDO.md); the per-page WAL index population follows that rule.
- `recovery_prefetch=off` is required by the redo worker (the backend's
  recovery-prefetch/AIO path is not wired).
