# pagestore LSM-like storage and object-tier plan

This document records the planned evolution of `contrib/pagestore` from the
current append-only page-version segment log into an LSM-like page store whose
lowest tier can be object storage.

The design target is intentionally scoped to pagestore.  It should not require
rewriting PostgreSQL heapam/tableam, and the existing smgr/local-service IPC ABI
should remain stable unless a later performance phase proves that it must
change.

## Current state

The current pagestore stores page versions in append-only segment files:

- page writes append `[SegRecHdr | page bytes]` to `seg_NNNNNNNN`;
- an in-memory hash table maps `(timeline, key, block)` to page versions;
- reads locate a version in memory and read page bytes from the segment;
- restart rebuilds the in-memory indexes by scanning all segment records;
- there is no compaction, garbage collection, or persistent layer index.

This is already log-structured, but it is not yet LSM-like: historical state is
not organized into immutable searchable layers, and metadata is not persistent
except by replaying the full data log.

## Target model

The target model is a layered page store:

```text
mutable memtable / staging
  -> local hot immutable layers
  -> local cold layer cache
  -> remote object-storage layers
```

The LSM-like core uses:

- image layers: materialized page images at an LSN over a key range;
- delta layers: WAL records or page deltas over a key range and LSN range;
- a layer map: persistent logical index over `(timeline, key range, LSN range)`;
- compaction: merge delta chains and image layers into fresher image layers;
- GC: drop layers no longer needed by retention horizons or branches;
- tiering: upload sealed immutable layers to object storage and optionally evict
  local copies.

Object storage is not a separate storage engine.  It is the cold durable tier for
immutable layers.

## Core invariants

These invariants should be preserved across all phases:

- Layers are immutable once sealed.
- The manifest is the authoritative metadata source.
- The read path uses the layer map and manifest, not object-storage listing.
- A layer must not lose its local copy until the manifest records it as remotely
  durable.
- A layer must not be removed from remote storage until no retained LSN,
  timeline, branch, or active read can need it.
- Object storage must not be on the hot write transaction path.
- Compaction creates new local sealed layers; tiering uploads those layers later.
- GC is persistent and retryable: failed local or remote deletes leave durable
  metadata that allows retry after restart.

## Logical layer metadata

Layer identity must be separate from physical location.  Core code should reason
about logical layers; a lower layer-store abstraction decides whether bytes come
from local storage or object storage.

Suggested logical descriptor:

```c
typedef enum PsLayerKind
{
	PS_LAYER_IMAGE,
	PS_LAYER_DELTA,
} PsLayerKind;

typedef enum PsLayerTier
{
	PS_LAYER_TIER_LOCAL_HOT,
	PS_LAYER_TIER_LOCAL_COLD,
	PS_LAYER_TIER_REMOTE_OBJECT,
} PsLayerTier;

typedef struct PsLayerLocation
{
	PsLayerTier tier;
	char uri[...];          /* local path, device location, or object URI */
	uint64_t size;
	uint32_t generation;
	bool available;
} PsLayerLocation;

typedef struct PsLayerDesc
{
	uint64_t layer_id;
	PsLayerKind kind;

	uint32_t timeline;
	PsKey start_key;
	PsKey end_key;
	uint32_t start_block;
	uint32_t end_block;

	uint64_t lsn_start;
	uint64_t lsn_end;

	uint32_t location_count;
	PsLayerLocation locations[...];

	uint64_t created_at_lsn;
	uint64_t remote_uploaded_lsn;
	bool remote_durable;
	bool local_pinned;
	bool deleting;
} PsLayerDesc;
```

The exact key-range representation can change, but it must be able to describe
both relation/fork ranges and block ranges.

## Layer store abstraction

The existing `PsStorage` interface is byte-log oriented.  LSM layers need a
higher-level layer-store abstraction.

Suggested responsibilities:

```c
typedef struct PsLayerStore
{
	int (*create_local_layer)(...);
	int (*seal_local_layer)(uint64_t layer_id);
	int (*read_layer_block)(uint64_t layer_id, uint64_t off,
							void *buf, uint32_t len);
	int (*upload_layer)(uint64_t layer_id);
	int (*download_layer)(uint64_t layer_id);
	int (*delete_local_layer)(uint64_t layer_id);
	int (*delete_remote_layer)(uint64_t layer_id);
	int (*layer_exists_remote)(uint64_t layer_id);
} PsLayerStore;
```

`pagestore_core.c` should not open local files or call object-storage APIs
directly.  It should ask the LSM/layer-store layer to read a logical layer
block, ensure a layer is local, upload a sealed layer, or evict a local copy.

## Manifest

The manifest is the durable source of truth for layers and their lifecycle.
It should be an append-only event log initially, with optional checkpointing
later.

Recommended manifest events:

```text
MANIFEST_ADD_LAYER
MANIFEST_SEAL_LAYER
MANIFEST_SET_REMOTE_DURABLE
MANIFEST_DROP_LOCAL
MANIFEST_MARK_DELETE
MANIFEST_REMOVE_LAYER
```

Crash-recovery requirements:

- if a layer is present but not sealed, it is ignored or repaired;
- if upload started but `REMOTE_DURABLE` was not recorded, local copy remains
  authoritative and upload may be retried;
- if `REMOTE_DURABLE` is recorded, the layer can be recovered from object
  storage even if the local file is absent;
- if `MARK_DELETE` is recorded, local and remote delete operations may be
  retried until `REMOVE_LAYER` is recorded.

Object-store listing may be used for diagnostics or repair tooling, but not as
normal startup authority.

## Layer file format

Layer files should be immutable, range-readable, and object-storage friendly.

Recommended layout:

```text
[data blocks]
[sparse index]
[optional bloom/filter]
[footer]
[magic/version/checksum]
```

Important properties:

- footer can be found by reading the final fixed-size trailer;
- data blocks have checksums;
- sparse index maps `(key, block, lsn)` or key ranges to data offsets;
- files are large enough to avoid excessive object counts;
- small layers are merged by compaction.

The first implementation can use uncompressed full-page image records.  Delta
records, compression, bloom filters, and remote range reads can be added later.

## Read path

The eventual read path is:

```text
read(timeline, key, block, read_lsn)
  -> check materialized-page cache
  -> check mutable memtable
  -> layer map finds nearest image layer <= read_lsn
  -> layer map finds delta layers between image_lsn and read_lsn
  -> layer store reads needed blocks
  -> apply deltas in LSN order
  -> cache and return materialized page
```

For remote layers:

```text
read layer block
  -> try local hot/cold copy
  -> if local miss and remote_durable, download whole layer or remote range-read
  -> populate local layer-block cache
  -> return bytes
```

The first object-storage version should prefer whole-layer download because it
is simpler and safer.  Remote range reads can be added after the layer footer and
index format are stable.

## Write path

The first LSM write path should keep the current full-page-write semantics:

```text
append_page()
  -> append full page to mutable staging
  -> insert into memtable
  -> flush memtable/staging into sealed image layer at threshold
  -> record layer in manifest
```

Later, when delta layers are introduced:

```text
WAL ingest / WAL index
  -> append delta record to mutable delta staging
  -> flush into sealed delta layer
  -> reads reconstruct page from image + deltas
```

Delta-layer implementation should not reimplement PostgreSQL WAL parsing inside
the pagestore daemon.  It should reuse the existing WAL reader/redo-worker
direction and allow fallback to full-page images for unsupported records.

## Compaction

Compaction is separate from tiering.

Compaction:

```text
old image layer + delta layers -> new local sealed image layer
```

Tiering:

```text
sealed local layer -> upload object -> mark remote durable -> optional local eviction
```

Initial compaction triggers can be simple:

- per-page delta chain length exceeds a threshold;
- too many delta layers cover the same key range;
- too many small layers exist in a shard;
- retention horizon moved far enough to make older layers mergeable.

Compaction must install new layers via the manifest before old layers become
GC candidates.

## Garbage collection

GC depends on a retained-LSN horizon:

```text
retain_lsn = min(active read LSNs,
                 branch/timeline required LSNs,
                 configured retention LSN)
```

A layer can be removed only when:

- its contents are not needed by any retained read or branch;
- for delta layers, its effects are covered by a newer image layer where needed;
- the manifest records the deletion state durably.

Recommended remote-aware GC sequence:

```text
1. mark layer deleting in manifest
2. delete local copy if present
3. delete remote object if present
4. record manifest remove
```

Every step must be idempotent and retryable.

## Cache model

The cache should evolve into two tiers:

- layer-block cache: keyed by `(layer_id, offset)`, caches immutable layer bytes;
- materialized-page cache: keyed by `(timeline, key, block, read_lsn)`, caches
  the reconstructed page after applying deltas.

The existing segment-offset page cache can become the layer-block cache.  GC
must be able to evict all cache entries for a dropped layer by `layer_id`.

The materialized-page cache should be added after delta reads exist; before that,
full-page image reads make the two tiers largely equivalent.

## Multi-core sharding

Sharding should be a later phase, after layer semantics are stable.

Suggested ownership:

```text
shard = hash(timeline, key, block) % nshards
```

Each shard owns:

- memtable;
- layer map;
- layer-block cache slice;
- materialized-page cache slice;
- compaction queue;
- tiering queue.

This preserves the current single-owner property for hot-path data structures
while allowing the daemon to scale across cores.

## Implementation phases

### Phase 1: local-only layer metadata foundation

- Add `PsLayerDesc`, layer map, and manifest event log.
- Add a `PsLayerStore` abstraction, initially backed only by local POSIX files.
- Keep existing page-write semantics.
- Do not add object storage yet.

Acceptance criteria:

- startup rebuilds the layer map from manifest;
- sealed layers are visible through the layer map;
- core read code does not depend on segment filenames directly.

### Phase 2: full-page image layers

- Flush mutable full-page records into immutable image layers.
- Read from memtable first, then image layers.
- Keep old segment recovery temporarily for migration/debugging.

Acceptance criteria:

- read semantics match current full-page version store;
- restart does not need to scan all historical page records;
- image layer files have checksums and sparse indexes.

### Phase 3: local compaction and local GC

- Merge image layers and small ranges locally.
- Add manifest state transitions for replacement and deletion.
- Add cache invalidation by `layer_id`.

Acceptance criteria:

- compaction installs new layers atomically;
- old local layers are removed only after manifest state is durable;
- restart after interrupted compaction or GC is safe.

### Phase 4: object-tier metadata and upload

- Extend layer locations to include remote object URIs.
- Add object-storage provider code behind the layer-store/tiering layer.
- Upload sealed local layers asynchronously.
- Mark layers `remote_durable` only after upload verification.

Acceptance criteria:

- local copy is never deleted before `remote_durable`;
- restart can resume or retry incomplete uploads;
- normal reads still work from local copies.

### Phase 5: local eviction and remote download

- Add local cold-cache eviction policy.
- On read, download a remote-durable layer when local copy is missing.
- Initially prefer whole-layer download over remote range GET.

Acceptance criteria:

- a layer with only remote durable location can be read after download;
- downloaded layers are verified before use;
- repeated reads use the local cache/copy.

### Phase 6: remote-aware GC

- Extend GC to delete remote objects.
- Persist `deleting` state and retry failed deletes.

Acceptance criteria:

- object deletion is idempotent;
- manifest never references a removed layer as readable;
- remote leaks can be detected and repaired by tooling.

### Phase 7: delta layers and page redo

- Add mutable delta staging and immutable delta layers.
- Use per-page WAL indexing to find deltas for reads.
- Reconstruct pages from image + ordered deltas.
- Keep fallback to full-page images for unsupported redo cases.

Acceptance criteria:

- bounded redo chain by compaction policy;
- page reads at an LSN match full-page materialization semantics;
- unsupported WAL records do not corrupt reads.

### Phase 8: materialized-page cache

- Add cache keyed by `(timeline, key, block, read_lsn)`.
- Use delta-chain length as an admission/value signal.
- Keep the layer-block cache as the lower tier.

Acceptance criteria:

- repeated reads avoid both layer I/O and redo;
- GC does not invalidate fixed-LSN materialized pages incorrectly;
- latest-read entries are invalidated or versioned correctly.

### Phase 9: sharded multi-core daemon

- Partition key space across shards.
- Give each shard its own memtable, layer map, caches, and queues.
- Route channels or requests to the owning shard.

Acceptance criteria:

- hot path remains mostly single-owner per shard;
- compaction/tiering does not block unrelated shards;
- read/write semantics match single-shard behavior.

## First milestone recommendation

The first milestone should be `local-only full-page image layers`.

It should not include delta redo, object storage, remote range reads, SPDK async,
or multi-core sharding.  That milestone establishes the persistent layer map and
manifest, which are prerequisites for every later phase.

Minimal first-milestone deliverables:

- manifest event log;
- layer map rebuilt from manifest;
- local image-layer writer and reader;
- memtable/staging flush into image layers;
- read path through memtable plus image layers;
- restart without full segment scan;
- no-op placeholders for tiering and remote locations.

