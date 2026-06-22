# WAL redo design (reconstructing pages from WAL in the store)

This is the third and largest WAL-shipping layer: having the store **reconstruct
pages by replaying WAL**, so that a read compute can be stateless and a branch
is a complete clone.  It is a multi-step effort; this document records the plan
and what is implemented so far.

## Why not reimplement redo

PostgreSQL applies WAL per resource manager (`heap`, `btree`, `gin`, `xact`,
`clog`, ... — each with an `rm_redo`).  Reimplementing all of that in the daemon
would be rewriting half of PostgreSQL and would break every major version.

So we **reuse PostgreSQL's own redo** (the approach Neon takes).  The cleanest
realization that needs *no* redo reimplementation: a dedicated PostgreSQL
instance in **continuous recovery** whose storage is the page store —

- relations routed to the store (`pagestore.route_all`),
- WAL fed from the store (the shipped WAL, M6),
- recovery's `rm_redo` reads base pages from the store via smgr, applies the
  records, and writes the resulting pages back to the store via smgr.

That "redo worker" materializes pages into the store using stock recovery code.
Write computes ship WAL; the redo worker turns WAL into pages; read computes
read pages at an LSN.

## Layers and status

1. **WAL transport** — compute ships WAL to the store. ✅ (M6: archive module →
   `PS_OP_WAL_APPEND`, per-timeline `wal_<tl>` log.)
2. **WAL serving** — store hands WAL back by LSN range, so a redo worker can pull
   it. ✅ (`PS_OP_WAL_READ`; `wal_read()` assembles bytes across records.)
3. **Redo worker** — a recovery PostgreSQL that consumes the store's WAL and
   materializes pages into the store. 🔶 In progress:
   - **3a** Reconstruct standard WAL segment files from the `wal_<tl>` log. ✅
     `pagestore_walrestore` does this and works as a `restore_command`
     (`pagestore_walrestore --shm NAME --timeline N --segsize B %f %p`); the
     integration test reconstructs a shipped segment as a full standard segment.
   - **3b** Bring up a PG node in archive recovery with `route_all` on the store
     and its WAL fetched from the store; verify it materializes pages. ✅
     `redo_worker_demo.sh`: a base backup (`pg_backup_start`/`stop`) marks the
     recovery start, then the instance recovers with
     `restore_command = pagestore_walrestore` and empty local pg_wal, so all WAL
     comes from the store; recovery's rm_redo replays it into the store and the
     post-backup change is recovered.  Reuses PostgreSQL's redo wholesale.
     Caveat: the redo instance runs with `recovery_prefetch = off` (the
     backend's recovery-prefetch/AIO path is not wired yet).
   - **3d-1** WAL-only compute -> non-redundant redo. ✅ `wal_only_redo_demo.sh`:
     the writer runs with `route_all = off`, so its relation pages stay local
     and only its WAL is shipped; the redo worker (`route_all = on`) then
     materializes the relations into the store purely by replaying that WAL.
     Verified the table never reached the store from the compute (its file is
     local) yet exists in the store after redo, and the store grew.  This is the
     point of redo: the store's pages come from WAL, not from the compute.
   - **3c** Materialize-on-demand: when a read misses a page at an LSN, drive
     redo for just that page (Neon's per-page model) instead of replaying
     everything.
       - **3c-1** Per-page WAL index. ✅ The store maps (timeline, key, block) ->
         the LSNs of WAL records that modify that page (`PS_OP_WAL_INDEX_ADD` /
         `PS_OP_WAL_INDEX_GET`, with branch read-through capped at the fork LSN).
         This is the lookup the single-page redo needs.  (Populating it by
         decoding shipped WAL via PostgreSQL's XLogReader / pg_walinspect is next;
         reimplementing the WAL format in the daemon is deliberately avoided.)
       - **3c-2** Populate the index by decoding shipped WAL. ✅ Reuses
         PostgreSQL's own WAL reader (`read_local_xlog_page`), exposed as
         `pagestore_index_wal(start, end)`.  Note: decoding **must run in a
         normal backend** -- the archiver process lacks the recovery/timeline
         context the reader asserts on (both `read_local_xlog_page` and `WALRead`
         abort there).  In production a background worker would call it as WAL is
         shipped; the SQL function lets a test drive it (integration test indexes
         a fresh table and confirms its block has records).  No daemon-side WAL
         parser is written.
       - **3c-3** Reconstruct a single page's base image from WAL. ✅
         `pagestore_redo_page(rel, fork, block, lsn)` uses the per-page index to
         find the newest full-page image at/below lsn and restores it
         (`RestoreBlockImage`) -- rebuilding one page from WAL on demand.  A WAL
         full-page image is the page *as modified by* its record (it is captured at
         XLogInsert, after the change; recovery restores it and returns
         `BLK_RESTORED`, skipping the delta), so this image is the page exactly
         as-of that record's end LSN -- the base for any later deltas.
       - **3c-4** The `--wal-redo`-style helper: apply the delta records *after*
         the base image (not the base record itself, which the image already
         includes) with `rm_redo` to get the page exactly as-of lsn.  This
         is the one piece that needs PostgreSQL's redo run against a single held
         page (Neon's wal-redo process / a small core mode) -- the last hard
         piece of the read path.  📐 Designed, not yet implemented: see
         "3c-4 detailed design" below.
   - **3d-2/3** SLRU/clog + `pg_control` on the store, and branch WAL
     read-through (serve a branch's WAL across its fork point), so multiple
     independent computes can run concurrently on different branches with no
     shared local state.

## 3c-4 detailed design — single-page redo (apply deltas onto a base image)

This is the last hard piece of the per-page read path.  3c-3 already gives the
*base* image (newest full-page image at/below the target LSN); 3c-4 applies the
WAL **delta** records after it, via PostgreSQL's `rm_redo`, to produce the page
*exactly as-of* a target LSN.  This is the `materialize()` step from
MATERIALIZATION.md (`base + deltas -> rm_redo -> image-layer page`).

### Why it needs a dedicated mechanism

`rm_redo` handlers do not take a page argument; they fetch the page they mutate
through `XLogReadBufferForRedo` -> `XLogReadBufferForRedoExtended` ->
`XLogReadBufferExtended` (the buffer manager / smgr), and that path drives the
gating we must honor (see `xlogutils.c`):

- if the record carries a usable full-page image, it is restored and the call
  returns `BLK_RESTORED`;
- else if `record_lsn <= PageGetLSN(page)` it returns `BLK_DONE` (already
  applied) and the handler skips the block;
- else `BLK_NEEDS_REDO` and the handler mutates the buffer.

So to redo *one* page in isolation we must make those buffer reads return our
single held page for the target `(rlocator, forknum, blkno)`, seeded to the base
image with its page LSN = the base record's end LSN so the gating applies exactly
the records in the half-open window the driver sends.  Other blocks a record also
touches are served by *scratch* buffers marked already-applied (details under
"Buffer redirection" below) — returning `BLK_NOTFOUND` is wrong, because some
handlers init-then-dereference those blocks unconditionally.  Doing this inside a
normal backend is unsafe: the handlers assume a recovery-ish context, and they
would read/dirty the *real* shared buffers.

### Approach: a sandboxed `postgres --wal-redo` process (Neon model)

A dedicated, short-lived process that holds exactly one page and applies records
to it — the approach the doc has pointed at throughout.  Chosen over an in-backend
hack for correctness (isolation from shared buffers / concurrency) and security
(it only ever sees a base page + record bytes; it can be seccomp-sandboxed, so
decoding attacker-influenced WAL cannot touch the system).

**Process**: a new `--wal-redo` startup mode (bootstrap-like: no postmaster, no
catalog access).  Its "buffer pool" is a **small fake buffer table keyed by block
id**, not a single `BLCKSZ` slot: a record can hold several registered buffers
live at once (e.g. `btree_xlog_split()` keeps the new right sibling, the original
page, and possibly the right neighbor pinned until it finishes), so one slot would
alias releases / overwrite contents.  One entry is the target (kept); the rest are
scratch (discarded).  Driven by a length-prefixed binary protocol on a pipe pair:

| msg | payload | effect |
|-----|---------|--------|
| `BEGIN` | rlocator, forknum, blkno | set the target page identity |
| `PUSHBASE` | base_end_lsn, page bytes \| (none = zero) | seed the held page (base FPI, or zero for a WILL_INIT first record) **and set its page LSN to base_end_lsn** — `RestoreBlockImage` copies only bytes; PG sets `pd_lsn` separately, so the gating needs the LSN sent explicitly |
| `APPLY` | record **start LSN, end LSN**, raw record bytes | `rm_redo` the record onto the held page; `EndRecPtr` is set from the supplied end LSN |
| `GET` | — | return the current page bytes (the as-of result) |

The record **end LSN must come from a real decode**, not from the byte length: a
record can span a WAL page, so its end pointer accounts for continuation page
headers/padding.  The driver decodes each delta with `XLogReader` over the store
WAL (segments reconstructed à la `pagestore_walrestore`), which yields a
`DecodedXLogRecord` with the correct `EndRecPtr`, and sends that end LSN in
`APPLY`.  (Equivalently the helper could run the `XLogReader` itself; keeping it in
the driver leaves the helper a pure record-applier.)  Both redo gating
(`XLogReadBufferForRedoExtended`) and the page LSN it stamps use
`record->EndRecPtr`, so getting it wrong skips or replays the wrong records.

Buffer redirection (under an `am_walredo` flag):

- **Target block** → the held buffer, seeded to the base image with its page LSN
  set to the base record's *end* LSN (Driver step 1) so the
  `BLK_DONE`/`BLK_NEEDS_REDO` gating is exact.
- **Non-target blocks a record also registers** → a *scratch* buffer, **not**
  `BLK_NOTFOUND`.  Some multi-block handlers init-then-dereference
  unconditionally — e.g. `btree_xlog_split()` does `XLogInitBufferForRedo(record,
  1)` for the right sibling and writes it before touching the original page — so
  returning not-found would leave them operating on an invalid buffer and crash.
  Every registered block therefore gets a usable buffer; only the target's
  contents are returned by `GET`, the scratch buffers are discarded.
  - **The scratch buffer must read as already-applied.** A fresh zero scratch
    page has `pd_lsn = 0`, so `XLogReadBufferForRedo` would return
    `BLK_NEEDS_REDO` and a handler (e.g. heap update touching the *other* page)
    would dereference its bogus line pointers.  So a non-target scratch read
    returns `BLK_DONE` (equivalently, present `pd_lsn >= record->EndRecPtr`) — the
    handler skips it — **except** for a block actually flagged `WILL_INIT`, which
    is meant to be zero-initialized and replayed; that one is initialized
    normally.  We never need the scratch *result*, only that the handler doesn't
    crash and doesn't touch the target.
- **Side effects beyond the buffer read must also be stubbed — target-aware.**
  Redo handlers reach auxiliary storage outside `XLogReadBufferExtended`: heap
  redo calls `visibilitymap_pin`/`visibilitymap_clear` and
  `XLogRecordPageWithFreeSpace` when VM/FSM bits change.  When the requested fork
  is the **heap (main) fork**, these are no-ops under `am_walredo` (don't read or
  dirty real VM/FSM storage).  But when the requested fork **is** the VM or FSM,
  those very calls *are* the change to capture — so the redirection must route
  the VM/FSM update to the held page instead of no-oping it.  A wrinkle: some heap
  records clear VM bits without registering the VM block in the per-page index, so
  materializing a VM page cannot rely on `walidx_get` alone; the VM/FSM forks need
  their own derive-from-heap rebuild rule (or be excluded from on-demand redo and
  always rebuilt), documented as a follow-up (3c-4b).  The patch enumerates every
  such call reachable from `rm_redo` and guards it per target fork — the bulk of
  the "small core mode" work (Neon's wal-redo does the same).

Mechanism (wal-redo-specific tiny smgr vs guarded branches in the read path) is a
patch detail; the smgr route keeps core least-touched, but the scratch-LSN and
fork-aware VM/FSM guards are needed either way.

### Driver side (normal backend, contrib/pagestore)

Core API — keyed by store identity, not a live relation:
`redo_page_asof(timeline, RelFileLocator/PageStoreRelKey, fork, block, lsn) ->
page`.  The background materializer and read-miss callers work in page-store keys
and must materialize pages for relations that are dropped, rewritten, or absent
from any catalog the helper can see; resolving a `regclass`/`rel` could fail or
point at the wrong relfilenode.  A `pagestore_redo_page_asof(rel regclass, …)` SQL
wrapper that resolves the locator stays only as a test convenience.

0. **Truncation/drop check first — by fork size at the target LSN.** `walidx_get`
   only knows records that register *this* block, so it misses page-removing
   records that carry just a relation locator — `XLOG_SMGR_TRUNCATE` (new block
   count), relation drop.  Decide from the **fork size as-of `lsn`** (equivalently
   the latest remove vs. any later re-creation): if `block >= size_at(lsn)` (or
   the relation is dropped with no later re-create at `lsn`) the page is gone —
   return "no page".  A truncation followed by re-extension/reinit before `lsn` is
   *not* terminal: the block exists again and is materialized from the
   post-truncation `WILL_INIT`/FPI chain.  (Requires the store to track per-fork
   size / truncation+create LSNs; a dependency.)
1. `base, base_end_lsn` = 3c-3 (`pagestore_redo_page`, extended): newest FPI
   at/below `lsn`, **and the end LSN of the record that carried it** — or a zero
   page if the first relevant record has `BKPBLOCK_WILL_INIT`.  The end LSN
   matters: normal redo sets a restored page's LSN to `record->EndRecPtr`, and the
   held page must be seeded with that LSN (not bytes alone) or the gating treats
   already-covered records as needing redo / mis-orders the chain.
2. `deltas` = the WAL records for this page in `[base_end_lsn, lsn)` from the
   per-page index, ascending.  The index is keyed by record **start** LSN while
   page LSNs / redo gating use record **end** LSN; this is the same half-open,
   start-LSN-keyed window `ps_read_plan_build` builds (`[base_lsn, read_lsn)`),
   *not* `(base_lsn, lsn]` — a closed upper bound would replay a record starting
   exactly at `lsn` and overshoot.
   - **Order across timeline ancestry, and keep each record's source timeline.**
     On a branched read `walidx_get()` walks the branch by appending the child
     timeline's LSNs after the parent's capped LSNs (`pagestore_core.c`
     ~1614–1631), so the raw result is *not* globally ascending — applying it
     as-is would replay child WAL before inherited parent WAL.  The driver must
     merge/sort into a single ascending chain before `APPLY`.  Sorting by LSN
     alone is insufficient: the current response is a bare `uint64` LSN array, and
     `PS_OP_WAL_READ` / `wal_read()` are single-timeline, so an inherited parent
     LSN fetched against the child stream would hit a hole / short read.  So each
     delta must carry its **source timeline** (the index returns `(timeline, lsn)`
     pairs), and step 3 fetches each record from *that* timeline's WAL — or
     `WAL_READ` becomes branch-aware.  Dependency: `walidx_get` exposing the
     source timeline per LSN.
   - **Chain completeness is mandatory.** A single `walidx_get` response is capped
     by the IPC payload (`PS_IO_UNIT / sizeof(uint64)`) with no continuation, so a
     hot page with more records than fit would be silently shortened and
     materialize an *older* page while reporting success.  The lookup must paginate
     / range-seek (cursor by start LSN) or return an explicit overflow error;
     truncation is never silently accepted.
3. open (or reuse a pooled) wal-redo process; `BEGIN(target)`; `PUSHBASE(base,
   base_end_lsn)`; for each delta **read+decode the record from the store's WAL
   service**, fetching from the record's *own* source timeline (`PS_OP_WAL_READ`,
   the same per-timeline source `pagestore_walrestore` uses) — not a local
   `XLogReader`, because the target stateless/WAL-only compute has an empty local
   `pg_wal`; the bytes live in the store.  The decode yields the record's
   `EndRecPtr`; `APPLY(start, end, bytes)`; `GET`.
4. return the page as-of lsn.

This is the function both the **background materializer** and the **on-demand
read-miss** path call.  Per MATERIALIZATION.md redo stays *off the hot read
path*: the materializer turns delta layers into image-layer pages (write back via
`WRITEV` / `ADD_LAYER`) so later reads are already materialized; an uncovered
cold read drives the same function for just that page.

### Edge cases / failure modes

- **WILL_INIT first record** — no base needed; seed a zero page, the record
  initializes it.
- **Multi-block record** — only the target is materialized, but every registered
  block gets a usable (scratch) buffer so init-then-deref handlers (btree split)
  don't crash; non-target results are discarded (see Buffer redirection).
- **A delta carries a newer FPI** — handled inside redo (`BLK_RESTORED`); start
  effectively re-bases there.
- **Page removed after the base** — `XLOG_SMGR_TRUNCATE` / drop below the
  fork size as-of `lsn` means "no page", not a stale replay (Driver step 0). But a
  truncate-then-re-extend before `lsn` makes the block live again — materialize it
  from the post-truncation chain, don't treat the truncation as terminal.
- **Branched read ordering** — the per-page index returns child-then-parent LSNs,
  not globally ascending; merge/sort across ancestry *and* fetch each record from
  its own source timeline before replay (Driver steps 2–3).
- **Record spans a WAL page** — its end LSN ≠ start + byte length; the driver's
  `XLogReader` decode supplies the real `EndRecPtr` in `APPLY` (don't infer it).
- **Concurrent multi-buffer redo** — a record holding several registered buffers
  live at once (btree split) needs the fake-buffer *table*, not one slot.
- **WAL-index chain overflow** — more indexed records than one IPC response holds
  must paginate or error, never silently truncate to an older page (Driver step
  2).
- **target_lsn must be a record boundary** (a page LSN) — same constraint as the
  read plan's half-open delta range.
- **Unretrievable / short WAL** (from the store WAL service) — fail the
  materialization (NULL/error); the page stays un-materialized rather than wrong.
- **wal-redo process crash/timeout** — driver sees pipe EOF, retries on a fresh
  process; state is per-page so restart is safe.

### Test plan

- Unit: write rows to a table, index its WAL (3c-2), assert
  `pagestore_redo_page_asof(block, lsn)` equals a direct read of that page as-of
  `lsn`, for several LSNs across multiple updates.
- WILL_INIT: a freshly-created page materializes from zero.
- Multi-block: a btree split / heap update — target page correct, neighbor
  untouched (and the split's right-sibling init does not crash the helper).
- VM/FSM: a heap change that sets all-visible / updates free space materializes
  the heap page without touching real VM/FSM storage.
- Truncation: truncate a relation below a block after its base image, then
  `redo_page_asof` for that block returns "no page", not the stale pre-truncation
  page; and a truncate-then-re-extend before the target LSN materializes the
  re-created block from its post-truncation chain.
- Branched ordering: a page modified on both a parent and its child branch
  materializes with parent WAL applied before child WAL, each record fetched from
  its own timeline's WAL (merge/sort + source-timeline), matching a full branch
  recovery.
- Page-spanning record: a delta large enough to cross a WAL page boundary
  materializes correctly (the decoded `EndRecPtr`, not start+len, drives gating).
- Hot page: more indexed records for one page than fit in a single index response
  still replays the full chain (pagination), or fails loudly — never silently
  returns an older page.
- WAL bytes come from the store: run the materializer on a compute with empty
  local `pg_wal` (WAL-only/stateless) and confirm it still reconstructs the page.
- Integration: extend `redo_worker_demo.sh` / `wal_only_redo_demo.sh` to assert
  per-page materialization matches full-recovery materialization.

### What 3c-4 unblocks

The store can then serve any page as-of any LSN from WAL alone (background or
on-demand), completing the per-page read path.  With 3d-2/3 (SLRU/clog + branch
WAL read-through) it removes the single-compute-per-branch boundary.

## 4b implementation design — APPLY (rm_redo against the held page)

Status of the parts built so far: the per-page `(timeline,lsn)` index (#34), the
truncation floor + LSN (#35), the base-image end LSN (#36), the `postgres
--wal-redo` process + `BEGIN/PUSHBASE/APPLY/GET` protocol skeleton (#37, APPLY
stubbed), and the driver's block-liveness step (#38) are all in.  4b is the
`APPLY` body and the materializing driver that drives it.

**Base-only is correct; the base record is *not* re-applied.** A WAL full-page
image is the page *as modified by* its record -- it is captured at XLogInsert,
after the change, and recovery installs it via `RestoreBlockImage` and returns
`BLK_RESTORED`, so the delta for that record is skipped.  Therefore the 3c-3 base
image already includes the base record's change and *is* the page as-of
`base_end_lsn`.  4b applies only the records strictly after the base
(`[base_end_lsn, lsn)` by start LSN -- the base record's start is < base_end_lsn,
so it is excluded); if that set is empty the base image is returned unchanged.
Re-applying the base record would double-apply it.

### Helper init (the part that needs a real build/run loop)

`rm_redo` handlers fetch the page they mutate through `XLogReadBufferForRedo` ->
`XLogReadBufferForRedoExtended` -> `XLogReadBufferExtended`, and then use the
returned `Buffer` through the normal buffer API, so the wal-redo process needs
*some* backend context -- but deliberately the minimum:

- **Resource managers must be started, not just looked up.** `RmgrTable` is static,
  but several page rmgrs (btree, GIN, GiST, SP-GiST) set up recovery memory
  contexts in their `rm_startup` and switch to them in redo, so the helper must run
  `RmgrStartup()` at init and `RmgrCleanup()` at shutdown, not only index the
  table.  No catalog access is needed for redo.
- **A real (tiny) buffer pool, not a bare table.** Redirecting only
  `XLogReadBufferExtended` is not enough: handlers pass the returned `Buffer` to
  `BufferGetPage`, `BufferGetPageSize`, `MarkBufferDirty`, `UnlockReleaseBuffer`,
  etc.  So the held/scratch pages must be backed by genuine buffer descriptors the
  buffer API accepts.  The cheapest correct route is to initialise a minimal shared
  buffer pool (a handful of buffers) and, under an `am_walredo` flag, satisfy
  `XLogReadBufferExtended` from it without going to smgr: the target block id ->
  the held page's buffer; every other block id a record registers -> a scratch
  buffer reported `BLK_DONE` (pd_lsn >= record EndRecPtr) unless the block is
  `WILL_INIT` (a zeroed scratch the handler initialises).  This still avoids a data
  directory / catalogs, but uses the real `BufferManager` so the buffer API works.
- VM/FSM side effects (`visibilitymap_pin`/`clear`, `XLogRecordPageWithFreeSpace`)
  are no-ops under `am_walredo` when the target fork is the heap; VM/FSM-fork
  materialization is the deferred 3c-4b follow-up.

This minimal-but-real init is exactly the piece that cannot be developed blind: it
must be iterated against `initdb` + a running `postgres` generating real WAL.

### APPLY body

For each `APPLY(start_lsn, end_lsn, bytes)`:

1. Wrap the record bytes in an `XLogReaderState` (a memory-backed `page_read`, or
   `DecodeXLogRecord` directly) with `EndRecPtr = end_lsn` (the gating LSN; the
   driver supplies it because a record can span a WAL page -- see 3c-4 design).
2. `RmgrTable[XLogRecGetRmid(reader)].rm_redo(reader)` -- the buffer reads land on
   the held/scratch buffers via the redirection above; the handler stamps the
   held page's pd_lsn to `end_lsn`.
3. `GET` returns the held page bytes (pd_lsn correct, pd_checksum left to the
   caller, per 4a).

### Materializing driver (slice 5b, contrib backend)

`redo_page_asof(timeline, key, fork, block, lsn) -> page`:

1. liveness (#38) -- dead block -> no page.
2. base + base_end_lsn (#36) -- or a zero page if the first record `WILL_INIT`s.
3. deltas = the per-page index records in `[base_end_lsn, lsn)`, merged ascending
   across the timeline ancestry and tagged with their source timeline (#34); each
   record's bytes fetched from *its* timeline's WAL (`PS_OP_WAL_READ`).
4. if there are no deltas, the base image is already the page as-of lsn -- return
   it directly (no helper round trip).  Otherwise spawn (or reuse a pooled)
   `postgres --wal-redo`; `BEGIN`; `PUSHBASE(base_end_lsn, base)`; `APPLY` each
   delta; `GET`.
5. recompute `pd_checksum` (the backend has cluster context + the block), then
   return / write the page (`WRITEV`/`ADD_LAYER`).

### Verification plan (running cluster)

`initdb`; create+modify a table so its block gets an FPI then later deltas; index
the WAL (3c-2); assert `redo_page_asof(block, lsn)` equals a direct read of that
page as-of `lsn`, for several LSNs.  Multi-block (btree split), WILL_INIT, and
branched-replay cases as in the 3c-4 test plan.  Extends `redo_worker_demo.sh`.

## Building and testing the wal-redo helper

The `postgres --wal-redo` helper (`src/backend/postmaster/walredo.c`) is plain
backend code: it links into the `postgres` binary and needs no contrib module.
The `postgres` target alone is enough to exercise the helper; the 4b real-WAL test
below also needs a runnable cluster + WAL tools, so build those too (a full
`ninja` + `tmp_install` is simplest -- it provides `initdb`/`pg_ctl`/`psql`/
`pg_waldump`; the contrib can be skipped per the note below if the smgr-export
patch is absent):

    meson setup BUILD -Dcassert=true
    ninja -C BUILD src/backend/postgres        # just the --wal-redo mode
    meson test -C BUILD --suite setup          # builds all + tmp_install (cluster + tools)

**4a (protocol) is verified** by driving the exact wire format on stdin/stdout.
Integer fields are in **host/native byte order** -- the helper copies raw bytes
(`read_u32`/`read_u64`) with no byte-order conversion, since helper and driver run
on the same host.  The helper exits on EOF and replies `BLCKSZ` bytes to `GET`:

    'b' BEGIN     5x uint32: spcOid, dbOid, relNumber, forkNum, blockNum
    'p' PUSHBASE  uint64 base_end_lsn, uint32 len, then len page bytes (len = BLCKSZ or 0)
    'g' GET       (no payload)  -> replies BLCKSZ bytes

`BEGIN` + `PUSHBASE`(base_end_lsn, BLCKSZ, page) + `GET` returns the page with
`pd_lsn` stamped to base_end_lsn and the body intact.

**4b (APPLY) is testable the same way against real WAL**, with two requirements:
the example must produce an actual *delta after* the base (the driver excludes the
base record -- its FPI already includes that change), and the page compared against
must be flushed to disk first.  A reliable sequence:

    initdb;  CREATE TABLE t(...);  INSERT ...;   -- create + populate the row first
    CHECKPOINT;                                  -- so the next change logs an FPI
    UPDATE t ...;                                -- record A: carries the block's FPI
    UPDATE t ...;                                -- record B: the delta to apply
    CHECKPOINT;                                  -- flush the dirty heap buffer to disk

Then `PUSHBASE`(record A's end LSN, record A's FPI page) + `APPLY`(record B) and
compare `GET` to the heap block read from the relation file (now flushed).
(Feeding record A itself to APPLY would re-apply the base; reading the file without
the final CHECKPOINT can compare against stale, not-yet-written bytes.)

### Core-integration prerequisite for the PG module (important)

The freestanding daemon (`pagestore_core.c`) and the helper (`walredo.c`) build
anywhere, but the **contrib PG module** (`pagestore.c` -- the smgr shim and the
`pagestore_*` SQL functions, including the redo driver `redo_page_asof`) requires
the core "export `f_smgr` / `smgr_register`" patch in `storage/smgr.h`.  That patch
lives on the per-version integration branch (`branchdb_18`), **not** on the
`pagestore` line the redo feature branches were stacked on, so on those branches
`pagestore.c` does not compile (`unknown type name 'f_smgr'`).  CI did not catch
this because it only builds the freestanding daemon, never the PG module.

Consequence: the redo driver + the SQL-level pieces (#36 `redo_base_lsn`, #38
`block_live`, and the future `redo_page_asof`) must be built and tested on a base
that carries the smgr-export patch (i.e. integrated onto `branchdb_18`).  To build
a runnable cluster from a redo-feature branch without that patch, the pagestore
contrib can be skipped (`# subdir('pagestore')` in `contrib/meson.build`); the
`postgres` binary (and thus the wal-redo helper) is unaffected.

## Known scope boundaries

Until 3c/3d land, branches remain single-compute-at-a-time and WAL/SLRU/control
are not fully branched (see compute_on_branch_demo.sh).  The redo worker is the
piece that removes those boundaries.
