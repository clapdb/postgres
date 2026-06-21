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
         (`RestoreBlockImage`) -- rebuilding one page from WAL on demand.  Note a
         WAL full-page image is the page *before* that record's change (torn-page
         protection), so this returns the base image, not the page exactly as-of
         lsn.
       - **3c-4** The `--wal-redo`-style helper: apply the delta records after
         the base image with `rm_redo` to get the page exactly as-of lsn.  This
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
single held page for the target `(rlocator, forknum, blkno)` (seeded to the base
image with its LSN = base_lsn so the gating applies exactly the records in
`(base_lsn, target_lsn]`), and return `BLK_NOTFOUND` for any *other* block a
record touches (multi-block records: heap update, btree split, …) so the handler
simply skips those — correct, because each page is materialized independently.
Doing this inside a normal backend is unsafe: the handlers assume a recovery-ish
context, and they would read/dirty the *real* shared buffers.

### Approach: a sandboxed `postgres --wal-redo` process (Neon model)

A dedicated, short-lived process that holds exactly one page and applies records
to it — the approach the doc has pointed at throughout.  Chosen over an in-backend
hack for correctness (isolation from shared buffers / concurrency) and security
(it only ever sees a base page + record bytes; it can be seccomp-sandboxed, so
decoding attacker-influenced WAL cannot touch the system).

**Process**: a new `--wal-redo` startup mode (bootstrap-like: no postmaster, no
catalog access), holding one `BLCKSZ` slot as its entire "buffer pool", driven by
a length-prefixed binary protocol on a pipe pair:

| msg | payload | effect |
|-----|---------|--------|
| `BEGIN` | rlocator, forknum, blkno | set the target page identity |
| `PUSHBASE` | page bytes \| (none = zero) | seed the held page (base FPI, or zero for a WILL_INIT first record) |
| `APPLY` | record LSN, raw WAL record bytes | decode + `rm_redo` the record onto the held page |
| `GET` | — | return the current page bytes (the as-of result) |

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
- **Side effects beyond the buffer read must also be stubbed.** Redo handlers
  reach auxiliary storage outside `XLogReadBufferExtended`: heap redo calls
  `visibilitymap_pin`/`visibilitymap_clear` and `XLogRecordPageWithFreeSpace`
  when VM/FSM bits change.  Under `am_walredo` these must be no-ops (or serve
  scratch buffers) so single-page redo never reads or dirties real VM/FSM
  storage.  The patch enumerates every such call reachable from `rm_redo` and
  guards it — this is the bulk of the "small core mode" work (Neon's wal-redo
  does the same).

Mechanism (wal-redo-specific tiny smgr vs guarded branches in the read path) is a
patch detail; the smgr route keeps core least-touched, but the VM/FSM guards are
needed either way.

### Driver side (normal backend, contrib/pagestore)

`pagestore_redo_page_asof(rel, fork, block, lsn) -> bytea`:

0. **Truncation/drop check first.** `walidx_get` only knows records that register
   *this* block, so it misses page-removing records that carry just a relation
   locator — `XLOG_SMGR_TRUNCATE` (new block count), relation drop.  Before
   replaying anything, consult fork-size / truncation / drop metadata as-of `lsn`:
   if the relation was truncated below `block` (or dropped) after the base, the
   page is gone — return "no page", never a stale replay.  (This requires the
   store to track per-fork size / truncation LSNs; called out as a dependency.)
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
   - **Chain completeness is mandatory.** A single `walidx_get` response is capped
     by the IPC payload (`PS_IO_UNIT / sizeof(uint64)`) with no continuation, so a
     hot page with more records than fit would be silently shortened and
     materialize an *older* page while reporting success.  The lookup must paginate
     / range-seek (cursor by start LSN) or return an explicit overflow error;
     truncation is never silently accepted.
3. open (or reuse a pooled) wal-redo process; `BEGIN(target)`; `PUSHBASE(base,
   base_end_lsn)`; for each delta **read the raw record bytes from the store's WAL
   service** (`PS_OP_WAL_READ`, the same source `pagestore_walrestore` uses) — not
   a local `XLogReader`, because the target stateless/WAL-only compute has an empty
   local `pg_wal`; the bytes live in the store.  `APPLY` each; `GET`.
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
- **Page removed after the base** — `XLOG_SMGR_TRUNCATE` / drop below `block`
  means "no page", not a stale replay; caught by Driver step 0, which needs
  per-fork size / truncation metadata in the store.
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
  page.
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

## Known scope boundaries

Until 3c/3d land, branches remain single-compute-at-a-time and WAL/SLRU/control
are not fully branched (see compute_on_branch_demo.sh).  The redo worker is the
piece that removes those boundaries.
