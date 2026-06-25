# SLRU on the store: lifecycle design (M4)

Status: design. The PR-#49 prototype (`feat/pagestore-slru`) mirrors and serves
SLRU pages through the `klass` storage seam, but review showed the gaps are not
local bugs -- they span the whole SLRU object lifecycle. Per the read-path stack
plan, store-backed SLRU stays frozen until that lifecycle is defined here. This
document is the gate; implementation resumes against it.

## Scope

SLRUs are the small fixed-page logs PostgreSQL keeps under `pg_xact`,
`pg_multixact/{offsets,members}`, `pg_subtrans`, `pg_commit_ts`, `pg_serial`,
and `pg_notify`. Putting them on the store lets a branch compute read shared
transaction-status state (clog, commit-ts, multixact) it has no local copy of,
and lets a writer's SLRU updates become visible to other computes of the same
branch.

**M4 covers only the WAL-logged, branch-relevant SLRUs: `pg_xact` (clog),
`pg_multixact/{offsets,members}`, and `pg_commit_ts`.** The rest are explicitly
out of scope for M4, for two concrete reasons surfaced in review:

- **Non-WAL truncation.** `pg_subtrans`, `pg_notify`, and the serializable SLRU
  (`pg_serial`) truncate during checkpoint/cleanup with **no truncate WAL record**,
  so there is no LSN to version a tombstone with (see truncation). The in-scope
  three all truncate via WAL (`XLOG_*_TRUNCATE`-style records), giving a real LSN.
- **Page-number width.** `SimpleLru` page numbers are `int64`, and `pg_notify`'s
  queue page is unbounded `int64`, but the pagestore object `block` key is
  `uint32`. The in-scope three have page numbers bounded by the 32-bit xid /
  multixact-id space, so they fit `uint32`; `pg_notify` does not. Supporting
  int64-pageno SLRUs requires widening the object key first (out of scope here).

Also out of scope for M4: these are transient/derivable (`pg_subtrans` is rebuilt,
`pg_notify`/`pg_serial` are not needed to read a branch's transaction status).

In scope: identity/versioning, the write/mirror protocol, truncation, existence
probes, the read path and cache coherence, and interrupt/lock safety. Out of
scope: cross-branch SLRU retention/GC (M5), SLRU-specific compaction, and the
non-WAL/int64 SLRUs above (need a per-SLRU truncate-version source and a wider
object key before they can be added).

## Why the prototype is not mergeable

The prototype installs two hooks in `slru.c` -- a write hook after
`SlruPhysicalWritePage()` and a read hook in `SlruPhysicalReadPage()` -- and
mirrors/serves each page as a `PS_KLASS_SLRU` object keyed by
`(slru_klass_id(Dir), pageno)`. The hooks run **with the SLRU bank lock held**,
and the write hook can fire **inside a transaction commit/abort critical
section** (an eviction via `SlruSelectLRUPage`). From there it does fallible
shared-memory I/O against the daemon. The review findings (15 P1 / 11 P2) reduce
to seven lifecycle questions the prototype answers unsafely or not at all:

1. **Critical-section writes.** The hook returns early when `CritSectionCount > 0`
   so it never PANICs -- but then the page is silently *not* mirrored, and there
   is no guarantee a later non-critical write ever re-mirrors it. The store goes
   permanently stale for that page (e.g. an evicted `pg_xact` page during commit).
2. **Mirror fidelity.** The hook must mirror exactly the image that was written
   to the local segment, snapshotted under the bank lock -- not whatever the
   shared buffer holds when the (possibly deferred) mirror later runs.
3. **Version ordering.** SLRU pages have no `pd_lsn`; their first bytes are
   payload. Versions must come from a real monotonic order, not page bytes, or an
   overwrite can compare lower than the prior version and be lost.
4. **Truncation / tombstones.** `SimpleLruTruncate()` drops segments. Without a
   durable tombstone on the store, a store read can resurrect a truncated page.
5. **Existence probes.** `SimpleLruDoesPhysicalPageExist()` is consulted *before*
   the read hook by commit-ts activation and multixact offset reads
   (`commit_ts.c`, `multixact.c`). With local segments absent it reports "missing"
   and the caller zero-initializes -- the store is never consulted.
6. **Read cache coherence.** A compute may hold a cached local SLRU page in a
   shared buffer; serving an *older* store page, or trusting a stale local page
   over a newer store version, both corrupt transaction state.
7. **Interrupt / lock safety.** The read hook runs after the slot is marked
   `SLRU_PAGE_READ_IN_PROGRESS` and the bank lock is released; a `PG_RE_THROW()`
   of a query-cancel exits before `slru.c` marks the slot valid/empty, wedging
   every later reader of that slot. The write hook rethrows while the bank lock is
   still held.

## SLRU object model on the store

- **Identity.** `PsKey{ klass = PS_KLASS_SLRU, relNumber = slru_klass_id(Dir),
  block = pageno }`, `spcOid = dbOid = forkNum = 0`. `slru_klass_id(Dir)` maps a
  stable small integer per SLRU directory (`pg_xact` = 1, `pg_multixact/offsets`
  = 2, ...). The mapping is fixed and versioned; adding an SLRU appends an id.
- **Version order (branch-aware), captured at dirty time.** The store resolves
  branch reads by walking timeline ancestry and capping parent versions at the
  branch-creation LSN (`read_through`). An SLRU object version must be comparable to
  that cap, so the daemon-local `max+1` counter the relation prototype uses is
  **wrong for objects**: a parent SLRU write after a branch is created still gets a
  small counter value, compares below the branch LSN, and leaks post-branch parent
  status into the branch. The version is therefore the **WAL LSN of the change that
  dirtied the page** -- recorded **per slot when the buffer is dirtied**, not the
  LSN read at physical-write time. Physical writes happen later at
  checkpoint/eviction, where there is no meaningful WAL LSN for the logical change;
  capturing a flush-time LSN would tag pre-branch contents with a post-branch LSN
  and hide data the branch should see. For clog the dirtying LSN is the commit
  record's LSN (`TransactionIdSetTreeStatus`'s `lsn`, already plumbed to
  `SlruSharedData.group_lsn` for clog); SLRUs without group-lsn tracking need an
  equivalent per-page "dirtied at" LSN added. The daemon stores the supplied LSN
  verbatim (no daemon counter for objects); it is cluster-global, monotonic,
  globally ordered across backends, and is the branch read cutoff.
- **Read returns the version.** `obj_read` / `PS_OP_READ_AT` currently return only
  page bytes plus status/result; they must also return the resolved object version
  (LSN) so the read hook can compare it against a cached local page (see cache
  coherence). This is a required IPC addition, not optional.
- **Granularity.** One object block per SLRU page (`BLCKSZ`). A segment's worth of
  pages is many objects; there is no segment-level object.

## Write / mirror protocol

The core decision: **mirroring is deferred, never synchronous from inside a
critical section.** A synchronous mirror cannot be made both crash-safe and
PANIC-safe in a commit critical section, so it is removed from the hot write path.

### Deferred mirror queue (shared memory)

- The write hook's only job under the bank lock is to **stage** the dirty page into
  a **shared-memory** queue: the page image, `(slru_klass_id, pageno)`, and the
  page's **dirtied-at LSN** (the per-slot version above) as the object version.
  - **Snapshot before releasing the lock.** `SlruInternalWritePage()` drops the
    bank lock while `pg_pwrite()` reads the shared buffer, and a write-OK caller may
    update the same page meanwhile, so an image copied *after* the physical write
    can differ from what was written. Copy the buffer into the staged entry **under
    the bank lock, before local I/O**, and use that frozen copy for the mirror (and,
    ideally, for the local write too) so the mirrored image matches the local one.
  - **Shared, not per-backend.** PostgreSQL backends are separate processes, so a
    private queue is invisible to the checkpointer and would never be drained for an
    idle backend; the queue lives in shared memory.
  - Staging is allocation-free and cannot fail in a way that breaks the SLRU write,
    so it is safe in a critical section.
- **Global order, last-writer-wins by LSN.** Because the version is the captured
  WAL LSN, two backends staging the same page and draining in either order are
  resolved correctly: the daemon keeps the highest-LSN version and **rejects a
  drained entry whose LSN is below the page's current stored version**. Ordering is
  by LSN, never by drain wall-clock or a per-process stamp (fixes the
  "keep mirrors ordered across deferred drains" finding).
- **Drain points.** Any backend or the checkpointer ships queued images via
  `obj_write` from outside any critical section: at (a) the next non-critical SLRU
  access and (b) `SimpleLruWriteAll()` / checkpoint. Because the queue is shared
  and entries carry their LSN, the checkpointer drains entries staged by an
  idle backend, bounding staleness regardless of which process flushes.
- **No fallible post-commit drain.** There is deliberately *no* "drain before the
  commit returns to the client" point: a page dirtied inside the commit critical
  section can only be drained after the transaction is already durable locally, so
  an `obj_write` failure there is too late to abort and must not throw. The
  durability contract is instead anchored at the checkpoint barrier (below); a
  backend that needs durable-on-commit uses `synchronous` mode, which drains
  **before** the commit decision, where an error can still abort.
- **Overflow.** If the queue fills (a long critical section dirtying many pages),
  staging records a "dirty SLRU range" watermark instead of dropping pages; the
  drain re-reads those pages from the local segment and re-stages them with their
  recorded LSN. The store is never left believing it is current when a page was
  dropped.

### Durability vs. cross-compute visibility

Two different guarantees must not be conflated. **Durability** (the mirror is on the
store) is anchored at the checkpoint barrier. **Visibility** (another compute on the
branch may treat a commit as committed) is a *separate, stronger* requirement that
the deferred queue alone does not provide.

- **Deferred is durable-enough but not visible-enough on its own.** A writer can
  commit (clog dirtied at the commit LSN) and leave that `pg_xact` page merely
  staged until the next checkpoint. A *different* compute on the same branch, which
  never had the dirty page, can read the store in that window and see the old
  status. Local-slot revalidation cannot help a process that never cached the page.
  So checkpoint-delayed mirroring is fine for crash durability but **wrong for live
  read-sharing of transaction status**.
- **Commit-visibility watermark.** Cross-compute visibility is gated, not the local
  commit. The writer publishes a monotonic **`mirrored_status_lsn`** watermark =
  the highest commit LSN whose clog/multixact/commit-ts mirrors are durable on the
  store. A reader compute must **not treat a commit at LSN `L` as visible until the
  branch's `mirrored_status_lsn >= L`** (it waits, or treats the xid as
  in-progress). This keeps the local transaction genuinely committed while making
  its *visibility elsewhere* depend on the mirror -- so there is no "lie about a
  committed transaction": the local commit is never held back or aborted.
- **Where the mirror happens for clog.** The committable status image is produced by
  `TransactionIdCommitTree()` **after** the commit record is flushed, inside the
  commit critical section -- a daemon failure there cannot abort the transaction. So
  the mirror cannot gate the *local* commit. It instead advances
  `mirrored_status_lsn` *after* the page is shipped (from the deferred drain or a
  status shipper), and readers gate on that watermark. "Synchronous mode" therefore
  does **not** mean "abort the commit if the mirror fails" for clog; it means
  "advance the visibility watermark only once durable", retrying on failure.

| Guarantee | Mechanism | Use |
|-----------|-----------|-----|
| crash durability | checkpoint barrier drains all pre-checkpoint pages | restart/failover |
| live read-sharing | `mirrored_status_lsn` watermark gates cross-compute visibility | another compute reads this branch's xact status |

The prototype's `CritSectionCount > 0 -> return` becomes `CritSectionCount > 0 ->
stage only`; the actual `obj_write` always runs outside critical sections.

## Truncation / tombstone semantics

`SimpleLruTruncate()` and segment deletion must be represented on the store so a
store read cannot resurrect a truncated page (findings: "Mirror SLRU truncations
before serving store pages", "Do not resurrect truncated SLRU pages").

- A truncate installs a durable **tombstone** object for the truncated page range
  on the relevant timeline: a versioned record `(klass, pageno-range, truncated)`
  at the truncation's WAL LSN that the daemon honours in `read_resolve` -- a read
  whose cutoff is at/after the tombstone LSN returns "not present" for those pages
  even if an older image exists.
- **Requires a WAL-logged truncation LSN.** This versioning only works when the
  truncation has a WAL record to take the LSN from. The M4-scoped SLRUs (clog,
  multixact, commit-ts) truncate via WAL and qualify. SLRUs that truncate during
  checkpoint/cleanup without a WAL record (`pg_subtrans`, `pg_notify`,
  serializable) have no LSN to version the tombstone with -- they are out of scope
  (see Scope); adding them later needs a defined per-SLRU truncate-version source,
  not an invented LSN (which would break branch visibility).
- **Synchronous order-barrier, not deferred.** Unlike page mirrors, the tombstone
  must be **durable on the store before the local segments are deleted**. If it
  rode the deferred queue, there would be a window after `SimpleLruTruncate()`
  removes the local segment but before the tombstone is durable where another
  compute still reads the older store image -- exactly the resurrection this
  prevents. So truncation drains any pending mirrors for the range, writes the
  tombstone, waits for the daemon ack, and only then proceeds with local deletion.
- Ordering is by LSN: the tombstone carries the truncation LSN, so a re-extension
  (a new page write in a previously truncated range) at a higher LSN wins over the
  tombstone, and a stale page write at a lower LSN stays hidden. The daemon must
  apply tombstone and page versions in LSN order regardless of drain arrival order.

## Existence semantics

`SimpleLruDoesPhysicalPageExist()` must consult the same source of truth as reads
(findings: "Add an existence hook for SLRU probes", "Check the SLRU store before
declaring pages absent").

- Add a `slru_page_exists_hook` symmetric to the read hook, called from
  `SimpleLruDoesPhysicalPageExist()` *before* the local-segment stat. With
  `slru_read_from_store` on, it asks the daemon about the object block and returns
  a **tri-state**, not a bool:
  - **present** -- a live version exists; report exists, skip the local stat.
  - **tombstoned (negative)** -- the store has truncated this range; report
    **absent and suppress the local fallback**. A compute that still has an old
    local segment must not report the page present (fixes the "treat tombstones as
    negative cache entries" finding); the store's tombstone is authoritative.
  - **unknown/miss** -- the store has nothing for this page; fall through to the
    local stat.
  - **failed** -- a daemon error or cancel during the probe. For a store-required
    compute this must **fail closed** exactly like the read hook (below): collapsing
    it into unknown/miss would let `SimpleLruDoesPhysicalPageExist()` fall through to
    the local stat, report the page absent, and the callers (commit-ts activation,
    multixact offset) would zero-initialize real state. So the probe propagates a
    safe error rather than reporting absence on error.
- The probe shares the read path's found-ness contract: a block below fork length
  but never written is a miss (sparse), reported absent so the caller
  zero-initializes -- distinct from a tombstone, which actively forbids the page.

## Read path and cache coherence

- **Found-ness (both daemons).** `obj_read` uses `PS_OP_READ_AT` and must report
  presence in `ch->result`. **Neither merged daemon does this yet**: the POSIX
  `PS_OP_READ_AT` path initializes `ch->result = 0` and never sets it on a
  successful `read_resolve()`, and the SPDK path likewise leaves it 0. Both must be
  fixed before this contract holds (the POSIX fix is the first sequencing item, not
  a precondition assumed done), or every store read looks like a miss and SLRU
  falls back to local/zeros.
- **Version return.** The same `READ_AT` reply must carry the resolved version
  (LSN) so the read hook can do cache revalidation below.
- **Sparse blocks.** A block below the object's fork length but never written reads
  as a miss (sparse), never as a zero-filled "present" page.
- **Cache hits must be revalidated too.** The physical read hook only runs on a
  cache *miss*; `SimpleLruReadPage()` returns an already-valid cached slot without
  reaching it. A compute that cached an older `pg_xact`/multixact page would keep
  serving stale status indefinitely until eviction. So the cache-*hit* path needs a
  store-version check or an invalidation signal, not just the miss path. The design
  uses the branch-shared **`mirrored_status_lsn` watermark** (above) plus a per-slot
  "validated through LSN": a reader gates xid visibility on the watermark, and a
  cached SLRU slot whose validated-through LSN is behind the watermark is treated as
  stale (re-fetched through the hook) before its status is trusted. A coarse epoch
  bump on the watermark that forces revalidation of SLRU slots is the minimal
  mechanism; the alternative is invalidating affected slots when the watermark
  advances. Either way, a cache hit alone is not sufficient proof of currency for a
  store-reading compute.
- **Local cache revalidation (miss path).** On a miss, using the returned version,
  the read hook serves the store page only when its LSN is at least as new as the
  cached slot's known version, falls back to the buffered page when that is newer,
  and invalidates the cached page when the store is newer. A read-only compute (no
  local writer) never caches a newer page, so the common case is a plain store read.
- **Fail closed for store-required pages.** Returning `false` (fall back to local)
  on a store error is only safe when a valid local copy exists. A branch compute
  booted with no local SLRU has none, and PostgreSQL treats a missing local SLRU
  page during recovery as a **zero page** -- so a transient daemon error or cancel
  while reading `pg_xact`/multixact would silently turn real transaction status
  into zeros. When the page is store-required (no local segment / store-only mode),
  the hook must instead **fail closed**: clean up the slot and propagate a safe
  error, never let the core zero-fill. Distinguish the two modes explicitly:
  - **store-optional** (local copy present): error -> fall back to local read.
  - **store-required** (no local copy): error -> slot cleanup + error, never zero.

## Interrupt and lock safety

The hooks run inside `slru.c` with transitional lock/slot state held; they must
not let an interrupt escape before the core restores that state (findings: "Do not
rethrow while an SLRU read is in progress", "...while an SLRU write holds locks").

- **Read hook.** Invoked after the slot is `SLRU_PAGE_READ_IN_PROGRESS` and the
  bank lock is dropped. The hook must **never `PG_RE_THROW()`** out of this window
  directly -- a raw throw exits before `slru.c` resets the slot and wedges later
  readers. Instead the hook returns a small status to the core: `SERVED`,
  `FALLBACK` (store-optional miss/error -> read local), or `FAILED` (store-required
  error). `slru.c` is taught to restore the slot to a clean state (mark it
  empty/error and release the in-progress marker) for both `FALLBACK` and `FAILED`,
  and *then*, for `FAILED`, raise the error -- so the store-required fail-closed
  path (above) propagates a real error without ever leaving the slot transitional.
  This replaces the prototype's "swallow-but-rethrow-cancel" dance, which threw
  across the in-progress window.
- **Write hook.** With deferral, the hook only stages into the shared-memory queue
  and does no fallible daemon I/O under the bank lock, so it has nothing to throw.
  The actual `obj_write` happens at a drain point where normal error handling
  applies and no SLRU lock is held.
- **No swallowing of cancel/shutdown at unsafe points.** Because the fallible work
  moved out of the locked/critical window, the prototype's
  "swallow-but-rethrow-cancel" `PG_TRY/PG_CATCH` dance in the hooks is gone; there
  is no interrupt to mishandle while a lock is held.

## Sequencing

1. Scope: enable store-backing only for the WAL-logged, uint32-pageno SLRUs
   (clog, multixact, commit-ts); refuse the rest.
2. Daemon IPC: `READ_AT` reports found-ness in `ch->result` **and returns the
   resolved version (LSN)** -- on **both** POSIX and SPDK paths (neither does today).
3. Daemon: store object versions are writer-supplied LSNs (no daemon `max+1`
   counter for objects); `read_resolve` caps by the branch LSN using those.
4. Daemon: SLRU tombstone object + LSN-ordered `read_resolve` honouring it,
   including a negative (tombstoned) result for existence probes.
5. Core: per-slot **dirtied-at LSN** captured when the buffer is dirtied; image
   snapshotted under the bank lock; shared-memory mirror queue carries both.
6. Core: `mirrored_status_lsn` watermark; readers gate xid visibility on it;
   cache-hit revalidation against it.
7. Core: `slru_page_exists_hook` and read hook return
   `SERVED`/`FALLBACK`/`FAILED`; `slru.c` cleans the slot and fails closed for
   store-required pages (read and existence).
8. Module: drain points (checkpoint + next access), miss-path revalidation, sparse
   fallback; truncate -> synchronous tombstone barrier before local deletion.
9. Tests: the acceptance scenarios below.

## Acceptance criteria

- A committed xid's `pg_xact` page evicted *inside* the commit critical section is
  still mirrored (via the shared deferred queue, drained at checkpoint even if the
  writing backend went idle) and readable from the store on another compute.
- A parent SLRU write *after* a branch is created is **not** visible to the branch
  (the dirtied-at LSN compares above the branch cutoff), and a write before it is.
- The version reflects when the page was **dirtied**, not when it was flushed: a
  pre-branch change flushed by a post-branch checkpoint is still visible to the
  branch.
- A second compute on the branch does not observe a commit until the branch's
  `mirrored_status_lsn` reaches it -- no stale-status window, including for a
  cached SLRU slot it already held (cache-hit revalidation).
- Two backends mirroring the same page in either drain order converge to the
  higher-LSN image; a stale lower-LSN drain is rejected.
- A truncated SLRU range is reported absent by both the read hook and the existence
  hook, suppressing local fallback even when an old local segment is still present;
  a re-extension at a higher LSN supersedes the tombstone.
- `SimpleLruDoesPhysicalPageExist()`-first callers (commit-ts activation,
  multixact offset) see store-backed pages on a compute with no local segments.
- A sparse/unwritten page below fork length falls back to local (or reports absent)
  rather than serving zeros, on both POSIX and SPDK daemons.
- A store page never overrides a newer locally-cached page, and a stale local
  cache is invalidated when the store is newer (using the returned version).
- For a store-required page, a daemon error or query cancel during **either** the
  read hook or the existence probe **fails closed** (clean slot + error) -- it never
  turns transaction status into zeros -- and never wedges a later reader of that
  SLRU slot/bank.
- The mirror image matches the locally written bytes even when a write-OK caller
  updates the page during the physical write (snapshot-under-lock).
- A non-WAL-truncating / int64-pageno SLRU (e.g. `pg_notify`) is refused rather
  than silently store-backed with an undefined tombstone version or an aliased key.

## Open questions

- Tombstone granularity: per-page vs per-segment range, and how it interacts with
  M3 layer/manifest persistence.
- The exact cache-hit revalidation mechanism: a coarse `mirrored_status_lsn`-epoch
  bump that forces SLRU slot revalidation vs. targeted slot invalidation.
- Where `mirrored_status_lsn` is advanced (deferred-drain completion vs. a dedicated
  status shipper) and how a reader waits on it without livelock.
- A per-SLRU truncate-version source + a 64-bit object key, to admit the non-WAL /
  int64 SLRUs (`pg_subtrans`, `pg_notify`, serializable) post-M4.

See also: the read-path PR stack plan and the branch-PostgreSQL milestone plan
(M4: bootable branch compute), and [PGCONTROL_ON_STORE_DESIGN.md] for the sibling
pg_control protocol.
