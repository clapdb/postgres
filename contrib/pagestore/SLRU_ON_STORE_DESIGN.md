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

In scope: identity/versioning, the write/mirror protocol, truncation, existence
probes, the read path and cache coherence, and interrupt/lock safety. Out of
scope: cross-branch SLRU retention/GC (M5) and any SLRU-specific compaction.

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
- **Version order.** Each object write carries a monotonic version assigned by the
  daemon as `max(existing versions across ancestry) + 1` (the existing
  non-relation path in `append_page`). The compute never derives an SLRU version
  from page bytes. This already satisfies finding (3); the design keeps it.
- **Granularity.** One object block per SLRU page (`BLCKSZ`). A segment's worth of
  pages is many objects; there is no segment-level object.

## Write / mirror protocol

The core decision: **mirroring is deferred, never synchronous from inside a
critical section.** A synchronous mirror cannot be made both crash-safe and
PANIC-safe in a commit critical section, so it is removed from the hot write path.

### Deferred mirror queue

- The write hook's only job under the bank lock is to **stage** the dirty page: it
  copies the just-written page image (finding 2: snapshot under the lock) plus
  `(slru_klass_id, pageno)` and a monotonically increasing local stamp into a
  bounded in-process queue. Staging is allocation-free and lock-free against the
  daemon; it cannot fail in a way that breaks the SLRU write, and it is safe in a
  critical section.
- A **drain point** outside any critical section ships the queued images via
  `obj_write`. Drain runs at: (a) the next non-critical SLRU access, (b)
  `SimpleLruWriteAll()` / checkpoint, and (c) before transaction commit returns to
  the client. Whichever comes first flushes the page; (b) bounds staleness even
  for an idle backend, because the checkpointer's `SimpleLruWriteAll` drains.
- **Durability handoff.** A commit is not acknowledged as durable-on-store until
  the queue entries for SLRU pages it dirtied have been shipped and the daemon has
  acked. For correctness of branch *reads* the weaker contract is enough: a store
  read must never return an image older than what a reader on the writing compute
  would see locally; the drain-at-checkpoint plus read-side revalidation (below)
  provides that. Strict durable-on-commit is an opt-in (`synchronous` mode) for
  computes that must fail over without replaying local WAL.
- **Overflow.** If the queue fills (a long critical section dirtying many pages),
  staging records a "dirty SLRU range" watermark instead of dropping pages; the
  drain re-reads those pages from the local segment and ships them. The store is
  never left believing it is current when a page was dropped (fixes finding 1).

### Mode summary

| Mode | When mirror ships | Durability on store | Use |
|------|-------------------|---------------------|-----|
| deferred (default) | checkpoint / next access / pre-commit | bounded staleness | branch read-sharing |
| synchronous | before commit ack | durable-on-commit | failover without local WAL replay |

The prototype's `CritSectionCount > 0 -> return` becomes `CritSectionCount > 0 ->
stage only`; the actual `obj_write` always runs outside critical sections.

## Truncation / tombstone semantics

`SimpleLruTruncate()` and segment deletion must be represented on the store so a
store read cannot resurrect a truncated page (findings: "Mirror SLRU truncations
before serving store pages", "Do not resurrect truncated SLRU pages").

- A truncate installs a durable **tombstone** object for the truncated page range
  on the relevant timeline: a versioned record `(klass, pageno-range, truncated)`
  that the daemon honours in `read_resolve` -- a read at/after the tombstone's
  version returns "not present" for those pages even if an older image exists.
- Truncation is logged (it follows WAL) so the tombstone write can be deferred to
  the same drain mechanism, ordered after the page writes it supersedes by the
  monotonic version. Ordering is by version, not wall-clock.
- A re-extension (new page in a previously truncated range) is just a newer
  version above the tombstone and wins normally.

## Existence semantics

`SimpleLruDoesPhysicalPageExist()` must consult the same source of truth as reads
(findings: "Add an existence hook for SLRU probes", "Check the SLRU store before
declaring pages absent").

- Add a `slru_page_exists_hook` symmetric to the read hook, called from
  `SimpleLruDoesPhysicalPageExist()` *before* the local-segment stat. With
  `slru_read_from_store` on, it asks the daemon whether the object block has a
  live (non-tombstoned) version and returns existence accordingly; on a miss it
  falls through to the local check.
- The probe shares the read path's found-ness contract: it must distinguish "has a
  stored version" from "below fork length but never written" so a sparse page is
  reported absent (so the caller zero-initializes) rather than present-as-zeros.

## Read path and cache coherence

- **Found-ness.** `obj_read` uses `PS_OP_READ_AT` and reports presence in
  `ch->result` (already fixed for the POSIX daemon). The **SPDK daemon must set the
  same `ch->result` on its `READ_AT` path** before this is relied on (P2 finding),
  or store-backed SLRU silently misses on SPDK. This is a precondition for serving
  SLRU from an SPDK-backed store.
- **Sparse blocks.** A block below the object's fork length but never written must
  read as a miss (fall back to local), never as a zero-filled "present" page.
- **Local cache revalidation.** A compute with `slru_read_from_store` on may also
  hold the page in an SLRU shared buffer. The design serves the store version only
  when it is at least as new as any cached local copy: the read hook compares the
  store version against the buffered page's known version (tracked per slot) and
  falls back to the local/buffered page when it is newer, and invalidates the
  cached page when the store is newer. A compute that only reads from the store
  (no local writer) never caches a newer page, so the common case is a plain
  store read.

## Interrupt and lock safety

The hooks run inside `slru.c` with transitional lock/slot state held; they must
not let an interrupt escape before the core restores that state (findings: "Do not
rethrow while an SLRU read is in progress", "...while an SLRU write holds locks").

- **Read hook.** Invoked after the slot is `SLRU_PAGE_READ_IN_PROGRESS` and the
  bank lock is dropped. The hook must **never `PG_RE_THROW()`** out of this window.
  On any error -- including query-cancel -- it returns `false` (fall back to local
  read) and lets `slru.c` finish the slot transition; the pending interrupt is
  serviced at the next `CHECK_FOR_INTERRUPTS()` after the slot is consistent.
  Equivalently, `slru.c` can be taught to run the hook before marking the slot and
  to clean up the slot if the hook signals failure -- but the simpler rule is
  "hook never throws across the in-progress window".
- **Write hook.** With deferral, the hook only stages into the in-process queue
  and does no fallible daemon I/O under the bank lock, so it has nothing to throw.
  The actual `obj_write` happens at a drain point where normal error handling
  applies and no SLRU lock is held.
- **No swallowing of cancel/shutdown at unsafe points.** Because the fallible work
  moved out of the locked/critical window, the prototype's
  "swallow-but-rethrow-cancel" `PG_TRY/PG_CATCH` dance in the hooks is gone; there
  is no interrupt to mishandle while a lock is held.

## Sequencing

1. Daemon: SLRU tombstone object + version-ordered `read_resolve` honouring it.
2. Daemon: SPDK `READ_AT` found-ness parity (precondition for serving SLRU on SPDK).
3. Core: `slru_page_exists_hook` from `SimpleLruDoesPhysicalPageExist()`.
4. Core/module: deferred mirror queue + drain points; write hook stages only.
5. Module: read-hook cache revalidation + sparse-block fallback.
6. Module: truncate -> tombstone wiring through the drain.
7. Tests: the acceptance scenarios below.

## Acceptance criteria

- A committed xid's `pg_xact` page evicted *inside* the commit critical section is
  still mirrored (via deferral) and readable from the store on another compute.
- A truncated SLRU page is reported absent by both the read hook and the existence
  hook; it is not resurfaced from an older store image.
- `SimpleLruDoesPhysicalPageExist()`-first callers (commit-ts activation,
  multixact offset) see store-backed pages on a compute with no local segments.
- A sparse/unwritten page below fork length falls back to local rather than
  serving zeros, on both POSIX and SPDK daemons.
- A store page never overrides a newer locally-cached page, and a stale local
  cache is invalidated when the store is newer.
- A query cancel during a store-backed SLRU read or a mirror write never wedges a
  later reader of that SLRU slot/bank.

## Open questions

- Tombstone granularity: per-page vs per-segment range, and how it interacts with
  M3 layer/manifest persistence.
- Whether the synchronous (durable-on-commit) mode is needed for the first bootable
  branch (M4) or can wait until failover work.
- Per-slot store-version tracking cost vs a coarser epoch-based invalidation.

See also: the read-path PR stack plan and the branch-PostgreSQL milestone plan
(M4: bootable branch compute), and [PGCONTROL_ON_STORE_DESIGN.md] for the sibling
pg_control protocol.
