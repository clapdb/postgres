# SLRU on the store: lifecycle design (M4)

Status: design. The PR-#49 prototype (`feat/pagestore-slru`) mirrors flushed SLRU
page images to the store and serves them back through a read hook. Three rounds of
review showed this model is the wrong primitive for *branch-correct* transaction
status, so the M4 design pivots to **WAL-based as-of reconstruction** and defers
live page mirroring. This document is the gate; implementation resumes against it.

## What M4 actually needs

M4 is a bootable branch compute. The milestone goal for SLRU is **"safe inheritance
of parent transaction status at the fork point"** plus **"branch-local transaction
status handling"**:

1. A branch is created at a consistent LSN `L` on a parent timeline.
2. The new branch compute must see the parent's transaction status (clog /
   multixact / commit-ts) **exactly as of `L`** -- no earlier, no later.
3. From there the branch writes its **own** status forward on its own timeline,
   using ordinary local SLRU.

That is the whole M4 requirement. It is a *seed* problem, not a live-sharing
problem.

## The core correction: page images cannot represent as-of-`L` status

The prototype mirrors the **flushed page image** and versions it by one LSN. Review
(round 3) showed this is irreparably wrong for branch reads, because an SLRU page
**coalesces many independent logical updates** between flushes:

- A `pg_xact` page holds the commit/abort bits of thousands of xids. If xid A sets
  its bit before `L` and xid B sets its bit after `L` on the **same page**, with no
  checkpoint/eviction between them, the only flushed image that ever exists is the
  combined A+B page. Versioning it by A's LSN leaks B into the branch; versioning it
  by B's LSN hides A from the branch. There is no single-LSN page image that equals
  "the page as of `L`".
- Overflow/eviction make it worse: a dropped image cannot be reconstructed by
  re-reading the local segment later (that only yields the newest bytes), so a
  range-watermark overflow scheme loses the as-of snapshot entirely.

The fix is not a cleverer snapshot. **SLRU status as of an LSN must be reconstructed
from the per-update WAL records**, exactly as relation pages are reconstructed by
`redo_page_asof` over shipped WAL (PR #53). The SLRU-changing records --
`XLOG_XACT_COMMIT`/`ABORT`/`COMMIT_PREPARED`/... for clog, multixact create,
commit-ts set -- are **per-logical-update and LSN-ordered**, so replaying them up to
`L` yields the exact status page as of `L` with no coalescing ambiguity.

## Scope

In scope (M4): reconstructing clog, multixact (`offsets`+`members`), and commit-ts
as of the fork LSN to seed a branch, and the branch then running on local SLRU.

These are the WAL-logged, `uint32`-page SLRUs. Out of scope and explicitly excluded:

- `pg_subtrans`, `pg_notify`, serializable (`pg_serial`): they truncate during
  checkpoint/cleanup with **no truncate WAL record** (no LSN to reconstruct or
  tombstone against), and `pg_notify`'s queue page is an unbounded `int64` while the
  pagestore object `block` key is `uint32`. `pg_subtrans` is rebuilt on its own and
  is not needed across a fork; `pg_notify`/`pg_serial` are not branch transaction
  status. Admitting them later needs a per-SLRU version source and a 64-bit key.
- **Live multi-compute SLRU read-sharing** (several concurrent computes on one
  branch sharing each other's in-flight status). That is a separate, harder feature
  with its own section below; it is **not** required to boot a branch and is
  deferred past M4.

## M4 design: as-of-fork reconstruction

The seed is a **base snapshot + forward replay** (the same base-image-plus-redo
pattern relation pages use), not replay into empty segments. At branch creation (or
first start of the branch compute):

- **Base snapshot.** Start from a **consistent SLRU segment snapshot with a proven
  cutoff `C <= L`** -- the parent's actual `pg_xact` / `pg_multixact` / commit-ts
  segment contents as of `C` (see "`C` is not the checkpoint redo LSN" below for how
  `C` is established). WAL alone is not enough: an xid committed *before* `C` but
  still within the retained clog/multixact range has **no** record in `(C, L]`
  (`CheckPointCLOG()` / `SimpleLruWriteAll()` only flush pages, `CLOG_TRUNCATE` only
  removes them), so replaying into empty segments would leave its status unset and
  change visibility. The base must carry those already-set statuses.
  - The base must be a **clean as-of-`C` image**: all status with LSN `<= C`, none
    after. Then everything after `C` comes from per-update WAL and there is no
    coalescing. This is *not* the per-write flushed-page mirror round 3 ruled out.
  - **`C` is not the checkpoint redo LSN.** An online checkpoint fixes `redo`,
    releases the WAL insertion locks, and only later flushes SLRUs in
    `CheckPointGuts()`; `SimpleLruWriteAll(.., allow_redirtied=true)` lets a commit
    *after* `redo` land in that flush. So the flushed image contains status past
    `redo`, and keying it by `redo` would let a branch whose `L` falls between
    `redo` and such a commit seed with post-branch status. `C` must instead be an
    LSN that truly upper-bounds the snapshot's contents (at or after the checkpoint
    **completion record**), and the snapshot must provably contain nothing past `C`
    -- captured at a quiesce/restartpoint, or under a brief SLRU write barrier that
    stamps the current insert LSN as `C`. A base may seed only branches with
    `L >= C`; a branch below the nearest snapshot uses an earlier one.
  - Source: the parent ships such a clean whole-segment SLRU snapshot to the store
    (coarse, periodic, keyed by its proven cutoff `C`), or its current segments are
    copied as the base when the parent is reachable at branch time.
- **Forward replay `(C, L]`.** Apply only the SLRU status effects of the records in
  `(C, L]` onto the base to bring it to exactly `L`, reusing #53's shipped-WAL +
  `XLogReader` path. Use a **narrowly scoped SLRU-status applier**, not full
  `xact_redo`: commit/abort redo also drops relation files, updates stats, and
  issues invalidations, none of which may run during branch seeding (they would
  mutate unrelated branch-local state). The applier extracts only the
  xid->status / multixact offset+member / commit-ts effects and writes them to the
  branch's segments. It must **also** process `XLOG_PARAMETER_CHANGE`
  (`RM_XLOG_ID`), whose redo (`CommitTsParameterChange()`) activates/deactivates
  commit-ts and creates/resets its segment state: a parent that toggles
  `track_commit_timestamp` between `C` and `L` would otherwise seed commit-ts active
  over the wrong interval or miss the activation page.
- After materialization the branch has ordinary local SLRU and **writes forward
  itself**; nothing is served from the store on the steady-state path, so none of
  the live-mirror hazards (critical-section mirroring, read-hook interrupt safety,
  cache coherence) arise for M4.

Why this dissolves the round-1/2/3 findings:

- The base is a consistent as-of-`C` image and everything after it is per-update
  WAL, so there is no flushed-page coalescing and no single-image-answers-as-of-`L`
  problem.
- The result's "version" is intrinsic -- base at `C` plus exactly the WAL through
  `L` -- so there is no daemon-counter-vs-branch-LSN mismatch and no per-object
  version IPC for the seed path.
- Truncation as of `L` is the base's truncate horizon plus the truncate records in
  `(C, L]`; no store tombstone is needed to seed a branch.

### Reconstruction correctness

- The replay must stop at exactly `L` (record-aligned): a record that *starts* at or
  before `L` but ends after it is not part of the as-of-`L` state (same rule
  `redo_page_asof` already applies for relation pages).
- multixact needs both `offsets` and `members` replayed together to a consistent
  point; commit-ts is reconstructed for whatever intervals `track_commit_timestamp`
  was on, per the `XLOG_PARAMETER_CHANGE` records the applier replays.
- Fail closed: if either the base snapshot at `C` or the parent WAL across `(C, L]`
  is unavailable on the store, branch creation fails rather than seeding partial or
  zeroed status -- a half-known clog must never boot.

## Identity and versioning (for any persisted SLRU object)

The seed path needs no SLRU store objects. *If* SLRU pages are ever persisted as
store objects -- as a reconstruction cache, or for the deferred live-sharing path --
they use:

- **Identity.** `PsKey{ klass = PS_KLASS_SLRU, relNumber = slru_klass_id(Dir),
  block = pageno }` (`pageno` fits `uint32` for the in-scope SLRUs).
- **Version = the dirtying WAL LSN**, supplied by the writer and stored verbatim
  (never a daemon `max+1` counter -- not comparable to a branch cutoff), captured
  when the page is **logically dirtied** (clog: the commit record LSN), not at
  physical-write time. This keeps versions comparable to branch cutoffs. (Even so,
  per round 3, a single image per page cannot be branch-correct on its own; WAL
  reconstruction remains the source of truth.)

## Deferred: live multi-compute SLRU read-sharing (post-M4)

Letting several **concurrently running** computes on one branch observe each other's
in-flight transaction status is a distinct feature, not needed to boot a branch.
When it is built, the three review rounds established the requirements it must meet
-- recorded here so they are not lost:

- **Per-update capture, not flushed-page snapshots.** Because pages coalesce
  updates, live sharing must ship the per-update status changes (or reconstruct via
  WAL on the reader), not single-LSN page images.
- **No-drop overflow.** A staging queue may not drop an image and "reread later"
  (the as-of bytes are gone); it must block/backpressure or spill durably.
- **Snapshot under the bank lock.** Any page image staged must be copied under the
  bank lock before `SlruInternalWritePage()` releases it for `pg_pwrite`, or a
  concurrent write-OK caller changes the bytes.
- **Contiguous-durable-prefix visibility watermark.** A reader gates xid visibility
  on a `mirrored_status_lsn` watermark that advances only over a **contiguous
  durable prefix** -- not "highest mirrored", since out-of-order/failed drains mean
  a high LSN being durable does not imply lower ones are. The local commit is never
  held back; only its visibility to *other* computes waits.
- **Cache-hit revalidation, including tombstones.** `SimpleLruReadPage()` returns a
  valid cached slot without hitting the physical-read hook, so cached slots must be
  revalidated against both the status watermark **and** truncation tombstones
  (tombstones are a separate versioned negative result with their own
  epoch/invalidation).
- **Tombstones with a defined version + synchronous truncate barrier.** A store
  tombstone must be durable before local segment deletion, versioned by the
  truncation LSN -- which only the WAL-logged SLRUs have.
- **Fail-closed, interrupt-safe hooks.** Read and existence hooks return
  `SERVED`/`FALLBACK`/`FAILED`; `slru.c` cleans the slot before any error; a
  store-required page fails closed (never a zero page); no `PG_RE_THROW()` across the
  `SLRU_PAGE_READ_IN_PROGRESS` window.
- **Daemon IPC.** `READ_AT` must report found-ness in `ch->result` and return the
  resolved version on **both** POSIX and SPDK paths (neither does today).
- **Critical-section-safe write path.** Mirroring must never do fallible store I/O
  in a commit/abort critical section; it stages into shared memory and drains
  outside.

This list is the spec for that feature; none of it blocks M4.

## Sequencing (M4)

1. Parent: ship a clean whole-segment SLRU snapshot (clog/multixact/commit-ts) to
   the store, keyed by a **proven cutoff `C`** (at/after the checkpoint completion
   record, or a barrier-stamped insert LSN) -- never the redo LSN.
2. A narrowly scoped **SLRU-status applier** that applies only the status effects of
   xact/multixact/commit-ts **and `XLOG_PARAMETER_CHANGE`** records (no relation
   drops, stats, or invalidations).
3. Branch-create: load the base snapshot at `C <= L`, replay `(C, L]` through the
   applier into the branch's segments; fail closed if the base or that WAL is
   unavailable.
4. Verify the branch boots on its reconstructed status and writes forward locally.
5. Tests: the acceptance scenarios below.

(The deferred live-sharing items above are sequenced separately, after M4.)

## Acceptance criteria (M4)

- A branch created at `L` sees a committed xid iff its commit record is at/below `L`;
  an xid committed on the parent after `L` is **not** committed on the branch, even
  when it shares a `pg_xact` page with a pre-`L` commit.
- An xid committed **before the base `C`** but still in the retained clog range is
  committed on the branch -- its status comes from the base snapshot, not from
  `(C, L]` WAL (which does not contain it).
- A base snapshot is keyed by a cutoff `C` that provably bounds its contents (no
  status past `C`), so a branch at `L >= C` never inherits a commit that landed in
  an online checkpoint's flush after `redo` but after `L`.
- Branch seeding does not drop relation files, touch stats, or fire invalidations
  (narrow applier, not full `xact_redo`).
- multixact and commit-ts as of `L` match what the parent saw at `L`, including when
  the parent toggled `track_commit_timestamp` in `(C, L]` (the applier replays
  `XLOG_PARAMETER_CHANGE`).
- The branch boots from its reconstructed SLRU and then writes its own status
  forward with no store involvement on the steady-state path.
- If the base snapshot or the parent WAL through `L` is not available on the store,
  branch creation fails closed rather than seeding partial/zeroed status.
- A non-WAL/`int64` SLRU (`pg_subtrans`, `pg_notify`, serializable) is excluded, not
  silently store-backed.

## Open questions

- Materialize-at-create (simple; a one-time replay cost) vs. serve SLRU pages
  on-demand via as-of redo (lazier; needs the read hooks and their safety). M4
  leans materialize-at-create.
- How far back the replay base must sit for each in-scope SLRU, and how that
  interacts with parent WAL retention (see PGCONTROL_ON_STORE_DESIGN.md's
  retention-gating point).
- Whether commit-ts should be reconstructed eagerly or only when
  `track_commit_timestamp` is on.

See also: the read-path PR stack plan, the branch-PostgreSQL milestone plan (M4:
bootable branch compute), the store-backed WAL reader (#53) this reuses, and
[PGCONTROL_ON_STORE_DESIGN.md] for the sibling pg_control protocol.
