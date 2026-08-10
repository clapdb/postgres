# SLRU on the store: lifecycle design (M4)

Status: implemented (M4), with explicit caveats.  The WAL-based as-of
reconstruction this document specifies has landed: snapshot shipping
(`pagestore_ship_slru_snapshot`), recovery-materializer proven-cutoff capture
(`pagestore_capture_slru_snapshot`), the as-of appliers for clog / commit-ts /
multixact offsets+members, the branch seeders, and the
prepare/install/manifest bootstrap flow (`pagestore_prepare_branch` /
`pagestore_install_prepared_branch`) with fail-closed branch startup
validation.  The control-derived path additionally publishes a CRC-bound
portable catalog-map artifact, and
`pagestore_install_prepared_branch_bootstrap` installs it together with these
SLRUs into a fresh same-build `initdb` skeleton after archive-bootstrap control
restore; the ordinary branch manifest remains the last readiness marker.  The
current portable format rejects user tablespaces explicitly.  The "matching
parent state across `track_commit_timestamp`
toggles" acceptance criterion is met: the commit-ts appliers replay toggle
eras (first item below).  64-bit multixact
member horizons are accepted end to end, but store page addressing uses
uint32 block numbers, capping members pages at 2^32 (about 2^42 member
offsets) until the object key grows a 64-bit block field.

Known caveats -- each remains open work, with its exact failure mode stated:

- **Commit-ts toggles in (C, L] are replayed as eras.**  DONE: the GUC is
  PGC_POSTMASTER, so a toggle always crosses a parent restart and lands a
  `XLOG_PARAMETER_CHANGE` in the window.  The appliers treat each toggle as
  an era boundary: DEACTIVATION wipes everything accumulated (the parent
  deleted every segment), ACTIVATION wipes as well (defending against
  era-crossing bytes a base snapshot carried) and starts the new era, whose
  one silently-zeroed page -- `ActivateCommitTs` zeroes the page holding
  nextXid-at-activation WITHOUT WAL; every later page has a WAL-logged
  ZEROPAGE -- is recovered two ways: the seeder marks the horizon page
  (`oldest_xid`, which a correct caller sets to the activation nextXid) as
  zero-present, and the single-page applier infers it as the only era page
  commits touch with no prior ZEROPAGE.  A target inside an off window
  fails closed with a distinct error; an inactive fork is seeded with a
  non-normal horizon (the pre-existing empty-`pg_commit_ts` path).  The
  branch horizon comes from the fork's pg_control (the control mirror ships
  a fresh image at the toggle itself, via XLogReportParameters'
  UpdateControlFile, so `track_commit_timestamp` as-of-L is reliable).
  `pagestore_prepare_branch_from_control` performs that derivation for an
  exact, admission-fenced checkpoint redo and cuts ancestry at a separately
  supplied materialized fork boundary covering the checkpoint; the legacy
  prepare ABI still accepts explicit horizons for compatibility.
- **The legacy expert snapshot API trusts the caller's cutoff.**
  `pagestore_ship_slru_snapshot` copies one named directory and keys it by a
  caller-supplied cutoff C; it does not validate the quiescence proof.  The
  installed `pagestore_capture_slru_snapshot()` path closes that gap for the
  recovery-materializer topology: the caller first confirms WAL replay is
  paused; capture requests a restartpoint, requires its durable materializer
  marker to equal the unchanged replay LSN, stages all four SLRUs, rechecks the
  pause/replay position, then publishes and syncs the staged image at C.  A
  failed or concurrent-resume attempt returns no usable cutoff.  Automating the
  surrounding pause/resume remains control-plane orchestration.
- **The appliers can read the (C, L] WAL from the store.**  DONE:
  `pagestore.redo_wal_from_store` redirects the SLRU appliers' and seeders'
  linear scans to the store's shipped per-timeline WAL log, exactly as it
  does for `redo_page_asof` -- so prepare can run on a compute with no
  local WAL (a utility compute sets `pagestore.timeline` to the timeline it
  acts for).  The store holds completed segments only, so a window ending
  in the current partial segment fails the coverage probe and the caller
  fails closed, as with locally-truncated WAL.  Live SLRU page mirroring through the
real SLRU code path (the PR-#49 direction, rejected as the primitive for
branch correctness) remains deferred to the post-M4 multi-compute milestone,
as does `pg_subtrans`/`pg_notify`/`pg_serial` coverage.  History: the PR-#49
prototype mirrored flushed SLRU page images and served them via a read hook;
three review rounds showed that model cannot give branch-correct status at a
fork LSN, which is why M4 pivoted to as-of reconstruction.

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
  - Source: a recovery materializer ships such a clean whole-segment SLRU
    snapshot to the store with `pagestore_capture_slru_snapshot()` (coarse,
    periodic, keyed by its proven cutoff `C`), or the parent's current segments
    are copied as the base under an equivalent proven quiesce.
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

- **Per-update capture, not flushed-page snapshots.** ADDRESSED without
  per-update shipping: the coalescing objection is fatal only when a page
  image must answer an exact as-of question (branch seeding, which stays
  on WAL reconstruction).  The live mirror never answers as-of: readers
  take the NEWEST image -- every bit in any shipped image is a durable
  commit (ship happens only after `XLogFlush(fence)`) -- and the
  watermark is a completeness floor, not a cap (an image's version can
  exceed the watermark, since a checkpoint's flush includes commits after
  its redo; capping the read would hide status below the floor that only
  that image carries).
- **No-drop overflow.** A staging queue may not drop an image and "reread later"
  (the as-of bytes are gone); it must block/backpressure or spill durably.
- **Snapshot under the bank lock.** Any page image staged must be copied under the
  bank lock before `SlruInternalWritePage()` releases it for `pg_pwrite`, or a
  concurrent write-OK caller changes the bytes.
- **Contiguous-durable-prefix visibility watermark.** DONE
  (`pagestore_slru_mirror_watermark()`): the candidate is the redo pointer
  of the last completed checkpoint whose pg_control image durably shipped
  (a completed checkpoint flushed -- and the mirror staged -- every dirty
  SLRU page), and the watermark advances to it only while **no** process
  holds a staged-but-unsynced image (per-process pending floors in shared
  memory; drain consumes entries only after the store sync).  Not "highest
  mirrored": a staged image carries all status on its page since the
  page's previous durable image, so its uncovered low end is unknown and
  no partial bound is safe.  A lost capture freezes the watermark **for
  good**: the lost page is clean locally, so no later checkpoint provably
  re-flushes (and re-captures) it -- W keeps what it already vouched for
  and never grows until an operator re-primes the mirror and calls
  `pagestore_slru_mirror_reset_debt()`.  Losses are persistent: a debt
  marker file survives clean shutdowns, and any unclean previous life
  (pg_control not `DB_SHUTDOWNED` at boot) is itself boot debt, since a
  dying process may have held staged images -- and enabling the mirror on
  a cluster with pre-existing SLRU history starts unprimed, since clean
  local segments are never flushed (captured) again: the watermark stays
  frozen until `pagestore_slru_mirror_reset_debt()` both primes and
  forgives (a persistent primed marker).  Image versions are always real
  WAL positions: the capture-time fence, lifted at post time above every
  version the page ever shipped at (a shared CAS-max floor table) but
  never past the insert pointer -- when the floor catches up, the entry
  defers to a later drain, its pending floor keeping the watermark honest
  until WAL insertion unblocks it.  LSN-comparable, restart-ordered (WAL
  only grows), and two images of one page can never tie, so store arrival
  order never decides which bytes win.  The local commit is never held
  back; only its visibility to *other* computes waits.
- **Cache-hit revalidation, including tombstones.** DONE:
  `slru_page_revalidate_hook` runs on every `SimpleLruReadPage()` cache
  hit AND on `SimpleLruReadPage_ReadOnly()`'s shared-lock fast path (the
  normal status lookups all use it); bank lock held, shared-memory checks
  only.  The revalidation state is **shared**: SLRU buffers are shared,
  so a page one backend served from the mirror is every backend's cache
  hit -- the fetched watermark, the served-page decision epochs, and the
  newest known tombstone per SLRU (which can advance independently of
  the watermark) all live in the mirror's shared segment, on a shared
  fetch TTL.  A slot is stale once the watermark or the page's tombstone
  coverage moves past its epoch -- one physical re-read per page per
  epoch, re-running the full read-hook gating.  The fetch refreshes at
  read misses and transaction boundaries (TTL-bounded IPC, never under
  the bank lock); unknown pages count as stale (a redundant re-read,
  never a stale answer).  Dirty slots are local truth and are never
  discarded.  The live mirror has exactly ONE writer per branch timeline
  (`pagestore.slru_mirror` on the branch primary; live-read computes only
  consume): newest-image reads depend on that invariant.  Mirror objects
  are timeline-local (the compute's timeline brands the key), so a branch
  never resolves its ancestor's live images or watermark past the fork
  point; on the writer itself, a locally present segment always outranks
  the (lagging) mirror.  Cross-SLRU pairs (multixact offsets/members) are
  coherent for anything at/below the watermark -- a checkpoint flushes
  both -- which is exactly the range a reader may trust; consistency
  beyond W is the multi-compute read-consistency work.
- **Tombstones with a defined version + synchronous truncate barrier.**
  DONE: `slru_truncate_hook` fires before any local segment deletion --
  `SimpleLruTruncate` (after its wraparound backstop), `SlruDeleteSegment`,
  and `DeactivateCommitTs()`'s delete-all reset (`PG_INT64_MAX` cutoff).
  Multixact truncation runs critical, so `TruncateMultiXact()` invokes the
  hook as a fallible pre-barrier before `START_CRIT_SECTION()`; the
  in-critical calls find their cutoff covered and no-op.  The consumer
  flushes the truncation WAL first (TruncateCommitTs inserts without
  flushing), ships a `PS_KLASS_SLRU_TOMB` cutoff tombstone and syncs it
  durably before returning, versioned by the exact truncation record where
  the caller knows it (TruncateCLOG/TruncateCommitTs run the barrier
  themselves with the record's end LSN; SimpleLruTruncate's hook call then
  finds the cutoff covered) and by the current position otherwise -- in
  recovery the END of the record being replayed (`GetCurrentReplayRecPtr`),
  never the last-replayed position, which would make the tombstone visible
  below the truncation record itself.  The synchronous ship publishes its
  own pending floor so no concurrent checkpoint advances the watermark past
  a truncation whose tombstone is still in flight; the covered cache skips
  exact duplicates only (page spaces wrap).  A store failure freezes the
  watermark (the truncation record is typically already WAL-logged) and
  then raises, abandoning the local truncation -- except in recovery and
  for the delete-all reset, where nothing can be abandoned: there it stays
  a counted loss (interrupts still propagate).  Read-side enforcement is
  DONE too: a tombstoned page fails closed on a live-read compute, but an
  image shipped after the tombstone outranks it (legitimate re-creation,
  e.g. commit-ts re-activation), and on the mirror's own writer the local
  files win.
- **Fail-closed, interrupt-safe hooks.** DONE: `slru_page_read_hook` returns
  `SLRU_READ_HOOK_{SERVED,FALLBACK,FAILED}` at the top of
  `SlruPhysicalReadPage()`; a `FAILED` result flows through the existing
  `SlruReportIOError` machinery (slot cleaned first, never a zero page, no
  throw inside the `SLRU_PAGE_READ_IN_PROGRESS` window);
  `slru_page_exists_hook` answers `SimpleLruDoesPhysicalPageExist()` probes
  (ActivateCommitTs's zero-create, find_multixact_start) with the same
  contract; and `slru_page_write_hook` stages the image in
  `SlruInternalWritePage()` while the bank lock is still held (the
  snapshot-under-the-bank-lock requirement), documented infallible, carrying
  the page's largest group commit LSN as a **WAL fence** the consumer must
  not publish past before that WAL is flushed (the per-update-capture and
  visibility-watermark items build on it).  NULL-default; the module
  consumer is this feature.
- **Daemon IPC.** DONE: `READ_AT` reports found-ness in `ch->result` and
  returns the resolved version on both the POSIX and SPDK paths (SPDK defers
  found-ness to its read completion, so a failed async read is not advertised
  as a found zero page).
- **Critical-section-safe write path.** DONE: `pagestore_slru.c`
  (`pagestore.slru_mirror`) consumes the write hook -- the bank-lock
  snapshot goes into a pre-reserved in-process queue (infallible at the
  hook, like the pg_control mirror) and ships at post-critical drain
  points, `XLogFlush(fence)` before each image so no other compute can see
  a status bit whose WAL is not durable.  Images are keyed
  `PS_KLASS_SLRU_LIVE`, versioned by the fence -- deliberately NOT the
  `PS_KLASS_SLRU` seed keyspace, whose snapshots promise a proven
  clean-as-of-cutoff that flushed page images do not have; a live image
  means only "newest flushed bytes, contents bounded by the version".
  No-drop staging: queue overflow degrades to a recapture-by-identity
  table (re-snapshot under the bank lock, or from the local segment if
  evicted, under a freshly issued stamp); only double overflow loses
  coverage, and that is counted (`pagestore_slru_mirror_stats()`) so the
  watermark fails conservative.  Post-then-sync: entries pop only after
  the store sync, and once posted their bytes are frozen at the posted
  version (a timed-out request may still land later; byte-identical
  retries make same-version arrival order irrelevant, and newer bytes
  re-ship via recapture under a strictly higher stamp).

This list is the spec for that feature; none of it blocks M4.

## Sequencing (M4)

1. Recovery materializer: pause replay, request a restartpoint, and use
   `pagestore_capture_slru_snapshot()` to ship a clean whole-segment SLRU
   snapshot (clog/multixact/commit-ts) to the store, keyed by its **proven
   cutoff `C`** -- never the checkpoint redo LSN.  DONE for the local POSIX MVP.
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
