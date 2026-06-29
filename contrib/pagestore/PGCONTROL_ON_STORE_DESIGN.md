# pg_control on the store: mirror + bootstrap-restore protocol (M4)

Status: design. The PR-#50 prototype (`feat/pagestore-control`) mirrors
`pg_control` to the store on every write and restores it before startup with a
freestanding tool. Review (5 P1 / 11 P2) showed the prototype runs fallible work
inside recovery-critical sections and restores non-atomically. Per the read-path
stack plan, store-backed pg_control stays frozen until this protocol is defined.
This document is the gate; implementation resumes against it.

## Scope

`pg_control` (`global/pg_control`) is the cluster's root metadata: state, latest
checkpoint location and its redo pointer, timeline, system identifier, and format
versions. A bootable branch compute needs a consistent `pg_control` to start
recovery on its branch timeline. Putting it on the store lets a branch compute
bootstrap that metadata without a pre-seeded local file.

In scope: the control object model, a critical-section-safe mirror protocol, and
an atomic, durable restore protocol. Out of scope: full branch startup/recovery
flow (the rest of M4) and retention (M5).

## Why the prototype is not mergeable

`pg_control` sits on the most conservative recovery path. The prototype hooks
`UpdateControlFile()` and calls `obj_write(PS_KLASS_CONTROL, ...)` synchronously.
The findings reduce to five invariants the prototype breaks:

1. **Critical-section mirroring.** `UpdateControlFile()` is called from
   `CreateCheckPoint()` / shutdown inside a critical section. A fallible daemon
   write there means a daemon error becomes a PANIC, and the prototype itself
   documents this as a "known prototype risk."
2. **Version key.** The control object is written at a fixed block with a version
   derived like any object; repeated control writes must carry a real changing
   version so a later image cannot compare equal-or-lower and be dropped.
3. **Mirror durability.** A mirrored control image is only useful for restore if it
   is durable on the store before the corresponding local control update is relied
   on; the prototype does not define or enforce that ordering.
4. **Atomic restore.** The restore tool truncates the existing `global/pg_control`
   in place and rewrites it. A crash mid-write leaves a torn control file -- the
   one file that must never be torn.
5. **Restore durability + correctness.** Restore does not fsync the containing
   directory, may read a different timeline than the one written, and leaks the
   claimed daemon channel on some exit paths. An installed prior control-file hook
   is also clobbered rather than chained.

## Control object model

- **Identity.** `PsKey{ klass = PS_KLASS_CONTROL, spcOid = dbOid = relNumber =
  forkNum = 0, block = 0 }`.
- **Block-size requirement.** `PG_CONTROL_FILE_SIZE` is fixed at 8192 bytes, but
  `BLCKSZ` can be 1024/2048/4096 on `--with-blocksize` builds, where a single
  object block cannot hold the control file. The model therefore **requires
  `BLCKSZ >= PG_CONTROL_FILE_SIZE`** for control mirroring: the mirror/restore code
  asserts it and the module refuses to enable control-on-store on a smaller-block
  build (the default 8192 satisfies it). Splitting `pg_control` across multiple
  object blocks is possible but deferred -- it buys nothing on real deployments.
  Where it fits, the image is zero-padded to `BLCKSZ`.
- **Version order (branch-aware), per update.** Like SLRU objects (see
  SLRU_ON_STORE_DESIGN.md), the control object's version is a WAL LSN supplied by the
  writer and stored verbatim -- not a daemon `max+1` counter (not comparable to a
  branch cutoff) and not the control bytes. It must be the **LSN of the specific
  update that caused this control write**, passed in by the caller, **not
  `ControlFile->checkPoint` unconditionally** and **not the redo pointer**
  (`checkPointCopy.redo`):
  - a checkpoint writes its **checkpoint-record END LSN** -- the value `XLogInsert()`
    *returned* for the checkpoint record (`recptr`), not `ProcLastRecPtr`, which is
    the record's **start**. A branch cutoff that falls between the record's start and
    end would otherwise restore a control image for a record not fully in the
    branch's WAL stream. (`ControlFile->checkPoint` itself stays the start pointer;
    only the object *version* uses the end.) The redo pointer is likewise wrong --
    it precedes the branch while the completion record follows it.
  - non-checkpoint updates that do **not** advance `ControlFile->checkPoint` --
    `CreateEndOfRecoveryRecord()` (sets `minRecoveryPoint` to its `recptr`),
    promotion (`DB_IN_PRODUCTION`), and `minRecoveryPoint`/backup-state writes --
    must pass **their own** LSN. Versioning these by the stale `checkPoint` LSN would
    let an as-of-branch restore pick control bytes written after the branch, or
    coalesce distinct updates at one version. So the write hook takes an explicit
    `update_lsn` from each `UpdateControlFile()` caller.
  - **promotion has no inserted record**: reaching `DB_IN_PRODUCTION` sets
    `ControlFile->state` and calls `UpdateControlFile()` after recovery cleanup with
    no WAL record at that point. Its `update_lsn` must be a **durable end-of-log LSN
    captured after promotion cleanup** (the current insert position, which is `>=`
    all replayed WAL), or an explicit promotion marker record must be emitted to
    carry the LSN. Falling back to the previous checkpoint/`minRecoveryPoint` LSN is
    wrong -- it would make the `DB_IN_PRODUCTION` image visible to an as-of-branch
    cut taken before the state transition, or coalesce it with earlier control bytes.
- **Timeline.** The control object is written on the writer's timeline; restore
  reads it as of the branch-creation LSN on the branch's ancestry (see restore).

## Mirror protocol (critical-section-safe)

The core decision: **the mirror never does fallible work inside a checkpoint or
shutdown critical section.** `UpdateControlFile()`'s synchronous `obj_write` is
removed.

### Checkpoint-completion handoff

- `UpdateControlFile()` (and the hook) under the critical section only **records
  intent**: it appends the just-written `ControlFileData` image plus that update's
  LSN to an **ordered handoff queue**, not a single replaceable slot. Two control
  updates can occur before the shipper drains (e.g. an end-of-recovery update then a
  `DB_IN_PRODUCTION` update); a single slot would coalesce them to the latest image,
  so a branch point between their LSNs could restore post-branch control bytes. Each
  LSN-versioned image must survive until shipped.
- **The in-critical enqueue must never block or fail.** It runs inside the
  checkpoint/shutdown critical section, so it cannot apply backpressure when the
  store shipper is stalled -- blocking there would hang, and erroring would PANIC,
  the very critical section this design keeps fallible-free. The queue must
  therefore **pre-reserve capacity for every control write that can occur between
  two mandatory drains** (a small, bounded count: checkpoint + the handful of
  recovery/promotion/backup updates), so the append is always a guaranteed
  non-blocking slot; alternatively, drain-before-entry guarantees a free slot. All
  backpressure/draining happens only at the post-critical ship points, never inside
  the critical section. Appending is allocation-free and cannot fail the checkpoint.
- The actual `obj_write` of each queued image runs at a **post-critical-section
  point**, in LSN order. The queue is read by whoever ships it, so it must be
  reachable by that process: either ship from a **same-process completion callback**
  (after `CreateCheckPoint()` leaves its critical section, in the checkpointer/
  startup process that recorded it) -- the default -- or, if a **separate** shipper
  process is used, put the queue in **shared memory**, never a private per-process
  buffer (a private queue would be invisible to the shipper and the images dropped).
  Shutdown's final control update ships from the shutdown path *after* the critical
  section, before the postmaster exits.
- **Durability contract.** The mirrored control object is shipped and the daemon
  `sync`'d before the checkpoint is considered complete-on-store. Restore-after-
  crash therefore sees a control image no newer than the last completed checkpoint
  -- exactly the recovery contract local `pg_control` already provides. Define the
  ordering as: local `update_controlfile(..., do_sync=true)` first (unchanged),
  then store mirror + store sync; a compute that restores from the store replays
  from the mirrored checkpoint's redo pointer.
- **Gate WAL recycling on mirror durability, with a durable horizon.** A checkpoint
  removes old WAL (using the new `RedoRecPtr`) *after* `UpdateControlFile()`. If the
  store mirror/sync fails or lags after the local control file advanced, the store's
  control image is older -- its redo pointer needs WAL that the completed local
  checkpoint is now free to recycle, so a branch restoring that image would have a
  valid old `pg_control` but **missing WAL**. So shipped-WAL retention must never
  drop below the redo pointer of the control image **currently on the store**.
  - **The horizon lives on the store and must be durable.** An in-memory "block WAL
    removal" flag is lost if the process crashes after the local `pg_control` sync
    but before the store mirror finishes -- on restart, ordinary checkpoint/
    restartpoint cleanup would recycle WAL using the newer local redo while the store
    still holds the older control image. The pagestore daemon (itself durable) is the
    authority: it retains shipped WAL at/above the redo pointer of the control object
    it currently holds, and refuses to GC below it, independent of what the
    transient local compute decided. Branch restore uses the store's WAL + the
    store's control, both governed by this one durable horizon, so the crash window
    cannot strand a branch.
- **Hook chaining.** Installing `control_file_write_hook` must save and call any
  previously installed hook, so the seam composes with other extensions.

## Restore protocol (bootstrap before shared_preload)

`pg_control` is read and CRC-checked *before* `shared_preload_libraries` load, so
the module's hooks cannot serve it. A compute with no local control file restores
it with the freestanding `pagestore_control_restore` tool, then starts normally.

The restore must be atomic and durable:

1. **Read as of the branch point, not just the source timeline.** Selecting the
   source/parent timeline is not enough: if the parent advances after the branch is
   created, its *latest* control object carries a checkpoint redo pointer **beyond
   the branch point**, and restoring that would start the branch compute recovering
   from WAL/state it must not see. Restore must read the control object **as of the
   branch-creation LSN** -- either through the new branch timeline (whose ancestry
   walk already caps the parent at the branch LSN) or as an explicit as-of read of
   the source timeline at that LSN. The LSN-based control version (above) is what
   makes this cap meaningful (fixes "read pg_control at the branch point"; supersedes
   the earlier "just pick the source timeline" wording).
2. **Fetch + verify.** Read the control object via the daemon channel; verify the
   CRC of the restored `ControlFileData` before writing anything. A bad/missing
   image fails closed (the compute does not start on a half-known cluster).
3. **Write atomically, at the fixed file size.** Write exactly the first
   `PG_CONTROL_FILE_SIZE` (8192) bytes of the object image -- **not** the
   `BLCKSZ`-padded object -- to `global/pg_control.tmp`, fsync it, then `rename()`
   over `global/pg_control` (never truncate the live file in place). On a
   `BLCKSZ > 8192` build the object carries trailing pad that must be dropped, or
   tools that check the control-file length (pg_rewind, backup verification) reject
   the data directory. `rename` within a directory is atomic.
4. **Directory-durable.** fsync the `global/` directory after the rename so the
   rename survives a crash (fix: "Make restored pg_control durable in its
   directory").
5. **Release the channel.** Release the claimed daemon channel on every exit path,
   including error paths (fix: "Release the daemon channel before exiting").
6. **Fail closed, claim no durability it lacks.** If any step fails, leave no
   `global/pg_control` (or the pre-existing one) and exit non-zero; the tool never
   reports success for a torn or unverified control file.

## Sequencing

1. Module: assert `BLCKSZ >= PG_CONTROL_FILE_SIZE`; refuse control-on-store on
   smaller-block builds.
2. Core: `UpdateControlFile()` appends to an **ordered, pre-reserved handoff queue**
   (capacity for every in-critical control write between drains, so the enqueue never
   blocks/fails in the critical section); add the post-critical ship point
   (checkpoint-completion callback / shutdown ship, draining in LSN order) and chain
   the hook.
3. Core: each `UpdateControlFile()` caller passes the **LSN of its own update** --
   checkpoint **record-end** LSN (the `XLogInsert()` return, not `ProcLastRecPtr`),
   end-of-recovery `recptr`, and for promotion a durable **end-of-log LSN captured
   after cleanup** (or a marker record); never `ControlFile->checkPoint`.
4. Daemon/module: store the writer-supplied version + `sync` before
   checkpoint-complete-on-store; same-process completion callback, or a
   shared-memory queue if a separate shipper is used.
5. Daemon: a **durable** retention horizon -- the daemon retains shipped WAL at/above
   the redo of the control image it currently holds and refuses to GC below it,
   surviving a compute crash between local sync and store mirror.
6. Tool: atomic temp+rename **at `PG_CONTROL_FILE_SIZE`**, dir fsync,
   **as-of-branch-LSN** read, channel release, CRC-verify, fail-closed.
7. Tests: the acceptance scenarios below.

## Acceptance criteria

- No fallible store I/O runs inside a checkpoint or shutdown critical section; a
  daemon error during mirror cannot PANIC the checkpoint.
- After a completed checkpoint, the store's control image carries that
  checkpoint's redo pointer, is versioned by the checkpoint **record** LSN, and is
  durable (survives daemon restart).
- An online checkpoint whose completion record is after a branch point is **not**
  restored by that branch; the image is versioned by the checkpoint **record-end**
  LSN, so a branch cut between the record's start and end also excludes it.
- A non-checkpoint control update (end-of-recovery, promotion) after a branch point
  is not restored by that branch -- it is versioned by its own update LSN (promotion
  by a durable end-of-log LSN, since it inserts no record), not the stale
  `checkPoint` LSN.
- Two control updates between drains are both preserved (ordered queue), so a branch
  point between their LSNs restores the correct pre-branch image, not the later one.
- A stalled store shipper never blocks or PANICs a checkpoint/shutdown: the
  in-critical enqueue uses pre-reserved capacity and never applies backpressure.
- A branch restored from the store never has a control redo pointer whose WAL was
  recycled -- the daemon's **durable** retention horizon holds even across a compute
  crash between local `pg_control` sync and the store mirror.
- Restore produces a CRC-valid `global/pg_control` of exactly
  `PG_CONTROL_FILE_SIZE` bytes via temp+rename with a fsync'd directory; a crash
  mid-restore never leaves a torn control file, and length-checking tools accept it.
- A compute with no local `pg_control` restores the control image **as of its
  branch-creation LSN** -- even if the parent advanced afterward -- and boots into
  recovery from the checkpoint visible at the branch point, never a later one.
- Control-on-store is refused on a `BLCKSZ < PG_CONTROL_FILE_SIZE` build rather
  than silently truncating the image.
- A previously installed control-file write hook still runs after ours.
- Every restore exit path releases the daemon channel; failure exits non-zero and
  leaves no half-written control file.

## Open questions

- Whether to ship the control image from the checkpointer's completion callback or
  a separate shipper process, and how that interacts with the SLRU drain
  (SLRU_ON_STORE_DESIGN.md) which also drains at checkpoint.
- How branch metadata names the timeline restore should read (depends on the
  branch-create protocol in the rest of M4).
- Whether `pg_control` restore and the first WAL restore for a branch should be one
  tool or two.

See also: the read-path PR stack plan and the branch-PostgreSQL milestone plan
(M4: bootable branch compute), and [SLRU_ON_STORE_DESIGN.md] for the sibling SLRU
protocol.
