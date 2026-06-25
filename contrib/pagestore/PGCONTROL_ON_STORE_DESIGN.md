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
- **Version order (branch-aware).** Like SLRU objects (see SLRU_ON_STORE_DESIGN.md),
  the control object's version is a WAL LSN supplied by the writer and stored
  verbatim -- not a daemon `max+1` counter (not comparable to a branch cutoff) and
  not the control bytes. The LSN must be the **checkpoint/control-update record's
  LSN** (`ProcLastRecPtr`, i.e. `ControlFile->checkPoint`), **not the redo pointer**
  (`checkPointCopy.redo`). For an online checkpoint that spans a branch point the
  redo pointer is *before* the branch while the checkpoint completion record is
  *after* it; versioning by redo would make that just-completed checkpoint visible
  to an as-of-branch read and restore the branch to post-branch state. Versioning by
  the checkpoint record's own LSN keeps restore-as-of-branch-point (below) correct.
- **Timeline.** The control object is written on the writer's timeline; restore
  reads it as of the branch-creation LSN on the branch's ancestry (see restore).

## Mirror protocol (critical-section-safe)

The core decision: **the mirror never does fallible work inside a checkpoint or
shutdown critical section.** `UpdateControlFile()`'s synchronous `obj_write` is
removed.

### Checkpoint-completion handoff

- `UpdateControlFile()` (and the hook) under the critical section only **records
  intent**: it snapshots the just-written `ControlFileData` image plus the
  checkpoint-record LSN into a small handoff slot (in-process for a same-process
  shipper, shared memory for a separate one -- see below). This is allocation-free
  and cannot fail the checkpoint.
- The actual `obj_write` of the control image runs at a **post-critical-section
  point**. The recorded-intent slot is read by whoever ships it, so it must be
  reachable by that process: either ship from a **same-process completion callback**
  (after `CreateCheckPoint()` leaves its critical section, in the checkpointer/
  startup process that recorded it) -- the default -- or, if a **separate** shipper
  process is used, put the handoff slot in **shared memory**, never a private
  per-process buffer (a private slot would be invisible to the shipper and the
  image would be dropped). Shutdown's final control update ships from the shutdown
  path *after* the critical section, before the postmaster exits.
- **Durability contract.** The mirrored control object is shipped and the daemon
  `sync`'d before the checkpoint is considered complete-on-store. Restore-after-
  crash therefore sees a control image no newer than the last completed checkpoint
  -- exactly the recovery contract local `pg_control` already provides. Define the
  ordering as: local `update_controlfile(..., do_sync=true)` first (unchanged),
  then store mirror + store sync; a compute that restores from the store replays
  from the mirrored checkpoint's redo pointer.
- **Hook chaining.** Installing `control_file_write_hook` must save and call any
  previously installed hook (fix for finding 5), so the seam composes with other
  extensions.

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
2. Core: `UpdateControlFile()` records intent only; add the post-critical-section
   ship point (checkpoint-completion callback / shutdown ship) and chain the hook.
3. Daemon/module: control object version = the checkpoint **record** LSN
   (writer-supplied, not the redo pointer) + `sync` before checkpoint-complete-on-
   store. Same-process completion callback, or a shared-memory handoff slot if a
   separate shipper is used.
4. Tool: atomic temp+rename **at `PG_CONTROL_FILE_SIZE`**, dir fsync,
   **as-of-branch-LSN** read, channel release, CRC-verify, fail-closed.
5. Tests: the acceptance scenarios below.

## Acceptance criteria

- No fallible store I/O runs inside a checkpoint or shutdown critical section; a
  daemon error during mirror cannot PANIC the checkpoint.
- After a completed checkpoint, the store's control image carries that
  checkpoint's redo pointer, is versioned by the checkpoint **record** LSN, and is
  durable (survives daemon restart).
- An online checkpoint whose completion record is after a branch point is **not**
  restored by that branch (versioned by the record LSN, not the redo pointer).
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
