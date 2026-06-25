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
  forkNum = 0, block = 0 }`. `pg_control` fits in one block
  (`PG_CONTROL_FILE_SIZE <= BLCKSZ`); the image is zero-padded to `BLCKSZ`.
- **Version order.** Each mirror write gets a daemon-assigned monotonic version
  (`max across ancestry + 1`), never derived from the control bytes. The control
  file's own `time`/checkpoint fields are not a reliable version source (fix for
  finding 2).
- **Timeline.** The control object is written on the writer's timeline; restore
  reads it from the timeline the branch was created on (see restore).

## Mirror protocol (critical-section-safe)

The core decision: **the mirror never does fallible work inside a checkpoint or
shutdown critical section.** `UpdateControlFile()`'s synchronous `obj_write` is
removed.

### Checkpoint-completion handoff

- `UpdateControlFile()` (and the hook) under the critical section only **records
  intent**: it snapshots the just-written `ControlFileData` image (or just the
  fact that pg_control changed, plus the bytes) into a small in-process slot. This
  is allocation-free and cannot fail the checkpoint.
- The actual `obj_write` of the control image runs at a **post-critical-section
  point**: a checkpoint-completion callback (after `CreateCheckPoint()` leaves its
  critical section) or a dedicated async shipper. Shutdown's final control update
  ships from the shutdown path *after* the critical section, before the postmaster
  exits.
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

1. **Select the timeline.** Restore reads the control object from the timeline the
   branch was created on (passed in / derived from branch metadata), not a fixed
   timeline 0 (fix: "Read the same timeline that pg_control was written to").
2. **Fetch + verify.** Read the control object via the daemon channel; verify the
   CRC of the restored `ControlFileData` before writing anything. A bad/missing
   image fails closed (the compute does not start on a half-known cluster).
3. **Write atomically.** Write to `global/pg_control.tmp`, fsync it, then
   `rename()` over `global/pg_control` -- never truncate the live file in place
   (fix for finding 4). `rename` within a directory is atomic.
4. **Directory-durable.** fsync the `global/` directory after the rename so the
   rename survives a crash (fix: "Make restored pg_control durable in its
   directory").
5. **Release the channel.** Release the claimed daemon channel on every exit path,
   including error paths (fix: "Release the daemon channel before exiting").
6. **Fail closed, claim no durability it lacks.** If any step fails, leave no
   `global/pg_control` (or the pre-existing one) and exit non-zero; the tool never
   reports success for a torn or unverified control file.

## Sequencing

1. Core: `UpdateControlFile()` records intent only; add the post-critical-section
   ship point (checkpoint-completion callback / shutdown ship) and chain the hook.
2. Daemon/module: control object monotonic version + `sync` before
   checkpoint-complete-on-store.
3. Tool: atomic temp+rename, dir fsync, timeline selection, channel release,
   CRC-verify, fail-closed.
4. Tests: the acceptance scenarios below.

## Acceptance criteria

- No fallible store I/O runs inside a checkpoint or shutdown critical section; a
  daemon error during mirror cannot PANIC the checkpoint.
- After a completed checkpoint, the store's control image carries that
  checkpoint's redo pointer, and is durable (survives daemon restart).
- Restore produces a CRC-valid `global/pg_control` via temp+rename with a fsync'd
  directory; a crash mid-restore never leaves a torn control file.
- A compute with no local `pg_control` restores from the correct branch timeline
  and boots into recovery from the mirrored checkpoint.
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
