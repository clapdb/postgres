# Pagestore endurance driver

`pagestore_endurance.py` runs the MVP topology under real PostgreSQL computes
and concurrent SQL for as long as it is left running, and stops with a
preserved run root at the first thing it cannot explain.  It is the first
executable slice of `RELEASE_VALIDATION.md`'s V1 (real PostgreSQL endurance),
V2 (independent SQL correctness) and V3 (faults in the composed workload).
It is a bug-finding tool, not yet release evidence: see "Not covered yet".

```sh
meson setup build -Dcassert=true -Dauto_features=disabled && ninja -C build
meson test -C build --suite setup
contrib/pagestore/endurance/pagestore_endurance.py --build build \
    --root /path/on/a/real/filesystem --forever --duration 3600
```

Put `--root` on a real filesystem.  On tmpfs every fsync is free and the run
says nothing about the store's durability ordering.

`.github/workflows/pagestore-endurance.yml` runs a bounded run on demand (any
ref, seed, duration and scale), a 15-minute one weekly, and a five-minute
one on a pull request that changes the driver, uploading the
history, the passed runs' events and metrics, and a failed run's logs,
diagnostics and controller journals as the `pagestore-endurance-<seed>`
artifact.  It keeps the driver exercised on the current tree; multi-hour
runs stay on a developer box, and neither is release evidence
(`RELEASE_VALIDATION.md`).

## Topology

The provisioning mirrors `mvp_golden_test.sh` step for step, so a failure here
that the golden scenario does not show is caused by load, duration or
interleaving, not by a different setup:

- a WAL-only **writer** (`route_all = off`, `archive_library = 'pagestore'`);
- a continuous **materializer** (hot standby, `route_all = on`, 32 MB of shared
  buffers so that most reads come from the store);
- up to `--max-branches` **branch computes** (0 disables branching; the
  oldest is retired before a new one is created), each created by the installed
  `pagestore_branch_prepare` controller with
  `--verify-seed-against-materializer`, booted the portable way
  (`pagestore_control_restore --archive-bootstrap`, then
  `pagestore_install_prepared_branch_bootstrap`);
- one `pagestore_daemon` on a POSIX store.

One run is one topology lifetime: provisioning, then rounds for
`--duration` seconds (at least one).  `--forever` runs seed,
seed+1, ... each from a fresh `initdb` and import, so the bootstrap paths are
exercised as often as the steady state.

## Rounds

Each round is a seeded choice of:

1. **Burst.**  `pgbench` with eight custom scripts: balance transfers, FK
   inserts and range deletes on an indexed table, out-of-line TOAST rewrites
   up to 96 KB, aborts, subtransaction aborts, two-phase commits and
   overlapping `FOR KEY SHARE` lockers (which, with the FK, produce
   multixacts).  A second thread issues DDL and maintenance against the
   writer: create/drop/truncate, `VACUUM FULL`, `CLUSTER`, `REINDEX`,
   `ADD COLUMN`, bulk churn, `VACUUM (FREEZE)`, `CHECKPOINT`.  Autovacuum runs
   with a 5 s naptime and checkpoints every 30 s / 128 MB.
2. **One injected event during the burst:** materializer fast restart,
   immediate stop, `SIGKILL` of its postmaster, a forced restartpoint (a
   writer `CHECKPOINT` is replayed first, and the materializer's `CHECKPOINT`
   must move `pg_control_checkpoint()` up to it; if the materializer's own
   timed restartpoint gets there first, the driver retries with a newer
   checkpoint); writer
   immediate stop, or `SIGKILL` of one writer backend (crash-restart cycle).
   Orphaned prepared transactions are rolled back afterwards.
3. **Verify** (below), sometimes after restarting the materializer so that its
   buffer cache is empty.
4. **Branch** (`--branch-probability`), or a **coordinated store restart**
   (every compute stopped first, which is the MVP contract; the daemon gets
   `SIGTERM` or `SIGKILL`), followed by another verify.
5. Live branches are written to again and restarted at random.

## Oracles

The design rule from `HARNESS_DESIGN.md` holds: no test-side model of
visibility or redo.  The independent reference is PostgreSQL itself on a
path that does not touch the store.

| Check | Reference | What a mismatch means |
|---|---|---|
| Per-table `count` and order-independent row hash, materializer vs writer, with the writer quiescent and replay past the switch LSN | The writer's relations live in its local `md` files; the materializer's come from the store | WAL shipping, redo into the store, page versioning, reclamation or restart recovery lost or invented a row |
| The same hash, new branch vs the writer's state in the quiescent branch window | Writer | Fork-point visibility, SLRU seeding, relation-map/bootstrap install |
| Writer hash unchanged after only the branch was written | Writer before | Ancestry cutoff leaked a branch write into the parent |
| Branch hash before vs after a fast or immediate restart following `CHECKPOINT` | The same compute with a warm cache | A branch write did not survive in the store |
| Every live branch's hash before vs after a coordinated store restart (clean or `SIGKILL`) | The same compute before the restart | Branch-only pages were lost or changed when the store reopened |
| Every `public` index definition and validity, alongside each row hash above | The same reference | Replay or bootstrap lost, invented or changed an index |
| `sum(bal)` equals its initial value on every compute | Workload invariant | Torn or duplicated transaction effects (covers 2PC and aborts) |
| Index scan vs sequential scan for one `ev.k` | Same compute | Index and heap disagree |
| `bt_index_check(idx, heapallindexed => true)` on every btree | amcheck | Structural index corruption |
| The controller's seeded SLRU pages vs the materializer's | PostgreSQL recovery | `clog`/`commit_ts`/`multixact` appliers diverge from redo |
| Log scan: `PANIC`, `TRAP`, invalid page, unreadable block, signals 6/7/9/11 (9 only for a backend the driver did not kill itself) | -- | Assertion or crash anywhere |
| Liveness: every wait has a deadline; daemon and postmasters must be alive when expected | -- | Hang or unexplained exit |
| During a writer fault, pgbench and DDL errors must all be connection loss or recovery-in-progress | -- | A read or write error hidden behind the injected crash |
| The run ends with a fast stop of every compute and a `SIGTERM` of the daemon, which must exit 0 | -- | The final flush or shutdown failed |

Sequences are not compared: a sequence page legitimately differs between a
primary's buffer and replayed WAL.

## Output

`<root>/run-<seed>/` holds the store, every data directory and log,
`events.jsonl` (every action with its seed-derived choice), `metrics.jsonl`
(every `--sample-interval` seconds: RSS and open descriptors of the daemon and
each postmaster, store size by top-level directory) and `diagnostics/`
(`pagestore_inspect` dumps and `pg_stat_activity` at a timeout or failure).

- Pass: `PASS.json`, `events.jsonl` and `metrics.jsonl` move to
  `<root>/passed/<seed>/`; the bulk is deleted.
- Failure: the root stays as it was -- computes stopped immediate, the daemon
  `SIGKILL`ed so the store is not flushed past the failure -- with
  `FAILURE.json` naming the kind, detail, seed, round and commit.
- `<root>/history.jsonl` has one line per run.  `--forever` stops after
  `--max-failures` preserved roots or below `--min-free-gb`.

A seed fixes every driver choice (burst lengths, events, DDL, pgbench seeds)
but not the interleaving of concurrent sessions, so a rerun repeats the
schedule, not necessarily the failure.  Rerunning a seed whose failure root
is still under `--root` renames that root to `run-<seed>.<mtime>` first.  Rule from `RELEASE_VALIDATION.md`: an
unexplained failure stays open even if the rerun passes.

## Findings so far

**E-1. Branch preparation failed whenever the newest replayed checkpoint
record already had its restartpoint** (fixed in the controller).
`pagestore_capture_slru_snapshot()` requests a restartpoint and then requires
the durable materializer watermark to equal the paused replay position.
`CreateRestartPoint()` is a no-op when no checkpoint record newer than the
last restartpoint has been replayed, so the capture raised "restartpoint did
not durably cover the paused WAL replay position".  The golden scenario never
saw it because the script issues `CHECKPOINT` just before the controller and
the materializer's 5-minute timer has not consumed it; on a system that
checkpoints normally the consumed state is the common one.  The controller
now publishes its own checkpoint before the base capture (`CHECKPOINT`, WAL
switch, wait for replay) and retries a bounded number of times when the
materializer's own restartpoint still wins the race.  The fork capture uses
the writer's shutdown checkpoint and can in principle lose the same race; the
driver retries the whole controller and logs `branch_prepare_retry`, so its
frequency is measured rather than assumed.

**E-2. Materializer replay is far slower than WAL production, and got slower
as the store grew** (seed 1000: 16 MB segments went from 5 s to over 2 min
each, and a 27 s burst was not replayed in 30 min).  Stack samples of the
daemon showed the request worker almost always waiting for a lock held by the
maintenance thread.  Three causes, in the order found:

1. *Fixed.*  `ps_image_layer_lookup()` read and checksummed the layer's whole
   index section on every page lookup, then scanned it linearly; one
   `wal_retain_floor_level()` pass does that for every candidate layer while
   holding the admission lock.  Image indexes are now cached per process,
   keyed by layer id plus the footer that certifies them, and binary-searched.
2. *Fixed.*  With the map lock already held, `layer_map_lookup_impl()` dropped
   the `data_verified` result, so every such lookup checksummed the layer's
   entire data section again.
3. *Open (design).*  `compact_timeline()` merges every image layer of the
   shard into one -- cost proportional to the store, once per eight flushes --
   and does all of it (read, checksum, sort, write, fsync) under the shard and
   map write locks that every page write needs.  The redundant second
   checksum of inputs under the lock is gone, but building the replacement
   outside the lock and tiered compaction (both already on the post-MVP list
   in `MVP_COMPLETION_PLAN.md`) are what would change the order of magnitude.
   Disabling segment GC (`--segment-gc 0`) made no measurable difference.

Same seed, scale 1, 4 clients for 20 s (~800 MB of WAL), cassert build: replay
288 s before, 238 s after (2.8 -> 3.5 MB/s).  The index cache matters more as
layers grow; the long runs are the measurement for that.  None of this has
been measured on a production build yet.

**E-3. Image layers over 2 GiB cannot be read** (seed 2100; fixed in #286).
`local_read_layer_block()` issued one `pread()` and required the full
length; Linux returns at most `0x7ffff000` bytes per read and data
verification reads a layer's whole data section at once, so a layer past
2 GiB failed every lookup and could never be compacted away.  The
materializer died with `daemon reported error for op 9` (READV).

**E-6. Writes of WAL-less pages are refused once the fork-metadata
compaction cutoff passes the fork's definition** (seeds 2101, 2103, 2104;
open, the dominant failure).  Symptom: a `route_all` compute logs
`daemon reported error for op 8` (WRITEV) for the same block again and again
-- `base/5/2840_vm` block 0 on a branch, `base/5/37249_fsm` block 2 on the
materializer -- until the checkpointer cannot write it and the compute dies
(FATAL on the materializer, aborted transactions on the branch).  The
daemon logs nothing.

Root cause, established on the preserved seed-2104 store:

- The relation's FSM fork was defined (`SET`, `GROW` to 3 blocks) at
  `0/FA14F078`; those events sit in the compacted fork-metadata checkpoint
  of generation 4, whose cutoff is `0/FFA58DB0` (seq 890109).
- FSM pages are never WAL-logged, so their `pd_lsn` is 0.  In
  `append_page_impl()` a zero-LSN relation page becomes a *WAL-less ordered
  record* whose header LSN is the fork's growth floor
  (`fe->last_def_lsn` = `0/FA14F078`), and every ordered record must satisfy
  `fork_meta_mutation_future(hdr_grow_lsn, admission_seq)`, i.e. be
  lexicographically above the cutoff -- which `0/FA14F078 < 0/FFA58DB0`
  never is, whatever the admission sequence.  The write is refused with
  `PS_APPEND_REFUSED_FORKMETA_CUTOFF` even though block 2 already exists
  (no growth) and the segment could hold it.
- On the branch the floor is `branch_lsn + 1` (a visibility-map page
  inherited from the parent keeps a pre-fork `pd_lsn`, so it is *clamped*
  to that floor); the branch is exempt from the cutoff only while it has no
  durable page frontier, so after enough branch writes the same refusal
  appears there.

So every relation's FSM (and any VM page that is dirtied without a WAL
record) stops being writable on a `route_all` compute as soon as one
fork-metadata compaction has run past the fork's definition, which on a busy
store is a matter of an hour.  The soak test never sees it because its
workload writes every page with a real LSN; the integration scripts are too
short for a compaction cutover.

Fix direction (not done here -- it touches the ordered-record and recovery
partition invariants of R4b and needs the design owner): the marker of an
ordered record that does not grow the fork carries no size information, only
ordering against same-LSN definitive events; once the cutoff is above the
floor no same-LSN event can be admitted after it, so such a record could be
stamped at `max(floor, cutoff_lsn)` (making it "future" by construction)
or admitted without a marker.  A growth below the cutoff is the harder case.
Whatever the fix, a unit test that defines a fork, drives a fork-metadata
cutover past it, and then rewrites an existing block with `pd_lsn = 0` is the
fail-before evidence.

**E-7. Branch preparation intermittently finds no control image for the
shutdown checkpoint it forks from** (local seed 42, scale 1, `--max-branches
1`, third branch; open).  The controller stops the writer and forks from its
shutdown checkpoint (`lsn = redo = 0/40000028`, the first record of a fresh
segment), restarts it on a private socket, and
`pagestore_prepare_branch_from_control()` fails four seconds later -- well
inside its 10 s horizon timeout -- with `branch checkpoint redo is not
exactly mirrored`.  So the newest mirrored control image at or below
`0/40000028` still carried an older redo: the shutdown checkpoint's image,
written by the exiting checkpointer and shipped only by
`ps_control_exit_drain()` with bounded waits, did not reach the store (or not
at that LSN).  The two earlier branches of the same run took the identical
path (`0/14000028`, `0/2F000028`) and passed; the second, like the third,
directly followed the deletion of the retired branch.  The next step is to
count `ps_control_dropped` and the exit drain's outcome, and to read the
control object's versions on the preserved store.

**E-8. `pagestore_inspect health` reports a dead daemon's segment as
ready** (CI seed 1, round 5; worked around in the driver).  A daemon killed
with `SIGKILL` leaves its shared-memory object with a valid header and
`startup_state = READY`.  The next daemon reuses the object (`O_CREAT`
without unlinking) and re-initializes it under lock byte zero, but `health`
maps the object read-only, checks only the header, and never probes the
daemon lease on byte one.  A readiness probe issued between the new daemon's
launch and its initialization therefore succeeds against the stale header;
the writer then attached mid-initialization and failed with `pagestore
localsvc shared memory incompatible ... magic=0x0`.  Any orchestrator that
restarts the daemon after a crash and waits on `health` has the same race.
Fix direction: `health` (and every client attach) should require byte one
to be held by a live daemon, or the daemon should unlink and recreate the
object.  The driver now removes the stale object before each daemon start.

Also needed: the daemon should log one line for every refused or failed
READV/WRITEV/BEGIN_DELETE (opcode, timeline, key, block, reason), as it
already does for artifact refusals.  E-3 and E-6 each took a preserved store
and a debugger session to explain what one log line would have said.

## Not covered yet

- **Bounded space.**  Retired branches are deleted through
  `pagestore_delete_branch()` (extension 1.3) and the driver requires the
  store to reach `deleted`; the store size before and after is recorded in
  `branch_deleted`, but no bound is asserted yet.  On a build without the
  function the timeline stays in the store.
- **Pinned and advancing readers.**  Their provisioning (reader base,
  prepare/install, retention owner) is only scripted inside
  `integration_test.sh`.
- **The materializer supervisor.**  The driver restarts the materializer
  itself; `pagestore_materializer_supervisor` should own it.
- **Storage faults** (disk full, write/fsync errors, slow device) and the
  named fault points under load.
- **Power loss.**  Process kills leave the OS page cache intact.
- **Leak verdicts.**  RSS, descriptors and store size are recorded, not yet
  judged.
- Branches of branches, user tablespaces, more than one database.
