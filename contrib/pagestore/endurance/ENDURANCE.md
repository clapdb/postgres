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

## Topology

The provisioning mirrors `mvp_golden_test.sh` step for step, so a failure here
that the golden scenario does not show is caused by load, duration or
interleaving, not by a different setup:

- a WAL-only **writer** (`route_all = off`, `archive_library = 'pagestore'`);
- a continuous **materializer** (hot standby, `route_all = on`, 32 MB of shared
  buffers so that most reads come from the store);
- up to `--max-branches` **branch computes**, each created by the installed
  `pagestore_branch_prepare` controller with
  `--verify-seed-against-materializer`, booted the portable way
  (`pagestore_control_restore --archive-bootstrap`, then
  `pagestore_install_prepared_branch_bootstrap`);
- one `pagestore_daemon` on a POSIX store.

One run is one topology lifetime (`--duration`).  `--forever` runs seed,
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
   immediate stop, `SIGKILL` of its postmaster, a forced restartpoint; writer
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
| `sum(bal)` equals its initial value on every compute | Workload invariant | Torn or duplicated transaction effects (covers 2PC and aborts) |
| Index scan vs sequential scan for one `ev.k` | Same compute | Index and heap disagree |
| `bt_index_check(idx, heapallindexed => true)` on every btree | amcheck | Structural index corruption |
| The controller's seeded SLRU pages vs the materializer's | PostgreSQL recovery | `clog`/`commit_ts`/`multixact` appliers diverge from redo |
| Log scan: `PANIC`, `TRAP`, invalid page, unreadable block, signals 6/7/11 | -- | Assertion or crash anywhere |
| Liveness: every wait has a deadline; daemon and postmasters must be alive when expected | -- | Hang or unexplained exit |

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
schedule, not necessarily the failure.  Rule from `RELEASE_VALIDATION.md`: an
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
