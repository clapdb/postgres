# Pagestore release validation

## Assessment and scope

Assessment date: 2026-09-13. The reviewed development revision is
`24d8ff282a4` on `pagestore`, including PR #260.

The existing tests provide substantial evidence for a restricted developer
preview of the default-tablespace local POSIX MVP. They do not yet establish
production release readiness. Meeting the MVP implementation gates and
qualifying a supported release are separate decisions.

[`MVP_STATUS.md`](MVP_STATUS.md) remains the source of truth for the accepted
MVP scope, and [`MVP_COMPLETION_PLAN.md`](MVP_COMPLETION_PLAN.md) defines its
completion gates. The additional acceptance work below is proposed release
qualification, not a retroactive expansion of those gates. It has not been
executed or approved merely by being listed here. S3, SPDK layer recovery,
multi-writer timelines and user-tablespace bootstrap remain outside this MVP.

## Existing evidence

| Area | Evidence | What it establishes |
|---|---|---|
| Real PostgreSQL data path | `integration_test.sh`, `mvp_golden_test.sh`, `branch_boot_test.sh`, WAL redo demos | WAL-only ingest, continuous materialization, portable independent branch boot, fork-point visibility, parent/child writes and restarts |
| Process-crash recovery | H1 harness scenarios and focused crash matrices | Recovery at declared layer, manifest, pruning, WAL-index, WAL reclaim, timeline deletion, forkmeta, materializer and branch-controller boundaries; state and restart-idempotence assertions |
| Concurrent publication | Forkmeta crash matrix and acknowledged-append ledger | Acknowledged events survive the covered publication boundaries exactly once |
| Persisted formats | Store, PGDATA and controller fixtures | Supported loading and declared rejection of unknown versions, corruption and truncation through the relevant loaders |
| Bounded space | `pagestore_soak_test.c` and nightly seed matrix | Retained-view correctness and per-category physical bounds under the configured daemon workload, plus reclamation after ingestion stops |

The reviewed revision's [pagestore CI run](https://github.com/clapdb/postgres/actions/runs/34728309591)
passed both standalone and in-engine integration jobs. The latter includes
the golden scenario and branch boot. This is an existing CI result, not a new
local execution performed for this assessment.

The first [scheduled nightly run](https://github.com/clapdb/postgres/actions/runs/34681956749),
on 2026-09-12, passed seeds `20260909`, `7` and `4242`, each at 8000 rounds:

| Seed | Checks | Failures |
|---|---:|---:|
| 20260909 | 44224 | 0 |
| 7 | 44179 | 0 |
| 4242 | 44135 | 0 |

All three reports passed their during-run and quiescent space bounds. Their
workload execution lasted approximately 38–68 seconds per seed. The soak's
configured maximum live relation data is `6 * 48 * 8192` bytes (2.25 MiB), with
two readers and two reusable branch IDs. It drives a real daemon through a
freestanding C client; it does not run PostgreSQL computes. This is useful
bounded-history and recovery evidence, but neither a multi-day endurance test
nor a large-data PostgreSQL test. The scheduled run also predates the reviewed
revision; its result must not be attributed to the final release candidate.

Two later scheduled runs, [34747373574](https://github.com/clapdb/postgres/actions/runs/34747373574)
(2026-09-13) and [34825221247](https://github.com/clapdb/postgres/actions/runs/34825221247)
(2026-09-14), failed seed `20260909`'s `wal` during-bound check by a few KiB
(4698112 against the then-current bound of 4685824), both at the sample
between rounds 6400 and 6500; the other two seeds passed in both runs. The
failure is explained, not a new regression: physical shipped WAL in this
workload was governed by how often the WAL reclaimer's raw WAL-index
dependency floor moved rather than by the WAL controller's proven-lag
high-water, and nothing requested a WAL-index compaction on the reclaimer's
own behalf, so a fully proven segment waited on the WAL-index controller's own
~1000-round cadence; a fixed one-second no-progress backoff, uncancelled by
the publication that unblocked it, added up to another 237 rounds on the
hosted runner.  The fix (`wal_segment_reclaim_one` requests an on-demand
compacted WAL-index publication for a segment blocked only by the stale raw
floor, and the no-progress backoff is cancelled by a proof-epoch bump on
retention/WAL-index-publish/GC/progress/timeline-delete events, not only by
the clock) closes it: the soak's `wal` bound is restated from five declared
terms (18 KiB tighter, 4667392) with a per-sample check tying physical WAL to
the soak's own fences, and seed 20260909 at 8000 rounds now peaks at ~1.75 MiB
across repeated local runs, plain and CPU-contended.  The first post-fix
nightly dispatch is run <PAGESTORE_NIGHTLY_POSTFIX_RUN_ID> (to be filled in
after this change merges to `pagestore` and the nightly lane runs against it).

## Gaps in release evidence

1. **Duration and scale.** Repeated short runs cannot establish multi-day
   stability, resource-leak behavior, or behavior with data exceeding caches.
2. **Composed PostgreSQL endurance.** Real SQL scenarios and the daemon soak
   are separate. A sustained writer/materializer/branch/reader topology under
   concurrent SQL and maintenance still needs a retained validation report.
3. **Concurrency breadth and independent oracles.** Focused publication
   races do not establish correctness for arbitrary combinations of DDL,
   transactions, VACUUM, reader advancement, branching and GC. Add comparisons
   against independently derived SQL results at explicit visibility boundaries.
4. **Faults under sustained load.** Existing fault and backpressure tests are
   valuable; release qualification must compose them with the real PG workload
   and verify progress after each injected condition is removed.
5. **Durability model.** Current POSIX coverage establishes process-crash
   recovery under declared fsync/rename assumptions. Killing a process leaves
   the OS page cache alive and does not certify machine power-loss durability.
6. **Release-major coverage.** Development-branch success does not qualify a
   `branchdb_N` build. At assessment time, remote `branchdb_13` through
   `branchdb_19` still pointed to the early LSM metadata foundation. Each
   advertised major needs the implementation and matching native-payload
   fixtures. The existing workflow requires a matching store fixture on
   `pagestore`, but permits a missing build match on release branches; a release
   acceptance run must require that match rather than accept the warning.

The two artifact lifecycle gaps identified in the reviewed baseline are
addressed by the follow-up described in [`ARTIFACT_LIFECYCLE.md`](ARTIFACT_LIFECYCLE.md):
completion records prevent partial generations from superseding complete ones,
and durable drops allow removed objects' data to be reclaimed after retained
dependencies disappear. The follow-up adds deterministic validation; it does
not replace the proposed endurance work below. Small lifecycle metadata
remains per object identity, so arbitrary distinct-key churn still needs its
own resource assessment. The CI links above describe the original reviewed
revision, not qualification of the follow-up release candidate.

## Proposed acceptance work

All items below are pending as release-qualification evidence. The durations
are proposed starting thresholds, not a mathematical reliability guarantee.
Choose and record resource budgets and recovery deadlines before each run.

### V1. Real PostgreSQL endurance

Run the selected release candidate for at least 24 hours before a broader
preview and 72 hours before production qualification, using multiple recorded
seeds. Provision a WAL-only writer, managed materializer, fixed and advancing
readers, and repeatedly created, written and deleted branch computes. Use
multiple SQL sessions and data larger than the configured caches. Exercise
transactions, large/TOAST values, indexes, supported DDL and VACUUM alongside
checkpoints, reader changes and maintenance.

Pass requires correct retained and latest views, bounded disk usage relative
to declared live data and pins, no unexplained exits or hangs, and no sustained
unexplained RSS or file-descriptor growth. After ingestion stops, lag and
reclamation debt must meet predeclared catch-up deadlines. Preserve resource
time series; a final low-water reading alone is insufficient.

### V2. Independent SQL correctness

Replay deterministic transactions against ordinary PostgreSQL or maintain an
independent expected-state model. Compare full results or canonical checksums
at declared commit and fork boundaries, including branch divergence and pinned
reader visibility. Concurrent schedules must record their committed outcomes;
different legal transaction orders must not be mistaken for corruption.
Check indexes with supported `amcheck` operations and compare indexed access
with reference query results where applicable.

Pass requires zero unexplained mismatches before and after recovery. Include
aborts, prepared transactions, relation truncate/drop/recreate and supported
index/DDL lifecycle operations. Retain failing seeds and minimized schedules.

### V3. Recovery and overload in the composed workload

During V1, inject compute and materializer crashes, and daemon crashes at
supported coordinated shutdown/restart boundaries. The MVP requires all
attached clients to be stopped before a daemon restart; do not silently extend
that contract to transparent restart with live clients. Also exercise disk
full, write/fsync errors, slow storage and stalled materialization. Record
which named faults were reached and which acknowledged operations preceded
them. Declared deterministic publication probes should retain their focused
coverage alongside these workload-level failures.

Pass requires no loss of operations acknowledged as durable under the declared
contract, correct retained views, no admission below reclaimed frontiers, and
bounded backpressure or explicit errors. After removing a fault, require
recovery and catch-up within the recorded deadline and a second successful
restart. An expected error must not mask a dead process or an unreached fault.

If the release promises power-loss durability, add VM/block-device fault tests
that model loss of volatile writes on the supported filesystem/storage stack;
record flush assumptions separately from process-crash results.

### V4. Candidate and delivery validation

Select the first supported PG major and freeze an exact candidate SHA. Run
the complete relevant CI suites, golden/branch boot scenarios, multi-seed soak
and V1–V3 against that candidate. Capture matching PostgreSQL payload fixtures
and require `--require-build-match` for the store fixture check. Validate
supported older formats and refusal of unsupported ones.

On a clean deployment, follow the published installation and branch-creation
instructions. For production qualification, also rehearse the documented
backup/restore and upgrade/recovery procedures and verify SQL contents after
restore. Any claimed rollback path must be tested against its persisted-format
constraints. Only advertise PG majors and deployment configurations with their
own passing evidence.

## Evidence required for sign-off

For each acceptance run, retain the candidate SHA, PG identity, build options,
OS/filesystem/storage configuration, cache and resource limits, topology, seed,
wall-clock duration, workload sizes, fault reachability, oracle results,
resource time series and recovery times. Link CI runs and durable copies of
their reports from the release record; nightly artifacts currently expire
after 30 days.

An unexplained failure remains open even if a retry passes. A harness/setup
failure requires a valid replacement run, not a product pass. Changes after
qualification require an explicit assessment of which evidence to rerun.

The release record should distinguish:

- **Restricted developer preview:** existing functional coverage plus a
  qualified candidate build, reproducible setup and explicit operating limits;
  broader preview endurance is proposed in V1.
- **Production release:** passing candidate-specific V1–V4 evidence, resolved
  or explicitly constrained known limitations, and tested operational recovery
  procedures within the advertised durability model.

No production sign-off is implied by the current assessment.

## Additional recovery finding during PR #262 review

A cassert integration run passed with the final artifact publication producers,
but reopening its retained store after shutdown failed in
`recover_layer_prefix()` / `replay_page_record()`. The failing tuple was a
relation-class FSM page (`spcOid=1664`, `dbOid=0`, `relNumber=2396`, `forkNum=1`,
block 0), with a nonzero ordered-write identity. Its recovered fork history
contained plain GROW events with zero order IDs, so `fork_event_activate_seg()`
correctly refused the missing identity. The artifact lifecycle fixtures and
focused restart tests passed; they do not cover this full integration tail.

The failure was reproduced on the unmodified `pagestore` baseline
`2b2349887c2` in an isolated cassert build: its integration script passed, then
its own daemon failed reopening the retained store. Debugging found the same
FSM key and block, LSN 318767144 and order ID 2; the recovered GROW events again
had zero order IDs. This predates the artifact changes. Diagnosis and repair
of the lost ordering marker remain separate release work. Treat independent
reopen of the full integration store as a release blocker; do not generalize
the script's PASS into a claim that this additional recovery check passed.

Reproduce with `KEEPTMP=1 contrib/pagestore/integration_test.sh <build>`, then
start `<build>/contrib/pagestore/pagestore_daemon` on the reported retained
STORE after the script has stopped its processes. Preserve that store for
forensic checks. Refresh `<build>/tmp_install` with the setup suite after
rebuilding PostgreSQL producers, so the script loads the intended extension.

## Integration-lane finding: a silent artifact key collision (PRs #264/#265/#266)

Three independent PRs (#264, #265, and #266, which only added the CI log-dump
diagnostics below) failed the integration lane identically: `ERROR: pagestore
localsvc: daemon reported error for op 34` immediately followed by `FAIL -
reader snapshot publishes as a multi-block page-store artifact`, with no
daemon or server log lines in the job output to say why op 34 (`ARTIFACT_
BEGIN`) was refused. The base branch had passed once, and every local run
passed, which made this look environment-dependent; it is not.

**Root cause.** Two producers publish generations of the same page-store
artifact key (klass `PS_KLASS_READER_SNAPSHOT`, spc 0, db `InvalidOid`, rel
`PS_READER_SNAPSHOT_DATA_OBJECT` = 1, fork 0): the automatic
checkpoint-driven reader-artifact worker (`pagestore.auto_reader_artifacts =
on`, one generation published at every checkpoint redo) and the explicit
exact-R snapshot (`pagestore_publish_reader_snapshot_artifact`, published at
a retention owner's pinned R). The artifact lifecycle admits only monotonic
generations per key (`ps_artifact_begin`'s `artifact_mutation_horizon()` and
commit-block checks), so once the automatic worker had already published a
generation at a checkpoint redo newer than R, the explicit publish's BEGIN at
R was refused -- and refused silently: the daemon mapped the refusal straight
to `PS_STATUS_ERROR` with no log line, and the client reported only the
opcode number. On a loaded CI runner the automatic worker's first cycle after
a restart lands inside the roughly one-second window between the post-R
checkpoint and the explicit publish; locally that cycle either runs before
the checkpoint (nothing reserved yet) or five seconds after the publish, so
the collision never landed there. This is the entire CI-vs-local difference;
no forkmeta-cutoff or worker-poisoning hypothesis from an earlier pass at this
failure was the cause (both are withdrawn).

**Invariant.** One producer per artifact key, with monotonic generations per
key; an artifact published at a controller-chosen LSN (an exact-R reader
snapshot, R pinned by a retention owner) must live under a key owned by that
request, never under the key of a checkpoint-driven producer.

**Fix.**
- The explicit exact-R publish and its loader use an owner-scoped key for the
  DATA object (`dbOid` = the retention owner id that pinned R); the automatic
  checkpoint snapshot keeps `dbOid = InvalidOid`. The owner id is
  range-checked to fit `dbOid`'s 32 bits, not truncated:
  `pagestore_publish_reader_snapshot_artifact` errors if it is not a positive
  value `<= 2^32 - 1` (owner 0 is rejected outright -- it would alias the
  automatic key); `pagestore_validate_published_reader_snapshot` additionally
  accepts 0, meaning "look up the automatic snapshot instead of an owner's".
  A controller that assigns 64-bit owner ids with the high bit set therefore
  cannot publish an exact-R snapshot for that owner under the current 32-bit
  `dbOid` encoding.
- The manifest does **not** reuse the automatic path's MANIFEST object
  (`PS_READER_SNAPSHOT_MANIFEST_OBJECT`, object 0) with the owner id in its
  `dbOid` slot: that slot is the automatic per-database manifest's own
  namespace -- real database OIDs, written by the reader-artifact database
  workers at every checkpoint redo and tracked/retired by the reader database
  barrier (`pagestore.c`'s `pagestore_publish_database_reader_manifest`,
  `pagestore_reader_database_dir_valid`, and the barrier's per-database
  drop). An owner id that happened to equal a live database's OID would
  alias that database's manifest slot and could be dropped by the barrier's
  cleanup of databases no longer in the catalog set. The explicit publish's
  manifest instead uses a new, dedicated object,
  `PS_READER_SNAPSHOT_OWNER_MANIFEST_OBJECT`, keyed by the same owner id --
  an additive namespace entry alongside MANIFEST/DATA/READY/RELMAP/
  DATABASE_BARRIER, not a change to any of them.
- The explicit path no longer publishes the database-independent "global"
  manifest at `InvalidOid` either -- nothing in the pinned-reader boot path
  reads that fallback (it exists only for an advancing reader adopting the
  automatic snapshot before `MyDatabaseId` is known at early backend init),
  and publishing there re-opened the identical DATA-object collision one
  field over.
- Every `ps_artifact_begin`/`commit`/`drop` refusal now reports a reason
  (`PsArtifactRefuseReason`, `pagestore_artifact_format.h`, append-only)
  through an out-parameter. `pagestore_daemon.c` logs one stderr line per
  refusal (`pagestore_daemon: artifact BEGIN refused: reason=... timeline=...
  key=(...) lsn=... last_page_lsn=... last_commit=...`) and returns the
  reason in `ch->result`; `backend_localsvc.c` appends it to the `ERROR` it
  raises. When the daemon refuses an artifact op before
  `ps_artifact_begin`/`commit`/`drop` even runs -- the opcode/klass gate in
  `pagestore_daemon.c`'s `handle_request()`, or the timeline-incarnation gate
  in `ps_handle_meta()` -- `ch->result` stays 0
  (`PS_ARTIFACT_REFUSE_NONE`), which is reported as "refused before admission
  (timeline/klass gate)", not a bare "none". A silent refusal was itself the
  diagnostic bug: with this in place, the original CI failure would have
  named its cause in the same run.
- `integration_test.sh`'s reader section now forces the exact collision order
  CI hit instead of racing on worker timing: after the post-R checkpoint, it
  polls `pagestore_validate_checkpoint_reader_snapshot()` at that checkpoint's
  redo until the automatic generation exists, *then* runs the explicit
  publish at R -- and asserts the daemon log contains no `artifact ...
  refused` line at the end of a passing run. This block *is* the regression
  test for the key split: reverting `pagestore.c`'s owner-scoped key back to
  `InvalidOid` (as base had it) makes it fail deterministically, the explicit
  publish's `ERROR: pagestore localsvc: daemon reported error for op 34
  (artifact begin: newer generation exists)` -- confirmed by reverting and
  rerunning.

Format/identity impact: none. The key split and the new OWNER_MANIFEST_OBJECT
change which `dbOid`/object an artifact is stored under, not any on-disk
payload layout or magic/version identity, so no fixture regeneration was
needed (verified: the persisted-format, pgdata-artifact, and controller
fixture checks below -- including `--require-build-match` -- still pass
unchanged).

Unit coverage: `pagestore_artifact_lifecycle_test.c`'s
`test_reader_snapshot_owner_key_split()` exercises the refusal-reason
plumbing (R5-2) and pins the per-`dbOid` producer independence the key split
(R5-1) relies on -- BEGIN on a second key that differs only in `dbOid`
succeeds at an older LSN despite a completed generation on the first (and
reads back exactly), while a same-key BEGIN at that LSN is refused with a
reason naming an older/superseded generation instead of a silent -1. This is
*not* a regression test for the base bug: `ps_artifact_begin`'s per-key
independence (two different `dbOid` values are unrelated keys) was never
broken -- the bug was entirely `pagestore.c` choosing the *same* key
(`InvalidOid`) for both producers, one call site up from anything this unit
test reaches -- so it cannot fail on an unfixed base. The integration block
above is the test that does.

## Open follow-up work from the artifact-key-collision fix (R5-5, not blocking)

- **Admission-refusal poisoning.** `artifact_store_record()` sets
  `artifact_io_failed` on any storage/admission failure
  (`pagestore_artifact_lifecycle.inc`'s `append_page_raw` callers), which
  then refuses every subsequent artifact BEGIN/COMMIT/DROP for the rest of
  the daemon's lifetime (`PS_ARTIFACT_REFUSE_STORE_RECORD`/
  `PS_ARTIFACT_REFUSE_POISONED` now make this visible instead of a bare -1;
  it was always the behavior). Whether one transient admission refusal should
  poison the whole process needs its own semantics review.
- **forkmeta-cutoff vs. fenced-artifact refusal.** `append_page_impl`'s
  page-prune-frontier fence for `PS_KLASS_SLRU`/`PS_KLASS_READER_SNAPSHOT`
  objects can refuse a late-shipped artifact at or below the cutoff even when
  its key and generation ordering are otherwise fine -- a real, separate
  hazard from the key collision fixed here (surfaced writing this fix's own
  unit test: its LSNs had to be chosen comfortably ahead of the page-prune
  frontier `pagestore_artifact_lifecycle_test.c`'s other tests had already
  advanced, or the fence refused them). Needs a reproducer and its own fix.
