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
hosted runner.  The fix closes it: `wal_segment_reclaim_one` requests an
on-demand, fence-keyed WAL-index compaction for a segment blocked only by
the stale raw dependency (re-issued when the raw floor or a
retention-registry fence changes, not on every durable progress op, which
the backend materializer publishes once per indexing batch and which alone
can never retire the blocking item), and the no-progress backoff is
cancelled by a proof-epoch bump on the same events, rate limited to a 20 ms
floor so a retention pin drop -- dispatched without the admission lock --
cannot turn drop-heavy churn into a drain storm.  Two review rounds on the
fix itself found and closed four more gaps before merge: a lost-wakeup
ordering bug in the backoff's epoch read; the missing 20 ms rate limit; the
request needing to be fence-keyed rather than progress-keyed (plus two
spec bugs found validating that redesign: the wrong "never requested"
sentinel, and comparing `retention_effective_floor`'s raw value instead of a
dedicated fence epoch); and, in the second round, WAL_INDEX-only pin changes
not bumping either epoch, and a stale `walidx_snapshot_end[tl] < progress`
guard that wrongly assumed a publication already covering current progress
could never drop more.  The soak's `wal` bound is restated from five
declared terms (18 KiB tighter, 4667392) with a per-sample check tying
physical WAL to the soak's own fences, and seed 20260909 at 8000 rounds now
peaks at ~1.7-1.8 MiB across repeated local runs, plain and CPU-contended.
The first post-fix nightly dispatch is
[run 35458043758](https://github.com/clapdb/postgres/actions/runs/35458043758),
manually dispatched on 2026-09-19 against `316401d8d4b` (the #264 merge
commit on `pagestore`, with this fix -- #265 -- already merged in its
ancestry): seed 20260909 at 8000 rounds passed with 45024 checks, 0 failures,
and both the during-run and quiescent bounds satisfied; physical WAL peaked
at 1736704 bytes against the 4667392 bound, with `wal_fence_slack_max` at
1568768 bytes, inside the ~2.1 MiB expected range from the previous section.
This is one dispatched run against the fix, not yet a scheduled-run history:
the three-seed scheduled nightlies must still accumulate green runs against
this revision before the final MVP status update.

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

## Recovery finding during PR #262 review (resolved)

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
had zero order IDs. This predates the artifact changes.

**Diagnosis.** A live ordered page write (a WAL-less or below-floor "clamped"
relation page) durably persists a bound marker (`FEV_SEG_GROW_BOUND` or
`FEV_SEG_COMMIT_BOUND`, carrying its `order_id`) to the forkmeta source log,
but the *in-memory* fork history recorded it as a plain `FEV_GROW` with
`marker_kind = 0` and `order_id = 0` -- `append_page_impl()`'s live path called
the same `fork_grow_apply()` used for ordinary writes instead of inserting the
marker-plus-activation recovery itself would rebuild. The forkmeta snapshot
serializer builds a new generation from memory and compensates only by
rescuing markers still present in the *current* source log; the first cutover
after the write keeps the marker (copied from the source log) and resets the
source log to a bare epoch record, but a *second* cutover in the same daemon
lifetime finds the marker in neither memory nor the (already-reset) source log
and publishes a plain GROW. On the next open, the image-layer index still
carries the record's `order_id`; `fork_event_activate_seg()` finds no matching
marker, `recover_layer_prefix()` fails, and the daemon exits with a stale
`storage open: Invalid argument` (or whatever `errno` a prior syscall left
behind). Nothing in the persisted formats was wrong; the in-memory
representation had diverged from the one recovery reconstructs.

**Fix.** The live path now inserts exactly the representation recovery would
rebuild: `fork_event_add_seg_marker()` followed by `fork_event_activate_seg()`,
using the same growth/commit distinction as the durable
`fork_meta_persist_segment()` call a few lines above. This closes the live-path
bug (finding F1) outright: a store written entirely by the fixed daemon never
degrades its own marker to a plain GROW.

Recovery additionally gained an explicit, logged, **fail-closed** adoption
rule for an ordered record whose marker is missing from memory even though its
admission identity (`order_id`, `admission_seq`, both nonzero) survives on its
segment/image-layer record. The rule requires a proof, not just a nonzero
identity (review finding F2): the *selected forkmeta snapshot's freeze
sequence* must be at or above `admission_seq`
(`fork_meta_orphan_proven()`, `pagestore_core.c`). `fork_meta_snapshot_maintenance()`
computes that freeze sequence while holding the admission *write* lock, which
blocks until every in-flight append -- each holding the admission *read* lock
across the whole of `append_page_impl()` -- has returned; a failed marker
append poisons forkmeta, so a proven admission_seq's append had returned by
that freeze. **This alone is a NECESSARY condition, not sufficient proof
against a torn append (finding R2-F1, from independent re-review of F2/F3):**
a crash between a record's segment body write and its marker append also
returns via `_exit()`, and the refused record's `admission_seq` is still
observed (`admission_seq_observe()`/`segment_order_id_observe()`, kept
deliberately -- see below) on the very recovery pass that refuses it; a
*later* cutover in that same or a subsequent daemon lifetime then freezes at
or above the torn sequence, so the predicate alone would call a torn append
"proven" on any rescan after that cutover, not only the first one immediately
following the crash. The real torn-exclusion proof is structural and applied
separately by each of the two adoption sites below:

- **Growth class**: a plain `GROW` event with the record's exact `(lsn,
  admission_seq, nblocks)` is that record's own marker, degraded (the shape
  the F1 bug produces). It is promoted back into a bound marker; one
  `pagestore: adopting orphaned ordered record as bound marker ...` line is
  logged. Sound on both recovery paths unconditionally, with no look-ahead
  needed: a torn growth-class append never left a durable marker to begin
  with (the crash precedes any in-memory bookkeeping too), so no durable
  source can ever hold a degraded `GROW` at a torn sequence, and sequences
  are never reused (see below), so this rule has no torn-append exposure.
- **Commit class** (review finding F2, initially missed): a second below-floor
  or WAL-less rewrite of an already-sized block -- the FSM/VM pattern,
  rewritten at every checkpoint -- never left a plain `GROW` behind even
  before the live-path bug, because the in-memory apply short-circuits for a
  growth event that does not raise the fork's size. There is no degraded
  identity to promote for this shape, only a missing one, so the rule instead
  proves the record safe to admit **by size**: if the fork's size at this
  position already covers the block, an inert `FEV_SEG_COMMIT_BOUND` marker
  (the one recovery itself would have loaded) is inserted at its recorded
  position; one `pagestore: adopting orphaned ordered commit record as inert
  bound marker ...` line is logged. Unlike the growth rule, the freeze proof
  alone is not torn-safe here, so each recovery path supplies its own
  additional proof:
  - **Image-layer path** (`recover_layer_prefix()`): adopts directly.
    Residency is the proof -- a layer-resident record was staged into the
    memtable only after its marker append returned from an fsynced segment
    write, so a layer-resident record's marker append cannot still be in
    flight.
  - **Segment-suffix path** (`recover()`): does **not** adopt directly. It
    requires, in addition, that **at least one complete record follows the
    unmatched record in the same segment**. `append_page_impl()` advances a
    shard's append cursor only after the marker append for the *previous*
    record succeeded (and sets the segment-retired sentinel, `cur_off =
    segment_size`, on failure), and every append holds the shard write lock,
    so a torn body is always the last complete record of its segment and
    nothing can ever be appended after it there -- on any rescan, not only
    the first. `recover()` therefore stashes a refused, otherwise-adoptable
    commit-class record (at most one at a time) and keeps scanning; if the
    next record parses completely, the stashed one adopts (logging which
    offset proved it: `... followed by a complete record at offset N`); if
    the scan instead ends (any reason, including a clean end of written
    data) with the stash still unresolved, it is retired exactly as an
    unmatched, non-adoptable record would be, now saying so explicitly
    (`... no complete record follows in this segment`). A record that is
    last in its segment is retired unconditionally on the segment path,
    because it cannot be told apart from a torn append by any proof
    available at scan time, and the two possible identities it can have
    differ in what is lost: for a store written entirely by the fixed live
    path this is a genuine F3 survivor (see the F3 section below), so
    retiring it loses only an already-pruned version, nothing acknowledged;
    for a store written by the pre-fix daemon that crashed (no close-time
    flush) after two same-lifetime cutovers, it can instead be that record's
    own last acknowledged commit-class write (the F2 shape --
    `test_orphaned_commit_marker_segment_path_last_is_retired()` -- which is
    exactly why that test calls it torn-indistinguishable), so retiring it
    falls back to serving the previous version rather than losing data.

Why `admission_seq_observe()`/`segment_order_id_observe()` are kept for a
refused record, rather than removed to make the freeze proof itself
torn-safe: a post-crash retry of the same page write would then be allocated
the torn record's own sequence and, for a clamped rewrite of the same block,
the same growth LSN and (if the order-id observe were skipped too) the same
order id -- a full identity collision, where a later rescan could match the
torn body to the retry's legitimate marker and admit it as that record's
version. Identities must never be reused; keeping both calls is what makes
that true, at the cost of the freeze predicate alone no longer being a
complete proof -- which is exactly why the two adoption sites above supply
their own additional, path-specific proof instead of trusting it alone.

`recover_layer_prefix()` and `recover()` also now print the refused/retired
record's full tuple (and `recover_layer_prefix()` sets `errno = EINVAL`)
before failing, so a bare, stale-errno `storage open: Invalid argument` cannot
come back unexplained. No persisted format changed; `pagestore_format_versions`
output and all format fixtures are unaffected.

Precise scope of what "self-heals": a store whose only broken records are
growth-class orphans of the fixed live-path bug (F1) opens via adoption and
publishes proper markers at its next cutover -- this was the retained
integration store's failure and is now closed by the F1 fix, with F2 as an
independent generalization. A store carrying a *commit*-class orphan from the
pre-F2 code (F1 fixed, F2 not applied) could not adopt it and stayed refused;
F2 closes that gap, on the image-layer path unconditionally and on the
segment-suffix path when a complete record follows it. **Recovery adopts an
orphaned ordered record only when the selected forkmeta snapshot proves the
append completed AND either a plain GROW with the identical identity exists
(growth class, either path) or the fork's size at that position already
covers the block AND (residency on the image-layer path, or a following
complete record in the same segment on the segment-suffix path) (commit
class); every other unmatched ordered record is refused (layer prefix) or
retires the segment tail (segment suffix), with a logged tuple either way.**
A store written entirely by the fixed timeline-delete path (invariant I3:
segment bytes are immutable once written, so a rescan region is never
created) needs the adoption rule only for a store that deleted a timeline
before that fix -- see "Resolved: pruned ordered marker rescanned after a
timeline-delete rewrite (F3)" below.

`integration_test.sh` now stops every cluster and daemon it started, then
starts a fresh daemon against the same retained store and asserts it reopens
independently, with `ok - retained store reopens independently after clean
shutdown`, `ok - no orphaned ordered records were adopted on reopen` (invariant
I3: a fixed daemon's store never needs adoption, so nonzero here means either
a pre-fix deletion or a regression and needs investigation, not a retry),
`ok - no segment tail was retired on reopen`, and `ok - no ordered record was
refused on reopen` (both unconditionally fatal: the adoption rule is supposed
to turn every reachable case of either into a logged adoption instead).
Independent reopen of the full integration store is now part of the script's
PASS, not a separate manual step.

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

## Resolved follow-up work from the artifact-key-collision fix (R5-5)

Both items below were opened as follow-ups from the key-collision fix above
and are now resolved.

- **Admission-refusal poisoning.** `append_page_impl()` returned the same
  `-1` both when it refused to admit a request (nothing durable changed:
  a forked child, an exhausted admission allocator, an unfenced generation
  LSN, or growth ordered before the forkmeta snapshot cutoff) and when a
  storage write actually failed (bytes may be on disk). The lifecycle layer
  could not tell the two apart, so `artifact_store_record()` and
  `ps_artifact_write()` poisoned `artifact_io_failed` on *any* nonzero `rc`
  -- an admission refusal was indistinguishable from a real I/O failure, and
  poisoning fails every artifact BEGIN/COMMIT/DROP **and every artifact read
  on every key** until the daemon reopens (`artifact_record()`, the read-side
  validator, is gated by the same flag). One late, unfenced automatic
  reader-snapshot publication was enough to take the whole artifact path
  down: this is the exact shape the original CI failure hit (op 34/`BEGIN`
  on one attempt, op 11/`READ_AT` on another, plus a `WARNING: pagestore:
  automatic reader snapshot publication failed at ...` from the worker).
  Fix: a `PsAppendOutcome` out-parameter classifies every `append_page_impl`
  return site (`append_page_raw_outcome()`; the plain `append_page_raw()`
  wrapper keeps its old contract for every other caller). The lifecycle
  layer now poisons only when the outcome is a real storage I/O failure, or
  when an append succeeded but the sync that must follow it failed (the
  record is indexed in memory but not proven durable) -- exactly the cases
  where durable state may now differ from in-memory state. Every other
  refusal returns a named, non-poisoning `PsArtifactRefuseReason`
  (`PS_ARTIFACT_REFUSE_UNFENCED`, `_FORKMETA_CUTOFF`, `_SYNC`,
  `_LEGACY_BYPASS`, `_IMMUTABLE_MISMATCH`, appended to the existing
  append-only enum; no `PS_SHM_VERSION` bump, matching how #266 added the
  first set of reasons). `ps_artifact_write()` gained the same reason
  out-parameter WRITE never had; the daemon now logs a WRITE refusal and
  returns its reason in `ch->result` for `EXTEND`/`WRITEV` on an SLRU/
  reader-artifact key, and the client message carries
  klass/db/object/block/LSN/reason, so the reader-artifact worker WARNING
  sites get that context for free (they already log `edata->message`).
  Evidence: `test_admission_refusal_does_not_poison` (T1) and
  `test_io_failure_still_poisons` (T2) in
  `pagestore_artifact_lifecycle_test.c`, and
  `test_artifact_generation_vs_cutoff` (T5) in
  `pagestore_forkmeta_cutover_test.c`, each fail on an unfixed tree and pass
  on this one; `integration_test.sh` now also asserts `reason=poisoned` and
  `reason=storage failure` never appear in a passing run's daemon log.
- **forkmeta-cutoff vs. fenced-artifact refusal.** This is *not* a separate
  hazard from the key collision fixed above -- the hazard was the poisoning
  above; once that no longer poisons, a refused generation here was already
  the correct outcome, just misreported. The cutoff derivation chain proves
  it cannot conflict with a fenced artifact: the page-history retention
  floor is the minimum of every active same-timeline `PAGE_HISTORY` pin and
  the control checkpoint cutoff (`retention_effective_floor_internal`), the
  durable page frontier never exceeds that floor (`page_frontier_advance`),
  a new same-timeline pin below the frontier is refused by the daemon
  (`page_frontier_ancestry_allows`), and the forkmeta cutoff is the
  lexicographic minimum of every fork-owning timeline's frontier
  (`fork_meta_snapshot_cutoff`; a live child without its own durable
  frontier caps it at its branch point). So `cutoff <= frontier <= floor <=
  every active pin`: a generation at a pinned LSN is never below the cutoff,
  and at the cutoff LSN itself a fresh admission sequence is future and
  admitted. What *was* wrong: the artifact BEGIN/COMMIT/DROP lifecycle
  records were not fence-checked at all. Before a forkmeta cutover, a BEGIN
  at an unfenced LSN was admitted and its first data WRITE was refused
  instead (the write-side fence already existed and was correct); after a
  cutover, the same BEGIN was refused by the forkmeta growth check instead
  -- two different gates reporting the same rule, both as a bare
  `PS_ARTIFACT_REFUSE_STORE_RECORD` (or `-1`), and both poisoning before the
  fix above. Fix: `ps_artifact_begin` now checks the same fence predicate
  used by the data append (`artifact_lsn_fenced()`, extracted so both share
  it) before admitting a *new* generation -- ordered after the same-LSN
  completed-generation short-circuit, so an immutable re-ship of an
  already-published generation keeps working even once its LSN has fallen
  behind the frontier -- and refuses by name
  (`PS_ARTIFACT_REFUSE_UNFENCED`). The forkmeta growth check stays in place
  as defence in depth for the one remaining theoretical gap (a live child
  that already has its own durable frontier while the parent's frontier has
  passed the branch point; page-history retention does not treat a
  descendant's branch point as a floor, only as a fence) and is now reported
  by name too (`PS_ARTIFACT_REFUSE_FORKMETA_CUTOFF`) if it ever fires; no
  producer clamps its LSN and the cutoff derivation still does not consult
  retention owners, so R6's bounded space is unchanged. Evidence:
  `test_artifact_generation_vs_cutoff` (T5) and
  `test_artifact_forkmeta_cutoff_reason` (T6, which installs a pin below the
  cutoff through the raw retention registry -- bypassing the daemon's own
  admission gate -- specifically to exercise the theoretical gap) in
  `pagestore_forkmeta_cutover_test.c`.

## Resolved: pruned ordered marker rescanned after a timeline-delete rewrite (F3)

**Root cause.** The deletion rewrite moved bytes and retreated (rebased) the
watermark; everything below followed from that. Found during independent
review of the fix above.
Mechanism, confirmed from the code (not yet from a synthetic worst-case
reproduction beyond the regression tests below):

- `fork_meta_snapshot_build()` keeps a non-future marker only while
  `fork_meta_snapshot_marker_page_retained()` finds a live in-memory page
  version at the marker's admission sequence; otherwise a growth marker
  degrades to a plain `GROW` (still adoptable by identity, per F2 above) and a
  commit marker is dropped outright (nothing to adopt by identity; F2's size
  proof covers it instead).
- `page_remove_compacted_versions()` removes in-memory page versions once a
  compacted layer publishes. The segment record bytes are untouched. This is
  harmless on its own: the record stays below the durable flush watermark, and
  `recover_layer_prefix()` only replays the covered prefix from layer indexes,
  where the dropped identity no longer exists, so recovery never rescans it.
- `page_cleanup_rewrite_segment()` (timeline delete) rewrites a segment and,
  when bytes inside the covered prefix moved, rebases the shard's flush
  watermark to `(seg, 0)`. The next open then rescans the *whole* segment from
  offset 0 and meets that now-orphaned record: no marker in memory, no size
  covering it. Before this PR, `recover()` silently retired the rest of the
  segment (a commit-class record) or was incidentally saved by growth
  adoption once F1 existed (a growth-class record); after this PR, F2's
  generalized adoption rule turns the commit-class outcome into a logged
  pruning reversal too -- the pruned version becomes visible again, which is
  strictly better than discarding every record after it in the segment, but
  it is still stale data resurfacing, not the invariant holding.

**Q1 -- silent loss of acknowledged data, no crash required.** The same
rewrite has a second, worse consequence than F3's rescanned marker. Image
index entries of layers published *before* the rewrite still carry
survivors' old, higher offsets. Once any later flush moves the watermark
past the survivors' new (relocated, lower) offsets but not their stale
(pre-rewrite) ones, a survivor is covered by neither: its stale layer offset
is above the new watermark ("not covered", so `recover_layer_prefix()` skips
it), and its true offset is below the watermark (so `recover()`'s rescan
never reaches it either). `read_resolve_version()` finds nothing --
`$SP/f3/q1_test.c` reproduced this on the unmodified core (six target pages
then two survivors in one segment, delete the target timeline, append one
more survivor page after the rewrite, reopen: both survivors return `rc ==
0`), now folded into `test_timeline_delete_keeps_offsets()`
(`pagestore_forkmeta_cutover_test.c`). This is "drop a branch, write a page,
restart" silently losing already-acknowledged, already-flushed data -- worse
than F3, and the same root cause (moved bytes, rebased watermark).

**Invariant I3** (the one this PR restores): segment bytes are immutable once
written, a record's `(seg_id, seg_off)` never changes, and the flush
watermark never retreats; consequently a page version is removed from memory
only while its segment record is, and remains, below the durable flush
watermark -- equivalently, every record a recovery rescan can encounter has a
live in-memory identity, and therefore a retained marker. The deletion
rewrite's relocation and watermark rebase-to-zero violated both the
immutability premise and the "remains" half of the consequence; this PR
removes the rewrite (2c below) instead of compensating for what it moves.

**Retirement is logical, not physical** (a known property, not itself part of
this finding, but relevant to how it manifests): `recover()`'s retirement of
a segment tail -- whether the plain, pre-R2-F1 form or the last-in-segment
case the segment-path look-ahead (R2-F1) still retires -- only sets the
in-memory cursor sentinel (`cur_off = segment_size`); it never truncates or
overwrites the segment file. The retired bytes remain on disk exactly as
written, unreachable through the live index but not destroyed, until a later
flush moves the durable watermark past them (at which point a fresh open
never rescans that region again). Until then, **every** open re-rescans and
re-retires the same tail; this is why `test_torn_commit_append_never_adopted()`
must reopen a third time (lifetime 3, after an intervening cutover) to prove
the freeze proof alone would have let the torn record through, and why the
log line differs between the first refusal (no generation selected yet, so
`fork_meta_orphan_proven()` is trivially false and the record never reaches
the look-ahead stash) and every later one (`... no complete record follows in
this segment`). A follow-up that wants a retirement to become durable -- so a
later open does not have to re-derive it -- must do so by recording the
retire boundary in forkmeta or manifest metadata (a D5 format change with a
fixture), never by truncating or rewriting page bytes: physical truncation
during open would make a forensic or read-only reopen mutate the store, has
no safe same-id whole-segment replacement primitive to build it on any more
(the storage backends' `seg_rewrite` existed only for the pre-fix rewrite
path below and was removed as dead code once tombstoning replaced it), and
-- the reason this matters most for F3 specifically -- it would make F3's
data loss
irreversible: the records after an unmatched ordered record are acknowledged
data a root fix is meant to recover, and today they are only *logically*
discarded (unreachable through the index, but the bytes are still there for
that fix to find); truncating them would foreclose that.

**Evidence before the fix** (regression tests, `pagestore_forkmeta_cutover_test.c`,
against the unmodified `page_cleanup_rewrite_segment()`): `test_deletion_filtered_forkmeta`
asserted, after its final reopen, that the pinned sibling timeline's page
version at LSN 200 is still resolvable (`read_resolve_version(...) == 1 &&
ver == 200`); on the unmodified baseline this returned `rc == 0` -- the
version was silently gone. A commit-shape variant,
`test_deletion_filtered_forkmeta_commit_shape` (one extra WAL-less rewrite of
the same block before the pinned write, producing a commit-class orphan for
the rescan to meet), used to assert at least one adoption line (F2's
mitigation); it now asserts zero, since I3 means there is nothing left to
adopt. `test_torn_commit_append_never_adopted` (folding in the reviewer's
standalone `torn_test.c` reproduction, R2-F1) still proves the freeze proof
alone is not a torn-append exclusion, independent of I3: it crashes a
commit-class write after its segment body but before its marker, reopens a
third time (with an intervening cutover in between so the torn sequence is
frozen-covered), and asserts the tail is retired again -- with `... no
complete record follows in this segment` -- and the block still serves the
last acknowledged tag. `test_torn_growth_append_never_adopted` proves the
growth rule has no equivalent exposure on either lifetime. `$SP/f3/q1_test.c`
(Q1 above) is now `test_timeline_delete_keeps_offsets()`.

**How it was fixed.** `page_cleanup_rewrite_segment()` became
`page_cleanup_tombstone_segment()`: every target-timeline record is
overwritten in place with a hole record of identical size (body zeroed, only
the magic changed to one of three new `SEG_HOLE48/56/64_MAGIC` values, one
per header shape), instead of being dropped from a rebuilt replacement
buffer that every survivor after it then had to be copied into at a new
offset. No survivor is ever relocated, no image-index entry ever goes stale,
and the flush watermark is never rebased -- I3 holds by construction, not by
tracking which relocated versions are still live in memory. Space is
reclaimed the way any other covered-prefix segment already is: by segment GC
once the whole segment is below the watermark. This is a persisted-format
change (D5): the three hole magics are registered in
`pagestore_format_versions`; per D5 rules 2-3, `fixtures/posix-artifact-lifecycle`
(the format that shipped before this PR) was demoted to `role: legacy`
rather than recaptured in place, and `fixtures/posix-timeline-delete-holes`
is the new current-role fixture, capturing a deleted branch whose target
records are still memtable-resident (unflushed) at the extend daemon's clean
stop, so the holes land *above* the final flush watermark -- inside the
region a reopen actually rescans, which is what lets the fixture prove the
pre-fix daemon binary fails closed on it (`incompatible record magic
0x53454831`) instead of silently accepting the new format. See the D5 note
in `MVP_COMPLETION_PLAN.md` for the fixture-capture detail and the exact
downgrade behavior: an older daemon fails closed on a hole only when it
lies in that daemon's own rescan region (above its last flush watermark); a
hole below the watermark is invisible to an older `recover()`, which reopens
successfully but then stalls any later deletion that touches that segment in
DELETING (pass 1 has no path that expects a hole magic). Older fixtures
(everything at `role: legacy`) keep reopening unchanged: no hole magics
exist in a store written before this fix, and a rebased watermark left by a
pre-fix deletion is simply a valid (low, never retreating further) watermark
to the new daemon.

**SPDK is unaffected and remains unable to complete a timeline deletion,
unchanged by this PR.** `timeline_delete_page_cleanup_one()` and
`timeline_delete_publish_ready()` gate on `ps_storage->seg_write`, which SPDK
does implement, so this PR removes the previous (already stale, since
tombstoning never called `seg_rewrite`) blanket refusal on that backend.
Tombstoning's pass 1 still cannot get past its very first segment there:
`spdk_seg_size()` reports the backend's fixed `g_segsize` for every segment
regardless of how much of it actually holds records, so the validation scan
runs straight into the unwritten tail as if it were a torn record and fails
closed as malformed, forever retryable and never destructive. This is a
pre-existing SPDK gap, not introduced or widened by this PR, and SPDK is out
of the MVP deployment boundary per D6 in `MVP_COMPLETION_PLAN.md`.

**What is not yet fixed (follow-up, not blocking), and how little evidence
of it survives.** A store that already underwent a timeline deletion before
this fix may have lost survivors to Q1 (entries whose stale, pre-rewrite
offset sits above the now-rebased watermark): those versions are not
recovered by this PR, and plainly: a store that deleted a timeline under the
old daemon and was then flushed -- ordinary operation, not a rare condition
-- has already lost those versions, and nothing in this PR recovers them.

The `PS_MANIFEST_REBASE_FLUSH_WATERMARK` record `ps_manifest_rebase_flush_watermark()`
wrote for the old rewrite never carried the *pre*-rebase watermark to begin
with -- its payload is the same `PsFlushWatermark {shard, seg_id, seg_off}`
as an ordinary `SET_FLUSH_WATERMARK` record, just tagged with a different
opcode so a reader could tell a rebase happened at all. And even that tag is
not durable: `ps_manifest_compact()` (routine maintenance once the manifest
log has grown past the live layer count, `pagestore_core.c` ~19764) rewrites
every shard's current watermark out as a plain `SET_FLUSH_WATERMARK`
(`pagestore_manifest.c` ~1289), so the very next compaction after a pre-fix
deletion erases the last trace that a rebase ever happened. A prospective
repair tool (task T7 in the implementation plan) cannot reconstruct the lost
versions' identities from the manifest at all once that compaction has run;
it would have to fall back to scanning layer files directly for entries a
deletion's rebase could have invalidated, independent of manifest history,
or accept that a store past that point is simply unrepairable by inspection
and can only be diagnosed as *possibly* affected. T7 is tracked as a
separate PR with its own tests and a `pagestore_inspect` report so operators
can at least tell whether a store selected a rebased watermark at any
`ps_core_open()` in its history. Until then, a pre-fix deletion's F3 orphans
are still adopted by the rules above (unchanged, and documented as recovery
for pre-fix stores in `fork_event_adopt_orphaned_seg()`/`_commit_seg()`'s
header comments); do not remove that adoption code before a release that no
longer supports opening a store written by a pre-fix daemon.

## Resolved: linear event scans over inert commit markers, and their per-lifetime memory bound (F5)

Found during an earlier review. Every commit-class rewrite of a fork leaves
one inert `FEV_SEG_GROW_BOUND`/`FEV_SEG_COMMIT_BOUND`-derived event in memory;
the snapshot builder drops a commit marker once its version is pruned, so the
*durable* count is bounded by live versions, and (design B, below) the
*in-memory* count is now kept equal to it, once per cutover, instead of only
at the next restart. The cost was not memory (40
bytes/event) but algorithmic: `fork_event_activate_seg()`, both
`fork_event_adopt_orphaned_*()` rules, and
`fork_meta_snapshot_marker_present()` scanned a fork's event array linearly,
and the existing lsn-only bisection in `fork_asof_hop()`/
`fork_inheritance_fenced()` still walked every event at the fork's growth
floor LSN (every commit-class marker of a fork shares that one LSN) -- O(N)
per write, O(N^2) per fork at recovery and per cutover. FSM/VM forks of hot
tables are exactly the forks that accumulate these.

Fixed by indexing the array both insertion routines already kept in
`(lsn, admission_seq)` tuple order: `fork_event_lower_bound()`/
`upper_bound()` bisect on the tuple instead of LSN alone, `identity_range()`
narrows a lookup to its exact `(lsn, admission_seq)` group instead of the
whole array, and `fork_event_insert_pos()` bisects the insertion slot instead
of walking backward from the tail. A per-fork `nlegacy_seq` counter (events
with `admission_seq == 0`, i.e. legacy V1 records) gates the index: a legacy
event is pinned at the end of its LSN run at insertion time and later events
never pass it, so a fork holding one is not tuple-ordered inside a run and
keeps the exact old linear code as a fallback (proven equal to it by a
randomized self-test, `ps_test_fork_event_index_selftest()`, cross-checking
both paths including long equal-LSN runs, equal-tuple duplicates, activation,
and zero-seq legacy events). A deterministic step-counter guard
(`ps_test_fork_event_scan_steps()`) makes the regression reproducible without
a wall clock: a K=5,000-rewrite case asserts fewer than K*128 scan steps for
the writes, the cutover, and the reopen (reopening with a raised
`flush_pages` first, so the case's own cost is dominated by the index, not
by a memtable flush -- and one image layer's worth of fsyncs -- on every
single rewrite), and fails deterministically on the unmodified algorithm
(measured: 25,010,000 steps for the writes, 12,507,500 for the cutover,
12,507,501 for the reopen -- all ~K^2 or ~K^2/2, versus the 640,000
ceiling). The wall clock is logged, not asserted (0.25 s on tmpfs, 2.5 s on
a disk-backed directory for the whole case); the step counts are the actual
guard.

Measured on the microbenchmark (one fork, K commit-class WAL-less rewrites of
one block; `contrib/pagestore/fev_bench.c`, tmpfs, `cc -O2`):

| K | live path (avg us/write) | cutover | reopen after cutover |
|---|---|---|---|
| 10,000 | 17.9 -> 13.9 | 0.046 s -> 0.028 s | 0.140 s -> 0.106 s |
| 50,000 | 41.7 -> 11.9 | 0.536 s -> 0.143 s | 1.052 s -> 0.451 s |

The live path is now flat in N (the residual per-write cost is the marker
fsync, the segment write, and the periodic memtable flush); cutover and
reopen now scale linearly in K, the remainder being the source-log read/CRC
and image-layer CRC verification. Real workloads are far from the old knee:
`integration_test.sh` peaks at 104 inert markers on one fork (pg_proc's main
fork) and 1641 events on the busiest fork (a growing table, already
O(log N) reads even before this fix); `mvp_golden_test.sh` peaks at 2
markers on one fork.

No persisted-format change from the index (design A); the array's *contents*
and order are unchanged, only how they are searched.

### Design B: bounding the array per daemon lifetime

The index above made every *lookup* O(log N), but the array itself still grew
without bound within one daemon lifetime: a successful cutover already
computes, per event, whether it survives into the new checkpoint/tail (the
snapshot builder's per-entry loop), but the in-memory copy kept every event
regardless -- "keep the in-memory chain conservative until the next restart"
was the comment at the call site. Design B removes that gap: the builder's
per-entry loop now also stamps a spare `ForkEvent.snapshot_dropped` byte (0 on
every branch that emits the event into the checkpoint or tail, 1 on the
`else continue;` branch that drops it), and
`fork_event_compact_dropped_markers()`, called once right after a successful
publish (under the same admission/shard/prune/map lock set that serialized
the build), removes every event with `snapshot_dropped && kind > FEV_DEAD`
(an inert marker that was never activated to a size event) from each fork's
array in place, decrementing `nlegacy_seq` and rebuilding `def_idx` as it
goes. Deleting forks (excluded from the build wholesale) and
`preserve_survivors` generations (every record re-emitted, nothing flagged)
are untouched by construction: neither ever gets the flag set to 1, so
compaction is a no-op for them. Recovery-equivalent by construction: the
events dropped from memory are exactly the events the durable checkpoint no
longer carries, so the in-memory array after a live cutover equals what the
next boot rebuilds from that checkpoint -- the merged invariant that a live
operation's in-memory state must match recovery's (see `MVP_STATUS.md`).

No persisted-format change. Tests (`pagestore_forkmeta_cutover_test.c`):
`test_inert_markers_compacted_after_cutover` (12 live writes leave 11 inert
markers in memory; after a reclaiming cutover the in-memory count drops to
match the durable snapshot's, and a reopen's in-memory event/inert counts
match the post-compaction in-memory counts exactly -- the recovery
equivalence check; marker count is deliberately excluded from that
comparison, see the test's own comment: an activated growth marker's
`marker_kind` stays set in memory for the rest of the daemon lifetime by
design, but the durable checkpoint record for it is an ordinary plain-GROW
record with no marker identity, a pre-existing asymmetry design B does not
touch; fails before this PR, since the in-memory count stayed at 11 forever
in that process), `test_inert_markers_kept_when_retained` (no
version reclaimed -> compaction is a no-op), `test_inert_markers_kept_on_preserve_survivors`
(a deletion-forced generation with no provable operational cutoff -> every
record re-emitted, compaction is a no-op), a `compact` phase added to the
randomized index self-test (flags a random subset of a private fork's inert
markers, runs the compaction routine, and cross-checks array order,
`nlegacy_seq`, `def_idx`, and every surviving event's own fields including
its `cached_*` triple against a pre-compaction copy), and
`test_fork_event_index_periodic_cutover_bounded` (6 rounds of 30 WAL-less
rewrites each to one fork's block 0, 180 writes total, each round ending in
its own reclaiming cutover: the in-memory event count stays flat at 3 after
every round with this PR; reverted, it grows round-by-round (31, 61, 91,
121, 151, 181), tracking round * 30 + 1, unbounded in the number of rounds).
`fev_bench K periodic` (same fixture and mechanism, K = 50,000 split into 10
rounds instead) confirms the pattern at scale: max in-memory `nevents`
across every round is 11 with this PR versus 50,001 (K + 1) reverted. `pagestore_forkmeta_crash_matrix_test` is the
regression suite that matters most here: a crash between publish and the
next open must see identical state with or without the in-memory compaction,
and it does by construction (the durable side never changes), which that
suite's 273 checks confirm stayed green.

Remaining follow-ups, deliberately out of this PR: (1)
`fork_meta_snapshot_marker_page_retained()`'s version-chain walk (design C,
optional, only if a later profile shows it dominating); (2) `def_idx` for
the remaining newest-first SET/DEAD linear scans
(`fork_block_death_through()`, the walidx planner loops, the snapshot
builder's per-entry loop) if they are ever shown to matter.
