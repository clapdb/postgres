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
