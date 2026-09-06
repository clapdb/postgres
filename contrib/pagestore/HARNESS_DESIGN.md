# pagestore correctness harness

## Purpose

`pagestore_test.c` checks the freestanding daemon protocol and storage core.
The focused C unit tests check layer, manifest, cache, and GC invariants.
`integration_test.sh` proves several PostgreSQL-facing paths end to end.
`harness/pagestore_harness.py` now provides the first reusable runner: it
validates the basic JSONL schema and some boundary references, owns private
test environments, captures diagnostic run roots, runs daemon-smoke,
named daemon-fault recovery, writer-smoke, and managed materializer-smoke
modes, and can wrap the legacy integration script in a failure bundle.  The
named-fault mode pre-arms one crash-only fault before launch, proves its exact
hit from a bounded report, and performs two clean recovery opens.  It is not
yet the full semantic
harness described below: full
mode-specific plan validation, per-action timeouts, replay commands, generated
workloads, error/pause faults, branch plans, redo/materialization comparisons,
shrinking, and compatibility fixtures remain future work.

This document specifies that harness.  Its job is to find semantic regressions
in the page-store contract, not to replace PostgreSQL's regression suite or to
become a generic database benchmark framework.

The central property is:

```text
after every declared durability boundary and recovery,
the observable PostgreSQL state equals the scenario's declared state at its
chosen timeline and LSN horizon.
```

That includes relation contents, relation existence and size, MVCC visibility,
SLRU/control artifacts, branch ancestry, and the inability of a pinned reader
to observe state newer than its horizon.

## Non-goals

- Do not reimplement PostgreSQL visibility, WAL redo, or the pagestore core in
  a test-side model.  A duplicate implementation would share too many wrong
  assumptions and become another engine to maintain.
- Do not require Docker, object storage, SPDK hardware, or a Python package
  outside the standard library for the default suite.
- Do not put performance pass/fail thresholds in correctness PR CI.  Latency
  and throughput probes belong to a separate benchmark lane.
- Do not hide every existing test behind one runner immediately.  The current
  C tests and integration script remain direct, fast entry points.

## Shape

The host-side executable is `contrib/pagestore/harness/pagestore_harness.py`,
with only the Python standard library.  It receives capability metadata plus a
scenario plan for `--validate`, `--daemon-smoke`, `--writer-smoke`, and
`--materializer-smoke`; `--daemon-fault-recovery` runs the first named,
crash-only daemon recovery case;
`--list` instead receives a scenario directory and inventories the plans below
it.  Runtime modes then add the inputs they actually need: `--daemon-smoke`
takes daemon and inspector binaries, `--writer-smoke` and
`--materializer-smoke` also take the Meson build directory, while the latter
additionally takes the installed or source `--materializer-supervisor`, and
`--legacy-integration` takes the Meson build directory plus the legacy script
path.  It owns temporary directories,
daemon/PostgreSQL lifecycle, and capture of diagnostic artifacts.  Full
per-action timeout enforcement, complete replay metadata, and deterministic
scheduling are still future generated-scenario work.

The materializer runtime is the acceptance client for the first concrete
managed-worker contract.  It provisions a WAL-only writer and route-all
recovery worker, then starts the production-facing supervisor.  The supervisor,
not the coordinator, turns each declared writer checkpoint into an archived and
durably materialized boundary and replaces a worker killed with the `compute`
crash model.  The runtime also starts a competing owner and requires temporary-
failure exit status 75 before continuing, and hands ownership to a replacement
supervisor without restarting the healthy worker.  Worker generations, owner
epochs, and progress come from the supervisor's atomic status file and are
copied into the failure bundle.

Use JSON Lines for plans rather than YAML.  It is dependency-free, easy to
generate or shrink, and each action is independently visible in failure logs.
A scenario is a sequence of operations plus assertions, with one explicit
integer seed.  The runner emits operation events as JSON Lines and copies the
plan into diagnostic run roots.  A future replay mode must also record the
exact invocation, build revision, runtime paths, and environment needed to
recreate a failing run with one command.

```text
scenario JSONL + seed
        |
        v
  harness coordinator
   |       |       |
 writer  daemon  reader/branch computes
   |       |       |
        checkpoint / fault scheduler
                    |
                    v
             oracle assertions
                    |
                    v
       pass, or self-contained failure bundle
```

The coordinator is intentionally outside the daemon.  A harness crash must not
be confused with a daemon crash, and fault timing must remain observable from
the test driver.

## System under test and topology

The harness validates a deployment, rather than a collection of library calls.
A run creates one isolated store root and these named process groups as needed:

| Component | Role | Owner |
|---|---|---|
| `store` | daemon, storage root, IPC endpoint, and optional shard workers | harness |
| `writer` | read-write PostgreSQL compute producing WAL/checkpoints | harness |
| `branch-N` | independent compute booted at a declared branch point | harness |
| `reader-R` | pinned, read-only compute at a declared horizon | harness |
| `redo-N` | optional WAL redo/materialization worker | harness |
| `observer` | short-lived inspection client | harness |

Branches and readers must share the actual store root: copying it would hide
timeline and horizon bugs.  Each PostgreSQL compute instead gets a private
`PGDATA`, port, socket directory, log, and process group.  A crash action can
therefore state exactly whether it kills the store, a writer, or a reader.

The coordinator uses a stable run directory:

```text
run/
  store/                 daemon data, IPC sockets, storage settings
  computes/writer/       private data/log/config for each compute
  work/                  generated SQL and prepared artifacts
  trace/                 events and inspection snapshots
  failure/               completed only when the run fails
```

All subprocesses use an allowlisted environment.  Inherited `PGDATA`,
`PGPORT`, `PGHOST`, `PAGESTORE_*`, and fault variables are rejected unless the
scenario explicitly declares them.  This prevents a developer's local cluster
or fault setting from changing CI semantics.

### Configuration matrix

A scenario declares a case, not arbitrary shell flags.  The manifest expands
only supported dimensions and writes the concrete result to `case.json`:

| Dimension | Initial values | Later values |
|---|---|---|
| storage backend | `posix` | `spdk`, object storage |
| daemon shards | `1` | `2`, power-of-two counts |
| compute topology | writer, pinned reader, branch | redo-only/materializer |
| WAL path | current local path | WAL-only redo |
| relation class | permanent | default/global, temp, unlogged |
| build geometry | current page size | supported non-default page sizes |

The runner must reject unsupported combinations instead of silently falling
back to one shard or a different backend.  Matrix expansion is in the test
manifest, not in opaque per-test shell loops.

## Contracts and test taxonomy

Every scenario names the pagestore contract it proves and its supported case
dimensions.  This turns coverage into an auditable matrix.

| Contract | Observable promise |
|---|---|
| Write/read | `(timeline, LSN, key)` selects the correct page/relation version. |
| Durability | State acknowledged at a named boundary survives its crash model. |
| Atomic metadata | Manifest/control recovery is old-valid or new-valid, never mixed. |
| Timeline isolation | Parent, branch, and reader share only declared ancestry. |
| Snapshot visibility | Reader visibility uses its fixed running-XID horizon. |
| Lifecycle | Seal, compaction, and GC preserve every retained horizon. |
| Routing | Permanent relations, catalogs, SLRUs, and control use the intended path. |
| WAL/materialization | Redo and direct paths agree at the same materialized LSN. |
| Concurrency | Parallel work preserves ordering/ownership and provides bounded progress. |
| Compatibility | Persisted bytes obey an explicit reopen and format-upgrade policy. |

Unit tests remain the proof for local data-structure invariants.  The harness
owns contracts crossing daemon persistence, PostgreSQL state, and process
lifetime.  A harness-found bug should gain the smallest appropriate regression
test, rather than automatically adding another expensive end-to-end scenario.

## Scenario operations

Plans use a deliberately small vocabulary.  Every operation has a stable
`id`, and all random choices come from the plan seed.

| Operation | Meaning | Required observation |
|---|---|---|
| `sql` | Run SQL on a named writer, branch, or reader compute. | Result rows, SQLSTATE, and command tag. |
| `checkpoint` | Complete a writer checkpoint and record its redo LSN and control image identity. | R, next/oldest XID horizons, and admission fence. |
| `prepare_branch` / `install_branch` | Build and install branch artifacts at a declared fork LSN. | Timeline ancestry and prepared artifact hashes. |
| `prepare_reader` / `install_reader` | Build and install an as-of reader at R. | Reader manifest plus fixed running-XID snapshot identity. |
| `restart` | Cleanly stop and start a named process group. | Restart generation and post-replay health query. |
| `crash` | Kill a declared process group at a named fault point. | Fault name, generation, and durable-boundary sequence. |
| `advance` | Apply generated, deterministic DML/DDL from a workload profile. | Writer-side expected result and LSN range. |
| `assert` | Evaluate an oracle predicate at a named compute/horizon. | Actual value and the expected predicate. |

No operation implies a durability boundary.  `checkpoint`, `prepare_*`, and
explicit `sync` actions are the only durable handoff points.  This prevents a
scenario from accidentally treating a successful page write as a committed
database state.

### Plan schema and scheduling

The JSONL format is versioned.  Its first record is a required header; all
later records are actions or assertions:

```json
{"schema":1,"scenario":"reader-visibility","seed":184467,"case":{"storage":"posix","shards":1}}
{"op":"checkpoint","id":"r0","target":"writer","name":"R"}
```

The runner validates the full plan before starting a daemon: unique IDs, known
targets, valid profile parameters and fault names, declared dependencies, and
references such as `$R` only after their boundary exists.  Unknown fields are
rejected except under a reserved `extra` diagnostic object.  A typo must not
quietly change test meaning.

Actions are sequential by default.  A `parallel` action contains named lanes;
each lane is sequential and the action completes at a declared barrier.  The
schedule is explicit: generated plans record every lane step, barrier, wait,
and fault release.  The harness waits on a health endpoint, SQL predicate,
test hook, or durable event, never on a correctness sleep.

Additional operations are `parallel`, `wait`, `sync`, `set_fault`,
`release_fault`, `capture`, `compare`, `expect_failure`, and `cleanup`.
`crash` names a target and model: `power_loss` kills the daemon process group,
`compute` kills only the compute, `network_partition` requires a declared test
transport, and `process_abort` asks a hook to abort at a controlled point.  The
plan validator rejects crash models the active backend cannot represent.

Every action emits `start`, `ready`, `end`, or `failed` as JSONL with monotonic
sequence number, target generation, LSN range, and armed-fault state.  Action
IDs are stable diagnostic identities and are not renumbered by shrinking.

Example skeleton:

```json
{"op":"sql","id":"create","target":"writer","sql":"CREATE TABLE t(id int primary key, v text) TABLESPACE ts"}
{"op":"advance","id":"load","profile":"mixed-dml","steps":200}
{"op":"checkpoint","id":"r0","name":"R"}
{"op":"prepare_reader","id":"reader-r0","read_lsn":"$R"}
{"op":"advance","id":"after-r","profile":"mixed-dml","steps":50}
{"op":"assert","id":"writer-new","target":"writer","oracle":"table_digest","table":"t","expect":"latest"}
{"op":"assert","id":"reader-old","target":"reader-r0","oracle":"table_digest","table":"t","expect":"$R"}
```

## Oracles

The harness combines independent observations.  No single checksum is enough.

### SQL state oracle

For each generated relation, retain a canonical SQL query: primary-key order,
all user columns, and `md5(string_agg(row_to_json(...)::text, ...))` or an
equivalent type-safe digest.  Run it on the writer after each checkpoint and
on a branch/reader at its declared horizon.  This is the primary end-user
correctness oracle.

### Physical metadata oracle

Record at every declared horizon:

- relation existence and `pg_relation_size` / block count;
- fork size and existence through the existing as-of inspection functions;
- timeline, parent timeline, and fork LSN;
- checkpoint redo, `nextXid`, `oldestXid`, multixact, and commit-ts horizons.

This catches page-content agreement with incorrect truncation, drop, or
ancestry state.

### Visibility oracle

The harness creates transactions deliberately spanning a checkpoint R:

- committed before R;
- prepared or open at R, committed after R;
- aborted after R;
- released subtransactions exceeding ordinary snapshot capacity.

It verifies that a reader at R sees exactly the first group.  Before starting
the reader it can replace its local `pg_xact` segment with a newer writer copy,
which proves that fixed snapshot membership rather than current CLOG status is
deciding visibility.

### Recovery oracle

After every crash/restart sequence, assert all of:

- the daemon opens the manifest and reaches health-check readiness;
- records at or below the last declared durable boundary are readable;
- branch and reader artifacts either validate completely or startup fails
  closed before serving SQL;
- the manifest/layer state has no live reference to a missing layer file;
- a repeated restart produces the same result.

The last property turns a one-off recovery success into an idempotence check.

### Differential and metamorphic checks

For deterministic DML/DDL that PostgreSQL can run without pagestore, run the
same trace on a local-control PostgreSQL instance and compare canonical SQL
state at each logical checkpoint.  The control run is not a reference storage
engine and does not decide branch, reader, or crash behavior; it catches
ordinary SQL-visible divergence without duplicating pagestore logic.

Also assert relations that need no second cluster:

- clean restart and crash/recovery after the same durable boundary agree;
- a retained `(timeline, LSN)` query agrees before and after compaction/GC;
- one-shard and supported multi-shard runs agree externally for the same trace;
- direct ingest and WAL redo agree at the same materialized LSN; and
- transaction traces executed in deterministic chunks agree with the same
  committed trace executed without those boundaries.

Oracle output is structured data, not a `psql` transcript.  Use ordered,
type-aware output with normalized encoding, timezone, `DateStyle`, collation,
and relevant GUCs.  A digest is a fast mismatch detector; failure bundles also
retain ordered rows so collisions and formatting differences are not treated as
proof.  Physical observations are classified as **must equal** (ancestry,
fork LSN, existence, retained block count), **must be monotonic** (durable LSN,
generation), or **may differ** (layer IDs, segment offsets, compaction layout,
PIDs, and timing).  This avoids freezing LSM internals unnecessarily.

### Daemon inspection interface

Add a narrow, versioned, read-only test IPC interface.  It is private to the
test endpoint or an explicit test build switch and never supplies mutation
operations:

```text
health() -> format, generation, shard count, last durable LSN
timeline(id) -> parent, fork LSN, retained horizon
relation(timeline, key, lsn) -> existence, forks, nblocks, selected version
manifest() -> generation, checksum, live layers, pending deletion
gc() -> retained horizons, queue length, completed phase
backpressure() -> per-shard queued/in-flight work and admission state
```

The interface explains failures and proves fault reachability.  SQL-visible
state remains the primary external correctness oracle.

The first implementation is `pagestore_inspect`, a separate read-only client
of the existing private shared-memory transport.  The H0 foundation provides
schema-validated aggregate observations plus a per-timeline query foundation
and never claims a mailbox.  Relation inspection remains a planned,
non-advertised H1 operation until it can be exposed without creating a
mutation or production-management surface.

## Fault model

Existing one-off environment faults, such as
`PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES`, should migrate to one test-only fault
registry.  A fault has a stable name, optional hit count, and action:

```text
manifest.before_fsync      crash | error | pause
manifest.after_rename      crash | error | pause
layer.before_seal          crash | error | pause
gc.after_mark_delete       crash | error | pause
segment.after_append:N     crash | error | pause
control.after_local_sync   crash | error | pause
slru.after_ship            crash | error | pause
```

`pause` is important: the harness can stop at a precise point, checkpoint or
inspect another process, then release it.  Faults must be compiled into test
builds only or guarded by explicit `PAGESTORE_TEST_FAULT_*` settings;
production configuration never activates them.

Canonical names, allowed actions, and harness metadata share
`pagestore_fault_points.def`; the registry is inert when no fault variables are
present, while a partial
`PAGESTORE_TEST_FAULT_{NAME,ACTION,HIT,DIR}` configuration fails closed.
The control directory is outside the store, contains fixed `arm` and
`release`/`report.jsonl` names, and is never fsynced by a fault probe.  The
lock-free `daemon.after_ready` point supports crash, error, and bounded pause;
publication probes reached while holding shard, map, or WAL-index locks remain
crash-only.  Reports are atomically published with scenario, seed, hit, PID,
and operation identity before the selected action becomes observable.

Each scenario explores one fault point per run.  The coordinator enumerates
them deterministically instead of injecting several independent crashes into
one opaque run.  That gives a useful failure statement: `(scenario, seed,
fault-name, hit-count, operation-id)`.

### Fault implementation and crash semantics

Fault points belong on state transitions, not arbitrary source lines.  Each
registration documents owner, precondition, durability before the transition,
state being changed, and recovery expectation:

| Fault | Transition | Required recovery |
|---|---|---|
| `manifest.before_fsync` | new manifest written but not durable | old manifest remains authoritative |
| `manifest.after_rename` | name switched, directory durability pending | old or new complete manifest, never torn authority |
| `layer.before_seal` | immutable layer incomplete | layer absent or fully validated, never partially live |
| `gc.after_mark_delete` | delete intent durable, unlink pending | deletion resumes or object is safely retained |
| `segment.after_append:N` | Nth segment record appended | complete records replay; invalid tail is discarded |
| `control.after_local_sync` | local control durable, mirror pending | startup selects only the safe declared horizon |
| `slru.after_ship` | SLRU transfer done, publish pending | no reader sees an unpaired horizon |

The hook must not create a durability edge: it may increment an in-memory hit
counter and signal the coordinator, but must not fsync data, take a global lock
that changes ordinary ordering, or allocate unbounded memory.  `pause` has a
watchdog that captures process state before ending a hung case.  A scenario
fails if an expected fault was never reached; otherwise skipped code paths can
make recovery tests pass vacuously.

The POSIX lane proves process-crash recovery plus explicitly named fsync/rename
ordering assumptions.  It does not claim arbitrary machine power-loss coverage
for a filesystem and drive.  SPDK and future object-store backends declare
their own flush/atomicity/visibility guarantees and map the same logical fault
names to backend-specific injection points.  Every report states its crash
model.

## Workload profiles

Profiles are deterministic generators, not a SQL fuzzing language:

- `append_update`: inserts, updates, deletes, checkpoints, and relation growth;
- `fork_metadata`: extend, truncate, unlink, and recreate relation forks;
- `branch_diverge`: fork, apply independent parent/child writes, and read both;
- `reader_visibility`: R-spanning transactions, pinned reads, and read-only
  rejection paths;
- `slru_control`: transaction churn, multixacts, commit-ts toggles, and control
  mirror boundaries;
- `gc_compaction`: seal layers, compact, retain a horizon, and inject recovery
  faults around lifecycle transitions.

Profiles should use a small set of tables and bounded data sizes.  The value of
the harness is schedule coverage and shrinking, not random gigabytes of data.

### Coverage inventory and concurrency

The roadmap must cover, at minimum:

| Family | Required cases |
|---|---|
| Relation lifecycle | create/load/extend/truncate/drop/recreate, all forks, tablespaces |
| DDL/catalog routing | indexes, toast, sequences, default/global catalogs, temp/unlogged policy |
| MVCC | commit, abort, concurrent update/delete, R-spanning xacts, multixact, subxid overflow |
| Reader/branch | fixed R, CLOG substitution, artifact rejection, divergence, multiple children when supported |
| Control/SLRU | repeated checkpoints, mirror recovery, `pg_xact`, multixact, commit-ts, truncation |
| Segment/layer/GC | replay, seal, compaction, retained horizons, mark-delete/unlink/restart |
| WAL redo | create/extend/truncate/drop, restart at window boundaries, unsupported/corrupt WAL rejection |
| Resource limits | backpressure, disk/write errors, bounded queues, descriptor pressure |
| Compatibility | same-format reopen, supported migration, unknown-format rejection, checksum corruption |

Unsupported product behavior is still represented as an `expect_failure` case;
it must reject clearly rather than disappear from test coverage.

For the share-nothing shard direction, concurrency needs deterministic
interleavings rather than only high client counts.  Test builds expose bounded
barriers at admission, publish, checkpoint/control sync, reader install, and
compaction/manifest publish.  Plans release them in recorded order.  Fixed PR
tests use hand-authored schedules; nightly runs enumerate bounded permutations
then use seeded choices.  Diagnostics include key owner, per-shard queue
high-water marks, and cross-shard messages.  Assertions require bounded
progress or a declared error, not merely absence of deadlock.

## WAL redo, corruption, and compatibility

`WAL_REDO.md` and `MATERIALIZATION.md` require a first-class redo topology.
Retain each test WAL window with start/end LSN, timeline, record count, and
checksum.  For every supported window run direct, redo, split-redo (restart at
selected boundaries), delayed-materialization, and invalid-input cases.  Direct
and redo must agree at the same materialized LSN; corrupt, missing,
out-of-order, wrong-timeline, or unsupported WAL must fail before serving false
state.  The harness uses PostgreSQL tools for WAL diagnostics and does not
implement another WAL decoder in Python.

Crash faults cover incomplete writes; corruption cases mutate bytes known to be
durably wrong.  Test stopped copies or a test storage wrapper, never `dd` on a
live daemon.  Cover manifest checksum/header, layer header/trailer, segment
length/checksum, control metadata, timeline ancestry, and SLRU identity.  The
required result is fail closed, not silently initializing an empty relation or
timeline.

Persisted pagestore state is an API.  Maintain minimal reproducible fixtures
for every supported on-disk format transition: manifest/layer/segment set,
branch and reader artifacts, control image, and relevant SLRU image.  Each
fixture records format version, PostgreSQL major, page size, backend
assumptions, checksums, and expected queries.  Test current reopen, supported
upgrade then second restart, unknown/future version rejection before mutation,
and recovery/fail-closed behavior for a failed migration.  Release branches run
only fixtures declared compatible with their PostgreSQL major.

## Failure bundle and replay

On any failure retain a single directory containing:

```text
plan.jsonl                 exact expanded operation trace
seed.txt                   seed, build revision, page size, shard count
events.jsonl               operation start/end, LSNs, process generations
writer.log daemon.log      complete process logs
postgresql.conf            effective configuration for every compute
artifacts/                 manifests, reader snapshots, control metadata
sql/                       expected and actual oracle queries/results
```

Also retain `case.json`, version/build metadata, and read-only diagnostic
snapshots for daemon health, manifest, timelines, GC, and backpressure.  Output
is structured: event sequence, action ID, target/generation, case, seed,
fault, LSN range, command/environment allowlist, process exit/signal, and
normalized oracle results.  On failure the coordinator freezes surviving
processes where possible, captures snapshots, then terminates.  Collection has
a size cap and records omitted artifacts rather than recursively copying an
unbounded data directory.

Classify the final result as `setup`, `liveness`, `unexpected_exit`, `timeout`,
`oracle_mismatch`, `durability_violation`, `corruption_not_rejected`,
`fault_not_reached`, or `harness_error`.  A harness error is neither a
pagestore pass nor a pagestore failure and is governed by separate CI
infrastructure policy.

The runner prints a replay command using that directory.  A `--shrink` mode
uses prefix deletion followed by operation deletion while preserving declared
dependencies, reducing a failed generated plan to the smallest reproducible
sequence.  Shrinking is post-failure work; it does not run in normal CI.

## CI lanes

| Lane | Trigger | Scope | Budget |
|---|---|---|---|
| Unit | every relevant PR | existing C unit tests plus pure plan parser tests | seconds |
| Smoke | every relevant PR | one fixed seed for each profile, no injected crash | under 5 minutes |
| Recovery | every relevant PR | selected fault points for branch, reader, manifest, and GC | under 10 minutes |
| Soak | nightly / manual | seed matrix, all registered fault points, restart idempotence | bounded but longer |
| Performance | manual / scheduled hardware | `pagestore_bench`, shard and storage-backend matrix | informational trend data |

The existing `integration_test.sh` should initially remain the smoke scenario
source.  Port one section at a time into plans, keeping its direct assertions
until the harness has demonstrated equivalent coverage.  Do not make the PR
lane random: any generated seed admitted there must be fixed in the repository.

### Manifest, CI, and performance governance

Store plans in `contrib/pagestore/harness/scenarios/`, profiles in
`contrib/pagestore/harness/profiles/`, and runner-only tests in
`contrib/pagestore/harness/tests/`.  A machine-readable manifest maps each
scenario to contracts, supported cases, fixed seeds, capabilities, estimated
cost, and CI lanes.  Meson exposes named suites so developers can run a smoke
or recovery lane directly instead of knowing private shell conventions.

Every contract-changing bug fix adds a local unit test, fixed harness scenario,
fault case, or format fixture as appropriate.  CI publishes failure bundles and
the replay command.  Deterministic suite retries may collect diagnostics but
cannot turn an unexplained failure green.  Dedicated hardware tests may be
tagged environmental, with a separate availability policy.

Correctness tests record timing only for diagnosis.  A separate pinned-hardware
lane uses `pagestore_bench` and scenario-derived traces to measure latency,
throughput, write amplification, recovery time, GC lag, space, descriptors,
and per-shard queue occupancy.  It also tests overload: full disk, slow
storage, exhausted queues, and stalled materialization must yield bounded
backpressure and explicit errors, never unbounded memory or a silently
advancing durable horizon.

## Implementation sequence

0. [implemented] Define the read-only test IPC schema and capability manifest.
   The schema is versioned, pure runner tests cover parsing, and each executable
   runner validates its supported operations and observed daemon protocol,
   page/I/O dimensions, shard count, and inspection operations before execution.
1. [partial] Add the Python coordinator with private environments, lifecycle
   management, JSONL validation/event logging, and diagnostic failure bundles.
   Current automated smoke coverage includes daemon readiness and the writer
   lifecycle/reader scenario; the legacy integration bridge is available as a
   manual bundle wrapper.  Full mode-specific validation, per-action/run
   timeouts, and replay-command metadata are still future work.
2. Add checkpoint records and SQL/metadata/visibility oracles.  Port the
   current reader R test, including running-XID and oversized-subxid cases, and
   add the local-control differential trace.
3. [partial] Add the test-only fault registry with reachability accounting.
   The registry supports crash/error/pause at the safe daemon after-ready
   point, and existing page/GC/WAL-index publication crash windows use
   canonical names.  Composed manifest/segment/GC daemon-restart scenarios and
   additional lock-free error/pause points remain to be added.
4. Add branch, control, SLRU, lifecycle, and corruption scenarios, retaining
   one understandable topology per scenario.
5. Add WAL redo/materialization topology and direct-versus-redo cases; then add
   deterministic profile generation, schedules, replay, and shrink.
6. Add format fixtures and the compatibility lane.  Require one for every
   persisted-format change.
7. Extend supported plans to shards and storage backends.  The oracles remain
   shard-agnostic; the case matrix and diagnostics change.
8. Establish the non-blocking performance/resource lane on pinned hardware.

## Acceptance criteria

The harness is useful when a maintainer can add a new durability rule by
declaring its boundary, fault points, configuration dimensions, and observable
invariant in one scenario, then obtain a replayable failure bundle without
reading an interleaved shell script or guessing which crash window was
exercised.

Before it replaces any existing integration coverage, the first release must
demonstrate all of the following:

- each migrated test has a named contract, fixed plan, and selected case;
- injected SQL, metadata, and fault-reachability failures produce the expected
  classification and replay command;
- every initially registered manifest/segment/GC fault is either reached and
  recovered according to its rule or fails closed;
- reader tests cover committed, open, aborted, and oversized-subtransaction
  state across R, including a newer local CLOG substitution;
- branch and reader topologies coexist without observing post-horizon writes;
- direct and redo paths agree for the supported redo cases; and
- passing and failing runs leave no process, socket, or mutable data outside
  the run root, while the fixed PR lane meets its published budget.
