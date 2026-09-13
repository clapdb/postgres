# Artifact publication and retirement

This is the local POSIX lifecycle for exact-generation SLRU seeds and reader
snapshot objects. It closes the two publication/retirement gaps previously
listed in MVP status. It does not change PostgreSQL-native page bytes or add a
multi-writer timeline contract.

## Durable identity

Data keeps its existing `(timeline, class, spcOid, dbOid, relNumber, fork=0,
block, LSN, admission sequence)` key. A companion `PS_KLASS_ARTIFACT` key uses
`forkNum` to name the data class and routes to the **same shard** as the data.
Its block 0 holds BEGIN records; block 1 holds COMMIT or DROP records. These
are ordinary versioned pages, recovered from the segment/layer path and
compacted with the data.

`PsArtifactLifecycle` in `pagestore_artifact_format.h` binds the record to its
object, timeline incarnation and generation LSN. Magic, version and CRC-32C
are checked on recovery and use. The completion record carries the BEGIN
admission sequence, expected distinct page count and maximum block plus one. Its own persisted page
identity supplies the upper admission boundary.

## Publication

1. BEGIN durably records a new attempt and returns its admission sequence as
   the token. Generations cannot move backwards. A new attempt supersedes an
   unfinished attempt; writes using the old token are rejected.
2. Every staged page carries that token and the generation LSN. Pages remain
   hidden until completion. Pages can be sparse, and retries within an attempt
   may overwrite a block; the latest admitted copy is authoritative.
3. Writes maintain a per-attempt distinct-block counter and maximum block in
   memory; repeated writes of a block count once. COMMIT compares that counter
   with the declared count without scanning retained page history, syncs the data, appends the identity-bound completion record, then
   syncs again before acknowledging. A completed attempt accepts no further
   writes. BEGIN/COMMIT/DROP bypass reclamation backpressure so closing an
   interval cannot wait behind the debt it releases; ordinary data writes keep
   their existing admission policy.
4. Readers select the latest complete generation at their horizon, then read
   only page versions inside its BEGIN/COMMIT admission interval. A block
   absent from that complete generation is absent, even if older generations
   or ancestors contain it. An unfinished newer generation leaves the prior
   complete generation available. Consumers requiring an exact cutoff still
   check the resolved LSN and fail if that cutoff was never completed.

`EXISTS` and `NBLOCKS` resolve the same completed interval, including through
ancestry and at historical horizons. A zero-page COMMIT is an existing empty
object (`EXISTS=1`, `NBLOCKS=0`); DROP reports nonexistence and zero blocks.
Pending growth cannot change either answer. Lifecycle record version 2 stores
the completed size alongside the distinct-page count. Live attempt counters
need no recovery because unfinished pre-restart attempts cannot commit.

An unfinished generation can be retried at the same LSN with a fresh token.
Once completed, a generation is immutable: BEGIN returns its original token,
WRITE verifies identical durable bytes without appending, and COMMIT verifies
the existing page count without publishing another interval. Different bytes
or block sets require a newer LSN. This also holds after restart, so readers
and branches using an LSN without an admission cap cannot switch to a replacement.
Exact admission-sequence fences still preserve the corresponding interval.

Publication errors do not imply that an operation was absent from disk. If
completion-record append/sync has an ambiguous outcome, artifact operations
fail closed until reopen; recovery can select the previous complete state or
the complete new state. A process-crash test is not a power-loss guarantee.

The PostgreSQL producers use this protocol for whole SLRU directory snapshots,
running-XID snapshot data and multi-page database barriers. Single-page reader
manifests, READY records and relation maps publish as one-page generations.
READY stages the snapshot; the global manifest is published only after its
exact-generation relmap checksum is known, before the all-database adoption
barrier. It omits the database-local relmap checksum, so different database
workers publish identical global bytes rather than overwrite a placeholder.
Single-page artifact preparation bypasses legacy CREATE/NBLOCKS: the first
BEGIN is the creation boundary, so a failed initial publication cannot expose
a legacy empty fork.
Empty SLRU snapshots publish complete zero-page generations and can be retried
at the same cutoff. The four SLRU banks are separate objects; the capture API
returns its cutoff only after every bank is complete, retaining the existing
paused-materializer/control identity checks.

## Drop and reuse

DROP durably records absence at a supplied LSN strictly later than the latest
completed generation. Repeating that drop is
idempotent. It hides the object at and above the drop horizon while readers and
branches below that boundary keep their complete generations. An older writer
cannot resurrect it. A new generation strictly after the drop can reuse the
object identity. Branch-local mutations must be later than the branch point.
Legacy raw writes and fork unlink/truncate/zero-extend operations cannot bypass
an existing lifecycle protocol, including one inherited from a parent.

The reader-artifact launcher compares the previous durable database barrier
with the new catalog membership set while holding the existing `pg_database`
ShareLock. Before publishing the replacement barrier it drops the removed
databases' manifest and relation-map objects. If it crashes between those
steps, the old barrier remains an inventory for idempotent retry. Retirement
therefore occurs on a successful subsequent artifact publication cycle, rather
than in the DROP DATABASE SQL transaction itself. It never retires the shared
running-XID snapshot merely because one database disappeared.

## Retention and recovery

Compaction decodes selected intervals once per object and reuses them across
page groups, locating page tuples with binary search instead of nested scans
of each version at each horizon. It retains completed intervals above the
operational floor and those
selected at the floor and retained reader/branch fences. A DROP selected at a
horizon requires no data pages there. When no surviving horizon selects the
old generation, its pages and their control-era fences are released.

The latest unfinished attempt in the current daemon stays protected while its
producer can finish. Superseding it makes its uncommitted pages reclaimable;
reopening the daemon abandons pre-restart unfinished attempts and rejects
stale tokens. Reclamation follows the existing operational floor and
maintenance scheduling, so this is not immediate deletion of every failed
write. Completion and drop metadata needed by retained views survives with
them. A small first-BEGIN legacy boundary, current attempt and applicable
completion/drop records remain; object-key tombstones are not themselves
forgotten. This closes retained **data-generation** leakage without claiming
bounded metadata under an unbounded number of distinct object identities.

Legacy objects without lifecycle records remain readable. The first BEGIN
provides an admission boundary: pages admitted before it retain legacy
semantics, while uncommitted later pages cannot masquerade as legacy data.
Legacy non-versioned diagnostic writes remain supported only before a key
enters the protocol.

## Store compatibility

The checked `PSS2` representation of `.pagestore-nshards` is the minimum-reader
fence for these semantics. A validated legacy decimal shard count is atomically
upgraded (temp file, fsync, rename, directory fsync) before the daemon becomes
ready. Old daemons cannot parse PSS2 and refuse the store. Removing or editing
this file is not a downgrade procedure. Shard-count restrictions remain in
force. IPC version 47 also prevents an older compute/daemon pair from silently
using an unsupported protocol. The native PostgreSQL payload identities are
unchanged.

`fixtures/posix-artifact-lifecycle` is the new current fixture; the previous
`posix-backend-objects` fixture is retained as legacy. The new fixture includes a
complete sparse generation, an unfinished newer attempt and a dropped object,
with reopen/read/restart checks. This protocol is qualified for the local POSIX
provider. Startup rejects logical pages smaller than the 72-byte lifecycle
record before opening storage. Providers requiring all-shard write locking for sync are rejected by
the lifecycle operations.

## Deterministic validation

- `pagestore_artifact_lifecycle_test` runs for both SLRU and reader classes on
  three logical shards: partial publication, page-count rejection, superseded
  tokens, same-LSN retry, sparse absence, empty generations, process exit before
  and after completion, injected sync errors, corrupt metadata rejection,
  drop/restart, branch retention, physical-version reclamation and reuse.
- `integration_test.sh` checks real PostgreSQL producers and DROP DATABASE:
  both database artifacts exist before deletion, disappear from the newest
  view after the launcher cycle, and remain byte-identical at a retained old
  horizon.
- The golden and branch-boot scenarios exercise independent computes and
  portable SLRU bootstrap using the protocol.
- Persisted-format checks cover legacy migration and the new fixture. Existing
  control, lifecycle, retention, GC and WAL-reclaim tests protect the adjacent
  reclamation paths.

No stress/soak run is required or claimed by this change. The larger proposed
release-qualification work remains in `RELEASE_VALIDATION.md`.

### Validation for this change (2026-09-13)

The cassert-enabled Meson build passed. Both lifecycle variants passed all
61 checks, and the standalone `-O2 -Wall -Wextra -Werror` build passed. The
control-prune, lifecycle-prune, retention, GC, forkmeta-snapshot, WAL-reclaim
and harness-plan tests passed. PostgreSQL integration (including database
retirement), MVP golden and independent branch boot passed. All five persisted
fixtures passed, including legacy migration and current-format corruption
checks.

Stress/soak was intentionally excluded. The PR commit uses `[skip ci]` because
the existing automatic workflow includes a soak job; these results are local
validation, not a claim that the PR's hosted CI ran. Full CI and release-branch
qualification remain required before release.
