# Porting `pagestore` across PostgreSQL majors

Two related but separate bodies of work, both driven by the V4 plan
(`branchdb_18` is the first release candidate; see `RELEASE_VALIDATION.md`
and `MVP_COMPLETION_PLAN.md`'s 2026-09-21 row):

1. **The core patch series (C1-C7)** — a small, curated set of PostgreSQL
   core changes (outside `contrib/pagestore`) that `pagestore` carries on top
   of upstream. These are not in `contrib/pagestore` and are not guardable
   from within it; each `branchdb_N` gets its own port of the series, done
   once per major by cherry-picking and resolving conflicts against that
   major's headers (`scripts/branchdb-sync.sh forward`, once the series
   carries `Branchdb-Series:` trailers).
2. **`contrib/pagestore`'s own version-specific API surface** — places where
   `contrib/pagestore` calls a PostgreSQL core symbol, type or macro that
   does not exist, or has a different shape, before 19. Unlike the core
   series, `contrib/pagestore` stays **byte-identical** across `pagestore`
   and every `branchdb_N` (AGENTS.md), so these need `#if PG_VERSION_NUM`
   guards landed on `pagestore` itself *before* a release branch's
   `sync-contrib` copies the tree — a release branch cannot special-case its
   own copy without breaking that invariant.

## 1. The core patch series (C1-C7)

Source: the V4 plan (`git log --no-merges dc511678084..origin/pagestore --
src/ doc/ contrib/meson.build`, 51 commits, +2414/-211 across 58 files). Each
series becomes one squashed commit per release branch, carrying a
`Branchdb-Series: C<n>` trailer naming the series and listing its source
SHAs, cherry-picked in order by `scripts/branchdb-sync.sh forward`.

| # | Series | Source commits (oldest -> newest) | Files |
|---|---|---|---|
| C1 | smgr pluggable switch, md seams, recovery prefetch | `1ad5b2c7c17` (smgr part), `c0f4e0c320b`, `aa13cf51ce9` | `storage/smgr/smgr.c`, `smgr.h`, `md.c`, `md.h`, `xlogprefetcher.c`, `xlogutils.c` |
| C2 | walredo helper process + VM buffer router | `1ad5b2c7c17` (walredo part), `b0bb42d98e5`, `4f69e7a3b66`, `8e967c9dafe`, `0db86c4d4e6`, `6dd9d38205a` | `postmaster/walredo.c` (new), `walredo.h`, `main.c`, `postmaster/Makefile`, `postmaster/meson.build`, `postmaster.h`, `tcop/postgres.c`, `tcopprot.h`, `heap/visibilitymap.c`, `doc/src/sgml/config.sgml` |
| C3 | control-file write hook, LSN-versioned pg_control mirror, restartpoint ordering | `e2650f9cffb`, `3cdbed7c33e`, `0a23790e225`, `fd4c6f2ded2`, `1c65c1b425a`, `00ebe43db73`, `c24caa82cec`, `8d140241824`, `82ae54c6ae6`, `18b0d418ab8`, `95f7593d53c`, `58c8ff5c369`, `c8053577b8a`, `fc409dc117b`, `87b6fe0f104`, `ba9baf898fc` | `transam/xlog.c`, `xlog.h`, `xlogrecovery.c`, `xlogrecovery.h`, `catalog/pg_control.h` |
| C4 | SLRU read/write hooks and store-backed mirrors | `ec89df7fadf`, `317c69cdc54`, `d2c50638f95` | `transam/slru.c`, `slru.h`, `clog.c`, `commit_ts.c`, `multixact.c` |
| C5 | pinned/advancing reader: read horizon, snapshot pinning, read-only enforcement | `a710f29e37d`, `b8faccc7201`, `fe5f36f2a66`, `8521a7f7818`, `5c135ee76a6`, `f736ff6f92b`, `38f5834e09e`, `5def2a80515`, `22b1e577561`, `5f2cba89711`, `e677c12af20`, `f48c4e25887`, `39b998a8e5e`, `5ab5bf9e896`, `bc18352304b`, `841a79f0fe8`, `4196143380d`, `74dc472d58d`, `cca6d192978` | `procarray.c/h`, `snapmgr.c/h`, `bufmgr.c`, `buf_internals.h`, `bufmgr.h`, `xact.c/h`, `varsup.c`, `transam.h`, `commit_ts.c/h`, `postinit.c`, `miscadmin.h`, `variable.c`, `xid8funcs.c`, `lockfuncs.c`, `locktag.h`, `lmgr.c`, `relmapper.c/h`, `pruneheap.c`, `autovacuum.c`, `bgwriter.c`, `wait_event_names.txt`, `datachecksum_state.c`, `system-views.sgml` |
| C6 | fork-event stamping and ordering seams | `541b950dfbd`, `d055356e4ab`, `6ad088dcbb9`, `8559a3d5355` | `catalog/storage.c`, `catalog/index.c`, `heapam_handler.c`, `commands/sequence.c`, `commands/tablecmds.c`, `bufmgr.c`, `xact.c`, `xlog.c` |
| C7 | build wiring | `11e69d85266` | `contrib/meson.build`; `contrib/Makefile` is not touched on `pagestore` but every `branchdb_N` adds the subdir there |

`datachecksum_state.c` (C5) is gone from `REL_19_STABLE` too (upstream
reverted online data checksums after the `pagestore` base) and from every
`branchdb_N` target -- the hunk touching it is dropped, not ported, on every
major, including the eventual `pagestore`-onto-`REL_19_STABLE` merge (P0).

## 2. `contrib/pagestore`'s version-specific API surface

`contrib/pagestore` had zero `#if PG_VERSION_NUM` guards before this PR
(P1), which added exactly one (`pagestore_slru.c`'s
`access/multixact_internal.h` vs `access/multixact.h` include, see below).
**That one guard does not make the file, or the module, build on 18.** A
real `-Dbuildtype=debug` compile of `contrib/pagestore` against a plain
`REL_18_6` tree (i.e. without the C1-C7 core series, since `branchdb_18`
does not exist yet) fails with the errors below. Most of them are core-side
(missing hook types, `SlruDesc`, etc.) and are **not contrib's job to
guard** -- they go away once the C1-C7 series lands on the target major
(P2). The ones listed here are the subset that is `contrib/pagestore`
calling a core symbol whose *name, arity or field layout* differs between
18 and 19, independent of whether the core series is present, and so must
be guarded in `contrib/pagestore` itself before a release branch's
`sync-contrib` can copy a working tree. **None of these are fixed by this
PR**; they are tracked here for the separate "PG 18 compatibility guards"
PR being prepared from the P2 work.

| Symbol | 19 (pagestore's base) | 18 | Sites (file:line, this PR's HEAD) |
|---|---|---|---|
| `GetMultiXactInfo()` | declared in `access/multixact.h`, reads `MultiXactState`'s `oldestMultiXactId`/`oldestOffset` | **no public API** exposes `oldestMulti`/`oldestOffset` before 19; `ReadMultiXactCounts()` (static, `multixact.c`) is the closest and does not return them | `pagestore_slru.c:668` (`ps_slru_tomb_horizon_cutoff`) |
| `MultiXactIdToOffsetPage()`, `MXOffsetToMemberPage()` | public `static inline` in `access/multixact_internal.h` | `static` in `access/transam/multixact.c`, not exported | `pagestore_slru.c:687,690` -- **fixed in this PR** (identical formula re-implemented under the `#else`, see the guard's comment) |
| `ReplOriginId` | `access/xlogdefs.h` typedef, renamed from `RepOriginId` | `RepOriginId` (`replication/origin.h`) | `pagestore.c:212,3802,4094,4113,4124,4334,...`; `pagestore_slru.c:623,628` |
| `XLogFindNextRecord()` | 3 arguments | 2 arguments | 10 sites in `pagestore.c` |
| `xl_multixact_truncate` fields | `oldestMulti`, `oldestOffset` | `startTruncOff`/`endTruncOff`, `startTruncMemb`/`endTruncMemb` | `pagestore.c:4601,4780,4864,5384-5385,5555` |
| `CHECKPOINT_FAST` | `CHECKPOINT_FAST` | `CHECKPOINT_IMMEDIATE` (no `CHECKPOINT_FAST` flag) | `pagestore.c:3222,13718,13754` |
| `CheckPoint.logicalDecodingEnabled` | present | present on 18 too, but verify against the 18 `CheckPoint` struct actually in use once C3 lands (needs re-check once the core series is ported, since C3 touches checkpoint-adjacent code) | `pagestore.c:3478` |
| `CheckPoint.dataChecksumState` / `ControlFileData.dataChecksumState` | **removed** from `REL_19_STABLE` (upstream reverted online data checksums after the `pagestore` base; the same revert P0 must apply) | present (18 still has online checksums) | `pagestore.c:3491` -- this one breaks the *other* way: it compiles on 18 as-is but will break on `pagestore` once P0 merges `REL_19_STABLE`; not an 18-only gap |
| `ControlFileData.slru_pages_per_segment`, `SLRU_PAGES_PER_SEGMENT` | `SLRU_PAGES_PER_SEGMENT` moved to `pg_config_manual.h` (compile-time constant, not in `ControlFileData` the same way) | `SLRU_PAGES_PER_SEGMENT` in `access/slru.h`; check whether `ControlFileData.slru_pages_per_segment` exists on 18 at all before assuming a rename | `pagestore.c:3877-3893`; `pagestore_control_restore.c:346,572` |
| `PageSetChecksum()` | `PageSetChecksum()` | `PageSetChecksumInplace()` | `pagestore.c:2784` |
| `BGWORKER_INTERRUPTIBLE` | present | needs verification against 18's `bgworker.h` (may not exist; if so, needs a version-gated flag expression, not a straight rename) | `pagestore.c:13637,14157` |
| `T_RepackStmt` | present (`REPACK` statement) | needs verification whether `REPACK` exists on 18 at all (if it is a 19+ feature, the whole code path guarding `backend_localsvc.c:2210` needs a `#if`, not just the node tag) | `backend_localsvc.c:2210` |

Process for adding a guard here: confirm the exact 18 shape against
`upstream/REL_18_STABLE` (`git show upstream/REL_18_STABLE:<path>`, the same
way this PR verified the `multixact_internal.h` guard -- do not guess), add
the `#if PG_VERSION_NUM` guard on `pagestore`, update this table, and re-run
a real compile against `REL_18_6` (ideally once `branchdb_18-rc` exists and
carries the C1-C7 series, so the compile actually reaches these sites
instead of failing earlier on a missing core hook).
