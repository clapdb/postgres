# WAL redo design (reconstructing pages from WAL in the store)

This is the third and largest WAL-shipping layer: having the store **reconstruct
pages by replaying WAL**, so that a read compute can be stateless and a branch
is a complete clone.  It is a multi-step effort; this document records the plan
and what is implemented so far.

## Why not reimplement redo

PostgreSQL applies WAL per resource manager (`heap`, `btree`, `gin`, `xact`,
`clog`, ... — each with an `rm_redo`).  Reimplementing all of that in the daemon
would be rewriting half of PostgreSQL and would break every major version.

So we **reuse PostgreSQL's own redo** (the approach Neon takes).  The cleanest
realization that needs *no* redo reimplementation: a dedicated PostgreSQL
instance in **continuous recovery** whose storage is the page store —

- relations routed to the store (`pagestore.route_all`),
- WAL fed from the store (the shipped WAL, M6),
- recovery's `rm_redo` reads base pages from the store via smgr, applies the
  records, and writes the resulting pages back to the store via smgr.

That "redo worker" materializes pages into the store using stock recovery code.
Write computes ship WAL; the redo worker turns WAL into pages; read computes
read pages at an LSN.

## Layers and status

1. **WAL transport** — compute ships WAL to the store. ✅ (M6: archive module →
   `PS_OP_WAL_APPEND`, per-timeline `wal_<tl>` log.)
2. **WAL serving** — store hands WAL back by LSN range, so a redo worker can pull
   it. ✅ (`PS_OP_WAL_READ`; `wal_read()` assembles bytes across records.)
3. **Redo worker** — a recovery PostgreSQL that consumes the store's WAL and
   materializes pages into the store. 🔶 3a-3d-1 and 3c-1..4 done; 3d-2/3 partial:
   - **3a** Reconstruct standard WAL segment files from the `wal_<tl>` log. ✅
     `pagestore_walrestore` does this and works as a `restore_command`
     (`pagestore_walrestore --shm NAME --timeline N --segsize B %f %p`); the
     integration test reconstructs a shipped segment as a full standard segment.
   - **3b** Bring up a PG node in archive recovery with `route_all` on the store
     and its WAL fetched from the store; verify it materializes pages. ✅
     `redo_worker_demo.sh`: a base backup (`pg_backup_start`/`stop`) marks the
     recovery start, then the instance recovers with
     `restore_command = pagestore_walrestore` and empty local pg_wal, so all WAL
     comes from the store; recovery's rm_redo replays it into the store and the
     post-backup change is recovered.  Reuses PostgreSQL's redo wholesale.
     Caveat: the redo instance runs with `recovery_prefetch = off` (the
     backend's recovery-prefetch/AIO path is not wired yet).
   - **3d-1** WAL-only compute -> non-redundant redo. ✅ `wal_only_redo_demo.sh`:
     the writer runs with `route_all = off`, so its relation pages stay local
     and only its WAL is shipped; the redo worker (`route_all = on`) then
     materializes the relations into the store purely by replaying that WAL.
     Verified the table never reached the store from the compute (its file is
     local) yet exists in the store after redo, and the store grew.  This is the
     point of redo: the store's pages come from WAL, not from the compute.
   - **3c** Materialize-on-demand: when a read misses a page at an LSN, drive
     redo for just that page (Neon's per-page model) instead of replaying
     everything.
       - **3c-1** Per-page WAL index. ✅ The store maps (timeline, key, block) ->
         the LSNs of WAL records that modify that page (`PS_OP_WAL_INDEX_ADD` /
         `PS_OP_WAL_INDEX_GET`, with branch read-through capped at the fork LSN).
         This is the lookup the single-page redo needs.  (Populating it by
         decoding shipped WAL via PostgreSQL's XLogReader / pg_walinspect is next;
         reimplementing the WAL format in the daemon is deliberately avoided.)
       - **3c-2** Populate the index by decoding shipped WAL. ✅ Reuses
         PostgreSQL's own WAL reader (`read_local_xlog_page`), exposed as
         `pagestore_index_wal(start, end)`.  Note: decoding **must run in a
         normal backend** -- the archiver process lacks the recovery/timeline
         context the reader asserts on (both `read_local_xlog_page` and `WALRead`
         abort there).  With `pagestore.auto_wal_index`, a background worker
         continuously decodes the durable shipped prefix and resumes from the
         store's durable progress marker after restart.  The SQL function remains
         available for targeted tests.  No daemon-side WAL parser is written.
       - **3c-3** Reconstruct a single page's base image from WAL. ✅
         `pagestore_redo_page(rel, fork, block, lsn)` uses the per-page index to
         find the newest full-page image at/below lsn and restores it
         (`RestoreBlockImage`) -- rebuilding one page from WAL on demand.  Note a
         WAL full-page image is the page *before* that record's change (torn-page
         protection), so this returns the base image, not the page exactly as-of
         lsn.
       - **3c-4** The `--wal-redo`-style helper: apply the delta records after
         the base image with `rm_redo` to get the page exactly as-of lsn. ✅
         `postgres --wal-redo` (src/backend/postmaster/walredo.c) holds a single
         page and runs each record's resource-manager redo against it with the
         buffer manager redirected (`am_walredo`); `pagestore_redo_page_asof()`
         drives it over the per-page index (base FPI + deltas).  Records from
         ancestor timelines are always fetched from the store; SAME-timeline
         records come from local pg_wal unless `pagestore.redo_wal_from_store`
         is enabled -- a no-local-WAL compute (a fresh branch) must set that
         GUC or local-timeline deltas fail to replay.  The integration test
         proves the materialized page contains a change that the base image
         alone lacks.  Caveats: a page with
         PS_REDO_MAX_RECS (4096) or more indexed records at/below the target
         LSN fails closed (the capped index result would otherwise be treated as
         complete), and when the last write is inherited from an ancestor
         timeline the truncate-liveness check fails closed (a truncate on the
         reading branch after the fork is not visible to the ancestor-WAL
         scan).
   - **3d-2/3** SLRU/clog + `pg_control` on the store, and branch WAL
     read-through (serve a branch's WAL across its fork point), so multiple
     independent computes can run concurrently on different branches with no
     shared local state.  🔶 Partial: branch SLRU state is solved as-of the fork
     point by the prepared branch flow (`pagestore_prepare_branch` seeds
     clog/commit-ts/multixact from base snapshots + WAL replay, and
     `pagestore_install_prepared_branch` installs them with a durable manifest;
     see SLRU_ON_STORE_DESIGN.md), and the per-page WAL index serves branch
     reads capped at the fork LSN.  ✅ Serving a branch compute's WAL across
     its fork point is DONE: the store's `wal_read` walks the branch
     ancestry (each hop capped at the child's fork LSN, since an ancestor's
     log continues past the fork with records that are not the branch's
     history), so `PS_OP_WAL_READ` -- and everything built on it: the
     walrestore tool, `redo_page_asof`'s store reader, the store-backed
     SLRU applier scans -- serves a timeline's full history.  `pg_control`
     on the store and live SLRU traffic through the store are done as well
     (PGCONTROL_ON_STORE_DESIGN.md, SLRU_ON_STORE_DESIGN.md).

## Known scope boundaries

Branch computes must be booted through the prepared branch manifest/install
flow so WAL/SLRU/control state matches the fork point.  The older same-PGDATA
timeline-switch demo has been removed because branch timeline startup now fails
closed without `pagestore_branch.manifest`.
