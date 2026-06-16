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
   materializes pages into the store. 🔶 In progress:
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
       - **3c-2** A `--wal-redo`-style single-page helper: take a page's newest
         stored image plus its indexed records up to L and apply rm_redo.
   - **3d-2/3** SLRU/clog + `pg_control` on the store, and branch WAL
     read-through (serve a branch's WAL across its fork point), so multiple
     independent computes can run concurrently on different branches with no
     shared local state.

## Known scope boundaries

Until 3c/3d land, branches remain single-compute-at-a-time and WAL/SLRU/control
are not fully branched (see compute_on_branch_demo.sh).  The redo worker is the
piece that removes those boundaries.
