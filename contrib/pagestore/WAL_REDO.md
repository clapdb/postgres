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
   - **3b** Bring up a PG node in standby/recovery with `route_all` on the store
     and point its WAL source at the store; verify it materializes pages.
   - **3c** Materialize-on-demand: when a read misses a page at an LSN, drive
     redo for just that page (Neon's per-page model) instead of replaying
     everything.  This is where a `--wal-redo`-style single-page helper, or a
     bounded recovery, comes in.
   - **3d** Branch WAL read-through (serve a branch's WAL across its fork point)
     and SLRU/clog + `pg_control` on the store, so an independent compute can
     run on a branch with no local state.

## Known scope boundaries

Until 3c/3d land, branches remain single-compute-at-a-time and WAL/SLRU/control
are not fully branched (see compute_on_branch_demo.sh).  The redo worker is the
piece that removes those boundaries.
