# Multi-compute read consistency: design

Status: increment 1a implemented (the page-read pin); increments 1b-3 are
specified here and not yet built.

## Problem

One compute per timeline WRITES through the store (the branch's primary).
Nothing today defines what a SECOND compute on the same timeline may
correctly read.  The pieces that exist pull in different directions:

- Normal relation reads resolve at `UINT64_MAX` -- always the newest store
  version (`PS_OP_READV` -> `read_resolve(..., UINT64_MAX, ..)`).  There is
  no per-compute read LSN.
- A store page version becomes visible to reads the moment the writer's
  `smgrwrite` returns, BEFORE any fsync: segment fsync is deferred to the
  writer's checkpoints (`PS_OP_IMMEDSYNC`).  A "newest" reader can observe
  a version that a writer crash then makes never-have-existed.
- The writer's shared buffers hold dirty pages for arbitrarily long.  The
  store's newest version of a page can lag the writer's logical state by a
  full checkpoint cycle, and different pages lag DIFFERENTLY -- "newest of
  every page" is not any LSN-consistent state of the database.
- MVCC visibility needs transaction status coherent with the page bytes.
  The SLRU live mirror serves NEWEST status gated by a completeness
  watermark -- fine for the fail-closed status lookups it was built for,
  but newest status against lagging pages (or vice versa) tears
  transactions: one tuple of xact X visible through fresh clog while X's
  second tuple is missing from a stale page.

## The read horizon R

Everything follows from one observation: **the redo pointer of the last
completed checkpoint whose pg_control image durably shipped is already a
correct, store-published, relation-page horizon.**

- A completed checkpoint wrote back and durably synced (CheckPointGuts ->
  smgr sync -> `PS_OP_IMMEDSYNC`) every page dirtied before its redo
  pointer.  So for every page, the store durably holds a version >= the
  page's state as of redo: as-of-redo reads (`READ_AT`, `page_visible`'s
  newest-`<=`-R rule) are complete -- no in-flight writer state is missing
  from them.
- The control mirror ships exactly this (an LSN-versioned control image at
  every `UpdateControlFile`, post+sync), so a reader can derive R with one
  as-of read of `PS_KLASS_CONTROL`: **R = checkPointCopy.redo of the newest
  durable control image**.  No new writer-side machinery at all.
- The SLRU visibility watermark is the SAME LSN by construction (its
  candidate is that same redo, gated on the SLRU mirror's own pending
  ships), so SLRU status coverage and relation-page coverage meet at R.

## Snapshot semantics at R

Pages as-of R plus NEWEST commit status still tears (a tuple written at
lsn <= R whose xact commits after R flips visible early, while the xact's
later tuples are missing from as-of-R pages).  Status must be judged AS OF
R.  Reconstructing clog-as-of-R per lookup is the appliers' job and far
too heavy for the read path; the standby trick is not:

- The checkpoint whose redo defines R logged the set of xids running at
  it (`oldestActiveXid`, and the `XLOG_RUNNING_XACTS` record near redo).
- An xact NOT in that running set with newest-clog = committed must have
  committed at/before R (had it committed later, it would still have been
  running at R).  An xact IN the set was uncommitted at R -- invisible,
  whatever newest clog says now.

So a reader query's snapshot at R is `(nextXid-at-checkpoint, running set
at redo)`: a static, per-R snapshot exactly like a hot-standby snapshot at
that record, evaluable against NEWEST-wins mirrored clog for every xid
outside the running set.  Repeatable, transaction-atomic, no per-lookup
reconstruction.

## Increments

1a. **The page-read pin (implemented).**  `pagestore.read_lsn`
   (POSTMASTER): every store relation page read is capped at R --
   `PS_OP_READV` gains an honoured `req_lsn` (0 keeps the newest
   semantics, so the writer path is untouched) -- and store mutations are
   refused: relation extend/create/unlink/truncate, WAL append, WAL-index
   append, and object writes all error; page writes are dropped when
   hint-bit-only (pd_lsn <= R; they must not fork the frozen history and
   re-derive on later reads) and refused loudly when the page LSN
   advanced past R (a real local mutation that escaped read-only mode).
   Because R never moves, shared-buffer staleness is a non-issue.

   A pinned compute must also generate NO page-content WAL of its own:
   with checksums on, a plain SELECT sets hint bits and emits an
   `FPI_FOR_HINT` carrying the as-of-R page image; replaying that WAL
   after the pin is lifted would republish the stale image ABOVE the
   writer's shipped versions and rewind history for every later reader.
   So a pinned start suppresses hint-bit dirtying of shared buffers and
   on-access pruning (the `page_maintenance_suppressed` seam in core),
   forces `autovacuum = off` and `default_transaction_read_only = on`,
   disables the SLRU/control mirrors (nothing legitimate to publish),
   and pauses WAL archiving (segments stay `.ready` and ship after
   unpinning, keeping the store's WAL history gapless).  The store-write
   refusals above remain as the fail-closed backstop.  This is the
   MECHANISM increment: on its own it freezes page bytes, not the whole
   compute.

1b. **As-of metadata (next).**  `NBLOCKS`/`EXISTS` still answer newest, so
   a writer-side truncate/drop after R shrinks the frozen view.  Needs
   fork-size versioning in the store (a per-fork (lsn, nblocks) history
   fed by the extend path's page LSNs plus an LSN-stamped truncate op,
   made recoverable) so size and existence resolve as-of R.

1c. **The as-of compute (boot + snapshot).**  A consistent QUERY compute
   at R needs its LOCAL state as-of R too: catalogs, pg_control, and
   SLRUs come from the prepared-branch-style artifacts restored at R (a
   pinned reader does NOT create a store timeline -- it reads the
   writer's timeline as-of R, which `tl_walk`/`page_visible` already
   serve).  Transaction visibility must not consult newest status: wire
   the running-xacts snapshot (above) so tuples of xacts in flight at R
   stay invisible whatever newest clog says.

1d. **The same-LSN admission fence.**  Relation versions are keyed by
   pd_lsn, and the writer's hint-bit-only page writes re-ship a page
   under an UNCHANGED pd_lsn: a version written after R can win a
   newest-<=-R resolve because it ties at the same LSN.  The bytes differ
   only in hint bits -- but those assert commit status decided possibly
   after R.  The store needs an admission-order fence for capped reads
   (e.g. versions carry a monotone store sequence, and R pairs with the
   sequence the control image shipped at), or same-pd_lsn re-admissions
   must be rejected below the pin.

2. **The advancing reader.**  R advances by re-deriving from the control
   mirror (the SLRU reader's TTL/epoch protocol, generalized): adopting a
   new R invalidates relation buffers read under the old R.  Needs a
   buffer-tag epoch (the SLRU served-table pattern applied to shared
   buffers via an smgr read-through revalidation) or a bulk drop on adopt.
   The snapshot (running set) re-derives with each R.

3. **Read-your-writes handoff.**  A writer hands a session over to a reader
   with a token (the writer's current insert LSN); the reader serves the
   session only once R >= token.  Pure policy on top of increment 2.

## Non-goals

- Multi-writer timelines (the live SLRU mirror's single-writer invariant
  stands).
- Sub-checkpoint freshness for readers: R advances at the writer's
  checkpoint cadence by design.  Tighter horizons would need the writer to
  publish per-batch page-write fences -- possible later, orthogonal.
