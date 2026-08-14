# Multi-compute read consistency: design

Status: increments 1a (the page-read pin), 1b (as-of size/existence
metadata), 1c (the complete fixed-reader boot contract, including catalog
provenance), 1d (the same-LSN admission fence), 2a (buffer generations),
2b (candidate publication), 2c (running-XID artifact transport), and 2d
(transaction-boundary view adoption), including automatic snapshot derivation
and per-database publication, are implemented.  Increment 3's transaction-boundary
read-your-writes handoff policy is also implemented.

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

- The exact-R `pg_xact` artifact identifies every assigned xid that was still
  `IN_PROGRESS` or `SUB_COMMITTED` at R.  Deriving the set from this artifact
  avoids treating a nearby `XLOG_RUNNING_XACTS` record as if it occurred at
  the checkpoint redo pointer.
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
   refused: relation extend/create/unlink/truncate, page writes, branch
   creation, WAL append, WAL-index append, and object writes all error.
   Page writes fail closed wholesale -- with hint-bit dirtying suppressed
   at the source (below) nothing hint-only can reach the write path, so
   anything arriving is a real mutation that escaped read-only mode,
   including WAL-less writes (unlogged relations, fake-LSN index pages)
   whose pd_lsn never advances past the pin.  Because R never moves,
   shared-buffer staleness is a non-issue.

   Before startup performs its first store lookup, the reader durably registers
   `PS_RETENTION_OWNER_READER` for page history, WAL, and the WAL index at R.
   The controller supplies a stable nonzero `pagestore.retention_owner_id` and
   a monotonically increasing `pagestore.retention_owner_generation`; stale
   authority or an ambiguous registration failure aborts startup.  A restart
   repeats the same idempotent SET.  Crashes and ordinary shutdown do not DROP
   the record: only an explicit controller deprovision may release it, so
   uncertainty always retains data.

   A pinned compute must also generate NO page-content WAL of its own:
   with checksums on, a plain SELECT sets hint bits and emits an
   `FPI_FOR_HINT` carrying the as-of-R page image; replaying that WAL
   after the pin is lifted would republish the stale image ABOVE the
   writer's shipped versions and rewind history for every later reader.
   So a pinned start layers its defenses, outermost first: the read-only
   state is FORCED (`transaction_read_only_forced` in core: every
   transaction starts read-only and SET TRANSACTION READ WRITE is refused
   exactly as during recovery -- the hot-standby enforcement model), which
   holds every PreventCommandIfReadOnly path: DML, DDL, TRUNCATE,
   nextval(), COPY FROM; XID assignment itself is refused at
   GetNewTransactionId (an assigned XID forces a WAL'd commit record, so
   pg_current_xact_id() and friends fail cleanly instead of PANICking at
   commit), and the data-checksum control functions and their
   page-rewriting workers are refused/exited too; executor and utility
   gates additionally refuse DML, row locking, VACUUM/ANALYZE, REINDEX,
   REPACK, COPY FROM and NOTIFY at the entry points (all legal in
   read-only transactions or XID-assigning, and they get the clearer
   pinned-reader error); hint-bit dirtying and on-access pruning are suppressed (the
   `page_maintenance_suppressed` seam in core, which also shuts down the
   autovacuum launcher/workers -- including the wraparound-defense
   launcher that ignores `autovacuum = off` -- and bgwriter standby
   snapshots); `wal_insert_restricted` (core) refuses WAL insertion from
   anything but the WAL-essential auxiliary processes, so even a direct
   C-level bypass of the gates cannot plant replayable page-content WAL;
   it also forces `autovacuum = off` and `default_transaction_read_only =
   on`, disables the SLRU/control mirrors (nothing legitimate to
   publish),
   refuses to start at all if the localsvc backend is not active (the pin
   would force the server read-only while capping nothing) or if
   `archive_mode` is on: a pinned reader must never archive.  No
   archiver-side heuristic can tell, after the pin is lifted, which
   retained bytes were the reader's own (a floor recorded at the
   archiver's first call starts too late after archiver lag; a mixed
   segment's private suffix would ship as gap-fill the store has no
   coverage to refuse), so the combination is refused up front and the
   operator disables archiving for the pinned run.  What pinned-era WAL
   remains is meta-only -- the shutdown checkpoint record (periodic
   checkpoints self-skip with no WAL activity) -- and on an unpinned
   restart of the SAME cluster the completed segments ship as one true
   chain, gapless.  Behind that sits a STORE-side invariant: wal_append
   refuses a chunk whose bytes differ from already-shipped coverage of
   the same LSN range (identical re-ships stay idempotent and add no
   duplicate chunk), so a divergent compute -- a clone unpinned into a
   would-be writer, or an accidental second writer -- cannot rewrite
   recorded history that later-chunks-win reads would otherwise adopt.
   Reads of WAL-less content fail closed too: a stored relation-page
   version with LSN 0 (unlogged relations, skip-WAL builds) is not
   LSN-ordered, so a capped read refuses it rather than serve whatever
   the writer last flushed.  The store-write refusals above remain as
   the fail-closed backstop.  This is the
   MECHANISM increment: on its own it freezes page bytes, not the whole
   compute.

1b. **As-of metadata (implemented).**  Without it, `NBLOCKS`/`EXISTS`
   answer newest, so a writer-side truncate/drop after R shrinks the
   frozen view.  The store now versions fork sizes: every page append
   records growth at the block's own pd_lsn (so as-of NBLOCKS agrees
   with as-of page reads block for block), and create/truncate/unlink/
   zero-extend carry the backend's WAL position and land as definitive
   events -- create and truncate SET the size, unlink marks the fork
   DEAD, all COW (history below the event still resolves).  A pinned
   reader's smgr NBLOCKS/EXISTS pass its pin as the horizon; a branch's
   size walk caps at each fork point exactly like page reads, which
   also fixes the old leak of parent growth into a branch.  Definitive
   events persist in a fork-meta log (loaded before the segment scan,
   so recovery re-derives growth from the segment records' LSNs against
   the same definitive backdrop the live path saw); growth needs no
   extra persistence.  Note: WAL-less pages (unlogged relations) carry
   pd_lsn 0; their growth is ordered at the fork's newest definitive
   event (create/truncate) so the newest size stays right -- their
   content is not LSN-ordered to begin with, and unlogged crash-reset
   semantics are outside what LSN-versioning can express.

1c. **The as-of compute (boot artifacts and fixed snapshot implemented).**  A consistent QUERY compute
   at R needs its LOCAL state as-of R too: catalogs, pg_control, and
   SLRUs come from the prepared-branch-style artifacts restored at R (a
   pinned reader does NOT create a store timeline -- it reads the
   writer's timeline as-of R, which `tl_walk`/`page_visible` already
   serve).  The boot half uses `pagestore_prepare_reader` to materialize
   the SLRUs without creating a timeline, then
   `pagestore_install_prepared_reader` installs them and publishes a durable
   `pagestore_reader.manifest` last.  `pagestore_control_restore --lsn R`
   installs the exact-R control image before startup.  A configured pin fails
   startup unless the manifest's timeline/read LSN and `pg_control` redo all
   agree.  Prepared readers require `pagestore.route_all`: every non-temporary
   relation must use the versioned store, including default/global tablespace
   relations that would otherwise bypass the pin through local `md` files.  A
   reader on a nonzero timeline also records that timeline's parent and fork
   LSN in its manifest.  Prepare and install verify the ancestry against the
   daemon, and startup re-verifies it after the target branch manifest has been
   removed, so an undefined or reused timeline cannot silently lose its parent
   history.  Prepare also scans the reconstructed exact-R `pg_xact` horizon
   and durably writes every in-progress/subcommitted xid to
   `pagestore_reader.snapshot`, with its timeline, R, xmin/xmax, and CRC32C.
   Install and startup require and validate that artifact.  A pinned backend's
   snapshot hook uses its fixed xmax and running set for every MVCC snapshot,
   so a transaction in flight at R remains invisible even if local recovery
   later replays its commit.

   Catalog provenance is part of the boot contract too.  The control plane
   restores the target copy's pg_control to exact R and calls
   `pagestore_mark_reader_catalog_snapshot`; that publishes a CRC-protected
   artifact bound to the system identifier, timeline, and R.  Reader manifest
   format 2 declares that artifact, and install plus every startup validate it
   before accepting the local catalog snapshot.  The control plane is still
   responsible for producing the snapshot under quiescence, or from recovery
   at R; the protocol no longer permits an unstamped or differently stamped
   catalog directory to boot as the reader.
1d. **The same-LSN admission fence (implemented).**  Relation versions are keyed by
   pd_lsn, and the writer's hint-bit-only page writes re-ship a page
   under an UNCHANGED pd_lsn: a version written after R can win a
   newest-<=-R resolve because it ties at the same LSN.  The bytes differ
   only in hint bits -- but those assert commit status decided possibly
   after R.  Every page append and fork event now carries a daemon-global,
   monotone admission sequence.  The sequence is durable in segment records,
   image-layer v4 indexes, and forkmeta v2 records, and is preserved through
   flush, compaction, segment reclamation, and recovery.  At the checkpoint
   boundary the control hook publishes a shared admission gate for R without
   doing store I/O.  Daemon workers defer new relation mutations at or below R;
   the later control drain takes an exclusive admission barrier, which waits for
   every mutation already admitted on every shard and assigns the fence
   sequence.  Only after block 2 carries `(R, sequence)` and all control writes
   are synced does the drain release the gate.  A pinned compute resolves that
   exact marker at first access and sends `(R, sequence)` on relation page,
   EXISTS, and NBLOCKS reads.  Selection orders by
   `(lsn, admission_sequence)` and rejects versions/events above the fence.
   Stores with legacy records can be upgraded in place (legacy sequence zero
   predates a newly published fence), but a pinned read fails closed until an
   exact block-2 marker for R exists.  WAL-less relation content remains
   fail-closed: checkpoint R does not prove those pages complete even though
   their store admission order is known.

2. **The advancing reader (2a-2d implemented through view adoption).**
   R advances by re-deriving from
   the control mirror (the SLRU reader's TTL/epoch protocol, generalized).
   Increment 2a adds an out-of-core storage-manager read generation to
   shared-buffer tags; pagestore uses generation 1 for a pinned reader, so its
   buffers cannot alias the writer's generation-0 buffers.  Future generations
   allow old and new horizons to coexist while transactions drain.
   Increment 2b lets an opt-in reader discover the newest durable control
   checkpoint at snapshot boundaries, validates its exact admission fence, and
   publishes the candidate plus a monotone shared generation.  Discovery does
   not change the effective view: adoption remains gated on publishing and
   loading the exact-R running-XID snapshot.  Increment 2c provides that
   transport: the control plane can publish an existing CRC-protected snapshot
   as versioned `PS_KLASS_READER_SNAPSHOT` data blocks followed by its manifest.
   Both identities bind timeline and R; readers require exact-R versions of the
   manifest and every block, validate both CRC layers, and sync the observed
   objects before accepting them.  Increment 2d resolves the newest published
   snapshot at or below the discovered candidate at top-level transaction
   start, so a publisher that trails checkpoint discovery remains adoptable.
   It resolves the exact admission fence and checkpoint control state, advances
   the local full-XID, multixact, and commit-timestamp interpretation horizons,
   and then atomically
   swaps the backend's `(R, generation, snapshot)`.  Buffer
   tags use the effective generation, so pages from the old and new views cannot
   alias.  A missing, corrupt, or temporarily unavailable candidate artifact
   leaves the old view intact.  Adoption also invalidates catalog, relation,
   and plan caches before the transaction can use the new view.  Transaction
   advancement is retention-atomic.  The MVP serializes advancing-reader
   transactions behind one shared gate, waits for the old-view transaction to
   leave, durably replaces the reader owner's pin with the new R, and only then
   swaps the backend view.  Every transaction re-reads the durable owner record,
   so another backend's advance or a crash between durable SET and shared-memory
   publication cannot expose an unprotected old view.  A definitive rejection
   leaves both the old pin and old view intact; an ambiguous result aborts the
   transaction and reconciles from the durable record on retry.  A stale
   generation terminates the fenced compute.  Startup likewise preserves an
   already-advanced same-generation pin and adopts it before serving.  As with
   every prepared reader restart, the controller first reinstalls the manifest's
   exact boot-R `pg_control`; pinned shutdown checkpoints are local process
   artifacts and are not a replacement boot identity.  This intentionally
   favors correctness over read concurrency until per-epoch owner slots replace
   the gate.  Transaction status is read from the newest complete live SLRU
   image; the exact-R running set masks commits after R.  If the writer has
   since tombstoned a status page,
   the reader retains its prepared local seed instead of letting newest
   truncation erase history needed by old relation pages.  Commit-timestamp
   validity bounds are backend-local, so transactions on different adopted
   views do not overwrite one another's range.  The published snapshot manifest
   also binds the writer's global and database relation-map checksums; adoption
   fails closed when the reader's prepared maps differ, rather than combining a
   boot-time mapped relfilenumber with newer catalog pages.  Advancing readers force
   non-parallel planning because worker transaction state does not yet carry
   this extension's view identity.  Checkpoint completion only replaces a
   shared latest-job slot; a dedicated worker reconstructs and publishes the
   database-independent exact-R running-XID snapshot as a staging artifact.
   The worker scans current CLOG once for transactions still running, samples
   and flushes its WAL endpoint, then makes one pass over `(R, endpoint]` to
   add transactions that finished after R.  This needs no manually seeded CLOG
   base, does not rescan WAL per CLOG page, and keeps horizon work and store I/O
   out of the checkpoint drain.  Database-specific catalog and relation-map
   provenance remains a control-plane action.  A database publisher reads both
   maps while holding `RelationMappingLock`, samples the WAL position after the
   reads, and durably publishes changed bytes at that position.  It can bind
   those maps only to a staged checkpoint whose R covers the sampled position;
   recovery or a failed publication simply causes the current durable maps to
   be sampled again.  Unchanged maps retain their prior sampled position.  The
   publisher then emits the adoption manifest.
   The staging marker is distinct from that manifest, so an incomplete
   per-database artifact cannot become a reader candidate.  When
   `pagestore.auto_reader_artifacts` is enabled on a fully routed writer (the
   server rejects the option without `pagestore.route_all`), a controller
   enumerates connectable databases and runs one short-lived database worker
   at a time.  This bounds the scheduler to two worker slots regardless of the
   database count.  Each worker primes its relation maps and binds the newest
   complete staged snapshot to a database manifest; unchanged artifacts are
   not rewritten, so publication no longer requires control-plane scheduling
   or generates idle store traffic.  Advancing-reader artifact publication
   requires this automatic mode on the writer; the manual per-database SQL
   probe does not publish the instance-wide barrier.  The launcher locks
   `pg_database` against create/drop, selects a fresh checkpoint while holding
   that lock, and keeps it through every per-database manifest and the final
   barrier.  The barrier records the exact ordered `(database OID, default
   tablespace OID)` set at R, so readers neither infer membership from
   non-connectable PGDATA directories nor mix the set with later catalog state.

3. **Read-your-writes handoff (implemented).**  A writer hands a session over
   to a reader with `pagestore_writer_handoff_token()`, which returns the
   writer's timeline and current insert LSN in an opaque token.  The writer must
   request it in a new transaction after committing its writes; token generation
   rejects transaction blocks and transactions that have assigned an XID.  This
   ensures the commit record precedes the token.  Its implicit transaction is
   required to be the sole expression of a FROM-less SELECT and permanently
   forces the backend read-only, so neither the remainder of the current
   statement, a libpq fast-path function call, nor an extended-query pipeline
   can append a write after the sampled LSN.  Issuance permanently seals that
   writer backend against later query execution and utility commands, while
   allowing only non-writing transaction
   control to finish; the router must close or transfer the source connection.
   Token issuance also requires
   full relation routing.  Readiness rejects a token from another timeline.  At the
   start of a subsequent transaction the reader first performs normal view adoption, then
   `pagestore_reader_handoff_ready(token)` reports whether its effective
   R covers the token.  A false result tells the session router to retry in a
   new transaction; the API never changes snapshots in the middle of a
   transaction.

## Non-goals

- Multi-writer timelines (the live SLRU mirror's single-writer invariant
  stands).
- Sub-checkpoint freshness for readers: R advances at the writer's
  checkpoint cadence by design.  Tighter horizons would need the writer to
  publish per-batch page-write fences -- possible later, orthogonal.
