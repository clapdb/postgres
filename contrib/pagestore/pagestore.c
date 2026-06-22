/*-------------------------------------------------------------------------
 *
 * pagestore.c
 *	  smgr shim that routes relation I/O through a pluggable storage backend.
 *
 * This is the thin, per-major-version adapter half of the design: it
 * implements PostgreSQL's smgr (f_smgr) interface and translates each call
 * into the version-neutral PageStoreBackend operations declared in
 * pagestore_backend.h.  All the version-specific knowledge (smgr signatures,
 * vectored vs single-block, AIO) lives here; the backend below the boundary
 * stays portable across major versions.
 *
 * Load via shared_preload_libraries so the smgr implementation is registered
 * before backends are forked.  GUCs:
 *	 pagestore.route_all  -- if on, all non-temp relations use the backend
 *	 pagestore.backend	  -- which backend to use (default "passthrough")
 *
 * For M0 only the passthrough backend exists (forwards to md.c), so enabling
 * the module changes nothing observable -- which is exactly what the
 * regression suite verifies.
 *
 * Note: the asynchronous read path (smgr_startreadv) and the local-only hints
 * (prefetch, maxcombine, writeback, registersync, fd) are delegated straight
 * to md.c for now; only the synchronous data plane crosses the boundary.  Run
 * with io_method=sync to exercise the backend read path.
 *
 * src/../contrib/pagestore/pagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/relation.h"
#include "access/rmgr.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "archive/archive_module.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pagestore_backend.h"
#include "pagestore_ipc.h"
#include "port/pg_iovec.h"
#include "storage/aio.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* GUC state */
static bool pagestore_route_all = false;
static bool pagestore_route_user_tablespaces = false;
static char *pagestore_backend_name = NULL;

/* "which" index assigned to our smgr implementation by smgr_register() */
static int	pagestore_smgr_which = -1;

/* the active backend (selected by pagestore.backend) */
static const PageStoreBackend *pagestore_active_backend = &PageStoreBackendPassthrough;

/* --- backend registry --------------------------------------------------- */

#define PAGESTORE_MAX_BACKENDS 8
static const PageStoreBackend *pagestore_backends[PAGESTORE_MAX_BACKENDS];
static int	pagestore_nbackends = 0;

void
pagestore_register_backend(const PageStoreBackend *backend)
{
	if (pagestore_nbackends >= PAGESTORE_MAX_BACKENDS)
		elog(ERROR, "too many pagestore backends registered");
	pagestore_backends[pagestore_nbackends++] = backend;
}

const PageStoreBackend *
pagestore_lookup_backend(const char *name)
{
	for (int i = 0; i < pagestore_nbackends; i++)
	{
		if (strcmp(pagestore_backends[i]->name, name) == 0)
			return pagestore_backends[i];
	}
	return NULL;
}

/* --- helpers ------------------------------------------------------------ */

static inline PageStoreRelKey
pagestore_key(const RelFileLocator *locator, ForkNumber forknum)
{
	PageStoreRelKey key;

	key.spcOid = locator->spcOid;
	key.dbOid = locator->dbOid;
	key.relNumber = locator->relNumber;
	key.forkNum = (int32) forknum;
	return key;
}

#define ACTIVE() (pagestore_active_backend)

/* --- smgr shim: f_smgr entry points ------------------------------------- */

static void
ps_init(void)
{
	if (ACTIVE()->init)
		ACTIVE()->init();
}

static void
ps_open(SMgrRelation reln)
{
	/* md keeps per-fork fd state in the SMgrRelation; set it up. */
	mdopen(reln);
}

static void
ps_close(SMgrRelation reln, ForkNumber forknum)
{
	mdclose(reln, forknum);
}

static void
ps_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->create(&key, reln, isRedo);
}

static bool
ps_exists(SMgrRelation reln, ForkNumber forknum)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	return ACTIVE()->fork_exists(&key, reln);
}

static void
ps_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	PageStoreRelKey key = pagestore_key(&rlocator.locator, forknum);

	ACTIVE()->unlink(&key, isRedo);
}

static void
ps_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  const void *buffer, bool skipFsync)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->extend(&key, reln, blocknum, buffer, skipFsync);
}

static void
ps_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  int nblocks, bool skipFsync)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->zeroextend(&key, reln, blocknum, nblocks, skipFsync);
}

static bool
ps_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			int nblocks)
{
	/* local kernel readahead hint only applies to md-backed storage */
	if (ACTIVE()->uses_local_files)
		return mdprefetch(reln, forknum, blocknum, nblocks);
	return false;
}

static uint32
ps_maxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	if (ACTIVE()->max_combine_pages > 0)
		return ACTIVE()->max_combine_pages;
	return mdmaxcombine(reln, forknum, blocknum);
}

static void
ps_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 void **buffers, BlockNumber nblocks)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->readv(&key, reln, blocknum, buffers, nblocks);
}

static void
ps_startreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
			  BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	int			fd;
	uint64		offset;
	struct iovec *iov;
	int			iovcnt;
	PageStoreRelKey key;

	/*
	 * Backends without an async path (e.g. passthrough) use md's local-file
	 * read directly.
	 */
	if (ACTIVE()->fetch_to_fd == NULL)
	{
		mdstartreadv(ioh, reln, forknum, blocknum, buffers, nblocks);
		return;
	}

	/*
	 * Remote read: have the backend place the pages into a region exposed as
	 * (fd, offset), then issue a normal AIO readv from there into the buffer
	 * pool.  This reuses md's and bufmgr's completion callbacks verbatim --
	 * checksum verification, marking buffers valid, error handling -- because
	 * from PostgreSQL's perspective this is an ordinary vectored read of a
	 * real file descriptor.
	 */
	key = pagestore_key(&reln->smgr_rlocator.locator, forknum);
	if (!ACTIVE()->fetch_to_fd(&key, blocknum, nblocks, &fd, &offset))
		elog(ERROR, "pagestore: backend \"%s\" failed to fetch blocks",
			 ACTIVE()->name);

	iovcnt = pgaio_io_get_iovec(ioh, &iov);
	Assert(nblocks <= iovcnt);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		iov[i].iov_base = buffers[i];
		iov[i].iov_len = BLCKSZ;
	}

	pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);
	pgaio_io_start_readv(ioh, fd, nblocks, offset);
}

static void
ps_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->writev(&key, reln, blocknum, buffers, nblocks, skipFsync);
}

static void
ps_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 BlockNumber nblocks)
{
	/* kernel writeback hint only applies to md-backed storage */
	if (ACTIVE()->uses_local_files)
		mdwriteback(reln, forknum, blocknum, nblocks);
}

static BlockNumber
ps_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	return ACTIVE()->nblocks(&key, reln);
}

static void
ps_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks,
			BlockNumber nblocks)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->truncate(&key, reln, old_blocks, nblocks);
}

static void
ps_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	PageStoreRelKey key = pagestore_key(&reln->smgr_rlocator.locator, forknum);

	ACTIVE()->immedsync(&key, reln);
}

static void
ps_registersync(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * md uses this to queue an fsync with the checkpointer for a locally
	 * written file.  Remote backends own their own durability (M1 relies on
	 * immedsync); nothing to defer here.
	 */
	if (ACTIVE()->uses_local_files)
		mdregistersync(reln, forknum);
}

static int
ps_fd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off)
{
	/* only reached by AIO IO workers, which we don't use for remote backends */
	if (ACTIVE()->uses_local_files)
		return mdfd(reln, forknum, blocknum, off);
	return -1;
}

static const f_smgr pagestore_smgr = {
	.smgr_init = ps_init,
	.smgr_shutdown = NULL,
	.smgr_open = ps_open,
	.smgr_close = ps_close,
	.smgr_create = ps_create,
	.smgr_exists = ps_exists,
	.smgr_unlink = ps_unlink,
	.smgr_extend = ps_extend,
	.smgr_zeroextend = ps_zeroextend,
	.smgr_prefetch = ps_prefetch,
	.smgr_maxcombine = ps_maxcombine,
	.smgr_readv = ps_readv,
	.smgr_startreadv = ps_startreadv,
	.smgr_writev = ps_writev,
	.smgr_writeback = ps_writeback,
	.smgr_nblocks = ps_nblocks,
	.smgr_truncate = ps_truncate,
	.smgr_immedsync = ps_immedsync,
	.smgr_registersync = ps_registersync,
	.smgr_fd = ps_fd,
};

/* --- relation routing --------------------------------------------------- */

static int
pagestore_which(RelFileLocator rlocator, ProcNumber backend)
{
	/*
	 * Temp relations live in backend-local buffers and have backend-specific
	 * file paths; leave them on md.  Everything else goes to the backend when
	 * routing is enabled.
	 */
	if (backend != INVALID_PROC_NUMBER)
		return SMGR_MD;

	if (pagestore_route_all)
		return pagestore_smgr_which;

	/* route relations living in user-created tablespaces */
	if (pagestore_route_user_tablespaces &&
		OidIsValid(rlocator.spcOid) &&
		rlocator.spcOid != DEFAULTTABLESPACE_OID &&
		rlocator.spcOid != GLOBALTABLESPACE_OID)
		return pagestore_smgr_which;

	return SMGR_MD;
}

/* --- GUC plumbing ------------------------------------------------------- */

static bool
check_backend_name(char **newval, void **extra, GucSource source)
{
	if (*newval == NULL || pagestore_lookup_backend(*newval) == NULL)
	{
		/* registry not populated yet during early assignment: allow */
		if (pagestore_nbackends == 0)
			return true;
		GUC_check_errdetail("No pagestore backend named \"%s\" is registered.",
							*newval ? *newval : "");
		return false;
	}
	return true;
}

static void
assign_backend_name(const char *newval, void *extra)
{
	const PageStoreBackend *b;

	if (newval == NULL)
		return;
	b = pagestore_lookup_backend(newval);
	if (b != NULL)
		pagestore_active_backend = b;
}

/* --- SQL-callable: COW time-travel read --------------------------------- */

/*
 * pagestore_read_at(rel regclass, forknum int, blocknum int, lsn pg_lsn)
 *   -> bytea
 *
 * Returns the raw image of one page as-of a snapshot LSN, read from the
 * backend's copy-on-write version history.  Demonstrates that overwriting a
 * page does not destroy its earlier versions.
 */
PG_FUNCTION_INFO_V1(pagestore_read_at);

Datum
pagestore_read_at(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	int32		blocknum = PG_GETARG_INT32(2);
	XLogRecPtr	lsn = PG_GETARG_LSN(3);
	Relation	rel;
	PageStoreRelKey key;
	bytea	   *result;

	rel = relation_open(relid, AccessShareLock);

	key.spcOid = rel->rd_locator.spcOid;
	key.dbOid = rel->rd_locator.dbOid;
	key.relNumber = rel->rd_locator.relNumber;
	key.forkNum = forknum;

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	pagestore_localsvc_read_at(&key, (BlockNumber) blocknum, (uint64) lsn,
							   VARDATA(result));

	relation_close(rel, AccessShareLock);

	PG_RETURN_BYTEA_P(result);
}

/*
 * pagestore_create_branch(new_timeline int, parent_timeline int, lsn pg_lsn)
 *
 * Create a copy-on-write branch: a new timeline forked from parent_timeline at
 * the given LSN.  Instant -- no page data is copied; the branch shares the
 * parent's pages until it writes.  A compute can then run on the branch by
 * setting pagestore.timeline to new_timeline.
 */
PG_FUNCTION_INFO_V1(pagestore_create_branch);

Datum
pagestore_create_branch(PG_FUNCTION_ARGS)
{
	int32		new_tl = PG_GETARG_INT32(0);
	int32		parent_tl = PG_GETARG_INT32(1);
	XLogRecPtr	lsn = PG_GETARG_LSN(2);

	if (new_tl <= 0)
		ereport(ERROR,
				(errmsg("pagestore branch timeline must be > 0 (0 is the main timeline)")));

	pagestore_localsvc_create_branch((uint32) new_tl, (uint32) parent_tl,
									 (uint64) lsn);
	PG_RETURN_VOID();
}

/* --- archive module: ship completed WAL segments to the store ---------- */

/*
 * pagestore can also act as an archive_library: when a WAL segment fills, the
 * archiver hands it here and we stream it to the daemon's per-timeline WAL log.
 * This is the compute-side half of WAL shipping (transport + durability).  It
 * uses the official archive module API, so no core changes are needed; the
 * granularity is one completed segment (force one with pg_switch_wal()).
 */
static bool
pagestore_archive_configured(ArchiveModuleState *state)
{
	/* only meaningful when relations are served by the localsvc backend */
	return strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") == 0;
}

/*
 * Decode the WAL in [start, end) and record, for each block a record modifies,
 * an entry in the store's per-page WAL index.  Reuses PostgreSQL's WAL reader
 * (read_local_xlog_page) -- which is why this must run in a normal backend, not
 * the archiver (the archiver lacks the recovery/timeline context the reader
 * asserts on).  In production a background worker would call this as WAL is
 * shipped; the SQL wrapper below lets a test drive it.
 */
static void
pagestore_index_wal_range(XLogRecPtr start, XLogRecPtr end)
{
	ReadLocalXLogPageNoWaitPrivate *pd;
	XLogReaderState *reader;
	XLogRecPtr	first;

	pd = palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
	{
		pfree(pd);
		return;
	}

	first = XLogFindNextRecord(reader, start);
	while (!XLogRecPtrIsInvalid(first))
	{
		char	   *errm;
		XLogRecord *rec = XLogReadRecord(reader, &errm);

		if (rec == NULL)
			break;				/* end of flushed WAL / torn tail */
		if (reader->ReadRecPtr >= end)
			break;

		for (int b = 0; b <= XLogRecMaxBlockId(reader); b++)
		{
			RelFileLocator rloc;
			ForkNumber	fk;
			BlockNumber blk;
			PageStoreRelKey key;

			if (!XLogRecHasBlockRef(reader, b))
				continue;
			XLogRecGetBlockTagExtended(reader, b, &rloc, &fk, &blk, NULL);
			key.spcOid = rloc.spcOid;
			key.dbOid = rloc.dbOid;
			key.relNumber = rloc.relNumber;
			key.forkNum = fk;
			pagestore_localsvc_walidx_add(&key, blk, reader->ReadRecPtr);
			/* Extensions are not derived here: a block reference -- even one flagged
			 * WILL_INIT, which btree unlink and friends set on *existing* pages --
			 * is not proof the fork grew to blk+1.  The per-page index above already
			 * records every block touch, which is what the liveness check uses above
			 * the truncation floor; only truncations are recorded (below). */
		}

		/* smgr truncation shrinks a fork to an exact length -- the one fork-size
		 * signal the per-block index cannot express, and what the redo "is this
		 * block live?" check needs as its floor.  Only the heap (main) fork's new
		 * length is carried by the record (xlrec->blkno); the VM/FSM forks shrink to
		 * derived, smaller lengths (visibilitymap_prepare_truncate /
		 * FreeSpaceMapPrepareTruncateRel), so recording blkno for them would
		 * overstate their size.  VM/FSM materialization is deferred (3c-4b), so
		 * record only the heap fork here. */
		if (XLogRecGetRmid(reader) == RM_SMGR_ID &&
			(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_TRUNCATE)
		{
			xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(reader);

			if (xlrec->flags & SMGR_TRUNCATE_HEAP)
			{
				PageStoreRelKey key;

				key.spcOid = xlrec->rlocator.spcOid;
				key.dbOid = xlrec->rlocator.dbOid;
				key.relNumber = xlrec->rlocator.relNumber;
				key.forkNum = MAIN_FORKNUM;
				pagestore_localsvc_forksize_add(&key, reader->EndRecPtr,
												xlrec->blkno);
			}
		}
	}

	XLogReaderFree(reader);
	pfree(pd);
}

/*
 * pagestore_index_wal(start_lsn pg_lsn, end_lsn pg_lsn) -> void
 *
 * Decode WAL in [start, end) and populate the per-page WAL index.  Stand-in for
 * the background worker that would do this continuously.
 */
PG_FUNCTION_INFO_V1(pagestore_index_wal);

Datum
pagestore_index_wal(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start = PG_GETARG_LSN(0);
	XLogRecPtr	end = PG_GETARG_LSN(1);

	pagestore_index_wal_range(start, end);
	PG_RETURN_VOID();
}

static bool
pagestore_archive_file(ArchiveModuleState *state, const char *file,
					   const char *path)
{
	TimeLineID	tli;
	XLogSegNo	segno;
	XLogRecPtr	seg_start;
	int			fd;
	char	   *buf;
	uint64		off = 0;

	/*
	 * Only ship real WAL segment files.  The archiver also offers backup
	 * history (".backup") and timeline history (".history") files, whose names
	 * begin with a segment number -- shipping them would clobber that segment's
	 * WAL in the store.  Report them archived without storing them.
	 */
	if (!IsXLogFileName(file))
		return true;

	XLogFromFileName(file, &tli, &segno, wal_segment_size);
	XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, seg_start);

	fd = open(path, O_RDONLY);
	if (fd < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("pagestore archive: could not open WAL segment \"%s\": %m",
						path)));
		return false;
	}

	buf = palloc(PS_IO_UNIT);
	for (;;)
	{
		ssize_t		n = read(fd, buf, PS_IO_UNIT);

		if (n < 0)
		{
			pfree(buf);
			close(fd);
			return false;
		}
		if (n == 0)
			break;
		/* ship this chunk at its WAL position */
		pagestore_localsvc_wal_append(seg_start + off, buf, (uint32) n);
		off += (uint64) n;
	}
	pfree(buf);
	close(fd);
	return true;
}

static const ArchiveModuleCallbacks pagestore_archive_callbacks = {
	.startup_cb = NULL,
	.check_configured_cb = pagestore_archive_configured,
	.archive_file_cb = pagestore_archive_file,
	.shutdown_cb = NULL,
};

const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &pagestore_archive_callbacks;
}

/*
 * pagestore_walidx_count(rel regclass, forknum int, blocknum int) -> int
 *
 * How many shipped WAL records the store has indexed as modifying that page.
 * Lets a test confirm the per-page WAL index is populated from real WAL.
 */
PG_FUNCTION_INFO_V1(pagestore_walidx_count);

Datum
pagestore_walidx_count(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	int32		blocknum = PG_GETARG_INT32(2);
	Relation	rel;
	PageStoreRelKey key;
	int			n;

	rel = relation_open(relid, AccessShareLock);
	key.spcOid = rel->rd_locator.spcOid;
	key.dbOid = rel->rd_locator.dbOid;
	key.relNumber = rel->rd_locator.relNumber;
	key.forkNum = forknum;
	n = pagestore_localsvc_walidx_count(&key, (BlockNumber) blocknum);
	relation_close(rel, AccessShareLock);

	PG_RETURN_INT32(n);
}

/*
 * pagestore_redo_page(rel regclass, forknum int, blocknum int, lsn pg_lsn)
 *   -> bytea
 *
 * Single-page materialization, base-image step: return the newest full-page
 * image of the page at/below 'lsn', found via the per-page index and restored
 * with RestoreBlockImage -- reconstructing a page from WAL alone, on demand,
 * for one page (instead of replaying everything).
 *
 * Note this returns the *base* image: a WAL full-page image is the page as it
 * was when that record needed torn-page protection, so to get the page exactly
 * as-of 'lsn' a single-page redo must then apply the delta records after the
 * image with rm_redo (the `--wal-redo`-style helper -- the remaining step).
 * Returns NULL if no full-page image for the page is indexed at/below lsn.
 */
#define PS_REDO_MAX_RECS 4096

/*
 * Shared base-image step: find the newest full-page image of (key,block) at/below
 * 'lsn' via the per-page index, restore it into 'page' (BLCKSZ), and report the
 * end LSN of the record that carried it in *base_end_lsn.  Returns true on
 * success, false if no usable FPI is indexed at/below lsn (or the chain
 * overflowed PS_REDO_MAX_RECS, in which case a stale prefix is refused).
 *
 * base_end_lsn is what a single-page redo must seed the held page's LSN with
 * (RestoreBlockImage copies only bytes; PostgreSQL sets pd_lsn to record->EndRecPtr
 * separately), so the BLK_DONE/BLK_NEEDS_REDO gating of the delta replay is exact.
 */
static bool
redo_base_image(const PageStoreRelKey *key, int forknum, int blocknum,
				XLogRecPtr lsn, char *page, XLogRecPtr *base_end_lsn)
{
	uint64	   *lsns;
	int			n;
	bool		overflow = false;
	ReadLocalXLogPageNoWaitPrivate *pd;
	XLogReaderState *reader;
	bool		found = false;

	lsns = palloc(sizeof(uint64) * PS_REDO_MAX_RECS);
	n = pagestore_localsvc_walidx_get(key, (BlockNumber) blocknum, (uint64) lsn,
									  lsns, NULL, PS_REDO_MAX_RECS, &overflow);
	if (overflow)
	{
		/* The chain is longer than we fetched, so the newest full-page image may
		 * be in the unseen suffix; a base built from the truncated prefix could be
		 * stale.  Refuse until pagination exists. */
		ereport(WARNING,
				(errmsg("pagestore WAL-index chain for block %d exceeds %d records; "
						"cannot reconstruct base image without pagination",
						blocknum, PS_REDO_MAX_RECS)));
		pfree(lsns);
		return false;
	}
	if (n == 0)
	{
		pfree(lsns);
		return false;
	}

	pd = palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
	{
		pfree(pd);
		pfree(lsns);
		return false;
	}

	/* newest indexed record first: find one carrying a full-page image */
	for (int i = n - 1; i >= 0 && !found; i--)
	{
		char	   *errm;
		XLogRecord *rec;

		XLogBeginRead(reader, lsns[i]);
		rec = XLogReadRecord(reader, &errm);
		if (rec == NULL)
			continue;

		/*
		 * The index is keyed by record start LSN, so a record can start at/below
		 * 'lsn' yet end after it.  Such a record is in the future relative to the
		 * target -- using its image (and reporting its end LSN) would seed the page
		 * with a change from after the requested point -- so skip it; the base must
		 * be a record whose end LSN is at/below 'lsn'.
		 */
		if (reader->EndRecPtr > lsn)
			continue;

		for (int b = 0; b <= XLogRecMaxBlockId(reader); b++)
		{
			RelFileLocator rloc;
			ForkNumber	fk;
			BlockNumber blk;

			if (!XLogRecHasBlockImage(reader, b))
				continue;
			XLogRecGetBlockTagExtended(reader, b, &rloc, &fk, &blk, NULL);
			if (rloc.relNumber != key->relNumber || rloc.dbOid != key->dbOid ||
				rloc.spcOid != key->spcOid || fk != forknum ||
				blk != (BlockNumber) blocknum)
				continue;
			if (RestoreBlockImage(reader, b, page))
			{
				*base_end_lsn = reader->EndRecPtr;
				found = true;
			}
			break;
		}
	}

	XLogReaderFree(reader);
	pfree(pd);
	pfree(lsns);
	return found;
}

static void
redo_key_from_relid(Oid relid, int forknum, PageStoreRelKey *key)
{
	Relation	rel = relation_open(relid, AccessShareLock);

	key->spcOid = rel->rd_locator.spcOid;
	key->dbOid = rel->rd_locator.dbOid;
	key->relNumber = rel->rd_locator.relNumber;
	key->forkNum = forknum;
	relation_close(rel, AccessShareLock);
}

PG_FUNCTION_INFO_V1(pagestore_redo_page);

Datum
pagestore_redo_page(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	int32		blocknum = PG_GETARG_INT32(2);
	XLogRecPtr	lsn = PG_GETARG_LSN(3);
	PageStoreRelKey key;
	char	   *page = palloc(BLCKSZ);
	XLogRecPtr	base_end_lsn;
	bytea	   *result;

	redo_key_from_relid(relid, forknum, &key);
	if (!redo_base_image(&key, forknum, blocknum, lsn, page, &base_end_lsn))
	{
		pfree(page);
		PG_RETURN_NULL();
	}
	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);
	pfree(page);
	PG_RETURN_BYTEA_P(result);
}

/*
 * pagestore_redo_base_lsn(rel regclass, forknum int, blocknum int, lsn pg_lsn)
 *   -> pg_lsn
 *
 * The end LSN of the record carrying the base full-page image that
 * pagestore_redo_page returns -- the LSN a single-page redo seeds the held page
 * with before replaying the deltas after it.  NULL when no FPI is indexed at/below
 * lsn (same condition under which pagestore_redo_page returns NULL).
 */
PG_FUNCTION_INFO_V1(pagestore_redo_base_lsn);

Datum
pagestore_redo_base_lsn(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	int32		blocknum = PG_GETARG_INT32(2);
	XLogRecPtr	lsn = PG_GETARG_LSN(3);
	PageStoreRelKey key;
	char	   *page = palloc(BLCKSZ);
	XLogRecPtr	base_end_lsn;
	bool		found;

	redo_key_from_relid(relid, forknum, &key);
	found = redo_base_image(&key, forknum, blocknum, lsn, page, &base_end_lsn);
	pfree(page);
	if (!found)
		PG_RETURN_NULL();
	PG_RETURN_LSN(base_end_lsn);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("pagestore must be loaded via shared_preload_libraries")));

	/* register built-in backends */
	pagestore_register_backend(&PageStoreBackendPassthrough);
	pagestore_register_backend(&PageStoreBackendLocalSvc);
	pagestore_active_backend = &PageStoreBackendPassthrough;

	/* let the localsvc backend define its own GUCs */
	pagestore_localsvc_init();

	DefineCustomBoolVariable("pagestore.route_all",
							 "Route all non-temp relation I/O through the pagestore backend.",
							 NULL,
							 &pagestore_route_all,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pagestore.route_user_tablespaces",
							 "Route relations in user-created tablespaces through the pagestore backend.",
							 NULL,
							 &pagestore_route_user_tablespaces,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("pagestore.backend",
							   "Name of the active pagestore storage backend.",
							   NULL,
							   &pagestore_backend_name,
							   "passthrough",
							   PGC_POSTMASTER,
							   0,
							   check_backend_name,
							   assign_backend_name,
							   NULL);

	MarkGUCPrefixReserved("pagestore");

	/* register our smgr implementation and claim relations via the hook */
	pagestore_smgr_which = smgr_register(&pagestore_smgr);
	smgr_which_hook = pagestore_which;
}
