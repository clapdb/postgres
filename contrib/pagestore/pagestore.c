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
#include "access/slru.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "archive/archive_module.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pagestore_backend.h"
#include "pagestore_ipc.h"
#include "port/pg_iovec.h"
#include "storage/aio.h"
#include "storage/bufpage.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "walredo_client.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* GUC state */
static bool pagestore_route_all = false;
static bool pagestore_route_user_tablespaces = false;
static char *pagestore_backend_name = NULL;
static char *pagestore_walredo_datadir = NULL;

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

PG_FUNCTION_INFO_V1(pagestore_redo_page);

Datum
pagestore_redo_page(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	int32		blocknum = PG_GETARG_INT32(2);
	XLogRecPtr	lsn = PG_GETARG_LSN(3);
	Relation	rel;
	PageStoreRelKey key;
	PsWalRec  *recs;
	int			n;
	ReadLocalXLogPageNoWaitPrivate *pd;
	XLogReaderState *reader;
	char	   *page;
	bytea	   *result = NULL;

	rel = relation_open(relid, AccessShareLock);
	key.spcOid = rel->rd_locator.spcOid;
	key.dbOid = rel->rd_locator.dbOid;
	key.relNumber = rel->rd_locator.relNumber;
	key.forkNum = forknum;
	relation_close(rel, AccessShareLock);

	recs = palloc(sizeof(PsWalRec) * PS_REDO_MAX_RECS);
	n = pagestore_localsvc_walidx_get(&key, (BlockNumber) blocknum, (uint64) lsn,
									  recs, PS_REDO_MAX_RECS);
	if (n == 0)
		PG_RETURN_NULL();

	pd = palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
	{
		pfree(pd);
		PG_RETURN_NULL();
	}
	page = palloc(BLCKSZ);

	/* newest indexed record first: find one carrying a full-page image */
	for (int i = n - 1; i >= 0 && result == NULL; i--)
	{
		char	   *errm;
		XLogRecord *rec;

		XLogBeginRead(reader, recs[i].lsn);
		rec = XLogReadRecord(reader, &errm);
		if (rec == NULL)
			continue;

		for (int b = 0; b <= XLogRecMaxBlockId(reader); b++)
		{
			RelFileLocator rloc;
			ForkNumber	fk;
			BlockNumber blk;

			if (!XLogRecHasBlockImage(reader, b))
				continue;
			XLogRecGetBlockTagExtended(reader, b, &rloc, &fk, &blk, NULL);
			if (rloc.relNumber != key.relNumber || rloc.dbOid != key.dbOid ||
				rloc.spcOid != key.spcOid || fk != forknum ||
				blk != (BlockNumber) blocknum)
				continue;
			if (RestoreBlockImage(reader, b, page))
			{
				result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
				SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
				memcpy(VARDATA(result), page, BLCKSZ);
			}
			break;
		}
	}

	XLogReaderFree(reader);
	pfree(pd);
	pfree(recs);
	pfree(page);

	if (result == NULL)
		PG_RETURN_NULL();
	PG_RETURN_BYTEA_P(result);
}

/*
 * Liveness: scan local WAL in (from_lsn, to_lsn] for an smgr truncate of
 * (rloc, forknum) down to <= block.  If found, the block was truncated away after
 * its last write and not re-extended, so it is not live as of to_lsn and must not
 * be materialized.  Only the main fork is checked -- the truncate record carries
 * only the main-fork size; other forks are conservatively treated as live.
 *
 * Cost: a linear pass over (from_lsn, to_lsn].  redo_page_asof is off the read hot
 * path; a daemon-side fork-size-at-lsn index would make this O(1) -- a later step.
 */
static bool
redo_block_truncated_away(XLogReaderState *reader, RelFileLocator rloc,
						  ForkNumber forknum, BlockNumber block,
						  XLogRecPtr from_lsn, XLogRecPtr to_lsn)
{
	if (forknum != MAIN_FORKNUM || from_lsn >= to_lsn)
		return false;

	XLogBeginRead(reader, from_lsn);
	for (;;)
	{
		char	   *errm;
		XLogRecord *rec = XLogReadRecord(reader, &errm);

		if (rec == NULL)
			break;
		if (reader->ReadRecPtr <= from_lsn)
			continue;			/* skip the block's own last-write record */
		if (reader->ReadRecPtr > to_lsn)
			break;
		if (XLogRecGetRmid(reader) == RM_SMGR_ID &&
			(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_TRUNCATE)
		{
			xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(reader);

			if (RelFileLocatorEquals(xlrec->rlocator, rloc) &&
				(xlrec->flags & SMGR_TRUNCATE_HEAP) &&
				xlrec->blkno <= block)
				return true;
		}
	}
	return false;
}

/*
 * pagestore_redo_page_asof(relid, forknum, blocknum, lsn) returns bytea
 *
 * Materialize a page as of 'lsn' (the 5b driver): find the newest full-page
 * image at/below lsn as the base, then apply every WAL record that touched the
 * block after it, through the wal-redo helper.  Returns the materialized page
 * (pd_checksum recomputed), or NULL if no base image is indexed for the block.
 *
 * Single-branch: the records are read from this compute's local WAL by LSN.
 * (Serving deltas across branches from the store -- using the per-record source
 * timeline -- is a later step.)
 */
PG_FUNCTION_INFO_V1(pagestore_redo_page_asof);
Datum
pagestore_redo_page_asof(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	int32		blocknum = PG_GETARG_INT32(2);
	XLogRecPtr	lsn = PG_GETARG_LSN(3);
	Relation	rel;
	PageStoreRelKey key;
	RelFileLocator rloc;
	PsWalRec  *recs;
	int			n;
	int			base_idx = -1;
	XLogRecPtr	base_end_lsn = InvalidXLogRecPtr;
	ReadLocalXLogPageNoWaitPrivate *pd;
	XLogReaderState *reader;
	char	   *base;
	char	   *page;
	WalRedoProc *p;
	bytea	   *result;

	if (pagestore_walredo_datadir == NULL || pagestore_walredo_datadir[0] == '\0')
		ereport(ERROR,
				(errmsg("pagestore.walredo_datadir is not set")));

	rel = relation_open(relid, AccessShareLock);
	rloc = rel->rd_locator;
	relation_close(rel, AccessShareLock);
	key.spcOid = rloc.spcOid;
	key.dbOid = rloc.dbOid;
	key.relNumber = rloc.relNumber;
	key.forkNum = forknum;

	recs = palloc(sizeof(PsWalRec) * PS_REDO_MAX_RECS);
	n = pagestore_localsvc_walidx_get(&key, (BlockNumber) blocknum, (uint64) lsn,
									  recs, PS_REDO_MAX_RECS);
	if (n == 0)
		PG_RETURN_NULL();

	pd = palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
	{
		pfree(pd);
		pfree(recs);
		PG_RETURN_NULL();
	}
	base = palloc(BLCKSZ);
	page = palloc(BLCKSZ);

	/* base = newest record at/below lsn carrying an FPI for the block */
	for (int i = n - 1; i >= 0 && base_idx < 0; i--)
	{
		char	   *errm;
		XLogRecord *rec;

		XLogBeginRead(reader, recs[i].lsn);
		rec = XLogReadRecord(reader, &errm);
		if (rec == NULL)
			continue;
		for (int b = 0; b <= XLogRecMaxBlockId(reader); b++)
		{
			RelFileLocator brloc;
			ForkNumber	fk;
			BlockNumber blk;

			if (!XLogRecHasBlockImage(reader, b))
				continue;
			XLogRecGetBlockTagExtended(reader, b, &brloc, &fk, &blk, NULL);
			if (!RelFileLocatorEquals(brloc, rloc) || fk != forknum ||
				blk != (BlockNumber) blocknum)
				continue;
			if (RestoreBlockImage(reader, b, base))
			{
				base_idx = i;
				base_end_lsn = reader->EndRecPtr;
			}
			break;
		}
	}

	if (base_idx < 0)
	{
		/* no base image indexed for this block; cannot materialize yet */
		XLogReaderFree(reader);
		pfree(pd);
		pfree(recs);
		pfree(base);
		pfree(page);
		PG_RETURN_NULL();
	}

	/*
	 * Liveness: if the block was truncated away after its last write and at or
	 * before lsn (and not re-extended), it is not live as of lsn -- do not
	 * materialize a stale page for it.  recs[n - 1].lsn is the block's newest record
	 * at/below lsn.
	 */
	if (redo_block_truncated_away(reader, rloc, forknum, (BlockNumber) blocknum,
								  (XLogRecPtr) recs[n - 1].lsn, lsn))
	{
		XLogReaderFree(reader);
		pfree(pd);
		pfree(recs);
		pfree(base);
		pfree(page);
		PG_RETURN_NULL();
	}

	/* replay: base image, then every record after it, through the helper */
	p = walredo_start(pagestore_walredo_datadir);
	walredo_begin(p, rloc, forknum, (BlockNumber) blocknum);
	walredo_pushbase(p, base_end_lsn, base);
	for (int i = base_idx + 1; i < n; i++)
	{
		char	   *errm;
		XLogRecord *rec;

		uint32		tot;
		uint32		firstpage;
		char	   *raw;

		XLogBeginRead(reader, recs[i].lsn);
		rec = XLogReadRecord(reader, &errm);
		if (rec == NULL)
		{
			walredo_stop(p);
			ereport(ERROR,
					(errmsg("could not read WAL record at %X/%08X for redo: %s",
							LSN_FORMAT_ARGS(recs[i].lsn), errm ? errm : "(unknown)")));
		}

		/*
		 * XLogReadRecord returns the decoded header, not the contiguous raw
		 * record the helper needs.  Recover the raw bytes the way the decoder
		 * laid them out: a record that fit on its page is still in the page
		 * buffer at its offset; one that spanned pages was reassembled into
		 * readRecordBuf.
		 */
		tot = rec->xl_tot_len;
		firstpage = XLOG_BLCKSZ - (uint32) (reader->ReadRecPtr % XLOG_BLCKSZ);
		if (tot > firstpage)
			raw = reader->readRecordBuf;
		else
			raw = reader->readBuf + (reader->ReadRecPtr % XLOG_BLCKSZ);
		walredo_apply(p, reader->ReadRecPtr, reader->EndRecPtr, raw, tot);
	}
	walredo_get(p, page);
	walredo_stop(p);

	/* the helper leaves pd_checksum to the caller; recompute it here */
	PageSetChecksumInplace((Page) page, (BlockNumber) blocknum);

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);

	XLogReaderFree(reader);
	pfree(pd);
	pfree(recs);
	pfree(base);
	pfree(page);
	PG_RETURN_BYTEA_P(result);
}

/*
 * pagestore_walredo_roundtrip(page bytea, base_lsn pg_lsn) returns bytea
 *
 * Spawn the wal-redo helper, push 'page' as the base (stamping its page LSN to
 * base_lsn), and read it back -- a round-trip test of the helper spawn + the
 * stdin/stdout protocol, independent of any redo.  The returned page matches the
 * input except pd_lsn (stamped to base_lsn) and pd_checksum (the helper leaves
 * checksums to the caller).
 */
PG_FUNCTION_INFO_V1(pagestore_walredo_roundtrip);
Datum
pagestore_walredo_roundtrip(PG_FUNCTION_ARGS)
{
	bytea	   *inpage = PG_GETARG_BYTEA_PP(0);
	XLogRecPtr	base_lsn = PG_GETARG_LSN(1);
	RelFileLocator rloc = {.spcOid = 1663,.dbOid = 1,.relNumber = 0x7e000000};
	WalRedoProc *p;
	char	   *out;
	bytea	   *result;

	if (VARSIZE_ANY_EXHDR(inpage) != BLCKSZ)
		ereport(ERROR,
				(errmsg("page must be exactly %d bytes", BLCKSZ)));
	if (pagestore_walredo_datadir == NULL || pagestore_walredo_datadir[0] == '\0')
		ereport(ERROR,
				(errmsg("pagestore.walredo_datadir is not set")));

	out = palloc(BLCKSZ);
	p = walredo_start(pagestore_walredo_datadir);
	walredo_begin(p, rloc, MAIN_FORKNUM, 0);
	walredo_pushbase(p, base_lsn, VARDATA_ANY(inpage));
	walredo_get(p, out);
	walredo_stop(p);

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), out, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * pagestore_object_roundtrip(klass int, objid int, page bytea) returns bytea
 *
 * Exercises the PsKey klass discriminator (the seam for non-relation objects):
 * write 'page' as block 0 of a non-relation object identified by (klass, objid),
 * then read it back.  Objects of different klass with the same objid are distinct
 * keys, so this also demonstrates klass isolation from relation pages.
 */
PG_FUNCTION_INFO_V1(pagestore_object_roundtrip);
Datum
pagestore_object_roundtrip(PG_FUNCTION_ARGS)
{
	int32		klass = PG_GETARG_INT32(0);
	int32		objid = PG_GETARG_INT32(1);
	bytea	   *inpage = PG_GETARG_BYTEA_PP(2);
	PageStoreRelKey key;
	char	   *out;
	bytea	   *result;

	if (VARSIZE_ANY_EXHDR(inpage) != BLCKSZ)
		ereport(ERROR,
				(errmsg("page must be exactly %d bytes", BLCKSZ)));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));

	/* a non-relation object key: the identity fields are reinterpreted per klass */
	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = (RelFileNumber) objid;
	key.forkNum = 0;

	out = palloc(BLCKSZ);
	pagestore_localsvc_obj_write((uint32) klass, &key, 0, VARDATA_ANY(inpage));
	pagestore_localsvc_obj_read((uint32) klass, &key, 0, out);

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), out, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * pagestore_object_get(klass int, objid int) returns bytea -- read block 0 of a
 * non-relation object without writing (used to verify klass isolation).
 */
PG_FUNCTION_INFO_V1(pagestore_object_get);
Datum
pagestore_object_get(PG_FUNCTION_ARGS)
{
	int32		klass = PG_GETARG_INT32(0);
	int32		objid = PG_GETARG_INT32(1);
	PageStoreRelKey key;
	char	   *out;
	bytea	   *result;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));

	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = (RelFileNumber) objid;
	key.forkNum = 0;

	out = palloc(BLCKSZ);
	pagestore_localsvc_obj_read((uint32) klass, &key, 0, out);

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), out, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/* Stable per-SLRU object id derived from its directory name (e.g. "pg_xact"). */
static uint32
slru_klass_id(const char *dir)
{
	uint32		h = 2166136261u;	/* FNV-1a over the SLRU subdir name */

	for (const unsigned char *p = (const unsigned char *) dir; *p != '\0'; p++)
	{
		h ^= *p;
		h *= 16777619u;
	}
	return h;
}

/*
 * SLRU page-write hook (installed into slru.c): mirror each just-written SLRU
 * page (clog, multixact, ...) onto the page store as a PS_KLASS_SLRU object,
 * keyed by (SLRU id, page number).  This is the first real consumer that puts
 * non-relation cluster state on the store via the klass seam.
 *
 * Best-effort: any failure is swallowed so it can never break an SLRU write.
 * Runs under the SLRU bank lock, so it stays a single synchronous obj_write --
 * a production version would copy the page and ship it asynchronously.
 */
static void
pagestore_slru_write_hook(SlruCtl ctl, int64 pageno, const char *page)
{
	PageStoreRelKey key;
	MemoryContext oldcontext;

	/* only meaningful when relations are served by the localsvc backend */
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		return;
	if (pageno < 0 || pageno > (int64) UINT32_MAX)
		return;					/* page number outside our block space */

	/*
	 * The mirror does fallible shm I/O and busy-waits with CHECK_FOR_INTERRUPTS.
	 * An SLRU buffer can be evicted/written from inside a transaction commit/abort
	 * critical section (e.g. via SlruSelectLRUPage), where any ERROR would PANIC --
	 * so never mirror there; the page is mirrored by a later write/checkpoint.
	 */
	if (CritSectionCount > 0)
		return;

	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = slru_klass_id(ctl->Dir);
	key.forkNum = 0;

	oldcontext = CurrentMemoryContext;
	PG_TRY();
	{
		pagestore_localsvc_obj_write(PS_KLASS_SLRU, &key, (BlockNumber) pageno, page);
	}
	PG_CATCH();
	{
		int			sqlerrcode = geterrcode();

		/*
		 * Restore the context (PG_CATCH left it at ErrorContext), then swallow a
		 * genuine mirror I/O failure so it cannot break the SLRU write -- but never
		 * swallow a query-cancel / shutdown interrupt; re-throw those so cancellation
		 * and fast shutdown still work.
		 */
		MemoryContextSwitchTo(oldcontext);
		if (sqlerrcode == ERRCODE_QUERY_CANCELED ||
			sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
			PG_RE_THROW();
		FlushErrorState();
	}
	PG_END_TRY();
}

/* GUC: when on, the read hook below serves SLRU pages from the store. */
static bool pagestore_slru_read_from_store = false;

/*
 * SLRU page-read hook (installed into slru.c): serve an SLRU page from the store
 * instead of the local segment file.  Returns true (page filled) when enabled and
 * the store has the page, else false to fall back to the local read.  This lets a
 * compute read shared cluster state (clog, ...) from the store -- e.g. one whose
 * local SLRU files are absent or stale.  Best-effort: any error falls back.
 */
static bool
pagestore_slru_read_hook(SlruCtl ctl, int64 pageno, char *page)
{
	PageStoreRelKey key;
	bool		served = false;
	MemoryContext oldcontext;

	if (!pagestore_slru_read_from_store)
		return false;
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		return false;
	if (pageno < 0 || pageno > (int64) UINT32_MAX)
		return false;
	/* don't do fallible store I/O in a critical section; fall back to local */
	if (CritSectionCount > 0)
		return false;

	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = slru_klass_id(ctl->Dir);
	key.forkNum = 0;

	oldcontext = CurrentMemoryContext;
	PG_TRY();
	{
		served = pagestore_localsvc_obj_read(PS_KLASS_SLRU, &key,
											 (BlockNumber) pageno, page);
	}
	PG_CATCH();
	{
		int			sqlerrcode = geterrcode();

		/* restore context; re-throw cancel/shutdown, else fall back to local read */
		MemoryContextSwitchTo(oldcontext);
		if (sqlerrcode == ERRCODE_QUERY_CANCELED ||
			sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
			PG_RE_THROW();
		FlushErrorState();
		served = false;
	}
	PG_END_TRY();
	return served;
}

/*
 * pagestore_slru_get(slru_name text, pageno int) returns bytea -- read an SLRU
 * page back from the store (used to verify the write hook mirrored it).
 */
PG_FUNCTION_INFO_V1(pagestore_slru_get);
Datum
pagestore_slru_get(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		pageno = PG_GETARG_INT32(1);
	PageStoreRelKey key;
	char	   *out;
	bytea	   *result;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));

	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = slru_klass_id(name);
	key.forkNum = 0;

	out = palloc(BLCKSZ);
	pagestore_localsvc_obj_read(PS_KLASS_SLRU, &key, (BlockNumber) pageno, out);

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), out, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * Control-file write hook (installed into xlog.c): mirror pg_control onto the
 * store as a PS_KLASS_CONTROL object whenever it is written.  Cluster control
 * state on the store via the same klass seam -- the foundation for a compute
 * reading cluster metadata from the store.  Best-effort (PG_TRY); runs in many
 * contexts (checkpointer, startup, backends), each attaching the shm lazily.
 */
static void
pagestore_control_write_hook(const ControlFileData *cf)
{
	PageStoreRelKey key;
	char		buf[BLCKSZ];
	MemoryContext oldcontext;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		return;
	/*
	 * NB: unlike the SLRU hook we do NOT skip critical sections here -- pg_control
	 * is updated almost exclusively from inside the checkpoint/shutdown critical
	 * section (e.g. xlog.c CreateCheckPoint), so skipping would never mirror it.
	 * The mirror's fallible shm I/O is therefore a known prototype risk in a
	 * critical section (a daemon error would PANIC); a production build would defer
	 * the mirror to a checkpoint-completion callback / async shipper instead.
	 */

	/* pg_control fits in a single block (PG_CONTROL_FILE_SIZE <= BLCKSZ) */
	memset(buf, 0, sizeof(buf));
	memcpy(buf, cf, Min(sizeof(ControlFileData), (size_t) BLCKSZ));

	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = 0;
	key.forkNum = 0;

	oldcontext = CurrentMemoryContext;
	PG_TRY();
	{
		pagestore_localsvc_obj_write(PS_KLASS_CONTROL, &key, 0, buf);
	}
	PG_CATCH();
	{
		int			sqlerrcode = geterrcode();

		/* restore context; re-throw cancel/shutdown, swallow only mirror I/O errors */
		MemoryContextSwitchTo(oldcontext);
		if (sqlerrcode == ERRCODE_QUERY_CANCELED ||
			sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
			PG_RE_THROW();
		FlushErrorState();
	}
	PG_END_TRY();
}

/*
 * pagestore_control_checkpoint_lsn() returns pg_lsn -- the checkpoint LSN from
 * the pg_control image on the store (verifies the write hook mirrored the
 * current control file).
 */
PG_FUNCTION_INFO_V1(pagestore_control_checkpoint_lsn);
Datum
pagestore_control_checkpoint_lsn(PG_FUNCTION_ARGS)
{
	PageStoreRelKey key;
	char	   *buf;
	ControlFileData *cf;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));

	key.spcOid = 0;
	key.dbOid = 0;
	key.relNumber = 0;
	key.forkNum = 0;

	buf = palloc(BLCKSZ);
	if (!pagestore_localsvc_obj_read(PS_KLASS_CONTROL, &key, 0, buf))
		ereport(ERROR,
				(errmsg("pg_control is not present on the store")));
	cf = (ControlFileData *) buf;
	PG_RETURN_LSN(cf->checkPoint);
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

	DefineCustomStringVariable("pagestore.walredo_datadir",
							   "Private scratch data directory for the postgres --wal-redo helper.",
							   "Must be a throwaway initdb'd cluster, never the live one; "
							   "the helper only ever mutates pages handed to it over the protocol.",
							   &pagestore_walredo_datadir,
							   "",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("pagestore.slru_read_from_store",
							 "Serve SLRU pages (clog, ...) from the page store instead of local files.",
							 "Lets a compute read shared cluster state from the store; falls back "
							 "to the local segment for pages the store does not have.",
							 &pagestore_slru_read_from_store,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pagestore");

	/* register our smgr implementation and claim relations via the hook */
	pagestore_smgr_which = smgr_register(&pagestore_smgr);
	smgr_which_hook = pagestore_which;

	/* mirror SLRU pages (clog, multixact, ...) onto the store via the klass seam */
	slru_page_write_hook = pagestore_slru_write_hook;
	slru_page_read_hook = pagestore_slru_read_hook;

	/* mirror the control file (pg_control) onto the store as well */
	control_file_write_hook = pagestore_control_write_hook;
}
