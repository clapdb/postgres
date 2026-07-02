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

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/relation.h"
#include "access/rmgr.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "archive/archive_module.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "common/file_perm.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pagestore_backend.h"
#include "pagestore_ipc.h"
#include "port/pg_iovec.h"
#include "storage/aio.h"
#include "storage/bufpage.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
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
static bool pagestore_redo_wal_from_store = false;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

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

	if (reader != NULL)
		XLogReaderFree(reader);
	if (pd != NULL)
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

	if (reader != NULL)
		XLogReaderFree(reader);
	if (pd != NULL)
		pfree(pd);
	pfree(recs);
	pfree(page);

	if (result == NULL)
		PG_RETURN_NULL();
	PG_RETURN_BYTEA_P(result);
}

/*
 * Store-backed WAL page reader for redo.  The records that materialize a page
 * can live on ancestor timelines (a branch's deltas predate the branch point);
 * a branch compute may not have that ancestor WAL locally.  This page_read
 * callback serves a WAL page from the store's shipped per-timeline log when the
 * record's source timeline (ps_redo_cur_timeline, set per record from the walidx
 * tag) is not this compute's own, or when pagestore.redo_wal_from_store forces it
 * (a compute with no local WAL at all); otherwise it reads the local WAL.
 */
static uint32 ps_redo_cur_timeline;		/* timeline of the record being read */
static uint32 ps_redo_local_timeline;	/* this compute's own timeline */

static int
ps_redo_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				  XLogRecPtr targetRecPtr, char *readBuf)
{
	if (pagestore_redo_wal_from_store ||
		ps_redo_cur_timeline != ps_redo_local_timeline)
	{
		int			n = pagestore_localsvc_wal_read(ps_redo_cur_timeline,
													(uint64) targetPagePtr,
													(uint32) reqLen, readBuf);

		return (n >= reqLen) ? reqLen : -1;
	}
	return read_local_xlog_page_no_wait(state, targetPagePtr, reqLen,
										targetRecPtr, readBuf);
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
 *
 * Tri-state so an unreadable scan fails closed rather than passing as "live":
 */
typedef enum
{
	REDO_BLOCK_LIVE = 0,		/* scanned cleanly through to_lsn, no truncate */
	REDO_BLOCK_TRUNCATED,		/* truncated away at/below the block, not re-extended */
	REDO_BLOCK_SCAN_INCOMPLETE, /* WAL unreadable through to_lsn -- fail closed */
} RedoBlockLiveness;

static RedoBlockLiveness
redo_block_truncated_away(XLogReaderState *reader, RelFileLocator rloc,
						  ForkNumber forknum, BlockNumber block,
						  XLogRecPtr from_lsn, XLogRecPtr to_lsn)
{
	XLogRecPtr	scanned_through;

	if (forknum != MAIN_FORKNUM || from_lsn >= to_lsn)
		return REDO_BLOCK_LIVE;

	XLogBeginRead(reader, from_lsn);
	scanned_through = from_lsn;
	for (;;)
	{
		char	   *errm;
		XLogRecord *rec = XLogReadRecord(reader, &errm);

		/*
		 * End of readable WAL.  If the read cursor already advanced through to_lsn
		 * the range was fully scanned and no truncate was found -- the block is
		 * live.  Otherwise the WAL could not be read through to_lsn (e.g. the
		 * shipped log has not reached this segment; the archive callback ships only
		 * completed segments), so a truncate in the unread tail would be silently
		 * missed: report the scan incomplete and let the caller fail closed.
		 */
		if (rec == NULL)
			return scanned_through >= to_lsn ? REDO_BLOCK_LIVE
				: REDO_BLOCK_SCAN_INCOMPLETE;
		scanned_through = reader->EndRecPtr;
		if (reader->ReadRecPtr <= from_lsn)
			continue;			/* skip the block's own last-write record */
		if (reader->ReadRecPtr > to_lsn)
			return REDO_BLOCK_LIVE;	/* scanned cleanly past to_lsn, no truncate */
		if (XLogRecGetRmid(reader) == RM_SMGR_ID &&
			(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_TRUNCATE)
		{
			xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(reader);

			if (RelFileLocatorEquals(xlrec->rlocator, rloc) &&
				(xlrec->flags & SMGR_TRUNCATE_HEAP) &&
				xlrec->blkno <= block)
				return REDO_BLOCK_TRUNCATED;
		}
	}
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
								XL_ROUTINE(.page_read = &ps_redo_page_read,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
	{
		pfree(pd);
		pfree(recs);
		PG_RETURN_NULL();
	}
	/* records on ancestor timelines are served from the store's shipped WAL */
	ps_redo_local_timeline = pagestore_localsvc_timeline();
	base = palloc(BLCKSZ);
	page = palloc(BLCKSZ);

	/* base = newest record at/below lsn carrying an FPI for the block */
	for (int i = n - 1; i >= 0 && base_idx < 0; i--)
	{
		char	   *errm;
		XLogRecord *rec;

		ps_redo_cur_timeline = recs[i].timeline;
		/* the reader caches a page by (segno,offset) only -- not timeline -- so a
		 * same-offset page on another timeline would be a false hit; invalidate it
		 * whenever the source timeline may change (cross-branch redo). */
		reader->readLen = 0;
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
	{
		RedoBlockLiveness liveness;

		/*
		 * The truncate scan walks the WAL after the block's last write.  Read it
		 * from that record's source timeline (an ancestor in cross-branch redo)
		 * and from the store when store-backed -- so DON'T force local/off here, or
		 * a no-local-WAL (store-backed) compute would skip the truncate check.
		 * Invalidate the reader's page cache first since the timeline may differ
		 * from the record loop above.  (A scan range that itself crosses a branch
		 * point spans timelines and is a known limitation.)
		 */
		ps_redo_cur_timeline = recs[n - 1].timeline;
		reader->readLen = 0;
		liveness = redo_block_truncated_away(reader, rloc, forknum,
											 (BlockNumber) blocknum,
											 (XLogRecPtr) recs[n - 1].lsn, lsn);
		/*
		 * Fail closed unless the scan proved the block live: a confirmed truncate
		 * and an incomplete scan (the WAL could not be read through lsn) both mean
		 * we must not return a possibly-stale FPI for a block that may be gone.
		 */
		if (liveness != REDO_BLOCK_LIVE)
		{
			XLogReaderFree(reader);
			pfree(pd);
			pfree(recs);
			pfree(base);
			pfree(page);
			PG_RETURN_NULL();
		}
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

		ps_redo_cur_timeline = recs[i].timeline;
		/* the reader caches a page by (segno,offset) only -- not timeline -- so a
		 * same-offset page on another timeline would be a false hit; invalidate it
		 * whenever the source timeline may change (cross-branch redo). */
		reader->readLen = 0;
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

	if (reader != NULL)
		XLogReaderFree(reader);
	if (pd != NULL)
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
	pagestore_localsvc_obj_write((uint32) klass, &key, 0, VARDATA_ANY(inpage), 0);
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

/*
 * SLRU snapshot shipping (M4 step 1).
 *
 * An SLRU directory's segment files are a clean as-of image once they are flushed
 * and no longer being written.  We ship every page of an in-scope SLRU (clog,
 * multixact offsets/members, commit-ts) to the store, versioning each by a cutoff C
 * so a branch reads the snapshot as-of an LSN >= C (the seed base; per-update WAL in
 * (C, L] then brings it to exactly L).  The store key is
 *   PsKey{ klass = PS_KLASS_SLRU, relNumber = slru_klass_id(dir), block = pageno }.
 */

/* Stable per-SLRU object id from its directory name (FNV-1a; libc-only). */
static uint32
slru_klass_id(const char *name)
{
	uint32		h = 2166136261u;
	const unsigned char *p;

	for (p = (const unsigned char *) name; *p != '\0'; p++)
	{
		h ^= *p;
		h *= 16777619u;
	}
	return h;
}

/* The WAL-logged, uint32-page SLRUs M4 may snapshot/reconstruct.  A whitelist: the
 * SQL helpers take a directory name, so without it a caller could point them at
 * 'base/<db>' or 'global' and snapshot ordinary relation files (whose numeric names
 * also parse as hex) under an SLRU key. */
static const char *const ps_slru_dirs[] = {
	"pg_xact",
	"pg_multixact/offsets",
	"pg_multixact/members",
	"pg_commit_ts",
};

static void
slru_check_dir(const char *slru)
{
	for (int i = 0; i < (int) lengthof(ps_slru_dirs); i++)
		if (strcmp(ps_slru_dirs[i], slru) == 0)
			return;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("\"%s\" is not an in-scope SLRU directory", slru)));
}

static void
slru_obj_key(PageStoreRelKey *key, const char *slru)
{
	key->spcOid = 0;
	key->dbOid = 0;
	key->relNumber = (RelFileNumber) slru_klass_id(slru);
	key->forkNum = 0;
}

/*
 * pagestore_ship_slru_snapshot(slru text, cutoff pg_lsn) returns bigint
 *
 * Ship a clean whole-segment snapshot of SLRU directory 'slru' (e.g. 'pg_xact')
 * keyed by 'cutoff'.  'cutoff' must provably upper-bound the on-disk segments'
 * contents -- the caller captures it at a quiescent point (a clean
 * shutdown/restartpoint, or under a brief SLRU write barrier).  The online,
 * automatically-triggered, proven-C path is a follow-up.  Returns the page count;
 * fails closed (ereport) on any read/IPC error rather than shipping a partial snap.
 */
PG_FUNCTION_INFO_V1(pagestore_ship_slru_snapshot);
Datum
pagestore_ship_slru_snapshot(PG_FUNCTION_ARGS)
{
	char	   *slru = text_to_cstring(PG_GETARG_TEXT_PP(0));
	XLogRecPtr	cutoff = PG_GETARG_LSN(1);
	DIR		   *dir;
	struct dirent *de;
	int64		shipped = 0;
	char	   *page = palloc(BLCKSZ);

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	slru_check_dir(slru);

	dir = AllocateDir(slru);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open SLRU directory \"%s\": %m", slru)));

	while ((de = ReadDir(dir, slru)) != NULL)
	{
		int64		segno;
		char	   *endp;
		char		segpath[MAXPGPATH];
		int			fd;
		int			pageidx;
		size_t		namelen = strlen(de->d_name);

		/*
		 * Mirror SlruScanDirectory()'s filename test so we ship only real segment
		 * files: the in-scope SLRUs all use short names, which SlruCorrectSegment-
		 * FilenameLength() allows to be 4, 5 or 6 upper-case hex characters (the
		 * "%04X" width is a minimum, so a segno past 0xFFFF -- e.g. advanced
		 * commit_ts / multixact members -- yields 5-6 chars).  Anything else in the
		 * directory (a stray or long-name file) is skipped, not shipped as bogus
		 * SLRU pages.
		 */
		if (namelen < 4 || namelen > 6 ||
			strspn(de->d_name, "0123456789ABCDEF") != namelen)
			continue;
		errno = 0;
		segno = strtoll(de->d_name, &endp, 16);
		if (endp == de->d_name || *endp != '\0' || errno != 0 || segno < 0)
			continue;

		snprintf(segpath, sizeof(segpath), "%s/%s", slru, de->d_name);
		fd = OpenTransientFile(segpath, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open SLRU segment \"%s\": %m", segpath)));

		for (pageidx = 0; pageidx < SLRU_PAGES_PER_SEGMENT; pageidx++)
		{
			int			n = read(fd, page, BLCKSZ);
			PageStoreRelKey key;

			if (n == 0)
				break;			/* fewer than a full segment's pages on disk */
			if (n != BLCKSZ)
			{
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("short read on SLRU segment \"%s\"", segpath)));
			}
			slru_obj_key(&key, slru);
			pagestore_localsvc_obj_write(PS_KLASS_SLRU, &key,
										 (BlockNumber) (segno * SLRU_PAGES_PER_SEGMENT
														+ pageidx),
										 page, (uint64) cutoff);
			shipped++;
		}
		CloseTransientFile(fd);
	}
	FreeDir(dir);

	PG_RETURN_INT64(shipped);
}

/*
 * pagestore_slru_read_at(slru text, pageno int, lsn pg_lsn) returns bytea -- read an
 * SLRU page back from the store as-of 'lsn' (verifies a shipped snapshot; the branch
 * seed path reads its base pages this way).
 */
PG_FUNCTION_INFO_V1(pagestore_slru_read_at);
Datum
pagestore_slru_read_at(PG_FUNCTION_ARGS)
{
	char	   *slru = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		pageno = PG_GETARG_INT32(1);
	XLogRecPtr	lsn = PG_GETARG_LSN(2);
	PageStoreRelKey key;
	char	   *out = palloc(BLCKSZ);
	bytea	   *result;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	slru_check_dir(slru);

	slru_obj_key(&key, slru);
	/*
	 * As-of read: return the newest snapshot version <= lsn (so a base shipped at
	 * cutoff C still reads back when requested at a later L >= C), or NULL on a genuine
	 * miss (no version <= lsn) so a caller never mistakes a zero page for real clog
	 * state.  Exact-cutoff matching -- the tombstone guard that stops an older image
	 * resurfacing after a truncation -- lives in the branch reconstruct/seed, which
	 * read the base at the chosen cutoff itself (resolved == that cutoff).
	 */
	if (!pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key, (BlockNumber) pageno,
										(uint64) lsn, out, NULL))
		PG_RETURN_NULL();

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), out, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * SLRU-status applier (M4 step 2): clog reconstruction as of an LSN.
 *
 * A branch's transaction status as of fork LSN L = the parent's clog snapshot at a
 * cutoff C (the base, read from the store) PLUS the per-commit/abort effects of the
 * xact WAL records in (C, L].  Replaying per-update records -- not coalescing a
 * flushed page image -- is what makes it branch-correct: two xids on the same clog
 * page that commit on either side of L get the right answer (SLRU_ON_STORE_DESIGN.md).
 * This applies clog only; multixact/commit-ts are the same shape and follow.  It reads
 * the parent's local WAL; a store-backed reader for a branch with no local WAL (as in
 * redo_page_asof) is a follow-up.
 */

/* clog page/bit math -- mirrors the static macros in clog.c (stable on-disk format) */
#define PS_CLOG_BITS_PER_XACT	2
#define PS_CLOG_XACTS_PER_BYTE	4
#define PS_CLOG_XACTS_PER_PAGE	(BLCKSZ * PS_CLOG_XACTS_PER_BYTE)
#define PS_CLOG_XACT_BITMASK	((1 << PS_CLOG_BITS_PER_XACT) - 1)

/* Set xid's 2-bit status in clog page 'page' (page number 'pageno'); xids that map
 * to a different page, and non-normal xids (bootstrap/frozen), are ignored. */
static void
ps_clog_set_status(char *page, int64 pageno, TransactionId xid, int status)
{
	int			pgidx,
				byteno,
				bshift;

	if (!TransactionIdIsNormal(xid) ||
		(int64) (xid / PS_CLOG_XACTS_PER_PAGE) != pageno)
		return;
	pgidx = xid % PS_CLOG_XACTS_PER_PAGE;
	byteno = pgidx / PS_CLOG_XACTS_PER_BYTE;
	bshift = (pgidx % PS_CLOG_XACTS_PER_BYTE) * PS_CLOG_BITS_PER_XACT;
	page[byteno] = (page[byteno] & ~(PS_CLOG_XACT_BITMASK << bshift)) |
		(status << bshift);
}

/* Set xid's status in a contiguous array of clog pages starting at 'page_lo'
 * (np pages); xids outside the covered range are ignored.  Used by the seeder's
 * single multi-page WAL pass. */
static inline void
ps_clog_seed_set(char *pages, int64 page_lo, int np, TransactionId xid, int status)
{
	int64		pg;

	if (!TransactionIdIsNormal(xid))
		return;
	pg = (int64) xid / PS_CLOG_XACTS_PER_PAGE;
	if (pg < page_lo || pg >= page_lo + np)
		return;
	ps_clog_set_status(pages + (pg - page_lo) * BLCKSZ, pg, xid, status);
}

/* What the replay of (base, target] observed for a single clog page. */
typedef struct PsClogReplay
{
	bool		reached_target; /* scan covered the WAL up to target_lsn */
	bool		page_zeroed;	/* a CLOG_ZEROPAGE (re)created this page in range */
	bool		page_truncated; /* a CLOG_TRUNCATE removed this page in range */
} PsClogReplay;

/* Highest LSN the local WAL reader (read_local_xlog_page_no_wait) can serve: the replay
 * pointer during recovery / on a standby, the flush pointer on a primary.  Used to tell
 * "target is past the last record but the WAL is fully present" (complete) from "target
 * is beyond the readable WAL" (a genuine short read). */
static XLogRecPtr
ps_local_wal_limit(void)
{
	return RecoveryInProgress() ? GetXLogReplayRecPtr(NULL) : GetFlushRecPtr(NULL);
}

/*
 * Replay (base_lsn, target_lsn] from the local WAL onto clog page 'pageno' in 'page':
 *  - XLOG_XACT_{COMMIT,ABORT}[_PREPARED]: set the top xid + subxids' status;
 *  - RM_CLOG_ID CLOG_ZEROPAGE: zero this page (a page reused after wraparound must not
 *    keep the base image's stale bits, which clog_redo() would have cleared);
 *  - RM_CLOG_ID CLOG_TRUNCATE: note that this page was truncated away.
 * Record-aligned at target: a record ending after target_lsn is not part of as-of-L.
 * Reports how far the scan reached so the caller can fail closed on a short read.
 */
static PsClogReplay
ps_clog_apply_range(char *page, int64 pageno, XLogRecPtr base_lsn,
					XLogRecPtr target_lsn)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	PsClogReplay r = {false, false, false};

	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	/*
	 * Start from the beginning of base_lsn's WAL segment, not base_lsn itself.  A
	 * record that straddles the cutoff (starts before base_lsn, ends in (base_lsn,
	 * target_lsn]) still has its status effect inside the replay window -- the effect
	 * is record-aligned at EndRecPtr -- so it must be read; seeking to base_lsn would
	 * skip it.  Records ending at or before base_lsn are already reflected in the base
	 * snapshot and are filtered out below.  This also makes a base_lsn that does not
	 * fall on a record boundary harmless.
	 */
	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;
		RmgrId		rmid;
		int			status;
		TransactionId xid;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;						/* straddles/past L: as-of-L fully covered */
		if (reader->EndRecPtr <= base_lsn)
			continue;					/* effect already in the base snapshot */
		rmid = XLogRecGetRmid(reader);

		if (rmid == RM_CLOG_ID)
		{
			uint8		cinfo = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

			if (cinfo == CLOG_ZEROPAGE)
			{
				int64		zp;

				memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
				if (zp == pageno)
				{
					memset(page, 0, BLCKSZ);
					r.page_zeroed = true;
					r.page_truncated = false;	/* recreated: undo an earlier truncate */
				}
			}
			else if (cinfo == CLOG_TRUNCATE)
			{
				xl_clog_truncate xlrec;

				memcpy(&xlrec, XLogRecGetData(reader), sizeof(xlrec));
				/* SimpleLruTruncate drops whole segments below the cutoff page's
				 * segment; a fork window is far too short to wrap, so segment order
				 * suffices (no MaxTransactionId/2 wraparound compare needed). */
				if (pageno / SLRU_PAGES_PER_SEGMENT <
					xlrec.pageno / SLRU_PAGES_PER_SEGMENT)
					r.page_truncated = true;
			}
			continue;
		}
		if (rmid != RM_XACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_parsed_commit parsed;

			status = TRANSACTION_STATUS_COMMITTED;
			ParseCommitRecord(XLogRecGetInfo(reader),
							  (xl_xact_commit *) XLogRecGetData(reader), &parsed);
			xid = (info == XLOG_XACT_COMMIT_PREPARED) ? parsed.twophase_xid
				: XLogRecGetXid(reader);
			ps_clog_set_status(page, pageno, xid, status);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_clog_set_status(page, pageno, parsed.subxacts[i], status);
		}
		else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
		{
			xl_xact_parsed_abort parsed;

			status = TRANSACTION_STATUS_ABORTED;
			ParseAbortRecord(XLogRecGetInfo(reader),
							 (xl_xact_abort *) XLogRecGetData(reader), &parsed);
			xid = (info == XLOG_XACT_ABORT_PREPARED) ? parsed.twophase_xid
				: XLogRecGetXid(reader);
			ps_clog_set_status(page, pageno, xid, status);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_clog_set_status(page, pageno, parsed.subxacts[i], status);
		}
	}

	/*
	 * "Reached" means the whole (base, target] window was covered.  Normally that is
	 * the last applied record ending at/after target, but target may be an arbitrary
	 * LSN past the last record in the window (a branch cutoff between records): if the
	 * scan actually started (readfrom valid -- base's WAL segment was readable) and
	 * ended *cleanly* (no decode error) and the readable WAL extends through target,
	 * there are simply no further effects -- complete, not a short read.  An unreadable
	 * start (recycled base segment -> readfrom invalid), a decode error (errm set), or a
	 * target beyond the readable WAL is incomplete and must fail closed, so we never
	 * seed a clog that skipped the (base, target] records.
	 */
	r.reached_target = (scanned >= target_lsn) ||
		(!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		 ps_local_wal_limit() >= target_lsn);
	XLogReaderFree(reader);
	pfree(pd);
	return r;
}

/*
 * Load the base clog page (the store snapshot as-of base_lsn) and replay (base, L].
 * Returns true with the page filled, or false when the page was truncated away by L
 * (a normal CLOG_TRUNCATE in the window) -- the caller should then omit it, exactly as
 * relation redo_page_asof omits a truncated block, rather than fail the whole branch.
 * Still fails closed (ereport) on a real error rather than return a wrong page:
 *  - incomplete WAL (scan did not reach L) -- unless L is the "latest readable"
 *    sentinel PG_UINT64_MAX, which means "replay all WAL on hand";
 *  - the base page existed at base_lsn but is absent from the store (a miss that was
 *    not (re)created by an in-range zero-page), which would otherwise read back as an
 *    all-zero clog and make pre-base commits look IN_PROGRESS.
 */
static bool
ps_clog_reconstruct(char *page, int64 pageno, XLogRecPtr base_lsn,
					XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	bool		base_found;
	uint64		resolved = 0;
	PsClogReplay r;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	slru_obj_key(&key, "pg_xact");
	/* the base must be the snapshot shipped at exactly this cutoff: an older newest-<=
	 * image would skip the WAL between it and base_lsn (lost commits / a resurrected
	 * truncated page), so treat a non-exact resolve as "base absent". */
	base_found = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												(BlockNumber) pageno,
												(uint64) base_lsn, page, &resolved) &&
		resolved == (uint64) base_lsn;

	/* target == base: the empty (base, target] needs no WAL, so don't even open the
	 * reader -- the snapshot may predate the locally-retained WAL.  The base page is
	 * the whole answer; it must be present at exactly this cutoff. */
	if (target_lsn == base_lsn)
	{
		if (!base_found)
			ereport(ERROR,
					(errmsg("pagestore: base clog snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
		return true;
	}

	r = ps_clog_apply_range(page, pageno, base_lsn, target_lsn);

	if (!r.reached_target && target_lsn != PG_UINT64_MAX)
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct clog as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (r.page_truncated)
		return false;			/* removed by a CLOG_TRUNCATE in (base, L]: omit it */
	if (!base_found && !r.page_zeroed)
		ereport(ERROR,
				(errmsg("pagestore: base clog snapshot for page %lld is absent at %X/%08X",
						(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	return true;
}

/*
 * pagestore_clog_page_asof(pageno int, base pg_lsn, target pg_lsn) returns bytea --
 * the clog page reconstructed as of 'target' from the base snapshot at 'base'.
 */
PG_FUNCTION_INFO_V1(pagestore_clog_page_asof);
Datum
pagestore_clog_page_asof(PG_FUNCTION_ARGS)
{
	int32		pageno = PG_GETARG_INT32(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	char	   *page = palloc(BLCKSZ);
	bytea	   *result;

	if (!ps_clog_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();		/* page truncated away by the target LSN: omit it */

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * pagestore_clog_status_asof(xid xid, base pg_lsn, target pg_lsn) returns int -- the
 * 2-bit clog status of 'xid' as of 'target' (0 in-progress, 1 committed, 2 aborted).
 */
PG_FUNCTION_INFO_V1(pagestore_clog_status_asof);
Datum
pagestore_clog_status_asof(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_TRANSACTIONID(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	int64		pageno = xid / PS_CLOG_XACTS_PER_PAGE;
	int			pgidx = xid % PS_CLOG_XACTS_PER_PAGE;
	int			byteno = pgidx / PS_CLOG_XACTS_PER_BYTE;
	int			bshift = (pgidx % PS_CLOG_XACTS_PER_BYTE) * PS_CLOG_BITS_PER_XACT;
	char	   *page = palloc(BLCKSZ);

	if (!ps_clog_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();		/* xid's clog page truncated away by the target LSN */

	PG_RETURN_INT32((page[byteno] >> bshift) & PS_CLOG_XACT_BITMASK);
}

/*
 * commit-ts reconstruction (M4): the same shape as the clog applier, for pg_commit_ts.
 *
 * Each xid's commit timestamp is set as a side effect of its XLOG_XACT_COMMIT record (no
 * separate WAL record), so reconstruction loads the parent's commit-ts snapshot as of the
 * base cutoff C and replays the commit records in (C, L], writing each committed xid's
 * (timestamp, origin) into its commit-ts page entry -- exactly what clog_redo's
 * TransactionTreeSetCommitTsData() does.  pg_commit_ts entries are fixed-width
 * (TimestampTz + RepOriginId), packed COMMIT_TS_XACTS_PER_PAGE to a page.
 *
 * Scope: assumes track_commit_timestamp is stable across (C, L].  A XLOG_PARAMETER_CHANGE
 * that turns it off in the window makes a faithful image impossible, so we fail closed;
 * full toggle replay (re-activating/zeroing segment state) is a follow-up.
 */
#define PS_CTS_ENTRY_SIZE	(sizeof(TimestampTz) + sizeof(RepOriginId))
#define PS_CTS_XACTS_PER_PAGE	(BLCKSZ / PS_CTS_ENTRY_SIZE)

/* Write xid's (ts, nodeid) into commit-ts page 'pageno' in 'page' if it lives there. */
static void
ps_commit_ts_set(char *page, int64 pageno, TransactionId xid,
				 TimestampTz ts, RepOriginId nodeid)
{
	int			entryno;
	char	   *e;

	if (!TransactionIdIsNormal(xid) ||
		(int64) (xid / PS_CTS_XACTS_PER_PAGE) != pageno)
		return;
	entryno = xid % PS_CTS_XACTS_PER_PAGE;
	e = page + (Size) entryno * PS_CTS_ENTRY_SIZE;
	memcpy(e, &ts, sizeof(TimestampTz));
	memcpy(e + sizeof(TimestampTz), &nodeid, sizeof(RepOriginId));
}

/* Replay (base_lsn, target_lsn] onto commit-ts page 'pageno'.  Mirrors
 * ps_clog_apply_range: same straddle/zeropage/truncate/completeness handling, but applies
 * commit timestamps (commits only -- aborts have no commit-ts) and watches for a
 * commit-ts deactivation.  *deactivated is set if track_commit_timestamp is turned off. */
static PsClogReplay
ps_commit_ts_apply_range(char *page, int64 pageno, XLogRecPtr base_lsn,
						 XLogRecPtr target_lsn, bool *deactivated)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	PsClogReplay r = {false, false, false};

	*deactivated = false;
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;
		RmgrId		rmid;
		TransactionId xid;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;
		if (reader->EndRecPtr <= base_lsn)
			continue;
		rmid = XLogRecGetRmid(reader);

		if (rmid == RM_COMMIT_TS_ID)
		{
			uint8		cinfo = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

			if (cinfo == COMMIT_TS_ZEROPAGE)
			{
				int64		zp;

				memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
				if (zp == pageno)
				{
					memset(page, 0, BLCKSZ);
					r.page_zeroed = true;
					r.page_truncated = false;
				}
			}
			else if (cinfo == COMMIT_TS_TRUNCATE)
			{
				xl_commit_ts_truncate xlrec;

				/* the record carries only SizeOfCommitTsTruncate bytes; sizeof() would
				 * include trailing struct padding and read past the WAL payload */
				memcpy(&xlrec, XLogRecGetData(reader), SizeOfCommitTsTruncate);
				if (pageno / SLRU_PAGES_PER_SEGMENT <
					xlrec.pageno / SLRU_PAGES_PER_SEGMENT)
					r.page_truncated = true;
			}
			continue;
		}
		if (rmid == RM_XLOG_ID)
		{
			if ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_PARAMETER_CHANGE)
			{
				xl_parameter_change xlrec;

				memcpy(&xlrec, XLogRecGetData(reader), sizeof(xlrec));
				if (!xlrec.track_commit_timestamp)
					*deactivated = true;
			}
			continue;
		}
		if (rmid != RM_XACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_parsed_commit parsed;
			TimestampTz ts;
			RepOriginId origin = XLogRecGetOrigin(reader);

			ParseCommitRecord(XLogRecGetInfo(reader),
							  (xl_xact_commit *) XLogRecGetData(reader), &parsed);
			ts = (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) ? parsed.origin_timestamp
				: parsed.xact_time;
			xid = (info == XLOG_XACT_COMMIT_PREPARED) ? parsed.twophase_xid
				: XLogRecGetXid(reader);
			ps_commit_ts_set(page, pageno, xid, ts, origin);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_commit_ts_set(page, pageno, parsed.subxacts[i], ts, origin);
		}
	}

	r.reached_target = (scanned >= target_lsn) ||
		(!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		 ps_local_wal_limit() >= target_lsn);
	XLogReaderFree(reader);
	pfree(pd);
	return r;
}

/* Load the base commit-ts page (snapshot as-of base_lsn) and replay (base, L].  Same
 * fail-closed contract as ps_clog_reconstruct, plus: fail closed if commit-ts was turned
 * off in the window (the reconstructed image would not match the parent's). */
static bool
ps_commit_ts_reconstruct(char *page, int64 pageno, XLogRecPtr base_lsn,
						 XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	bool		base_found;
	bool		deactivated = false;
	uint64		resolved = 0;
	PsClogReplay r;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	slru_obj_key(&key, "pg_commit_ts");
	base_found = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												(BlockNumber) pageno,
												(uint64) base_lsn, page, &resolved) &&
		resolved == (uint64) base_lsn;
	/* Unlike clog (where an all-zero page reads as IN_PROGRESS and an absent base must fail
	 * closed), a zero commit-ts entry simply means "no commit timestamp" -- the correct
	 * as-of answer for a page that does not exist at base_lsn: one first created after the
	 * cutoff, or below the commit-ts validity window when track_commit_timestamp was turned
	 * on partway through history or after a commit-ts truncation.  So treat an absent base
	 * as an empty page and replay (base, L] onto zeros rather than erroring. */
	if (!base_found)
		memset(page, 0, BLCKSZ);

	if (target_lsn == base_lsn)
		return true;

	r = ps_commit_ts_apply_range(page, pageno, base_lsn, target_lsn, &deactivated);

	if (!r.reached_target && target_lsn != PG_UINT64_MAX)
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct commit-ts as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (deactivated)
		ereport(ERROR,
				(errmsg("pagestore: track_commit_timestamp was turned off in (base, target]; commit-ts reconstruction across a toggle is not supported")));
	if (r.page_truncated)
		return false;
	return true;
}

/*
 * pagestore_commit_ts_asof(xid xid, base pg_lsn, target pg_lsn, oldest xid) returns
 * timestamptz -- the commit timestamp of 'xid' as of 'target', or NULL if it has none.
 *
 * 'oldest' is the commit-ts validity horizon as of target (the parent's oldestCommitTsXid
 * from pg_control), which the booted branch's TransactionIdGetCommitTsData() enforces:
 * xids below it return NULL even though their bytes may physically remain on a retained
 * SLRU page (a COMMIT_TS_TRUNCATE only drops whole earlier segments), and xids that
 * committed while commit-ts was inactive -- e.g. before an off->on activation in the window
 * -- are below it too.  The page reconstruction stays byte-faithful to the parent's disk
 * page; this horizon check is what makes the lookup match the parent.  Pass FirstNormal
 * (or any value <= xid) to disable the check.
 */
PG_FUNCTION_INFO_V1(pagestore_commit_ts_asof);
Datum
pagestore_commit_ts_asof(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_TRANSACTIONID(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	TransactionId oldest = PG_GETARG_TRANSACTIONID(3);
	int64		pageno = xid / PS_CTS_XACTS_PER_PAGE;
	int			entryno = xid % PS_CTS_XACTS_PER_PAGE;
	char	   *page = palloc(BLCKSZ);
	TimestampTz ts;

	/* below the commit-ts horizon as of target -> no valid timestamp, exactly as
	 * TransactionIdGetCommitTsData() rejects xids before oldestCommitTsXid */
	if (TransactionIdIsNormal(oldest) && TransactionIdPrecedes(xid, oldest))
		PG_RETURN_NULL();

	if (!ps_commit_ts_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();		/* page truncated away by the target LSN */

	memcpy(&ts, page + (Size) entryno * PS_CTS_ENTRY_SIZE, sizeof(TimestampTz));
	if (ts == 0)
		PG_RETURN_NULL();		/* no commit timestamp recorded for this xid */
	PG_RETURN_TIMESTAMPTZ(ts);
}

PG_FUNCTION_INFO_V1(pagestore_commit_ts_page_asof);
Datum
pagestore_commit_ts_page_asof(PG_FUNCTION_ARGS)
{
	int32		pageno = PG_GETARG_INT32(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	char	   *page = palloc(BLCKSZ);
	bytea	   *result;

	if (!ps_commit_ts_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * multixact offsets reconstruction (M4): the same shape as the clog applier, for
 * pg_multixact/offsets -- the multixid -> member-offset map.  Each XLOG_MULTIXACT_CREATE_ID
 * record assigns one multixid its starting offset into the members file (what
 * RecordNewMultiXact writes to the offsets SLRU), so reconstruction loads the offsets
 * snapshot as of the base cutoff C and replays the create records in (C, L], writing each
 * new multixid's offset into its page.  Offsets are fixed-width MultiXactOffset (uint32),
 * MULTIXACT_OFFSETS_PER_PAGE to a page.  (The members file -- offset -> member list -- is
 * the second half, a follow-up; this reconstructs the offsets map only.)
 */
#define PS_MXOFF_PER_PAGE	(BLCKSZ / (int) sizeof(MultiXactOffset))

/* Write multixid's member offset into offsets page 'pageno' in 'page' if it lives there. */
static void
ps_mxoff_set(char *page, int64 pageno, MultiXactId multi, MultiXactOffset offset)
{
	int			entryno;

	if (!MultiXactIdIsValid(multi) ||
		(int64) (multi / PS_MXOFF_PER_PAGE) != pageno)
		return;
	entryno = multi % PS_MXOFF_PER_PAGE;
	memcpy(page + (Size) entryno * sizeof(MultiXactOffset), &offset,
		   sizeof(MultiXactOffset));
}

/* Replay (base_lsn, target_lsn] onto offsets page 'pageno'.  Mirrors ps_clog_apply_range:
 * same straddle/zeropage/truncate/completeness handling, applying create-id offsets. */
static PsClogReplay
ps_mxoff_apply_range(char *page, int64 pageno, XLogRecPtr base_lsn,
					 XLogRecPtr target_lsn)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	PsClogReplay r = {false, false, false};

	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;
		if (reader->EndRecPtr <= base_lsn)
			continue;
		if (XLogRecGetRmid(reader) != RM_MULTIXACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLR_RMGR_INFO_MASK;

		if (info == XLOG_MULTIXACT_ZERO_OFF_PAGE)
		{
			int64		zp;

			memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
			if (zp == pageno)
			{
				memset(page, 0, BLCKSZ);
				r.page_zeroed = true;
				r.page_truncated = false;
			}
		}
		else if (info == XLOG_MULTIXACT_CREATE_ID)
		{
			xl_multixact_create *xlrec =
				(xl_multixact_create *) XLogRecGetData(reader);
			MultiXactOffset next = xlrec->moff + xlrec->nmembers;
			MultiXactId succ;

			/* RecordNewMultiXact writes this multixid's offset *and* the next
			 * multixid's slot with the end offset (skipping 0 on wrap, like
			 * GetNewMultiXactId), so a faithful page carries the successor entry
			 * too -- and a consumer needs it to size this multixid's member run.
			 * The successor wraps MaxMultiXactId -> FirstMultiXactId, matching
			 * GetNewMultiXactId, so a fork across multixid wraparound is exact. */
			ps_mxoff_set(page, pageno, xlrec->mid, xlrec->moff);
			if (next == 0)
				next = 1;
			succ = (xlrec->mid == MaxMultiXactId) ? FirstMultiXactId : xlrec->mid + 1;
			ps_mxoff_set(page, pageno, succ, next);
		}
		else if (info == XLOG_MULTIXACT_TRUNCATE_ID)
		{
			xl_multixact_truncate xlrec;
			MultiXactId cutoff;

			memcpy(&xlrec, XLogRecGetData(reader), SizeOfMultiXactTruncate);
			/* PerformOffsetsTruncation truncates to
			 * MultiXactIdToOffsetPage(PreviousMultiXactId(endTruncOff)), keeping
			 * that page's segment; a page is dropped only if its segment is below
			 * it.  The bounded fork window cannot wrap, so segment order suffices. */
			cutoff = (xlrec.endTruncOff == FirstMultiXactId) ? MaxMultiXactId
				: xlrec.endTruncOff - 1;
			if (pageno / SLRU_PAGES_PER_SEGMENT <
				((int64) cutoff / PS_MXOFF_PER_PAGE) / SLRU_PAGES_PER_SEGMENT)
				r.page_truncated = true;
		}
	}

	r.reached_target = (scanned >= target_lsn) ||
		(!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		 ps_local_wal_limit() >= target_lsn);
	XLogReaderFree(reader);
	pfree(pd);
	return r;
}

/* Load the base offsets page (snapshot as-of base_lsn) and replay (base, L].  Same
 * fail-closed contract as ps_clog_reconstruct. */
static bool
ps_mxoff_reconstruct(char *page, int64 pageno, XLogRecPtr base_lsn,
					 XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	bool		base_found;
	uint64		resolved = 0;
	PsClogReplay r;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	slru_obj_key(&key, "pg_multixact/offsets");
	base_found = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												(BlockNumber) pageno,
												(uint64) base_lsn, page, &resolved) &&
		resolved == (uint64) base_lsn;

	if (target_lsn == base_lsn)
	{
		if (!base_found)
			ereport(ERROR,
					(errmsg("pagestore: base offsets snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
		return true;
	}

	r = ps_mxoff_apply_range(page, pageno, base_lsn, target_lsn);

	if (!r.reached_target && target_lsn != PG_UINT64_MAX)
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct multixact offsets as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (r.page_truncated)
		return false;
	if (!base_found && !r.page_zeroed)
		ereport(ERROR,
				(errmsg("pagestore: base offsets snapshot for page %lld is absent at %X/%08X",
						(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	return true;
}

/*
 * pagestore_multixact_offset_asof(multi xid, base pg_lsn, target pg_lsn) returns bigint --
 * multixid 'multi's starting member offset as of 'target', or NULL if its offsets page was
 * truncated away.
 */
PG_FUNCTION_INFO_V1(pagestore_multixact_offset_asof);
Datum
pagestore_multixact_offset_asof(PG_FUNCTION_ARGS)
{
	MultiXactId multi = PG_GETARG_TRANSACTIONID(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	int64		pageno = multi / PS_MXOFF_PER_PAGE;
	int			entryno = multi % PS_MXOFF_PER_PAGE;
	char	   *page = palloc(BLCKSZ);
	MultiXactOffset offset;

	if (!ps_mxoff_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();		/* multi's offsets page truncated away by the target LSN */

	memcpy(&offset, page + (Size) entryno * sizeof(MultiXactOffset),
		   sizeof(MultiXactOffset));
	PG_RETURN_INT64((int64) offset);
}

/*
 * pagestore_multixact_offsets_page_asof(pageno int, base pg_lsn, target pg_lsn) returns
 * bytea -- the offsets page reconstructed as of 'target', or NULL if truncated away.  Lets
 * a caller compare the reconstructed page to the live file byte-for-byte (endian-agnostic).
 */
PG_FUNCTION_INFO_V1(pagestore_multixact_offsets_page_asof);
Datum
pagestore_multixact_offsets_page_asof(PG_FUNCTION_ARGS)
{
	int32		pageno = PG_GETARG_INT32(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	char	   *page = palloc(BLCKSZ);
	bytea	   *result;

	if (!ps_mxoff_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/*
 * multixact members reconstruction (M4): the second half, pg_multixact/members -- the
 * offset -> (xid, status) member list that the offsets map points into.  Each
 * XLOG_MULTIXACT_CREATE_ID carries the member array, which RecordNewMultiXact lays out in
 * fixed groups: a group is 4 status-flag bytes followed by 4 member TransactionIds, packed
 * MULTIXACT_MEMBERGROUPS_PER_PAGE to a page.  Reconstruction loads the members snapshot at
 * C and replays the create records in (C, L], writing each member's xid and 8-bit status
 * into its group slot -- exactly as multixact_redo does.  With the offsets half (#68), a
 * full multixact resolves as-of L: offsets[M] gives its first member offset.
 */
#define PS_MXMEMB_FLAGBYTES_PER_GROUP	4
#define PS_MXMEMB_BITS_PER_XACT			8
#define PS_MXMEMB_PER_GROUP				PS_MXMEMB_FLAGBYTES_PER_GROUP	/* 1 flag/byte */
#define PS_MXMEMB_GROUP_SIZE \
	((int) (sizeof(TransactionId) * PS_MXMEMB_PER_GROUP + PS_MXMEMB_FLAGBYTES_PER_GROUP))
#define PS_MXMEMB_GROUPS_PER_PAGE		(BLCKSZ / PS_MXMEMB_GROUP_SIZE)
#define PS_MXMEMB_PER_PAGE				(PS_MXMEMB_GROUPS_PER_PAGE * PS_MXMEMB_PER_GROUP)

/* Write the member at 'offset' (xid + 8-bit status) into members page 'pageno' if it lives
 * there -- the same group layout as multixact.c's MXOffsetTo{Member,Flags}Offset. */
static void
ps_mxmemb_set(char *page, int64 pageno, MultiXactOffset offset,
			  TransactionId xid, uint32 status)
{
	int			group,
				grouponpg,
				byteoff,
				member_in_group,
				memberoff,
				bshift;
	uint32		flagsval;

	if ((int64) (offset / PS_MXMEMB_PER_PAGE) != pageno)
		return;
	group = offset / PS_MXMEMB_PER_GROUP;
	grouponpg = group % PS_MXMEMB_GROUPS_PER_PAGE;
	byteoff = grouponpg * PS_MXMEMB_GROUP_SIZE;		/* flags word at byteoff */
	member_in_group = offset % PS_MXMEMB_PER_GROUP;
	memberoff = byteoff + PS_MXMEMB_FLAGBYTES_PER_GROUP +
		member_in_group * (int) sizeof(TransactionId);
	bshift = member_in_group * PS_MXMEMB_BITS_PER_XACT;

	memcpy(page + memberoff, &xid, sizeof(TransactionId));
	memcpy(&flagsval, page + byteoff, sizeof(uint32));
	flagsval &= ~(((1 << PS_MXMEMB_BITS_PER_XACT) - 1) << bshift);
	flagsval |= (status << bshift);
	memcpy(page + byteoff, &flagsval, sizeof(uint32));
}

static int64
ps_mxmemb_segment(MultiXactOffset offset)
{
	return ((int64) (offset / PS_MXMEMB_PER_PAGE)) / SLRU_PAGES_PER_SEGMENT;
}

static bool
ps_mxmemb_segment_truncated(int64 pageseg, MultiXactOffset start,
							MultiXactOffset end)
{
	int64		startseg = ps_mxmemb_segment(start);
	int64		endseg = ps_mxmemb_segment(end);

	if (startseg == endseg)
		return false;
	if (startseg < endseg)
		return pageseg >= startseg && pageseg < endseg;
	return pageseg >= startseg || pageseg < endseg;
}

/* Replay (base_lsn, target_lsn] onto members page 'pageno'.  Mirrors ps_mxoff_apply_range,
 * applying each create record's member array and handling the members zero-page/truncate. */
static PsClogReplay
ps_mxmemb_apply_range(char *page, int64 pageno, XLogRecPtr base_lsn,
					  XLogRecPtr target_lsn)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	PsClogReplay r = {false, false, false};

	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;
		if (reader->EndRecPtr <= base_lsn)
			continue;
		if (XLogRecGetRmid(reader) != RM_MULTIXACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLR_RMGR_INFO_MASK;

		if (info == XLOG_MULTIXACT_ZERO_MEM_PAGE)
		{
			int64		zp;

			memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
			if (zp == pageno)
			{
				memset(page, 0, BLCKSZ);
				r.page_zeroed = true;
				r.page_truncated = false;
			}
		}
		else if (info == XLOG_MULTIXACT_CREATE_ID)
		{
			xl_multixact_create *xlrec =
				(xl_multixact_create *) XLogRecGetData(reader);

			for (int i = 0; i < xlrec->nmembers; i++)
				ps_mxmemb_set(page, pageno, xlrec->moff + i,
							  xlrec->members[i].xid,
							  (uint32) xlrec->members[i].status);
		}
		else if (info == XLOG_MULTIXACT_TRUNCATE_ID)
		{
			xl_multixact_truncate xlrec;

			memcpy(&xlrec, XLogRecGetData(reader), SizeOfMultiXactTruncate);
			if (ps_mxmemb_segment_truncated(pageno / SLRU_PAGES_PER_SEGMENT,
											xlrec.startTruncMemb,
											xlrec.endTruncMemb))
				r.page_truncated = true;
		}
	}

	r.reached_target = (scanned >= target_lsn) ||
		(!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		 ps_local_wal_limit() >= target_lsn);
	XLogReaderFree(reader);
	pfree(pd);
	return r;
}

/* Load the base members page (snapshot as-of base_lsn) and replay (base, L].  Same
 * fail-closed contract as ps_clog_reconstruct. */
static bool
ps_mxmemb_reconstruct(char *page, int64 pageno, XLogRecPtr base_lsn,
					  XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	bool		base_found;
	uint64		resolved = 0;
	PsClogReplay r;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	slru_obj_key(&key, "pg_multixact/members");
	base_found = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												(BlockNumber) pageno,
												(uint64) base_lsn, page, &resolved) &&
		resolved == (uint64) base_lsn;

	if (target_lsn == base_lsn)
	{
		if (!base_found)
			ereport(ERROR,
					(errmsg("pagestore: base members snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
		return true;
	}

	r = ps_mxmemb_apply_range(page, pageno, base_lsn, target_lsn);

	if (!r.reached_target && target_lsn != PG_UINT64_MAX)
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct multixact members as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (r.page_truncated)
		return false;
	if (!base_found && !r.page_zeroed)
		ereport(ERROR,
				(errmsg("pagestore: base members snapshot for page %lld is absent at %X/%08X",
						(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	return true;
}

/*
 * pagestore_multixact_members_page_asof(pageno int, base pg_lsn, target pg_lsn) returns
 * bytea -- the members page reconstructed as of 'target', or NULL if truncated away.
 */
PG_FUNCTION_INFO_V1(pagestore_multixact_members_page_asof);
Datum
pagestore_multixact_members_page_asof(PG_FUNCTION_ARGS)
{
	int32		pageno = PG_GETARG_INT32(0);
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	char	   *page = palloc(BLCKSZ);
	bytea	   *result;

	if (!ps_mxmemb_reconstruct(page, pageno, base, target))
		PG_RETURN_NULL();

	result = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

/* Load commit-ts pages in [page_lo, page_hi], replay (base, target] once, and mark each
 * page as present when we have a known byte value for at least one slot.
 */
static bool
ps_commit_ts_seed_reconstruct_range(char *pages, bool *present, int64 page_lo,
								   int64 page_hi, int64 req_lo, int64 req_hi,
								   XLogRecPtr base_lsn,
								   XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	ReadLocalXLogPageNoWaitPrivate *pd = NULL;
	XLogReaderState *reader = NULL;
	RmgrId		rmid;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	int64		np = page_hi - page_lo + 1;
	bool		deactivated = false;
	XLogRecPtr	reached_from;
	TransactionId xid;
	TransactionId prepared_xid;
	bool	   *base_found;
	bool	   *zeroed;
	bool	   *truncated;

	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	base_found = palloc0(np * sizeof(bool));
	zeroed = palloc0(np * sizeof(bool));
	truncated = palloc0(np * sizeof(bool));
	slru_obj_key(&key, "pg_commit_ts");
	for (int64 p = 0; p < np; p++)
	{
		uint64		resolved = 0;

		base_found[p] = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												   (BlockNumber) (page_lo + p),
												   (uint64) base_lsn,
												   pages + p * BLCKSZ,
												   &resolved) &&
			resolved == (uint64) base_lsn;
		present[p] = base_found[p];
		if (!base_found[p])
			memset(pages + p * BLCKSZ, 0, BLCKSZ);
	}

	if (target_lsn == base_lsn)
		goto check_required_pages;

	pd = palloc0(sizeof(*pd));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	reached_from = readfrom;
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		cinfo;
		uint8		info;
		int64		pageno;
		int64		idx;
		TimestampTz ts;
		RepOriginId origin;
		xl_xact_parsed_commit parsed;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;
		if (reader->EndRecPtr <= base_lsn)
			continue;

		rmid = XLogRecGetRmid(reader);
		if (rmid == RM_COMMIT_TS_ID)
		{
			cinfo = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
			if (cinfo == COMMIT_TS_ZEROPAGE)
			{
				int64		zp;

				memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
				if (zp >= page_lo && zp <= page_hi)
				{
					idx = zp - page_lo;
					memset(pages + idx * BLCKSZ, 0, BLCKSZ);
					present[idx] = true;
					zeroed[idx] = true;
					truncated[idx] = false;
				}
			}
			else if (cinfo == COMMIT_TS_TRUNCATE)
			{
				xl_commit_ts_truncate xlrec;
				int64		pageseg;
				int64		cut_seg;

				/* the record carries only SizeOfCommitTsTruncate bytes; sizeof() would
				 * include trailing struct padding and read past the WAL payload */
				memcpy(&xlrec, XLogRecGetData(reader), SizeOfCommitTsTruncate);
				cut_seg = xlrec.pageno / SLRU_PAGES_PER_SEGMENT;
				for (int64 p = 0; p < np; p++)
				{
					pageseg = (page_lo + p) / SLRU_PAGES_PER_SEGMENT;
					if (pageseg < cut_seg)
					{
						present[p] = false;
						truncated[p] = true;
					}
				}
			}
		}
		else if (rmid == RM_XLOG_ID)
		{
			if ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_PARAMETER_CHANGE)
			{
				xl_parameter_change xlrec;

				memcpy(&xlrec, XLogRecGetData(reader), sizeof(xlrec));
				if (!xlrec.track_commit_timestamp)
					deactivated = true;
			}
			continue;
		}
		else if (rmid != RM_XACT_ID)
			continue;

		info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
		if (info != XLOG_XACT_COMMIT && info != XLOG_XACT_COMMIT_PREPARED)
			continue;
		ParseCommitRecord(XLogRecGetInfo(reader),
						 (xl_xact_commit *) XLogRecGetData(reader), &parsed);
		ts = (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) ? parsed.origin_timestamp
			: parsed.xact_time;
		origin = XLogRecGetOrigin(reader);
		prepared_xid = parsed.twophase_xid;
		xid = (info == XLOG_XACT_COMMIT_PREPARED) ? prepared_xid : XLogRecGetXid(reader);
		pageno = (int64) xid / PS_CTS_XACTS_PER_PAGE;
		if (pageno >= page_lo && pageno <= page_hi)
		{
			idx = pageno - page_lo;
			ps_commit_ts_set(pages + idx * BLCKSZ, pageno, xid, ts, origin);
			present[idx] = true;
		}
		for (int i = 0; i < parsed.nsubxacts; i++)
		{
			xid = parsed.subxacts[i];
			pageno = (int64) xid / PS_CTS_XACTS_PER_PAGE;
			if (pageno >= page_lo && pageno <= page_hi)
			{
				idx = pageno - page_lo;
				ps_commit_ts_set(pages + idx * BLCKSZ, pageno, xid, ts, origin);
				present[idx] = true;
			}
		}
	}

	if (scanned < target_lsn && target_lsn != PG_UINT64_MAX &&
		(XLogRecPtrIsInvalid(reached_from) || errm != NULL ||
		 ps_local_wal_limit() < target_lsn))
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct commit-ts as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (deactivated)
		ereport(ERROR,
				(errmsg("pagestore: track_commit_timestamp was turned off in (base, target]; commit-ts reconstruction across a toggle is not supported")));

check_required_pages:
	for (int64 pageno = req_lo; pageno <= req_hi; pageno++)
	{
		int64		idx = pageno - page_lo;

		if (truncated[idx])
			ereport(ERROR,
					(errmsg("pagestore: requested commit-ts page %lld was truncated before the target LSN",
							(long long) pageno)));
		if (!base_found[idx] && !zeroed[idx])
			ereport(ERROR,
					(errmsg("pagestore: base commit-ts snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	}

	XLogReaderFree(reader);
	pfree(pd);
	pfree(truncated);
	pfree(zeroed);
	pfree(base_found);
	return true;
}

/* Load and replay multixact offsets for all requested pages in one WAL pass, marking each
 * requested page as present when any content is known.
 */
static bool
ps_mxoff_seed_reconstruct_range(char *pages, bool *present,
								int64 page_lo, int64 page_hi,
								int64 req_lo, int64 req_hi,
								XLogRecPtr base_lsn, XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	ReadLocalXLogPageNoWaitPrivate *pd = NULL;
	XLogReaderState *reader = NULL;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	int64		np = page_hi - page_lo + 1;
	uint64		resolved = 0;
	bool	   *base_found;
	bool	   *zeroed;
	bool	   *truncated;

	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	base_found = palloc0(np * sizeof(bool));
	zeroed = palloc0(np * sizeof(bool));
	truncated = palloc0(np * sizeof(bool));
	slru_obj_key(&key, "pg_multixact/offsets");
	for (int64 p = 0; p < np; p++)
	{
		base_found[p] = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												   (BlockNumber) (page_lo + p),
												   (uint64) base_lsn,
												   pages + p * BLCKSZ,
												   &resolved) &&
			resolved == (uint64) base_lsn;
		present[p] = base_found[p];
		if (!base_found[p])
			memset(pages + p * BLCKSZ, 0, BLCKSZ);
	}
	if (target_lsn == base_lsn)
		goto check_required_pages;

	pd = palloc0(sizeof(*pd));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;
		if (reader->EndRecPtr <= base_lsn)
			continue;
		if (XLogRecGetRmid(reader) != RM_MULTIXACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLR_RMGR_INFO_MASK;

		if (info == XLOG_MULTIXACT_ZERO_OFF_PAGE)
		{
			int64		zp;
			int64		idx;

			memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
			if (zp >= page_lo && zp <= page_hi)
			{
				idx = zp - page_lo;
				memset(pages + idx * BLCKSZ, 0, BLCKSZ);
				present[idx] = true;
				zeroed[idx] = true;
				truncated[idx] = false;
			}
		}
		else if (info == XLOG_MULTIXACT_CREATE_ID)
		{
			xl_multixact_create *xlrec =
				(xl_multixact_create *) XLogRecGetData(reader);
			MultiXactOffset next = xlrec->moff + xlrec->nmembers;
			MultiXactId succ;
			int64		mpageno;

			if (next == 0)
				next = 1;
			succ = (xlrec->mid == MaxMultiXactId) ? FirstMultiXactId : xlrec->mid + 1;

			mpageno = (int64) (xlrec->mid / PS_MXOFF_PER_PAGE);
			if (mpageno >= page_lo && mpageno <= page_hi)
			{
				int64	 idx = mpageno - page_lo;

				ps_mxoff_set(pages + idx * BLCKSZ, mpageno, xlrec->mid, xlrec->moff);
				present[idx] = true;
			}
			mpageno = (int64) (succ / PS_MXOFF_PER_PAGE);
			if (mpageno >= page_lo && mpageno <= page_hi)
			{
				int64	 idx = mpageno - page_lo;

				ps_mxoff_set(pages + idx * BLCKSZ, mpageno, succ, next);
				present[idx] = true;
			}
		}
		else if (info == XLOG_MULTIXACT_TRUNCATE_ID)
		{
			xl_multixact_truncate xlrec;
			MultiXactId cutoff;
			int64		cutoff_seg;

			memcpy(&xlrec, XLogRecGetData(reader), SizeOfMultiXactTruncate);
			cutoff = (xlrec.endTruncOff == FirstMultiXactId) ? MaxMultiXactId
				: xlrec.endTruncOff - 1;
			cutoff_seg = ((int64) cutoff / PS_MXOFF_PER_PAGE) / SLRU_PAGES_PER_SEGMENT;
			for (int64 p = 0; p < np; p++)
			{
				int64		pageseg = (page_lo + p) / SLRU_PAGES_PER_SEGMENT;

				if (pageseg < cutoff_seg)
				{
					present[p] = false;
					truncated[p] = true;
				}
			}
		}
	}

	if (scanned < target_lsn && target_lsn != PG_UINT64_MAX &&
		(XLogRecPtrIsInvalid(readfrom) || errm != NULL ||
		 ps_local_wal_limit() < target_lsn))
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct multixact offsets as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));

check_required_pages:
	for (int64 pageno = req_lo; pageno <= req_hi; pageno++)
	{
		int64		idx = pageno - page_lo;

		if (truncated[idx])
			ereport(ERROR,
					(errmsg("pagestore: requested multixact offsets page %lld was truncated before the target LSN",
							(long long) pageno)));
		if (!base_found[idx] && !zeroed[idx])
			ereport(ERROR,
					(errmsg("pagestore: base offsets snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	}

	XLogReaderFree(reader);
	pfree(pd);
	pfree(truncated);
	pfree(zeroed);
	pfree(base_found);
	return true;
}

/* Load and replay multixact members for all requested pages in one WAL pass, marking each
 * requested page as present when any content is known.
 */
static bool
ps_mxmemb_seed_reconstruct_range(char *pages, bool *present,
								int64 page_lo, int64 page_hi,
								int64 req_lo, int64 req_hi,
								XLogRecPtr base_lsn, XLogRecPtr target_lsn)
{
	PageStoreRelKey key;
	ReadLocalXLogPageNoWaitPrivate *pd = NULL;
	XLogReaderState *reader = NULL;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	int64		np = page_hi - page_lo + 1;
	uint64		resolved = 0;
	bool	   *base_found;
	bool	   *zeroed;
	bool	   *truncated;

	if (target_lsn < base_lsn)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	base_found = palloc0(np * sizeof(bool));
	zeroed = palloc0(np * sizeof(bool));
	truncated = palloc0(np * sizeof(bool));
	slru_obj_key(&key, "pg_multixact/members");
	for (int64 p = 0; p < np; p++)
	{
		base_found[p] = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
												   (BlockNumber) (page_lo + p),
												   (uint64) base_lsn,
												   pages + p * BLCKSZ,
												   &resolved) &&
			resolved == (uint64) base_lsn;
		present[p] = base_found[p];
		if (!base_found[p])
			memset(pages + p * BLCKSZ, 0, BLCKSZ);
	}
	if (target_lsn == base_lsn)
		goto check_required_pages;

	pd = palloc0(sizeof(*pd));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	{
		XLogSegNo	segno;

		XLByteToSeg(base_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target_lsn)
			break;
		if (reader->EndRecPtr <= base_lsn)
			continue;
		if (XLogRecGetRmid(reader) != RM_MULTIXACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLR_RMGR_INFO_MASK;

		if (info == XLOG_MULTIXACT_ZERO_MEM_PAGE)
		{
			int64		zp;
			int64		idx;

			memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
			if (zp >= page_lo && zp <= page_hi)
			{
				idx = zp - page_lo;
				memset(pages + idx * BLCKSZ, 0, BLCKSZ);
				present[idx] = true;
				zeroed[idx] = true;
				truncated[idx] = false;
			}
		}
		else if (info == XLOG_MULTIXACT_CREATE_ID)
		{
			xl_multixact_create *xlrec =
				(xl_multixact_create *) XLogRecGetData(reader);

			for (int i = 0; i < xlrec->nmembers; i++)
			{
				int64		pageno = (xlrec->moff + i) / PS_MXMEMB_PER_PAGE;

				if (pageno >= page_lo && pageno <= page_hi)
				{
					int64		idx = pageno - page_lo;

					ps_mxmemb_set(pages + idx * BLCKSZ, pageno,
								  xlrec->moff + i,
								  xlrec->members[i].xid,
								  (uint32) xlrec->members[i].status);
					present[idx] = true;
				}
			}
		}
		else if (info == XLOG_MULTIXACT_TRUNCATE_ID)
		{
			xl_multixact_truncate xlrec;

			memcpy(&xlrec, XLogRecGetData(reader), SizeOfMultiXactTruncate);
			for (int64 p = 0; p < np; p++)
			{
				int64		segno = (page_lo + p) / SLRU_PAGES_PER_SEGMENT;

				if (ps_mxmemb_segment_truncated(segno,
												xlrec.startTruncMemb,
												xlrec.endTruncMemb))
				{
					present[p] = false;
					truncated[p] = true;
				}
			}
		}
	}

	if (scanned < target_lsn && target_lsn != PG_UINT64_MAX &&
		(XLogRecPtrIsInvalid(readfrom) || errm != NULL ||
		 ps_local_wal_limit() < target_lsn))
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct multixact members as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));

check_required_pages:
	for (int64 pageno = req_lo; pageno <= req_hi; pageno++)
	{
		int64		idx = pageno - page_lo;

		if (truncated[idx])
			ereport(ERROR,
					(errmsg("pagestore: requested multixact members page %lld was truncated before the target LSN",
							(long long) pageno)));
		if (!base_found[idx] && !zeroed[idx])
			ereport(ERROR,
					(errmsg("pagestore: base members snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	}

	XLogReaderFree(reader);
	pfree(pd);
	pfree(truncated);
	pfree(zeroed);
	pfree(base_found);
	return true;
}

typedef bool (*SlruPageRangeReconstructFn) (char *pages, bool *present,
										   int64 page_lo, int64 page_hi,
										   int64 req_lo, int64 req_hi,
										   XLogRecPtr base_lsn,
										   XLogRecPtr target_lsn);

#define PS_CHECK_PATH_FORMAT(ret, buf) \
	do { \
		if ((ret) < 0 || (ret) >= (int) sizeof(buf)) \
			ereport(ERROR, \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
					 errmsg("branch target directory path is too long"))); \
	} while (0)

static void
pagestore_write_zero_slru_page(const char *slru_dir, const char *label,
							   int64 pageno)
{
	char		segpath[MAXPGPATH];
	char		zerobuf[BLCKSZ];
	int64		first = (pageno / SLRU_PAGES_PER_SEGMENT) * SLRU_PAGES_PER_SEGMENT;
	int			pathlen;
	int			fd;

	pathlen = snprintf(segpath, sizeof(segpath), "%s/%04X", slru_dir,
					   (unsigned int) (pageno / SLRU_PAGES_PER_SEGMENT));
	PS_CHECK_PATH_FORMAT(pathlen, segpath);
	fd = OpenTransientFilePerm(segpath,
							   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
							   pg_file_create_mode);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch %s bootstrap segment \"%s\": %m",
						label, segpath)));
	memset(zerobuf, 0, sizeof(zerobuf));
	for (int64 p = first; p <= pageno; p++)
	{
		for (int done = 0; done < BLCKSZ;)
		{
			ssize_t		written;

			errno = 0;
			written = write(fd, zerobuf + done, BLCKSZ - done);
			if (written <= 0)
			{
				if (written == 0)
					errno = ENOSPC;
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write branch %s bootstrap segment \"%s\": %m",
								label, segpath)));
			}
			done += written;
		}
	}
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync branch %s bootstrap segment \"%s\": %m",
						label, segpath)));
	}
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close branch %s bootstrap segment \"%s\": %m",
						label, segpath)));
	fsync_fname(slru_dir, true);
}

static int64
pagestore_seed_slru_pages(const char *target_dir, const char *slru_dir,
						  int64 page_lo, int64 page_hi,
						  SlruPageRangeReconstructFn reconstruct,
						  XLogRecPtr base, XLogRecPtr target,
						  const char *label, bool publish_dir)
{
	char		dstdir[MAXPGPATH];
	char		stagedir[MAXPGPATH];
	char	   *pages;
	bool	   *present;
	int64		seg_lo,
				seg_hi,
				req_lo,
				req_hi,
				seg,
				p;
	int			np;
	int64		seeded = 0;
	int			pathlen;

	if (page_hi < page_lo)
		return 0;
	if (strlen(target_dir) + strlen(slru_dir) + sizeof("/.tmp/000000.tmp") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch target directory path is too long")));

	req_lo = page_lo;
	req_hi = page_hi;
	seg_lo = req_lo / SLRU_PAGES_PER_SEGMENT;
	page_lo = seg_lo * SLRU_PAGES_PER_SEGMENT;
	seg_hi = req_hi / SLRU_PAGES_PER_SEGMENT;
	page_hi = req_hi;
	np = (int) (page_hi - page_lo + 1);

	pages = palloc((Size) np * BLCKSZ);
	present = palloc0(np * sizeof(bool));
	for (p = 0; p < np; p++)
		memset(pages + p * BLCKSZ, 0, BLCKSZ);
	if (!reconstruct(pages, present, page_lo, page_hi, req_lo, req_hi,
					 base, target))
	{
		ereport(ERROR,
				(errmsg("pagestore: failed to reconstruct %s pages in [%lld, %lld] as of %X/%08X",
						label, (long long) req_lo, (long long) req_hi,
						LSN_FORMAT_ARGS(target))));
	}
	for (p = 0; p < np; p++)
		if (page_lo + p < req_lo)
			memset(pages + p * BLCKSZ, 0, BLCKSZ);

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));
	pathlen = snprintf(dstdir, sizeof(dstdir), "%s/%s", target_dir, slru_dir);
	PS_CHECK_PATH_FORMAT(pathlen, dstdir);
	pathlen = snprintf(stagedir, sizeof(stagedir), "%s/%s.tmp", target_dir, slru_dir);
	PS_CHECK_PATH_FORMAT(pathlen, stagedir);

	if (publish_dir)
	{
		if (access(stagedir, F_OK) == 0 && !rmtree(stagedir, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not clear branch staging dir \"%s\"", stagedir)));
		if (pg_mkdir_p(stagedir, pg_dir_create_mode) != 0 && errno != EEXIST)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create branch staging dir \"%s\": %m", stagedir)));
	}
	else if (MakePGDirectory(dstdir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch %s dir \"%s\": %m", label, dstdir)));

	for (seg = seg_lo; seg <= seg_hi; seg++)
	{
		char		segpath[MAXPGPATH];
		int64		first = seg * SLRU_PAGES_PER_SEGMENT;
		int64		last = first + SLRU_PAGES_PER_SEGMENT - 1;
		int			fd;
		int64		trim_last = last;

		if (trim_last > page_hi)
			trim_last = page_hi;
		while (trim_last >= first && !present[trim_last - page_lo])
			trim_last--;
		if (trim_last < first)
			continue;

		pathlen = snprintf(segpath, sizeof(segpath), "%s/%04X",
							publish_dir ? stagedir : dstdir, (unsigned int) seg);
		PS_CHECK_PATH_FORMAT(pathlen, segpath);
		fd = OpenTransientFilePerm(segpath,
								   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
								   pg_file_create_mode);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create branch %s segment \"%s\": %m",
							label, segpath)));
		for (p = first; p <= trim_last; p++)
		{
			const char *src;
			char		zerobuf[BLCKSZ];

			if (present[p - page_lo])
				src = pages + (p - page_lo) * BLCKSZ;
			else
			{
				memset(zerobuf, 0, sizeof(zerobuf));
				src = zerobuf;
			}
			for (int done = 0; done < BLCKSZ;)
			{
				ssize_t		written;

				errno = 0;
				written = write(fd, src + done, BLCKSZ - done);
				if (written <= 0)
				{
					if (written == 0)
						errno = ENOSPC;
					CloseTransientFile(fd);
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write branch %s segment \"%s\": %m",
									label, segpath)));
				}
				done += written;
			}
			seeded++;
		}
		if (pg_fsync(fd) != 0)
		{
			CloseTransientFile(fd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync branch %s segment \"%s\": %m",
							label, segpath)));
		}
		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close branch %s segment \"%s\": %m",
							label, segpath)));
	}
	fsync_fname(publish_dir ? stagedir : dstdir, true);
	if (publish_dir)
	{
		if (access(dstdir, F_OK) == 0 && !rmtree(dstdir, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove existing %s dir \"%s\"", label, dstdir)));
		if (rename(stagedir, dstdir) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not publish branch %s dir \"%s\": %m", label, dstdir)));
	}
	fsync_fname(target_dir, true);

	pfree(present);
	pfree(pages);
	return seeded;
}

/*
 * pagestore_seed_clog(target_dir text, base pg_lsn, target pg_lsn,
 *					   oldest_xid xid, next_xid xid) returns bigint
 *
 * M4 step 3 (branch create): materialize a new branch's clog as of fork LSN 'target'
 *
 * M4 step 3 (branch create): materialize a new branch's clog as of fork LSN 'target'
 * into <target_dir>/pg_xact, by reconstructing each page (base snapshot at 'base' +
 * replay of (base, target]) over the fork's clog horizon [oldest_xid, next_xid) and
 * writing whole segments.  The branch then
 * boots on this clog and writes its own status forward on its timeline.  Fails closed
 * (ereport) on any error -- a half-seeded clog must never boot.  Returns the page
 * count.  clog only; multixact/commit-ts seed the same way (follow-up).
 */
PG_FUNCTION_INFO_V1(pagestore_seed_clog);
Datum
pagestore_seed_clog(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	TransactionId oldest_xid = PG_GETARG_TRANSACTIONID(3);
	TransactionId next_xid = PG_GETARG_TRANSACTIONID(4);
	char		dstdir[MAXPGPATH];
	char		stagedir[MAXPGPATH];
	PageStoreRelKey key;
	int64		page_lo,
				page_hi,
				seg_lo,
				seg_hi,
				seg,
				p;
	int			np;
	char	   *pages;
	bool	   *established;		/* page content known (in base, or zeroed in range) */
	XLogRecPtr	scanned = base;
	XLogRecPtr	readfrom;
	ReadLocalXLogPageNoWaitPrivate *pd;
	XLogReaderState *reader;
	char	   *errm = NULL;
	int64		seeded = 0;

	/* Writes server-side files under a caller-supplied path: superuser only, like the
	 * other server-file-access functions, and checked before any filesystem effect. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to seed a branch clog")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target < base)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));
	if (!TransactionIdIsNormal(oldest_xid) || !TransactionIdIsNormal(next_xid) ||
		TransactionIdFollows(oldest_xid, next_xid))
		ereport(ERROR,
				(errmsg("invalid fork xid horizon [%u, %u)", oldest_xid, next_xid)));
	/* every path we build is <target_dir>/pg_xact/XXXX[.tmp]; reject if it won't fit */
	if (strlen(target_dir) + sizeof("/pg_xact/0000.tmp") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch target directory path is too long")));

	/*
	 * Derive the seeded segments from the fork's clog horizon as of L, not from the
	 * parent's current on-disk layout (which may have truncated or extended since L).
	 * clog truncates whole segments, so round the oldest live xid down to a segment;
	 * the newest page is the one holding next_xid - 1.  Whole segments are written so
	 * the branch's pg_xact is the layout SimpleLru expects.
	 */
	page_lo = ((int64) oldest_xid / PS_CLOG_XACTS_PER_PAGE);
	seg_lo = page_lo / SLRU_PAGES_PER_SEGMENT;
	page_lo = seg_lo * SLRU_PAGES_PER_SEGMENT;
	page_hi = ((int64) (next_xid - 1)) / PS_CLOG_XACTS_PER_PAGE;
	seg_hi = page_hi / SLRU_PAGES_PER_SEGMENT;
	/* The horizon is wraparound-aware (TransactionIdFollows above), but the page
	 * numbers are not: a horizon straddling the 32-bit wrap makes page_lo land near
	 * the top and page_hi near zero.  Rather than seed a bogus/negative range, fail
	 * closed -- clog seeding across the wrap point is out of scope. */
	if (page_hi < page_lo)
		ereport(ERROR,
				(errmsg("fork xid horizon [%u, %u) spans XID wraparound; clog seeding across wrap is not supported",
						oldest_xid, next_xid)));
	np = (int) (page_hi - page_lo + 1);

	/* Build every page in [page_lo, page_hi] in memory, then a single WAL pass over
	 * (base, target] applies statuses/zero-pages/truncations to all of them at once
	 * -- not a fresh scan per page. */
	pages = palloc((Size) np * BLCKSZ);
	established = palloc0(np * sizeof(bool));
	slru_obj_key(&key, "pg_xact");
	for (p = 0; p < np; p++)
	{
		uint64		resolved = 0;

		/* require the exact-cutoff snapshot version: an older newest-<= image would
		 * skip WAL between it and base (see ps_clog_reconstruct) */
		established[p] = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
													   (BlockNumber) (page_lo + p),
													   (uint64) base,
													   pages + p * BLCKSZ, &resolved) &&
			resolved == (uint64) base;
	}
	/*
	 * established[p] means page p's content is known: found in the base snapshot, or
	 * (re)created by an in-range CLOG_ZEROPAGE below.  After the scan, any page still
	 * unestablished means the base is missing a page that existed at the cutoff --
	 * fail closed rather than seed an all-zero (IN_PROGRESS) page.
	 */

	pd = palloc0(sizeof(*pd));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	/*
	 * target == base: empty (base, target] -- skip the scan entirely (don't open
	 * possibly-recycled WAL); the base pages alone are the seed.  Otherwise start at the
	 * beginning of base's WAL segment so a record that straddles the cutoff (starts
	 * before base, ends in (base, target]) is still read; records ending at/before base
	 * are already in the base snapshot and are filtered out below.
	 */
	if (target == base)
		readfrom = InvalidXLogRecPtr;
	else
	{
		XLogSegNo	segno;

		XLByteToSeg(base, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
		readfrom = XLogFindNextRecord(reader, readfrom);
	}
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) && XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;
		RmgrId		rmid;
		int			status;
		TransactionId xid;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > target)
			break;
		if (reader->EndRecPtr <= base)
			continue;				/* effect already in the base snapshot */
		rmid = XLogRecGetRmid(reader);

		if (rmid == RM_CLOG_ID)
		{
			uint8		cinfo = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

			if (cinfo == CLOG_ZEROPAGE)
			{
				int64		zp;

				memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
				if (zp >= page_lo && zp <= page_hi)
				{
					memset(pages + (zp - page_lo) * BLCKSZ, 0, BLCKSZ);
					established[zp - page_lo] = true;
				}
			}
			else if (cinfo == CLOG_TRUNCATE)
			{
				xl_clog_truncate xlrec;

				memcpy(&xlrec, XLogRecGetData(reader), sizeof(xlrec));
				/* truncation drops whole segments below the cutoff page's segment; if
				 * that reaches the oldest seeded segment the caller's horizon is stale
				 * (a fork window is far too short to wrap, so segment order suffices) */
				if (xlrec.pageno / SLRU_PAGES_PER_SEGMENT > seg_lo)
					ereport(ERROR,
							(errmsg("pagestore: clog truncation to page %lld occurs within (base, target]; horizon is stale",
									(long long) xlrec.pageno)));
			}
			continue;
		}
		if (rmid != RM_XACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_parsed_commit parsed;

			status = TRANSACTION_STATUS_COMMITTED;
			ParseCommitRecord(XLogRecGetInfo(reader),
							  (xl_xact_commit *) XLogRecGetData(reader), &parsed);
			xid = (info == XLOG_XACT_COMMIT_PREPARED) ? parsed.twophase_xid
				: XLogRecGetXid(reader);
			ps_clog_seed_set(pages, page_lo, np, xid, status);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_clog_seed_set(pages, page_lo, np, parsed.subxacts[i], status);
		}
		else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
		{
			xl_xact_parsed_abort parsed;

			status = TRANSACTION_STATUS_ABORTED;
			ParseAbortRecord(XLogRecGetInfo(reader),
							 (xl_xact_abort *) XLogRecGetData(reader), &parsed);
			xid = (info == XLOG_XACT_ABORT_PREPARED) ? parsed.twophase_xid
				: XLogRecGetXid(reader);
			ps_clog_seed_set(pages, page_lo, np, xid, status);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_clog_seed_set(pages, page_lo, np, parsed.subxacts[i], status);
		}
	}
	XLogReaderFree(reader);
	pfree(pd);

	/* Fail closed unless the window was fully covered: complete only if the scan
	 * actually started (readfrom valid -- base's WAL segment was readable), ended
	 * cleanly (no decode error), and the readable WAL extends through target.  An
	 * unreadable start (recycled base segment), a decode error (errm), or target beyond
	 * the readable WAL is a short read -- never seed a clog that skipped (base, target].
	 * (ps_local_wal_limit is replay-aware, so this also holds on a standby.) */
	if (scanned < target && target != PG_UINT64_MAX &&
		(XLogRecPtrIsInvalid(readfrom) || errm != NULL ||
		 ps_local_wal_limit() < target))
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot seed clog as of %X/%08X",
						LSN_FORMAT_ARGS(target))));
	for (p = 0; p < np; p++)
		if (!established[p])
			ereport(ERROR,
					(errmsg("pagestore: base clog snapshot for page %lld is absent at %X/%08X",
							(long long) (page_lo + p), LSN_FORMAT_ARGS(base))));

	/*
	 * Publish atomically at directory granularity: stage every segment under a
	 * sibling pg_xact.tmp, fsync it, then swap the whole directory into place with a
	 * single rename().  Any failure -- a write, an fsync, a missing base -- leaves
	 * only the staging directory, never a partly-populated live pg_xact, so a
	 * multi-segment seed is all-or-nothing even if it aborts between segments.
	 */
	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));
	snprintf(dstdir, sizeof(dstdir), "%s/pg_xact", target_dir);
	snprintf(stagedir, sizeof(stagedir), "%s/pg_xact.tmp", target_dir);
	/* Start from an empty staging dir.  A leftover pg_xact.tmp from an interrupted
	 * retry could hold hex-named segments outside this seed's [seg_lo, seg_hi]; those
	 * stale files would survive the rename and pollute the branch's live pg_xact, so
	 * remove the whole staging dir first rather than reusing it. */
	if (access(stagedir, F_OK) == 0 && !rmtree(stagedir, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear branch staging dir \"%s\"", stagedir)));
	if (MakePGDirectory(stagedir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch staging dir \"%s\": %m", stagedir)));

	for (seg = seg_lo; seg <= seg_hi; seg++)
	{
		char		segpath[MAXPGPATH];
		int64		first = seg * SLRU_PAGES_PER_SEGMENT;
		int64		last = first + SLRU_PAGES_PER_SEGMENT - 1;
		int			fd;

		if (last > page_hi)
			last = page_hi;
		snprintf(segpath, sizeof(segpath), "%s/%04X", stagedir, (unsigned int) seg);
		fd = OpenTransientFilePerm(segpath,
								   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
								   pg_file_create_mode);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create branch segment \"%s\": %m", segpath)));
		for (p = first; p <= last; p++)
		{
			if (write(fd, pages + (p - page_lo) * BLCKSZ, BLCKSZ) != BLCKSZ)
			{
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write branch segment \"%s\": %m", segpath)));
			}
			seeded++;
		}
		if (pg_fsync(fd) != 0)
		{
			CloseTransientFile(fd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync branch segment \"%s\": %m", segpath)));
		}
		/* a close error can surface a deferred write failure -- fail the seed */
		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close branch segment \"%s\": %m", segpath)));
	}
	fsync_fname(stagedir, true);	/* segment entries durable inside the staging dir */
	/*
	 * A branch datadir is initdb'd, so pg_xact already exists with BootStrapCLOG's
	 * 0000; rename() onto a non-empty directory fails with ENOTEMPTY.  Remove the
	 * default first, then swap the staged dir in.  (The branch is not yet running, and
	 * a crash in the gap is recoverable by re-seeding -- the staging dir is rebuilt.)
	 */
	if (access(dstdir, F_OK) == 0 && !rmtree(dstdir, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove existing clog dir \"%s\"", dstdir)));
	if (rename(stagedir, dstdir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish branch clog \"%s\": %m", dstdir)));
	fsync_fname(target_dir, true);	/* the now-live pg_xact directory entry */

	PG_RETURN_INT64(seeded);
}

/*
 * pagestore_seed_commit_ts(target_dir text, base pg_lsn, target pg_lsn,
 *                          oldest_xid xid, next_xid xid) returns bigint
 *
 * Materialize pg_commit_ts as of target into a branch data directory.  The xid
 * horizon is the branch's commit-ts validity window as of target; pages outside
 * it are intentionally not seeded.
 */
PG_FUNCTION_INFO_V1(pagestore_seed_commit_ts);
Datum
pagestore_seed_commit_ts(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	TransactionId oldest_xid = PG_GETARG_TRANSACTIONID(3);
	TransactionId next_xid = PG_GETARG_TRANSACTIONID(4);
	int64		page_lo,
				page_hi;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to seed branch commit-ts")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target < base)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));
	if (!TransactionIdIsNormal(oldest_xid) || !TransactionIdIsNormal(next_xid) ||
		TransactionIdFollows(oldest_xid, next_xid))
		ereport(ERROR,
				(errmsg("invalid fork xid horizon [%u, %u)", oldest_xid, next_xid)));
	page_lo = ((int64) oldest_xid / PS_CTS_XACTS_PER_PAGE);
	page_hi = ((int64) (next_xid - 1)) / PS_CTS_XACTS_PER_PAGE;
	if (page_hi < page_lo)
		ereport(ERROR,
				(errmsg("fork xid horizon [%u, %u) spans XID wraparound; commit-ts seeding across wrap is not supported",
						oldest_xid, next_xid)));

	PG_RETURN_INT64(pagestore_seed_slru_pages(target_dir, "pg_commit_ts",
											 page_lo, page_hi,
											 ps_commit_ts_seed_reconstruct_range,
											 base, target, "commit-ts", true));
}

/*
 * pagestore_seed_multixact(target_dir text, base pg_lsn, target pg_lsn,
 *                          oldest_multi xid, next_multi xid,
 *                          oldest_member bigint, next_member bigint) returns bigint
 *
 * Materialize pg_multixact/offsets and pg_multixact/members as of target into a
 * branch data directory.  The caller supplies the branch horizons from pg_control.
 */
PG_FUNCTION_INFO_V1(pagestore_seed_multixact);
Datum
pagestore_seed_multixact(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	MultiXactId oldest_multi = PG_GETARG_TRANSACTIONID(3);
	MultiXactId next_multi = PG_GETARG_TRANSACTIONID(4);
	int64		oldest_member = PG_GETARG_INT64(5);
	int64		next_member = PG_GETARG_INT64(6);
	int64		off_page_lo,
				off_page_hi,
				mem_page_lo,
				mem_page_hi;
	int64		seeded = 0;
	char		mxdir[MAXPGPATH],
				mxstage[MAXPGPATH],
				offdir[MAXPGPATH],
				memdir[MAXPGPATH];
	int			pathlen;
	int64		off_seeded = 0,
				mem_seeded = 0;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to seed branch multixact")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target < base)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));
	if (!MultiXactIdIsValid(oldest_multi) ||
		(next_multi != InvalidMultiXactId && !MultiXactIdIsValid(next_multi)) ||
		(oldest_multi != next_multi &&
		 !MultiXactIdPrecedes(oldest_multi, next_multi)))
		ereport(ERROR,
				(errmsg("invalid fork multixact horizon [%u, %u)",
						oldest_multi, next_multi)));
	if (oldest_member < 0 || next_member < 0 ||
		oldest_member > UINT32_MAX ||
		next_member > UINT32_MAX)
		ereport(ERROR,
				(errmsg("invalid fork multixact member horizon [%lld, %lld)",
						(long long) oldest_member, (long long) next_member)));

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));
	pathlen = snprintf(mxdir, sizeof(mxdir), "%s/pg_multixact", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, mxdir);
	pathlen = snprintf(mxstage, sizeof(mxstage), "%s/pg_multixact.tmp", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, mxstage);
	pathlen = snprintf(offdir, sizeof(offdir), "%s/offsets", mxstage);
	PS_CHECK_PATH_FORMAT(pathlen, offdir);
	pathlen = snprintf(memdir, sizeof(memdir), "%s/members", mxstage);
	PS_CHECK_PATH_FORMAT(pathlen, memdir);
	if (access(mxstage, F_OK) == 0 && !rmtree(mxstage, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear branch multixact staging dir \"%s\"", mxstage)));
	if (MakePGDirectory(mxstage) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch multixact staging dir \"%s\": %m", mxstage)));
	if (MakePGDirectory(offdir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch multixact offsets dir \"%s\": %m", offdir)));
	if (MakePGDirectory(memdir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch multixact members dir \"%s\": %m", memdir)));

	if (oldest_multi != next_multi)
	{
		if (oldest_multi < next_multi)
		{
			off_page_lo = (int64) oldest_multi / PS_MXOFF_PER_PAGE;
			/* include boundary page for next_multi when it is the first entry on a new
			 * offsets page so next allocation can safely read page_next_multi; this
			 * page already exists in the parent and must be preserved for the branch.
			 */
			off_page_hi = (int64) (next_multi - 1) / PS_MXOFF_PER_PAGE;
			if (next_multi != InvalidMultiXactId &&
				next_multi % PS_MXOFF_PER_PAGE == 0)
				off_page_hi++;
			off_seeded += pagestore_seed_slru_pages(mxstage, "offsets",
													off_page_lo, off_page_hi,
													ps_mxoff_seed_reconstruct_range,
													base, target, "multixact offsets", false);
		}
		else
		{
			off_page_lo = (int64) oldest_multi / PS_MXOFF_PER_PAGE;
			off_page_hi = (int64) MaxMultiXactId / PS_MXOFF_PER_PAGE;
			off_seeded += pagestore_seed_slru_pages(mxstage, "offsets",
													off_page_lo, off_page_hi,
													ps_mxoff_seed_reconstruct_range,
													base, target, "multixact offsets", false);
			if (next_multi > FirstMultiXactId)
			{
				off_page_lo = (int64) FirstMultiXactId / PS_MXOFF_PER_PAGE;
				off_page_hi = (int64) (next_multi - 1) / PS_MXOFF_PER_PAGE;
				if (next_multi % PS_MXOFF_PER_PAGE == 0)
					off_page_hi++;
				off_seeded += pagestore_seed_slru_pages(mxstage, "offsets",
													off_page_lo, off_page_hi,
													ps_mxoff_seed_reconstruct_range,
													base, target, "multixact offsets", false);
			}
			else
			{
				off_page_lo = (int64) next_multi / PS_MXOFF_PER_PAGE;
				pagestore_write_zero_slru_page(offdir, "multixact offsets",
											   off_page_lo);
				off_seeded++;
			}
		}
	}
	else
	{
		off_page_lo = (int64) next_multi / PS_MXOFF_PER_PAGE;
		/*
		 * Even when the fork has no live multixacts, bootstrap must preserve page 0 so
		 * simple-lru startup can read the page containing zero or wrap counters, and
		 * preserve the page that tracks next_multi as the parent bootstrap state.
		 */
		pagestore_write_zero_slru_page(offdir, "multixact offsets",
									   0);
		pagestore_write_zero_slru_page(offdir, "multixact offsets",
									   off_page_lo);
		off_seeded += (off_page_lo == 0 ? 1 : 2);
	}
	if (next_member != oldest_member)
	{
		if (oldest_member < next_member)
		{
			mem_page_lo = oldest_member / PS_MXMEMB_PER_PAGE;
			/* include boundary page for next_member when it is the first slot on a new
			 * members page so the branch can allocate the next multixact immediately.
			 */
			mem_page_hi = (next_member - 1) / PS_MXMEMB_PER_PAGE;
			if (next_member % PS_MXMEMB_PER_PAGE == 0)
				mem_page_hi++;
			mem_seeded += pagestore_seed_slru_pages(mxstage, "members",
													mem_page_lo, mem_page_hi,
													ps_mxmemb_seed_reconstruct_range,
													base, target, "multixact members", false);
		}
		else
		{
			mem_page_lo = oldest_member / PS_MXMEMB_PER_PAGE;
			mem_page_hi = (int64) UINT32_MAX / PS_MXMEMB_PER_PAGE;
			mem_seeded += pagestore_seed_slru_pages(mxstage, "members",
													mem_page_lo, mem_page_hi,
													ps_mxmemb_seed_reconstruct_range,
													base, target, "multixact members", false);
			if (next_member > 0)
			{
				mem_page_lo = 0;
				mem_page_hi = (next_member - 1) / PS_MXMEMB_PER_PAGE;
				if (next_member % PS_MXMEMB_PER_PAGE == 0)
					mem_page_hi++;
				mem_seeded += pagestore_seed_slru_pages(mxstage, "members",
													mem_page_lo, mem_page_hi,
													ps_mxmemb_seed_reconstruct_range,
													base, target, "multixact members", false);
			}
			else
			{
				pagestore_write_zero_slru_page(memdir, "multixact members", 0);
				mem_seeded++;
			}
		}
	}
	else
	{
		mem_page_lo = next_member / PS_MXMEMB_PER_PAGE;
		pagestore_write_zero_slru_page(memdir, "multixact members",
									   0);
		pagestore_write_zero_slru_page(memdir, "multixact members",
									   mem_page_lo);
		mem_seeded += (mem_page_lo == 0 ? 1 : 2);
	}
	seeded += off_seeded + mem_seeded;

	fsync_fname(mxstage, true);
	if (access(mxdir, F_OK) == 0 && !rmtree(mxdir, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove existing branch multixact dir \"%s\"", mxdir)));
	if (rename(mxstage, mxdir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish branch multixact dir \"%s\": %m", mxdir)));
	fsync_fname(target_dir, true);
	PG_RETURN_INT64(seeded);
}

/* Publish one staged SLRU directory into the final target branch directory,
 * replacing any prior target version.  Missing staged directories (for skipped
 * optional SLRUs) are treated as a no-op.
 */
static bool
publish_seeded_slru_dir(const char *staging_root, const char *target_root,
						const char *backup_root, const char *slru_name)
{
	char		stagedir[MAXPGPATH];
	char		targetdir[MAXPGPATH];
	char		backupdir[MAXPGPATH];
	int			pathlen;

	pathlen = snprintf(stagedir, sizeof(stagedir), "%s/%s", staging_root,
					   slru_name);
	PS_CHECK_PATH_FORMAT(pathlen, stagedir);
	pathlen = snprintf(targetdir, sizeof(targetdir), "%s/%s", target_root,
					   slru_name);
	PS_CHECK_PATH_FORMAT(pathlen, targetdir);
	pathlen = snprintf(backupdir, sizeof(backupdir), "%s/%s", backup_root,
					   slru_name);
	PS_CHECK_PATH_FORMAT(pathlen, backupdir);

	if (access(stagedir, F_OK) != 0)
		return false;
	if (access(targetdir, F_OK) == 0 && rename(targetdir, backupdir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not move existing branch %s dir \"%s\" aside: %m",
						slru_name, targetdir)));
	if (rename(stagedir, targetdir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish branch %s dir \"%s\"", slru_name, targetdir)));
	return true;
}

static void
rollback_seeded_slru_dir(const char *target_root, const char *backup_root,
						 const char *slru_name, bool published)
{
	char		targetdir[MAXPGPATH];
	char		backupdir[MAXPGPATH];
	int			pathlen;

	pathlen = snprintf(targetdir, sizeof(targetdir), "%s/%s", target_root,
					   slru_name);
	PS_CHECK_PATH_FORMAT(pathlen, targetdir);
	pathlen = snprintf(backupdir, sizeof(backupdir), "%s/%s", backup_root,
					   slru_name);
	PS_CHECK_PATH_FORMAT(pathlen, backupdir);

	if (!published && access(backupdir, F_OK) != 0)
		return;
	if (published && access(targetdir, F_OK) == 0 && !rmtree(targetdir, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove partly-published branch %s dir \"%s\"",
						slru_name, targetdir)));
	if (access(backupdir, F_OK) == 0 && rename(backupdir, targetdir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not restore previous branch %s dir \"%s\": %m",
						slru_name, targetdir)));
}

/*
 * pagestore_seed_branch_slrus(target_dir text, base pg_lsn, target pg_lsn,
 *                             oldest_xid xid, next_xid xid,
 *                             oldest_commit_ts_xid xid, next_commit_ts_xid xid,
 *                             oldest_multi xid, next_multi xid,
 *                             oldest_member bigint, next_member bigint)
 * returns bigint
 *
 * Branch bootstrap convenience entrypoint: materialize every SLRU class needed
 * for a branch datadir to boot at target.  This intentionally centralizes the
 * ordering and fail-closed behavior that tests previously had to spell out as
 * separate seed_clog/seed_commit_ts/seed_multixact calls.  A later pg_control
 * bootstrap helper can derive these horizons from the fork manifest and call
 * this single function.
 */
static int64
pagestore_seed_branch_slrus_impl(const char *target_dir, XLogRecPtr base,
								 XLogRecPtr target, TransactionId oldest_xid,
								 TransactionId next_xid,
								 TransactionId oldest_commit_ts_xid,
								 TransactionId next_commit_ts_xid,
								 MultiXactId oldest_multi,
								 MultiXactId next_multi,
								 int64 oldest_member, int64 next_member)
{
	Datum		staging_dir_datum;
	char		staging_root[MAXPGPATH];
	char		backup_root[MAXPGPATH];
	bool		seed_commit_ts;
	volatile bool published_xact = false;
	volatile bool published_commit_ts = false;
	volatile bool published_multixact = false;
	int64		seeded = 0;
	int			pathlen;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to seed branch SLRUs")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (target < base)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));
	if (!TransactionIdIsNormal(oldest_xid) || !TransactionIdIsNormal(next_xid) ||
		TransactionIdFollows(oldest_xid, next_xid))
		ereport(ERROR,
				(errmsg("invalid fork xid horizon [%u, %u)", oldest_xid, next_xid)));
	if (!TransactionIdIsNormal(oldest_commit_ts_xid) &&
		!TransactionIdIsNormal(next_commit_ts_xid))
		seed_commit_ts = false;
	else if (!TransactionIdIsNormal(oldest_commit_ts_xid) ||
			 !TransactionIdIsNormal(next_commit_ts_xid) ||
			 TransactionIdFollows(oldest_commit_ts_xid, next_commit_ts_xid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid commit-ts horizon [%u, %u)",
						oldest_commit_ts_xid, next_commit_ts_xid)));
	else
		seed_commit_ts = true;
	if (!MultiXactIdIsValid(oldest_multi) ||
		(next_multi != InvalidMultiXactId && !MultiXactIdIsValid(next_multi)) ||
		(oldest_multi != next_multi &&
		 !MultiXactIdPrecedes(oldest_multi, next_multi)))
		ereport(ERROR,
				(errmsg("invalid fork multixact horizon [%u, %u)",
						oldest_multi, next_multi)));
	if (oldest_member < 0 || next_member < 0 ||
		oldest_member > UINT32_MAX ||
		next_member > UINT32_MAX)
		ereport(ERROR,
				(errmsg("invalid fork multixact member horizon [%lld, %lld)",
						(long long) oldest_member, (long long) next_member)));

	pathlen = snprintf(staging_root, sizeof(staging_root),
					   "%s/.pagestore-branch-seed.%ld",
					   target_dir, (long) MyProcPid);
	PS_CHECK_PATH_FORMAT(pathlen, staging_root);
	pathlen = snprintf(backup_root, sizeof(backup_root),
					   "%s/.pagestore-branch-backup.%ld",
					   target_dir, (long) MyProcPid);
	PS_CHECK_PATH_FORMAT(pathlen, backup_root);
	staging_dir_datum = CStringGetTextDatum(staging_root);

	if (access(staging_root, F_OK) == 0 && !rmtree(staging_root, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear previous branch seeding staging area \"%s\"", staging_root)));
	if (access(backup_root, F_OK) == 0 && !rmtree(backup_root, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear previous branch seeding backup area \"%s\"", backup_root)));
	if (MakePGDirectory(staging_root) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch seeding staging area \"%s\"", staging_root)));

	PG_TRY();
	{
		seeded += DatumGetInt64(DirectFunctionCall5(pagestore_seed_clog,
									  staging_dir_datum,
									  LSNGetDatum(base),
									  LSNGetDatum(target),
									  TransactionIdGetDatum(oldest_xid),
									  TransactionIdGetDatum(next_xid)));

		if (seed_commit_ts)
			seeded += DatumGetInt64(DirectFunctionCall5(pagestore_seed_commit_ts,
										  staging_dir_datum,
										  LSNGetDatum(base),
										  LSNGetDatum(target),
										  TransactionIdGetDatum(oldest_commit_ts_xid),
										  TransactionIdGetDatum(next_commit_ts_xid)));

		seeded += DatumGetInt64(DirectFunctionCall7(pagestore_seed_multixact,
									  staging_dir_datum,
									  LSNGetDatum(base),
									  LSNGetDatum(target),
									  MultiXactIdGetDatum(oldest_multi),
									  MultiXactIdGetDatum(next_multi),
									  Int64GetDatum(oldest_member),
									  Int64GetDatum(next_member)));
	}
	PG_CATCH();
	{
		if (!rmtree(staging_root, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove branch seeding staging area after seed failure \"%s\"", staging_root)));
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_TRY();
	{
		if (MakePGDirectory(backup_root) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create branch seeding backup area \"%s\"", backup_root)));
		published_xact = publish_seeded_slru_dir(staging_root, target_dir,
												 backup_root, "pg_xact");
		if (seed_commit_ts)
			published_commit_ts = publish_seeded_slru_dir(staging_root, target_dir,
														  backup_root,
														  "pg_commit_ts");
		published_multixact = publish_seeded_slru_dir(staging_root, target_dir,
													  backup_root,
													  "pg_multixact");
		fsync_fname(target_dir, true);
		if (!rmtree(staging_root, true))
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove branch seeding staging area \"%s\"", staging_root)));
		if (!rmtree(backup_root, true))
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove branch seeding backup area \"%s\"", backup_root)));
	}
	PG_CATCH();
	{
		rollback_seeded_slru_dir(target_dir, backup_root, "pg_multixact",
								 published_multixact);
		if (seed_commit_ts)
			rollback_seeded_slru_dir(target_dir, backup_root, "pg_commit_ts",
									 published_commit_ts);
		rollback_seeded_slru_dir(target_dir, backup_root, "pg_xact",
								 published_xact);
		if (!rmtree(staging_root, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove branch seeding staging area after publish failure \"%s\"", staging_root)));
		if (access(backup_root, F_OK) == 0 && !rmtree(backup_root, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove branch seeding backup area after publish failure \"%s\"", backup_root)));
		PG_RE_THROW();
	}
	PG_END_TRY();

	return seeded;
}

PG_FUNCTION_INFO_V1(pagestore_seed_branch_slrus);
Datum
pagestore_seed_branch_slrus(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));

	PG_RETURN_INT64(pagestore_seed_branch_slrus_impl(target_dir,
													 PG_GETARG_LSN(1),
													 PG_GETARG_LSN(2),
													 PG_GETARG_TRANSACTIONID(3),
													 PG_GETARG_TRANSACTIONID(4),
													 PG_GETARG_TRANSACTIONID(5),
													 PG_GETARG_TRANSACTIONID(6),
													 PG_GETARG_TRANSACTIONID(7),
													 PG_GETARG_TRANSACTIONID(8),
													 PG_GETARG_INT64(9),
													 PG_GETARG_INT64(10)));
}

#define PAGESTORE_BRANCH_MANIFEST_MAXLEN 8192

static void
pagestore_write_branch_manifest(const char *target_dir,
								int32 new_tl, int32 parent_tl,
								XLogRecPtr base, XLogRecPtr target,
								TransactionId oldest_xid,
								TransactionId next_xid,
								TransactionId oldest_commit_ts_xid,
								TransactionId next_commit_ts_xid,
								MultiXactId oldest_multi,
								MultiXactId next_multi,
								int64 oldest_member,
								int64 next_member,
								int64 seeded_pages)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	char		manifest[2048];
	int			len;
	int			fd;
	int			done = 0;

	if (strlen(target_dir) + sizeof("/pagestore_branch.manifest.tmp") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch target directory path is too long")));

	len = snprintf(manifest, sizeof(manifest),
				   "{\n"
				   "  \"format\": 1,\n"
				   "  \"new_timeline\": %d,\n"
				   "  \"parent_timeline\": %d,\n"
				   "  \"base_lsn\": \"%X/%08X\",\n"
				   "  \"fork_lsn\": \"%X/%08X\",\n"
				   "  \"oldest_xid\": \"%u\",\n"
				   "  \"next_xid\": \"%u\",\n"
				   "  \"oldest_commit_ts_xid\": \"%u\",\n"
				   "  \"next_commit_ts_xid\": \"%u\",\n"
				   "  \"oldest_multi\": \"%u\",\n"
				   "  \"next_multi\": \"%u\",\n"
				   "  \"oldest_member\": \"%lld\",\n"
				   "  \"next_member\": \"%lld\",\n"
				   "  \"seeded_slru_pages\": \"%lld\"\n"
				   "}\n",
				   new_tl, parent_tl,
				   LSN_FORMAT_ARGS(base),
				   LSN_FORMAT_ARGS(target),
				   oldest_xid, next_xid,
				   oldest_commit_ts_xid, next_commit_ts_xid,
				   oldest_multi, next_multi,
				   (long long) oldest_member,
				   (long long) next_member,
				   (long long) seeded_pages);
	if (len < 0 || len >= (int) sizeof(manifest))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch manifest is too large")));

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));

	snprintf(path, sizeof(path), "%s/pagestore_branch.manifest", target_dir);
	snprintf(tmppath, sizeof(tmppath), "%s/pagestore_branch.manifest.tmp", target_dir);
	fd = OpenTransientFilePerm(tmppath,
							   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
							   pg_file_create_mode);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch manifest \"%s\": %m", tmppath)));
	while (done < len)
	{
		ssize_t		written;

		errno = 0;
		written = write(fd, manifest + done, len - done);
		if (written <= 0)
		{
			if (written == 0)
				errno = ENOSPC;
			CloseTransientFile(fd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write branch manifest \"%s\": %m", tmppath)));
		}
		done += written;
	}
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync branch manifest \"%s\": %m", tmppath)));
	}
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close branch manifest \"%s\": %m", tmppath)));
	if (rename(tmppath, path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish branch manifest \"%s\": %m", path)));
	fsync_fname(target_dir, true);
}

static char *
pagestore_read_branch_manifest(const char *target_dir)
{
	char		path[MAXPGPATH];
	FILE	   *file;
	StringInfoData buf;
	char		tmp[1024];
	size_t		nread;
	long		manifest_size;

	if (strlen(target_dir) + sizeof("/pagestore_branch.manifest") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch target directory path is too long")));

	snprintf(path, sizeof(path), "%s/pagestore_branch.manifest", target_dir);
	file = AllocateFile(path, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
		{
			/* No manifest means this is a legacy/non-branch datadir. */
			return NULL;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open branch manifest \"%s\": %m", path)));
	}
	if (fseek(file, 0L, SEEK_END) != 0)
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read branch manifest \"%s\": %m", path)));
	}
	manifest_size = ftell(file);
	if (manifest_size < 0 || manifest_size > PAGESTORE_BRANCH_MANIFEST_MAXLEN)
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch manifest \"%s\" is too large", path)));
	}
	if (fseek(file, 0L, SEEK_SET) != 0)
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read branch manifest \"%s\": %m", path)));
	}

	initStringInfo(&buf);
		while ((nread = fread(tmp, 1, sizeof(tmp), file)) > 0)
		{
			if (buf.len + nread > PAGESTORE_BRANCH_MANIFEST_MAXLEN)
			{
			FreeFile(file);
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("branch manifest \"%s\" is too large", path)));
			}
			if (memchr(tmp, '\0', nread) != NULL)
			{
				FreeFile(file);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("branch manifest \"%s\" contains embedded NUL bytes",
								path)));
			}
			appendBinaryStringInfo(&buf, tmp, nread);
		}
	if (ferror(file))
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read branch manifest \"%s\": %m", path)));
	}
	FreeFile(file);
	return buf.data;
}

static const char *
pagestore_manifest_skip_ws(const char *p)
{
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;
	return p;
}

static const char *
pagestore_manifest_find_unique_field(const char *manifest, const char *key)
{
	size_t		keylen = strlen(key);
	const char *p = manifest;
	const char *field = NULL;
	int			depth = 0;

	while (*p != '\0')
	{
		if (*p == '{' || *p == '[')
		{
			depth++;
			p++;
			continue;
		}
		if (*p == '}' || *p == ']')
		{
			if (depth > 0)
				depth--;
			p++;
			continue;
		}
		if (*p == '"')
		{
			const char *name = p + 1;
			const char *end = name;

			while (*end != '\0')
			{
				if (*end == '\\' && end[1] != '\0')
				{
					end += 2;
					continue;
				}
				if (*end == '"')
					break;
				end++;
			}
			if (*end == '\0')
				return NULL;

			if (depth == 1)
			{
				const char *value = pagestore_manifest_skip_ws(end + 1);

				if (*value == ':' &&
					(size_t) (end - name) == keylen &&
					strncmp(name, key, keylen) == 0)
				{
					if (field != NULL)
						return NULL;
					field = pagestore_manifest_skip_ws(value + 1);
				}
			}
			p = end + 1;
			continue;
		}

		p++;
	}
	return field;
}

static bool
pagestore_manifest_value_delimited(const char *p)
{
	p = pagestore_manifest_skip_ws(p);
	return *p == '\0' || *p == ',' || *p == '}';
}

static bool
pagestore_manifest_get_uint_token(const char *manifest, const char *key,
								  uint32_t *value)
{
	const char *field = pagestore_manifest_find_unique_field(manifest, key);
	char	   *endptr;
	unsigned long long parsed;

	if (field == NULL || *field == '"')
		return false;
	if (*field < '0' || *field > '9')
		return false;
	errno = 0;
	parsed = strtoull(field, &endptr, 10);
	if (errno != 0 || endptr == field)
		return false;
	if (parsed > UINT32_MAX)
		return false;
	if (!pagestore_manifest_value_delimited(endptr))
		return false;
	*value = (uint32_t) parsed;
	return true;
}

static bool
pagestore_manifest_has_uint_token(const char *manifest, const char *key,
								 uint32_t value)
{
	uint32_t	parsed;

	if (!pagestore_manifest_get_uint_token(manifest, key, &parsed))
		return false;
	return parsed == value;
}

static bool
pagestore_manifest_has_string_token(const char *manifest, const char *key,
									const char *value)
{
	const char *field = pagestore_manifest_find_unique_field(manifest, key);
	size_t		len = strlen(value);

	if (field == NULL)
		return false;
	if (*field != '"')
		return false;
	field++;
	if (strncmp(field, value, len) != 0)
		return false;
	if (field[len] != '"')
		return false;
	return pagestore_manifest_value_delimited(field + len + 1);
}

static bool
pagestore_manifest_get_lsn_token(const char *manifest, const char *key,
								XLogRecPtr *lsn)
{
	const char *field = pagestore_manifest_find_unique_field(manifest, key);
	const char *end;
	char		buf[64];
	size_t		len;
	unsigned int hi,
				lo;
	char		extra;

	if (field == NULL || *field != '"')
		return false;
	field++;
	end = strchr(field, '"');
	if (end == NULL)
		return false;
	len = end - field;
	if (len == 0 || len >= sizeof(buf))
		return false;
	memcpy(buf, field, len);
	buf[len] = '\0';
	if (sscanf(buf, "%X/%X%c", &hi, &lo, &extra) != 2)
		return false;
	if (!pagestore_manifest_value_delimited(end + 1))
		return false;
	*lsn = ((uint64) hi << 32) | lo;
	return true;
}

static bool
pagestore_manifest_is_single_object(const char *manifest)
{
	const char *p = pagestore_manifest_skip_ws(manifest);
	int			depth = 0;

	if (*p != '{')
		return false;
	while (*p != '\0')
	{
		if (*p == '"')
		{
			p++;
			while (*p != '\0')
			{
				if (*p == '\\' && p[1] != '\0')
				{
					p += 2;
					continue;
				}
				if (*p == '"')
					break;
				p++;
			}
			if (*p == '\0')
				return false;
		}
		else if (*p == '{' || *p == '[')
			depth++;
		else if (*p == '}' || *p == ']')
		{
			depth--;
			if (depth == 0)
			{
				p = pagestore_manifest_skip_ws(p + 1);
				return *p == '\0';
			}
			if (depth < 0)
				return false;
		}
		p++;
	}
	return false;
}

static bool
pagestore_manifest_matches(const char *manifest, int32 new_tl, int32 parent_tl,
						   XLogRecPtr fork_lsn)
{
	char		buf[64];
	int			len;

	if (new_tl <= 0 || parent_tl < 0)
		return false;
	if (!pagestore_manifest_is_single_object(manifest))
		return false;

	if (!pagestore_manifest_has_uint_token(manifest, "format", 1))
		return false;
	len = snprintf(buf, sizeof(buf), "%u", new_tl);
	if (len < 0 || len >= (int) sizeof(buf))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch manifest token is too large")));
	if (!pagestore_manifest_has_uint_token(manifest, "new_timeline", new_tl))
		return false;
	len = snprintf(buf, sizeof(buf), "%u", parent_tl);
	if (len < 0 || len >= (int) sizeof(buf))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch manifest token is too large")));
	if (!pagestore_manifest_has_uint_token(manifest, "parent_timeline", parent_tl))
		return false;
	len = snprintf(buf, sizeof(buf), "%X/%08X", LSN_FORMAT_ARGS(fork_lsn));
	if (len < 0 || len >= (int) sizeof(buf))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch manifest token is too large")));
	if (!pagestore_manifest_has_string_token(manifest, "fork_lsn", buf))
		return false;
	return true;
}

static bool
pagestore_manifest_get_branch_identity(const char *manifest, uint32_t *new_tl,
									   uint32_t *parent_tl,
									   XLogRecPtr *fork_lsn)
{
	uint32_t	format;

	if (!pagestore_manifest_is_single_object(manifest))
		return false;

	return pagestore_manifest_get_uint_token(manifest, "format", &format) &&
		format == 1 &&
		pagestore_manifest_get_uint_token(manifest, "new_timeline", new_tl) &&
		*new_tl != 0 &&
		pagestore_manifest_get_uint_token(manifest, "parent_timeline", parent_tl) &&
		pagestore_manifest_get_lsn_token(manifest, "fork_lsn", fork_lsn);
}

static inline bool
pagestore_branch_backend_active(void)
{
	return pagestore_active_backend == &PageStoreBackendLocalSvc;
}

/*
 * pagestore_validate_branch_manifest(target_dir text, new_timeline int,
 *                                    parent_timeline int, fork_lsn pg_lsn)
 * returns bool
 *
 * Bootstrap preflight for a prepared branch datadir.  It verifies that the
 * durable manifest written by pagestore_prepare_branch() matches the timeline
 * identity the caller is about to boot.  This intentionally does not mutate the
 * datadir; it is a guard against pointing a compute at the wrong copied
 * datadir or store timeline.
 */
PG_FUNCTION_INFO_V1(pagestore_validate_branch_manifest);
Datum
pagestore_validate_branch_manifest(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		new_tl = PG_GETARG_INT32(1);
	int32		parent_tl = PG_GETARG_INT32(2);
	XLogRecPtr	fork_lsn = PG_GETARG_LSN(3);
	char	   *manifest;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to validate a branch manifest")));
	if (!pagestore_branch_backend_active())
		ereport(ERROR,
				(errmsg("pagestore.backend must be \"localsvc\" to validate a branch manifest")));
	if (new_tl <= 0 || parent_tl < 0)
		PG_RETURN_BOOL(false);

	manifest = pagestore_read_branch_manifest(target_dir);
	if (manifest == NULL)
		PG_RETURN_BOOL(false);
	PG_RETURN_BOOL(pagestore_manifest_matches(manifest, new_tl, parent_tl,
											  fork_lsn));
}

static void
pagestore_validate_datadir_branch_manifest(void)
{
	char	   *manifest;
	uint32_t	new_tl;
	uint32_t	parent_tl;
	XLogRecPtr	fork_lsn;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	if (DataDir == NULL)
		return;

	manifest = pagestore_read_branch_manifest(DataDir);
	if (manifest == NULL)
		return;					/* legacy/non-branch datadir */
	if (!pagestore_branch_backend_active())
		ereport(FATAL,
				(errmsg("pagestore.backend must be \"localsvc\" to validate a branch manifest")));
	if (!pagestore_manifest_get_branch_identity(manifest, &new_tl, &parent_tl,
												&fork_lsn))
		ereport(FATAL,
				(errmsg("invalid pagestore branch manifest in data directory")));
	if (new_tl != pagestore_localsvc_timeline())
		ereport(FATAL,
				(errmsg("pagestore.timeline does not match pagestore_branch.manifest"),
				 errdetail("Configured timeline is %u.", pagestore_localsvc_timeline())));
	pagestore_localsvc_require_branch_timeout(new_tl, parent_tl,
											  (uint64) fork_lsn, 5000);
	pagestore_localsvc_detach();
}

static void
pagestore_preflight_prepared_artifact(const char *prepared_dir,
									  const char *target_dir,
									  const char *relpath, bool required)
{
	char		src[MAXPGPATH];

	if (strlen(prepared_dir) + strlen(relpath) + sizeof("/") > MAXPGPATH ||
		strlen(target_dir) + strlen(relpath) + sizeof(".install/") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch install path is too long")));

	snprintf(src, sizeof(src), "%s/%s", prepared_dir, relpath);
	if (access(src, F_OK) != 0 && required)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("prepared branch artifact \"%s\" is missing: %m", src)));
}

static void
pagestore_install_prepared_dir(const char *prepared_dir, const char *target_dir,
							   const char *relpath, bool required)
{
	char		src[MAXPGPATH];
	char		dst[MAXPGPATH];
	char		stage[MAXPGPATH];

	if (strlen(prepared_dir) + strlen(relpath) + sizeof("/") > MAXPGPATH ||
		strlen(target_dir) + strlen(relpath) + sizeof(".install/") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch install path is too long")));

	snprintf(src, sizeof(src), "%s/%s", prepared_dir, relpath);
	snprintf(dst, sizeof(dst), "%s/%s", target_dir, relpath);
	snprintf(stage, sizeof(stage), "%s/%s.install", target_dir, relpath);
	if (access(src, F_OK) != 0)
	{
		if (required)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("prepared branch artifact \"%s\" is missing: %m", src)));
		return;
	}
	if (access(stage, F_OK) == 0 && !rmtree(stage, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear branch install staging dir \"%s\"", stage)));
	copydir(src, stage, true);
	fsync_fname(stage, true);
	if (access(dst, F_OK) == 0 && !rmtree(dst, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove existing branch artifact \"%s\"", dst)));
	if (rename(stage, dst) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not install branch artifact \"%s\": %m", dst)));
	fsync_fname(target_dir, true);
}

static void
pagestore_install_prepared_file(const char *prepared_dir, const char *target_dir,
								const char *relpath, bool required)
{
	char		src[MAXPGPATH];
	char		dst[MAXPGPATH];
	char		stage[MAXPGPATH];

	if (strlen(prepared_dir) + strlen(relpath) + sizeof("/") > MAXPGPATH ||
		strlen(target_dir) + strlen(relpath) + sizeof(".install/") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch install path is too long")));

	snprintf(src, sizeof(src), "%s/%s", prepared_dir, relpath);
	snprintf(dst, sizeof(dst), "%s/%s", target_dir, relpath);
	snprintf(stage, sizeof(stage), "%s/%s.install", target_dir, relpath);
	if (access(src, F_OK) != 0)
	{
		if (required)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("prepared branch artifact \"%s\" is missing: %m", src)));
		return;
	}
	if (access(stage, F_OK) == 0 && unlink(stage) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear branch install staging file \"%s\": %m", stage)));
	copy_file(src, stage);
	if (durable_rename(stage, dst, ERROR) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not install branch artifact \"%s\": %m", dst)));
	fsync_fname(target_dir, true);
}

/*
 * pagestore_install_prepared_branch(prepared_dir text, target_dir text,
 *                                  new_timeline int, parent_timeline int,
 *                                  fork_lsn pg_lsn)
 * returns void
 *
 * Install the artifacts produced by pagestore_prepare_branch() into an
 * initdb/copied branch datadir.  pg_xact and any prepared SLRUs are installed
 * before the manifest.  The prepared manifest must match the expected branch
 * identity before any artifact is installed, and the manifest is installed last
 * so its presence remains the startup-time signal that the datadir has a
 * prepared branch identity and must
 * pass timeline validation.
 */
PG_FUNCTION_INFO_V1(pagestore_install_prepared_branch);
Datum
pagestore_install_prepared_branch(PG_FUNCTION_ARGS)
{
	char	   *prepared_dir;
	char	   *target_dir;
	int32		new_tl;
	int32		parent_tl;
	XLogRecPtr	fork_lsn;
	char	   *manifest_data;
	char		stage[MAXPGPATH];

	if (PG_NARGS() != 5)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pagestore_install_prepared_branch requires 5 arguments (prepared_dir, target_dir, new_timeline, parent_timeline, fork_lsn)")));

	prepared_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	target_dir = text_to_cstring(PG_GETARG_TEXT_PP(1));
	new_tl = PG_GETARG_INT32(2);
	parent_tl = PG_GETARG_INT32(3);
	fork_lsn = PG_GETARG_LSN(4);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to install a prepared branch")));
	if (new_tl <= 0 || parent_tl < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid branch timeline identity")));
	manifest_data = pagestore_read_branch_manifest(prepared_dir);
	if (manifest_data == NULL)
		ereport(ERROR,
				(errmsg("prepared branch manifest is missing")));
	if (!pagestore_manifest_matches(manifest_data, new_tl, parent_tl, fork_lsn))
		ereport(ERROR,
				(errmsg("prepared branch manifest does not match the requested branch identity")));

	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_xact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_commit_ts", false);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_multixact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pagestore_branch.manifest", true);

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));
	snprintf(stage, sizeof(stage), "%s/pagestore_branch.manifest", target_dir);
	if (access(stage, F_OK) == 0 && unlink(stage) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear existing branch artifact \"%s\": %m", stage)));
	snprintf(stage, sizeof(stage), "%s/pagestore_branch.manifest.install", target_dir);
	if (access(stage, F_OK) == 0 && unlink(stage) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear existing branch artifact staging file \"%s\": %m", stage)));
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_xact", true);
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_commit_ts", false);
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_multixact", true);
	pagestore_install_prepared_file(prepared_dir, target_dir,
									"pagestore_branch.manifest", true);

	PG_RETURN_VOID();
}

/*
 * pagestore_prepare_branch(target_dir text, new_timeline int, parent_timeline int,
 *                          base pg_lsn, target pg_lsn,
 *                          oldest_xid xid, next_xid xid,
 *                          oldest_commit_ts_xid xid, next_commit_ts_xid xid,
 *                          oldest_multi xid, next_multi xid,
 *                          oldest_member bigint, next_member bigint)
 * returns bigint
 *
 * Control-plane bootstrap entrypoint for a branch-capable compute: materialize
 * branch SLRUs, fork the store timeline, and durably record the fork metadata in
 * the target datadir.  This is still intentionally pg_control-adjacent rather
 * than pg_control-editing: the manifest gives later bootstrap code one durable
 * protocol artifact to consume.
 */
PG_FUNCTION_INFO_V1(pagestore_prepare_branch);
Datum
pagestore_prepare_branch(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		new_tl = PG_GETARG_INT32(1);
	int32		parent_tl = PG_GETARG_INT32(2);
	XLogRecPtr	base = PG_GETARG_LSN(3);
	XLogRecPtr	target = PG_GETARG_LSN(4);
	TransactionId oldest_xid = PG_GETARG_TRANSACTIONID(5);
	TransactionId next_xid = PG_GETARG_TRANSACTIONID(6);
	TransactionId oldest_commit_ts_xid = PG_GETARG_TRANSACTIONID(7);
	TransactionId next_commit_ts_xid = PG_GETARG_TRANSACTIONID(8);
	MultiXactId oldest_multi = PG_GETARG_TRANSACTIONID(9);
	MultiXactId next_multi = PG_GETARG_TRANSACTIONID(10);
	int64		oldest_member = PG_GETARG_INT64(11);
	int64		next_member = PG_GETARG_INT64(12);
	int64		seeded;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to prepare a branch")));
	if (new_tl <= 0)
		ereport(ERROR,
				(errmsg("pagestore branch timeline must be > 0 (0 is the main timeline)")));
	if (parent_tl < 0)
		ereport(ERROR,
				(errmsg("pagestore parent timeline must be >= 0")));
	if (target < base)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	pagestore_localsvc_check_branch((uint32) new_tl, (uint32) parent_tl,
									(uint64) target);
	seeded = pagestore_seed_branch_slrus_impl(target_dir, base, target,
											  oldest_xid, next_xid,
											  oldest_commit_ts_xid,
											  next_commit_ts_xid,
											  oldest_multi, next_multi,
											  oldest_member, next_member);
	pagestore_localsvc_create_branch((uint32) new_tl, (uint32) parent_tl,
									 (uint64) target);
	pagestore_write_branch_manifest(target_dir, new_tl, parent_tl, base, target,
									oldest_xid, next_xid,
									oldest_commit_ts_xid, next_commit_ts_xid,
									oldest_multi, next_multi,
									oldest_member, next_member,
									seeded);

	PG_RETURN_INT64(seeded);
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

	DefineCustomBoolVariable("pagestore.redo_wal_from_store",
							 "Read redo_page_asof's WAL records from the store's shipped WAL, not local files.",
							 "Ancestor-timeline records always come from the store; this forces it for "
							 "all records, so a compute with no local WAL (a fresh branch) can replay deltas.",
							 &pagestore_redo_wal_from_store,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pagestore");

	/* register our smgr implementation and claim relations via the hook */
	pagestore_smgr_which = smgr_register(&pagestore_smgr);
	smgr_which_hook = pagestore_which;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pagestore_validate_datadir_branch_manifest;
}
