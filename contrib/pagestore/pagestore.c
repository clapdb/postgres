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
#include "access/xlog_internal.h"
#include "archive/archive_module.h"
#include "catalog/pg_tablespace_d.h"
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
