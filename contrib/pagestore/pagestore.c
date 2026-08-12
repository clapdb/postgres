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
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/rmgr.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "archive/archive_module.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/relpath.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/optimizer.h"
#include "pagestore_backend.h"
#include "pagestore_ipc.h"
#include "port/pg_iovec.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/aio.h"
#include "storage/bufpage.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/md.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/plancache.h"
#include "utils/rel.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/wait_classes.h"
#include "utils/wait_event.h"
#include "walredo_client.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
PGDLLEXPORT void pagestore_reader_snapshot_worker_main(Datum main_arg);
PGDLLEXPORT void pagestore_reader_artifact_launcher_main(Datum main_arg);
PGDLLEXPORT void pagestore_reader_artifact_database_main(Datum main_arg);
PGDLLEXPORT void pagestore_wal_index_worker_main(Datum main_arg);

/* GUC state */
static bool pagestore_route_all = false;
static bool pagestore_route_user_tablespaces = false;
static char *pagestore_backend_name = NULL;
static char *pagestore_walredo_datadir = NULL;
static bool pagestore_redo_wal_from_store = false;
static bool pagestore_advance_read_lsn = false;
static bool pagestore_auto_reader_artifacts = false;
static bool pagestore_auto_wal_index = false;
static bool pagestore_materializer = false;
static char *pagestore_retention_owner_id_str = NULL;
static char *pagestore_retention_owner_generation_str = NULL;
static uint64 pagestore_retention_owner_id = 0;
static uint32 pagestore_retention_owner_generation = 0;
static int pagestore_materializer_max_lag_mb = 0;
static int pagestore_wal_index_max_lag_mb = 0;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static get_snapshot_data_hook_type prev_get_snapshot_data_hook = NULL;
static transaction_id_is_in_progress_hook_type prev_xid_in_progress_hook = NULL;
static buffer_tag_read_epoch_hook_type prev_buffer_tag_read_epoch_hook = NULL;
static xact_start_hook_type prev_xact_start_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorRun_hook_type prev_executor_run_hook = NULL;
static recovery_start_hook_type prev_recovery_start_hook = NULL;
static recovery_restartpoint_flush_hook_type prev_restartpoint_flush_hook = NULL;

#define PS_MATERIALIZER_MARKER_MAGIC		0x50534d57
#define PS_MATERIALIZER_MARKER_VERSION	2
#define PS_MATERIALIZER_MARKER_BLOCK		3
#define PS_MATERIALIZER_RELEASE_MAGIC		0x50534d52
#define PS_MATERIALIZER_RELEASE_VERSION	2
#define PS_MATERIALIZER_RELEASE_BLOCK		4
#define PS_MATERIALIZER_MARKER_TIMEOUT_MS 10000
#define PS_MATERIALIZER_RETENTION_RESOURCES \
	(PS_RETENTION_RESOURCE_WAL | PS_RETENTION_RESOURCE_WAL_INDEX)
#define PS_READER_RETENTION_RESOURCES PS_RETENTION_RESOURCE_ALL
#define PS_READER_RETENTION_TIMEOUT_MS 10000

typedef struct PsMaterializerMarker
{
	uint32		magic;
	uint32		version;
	uint32		timeline;
	uint32		pad;
	uint64		materialized_lsn;
	uint64		materialized_lsn_complement;
} PsMaterializerMarker;

typedef struct PsMaterializerRelease
{
	uint32		magic;
	uint32		version;
	uint32		timeline;
	uint32		pad;
	uint64		materialized_lsn;
	uint64		materialized_lsn_complement;
	uint64		checkpoint_lsn;
	uint64		checkpoint_lsn_complement;
} PsMaterializerRelease;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
static CmdType pagestore_current_command_type = CMD_UNKNOWN;
static bool pagestore_current_has_modifying_cte = false;
static PlannedStmt *pagestore_current_planned_stmt = NULL;
static bool pagestore_handoff_issued = false;
static int pagestore_utility_nesting_level = 0;
static commit_ts_bounds_hook_type prev_commit_ts_bounds_hook = NULL;
static commit_ts_latest_hook_type prev_commit_ts_latest_hook = NULL;
static snapshot_transfer_hook_type prev_snapshot_transfer_hook = NULL;
static post_database_path_hook_type prev_post_database_path_hook = NULL;
static bool pagestore_commit_ts_bounds_valid = false;
static bool pagestore_commit_ts_active = false;
static TransactionId pagestore_oldest_commit_ts_xid = InvalidTransactionId;
static TransactionId pagestore_newest_commit_ts_xid = InvalidTransactionId;

static bool
pagestore_commit_ts_latest(TransactionId *xid, TimestampTz *ts,
						   ReplOriginId *nodeid)
{
	if (pagestore_commit_ts_bounds_valid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_last_committed_xact() is not supported on an advancing pagestore reader")));
	return prev_commit_ts_latest_hook != NULL &&
		prev_commit_ts_latest_hook(xid, ts, nodeid);
}

static void
pagestore_snapshot_transfer(bool is_export)
{
	if (pagestore_advance_read_lsn && pagestore_localsvc_read_lsn() != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot %s a snapshot on an advancing pagestore reader",
						is_export ? "export" : "import")));
	if (prev_snapshot_transfer_hook != NULL)
		prev_snapshot_transfer_hook(is_export);
}

/* "which" index assigned to our smgr implementation by smgr_register() */
static int	pagestore_smgr_which = -1;
static int pagestore_which(RelFileLocator rlocator, ProcNumber backend);
static void pagestore_refresh_reader_horizon(void);
static bool pagestore_branch_backend_active(void);

static bool
pagestore_commit_ts_bounds(bool *active, TransactionId *oldest_xid,
						   TransactionId *newest_xid)
{
	if (pagestore_commit_ts_bounds_valid)
	{
		*active = pagestore_commit_ts_active;
		*oldest_xid = pagestore_oldest_commit_ts_xid;
		*newest_xid = pagestore_newest_commit_ts_xid;
		return true;
	}
	return prev_commit_ts_bounds_hook != NULL &&
		prev_commit_ts_bounds_hook(active, oldest_xid, newest_xid);
}

static PlannedStmt *
pagestore_planner(Query *parse, const char *query_string, int cursor_options,
				  ParamListInfo bound_params, ExplainState *es)
{
	PlannedStmt *result;
	int			saved_max_parallel_workers_per_gather;
	int			saved_debug_parallel_query;

	if (!pagestore_advance_read_lsn)
		return prev_planner_hook != NULL ?
			prev_planner_hook(parse, query_string, cursor_options, bound_params, es) :
			standard_planner(parse, query_string, cursor_options, bound_params, es);

	saved_max_parallel_workers_per_gather = max_parallel_workers_per_gather;
	saved_debug_parallel_query = debug_parallel_query;
	max_parallel_workers_per_gather = 0;
	debug_parallel_query = DEBUG_PARALLEL_OFF;
	PG_TRY();
	{
		result = prev_planner_hook != NULL ?
			prev_planner_hook(parse, query_string, cursor_options, bound_params, es) :
			standard_planner(parse, query_string, cursor_options, bound_params, es);
	}
	PG_FINALLY();
	{
		max_parallel_workers_per_gather =
			saved_max_parallel_workers_per_gather;
		debug_parallel_query = saved_debug_parallel_query;
	}
	PG_END_TRY();
	return result;
}

static void
pagestore_executor_run(QueryDesc *query_desc, ScanDirection direction,
					   uint64 count)
{
	CmdType		saved_command_type = pagestore_current_command_type;
	bool		saved_has_modifying_cte = pagestore_current_has_modifying_cte;
	PlannedStmt *saved_planned_stmt = pagestore_current_planned_stmt;

	if (pagestore_handoff_issued)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("a writer backend cannot execute queries after issuing a reader handoff token")));
	pagestore_current_command_type = query_desc->plannedstmt->commandType;
	pagestore_current_has_modifying_cte =
		query_desc->plannedstmt->hasModifyingCTE;
	pagestore_current_planned_stmt = query_desc->plannedstmt;
	PG_TRY();
	{
		if (prev_executor_run_hook != NULL)
			prev_executor_run_hook(query_desc, direction, count);
		else
			standard_ExecutorRun(query_desc, direction, count);
	}
	PG_FINALLY();
	{
		pagestore_current_command_type = saved_command_type;
		pagestore_current_has_modifying_cte = saved_has_modifying_cte;
		pagestore_current_planned_stmt = saved_planned_stmt;
	}
	PG_END_TRY();
}

static void
pagestore_process_utility(PlannedStmt *pstmt, const char *query_string,
					  bool read_only_tree, ProcessUtilityContext context,
					  ParamListInfo params, QueryEnvironment *query_env,
					  DestReceiver *dest, QueryCompletion *qc)
{
	bool		allow_after_handoff = false;

	if (IsA(pstmt->utilityStmt, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *) pstmt->utilityStmt;

		switch (stmt->kind)
		{
			case TRANS_STMT_BEGIN:
			case TRANS_STMT_START:
			case TRANS_STMT_COMMIT:
			case TRANS_STMT_ROLLBACK:
			case TRANS_STMT_SAVEPOINT:
			case TRANS_STMT_RELEASE:
			case TRANS_STMT_ROLLBACK_TO:
				allow_after_handoff = true;
				break;
			default:
				break;
		}
	}
	if (pagestore_handoff_issued && !allow_after_handoff)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("a writer backend cannot run utility commands after issuing a reader handoff token")));
	pagestore_utility_nesting_level++;
	PG_TRY();
	{
		if (prev_process_utility_hook != NULL)
			prev_process_utility_hook(pstmt, query_string, read_only_tree, context,
								  params, query_env, dest, qc);
		else
			standard_ProcessUtility(pstmt, query_string, read_only_tree, context,
								params, query_env, dest, qc);
	}
	PG_FINALLY();
	{
		pagestore_utility_nesting_level--;
	}
	PG_END_TRY();
}

#define PAGESTORE_READER_HORIZON_TTL_MS 1000
#define PAGESTORE_READER_HORIZON_TIMEOUT_MS 10000
#define PAGESTORE_READER_HORIZON_LEASE_MS \
	(3 * PAGESTORE_READER_HORIZON_TIMEOUT_MS)

typedef struct PagestoreReaderHorizonShmem
{
	slock_t		mutex;
	XLogRecPtr	candidate_lsn;
	uint32		candidate_generation;
	XLogRecPtr	adoptable_lsn;
	uint32		adoptable_generation;
	TimestampTz refreshed_at;
	int			refresh_owner_pid;
	TimestampTz refresh_started_at;
} PagestoreReaderHorizonShmem;

typedef struct PagestoreReaderSnapshotJobShmem
{
	slock_t		mutex;
	ControlFileData control;
	uint64		generation;
} PagestoreReaderSnapshotJobShmem;

static PagestoreReaderHorizonShmem *pagestore_reader_horizon = NULL;
static PagestoreReaderSnapshotJobShmem *pagestore_reader_snapshot_job = NULL;
static bool pagestore_reader_advance_lock_held = false;

static void
pagestore_reader_advance_locktag(LOCKTAG *tag)
{
	SET_LOCKTAG_PAGESTORE_READER(*tag, pagestore_localsvc_timeline(),
								 (uint32) (pagestore_retention_owner_id >> 32),
								 (uint32) pagestore_retention_owner_id);
}

static void
pagestore_shmem_request(void)
{
	if (prev_shmem_request_hook != NULL)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(MAXALIGN(sizeof(PagestoreReaderHorizonShmem)));
	RequestAddinShmemSpace(MAXALIGN(sizeof(PagestoreReaderSnapshotJobShmem)));
}

static bool
pagestore_buffer_tag_read_epoch(RelFileLocatorBackend rlocator,
								uint32 *read_epoch)
{
	if (pagestore_which(rlocator.locator, rlocator.backend) ==
		pagestore_smgr_which)
	{
		*read_epoch = pagestore_localsvc_read_epoch();
		return true;
	}
	return prev_buffer_tag_read_epoch_hook != NULL ?
		prev_buffer_tag_read_epoch_hook(rlocator, read_epoch) : false;
}

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

static bool
ps_prefetch_supported(SMgrRelation reln)
{
	return ACTIVE()->uses_local_files;
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
	.smgr_prefetch_supported = ps_prefetch_supported,
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

static bool
pagestore_branch_routing_active(void)
{
	return pagestore_route_all;
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

static bool
check_retention_owner_id(char **newval, void **extra, GucSource source)
{
	char	   *end;
	unsigned long long parsed;

	if (*newval == NULL || **newval == '\0')
		return true;
	if (**newval < '0' || **newval > '9')
		goto invalid;
	errno = 0;
	parsed = strtoull(*newval, &end, 10);
	if (errno == 0 && *end == '\0' && parsed != 0)
		return true;
invalid:
	GUC_check_errdetail("Expected a nonzero unsigned 64-bit decimal integer.");
	return false;
}

static void
assign_retention_owner_id(const char *newval, void *extra)
{
	pagestore_retention_owner_id =
		(newval != NULL && *newval != '\0') ? (uint64) strtoull(newval, NULL, 10) : 0;
}

static bool
check_retention_owner_generation(char **newval, void **extra, GucSource source)
{
	char	   *end;
	unsigned long long parsed;

	if (*newval == NULL || **newval == '\0')
		return true;
	if (**newval < '0' || **newval > '9')
		goto invalid;
	errno = 0;
	parsed = strtoull(*newval, &end, 10);
	if (errno == 0 && *end == '\0' && parsed > 0 && parsed <= UINT32_MAX)
		return true;
invalid:
	GUC_check_errdetail("Expected an unsigned decimal integer from 1 through %u.",
						UINT32_MAX);
	return false;
}

static void
assign_retention_owner_generation(const char *newval, void *extra)
{
	pagestore_retention_owner_generation =
		(newval != NULL && *newval != '\0') ?
		(uint32) strtoull(newval, NULL, 10) : 0;
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

/* SQL-callable protocol probes used by integration tests and controller bringup. */
PG_FUNCTION_INFO_V1(pagestore_retention_set);

Datum
pagestore_retention_set(PG_FUNCTION_ARGS)
{
	int32		timeline = PG_GETARG_INT32(0);
	int32		owner_kind = PG_GETARG_INT32(1);
	int64		owner_id = PG_GETARG_INT64(2);
	int64		generation = PG_GETARG_INT64(3);
	int32		resources = PG_GETARG_INT32(4);
	XLogRecPtr	lsn = PG_GETARG_LSN(5);
	uint64		admission_seq;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to manage pagestore retention owners")));

	if (pagestore_backend_name == NULL ||
		strcmp(pagestore_backend_name, "localsvc") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore retention operations require the localsvc backend")));
	/* SQL bigint supplies the uint64 bit pattern.  Negative values therefore
	 * represent owner IDs with the high bit set; only the all-zero ID is
	 * reserved. */
	if (timeline < 0 || owner_kind < 0 || owner_id == 0 || generation <= 0 ||
		(uint64) generation > UINT32_MAX || resources < 0)
		ereport(ERROR,
				(errmsg("pagestore retention owner fields are invalid or out of range")));
	admission_seq = PG_NARGS() > 6 ?
		(uint64) PG_GETARG_INT64(6) :
		pagestore_localsvc_admission_barrier_timeout(30000);
	PG_RETURN_INT32((int32) pagestore_localsvc_retention_set(
		(uint32) timeline, (uint32) owner_kind, (uint64) owner_id,
		(uint32) generation, (uint32) resources, (uint64) lsn,
		admission_seq));
}

PG_FUNCTION_INFO_V1(pagestore_retention_drop);

Datum
pagestore_retention_drop(PG_FUNCTION_ARGS)
{
	int32		timeline = PG_GETARG_INT32(0);
	int32		owner_kind = PG_GETARG_INT32(1);
	int64		owner_id = PG_GETARG_INT64(2);
	int64		generation = PG_GETARG_INT64(3);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to manage pagestore retention owners")));

	if (pagestore_backend_name == NULL ||
		strcmp(pagestore_backend_name, "localsvc") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore retention operations require the localsvc backend")));
	if (timeline < 0 || owner_kind < 0 || owner_id == 0 || generation <= 0 ||
		(uint64) generation > UINT32_MAX)
		ereport(ERROR,
				(errmsg("pagestore retention owner fields are invalid or out of range")));
	PG_RETURN_INT32((int32) pagestore_localsvc_retention_drop(
		(uint32) timeline, (uint32) owner_kind, (uint64) owner_id,
		(uint32) generation));
}

PG_FUNCTION_INFO_V1(pagestore_retention_owner_lsn);

static bool
pagestore_find_retention_owner(uint32 timeline, uint32 owner_kind,
							   uint64 owner_id, PsRetentionPin *result)
{
	bool		found = false;

	if (pagestore_localsvc_retention_lookup(timeline, owner_kind, owner_id,
			result, &found) != PS_STATUS_OK)
		ereport(ERROR,
				(errmsg("pagestore retention owner lookup failed")));
	return found;
}

Datum
pagestore_retention_owner_lsn(PG_FUNCTION_ARGS)
{
	int32		timeline = PG_GETARG_INT32(0);
	int32		owner_kind = PG_GETARG_INT32(1);
	int64		owner_id = PG_GETARG_INT64(2);
	int64		generation = PG_GETARG_INT64(3);
	PsRetentionPin pin;

	if (timeline < 0 || owner_kind <= 0 || owner_id == 0 || generation <= 0 ||
		(uint64) generation > UINT32_MAX)
		ereport(ERROR,
				(errmsg("pagestore retention owner fields must be positive and in range")));
	if (pagestore_find_retention_owner((uint32) timeline,
			(uint32) owner_kind, (uint64) owner_id, &pin) &&
		pin.generation == (uint32) generation)
		PG_RETURN_LSN((XLogRecPtr) pin.lsn);
	PG_RETURN_NULL();
}

/*
 * pagestore_create_branch(new_timeline int, parent_timeline int, lsn pg_lsn)
 *
 * Create a copy-on-write branch: a new timeline forked from parent_timeline at
 * the given LSN.  Instant -- no page data is copied; the branch shares the
 * parent's pages until it writes.  This only creates the store-side timeline;
 * booting a compute on a branch timeline must use the prepared branch
 * manifest/install flow so local SLRUs match the fork point.
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
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		return false;

	/*
	 * A pinned reader never ships WAL.  The supported configuration is
	 * archive_mode = off (enforced at startup); this is the belt for the
	 * unreachable case.
	 */
	return pagestore_localsvc_read_lsn() == 0;
}

/* Index only WAL that the daemon has durably accepted. */
static int
pagestore_index_wal_page_read(XLogReaderState *state,
							 XLogRecPtr targetPagePtr, int reqLen,
							 XLogRecPtr targetRecPtr, char *readBuf)
{
	int			n = pagestore_localsvc_wal_read(pagestore_localsvc_timeline(),
											(uint64) targetPagePtr,
											XLOG_BLCKSZ, readBuf);

	(void) state;
	(void) targetRecPtr;
	return n >= reqLen ? n : -1;
}

#define PAGESTORE_WAL_INDEX_BATCH_MAX 4096

/*
 * Decode WAL in [start, end) and record, for each block a record modifies, an
 * entry in the store's per-page WAL index.  The worker reads the durable store;
 * the test-only SQL wrapper can still inspect the current local WAL tail.
 */
static XLogRecPtr
pagestore_index_wal_range(XLogRecPtr start, XLogRecPtr end, bool from_store)
{
	XLogReaderState *volatile reader = NULL;
	ReadLocalXLogPageNoWaitPrivate *volatile pd = NULL;
	XLogRecPtr	first;
	XLogRecPtr	indexed_end = start;
	PageStoreWalIndexEntry *volatile batch = NULL;
	int			nbatch = 0;

	PG_TRY();
	{
		batch = palloc(sizeof(*batch) * PAGESTORE_WAL_INDEX_BATCH_MAX);
		if (from_store)
			reader = XLogReaderAllocate(wal_segment_size, NULL,
									 XL_ROUTINE(.page_read = &pagestore_index_wal_page_read),
									 NULL);
		else
		{
			pd = palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
			reader = XLogReaderAllocate(wal_segment_size, NULL,
									 XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
												.segment_open = &wal_segment_open,
												.segment_close = &wal_segment_close),
									 (void *) pd);
		}
		if (reader != NULL)
		{
			{
				char	   *ferrm;

				first = XLogFindNextRecord((XLogReaderState *) reader, start, &ferrm);
			}
			while (!XLogRecPtrIsInvalid(first))
			{
				char	   *errm;
				XLogRecord *rec = XLogReadRecord((XLogReaderState *) reader, &errm);

				if (rec == NULL)
					break;				/* end of shipped WAL / torn tail */
				if (reader->ReadRecPtr >= end)
					break;
				if (reader->EndRecPtr > end)
					break;				/* record crosses shipped boundary */

				for (int b = 0;
					 b <= XLogRecMaxBlockId((XLogReaderState *) reader); b++)
				{
					RelFileLocator rloc;
					ForkNumber	fk;
					BlockNumber blk;
					PageStoreRelKey key;

					if (!XLogRecHasBlockRef((XLogReaderState *) reader, b))
						continue;
					XLogRecGetBlockTagExtended((XLogReaderState *) reader, b,
										   &rloc, &fk, &blk, NULL);
					key.spcOid = rloc.spcOid;
					key.dbOid = rloc.dbOid;
					key.relNumber = rloc.relNumber;
					key.forkNum = fk;
					batch[nbatch].key = key;
					batch[nbatch].block = blk;
					batch[nbatch].lsn = reader->ReadRecPtr;
					nbatch++;
					if (nbatch == PAGESTORE_WAL_INDEX_BATCH_MAX)
					{
						pagestore_localsvc_walidx_add_batch(
							(PageStoreWalIndexEntry *) batch, nbatch);
						nbatch = 0;
					}
				}
				indexed_end = reader->EndRecPtr;
			}
			if (nbatch > 0)
				pagestore_localsvc_walidx_add_batch(
					(PageStoreWalIndexEntry *) batch, nbatch);
		}
	}
	PG_FINALLY();
	{
		if (reader != NULL)
			XLogReaderFree((XLogReaderState *) reader);
		if (pd != NULL)
			pfree((ReadLocalXLogPageNoWaitPrivate *) pd);
		if (batch != NULL)
			pfree((PageStoreWalIndexEntry *) batch);
	}
	PG_END_TRY();
	return indexed_end;
}

void
pagestore_wal_index_worker_main(Datum main_arg)
{
	MemoryContext work_context;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();
	work_context = AllocSetContextCreate(TopMemoryContext,
									  "pagestore WAL index worker",
									  ALLOCSET_DEFAULT_SIZES);

	while (!ShutdownRequestPending)
	{
		bool		advanced = false;
		MemoryContext old_context;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		old_context = MemoryContextSwitchTo(work_context);
		PG_TRY();
		{
			XLogRecPtr start = pagestore_localsvc_walidx_progress();
			XLogRecPtr shipped_end = pagestore_localsvc_wal_end();
			XLogRecPtr scan_start = start;
			uint32		parent_timeline;
			uint64		branch_lsn;

			if (!XLogRecPtrIsInvalid(start) && shipped_end > start)
			{
				XLogRecPtr indexed_end;

				/* A branch's first segment contains a copied parent prefix. */
				if (pagestore_localsvc_timeline_parent(
						pagestore_localsvc_timeline(), &parent_timeline,
						&branch_lsn) && scan_start < (XLogRecPtr) branch_lsn)
					scan_start = (XLogRecPtr) branch_lsn;
				if (scan_start >= shipped_end)
					indexed_end = shipped_end;
				else
					indexed_end = pagestore_index_wal_range(scan_start,
														 shipped_end, true);

				if (indexed_end > start)
				{
					pagestore_localsvc_walidx_commit(start, indexed_end);
					advanced = true;
				}
			}
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			MemoryContext error_context = MemoryContextSwitchTo(TopMemoryContext);

			edata = CopyErrorData();
			MemoryContextSwitchTo(error_context);
			if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
				edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
			{
				FreeErrorData(edata);
				PG_RE_THROW();
			}
			FlushErrorState();
			ereport(WARNING,
					(errmsg("pagestore WAL index worker failed: %s",
							edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();
		MemoryContextSwitchTo(old_context);
		MemoryContextReset(work_context);
		if (advanced)
			continue;
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	proc_exit(0);
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
	XLogRecPtr	indexed_end = pagestore_index_wal_range(start, end, false);

	/* Only a contiguous scan may advance the durable resume boundary. */
	if (pagestore_localsvc_walidx_progress() == start && indexed_end > start)
		pagestore_localsvc_walidx_commit(start, indexed_end);
	PG_RETURN_VOID();
}

/*
 * End of the durable WAL prefix available to this timeline.  A new branch may
 * have no child-local WAL yet, but its inherited history is complete through
 * the fork LSN and is served by the ancestry-aware WAL read path.
 */
static XLogRecPtr
pagestore_materializer_shipped_lsn(int timeout_ms)
{
	uint64		shipped = pagestore_localsvc_wal_end_timeout(timeout_ms);
	uint32		parent_timeline;
	uint64		branch_lsn;

	if (pagestore_localsvc_timeline_parent_timeout(
			pagestore_localsvc_timeline(), &parent_timeline, &branch_lsn,
			timeout_ms) &&
		shipped < branch_lsn)
		shipped = branch_lsn;
	return (XLogRecPtr) shipped;
}

/* Fence this recovery process before it can consume the first WAL record. */
static void
pagestore_materializer_recovery_start(XLogRecPtr redo_lsn)
{
	uint8		status;
	uint64		admission_seq;

	if (pagestore_materializer)
	{
		if (XLogRecPtrIsInvalid(redo_lsn) ||
			pagestore_retention_owner_id == 0 ||
			pagestore_retention_owner_generation == 0)
				ereport(FATAL,
						(errmsg("pagestore materializer has no valid retention owner authority")));
		admission_seq = pagestore_localsvc_admission_barrier_timeout(
			PS_MATERIALIZER_MARKER_TIMEOUT_MS);
		if (admission_seq == 0)
			ereport(FATAL,
					(errmsg("pagestore materializer could not establish an admission fence")));
		status = pagestore_localsvc_retention_set_timeout(
			pagestore_localsvc_timeline(), PS_RETENTION_OWNER_MATERIALIZER,
			pagestore_retention_owner_id,
			pagestore_retention_owner_generation,
			PS_MATERIALIZER_RETENTION_RESOURCES, (uint64) redo_lsn,
			admission_seq,
			PS_MATERIALIZER_MARKER_TIMEOUT_MS);
		if (status == PS_STATUS_STALE)
			ereport(FATAL,
					(errmsg("pagestore materializer retention generation is stale"),
					 errdetail("Owner %llu generation %u was fenced by a newer controller generation.",
							   (unsigned long long) pagestore_retention_owner_id,
							   pagestore_retention_owner_generation)));
		if (status != PS_STATUS_OK)
			ereport(FATAL,
					(errmsg("pagestore materializer could not register its retention owner")));
	}

	if (prev_recovery_start_hook)
		(*prev_recovery_start_hook) (redo_lsn);
}

/* The durable marker may move first; a failed pin update simply retains the
 * older, conservative floor.  A stale result means this worker has lost
 * authority and must stop before applying more WAL. */
static void
pagestore_materializer_retention_advance(XLogRecPtr replay_lsn)
{
	uint8		status = PS_STATUS_ERROR;
	bool		failed = false;

	PG_TRY();
	{
		uint64		admission_seq;

		admission_seq = pagestore_localsvc_admission_barrier_timeout(
			PS_MATERIALIZER_MARKER_TIMEOUT_MS);
		if (admission_seq == 0)
			ereport(ERROR,
					(errmsg("pagestore materializer could not establish an admission fence")));
		status = pagestore_localsvc_retention_set_timeout(
			pagestore_localsvc_timeline(), PS_RETENTION_OWNER_MATERIALIZER,
			pagestore_retention_owner_id,
			pagestore_retention_owner_generation,
			PS_MATERIALIZER_RETENTION_RESOURCES, (uint64) replay_lsn,
			admission_seq,
			PS_MATERIALIZER_MARKER_TIMEOUT_MS);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

		edata = CopyErrorData();
		MemoryContextSwitchTo(old_context);
		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore materializer retention advance failed: %s",
						edata->message)));
		FreeErrorData(edata);
		failed = true;
	}
	PG_END_TRY();
	if (failed)
		return;

	if (status == PS_STATUS_STALE)
		ereport(FATAL,
				(errmsg("pagestore materializer lost retention owner authority"),
				 errdetail("Owner %llu generation %u was fenced by a newer controller generation.",
						   (unsigned long long) pagestore_retention_owner_id,
						   pagestore_retention_owner_generation)));
	if (status != PS_STATUS_OK)
		ereport(WARNING,
				(errmsg("pagestore materializer retention advance was rejected by the daemon")));
}

/* Publish a durable marker only after a restartpoint flushed relation pages. */
static void
pagestore_materializer_restartpoint_flush(XLogRecPtr replay_lsn,
											XLogRecPtr restart_redo_lsn)
{
	PageStoreRelKey key = {0};
	PsMaterializerMarker marker;
	char		page[BLCKSZ];
	bool		published = false;

	if (prev_restartpoint_flush_hook)
		(*prev_restartpoint_flush_hook) (replay_lsn, restart_redo_lsn);
	if (!RecoveryInProgress() || XLogRecPtrIsInvalid(replay_lsn))
		return;

	memset(&marker, 0, sizeof(marker));
	marker.magic = PS_MATERIALIZER_MARKER_MAGIC;
	marker.version = PS_MATERIALIZER_MARKER_VERSION;
	marker.timeline = pagestore_localsvc_timeline();
	marker.materialized_lsn = (uint64) replay_lsn;
	marker.materialized_lsn_complement = ~marker.materialized_lsn;
	memset(page, 0, sizeof(page));
	memcpy(page, &marker, sizeof(marker));

	/*
	 * A store outage must leave the old marker in place, not fail recovery.
	 * CheckPointGuts has issued the relation writes, but remote smgr sync is a
	 * no-op.  Persist those pages before making the new marker readable, then
	 * persist the marker itself.  A crash between marker write and its sync can
	 * only regress monitoring to the previous conservative boundary.
	 */
	PG_TRY();
	{
		pagestore_localsvc_store_sync_timeout(
			PS_MATERIALIZER_MARKER_TIMEOUT_MS);
		pagestore_localsvc_obj_write_timeout(PS_KLASS_CONTROL, &key,
										PS_MATERIALIZER_MARKER_BLOCK, page,
										(uint64) replay_lsn,
										PS_MATERIALIZER_MARKER_TIMEOUT_MS);
		pagestore_localsvc_store_sync_timeout(
			PS_MATERIALIZER_MARKER_TIMEOUT_MS);
		published = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

		edata = CopyErrorData();
		MemoryContextSwitchTo(old_context);
		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore materializer watermark publish failed: %s",
						edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
	if (published && !XLogRecPtrIsInvalid(restart_redo_lsn))
		pagestore_materializer_retention_advance(restart_redo_lsn);
}

static XLogRecPtr
pagestore_materialized_wal_lsn_internal(void)
{
	PageStoreRelKey key = {0};
	PsMaterializerMarker marker;
	char		page[BLCKSZ];

	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_CONTROL, &key,
													PS_MATERIALIZER_MARKER_BLOCK,
													UINT64_MAX, page, NULL,
													PS_MATERIALIZER_MARKER_TIMEOUT_MS))
		return InvalidXLogRecPtr;
	memcpy(&marker, page, sizeof(marker));
	if (marker.magic != PS_MATERIALIZER_MARKER_MAGIC ||
		marker.version != PS_MATERIALIZER_MARKER_VERSION ||
		marker.timeline != pagestore_localsvc_timeline() ||
		marker.materialized_lsn_complement != ~marker.materialized_lsn)
		return InvalidXLogRecPtr;
	return (XLogRecPtr) marker.materialized_lsn;
}

/* Read the durable release checkpoint only when it matches this marker. */
static XLogRecPtr
pagestore_materializer_release_checkpoint_read(XLogRecPtr materialized)
{
	PageStoreRelKey key = {0};
	PsMaterializerRelease release;
	uint64		marker;
	char		page[BLCKSZ];

	if (XLogRecPtrIsInvalid(materialized))
		return InvalidXLogRecPtr;
	marker = (uint64) materialized;

	if (pagestore_localsvc_obj_read_at_timeout(
			PS_KLASS_CONTROL, &key, PS_MATERIALIZER_RELEASE_BLOCK,
			UINT64_MAX, page, NULL, PS_MATERIALIZER_MARKER_TIMEOUT_MS))
	{
		memcpy(&release, page, sizeof(release));
		if (release.magic == PS_MATERIALIZER_RELEASE_MAGIC &&
			release.version == PS_MATERIALIZER_RELEASE_VERSION &&
			release.timeline == pagestore_localsvc_timeline() &&
			release.materialized_lsn_complement ==
			~release.materialized_lsn &&
			release.checkpoint_lsn_complement == ~release.checkpoint_lsn &&
			release.materialized_lsn == marker &&
			release.checkpoint_lsn > marker)
			return (XLogRecPtr) release.checkpoint_lsn;
	}
	return InvalidXLogRecPtr;
}

/*
 * Pin the first completed checkpoint observed for one materialized watermark.
 * Persisting the choice prevents repeated archive retries (or an archiver
 * restart) from chasing newer checkpoints forever while the materializer is
 * stalled.  A newer marker invalidates the old choice naturally.
 */
static XLogRecPtr
pagestore_materializer_release_checkpoint(XLogRecPtr materialized)
{
	PageStoreRelKey key = {0};
	PsMaterializerRelease release;
	XLogRecPtr	checkpoint;
	XLogRecPtr	latched;
	uint64		marker;
	char		page[BLCKSZ];

	/* Provisioning establishes the first marker before enabling the limiter. */
	if (XLogRecPtrIsInvalid(materialized))
		return InvalidXLogRecPtr;
	marker = (uint64) materialized;
	latched = pagestore_materializer_release_checkpoint_read(materialized);
	if (XLogRecPtrIsValid(latched))
		return latched;

	checkpoint = pagestore_control_writer_checkpoint_lsn_timeout(
		PS_MATERIALIZER_MARKER_TIMEOUT_MS);
	if (XLogRecPtrIsInvalid(checkpoint) || (uint64) checkpoint <= marker)
		return InvalidXLogRecPtr;

	memset(&release, 0, sizeof(release));
	release.magic = PS_MATERIALIZER_RELEASE_MAGIC;
	release.version = PS_MATERIALIZER_RELEASE_VERSION;
	release.timeline = pagestore_localsvc_timeline();
	release.materialized_lsn = marker;
	release.materialized_lsn_complement = ~marker;
	release.checkpoint_lsn = (uint64) checkpoint;
	release.checkpoint_lsn_complement = ~release.checkpoint_lsn;
	memset(page, 0, sizeof(page));
	memcpy(page, &release, sizeof(release));
	pagestore_localsvc_obj_write_timeout(
		PS_KLASS_CONTROL, &key, PS_MATERIALIZER_RELEASE_BLOCK, page,
		(uint64) checkpoint, PS_MATERIALIZER_MARKER_TIMEOUT_MS);
	pagestore_localsvc_store_sync_timeout(PS_MATERIALIZER_MARKER_TIMEOUT_MS);
	return checkpoint;
}

static uint64
pagestore_wal_align_up(uint64 lsn)
{
	uint64		remainder = lsn % wal_segment_size;
	uint64		advance;

	if (remainder == 0)
		return lsn;
	advance = wal_segment_size - remainder;
	return lsn > UINT64_MAX - advance ? UINT64_MAX : lsn + advance;
}

/* End of the nth complete segment after the segment containing lsn. */
static uint64
pagestore_wal_segments_after(uint64 lsn, uint64 nsegments)
{
	uint64		segment_start = lsn - lsn % wal_segment_size;
	uint64		advance = nsegments * (uint64) wal_segment_size;

	return segment_start > UINT64_MAX - advance ?
		UINT64_MAX : segment_start + advance;
}

/*
 * Compute an exclusive, segment-aligned shipping boundary.  Besides the
 * configured lag, always finish the materializer's next input segment and the
 * segment after the latched release checkpoint's start (the checkpoint record
 * itself may cross a segment boundary).  Thus recovery can consume a complete
 * segment containing a checkpoint and publish a newer restartpoint.
 */
static uint64
pagestore_materializer_archive_limit(XLogRecPtr marker,
									 XLogRecPtr checkpoint,
									 XLogRecPtr seg_start,
									 uint64 shipped_end)
{
	uint64		limit = 0;

	if (!XLogRecPtrIsInvalid(marker))
	{
		uint64		max_lag =
			(uint64) pagestore_materializer_max_lag_mb * 1024 * 1024;
		uint64		lag_end = (uint64) marker > UINT64_MAX - max_lag ?
			UINT64_MAX : (uint64) marker + max_lag;

		limit = pagestore_wal_align_up(lag_end);
		limit = Max(limit,
					pagestore_wal_segments_after((uint64) marker, 2));
	}
	else if (shipped_end == 0 ||
			 (shipped_end > (uint64) seg_start &&
			  shipped_end < (uint64) seg_start + wal_segment_size))
	{
		/* Complete a fresh or interrupted bootstrap segment. */
		limit = (uint64) seg_start + wal_segment_size;
	}

	if (!XLogRecPtrIsInvalid(checkpoint))
		limit = Max(limit,
					pagestore_wal_segments_after((uint64) checkpoint, 2));
	if (shipped_end % wal_segment_size != 0)
		limit = Max(limit, pagestore_wal_align_up(shipped_end));
	return limit;
}

static void
pagestore_require_localsvc_monitoring(void)
{
	if (pagestore_backend_name == NULL ||
		strcmp(pagestore_backend_name, "localsvc") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore WAL monitoring requires the localsvc backend")));
}

static void
pagestore_require_materializer(void)
{
	pagestore_require_localsvc_monitoring();
	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore materializer monitoring requires recovery mode")));
	if (!pagestore_materializer)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore materializer monitoring requires pagestore.materializer = on")));
	if (!pagestore_route_all)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore materializer monitoring requires full relation routing")));
	if (pagestore_localsvc_read_lsn() != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore materializer monitoring requires an unpinned recovery worker")));
}

/* End of the durable WAL prefix currently available from pagestore. */
PG_FUNCTION_INFO_V1(pagestore_shipped_wal_lsn);

Datum
pagestore_shipped_wal_lsn(PG_FUNCTION_ARGS)
{
	pagestore_require_localsvc_monitoring();
	PG_RETURN_LSN(pagestore_materializer_shipped_lsn(
		PS_MATERIALIZER_MARKER_TIMEOUT_MS));
}

/* Last restartpoint boundary whose relation pages are durable in pagestore. */
PG_FUNCTION_INFO_V1(pagestore_materialized_wal_lsn);

Datum
pagestore_materialized_wal_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	materialized;

	pagestore_require_materializer();
	materialized = pagestore_materialized_wal_lsn_internal();
	/* Promotion during the store read must fail closed. */
	pagestore_require_materializer();
	PG_RETURN_LSN(materialized);
}

/*
 * Bytes between the store's durable WAL end and this recovery worker's flushed
 * materialization watermark.  This is deliberately recovery-only: a writer
 * does not publish restartpoint materialization progress and could otherwise
 * masquerade as a caught-up worker.
 */
PG_FUNCTION_INFO_V1(pagestore_materializer_lag_bytes);

Datum
pagestore_materializer_lag_bytes(PG_FUNCTION_ARGS)
{
	XLogRecPtr	shipped;
	XLogRecPtr	materialized;
	uint64		lag;

	pagestore_require_materializer();

	/* Sample the flushed watermark first to prevent a concurrent false zero. */
	materialized = pagestore_materialized_wal_lsn_internal();
	shipped = pagestore_materializer_shipped_lsn(
		PS_MATERIALIZER_MARKER_TIMEOUT_MS);
	/* Promotion during either store read must fail closed. */
	pagestore_require_materializer();
	lag = shipped > materialized ? shipped - materialized : 0;
	if (lag > PG_INT64_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("pagestore materializer lag exceeds bigint range")));
	PG_RETURN_INT64((int64) lag);
}

/*
 * Store-observed materializer state for writer-side control-plane monitoring.
 * Unlike pagestore_materializer_lag_bytes(), this does not claim that the
 * caller is the materializer.  A missing durable marker is represented by
 * NULL marker/lag/release fields rather than a false zero-lag result.
 */
PG_FUNCTION_INFO_V1(pagestore_materializer_status);

Datum
pagestore_materializer_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[4] = {0};
	bool		nulls[4] = {false, false, false, false};
	XLogRecPtr	shipped;
	XLogRecPtr	materialized;
	XLogRecPtr	release;
	uint64		lag;

	pagestore_require_localsvc_monitoring();

	/* Sample marker first so concurrent progress can only overstate the lag. */
	materialized = pagestore_materialized_wal_lsn_internal();
	release = pagestore_materializer_release_checkpoint_read(materialized);
	shipped = pagestore_materializer_shipped_lsn(
		PS_MATERIALIZER_MARKER_TIMEOUT_MS);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = LSNGetDatum(shipped);

	if (XLogRecPtrIsInvalid(materialized))
	{
		nulls[1] = true;
		nulls[2] = true;
		nulls[3] = true;
	}
	else
	{
		values[1] = LSNGetDatum(materialized);
		lag = shipped > materialized ? shipped - materialized : 0;
		if (lag > PG_INT64_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("pagestore materializer lag exceeds bigint range")));
		values[2] = Int64GetDatum((int64) lag);
		if (XLogRecPtrIsInvalid(release))
			nulls[3] = true;
		else
			values[3] = LSNGetDatum(release);
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
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
	uint64		shipped_end;
	uint64		indexed_end = 0;
	uint64		max_lag = 0;
	uint64		headroom = 0;
	XLogRecPtr	materialized = InvalidXLogRecPtr;
	XLogRecPtr	checkpoint = InvalidXLogRecPtr;
	uint64		materializer_limit = 0;
	uint64		max_record_wal_span;

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

	/* unreachable when pinned: archive_mode = off is enforced at startup */
	if (pagestore_localsvc_read_lsn() != 0)
		return false;

	/* Both lag controllers compare against the store's durable WAL prefix. */
	shipped_end = pagestore_localsvc_wal_end();
	if (pagestore_materializer_max_lag_mb > 0)
	{
		materialized = pagestore_materialized_wal_lsn_internal();
		checkpoint = pagestore_materializer_release_checkpoint(materialized);
		materializer_limit = pagestore_materializer_archive_limit(
			materialized, checkpoint, seg_start, shipped_end);
	}
	if (pagestore_wal_index_max_lag_mb > 0)
	{
		indexed_end = shipped_end == 0 ? seg_start :
			pagestore_localsvc_walidx_progress();
		max_lag = (uint64) pagestore_wal_index_max_lag_mb * 1024 * 1024;
		/*
		 * XLogRecordMaxSize counts record bytes, not their WAL-address span.
		 * Conservatively charge every page the larger long-header size, plus
		 * one initial partial page for a record that starts at page end.
		 */
		max_record_wal_span =
			((uint64) XLogRecordMaxSize +
			 (XLOG_BLCKSZ - SizeOfXLogLongPHD) - 1) /
			(XLOG_BLCKSZ - SizeOfXLogLongPHD) * XLOG_BLCKSZ + XLOG_BLCKSZ;
		headroom = max_lag + max_record_wal_span + wal_segment_size;
	}

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
		ssize_t		n;
		uint64		current = seg_start + off;

		n = read(fd, buf, PS_IO_UNIT);
		if (n < 0)
		{
			pfree(buf);
			close(fd);
			return false;
		}
		if (n == 0)
			break;

		if (pagestore_materializer_max_lag_mb > 0 &&
			current >= shipped_end && current >= materializer_limit)
		{
			materialized = pagestore_materialized_wal_lsn_internal();
			checkpoint = pagestore_materializer_release_checkpoint(materialized);
			materializer_limit = pagestore_materializer_archive_limit(
				materialized, checkpoint, seg_start, shipped_end);
			if (current >= materializer_limit)
			{
				pfree(buf);
				close(fd);
				if (XLogRecPtrIsInvalid(materialized))
					ereport(WARNING,
							(errmsg("pagestore archive paused: materializer watermark is unavailable"),
							 errhint("Start or repair the declared pagestore materializer, "
									 "or disable pagestore.materializer_max_lag_mb.")));
				else
					ereport(WARNING,
							(errmsg("pagestore archive paused: materializer is %llu bytes behind",
									(unsigned long long) (current - (uint64) materialized)),
							 errhint("Increase pagestore.materializer_max_lag_mb or restore materializer progress.")));
				return false;
			}
		}

		if (pagestore_wal_index_max_lag_mb > 0)
		{
			/*
			 * A decoder cannot advance past a partial record.  Reserve enough for
			 * PostgreSQL's largest legal record plus segment/page padding so
			 * backpressure cannot strand the index boundary inside that record.
			 */
			if (seg_start + off > indexed_end &&
				seg_start + off - indexed_end >= headroom)
			{
				indexed_end = pagestore_localsvc_walidx_progress();
				if (seg_start + off > indexed_end &&
					seg_start + off - indexed_end >= headroom)
				{
					pfree(buf);
					close(fd);
					ereport(WARNING,
							(errmsg("pagestore archive paused: WAL index is %llu bytes behind",
									(unsigned long long) (seg_start + off - indexed_end)),
							 errhint("Increase pagestore.wal_index_max_lag_mb or restore WAL indexing progress.")));
					return false;
				}
			}
		}
		/*
		 * Resend overlap deliberately: WAL_APPEND compares every durable byte,
		 * rejecting a second writer or divergent history before accepting a
		 * suffix.  Matching overlap is idempotent.
		 */
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
 * pagestore_rel_nblocks_asof(rel regclass, forknum int, lsn pg_lsn) -> int8
 * pagestore_rel_exists_asof(rel regclass, forknum int, lsn pg_lsn) -> bool
 *
 * The store's fork size/existence as of an LSN horizon (the same resolution a
 * pinned reader's smgr NBLOCKS/EXISTS uses).  Test/inspection helpers.
 */
PG_FUNCTION_INFO_V1(pagestore_rel_nblocks_asof);

Datum
pagestore_rel_nblocks_asof(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	uint64		lsn = (uint64) PG_GETARG_LSN(2);
	Relation	rel;
	PageStoreRelKey key;
	uint64		n;

	rel = relation_open(relid, AccessShareLock);
	key.spcOid = rel->rd_locator.spcOid;
	key.dbOid = rel->rd_locator.dbOid;
	key.relNumber = rel->rd_locator.relNumber;
	key.forkNum = forknum;
	n = pagestore_localsvc_nblocks_asof(&key, lsn);
	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64((int64) n);
}

PG_FUNCTION_INFO_V1(pagestore_rel_exists_asof);

Datum
pagestore_rel_exists_asof(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		forknum = PG_GETARG_INT32(1);
	uint64		lsn = (uint64) PG_GETARG_LSN(2);
	Relation	rel;
	PageStoreRelKey key;
	int			r;

	rel = relation_open(relid, AccessShareLock);
	key.spcOid = rel->rd_locator.spcOid;
	key.dbOid = rel->rd_locator.dbOid;
	key.relNumber = rel->rd_locator.relNumber;
	key.forkNum = forknum;
	r = pagestore_localsvc_exists_asof(&key, lsn);
	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(r != 0);
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

	n = pagestore_localsvc_walidx_get(&key, (BlockNumber) blocknum,
									  (uint64) lsn, &recs);
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
			/* a record ending after lsn is not in the as-of stream */
			if (reader->EndRecPtr <= (XLogRecPtr) lsn &&
				RestoreBlockImage(reader, b, page))
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
static bool ps_redo_liveness_from_store;	/* branch-aware truncate scan */

static int
ps_redo_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				  XLogRecPtr targetRecPtr, char *readBuf)
{
	if (pagestore_redo_wal_from_store || ps_redo_liveness_from_store ||
		ps_redo_cur_timeline != ps_redo_local_timeline)
	{
		int			n = pagestore_localsvc_wal_read(ps_redo_cur_timeline,
													(uint64) targetPagePtr,
													XLOG_BLCKSZ, readBuf);

		/* whole page, not reqLen: header validation reads past reqLen on
		 * segment-start pages (see ps_slru_wal_page_read) */
		return (n >= reqLen) ? n : -1;
	}
	return read_local_xlog_page_no_wait(state, targetPagePtr, reqLen,
										targetRecPtr, readBuf);
}

/*
 * Liveness: scan the branch WAL stream in (from_lsn, to_lsn] for an smgr truncate of
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

	n = pagestore_localsvc_walidx_get(&key, (BlockNumber) blocknum,
									  (uint64) lsn, &recs);
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
			/* a record ending after the as-of point is not in the as-of
			 * stream; its image must not become the base either */
			if (reader->EndRecPtr <= (XLogRecPtr) lsn &&
				RestoreBlockImage(reader, b, base))
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
		volatile RedoBlockLiveness liveness = REDO_BLOCK_SCAN_INCOMPLETE;
		bool		saved_liveness_from_store = ps_redo_liveness_from_store;

		/*
		 * The truncate scan walks the WAL after the block's last write.  Force the
		 * current branch's store-backed stream: it performs the ancestry walk when
		 * the range crosses a fork, and also works on a compute with no local WAL.
		 * Invalidate the reader's page cache first since the source timeline may
		 * differ from the record loop above.
		 */
		/*
		 * Read the current branch's WAL from the store.  Its WAL read-through
		 * walks ancestors below each fork point, so this single scan covers an
		 * ancestor last-write and a branch-local truncate in the same history.
		 */
		PG_TRY();
		{
			ps_redo_liveness_from_store =
				recs[n - 1].timeline != pagestore_localsvc_timeline();
			ps_redo_cur_timeline = pagestore_localsvc_timeline();
			reader->readLen = 0;
			liveness = redo_block_truncated_away(reader, rloc, forknum,
												 (BlockNumber) blocknum,
												 (XLogRecPtr) recs[n - 1].lsn, lsn);
		}
		PG_FINALLY();
		{
			ps_redo_liveness_from_store = saved_liveness_from_store;
		}
		PG_END_TRY();
		/*
		 * Fail closed unless the scan proved the block live: a confirmed truncate
		 * and an incomplete scan (the WAL could not be read through lsn) both mean
		 * we must not return a possibly-stale FPI for a block that may be gone.
		 */
		if ((RedoBlockLiveness) liveness != REDO_BLOCK_LIVE)
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
		if (rec != NULL && reader->EndRecPtr > (XLogRecPtr) lsn)
		{
			/*
			 * The walidx keys records by their START LSN, so an as-of point
			 * falling inside a record still lists it -- but a record whose
			 * END is past the as-of point is not part of the WAL stream "as
			 * of lsn" and must not be applied.  Later records start even
			 * later, so stop replaying here.
			 */
			break;
		}
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
	PageSetChecksum((Page) page, (BlockNumber) blocknum);

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

/* Stable per-SLRU object id; shared with the live mirror (pagestore_slru.c)
 * so seed snapshots and live images address the same logical SLRU. */
static uint32
slru_klass_id(const char *name)
{
	return pagestore_slru_klass_id(name);
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

static int
slru_check_dir(const char *slru)
{
	for (int i = 0; i < (int) lengthof(ps_slru_dirs); i++)
		if (strcmp(ps_slru_dirs[i], slru) == 0)
			return i;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("\"%s\" is not an in-scope SLRU directory", slru)));
	pg_unreachable();
}

static void
slru_obj_key(PageStoreRelKey *key, const char *slru)
{
	key->spcOid = 0;
	key->dbOid = 0;
	key->relNumber = (RelFileNumber) slru_klass_id(slru);
	key->forkNum = 0;
}

typedef void (*PsSlruPageConsumer) (const char *slru, BlockNumber pageno,
									const char *page, void *arg);

/* Scan one clean SLRU directory and hand every complete page to a consumer. */
static int64
slru_scan_snapshot(const char *slru, PsSlruPageConsumer consume, void *arg)
{
	DIR		   *dir;
	struct dirent *de;
	int64		pages = 0;
	char	   *page = palloc(BLCKSZ);

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
		 * Match SlruScanDirectory()'s filename test: short SLRUs use
		 * 4-6 upper-case hex characters, while long-name SLRUs such as
		 * pg_multixact/members use exactly 15.
		 */
		if ((namelen < 4 || namelen > 6) && namelen != 15)
			continue;
		if (strspn(de->d_name, "0123456789ABCDEF") != namelen)
			continue;
		errno = 0;
		segno = strtoll(de->d_name, &endp, 16);
		if (endp == de->d_name || *endp != '\0' || errno != 0 || segno < 0)
			continue;
		if ((uint64) segno > MaxBlockNumber / SLRU_PAGES_PER_SEGMENT)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("SLRU page number exceeds pagestore block range")));

		snprintf(segpath, sizeof(segpath), "%s/%s", slru, de->d_name);
		fd = OpenTransientFile(segpath, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open SLRU segment \"%s\": %m", segpath)));

		for (pageidx = 0; pageidx < SLRU_PAGES_PER_SEGMENT; pageidx++)
		{
			int			n = read(fd, page, BLCKSZ);
			uint64		pageno;

			if (n == 0)
				break;
			if (n != BLCKSZ)
			{
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("short read on SLRU segment \"%s\"", segpath)));
			}
			pageno = (uint64) segno * SLRU_PAGES_PER_SEGMENT + pageidx;
			if (pageno > MaxBlockNumber)
			{
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("SLRU page number exceeds pagestore block range")));
			}
			consume(slru, (BlockNumber) pageno, page, arg);
			pages++;
		}
		CloseTransientFile(fd);
		CHECK_FOR_INTERRUPTS();
	}
	FreeDir(dir);
	pfree(page);
	return pages;
}

typedef struct PsSlruDirectContext
{
	XLogRecPtr	cutoff;
} PsSlruDirectContext;

static void
slru_publish_page(const char *slru, BlockNumber pageno, const char *page,
				  void *arg)
{
	PsSlruDirectContext *context = arg;
	PageStoreRelKey key;

	slru_obj_key(&key, slru);
	pagestore_localsvc_obj_write(PS_KLASS_SLRU, &key, pageno, page,
								 (uint64) context->cutoff);
}

typedef struct PsSlruStagePage
{
	uint32		dir_index;
	BlockNumber pageno;
	char		page[BLCKSZ];
} PsSlruStagePage;

typedef struct PsSlruStageContext
{
	File		file;
	pgoff_t		offset;
} PsSlruStageContext;

static void
slru_stage_page(const char *slru, BlockNumber pageno, const char *page,
				void *arg)
{
	PsSlruStageContext *context = arg;
	PsSlruStagePage staged;
	ssize_t		written;

	memset(&staged, 0, sizeof(staged));
	staged.dir_index = (uint32) slru_check_dir(slru);
	staged.pageno = pageno;
	memcpy(staged.page, page, BLCKSZ);
	written = FileWrite(context->file, &staged, sizeof(staged), context->offset,
						WAIT_EVENT_DATA_FILE_WRITE);
	if (written < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stage pagestore SLRU snapshot: %m")));
	if (written != sizeof(staged))
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("short write while staging pagestore SLRU snapshot")));
	context->offset += sizeof(staged);
}

static void
slru_publish_stage(File file, int64 pages, XLogRecPtr cutoff)
{
	PsSlruDirectContext context = {.cutoff = cutoff};
	PsSlruStagePage staged;
	pgoff_t		offset = 0;

	for (int64 i = 0; i < pages; i++)
	{
		ssize_t		read_bytes;

		read_bytes = FileRead(file, &staged, sizeof(staged), offset,
						  WAIT_EVENT_DATA_FILE_READ);
		if (read_bytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read staged pagestore SLRU snapshot: %m")));
		if (read_bytes != sizeof(staged))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("short read from staged pagestore SLRU snapshot")));
		if (staged.dir_index >= lengthof(ps_slru_dirs))
			elog(ERROR, "invalid staged pagestore SLRU directory index");
		slru_publish_page(ps_slru_dirs[staged.dir_index], staged.pageno,
						  staged.page, &context);
		offset += sizeof(staged);
	}
}

/*
 * pagestore_ship_slru_snapshot(slru text, cutoff pg_lsn) returns bigint
 *
 * Ship a clean whole-segment snapshot of SLRU directory 'slru' (e.g. 'pg_xact')
 * keyed by 'cutoff'.  'cutoff' must provably upper-bound the on-disk segments'
 * contents -- the caller captures it at a quiescent point (a clean
 * shutdown/restartpoint, or under a brief SLRU write barrier).  This remains
 * the expert API; pagestore_capture_slru_snapshot() is the installed path that
 * proves C on a recovery materializer.  Returns the page count; fails closed
 * (ereport) on any read/IPC error rather than shipping a partial snapshot.
 */
PG_FUNCTION_INFO_V1(pagestore_ship_slru_snapshot);
Datum
pagestore_ship_slru_snapshot(PG_FUNCTION_ARGS)
{
	char	   *slru = text_to_cstring(PG_GETARG_TEXT_PP(0));
	XLogRecPtr	cutoff = PG_GETARG_LSN(1);
	PsSlruDirectContext context = {.cutoff = cutoff};
	int64		shipped;

	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	shipped = slru_scan_snapshot(slru, slru_publish_page, &context);

	PG_RETURN_INT64(shipped);
}

/*
 * Capture all in-scope SLRUs at a cutoff proven by a paused recovery worker.
 * The temporary stage prevents a concurrent resume from publishing a mixed
 * image: replay is rechecked before any staged page reaches the store.
 */
PG_FUNCTION_INFO_V1(pagestore_capture_slru_snapshot);
Datum
pagestore_capture_slru_snapshot(PG_FUNCTION_ARGS)
{
	PsSlruStageContext stage;
	XLogRecPtr	replay_before;
	XLogRecPtr	replay_after;
	XLogRecPtr	materialized;
	int64		pages = 0;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to capture a pagestore SLRU snapshot")));
	pagestore_require_materializer();
	if (GetRecoveryPauseState() != RECOVERY_PAUSED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore SLRU snapshot capture requires paused WAL replay"),
				 errhint("Call pg_wal_replay_pause() and wait until pg_get_wal_replay_pause_state() returns 'paused'.")));

	replay_before = GetXLogReplayRecPtr(NULL);
	if (XLogRecPtrIsInvalid(replay_before))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore SLRU snapshot cutoff is unavailable")));

	/* Flush all four SLRUs and publish the matching durable marker. */
	RequestCheckpoint(CHECKPOINT_WAIT | CHECKPOINT_FAST);
	pagestore_require_materializer();
	if (GetRecoveryPauseState() != RECOVERY_PAUSED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL replay resumed during pagestore SLRU snapshot capture")));
	replay_after = GetXLogReplayRecPtr(NULL);
	if (replay_after != replay_before)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL replay advanced during pagestore SLRU snapshot capture")));
	materialized = pagestore_materialized_wal_lsn_internal();
	if (materialized != replay_before)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("restartpoint did not durably cover the paused WAL replay position"),
				 errdetail("Paused replay is at %X/%08X, but the durable materializer watermark is at %X/%08X.",
						   LSN_FORMAT_ARGS(replay_before),
						   LSN_FORMAT_ARGS(materialized)),
				 errhint("Replay through a newer checkpoint record, pause recovery again, and retry.")));

	stage.file = OpenTemporaryFile(false);
	stage.offset = 0;
	for (int i = 0; i < (int) lengthof(ps_slru_dirs); i++)
		pages += slru_scan_snapshot(ps_slru_dirs[i], slru_stage_page, &stage);

	/* Fail before store writes if the supposedly stable local image moved. */
	pagestore_require_materializer();
	replay_after = GetXLogReplayRecPtr(NULL);
	if (GetRecoveryPauseState() != RECOVERY_PAUSED ||
		replay_after != replay_before)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL replay changed while staging pagestore SLRU snapshot")));

	slru_publish_stage(stage.file, pages, replay_before);
	pagestore_localsvc_store_sync_timeout(PS_MATERIALIZER_MARKER_TIMEOUT_MS);
	FileClose(stage.file);
	PG_RETURN_LSN(replay_before);
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
 * The same shape is implemented for multixact and commit-ts below.  It reads
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

/* Return the logical-array index for a physical CLOG page, or -1 if absent. */
static int
ps_clog_seed_page_index(int64 pageno, int64 page_lo, int64 page_hi,
						bool wraps)
{
	int64		max_page = PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE;

	if (!wraps)
		return pageno >= page_lo && pageno <= page_hi ?
			(int) (pageno - page_lo) : -1;
	if (pageno >= page_lo && pageno <= max_page)
		return (int) (pageno - page_lo);
	if (pageno >= 0 && pageno <= page_hi)
		return (int) (max_page - page_lo + 1 + pageno);
	return -1;
}

/* Match clog.c's wraparound-aware page ordering for truncation decisions. */
static bool
ps_clog_page_precedes(int64 page1, int64 page2)
{
	TransactionId xid1 = (TransactionId) page1 * PS_CLOG_XACTS_PER_PAGE +
		FirstNormalTransactionId + 1;
	TransactionId xid2 = (TransactionId) page2 * PS_CLOG_XACTS_PER_PAGE +
		FirstNormalTransactionId + 1;

	return TransactionIdPrecedes(xid1, xid2) &&
		TransactionIdPrecedes(xid1, xid2 + PS_CLOG_XACTS_PER_PAGE - 1);
}

/* Set xid's status in a CLOG page array that can cross the XID wrap point. */
static inline void
ps_clog_seed_set(char *pages, int64 page_lo, int64 page_hi, bool wraps,
				 TransactionId xid, int status)
{
	int64		pg;
	int			idx;

	if (!TransactionIdIsNormal(xid))
		return;
	pg = (int64) xid / PS_CLOG_XACTS_PER_PAGE;
	idx = ps_clog_seed_page_index(pg, page_lo, page_hi, wraps);
	if (idx < 0)
		return;
	ps_clog_set_status(pages + idx * BLCKSZ, pg, xid, status);
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
 * SLRU appliers/seeders: WAL access that honours pagestore.redo_wal_from_store.
 *
 * The appliers replay (C, L] with a linear scan.  On a compute whose local
 * pg_wal does not hold that window (a fresh utility compute preparing a
 * branch; pagestore.timeline names the timeline it acts for), the same GUC
 * that redirects redo_page_asof redirects these scans to the store's shipped
 * per-timeline WAL log.  The store holds completed segments only (the
 * archive callback ships on segment completion), so a window ending in the
 * current partial segment fails the coverage probe and the caller fails
 * closed, exactly as it does when local WAL ends early.
 *
 * The store serves a timeline's full HISTORY: wal_read() walks the branch
 * ancestry, so a window whose base predates the acting timeline's fork
 * point reads the ancestor's log below the fork and the own log above it.
 */
static int
ps_slru_wal_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr,
					  int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
	if (pagestore_redo_wal_from_store)
	{
		int			n = pagestore_localsvc_wal_read(pagestore_localsvc_timeline(),
													(uint64) targetPagePtr,
													XLOG_BLCKSZ, readBuf);

		/*
		 * The page_read contract wants ALL available bytes of the page,
		 * not just reqLen: the reader validates page headers from the
		 * returned buffer beyond reqLen (a segment-start page's LONG
		 * header is checked with fields past the 24-byte short header the
		 * first probe requests), so a reqLen-sized fill leaves garbage
		 * where validation looks.  Read the whole page; succeed only when
		 * at least reqLen of it is shipped.
		 */
		return (n >= reqLen) ? n : -1;
	}
	return read_local_xlog_page_no_wait(state, targetPagePtr, reqLen,
										targetRecPtr, readBuf);
}

/*
 * Does readable WAL extend through 'target'?  The completeness escape for a
 * window whose records all ended before the target: the scan proved nothing
 * past its last record, so the caller must know WAL itself reaches the
 * target.  Local mode asks the replay/insert position; store mode probes the
 * shipped log for the byte just below the target.
 */
static bool
ps_wal_reaches(XLogRecPtr target)
{
	if (pagestore_redo_wal_from_store)
	{
		char		b;

		if (target == 0)
			return false;
		return pagestore_localsvc_wal_read(pagestore_localsvc_timeline(),
										   (uint64) (target - 1), 1, &b) == 1;
	}
	return ps_local_wal_limit() >= target;
}

static bool
ps_checkpoint_matches_control(const CheckPoint *record,
							  const CheckPoint *control)
{
	return record->redo == control->redo &&
		record->ThisTimeLineID == control->ThisTimeLineID &&
		record->PrevTimeLineID == control->PrevTimeLineID &&
		record->fullPageWrites == control->fullPageWrites &&
		record->wal_level == control->wal_level &&
		record->logicalDecodingEnabled == control->logicalDecodingEnabled &&
		FullTransactionIdEquals(record->nextXid, control->nextXid) &&
		record->nextOid == control->nextOid &&
		record->nextMulti == control->nextMulti &&
		record->nextMultiOffset == control->nextMultiOffset &&
		record->oldestXid == control->oldestXid &&
		record->oldestXidDB == control->oldestXidDB &&
		record->oldestMulti == control->oldestMulti &&
		record->oldestMultiDB == control->oldestMultiDB &&
		record->time == control->time &&
		record->oldestCommitTsXid == control->oldestCommitTsXid &&
		record->newestCommitTsXid == control->newestCommitTsXid &&
		record->oldestActiveXid == control->oldestActiveXid &&
		record->dataChecksumState == control->dataChecksumState;
}

/*
 * Resolve and verify the checkpoint record named by a mirrored control image.
 * Its EndRecPtr is the earliest branch LSN which contains the complete record;
 * comparing only against ControlFileData.checkPoint (the record start) would
 * admit a cutoff in the middle of the record.
 */
static XLogRecPtr
ps_checkpoint_record_end(XLogRecPtr checkpoint_lsn,
						 const CheckPoint *expected)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	char	   *errm = NULL;
	XLogRecPtr	end = InvalidXLogRecPtr;
	bool		matches = false;

	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("pagestore: could not allocate a WAL reader")));

	XLogBeginRead(reader, checkpoint_lsn);
	if (XLogReadRecord(reader, &errm) != NULL &&
		reader->ReadRecPtr == checkpoint_lsn &&
		XLogRecGetRmid(reader) == RM_XLOG_ID &&
		((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_ONLINE ||
		 (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN) &&
		XLogRecGetDataLen(reader) == sizeof(CheckPoint) &&
		ps_checkpoint_matches_control(
			(const CheckPoint *) XLogRecGetData(reader), expected))
	{
		end = reader->EndRecPtr;
		matches = true;
	}

	XLogReaderFree(reader);
	pfree(pd);
	if (!matches)
		ereport(ERROR,
				(errmsg("mirrored control image has no matching readable checkpoint record"),
				 errdetail("Expected checkpoint record at %X/%08X.",
							   LSN_FORMAT_ARGS(checkpoint_lsn))));
	return end;
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
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
		 ps_wal_reaches(target_lsn));
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
 * (TimestampTz + ReplOriginId), packed COMMIT_TS_XACTS_PER_PAGE to a page.
 *
 * Toggles: track_commit_timestamp flips only across a restart (PGC_POSTMASTER), landing a
 * XLOG_PARAMETER_CHANGE in the window.  Each toggle is an era boundary -- DEACTIVATION
 * deletes every segment, ACTIVATION restarts from nothing -- replayed by wiping the
 * accumulated state; see the era comments in the scan loops.  A window that ENDS
 * deactivated fails closed (an inactive fork has no commit-ts state to reconstruct).
 */
#include "catalog/pg_control.h"
#include "port/pg_crc32c.h"

/*
 * Read the mirrored control image as of 'lsn' into *cf.  Returns true only
 * for a CRC-valid image at/below lsn; false means "unknown" and callers
 * must stay conservative.  The control mirror ships a fresh image at every
 * UpdateControlFile -- including the one XLogReportParameters issues at a
 * track_commit_timestamp toggle -- so the flag as of any LSN is faithful
 * whenever an image exists.
 */
static bool
ps_control_asof_timeout(XLogRecPtr lsn, ControlFileData *cf, int timeout_ms)
{
	PageStoreRelKey key = {0};
	char		page[BLCKSZ];
	uint64		resolved = 0;
	pg_crc32c	crc;

	if (BLCKSZ < PG_CONTROL_FILE_SIZE)
		return false;
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_CONTROL, &key, 0,
												(uint64) lsn, page, &resolved,
												timeout_ms))
		return false;
	memcpy(cf, page, sizeof(*cf));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	return EQ_CRC32C(crc, cf->crc);
}

static bool
ps_control_asof(XLogRecPtr lsn, ControlFileData *cf)
{
	return ps_control_asof_timeout(lsn, cf, 0);
}

/*
 * Discover a newer durable checkpoint view.  This only publishes a candidate:
 * adopting it also requires the exact-R running-XID snapshot, which is a
 * separate artifact.  Keeping discovery shared avoids one control-store read
 * per backend while preserving the fixed reader's current semantics.
 */
static void
pagestore_refresh_reader_horizon(void)
{
	ControlFileData control;
	TimestampTz now;
	TimestampTz refreshed_at;
	XLogRecPtr candidate;
	uint64		read_seq;
	MemoryContext cxt = CurrentMemoryContext;
	bool		valid = false;
	bool		owns_lease;
	ErrorData  *edata = NULL;

	if (!pagestore_advance_read_lsn || pagestore_reader_horizon == NULL ||
		pagestore_localsvc_read_lsn() == 0)
		return;

	now = GetCurrentTimestamp();
	SpinLockAcquire(&pagestore_reader_horizon->mutex);
	refreshed_at = pagestore_reader_horizon->refreshed_at;
	if (refreshed_at != 0 &&
		!TimestampDifferenceExceeds(refreshed_at, now,
								PAGESTORE_READER_HORIZON_TTL_MS))
	{
		SpinLockRelease(&pagestore_reader_horizon->mutex);
		return;
	}
	if (pagestore_reader_horizon->refresh_owner_pid != 0 &&
		!TimestampDifferenceExceeds(
			pagestore_reader_horizon->refresh_started_at, now,
			PAGESTORE_READER_HORIZON_LEASE_MS))
	{
		SpinLockRelease(&pagestore_reader_horizon->mutex);
		return;
	}
	pagestore_reader_horizon->refresh_owner_pid = MyProcPid;
	pagestore_reader_horizon->refresh_started_at = now;
	SpinLockRelease(&pagestore_reader_horizon->mutex);

	PG_TRY();
	{
		if (ps_control_asof_timeout((XLogRecPtr) UINT64_MAX, &control,
									PAGESTORE_READER_HORIZON_TIMEOUT_MS))
		{
			candidate = control.checkPointCopy.redo;
			valid = !XLogRecPtrIsInvalid(candidate) &&
				pagestore_localsvc_read_fence_timeout((uint64) candidate,
													&read_seq,
													PAGESTORE_READER_HORIZON_TIMEOUT_MS);
			/*
			 * READ_AT can see a writer's posted control batch before its drain
			 * reaches sync.  Sync after both reads makes the exact control/fence
			 * pair durable before the shared candidate becomes visible.
			 */
			if (valid)
				pagestore_localsvc_store_sync_timeout(
					PAGESTORE_READER_HORIZON_TIMEOUT_MS);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(cxt);
		edata = CopyErrorData();
	}
	PG_END_TRY();

	SpinLockAcquire(&pagestore_reader_horizon->mutex);
	owns_lease = pagestore_reader_horizon->refresh_owner_pid == MyProcPid;
	if (owns_lease)
	{
		if (edata == NULL && valid &&
			candidate > pagestore_reader_horizon->candidate_lsn)
		{
			pagestore_reader_horizon->candidate_lsn = candidate;
			pagestore_reader_horizon->candidate_generation++;
			if (pagestore_reader_horizon->candidate_generation == 0)
				pagestore_reader_horizon->candidate_generation = 1;
		}
		pagestore_reader_horizon->refreshed_at = GetCurrentTimestamp();
		pagestore_reader_horizon->refresh_owner_pid = 0;
		pagestore_reader_horizon->refresh_started_at = 0;
	}
	SpinLockRelease(&pagestore_reader_horizon->mutex);

	if (edata != NULL)
	{
		if (edata->elevel >= FATAL)
			ReThrowError(edata);
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		{
			ReThrowError(edata);
		}
		FreeErrorData(edata);
		FlushErrorState();
	}
}

static bool
ps_track_commit_ts_asof(XLogRecPtr lsn, bool *track)
{
	ControlFileData cf;

	if (!ps_control_asof(lsn, &cf))
		return false;
	*track = cf.track_commit_timestamp;
	return true;
}

/*
 * The commit-ts horizon (oldestCommitTsXid) as of 'lsn', derived from the
 * mirrored control image.  Returns false when commit-ts is off at lsn or no
 * image exists.  Between an activation and the next checkpoint the
 * checkpoint copy's oldestCommitTsXid is still the OFF era's Invalid; the
 * activation horizon is then the checkpoint copy's nextXid -- the toggle
 * crossed a restart, so the shutdown checkpoint's nextXid IS the nextXid
 * ActivateCommitTs read at startup.  (After a crash restart with a changed
 * GUC the pre-crash checkpoint understates it; the seeder's required-page
 * check fails closed on the gap rather than fabricating state.)
 */
static bool
ps_commit_ts_horizon_asof(XLogRecPtr lsn, TransactionId *oldest)
{
	ControlFileData cf;

	if (!ps_control_asof(lsn, &cf) || !cf.track_commit_timestamp)
		return false;
	if (TransactionIdIsNormal(cf.checkPointCopy.oldestCommitTsXid))
		*oldest = cf.checkPointCopy.oldestCommitTsXid;
	else
		*oldest = XidFromFullTransactionId(cf.checkPointCopy.nextXid);
	return true;
}

/*
 * Normalize a caller-supplied commit-ts horizon pair for a fork at
 * 'target'.  pg_control cannot express "active but not yet
 * checkpointed": between an activation and its first checkpoint BOTH
 * oldestCommitTsXid and newestCommitTsXid are still Invalid while the
 * mirrored control flag is already on.  So:
 *
 * - (Invalid, normal): derive oldest from the toggle-time control image.
 * - (Invalid, Invalid) with the control flag ON as of target: an active
 *   pre-checkpoint fork -- derive oldest the same way and bound next by
 *   the fork's nextXid (commit-ts entries exist only for assigned xids,
 *   exactly clog's bound); treating the pair as "inactive" would install
 *   an empty pg_commit_ts and silently lose the era's timestamps.
 * - (Invalid, Invalid) with the flag off/unknown: genuinely inactive.
 */
static void
ps_commit_ts_normalize_horizons(XLogRecPtr target, TransactionId next_xid,
								TransactionId *oldest, TransactionId *next)
{
	bool		track;

	if (TransactionIdIsNormal(*oldest))
		return;
	if (!TransactionIdIsNormal(*next))
	{
		if (!ps_track_commit_ts_asof(target, &track))
			ereport(ERROR,
					(errmsg("pagestore: commit-ts state as of the target LSN is unknown (no readable control image)"),
					 errhint("An active pre-checkpoint fork is indistinguishable from an inactive one without the control mirror; refusing to guess.")));
		if (!track)
			return;				/* known inactive: leave the pair non-normal */
		*next = next_xid;
	}
	if (!ps_commit_ts_horizon_asof(target, oldest))
		ereport(ERROR,
				(errmsg("pagestore: no commit-ts horizon supplied and none derivable from the control image as of the target LSN")));
}

#define PS_CTS_ENTRY_SIZE	(sizeof(TimestampTz) + sizeof(ReplOriginId))
#define PS_CTS_XACTS_PER_PAGE	(BLCKSZ / PS_CTS_ENTRY_SIZE)

/* Match commit_ts.c's wraparound-aware page ordering for truncation. */
static bool
ps_commit_ts_page_precedes(int64 page1, int64 page2)
{
	TransactionId xid1 = (TransactionId) page1 * PS_CTS_XACTS_PER_PAGE +
		FirstNormalTransactionId + 1;
	TransactionId xid2 = (TransactionId) page2 * PS_CTS_XACTS_PER_PAGE +
		FirstNormalTransactionId + 1;

	return TransactionIdPrecedes(xid1, xid2) &&
		TransactionIdPrecedes(xid1, xid2 + PS_CTS_XACTS_PER_PAGE - 1);
}

/* Write xid's (ts, nodeid) into commit-ts page 'pageno' in 'page' if it lives there. */
static void
ps_commit_ts_set(char *page, int64 pageno, TransactionId xid,
				 TimestampTz ts, ReplOriginId nodeid)
{
	int			entryno;
	char	   *e;

	if (!TransactionIdIsNormal(xid) ||
		(int64) (xid / PS_CTS_XACTS_PER_PAGE) != pageno)
		return;
	entryno = xid % PS_CTS_XACTS_PER_PAGE;
	e = page + (Size) entryno * PS_CTS_ENTRY_SIZE;
	memcpy(e, &ts, sizeof(TimestampTz));
	memcpy(e + sizeof(TimestampTz), &nodeid, sizeof(ReplOriginId));
}

/* Replay (base_lsn, target_lsn] onto commit-ts page 'pageno'.  Mirrors
 * ps_clog_apply_range: same straddle/zeropage/truncate/completeness handling, but applies
 * commit timestamps (commits only -- aborts have no commit-ts) and watches for a
 * commit-ts deactivation.  *deactivated is set if track_commit_timestamp is turned off. */
static PsClogReplay
ps_commit_ts_apply_range(char *page, int64 pageno, XLogRecPtr base_lsn,
						 XLogRecPtr target_lsn, TransactionId horizon_xid,
						 bool *deactivated)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	char	   *errm = NULL;
	XLogRecPtr	scanned = base_lsn;
	XLogRecPtr	readfrom;
	PsClogReplay r = {false, false, false};
	int			era = -1;		/* -1 unknown-assume-on, 0 off, 1 on */

	*deactivated = false;

	/*
	 * Initialize the era state from the base's control image: a window that
	 * BEGINS in an off era must not replay commit records into pages (the
	 * parent's TransactionTreeSetCommitTsData no-ops while inactive; commit
	 * WAL still carries xact_time), and its first track=true record is a
	 * real activation even with no in-window false.  Unknown (no image)
	 * keeps the historical assume-on behavior.
	 */
	{
		bool		track;

		if (ps_track_commit_ts_asof(base_lsn, &track))
			era = track ? 1 : 0;
	}
	if (era == 0)
	{
		memset(page, 0, BLCKSZ);
		r.page_truncated = true;	/* nothing exists in an off era */
		*deactivated = true;
	}
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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

				/*
				 * XLOG_PARAMETER_CHANGE fires for ANY changed parameter
				 * and carries the CURRENT track_commit_timestamp value,
				 * not a transition flag -- a restart that changed only,
				 * say, max_connections emits one with track=true while
				 * commit-ts stayed active throughout.  Only real
				 * transitions are era boundaries:
				 *
				 * - track=false: as of this record the state is off,
				 *   whatever it was before.  DeactivateCommitTs deleted
				 *   every local segment, so wipe (an off->off repeat
				 *   wipes an already-empty state -- harmless).
				 *
				 * - track=true after an in-window false: a real
				 *   activation; the era starts from nothing.  Within it,
				 *   ActivateCommitTs zeroes the page holding
				 *   nextXid-at-activation WITHOUT WAL; every later page
				 *   has a WAL-logged ZEROPAGE (ExtendCommitTs), so
				 *   exactly one era page may be touched by commits with
				 *   no ZEROPAGE seen -- known to start zero (see below).
				 *
				 * - track=true with no in-window false: either an
				 *   unrelated restart while active (do NOTHING -- the
				 *   accumulated state is valid), or an activation whose
				 *   deactivation predates the window (the base is empty
				 *   then, so there is nothing to wipe anyway; the
				 *   silent-zero inference below still applies).
				 */
				if (!xlrec.track_commit_timestamp)
				{
					memset(page, 0, BLCKSZ);
					r.page_zeroed = false;
					r.page_truncated = true;	/* absent until touched */
					*deactivated = true;
					era = 0;
				}
				else if (era == 0)
				{
					/*
					 * A real off->on transition (the era state says off --
					 * from an in-window false, or from the base's control
					 * image).  The new era starts from nothing; its one
					 * silently-zeroed page (ActivateCommitTs zeroes the
					 * nextXid page WITHOUT WAL; later pages have logged
					 * ZEROPAGEs) is the horizon page -- the caller's, or
					 * derived from the toggle-time control image when the
					 * caller has none (a fork before the first
					 * post-activation checkpoint).  Once consumed, the era
					 * is ON: a later true record with no intervening false
					 * is an unrelated restart and must not wipe again.
					 */
					TransactionId hx = InvalidTransactionId;

					/*
					 * Derive the activation horizon from the toggle-time
					 * control image FIRST: the caller's 'oldest' argument
					 * doubles as the lookup filter, and callers disable
					 * that filter with FirstNormal-ish values that would
					 * mis-mark page 0 as the unlogged zero page.  The
					 * caller value is only the fallback when no control
					 * image exists.
					 */
					if (!ps_commit_ts_horizon_asof(reader->EndRecPtr, &hx))
						hx = horizon_xid;
					memset(page, 0, BLCKSZ);
					if (TransactionIdIsNormal(hx) &&
						(int64) (hx / PS_CTS_XACTS_PER_PAGE) == pageno)
					{
						r.page_zeroed = true;
						r.page_truncated = false;
					}
					else
					{
						r.page_zeroed = false;
						r.page_truncated = true;
					}
					*deactivated = false;
					era = 1;
				}
				else if (era == -1)
					/*
					 * Unknown base era: this record is an activation or an
					 * unrelated restart, and the two demand opposite
					 * handling (wipe vs keep).  Guessing either way can
					 * fabricate or destroy timestamps -- refuse.
					 */
					ereport(ERROR,
							(errmsg("pagestore: cannot replay a track_commit_timestamp record: the commit-ts era at the base is unknown (no readable control image)")));
				/* else: era on -- an unrelated restart, keep everything */
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
			ReplOriginId origin = XLogRecGetOrigin(reader);

			if (era == 0)
				continue;		/* commit-ts is off: the parent recorded
								 * nothing for this commit */
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
		 ps_wal_reaches(target_lsn));
	XLogReaderFree(reader);
	pfree(pd);
	return r;
}

/* Load the base commit-ts page (snapshot as-of base_lsn) and replay (base, L].  Same
 * fail-closed contract as ps_clog_reconstruct, plus: fail closed if commit-ts was turned
 * off in the window (the reconstructed image would not match the parent's). */
static bool
ps_commit_ts_reconstruct(char *page, int64 pageno, XLogRecPtr base_lsn,
						 XLogRecPtr target_lsn, TransactionId horizon_xid)
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
	{
		bool		track;

		/*
		 * No window to scan, but the base era still decides existence: a
		 * read exactly at an off-era LSN must fail like any other
		 * off-as-of-target read, not serve an all-zero page.
		 */
		if (ps_track_commit_ts_asof(base_lsn, &track) && !track)
			ereport(ERROR,
					(errmsg("pagestore: track_commit_timestamp is off as of the target LSN; the fork has no commit-ts state")));
		return true;
	}

	r = ps_commit_ts_apply_range(page, pageno, base_lsn, target_lsn,
								 horizon_xid, &deactivated);

	if (!r.reached_target && target_lsn != PG_UINT64_MAX)
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct commit-ts as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (deactivated)
		ereport(ERROR,
				(errmsg("pagestore: track_commit_timestamp is off as of the target LSN; the fork has no commit-ts state")));
	if (r.page_truncated)
		return false;
	return true;
}

/*
 * pagestore_commit_ts_asof(xid xid, base pg_lsn, target pg_lsn, oldest xid) returns
 * timestamptz -- the commit timestamp of 'xid' as of 'target', or NULL if it has none.
 *
 * 'oldest' is the LOOKUP horizon as of target (the parent's oldestCommitTsXid
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

	if (!ps_commit_ts_reconstruct(page, pageno, base, target, oldest))
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
	TransactionId oldest;
	char	   *page = palloc(BLCKSZ);
	bytea	   *result;

	/*
	 * The horizon argument was added later; SQL wrappers created against
	 * the 3-argument signature still call this symbol.  Reading a fourth
	 * argument that was never supplied would be garbage (or a crash), so
	 * default it -- the applier derives the activation horizon from the
	 * control image anyway; the argument is only its fallback.
	 */
	oldest = (PG_NARGS() >= 4) ? PG_GETARG_TRANSACTIONID(3)
		: InvalidTransactionId;

	if (!ps_commit_ts_reconstruct(page, pageno, base, target, oldest))
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
 * new multixid's offset into its page.  Offsets are fixed-width MultiXactOffset (uint64),
 * MULTIXACT_OFFSETS_PER_PAGE to a page.  (The members file -- offset -> member list -- is
 * the second half, reconstructed by the members applier below; this is the offsets map.)
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
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
			 * MultiXactIdToOffsetPage(PreviousMultiXactId(oldestMulti)), keeping
			 * that page's segment; a page is dropped only if its segment is below
			 * it.  The bounded fork window cannot wrap, so segment order suffices. */
			cutoff = (xlrec.oldestMulti == FirstMultiXactId) ? MaxMultiXactId
				: xlrec.oldestMulti - 1;
			if (pageno / SLRU_PAGES_PER_SEGMENT <
				((int64) cutoff / PS_MXOFF_PER_PAGE) / SLRU_PAGES_PER_SEGMENT)
				r.page_truncated = true;
		}
	}

	r.reached_target = (scanned >= target_lsn) ||
		(!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		 ps_wal_reaches(target_lsn));
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
	int64		group;
	int			grouponpg,
				byteoff,
				member_in_group,
				memberoff,
				bshift;
	uint32		flagsval;

	if ((int64) (offset / PS_MXMEMB_PER_PAGE) != pageno)
		return;

	/*
	 * Keep the group arithmetic 64-bit: MultiXactOffset can exceed
	 * INT_MAX * PS_MXMEMB_PER_GROUP members on long-lived clusters, and an
	 * int intermediate would wrap grouponpg onto the wrong slots.  The
	 * within-page values all fit in int.
	 */
	group = (int64) (offset / PS_MXMEMB_PER_GROUP);
	grouponpg = (int) (group % PS_MXMEMB_GROUPS_PER_PAGE);
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

/*
 * PerformMembersTruncation() truncates to MXOffsetToMemberPage(oldestOffset),
 * and SimpleLruTruncate keeps the segment containing that cutoff page, so a
 * members segment is gone only if it sorts strictly below the cutoff's
 * segment.  The truncate record carries just the new horizon (MultiXactOffset
 * is 64-bit and never wraps), so plain segment order suffices.
 */
static bool
ps_mxmemb_segment_truncated(int64 pageseg, MultiXactOffset oldestOffset)
{
	return pageseg < ps_mxmemb_segment(oldestOffset);
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
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
											xlrec.oldestOffset))
				r.page_truncated = true;
		}
	}

	r.reached_target = (scanned >= target_lsn) ||
		(!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		 ps_wal_reaches(target_lsn));
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
	bool		era_reset = false;
	int			era = -1;		/* -1 unknown-assume-on, 0 off, 1 on */
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

	/* era state from the base's control image; see the single-page applier */
	{
		bool		track;

		if (ps_track_commit_ts_asof(base_lsn, &track))
			era = track ? 1 : 0;
	}
	if (era == 0)
	{
		memset(pages, 0, np * BLCKSZ);
		for (int64 p = 0; p < np; p++)
		{
			present[p] = false;
			base_found[p] = false;
			zeroed[p] = false;
			truncated[p] = true;
		}
		era_reset = true;
		deactivated = true;
	}

	if (target_lsn == base_lsn)
		goto check_required_pages;

	pd = palloc0(sizeof(*pd));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
		ReplOriginId origin;
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
					if (pageseg != cut_seg &&
						ps_commit_ts_page_precedes(pageseg * SLRU_PAGES_PER_SEGMENT,
										cut_seg * SLRU_PAGES_PER_SEGMENT))
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

				/*
				 * XLOG_PARAMETER_CHANGE fires for ANY changed parameter
				 * and carries the CURRENT track_commit_timestamp value,
				 * not a transition flag.  Only real transitions are era
				 * boundaries (see the single-page applier for the full
				 * case analysis):
				 *
				 * - track=false: state is off from here; wipe (matches
				 *   DeactivateCommitTs deleting every segment; an
				 *   off->off repeat wipes an empty state, harmless).
				 *
				 * - track=true after an in-window false: real activation;
				 *   wipe (defends against era-crossing bytes the base
				 *   snapshot carried) and mark the caller's horizon page
				 *   zero-present -- ActivateCommitTs zeroes the
				 *   nextXid-at-activation page WITHOUT WAL, and a correct
				 *   caller passes oldest_xid = that nextXid.
				 *
				 * - track=true with no in-window false: an unrelated
				 *   restart while active (keep everything), or an
				 *   activation whose deactivation predates the window
				 *   (the base is empty; only the silent-zero horizon-page
				 *   marking is needed, and only where no base exists).
				 */
				if (!xlrec.track_commit_timestamp)
				{
					memset(pages, 0, np * BLCKSZ);
					for (int64 p = 0; p < np; p++)
					{
						present[p] = false;
						base_found[p] = false;
						zeroed[p] = false;
						truncated[p] = true;
					}
					era_reset = true;
					deactivated = true;
					era = 0;
				}
				else if (era == 0)
				{
					/*
					 * A real off->on transition (an in-window false, or
					 * the base's control image said off).  Wipe, mark the
					 * activation's silently-zeroed horizon page (req_lo;
					 * the seed entry point resolves its xid, deriving it
					 * from the toggle-time control image when the caller
					 * has none), and remember the era is ON: a later true
					 * record with no intervening false is an unrelated
					 * restart and must not wipe again.
					 */
					memset(pages, 0, np * BLCKSZ);
					for (int64 p = 0; p < np; p++)
					{
						present[p] = false;
						base_found[p] = false;
						zeroed[p] = false;
						truncated[p] = true;
					}
					era_reset = true;
					deactivated = false;
					era = 1;
					if (req_lo >= page_lo && req_lo <= page_hi)
					{
						idx = req_lo - page_lo;
						present[idx] = true;
						zeroed[idx] = true;
						truncated[idx] = false;
					}
								}
				else if (era == -1)
					ereport(ERROR,
							(errmsg("pagestore: cannot replay a track_commit_timestamp record: the commit-ts era at the base is unknown (no readable control image)")));
			}
			continue;
		}
		else if (rmid != RM_XACT_ID)
			continue;

		info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
		if (info != XLOG_XACT_COMMIT && info != XLOG_XACT_COMMIT_PREPARED)
			continue;
		if (era == 0)
			continue;			/* commit-ts is off: nothing was recorded */
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
		 !ps_wal_reaches(target_lsn)))
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot reconstruct commit-ts as of %X/%08X",
						LSN_FORMAT_ARGS(target_lsn))));
	if (deactivated)
		ereport(ERROR,
				(errmsg("pagestore: track_commit_timestamp is off as of the target LSN"),
				 errhint("An inactive fork inherits an empty pg_commit_ts; seed it with a non-normal horizon.")));

check_required_pages:
	for (int64 pageno = req_lo; pageno <= req_hi; pageno++)
	{
		int64		idx = pageno - page_lo;

		if (truncated[idx])
			ereport(ERROR,
					era_reset
					? errmsg("pagestore: commit-ts page %lld does not exist in the era active as of the target LSN (horizon inconsistent with the WAL's activation)",
							 (long long) pageno)
					: errmsg("pagestore: requested commit-ts page %lld was truncated before the target LSN",
							 (long long) pageno));
		if (!base_found[idx] && !zeroed[idx])
			ereport(ERROR,
					(errmsg("pagestore: base commit-ts snapshot for page %lld is absent at %X/%08X",
							(long long) pageno, LSN_FORMAT_ARGS(base_lsn))));
	}

	/* reader/pd are never allocated when (base, target] was empty */
	if (reader)
		XLogReaderFree(reader);
	if (pd)
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
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
			cutoff = (xlrec.oldestMulti == FirstMultiXactId) ? MaxMultiXactId
				: xlrec.oldestMulti - 1;
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
		 !ps_wal_reaches(target_lsn)))
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

	/* reader/pd are never allocated when (base, target] was empty */
	if (reader)
		XLogReaderFree(reader);
	if (pd)
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
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
												xlrec.oldestOffset))
				{
					present[p] = false;
					truncated[p] = true;
				}
			}
		}
	}

	if (scanned < target_lsn && target_lsn != PG_UINT64_MAX &&
		(XLogRecPtrIsInvalid(readfrom) || errm != NULL ||
		 !ps_wal_reaches(target_lsn)))
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

	/* reader/pd are never allocated when (base, target] was empty */
	if (reader)
		XLogReaderFree(reader);
	if (pd)
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

/*
 * Segment file name under an SLRU dir, mirroring slru.c's SlruFileName():
 * pg_multixact/members uses 15-hex "long" segment names now that
 * MultiXactOffset is 64-bit; every other in-scope SLRU keeps the short
 * (4-6 hex, "%04X"-minimum) form.
 */
static int
ps_slru_seg_path(char *buf, size_t buflen, const char *dir, int64 segno,
				 bool long_names)
{
	if (long_names)
		return snprintf(buf, buflen, "%s/%015" PRIX64, dir, segno);
	return snprintf(buf, buflen, "%s/%04X", dir, (unsigned int) segno);
}

static void
pagestore_write_zero_slru_page(const char *slru_dir, const char *label,
							   int64 pageno, bool long_names)
{
	char		segpath[MAXPGPATH];
	char		zerobuf[BLCKSZ];
	int64		first = (pageno / SLRU_PAGES_PER_SEGMENT) * SLRU_PAGES_PER_SEGMENT;
	int			pathlen;
	int			fd;

	pathlen = ps_slru_seg_path(segpath, sizeof(segpath), slru_dir,
							   pageno / SLRU_PAGES_PER_SEGMENT, long_names);
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
						  const char *label, bool publish_dir,
						  bool long_seg_names)
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

		pathlen = ps_slru_seg_path(segpath, sizeof(segpath),
								   publish_dir ? stagedir : dstdir, seg,
								   long_seg_names);
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
 * count.  Commit-ts and multixact have matching seeders below, and
 * pagestore_seed_branch_slrus()/pagestore_prepare_branch() drive all three.
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
				seg,
				p;
	bool		wraps;
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
	wraps = next_xid < oldest_xid;
	/* XID 3 has not extended page zero yet.  A horizon ending at 3 includes
	 * the pre-wrap run only; requiring a page-zero base image would be wrong. */
	if (wraps && next_xid == FirstNormalTransactionId)
		page_hi = -1;
	if (wraps)
		np = (int) ((PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE - page_lo + 1) +
					page_hi + 1);
	else
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
		int64			physical_page = wraps && page_lo + p >
			PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE ?
			p - (PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE - page_lo + 1) : page_lo + p;

		established[p] = pagestore_localsvc_obj_read_at(PS_KLASS_SLRU, &key,
													   (BlockNumber) physical_page,
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
								XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
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
		readfrom = XLogFindNextRecord(reader, readfrom, &errm);
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
				int			idx;

				memcpy(&zp, XLogRecGetData(reader), sizeof(zp));
				idx = ps_clog_seed_page_index(zp, page_lo, page_hi, wraps);

				if (idx >= 0)
				{
					memset(pages + idx * BLCKSZ, 0, BLCKSZ);
					established[idx] = true;
				}
			}
			else if (cinfo == CLOG_TRUNCATE)
			{
				xl_clog_truncate xlrec;
				int64		cut_seg;

				memcpy(&xlrec, XLogRecGetData(reader), sizeof(xlrec));
				/* Truncation removes whole segments *before* the cutoff segment.
				 * page_lo is segment-aligned, so a cutoff later within that same
				 * segment leaves the oldest seeded segment intact.  Compare segment
				 * starts with CLOG's modular page ordering for wrapped horizons. */
				cut_seg = xlrec.pageno / SLRU_PAGES_PER_SEGMENT;
				if (cut_seg != seg_lo &&
					ps_clog_page_precedes(seg_lo * SLRU_PAGES_PER_SEGMENT,
									  cut_seg * SLRU_PAGES_PER_SEGMENT))
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
			ps_clog_seed_set(pages, page_lo, page_hi, wraps, xid, status);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_clog_seed_set(pages, page_lo, page_hi, wraps, parsed.subxacts[i], status);
		}
		else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
		{
			xl_xact_parsed_abort parsed;

			status = TRANSACTION_STATUS_ABORTED;
			ParseAbortRecord(XLogRecGetInfo(reader),
							 (xl_xact_abort *) XLogRecGetData(reader), &parsed);
			xid = (info == XLOG_XACT_ABORT_PREPARED) ? parsed.twophase_xid
				: XLogRecGetXid(reader);
			ps_clog_seed_set(pages, page_lo, page_hi, wraps, xid, status);
			for (int i = 0; i < parsed.nsubxacts; i++)
				ps_clog_seed_set(pages, page_lo, page_hi, wraps, parsed.subxacts[i], status);
		}
	}
	XLogReaderFree(reader);
	pfree(pd);

	/* Fail closed unless the window was fully covered: complete only if the scan
	 * actually started (readfrom valid -- base's WAL segment was readable), ended
	 * cleanly (no decode error), and the readable WAL extends through target.  An
	 * unreadable start (recycled base segment), a decode error (errm), or target beyond
	 * the readable WAL is a short read -- never seed a clog that skipped (base, target].
	 * (ps_wal_reaches is replay-aware locally and probes the shipped log in
	 * store mode, so this also holds on a standby / a no-local-WAL compute.) */
	if (scanned < target && target != PG_UINT64_MAX &&
		(XLogRecPtrIsInvalid(readfrom) || errm != NULL ||
		 !ps_wal_reaches(target)))
		ereport(ERROR,
				(errmsg("pagestore: WAL ends before the target LSN; cannot seed clog as of %X/%08X",
						LSN_FORMAT_ARGS(target))));
	for (p = 0; p < np; p++)
		if (!established[p])
			ereport(ERROR,
					(errmsg("pagestore: base clog snapshot for page %lld is absent at %X/%08X",
					(long long) (wraps && page_lo + p >
					PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE ?
					p - (PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE - page_lo + 1) : page_lo + p),
					LSN_FORMAT_ARGS(base))));

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

	seg = -1;
	for (p = 0; p < np; p++)
	{
		char		segpath[MAXPGPATH];
		int			fd;
		int64		physical_page = wraps && page_lo + p >
			PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE ?
			p - (PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE - page_lo + 1) : page_lo + p;
		int64		physical_seg = physical_page / SLRU_PAGES_PER_SEGMENT;

		if (physical_seg != seg)
		{
			seg = physical_seg;
			snprintf(segpath, sizeof(segpath), "%s/%04X", stagedir,
					 (unsigned int) seg);
			fd = OpenTransientFilePerm(segpath,
									   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
									   pg_file_create_mode);
			if (fd < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create branch segment \"%s\": %m", segpath)));
			for (;;)
			{
				if (write(fd, pages + p * BLCKSZ, BLCKSZ) != BLCKSZ)
				{
					CloseTransientFile(fd);
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write branch segment \"%s\": %m", segpath)));
				}
				seeded++;
				p++;
				if (p == np)
					break;
				physical_page = wraps && page_lo + p >
					PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE ?
					p - (PG_UINT32_MAX / PS_CLOG_XACTS_PER_PAGE - page_lo + 1) : page_lo + p;
				if (physical_page / SLRU_PAGES_PER_SEGMENT != seg)
					break;
			}
			if (pg_fsync(fd) != 0 || CloseTransientFile(fd) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not finish branch segment \"%s\": %m", segpath)));
			p--;
		}
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
 * Seed a commit-ts horizon which crosses the 32-bit XID/page boundary.  The
 * generic SLRU seeder operates on one physical interval, so materialize the
 * high and low intervals into one private directory and publish it only after
 * both have succeeded.  This preserves the normal all-or-nothing directory
 * swap while keeping each reconstruction WAL pass and page array bounded.
 */
static int64
pagestore_seed_commit_ts_wrapped(const char *target_dir, int64 page_lo,
							 int64 page_hi, bool seed_low,
							 XLogRecPtr base, XLogRecPtr target)
{
	char		staging_root[MAXPGPATH];
	char		dstdir[MAXPGPATH];
	char		stagedir[MAXPGPATH];
	int			pathlen;
	int64		seeded;
	int64		max_page = PG_UINT32_MAX / PS_CTS_XACTS_PER_PAGE;

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));
	pathlen = snprintf(staging_root, sizeof(staging_root),
					   "%s/pg_commit_ts.seed.tmp", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, staging_root);
	pathlen = snprintf(dstdir, sizeof(dstdir), "%s/pg_commit_ts", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, dstdir);
	pathlen = snprintf(stagedir, sizeof(stagedir), "%s/pg_commit_ts", staging_root);
	PS_CHECK_PATH_FORMAT(pathlen, stagedir);
	if (access(staging_root, F_OK) == 0 && !rmtree(staging_root, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear branch commit-ts staging dir \"%s\": %m",
						staging_root)));
	if (MakePGDirectory(staging_root) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch commit-ts staging dir \"%s\": %m",
						staging_root)));

	seeded = pagestore_seed_slru_pages(staging_root, "pg_commit_ts",
								 page_lo, max_page,
								 ps_commit_ts_seed_reconstruct_range,
								 base, target, "commit-ts", false, false);
	if (seed_low)
		seeded += pagestore_seed_slru_pages(staging_root, "pg_commit_ts",
								  0, page_hi,
								  ps_commit_ts_seed_reconstruct_range,
								  base, target, "commit-ts", false, false);
	fsync_fname(stagedir, true);
	if (access(dstdir, F_OK) == 0 && !rmtree(dstdir, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove existing commit-ts dir \"%s\": %m", dstdir)));
	if (rename(stagedir, dstdir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish branch commit-ts dir \"%s\": %m", dstdir)));
	(void) rmdir(staging_root);
	fsync_fname(target_dir, true);
	return seeded;
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
	if (!TransactionIdIsNormal(oldest_xid) && TransactionIdIsNormal(next_xid))
	{
		/*
		 * The caller has no horizon: the fork sits between an activation
		 * and the first checkpoint that would publish oldestCommitTsXid
		 * into pg_control.  Derive it from the mirrored control image as
		 * of the target -- the image the toggle shipped carries the
		 * shutdown checkpoint's nextXid, which IS the activation horizon
		 * (the toggle crossed a restart).
		 */
		if (!ps_commit_ts_horizon_asof(target, &oldest_xid))
			ereport(ERROR,
					(errmsg("pagestore: no commit-ts horizon supplied and none derivable from the control image as of the target LSN")));
	}
	if (!TransactionIdIsNormal(oldest_xid) || !TransactionIdIsNormal(next_xid) ||
		TransactionIdFollows(oldest_xid, next_xid))
		ereport(ERROR,
				(errmsg("invalid fork xid horizon [%u, %u)", oldest_xid, next_xid)));
	if (oldest_xid == next_xid)
	{
		char		dstdir[MAXPGPATH];
		char		stagedir[MAXPGPATH];
		int			pathlen;

		/*
		 * Empty horizon: there is nothing to reconstruct, and the page holding
		 * next_xid may not exist in the store yet (e.g. commit-ts was just
		 * activated), so reconstruction would fail closed.  The manifest still
		 * marks pg_commit_ts as required for a normal horizon, so publish the
		 * artifact anyway: a zeroed bootstrap page covering next_xid, the same
		 * shape the multixact seeder uses for its empty horizons.
		 */
		if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create branch dir \"%s\": %m", target_dir)));
		pathlen = snprintf(dstdir, sizeof(dstdir), "%s/pg_commit_ts", target_dir);
		PS_CHECK_PATH_FORMAT(pathlen, dstdir);
		pathlen = snprintf(stagedir, sizeof(stagedir), "%s/pg_commit_ts.tmp",
						   target_dir);
		PS_CHECK_PATH_FORMAT(pathlen, stagedir);
		if (access(stagedir, F_OK) == 0 && !rmtree(stagedir, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not clear branch staging dir \"%s\"", stagedir)));
		if (MakePGDirectory(stagedir) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create branch staging dir \"%s\": %m",
							stagedir)));
		pagestore_write_zero_slru_page(stagedir, "commit-ts",
									   (int64) next_xid / PS_CTS_XACTS_PER_PAGE,
									   false);
		if (access(dstdir, F_OK) == 0 && !rmtree(dstdir, true))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove existing commit-ts dir \"%s\"", dstdir)));
		if (rename(stagedir, dstdir) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not publish branch commit-ts dir \"%s\": %m",
							dstdir)));
		fsync_fname(target_dir, true);
		PG_RETURN_INT64(1);
	}
	page_lo = ((int64) oldest_xid / PS_CTS_XACTS_PER_PAGE);
	page_hi = ((int64) (next_xid - 1)) / PS_CTS_XACTS_PER_PAGE;
	if (next_xid < oldest_xid)
		PG_RETURN_INT64(pagestore_seed_commit_ts_wrapped(target_dir, page_lo,
													 page_hi,
													 next_xid != FirstNormalTransactionId,
													 base, target));

	PG_RETURN_INT64(pagestore_seed_slru_pages(target_dir, "pg_commit_ts",
											 page_lo, page_hi,
											 ps_commit_ts_seed_reconstruct_range,
											 base, target, "commit-ts", true,
											 false));
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
	/*
	 * MultiXactOffset is 64-bit and monotonic (it no longer wraps), so any
	 * non-negative ordered horizon is valid; a long-lived cluster can exceed
	 * 2^32 members.  The bigint SQL argument bounds it at INT64_MAX.
	 */
	if (oldest_member < 0 || next_member < 0 || oldest_member > next_member)
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
													base, target, "multixact offsets", false,
													false);
		}
		else
		{
			off_page_lo = (int64) oldest_multi / PS_MXOFF_PER_PAGE;
			off_page_hi = (int64) MaxMultiXactId / PS_MXOFF_PER_PAGE;
			off_seeded += pagestore_seed_slru_pages(mxstage, "offsets",
													off_page_lo, off_page_hi,
													ps_mxoff_seed_reconstruct_range,
													base, target, "multixact offsets", false,
													false);
			if (next_multi > FirstMultiXactId)
			{
				off_page_lo = (int64) FirstMultiXactId / PS_MXOFF_PER_PAGE;
				off_page_hi = (int64) (next_multi - 1) / PS_MXOFF_PER_PAGE;
				if (next_multi % PS_MXOFF_PER_PAGE == 0)
					off_page_hi++;
				off_seeded += pagestore_seed_slru_pages(mxstage, "offsets",
													off_page_lo, off_page_hi,
													ps_mxoff_seed_reconstruct_range,
													base, target, "multixact offsets", false,
													false);
			}
			else
			{
				off_page_lo = (int64) next_multi / PS_MXOFF_PER_PAGE;
				pagestore_write_zero_slru_page(offdir, "multixact offsets",
											   off_page_lo, false);
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
									   0, false);
		pagestore_write_zero_slru_page(offdir, "multixact offsets",
									   off_page_lo, false);
		off_seeded += (off_page_lo == 0 ? 1 : 2);
	}
	if (next_member != oldest_member)
	{
		/* 64-bit member offsets are monotonic: the horizon never wraps */
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
												base, target, "multixact members", false,
												true);
	}
	else
	{
		mem_page_lo = next_member / PS_MXMEMB_PER_PAGE;
		pagestore_write_zero_slru_page(memdir, "multixact members",
									   0, true);
		pagestore_write_zero_slru_page(memdir, "multixact members",
									   mem_page_lo, true);
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
	/*
	 * MultiXactOffset is 64-bit and monotonic (it no longer wraps), so any
	 * non-negative ordered horizon is valid; a long-lived cluster can exceed
	 * 2^32 members.  The bigint SQL argument bounds it at INT64_MAX.
	 */
	if (oldest_member < 0 || next_member < 0 || oldest_member > next_member)
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
		else
		{
			char		emptycts[MAXPGPATH];
			int			ctslen;

			/*
			 * Commit-ts was never active at the fork, so the branch's fork
			 * state is an empty pg_commit_ts.  Stage one anyway so the
			 * publish below replaces whatever a reused target dir may still
			 * carry from an earlier prepare or cluster life.
			 */
			ctslen = snprintf(emptycts, sizeof(emptycts), "%s/pg_commit_ts",
							  staging_root);
			PS_CHECK_PATH_FORMAT(ctslen, emptycts);
			if (MakePGDirectory(emptycts) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create branch commit-ts staging dir \"%s\": %m",
								emptycts)));
		}

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
	XLogRecPtr	base = PG_GETARG_LSN(1);
	XLogRecPtr	target = PG_GETARG_LSN(2);
	TransactionId oldest_xid = PG_GETARG_TRANSACTIONID(3);
	TransactionId next_xid = PG_GETARG_TRANSACTIONID(4);
	TransactionId oldest_commit_ts_xid = PG_GETARG_TRANSACTIONID(5);
	TransactionId next_commit_ts_xid = PG_GETARG_TRANSACTIONID(6);

	/*
	 * The normalizer may read the mirrored control image (daemon IPC), so
	 * the privilege and backend gates must run first -- the impl repeats
	 * them, but by then a non-superuser would already have touched the
	 * daemon.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to seed branch SLRUs")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));

	ps_commit_ts_normalize_horizons(target, next_xid,
									&oldest_commit_ts_xid,
									&next_commit_ts_xid);

	PG_RETURN_INT64(pagestore_seed_branch_slrus_impl(target_dir,
													 base, target,
													 oldest_xid, next_xid,
													 oldest_commit_ts_xid,
													 next_commit_ts_xid,
													 PG_GETARG_TRANSACTIONID(7),
													 PG_GETARG_TRANSACTIONID(8),
													 PG_GETARG_INT64(9),
													 PG_GETARG_INT64(10)));
}

#define PAGESTORE_MANIFEST_MAXLEN 8192

static void
pagestore_publish_artifact(const char *target_dir, const char *filename,
						   const char *kind, const char *contents,
						   int contents_len)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	int			len;
	int			fd;
	int			done = 0;

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create %s artifact dir \"%s\": %m", kind,
						target_dir)));
	len = snprintf(path, sizeof(path), "%s/%s", target_dir, filename);
	PS_CHECK_PATH_FORMAT(len, path);
	len = snprintf(tmppath, sizeof(tmppath), "%s/%s.tmp.%ld", target_dir,
				   filename, (long) MyProcPid);
	PS_CHECK_PATH_FORMAT(len, tmppath);
	if (access(tmppath, F_OK) == 0 && unlink(tmppath) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear stale %s temp file \"%s\": %m",
						kind, tmppath)));
	fd = OpenTransientFilePerm(tmppath,
						   O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
						   pg_file_create_mode);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create %s \"%s\": %m", kind,
						tmppath)));
	while (done < contents_len)
	{
		ssize_t		written;

		errno = 0;
		written = write(fd, contents + done, contents_len - done);
		if (written <= 0)
		{
			if (written == 0)
				errno = ENOSPC;
			CloseTransientFile(fd);
			(void) unlink(tmppath);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write %s \"%s\": %m", kind,
							tmppath)));
		}
		done += written;
	}
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		(void) unlink(tmppath);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync %s \"%s\": %m", kind,
						tmppath)));
	}
	if (CloseTransientFile(fd) != 0)
	{
		(void) unlink(tmppath);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close %s \"%s\": %m", kind,
						tmppath)));
	}
	if (durable_rename(tmppath, path, LOG) != 0)
	{
		(void) unlink(tmppath);
		(void) unlink(path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not durably publish %s \"%s\"", kind,
						path)));
	}
}

#define PAGESTORE_READER_SNAPSHOT_MAGIC UINT32_C(0x50535253)
#define PAGESTORE_READER_SNAPSHOT_FORMAT 1
#define PAGESTORE_READER_SNAPSHOT_FILE "pagestore_reader.snapshot"
#define PAGESTORE_READER_CATALOG_MAGIC UINT32_C(0x50534350)
#define PAGESTORE_READER_CATALOG_FORMAT 1
#define PAGESTORE_READER_CATALOG_FILE "pagestore_reader.catalog"
#define PAGESTORE_READER_HANDOFF_MAGIC UINT32_C(0x50534854)
#define PAGESTORE_READER_HANDOFF_FORMAT 1

typedef struct PagestoreReaderHandoffToken
{
	uint32		magic;
	uint32		format;
	uint32		timeline;
	uint32		reserved;
	uint64		lsn;
} PagestoreReaderHandoffToken;

typedef struct PagestoreReaderSnapshotHeader
{
	uint64		read_lsn;
	uint32		magic;
	uint32		format;
	uint32		timeline;
	uint32		count;
	TransactionId xmin;
	TransactionId xmax;
	pg_crc32c	crc;
	uint32		reserved;
} PagestoreReaderSnapshotHeader;

typedef struct PagestoreReaderSnapshot
{
	PagestoreReaderSnapshotHeader header;
	TransactionId *xids;
} PagestoreReaderSnapshot;

#define PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC UINT32_C(0x5053524D)
#define PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT 2
#define PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT 0
#define PAGESTORE_READER_SNAPSHOT_DATA_OBJECT 1
#define PAGESTORE_READER_SNAPSHOT_READY_OBJECT 2
#define PAGESTORE_READER_RELMAP_OBJECT 3
#define PAGESTORE_READER_DATABASE_BARRIER_OBJECT 4
#define PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS 10000

#define PAGESTORE_READER_RELMAP_MAGIC UINT32_C(0x5053524C)
#define PAGESTORE_READER_RELMAP_FORMAT 1
#define PAGESTORE_READER_RELMAP_MAX_SIZE \
	(BLCKSZ - MAXALIGN(sizeof(PagestoreReaderRelmap)))

typedef struct PagestoreReaderRelmap
{
	uint32		magic;
	uint32		format;
	Oid			dbid;
	Oid			tsid;
	uint32		size;
	pg_crc32c	data_crc;
	pg_crc32c	crc;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} PagestoreReaderRelmap;

static XLogRecPtr pagestore_primed_relmap_lsn = InvalidXLogRecPtr;
static Size pagestore_primed_global_relmap_size = 0;
static Size pagestore_primed_local_relmap_size = 0;
static char pagestore_primed_global_relmap[PAGESTORE_READER_RELMAP_MAX_SIZE];
static char pagestore_primed_local_relmap[PAGESTORE_READER_RELMAP_MAX_SIZE];

typedef struct PagestoreReaderSnapshotReady
{
	PagestoreReaderSnapshotHeader header;
	uint32		block_count;
	uint32		reserved;
	pg_crc32c	crc;
} PagestoreReaderSnapshotReady;

static void
pagestore_reader_snapshot_ready_crc(PagestoreReaderSnapshotReady *ready)
{
	INIT_CRC32C(ready->crc);
	COMP_CRC32C(ready->crc, ready,
				offsetof(PagestoreReaderSnapshotReady, crc));
	FIN_CRC32C(ready->crc);
}

typedef struct PagestoreReaderSnapshotManifest
{
	uint64		read_lsn;
	uint64		artifact_size;
	uint32		magic;
	uint32		format;
	uint32		timeline;
	uint32		block_count;
	pg_crc32c	artifact_crc;
	pg_crc32c	global_relmap_crc;
	pg_crc32c	local_relmap_crc;
	pg_crc32c	crc;
} PagestoreReaderSnapshotManifest;

#define PAGESTORE_READER_DATABASE_BARRIER_MAGIC UINT32_C(0x50535242)
#define PAGESTORE_READER_DATABASE_BARRIER_FORMAT 2
typedef struct PagestoreReaderDatabaseEntry
{
	Oid			database_oid;
	Oid			tablespace_oid;
} PagestoreReaderDatabaseEntry;

#define PAGESTORE_READER_DATABASE_MAX ((BLCKSZ - 32) / \
									 sizeof(PagestoreReaderDatabaseEntry))
typedef struct PagestoreReaderDatabaseBarrier
{
	uint64		read_lsn;
	uint32		magic;
	uint32		format;
	uint32		timeline;
	uint32		database_count;
	PagestoreReaderDatabaseEntry databases[PAGESTORE_READER_DATABASE_MAX];
	pg_crc32c	crc;
	uint32		reserved;
} PagestoreReaderDatabaseBarrier;
StaticAssertDecl(sizeof(PagestoreReaderDatabaseBarrier) <= BLCKSZ,
				 "reader database barrier must fit in one pagestore page");

typedef struct PagestoreReaderCatalogProvenance
{
	uint64		read_lsn;
	uint64		system_identifier;
	uint32		magic;
	uint32		format;
	uint32		timeline;
	uint32		reserved;
	pg_crc32c	crc;
	uint32		padding;
} PagestoreReaderCatalogProvenance;

static bool pagestore_pread_exact(int fd, void *buf, Size size, off_t offset);

static pg_crc32c
pagestore_reader_relmap_crc(const char *dir)
{
	char		path[MAXPGPATH];
	char		buf[1024];
	pg_crc32c	crc;
	int			fd;
	ssize_t		nread;
	int			len;

	len = snprintf(path, sizeof(path), "%s/pg_filenode.map", dir);
	PS_CHECK_PATH_FORMAT(len, path);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open relation map \"%s\": %m", path)));
	INIT_CRC32C(crc);
	while ((nread = read(fd, buf, sizeof(buf))) > 0)
		COMP_CRC32C(crc, buf, nread);
	if (nread < 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read relation map \"%s\": %m", path)));
	}
	CloseTransientFile(fd);
	FIN_CRC32C(crc);
	return crc;
}

static pg_crc32c
pagestore_prepared_reader_relmap_crc(const char *dir, bool shared)
{
	char		mapdir[MAXPGPATH];
	int			len;

	if (shared)
		len = snprintf(mapdir, sizeof(mapdir), "%s/relmaps/global", dir);
	else
		len = snprintf(mapdir, sizeof(mapdir), "%s/relmaps/%u", dir,
					   MyDatabaseId);
	PS_CHECK_PATH_FORMAT(len, mapdir);
	return pagestore_reader_relmap_crc(mapdir);
}

static void
pagestore_reader_catalog_crc(PagestoreReaderCatalogProvenance *provenance)
{
	INIT_CRC32C(provenance->crc);
	COMP_CRC32C(provenance->crc, provenance,
				offsetof(PagestoreReaderCatalogProvenance, crc));
	FIN_CRC32C(provenance->crc);
}

static void
pagestore_load_reader_catalog_provenance(const char *dir, uint32 timeline,
										 XLogRecPtr read_lsn,
										 uint64 system_identifier, int elevel)
{
	PagestoreReaderCatalogProvenance provenance;
	PagestoreReaderCatalogProvenance checked;
	char		path[MAXPGPATH];
	struct stat st;
	int			fd;
	int			len;

	len = snprintf(path, sizeof(path), "%s/%s", dir,
				   PAGESTORE_READER_CATALOG_FILE);
	PS_CHECK_PATH_FORMAT(len, path);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open reader catalog provenance \"%s\": %m", path)));
	if (fstat(fd, &st) != 0 ||
		st.st_size != (off_t) sizeof(provenance) ||
		!pagestore_pread_exact(fd, &provenance, sizeof(provenance), 0))
	{
		CloseTransientFile(fd);
		ereport(elevel,
				(errmsg("reader catalog provenance \"%s\" has an invalid size", path)));
	}
	CloseTransientFile(fd);
	if (provenance.magic != PAGESTORE_READER_CATALOG_MAGIC ||
		provenance.format != PAGESTORE_READER_CATALOG_FORMAT ||
		provenance.reserved != 0 || provenance.padding != 0 ||
		provenance.timeline != timeline || provenance.read_lsn != read_lsn ||
		provenance.system_identifier != system_identifier)
		ereport(elevel,
				(errmsg("reader catalog provenance \"%s\" has an invalid identity", path)));
	checked = provenance;
	pagestore_reader_catalog_crc(&checked);
	if (!EQ_CRC32C(checked.crc, provenance.crc))
		ereport(elevel,
				(errmsg("reader catalog provenance \"%s\" has an invalid checksum", path)));
}

static void
pagestore_write_reader_catalog_provenance(const char *target_dir,
										  uint32 timeline,
										  XLogRecPtr read_lsn,
										  uint64 system_identifier)
{
	PagestoreReaderCatalogProvenance provenance;

	memset(&provenance, 0, sizeof(provenance));
	provenance.read_lsn = read_lsn;
	provenance.system_identifier = system_identifier;
	provenance.magic = PAGESTORE_READER_CATALOG_MAGIC;
	provenance.format = PAGESTORE_READER_CATALOG_FORMAT;
	provenance.timeline = timeline;
	pagestore_reader_catalog_crc(&provenance);
	pagestore_publish_artifact(target_dir, PAGESTORE_READER_CATALOG_FILE,
							   "reader catalog provenance",
							   (char *) &provenance, sizeof(provenance));
}

static bool
pagestore_pread_exact(int fd, void *buf, Size size, off_t offset)
{
	Size		done = 0;

	while (done < size)
	{
		ssize_t		got;

		errno = 0;
		got = pread(fd, (char *) buf + done, size - done, offset + done);
		if (got < 0 && errno == EINTR)
			continue;
		if (got <= 0)
			return false;
		done += got;
	}
	return true;
}

static void
pagestore_reader_snapshot_crc(PagestoreReaderSnapshotHeader *header,
							  const TransactionId *xids)
{
	INIT_CRC32C(header->crc);
	COMP_CRC32C(header->crc, header,
				offsetof(PagestoreReaderSnapshotHeader, crc));
	if (header->count > 0)
		COMP_CRC32C(header->crc, xids,
					header->count * sizeof(TransactionId));
	FIN_CRC32C(header->crc);
}

static void
pagestore_reader_snapshot_manifest_crc(PagestoreReaderSnapshotManifest *manifest)
{
	INIT_CRC32C(manifest->crc);
	COMP_CRC32C(manifest->crc, manifest,
				offsetof(PagestoreReaderSnapshotManifest, crc));
	FIN_CRC32C(manifest->crc);
}

static PageStoreRelKey
pagestore_reader_snapshot_key(uint32 object, Oid dbid)
{
	PageStoreRelKey key = {0};

	key.dbOid = dbid;
	key.relNumber = object;
	return key;
}

static bool
pagestore_database_reader_manifest_ready(Oid dbid, XLogRecPtr read_lsn)
{
	PagestoreReaderSnapshotManifest manifest;
	PagestoreReaderSnapshotManifest checked;
	PageStoreRelKey key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, dbid);
	char		page[BLCKSZ];
	uint64		resolved = 0;

	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) read_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || resolved != read_lsn)
		return false;
	memcpy(&manifest, page, sizeof(manifest));
	checked = manifest;
	pagestore_reader_snapshot_manifest_crc(&checked);
	return manifest.magic == PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC &&
		manifest.format == PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT &&
		manifest.timeline == pagestore_localsvc_timeline() &&
		manifest.read_lsn == read_lsn && manifest.block_count != 0 &&
		EQ_CRC32C(manifest.crc, checked.crc);
}

static XLogRecPtr
pagestore_latest_reader_snapshot_ready(void)
{
	PagestoreReaderSnapshotReady ready;
	PagestoreReaderSnapshotReady checked;
	PageStoreRelKey key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_READY_OBJECT, InvalidOid);
	char		page[BLCKSZ];
	uint64		resolved = 0;

	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, PG_UINT64_MAX, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS))
		return InvalidXLogRecPtr;
	memcpy(&ready, page, sizeof(ready));
	checked = ready;
	pagestore_reader_snapshot_ready_crc(&checked);
	if (ready.header.magic != PAGESTORE_READER_SNAPSHOT_MAGIC ||
		ready.header.format != PAGESTORE_READER_SNAPSHOT_FORMAT ||
		ready.header.timeline != pagestore_localsvc_timeline() ||
		ready.header.read_lsn != (XLogRecPtr) resolved ||
		ready.block_count == 0 || ready.reserved != 0 ||
		!EQ_CRC32C(ready.crc, checked.crc))
		return InvalidXLogRecPtr;
	return (XLogRecPtr) resolved;
}

static bool
pagestore_reader_database_dir_valid(const char *mapdir, Oid dboid,
									XLogRecPtr read_lsn, pg_crc32c global_crc)
{
	PagestoreReaderSnapshotManifest manifest;
	PagestoreReaderSnapshotManifest checked;
	PageStoreRelKey key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, dboid);
	char page[BLCKSZ];
	uint64 manifest_lsn = 0;

	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) read_lsn, page, &manifest_lsn,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || manifest_lsn != read_lsn)
		return false;
	memcpy(&manifest, page, sizeof(manifest));
	checked = manifest;
	pagestore_reader_snapshot_manifest_crc(&checked);
	return manifest.magic == PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC &&
		manifest.format == PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT &&
		manifest.timeline == pagestore_localsvc_timeline() &&
		manifest.read_lsn == read_lsn &&
		EQ_CRC32C(manifest.crc, checked.crc) &&
		EQ_CRC32C(manifest.global_relmap_crc, global_crc) &&
		EQ_CRC32C(manifest.local_relmap_crc,
					 pagestore_reader_relmap_crc(mapdir));
}

static bool
pagestore_reader_database_barrier_valid(XLogRecPtr read_lsn)
{
	PagestoreReaderDatabaseBarrier barrier;
	PagestoreReaderDatabaseBarrier checked;
	PageStoreRelKey key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_DATABASE_BARRIER_OBJECT, InvalidOid);
	char		page[BLCKSZ];
	uint64		resolved = 0;

	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) read_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || resolved != read_lsn)
		return false;
	memcpy(&barrier, page, sizeof(barrier));
	checked = barrier;
	INIT_CRC32C(checked.crc);
	COMP_CRC32C(checked.crc, &checked,
				offsetof(PagestoreReaderDatabaseBarrier, crc));
	FIN_CRC32C(checked.crc);
	if (!(barrier.magic == PAGESTORE_READER_DATABASE_BARRIER_MAGIC &&
		barrier.format == PAGESTORE_READER_DATABASE_BARRIER_FORMAT &&
		barrier.timeline == pagestore_localsvc_timeline() &&
		barrier.read_lsn == read_lsn && barrier.database_count != 0 &&
		barrier.database_count <= PAGESTORE_READER_DATABASE_MAX &&
		barrier.reserved == 0 &&
		EQ_CRC32C(barrier.crc, checked.crc)))
		return false;

	/*
	 * Validate exactly the connectable database set frozen by the writer at R.
	 * Do not infer membership from numeric PGDATA directories: template0 is
	 * deliberately non-connectable, and default tablespaces may live elsewhere.
	 */
	{
		pg_crc32c	global_crc;

		global_crc = pagestore_reader_relmap_crc("global");
		for (uint32 i = 0; i < barrier.database_count; i++)
		{
			PagestoreReaderDatabaseEntry *entry = &barrier.databases[i];
			char *mapdir;

			if (!OidIsValid(entry->database_oid) ||
				!OidIsValid(entry->tablespace_oid) ||
				(i > 0 && barrier.databases[i - 1].database_oid >=
				 entry->database_oid))
				return false;
			mapdir = GetDatabasePath(entry->database_oid,
								 entry->tablespace_oid);
			if (!pagestore_reader_database_dir_valid(mapdir,
					entry->database_oid, read_lsn, global_crc))
			{
				pfree(mapdir);
				return false;
			}
			pfree(mapdir);
		}
		return true;
	}
}

static void
pagestore_publish_reader_database_barrier(List *databases, XLogRecPtr read_lsn)
{
	PagestoreReaderDatabaseBarrier barrier;
	PageStoreRelKey key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_DATABASE_BARRIER_OBJECT, InvalidOid);
	char		page[BLCKSZ];
	BlockNumber nblocks;

	memset(&barrier, 0, sizeof(barrier));
	if (list_length(databases) > PAGESTORE_READER_DATABASE_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many databases for a pagestore reader barrier")));
	barrier.read_lsn = read_lsn;
	barrier.magic = PAGESTORE_READER_DATABASE_BARRIER_MAGIC;
	barrier.format = PAGESTORE_READER_DATABASE_BARRIER_FORMAT;
	barrier.timeline = pagestore_localsvc_timeline();
	barrier.database_count = list_length(databases);
	{
		uint32 i = 0;
		ListCell *lc;

		foreach(lc, databases)
			barrier.databases[i++] =
				*((PagestoreReaderDatabaseEntry *) lfirst(lc));
	}
	INIT_CRC32C(barrier.crc);
	COMP_CRC32C(barrier.crc, &barrier,
				offsetof(PagestoreReaderDatabaseBarrier, crc));
	FIN_CRC32C(barrier.crc);
	memset(page, 0, sizeof(page));
	memcpy(page, &barrier, sizeof(barrier));
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
		&key, 0, page, (uint64) read_lsn, nblocks,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_store_sync_timeout(
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
}

static Size
pagestore_read_reader_relmap(const char *dir, char *data, Size capacity)
{
	char		path[MAXPGPATH];
	struct stat st;
	int			fd;
	int			len;

	len = snprintf(path, sizeof(path), "%s/pg_filenode.map", dir);
	PS_CHECK_PATH_FORMAT(len, path);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size <= 0 ||
		st.st_size > (off_t) capacity ||
		!pagestore_pread_exact(fd, data, (Size) st.st_size, 0))
	{
		if (fd >= 0)
			CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read relation map \"%s\": %m", path)));
	}
	CloseTransientFile(fd);
	return (Size) st.st_size;
}

static void
pagestore_publish_reader_relmap(Oid dbid, Oid tsid, XLogRecPtr lsn,
								const char *data, Size size)
{
	PagestoreReaderRelmap *artifact;
	PageStoreRelKey key;
	char		page[BLCKSZ];
	BlockNumber nblocks;

	memset(page, 0, sizeof(page));
	artifact = (PagestoreReaderRelmap *) page;
	artifact->magic = PAGESTORE_READER_RELMAP_MAGIC;
	artifact->format = PAGESTORE_READER_RELMAP_FORMAT;
	artifact->dbid = dbid;
	artifact->tsid = tsid;
	artifact->size = size;
	INIT_CRC32C(artifact->data_crc);
	COMP_CRC32C(artifact->data_crc, data, size);
	FIN_CRC32C(artifact->data_crc);
	memcpy(artifact->data, data, size);
	INIT_CRC32C(artifact->crc);
	COMP_CRC32C(artifact->crc, artifact,
				  offsetof(PagestoreReaderRelmap, crc));
	FIN_CRC32C(artifact->crc);
	key = pagestore_reader_snapshot_key(PAGESTORE_READER_RELMAP_OBJECT, dbid);
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
		&key, 0, page, (uint64) lsn, nblocks,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
}

static bool pagestore_load_reader_relmap(Oid dbid, Oid tsid,
										 XLogRecPtr read_lsn,
										 pg_crc32c *data_crc);

PG_FUNCTION_INFO_V1(pagestore_prime_reader_relmaps);
Datum
pagestore_prime_reader_relmaps(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn;
	char		global_map[PAGESTORE_READER_RELMAP_MAX_SIZE];
	char		local_map[PAGESTORE_READER_RELMAP_MAX_SIZE];
	Size		global_size;
	Size		local_size;
	bool		changed;
	pg_crc32c	global_crc;
	pg_crc32c	local_crc;
	pg_crc32c	stored_global_crc;
	pg_crc32c	stored_local_crc;

	if (!superuser() || !OidIsValid(MyDatabaseId) || DatabasePath == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("reader relation-map priming requires a database superuser backend")));
	LWLockAcquire(RelationMappingLock, LW_SHARED);
	global_size = pagestore_read_reader_relmap("global", global_map,
										 sizeof(global_map));
	local_size = pagestore_read_reader_relmap(DatabasePath, local_map,
									  sizeof(local_map));
	lsn = GetXLogInsertRecPtr();
	LWLockRelease(RelationMappingLock);
	changed = global_size != pagestore_primed_global_relmap_size ||
		local_size != pagestore_primed_local_relmap_size ||
		memcmp(global_map, pagestore_primed_global_relmap, global_size) != 0 ||
		memcmp(local_map, pagestore_primed_local_relmap, local_size) != 0;
	if (!changed)
		PG_RETURN_LSN(pagestore_primed_relmap_lsn);
	INIT_CRC32C(global_crc);
	COMP_CRC32C(global_crc, global_map, global_size);
	FIN_CRC32C(global_crc);
	INIT_CRC32C(local_crc);
	COMP_CRC32C(local_crc, local_map, local_size);
	FIN_CRC32C(local_crc);
	if (pagestore_load_reader_relmap(InvalidOid, GLOBALTABLESPACE_OID,
			PG_UINT64_MAX,
			&stored_global_crc) &&
		pagestore_load_reader_relmap(MyDatabaseId, MyDatabaseTableSpace,
			PG_UINT64_MAX,
			&stored_local_crc) &&
		EQ_CRC32C(global_crc, stored_global_crc) &&
		EQ_CRC32C(local_crc, stored_local_crc))
	{
		memcpy(pagestore_primed_global_relmap, global_map, global_size);
		memcpy(pagestore_primed_local_relmap, local_map, local_size);
		pagestore_primed_global_relmap_size = global_size;
		pagestore_primed_local_relmap_size = local_size;
		pagestore_primed_relmap_lsn = lsn;
		PG_RETURN_LSN(lsn);
	}
	pagestore_publish_reader_relmap(InvalidOid, GLOBALTABLESPACE_OID, lsn,
								 global_map, global_size);
	pagestore_publish_reader_relmap(MyDatabaseId, MyDatabaseTableSpace, lsn,
								 local_map, local_size);
	pagestore_localsvc_store_sync_timeout(
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	memcpy(pagestore_primed_global_relmap, global_map, global_size);
	memcpy(pagestore_primed_local_relmap, local_map, local_size);
	pagestore_primed_global_relmap_size = global_size;
	pagestore_primed_local_relmap_size = local_size;
	pagestore_primed_relmap_lsn = lsn;
	PG_RETURN_LSN(lsn);
}

static bool
pagestore_load_reader_relmap(Oid dbid, Oid tsid, XLogRecPtr read_lsn,
							 pg_crc32c *data_crc)
{
	PagestoreReaderRelmap artifact;
	PagestoreReaderRelmap checked;
	PageStoreRelKey key;
	char		page[BLCKSZ];
	uint64		resolved = 0;
	pg_crc32c	crc;

	key = pagestore_reader_snapshot_key(PAGESTORE_READER_RELMAP_OBJECT, dbid);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) read_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS))
		return false;
	memcpy(&artifact, page, offsetof(PagestoreReaderRelmap, data));
	checked = artifact;
	INIT_CRC32C(checked.crc);
	COMP_CRC32C(checked.crc, &checked,
				offsetof(PagestoreReaderRelmap, crc));
	FIN_CRC32C(checked.crc);
	if (artifact.magic != PAGESTORE_READER_RELMAP_MAGIC ||
		artifact.format != PAGESTORE_READER_RELMAP_FORMAT ||
		artifact.dbid != dbid || artifact.tsid != tsid || artifact.size == 0 ||
		artifact.size > BLCKSZ - offsetof(PagestoreReaderRelmap, data) ||
		!EQ_CRC32C(artifact.crc, checked.crc))
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page + offsetof(PagestoreReaderRelmap, data),
				artifact.size);
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, artifact.data_crc))
		return false;
	*data_crc = artifact.data_crc;
	return true;
}

static void
pagestore_validate_reader_snapshot(PagestoreReaderSnapshot *snapshot,
								   uint32 timeline, XLogRecPtr read_lsn,
								   const char *source, int elevel)
{
	PagestoreReaderSnapshotHeader checked = snapshot->header;
	PagestoreReaderSnapshotHeader *header = &snapshot->header;

	if (header->magic != PAGESTORE_READER_SNAPSHOT_MAGIC ||
		header->format != PAGESTORE_READER_SNAPSHOT_FORMAT ||
		header->reserved != 0 || header->timeline != timeline ||
		header->read_lsn != read_lsn ||
		header->count > MaxAllocSize / sizeof(TransactionId) ||
		!TransactionIdIsNormal(header->xmin) ||
		!TransactionIdIsNormal(header->xmax) ||
		TransactionIdPrecedes(header->xmax, header->xmin))
		ereport(elevel,
				(errmsg("reader snapshot %s has an invalid identity or header",
						source)));
	pagestore_reader_snapshot_crc(&checked, snapshot->xids);
	if (!EQ_CRC32C(checked.crc, header->crc))
		ereport(elevel,
				(errmsg("reader snapshot %s has an invalid checksum", source)));
	for (uint32 i = 0; i < header->count; i++)
	{
		if (!TransactionIdIsNormal(snapshot->xids[i]) ||
			TransactionIdPrecedes(snapshot->xids[i], header->xmin) ||
			!TransactionIdPrecedes(snapshot->xids[i], header->xmax) ||
			(i > 0 && !TransactionIdPrecedes(snapshot->xids[i - 1],
										 snapshot->xids[i])))
			ereport(elevel,
					(errmsg("reader snapshot %s has invalid transaction IDs",
							source)));
	}
}

static void
pagestore_write_reader_snapshot(const char *target_dir, uint32 timeline,
								XLogRecPtr read_lsn,
								TransactionId oldest_xid,
								TransactionId next_xid)
{
	PagestoreReaderSnapshotHeader header;
	TransactionId *xids;
	char		slru_dir[MAXPGPATH];
	char		segpath[MAXPGPATH];
	char		page[BLCKSZ];
	char	   *artifact;
	int			max_count = GetMaxSnapshotSubxidCount();
	int			capacity;
	int			count = 0;
	int			fd = -1;
	int64		loaded_page = -1;
	int64		loaded_seg = -1;
	int			len;

	if (TransactionIdFollows(oldest_xid, next_xid))
		ereport(ERROR,
				(errmsg("reader XID horizon spans wraparound")));
	capacity = Min(1024, max_count);
	if (capacity > MaxAllocSize / sizeof(TransactionId))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("reader snapshot transaction ID capacity is too large")));
	xids = palloc_array(TransactionId, capacity);
	len = snprintf(slru_dir, sizeof(slru_dir), "%s/pg_xact", target_dir);
	PS_CHECK_PATH_FORMAT(len, slru_dir);

	/* CLOG page numbers and transaction IDs both wrap at 2^32. */
	for (TransactionId xid = oldest_xid;
		 !TransactionIdEquals(xid, next_xid);)
	{
		int64		pageno = xid / PS_CLOG_XACTS_PER_PAGE;
		int64		segno = pageno / SLRU_PAGES_PER_SEGMENT;
		int			pgidx;
		int			status;

		if (pageno != loaded_page)
		{
			if (segno != loaded_seg)
			{
				if (fd >= 0)
					CloseTransientFile(fd);
				len = ps_slru_seg_path(segpath, sizeof(segpath), slru_dir,
									segno, false);
				PS_CHECK_PATH_FORMAT(len, segpath);
				fd = OpenTransientFile(segpath, O_RDONLY | PG_BINARY);
				if (fd < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open reader pg_xact segment \"%s\": %m",
									segpath)));
				loaded_seg = segno;
			}
			if (!pagestore_pread_exact(fd, page, BLCKSZ,
									 (pageno % SLRU_PAGES_PER_SEGMENT) * BLCKSZ))
			{
				if (errno == 0)
					errno = EIO;
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read reader pg_xact page from \"%s\": %m",
								segpath)));
			}
			loaded_page = pageno;
		}

		pgidx = xid % PS_CLOG_XACTS_PER_PAGE;
		status = (((unsigned char *) page)[pgidx / PS_CLOG_XACTS_PER_BYTE] >>
				  ((pgidx % PS_CLOG_XACTS_PER_BYTE) * PS_CLOG_BITS_PER_XACT)) &
			PS_CLOG_XACT_BITMASK;
		if (status == TRANSACTION_STATUS_IN_PROGRESS ||
			status == TRANSACTION_STATUS_SUB_COMMITTED)
		{
			if (count == capacity)
			{
				if (capacity > MaxAllocSize / (2 * sizeof(TransactionId)))
				{
					if (fd >= 0)
						CloseTransientFile(fd);
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("reader snapshot is too large to materialize")));
				}
				capacity *= 2;
				xids = repalloc_array(xids, TransactionId, capacity);
			}
			xids[count++] = xid;
		}
		TransactionIdAdvance(xid);
	}
	if (fd >= 0)
		CloseTransientFile(fd);

	memset(&header, 0, sizeof(header));
	header.read_lsn = read_lsn;
	header.magic = PAGESTORE_READER_SNAPSHOT_MAGIC;
	header.format = PAGESTORE_READER_SNAPSHOT_FORMAT;
	header.timeline = timeline;
	header.count = count;
	header.xmin = count > 0 ? xids[0] : next_xid;
	header.xmax = next_xid;
	pagestore_reader_snapshot_crc(&header, xids);

	artifact = palloc(sizeof(header) + count * sizeof(TransactionId));
	memcpy(artifact, &header, sizeof(header));
	if (count > 0)
		memcpy(artifact + sizeof(header), xids,
			   count * sizeof(TransactionId));
	pagestore_publish_artifact(target_dir, PAGESTORE_READER_SNAPSHOT_FILE,
						   "reader snapshot", artifact,
						   sizeof(header) + count * sizeof(TransactionId));
	pfree(artifact);
	pfree(xids);
}

static PagestoreReaderSnapshot *
pagestore_load_reader_snapshot(const char *dir, uint32 timeline,
							   XLogRecPtr read_lsn, MemoryContext context,
							   int elevel)
{
	PagestoreReaderSnapshotHeader header;
	PagestoreReaderSnapshot *snapshot;
	char		path[MAXPGPATH];
	struct stat st;
	int			fd;
	int			len;
	Size		xids_size;

	len = snprintf(path, sizeof(path), "%s/%s", dir,
				   PAGESTORE_READER_SNAPSHOT_FILE);
	PS_CHECK_PATH_FORMAT(len, path);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open reader snapshot \"%s\": %m", path)));
	if (fstat(fd, &st) != 0)
	{
		CloseTransientFile(fd);
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not stat reader snapshot \"%s\": %m", path)));
	}
	if (!pagestore_pread_exact(fd, &header, sizeof(header), 0) ||
		header.magic != PAGESTORE_READER_SNAPSHOT_MAGIC ||
		header.format != PAGESTORE_READER_SNAPSHOT_FORMAT ||
		header.reserved != 0 ||
		header.timeline != timeline || header.read_lsn != read_lsn ||
		header.count > MaxAllocSize / sizeof(TransactionId) ||
		!TransactionIdIsNormal(header.xmin) ||
		!TransactionIdIsNormal(header.xmax) ||
		TransactionIdPrecedes(header.xmax, header.xmin))
	{
		CloseTransientFile(fd);
		ereport(elevel,
				(errmsg("reader snapshot \"%s\" has an invalid identity or header",
						path)));
	}
	xids_size = header.count * sizeof(TransactionId);
	if (st.st_size != (off_t) (sizeof(header) + xids_size))
	{
		CloseTransientFile(fd);
		ereport(elevel,
				(errmsg("reader snapshot \"%s\" has an invalid size", path)));
	}
	snapshot = MemoryContextAlloc(context, sizeof(*snapshot));
	snapshot->header = header;
	snapshot->xids = header.count > 0 ?
		MemoryContextAlloc(context, xids_size) : NULL;
	if (xids_size > 0)
	{
		if (!pagestore_pread_exact(fd, snapshot->xids, xids_size,
								   sizeof(header)))
		{
			CloseTransientFile(fd);
			ereport(elevel,
					(errmsg("could not read reader snapshot \"%s\"", path)));
		}
	}
	CloseTransientFile(fd);
	pagestore_validate_reader_snapshot(snapshot, timeline, read_lsn, path,
									 elevel);
	return snapshot;
}

static BlockNumber
pagestore_publish_reader_snapshot_data(PagestoreReaderSnapshot *snapshot)
{
	PageStoreRelKey data_key;
	char	   *artifact;
	char		page[BLCKSZ];
	Size		xids_size;
	Size		artifact_size;
	BlockNumber block_count;
	BlockNumber nblocks;

	xids_size = snapshot->header.count * sizeof(TransactionId);
	artifact_size = sizeof(snapshot->header) + xids_size;
	block_count = (BlockNumber) ((artifact_size + BLCKSZ - 1) / BLCKSZ);
	artifact = palloc(artifact_size);
	memcpy(artifact, &snapshot->header, sizeof(snapshot->header));
	if (xids_size > 0)
		memcpy(artifact + sizeof(snapshot->header), snapshot->xids, xids_size);

	data_key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_DATA_OBJECT, InvalidOid);
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &data_key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	for (BlockNumber block = 0; block < block_count; block++)
	{
		Size		offset = (Size) block * BLCKSZ;
		Size		chunk = Min((Size) BLCKSZ, artifact_size - offset);

		memset(page, 0, sizeof(page));
		memcpy(page, artifact + offset, chunk);
		pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
			&data_key, block, page,
			(uint64) snapshot->header.read_lsn, nblocks,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
		if (block >= nblocks)
			nblocks = block + 1;
	}
	pagestore_localsvc_store_sync_timeout(
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pfree(artifact);
	return block_count;
}

static void
pagestore_reader_mark_completed(TransactionId xid, TransactionId oldest_xid,
								TransactionId next_xid, uint8 *running)
{
	if (!TransactionIdPrecedes(xid, oldest_xid) &&
		TransactionIdPrecedes(xid, next_xid))
		running[(uint32) (xid - oldest_xid) / 8] |=
			1U << ((uint32) (xid - oldest_xid) % 8);
}

static void
pagestore_reader_mark_completions(XLogRecPtr start_lsn, XLogRecPtr end_lsn,
								  TransactionId oldest_xid,
								  TransactionId next_xid, uint8 *running)
{
	ReadLocalXLogPageNoWaitPrivate *pd = palloc0(sizeof(*pd));
	XLogReaderState *reader;
	XLogRecPtr	readfrom;
	XLogRecPtr	scanned = start_lsn;
	char	   *errm = NULL;

	reader = XLogReaderAllocate(wal_segment_size, NULL,
		XL_ROUTINE(.page_read = &ps_slru_wal_page_read,
				   .segment_open = &wal_segment_open,
				   .segment_close = &wal_segment_close), pd);
	if (reader == NULL)
		ereport(ERROR, (errmsg("could not allocate a reader snapshot WAL reader")));
	{
		XLogSegNo	segno;

		XLByteToSeg(start_lsn, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, readfrom);
	}
	readfrom = XLogFindNextRecord(reader, readfrom, &errm);
	if (!XLogRecPtrIsInvalid(readfrom))
		XLogBeginRead(reader, readfrom);
	while (!XLogRecPtrIsInvalid(readfrom) &&
		   XLogReadRecord(reader, &errm) != NULL)
	{
		uint8		info;
		TransactionId xid;

		if (reader->EndRecPtr > scanned)
			scanned = reader->EndRecPtr;
		if (reader->EndRecPtr > end_lsn)
			break;
		if (reader->EndRecPtr <= start_lsn ||
			XLogRecGetRmid(reader) != RM_XACT_ID)
			continue;
		info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_parsed_commit parsed;

			ParseCommitRecord(XLogRecGetInfo(reader),
				(xl_xact_commit *) XLogRecGetData(reader), &parsed);
			xid = info == XLOG_XACT_COMMIT_PREPARED ? parsed.twophase_xid :
				XLogRecGetXid(reader);
			pagestore_reader_mark_completed(xid, oldest_xid, next_xid, running);
			for (int i = 0; i < parsed.nsubxacts; i++)
				pagestore_reader_mark_completed(parsed.subxacts[i], oldest_xid,
										 next_xid, running);
		}
		else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
		{
			xl_xact_parsed_abort parsed;

			ParseAbortRecord(XLogRecGetInfo(reader),
				(xl_xact_abort *) XLogRecGetData(reader), &parsed);
			xid = info == XLOG_XACT_ABORT_PREPARED ? parsed.twophase_xid :
				XLogRecGetXid(reader);
			pagestore_reader_mark_completed(xid, oldest_xid, next_xid, running);
			for (int i = 0; i < parsed.nsubxacts; i++)
				pagestore_reader_mark_completed(parsed.subxacts[i], oldest_xid,
										 next_xid, running);
		}
	}
	if (!((scanned >= end_lsn) ||
		  (!XLogRecPtrIsInvalid(readfrom) && errm == NULL &&
		   ps_wal_reaches(end_lsn))))
		ereport(ERROR,
				(errmsg("WAL does not cover reader snapshot completion window (%X/%08X, %X/%08X]",
						LSN_FORMAT_ARGS(start_lsn), LSN_FORMAT_ARGS(end_lsn))));
	XLogReaderFree(reader);
	pfree(pd);
}

/* Build the database-independent half of a reader artifact off-checkpoint. */
static bool
pagestore_build_checkpoint_reader_snapshot(const ControlFileData *control)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	MemoryContext work = NULL;
	bool		succeeded = false;
	XLogRecPtr	read_lsn = control->checkPointCopy.redo;
	TransactionId oldest_xid = control->checkPointCopy.oldestXid;
	TransactionId next_xid = XidFromFullTransactionId(control->checkPointCopy.nextXid);

	if (!pagestore_branch_backend_active() || XLogRecPtrIsInvalid(read_lsn) ||
		!TransactionIdIsNormal(oldest_xid) ||
		!TransactionIdIsNormal(next_xid) ||
		TransactionIdFollows(oldest_xid, next_xid))
		return false;

	PG_TRY();
	{
		PagestoreReaderSnapshot snapshot;
		PagestoreReaderSnapshotReady ready;
		PagestoreReaderSnapshotManifest manifest;
		PageStoreRelKey key;
		char		page[BLCKSZ];
		TransactionId *xids;
		TransactionId xid;
		uint8	   *running;
		uint32		horizon;
		Size		bitmap_size;
		uint32		count = 0;
		uint32		scan_iterations = 0;
		XLogRecPtr	scan_end;
		BlockNumber blocks;
		BlockNumber nblocks;

		work = AllocSetContextCreate(CurrentMemoryContext,
			"pagestore checkpoint reader snapshot", ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(work);
		horizon = (uint32) (next_xid - oldest_xid);
		bitmap_size = ((Size) horizon + 7) / 8;
		running = palloc0(bitmap_size);

		/*
		 * Read current status first, then sample scan_end.  A transaction that
		 * finishes concurrently is either observed still running here or has
		 * its completion record included by the subsequent WAL pass.
		 */
		for (xid = oldest_xid; !TransactionIdEquals(xid, next_xid);)
		{
			XLogRecPtr	status_lsn;
			XidStatus	status = TransactionIdGetStatus(xid, &status_lsn);

			if ((++scan_iterations & 0xfff) == 0 && ShutdownRequestPending)
				ereport(ERROR,
						(errcode(ERRCODE_ADMIN_SHUTDOWN),
						 errmsg("terminating automatic reader snapshot scan due to administrator command")));

			if (status == TRANSACTION_STATUS_IN_PROGRESS ||
				status == TRANSACTION_STATUS_SUB_COMMITTED)
				pagestore_reader_mark_completed(xid, oldest_xid, next_xid, running);
			TransactionIdAdvance(xid);
		}
		scan_end = GetXLogInsertRecPtr();
		XLogFlush(scan_end);
		pagestore_reader_mark_completions(read_lsn, scan_end, oldest_xid,
										 next_xid, running);
		scan_iterations = 0;
		for (xid = oldest_xid; !TransactionIdEquals(xid, next_xid);)
		{
			uint32		offset = (uint32) (xid - oldest_xid);

			if ((++scan_iterations & 0xfff) == 0 && ShutdownRequestPending)
				ereport(ERROR,
						(errcode(ERRCODE_ADMIN_SHUTDOWN),
						 errmsg("terminating automatic reader snapshot scan due to administrator command")));

			if ((running[offset / 8] & (1U << (offset % 8))) != 0)
				count++;
			TransactionIdAdvance(xid);
		}
		if (count > MaxAllocSize / sizeof(TransactionId))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("automatic reader snapshot is too large")));
		xids = count > 0 ? palloc_array(TransactionId, count) : NULL;
		count = 0;
		scan_iterations = 0;
		for (xid = oldest_xid; !TransactionIdEquals(xid, next_xid);)
		{
			uint32		offset = (uint32) (xid - oldest_xid);

			if ((++scan_iterations & 0xfff) == 0 && ShutdownRequestPending)
				ereport(ERROR,
						(errcode(ERRCODE_ADMIN_SHUTDOWN),
						 errmsg("terminating automatic reader snapshot scan due to administrator command")));

			if ((running[offset / 8] & (1U << (offset % 8))) != 0)
				xids[count++] = xid;
			TransactionIdAdvance(xid);
		}

		memset(&snapshot, 0, sizeof(snapshot));
		snapshot.header.read_lsn = read_lsn;
		snapshot.header.magic = PAGESTORE_READER_SNAPSHOT_MAGIC;
		snapshot.header.format = PAGESTORE_READER_SNAPSHOT_FORMAT;
		snapshot.header.timeline = pagestore_localsvc_timeline();
		snapshot.header.count = count;
		snapshot.header.xmin = count > 0 ? xids[0] : next_xid;
		snapshot.header.xmax = next_xid;
		snapshot.xids = xids;
		pagestore_reader_snapshot_crc(&snapshot.header, xids);
		blocks = pagestore_publish_reader_snapshot_data(&snapshot);

		/*
		 * Publish a database-independent manifest before READY.  A backend can
		 * be forked after the reader pin advances but before DatabasePath is
		 * available; this exact-R global artifact lets it adopt safely during
		 * catalog startup.  The database worker later adds the local relmap CRC.
		 */
		memset(&manifest, 0, sizeof(manifest));
		manifest.read_lsn = read_lsn;
		manifest.artifact_size = sizeof(snapshot.header) +
			(Size) snapshot.header.count * sizeof(TransactionId);
		manifest.magic = PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC;
		manifest.format = PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT;
		manifest.timeline = pagestore_localsvc_timeline();
		manifest.block_count = blocks;
		manifest.artifact_crc = snapshot.header.crc;
		/* READY is database-independent staging.  The database workers prime
		 * and validate the exact-R global map together with each local map before
		 * publishing adoption manifests and the all-database barrier. */
		manifest.global_relmap_crc = 0;
		pagestore_reader_snapshot_manifest_crc(&manifest);
		memset(page, 0, sizeof(page));
		memcpy(page, &manifest, sizeof(manifest));
		key = pagestore_reader_snapshot_key(
			PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, InvalidOid);
		nblocks = pagestore_localsvc_obj_write_prepare_timeout(
			PS_KLASS_READER_SNAPSHOT, &key,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
		pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, page, (uint64) read_lsn, nblocks,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
		pagestore_localsvc_store_sync_timeout(
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);

		memset(&ready, 0, sizeof(ready));
		ready.header = snapshot.header;
		ready.block_count = blocks;
		pagestore_reader_snapshot_ready_crc(&ready);
		memset(page, 0, sizeof(page));
		memcpy(page, &ready, sizeof(ready));
		key = pagestore_reader_snapshot_key(
			PAGESTORE_READER_SNAPSHOT_READY_OBJECT, InvalidOid);
		nblocks = pagestore_localsvc_obj_write_prepare_timeout(
			PS_KLASS_READER_SNAPSHOT, &key,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
		pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, page, (uint64) read_lsn, nblocks,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
		pagestore_localsvc_store_sync_timeout(
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(work);
		work = NULL;
		succeeded = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext error_context;

		error_context = MemoryContextSwitchTo(TopMemoryContext);
		edata = CopyErrorData();
		MemoryContextSwitchTo(error_context);
		MemoryContextSwitchTo(oldcontext);
		if (work != NULL)
			MemoryContextDelete(work);
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		{
			FreeErrorData(edata);
			PG_RE_THROW();
		}
		FlushErrorState();
		ereport(WARNING,
				(errmsg("pagestore: automatic reader snapshot publication failed at %X/%08X: %s",
						LSN_FORMAT_ARGS(read_lsn), edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
	return succeeded;
}

/* Checkpoint completion only replaces the pending job; the worker does I/O. */
void
pagestore_publish_checkpoint_reader_snapshot(const ControlFileData *control)
{
	if (pagestore_reader_snapshot_job == NULL ||
		!pagestore_branch_backend_active())
		return;
	SpinLockAcquire(&pagestore_reader_snapshot_job->mutex);
	pagestore_reader_snapshot_job->control = *control;
	pagestore_reader_snapshot_job->generation++;
	SpinLockRelease(&pagestore_reader_snapshot_job->mutex);
}

void
pagestore_reader_snapshot_worker_main(Datum main_arg)
{
	uint64		processed = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	while (!ShutdownRequestPending)
	{
		ControlFileData control;
		uint64		generation;

		SpinLockAcquire(&pagestore_reader_snapshot_job->mutex);
		control = pagestore_reader_snapshot_job->control;
		generation = pagestore_reader_snapshot_job->generation;
		SpinLockRelease(&pagestore_reader_snapshot_job->mutex);
		if (generation > processed)
		{
			if (pagestore_build_checkpoint_reader_snapshot(&control))
			{
				processed = generation;
				continue;
			}
		}
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	proc_exit(0);
}

PG_FUNCTION_INFO_V1(pagestore_validate_checkpoint_reader_snapshot);
Datum
pagestore_validate_checkpoint_reader_snapshot(PG_FUNCTION_ARGS)
{
	XLogRecPtr	read_lsn = PG_GETARG_LSN(0);
	PagestoreReaderSnapshotReady ready;
	PagestoreReaderSnapshotReady checked;
	PageStoreRelKey key;
	char		page[BLCKSZ];
	uint64		resolved = 0;

	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_READY_OBJECT, InvalidOid);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) read_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || resolved != read_lsn)
		ereport(ERROR,
				(errmsg("no automatic reader snapshot at %X/%08X",
						LSN_FORMAT_ARGS(read_lsn))));
	memcpy(&ready, page, sizeof(ready));
	checked = ready;
	pagestore_reader_snapshot_ready_crc(&checked);
	if (ready.header.magic != PAGESTORE_READER_SNAPSHOT_MAGIC ||
		ready.header.format != PAGESTORE_READER_SNAPSHOT_FORMAT ||
		ready.header.timeline != pagestore_localsvc_timeline() ||
		ready.header.read_lsn != read_lsn || ready.block_count == 0 ||
		ready.reserved != 0 || !EQ_CRC32C(ready.crc, checked.crc))
		ereport(ERROR,
				(errmsg("automatic reader snapshot at %X/%08X is invalid",
						LSN_FORMAT_ARGS(read_lsn))));
	PG_RETURN_INT64((int64) ready.header.count);
}

PG_FUNCTION_INFO_V1(pagestore_publish_database_reader_manifest);
Datum
pagestore_publish_database_reader_manifest(PG_FUNCTION_ARGS)
{
	PagestoreReaderSnapshotReady ready;
	PagestoreReaderSnapshotReady checked_ready;
	PagestoreReaderSnapshotManifest manifest;
	PageStoreRelKey key;
	char		page[BLCKSZ];
	uint64		resolved = 0;
	BlockNumber nblocks;
	pg_crc32c	current_global_crc;
	pg_crc32c	current_local_crc;

	if (!superuser() || !OidIsValid(MyDatabaseId) || DatabasePath == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("reader manifest publication requires a database superuser backend")));
	if (!pagestore_branch_backend_active() ||
		!pagestore_branch_routing_active() ||
		pagestore_localsvc_read_lsn() != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("reader manifest publication requires a fully routed writable pagestore compute")));
	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_READY_OBJECT, InvalidOid);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, PG_UINT64_MAX, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS))
		PG_RETURN_NULL();
	memcpy(&ready, page, sizeof(ready));
	checked_ready = ready;
	pagestore_reader_snapshot_ready_crc(&checked_ready);
	if (ready.header.magic != PAGESTORE_READER_SNAPSHOT_MAGIC ||
		ready.header.format != PAGESTORE_READER_SNAPSHOT_FORMAT ||
		ready.header.timeline != pagestore_localsvc_timeline() ||
		ready.header.read_lsn != (XLogRecPtr) resolved ||
		ready.header.count > MaxAllocSize / sizeof(TransactionId) ||
		ready.block_count == 0 ||
		ready.block_count != (BlockNumber)
		((sizeof(ready.header) +
		  (Size) ready.header.count * sizeof(TransactionId) + BLCKSZ - 1) /
		 BLCKSZ) || ready.reserved != 0 ||
		!EQ_CRC32C(ready.crc, checked_ready.crc))
		ereport(ERROR, (errmsg("latest automatic reader snapshot is invalid")));
	memset(&manifest, 0, sizeof(manifest));
	manifest.read_lsn = resolved;
	manifest.artifact_size = sizeof(ready.header) +
		(Size) ready.header.count * sizeof(TransactionId);
	manifest.magic = PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC;
	manifest.format = PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT;
	manifest.timeline = ready.header.timeline;
	manifest.block_count = ready.block_count;
	manifest.artifact_crc = ready.header.crc;
	if (!pagestore_load_reader_relmap(InvalidOid, GLOBALTABLESPACE_OID,
			(XLogRecPtr) resolved,
			&manifest.global_relmap_crc) ||
		!pagestore_load_reader_relmap(MyDatabaseId, MyDatabaseTableSpace,
			(XLogRecPtr) resolved,
			&manifest.local_relmap_crc))
		PG_RETURN_NULL();
	LWLockAcquire(RelationMappingLock, LW_SHARED);
	current_global_crc = pagestore_reader_relmap_crc("global");
	current_local_crc = pagestore_reader_relmap_crc(DatabasePath);
	LWLockRelease(RelationMappingLock);
	if (!EQ_CRC32C(current_global_crc, manifest.global_relmap_crc) ||
		!EQ_CRC32C(current_local_crc, manifest.local_relmap_crc))
		PG_RETURN_NULL();
	pagestore_reader_snapshot_manifest_crc(&manifest);
	memset(page, 0, sizeof(page));
	memcpy(page, &manifest, sizeof(manifest));
	/* READY deliberately carries no relmap checksum.  Once a database worker
	 * has primed the exact-R global map, replace the global manifest first so
	 * backends can validate the database-independent snapshot header. */
	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, InvalidOid);
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
		&key, 0, page, resolved, nblocks,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, MyDatabaseId);
	{
		char		existing[BLCKSZ];
		uint64		existing_resolved = 0;

		if (pagestore_localsvc_obj_read_at_timeout(
				PS_KLASS_READER_SNAPSHOT, &key, 0, resolved, existing,
				&existing_resolved,
				PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) &&
			existing_resolved == resolved &&
			memcmp(existing, page, BLCKSZ) == 0)
		{
			pagestore_localsvc_store_sync_timeout(
				PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
			PG_RETURN_LSN((XLogRecPtr) resolved);
		}
	}
	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, MyDatabaseId);
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
		&key, 0, page, resolved, nblocks,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_store_sync_timeout(
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	PG_RETURN_LSN((XLogRecPtr) resolved);
}

static BlockNumber
pagestore_publish_reader_snapshot(const char *dir, uint32 timeline,
								  XLogRecPtr read_lsn)
{
	PagestoreReaderSnapshot *snapshot;
	PagestoreReaderSnapshotManifest manifest;
	PageStoreRelKey manifest_key;
	char		page[BLCKSZ];
	BlockNumber block_count;
	BlockNumber nblocks;
	Size		xids_size;
	Size		artifact_size;

	snapshot = pagestore_load_reader_snapshot(dir, timeline, read_lsn,
										 CurrentMemoryContext, ERROR);
	xids_size = snapshot->header.count * sizeof(TransactionId);
	artifact_size = sizeof(snapshot->header) + xids_size;
	block_count = pagestore_publish_reader_snapshot_data(snapshot);

	memset(&manifest, 0, sizeof(manifest));
	manifest.read_lsn = read_lsn;
	manifest.artifact_size = artifact_size;
	manifest.magic = PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC;
	manifest.format = PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT;
	manifest.timeline = timeline;
	manifest.block_count = block_count;
	manifest.artifact_crc = snapshot->header.crc;
	manifest.global_relmap_crc =
		pagestore_prepared_reader_relmap_crc(dir, true);
	manifest.local_relmap_crc =
		pagestore_prepared_reader_relmap_crc(dir, false);
	pagestore_reader_snapshot_manifest_crc(&manifest);
	memset(page, 0, sizeof(page));
	memcpy(page, &manifest, sizeof(manifest));
	manifest_key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, InvalidOid);
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &manifest_key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
		&manifest_key, 0, page, (uint64) read_lsn, nblocks,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	manifest_key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, MyDatabaseId);
	nblocks = pagestore_localsvc_obj_write_prepare_timeout(
		PS_KLASS_READER_SNAPSHOT, &manifest_key,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_obj_write_post_timeout(PS_KLASS_READER_SNAPSHOT,
		&manifest_key, 0, page, (uint64) read_lsn, nblocks,
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);
	pagestore_localsvc_store_sync_timeout(
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);

	if (snapshot->xids != NULL)
		pfree(snapshot->xids);
	pfree(snapshot);
	return block_count;
}

static PagestoreReaderSnapshot *
pagestore_load_published_reader_snapshot(uint32 timeline, XLogRecPtr read_lsn,
										MemoryContext context, int elevel)
{
	PagestoreReaderSnapshotManifest manifest;
	PagestoreReaderSnapshotManifest checked_manifest;
	PagestoreReaderSnapshot *snapshot;
	PageStoreRelKey key;
	char		page[BLCKSZ];
	char	   *artifact;
	uint64		resolved;
	Size		xids_size;

	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT,
		OidIsValid(MyDatabaseId) && DatabasePath != NULL ?
		MyDatabaseId : InvalidOid);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) read_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || resolved != read_lsn)
		ereport(elevel,
				(errmsg("no published reader snapshot manifest for timeline %u at %X/%08X",
						timeline, LSN_FORMAT_ARGS(read_lsn))));
	memcpy(&manifest, page, sizeof(manifest));
	checked_manifest = manifest;
	pagestore_reader_snapshot_manifest_crc(&checked_manifest);
	if (manifest.magic != PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC ||
		manifest.format != PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT ||
		manifest.timeline != timeline || manifest.read_lsn != read_lsn ||
		manifest.artifact_size < sizeof(PagestoreReaderSnapshotHeader) ||
		manifest.artifact_size > MaxAllocSize || manifest.block_count == 0 ||
		manifest.block_count !=
		(BlockNumber) ((manifest.artifact_size + BLCKSZ - 1) / BLCKSZ) ||
		!EQ_CRC32C(manifest.global_relmap_crc,
					 pagestore_reader_relmap_crc("global")) ||
		!EQ_CRC32C(checked_manifest.crc, manifest.crc))
		ereport(elevel,
				(errmsg("published reader snapshot manifest has an invalid identity, relation map, or header")));

	artifact = MemoryContextAlloc(context, (Size) manifest.artifact_size);
	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_DATA_OBJECT, InvalidOid);
	for (BlockNumber block = 0; block < manifest.block_count; block++)
	{
		Size		offset = (Size) block * BLCKSZ;
		Size		chunk = Min((Size) BLCKSZ,
							(Size) manifest.artifact_size - offset);

		if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
				&key, block, (uint64) read_lsn, page, &resolved,
				PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || resolved != read_lsn)
			ereport(elevel,
					(errmsg("published reader snapshot is missing data block %u",
							block)));
		memcpy(artifact + offset, page, chunk);
	}
	pagestore_localsvc_store_sync_timeout(
		PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS);

	snapshot = MemoryContextAlloc(context, sizeof(*snapshot));
	memcpy(&snapshot->header, artifact, sizeof(snapshot->header));
	if (snapshot->header.count > MaxAllocSize / sizeof(TransactionId))
		ereport(elevel,
				(errmsg("published reader snapshot has an invalid size or checksum identity")));
	xids_size = snapshot->header.count * sizeof(TransactionId);
	if (manifest.artifact_size != sizeof(snapshot->header) + xids_size ||
		!EQ_CRC32C(snapshot->header.crc, manifest.artifact_crc))
		ereport(elevel,
				(errmsg("published reader snapshot has an invalid size or checksum identity")));
	snapshot->xids = xids_size > 0 ? MemoryContextAlloc(context, xids_size) : NULL;
	if (xids_size > 0)
		memcpy(snapshot->xids, artifact + sizeof(snapshot->header), xids_size);
	pagestore_validate_reader_snapshot(snapshot, timeline, read_lsn,
									 "from the page store", elevel);
	pfree(artifact);
	return snapshot;
}

static XLogRecPtr
pagestore_resolve_published_reader_snapshot(XLogRecPtr upper_lsn)
{
	PageStoreRelKey key;
	char		page[BLCKSZ];
	uint64		resolved = 0;

	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, MyDatabaseId);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, (uint64) upper_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS))
		return InvalidXLogRecPtr;
	return (XLogRecPtr) resolved;
}

static void
pagestore_validate_database_reader_manifest(XLogRecPtr read_lsn)
{
	PagestoreReaderSnapshotManifest manifest;
	PagestoreReaderSnapshotManifest checked;
	PageStoreRelKey key;
	char		page[BLCKSZ];
	uint64		resolved;

	key = pagestore_reader_snapshot_key(
		PAGESTORE_READER_SNAPSHOT_MANIFEST_OBJECT, MyDatabaseId);
	if (!pagestore_localsvc_obj_read_at_timeout(PS_KLASS_READER_SNAPSHOT,
			&key, 0, read_lsn, page, &resolved,
			PAGESTORE_READER_SNAPSHOT_IO_TIMEOUT_MS) || resolved != read_lsn)
		ereport(ERROR,
				(errmsg("no database reader manifest for database %u at %X/%08X",
						MyDatabaseId, LSN_FORMAT_ARGS((XLogRecPtr) read_lsn))));
	memcpy(&manifest, page, sizeof(manifest));
	checked = manifest;
	pagestore_reader_snapshot_manifest_crc(&checked);
	if (manifest.magic != PAGESTORE_READER_SNAPSHOT_MANIFEST_MAGIC ||
		manifest.format != PAGESTORE_READER_SNAPSHOT_MANIFEST_FORMAT ||
		manifest.timeline != pagestore_localsvc_timeline() ||
		manifest.read_lsn != read_lsn ||
		!EQ_CRC32C(manifest.local_relmap_crc,
					 pagestore_reader_relmap_crc(DatabasePath)) ||
		!EQ_CRC32C(checked.crc, manifest.crc))
		ereport(ERROR,
				(errmsg("database reader manifest does not match database %u",
						MyDatabaseId)));
}

static void
pagestore_validate_database_reader_view(void)
{
	if (prev_post_database_path_hook != NULL)
		prev_post_database_path_hook();
	if (!pagestore_advance_read_lsn || !OidIsValid(MyDatabaseId) ||
		DatabasePath == NULL || pagestore_localsvc_read_epoch() <= 1)
		return;
	pagestore_validate_database_reader_manifest(
		(XLogRecPtr) pagestore_localsvc_read_lsn());
}

PG_FUNCTION_INFO_V1(pagestore_publish_reader_snapshot_artifact);
Datum
pagestore_publish_reader_snapshot_artifact(PG_FUNCTION_ARGS)
{
	char	   *dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		timeline = PG_GETARG_INT32(1);
	XLogRecPtr	read_lsn = PG_GETARG_LSN(2);
	BlockNumber blocks;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to publish a reader snapshot")));
	if (timeline < 0 || XLogRecPtrIsInvalid(read_lsn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid reader timeline or read LSN")));
	if (!pagestore_branch_backend_active())
		ereport(ERROR,
				(errmsg("pagestore.backend must be \"localsvc\" to publish a reader snapshot")));
	if ((uint32) timeline != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errmsg("reader timeline %d is not the active localsvc timeline %u",
						timeline, pagestore_localsvc_timeline())));
	blocks = pagestore_publish_reader_snapshot(dir, (uint32) timeline,
										 read_lsn);
	PG_RETURN_INT64((int64) blocks);
}

PG_FUNCTION_INFO_V1(pagestore_validate_published_reader_snapshot);
Datum
pagestore_validate_published_reader_snapshot(PG_FUNCTION_ARGS)
{
	int32		timeline = PG_GETARG_INT32(0);
	XLogRecPtr	read_lsn = PG_GETARG_LSN(1);
	PagestoreReaderSnapshot *snapshot;
	uint32		count;

	if (!pagestore_branch_backend_active())
		ereport(ERROR,
				(errmsg("pagestore.backend must be \"localsvc\" to load a published reader snapshot")));
	if (timeline < 0 || XLogRecPtrIsInvalid(read_lsn) ||
		(uint32) timeline != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid reader timeline or read LSN")));
	snapshot = pagestore_load_published_reader_snapshot((uint32) timeline,
		read_lsn, CurrentMemoryContext, ERROR);
	count = snapshot->header.count;
	if (snapshot->xids != NULL)
		pfree(snapshot->xids);
	pfree(snapshot);
	PG_RETURN_INT64((int64) count);
}

static PagestoreReaderSnapshot *pagestore_fixed_snapshot = NULL;
static MemoryContext pagestore_fixed_snapshot_context = NULL;

static bool
pagestore_transaction_id_is_in_progress(TransactionId xid, bool *result)
{
	uint32		lo;
	uint32		hi;

	if (!pagestore_advance_read_lsn || pagestore_fixed_snapshot == NULL)
		return prev_xid_in_progress_hook != NULL &&
			prev_xid_in_progress_hook(xid, result);
	if (!TransactionIdIsNormal(xid) ||
		TransactionIdPrecedes(xid, pagestore_fixed_snapshot->header.xmin))
	{
		*result = false;
		return true;
	}
	if (!TransactionIdPrecedes(xid, pagestore_fixed_snapshot->header.xmax))
	{
		*result = true;
		return true;
	}
	lo = 0;
	hi = pagestore_fixed_snapshot->header.count;
	while (lo < hi)
	{
		uint32		mid = lo + (hi - lo) / 2;
		TransactionId candidate = pagestore_fixed_snapshot->xids[mid];

		if (TransactionIdPrecedes(candidate, xid))
			lo = mid + 1;
		else
			hi = mid;
	}
	*result = lo < pagestore_fixed_snapshot->header.count &&
		TransactionIdEquals(pagestore_fixed_snapshot->xids[lo], xid);
	return true;
}

/*
 * One controller-owned pin cannot protect two concurrently served reader
 * epochs.  Serialize advancing-reader transactions until per-epoch owners are
 * introduced: the durable pin can then move only after the old transaction
 * has ended, and every later transaction adopts the protected view first.
 */
static void
pagestore_reader_advance_xact_end(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PREPARE && event != XACT_EVENT_PARALLEL_COMMIT &&
		event != XACT_EVENT_PARALLEL_ABORT)
		return;

	if (pagestore_reader_advance_lock_held)
	{
		LOCKTAG		tag;

		pagestore_reader_advance_locktag(&tag);
		(void) LockRelease(&tag, ExclusiveLock, true);
		pagestore_reader_advance_lock_held = false;
	}
}

static void
pagestore_adopt_reader_view_at_xact_start(void)
{
	PagestoreReaderSnapshot *snapshot = NULL;
	PagestoreReaderSnapshot *old_snapshot;
	MemoryContext snapshot_context;
	MemoryContext old_snapshot_context;
	MemoryContext cxt = CurrentMemoryContext;
	ErrorData  *edata = NULL;
	XLogRecPtr	candidate;
	XLogRecPtr	published = InvalidXLogRecPtr;
	XLogRecPtr	protected_lsn;
	XLogRecPtr	newest_adoptable;
	ControlFileData control;
	uint32		adoption_generation;
	uint64		read_seq = 0;
	bool		valid = false;
	bool		must_adopt;
	PsRetentionPin protected_pin;

	if (prev_xact_start_hook != NULL)
		prev_xact_start_hook();
	if (!pagestore_advance_read_lsn || pagestore_reader_horizon == NULL ||
		pagestore_localsvc_read_lsn() == 0 || IsParallelWorker())
		return;
	{
		LOCKTAG		tag;

		pagestore_reader_advance_locktag(&tag);
		/* A session-owned heavyweight lock survives savepoint rollback; the
		 * top-level transaction callback releases it explicitly. */
		(void) LockAcquire(&tag, ExclusiveLock, true, false);
	}
	pagestore_reader_advance_lock_held = true;
	if (!pagestore_find_retention_owner(pagestore_localsvc_timeline(),
			PS_RETENTION_OWNER_READER, pagestore_retention_owner_id,
			&protected_pin) ||
		protected_pin.generation != pagestore_retention_owner_generation ||
		protected_pin.resources != PS_READER_RETENTION_RESOURCES)
		ereport(FATAL,
				(errmsg("pagestore advancing reader lost retention owner authority")));
	protected_lsn = (XLogRecPtr) protected_pin.lsn;
	must_adopt = protected_lsn > (XLogRecPtr) pagestore_localsvc_read_lsn();

	pagestore_refresh_reader_horizon();
	SpinLockAcquire(&pagestore_reader_horizon->mutex);
	candidate = pagestore_reader_horizon->candidate_lsn;
	SpinLockRelease(&pagestore_reader_horizon->mutex);
	if (candidate < protected_lsn)
		candidate = protected_lsn;
	if (candidate <= (XLogRecPtr) pagestore_localsvc_read_lsn())
		return;
	snapshot_context = AllocSetContextCreate(TopMemoryContext,
		"pagestore advancing reader snapshot", ALLOCSET_DEFAULT_SIZES);

	PG_TRY();
	{
		/*
		 * A backend forked after another process advanced the durable pin still
		 * starts with the postmaster's boot-time view.  During early database
		 * initialization MyDatabaseId/DatabasePath may not yet identify the
		 * database-specific manifest, so recover the exact protected global
		 * artifact.  The post-database-path hook validates the per-database map
		 * before normal query service.
		 */
		published = must_adopt ? protected_lsn :
			pagestore_resolve_published_reader_snapshot(candidate);
		if (XLogRecPtrIsInvalid(published) ||
			published <= (XLogRecPtr) pagestore_localsvc_read_lsn())
			goto adoption_done;
		snapshot = pagestore_load_published_reader_snapshot(
			pagestore_localsvc_timeline(), published, snapshot_context, ERROR);
		if (OidIsValid(MyDatabaseId) && DatabasePath != NULL)
			pagestore_validate_database_reader_manifest(published);
		valid = ps_control_asof_timeout(published, &control,
			PAGESTORE_READER_HORIZON_TIMEOUT_MS) &&
			pagestore_reader_database_barrier_valid(published) &&
			control.checkPointCopy.redo == published &&
			pagestore_localsvc_read_fence_timeout((uint64) published,
				&read_seq, PAGESTORE_READER_HORIZON_TIMEOUT_MS);
adoption_done:
		;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(cxt);
		edata = CopyErrorData();
	}
	PG_END_TRY();

	if (edata != NULL)
	{
		MemoryContextDelete(snapshot_context);
		snapshot_context = NULL;
		if (edata->elevel >= FATAL ||
			edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN || must_adopt)
			ReThrowError(edata);
		FreeErrorData(edata);
		FlushErrorState();
	}
	if (!valid)
	{
		if (snapshot_context != NULL)
			MemoryContextDelete(snapshot_context);
		if (must_adopt)
			ereport(ERROR,
					(errmsg("pagestore reader cannot adopt its durable retention horizon"),
					 errdetail("Durable pin %X/%08X, candidate %X/%08X, published snapshot %X/%08X.",
							   LSN_FORMAT_ARGS(protected_lsn),
							   LSN_FORMAT_ARGS(candidate),
							   LSN_FORMAT_ARGS(published))));
		return;
	}
	if (published < protected_lsn)
	{
		MemoryContextDelete(snapshot_context);
		ereport(ERROR,
				(errmsg("pagestore reader snapshot is older than its durable retention horizon")));
	}
	/*
	 * The transaction gate proves that no old-view transaction remains.  Move
	 * the durable pin before publishing or adopting the new view.  An ambiguous
	 * failure keeps the old pin and old view; stale authority must stop serving.
	 */
	{
		uint8		status = PS_STATUS_ERROR;
		ErrorData  *pin_edata = NULL;

		PG_TRY();
		{
			status = pagestore_localsvc_retention_set_timeout(
				pagestore_localsvc_timeline(), PS_RETENTION_OWNER_READER,
				pagestore_retention_owner_id,
				pagestore_retention_owner_generation,
				PS_READER_RETENTION_RESOURCES, (uint64) published,
				read_seq,
				PS_READER_RETENTION_TIMEOUT_MS);
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(cxt);
			pin_edata = CopyErrorData();
			FlushErrorState();
		}
		PG_END_TRY();
		if (pin_edata != NULL)
		{
			MemoryContextDelete(snapshot_context);
			ReThrowError(pin_edata);
		}
		if (status == PS_STATUS_ERROR)
		{
			MemoryContextDelete(snapshot_context);
			if (must_adopt)
				ereport(ERROR,
						(errmsg("pagestore reader retention advance was rejected")));
			return;
		}
		if (status == PS_STATUS_STALE)
			ereport(FATAL,
					(errmsg("pagestore reader lost retention owner authority"),
					 errdetail("Owner %llu generation %u was fenced by a newer controller generation.",
							   (unsigned long long) pagestore_retention_owner_id,
							   pagestore_retention_owner_generation)));
		if (status != PS_STATUS_OK)
		{
			MemoryContextDelete(snapshot_context);
			return;
		}
	}
	SpinLockAcquire(&pagestore_reader_horizon->mutex);
	if (published > pagestore_reader_horizon->adoptable_lsn)
	{
		pagestore_reader_horizon->adoptable_lsn = published;
		pagestore_reader_horizon->candidate_generation++;
		if (pagestore_reader_horizon->candidate_generation == 0)
			pagestore_reader_horizon->candidate_generation = 1;
		pagestore_reader_horizon->adoptable_generation =
			pagestore_reader_horizon->candidate_generation;
	}
	adoption_generation = pagestore_reader_horizon->adoptable_generation;
	newest_adoptable = pagestore_reader_horizon->adoptable_lsn;
	SpinLockRelease(&pagestore_reader_horizon->mutex);
	if (published < newest_adoptable ||
		adoption_generation <= pagestore_localsvc_read_epoch())
	{
		MemoryContextDelete(snapshot_context);
		return;
	}
	AdvanceNextFullTransactionIdToReadOnlyHorizon(
		control.checkPointCopy.nextXid);
	MultiXactAdvanceNextMXact(control.checkPointCopy.nextMulti,
							 control.checkPointCopy.nextMultiOffset);
	pagestore_oldest_commit_ts_xid =
		control.checkPointCopy.oldestCommitTsXid;
	pagestore_newest_commit_ts_xid =
		control.checkPointCopy.newestCommitTsXid;
	pagestore_commit_ts_active = control.track_commit_timestamp;
	pagestore_commit_ts_bounds_valid = true;

	old_snapshot = pagestore_fixed_snapshot;
	old_snapshot_context = pagestore_fixed_snapshot_context;
	pagestore_fixed_snapshot = snapshot;
	pagestore_fixed_snapshot_context = snapshot_context;
	pagestore_localsvc_adopt_read_view((uint64) published, read_seq,
									 adoption_generation);
	InvalidateSystemCachesExtended(true);
	ResetPlanCache();
	if (old_snapshot_context != NULL)
		MemoryContextDelete(old_snapshot_context);
	else if (old_snapshot != NULL)
	{
		if (old_snapshot->xids != NULL)
			pfree(old_snapshot->xids);
		pfree(old_snapshot);
	}
}

static bool
pagestore_get_snapshot_data(Snapshot snapshot)
{
	uint64		read_lsn = pagestore_localsvc_read_lsn();

	if (read_lsn == 0)
		return prev_get_snapshot_data_hook != NULL &&
			prev_get_snapshot_data_hook(snapshot);
	pagestore_refresh_reader_horizon();
	if (pagestore_fixed_snapshot == NULL)
		pagestore_fixed_snapshot = pagestore_load_reader_snapshot(DataDir,
			pagestore_localsvc_timeline(), (XLogRecPtr) read_lsn,
			TopMemoryContext, ERROR);

	snapshot->xmin = pagestore_fixed_snapshot->header.xmin;
	snapshot->xmax = pagestore_fixed_snapshot->header.xmax;
	snapshot->xcnt = 0;
	snapshot->subxcnt = pagestore_fixed_snapshot->header.count;
	snapshot->suboverflowed = false;
	snapshot->takenDuringRecovery = true;
	if (snapshot->subxcnt > GetMaxSnapshotSubxidCount())
	{
		TransactionId *subxip;

		subxip = realloc(snapshot->subxip,
						 snapshot->subxcnt * sizeof(TransactionId));
		if (subxip == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		snapshot->subxip = subxip;
	}
	if (snapshot->subxcnt > 0)
		memcpy(snapshot->subxip, pagestore_fixed_snapshot->xids,
			   snapshot->subxcnt * sizeof(TransactionId));
	return true;
}

PG_FUNCTION_INFO_V1(pagestore_reader_candidate_lsn);
Datum
pagestore_reader_candidate_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	candidate;

	if (pagestore_reader_horizon == NULL)
		PG_RETURN_NULL();
	SpinLockAcquire(&pagestore_reader_horizon->mutex);
	candidate = pagestore_reader_horizon->candidate_lsn;
	SpinLockRelease(&pagestore_reader_horizon->mutex);
	if (XLogRecPtrIsInvalid(candidate))
		PG_RETURN_NULL();
	PG_RETURN_LSN(candidate);
}

PG_FUNCTION_INFO_V1(pagestore_reader_candidate_generation);
Datum
pagestore_reader_candidate_generation(PG_FUNCTION_ARGS)
{
	uint32		generation;

	if (pagestore_reader_horizon == NULL)
		PG_RETURN_INT64(0);
	SpinLockAcquire(&pagestore_reader_horizon->mutex);
	generation = pagestore_reader_horizon->candidate_generation;
	SpinLockRelease(&pagestore_reader_horizon->mutex);
	PG_RETURN_INT64((int64) generation);
}

PG_FUNCTION_INFO_V1(pagestore_reader_effective_lsn);
Datum
pagestore_reader_effective_lsn(PG_FUNCTION_ARGS)
{
	uint64		read_lsn = pagestore_localsvc_read_lsn();

	if (read_lsn == 0)
		PG_RETURN_NULL();
	PG_RETURN_LSN((XLogRecPtr) read_lsn);
}

PG_FUNCTION_INFO_V1(pagestore_reader_effective_generation);
Datum
pagestore_reader_effective_generation(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64((int64) pagestore_localsvc_read_epoch());
}

PG_FUNCTION_INFO_V1(pagestore_writer_handoff_token);
Datum
pagestore_writer_handoff_token(PG_FUNCTION_ARGS)
{
	bytea	   *result;
	PagestoreReaderHandoffToken token;
	Plan	   *plan = pagestore_current_planned_stmt != NULL ?
		pagestore_current_planned_stmt->planTree : NULL;
	TargetEntry *tle = plan != NULL && list_length(plan->targetlist) == 1 ?
		linitial_node(TargetEntry, plan->targetlist) : NULL;

	if (RecoveryInProgress() || !pagestore_branch_backend_active() ||
		!pagestore_branch_routing_active() ||
		pagestore_localsvc_read_lsn() != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("a reader handoff token requires a writable pagestore compute")));
	if (pagestore_current_command_type != CMD_SELECT ||
		pagestore_current_has_modifying_cte ||
		pagestore_utility_nesting_level != 0 || !IsA(plan, Result) ||
		outerPlan(plan) != NULL || innerPlan(plan) != NULL ||
		((Result *) plan)->resconstantqual != NULL || tle == NULL ||
		!IsA(tle->expr, FuncExpr) ||
		((FuncExpr *) tle->expr)->funcid != fcinfo->flinfo->fn_oid)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("a reader handoff token must be called by a standalone SELECT")));
	if (TransactionIdIsValid(GetTopTransactionIdIfAny()))
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("a reader handoff token cannot be issued by a write transaction"),
				 errhint("Issue the token in a new transaction after committing the writes.")));
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("a reader handoff token cannot be issued inside a transaction block"),
				 errhint("Issue the token as a standalone autocommit statement.")));
	pagestore_handoff_issued = true;
	transaction_read_only_forced = true;
	XactReadOnly = true;
	memset(&token, 0, sizeof(token));
	token.magic = PAGESTORE_READER_HANDOFF_MAGIC;
	token.format = PAGESTORE_READER_HANDOFF_FORMAT;
	token.timeline = pagestore_localsvc_timeline();
	token.lsn = GetXLogInsertRecPtr();
	result = palloc(VARHDRSZ + sizeof(token));
	SET_VARSIZE(result, VARHDRSZ + sizeof(token));
	memcpy(VARDATA(result), &token, sizeof(token));
	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(pagestore_reader_handoff_ready);
Datum
pagestore_reader_handoff_ready(PG_FUNCTION_ARGS)
{
	bytea	   *value = PG_GETARG_BYTEA_PP(0);
	PagestoreReaderHandoffToken token;
	uint64		read_lsn = pagestore_localsvc_read_lsn();

	if (VARSIZE_ANY_EXHDR(value) != sizeof(token))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("reader handoff token has an invalid size")));
	memcpy(&token, VARDATA_ANY(value), sizeof(token));
	if (token.magic != PAGESTORE_READER_HANDOFF_MAGIC ||
		token.format != PAGESTORE_READER_HANDOFF_FORMAT ||
		token.reserved != 0 || XLogRecPtrIsInvalid(token.lsn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("reader handoff token has an invalid identity")));
	if (!pagestore_branch_backend_active() || read_lsn == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("reader handoff readiness requires a pinned pagestore reader")));
	if (token.timeline != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("reader handoff token belongs to a different pagestore timeline")));
	PG_RETURN_BOOL((XLogRecPtr) read_lsn >= (XLogRecPtr) token.lsn);
}

#define PAGESTORE_BRANCH_BOOTSTRAP_FILE "pagestore_branch.bootstrap"
#define PAGESTORE_BRANCH_BOOTSTRAP_MAGIC UINT32_C(0x50534242)
#define PAGESTORE_BRANCH_BOOTSTRAP_FORMAT 1
#define PAGESTORE_BRANCH_BOOTSTRAP_HAS_USER_TABLESPACES UINT32_C(0x00000001)

typedef struct PagestoreBranchBootstrapHeader
{
	uint64		checkpoint_redo;
	uint64		recovery_lsn;
	uint64		fork_lsn;
	uint64		system_identifier;
	uint64		artifact_size;
	uint32		magic;
	uint32		format;
	uint32		new_timeline;
	uint32		parent_timeline;
	uint32		map_count;
	uint32		flags;
	pg_crc32c	crc;
	pg_crc32c	manifest_crc;
} PagestoreBranchBootstrapHeader;

typedef struct PagestoreBranchBootstrapMapHeader
{
	uint32		database_oid;
	uint32		size;
} PagestoreBranchBootstrapMapHeader;

typedef struct PagestoreBranchBootstrapMap
{
	Oid			database_oid;
	Size		size;
	char	   *data;
} PagestoreBranchBootstrapMap;

static void pagestore_require_prepared_artifact(const char *prepared_dir,
											 const char *relpath,
											 bool directory);
static char *pagestore_read_branch_manifest(const char *target_dir);

static pg_crc32c
pagestore_branch_manifest_crc(const char *manifest)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, manifest, strlen(manifest));
	FIN_CRC32C(crc);
	return crc;
}

static int
pagestore_branch_bootstrap_map_cmp(const void *a, const void *b)
{
	const PagestoreBranchBootstrapMap *ma = a;
	const PagestoreBranchBootstrapMap *mb = b;

	if (ma->database_oid < mb->database_oid)
		return -1;
	if (ma->database_oid > mb->database_oid)
		return 1;
	return 0;
}

static void
pagestore_branch_bootstrap_add_map(PagestoreBranchBootstrapMap **maps,
								   int *count, int *capacity,
								   Oid database_oid, const char *dir)
{
	PagestoreBranchBootstrapMap *map;
	char	   *data;

	if (*count == *capacity)
	{
		int			new_capacity;

		if (*capacity == 0)
			new_capacity = 8;
		else if (*capacity > INT_MAX / 2)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("too many database relation maps for branch bootstrap")));
		else
			new_capacity = *capacity * 2;
		if ((Size) new_capacity > MaxAllocSize / sizeof(**maps))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("too many database relation maps for branch bootstrap")));
		if (*maps == NULL)
			*maps = palloc(sizeof(**maps) * new_capacity);
		else
			*maps = repalloc(*maps, sizeof(**maps) * new_capacity);
		*capacity = new_capacity;
	}

	data = palloc(PAGESTORE_READER_RELMAP_MAX_SIZE);
	map = &(*maps)[(*count)++];
	map->database_oid = database_oid;
	map->size = pagestore_read_reader_relmap(dir, data,
											PAGESTORE_READER_RELMAP_MAX_SIZE);
	map->data = data;
}

static bool
pagestore_branch_bootstrap_has_user_tablespaces(void)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir("pg_tblspc");
	while ((de = ReadDir(dir, "pg_tblspc")) != NULL)
	{
		if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0)
		{
			FreeDir(dir);
			return true;
		}
	}
	FreeDir(dir);
	return false;
}

/*
 * Capture the local catalog identity which a fully routed branch still needs
 * before it can read catalog relations from pagestore.  One cluster-wide lock
 * protects every relation map, so the global map and all default-tablespace
 * database maps form one point-in-time bundle.  The surrounding branch-create
 * quiesce protects database directory topology.
 */
static void
pagestore_write_branch_bootstrap(const char *target_dir,
								 int32 new_tl, int32 parent_tl,
								 XLogRecPtr checkpoint_redo,
								 XLogRecPtr recovery_lsn,
								 XLogRecPtr fork_lsn,
								 uint64 system_identifier)
{
	PagestoreBranchBootstrapMap *maps = NULL;
	PagestoreBranchBootstrapHeader *header;
	char	   *artifact;
	char	   *cursor;
	DIR		   *dir = NULL;
	struct dirent *de;
	Size		artifact_size;
	int			map_count = 0;
	int			map_capacity = 0;
	uint32		flags = 0;
	pg_crc32c	crc;
	char	   *manifest;

	if (pagestore_branch_bootstrap_has_user_tablespaces())
		flags |= PAGESTORE_BRANCH_BOOTSTRAP_HAS_USER_TABLESPACES;
	manifest = pagestore_read_branch_manifest(target_dir);
	if (manifest == NULL)
		ereport(ERROR,
				(errmsg("branch manifest is missing while publishing portable bootstrap")));

	LWLockAcquire(RelationMappingLock, LW_SHARED);
	PG_TRY();
	{
		pagestore_branch_bootstrap_add_map(&maps, &map_count, &map_capacity,
										   InvalidOid, "global");
		dir = AllocateDir("base");
		while ((de = ReadDir(dir, "base")) != NULL)
		{
			char	   *end;
			char		path[MAXPGPATH];
			unsigned long oid_value;
			struct stat st;
			int			pathlen;

			if (*de->d_name == '\0' ||
				strspn(de->d_name, "0123456789") != strlen(de->d_name))
				continue;
			errno = 0;
			oid_value = strtoul(de->d_name, &end, 10);
			if (errno != 0 || end == de->d_name || *end != '\0' ||
				oid_value == InvalidOid || oid_value > UINT32_MAX)
				continue;
			pathlen = snprintf(path, sizeof(path), "base/%s", de->d_name);
			PS_CHECK_PATH_FORMAT(pathlen, path);
			if (lstat(path, &st) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat database directory \"%s\": %m",
								path)));
			if (!S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("database path \"%s\" is not a directory",
								path)));
			pagestore_branch_bootstrap_add_map(&maps, &map_count,
											   &map_capacity,
											   (Oid) oid_value, path);
		}
		FreeDir(dir);
		dir = NULL;
	}
	PG_FINALLY();
	{
		if (dir != NULL)
			FreeDir(dir);
		LWLockRelease(RelationMappingLock);
	}
	PG_END_TRY();

	qsort(maps, map_count, sizeof(*maps),
		  pagestore_branch_bootstrap_map_cmp);
	if (map_count < 2 || maps[0].database_oid != InvalidOid)
		ereport(ERROR,
				(errmsg("branch bootstrap relation-map set is incomplete")));

	artifact_size = sizeof(*header);
	for (int i = 0; i < map_count; i++)
	{
		if (i > 0 && maps[i - 1].database_oid == maps[i].database_oid)
			ereport(ERROR,
					(errmsg("branch bootstrap contains duplicate database relation maps")));
		if (artifact_size >
			MaxAllocSize - sizeof(PagestoreBranchBootstrapMapHeader) ||
			maps[i].size > MaxAllocSize - artifact_size -
			sizeof(PagestoreBranchBootstrapMapHeader))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("branch bootstrap artifact is too large")));
		artifact_size += sizeof(PagestoreBranchBootstrapMapHeader) + maps[i].size;
	}
	if (artifact_size > PG_INT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch bootstrap artifact is too large")));

	artifact = palloc0(artifact_size);
	header = (PagestoreBranchBootstrapHeader *) artifact;
	header->checkpoint_redo = (uint64) checkpoint_redo;
	header->recovery_lsn = (uint64) recovery_lsn;
	header->fork_lsn = (uint64) fork_lsn;
	header->system_identifier = system_identifier;
	header->artifact_size = artifact_size;
	header->magic = PAGESTORE_BRANCH_BOOTSTRAP_MAGIC;
	header->format = PAGESTORE_BRANCH_BOOTSTRAP_FORMAT;
	header->new_timeline = (uint32) new_tl;
	header->parent_timeline = (uint32) parent_tl;
	header->map_count = map_count;
	header->flags = flags;
	header->manifest_crc = pagestore_branch_manifest_crc(manifest);
	cursor = artifact + sizeof(*header);
	for (int i = 0; i < map_count; i++)
	{
		PagestoreBranchBootstrapMapHeader entry;

		entry.database_oid = maps[i].database_oid;
		entry.size = maps[i].size;
		memcpy(cursor, &entry, sizeof(entry));
		cursor += sizeof(entry);
		memcpy(cursor, maps[i].data, maps[i].size);
		cursor += maps[i].size;
	}
	Assert(cursor == artifact + artifact_size);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, artifact, artifact_size);
	FIN_CRC32C(crc);
	header->crc = crc;
	pagestore_publish_artifact(target_dir, PAGESTORE_BRANCH_BOOTSTRAP_FILE,
								   "branch bootstrap", artifact,
								   (int) artifact_size);
	for (int i = 0; i < map_count; i++)
		pfree(maps[i].data);
	pfree(maps);
	pfree(artifact);
	pfree(manifest);
}

/*
 * Load and completely validate the self-contained bootstrap artifact before
 * an installer changes the target datadir.  The artifact CRC covers both its
 * identity header and every relation-map byte; relation-map files also carry
 * PostgreSQL's own checksum, which startup verifies when it consumes them.
 */
static char *
pagestore_load_branch_bootstrap(const char *dir,
								int32 new_tl, int32 parent_tl,
								XLogRecPtr checkpoint_redo,
								XLogRecPtr recovery_lsn,
								XLogRecPtr fork_lsn,
								PagestoreBranchBootstrapHeader **header_out)
{
	PagestoreBranchBootstrapHeader *header;
	char	   *artifact;
	char	   *cursor;
	char	   *end;
	char		path[MAXPGPATH];
	struct stat st;
	pg_crc32c	stored_crc;
	pg_crc32c	crc;
	uint32		previous_oid = InvalidOid;
	int			fd;
	int			pathlen;

	pathlen = snprintf(path, sizeof(path), "%s/%s", dir,
					   PAGESTORE_BRANCH_BOOTSTRAP_FILE);
	PS_CHECK_PATH_FORMAT(pathlen, path);
	pagestore_require_prepared_artifact(dir, PAGESTORE_BRANCH_BOOTSTRAP_FILE,
										false);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open branch bootstrap artifact \"%s\": %m",
						path)));
	if (fstat(fd, &st) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat branch bootstrap artifact \"%s\": %m",
						path)));
	}
	if (st.st_size < (off_t) sizeof(*header) || st.st_size > PG_INT32_MAX)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("branch bootstrap artifact \"%s\" has an invalid size",
						path)));
	}
	artifact = palloc((Size) st.st_size);
	if (!pagestore_pread_exact(fd, artifact, (Size) st.st_size, 0))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read branch bootstrap artifact \"%s\": %m",
						path)));
	}
	CloseTransientFile(fd);
	header = (PagestoreBranchBootstrapHeader *) artifact;
	if (header->magic != PAGESTORE_BRANCH_BOOTSTRAP_MAGIC ||
		header->format != PAGESTORE_BRANCH_BOOTSTRAP_FORMAT ||
		header->artifact_size != (uint64) st.st_size ||
		header->new_timeline != (uint32) new_tl ||
		header->parent_timeline != (uint32) parent_tl ||
		header->checkpoint_redo != (uint64) checkpoint_redo ||
		header->recovery_lsn != (uint64) recovery_lsn ||
		header->fork_lsn != (uint64) fork_lsn ||
		header->system_identifier == 0 ||
		header->map_count < 2 ||
		(header->flags & ~PAGESTORE_BRANCH_BOOTSTRAP_HAS_USER_TABLESPACES) != 0 ||
		XLogRecPtrIsInvalid((XLogRecPtr) header->checkpoint_redo) ||
		header->recovery_lsn < header->checkpoint_redo ||
		header->fork_lsn < header->recovery_lsn ||
		header->map_count >
		((Size) st.st_size - sizeof(*header)) /
		sizeof(PagestoreBranchBootstrapMapHeader))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("branch bootstrap artifact \"%s\" has an invalid identity or header",
						path)));

	stored_crc = header->crc;
	header->crc = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, artifact, (Size) st.st_size);
	FIN_CRC32C(crc);
	header->crc = stored_crc;
	if (!EQ_CRC32C(crc, stored_crc))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("branch bootstrap artifact \"%s\" has an invalid checksum",
						path)));

	cursor = artifact + sizeof(*header);
	end = artifact + (Size) st.st_size;
	for (uint32 i = 0; i < header->map_count; i++)
	{
		PagestoreBranchBootstrapMapHeader entry;

		if ((Size) (end - cursor) < sizeof(entry))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("branch bootstrap artifact \"%s\" has a truncated relation-map table",
							path)));
		memcpy(&entry, cursor, sizeof(entry));
		cursor += sizeof(entry);
		if ((i == 0 && entry.database_oid != InvalidOid) ||
			(i > 0 && (entry.database_oid == InvalidOid ||
						   entry.database_oid <= previous_oid)) ||
			entry.size == 0 || entry.size > PAGESTORE_READER_RELMAP_MAX_SIZE ||
			(Size) (end - cursor) < entry.size)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("branch bootstrap artifact \"%s\" has an invalid relation-map entry",
							path)));
		previous_oid = entry.database_oid;
		cursor += entry.size;
	}
	if (cursor != end)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("branch bootstrap artifact \"%s\" has trailing data",
						path)));

	*header_out = header;
	return artifact;
}

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
	char		manifest[2048];
	int			manifest_len;

	manifest_len = snprintf(manifest, sizeof(manifest),
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
	if (manifest_len < 0 || manifest_len >= (int) sizeof(manifest))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("branch manifest is too large")));

	pagestore_publish_artifact(target_dir, "pagestore_branch.manifest",
						   "branch manifest", manifest, manifest_len);
}

static void
pagestore_write_reader_manifest(const char *target_dir, int32 timeline,
								XLogRecPtr base, XLogRecPtr read_lsn,
								uint32 parent_timeline,
								XLogRecPtr fork_lsn,
								TransactionId oldest_xid,
								TransactionId next_xid,
								TransactionId oldest_commit_ts_xid,
								TransactionId next_commit_ts_xid,
								MultiXactId oldest_multi,
								MultiXactId next_multi,
								int64 oldest_member, int64 next_member,
								int64 seeded_pages)
{
	char		manifest[2048];
	char		ancestry[128] = "";
	int			manifest_len;
	int			ancestry_len;

	if (timeline > 0)
	{
		ancestry_len = snprintf(ancestry, sizeof(ancestry),
								"  \"parent_timeline\": %u,\n"
								"  \"fork_lsn\": \"%X/%08X\",\n",
								parent_timeline, LSN_FORMAT_ARGS(fork_lsn));
		if (ancestry_len < 0 || ancestry_len >= (int) sizeof(ancestry))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("reader ancestry is too large")));
	}

	manifest_len = snprintf(manifest, sizeof(manifest),
							"{\n"
							"  \"format\": 2,\n"
							"  \"kind\": \"pinned_reader\",\n"
							"  \"catalog_provenance\": \"%s\",\n"
							"  \"timeline\": %d,\n"
							"%s"
							"  \"base_lsn\": \"%X/%08X\",\n"
							"  \"read_lsn\": \"%X/%08X\",\n"
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
							PAGESTORE_READER_CATALOG_FILE,
							timeline, ancestry, LSN_FORMAT_ARGS(base),
							LSN_FORMAT_ARGS(read_lsn), oldest_xid, next_xid,
							oldest_commit_ts_xid, next_commit_ts_xid,
							oldest_multi, next_multi,
							(long long) oldest_member, (long long) next_member,
							(long long) seeded_pages);
	if (manifest_len < 0 || manifest_len >= (int) sizeof(manifest))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("reader manifest is too large")));
	pagestore_publish_artifact(target_dir, "pagestore_reader.manifest",
						   "reader manifest", manifest, manifest_len);
}

static char *
pagestore_read_manifest(const char *target_dir, const char *filename,
						const char *kind)
{
	char		path[MAXPGPATH];
	FILE	   *file;
	StringInfoData buf;
	char		tmp[1024];
	size_t		nread;
	long		manifest_size;

	if (strlen(target_dir) + strlen(filename) + sizeof("/") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s target directory path is too long", kind)));

	snprintf(path, sizeof(path), "%s/%s", target_dir, filename);
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
				 errmsg("could not open %s manifest \"%s\": %m", kind, path)));
	}
	if (fseek(file, 0L, SEEK_END) != 0)
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read %s manifest \"%s\": %m", kind, path)));
	}
	manifest_size = ftell(file);
	if (manifest_size < 0 || manifest_size > PAGESTORE_MANIFEST_MAXLEN)
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("%s manifest \"%s\" is too large", kind, path)));
	}
	if (fseek(file, 0L, SEEK_SET) != 0)
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read %s manifest \"%s\": %m", kind, path)));
	}

	initStringInfo(&buf);
		while ((nread = fread(tmp, 1, sizeof(tmp), file)) > 0)
		{
			if (buf.len + nread > PAGESTORE_MANIFEST_MAXLEN)
			{
			FreeFile(file);
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("%s manifest \"%s\" is too large", kind, path)));
			}
			if (memchr(tmp, '\0', nread) != NULL)
			{
				FreeFile(file);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("%s manifest \"%s\" contains embedded NUL bytes",
							kind, path)));
			}
			appendBinaryStringInfo(&buf, tmp, nread);
		}
	if (ferror(file))
	{
		FreeFile(file);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read %s manifest \"%s\": %m", kind, path)));
	}
	FreeFile(file);
	return buf.data;
}

static char *
pagestore_read_branch_manifest(const char *target_dir)
{
	return pagestore_read_manifest(target_dir, "pagestore_branch.manifest",
							   "branch");
}

static char *
pagestore_read_reader_manifest(const char *target_dir)
{
	return pagestore_read_manifest(target_dir, "pagestore_reader.manifest",
							   "reader");
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

				if (memchr(name, '\\', end - name) != NULL)
					return NULL;
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
pagestore_manifest_get_xid_string_token(const char *manifest, const char *key,
										TransactionId *value)
{
	const char *field = pagestore_manifest_find_unique_field(manifest, key);
	char	   *endptr;
	unsigned long long parsed;

	if (field == NULL || *field != '"')
		return false;
	field++;
	if (*field < '0' || *field > '9')
		return false;
	errno = 0;
	parsed = strtoull(field, &endptr, 10);
	if (errno != 0 || endptr == field)
		return false;
	if (parsed > UINT32_MAX)
		return false;
	if (*endptr != '"')
		return false;
	if (!pagestore_manifest_value_delimited(endptr + 1))
		return false;
	*value = (TransactionId) parsed;
	return true;
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
	unsigned long long hi,
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
	if (sscanf(buf, "%llX/%llX%c", &hi, &lo, &extra) != 2)
		return false;
	if (hi > UINT32_MAX || lo > UINT32_MAX)
		return false;
	if (!pagestore_manifest_value_delimited(end + 1))
		return false;
	*lsn = ((uint64) hi << 32) | lo;
	return true;
}

static bool
pagestore_manifest_parse_value(const char **pp);

static bool
pagestore_manifest_parse_string(const char **pp)
{
	const char *p = *pp;
	int			i;

	if (*p != '"')
		return false;
	p++;
	while (*p != '\0')
	{
		if ((unsigned char) *p < 0x20)
			return false;
		if (*p == '"')
		{
			*pp = p + 1;
			return true;
		}
		if (*p == '\\')
		{
			p++;
			if (*p == '"' || *p == '\\' || *p == '/' ||
				*p == 'b' || *p == 'f' || *p == 'n' ||
				*p == 'r' || *p == 't')
			{
				p++;
				continue;
			}
			if (*p == 'u')
			{
				for (i = 0; i < 4; i++)
				{
					p++;
					if (!((*p >= '0' && *p <= '9') ||
						  (*p >= 'a' && *p <= 'f') ||
						  (*p >= 'A' && *p <= 'F')))
						return false;
				}
				p++;
				continue;
			}
			return false;
		}
		p++;
	}
	return false;
}

static bool
pagestore_manifest_parse_number(const char **pp)
{
	const char *p = *pp;

	if (*p == '-')
		p++;
	if (*p == '0')
		p++;
	else
	{
		if (*p < '1' || *p > '9')
			return false;
		while (*p >= '0' && *p <= '9')
			p++;
	}
	if (*p == '.')
	{
		p++;
		if (*p < '0' || *p > '9')
			return false;
		while (*p >= '0' && *p <= '9')
			p++;
	}
	if (*p == 'e' || *p == 'E')
	{
		p++;
		if (*p == '+' || *p == '-')
			p++;
		if (*p < '0' || *p > '9')
			return false;
		while (*p >= '0' && *p <= '9')
			p++;
	}
	*pp = p;
	return true;
}

static bool
pagestore_manifest_parse_literal(const char **pp, const char *literal)
{
	size_t		len = strlen(literal);

	if (strncmp(*pp, literal, len) != 0)
		return false;
	*pp += len;
	return true;
}

static bool
pagestore_manifest_parse_array(const char **pp)
{
	const char *p = pagestore_manifest_skip_ws(*pp + 1);

	if (*p == ']')
	{
		*pp = p + 1;
		return true;
	}
	for (;;)
	{
		if (!pagestore_manifest_parse_value(&p))
			return false;
		p = pagestore_manifest_skip_ws(p);
		if (*p == ']')
		{
			*pp = p + 1;
			return true;
		}
		if (*p != ',')
			return false;
		p = pagestore_manifest_skip_ws(p + 1);
	}
}

static bool
pagestore_manifest_parse_object(const char **pp)
{
	const char *p = pagestore_manifest_skip_ws(*pp + 1);

	if (*p == '}')
	{
		*pp = p + 1;
		return true;
	}
	for (;;)
	{
		if (!pagestore_manifest_parse_string(&p))
			return false;
		p = pagestore_manifest_skip_ws(p);
		if (*p != ':')
			return false;
		p = pagestore_manifest_skip_ws(p + 1);
		if (!pagestore_manifest_parse_value(&p))
			return false;
		p = pagestore_manifest_skip_ws(p);
		if (*p == '}')
		{
			*pp = p + 1;
			return true;
		}
		if (*p != ',')
			return false;
		p = pagestore_manifest_skip_ws(p + 1);
	}
}

static bool
pagestore_manifest_parse_value(const char **pp)
{
	const char *p = pagestore_manifest_skip_ws(*pp);

	if (*p == '"')
	{
		if (!pagestore_manifest_parse_string(&p))
			return false;
	}
	else if (*p == '{')
	{
		if (!pagestore_manifest_parse_object(&p))
			return false;
	}
	else if (*p == '[')
	{
		if (!pagestore_manifest_parse_array(&p))
			return false;
	}
	else if (*p == '-' || (*p >= '0' && *p <= '9'))
	{
		if (!pagestore_manifest_parse_number(&p))
			return false;
	}
	else if (*p == 't')
	{
		if (!pagestore_manifest_parse_literal(&p, "true"))
			return false;
	}
	else if (*p == 'f')
	{
		if (!pagestore_manifest_parse_literal(&p, "false"))
			return false;
	}
	else if (*p == 'n')
	{
		if (!pagestore_manifest_parse_literal(&p, "null"))
			return false;
	}
	else
		return false;

	*pp = p;
	return true;
}

static bool
pagestore_manifest_is_single_object(const char *manifest)
{
	const char *p = pagestore_manifest_skip_ws(manifest);

	if (*p != '{')
		return false;
	if (!pagestore_manifest_parse_object(&p))
		return false;
	p = pagestore_manifest_skip_ws(p);
	return *p == '\0';
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
pagestore_reader_manifest_get_branch_identity(const char *manifest,
											  uint32 *timeline,
											  uint32 *parent_timeline,
											  XLogRecPtr *fork_lsn)
{
	uint32		format;

	return pagestore_manifest_is_single_object(manifest) &&
		pagestore_manifest_get_uint_token(manifest, "format", &format) &&
		format == 2 &&
		pagestore_manifest_has_string_token(manifest, "kind", "pinned_reader") &&
		pagestore_manifest_has_string_token(manifest, "catalog_provenance",
										PAGESTORE_READER_CATALOG_FILE) &&
		pagestore_manifest_get_uint_token(manifest, "timeline", timeline) &&
		*timeline != 0 &&
		pagestore_manifest_get_uint_token(manifest, "parent_timeline",
										 parent_timeline) &&
		pagestore_manifest_get_lsn_token(manifest, "fork_lsn", fork_lsn);
}

static bool
pagestore_reader_manifest_matches(const char *manifest, int32 timeline,
								 XLogRecPtr read_lsn)
{
	char		buf[64];
	int			len;
	uint32		manifest_timeline;
	uint32		parent_timeline;
	XLogRecPtr	fork_lsn;

	if (timeline < 0 || XLogRecPtrIsInvalid(read_lsn) ||
		!pagestore_manifest_is_single_object(manifest) ||
		!pagestore_manifest_has_uint_token(manifest, "format", 2) ||
		!pagestore_manifest_has_string_token(manifest, "kind", "pinned_reader") ||
		!pagestore_manifest_has_string_token(manifest, "catalog_provenance",
										 PAGESTORE_READER_CATALOG_FILE) ||
		!pagestore_manifest_has_uint_token(manifest, "timeline", timeline))
		return false;
	if (timeline > 0 &&
		(!pagestore_reader_manifest_get_branch_identity(manifest,
												  &manifest_timeline,
												  &parent_timeline,
												  &fork_lsn) ||
		 manifest_timeline != (uint32) timeline))
		return false;
	len = snprintf(buf, sizeof(buf), "%X/%08X", LSN_FORMAT_ARGS(read_lsn));
	if (len < 0 || len >= (int) sizeof(buf))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("reader manifest LSN token is too large")));
	return pagestore_manifest_has_string_token(manifest, "read_lsn", buf);
}

static bool
pagestore_manifest_has_line(const char *manifest, const char *line)
{
	return strstr(manifest, line) != NULL;
}

static bool
pagestore_existing_branch_manifest_matches(const char *target_dir,
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
										   int64 *seeded_pages)
{
	char	   *manifest;
	char		line[128];
	char	   *seeded_start;
	char	   *seeded_end;
	long long	seeded;
	int			len;
	volatile bool read_failed = false;

#define CHECK_MANIFEST_LINE(...) \
	do { \
		len = snprintf(line, sizeof(line), __VA_ARGS__); \
		if (len < 0 || len >= (int) sizeof(line) || \
			!pagestore_manifest_has_line(manifest, line)) \
			return false; \
	} while (0)

	PG_TRY();
	{
		manifest = pagestore_read_branch_manifest(target_dir);
	}
	PG_CATCH();
	{
		FlushErrorState();
		read_failed = true;
	}
	PG_END_TRY();
	if (read_failed)
		return false;
	if (manifest == NULL)
		return false;
	if (!pagestore_manifest_matches(manifest, new_tl, parent_tl, target))
		return false;

	CHECK_MANIFEST_LINE("  \"base_lsn\": \"%X/%08X\",\n", LSN_FORMAT_ARGS(base));
	CHECK_MANIFEST_LINE("  \"oldest_xid\": \"%u\",\n", oldest_xid);
	CHECK_MANIFEST_LINE("  \"next_xid\": \"%u\",\n", next_xid);
	CHECK_MANIFEST_LINE("  \"oldest_commit_ts_xid\": \"%u\",\n", oldest_commit_ts_xid);
	CHECK_MANIFEST_LINE("  \"next_commit_ts_xid\": \"%u\",\n", next_commit_ts_xid);
	CHECK_MANIFEST_LINE("  \"oldest_multi\": \"%u\",\n", oldest_multi);
	CHECK_MANIFEST_LINE("  \"next_multi\": \"%u\",\n", next_multi);
	CHECK_MANIFEST_LINE("  \"oldest_member\": \"%lld\",\n", (long long) oldest_member);
	CHECK_MANIFEST_LINE("  \"next_member\": \"%lld\",\n", (long long) next_member);

	seeded_start = strstr(manifest, "  \"seeded_slru_pages\": \"");
	if (seeded_start == NULL)
		return false;
	seeded_start += strlen("  \"seeded_slru_pages\": \"");
	errno = 0;
	seeded = strtoll(seeded_start, &seeded_end, 10);
	if (errno != 0 || seeded_end == seeded_start || seeded < 0 ||
		strncmp(seeded_end, "\"\n", 2) != 0)
		return false;
	*seeded_pages = (int64) seeded;
	return true;

#undef CHECK_MANIFEST_LINE
}

/*
 * Require every entry under an artifact directory to be a regular file or a
 * (recursively validated) subdirectory.  copydir() classifies entries without
 * following symlinks and silently skips anything else, so a symlinked SLRU
 * segment would survive preflight on its container yet be absent from the
 * installed tree after the target has already been replaced.
 */
static void
pagestore_require_regular_tree(const char *path)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(path);
	while ((de = ReadDir(dir, path)) != NULL)
	{
		char		sub[MAXPGPATH];
		struct stat st;
		int			pathlen;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;
		pathlen = snprintf(sub, sizeof(sub), "%s/%s", path, de->d_name);
		PS_CHECK_PATH_FORMAT(pathlen, sub);
		if (lstat(sub, &st) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat prepared branch artifact entry \"%s\": %m",
							sub)));
		if (S_ISDIR(st.st_mode))
			pagestore_require_regular_tree(sub);
		else if (!S_ISREG(st.st_mode))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("prepared branch artifact entry \"%s\" has the wrong file type",
							sub)));
	}
	FreeDir(dir);
}

static void
pagestore_require_prepared_artifact(const char *prepared_dir,
									const char *relpath, bool directory)
{
	char		path[MAXPGPATH];
	struct stat st;

	if (strlen(prepared_dir) + strlen(relpath) + sizeof("/") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("prepared branch artifact path is too long")));

	snprintf(path, sizeof(path), "%s/%s", prepared_dir, relpath);

	/*
	 * lstat, not stat: copydir() classifies entries without following
	 * symlinks and silently skips them, so a symlinked artifact would pass a
	 * stat-based check here and then be missing from the installed tree.
	 */
	if (lstat(path, &st) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("prepared branch artifact \"%s\" is missing: %m", path)));
	if (directory ? !S_ISDIR(st.st_mode) : !S_ISREG(st.st_mode))
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("prepared branch artifact \"%s\" has the wrong file type",
							path)));
	if (directory)
		pagestore_require_regular_tree(path);
}

/*
 * Absolutize (against the backend cwd, i.e. the data directory) and
 * canonicalize an install path so relative and absolute spellings of the same
 * tree compare equal.
 */
static void
pagestore_canonical_install_path(const char *path, char *abs, size_t abslen)
{
	if (is_absolute_path(path))
	{
		if (strlcpy(abs, path, abslen) >= abslen)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("branch install path is too long")));
	}
	else
	{
		char		cwd[MAXPGPATH];

		if (!getcwd(cwd, sizeof(cwd)))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not determine current directory: %m")));
		if (snprintf(abs, abslen, "%s/%s", cwd, path) >= (int) abslen)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("branch install path is too long")));
	}
	canonicalize_path(abs);
}

/*
 * Reject install calls whose prepared and target dirs are the same directory
 * or nest one inside the other: install mutates the target (manifest unlink,
 * SLRU rmtree+rename), so an overlapping source would be destroyed or copied
 * into itself.  Both paths are absolutized so relative and absolute spellings
 * compare, then checked textually plus by dir inode; this is a guard against
 * typos, not a defense against adversarial symlink layouts.
 */
static void
pagestore_require_disjoint_install_dirs(const char *prepared_dir,
										const char *target_dir)
{
	char		prep[MAXPGPATH];
	char		targ[MAXPGPATH];
	size_t		preplen;
	size_t		targlen;
	struct stat prepst;
	struct stat targst;

	pagestore_canonical_install_path(prepared_dir, prep, sizeof(prep));
	pagestore_canonical_install_path(target_dir, targ, sizeof(targ));
	preplen = strlen(prep);
	targlen = strlen(targ);
	if (strcmp(prep, targ) == 0 ||
		(preplen < targlen && strncmp(prep, targ, preplen) == 0 &&
		 targ[preplen] == '/') ||
		(targlen < preplen && strncmp(targ, prep, targlen) == 0 &&
		 prep[targlen] == '/') ||
		(stat(prep, &prepst) == 0 && stat(targ, &targst) == 0 &&
		 prepst.st_dev == targst.st_dev && prepst.st_ino == targst.st_ino))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("prepared branch dir \"%s\" and install target dir \"%s\" must not overlap",
						prepared_dir, target_dir)));
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

static bool
pagestore_manifest_get_commit_ts_required(const char *manifest, bool *required)
{
	TransactionId oldest_xid;
	TransactionId next_xid;

	if (!pagestore_manifest_get_xid_string_token(manifest,
												 "oldest_commit_ts_xid",
												 &oldest_xid) ||
		!pagestore_manifest_get_xid_string_token(manifest,
												 "next_commit_ts_xid",
												 &next_xid))
		return false;

	if (!TransactionIdIsNormal(oldest_xid) &&
		!TransactionIdIsNormal(next_xid))
	{
		*required = false;
		return true;
	}
	if (TransactionIdIsNormal(oldest_xid) &&
		TransactionIdIsNormal(next_xid) &&
		!TransactionIdFollows(oldest_xid, next_xid))
	{
		*required = true;
		return true;
	}
	return false;
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

PG_FUNCTION_INFO_V1(pagestore_validate_reader_manifest);
Datum
pagestore_validate_reader_manifest(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		timeline = PG_GETARG_INT32(1);
	XLogRecPtr	read_lsn = PG_GETARG_LSN(2);
	char	   *manifest;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to validate a reader manifest")));
	if (!pagestore_branch_backend_active())
		ereport(ERROR,
				(errmsg("pagestore.backend must be \"localsvc\" to validate a reader manifest")));
	manifest = pagestore_read_reader_manifest(target_dir);
	PG_RETURN_BOOL(manifest != NULL &&
				   pagestore_reader_manifest_matches(manifest, timeline, read_lsn));
}

/*
 * Attest that target_dir is the control plane's catalog snapshot for R.  The
 * exact-R pg_control must already be installed, so the durable artifact cannot
 * be minted for an arbitrary running or copied data directory.
 */
PG_FUNCTION_INFO_V1(pagestore_mark_reader_catalog_snapshot);
Datum
pagestore_mark_reader_catalog_snapshot(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		timeline = PG_GETARG_INT32(1);
	XLogRecPtr	read_lsn = PG_GETARG_LSN(2);
	ControlFileData *control;
	ControlFileData mirrored;
	bool		crc_ok;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to mark a reader catalog snapshot")));
	if (timeline < 0 || XLogRecPtrIsInvalid(read_lsn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid reader timeline or read LSN")));
	if (!pagestore_branch_backend_active())
		ereport(ERROR,
				(errmsg("pagestore.backend must be \"localsvc\" to mark a reader catalog snapshot")));
	if ((uint32) timeline != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errmsg("reader timeline %d is not the active localsvc timeline %u",
						timeline, pagestore_localsvc_timeline())));
	if (!ps_control_asof(read_lsn, &mirrored) ||
		mirrored.checkPointCopy.redo != read_lsn)
		ereport(ERROR,
				(errmsg("reader LSN is not a durably mirrored checkpoint redo")));
	control = get_controlfile(target_dir, &crc_ok);
	if (!crc_ok || control->checkPointCopy.redo != read_lsn ||
		control->system_identifier != mirrored.system_identifier)
		ereport(ERROR,
				(errmsg("catalog snapshot pg_control does not match the requested reader checkpoint"),
				 errdetail("The catalog snapshot requires checkpoint redo %X/%08X.",
						   LSN_FORMAT_ARGS(read_lsn))));
	pagestore_write_reader_catalog_provenance(target_dir, (uint32) timeline,
										 read_lsn,
										 control->system_identifier);
	pfree(control);
	PG_RETURN_VOID();
}

static void
pagestore_validate_datadir_branch_manifest(void)
{
	char	   *manifest;
	char	   *reader_manifest;
	uint32_t	new_tl;
	uint32_t	parent_tl;
	uint32		reader_tl;
	uint32		reader_parent_tl;
	XLogRecPtr	fork_lsn;
	XLogRecPtr	reader_fork_lsn;
	uint64		read_lsn;
	uint64		read_seq = 0;
	bool		found;
	uint8		retention_status;
	PsRetentionPin existing_pin;
	bool		have_existing_pin;
	bool		set_reader_pin;
	bool		provisional_reader_pin = false;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	pagestore_reader_horizon = ShmemInitStruct("pagestore reader horizon",
											 sizeof(PagestoreReaderHorizonShmem),
											 &found);
	if (!found)
	{
		SpinLockInit(&pagestore_reader_horizon->mutex);
		pagestore_reader_horizon->candidate_lsn =
			(XLogRecPtr) pagestore_localsvc_read_lsn();
		pagestore_reader_horizon->candidate_generation =
			pagestore_localsvc_read_lsn() != 0 ? 1 : 0;
		pagestore_reader_horizon->adoptable_lsn =
			(XLogRecPtr) pagestore_localsvc_read_lsn();
		pagestore_reader_horizon->adoptable_generation =
			pagestore_localsvc_read_lsn() != 0 ? 1 : 0;
		pagestore_reader_horizon->refreshed_at = 0;
		pagestore_reader_horizon->refresh_owner_pid = 0;
		pagestore_reader_horizon->refresh_started_at = 0;
	}
	pagestore_reader_snapshot_job = ShmemInitStruct(
		"pagestore reader snapshot job",
		sizeof(PagestoreReaderSnapshotJobShmem), &found);
	if (!found)
	{
		SpinLockInit(&pagestore_reader_snapshot_job->mutex);
		memset(&pagestore_reader_snapshot_job->control, 0,
			   sizeof(ControlFileData));
		pagestore_reader_snapshot_job->generation = 0;
	}
	LWLockRelease(AddinShmemInitLock);
	if (DataDir == NULL)
		return;

	manifest = pagestore_read_branch_manifest(DataDir);
	reader_manifest = pagestore_read_reader_manifest(DataDir);
	read_lsn = pagestore_localsvc_read_lsn();
	if (manifest != NULL && reader_manifest != NULL)
		ereport(FATAL,
				(errmsg("a data directory cannot contain both pagestore branch and reader manifests")));
	if (reader_manifest != NULL)
	{
		ControlFileData *control;
		PagestoreReaderSnapshot *reader_snapshot;
		bool		crc_ok;

		if (!pagestore_branch_backend_active())
			ereport(FATAL,
					(errmsg("pagestore.backend must be \"localsvc\" to validate a reader manifest")));
		if (read_lsn == 0)
			ereport(FATAL,
					(errmsg("pagestore_reader.manifest requires pagestore.read_lsn")));
		if (!pagestore_branch_routing_active())
			ereport(FATAL,
					(errmsg("pagestore.route_all must be enabled to use a reader manifest")));
		if (!pagestore_reader_manifest_matches(reader_manifest,
										 (int32) pagestore_localsvc_timeline(),
										 (XLogRecPtr) read_lsn))
			ereport(FATAL,
					(errmsg("pagestore.read_lsn or timeline does not match pagestore_reader.manifest")));
		control = get_controlfile(DataDir, &crc_ok);
		if (!crc_ok || control->checkPointCopy.redo != (XLogRecPtr) read_lsn)
			ereport(FATAL,
					(errmsg("pg_control does not match pagestore_reader.manifest"),
					 errdetail("The reader requires checkpoint redo %X/%08X.",
							   LSN_FORMAT_ARGS((XLogRecPtr) read_lsn))));
		pagestore_load_reader_catalog_provenance(DataDir,
			pagestore_localsvc_timeline(), (XLogRecPtr) read_lsn,
			control->system_identifier, FATAL);
		pfree(control);
		reader_snapshot = pagestore_load_reader_snapshot(DataDir,
			pagestore_localsvc_timeline(), (XLogRecPtr) read_lsn,
			TopMemoryContext, FATAL);
		if (reader_snapshot->xids != NULL)
			pfree(reader_snapshot->xids);
		pfree(reader_snapshot);
		/*
		 * This is the last local-only validation step.  Install a conservative
		 * zero-sequence pin before even reading the exact admission fence.  It
		 * protects every same-LSN variant while the fence is resolved, closing
		 * the read-fence/SET race for a new or advancing owner.
		 */
		have_existing_pin = pagestore_find_retention_owner(
			pagestore_localsvc_timeline(), PS_RETENTION_OWNER_READER,
			pagestore_retention_owner_id, &existing_pin);
		set_reader_pin = !have_existing_pin;
		if (have_existing_pin)
		{
			if (existing_pin.generation > pagestore_retention_owner_generation)
				ereport(FATAL,
						(errmsg("pagestore reader retention generation is stale"),
						 errdetail("Owner %llu generation %u was fenced by generation %u.",
								   (unsigned long long) pagestore_retention_owner_id,
								   pagestore_retention_owner_generation,
								   existing_pin.generation)));
			/* A different postmaster can still be serving the old generation;
			 * this process-local startup path cannot prove it quiescent.  The
			 * controller must stop it and durably DROP that generation before a
			 * replacement may register against the tombstone. */
			if (existing_pin.generation < pagestore_retention_owner_generation)
				ereport(FATAL,
						(errmsg("pagestore reader generation takeover is not quiesced"),
						 errhint("Stop the old reader and durably drop its generation before starting the replacement.")));
			if (existing_pin.resources != PS_READER_RETENTION_RESOURCES)
				ereport(FATAL,
						(errmsg("pagestore reader retention owner has the wrong resource mask")));
			if (!pagestore_advance_read_lsn && existing_pin.lsn > read_lsn)
				ereport(FATAL,
						(errmsg("fixed pagestore reader owner is already above its configured horizon"),
						 errhint("Reprovision the fixed reader with a new owner identity at a retained horizon.")));
			set_reader_pin = existing_pin.lsn < read_lsn;
		}
		if (set_reader_pin)
		{
			retention_status = pagestore_localsvc_retention_set_timeout(
				pagestore_localsvc_timeline(), PS_RETENTION_OWNER_READER,
				pagestore_retention_owner_id,
				pagestore_retention_owner_generation,
				PS_READER_RETENTION_RESOURCES, read_lsn, 0,
				PS_READER_RETENTION_TIMEOUT_MS);
			if (retention_status == PS_STATUS_STALE)
				ereport(FATAL,
						(errmsg("pagestore reader retention generation is stale")));
			if (retention_status != PS_STATUS_OK)
				ereport(FATAL,
						(errmsg("pagestore reader could not install its provisional retention owner")));
			provisional_reader_pin = true;
		}
		if (!pagestore_localsvc_read_fence_timeout(read_lsn, &read_seq,
				PAGESTORE_READER_HORIZON_TIMEOUT_MS))
			ereport(FATAL,
					(errmsg("pagestore reader has no durable admission fence at its configured horizon")));
		set_reader_pin = provisional_reader_pin ||
			(have_existing_pin && existing_pin.lsn == read_lsn &&
			 existing_pin.admission_seq != read_seq);
		if (set_reader_pin)
		{
			retention_status = pagestore_localsvc_retention_set_timeout(
				pagestore_localsvc_timeline(), PS_RETENTION_OWNER_READER,
				pagestore_retention_owner_id,
				pagestore_retention_owner_generation,
				PS_READER_RETENTION_RESOURCES, read_lsn,
				read_seq,
				PS_READER_RETENTION_TIMEOUT_MS);
			if (retention_status == PS_STATUS_STALE)
				ereport(FATAL,
						(errmsg("pagestore reader retention generation is stale"),
						 errdetail("Owner %llu generation %u was fenced by a newer controller generation.",
								   (unsigned long long) pagestore_retention_owner_id,
								   pagestore_retention_owner_generation)));
			if (retention_status != PS_STATUS_OK)
				ereport(FATAL,
						(errmsg("pagestore reader could not register its retention owner")));
		}
		else if (existing_pin.lsn > read_lsn)
		{
			SpinLockAcquire(&pagestore_reader_horizon->mutex);
			pagestore_reader_horizon->candidate_lsn =
				(XLogRecPtr) existing_pin.lsn;
			SpinLockRelease(&pagestore_reader_horizon->mutex);
		}
		pagestore_localsvc_detach();
		if (pagestore_localsvc_timeline() != 0)
		{
			if (!pagestore_reader_manifest_get_branch_identity(reader_manifest,
													  &reader_tl,
													  &reader_parent_tl,
													  &reader_fork_lsn))
				ereport(FATAL,
						(errmsg("invalid branch identity in pagestore reader manifest")));
			pagestore_localsvc_require_branch_timeout(reader_tl,
											  reader_parent_tl,
											  (uint64) reader_fork_lsn, 5000);
			pagestore_localsvc_detach();
		}
		return;
	}
	if (read_lsn != 0)
		ereport(FATAL,
				(errmsg("pagestore.read_lsn requires pagestore_reader.manifest")));
	if (manifest == NULL)
	{
		if (pagestore_localsvc_timeline() != 0)
			ereport(FATAL,
					(errmsg("pagestore.timeline requires pagestore_branch.manifest")));
		return;
	}
	if (!pagestore_branch_backend_active())
		ereport(FATAL,
				(errmsg("pagestore.backend must be \"localsvc\" to validate a branch manifest")));
	if (!pagestore_branch_routing_active())
		ereport(FATAL,
				(errmsg("pagestore.route_all must be enabled to use a branch manifest")));
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

/*
 * Replace <target_dir>/<relpath> with an empty directory, via the same
 * stage-and-rename shape as pagestore_install_prepared_dir().  Used when the
 * manifest says an SLRU was never active at the fork: the branch's fork state
 * for it is empty, and whatever the copied target datadir still carries is
 * post-fork noise that must not survive the install.
 */
static void
pagestore_install_empty_dir(const char *target_dir, const char *relpath)
{
	char		dst[MAXPGPATH];
	char		stage[MAXPGPATH];

	if (strlen(target_dir) + strlen(relpath) + sizeof(".install/") > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch install path is too long")));

	snprintf(dst, sizeof(dst), "%s/%s", target_dir, relpath);
	snprintf(stage, sizeof(stage), "%s/%s.install", target_dir, relpath);
	if (access(stage, F_OK) == 0 && !rmtree(stage, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear branch install staging dir \"%s\"", stage)));
	if (MakePGDirectory(stage) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch install staging dir \"%s\": %m",
						stage)));
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

static void
pagestore_require_real_directory(const char *path, const char *kind)
{
	struct stat st;

	if (lstat(path, &st) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat %s directory \"%s\": %m", kind, path)));
	if (!S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s path \"%s\" is not a directory", kind, path)));
}

/*
 * A portable install targets an offline, freshly initdb'd skeleton.  Validate
 * all of its fixed directories and every existing database directory before
 * writing the first map.  User tablespace links would carry a different local
 * filesystem topology, so both the source artifact and target must be empty
 * until that topology gets its own portable artifact format.
 */
static void
pagestore_preflight_branch_bootstrap_target(const char *target_dir,
										 const char *artifact,
										 const PagestoreBranchBootstrapHeader *header)
{
	const char *cursor = artifact + sizeof(*header);
	char		base[MAXPGPATH];
	char		global[MAXPGPATH];
	char		tblspc[MAXPGPATH];
	char		pidfile[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	struct stat pidst;
	int			pathlen;

	pagestore_require_real_directory(target_dir, "branch target");
	pathlen = snprintf(base, sizeof(base), "%s/base", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, base);
	pathlen = snprintf(global, sizeof(global), "%s/global", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, global);
	pathlen = snprintf(tblspc, sizeof(tblspc), "%s/pg_tblspc", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, tblspc);
	pathlen = snprintf(pidfile, sizeof(pidfile), "%s/postmaster.pid", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, pidfile);
	pagestore_require_real_directory(base, "branch base");
	pagestore_require_real_directory(global, "branch global");
	pagestore_require_real_directory(tblspc, "branch tablespace");
	if (lstat(pidfile, &pidst) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("branch target \"%s\" has a postmaster.pid file",
						target_dir),
				 errhint("Stop the target server before installing bootstrap artifacts.")));
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat branch target lock file \"%s\": %m",
						pidfile)));

	dir = AllocateDir(tblspc);
	while ((de = ReadDir(dir, tblspc)) != NULL)
	{
		if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0)
		{
			FreeDir(dir);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("portable branch bootstrap requires an empty target pg_tblspc")));
		}
	}
	FreeDir(dir);

	for (uint32 i = 0; i < header->map_count; i++)
	{
		PagestoreBranchBootstrapMapHeader entry;
		char		mapdir[MAXPGPATH];
		struct stat st;

		memcpy(&entry, cursor, sizeof(entry));
		cursor += sizeof(entry) + entry.size;
		if (entry.database_oid == InvalidOid)
			continue;
		pathlen = snprintf(mapdir, sizeof(mapdir), "%s/%u", base,
						   entry.database_oid);
		PS_CHECK_PATH_FORMAT(pathlen, mapdir);
		if (lstat(mapdir, &st) == 0)
		{
			if (!S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("branch database path \"%s\" is not a directory",
								mapdir)));
		}
		else if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat branch database directory \"%s\": %m",
							mapdir)));
	}
}

static void
pagestore_install_branch_bootstrap_maps(const char *prepared_dir,
										const char *target_dir,
										const char *artifact,
										const PagestoreBranchBootstrapHeader *header)
{
	const char *cursor = artifact + sizeof(*header);
	char		base[MAXPGPATH];
	int			pathlen;

	pathlen = snprintf(base, sizeof(base), "%s/base", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, base);
	for (uint32 i = 0; i < header->map_count; i++)
	{
		PagestoreBranchBootstrapMapHeader entry;
		const char *mapdata;
		char		mapdir[MAXPGPATH];
		char		initpath[MAXPGPATH];
		struct stat st;

		memcpy(&entry, cursor, sizeof(entry));
		cursor += sizeof(entry);
		mapdata = cursor;
		cursor += entry.size;
		if (entry.database_oid == InvalidOid)
			pathlen = snprintf(mapdir, sizeof(mapdir), "%s/global", target_dir);
		else
			pathlen = snprintf(mapdir, sizeof(mapdir), "%s/%u", base,
							   entry.database_oid);
		PS_CHECK_PATH_FORMAT(pathlen, mapdir);
		if (lstat(mapdir, &st) != 0)
		{
			if (errno != ENOENT || MakePGDirectory(mapdir) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create branch database directory \"%s\": %m",
								mapdir)));
			fsync_fname(base, true);
		}
		else if (!S_ISDIR(st.st_mode))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("branch database path \"%s\" is not a directory",
							mapdir)));
		/* initdb's relcache files can describe different catalog filenodes.
		 * Rebuild them from the restored map and catalog pages on first boot. */
		pathlen = snprintf(initpath, sizeof(initpath), "%s/pg_internal.init",
							   mapdir);
		PS_CHECK_PATH_FORMAT(pathlen, initpath);
		if (unlink(initpath) == 0)
			fsync_fname(mapdir, true);
		else if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove stale relcache init file \"%s\": %m",
							initpath)));
		pagestore_publish_artifact(mapdir, "pg_filenode.map",
								   "branch relation map", mapdata,
								   (int) entry.size);
	}
	pagestore_install_prepared_file(prepared_dir, target_dir,
									PAGESTORE_BRANCH_BOOTSTRAP_FILE, true);
}

/*
 * pagestore_install_prepared_branch(prepared_dir text, target_dir text,
 *                                  new_timeline int, parent_timeline int,
 *                                  fork_lsn pg_lsn)
 * returns void
 *
 * Install the artifacts produced by pagestore_prepare_branch() into an
 * initdb/copied branch datadir.  pg_xact and pg_multixact are always
 * materialized by prepare (multixact seeds bootstrap pages even for an empty
 * horizon), so both are required; pg_commit_ts is required only when the
 * manifest's commit-ts horizons say it was seeded, and is otherwise reset to
 * the empty fork state in the target.  The prepared manifest must
 * match the expected branch identity before any artifact is installed, and the
 * manifest is installed last so its presence remains the startup-time signal
 * that the datadir has a prepared branch identity and must pass timeline
 * validation.
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
	char	   *target_manifest;
	char		stage[MAXPGPATH];
	uint32_t	target_new_tl;
	uint32_t	target_parent_tl;
	XLogRecPtr	target_fork_lsn;
	bool		commit_ts_required;

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
	pagestore_require_disjoint_install_dirs(prepared_dir, target_dir);
	manifest_data = pagestore_read_branch_manifest(prepared_dir);
	if (manifest_data == NULL)
		ereport(ERROR,
				(errmsg("prepared branch manifest is missing")));
	if (!pagestore_manifest_matches(manifest_data, new_tl, parent_tl, fork_lsn))
		ereport(ERROR,
				(errmsg("prepared branch manifest does not match the requested branch identity")));
	if (!pagestore_manifest_get_commit_ts_required(manifest_data,
												   &commit_ts_required))
		ereport(ERROR,
				(errmsg("prepared branch manifest has invalid commit-ts horizons")));
	pagestore_require_prepared_artifact(prepared_dir, "pg_xact", true);
	if (commit_ts_required)
		pagestore_require_prepared_artifact(prepared_dir, "pg_commit_ts", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact/offsets", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact/members", true);
	pagestore_require_prepared_artifact(prepared_dir,
										"pagestore_branch.manifest", false);
	target_manifest = pagestore_read_branch_manifest(target_dir);
	if (target_manifest != NULL &&
		!pagestore_manifest_matches(target_manifest, new_tl, parent_tl, fork_lsn))
	{
		if (parent_tl <= 0 ||
			!pagestore_manifest_get_branch_identity(target_manifest, &target_new_tl,
													&target_parent_tl,
													&target_fork_lsn) ||
			target_new_tl != (uint32_t) parent_tl)
			ereport(ERROR,
					(errmsg("target branch manifest does not match the requested branch identity")));
		pagestore_localsvc_require_branch(target_new_tl, target_parent_tl,
										  (uint64) target_fork_lsn);
	}

	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_xact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_commit_ts", commit_ts_required);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_multixact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pagestore_branch.manifest", true);

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create branch dir \"%s\": %m", target_dir)));
	snprintf(stage, sizeof(stage), "%s/pagestore_branch.manifest", target_dir);
	if (access(stage, F_OK) == 0)
	{
		if (unlink(stage) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not clear existing branch artifact \"%s\": %m", stage)));
		fsync_fname(target_dir, true);
	}
	snprintf(stage, sizeof(stage), "%s/pagestore_branch.manifest.install", target_dir);
	if (access(stage, F_OK) == 0 && unlink(stage) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear existing branch artifact staging file \"%s\": %m", stage)));
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_xact", true);
	if (commit_ts_required)
		pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_commit_ts",
									   true);
	else
		/*
		 * The manifest says commit-ts was never active at the fork, so ignore
		 * any pg_commit_ts a reused prepared dir may still carry and reset
		 * the target's to the fork state: empty.
		 */
		pagestore_install_empty_dir(target_dir, "pg_commit_ts");
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_multixact", true);
	pagestore_install_prepared_file(prepared_dir, target_dir,
									"pagestore_branch.manifest", true);

	PG_RETURN_VOID();
}

/*
 * Install a prepared branch into a fresh same-build initdb skeleton without a
 * parent PGDATA copy.  The caller first restores exact checkpoint control with
 * pagestore_control_restore --archive-bootstrap.  This entrypoint binds that
 * control file to the prepared bootstrap artifact, installs every source
 * relation map and the SLRU bundle, and publishes the ordinary branch manifest
 * last.  Skeleton WAL in target/pg_wal is deliberately a separate orchestrator
 * step: archive recovery must start with no foreign initdb WAL.
 */
PG_FUNCTION_INFO_V1(pagestore_install_prepared_branch_bootstrap);
Datum
pagestore_install_prepared_branch_bootstrap(PG_FUNCTION_ARGS)
{
	char	   *prepared_dir;
	char	   *target_dir;
	int32		new_tl;
	int32		parent_tl;
	XLogRecPtr	checkpoint_redo;
	XLogRecPtr	recovery_lsn;
	XLogRecPtr	fork_lsn;
	char	   *manifest_data;
	char	   *target_manifest;
	char		manifest_path[MAXPGPATH];
	char	   *artifact;
	PagestoreBranchBootstrapHeader *header;
	ControlFileData *control;
	bool		control_crc_ok;
	bool		commit_ts_required;
	Datum		result;

	if (PG_NARGS() != 7)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pagestore_install_prepared_branch_bootstrap requires 7 arguments (prepared_dir, target_dir, new_timeline, parent_timeline, checkpoint_redo, recovery_lsn, fork_lsn)")));
	prepared_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	target_dir = text_to_cstring(PG_GETARG_TEXT_PP(1));
	new_tl = PG_GETARG_INT32(2);
	parent_tl = PG_GETARG_INT32(3);
	checkpoint_redo = PG_GETARG_LSN(4);
	recovery_lsn = PG_GETARG_LSN(5);
	fork_lsn = PG_GETARG_LSN(6);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to install a prepared branch bootstrap")));
	if (new_tl <= 0 || parent_tl < 0 ||
		XLogRecPtrIsInvalid(checkpoint_redo) ||
		recovery_lsn < checkpoint_redo || fork_lsn < recovery_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid portable branch bootstrap identity or LSN bounds")));
	pagestore_require_disjoint_install_dirs(prepared_dir, target_dir);

	/* Preflight the complete legacy bundle before any target map is changed. */
	manifest_data = pagestore_read_branch_manifest(prepared_dir);
	if (manifest_data == NULL ||
		!pagestore_manifest_matches(manifest_data, new_tl, parent_tl, fork_lsn))
		ereport(ERROR,
				(errmsg("prepared branch manifest does not match the requested branch identity")));
	if (!pagestore_manifest_get_commit_ts_required(manifest_data,
											   &commit_ts_required))
		ereport(ERROR,
				(errmsg("prepared branch manifest has invalid commit-ts horizons")));
	pagestore_require_prepared_artifact(prepared_dir, "pg_xact", true);
	if (commit_ts_required)
		pagestore_require_prepared_artifact(prepared_dir, "pg_commit_ts", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact/offsets", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact/members", true);
	pagestore_require_prepared_artifact(prepared_dir,
										"pagestore_branch.manifest", false);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_xact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_commit_ts", commit_ts_required);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_multixact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pagestore_branch.manifest", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  PAGESTORE_BRANCH_BOOTSTRAP_FILE, true);
	target_manifest = pagestore_read_branch_manifest(target_dir);
	if (target_manifest != NULL &&
		!pagestore_manifest_matches(target_manifest, new_tl, parent_tl, fork_lsn))
		ereport(ERROR,
				(errmsg("target branch manifest does not match the requested portable branch identity")));

	artifact = pagestore_load_branch_bootstrap(prepared_dir, new_tl, parent_tl,
											   checkpoint_redo, recovery_lsn,
											   fork_lsn, &header);
	if (!EQ_CRC32C(header->manifest_crc,
					   pagestore_branch_manifest_crc(manifest_data)))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("branch bootstrap artifact does not match the prepared branch manifest")));
	if (header->flags & PAGESTORE_BRANCH_BOOTSTRAP_HAS_USER_TABLESPACES)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("portable branch bootstrap does not yet support user tablespaces"),
				 errdetail("The prepared source has entries in pg_tblspc.")));
	pagestore_preflight_branch_bootstrap_target(target_dir, artifact, header);

	control = get_controlfile(target_dir, &control_crc_ok);
	if (!control_crc_ok ||
		control->system_identifier != header->system_identifier ||
		control->checkPointCopy.redo != checkpoint_redo ||
		control->minRecoveryPoint != checkpoint_redo ||
		control->minRecoveryPointTLI != control->checkPointCopy.ThisTimeLineID ||
		!XLogRecPtrIsInvalid(control->backupStartPoint) ||
		!XLogRecPtrIsInvalid(control->backupEndPoint) ||
		control->backupEndRequired)
		ereport(ERROR,
				(errmsg("target pg_control does not match the prepared archive bootstrap"),
				 errdetail("The target requires system identifier " UINT64_FORMAT
						   " and checkpoint redo %X/%08X restored with --archive-bootstrap.",
						   header->system_identifier,
						   LSN_FORMAT_ARGS(checkpoint_redo))));
	pfree(control);

	/* A previous successful install is no longer evidence of readiness while
	 * maps are being replaced.  Publish the new manifest only after all files. */
	snprintf(manifest_path, sizeof(manifest_path), "%s/pagestore_branch.manifest",
			 target_dir);
	if (unlink(manifest_path) == 0)
		fsync_fname(target_dir, true);
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove stale branch manifest \"%s\": %m",
						manifest_path)));

	pagestore_install_branch_bootstrap_maps(prepared_dir, target_dir,
											artifact, header);
	pfree(artifact);
	result = DirectFunctionCall5(pagestore_install_prepared_branch,
								 PG_GETARG_DATUM(0), PG_GETARG_DATUM(1),
								 PG_GETARG_DATUM(2), PG_GETARG_DATUM(3),
								 PG_GETARG_DATUM(6));
	return result;
}

/* Install an as-of local-state bundle without creating a store timeline. */
PG_FUNCTION_INFO_V1(pagestore_install_prepared_reader);
Datum
pagestore_install_prepared_reader(PG_FUNCTION_ARGS)
{
	char	   *prepared_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int32		timeline = PG_GETARG_INT32(2);
	XLogRecPtr	read_lsn = PG_GETARG_LSN(3);
	char	   *manifest;
	char	   *target_manifest;
	char	   *branch_manifest;
	char		path[MAXPGPATH];
	uint32_t	branch_tl;
	uint32_t	branch_parent;
	uint32		reader_tl = 0;
	uint32		reader_parent = 0;
	uint32		target_reader_tl = 0;
	uint32		target_reader_parent = 0;
	XLogRecPtr	branch_lsn;
	XLogRecPtr	reader_fork_lsn = InvalidXLogRecPtr;
	XLogRecPtr	target_reader_fork_lsn = InvalidXLogRecPtr;
	PagestoreReaderSnapshot *prepared_snapshot;
	ControlFileData *target_control;
	bool		target_control_crc_ok;
	bool		commit_ts_required;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to install a prepared reader")));
	if (timeline < 0 || XLogRecPtrIsInvalid(read_lsn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid reader timeline or read LSN")));
	pagestore_require_disjoint_install_dirs(prepared_dir, target_dir);
	manifest = pagestore_read_reader_manifest(prepared_dir);
	if (manifest == NULL ||
		!pagestore_reader_manifest_matches(manifest, timeline, read_lsn))
		ereport(ERROR,
				(errmsg("prepared reader manifest does not match the requested reader identity")));
	prepared_snapshot = pagestore_load_reader_snapshot(prepared_dir,
		(uint32) timeline, read_lsn, CurrentMemoryContext, ERROR);
	if (timeline > 0 &&
		!pagestore_reader_manifest_get_branch_identity(manifest, &reader_tl,
													 &reader_parent,
													 &reader_fork_lsn))
		ereport(ERROR,
				(errmsg("prepared reader manifest has invalid branch identity")));
	if (!pagestore_manifest_get_commit_ts_required(manifest,
											   &commit_ts_required))
		ereport(ERROR,
				(errmsg("prepared reader manifest has invalid commit-ts horizons")));
	target_manifest = pagestore_read_reader_manifest(target_dir);
	if (target_manifest != NULL &&
		!pagestore_reader_manifest_matches(target_manifest, timeline, read_lsn))
		ereport(ERROR,
				(errmsg("target reader manifest does not match the requested reader identity")));
	if (target_manifest != NULL && timeline > 0 &&
		(!pagestore_reader_manifest_get_branch_identity(target_manifest,
													   &target_reader_tl,
													   &target_reader_parent,
													   &target_reader_fork_lsn) ||
		 target_reader_tl != reader_tl ||
		 target_reader_parent != reader_parent ||
		 target_reader_fork_lsn != reader_fork_lsn))
		ereport(ERROR,
				(errmsg("target reader manifest has a different branch identity")));
	branch_manifest = pagestore_read_branch_manifest(target_dir);
	if (branch_manifest != NULL)
	{
		if (!pagestore_manifest_get_branch_identity(branch_manifest, &branch_tl,
												&branch_parent, &branch_lsn) ||
			branch_tl != (uint32) timeline ||
			(timeline > 0 &&
			 (branch_parent != reader_parent || branch_lsn != reader_fork_lsn)))
			ereport(ERROR,
					(errmsg("target branch manifest does not match the reader branch identity")));
	}
	if (timeline > 0)
		pagestore_localsvc_require_branch(reader_tl, reader_parent,
									  (uint64) reader_fork_lsn);
	target_control = get_controlfile(target_dir, &target_control_crc_ok);
	if (!target_control_crc_ok || target_control->checkPointCopy.redo != read_lsn)
		ereport(ERROR,
				(errmsg("target catalog snapshot pg_control does not match the reader LSN"),
				 errdetail("The reader requires checkpoint redo %X/%08X.",
						   LSN_FORMAT_ARGS(read_lsn))));
	pagestore_load_reader_catalog_provenance(target_dir, (uint32) timeline,
										 read_lsn,
										 target_control->system_identifier,
										 ERROR);
	pfree(target_control);

	pagestore_require_prepared_artifact(prepared_dir, "pg_xact", true);
	if (commit_ts_required)
		pagestore_require_prepared_artifact(prepared_dir, "pg_commit_ts", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact/offsets", true);
	pagestore_require_prepared_artifact(prepared_dir, "pg_multixact/members", true);
	pagestore_require_prepared_artifact(prepared_dir,
									"pagestore_reader.manifest", false);
	pagestore_require_prepared_artifact(prepared_dir,
									PAGESTORE_READER_SNAPSHOT_FILE, false);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_xact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_commit_ts", commit_ts_required);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pg_multixact", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  "pagestore_reader.manifest", true);
	pagestore_preflight_prepared_artifact(prepared_dir, target_dir,
										  PAGESTORE_READER_SNAPSHOT_FILE, true);

	if (MakePGDirectory(target_dir) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create reader dir \"%s\": %m", target_dir)));
	if (snprintf(path, sizeof(path), "%s/pagestore_reader.manifest", target_dir) >=
		(int) sizeof(path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("reader install path is too long")));
	if (access(path, F_OK) == 0 && unlink(path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not clear existing reader manifest \"%s\": %m", path)));
	if (snprintf(path, sizeof(path), "%s/pagestore_branch.manifest", target_dir) >=
		(int) sizeof(path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("reader install path is too long")));
	if (access(path, F_OK) == 0 && unlink(path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove target branch manifest \"%s\": %m", path)));
	fsync_fname(target_dir, true);
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_xact", true);
	if (commit_ts_required)
		pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_commit_ts", true);
	else
		pagestore_install_empty_dir(target_dir, "pg_commit_ts");
	pagestore_install_prepared_dir(prepared_dir, target_dir, "pg_multixact", true);
	pagestore_install_prepared_file(prepared_dir, target_dir,
								   PAGESTORE_READER_SNAPSHOT_FILE, true);
	pagestore_install_prepared_file(prepared_dir, target_dir,
								   "pagestore_reader.manifest", true);
	if (prepared_snapshot->xids != NULL)
		pfree(prepared_snapshot->xids);
	pfree(prepared_snapshot);
	PG_RETURN_VOID();
}

typedef struct PagestoreBranchHorizons
{
	XLogRecPtr	checkpoint_end_lsn;
	uint64		system_identifier;
	TransactionId oldest_xid;
	TransactionId next_xid;
	TransactionId oldest_commit_ts_xid;
	TransactionId next_commit_ts_xid;
	MultiXactId oldest_multi;
	MultiXactId next_multi;
	int64		oldest_member;
	int64		next_member;
} PagestoreBranchHorizons;

/*
 * Derive one complete SLRU bootstrap horizon from an exact, durably mirrored
 * checkpoint.  A merely newest-at-or-below control image is insufficient:
 * requiring both checkPointCopy.redo == target and the exact-redo admission
 * fence prevents an older/still-unsynced control state from being blessed as
 * the branch horizon source.
 *
 * The checkpoint contains every bound except the starting member offset of
 * oldestMulti.  Reconstruct that one offsets entry from the exact base + WAL
 * window which the seeders will consume.  If the oldest multixact does not
 * exist because the range is empty, PostgreSQL defines its offset as the next
 * free member offset.
 */
static void
pagestore_branch_horizons_from_control(XLogRecPtr base, XLogRecPtr target,
									   PagestoreBranchHorizons *h)
{
	ControlFileData control;
	CheckPoint  *checkpoint = &control.checkPointCopy;
	uint64		read_seq;
	MultiXactOffset next_member;

	if (target < base || XLogRecPtrIsInvalid(target))
		ereport(ERROR,
				(errmsg("branch checkpoint redo is invalid or precedes the base cutoff")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR,
				(errmsg("pagestore.backend must be 'localsvc'")));
	if (!ps_control_asof_timeout(target, &control,
								 PAGESTORE_READER_HORIZON_TIMEOUT_MS) ||
		checkpoint->redo != target)
		ereport(ERROR,
				(errmsg("branch checkpoint redo is not exactly mirrored"),
				 errdetail("Requested checkpoint redo is %X/%08X.",
							   LSN_FORMAT_ARGS(target))));
	if (!pagestore_localsvc_read_fence_timeout(
			(uint64) target, &read_seq, PAGESTORE_READER_HORIZON_TIMEOUT_MS))
		ereport(ERROR,
				(errmsg("branch checkpoint redo has no matching durable admission fence"),
				 errdetail("Requested branch target is %X/%08X.",
							   LSN_FORMAT_ARGS(target))));

	/* READ_AT may race the control drain before its final sync. */
	pagestore_localsvc_store_sync_timeout(PAGESTORE_READER_HORIZON_TIMEOUT_MS);

	memset(h, 0, sizeof(*h));
	h->checkpoint_end_lsn = ps_checkpoint_record_end(control.checkPoint,
												 checkpoint);
	h->system_identifier = control.system_identifier;
	h->oldest_xid = checkpoint->oldestXid;
	h->next_xid = XidFromFullTransactionId(checkpoint->nextXid);
	if (!TransactionIdIsNormal(h->oldest_xid) ||
		!TransactionIdIsNormal(h->next_xid) ||
		TransactionIdFollows(h->oldest_xid, h->next_xid))
		ereport(ERROR,
				(errmsg("checkpoint has invalid XID horizons [%u, %u)",
						h->oldest_xid, h->next_xid)));

	if (control.track_commit_timestamp)
	{
		TransactionId oldest = checkpoint->oldestCommitTsXid;
		TransactionId newest = checkpoint->newestCommitTsXid;

		if (!TransactionIdIsNormal(oldest) &&
			!TransactionIdIsNormal(newest))
		{
			/* Active but with no timestamped XID yet: publish an empty era. */
			h->oldest_commit_ts_xid = h->next_xid;
			h->next_commit_ts_xid = h->next_xid;
		}
		else if (!TransactionIdIsNormal(oldest) ||
				 !TransactionIdIsNormal(newest))
			ereport(ERROR,
					(errmsg("checkpoint has inconsistent commit-ts horizons [%u, %u]",
							oldest, newest)));
		else
		{
			h->oldest_commit_ts_xid = oldest;
			h->next_commit_ts_xid = newest;
			TransactionIdAdvance(h->next_commit_ts_xid);
		}
	}
	/* Both InvalidTransactionId when commit-ts is inactive (memset above). */

	h->oldest_multi = checkpoint->oldestMulti;
	h->next_multi = checkpoint->nextMulti;
	if (!MultiXactIdIsValid(h->oldest_multi) ||
		!MultiXactIdIsValid(h->next_multi) ||
		(h->oldest_multi != h->next_multi &&
		 !MultiXactIdPrecedes(h->oldest_multi, h->next_multi)))
		ereport(ERROR,
				(errmsg("checkpoint has invalid multixact horizons [%u, %u)",
						h->oldest_multi, h->next_multi)));
	next_member = checkpoint->nextMultiOffset;
	if (next_member > PG_INT64_MAX)
		ereport(ERROR,
				(errmsg("checkpoint multixact member offset " UINT64_FORMAT
						" exceeds the SQL bigint bootstrap limit",
						(uint64) next_member)));
	h->next_member = (int64) next_member;
	if (h->oldest_multi == h->next_multi)
		h->oldest_member = h->next_member;
	else
	{
		int64		pageno = h->oldest_multi / PS_MXOFF_PER_PAGE;
		int		entryno = h->oldest_multi % PS_MXOFF_PER_PAGE;
		char	   *page = palloc(BLCKSZ);
		MultiXactOffset oldest_member;

		/*
		 * The control file's multixact horizons describe the completed
		 * checkpoint record, not merely its redo pointer.  CreateCheckPoint()
		 * selects those horizons after establishing the redo pointer, so WAL
		 * for oldestMulti can fall between target and the checkpoint record's
		 * end.  Replay through that end before reading the offsets page.
		 */
		if (!ps_mxoff_reconstruct(page, pageno, base, h->checkpoint_end_lsn))
			ereport(ERROR,
					(errmsg("checkpoint oldest multixact %u was truncated at the checkpoint record",
							h->oldest_multi)));
		memcpy(&oldest_member,
			   page + (Size) entryno * sizeof(MultiXactOffset),
			   sizeof(MultiXactOffset));
		pfree(page);
		/* Zero is an uninitialized offset slot, never a created multixact. */
		if (oldest_member == 0 || oldest_member > PG_INT64_MAX)
			ereport(ERROR,
					(errmsg("checkpoint oldest multixact %u has no usable member offset",
							h->oldest_multi)));
		h->oldest_member = (int64) oldest_member;
	}
	if (h->oldest_member > h->next_member)
		ereport(ERROR,
				(errmsg("checkpoint has invalid multixact member horizons [%lld, %lld)",
						(long long) h->oldest_member,
						(long long) h->next_member)));
}

/*
 * Return the exact R/E pair for the local writer's current checkpoint, but
 * only after proving that the same checkpoint and its admission fence are
 * durable in the store.  The serialized branch controller calls this after a
 * clean stop/restart: the shutdown checkpoint is therefore the first control
 * state selected after all public clients have drained.  pg_current_wal_lsn()
 * cannot substitute for E because startup can advance WAL beyond the
 * checkpoint record without changing its control identity.
 */
PG_FUNCTION_INFO_V1(pagestore_branch_checkpoint);
Datum
pagestore_branch_checkpoint(PG_FUNCTION_ARGS)
{
	ControlFileData *local;
	ControlFileData mirrored;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	bool		crc_ok;
	XLogRecPtr	redo;
	XLogRecPtr	end;
	uint64		read_seq;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to select a pagestore branch checkpoint")));
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pagestore branch checkpoint selection requires a writer")));
	if (strcmp(pagestore_backend_name ? pagestore_backend_name : "", "localsvc") != 0)
		ereport(ERROR, (errmsg("pagestore.backend must be 'localsvc'")));

	local = get_controlfile(DataDir, &crc_ok);
	if (!crc_ok)
		ereport(ERROR, (errmsg("local pg_control has an invalid CRC")));
	redo = local->checkPointCopy.redo;
	if (XLogRecPtrIsInvalid(redo) ||
		!ps_control_asof_timeout(redo, &mirrored,
							 PAGESTORE_READER_HORIZON_TIMEOUT_MS) ||
		mirrored.checkPointCopy.redo != redo ||
		mirrored.checkPoint != local->checkPoint ||
		mirrored.system_identifier != local->system_identifier ||
		!ps_checkpoint_matches_control(&mirrored.checkPointCopy,
								   &local->checkPointCopy))
		ereport(ERROR,
				(errmsg("current writer checkpoint is not exactly mirrored"),
				 errdetail("Requested checkpoint redo is %X/%08X.",
						   LSN_FORMAT_ARGS(redo))));
	if (!pagestore_localsvc_read_fence_timeout(
			(uint64) redo, &read_seq, PAGESTORE_READER_HORIZON_TIMEOUT_MS))
		ereport(ERROR,
				(errmsg("current writer checkpoint has no matching durable admission fence"),
				 errdetail("Requested checkpoint redo is %X/%08X.",
						   LSN_FORMAT_ARGS(redo))));
	pagestore_localsvc_store_sync_timeout(PAGESTORE_READER_HORIZON_TIMEOUT_MS);
	end = ps_checkpoint_record_end(mirrored.checkPoint,
								&mirrored.checkPointCopy);
	pfree(local);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = LSNGetDatum(redo);
	values[1] = LSNGetDatum(end);
	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

static int64
pagestore_prepare_branch_impl(const char *target_dir, int32 new_tl,
							  int32 parent_tl, XLogRecPtr base,
							  XLogRecPtr target,
							  TransactionId oldest_xid,
							  TransactionId next_xid,
							  TransactionId oldest_commit_ts_xid,
							  TransactionId next_commit_ts_xid,
							  MultiXactId oldest_multi,
							  MultiXactId next_multi,
							  int64 oldest_member, int64 next_member)
{
	int64		seeded;
	char		manifest_path[MAXPGPATH];
	int			pathlen;

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
	if ((uint32) parent_tl != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errmsg("pagestore parent timeline %d is not the active localsvc timeline %u",
						parent_tl, pagestore_localsvc_timeline())));
	if (target < base)
		ereport(ERROR,
				(errmsg("target LSN precedes the base cutoff")));

	pagestore_localsvc_check_branch((uint32) new_tl, (uint32) parent_tl,
									(uint64) target);

	/*
	 * Normalize the commit-ts horizons BEFORE the idempotency check and
	 * the manifest unlink: derivation must yield the same values a
	 * previous successful prepare wrote into its manifest, or a retry
	 * would never match, delete a valid prepared manifest, and leave the
	 * branch inert if the reseed then fails.
	 */
	ps_commit_ts_normalize_horizons(target, next_xid,
									&oldest_commit_ts_xid,
									&next_commit_ts_xid);

	if (pagestore_existing_branch_manifest_matches(target_dir, new_tl, parent_tl,
												   base, target,
												   oldest_xid, next_xid,
												   oldest_commit_ts_xid,
												   next_commit_ts_xid,
												   oldest_multi, next_multi,
												   oldest_member, next_member,
												   &seeded))
	{
		pagestore_localsvc_create_branch((uint32) new_tl, (uint32) parent_tl,
										 (uint64) target);
		return seeded;
	}

	/*
	 * From here until the new manifest is published the target's SLRUs may
	 * not match any manifest, so durably invalidate a manifest left by a
	 * previous prepare before touching them.  The manifest is what marks a
	 * prepared dir as consumable, so every failure below -- SLRU seeding,
	 * CREATE_BRANCH being refused after the store raced ahead, the manifest
	 * write itself -- leaves the dir inert instead of advertising stale or
	 * half-updated contents.
	 */
	pathlen = snprintf(manifest_path, sizeof(manifest_path),
					   "%s/pagestore_branch.manifest", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, manifest_path);
	if (unlink(manifest_path) == 0)
		fsync_fname(target_dir, true);
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove stale branch manifest \"%s\": %m",
						manifest_path)));

	seeded = pagestore_seed_branch_slrus_impl(target_dir, base, target,
											  oldest_xid, next_xid,
											  oldest_commit_ts_xid,
											  next_commit_ts_xid,
											  oldest_multi, next_multi,
											  oldest_member, next_member);
	pagestore_localsvc_create_branch((uint32) new_tl, (uint32) parent_tl,
									 (uint64) target);

	/*
	 * Publish the manifest only after the store-side timeline exists: the
	 * manifest is the durable handoff artifact for later bootstrap, so it must
	 * never advertise a branch that CREATE_BRANCH refused (e.g. the same
	 * timeline raced into existence with different ancestry during seeding).
	 * The reverse window is retry-safe: if the manifest write fails here, the
	 * timeline already exists and a retried prepare passes CHECK_BRANCH and
	 * re-runs CREATE_BRANCH idempotently.
	 */
	pagestore_write_branch_manifest(target_dir, new_tl, parent_tl,
									base, target,
									oldest_xid, next_xid,
									oldest_commit_ts_xid,
									next_commit_ts_xid,
									oldest_multi, next_multi,
									oldest_member, next_member,
									seeded);

	return seeded;
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
 * Legacy expert entrypoint.  Keep its ABI for existing operators and tests;
 * new control-plane callers should use pagestore_prepare_branch_from_control.
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
	int64		seeded;
	char		bootstrap_path[MAXPGPATH];
	int			pathlen;

	seeded = pagestore_prepare_branch_impl(target_dir, new_tl, parent_tl,
											base, target,
											PG_GETARG_TRANSACTIONID(5),
											PG_GETARG_TRANSACTIONID(6),
											PG_GETARG_TRANSACTIONID(7),
											PG_GETARG_TRANSACTIONID(8),
											PG_GETARG_TRANSACTIONID(9),
											PG_GETARG_TRANSACTIONID(10),
											PG_GETARG_INT64(11),
											PG_GETARG_INT64(12));

	/* The expert ABI does not produce the catalog/control-bound artifact. */
	pathlen = snprintf(bootstrap_path, sizeof(bootstrap_path), "%s/%s",
					   target_dir, PAGESTORE_BRANCH_BOOTSTRAP_FILE);
	PS_CHECK_PATH_FORMAT(pathlen, bootstrap_path);
	if (unlink(bootstrap_path) == 0)
		fsync_fname(target_dir, true);
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove stale branch bootstrap artifact \"%s\": %m",
						bootstrap_path)));
	PG_RETURN_INT64(seeded);
}

/*
 * pagestore_prepare_branch_from_control(target_dir text, new_timeline int,
 *                                       parent_timeline int, base pg_lsn,
 *                                       checkpoint_redo pg_lsn,
 *                                       fork_lsn pg_lsn) returns bigint
 *
 * Control-derived entrypoint: the caller supplies the proven exact base
 * snapshot cutoff C, a durably mirrored checkpoint redo R, and a materialized
 * fork boundary L which covers that completed checkpoint.  XID,
 * commit-ts, multixact-ID and member-offset horizons all come from that same
 * checkpoint/control state at R; the store branch is cut at L, where no
 * admission-sequence tie remains.  Retrying the operation is idempotent
 * through the same prepared-manifest and CREATE_BRANCH checks as the legacy
 * entrypoint.  The control plane must keep the parent quiescent between the
 * selected checkpoint and L; automatically establishing that quiesce together
 * with the proven base snapshot is the remaining producer-side protocol.
 */
PG_FUNCTION_INFO_V1(pagestore_prepare_branch_from_control);
Datum
pagestore_prepare_branch_from_control(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		new_tl = PG_GETARG_INT32(1);
	int32		parent_tl = PG_GETARG_INT32(2);
	XLogRecPtr	base = PG_GETARG_LSN(3);
	XLogRecPtr	checkpoint_redo = PG_GETARG_LSN(4);
	XLogRecPtr	fork_lsn = PG_GETARG_LSN(5);
	XLogRecPtr	materialized;
	PagestoreBranchHorizons h;
	int64		seeded;
	char		bootstrap_path[MAXPGPATH];
	PagestoreBranchBootstrapHeader *existing_header;
	char	   *existing;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to prepare a branch")));
	if (new_tl <= 0 || parent_tl < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid branch timeline identity")));
	if ((uint32) parent_tl != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errmsg("pagestore parent timeline %d is not the active localsvc timeline %u",
						parent_tl, pagestore_localsvc_timeline())));
	pagestore_branch_horizons_from_control(base, checkpoint_redo, &h);
	if (fork_lsn < h.checkpoint_end_lsn)
		ereport(ERROR,
				(errmsg("branch fork LSN does not cover the selected checkpoint"),
				 errdetail("Fork %X/%08X precedes checkpoint record end %X/%08X.",
							   LSN_FORMAT_ARGS(fork_lsn),
							   LSN_FORMAT_ARGS(h.checkpoint_end_lsn))));
	/*
	 * The declared materializer can only fork at a restartpoint marker: that is
	 * the point through which its replayed relation pages are durable.  Other
	 * direct-write computes synchronously persist each routed page, including
	 * pages whose WAL record is still in the current unarchived segment.  They
	 * therefore need no materializer-watermark bound.
	 */
	if (pagestore_materializer)
	{
		materialized = pagestore_materialized_wal_lsn_internal();
		if (fork_lsn > materialized)
			ereport(ERROR,
					(errmsg("branch fork LSN exceeds the durable materialized horizon"),
					 errdetail("Fork %X/%08X exceeds materialized horizon %X/%08X.",
							   LSN_FORMAT_ARGS(fork_lsn),
							   LSN_FORMAT_ARGS(materialized))));
	}

	seeded = pagestore_prepare_branch_impl(target_dir, new_tl, parent_tl,
											base, fork_lsn,
											h.oldest_xid, h.next_xid,
											h.oldest_commit_ts_xid,
											h.next_commit_ts_xid,
											h.oldest_multi, h.next_multi,
											h.oldest_member, h.next_member);
	/*
	 * The portable artifact is its own readiness marker in the prepared dir.
	 * Publish it only after the SLRU manifest exists, and bind its checksum to
	 * that exact manifest.  The target installer still publishes the ordinary
	 * branch manifest last.
	 */
	if (snprintf(bootstrap_path, sizeof(bootstrap_path), "%s/%s", target_dir,
				 PAGESTORE_BRANCH_BOOTSTRAP_FILE) >= (int) sizeof(bootstrap_path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("branch bootstrap path is too long")));
	if (access(bootstrap_path, F_OK) == 0)
	{
		/* A retry must retain the relation maps captured at the original fork;
		 * recapturing live parent maps can name filenodes absent on the child. */
		existing = pagestore_load_branch_bootstrap(target_dir, new_tl,
			parent_tl, checkpoint_redo, h.checkpoint_end_lsn, fork_lsn,
			&existing_header);

		pfree(existing);
	}
	else if (errno == ENOENT)
		pagestore_write_branch_bootstrap(target_dir, new_tl, parent_tl,
									checkpoint_redo,
									h.checkpoint_end_lsn, fork_lsn,
									h.system_identifier);
	else
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not inspect branch bootstrap artifact \"%s\": %m",
						bootstrap_path)));
	PG_RETURN_INT64(seeded);
}

/*
 * Materialize local SLRUs for a pinned compute at a checkpoint redo without
 * forking the store timeline.  pg_control is restored separately, before
 * server startup, by pagestore_control_restore -- shared_preload_libraries is
 * too late to replace it safely.
 */
PG_FUNCTION_INFO_V1(pagestore_prepare_reader);
Datum
pagestore_prepare_reader(PG_FUNCTION_ARGS)
{
	char	   *target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		timeline = PG_GETARG_INT32(1);
	XLogRecPtr	base = PG_GETARG_LSN(2);
	XLogRecPtr	read_lsn = PG_GETARG_LSN(3);
	TransactionId oldest_xid = PG_GETARG_TRANSACTIONID(4);
	TransactionId next_xid = PG_GETARG_TRANSACTIONID(5);
	TransactionId oldest_commit_ts_xid = PG_GETARG_TRANSACTIONID(6);
	TransactionId next_commit_ts_xid = PG_GETARG_TRANSACTIONID(7);
	MultiXactId oldest_multi = PG_GETARG_TRANSACTIONID(8);
	MultiXactId next_multi = PG_GETARG_TRANSACTIONID(9);
	int64		oldest_member = PG_GETARG_INT64(10);
	int64		next_member = PG_GETARG_INT64(11);
	ControlFileData control;
	char	   *branch_manifest = NULL;
	char		manifest_path[MAXPGPATH];
	uint32		branch_timeline = 0;
	uint32		parent_timeline = 0;
	XLogRecPtr	fork_lsn = InvalidXLogRecPtr;
	int			pathlen;
	int64		seeded;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to prepare a reader")));
	if (timeline < 0 || (uint32) timeline != pagestore_localsvc_timeline())
		ereport(ERROR,
				(errmsg("reader timeline %d is not the active localsvc timeline %u",
						timeline, pagestore_localsvc_timeline())));
	if (read_lsn < base || XLogRecPtrIsInvalid(read_lsn))
		ereport(ERROR,
				(errmsg("reader LSN is invalid or precedes the base cutoff")));
	if (timeline > 0)
	{
		branch_manifest = pagestore_read_branch_manifest(DataDir);
		if (branch_manifest == NULL ||
			!pagestore_manifest_get_branch_identity(branch_manifest,
													   &branch_timeline,
													   &parent_timeline,
													   &fork_lsn) ||
			branch_timeline != (uint32) timeline)
			ereport(ERROR,
					(errmsg("active branch manifest does not match the reader timeline")));
		pagestore_localsvc_require_branch(branch_timeline, parent_timeline,
										  (uint64) fork_lsn);
	}
	if (!ps_control_asof(read_lsn, &control) ||
		control.checkPointCopy.redo != read_lsn)
		ereport(ERROR,
				(errmsg("reader LSN is not a durably mirrored checkpoint redo"),
					 errdetail("Requested reader LSN is %X/%08X.",
							LSN_FORMAT_ARGS(read_lsn))));
	if (oldest_xid != control.checkPointCopy.oldestXid ||
		next_xid != XidFromFullTransactionId(control.checkPointCopy.nextXid))
		ereport(ERROR,
				(errmsg("reader XID horizons do not match the checkpoint at the requested reader LSN"),
				 errdetail("Expected [%u, %u), got [%u, %u).",
						control.checkPointCopy.oldestXid,
						XidFromFullTransactionId(control.checkPointCopy.nextXid),
						oldest_xid, next_xid)));

	ps_commit_ts_normalize_horizons(read_lsn, next_xid,
								&oldest_commit_ts_xid,
								&next_commit_ts_xid);
	pathlen = snprintf(manifest_path, sizeof(manifest_path),
					   "%s/pagestore_reader.manifest", target_dir);
	PS_CHECK_PATH_FORMAT(pathlen, manifest_path);
	if (unlink(manifest_path) == 0)
		fsync_fname(target_dir, true);
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
					 errmsg("could not remove stale reader manifest \"%s\": %m",
							manifest_path)));
	pathlen = snprintf(manifest_path, sizeof(manifest_path), "%s/%s",
					   target_dir, PAGESTORE_READER_SNAPSHOT_FILE);
	PS_CHECK_PATH_FORMAT(pathlen, manifest_path);
	if (unlink(manifest_path) == 0)
		fsync_fname(target_dir, true);
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove stale reader snapshot \"%s\": %m",
							manifest_path)));
	seeded = pagestore_seed_branch_slrus_impl(target_dir, base, read_lsn,
										  oldest_xid, next_xid,
										  oldest_commit_ts_xid,
										  next_commit_ts_xid,
										  oldest_multi, next_multi,
										  oldest_member, next_member);
	pagestore_write_reader_snapshot(target_dir, (uint32) timeline, read_lsn,
								oldest_xid, next_xid);
	pagestore_write_reader_manifest(target_dir, timeline, base, read_lsn,
								parent_timeline, fork_lsn,
								oldest_xid, next_xid,
								oldest_commit_ts_xid,
								next_commit_ts_xid,
								oldest_multi, next_multi,
								oldest_member, next_member, seeded);
	PG_RETURN_INT64(seeded);
}

static bool
pagestore_reader_artifact_worker_cycle(bool prime)
{
	LOCAL_FCINFO(fcinfo, 0);
	bool		succeeded = false;

	InitFunctionCallInfoData(*fcinfo, NULL, 0, InvalidOid, NULL, NULL);
	PG_TRY();
	{
		StartTransactionCommand();
		if (prime)
			(void) pagestore_prime_reader_relmaps(fcinfo);
		fcinfo->isnull = false;
		(void) pagestore_publish_database_reader_manifest(fcinfo);
		CommitTransactionCommand();
		succeeded = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		edata = CopyErrorData();
		MemoryContextSwitchTo(oldcontext);
		if (edata->elevel >= FATAL ||
			edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		{
			FreeErrorData(edata);
			PG_RE_THROW();
		}
		FlushErrorState();
		AbortCurrentTransaction();
		MemoryContextSwitchTo(TopMemoryContext);
		ereport(WARNING,
				(errmsg("pagestore reader artifact worker cycle failed: %s",
						edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
	return succeeded;
}

void
pagestore_reader_artifact_database_main(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);
	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
	if (!ShutdownRequestPending)
		(void) pagestore_reader_artifact_worker_cycle(true);
	proc_exit(0);
}

static List *
pagestore_reader_artifact_databases(void)
{
	List	   *databases = NIL;
	MemoryContext oldcontext;

	StartTransactionCommand();
	/* CREATE/DROP DATABASE take a conflicting lock on pg_database.  Keep this
	 * lock and transaction through the checkpoint, per-database publication,
	 * and barrier write so the recorded set is exactly the set at R. */
	LockRelationOid(DatabaseRelationId, ShareLock);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_execute("SELECT oid, dattablespace FROM pg_database "
					"WHERE datallowconn ORDER BY oid",
					true, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not enumerate databases");
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	for (uint64 i = 0; i < SPI_processed; i++)
	{
		bool		isnull;
		PagestoreReaderDatabaseEntry *entry = palloc(sizeof(*entry));
		Oid			dboid = DatumGetObjectId(SPI_getbinval(
			SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));

		if (isnull)
			continue;
		entry->database_oid = dboid;
		entry->tablespace_oid = DatumGetObjectId(SPI_getbinval(
			SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
		if (!isnull)
			databases = lappend(databases, entry);
	}
	MemoryContextSwitchTo(oldcontext);
	PopActiveSnapshot();
	SPI_finish();
	return databases;
}

static void
pagestore_run_reader_artifact_worker(Oid dboid)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_INTERRUPTIBLE;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pagestore");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "pagestore_reader_artifact_database_main");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "pagestore reader artifacts %u", dboid);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pagestore reader artifacts");
	worker.bgw_main_arg = ObjectIdGetDatum(dboid);
	worker.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(WARNING,
				(errmsg("could not start reader artifact worker for database %u",
						dboid)));
		return;
	}
	(void) WaitForBackgroundWorkerShutdown(handle);
	pfree(handle);
}

void
pagestore_reader_artifact_launcher_main(Datum main_arg)
{

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(Template1DbOid, InvalidOid,
										  BGWORKER_BYPASS_ALLOWCONN);

	while (!ShutdownRequestPending)
	{
		List	   *databases = NIL;
		XLogRecPtr	barrier_lsn = InvalidXLogRecPtr;
		bool		barrier_complete = false;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		PG_TRY();
		{
			databases = pagestore_reader_artifact_databases();
			barrier_lsn = pagestore_latest_reader_snapshot_ready();
			/* LockRelationOid() above prevents membership changes now; page LSNs
			 * prove the enumerated catalog has not changed since this exact R. */
			barrier_complete = !XLogRecPtrIsInvalid(barrier_lsn) &&
				databases != NIL;
			for (int pass = 0; barrier_complete && pass < 3; pass++)
			{
				ListCell *lc;
				XLogRecPtr pass_lsn = barrier_lsn;

				foreach(lc, databases)
				{
					PagestoreReaderDatabaseEntry *entry = lfirst(lc);

					if (ShutdownRequestPending)
					{
						barrier_complete = false;
						break;
					}
					pagestore_run_reader_artifact_worker(entry->database_oid);
					CHECK_FOR_INTERRUPTS();
				}
				barrier_lsn = pagestore_latest_reader_snapshot_ready();
				if (barrier_lsn != pass_lsn)
				{
					if (pass == 2)
						barrier_complete = false;
					continue;
				}
				foreach(lc, databases)
				{
					PagestoreReaderDatabaseEntry *entry = lfirst(lc);

					if (!pagestore_database_reader_manifest_ready(
							entry->database_oid, barrier_lsn))
						barrier_complete = false;
				}
				break;
			}
			if (barrier_complete && !ShutdownRequestPending)
				pagestore_publish_reader_database_barrier(databases, barrier_lsn);
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

			edata = CopyErrorData();
			MemoryContextSwitchTo(oldcontext);
			if (edata->elevel >= FATAL ||
				edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
			{
				FreeErrorData(edata);
				PG_RE_THROW();
			}
			FlushErrorState();
			AbortCurrentTransaction();
			MemoryContextSwitchTo(TopMemoryContext);
			ereport(WARNING,
					(errmsg("pagestore reader artifact launcher cycle failed: %s",
							edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();
		list_free_deep(databases);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	proc_exit(0);
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

	DefineCustomBoolVariable("pagestore.advance_read_lsn",
							 "Discover newer durable read horizons for a pinned reader.",
							 "New horizons are published as candidates at snapshot boundaries; "
							 "they are not adopted until their exact running-XID snapshot is available.",
							 &pagestore_advance_read_lsn,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("pagestore.auto_reader_artifacts",
							 "Automatically publish per-database reader artifacts.",
							 "Required on the writer for advancing readers; a launcher maintains one artifact worker for every connectable database.",
							 &pagestore_auto_reader_artifacts,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("pagestore.auto_wal_index",
							 "Continuously index WAL after it is shipped to pagestore.",
							 "The worker advances only across a record-aligned, durable shipped-WAL prefix.",
							 &pagestore_auto_wal_index,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("pagestore.materializer",
							 "Declare this recovery instance as the pagestore materializer.",
							 "The role publishes durable restartpoint progress and enables "
							 "materializer supervision. It requires localsvc, full relation "
							 "routing, and an unpinned read horizon.",
							 &pagestore_materializer,
							 false,
							 PGC_POSTMASTER,
							 0,
								 NULL, NULL, NULL);
	DefineCustomStringVariable("pagestore.retention_owner_id",
							   "Controller-assigned stable retention owner ID.",
							   "Required for a managed materializer or reader; it remains stable across replacement processes.",
							   &pagestore_retention_owner_id_str,
							   "",
							   PGC_POSTMASTER,
							   0,
							   check_retention_owner_id,
							   assign_retention_owner_id,
							   NULL);
	DefineCustomStringVariable("pagestore.retention_owner_generation",
							   "Controller-assigned retention owner takeover generation.",
							   "Required for a managed materializer or reader and incremented before each replacement starts.",
							   &pagestore_retention_owner_generation_str,
							   "",
							   PGC_POSTMASTER,
							   0,
							   check_retention_owner_generation,
							   assign_retention_owner_generation,
							   NULL);
	DefineCustomIntVariable("pagestore.materializer_max_lag_mb",
							"Pause WAL archiving when durable materialization falls too far behind.",
							"Zero disables the limit. The boundary stays segment-aligned and "
							"latches one completed checkpoint so recovery cannot strand.",
							&pagestore_materializer_max_lag_mb,
							0, 0, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("pagestore.wal_index_max_lag_mb",
							"Pause WAL archiving when durable WAL indexing falls too far behind.",
							"Zero disables the limit. Headroom for one maximum-size WAL record "
							"and segment padding is always reserved.",
							&pagestore_wal_index_max_lag_mb,
							0, 0, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	if (pagestore_advance_read_lsn && pagestore_localsvc_read_lsn() == 0)
		ereport(ERROR,
				(errmsg("pagestore.advance_read_lsn requires pagestore.read_lsn")));
	if (pagestore_auto_reader_artifacts &&
		!pagestore_branch_routing_active())
		ereport(ERROR,
				(errmsg("pagestore.auto_reader_artifacts requires full pagestore relation routing")));
	if (pagestore_materializer &&
		(pagestore_backend_name == NULL ||
		 strcmp(pagestore_backend_name, "localsvc") != 0))
		ereport(ERROR,
				(errmsg("pagestore.materializer requires pagestore.backend = 'localsvc'")));
	if (pagestore_materializer && !pagestore_route_all)
		ereport(ERROR,
				(errmsg("pagestore.materializer requires pagestore.route_all = on")));
	if (pagestore_materializer && pagestore_localsvc_read_lsn() != 0)
		ereport(ERROR,
				(errmsg("pagestore.materializer cannot use pagestore.read_lsn")));
	if (pagestore_materializer &&
		(pagestore_retention_owner_id == 0 ||
		 pagestore_retention_owner_generation == 0))
		ereport(ERROR,
				(errmsg("pagestore.materializer requires retention owner authority"),
				 errhint("Set pagestore.retention_owner_id and pagestore.retention_owner_generation from durable controller state.")));
	if (pagestore_localsvc_read_lsn() != 0 &&
		(pagestore_retention_owner_id == 0 ||
		 pagestore_retention_owner_generation == 0))
		ereport(ERROR,
				(errmsg("pagestore.read_lsn requires retention owner authority"),
				 errhint("Set pagestore.retention_owner_id and pagestore.retention_owner_generation from durable controller state.")));
	prev_planner_hook = planner_hook;
	planner_hook = pagestore_planner;
	prev_executor_run_hook = ExecutorRun_hook;
	ExecutorRun_hook = pagestore_executor_run;
	prev_process_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pagestore_process_utility;
	prev_commit_ts_bounds_hook = commit_ts_bounds_hook;
	commit_ts_bounds_hook = pagestore_commit_ts_bounds;
	prev_commit_ts_latest_hook = commit_ts_latest_hook;
	commit_ts_latest_hook = pagestore_commit_ts_latest;
	prev_snapshot_transfer_hook = snapshot_transfer_hook;
	snapshot_transfer_hook = pagestore_snapshot_transfer;
	prev_post_database_path_hook = post_database_path_hook;
	post_database_path_hook = pagestore_validate_database_reader_view;

	/*
	 * Pinned-reader (pagestore.read_lsn) instance-wide side effects: needs
	 * the final backend name, so it cannot run inside
	 * pagestore_localsvc_init() above.
	 */
	pagestore_localsvc_pinned_init(pagestore_backend_name != NULL &&
								   strcmp(pagestore_backend_name, "localsvc") == 0);

	/*
	 * Live SLRU page mirror (write-side capture).  Defines its GUC, so it
	 * must run before the prefix is reserved; the backend-name GUC it
	 * checks is final here.
	 */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pagestore_shmem_request;
	pagestore_slru_mirror_init(pagestore_backend_name != NULL &&
							   strcmp(pagestore_backend_name, "localsvc") == 0);

	MarkGUCPrefixReserved("pagestore");

	/* register our smgr implementation and claim relations via the hook */
	pagestore_smgr_which = smgr_register(&pagestore_smgr);
	smgr_which_hook = pagestore_which;
	prev_buffer_tag_read_epoch_hook = buffer_tag_read_epoch_hook;
	buffer_tag_read_epoch_hook = pagestore_buffer_tag_read_epoch;
	prev_get_snapshot_data_hook = get_snapshot_data_hook;
	get_snapshot_data_hook = pagestore_get_snapshot_data;
	prev_xid_in_progress_hook = transaction_id_is_in_progress_hook;
	transaction_id_is_in_progress_hook =
		pagestore_transaction_id_is_in_progress;
	prev_xact_start_hook = xact_start_hook;
	xact_start_hook = pagestore_adopt_reader_view_at_xact_start;
	if (pagestore_advance_read_lsn)
		RegisterXactCallback(pagestore_reader_advance_xact_end, NULL);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pagestore_validate_datadir_branch_manifest;

	/*
	 * Mirror pg_control to the store whenever the localsvc backend is
	 * active: the GUCs above have already absorbed postgresql.conf, so
	 * pagestore_backend_name is final here.
	 */
	pagestore_control_mirror_init(pagestore_backend_name != NULL &&
								  strcmp(pagestore_backend_name, "localsvc") == 0);
	if (pagestore_materializer && pagestore_backend_name != NULL &&
		strcmp(pagestore_backend_name, "localsvc") == 0 &&
		pagestore_route_all && pagestore_localsvc_read_lsn() == 0)
	{
		prev_recovery_start_hook = recovery_start_hook;
		recovery_start_hook = pagestore_materializer_recovery_start;
		prev_restartpoint_flush_hook = recovery_restartpoint_flush_hook;
		recovery_restartpoint_flush_hook =
			pagestore_materializer_restartpoint_flush;
	}
	if (pagestore_backend_name != NULL &&
		strcmp(pagestore_backend_name, "localsvc") == 0 &&
		pagestore_localsvc_read_lsn() == 0)
	{
		BackgroundWorker worker;

		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 10;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "pagestore");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				 "pagestore_reader_snapshot_worker_main");
		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "pagestore reader snapshot worker");
		snprintf(worker.bgw_type, BGW_MAXLEN,
				 "pagestore reader snapshot worker");
		RegisterBackgroundWorker(&worker);
	}
	if (pagestore_auto_reader_artifacts &&
		pagestore_backend_name != NULL &&
		strcmp(pagestore_backend_name, "localsvc") == 0 &&
		pagestore_localsvc_read_lsn() == 0)
	{
		BackgroundWorker worker;

		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_INTERRUPTIBLE;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 10;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "pagestore");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				 "pagestore_reader_artifact_launcher_main");
		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "pagestore reader artifact launcher");
		snprintf(worker.bgw_type, BGW_MAXLEN,
				 "pagestore reader artifact launcher");
		RegisterBackgroundWorker(&worker);
	}
	if (pagestore_auto_wal_index &&
		pagestore_backend_name != NULL &&
		strcmp(pagestore_backend_name, "localsvc") == 0 &&
		pagestore_localsvc_read_lsn() == 0)
	{
		BackgroundWorker worker;

		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 10;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "pagestore");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				 "pagestore_wal_index_worker_main");
		snprintf(worker.bgw_name, BGW_MAXLEN, "pagestore WAL index worker");
		snprintf(worker.bgw_type, BGW_MAXLEN, "pagestore WAL index worker");
		RegisterBackgroundWorker(&worker);
	}
}
