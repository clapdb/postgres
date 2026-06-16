/*-------------------------------------------------------------------------
 *
 * backend_passthrough.c
 *	  Passthrough storage backend: forwards every operation to the built-in
 *	  magnetic disk manager (md.c).
 *
 * This is the reference backend used to validate the pagestore boundary: it
 * exercises the full version-neutral interface while producing behaviour
 * byte-for-byte identical to stock PostgreSQL, so the regression suite must
 * stay green with it routing all relation I/O.
 *
 * It is the one backend allowed to use the opaque "localreln" cookie (the
 * originating SMgrRelation), because md operates on SMgrRelation-private state
 * (cached segment fds).  Remote/SPDK backends rely on the key instead.
 *
 * src/../contrib/pagestore/backend_passthrough.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pagestore_backend.h"
#include "storage/md.h"
#include "storage/smgr.h"

static void
passthrough_create(const PageStoreRelKey *key, void *localreln, bool isRedo)
{
	mdcreate((SMgrRelation) localreln, (ForkNumber) key->forkNum, isRedo);
}

static bool
passthrough_fork_exists(const PageStoreRelKey *key, void *localreln)
{
	return mdexists((SMgrRelation) localreln, (ForkNumber) key->forkNum);
}

static void
passthrough_unlink(const PageStoreRelKey *key, bool isRedo)
{
	RelFileLocatorBackend rlocator;

	rlocator.locator.spcOid = key->spcOid;
	rlocator.locator.dbOid = key->dbOid;
	rlocator.locator.relNumber = key->relNumber;
	rlocator.backend = INVALID_PROC_NUMBER;

	mdunlink(rlocator, (ForkNumber) key->forkNum, isRedo);
}

static BlockNumber
passthrough_nblocks(const PageStoreRelKey *key, void *localreln)
{
	return mdnblocks((SMgrRelation) localreln, (ForkNumber) key->forkNum);
}

static void
passthrough_truncate(const PageStoreRelKey *key, void *localreln,
					 BlockNumber old_blocks, BlockNumber nblocks)
{
	mdtruncate((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			   old_blocks, nblocks);
}

static void
passthrough_readv(const PageStoreRelKey *key, void *localreln,
				  BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	mdreadv((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			blocknum, buffers, nblocks);
}

static void
passthrough_writev(const PageStoreRelKey *key, void *localreln,
				   BlockNumber blocknum, const void **buffers,
				   BlockNumber nblocks, bool skipFsync)
{
	mdwritev((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			 blocknum, buffers, nblocks, skipFsync);
}

static void
passthrough_extend(const PageStoreRelKey *key, void *localreln,
				   BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	mdextend((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			 blocknum, buffer, skipFsync);
}

static void
passthrough_zeroextend(const PageStoreRelKey *key, void *localreln,
					   BlockNumber blocknum, int nblocks, bool skipFsync)
{
	mdzeroextend((SMgrRelation) localreln, (ForkNumber) key->forkNum,
				 blocknum, nblocks, skipFsync);
}

static void
passthrough_immedsync(const PageStoreRelKey *key, void *localreln)
{
	mdimmedsync((SMgrRelation) localreln, (ForkNumber) key->forkNum);
}

const PageStoreBackend PageStoreBackendPassthrough = {
	.name = "passthrough",
	.uses_local_files = true,
	.init = NULL,
	.create = passthrough_create,
	.fork_exists = passthrough_fork_exists,
	.unlink = passthrough_unlink,
	.nblocks = passthrough_nblocks,
	.truncate = passthrough_truncate,
	.readv = passthrough_readv,
	.writev = passthrough_writev,
	.extend = passthrough_extend,
	.zeroextend = passthrough_zeroextend,
	.immedsync = passthrough_immedsync,
};
