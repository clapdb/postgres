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

/*
 * Make the fork exist (create its underlying file).  isRedo is set when called
 * during WAL replay, where an already-existing file is tolerated rather than
 * treated as an error.
 */
static void
passthrough_create(const PageStoreRelKey *key, void *localreln, bool isRedo)
{
	mdcreate((SMgrRelation) localreln, (ForkNumber) key->forkNum, isRedo);
}

/* Report whether the fork's file exists on disk. */
static bool
passthrough_fork_exists(const PageStoreRelKey *key, void *localreln)
{
	return mdexists((SMgrRelation) localreln, (ForkNumber) key->forkNum);
}

/* Delete the fork's file(s). */
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

/* Current number of blocks in the fork. */
static BlockNumber
passthrough_nblocks(const PageStoreRelKey *key, void *localreln)
{
	return mdnblocks((SMgrRelation) localreln, (ForkNumber) key->forkNum);
}

/*
 * Shrink the fork from old_blocks down to nblocks blocks (e.g. when VACUUM
 * trims trailing empty pages).  old_blocks is the size before truncation,
 * which md uses to know which segment files to drop.
 */
static void
passthrough_truncate(const PageStoreRelKey *key, void *localreln,
					 BlockNumber old_blocks, BlockNumber nblocks)
{
	mdtruncate((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			   old_blocks, nblocks);
}

/* Read nblocks pages from blocknum into buffers[]. */
static void
passthrough_readv(const PageStoreRelKey *key, void *localreln,
				  BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	mdreadv((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			blocknum, buffers, nblocks);
}

/*
 * Overwrite nblocks already-existing blocks starting at blocknum from
 * buffers[] (the dirty-page flush path).  Unlike extend(), this does not grow
 * the fork -- the blocks must already exist.  skipFsync defers the fsync to the
 * next checkpoint (md registers a sync request instead of syncing now).
 */
static void
passthrough_writev(const PageStoreRelKey *key, void *localreln,
				   BlockNumber blocknum, const void **buffers,
				   BlockNumber nblocks, bool skipFsync)
{
	mdwritev((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			 blocknum, buffers, nblocks, skipFsync);
}

/*
 * Grow the fork by exactly one block at blocknum, written from buffer -- the
 * single-page grow path.  Contrast: zeroextend() bulk-adds many empty blocks
 * with no content, and writev() overwrites blocks that already exist rather
 * than growing the fork.
 */
static void
passthrough_extend(const PageStoreRelKey *key, void *localreln,
				   BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	mdextend((SMgrRelation) localreln, (ForkNumber) key->forkNum,
			 blocknum, buffer, skipFsync);
}

/*
 * Bulk-extend the fork by nblocks zero-filled blocks starting at blocknum,
 * without supplying any page contents.
 *
 * This is the batch counterpart to extend(): extend() adds exactly one block
 * written from a caller-provided buffer, whereas zeroextend() pre-allocates
 * many empty blocks in a single call.  PostgreSQL uses it to grow a relation
 * by several pages at once (e.g. under concurrent insertion) far more cheaply
 * than issuing one extend() per page; md.c implements it with
 * posix_fallocate()/ftruncate().  The new blocks read back as zeros until they
 * are actually written.
 */
static void
passthrough_zeroextend(const PageStoreRelKey *key, void *localreln,
					   BlockNumber blocknum, int nblocks, bool skipFsync)
{
	mdzeroextend((SMgrRelation) localreln, (ForkNumber) key->forkNum,
				 blocknum, nblocks, skipFsync);
}

/*
 * Force the fork's data durable immediately (fsync now), as opposed to the
 * usual path where writev() defers the fsync to the next checkpoint.  Used when
 * a relation must be on disk before proceeding (e.g. after an unlogged build).
 */
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
