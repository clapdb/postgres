/*-------------------------------------------------------------------------
 *
 * pagestore_backend.h
 *	  Version-neutral storage backend interface ("the lib boundary").
 *
 * This header is the encapsulation boundary between the per-version smgr shim
 * (pagestore.c) and the actual storage implementation.  The shim translates
 * PostgreSQL's version-specific smgr calls into the version-neutral operations
 * declared here; a backend implements them by talking to whatever actually
 * stores the data -- a distributed page service, a local SPDK device, a local
 * daemon, or (for now) the built-in magnetic disk manager.
 *
 * Everything a backend needs to identify a page is expressed in
 * version-neutral terms (PageStoreRelKey + fork + block).  The only
 * PG-coupled value that crosses the boundary is the opaque "localreln"
 * cookie, which carries the SMgrRelation through for the passthrough backend
 * only; real (remote/SPDK) backends must ignore it and rely on the key.
 *
 * src/../contrib/pagestore/pagestore_backend.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_BACKEND_H
#define PAGESTORE_BACKEND_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

#include "pagestore_ipc.h"		/* PsWalRec */

/*
 * Version-neutral physical identity of a relation fork.  Deliberately built
 * from plain OIDs / numbers rather than RelFileLocator so the on-the-wire
 * identity does not change when PostgreSQL reshuffles its internal structs
 * between major versions.
 */
typedef struct PageStoreRelKey
{
	Oid			spcOid;			/* tablespace */
	Oid			dbOid;			/* database */
	RelFileNumber relNumber;	/* relation filenode */
	int32		forkNum;		/* ForkNumber, widened for stability */
} PageStoreRelKey;

/*
 * A storage backend.  All ops are page-granular (BLCKSZ buffers) and keyed by
 * version-neutral identity.  "localreln" is the originating SMgrRelation,
 * passed opaquely for the passthrough backend; remote backends ignore it.
 *
 * For M0 the interface is synchronous; later milestones add async
 * submit/complete variants (for SPDK polling and PG's AIO) alongside these.
 */
typedef struct PageStoreBackend
{
	const char *name;

	/*
	 * If true, this backend stores data in local files managed by md.c, so the
	 * shim may delegate local-only concerns (prefetch, writeback,
	 * registersync, fd, async startreadv) straight to md.  Remote backends set
	 * this false; the shim then handles those itself.
	 */
	bool		uses_local_files;

	/*
	 * Upper bound (in pages) on how many blocks the buffer manager may combine
	 * into a single readv/writev for this backend.  0 means "use md's default"
	 * (segment-boundary based).  Remote backends use this to keep an I/O
	 * within their transfer buffer.
	 */
	uint32		max_combine_pages;

	/* one-time per-backend initialization (may be NULL) */
	void		(*init) (void);

	/* --- fork lifecycle / metadata --- */

	/* create the fork (make it exist with zero blocks) */
	void		(*create) (const PageStoreRelKey *key, void *localreln, bool isRedo);
	/* does the fork exist? */
	bool		(*fork_exists) (const PageStoreRelKey *key, void *localreln);
	/* remove the fork entirely */
	void		(*unlink) (const PageStoreRelKey *key, bool isRedo);
	/* current size of the fork, in blocks */
	BlockNumber (*nblocks) (const PageStoreRelKey *key, void *localreln);
	/* shrink the fork from old_blocks down to nblocks blocks */
	void		(*truncate) (const PageStoreRelKey *key, void *localreln,
							 BlockNumber old_blocks, BlockNumber nblocks);

	/* --- data plane (vectored: nblocks contiguous BLCKSZ buffers) --- */

	/* read nblocks pages starting at blocknum into buffers[] */
	void		(*readv) (const PageStoreRelKey *key, void *localreln,
						  BlockNumber blocknum, void **buffers, BlockNumber nblocks);
	/* overwrite nblocks existing pages starting at blocknum from buffers[] */
	void		(*writev) (const PageStoreRelKey *key, void *localreln,
						   BlockNumber blocknum, const void **buffers,
						   BlockNumber nblocks, bool skipFsync);
	/* grow the fork by one block at blocknum, written from buffer */
	void		(*extend) (const PageStoreRelKey *key, void *localreln,
						   BlockNumber blocknum, const void *buffer, bool skipFsync);
	/*
	 * Grow the fork by nblocks *zero-filled* blocks starting at blocknum,
	 * without supplying page contents.  This is the bulk pre-allocation
	 * counterpart to extend(): the engine uses it to add many empty pages in
	 * one call (e.g. when extending a relation under concurrent insertion).
	 * The new pages read back as zeros until later written.
	 */
	void		(*zeroextend) (const PageStoreRelKey *key, void *localreln,
							   BlockNumber blocknum, int nblocks, bool skipFsync);

	/* --- durability --- */

	/* flush the fork's data durably to storage (immediate fsync equivalent) */
	void		(*immedsync) (const PageStoreRelKey *key, void *localreln);

	/*
	 * Asynchronous-read support for the AIO (smgr_startreadv) path.  Fetch
	 * nblocks pages starting at blocknum into a region the caller can read
	 * with preadv, and return that region as (fd, offset).  The shim then
	 * issues a normal AIO readv from (fd, offset) into the buffer pool, so all
	 * of PostgreSQL's read-completion machinery (checksum verify, marking
	 * BM_VALID) runs unchanged.  NULL for backends without an async path (the
	 * shim falls back to md for startreadv).
	 */
	bool		(*fetch_to_fd) (const PageStoreRelKey *key, BlockNumber blocknum,
								BlockNumber nblocks, int *out_fd, uint64 *out_offset);
} PageStoreBackend;

/*
 * Backend registry.  Backends register themselves (typically from this
 * module's _PG_init); the GUC pagestore.backend selects the active one.
 */
extern void pagestore_register_backend(const PageStoreBackend *backend);
extern const PageStoreBackend *pagestore_lookup_backend(const char *name);

/* The passthrough backend, defined in backend_passthrough.c. */
extern const PageStoreBackend PageStoreBackendPassthrough;

/* The localsvc backend (talks to the daemon), in backend_localsvc.c. */
extern const PageStoreBackend PageStoreBackendLocalSvc;
extern void pagestore_localsvc_init(void);
extern void pagestore_localsvc_read_at(const PageStoreRelKey *key,
									   BlockNumber blocknum, uint64 lsn, void *out);
extern void pagestore_localsvc_create_branch(uint32 new_tl, uint32 parent_tl,
											 uint64 branch_lsn);
extern void pagestore_localsvc_wal_append(uint64 start_lsn, const void *data,
										  uint32 len);
extern void pagestore_localsvc_walidx_add(const PageStoreRelKey *key,
										  BlockNumber block, uint64 lsn);
extern int	pagestore_localsvc_walidx_count(const PageStoreRelKey *key,
											BlockNumber block);
extern int	pagestore_localsvc_walidx_get(const PageStoreRelKey *key,
										  BlockNumber block, uint64 lsn_max,
										  PsWalRec *out, int maxn);
extern void pagestore_localsvc_obj_write(uint32 klass, const PageStoreRelKey *key,
										 BlockNumber block, const void *page);
extern void pagestore_localsvc_obj_read(uint32 klass, const PageStoreRelKey *key,
										BlockNumber block, void *page);

#endif							/* PAGESTORE_BACKEND_H */
