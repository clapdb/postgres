/*-------------------------------------------------------------------------
 *
 * walredo_client.h
 *	  Client for the `postgres --wal-redo` single-page redo helper.
 *
 * Spawns the helper as a child process and drives its stdin/stdout protocol
 * (BEGIN / PUSHBASE / APPLY / GET) so the page store can materialize a page
 * "as of" an LSN from a base image plus the WAL records after it, off the read
 * hot path.  See contrib/pagestore/WAL_REDO.md and
 * src/backend/postmaster/walredo.c (the helper).
 *
 *-------------------------------------------------------------------------
 */
#ifndef WALREDO_CLIENT_H
#define WALREDO_CLIENT_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

typedef struct WalRedoProc WalRedoProc;

/*
 * Start a helper against a private scratch data directory (never the live
 * cluster's).  ereport(ERROR) on failure.  Returns a handle owning the child.
 */
extern WalRedoProc *walredo_start(const char *datadir);

/* Set the target page identity. */
extern void walredo_begin(WalRedoProc *p, RelFileLocator rlocator,
						  ForkNumber forknum, BlockNumber blkno);

/* Seed the held page: a BLCKSZ base image (page != NULL) or zero (page == NULL),
 * and stamp its page-LSN baseline to base_end_lsn. */
extern void walredo_pushbase(WalRedoProc *p, XLogRecPtr base_end_lsn,
							 const char *page);

/* Apply one WAL record (its raw bytes) to the held page. */
extern void walredo_apply(WalRedoProc *p, XLogRecPtr start_lsn,
						  XLogRecPtr end_lsn, const char *record, uint32 len);

/* Fetch the materialized page (BLCKSZ bytes) into page_out. */
extern void walredo_get(WalRedoProc *p, char *page_out);

/* Close stdin (clean EOF shutdown) and reap the child; frees the handle. */
extern void walredo_stop(WalRedoProc *p);

#endif							/* WALREDO_CLIENT_H */
