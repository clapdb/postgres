/*-------------------------------------------------------------------------
 *
 * walredo.h
 *	  single-page WAL-redo helper process (postgres --wal-redo)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/walredo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WALREDO_H
#define WALREDO_H

#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/relfilelocator.h"
#include "common/relpath.h"

pg_noreturn extern void WalRedoMain(int argc, char *argv[]);

/*
 * Set while a record's rm_redo runs in the wal-redo helper, so that
 * XLogReadBufferExtended() is redirected to the helper's held/scratch buffers
 * instead of the (absent) on-disk relation.  Defined in walredo.c; always false
 * in a normal backend.
 */
extern PGDLLIMPORT bool am_walredo;

/*
 * Redirect target for XLogReadBufferExtended() while am_walredo is set: returns
 * the held page's buffer for the BEGIN target block, a scratch buffer otherwise.
 */
extern Buffer WalRedoReadBuffer(RelFileLocator rlocator, ForkNumber forknum,
								BlockNumber blkno, ReadBufferMode mode);

#endif							/* WALREDO_H */
