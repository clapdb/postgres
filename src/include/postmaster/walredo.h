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

pg_noreturn extern void WalRedoMain(int argc, char *argv[]);

#endif							/* WALREDO_H */
