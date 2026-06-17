/*-------------------------------------------------------------------------
 *
 * pagestore_core.h
 *	  Shared "brain" of the page-store daemon.
 *
 * The in-memory indexes, copy-on-write read-through, timelines, per-page WAL
 * index, shipped-WAL metadata and recovery live here and are compiled into both
 * the POSIX daemon and the (optional) SPDK daemon, so the store's core logic is
 * single-sourced.  Each frontend supplies only its request loop and the page
 * byte I/O -- synchronous for POSIX, callback-driven for SPDK -- which goes
 * through the PsStorage interface.  The IPC ABI is unaffected by any of this.
 *
 * Seam: ps_handle_meta() handles every request that is not page byte I/O; the
 * four byte-I/O ops are done by each frontend using read_through()/read_version()
 * (reads) and append_page()/fork_grow() (writes).
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_CORE_H
#define PAGESTORE_CORE_H

#include <stdint.h>

#include "pagestore_ipc.h"
#include "pagestore_storage.h"

/* One stored version of a page: its LSN and where the bytes live in the store. */
typedef struct PageVer
{
	uint64_t	lsn;			/* the page's pd_lsn when it was written */
	int			seg;			/* segment id holding the bytes */
	uint64_t	off;			/* byte offset of the page within that segment */
} PageVer;

/* Configuration shared with the frontend; set by the frontend before open. */
extern uint32_t page_size;
extern uint64_t segment_size;
extern const PsStorage *ps_storage;

/* Open the store and rebuild all in-memory state (timelines, indexes, WAL). */
extern int	ps_core_open(const char *store_dir);

/*
 * Handle every request that is NOT page byte I/O and return 1.  The four
 * byte-I/O ops (EXTEND/WRITEV/READV/READ_AT) and unknown ops return 0 for the
 * frontend to handle.  Sets ch->status/ch->result as appropriate.
 */
extern int	ps_handle_meta(PsChannel *ch);

/* Page byte-I/O helpers used by the frontends' byte-op handlers. */
extern int	append_page(uint32_t timeline, const PsKey *key, uint32_t block,
						const unsigned char *page);
extern PageVer *read_through(uint32_t timeline, const PsKey *key, uint32_t block,
							 uint64_t read_lsn);
extern int	read_version(const PageVer *v, unsigned char *out);
extern void fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks);

#endif							/* PAGESTORE_CORE_H */
