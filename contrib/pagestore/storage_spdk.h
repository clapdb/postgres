/*-------------------------------------------------------------------------
 *
 * storage_spdk.h
 *	  Extra (beyond the PsStorage vtable) entry points of the SPDK backend,
 *	  used only by the SPDK daemon frontend for the asynchronous read path.
 *
 * The PsStorage vtable stays synchronous (the shared brain and recover() use
 * it).  This async submit/poll API lets the SPDK frontend keep many page reads
 * -- across many request channels -- in flight on the NVMe queue at once and
 * serve each reply from its completion, instead of finishing one request before
 * starting the next.
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_SPDK_H
#define STORAGE_SPDK_H

#include <stdint.h>

/* Completion callback: ok=1 if the page was delivered, 0 on a device error
 * (in which case the engine has zero-filled dst). */
typedef void (*PsSpdkDone) (void *arg, int ok);

/*
 * Submit one page read of 'len' bytes at (seg, off) into 'dst' (shared memory).
 * A read of the current (buffered) append segment is served from memory and
 * 'done' is called before returning; a device read is queued and 'done' fires
 * later from ps_spdk_poll().  Either way the engine copies into dst (or zeroes
 * it on error) before calling done.  If no DMA buffer is free it polls until one
 * frees, so submission always eventually succeeds.  Returns 0.
 */
extern int	ps_spdk_read_async(int seg, uint64_t off, void *dst, uint32_t len,
							   PsSpdkDone done, void *arg);

/* Drive NVMe completions on shard's queue pair; returns the number processed.
 * Each shard's worker polls only its own qpair (sharding step 5). */
extern int	ps_spdk_poll(uint32_t shard);

/* Flush one shard's current-segment buffer; call from that shard's worker thread
 * (drives the shard's qpair).  *out_count (if non-NULL) returns the shard's
 * segment count as of the flush, for a consistent superblock snapshot.  Used to
 * coordinate a cross-shard IMMEDSYNC. */
extern int	ps_spdk_flush(uint32_t shard, uint32_t *out_count);

/* Persist the superblock from a per-shard count snapshot (from ps_spdk_flush);
 * call once after all shards have flushed for an IMMEDSYNC. */
extern void ps_spdk_super_write_counts(const uint32_t *counts);

#endif							/* STORAGE_SPDK_H */
