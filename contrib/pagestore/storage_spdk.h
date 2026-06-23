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
 * Submit one page read of 'len' bytes at (shard, seg, off) into 'dst' (shared memory).
 * A read of the current (buffered) append segment is served from memory and
 * 'done' is called before returning; a device read is queued and 'done' fires
 * later from ps_spdk_poll().  Either way the engine copies into dst (or zeroes
 * it on error) before calling done.  If no DMA buffer is free it polls until one
 * frees, so submission always eventually succeeds.  Returns 0.
 */
extern int	ps_spdk_read_async(uint32_t shard, int seg, uint64_t off,
						void *dst, uint32_t len,
						PsSpdkDone done, void *arg);

/* Per-thread/per-shard asynchronous I/O context lifecycle. */
extern int	ps_spdk_thread_init(uint32_t shard);
extern void	ps_spdk_thread_close(uint32_t shard);

/* Drive NVMe completions for one shard context; returns the number processed. */
extern int	ps_spdk_poll(uint32_t shard);

#endif							/* STORAGE_SPDK_H */
