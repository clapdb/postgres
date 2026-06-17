/*-------------------------------------------------------------------------
 *
 * storage_spdk.h
 *	  Extra (beyond the PsStorage vtable) entry points of the SPDK backend,
 *	  used only by the SPDK daemon frontend for the asynchronous read path.
 *
 * The PsStorage vtable stays synchronous (the shared brain and recover() use
 * it); this batched read API lets the SPDK frontend overlap many page reads of
 * one request on the NVMe queue instead of waiting for each in turn.
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_SPDK_H
#define STORAGE_SPDK_H

#include <stdint.h>

/* One page read: 'len' bytes at (seg, off) copied into 'dst' (shared memory). */
typedef struct PsSpdkRead
{
	int			seg;
	uint64_t	off;
	void	   *dst;
	uint32_t	len;
} PsSpdkRead;

/*
 * Issue all 'n' reads, overlapping the device reads on the NVMe queue, and wait
 * for them all.  Reads of the current (buffered) append segment are served from
 * memory.  A read that fails leaves its 'dst' zero-filled.  Returns 0.
 */
extern int	ps_spdk_readv(const PsSpdkRead *reqs, int n);

#endif							/* STORAGE_SPDK_H */
