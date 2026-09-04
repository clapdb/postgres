#ifndef PAGESTORE_WAL_STORE_H
#define PAGESTORE_WAL_STORE_H

#include <stdint.h>
#include <pthread.h>

#include "pagestore_wal_segment.h"

typedef struct PsWalStoreEntry
{
	PsWalSegmentHeader header;
	uint32_t   *chunk_hashes;
	uint32_t	nchunks;
} PsWalStoreEntry;

#define PS_WAL_STORE_VERIFY_CHUNK_BYTES (64u * 1024u)
/* Keep the path stable so existing timeline directories remain discoverable.
 * The contents are PSWALSTORE2; PSWALSTORE1 is accepted only long enough to
 * validate the directory and migrate it atomically. */
#define PS_WAL_STORE_IDENTITY_FILE "wal_store_identity_v1"

typedef struct PsWalStore
{
	char		directory[4096];
	uint32_t	timeline;
	uint32_t	segment_size;
	uint64_t	next_segment_no;
	/* First segment still needed by the logical store.  Older, already
	 * authorized segments may remain on disk until a later reclaimer unlinks
	 * them. */
	uint64_t	start_lsn;
	uint64_t	retained_base_lsn;
	uint64_t	end_lsn;
	/* A durable physical frontier can outlive the unlink phase across a
	 * process stop.  This state is runtime-only and is reconstructed by the
	 * open-time residual-prefix scan. */
	int		residual_prefix_pending;
	uint64_t	residual_prefix_target_lsn;
	int			directory_fd;
	int			metadata_fenced;
	pthread_mutex_t lock;
	int			lock_initialized;
	PsWalStoreEntry *entries;
	uint32_t	nentries;
	uint32_t	capacity;
} PsWalStore;

extern int ps_wal_store_create(PsWalStore *store, const char *directory,
							   uint32_t timeline, uint64_t start_lsn,
							   uint32_t segment_size);
/* Open and validate an existing contiguous immutable WAL segment store. */
extern int ps_wal_store_open(PsWalStore *store, const char *directory,
							 uint32_t timeline, uint64_t start_lsn,
							 uint32_t segment_size);
/* Read the durable store identity and validate the full immutable store.  This
 * is used after the duplicate flat-log prefix no longer records the immutable
 * store's original start LSN.  Suffix reconciliation assumes this is a private
 * single-writer directory managed by timeline/incarnation cleanup: only
 * ps_wal_store_append/publish_segment can create a legal contiguous final
 * segment, so a valid contiguous suffix represents a pre-metadata crash. */
extern int ps_wal_store_open_existing(PsWalStore *store, const char *directory,
								  uint32_t timeline, uint32_t segment_size);
/* Append one or more complete, segment-aligned immutable WAL segments. */
extern int ps_wal_store_append(PsWalStore *store, uint64_t start_lsn,
								   const void *data, uint32_t len);
/* Return the durable logical floor.  Invalid, unopened, or fenced stores fail
 * without writing *out; this never removes an immutable file. */
extern int ps_wal_store_retained_base(PsWalStore *store, uint64_t *out);
/* Atomically publish a segment-aligned, monotonic logical floor.  This is a
 * logical retention operation only; it never authorizes physical deletion. */
extern int ps_wal_store_advance_retained_base(PsWalStore *store,
									  uint64_t retained_base_lsn);
/* Publish the logical/physical frontier, then reclaim only immutable segments
 * strictly below target_lsn.  The store mutex drains readers and bars new
 * below-frontier reads for the whole operation.  A successful call is the
 * only WAL-store API that authorizes prefix unlink. */
extern int ps_wal_store_reclaim_prefix(PsWalStore *store,
								   uint64_t target_lsn);
/* Return whether a previously published prefix still needs physical unlink.
 * The result and target are read under the store mutex. */
extern int ps_wal_store_residual_prefix_pending(PsWalStore *store,
										 uint64_t *target_lsn_out);
extern int ps_wal_store_read(PsWalStore *store, uint64_t start_lsn,
								 void *data, uint32_t len);
/* The caller must externally prevent concurrent operations and retain the
 * store object's lifetime until all other API calls have returned. */
extern void ps_wal_store_close(PsWalStore *store);

#endif
