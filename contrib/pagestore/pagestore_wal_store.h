#ifndef PAGESTORE_WAL_STORE_H
#define PAGESTORE_WAL_STORE_H

#include <stdint.h>

#include "pagestore_wal_segment.h"

typedef struct PsWalStoreEntry
{
	PsWalSegmentHeader header;
	uint32_t   *chunk_hashes;
	uint32_t	nchunks;
} PsWalStoreEntry;

#define PS_WAL_STORE_VERIFY_CHUNK_BYTES (64u * 1024u)

typedef struct PsWalStore
{
	char		directory[4096];
	uint32_t	timeline;
	uint32_t	segment_size;
	uint64_t	next_segment_no;
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	int			directory_fd;
	PsWalStoreEntry *entries;
	uint32_t	nentries;
	uint32_t	capacity;
} PsWalStore;

extern int ps_wal_store_create(PsWalStore *store, const char *directory,
							   uint32_t timeline, uint64_t start_lsn,
							   uint32_t segment_size);
/* Reopen against the caller checkpoint.  A fully validated published suffix
 * advances store->end_lsn so the caller can checkpoint the recovered bound. */
extern int ps_wal_store_open(PsWalStore *store, const char *directory,
							 uint32_t timeline, uint64_t start_lsn,
							 uint64_t end_lsn, uint32_t segment_size);
/* Append one or more complete, segment-aligned immutable WAL segments. */
extern int ps_wal_store_append(PsWalStore *store, uint64_t start_lsn,
							   const void *data, uint32_t len);
extern int ps_wal_store_read(PsWalStore *store, uint64_t start_lsn,
							 void *data, uint32_t len);
extern void ps_wal_store_close(PsWalStore *store);

#endif
