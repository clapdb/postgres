#ifndef PAGESTORE_WALIDX_SNAPSHOT_H
#define PAGESTORE_WALIDX_SNAPSHOT_H

#include <stddef.h>
#include <stdint.h>

#define PS_WALIDX_SNAPSHOT_MAX_SHARDS 128

typedef int (*PsWalIdxSnapshotConsume)(void *arg, const void *data, size_t len);
typedef int (*PsWalIdxSnapshotProduce)(void *arg,
									  PsWalIdxSnapshotConsume consume,
									  void *consume_arg);

typedef struct PsWalIdxSnapshotInput
{
	const void *data;
	uint64_t	len;
	PsWalIdxSnapshotProduce produce;
	void	   *produce_arg;
} PsWalIdxSnapshotInput;

typedef struct PsWalIdxSnapshotShard
{
	uint64_t	len;
	uint32_t	crc;
} PsWalIdxSnapshotShard;

typedef struct PsWalIdxSnapshot
{
	char		directory[4096];
	int			directory_fd;
	uint32_t	timeline;
	uint32_t	nshards;
	uint64_t	generation;
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	PsWalIdxSnapshotShard shards[PS_WALIDX_SNAPSHOT_MAX_SHARDS];
} PsWalIdxSnapshot;

typedef struct PsWalIdxSnapshotPrepared
{
	char		directory[4096];
	uint32_t	timeline;
	uint32_t	nshards;
	uint64_t	generation;
	uint64_t	start_lsn;
	uint64_t	end_lsn;
	PsWalIdxSnapshotShard shards[PS_WALIDX_SNAPSHOT_MAX_SHARDS];
} PsWalIdxSnapshotPrepared;

/* Write and fsync every immutable shard without changing the selected
 * generation.  The caller may durably publish a reclamation frontier after
 * this succeeds and before commit atomically replaces the manifest. */
extern int ps_walidx_snapshot_prepare(PsWalIdxSnapshotPrepared *prepared,
									  const char *directory, uint32_t timeline,
									  uint64_t generation, uint64_t start_lsn,
									  uint64_t end_lsn,
									  const PsWalIdxSnapshotInput *shards,
									  uint32_t nshards);
extern int ps_walidx_snapshot_commit(const PsWalIdxSnapshotPrepared *prepared);
/* Remove a prepared generation that has not been selected by the manifest. */
extern int ps_walidx_snapshot_abort(const PsWalIdxSnapshotPrepared *prepared);
/* Reconcile a durable prepare intent before the owning timeline accepts writes.
 * A frontier covering the prepared end retains it for publication retry; an
 * older frontier aborts it.  Returns zero when no intent needs cleanup. */
extern int ps_walidx_snapshot_recover_prepared(const char *directory,
										uint32_t timeline,
										uint64_t durable_frontier);

/* Publish a complete immutable shard generation, then atomically select it.
 * The owning timeline must serialize publishers with its append cutover.  Old
 * generation files deliberately remain available until reader drain and GC. */
extern int ps_walidx_snapshot_publish(const char *directory, uint32_t timeline,
									  uint64_t generation, uint64_t start_lsn,
									  uint64_t end_lsn,
									  const PsWalIdxSnapshotInput *shards,
									  uint32_t nshards);
/* Allocate above both the selected generation and immutable publication debris. */
extern int ps_walidx_snapshot_next_generation(const char *directory,
									  uint64_t selected_generation,
									  uint64_t *generation_out);
/* Return the durable prepare generation, or zero when no intent exists. */
extern int ps_walidx_snapshot_prepared_generation(const char *directory,
										 uint32_t timeline,
										 uint64_t *generation_out);
/* Read and validate the durable prepare intent, returning 0 when absent. */
extern int ps_walidx_snapshot_read_prepared(const char *directory,
										 uint32_t timeline,
										 PsWalIdxSnapshotPrepared *prepared);
/* Remove one failed, unselected publication; return 1 if it was selected. */
extern int ps_walidx_snapshot_discard_generation(const char *directory,
											 uint32_t timeline,
											 uint64_t generation,
											 uint32_t nshards);
/* Open only the generation named by the durable manifest and validate every shard. */
extern int ps_walidx_snapshot_open(PsWalIdxSnapshot *snapshot,
								   const char *directory,
								   uint32_t timeline);
/* Validate only manifest identity and selected shard metadata.  Immutable
 * payload CRCs are established at startup/publication, not every refresh. */
extern int ps_walidx_snapshot_open_metadata(PsWalIdxSnapshot *snapshot,
									 const char *directory,
									 uint32_t timeline);
extern int ps_walidx_snapshot_read(const PsWalIdxSnapshot *snapshot,
								   uint32_t shard, uint64_t offset,
								   void *data, uint32_t len);
/* Remove immutable shard files older than the generation selected by the
 * validated manifest.  Returns 1 if files were removed, 0 if already clean,
 * and -1 on error.  Newer unpublished files are left for publication retry. */
extern int ps_walidx_snapshot_gc(const char *directory, uint32_t timeline);
/* Sum physical immutable snapshot generations other than the selected one.
 * Canonical names and regular-file types are required; any scan/stat error
 * fails closed. */
extern int ps_walidx_snapshot_reclaim_bytes(const char *directory,
										uint32_t timeline,
										uint64_t selected_generation,
										uint64_t *bytes_out);
extern void ps_walidx_snapshot_close(PsWalIdxSnapshot *snapshot);

#endif
