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
/* Remove one failed, unselected publication; return 1 if it was selected. */
extern int ps_walidx_snapshot_discard_generation(const char *directory,
											 uint32_t timeline,
											 uint64_t generation,
											 uint32_t nshards);
/* Open only the generation named by the durable manifest and validate every shard. */
extern int ps_walidx_snapshot_open(PsWalIdxSnapshot *snapshot,
								   const char *directory,
								   uint32_t timeline);
extern int ps_walidx_snapshot_read(const PsWalIdxSnapshot *snapshot,
								   uint32_t shard, uint64_t offset,
								   void *data, uint32_t len);
/* Remove immutable shard files older than the generation selected by the
 * validated manifest.  Returns 1 if files were removed, 0 if already clean,
 * and -1 on error.  Newer unpublished files are left for publication retry. */
extern int ps_walidx_snapshot_gc(const char *directory, uint32_t timeline);
extern void ps_walidx_snapshot_close(PsWalIdxSnapshot *snapshot);

#endif
