/*-------------------------------------------------------------------------
 *
 * pagestore_manifest.h
 *	  Durable metadata log for immutable pagestore layers.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_MANIFEST_H
#define PAGESTORE_MANIFEST_H

#include <stdint.h>

#include "pagestore_layer.h"

/*
 * A manifest is a durable event log plus the in-memory layer map it rebuilds.
 * One per shard (sharding step 4): each shard owns a separate manifest file and
 * layer map, so its compaction/GC touch no shared state.  layer_ids are
 * shard-namespaced (high bits = shard), so the layer files -- named by id -- never
 * collide across shards even though they share one directory.
 */
typedef struct PsManifest
{
	char		path[4096];
	char		dir[2048];
	PsLayerMap	map;
} PsManifest;

extern int	ps_manifest_open(PsManifest *m, const char *store_dir, uint32_t shard);
extern void ps_manifest_close(PsManifest *m);
extern int	ps_manifest_replay(PsManifest *m);
extern int	ps_manifest_add_layer(PsManifest *m, const PsLayerDesc *desc);
extern int	ps_manifest_set_remote_durable(PsManifest *m, uint64_t layer_id,
										   uint64_t uploaded_lsn);
extern int	ps_manifest_mark_delete(PsManifest *m, uint64_t layer_id);
extern int	ps_manifest_remove_layer(PsManifest *m, uint64_t layer_id);

#endif							/* PAGESTORE_MANIFEST_H */
