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

extern PsLayerMap ps_layer_map;

typedef struct PsFlushWatermark
{
	uint32_t	shard;
	uint32_t	seg_id;
	uint64_t	seg_off;
} PsFlushWatermark;

extern int	ps_manifest_open(const char *store_dir);
extern void ps_manifest_close(void);
extern int	ps_manifest_poisoned(void);
extern int	ps_manifest_replay(PsLayerMap *map);
extern int	ps_manifest_add_layer(const PsLayerDesc *desc);
extern int	ps_manifest_set_remote_location(uint64_t layer_id,
											const PsLayerLocation *location);
extern int	ps_manifest_set_remote_durable(uint64_t layer_id,
										   uint64_t uploaded_lsn);
extern int	ps_manifest_drop_local(uint64_t layer_id);
extern int	ps_manifest_mark_delete(uint64_t layer_id);
extern int	ps_manifest_remove_layer(uint64_t layer_id);
extern int	ps_manifest_set_flush_watermark(uint32_t shard, uint32_t seg_id,
										 uint64_t seg_off);
extern int	ps_manifest_rebase_flush_watermark(uint32_t shard, uint32_t seg_id,
										 uint64_t seg_off);
extern int	ps_manifest_get_flush_watermark(uint32_t shard,
										 PsFlushWatermark *out);

/* Bound replay time by rewriting the append-only log to one record per live layer. */
extern int	ps_manifest_should_compact(void);
extern int	ps_manifest_compact(void);

#endif							/* PAGESTORE_MANIFEST_H */
