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

extern int	ps_manifest_open(const char *store_dir);
extern void ps_manifest_close(void);
extern int	ps_manifest_poisoned(void);
extern int	ps_manifest_replay(PsLayerMap *map);
extern int	ps_manifest_add_layer(const PsLayerDesc *desc);
extern int	ps_manifest_set_remote_durable(uint64_t layer_id,
										   uint64_t uploaded_lsn);
extern int	ps_manifest_mark_delete(uint64_t layer_id);
extern int	ps_manifest_remove_layer(uint64_t layer_id);

#endif							/* PAGESTORE_MANIFEST_H */
