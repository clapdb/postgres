/*-------------------------------------------------------------------------
 *
 * pagestore_layer_store.h
 *	  Physical byte access for immutable pagestore layers.
 *
 * The LSM core addresses layers by logical layer id.  This interface hides
 * whether a layer is local, cached from object storage, or remote-only.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_LAYER_STORE_H
#define PAGESTORE_LAYER_STORE_H

#include <stdint.h>

#include "pagestore_layer.h"

typedef struct PsLayerStore
{
	const char *name;

	int			(*open) (const char *store_dir);
	void		(*close) (void);
	int			(*create_local_layer) (uint64_t layer_id, char *uri,
									   uint32_t uri_len);
	/* append-write the whole sealed contents of a created local layer */
	int			(*write_local_layer) (uint64_t layer_id, const void *buf,
									  uint64_t len);
	int			(*seal_local_layer) (uint64_t layer_id);
	int			(*read_layer_block) (const PsLayerDesc *layer, uint64_t off,
									 void *buf, uint32_t len);
	int			(*upload_layer) (const PsLayerDesc *layer);
	int			(*download_layer) (const PsLayerDesc *layer);
	int			(*delete_local_layer) (const PsLayerDesc *layer);
	int			(*delete_remote_layer) (const PsLayerDesc *layer);
	int			(*layer_exists_remote) (const PsLayerDesc *layer);
} PsLayerStore;

extern const PsLayerStore PsLayerStoreLocal;
extern const PsLayerStore *ps_layer_store;

/*
 * Configure the object tier (LSM phase 4): 'dir' is the object store (a local
 * directory standing in for a remote bucket).  NULL/unset disables it, and the
 * upload/download/delete-remote ops then return ENOTSUP.  Call before open.
 * Returns -1 (and disables the tier) if 'dir' is too long to store.
 */
extern int	ps_layer_store_set_object_dir(const char *dir);

#endif							/* PAGESTORE_LAYER_STORE_H */
