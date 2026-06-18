/*-------------------------------------------------------------------------
 *
 * pagestore_layer.h
 *	  LSM-like immutable layer metadata for the page-store daemon.
 *
 * This file defines logical layer identity separately from physical placement.
 * A layer map answers "which immutable layers cover this key/LSN range";
 * layer-store code answers "where do the bytes come from".
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_LAYER_H
#define PAGESTORE_LAYER_H

#include <stdbool.h>
#include <stdint.h>

#include "pagestore_ipc.h"

#define PS_LAYER_URI_MAX	512

typedef enum PsLayerKind
{
	PS_LAYER_IMAGE = 1,
	PS_LAYER_DELTA = 2,
} PsLayerKind;

typedef enum PsLayerTier
{
	PS_LAYER_TIER_LOCAL_HOT = 1,
	PS_LAYER_TIER_LOCAL_COLD = 2,
	PS_LAYER_TIER_REMOTE_OBJECT = 3,
} PsLayerTier;

typedef struct PsLayerLocation
{
	PsLayerTier tier;
	char		uri[PS_LAYER_URI_MAX];
	uint64_t	size;
	uint32_t	generation;
	bool		available;
} PsLayerLocation;

typedef struct PsLayerDesc
{
	uint64_t	layer_id;
	PsLayerKind kind;

	uint32_t	timeline;
	PsKey		start_key;
	PsKey		end_key;
	uint32_t	start_block;
	uint32_t	end_block;

	uint64_t	lsn_start;
	uint64_t	lsn_end;

	uint32_t	location_count;
	PsLayerLocation locations[3];

	uint64_t	created_at_lsn;
	uint64_t	remote_uploaded_lsn;
	bool		remote_durable;
	bool		local_pinned;
	bool		deleting;
} PsLayerDesc;

typedef struct PsLayerMap
{
	PsLayerDesc *layers;
	uint32_t	nlayers;
	uint32_t	capacity;
} PsLayerMap;

extern void ps_layer_map_init(PsLayerMap *map);
extern void ps_layer_map_free(PsLayerMap *map);
extern int	ps_layer_map_add(PsLayerMap *map, const PsLayerDesc *desc);
extern uint32_t ps_layer_map_count(const PsLayerMap *map);

#endif							/* PAGESTORE_LAYER_H */
