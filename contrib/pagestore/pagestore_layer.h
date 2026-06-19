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

#define PS_LAYER_MAX_LOCATIONS	3
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
	PsLayerLocation locations[PS_LAYER_MAX_LOCATIONS];

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
extern int	ps_layer_map_reserve(PsLayerMap *map, uint32_t capacity);
extern int	ps_layer_map_add(PsLayerMap *map, const PsLayerDesc *desc);
extern uint32_t ps_layer_map_count(const PsLayerMap *map);

/* ---------------------------------------------------------------------------
 * Image layer file format (phase 2).
 *
 * An image layer is an immutable file holding full-page versions:
 *     [ page data : nrecs * page_size, back to back ]
 *     [ index     : nrecs * PsImgIndexEnt, sorted by (key, block, lsn) ]
 *     [ footer    : PsImgFooter, fixed-size trailer at end of file ]
 * It stores *multiple versions* per (key, block) (keyed by page_lsn), so a read
 * picks the newest version <= read_lsn -- matching the current version-chain
 * (COW) semantics, just persisted immutably.  Data, index and footer carry
 * checksums.
 * ------------------------------------------------------------------------- */
#define PS_IMG_MAGIC	0x47494d50	/* "PIMG" */
#define PS_IMG_VERSION	2			/* v2 added the per-page crc */

typedef struct PsImgIndexEnt
{
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;			/* page_lsn of this version */
	uint64_t	data_off;		/* byte offset of the page within the file */
	uint32_t	crc;			/* checksum of this page, verified before serving */
} PsImgIndexEnt;

typedef struct PsImgFooter
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	page_size;
	uint32_t	nrecs;
	uint64_t	index_off;		/* == page-data section size */
	uint32_t	data_crc;		/* crc of the page-data section */
	uint32_t	index_crc;		/* crc of the index section */
} PsImgFooter;

/* One full-page version to write into an image layer. */
typedef struct PsImgRec
{
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	const void *page;
} PsImgRec;

/*
 * Write 'n' page versions into image layer 'layer_id' via the layer store, seal
 * it, and fill *out with the resulting descriptor (key/block/LSN range, local
 * location).  Returns 0 on success.
 */
extern int	ps_image_layer_write(uint64_t layer_id, uint32_t timeline,
								 const PsImgRec *recs, uint32_t n,
								 uint32_t page_size, PsLayerDesc *out);

/*
 * Look up the newest version of (key, block) with lsn <= read_lsn in image
 * layer 'layer'.  On a hit copies page_size bytes into out, stores that
 * version's lsn in *out_lsn (if non-NULL), and returns 1; 0 if the layer has no
 * such page; -1 on read/format error.
 */
extern int	ps_image_layer_lookup(const PsLayerDesc *layer, const PsKey *key,
								  uint32_t block, uint64_t read_lsn,
								  void *out, uint32_t page_size,
								  uint64_t *out_lsn);

/*
 * Read an image layer's full index (every (key, block, lsn) entry), for
 * rebuilding the in-memory version index at startup without scanning page data.
 * On success *out is a malloc'd array of *n entries (caller frees) and the
 * return is 0; -1 on read/format/checksum error.
 */
extern int	ps_image_layer_read_index(const PsLayerDesc *layer,
									  PsImgIndexEnt **out, uint32_t *n);

/* Per-page checksum (matches the writer's per-page index crc); lets callers that
 * read page bytes directly (compaction) validate them against idx[].crc. */
extern uint32_t ps_image_page_crc(const void *page, uint32_t len);

#endif							/* PAGESTORE_LAYER_H */
