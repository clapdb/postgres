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
 * version's lsn in *out_lsn (if non-NULL), sets *out_ambiguous (if non-NULL) to
 * 1 when more than one version shares that lsn (a same-lsn rewrite the lsn can't
 * disambiguate), and returns 1; 0 if the layer has no such page; -1 on
 * read/format error.
 */
extern int	ps_image_layer_lookup(const PsLayerDesc *layer, const PsKey *key,
								  uint32_t block, uint64_t read_lsn,
								  void *out, uint32_t page_size,
								  uint64_t *out_lsn, int *out_ambiguous);

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

/* ---------------------------------------------------------------------------
 * Delta layer file format (phase 7).
 *
 * A delta layer is an immutable file holding redo inputs (WAL records / page
 * deltas) keyed by (key, block, record_lsn), variable length:
 *     [ delta payloads, back to back ]
 *     [ index : nrecs * PsDeltaIndexEnt, sorted by (key, block, lsn) ]
 *     [ footer : PsDeltaFooter ]
 * A read applies the deltas of (key, block) in ascending LSN order on top of a
 * base image (an image layer) to reconstruct the page as of read_lsn.  The redo
 * itself (rm_redo on a held page) is the wal-redo helper -- this format just
 * stores and serves the ordered deltas.
 * ------------------------------------------------------------------------- */
#define PS_DELTA_MAGIC		0x544c4450	/* "PDLT" */
#define PS_DELTA_VERSION	2			/* v2 added the per-record crc */

typedef struct PsDeltaIndexEnt
{
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;			/* record LSN of this delta */
	uint64_t	data_off;		/* byte offset of the payload in the file */
	uint32_t	data_len;		/* payload length */
	uint32_t	crc;			/* checksum of this payload, verified before use */
} PsDeltaIndexEnt;

typedef struct PsDeltaFooter
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	nrecs;
	uint32_t	_pad;
	uint64_t	index_off;		/* == payload section size */
	uint32_t	data_crc;
	uint32_t	index_crc;
} PsDeltaFooter;

/* One delta (redo input) to write into a delta layer. */
typedef struct PsDeltaRec
{
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;
	const void *delta;
	uint32_t	delta_len;
} PsDeltaRec;

/* A delta read back from a layer (payload pointer is into a caller-freed buf). */
typedef struct PsDeltaOut
{
	uint64_t	lsn;
	uint64_t	data_off;
	uint32_t	data_len;
	uint32_t	crc;			/* expected checksum of the payload bytes */
} PsDeltaOut;

/* Write 'n' deltas into delta layer 'layer_id', seal it, fill *out. */
extern int	ps_delta_layer_write(uint64_t layer_id, uint32_t timeline,
								 const PsDeltaRec *recs, uint32_t n,
								 PsLayerDesc *out);

/*
 * Collect the deltas of (key, block) with lo_lsn <= lsn < hi_lsn from delta
 * layer 'layer', in ascending LSN order, appending to *outs and updating *n.
 * (Half-open: delta keys are WAL record start-LSNs and an image base_lsn is the
 * start of the first record to apply, so the base's record is included and a
 * record starting exactly at hi_lsn==read_lsn is excluded.)
 * *outs and *cap are a caller-owned growable buffer (start them NULL and 0; the
 * callee realloc's as needed and the caller frees *outs); there is no fixed
 * per-layer chain cap.  Each entry describes offset+len+crc of the payload in the
 * layer; call ps_layer_store read_layer_block at (data_off,data_len) to fetch a
 * payload.  Returns 0 on success, -1 on read/format error.
 */
extern int	ps_delta_layer_collect(const PsLayerDesc *layer, const PsKey *key,
								   uint32_t block, uint64_t lo_lsn,
								   uint64_t hi_lsn, PsDeltaOut **outs,
								   uint32_t *cap, uint32_t *n);

/* ---------------------------------------------------------------------------
 * Read plan (phase 7b): how to reconstruct (key, block) as of read_lsn from the
 * layers -- the newest image-layer version <= read_lsn (the base) plus every
 * delta in [base_lsn, read_lsn), in ascending LSN order (half-open: delta keys
 * are WAL record start-LSNs, and base_lsn is the start of the first record to
 * apply, so the base's record is included and one starting at read_lsn is not).
 * The redo helper (7c)
 * applies the deltas onto the base; if there are no deltas the base *is* the
 * page.  Payloads are materialized into the plan so the consumer needs no layer
 * access.  Single timeline for now (branch ancestry is a follow-up).
 * ------------------------------------------------------------------------- */
typedef struct PsPlanDelta
{
	uint64_t	lsn;
	uint32_t	len;
	unsigned char *bytes;		/* malloc'd payload */
} PsPlanDelta;

typedef struct PsReadPlan
{
	int			has_base;
	uint64_t	base_lsn;
	unsigned char *base;		/* malloc'd page_size, or NULL if no base */
	PsPlanDelta *deltas;		/* malloc'd, ascending lsn */
	uint32_t	ndelta;
} PsReadPlan;

/*
 * Build the read plan for (timeline, key, block) as of read_lsn from 'map'.
 * Returns 0 on success (plan filled; plan->has_base may be 0 if no image covers
 * it), -1 on a read/format error.  Free with ps_read_plan_free.
 */
extern int	ps_read_plan_build(const PsLayerMap *map, uint32_t timeline,
							   const PsKey *key, uint32_t block,
							   uint64_t read_lsn, uint32_t page_size,
							   PsReadPlan *plan);
extern void ps_read_plan_free(PsReadPlan *plan);

#endif							/* PAGESTORE_LAYER_H */
