/*-------------------------------------------------------------------------
 *
 * pagestore_retention.h
 *	  Durable registry of page-store retention pins.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_RETENTION_H
#define PAGESTORE_RETENTION_H

#include <stdint.h>

#include "pagestore_ipc.h"

extern int ps_retention_open(const char *store_dir);
extern void ps_retention_close(void);

#define PS_RETENTION_OK		0
#define PS_RETENTION_ERROR	(-1)
#define PS_RETENTION_STALE	(-2)

extern int ps_retention_set(const PsRetentionPin *pin);
extern int ps_retention_drop(uint32_t timeline, uint32_t owner_kind,
								 uint64_t owner_id, uint32_t generation);
extern int ps_retention_count(uint32_t *count_out);
extern int ps_retention_get(uint32_t index, PsRetentionPin *pin_out,
								 uint32_t *count_out);
extern int ps_retention_lookup(uint32_t timeline, uint32_t owner_kind,
									uint64_t owner_id, PsRetentionPin *pin_out);
extern int ps_retention_snapshot(PsRetentionPin *pins, uint32_t capacity,
								 uint32_t *count_out);
extern int ps_retention_snapshot_alloc(PsRetentionPin **pins_out,
									   uint32_t *count_out);

extern int ps_retention_should_compact(void);
extern int ps_retention_compact(void);

#endif							/* PAGESTORE_RETENTION_H */
