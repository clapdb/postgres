#ifndef PAGESTORE_FAULT_H
#define PAGESTORE_FAULT_H

#include <stdint.h>

/* ps_fault_probe() return values.  Crash never returns: it exits with 88.
 * A pause watchdog timeout never returns: it exits with 90. */
#define PS_FAULT_PROBE_INACTIVE 0
#define PS_FAULT_PROBE_ERROR (-1)
#define PS_FAULT_PROBE_PAUSE_TIMEOUT (-2) /* internal wait result */
#define PS_FAULT_CRASH_EXIT 88
#define PS_FAULT_REPORT_FAILURE_EXIT 89
#define PS_FAULT_PAUSE_TIMEOUT_EXIT 90 /* reserved process-level mapping */

typedef enum PsFaultPoint
{
#define PAGESTORE_FAULT_POINT(symbol, name, target, model, action, hit, max_hit) \
	PS_FAULT_POINT_##symbol,
#include "pagestore_fault_points.def"
#undef PAGESTORE_FAULT_POINT
	PS_FAULT_POINT_COUNT,
	PS_FAULT_POINT_INVALID = -1
} PsFaultPoint;

int ps_fault_init(const char *store_dir);
int ps_fault_is_initialized(void);
int ps_fault_lookup(const char *name, PsFaultPoint *point);
const char *ps_fault_name(PsFaultPoint point);
const char *ps_fault_allowed_actions(PsFaultPoint point);

typedef struct PsFaultStatus
{
	int initialized;
	int enabled;
	int reached;
	PsFaultPoint point;
	uint64_t target_hit;
	uint64_t hits;
} PsFaultStatus;

/* Query is process-local and allocation-free.  It is valid for unhit faults,
 * which lets a harness distinguish an armed-but-unreached point from a bad
 * setup without inspecting the report file. */
int ps_fault_query(PsFaultPoint point, PsFaultStatus *status);
int ps_fault_probe(PsFaultPoint point);
void ps_fault_reset(void);

#endif
