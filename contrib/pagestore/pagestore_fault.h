#ifndef PAGESTORE_FAULT_H
#define PAGESTORE_FAULT_H

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
int ps_fault_lookup(const char *name, PsFaultPoint *point);
const char *ps_fault_name(PsFaultPoint point);
int ps_fault_probe(PsFaultPoint point);
void ps_fault_reset(void);

#endif
