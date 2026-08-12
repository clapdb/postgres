/* Pure page-version retention policy used by image compaction. */
#ifndef PAGESTORE_PRUNE_H
#define PAGESTORE_PRUNE_H

#include <stdint.h>

typedef struct PsPruneVersion
{
	uint64_t	lsn;
	uint64_t	admission_seq;
} PsPruneVersion;

/*
 * versions must be sorted by (lsn, admission_seq).  keep receives one byte per
 * input version.  A zero floor means no historical horizon is retained.
 * Returns the number of kept versions, or -1 for invalid input.
 */
extern int ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
							  uint64_t floor, unsigned char *keep);

#endif
