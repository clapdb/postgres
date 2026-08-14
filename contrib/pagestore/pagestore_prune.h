/* Pure page-version retention policy used by image compaction. */
#ifndef PAGESTORE_PRUNE_H
#define PAGESTORE_PRUNE_H

#include <stdint.h>

typedef struct PsPruneVersion
{
	uint64_t	lsn;
	uint64_t	admission_seq;
} PsPruneVersion;

typedef PsPruneVersion PsPruneFence;

/*
 * versions must be sorted by (lsn, admission_seq), preserving source append
 * order for exact tuple ties; the last tied element is authoritative.  keep
 * receives one byte per input version.  A floor with either component zero has
 * no proven reclamation meaning and is rejected; the caller must supply an
 * exact durable materialization cutoff.  fences
 * are exact reader-admission or structural branch requirements and do not
 * lower the moving operational floor.
 * Returns the number of kept versions, or -1 for invalid input.
 */
extern int ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
							  PsPruneFence floor, const PsPruneFence *fences,
							  uint32_t nfences, unsigned char *keep);

#endif
