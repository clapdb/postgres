/*
 * Small, dependency-free marker publication rule shared by the materializer
 * and its boundary unit test.
 */
#ifndef PAGESTORE_MATERIALIZER_MARKER_H
#define PAGESTORE_MATERIALIZER_MARKER_H

#include <stdbool.h>
#include <stdint.h>

static inline bool
pagestore_materializer_marker_needs_publish(bool marker_valid,
									 uint64_t marker_lsn,
									 bool candidate_valid,
									 uint64_t candidate_lsn)
{
	return candidate_valid && (!marker_valid || candidate_lsn > marker_lsn);
}

#endif
