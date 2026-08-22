#include <limits.h>
#include <string.h>

#include "pagestore_forkmeta_prune.h"

static int
event_visible(const PsForkMetaEvent *event, PsForkMetaFence fence)
{
	return event->lsn < fence.lsn ||
		(event->lsn == fence.lsn &&
		 (fence.admission_seq == 0 || event->admission_seq == 0 ||
		  event->admission_seq <= fence.admission_seq));
}

static int
fence_above_cutoff(PsForkMetaFence fence, PsForkMetaFence cutoff)
{
	if (fence.lsn != cutoff.lsn)
		return fence.lsn > cutoff.lsn;
	/* A zero fence sequence exposes more than a precise cutoff does. */
	return fence.admission_seq == 0 ||
		fence.admission_seq > cutoff.admission_seq;
}

static void
retain_horizon(const PsForkMetaEvent *events, uint32_t nitems,
			   PsForkMetaFence horizon, unsigned char *keep)
{
	uint32_t latest_def = 0;
	uint32_t largest_grow = 0;
	int have_def = 0;
	int have_grow = 0;

	for (uint32_t i = 0; i < nitems; i++)
	{
		if (!event_visible(&events[i], horizon))
			continue;
		if (events[i].kind == PS_FORKMETA_SET ||
			events[i].kind == PS_FORKMETA_DEAD)
		{
			latest_def = i;
			have_def = 1;
			keep[i] = 1;
		}
	}

	for (uint32_t i = have_def ? latest_def + 1 : 0; i < nitems; i++)
	{
		if (event_visible(&events[i], horizon) &&
			events[i].kind == PS_FORKMETA_GROW &&
			(!have_grow ||
			 events[i].nblocks > events[largest_grow].nblocks ||
			 (events[i].nblocks == events[largest_grow].nblocks &&
			  i > largest_grow)))
		{
			largest_grow = i;
			have_grow = 1;
		}
	}
	if (have_grow)
		keep[largest_grow] = 1;
}

int
ps_forkmeta_prune_plan(const PsForkMetaEvent *events, uint32_t nitems,
					   PsForkMetaFence cutoff,
					   const PsForkMetaFence *fences, uint32_t nfences,
					   unsigned char *keep)
{
	int kept = 0;
	uint64_t last_nonzero_seq = 0;

	if (cutoff.lsn == 0 || cutoff.admission_seq == 0 ||
		nitems > (uint32_t) INT_MAX ||
		(nitems != 0 && (events == NULL || keep == NULL)) ||
		(nfences != 0 && fences == NULL))
		return -1;
	for (uint32_t i = 0; i < nfences; i++)
		if (fences[i].lsn == 0 || fence_above_cutoff(fences[i], cutoff))
			return -1;
	if (nitems == 0)
		return 0;
	memset(keep, 0, nitems);

	for (uint32_t i = 0; i < nitems; i++)
	{
		if (events[i].kind > PS_FORKMETA_DEAD ||
			(events[i].kind == PS_FORKMETA_DEAD && events[i].nblocks != 0) ||
			(i != 0 && events[i].lsn < events[i - 1].lsn))
			return -1;
		if (i == 0 || events[i].lsn != events[i - 1].lsn)
			last_nonzero_seq = 0;
		if (events[i].admission_seq != 0)
		{
			if (last_nonzero_seq != 0 &&
				events[i].admission_seq < last_nonzero_seq)
				return -1;
			last_nonzero_seq = events[i].admission_seq;
		}
		if (!event_visible(&events[i], cutoff))
			keep[i] = 1;
	}

	retain_horizon(events, nitems, cutoff, keep);
	for (uint32_t i = 0; i < nfences; i++)
		retain_horizon(events, nitems, fences[i], keep);
	for (uint32_t i = 0; i < nitems; i++)
		if (keep[i])
			kept++;
	return kept;
}
