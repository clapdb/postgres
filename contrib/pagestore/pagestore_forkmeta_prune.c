#include <limits.h>
#include <string.h>

#include "pagestore_admissible.h"
#include "pagestore_forkmeta_prune.h"

_Static_assert(PS_FORKMETA_F_META == PS_ADM_F_META &&
			   PS_FORKMETA_F_META_FIRST == PS_ADM_F_META_FIRST &&
			   PS_FORKMETA_F_UNSTAMPED == PS_ADM_F_UNSTAMPED,
			   "PS_FORKMETA_F_* must track pagestore_admissible.h's "
			   "PS_ADM_F_* bit for bit -- event_admit_flags() passes a "
			   "PsForkMetaEvent's flags straight through");

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

/*
 * The effective S1.3/S3.2 classification flags for an event (design doc
 * S3.7 item 1): SET/DEAD are always META (kind implies it -- callers never
 * need to set PS_FORKMETA_F_META for them); a GROW is META only when its
 * flags say so (a ZEROEXTEND-origin GROW), otherwise PAGE-class.
 * UNSTAMPED/META_FIRST pass through unchanged; PS_FORKMETA_F_* and
 * pagestore_admissible.h's PS_ADM_F_* share bit numbering (the
 * _Static_assert above), so no translation is needed.
 */
static unsigned char
event_admit_flags(const PsForkMetaEvent *ev)
{
	unsigned char flags = ev->flags;

	if (ev->kind == PS_FORKMETA_SET || ev->kind == PS_FORKMETA_DEAD)
		flags |= PS_FORKMETA_F_META;
	return flags;
}

static void
retain_horizon(const PsForkMetaEvent *events, uint32_t nitems,
			   PsForkMetaFence horizon, unsigned char *keep)
{
	uint32_t latest_def = 0;
	uint32_t largest_grow = 0;
	uint32_t envelope = UINT32_MAX;
	int have_def = 0;
	int have_grow = 0;

	/*
	 * Walking newest to oldest, every visible definitive event whose size is
	 * a new strict minimum is the newest death of the blocks at or above that
	 * size: the latest one fixes the size at the horizon, the smallest fences
	 * inherited blocks, and each one in between is the zero-page base that
	 * WAL-index compaction and single-page redo use for the blocks it killed
	 * last.  An equal size does not start a new death, so the newest of equal
	 * sizes is the one kept and truncate churn at the same sizes adds nothing.
	 */
	for (uint32_t i = nitems; i > 0; i--)
	{
		uint32_t size;

		if (!event_visible(&events[i - 1], horizon))
			continue;
		if (events[i - 1].kind != PS_FORKMETA_SET &&
			events[i - 1].kind != PS_FORKMETA_DEAD)
			continue;
		size = events[i - 1].kind == PS_FORKMETA_DEAD ? 0 :
			events[i - 1].nblocks;
		if (!have_def)
		{
			latest_def = i - 1;
			have_def = 1;
		}
		if (size < envelope)
		{
			keep[i - 1] = 1;
			envelope = size;
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
ps_forkmeta_prune_plan_required(const PsForkMetaEvent *events,
								uint32_t nitems, PsForkMetaFence cutoff,
								const PsForkMetaFence *fences,
								uint32_t nfences,
								const unsigned char *required,
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
		if (!event_visible(&events[i], cutoff) ||
			(required != NULL && required[i]))
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

int
ps_forkmeta_prune_plan(const PsForkMetaEvent *events, uint32_t nitems,
					   PsForkMetaFence cutoff,
					   const PsForkMetaFence *fences, uint32_t nfences,
					   unsigned char *keep)
{
	return ps_forkmeta_prune_plan_required(events, nitems, cutoff, fences,
										   nfences, NULL, keep);
}

/* --- P2: the full-admissibility planner (design doc S3.7) --- */

static void
retain_horizon_capped(const PsForkMetaEvent *events, uint32_t nitems,
					  const PsAdmitCap *cap, uint64_t B, int has_B,
					  unsigned char *keep)
{
	uint32_t latest_def = 0;
	uint32_t largest_grow = 0;
	uint32_t envelope = UINT32_MAX;
	int have_def = 0;
	int have_grow = 0;

	for (uint32_t i = nitems; i > 0; i--)
	{
		uint32_t idx = i - 1;
		uint32_t size;

		if (ps_event_hidden(events[idx].lsn, events[idx].admission_seq,
							event_admit_flags(&events[idx]), cap, B,
							has_B != 0))
			continue;
		if (events[idx].kind != PS_FORKMETA_SET &&
			events[idx].kind != PS_FORKMETA_DEAD)
			continue;
		size = events[idx].kind == PS_FORKMETA_DEAD ? 0 : events[idx].nblocks;
		if (!have_def)
		{
			latest_def = idx;
			have_def = 1;
		}
		if (size < envelope)
		{
			keep[idx] = 1;
			envelope = size;
		}
	}
	for (uint32_t i = have_def ? latest_def + 1 : 0; i < nitems; i++)
	{
		if (!ps_event_hidden(events[i].lsn, events[i].admission_seq,
							 event_admit_flags(&events[i]), cap, B,
							 has_B != 0) &&
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

/*
 * S3.7 item 2: closure.  Runs once, over steps 1's mask folds' kept set;
 * not transitive over its own additions (matching the page planner's S3.5
 * item 3 closure, pagestore_prune.c).
 */
static void
apply_forkmeta_closure(const PsForkMetaEvent *events, uint32_t nitems,
					   uint64_t B, int has_B, unsigned char *keep)
{
	for (uint32_t i = 0; i < nitems; i++)
	{
		unsigned char aflags;

		if (!keep[i])
			continue;
		aflags = event_admit_flags(&events[i]);
		if (!(aflags & PS_FORKMETA_F_META) || (aflags & PS_FORKMETA_F_META_FIRST))
			continue;
		if (has_B && events[i].lsn <= B)
			continue;			/* inherited range: no escape (S1.5) */
		for (uint32_t j = 0; j < nitems; j++)
		{
			unsigned char jflags = event_admit_flags(&events[j]);

			if (events[j].lsn == events[i].lsn &&
				(jflags & PS_FORKMETA_F_META) &&
				(jflags & PS_FORKMETA_F_META_FIRST))
			{
				keep[j] = 1;
				break;
			}
		}
	}
	for (uint32_t i = 0; i < nitems; i++)
	{
		unsigned char aflags = event_admit_flags(&events[i]);

		if (keep[i] || !(aflags & PS_FORKMETA_F_UNSTAMPED))
			continue;
		for (uint32_t j = 0; j < nitems; j++)
			if (j != i && keep[j] && events[j].lsn == events[i].lsn &&
				events[j].admission_seq != 0 &&
				events[j].admission_seq > events[i].admission_seq)
			{
				keep[i] = 1;
				break;
			}
	}
}

int
ps_forkmeta_prune_plan_capped(const PsForkMetaEvent *events, uint32_t nitems,
							  PsForkMetaFence cutoff,
							  const PsForkMetaViewFence *fences,
							  uint32_t nfences, uint64_t inherited_below,
							  int has_inherited_below,
							  const unsigned char *required,
							  unsigned char *keep)
{
	int kept = 0;
	uint64_t last_nonzero_seq = 0;
	PsAdmitCap cutoff_cap;

	if (cutoff.lsn == 0 || cutoff.admission_seq == 0 ||
		nitems > (uint32_t) INT_MAX ||
		(nitems != 0 && (events == NULL || keep == NULL)) ||
		(nfences != 0 && fences == NULL))
		return -1;
	/* The cutoff itself always stays positional (S = infinity), exactly
	 * like ps_page_prune_plan_capped()'s floor (design doc S3.5 item 1's
	 * analog for forkmeta). */
	cutoff_cap.lsn = cutoff.lsn;
	cutoff_cap.seq = PS_ADM_SEQ_UNBOUNDED;
	cutoff_cap.strict_seq = cutoff.admission_seq;
	for (uint32_t i = 0; i < nfences; i++)
	{
		if (fences[i].lsn == 0 || fences[i].lsn > cutoff.lsn)
			return -1;
		if (fences[i].lsn == cutoff.lsn &&
			(fences[i].strict_seq == PS_FORKMETA_SEQ_UNBOUNDED ||
			 fences[i].strict_seq > cutoff.admission_seq))
			return -1;
	}
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
		if (ps_event_hidden(events[i].lsn, events[i].admission_seq,
							event_admit_flags(&events[i]), &cutoff_cap,
							inherited_below, has_inherited_below != 0) ||
			(required != NULL && required[i]))
			keep[i] = 1;
	}
	retain_horizon_capped(events, nitems, &cutoff_cap, inherited_below,
						  has_inherited_below, keep);
	for (uint32_t f = 0; f < nfences; f++)
	{
		PsAdmitCap ac;

		ac.lsn = fences[f].lsn;
		ac.seq = fences[f].seq;
		ac.strict_seq = fences[f].strict_seq;
		retain_horizon_capped(events, nitems, &ac, inherited_below,
							  has_inherited_below, keep);
	}
	apply_forkmeta_closure(events, nitems, inherited_below,
						   has_inherited_below, keep);
	for (uint32_t i = 0; i < nitems; i++)
		if (keep[i])
			kept++;
	return kept;
}

uint32_t
ps_forkmeta_derive_fences(const PsForkMetaViewFence *views, uint32_t nviews,
						  PsForkMetaFence cutoff, PsForkMetaViewFence *out)
{
	uint32_t n = 0;

	for (uint32_t i = 0; i < nviews; i++)
	{
		if (views[i].lsn < cutoff.lsn)
			continue;			/* already an ordinary in-domain fence */
		if (views[i].seq == PS_FORKMETA_SEQ_UNBOUNDED)
			continue;			/* positional: nothing to protect at cutoff */
		if (views[i].seq >= cutoff.admission_seq)
			continue;			/* sees everything the cutoff folds anyway */
		out[n].lsn = cutoff.lsn;
		out[n].seq = views[i].seq;
		out[n].strict_seq = cutoff.admission_seq;
		n++;
	}
	return n;
}
