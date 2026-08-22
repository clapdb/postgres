#include <stddef.h>
#include <string.h>

#include "pagestore_walidx_prune.h"

static int
retain_chain(const PsWalIdxPruneItem *items, uint32_t n, uint64_t horizon,
			 unsigned char *keep)
{
	int last = -1;
	int base = -1;

	for (uint32_t i = 0; i < n; i++)
	{
		if (!items[i].known)
			continue;
		if (items[i].end_lsn <= horizon)
			last = (int) i;
		else if (items[i].lsn > horizon)
			break;
	}
	/* The page did not yet have a visible WAL-index record. */
	if (last < 0)
		return 0;
	for (int i = last; i >= 0; i--)
		if (items[i].fpi)
		{
			base = i;
			break;
		}
	if (base < 0)
		return -1;
	for (int i = base; i <= last; i++)
		keep[i] = 1;
	return 0;
}

int
ps_walidx_prune_plan(const PsWalIdxPruneItem *items, uint32_t n,
					uint64_t cutoff, const uint64_t *horizons,
					uint32_t nhorizons, unsigned char *keep)
{
	int kept = 0;
	uint64_t last_known_end = 0;

	if ((n != 0 && (items == NULL || keep == NULL)) ||
		(nhorizons != 0 && horizons == NULL) || cutoff == 0)
		return -1;
	if (n == 0)
		return 0;
	memset(keep, 0, n);
	for (uint32_t i = 0; i < n; i++)
	{
		if (items[i].known > 1 || items[i].fpi > 1 ||
			(i != 0 && items[i].lsn <= items[i - 1].lsn) ||
			(items[i].known && items[i].end_lsn <= items[i].lsn) ||
			(items[i].known && last_known_end != 0 &&
			 items[i].end_lsn <= last_known_end) ||
			(!items[i].known && items[i].end_lsn != 0) ||
			(items[i].fpi && !items[i].known))
			return -1;
		if (items[i].known)
			last_known_end = items[i].end_lsn;
		/* Legacy entries cannot establish visibility or a replacement base.
		 * Once the operational cutoff reaches one, leave this page intact. */
		if (!items[i].known && items[i].lsn <= cutoff)
			return -1;
		if ((!items[i].known && items[i].lsn > cutoff) ||
			(items[i].known && items[i].end_lsn > cutoff))
			keep[i] = 1;
	}
	if (retain_chain(items, n, cutoff, keep) != 0)
		return -1;
	for (uint32_t i = 0; i < nhorizons; i++)
	{
		uint64_t horizon = horizons[i] < cutoff ? horizons[i] : cutoff;

		if (retain_chain(items, n, horizon, keep) != 0)
			return -1;
	}
	for (uint32_t i = 0; i < n; i++)
		if (keep[i])
			kept++;
	return kept;
}
