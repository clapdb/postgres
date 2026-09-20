#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_walidx_prune.h"

static uint64_t
newest_base_at(const uint64_t *bases, uint32_t nbases, uint64_t horizon,
			   int *found)
{
	uint64_t	best = 0;

	*found = 0;
	for (uint32_t j = 0; j < nbases; j++)
		if (bases[j] <= horizon)
		{
			*found = 1;
			best = bases[j];
		}
	return best;
}

static int
retain_chain(const PsWalIdxPruneItem *items, uint32_t n,
			 const uint64_t *bases, uint32_t nbases,
			 const uint64_t *deaths, uint32_t ndeaths, uint64_t horizon,
			 unsigned char *keep)
{
	int last = -1;
	int base = -1;
	int have_page_base = 0;
	uint64_t page_base = 0;
	int have_death;
	uint64_t death;

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
	/* The newest stored image, or absence, visible at this horizon. */
	page_base = newest_base_at(bases, nbases, horizon, &have_page_base);
	death = newest_base_at(deaths, ndeaths, horizon, &have_death);
	if (have_death && (!have_page_base || death > page_base))
	{
		page_base = death;
		have_page_base = 1;
	}
	/* Every visible record is covered: no chain. */
	if (have_page_base && items[last].end_lsn <= page_base)
		return 0;
	for (int i = last; i >= 0; i--)
		if (items[i].fpi)
		{
			base = i;
			break;
		}
	if (have_page_base)
	{
		int start = -1;

		/* The chain starts at the first record the base does not cover, or
		 * at a newer FPI when one exists. */
		for (int i = 0; i <= last; i++)
			if (items[i].known && items[i].end_lsn > page_base)
			{
				start = i;
				break;
			}
		if (start < 0)
			return -1;
		if (base > start)
			start = base;
		for (int i = start; i <= last; i++)
			keep[i] = 1;
		return 0;
	}
	if (base < 0)
		return -1;
	for (int i = base; i <= last; i++)
		keep[i] = 1;
	return 0;
}

static int
bases_valid(const uint64_t *bases, uint32_t nbases)
{
	if (nbases != 0 && bases == NULL)
		return 0;
	for (uint32_t j = 0; j < nbases; j++)
		if (bases[j] == 0 || (j != 0 && bases[j] <= bases[j - 1]))
			return 0;
	return 1;
}

int
ps_walidx_prune_plan_bases(const PsWalIdxPruneItem *items, uint32_t n,
						   const uint64_t *bases, uint32_t nbases,
						   const uint64_t *deaths, uint32_t ndeaths,
						   uint64_t cutoff, int cutoff_bases_ok,
						   const uint64_t *horizons, uint32_t nhorizons,
						   const unsigned char *horizon_bases_ok,
						   unsigned char *keep)
{
	int kept = 0;
	uint64_t last_known_end = 0;

	if ((n != 0 && (items == NULL || keep == NULL)) ||
		(nhorizons != 0 && (horizons == NULL || horizon_bases_ok == NULL)) ||
		!bases_valid(bases, nbases) || !bases_valid(deaths, ndeaths) ||
		cutoff == 0)
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
	if (retain_chain(items, n, cutoff_bases_ok ? bases : NULL,
					 cutoff_bases_ok ? nbases : 0, deaths, ndeaths, cutoff,
					 keep) != 0)
		return -1;
	for (uint32_t i = 0; i < nhorizons; i++)
	{
		uint64_t horizon = horizons[i] < cutoff ? horizons[i] : cutoff;

		if (retain_chain(items, n, horizon_bases_ok[i] ? bases : NULL,
						 horizon_bases_ok[i] ? nbases : 0, deaths, ndeaths,
						 horizon, keep) != 0)
			return -1;
	}
	for (uint32_t i = 0; i < n; i++)
		if (keep[i])
			kept++;
	return kept;
}

int
ps_walidx_prune_plan(const PsWalIdxPruneItem *items, uint32_t n,
					uint64_t cutoff, const uint64_t *horizons,
					uint32_t nhorizons, unsigned char *keep)
{
	unsigned char ok[64];
	unsigned char *flags = ok;
	int rc;

	if (nhorizons > sizeof(ok))
	{
		flags = malloc(nhorizons);
		if (flags == NULL)
			return -1;
	}
	memset(flags, 0, nhorizons != 0 ? nhorizons : 1);
	rc = ps_walidx_prune_plan_bases(items, n, NULL, 0, NULL, 0, cutoff, 0,
									horizons, nhorizons,
									nhorizons != 0 ? flags : NULL, keep);
	if (flags != ok)
		free(flags);
	return rc;
}
