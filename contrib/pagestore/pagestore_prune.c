#include <stddef.h>
#include <string.h>

#include "pagestore_prune.h"

int
ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
				   PsPruneFence floor, const PsPruneFence *fences,
				   uint32_t nfences, unsigned char *keep)
{
	int		base = -1;
	int		kept = 0;

	if ((n != 0 && (versions == NULL || keep == NULL)) ||
		(nfences != 0 && fences == NULL))
		return -1;
	if (n == 0)
		return 0;
	memset(keep, 0, n);
	for (uint32_t i = 1; i < n; i++)
		if (versions[i].lsn < versions[i - 1].lsn ||
			(versions[i].lsn == versions[i - 1].lsn &&
			 versions[i].admission_seq < versions[i - 1].admission_seq))
			return -1;

	if (floor.lsn == 0 || floor.admission_seq == 0)
		return -1;

	for (uint32_t i = 0; i < n; i++)
	{
		/* An exact tuple has one authoritative value: the last append.  This
		 * remains true above the prune frontier, where a future fence at this
		 * tuple also resolves to that last value. */
		if (i + 1 < n && versions[i].lsn == versions[i + 1].lsn &&
			versions[i].admission_seq == versions[i + 1].admission_seq)
			continue;
		if (versions[i].lsn <= floor.lsn &&
			(floor.admission_seq == 0 || versions[i].admission_seq == 0 ||
			 versions[i].admission_seq <= floor.admission_seq))
			base = (int) i;
		/* Every distinct tuple outside the admitted frontier remains a legal
		 * future fence. */
		else
		{
			keep[i] = 1;
			kept++;
		}
	}
	/* The operational floor retains one current base.  Reader admission fences
	 * and structural branch fences are discrete requirements and retain only
	 * the version visible at each exact fence. */
	if (base >= 0 && !keep[base])
	{
		keep[base] = 1;
		kept++;
	}
	/* A later fence can move forward in LSN while lowering its admission
	 * sequence.  Preserve the reverse record-low staircase below the current
	 * base so that each such legal fence still has a visible version. */
	if (base >= 0)
	{
		uint64_t lowest_seq = versions[base].admission_seq;

		for (int i = base - 1; i >= 0; i--)
		{
			if (i + 1 < n && versions[i].lsn == versions[i + 1].lsn &&
				versions[i].admission_seq == versions[i + 1].admission_seq)
				continue;
			if (versions[i].admission_seq == 0 ||
				(lowest_seq != 0 && versions[i].admission_seq < lowest_seq))
			{
				if (!keep[i])
				{
					keep[i] = 1;
					kept++;
				}
				lowest_seq = versions[i].admission_seq;
			}
		}
	}
	for (uint32_t f = 0; f < nfences; f++)
	{
		int visible = -1;

		for (uint32_t i = 0; i < n; i++)
			if (versions[i].lsn <= fences[f].lsn &&
				(fences[f].admission_seq == 0 ||
				 versions[i].admission_seq == 0 ||
				 versions[i].admission_seq <= fences[f].admission_seq))
				visible = (int) i;
			else if (versions[i].lsn > fences[f].lsn)
				break;
		if (visible >= 0 && !keep[visible])
		{
			keep[visible] = 1;
			kept++;
		}
	}
	return kept;
}
