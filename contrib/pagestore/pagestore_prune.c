#include <stddef.h>
#include <string.h>

#include "pagestore_prune.h"

int
ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
				   uint64_t floor, const PsPruneFence *fences,
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

	if (floor == 0)
		return -1;

	for (uint32_t i = 0; i < n; i++)
	{
		if (versions[i].lsn < floor)
			base = (int) i;
		/* An unfenced operational read sees only the last admission at one
		 * LSN.  Exact reader fences below retain any older variant they need. */
		else if (i + 1 == n || versions[i + 1].lsn != versions[i].lsn)
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
