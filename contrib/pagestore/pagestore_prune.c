#include <stddef.h>
#include <string.h>

#include "pagestore_prune.h"

int
ps_page_prune_plan(const PsPruneVersion *versions, uint32_t n,
				   uint64_t floor, unsigned char *keep)
{
	uint64_t	base_lsn = 0;
	int		have_base = 0;
	int		kept = 0;

	if ((n != 0 && (versions == NULL || keep == NULL)))
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
	{
		keep[n - 1] = 1;
		return 1;
	}

	for (uint32_t i = 0; i < n; i++)
	{
		if (versions[i].lsn < floor)
		{
			base_lsn = versions[i].lsn;
			have_base = 1;
		}
		else
		{
			keep[i] = 1;
			kept++;
		}
	}
	/* Retention pins carry an LSN but not an admission fence.  A reader at the
	 * floor can therefore be fenced between any same-LSN rewrites of its base
	 * image.  Preserve every admission variant at the greatest LSN below the
	 * floor; keeping only the last one could make an earlier fenced view vanish. */
	if (have_base)
		for (uint32_t i = 0; i < n && versions[i].lsn <= base_lsn; i++)
			if (versions[i].lsn == base_lsn)
			{
				keep[i] = 1;
				kept++;
			}
	return kept;
}
