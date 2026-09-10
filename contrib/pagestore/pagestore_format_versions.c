/*-------------------------------------------------------------------------
 *
 * pagestore_format_versions.c
 *	Print the compiled identities of every persisted pagestore format as
 *	JSON, for the persisted-format fixture check.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_format.h"
#include "pagestore_ipc.h"
#include "pagestore_layer.h"
#include "pagestore_wal_segment.h"

typedef size_t (*IdentityProvider) (const PsFormatIdentity **out);

/* Delta layers have a reader and writer in pagestore_layer.c but no
 * maintenance path produces them yet, so their footer is not an identity a
 * fixture can carry; add it here when a writer lands. */
static const PsFormatIdentity header_identities[] = {
	{"image_layer", "layer_<shard>_<id> footer", PS_IMG_MAGIC, PS_IMG_VERSION},
	{"wal_segment", "wal_segments_<tl>/walv1_* header", PS_WAL_SEGMENT_MAGIC,
	 PS_WAL_SEGMENT_VERSION},
	{"control", "control object admission fence block", PS_ADMISSION_FENCE_MAGIC,
	 PS_ADMISSION_FENCE_VERSION},
};

static int
compare(const void *left, const void *right)
{
	const PsFormatIdentity *a = left;
	const PsFormatIdentity *b = right;
	int			rc = strcmp(a->family, b->family);

	return rc != 0 ? rc : strcmp(a->artifact, b->artifact);
}

int
main(void)
{
	const IdentityProvider providers[] = {
		ps_manifest_format_identities, ps_retention_format_identities,
		ps_wal_store_format_identities, ps_forkmeta_snapshot_format_identities,
		ps_walidx_snapshot_format_identities, ps_core_format_identities,
	};
	PsFormatIdentity all[64];
	size_t		n = 0;

	for (size_t i = 0; i < sizeof(header_identities) / sizeof(header_identities[0]); i++)
		all[n++] = header_identities[i];
	for (size_t p = 0; p < sizeof(providers) / sizeof(providers[0]); p++)
	{
		const PsFormatIdentity *table = NULL;
		size_t		count = providers[p](&table);

		for (size_t i = 0; i < count; i++)
		{
			if (n >= sizeof(all) / sizeof(all[0]))
			{
				fprintf(stderr, "too many format identities\n");
				return 1;
			}
			all[n++] = table[i];
		}
	}
	qsort(all, n, sizeof(all[0]), compare);
	printf("[\n");
	for (size_t i = 0; i < n; i++)
		printf("  {\"family\": \"%s\", \"artifact\": \"%s\", \"magic\": \"0x%08x\", "
			   "\"version\": %u}%s\n", all[i].family, all[i].artifact,
			   all[i].magic, all[i].version, i + 1 < n ? "," : "");
	printf("]\n");
	return 0;
}
