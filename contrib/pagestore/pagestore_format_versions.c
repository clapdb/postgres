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

#include "pagestore_artifact_format.h"
#include "pagestore_format.h"
#include "pagestore_ipc.h"
#include "pagestore_layer.h"
#include "pagestore_spdk_super.h"
#include "pagestore_wal_segment.h"

typedef size_t (*IdentityProvider) (const PsFormatIdentity **out);

/* Delta layers have a reader and writer in pagestore_layer.c but no
 * maintenance path produces them yet, so their footer is not an identity a
 * fixture can carry; add it here when a writer lands. */
static const PsFormatIdentity header_identities[] = {
	{"image_layer", "layer_<shard>_<id> footer", PS_IMG_MAGIC, PS_IMG_VERSION},
	{"wal_segment", "wal_segments_<tl>/walv1_* header", PS_WAL_SEGMENT_MAGIC,
	 PS_WAL_SEGMENT_VERSION},
	{"wal_segment", "wal_segments_<tl>/walv1_* header (accepted legacy, no payload identity)",
	 PS_WAL_SEGMENT_MAGIC, PS_WAL_SEGMENT_LEGACY_VERSION},
	{"control", "control object admission fence block", PS_ADMISSION_FENCE_MAGIC,
	 PS_ADMISSION_FENCE_VERSION},
	/* the backend's own object payloads (pagestore_artifact_format.h): raw
	 * values with an identity trailer, and the headed control blocks and
	 * reader snapshot objects */
	{"control", "control object block 1 redo note (value at 0, identity trailer)",
	 PS_REDO_NOTE_MAGIC, PS_REDO_NOTE_VERSION},
	{"control", "control object block 3 materializer marker",
	 PS_MATERIALIZER_MARKER_MAGIC, PS_MATERIALIZER_MARKER_VERSION},
	{"control", "control object block 4 materializer release",
	 PS_MATERIALIZER_RELEASE_MAGIC, PS_MATERIALIZER_RELEASE_VERSION},
	{"control", "control object block 5 writer checkpoint",
	 PS_WRITER_CHECKPOINT_MAGIC, PS_WRITER_CHECKPOINT_VERSION},
	{"slru_mirror", "watermark object (value at 0, identity trailer)",
	 PS_SLRU_WATERMARK_MAGIC, PS_SLRU_WATERMARK_VERSION},
	{"slru_mirror", "truncation tombstone object (value at 0, identity trailer)",
	 PS_SLRU_TOMBSTONE_MAGIC, PS_SLRU_TOMBSTONE_VERSION},
	{"reader_snapshot", "object 0 manifest", PS_READER_SNAPSHOT_MANIFEST_MAGIC,
	 PS_READER_SNAPSHOT_MANIFEST_FORMAT},
	{"reader_snapshot", "object 1 running-transaction snapshot",
	 PS_READER_SNAPSHOT_MAGIC, PS_READER_SNAPSHOT_FORMAT},
	{"reader_snapshot", "object 2 ready record", PS_READER_SNAPSHOT_MAGIC,
	 PS_READER_SNAPSHOT_FORMAT},
	{"reader_snapshot", "object 3 relation map", PS_READER_RELMAP_MAGIC,
	 PS_READER_RELMAP_FORMAT},
	{"reader_snapshot", "object 4 database barrier",
	 PS_READER_DATABASE_BARRIER_MAGIC, PS_READER_DATABASE_BARRIER_FORMAT},
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
		ps_storage_posix_format_identities, ps_storage_spdk_format_identities,
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
		printf("  {\"family\": \"%s\", \"artifact\": \"%s\", \"magic\": \"0x%08llx\", "
			   "\"version\": %u}%s\n", all[i].family, all[i].artifact,
			   (unsigned long long) all[i].magic, all[i].version,
			   i + 1 < n ? "," : "");
	printf("]\n");
	return 0;
}
