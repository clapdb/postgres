/*-------------------------------------------------------------------------
 *
 * pagestore_artifact_format.h
 *	  The page-store-defined payloads a PostgreSQL compute stores as objects
 *	  in the daemon, and the files it leaves in a data directory: their
 *	  layouts and identities, in one freestanding header shared by the
 *	  backend that writes and loads them, the daemon that reads the few it
 *	  interprets, the fixture workloads that seed them, and
 *	  pagestore_format_versions, which reports their identities.
 *
 * D5 rule 4: these are page-store envelopes -- the store wraps them like any
 * page, but their bytes are ours, not PostgreSQL's -- so each carries an
 * identity a fixture can pin.  Three of them were bare values (a
 * checkpoint-redo LSN, a watermark, a truncation cutoff) that consumers
 * memcpy out of byte 0 of the object; they keep that value at byte 0, where
 * every existing reader finds it, and gain an identity trailer at byte 8 --
 * magic and version -- which a legacy object leaves zero.  The others already
 * carried a magic and version; their layouts are restated here so a
 * freestanding client can write and check them.
 *
 * Every field is stored in the writing host's byte order, as the backend's
 * structs are; the store does not move objects between byte orders.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_ARTIFACT_FORMAT_H
#define PAGESTORE_ARTIFACT_FORMAT_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ---- control object (PS_KLASS_CONTROL, object 0) -------------------- */

/* block 0: the ControlFileData image (PostgreSQL's; not versioned here) */
#define PS_CONTROL_IMAGE_BLOCK		0u

/*
 * block 1: the retention "floor note" -- the checkpoint redo of the image
 * shipped at the same version.  Value at 0; identity trailer at 8.
 */
#define PS_REDO_NOTE_BLOCK			1u
#define PS_REDO_NOTE_MAGIC			0x4e525350u	/* "PSRN" */
#define PS_REDO_NOTE_VERSION		1u

/* block 2: the admission fence (PsAdmissionFence in pagestore_ipc.h) */
#define PS_ADMISSION_FENCE_BLOCK		2u
/* blocks 0-2 are written as a same-version group; the daemon retains them
 * by the image block's plan, and the higher blocks each by their own */
#define PS_CONTROL_PAIRED_BLOCKS		3u

/* block 3: the materializer's durable materialized-through marker */
#define PS_MATERIALIZER_MARKER_BLOCK	3u
#define PS_MATERIALIZER_MARKER_MAGIC	0x50534d57u	/* "PSMW" */
#define PS_MATERIALIZER_MARKER_VERSION	2u

typedef struct PsMaterializerMarkerFormat
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	timeline;
	uint32_t	pad;
	uint64_t	materialized_lsn;
	uint64_t	materialized_lsn_complement;
} PsMaterializerMarkerFormat;

/* block 4: the materializer's release of a materialized checkpoint */
#define PS_MATERIALIZER_RELEASE_BLOCK	4u
#define PS_MATERIALIZER_RELEASE_MAGIC	0x50534d52u	/* "PSMR" */
#define PS_MATERIALIZER_RELEASE_VERSION	2u

typedef struct PsMaterializerReleaseFormat
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	timeline;
	uint32_t	pad;
	uint64_t	materialized_lsn;
	uint64_t	materialized_lsn_complement;
	uint64_t	checkpoint_lsn;
	uint64_t	checkpoint_lsn_complement;
} PsMaterializerReleaseFormat;

/* block 5: the writer's declared checkpoint for branch preparation */
#define PS_WRITER_CHECKPOINT_BLOCK		5u
#define PS_WRITER_CHECKPOINT_MAGIC		0x50535743u	/* "PSWC" */
#define PS_WRITER_CHECKPOINT_VERSION	1u

typedef struct PsWriterCheckpointFormat
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	timeline;
	uint32_t	pad;
	uint64_t	checkpoint_lsn;
	uint64_t	checkpoint_lsn_complement;
} PsWriterCheckpointFormat;

/* ---- SLRU mirror objects ---------------------------------------------- */

/* PS_KLASS_SLRU_WM, object 0, block 0: the mirror's visibility watermark
 * (a uint64 LSN at 0); PS_KLASS_SLRU_TOMB, per SLRU object, block 0: the
 * truncation cutoff page (an int64 at 0).  Identity trailer at 8. */
#define PS_SLRU_WATERMARK_MAGIC		0x4d575350u	/* "PSWM" */
#define PS_SLRU_WATERMARK_VERSION	1u
#define PS_SLRU_TOMBSTONE_MAGIC		0x42545350u	/* "PSTB" */
#define PS_SLRU_TOMBSTONE_VERSION	1u

/*
 * The object an SLRU's mirror pages and tombstone live under is derived from
 * its directory name (FNV-1a), so the keys a reader asks for are part of the
 * persisted format too; a fixture seeds a tombstone at the id of "pg_xact".
 */
static inline uint32_t
ps_slru_object_id(const char *dir)
{
	uint32_t	h = 2166136261u;

	for (const unsigned char *p = (const unsigned char *) dir; *p != '\0'; p++)
	{
		h ^= *p;
		h *= 16777619u;
	}
	return h;
}

/* ---- reader snapshot objects (PS_KLASS_READER_SNAPSHOT) ---------------- */

#define PS_READER_SNAPSHOT_MANIFEST_OBJECT	0u
#define PS_READER_SNAPSHOT_DATA_OBJECT		1u
#define PS_READER_SNAPSHOT_READY_OBJECT		2u
#define PS_READER_RELMAP_OBJECT				3u
#define PS_READER_DATABASE_BARRIER_OBJECT	4u

#define PS_READER_SNAPSHOT_MAGIC			0x50535253u	/* "PSRS" */
#define PS_READER_SNAPSHOT_FORMAT			1u
#define PS_READER_SNAPSHOT_MANIFEST_MAGIC	0x5053524du	/* "PSRM" */
#define PS_READER_SNAPSHOT_MANIFEST_FORMAT	2u
#define PS_READER_RELMAP_MAGIC				0x5053524cu	/* "PSRL" */
#define PS_READER_RELMAP_FORMAT				1u
#define PS_READER_DATABASE_BARRIER_MAGIC	0x50535242u	/* "PSRB" */
#define PS_READER_DATABASE_BARRIER_FORMAT	3u

/* the snapshot header (object 1, and the ready record of object 2); the
 * data object continues with count TransactionIds (uint32) */
typedef struct PsReaderSnapshotHeaderFormat
{
	uint64_t	read_lsn;
	uint32_t	magic;
	uint32_t	format;
	uint32_t	timeline;
	uint32_t	count;
	uint32_t	xmin;
	uint32_t	xmax;
	uint32_t	crc;			/* CRC-32C of the header with crc zero, then the xids */
	uint32_t	reserved;
} PsReaderSnapshotHeaderFormat;

typedef struct PsReaderSnapshotReadyFormat
{
	PsReaderSnapshotHeaderFormat header;
	uint32_t	block_count;
	uint32_t	reserved;
	uint32_t	crc;			/* CRC-32C of the bytes before it */
} PsReaderSnapshotReadyFormat;

typedef struct PsReaderSnapshotManifestFormat
{
	uint64_t	read_lsn;
	uint64_t	artifact_size;
	uint32_t	magic;
	uint32_t	format;
	uint32_t	timeline;
	uint32_t	block_count;
	uint32_t	artifact_crc;
	uint32_t	global_relmap_crc;
	uint32_t	local_relmap_crc;
	uint32_t	crc;			/* CRC-32C of the bytes before it */
} PsReaderSnapshotManifestFormat;

typedef struct PsReaderRelmapFormat
{
	uint32_t	magic;
	uint32_t	format;
	uint32_t	dbid;
	uint32_t	tsid;
	uint32_t	size;
	uint32_t	data_crc;		/* CRC-32C of the size bytes of data */
	uint32_t	crc;			/* CRC-32C of the bytes before it */
	/* followed by size bytes: a pg_filenode.map, PostgreSQL's */
} PsReaderRelmapFormat;

/* the barrier header is followed by database_count entries, spanning
 * block_count blocks; the CRC covers the header before it and the entries */
typedef struct PsReaderDatabaseBarrierFormat
{
	uint64_t	read_lsn;
	uint32_t	magic;
	uint32_t	format;
	uint32_t	timeline;
	uint32_t	database_count;
	uint32_t	block_count;
	uint32_t	crc;			/* CRC-32C of the bytes before it, then the entries */
	uint32_t	reserved;
} PsReaderDatabaseBarrierFormat;

typedef struct PsReaderDatabaseEntryFormat
{
	uint32_t	database_oid;
	uint32_t	tablespace_oid;
} PsReaderDatabaseEntryFormat;

/* ---- data-directory artifacts (D5 rule 4) ------------------------------- */

/*
 * The files a compute leaves in a data directory for another compute -- a
 * prepared branch or reader, or itself after a restart -- to load.  Text
 * manifests carry their format in a "format" member; binary artifacts a
 * magic and format at a fixed offset; the two raw-value markers a value at
 * 0 and an identity trailer at 8 like the raw store objects above (a legacy
 * marker is 8 bytes long and carries none).
 */

/* prepared branch: a JSON manifest and the CRC-bound bootstrap bundle */
#define PS_BRANCH_MANIFEST_FILE			"pagestore_branch.manifest"
#define PS_BRANCH_MANIFEST_FORMAT		2u	/* format 1 accepted, legacy */
#define PS_BRANCH_BOOTSTRAP_FILE		"pagestore_branch.bootstrap"
#define PS_BRANCH_BOOTSTRAP_MAGIC		0x50534242u	/* "PSBB" */
#define PS_BRANCH_BOOTSTRAP_FORMAT		1u
#define PS_BRANCH_BOOTSTRAP_HAS_USER_TABLESPACES 0x00000001u

/* the header; map_count relation maps follow, each a map header and the
 * pg_filenode.map bytes (PostgreSQL's); crc is CRC-32C of the whole
 * artifact with crc zero */
typedef struct PsBranchBootstrapHeaderFormat
{
	uint64_t	checkpoint_redo;
	uint64_t	recovery_lsn;
	uint64_t	fork_lsn;
	uint64_t	system_identifier;
	uint64_t	artifact_size;
	uint32_t	magic;
	uint32_t	format;
	uint32_t	new_timeline;
	uint32_t	parent_timeline;
	uint32_t	map_count;
	uint32_t	flags;
	uint32_t	crc;
	uint32_t	manifest_crc;	/* CRC-32C of the manifest text it binds */
} PsBranchBootstrapHeaderFormat;

typedef struct PsBranchBootstrapMapHeaderFormat
{
	uint32_t	database_oid;
	uint32_t	size;
} PsBranchBootstrapMapHeaderFormat;

/* prepared reader: a JSON manifest, the snapshot file (a
 * PsReaderSnapshotHeaderFormat and its xids, as the store object), and the
 * catalog provenance stamp on the data directory the catalog was copied to */
#define PS_READER_MANIFEST_FILE			"pagestore_reader.manifest"
#define PS_READER_MANIFEST_FORMAT		3u	/* format 2 accepted, legacy */
#define PS_READER_SNAPSHOT_FILE			"pagestore_reader.snapshot"
#define PS_READER_CATALOG_FILE			"pagestore_reader.catalog"
#define PS_READER_CATALOG_MAGIC			0x50534350u	/* "PSCP" */
#define PS_READER_CATALOG_FORMAT		1u

typedef struct PsReaderCatalogProvenanceFormat
{
	uint64_t	read_lsn;
	uint64_t	system_identifier;
	uint32_t	magic;
	uint32_t	format;
	uint32_t	timeline;
	uint32_t	reserved;
	uint32_t	crc;			/* CRC-32C of the bytes before it */
	uint32_t	padding;
} PsReaderCatalogProvenanceFormat;

/* the reader's relation-map intent marker, in global/ and each database
 * directory while a map is published but not yet adopted: the horizon
 * (a uint64 LSN) at 0, identity trailer at 8 */
#define PS_READER_MAP_PENDING_FILE		".pagestore-reader-map-pending"
#define PS_READER_MAP_PENDING_MAGIC		0x504d5350u	/* "PSMP" */
#define PS_READER_MAP_PENDING_VERSION	1u

/* the SLRU mirror's continuity markers at the data directory root: the
 * primed marker, stamped with the redo (a uint64 LSN at 0, identity trailer
 * at 8; an older marker is 8 bytes or empty) of the newest checkpoint the
 * mirror durably shipped, and the debt marker, whose presence is its
 * meaning */
#define PS_SLRU_PRIMED_FILE_NAME		"pagestore.slru_mirror_primed"
#define PS_SLRU_PRIMED_MAGIC			0x4d505350u	/* "PSPM" */
#define PS_SLRU_PRIMED_VERSION			1u
#define PS_SLRU_DEBT_FILE_NAME			"pagestore.slru_mirror_debt"

/* the writer-to-reader handoff token (a bytea a SQL caller carries; not a
 * file, but an envelope another build must recognize or refuse) */
#define PS_READER_HANDOFF_MAGIC			0x50534854u	/* "PSHT" */
#define PS_READER_HANDOFF_FORMAT		1u

typedef struct PsReaderHandoffTokenFormat
{
	uint32_t	magic;
	uint32_t	format;
	uint32_t	timeline;
	uint32_t	reserved;
	uint64_t	lsn;
} PsReaderHandoffTokenFormat;

/* ---- binding a producer's struct to a layout here ----------------------- */

/*
 * The backend keeps its own structs for these objects (PostgreSQL types,
 * flexible members); each is pinned field by field to the layout restated
 * here, so a change there that this header does not follow fails to
 * compile instead of shipping a new layout under the old identity.
 */
#define PS_ARTIFACT_LAYOUT_SIZE(prod, shared) \
	_Static_assert(sizeof(prod) == sizeof(shared), \
				   #prod " must match pagestore_artifact_format.h")
#define PS_ARTIFACT_LAYOUT_FIELD(prod, shared, field) \
	_Static_assert(offsetof(prod, field) == offsetof(shared, field) && \
				   sizeof(((prod *) 0)->field) == sizeof(((shared *) 0)->field), \
				   #prod "." #field " must match pagestore_artifact_format.h")

/* ---- the raw-value trailer ---------------------------------------------- */

#define PS_ARTIFACT_TRAILER_OFFSET 8u
/* a raw-value marker FILE: the value, then the trailer; a legacy one stops
 * at the value */
#define PS_RAW_MARKER_SIZE 16u
#define PS_RAW_MARKER_LEGACY_SIZE 8u

typedef struct PsArtifactTrailer
{
	uint32_t	magic;
	uint32_t	version;
} PsArtifactTrailer;

/* Stamp a raw-value object's identity after its value. */
static inline void
ps_artifact_trailer_set(unsigned char *page, uint32_t magic, uint32_t version)
{
	PsArtifactTrailer trailer;

	trailer.magic = magic;
	trailer.version = version;
	memcpy(page + PS_ARTIFACT_TRAILER_OFFSET, &trailer, sizeof(trailer));
}

/* Does the object carry exactly this identity?  (A fixture captured under
 * this build must; a reader also accepts the legacy zero trailer below.) */
static inline int
ps_artifact_trailer_is(const unsigned char *page, uint32_t magic,
					   uint32_t version)
{
	PsArtifactTrailer trailer;

	memcpy(&trailer, page + PS_ARTIFACT_TRAILER_OFFSET, sizeof(trailer));
	return trailer.magic == magic && trailer.version == version;
}

/*
 * Check a raw-value object's identity.  A zero trailer is a legacy object
 * that recorded none and is accepted; a trailer naming another format, or a
 * version this build does not know, is not.  Returns 0 when acceptable.
 */
static inline int
ps_artifact_trailer_check(const unsigned char *page, uint32_t magic,
						  uint32_t version)
{
	PsArtifactTrailer trailer;

	memcpy(&trailer, page + PS_ARTIFACT_TRAILER_OFFSET, sizeof(trailer));
	if (trailer.magic == 0 && trailer.version == 0)
		return 0;
	return ps_artifact_trailer_is(page, magic, version) ? 0 : -1;
}

/* ---- CRC-32C, as PostgreSQL's pg_crc32c computes it -------------------- */

/*
 * The reader objects are checksummed with PostgreSQL's CRC-32C (Castagnoli
 * polynomial, initial value and final inversion of 0xFFFFFFFF).  A
 * freestanding client that seeds them needs the same function; this is the
 * bytewise form of the algorithm, checked against pg_crc32c by the backend
 * test that links both.
 */
static inline uint32_t
ps_crc32c_update(uint32_t crc, const void *data, size_t len)
{
	const unsigned char *p = (const unsigned char *) data;

	for (size_t i = 0; i < len; i++)
	{
		crc ^= p[i];
		for (int bit = 0; bit < 8; bit++)
			crc = (crc >> 1) ^ (0x82f63b78u & (0u - (crc & 1u)));
	}
	return crc;
}

#define PS_CRC32C_INIT 0xffffffffu
#define PS_CRC32C_FIN(crc) ((crc) ^ 0xffffffffu)

#endif							/* PAGESTORE_ARTIFACT_FORMAT_H */
