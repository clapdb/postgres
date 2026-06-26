/*-------------------------------------------------------------------------
 *
 * pagestore_manifest.c
 *	  Append-only manifest event log for immutable pagestore layers.
 *
 * Phase 1 uses this as a durable skeleton only.  Later phases will add layer
 * creation, compaction, remote durability, local eviction, and GC state changes
 * through this same event stream.
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_manifest.h"

#define PS_MANIFEST_MAGIC	0x504d414e	/* "PMAN" */
#define PS_MANIFEST_VERSION 3	/* 3: per-record CRC (over the header + payload) */
#define PS_MANIFEST_VERSION_V2 2	/* legacy: no per-record CRC (16-byte header) */
#define PS_MANIFEST_FNV_INIT	2166136261u

typedef enum PsManifestEventType
{
	PS_MANIFEST_ADD_LAYER = 1,
	PS_MANIFEST_SET_REMOTE_DURABLE = 2,
	PS_MANIFEST_DROP_LOCAL = 3,
	PS_MANIFEST_MARK_DELETE = 4,
	PS_MANIFEST_REMOVE_LAYER = 5,
} PsManifestEventType;

typedef struct PsManifestRecord
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	type;
	uint32_t	len;
	uint32_t	crc;			/* FNV-1a over the header bytes above + the payload */
} PsManifestRecord;

typedef struct PsManifestLayerIdEvent
{
	uint64_t	layer_id;
	uint64_t	value;
} PsManifestLayerIdEvent;

typedef struct PsManifestKeyDisk
{
	uint32_t	spcOid;
	uint32_t	dbOid;
	uint32_t	relNumber;
	int32_t		forkNum;
	uint32_t	klass;			/* PsObjClass; manifest v2 */
} PsManifestKeyDisk;

typedef struct PsManifestLocationDisk
{
	uint32_t	tier;
	char		uri[PS_LAYER_URI_MAX];
	uint64_t	size;
	uint32_t	generation;
	uint8_t		available;
	uint8_t		pad[3];
} PsManifestLocationDisk;

typedef struct PsManifestLayerDisk
{
	uint64_t	layer_id;
	uint32_t	kind;
	uint32_t	timeline;
	PsManifestKeyDisk start_key;
	PsManifestKeyDisk end_key;
	uint32_t	start_block;
	uint32_t	end_block;
	uint64_t	lsn_start;
	uint64_t	lsn_end;
	uint32_t	location_count;
	PsManifestLocationDisk locations[PS_LAYER_MAX_LOCATIONS];
	uint64_t	created_at_lsn;
	uint64_t	remote_uploaded_lsn;
	uint8_t		remote_durable;
	uint8_t		local_pinned;
	uint8_t		deleting;
	uint8_t		pad;
} PsManifestLayerDisk;

static char manifest_path[4096];
static char manifest_dir[2048];
PsLayerMap ps_layer_map;

/*
 * Sticky once a record append fails: a short write or failed fsync may have left
 * a torn record at the tail, which replay can only recover if it stays the *last*
 * record.  So after any append error we refuse every further append for the life
 * of this process -- no later flush/compaction/GC record can land after the torn
 * tail and turn it into unrecoverable interior corruption.  Reset on (re)open,
 * where replay truncates the torn tail.  Layer data itself is not lost: the
 * segment log stays authoritative and recover() rebuilds from it.
 *
 * Accessed from multiple shard worker threads (one shard can poison while another
 * reads the flag), so all reads/writes go through __atomic.
 */
static int	manifest_poisoned = 0;

/*
 * Records currently in the on-disk log (set by replay, bumped by append, reset by
 * compaction).  The log is append-only, so add/delete churn makes it grow without
 * bound; ps_manifest_should_compact() uses this to decide when to rewrite it down
 * to one ADD_LAYER per live layer (ps_manifest_compact()), bounding replay time.
 */
static uint64_t manifest_nrecords = 0;

static int
manifest_fsync_dir(void)
{
	int			fd;
	int			rc;

	fd = open(manifest_dir, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	return rc;
}

/* FNV-1a (streaming): not cryptographic, just integrity.  Matches img_crc(). */
static uint32_t
manifest_fnv1a(uint32_t h, const void *p, size_t n)
{
	const unsigned char *b = p;

	for (size_t i = 0; i < n; i++)
	{
		h ^= b[i];
		h *= 16777619u;
	}
	return h;
}

/* CRC over the header fields preceding rec.crc, then the payload. */
static uint32_t
manifest_record_crc(const PsManifestRecord *rec, const void *payload, uint32_t len)
{
	uint32_t	crc;

	crc = manifest_fnv1a(PS_MANIFEST_FNV_INIT, rec,
						 offsetof(PsManifestRecord, crc));
	if (len > 0)
		crc = manifest_fnv1a(crc, payload, len);
	return crc;
}

/* Fixed on-disk payload length for a record type, or -1 if the type is unknown. */
static int
manifest_type_payload_len(uint32_t type)
{
	switch ((PsManifestEventType) type)
	{
		case PS_MANIFEST_ADD_LAYER:
			return (int) sizeof(PsManifestLayerDisk);
		case PS_MANIFEST_SET_REMOTE_DURABLE:
		case PS_MANIFEST_DROP_LOCAL:
		case PS_MANIFEST_MARK_DELETE:
		case PS_MANIFEST_REMOVE_LAYER:
			return (int) sizeof(PsManifestLayerIdEvent);
		default:
			return -1;
	}
}

/* Write one [header | payload] record to an open fd (no fsync). */
static int
manifest_write_record(int fd, uint32_t type, const void *payload, uint32_t len)
{
	PsManifestRecord rec;

	rec.magic = PS_MANIFEST_MAGIC;
	rec.version = PS_MANIFEST_VERSION;
	rec.type = type;
	rec.len = len;
	rec.crc = manifest_record_crc(&rec, payload, len);

	if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec) ||
		(len > 0 && write(fd, payload, len) != (ssize_t) len))
		return -1;
	return 0;
}

static int
manifest_append(uint32_t type, const void *payload, uint32_t len)
{
	int			fd;
	int			rc = 0;
	int			created = 0;

	/* once the tail may be torn, never append again (see manifest_poisoned) */
	if (__atomic_load_n(&manifest_poisoned, __ATOMIC_ACQUIRE))
		return -1;

	fd = open(manifest_path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;				/* nothing written; tail not torn */
	if (lseek(fd, 0, SEEK_END) == 0)
		created = 1;

	if (manifest_write_record(fd, type, payload, len) != 0)
		rc = -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (created && manifest_fsync_dir() != 0)
		rc = -1;
	close(fd);
	if (rc != 0)
		/* a partial write/fsync may have torn the tail */
		__atomic_store_n(&manifest_poisoned, 1, __ATOMIC_RELEASE);
	else
		manifest_nrecords++;
	return rc;
}

static PsLayerDesc *
manifest_find_layer(PsLayerMap *map, uint64_t layer_id)
{
	for (uint32_t i = 0; i < map->nlayers; i++)
		if (map->layers[i].layer_id == layer_id)
			return &map->layers[i];
	return NULL;
}

static int
manifest_layer_exists(PsLayerMap *map, uint64_t layer_id)
{
	return manifest_find_layer(map, layer_id) != NULL;
}

static int
manifest_layer_desc_valid(const PsLayerDesc *desc)
{
	return desc->location_count <= PS_LAYER_MAX_LOCATIONS;
}

static void
manifest_encode_key(PsManifestKeyDisk *dst, const PsKey *src)
{
	dst->spcOid = src->spcOid;
	dst->dbOid = src->dbOid;
	dst->relNumber = src->relNumber;
	dst->forkNum = src->forkNum;
	dst->klass = src->klass;
}

static void
manifest_decode_key(PsKey *dst, const PsManifestKeyDisk *src)
{
	dst->spcOid = src->spcOid;
	dst->dbOid = src->dbOid;
	dst->relNumber = src->relNumber;
	dst->forkNum = src->forkNum;
	dst->klass = src->klass;	/* mirror encode; manifest v2 -- without this a
								 * replayed non-relation layer key decodes as
								 * PS_KLASS_RELATION and its lookups are pruned */
}

static int
manifest_encode_layer(PsManifestLayerDisk *dst, const PsLayerDesc *src)
{
	if (!manifest_layer_desc_valid(src))
		return -1;
	memset(dst, 0, sizeof(*dst));
	dst->layer_id = src->layer_id;
	dst->kind = (uint32_t) src->kind;
	dst->timeline = src->timeline;
	manifest_encode_key(&dst->start_key, &src->start_key);
	manifest_encode_key(&dst->end_key, &src->end_key);
	dst->start_block = src->start_block;
	dst->end_block = src->end_block;
	dst->lsn_start = src->lsn_start;
	dst->lsn_end = src->lsn_end;
	dst->location_count = src->location_count;
	for (uint32_t i = 0; i < src->location_count; i++)
	{
		dst->locations[i].tier = (uint32_t) src->locations[i].tier;
		memcpy(dst->locations[i].uri, src->locations[i].uri,
			   sizeof(dst->locations[i].uri));
		dst->locations[i].uri[PS_LAYER_URI_MAX - 1] = '\0';
		dst->locations[i].size = src->locations[i].size;
		dst->locations[i].generation = src->locations[i].generation;
		dst->locations[i].available = src->locations[i].available ? 1 : 0;
	}
	dst->created_at_lsn = src->created_at_lsn;
	dst->remote_uploaded_lsn = src->remote_uploaded_lsn;
	dst->remote_durable = src->remote_durable ? 1 : 0;
	dst->local_pinned = src->local_pinned ? 1 : 0;
	dst->deleting = src->deleting ? 1 : 0;
	return 0;
}

static int
manifest_decode_layer(PsLayerDesc *dst, const PsManifestLayerDisk *src)
{
	if (src->location_count > PS_LAYER_MAX_LOCATIONS)
		return -1;
	memset(dst, 0, sizeof(*dst));
	dst->layer_id = src->layer_id;
	dst->kind = (PsLayerKind) src->kind;
	dst->timeline = src->timeline;
	manifest_decode_key(&dst->start_key, &src->start_key);
	manifest_decode_key(&dst->end_key, &src->end_key);
	dst->start_block = src->start_block;
	dst->end_block = src->end_block;
	dst->lsn_start = src->lsn_start;
	dst->lsn_end = src->lsn_end;
	dst->location_count = src->location_count;
	for (uint32_t i = 0; i < src->location_count; i++)
	{
		dst->locations[i].tier = (PsLayerTier) src->locations[i].tier;
		memcpy(dst->locations[i].uri, src->locations[i].uri,
			   sizeof(dst->locations[i].uri));
		dst->locations[i].uri[PS_LAYER_URI_MAX - 1] = '\0';
		dst->locations[i].size = src->locations[i].size;
		dst->locations[i].generation = src->locations[i].generation;
		dst->locations[i].available = src->locations[i].available != 0;
	}
	dst->created_at_lsn = src->created_at_lsn;
	dst->remote_uploaded_lsn = src->remote_uploaded_lsn;
	dst->remote_durable = src->remote_durable != 0;
	dst->local_pinned = src->local_pinned != 0;
	dst->deleting = src->deleting != 0;
	return 0;
}

static void
manifest_remove_from_map(PsLayerMap *map, uint64_t layer_id)
{
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		if (map->layers[i].layer_id == layer_id)
		{
			if (i + 1 < map->nlayers)
				memmove(&map->layers[i], &map->layers[i + 1],
						(size_t) (map->nlayers - i - 1) * sizeof(PsLayerDesc));
			map->nlayers--;
			return;
		}
	}
}

int
ps_manifest_open(const char *store_dir)
{
	int			n;

	n = snprintf(manifest_dir, sizeof(manifest_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(manifest_dir))
		return -1;
	n = snprintf(manifest_path, sizeof(manifest_path), "%s/layers.manifest", store_dir);
	if (n < 0 || (size_t) n >= sizeof(manifest_path))
		return -1;
	/* replay truncates any torn tail; start clean */
	__atomic_store_n(&manifest_poisoned, 0, __ATOMIC_RELEASE);
	manifest_nrecords = 0;
	ps_layer_map_init(&ps_layer_map);
	return 0;
}

void
ps_manifest_close(void)
{
	ps_layer_map_free(&ps_layer_map);
	manifest_path[0] = '\0';
}

/*
 * True once an append failed (torn tail).  The metadata is no longer durable, so
 * callers must stop persisting new layers -- the daemon rejects further writes and
 * skips compaction rather than accepting data it can never record (and that
 * layer-based recovery would not see).  Cleared by (re)open after replay recovers.
 */
int
ps_manifest_poisoned(void)
{
	return __atomic_load_n(&manifest_poisoned, __ATOMIC_ACQUIRE);
}

int
ps_manifest_replay(PsLayerMap *map)
{
	int			fd;
	off_t		good_off = 0;
	int			truncate_tail = 0;
	off_t		file_size;
	struct stat st;

	manifest_nrecords = 0;
	fd = open(manifest_path, O_RDWR);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return 0;
		return -1;
	}
	if (fstat(fd, &st) != 0)
	{
		close(fd);
		return -1;
	}
	file_size = st.st_size;

	for (;;)
	{
		PsManifestRecord rec;
		union					/* aligned buffer sized for the largest payload */
		{
			unsigned char bytes[sizeof(PsManifestLayerDisk)];
			PsManifestLayerDisk layer;
			PsManifestLayerIdEvent ev;
		}			payload;
		ssize_t		n;
		off_t		rec_off = good_off;
		int			has_crc,
					plen;
		off_t		hdr_size,
					rec_total;

		/*
		 * Read the 16-byte common prefix (magic, version, type, len) shared by
		 * every format version.  A short read or wrong magic at end-of-file is a
		 * recoverable torn tail; the same fault earlier in the file is corruption.
		 */
		n = read(fd, &rec, offsetof(PsManifestRecord, crc));
		if (n == 0)
			break;				/* clean end of log */
		if (n != (ssize_t) offsetof(PsManifestRecord, crc) ||
			rec.magic != PS_MANIFEST_MAGIC)
		{
			if (rec_off + (off_t) offsetof(PsManifestRecord, crc) >= file_size)
			{
				truncate_tail = 1;
				good_off = rec_off;
				break;
			}
			close(fd);
			return -1;
		}

		/* accept v3 (per-record CRC) and the legacy v2 (no CRC); reject others */
		if (rec.version == PS_MANIFEST_VERSION)
		{
			has_crc = 1;
			hdr_size = (off_t) sizeof(PsManifestRecord);
		}
		else if (rec.version == PS_MANIFEST_VERSION_V2)
		{
			has_crc = 0;
			hdr_size = (off_t) offsetof(PsManifestRecord, crc);
		}
		else
		{
			if (rec_off + (off_t) offsetof(PsManifestRecord, crc) >= file_size)
			{
				truncate_tail = 1;
				good_off = rec_off;
				break;
			}
			close(fd);
			return -1;
		}
		if (has_crc &&
			read(fd, &rec.crc, sizeof(rec.crc)) != (ssize_t) sizeof(rec.crc))
		{
			truncate_tail = 1;
			good_off = rec_off;
			break;				/* torn CRC */
		}

		/*
		 * Size the record from its TYPE, never the (untrusted) len field, so a
		 * corrupted len cannot misalign the read.  An unknown type with data after
		 * it is interior corruption; at EOF it is a torn tail.
		 */
		plen = manifest_type_payload_len(rec.type);
		if (plen < 0 || (size_t) plen > sizeof(payload.bytes))
		{
			if (rec_off + hdr_size >= file_size)
			{
				truncate_tail = 1;
				good_off = rec_off;
				break;
			}
			close(fd);
			return -1;
		}
		rec_total = hdr_size + plen;

		/* a record that does not fully fit in the file is a truncated torn tail */
		if (rec_off + rec_total > file_size)
		{
			truncate_tail = 1;
			good_off = rec_off;
			break;
		}
		if (plen > 0 &&
			read(fd, payload.bytes, (size_t) plen) != (ssize_t) plen)
		{
			truncate_tail = 1;
			good_off = rec_off;
			break;				/* (unreachable given the fit check above) */
		}

		/*
		 * Integrity: v3 verifies the CRC; both versions require the len field to
		 * equal the type's fixed size (this catches a flipped len on v2 and is
		 * defence-in-depth on v3).  A full-sized record that fails is interior
		 * corruption unless it is physically the last record (then a torn tail).
		 */
		if (rec.len != (uint32_t) plen ||
			(has_crc &&
			 manifest_record_crc(&rec, payload.bytes, (uint32_t) plen) != rec.crc))
		{
			if (rec_off + rec_total >= file_size)
			{
				truncate_tail = 1;
				good_off = rec_off;
				break;
			}
			close(fd);
			return -1;
		}

		switch ((PsManifestEventType) rec.type)
		{
			case PS_MANIFEST_ADD_LAYER:
				{
					PsLayerDesc desc;

					if (manifest_decode_layer(&desc, &payload.layer) != 0 ||
						(!manifest_layer_exists(map, desc.layer_id) &&
						 ps_layer_map_add(map, &desc) != 0))
					{
						close(fd);
						return -1;
					}
					break;
				}

			case PS_MANIFEST_SET_REMOTE_DURABLE:
				{
					PsLayerDesc *layer = manifest_find_layer(map, payload.ev.layer_id);

					if (layer != NULL)
					{
						layer->remote_durable = true;
						layer->remote_uploaded_lsn = payload.ev.value;
					}
					break;
				}

			case PS_MANIFEST_MARK_DELETE:
				{
					PsLayerDesc *layer = manifest_find_layer(map, payload.ev.layer_id);

					if (layer != NULL)
						layer->deleting = true;
					break;
				}

			case PS_MANIFEST_REMOVE_LAYER:
				manifest_remove_from_map(map, payload.ev.layer_id);
				break;

			case PS_MANIFEST_DROP_LOCAL:
				{
					PsLayerDesc *layer = manifest_find_layer(map, payload.ev.layer_id);

					if (layer != NULL)
						for (uint32_t i = 0; i < layer->location_count; i++)
							if (layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
								layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD)
								layer->locations[i].available = false;
					break;
				}

			default:
				close(fd);
				return -1;
		}
		good_off = rec_off + rec_total;	/* we read exactly rec_total above */
		manifest_nrecords++;
	}

	if (truncate_tail &&
		(ftruncate(fd, good_off) != 0 || fsync(fd) != 0))
	{
		close(fd);
		return -1;
	}
	close(fd);
	return 0;
}

int
ps_manifest_add_layer(const PsLayerDesc *desc)
{
	PsManifestLayerDisk disk;

	if (!manifest_layer_desc_valid(desc))
		return -1;
	if (manifest_layer_exists(&ps_layer_map, desc->layer_id))
		return 0;
	if (ps_layer_map_reserve(&ps_layer_map, ps_layer_map_count(&ps_layer_map) + 1) != 0)
		return -1;
	if (manifest_encode_layer(&disk, desc) != 0)
		return -1;
	if (manifest_append(PS_MANIFEST_ADD_LAYER, &disk, sizeof(disk)) != 0)
		return -1;
	return ps_layer_map_add(&ps_layer_map, desc);
}

int
ps_manifest_set_remote_durable(uint64_t layer_id, uint64_t uploaded_lsn)
{
	PsManifestLayerIdEvent ev;
	PsLayerDesc *layer;

	ev.layer_id = layer_id;
	ev.value = uploaded_lsn;
	if (manifest_append(PS_MANIFEST_SET_REMOTE_DURABLE, &ev, sizeof(ev)) != 0)
		return -1;
	layer = manifest_find_layer(&ps_layer_map, layer_id);
	if (layer != NULL)
	{
		layer->remote_durable = true;
		layer->remote_uploaded_lsn = uploaded_lsn;
	}
	return 0;
}

int
ps_manifest_mark_delete(uint64_t layer_id)
{
	PsManifestLayerIdEvent ev;
	PsLayerDesc *layer;

	ev.layer_id = layer_id;
	ev.value = 0;
	if (manifest_append(PS_MANIFEST_MARK_DELETE, &ev, sizeof(ev)) != 0)
		return -1;
	layer = manifest_find_layer(&ps_layer_map, layer_id);
	if (layer != NULL)
		layer->deleting = true;
	return 0;
}

int
ps_manifest_remove_layer(uint64_t layer_id)
{
	PsManifestLayerIdEvent ev;

	ev.layer_id = layer_id;
	ev.value = 0;
	if (manifest_append(PS_MANIFEST_REMOVE_LAYER, &ev, sizeof(ev)) != 0)
		return -1;
	manifest_remove_from_map(&ps_layer_map, layer_id);
	return 0;
}

/*
 * Should the log be rewritten?  True once it has grown well past the live layer
 * count (add/seal/delete churn appends records the live set no longer needs), so
 * replay stays bounded.  Never while poisoned -- a restart recovers that first.
 */
int
ps_manifest_should_compact(void)
{
	if (__atomic_load_n(&manifest_poisoned, __ATOMIC_ACQUIRE))
		return 0;
	return manifest_nrecords >= 64 &&
		manifest_nrecords > 4 * ((uint64_t) ps_layer_map.nlayers + 1);
}

/*
 * Rewrite the log to one ADD_LAYER record per current layer (an ADD encodes every
 * flag -- deleting, remote_durable, ... -- and replay restores them, so one record
 * fully captures a layer's state).  Atomic via a temp file + rename + dir fsync:
 * a crash before the rename leaves the old full log intact; after it, the new
 * compacted log; both replay to the same layer map.  Caller must hold the state
 * stable (no concurrent map mutation) across this.
 */
int
ps_manifest_compact(void)
{
	char		tmp[4096];
	int			fd;
	int			rc = 0;
	uint64_t	nrec = 0;
	int			n;

	if (__atomic_load_n(&manifest_poisoned, __ATOMIC_ACQUIRE))
		return -1;
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", manifest_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;

	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	for (uint32_t i = 0; i < ps_layer_map.nlayers && rc == 0; i++)
	{
		PsManifestLayerDisk disk;

		if (manifest_encode_layer(&disk, &ps_layer_map.layers[i]) != 0 ||
			manifest_write_record(fd, PS_MANIFEST_ADD_LAYER, &disk, sizeof(disk)) != 0)
			rc = -1;
		else
			nrec++;
	}
	if (rc == 0 && fsync(fd) != 0)
		rc = -1;
	close(fd);
	if (rc != 0)
	{
		unlink(tmp);			/* never touched the live manifest */
		return -1;
	}
	if (rename(tmp, manifest_path) != 0)
	{
		unlink(tmp);
		return -1;
	}
	if (manifest_fsync_dir() != 0)
	{
		/*
		 * The rename is in place but its durability is unconfirmed (the directory
		 * fsync failed -- the disk is misbehaving).  Poison so no further append
		 * lands until a restart re-establishes a known-durable manifest, matching
		 * the fail-stop policy for any other manifest write/fsync error.  Atomic:
		 * a concurrent shard may read the flag without the map lock.
		 */
		__atomic_store_n(&manifest_poisoned, 1, __ATOMIC_RELEASE);
		return -1;
	}
	manifest_nrecords = nrec;
	return 0;
}
