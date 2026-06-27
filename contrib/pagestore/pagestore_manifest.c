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

/*
 * Does a fully-intact record begin at byte offset 'off'?  "Fully intact" means the
 * header fits, magic and version are exact, the type is known, the len field equals
 * that type's fixed payload size, the payload fits, and (for v3) the CRC matches --
 * so no byte of the record is corrupted.  Nothing is trusted before the CRC confirms
 * it, so a flipped version/type/len/magic cannot be reinterpreted into a "valid"
 * record of a different shape.  'v2' selects the legacy 16-byte header (no CRC) -- a
 * whole manifest file is one version or the other (decided from its first record),
 * so this can never be used to read a corrupted v3 record as an unchecked v2 one.
 * On success copies the header into *rec and the payload into payload_buf, sets
 * *rec_size to the on-disk record size, and returns 1; otherwise 0.  Uses pread so
 * callers can probe an arbitrary offset without disturbing the file position.
 */
static int
manifest_record_valid_at(int fd, off_t off, off_t file_size, int v2,
						 PsManifestRecord *rec, void *payload_buf,
						 size_t payload_cap, off_t *rec_size)
{
	off_t		hdr = v2 ? (off_t) offsetof(PsManifestRecord, crc)
		: (off_t) sizeof(*rec);
	uint32_t	want_ver = v2 ? 2u : (uint32_t) PS_MANIFEST_VERSION;
	int			plen;

	if (off < 0 || off + hdr > file_size)
		return 0;				/* no room for even a header */
	if (pread(fd, rec, (size_t) hdr, off) != (ssize_t) hdr)
		return 0;
	if (rec->magic != PS_MANIFEST_MAGIC || rec->version != want_ver)
		return 0;
	plen = manifest_type_payload_len(rec->type);
	if (plen < 0 || (size_t) plen > payload_cap || rec->len != (uint32_t) plen)
		return 0;
	if (off + hdr + plen > file_size)
		return 0;				/* body does not fit */
	if (plen > 0 &&
		pread(fd, payload_buf, (size_t) plen, off + hdr) != (ssize_t) plen)
		return 0;
	if (!v2 && manifest_record_crc(rec, payload_buf, (uint32_t) plen) != rec->crc)
		return 0;
	*rec_size = hdr + plen;
	return 1;
}

int
ps_manifest_replay(PsLayerMap *map)
{
	int			fd;
	off_t		good_off = 0;
	int			truncate_tail = 0;
	int			file_v2;
	off_t		file_size;
	struct stat st;

	/*
	 * The most bytes one record can occupy on disk (header + the largest payload).
	 * A record after a corrupt one is contiguous, so it cannot begin farther than
	 * this past the corrupt record -- which bounds the torn-tail probe below.
	 */
	const off_t max_record = (off_t) sizeof(PsManifestRecord) +
		(off_t) sizeof(PsManifestLayerDisk);

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

	/*
	 * Decide the whole file's format from its first record: a manifest is entirely
	 * v3 (per-record CRC) or entirely legacy v2 (no CRC), never a mix.  Probing the
	 * first record as v3 first means a v3 record whose version was bit-flipped to 2
	 * is not reinterpreted as an unchecked v2 record (it just fails the v3 probe and
	 * is handled as corruption below) -- only a genuine v2 file, whose first record
	 * is a valid v2 record, is read without CRC.  A v2 file is migrated to v3 after
	 * replay so this branch is taken at most once per store.
	 */
	{
		PsManifestRecord r0;
		unsigned char b0[sizeof(PsManifestLayerDisk)];
		off_t		s0;

		if (file_size == 0)
			file_v2 = 0;
		else if (manifest_record_valid_at(fd, 0, file_size, 0, &r0, b0,
										  sizeof(b0), &s0))
			file_v2 = 0;
		else if (manifest_record_valid_at(fd, 0, file_size, 1, &r0, b0,
										  sizeof(b0), &s0))
			file_v2 = 1;
		else
			file_v2 = 0;		/* not a valid first record either way; the v3 path
								 * below truncates a torn/garbage start as an empty
								 * manifest (nothing was committed) */
	}

	while (good_off < file_size)
	{
		PsManifestRecord rec;
		union					/* aligned buffer sized for the largest payload */
		{
			unsigned char bytes[sizeof(PsManifestLayerDisk)];
			PsManifestLayerDisk layer;
			PsManifestLayerIdEvent ev;
		}			payload;
		off_t		rec_off = good_off;
		off_t		rec_size;
		off_t		scan;
		int			interior;

		if (!manifest_record_valid_at(fd, rec_off, file_size, file_v2, &rec,
									  payload.bytes, sizeof(payload.bytes),
									  &rec_size))
		{
			/*
			 * No intact record begins at rec_off: it is corrupt or torn.  Treat it
			 * as a recoverable torn tail only if no intact record begins after it
			 * -- then the damage is confined to the final record and the earlier
			 * entries survive.  If an intact record does follow, this is interior
			 * corruption and we must fail rather than truncate valid history (or
			 * resurrect a dropped layer).  A following record is contiguous, so it
			 * begins within one max_record of rec_off; that bounds the probe and
			 * keeps a corrupted type/len/magic from being trusted to size anything.
			 */
			interior = 0;
			for (scan = rec_off + 1;
				 scan <= rec_off + max_record && scan < file_size; scan++)
			{
				PsManifestRecord prec;
				union
				{
					unsigned char bytes[sizeof(PsManifestLayerDisk)];
					PsManifestLayerDisk layer;
					PsManifestLayerIdEvent ev;
				}			pbuf;
				off_t		psize;

				if (manifest_record_valid_at(fd, scan, file_size, file_v2, &prec,
											 pbuf.bytes, sizeof(pbuf.bytes),
											 &psize))
				{
					interior = 1;
					break;
				}
			}
			if (interior)
			{
				close(fd);
				return -1;
			}
			truncate_tail = 1;
			good_off = rec_off;
			break;
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
		good_off = rec_off + rec_size;
		manifest_nrecords++;
	}

	if (truncate_tail &&
		(ftruncate(fd, good_off) != 0 || fsync(fd) != 0))
	{
		close(fd);
		return -1;
	}
	close(fd);

	/*
	 * Upgrade a legacy v2 manifest in place: the replayed state is now in 'map', so
	 * rewrite the log as v3 (one ADD per live layer) via temp+rename.  This both
	 * adds CRC protection going forward and -- crucially -- avoids appending v3
	 * records onto a v2 file (which would defeat the whole-file version check above)
	 * and avoids discarding the old metadata.  Done once; later opens see v3.
	 */
	if (file_v2 && map == &ps_layer_map && ps_manifest_compact() != 0)
		return -1;

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
