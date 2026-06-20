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
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_manifest.h"

#define PS_MANIFEST_MAGIC	0x504d414e	/* "PMAN" */
#define PS_MANIFEST_VERSION 1

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

static int
manifest_fsync_dir(PsManifest *m)
{
	int			fd;
	int			rc;

	fd = open(m->dir, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	return rc;
}

static int
manifest_append(PsManifest *m, uint32_t type, const void *payload, uint32_t len)
{
	PsManifestRecord rec;
	int			fd;
	int			rc = 0;
	int			created = 0;

	fd = open(m->path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;
	if (lseek(fd, 0, SEEK_END) == 0)
		created = 1;

	rec.magic = PS_MANIFEST_MAGIC;
	rec.version = PS_MANIFEST_VERSION;
	rec.type = type;
	rec.len = len;

	if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec) ||
		(len > 0 && write(fd, payload, len) != (ssize_t) len))
		rc = -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (created && manifest_fsync_dir(m) != 0)
		rc = -1;
	close(fd);
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
}

static void
manifest_decode_key(PsKey *dst, const PsManifestKeyDisk *src)
{
	dst->spcOid = src->spcOid;
	dst->dbOid = src->dbOid;
	dst->relNumber = src->relNumber;
	dst->forkNum = src->forkNum;
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
ps_manifest_open(PsManifest *m, const char *store_dir, uint32_t shard)
{
	int			n;

	n = snprintf(m->dir, sizeof(m->dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(m->dir))
		return -1;
	/* one manifest file per shard; the shard id keeps them distinct in the one
	 * store directory (layer files are likewise shard-namespaced via layer_id) */
	n = snprintf(m->path, sizeof(m->path), "%s/layers.%u.manifest", store_dir,
				 shard);
	if (n < 0 || (size_t) n >= sizeof(m->path))
		return -1;
	ps_layer_map_init(&m->map);
	return 0;
}

void
ps_manifest_close(PsManifest *m)
{
	ps_layer_map_free(&m->map);
	m->path[0] = '\0';
}

int
ps_manifest_replay(PsManifest *m)
{
	PsLayerMap *map = &m->map;
	int			fd;
	off_t		good_off = 0;
	int			truncate_tail = 0;
	off_t		file_size;
	struct stat st;

	fd = open(m->path, O_RDWR);
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
		ssize_t		n;
		off_t		rec_off = good_off;

		n = read(fd, &rec, sizeof(rec));
		if (n == 0)
			break;
		if (n != (ssize_t) sizeof(rec))
		{
			truncate_tail = 1;
			good_off = rec_off;
			break;				/* torn tail */
		}
		if (rec.magic != PS_MANIFEST_MAGIC ||
			rec.version != PS_MANIFEST_VERSION)
		{
			if (rec_off + (off_t) sizeof(rec) >= file_size)
			{
				truncate_tail = 1;
				good_off = rec_off;
				break;			/* invalid torn tail header */
			}
			close(fd);
			return -1;
		}

		switch ((PsManifestEventType) rec.type)
		{
			case PS_MANIFEST_ADD_LAYER:
				{
					PsManifestLayerDisk disk;
					PsLayerDesc desc;
					ssize_t		nread;

					if (rec.len != sizeof(disk))
					{
						close(fd);
						return -1;
					}
					nread = read(fd, &disk, sizeof(disk));
					if (nread != (ssize_t) sizeof(disk))
					{
						truncate_tail = 1;
						good_off = rec_off;
						goto done;	/* torn tail */
					}
					if (manifest_decode_layer(&desc, &disk) != 0 ||
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
					PsManifestLayerIdEvent ev;
					PsLayerDesc *layer;
					ssize_t		nread;

					if (rec.len != sizeof(ev))
					{
						close(fd);
						return -1;
					}
					nread = read(fd, &ev, sizeof(ev));
					if (nread != (ssize_t) sizeof(ev))
					{
						truncate_tail = 1;
						good_off = rec_off;
						goto done;	/* torn tail */
					}
					layer = manifest_find_layer(map, ev.layer_id);
					if (layer != NULL)
					{
						layer->remote_durable = true;
						layer->remote_uploaded_lsn = ev.value;
					}
					break;
				}

			case PS_MANIFEST_MARK_DELETE:
				{
					PsManifestLayerIdEvent ev;
					PsLayerDesc *layer;
					ssize_t		nread;

					if (rec.len != sizeof(ev))
					{
						close(fd);
						return -1;
					}
					nread = read(fd, &ev, sizeof(ev));
					if (nread != (ssize_t) sizeof(ev))
					{
						truncate_tail = 1;
						good_off = rec_off;
						goto done;	/* torn tail */
					}
					layer = manifest_find_layer(map, ev.layer_id);
					if (layer != NULL)
						layer->deleting = true;
					break;
				}

			case PS_MANIFEST_REMOVE_LAYER:
				{
					PsManifestLayerIdEvent ev;
					ssize_t		nread;

					if (rec.len != sizeof(ev))
					{
						close(fd);
						return -1;
					}
					nread = read(fd, &ev, sizeof(ev));
					if (nread != (ssize_t) sizeof(ev))
					{
						truncate_tail = 1;
						good_off = rec_off;
						goto done;	/* torn tail */
					}
					manifest_remove_from_map(map, ev.layer_id);
					break;
				}

			case PS_MANIFEST_DROP_LOCAL:
				{
					PsManifestLayerIdEvent ev;
					PsLayerDesc *layer;
					ssize_t		nread;

					if (rec.len != sizeof(ev))
					{
						close(fd);
						return -1;
					}
					nread = read(fd, &ev, sizeof(ev));
					if (nread != (ssize_t) sizeof(ev))
					{
						truncate_tail = 1;
						good_off = rec_off;
						goto done;	/* torn tail */
					}
					layer = manifest_find_layer(map, ev.layer_id);
					if (layer != NULL)
					{
						for (uint32_t i = 0; i < layer->location_count; i++)
						{
							if (layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
								layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD)
								layer->locations[i].available = false;
						}
					}
					break;
				}

			default:
				close(fd);
				return -1;
		}
		good_off = lseek(fd, 0, SEEK_CUR);
		if (good_off < 0)
		{
			close(fd);
			return -1;
		}
	}

done:
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
ps_manifest_add_layer(PsManifest *m, const PsLayerDesc *desc)
{
	PsManifestLayerDisk disk;

	if (!manifest_layer_desc_valid(desc))
		return -1;
	if (manifest_layer_exists(&m->map, desc->layer_id))
		return 0;
	if (ps_layer_map_reserve(&m->map, ps_layer_map_count(&m->map) + 1) != 0)
		return -1;
	if (manifest_encode_layer(&disk, desc) != 0)
		return -1;
	if (manifest_append(m, PS_MANIFEST_ADD_LAYER, &disk, sizeof(disk)) != 0)
		return -1;
	return ps_layer_map_add(&m->map, desc);
}

int
ps_manifest_set_remote_durable(PsManifest *m, uint64_t layer_id,
							   uint64_t uploaded_lsn)
{
	PsManifestLayerIdEvent ev;
	PsLayerDesc *layer;

	ev.layer_id = layer_id;
	ev.value = uploaded_lsn;
	if (manifest_append(m, PS_MANIFEST_SET_REMOTE_DURABLE, &ev, sizeof(ev)) != 0)
		return -1;
	layer = manifest_find_layer(&m->map, layer_id);
	if (layer != NULL)
	{
		layer->remote_durable = true;
		layer->remote_uploaded_lsn = uploaded_lsn;
	}
	return 0;
}

int
ps_manifest_mark_delete(PsManifest *m, uint64_t layer_id)
{
	PsManifestLayerIdEvent ev;
	PsLayerDesc *layer;

	ev.layer_id = layer_id;
	ev.value = 0;
	if (manifest_append(m, PS_MANIFEST_MARK_DELETE, &ev, sizeof(ev)) != 0)
		return -1;
	layer = manifest_find_layer(&m->map, layer_id);
	if (layer != NULL)
		layer->deleting = true;
	return 0;
}

int
ps_manifest_remove_layer(PsManifest *m, uint64_t layer_id)
{
	PsManifestLayerIdEvent ev;

	ev.layer_id = layer_id;
	ev.value = 0;
	if (manifest_append(m, PS_MANIFEST_REMOVE_LAYER, &ev, sizeof(ev)) != 0)
		return -1;
	manifest_remove_from_map(&m->map, layer_id);
	return 0;
}
