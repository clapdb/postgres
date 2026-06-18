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

static char manifest_path[4096];
PsLayerMap ps_layer_map;

static int
manifest_append(uint32_t type, const void *payload, uint32_t len)
{
	PsManifestRecord rec;
	int			fd;
	int			rc = 0;

	fd = open(manifest_path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;

	rec.magic = PS_MANIFEST_MAGIC;
	rec.version = PS_MANIFEST_VERSION;
	rec.type = type;
	rec.len = len;

	if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec) ||
		(len > 0 && write(fd, payload, len) != (ssize_t) len))
		rc = -1;
	if (fsync(fd) != 0)
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
manifest_layer_desc_valid(const PsLayerDesc *desc)
{
	return desc->location_count <= PS_LAYER_MAX_LOCATIONS;
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
	snprintf(manifest_path, sizeof(manifest_path), "%s/layers.manifest", store_dir);
	ps_layer_map_init(&ps_layer_map);
	return 0;
}

void
ps_manifest_close(void)
{
	ps_layer_map_free(&ps_layer_map);
	manifest_path[0] = '\0';
}

int
ps_manifest_replay(PsLayerMap *map)
{
	int			fd;

	fd = open(manifest_path, O_RDONLY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return 0;
		return -1;
	}

	for (;;)
	{
		PsManifestRecord rec;
		ssize_t		n;

		n = read(fd, &rec, sizeof(rec));
		if (n == 0)
			break;
		if (n != (ssize_t) sizeof(rec))
			break;				/* torn tail */
		if (rec.magic != PS_MANIFEST_MAGIC ||
			rec.version != PS_MANIFEST_VERSION)
		{
			close(fd);
			return -1;
		}

		switch ((PsManifestEventType) rec.type)
		{
			case PS_MANIFEST_ADD_LAYER:
				{
					PsLayerDesc desc;
					ssize_t		nread;

					if (rec.len != sizeof(desc))
					{
						close(fd);
						return -1;
					}
					nread = read(fd, &desc, sizeof(desc));
					if (nread != (ssize_t) sizeof(desc))
						goto done;	/* torn tail */
					if (!manifest_layer_desc_valid(&desc) ||
						ps_layer_map_add(map, &desc) != 0)
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
						goto done;	/* torn tail */
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
						goto done;	/* torn tail */
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
						goto done;	/* torn tail */
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
						goto done;	/* torn tail */
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
	}

done:
	close(fd);
	return 0;
}

int
ps_manifest_add_layer(const PsLayerDesc *desc)
{
	if (!manifest_layer_desc_valid(desc))
		return -1;
	if (manifest_append(PS_MANIFEST_ADD_LAYER, desc, sizeof(*desc)) != 0)
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
