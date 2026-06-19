/*-------------------------------------------------------------------------
 *
 * pagestore_layer.c
 *	  LSM-like immutable layer metadata helpers.
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <string.h>

#include "pagestore_layer.h"

void
ps_layer_map_init(PsLayerMap *map)
{
	memset(map, 0, sizeof(*map));
}

void
ps_layer_map_free(PsLayerMap *map)
{
	free(map->layers);
	memset(map, 0, sizeof(*map));
}

int
ps_layer_map_reserve(PsLayerMap *map, uint32_t capacity)
{
	if (capacity > map->capacity)
	{
		uint32_t	newcap = map->capacity ? map->capacity * 2 : 64;
		PsLayerDesc *newlayers;

		while (newcap < capacity)
			newcap *= 2;
		newlayers = realloc(map->layers, (size_t) newcap * sizeof(PsLayerDesc));
		if (newlayers == NULL)
			return -1;
		map->layers = newlayers;
		map->capacity = newcap;
	}
	return 0;
}

int
ps_layer_map_add(PsLayerMap *map, const PsLayerDesc *desc)
{
	if (ps_layer_map_reserve(map, map->nlayers + 1) != 0)
		return -1;
	map->layers[map->nlayers++] = *desc;
	return 0;
}

uint32_t
ps_layer_map_count(const PsLayerMap *map)
{
	return map->nlayers;
}
