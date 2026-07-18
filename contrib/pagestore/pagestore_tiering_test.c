/*-------------------------------------------------------------------------
 *
 * pagestore_tiering_test.c
 *    Integration test for idle layer upload and manifest durability ordering.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_layer_store.h"
#include "pagestore_manifest.h"

#define PSZ 8192

static int run = 0,
			failed = 0;

static void
check(int cond, const char *msg)
{
	run++;
	if (!cond)
	{
		failed++;
		fprintf(stderr, "  FAIL: %s\n", msg);
	}
}

static PsLayerDesc *
find_layer(uint64_t id)
{
	for (uint32_t i = 0; i < ps_layer_map.nlayers; i++)
		if (ps_layer_map.layers[i].layer_id == id)
			return &ps_layer_map.layers[i];
	return NULL;
}

int
main(void)
{
	char		store[] = "/tmp/pstieringstoreXXXXXX";
	char		objects[] = "/tmp/pstieringobjectsXXXXXX";
	unsigned char page[PSZ];
	unsigned char out[PSZ];
	PsKey		key = {1, 1, 1, 0, PS_KLASS_RELATION};
	uint32_t	lsn_hi = 0, lsn_lo = 100;
	uint64_t	layer_id;
	PsLayerDesc *layer;
	struct timespec deadline;

	if (mkdtemp(store) == NULL || mkdtemp(objects) == NULL ||
		setenv("PAGESTORE_OBJECT_DIR", objects, 1) != 0)
	{
		fprintf(stderr, "setup failed\n");
		return 2;
	}
	page_size = PSZ;
	segment_size = 1024 * 1024;
	flush_pages = 1;
	segment_gc_enabled = 0;
	cache_pages = 0;
	ps_nshards = 1;
	use_layers = 1;
	if (ps_core_open(store) != 0)
	{
		fprintf(stderr, "core open failed\n");
		return 2;
	}
	memset(page, 0xA5, sizeof(page));
	memcpy(page, &lsn_hi, sizeof(lsn_hi));
	memcpy(page + sizeof(lsn_hi), &lsn_lo, sizeof(lsn_lo));
	ps_lock_shard_wr(ps_shard_of(&key));
	check(append_page(0, &key, 0, page, 0, NULL) == 0,
		  "write and flush an image layer");
	ps_unlock_shard(ps_shard_of(&key));
	check(ps_layer_map.nlayers == 1, "flush created one layer");
	check(ps_core_maintenance() == 1, "idle maintenance starts one layer upload");
	clock_gettime(CLOCK_MONOTONIC, &deadline);
	deadline.tv_sec += 5;
	for (;;)
	{
		struct timespec now;

		layer = ps_layer_map.nlayers == 1 ? &ps_layer_map.layers[0] : NULL;
		if (layer != NULL && layer->remote_durable)
			break;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec > deadline.tv_sec ||
			(now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec))
			break;
		ps_core_maintenance();
		usleep(1000);
	}
	layer = ps_layer_map.nlayers == 1 ? &ps_layer_map.layers[0] : NULL;
	layer_id = layer ? layer->layer_id : 0;
	check(layer != NULL && layer->remote_durable &&
		  ps_layer_store->layer_exists_remote(layer) == 1,
		  "uploaded layer is durably recorded and present remotely");
	check(ps_core_maintenance() == 1 &&
		  !layer->locations[0].available &&
		  ps_layer_store->layer_exists_local(layer->layer_id) == 0,
		  "next idle pass evicts the remote-durable local layer");
	ps_core_close();
	ps_layer_store->close();
	check(ps_layer_store->open(store) == 0 && ps_manifest_open(store) == 0 &&
		  ps_manifest_replay(&ps_layer_map) == 0,
		  "replay a remote-only layer after restart");
	layer = find_layer(layer_id);
	check(layer != NULL && !layer->locations[0].available &&
		  ps_image_layer_lookup(layer, &key, 0, 100, 0, out, PSZ, NULL, NULL) == 1 &&
		  memcmp(out, page, PSZ) == 0 &&
		  ps_layer_store->layer_exists_local(layer->layer_id) == 1,
		  "remote-only layer downloads into the local cache after restart");
	ps_manifest_close();
	ps_layer_store->close();
	unsetenv("PAGESTORE_OBJECT_DIR");
	printf("pagestore_tiering_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
