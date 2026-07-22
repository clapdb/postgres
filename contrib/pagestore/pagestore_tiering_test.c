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

int
main(void)
{
	char		store[] = "/tmp/pstieringstoreXXXXXX";
	char		objects[] = "/tmp/pstieringobjectsXXXXXX";
	unsigned char page[PSZ];
	PsKey		key = {1, 1, 1, 0, PS_KLASS_RELATION};
	uint32_t	lsn_hi = 0, lsn_lo = 100;
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
	check(layer != NULL && layer->remote_durable &&
		  ps_layer_store->layer_exists_remote(layer) == 1,
		  "uploaded layer is durably recorded and present remotely");
	ps_core_close();
	ps_layer_store->close();
	unsetenv("PAGESTORE_OBJECT_DIR");
	printf("pagestore_tiering_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
