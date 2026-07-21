/*-------------------------------------------------------------------------
 *
 * pagestore_layer_store_test.c
 *    Standalone tests for the filesystem-backed immutable layer object tier.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_layer_store.h"

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

static int
file_matches(const char *path, const char *want)
{
	char		buf[128];
	int			fd;
	ssize_t		n;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return 0;
	n = read(fd, buf, sizeof(buf));
	close(fd);
	return n == (ssize_t) strlen(want) && memcmp(buf, want, (size_t) n) == 0;
}

int
main(void)
{
	char		local_dir[] = "/tmp/pslayerstorelocalXXXXXX";
	char		other_local_dir[] = "/tmp/pslayerstoreotherXXXXXX";
	char		object_dir[] = "/tmp/pslayerstoreobjectXXXXXX";
	char		owner_path[sizeof(object_dir) + 32];
	char		local_uri[PS_LAYER_URI_MAX];
	char		remote_uri[PS_LAYER_URI_MAX];
	const char *contents = "sealed layer object bytes";
	PsLayerDesc layer;

	if (mkdtemp(local_dir) == NULL || mkdtemp(other_local_dir) == NULL ||
		mkdtemp(object_dir) == NULL)
	{
		fprintf(stderr, "setup failed\n");
		return 2;
	}
	check(setenv("PAGESTORE_OBJECT_DIR", local_dir, 1) == 0 &&
		  ps_layer_store->open(local_dir) != 0,
		  "reject an object directory that aliases the local store");
	ps_layer_store->close();
	check(setenv("PAGESTORE_OBJECT_DIR", object_dir, 1) == 0 &&
		  ps_layer_store->open(local_dir) == 0,
		  "open exclusive object directory");
	ps_layer_store->close();
	check(ps_layer_store->open(other_local_dir) != 0,
		  "reject object directory owned by another store");
	ps_layer_store->close();
	if (ps_layer_store->open(local_dir) != 0)
	{
		fprintf(stderr, "could not reopen object directory\n");
		return 2;
	}

	memset(&layer, 0, sizeof(layer));
	layer.layer_id = (3ULL << 48) | 17;
	layer.location_count = 1;
	layer.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
	layer.locations[0].available = true;
	check(ps_layer_store->create_local_layer(layer.layer_id, local_uri,
												 sizeof(local_uri)) == 0,
		  "create local layer");
	check(ps_layer_store->write_local_layer(layer.layer_id, contents,
												strlen(contents)) == 0 &&
		  ps_layer_store->seal_local_layer(layer.layer_id) == 0,
		  "write and seal local layer");
	snprintf(layer.locations[0].uri, sizeof(layer.locations[0].uri), "%s", local_uri);
	layer.locations[0].size = strlen(contents);

	check(ps_layer_store->remote_uri(layer.layer_id, remote_uri,
											 sizeof(remote_uri)) == 0,
		  "derive remote object URI");
	layer.locations[0].size++;
	check(ps_layer_store->upload_layer(&layer) != 0,
		  "reject upload whose source size differs from layer metadata");
	layer.locations[0].size--;
	check(ps_layer_store->upload_layer(&layer) == 0,
		  "upload local layer atomically");
	check(ps_layer_store->upload_layer(&layer) == 0,
		  "re-upload matching object is idempotent");
	layer.locations[1].tier = PS_LAYER_TIER_REMOTE_OBJECT;
	layer.locations[1].available = true;
	layer.locations[1].size = layer.locations[0].size;
	snprintf(layer.locations[1].uri, sizeof(layer.locations[1].uri), "%s", remote_uri);
	layer.location_count = 2;
	check(ps_layer_store->layer_exists_remote(&layer) == 1 &&
		  file_matches(remote_uri, contents),
		  "uploaded object is present and complete");

	check(unlink(local_uri) == 0, "simulate local layer eviction");
	check(ps_layer_store->download_layer(&layer) == 0 &&
		  file_matches(local_uri, contents),
		  "download restores the complete local layer");
	check(ps_layer_store->delete_remote_layer(&layer) == 0,
		  "delete remote object");
	check(ps_layer_store->delete_remote_layer(&layer) == 0 &&
		  ps_layer_store->layer_exists_remote(&layer) == 0,
		  "remote delete is idempotent");

	ps_layer_store->close();
	unsetenv("PAGESTORE_OBJECT_DIR");
	snprintf(owner_path, sizeof(owner_path), "%s/.pagestore-owner", object_dir);
	unlink(owner_path);
	unlink(local_uri);
	rmdir(local_dir);
	rmdir(other_local_dir);
	rmdir(object_dir);
	printf("pagestore_layer_store_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
