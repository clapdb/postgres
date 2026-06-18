/*-------------------------------------------------------------------------
 *
 * pagestore_layer_store.c
 *	  Local implementation of immutable layer byte access.
 *
 * Object storage will be added behind this interface.  Phase 1 only supports
 * local files and returns "unsupported" for remote operations.
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pagestore_layer_store.h"

static char layer_dir[2048];

const PsLayerStore *ps_layer_store = &PsLayerStoreLocal;

static int
local_open(const char *store_dir)
{
	int			n;

	n = snprintf(layer_dir, sizeof(layer_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(layer_dir))
		return -1;
	return 0;
}

static void
local_close(void)
{
	layer_dir[0] = '\0';
}

static int
local_layer_path(uint64_t layer_id, char *buf, size_t buflen)
{
	int			n;

	n = snprintf(buf, buflen, "%s/layer_%016llx",
				 layer_dir, (unsigned long long) layer_id);
	if (n < 0 || (size_t) n >= buflen)
		return -1;
	return 0;
}

static int
local_fsync_dir(void)
{
	int			fd;
	int			rc;

	fd = open(layer_dir, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	return rc;
}

static int
local_create_local_layer(uint64_t layer_id, char *uri, uint32_t uri_len)
{
	char		path[4096];
	int			fd;
	int			n;

	if (local_layer_path(layer_id, path, sizeof(path)) != 0)
		return -1;
	fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
		return -1;
	close(fd);
	if (local_fsync_dir() != 0)
	{
		unlink(path);
		return -1;
	}
	n = snprintf(uri, uri_len, "%s", path);
	if (n < 0 || (uint32_t) n >= uri_len)
	{
		unlink(path);
		return -1;
	}
	return 0;
}

static int
local_write_local_layer(uint64_t layer_id, const void *buf, uint64_t len)
{
	char		path[4096];
	int			fd;
	const char *p = buf;
	uint64_t	done = 0;

	if (local_layer_path(layer_id, path, sizeof(path)) != 0)
		return -1;
	fd = open(path, O_WRONLY);
	if (fd < 0)
		return -1;
	while (done < len)
	{
		ssize_t		w = write(fd, p + done, (size_t) (len - done));

		if (w <= 0)
		{
			close(fd);
			return -1;
		}
		done += (uint64_t) w;
	}
	close(fd);
	return 0;
}

static int
local_seal_local_layer(uint64_t layer_id)
{
	char		path[4096];
	int			fd;
	int			rc;

	if (local_layer_path(layer_id, path, sizeof(path)) != 0)
		return -1;
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	return rc;
}

static int
local_read_layer_block(const PsLayerDesc *layer, uint64_t off,
					   void *buf, uint32_t len)
{
	const char *path = NULL;
	int			fd;
	ssize_t		n;
	uint32_t	nlocs;

	nlocs = layer->location_count;
	if (nlocs > PS_LAYER_MAX_LOCATIONS)
		return -1;

	for (uint32_t i = 0; i < nlocs; i++)
	{
		if ((layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			 layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD) &&
			layer->locations[i].available)
		{
			path = layer->locations[i].uri;
			break;
		}
	}

	if (path == NULL)
		return -1;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	n = pread(fd, buf, len, (off_t) off);
	close(fd);
	return n == (ssize_t) len ? 0 : -1;
}

static int
local_unsupported_layer_op(const PsLayerDesc *layer)
{
	(void) layer;
	errno = ENOTSUP;
	return -1;
}

static int
local_delete_local_layer(const PsLayerDesc *layer)
{
	int			rc = 0;
	int			unlinked = 0;
	uint32_t	nlocs;

	nlocs = layer->location_count;
	if (nlocs > PS_LAYER_MAX_LOCATIONS)
		return -1;

	for (uint32_t i = 0; i < nlocs; i++)
	{
		if ((layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			 layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD) &&
			layer->locations[i].available)
		{
			if (unlink(layer->locations[i].uri) != 0 && errno != ENOENT)
				rc = -1;
			else
				unlinked = 1;
		}
	}
	if (unlinked && local_fsync_dir() != 0)
		rc = -1;
	return rc;
}

static int
local_layer_exists_remote(const PsLayerDesc *layer)
{
	(void) layer;
	return 0;
}

const PsLayerStore PsLayerStoreLocal = {
	.name = "local",
	.open = local_open,
	.close = local_close,
	.create_local_layer = local_create_local_layer,
	.write_local_layer = local_write_local_layer,
	.seal_local_layer = local_seal_local_layer,
	.read_layer_block = local_read_layer_block,
	.upload_layer = local_unsupported_layer_op,
	.download_layer = local_unsupported_layer_op,
	.delete_local_layer = local_delete_local_layer,
	.delete_remote_layer = local_unsupported_layer_op,
	.layer_exists_remote = local_layer_exists_remote,
};
