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
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_layer_store.h"

static char layer_dir[2048];

/*
 * Object tier (LSM phase 4).  A configured object store is, for now, a local
 * directory standing in for a remote bucket: "upload" copies a sealed layer file
 * into it keyed by layer id, "download" copies it back, keyed the same way.  A
 * real provider (S3, ...) drops in behind these same four ops.  Empty until
 * ps_layer_store_set_object_dir() configures it; the remote ops then return
 * ENOTSUP, so a daemon without an object tier behaves exactly as before.
 */
static char object_dir[2048];

const PsLayerStore *ps_layer_store = &PsLayerStoreLocal;

int
ps_layer_store_set_object_dir(const char *dir)
{
	if (dir == NULL)
	{
		object_dir[0] = '\0';
		return 0;
	}
	if ((size_t) snprintf(object_dir, sizeof(object_dir), "%s", dir)
		>= sizeof(object_dir))
	{
		object_dir[0] = '\0';	/* reject rather than store a truncated path */
		return -1;
	}
	return 0;
}

static int
object_path(uint64_t layer_id, char *buf, size_t buflen)
{
	int			n;

	if (object_dir[0] == '\0')
		return -1;				/* no object tier configured */
	n = snprintf(buf, buflen, "%s/obj_%016llx",
				 object_dir, (unsigned long long) layer_id);
	if (n < 0 || (size_t) n >= buflen)
		return -1;
	return 0;
}

/*
 * Copy src -> dst durably.  Layer copies are immutable, so never truncate an
 * existing dst in place: write to "<dst>.tmp", fsync it, then atomically rename
 * it over dst (single writer per layer id, so a fixed temp suffix is safe).  A
 * crash leaves either the old complete copy or a stale .tmp, never a truncated
 * dst.  Used for upload/download (the object store is a local directory here).
 */
static int
copy_file(const char *src, const char *dst)
{
	int			in,
				out,
				rc = 0;
	char		tmp[4096];
	char		buf[1 << 16];
	ssize_t		r;

	if ((size_t) snprintf(tmp, sizeof(tmp), "%s.tmp", dst) >= sizeof(tmp))
		return -1;
	in = open(src, O_RDONLY);
	if (in < 0)
		return -1;
	out = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (out < 0)
	{
		close(in);
		return -1;
	}
	while ((r = read(in, buf, sizeof(buf))) > 0)
	{
		ssize_t		off = 0;

		while (off < r)
		{
			ssize_t		w = write(out, buf + off, (size_t) (r - off));

			if (w <= 0)
			{
				rc = -1;
				goto done;
			}
			off += w;
		}
	}
	if (r < 0)
		rc = -1;
done:
	if (rc == 0 && fsync(out) != 0)
		rc = -1;
	close(in);
	close(out);
	if (rc == 0 && rename(tmp, dst) != 0)
		rc = -1;
	if (rc != 0)
		unlink(tmp);			/* don't leave a partial temp behind */
	return rc;
}

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
object_fsync_dir(void)
{
	int			fd;
	int			rc;

	fd = open(object_dir, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	return rc;
}

/* Upload the sealed local layer into the object store (copy + durable). */
static int
local_upload_layer(const PsLayerDesc *layer)
{
	char		local[4096],
				obj[4096];

	if (local_layer_path(layer->layer_id, local, sizeof(local)) != 0)
		return -1;
	if (object_path(layer->layer_id, obj, sizeof(obj)) != 0)
	{
		errno = ENOTSUP;		/* no object tier configured */
		return -1;
	}
	if (copy_file(local, obj) != 0)
		return -1;
	return object_fsync_dir();
}

/*
 * Download a remote-durable layer back to its canonical local path and restore a
 * usable local location, so readers (which require an available local location,
 * with its size) can find it even after a durable eviction marked the old local
 * copy unavailable.  Reuses an existing local-tier slot if present, else appends
 * one; size comes from the downloaded file.
 */
static int
local_download_layer(PsLayerDesc *layer)
{
	char		local[4096],
				obj[4096];
	struct stat st;
	PsLayerLocation *loc = NULL;
	bool		appended = false;

	if (local_layer_path(layer->layer_id, local, sizeof(local)) != 0)
		return -1;
	if (strlen(local) >= PS_LAYER_URI_MAX)
		return -1;				/* would not fit a location uri */
	if (object_path(layer->layer_id, obj, sizeof(obj)) != 0)
	{
		errno = ENOTSUP;
		return -1;
	}
	if (copy_file(obj, local) != 0)
		return -1;
	if (local_fsync_dir() != 0 || stat(local, &st) != 0)
		return -1;

	/* find an existing local-tier slot to restore, else append one */
	for (uint32_t i = 0; i < layer->location_count &&
		 i < PS_LAYER_MAX_LOCATIONS; i++)
		if (layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD)
		{
			loc = &layer->locations[i];
			break;
		}
	if (loc == NULL)
	{
		if (layer->location_count >= PS_LAYER_MAX_LOCATIONS)
			return -1;
		loc = &layer->locations[layer->location_count++];
		loc->tier = PS_LAYER_TIER_LOCAL_HOT;
		loc->generation = 0;
		appended = true;
	}
	if ((size_t) snprintf(loc->uri, sizeof(loc->uri), "%s", local)
		>= sizeof(loc->uri))
	{
		if (appended)
			layer->location_count--;
		return -1;
	}
	loc->size = (uint64_t) st.st_size;
	loc->available = true;
	return 0;
}

/* Remove the layer's object-store copy (idempotent). */
static int
local_delete_remote_layer(const PsLayerDesc *layer)
{
	char		obj[4096];

	if (object_path(layer->layer_id, obj, sizeof(obj)) != 0)
	{
		errno = ENOTSUP;
		return -1;
	}
	if (unlink(obj) != 0 && errno != ENOENT)
		return -1;
	return object_fsync_dir();
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

/* 1 if the layer has an object-store copy, 0 if not (or no tier configured). */
static int
local_layer_exists_remote(const PsLayerDesc *layer)
{
	char		obj[4096];
	struct stat st;

	if (object_path(layer->layer_id, obj, sizeof(obj)) != 0)
		return 0;
	return stat(obj, &st) == 0 ? 1 : 0;
}

const PsLayerStore PsLayerStoreLocal = {
	.name = "local",
	.open = local_open,
	.close = local_close,
	.create_local_layer = local_create_local_layer,
	.write_local_layer = local_write_local_layer,
	.seal_local_layer = local_seal_local_layer,
	.read_layer_block = local_read_layer_block,
	.upload_layer = local_upload_layer,
	.download_layer = local_download_layer,
	.delete_local_layer = local_delete_local_layer,
	.delete_remote_layer = local_delete_remote_layer,
	.layer_exists_remote = local_layer_exists_remote,
};
