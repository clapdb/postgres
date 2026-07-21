/*-------------------------------------------------------------------------
 *
 * pagestore_layer_store.c
 *	  Local implementation of immutable layer byte access.
 *
 * Local files are always available.  When PAGESTORE_OBJECT_DIR names a local
 * directory, it also serves as a filesystem-backed object tier for integration
 * testing and the first tiering implementation.
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_layer_store.h"

static char layer_dir[2048];
static char object_dir[2048];

static int layer_id_shard(uint64_t layer_id);
static int fsync_dir(const char *dir);
static int cleanup_stale_copy_temps(const char *dir);
static const PsLayerLocation *remote_location(const PsLayerDesc *layer);
static int local_download_layer(const PsLayerDesc *layer);

/*
 * Object directories are deliberately single-store resources.  Layer IDs are
 * allocated by each store, so sharing a directory would otherwise make two
 * stores publish (and later delete) the same object names.
 */
static int
claim_object_dir(void)
{
	char		path[4096];
	char		owner_tmp[4096];
	char		idpath[4096];
	char		idtmp[4096];
	char		owner[128];
	char		got[sizeof(owner)];
	int		fd;
	int		n;
	int		owner_len;
	ssize_t		len;

	n = snprintf(idpath, sizeof(idpath), "%s/.pagestore-store-id", layer_dir);
	if (n < 0 || (size_t) n >= sizeof(idpath))
		return -1;
	fd = open(idpath, O_RDONLY);
	if (fd >= 0)
	{
		len = read(fd, owner, sizeof(owner) - 1);
		close(fd);
		if (len <= 0)
			return -1;
		owner[len] = '\0';
	}
	else if (errno == ENOENT)
	{
		unsigned char random[16];
		int			rfd = open("/dev/urandom", O_RDONLY);

		if (rfd < 0 || read(rfd, random, sizeof(random)) != sizeof(random))
		{
			if (rfd >= 0)
				close(rfd);
			return -1;
		}
		close(rfd);
		owner_len = 0;
		for (int i = 0; i < (int) sizeof(random); i++)
			owner_len += snprintf(owner + owner_len, sizeof(owner) - (size_t) owner_len, "%02x", random[i]);
		if (owner_len < 0 || (size_t) owner_len >= sizeof(owner))
			return -1;
		n = snprintf(idtmp, sizeof(idtmp), "%s.tmp.%ld", idpath, (long) getpid());
		if (n < 0 || (size_t) n >= sizeof(idtmp))
			return -1;
		fd = open(idtmp, O_WRONLY | O_CREAT | O_EXCL, 0600);
		if (fd < 0 || write(fd, owner, (size_t) owner_len) != owner_len || fsync(fd) != 0)
		{
			if (fd >= 0)
				close(fd);
			unlink(idtmp);
			return -1;
		}
		if (close(fd) != 0 || rename(idtmp, idpath) != 0 || fsync_dir(layer_dir) != 0)
		{
			unlink(idtmp);
			return -1;
		}
	}
	else
		return -1;
	n = snprintf(path, sizeof(path), "%s/.pagestore-owner", object_dir);
	if (n < 0 || (size_t) n >= sizeof(path))
		return -1;
	if (access(path, F_OK) == 0)
		goto owner_exists;
	if (errno != ENOENT)
		return -1;
	n = snprintf(owner_tmp, sizeof(owner_tmp), "%s.tmp.%ld", path, (long) getpid());
	if (n < 0 || (size_t) n >= sizeof(owner_tmp))
		return -1;
	fd = open(owner_tmp, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd >= 0)
	{
		owner_len = (int) strlen(owner);
		if (write(fd, owner, (size_t) owner_len) != owner_len || fsync(fd) != 0)
		{
			close(fd);
			unlink(owner_tmp);
			return -1;
		}
		if (close(fd) != 0 || rename(owner_tmp, path) != 0 ||
			fsync_dir(object_dir) != 0)
		{
			unlink(owner_tmp);
			return -1;
		}
		return 0;
	}
owner_exists:
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	len = read(fd, got, sizeof(got) - 1);
	close(fd);
	if (len < 0)
		return -1;
	got[len] = '\0';
	return strcmp(got, owner) == 0 ? 0 : -1;
}

const PsLayerStore *ps_layer_store = &PsLayerStoreLocal;

static int
local_open(const char *store_dir)
{
	const char *configured_object_dir;
	struct stat store_st;
	struct stat object_st;
	int			n;

	n = snprintf(layer_dir, sizeof(layer_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(layer_dir))
		return -1;
	if (stat(layer_dir, &store_st) != 0 || !S_ISDIR(store_st.st_mode))
		return -1;
	/* Reap interrupted copies once at provider startup, not on every copy. */
	if (cleanup_stale_copy_temps(layer_dir) != 0)
		return -1;
	object_dir[0] = '\0';
	configured_object_dir = getenv("PAGESTORE_OBJECT_DIR");
	if (configured_object_dir == NULL || configured_object_dir[0] == '\0')
		return 0;
	if (realpath(configured_object_dir, object_dir) == NULL ||
		stat(object_dir, &object_st) != 0 || !S_ISDIR(object_st.st_mode) ||
		(store_st.st_dev == object_st.st_dev &&
		 store_st.st_ino == object_st.st_ino) ||
		claim_object_dir() != 0 || cleanup_stale_copy_temps(object_dir) != 0)
		return -1;
	return 0;
}

static void
local_close(void)
{
	layer_dir[0] = '\0';
	object_dir[0] = '\0';
}

static int
object_layer_path(uint64_t layer_id, char *buf, size_t buflen)
{
	int			n;

	if (object_dir[0] == '\0')
	{
		errno = ENOTSUP;
		return -1;
	}
	n = snprintf(buf, buflen, "%s/layer_%d_%016llx", object_dir,
				 layer_id_shard(layer_id), (unsigned long long) layer_id);
	if (n < 0 || (size_t) n >= buflen)
		return -1;
	return 0;
}

static int
layer_id_shard(uint64_t layer_id)
{
	return (int) ((layer_id >> 48) & 0xFFFF);
}

static int
local_layer_path(uint64_t layer_id, char *buf, size_t buflen)
{
	int			n;

	n = snprintf(buf, buflen, "%s/layer_%d_%016llx",
				 layer_dir, layer_id_shard(layer_id),
				 (unsigned long long) layer_id);
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
fsync_dir(const char *dir)
{
	int			fd;
	int			rc;

	fd = open(dir, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	return rc;
}

static int
files_equal(const char *left, const char *right)
{
	unsigned char lbuf[8192], rbuf[8192];
	struct stat lst, rst;
	int			lfd = -1, rfd = -1;
	int			rc = -1;

	if (stat(left, &lst) != 0 || stat(right, &rst) != 0 ||
		lst.st_size != rst.st_size)
		return 0;
	lfd = open(left, O_RDONLY);
	rfd = open(right, O_RDONLY);
	if (lfd < 0 || rfd < 0)
		goto done;
	for (;;)
	{
		ssize_t ln = read(lfd, lbuf, sizeof(lbuf));
		ssize_t rn = read(rfd, rbuf, sizeof(rbuf));

		if (ln < 0 || rn < 0 || ln != rn)
			goto done;
		if (ln == 0)
		{
			rc = 1;
			goto done;
		}
		if (memcmp(lbuf, rbuf, (size_t) ln) != 0)
			goto done;
	}

done:
	if (lfd >= 0)
		close(lfd);
	if (rfd >= 0)
		close(rfd);
	return rc;
}

static int
cleanup_stale_copy_temps(const char *dir)
{
	DIR			*d;
	struct dirent *ent;
	char		path[4096];
	char		*tmp;
	char		*end;
	long		pid;
	int			n;

	d = opendir(dir);
	if (d == NULL)
		return -1;
	while ((ent = readdir(d)) != NULL)
	{
		if (strncmp(ent->d_name, "layer_", strlen("layer_")) != 0 ||
			(tmp = strstr(ent->d_name, ".tmp.")) == NULL)
			continue;
		errno = 0;
		pid = strtol(tmp + strlen(".tmp."), &end, 10);
		if (errno != 0 || end == tmp + strlen(".tmp.") || *end != '.' || pid <= 0 ||
			(kill((pid_t) pid, 0) != -1 || errno != ESRCH))
			continue;
		n = snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
		if (n >= 0 && (size_t) n < sizeof(path))
			unlink(path);
	}
	closedir(d);
	return 0;
}

static int
copy_file_atomic(const char *source, const char *destination, const char *dir)
{
	char		tmp[4096] = "";
	unsigned char buf[8192];
	int			sfd = -1, dfd = -1;
	int			attempt;
	int			n;
	int			rc = -1;

	if (access(destination, F_OK) == 0)
		return files_equal(source, destination) == 1 && fsync_dir(dir) == 0 ? 0 : -1;
	sfd = open(source, O_RDONLY);
	if (sfd < 0)
		goto done;
	for (attempt = 0; attempt < 100; attempt++)
	{
		n = snprintf(tmp, sizeof(tmp), "%s.tmp.%ld.%d", destination,
					 (long) getpid(), attempt);
		if (n < 0 || (size_t) n >= sizeof(tmp))
			goto done;
		dfd = open(tmp, O_WRONLY | O_CREAT | O_EXCL, 0600);
		if (dfd >= 0 || errno != EEXIST)
			break;
	}
	if (dfd < 0)
		goto done;
	for (;;)
	{
		ssize_t nr = read(sfd, buf, sizeof(buf));
		size_t done_bytes = 0;

		if (nr < 0)
			goto done;
		if (nr == 0)
			break;
		while (done_bytes < (size_t) nr)
		{
			ssize_t nw = write(dfd, buf + done_bytes, (size_t) nr - done_bytes);

			if (nw <= 0)
				goto done;
			done_bytes += (size_t) nw;
		}
	}
	if (fsync(dfd) != 0 || close(dfd) != 0)
		goto done;
	dfd = -1;
	if (link(tmp, destination) != 0)
	{
		if (errno != EEXIST || files_equal(source, destination) != 1)
			goto done;
	}
	if (fsync_dir(dir) != 0)
		goto done;
	if (unlink(tmp) != 0)
		goto done;
	if (fsync_dir(dir) != 0)
		goto done;
	rc = 0;

done:
	if (sfd >= 0)
		close(sfd);
	if (dfd >= 0)
		close(dfd);
	if (rc != 0 && tmp[0] != '\0')
		unlink(tmp);
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
local_layer_exists(uint64_t layer_id)
{
	char		path[4096];

	if (local_layer_path(layer_id, path, sizeof(path)) != 0)
		return -1;
	if (access(path, F_OK) == 0)
		return 1;
	return errno == ENOENT ? 0 : -1;
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
	{
		char		cached[4096];

		if (local_layer_path(layer->layer_id, cached, sizeof(cached)) != 0)
			return -1;
		if (access(cached, R_OK) != 0 &&
			(remote_location(layer) == NULL || local_download_layer(layer) != 0))
			return -1;
		path = cached;
	}

	fd = open(path, O_RDONLY);
	if (fd < 0 && remote_location(layer) != NULL)
	{
		char cached[4096];

		if (local_layer_path(layer->layer_id, cached, sizeof(cached)) == 0 &&
			local_download_layer(layer) == 0)
			fd = open(cached, O_RDONLY);
	}
	if (fd < 0)
		return -1;
	n = pread(fd, buf, len, (off_t) off);
	close(fd);
	return n == (ssize_t) len ? 0 : -1;
}

static const PsLayerLocation *
local_location(const PsLayerDesc *layer)
{
	for (uint32_t i = 0; i < layer->location_count; i++)
		if ((layer->locations[i].tier == PS_LAYER_TIER_LOCAL_HOT ||
			 layer->locations[i].tier == PS_LAYER_TIER_LOCAL_COLD) &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

static int
local_remote_uri(uint64_t layer_id, char *uri, uint32_t uri_len)
{
	char		path[4096];
	int			n;

	if (object_layer_path(layer_id, path, sizeof(path)) != 0)
		return -1;
	n = snprintf(uri, uri_len, "%s", path);
	return n < 0 || (uint32_t) n >= uri_len ? -1 : 0;
}

static const PsLayerLocation *
remote_location(const PsLayerDesc *layer)
{
	for (uint32_t i = 0; i < layer->location_count; i++)
		if (layer->locations[i].tier == PS_LAYER_TIER_REMOTE_OBJECT &&
			layer->locations[i].available)
			return &layer->locations[i];
	return NULL;
}

static int
local_upload_layer(const PsLayerDesc *layer)
{
	const PsLayerLocation *source;
	const PsLayerLocation *published;
	char		remote[4096];

	source = local_location(layer);
	published = remote_location(layer);
	if (source == NULL ||
		(published == NULL && object_layer_path(layer->layer_id, remote, sizeof(remote)) != 0) ||
		(published != NULL && snprintf(remote, sizeof(remote), "%s", published->uri) >= (int) sizeof(remote)))
		return -1;
	/* The manifest's declared length is the minimum identity available here. */
	{
		struct stat st;

		if (stat(source->uri, &st) != 0 || st.st_size < 0 ||
			(uint64_t) st.st_size != source->size)
			return -1;
	}
	if (copy_file_atomic(source->uri, remote, object_dir) != 0)
		return -1;
	{
		struct stat st;

		return stat(remote, &st) == 0 && st.st_size >= 0 &&
			(uint64_t) st.st_size == source->size &&
			files_equal(source->uri, remote) == 1 ? 0 : -1;
	}
}

static int
local_download_layer(const PsLayerDesc *layer)
{
	const PsLayerLocation *source;
	char		local[4096];
	struct stat st;

	source = remote_location(layer);
	if (source == NULL || local_layer_path(layer->layer_id, local, sizeof(local)) != 0)
		return -1;
	if (copy_file_atomic(source->uri, local, layer_dir) != 0)
		return -1;
	if (stat(local, &st) != 0 || st.st_size < 0 ||
		(uint64_t) st.st_size != source->size)
	{
		unlink(local);
		return -1;
	}
	if (layer->kind == PS_LAYER_IMAGE)
	{
		PsImgFooter foot;
		int fd = open(local, O_RDONLY);

		if (st.st_size < (off_t) sizeof(foot) || fd < 0 ||
			pread(fd, &foot, sizeof(foot),
						  st.st_size - sizeof(foot)) != sizeof(foot))
		{
			if (fd >= 0)
				close(fd);
			unlink(local);
			return -1;
		}
		if (close(fd) != 0 ||
			ps_image_layer_verify_data(layer, foot.page_size) != 0)
		{
			unlink(local);
			return -1;
		}
	}
	return 0;
}

static int
local_delete_remote_layer(const PsLayerDesc *layer)
{
	const PsLayerLocation *location;
	char		expected[4096];

	location = remote_location(layer);
	if (location == NULL ||
		object_layer_path(layer->layer_id, expected, sizeof(expected)) != 0 ||
		strcmp(location->uri, expected) != 0)
		return -1;
	if (unlink(expected) != 0 && errno != ENOENT)
		return -1;
	return fsync_dir(object_dir);
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
	/* DROP_LOCAL is durable before unlink.  Retry the canonical physical file
	 * even when the manifest has already marked its local location unavailable. */
	if (!unlinked)
	{
		char	path[4096];

		if (local_layer_path(layer->layer_id, path, sizeof(path)) != 0)
			return -1;
		if (unlink(path) == 0 || errno == ENOENT)
			unlinked = 1;
		else
			rc = -1;
	}
	if (unlinked && local_fsync_dir() != 0)
		rc = -1;
	return rc;
}

static int
local_layer_exists_remote(const PsLayerDesc *layer)
{
	const PsLayerLocation *location;
	char		expected[4096];

	location = remote_location(layer);
	if (location == NULL ||
		object_layer_path(layer->layer_id, expected, sizeof(expected)) != 0 ||
		strcmp(location->uri, expected) != 0)
		return 0;
	if (access(expected, F_OK) == 0)
		return 1;
	return errno == ENOENT ? 0 : -1;
}

const PsLayerStore PsLayerStoreLocal = {
	.name = "local",
	.open = local_open,
	.close = local_close,
	.create_local_layer = local_create_local_layer,
	.layer_exists_local = local_layer_exists,
	.write_local_layer = local_write_local_layer,
	.seal_local_layer = local_seal_local_layer,
	.remote_uri = local_remote_uri,
	.read_layer_block = local_read_layer_block,
	.upload_layer = local_upload_layer,
	.download_layer = local_download_layer,
	.delete_local_layer = local_delete_local_layer,
	.delete_remote_layer = local_delete_remote_layer,
	.layer_exists_remote = local_layer_exists_remote,
};
