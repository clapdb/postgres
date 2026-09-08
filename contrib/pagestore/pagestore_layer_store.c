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
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_fault.h"
#include "pagestore_layer_store.h"
#include "pagestore_store_owner.h"

static uint32_t layer_page_size = PS_DEFAULT_PAGE_SIZE;

void
ps_layer_store_set_page_size(uint32_t value)
{
	layer_page_size = value;
}

/* realpath(3) requires a PATH_MAX-sized destination when a caller supplies
 * one; keep provider roots large enough for canonicalized object directories. */
static char layer_dir[4096];
static char object_dir[4096];
static PsStoreOwner *layer_owner;

static int layer_id_shard(uint64_t layer_id);
static int fsync_dir(const char *dir);
static int cleanup_stale_copy_temps(const char *dir);
static const PsLayerLocation *remote_location(const PsLayerDesc *layer);
static int local_download_layer(const PsLayerDesc *layer);
static int local_refresh_layer_cache(const PsLayerDesc *layer);
static void local_close(void);

typedef struct LocalLayerCandidate
{
	char		name[NAME_MAX + 1];
	uint64_t	layer_id;
	dev_t		dev;
	ino_t		ino;
	off_t		size;
} LocalLayerCandidate;

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
		if (close(fd) != 0)
		{
			unlink(idtmp);
			return -1;
		}
		if (link(idtmp, idpath) != 0)
		{
			if (errno != EEXIST)
			{
				unlink(idtmp);
				return -1;
			}
			unlink(idtmp);
			fd = open(idpath, O_RDONLY);
			if (fd < 0)
				return -1;
			len = read(fd, owner, sizeof(owner) - 1);
			close(fd);
			if (len <= 0)
				return -1;
			owner[len] = '\0';
		}
		else if (unlink(idtmp) != 0 || fsync_dir(layer_dir) != 0)
			return -1;
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
		if (close(fd) != 0)
		{
			unlink(owner_tmp);
			return -1;
		}
		fd = -1;
		if (link(owner_tmp, path) != 0)
		{
			if (errno != EEXIST)
			{
				unlink(owner_tmp);
				return -1;
			}
		}
		else if (fsync_dir(object_dir) != 0)
		{
			unlink(path);
			unlink(owner_tmp);
			return -1;
		}
		if (unlink(owner_tmp) != 0 || fsync_dir(object_dir) != 0)
			return -1;
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
	int			save_errno;
	char		probe[PS_LAYER_URI_MAX];

	if (layer_owner != NULL)
	{
		/* Do not enter the process-global owner mutex from a fork child.  A
		 * provider inherited across fork must exec before it can be reopened. */
		if (ps_store_owner_require_current(layer_owner) != 0)
			return -1;
		local_close();
	}
	if (ps_store_owner_acquire(store_dir, &layer_owner) != 0)
		return -1;
	if (ps_store_owner_require_current(layer_owner) != 0)
	{
		goto fail;
	}
	n = snprintf(layer_dir, sizeof(layer_dir), "%s",
				 ps_store_owner_root(layer_owner));
	if (n < 0 || (size_t) n >= sizeof(layer_dir))
	{
		errno = ENAMETOOLONG;
		goto fail;
	}
	/* Reap interrupted copies once at provider startup, not on every copy. */
	if (cleanup_stale_copy_temps(layer_dir) != 0)
		goto fail;
	if (stat(layer_dir, &store_st) != 0 || !S_ISDIR(store_st.st_mode))
		goto fail;
	object_dir[0] = '\0';
	configured_object_dir = getenv("PAGESTORE_OBJECT_DIR");
	if (configured_object_dir == NULL || configured_object_dir[0] == '\0')
		return 0;
	if (realpath(configured_object_dir, object_dir) == NULL ||
		stat(object_dir, &object_st) != 0 || !S_ISDIR(object_st.st_mode) ||
		(store_st.st_dev == object_st.st_dev &&
		 store_st.st_ino == object_st.st_ino) ||
		snprintf(probe, sizeof(probe), "%s/layer_%d_%016llx", object_dir,
				 PS_MAX_CHANNELS - 1, (unsigned long long) UINT64_MAX) >=
		(int) sizeof(probe) ||
		claim_object_dir() != 0 || cleanup_stale_copy_temps(object_dir) != 0)
		goto fail;
	return 0;

fail:
	save_errno = errno;
	local_close();
	errno = save_errno;
	return -1;
}

static void
local_close(void)
{
	layer_dir[0] = '\0';
	object_dir[0] = '\0';
	if (layer_owner != NULL)
	{
		ps_store_owner_release(layer_owner);
		layer_owner = NULL;
	}
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
local_owner_current(void)
{
	if (layer_owner == NULL)
	{
		errno = EPERM;
		return 0;
	}
	return ps_store_owner_require_current(layer_owner) == 0;
}

static int
hex_digit(unsigned char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	return -1;
}

/* The writer emits layer_<decimal upper-16 shard>_<16 lowercase hex id>. */
static int
parse_canonical_layer_name(const char *name, uint64_t *layer_id)
{
	const char *p;
	const char *hex;
	char		*end;
	unsigned long shard;
	uint64_t id = 0;
	char		canonical[NAME_MAX + 1];

	if (strncmp(name, "layer_", 6) != 0)
		return 0;
	p = name + 6;
	if (*p == '\0' || (*p == '0' && p[1] >= '0' && p[1] <= '9'))
		return -1;
	errno = 0;
	shard = strtoul(p, &end, 10);
	if (errno != 0 || end == p || shard > 0xFFFF || *end != '_')
		return -1;
	hex = end + 1;
	if (strlen(hex) != 16)
		return -1;
	for (int i = 0; i < 16; i++)
	{
		int digit = hex_digit((unsigned char) hex[i]);

		if (digit < 0)
			return -1;
		id = (id << 4) | (uint64_t) digit;
	}
	if (((id >> 48) & UINT64_C(0xFFFF)) != shard ||
		snprintf(canonical, sizeof(canonical), "layer_%lu_%016llx", shard,
				 (unsigned long long) id) < 0 || strcmp(canonical, name) != 0)
		return -1;
	*layer_id = id;
	return 1;
}

/* A copy temp is still provider-owned namespace, even when its PID is alive
 * because that PID has since been reused.  Recovery must recognize the name
 * before applying canonical-layer validation, then leave it for the copier. */
static int
parse_copy_temp_name(const char *name, pid_t *pid_out)
{
	const char *tmp;
	const char *pid_text;
	const char *attempt_text;
	char		base[NAME_MAX + 1];
	char		canonical_suffix[64];
	char		*end;
	unsigned long attempt;
	long		pid;
	uint64_t	layer_id;
	size_t		base_len;

	if (strncmp(name, "layer_", 6) != 0)
		return 0;
	tmp = strstr(name, ".tmp.");
	if (tmp == NULL)
		return 0;
	base_len = (size_t) (tmp - name);
	if (base_len == 0 || base_len > NAME_MAX)
		return -1;
	memcpy(base, name, base_len);
	base[base_len] = '\0';
	if (parse_canonical_layer_name(base, &layer_id) != 1)
		return -1;

	pid_text = tmp + strlen(".tmp.");
	if (*pid_text < '0' || *pid_text > '9')
		return -1;
	errno = 0;
	pid = strtol(pid_text, &end, 10);
	if (errno == ERANGE || end == pid_text || pid <= 0 ||
		(pid_t) pid != pid || *end != '.')
		return -1;
	attempt_text = end + 1;
	if (*attempt_text < '0' || *attempt_text > '9')
		return -1;
	errno = 0;
	attempt = strtoul(attempt_text, &end, 10);
	if (errno == ERANGE || end == attempt_text || *end != '\0' ||
		attempt > UINT_MAX ||
		snprintf(canonical_suffix, sizeof(canonical_suffix), "%ld.%u", pid,
				 (unsigned int) attempt) < 0 ||
		strcmp(canonical_suffix, pid_text) != 0)
		return -1;
	(void) layer_id;
	(void) attempt;
	if (pid_out != NULL)
		*pid_out = (pid_t) pid;
	return 1;
}

static int
compare_layer_ids(const void *left, const void *right)
{
	const uint64_t a = *(const uint64_t *) left;
	const uint64_t b = *(const uint64_t *) right;

	return a < b ? -1 : (a > b ? 1 : 0);
}

static int
layer_map_has_id(const uint64_t *map_ids, size_t nmap_ids, uint64_t layer_id)
{
	if (nmap_ids == 0)
		return 0;
	return bsearch(&layer_id, map_ids, nmap_ids, sizeof(*map_ids),
				   compare_layer_ids) != NULL;
}

static int
canonicalize_local_layer_uri(PsLayerLocation *location,
							 const char *expected)
{
	char		resolved[4096];
	char		parent[PS_LAYER_URI_MAX];
	char		joined[4096];
	const char *slash;
	const char *basename;
	const char *expected_basename;
	struct stat st;
	struct stat parent_st;
	struct stat root_st;
	size_t		uri_len;
	size_t		parent_len;
	int		n;

	uri_len = strnlen(location->uri, sizeof(location->uri));
	if (uri_len == sizeof(location->uri))
	{
		errno = EINVAL;
		return -1;
	}
	slash = strrchr(location->uri, '/');
	if (slash == NULL)
	{
		errno = EINVAL;
		return -1;
	}
	parent_len = (size_t) (slash - location->uri);
	if (parent_len == 0)
		parent_len = 1;
	if (parent_len >= sizeof(parent))
	{
		errno = EINVAL;
		return -1;
	}
	memcpy(parent, location->uri, parent_len);
	if (slash == location->uri)
		parent[0] = '/';
	parent[parent_len] = '\0';
	basename = slash + 1;
	if (basename[0] == '\0')
	{
		errno = EINVAL;
		return -1;
	}
	expected_basename = strrchr(expected, '/') + 1;
	if (strcmp(basename, expected_basename) != 0 ||
		realpath(parent, resolved) == NULL)
	{
		errno = EINVAL;
		return -1;
	}
	if (stat(resolved, &parent_st) != 0 || stat(layer_dir, &root_st) != 0 ||
		parent_st.st_dev != root_st.st_dev || parent_st.st_ino != root_st.st_ino)
	{
		errno = EINVAL;
		return -1;
	}
	n = snprintf(joined, sizeof(joined), "%s/%s", resolved, basename);
	if (n < 0 || (size_t) n >= sizeof(joined) ||
		strcmp(joined, expected) != 0)
	{
		errno = EINVAL;
		return -1;
	}
	/* The leaf may be absent while a remote-durable or deleting layer is
	 * replayed.  If present, it must already be a non-symlink regular file. */
	if (lstat(location->uri, &st) == 0)
	{
		if (!S_ISREG(st.st_mode))
		{
			errno = EINVAL;
			return -1;
		}
	}
	else if (errno != ENOENT)
	{
		errno = EINVAL;
		return -1;
	}
	if (strlen(expected) >= sizeof(location->uri))
	{
		errno = EINVAL;
		return -1;
	}
	if (strcmp(location->uri, expected) != 0)
		snprintf(location->uri, sizeof(location->uri), "%s", expected);
	return 0;
}

static int
local_validate_layer_locations(PsLayerMap *map)
{
	if (map == NULL)
	{
		errno = EINVAL;
		return -1;
	}
	if (!local_owner_current())
		return -1;
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		PsLayerDesc *layer = &map->layers[i];
		char expected[4096];
		int local_count = 0;

		if (layer->location_count > PS_LAYER_MAX_LOCATIONS)
		{
			errno = EINVAL;
			return -1;
		}
		for (uint32_t j = 0; j < layer->location_count; j++)
		{
			PsLayerLocation *location = &layer->locations[j];

			switch (location->tier)
			{
				case PS_LAYER_TIER_LOCAL_HOT:
				case PS_LAYER_TIER_LOCAL_COLD:
					if (++local_count > 1 ||
						local_layer_path(layer->layer_id, expected,
										 sizeof(expected)) != 0 ||
						canonicalize_local_layer_uri(location,
												 expected) != 0)
					{
						errno = EINVAL;
						return -1;
					}
					break;
				case PS_LAYER_TIER_REMOTE_OBJECT:
					/* Object locations are not scanned or unlinked here, but a
					 * replayed location still needs a bounded nonempty URI. */
					if (location->uri[0] == '\0')
					{
						errno = EINVAL;
						return -1;
					}
					break;
				default:
					errno = EINVAL;
					return -1;
			}
		}
	}
	return 0;
}

static int
validate_layer_candidate(int dirfd, const LocalLayerCandidate *candidate)
{
	struct stat st;
	int		fd;

	if (fstatat(dirfd, candidate->name, &st, AT_SYMLINK_NOFOLLOW) != 0)
		return -1;
	if (!S_ISREG(st.st_mode) || st.st_nlink != 1 ||
		st.st_dev != candidate->dev || st.st_ino != candidate->ino)
	{
		errno = EINVAL;
		return -1;
	}
	fd = openat(dirfd, candidate->name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
	if (fd < 0)
		return -1;
	if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_nlink != 1 ||
		st.st_dev != candidate->dev || st.st_ino != candidate->ino)
	{
		int save_errno = errno;

		close(fd);
		errno = save_errno != 0 ? save_errno : EINVAL;
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	return 0;
}

static int
local_recover_local_layers(PsLayerMap *map)
{
	LocalLayerCandidate *candidates = NULL;
	uint64_t  *map_ids = NULL;
	uint32_t	ncandidates = 0;
	uint32_t	capacity = 0;
	DIR		*dir = NULL;
	struct dirent *ent;
	int		scanfd = -1;
	int		unlinkfd = -1;
	int		changed = 0;
	int		rc = -1;
	int		save_errno = 0;
	size_t		map_count = (size_t) 0;

	if (map == NULL || layer_dir[0] == '\0')
	{
		errno = EINVAL;
		return -1;
	}
	if (!local_owner_current())
		return -1;
	if (local_validate_layer_locations(map) != 0)
		return -1;
	map_count = (size_t) map->nlayers;
	if (map_count > 0)
	{
		/* On 64-bit builds uint32_t nlayers cannot overflow this allocation.
		 * Keep the check for narrower size_t targets without provoking a
		 * -Wtype-limits diagnostic on the normal build. */
#if SIZE_MAX < UINT64_MAX
		if ((uint64_t) map_count > (uint64_t) SIZE_MAX / sizeof(*map_ids))
		{
			errno = EOVERFLOW;
			return -1;
		}
#endif
		map_ids = malloc(map_count * sizeof(*map_ids));
		if (map_ids == NULL)
			return -1;
		for (size_t i = 0; i < map_count; i++)
			map_ids[i] = map->layers[i].layer_id;
		qsort(map_ids, map_count, sizeof(*map_ids), compare_layer_ids);
	}
	scanfd = open(layer_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (scanfd < 0)
		goto done;
	unlinkfd = dup(scanfd);
	if (unlinkfd < 0 || fcntl(unlinkfd, F_SETFD, FD_CLOEXEC) != 0)
	{
		save_errno = errno;
		goto done;
	}
	dir = fdopendir(scanfd);
	if (dir == NULL)
	{
		save_errno = errno;
		goto done;
	}
	scanfd = -1; /* owned by DIR now */
	errno = 0;
	while ((ent = readdir(dir)) != NULL)
	{
		LocalLayerCandidate candidate;
		uint64_t		layer_id;
		int			parsed;
		int			copy_temp;
		struct stat	st;

		if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
			continue;
		copy_temp = parse_copy_temp_name(ent->d_name, NULL);
		if (copy_temp == 1)
			continue;
		if (copy_temp < 0)
		{
			errno = EINVAL;
			goto done;
		}
		parsed = parse_canonical_layer_name(ent->d_name, &layer_id);
		if (parsed == 0)
			continue;
		if (parsed < 0 || strlen(ent->d_name) > NAME_MAX)
		{
			errno = EINVAL;
			goto done;
		}
		memset(&candidate, 0, sizeof(candidate));
		snprintf(candidate.name, sizeof(candidate.name), "%s", ent->d_name);
		candidate.layer_id = layer_id;
		if (fstatat(dirfd(dir), candidate.name, &st, AT_SYMLINK_NOFOLLOW) != 0)
			goto done;
		if (!S_ISREG(st.st_mode) || st.st_nlink != 1)
		{
			errno = EINVAL;
			goto done;
		}
		candidate.dev = st.st_dev;
		candidate.ino = st.st_ino;
		candidate.size = st.st_size;
		if (validate_layer_candidate(dirfd(dir), &candidate) != 0)
			goto done;
		if (ncandidates == capacity)
		{
			uint32_t new_capacity = capacity == 0 ? 16 : capacity * 2;
			LocalLayerCandidate *grown = realloc(candidates,
										(size_t) new_capacity * sizeof(*grown));

			if (grown == NULL)
				goto done;
			candidates = grown;
			capacity = new_capacity;
		}
		candidates[ncandidates++] = candidate;
	}
	if (errno != 0)
		goto done;
	if (closedir(dir) != 0)
	{
		dir = NULL;
		goto done;
	}
	dir = NULL;
	/* Validate all map-owned local paths before deleting any orphan.  IDs remain
	 * protected even when their manifest record is deleting, remote-only, or has
	 * no currently available local location. */
	for (uint32_t i = 0; i < map->nlayers; i++)
	{
		for (uint32_t j = 0; j < map->layers[i].location_count; j++)
		{
			const PsLayerLocation *location = &map->layers[i].locations[j];
			char expected[4096];
			struct stat st;

			if (location->tier != PS_LAYER_TIER_LOCAL_HOT &&
				location->tier != PS_LAYER_TIER_LOCAL_COLD)
				continue;
			if (local_layer_path(map->layers[i].layer_id, expected,
								 sizeof(expected)) != 0 ||
				strcmp(location->uri, expected) != 0)
			{
				errno = EINVAL;
				goto done;
			}
			if (map->layers[i].remote_durable)
			{
				if (stat(expected, &st) != 0)
				{
					if (errno == ENOENT)
						continue;
					goto done;
				}
				if (!S_ISREG(st.st_mode) || st.st_nlink != 1 ||
					(location->size != 0 &&
					 (st.st_size < 0 ||
					  (uint64_t) st.st_size != location->size)))
				{
					errno = EIO;
					goto done;
				}
			}
			else if (location->available && !map->layers[i].deleting)
			{
				if (stat(expected, &st) != 0 || !S_ISREG(st.st_mode) ||
					st.st_nlink != 1 || (location->size != 0 &&
						(st.st_size < 0 ||
						 (uint64_t) st.st_size != location->size)))
				{
					errno = errno == ENOENT ? ENOENT : EIO;
					goto done;
				}
			}
		}
	}

	for (uint32_t i = 0; i < ncandidates; i++)
	{
		struct stat st;
		int		fd;

		if (layer_map_has_id(map_ids, map_count, candidates[i].layer_id))
			continue;
		if (fstatat(unlinkfd, candidates[i].name, &st, AT_SYMLINK_NOFOLLOW) != 0)
		{
			if (errno == ENOENT)
				continue;
			goto done;
		}
		if (st.st_dev != candidates[i].dev || st.st_ino != candidates[i].ino ||
			!S_ISREG(st.st_mode) || st.st_nlink != 1)
		{
			errno = EINVAL;
			goto done;
		}
		fd = openat(unlinkfd, candidates[i].name,
					O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
		if (fd < 0)
			goto done;
		if (fstat(fd, &st) != 0 || st.st_dev != candidates[i].dev ||
			st.st_ino != candidates[i].ino || !S_ISREG(st.st_mode) ||
			st.st_nlink != 1)
		{
			if (fd >= 0)
				close(fd);
			errno = EINVAL;
			goto done;
		}
		if (close(fd) != 0)
			goto done;
		if (unlinkat(unlinkfd, candidates[i].name, 0) != 0 && errno != ENOENT)
			goto done;
		changed = 1;
	}
	rc = 0;

done:
	if (changed && fsync(unlinkfd) != 0 && rc == 0)
	{
		rc = -1;
		save_errno = errno;
	}
	if (dir != NULL)
		closedir(dir);
	if (unlinkfd >= 0)
		close(unlinkfd);
	if (scanfd >= 0)
		close(scanfd);
	free(map_ids);
	free(candidates);
	if (rc != 0)
	{
		if (save_errno == 0)
			save_errno = errno != 0 ? errno : EIO;
		errno = save_errno;
	}
	return rc;
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
	pid_t		pid;
	int			n;

	d = opendir(dir);
	if (d == NULL)
		return -1;
	while ((ent = readdir(d)) != NULL)
	{
		if (parse_copy_temp_name(ent->d_name, &pid) != 1)
			continue;
		if (kill(pid, 0) != -1 || errno != ESRCH)
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

	if (!local_owner_current())
		return -1;
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
	if (ps_fault_probe(PS_FAULT_POINT_IMAGE_LAYER_AFTER_CREATE) != 0)
		return -1;
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

	if (!local_owner_current())
		return -1;
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

	if (!local_owner_current())
		return -1;
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
	return ps_fault_probe(PS_FAULT_POINT_IMAGE_LAYER_AFTER_WRITE) == 0 ? 0 : -1;
}

static int
local_seal_local_layer(uint64_t layer_id)
{
	char		path[4096];
	int			fd;
	int			rc;

	if (!local_owner_current())
		return -1;
	if (local_layer_path(layer_id, path, sizeof(path)) != 0)
		return -1;
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	close(fd);
	if (rc != 0)
		return rc;
	return ps_fault_probe(PS_FAULT_POINT_IMAGE_LAYER_AFTER_SEAL) == 0 ? 0 : -1;
}

static int
local_read_layer_block(const PsLayerDesc *layer, uint64_t off,
					   void *buf, uint32_t len)
{
	const char *path = NULL;
	int			fd;
	ssize_t		n;
	uint32_t	nlocs;

	if (!local_owner_current())
		return -1;
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
local_refresh_layer_cache(const PsLayerDesc *layer)
{
	const PsLayerLocation *local_loc;
	char		local[4096];

	if (!local_owner_current())
		return -1;

	local_loc = local_location(layer);
	/* Before upload durability, a manifest-owned local layer is the only source
	 * of truth.  After remote durability, the verified remote object may repair
	 * the still-present local copy. */
	if ((local_loc != NULL && !layer->remote_durable) ||
		remote_location(layer) == NULL ||
		local_layer_path(layer->layer_id, local, sizeof(local)) != 0)
		return -1;
	if (local_loc != NULL)
		snprintf(local, sizeof(local), "%s", local_loc->uri);
	if (unlink(local) != 0 && errno != ENOENT)
		return -1;
	return local_download_layer(layer);
}

static int
local_remote_uri(uint64_t layer_id, char *uri, uint32_t uri_len)
{
	char		path[4096];
	int			n;

	if (!local_owner_current())
		return -1;
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

	if (!local_owner_current())
		return -1;

	source = local_location(layer);
	published = remote_location(layer);
	if (source == NULL ||
		(published == NULL && object_layer_path(layer->layer_id, remote, sizeof(remote)) != 0) ||
		(published != NULL && snprintf(remote, sizeof(remote), "%s", published->uri) >= (int) sizeof(remote)))
		return -1;
	{
		struct stat st;

		if (stat(source->uri, &st) != 0 || st.st_size < 0 ||
			(uint64_t) st.st_size != source->size)
			return -1;
	}
	if (layer->kind == PS_LAYER_IMAGE)
	{
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;
		int			rc;

		rc = ps_image_layer_read_index(layer, &idx, &nidx);
		free(idx);
		if (rc != 0 || ps_image_layer_verify_data(layer, layer_page_size) != 0)
			return -1;
	}
	else if (layer->kind == PS_LAYER_DELTA &&
			 ps_delta_layer_verify_data(layer) != 0)
		return -1;
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

	if (!local_owner_current())
		return -1;

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
			ps_image_layer_verify_data(layer, layer_page_size) != 0)
		{
			unlink(local);
			return -1;
		}
	}
	else if (layer->kind == PS_LAYER_DELTA &&
		ps_delta_layer_verify_data(layer) != 0)
	{
		unlink(local);
		return -1;
	}
	return 0;
}

static int
local_delete_remote_layer(const PsLayerDesc *layer)
{
	const PsLayerLocation *location;
	char		expected[4096];

	if (!local_owner_current())
		return -1;

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

	if (!local_owner_current())
		return -1;

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

	if (!local_owner_current())
		return -1;
	location = remote_location(layer);
	if (location == NULL ||
		object_layer_path(layer->layer_id, expected, sizeof(expected)) != 0 ||
		strcmp(location->uri, expected) != 0)
		return 0;
	if (access(expected, F_OK) == 0)
		return 1;
	return errno == ENOENT ? 0 : -1;
}

static int
local_verify_remote_layer(const PsLayerDesc *layer)
{
	const PsLayerLocation *location;
	PsLayerDesc remote_only;
	char		expected[4096];
	struct stat st;

	if (!local_owner_current())
		return -1;
	location = remote_location(layer);
	if (location == NULL ||
		object_layer_path(layer->layer_id, expected, sizeof(expected)) != 0 ||
		strcmp(location->uri, expected) != 0 ||
		stat(expected, &st) != 0 || st.st_size < 0 ||
		(uint64_t) st.st_size != location->size)
		return -1;
	remote_only = *layer;
	remote_only.location_count = 1;
	remote_only.locations[0] = *location;
	remote_only.locations[0].tier = PS_LAYER_TIER_LOCAL_COLD;
	if (layer->kind == PS_LAYER_IMAGE)
	{
		PsImgIndexEnt *idx = NULL;
		uint32_t	nidx = 0;
		int			rc;

		rc = ps_image_layer_read_index(&remote_only, &idx, &nidx);
		free(idx);
		if (rc != 0)
			return -1;
		return ps_image_layer_verify_data(&remote_only, layer_page_size);
	}
	if (layer->kind == PS_LAYER_DELTA)
		return ps_delta_layer_verify_data(&remote_only);
	return -1;
}

const PsLayerStore PsLayerStoreLocal = {
	.name = "local",
	.open = local_open,
	.close = local_close,
	.validate_local_layers = local_validate_layer_locations,
	.recover_local_layers = local_recover_local_layers,
	.create_local_layer = local_create_local_layer,
	.layer_exists_local = local_layer_exists,
	.write_local_layer = local_write_local_layer,
	.seal_local_layer = local_seal_local_layer,
	.remote_uri = local_remote_uri,
	.read_layer_block = local_read_layer_block,
	.upload_layer = local_upload_layer,
	.download_layer = local_download_layer,
	.refresh_layer_cache = local_refresh_layer_cache,
	.delete_local_layer = local_delete_local_layer,
	.delete_remote_layer = local_delete_remote_layer,
	.layer_exists_remote = local_layer_exists_remote,
	.verify_remote_layer = local_verify_remote_layer,
};
