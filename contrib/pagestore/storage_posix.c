/*-------------------------------------------------------------------------
 *
 * storage_posix.c
 *	  Portable, libc-only storage backend for the page-store daemon.
 *
 * Implements the PsStorage interface over plain files, exactly the on-disk
 * layout the daemon used before storage was made pluggable: one append-only
 * file per segment (seg_NNNNNNNN), one per-timeline shipped-WAL log (wal_<tl>),
 * and a single timeline metadata log (timelines), all under the store dir.
 *
 * This backend has no external dependencies and is the default everywhere; the
 * SPDK backend is an optional, higher-performance alternative behind the same
 * interface.
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_storage.h"

/* bounded well under the 4096-byte path buffers so suffixes never truncate */
static char posix_dir[2048];

/*
 * One cached OS fd per segment shard+id (opened lazily, never closed during a
 * run).  Only the segment log keeps fds; the WAL and metadata logs open per
 * call, matching the original daemon (they are not on the hot path).
 *
 * Reads run concurrently across shard workers (the daemon holds only a shared
 * read lock), and any of them can open a segment in any storage shard, so the
 * fd-cache structures below are shared mutable state.  seg_fds_lock serializes
 * their growth and the lazy open/cache, which happen rarely (once per segment
 * file) and so are not on the hot path.
 */
static int **seg_fds;
static int *seg_fds_caps;
static int seg_shards_cap;
static pthread_mutex_t seg_fds_lock = PTHREAD_MUTEX_INITIALIZER;

static void
seg_path(char *buf, size_t buflen, uint32_t shard, int seg)
{
	/*
	 * Shard 0 keeps the pre-sharding filename "seg_<id>" so stores written by
	 * earlier (single-shard) versions are still discovered and read; only the
	 * additional shards use the "seg_<shard>_<id>" form.
	 */
	if (shard == 0)
		snprintf(buf, buflen, "%s/seg_%08d", posix_dir, seg);
	else
		snprintf(buf, buflen, "%s/seg_%u_%08d", posix_dir, shard, seg);
}

static void
free_shard_caches(void)
{
	for (int i = 0; i < seg_shards_cap; i++)
	{
		int *fds = seg_fds[i];
		int cap = seg_fds_caps[i];

		if (!fds)
			continue;
		for (int id = 0; id < cap; id++)
			if (fds[id] >= 0)
				close(fds[id]);
		free(fds);
	}
	free(seg_fds);
	free(seg_fds_caps);
	seg_fds = NULL;
	seg_fds_caps = NULL;
	seg_shards_cap = 0;
}

static int
ensure_shard_slot(uint32_t shard)
{
	if (shard < (uint32_t) seg_shards_cap)
		return 0;

	/* expand to exactly shard+1 because shard count is tiny and bounded by config */
	{
		int		new_cap = (int) shard + 1;
		int	**nfds;
		int	*ncaps;

		nfds = realloc(seg_fds, (size_t) new_cap * sizeof(*nfds));
		ncaps = realloc(seg_fds_caps, (size_t) new_cap * sizeof(*ncaps));
		if (!nfds || !ncaps)
		{
			free(nfds);
			free(ncaps);
			return -1;
		}
		for (int i = seg_shards_cap; i < new_cap; i++)
		{
			nfds[i] = NULL;
			ncaps[i] = 0;
		}
		seg_fds = nfds;
		seg_fds_caps = ncaps;
		seg_shards_cap = new_cap;
	}
	return 0;
}

/* Return a cached fd for shard-local segment 'seg', opening (optionally creating)
 * it once. */
static int
seg_fd(uint32_t shard, int seg, int create)
{
	char		path[4096];
	int		fd;
	int		*fds;
	int		cap;
	int		result = -1;

	/*
	 * Serialize the whole lookup/grow/open: concurrent shard workers can hit
	 * this for any storage shard at once.  The cached fd is returned on
	 * subsequent calls, so the lock is contended only on the first open of each
	 * segment file.
	 */
	pthread_mutex_lock(&seg_fds_lock);

	if (ensure_shard_slot(shard) != 0)
		goto out;

	fds = seg_fds[shard];
	cap = seg_fds_caps[shard];
	if (!fds)
	{
		fds = NULL;
		cap = 0;
	}
	if (seg >= cap)
	{
		int		new_cap = (seg + 16) * 2;
		int		*nfds;

		nfds = realloc(fds, (size_t) new_cap * sizeof(int));
		if (!nfds)
			goto out;
		for (int i = cap; i < new_cap; i++)
			nfds[i] = -1;
		seg_fds[shard] = nfds;
		seg_fds_caps[shard] = new_cap;
		fds = nfds;
	}
	if (fds[seg] >= 0)
	{
		result = fds[seg];
		goto out;
	}

	seg_path(path, sizeof(path), shard, seg);
	fd = open(path, O_RDWR | (create ? O_CREAT : 0), 0600);
	if (fd >= 0)
		fds[seg] = fd;
	result = fd;

out:
	pthread_mutex_unlock(&seg_fds_lock);
	return result;
}

static int
posix_open(const char *path, uint64_t segment_size)
{
	(void) segment_size; 	/* the file backend has no fixed-region layout */
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		return -1;

	seg_fds = NULL;
	seg_fds_caps = NULL;
	seg_shards_cap = 0;
	snprintf(posix_dir, sizeof(posix_dir), "%s", path);
	return 0;
}

static void
posix_close(void)
{
	free_shard_caches();
}

static int
posix_sync(void)
{
	int			rc = 0;

	/*
	 * Walk the shared seg-fd cache under seg_fds_lock so a concurrent shard
	 * worker cannot lazily open a segment and realloc seg_fds[]/seg_fds_caps[]
	 * while we iterate (with per-shard locking the caller no longer holds a
	 * global write lock that would have excluded that).
	 */
	pthread_mutex_lock(&seg_fds_lock);
	for (int shard = 0; shard < seg_shards_cap; shard++)
	{
		int *fds = seg_fds[shard];
		int cap = seg_fds_caps[shard];

		if (!fds)
			continue;
		for (int id = 0; id < cap; id++)
			if (fds[id] >= 0 && fsync(fds[id]) != 0)
			{
				rc = -1;
				goto out;
			}
	}
out:
	pthread_mutex_unlock(&seg_fds_lock);

	/*
	 * Segment files are created lazily with O_CREAT, so a new segment's directory
	 * entry is not durable until the store directory itself is fsynced.  Recovery
	 * scans the segment log by name, so persist the directory here too; otherwise a
	 * power loss after a clean shutdown that created a new segment could drop the
	 * name and hide acknowledged writes.
	 */
	{
		int			dfd = open(posix_dir, O_RDONLY);

		if (dfd < 0 || fsync(dfd) != 0)
			rc = -1;
		if (dfd >= 0)
			close(dfd);
	}
	return rc;
}

static int
posix_seg_write(uint32_t shard, int seg, uint64_t off, const void *buf,
			uint32_t len)
{
	int		fd = seg_fd(shard, seg, 1);

	if (fd < 0)
		return -1;
	if (pwrite(fd, buf, len, (off_t) off) != (ssize_t) len)
		return -1;
	return 0;
}

static int
posix_seg_read(uint32_t shard, int seg, uint64_t off, void *buf, uint32_t len)
{
	int		fd = seg_fd(shard, seg, 0);

	if (fd < 0)
		return -1;
	if (pread(fd, buf, len, (off_t) off) != (ssize_t) len)
		return -1;
	return 0;
}

static int64_t
posix_seg_size(uint32_t shard, int seg)
{
	char		path[4096];
	struct stat st;

	seg_path(path, sizeof(path), shard, seg);
	if (stat(path, &st) != 0)
		return -1;
	return (int64_t) st.st_size;
}

static int
posix_wal_append(uint32_t tl, const void *a, uint32_t alen,
			 const void *b, uint32_t blen)
{
	char		path[4096];
	int		fd;

	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;
	if (write(fd, a, alen) != (ssize_t) alen ||
		(blen > 0 && write(fd, b, blen) != (ssize_t) blen))
	{
		close(fd);
		return -1;
	}
	close(fd);
	return 0;
}

static int
posix_wal_read(uint32_t tl, uint64_t off, void *buf, uint32_t len)
{
	char		path[4096];
	int		fd;
	ssize_t		n;

	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	n = pread(fd, buf, len, (off_t) off);
	close(fd);
	return (int) n;
}

static int
posix_meta_append(const void *buf, uint32_t len)
{
	char		path[4096];
	int		fd;

	snprintf(path, sizeof(path), "%s/timelines", posix_dir);
	fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;
	if (write(fd, buf, len) != (ssize_t) len)
	{
		close(fd);
		return -1;
	}
	if (fsync(fd) != 0)
	{
		close(fd);
		return -1;
	}
	if (close(fd) != 0)
		return -1;
	return 0;
}

static int
posix_meta_read(uint64_t off, void *buf, uint32_t len)
{
	char		path[4096];
	int		fd;
	ssize_t		n;

	snprintf(path, sizeof(path), "%s/timelines", posix_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	n = pread(fd, buf, len, (off_t) off);
	close(fd);
	return (int) n;
}

const PsStorage PsStoragePosix = {
	.name = "posix",
	.open = posix_open,
	.close = posix_close,
	.sync = posix_sync,
	.seg_write = posix_seg_write,
	.seg_read = posix_seg_read,
	.seg_size = posix_seg_size,
	.wal_append = posix_wal_append,
	.wal_read = posix_wal_read,
	.meta_append = posix_meta_append,
	.meta_read = posix_meta_read,
};
