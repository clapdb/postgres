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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_storage.h"

/* bounded well under the 4096-byte path buffers so suffixes never truncate */
static char posix_dir[2048];

/*
 * One cached OS fd per segment id (opened lazily, never closed during a run).
 * Only the segment log keeps fds; the WAL and metadata logs open per call,
 * matching the original daemon (they are not on the hot path).
 */
static int *seg_fds;
static int	segs_cap;

static void
seg_path(char *buf, size_t buflen, int id)
{
	snprintf(buf, buflen, "%s/seg_%08d", posix_dir, id);
}

/* Return a cached fd for segment 'id', opening (optionally creating) it once. */
static int
seg_fd(int id, int create)
{
	char		path[4096];
	int			fd;

	if (id < segs_cap && seg_fds[id] >= 0)
		return seg_fds[id];

	if (id >= segs_cap)
	{
		int			newcap = (id + 16) * 2;

		seg_fds = realloc(seg_fds, (size_t) newcap * sizeof(int));
		for (int i = segs_cap; i < newcap; i++)
			seg_fds[i] = -1;
		segs_cap = newcap;
	}

	seg_path(path, sizeof(path), id);
	fd = open(path, O_RDWR | (create ? O_CREAT : 0), 0600);
	if (fd >= 0)
		seg_fds[id] = fd;
	return fd;
}

static int
posix_open(const char *path, uint64_t segment_size)
{
	(void) segment_size;		/* the file backend has no fixed-region layout */
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		return -1;
	snprintf(posix_dir, sizeof(posix_dir), "%s", path);
	return 0;
}

static void
posix_close(void)
{
	for (int id = 0; id < segs_cap; id++)
		if (seg_fds[id] >= 0)
			close(seg_fds[id]);
	free(seg_fds);
	seg_fds = NULL;
	segs_cap = 0;
}

static int
posix_sync(void)
{
	for (int id = 0; id < segs_cap; id++)
		if (seg_fds[id] >= 0 && fsync(seg_fds[id]) != 0)
			return -1;
	return 0;
}

static int
posix_seg_write(int seg, uint64_t off, const void *buf, uint32_t len)
{
	int			fd = seg_fd(seg, 1);

	if (fd < 0)
		return -1;
	if (pwrite(fd, buf, len, (off_t) off) != (ssize_t) len)
		return -1;
	return 0;
}

static int
posix_seg_read(int seg, uint64_t off, void *buf, uint32_t len)
{
	int			fd = seg_fd(seg, 0);

	if (fd < 0)
		return -1;
	if (pread(fd, buf, len, (off_t) off) != (ssize_t) len)
		return -1;
	return 0;
}

static int64_t
posix_seg_size(int seg)
{
	char		path[4096];
	struct stat st;

	seg_path(path, sizeof(path), seg);
	if (stat(path, &st) != 0)
		return -1;
	return (int64_t) st.st_size;
}

static int
posix_wal_append(uint32_t tl, const void *a, uint32_t alen,
				 const void *b, uint32_t blen)
{
	char		path[4096];
	int			fd;

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
	int			fd;
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
	int			fd;

	snprintf(path, sizeof(path), "%s/timelines", posix_dir);
	fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;
	if (write(fd, buf, len) != (ssize_t) len)
	{
		close(fd);
		return -1;
	}
	close(fd);
	return 0;
}

static int
posix_meta_read(uint64_t off, void *buf, uint32_t len)
{
	char		path[4096];
	int			fd;
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
