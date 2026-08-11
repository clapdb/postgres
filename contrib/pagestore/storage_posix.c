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
static int test_fail_seg_writes;
static int test_crash_after_seg_writes;
static int test_fail_fork_meta_append_at;
static int test_max_log_read;
static pthread_mutex_t seg_fds_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * WAL-index records are independent per (timeline, shard).  The metadata
 * append lock below intentionally remains global because its rollback path
 * samples a shared file length; using it for index logs would serialize
 * unrelated shard workers through write+fsync.
 */
typedef struct PosixWalIdxLock
{
	uint32_t	tl;
	uint32_t	shard;
	pthread_mutex_t lock;
	struct PosixWalIdxLock *next;
} PosixWalIdxLock;

static PosixWalIdxLock *posix_walidx_locks;
static pthread_mutex_t posix_walidx_locks_lock = PTHREAD_MUTEX_INITIALIZER;

static void posix_walidx_locks_clear(void);

static int posix_log_read(const char *name, uint64_t off, void *buf, uint32_t len);
static int posix_log_append(const char *name, const void *buf, uint32_t len);
static int posix_log_append_locked(const char *name, const void *buf,
							 uint32_t len);
static PosixWalIdxLock *posix_walidx_lock_for(uint32_t tl, uint32_t shard);

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

		/*
		 * Grow the pair one at a time, publishing each successful realloc
		 * into its global immediately: realloc may have MOVED (and freed)
		 * the old block even when the sibling allocation then fails, so
		 * freeing the survivor here -- or leaving the global pointing at
		 * the old address -- would dangle later opens/frees.
		 */
		nfds = realloc(seg_fds, (size_t) new_cap * sizeof(*nfds));
		if (!nfds)
			return -1;
		seg_fds = nfds;
		ncaps = realloc(seg_fds_caps, (size_t) new_cap * sizeof(*ncaps));
		if (!ncaps)
			return -1;			/* seg_fds grew; caps/cap count unchanged: consistent */
		seg_fds_caps = ncaps;
		for (int i = seg_shards_cap; i < new_cap; i++)
		{
			seg_fds[i] = NULL;
			seg_fds_caps[i] = 0;
		}
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
	const char *fail_writes;
	const char *crash_after_writes;
	const char *fail_fork_meta_at;
	const char *max_log_read;
	int		dfd;

	(void) segment_size; 	/* the file backend has no fixed-region layout */
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		return -1;

	seg_fds = NULL;
	seg_fds_caps = NULL;
	seg_shards_cap = 0;
	/* Standalone-test fault injection; ordinary deployments never set it. */
	fail_writes = getenv("PAGESTORE_TEST_FAIL_SEG_WRITES");
	test_fail_seg_writes = fail_writes ? atoi(fail_writes) : 0;
	crash_after_writes = getenv("PAGESTORE_TEST_CRASH_AFTER_SEG_WRITES");
	test_crash_after_seg_writes = crash_after_writes ?
		atoi(crash_after_writes) : 0;
	fail_fork_meta_at = getenv("PAGESTORE_TEST_FAIL_FORK_META_APPEND_AT");
	test_fail_fork_meta_append_at = fail_fork_meta_at ?
		atoi(fail_fork_meta_at) : 0;
	max_log_read = getenv("PAGESTORE_TEST_MAX_LOG_READ");
	test_max_log_read = max_log_read ? atoi(max_log_read) : 0;
	snprintf(posix_dir, sizeof(posix_dir), "%s", path);
	/* A prior metadata rename whose directory sync failed must be made durable
	 * before this process can accept writes against its visible replacement. */
	dfd = open(posix_dir, O_RDONLY | O_DIRECTORY);
	if (dfd < 0 || fsync(dfd) != 0)
	{
		if (dfd >= 0)
			close(dfd);
		return -1;
	}
	if (close(dfd) != 0)
		return -1;
	return 0;
}

static void
posix_close(void)
{
	free_shard_caches();
	posix_walidx_locks_clear();
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
posix_seg_remove(uint32_t shard, int seg)
{
	char		path[4096];
	int			dfd;
	int			rc = 0;

	seg_path(path, sizeof(path), shard, seg);
	pthread_mutex_lock(&seg_fds_lock);
	if (shard < (uint32_t) seg_shards_cap && seg >= 0 &&
		seg < seg_fds_caps[shard] && seg_fds[shard] &&
		seg_fds[shard][seg] >= 0)
	{
		close(seg_fds[shard][seg]);
		seg_fds[shard][seg] = -1;
	}
	if (unlink(path) != 0 && errno != ENOENT)
		rc = -1;
	if (rc == 0)
	{
		dfd = open(posix_dir, O_RDONLY);
		if (dfd < 0 || fsync(dfd) != 0)
			rc = -1;
		if (dfd >= 0)
			close(dfd);
	}
	pthread_mutex_unlock(&seg_fds_lock);
	return rc;
}

static int
posix_seg_write(uint32_t shard, int seg, uint64_t off, const void *buf,
			uint32_t len)
{
	int		fd = seg_fd(shard, seg, 1);

	if (fd < 0)
		return -1;
	if (test_fail_seg_writes > 0)
	{
		test_fail_seg_writes--;
		return -1;
	}
	if (pwrite(fd, buf, len, (off_t) off) != (ssize_t) len)
		return -1;
	/* Standalone-test crash point: after N successful segment writes. */
	if (test_crash_after_seg_writes > 0 &&
		--test_crash_after_seg_writes == 0)
		_exit(86);
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

static void
posix_wal_rollback(const char *path, int fd, off_t oldsz, int created)
{
	if (ftruncate(fd, oldsz) == 0)
		(void) fsync(fd);
	close(fd);
	if (created && oldsz == 0 && unlink(path) == 0)
	{
		int		dfd = open(posix_dir, O_RDONLY | O_DIRECTORY);

		if (dfd >= 0)
		{
			(void) fsync(dfd);
			close(dfd);
		}
	}
}

static int
posix_wal_append(uint32_t tl, const void *a, uint32_t alen,
			 const void *b, uint32_t blen)
{
	char		path[4096];
	int		fd;
	int		dfd;
	int		created = 0;
	struct stat st;
	off_t		oldsz;

	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_EXCL, 0600);
	if (fd >= 0)
		created = 1;
	else if (errno == EEXIST)
		fd = open(path, O_WRONLY | O_APPEND);
	if (fd < 0)
		return -1;
	if (fstat(fd, &st) != 0)
	{
		close(fd);
		if (created)
			unlink(path);
		return -1;
	}
	oldsz = st.st_size;
	if (write(fd, a, alen) != (ssize_t) alen ||
		(blen > 0 && write(fd, b, blen) != (ssize_t) blen) ||
		fsync(fd) != 0)
	{
		posix_wal_rollback(path, fd, oldsz, created);
		return -1;
	}
	if (created)
	{
		dfd = open(posix_dir, O_RDONLY | O_DIRECTORY);
		if (dfd < 0 || fsync(dfd) != 0)
		{
			if (dfd >= 0)
				close(dfd);
			posix_wal_rollback(path, fd, oldsz, created);
			return -1;
		}
		close(dfd);
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
	uint32_t	done = 0;

	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	while (done < len)
	{
		n = pread(fd, (char *) buf + done, len - done, (off_t) off + done);
		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			close(fd);
			return -1;
		}
		if (n == 0)
			break;
		done += (uint32_t) n;
	}
	close(fd);
	return (int) done;
}

static int
posix_wal_truncate(uint32_t tl, uint64_t len)
{
	char		path[4096];
	int			fd;
	int			rc = 0;

	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	fd = open(path, O_WRONLY);
	if (fd < 0)
		return errno == ENOENT && len == 0 ? 0 : -1;
	if (ftruncate(fd, (off_t) len) != 0 || fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

static int
posix_walidx_append(uint32_t tl, uint32_t shard, const void *buf, uint32_t len)
{
	char		name[64];
	PosixWalIdxLock *lock;
	int			rc;

	snprintf(name, sizeof(name), "walidx_%u_%u", tl, shard);
	lock = posix_walidx_lock_for(tl, shard);
	if (!lock)
		return -1;
	pthread_mutex_lock(&lock->lock);
	rc = posix_log_append_locked(name, buf, len);
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

static pthread_mutex_t posix_log_lock = PTHREAD_MUTEX_INITIALIZER;

static PosixWalIdxLock *
posix_walidx_lock_for(uint32_t tl, uint32_t shard)
{
	PosixWalIdxLock *entry;

	pthread_mutex_lock(&posix_walidx_locks_lock);
	for (entry = posix_walidx_locks; entry; entry = entry->next)
		if (entry->tl == tl && entry->shard == shard)
			break;
	if (!entry)
	{
		entry = calloc(1, sizeof(*entry));
		if (entry)
		{
			entry->tl = tl;
			entry->shard = shard;
			pthread_mutex_init(&entry->lock, NULL);
			entry->next = posix_walidx_locks;
			posix_walidx_locks = entry;
		}
	}
	pthread_mutex_unlock(&posix_walidx_locks_lock);
	return entry;
}

static void
posix_walidx_locks_clear(void)
{
	PosixWalIdxLock *entry;

	pthread_mutex_lock(&posix_walidx_locks_lock);
	entry = posix_walidx_locks;
	posix_walidx_locks = NULL;
	pthread_mutex_unlock(&posix_walidx_locks_lock);
	while (entry)
	{
		PosixWalIdxLock *next = entry->next;

		pthread_mutex_destroy(&entry->lock);
		free(entry);
		entry = next;
	}
}

static int
posix_walidx_read(uint32_t tl, uint32_t shard, uint64_t off, void *buf,
			  uint32_t len)
{
	char		name[64];

	snprintf(name, sizeof(name), "walidx_%u_%u", tl, shard);
	return posix_log_read(name, off, buf, len);
}

static int
posix_walidx_truncate(uint32_t tl, uint32_t shard, uint64_t len)
{
	char		path[4096];
	int			fd;
	int			rc = 0;
	PosixWalIdxLock *lock;

	snprintf(path, sizeof(path), "%s/walidx_%u_%u", posix_dir, tl, shard);
	lock = posix_walidx_lock_for(tl, shard);
	if (!lock)
		return -1;
	pthread_mutex_lock(&lock->lock);
	fd = open(path, O_WRONLY);
	if (fd < 0)
		rc = (errno == ENOENT && len == 0) ? 0 : -1;
	else
	{
		if (ftruncate(fd, (off_t) len) != 0 || fsync(fd) != 0)
			rc = -1;
		if (close(fd) != 0)
			rc = -1;
	}
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

/*
 * Truncate fd back to old_size on an error path.  Best-effort: the caller
 * is already returning an error and cannot act on a rollback failure; the
 * torn tail is detected and discarded on the next meta read.
 */
static void
posix_truncate_best_effort(int fd, off_t old_size)
{
	if (ftruncate(fd, old_size) != 0)
	{
		/* nothing we can do; see above */
	}
}

static void
posix_fsync_dir_best_effort(void)
{
	int		fd = open(posix_dir, O_RDONLY | O_DIRECTORY);

	if (fd >= 0)
	{
		(void) fsync(fd);
		close(fd);
	}
}

static void
posix_unlink_created_meta(const char *path)
{
	if (unlink(path) == 0)
		posix_fsync_dir_best_effort();
}

/*
 * Roll back an append whose record may already be durable in the metadata
 * log: reopen the log, truncate it back to old_size, and push the truncate
 * out.  If this append created the file, remove it again -- otherwise a
 * failed append is reported as an error yet a daemon restart would replay
 * the record from the log and resurrect it.  Best-effort, like
 * posix_truncate_best_effort().
 */
static void
posix_meta_rollback(const char *path, off_t old_size, int created)
{
	int		fd = open(path, O_WRONLY);

	if (fd >= 0)
	{
		posix_truncate_best_effort(fd, old_size);
		(void) fsync(fd);
		close(fd);
	}
	if (created && old_size == 0)
		posix_unlink_created_meta(path);
}

/*
 * Metadata logs are appended from per-shard workers (fork-meta events) as
 * well as the request path (timelines).  The append itself could rely on
 * O_APPEND, but the ERROR ROLLBACK below truncates to a size sampled before
 * the write -- racing appenders would let one failure chop another's
 * acknowledged record.  One lock serializes append+rollback per process.
 */

static int
posix_log_append_locked(const char *name, const void *buf, uint32_t len)
{
	char		path[4096];
	int		fd;
	int		created;
	off_t		old_size;

	snprintf(path, sizeof(path), "%s/%s", posix_dir, name);
	created = access(path, F_OK) != 0;
	fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return -1;
	old_size = lseek(fd, 0, SEEK_END);
	if (old_size < 0)
	{
		close(fd);
		if (created)
			posix_unlink_created_meta(path);
		return -1;
	}

	/*
	 * Every failure below must also remove a file this append created (not
	 * just truncate it): if an empty log lingered, the next successful append
	 * would see the file as pre-existing and skip the directory fsync, so its
	 * directory entry would never be made durable and a crash could drop an
	 * acknowledged record with it.
	 */
	if (write(fd, buf, len) != (ssize_t) len)
	{
		posix_truncate_best_effort(fd, old_size);
		close(fd);
		if (created)
			posix_unlink_created_meta(path);
		return -1;
	}
	if (fsync(fd) != 0)
	{
		posix_truncate_best_effort(fd, old_size);
		(void) fsync(fd);
		close(fd);
		if (created)
			posix_unlink_created_meta(path);
		return -1;
	}
	if (close(fd) != 0)
	{
		posix_meta_rollback(path, old_size, created);
		return -1;
	}
	if (created)
	{
		fd = open(posix_dir, O_RDONLY | O_DIRECTORY);
		if (fd < 0)
		{
			posix_meta_rollback(path, old_size, created);
			return -1;
		}
		if (fsync(fd) != 0)
		{
			close(fd);
			posix_meta_rollback(path, old_size, created);
			return -1;
		}
		if (close(fd) != 0)
		{
			posix_meta_rollback(path, old_size, created);
			return -1;
		}
	}
	return 0;
}

static int
posix_log_read(const char *name, uint64_t off, void *buf, uint32_t len)
{
	char		path[4096];
	int		fd;
	ssize_t		n;
	uint32_t	done = 0;

	snprintf(path, sizeof(path), "%s/%s", posix_dir, name);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	while (done < len)
	{
		uint32_t	read_len = len - done;

		/* Standalone-test fault injection; ordinary deployments leave this zero. */
		if (test_max_log_read > 0 &&
			read_len > (uint32_t) test_max_log_read)
			read_len = (uint32_t) test_max_log_read;
		n = pread(fd, (char *) buf + done, read_len, (off_t) off + done);
		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			close(fd);
			return -1;
		}
		if (n == 0)
			break;
		done += (uint32_t) n;
	}
	close(fd);
	return (int) done;
}

static int
posix_log_append(const char *name, const void *buf, uint32_t len)
{
	int			rc;

	pthread_mutex_lock(&posix_log_lock);
	rc = posix_log_append_locked(name, buf, len);
	pthread_mutex_unlock(&posix_log_lock);
	return rc;
}

static int
posix_meta_append(const void *buf, uint32_t len)
{
	return posix_log_append("timelines", buf, len);
}

static int
posix_meta_read(uint64_t off, void *buf, uint32_t len)
{
	return posix_log_read("timelines", off, buf, len);
}

static int
posix_meta_rewrite(const void *buf, uint32_t len)
{
	char path[4096], tmp[4096];
	int fd = -1, dfd = -1, rc = -1;
	ssize_t n;

	snprintf(path, sizeof(path), "%s/timelines", posix_dir);
	snprintf(tmp, sizeof(tmp), "%s/timelines.tmp", posix_dir);
	pthread_mutex_lock(&posix_log_lock);
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		goto out;
	for (uint32_t off = 0; off < len; )
	{
		n = write(fd, (const char *) buf + off, len - off);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto out;
		off += (uint32_t) n;
	}
	if (fsync(fd) != 0)
		goto out;
	/* A failing close leaves the descriptor's state unspecified; do not retry
	 * it in cleanup, but still remove the uncommitted temporary file. */
	if (close(fd) != 0)
	{
		fd = -1;
		goto out;
	}
	fd = -1;
	if (rename(tmp, path) != 0)
		goto out;
	dfd = open(posix_dir, O_RDONLY | O_DIRECTORY);
	if (dfd < 0 || fsync(dfd) != 0)
		goto out;
	if (close(dfd) != 0)
		goto out;
	dfd = -1;
	rc = 0;
out:
	if (fd >= 0)
		close(fd);
	if (dfd >= 0)
		close(dfd);
	if (rc != 0)
		unlink(tmp);
	pthread_mutex_unlock(&posix_log_lock);
	return rc;
}

static int
posix_fork_meta_append(const void *buf, uint32_t len)
{
	/* Standalone-test fault injection; ordinary deployments leave this zero. */
	if (test_fail_fork_meta_append_at > 0 &&
		--test_fail_fork_meta_append_at == 0)
	{
		errno = EIO;
		return -1;
	}
	return posix_log_append("forkmeta", buf, len);
}

static int
posix_fork_meta_read(uint64_t off, void *buf, uint32_t len)
{
	return posix_log_read("forkmeta", off, buf, len);
}

static int
posix_fork_meta_truncate(uint64_t len)
{
	char		path[4096];
	int			fd;
	int			rc = 0;

	snprintf(path, sizeof(path), "%s/forkmeta", posix_dir);
	pthread_mutex_lock(&posix_log_lock);
	fd = open(path, O_WRONLY);
	if (fd < 0)
		rc = (errno == ENOENT && len == 0) ? 0 : -1;
	else
	{
		if (ftruncate(fd, (off_t) len) != 0 || fsync(fd) != 0)
			rc = -1;
		if (close(fd) != 0)
			rc = -1;
	}
	pthread_mutex_unlock(&posix_log_lock);
	return rc;
}

const PsStorage PsStoragePosix = {
	.name = "posix",
	.open = posix_open,
	.close = posix_close,
	.sync = posix_sync,
	.seg_write = posix_seg_write,
	.seg_read = posix_seg_read,
	.seg_size = posix_seg_size,
	.seg_remove = posix_seg_remove,
	.wal_append = posix_wal_append,
	.wal_read = posix_wal_read,
	.wal_truncate = posix_wal_truncate,
	.walidx_append = posix_walidx_append,
	.walidx_read = posix_walidx_read,
	.walidx_truncate = posix_walidx_truncate,
	.meta_append = posix_meta_append,
	.meta_read = posix_meta_read,
	.meta_rewrite = posix_meta_rewrite,
	.fork_meta_append = posix_fork_meta_append,
	.fork_meta_read = posix_fork_meta_read,
	.fork_meta_truncate = posix_fork_meta_truncate,
};
