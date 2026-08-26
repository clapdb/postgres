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
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_storage.h"
#include "pagestore_wal_store.h"

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
static int test_fail_wal_rewrite_before_rename;
static int test_fail_wal_rewrite_dir_fsync;
static int test_fail_fork_meta_rewrite_before_rename;
static int test_fail_fork_meta_rewrite_dir_fsync;
static int test_fail_timeline_cleanup_private_dir_fsync;
static pthread_mutex_t seg_fds_lock = PTHREAD_MUTEX_INITIALIZER;

/* Flat WAL files are independent per timeline. */
typedef struct PosixWalLock
{
	uint32_t	tl;
	int		poisoned;
	pthread_mutex_t lock;
	struct PosixWalLock *next;
} PosixWalLock;

static PosixWalLock *posix_wal_locks;
static pthread_mutex_t posix_wal_locks_lock = PTHREAD_MUTEX_INITIALIZER;

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

static void posix_wal_locks_clear(void);
static void posix_walidx_locks_clear(void);

static int posix_log_read(const char *name, uint64_t off, void *buf, uint32_t len);
static int posix_log_append(const char *name, const void *buf, uint32_t len);
static int posix_log_append_locked(const char *name, const void *buf,
								 uint32_t len);
static PosixWalLock *posix_wal_lock_for(uint32_t tl);
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
	const char *fail_wal_rewrite;
	const char *fail_wal_rewrite_dir_fsync;
	const char *fail_timeline_cleanup_private_dir_fsync;
	int		dfd;

	(void) segment_size; 	/* the file backend has no fixed-region layout */
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		return -1;

	/* Backend ownership permits an explicit OPEN to abandon a previous POSIX
	 * instance.  Serialize teardown before installing the new path so stale fd
	 * caches cannot leak or accidentally address the replacement directory. */
	pthread_mutex_lock(&seg_fds_lock);
	free_shard_caches();
	pthread_mutex_unlock(&seg_fds_lock);
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
	fail_wal_rewrite = getenv("PAGESTORE_TEST_FAIL_WAL_REWRITE_BEFORE_RENAME");
	test_fail_wal_rewrite_before_rename = fail_wal_rewrite ?
		atoi(fail_wal_rewrite) : 0;
	fail_wal_rewrite_dir_fsync =
		getenv("PAGESTORE_TEST_FAIL_WAL_REWRITE_DIR_FSYNC");
	test_fail_wal_rewrite_dir_fsync = fail_wal_rewrite_dir_fsync ?
		atoi(fail_wal_rewrite_dir_fsync) : 0;
	fail_timeline_cleanup_private_dir_fsync =
		getenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_PRIVATE_DIR_FSYNC");
	test_fail_timeline_cleanup_private_dir_fsync =
		fail_timeline_cleanup_private_dir_fsync ?
		atoi(fail_timeline_cleanup_private_dir_fsync) : 0;
	{
		const char *value = getenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_BEFORE_RENAME");
		test_fail_fork_meta_rewrite_before_rename = value ? atoi(value) : 0;
		value = getenv("PAGESTORE_TEST_FAIL_FORK_META_REWRITE_DIR_FSYNC");
		test_fail_fork_meta_rewrite_dir_fsync = value ? atoi(value) : 0;
	}
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
	/* A successful reopen has reconciled any ambiguous post-rename state. */
	posix_wal_locks_clear();
	return 0;
}

static void
posix_close(void)
{
	pthread_mutex_lock(&seg_fds_lock);
	free_shard_caches();
	pthread_mutex_unlock(&seg_fds_lock);
	posix_wal_locks_clear();
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
posix_wal_append_locked(uint32_t tl, const void *a, uint32_t alen,
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
posix_wal_append(uint32_t tl, const void *a, uint32_t alen,
			 const void *b, uint32_t blen)
{
	PosixWalLock *lock = posix_wal_lock_for(tl);
	int rc;

	if (lock == NULL)
		return -1;
	pthread_mutex_lock(&lock->lock);
	rc = lock->poisoned ? -1 :
		posix_wal_append_locked(tl, a, alen, b, blen);
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

static int
posix_wal_read(uint32_t tl, uint64_t off, void *buf, uint32_t len)
{
	PosixWalLock *lock = posix_wal_lock_for(tl);
	char		path[4096];
	int		fd;
	ssize_t		n;
	uint32_t	done = 0;

	if (lock == NULL)
		return -1;
	pthread_mutex_lock(&lock->lock);
	if (lock->poisoned)
	{
		pthread_mutex_unlock(&lock->lock);
		return -1;
	}
	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	fd = open(path, O_RDONLY);
	if (fd < 0)
	{
		pthread_mutex_unlock(&lock->lock);
		return -1;
	}
	while (done < len)
	{
		n = pread(fd, (char *) buf + done, len - done, (off_t) off + done);
		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			close(fd);
			pthread_mutex_unlock(&lock->lock);
			return -1;
		}
		if (n == 0)
			break;
		done += (uint32_t) n;
	}
	close(fd);
	pthread_mutex_unlock(&lock->lock);
	return (int) done;
}

static int
posix_wal_truncate_locked(uint32_t tl, uint64_t len)
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
posix_wal_truncate(uint32_t tl, uint64_t len)
{
	PosixWalLock *lock = posix_wal_lock_for(tl);
	int rc;

	if (lock == NULL)
		return -1;
	pthread_mutex_lock(&lock->lock);
	rc = lock->poisoned ? -1 : posix_wal_truncate_locked(tl, len);
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

/*
 * Copy the retained suffix to a sibling, make it durable, then publish it with
 * rename.  The old file remains authoritative before rename.  Success is
 * reported only after the directory entry is durable; an ambiguous failure
 * after rename poisons the timeline until storage is reopened and its physical
 * offsets are recovered.
 */
static int
posix_wal_rewrite_prefix(uint32_t tl, uint64_t keep_off)
{
	PosixWalLock *lock = posix_wal_lock_for(tl);
	char		path[4096];
	char		tmp[4096];
	unsigned char buf[64 * 1024];
	struct stat st;
	uint64_t	off;
	int		src = -1;
	int		dst = -1;
	int		dfd = -1;
	int		rc = -1;

	if (lock == NULL)
		return -1;
	pthread_mutex_lock(&lock->lock);
	if (lock->poisoned)
	{
		pthread_mutex_unlock(&lock->lock);
		return -1;
	}
	snprintf(path, sizeof(path), "%s/wal_%u", posix_dir, tl);
	snprintf(tmp, sizeof(tmp), "%s/wal_%u.rewrite.tmp", posix_dir, tl);
	src = open(path, O_RDONLY);
	if (src < 0 || fstat(src, &st) != 0 || st.st_size < 0 ||
		keep_off > (uint64_t) st.st_size)
		goto out;
	dst = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (dst < 0)
		goto out;
	for (off = keep_off; off < (uint64_t) st.st_size;)
	{
		size_t want = (uint64_t) st.st_size - off < sizeof(buf) ?
			(size_t) ((uint64_t) st.st_size - off) : sizeof(buf);
		ssize_t nread = pread(src, buf, want, (off_t) off);
		size_t written = 0;

		if (nread < 0 && errno == EINTR)
			continue;
		if (nread <= 0)
			goto out;
		while (written < (size_t) nread)
		{
			ssize_t nwrite = write(dst, buf + written,
								 (size_t) nread - written);

			if (nwrite < 0 && errno == EINTR)
				continue;
			if (nwrite <= 0)
				goto out;
			written += (size_t) nwrite;
		}
		off += (uint64_t) nread;
	}
	if (fsync(dst) != 0)
		goto out;
	if (close(dst) != 0)
	{
		dst = -1;
		goto out;
	}
	dst = -1;
	if (test_fail_wal_rewrite_before_rename > 0 &&
		--test_fail_wal_rewrite_before_rename == 0)
	{
		errno = EIO;
		goto out;
	}
	if (rename(tmp, path) != 0)
		goto out;
	dfd = open(posix_dir, O_RDONLY | O_DIRECTORY);
	if (dfd < 0 ||
		(test_fail_wal_rewrite_dir_fsync > 0 &&
		 --test_fail_wal_rewrite_dir_fsync == 0) ||
		fsync(dfd) != 0)
	{
		/* The visible replacement has ambiguous crash durability.  Its physical
		 * offsets no longer match the caller's catalog; force a reopen before
		 * any further operation can observe or extend it. */
		lock->poisoned = 1;
		if (dfd >= 0)
			(void) close(dfd);
		dfd = -1;
		errno = EIO;
		goto out;
	}
	{
		int close_rc = close(dfd);

		dfd = -1;
		if (close_rc != 0)
		{
			lock->poisoned = 1;
			errno = EIO;
			goto out;
		}
	}
	rc = 0;

out:
	if (src >= 0)
		close(src);
	if (dst >= 0)
		close(dst);
	if (dfd >= 0)
		close(dfd);
	if (rc != 0)
		(void) unlink(tmp);
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

static PosixWalLock *
posix_wal_lock_for(uint32_t tl)
{
	PosixWalLock *entry;

	pthread_mutex_lock(&posix_wal_locks_lock);
	for (entry = posix_wal_locks; entry; entry = entry->next)
		if (entry->tl == tl)
			break;
	if (entry == NULL)
	{
		entry = calloc(1, sizeof(*entry));
		if (entry != NULL)
		{
			entry->tl = tl;
			if (pthread_mutex_init(&entry->lock, NULL) != 0)
			{
				free(entry);
				entry = NULL;
			}
			else
			{
				entry->next = posix_wal_locks;
				posix_wal_locks = entry;
			}
		}
	}
	pthread_mutex_unlock(&posix_wal_locks_lock);
	return entry;
}

static void
posix_wal_locks_clear(void)
{
	PosixWalLock *entry;

	pthread_mutex_lock(&posix_wal_locks_lock);
	entry = posix_wal_locks;
	posix_wal_locks = NULL;
	pthread_mutex_unlock(&posix_wal_locks_lock);
	while (entry != NULL)
	{
		PosixWalLock *next = entry->next;

		pthread_mutex_destroy(&entry->lock);
		free(entry);
		entry = next;
	}
}

static int
posix_walidx_name(uint32_t tl, uint32_t shard, uint64_t epoch,
				  char *name, size_t name_len)
{
	int n;

	if (epoch == 0)
		n = snprintf(name, name_len, "walidx_%u_%u", tl, shard);
	else
		n = snprintf(name, name_len, "walidx_%u_%u_e%020llu", tl, shard,
					 (unsigned long long) epoch);
	return n < 0 || (size_t) n >= name_len ? -1 : 0;
}

#define POSIX_WALIDX_WATERMARK_MAGIC UINT64_C(0x31524b4d58444957)
typedef struct PosixWalIdxWatermark
{
	uint64_t magic;
	uint64_t length;
	uint32_t crc;
	uint32_t reserved;
} PosixWalIdxWatermark;

static uint32_t
posix_walidx_watermark_crc(PosixWalIdxWatermark *watermark)
{
	const unsigned char *bytes = (const unsigned char *) watermark;
	uint32_t saved = watermark->crc;
	uint32_t hash = 2166136261u;

	watermark->crc = 0;
	for (size_t i = 0; i < sizeof(*watermark); i++)
	{
		hash ^= bytes[i];
		hash *= 16777619u;
	}
	watermark->crc = saved;
	return hash;
}

static int
posix_walidx_watermark_name(const char *log_name, char *name, size_t name_len)
{
	int n = snprintf(name, name_len, "%s.size", log_name);

	return n < 0 || (size_t) n >= name_len ? -1 : 0;
}

static int
posix_walidx_watermark_read(const char *log_name, uint64_t *length)
{
	PosixWalIdxWatermark watermark;
	char marker[160];
	char path[4096];
	struct stat st;
	int fd;

	if (posix_walidx_watermark_name(log_name, marker, sizeof(marker)) != 0 ||
		snprintf(path, sizeof(path), "%s/%s", posix_dir, marker) < 0 ||
		strlen(posix_dir) + 1 + strlen(marker) >= sizeof(path))
		return -1;
	fd = open(path, O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size != (off_t) sizeof(watermark) ||
		pread(fd, &watermark, sizeof(watermark), 0) !=
			(ssize_t) sizeof(watermark))
	{
		if (fd >= 0)
			close(fd);
		return -1;
	}
	if (close(fd) != 0 || watermark.magic != POSIX_WALIDX_WATERMARK_MAGIC ||
		watermark.reserved != 0 ||
		watermark.crc != posix_walidx_watermark_crc(&watermark))
		return -1;
	*length = watermark.length;
	return 0;
}

static int
posix_walidx_watermark_write(const char *log_name, uint64_t length, int create)
{
	PosixWalIdxWatermark watermark;
	char marker[160];
	char temporary[192] = {0};
	struct stat st;
	int directory_fd = -1;
	int fd = -1;
	int rc = -1;

	if (posix_walidx_watermark_name(log_name, marker, sizeof(marker)) != 0 ||
		(directory_fd = open(posix_dir,
						 O_RDONLY | O_DIRECTORY | O_CLOEXEC)) < 0)
		return -1;
	if (!create &&
		(fstatat(directory_fd, marker, &st, AT_SYMLINK_NOFOLLOW) != 0 ||
		 !S_ISREG(st.st_mode)))
		goto cleanup;
	memset(&watermark, 0, sizeof(watermark));
	watermark.magic = POSIX_WALIDX_WATERMARK_MAGIC;
	watermark.length = length;
	watermark.crc = posix_walidx_watermark_crc(&watermark);
	for (unsigned int attempt = 0; attempt < 128; attempt++)
	{
		int n = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%u",
						 marker, (long) getpid(), attempt);

		if (n < 0 || (size_t) n >= sizeof(temporary))
			goto cleanup;
		fd = openat(directory_fd, temporary,
					O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0 || errno != EEXIST)
			break;
	}
	if (fd < 0 || write(fd, &watermark, sizeof(watermark)) !=
			(ssize_t) sizeof(watermark) || fsync(fd) != 0)
		goto cleanup;
	if (close(fd) != 0)
	{
		fd = -1;
		goto cleanup;
	}
	fd = -1;
	if (renameat(directory_fd, temporary, directory_fd, marker) != 0 ||
		fsync(directory_fd) != 0)
		goto cleanup;
	rc = 0;

cleanup:
	if (fd >= 0)
		close(fd);
	if (temporary[0] != '\0')
		(void) unlinkat(directory_fd, temporary, 0);
	if (directory_fd >= 0 && close(directory_fd) != 0)
		rc = -1;
	return rc;
}

/* The watermark is the acknowledged epoch length.  A longer file is an
 * unacknowledged append tail and is rolled back; a shorter file lost
 * acknowledged records and must fail closed.  Caller holds the shard lock. */
static int
posix_walidx_epoch_reconcile_locked(const char *name, uint64_t *length)
{
	char path[4096];
	struct stat st;
	int fd = -1;
	int rc = -1;

	if (posix_walidx_watermark_read(name, length) != 0 ||
		snprintf(path, sizeof(path), "%s/%s", posix_dir, name) < 0 ||
		strlen(posix_dir) + 1 + strlen(name) >= sizeof(path) ||
		(fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW)) < 0 ||
		fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
		(uint64_t) st.st_size < *length)
		goto cleanup;
	if ((uint64_t) st.st_size > *length &&
		(ftruncate(fd, (off_t) *length) != 0 || fsync(fd) != 0))
		goto cleanup;
	rc = 0;

cleanup:
	if (fd >= 0 && close(fd) != 0)
		rc = -1;
	return rc;
}

static int
posix_walidx_append(uint32_t tl, uint32_t shard, uint64_t epoch,
					const void *buf, uint32_t len)
{
	char		name[128];
	char		path[4096];
	PosixWalIdxLock *lock;
	uint64_t	old_length = 0;
	int			rc;

	if (posix_walidx_name(tl, shard, epoch, name, sizeof(name)) != 0)
		return -1;
	lock = posix_walidx_lock_for(tl, shard);
	if (!lock)
		return -1;
	pthread_mutex_lock(&lock->lock);
	if (epoch != 0 &&
		posix_walidx_epoch_reconcile_locked(name, &old_length) != 0)
	{
		pthread_mutex_unlock(&lock->lock);
		return -1;
	}
	rc = posix_log_append_locked(name, buf, len);
	if (rc == 0 && epoch != 0 &&
		(UINT64_MAX - old_length < len ||
		 posix_walidx_watermark_write(name, old_length + len, 0) != 0))
	{
		int fd;

		snprintf(path, sizeof(path), "%s/%s", posix_dir, name);
		fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
		if (fd >= 0)
		{
			if (ftruncate(fd, (off_t) old_length) == 0)
				(void) fsync(fd);
			close(fd);
		}
		(void) posix_walidx_watermark_write(name, old_length, 0);
		rc = -1;
	}
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
posix_walidx_read(uint32_t tl, uint32_t shard, uint64_t epoch, uint64_t off,
			  void *buf, uint32_t len)
{
	char		name[128];
	PosixWalIdxLock *lock;
	uint64_t	length;
	int			rc;

	if (posix_walidx_name(tl, shard, epoch, name, sizeof(name)) != 0)
		return -1;
	if (epoch == 0)
		return posix_log_read(name, off, buf, len);
	lock = posix_walidx_lock_for(tl, shard);
	if (lock == NULL)
		return -1;
	pthread_mutex_lock(&lock->lock);
	if (posix_walidx_epoch_reconcile_locked(name, &length) != 0)
		rc = -1;
	else if (off >= length)
		rc = 0;
	else
	{
		uint64_t available = length - off;

		if (available < len)
			len = (uint32_t) available;
		rc = posix_log_read(name, off, buf, len);
	}
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

static int
posix_walidx_truncate(uint32_t tl, uint32_t shard, uint64_t epoch, uint64_t len)
{
	char		path[4096];
	char		name[128];
	int			fd;
	int			rc = 0;
	PosixWalIdxLock *lock;

	if (posix_walidx_name(tl, shard, epoch, name, sizeof(name)) != 0 ||
		snprintf(path, sizeof(path), "%s/%s", posix_dir, name) < 0 ||
		strlen(posix_dir) + 1 + strlen(name) >= sizeof(path))
		return -1;
	lock = posix_walidx_lock_for(tl, shard);
	if (!lock)
		return -1;
	pthread_mutex_lock(&lock->lock);
	if (epoch != 0)
	{
		uint64_t acknowledged;

		if (posix_walidx_epoch_reconcile_locked(name, &acknowledged) != 0 ||
			len != acknowledged)
		{
			pthread_mutex_unlock(&lock->lock);
			return -1;
		}
	}
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

static int
posix_walidx_epoch_create(uint32_t tl, uint32_t shard, uint64_t epoch)
{
	char name[128];
	char path[4096];
	struct stat st;
	PosixWalIdxLock *lock;
	int dir_fd = -1;
	int fd = -1;
	int rc = -1;

	if (epoch == 0 ||
		posix_walidx_name(tl, shard, epoch, name, sizeof(name)) != 0 ||
		snprintf(path, sizeof(path), "%s/%s", posix_dir, name) < 0 ||
		strlen(posix_dir) + 1 + strlen(name) >= sizeof(path))
		return -1;
	lock = posix_walidx_lock_for(tl, shard);
	if (lock == NULL)
		return -1;
	pthread_mutex_lock(&lock->lock);
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
	if (fd < 0 && errno == EEXIST)
		fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size != 0 || fsync(fd) != 0)
		goto cleanup;
	if (posix_walidx_watermark_write(name, 0, 1) != 0)
		goto cleanup;
	dir_fd = open(posix_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (dir_fd < 0 || fsync(dir_fd) != 0)
		goto cleanup;
	rc = 0;

cleanup:
	if (dir_fd >= 0 && close(dir_fd) != 0)
		rc = -1;
	if (fd >= 0 && close(fd) != 0)
		rc = -1;
	pthread_mutex_unlock(&lock->lock);
	return rc;
}

static int
posix_walidx_epoch_gc(uint32_t tl, const uint64_t *keep_epochs,
					 uint32_t nshards)
{
	char prefix[128];
	struct dirent *entry;
	DIR *dir = NULL;
	int directory_fd = -1;
	int scan_fd = -1;
	int removed = 0;
	int rc = -1;
	int n;

	if (keep_epochs == NULL || nshards == 0)
		return -1;
	/* The manifest cutover drains old-epoch appenders before selecting each
	 * shard's keep epoch.  Older files are therefore immutable. */
	n = snprintf(prefix, sizeof(prefix), "walidx_%u_", tl);
	if (n < 0 || (size_t) n >= sizeof(prefix))
		return -1;
	directory_fd = open(posix_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (directory_fd < 0 ||
		(scan_fd = fcntl(directory_fd, F_DUPFD_CLOEXEC, 0)) < 0 ||
		(dir = fdopendir(scan_fd)) == NULL)
		goto cleanup;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		char canonical[128];
		char marker[160];
		char *shard_end = NULL;
		unsigned long parsed_shard;
		uint64_t epoch = 0;
		const char *suffix;
		const char *digits = NULL;
		char *end = NULL;
		int is_marker = 0;

		if (strncmp(entry->d_name, prefix, (size_t) n) != 0)
			continue;
		errno = 0;
		parsed_shard = strtoul(entry->d_name + n, &shard_end, 10);
		if (errno != 0 || shard_end == entry->d_name + n ||
			parsed_shard >= nshards)
			goto next_entry;
		suffix = shard_end;
		if (strncmp(suffix, "_e", 2) == 0)
		{
			digits = suffix + 2;
			if (strlen(digits) != 20 &&
				!(strlen(digits) == 25 && strcmp(digits + 20, ".size") == 0))
				goto next_entry;
			for (size_t i = 0; i < 20; i++)
				if (digits[i] < '0' || digits[i] > '9')
					goto next_entry;
			errno = 0;
			epoch = strtoull(digits, &end, 10);
			if (errno != 0 || end != digits + 20 || epoch == 0)
				goto next_entry;
			is_marker = strcmp(end, ".size") == 0;
		}
		else if (*suffix != '\0')
			goto next_entry;
		if (posix_walidx_name(tl, (uint32_t) parsed_shard, epoch,
							 canonical, sizeof(canonical)) != 0 ||
			posix_walidx_watermark_name(canonical, marker, sizeof(marker)) != 0 ||
			strcmp(entry->d_name, is_marker ? marker : canonical) != 0)
			goto next_entry;
		if (epoch >= keep_epochs[parsed_shard])
			goto next_entry;
		if (unlinkat(directory_fd, entry->d_name, 0) != 0)
			goto cleanup;
		removed = 1;
next_entry:
		errno = 0;
	}
	if (errno != 0 || (removed && fsync(directory_fd) != 0))
		goto cleanup;
	rc = removed;

cleanup:
	if (dir != NULL)
		closedir(dir);
	else if (scan_fd >= 0)
		close(scan_fd);
	if (directory_fd >= 0)
		close(directory_fd);
	return rc;
}

static int
posix_all_digits(const char *begin, const char *end)
{
	if (begin == end)
		return 0;
	for (const char *p = begin; p < end; p++)
		if (*p < '0' || *p > '9')
			return 0;
	return 1;
}

/* The writer formats these fields with the ordinary non-negative decimal form.
 * Keep the cleanup matcher equally narrow: a stale name is harmless only if
 * it is exactly a name this producer could have created. */
static int
posix_canonical_decimal(const char *begin, const char *end,
						unsigned long long max, unsigned long long *value)
{
	unsigned long long parsed = 0;
	char canonical[32];
	int n;

	if (begin == end || (end - begin > 1 && *begin == '0'))
		return 0;
	for (const char *p = begin; p < end; p++)
	{
		unsigned int digit;

		if (*p < '0' || *p > '9')
			return 0;
		digit = (unsigned int) (*p - '0');
		if (digit > max || parsed > (max - digit) / 10)
			return 0;
		parsed = parsed * 10 + digit;
	}
	n = snprintf(canonical, sizeof(canonical), "%llu", parsed);
	if (n < 0 || (size_t) n != (size_t) (end - begin) ||
		memcmp(begin, canonical, (size_t) n) != 0)
		return 0;
	if (value != NULL)
		*value = parsed;
	return 1;
}

static int
posix_canonical_pid(const char *begin, const char *end)
{
	unsigned long long parsed;
	pid_t pid;
	char canonical[32];
	int n;

	/* The producer casts getpid() to long before formatting it.  Round-trip
	 * through pid_t and long so cleanup accepts the producer's actual domain,
	 * rather than every value accepted by an arbitrary integer parser. */
	if (!posix_canonical_decimal(begin, end, (unsigned long long) LONG_MAX,
								&parsed))
		return 0;
	pid = (pid_t) parsed;
	if (pid <= 0 || (long) pid != (long) parsed)
		return 0;
	n = snprintf(canonical, sizeof(canonical), "%ld", (long) pid);
	return n >= 0 && (size_t) n == (size_t) (end - begin) &&
		memcmp(begin, canonical, (size_t) n) == 0;
}

static int
posix_alnum6(const char *name)
{
	for (int i = 0; i < 6; i++)
		if (!((name[i] >= '0' && name[i] <= '9') ||
			  (name[i] >= 'A' && name[i] <= 'Z') ||
			  (name[i] >= 'a' && name[i] <= 'z')))
			return 0;
	return name[6] == '\0';
}

static int
posix_timeline_walidx_entry(uint32_t tl, const char *name)
{
	char prefix[64];
	char canonical[160];
	char watermark[192];
	const char *p;
	char *end;
	unsigned long shard;
	unsigned long long epoch;
	int n;

	n = snprintf(prefix, sizeof(prefix), "walidx_%u_", tl);
	if (n < 0 || (size_t) n >= sizeof(prefix) ||
		strncmp(name, prefix, (size_t) n) != 0)
		return 0;
	p = name + n;
	errno = 0;
	shard = strtoul(p, &end, 10);
	if (errno != 0 || end == p || shard > UINT32_MAX ||
		!posix_all_digits(p, end))
		return -1;
	if (*end == '\0')
	{
		if (posix_walidx_name(tl, (uint32_t) shard, 0,
						  canonical, sizeof(canonical)) != 0 ||
			strcmp(name, canonical) != 0)
			return -1;
		return 1;
	}
	if (strncmp(end, "_e", 2) != 0)
		return -1;
	p = end + 2;
	if (strlen(p) < 20 || !posix_all_digits(p, p + 20))
		return -1;
	errno = 0;
	epoch = strtoull(p, &end, 10);
	if (errno != 0 || epoch == 0 || end != p + 20)
		return -1;
	if (*end == '\0' || (strcmp(end, ".size") == 0))
	{
		if (posix_walidx_name(tl, (uint32_t) shard, epoch,
						  canonical, sizeof(canonical)) != 0)
			return -1;
		if (strcmp(name, canonical) == 0)
			return 1;
		if (posix_walidx_watermark_name(canonical, watermark,
							 sizeof(watermark)) != 0)
			return -1;
		return strcmp(name, watermark) == 0 ? 1 : -1;
	}
	/* Exact generated watermark temporary: <canonical>.size.tmp.<pid>.<try>. */
	if (strncmp(end, ".size.tmp.", 10) == 0)
	{
		const char *q = end + 10;
		const char *dot = strchr(q, '.');
		size_t base_len;

		if (dot == NULL || !posix_canonical_pid(q, dot) ||
			!posix_canonical_decimal(dot + 1, name + strlen(name), 127,
										NULL))
			return -1;
		if (posix_walidx_name(tl, (uint32_t) shard, epoch,
						  canonical, sizeof(canonical)) != 0)
			return -1;
		base_len = (size_t) (end - name);
		if (strlen(canonical) != base_len ||
			memcmp(name, canonical, base_len) != 0)
			return -1;
		return 1;
	}
	return -1;
}

static int
posix_wal_store_entry(uint32_t tl, const char *name)
{
	char prefix[64];
	char canonical[160];
	const char *p;
	char *end;
	unsigned long long segment;
	int n;

	if (strcmp(name, PS_WAL_STORE_IDENTITY_FILE) == 0)
		return 1;
	if (strncmp(name, PS_WAL_STORE_IDENTITY_FILE ".tmp.",
			   strlen(PS_WAL_STORE_IDENTITY_FILE ".tmp.")) == 0)
		return posix_alnum6(name + strlen(PS_WAL_STORE_IDENTITY_FILE ".tmp.")) ? 1 : -1;
	n = snprintf(prefix, sizeof(prefix), "walv1_%u_", tl);
	if (n < 0 || (size_t) n >= sizeof(prefix) ||
		strncmp(name, prefix, (size_t) n) != 0)
		return 0;
	p = name + n;
	if (strlen(p) < 20 || !posix_all_digits(p, p + 20))
		return -1;
	errno = 0;
	segment = strtoull(p, &end, 10);
	if (errno != 0 || end != p + 20 || segment == UINT64_MAX)
		return -1;
	n = snprintf(canonical, sizeof(canonical), "walv1_%u_%020llu", tl,
				 (unsigned long long) segment);
	if (n < 0 || (size_t) n >= sizeof(canonical))
		return -1;
	if (strcmp(name, canonical) == 0)
		return 1;
	if (strncmp(end, ".tmp.", 5) == 0 && posix_alnum6(end + 5))
		return 1;
	return -1;
}

static int
posix_snapshot_entry(const char *name)
{
	const char *p;
	char *end;
	unsigned long long generation;
	unsigned long shard;

	if (strcmp(name, "walidx_manifest_v1") == 0 ||
		strcmp(name, "walidx_prepared_v1") == 0)
		return 1;
	if (strncmp(name, "walidx_manifest_v1.tmp.", 23) == 0)
	{
		const char *dot;

		p = name + 23;
		if (strlen(p) < 20 || !posix_all_digits(p, p + 20))
			return -1;
		errno = 0;
		generation = strtoull(p, &end, 10);
		if (errno != 0 || generation == 0 || end != p + 20 || *end++ != '.')
			return -1;
		p = end;
		dot = strchr(p, '.');
		if (dot == NULL || !posix_all_digits(p, dot) ||
			!posix_all_digits(dot + 1, name + strlen(name)))
			return -1;
		return 1;
	}
	if (strncmp(name, "walidx_prepared_v1.tmp.", 23) == 0)
	{
		const char *dot;

		p = name + 23;
		dot = strchr(p, '.');
		if (dot == NULL || !posix_all_digits(p, dot) ||
			!posix_all_digits(dot + 1, name + strlen(name)))
			return -1;
		return 1;
	}
	if (strncmp(name, "walidxg1_", 9) != 0)
		return -1;
	p = name + 9;
	if (strlen(p) < 24 || !posix_all_digits(p, p + 20) || p[20] != '_')
		return -1;
	errno = 0;
	generation = strtoull(p, &end, 10);
	if (errno != 0 || generation == 0 || end != p + 20)
		return -1;
	p = end + 1;
	if (!posix_all_digits(p, p + 3))
		return -1;
	errno = 0;
	shard = strtoul(p, &end, 10);
	if (errno != 0 || shard >= 128 || end != p + 3)
		return -1;
	if (*end == '\0')
		return 1;
	if (strncmp(end, ".tmp.", 5) != 0)
		return -1;
	p = end + 5;
	{
		const char *dot = strchr(p, '.');

		return dot != NULL && posix_all_digits(p, dot) &&
			posix_all_digits(dot + 1, name + strlen(name)) ? 1 : -1;
	}
}

static int
posix_validate_entry(int directory_fd, const char *name, int recognized)
{
	struct stat st;

	if (recognized <= 0 || fstatat(directory_fd, name, &st,
							AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(st.st_mode))
		return -1;
	return 0;
}

static int
posix_validate_private_dir(int root_fd, const char *name, uint32_t tl,
						   int wal_store)
{
	struct dirent *entry;
	DIR *dir = NULL;
	int dir_fd = -1;
	int scan_fd = -1;
	int rc = -1;

	dir_fd = openat(root_fd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (dir_fd < 0)
		return errno == ENOENT ? 0 : -1;
	scan_fd = fcntl(dir_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto done;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		int recognized;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		recognized = wal_store ? posix_wal_store_entry(tl, entry->d_name) :
			posix_snapshot_entry(entry->d_name);
		if (posix_validate_entry(dir_fd, entry->d_name, recognized) != 0)
		{
			fprintf(stderr, "pagestore: refusing timeline WAL cleanup for %s/%s\n",
					name, entry->d_name);
			goto done;
		}
	}
	if (errno != 0)
		goto done;
	rc = 0;
done:
	if (dir != NULL)
		closedir(dir);
	else if (scan_fd >= 0)
		close(scan_fd);
	close(dir_fd);
	return rc;
}

static int
posix_remove_private_dir(int root_fd, const char *name, uint32_t tl,
						 int wal_store)
{
	struct dirent *entry;
	DIR *dir = NULL;
	int dir_fd = -1;
	int scan_fd = -1;
	int removed = 0;
	int rc = -1;
	int dir_fsync_rc;
	int dir_close_rc;

	dir_fd = openat(root_fd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (dir_fd < 0)
		return errno == ENOENT ? 0 : -1;
	scan_fd = fcntl(dir_fd, F_DUPFD_CLOEXEC, 0);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto done;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		int recognized = (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0) ? 1 :
			wal_store ? posix_wal_store_entry(tl, entry->d_name) :
			posix_snapshot_entry(entry->d_name);

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		if (posix_validate_entry(dir_fd, entry->d_name, recognized) != 0 ||
			(unlinkat(dir_fd, entry->d_name, 0) != 0 && errno != ENOENT))
			goto done;
		removed = 1;
	}
	if (errno != 0)
		goto done;
	if (test_fail_timeline_cleanup_private_dir_fsync > 0 &&
		--test_fail_timeline_cleanup_private_dir_fsync == 0)
	{
		errno = EIO;
		dir_fsync_rc = -1;
	}
	else
		dir_fsync_rc = fsync(dir_fd);
	/* Do not short-circuit closedir after a failed fsync: dir owns scan_fd,
	 * and every retry must release it before returning the error. */
	dir_close_rc = closedir(dir);
	dir = NULL;
	if (dir_fsync_rc != 0 || dir_close_rc != 0)
	{
		goto done;
	}
	if (close(dir_fd) != 0)
	{
		dir_fd = -1;
		goto done;
	}
	dir_fd = -1;
	if (unlinkat(root_fd, name, AT_REMOVEDIR) != 0 && errno != ENOENT)
		goto done;
	if (fsync(root_fd) != 0)
		goto done;
	(void) removed;
	rc = 1;
done:
	if (dir != NULL)
		closedir(dir);
	else if (scan_fd >= 0)
		close(scan_fd);
	if (dir_fd >= 0)
		close(dir_fd);
	return rc;
}

static int
posix_timeline_root_entry(uint32_t tl, const char *name,
						  const char *wal_name,
						  const char *wal_rewrite_name,
						  const char *walidx_prefix)
{
	if (strcmp(name, wal_name) == 0 || strcmp(name, wal_rewrite_name) == 0)
		return 1;
	if (strncmp(name, wal_name, strlen(wal_name)) == 0 &&
		name[strlen(wal_name)] == '.')
		return -1;
	if (strncmp(name, walidx_prefix, strlen(walidx_prefix)) == 0)
		return posix_timeline_walidx_entry(tl, name);
	return 0;
}

static int
posix_timeline_wal_cleanup(uint32_t tl)
{
	char wal_name[64];
	char wal_rewrite_name[96];
	char walidx_prefix[64];
	char wal_store[64];
	char snapshots[64];
	struct dirent *entry;
	DIR *dir = NULL;
	int root_fd = -1;
	int scan_fd = -1;
	int removed = 0;
	int rc = -1;

	if (tl == 0 || tl == UINT32_MAX ||
		snprintf(wal_name, sizeof(wal_name), "wal_%u", tl) < 0 ||
		snprintf(wal_rewrite_name, sizeof(wal_rewrite_name),
				 "wal_%u.rewrite.tmp", tl) < 0 ||
		snprintf(walidx_prefix, sizeof(walidx_prefix), "walidx_%u_", tl) < 0 ||
		snprintf(wal_store, sizeof(wal_store), "wal_segments_%u", tl) < 0 ||
		snprintf(snapshots, sizeof(snapshots), "walidx_snapshots_%u", tl) < 0)
		return -1;
	root_fd = open(posix_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (root_fd < 0)
		return -1;
	if (posix_validate_private_dir(root_fd, wal_store, tl + 1, 1) < 0 ||
		posix_validate_private_dir(root_fd, snapshots, tl, 0) < 0)
		goto done;
	/* Validate the complete shared-root target set before unlinking any entry.
	 * This keeps an unexpected target-shaped artifact fail-closed without
	 * partially deleting otherwise valid target files. */
	scan_fd = openat(root_fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto done;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		int recognized;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		recognized = posix_timeline_root_entry(tl, entry->d_name,
										 wal_name, wal_rewrite_name,
										 walidx_prefix);
		if (recognized < 0)
		{
			fprintf(stderr, "pagestore: refusing timeline WAL cleanup for %s\n",
					entry->d_name);
			goto done;
		}
		if (recognized != 0 &&
			posix_validate_entry(root_fd, entry->d_name, recognized) != 0)
			goto done;
	}
	if (errno != 0 || closedir(dir) != 0)
	{
		dir = NULL;
		goto done;
	}
	dir = NULL;
	/* Re-scan after complete validation and remove only exact target entries. */
	scan_fd = openat(root_fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto done;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		int target = posix_timeline_root_entry(tl, entry->d_name,
									   wal_name, wal_rewrite_name,
									   walidx_prefix);

		if (target < 0)
			goto done;
		if (target && unlinkat(root_fd, entry->d_name, 0) != 0 && errno != ENOENT)
			goto done;
		removed |= target;
	}
	if (errno != 0 || closedir(dir) != 0 || (removed && fsync(root_fd) != 0))
	{
		dir = NULL;
		goto done;
	}
	dir = NULL;
	if (getenv("PAGESTORE_TEST_FAIL_TIMELINE_CLEANUP_AFTER_ROOT") != NULL)
	{
		errno = EIO;
		goto done;
	}
	if (posix_remove_private_dir(root_fd, wal_store, tl + 1, 1) < 0 ||
		posix_remove_private_dir(root_fd, snapshots, tl, 0) < 0)
		goto done;
	/* Revalidate after deletion.  Internal publishers are drained by the core;
	 * this final pass also refuses a target-shaped entry introduced by an
	 * unsupported concurrent external writer instead of declaring completion. */
	scan_fd = openat(root_fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (scan_fd < 0 || (dir = fdopendir(scan_fd)) == NULL)
		goto done;
	scan_fd = -1;
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		int target = posix_timeline_root_entry(tl, entry->d_name,
									   wal_name, wal_rewrite_name,
									   walidx_prefix);

		if (strcmp(entry->d_name, wal_store) == 0 ||
			strcmp(entry->d_name, snapshots) == 0)
			target = 1;
		if (target != 0)
			goto done;
	}
	if (errno != 0 || closedir(dir) != 0)
	{
		dir = NULL;
		goto done;
	}
	dir = NULL;
	/* Always sync on a successful retry.  If an earlier unlink/rmdir became
	 * visible but its directory fsync reported an ambiguous failure, an empty
	 * target set is the operation that closes that durability ambiguity. */
	if (fsync(root_fd) != 0)
		goto done;
	rc = 0;
done:
	if (dir != NULL)
		closedir(dir);
	else if (scan_fd >= 0)
		close(scan_fd);
	if (root_fd >= 0)
		close(root_fd);
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
posix_log_truncate(const char *name, uint64_t len)
{
	char		path[4096];
	int			fd;
	int			rc = 0;

	snprintf(path, sizeof(path), "%s/%s", posix_dir, name);
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

static int
posix_meta_truncate(uint64_t len)
{
	return posix_log_truncate("timelines", len);
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
	return posix_log_truncate("forkmeta", len);
}

static int
posix_fork_meta_rewrite(const void *buf, uint32_t len)
{
	char path[4096], tmp[4096];
	int fd = -1, dfd = -1, rc = -1;
	uint32_t off = 0;

	if (buf == NULL && len != 0)
		return -1;
	pthread_mutex_lock(&posix_log_lock);
	snprintf(path, sizeof(path), "%s/forkmeta", posix_dir);
	snprintf(tmp, sizeof(tmp), "%s/forkmeta.rewrite.tmp", posix_dir);
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
	if (fd < 0)
		goto out;
	while (off < len)
	{
		ssize_t n = write(fd, (const unsigned char *) buf + off, len - off);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto out;
		off += (uint32_t) n;
	}
	{
		int sync_rc = fsync(fd);
		int close_rc = close(fd);

		fd = -1;
		if (sync_rc != 0 || close_rc != 0)
			goto out;
	}
	if (test_fail_fork_meta_rewrite_before_rename > 0 &&
		--test_fail_fork_meta_rewrite_before_rename == 0)
	{
		errno = EIO;
		goto out;
	}
	if (rename(tmp, path) != 0)
		goto out;
	dfd = open(posix_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (dfd < 0 || (test_fail_fork_meta_rewrite_dir_fsync > 0 &&
		--test_fail_fork_meta_rewrite_dir_fsync == 0) || fsync(dfd) != 0)
	{
		if (dfd >= 0)
			close(dfd);
		dfd = -1;
		errno = EIO;
		goto out;
	}
	if (close(dfd) != 0)
	{
		dfd = -1;
		errno = EIO;
		goto out;
	}
	dfd = -1;
	rc = 0;
out:
	if (fd >= 0)
		close(fd);
	if (dfd >= 0)
		close(dfd);
	if (rc != 0)
		(void) unlink(tmp);
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
	.wal_rewrite_prefix = posix_wal_rewrite_prefix,
	.walidx_append = posix_walidx_append,
	.walidx_read = posix_walidx_read,
	.walidx_truncate = posix_walidx_truncate,
	.walidx_epoch_create = posix_walidx_epoch_create,
	.walidx_epoch_gc = posix_walidx_epoch_gc,
	.timeline_wal_cleanup = posix_timeline_wal_cleanup,
	.meta_append = posix_meta_append,
	.meta_read = posix_meta_read,
	.meta_truncate = posix_meta_truncate,
	.meta_rewrite = posix_meta_rewrite,
	.fork_meta_append = posix_fork_meta_append,
	.fork_meta_read = posix_fork_meta_read,
	.fork_meta_truncate = posix_fork_meta_truncate,
	.fork_meta_rewrite = posix_fork_meta_rewrite,
};
