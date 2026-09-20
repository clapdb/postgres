/*-------------------------------------------------------------------------
 *
 * pagestore_shm.h
 *	  Portable open/unlink of the daemon's shared-memory IPC segment.
 *
 * Every pagestore process (daemon, backend, tools, tests) names the IPC
 * segment by a POSIX shm name such as "/pagestore".  On Linux that maps
 * straight onto shm_open()/shm_unlink().  macOS implements POSIX shm objects
 * as anonymous kernel memory rather than files: fcntl() record locks return
 * EBADF, ftruncate() succeeds only once per object, and names are limited to
 * PSHMNAMLEN (31) bytes.  The inspection lease protocol depends on the first
 * two, so on macOS the segment is a regular file in a per-user directory
 * instead; MAP_SHARED mappings of one file are coherent across processes on
 * Darwin exactly like a shm object.
 *
 * Included by BOTH the PG-side module and the standalone daemon, so it uses
 * only libc.
 *
 * src/../contrib/pagestore/pagestore_shm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_SHM_H
#define PAGESTORE_SHM_H

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __APPLE__

/* Backing files live in /tmp/pagestore-shm-<uid>/<name>.  /tmp is sticky
 * and world-writable, so the directory's name is predictable and another
 * local user could pre-create it; every use therefore validates the directory
 * (and the segment) as a private, uid-owned, non-symlink object before
 * touching anything inside it. */
static inline int
ps_shm_backing_dir(char *out, size_t outlen)
{
	int			n = snprintf(out, outlen, "/tmp/pagestore-shm-%lu",
							 (unsigned long) getuid());

	if (n < 0 || (size_t) n >= outlen)
	{
		errno = ENAMETOOLONG;
		return -1;
	}
	return 0;
}

static inline int
ps_shm_backing_name(const char *name, char *out, size_t outlen)
{
	size_t		i;

	if (name == NULL || name[0] == '\0')
	{
		errno = EINVAL;
		return -1;
	}
	if (name[0] == '/')
		name++;
	if (name[0] == '\0' || strlen(name) >= outlen)
	{
		errno = ENAMETOOLONG;
		return -1;
	}
	/* Slashes inside the name would escape the directory. */
	for (i = 0; name[i] != '\0'; i++)
		out[i] = name[i] == '/' ? '_' : name[i];
	out[i] = '\0';
	return 0;
}

/* Reject anything that is not owned by this uid or is reachable by others. */
static inline int
ps_shm_private_object(int fd, mode_t type)
{
	struct stat st;

	if (fstat(fd, &st) != 0)
		return -1;
	if ((st.st_mode & S_IFMT) != type || st.st_uid != getuid() ||
		(st.st_mode & (S_IRWXG | S_IRWXO)) != 0)
	{
		errno = EPERM;
		return -1;
	}
	return 0;
}

/* Open the per-user directory, creating it when asked, and prove it is a
 * private directory of ours rather than something another user planted. */
static inline int
ps_shm_open_backing_dir(int create)
{
	char		dir[128];
	int			dirfd;

	if (ps_shm_backing_dir(dir, sizeof(dir)) != 0)
		return -1;
	if (create && mkdir(dir, 0700) != 0 && errno != EEXIST)
		return -1;
	dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (dirfd < 0)
		return -1;
	if (ps_shm_private_object(dirfd, S_IFDIR) != 0)
	{
		int			saved = errno;

		(void) close(dirfd);
		errno = saved;
		return -1;
	}
	return dirfd;
}

static inline int
ps_shm_open(const char *name, int oflag, mode_t mode)
{
	char		file[256];
	int			dirfd;
	int			fd;
	int			saved;

	if (ps_shm_backing_name(name, file, sizeof(file)) != 0)
		return -1;
	dirfd = ps_shm_open_backing_dir((oflag & O_CREAT) != 0);
	if (dirfd < 0)
		return -1;
	fd = openat(dirfd, file, oflag | O_NOFOLLOW | O_CLOEXEC, mode & 0700);
	saved = errno;
	(void) close(dirfd);
	if (fd < 0)
	{
		errno = saved;
		return -1;
	}
	if (ps_shm_private_object(fd, S_IFREG) != 0)
	{
		saved = errno;
		(void) close(fd);
		errno = saved;
		return -1;
	}
	return fd;
}

static inline int
ps_shm_unlink(const char *name)
{
	char		file[256];
	int			dirfd;
	int			rc;
	int			saved;

	if (ps_shm_backing_name(name, file, sizeof(file)) != 0)
		return -1;
	dirfd = ps_shm_open_backing_dir(0);
	if (dirfd < 0)
		return -1;
	rc = unlinkat(dirfd, file, 0);
	saved = errno;
	(void) close(dirfd);
	errno = saved;
	return rc;
}

#else							/* !__APPLE__ */

static inline int
ps_shm_open(const char *name, int oflag, mode_t mode)
{
	return shm_open(name, oflag, mode);
}

static inline int
ps_shm_unlink(const char *name)
{
	return shm_unlink(name);
}

#endif							/* __APPLE__ */

#endif							/* PAGESTORE_SHM_H */
