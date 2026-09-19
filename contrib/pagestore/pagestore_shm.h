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

/* Map a shm name onto its backing file: /tmp/pagestore-shm-<uid>/<name>. */
static inline int
ps_shm_backing_path(const char *name, char *out, size_t outlen)
{
	size_t		i;
	int			n;

	if (name == NULL || name[0] == '\0')
	{
		errno = EINVAL;
		return -1;
	}
	if (name[0] == '/')
		name++;
	n = snprintf(out, outlen, "/tmp/pagestore-shm-%lu/",
				 (unsigned long) getuid());
	if (n < 0 || (size_t) n >= outlen ||
		strlen(name) == 0 || strlen(name) >= outlen - (size_t) n)
	{
		errno = ENAMETOOLONG;
		return -1;
	}
	/* Slashes inside the name would escape the directory. */
	for (i = 0; name[i] != '\0'; i++)
		out[(size_t) n + i] = name[i] == '/' ? '_' : name[i];
	out[(size_t) n + i] = '\0';
	return 0;
}

static inline int
ps_shm_open(const char *name, int oflag, mode_t mode)
{
	char		path[4096];
	char	   *slash;
	int			fd;

	if (ps_shm_backing_path(name, path, sizeof(path)) != 0)
		return -1;
	if (oflag & O_CREAT)
	{
		/* Create the per-user directory lazily; it is private to this uid so
		 * no other user can plant a file or symlink under it. */
		slash = strrchr(path, '/');
		*slash = '\0';
		if (mkdir(path, 0700) != 0 && errno != EEXIST)
			return -1;
		*slash = '/';
	}
	fd = open(path, oflag | O_NOFOLLOW | O_CLOEXEC, mode);
	if (fd < 0)
		return -1;
	return fd;
}

static inline int
ps_shm_unlink(const char *name)
{
	char		path[4096];

	if (ps_shm_backing_path(name, path, sizeof(path)) != 0)
		return -1;
	return unlink(path);
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
