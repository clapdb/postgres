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

/*
 * Return 1 when a live daemon owns the segment and has finished initializing
 * it, 0 when it does not, and -1 (errno set) when the locks cannot be probed.
 *
 * A READY header alone is not proof: a daemon killed with SIGKILL leaves its
 * header READY, and the next daemon only invalidates it after it has taken
 * its locks.  The daemon holds lease_byte for its whole lifetime and holds
 * init_byte from before it invalidates the header until it publishes READY,
 * so the segment is ready only while lease_byte has a holder that does not
 * also hold init_byte.  Inspectors take init_byte briefly as well; when the
 * holders' pids cannot be compared (another pid namespace reports 0), any
 * init_byte holder counts as initialization, which a caller only sees as a
 * transient "not ready".  F_GETLK needs no write access, so read-only
 * mappings can use this too.  Callers check the header's own fields as well;
 * probing the locks after reading it closes the window where a stale header
 * is read just before a new daemon takes over.
 *
 * The lease and init bytes are queried with two separate F_GETLK calls, so a
 * fast daemon restart between them can straddle two generations: the lease
 * query sees old daemon A (about to exit), A exits, successor B takes both
 * locks, and the init query then sees B holding init_byte.  A's and B's pids
 * differ, which looks exactly like an inspector holding init_byte alongside
 * a live daemon, so without more the segment would be reported ready while
 * B is still initializing.
 *
 * That ambiguity only arises when the init query finds a holder: if the init
 * query instead finds init_byte free, no pid comparison is needed at all.
 * The daemon holds init_byte continuously from before it invalidates the
 * header until it publishes READY, and the init query runs strictly after
 * the (successful) first lease query, so init_byte being free at that later
 * instant already proves that whoever currently holds the lease has passed
 * READY -- there is no live daemon still initializing right now, regardless
 * of whether it is the same daemon the first query saw.  A stale first-query
 * pid is harmless here, including the pid=0 that F_GETLK reports when the
 * lock holder is in a different pid namespace (a routine case for a
 * containerized daemon vs. an inspector on the host, or vice versa): pid
 * comparison is simply not needed to reach this conclusion.
 *
 * When the init query does find a holder, that holder's pid decides between
 * "a new daemon is still initializing" and "an inspector took init_byte
 * alongside an already-live daemon", and here the pids do have to be
 * compared, so a pid of 0 (or of the same process) is conservatively treated
 * as not ready.  Even a differing, known pid is not proof enough on its own,
 * because it is exactly what the fast-restart race above also produces: to
 * tell the two apart we query the lease byte a third time, after the init
 * query, and only accept the inspector explanation -- and report ready --
 * when that third query still finds the same holder (by pid) as the first
 * query.  If the restart happened, the third query now sees B, which
 * differs from A's first-query pid (or the lease is briefly unheld between
 * A's exit and B's acquisition), so we correctly fall through to "not
 * ready"; if no restart happened, the same daemon still holds the lease
 * throughout and the pids match.
 *
 * The decision itself lives in ps_shm_daemon_ready_decide() below, taking
 * the three F_GETLK results as plain data, so it can be unit tested with
 * hand-built results (including a changed pid between the first and third)
 * without needing to actually race two processes through the real window.
 */
static inline int
ps_shm_daemon_ready_decide(const struct flock *first_lease,
							const struct flock *init,
							const struct flock *third_lease)
{
	if (first_lease->l_type == F_UNLCK)
		return 0;
	if (init->l_type == F_UNLCK)
		return 1;
	if (init->l_pid <= 0 || first_lease->l_pid <= 0 ||
		init->l_pid == first_lease->l_pid)
		return 0;
	/* The inspector exception: confirm the lease holder hasn't changed. */
	if (third_lease->l_type == F_UNLCK)
		return 0;
	if (third_lease->l_pid <= 0 || third_lease->l_pid != first_lease->l_pid)
		return 0;
	return 1;
}

static inline int
ps_shm_daemon_ready(int fd, off_t init_byte, off_t lease_byte)
{
	struct flock first_lease;
	struct flock init;
	struct flock third_lease;

	memset(&first_lease, 0, sizeof(first_lease));
	first_lease.l_type = F_WRLCK;
	first_lease.l_whence = SEEK_SET;
	first_lease.l_start = lease_byte;
	first_lease.l_len = 1;
	if (fcntl(fd, F_GETLK, &first_lease) != 0)
		return -1;
	if (first_lease.l_type == F_UNLCK)
		return 0;

	memset(&init, 0, sizeof(init));
	init.l_type = F_WRLCK;
	init.l_whence = SEEK_SET;
	init.l_start = init_byte;
	init.l_len = 1;
	if (fcntl(fd, F_GETLK, &init) != 0)
		return -1;

	/*
	 * The third lease query is only needed for the inspector exception
	 * (see the comment above); when init_byte is free, skip it, since
	 * ps_shm_daemon_ready_decide() does not look at it in that case.
	 */
	memset(&third_lease, 0, sizeof(third_lease));
	if (init.l_type != F_UNLCK)
	{
		third_lease.l_type = F_WRLCK;
		third_lease.l_whence = SEEK_SET;
		third_lease.l_start = lease_byte;
		third_lease.l_len = 1;
		if (fcntl(fd, F_GETLK, &third_lease) != 0)
			return -1;
	}

	return ps_shm_daemon_ready_decide(&first_lease, &init, &third_lease);
}

#endif							/* PAGESTORE_SHM_H */
