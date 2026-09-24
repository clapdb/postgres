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
#include <time.h>
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
 * A READY header alone is not proof a live daemon owns the segment: a
 * daemon killed with SIGKILL leaves its header READY, and its successor
 * only invalidates that header after taking its own locks.  An earlier
 * version of this file tried to tell a live daemon from a dead one with
 * two independent F_GETLK probes (one of the lease byte, one of the init
 * byte) plus pid comparisons to rule out a fast restart landing between
 * them.  That scheme had a real gap (E-8 P1): the lease probe can observe
 * daemon A still alive, A can then exit, and the init probe -- run
 * strictly afterwards, so it necessarily sees init_byte free -- was
 * treated as proof the segment is ready, even though by then nobody holds
 * the lease at all.  F_GETLK never blocks and never takes anything, so
 * nothing stops that exact interleaving from landing between the two
 * probes; no amount of extra pid bookkeeping closes it, because the two
 * probes are simply not atomic with each other.
 *
 * The fix is to stop probing and instead take a real, non-blocking lock
 * that serializes with the daemon's own initialization protocol:
 *
 *   ps_shm_hold_init_shared() takes a *shared* (F_RDLCK) lock on init_byte.
 *   The daemon takes init_byte *exclusively* (F_WRLCK) from before it
 *   invalidates the header until it publishes READY (see pagestore_daemon.c
 *   / pagestore_daemon_spdk.c), and holds lease_byte exclusively for its
 *   entire lifetime after that.  A shared lock on init_byte therefore
 *   cannot be acquired while any daemon is between invalidating the header
 *   and publishing READY, and while our shared lock is held, no daemon can
 *   begin that window either (F_RDLCK excludes a concurrent F_WRLCK).  So:
 *   for as long as we hold init_byte shared, no daemon is initializing and
 *   none can start.  If ps_shm_lease_held() then finds a holder of
 *   lease_byte, that holder must already be past READY -- there is no
 *   other way to be holding the lease while init_byte cannot be taken
 *   exclusively -- and the header we validate while still holding the lock
 *   is that live daemon's own, stable, already-published header.  No pids
 *   are compared anywhere in this protocol, so pid namespaces (which made
 *   F_GETLK report l_pid=0 for a containerized daemon) are simply not a
 *   concern any more.
 *
 * This only reasons about daemons that are still alive when we take the
 * lock.  A daemon that dies after we've released it (i.e. after we've
 * already decided "ready" and gone on to use the segment) cannot be
 * detected by any probe taken before that -- per the MVP contract,
 * clients must be stopped across a daemon restart; this mechanism answers
 * "is a daemon alive and ready right now", not "will it stay so".
 *
 * F_RDLCK/F_GETLK need no write access, so a read-only fd (O_RDONLY) can
 * use all three of these.  A caller must always pair a successful
 * ps_shm_hold_init_shared() with ps_shm_release_init_shared(), on every
 * exit path, including error paths -- these are ordinary process-owned
 * POSIX record locks, so closing every fd on the segment also drops them,
 * but a long-lived process (e.g. a backend that keeps its shm fd open)
 * must not leave the shared lock held past the check that needs it, or it
 * would make a future daemon restart's exclusive init_byte acquisition
 * wait for that process's entire lifetime instead of ~10s.
 *
 * The relation-inspection path (pagestore_inspect.c) is a client of
 * init_byte too, but it needs *exclusive* ownership of the mailbox it
 * shares with the daemon while it runs, not just a readiness check; it
 * takes F_WRLCK on init_byte directly (via its own lock_relation_shm(),
 * unchanged by this) rather than going through
 * ps_shm_hold_init_shared().
 */
static inline int
ps_shm_hold_init_shared(int fd, off_t init_byte)
{
	struct flock lock;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_RDLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = init_byte;
	lock.l_len = 1;
	if (fcntl(fd, F_SETLK, &lock) == 0)
		return 1;
	if (errno == EACCES || errno == EAGAIN)
		return 0;
	return -1;
}

/*
 * Default bound for waiting out a transient exclusive holder of init_byte
 * (a daemon initializing, or relation inspection's own ~5s-bounded hold):
 * comfortably larger than either, so a routine wait never spuriously times
 * out.  Shared by ps_shm_hold_init_shared_wait() and by callers (such as
 * backend_localsvc.c's ls_attach()) that must drive their own interruptible
 * retry loop instead of using it directly.
 */
#define PS_INIT_LOCK_WAIT_MS	10000

/*
 * Bounded-wait version of ps_shm_hold_init_shared(): retry on EAGAIN/EACCES
 * (~10ms steps) until timeout_ms has elapsed, then give up.  Returns 1/0/-1
 * exactly like the non-blocking version.
 *
 * The two legitimate exclusive holders of init_byte -- a daemon
 * initializing, and relation inspection's own mailbox-ownership hold (see
 * pagestore_inspect.c) -- are both bounded (initialization completes in a
 * bounded time; relation inspection times out after ~5s), so a client that
 * only needs *a* live daemon, not the relation-inspection mailbox itself,
 * can simply wait the exclusive hold out instead of failing immediately.
 * Callers should pick a timeout comfortably larger than relation
 * inspection's own timeout (e.g. 10000ms) so a routine relation-inspection
 * call never produces a spurious "no running, initialized daemon" error.
 * This cannot deadlock against the daemon's own byte-zero retry (see
 * pagestore_daemon.c): the daemon holds the lock exclusively only while
 * actually initializing, and readers here only hold it, briefly, shared.
 *
 * This is a plain, uninterruptible libc sleep loop -- fine for the
 * standalone client tools, which have nothing else to service while
 * waiting.  A caller that must stay responsive to interrupts (e.g. a
 * PostgreSQL backend) should not use this function; it should drive its
 * own loop around the non-blocking ps_shm_hold_init_shared(), checking for
 * interrupts between retries (see backend_localsvc.c's ls_attach()).
 */
static inline int
ps_shm_hold_init_shared_wait(int fd, off_t init_byte, int timeout_ms)
{
	int			elapsed_ms = 0;

	for (;;)
	{
		int			held = ps_shm_hold_init_shared(fd, init_byte);

		if (held != 0)
			return held;
		if (elapsed_ms >= timeout_ms)
			return 0;
		{
			struct timespec ts;

			ts.tv_sec = 0;
			ts.tv_nsec = 10000000L;	/* 10ms */
			while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
				;
		}
		elapsed_ms += 10;
	}
}

static inline int
ps_shm_release_init_shared(int fd, off_t init_byte)
{
	struct flock lock;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_UNLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = init_byte;
	lock.l_len = 1;
	return fcntl(fd, F_SETLK, &lock);
}

/*
 * Return 1 when lease_byte has a holder, 0 when it does not, and -1 (errno
 * set) when the lock cannot be probed.  Only meaningful for deciding
 * daemon readiness while the caller holds init_byte (shared or exclusive);
 * see the block comment above.
 */
static inline int
ps_shm_lease_held(int fd, off_t lease_byte)
{
	struct flock lock;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = lease_byte;
	lock.l_len = 1;
	if (fcntl(fd, F_GETLK, &lock) != 0)
		return -1;
	return lock.l_type != F_UNLCK;
}

#endif							/* PAGESTORE_SHM_H */
