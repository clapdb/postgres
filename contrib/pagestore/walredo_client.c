/*-------------------------------------------------------------------------
 *
 * walredo_client.c
 *	  Client for the `postgres --wal-redo` single-page redo helper.
 *
 * See walredo_client.h.  The helper is spawned as a child of this backend with
 * a pipe pair wired to its stdin/stdout; we then drive the length-prefixed
 * binary protocol (native byte order) it implements in
 * src/backend/postmaster/walredo.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <unistd.h>

#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
#include "walredo_client.h"

struct WalRedoProc
{
	pid_t		pid;
	int			wfd;			/* write end -> helper stdin */
	int			rfd;			/* read end  <- helper stdout */
	int			lock_fd;		/* flock on the datadir lock file (cluster-wide) */
};

/* Kill+reap the child, close fds, and raise an error.  Used on any I/O failure
 * (a dead helper shows up as EOF/EPIPE on the next read/write). */
pg_noreturn static void
wc_die(WalRedoProc *p, const char *what)
{
	if (p->pid > 0)
	{
		kill(p->pid, SIGKILL);
		while (waitpid(p->pid, NULL, 0) < 0 && errno == EINTR)
			;
		p->pid = 0;
	}
	if (p->wfd >= 0)
		close(p->wfd);
	if (p->rfd >= 0)
		close(p->rfd);
	p->wfd = p->rfd = -1;
	if (p->lock_fd >= 0)
	{
		close(p->lock_fd);		/* releases the flock */
		p->lock_fd = -1;
	}
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("wal-redo helper: %s", what)));
}

static void
wc_write_all(WalRedoProc *p, const void *buf, size_t len)
{
	const char *b = (const char *) buf;

	while (len > 0)
	{
		ssize_t		n = write(p->wfd, b, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			wc_die(p, "write to helper failed");
		}
		b += n;
		len -= (size_t) n;
	}
}

static void
wc_read_all(WalRedoProc *p, void *buf, size_t len)
{
	char	   *b = (char *) buf;

	while (len > 0)
	{
		ssize_t		n = read(p->rfd, b, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			/*
			 * rfd is non-blocking (set in walredo_start): when the helper has
			 * not produced output yet, wait on the backend latch so a query
			 * cancel / statement timeout is honored promptly (a plain blocking
			 * read() would not be interrupted under SA_RESTART, leaving the
			 * helper -- and the datadir lock -- stuck until it replies).
			 */
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
				(void) WaitLatchOrSocket(MyLatch,
										 WL_LATCH_SET | WL_SOCKET_READABLE |
										 WL_EXIT_ON_PM_DEATH,
										 p->rfd, -1L, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			wc_die(p, "read from helper failed");
		}
		if (n == 0)
			wc_die(p, "helper exited unexpectedly");
		b += n;
		len -= (size_t) n;
	}
}

WalRedoProc *
walredo_start(const char *datadir)
{
	WalRedoProc *p = palloc(sizeof(WalRedoProc));
	int			in[2];
	int			out[2];
	char		lockpath[MAXPGPATH];

	p->pid = 0;
	p->wfd = p->rfd = -1;
	p->lock_fd = -1;

	/*
	 * The helper takes the data-directory lock (postmaster.pid) on the shared
	 * pagestore.walredo_datadir for its whole lifetime, so two helpers against it
	 * collide -- including helpers from different databases, which a per-database
	 * advisory lock would not serialize.  Take an exclusive flock on a lock file
	 * in that directory first: it is OS-level, hence cluster-wide, and is released
	 * when lock_fd is closed (walredo_stop / wc_die / process exit).  Spin with
	 * LOCK_NB + CHECK_FOR_INTERRUPTS so the wait stays cancelable.
	 */
	snprintf(lockpath, sizeof(lockpath), "%s/pagestore_walredo.lock", datadir);
	p->lock_fd = open(lockpath, O_RDWR | O_CREAT, 0600);
	if (p->lock_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open wal-redo lock file \"%s\": %m", lockpath)));
	PG_TRY();
	{
		while (flock(p->lock_fd, LOCK_EX | LOCK_NB) != 0)
		{
			if (errno != EWOULDBLOCK && errno != EINTR)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not lock wal-redo datadir \"%s\": %m", datadir)));
			CHECK_FOR_INTERRUPTS();
			pg_usleep(10000);	/* 10 ms */
		}

		if (pipe(in) != 0)
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("could not create pipe for wal-redo helper: %m")));
		if (pipe(out) != 0)
		{
			int			save = errno;

			close(in[0]);		/* don't leak the first pipe's descriptors */
			close(in[1]);
			errno = save;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("could not create pipe for wal-redo helper: %m")));
		}

		p->pid = fork();
		if (p->pid < 0)
		{
			int			save = errno;

			close(in[0]);
			close(in[1]);
			close(out[0]);
			close(out[1]);
			errno = save;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("could not fork wal-redo helper: %m")));
		}

		if (p->pid == 0)
		{
			/* child: wire pipes to stdin/stdout and exec the helper */
			dup2(in[0], STDIN_FILENO);
			dup2(out[1], STDOUT_FILENO);
			close(in[0]);
			close(in[1]);
			close(out[0]);
			close(out[1]);
			close(p->lock_fd);	/* the parent holds the flock; child must not */
			/* argv[0] must be the full path so the helper can locate its own
			 * executable (InitStandaloneProcess -> find_my_exec) */
			execl(my_exec_path, my_exec_path, "--wal-redo", "-D", datadir, (char *) NULL);
			_exit(127);			/* exec failed */
		}

		/* parent: keep our ends */
		close(in[0]);
		close(out[1]);
		p->wfd = in[1];
		p->rfd = out[0];

		/*
		 * Read end non-blocking so wc_read_all() can wait on the backend latch and
		 * stay cancelable while the helper computes.
		 */
		if (fcntl(p->rfd, F_SETFL, O_NONBLOCK) != 0)
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("could not set wal-redo pipe non-blocking: %m")));
	}
	PG_CATCH();
	{
		/*
		 * On any error -- including a cancel/timeout taken at CHECK_FOR_INTERRUPTS
		 * while queued on the flock -- reap whatever was started and close the raw
		 * fds; none are resource-owned, so they would otherwise leak across the
		 * ERROR unwind.
		 */
		if (p->pid > 0)
		{
			kill(p->pid, SIGKILL);
			while (waitpid(p->pid, NULL, 0) < 0 && errno == EINTR)
				;
			p->pid = 0;
		}
		if (p->wfd >= 0)
		{
			close(p->wfd);
			p->wfd = -1;
		}
		if (p->rfd >= 0)
		{
			close(p->rfd);
			p->rfd = -1;
		}
		if (p->lock_fd >= 0)
		{
			close(p->lock_fd);
			p->lock_fd = -1;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	return p;
}

void
walredo_begin(WalRedoProc *p, RelFileLocator rlocator, ForkNumber forknum,
			  BlockNumber blkno)
{
	uint32		ident[5];

	ident[0] = rlocator.spcOid;
	ident[1] = rlocator.dbOid;
	ident[2] = rlocator.relNumber;
	ident[3] = (uint32) forknum;
	ident[4] = (uint32) blkno;
	wc_write_all(p, "b", 1);
	wc_write_all(p, ident, sizeof(ident));
}

void
walredo_pushbase(WalRedoProc *p, XLogRecPtr base_end_lsn, const char *page)
{
	uint64		lsn = (uint64) base_end_lsn;
	uint32		len = (page != NULL) ? (uint32) BLCKSZ : 0;

	wc_write_all(p, "p", 1);
	wc_write_all(p, &lsn, sizeof(lsn));
	wc_write_all(p, &len, sizeof(len));
	if (len > 0)
		wc_write_all(p, page, len);
}

void
walredo_apply(WalRedoProc *p, XLogRecPtr start_lsn, XLogRecPtr end_lsn,
			  const char *record, uint32 len)
{
	uint64		s = (uint64) start_lsn;
	uint64		e = (uint64) end_lsn;

	wc_write_all(p, "a", 1);
	wc_write_all(p, &s, sizeof(s));
	wc_write_all(p, &e, sizeof(e));
	wc_write_all(p, &len, sizeof(len));
	wc_write_all(p, record, len);
}

void
walredo_get(WalRedoProc *p, char *page_out)
{
	wc_write_all(p, "g", 1);
	wc_read_all(p, page_out, BLCKSZ);
}

void
walredo_stop(WalRedoProc *p)
{
	int			status;

	/* EOF on stdin tells the helper to shut down cleanly */
	if (p->wfd >= 0)
	{
		close(p->wfd);
		p->wfd = -1;
	}
	if (p->rfd >= 0)
	{
		close(p->rfd);
		p->rfd = -1;
	}
	if (p->pid > 0)
	{
		while (waitpid(p->pid, &status, 0) < 0 && errno == EINTR)
			;
		p->pid = 0;
	}
	if (p->lock_fd >= 0)
	{
		close(p->lock_fd);		/* releases the cluster-wide flock */
		p->lock_fd = -1;
	}
	pfree(p);
}
