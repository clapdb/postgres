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
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "miscadmin.h"
#include "walredo_client.h"

struct WalRedoProc
{
	pid_t		pid;
	int			wfd;			/* write end -> helper stdin */
	int			rfd;			/* read end  <- helper stdout */
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

	p->pid = 0;
	p->wfd = p->rfd = -1;

	if (pipe(in) != 0 || pipe(out) != 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create pipe for wal-redo helper: %m")));

	p->pid = fork();
	if (p->pid < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not fork wal-redo helper: %m")));

	if (p->pid == 0)
	{
		/* child: wire pipes to stdin/stdout and exec the helper */
		dup2(in[0], STDIN_FILENO);
		dup2(out[1], STDOUT_FILENO);
		close(in[0]);
		close(in[1]);
		close(out[0]);
		close(out[1]);
		/* argv[0] must be the full path so the helper can locate its own
		 * executable (InitStandaloneProcess -> find_my_exec) */
		execl(my_exec_path, my_exec_path, "--wal-redo", "-D", datadir, (char *) NULL);
		_exit(127);				/* exec failed */
	}

	/* parent: keep our ends */
	close(in[0]);
	close(out[1]);
	p->wfd = in[1];
	p->rfd = out[0];
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
	pfree(p);
}
