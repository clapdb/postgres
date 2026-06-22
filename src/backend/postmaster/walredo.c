/*-------------------------------------------------------------------------
 *
 * walredo.c
 *	  single-page WAL-redo helper process (postgres --wal-redo)
 *
 * Holds exactly one target page and applies WAL records to it on request, so a
 * page store can materialize a page "as of" an LSN from a base image plus the
 * delta records after it, without replaying all of WAL.  See
 * contrib/pagestore/WAL_REDO.md (the 3c-4 design).
 *
 * Protocol (length-prefixed binary, on stdin; replies on stdout), one byte tag
 * then a fixed payload:
 *
 *   'b' BEGIN     spcOid,dbOid,relNumber,forkNum,blockNum (5x uint32)
 *                 -- set the target page identity, zero the held page
 *   'p' PUSHBASE  base_end_lsn (uint64), len (uint32), then len page bytes
 *                 -- seed the held page (len == BLCKSZ) or zero it (len == 0)
 *                    and set its page LSN baseline to base_end_lsn
 *   'a' APPLY     start_lsn (uint64), end_lsn (uint64), len (uint32), record
 *                 -- rm_redo the record onto the held page (see below)
 *   'g' GET       (no payload) -- reply with BLCKSZ bytes: the held page
 *
 * EOF on stdin is a clean shutdown.
 *
 * This is increment 4a: the process entry, the held-page state and the protocol
 * framing.  The APPLY step -- running the record's resource-manager redo against
 * the held page with the buffer manager redirected to it (and VM/FSM side
 * effects stubbed per target fork) -- is the next increment (4b); until then
 * APPLY reports that it is unimplemented.  All I/O is raw read()/write() over
 * static buffers, so the mode runs without the usual backend initialisation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/postmaster/walredo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <unistd.h>
#ifdef WIN32
#include <fcntl.h>
#include <io.h>
#endif

#include "postmaster/walredo.h"
#include "storage/bufpage.h"

/* protocol message tags */
#define WALREDO_BEGIN		'b'
#define WALREDO_PUSHBASE	'p'
#define WALREDO_APPLY		'a'
#define WALREDO_GET			'g'

/*
 * The single page this process holds.  Stored in PGAlignedBlock so it can be
 * accessed as a Page (PageIsNew/PageSetLSN) without an unaligned access on
 * strict-alignment platforms.  4b adds the target identity (RelFileLocator/
 * fork/block); 4a only needs the page bytes.
 */
static PGAlignedBlock held_page;

static bool
read_fully(void *buf, size_t len)
{
	char	   *p = (char *) buf;

	while (len > 0)
	{
		ssize_t		n = read(STDIN_FILENO, p, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		if (n == 0)
			return false;		/* EOF */
		p += n;
		len -= (size_t) n;
	}
	return true;
}

static void
write_fully(const void *buf, size_t len)
{
	const char *p = (const char *) buf;

	while (len > 0)
	{
		ssize_t		n = write(STDOUT_FILENO, p, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			_exit(1);
		}
		p += n;
		len -= (size_t) n;
	}
}

static bool
read_u32(uint32 *v)
{
	return read_fully(v, sizeof(*v));
}

static bool
read_u64(uint64 *v)
{
	return read_fully(v, sizeof(*v));
}

/* discard 'len' bytes from stdin so the stream stays framed */
static bool
skip_bytes(uint32 len)
{
	char		buf[1024];

	while (len > 0)
	{
		uint32		chunk = len < sizeof(buf) ? len : (uint32) sizeof(buf);

		if (!read_fully(buf, chunk))
			return false;
		len -= chunk;
	}
	return true;
}

void
WalRedoMain(int argc, char *argv[])
{
	(void) argc;
	(void) argv;

#ifdef WIN32
	/* the protocol is binary; keep the CRT from translating \n/\r\n/0x1a on the
	 * inherited stdin/stdout, which would corrupt PUSHBASE/GET page bytes */
	_setmode(_fileno(stdin), _O_BINARY);
	_setmode(_fileno(stdout), _O_BINARY);
#endif

	for (;;)
	{
		unsigned char tag;

		if (!read_fully(&tag, 1))
			_exit(0);			/* EOF: clean shutdown */

		switch (tag)
		{
			case WALREDO_BEGIN:
				{
					uint32		ident[5];	/* spc,db,rel,fork,block */

					for (int i = 0; i < 5; i++)
						if (!read_u32(&ident[i]))
							_exit(1);
					(void) ident;	/* 4b stores the target identity from here */
					/* 4a only resets the held page */
					memset(held_page.data, 0, BLCKSZ);
					break;
				}

			case WALREDO_PUSHBASE:
				{
					uint64		base_end_lsn;
					uint32		len;

					if (!read_u64(&base_end_lsn) || !read_u32(&len))
						_exit(1);
					if (len == BLCKSZ)
					{
						if (!read_fully(held_page.data, BLCKSZ))
							_exit(1);

						/*
						 * RestoreBlockImage (on the driver side) copies only the
						 * image bytes; PostgreSQL stamps a restored page's LSN
						 * separately.  Do the same here so a base-only GET, and the
						 * BLK_DONE/BLK_NEEDS_REDO gating once 4b replays deltas, see
						 * the correct pd_lsn.  Leave an uninitialized (new) page
						 * alone, as redo does.
						 *
						 * This invalidates pd_checksum, but the helper deliberately
						 * does not recompute it: without backend initialisation it
						 * cannot know whether data checksums are enabled.  The caller
						 * persists/serves the page in a normal backend (with cluster
						 * context and the block number it issued in BEGIN), and
						 * recomputes pd_checksum there before storing or serving.
						 */
						if (!PageIsNew((Page) held_page.data))
							PageSetLSN((Page) held_page.data, (XLogRecPtr) base_end_lsn);
					}
					else if (len == 0)
						memset(held_page.data, 0, BLCKSZ);
					else
						_exit(1);	/* malformed base length */
					break;
				}

			case WALREDO_APPLY:
				{
					uint64		start_lsn,
								end_lsn;
					uint32		len;
					const char *msg = "wal-redo: APPLY not implemented yet (4b)\n";

					if (!read_u64(&start_lsn) || !read_u64(&end_lsn) ||
						!read_u32(&len))
						_exit(1);
					if (!skip_bytes(len))
						_exit(1);
					(void) start_lsn;	/* used in 4b */
					(void) end_lsn;

					/*
					 * 4b: decode the record and run its rm_redo against held_page,
					 * with XLogReadBufferExtended redirected to the held (target)
					 * and scratch (other) buffers, and VM/FSM side effects stubbed
					 * per the requested fork.  Not yet implemented.
					 */
					write(STDERR_FILENO, msg, strlen(msg));
					_exit(2);
				}

			case WALREDO_GET:
				write_fully(held_page.data, BLCKSZ);
				break;

			default:
				_exit(1);		/* protocol error */
		}
	}
}
