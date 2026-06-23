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
 * Startup brings up a standalone backend (shared memory + a PGPROC + the buffer
 * manager) by reusing the single-user init path (InitStandaloneBackend), so it
 * needs a data directory: `postgres --wal-redo -D <datadir>`.  This must be a
 * private, throwaway cluster, not the live one -- the helper only ever mutates
 * pages handed to it over the protocol, so any cluster of the same BLCKSZ works,
 * and using the live datadir would collide with the postmaster's lock file.  The
 * full backend context is required because running a record's rm_redo goes
 * through the normal buffer manager.
 *
 * APPLY currently decodes and validates the record (header length, rmid, CRC) but
 * does not yet mutate the held page: running the record's resource-manager redo
 * against it with the buffer manager redirected to the held/scratch buffers (and
 * VM/FSM side effects stubbed per target fork) is the next increment.  Protocol
 * I/O is raw read()/write() over static buffers.
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
#include <signal.h>
#include <unistd.h>
#ifdef WIN32
#include <fcntl.h>
#include <io.h>
#endif

#include "access/rmgr.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "catalog/pg_class.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "postmaster/walredo.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/resowner.h"

/* XLogReader reused across APPLY messages (lazily allocated) */
static XLogReaderState *wr_reader = NULL;

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

/*
 * Set by the termination-signal handler.  The handler does nothing but set this
 * flag (async-signal-safe); the protocol read loop notices it when the blocking
 * read returns EINTR and shuts the helper down cleanly via proc_exit().
 */
static volatile sig_atomic_t walredo_shutdown_requested = 0;

static void
walredo_term_handler(int signo)
{
	(void) signo;
	walredo_shutdown_requested = 1;
}

static bool
read_fully(void *buf, size_t len)
{
	char	   *p = (char *) buf;

	while (len > 0)
	{
		ssize_t		n;

		/*
		 * Honor a termination signal that arrived between reads: at that point no
		 * EINTR is pending to interrupt the upcoming blocking read, so check the
		 * flag here as well as in the EINTR branch below.
		 */
		if (walredo_shutdown_requested)
			proc_exit(1);

		n = read(STDIN_FILENO, p, len);

		if (n < 0)
		{
			if (errno == EINTR)
			{
				/* a termination signal delivered while we block here interrupts
				 * the read; honor it instead of silently retrying */
				if (walredo_shutdown_requested)
					proc_exit(1);
				continue;
			}
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
		ssize_t		n;

		/* honor a termination signal that arrived between writes (e.g. while the
		 * controller has stopped draining stdout), as read_fully() does */
		if (walredo_shutdown_requested)
			proc_exit(1);

		n = write(STDOUT_FILENO, p, len);

		if (n < 0)
		{
			if (errno == EINTR)
			{
				/* a termination signal interrupted the write; act on it rather
				 * than retrying into a full pipe forever */
				if (walredo_shutdown_requested)
					proc_exit(1);
				continue;
			}
			proc_exit(1);
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

/* ---- 4b: buffer redirect so rm_redo mutates the held page ----------------- */

bool		am_walredo = false;

/* the BEGIN target page identity */
static RelFileLocator wr_target_rloc;
static ForkNumber wr_target_fork;
static BlockNumber wr_target_blk;
static bool wr_have_target = false;

/*
 * Real (backend-local) buffers backing the redirect: wr_target_buf holds the
 * page being materialized; wr_scratch_buf absorbs any other block a record
 * references (we only ever read wr_target_buf back, so scratch redo is
 * discarded).  Both are blocks of a private temp relation, so nothing is written
 * to the cluster.
 */
static Buffer wr_target_buf = InvalidBuffer;
static Buffer wr_scratch_buf = InvalidBuffer;

/* release the long-lived buffer pins at process exit, before the buffer manager's
 * own leak check (registered with before_shmem_exit so it runs first) */
static void
wr_release_buffers(int code, Datum arg)
{
	(void) code;
	(void) arg;
	if (wr_target_buf != InvalidBuffer)
	{
		ReleaseBuffer(wr_target_buf);
		wr_target_buf = InvalidBuffer;
	}
	if (wr_scratch_buf != InvalidBuffer)
	{
		ReleaseBuffer(wr_scratch_buf);
		wr_scratch_buf = InvalidBuffer;
	}
}

static void
wr_ensure_buffers(void)
{
	RelFileLocator tmp;
	SMgrRelation reln;

	if (wr_target_buf != InvalidBuffer)
		return;

	tmp.spcOid = 1663;			/* pg_default */
	tmp.dbOid = 1;				/* template1 (its base dir always exists) */
	tmp.relNumber = 0x7e000001; /* arbitrary, private to this backend */
	reln = smgropen(tmp, MyProcNumber);
	smgrcreate(reln, MAIN_FORKNUM, false);
	wr_target_buf = ExtendBufferedRel(BMR_SMGR(reln, RELPERSISTENCE_TEMP),
									  MAIN_FORKNUM, NULL, EB_SKIP_EXTENSION_LOCK);
	wr_scratch_buf = ExtendBufferedRel(BMR_SMGR(reln, RELPERSISTENCE_TEMP),
									   MAIN_FORKNUM, NULL, EB_SKIP_EXTENSION_LOCK);
	before_shmem_exit(wr_release_buffers, (Datum) 0);
}

Buffer
WalRedoReadBuffer(RelFileLocator rlocator, ForkNumber forknum,
				  BlockNumber blkno, ReadBufferMode mode)
{
	Buffer		b;

	if (wr_have_target &&
		RelFileLocatorEquals(rlocator, wr_target_rloc) &&
		forknum == wr_target_fork && blkno == wr_target_blk)
		b = wr_target_buf;
	else
	{
		b = wr_scratch_buf;
		if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK ||
			mode == RBM_ZERO_ON_ERROR)
			memset(BufferGetPage(b), 0, BLCKSZ);
	}

	/*
	 * The redo caller will UnlockReleaseBuffer() this, dropping one pin; add that
	 * reference so our long-lived buffer survives.  (LockBuffer is a no-op on
	 * these local buffers.)
	 */
	IncrBufferRefCount(b);
	return b;
}

void
WalRedoMain(int argc, char *argv[])
{
#ifdef WIN32
	/* the protocol is binary; keep the CRT from translating \n/\r\n/0x1a on the
	 * inherited stdin/stdout, which would corrupt PUSHBASE/GET page bytes */
	_setmode(_fileno(stdin), _O_BINARY);
	_setmode(_fileno(stdout), _O_BINARY);
#endif

	/*
	 * Drop the leading "--wal-redo" dispatch token so the shared standalone
	 * bring-up sees a normal argv (process_postgres_switches only special-cases
	 * "--single").  argv[0] is preserved as the program name.
	 */
	if (argc > 1)
	{
		argv[1] = argv[0];
		argv++;
		argc--;
	}

	/*
	 * Bring up a standalone backend: switches (incl. -D), config, control file,
	 * shared memory and a PGPROC -- everything the buffer manager needs.  This
	 * runs against a private, throwaway data directory, never the live cluster's
	 * (whose lock file is held by the postmaster): the helper only ever mutates
	 * pages handed to it over the protocol, so a generic scratch cluster of the
	 * same BLCKSZ is sufficient for redo.
	 *
	 * Pass a NULL dbname: this mode opens no database, and a non-NULL pointer
	 * would let process_postgres_switches() consume a stray positional argument as
	 * a database name (e.g. "--wal-redo scratch" silently falling back to PGDATA
	 * instead of the intended -D directory).
	 */
	InitStandaloneBackend(argc, argv, NULL, NULL, false, true);

	/*
	 * BaseInit() sets up smgr and the buffer manager (InitBufferManagerAccess),
	 * which need the shared memory + MyProc that InitStandaloneBackend created.
	 */
	BaseInit();
	SetProcessingMode(NormalProcessing);

	/*
	 * We skip InitPostgres (no database is opened), so adopt a regular backend
	 * type explicitly: the buffer manager's pgstat I/O accounting asserts that
	 * MyBackendType tracks the I/O ops our scratch buffers perform.
	 */
	MyBackendType = B_BACKEND;

	/* index rmgrs (btree/GIN/GiST/SP-GiST) allocate recovery contexts here */
	RmgrStartup();

	/* buffer pins are tracked against a resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "wal-redo");

	/*
	 * InitStandaloneProcess() leaves the signal mask blocked (BlockSig).  Install
	 * handlers and unblock so a long-lived/pooled helper can be stopped with
	 * SIGINT/SIGTERM rather than getting stuck until stdin closes; die() sets a
	 * pending interrupt that read_fully() acts on via CHECK_FOR_INTERRUPTS() when
	 * the blocking read returns EINTR, giving a clean proc_exit().
	 */
	{
		struct sigaction act;
		int			diesigs[] = {SIGINT, SIGTERM, SIGQUIT};

		/*
		 * Install the termination handler deliberately *without* SA_RESTART (so
		 * not via pqsignal): the helper blocks in read(), and we need that read to
		 * return EINTR on a termination signal so the loop can shut down.  With
		 * SA_RESTART the read would silently resume and the process would stay
		 * stuck until stdin closed.  The handler only sets a flag, so it is
		 * async-signal-safe; proc_exit() runs later from read_fully().
		 */
		memset(&act, 0, sizeof(act));
		act.sa_handler = walredo_term_handler;
		sigemptyset(&act.sa_mask);
		act.sa_flags = 0;
		for (int i = 0; i < (int) lengthof(diesigs); i++)
			sigaction(diesigs[i], &act, NULL);
	}
	pqsignal(SIGHUP, PG_SIG_IGN);
	pqsignal(SIGPIPE, PG_SIG_IGN);
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/* the control file supplies the cluster's real WAL segment size */
	if (wal_segment_size == 0)
		wal_segment_size = DEFAULT_XLOG_SEG_SIZE;

	for (;;)
	{
		unsigned char tag;

		if (!read_fully(&tag, 1))
		{
			/* EOF: clean shutdown -- match RmgrStartup() so the index rmgrs'
			 * recovery contexts are torn down */
			RmgrCleanup();
			proc_exit(0);
		}

		switch (tag)
		{
			case WALREDO_BEGIN:
				{
					uint32		ident[5];	/* spc,db,rel,fork,block */

					for (int i = 0; i < 5; i++)
						if (!read_u32(&ident[i]))
							proc_exit(1);
					wr_target_rloc.spcOid = ident[0];
					wr_target_rloc.dbOid = ident[1];
					wr_target_rloc.relNumber = ident[2];
					wr_target_fork = (ForkNumber) ident[3];
					wr_target_blk = (BlockNumber) ident[4];
					wr_have_target = true;
					memset(held_page.data, 0, BLCKSZ);
					break;
				}

			case WALREDO_PUSHBASE:
				{
					uint64		base_end_lsn;
					uint32		len;

					if (!read_u64(&base_end_lsn) || !read_u32(&len))
						proc_exit(1);
					if (len == BLCKSZ)
					{
						if (!read_fully(held_page.data, BLCKSZ))
							proc_exit(1);

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
						proc_exit(1);	/* malformed base length */
					break;
				}

			case WALREDO_APPLY:
				{
					uint64		start_lsn,
								end_lsn;
					uint32		len;
					char	   *recbuf;
					DecodedXLogRecord *decoded;
					char	   *errm = NULL;

					if (!read_u64(&start_lsn) || !read_u64(&end_lsn) ||
						!read_u32(&len))
						proc_exit(1);
					if (len < SizeOfXLogRecord)
						proc_exit(1);
					recbuf = palloc(len);
					if (!read_fully(recbuf, len))
						proc_exit(1);

					/*
					 * This path feeds the bytes straight to DecodeXLogRecord,
					 * which (unlike XLogReadRecord) does not validate the record,
					 * so replicate the checks XLogReadRecord would make before
					 * trusting the record:
					 *
					 *  - the header length must equal the framed length, or the
					 *    decoder could run off the end of recbuf;
					 *  - the resource manager id must be valid;
					 *  - the CRC must match, so a damaged chunk that still frames
					 *    correctly is rejected rather than (once rm_redo is wired)
					 *    applied as if it were good WAL.
					 */
					{
						XLogRecord *rec = (XLogRecord *) recbuf;
						pg_crc32c	crc;

						if (rec->xl_tot_len != len)
							proc_exit(1);
						if (!RmgrIdIsValid(rec->xl_rmid))
							proc_exit(1);

						INIT_CRC32C(crc);
						COMP_CRC32C(crc, recbuf + SizeOfXLogRecord,
									len - SizeOfXLogRecord);
						COMP_CRC32C(crc, rec, offsetof(XLogRecord, xl_crc));
						FIN_CRC32C(crc);
						if (!EQ_CRC32C(crc, rec->xl_crc))
							proc_exit(1);
					}

					if (wr_reader == NULL)
					{
						wr_reader = XLogReaderAllocate(wal_segment_size, NULL,
													   XL_ROUTINE(.page_read = NULL,
																  .segment_open = NULL,
																  .segment_close = NULL),
													   NULL);
						if (wr_reader == NULL)
							proc_exit(1);
					}

					decoded = palloc(DecodeXLogRecordRequiredSpace(
										 ((XLogRecord *) recbuf)->xl_tot_len));
					if (!DecodeXLogRecord(wr_reader, decoded, (XLogRecord *) recbuf,
										  (XLogRecPtr) end_lsn, &errm))
						proc_exit(1);

					/*
					 * Populate the reader so the XLogRecGet* accessors and (once
					 * wired) rm_redo see a consistent record, including the start
					 * and end LSNs used for page-LSN decisions.
					 */
					wr_reader->ReadRecPtr = (XLogRecPtr) start_lsn;
					wr_reader->EndRecPtr = (XLogRecPtr) end_lsn;
					wr_reader->record = decoded;

					/*
					 * Run the record's resource-manager redo against the held page.
					 * Stage the held page into the target buffer (its pd_lsn drives
					 * the BLK_DONE/BLK_NEEDS_REDO gating), zero the scratch buffer,
					 * then dispatch rm_redo with the buffer manager redirected
					 * (am_walredo) so XLogReadBufferExtended resolves to those
					 * buffers.  Read the materialized page back for GET.
					 */
					wr_ensure_buffers();
					memcpy(BufferGetPage(wr_target_buf), held_page.data, BLCKSZ);
					memset(BufferGetPage(wr_scratch_buf), 0, BLCKSZ);

					am_walredo = true;
					RmgrTable[XLogRecGetRmid(wr_reader)].rm_redo(wr_reader);
					am_walredo = false;

					memcpy(held_page.data, BufferGetPage(wr_target_buf), BLCKSZ);
					break;
				}

			case WALREDO_GET:
				write_fully(held_page.data, BLCKSZ);
				break;

			default:
				proc_exit(1);		/* protocol error */
		}
	}
}
