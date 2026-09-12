/*-------------------------------------------------------------------------
 *
 * pagestore_walrestore.c
 *	  Reconstruct a standard WAL segment file from the WAL a timeline shipped to
 *	  the pagestore daemon.  Usable directly as a PostgreSQL restore_command, so
 *	  a recovery instance can replay WAL that lives in the store (the bridge from
 *	  shipped WAL to redo).
 *
 * Given a segment file name (e.g. 000000010000000000000005) it computes the
 * segment's start LSN, reads wal_segment_size bytes from the store via
 * PS_OP_WAL_READ (in io_unit chunks), and writes them to the output path.
 * Exit 0 if the whole segment was available, non-zero otherwise (which tells
 * recovery there is no more WAL) -- standard restore_command semantics.
 *
 * The bytes are handed to PostgreSQL untouched, so the one thing this tool
 * checks about them is their PostgreSQL identity: the segment begins with a
 * long WAL page header whose xlp_seg_size must be the --segsize the LSN was
 * computed from (a cluster initialized with another --wal-segsize names a
 * different LSN range by the same file name), and whose xlp_magic must be
 * the XLOG_PAGE_MAGIC of the recovering build when --xlog-magic names it
 * (pagestore_control_restore --payload-identity prints that build's
 * value).  A mismatch is fatal to recovery: PostgreSQL treats a
 * restore_command exit status above 125 (like a signal) as a hard error
 * that aborts recovery, while any other nonzero status only means "no such
 * archive file" -- which for a foreign-format segment would end recovery
 * quietly and start the database.  So the mismatch exits with
 * PS_WALRESTORE_EXIT_FATAL and names the payload identity on stderr.
 *
 * Freestanding: only pagestore_ipc.h and libc.
 *
 * Usage (as restore_command):
 *   pagestore_walrestore --shm NAME --timeline N --incarnation N \
 *       --segsize BYTES [--xlog-magic 0xD120] %f %p
 *
 * src/../contrib/pagestore/pagestore_walrestore.c
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "pagestore_ipc.h"

static void *shm;
static volatile sig_atomic_t chan = -1;
static volatile sig_atomic_t request_in_flight;

static void
restore_signal_exit(int signo)
{
	if (shm != NULL && chan >= 0)
		ps_store_release(&ps_channel(shm, chan)->claimed,
						 request_in_flight ? 2 : 0);
	_exit(128 + signo);
}

static void
release_channel(void)
{
	sigset_t	blockset,
				oldset;

	sigemptyset(&blockset);
	sigaddset(&blockset, SIGTERM);
	sigaddset(&blockset, SIGINT);
	sigaddset(&blockset, SIGQUIT);
	sigprocmask(SIG_BLOCK, &blockset, &oldset);
	if (shm != NULL && chan >= 0)
	{
		int			release_chan = (int) chan;

		/* Disarm the signal handler before making the mailbox claimable. */
		chan = -1;
		ps_store_release(&ps_channel(shm, release_chan)->claimed,
						 request_in_flight ? 2 : 0);
	}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
}

/* A restore_command also receives .history and .backup archive objects. */
static int
is_wal_segment_name(const char *name)
{
	size_t		len = strlen(name);

	if (len != 24)
		return 0;
	for (size_t i = 0; i < len; i++)
		if (!((name[i] >= '0' && name[i] <= '9') ||
			  (name[i] >= 'A' && name[i] <= 'F') ||
			  (name[i] >= 'a' && name[i] <= 'f')))
			return 0;
	return 1;
}

static void
client_attach(const char *shm_name, uint32_t page_size_unused)
{
	int			fd = shm_open(shm_name, O_RDWR, 0600);
	PsShmHeader *hdr;
	sigset_t	claimset,
				oldset;

	(void) page_size_unused;
	if (fd < 0)
	{
		perror("shm_open (is the daemon running?)");
		exit(2);
	}
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (shm == MAP_FAILED)
	{
		perror("mmap");
		exit(2);
	}
	close(fd);
	hdr = (PsShmHeader *) shm;
	if (hdr->magic != PS_SHM_MAGIC || hdr->version != PS_SHM_VERSION ||
		__atomic_load_n(&hdr->startup_state, __ATOMIC_ACQUIRE) != PS_SHM_READY)
	{
		fprintf(stderr, "bad shm header (magic/version mismatch; daemon and "
				"walrestore built against different PS_SHM_VERSION?)\n");
		exit(2);
	}

	/*
	 * Arm cleanup before claiming, then block terminating signals across the
	 * CAS-to-chan assignment gap.  A signal during an active request marks the
	 * mailbox abandoned so it cannot be reused while the daemon may write it.
	 */
	atexit(release_channel);
	signal(SIGTERM, restore_signal_exit);
	signal(SIGINT, restore_signal_exit);
	signal(SIGQUIT, restore_signal_exit);
	sigemptyset(&claimset);
	sigaddset(&claimset, SIGTERM);
	sigaddset(&claimset, SIGINT);
	sigaddset(&claimset, SIGQUIT);
	sigprocmask(SIG_BLOCK, &claimset, &oldset);

	for (uint32_t i = 0; i < hdr->nchannels; i++)
		if (ps_cas(&ps_channel(shm, i)->claimed, 0, 1))
		{
			chan = (int) i;
			sigprocmask(SIG_SETMASK, &oldset, NULL);
			return;
		}

	/* Reuse abandoned mailboxes only after no daemon write can still arrive. */
	for (uint32_t i = 0; i < hdr->nchannels; i++)
		if (ps_cas(&ps_channel(shm, i)->claimed, 2, 1))
		{
			uint32_t	state = ps_load_acquire(&ps_channel(shm, i)->state);

			if (state == PS_STATE_DONE || state == PS_STATE_IDLE)
			{
				chan = (int) i;
				sigprocmask(SIG_SETMASK, &oldset, NULL);
				return;
			}
			ps_store_release(&ps_channel(shm, i)->claimed, 2);
		}
	sigprocmask(SIG_SETMASK, &oldset, NULL);
	fprintf(stderr, "no free channel\n");
	exit(2);
}

/* Read up to len WAL bytes from start_lsn on a timeline; returns bytes read. */
static uint32_t
wal_read(uint32_t tl, uint64_t incarnation, uint64_t start_lsn,
			 uint32_t len, void *out)
{
	PsChannel  *ch = ps_channel(shm, chan);

	ch->timeline = tl;
	ch->incarnation = incarnation;
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	request_in_flight = 1;
	ps_request_generation_next(ch);
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;
	request_in_flight = 0;
	memcpy(out, ch->data, len);
	return ch->result;
}

/* an exit status PostgreSQL's RestoreArchivedFile() treats as fatal */
#define PS_WALRESTORE_EXIT_FATAL 126

/*
 * The segment's first page header, in the byte order PostgreSQL wrote it
 * (the store does not move WAL between byte orders).  XLogPageHeaderData:
 * xlp_magic u16, xlp_info u16, xlp_tli u32, xlp_pageaddr u64, xlp_rem_len
 * u32, padding; XLogLongPageHeaderData adds xlp_sysid u64 at 24,
 * xlp_seg_size u32 at 32, xlp_xlog_blcksz u32 at 36.
 */
#define XLP_LONG_HEADER_FLAG	0x0002
#define XLP_LONG_HEADER_BYTES	40

static int
check_payload_identity(const unsigned char *page, uint32_t len,
					   uint64_t segsize, int have_xlog_magic,
					   unsigned xlog_magic)
{
	uint16_t	magic;
	uint16_t	info;
	uint32_t	seg_size;

	if (len < XLP_LONG_HEADER_BYTES)
	{
		fprintf(stderr, "segment start is %u bytes, shorter than a WAL page header\n",
				len);
		return PS_WALRESTORE_EXIT_FATAL;
	}
	memcpy(&magic, page, sizeof(magic));
	memcpy(&info, page + 2, sizeof(info));
	if (have_xlog_magic && magic != xlog_magic)
	{
		fprintf(stderr, "payload needs a PostgreSQL build with XLOG_PAGE_MAGIC 0x%04x; "
				"this build expects 0x%04x\n", magic, xlog_magic);
		return PS_WALRESTORE_EXIT_FATAL;
	}
	if ((info & XLP_LONG_HEADER_FLAG) == 0)
	{
		fprintf(stderr, "segment start carries no long WAL page header "
				"(xlp_info 0x%04x); not a PostgreSQL WAL segment boundary\n", info);
		return PS_WALRESTORE_EXIT_FATAL;
	}
	memcpy(&seg_size, page + 32, sizeof(seg_size));
	if (seg_size != segsize)
	{
		fprintf(stderr, "payload was written by a cluster with a %u-byte WAL "
				"segment size; --segsize %llu names a different LSN range\n",
				seg_size, (unsigned long long) segsize);
		return PS_WALRESTORE_EXIT_FATAL;
	}
	return 0;
}

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	uint32_t	timeline = 0;
	uint64_t	incarnation = 0;
	int			have_incarnation = 0;
	uint64_t	segsize = 16 * 1024 * 1024;
	unsigned long xlog_magic = 0;
	int			have_xlog_magic = 0;
	const char *segname = NULL;
	const char *outpath = NULL;
	uint32_t	tli,
				hi,
				lo;
	uint64_t	segs_per_id,
				segno,
				start_lsn,
				off = 0;
	int			outfd;
	unsigned char *buf;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--timeline") == 0 && i + 1 < argc)
			timeline = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--incarnation") == 0 && i + 1 < argc)
		{
			char	   *end;
			unsigned long long value;

			if (argv[i + 1][0] < '0' || argv[i + 1][0] > '9')
			{
				fprintf(stderr, "invalid --incarnation \"%s\"\n", argv[i + 1]);
				return 2;
			}
			errno = 0;
			value = strtoull(argv[++i], &end, 10);
			if (errno != 0 || *end != '\0' || value == 0)
			{
				fprintf(stderr, "invalid --incarnation \"%s\"\n", argv[i]);
				return 2;
			}
			incarnation = (uint64_t) value;
			have_incarnation = 1;
		}
		else if (strcmp(argv[i], "--segsize") == 0 && i + 1 < argc)
			segsize = strtoull(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--xlog-magic") == 0 && i + 1 < argc)
		{
			char	   *end;

			errno = 0;
			xlog_magic = strtoul(argv[++i], &end, 0);
			if (errno != 0 || *end != '\0' || xlog_magic == 0 || xlog_magic > 0xffff)
			{
				fprintf(stderr, "invalid --xlog-magic \"%s\"\n", argv[i]);
				return 2;
			}
			have_xlog_magic = 1;
		}
		else if (!segname)
			segname = argv[i];
		else if (!outpath)
			outpath = argv[i];
	}
	if (!shm_name || !segname || !outpath || !have_incarnation)
	{
		fprintf(stderr, "usage: %s --shm NAME [--timeline N] --incarnation N [--segsize B] [--xlog-magic M] <segfile> <outpath>\n",
				argv[0]);
		return 2;
	}

	/*
	 * The WAL segment size must be a power of two in [1 MB, 1 GB] (PostgreSQL's
	 * own constraint).  Reject anything else before dividing by it: a zero or
	 * bogus --segsize would divide-by-zero / miscompute the LSN and break
	 * recovery.
	 */
	if (segsize < 1024 * 1024 || segsize > 1024 * 1024 * 1024 ||
		(segsize & (segsize - 1)) != 0)
	{
		fprintf(stderr, "invalid --segsize %llu (must be a power of two, "
				"1MB..1GB)\n", (unsigned long long) segsize);
		return 2;
	}

	/*
	 * PostgreSQL invokes restore_command for history and backup files as well
	 * as WAL segments.  The archive module intentionally does not store those
	 * auxiliary files, so report them unavailable (exit 1), rather than a hard
	 * command error (exit 2) that would abort recovery.
	 */
	if (!is_wal_segment_name(segname))
		return 1;

	/* segment file name is TLI(8 hex) + xlogid(8 hex) + segment-in-id(8 hex) */
	if (sscanf(segname, "%8X%8X%8X", &tli, &hi, &lo) != 3)
	{
		fprintf(stderr, "bad segment name \"%s\"\n", segname);
		return 2;
	}
	segs_per_id = 0x100000000ULL / segsize;
	segno = (uint64_t) hi * segs_per_id + lo;
	start_lsn = segno * segsize;

	client_attach(shm_name, 0);

	buf = malloc(PS_IO_UNIT);
	if (buf == NULL)
	{
		fprintf(stderr, "out of memory\n");
		return 2;
	}
	outfd = open(outpath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (outfd < 0)
	{
		perror("open outpath");
		return 2;
	}

	while (off < segsize)
	{
		uint32_t	want = (uint32_t) ((segsize - off) < PS_IO_UNIT ?
									   (segsize - off) : PS_IO_UNIT);
		uint32_t	got = wal_read(timeline, incarnation, start_lsn + off,
							 want, buf);

		if (got == 0)
			break;				/* not in the store: segment unavailable */
		if (off == 0)
		{
			int			rc = check_payload_identity(buf, got, segsize,
													have_xlog_magic,
													(unsigned) xlog_magic);

			if (rc != 0)
			{
				close(outfd);
				unlink(outpath);
				return rc;
			}
		}
		if (write(outfd, buf, got) != (ssize_t) got)
		{
			perror("write");
			close(outfd);
			return 2;
		}
		off += got;
		if (got < want)
			break;
	}
	close(outfd);
	free(buf);

	if (off < segsize)
	{
		/* segment not (fully) available -> tell recovery there is no more WAL */
		unlink(outpath);
		return 1;
	}
	return 0;
}
