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
 * Freestanding: only pagestore_ipc.h and libc.
 *
 * Usage (as restore_command):
 *   pagestore_walrestore --shm NAME --timeline N --segsize BYTES %f %p
 *
 * src/../contrib/pagestore/pagestore_walrestore.c
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "pagestore_ipc.h"

static void *shm;
static int	chan;

static void
client_attach(const char *shm_name, uint32_t page_size_unused)
{
	int			fd = shm_open(shm_name, O_RDWR, 0600);
	PsShmHeader *hdr;

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
	if (hdr->magic != PS_SHM_MAGIC)
	{
		fprintf(stderr, "bad shm header\n");
		exit(2);
	}
	for (uint32_t i = 0; i < hdr->nchannels; i++)
		if (ps_cas(&ps_channel(shm, i)->claimed, 0, 1))
		{
			chan = (int) i;
			return;
		}
	fprintf(stderr, "no free channel\n");
	exit(2);
}

/* Read up to len WAL bytes from start_lsn on a timeline; returns bytes read. */
static uint32_t
wal_read(uint32_t tl, uint64_t start_lsn, uint32_t len, void *out)
{
	PsChannel  *ch = ps_channel(shm, chan);

	ch->timeline = tl;
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;
	memcpy(out, ch->data, len);
	return ch->result;
}

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	uint32_t	timeline = 0;
	uint64_t	segsize = 16 * 1024 * 1024;
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
		else if (strcmp(argv[i], "--segsize") == 0 && i + 1 < argc)
			segsize = strtoull(argv[++i], NULL, 10);
		else if (!segname)
			segname = argv[i];
		else if (!outpath)
			outpath = argv[i];
	}
	if (!shm_name || !segname || !outpath)
	{
		fprintf(stderr, "usage: %s --shm NAME [--timeline N] [--segsize B] <segfile> <outpath>\n",
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
		uint32_t	got = wal_read(timeline, start_lsn + off, want, buf);

		if (got == 0)
			break;				/* not in the store: segment unavailable */
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
