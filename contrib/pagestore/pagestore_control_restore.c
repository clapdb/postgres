/*-------------------------------------------------------------------------
 *
 * pagestore_control_restore.c
 *	  Restore $PGDATA/global/pg_control from the page store, before postgres
 *	  starts.
 *
 * The postmaster reads (and CRC-checks) pg_control very early -- before
 * shared_preload_libraries are loaded -- so the pagestore module's in-process
 * read hook cannot serve that read.  A compute bootstrapping cluster metadata
 * from the shared store therefore fetches pg_control with this freestanding
 * tool first, then starts postgres normally.  Symmetric to pagestore_walrestore
 * (which restores shipped WAL) and pagestore_import (which loads relations); it
 * speaks the same shared-memory protocol to the daemon and links no PostgreSQL
 * libraries.
 *
 *	  Usage: pagestore_control_restore --shm <name> --pgdata <dir> [--page-size N]
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_ipc.h"

/* Fixed on-disk size of pg_control (independent of BLCKSZ). */
#define PG_CONTROL_FILE_SIZE	8192

static uint32_t page_size = PS_DEFAULT_PAGE_SIZE;
static void *shm;
static int	chan;

static void
client_attach(const char *shm_name)
{
	int			fd = shm_open(shm_name, O_RDWR, 0600);
	PsShmHeader *hdr;

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
		hdr->page_size != page_size)
	{
		fprintf(stderr, "shm header mismatch (daemon magic=0x%x version=%u "
				"page_size=%u; expected version=%u page_size=%u)\n",
				hdr->magic, hdr->version, hdr->page_size,
				PS_SHM_VERSION, page_size);
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

static void
cl_exec(PsChannel *ch)
{
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;
	if (ch->status != PS_STATUS_OK)
	{
		fprintf(stderr, "daemon error on op %u\n", ch->opcode);
		exit(1);
	}
}

/* Timeline pg_control was written to (the writer's localsvc_timeline; 0 = main,
 * >0 for a branch compute).  Set with --timeline. */
static uint32_t restore_timeline = 0;

/* The single control object: klass=CONTROL, block 0, on the chosen timeline. */
static void
fill_control_key(PsChannel *ch)
{
	ch->timeline = restore_timeline;
	ch->key.spcOid = 0;
	ch->key.dbOid = 0;
	ch->key.relNumber = 0;
	ch->key.forkNum = 0;
	ch->key.klass = PS_KLASS_CONTROL;
}

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	const char *pgdata = NULL;
	PsChannel  *ch;
	char		dirpath[1024];
	char		path[1024];
	char		tmppath[1024];
	char		buf[PG_CONTROL_FILE_SIZE];
	size_t		n;
	int			fd;
	int			dirfd;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--pgdata") == 0 && i + 1 < argc)
			pgdata = argv[++i];
		else if (strcmp(argv[i], "--page-size") == 0 && i + 1 < argc)
			page_size = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--timeline") == 0 && i + 1 < argc)
			restore_timeline = (uint32_t) strtoul(argv[++i], NULL, 10);
		else
		{
			fprintf(stderr, "usage: %s --shm <name> --pgdata <dir> "
					"[--page-size N] [--timeline N]\n", argv[0]);
			return 2;
		}
	}
	if (shm_name == NULL || pgdata == NULL)
	{
		fprintf(stderr, "usage: %s --shm <name> --pgdata <dir> "
				"[--page-size N] [--timeline N]\n", argv[0]);
		return 2;
	}

	client_attach(shm_name);
	ch = ps_channel(shm, chan);

	/* is pg_control present on the store? */
	fill_control_key(ch);
	ch->opcode = PS_OP_NBLOCKS;
	cl_exec(ch);
	if (ch->result == 0)
	{
		fprintf(stderr, "pg_control is not present on the store\n");
		ps_store_release(&ps_channel(shm, chan)->claimed, 0);
		return 1;
	}

	/* read control page 0, requiring a real stored version */
	fill_control_key(ch);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = 0;
	ch->req_lsn = UINT64_MAX;
	cl_exec(ch);
	if (ch->result == 0)
	{
		fprintf(stderr, "pg_control block 0 is not resolvable on timeline %u\n",
				restore_timeline);
		ps_store_release(&ps_channel(shm, chan)->claimed, 0);
		return 1;
	}

	/* assemble the fixed-size control file (page + zero pad) and write it */
	memset(buf, 0, sizeof(buf));
	n = page_size < PG_CONTROL_FILE_SIZE ? page_size : PG_CONTROL_FILE_SIZE;
	memcpy(buf, ch->data, n);

	/* daemon work is done; release the channel for reuse before file I/O */
	ps_store_release(&ps_channel(shm, chan)->claimed, 0);

	snprintf(dirpath, sizeof(dirpath), "%s/global", pgdata);
	snprintf(path, sizeof(path), "%s/pg_control", dirpath);
	snprintf(tmppath, sizeof(tmppath), "%s/pg_control.tmp.%ld",
			 dirpath, (long) getpid());

	if (access(path, F_OK) == 0)
	{
		fprintf(stderr, "%s already exists; refusing to overwrite\n", path);
		return 1;
	}
	if (errno != ENOENT)
	{
		perror("stat pg_control");
		return 1;
	}

	fd = open(tmppath, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
	{
		perror("open temporary pg_control");
		return 1;
	}
	if (write(fd, buf, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE)
	{
		perror("write temporary pg_control");
		close(fd);
		unlink(tmppath);
		return 1;
	}
	if (fsync(fd) != 0 || close(fd) != 0)
	{
		perror("fsync/close temporary pg_control");
		unlink(tmppath);
		return 1;
	}
	if (rename(tmppath, path) != 0)
	{
		perror("rename pg_control");
		unlink(tmppath);
		return 1;
	}
	dirfd = open(dirpath, O_RDONLY | O_DIRECTORY);
	if (dirfd < 0)
	{
		perror("open global directory");
		return 1;
	}
	if (fsync(dirfd) != 0 || close(dirfd) != 0)
	{
		perror("fsync/close global directory");
		return 1;
	}
	fprintf(stderr, "restored %s from the store\n", path);
	return 0;
}
