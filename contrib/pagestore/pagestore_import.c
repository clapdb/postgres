/*-------------------------------------------------------------------------
 *
 * pagestore_import.c
 *	  Import an existing PostgreSQL data directory's relation files into the
 *	  pagestore daemon (timeline 0).
 *
 * This bootstraps "whole-database on the page store" without needing the
 * library loaded during initdb: run initdb normally, start the daemon, then
 * import.  Afterwards PostgreSQL can run with pagestore.route_all = on and read
 * every relation (catalogs included) from the store.
 *
 * It walks the per-database directories under PGDATA/base and the shared
 * PGDATA/global directory, parses each relation file name into
 * (tablespace, database, relfilenode, fork, segment), and writes its
 * blocks to the daemon over the shared-memory protocol -- exactly the keys the
 * smgr shim will later use.  Freestanding: only pagestore_ipc.h and libc.
 *
 * Limitation: only the default tablespace (base/) and shared catalogs (global/)
 * are imported; relations in user-created tablespaces (pg_tblspc/<oid>/...) are
 * not, and would be missing under route_all.  The tool warns if any exist.  Use
 * the default tablespace, or extend the walk to resolve pg_tblspc links.
 *
 * Usage: pagestore_import --shm NAME --pgdata DIR [--page-size N]
 *
 * src/../contrib/pagestore/pagestore_import.c
 *
 *-------------------------------------------------------------------------
 */
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_ipc.h"

/* Physical tablespace OIDs (stable PostgreSQL constants). */
#define DEFAULTTABLESPACE_OID	1663
#define GLOBALTABLESPACE_OID	1664

/* Blocks per 1GB segment in a default build (RELSEG_SIZE for the page size). */
static uint32_t seg_blocks;
static uint32_t page_size = PS_DEFAULT_PAGE_SIZE;

/* shared-memory client state */
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

static void
put_create(uint32_t spc, uint32_t db, uint32_t rel, int32_t fork)
{
	PsChannel  *ch = ps_channel(shm, chan);

	ch->timeline = 0;
	ch->key.spcOid = spc;
	ch->key.dbOid = db;
	ch->key.relNumber = rel;
	ch->key.forkNum = fork;
	ch->key.klass = PS_KLASS_RELATION;
	ch->opcode = PS_OP_CREATE;
	cl_exec(ch);
}

static void
put_block(uint32_t spc, uint32_t db, uint32_t rel, int32_t fork,
		  uint32_t block, const void *page)
{
	PsChannel  *ch = ps_channel(shm, chan);

	ch->timeline = 0;
	ch->key.spcOid = spc;
	ch->key.dbOid = db;
	ch->key.relNumber = rel;
	ch->key.forkNum = fork;
	ch->key.klass = PS_KLASS_RELATION;
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, page_size);
	cl_exec(ch);
}

/*
 * Parse a relation file name into relfilenode, fork and segment.
 * Forms: "<n>", "<n>.<seg>", "<n>_fsm[.<seg>]", "<n>_vm[.<seg>]",
 * "<n>_init[.<seg>]".  Returns 0 on success, -1 if not a relation file.
 */
static int
parse_relfile(const char *name, uint32_t *rel, int32_t *fork, uint32_t *seg)
{
	char		buf[256];
	char	   *dot,
			   *us;

	if (!isdigit((unsigned char) name[0]))
		return -1;
	if (strlen(name) >= sizeof(buf))
		return -1;
	strcpy(buf, name);

	*seg = 0;
	dot = strchr(buf, '.');
	if (dot)
	{
		*seg = (uint32_t) strtoul(dot + 1, NULL, 10);
		*dot = '\0';
	}

	*fork = 0;					/* MAIN_FORKNUM */
	us = strchr(buf, '_');
	if (us)
	{
		if (strcmp(us, "_fsm") == 0)
			*fork = 1;			/* FSM_FORKNUM */
		else if (strcmp(us, "_vm") == 0)
			*fork = 2;			/* VISIBILITYMAP_FORKNUM */
		else if (strcmp(us, "_init") == 0)
			*fork = 3;			/* INIT_FORKNUM */
		else
			return -1;			/* unknown suffix */
		*us = '\0';
	}

	for (char *p = buf; *p; p++)
		if (!isdigit((unsigned char) *p))
			return -1;			/* not a pure relfilenode */
	*rel = (uint32_t) strtoul(buf, NULL, 10);
	return 0;
}

/* Import every relation file in one directory (one database, or global/). */
static void
import_dir(const char *dirpath, uint32_t spc, uint32_t db)
{
	DIR		   *d = opendir(dirpath);
	struct dirent *de;
	void	   *page = malloc(page_size);

	if (!d)
		return;

	while ((de = readdir(d)) != NULL)
	{
		char		path[8192];
		uint32_t	rel,
					seg;
		int32_t		fork;
		int			fd;
		ssize_t		n;
		uint32_t	local_blk = 0;

		if (parse_relfile(de->d_name, &rel, &fork, &seg) != 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", dirpath, de->d_name);
		fd = open(path, O_RDONLY);
		if (fd < 0)
			continue;

		put_create(spc, db, rel, fork);

		while ((n = read(fd, page, page_size)) != 0)
		{
			uint32_t	block;

			if (n < 0)			/* I/O error: abort rather than import partially */
			{
				fprintf(stderr, "read %s: %s\n", path, strerror(errno));
				exit(2);
			}
			if ((uint32_t) n < page_size)	/* pad a short trailing block */
				memset((char *) page + n, 0, page_size - n);
			block = seg * seg_blocks + local_blk;
			put_block(spc, db, rel, fork, block, page);
			local_blk++;
		}
		close(fd);
	}
	closedir(d);
	free(page);
}

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	const char *pgdata = NULL;
	char		path[8192];
	DIR		   *based;
	struct dirent *de;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--pgdata") == 0 && i + 1 < argc)
			pgdata = argv[++i];
		else if (strcmp(argv[i], "--page-size") == 0 && i + 1 < argc)
			page_size = (uint32_t) strtoul(argv[++i], NULL, 10);
		else
		{
			fprintf(stderr, "usage: %s --shm NAME --pgdata DIR [--page-size N]\n",
					argv[0]);
			return 2;
		}
	}
	if (!shm_name || !pgdata)
	{
		fprintf(stderr, "usage: %s --shm NAME --pgdata DIR [--page-size N]\n",
				argv[0]);
		return 2;
	}
	seg_blocks = (uint32_t) ((1024ULL * 1024 * 1024) / page_size);

	client_attach(shm_name);

	/* shared catalogs in global/ (tablespace 1664, database 0) */
	snprintf(path, sizeof(path), "%s/global", pgdata);
	import_dir(path, GLOBALTABLESPACE_OID, 0);

	/* per-database relations in base/<dboid>/ (default tablespace 1663) */
	snprintf(path, sizeof(path), "%s/base", pgdata);
	based = opendir(path);
	if (based)
	{
		while ((de = readdir(based)) != NULL)
		{
			char		dbpath[8192];
			uint32_t	db;

			if (!isdigit((unsigned char) de->d_name[0]))
				continue;
			db = (uint32_t) strtoul(de->d_name, NULL, 10);
			snprintf(dbpath, sizeof(dbpath), "%s/base/%s", pgdata, de->d_name);
			import_dir(dbpath, DEFAULTTABLESPACE_OID, db);
		}
		closedir(based);
	}

	/*
	 * Limitation: only the default tablespace (base/) and shared catalogs
	 * (global/) are imported.  Relations in user-created tablespaces live under
	 * pg_tblspc/<oid>/... and are NOT walked, so route_all=on would miss them.
	 * Warn loudly rather than silently produce a partial import.
	 */
	snprintf(path, sizeof(path), "%s/pg_tblspc", pgdata);
	based = opendir(path);
	if (based)
	{
		int			n = 0;

		while ((de = readdir(based)) != NULL)
			if (de->d_name[0] != '.')
				n++;
		closedir(based);
		if (n > 0)
			fprintf(stderr, "pagestore_import: WARNING: %d user tablespace(s) in "
					"pg_tblspc are NOT imported; relations there will be missing "
					"under route_all (unsupported -- use the default tablespace)\n",
					n);
	}

	fprintf(stderr, "pagestore_import: done (%s -> timeline 0)\n", pgdata);
	return 0;
}
