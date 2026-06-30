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
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_ipc.h"

/*
 * Minimal freestanding copy of the pg_control layout and CRC machinery.  This
 * tool intentionally links no PostgreSQL libraries and is built without the
 * server/frontend include paths, but it must reject corrupt control images before
 * installing them as global/pg_control.
 */
#define PG_CONTROL_VERSION	1800
#define PG_CONTROL_FILE_SIZE	8192
#define MOCK_AUTH_NONCE_LEN	32

typedef uint64_t XLogRecPtr;
typedef uint32_t TimeLineID;
typedef uint32_t TransactionId;
typedef uint32_t Oid;
typedef uint32_t MultiXactId;
typedef uint32_t MultiXactOffset;
typedef int64_t pg_time_t;

typedef struct CheckPoint
{
	XLogRecPtr	redo;
	TimeLineID	ThisTimeLineID;
	TimeLineID	PrevTimeLineID;
	bool		fullPageWrites;
	int			wal_level;
	uint64_t	nextXid;
	Oid			nextOid;
	MultiXactId nextMulti;
	MultiXactOffset nextMultiOffset;
	TransactionId oldestXid;
	Oid			oldestXidDB;
	MultiXactId oldestMulti;
	Oid			oldestMultiDB;
	pg_time_t	time;
	TransactionId oldestCommitTsXid;
	TransactionId newestCommitTsXid;
	TransactionId oldestActiveXid;
} CheckPoint;

typedef enum DBState
{
	DB_STARTUP = 0,
	DB_SHUTDOWNED,
	DB_SHUTDOWNED_IN_RECOVERY,
	DB_SHUTDOWNING,
	DB_IN_CRASH_RECOVERY,
	DB_IN_ARCHIVE_RECOVERY,
	DB_IN_PRODUCTION,
} DBState;

typedef struct ControlFileData
{
	uint64_t	system_identifier;
	uint32_t	pg_control_version;
	uint32_t	catalog_version_no;
	DBState		state;
	pg_time_t	time;
	XLogRecPtr	checkPoint;
	CheckPoint	checkPointCopy;
	XLogRecPtr	unloggedLSN;
	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;
	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_wal_senders;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;
	uint32_t	maxAlign;
	double		floatFormat;
	uint32_t	blcksz;
	uint32_t	relseg_size;
	uint32_t	xlog_blcksz;
	uint32_t	xlog_seg_size;
	uint32_t	nameDataLen;
	uint32_t	indexMaxKeys;
	uint32_t	toast_max_chunk_size;
	uint32_t	loblksize;
	bool		float8ByVal;
	uint32_t	data_checksum_version;
	bool		default_char_signedness;
	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];
	uint32_t	crc;
} ControlFileData;

static uint32_t page_size = PS_DEFAULT_PAGE_SIZE;
static void *shm;
static int	chan = -1;

static void
fill_control_pskey(PsKey *key)
{
	key->spcOid = 0;
	key->dbOid = 0;
	key->relNumber = 0;
	key->forkNum = 0;
	key->klass = PS_KLASS_CONTROL;
}

static void
release_claimed_channel(void)
{
	if (shm != NULL && chan >= 0)
	{
		ps_store_release(&ps_channel(shm, (uint32_t) chan)->claimed, 0);
		chan = -1;
	}
}

static uint32_t
crc32c_update(uint32_t crc, const void *data, size_t len)
{
	const unsigned char *p = (const unsigned char *) data;

	while (len-- > 0)
	{
		crc ^= *p++;
		for (int i = 0; i < 8; i++)
			crc = (crc >> 1) ^ (0x82F63B78U & (0U - (crc & 1U)));
	}
	return crc;
}

static uint32_t
crc32c_finalize(const void *data, size_t len)
{
	uint32_t	crc = 0xFFFFFFFFU;

	crc = crc32c_update(crc, data, len);
	return crc ^ 0xFFFFFFFFU;
}

static void
client_attach(const char *shm_name)
{
	int			fd = shm_open(shm_name, O_RDWR, 0600);
	PsShmHeader *hdr;
	PsKey		control_key;
	uint32_t	target;
	uint32_t	stride;

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
	fill_control_pskey(&control_key);
	target = ps_key_shard(&control_key, hdr->nshards);
	stride = hdr->nshards ? hdr->nshards : 1;
	for (uint32_t i = target; i < hdr->nchannels; i += stride)
	{
		PsChannel  *ch = ps_channel(shm, i);

		if (ps_cas(&ch->claimed, 0, 1))
		{
			ch->shard = target;
			chan = (int) i;
			atexit(release_claimed_channel);
			return;
		}
	}
	fprintf(stderr, "no free channel for pg_control shard %u\n", target);
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
		release_claimed_channel();
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
	fill_control_pskey(&ch->key);
}

static int
control_file_crc_ok(const char *buf)
{
	ControlFileData *cf = (ControlFileData *) buf;
	uint32_t	crc;

	if (cf->pg_control_version != PG_CONTROL_VERSION)
		return 0;

	crc = crc32c_finalize(cf, offsetof(ControlFileData, crc));
	return crc == cf->crc;
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
		return 1;
	}

	/* assemble the fixed-size control file (page + zero pad) and write it */
	memset(buf, 0, sizeof(buf));
	n = page_size < PG_CONTROL_FILE_SIZE ? page_size : PG_CONTROL_FILE_SIZE;
	memcpy(buf, ch->data, n);

	if (!control_file_crc_ok(buf))
	{
		fprintf(stderr, "restored pg_control image failed CRC/version validation\n");
		return 1;
	}

	/* daemon work is done; release the channel for reuse before file I/O */
	release_claimed_channel();

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
