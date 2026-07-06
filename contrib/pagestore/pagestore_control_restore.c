/*-------------------------------------------------------------------------
 *
 * pagestore_control_restore.c
 *	  Restore global/pg_control from the store's mirrored control image
 *	  (PGCONTROL_ON_STORE_DESIGN.md restore protocol).
 *
 * pg_control is read and CRC-checked before shared_preload_libraries load, so
 * the pagestore module's hooks cannot serve it at startup.  A compute that has
 * no usable local control file -- a branch bootstrapping without a parent
 * datadir copy -- runs this tool first: it reads the PS_KLASS_CONTROL object
 * as of a given LSN on a given timeline (the daemon's ancestry walk caps a
 * branch's read at its fork point), CRC-verifies the ControlFileData, and
 * installs it atomically.
 *
 * The restore is atomic and durable, and fails closed:
 *   - the image is CRC-verified before anything is written;
 *   - exactly PG_CONTROL_FILE_SIZE bytes go to global/pg_control.tmp, which
 *     is fsync'd and then rename()d over global/pg_control (the live file is
 *     never truncated in place, so a crash mid-restore cannot tear it);
 *   - the containing directory is fsync'd after the rename;
 *   - the claimed daemon channel is released on every exit path;
 *   - any failure exits non-zero without replacing the existing file.
 *
 * Usage:
 *   pagestore_control_restore --shm NAME --timeline N [--lsn X/Y] <datadir>
 *
 * --lsn is the as-of point (a branch passes its fork LSN); without it the
 * newest mirrored image on the timeline's ancestry is restored.
 *
 * src/../contrib/pagestore/pagestore_control_restore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "pagestore_ipc.h"

static void *shm = NULL;
static int	chan = -1;

/* release the claimed channel on every exit path (registered with atexit) */
static void
release_channel(void)
{
	if (shm != NULL && chan >= 0)
	{
		ps_store_release(&ps_channel(shm, chan)->claimed, 0);
		chan = -1;
	}
}

static void
client_attach(const char *shm_name)
{
	int			fd = shm_open(shm_name, O_RDWR, 0600);
	PsShmHeader *hdr;

	if (fd < 0)
	{
		fprintf(stderr, "pagestore_control_restore: shm_open(\"%s\"): %m (is the daemon running?)\n",
				shm_name);
		exit(2);
	}
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);
	if (shm == MAP_FAILED)
	{
		fprintf(stderr, "pagestore_control_restore: mmap: %m\n");
		exit(2);
	}
	hdr = (PsShmHeader *) shm;
	if (hdr->magic != PS_SHM_MAGIC || hdr->version != PS_SHM_VERSION)
	{
		fprintf(stderr, "pagestore_control_restore: shm header mismatch (daemon built against a different PS_SHM_VERSION?)\n");
		exit(2);
	}
	if (hdr->page_size < PG_CONTROL_FILE_SIZE)
	{
		fprintf(stderr, "pagestore_control_restore: daemon page size %u cannot hold a %d-byte control file\n",
				hdr->page_size, PG_CONTROL_FILE_SIZE);
		exit(2);
	}
	/*
	 * Claim a channel that belongs to the control key's owner shard: with a
	 * sharded daemon (--nshards > 1) the channel index selects the serving
	 * shard worker, so a channel from the wrong pool would read another
	 * shard's (empty) index.  Same stride walk as the backend's
	 * ls_claim_channel().
	 */
	{
		PsKey		key;
		uint32_t	nshards = hdr->nshards ? hdr->nshards : 1;
		uint32_t	target;

		memset(&key, 0, sizeof(key));
		key.klass = PS_KLASS_CONTROL;
		target = ps_key_shard(&key, nshards);

		for (uint32_t i = target; i < hdr->nchannels; i += nshards)
			if (ps_cas(&ps_channel(shm, i)->claimed, 0, 1))
			{
				chan = (int) i;
				ps_channel(shm, chan)->shard = target;
				atexit(release_channel);
				return;
			}
	}
	fprintf(stderr, "pagestore_control_restore: no free daemon channel\n");
	exit(2);
}

/*
 * Read the control object as of read_lsn on the timeline's ancestry.
 * Returns true and fills 'out' (PG_CONTROL_FILE_SIZE bytes) on a hit.
 */
static bool
control_read_asof(uint32_t timeline, uint64_t read_lsn, unsigned char *out)
{
	PsChannel  *ch = ps_channel(shm, chan);

	memset((void *) &ch->key, 0, sizeof(ch->key));
	ch->key.klass = PS_KLASS_CONTROL;
	ch->timeline = timeline;
	ch->blocknum = 0;
	ch->req_lsn = read_lsn;
	ch->opcode = PS_OP_READ_AT;
	ps_store_release(&ch->state, PS_STATE_REQUEST);

	/*
	 * Bound the wait: a stale shm object whose daemon is gone would spin
	 * forever, hanging branch bootstrap instead of failing closed.  10s is
	 * orders of magnitude above a healthy daemon's single-page latency.
	 */
	for (int spins = 0; ps_load_acquire(&ch->state) != PS_STATE_DONE; spins++)
	{
		if (spins >= 10 * 1000)
		{
			fprintf(stderr, "pagestore_control_restore: daemon did not answer within 10s (stale --shm or daemon down?)\n");
			exit(1);
		}
		usleep(1000);
	}
	if (ch->result != 1)
		return false;
	memcpy(out, (const void *) ch->data, PG_CONTROL_FILE_SIZE);
	return true;
}

int
main(int argc, char **argv)
{
	const char *shm_name = NULL;
	const char *datadir = NULL;
	uint32_t	timeline = 0;
	bool		have_timeline = false;
	uint64_t	read_lsn = UINT64_MAX;
	unsigned char image[PG_CONTROL_FILE_SIZE];
	ControlFileData control;
	pg_crc32c	crc;
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	char		dirpath[MAXPGPATH];
	int			fd;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--timeline") == 0 && i + 1 < argc)
		{
			char	   *end;
			unsigned long v;

			errno = 0;
			v = strtoul(argv[++i], &end, 10);
			if (errno != 0 || end == argv[i] || *end != '\0' || v > UINT32_MAX)
			{
				fprintf(stderr, "pagestore_control_restore: invalid --timeline \"%s\"\n",
						argv[i]);
				return 2;
			}
			timeline = (uint32_t) v;
			have_timeline = true;
		}
		else if (strcmp(argv[i], "--lsn") == 0 && i + 1 < argc)
		{
			unsigned long long hi,
						lo;
			char	   *end;
			const char *arg = argv[++i];

			errno = 0;
			hi = strtoull(arg, &end, 16);
			if (errno != 0 || end == arg || *end != '/' || hi > UINT32_MAX)
				end = NULL;
			if (end)
			{
				const char *lostr = end + 1;

				errno = 0;
				lo = strtoull(lostr, &end, 16);
				if (errno != 0 || end == lostr || *end != '\0' || lo > UINT32_MAX)
					end = NULL;
			}
			if (!end)
			{
				fprintf(stderr, "pagestore_control_restore: invalid --lsn \"%s\" (expected X/Y hex, no trailing junk)\n",
						arg);
				return 2;
			}
			read_lsn = ((uint64_t) hi << 32) | (uint64_t) lo;
		}
		else if (!datadir)
			datadir = argv[i];
		else
		{
			fprintf(stderr, "pagestore_control_restore: unexpected argument \"%s\"\n",
					argv[i]);
			return 2;
		}
	}
	if (!shm_name || !have_timeline || !datadir)
	{
		fprintf(stderr, "usage: %s --shm NAME --timeline N [--lsn X/Y] <datadir>\n",
				argv[0]);
		return 2;
	}
	if (read_lsn == 0)
	{
		fprintf(stderr, "pagestore_control_restore: --lsn 0/0 selects nothing; a branch passes its fork LSN\n");
		return 2;
	}

	if (snprintf(path, sizeof(path), "%s/global/pg_control", datadir) >= (int) sizeof(path) ||
		snprintf(tmppath, sizeof(tmppath), "%s/global/pg_control.tmp", datadir) >= (int) sizeof(tmppath) ||
		snprintf(dirpath, sizeof(dirpath), "%s/global", datadir) >= (int) sizeof(dirpath))
	{
		fprintf(stderr, "pagestore_control_restore: data directory path too long\n");
		return 2;
	}

	client_attach(shm_name);

	if (!control_read_asof(timeline, read_lsn, image))
	{
		fprintf(stderr, "pagestore_control_restore: no mirrored control image at/below %X/%08X on timeline %u\n",
				(uint32_t) (read_lsn >> 32), (uint32_t) read_lsn, timeline);
		return 1;
	}

	/*
	 * Verify the ControlFileData CRC before writing anything: a torn or
	 * corrupt image must fail closed, never become the cluster's root
	 * metadata.
	 */
	memcpy(&control, image, sizeof(ControlFileData));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, control.crc))
	{
		fprintf(stderr, "pagestore_control_restore: restored control image fails CRC validation\n");
		return 1;
	}
	if (control.pg_control_version != PG_CONTROL_VERSION)
	{
		fprintf(stderr, "pagestore_control_restore: control image version %u does not match this build (%u)\n",
				control.pg_control_version, PG_CONTROL_VERSION);
		return 1;
	}

	/*
	 * Run the same compatibility checks startup would: a CRC-valid image
	 * from an incompatible cluster (different catalog version, block size,
	 * segment geometry) must fail closed HERE, not after it has replaced
	 * pg_control.
	 */
	if (control.catalog_version_no != CATALOG_VERSION_NO)
	{
		fprintf(stderr, "pagestore_control_restore: control image catalog version %u does not match this build (%u)\n",
				control.catalog_version_no, CATALOG_VERSION_NO);
		return 1;
	}
	/* toast_max_chunk_size is backend-only; it derives from BLCKSZ, which is
	 * checked, and startup re-validates the full set anyway */
	if (control.blcksz != BLCKSZ ||
		control.relseg_size != RELSEG_SIZE ||
		control.xlog_blcksz != XLOG_BLCKSZ ||
		control.nameDataLen != NAMEDATALEN ||
		control.indexMaxKeys != INDEX_MAX_KEYS ||
		control.loblksize != (BLCKSZ / 4) ||	/* LOBLKSIZE (backend-only header) */
		control.maxAlign != MAXIMUM_ALIGNOF)
	{
		fprintf(stderr, "pagestore_control_restore: control image layout parameters do not match this build\n");
		return 1;
	}

	/*
	 * Atomic install: exactly PG_CONTROL_FILE_SIZE bytes (never the padded
	 * store object) to a temp file, fsync, rename over the live name, fsync
	 * the directory.  A crash anywhere in this sequence leaves either the old
	 * file or the new one, never a torn mix.
	 */
	fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not create \"%s\": %m\n", tmppath);
		return 1;
	}
	if (write(fd, image, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE ||
		fsync(fd) != 0 || close(fd) != 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not write \"%s\": %m\n", tmppath);
		close(fd);
		unlink(tmppath);
		return 1;
	}
	if (rename(tmppath, path) != 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not rename \"%s\" into place: %m\n", tmppath);
		unlink(tmppath);
		return 1;
	}
	fd = open(dirpath, O_RDONLY);
	if (fd < 0 || fsync(fd) != 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not fsync \"%s\": %m\n", dirpath);
		if (fd >= 0)
			close(fd);

		/*
		 * The rename already happened; a nonzero exit that leaves the new
		 * file installed would break the fail-closed contract (callers
		 * treat failure as "no restored control file").  Best-effort remove
		 * it so no half-durable image is trusted.
		 */
		unlink(path);
		return 1;
	}
	close(fd);

	printf("restored pg_control as of %X/%08X on timeline %u (checkpoint %X/%08X, redo %X/%08X)\n",
		   (uint32_t) (read_lsn >> 32), (uint32_t) read_lsn, timeline,
		   (uint32_t) (control.checkPoint >> 32), (uint32_t) control.checkPoint,
		   (uint32_t) (control.checkPointCopy.redo >> 32),
		   (uint32_t) control.checkPointCopy.redo);
	return 0;
}
