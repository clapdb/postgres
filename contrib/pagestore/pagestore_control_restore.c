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
 *   pagestore_control_restore --shm NAME --timeline N [--lsn X/Y]
 *       [--archive-bootstrap] <datadir>
 *
 * --lsn is the as-of point (a branch passes its fork LSN); without it the
 * newest mirrored image on the timeline's ancestry is restored.
 * --archive-bootstrap requires an exact checkpoint-redo --lsn and adjusts
 * only minRecoveryPoint so a fresh cluster skeleton enters archive recovery
 * immediately and can fetch the checkpoint record itself from pagestore.
 *
 * src/../contrib/pagestore/pagestore_control_restore.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * Like pg_resetwal: use postgres.h with FRONTEND defined, not postgres_fe.h,
 * because the control/heap headers we validate against (heaptoast.h and
 * friends) drag in backend-only types.
 */
#define FRONTEND 1
#include "postgres.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/heaptoast.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "common/file_perm.h"
#include "catalog/pg_control.h"
#include "pagestore_ipc.h"

static void *shm = NULL;

/*
 * Shared with the signal handler: only volatile sig_atomic_t reads/writes
 * are defined behavior from an async handler.  'shm' is written once before
 * the handlers are installed and never changes afterwards.
 */
static volatile sig_atomic_t chan = -1;
static volatile sig_atomic_t request_in_flight = 0;

/*
 * Orchestrators cancel bootstrap with SIGTERM/SIGINT, which does NOT run
 * atexit handlers: release the channel from the handler itself (plain
 * atomic stores, async-signal-safe) or repeated cancelled restores leak the
 * control shard's channel pool dry.
 */
static void
restore_signal_exit(int signo)
{
	if (shm != NULL && chan >= 0)
		ps_store_release(&ps_channel(shm, chan)->claimed,
						 request_in_flight ? 2 : 0);
	_exit(128 + signo);
}

/*
 * Release the claimed channel on every exit path (registered with atexit).
 * If a request may still be executing in the daemon (a timed-out wait),
 * mark the channel ABANDONED (2) instead of FREE: handing it to another
 * client while the daemon can still write into the mailbox would corrupt
 * that client's request.  The daemon-side reclaim recovers abandoned
 * channels once their late completion lands.
 */
static void
release_channel(void)
{
	if (shm != NULL && chan >= 0)
	{
		ps_store_release(&ps_channel(shm, chan)->claimed,
						 request_in_flight ? 2 : 0);
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
		sigset_t	claimset,
					oldset;

		memset(&key, 0, sizeof(key));
		key.klass = PS_KLASS_CONTROL;
		target = ps_key_shard(&key, nshards);

		/*
		 * Arm the release paths BEFORE owning anything: a cancellation
		 * landing between the CAS and a later handler install would take
		 * the default signal action, skip atexit, and leave claimed==1
		 * leaked forever (the reclaim path only recovers abandoned
		 * channels).
		 */
		atexit(release_channel);
		signal(SIGTERM, restore_signal_exit);
		signal(SIGINT, restore_signal_exit);
		signal(SIGQUIT, restore_signal_exit);

		/*
		 * A broken stdout pipe must not kill the tool: after the install is
		 * durable the final status printf would otherwise raise the default
		 * SIGPIPE and report failure for a restore that completed.  Writes
		 * fail with EPIPE instead, which the status output ignores.
		 */
		signal(SIGPIPE, SIG_IGN);

		/*
		 * And block those signals across the claim itself: a handler firing
		 * between a successful CAS and the 'chan' assignment would still
		 * see chan == -1 and exit without releasing the channel it just
		 * marked owned -- an unreclaimable leak (reclaim only recovers
		 * ABANDONED channels).
		 */
		sigemptyset(&claimset);
		sigaddset(&claimset, SIGTERM);
		sigaddset(&claimset, SIGINT);
		sigaddset(&claimset, SIGQUIT);
		sigprocmask(SIG_BLOCK, &claimset, &oldset);

		for (uint32_t i = target; i < hdr->nchannels; i += nshards)
			if (ps_cas(&ps_channel(shm, i)->claimed, 0, 1))
			{
				chan = (int) i;
				ps_channel(shm, chan)->shard = target;
				sigprocmask(SIG_SETMASK, &oldset, NULL);
				return;
			}

		/*
		 * No free channel: reclaim an abandoned one whose late completion
		 * has landed (claimed==2 with state DONE) -- once the daemon
		 * publishes DONE it will not touch the mailbox again until the next
		 * REQUEST.  Without this, a single transient timeout on a
		 * few-channels-per-shard configuration would make every later
		 * restore fail with "no free daemon channel".  Claim first, then
		 * verify; put it back if the completion is still owed.
		 */
		for (uint32_t i = target; i < hdr->nchannels; i += nshards)
			if (ps_cas(&ps_channel(shm, i)->claimed, 2, 1))
			{
				uint32_t	st = ps_load_acquire(&ps_channel(shm, i)->state);

				/*
				 * DONE: the daemon finished the abandoned op and will not
				 * touch the mailbox again.  IDLE: the abandoning owner was
				 * cancelled before ever publishing a request, so the daemon
				 * never saw one.  Both are safe to reuse.
				 */
				if (st == PS_STATE_DONE || st == PS_STATE_IDLE)
				{
					chan = (int) i;
					ps_channel(shm, chan)->shard = target;
					sigprocmask(SIG_SETMASK, &oldset, NULL);
					return;
				}
				ps_store_release(&ps_channel(shm, i)->claimed, 2);
			}
		sigprocmask(SIG_SETMASK, &oldset, NULL);
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
	ch->req_seq = 0;
	ch->opcode = PS_OP_READ_AT;

	/*
	 * Mark in-flight BEFORE publishing the request: a cancellation in the
	 * gap then abandons the channel conservatively (freeing a mailbox the
	 * daemon might be writing would be corruption).  The over-abandonment
	 * this can cause -- state still IDLE because the request was never
	 * published -- is reclaimable: both this tool's and the backend's claim
	 * paths reuse abandoned channels whose state is IDLE or DONE.
	 */
	request_in_flight = 1;
	ps_store_release(&ch->state, PS_STATE_REQUEST);

	/*
	 * Bound the wait: a stale shm object whose daemon is gone would spin
	 * forever, hanging branch bootstrap instead of failing closed.  10s is
	 * orders of magnitude above a healthy daemon's single-page latency.  On
	 * timeout the atexit handler marks the channel ABANDONED, never FREE: a
	 * slow-but-live daemon may still write into this mailbox.
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
	request_in_flight = 0;
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
	bool		archive_bootstrap = false;
	unsigned char image[PG_CONTROL_FILE_SIZE];
	ControlFileData control;
	pg_crc32c	crc;
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	char		bakpath[MAXPGPATH];
	char		dirpath[MAXPGPATH];
	bool		had_previous = false;
	sigset_t	installmask;
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
		else if (strcmp(argv[i], "--archive-bootstrap") == 0)
			archive_bootstrap = true;
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
		fprintf(stderr, "usage: %s --shm NAME --timeline N [--lsn X/Y] [--archive-bootstrap] <datadir>\n",
				argv[0]);
		return 2;
	}
	if (read_lsn == 0)
	{
		fprintf(stderr, "pagestore_control_restore: --lsn 0/0 selects nothing; a branch passes its fork LSN\n");
		return 2;
	}
	if (archive_bootstrap && read_lsn == UINT64_MAX)
	{
		fprintf(stderr, "pagestore_control_restore: --archive-bootstrap requires an exact --lsn\n");
		return 2;
	}

	if (snprintf(bakpath, sizeof(bakpath), "%s/global/pg_control.restorebak.XXXXXX", datadir) >= (int) sizeof(bakpath) ||
		snprintf(path, sizeof(path), "%s/global/pg_control", datadir) >= (int) sizeof(path) ||
		snprintf(tmppath, sizeof(tmppath), "%s/global/pg_control.tmp.XXXXXX",
				 datadir) >= (int) sizeof(tmppath) ||
		snprintf(dirpath, sizeof(dirpath), "%s/global", datadir) >= (int) sizeof(dirpath))
	{
		fprintf(stderr, "pagestore_control_restore: data directory path too long\n");
		return 2;
	}

#ifndef WIN32
	/*
	 * Refuse the wrong uid, like the other control-file utilities
	 * (pg_resetwal): a root-owned replacement written by a privileged
	 * wrapper would leave global/pg_control unreadable and unwritable for
	 * the postgres user after the rename.
	 */
	if (geteuid() == 0)
	{
		fprintf(stderr, "pagestore_control_restore: cannot be executed by \"root\"; run as the user that will own the server process\n");
		return 2;
	}
	{
		struct stat st;

		if (stat(datadir, &st) == 0 && st.st_uid != geteuid())
		{
			fprintf(stderr, "pagestore_control_restore: data directory \"%s\" is owned by another user\n", datadir);
			return 2;
		}
	}
#endif

	/*
	 * Bootstrap-only guard: restoring pg_control underneath a running
	 * postmaster would race its own control-file writes.  The same lock
	 * file check the server and pg_resetwal use.
	 */
	{
		char		pidpath[MAXPGPATH];

		if (snprintf(pidpath, sizeof(pidpath), "%s/postmaster.pid", datadir) < (int) sizeof(pidpath) &&
			access(pidpath, F_OK) == 0)
		{
			fprintf(stderr, "pagestore_control_restore: lock file \"%s\" exists; is a server running in this data directory?\n", pidpath);
			return 2;
		}
	}

	/*
	 * Adopt the cluster's file-creation mode (group access or not) so the
	 * restored pg_control keeps the permissions initdb chose; mkstemp
	 * creates 0600 regardless.  On stat failure keep the defaults -- the
	 * datadir will fail harder soon enough.
	 */
	(void) GetDataDirectoryCreatePerm(datadir);

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
	if (control.blcksz != BLCKSZ ||
		control.relseg_size != RELSEG_SIZE ||
		control.xlog_blcksz != XLOG_BLCKSZ ||
		control.slru_pages_per_segment != SLRU_PAGES_PER_SEGMENT ||
		control.nameDataLen != NAMEDATALEN ||
		control.indexMaxKeys != INDEX_MAX_KEYS ||
		control.toast_max_chunk_size != TOAST_MAX_CHUNK_SIZE ||
		control.loblksize != (BLCKSZ / 4) ||	/* LOBLKSIZE (backend-only header) */
		control.maxAlign != MAXIMUM_ALIGNOF ||
		control.floatFormat != FLOATFORMAT_VALUE ||
		control.float8ByVal != FLOAT8PASSBYVAL)
	{
		fprintf(stderr, "pagestore_control_restore: control image layout parameters do not match this build\n");
		return 1;
	}

	/*
	 * xlog_seg_size is per-cluster, not per-build, so it cannot be compared
	 * against a compile-time constant -- but startup rejects anything that
	 * is not a power-of-two in [1MB, 1GB], so an image carrying one must
	 * fail closed here, before it has replaced pg_control.
	 */
	if (!IsValidWalSegSize(control.xlog_seg_size))
	{
		fprintf(stderr, "pagestore_control_restore: control image WAL segment size %u is invalid\n",
				control.xlog_seg_size);
		return 1;
	}

	/*
	 * A fresh initdb skeleton has no source-cluster WAL locally.  With the
	 * ordinary primary control state PostgreSQL first attempts crash recovery,
	 * and therefore refuses to invoke restore_command for the checkpoint
	 * record.  An exact mirrored checkpoint is already a valid archive-recovery
	 * starting point: make that requirement explicit through minRecoveryPoint.
	 * Startup will then fetch the online checkpoint record from the store,
	 * replay through the configured target, and produce its own legitimate
	 * end-of-recovery control state.  Do not forge DB_SHUTDOWNED or checkpoint
	 * record bytes.
	 */
	if (archive_bootstrap)
	{
		if (control.checkPointCopy.redo != (XLogRecPtr) read_lsn)
		{
			fprintf(stderr, "pagestore_control_restore: archive bootstrap requires an exact checkpoint-redo control image\n");
			return 1;
		}
		control.minRecoveryPoint = control.checkPointCopy.redo;
		control.minRecoveryPointTLI = control.checkPointCopy.ThisTimeLineID;
		control.backupStartPoint = InvalidXLogRecPtr;
		control.backupEndPoint = InvalidXLogRecPtr;
		control.backupEndRequired = false;
		INIT_CRC32C(control.crc);
		COMP_CRC32C(control.crc, (char *) &control,
					offsetof(ControlFileData, crc));
		FIN_CRC32C(control.crc);
		memset(image, 0, sizeof(image));
		memcpy(image, &control, sizeof(control));
	}

	/*
	 * Atomic install: exactly PG_CONTROL_FILE_SIZE bytes (never the padded
	 * store object) to a temp file, fsync, rename over the live name, fsync
	 * the directory.  A crash anywhere in this sequence leaves either the old
	 * file or the new one, never a torn mix.
	 */
	/*
	 * mkstemp: two restores racing on the same datadir can share a pid
	 * across pid namespaces (both pid 1 in containers), so a pid-based name
	 * lets one process unlink or rename the other's still-open temp.  A
	 * kernel-unique random name closes that entirely; leftovers from
	 * crashed runs are bounded noise under global/.
	 */
	fd = mkstemp(tmppath);
	if (fd < 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not create temp file \"%s\": %m\n", tmppath);
		return 1;
	}
#ifdef WIN32
	_setmode(fd, _O_BINARY);
#endif
	/* mkstemp creates 0600; the rename must not tighten the cluster's mode */
	if (fchmod(fd, pg_file_create_mode) != 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not set permissions on \"%s\": %m\n", tmppath);
		close(fd);
		unlink(tmppath);
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
	/*
	 * Keep the pre-existing control file recoverable across the rename: on
	 * a real post-rename fsync failure the fail-closed contract requires
	 * the OLD file (or nothing) to be what a failed restore leaves behind.
	 * A hard link costs nothing and is atomic.  The link name is private to
	 * this restore (mkstemp-unique, like the temp file): a shared fixed
	 * name would let two racing restores unlink or replace each other's
	 * rollback link.  Leftovers from crashed runs are bounded noise.
	 */
	{
		int			bakfd = mkstemp(bakpath);

		if (bakfd < 0)
		{
			fprintf(stderr, "pagestore_control_restore: could not reserve backup name \"%s\": %m\n", bakpath);
			unlink(tmppath);
			return 1;
		}
		close(bakfd);
		unlink(bakpath);
	}
	if (link(path, bakpath) == 0)
		had_previous = true;
	else if (errno != ENOENT)
	{
		fprintf(stderr, "pagestore_control_restore: could not back up existing \"%s\": %m\n", path);
		unlink(tmppath);
		return 1;
	}

	/*
	 * Defer termination signals across rename + directory fsync + rollback:
	 * the handler _exit()s immediately, and a cancellation landing between
	 * the rename and the fsync (or its rollback) would leave the NEW file
	 * installed while the tool exits non-zero -- exactly what fail-closed
	 * forbids.  The window is a handful of syscalls; pending signals fire
	 * when the mask is restored.
	 */
	{
		sigset_t	installset;

		sigemptyset(&installset);
		sigaddset(&installset, SIGTERM);
		sigaddset(&installset, SIGINT);
		sigaddset(&installset, SIGQUIT);
		sigprocmask(SIG_BLOCK, &installset, &installmask);
	}

	if (rename(tmppath, path) != 0)
	{
		fprintf(stderr, "pagestore_control_restore: could not rename \"%s\" into place: %m\n", tmppath);
		unlink(tmppath);
		unlink(bakpath);
		sigprocmask(SIG_SETMASK, &installmask, NULL);
		return 1;
	}
	fd = open(dirpath, O_RDONLY);
	if (fd < 0 || fsync(fd) != 0)
	{
		/*
		 * Directories cannot be fsync'd everywhere: mirror frontend
		 * fsync_fname() (src/common/file_utils.c) and ignore EBADF/EINVAL,
		 * which just mean this platform/filesystem does not support it --
		 * retrying forever would brick bootstrap over a non-error.
		 */
		if (fd >= 0 && (errno == EBADF || errno == EINVAL))
		{
			close(fd);
		}
		else
		{
			/*
			 * A real failure: honor the fail-closed contract -- a nonzero
			 * exit leaves the PRE-EXISTING file (restored from the backup
			 * link) or nothing behind, never a half-durable replacement the
			 * caller was told to distrust.
			 */
			fprintf(stderr, "pagestore_control_restore: could not fsync \"%s\": %m\n", dirpath);
			if (fd >= 0)
				close(fd);
			if (had_previous)
			{
				if (rename(bakpath, path) != 0)
					fprintf(stderr, "pagestore_control_restore: could not roll back to the previous pg_control (still at \"%s\"): %m\n", bakpath);
			}
			else
				unlink(path);
			sigprocmask(SIG_SETMASK, &installmask, NULL);
			return 1;
		}
	}
	else
		close(fd);

	/*
	 * The install is durable: from here this run must report success.  A
	 * cancellation deferred by the mask above would _exit(128+sig) on
	 * unmask and tell the orchestrator the restore failed while the new
	 * pg_control is installed; ignore the termination signals instead
	 * (which also discards any pending ones) before restoring the mask.
	 */
	signal(SIGTERM, SIG_IGN);
	signal(SIGINT, SIG_IGN);
	signal(SIGQUIT, SIG_IGN);
	sigprocmask(SIG_SETMASK, &installmask, NULL);

	/* success: the backup link of the replaced file is no longer needed */
	if (had_previous)
		unlink(bakpath);

	printf("restored pg_control as of %X/%08X on timeline %u (checkpoint %X/%08X, redo %X/%08X)\n",
		   (uint32_t) (read_lsn >> 32), (uint32_t) read_lsn, timeline,
		   (uint32_t) (control.checkPoint >> 32), (uint32_t) control.checkPoint,
		   (uint32_t) (control.checkPointCopy.redo >> 32),
		   (uint32_t) control.checkPointCopy.redo);
	return 0;
}
