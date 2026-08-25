/*-------------------------------------------------------------------------
 *
 * backend_localsvc.c
 *	  "localsvc" storage backend: forwards every operation to a separate
 *	  pagestore daemon over a shared-memory mailbox.
 *
 * This is the first backend that takes I/O out of the PostgreSQL process: the
 * backend never opens the data files.  It attaches the daemon's shared-memory
 * segment, claims a private channel, and for each operation posts a request
 * into the channel and busy-waits for the daemon to complete it.  The daemon
 * performs the real storage I/O.
 *
 * Page data crosses the boundary through the channel's data buffer (one copy
 * each way).  For reads on the AIO path, fetch_to_fd() has the daemon place
 * the pages into that buffer and hands the shared-memory fd + offset back to
 * the shim, which issues a normal AIO readv from it -- so PostgreSQL's
 * read-completion machinery runs unmodified.
 *
 * Requires io_method=sync (the AIO read is performed inline by the issuing
 * backend; IO workers would not share this backend's shm fd).
 *
 * src/../contrib/pagestore/backend_localsvc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufpage.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "pagestore_backend.h"
#include "tcop/utility.h"
#include "pagestore_ipc.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "utils/guc.h"

/* GUC: name of the POSIX shm object shared with the daemon */
static char *localsvc_shm_name = NULL;

/* GUC: timeline this backend reads/writes on (0 = main; >0 = a branch) */
static int	localsvc_timeline = 0;

/*
 * GUC: pin every store relation read at this LSN and refuse store writes --
 * the compute becomes a PINNED READER of its timeline's history
 * (READ_CONSISTENCY_DESIGN.md increment 1).  Empty/unset = a normal writer.
 * The horizon of choice is the redo pointer of a durably mirrored checkpoint;
 * its control fence supplies the matching admission sequence that excludes
 * writes accepted later at the same LSN.
 */
static char *localsvc_read_lsn_str = NULL;
static uint64 localsvc_read_lsn = 0;
static uint64 localsvc_read_seq = 0;
static bool localsvc_read_seq_loaded = false;
static uint32 localsvc_read_epoch = 0;

static bool
ls_check_read_lsn(char **newval, void **extra, GucSource source)
{
	uint32		hi,
				lo;

	int			consumed = 0;

	if (*newval == NULL || **newval == '\0')
		return true;
	/* require the whole string to parse: a trailing typo must not silently
	 * pin the reader at a different horizon than configured */
	if (sscanf(*newval, "%X/%X%n", &hi, &lo, &consumed) != 2 ||
		(*newval)[consumed] != '\0')
	{
		GUC_check_errdetail("Expected a WAL LSN like \"0/1A2B3C4D\".");
		return false;
	}
	return true;
}

static void
ls_assign_read_lsn(const char *newval, void *extra)
{
	uint32		hi = 0,
				lo = 0;
	int			consumed = 0;

	if (newval && *newval &&
		sscanf(newval, "%X/%X%n", &hi, &lo, &consumed) == 2 &&
		newval[consumed] == '\0')
		localsvc_read_lsn = ((uint64) hi << 32) | lo;
	else
		localsvc_read_lsn = 0;
	localsvc_read_seq = 0;
	localsvc_read_seq_loaded = false;
	localsvc_read_epoch = localsvc_read_lsn != 0 ? 1 : 0;
}

/* Refuse a store mutation on a pinned reader. */
static void
ls_reject_pinned_write(const char *op)
{
	ereport(ERROR,
			(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
			 errmsg("pagestore: %s refused: this compute is a pinned reader (pagestore.read_lsn)", op)));
}

/* per-backend attachment state */
static void *ls_shm = NULL;
static int	ls_shm_fd = -1;
static int	ls_channel = -1;
static uint32	ls_channel_shard = UINT32_MAX;
static uint32	ls_nchannels = 0;
static uint32	ls_nshards = 1;

/* max logical pages that fit in one transfer (io_unit) for this engine */
#define LS_MAX_PAGES_PER_OP		(PS_IO_UNIT / BLCKSZ)

/*
 * claimed states: 0 = free, 1 = owned by a backend, 2 = abandoned after the
 * owner timed out.  An abandoned channel may still receive a late DONE from the
 * daemon, so never hand it to another backend.
 */
#define LS_CLAIMED_FREE		0
#define LS_CLAIMED_OWNED	1
#define LS_CLAIMED_ABANDONED	2

static void
ls_detach(int code, Datum arg)
{
	if (ls_shm != NULL)
	{
		if (ls_channel >= 0)
		{
			PsChannel  *ch = ps_channel(ls_shm, ls_channel);

			/* release the channel for reuse by a future backend */
			ps_store_release(&ch->claimed, LS_CLAIMED_FREE);
			ls_channel = -1;
			ls_channel_shard = UINT32_MAX;
		}
		munmap(ls_shm, PS_SHM_SIZE);
		ls_shm = NULL;
	}
	if (ls_shm_fd >= 0)
	{
		close(ls_shm_fd);
		ls_shm_fd = -1;
	}
}

void
pagestore_localsvc_detach(void)
{
	ls_detach(0, (Datum) 0);
}

static void
ls_attach(void)
{
	int			fd;
	void	   *shm;
	PsShmHeader *hdr;

	if (ls_shm != NULL)
		return;

	fd = shm_open(localsvc_shm_name, O_RDWR, 0600);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pagestore localsvc could not open shared memory \"%s\": %m",
						localsvc_shm_name),
				 errhint("Is the pagestore daemon running?")));

	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (shm == MAP_FAILED)
	{
		close(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pagestore localsvc could not mmap shared memory: %m")));
	}

	hdr = (PsShmHeader *) shm;
	if (hdr->magic != PS_SHM_MAGIC || hdr->version != PS_SHM_VERSION ||
		hdr->page_size != BLCKSZ)
	{
		uint32		got_magic = hdr->magic;
		uint32		got_version = hdr->version;
		uint32		got_page_size = hdr->page_size;

		munmap(shm, PS_SHM_SIZE);
		close(fd);
		ereport(ERROR,
				(errmsg("pagestore localsvc shared memory incompatible"),
				 errdetail("daemon page_size=%u, this engine BLCKSZ=%d (magic=0x%x version=%u)",
						   got_page_size, BLCKSZ, got_magic, got_version)));
	}
	ls_shm = shm;
	ls_shm_fd = fd;
	ls_nchannels = hdr->nchannels;
	ls_nshards = hdr->nshards ? hdr->nshards : 1;
	on_proc_exit(ls_detach, 0);
}

static uint32
ls_key_shard_klass(const PageStoreRelKey *key, uint32 klass)
{
	if (!key)
		return 0;
	{
		PsKey	k;

		k.spcOid = key->spcOid;
		k.dbOid = key->dbOid;
		k.relNumber = key->relNumber;
		k.forkNum = key->forkNum;
		k.klass = klass;
		return ps_key_shard(&k, ls_nshards);
	}
}

static uint32
ls_key_shard(const PageStoreRelKey *key)
{
	return ls_key_shard_klass(key, PS_KLASS_RELATION);
}

static void
ls_claim_channel(uint32_t shard)
{
	uint32_t stride;
	uint32_t target;

	ls_attach();

	if (ls_channel >= 0 && ls_channel_shard == shard)
		return;

	if (ls_channel >= 0)
	{
		PsChannel  *old = ps_channel(ls_shm, ls_channel);

		ps_store_release(&old->claimed, LS_CLAIMED_FREE);
		ls_channel = -1;
		ls_channel_shard = UINT32_MAX;
	}

	if (ls_nchannels == 0 || ls_nshards == 0)
	{
		ereport(ERROR,
				(errmsg("pagestore localsvc: daemon has zero channels")));
	}

	target = shard % ls_nshards;
	stride = ls_nshards;

	for (uint32_t i = target; i < ls_nchannels; i += stride)
	{
		PsChannel  *ch = ps_channel(ls_shm, i);

		if (ps_cas(&ch->claimed, LS_CLAIMED_FREE, LS_CLAIMED_OWNED))
		{
			ch->shard = target;
			ls_channel = (int) i;
			ls_channel_shard = target;
			return;
		}
	}

	/*
	 * No free channel: try to reclaim an abandoned one whose late completion
	 * has since arrived.  A channel is abandoned when its owner timed out
	 * after posting REQUEST; once the daemon store-releases DONE it will not
	 * touch the channel again until the next REQUEST, so a DONE abandoned
	 * channel is safe to reuse.  Claim it first (so no other backend races
	 * the same reclaim), then check the state; if the daemon still owes the
	 * completion, put it back.  Without this, every timed-out op (e.g. a
	 * REQUIRE_BRANCH startup check against a slow daemon) leaks a channel
	 * until the fixed pool is exhausted.
	 */
	for (uint32_t i = target; i < ls_nchannels; i += stride)
	{
		PsChannel  *ch = ps_channel(ls_shm, i);

		if (ps_cas(&ch->claimed, LS_CLAIMED_ABANDONED, LS_CLAIMED_OWNED))
		{
			uint32_t	st = ps_load_acquire(&ch->state);

			/*
			 * DONE: the daemon finished the abandoned op.  IDLE: the
			 * abandoning owner was cancelled before ever publishing a
			 * request (e.g. the restore tool's signal path), so the daemon
			 * never saw one.  Both mean the daemon will not touch the
			 * mailbox until the next REQUEST -- safe to reuse.
			 */
			if (st == PS_STATE_DONE || st == PS_STATE_IDLE)
			{
				ch->shard = target;
				ls_channel = (int) i;
				ls_channel_shard = target;
				return;
			}
			ps_store_release(&ch->claimed, LS_CLAIMED_ABANDONED);
		}
	}
	ereport(ERROR,
			(errmsg("pagestore localsvc: no free channel in shard %u (max %u)",
					target, ls_nchannels)));
}

static PsChannel *
ls_chan_for_key(const PageStoreRelKey *key)
{
	ls_claim_channel(ls_key_shard(key));
	return ps_channel(ls_shm, ls_channel);
}

/* Like ls_chan_for_key but routes by the object's actual class, so a non-relation
 * object lands on the shard worker that owns shard_for(key) with that klass. */
static PsChannel *
ls_chan_for_key_klass(const PageStoreRelKey *key, uint32 klass)
{
	ls_claim_channel(ls_key_shard_klass(key, klass));
	return ps_channel(ls_shm, ls_channel);
}

static PsChannel *
ls_chan(void)
{
	ls_claim_channel(0);
	return ps_channel(ls_shm, ls_channel);
}

/*
 * Post the request the caller has already filled into the channel, then
 * busy-wait for the daemon to complete it.
 *
 * Ordering protocol (pairs with the daemon's poll loop):
 *	 - The store-release of state=REQUEST publishes all the request fields and
 *	   payload we wrote before it; the daemon's load-acquire of REQUEST sees them.
 *	 - The daemon writes the result/payload, then store-releases state=DONE; our
 *	   load-acquire of DONE makes those writes visible here.
 * So no result field may be read before the DONE is observed.
 *
 * We busy-wait rather than block because the smgr call is synchronous anyway
 * (the backend has nothing else to do).  CHECK_FOR_INTERRUPTS() lets a query
 * cancel / backend terminate escape a wedged daemon.  pause() is a CPU hint
 * that makes the spin cheaper on x86.
 *
 * There is exactly one outstanding request per channel, so REQUEST and DONE
 * simply alternate; the IDLE state is only the post-zeroing initial value.
 *
 * ls_exec_wait returns the daemon's status; ls_exec_timeout wraps it and
 * raises ERROR on anything but OK.  End-of-transaction paths that must not
 * throw (post-commit/post-abort unlinks) use the wait variant and downgrade.
 */
static uint8
ls_exec_wait(PsChannel *ch, int timeout_ms)
{
	uint32		spins = 0;
	time_t		deadline = 0;

	if (timeout_ms > 0)
		deadline = time(NULL) + (timeout_ms + 999) / 1000;

	ch->shard = ls_channel_shard;

	ps_store_release(&ch->state, PS_STATE_REQUEST);

	/*
	 * Any error escaping this wait -- the timeout below, but also a query
	 * cancel or backend terminate raised by CHECK_FOR_INTERRUPTS() -- must
	 * abandon the channel first: the REQUEST may still be in the daemon's
	 * pipeline, and reusing the channel for the next op would collide with
	 * its late completion.  (An abandoned channel is reclaimed once its
	 * DONE arrives; see ls_claim_channel.)
	 */
	PG_TRY();
	{
		while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		{
			if (((++spins) & 0xFFF) == 0)
			{
				CHECK_FOR_INTERRUPTS();
				if (deadline != 0 && time(NULL) >= deadline)
					ereport(ERROR,
							(errmsg("pagestore localsvc: timed out waiting for daemon op %u",
									ch->opcode)));
			}
#if defined(__x86_64__) || defined(__i386__)
			__builtin_ia32_pause();
#endif
		}
	}
	PG_CATCH();
	{
		ps_store_release(&ch->claimed, LS_CLAIMED_ABANDONED);
		ls_channel = -1;
		ls_channel_shard = UINT32_MAX;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return ch->status;
}

static void
ls_exec_timeout(PsChannel *ch, int timeout_ms)
{
	if (ls_exec_wait(ch, timeout_ms) != PS_STATUS_OK)
		ereport(ERROR,
				(errmsg("pagestore localsvc: daemon reported error for op %u",
						ch->opcode)));
}

static void
ls_exec(PsChannel *ch)
{
	ls_exec_timeout(ch, 0);
}

static void
ls_fill_key(PsChannel *ch, const PageStoreRelKey *key)
{
	ch->shard = ls_channel_shard;
	ch->key.spcOid = key->spcOid;
	ch->key.dbOid = key->dbOid;
	ch->key.relNumber = key->relNumber;
	ch->key.forkNum = key->forkNum;
	ch->key.klass = PS_KLASS_RELATION;	/* the smgr shim only stores relation pages */
	ch->timeline = (uint32) localsvc_timeline;	/* this backend's timeline */

	/*
	 * Channels are reused across op kinds and req_lsn now has meaning for
	 * every metadata op (an event LSN for mutations, an as-of horizon for
	 * queries).  Clear it here so no sender inherits a stale value; ops
	 * that need one assign it after this call.
	 */
	ch->req_lsn = 0;
	ch->req_seq = 0;
}

/* Resolve the configured checkpoint redo to the control mirror's durable
 * admission fence.  Block 2 is versioned by redo itself, so requiring an exact
 * hit prevents an older checkpoint's fence from being used for a newer R. */
bool
pagestore_localsvc_read_fence_for_timeline_timeout(uint32 timeline,
											  uint64 read_lsn, uint64 *read_seq,
											  int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsAdmissionFence fence;
	PsChannel  *ch;

	Assert(read_lsn != 0);

	ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);
	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->timeline = timeline;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = 2;
	ch->req_lsn = read_lsn;
	ls_exec_timeout(ch, timeout_ms);
	if (ch->result == 0 || ch->req_lsn != read_lsn)
		return false;
	memcpy(&fence, ch->data, sizeof(fence));
	if (fence.magic != PS_ADMISSION_FENCE_MAGIC ||
		fence.version != PS_ADMISSION_FENCE_VERSION ||
		fence.redo_lsn != read_lsn || fence.admission_seq == 0)
		return false;
	*read_seq = fence.admission_seq;
	return true;
}

bool
pagestore_localsvc_read_fence_timeout(uint64 read_lsn, uint64 *read_seq,
									 int timeout_ms)
{
	return pagestore_localsvc_read_fence_for_timeline_timeout(
		pagestore_localsvc_timeline(), read_lsn, read_seq, timeout_ms);
}

static uint64
ls_pinned_read_seq(void)
{
	if (localsvc_read_lsn == 0)
		return 0;
	if (localsvc_read_seq_loaded)
		return localsvc_read_seq;
	if (!pagestore_localsvc_read_fence_timeout(localsvc_read_lsn,
											  &localsvc_read_seq, 0))
		ereport(ERROR,
				(errmsg("pagestore: no valid admission fence for pinned read LSN %X/%08X",
						(uint32) (localsvc_read_lsn >> 32),
						(uint32) localsvc_read_lsn)));
	localsvc_read_seq_loaded = true;
	return localsvc_read_seq;
}

/*
 * --- vtable ops ---------------------------------------------------------
 *
 * Each op fills the claimed channel's request fields and calls ls_exec(),
 * which posts it to the daemon and waits for the result.  See the
 * PageStoreBackend interface in pagestore_backend.h for the exact contract of
 * each operation.
 */

/*
 * WAL-position stamp for a fork-mutating op (create/truncate/unlink).  The
 * daemon keys the fork's size history by it, so as-of NBLOCKS/EXISTS answers
 * resolve against these events like page reads resolve against pd_lsns.
 *
 * Normal operation stamps XactLastRecEnd: the end of the WAL record THIS
 * backend just inserted, which for every caller is the record of the
 * mutation itself -- log_smgrcreate before smgrcreate (core orders WAL
 * before action), SMGR_TRUNCATE just before smgr_truncate, the commit
 * record just before post-commit unlinks.  The global insert pointer would
 * over-stamp: unrelated concurrent inserts push it past the mutation
 * record, and a horizon between the two would miss the mutation.  During
 * replay the honest position is the END of the record being replayed
 * (GetCurrentReplayRecPtr; the last-REPLAYED pointer only advances after
 * rm_redo returns, i.e. it names the PREVIOUS record).
 *
 * End-of-transaction unlinks are the subtle case: smgrDoPendingDeletes()
 * runs after RecordTransactionCommit()/RecordTransactionAbort(), both of
 * which RESET XactLastRecEnd -- but the commit record's end survives in
 * XactLastCommitEnd and the abort record's in XactLastAbortEnd, and
 * whichever this backend produced LAST is the record that decided the
 * cleanup (a commit for a DROP, an abort for a created-then-rolled-back
 * relation; stamping the older one would sort the unlink below the
 * relation's own CREATE and resurrect it).  WAL-less mutations (unlogged
 * relations) can leave all of these at older records; their content is
 * not LSN-ordered to begin with.
 */
static uint64
ls_op_lsn(void)
{
	if (AmStartupProcess())
		return (uint64) GetCurrentReplayRecPtr(NULL);
	if (XactLastRecEnd != 0)
		return (uint64) XactLastRecEnd;
	return (uint64) Max(XactLastCommitEnd, XactLastAbortEnd);
}

/* A materializer is writable, not a pinned reader, but during recovery it
 * must not resolve relation metadata or page bytes beyond the WAL record it
 * is currently replaying.  Doing so exposes a future CREATE/TRUNCATE and can
 * make redo attempt an extension against the wrong fork generation. */
static uint64
ls_read_lsn(void)
{
	if (localsvc_read_lsn != 0)
		return localsvc_read_lsn;
	if (RecoveryInProgress())
	{
		XLogRecPtr replay;

		/* Only startup is inside rm_redo and may use the in-progress record.
		 * A hot-standby backend must observe the last completed replay record. */
		if (!AmStartupProcess())
			return (uint64) GetXLogReplayRecPtr(NULL);
		replay = GetCurrentReplayRecPtr(NULL);

		/* Between records the current pointer is invalid, but read-only
		 * backends can still touch catalogs.  Bound those reads at the last
		 * completed replay position rather than accidentally using newest. */
		if (replay == InvalidXLogRecPtr)
			replay = GetXLogReplayRecPtr(NULL);
		return (uint64) replay;
	}
	return 0;
}

/*
 * Make the fork exist in the store (with zero blocks).  isRedo is set during
 * WAL replay, where re-creating an existing fork must be tolerated.
 */
static void
ls_create(const PageStoreRelKey *key, void *localreln, bool isRedo,
		  bool isRedoEnsure)
{
	PsChannel  *ch = ls_chan_for_key(key);


	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("relation create");

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = isRedoEnsure ? 2 : (isRedo ? 1 : 0);
	ch->req_lsn = ls_op_lsn();
	ls_exec(ch);
}

/* Does the fork exist in the store? */
static bool
ls_fork_exists(const PageStoreRelKey *key, void *localreln)
{
	uint64		read_seq = ls_pinned_read_seq();
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_EXISTS;
	ch->req_lsn = ls_read_lsn();
	ch->req_seq = read_seq;
	ls_exec(ch);
	return ch->result != 0;
}

/* Remove the fork entirely. */
static void
ls_unlink(const PageStoreRelKey *key, bool isRedo)
{
	PsChannel  *ch = ls_chan_for_key(key);


	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("relation unlink");

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_UNLINK;
	ch->is_redo = isRedo ? 1 : 0;
	ch->req_lsn = ls_op_lsn();

	/* WAL redo must fail if its durable DEAD event cannot be recorded. */
	if (isRedo)
	{
		ls_exec(ch);
		return;
	}

	/*
	 * Non-redo unlinks run from end-of-transaction cleanup, after the
	 * transaction's outcome is decided and its buffers are dropped.  A failed
	 * durable DEAD event means the fork lingers; warn rather than throwing from
	 * cleanup after commit/abort.
	 */
	if (ls_exec_wait(ch, 0) != PS_STATUS_OK)
		ereport(WARNING,
				(errmsg("pagestore: store unlink failed; the fork remains recorded in the store")));
}

/* Current size of the fork, in blocks. */
static BlockNumber
ls_nblocks(const PageStoreRelKey *key, void *localreln)
{
	uint64		read_seq = ls_pinned_read_seq();
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = ls_read_lsn();
	ch->is_redo = AmStartupProcess() ? 1 : 0;
	ch->req_seq = read_seq;
	ls_exec(ch);
	return (BlockNumber) ch->result;
}

/*
 * Shrink the fork from old_blocks down to nblocks blocks (e.g. VACUUM trimming
 * trailing empty pages).  In this COW store the daemon only lowers the fork's
 * recorded size; historical versions of the trimmed blocks stay in the log.
 */
static void
ls_truncate(const PageStoreRelKey *key, void *localreln,
			BlockNumber old_blocks, BlockNumber nblocks)
{
	PsChannel  *ch = ls_chan_for_key(key);


	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("relation truncate");

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_TRUNCATE;
	ch->old_nblocks = old_blocks;
	ch->nblocks = nblocks;
	ch->req_lsn = ls_op_lsn();
	ls_exec(ch);
}

/*
 * Read nblocks pages starting at blocknum into buffers[].  A request carries at
 * most one io_unit of page data, so larger reads are split into io_unit-sized
 * chunks; the daemon's page bytes are copied out of the channel buffer.
 */
static void
ls_readv(const PageStoreRelKey *key, void *localreln,
		 BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	uint64		read_seq = ls_pinned_read_seq();
	PsChannel  *ch = ls_chan_for_key(key);
	BlockNumber done = 0;

	while (done < nblocks)
	{
		BlockNumber chunk = Min(nblocks - done, LS_MAX_PAGES_PER_OP);

		ls_fill_key(ch, key);
		ch->opcode = PS_OP_READV;
		ch->blocknum = blocknum + done;
		ch->nblocks = chunk;
		ch->req_lsn = ls_read_lsn();
		ch->req_seq = read_seq;
		ls_exec(ch);

		for (BlockNumber i = 0; i < chunk; i++)
			memcpy(buffers[done + i], ch->data + (size_t) i * BLCKSZ, BLCKSZ);
		done += chunk;
	}
}

/*
 * Overwrite nblocks existing pages starting at blocknum from buffers[] (the
 * dirty-page flush path); unlike extend() this does not grow the fork.  Pages
 * are copied into the channel buffer and sent in io_unit-sized chunks.
 */
static void
ls_writev(const PageStoreRelKey *key, void *localreln,
		  BlockNumber blocknum, const void **buffers, BlockNumber nblocks,
		  bool skipFsync)
{
	PsChannel  *ch = ls_chan_for_key(key);
	BlockNumber done = 0;


	/*
	 * A pinned reader has no legitimate page writes at all: hint-bit
	 * dirtying and on-access pruning are suppressed at the source
	 * (page_maintenance_suppressed), so nothing hint-only can reach this
	 * path.  Anything that does arrive is a real mutation that escaped the
	 * read-only default -- including WAL-less writes (unlogged relations,
	 * fake-LSN index pages) whose pd_lsn never advances past the pin.
	 * Fail closed instead of inferring intent from pd_lsn.
	 */
	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("page write");

	while (done < nblocks)
	{
		BlockNumber chunk = Min(nblocks - done, LS_MAX_PAGES_PER_OP);

		ls_fill_key(ch, key);
		ch->opcode = PS_OP_WRITEV;
		ch->blocknum = blocknum + done;
		ch->nblocks = chunk;
		ch->skip_fsync = skipFsync ? 1 : 0;
		for (BlockNumber i = 0; i < chunk; i++)
			memcpy(ch->data + (size_t) i * BLCKSZ, buffers[done + i], BLCKSZ);
		ls_exec(ch);
		done += chunk;
	}
}

/*
 * Grow the fork by exactly one block at blocknum, written from buffer -- the
 * single-page grow path.  Contrast: zeroextend() bulk-adds many empty blocks,
 * and writev() overwrites existing blocks instead of growing the fork.
 */
static void
ls_extend(const PageStoreRelKey *key, void *localreln,
		  BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	PsChannel  *ch = ls_chan_for_key(key);


	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("relation extend");

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_EXTEND;
	ch->blocknum = blocknum;
	ch->nblocks = 1;
	ch->skip_fsync = skipFsync ? 1 : 0;
	memcpy(ch->data, buffer, BLCKSZ);
	ls_exec(ch);
}

/*
 * Bulk-extend the fork by nblocks zero-filled blocks starting at blocknum.
 *
 * Unlike extend() (which adds one block written from a buffer), zeroextend()
 * pre-allocates many empty blocks in one call and sends no page data at all --
 * only the block count.  The engine uses it to grow a relation by several
 * pages at once (e.g. under concurrent insertion).  In this backend the daemon
 * just advances the fork's recorded size; the new blocks have no stored
 * version yet, so they read back as zeros until written.
 */
static void
ls_zeroextend(const PageStoreRelKey *key, void *localreln,
			  BlockNumber blocknum, int nblocks, bool skipFsync)
{
	PsChannel  *ch = ls_chan_for_key(key);


	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("relation extend");

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = blocknum;
	ch->nblocks = nblocks;
	ch->skip_fsync = skipFsync ? 1 : 0;
	/*
	 * Zero-extends have no WAL record of their own (the extension is
	 * implied by later content); the insert position is the best honest
	 * upper bound, and never-written blocks read as zeros either way.
	 */
	ch->req_lsn = RecoveryInProgress() ?
		(uint64) GetCurrentReplayRecPtr(NULL) : (uint64) GetXLogInsertRecPtr();
	ls_exec(ch);
}

/*
 * Force the fork's data durable in the daemon immediately (fsync now), versus
 * the normal path where writev() lets durability be deferred to a checkpoint.
 */
static void
ls_immedsync(const PageStoreRelKey *key, void *localreln)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_IMMEDSYNC;
	ls_exec(ch);
}

static bool
ls_fetch_to_fd(const PageStoreRelKey *key, BlockNumber blocknum,
			   BlockNumber nblocks, int *out_fd, uint64 *out_offset)
{
	uint64		read_seq = ls_pinned_read_seq();
	PsChannel  *ch = ls_chan_for_key(key);

	if (nblocks > LS_MAX_PAGES_PER_OP)
		ereport(ERROR,
				(errmsg("pagestore localsvc: read of %u blocks exceeds channel capacity %d",
						nblocks, LS_MAX_PAGES_PER_OP),
				 errhint("Lower io_combine_limit.")));

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_READV;
	ch->blocknum = blocknum;
	ch->nblocks = nblocks;
	ch->req_lsn = ls_read_lsn();
	ch->req_seq = read_seq;
	ls_exec(ch);

	/* the pages now live in the channel data buffer, readable via the shm fd */
	*out_fd = ls_shm_fd;
	*out_offset = ps_channel_data_offset((uint32) ls_channel);
	return true;
}

const PageStoreBackend PageStoreBackendLocalSvc = {
	.name = "localsvc",
	.uses_local_files = false,
	.max_combine_pages = LS_MAX_PAGES_PER_OP,
	.init = NULL,
	.create = ls_create,
	.fork_exists = ls_fork_exists,
	.unlink = ls_unlink,
	.nblocks = ls_nblocks,
	.truncate = ls_truncate,
	.readv = ls_readv,
	.writev = ls_writev,
	.extend = ls_extend,
	.zeroextend = ls_zeroextend,
	.immedsync = ls_immedsync,
	.fetch_to_fd = ls_fetch_to_fd,
};

/*
 * Read a single page as-of a snapshot LSN from the daemon's COW version log.
 * Exposed for the pagestore_read_at() SQL function; copies BLCKSZ bytes into
 * 'out'.
 */
void
pagestore_localsvc_read_at(const PageStoreRelKey *key, BlockNumber blocknum,
						   uint64 lsn, void *out)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = blocknum;
	ch->nblocks = 1;
	ch->req_lsn = lsn;
	ls_exec(ch);
	memcpy(out, ch->data, BLCKSZ);
}

/*
 * Create a branch (new timeline) forking from parent_tl at branch_lsn.  This is
 * an O(1) metadata operation in the daemon -- no page data is copied.  Exposed
 * for the pagestore_create_branch() SQL function.
 */
void
pagestore_localsvc_check_branch(uint32 new_tl, uint32 parent_tl,
								uint64 branch_lsn)
{
	PsChannel  *ch = ls_chan();

	ch->opcode = PS_OP_CHECK_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	ls_exec(ch);
}

void
pagestore_localsvc_require_branch(uint32 new_tl, uint32 parent_tl,
								  uint64 branch_lsn)
{
	pagestore_localsvc_require_branch_timeout(new_tl, parent_tl, branch_lsn, 0);
}

void
pagestore_localsvc_require_branch_timeout(uint32 new_tl, uint32 parent_tl,
										  uint64 branch_lsn, int timeout_ms)
{
	PsChannel  *ch = ls_chan();

	ch->opcode = PS_OP_REQUIRE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	ls_exec_timeout(ch, timeout_ms);
}

/*
 * Make everything the daemon has accepted durable (ps_storage->sync()).
 * Used by the control mirror after draining queued pg_control images: the
 * design's durability contract is local file first, then store mirror +
 * store sync.
 */
void
pagestore_localsvc_store_sync(void)
{
	pagestore_localsvc_store_sync_timeout(0);
}

/* Bounded-wait variant; see pagestore_localsvc_obj_write_timeout. */
void
pagestore_localsvc_store_sync_timeout(int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);

	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_IMMEDSYNC;
	ls_exec_timeout(ch, timeout_ms);
}

/* Publish the nonblocking side of a checkpoint admission gate.  The control
 * hook calls this in a critical section, so attachment must already exist and
 * failure merely means that this control image cannot publish a fence. */
bool
pagestore_localsvc_admission_fence_begin(uint64 redo_lsn, uint64 *token)
{
	PsShmHeader *hdr;
	uint64		epoch;

	*token = 0;
	if (ls_shm == NULL)
		return false;
	hdr = (PsShmHeader *) ls_shm;
	if (!ps_cas(&hdr->admission_fence_owner, 0, (uint32) MyProcPid))
		return false;
	epoch = ps_fetch_add_u64(&hdr->admission_fence_epoch, 1) + 1;
	ps_store_release_u64(&hdr->admission_pending_lsn, redo_lsn);
	ps_store_release_u64(&hdr->admission_pending_epoch, epoch);
	*token = epoch;
	return true;
}

bool
pagestore_localsvc_admission_fence_active(uint64 token)
{
	PsShmHeader *hdr;

	if (token == 0 || ls_shm == NULL)
		return false;
	hdr = (PsShmHeader *) ls_shm;
	return ps_load_acquire_u64(&hdr->admission_pending_epoch) == token &&
		ps_load_acquire(&hdr->admission_fence_owner) == (uint32) MyProcPid;
}

uint64
pagestore_localsvc_admission_barrier_timeout(int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);

	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_ADMISSION_BARRIER;
	ls_exec_timeout(ch, timeout_ms);
	return ch->req_seq;
}

void
pagestore_localsvc_admission_fence_end(uint64 token)
{
	PsShmHeader *hdr;

	if (token == 0 || ls_shm == NULL)
		return;
	hdr = (PsShmHeader *) ls_shm;
	if (ps_load_acquire(&hdr->admission_fence_owner) != (uint32) MyProcPid ||
		!ps_cas_u64(&hdr->admission_pending_epoch, token, 0))
		return;
	ps_store_release_u64(&hdr->admission_pending_lsn, 0);
	ps_store_release(&hdr->admission_fence_owner, 0);
}

/*
 * Durable WAL retention floor of this compute's timeline ancestry: the store
 * must keep shipped WAL at/above it for the mirrored pg_control images to
 * stay restorable.  0 = no control image constrains WAL yet.
 */
uint64
pagestore_localsvc_wal_retain_floor(void)
{
	PsChannel  *ch;
	PageStoreRelKey key = {0};

	/* route by the control key so the query lands on its owner shard */
	ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);
	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_WAL_RETAIN_FLOOR;
	ch->timeline = (uint32) localsvc_timeline;
	ls_exec(ch);
	return ch->req_lsn;
}

/*
 * Publish or replace one controller-owned retention horizon.  Keep the daemon
 * status visible: PS_STATUS_STALE is an ownership-fencing result, not a
 * transport failure and not a successful retry.
 */
uint8
pagestore_localsvc_retention_set(uint32 timeline, uint32 owner_kind,
								 uint64 owner_id, uint32 generation,
								 uint32 resources, uint64 lsn,
								 uint64 admission_seq)
{
	return pagestore_localsvc_retention_set_timeout(timeline, owner_kind,
												 owner_id, generation, resources,
												 lsn, admission_seq, 0);
}

uint8
pagestore_localsvc_retention_set_timeout(uint32 timeline, uint32 owner_kind,
										 uint64 owner_id, uint32 generation,
										 uint32 resources, uint64 lsn,
										 uint64 admission_seq,
										 int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch;

	/* Zero is reserved for replaying retention records predating protocol v28. */
	if (generation == 0 || admission_seq == 0 ||
		admission_seq >= UINT64_MAX - 1)
		return PS_STATUS_ERROR;
	ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);

	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_RETENTION_PIN_SET;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->old_nblocks = generation;
	ch->parent_timeline = resources;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	ch->nblocks = (uint32) admission_seq;
	ch->pad1 = (uint32) (admission_seq >> 32);
	return ls_exec_wait(ch, timeout_ms);
}

uint8
pagestore_localsvc_retention_reserve_timeout(uint32 timeline,
									 uint32 owner_kind, uint64 owner_id,
									 uint32 generation, uint32 resources,
									 uint64 lsn, uint64 *admission_seq,
									 int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch;
	uint8		status;

	*admission_seq = 0;
	if (generation == 0)
		return PS_STATUS_ERROR;
	ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);
	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->old_nblocks = generation;
	ch->parent_timeline = resources;
	ch->req_seq = owner_id;
	ch->req_lsn = lsn;
	status = ls_exec_wait(ch, timeout_ms);
	if (status == PS_STATUS_OK && ch->datalen == sizeof(*admission_seq))
		memcpy(admission_seq, ch->data, sizeof(*admission_seq));
	else if (status == PS_STATUS_OK)
		status = PS_STATUS_ERROR;
	return status;
}

/* A successful DROP leaves the store-side generation tombstone in place. */
uint8
pagestore_localsvc_retention_drop(uint32 timeline, uint32 owner_kind,
								  uint64 owner_id, uint32 generation)
{
	return pagestore_localsvc_retention_drop_timeout(timeline, owner_kind,
												  owner_id, generation, 0);
}

uint8
pagestore_localsvc_retention_drop_timeout(uint32 timeline, uint32 owner_kind,
										  uint64 owner_id, uint32 generation,
										  int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch;

	if (generation == 0)
		return PS_STATUS_ERROR;
	ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);

	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_RETENTION_PIN_DROP;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->old_nblocks = generation;
	ch->req_seq = owner_id;
	return ls_exec_wait(ch, timeout_ms);
}

uint8
pagestore_localsvc_retention_lookup(uint32 timeline, uint32 owner_kind,
									uint64 owner_id, PsRetentionPin *pin,
									bool *found, int timeout_ms)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);
	uint8		status;

	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_RETENTION_PIN_LOOKUP;
	ch->timeline = timeline;
	ch->blocknum = owner_kind;
	ch->req_seq = owner_id;
	status = ls_exec_wait(ch, timeout_ms);
	if (found != NULL)
		*found = status == PS_STATUS_OK && ch->result != 0;
	if (pin != NULL && status == PS_STATUS_OK && ch->result != 0)
	{
		memset(pin, 0, sizeof(*pin));
		pin->timeline = ch->timeline;
		pin->owner_kind = ch->blocknum;
		pin->resources = ch->parent_timeline;
		pin->generation = ch->old_nblocks;
		pin->owner_id = ch->req_seq;
		pin->lsn = ch->req_lsn;
		if (ch->datalen != sizeof(pin->admission_seq))
			return PS_STATUS_ERROR;
		memcpy(&pin->admission_seq, ch->data, sizeof(pin->admission_seq));
	}
	return status;
}

/*
 * Enumerate the durable retention registry.  Keep a missing index distinct
 * from an IPC/daemon failure so a restarting controller can reconcile the
 * complete owner set without treating an error as end-of-enumeration.
 */
uint8
pagestore_localsvc_retention_get(uint32 index, PsRetentionPin *pin,
								 uint32 *count, uint64 *epoch, bool *found)
{
	PageStoreRelKey key = {0};
	PsChannel  *ch;
	PsRetentionGetResult result;
	uint8		status;

	if (pin != NULL)
		memset(pin, 0, sizeof(*pin));
	if (count != NULL)
		*count = 0;
	if (found != NULL)
		*found = false;
	if (epoch == NULL)
		return PS_STATUS_ERROR;

	ch = ls_chan_for_key_klass(&key, PS_KLASS_CONTROL);
	ls_fill_key(ch, &key);
	ch->key.klass = PS_KLASS_CONTROL;
	ch->opcode = PS_OP_RETENTION_PIN_GET;
	ch->blocknum = index;
	ch->req_lsn = *epoch;
	status = ls_exec_wait(ch, 0);
	if (status != PS_STATUS_OK)
	{
		if (status == PS_STATUS_STALE)
			*epoch = 0;
		return status;
	}
	if (ch->datalen != sizeof(result))
		return PS_STATUS_ERROR;
	memcpy(&result, ch->data, sizeof(result));
	*epoch = result.mutation_epoch;
	if (count != NULL)
		*count = ch->nblocks;
	if (ch->result == 0)
		return PS_STATUS_OK;
	if (pin != NULL)
	{
		pin->timeline = ch->timeline;
		pin->owner_kind = ch->blocknum;
		pin->resources = ch->parent_timeline;
		pin->generation = ch->old_nblocks;
		pin->owner_id = ch->req_seq;
		pin->lsn = ch->req_lsn;
		pin->admission_seq = result.admission_seq;
	}
	if (found != NULL)
		*found = true;
	return PS_STATUS_OK;
}

/*
 * Create a branch (new timeline) forking from parent_tl at branch_lsn.  This is
 * an O(1) metadata operation in the daemon -- no page data is copied.  Exposed
 * for the pagestore_create_branch() SQL function.
 */
void
pagestore_localsvc_create_branch(uint32 new_tl, uint32 parent_tl,
								 uint64 branch_lsn)
{
	PsChannel  *ch = ls_chan();

	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("branch creation");

	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->timeline = new_tl;
	ch->parent_timeline = parent_tl;
	ch->req_lsn = branch_lsn;
	ls_exec(ch);
}

/*
 * Ship a chunk of WAL (len bytes starting at WAL position start_lsn) to the
 * daemon, tagged with this process's timeline.  Used by the archive module to
 * stream completed WAL segments into the store.  len must be <= PS_IO_UNIT.
 */
void
pagestore_localsvc_wal_append(uint64 start_lsn, const void *data, uint32 len)
{
	PsChannel  *ch = ls_chan();

	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("WAL append");

	ch->timeline = (uint32) localsvc_timeline;
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	memcpy(ch->data, data, len);
	ls_exec(ch);
}

/* End of the WAL prefix durably shipped for this compute's timeline. */
uint64
pagestore_localsvc_wal_end(void)
{
	return pagestore_localsvc_wal_end_timeout(0);
}

uint64
pagestore_localsvc_wal_end_timeout(int timeout_ms)
{
	PsChannel  *ch = ls_chan();

	ch->timeline = (uint32) localsvc_timeline;
	ch->opcode = PS_OP_WAL_SIZE;
	ls_exec_timeout(ch, timeout_ms);
	return ch->req_lsn;
}

/*
 * Read up to 'len' bytes of shipped WAL starting at 'start_lsn' from a SPECIFIC
 * timeline's log on the store (not necessarily this backend's).  Used to serve a
 * branch's ancestor WAL records for cross-branch redo.  Returns the byte count.
 */
int
pagestore_localsvc_wal_read(uint32 timeline, uint64 start_lsn, uint32 len,
							void *out)
{
	PsChannel  *ch = ls_chan();
	int			n;

	ch->timeline = timeline;
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	ls_exec(ch);
	n = (int) ch->result;
	if (n > (int) len)
		n = (int) len;
	memcpy(out, ch->data, (size_t) n);
	return n;
}

/* This backend's current timeline (0 = main, >0 = a branch). */
uint32
pagestore_localsvc_timeline(void)
{
	return (uint32) localsvc_timeline;
}

/* The pinned read horizon (pagestore.read_lsn), 0 when this is a writer. */
/*
 * As-of size/existence queries for SQL-level tests: NBLOCKS/EXISTS with an
 * explicit horizon instead of this backend's own pin.
 */
uint64
pagestore_localsvc_nblocks_asof(const PageStoreRelKey *key, uint64 lsn)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_NBLOCKS;
	ch->req_lsn = lsn;
	ls_exec(ch);
	return ch->result;
}

int
pagestore_localsvc_exists_asof(const PageStoreRelKey *key, uint64 lsn)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_EXISTS;
	ch->req_lsn = lsn;
	ls_exec(ch);
	return ch->result != 0;
}

uint64
pagestore_localsvc_read_lsn(void)
{
	return localsvc_read_lsn;
}

uint32
pagestore_localsvc_read_epoch(void)
{
	return localsvc_read_epoch;
}

void
pagestore_localsvc_adopt_read_view(uint64 read_lsn, uint64 read_seq,
								   uint32 read_epoch)
{
	Assert(localsvc_read_lsn != 0);
	Assert(read_lsn >= localsvc_read_lsn);
	Assert(read_seq != 0);
	Assert(read_epoch != 0);

	localsvc_read_lsn = read_lsn;
	localsvc_read_seq = read_seq;
	localsvc_read_seq_loaded = true;
	localsvc_read_epoch = read_epoch;
}

/* Record in the store that the WAL record at 'lsn' modifies (key, block). */
void
pagestore_localsvc_walidx_add(const PageStoreRelKey *key, BlockNumber block,
							  uint64 lsn)
{
	PsChannel  *ch = ls_chan_for_key(key);

	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("WAL index append");

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_WAL_INDEX_ADD;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ls_exec(ch);
}

void
pagestore_localsvc_walidx_add_batch(const PageStoreWalIndexEntry *entries,
									int nentries)
{
	PsWalIndexEntry *wire;
	uint32		nshards;

	if (nentries <= 0)
		return;
	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("WAL index append");
	ls_attach();
	nshards = ls_nshards;
	wire = palloc(PS_IO_UNIT);
	for (uint32 shard = 0; shard < nshards; shard++)
	{
		int			count = 0;
		PsChannel  *ch;

		for (int i = 0; i < nentries; i++)
		{
			PsKey		key;

			key.spcOid = entries[i].key.spcOid;
			key.dbOid = entries[i].key.dbOid;
			key.relNumber = entries[i].key.relNumber;
			key.forkNum = entries[i].key.forkNum;
			key.klass = PS_KLASS_RELATION;
			if (ps_key_shard(&key, nshards) != shard)
				continue;
			if ((count + 1) * sizeof(PsWalIndexEntry) > PS_IO_UNIT)
				elog(ERROR, "pagestore WAL index batch is too large");
			wire[count].key = key;
			wire[count].block = entries[i].block;
			wire[count].flags = entries[i].flags;
			wire[count].lsn = entries[i].lsn;
			wire[count].end_lsn = entries[i].end_lsn;
			count++;
		}
		if (count == 0)
			continue;
		ls_claim_channel(shard);
		ch = ps_channel(ls_shm, ls_channel);
		ch->key = wire[0].key;
		ch->timeline = (uint32) localsvc_timeline;
		ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
		ch->nblocks = count;
		ch->datalen = count * sizeof(PsWalIndexEntry);
		memcpy(ch->data, wire, ch->datalen);
		ls_exec(ch);
	}
	pfree(wire);
}

/* Number of indexed WAL records that modify (key, block) on this timeline. */
int
pagestore_localsvc_walidx_count(const PageStoreRelKey *key, BlockNumber block)
{
	PsWalRec  *recs;
	int			n = pagestore_localsvc_walidx_get(key, block, PG_UINT64_MAX, &recs);

	pfree(recs);
	return n;
}

/* Fetch every record LSN <= lsn_max in ascending (LSN,timeline) order. */
int
pagestore_localsvc_walidx_get(const PageStoreRelKey *key, BlockNumber block,
							  uint64 lsn_max, PsWalRec **out)
{
	const int	batch_max = PS_IO_UNIT / sizeof(PsWalRec);
	PsWalRec  *result = palloc(sizeof(*result));
	int			nresult = 0;
	int			capacity = 1;

	for (;;)
	{
		PsChannel  *ch = ls_chan_for_key(key);
		int			n;

		ls_fill_key(ch, key);
		ch->opcode = PS_OP_WAL_INDEX_GET;
		ch->blocknum = block;
		ch->nblocks = batch_max;
		ch->req_lsn = lsn_max;
		ch->pad1 = 0;
		ch->req_seq = 0;
		ch->parent_timeline = 0;
		if (nresult > 0)
		{
			ch->pad1 = 1;
			ch->req_seq = result[nresult - 1].lsn;
			ch->parent_timeline = result[nresult - 1].timeline;
		}
		ls_exec(ch);
		n = (int) ch->result;
		if (n > 0)
		{
			int			required;

			if (nresult > PG_INT32_MAX - n)
				elog(ERROR, "pagestore WAL index result is too large");
			required = nresult + n;
			if (required > capacity)
			{
				while (capacity < required)
				{
					if (capacity > PG_INT32_MAX / 2)
					{
						capacity = required;
						break;
					}
					capacity *= 2;
				}
				result = repalloc_array(result, PsWalRec, capacity);
			}
			memcpy(&result[nresult], ch->data, (size_t) n * sizeof(*result));
			nresult = required;
		}
		if (n < batch_max)
			break;
	}
	*out = result;
	return nresult;
}

uint64
pagestore_localsvc_walidx_progress(void)
{
	PsChannel  *ch = ls_chan();

	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->timeline = (uint32) localsvc_timeline;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	ls_exec(ch);
	return ch->req_lsn;
}

void
pagestore_localsvc_walidx_commit(uint64 start_lsn, uint64 end_lsn)
{
	PsChannel  *ch = ls_chan();

	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("WAL index progress");
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->timeline = (uint32) localsvc_timeline;
	ch->req_lsn = start_lsn;
	ch->req_seq = end_lsn;
	ls_exec(ch);
}

/* Return this timeline's parent and fork point, or false for the root. */
bool
pagestore_localsvc_timeline_parent(uint32 timeline, uint32 *parent_timeline,
								   uint64 *branch_lsn)
{
	return pagestore_localsvc_timeline_parent_timeout(timeline,
													 parent_timeline, branch_lsn, 0);
}

bool
pagestore_localsvc_timeline_parent_timeout(uint32 timeline,
										  uint32 *parent_timeline,
										  uint64 *branch_lsn,
										  int timeout_ms)
{
	PsChannel  *ch = ls_chan();

	ch->opcode = PS_OP_TIMELINE_INFO;
	ch->timeline = timeline;
	ls_exec_timeout(ch, timeout_ms);
	if (ch->result == 0)
		return false;
	*parent_timeline = ch->parent_timeline;
	*branch_lsn = ch->req_lsn;
	return true;
}

uint8
pagestore_localsvc_begin_delete(uint32 timeline, uint64 expected_incarnation)
{
	PsChannel  *ch = ls_chan();

	ch->opcode = PS_OP_BEGIN_DELETE;
	ch->timeline = timeline;
	ch->req_seq = expected_incarnation;
	return ls_exec_wait(ch, 0);
}

bool
pagestore_localsvc_timeline_state(uint32 timeline, uint32 *state,
								  uint64 *incarnation)
{
	PsChannel  *ch = ls_chan();

	ch->opcode = PS_OP_TIMELINE_STATE;
	ch->timeline = timeline;
	if (ls_exec_wait(ch, 0) != PS_STATUS_OK)
		return false;
	if (state != NULL)
		*state = ch->result;
	if (incarnation != NULL)
		*incarnation = ch->req_seq;
	return true;
}

/*
 * Non-relation object I/O.  The store is keyed by the full PsKey including klass,
 * so a non-relation object (SLRU page, control state, ...) goes through the same
 * CREATE/EXTEND/WRITEV/READV path as a relation page -- only key.klass differs.
 * These take the four identity fields in a PageStoreRelKey (reinterpreted per the
 * class) plus the class explicitly.  ls_fill_key sets klass = RELATION, so we
 * override it after.
 */
void
pagestore_localsvc_obj_write(uint32 klass, const PageStoreRelKey *key,
							 BlockNumber block, const void *page, uint64 version)
{
	pagestore_localsvc_obj_write_timeout(klass, key, block, page, version, 0);
}

/*
 * Like pagestore_localsvc_obj_write, but each mailbox wait is bounded by
 * timeout_ms (0 = wait forever): callers on paths that must not hang on a
 * wedged daemon -- the control-mirror ship points -- turn a stall into an
 * error they can defer and retry.
 */
void
pagestore_localsvc_obj_write_timeout(uint32 klass, const PageStoreRelKey *key,
									 BlockNumber block, const void *page,
									 uint64 version, int timeout_ms)
{
	BlockNumber nb;

	nb = pagestore_localsvc_obj_write_prepare_timeout(klass, key, timeout_ms);
	pagestore_localsvc_obj_write_post_timeout(klass, key, block, page,
											  version, nb, timeout_ms);
}

/*
 * Split-phase variant of obj_write for callers that must know exactly when
 * their bytes can be in flight to the daemon: the preliminary CREATE and
 * NBLOCKS carry no caller data, so a timeout there cannot result in the
 * image being applied late.  Only the WRITEV/EXTEND issued by _post puts
 * the page bytes in flight.  The control mirror freezes its queued image
 * between the two phases.  Returns the object's current block count, to be
 * passed to _post.
 */
BlockNumber
pagestore_localsvc_obj_write_prepare_timeout(uint32 klass,
											 const PageStoreRelKey *key,
											 int timeout_ms)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);

	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("object write");

	/* ensure the object's fork exists (tolerate an existing one) */
	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = 1;
	ls_exec_timeout(ch, timeout_ms);

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_NBLOCKS;
	ls_exec_timeout(ch, timeout_ms);
	return (BlockNumber) ch->result;
}

uint64
pagestore_localsvc_obj_write_post_timeout(uint32 klass,
										  const PageStoreRelKey *key,
										  BlockNumber block, const void *page,
										  uint64 version, BlockNumber nb,
										  int timeout_ms)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);

	if (localsvc_read_lsn != 0)
		ls_reject_pinned_write("object write");

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	/* overwrite if the block already exists, else append */
	ch->opcode = (block < nb) ? PS_OP_WRITEV : PS_OP_EXTEND;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->skip_fsync = 0;
	/*
	 * For an SLRU- or control-class object the daemon versions the write by
	 * req_lsn (the caller's cutoff/dirtying/update LSN), so it reads back
	 * as-of an LSN >= that version; ignored for relations (pd_lsn).
	 */
	ch->req_lsn = version;
	memcpy(ch->data, page, BLCKSZ);
	ls_exec_timeout(ch, timeout_ms);
	return ch->req_seq;
}

void
pagestore_localsvc_obj_read(uint32 klass, const PageStoreRelKey *key,
							BlockNumber block, void *page)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->req_lsn = 0;			/* explicit: channels are reused across op kinds */
	ls_exec(ch);
	memcpy(page, ch->data, BLCKSZ);
}

/*
 * As-of read of a non-relation object block (honours branch ancestry).  Returns
 * true if a version <= 'version' was found; on a miss returns false and leaves
 * 'page' zero-filled (so callers can fail closed rather than read a phantom page).
 * If 'resolved' is non-NULL, it receives the version actually resolved (the newest
 * <= 'version'), so a caller wanting an exact-cutoff hit can compare it.
 */
bool
pagestore_localsvc_obj_read_at(uint32 klass, const PageStoreRelKey *key,
							   BlockNumber block, uint64 version, void *page,
							   uint64 *resolved)
{
	return pagestore_localsvc_obj_read_at_timeout(klass, key, block, version,
												 page, resolved, 0);
}

/* Bounded-wait variant; see pagestore_localsvc_obj_write_timeout. */
bool
pagestore_localsvc_obj_read_at_timeout(uint32 klass, const PageStoreRelKey *key,
									   BlockNumber block, uint64 version,
									   void *page, uint64 *resolved,
									   int timeout_ms)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);
	bool		found;

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = version;
	ls_exec_timeout(ch, timeout_ms);
	memcpy(page, ch->data, BLCKSZ);
	found = ch->result != 0;
	if (resolved)
		*resolved = found ? ch->req_lsn : 0;		/* daemon wrote the resolved ver */
	return found;
}

/* Called from _PG_init to register the GUCs owned by this backend. */
void
pagestore_localsvc_init(void)
{
	DefineCustomStringVariable("pagestore.localsvc_shm",
							   "Name of the POSIX shared-memory object shared with the pagestore daemon.",
							   NULL,
							   &localsvc_shm_name,
							   "/pagestore",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("pagestore.read_lsn",
							   "Cap every store relation page read at this LSN and refuse store writes.",
							   "The pinned-reader MECHANISM increment (READ_CONSISTENCY_DESIGN.md): "
							   "page reads freeze at R, mutations are refused.  A fully R-consistent "
							   "query compute additionally needs as-of local artifacts (catalogs, "
							   "control, SLRUs -- the prepared-branch flow), as-of size/existence "
							   "metadata, and the running-xacts snapshot; see the design doc's "
							   "increments.  Use the redo pointer of a durably mirrored checkpoint.  "
							   "Empty = a normal writer compute.",
							   &localsvc_read_lsn_str,
							   "",
							   PGC_POSTMASTER,
							   0,
							   ls_check_read_lsn, ls_assign_read_lsn, NULL);

	DefineCustomIntVariable("pagestore.timeline",
							"Timeline (branch) this backend reads and writes on; 0 is the main timeline.",
							NULL,
							&localsvc_timeline,
							0,
							0, 1023,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
}

/*
 * Pinned-reader command gates.  The read-only transaction default is
 * advisory (any session can SET TRANSACTION READ WRITE), and read-only
 * transactions legitimately admit VACUUM and REINDEX -- both of which
 * generate page-content WAL from paths the page-maintenance suppression
 * does not cover.  Refuse at the executor and utility entry points, before
 * any buffer is dirtied or WAL inserted.
 */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void
ls_pinned_executor_start(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *ps = queryDesc->plannedstmt;

	/* plan-only runs (plain EXPLAIN) execute nothing; upstream skips its
	 * read-only checks for them too */
	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
	{
		if (queryDesc->operation != CMD_SELECT || ps->hasModifyingCTE)
			ereport(ERROR,
					(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
					 errmsg("data modification is not allowed on a pinned reader (pagestore.read_lsn)")));
		if (ps->rowMarks != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
					 errmsg("row locking is not allowed on a pinned reader (pagestore.read_lsn)")));

		/*
		 * A CMD_SELECT feeding an into-rel receiver is CREATE TABLE AS /
		 * SELECT INTO in disguise (e.g. under EXPLAIN ANALYZE, where the
		 * utility gate sees only T_ExplainStmt): it creates and fills a
		 * table.  Refuse by destination, whatever the wrapping.
		 */
		if (queryDesc->dest && queryDesc->dest->mydest == DestIntoRel)
			ereport(ERROR,
					(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
					 errmsg("CREATE TABLE AS is not allowed on a pinned reader (pagestore.read_lsn)")));
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
ls_pinned_process_utility(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree, ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	const char *deny = NULL;

	switch (nodeTag(pstmt->utilityStmt))
	{
		case T_VacuumStmt:		/* VACUUM and ANALYZE */
			deny = "VACUUM/ANALYZE";
			break;
		case T_ReindexStmt:
			deny = "REINDEX";
			break;
		case T_RepackStmt:		/* REPACK (and its CLUSTER/VACUUM FULL legacy forms) */
			deny = "REPACK";
			break;
		case T_CopyStmt:
			if (((CopyStmt *) pstmt->utilityStmt)->is_from)
				deny = "COPY FROM";
			break;
		case T_TransactionStmt:
			/*
			 * PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED are
			 * read-only-legal but write XACT WAL inside critical sections
			 * (twophase.c); refuse them before that becomes a PANIC at the
			 * wal_insert_restricted backstop.  Plain BEGIN/COMMIT/etc. pass.
			 */
			switch (((TransactionStmt *) pstmt->utilityStmt)->kind)
			{
				case TRANS_STMT_PREPARE:
					deny = "PREPARE TRANSACTION";
					break;
				case TRANS_STMT_COMMIT_PREPARED:
					deny = "COMMIT PREPARED";
					break;
				case TRANS_STMT_ROLLBACK_PREPARED:
					deny = "ROLLBACK PREPARED";
					break;
				default:
					break;
			}
			break;
		case T_CheckPointStmt:
			/*
			 * ExecCheckpoint would wake the checkpointer with
			 * CHECKPOINT_FORCE -- and the checkpointer is exactly the
			 * process wal_insert_restricted must exempt, so a manual
			 * CHECKPOINT would insert private WAL the pin exists to avoid.
			 */
			deny = "CHECKPOINT";
			break;
		case T_ExplainStmt:
			/*
			 * EXPLAIN ANALYZE of CREATE TABLE AS / SELECT INTO executes the
			 * creation; the executor gate also refuses the into-rel
			 * destination, but refuse the statement up front too.
			 */
			{
				Query	   *q = castNode(Query,
										 ((ExplainStmt *) pstmt->utilityStmt)->query);

				if (q->commandType == CMD_UTILITY && q->utilityStmt &&
					IsA(q->utilityStmt, CreateTableAsStmt))
					deny = "EXPLAIN of CREATE TABLE AS / SELECT INTO";
			}
			break;
		case T_NotifyStmt:
			/*
			 * Read-only-legal but XID-assigning and pg_notify-SLRU-writing;
			 * the GetNewTransactionId refusal would catch it at pre-commit,
			 * but refuse it up front with the clearer error.
			 */
			deny = "NOTIFY";
			break;
		default:
			break;
	}
	if (deny)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("%s is not allowed on a pinned reader (pagestore.read_lsn)", deny)));

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
							params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
}

/*
 * Apply the pinned-reader (pagestore.read_lsn) instance-wide side effects.
 * Called from _PG_init AFTER pagestore.backend is final: a pin without the
 * localsvc backend would force the server read-only while capping nothing
 * (passthrough/md reads ignore the horizon), so that combination is refused
 * outright.
 */
void
pagestore_localsvc_pinned_init(bool localsvc_active)
{
	if (localsvc_read_lsn == 0)
		return;
	if (!localsvc_active)
		ereport(ERROR,
				(errmsg("pagestore.read_lsn requires pagestore.backend = 'localsvc'")));

	/*
	 * A pinned reader must not archive WAL, full stop.  Its local WAL
	 * diverges from the timeline writer's, and no archiver-side heuristic
	 * can tell, after the pin is lifted, which retained bytes were private
	 * (a floor recorded at first archive call starts too late after
	 * archiver lag; a mixed segment's private suffix ships as gap-fill
	 * that the store's overlap check has no coverage to refuse).  Refuse
	 * the combination at startup: the operator disables archive_mode for
	 * the pinned run, and on an unpinned restart of the SAME cluster the
	 * completed segments ship as one true chain, gapless.
	 */
	if (XLogArchiveMode != ARCHIVE_MODE_OFF)
		ereport(ERROR,
				(errmsg("a pinned reader (pagestore.read_lsn) must not archive WAL; set archive_mode = off")));

	/*
	 * A pinned reader serves history and must not generate page-content
	 * WAL: replaying that WAL after the pin is lifted would republish stale
	 * as-of-R page images above the writer's shipped versions.  The layers,
	 * outermost first:
	 *
	 * - transaction_read_only_forced makes XactReadOnly non-overridable
	 *   (SET TRANSACTION READ WRITE is refused the way it is during
	 *   recovery), so every PreventCommandIfReadOnly-guarded path -- DML,
	 *   DDL, TRUNCATE, nextval(), COPY FROM -- holds, exactly the hot
	 *   standby enforcement model;
	 * - executor/utility gates additionally refuse DML, row locking,
	 *   VACUUM/ANALYZE, REINDEX, REPACK and COPY FROM at the entry points
	 *   -- VACUUM and REINDEX are legal in read-only transactions, and the
	 *   rest get a clearer pinned-reader error before the generic
	 *   read-only one;
	 * - page-maintenance suppression stops the WAL a plain SELECT emits
	 *   (hint-bit FPIs, on-access pruning) and shuts down autovacuum
	 *   entirely, including the wraparound-defense launcher that ignores
	 *   autovacuum = off;
	 * - wal_insert_restricted refuses WAL insertion from anything but the
	 *   WAL-essential auxiliary processes, so even a direct C-level bypass
	 *   of the gates above cannot plant replayable page-content WAL;
	 * - the localsvc store-write refusals remain underneath as the final
	 *   fail-closed backstop.
	 */
	transaction_read_only_forced = true;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = ls_pinned_executor_start;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = ls_pinned_process_utility;
	page_maintenance_suppressed = true;
	wal_insert_restricted = true;
	SetConfigOption("autovacuum", "off", PGC_POSTMASTER, PGC_S_OVERRIDE);
	SetConfigOption("default_transaction_read_only", "on",
					PGC_POSTMASTER, PGC_S_OVERRIDE);
}
