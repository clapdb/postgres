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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "miscadmin.h"
#include "pagestore_backend.h"
#include "pagestore_ipc.h"
#include "storage/ipc.h"
#include "utils/guc.h"

/* GUC: name of the POSIX shm object shared with the daemon */
static char *localsvc_shm_name = NULL;

/* GUC: timeline this backend reads/writes on (0 = main; >0 = a branch) */
static int	localsvc_timeline = 0;

/* per-backend attachment state */
static void *ls_shm = NULL;
static int	ls_shm_fd = -1;
static int	ls_channel = -1;
static uint32	ls_channel_shard = UINT32_MAX;
static uint32	ls_nchannels = 0;
static uint32	ls_nshards = 1;

/* max logical pages that fit in one transfer (io_unit) for this engine */
#define LS_MAX_PAGES_PER_OP		(PS_IO_UNIT / BLCKSZ)

static void
ls_detach(int code, Datum arg)
{
	if (ls_shm != NULL)
	{
		if (ls_channel >= 0)
		{
			PsChannel  *ch = ps_channel(ls_shm, ls_channel);

			/* release the channel for reuse by a future backend */
			ps_store_release(&ch->claimed, 0);
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
		uint32		got_page_size = hdr->page_size;

		munmap(shm, PS_SHM_SIZE);
		close(fd);
		ereport(ERROR,
				(errmsg("pagestore localsvc shared memory incompatible"),
				 errdetail("daemon page_size=%u, this engine BLCKSZ=%d (magic=%#x version=%u)",
						   got_page_size, BLCKSZ, hdr->magic, hdr->version)));
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

		ps_store_release(&old->claimed, 0);
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

		if (ps_cas(&ch->claimed, 0, 1))
		{
			ch->shard = target;
			ls_channel = (int) i;
			ls_channel_shard = target;
			return;
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
 */
static void
ls_exec(PsChannel *ch)
{
	uint32		spins = 0;
	ch->shard = ls_channel_shard;

	ps_store_release(&ch->state, PS_STATE_REQUEST);

	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
	{
		if (((++spins) & 0xFFF) == 0)
			CHECK_FOR_INTERRUPTS();
#if defined(__x86_64__) || defined(__i386__)
		__builtin_ia32_pause();
#endif
	}

	if (ch->status != PS_STATUS_OK)
		ereport(ERROR,
				(errmsg("pagestore localsvc: daemon reported error for op %u",
						ch->opcode)));
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
 * Make the fork exist in the store (with zero blocks).  isRedo is set during
 * WAL replay, where re-creating an existing fork must be tolerated.
 */
static void
ls_create(const PageStoreRelKey *key, void *localreln, bool isRedo)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = isRedo ? 1 : 0;
	ls_exec(ch);
}

/* Does the fork exist in the store? */
static bool
ls_fork_exists(const PageStoreRelKey *key, void *localreln)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_EXISTS;
	ls_exec(ch);
	return ch->result != 0;
}

/* Remove the fork entirely. */
static void
ls_unlink(const PageStoreRelKey *key, bool isRedo)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_UNLINK;
	ch->is_redo = isRedo ? 1 : 0;
	ls_exec(ch);
}

/* Current size of the fork, in blocks. */
static BlockNumber
ls_nblocks(const PageStoreRelKey *key, void *localreln)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_NBLOCKS;
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

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_TRUNCATE;
	ch->old_nblocks = old_blocks;
	ch->nblocks = nblocks;
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
	PsChannel  *ch = ls_chan_for_key(key);
	BlockNumber done = 0;

	while (done < nblocks)
	{
		BlockNumber chunk = Min(nblocks - done, LS_MAX_PAGES_PER_OP);

		ls_fill_key(ch, key);
		ch->opcode = PS_OP_READV;
		ch->blocknum = blocknum + done;
		ch->nblocks = chunk;
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

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = blocknum;
	ch->nblocks = nblocks;
	ch->skip_fsync = skipFsync ? 1 : 0;
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
pagestore_localsvc_create_branch(uint32 new_tl, uint32 parent_tl,
								 uint64 branch_lsn)
{
	PsChannel  *ch = ls_chan();

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

	ch->timeline = (uint32) localsvc_timeline;
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = start_lsn;
	ch->datalen = len;
	memcpy(ch->data, data, len);
	ls_exec(ch);
}

/* Record in the store that the WAL record at 'lsn' modifies (key, block). */
void
pagestore_localsvc_walidx_add(const PageStoreRelKey *key, BlockNumber block,
							  uint64 lsn)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_WAL_INDEX_ADD;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	ls_exec(ch);
}

/* Number of indexed WAL records that modify (key, block) on this timeline. */
int
pagestore_localsvc_walidx_count(const PageStoreRelKey *key, BlockNumber block)
{
	PsChannel  *ch = ls_chan_for_key(key);

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_WAL_INDEX_GET;
	ch->blocknum = block;
	ch->req_lsn = PG_UINT64_MAX;	/* all records */
	ls_exec(ch);
	return (int) ch->result;
}

/* Fetch up to maxn record LSNs (<= lsn_max) for (key, block) into out;
 * returns how many.  Ascending order. */
int
pagestore_localsvc_walidx_get(const PageStoreRelKey *key, BlockNumber block,
							  uint64 lsn_max, PsWalRec *out, int maxn)
{
	PsChannel  *ch = ls_chan_for_key(key);
	int			n;

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_WAL_INDEX_GET;
	ch->blocknum = block;
	ch->req_lsn = lsn_max;
	ls_exec(ch);
	n = (int) ch->result;
	if (n > maxn)
		n = maxn;
	memcpy(out, ch->data, (size_t) n * sizeof(PsWalRec));
	return n;
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
							 BlockNumber block, const void *page)
{
	pagestore_localsvc_obj_write_lsn(klass, key, block, page, 0);
}

void
pagestore_localsvc_obj_write_lsn(uint32 klass, const PageStoreRelKey *key,
								 BlockNumber block, const void *page,
								 uint64 lsn)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);
	BlockNumber nb;

	/* ensure the object's fork exists (tolerate an existing one) */
	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = 1;
	ls_exec(ch);

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_NBLOCKS;
	ls_exec(ch);
	nb = (BlockNumber) ch->result;

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	/* overwrite if the block already exists, else append */
	ch->opcode = (block < nb) ? PS_OP_WRITEV : PS_OP_EXTEND;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->skip_fsync = 0;
	ch->req_lsn = lsn;
	memcpy(ch->data, page, BLCKSZ);
	ls_exec(ch);
}

void
pagestore_localsvc_obj_sync(uint32 klass, const PageStoreRelKey *key)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);

	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_IMMEDSYNC;
	ls_exec(ch);
}

bool
pagestore_localsvc_obj_read(uint32 klass, const PageStoreRelKey *key,
							BlockNumber block, void *page)
{
	PsChannel  *ch = ls_chan_for_key_klass(key, klass);

	/*
	 * Read the block as-of the latest LSN.  READ_AT reports in ch->result whether
	 * the block actually has a stored version, which is what we need: NBLOCKS
	 * alone would treat a sparse object block (below the fork length but never
	 * written -- e.g. an SLRU mirror that was skipped/swallowed) as present and
	 * serve a zero page, when the caller must fall back to its local copy instead.
	 */
	ls_fill_key(ch, key);
	ch->key.klass = klass;
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = UINT64_MAX;
	ls_exec(ch);
	if (ch->result == 0)
	{
		memset(page, 0, BLCKSZ);	/* no stored version -> not in the store */
		return false;
	}
	memcpy(page, ch->data, BLCKSZ);
	return true;
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
