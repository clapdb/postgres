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

/* per-backend attachment state */
static void *ls_shm = NULL;
static int	ls_shm_fd = -1;
static int	ls_channel = -1;

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

	/*
	 * Claim a free channel with an atomic compare-and-swap on its 'claimed'
	 * flag.  This is the only contended operation between backends; once a
	 * channel is claimed it is owned exclusively until ls_detach() releases it,
	 * so all subsequent mailbox traffic on it is single-producer/single-consumer.
	 */
	for (uint32 i = 0; i < hdr->nchannels; i++)
	{
		PsChannel  *ch = ps_channel(shm, i);

		if (ps_cas(&ch->claimed, 0, 1))
		{
			ls_channel = (int) i;
			break;
		}
	}
	if (ls_channel < 0)
	{
		munmap(shm, PS_SHM_SIZE);
		close(fd);
		ereport(ERROR,
				(errmsg("pagestore localsvc: no free channel (max %d)",
						PS_MAX_CHANNELS)));
	}

	ls_shm = shm;
	ls_shm_fd = fd;
	on_proc_exit(ls_detach, 0);
}

static PsChannel *
ls_chan(void)
{
	ls_attach();
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
	ch->key.spcOid = key->spcOid;
	ch->key.dbOid = key->dbOid;
	ch->key.relNumber = key->relNumber;
	ch->key.forkNum = key->forkNum;
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
	PsChannel  *ch = ls_chan();

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = isRedo ? 1 : 0;
	ls_exec(ch);
}

/* Does the fork exist in the store? */
static bool
ls_fork_exists(const PageStoreRelKey *key, void *localreln)
{
	PsChannel  *ch = ls_chan();

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_EXISTS;
	ls_exec(ch);
	return ch->result != 0;
}

/* Remove the fork entirely. */
static void
ls_unlink(const PageStoreRelKey *key, bool isRedo)
{
	PsChannel  *ch = ls_chan();

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_UNLINK;
	ch->is_redo = isRedo ? 1 : 0;
	ls_exec(ch);
}

/* Current size of the fork, in blocks. */
static BlockNumber
ls_nblocks(const PageStoreRelKey *key, void *localreln)
{
	PsChannel  *ch = ls_chan();

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
	PsChannel  *ch = ls_chan();

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
	PsChannel  *ch = ls_chan();
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
	PsChannel  *ch = ls_chan();
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
	PsChannel  *ch = ls_chan();

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
	PsChannel  *ch = ls_chan();

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
	PsChannel  *ch = ls_chan();

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_IMMEDSYNC;
	ls_exec(ch);
}

static bool
ls_fetch_to_fd(const PageStoreRelKey *key, BlockNumber blocknum,
			   BlockNumber nblocks, int *out_fd, uint64 *out_offset)
{
	PsChannel  *ch = ls_chan();

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
	PsChannel  *ch = ls_chan();

	ls_fill_key(ch, key);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = blocknum;
	ch->nblocks = 1;
	ch->req_lsn = lsn;
	ls_exec(ch);
	memcpy(out, ch->data, BLCKSZ);
}

/* Called from _PG_init to register the GUC owned by this backend. */
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
}
