/*-------------------------------------------------------------------------
 *
 * pagestore_ipc.h
 *	  Shared-memory IPC protocol between the PostgreSQL backend (localsvc
 *	  backend) and the standalone pagestore daemon.
 *
 * Included by BOTH the PG-side module (with PG headers) and the standalone
 * daemon (without them), so it uses only plain fixed-width C types and defines
 * an identical layout on both sides.  Synchronization uses __atomic builtins on
 * plain uint32 fields.
 *
 * Page-size independence: nothing here hardcodes the engine page size.  The
 * channel data buffer is sized in terms of PS_IO_UNIT -- the transfer/IO unit,
 * deliberately decoupled from the logical page size so that small engine pages
 * (PostgreSQL 8K, InnoDB 16K, ...) never become small network/SPDK transfers.
 * The actual page_size is carried in the shm header and validated at attach.
 *
 * Concurrency: the smgr shim is synchronous (one I/O per backend at a time), so
 * each backend owns one channel with a single request/response mailbox.
 *
 * src/../contrib/pagestore/pagestore_ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_IPC_H
#define PAGESTORE_IPC_H

#include <stddef.h>
#include <stdint.h>

#define PS_SHM_MAGIC		0x50414753	/* "PAGS" */
#define PS_SHM_VERSION		8			/* 8: BLOCK_LIVE opcode */

/* Default logical page size (overridable via the daemon's --page-size). */
#define PS_DEFAULT_PAGE_SIZE	8192

/*
 * Transfer / I/O unit: the size of each channel's data buffer and the largest
 * amount of page data moved in one request.  Must be >= any supported page
 * size and a multiple of it.  This is the unit the transport/SPDK layer cares
 * about, independent of the engine's logical page size.
 */
#define PS_IO_UNIT			(256 * 1024)

/* result_flags bits.  WAL_INDEX_GET: the (timeline,lsn) chain had more records
 * than fit in one response -- the returned set is a prefix, not the whole chain;
 * the caller must paginate or treat it as an error rather than a complete list. */
#define PS_WALIDX_OVERFLOW	0x1u

/* WAL_INDEX_GET payload: lsns[] (uint64) at data[0], then the parallel source
 * timelines tls[] (uint32) at data[cap*8]; cap = how many pairs fit. */
#define PS_WALIDX_CAP		((int) (PS_IO_UNIT / (sizeof(uint64_t) + sizeof(uint32_t))))

/* Geometry */
#define PS_MAX_CHANNELS		128

/*
 * Sharding (LSM phase 9).  The daemon owns one Shard per core; the actual count
 * is chosen at startup (--nshards, default 1) and published in the shm header
 * (PsShmHeader.nshards) so clients route the same way.  PS_MAX_SHARDS only caps
 * the compile-time array sizing on both sides; the live count is runtime.  The
 * channel array is partitioned into nshards contiguous pools (see
 * ps_shard_channel_range), and a request for 'key' goes to ps_shard_for_key()'s
 * pool.  nshards == 1 is identical to the old single-owner daemon.
 */
#define PS_MAX_SHARDS		64

/* Each shard owns a non-empty channel pool, so there can't be more shards than
 * channels (see ps_shard_channel_range). */
_Static_assert(PS_MAX_SHARDS >= 1 && PS_MAX_SHARDS <= PS_MAX_CHANNELS,
			   "PS_MAX_SHARDS must be in [1, PS_MAX_CHANNELS]");

/* Mailbox state (atomic uint32) */
#define PS_STATE_IDLE		0
#define PS_STATE_REQUEST	1
#define PS_STATE_DONE		2

/* Operation codes */
typedef enum PsOpcode
{
	PS_OP_NONE = 0,
	PS_OP_CREATE,
	PS_OP_EXISTS,
	PS_OP_UNLINK,
	PS_OP_NBLOCKS,
	PS_OP_TRUNCATE,
	PS_OP_EXTEND,				/* write one page at blocknum (grow) */
	PS_OP_ZEROEXTEND,			/* grow by nblocks zero pages at blocknum */
	PS_OP_WRITEV,				/* write nblocks pages at blocknum from data */
	PS_OP_READV,				/* read nblocks pages at blocknum into data */
	PS_OP_IMMEDSYNC,
	PS_OP_READ_AT,				/* read 1 page at blocknum as-of req_lsn (COW) */
	PS_OP_CREATE_BRANCH,		/* create timeline from parent_timeline @ req_lsn */
	PS_OP_WAL_APPEND,			/* append datalen WAL bytes at LSN req_lsn (timeline) */
	PS_OP_WAL_SIZE,				/* return end LSN of the timeline's WAL in req_lsn */
	PS_OP_WAL_READ,				/* read datalen WAL bytes from LSN req_lsn into data */
	PS_OP_WAL_INDEX_ADD,		/* record: WAL at req_lsn modifies (key, blocknum) */
	PS_OP_WAL_INDEX_GET,		/* list record LSNs <= req_lsn for (key, blocknum) */
	PS_OP_FORK_SIZE_ADD,		/* record: fork truncated to nblocks as-of req_lsn */
	PS_OP_FORK_SIZE_AT,			/* truncation floor (blocks) as-of req_lsn; PS_FORKSIZE_UNKNOWN if never truncated */
	PS_OP_BLOCK_LIVE,			/* is (key, blocknum) live as-of req_lsn? result 0/1 (redo step-0) */
} PsOpcode;

/* PS_OP_FORK_SIZE_AT: no truncation at/below the queried LSN. */
#define PS_FORKSIZE_UNKNOWN	0xffffffffu

/* Status codes */
#define PS_STATUS_OK		0
#define PS_STATUS_ERROR		1

/* Version-neutral relation-fork identity (mirrors PageStoreRelKey). */
typedef struct PsKey
{
	uint32_t	spcOid;
	uint32_t	dbOid;
	uint32_t	relNumber;
	int32_t		forkNum;
} PsKey;

/* One channel = one backend's mailbox. */
typedef struct PsChannel
{
	uint32_t	claimed;		/* atomic: 0 free, 1 owned */
	uint32_t	state;			/* atomic: PS_STATE_* */

	/* request */
	uint32_t	opcode;
	uint32_t	is_redo;
	uint32_t	skip_fsync;
	uint32_t	blocknum;
	uint32_t	nblocks;
	uint32_t	old_nblocks;
	uint32_t	timeline;		/* timeline this op targets (0 = main) */
	uint32_t	parent_timeline;	/* CREATE_BRANCH: parent timeline */
	uint32_t	datalen;		/* WAL_APPEND: number of WAL bytes in data[] */
	uint32_t	pad1;			/* keep req_lsn 8-byte aligned */
	uint64_t	req_lsn;		/* READ_AT/WAL_APPEND: LSN; WAL_SIZE: out end LSN */
	PsKey		key;

	/* result */
	uint32_t	status;
	uint32_t	result;			/* NBLOCKS -> count; EXISTS -> 0/1 */
	uint32_t	result_flags;	/* WAL_INDEX_GET -> PS_WALIDX_* (was pad1) */
	uint32_t	result_pad;		/* keep data[] 8-byte aligned for (uint64*)data */

	/* payload: up to PS_IO_UNIT bytes (io_unit / page_size pages) */
	unsigned char data[PS_IO_UNIT];
} PsChannel;

typedef struct PsShmHeader
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	page_size;		/* logical page size negotiated with engine */
	uint32_t	io_unit;		/* == PS_IO_UNIT */
	uint32_t	nchannels;
	uint32_t	nshards;		/* daemon's shard count; clients route to match */
	uint64_t	channel_stride;
	uint64_t	channels_off;
} PsShmHeader;

#define PS_CHANNELS_OFF		(((sizeof(PsShmHeader) + 63) / 64) * 64)
#define PS_CHANNEL_STRIDE	(((sizeof(PsChannel) + 63) / 64) * 64)
#define PS_SHM_SIZE			\
	(PS_CHANNELS_OFF + (uint64_t) PS_MAX_CHANNELS * PS_CHANNEL_STRIDE)

static inline PsChannel *
ps_channel(void *shm_base, uint32_t idx)
{
	return (PsChannel *) ((unsigned char *) shm_base + PS_CHANNELS_OFF +
						  (uint64_t) idx * PS_CHANNEL_STRIDE);
}

/* Byte offset of a channel's data[] region within the shm object. */
static inline uint64_t
ps_channel_data_offset(uint32_t idx)
{
	return PS_CHANNELS_OFF + (uint64_t) idx * PS_CHANNEL_STRIDE +
		offsetof(PsChannel, data);
}

/* --- sharding: key -> shard and the channel pool a shard owns ----------- */

/*
 * Which shard owns 'key'.  Block- and timeline-independent (per the sharding
 * design): every block and every timeline of a logical key live on one shard, so
 * only the four key fields feed the hash (FNV-1a).  nshards <= 1 -> shard 0.
 */
static inline uint32_t
ps_shard_for_key(const PsKey *key, uint32_t nshards)
{
	uint32_t	vals[4];
	uint32_t	h = 2166136261u;	/* FNV-1a offset basis, mixed per 32-bit word */

	if (nshards <= 1)
		return 0;
	vals[0] = key->spcOid;
	vals[1] = key->dbOid;
	vals[2] = key->relNumber;
	vals[3] = (uint32_t) key->forkNum;
	for (unsigned i = 0; i < 4; i++)
	{
		h = (h ^ vals[i]) * 16777619u;
	}
	return h % nshards;
}

/*
 * The contiguous channel pool shard 'shard' owns: *first channel index and
 * *count, partitioning [0, nchannels) into nshards near-equal ranges.  A client
 * claims a channel within ps_shard_for_key(key)'s pool so the request reaches the
 * shard that owns the key; at nshards == 1 the pool is the whole array.
 *
 * Proportional boundaries (shard*nchannels/nshards) spread any remainder evenly
 * instead of dumping it all on the last shard, so every shard's pool size (its
 * available concurrency) is within one of the others.  Requires
 * nshards <= nchannels so no pool is empty (PS_MAX_SHARDS <= PS_MAX_CHANNELS by
 * the _Static_assert above; the daemon validates --nshards at startup).
 */
static inline void
ps_shard_channel_range(uint32_t shard, uint32_t nshards, uint32_t nchannels,
					   uint32_t *first, uint32_t *count)
{
	uint32_t	end;

	if (nshards == 0)
		nshards = 1;
	*first = (uint32_t) ((uint64_t) shard * nchannels / nshards);
	end = (uint32_t) ((uint64_t) (shard + 1) * nchannels / nshards);
	*count = end - *first;
}

/* --- atomic helpers (acquire/release) ---------------------------------- */

static inline uint32_t
ps_load_acquire(volatile uint32_t *p)
{
	return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

static inline void
ps_store_release(volatile uint32_t *p, uint32_t v)
{
	__atomic_store_n(p, v, __ATOMIC_RELEASE);
}

static inline int
ps_cas(volatile uint32_t *p, uint32_t expected, uint32_t desired)
{
	return __atomic_compare_exchange_n(p, &expected, desired, 0,
									   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

#endif							/* PAGESTORE_IPC_H */
