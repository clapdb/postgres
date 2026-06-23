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
#define PS_SHM_VERSION		5

/* Default logical page size (overridable via the daemon's --page-size). */
#define PS_DEFAULT_PAGE_SIZE	8192

/*
 * Transfer / I/O unit: the size of each channel's data buffer and the largest
 * amount of page data moved in one request.  Must be >= any supported page
 * size and a multiple of it.  This is the unit the transport/SPDK layer cares
 * about, independent of the engine's logical page size.
 */
#define PS_IO_UNIT			(256 * 1024)

/* Geometry */
#define PS_MAX_CHANNELS		128

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
} PsOpcode;

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

/* Shared hash helper for key routing.  FNV-1a over bytes keeps this cheap and
 * stable enough for shard selection, and it is reused for client+daemon key->shard.
 */
static inline uint32_t
ps_fnv1a32(const void *data, size_t n)
{
	const unsigned char *p = (const unsigned char *) data;
	uint32_t		h = 2166136261u;

	for (size_t i = 0; i < n; i++)
	{
		h ^= p[i];
		h *= 16777619u;
	}
	return h;
}

/* Shard this key belongs to; timeline/block must not be involved in routing so
 * READV/WRITEV stay on one shard for a single relation.  nshards=0 means 1.
 */
static inline uint32_t
ps_key_shard(const PsKey *key, uint32_t nshards)
{
	if (nshards <= 1)
		return 0;
	return ps_fnv1a32(key, sizeof(*key)) % nshards;
}

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
	uint32_t	pad1;
	uint64_t	req_lsn;		/* READ_AT/WAL_APPEND: LSN; WAL_SIZE: out end LSN */
	PsKey		key;

	/* result */
	uint32_t	status;
	uint32_t	result;			/* NBLOCKS -> count; EXISTS -> 0/1 */
	uint32_t	shard;			/* key-owner shard for this request */

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
	uint32_t	nshards;		/* channel pools = channel index mod nshards */
	uint32_t	pad0;			/* reserved */
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
