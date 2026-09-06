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
#define PS_SHM_VERSION		42	/* 42: per-timeline inspection snapshots;
								 * 41: bounded runtime inspection snapshots;
							 * 40: forkmeta reclaim backpressure metrics;
								 * 39: WAL-index reclaim backpressure metrics;
								 * 38: request generations and daemon-owned
								 * shutdown cancellation state;
								 * 37: page/WAL reclaim backpressure controller metrics;
								 * 36: exact-token timeline info returns parent incarnation;
								 * 34: timeline deletion lifecycle/state query;
								 * 33: WAL-index known/FPI + record-end metadata;
								 * 32: coherent page-pruning metrics;
								 * 31: page-pruning metrics;
								 * 30: keyed retention lookup;
								 * 29: retention GET epoch/result payload;
								 * 28: exact retention admission sequences;
								 * 27: retention owner generations + stale status;
								 * 26: durable retention registry opcodes;
								 * 25: WAL_INDEX_GET cursor pagination;
								 * 24: WAL-index lag metrics added;
								 * 23: WAL_INDEX_ADD_BATCH opcode added;
								 * 22: TIMELINE_INFO opcode added;
								 * 21: READER_SNAPSHOT object class added;
								 * 20: WAL_INDEX_PROGRESS opcode added;
							 * 19: checkpoint admission gate + barrier;
								 * 18: req_seq caps same-LSN admission order;
								 *     writes return their admission sequence
								 * 17: NBLOCKS/EXISTS honour req_lsn as an
								 *     as-of horizon; fork-mutating ops carry
								 *     their WAL position in req_lsn;
								 * 16: PS_OP_READV honours req_lsn as a
								 *     read cap (pinned readers);
								 * 15: PS_KLASS_SLRU_WM watermark object;
								 * 14: PS_KLASS_SLRU_TOMB truncation
								 *     tombstones;
								 * 13: PS_KLASS_SLRU_LIVE (caller-versioned
								 *     live SLRU mirror images);
								 * 12: WAL_RETAIN_FLOOR opcode added;
								 * 11: PS_KLASS_CONTROL writes versioned by the
								 *     caller-supplied update LSN (was a daemon
								 *     max+1 counter); mixed binaries must fail
								 *     the shm handshake, not store wrong versions;
								 * 10: REQUIRE_BRANCH opcode added;
								 * 9: CHECK_BRANCH opcode added;
								 * 8: READ_AT reports found-ness in ch->result;
								 * 7: walidx_get returns timeline-tagged PsWalRec */

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
#define PS_STATE_CANCELLING	3	/* daemon owns a deferred request during shutdown */

/* Shared-memory lifecycle.  The daemon keeps the magic invalid until the
 * store has recovered and all request workers exist, so a restart cannot be
 * mistaken for a ready daemon by a stale-header health check. */
#define PS_SHM_STARTING		0
#define PS_SHM_READY		1
#define PS_SHM_STOPPING		2

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
	PS_OP_CHECK_BRANCH,			/* validate timeline creation request, no mutation */
	PS_OP_REQUIRE_BRANCH,		/* require existing timeline ancestry metadata */
	PS_OP_CREATE_BRANCH,		/* create timeline from parent_timeline @ req_lsn */
	PS_OP_TIMELINE_INFO,		/* return parent/fork metadata; result says has-parent */
	PS_OP_WAL_APPEND,			/* append datalen WAL bytes at LSN req_lsn (timeline) */
	PS_OP_WAL_SIZE,				/* return end LSN of the timeline's WAL in req_lsn */
	PS_OP_WAL_READ,				/* read datalen WAL bytes from LSN req_lsn into data */
	PS_OP_WAL_INDEX_ADD,		/* record: WAL at req_lsn modifies (key, blocknum) */
	PS_OP_WAL_INDEX_ADD_BATCH,	/* nblocks PsWalIndexEntry records in data[] */
	PS_OP_WAL_INDEX_GET,		/* list record LSNs <= req_lsn for (key, blocknum) */
	PS_OP_WAL_INDEX_PROGRESS,	/* req_lsn=start, req_seq=end; 0/0 reads end */
	PS_OP_WAL_RETAIN_FLOOR,		/* out req_lsn: durable WAL retention floor (timeline) */
	PS_OP_ADMISSION_BARRIER,	/* out req_seq: sequence after prior mutations */
	PS_OP_RETENTION_PIN_RESERVE, /* allocate fence and atomically install pin */
	PS_OP_RETENTION_PIN_SET,	/* durable set/update; fields described below */
	PS_OP_RETENTION_PIN_DROP,	/* durable idempotent drop by owner key */
	PS_OP_RETENTION_PIN_GET,	/* enumerate by blocknum; nblocks = total count */
	PS_OP_RETENTION_PIN_LOOKUP,	/* atomic lookup by timeline/kind/owner id */
	PS_OP_RETENTION_FLOOR,		/* effective floor for parent_timeline resource */
	PS_OP_BEGIN_DELETE,			/* durable LIVE -> DELETING transition */
	PS_OP_TIMELINE_STATE,		/* return lifecycle state/incarnation */
} PsOpcode;

typedef enum PsTimelineState
{
	PS_TIMELINE_LIVE = 1,
	PS_TIMELINE_DELETING = 2,
	PS_TIMELINE_DELETED = 3,
} PsTimelineState;

/* Status codes */
#define PS_STATUS_OK		0
#define PS_STATUS_ERROR		1
#define PS_STATUS_STALE		2	/* fenced by a newer owner generation */

/*
 * Object class of a stored key.  Almost everything is a relation fork page
 * (PS_KLASS_RELATION); the discriminator lets non-relation cluster state -- SLRU
 * banks (clog/multixact/...), the control file, etc. -- share the same key space,
 * indexes, segments, layers and cache, distinguished only by klass.  Relation
 * keys carry PS_KLASS_RELATION (0), so existing behavior is unchanged.  Future
 * classes reinterpret the spc/db/rel/fork fields as that class needs (e.g. an
 * SLRU encodes its bank/segment there).
 */
typedef enum PsObjClass
{
	PS_KLASS_RELATION = 0,		/* a relation fork's page (spc/db/rel/fork) */
	PS_KLASS_SLRU = 1,			/* an SLRU seed-snapshot page (clean as-of a
								 * proven cutoff; branch seeding reads these) */
	PS_KLASS_CONTROL = 2,		/* pg_control / cluster control state */
	PS_KLASS_SLRU_LIVE = 3,		/* a live-mirrored SLRU page image (newest
								 * flushed bytes, contents bounded by the
								 * version LSN; never a seed base) */
	PS_KLASS_SLRU_TOMB = 4,		/* an SLRU truncation tombstone: block 0
								 * carries the cutoff page number (int64),
								 * versioned by the truncation LSN; pages
								 * below the cutoff are dead at/after it */
	PS_KLASS_SLRU_WM = 5,		/* the SLRU mirror visibility watermark:
								 * block 0 of object 0, versioned by (and
								 * carrying) the watermark LSN; readers on
								 * other computes trust the live mirror up
								 * to the newest one and no further */
	PS_KLASS_READER_SNAPSHOT = 6, /* exact-R running-XID snapshot artifacts */
} PsObjClass;

/* Version-neutral object identity (relation forks: mirrors PageStoreRelKey). */
typedef struct PsKey
{
	uint32_t	spcOid;
	uint32_t	dbOid;
	uint32_t	relNumber;
	int32_t		forkNum;
	uint32_t	klass;			/* PsObjClass; 0 = relation (default) */
} PsKey;

/*
 * A per-page WAL-index record: one WAL record that touched the page, tagged with
 * the timeline it lives on.  walidx_get returns these in ascending LSN order,
 * merging a branch's records with its ancestors' (the timeline tag tells a reader
 * which timeline's shipped WAL to fetch each record from -- the cross-branch
 * store-served read path).
 */
typedef struct PsWalRec
{
	uint64_t	lsn;			/* record start LSN (ReadRecPtr) */
	uint64_t	end_lsn;		/* record end LSN; zero for legacy unknown */
	uint32_t	timeline;		/* source timeline the record lives on */
	uint32_t	flags;			/* PS_WAL_INDEX_FLAG_* */
} PsWalRec;

#define PS_WAL_INDEX_FLAG_KNOWN	(1u << 0)
#define PS_WAL_INDEX_FLAG_FPI	(1u << 1)
#define PS_WAL_INDEX_FLAG_MASK	(PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI)

typedef struct PsWalIndexEntry
{
	PsKey		key;
	uint32_t	block;
	uint32_t	flags;			/* PS_WAL_INDEX_FLAG_* */
	uint64_t	lsn;
	uint64_t	end_lsn;		/* record EndRecPtr; zero for legacy unknown */
} PsWalIndexEntry;

/* Retention owners are intentionally few: structural branch pins are derived
 * from timeline metadata instead of duplicated in this mutable registry. */
typedef enum PsRetentionOwnerKind
{
	PS_RETENTION_OWNER_READER = 1,
	PS_RETENTION_OWNER_MATERIALIZER = 2,
	PS_RETENTION_OWNER_CONFIGURED = 3,
} PsRetentionOwnerKind;

typedef enum PsRetentionResource
{
	PS_RETENTION_RESOURCE_PAGE_HISTORY = 1u << 0,
	PS_RETENTION_RESOURCE_WAL = 1u << 1,
	PS_RETENTION_RESOURCE_WAL_INDEX = 1u << 2,
	PS_RETENTION_RESOURCE_ALL = (1u << 3) - 1,
} PsRetentionResource;

/* Stable owner key = (timeline, owner_kind, owner_id); SET replaces its value
 * only at the current or a newer generation.  Controllers allocate owner_id
 * durably and increment generation on logical-owner takeover.  Generation 0
 * is reserved for records written by the version-26 protocol.
 *
 * IPC uses timeline/req_seq as the key, blocknum as owner_kind,
 * parent_timeline as resources, old_nblocks as generation, req_lsn as lsn,
 * and nblocks/pad1 as the low/high halves of admission_seq.  GET keeps
 * nblocks for the result count and returns admission_seq in data[0..7].
 */
typedef struct PsRetentionPin
{
	uint32_t	timeline;
	uint32_t	owner_kind;
	uint32_t	resources;
	uint32_t	generation;
	uint64_t	owner_id;
	uint64_t	lsn;
	uint64_t	admission_seq;
} PsRetentionPin;

/* RETENTION_PIN_GET returns the pin's fence and the registry mutation epoch.
 * A caller sends its prior epoch in req_lsn; PS_STATUS_STALE means it must
 * restart enumeration from index zero. */
typedef struct PsRetentionGetResult
{
	uint64_t	admission_seq;
	uint64_t	mutation_epoch;
} PsRetentionGetResult;

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
	uint32_t	nblocks;		/* WAL_INDEX_GET: optional result-page limit */
	uint32_t	old_nblocks;		/* RETENTION_PIN_{SET,DROP}: generation */
	uint32_t	timeline;		/* timeline this op targets (0 = main) */
	uint32_t	parent_timeline;	/* CREATE_BRANCH parent; WAL_INDEX_GET cursor timeline */
	uint32_t	datalen;		/* WAL_APPEND: number of WAL bytes in data[] */
	uint32_t	pad1;			/* WAL_INDEX_GET: cursor is present */
	uint64_t	req_lsn;		/* READ_AT/WAL_APPEND: LSN; WAL_SIZE: out end LSN */
	uint64_t	req_seq;		/* admission sequence; WAL_INDEX_GET cursor LSN */
	uint64_t	incarnation;	/* expected timeline incarnation; 0 = legacy inc-1 */
	uint64_t	request_generation;	/* monotonically advances for every published request */
	PsKey		key;

	/* result */
	uint32_t	status;
	uint32_t	result;			/* NBLOCKS -> count; EXISTS -> 0/1 */
	uint32_t	shard;			/* key-owner shard for this request */

	/* payload: up to PS_IO_UNIT bytes (io_unit / page_size pages) */
	unsigned char data[PS_IO_UNIT];
} PsChannel;

/* The generation is written before the release that publishes REQUEST and is
 * never reset when a channel is reclaimed.  A zero value is the initial
 * generation; wrapping skips zero so a stale zero-initialized channel cannot
 * alias a later request. */
static inline uint64_t
ps_request_generation_next(PsChannel *ch)
{
	uint64_t generation = ch->request_generation + 1;

	if (generation == 0)
		generation = 1;
	ch->request_generation = generation;
	return generation;
}

/* Block 2 of the mirrored control object maps checkpoint redo to the global
 * store-admission sequence that freezes same-LSN page/fork history. */
#define PS_ADMISSION_FENCE_MAGIC	0x434e4641 /* "AFNC" */
#define PS_ADMISSION_FENCE_VERSION	1

typedef struct PsAdmissionFence
{
	uint32_t	magic;
	uint32_t	version;
	uint64_t	redo_lsn;
	uint64_t	admission_seq;
} PsAdmissionFence;

/* A controller snapshot is copied under backpressure_metrics_seq.  The
 * fields themselves are deliberately plain integers: inspectors use the
 * surrounding seqlock and daemon writers use the acquire/release helpers. */
typedef struct PsBackpressureMetrics
{
	uint64_t	lag_bytes;
	uint64_t	high_water_bytes;
	uint64_t	catchup_bytes;
	uint32_t	throttled;
	uint32_t	reserved;
	uint64_t	throttle_enters;
	uint64_t	throttle_exits;
	uint64_t	foreground_wait_ns;
} PsBackpressureMetrics;

#define PS_INSPECTION_MAX_TIMELINES	1024

/* One immutable timeline entry in the diagnostic publication.  `defined` is
 * an internal presence bit; inspectors expose only the three contract fields.
 * The signed parent makes the root's -1 unambiguous. */
typedef struct PsInspectionTimeline
{
	int64_t		parent_timeline;
	uint64_t	fork_lsn;
	uint64_t	retained_horizon;
	uint32_t	defined;
	uint32_t	reserved;
} PsInspectionTimeline;

/* A bounded runtime diagnostic snapshot.  Writers publish the complete
 * structure under inspection_metrics_seq; readers never inspect live core
 * state or claim a channel.  Plain fields are intentional: the sequence is
 * the coherence mechanism for this private protocol object. */
typedef struct PsInspectionMetrics
{
	uint64_t	timeline_count;
	uint64_t	live_timelines;
	uint64_t	deleting_timelines;
	uint64_t	deleted_timelines;
	uint32_t	metadata_poisoned;
	uint32_t	reserved1;

	uint64_t	layer_count;
	uint64_t	deleting_layers;
	uint64_t	local_layers;
	uint64_t	remote_durable_layers;
	uint32_t	manifest_poisoned;
	uint32_t	reserved2;

	uint64_t	page_debt_segments;
	uint64_t	gc_deleting_layers;
	uint64_t	remote_cleanup_pending;
	uint32_t	forkmeta_pending;
	uint32_t	reserved3;

	uint64_t	owner_count;
	uint64_t	page_history_owners;
	uint64_t	wal_owners;
	uint64_t	wal_index_owners;
	uint64_t	max_generation;
	uint32_t	retention_poisoned;
	uint32_t	reserved4;
	uint32_t	forkmeta_poisoned;
	uint32_t	reserved5;

	PsInspectionTimeline timeline_entries[PS_INSPECTION_MAX_TIMELINES];
} PsInspectionMetrics;

typedef struct PsShmHeader
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	page_size;		/* logical page size negotiated with engine */
	uint32_t	io_unit;		/* == PS_IO_UNIT */
	uint32_t	nchannels;
	uint32_t	nshards;		/* channel pools = channel index mod nshards */
	uint32_t	admission_fence_owner; /* PID owning the pending checkpoint gate */
	uint64_t	channel_stride;
	uint64_t	channels_off;
	uint64_t	admission_fence_epoch; /* monotonically identifies gate attempts */
	uint64_t	admission_pending_epoch; /* 0, or the currently active gate */
	uint64_t	admission_pending_lsn; /* relation mutations <= this LSN defer */
	uint64_t	wal_index_pending_bytes; /* shipped WAL not durably indexed */
	uint32_t	wal_index_lagging_timelines;
	uint32_t	startup_state; /* atomic: PS_SHM_{STARTING,READY,STOPPING} */
	uint64_t	page_prune_metrics_seq;
	uint64_t	page_prune_compactions;
	uint64_t	page_prune_versions_scanned;
	uint64_t	page_prune_versions_kept;
	uint64_t	page_prune_versions_deleted;
	uint64_t	backpressure_metrics_seq;
	PsBackpressureMetrics page_backpressure;
	PsBackpressureMetrics wal_backpressure;
	PsBackpressureMetrics walidx_backpressure;
	PsBackpressureMetrics forkmeta_backpressure;
	uint64_t	inspection_metrics_seq;
	PsInspectionMetrics inspection;
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

static inline uint64_t
ps_load_acquire_u64(volatile uint64_t *p)
{
	return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

static inline void
ps_store_release_u64(volatile uint64_t *p, uint64_t v)
{
	__atomic_store_n(p, v, __ATOMIC_RELEASE);
}

static inline uint64_t
ps_fetch_add_u64(volatile uint64_t *p, uint64_t v)
{
	return __atomic_fetch_add(p, v, __ATOMIC_ACQ_REL);
}

static inline uint64_t
ps_saturating_add_u64(uint64_t a, uint64_t b)
{
	return UINT64_MAX - a < b ? UINT64_MAX : a + b;
}

static inline int
ps_cas_u64(volatile uint64_t *p, uint64_t expected, uint64_t desired)
{
	return __atomic_compare_exchange_n(p, &expected, desired, 0,
									   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

#endif							/* PAGESTORE_IPC_H */
