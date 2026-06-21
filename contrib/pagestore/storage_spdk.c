/*-------------------------------------------------------------------------
 *
 * storage_spdk.c
 *	  SPDK (userspace NVMe) storage backend for the page-store daemon.
 *
 * Optional, higher-performance alternative to storage_posix.c behind the same
 * PsStorage interface, so the IPC ABI is unchanged and a machine without SPDK
 * just uses the POSIX backend.  Built only when PAGESTORE_SPDK is defined and
 * linked against SPDK; see spdk_build.sh.
 *
 * What lives where (S1.2c):
 *   - The page *segments* -- the bulk and the hot path -- live on the raw NVMe
 *     namespace.  Segment id S, byte offset O maps to device byte S*segsize+O.
 *     A segment is written as one sector-aligned, zero-padded extent; the unused
 *     tail reads back as zeros so recover() stops at the first non-magic record
 *     (no per-segment length bookkeeping needed).  The current append segment is
 *     held in a DMA buffer and flushed on roll-over / sync; older segments are
 *     read back with an aligned read + slice.
 *   - The shipped WAL and the timeline metadata are small and not hot, so for
 *     now they stay on the filesystem, delegated to the POSIX backend under the
 *     same --store dir.  (Putting them on-device is a later LSM-layer refinement.)
 *   - The segment count is persisted in <store>/spdk_super, so a fresh --store
 *     dir means a fresh store (matching POSIX semantics) and a restart with the
 *     same dir continues.
 *
 * I/O uses the asynchronous spdk_nvme_ns_cmd_* API; for now each op polls the
 * qpair to completion before returning (correct, not yet pipelined across
 * channels -- that is S1.2c-2).  Library mode: no spdk_app_start.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "spdk/env.h"
#include "spdk/nvme.h"

#include "pagestore_ipc.h"		/* PS_MAX_SHARDS */
#include "pagestore_storage.h"
#include "storage_spdk.h"

/* shard count, set by the frontend before open(); drives the seg-id interleave */
extern uint32_t ps_nshards;

/*
 * Async read engine (see storage_spdk.h): a pool of DMA buffers lets many page
 * reads -- across many request channels -- be in flight on the NVMe queue at
 * once.  Each buffer holds one aligned page read; 64 KiB covers any page size we
 * use plus the alignment slack.  Buffer index == its completion context; a
 * free-list hands them out.
 */
#define PS_SPDK_POOL	256
#define PS_SPDK_BUFSZ	65536

typedef struct PsSpdkRd
{
	struct SpdkShard *sh;		/* shard whose qpair/pool this read uses */
	int			idx;			/* this context's buffer index in that shard */
	void	   *dst;			/* caller's destination (shared memory) */
	uint64_t	slice;			/* byte offset of the page within the DMA buffer */
	uint32_t	len;
	PsSpdkDone	done;
	void	   *arg;
} PsSpdkRd;

#define SPDK_SUPER_MAGIC	0x53504b54	/* "SPKT" (per-shard segment counts) */
#define SPDK_SUPER_MAGIC_V1 0x53504b53	/* "SPKS" (legacy single segment count) */

typedef struct SpdkSuper
{
	uint32_t	magic;
	uint32_t	sector_size;
	uint64_t	segment_size;
	uint32_t	nshards;		/* the store's shard count (must match to reuse) */
	uint32_t	num_segments[PS_MAX_SHARDS];	/* per-shard local segment count */
} SpdkSuper;

/* legacy on-disk superblock (pre-step-5b): one global segment count, no nshards */
typedef struct SpdkSuperV1
{
	uint32_t	magic;
	uint32_t	sector_size;
	uint64_t	segment_size;
	uint32_t	num_segments;
} SpdkSuperV1;

/*
 * Per-shard I/O state (sharding step 5).  Each shard's worker thread owns its own
 * NVMe queue pair, current-append-segment buffer, and read DMA pool, so shards
 * drive the device concurrently with no shared mutable state.  Segment ids are
 * interleaved (seg % nshards == owning shard, seg / nshards == that shard's local
 * index), so every op derives its shard from the seg id and only ever touches
 * that shard's state; a worker thread therefore uses only its own qpair, which
 * SPDK requires (qpairs are not safe for concurrent use).
 */
typedef struct SpdkShard
{
	struct spdk_nvme_qpair *qpair;
	char	   *curbuf;			/* segment_size bytes, DMA-able */
	int			curseg;			/* which segment curbuf holds (-1: none) */
	int			dirty;			/* curbuf has unflushed writes */
	char	   *iobuf;			/* segment_size scratch for aligned reads */
	char	   *pool[PS_SPDK_POOL];		/* read DMA buffers */
	PsSpdkRd	rd[PS_SPDK_POOL];		/* in-flight read context per buffer */
	int			free[PS_SPDK_POOL];		/* free-list of buffer indices */
	int			nfree;
	uint32_t	num_segments;	/* this shard's segment count (local indices) */
} SpdkShard;

static SpdkShard g_spdk[PS_MAX_SHARDS];
static uint32_t g_nshards = 1;	/* segment-id interleave factor (== ps_nshards) */

/* the single control device this daemon owns (shared, read-only after open) */
static struct spdk_nvme_ctrlr *g_ctrlr;
static struct spdk_nvme_ns *g_ns;
static uint32_t g_sector;		/* device LBA size (e.g. 512) */
static uint64_t g_segsize;		/* bytes per segment */
static uint32_t g_secs_per_seg; /* sectors per segment */
static char g_store[2048];		/* --store dir (for spdk_super + WAL/meta) */

/* the shard that owns segment 'seg' and its local index within that shard */
static inline SpdkShard *
shard_of(int seg)
{
	return &g_spdk[(uint32_t) seg % g_nshards];
}

static inline uint32_t
seg_local(int seg)
{
	return (uint32_t) seg / g_nshards;
}

/* --- low-level synchronous-but-polled NVMe I/O --------------------------- */

struct io_ctx
{
	volatile int done;
	volatile int err;
};

static void
io_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct io_ctx *c = arg;

	c->err = spdk_nvme_cpl_is_error(cpl) ? -1 : 0;
	c->done = 1;
}

/* read/write 'sectors' LBAs at 'lba' to/from DMA buffer 'buf' on 'qpair'; poll to
 * done.  The qpair is the calling shard's own (or, during recovery before workers
 * spawn, used single-threaded by the main thread). */
static int
do_io(struct spdk_nvme_qpair *qpair, void *buf, uint64_t lba, uint32_t sectors,
	  int is_write)
{
	struct io_ctx c = {0, 0};
	int			rc;

	rc = is_write
		? spdk_nvme_ns_cmd_write(g_ns, qpair, buf, lba, sectors, io_cb, &c, 0)
		: spdk_nvme_ns_cmd_read(g_ns, qpair, buf, lba, sectors, io_cb, &c, 0);
	if (rc != 0)
		return -1;
	while (!c.done)
		spdk_nvme_qpair_process_completions(qpair, 0);
	return c.err;
}

static uint64_t
seg_lba(int seg)
{
	return (uint64_t) seg * g_secs_per_seg;
}

/* --- spdk_super (segment count) in the store dir ------------------------- */

static void
super_path(char *buf, size_t buflen)
{
	snprintf(buf, buflen, "%s/spdk_super", g_store);
}

/* Write the superblock from an explicit per-shard segment-count array, so the
 * IMMEDSYNC coordinator can persist a consistent post-flush snapshot rather than
 * the live counters that concurrent shard writers keep advancing. */
/*
 * Persist the superblock durably.  recover() trusts these per-shard segment counts
 * as the synced extent, and the sync watermark is committed right after this on an
 * IMMEDSYNC, so the super must hit the disk before the watermark does -- otherwise a
 * crash that loses the super leaves a watermark pointing past the counts recovery
 * can see, bricking the store.  Returns 0 on success, -1 on any I/O failure.
 */
static int
super_write_counts(const uint32_t *counts)
{
	char		path[2300];
	SpdkSuper	s;
	int			fd;

	memset(&s, 0, sizeof(s));
	s.magic = SPDK_SUPER_MAGIC;
	s.sector_size = g_sector;
	s.segment_size = g_segsize;
	s.nshards = g_nshards;
	for (uint32_t sh = 0; sh < g_nshards; sh++)
		s.num_segments[sh] = counts[sh];

	super_path(path, sizeof(path));
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	if (write(fd, &s, sizeof(s)) != (ssize_t) sizeof(s) || fsync(fd) != 0)
	{
		close(fd);
		return -1;
	}
	close(fd);
	fd = open(g_store, O_RDONLY);	/* make the entry durable on first create */
	if (fd >= 0)
	{
		fsync(fd);
		close(fd);
	}
	return 0;
}

/* Superblock from the live counters; safe at close (main thread, workers joined).
 * Returns 0 on success, -1 if it could not be made durable. */
static int
super_write(void)
{
	uint32_t	counts[PS_MAX_SHARDS];

	for (uint32_t sh = 0; sh < g_nshards; sh++)
		counts[sh] = g_spdk[sh].num_segments;
	return super_write_counts(counts);
}

/*
 * Load the persisted per-shard segment counts.  Returns 0 on success (fresh
 * store, current format, or a migrated legacy single-shard store) and -1 on an
 * incompatible superblock, so the caller fails the open rather than silently
 * treating existing on-device data as fresh and overwriting it.
 */
static int
super_read(void)
{
	char		path[2300];
	uint32_t	magic;
	FILE	   *f;

	for (uint32_t sh = 0; sh < g_nshards; sh++)
		g_spdk[sh].num_segments = 0;	/* default: fresh store */
	super_path(path, sizeof(path));
	f = fopen(path, "rb");
	if (!f)
		return 0;				/* no super -> fresh store */
	if (fread(&magic, sizeof(magic), 1, f) != 1)
	{
		fclose(f);
		return -1;				/* unreadable/corrupt */
	}
	rewind(f);

	if (magic == SPDK_SUPER_MAGIC)
	{
		SpdkSuper	s;

		/* reuse the on-device data only if geometry AND shard count match -- a
		 * different nshards reinterprets the interleaved seg-id space */
		if (fread(&s, sizeof(s), 1, f) == 1 && s.sector_size == g_sector &&
			s.segment_size == g_segsize && s.nshards == g_nshards)
		{
			for (uint32_t sh = 0; sh < g_nshards; sh++)
				g_spdk[sh].num_segments = s.num_segments[sh];
			fclose(f);
			return 0;
		}
		fclose(f);
		return -1;				/* current format but mismatched geometry/nshards */
	}
	if (magic == SPDK_SUPER_MAGIC_V1)
	{
		SpdkSuperV1 v1;

		/* legacy single-count store is only meaningful at nshards == 1 (seg ids
		 * were 0,1,2,...); migrate it, else refuse rather than lose the data */
		if (g_nshards == 1 && fread(&v1, sizeof(v1), 1, f) == 1 &&
			v1.sector_size == g_sector && v1.segment_size == g_segsize)
		{
			g_spdk[0].num_segments = v1.num_segments;
			fclose(f);
			return 0;
		}
		fclose(f);
		return -1;
	}
	fclose(f);
	return -1;					/* unknown magic */
}

/* --- current-segment buffer management ----------------------------------- */

static int
flush_curbuf(SpdkShard *sh)
{
	if (sh->curseg < 0 || !sh->dirty)
		return 0;
	if (do_io(sh->qpair, sh->curbuf, seg_lba(sh->curseg), g_secs_per_seg, 1) != 0)
		return -1;
	sh->dirty = 0;
	return 0;
}

/* Make this shard's curbuf hold segment 'seg' (loading it if it exists). */
static int
load_curbuf(SpdkShard *sh, int seg)
{
	if (flush_curbuf(sh) != 0)
		return -1;
	if (seg_local(seg) < sh->num_segments)
	{
		if (do_io(sh->qpair, sh->curbuf, seg_lba(seg), g_secs_per_seg, 0) != 0)
			return -1;
	}
	else
		memset(sh->curbuf, 0, g_segsize);	/* fresh segment: zero tail */
	sh->curseg = seg;
	sh->dirty = 0;
	return 0;
}

/* --- SPDK bring-up ------------------------------------------------------- */

static bool
probe_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		 struct spdk_nvme_ctrlr_opts *opts)
{
	(void) ctx; (void) trid; (void) opts;
	return true;
}

static void
attach_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	(void) ctx; (void) trid; (void) opts;
	if (!g_ctrlr)
		g_ctrlr = ctrlr;
}

/*
 * open(): 'path' is the --store dir (for spdk_super and the delegated WAL/meta
 * files); the control disk's PCI address comes from $PS_SPDK_PCI.
 */
static int
spdk_open(const char *path, uint64_t segment_size)
{
	struct spdk_env_opts opts;
	struct spdk_nvme_transport_id trid;
	const char *pci = getenv("PS_SPDK_PCI");

	if (!pci)
	{
		fprintf(stderr, "storage_spdk: PS_SPDK_PCI not set "
				"(control disk PCI address, e.g. 0000:06:00.0)\n");
		return -1;
	}

	/* WAL + timeline metadata are delegated to the POSIX backend under store */
	if (PsStoragePosix.open(path, segment_size) != 0)
		return -1;
	snprintf(g_store, sizeof(g_store), "%s", path);
	g_segsize = segment_size;

	opts.opts_size = sizeof(opts);	/* required by spdk_env_opts_init in v26.01 */
	spdk_env_opts_init(&opts);
	opts.name = "pagestore_daemon_spdk";
	if (spdk_env_init(&opts) < 0)
	{
		fprintf(stderr, "storage_spdk: spdk_env_init failed\n");
		return -1;
	}

	memset(&trid, 0, sizeof(trid));
	spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
	snprintf(trid.traddr, sizeof(trid.traddr), "%s", pci);
	if (spdk_nvme_probe(&trid, NULL, probe_cb, attach_cb, NULL) != 0 || !g_ctrlr)
	{
		fprintf(stderr, "storage_spdk: could not attach NVMe at %s "
				"(bound to vfio-pci? see spdk_setup.sh)\n", pci);
		return -1;
	}

	g_ns = spdk_nvme_ctrlr_get_ns(g_ctrlr, 1);
	if (!g_ns || !spdk_nvme_ns_is_active(g_ns))
	{
		fprintf(stderr, "storage_spdk: namespace 1 not active\n");
		return -1;
	}
	g_sector = spdk_nvme_ns_get_sector_size(g_ns);
	if (g_segsize % g_sector != 0)
	{
		fprintf(stderr, "storage_spdk: segment size %llu not a multiple of "
				"sector %u\n", (unsigned long long) g_segsize, g_sector);
		return -1;
	}
	g_secs_per_seg = (uint32_t) (g_segsize / g_sector);

	/* one qpair + buffer set per shard, so each shard's worker drives the device
	 * independently (sharding step 5); the frontend set ps_nshards before open */
	g_nshards = (ps_nshards >= 1 && ps_nshards <= PS_MAX_SHARDS) ? ps_nshards : 1;
	for (uint32_t sh = 0; sh < g_nshards; sh++)
	{
		SpdkShard  *S = &g_spdk[sh];

		S->curseg = -1;
		S->qpair = spdk_nvme_ctrlr_alloc_io_qpair(g_ctrlr, NULL, 0);
		S->curbuf = spdk_zmalloc(g_segsize, 0x1000, NULL,
								 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		S->iobuf = spdk_zmalloc(g_segsize, 0x1000, NULL,
								SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		if (!S->qpair || !S->curbuf || !S->iobuf)
		{
			fprintf(stderr, "storage_spdk: qpair/DMA buffer allocation failed\n");
			return -1;
		}
		for (int j = 0; j < PS_SPDK_POOL; j++)
		{
			S->pool[j] = spdk_zmalloc(PS_SPDK_BUFSZ, 0x1000, NULL,
									  SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
			if (!S->pool[j])
			{
				fprintf(stderr, "storage_spdk: read-pool DMA allocation failed\n");
				return -1;
			}
			S->free[j] = j;
		}
		S->nfree = PS_SPDK_POOL;
	}

	if (super_read() != 0)		/* segment counts: fresh dir -> 0, else load/migrate */
	{
		fprintf(stderr, "storage_spdk: incompatible spdk_super "
				"(format/geometry/nshards mismatch); refusing to open\n");
		return -1;
	}

	fprintf(stderr, "storage_spdk: %s ns1 sector=%u segsize=%llu segments=%u "
			"nshards=%u\n", pci, g_sector, (unsigned long long) g_segsize,
			g_spdk[0].num_segments, g_nshards);
	return 0;
}

static void
spdk_close(void)
{
	for (uint32_t sh = 0; sh < g_nshards; sh++)
		flush_curbuf(&g_spdk[sh]);
	super_write();
	for (uint32_t sh = 0; sh < g_nshards; sh++)
	{
		SpdkShard  *S = &g_spdk[sh];

		if (S->curbuf)
			spdk_free(S->curbuf);
		if (S->iobuf)
			spdk_free(S->iobuf);
		for (int j = 0; j < PS_SPDK_POOL; j++)
			if (S->pool[j])
			{
				spdk_free(S->pool[j]);
				S->pool[j] = NULL;
			}
		if (S->qpair)
			spdk_nvme_ctrlr_free_io_qpair(S->qpair);
		S->curbuf = S->iobuf = NULL;
		S->qpair = NULL;
	}
	if (g_ctrlr)
		spdk_nvme_detach(g_ctrlr);
	g_ctrlr = NULL;
	g_ns = NULL;
	PsStoragePosix.close();
}

static int
spdk_sync(void)
{
	for (uint32_t sh = 0; sh < g_nshards; sh++)
		if (flush_curbuf(&g_spdk[sh]) != 0)
			return -1;
	/* propagate a failed superblock write: the clean-close path advances the sync
	 * watermark on a 0 return, and a watermark past a non-persisted super would brick
	 * the store on the next open */
	return super_write();
}

/* --- segment byte I/O ---------------------------------------------------- */

static int
spdk_seg_write(int seg, uint64_t off, const void *buf, uint32_t len)
{
	SpdkShard  *sh = shard_of(seg);

	if (off + len > g_segsize)
		return -1;
	if (seg != sh->curseg && load_curbuf(sh, seg) != 0)
		return -1;
	memcpy(sh->curbuf + off, buf, len);
	sh->dirty = 1;
	if (seg_local(seg) + 1 > sh->num_segments)
		sh->num_segments = seg_local(seg) + 1;
	return 0;
}

static int
spdk_seg_read(int seg, uint64_t off, void *buf, uint32_t len)
{
	SpdkShard  *sh = shard_of(seg);
	uint64_t	byte0,
				start,
				end,
				lba;
	uint32_t	sectors;

	if (off + len > g_segsize)
		return -1;

	/* current append segment is authoritative in the buffer */
	if (seg == sh->curseg)
	{
		memcpy(buf, sh->curbuf + off, len);
		return 0;
	}
	if (seg_local(seg) >= sh->num_segments)
		return -1;				/* no such segment -> end of log for recover() */

	/* aligned read of the covering sectors, then slice out [off, off+len) */
	byte0 = (uint64_t) seg * g_segsize + off;
	start = byte0 - (byte0 % g_sector);
	end = byte0 + len;
	if (end % g_sector)
		end += g_sector - (end % g_sector);
	lba = start / g_sector;
	sectors = (uint32_t) ((end - start) / g_sector);
	if ((uint64_t) sectors * g_sector > g_segsize)
		return -1;				/* scratch buffer is one segment */
	if (do_io(sh->qpair, sh->iobuf, lba, sectors, 0) != 0)
		return -1;
	memcpy(buf, sh->iobuf + (byte0 - start), len);
	return 0;
}

static int64_t
spdk_seg_size(int seg)
{
	SpdkShard  *sh = shard_of(seg);

	if (seg_local(seg) < sh->num_segments || seg == sh->curseg)
		return (int64_t) g_segsize;
	return -1;
}

/*
 * Completion of one queued device read: deliver the page (zero-fill on error),
 * return the buffer to the free-list, and notify the caller.  Runs inside
 * spdk_nvme_qpair_process_completions, i.e. single-threaded with the loop.
 */
static void
rd_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	PsSpdkRd   *rd = arg;
	SpdkShard  *sh = rd->sh;
	int			ok = !spdk_nvme_cpl_is_error(cpl);

	if (ok)
		memcpy(rd->dst, sh->pool[rd->idx] + rd->slice, rd->len);
	else
		memset(rd->dst, 0, rd->len);
	sh->free[sh->nfree++] = rd->idx;	/* free the buffer before notifying */
	rd->done(rd->arg, ok);
}

int
ps_spdk_read_async(int seg, uint64_t off, void *dst, uint32_t len,
				   PsSpdkDone done, void *arg)
{
	SpdkShard  *sh = shard_of(seg);
	uint64_t	byte0,
				start,
				end;
	uint32_t	sectors;
	int			b;
	PsSpdkRd   *rd;

	/* current append segment is authoritative in memory */
	if (seg == sh->curseg)
	{
		memcpy(dst, sh->curbuf + off, len);
		done(arg, 1);
		return 0;
	}
	if (seg_local(seg) >= sh->num_segments)
	{
		memset(dst, 0, len);	/* no such segment */
		done(arg, 1);
		return 0;
	}

	byte0 = (uint64_t) seg * g_segsize + off;
	start = byte0 - (byte0 % g_sector);
	end = byte0 + len;
	if (end % g_sector)
		end += g_sector - (end % g_sector);
	sectors = (uint32_t) ((end - start) / g_sector);
	if ((uint64_t) sectors * g_sector > PS_SPDK_BUFSZ)
	{
		memset(dst, 0, len);	/* read too large for a pool buffer */
		done(arg, 0);
		return 0;
	}

	/* take a free DMA buffer, polling completions until one frees if needed */
	while (sh->nfree == 0)
		spdk_nvme_qpair_process_completions(sh->qpair, 0);
	b = sh->free[--sh->nfree];
	rd = &sh->rd[b];
	rd->sh = sh;
	rd->idx = b;
	rd->dst = dst;
	rd->slice = byte0 - start;
	rd->len = len;
	rd->done = done;
	rd->arg = arg;
	if (spdk_nvme_ns_cmd_read(g_ns, sh->qpair, sh->pool[b], start / g_sector,
							  sectors, rd_cb, rd, 0) != 0)
	{
		sh->free[sh->nfree++] = b;	/* submission failed: give the buffer back */
		memset(dst, 0, len);
		done(arg, 0);
	}
	return 0;
}

int
ps_spdk_poll(uint32_t shard)
{
	if (shard >= g_nshards)
		return 0;
	return spdk_nvme_qpair_process_completions(g_spdk[shard].qpair, 0);
}

/*
 * Flush one shard's current-segment buffer to the device.  Must be called from
 * that shard's own worker thread (it drives the shard's qpair), which is how the
 * SPDK daemon coordinates a cross-shard IMMEDSYNC: each shard flushes itself.
 * On return *out_count (if non-NULL) holds that shard's segment count AS OF the
 * flush, so the coordinator can persist a count covering exactly the flushed data
 * even if the shard appends more afterwards.
 */
int
ps_spdk_flush(uint32_t shard, uint32_t *out_count)
{
	int			rc = 0;

	if (shard < g_nshards)
		rc = flush_curbuf(&g_spdk[shard]);
	if (out_count)
		*out_count = (shard < g_nshards) ? g_spdk[shard].num_segments : 0;
	return rc;
}

/* Persist the superblock from a caller-supplied per-shard count snapshot (taken
 * at flush time by ps_spdk_flush), so the persisted counts cover exactly the
 * flushed data and never a segment a concurrent writer added but did not flush.
 * Returns 0 on success, -1 if the superblock could not be made durable -- the
 * caller must not advance the sync watermark past a super it failed to persist. */
int
ps_spdk_super_write_counts(const uint32_t *counts)
{
	return super_write_counts(counts);
}

/* --- WAL + timeline metadata: delegated to the POSIX backend ------------- */

static int
spdk_wal_append(uint32_t tl, const void *a, uint32_t alen,
				const void *b, uint32_t blen)
{
	return PsStoragePosix.wal_append(tl, a, alen, b, blen);
}

static int
spdk_wal_read(uint32_t tl, uint64_t off, void *buf, uint32_t len)
{
	return PsStoragePosix.wal_read(tl, off, buf, len);
}

static int
spdk_meta_append(const void *buf, uint32_t len)
{
	return PsStoragePosix.meta_append(buf, len);
}

static int
spdk_meta_read(uint64_t off, void *buf, uint32_t len)
{
	return PsStoragePosix.meta_read(off, buf, len);
}

const PsStorage PsStorageSpdk = {
	.name = "spdk",
	.open = spdk_open,
	.close = spdk_close,
	.sync = spdk_sync,
	.seg_write = spdk_seg_write,
	.seg_read = spdk_seg_read,
	.seg_size = spdk_seg_size,
	.wal_append = spdk_wal_append,
	.wal_read = spdk_wal_read,
	.meta_append = spdk_meta_append,
	.meta_read = spdk_meta_read,
};
