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
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "spdk/env.h"
#include "spdk/nvme.h"

#include "pagestore_storage.h"
#include "storage_spdk.h"

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
	void	   *dst;			/* caller's destination (shared memory) */
	uint64_t	slice;			/* byte offset of the page within the DMA buffer */
	uint32_t	len;
	PsSpdkDone	done;
	void	   *arg;
} PsSpdkRd;

static char *g_pool[PS_SPDK_POOL];	/* DMA buffers */
static PsSpdkRd g_rd[PS_SPDK_POOL]; /* one in-flight read context per buffer */
static int	g_free[PS_SPDK_POOL];	/* free-list of buffer indices */
static int	g_nfree;

#define SPDK_SUPER_MAGIC	0x53504b53	/* "SPKS" */

typedef struct SpdkSuper
{
	uint32_t	magic;
	uint32_t	sector_size;
	uint64_t	segment_size;
	uint32_t	num_segments;
} SpdkSuper;

/* the single control device this daemon owns */
static struct spdk_nvme_ctrlr *g_ctrlr;
static struct spdk_nvme_ns *g_ns;
static struct spdk_nvme_qpair *g_qpair;
static uint32_t g_sector;		/* device LBA size (e.g. 512) */
static uint64_t g_segsize;		/* bytes per segment */
static uint32_t g_secs_per_seg; /* sectors per segment */
static uint32_t g_num_segments; /* segments that hold data */
static char g_store[2048];		/* --store dir (for spdk_super + WAL/meta) */

/* DMA buffers: the current append segment, and a scratch buffer for reads */
static char *g_curbuf;			/* segment_size bytes, DMA-able */
static int	g_curseg = -1;		/* which segment g_curbuf holds (-1: none) */
static int	g_dirty;			/* g_curbuf has unflushed writes */
static char *g_iobuf;			/* segment_size scratch for aligned reads */

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

/* read/write 'sectors' LBAs at 'lba' to/from DMA buffer 'buf'; poll to done. */
static int
do_io(void *buf, uint64_t lba, uint32_t sectors, int is_write)
{
	struct io_ctx c = {0, 0};
	int			rc;

	rc = is_write
		? spdk_nvme_ns_cmd_write(g_ns, g_qpair, buf, lba, sectors, io_cb, &c, 0)
		: spdk_nvme_ns_cmd_read(g_ns, g_qpair, buf, lba, sectors, io_cb, &c, 0);
	if (rc != 0)
		return -1;
	while (!c.done)
		spdk_nvme_qpair_process_completions(g_qpair, 0);
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

static void
super_write(void)
{
	char		path[2300];
	SpdkSuper	s = {SPDK_SUPER_MAGIC, g_sector, g_segsize, g_num_segments};
	FILE	   *f;

	super_path(path, sizeof(path));
	f = fopen(path, "wb");
	if (!f)
		return;					/* best-effort */
	fwrite(&s, sizeof(s), 1, f);
	fclose(f);
}

static void
super_read(void)
{
	char		path[2300];
	SpdkSuper	s;
	FILE	   *f;

	g_num_segments = 0;			/* default: fresh store */
	super_path(path, sizeof(path));
	f = fopen(path, "rb");
	if (!f)
		return;
	if (fread(&s, sizeof(s), 1, f) == 1 && s.magic == SPDK_SUPER_MAGIC &&
		s.sector_size == g_sector && s.segment_size == g_segsize)
		g_num_segments = s.num_segments;
	fclose(f);
}

/* --- current-segment buffer management ----------------------------------- */

static int
flush_curbuf(void)
{
	if (g_curseg < 0 || !g_dirty)
		return 0;
	if (do_io(g_curbuf, seg_lba(g_curseg), g_secs_per_seg, 1) != 0)
		return -1;
	g_dirty = 0;
	return 0;
}

/* Make g_curbuf hold segment 'seg' (loading it from the device if it exists). */
static int
load_curbuf(int seg)
{
	if (flush_curbuf() != 0)
		return -1;
	if ((uint32_t) seg < g_num_segments)
	{
		if (do_io(g_curbuf, seg_lba(seg), g_secs_per_seg, 0) != 0)
			return -1;
	}
	else
		memset(g_curbuf, 0, g_segsize);		/* fresh segment: zero tail */
	g_curseg = seg;
	g_dirty = 0;
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

	g_qpair = spdk_nvme_ctrlr_alloc_io_qpair(g_ctrlr, NULL, 0);
	g_curbuf = spdk_zmalloc(g_segsize, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY,
							SPDK_MALLOC_DMA);
	g_iobuf = spdk_zmalloc(g_segsize, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY,
						   SPDK_MALLOC_DMA);
	if (!g_qpair || !g_curbuf || !g_iobuf)
	{
		fprintf(stderr, "storage_spdk: qpair/DMA buffer allocation failed\n");
		return -1;
	}
	for (int j = 0; j < PS_SPDK_POOL; j++)
	{
		g_pool[j] = spdk_zmalloc(PS_SPDK_BUFSZ, 0x1000, NULL,
								 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		if (!g_pool[j])
		{
			fprintf(stderr, "storage_spdk: read-pool DMA allocation failed\n");
			return -1;
		}
		g_free[j] = j;
	}
	g_nfree = PS_SPDK_POOL;

	super_read();				/* segment count: fresh dir -> 0 */

	fprintf(stderr, "storage_spdk: %s ns1 sector=%u segsize=%llu segments=%u\n",
			pci, g_sector, (unsigned long long) g_segsize, g_num_segments);
	return 0;
}

static void
spdk_close(void)
{
	flush_curbuf();
	super_write();
	if (g_curbuf)
		spdk_free(g_curbuf);
	if (g_iobuf)
		spdk_free(g_iobuf);
	for (int j = 0; j < PS_SPDK_POOL; j++)
		if (g_pool[j])
		{
			spdk_free(g_pool[j]);
			g_pool[j] = NULL;
		}
	if (g_qpair)
		spdk_nvme_ctrlr_free_io_qpair(g_qpair);
	if (g_ctrlr)
		spdk_nvme_detach(g_ctrlr);
	g_curbuf = g_iobuf = NULL;
	g_qpair = NULL;
	g_ctrlr = NULL;
	g_ns = NULL;
	PsStoragePosix.close();
}

static int
spdk_sync(void)
{
	if (flush_curbuf() != 0)
		return -1;
	super_write();
	return 0;
}

/* --- segment byte I/O ---------------------------------------------------- */

static int
spdk_seg_write(int seg, uint64_t off, const void *buf, uint32_t len)
{
	if (off + len > g_segsize)
		return -1;
	if (seg != g_curseg && load_curbuf(seg) != 0)
		return -1;
	memcpy(g_curbuf + off, buf, len);
	g_dirty = 1;
	if ((uint32_t) seg + 1 > g_num_segments)
		g_num_segments = (uint32_t) seg + 1;
	return 0;
}

static int
spdk_seg_read(int seg, uint64_t off, void *buf, uint32_t len)
{
	uint64_t	byte0,
				start,
				end,
				lba;
	uint32_t	sectors;

	if (off + len > g_segsize)
		return -1;

	/* current append segment is authoritative in the buffer */
	if (seg == g_curseg)
	{
		memcpy(buf, g_curbuf + off, len);
		return 0;
	}
	if ((uint32_t) seg >= g_num_segments)
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
	if (do_io(g_iobuf, lba, sectors, 0) != 0)
		return -1;
	memcpy(buf, g_iobuf + (byte0 - start), len);
	return 0;
}

static int64_t
spdk_seg_size(int seg)
{
	if ((uint32_t) seg < g_num_segments || seg == g_curseg)
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
	int			b = (int) (intptr_t) arg;	/* buffer/context index */
	PsSpdkRd   *rd = &g_rd[b];
	int			ok = !spdk_nvme_cpl_is_error(cpl);

	if (ok)
		memcpy(rd->dst, g_pool[b] + rd->slice, rd->len);
	else
		memset(rd->dst, 0, rd->len);
	g_free[g_nfree++] = b;		/* free the buffer before notifying */
	rd->done(rd->arg, ok);
}

int
ps_spdk_read_async(int seg, uint64_t off, void *dst, uint32_t len,
				   PsSpdkDone done, void *arg)
{
	uint64_t	byte0,
				start,
				end;
	uint32_t	sectors;
	int			b;
	PsSpdkRd   *rd;

	/* current append segment is authoritative in memory */
	if (seg == g_curseg)
	{
		memcpy(dst, g_curbuf + off, len);
		done(arg, 1);
		return 0;
	}
	if ((uint32_t) seg >= g_num_segments)
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
	while (g_nfree == 0)
		spdk_nvme_qpair_process_completions(g_qpair, 0);
	b = g_free[--g_nfree];
	rd = &g_rd[b];
	rd->dst = dst;
	rd->slice = byte0 - start;
	rd->len = len;
	rd->done = done;
	rd->arg = arg;
	if (spdk_nvme_ns_cmd_read(g_ns, g_qpair, g_pool[b], start / g_sector,
							  sectors, rd_cb, (void *) (intptr_t) b, 0) != 0)
	{
		g_free[g_nfree++] = b;	/* submission failed: give the buffer back */
		memset(dst, 0, len);
		done(arg, 0);
	}
	return 0;
}

int
ps_spdk_poll(void)
{
	return spdk_nvme_qpair_process_completions(g_qpair, 0);
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
