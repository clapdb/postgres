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
 *     namespace.  Segment id S, byte offset O maps to device byte
 *     (S * nshards + shard) * segment_size + O.
 *   - A segment is written as one sector-aligned, zero-padded extent; the unused
 *     tail reads back as zeros so recover() stops at the first non-magic record
 *     (no per-segment length bookkeeping needed).  The current append segment is
 *     held in a DMA buffer and flushed on roll-over / sync; older segments are
 *     read with an aligned read + slice.
 *   - The shipped WAL and timeline metadata are small and not hot, so for
 *     now they stay on the filesystem, delegated to the POSIX backend under the
 *     same --store dir.  (Putting them on-device is a later LSM-layer refinement.)
 *   - Segment counts are persisted in <store>/spdk_super per shard, so restart
 *     continues from the same append position.
 *
 * I/O uses spdk_nvme_ns_cmd_* asynchronously plus completion polling.  SPDK
 * daemon threads each use their own qpair and per-thread read-pool so multiple
 * shards can service independent submit/poll loops.
 *
 *-------------------------------------------------------------------------
 */
#include <stdbool.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "spdk/env.h"
#include "spdk/nvme.h"

#include "pagestore_core.h"
#include "pagestore_storage.h"
#include "storage_spdk.h"

/*
 * Async read engine (see storage_spdk.h): a pool of DMA buffers lets many page
 * reads -- across many request channels -- be in flight on the NVMe queue at
 * once.  Each buffer holds one aligned page read; 64 KiB covers any page size we
 * use plus the alignment slack.
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
	void	   *owner;			/* owning PsSpdkThread */
} PsSpdkRd;

typedef struct PsSpdkThread
{
	uint32_t	id;
	int			curseg;
	int			dirty;
	uint32_t	num_segments;
	struct spdk_nvme_qpair *qpair;
	char	   *curbuf;			/* segment_size bytes, DMA-able */
	char	   *iobuf;			/* segment_size scratch for aligned reads */
	char	   *pool[PS_SPDK_POOL];	/* one DMA buffer per in-flight read */
	PsSpdkRd	rd[PS_SPDK_POOL];/* read contexts */
	int			free[PS_SPDK_POOL];	/* free-list of buffer indices */
	int			nfree;
} PsSpdkThread;

static PsSpdkThread g_threads[PS_MAX_CHANNELS];
static uint32_t g_nshards = 1;

struct io_ctx
{
	volatile int done;
	volatile int err;
};

#define SPDK_SUPER_MAGIC	0x53504b53	/* "SPKS" */
#define SPDK_SUPER_VERSION	1

typedef struct SpdkSuperV1
{
	uint32_t	magic;
	uint32_t	sector_size;
	uint64_t	segment_size;
	uint32_t	num_segments;
} SpdkSuperV1;

typedef struct SpdkSuperV2
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	sector_size;
	uint64_t	segment_size;
	uint32_t	nshards;
	uint32_t	num_segments[PS_MAX_CHANNELS];
} SpdkSuperV2;

/* the single control device this daemon owns */
static struct spdk_nvme_ctrlr *g_ctrlr;
static struct spdk_nvme_ns *g_ns;
static uint32_t g_sector;		/* device LBA size (e.g. 512) */
static uint64_t g_segsize;		/* bytes per segment */
static uint32_t g_secs_per_seg; /* sectors per segment */
static char g_store[2048];		/* --store dir (for spdk_super + WAL/meta) */

/* --- low-level synchronous-but-polled NVMe I/O --------------------------- */

static PsSpdkThread *
thread_for(uint32_t shard)
{
	if (shard >= g_nshards)
		return NULL;
	return &g_threads[shard];
}
static void
thread_free(PsSpdkThread *t);

static void
io_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct io_ctx *c = arg;

	c->err = spdk_nvme_cpl_is_error(cpl) ? -1 : 0;
	c->done = 1;
}

/* read/write 'sectors' LBAs at 'lba' to/from DMA buffer 'buf'; poll to done. */
static int
do_io(PsSpdkThread *t, void *buf, uint64_t lba, uint32_t sectors, int is_write)
{
	struct io_ctx c = {0, 0};
	int			rc;

	rc = is_write
		? spdk_nvme_ns_cmd_write(g_ns, t->qpair, buf, lba, sectors, io_cb, &c, 0)
		: spdk_nvme_ns_cmd_read(g_ns, t->qpair, buf, lba, sectors, io_cb, &c, 0);
	if (rc != 0)
		return -1;
	while (!c.done)
		spdk_nvme_qpair_process_completions(t->qpair, 0);
	return c.err;
}

static uint64_t
seg_lba(PsSpdkThread *t, int seg)
{
	return ((uint64_t) seg * g_nshards + t->id) * g_secs_per_seg;
}

static uint64_t
seg_byte(PsSpdkThread *t, int seg, uint64_t off)
{
	return ((uint64_t) seg * g_nshards + t->id) * g_segsize + off;
}

/* --- spdk_super (segment count) in the store dir ------------------------- */

static void
super_path(char *buf, size_t buflen)
{
	snprintf(buf, buflen, "%s/spdk_super", g_store);
}

static void
super_init_threads(void)
{
	for (uint32_t i = 0; i < g_nshards; i++)
	{
		PsSpdkThread *t = &g_threads[i];

		t->id = i;
		t->curseg = -1;
		t->dirty = 0;
		t->num_segments = 0;
		t->nfree = 0;
		t->qpair = NULL;
		t->curbuf = NULL;
		t->iobuf = NULL;
		memset(t->pool, 0, sizeof(t->pool));
		memset(t->free, 0, sizeof(t->free));
		memset(t->rd, 0, sizeof(t->rd));
	}
}

static void
super_read(void)
{
	char			path[2300];
	FILE		   *f;
	SpdkSuperV2	 s2;
	SpdkSuperV1	 s1;

	for (uint32_t i = 0; i < g_nshards; i++)
		g_threads[i].num_segments = 0;
	super_path(path, sizeof(path));
	f = fopen(path, "rb");
	if (!f)
		return;

	if (fread(&s2, sizeof(s2), 1, f) == 1 &&
		s2.magic == SPDK_SUPER_MAGIC &&
		s2.version == SPDK_SUPER_VERSION &&
		s2.sector_size == g_sector &&
		s2.segment_size == g_segsize)
	{
		uint32_t copy = s2.nshards < g_nshards ? s2.nshards : g_nshards;

		for (uint32_t i = 0; i < copy; i++)
			g_threads[i].num_segments = s2.num_segments[i];
		fclose(f);
		return;
	}

	rewind(f);
	if (fread(&s1, sizeof(s1), 1, f) == 1 &&
		s1.magic == SPDK_SUPER_MAGIC &&
		s1.sector_size == g_sector &&
		s1.segment_size == g_segsize)
	{
		g_threads[0].num_segments = s1.num_segments;
	}
	fclose(f);
}

static void
super_write(void)
{
	char		path[2300];
	SpdkSuperV2	s = {0};
	FILE	   *f;

	s.magic = SPDK_SUPER_MAGIC;
	s.version = SPDK_SUPER_VERSION;
	s.sector_size = g_sector;
	s.segment_size = g_segsize;
	s.nshards = g_nshards;
	for (uint32_t i = 0; i < g_nshards; i++)
		s.num_segments[i] = g_threads[i].num_segments;

	super_path(path, sizeof(path));
	f = fopen(path, "wb");
	if (!f)
		return;					/* best-effort */
	fwrite(&s, sizeof(s), 1, f);
	fclose(f);
}

/* --- thread context allocation ------------------------------------------- */

static int
thread_alloc(PsSpdkThread *t)
{
	if (!t)
		return -1;
	t->qpair = spdk_nvme_ctrlr_alloc_io_qpair(g_ctrlr, NULL, 0);
	t->curbuf = spdk_zmalloc(g_segsize, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY,
							 SPDK_MALLOC_DMA);
	t->iobuf = spdk_zmalloc(g_segsize, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY,
							SPDK_MALLOC_DMA);
	if (!t->qpair || !t->curbuf || !t->iobuf)
	{
		thread_free(t);
		return -1;
	}

	for (int j = 0; j < PS_SPDK_POOL; j++)
	{
		t->pool[j] = spdk_zmalloc(PS_SPDK_BUFSZ, 0x1000, NULL,
								  SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		if (!t->pool[j])
		{
			thread_free(t);
			return -1;
		}
		t->free[j] = j;
	}
	t->nfree = PS_SPDK_POOL;
	for (int j = 0; j < PS_SPDK_POOL; j++)
		t->rd[j].owner = t;
	return 0;
}

static void
thread_free(PsSpdkThread *t)
{
	if (!t)
		return;
	if (t->dirty && t->curseg >= 0 && t->qpair)
		do_io(t, t->curbuf, seg_lba(t, t->curseg), g_secs_per_seg, 1);
	if (t->curbuf)
	{
		spdk_free(t->curbuf);
		t->curbuf = NULL;
	}
	if (t->iobuf)
	{
		spdk_free(t->iobuf);
		t->iobuf = NULL;
	}
	for (int j = 0; j < PS_SPDK_POOL; j++)
	{
		if (t->pool[j])
		{
			spdk_free(t->pool[j]);
			t->pool[j] = NULL;
		}
		t->free[j] = 0;
	}
	t->nfree = 0;
	if (t->qpair)
	{
		spdk_nvme_ctrlr_free_io_qpair(t->qpair);
		t->qpair = NULL;
	}
	t->curseg = -1;
	t->dirty = 0;
	t->num_segments = 0;
	memset(t->rd, 0, sizeof(t->rd));
}

static int
thread_init(PsSpdkThread *t)
{
	if (!t)
		return -1;
	if (t->qpair)
		return 0;				/* already initialized in spdk_open */
	return thread_alloc(t);
}

/* --- segment helpers ----------------------------------------------- */

static int
flush_curbuf(PsSpdkThread *t)
{
	if (t->curseg < 0 || !t->dirty)
		return 0;
	if (do_io(t, t->curbuf, seg_lba(t, t->curseg), g_secs_per_seg, 1) != 0)
		return -1;
	t->dirty = 0;
	return 0;
}

/* Make t->curbuf hold segment 'seg' (loading it from the device if it exists). */
static int
load_curbuf(PsSpdkThread *t, int seg)
{
	if (flush_curbuf(t) != 0)
		return -1;
	if ((uint32_t) seg < t->num_segments)
	{
		if (do_io(t, t->curbuf, seg_lba(t, seg), g_secs_per_seg, 0) != 0)
			return -1;
	}
	else
		memset(t->curbuf, 0, g_segsize);		/* fresh segment: zero tail */
	t->curseg = seg;
	t->dirty = 0;
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
spdk_emit_env_diag(const char *pci)
{
	uint32_t	uid;
	char		runtime_path[256];
	struct stat	st;
	char		cmdline[1024];
	FILE	   *f;

	uid = (uint32_t) getuid();
	snprintf(runtime_path, sizeof(runtime_path), "/run/user/%u/dpdk", uid);

	fprintf(stderr, "storage_spdk: environment diagnostics\n");
	fprintf(stderr, "  uid=%u user=%s euid=%u\n",
			uid, getenv("USER") ? getenv("USER") : "unknown",
			(uint32_t) geteuid());
	fprintf(stderr, "  runtime path: %s\n", runtime_path);
	if (stat(runtime_path, &st) != 0)
	{
		fprintf(stderr, "  runtime path missing (%s=%s)\n",
				(errno == EACCES ? "not accessible" : strerror(errno)),
				runtime_path);
	}
	else if (access(runtime_path, X_OK | W_OK) != 0)
	{
		fprintf(stderr, "  runtime path not writable: %s\n", strerror(errno));
	}
	else
	{
		fprintf(stderr, "  runtime path ok\n");
	}

	if (access("/dev/hugepages", F_OK) != 0)
		fprintf(stderr, "  /dev/hugepages not present: %s\n", strerror(errno));
	else if (stat("/dev/hugepages", &st) != 0 || !S_ISDIR(st.st_mode))
		fprintf(stderr, "  /dev/hugepages is not a directory\n");
	else if (access("/dev/hugepages", R_OK | W_OK) != 0)
		fprintf(stderr, "  /dev/hugepages not writable: %s\n", strerror(errno));
	else
		fprintf(stderr, "  /dev/hugepages accessible\n");

	f = fopen("/proc/meminfo", "r");
	if (f)
	{
		bool saw_total = false;
		bool saw_free = false;
		while (fgets(cmdline, sizeof(cmdline), f))
		{
			if (strncmp(cmdline, "HugePages_Total:", 16) == 0)
			{
				fprintf(stderr, "  %s", cmdline);
				saw_total = true;
			}
			else if (strncmp(cmdline, "HugePages_Free:", 15) == 0)
			{
				fprintf(stderr, "  %s", cmdline);
				saw_free = true;
			}
			else if (strncmp(cmdline, "Hugepagesize:", 12) == 0)
				fprintf(stderr, "  %s", cmdline);
			if (saw_total && saw_free)
				break;
		}
		fclose(f);
	}

	fprintf(stderr, "  PCI lookup: ");
	f = fopen("/proc/bus/pci/devices", "r");
	if (f)
	{
		fclose(f);
		fprintf(stderr, "pci bus entries visible\n");
	}
	else
		fprintf(stderr, "cannot read /proc/bus/pci/devices (%s)\n", strerror(errno));

	if (pci)
		fprintf(stderr, "  requested PCI: %s\n", pci);

	if (access("/sys/kernel/iommu_groups", F_OK) == 0)
	{
		DIR		   *d = opendir("/sys/kernel/iommu_groups");
		size_t		groups = 0;

		if (d)
		{
			while (readdir(d))
				groups++;
			closedir(d);
		}
		fprintf(stderr, "  /sys/kernel/iommu_groups: %zu entries\n", groups);
	}
	else
		fprintf(stderr, "  /sys/kernel/iommu_groups: missing (%s)\n", strerror(errno));

	fprintf(stderr, "  hint: ensure DPDK runtime dir is writable and hugepages are available\n");
}

static void
attach_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	(void) ctx; (void) trid; (void) opts;
	if (!g_ctrlr)
		g_ctrlr = ctrlr;
}

static uint32_t
resolve_nshards(void)
{
	uint32_t ns = ps_nshards;

	if (ns == 0 || ns > PS_MAX_CHANNELS)
		ns = 1;
	return ns;
}

/*
 * open(): 'path' is the --store dir (for spdk_super and delegated WAL/meta
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
		spdk_emit_env_diag(pci);
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

	g_nshards = resolve_nshards();
	super_init_threads();
	super_read();				/* segment counts */

	for (uint32_t i = 0; i < g_nshards; i++)
	{
		if (thread_alloc(&g_threads[i]) != 0)
		{
			fprintf(stderr, "storage_spdk: failed to allocate shard-%u context\n", i);
			for (uint32_t j = 0; j < i; j++)
				thread_free(&g_threads[j]);
			if (g_ctrlr)
			{
				spdk_nvme_detach(g_ctrlr);
				g_ctrlr = NULL;
			}
			g_ns = NULL;
			PsStoragePosix.close();
			return -1;
		}
	}

	fprintf(stderr, "storage_spdk: %s ns1 sector=%u segsize=%llu nshards=%u\n",
			pci, g_sector, (unsigned long long) g_segsize, g_nshards);
	return 0;
}

static void
spdk_close(void)
{
	for (uint32_t i = 0; i < g_nshards; i++)
		thread_free(&g_threads[i]);
	super_write();
	if (g_ctrlr)
		spdk_nvme_detach(g_ctrlr);
	g_ctrlr = NULL;
	g_ns = NULL;
	PsStoragePosix.close();
}

static int
spdk_sync(void)
{
	for (uint32_t i = 0; i < g_nshards; i++)
		if (flush_curbuf(&g_threads[i]) != 0)
			return -1;
	super_write();
	return 0;
}

/* --- segment byte I/O ---------------------------------------------------- */

static int
spdk_seg_write(uint32_t shard, int seg, uint64_t off, const void *buf,
			   uint32_t len)
{
	PsSpdkThread *t = thread_for(shard);

	if (!t || !t->qpair)
		return -1;
	if (off + len > g_segsize)
		return -1;
	if (seg != t->curseg && load_curbuf(t, seg) != 0)
		return -1;
	memcpy(t->curbuf + off, buf, len);
	t->dirty = 1;
	if ((uint32_t) seg + 1 > t->num_segments)
		t->num_segments = (uint32_t) seg + 1;
	return 0;
}

static int
spdk_seg_read(uint32_t shard, int seg, uint64_t off, void *buf, uint32_t len)
{
	PsSpdkThread *t = thread_for(shard);
	uint64_t	byte0,
				start,
				end,
				lba;
	uint32_t	sectors;

	if (!t || !t->qpair)
		return -1;
	if (off + len > g_segsize)
		return -1;

	/* current append segment is authoritative in the buffer */
	if (seg == t->curseg)
	{
		memcpy(buf, t->curbuf + off, len);
		return 0;
	}
	if ((uint32_t) seg >= t->num_segments)
		return -1;				/* no such segment -> end of log for recover() */

	/* aligned read of the covering sectors, then slice out [off, off+len) */
	byte0 = seg_byte(t, seg, off);
	start = byte0 - (byte0 % g_sector);
	end = byte0 + len;
	if (end % g_sector)
		end += g_sector - (end % g_sector);
	lba = start / g_sector;
	sectors = (uint32_t) ((end - start) / g_sector);
	if ((uint64_t) sectors * g_sector > g_segsize)
		return -1;				/* scratch buffer is one segment */
	if (do_io(t, t->iobuf, lba, sectors, 0) != 0)
		return -1;
	memcpy(buf, t->iobuf + (byte0 - start), len);
	return 0;
}

static int64_t
spdk_seg_size(uint32_t shard, int seg)
{
	PsSpdkThread *t = thread_for(shard);

	if (!t)
		return -1;
	if ((uint32_t) seg < t->num_segments || seg == t->curseg)
		return (int64_t) g_segsize;
	return -1;
}

/*
 * Completion of one queued device read: deliver the page (zero-fill on error),
 * return the buffer to the free-list, and notify the caller.  Runs inside
 * spdk_nvme_qpair_process_completions, i.e. one qpair's callback context.
 */
static void
rd_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	PsSpdkRd   *rd = arg;
	int			ok = !spdk_nvme_cpl_is_error(cpl);
	PsSpdkThread *t = rd ? (PsSpdkThread *) rd->owner : NULL;
	int			b = (int) (rd - (t ? t->rd : NULL));

	if (!rd || !t || b < 0 || b >= PS_SPDK_POOL)
		return;
	if (!t->pool[b])
		goto out_done_zero;

	if (ok)
		memcpy(rd->dst, t->pool[b] + rd->slice, rd->len);
	else
		memset(rd->dst, 0, rd->len);
	out_done:
	if (t->nfree < PS_SPDK_POOL)
		t->free[t->nfree++] = b;
	if (rd->done)
		rd->done(rd->arg, ok);
	return;
out_done_zero:
	ok = 0;
	goto out_done;
}

int
ps_spdk_read_async(uint32_t shard, int seg, uint64_t off, void *dst,
				   uint32_t len, PsSpdkDone done, void *arg)
{
	PsSpdkThread *t = thread_for(shard);
	uint64_t	byte0,
				start,
				end;
	uint32_t	sectors;
	int			b;
	PsSpdkRd   *rd;

	if (!t || !t->qpair)
		return -1;

	/* current append segment is authoritative in memory */
	if (seg == t->curseg)
	{
		memcpy(dst, t->curbuf + off, len);
		done(arg, 1);
		return 0;
	}
	if ((uint32_t) seg >= t->num_segments)
	{
		memset(dst, 0, len);	/* no such segment */
		done(arg, 1);
		return 0;
	}

	byte0 = seg_byte(t, seg, off);
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
	while (t->nfree == 0)
		spdk_nvme_qpair_process_completions(t->qpair, 0);
	b = t->free[--t->nfree];
	rd = &t->rd[b];
	rd->dst = dst;
	rd->slice = byte0 - start;
	rd->len = len;
	rd->done = done;
	rd->arg = arg;
	rd->owner = t;
	if (spdk_nvme_ns_cmd_read(g_ns, t->qpair, t->pool[b], start / g_sector,
							  sectors, rd_cb, rd, 0) != 0)
	{
		t->free[t->nfree++] = b;	/* submission failed: give the buffer back */
		memset(dst, 0, len);
		done(arg, 0);
	}
	return 0;
}

int
ps_spdk_poll(uint32_t shard)
{
	PsSpdkThread *t = thread_for(shard);

	if (!t || !t->qpair)
		return 0;
	return spdk_nvme_qpair_process_completions(t->qpair, 0);
}

int
ps_spdk_thread_init(uint32_t shard)
{
	return thread_init(thread_for(shard));
}

void
ps_spdk_thread_close(uint32_t shard)
{
	PsSpdkThread *t = thread_for(shard);

	if (!t)
		return;
	flush_curbuf(t);
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
