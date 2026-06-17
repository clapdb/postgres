/*-------------------------------------------------------------------------
 *
 * storage_spdk.c
 *	  SPDK (userspace NVMe) storage backend for the page-store daemon.
 *
 * Optional, higher-performance alternative to storage_posix.c behind the same
 * PsStorage interface (so the IPC ABI is unchanged and a machine without SPDK
 * just uses the POSIX backend).  Built only when PAGESTORE_SPDK is defined and
 * linked against SPDK; see spdk_build.sh.
 *
 * Status: S1.2b -- this brings up SPDK in *library mode* (spdk_env_init, no
 * spdk_app_start) and attaches the dedicated control NVMe by PCI address,
 * grabbing its namespace and an I/O qpair.  The actual byte movement
 * (seg/wal/meta) is stubbed and lands in S1.2c, where it becomes asynchronous
 * spdk_nvme_ns_cmd_* with completions driven by the daemon loop.
 *
 *-------------------------------------------------------------------------
 */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "spdk/env.h"
#include "spdk/nvme.h"

#include "pagestore_storage.h"

/* The single control device this daemon owns. */
static struct spdk_nvme_ctrlr *g_ctrlr;
static struct spdk_nvme_ns *g_ns;
static struct spdk_nvme_qpair *g_qpair;
static uint32_t g_sector_size;	/* device LBA size (e.g. 512 or 4096) */
static uint64_t g_num_sectors;	/* namespace capacity in sectors */

static bool
probe_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		 struct spdk_nvme_ctrlr_opts *opts)
{
	(void) ctx;
	(void) trid;
	(void) opts;
	return true;				/* attach every controller we probed for */
}

static void
attach_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	(void) ctx;
	(void) trid;
	(void) opts;
	if (!g_ctrlr)				/* we probe a single PCI addr; take the first */
		g_ctrlr = ctrlr;
}

/*
 * open(): 'path' is the control disk's PCI address (e.g. "0000:06:00.0").
 * Initialize the SPDK environment in library mode, attach the controller, and
 * grab namespace 1 plus an I/O qpair.
 */
static int
spdk_open(const char *path, uint64_t segment_size)
{
	struct spdk_env_opts opts;
	struct spdk_nvme_transport_id trid;

	(void) segment_size;

	spdk_env_opts_init(&opts);
	opts.name = "pagestore_daemon_spdk";
	if (spdk_env_init(&opts) < 0)
	{
		fprintf(stderr, "storage_spdk: spdk_env_init failed\n");
		return -1;
	}

	memset(&trid, 0, sizeof(trid));
	spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
	snprintf(trid.traddr, sizeof(trid.traddr), "%s", path);

	if (spdk_nvme_probe(&trid, NULL, probe_cb, attach_cb, NULL) != 0 || !g_ctrlr)
	{
		fprintf(stderr, "storage_spdk: could not attach NVMe at %s "
				"(bound to vfio-pci? see spdk_setup.sh)\n", path);
		return -1;
	}

	g_ns = spdk_nvme_ctrlr_get_ns(g_ctrlr, 1);
	if (!g_ns || !spdk_nvme_ns_is_active(g_ns))
	{
		fprintf(stderr, "storage_spdk: namespace 1 not active on %s\n", path);
		return -1;
	}
	g_sector_size = spdk_nvme_ns_get_sector_size(g_ns);
	g_num_sectors = spdk_nvme_ns_get_num_sectors(g_ns);

	g_qpair = spdk_nvme_ctrlr_alloc_io_qpair(g_ctrlr, NULL, 0);
	if (!g_qpair)
	{
		fprintf(stderr, "storage_spdk: could not allocate I/O qpair\n");
		return -1;
	}

	fprintf(stderr, "storage_spdk: attached %s ns1 sector=%u sectors=%llu "
			"capacity=%lluMiB\n", path, g_sector_size,
			(unsigned long long) g_num_sectors,
			(unsigned long long) (g_num_sectors * g_sector_size >> 20));
	return 0;
}

static void
spdk_close(void)
{
	if (g_qpair)
		spdk_nvme_ctrlr_free_io_qpair(g_qpair);
	if (g_ctrlr)
		spdk_nvme_detach(g_ctrlr);
	g_qpair = NULL;
	g_ctrlr = NULL;
	g_ns = NULL;
}

/*
 * Byte movement -- stubbed for S1.2b.  S1.2c implements these as asynchronous
 * spdk_nvme_ns_cmd_read/write over a fixed-region layout (segment id + offset
 * -> LBA), with DMA buffers and completions polled by the daemon loop.
 */
#define SPDK_TODO(name) \
	do { \
		fprintf(stderr, "storage_spdk: " name " not implemented yet (S1.2c)\n"); \
	} while (0)

static int
spdk_sync(void)
{
	return 0;					/* flush handled at S1.2c with the write path */
}

static int
spdk_seg_write(int seg, uint64_t off, const void *buf, uint32_t len)
{
	(void) seg; (void) off; (void) buf; (void) len;
	SPDK_TODO("seg_write");
	return -1;
}

static int
spdk_seg_read(int seg, uint64_t off, void *buf, uint32_t len)
{
	(void) seg; (void) off; (void) buf; (void) len;
	SPDK_TODO("seg_read");
	return -1;
}

static int64_t
spdk_seg_size(int seg)
{
	(void) seg;
	return -1;					/* no segments yet -> recover() finds an empty store */
}

static int
spdk_wal_append(uint32_t tl, const void *a, uint32_t alen,
				const void *b, uint32_t blen)
{
	(void) tl; (void) a; (void) alen; (void) b; (void) blen;
	SPDK_TODO("wal_append");
	return -1;
}

static int
spdk_wal_read(uint32_t tl, uint64_t off, void *buf, uint32_t len)
{
	(void) tl; (void) off; (void) buf; (void) len;
	return 0;					/* empty -> end of log */
}

static int
spdk_meta_append(const void *buf, uint32_t len)
{
	(void) buf; (void) len;
	SPDK_TODO("meta_append");
	return -1;
}

static int
spdk_meta_read(uint64_t off, void *buf, uint32_t len)
{
	(void) off; (void) buf; (void) len;
	return 0;					/* empty -> no timelines */
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
