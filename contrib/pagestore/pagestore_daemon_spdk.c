/*-------------------------------------------------------------------------
 *
 * pagestore_daemon_spdk.c
 *	  SPDK frontend for the page-store daemon (optional, higher performance).
 *
 * Reuses the shared brain pagestore_core.c verbatim; this file supplies the
 * SPDK-specific bring-up and the request loop.  SPDK is used in *library mode*:
 * we own the loop, and the SPDK storage backend's byte ops poll the NVMe qpair
 * to completion internally (S1.2c-1).  Pipelining many requests in flight across
 * channels is S1.2c-2.  The portable POSIX daemon is unaffected and remains the
 * default; this binary is built separately (spdk_build.sh) and links SPDK.
 *
 * Argument-compatible with pagestore_daemon (so the standalone test harness can
 * drive it): --shm/--store/--page-size/--segment-size.  The control disk's PCI
 * address is taken from --pci or $PS_SPDK_PCI (so the test, which only passes
 * the common args, can set it via the environment).  --store is the dir for the
 * spdk_super file and the delegated WAL/timeline metadata; page segments live on
 * the NVMe device.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"

static volatile sig_atomic_t stop_requested = 0;

static void
on_signal(int sig)
{
	(void) sig;
	stop_requested = 1;
}

/*
 * Serve one request.  Metadata ops go to the shared brain; the four byte-I/O
 * ops use the SPDK storage backend (which polls NVMe completions internally).
 */
static void
handle_request(PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	ch->status = PS_STATUS_OK;
	ch->result = 0;

	if (ps_handle_meta(ch))
		return;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_EXTEND:
			if (append_page(tl, &ch->key, ch->blocknum, ch->data) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(tl, &ch->key, ch->blocknum + 1);
			break;

		case PS_OP_WRITEV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + i,
								 ch->data + (size_t) i * page_size) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
			if (ch->status == PS_STATUS_OK)
				fork_grow(tl, &ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_READV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				unsigned char *dst = ch->data + (size_t) i * page_size;
				PageVer    *v = read_through(tl, &ch->key, ch->blocknum + i,
											 UINT64_MAX);

				if (!v || read_version(v, dst) != 0)
					memset(dst, 0, page_size);
			}
			break;

		case PS_OP_READ_AT:
			{
				PageVer    *v = read_through(tl, &ch->key, ch->blocknum,
											 ch->req_lsn);

				if (!v || read_version(v, ch->data) != 0)
					memset(ch->data, 0, page_size);
				break;
			}

		default:
			ch->status = PS_STATUS_ERROR;
			break;
	}
}

int
main(int argc, char **argv)
{
	int			fd;
	void	   *shm;
	PsShmHeader *hdr;
	struct sigaction sa;
	const char *store_dir = NULL;
	const char *shm_name = NULL;
	const char *pci_addr = NULL;

	ps_storage = &PsStorageSpdk;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--store") == 0 && i + 1 < argc)
			store_dir = argv[++i];
		else if (strcmp(argv[i], "--pci") == 0 && i + 1 < argc)
			pci_addr = argv[++i];
		else if (strcmp(argv[i], "--page-size") == 0 && i + 1 < argc)
			page_size = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--segment-size") == 0 && i + 1 < argc)
			segment_size = strtoull(argv[++i], NULL, 10);
		else
		{
			fprintf(stderr, "usage: %s --shm NAME --store DIR --pci ADDR "
					"[--page-size N] [--segment-size N]\n", argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR --pci ADDR "
				"[--page-size N] [--segment-size N]\n", argv[0]);
		return 2;
	}
	/* the PCI address reaches the storage backend through $PS_SPDK_PCI */
	if (pci_addr)
		setenv("PS_SPDK_PCI", pci_addr, 1);

	if (ps_core_open(store_dir) != 0)
	{
		fprintf(stderr, "pagestore_daemon_spdk: bring-up failed\n");
		return 1;
	}

	fd = shm_open(shm_name, O_CREAT | O_RDWR, 0600);
	if (fd < 0)
	{
		perror("shm_open");
		return 1;
	}
	if (ftruncate(fd, PS_SHM_SIZE) != 0)
	{
		perror("ftruncate shm");
		return 1;
	}
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (shm == MAP_FAILED)
	{
		perror("mmap");
		return 1;
	}
	close(fd);

	hdr = (PsShmHeader *) shm;
	memset(shm, 0, PS_SHM_SIZE);
	hdr->magic = PS_SHM_MAGIC;
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = page_size;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	fprintf(stderr, "pagestore_daemon_spdk: shm=%s store=%s storage=%s "
			"page_size=%u io_unit=%u channels=%d ready\n",
			shm_name, store_dir, ps_storage->name, page_size, PS_IO_UNIT,
			PS_MAX_CHANNELS);

	while (!stop_requested)
	{
		int			did_work = 0;

		for (uint32_t i = 0; i < PS_MAX_CHANNELS; i++)
		{
			PsChannel  *ch = ps_channel(shm, i);

			if (ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;

			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			did_work = 1;
		}

		if (!did_work)
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}

	fprintf(stderr, "pagestore_daemon_spdk: shutting down\n");
	ps_storage->close();
	munmap(shm, PS_SHM_SIZE);
	return 0;
}
