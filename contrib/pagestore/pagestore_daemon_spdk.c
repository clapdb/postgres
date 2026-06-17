/*-------------------------------------------------------------------------
 *
 * pagestore_daemon_spdk.c
 *	  SPDK frontend for the page-store daemon (optional, higher performance).
 *
 * Reuses the shared brain pagestore_core.c verbatim; this file supplies the
 * SPDK-specific bring-up and (in S1.2c) an asynchronous request loop.  SPDK is
 * used in *library mode*: we own the main loop and call spdk_thread_poll() to
 * drive completions, rather than handing the loop to spdk_app_start().  The
 * portable POSIX daemon (pagestore_daemon.c) is unaffected and remains the
 * default; this binary is built separately (spdk_build.sh) and links SPDK.
 *
 * Status: S1.2b -- brings up the SPDK environment and the control disk through
 * the PsStorage interface and reports readiness.  The shared-memory channel
 * loop with asynchronous spdk_nvme I/O is S1.2c.
 *
 * Usage: pagestore_daemon_spdk --pci 0000:06:00.0
 *                              [--page-size N] [--segment-size N]
 *
 *-------------------------------------------------------------------------
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"

int
main(int argc, char **argv)
{
	const char *pci_addr = NULL;

	ps_storage = &PsStorageSpdk;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--pci") == 0 && i + 1 < argc)
			pci_addr = argv[++i];
		else if (strcmp(argv[i], "--page-size") == 0 && i + 1 < argc)
			page_size = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--segment-size") == 0 && i + 1 < argc)
			segment_size = strtoull(argv[++i], NULL, 10);
		else
		{
			fprintf(stderr, "usage: %s --pci ADDR "
					"[--page-size N] [--segment-size N]\n", argv[0]);
			return 2;
		}
	}
	if (!pci_addr || page_size == 0 || page_size > PS_IO_UNIT)
	{
		fprintf(stderr, "usage: %s --pci ADDR "
				"[--page-size N] [--segment-size N]\n", argv[0]);
		return 2;
	}

	/* open the control disk via SPDK and rebuild in-memory state */
	if (ps_core_open(pci_addr) != 0)
	{
		fprintf(stderr, "pagestore_daemon_spdk: bring-up failed\n");
		return 1;
	}

	fprintf(stderr, "pagestore_daemon_spdk: storage=%s page_size=%u ready "
			"(channel loop + async I/O: S1.2c)\n", ps_storage->name, page_size);

	/*
	 * S1.2c: shm_open the channel region, then loop polling channels and
	 * spdk_thread_poll(); serve metadata via ps_handle_meta and the four
	 * byte-I/O ops via asynchronous spdk_nvme reads/writes with per-channel
	 * in-flight tracking.  For now bring-up is the deliverable.
	 */
	ps_storage->close();
	return 0;
}
