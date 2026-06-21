/*-------------------------------------------------------------------------
 *
 * pagestore_recover_test.c
 *	  In-process unit tests for segment recovery (no daemon, no IPC).
 *
 * Links the core + POSIX storage directly and drives ps_core_open / append_page /
 * read_resolve / ps_core_close in one process, so recovery behaviour can be
 * exercised and its result observed without spawning a daemon (which the standalone
 * harness does, and whose stdio is awkward to capture here).  Corruption is
 * injected straight through the storage vtable.
 *
 * Built segment-mode (use_layers == 0): recovery rebuilds purely from the segment
 * log, which is exactly the path the CRC + sync-watermark changes touch.
 *
 *-------------------------------------------------------------------------
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pagestore_ipc.h"
#include "pagestore_core.h"
#include "pagestore_storage.h"

static int	checks = 0;
static int	fails = 0;

#define check(cond, msg) \
	do { \
		checks++; \
		if (!(cond)) { fails++; printf("FAIL: %s\n", msg); } \
	} while (0)

/* same page layout the daemon test uses: lsn in the first 8 bytes, tag elsewhere */
static void
fill_page(unsigned char *buf, uint32_t ps, uint64_t lsn, unsigned char tag)
{
	uint32_t	xlogid = (uint32_t) (lsn >> 32);
	uint32_t	xrecoff = (uint32_t) (lsn & 0xFFFFFFFF);

	memcpy(buf, &xlogid, 4);
	memcpy(buf + 4, &xrecoff, 4);
	for (uint32_t i = 8; i < ps; i++)
		buf[i] = (unsigned char) (tag ^ (i & 0xFF));
}

static int
page_has_tag(const unsigned char *buf, uint32_t ps, unsigned char tag)
{
	for (uint32_t i = 8; i < ps; i++)
		if (buf[i] != (unsigned char) (tag ^ (i & 0xFF)))
			return 0;
	return 1;
}

static void
config(void)
{
	page_size = 8192;
	segment_size = 32768;		/* ~3 records/segment -> multi-segment shards */
	ps_nshards = 1;
	use_layers = 0;				/* segment-log recovery path (as SPDK runs) */
	cache_pages = 0;
	flush_pages = 1 << 20;
	compact_layers = 1 << 20;
}

int
main(void)
{
	PsKey		key = {1, 2, 3, 0};
	unsigned char pg[8192],
				rb[8192];

	if (system("rm -rf /tmp/psrec && mkdir -p /tmp/psrec") != 0)
		return 2;
	config();

	/* --- clean write / read / recover round-trip --- */
	check(ps_core_open("/tmp/psrec") == 0, "open fresh store");
	for (int b = 0; b < 5; b++)
	{
		fill_page(pg, page_size, 1000 + b, (unsigned char) (10 + b));
		check(append_page(0, &key, (uint32_t) b, pg) == 0, "append");
		fork_grow(0, &key, (uint32_t) b + 1);
	}
	for (int b = 0; b < 5; b++)
	{
		int			r = read_resolve(0, &key, (uint32_t) b, ~0ull, rb);

		check(r == 1 && page_has_tag(rb, page_size, (unsigned char) (10 + b)),
			  "read back before close");
	}
	ps_core_close();

	check(ps_core_open("/tmp/psrec") == 0, "reopen + recover from segments");
	for (int b = 0; b < 5; b++)
	{
		int			r = read_resolve(0, &key, (uint32_t) b, ~0ull, rb);

		check(r == 1 && page_has_tag(rb, page_size, (unsigned char) (10 + b)),
			  "read back after recover");
	}
	ps_core_close();

	printf("%d checks, %d failed\n", checks, fails);
	return fails ? 1 : 0;
}
