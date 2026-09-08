/*-------------------------------------------------------------------------
 *
 * pagestore_layer_crash_client.c
 *	Deterministic IPC workload/oracle for the POSIX image-layer H1 crash slice.
 *
 * The seed writes two relation pages.  With --flush-pages 2 the second write
 * drives exactly one image-layer publication.  The verify mode checks both
 * sentinel bytes and the resolved page LSN after recovery.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "pagestore_ipc.h"

#define TEST_REL 4242u
#define TEST_LSN0 UINT64_C(0x1000)
#define TEST_LSN1 UINT64_C(0x2000)

static void *shm_base;
static int shm_fd = -1;
static int channel = -1;
static uint32_t page_size;

static void
die(const char *message)
{
	fprintf(stderr, "pagestore_layer_crash_client: %s\n", message);
	exit(1);
}

static void
attach(const char *name)
{
	PsShmHeader *header;

	shm_fd = shm_open(name, O_RDWR, 0600);
	if (shm_fd < 0)
		die("cannot open shared memory");
	shm_base = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
					shm_fd, 0);
	if (shm_base == MAP_FAILED)
		die("cannot map shared memory");
	header = (PsShmHeader *) shm_base;
	if (header->magic != PS_SHM_MAGIC ||
		ps_load_acquire(&header->startup_state) != PS_SHM_READY ||
		header->nshards != 1)
		die("daemon is not ready for the single-shard H1 workload");
	page_size = header->page_size;
	if (page_size == 0 || page_size > PS_IO_UNIT)
		die("invalid daemon page size");
	for (uint32_t i = 0; i < header->nchannels; i++)
		if (ps_cas(&ps_channel(shm_base, i)->claimed, 0, 1))
		{
			channel = (int) i;
			return;
		}
	die("no free daemon channel");
}

static void
detach(void)
{
	if (shm_base != NULL && shm_base != MAP_FAILED)
	{
		if (channel >= 0)
			ps_store_release(&ps_channel(shm_base, channel)->claimed, 0);
		munmap(shm_base, PS_SHM_SIZE);
	}
	if (shm_fd >= 0)
		close(shm_fd);
}

static PsChannel *
execute(void)
{
	PsChannel *ch = ps_channel(shm_base, channel);

	ps_request_generation_next(ch);
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		;
	return ch;
}

static void
set_relation(PsChannel *ch)
{
	memset(&ch->key, 0, sizeof(ch->key));
	ch->key.spcOid = 1;
	ch->key.dbOid = 1;
	ch->key.relNumber = TEST_REL;
	ch->key.forkNum = 0;
	ch->key.klass = PS_KLASS_RELATION;
	ch->timeline = 0;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	ch->incarnation = 0;
}

static void
fill_page(unsigned char *page, uint64_t lsn, unsigned char tag)
{
	uint32_t high = (uint32_t) (lsn >> 32);
	uint32_t low = (uint32_t) lsn;

	memcpy(page, &high, sizeof(high));
	memcpy(page + sizeof(high), &low, sizeof(low));
	for (uint32_t i = 8; i < page_size; i++)
		page[i] = (unsigned char) (tag ^ (i & 0xff));
}

static int
page_matches(const unsigned char *page, uint64_t lsn, unsigned char tag)
{
	uint32_t high;
	uint32_t low;

	memcpy(&high, page, sizeof(high));
	memcpy(&low, page + sizeof(high), sizeof(low));
	if ((((uint64_t) high << 32) | low) != lsn)
		return 0;
	for (uint32_t i = 8; i < page_size; i++)
		if (page[i] != (unsigned char) (tag ^ (i & 0xff)))
			return 0;
	return 1;
}

static void
seed(void)
{
	PsChannel *ch = ps_channel(shm_base, channel);
	unsigned char *pages = malloc((size_t) page_size * 2);

	if (pages == NULL)
		die("out of memory");
	fill_page(pages, TEST_LSN0, 0xa1);
	fill_page(pages + page_size, TEST_LSN1, 0xb2);
	set_relation(ch);
	ch->opcode = PS_OP_CREATE;
	if (execute()->status != PS_STATUS_OK)
		die("relation create failed");
	set_relation(ch);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = 0;
	ch->nblocks = 2;
	memcpy(ch->data, pages, (size_t) page_size * 2);
	if (execute()->status != PS_STATUS_OK)
		die("layer-seeding write failed");
	free(pages);
}

static void
verify(void)
{
	PsChannel *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);
	const uint64_t lsns[] = {TEST_LSN0, TEST_LSN1};
	const unsigned char tags[] = {0xa1, 0xb2};

	if (page == NULL)
		die("out of memory");
	for (uint32_t block = 0; block < 2; block++)
	{
		set_relation(ch);
		ch->opcode = PS_OP_READ_AT;
		ch->blocknum = block;
		ch->req_lsn = TEST_LSN1 + 1;
		if (execute()->status != PS_STATUS_OK || ch->result != 1 ||
			ch->req_lsn != lsns[block])
			die("sentinel read did not return the expected page LSN");
		memcpy(page, ch->data, page_size);
		if (!page_matches(page, lsns[block], tags[block]))
			die("sentinel page contents changed across recovery");
	}
	free(page);
}

int
main(int argc, char **argv)
{
	const char *shm = NULL;
	const char *mode = NULL;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm = argv[++i];
		else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc)
			mode = argv[++i];
		else
			die("usage: --shm NAME --mode seed|verify");
	}
	if (shm == NULL || mode == NULL ||
		(strcmp(mode, "seed") != 0 && strcmp(mode, "verify") != 0))
		die("usage: --shm NAME --mode seed|verify");
	attach(shm);
	if (strcmp(mode, "seed") == 0)
		seed();
	else
		verify();
	detach();
	return 0;
}
