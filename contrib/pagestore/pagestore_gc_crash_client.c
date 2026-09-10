/*-------------------------------------------------------------------------
 *
 * pagestore_gc_crash_client.c
 *	Deterministic IPC workload/oracle for the POSIX page-pruning H1 crash
 *	slice.
 *
 * The seed writes three generations of relation history and a newer block 0,
 * arms the named fault marker the harness hands it, then installs a
 * configured page-history owner at 3500.  That cutoff makes
 * maintenance compact the history below it: the replacement layer is
 * published, the retired sources are marked for deletion, and the durable
 * page-prune frontier advances, which are the three named process-abort
 * boundaries this slice crashes at.  The seed then waits to be reaped, so a
 * workload that ends before the fault is reported as unreached.
 *
 * The verify mode checks, after recovery, that the newest block 0 and a block
 * present only in the compacted layer read back, that the retained block 0
 * history at the cutoff is still served, and that the pruned history below
 * the cutoff cannot be resurrected once recovery cleanup has run.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"

#define TEST_REL 4343u
#define TEST_OWNER UINT64_C(23000)
#define TEST_CUTOFF UINT64_C(3500)

static void *shm_base;
static int shm_fd = -1;
static int channel = -1;
static uint32_t page_size;
static const char *arm_marker;
static const char *resume_file;

static void
die(const char *message)
{
	fprintf(stderr, "pagestore_gc_crash_client: %s\n", message);
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
	PsChannel  *ch = ps_channel(shm_base, channel);

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
	ch->blocknum = 0;
	ch->nblocks = 0;
	ch->old_nblocks = 0;
	ch->parent_timeline = 0;
}

static void
fill_page(unsigned char *page, uint64_t lsn, unsigned char tag)
{
	uint32_t	high = (uint32_t) (lsn >> 32);
	uint32_t	low = (uint32_t) lsn;

	memcpy(page, &high, sizeof(high));
	memcpy(page + sizeof(high), &low, sizeof(low));
	for (uint32_t i = 8; i < page_size; i++)
		page[i] = (unsigned char) (tag ^ (i & 0xff));
}

static int
page_has_tag(const unsigned char *page, unsigned char tag)
{
	for (uint32_t i = 8; i < page_size; i++)
		if (page[i] != (unsigned char) (tag ^ (i & 0xff)))
			return 0;
	return 1;
}

static void
write_block(unsigned char *page, uint32_t block, uint64_t lsn,
			unsigned char tag)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	fill_page(page, lsn, tag);
	set_relation(ch);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, page_size);
	if (execute()->status != PS_STATUS_OK)
		die("history write failed");
}

static void
seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);

	if (page == NULL)
		die("out of memory");
	set_relation(ch);
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = 500;
	if (execute()->status != PS_STATUS_OK)
		die("relation create failed");
	/* three generations of block 0 plus seven blocks per generation that
	 * exist only in the layers compaction rewrites */
	for (uint32_t batch = 1; batch <= 3; batch++)
	{
		write_block(page, 0, batch * 1000, (unsigned char) (batch * 10));
		for (uint32_t i = 1; i < 8; i++)
		{
			uint32_t	block = (batch - 1) * 7 + i;

			write_block(page, block, batch * 1000 + i, (unsigned char) block);
		}
	}
	write_block(page, 0, 4000, 40);
	/* the durable page cutoff that lets maintenance retire the history
	 * below it */
	set_relation(ch);
	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->blocknum = PS_RETENTION_OWNER_CONFIGURED;
	ch->parent_timeline = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	ch->old_nblocks = 1;
	ch->req_seq = TEST_OWNER;
	ch->req_lsn = TEST_CUTOFF;
	if (execute()->status != PS_STATUS_OK)
		die("page-history owner registration failed");
	/* Arm the named fault only once the cutoff is durable: the probes also
	 * run for the flush-driven compactions that had nothing to retire, so a
	 * marker created earlier could be consumed by a pass planned against the
	 * old floor and validate the wrong transition.  The registry reads the
	 * marker at probe time, so arming after daemon start is the same
	 * protocol the standalone crash cases use. */
	if (arm_marker != NULL)
	{
		int			fd = open(arm_marker, O_CREAT | O_EXCL | O_WRONLY, 0600);

		if (fd < 0)
			die("cannot arm the named fault marker");
		close(fd);
	}
	/* Maintenance was paused across the cutoff and the arming, so the first
	 * pass it runs is planned against the new floor and can reach the armed
	 * probe.  Release it now. */
	if (resume_file != NULL && unlink(resume_file) != 0)
		die("cannot release the paused maintenance loop");
	free(page);
	/* The fault fires in daemon maintenance, not in this request stream.
	 * Stay alive until the harness reaps this process, so a daemon that
	 * never reaches the boundary is reported as an unreached fault. */
	for (;;)
		pause();
}

static uint64_t
page_lsn(const unsigned char *page)
{
	uint32_t	high;
	uint32_t	low;

	memcpy(&high, page, sizeof(high));
	memcpy(&low, page + sizeof(high), sizeof(low));
	return ((uint64_t) high << 32) | low;
}

static void
read_latest(unsigned char *page, uint32_t block)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_READV;
	ch->blocknum = block;
	ch->nblocks = 1;
	if (execute()->status != PS_STATUS_OK)
		die("latest read failed after recovery");
	memcpy(page, ch->data, page_size);
}

static void
die_page(const char *message, uint32_t block, const unsigned char *page)
{
	fprintf(stderr, "pagestore_gc_crash_client: %s (block %u resolved lsn %llu, byte 8 = 0x%02x)\n",
			message, block, (unsigned long long) page_lsn(page), page[8]);
	exit(1);
}

/* 1 when a version at or below lsn is served, 0 when the daemon reports
 * none, -1 when the read itself failed.  A failure is never "pruned". */
static int
read_at(unsigned char *page, uint32_t block, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_READ_AT;
	ch->blocknum = block;
	ch->req_lsn = lsn;
	if (execute()->status != PS_STATUS_OK)
		return -1;
	if (ch->result != 0 && page != NULL)
		memcpy(page, ch->data, page_size);
	return ch->result != 0;
}

static int
read_at_found(uint32_t block, uint64_t lsn)
{
	int			found = read_at(NULL, block, lsn);

	if (found < 0)
		die("as-of read failed after recovery");
	return found;
}

static void
verify(void)
{
	unsigned char *page = malloc(page_size);
	struct timespec pause_interval = {0, 20000000};

	if (page == NULL)
		die("out of memory");
	read_latest(page, 0);
	if (!page_has_tag(page, 40))
		die_page("recovery does not serve the published newest block 0", 0, page);
	read_latest(page, 1);
	if (!page_has_tag(page, 1))
		die_page("recovery lost a block present only in the compacted layer", 1, page);
	/* The configured owner at 3500 keeps the newest block 0 at or below it. */
	if (read_at(page, 0, 3500) != 1)
		die("recovery lost the retained block 0 history at the cutoff");
	if (!page_has_tag(page, 30))
		die_page("recovery serves the wrong block 0 version at the cutoff", 0, page);
	/* Recovery resumes the interrupted cleanup asynchronously; the retired
	 * history below the cutoff must be gone within a bounded wait. */
	for (int i = 0; i < 500 && read_at_found(0, 1000); i++)
		nanosleep(&pause_interval, NULL);
	if (read_at_found(0, 1000))
		die("recovery resurrected pruned history below the cutoff");
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
		else if (strcmp(argv[i], "--arm-marker") == 0 && i + 1 < argc)
			arm_marker = argv[++i];
		else if (strcmp(argv[i], "--resume-file") == 0 && i + 1 < argc)
			resume_file = argv[++i];
		else
			die("usage: --shm NAME --mode seed|verify [--arm-marker PATH] [--resume-file PATH]");
	}
	if (shm == NULL || mode == NULL ||
		(strcmp(mode, "seed") != 0 && strcmp(mode, "verify") != 0))
		die("usage: --shm NAME --mode seed|verify [--arm-marker PATH] [--resume-file PATH]");
	attach(shm);
	if (strcmp(mode, "seed") == 0)
		seed();
	else
		verify();
	detach();
	return 0;
}
