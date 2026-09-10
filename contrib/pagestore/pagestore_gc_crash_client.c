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
 * The wal_index workload targets the WAL-index compaction boundary instead:
 * a fixed WAL-index reader at 40 under three FPI-led chains on one block and
 * one committed interval, armed before the commit that makes the interval a
 * snapshot candidate.  Its verify mode waits for the retried generation to
 * serve the reader's chain plus the newest chain and requires the dropped
 * point below the durable frontier to stay refused.
 *
 * The wal_reclaim workload ships three sealed 1 MiB segments, publishes a
 * control note at the shipped end, arms the fault, and commits WAL-index
 * progress through the end so the whole prefix is reclaimable.  Its verify
 * mode waits for the prefix reads to be refused and requires the WAL end and
 * retain floor to stay at the shipped end.
 *
 * The timeline_delete workload creates a branch with private shipped WAL, a
 * committed WAL-index interval, an owner layer, and shared-segment pages,
 * arms the fault, and issues BEGIN_DELETE.  Its verify mode waits for
 * DELETED, keeps the parent's page readable, and requires branch reads to be
 * rejected.
 *
 * The manifest_compact workload writes 320 pages while the harness holds
 * maintenance paused, then arms the fault and releases maintenance so layer
 * compaction churn grows the manifest log past its rewrite trigger.  Its
 * verify mode reads every page back.
 *
 * The forkmeta workload layers persisted fork-size events for many relations
 * on the page_prune history, arms the fault, installs the cutoff pin, and
 * keeps trickling fork events so a second generation retires the first.
 * Its verify mode checks current sizes, retained size history above the
 * cutoff, and refused queries below it, on top of the page_prune oracle.
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

#include <errno.h>

#include "pagestore_ipc.h"

#define TEST_REL 4343u
#define TEST_OWNER UINT64_C(23000)
#define TEST_CUTOFF UINT64_C(3500)

/* wal_index workload: one metadata-complete WAL-index interval on timeline 0
 * with a fixed WAL-index reader below the operational chain.  Compaction
 * keeps the reader's exact FPI-led chain (10, 30) and the newest chain
 * (90, 110) and drops the middle chain (50, 70). */
#define WALIDX_BLOCK 12u
#define WALIDX_WAL_BYTES 512u
#define WALIDX_READER UINT64_C(5001)
#define WALIDX_READER_LSN UINT64_C(40)
#define WALIDX_DROPPED_LSN UINT64_C(60)

/* wal_reclaim workload: three complete 1 MiB shipped-WAL segments on timeline
 * 0 whose control note and WAL-index progress both reach the end, so the
 * whole sealed prefix is reclaimable. */
#define RECLAIM_SEGMENT (UINT64_C(1024) * 1024)
#define RECLAIM_SEGMENTS 3u
#define RECLAIM_TOTAL (RECLAIM_SEGMENT * RECLAIM_SEGMENTS)
#define RECLAIM_CHUNK (64u * 1024u)

/* timeline_delete workload: a branch of timeline 0 with its own shipped WAL,
 * WAL-index interval, and flushed relation pages, then BEGIN_DELETE. */
#define DELETE_BRANCH 1u
/* aligned to the immutable segment size so the branch's shipped WAL seals a
 * complete wal_segments_<tl> segment the deletion has to reclaim too */
#define DELETE_FORK_LSN RECLAIM_SEGMENT
#define DELETE_WAL_SEGMENT_CHUNKS 16u
#define DELETE_SURVIVOR_BRANCH 2u
#define DELETE_WAL_BYTES 65536u
#define DELETE_PAGES 24u
/* a fresh branch of a fresh store gets its first incarnation token; the
 * verify oracle compares recovery against that seeded value */
#define DELETE_INCARNATION UINT64_C(1)

/* manifest_compact workload: enough flushed layers and compaction churn,
 * released from a paused maintenance loop, that the manifest log is rewritten. */
#define MANIFEST_PAGES 320u

/* forkmeta workload: the page_prune history plus persisted fork-size events
 * (create, zero-extend, truncate) on many relations, so the fork-metadata
 * log exceeds the snapshot trigger once the page frontier proves a cutoff. */
#define FORKMETA_FIRST_REL 5000u
#define FORKMETA_RELS 32u
#define FORKMETA_TRICKLE_REL 7000u
#define FORKMETA_TRICKLE_RELS 400u

/* fixture workload: every persisted family on one store, then a clean exit.
 * The shipped WAL keeps one sealed segment because the control note's redo
 * sits inside it, so the WAL segment format is part of the fixture. */
#define FIXTURE_WAL_END (RECLAIM_SEGMENT + 64u * 1024u)
#define FIXTURE_WAL_REDO (RECLAIM_SEGMENT / 2)
#define FIXTURE_BRANCH 1u
#define FIXTURE_DELETED_BRANCH 2u
/* Above the WAL end: maintenance may publish the parent's WAL-index frontier
 * at any point up to FIXTURE_WAL_END before the branches are created, and a
 * fork below a published frontier is refused. */
#define FIXTURE_FORK_LSN (FIXTURE_WAL_END + UINT64_C(65536))
#define FIXTURE_BRANCH_LSN (FIXTURE_FORK_LSN + UINT64_C(4000))
#define FIXTURE_CLAMPED_LSN (FIXTURE_FORK_LSN - UINT64_C(4000))
#define FIXTURE_CLAMPED_BLOCK 3u
#define FIXTURE_WALLESS_BLOCK 4u
#define FIXTURE_TAIL_REL 8000u
#define FIXTURE_TAIL_LSN UINT64_C(9000)

static void *shm_base;
static int shm_fd = -1;
static int channel = -1;
static uint32_t page_size;
static const char *arm_marker;
static const char *resume_file;
/* where the seed records the admission sequence its reservation was granted,
 * so the oracle can require the durable frontier to carry it */
static const char *cutoff_seq_file;
static const char *workload = "page_prune";

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
arm_fault(void)
{
	int			fd;

	if (arm_marker != NULL)
	{
		fd = open(arm_marker, O_CREAT | O_EXCL | O_WRONLY, 0600);
		if (fd < 0)
			die("cannot arm the named fault marker");
		close(fd);
	}
	/* Maintenance was paused while this seed installed the condition its
	 * boundary needs; releasing it here means the first pass it runs is
	 * planned against that condition and can reach the armed probe. */
	if (resume_file != NULL && unlink(resume_file) != 0 && errno != ENOENT)
		die("cannot release the paused maintenance loop");
}

static void
wait_forever(void)
{
	/* The fault fires in daemon maintenance, not in this request stream.
	 * Stay alive until the harness reaps this process, so a daemon that
	 * never reaches the boundary is reported as an unreached fault. */
	for (;;)
		pause();
}

static void
page_history_seed(unsigned char *page)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

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
}

/* The durable page cutoff that lets maintenance retire the history below
 * it, and the page frontier that proves the fork-metadata cutoff. */
static void
page_cutoff_pin(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->blocknum = PS_RETENTION_OWNER_CONFIGURED;
	ch->parent_timeline = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	ch->old_nblocks = 1;
	ch->req_seq = TEST_OWNER;
	ch->req_lsn = TEST_CUTOFF;
	if (execute()->status != PS_STATUS_OK)
		die("page-history owner registration failed");
	/* The reservation answers with the admission sequence it was granted;
	 * the durable frontier the pruning pass publishes must carry exactly
	 * that sequence at the cutoff, not merely the same LSN. */
	if (ch->datalen != sizeof(uint64_t))
		die("page-history owner registration did not report its sequence");
	{
		uint64_t	granted;

		memcpy(&granted, ch->data, sizeof(granted));
		if (granted == 0)
			die("page-history owner registration reported sequence zero");
		if (cutoff_seq_file != NULL)
		{
			FILE	   *out = fopen(cutoff_seq_file, "w");

			if (out == NULL ||
				fprintf(out, "%llu\n", (unsigned long long) granted) < 0 ||
				fflush(out) != 0 || fsync(fileno(out)) != 0 || fclose(out) != 0)
				die("cannot publish the granted admission sequence");
		}
	}
}

static void
seed(void)
{
	unsigned char *page = malloc(page_size);

	if (page == NULL)
		die("out of memory");
	page_history_seed(page);
	page_cutoff_pin();
	/* Arm the named fault only once the cutoff is durable: the probes also
	 * run for the flush-driven compactions that had nothing to retire, so a
	 * marker created earlier could be consumed by a pass planned against the
	 * old floor and validate the wrong transition.  The registry reads the
	 * marker at probe time, so arming after daemon start is the same
	 * protocol the standalone crash cases use. */
	arm_fault();
	free(page);
	wait_forever();
}

/* ---- forkmeta workload ------------------------------------------------- */

static void verify(void);

static void
set_forkmeta_relation(PsChannel *ch, uint32_t index)
{
	set_relation(ch);
	ch->key.relNumber = FORKMETA_FIRST_REL + index;
}

/* Even relations truncate below the cutoff, odd ones above it, so the
 * snapshot compacts one half and retains the other half's history. */
static uint32_t
forkmeta_expected_size(uint32_t index)
{
	return index % 2 == 0 ? 2 : 3;
}

static void
forkmeta_create_grow(uint32_t rel, uint64_t lsn, uint32_t nblocks)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->key.relNumber = rel;
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = lsn;
	if (execute()->status != PS_STATUS_OK)
		die("fork create failed");
	set_relation(ch);
	ch->key.relNumber = rel;
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = 0;
	ch->nblocks = nblocks;
	ch->req_lsn = lsn + 1000;
	if (execute()->status != PS_STATUS_OK)
		die("fork zero-extend failed");
}

/* Persisted fork-size events the segment log cannot re-derive: a create and
 * an allocation-only growth below the page cutoff for every relation, then
 * a truncate below the cutoff (even) or above it (odd). */
static void
forkmeta_events_seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	for (uint32_t i = 0; i < FORKMETA_RELS; i++)
	{
		forkmeta_create_grow(FORKMETA_FIRST_REL + i, 1000 + i, 4);
		set_forkmeta_relation(ch, i);
		ch->opcode = PS_OP_TRUNCATE;
		ch->nblocks = forkmeta_expected_size(i);
		ch->req_lsn = i % 2 == 0 ? 2500 + i : 4500 + i;
		if (execute()->status != PS_STATUS_OK)
			die("fork truncate failed");
	}
}

static void
forkmeta_seed(void)
{
	unsigned char *page = malloc(page_size);

	if (page == NULL)
		die("out of memory");
	page_history_seed(page);
	forkmeta_events_seed();
	free(page);
	arm_fault();
	page_cutoff_pin();
	/* Keep appending fork-size events after the cutoff is proven, so a
	 * second generation is published and the first one is retired; this is
	 * the only way the snapshot GC boundary is reached.  Relations created
	 * here are not part of the oracle. */
	{
		struct timespec pause_interval = {0, 20000000};

		for (uint32_t j = 0; j < FORKMETA_TRICKLE_RELS; j++)
		{
			forkmeta_create_grow(FORKMETA_TRICKLE_REL + j, 6000 + j, 2);
			nanosleep(&pause_interval, NULL);
		}
	}
	wait_forever();
}

static void
forkmeta_check(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	for (uint32_t i = 0; i < FORKMETA_RELS; i++)
	{
		set_forkmeta_relation(ch, i);
		ch->opcode = PS_OP_NBLOCKS;
		if (execute()->status != PS_STATUS_OK ||
			ch->result != forkmeta_expected_size(i))
		{
			fprintf(stderr, "pagestore_gc_crash_client: relation %u has %u "
					"blocks after recovery, expected %u\n",
					FORKMETA_FIRST_REL + i, (unsigned) ch->result,
					forkmeta_expected_size(i));
			exit(1);
		}
		/* the growth to four blocks is retained above the cutoff for the
		 * odd relations, whose truncate comes later */
		set_forkmeta_relation(ch, i);
		ch->opcode = PS_OP_NBLOCKS;
		ch->req_lsn = 4200 + i;
		if (execute()->status != PS_STATUS_OK ||
			ch->result != (i % 2 == 0 ? 2u : 4u))
			die("recovery lost the fork size history retained above the cutoff");
		/* history below the durable cutoff is refused, never guessed */
		set_forkmeta_relation(ch, i);
		ch->opcode = PS_OP_NBLOCKS;
		ch->req_lsn = 2200 + i;
		if (execute()->status == PS_STATUS_OK)
			die("recovery answered a fork size query below the durable cutoff");
	}
}

static void
forkmeta_verify(void)
{
	verify();
	forkmeta_check();
}

/* ---- wal_index workload ------------------------------------------------ */

/* The WAL-index chains sit at base + {10, 30, 50, 70, 90, 110}; the fixed
 * reader at base + 40 keeps the first chain exact. */
static void
walidx_pin_reader(uint64_t base)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_RETENTION_PIN_RESERVE;
	ch->blocknum = PS_RETENTION_OWNER_READER;
	ch->parent_timeline = PS_RETENTION_RESOURCE_WAL_INDEX;
	ch->old_nblocks = 1;
	ch->req_seq = WALIDX_READER;
	ch->req_lsn = base + WALIDX_READER_LSN;
	if (execute()->status != PS_STATUS_OK)
		die("fixed WAL-index reader registration failed");
}

static void
walidx_batch_add(uint64_t base)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;
	const uint64_t lsns[] = {10, 30, 50, 70, 90, 110};
	const uint32_t flags[] = {
		PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI,
		PS_WAL_INDEX_FLAG_KNOWN,
		PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI,
		PS_WAL_INDEX_FLAG_KNOWN,
		PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI,
		PS_WAL_INDEX_FLAG_KNOWN
	};

	set_relation(ch);
	for (uint32_t i = 0; i < 6; i++)
	{
		entries[i].key = ch->key;
		entries[i].block = WALIDX_BLOCK;
		entries[i].flags = flags[i];
		entries[i].lsn = base + lsns[i];
		entries[i].end_lsn = base + lsns[i] + 1;
	}
	ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
	ch->nblocks = 6;
	ch->datalen = 6 * sizeof(*entries);
	if (execute()->status != PS_STATUS_OK)
		die("WAL-index batch add failed");
}

static void
walidx_commit(uint64_t end_lsn)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = 0;
	ch->req_seq = end_lsn;
	if (execute()->status != PS_STATUS_OK)
		die("WAL-index progress commit failed");
}

static void
walidx_seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = 0;
	ch->datalen = WALIDX_WAL_BYTES;
	memset(ch->data, 0, WALIDX_WAL_BYTES);
	if (execute()->status != PS_STATUS_OK)
		die("WAL append failed");
	walidx_pin_reader(0);
	/* Arm before the commit that makes the interval a snapshot candidate:
	 * nothing is published before progress moves, so this is the only
	 * publication the daemon can reach. */
	walidx_batch_add(0);
	arm_fault();
	walidx_commit(WALIDX_WAL_BYTES);
	wait_forever();
}

static void
walidx_batch_and_commit(uint64_t base, uint64_t end_lsn)
{
	walidx_batch_add(base);
	walidx_commit(end_lsn);
}

/* ---- manifest_compact workload ----------------------------------------- */

static void read_latest(unsigned char *page, uint32_t block);
static void die_page(const char *message, uint32_t block,
					 const unsigned char *page);
static void delete_seed_survivor(unsigned char *page);
static void delete_verify_survivor(unsigned char *page);
static void delete_verify_live(void);
static uint64_t page_lsn(const unsigned char *page);
static int walidx_get(uint64_t lsn_max, PsWalRec *out, uint32_t max_out, int *count);



static unsigned char
manifest_tag(uint32_t block)
{
	return (unsigned char) (block * 7 + 1);
}

static void
manifest_seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);

	if (page == NULL)
		die("out of memory");
	set_relation(ch);
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = 100;
	if (execute()->status != PS_STATUS_OK)
		die("relation create failed");
	/* Maintenance is paused by the harness while these land: the write path
	 * still flushes full memtables into layers (ADD and watermark records),
	 * but layer compaction and the manifest rewrite wait for the release. */
	for (uint32_t block = 0; block < MANIFEST_PAGES; block++)
		write_block(page, block, 1000 + block, manifest_tag(block));
	free(page);
	arm_fault();
	wait_forever();
}

static void
manifest_verify(void)
{
	unsigned char *page = malloc(page_size);

	if (page == NULL)
		die("out of memory");
	for (uint32_t block = 0; block < MANIFEST_PAGES; block++)
	{
		read_latest(page, block);
		/* the tag repeats every 256 blocks, so the page's unique LSN is what
		 * proves this block is not an alias of another one */
		if (!page_has_tag(page, manifest_tag(block)) ||
			page_lsn(page) != 1000 + block)
			die_page("recovery does not serve a page written before the "
					 "manifest rewrite", block, page);
	}
	free(page);
}

/* ---- timeline_delete workload ------------------------------------------ */

static uint64_t branch_incarnation;

static void
set_timeline(PsChannel *ch, uint32_t timeline, uint64_t incarnation)
{
	ch->timeline = timeline;
	ch->incarnation = incarnation;
}

static void
delete_write_block(unsigned char *page, uint32_t timeline, uint64_t incarnation,
				   uint32_t block, uint64_t lsn, unsigned char tag)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	fill_page(page, lsn, tag);
	set_relation(ch);
	set_timeline(ch, timeline, incarnation);
	ch->opcode = PS_OP_WRITEV;
	ch->blocknum = block;
	ch->nblocks = 1;
	memcpy(ch->data, page, page_size);
	if (execute()->status != PS_STATUS_OK)
		die("branch page write failed");
}

static void
delete_wal_append(uint32_t timeline, uint64_t incarnation, uint64_t lsn)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	set_timeline(ch, timeline, incarnation);
	ch->opcode = PS_OP_WAL_APPEND;
	ch->req_lsn = lsn;
	ch->datalen = DELETE_WAL_BYTES;
	memset(ch->data, (int) (timeline + 1), DELETE_WAL_BYTES);
	if (execute()->status != PS_STATUS_OK)
		die("WAL append failed");
}

static int
timeline_state(uint32_t timeline, uint64_t *incarnation)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->timeline = timeline;
	ch->opcode = PS_OP_TIMELINE_STATE;
	if (execute()->status != PS_STATUS_OK)
		return -1;
	if (incarnation != NULL)
		*incarnation = ch->req_seq;
	return (int) ch->result;
}

static void
delete_seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);
	uint64_t	parent_incarnation = 0;

	if (page == NULL)
		die("out of memory");
	set_relation(ch);
	ch->opcode = PS_OP_CREATE;
	ch->req_lsn = 100;
	if (execute()->status != PS_STATUS_OK)
		die("relation create failed");
	delete_write_block(page, 0, 0, 0, 200, 7);
	delete_wal_append(0, 0, 0);
	if (timeline_state(0, &parent_incarnation) != PS_TIMELINE_LIVE)
		die("timeline 0 is not live");
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, 0);
	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->parent_timeline = 0;
	ch->req_lsn = DELETE_FORK_LSN;
	ch->req_seq = parent_incarnation;
	if (execute()->status != PS_STATUS_OK)
		die("branch create failed");
	branch_incarnation = ch->incarnation;
	if (branch_incarnation != DELETE_INCARNATION)
		die("branch did not receive its expected first incarnation token");
	/* private shipped WAL, a complete immutable segment of it, plus a
	 * committed WAL-index interval */
	for (uint32_t chunk = 0; chunk < DELETE_WAL_SEGMENT_CHUNKS; chunk++)
		delete_wal_append(DELETE_BRANCH, branch_incarnation,
						  DELETE_FORK_LSN + (uint64_t) chunk * DELETE_WAL_BYTES);
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, branch_incarnation);
	{
		PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;

		entries[0].key = ch->key;
		entries[0].block = 0;
		entries[0].flags = PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI;
		entries[0].lsn = DELETE_FORK_LSN + 16;
		entries[0].end_lsn = DELETE_FORK_LSN + 17;
	}
	ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
	ch->nblocks = 1;
	ch->datalen = sizeof(PsWalIndexEntry);
	if (execute()->status != PS_STATUS_OK)
		die("branch WAL-index add failed");
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, branch_incarnation);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = DELETE_FORK_LSN;
	ch->req_seq = DELETE_FORK_LSN + DELETE_WAL_BYTES;
	if (execute()->status != PS_STATUS_OK)
		die("branch WAL-index progress commit failed");
	/* A live sibling with the same kind of private state: owner-scoped
	 * cleanup that reached past its owner would take these with it, and no
	 * scenario would notice while the root has none of them. */
	delete_seed_survivor(page);
	/* enough branch pages in shared segments that maintenance flushes an
	 * owner layer and the deletion must rewrite segments and retire a layer */
	for (uint32_t block = 0; block < DELETE_PAGES; block++)
		delete_write_block(page, DELETE_BRANCH, branch_incarnation, block,
						   DELETE_FORK_LSN + 1000 + block,
						   (unsigned char) (0x40 + block));
	/* a zero-extend is the one growth with no page record, so it leaves an
	 * owner-scoped fork-size event the deletion must settle while the
	 * parent's own fork metadata stays untouched */
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, branch_incarnation);
	ch->opcode = PS_OP_ZEROEXTEND;
	ch->blocknum = DELETE_PAGES;
	ch->nblocks = 1;
	ch->req_lsn = DELETE_FORK_LSN + 2000;
	if (execute()->status != PS_STATUS_OK)
		die("branch zero-extend failed");
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, branch_incarnation);
	ch->opcode = PS_OP_NBLOCKS;
	if (execute()->status != PS_STATUS_OK || ch->result != DELETE_PAGES + 1)
		die("branch zero-extend did not grow the relation");
	free(page);
	arm_fault();
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, branch_incarnation);
	ch->opcode = PS_OP_BEGIN_DELETE;
	ch->req_seq = branch_incarnation;
	if (execute()->status != PS_STATUS_OK)
		die("BEGIN_DELETE failed");
	wait_forever();
}

/* The sibling branch: private shipped WAL, a committed WAL-index interval,
 * and one page of its own, none of which the deletion may touch. */
static void
delete_seed_survivor(unsigned char *page)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	uint64_t	parent_incarnation = 0;
	uint64_t	incarnation;

	if (timeline_state(0, &parent_incarnation) != PS_TIMELINE_LIVE)
		die("timeline 0 is not live");
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, 0);
	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->parent_timeline = 0;
	ch->req_lsn = DELETE_FORK_LSN;
	ch->req_seq = parent_incarnation;
	if (execute()->status != PS_STATUS_OK)
		die("survivor branch create failed");
	incarnation = ch->incarnation;
	delete_wal_append(DELETE_SURVIVOR_BRANCH, incarnation, DELETE_FORK_LSN);
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
	{
		PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;

		entries[0].key = ch->key;
		entries[0].block = 0;
		entries[0].flags = PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI;
		entries[0].lsn = DELETE_FORK_LSN + 16;
		entries[0].end_lsn = DELETE_FORK_LSN + 17;
	}
	ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
	ch->nblocks = 1;
	ch->datalen = sizeof(PsWalIndexEntry);
	if (execute()->status != PS_STATUS_OK)
		die("survivor WAL-index add failed");
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = DELETE_FORK_LSN;
	ch->req_seq = DELETE_FORK_LSN + DELETE_WAL_BYTES;
	if (execute()->status != PS_STATUS_OK)
		die("survivor WAL-index progress commit failed");
	delete_write_block(page, DELETE_SURVIVOR_BRANCH, incarnation, 0,
					   DELETE_FORK_LSN + 3000, 0x5A);
}

/* The sibling is untouched by its neighbour's deletion. */
static void
delete_verify_survivor(unsigned char *page)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	uint64_t	incarnation = 0;

	if (timeline_state(DELETE_SURVIVOR_BRANCH, &incarnation) != PS_TIMELINE_LIVE ||
		incarnation == 0)
		die("deletion did not leave the sibling branch live");
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
	ch->opcode = PS_OP_READV;
	ch->blocknum = 0;
	ch->nblocks = 1;
	if (execute()->status != PS_STATUS_OK)
		die("sibling branch read failed");
	memcpy(page, ch->data, page_size);
	if (!page_has_tag(page, 0x5A))
		die_page("deletion damaged the sibling's own page", 0, page);
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
	ch->opcode = PS_OP_WAL_SIZE;
	if (execute()->status != PS_STATUS_OK ||
		ch->req_lsn != DELETE_FORK_LSN + DELETE_WAL_BYTES)
		die("deletion changed the sibling's WAL end");
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = DELETE_FORK_LSN;
	ch->datalen = 64;
	if (execute()->status != PS_STATUS_OK || ch->result != 64)
		die("deletion damaged the sibling's shipped WAL");
	for (uint32_t i = 0; i < 64; i++)
		if (ch->data[i] != (unsigned char) (DELETE_SURVIVOR_BRANCH + 1))
			die("deletion corrupted the sibling's shipped WAL bytes");
	set_relation(ch);
	set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
	/* 0/0 reads the committed progress back as req_lsn */
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	if (execute()->status != PS_STATUS_OK ||
		ch->req_lsn != DELETE_FORK_LSN + DELETE_WAL_BYTES)
		die("deletion dropped the sibling's WAL-index progress");
	/* the progress record alone says nothing about the indexed entry */
	{
		PsWalRec	out[4];
		int			count = 0;

		set_relation(ch);
		set_timeline(ch, DELETE_SURVIVOR_BRANCH, incarnation);
		ch->opcode = PS_OP_WAL_INDEX_GET;
		ch->blocknum = 0;
		ch->nblocks = 0;
		ch->req_lsn = DELETE_FORK_LSN + DELETE_WAL_BYTES;
		ch->pad1 = 0;
		if (execute()->status != PS_STATUS_OK)
			die("deletion dropped the sibling's WAL-index entry");
		count = (int) ch->result;
		if (count < 1)
			die("deletion left the sibling's WAL-index chain empty");
		memcpy(out, ch->data, sizeof(*out));
		if (out[0].lsn != DELETE_FORK_LSN + 16 ||
			out[0].end_lsn != DELETE_FORK_LSN + 17 ||
			(out[0].flags & (PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI)) !=
			(PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI))
			die("deletion damaged the sibling's WAL-index entry");
	}
}

/* The crash landed before the DELETING record was durable, so the request is
 * lost: the branch is still live, still serves its own pages, and keeps every
 * artifact the deletion would have reclaimed. */
static void
delete_verify_live(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);
	uint64_t	incarnation = 0;

	if (page == NULL)
		die("out of memory");
	if (timeline_state(DELETE_BRANCH, &incarnation) != PS_TIMELINE_LIVE ||
		incarnation != DELETE_INCARNATION)
		die("a deletion that never became durable did not leave the branch live");
	/* Its ancestry is durable metadata of its own.  Every page this branch
	 * serves below was seeded privately, so create metadata replaced with the
	 * same id and incarnation but a different parent or fork point would pass
	 * every other check here while moving the branch's as-of boundary. */
	{
		uint64_t	parent_incarnation = 0;

		if (timeline_state(0, &parent_incarnation) != PS_TIMELINE_LIVE ||
			parent_incarnation == 0)
			die("the parent timeline is not live after a lost deletion request");
		set_relation(ch);
		set_timeline(ch, DELETE_BRANCH, incarnation);
		ch->opcode = PS_OP_TIMELINE_INFO;
		if (execute()->status != PS_STATUS_OK || ch->result != 1)
			die("the surviving branch lost its persisted ancestry");
		if (ch->parent_timeline != 0 || ch->req_lsn != DELETE_FORK_LSN ||
			ch->req_seq != parent_incarnation)
			die("the surviving branch changed its persisted fork identity");
	}
	for (uint32_t block = 0; block < DELETE_PAGES; block++)
	{
		set_relation(ch);
		set_timeline(ch, DELETE_BRANCH, incarnation);
		ch->opcode = PS_OP_READV;
		ch->blocknum = block;
		ch->nblocks = 1;
		if (execute()->status != PS_STATUS_OK)
			die("the surviving branch does not serve its own page");
		memcpy(page, ch->data, page_size);
		if (!page_has_tag(page, (unsigned char) (0x40 + block)))
			die_page("the surviving branch lost its own page", block, page);
	}
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, incarnation);
	ch->opcode = PS_OP_NBLOCKS;
	if (execute()->status != PS_STATUS_OK || ch->result != DELETE_PAGES + 1)
		die("the surviving branch lost its zero-extended size");
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, incarnation);
	ch->opcode = PS_OP_WAL_SIZE;
	if (execute()->status != PS_STATUS_OK ||
		ch->req_lsn != DELETE_FORK_LSN +
		(uint64_t) DELETE_WAL_SEGMENT_CHUNKS * DELETE_WAL_BYTES)
		die("the surviving branch lost part of its shipped WAL extent");
	/* the whole extent, not only its first bytes: a truncation that keeps
	 * the prefix and the artifact names would otherwise pass */
	for (uint32_t chunk = 0; chunk < DELETE_WAL_SEGMENT_CHUNKS; chunk++)
	{
		set_relation(ch);
		set_timeline(ch, DELETE_BRANCH, incarnation);
		ch->opcode = PS_OP_WAL_READ;
		ch->req_lsn = DELETE_FORK_LSN + (uint64_t) chunk * DELETE_WAL_BYTES;
		ch->datalen = 64;
		if (execute()->status != PS_STATUS_OK || ch->result != 64)
			die("the surviving branch lost its shipped WAL");
		for (uint32_t i = 0; i < 64; i++)
			if (ch->data[i] != (unsigned char) (DELETE_BRANCH + 1))
				die("the surviving branch's shipped WAL is corrupt");
	}
	delete_verify_survivor(page);
	free(page);
}

static void
delete_verify(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);
	struct timespec pause_interval = {0, 20000000};
	uint64_t	incarnation = 0;
	int			state = -1;

	if (page == NULL)
		die("out of memory");
	for (int i = 0; i < 500; i++)
	{
		state = timeline_state(DELETE_BRANCH, &incarnation);
		if (state == PS_TIMELINE_DELETED)
			break;
		nanosleep(&pause_interval, NULL);
	}
	if (state != PS_TIMELINE_DELETED)
	{
		fprintf(stderr, "pagestore_gc_crash_client: branch did not reach DELETED "
				"(state %d)\n", state);
		exit(1);
	}
	if (incarnation != DELETE_INCARNATION)
	{
		fprintf(stderr, "pagestore_gc_crash_client: DELETED branch reports incarnation "
				"%llu, seeded %llu\n", (unsigned long long) incarnation,
				(unsigned long long) DELETE_INCARNATION);
		exit(1);
	}
	/* the parent keeps serving its own page and its own shipped WAL; the
	 * branch rejects reads */
	read_latest(page, 0);
	if (!page_has_tag(page, 7))
		die_page("deletion damaged the parent's page", 0, page);
	set_relation(ch);
	ch->opcode = PS_OP_WAL_SIZE;
	if (execute()->status != PS_STATUS_OK || ch->req_lsn != DELETE_WAL_BYTES)
		die("deletion changed the parent's WAL end");
	set_relation(ch);
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = 0;
	ch->datalen = 64;
	if (execute()->status != PS_STATUS_OK || ch->result != 64)
		die("deletion damaged the parent's shipped WAL");
	for (uint32_t i = 0; i < 64; i++)
		if (ch->data[i] != 1)
			die("deletion corrupted the parent's shipped WAL bytes");
	/* the parent's fork metadata survives the owner-scoped filtering */
	set_relation(ch);
	ch->opcode = PS_OP_EXISTS;
	if (execute()->status != PS_STATUS_OK || ch->result == 0)
		die("deletion dropped the parent's relation from its fork metadata");
	set_relation(ch);
	ch->opcode = PS_OP_NBLOCKS;
	if (execute()->status != PS_STATUS_OK || ch->result != 1)
		die("deletion changed the parent's relation size");
	set_relation(ch);
	set_timeline(ch, DELETE_BRANCH, incarnation);
	ch->opcode = PS_OP_READV;
	ch->blocknum = 0;
	ch->nblocks = 1;
	if (execute()->status == PS_STATUS_OK)
		die("a DELETED branch still serves reads");
	delete_verify_survivor(page);
	free(page);
}

/* ---- fixture workload -------------------------------------------------- */

static void walidx_pin_reader(uint64_t base);
static void walidx_batch_and_commit(uint64_t base, uint64_t end_lsn);
static void walidx_check(uint64_t base, uint64_t end);
static void write_control(uint32_t block, uint64_t version, uint64_t redo);
static int wal_read_status(uint64_t lsn);

static uint64_t
fixture_create_branch(uint32_t timeline)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	uint64_t	parent_incarnation = 0;

	if (timeline_state(0, &parent_incarnation) != PS_TIMELINE_LIVE)
		die("timeline 0 is not live");
	set_relation(ch);
	set_timeline(ch, timeline, 0);
	ch->opcode = PS_OP_CREATE_BRANCH;
	ch->parent_timeline = 0;
	ch->req_lsn = FIXTURE_FORK_LSN;
	ch->req_seq = parent_incarnation;
	if (execute()->status != PS_STATUS_OK)
		die("branch create failed");
	return ch->incarnation;
}

static void
fixture_seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);
	uint64_t	lsn = 0;
	uint64_t	incarnation;

	if (page == NULL)
		die("out of memory");
	/* page history and persisted fork-size events, then the cutoff: every
	 * event below the cutoff must land before its frontier is published,
	 * because history below a durable frontier is refused */
	page_history_seed(page);
	forkmeta_events_seed();
	page_cutoff_pin();
	/* shipped WAL with one sealed segment that stays retained, a WAL-index
	 * interval with a fixed reader, and the control note that derives the
	 * WAL floor inside that segment */
	while (lsn < FIXTURE_WAL_END)
	{
		set_relation(ch);
		ch->opcode = PS_OP_WAL_APPEND;
		ch->req_lsn = lsn;
		ch->datalen = RECLAIM_CHUNK;
		memset(ch->data, (int) (1 + lsn / RECLAIM_SEGMENT), RECLAIM_CHUNK);
		if (execute()->status != PS_STATUS_OK)
			die("WAL append failed");
		lsn += RECLAIM_CHUNK;
	}
	/* the index interval lives above the WAL floor the note derives */
	walidx_pin_reader(FIXTURE_WAL_REDO);
	write_control(0, FIXTURE_WAL_END, FIXTURE_WAL_REDO);
	write_control(1, FIXTURE_WAL_END, FIXTURE_WAL_REDO);
	walidx_batch_and_commit(FIXTURE_WAL_REDO, FIXTURE_WAL_END);
	/* a live branch with its own page version, and a deleted branch */
	incarnation = fixture_create_branch(FIXTURE_BRANCH);
	delete_write_block(page, FIXTURE_BRANCH, incarnation, 0,
					   FIXTURE_BRANCH_LSN, 0x77);
	/* the branch's own shipped WAL; its WAL-index interval is added by the
	 * extension, after the snapshot cutover has emptied the epoch logs */
	delete_wal_append(FIXTURE_BRANCH, incarnation, FIXTURE_FORK_LSN);
	/* One record of every page-segment format the daemon writes today: an
	 * ordinary versioned record above, a below-floor copy whose record is
	 * clamped to the branch point, and a zero-version (WAL-less) record. */
	delete_write_block(page, FIXTURE_BRANCH, incarnation, FIXTURE_CLAMPED_BLOCK,
					   FIXTURE_CLAMPED_LSN, 0x55);
	delete_write_block(page, FIXTURE_BRANCH, incarnation, FIXTURE_WALLESS_BLOCK,
					   0, 0x66);
	incarnation = fixture_create_branch(FIXTURE_DELETED_BRANCH);
	set_relation(ch);
	set_timeline(ch, FIXTURE_DELETED_BRANCH, incarnation);
	ch->opcode = PS_OP_BEGIN_DELETE;
	ch->req_seq = incarnation;
	if (execute()->status != PS_STATUS_OK)
		die("BEGIN_DELETE failed");
	free(page);
}

/* Fork-size events appended after the snapshot cutover live in the source
 * epoch's tail rather than in the checkpoint, so the fixture carries both. */
static void
fixture_extend(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	uint64_t	incarnation = 0;

	forkmeta_create_grow(FIXTURE_TAIL_REL, FIXTURE_TAIL_LSN, 2);
	/* The cutover leaves every epoch log empty, so the records of the
	 * WAL-index log format itself are appended afterwards, on the branch
	 * whose WAL the extension phase no longer snapshots. */
	if (timeline_state(FIXTURE_BRANCH, &incarnation) != PS_TIMELINE_LIVE ||
		incarnation == 0)
		die("fixture branch is not live for its WAL-index interval");
	{
		PsWalIndexEntry *entries = (PsWalIndexEntry *) ch->data;

		set_relation(ch);
		set_timeline(ch, FIXTURE_BRANCH, incarnation);
		entries[0].key = ch->key;
		entries[0].block = 0;
		entries[0].flags = PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI;
		entries[0].lsn = FIXTURE_FORK_LSN + 16;
		entries[0].end_lsn = FIXTURE_FORK_LSN + 17;
		ch->opcode = PS_OP_WAL_INDEX_ADD_BATCH;
		ch->nblocks = 1;
		ch->datalen = sizeof(*entries);
		if (execute()->status != PS_STATUS_OK)
			die("fixture branch WAL-index add failed");
	}
	set_relation(ch);
	set_timeline(ch, FIXTURE_BRANCH, incarnation);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = FIXTURE_FORK_LSN;
	ch->req_seq = FIXTURE_FORK_LSN + DELETE_WAL_BYTES;
	if (execute()->status != PS_STATUS_OK)
		die("fixture branch WAL-index progress commit failed");
}

/* The seed wrote each 64 KiB chunk full of a byte derived from its position,
 * so a compatibility regression that shifts or truncates the persisted WAL is
 * visible in the bytes, not only in the request status. */
static void
fixture_wal_check(uint64_t lsn)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char expected = (unsigned char) (1 + lsn / RECLAIM_SEGMENT);

	set_relation(ch);
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = lsn;
	ch->datalen = 64;
	if (execute()->status != PS_STATUS_OK)
		die("fixture shipped WAL is not readable");
	if (ch->result != 64)
		die("fixture shipped WAL returned a short read");
	for (uint32_t i = 0; i < 64; i++)
		if (ch->data[i] != expected)
			die("fixture shipped WAL returned the wrong bytes");
}

static void
fixture_verify(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	unsigned char *page = malloc(page_size);
	uint64_t	incarnation = 0;

	if (page == NULL)
		die("out of memory");
	verify();
	forkmeta_check();
	set_relation(ch);
	ch->key.relNumber = FIXTURE_TAIL_REL;
	ch->opcode = PS_OP_NBLOCKS;
	if (execute()->status != PS_STATUS_OK || ch->result != 2)
		die("fixture lost the fork-size events appended after the snapshot cutover");
	walidx_check(FIXTURE_WAL_REDO, FIXTURE_WAL_END);
	set_relation(ch);
	ch->opcode = PS_OP_WAL_SIZE;
	if (execute()->status != PS_STATUS_OK || ch->req_lsn != FIXTURE_WAL_END)
		die("fixture WAL end changed");
	set_relation(ch);
	ch->opcode = PS_OP_WAL_RETAIN_FLOOR;
	if (execute()->status != PS_STATUS_OK || ch->req_lsn != FIXTURE_WAL_REDO)
		die("fixture WAL retain floor changed");
	fixture_wal_check(0);
	fixture_wal_check(RECLAIM_SEGMENT);
	if (timeline_state(FIXTURE_BRANCH, &incarnation) != PS_TIMELINE_LIVE ||
		incarnation == 0)
		die("fixture branch is not live");
	set_relation(ch);
	set_timeline(ch, FIXTURE_BRANCH, incarnation);
	ch->opcode = PS_OP_READV;
	ch->blocknum = 0;
	ch->nblocks = 1;
	if (execute()->status != PS_STATUS_OK)
		die("fixture branch read failed");
	memcpy(page, ch->data, page_size);
	if (!page_has_tag(page, 0x77))
		die_page("fixture branch lost its own page version", 0, page);
	set_relation(ch);
	set_timeline(ch, FIXTURE_BRANCH, incarnation);
	ch->opcode = PS_OP_READV;
	ch->blocknum = 1;
	ch->nblocks = 1;
	if (execute()->status != PS_STATUS_OK)
		die("fixture branch inherited read failed");
	memcpy(page, ch->data, page_size);
	if (!page_has_tag(page, 1))
		die_page("fixture branch lost its inherited page", 1, page);
	set_relation(ch);
	set_timeline(ch, FIXTURE_BRANCH, incarnation);
	ch->opcode = PS_OP_READV;
	ch->blocknum = FIXTURE_CLAMPED_BLOCK;
	ch->nblocks = 1;
	if (execute()->status != PS_STATUS_OK)
		die("fixture branch clamped read failed");
	memcpy(page, ch->data, page_size);
	if (!page_has_tag(page, 0x55))
		die_page("fixture branch lost its clamped page version",
				 FIXTURE_CLAMPED_BLOCK, page);
	set_relation(ch);
	set_timeline(ch, FIXTURE_BRANCH, incarnation);
	ch->opcode = PS_OP_READV;
	ch->blocknum = FIXTURE_WALLESS_BLOCK;
	ch->nblocks = 1;
	if (execute()->status != PS_STATUS_OK)
		die("fixture branch WAL-less read failed");
	memcpy(page, ch->data, page_size);
	if (!page_has_tag(page, 0x66))
		die_page("fixture branch lost its WAL-less page version",
				 FIXTURE_WALLESS_BLOCK, page);
	set_relation(ch);
	set_timeline(ch, FIXTURE_BRANCH, incarnation);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = 0;
	ch->req_seq = 0;
	if (execute()->status != PS_STATUS_OK ||
		ch->req_lsn != FIXTURE_FORK_LSN + DELETE_WAL_BYTES)
		die("fixture branch lost its WAL-index progress");
	/* The progress record is only half the log format: read the seeded
	 * entry back so the current WAL-index record reader is exercised too. */
	{
		PsWalRec	out[4];

		set_relation(ch);
		set_timeline(ch, FIXTURE_BRANCH, incarnation);
		ch->opcode = PS_OP_WAL_INDEX_GET;
		ch->blocknum = 0;
		ch->nblocks = 0;
		ch->req_lsn = FIXTURE_FORK_LSN + DELETE_WAL_BYTES;
		ch->pad1 = 0;
		if (execute()->status != PS_STATUS_OK || (int) ch->result < 1)
			die("fixture branch lost its WAL-index entry");
		memcpy(out, ch->data, sizeof(*out));
		if (out[0].lsn != FIXTURE_FORK_LSN + 16 ||
			out[0].end_lsn != FIXTURE_FORK_LSN + 17 ||
			(out[0].flags & (PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI)) !=
			(PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI))
			die("fixture branch WAL-index entry changed");
	}
	if (timeline_state(FIXTURE_DELETED_BRANCH, &incarnation) != PS_TIMELINE_DELETED)
		die("fixture deleted branch is not DELETED");
	free(page);
}

/* ---- wal_reclaim workload ---------------------------------------------- */

static void
set_control(PsChannel *ch)
{
	set_relation(ch);
	memset(&ch->key, 0, sizeof(ch->key));
	ch->key.klass = PS_KLASS_CONTROL;
}

/* Publishes one control block at the given version; block 1 carries the
 * redo floor note that derives the timeline's WAL cutoff. */
static void
write_control(uint32_t block, uint64_t version, uint64_t redo)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	uint32_t	nblocks;

	set_control(ch);
	ch->opcode = PS_OP_CREATE;
	ch->is_redo = 1;
	if (execute()->status != PS_STATUS_OK)
		die("control object create failed");
	set_control(ch);
	ch->opcode = PS_OP_NBLOCKS;
	if (execute()->status != PS_STATUS_OK)
		die("control object size read failed");
	nblocks = ch->result;
	set_control(ch);
	ch->opcode = block < nblocks ? PS_OP_WRITEV : PS_OP_EXTEND;
	ch->blocknum = block;
	ch->nblocks = 1;
	ch->req_lsn = version;
	memset(ch->data, block == 0 ? 0xC3 : 0, page_size);
	if (block == 1)
		memcpy(ch->data, &redo, sizeof(redo));
	if (execute()->status != PS_STATUS_OK)
		die("control block write failed");
}

static void
reclaim_seed(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	uint64_t	lsn = 0;

	while (lsn < RECLAIM_TOTAL)
	{
		set_relation(ch);
		ch->opcode = PS_OP_WAL_APPEND;
		ch->req_lsn = lsn;
		ch->datalen = RECLAIM_CHUNK;
		memset(ch->data, (int) (1 + lsn / RECLAIM_SEGMENT), RECLAIM_CHUNK);
		if (execute()->status != PS_STATUS_OK)
			die("WAL append failed");
		lsn += RECLAIM_CHUNK;
	}
	write_control(0, RECLAIM_TOTAL, RECLAIM_TOTAL);
	write_control(1, RECLAIM_TOTAL, RECLAIM_TOTAL);
	/* The retained base needs both the note and durable WAL-index progress
	 * through the sealed prefix; arm before the commit that completes it. */
	arm_fault();
	set_relation(ch);
	ch->opcode = PS_OP_WAL_INDEX_PROGRESS;
	ch->req_lsn = 0;
	ch->req_seq = RECLAIM_TOTAL;
	if (execute()->status != PS_STATUS_OK)
		die("WAL-index progress commit failed");
	wait_forever();
}

static int
wal_read_status(uint64_t lsn)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_WAL_READ;
	ch->req_lsn = lsn;
	ch->datalen = 64;
	return execute()->status;
}

static void
reclaim_verify(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);
	struct timespec pause_interval = {0, 20000000};
	int			reclaimed = 0;

	/* Recovery retries the interrupted prefix unlink asynchronously; the
	 * sealed prefix must be gone within a bounded wait. */
	for (int i = 0; i < 500; i++)
	{
		if (wal_read_status(0) != PS_STATUS_OK &&
			wal_read_status(RECLAIM_TOTAL - RECLAIM_SEGMENT) != PS_STATUS_OK)
		{
			reclaimed = 1;
			break;
		}
		nanosleep(&pause_interval, NULL);
	}
	if (!reclaimed)
		die("recovery did not finish reclaiming the sealed WAL prefix");
	set_relation(ch);
	ch->opcode = PS_OP_WAL_SIZE;
	if (execute()->status != PS_STATUS_OK || ch->req_lsn != RECLAIM_TOTAL)
		die("recovery changed the timeline's WAL end");
	set_relation(ch);
	ch->opcode = PS_OP_WAL_RETAIN_FLOOR;
	if (execute()->status != PS_STATUS_OK || ch->req_lsn != RECLAIM_TOTAL)
	{
		fprintf(stderr, "pagestore_gc_crash_client: recovery reports WAL retain "
				"floor %llu, expected %llu\n", (unsigned long long) ch->req_lsn,
				(unsigned long long) RECLAIM_TOTAL);
		exit(1);
	}
}

/* Returns the daemon status; on OK fills *count and out[] (up to max_out). */
static int
walidx_get(uint64_t lsn_max, PsWalRec *out, uint32_t max_out, int *count)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	set_relation(ch);
	ch->opcode = PS_OP_WAL_INDEX_GET;
	ch->blocknum = WALIDX_BLOCK;
	ch->nblocks = 0;
	ch->req_lsn = lsn_max;
	ch->pad1 = 0;
	execute();
	*count = (int) ch->result;
	if (ch->status == PS_STATUS_OK && *count > 0)
		memcpy(out, ch->data,
			   (size_t) (*count < (int) max_out ? *count : (int) max_out) *
			   sizeof(*out));
	return ch->status;
}

/* Reads at the durable frontier (end) see the compacted chains; a read at
 * the reader's exact pin sees its chain; unrepresented points are refused. */
static void
walidx_check(uint64_t base, uint64_t end)
{
	PsWalRec	out[8];
	struct timespec pause_interval = {0, 20000000};
	int			count = 0;
	int			compacted = 0;

	/* Restart retries the prepared generation behind its durable frontier
	 * asynchronously; the compacted chains must appear within a bounded
	 * wait. */
	for (int i = 0; i < 500; i++)
	{
		if (walidx_get(end, out, 8, &count) == PS_STATUS_OK && count == 4)
		{
			compacted = 1;
			break;
		}
		nanosleep(&pause_interval, NULL);
	}
	if (!compacted)
	{
		fprintf(stderr, "pagestore_gc_crash_client: recovery did not serve the "
				"compacted WAL-index chains (count %d)\n", count);
		exit(1);
	}
	/* The seeded tuples, not only their positions: a recovery that keeps the
	 * LSNs but drops an end position, a flag or the source timeline no longer
	 * describes chains WAL replay can follow. */
	if (out[0].lsn != base + 10 || out[1].lsn != base + 30 ||
		out[2].lsn != base + 90 || out[3].lsn != base + 110)
	{
		const uint64_t expect_lsn[] = {10, 30, 90, 110};
		const uint32_t expect_flags[] = {
			PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI,
			PS_WAL_INDEX_FLAG_KNOWN,
			PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI,
			PS_WAL_INDEX_FLAG_KNOWN
		};

		for (int i = 0; i < 4; i++)
			if (out[i].lsn != expect_lsn[i] ||
				out[i].end_lsn != expect_lsn[i] + 1 ||
				out[i].flags != expect_flags[i] || out[i].timeline != 0)
			{
				fprintf(stderr, "pagestore_gc_crash_client: unexpected compacted "
						"chain %d: lsn=%llu end=%llu flags=%u timeline=%u\n", i,
						(unsigned long long) out[i].lsn,
						(unsigned long long) out[i].end_lsn,
						out[i].flags, out[i].timeline);
				exit(1);
			}
	}
	/* the fixed reader's retained chain, in full: an end position, a flag or
	 * a source timeline lost here sends WAL replay to the wrong range */
	if (walidx_get(base + WALIDX_READER_LSN, out, 8, &count) != PS_STATUS_OK ||
		count != 2 ||
		out[0].lsn != base + 10 || out[1].lsn != base + 30 ||
		out[0].end_lsn != base + 11 || out[1].end_lsn != base + 31 ||
		out[0].flags != (PS_WAL_INDEX_FLAG_KNOWN | PS_WAL_INDEX_FLAG_FPI) ||
		out[1].flags != PS_WAL_INDEX_FLAG_KNOWN ||
		out[0].timeline != 0 || out[1].timeline != 0)
		die("recovery lost the fixed reader's retained WAL-index chain");
	if (walidx_get(base + WALIDX_DROPPED_LSN, out, 8, &count) == PS_STATUS_OK)
		die("recovery resurrected a WAL-index point below the durable frontier");
}

static void
walidx_verify(void)
{
	PsChannel  *ch = ps_channel(shm_base, channel);

	walidx_check(0, WALIDX_WAL_BYTES);
	/* The retained pin must still be the seeded reader itself.  A pin that
	 * kept the horizon but lost its owner identity leaves the real owner
	 * unable to advance or drop it, and nothing else here would notice.
	 * Only this workload seeds that reader, so the check lives here rather
	 * than in the shared chain oracle. */
	set_relation(ch);
	ch->timeline = 0;
	ch->opcode = PS_OP_RETENTION_PIN_LOOKUP;
	ch->blocknum = PS_RETENTION_OWNER_READER;
	ch->req_seq = WALIDX_READER;
	if (execute()->status != PS_STATUS_OK || ch->result != 1)
		die("recovery lost the seeded WAL-index reader's pin");
	if (ch->timeline != 0 || ch->blocknum != PS_RETENTION_OWNER_READER ||
		ch->req_seq != WALIDX_READER || ch->old_nblocks != 1 ||
		ch->parent_timeline != PS_RETENTION_RESOURCE_WAL_INDEX ||
		ch->req_lsn != WALIDX_READER_LSN)
		die("recovery changed the seeded WAL-index reader's identity");
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
		else if (strcmp(argv[i], "--workload") == 0 && i + 1 < argc)
			workload = argv[++i];
		else if (strcmp(argv[i], "--resume-file") == 0 && i + 1 < argc)
			resume_file = argv[++i];
		else if (strcmp(argv[i], "--cutoff-seq-file") == 0 && i + 1 < argc)
			cutoff_seq_file = argv[++i];
		else
			die("usage: --shm NAME --mode seed|verify "
				"[--workload page_prune|wal_index|wal_reclaim|timeline_delete|"
				"timeline_delete_abort|manifest_compact|forkmeta|fixture] "
				"[--arm-marker PATH] [--resume-file PATH] [--cutoff-seq-file PATH]");
	}
	if (shm == NULL || mode == NULL ||
		(strcmp(mode, "seed") != 0 && strcmp(mode, "verify") != 0 &&
		 (strcmp(mode, "extend") != 0 || strcmp(workload, "fixture") != 0)) ||
		(strcmp(workload, "page_prune") != 0 &&
		 strcmp(workload, "wal_index") != 0 &&
		 strcmp(workload, "wal_reclaim") != 0 &&
		 strcmp(workload, "timeline_delete") != 0 &&
		 strcmp(workload, "timeline_delete_abort") != 0 &&
		 strcmp(workload, "manifest_compact") != 0 &&
		 strcmp(workload, "forkmeta") != 0 &&
		 strcmp(workload, "fixture") != 0))
		die("usage: --shm NAME --mode seed|verify "
			"[--workload page_prune|wal_index|wal_reclaim|timeline_delete|"
			"timeline_delete_abort|manifest_compact|forkmeta|fixture] "
			"[--arm-marker PATH] [--resume-file PATH] [--cutoff-seq-file PATH]");
	attach(shm);
	if (strcmp(workload, "fixture") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			fixture_seed();
		else if (strcmp(mode, "extend") == 0)
			fixture_extend();
		else
			fixture_verify();
	}
	else if (strcmp(workload, "forkmeta") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			forkmeta_seed();
		else
			forkmeta_verify();
	}
	else if (strcmp(workload, "manifest_compact") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			manifest_seed();
		else
			manifest_verify();
	}
	else if (strcmp(workload, "timeline_delete") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			delete_seed();
		else
			delete_verify();
	}
	else if (strcmp(workload, "timeline_delete_abort") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			delete_seed();
		else
			delete_verify_live();
	}
	else if (strcmp(workload, "wal_reclaim") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			reclaim_seed();
		else
			reclaim_verify();
	}
	else if (strcmp(workload, "wal_index") == 0)
	{
		if (strcmp(mode, "seed") == 0)
			walidx_seed();
		else
			walidx_verify();
	}
	else if (strcmp(mode, "seed") == 0)
		seed();
	else
		verify();
	detach();
	return 0;
}
