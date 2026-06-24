/*-------------------------------------------------------------------------
 *
 * pagestore_bench.c
 *	  Tiny throughput probe for the page-store daemon (indicative, not rigorous).
 *
 * Forks the daemon given on the command line (POSIX or SPDK -- both accept the
 * same --shm/--store/--page-size/--segment-size args; the SPDK one reads its PCI
 * address from $PS_SPDK_PCI), writes a dataset large enough to span many
 * segments, flushes, then times reading it all back in max-combine READVs.
 *
 * Caveats: the POSIX store reads through the OS page cache (and sits on a
 * different disk than the SPDK control disk), so this is not a fair POSIX-vs-SPDK
 * shootout -- it is mainly here to show the SPDK daemon's read path moving real
 * bytes off NVMe and the effect of batched (overlapped) reads.
 *
 * For a fair cold-read POSIX comparison, set PS_BENCH_DROPCACHE=1 (needs root):
 * the OS page cache is dropped between the write and read phases so POSIX reads
 * hit the disk rather than RAM.  It is a no-op for SPDK (no OS cache).
 *
 * Usage: [PS_SPDK_PCI=...] [PS_BENCH_DROPCACHE=1] \
 *            pagestore_bench <daemon-path> [total-MiB] [store-base-dir]
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
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"

#define PAGE	8192u
#define SEGSZ	"4194304"		/* 4 MiB segments */
#define REL		42u

static void *g_shm;
static int	g_chan = -1;

static PsChannel *
chp(void)
{
	return ps_channel(g_shm, g_chan);
}

static void
exec_on(int chan)
{
	PsChannel  *c = ps_channel(g_shm, chan);

	ps_store_release(&c->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&c->state) != PS_STATE_DONE)
		;
}

static void
exec1(void)
{
	exec_on(g_chan);
}

/* claim any free channel for this (possibly forked) reader; -1 if none */
static int
claim_channel(void)
{
	PsShmHeader *h = g_shm;

	for (uint32_t c = 0; c < h->nchannels; c++)
		if (ps_cas(&ps_channel(g_shm, c)->claimed, 0, 1))
			return (int) c;
	return -1;
}

/* read chunks [from, to) of the shuffled order on our own channel */
static void
read_range(const uint32_t *starts, uint32_t from, uint32_t to,
		   uint32_t npages, uint32_t combine)
{
	int			chan = claim_channel();

	if (chan < 0)
		_exit(3);
	for (uint32_t ci = from; ci < to; ci++)
	{
		uint32_t	b = starts[ci];
		uint32_t	k = npages - b < combine ? npages - b : combine;
		PsChannel  *c = ps_channel(g_shm, chan);

		c->key.spcOid = 1;
		c->key.dbOid = 1;
		c->key.relNumber = REL;
		c->key.forkNum = 0;
		c->key.klass = PS_KLASS_RELATION;
		c->timeline = 0;
		c->opcode = PS_OP_READV;
		c->blocknum = b;
		c->nblocks = k;
		exec_on(chan);
	}
	ps_store_release(&ps_channel(g_shm, chan)->claimed, 0);
}

static double
now(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

static pid_t
spawn(const char *daemon, const char *shm, const char *store)
{
	pid_t		pid = fork();

	if (pid == 0)
	{
		execl(daemon, daemon, "--shm", shm, "--store", store,
			  "--page-size", "8192", "--segment-size", SEGSZ, (char *) NULL);
		perror("execl");
		_exit(127);
	}
	return pid;
}

static void
attach(const char *shm)
{
	for (int i = 0; i < 1000; i++)
	{
		int			fd = shm_open(shm, O_RDWR, 0600);

		if (fd >= 0)
		{
			PsShmHeader *h = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE,
								  MAP_SHARED, fd, 0);

			close(fd);
			if (h != MAP_FAILED && h->magic == PS_SHM_MAGIC &&
				h->version == PS_SHM_VERSION && h->page_size == PAGE)
			{
				g_shm = h;
				for (uint32_t c = 0; c < h->nchannels; c++)
					if (ps_cas(&ps_channel(g_shm, c)->claimed, 0, 1))
					{
						g_chan = (int) c;
						return;
					}
			}
		}
		usleep(10000);
	}
	fprintf(stderr, "bench: daemon not ready\n");
	exit(2);
}

int
main(int argc, char **argv)
{
	const char *daemon = argc > 1 ? argv[1] : NULL;
	uint64_t	total_mib = argc > 2 ? strtoull(argv[2], NULL, 10) : 256;
	const char *base = argc > 3 ? argv[3] : "/tmp";
	const char *shm = "/psbench";
	char		store[1024];
	uint32_t	npages = (uint32_t) (total_mib * 1024 * 1024 / PAGE);
	uint32_t	combine = PS_IO_UNIT / PAGE;
	unsigned char *buf;
	pid_t		pid;
	double		t0,
				wsec,
				rsec;

	snprintf(store, sizeof(store), "%s/psbenchstoreXXXXXX", base);
	if (!daemon || !mkdtemp(store))
	{
		fprintf(stderr, "usage: %s <daemon-path> [total-MiB] [store-base-dir]\n",
				argv[0]);
		return 2;
	}
	shm_unlink(shm);
	pid = spawn(daemon, shm, store);
	attach(shm);

	buf = malloc((size_t) combine * PAGE);
	memset(buf, 0xab, (size_t) combine * PAGE);

	/* write phase */
	t0 = now();
	for (uint32_t b = 0; b < npages; b += combine)
	{
		uint32_t	k = npages - b < combine ? npages - b : combine;
		PsChannel  *c = chp();

		for (uint32_t i = 0; i < k; i++)
			memcpy(buf + (size_t) i * PAGE, &(uint64_t){b + i}, 8);	/* vary pd_lsn */
		c->key.spcOid = 1;
		c->key.dbOid = 1;
		c->key.relNumber = REL;
		c->key.forkNum = 0;
		c->key.klass = PS_KLASS_RELATION;
		c->timeline = 0;
		c->opcode = PS_OP_WRITEV;
		c->blocknum = b;
		c->nblocks = k;
		memcpy(c->data, buf, (size_t) k * PAGE);
		exec1();
	}
	chp()->opcode = PS_OP_IMMEDSYNC;
	exec1();
	wsec = now() - t0;

	/* optional: drop the OS page cache so POSIX reads hit the disk (root only) */
	if (getenv("PS_BENCH_DROPCACHE"))
	{
		FILE	   *f;

		sync();
		f = fopen("/proc/sys/vm/drop_caches", "w");
		if (f)
		{
			fputs("3\n", f);
			fclose(f);
		}
		else
			fprintf(stderr, "bench: could not drop caches (run as root)\n");
	}

	/* build the chunk order; shuffle for a random pattern (defeats the OS
	 * readahead that favors POSIX on sequential reads); optionally split the
	 * chunks across PS_BENCH_CLIENTS reader processes, each on its own channel,
	 * to drive cross-channel queue depth on the daemon. */
	{
		uint32_t	nchunks = (npages + combine - 1) / combine;
		uint32_t   *starts = malloc((size_t) nchunks * sizeof(uint32_t));
		int			random = getenv("PS_BENCH_RANDOM") != NULL;
		int			clients = getenv("PS_BENCH_CLIENTS") ?
			atoi(getenv("PS_BENCH_CLIENTS")) : 1;
		uint64_t	rng = 0x9e3779b97f4a7c15ull;

		if (clients < 1)
			clients = 1;
		for (uint32_t i = 0; i < nchunks; i++)
			starts[i] = i * combine;
		if (random)
			for (uint32_t i = nchunks; i > 1; i--)	/* Fisher-Yates, fixed seed */
			{
				uint32_t	j;
				uint32_t	t;

				rng = rng * 6364136223846793005ull + 1442695040888963407ull;
				j = (uint32_t) ((rng >> 33) % i);
				t = starts[i - 1];
				starts[i - 1] = starts[j];
				starts[j] = t;
			}

		t0 = now();
		if (clients == 1)
			read_range(starts, 0, nchunks, npages, combine);
		else
		{
			for (int c = 0; c < clients; c++)
			{
				uint32_t	from = (uint32_t) ((uint64_t) c * nchunks / clients);
				uint32_t	to = (uint32_t) ((uint64_t) (c + 1) * nchunks / clients);

				if (fork() == 0)
				{
					read_range(starts, from, to, npages, combine);
					_exit(0);
				}
			}
			for (int c = 0; c < clients; c++)
				wait(NULL);
		}
		rsec = now() - t0;
		free(starts);
		printf("  (read pattern: %s, clients: %d)\n",
			   random ? "random chunk order" : "sequential", clients);
	}

	printf("daemon=%s  %llu MiB  page=%u  combine=%u\n", daemon,
		   (unsigned long long) total_mib, PAGE, combine);
	printf("  write: %.3fs  %.0f MiB/s  %.0f Kpages/s\n",
		   wsec, total_mib / wsec, npages / wsec / 1000.0);
	printf("  read : %.3fs  %.0f MiB/s  %.0f Kpages/s\n",
		   rsec, total_mib / rsec, npages / rsec / 1000.0);

	ps_store_release(&chp()->claimed, 0);
	munmap(g_shm, PS_SHM_SIZE);
	kill(pid, SIGTERM);
	waitpid(pid, NULL, 0);

	/* best-effort cleanup of the store dir we created */
	{
		char		cmd[1100];

		snprintf(cmd, sizeof(cmd), "rm -rf '%s'", store);
		if (system(cmd) != 0)
			fprintf(stderr, "bench: cleanup of %s failed\n", store);
	}
	return 0;
}
