/*
 * fev_bench.c -- F5 microbenchmark: K commit-class ordered rewrites of one
 * block on one fork, then time (a) the live path per write, (b) a reopen
 * before cutover (source-log + segment replay), (c) a cutover
 * (fork_meta_snapshot_build), (d) a reopen after cutover (snapshot load).
 *
 * Developer tool: not built or run by CI or meson.build; build and run it
 * by hand when re-measuring F5 (see RELEASE_VALIDATION.md).
 *
 * Build (from contrib/pagestore):
 *   cc -O2 -g -Wall -Wextra -I. -o fev_bench fev_bench.c pagestore_core.c
 *      pagestore_fault.c storage_posix.c pagestore_layer.c
 *      pagestore_layer_store.c pagestore_manifest.c pagestore_memtable.c
 *      pagestore_pgcache.c pagestore_prune.c pagestore_retention.c
 *      pagestore_wal_store.c pagestore_wal_segment.c pagestore_walidx_prune.c
 *      pagestore_walidx_snapshot.c pagestore_forkmeta_prune.c
 *      pagestore_forkmeta_snapshot.c pagestore_store_owner.c -lrt -lpthread
 *
 * Usage: fev_bench K [future|past|periodic]
 *   future:   the cutover cutoff stays below the markers (they are all
 *             "future" and kept in the tail part -- the builder never
 *             evaluates marker_page_retained for them)
 *   past:     a page-history pin above the markers moves the cutoff past
 *             them, so the builder evaluates marker_page_retained per
 *             marker and drops the ones whose version was pruned.
 *   periodic: design B (per-lifetime bound).  K WAL-less rewrites of block 0
 *             split into FEV_PERIODIC_ROUNDS (env, default 10) rounds; each
 *             round advances one retention pin's admission_seq to that
 *             round's newest write and forces a cutover, then reports the
 *             in-memory event count (ps_test_fork_event_count()) after that
 *             cutover.  Reports the max in-memory event count seen across
 *             every round -- with fork_event_compact_dropped_markers() this
 *             stays near one round's worth regardless of K; reverted, it
 *             grows to roughly K.
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_core.h"
#include "pagestore_forkmeta_snapshot.h"
#include "pagestore_prune.h"
#include "pagestore_retention.h"

static double
now_s(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double) ts.tv_sec + (double) ts.tv_nsec / 1e9;
}

static void
remove_tree(const char *path)
{
	DIR *dir = opendir(path);
	struct dirent *entry;

	if (dir == NULL)
	{
		(void) unlink(path);
		return;
	}
	while ((entry = readdir(dir)) != NULL)
	{
		char child[1200];
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) < 0 ||
			lstat(child, &st) != 0)
			continue;
		if (S_ISDIR(st.st_mode))
			remove_tree(child);
		else
			(void) unlink(child);
	}
	closedir(dir);
	(void) rmdir(path);
}

static int
meta_request(PsOpcode opcode, const PsKey *key, uint64_t lsn,
			 uint64_t seq, uint32_t nblocks, uint32_t blocknum)
{
	PsChannel ch;
	uint32_t shard = ps_shard_of(key);

	memset(&ch, 0, sizeof(ch));
	ch.opcode = opcode;
	ch.timeline = 0;
	ch.key = *key;
	ch.req_lsn = lsn;
	ch.req_seq = seq;
	ch.nblocks = nblocks;
	ch.blocknum = blocknum;
	ch.status = PS_STATUS_OK;
	ps_admission_read_lock();
	ps_lock_shard_wr(shard);
	(void) ps_handle_meta(&ch);
	ps_unlock_shard(shard);
	ps_admission_read_unlock();
	return ch.status == PS_STATUS_OK;
}

static int
append_relation(const PsKey *key, uint32_t block, uint64_t lsn,
				unsigned char *page, unsigned char tag, uint64_t *seq)
{
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;
	int rc;

	memset(page, 0, page_size);
	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	page[128] = tag;
	ps_admission_read_lock();
	ps_lock_shard_wr(ps_shard_of(key));
	rc = append_page(0, key, block, page, 0, seq);
	ps_unlock_shard(ps_shard_of(key));
	ps_admission_read_unlock();
	return rc;
}

static int
run_maintenance_until(const char *path, int want_exists)
{
	for (unsigned int i = 0; i < 400; i++)
	{
		(void) ps_core_maintenance();
		if ((access(path, F_OK) == 0) == want_exists)
			return 1;
		usleep(10000);
	}
	return 0;
}

static void
close_runtime(void)
{
	ps_core_close();
	if (ps_storage->close)
		ps_storage->close();
}

static int
set_pin(uint64_t lsn, uint64_t seq, uint64_t generation)
{
	PsRetentionPin pin;

	memset(&pin, 0, sizeof(pin));
	pin.timeline = 0;
	pin.owner_kind = 1;
	pin.owner_id = 78;
	pin.resources = PS_RETENTION_RESOURCE_PAGE_HISTORY;
	pin.generation = generation;
	pin.lsn = lsn;
	pin.admission_seq = seq;
	return ps_retention_set(&pin) == PS_RETENTION_OK;
}

/*
 * design B: K WAL-less rewrites of block 0 split into ROUNDS rounds.  The
 * fork is created once at a fixed floor far above every round's pin (so
 * `existed_before` stays false for the whole run -- every rewrite keeps
 * generating an inert commit marker, not just round 0 -- and so later
 * rounds' rewrites are never refused as not-future of an advancing
 * operational cutoff); each round's retention pin targets that SAME floor
 * lsn with an advancing admission_seq (a higher generation supersedes its
 * own earlier pin), so every earlier round's versions become reclaimable,
 * then forces a cutover and reports the in-memory event count.
 */
static int
run_periodic(unsigned long K)
{
	char store[] = "/tmp/fevbenchperiodicXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 1, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t seq = 0;
	unsigned long rounds = getenv("FEV_PERIODIC_ROUNDS") ?
		strtoul(getenv("FEV_PERIODIC_ROUNDS"), NULL, 10) : 10;
	unsigned long per_round = K / rounds ? K / rounds : 1;
	const uint64_t floor = 100000;
	uint32_t max_nevents = 0, first_nevents = 0;

	if (mkdtemp(store) == NULL)
		return 1;
	snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	flush_pages = getenv("FEV_FLUSH_PAGES") ? atoi(getenv("FEV_FLUSH_PAGES")) : 16;
	compact_layers = getenv("FEV_COMPACT_LAYERS") ?
		atoi(getenv("FEV_COMPACT_LAYERS")) : 2;
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
	if (ps_core_open(store) != 0)
	{
		fprintf(stderr, "periodic: open failed\n");
		return 1;
	}
	if (!meta_request(PS_OP_CREATE, &key, floor, 0, 0, 0))
	{
		fprintf(stderr, "periodic: create failed\n");
		return 1;
	}

	for (unsigned long round = 0; round < rounds; round++)
	{
		unsigned long lo = round * per_round;
		unsigned long hi = (round + 1 == rounds) ? K : lo + per_round;
		uint32_t nevents = 0, nmarkers = 0, ninert = 0;
		double tr0 = now_s();

		for (unsigned long i = lo; i < hi; i++)
			if (append_relation(&key, 0, 50, page, (unsigned char) i, &seq) != 0)
			{
				fprintf(stderr, "periodic: write %lu failed (round %lu)\n", i, round);
				return 1;
			}
		if (!set_pin(floor, seq, round + 1))
		{
			fprintf(stderr, "periodic: retention advance failed (round %lu)\n", round);
			return 1;
		}
		if (round == 0)
		{
			if (!run_maintenance_until(frontier, 1))
			{
				fprintf(stderr, "periodic: first frontier failed\n");
				return 1;
			}
		}
		else
			for (int tick = 0; tick < 60; tick++)
			{
				(void) ps_core_maintenance();
				usleep(50000);
			}
		setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1);
		if (!run_maintenance_until(manifest, 1))
		{
			fprintf(stderr, "periodic: cutover failed (round %lu)\n", round);
			return 1;
		}
		setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
		if (!ps_test_fork_event_count(0, &key, &nevents, &nmarkers, &ninert))
		{
			fprintf(stderr, "periodic: event count read failed (round %lu)\n", round);
			return 1;
		}
		if (round == 0)
			first_nevents = nevents;
		if (nevents > max_nevents)
			max_nevents = nevents;
		printf("periodic round=%-3lu writes=%-7lu nevents=%-6u nmarkers=%-6u "
			   "ninert=%-6u  %.3f s\n",
			   round, hi - lo, nevents, nmarkers, ninert, now_s() - tr0);
	}
	printf("periodic K=%-7lu rounds=%-3lu max_nevents=%u (round0 alone left %u)\n",
		   K, rounds, max_nevents, first_nevents);
	close_runtime();
	if (getenv("FEV_KEEP") == NULL)
		remove_tree(store);
	else
		printf("periodic store kept at %s\n", store);
	return 0;
}

int
main(int argc, char **argv)
{
	char store[] = "/tmp/fevbenchXXXXXX";
	char snapshots[1024];
	char manifest[1200];
	char frontier[1200];
	PsKey key = {6, 6, 6, 1, PS_KLASS_RELATION};	/* the FSM-like fork */
	PsKey pin_key = {6, 6, 21, 0, PS_KLASS_RELATION};
	unsigned char page[8192];
	uint64_t first_seq = 0, second_seq = 0, seq = 0;
	unsigned long K = argc > 1 ? strtoul(argv[1], NULL, 10) : 1000;
	int past = argc > 2 && strcmp(argv[2], "past") == 0;
	double t0, t1;
	int ok;

	if (argc > 2 && strcmp(argv[2], "periodic") == 0)
		return run_periodic(K);

	if (mkdtemp(store) == NULL)
		return 1;
	snprintf(snapshots, sizeof(snapshots), "%s/forkmeta_snapshots", store);
	snprintf(manifest, sizeof(manifest), "%s/forkmeta_manifest_v1", snapshots);
	snprintf(frontier, sizeof(frontier), "%s/page-prune.frontiers", store);
	flush_pages = getenv("FEV_FLUSH_PAGES") ? atoi(getenv("FEV_FLUSH_PAGES")) : 256;
	if (getenv("FEV_COMPACT_LAYERS"))
		compact_layers = atoi(getenv("FEV_COMPACT_LAYERS"));
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1073741824", 1);
	if (ps_core_open(store) != 0)
	{
		fprintf(stderr, "open failed\n");
		return 1;
	}
	ok = meta_request(PS_OP_CREATE, &pin_key, 100, 0, 0, 0) &&
		append_relation(&pin_key, 0, 100, page, 0, &first_seq) == 0 &&
		append_relation(&pin_key, 0, 200, page, 0, &second_seq) == 0;
	if (!ok)
	{
		fprintf(stderr, "pin history failed\n");
		return 1;
	}
	/* Filler pages so the pin history leaves the memtable at the default
	 * flush threshold (a frontier is published only from flushed history). */
	for (uint32_t b = 1; b <= (uint32_t) flush_pages + 2; b++)
		if (append_relation(&pin_key, b, 150, page, 0, &seq) != 0)
		{
			fprintf(stderr, "filler failed\n");
			return 1;
		}
	if (!set_pin(200, second_seq, 1) || !run_maintenance_until(frontier, 1))
	{
		fprintf(stderr, "frontier failed\n");
		return 1;
	}
	if (!meta_request(PS_OP_CREATE, &key, 300, 0, 0, 0))
	{
		fprintf(stderr, "create failed\n");
		return 1;
	}

	/* (a) live path: K WAL-less rewrites of block 0.  The first grows the
	 * fork (SEG_GROW_BOUND, activated), every later one is commit-class
	 * (inert SEG_COMMIT_BOUND) at the same floor LSN 300. */
	{
		unsigned long chunk = K / 10 ? K / 10 : 1;
		double tstart = now_s();
		double tchunk = tstart;

		for (unsigned long i = 0; i < K; i++)
		{
			if (append_relation(&key, 0, 0, page, (unsigned char) i, &seq) != 0)
			{
				fprintf(stderr, "write %lu failed\n", i);
				return 1;
			}
			if ((i + 1) % chunk == 0)
			{
				double t = now_s();

				printf("live   K=%-7lu events~%-7lu chunk=%lu  %.3f us/write\n",
					   K, i + 1, chunk, (t - tchunk) * 1e6 / (double) chunk);
				tchunk = t;
			}
		}
		printf("live   K=%-7lu total %.3f s  (%.3f us/write avg)\n", K,
			   now_s() - tstart, (now_s() - tstart) * 1e6 / (double) K);
	}
	printf("versions of block 0 after writes: %u\n",
		   ps_test_page_version_count(0, &key, 0));

	if (past)
	{
		uint64_t s3 = 0, s4 = 0;

		if (append_relation(&pin_key, 0, 400, page, 0, &s3) != 0 ||
			append_relation(&pin_key, 0, 500, page, 0, &s4) != 0)
		{
			fprintf(stderr, "advance history failed\n");
			return 1;
		}
		for (uint32_t b = 1; b <= (uint32_t) flush_pages + 2; b++)
			if (append_relation(&pin_key, b, 450, page, 0, &seq) != 0)
			{
				fprintf(stderr, "filler2 failed\n");
				return 1;
			}
		if (!set_pin(500, s4, 2))
		{
			fprintf(stderr, "advance pin failed\n");
			return 1;
		}
		{
			uint32_t prev = ps_test_page_version_count(0, &key, 0);
			int stable = 0;

			for (int i = 0; i < 400 && stable < 20; i++)
			{
				uint32_t cur;

				(void) ps_core_maintenance();
				usleep(5000);
				cur = ps_test_page_version_count(0, &key, 0);
				stable = cur == prev ? stable + 1 : 0;
				prev = cur;
			}
		}
		printf("versions of block 0 after frontier advance: %u\n",
			   ps_test_page_version_count(0, &key, 0));
	}

	/* (b) reopen before cutover: source log preload + segment/layer replay */
	close_runtime();
	t0 = now_s();
	if (ps_core_open(store) != 0)
	{
		fprintf(stderr, "reopen(pre-cutover) failed\n");
		return 1;
	}
	t1 = now_s();
	printf("reopen-pre-cutover  K=%-7lu %.3f s\n", K, t1 - t0);

	/* (c) cutover */
	setenv("PAGESTORE_FORKMETA_SNAPSHOT_TRIGGER_BYTES", "1024", 1);
	t0 = now_s();
	if (!run_maintenance_until(manifest, 1))
	{
		fprintf(stderr, "cutover did not publish\n");
		return 1;
	}
	t1 = now_s();
	printf("cutover             K=%-7lu %.3f s\n", K, t1 - t0);
	{
		PsForkmetaSnapshot selected;

		if (ps_forkmeta_snapshot_open(&selected, snapshots) == 0)
		{
			printf("selected cutoff lsn=%llu seq=%llu (markers at lsn 300 are %s)\n",
				   (unsigned long long) selected.cutoff_lsn,
				   (unsigned long long) selected.cutoff_admission_seq,
				   selected.cutoff_lsn > 300 ? "PAST (builder evaluates page_retained)" : "FUTURE (kept in tail)");
			ps_forkmeta_snapshot_close(&selected);
		}
	}
	{
		struct stat st;
		char cp[1400];
		DIR *d = opendir(snapshots);
		struct dirent *de;
		long long bytes = 0;

		while (d && (de = readdir(d)) != NULL)
		{
			snprintf(cp, sizeof(cp), "%s/%s", snapshots, de->d_name);
			if (stat(cp, &st) == 0 && S_ISREG(st.st_mode))
				bytes += st.st_size;
		}
		if (d)
			closedir(d);
		printf("snapshot dir bytes: %lld (record=%zu -> ~%lld records)\n",
			   bytes, (size_t) 64, bytes / 64);
	}

	/* (d) reopen after cutover: snapshot load + segment/layer replay */
	close_runtime();
	t0 = now_s();
	if (ps_core_open(store) != 0)
	{
		fprintf(stderr, "reopen(post-cutover) failed\n");
		return 1;
	}
	t1 = now_s();
	printf("reopen-post-cutover K=%-7lu %.3f s\n", K, t1 - t0);
	memset(page, 0, sizeof(page));
	if (read_resolve(0, &key, 0, UINT64_MAX, 0, page, NULL) != 1 ||
		page[128] != (unsigned char) (K - 1))
	{
		fprintf(stderr, "newest tag mismatch after reopen (%u)\n", page[128]);
		return 1;
	}
	close_runtime();
	if (getenv("FEV_KEEP") == NULL)
		remove_tree(store);
	else
		printf("store kept at %s\n", store);
	return 0;
}
