/* Real POSIX-daemon integration coverage for nonblocking page backpressure. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"

static int checks;
static int failed;

static void
check(int ok, const char *name)
{
	checks++;
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", name);
		failed++;
	}
}

static void
sleep_ms(long milliseconds)
{
	struct timespec ts;

	ts.tv_sec = milliseconds / 1000;
	ts.tv_nsec = (milliseconds % 1000) * 1000000L;
	nanosleep(&ts, NULL);
}

static void
fill_page(unsigned char *page, uint32_t page_size, unsigned char tag)
{
	uint64_t lsn = 1000 + tag;
	uint32_t hi = (uint32_t) (lsn >> 32);
	uint32_t lo = (uint32_t) lsn;

	memcpy(page, &hi, sizeof(hi));
	memcpy(page + sizeof(hi), &lo, sizeof(lo));
	for (uint32_t i = 8; i < page_size; i++)
		page[i] = (unsigned char) (tag ^ (i & 0xff));
}

static int
page_has_tag(const unsigned char *page, uint32_t page_size, unsigned char tag)
{
	for (uint32_t i = 8; i < page_size; i++)
		if (page[i] != (unsigned char) (tag ^ (i & 0xff)))
			return 0;
	return 1;
}

static void
set_relation_key(PsChannel *ch, uint32_t rel)
{
	memset(&ch->key, 0, sizeof(ch->key));
	ch->key.spcOid = 1;
	ch->key.dbOid = 1;
	ch->key.relNumber = rel;
	ch->key.klass = PS_KLASS_RELATION;
	ch->timeline = 0;
	ch->incarnation = 0;
	ch->req_lsn = 0;
	ch->req_seq = 0;
}

static void
submit_and_wait(PsChannel *ch)
{
	ps_store_release(&ch->state, PS_STATE_REQUEST);
	while (ps_load_acquire(&ch->state) != PS_STATE_DONE)
		sched_yield();
}

static int
wait_for_state(PsChannel *ch, uint32_t state, int milliseconds)
{
	int steps = milliseconds;

	for (int i = 0; i < steps; i++)
	{
		if (ps_load_acquire(&ch->state) == state)
			return 1;
		sleep_ms(1);
	}
	return ps_load_acquire(&ch->state) == state;
}

static int
wait_for_ready(const char *shm_name)
{
	for (int i = 0; i < 500; i++)
	{
		int fd = shm_open(shm_name, O_RDONLY, 0600);

		if (fd >= 0)
		{
			PsShmHeader *hdr = mmap(NULL, sizeof(*hdr), PROT_READ, MAP_SHARED,
									fd, 0);

			if (hdr != MAP_FAILED)
			{
				int ready = hdr->magic == PS_SHM_MAGIC &&
					hdr->version == PS_SHM_VERSION &&
					hdr->startup_state == PS_SHM_READY;

				munmap(hdr, sizeof(*hdr));
				close(fd);
				if (ready)
					return 1;
			}
			else
				close(fd);
		}
		sleep_ms(10);
	}
	return 0;
}

static int
wait_for_exit(pid_t pid, int milliseconds)
{
	for (int i = 0; i < milliseconds; i++)
	{
		int status;
		pid_t result = waitpid(pid, &status, WNOHANG);

		if (result == pid)
			return WIFEXITED(status) && WEXITSTATUS(status) == 0;
		if (result < 0)
			return 0;
		sleep_ms(1);
	}
	return 0;
}

static pid_t
spawn_daemon(const char *daemon_path, const char *shm_name,
			 const char *store_dir, const char *pause_file, int enable_bp)
{
	pid_t pid = fork();

	if (pid == 0)
	{
		if (enable_bp)
			execl(daemon_path, daemon_path, "--shm", shm_name, "--store",
				  store_dir, "--page-size", "8192", "--segment-size", "32768",
				  "--nshards", "1", "--flush-pages", "1", "--compact-layers",
				  "100", "--segment-gc", "1", "--page-high-water-bytes",
				  "32768", "--page-catch-up-bytes", "1",
				  "--test-maintenance-pause-file", pause_file, (char *) NULL);
		else
			execl(daemon_path, daemon_path, "--shm", shm_name, "--store",
				  store_dir, "--page-size", "8192", "--segment-size", "32768",
				  "--nshards", "1", "--flush-pages", "1", "--compact-layers",
				  "100", "--segment-gc", "1", "--test-maintenance-pause-file",
				  pause_file, (char *) NULL);
		_exit(127);
	}
	return pid;
}

static int
run_test(const char *daemon_path)
{
	char shm_name[64];
	char store_dir[128];
	char pause_file[128];
	const uint32_t page_size = 8192;
	const uint32_t rel = 4242;
	pid_t pid = -1;
	int fd = -1;
	void *shm = MAP_FAILED;
	PsShmHeader *hdr;
	PsChannel *reader = NULL;
	PsChannel *pending = NULL;
	unsigned char *readback = NULL;
	int cleanup_kill = 0;
	int pause_fd;
	int ready;

	snprintf(shm_name, sizeof(shm_name), "/psbp_%d", (int) getpid());
	snprintf(store_dir, sizeof(store_dir), "/tmp/psbp_store_%d", (int) getpid());
	snprintf(pause_file, sizeof(pause_file), "/tmp/psbp_pause_%d", (int) getpid());
	shm_unlink(shm_name);
	unlink(pause_file);
	/* Keep maintenance paused for both phases.  The first phase seeds the
	 * durable debt with backpressure disabled; the second phase refreshes it at
	 * startup and enters throttle before accepting test requests. */
	pause_fd = open(pause_file, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(pause_fd >= 0, "integration creates maintenance pause file");
	if (pause_fd >= 0)
		close(pause_fd);
	{
		char command[256];

		snprintf(command, sizeof(command), "rm -rf '%s'", store_dir);
		(void) system(command);
	}

	/* Phase 1: seed complete reclaimable segments while the controller is
	 * disabled, so no prefill request can ever be deferred. */
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file, 0);
	check(pid > 0, "real daemon starts for backpressure integration");
	if (pid <= 0)
		goto cleanup;
	cleanup_kill = 1;
	ready = wait_for_ready(shm_name);
	check(ready, "seed daemon publishes ready state");
	if (!ready)
		goto cleanup;

	fd = shm_open(shm_name, O_RDWR, 0600);
	check(fd >= 0, "integration client opens daemon shared memory");
	if (fd < 0)
		goto cleanup;
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	check(shm != MAP_FAILED, "integration client maps daemon shared memory");
	if (shm == MAP_FAILED)
		goto cleanup;
	hdr = (PsShmHeader *) shm;
	reader = ps_channel(shm, 0);
	check(ps_cas(&reader->claimed, 0, 1), "integration claims seed channel");
	if (ps_load_acquire(&reader->claimed) == 0)
		goto cleanup;
	readback = malloc(page_size);
	check(readback != NULL, "integration allocates page buffers");
	if (readback == NULL)
		goto cleanup;

	set_relation_key(reader, rel);
	reader->opcode = PS_OP_CREATE;
	submit_and_wait(reader);
	check(reader->status == PS_STATUS_OK, "integration creates relation");
	for (uint32_t block = 0; block < 5; block++)
	{
		set_relation_key(reader, rel);
		reader->opcode = PS_OP_WRITEV;
		reader->blocknum = block;
		reader->nblocks = 1;
		fill_page(reader->data, page_size, (unsigned char) (10 + block));
		submit_and_wait(reader);
		check(reader->status == PS_STATUS_OK, "integration flushes page mutation");
	}
	ps_store_release(&reader->claimed, 0);
	munmap(shm, PS_SHM_SIZE);
	shm = MAP_FAILED;
	close(fd);
	fd = -1;
	reader = NULL;
	kill(pid, SIGTERM);
	if (!wait_for_exit(pid, 5000))
	{
		check(0, "seed daemon stops cleanly");
		goto cleanup;
	}
	cleanup_kill = 0;
	pid = -1;

	/* Phase 2: startup recovery retains the seed's complete segment debt. */
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file, 1);
	check(pid > 0, "throttle daemon restarts for backpressure integration");
	if (pid <= 0)
		goto cleanup;
	cleanup_kill = 1;
	ready = wait_for_ready(shm_name);
	check(ready, "throttle daemon publishes ready state");
	if (!ready)
		goto cleanup;
	fd = shm_open(shm_name, O_RDWR, 0600);
	check(fd >= 0, "integration reopens daemon shared memory");
	if (fd < 0)
		goto cleanup;
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	check(shm != MAP_FAILED, "integration remaps daemon shared memory");
	if (shm == MAP_FAILED)
		goto cleanup;
	hdr = (PsShmHeader *) shm;
	reader = ps_channel(shm, 0);
	pending = ps_channel(shm, 1);
	check(ps_cas(&reader->claimed, 0, 1) && ps_cas(&pending->claimed, 0, 1),
		  "integration claims two channels on one shard");
	if (ps_load_acquire(&reader->claimed) == 0 ||
		ps_load_acquire(&pending->claimed) == 0)
		goto cleanup;

	for (int i = 0; i < 500; i++)
	{
		if (ps_load_acquire(&hdr->page_backpressure.throttled) != 0)
			break;
		sleep_ms(1);
	}
	check(ps_load_acquire(&hdr->page_backpressure.throttled) != 0 &&
		  ps_load_acquire_u64(&hdr->page_backpressure.lag_bytes) >= 32768,
		  "real maintenance reports complete page-segment debt and throttles");

	set_relation_key(pending, rel);
	pending->opcode = PS_OP_WRITEV;
	pending->blocknum = 20;
	pending->nblocks = 1;
	fill_page(pending->data, page_size, 99);
	ps_store_release(&pending->state, PS_STATE_REQUEST);
	sleep_ms(100);
	check(ps_load_acquire(&pending->state) == PS_STATE_REQUEST,
		  "throttled mutation stays pending without blocking the shard worker");

	set_relation_key(reader, rel);
	reader->opcode = PS_OP_READV;
	reader->blocknum = 0;
	reader->nblocks = 1;
	ps_store_release(&reader->state, PS_STATE_REQUEST);
	check(wait_for_state(reader, PS_STATE_DONE, 1000) &&
		  reader->status == PS_STATUS_OK,
		  "read on the same shard completes while mutation is deferred");
	memcpy(readback, reader->data, page_size);
	check(page_has_tag(readback, page_size, 10),
		  "same-shard read returns the existing page during throttle");

	unlink(pause_file);
	check(wait_for_state(pending, PS_STATE_DONE, 5000) &&
		  pending->status == PS_STATUS_OK,
		  "maintenance catch-up releases the deferred mutation");
	check(ps_load_acquire(&hdr->page_backpressure.throttled) == 0 &&
		  ps_load_acquire_u64(&hdr->page_backpressure.throttle_exits) != 0,
		  "controller exits throttle after maintenance catch-up");

	/* Make a second complete-segment debt in one already-admitted vectored
	 * mutation.  This prepares a fresh throttle state for the shutdown phase
	 * without allowing a multi-request prefill to race its own controller. */
	pause_fd = open(pause_file, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(pause_fd >= 0, "integration re-pauses maintenance for shutdown phase");
	if (pause_fd >= 0)
		close(pause_fd);
	set_relation_key(reader, rel);
	reader->opcode = PS_OP_WRITEV;
	reader->blocknum = 30;
	reader->nblocks = 5;
	for (uint32_t block = 0; block < 5; block++)
		fill_page(reader->data + (size_t) block * page_size, page_size,
				  (unsigned char) (40 + block));
	submit_and_wait(reader);
	check(reader->status == PS_STATUS_OK,
		  "integration seeds shutdown throttle in one admitted mutation");

	ps_store_release(&pending->claimed, 0);
	ps_store_release(&reader->claimed, 0);
	munmap(shm, PS_SHM_SIZE);
	shm = MAP_FAILED;
	close(fd);
	fd = -1;
	pending = NULL;
	reader = NULL;
	kill(pid, SIGTERM);
	if (!wait_for_exit(pid, 5000))
	{
		check(0, "second daemon stops cleanly before shutdown restart");
		goto cleanup;
	}
	cleanup_kill = 0;
	pid = -1;

	/* Final phase: startup refresh enters throttle before any request, then a
	 * deferred mutation is present when SIGTERM arrives. */
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file, 1);
	check(pid > 0, "shutdown-phase daemon restarts");
	if (pid <= 0)
		goto cleanup;
	cleanup_kill = 1;
	ready = wait_for_ready(shm_name);
	check(ready, "shutdown-phase daemon publishes ready state");
	if (!ready)
		goto cleanup;
	fd = shm_open(shm_name, O_RDWR, 0600);
	check(fd >= 0, "shutdown-phase client opens shared memory");
	if (fd < 0)
		goto cleanup;
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	check(shm != MAP_FAILED, "shutdown-phase client maps shared memory");
	if (shm == MAP_FAILED)
		goto cleanup;
	hdr = (PsShmHeader *) shm;
	reader = ps_channel(shm, 0);
	pending = ps_channel(shm, 1);
	check(ps_cas(&reader->claimed, 0, 1) && ps_cas(&pending->claimed, 0, 1),
		  "shutdown phase claims two channels");
	if (ps_load_acquire(&reader->claimed) == 0 ||
		ps_load_acquire(&pending->claimed) == 0)
		goto cleanup;
	for (int i = 0; i < 500; i++)
	{
		if (ps_load_acquire(&hdr->page_backpressure.throttled) != 0)
			break;
		sleep_ms(1);
	}
	check(ps_load_acquire(&hdr->page_backpressure.throttled) != 0,
		  "shutdown phase starts throttled from recovered debt");
	set_relation_key(pending, rel);
	pending->opcode = PS_OP_WRITEV;
	pending->blocknum = 50;
	pending->nblocks = 1;
	fill_page(pending->data, page_size, 99);
	ps_store_release(&pending->state, PS_STATE_REQUEST);
	sleep_ms(100);
	check(ps_load_acquire(&pending->state) == PS_STATE_REQUEST,
		  "shutdown phase has a mutation pending under throttle");
	kill(pid, SIGTERM);
	if (wait_for_exit(pid, 2000))
	{
		cleanup_kill = 0;
		pid = -1;
		check(1, "SIGTERM exits the real daemon promptly while throttled");
	}
	else
	{
		check(0, "SIGTERM exits the real daemon promptly while throttled");
		goto cleanup;
	}

cleanup:
	if (cleanup_kill)
	{
		kill(pid, SIGTERM);
		if (!wait_for_exit(pid, 2000))
		{
			kill(pid, SIGKILL);
			waitpid(pid, NULL, 0);
		}
	}
	if (pending != NULL)
		ps_store_release(&pending->claimed, 0);
	if (reader != NULL)
		ps_store_release(&reader->claimed, 0);
	if (shm != MAP_FAILED)
		munmap(shm, PS_SHM_SIZE);
	if (fd >= 0)
		close(fd);
	shm_unlink(shm_name);
	unlink(pause_file);
	{
		char command[256];

		snprintf(command, sizeof(command), "rm -rf '%s'", store_dir);
		(void) system(command);
	}
	free(readback);
	return failed == 0;
}

int
main(int argc, char **argv)
{
	if (argc != 2)
	{
		fprintf(stderr, "usage: %s <path-to-pagestore_daemon>\n", argv[0]);
		return 2;
	}
	(void) run_test(argv[1]);
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
