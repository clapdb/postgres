/* Real POSIX-daemon integration coverage for nonblocking page backpressure. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <poll.h>
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

#define SHUTDOWN_CANCEL_READY_BYTE 0x5a

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
	ps_request_generation_next(ch);
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
wait_for_ready_byte(int fd, int milliseconds)
{
	struct pollfd pollfd;
	unsigned char byte;

	pollfd.fd = fd;
	pollfd.events = POLLIN;
	pollfd.revents = 0;
	for (int i = 0; i < milliseconds; i++)
	{
		int result;

		pollfd.revents = 0;
		result = poll(&pollfd, 1, 1);
		if (result < 0 && errno == EINTR)
			continue;
		if (result < 0)
			return 0;
		if (result == 0)
			continue;
		if ((pollfd.revents & (POLLERR | POLLNVAL)) != 0)
			return 0;
		if ((pollfd.revents & (POLLIN | POLLHUP)) != 0)
		{
			ssize_t n;

			do
				n = read(fd, &byte, 1);
			while (n < 0 && errno == EINTR);
			return n == 1 && byte == SHUTDOWN_CANCEL_READY_BYTE;
		}
	}
	return 0;
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

static void
test_unsupported_backend_cli_case(const char *daemon_path,
								const char *option, const char *value,
								const char *case_name)
{
	char shm_name[64];
	char store_dir[128];
	char output[1024];
	int pipefd[2];
	pid_t pid;
	int status = 0;
	ssize_t n;
	size_t used = 0;

	snprintf(shm_name, sizeof(shm_name), "/psbp_cli_%d", (int) getpid());
	snprintf(store_dir, sizeof(store_dir), "/tmp/psbp_cli_store_%d",
			 (int) getpid());
	shm_unlink(shm_name);
	if (pipe(pipefd) != 0)
	{
		check(0, "unsupported-backend CLI test creates stderr pipe");
		return;
	}
	pid = fork();
	if (pid == 0)
	{
		dup2(pipefd[1], STDERR_FILENO);
		close(pipefd[0]);
		close(pipefd[1]);
		execl(daemon_path, daemon_path, "--shm", shm_name, "--store",
			  store_dir, option, value,
			  (char *) NULL);
		_exit(127);
	}
	close(pipefd[1]);
	memset(output, 0, sizeof(output));
	while (used + 1 < sizeof(output))
	{
		n = read(pipefd[0], output + used, sizeof(output) - used - 1);
		if (n > 0)
		{
			used += (size_t) n;
			continue;
		}
		if (n < 0 && errno == EINTR)
			continue;
		break;
	}
	close(pipefd[0]);
	check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			WIFEXITED(status) && WEXITSTATUS(status) == 2,
		  case_name);
	check(strstr(output, "backpressure requires the POSIX storage backend") != NULL,
		  "unsupported-backend CLI reports the POSIX-only controller contract");
	shm_unlink(shm_name);
}

static void
test_unsupported_backend_cli(const char *daemon_path)
{
	test_unsupported_backend_cli_case(daemon_path,
								  "--page-high-water-bytes", "1",
								  "non-POSIX CLI rejects page high-water backpressure");
	test_unsupported_backend_cli_case(daemon_path,
								  "--page-catch-up-bytes", "1",
								  "non-POSIX CLI rejects page catch-up-only backpressure");
	test_unsupported_backend_cli_case(daemon_path,
								  "--wal-catch-up-bytes", "1",
								  "non-POSIX CLI rejects WAL catch-up-only backpressure");
}

static pid_t
spawn_daemon(const char *daemon_path, const char *shm_name,
				 const char *store_dir, const char *pause_file,
					 const char *shutdown_pause_file,
					 int ready_fd, int ready_read_fd, int enable_bp,
					 int enable_forkmeta_bp)
{
	char ready_fd_text[32];
	pid_t pid = fork();

	if (pid == 0)
	{
		if (ready_read_fd >= 0)
			close(ready_read_fd);
		if (ready_fd >= 0)
		{
			int flags = fcntl(ready_fd, F_GETFD);

			if (flags < 0 || fcntl(ready_fd, F_SETFD,
								 flags & ~FD_CLOEXEC) < 0)
			{
				close(ready_fd);
				_exit(127);
			}
		}
		if (ready_fd >= 0)
			snprintf(ready_fd_text, sizeof(ready_fd_text), "%d", ready_fd);
		if (enable_bp)
			execl(daemon_path, daemon_path, "--shm", shm_name, "--store",
				  store_dir, "--page-size", "8192", "--segment-size", "32768",
				  "--nshards", "1", "--flush-pages", "1", "--compact-layers",
				  "100", "--segment-gc", "1", "--page-high-water-bytes",
				  "32768", "--page-catch-up-bytes", "1",
				  "--test-maintenance-pause-file", pause_file,
				  shutdown_pause_file != NULL ?
				  "--test-shutdown-cancel-pause-file" : NULL,
				  shutdown_pause_file != NULL ? shutdown_pause_file : NULL,
				  ready_fd >= 0 ? "--test-shutdown-cancel-ready-fd" : NULL,
				  ready_fd >= 0 ? ready_fd_text : NULL,
				  (char *) NULL);
		else if (enable_forkmeta_bp)
			execl(daemon_path, daemon_path, "--shm", shm_name, "--store",
				  store_dir, "--page-size", "8192", "--segment-size", "32768",
				  "--nshards", "1", "--flush-pages", "1", "--compact-layers",
				  "100", "--segment-gc", "1", "--forkmeta-high-water-bytes",
				  "180", "--forkmeta-catch-up-bytes", "20",
				  "--test-maintenance-pause-file", pause_file,
				  (char *) NULL);
		else
			execl(daemon_path, daemon_path, "--shm", shm_name, "--store",
				  store_dir, "--page-size", "8192", "--segment-size", "32768",
				  "--nshards", "1", "--flush-pages", "1", "--compact-layers",
				  "100", "--segment-gc", "1", "--test-maintenance-pause-file",
				  pause_file,
				  shutdown_pause_file != NULL ?
				  "--test-shutdown-cancel-pause-file" : NULL,
				  shutdown_pause_file != NULL ? shutdown_pause_file : NULL,
				  ready_fd >= 0 ? "--test-shutdown-cancel-ready-fd" : NULL,
				  ready_fd >= 0 ? ready_fd_text : NULL,
				  (char *) NULL);
		if (ready_fd >= 0)
			close(ready_fd);
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
	char shutdown_pause_file[128];
	char empty_segment[256];
	const uint32_t page_size = 8192;
	const uint32_t rel = 4242;
	pid_t pid = -1;
	int fd = -1;
	void *shm = MAP_FAILED;
	PsShmHeader *hdr;
	PsChannel *reader = NULL;
	PsChannel *pending = NULL;
	PsChannel *aba = NULL;
	unsigned char *readback = NULL;
	int cleanup_kill = 0;
	int pause_fd;
	int ready;
	int ready_pipe[2] = {-1, -1};

	snprintf(shm_name, sizeof(shm_name), "/psbp_%d", (int) getpid());
	snprintf(store_dir, sizeof(store_dir), "/tmp/psbp_store_%d", (int) getpid());
	snprintf(pause_file, sizeof(pause_file), "/tmp/psbp_pause_%d", (int) getpid());
	snprintf(shutdown_pause_file, sizeof(shutdown_pause_file),
			 "/tmp/psbp_shutdown_pause_%d", (int) getpid());
	snprintf(empty_segment, sizeof(empty_segment), "%s/seg_%08d", store_dir, 0);
	shm_unlink(shm_name);
	unlink(pause_file);
	unlink(shutdown_pause_file);
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
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file,
					   NULL, -1, -1, 0, 0);
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
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file,
					   NULL, -1, -1, 1, 0);
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
	ps_request_generation_next(pending);
	ps_store_release(&pending->state, PS_STATE_REQUEST);
	sleep_ms(100);
	check(ps_load_acquire(&pending->state) == PS_STATE_REQUEST,
		  "throttled mutation stays pending without blocking the shard worker");

	set_relation_key(reader, rel);
	reader->opcode = PS_OP_READV;
	reader->blocknum = 0;
	reader->nblocks = 1;
	ps_request_generation_next(reader);
	ps_store_release(&reader->state, PS_STATE_REQUEST);
	check(wait_for_state(reader, PS_STATE_DONE, 1000) &&
		  reader->status == PS_STATUS_OK,
		  "read on the same shard completes while mutation is deferred");
	memcpy(readback, reader->data, page_size);
	check(page_has_tag(readback, page_size, 10),
		  "same-shard read returns the existing page during throttle");

	/* These metadata probes are reads even though request_is_write() retains
	 * its historical default-true behavior for unknown opcodes. */
	reader->opcode = PS_OP_TIMELINE_INFO;
	reader->timeline = 0;
	reader->incarnation = 1;
	reader->req_lsn = 0;
	reader->req_seq = 0;
	ps_request_generation_next(reader);
	ps_store_release(&reader->state, PS_STATE_REQUEST);
	check(wait_for_state(reader, PS_STATE_DONE, 1000) &&
		  reader->status == PS_STATUS_OK,
		  "TIMELINE_INFO continues during page throttle");
	reader->opcode = PS_OP_CHECK_BRANCH;
	reader->timeline = 77;
	reader->parent_timeline = 0;
	reader->incarnation = 0;
	reader->req_lsn = 0;
	reader->req_seq = 1;
	ps_request_generation_next(reader);
	ps_store_release(&reader->state, PS_STATE_REQUEST);
	check(wait_for_state(reader, PS_STATE_DONE, 1000) &&
		  reader->status == PS_STATUS_OK,
		  "CHECK_BRANCH continues during page throttle");

	unlink(pause_file);
	check(wait_for_state(pending, PS_STATE_DONE, 5000) &&
		  pending->status == PS_STATUS_OK,
		  "maintenance catch-up releases the deferred mutation");
	check(ps_load_acquire(&hdr->page_backpressure.throttled) == 0 &&
		  ps_load_acquire_u64(&hdr->page_backpressure.throttle_exits) != 0,
		  "controller exits throttle after maintenance catch-up");

	/* Restart with the old debt fully caught up.  This specifically exercises
	 * rebuilding gc_next_seg: deleted historical segment ids must not become
	 * phantom debt when the process starts with a fresh in-memory cursor. */
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
		check(0, "daemon stops cleanly before page cursor restart");
		goto cleanup;
	}
	cleanup_kill = 0;
	pid = -1;
	{
		int empty_fd = open(empty_segment, O_CREAT | O_TRUNC | O_WRONLY, 0600);

		check(empty_fd >= 0,
			  "integration creates an empty covered historical segment");
		if (empty_fd >= 0)
			close(empty_fd);
	}
	pause_fd = open(pause_file, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(pause_fd >= 0, "integration pauses maintenance for cursor restart");
	if (pause_fd >= 0)
		close(pause_fd);
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file,
					   NULL, -1, -1, 1, 0);
	check(pid > 0, "cursor-restart daemon starts");
	if (pid <= 0)
		goto cleanup;
	cleanup_kill = 1;
	ready = wait_for_ready(shm_name);
	check(ready, "cursor-restart daemon publishes ready state");
	if (!ready)
		goto cleanup;
	fd = shm_open(shm_name, O_RDWR, 0600);
	check(fd >= 0, "cursor-restart client opens shared memory");
	if (fd < 0)
		goto cleanup;
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	check(shm != MAP_FAILED, "cursor-restart client maps shared memory");
	if (shm == MAP_FAILED)
		goto cleanup;
	hdr = (PsShmHeader *) shm;
	reader = ps_channel(shm, 0);
	check(ps_cas(&reader->claimed, 0, 1),
		  "cursor-restart claims a channel");
	check(ps_load_acquire_u64(&hdr->page_backpressure.lag_bytes) == 0 &&
		  ps_load_acquire(&hdr->page_backpressure.throttled) == 0,
		  "restart ignores deleted and empty historical page segments");
	unlink(pause_file);
	for (int i = 0; i < 500 && access(empty_segment, F_OK) == 0; i++)
		sleep_ms(1);
	check(access(empty_segment, F_OK) != 0,
		  "maintenance can unlink an empty covered page segment");
	pause_fd = open(pause_file, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(pause_fd >= 0, "integration re-pauses after empty-segment cleanup");
	if (pause_fd >= 0)
		close(pause_fd);

	/* Seed one new covered segment only after the no-false-throttle assertion;
	 * this leaves a fresh, deterministic debt for the shutdown restart. */
	set_relation_key(reader, rel);
	reader->opcode = PS_OP_WRITEV;
	reader->blocknum = 30;
	reader->nblocks = 5;
	for (uint32_t block = 0; block < 5; block++)
		fill_page(reader->data + (size_t) block * page_size, page_size,
				  (unsigned char) (40 + block));
	submit_and_wait(reader);
	check(reader->status == PS_STATUS_OK,
		  "cursor-restart seeds shutdown throttle in one admitted mutation");

	ps_store_release(&reader->claimed, 0);
	munmap(shm, PS_SHM_SIZE);
	shm = MAP_FAILED;
	close(fd);
	fd = -1;
	reader = NULL;
	kill(pid, SIGTERM);
	if (!wait_for_exit(pid, 5000))
	{
		check(0, "cursor-restart daemon stops cleanly");
		goto cleanup;
	}
	cleanup_kill = 0;
	pid = -1;

	/* Final phase: startup refresh enters throttle before any request, then two
	 * deferred mutations are present when SIGTERM arrives.  One remains a
	 * normal deferred request; the other is reused after workers have joined to
	 * make the shutdown ABA race deterministic. */
	{
		int shutdown_fd = open(shutdown_pause_file, O_CREAT | O_EXCL | O_WRONLY,
						 0600);

		check(shutdown_fd >= 0, "shutdown phase creates cancellation pause file");
		if (shutdown_fd >= 0)
			close(shutdown_fd);
	}
	check(pipe(ready_pipe) == 0, "shutdown phase creates ready pipe");
	if (ready_pipe[0] < 0 || ready_pipe[1] < 0)
		goto cleanup;
	{
		int flags = fcntl(ready_pipe[1], F_GETFD);
		int set_flags = flags < 0 ? -1 : fcntl(ready_pipe[1], F_SETFD,
										flags & ~FD_CLOEXEC);

		check(flags >= 0 && set_flags == 0,
			  "ready pipe write fd is inherited across exec");
		if (flags < 0 || set_flags != 0)
			goto cleanup;
	}
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file,
					   shutdown_pause_file, ready_pipe[1], ready_pipe[0], 1, 0);
	close(ready_pipe[1]);
	ready_pipe[1] = -1;
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
	aba = ps_channel(shm, 2);
	check(ps_cas(&reader->claimed, 0, 1) && ps_cas(&pending->claimed, 0, 1) &&
		  ps_cas(&aba->claimed, 0, 1), "shutdown phase claims three channels");
	if (ps_load_acquire(&reader->claimed) == 0 ||
		ps_load_acquire(&pending->claimed) == 0 ||
		ps_load_acquire(&aba->claimed) == 0)
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
	ps_request_generation_next(pending);
	ps_store_release(&pending->state, PS_STATE_REQUEST);
	sleep_ms(100);
	check(ps_load_acquire(&pending->state) == PS_STATE_REQUEST,
		  "shutdown phase has a mutation pending under throttle");
	set_relation_key(aba, rel);
	aba->opcode = PS_OP_WRITEV;
	aba->blocknum = 51;
	aba->nblocks = 1;
	fill_page(aba->data, page_size, 100);
	ps_request_generation_next(aba);
	ps_store_release(&aba->state, PS_STATE_REQUEST);
	sleep_ms(100);
	check(ps_load_acquire(&aba->state) == PS_STATE_REQUEST,
		  "shutdown ABA setup has a second deferred mutation");
	kill(pid, SIGTERM);
	/* startup_state changes in the signal handler; the test-only daemon seam
	 * then keeps the parent after all workers have joined. */
	if (!wait_for_ready_byte(ready_pipe[0], 2000))
	{
		check(0, "ready pipe confirms workers joined before cancellation");
		goto cleanup;
	}
	check(1, "ready pipe confirms workers joined before cancellation");
	close(ready_pipe[0]);
	ready_pipe[0] = -1;
	{
		uint64_t new_generation;

		ps_store_release(&aba->state, PS_STATE_IDLE);
		aba->status = PS_STATUS_STALE;
		new_generation = ps_request_generation_next(aba);
		ps_store_release(&aba->state, PS_STATE_REQUEST);
		unlink(shutdown_pause_file);
		if (wait_for_exit(pid, 2000))
		{
			cleanup_kill = 0;
			pid = -1;
			check(1, "SIGTERM exits the real daemon promptly while throttled");
			check(ps_load_acquire(&pending->state) == PS_STATE_DONE &&
				  pending->status == PS_STATUS_ERROR,
				  "shutdown completes the normal deferred mutation with ERROR/DONE");
			check(ps_load_acquire(&aba->state) == PS_STATE_REQUEST &&
				  aba->status == PS_STATUS_STALE &&
				  aba->request_generation == new_generation,
				  "shutdown does not complete or pollute a reused generation");
			check(ps_load_acquire_u64(&hdr->page_backpressure.foreground_wait_ns) != 0,
				  "shutdown accounts deferred foreground wait before teardown");
		}
		else
		{
			check(0, "SIGTERM exits the real daemon promptly while throttled");
			goto cleanup;
		}
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
	unlink(shutdown_pause_file);
	if (ready_pipe[0] >= 0)
		close(ready_pipe[0]);
	if (ready_pipe[1] >= 0)
		close(ready_pipe[1]);
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

/* Keep the forkmeta admission proof small and independent from page/WAL debt:
 * CREATE is deferred after the source grows past its forkmeta controller, but
 * WAL_APPEND and a read-only timeline probe still complete on the same shard. */
static void
test_forkmeta_request_classification(const char *daemon_path)
{
	char shm_name[64];
	char store_dir[128];
	char pause_file[128];
	PsShmHeader *hdr = NULL;
	PsChannel *writer = NULL;
	PsChannel *pending = NULL;
	void *shm = MAP_FAILED;
	pid_t pid = -1;
	int fd = -1;
	int pause_fd;
	int ready;

	snprintf(shm_name, sizeof(shm_name), "/psforkmeta_%d", (int) getpid());
	snprintf(store_dir, sizeof(store_dir), "/tmp/psforkmeta_store_%d", (int) getpid());
	snprintf(pause_file, sizeof(pause_file), "/tmp/psforkmeta_pause_%d", (int) getpid());
	shm_unlink(shm_name);
	unlink(pause_file);
	{
		char command[256];

		snprintf(command, sizeof(command), "rm -rf '%s'", store_dir);
		(void) system(command);
	}
	pause_fd = open(pause_file, O_CREAT | O_EXCL | O_WRONLY, 0600);
	check(pause_fd >= 0, "forkmeta classification pauses maintenance");
	if (pause_fd >= 0)
		close(pause_fd);
	pid = spawn_daemon(daemon_path, shm_name, store_dir, pause_file,
					   NULL, -1, -1, 0, 1);
	check(pid > 0, "forkmeta classification daemon starts");
	if (pid <= 0)
		goto cleanup;
	ready = wait_for_ready(shm_name);
	check(ready, "forkmeta classification daemon publishes ready state");
	if (!ready)
		goto cleanup;
	fd = shm_open(shm_name, O_RDWR, 0600);
	check(fd >= 0, "forkmeta classification opens shared memory");
	if (fd < 0)
		goto cleanup;
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	check(shm != MAP_FAILED, "forkmeta classification maps shared memory");
	if (shm == MAP_FAILED)
		goto cleanup;
	hdr = (PsShmHeader *) shm;
	writer = ps_channel(shm, 0);
	pending = ps_channel(shm, 1);
	check(ps_cas(&writer->claimed, 0, 1) && ps_cas(&pending->claimed, 0, 1),
			"forkmeta classification claims two channels");
	if (ps_load_acquire(&writer->claimed) == 0 ||
		ps_load_acquire(&pending->claimed) == 0)
		goto cleanup;
	for (uint32_t i = 0; i < 3; i++)
	{
		set_relation_key(writer, 5000 + i);
		writer->opcode = PS_OP_CREATE;
		submit_and_wait(writer);
		check(writer->status == PS_STATUS_OK,
				"forkmeta classification seeds source growth");
	}
	for (int i = 0; i < 500; i++)
	{
		if (ps_load_acquire(&hdr->forkmeta_backpressure.throttled) != 0)
			break;
		sleep_ms(1);
	}
	check(ps_load_acquire(&hdr->forkmeta_backpressure.throttled) != 0,
			"forkmeta source growth enters forkmeta throttle");
	set_relation_key(pending, 6000);
	pending->opcode = PS_OP_CREATE;
	ps_request_generation_next(pending);
	ps_store_release(&pending->state, PS_STATE_REQUEST);
	sleep_ms(100);
	check(ps_load_acquire(&pending->state) == PS_STATE_REQUEST,
			"forkmeta-growing CREATE is deferred before admission locks");
	writer->opcode = PS_OP_WAL_APPEND;
	writer->timeline = 0;
	writer->incarnation = 0;
	writer->req_lsn = 100;
	writer->datalen = 1;
	writer->data[0] = 0x5a;
	submit_and_wait(writer);
	check(writer->status == PS_STATUS_OK,
			"non-forkmeta WAL mutation bypasses forkmeta throttle");
	writer->opcode = PS_OP_TIMELINE_STATE;
	writer->timeline = 0;
	writer->incarnation = 0;
	writer->req_lsn = 0;
	writer->req_seq = 0;
	submit_and_wait(writer);
	check(writer->status == PS_STATUS_OK,
			"read-only timeline probe bypasses forkmeta throttle");

cleanup:
	if (pid > 0)
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
	if (writer != NULL)
		ps_store_release(&writer->claimed, 0);
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
}

int
main(int argc, char **argv)
{
	if (argc != 2 && argc != 3)
	{
		fprintf(stderr, "usage: %s <path-to-pagestore_daemon> "
				"[path-to-pagestore_daemon_spdk]\n", argv[0]);
		return 2;
	}
	(void) run_test(argv[1]);
	test_forkmeta_request_classification(argv[1]);
	if (argc == 3)
		test_unsupported_backend_cli(argv[2]);
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed != 0;
}
