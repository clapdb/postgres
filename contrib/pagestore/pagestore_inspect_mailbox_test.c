/*
 * Focused POSIX-only coverage for the relation inspection mailbox.
 * This test uses a small fake daemon loop and the real inspector binary; it
 * deliberately never starts the pagestore daemon or any SPDK frontend.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
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

static int
set_inspection_lock(int fd, off_t byte, short type)
{
	struct flock lock;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = type;
	lock.l_whence = SEEK_SET;
	lock.l_start = byte;
	lock.l_len = 1;
	return fcntl(fd, F_SETLK, &lock);
}

static int
wait_child(pid_t pid, int milliseconds, int *status)
{
	int steps = milliseconds;

	for (int i = 0; i < steps; i++)
	{
		pid_t result = waitpid(pid, status, WNOHANG);

		if (result == pid)
			return 1;
		if (result < 0)
			return 0;
		sleep_ms(1);
	}
	return waitpid(pid, status, WNOHANG) == pid;
}

static int
wait_for_state(PsInspectionRequest *request, uint32_t state, int milliseconds)
{
	for (int i = 0; i < milliseconds; i++)
	{
		if (ps_load_acquire(&request->state) == state)
			return 1;
		sleep_ms(1);
	}
	return ps_load_acquire(&request->state) == state;
}

static void
init_header(PsShmHeader *hdr, uint32_t state, uint64_t generation)
{
	memset(hdr, 0, PS_SHM_SIZE);
	hdr->magic = PS_SHM_MAGIC;
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = PS_DEFAULT_PAGE_SIZE;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->nshards = 1;
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;
	hdr->frontend_capabilities = PS_FRONTEND_CAP_RELATION_INSPECTION;
	hdr->startup_state = PS_SHM_READY;
	hdr->daemon_instance = 1;
	hdr->inspection_request.request_generation = generation;
	hdr->inspection_request.daemon_instance = 1;
	hdr->inspection_request.state = state;
}

static int
open_fixture(char *name, size_t name_size, int *fd_out, PsShmHeader **hdr_out)
{
	int fd;
	void *mapping;

	snprintf(name, name_size, "/psinspect_mailbox_%ld", (long) getpid());
	shm_unlink(name);
	fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
	if (fd < 0 || ftruncate(fd, PS_SHM_SIZE) != 0)
	{
		if (fd >= 0)
			close(fd);
		return -1;
	}
	mapping = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
					   fd, 0);
	if (mapping == MAP_FAILED)
	{
		close(fd);
		shm_unlink(name);
		return -1;
	}
	*fd_out = fd;
	*hdr_out = (PsShmHeader *) mapping;
	return 0;
}

static void
close_fixture(const char *name, int fd, PsShmHeader *hdr)
{
	munmap(hdr, PS_SHM_SIZE);
	close(fd);
	shm_unlink(name);
}

static pid_t
start_inspector(const char *inspector, const char *name)
{
	pid_t pid = fork();

	if (pid == 0)
	{
		execl(inspector, inspector, "--shm", name, "relation", "0", "1",
			  "1", "1", "42", "0", (char *) NULL);
		_exit(127);
	}
	return pid;
}

static pid_t
start_lease_holder(const char *name, int ready_fd)
{
	pid_t pid = fork();

	if (pid == 0)
	{
		unsigned char ready = 1;
		int fd = shm_open(name, O_RDWR, 0);

		if (fd < 0 || set_inspection_lock(fd, PS_INSPECTION_DAEMON_LOCK_BYTE,
										F_WRLCK) != 0)
			_exit(127);
		if (write(ready_fd, &ready, 1) != 1)
			_exit(127);
		for (;;)
			pause();
	}
	return pid;
}

static void
complete_request(PsInspectionRequest *request, int inconsistent)
{
	memset(&request->relation, 0, sizeof(request->relation));
	request->relation.exists = inconsistent ? 0 : 1;
	request->relation.fork_count = 1;
	request->relation.forks[0].fork_num = 0;
	request->relation.forks[0].nblocks = 3;
	request->status = PS_STATUS_OK;
	ps_store_release(&request->state, PS_INSPECTION_STATE_DONE);
}

static void
test_lock_busy(const char *inspector)
{
	char name[64];
	int fd;
	int status;
	PsShmHeader *hdr;
	pid_t pid;

	check(open_fixture(name, sizeof(name), &fd, &hdr) == 0,
		  "create lock fixture");
	if (failed)
		return;
	init_header(hdr, PS_INSPECTION_STATE_IDLE, 1);
	check(set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE,
						 F_WRLCK) == 0, "hold relation mailbox lock");
	pid = start_inspector(inspector, name);
	check(pid > 0 && wait_child(pid, 2000, &status),
		  "busy inspector exits promptly");
	check(pid > 0 && WIFEXITED(status) && WEXITSTATUS(status) != 0,
		  "concurrent inspector is rejected by the POSIX lock");
	(void) set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE,
						 F_UNLCK);
	close_fixture(name, fd, hdr);
}

static void
test_stale_state(const char *inspector, uint32_t initial_state,
				 const char *label)
{
	char name[64];
	int fd;
	int status;
	PsShmHeader *hdr;
	PsInspectionRequest *request;
	pid_t pid;

	check(open_fixture(name, sizeof(name), &fd, &hdr) == 0, label);
	if (failed)
		return;
	init_header(hdr, initial_state, 100);
	request = &hdr->inspection_request;
	pid = start_inspector(inspector, name);
	check(wait_for_state(request, PS_INSPECTION_STATE_IDLE, 8000),
		  "dead daemon stale state is reclaimed");
	check(request->request_generation == 100,
		  "dead daemon recovery does not publish a new request");
	check(pid > 0 && wait_child(pid, 1000, &status),
		  "dead daemon inspector fails promptly after recovery");
	check(pid > 0 && WIFEXITED(status) && WEXITSTATUS(status) != 0,
		  "dead daemon inspection is unavailable");
	check(ps_load_acquire(&request->state) == PS_INSPECTION_STATE_IDLE,
		  "dead daemon recovery leaves mailbox idle");
	close_fixture(name, fd, hdr);
}

static void
test_live_daemon_lease(const char *inspector)
{
	char name[64];
	int fd;
	int ready_pipe[2];
	int status;
	PsShmHeader *hdr;
	PsInspectionRequest *request;
	pid_t holder;
	pid_t pid;
	unsigned char ready;

	check(open_fixture(name, sizeof(name), &fd, &hdr) == 0,
		  "create live lease fixture");
	if (failed)
		return;
	init_header(hdr, PS_INSPECTION_STATE_BUSY, 100);
	request = &hdr->inspection_request;
	check(pipe(ready_pipe) == 0, "create daemon lease handshake");
	if (failed)
	{
		close_fixture(name, fd, hdr);
		return;
	}
	holder = start_lease_holder(name, ready_pipe[1]);
	close(ready_pipe[1]);
	check(holder > 0 && read(ready_pipe[0], &ready, 1) == 1,
		  "forked daemon lease is held");
	close(ready_pipe[0]);
	if (failed)
	{
		if (holder > 0)
			kill(holder, SIGKILL);
		if (holder > 0)
			waitpid(holder, &status, 0);
		close_fixture(name, fd, hdr);
		return;
	}

	pid = start_inspector(inspector, name);
	check(pid > 0 && wait_child(pid, 8000, &status),
		  "live-daemon stale inspector exits bounded");
	check(pid > 0 && WIFEXITED(status) && WEXITSTATUS(status) != 0,
		  "live daemon prevents stale BUSY reclamation");
	check(ps_load_acquire(&request->state) == PS_INSPECTION_STATE_BUSY,
		  "live daemon leaves BUSY state untouched");

	kill(holder, SIGKILL);
	check(wait_child(holder, 2000, &status) && WIFSIGNALED(status),
		  "daemon lease owner exit releases byte one");
	pid = start_inspector(inspector, name);
	check(wait_for_state(request, PS_INSPECTION_STATE_IDLE, 8000),
		  "post-exit inspector reclaims stale BUSY");
	check(request->request_generation == 100,
		  "post-exit inspector does not publish without a lease");
	check(pid > 0 && wait_child(pid, 1000, &status) &&
		  WIFEXITED(status) && WEXITSTATUS(status) != 0,
		  "post-exit recovered inspection fails promptly");
	close_fixture(name, fd, hdr);
}

static void
test_initialization_lock_exclusion(void)
{
	char name[64];
	int fd;
	int ready_pipe[2];
	int status;
	PsShmHeader *hdr;
	pid_t holder;
	unsigned char ready;

	check(open_fixture(name, sizeof(name), &fd, &hdr) == 0,
		  "create initialization lock fixture");
	if (failed)
		return;
	check(pipe(ready_pipe) == 0, "create initialization handshake");
	if (failed)
	{
		close_fixture(name, fd, hdr);
		return;
	}
	holder = fork();
	if (holder == 0)
	{
		int child_fd = shm_open(name, O_RDWR, 0);

		if (child_fd < 0 || set_inspection_lock(child_fd,
									PS_INSPECTION_CLIENT_LOCK_BYTE, F_WRLCK) != 0)
			_exit(127);
		ready = 1;
		if (write(ready_pipe[1], &ready, 1) != 1)
			_exit(127);
		for (;;)
			pause();
	}
	close(ready_pipe[1]);
	check(holder > 0 && read(ready_pipe[0], &ready, 1) == 1,
		  "forked initializer owns byte zero");
	close(ready_pipe[0]);
	check(set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE,
						 F_WRLCK) != 0 && (errno == EACCES || errno == EAGAIN),
		  "client cannot overlap initialization lock");
	kill(holder, SIGKILL);
	check(wait_child(holder, 2000, &status) && WIFSIGNALED(status),
		  "initializer exit releases byte zero");
	check(set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE,
						 F_WRLCK) == 0, "client acquires byte zero after init");
	(void) set_inspection_lock(fd, PS_INSPECTION_CLIENT_LOCK_BYTE, F_UNLCK);
	close_fixture(name, fd, hdr);
}

static void
test_response_consistency(const char *inspector)
{
	char name[64];
	int fd;
	int ready_pipe[2];
	int status;
	PsShmHeader *hdr;
	PsInspectionRequest *request;
	pid_t holder;
	pid_t pid;
	unsigned char ready;

	check(open_fixture(name, sizeof(name), &fd, &hdr) == 0,
		  "create response validation fixture");
	if (failed)
		return;
	init_header(hdr, PS_INSPECTION_STATE_IDLE, 1);
	request = &hdr->inspection_request;
	check(pipe(ready_pipe) == 0, "create response daemon lease handshake");
	if (failed)
	{
		close_fixture(name, fd, hdr);
		return;
	}
	holder = start_lease_holder(name, ready_pipe[1]);
	close(ready_pipe[1]);
	check(holder > 0 && read(ready_pipe[0], &ready, 1) == 1,
		  "response fake daemon owns lease");
	close(ready_pipe[0]);
	if (failed)
	{
		if (holder > 0)
			kill(holder, SIGKILL);
		if (holder > 0)
			waitpid(holder, &status, 0);
		close_fixture(name, fd, hdr);
		return;
	}
	pid = start_inspector(inspector, name);
	check(wait_for_state(request, PS_INSPECTION_STATE_REQUEST, 2000),
		  "response validation request published");
	complete_request(request, 1);
	check(pid > 0 && wait_child(pid, 2000, &status),
		  "inconsistent response inspector exits");
	check(pid > 0 && WIFEXITED(status) && WEXITSTATUS(status) != 0,
		  "exists without main fork is rejected");
	check(ps_load_acquire(&request->state) == PS_INSPECTION_STATE_IDLE,
		  "invalid response mailbox is reclaimed");
	kill(holder, SIGKILL);
	check(wait_child(holder, 2000, &status) && WIFSIGNALED(status),
		  "response fake daemon lease is released");
	close_fixture(name, fd, hdr);
}

int
main(int argc, char **argv)
{
	if (argc != 2)
	{
		fprintf(stderr, "usage: %s PAGestore_INSPECT_BINARY\n", argv[0]);
		return 2;
	}
	test_initialization_lock_exclusion();
	test_lock_busy(argv[1]);
	test_stale_state(argv[1], PS_INSPECTION_STATE_REQUEST,
				 "create stale REQUEST fixture");
	test_stale_state(argv[1], PS_INSPECTION_STATE_BUSY,
				 "create stale BUSY fixture");
	test_live_daemon_lease(argv[1]);
	test_response_consistency(argv[1]);
	fprintf(stderr, "%d checks, %d failures\n", checks, failed);
	return failed == 0 ? 0 : 1;
}
