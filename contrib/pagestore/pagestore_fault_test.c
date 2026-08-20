#include "pagestore_fault.h"

#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;

static int
path_join(char *dst, size_t dstlen, const char *base, const char *leaf)
{
	int n = snprintf(dst, dstlen, "%s/%s", base, leaf);

	return n >= 0 && (size_t) n < dstlen;
}

static void
check(int condition, const char *message)
{
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", message);
		failures++;
	}
}

static void
clear_config(void)
{
	unsetenv("PAGESTORE_TEST_FAULT_NAME");
	unsetenv("PAGESTORE_TEST_FAULT_ACTION");
	unsetenv("PAGESTORE_TEST_FAULT_HIT");
	unsetenv("PAGESTORE_TEST_FAULT_DIR");
	ps_fault_reset();
}

static void
configure(const char *name, const char *action, const char *hit, const char *dir)
{
	setenv("PAGESTORE_TEST_FAULT_NAME", name, 1);
	setenv("PAGESTORE_TEST_FAULT_ACTION", action, 1);
	setenv("PAGESTORE_TEST_FAULT_HIT", hit, 1);
	setenv("PAGESTORE_TEST_FAULT_DIR", dir, 1);
}

static void *
probe_thread(void *arg)
{
	(void) arg;
	(void) ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER);
	return NULL;
}

int
main(void)
{
	char root[PATH_MAX];
	char control[PATH_MAX];
	char store[PATH_MAX];
	char report[PATH_MAX];
	char outside[PATH_MAX];
	PsFaultPoint point;
	pthread_t threads[4];
	pid_t pid;
	int fd;
	int status;
	FILE *fp;
	int lines;
	int root_len;

	root_len = snprintf(root, sizeof(root), "/tmp/pagestore-fault-test-%ld", (long) getpid());
	if (root_len < 0 || (size_t) root_len >= sizeof(root) ||
		!path_join(control, sizeof(control), root, "control") ||
		!path_join(store, sizeof(store), root, "store") ||
		!path_join(report, sizeof(report), control, "report.jsonl") ||
		!path_join(outside, sizeof(outside), root, "outside"))
	{
		fprintf(stderr, "FAIL: fault test path construction\n");
		return 1;
	}
	rmdir(control);
	rmdir(store);
	rmdir(root);
	check(mkdir(root, 0700) == 0 && mkdir(control, 0700) == 0 &&
		  mkdir(store, 0700) == 0, "create same-level store and control dirs");

	clear_config();
	check(ps_fault_init(store) == 0 && ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY) == 0,
		  "disabled registry is inert");
	check(ps_fault_lookup("daemon.after_ready", &point) == 0 &&
		  point == PS_FAULT_POINT_DAEMON_AFTER_READY &&
		  ps_fault_lookup("unknown.point", &point) != 0 &&
		  point == PS_FAULT_POINT_INVALID, "catalog accepts only .def names");

	configure("unknown.point", "crash", "1", control);
	check(ps_fault_init(store) != 0, "unknown catalog name rejected");
	configure("daemon.after_ready", "error", "1", control);
	check(ps_fault_init(store) != 0, "non-crash action rejected");
	configure("daemon.after_ready", "crash", "0", control);
	check(ps_fault_init(store) != 0, "zero hit rejected");
	configure("daemon.after_ready", "crash", "18446744073709551616", control);
	check(ps_fault_init(store) != 0, "hit overflow rejected");
	clear_config();

	configure("page_prune.after_frontier", "crash", "3", control);
	check(ps_fault_init(store) == 0 &&
		  ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0,
		  "pre-arm probe does not count");
	fd = openat(AT_FDCWD, control, O_RDONLY | O_DIRECTORY);
	check(fd >= 0, "open control directory for arm marker");
	if (fd >= 0)
	{
		int armfd = openat(fd, "arm", O_CREAT | O_WRONLY | O_EXCL, 0600);
		check(armfd >= 0, "create fixed arm marker");
		if (armfd >= 0)
			close(armfd);
		close(fd);
	}
	check(ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0 &&
		  ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0,
		  "armed hits one and two do not fire hit three");
	check(ps_fault_init(control) != 0 && ps_fault_init(root) != 0,
		  "control directory overlap with store is rejected");
	configure("page_prune.after_frontier", "crash", "3", control);
	check(ps_fault_init(store) == 0, "restore valid config after overlap checks");
	fd = openat(AT_FDCWD, control, O_RDONLY | O_DIRECTORY);
	if (fd >= 0)
	{
		unlinkat(fd, "arm", 0);
		close(fd);
	}
	check(ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0,
		  "pre-arm hit remains uncounted");
	fd = openat(AT_FDCWD, control, O_RDONLY | O_DIRECTORY);
	if (fd >= 0)
	{
		int armfd = openat(fd, "arm", O_CREAT | O_WRONLY | O_EXCL, 0600);
		if (armfd >= 0)
			close(armfd);
		close(fd);
	}
	check(ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0 &&
		  ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0,
		  "post-arm hits count toward target");

	/* All concurrent probes share one process-local atomic counter.  The target
	 * thread is the only one allowed to create the report and exit 88. */
	pid = fork();
	if (pid == 0)
	{
		for (int i = 0; i < 4; i++)
			pthread_create(&threads[i], NULL, probe_thread, NULL);
		for (int i = 0; i < 4; i++)
			pthread_join(threads[i], NULL);
		_exit(1);
	}
	check(pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
		  WEXITSTATUS(status) == 88, "target concurrent probe exits 88");
	fp = fopen(report, "r");
	lines = 0;
	if (fp != NULL)
	{
		char line[512];
		while (fgets(line, sizeof(line), fp) != NULL)
		{
			lines++;
			check(strstr(line, "\"schema\":1") != NULL &&
				  strstr(line, "\"name\":\"page_prune.after_frontier\"") != NULL &&
				  strstr(line, "\"action\":\"crash\"") != NULL &&
				  strstr(line, "\"hit\":3") != NULL &&
				  strstr(line, "\"pid\":") != NULL, "exact fault report schema");
		}
		fclose(fp);
	}
	check(lines == 1, "exactly one target report line");
	clear_config();

	/* Symlinks and non-regular control entries are rejected before probing. */
	unlink(report);
	unlink(outside);
	fd = open(outside, O_CREAT | O_WRONLY, 0600);
	if (fd >= 0)
		close(fd);
	check(symlink(outside, report) == 0, "create report symlink");
	configure("daemon.after_ready", "crash", "1", control);
	check(ps_fault_init(store) != 0, "report symlink rejected");
	unlink(report);
	fd = openat(AT_FDCWD, control, O_RDONLY | O_DIRECTORY);
	if (fd >= 0)
	{
		unlinkat(fd, "arm", 0);
		close(fd);
	}
	/* A symlink arm is checked by init's fstatat path. */
	unlink(report);
	fd = openat(AT_FDCWD, control, O_RDONLY | O_DIRECTORY);
	if (fd >= 0)
	{
		int link_rc = symlinkat(outside, fd, "arm");
		check(link_rc == 0, "create arm symlink");
		close(fd);
	}
	check(ps_fault_init(store) != 0, "arm symlink rejected");
	clear_config();
	fd = openat(AT_FDCWD, control, O_RDONLY | O_DIRECTORY);
	if (fd >= 0)
	{
		unlinkat(fd, "arm", 0);
		close(fd);
	}
	unlink(outside);
	rmdir(control);
	rmdir(store);
	rmdir(root);
	return failures == 0 ? 0 : 1;
}
