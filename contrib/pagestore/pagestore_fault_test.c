#include "pagestore_fault.h"

#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;

static void
check(int condition, const char *message)
{
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", message);
		failures++;
	}
}

static int
join_path(char *out, size_t size, const char *base, const char *leaf)
{
	int n = snprintf(out, size, "%s/%s", base, leaf);

	return n >= 0 && (size_t) n < size;
}

static void
remove_control_file(const char *control, const char *name)
{
	char path[PATH_MAX];

	if (join_path(path, sizeof(path), control, name))
		(void) unlink(path);
}

static int
create_control_file(const char *control, const char *name)
{
	char path[PATH_MAX];
	int fd;

	if (!join_path(path, sizeof(path), control, name))
		return -1;
	fd = open(path, O_CREAT | O_WRONLY | O_EXCL | O_CLOEXEC, 0600);
	if (fd >= 0)
		close(fd);
	return fd >= 0 ? 0 : -1;
}

static int
report_has(const char *report, const char *needle)
{
	FILE *stream = fopen(report, "r");
	char line[2048];
	int found = 0;

	if (stream == NULL)
		return 0;
	while (fgets(line, sizeof(line), stream) != NULL)
		if (strstr(line, needle) != NULL)
			found = 1;
	fclose(stream);
	return found;
}

static int
wait_for_file(const char *path, unsigned int milliseconds)
{
	struct timespec delay = {0, 1000000L};

	for (unsigned int i = 0; i < milliseconds; i++)
	{
		if (access(path, F_OK) == 0)
			return 0;
		(void) nanosleep(&delay, NULL);
	}
	return -1;
}

static void
clear_config(void)
{
	const char *names[] = {
		"PAGESTORE_TEST_FAULT_NAME", "PAGESTORE_TEST_FAULT_ACTION",
		"PAGESTORE_TEST_FAULT_HIT", "PAGESTORE_TEST_FAULT_DIR",
		"PAGESTORE_TEST_FAULT_SCENARIO", "PAGESTORE_TEST_FAULT_SEED",
		"PAGESTORE_TEST_FAULT_OPERATION", "PAGESTORE_TEST_FAULT_OPERATION_ID",
		"PAGESTORE_TEST_FAULT_WATCHDOG_MS"
	};

	for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++)
		unsetenv(names[i]);
	ps_fault_reset();
}

static void
configure(const char *name, const char *action, const char *hit, const char *control)
{
	setenv("PAGESTORE_TEST_FAULT_NAME", name, 1);
	setenv("PAGESTORE_TEST_FAULT_ACTION", action, 1);
	setenv("PAGESTORE_TEST_FAULT_HIT", hit, 1);
	setenv("PAGESTORE_TEST_FAULT_DIR", control, 1);
}

static void *
page_prune_probe(void *unused)
{
	(void) unused;
	(void) ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER);
	return NULL;
}

static void *
daemon_probe(void *result)
{
	*(int *) result = ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY);
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
	PsFaultStatus status;
	int root_length;

	root_length = snprintf(root, sizeof(root), "/tmp/pagestore-fault-test-%ld", (long) getpid());
	check(root_length > 0 && (size_t) root_length < sizeof(root) &&
		join_path(control, sizeof(control), root, "control") &&
		join_path(store, sizeof(store), root, "store") &&
		join_path(report, sizeof(report), control, "report.jsonl") &&
		join_path(outside, sizeof(outside), root, "outside"), "construct paths");
	(void) unlink(report);
	(void) unlink(outside);
	(void) rmdir(control);
	(void) rmdir(store);
	(void) rmdir(root);
	check(mkdir(root, 0700) == 0 && mkdir(control, 0700) == 0 &&
		mkdir(store, 0700) == 0, "create store and control directories");

	clear_config();
	check(ps_fault_init(store) == 0 &&
		ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY) == 0,
		"empty configuration is inert");
	check(ps_fault_lookup("daemon.after_ready", &point) == 0 &&
		point == PS_FAULT_POINT_DAEMON_AFTER_READY &&
		strcmp(ps_fault_allowed_actions(point), "crash|error|pause") == 0 &&
		strcmp(ps_fault_allowed_actions(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER),
			"crash") == 0, "catalog exposes action policy");

	/* Partial configuration, bad hit values, and non-crash lock-held actions fail closed. */
	setenv("PAGESTORE_TEST_FAULT_ACTION", "crash", 1);
	check(ps_fault_init(store) != 0, "partial configuration rejected");
	clear_config();
	configure("page_prune.after_frontier", "error", "1", control);
	check(ps_fault_init(store) != 0, "lock-held error rejected");
	configure("daemon.after_ready", "crash", "0", control);
	check(ps_fault_init(store) != 0, "zero hit rejected");
	configure("daemon.after_ready", "crash", "1", control);
	setenv("PAGESTORE_TEST_FAULT_SCENARIO", "only-scenario", 1);
	check(ps_fault_init(store) != 0, "partial identity rejected");
	clear_config();
	configure("daemon.after_ready", "error", "1", control);
	setenv("PAGESTORE_TEST_FAULT_SCENARIO", "fault-actions", 1);
	setenv("PAGESTORE_TEST_FAULT_OPERATION", "ready", 1);
	setenv("PAGESTORE_TEST_FAULT_SEED", "01", 1);
	check(ps_fault_init(store) != 0, "leading-zero seed rejected");
	setenv("PAGESTORE_TEST_FAULT_SEED", "+1", 1);
	check(ps_fault_init(store) != 0, "signed seed rejected");
	{
		char invalid_utf8[] = {'b', (char) 0xc0, (char) 0xaf, '\0'};

		setenv("PAGESTORE_TEST_FAULT_SEED", "1", 1);
		setenv("PAGESTORE_TEST_FAULT_SCENARIO", invalid_utf8, 1);
		check(ps_fault_init(store) != 0, "invalid UTF-8 identity rejected");
	}
	{
		char truncated_two[] = {(char) 0xc2, '\0'};
		char truncated_three[] = {(char) 0xe0, (char) 0xa0, '\0'};
		char truncated_four[] = {(char) 0xf0, (char) 0x90, (char) 0x80, '\0'};

		setenv("PAGESTORE_TEST_FAULT_SCENARIO", truncated_two, 1);
		check(ps_fault_init(store) != 0, "truncated two-byte UTF-8 rejected");
		setenv("PAGESTORE_TEST_FAULT_SCENARIO", truncated_three, 1);
		check(ps_fault_init(store) != 0, "truncated three-byte UTF-8 rejected");
		setenv("PAGESTORE_TEST_FAULT_SCENARIO", truncated_four, 1);
		check(ps_fault_init(store) != 0, "truncated four-byte UTF-8 rejected");
	}
	clear_config();

	/* Unhit reporting/query and exact hit accounting. */
	configure("page_prune.after_frontier", "crash", "3", control);
	check(ps_fault_init(store) == 0 &&
		ps_fault_query(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER, &status) == 0 &&
		status.enabled && status.hits == 0 && !status.reached,
		"unhit point is queryable");
	check(ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0 &&
		create_control_file(control, "arm") == 0 &&
		ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0 &&
		ps_fault_probe(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER) == 0 &&
		ps_fault_query(PS_FAULT_POINT_PAGE_PRUNE_AFTER_FRONTIER, &status) == 0 &&
		status.hits == 2 && !status.reached,
		"unarmed and pre-target probes count exactly");
	remove_control_file(control, "arm");
	clear_config();

	/* Error reports and returns nonzero exactly at the selected hit. */
	configure("daemon.after_ready", "error", "1", control);
	setenv("PAGESTORE_TEST_FAULT_SCENARIO", "fault-actions", 1);
	setenv("PAGESTORE_TEST_FAULT_SEED", "17", 1);
	setenv("PAGESTORE_TEST_FAULT_OPERATION", "ready", 1);
	check(ps_fault_init(store) == 0 && create_control_file(control, "arm") == 0 &&
		ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY) == PS_FAULT_PROBE_ERROR &&
		ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY) == 0,
		"error returns nonzero only at target");
	check(report_has(report, "\"name\":\"daemon.after_ready\"") &&
		report_has(report, "\"action\":\"error\"") &&
		report_has(report, "\"scenario\":\"fault-actions\"") &&
		report_has(report, "\"seed\":17") &&
		report_has(report, "\"operation\":\"ready\""),
		"error report contains reachability identity");
	remove_control_file(control, "arm");
	remove_control_file(control, "report.jsonl");
	clear_config();

	/* Pause reports before waiting, accepts only a regular release marker, and releases. */
	configure("daemon.after_ready", "pause", "1", control);
	setenv("PAGESTORE_TEST_FAULT_WATCHDOG_MS", "1000", 1);
	check(ps_fault_init(store) == 0 && create_control_file(control, "arm") == 0,
		"configure pause release case");
	{
		pthread_t thread;
		int result = PS_FAULT_PROBE_ERROR;

		check(pthread_create(&thread, NULL, daemon_probe, &result) == 0,
			"start pause probe");
		check(wait_for_file(report, 500) == 0, "pause reports before waiting");
		check(create_control_file(control, "release") == 0, "create regular release marker");
		(void) pthread_join(thread, NULL);
		check(result == 0 && report_has(report, "\"action\":\"pause\""),
			"pause returns success after release");
	}
	remove_control_file(control, "arm");
	remove_control_file(control, "release");
	remove_control_file(control, "report.jsonl");
	clear_config();

	/* Timeout exits distinctly and atomically replaces reached with timeout. */
	configure("daemon.after_ready", "pause", "1", control);
	setenv("PAGESTORE_TEST_FAULT_WATCHDOG_MS", "30", 1);
	check(ps_fault_init(store) == 0 && create_control_file(control, "arm") == 0,
		"configure pause timeout case");
	{
		char release[PATH_MAX];
		check(join_path(release, sizeof(release), control, "release") == 1 &&
			symlink(outside, release) == 0, "create release symlink");
	}
	{
		pid_t pid = fork();
		int wait_status;

		if (pid == 0)
			_exit(ps_fault_probe(PS_FAULT_POINT_DAEMON_AFTER_READY));
		check(pid > 0 && waitpid(pid, &wait_status, 0) == pid &&
			WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == PS_FAULT_PAUSE_TIMEOUT_EXIT &&
			report_has(report, "\"state\":\"timeout\"") &&
			report_has(report, "\"watchdog_ms\":30"),
			"pause watchdog exits distinctly with actionable report");
	}
	remove_control_file(control, "arm");
	remove_control_file(control, "release");
	remove_control_file(control, "report.jsonl");
	clear_config();

	/* Concurrent crash probes have one exact target winner. */
	configure("page_prune.after_frontier", "crash", "4", control);
	check(ps_fault_init(store) == 0 && create_control_file(control, "arm") == 0,
		"configure concurrent crash");
	{
		pid_t pid = fork();
		int wait_status;

		if (pid == 0)
		{
			pthread_t threads[8];
			for (size_t i = 0; i < 8; i++)
				(void) pthread_create(&threads[i], NULL, page_prune_probe, NULL);
			for (size_t i = 0; i < 8; i++)
				(void) pthread_join(threads[i], NULL);
			_exit(1);
		}
		check(pid > 0 && waitpid(pid, &wait_status, 0) == pid &&
			WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == PS_FAULT_CRASH_EXIT &&
			report_has(report, "\"name\":\"page_prune.after_frontier\"") &&
			report_has(report, "\"hit\":4"),
			"concurrent crash exits 88 with target report");
	}
	remove_control_file(control, "arm");
	remove_control_file(control, "report.jsonl");
	clear_config();

	/* Existing symlink/non-regular protections stay fail-closed. */
	{
		int fd = open(outside, O_CREAT | O_WRONLY | O_CLOEXEC, 0600);
		if (fd >= 0)
			close(fd);
	}
	check(symlink(outside, report) == 0, "create report symlink");
	configure("daemon.after_ready", "crash", "1", control);
	check(ps_fault_init(store) != 0, "report symlink rejected");
	remove_control_file(control, "report.jsonl");
	{
		char arm[PATH_MAX];
		check(join_path(arm, sizeof(arm), control, "arm") == 1 &&
			symlink(outside, arm) == 0, "create arm symlink");
	}
	check(ps_fault_init(store) != 0, "arm symlink rejected");
	remove_control_file(control, "arm");
	clear_config();
	(void) unlink(outside);
	(void) rmdir(control);
	(void) rmdir(store);
	(void) rmdir(root);
	return failures == 0 ? 0 : 1;
}
