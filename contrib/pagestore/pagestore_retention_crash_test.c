/*-------------------------------------------------------------------------
 *
 * pagestore_retention_crash_test.c
 *	Deterministic regression test for Bug C: a process death during
 *	retention-log compaction (or a plain in-place append) must never leave
 *	the store unable to open again.
 *
 * Unlike the daemon-level crash matrices, this exercises pagestore_retention.c
 * directly (no pagestore_core.c, no daemon, no IPC): retention_republish()
 * (shared by ps_retention_compact() and the v1 -> v2 migration rewrite) and
 * retention_append() are the only durable-mutation paths that install a
 * retention.pending intent, so a real fork()+process-abort at each of their
 * six named fault points (see pagestore_fault_points.def) is enough to
 * reproduce the crash deterministically and cheaply -- one fork per case,
 * no fuzzer workload, no gdb.
 *
 * Each case: a child process opens (and, except for the fresh-store append
 * case, populates) a scratch registry, arms one fault point, then performs
 * the mutation that reaches it.  ps_fault_probe()'s "crash" action calls
 * _exit(88) directly from inside the mutation, before anything is returned
 * to the (nonexistent, in this test) caller -- the same abrupt, cleanup-free
 * process death a real SIGKILL produces from the durability code's point of
 * view.  The parent reaps that exit, then reopens the registry twice (the
 * first open exercises the reconciliation in ps_retention_open(); the
 * second proves it left nothing behind to reconcile) and checks the active
 * pin set is exactly what it was expected to be before the crash.
 *
 *-------------------------------------------------------------------------
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_fault.h"
#include "pagestore_retention.h"

static int checks = 0;
static int failures = 0;

static void
check(int condition, const char *message)
{
	checks++;
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", message);
		failures++;
	}
}

static int
remove_tree(const char *path)
{
	DIR		   *dir = opendir(path);
	struct dirent *entry;
	int			ok = 1;

	if (dir == NULL)
		return unlink(path) == 0 || errno == ENOENT;
	while ((entry = readdir(dir)) != NULL)
	{
		char		child[1600];
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) < 0 ||
			lstat(child, &st) != 0)
		{
			ok = 0;
			continue;
		}
		if (S_ISDIR(st.st_mode))
			ok = remove_tree(child) && ok;
		else if (unlink(child) != 0 && errno != ENOENT)
			ok = 0;
	}
	if (closedir(dir) != 0)
		ok = 0;
	if (rmdir(path) != 0 && errno != ENOENT)
		ok = 0;
	return ok;
}

static int
arm_fault(const char *fault_dir)
{
	char		path[1600];
	int			fd;

	if (snprintf(path, sizeof(path), "%s/arm", fault_dir) < 0)
		return 0;
	fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
	if (fd < 0)
		return 0;
	return close(fd) == 0;
}

static int
configure_fault(const char *store, const char *fault_dir, const char *name)
{
	return setenv("PAGESTORE_TEST_FAULT_NAME", name, 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_ACTION", "crash", 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_HIT", "1", 1) == 0 &&
		setenv("PAGESTORE_TEST_FAULT_DIR", fault_dir, 1) == 0 &&
		ps_fault_init(store) == 0;
}

typedef enum RetentionCrashKind
{
	RCK_APPEND,
	RCK_APPEND_FRESH,
	RCK_COMPACT,
} RetentionCrashKind;

typedef struct RetentionCrashCase
{
	const char *fault_name;
	RetentionCrashKind kind;
	const char *label;
} RetentionCrashCase;

static const RetentionCrashCase cases[] = {
	{"retention_append.after_pending", RCK_APPEND_FRESH,
	 "append after_pending on a never-initialized store"},
	{"retention_append.after_pending", RCK_APPEND,
	 "append after_pending on a populated store"},
	{"retention_append.after_write", RCK_APPEND,
	 "append after_write on a populated store"},
	{"retention_compact.after_pending", RCK_COMPACT,
	 "compact after_pending"},
	{"retention_compact.after_tmp_sync", RCK_COMPACT,
	 "compact after_tmp_sync"},
	{"retention_compact.after_rename", RCK_COMPACT,
	 "compact after_rename"},
	{"retention_compact.after_state", RCK_COMPACT,
	 "compact after_state"},
};
#define NCASES (sizeof(cases) / sizeof(cases[0]))

#define PIN_A_OWNER  UINT64_C(100)
#define PIN_B_OWNER  UINT64_C(200)
#define PIN_C_OWNER  UINT64_C(300)
#define PIN_D_OWNER  UINT64_C(400)

static void
make_pin(PsRetentionPin *pin, uint32_t owner_kind, uint64_t owner_id,
		 uint32_t resources, uint64_t lsn, uint32_t generation,
		 uint64_t admission_seq)
{
	memset(pin, 0, sizeof(*pin));
	pin->timeline = 1;
	pin->owner_kind = owner_kind;
	pin->owner_id = owner_id;
	pin->resources = resources;
	pin->lsn = lsn;
	pin->generation = generation;
	pin->admission_seq = admission_seq;
}

/* Five durable records (one admission reservation, two SETs, a DROP
 * tombstone, one more SET) reducing to two active pins (A and C) -- the
 * same shape as the field repro that found Bug C: compaction shrinks the
 * record count (5 -> 4), which is legitimate and must not itself look like
 * corruption to the pending-intent reconciliation. */
static int
populate_store(const char *store)
{
	PsRetentionPin pin;

	if (ps_retention_open(store) != 0)
		return 0;
	if (ps_retention_reserve_admission_seq(5) != 0)
		return 0;
	make_pin(&pin, PS_RETENTION_OWNER_READER, PIN_A_OWNER,
			 PS_RETENTION_RESOURCE_PAGE_HISTORY, 1000, 1, 1);
	if (ps_retention_set(&pin) != PS_RETENTION_OK)
		return 0;
	make_pin(&pin, PS_RETENTION_OWNER_READER, PIN_B_OWNER,
			 PS_RETENTION_RESOURCE_WAL, 2000, 1, 2);
	if (ps_retention_set(&pin) != PS_RETENTION_OK)
		return 0;
	if (ps_retention_drop(1, PS_RETENTION_OWNER_READER, PIN_B_OWNER, 2) !=
		PS_RETENTION_OK)
		return 0;
	make_pin(&pin, PS_RETENTION_OWNER_MATERIALIZER, PIN_C_OWNER,
			 PS_RETENTION_RESOURCE_WAL_INDEX, 3000, 1, 3);
	if (ps_retention_set(&pin) != PS_RETENTION_OK)
		return 0;
	return 1;
}

static int
snapshot_matches_expected(const char *store, int expect_d)
{
	PsRetentionPin *pins = NULL;
	uint32_t	count = 0;
	int			ok;
	int			saw_a = 0,
				saw_c = 0,
				saw_d = 0;

	if (ps_retention_open(store) != 0)
	{
		check(0, "reopen after crash succeeds");
		return 0;
	}
	ok = ps_retention_snapshot_alloc(&pins, &count) == 0;
	check(ok, "snapshot_alloc succeeds on the reopened registry");
	if (ok)
	{
		uint32_t	expect_count = expect_d ? 3 : 2;

		check(count == expect_count, "active pin count matches expectation");
		for (uint32_t i = 0; i < count; i++)
		{
			if (pins[i].owner_id == PIN_A_OWNER)
				saw_a = 1;
			else if (pins[i].owner_id == PIN_C_OWNER)
				saw_c = 1;
			else if (pins[i].owner_id == PIN_D_OWNER)
				saw_d = 1;
			else
				check(0, "no unexpected owner in the recovered pin set");
		}
		check(saw_a, "pin A survives recovery");
		check(saw_c, "pin C survives recovery");
		if (expect_d)
			check(saw_d, "a fully durable append survives recovery");
		else
			check(!saw_d, "an unacknowledged interrupted append is dropped");
	}
	free(pins);
	ps_retention_close();
	return ok;
}

static void
run_child(const RetentionCrashCase *tc, const char *store,
		  const char *fault_dir)
{
	PsRetentionPin pin;

	if (!configure_fault(store, fault_dir, tc->fault_name))
		_exit(2);
	if (tc->kind == RCK_APPEND_FRESH)
	{
		if (ps_retention_open(store) != 0)
			_exit(2);
		if (!arm_fault(fault_dir))
			_exit(2);
		make_pin(&pin, PS_RETENTION_OWNER_READER, PIN_A_OWNER,
				 PS_RETENTION_RESOURCE_PAGE_HISTORY, 1000, 1, 1);
		(void) ps_retention_set(&pin);
		_exit(3);				/* the fault should have fired already */
	}
	if (!populate_store(store))
		_exit(2);
	if (!arm_fault(fault_dir))
		_exit(2);
	if (tc->kind == RCK_APPEND)
	{
		make_pin(&pin, PS_RETENTION_OWNER_READER, PIN_D_OWNER,
				 PS_RETENTION_RESOURCE_PAGE_HISTORY, 4000, 1, 4);
		(void) ps_retention_set(&pin);
	}
	else
		(void) ps_retention_compact();
	_exit(3);					/* the fault should have fired already */
}

static int
run_case(const RetentionCrashCase *tc)
{
	char		store[] = "/tmp/psretentioncrashXXXXXX";
	char		fault_dir[1600];
	pid_t		pid;
	int			status;
	int			ok;

	if (mkdtemp(store) == NULL)
		return 0;
	if (snprintf(fault_dir, sizeof(fault_dir), "%s.fault", store) < 0 ||
		mkdir(fault_dir, 0700) != 0)
	{
		remove_tree(store);
		return 0;
	}
	pid = fork();
	if (pid == 0)
		run_child(tc, store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, tc->label);
	if (ok)
	{
		int			expect_d = 0;	/* every case rolls back or is d-free */

		/* First open: exercises ps_retention_open()'s pending reconciliation. */
		check(snapshot_matches_expected(store, expect_d),
			  "first reopen recovers the expected pin set");
		/* Second open: nothing should be left to reconcile the second time. */
		check(snapshot_matches_expected(store, expect_d),
			  "second restart is idempotent");
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

static int
run_fresh_case(const RetentionCrashCase *tc)
{
	char		store[] = "/tmp/psretentioncrashfreshXXXXXX";
	char		fault_dir[1600];
	pid_t		pid;
	int			status;
	int			ok;
	PsRetentionPin *pins = NULL;
	uint32_t	count = 0;

	if (mkdtemp(store) == NULL)
		return 0;
	if (snprintf(fault_dir, sizeof(fault_dir), "%s.fault", store) < 0 ||
		mkdir(fault_dir, 0700) != 0)
	{
		remove_tree(store);
		return 0;
	}
	pid = fork();
	if (pid == 0)
		run_child(tc, store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, tc->label);
	if (ok)
	{
		check(ps_retention_open(store) == 0,
			  "fresh-store crash still reopens as an empty registry");
		check(ps_retention_snapshot_alloc(&pins, &count) == 0 && count == 0,
			  "no pin was ever recorded for the crashed first append");
		free(pins);
		pins = NULL;
		ps_retention_close();
		check(ps_retention_open(store) == 0,
			  "second restart of the fresh-store case is idempotent");
		check(ps_retention_snapshot_alloc(&pins, &count) == 0 && count == 0,
			  "still no pin after the second restart");
		free(pins);
		ps_retention_close();
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

int
main(void)
{
	int			ok = 1;

	for (size_t i = 0; i < NCASES; i++)
	{
		const RetentionCrashCase *tc = &cases[i];
		int			case_ok = tc->kind == RCK_APPEND_FRESH ?
			run_fresh_case(tc) : run_case(tc);

		if (!case_ok)
			ok = 0;
	}
	printf("pagestore_retention_crash_test: %d checks, %d failed\n",
		   checks, failures);
	return !ok || failures != 0;
}
