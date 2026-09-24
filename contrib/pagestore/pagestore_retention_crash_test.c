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

/*
 * Bug finding #6: compacting an already-empty, never-created registry
 * (retention.meta was never written because nothing was ever appended) must
 * survive a crash right after the durable pending intent (old_nrecords ==
 * new_nrecords == 0) even though there is no retention.meta to roll forward
 * or back to.  Before the fix, recovery's roll-forward branch unconditionally
 * installed retention.state, leaving a store with state but no meta that the
 * ordinary open path then permanently refused.
 */
static void
run_empty_compact_child(const char *store, const char *fault_dir)
{
	if (!configure_fault(store, fault_dir, "retention_compact.after_pending"))
		_exit(2);
	if (ps_retention_open(store) != 0)
		_exit(2);
	if (!arm_fault(fault_dir))
		_exit(2);
	(void) ps_retention_compact();
	_exit(3);					/* the fault should have fired already */
}

static int
run_empty_compact_case(void)
{
	char		store[] = "/tmp/psretentioncrashemptyXXXXXX";
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
		run_empty_compact_child(store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, "compact after_pending on an already-empty, never-created registry");
	if (ok)
	{
		PsRetentionPin *pins = NULL;
		uint32_t	count = 0;

		check(ps_retention_open(store) == 0,
			  "reopen after an empty-registry compact crash succeeds");
		check(ps_retention_snapshot_alloc(&pins, &count) == 0 && count == 0,
			  "no pins after recovering from an empty-registry compact crash");
		free(pins);
		pins = NULL;
		ps_retention_close();
		check(ps_retention_open(store) == 0,
			  "second restart after an empty-registry compact crash is idempotent");
		check(ps_retention_snapshot_alloc(&pins, &count) == 0 && count == 0,
			  "still no pins after the second restart");
		free(pins);
		ps_retention_close();
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

/*
 * Bug finding #1: a v2-format store that predates the committed-prefix
 * retention.state file records a state-only bootstrap intent (old_nrecords
 * == new_nrecords) the first time it is opened.  A crash between installing
 * that intent and writing retention.state must still be recoverable on the
 * next open.
 */
static void
run_bootstrap_child(const char *store, const char *fault_dir)
{
	if (!configure_fault(store, fault_dir, "retention_bootstrap.after_pending"))
		_exit(2);
	if (!arm_fault(fault_dir))
		_exit(2);
	(void) ps_retention_open(store);
	_exit(3);					/* the fault should have fired already */
}

/* Populate a v2 store the ordinary way, then strip its committed-prefix
 * state file to reproduce "a v2 prefix store without retention.state" --
 * the exact precondition the state-only bootstrap path in
 * ps_retention_open() reconciles. */
static int
setup_state_stripped_store(const char *store)
{
	char		state_path[1700];

	if (!populate_store(store))
		return 0;
	ps_retention_close();
	if (snprintf(state_path, sizeof(state_path), "%s/retention.state",
				 store) < 0)
		return 0;
	return unlink(state_path) == 0;
}

/*
 * Round-2 finding #1: the log a state-only bootstrap installs state over
 * predates crash-safe mutation and can carry the same kind of harmless torn
 * tail (a short, incomplete final record) the ordinary committed-prefix
 * replay path has always tolerated (see the "short final record is
 * recoverable" contract in pagestore_retention_test.c).  Before the fix,
 * bootstrap's own committed-prefix check demanded an exact size match, so a
 * crash at retention_bootstrap.after_pending -- before the non-crashed path's
 * own truncation of that same tail -- made recovery reject the store
 * forever.
 */
static int
run_bootstrap_torn_tail_case(void)
{
	char		store[] = "/tmp/psretentioncrashbstornXXXXXX";
	char		meta_path[1700];
	char		fault_dir[1600];
	pid_t		pid;
	int			status;
	int			ok;

	if (mkdtemp(store) == NULL)
		return 0;
	if (!setup_state_stripped_store(store))
	{
		remove_tree(store);
		return 0;
	}
	if (snprintf(meta_path, sizeof(meta_path), "%s/retention.meta",
				 store) < 0)
	{
		remove_tree(store);
		return 0;
	}
	{
		int			fd = open(meta_path, O_WRONLY | O_APPEND);
		int			wrote = fd >= 0 &&
			write(fd, "short", 5) == 5 && fsync(fd) == 0;

		if (fd >= 0 && close(fd) != 0)
			wrote = 0;
		check(wrote, "append an incomplete tail before the bootstrap crash "
			  "(test setup)");
		if (!wrote)
		{
			remove_tree(store);
			return 0;
		}
	}
	if (snprintf(fault_dir, sizeof(fault_dir), "%s.fault", store) < 0 ||
		mkdir(fault_dir, 0700) != 0)
	{
		remove_tree(store);
		return 0;
	}
	pid = fork();
	if (pid == 0)
		run_bootstrap_child(store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, "state bootstrap after_pending on a store with a torn tail");
	if (ok)
	{
		check(snapshot_matches_expected(store, 0),
			  "first reopen recovers the expected pin set after an "
			  "interrupted torn-tail bootstrap");
		check(snapshot_matches_expected(store, 0),
			  "second restart after an interrupted torn-tail bootstrap is "
			  "idempotent");
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

static int
run_bootstrap_case(void)
{
	char		store[] = "/tmp/psretentioncrashbootstrapXXXXXX";
	char		fault_dir[1600];
	pid_t		pid;
	int			status;
	int			ok;

	if (mkdtemp(store) == NULL)
		return 0;
	if (!setup_state_stripped_store(store))
	{
		remove_tree(store);
		return 0;
	}
	if (snprintf(fault_dir, sizeof(fault_dir), "%s.fault", store) < 0 ||
		mkdir(fault_dir, 0700) != 0)
	{
		remove_tree(store);
		return 0;
	}
	pid = fork();
	if (pid == 0)
		run_bootstrap_child(store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, "state bootstrap after_pending on a v2 store missing retention.state");
	if (ok)
	{
		check(snapshot_matches_expected(store, 0),
			  "first reopen recovers the expected pin set after an "
			  "interrupted state bootstrap");
		check(snapshot_matches_expected(store, 0),
			  "second restart after an interrupted state bootstrap is "
			  "idempotent");
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

/*
 * The other half of finding #1: if the on-disk log no longer matches the
 * bootstrap intent's recorded (old_nrecords, old_hash) -- something changed
 * retention.meta out from under the interrupted bootstrap -- recovery must
 * fail closed instead of guessing.  This is not itself a fresh-crash
 * scenario (nothing in this codebase mutates retention.meta without going
 * through the pending-intent protocol), but it is exactly the
 * "unrecognized intent" class retention_read_pending()/verify_v2_prefix()
 * exist to catch, so it is exercised directly here.
 *
 * The corruption must land *inside* the committed prefix (flip a byte of an
 * existing record), not merely extend the file: appending fewer than
 * sizeof(PsRetentionRecord) bytes is now the tolerated torn-tail case (see
 * run_bootstrap_torn_tail_case()), so it would no longer prove a mismatch.
 */
static int
run_bootstrap_mismatch_case(void)
{
	char		store[] = "/tmp/psretentioncrashbsmismatchXXXXXX";
	char		meta_path[1700];
	char		fault_dir[1600];
	pid_t		pid;
	int			status;
	int			ok;

	if (mkdtemp(store) == NULL)
		return 0;
	if (!setup_state_stripped_store(store))
	{
		remove_tree(store);
		return 0;
	}
	if (snprintf(meta_path, sizeof(meta_path), "%s/retention.meta",
				 store) < 0)
	{
		remove_tree(store);
		return 0;
	}
	if (snprintf(fault_dir, sizeof(fault_dir), "%s.fault", store) < 0 ||
		mkdir(fault_dir, 0700) != 0)
	{
		remove_tree(store);
		return 0;
	}
	pid = fork();
	if (pid == 0)
		run_bootstrap_child(store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, "state bootstrap after_pending setup for the mismatch case");
	if (ok)
	{
		/* Simulate the log changing out from under the interrupted
		 * bootstrap: flip a byte inside the first committed record so the
		 * pending intent's recorded (old_nrecords, old_hash) no longer
		 * matches what is on disk, without changing retention.meta's size
		 * (a size change alone is the tolerated torn-tail case). */
		int			fd = open(meta_path, O_RDWR);
		unsigned char byte = 0;
		int			wrote = fd >= 0 && pread(fd, &byte, 1, 0) == 1;

		if (wrote)
		{
			byte ^= 0x40;
			wrote = pwrite(fd, &byte, 1, 0) == 1 && fsync(fd) == 0;
		}
		if (fd >= 0 && close(fd) != 0)
			wrote = 0;

		check(wrote, "corrupting retention.meta under the pending bootstrap "
			  "succeeds (test setup)");
		if (wrote)
		{
			int			open_rc = ps_retention_open(store);

			check(open_rc != 0,
				  "a bootstrap intent that no longer matches the on-disk "
				  "log fails closed instead of guessing");
			if (open_rc == 0)
				ps_retention_close();
		}
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

/*
 * Bug finding #4: retention_read_pending() must reject a retention.pending
 * file carrying trailing bytes past the fixed-size record, not silently
 * read only the first sizeof(PsRetentionPending) bytes and ignore the rest.
 */
static int
run_pending_trailing_bytes_case(void)
{
	char		store[] = "/tmp/psretentioncrashtrailingXXXXXX";
	char		fault_dir[1600];
	char		pending_path[1700];
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
		run_child(&cases[2] /* "append after_write on a populated store" */,
				  store, fault_dir);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, "append after_write crash setup for the trailing-bytes case");
	if (ok)
	{
		int			fd;
		int			wrote;

		if (snprintf(pending_path, sizeof(pending_path),
					 "%s/retention.pending", store) < 0)
			ok = 0;
		check(ok, "retention.pending path fits (test setup)");
		if (ok)
		{
			fd = open(pending_path, O_WRONLY | O_APPEND);
			wrote = fd >= 0 && write(fd, "\0", 1) == 1 && close(fd) == 0;
			check(wrote, "appending a trailing byte to retention.pending "
				  "succeeds (test setup)");
			if (wrote)
			{
				int			open_rc = ps_retention_open(store);

				check(open_rc != 0,
					  "a retention.pending file with trailing bytes fails "
					  "closed instead of being silently truncated to the "
					  "fixed-size record");
				if (open_rc == 0)
					ps_retention_close();
			}
		}
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

/*
 * Round-2 finding #3: retention_begin_pending() now publishes
 * retention.pending atomically via a private retention.pending.tmp, fsync,
 * rename.  A crash before the rename can leave that tmp file behind, with
 * either partial contents (a crash right after O_CREAT, before any bytes
 * are written -- retention_pending.after_create) or complete contents (a
 * crash after the tmp is fully written and fsync'd, before the rename --
 * retention_pending.after_tmp_sync).  Neither window has touched
 * retention.pending or the log itself, so the next open must remove the
 * leftover tmp and proceed as if nothing had been attempted.
 */
static void
run_pending_tmp_leftover_child(const char *store, const char *fault_dir,
							   const char *fault_name)
{
	PsRetentionPin pin;

	if (!configure_fault(store, fault_dir, fault_name))
		_exit(2);
	if (ps_retention_open(store) != 0)
		_exit(2);
	if (!arm_fault(fault_dir))
		_exit(2);
	make_pin(&pin, PS_RETENTION_OWNER_READER, PIN_A_OWNER,
			 PS_RETENTION_RESOURCE_PAGE_HISTORY, 1000, 1, 1);
	(void) ps_retention_set(&pin);
	_exit(3);					/* the fault should have fired already */
}

static int
run_pending_tmp_leftover_case(const char *fault_name, const char *label)
{
	char		store[] = "/tmp/psretentioncrashtmpleftoverXXXXXX";
	char		fault_dir[1600];
	char		tmp_path[1700];
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
		run_pending_tmp_leftover_child(store, fault_dir, fault_name);
	ok = pid > 0 && waitpid(pid, &status, 0) == pid &&
		WIFEXITED(status) && WEXITSTATUS(status) == PS_FAULT_CRASH_EXIT;
	check(ok, label);
	if (ok)
	{
		PsRetentionPin *pins = NULL;
		uint32_t	count = 0;
		int			path_ok = snprintf(tmp_path, sizeof(tmp_path),
										"%s/retention.pending.tmp", store) > 0;

		check(path_ok, "retention.pending.tmp path fits (test setup)");
		check(ps_retention_open(store) == 0,
			  "reopen after a leftover retention.pending.tmp succeeds");
		check(ps_retention_snapshot_alloc(&pins, &count) == 0 && count == 0,
			  "the never-begun mutation left no pin behind");
		free(pins);
		pins = NULL;
		if (path_ok)
			check(access(tmp_path, F_OK) != 0 && errno == ENOENT,
				  "the leftover retention.pending.tmp is removed on open");
		ps_retention_close();
		check(ps_retention_open(store) == 0,
			  "second restart after a leftover retention.pending.tmp is "
			  "idempotent");
		check(ps_retention_snapshot_alloc(&pins, &count) == 0 && count == 0,
			  "still no pin after the second restart");
		free(pins);
		ps_retention_close();
	}
	remove_tree(fault_dir);
	remove_tree(store);
	return ok;
}

/*
 * Codex round-3 (pagestore_retention.c:1680): a retention.pending shorter
 * than a complete record must fail closed, not be discarded.  The
 * op-sequence fuzzer independently found a 0-byte retention.pending in
 * production (failure root 20260925T042327-w2-seed1211994399), but that was
 * a SIGKILL landing between retention_begin_pending()'s open(O_CREAT|O_EXCL)
 * and its write() -- an artifact of this PR branch's own intermediate,
 * never-released in-place-write implementation (the commit right before the
 * atomic tmp+rename publish above), not of the *previous* (pre-this-PR)
 * format.  That intermediate implementation cannot recur once fixed, and
 * needs no compatibility path.
 *
 * The previous format's retention.pending, however, was an intentionally
 * empty guard: created (and fsynced) before a mutation, removed only after
 * that mutation's own writes (the log append, and -- for a DROP -- the
 * matching retention.state) were durable.  A crash between that removal and
 * the mutation completing leaves retention.meta and retention.state fully
 * consistent with each other, but the mutation was never acknowledged to
 * its caller.  A short retention.pending cannot be told apart, by its shape
 * alone, from a crash before the mutation ever started, so it cannot be
 * discarded on the strength of meta/state looking fine: doing so could let
 * an unacknowledged DROP silently take effect and reclaim still-needed
 * history.  It must fail closed, exactly as the previous format always did.
 */
static int
run_short_pending_fails_closed_case(void)
{
	char		store[] = "/tmp/psretentioncrashshortpendXXXXXX";
	char		pending_path[1700];
	int			fd;
	int			ok;
	int			open_rc;

	if (mkdtemp(store) == NULL)
		return 0;
	ok = populate_store(store);
	check(ok, "populate a store for the short-pending fail-closed case");
	ps_retention_close();
	if (ok)
	{
		ok = snprintf(pending_path, sizeof(pending_path),
					  "%s/retention.pending", store) > 0;
		check(ok, "retention.pending path fits (test setup)");
	}
	if (ok)
	{
		fd = open(pending_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
		ok = fd >= 0 && close(fd) == 0;
		check(ok, "create a 0-byte retention.pending (test setup)");
	}
	if (ok)
	{
		open_rc = ps_retention_open(store);
		check(open_rc != 0,
			  "a 0-byte retention.pending fails closed instead of being "
			  "silently discarded");
		if (open_rc == 0)
			ps_retention_close();
		check(access(pending_path, F_OK) == 0,
			  "the 0-byte retention.pending is left in place for manual "
			  "inspection, not removed");
	}
	remove_tree(store);
	return ok;
}

/*
 * Codex's own scenario, built directly: retention.meta/retention.state
 * already durably and consistently reflect a completed DROP (via the
 * ordinary API, so both are exactly as a successful DROP leaves them), and
 * a 0-byte guard sits alongside them as if a previous-format implementation
 * had crashed after that DROP committed but before removing its guard.
 * Meta/state validation alone cannot distinguish this from a guard that
 * predates any mutation, so this must fail closed too, regardless of how
 * consistent meta/state look.
 */
static int
run_legacy_guard_unacked_drop_case(void)
{
	char		store[] = "/tmp/psretentioncrashlegacydropXXXXXX";
	char		pending_path[1700];
	PsRetentionPin pin;
	int			fd;
	int			ok;

	if (mkdtemp(store) == NULL)
		return 0;
	ok = ps_retention_open(store) == 0;
	check(ok, "open a store for the legacy-guard unacked-DROP case");
	if (ok)
	{
		make_pin(&pin, PS_RETENTION_OWNER_READER, PIN_D_OWNER,
				 PS_RETENTION_RESOURCE_PAGE_HISTORY, 4000, 1, 1);
		ok = ps_retention_set(&pin) == PS_RETENTION_OK;
		check(ok, "persist a pin to later drop (test setup)");
	}
	if (ok)
	{
		ok = ps_retention_drop(pin.timeline, pin.owner_kind, pin.owner_id,
								2) == PS_RETENTION_OK;
		check(ok, "durably drop the pin through the ordinary, already-fixed "
			  "API (test setup)");
	}
	ps_retention_close();
	if (ok)
	{
		ok = snprintf(pending_path, sizeof(pending_path),
					  "%s/retention.pending", store) > 0;
		check(ok, "retention.pending path fits (test setup)");
	}
	if (ok)
	{
		/* retention.meta/retention.state are now durably consistent with
		 * the DROP having fully committed, with no pending marker left
		 * behind (the fixed API already cleared it).  Drop in a 0-byte
		 * guard as if an old-format implementation had crashed after that
		 * same commit but before removing its own guard. */
		fd = open(pending_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
		ok = fd >= 0 && close(fd) == 0;
		check(ok, "create a 0-byte legacy guard over a committed DROP "
			  "(test setup)");
	}
	if (ok)
	{
		int			open_rc = ps_retention_open(store);

		check(open_rc != 0,
			  "a 0-byte legacy guard fails closed even though "
			  "retention.meta/retention.state already durably agree on the "
			  "DROP it might be guarding");
		if (open_rc == 0)
			ps_retention_close();
		check(access(pending_path, F_OK) == 0,
			  "the legacy guard is left in place, not silently removed");
	}
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
	if (!run_empty_compact_case())
		ok = 0;
	if (!run_bootstrap_case())
		ok = 0;
	if (!run_bootstrap_torn_tail_case())
		ok = 0;
	if (!run_bootstrap_mismatch_case())
		ok = 0;
	if (!run_pending_trailing_bytes_case())
		ok = 0;
	if (!run_pending_tmp_leftover_case("retention_pending.after_create",
									   "pending-tmp leftover with partial "
									   "contents (crash after create, "
									   "before write)"))
		ok = 0;
	if (!run_pending_tmp_leftover_case("retention_pending.after_tmp_sync",
									   "pending-tmp leftover with complete "
									   "contents (crash after tmp fsync, "
									   "before rename)"))
		ok = 0;
	if (!run_short_pending_fails_closed_case())
		ok = 0;
	if (!run_legacy_guard_unacked_drop_case())
		ok = 0;
	printf("pagestore_retention_crash_test: %d checks, %d failed\n",
		   checks, failures);
	return !ok || failures != 0;
}
