/*-------------------------------------------------------------------------
 *
 * pagestore_retention.c
 *	  Durable registry of page-store retention pins.
 *
 * A pin says that one owner still needs a timeline at an LSN for one or more
 * resources (page history, shipped WAL, and the per-page WAL index).  The log
 * is deliberately independent of layers.manifest: reader/materializer lease
 * churn must not poison immutable-layer publication, and every future space
 * reclaimer can consume the same small registry.
 *
 * Records are fixed-size and CRC-protected.  A short final record is an
 * uncommitted append and is truncated during recovery; a full corrupt record
 * fails startup.  Losing a valid DROP would only retain too much, but losing a
 * valid SET could reclaim live history, so recovery never guesses.
 *
 * A durable pending marker (retention.pending) is installed before every
 * mutation (an in-place log append, or a full compaction/migration rewrite).
 * Unlike a bare guard file, it carries the mutation's *intent*: the record
 * count and rolling hash the log/state pair had before the mutation started
 * (old_nrecords/old_hash) and the count/hash it is meant to reach
 * (new_nrecords/new_hash), CRC-protected.  A process death anywhere in the
 * window between installing that marker and removing it again leaves no
 * ambiguity: the next open reads the surviving intent, compares it against
 * whatever bytes retention.meta actually has, and deterministically rolls the
 * mutation back (if the log still looks like "old", tolerating a torn or
 * complete trailing append record) or forward (if it already looks like
 * "new"), then clears the marker and continues the ordinary replay path.
 * Every step of that reconciliation is itself crash-safe: replaying it twice
 * after a second crash mid-recovery reaches the same fixed point.  A pending
 * marker with unrecognized or truncated content (the previous format's
 * zero-byte guard, or disk corruption) cannot be reconciled and still fails
 * closed, exactly as before.
 *
 * That automatic reconciliation is only safe for genuine process death: it
 * assumes nobody was ever told whether the mutation succeeded.  A handful of
 * failures happen instead in a *live* process — an fsync, rename, or unlink
 * step reports an error while the daemon keeps running and keeps answering
 * (rejecting) requests.  Those durably install retention.failed, a permanent
 * marker predating the intent format (see the legacy guard this file already
 * honored) that open() refuses to look past even after retention.pending is
 * gone, so a later restart can never silently resurrect a mutation whose
 * failure the process already observed and may have acted on.
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_fault.h"
#include "pagestore_retention.h"
#include "pagestore_format.h"

#define PS_RETENTION_MAGIC		0x4e544552	/* "RETN" */
#define PS_RETENTION_VERSION	2
#define PS_RETENTION_VERSION_V1 1
#define PS_RETENTION_FNV_INIT	2166136261u
#define PS_RETENTION_STATE_MAGIC 0x53544552	/* "RETS" */
#define PS_RETENTION_PENDING_MAGIC 0x444e5052	/* "RPND" */
#define PS_RETENTION_PENDING_VERSION 1

/* What a durable retention.pending marker is in the middle of doing: extend
 * the log in place by one record, replace it wholesale (compaction, and the
 * identical v1 -> v2 migration rewrite), or install retention.state for the
 * first time over an unchanged log (the one-time bootstrap of a v2-format
 * store that predates the committed-prefix state file).  Bootstrap is not
 * an APPEND: it never grows the log (new_nrecords == old_nrecords), which
 * would otherwise be indistinguishable from a truncated/corrupt intent
 * under APPEND's "always grows by exactly one" rule. */
typedef enum PsRetentionPendingOp
{
	PS_RETENTION_PENDING_APPEND = 1,
	PS_RETENTION_PENDING_COMPACT = 2,
	PS_RETENTION_PENDING_BOOTSTRAP_STATE = 3,
} PsRetentionPendingOp;

/* The intent behind an in-flight mutation: the committed (nrecords, hash)
 * pair before and after.  Recovery classifies whatever is actually on disk
 * against these two fixed points instead of guessing. */
typedef struct PsRetentionPending
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	op;
	uint32_t	pad;
	uint64_t	old_nrecords;
	uint32_t	old_hash;
	uint32_t	pad2;
	uint64_t	new_nrecords;
	uint32_t	new_hash;
	uint32_t	crc;
} PsRetentionPending;

typedef enum PsRetentionRecordType
{
	PS_RETENTION_SET = 1,
	PS_RETENTION_DROP = 2,
	PS_RETENTION_ADMISSION_RESERVE = 3,
} PsRetentionRecordType;

typedef struct PsRetentionRecord
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	type;
	uint32_t	len;
	PsRetentionPin pin;
	uint32_t	crc;
	uint32_t	pad;
} PsRetentionRecord;

/* Version 1 predates exact same-LSN admission fences.  Keep its byte layout
 * here so an upgrade can validate the committed old prefix before rewriting
 * it.  Missing admission sequences replay as the conservative legacy value 0. */
typedef struct PsRetentionPinV1
{
	uint32_t	timeline;
	uint32_t	owner_kind;
	uint32_t	resources;
	uint32_t	generation;
	uint64_t	owner_id;
	uint64_t	lsn;
} PsRetentionPinV1;

typedef struct PsRetentionRecordV1
{
	uint32_t	magic;
	uint32_t	version;
	uint32_t	type;
	uint32_t	len;
	PsRetentionPinV1 pin;
	uint32_t	crc;
	uint32_t	pad;
} PsRetentionRecordV1;

typedef struct PsRetentionState
{
	uint32_t	magic;
	uint32_t	version;
	uint64_t	nrecords;
	uint32_t	log_hash;
	uint32_t	crc;
} PsRetentionState;

static char retention_path[4096];
static char retention_marker_path[4096];
static char retention_pending_path[4096];
static char retention_pending_tmp_path[4096];
static char retention_state_path[4096];
static char retention_failed_path[4096];
static char retention_dir[2048];
static PsRetentionPin *retention_pins;
static uint32_t retention_npins;
static uint32_t retention_cap;
static uint64_t retention_nrecords;
static uint64_t retention_mutation_epoch;
static uint64_t retention_admission_highwater;
static uint32_t retention_log_hash;
static int retention_is_poisoned;
static dev_t retention_dev;
static ino_t retention_ino;
static struct timespec retention_compact_retry_at;
static pthread_mutex_t retention_lock = PTHREAD_MUTEX_INITIALIZER;
#ifdef PAGESTORE_RETENTION_TEST
static int retention_test_fail_snapshot_alloc;
#endif

#ifdef PAGESTORE_RETENTION_TEST
void
ps_test_retention_fail_snapshot_alloc(int fail)
{
	retention_test_fail_snapshot_alloc = fail != 0;
}
#endif

static int
retention_new_incarnation(uint64_t *epoch)
{
	int fd = open("/dev/urandom", O_RDONLY);
	ssize_t amount;

	if (fd < 0)
		return -1;
	do
		amount = read(fd, epoch, sizeof(*epoch));
	while (amount < 0 && errno == EINTR);
	if (close(fd) != 0 || amount != sizeof(*epoch) || *epoch == 0 ||
		*epoch == UINT64_MAX)
		return -1;
	return 0;
}

/* Standalone-test fault injection; ordinary deployments leave these zero. */
static int test_fail_append_after_write;
static int test_fail_rollback;
static int test_fail_clear_pending_dir_fsync;

static int retention_identity_matches(const struct stat *st);

#define PS_RETENTION_COMPACT_RETRY_MS 1000

static int
retention_compact_retry_ready(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		return 0;
	return now.tv_sec > retention_compact_retry_at.tv_sec ||
		(now.tv_sec == retention_compact_retry_at.tv_sec &&
		 now.tv_nsec >= retention_compact_retry_at.tv_nsec);
}

static void
retention_defer_compact(void)
{
	if (clock_gettime(CLOCK_MONOTONIC, &retention_compact_retry_at) != 0)
		return;
	retention_compact_retry_at.tv_nsec +=
		(long) PS_RETENTION_COMPACT_RETRY_MS * 1000000L;
	if (retention_compact_retry_at.tv_nsec >= 1000000000L)
	{
		retention_compact_retry_at.tv_sec++;
		retention_compact_retry_at.tv_nsec -= 1000000000L;
	}
}

static uint32_t
retention_fnv1a(uint32_t h, const void *data, size_t len)
{
	const unsigned char *p = data;

	for (size_t i = 0; i < len; i++)
	{
		h ^= p[i];
		h *= 16777619u;
	}
	return h;
}

static uint32_t
retention_record_crc(const PsRetentionRecord *rec)
{
	return retention_fnv1a(PS_RETENTION_FNV_INIT, rec,
						   offsetof(PsRetentionRecord, crc));
}

static uint32_t
retention_record_v1_crc(const PsRetentionRecordV1 *rec)
{
	return retention_fnv1a(PS_RETENTION_FNV_INIT, rec,
						   offsetof(PsRetentionRecordV1, crc));
}

static int
retention_kind_valid(uint32_t kind)
{
	return kind == PS_RETENTION_OWNER_READER ||
		kind == PS_RETENTION_OWNER_MATERIALIZER ||
		kind == PS_RETENTION_OWNER_CONFIGURED;
}

static int
retention_resources_valid(uint32_t resources)
{
	return resources != 0 &&
		(resources & ~((uint32_t) PS_RETENTION_RESOURCE_ALL)) == 0;
}

static int
retention_pin_valid(const PsRetentionPin *pin)
{
	return pin != NULL && retention_kind_valid(pin->owner_kind) &&
		retention_resources_valid(pin->resources) && pin->owner_id != 0 &&
		pin->lsn != 0 && pin->admission_seq < UINT64_MAX - 1;
}

static int
retention_pin_active(const PsRetentionPin *pin)
{
	return pin->resources != 0;
}

static int
retention_record_valid(const PsRetentionRecord *rec)
{
	if (rec->magic != PS_RETENTION_MAGIC ||
		rec->version != PS_RETENTION_VERSION ||
		rec->len != sizeof(*rec) || rec->pad != 0 ||
		retention_record_crc(rec) != rec->crc)
		return 0;
	if (rec->type == PS_RETENTION_SET)
		return retention_pin_valid(&rec->pin);
	if (rec->type == PS_RETENTION_DROP)
		return retention_kind_valid(rec->pin.owner_kind) &&
			rec->pin.owner_id != 0 && rec->pin.resources == 0 &&
			rec->pin.lsn == 0;
	if (rec->type == PS_RETENTION_ADMISSION_RESERVE)
	{
		PsRetentionPin zero = {0};

		zero.admission_seq = rec->pin.admission_seq;
		return rec->pin.admission_seq != 0 &&
			rec->pin.admission_seq < UINT64_MAX - 1 &&
			memcmp(&rec->pin, &zero, sizeof(zero)) == 0;
	}
	return 0;
}

static int
retention_record_v1_convert(const PsRetentionRecordV1 *old,
							PsRetentionRecord *rec)
{
	PsRetentionPin pin;

	if (old->magic != PS_RETENTION_MAGIC ||
		old->version != PS_RETENTION_VERSION_V1 ||
		old->len != sizeof(*old) || old->pad != 0 ||
		retention_record_v1_crc(old) != old->crc)
		return -1;
	memset(&pin, 0, sizeof(pin));
	pin.timeline = old->pin.timeline;
	pin.owner_kind = old->pin.owner_kind;
	pin.resources = old->pin.resources;
	pin.generation = old->pin.generation;
	pin.owner_id = old->pin.owner_id;
	pin.lsn = old->pin.lsn;
	memset(rec, 0, sizeof(*rec));
	rec->magic = PS_RETENTION_MAGIC;
	rec->version = PS_RETENTION_VERSION;
	rec->type = old->type;
	rec->len = sizeof(*rec);
	rec->pin = pin;
	rec->crc = retention_record_crc(rec);
	return retention_record_valid(rec) ? 0 : -1;
}

static int
retention_find(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id)
{
	for (uint32_t i = 0; i < retention_npins; i++)
		if (retention_pins[i].timeline == timeline &&
			retention_pins[i].owner_kind == owner_kind &&
			retention_pins[i].owner_id == owner_id)
			return (int) i;
	return -1;
}

/* Caller holds retention_lock. */
static uint32_t
retention_active_count(void)
{
	uint32_t	count = 0;

	for (uint32_t i = 0; i < retention_npins; i++)
		if (retention_pin_active(&retention_pins[i]))
			count++;
	return count;
}

/* Caller holds retention_lock. */
static void
retention_diagnostic_fill_locked(PsRetentionDiagnostic *out)
{
	memset(out, 0, sizeof(*out));
	if (retention_is_poisoned)
	{
		out->poisoned = 1;
		return;
	}
	for (uint32_t i = 0; i < retention_npins; i++)
	{
		const PsRetentionPin *pin = &retention_pins[i];

		if (!retention_pin_active(pin))
			continue;
		out->owner_count++;
		if (pin->resources & PS_RETENTION_RESOURCE_PAGE_HISTORY)
			out->page_history_owners++;
		if (pin->resources & PS_RETENTION_RESOURCE_WAL)
			out->wal_owners++;
		if (pin->resources & PS_RETENTION_RESOURCE_WAL_INDEX)
			out->wal_index_owners++;
		if (pin->generation > out->max_generation)
			out->max_generation = pin->generation;
	}
}

/* Exact retries must still notice a lost, truncated, or replaced log. */
static int
retention_log_matches(void)
{
	struct stat st;

	return stat(retention_path, &st) == 0 &&
		retention_identity_matches(&st) &&
		st.st_size == (off_t) (retention_nrecords *
								 sizeof(PsRetentionRecord));
}

static int
retention_reserve(uint32_t need)
{
	PsRetentionPin *grown;
	uint32_t	newcap;

	if (need <= retention_cap)
		return 0;
	newcap = retention_cap ? retention_cap * 2 : 16;
	while (newcap < need)
		newcap *= 2;
	grown = realloc(retention_pins, (size_t) newcap * sizeof(*grown));
	if (grown == NULL)
		return -1;
	retention_pins = grown;
	retention_cap = newcap;
	return 0;
}

static int
retention_apply(const PsRetentionRecord *rec)
{
	int			idx;

	if (rec->type == PS_RETENTION_ADMISSION_RESERVE)
	{
		if (rec->pin.admission_seq <= retention_admission_highwater)
			return -1;
		retention_admission_highwater = rec->pin.admission_seq;
		return 0;
	}
	idx = retention_find(rec->pin.timeline,
					 rec->pin.owner_kind, rec->pin.owner_id);

	if (rec->type == PS_RETENTION_SET)
	{
		if (idx >= 0 &&
			rec->pin.generation < retention_pins[idx].generation)
			return -1;
		if (idx >= 0 &&
			rec->pin.generation == retention_pins[idx].generation &&
			rec->pin.generation != 0 &&
			!retention_pin_active(&retention_pins[idx]))
			return -1;
		if (idx >= 0)
			retention_pins[idx] = rec->pin;
		else
		{
			if (retention_reserve(retention_npins + 1) != 0)
				return -1;
			retention_pins[retention_npins++] = rec->pin;
		}
	}
	else if (idx >= 0)
	{
		if (rec->pin.generation < retention_pins[idx].generation)
			return -1;
		retention_pins[idx] = rec->pin;
	}
	else
	{
		if (retention_reserve(retention_npins + 1) != 0)
			return -1;
		retention_pins[retention_npins++] = rec->pin;
	}
	return 0;
}

static int
retention_fsync_dir(void)
{
	int			fd = open(retention_dir, O_RDONLY | O_DIRECTORY);
	int			rc;

	if (fd < 0)
		return -1;
	rc = fsync(fd);
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

/* Caller holds retention_lock.  The marker is permanent: once a registry has
 * existed, a missing log must never be interpreted as a new empty registry. */
static int
retention_mark_initialized(void)
{
	int fd;
	int rc = 0;

	fd = open(retention_marker_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
		return errno == EEXIST ? 0 : -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	if (rc == 0 && retention_fsync_dir() != 0)
		rc = -1;
	return rc;
}

static uint32_t
retention_pending_crc(const PsRetentionPending *pending)
{
	return retention_fnv1a(PS_RETENTION_FNV_INIT, pending,
						   offsetof(PsRetentionPending, crc));
}

/* Caller holds retention_lock.  No log byte may change until this guard and
 * its directory entry are durable.  The marker carries the mutation's
 * intent (the committed (nrecords, hash) before and after) so a later open
 * can reconcile an interrupted mutation instead of refusing to start.
 *
 * Published atomically -- write a private retention.pending.tmp, fsync it,
 * rename(2) it into place, fsync the directory -- rather than written in
 * place.  An in-place O_CREAT|O_EXCL write of the intent record left a
 * window where a crash between the create and the write produced a
 * retention.pending that existed but did not yet hold a complete, valid
 * record: retention_read_pending()'s size/CRC checks correctly refused to
 * trust it, but that meant every future open was permanently refused even
 * though no log byte had actually changed yet.  (That in-place-write
 * implementation was itself only ever an intermediate state of this
 * feature on this branch -- it never shipped -- but the op-sequence fuzzer
 * still caught it: a SIGKILL landing exactly in that window left a 0-byte
 * retention.pending in a captured failure store.)  With rename(2) atomic
 * on every filesystem this backend supports, retention_pending_path can
 * now only ever be observed either absent or holding the complete bytes
 * that were durable before the rename, so this format's own writer can no
 * longer produce a short retention.pending at all.  A leftover
 * retention.pending.tmp from a crash during this function is inert:
 * ps_retention_open() below removes it unconditionally before ever looking
 * at retention_pending_path, because no log byte can have changed while
 * only the tmp file existed.
 *
 * A short retention.pending can still be found, though: it is exactly the
 * shape of the *previous* format's retention.pending, an intentionally
 * empty guard whose presence alone cannot prove whether the crash that
 * left it landed before or after the mutation it guarded (see the
 * short-file branch in ps_retention_open()).  That is genuinely
 * unreconcilable, not merely inconvenient, so it fails closed rather than
 * being discarded. */
static int
retention_begin_pending(uint32_t op, uint64_t old_nrecords, uint32_t old_hash,
						uint64_t new_nrecords, uint32_t new_hash)
{
	PsRetentionPending pending;
	int fd;
	int rc = 0;

	memset(&pending, 0, sizeof(pending));
	pending.magic = PS_RETENTION_PENDING_MAGIC;
	pending.version = PS_RETENTION_PENDING_VERSION;
	pending.op = op;
	pending.old_nrecords = old_nrecords;
	pending.old_hash = old_hash;
	pending.new_nrecords = new_nrecords;
	pending.new_hash = new_hash;
	pending.crc = retention_pending_crc(&pending);

	fd = open(retention_pending_tmp_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
		return -1;
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_PENDING_AFTER_CREATE) != 0)
		rc = -1;
	if (rc == 0 &&
		write(fd, &pending, sizeof(pending)) != (ssize_t) sizeof(pending))
		rc = -1;
	if (rc == 0 && fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	if (rc == 0 &&
		ps_fault_probe(PS_FAULT_POINT_RETENTION_PENDING_AFTER_TMP_SYNC) != 0)
		rc = -1;
	if (rc == 0 &&
		rename(retention_pending_tmp_path, retention_pending_path) != 0)
		rc = -1;
	if (rc == 0 && retention_fsync_dir() != 0)
		rc = -1;
	if (rc != 0)
	{
		unlink(retention_pending_tmp_path);
		unlink(retention_pending_path);
		(void) retention_fsync_dir();
	}
	return rc;
}

/* Caller holds retention_lock (or is the single-threaded ps_retention_open
 * path).  Permanently refuses every future open: installed only when a
 * *live* process could not prove whether a mutation committed (an fsync,
 * rename, or unlink step failed) and may already have reported that failure
 * or acted on it.  Unlike retention.pending, this marker survives even after
 * the bytes it guards are gone, so automatic reconciliation can never
 * silently resurrect what a running process already gave up on.  Best
 * effort: the caller is already on a failure path and has no better option
 * than to log and keep the in-memory poison bit either way.
 *
 * Deliberately NOT published via retention.pending's tmp+rename pattern:
 * this marker carries no payload (it is pure existence -- ps_retention_open()
 * only ever does access(retention_failed_path, F_OK), never parses its
 * bytes), so it cannot end up "torn" the way an in-place-written
 * retention.pending could -- there is nothing to tear.  A rename-based
 * publish would instead make this guard *weaker*: a crash between deciding
 * to refuse forever and completing the rename would leave the live process's
 * decision undone, letting a later restart silently proceed as if nothing
 * had gone wrong -- exactly what this marker exists to prevent.  The
 * in-place O_CREAT|O_EXCL create is the safer choice here: it maximizes the
 * chance that even a partially-durable creation still leaves a trace (the
 * directory entry) after a crash, and every step (fsync, close, directory
 * fsync) is still attempted and logged even after an earlier one fails,
 * unlike the fail-fast chains elsewhere in this file. */
static void
retention_mark_failed(void)
{
	int fd;

	fd = open(retention_failed_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
	{
		if (errno != EEXIST)
			fprintf(stderr,
					"pagestore_retention: could not install %s: %s\n",
					retention_failed_path, strerror(errno));
		return;
	}
	if (fsync(fd) != 0)
		fprintf(stderr, "pagestore_retention: could not fsync %s: %s\n",
				retention_failed_path, strerror(errno));
	if (close(fd) != 0)
		fprintf(stderr, "pagestore_retention: could not close %s: %s\n",
				retention_failed_path, strerror(errno));
	if (retention_fsync_dir() != 0)
		fprintf(stderr, "pagestore_retention: could not fsync %s: %s\n",
				retention_dir, strerror(errno));
}

/* The mutation the marker guarded is about to be acknowledged as committed
 * to a caller outside this process, so its removal must be durable, not
 * merely attempted: if the directory fsync fails and the marker's directory
 * entry later reappears after a machine crash (unlink() without a durable
 * directory is not itself durable), the next open would reconcile against a
 * pending intent whose mutation the caller was already told succeeded --
 * rolling back or otherwise second-guessing an acknowledged commit.  Every
 * caller that clears a marker on its success path must therefore treat a
 * nonzero return here exactly like a failed rename/write: install
 * retention.failed and refuse to acknowledge success.
 *
 * PS_TEST_FAIL_RETENTION_CLEAR_PENDING_DIR_FSYNC lets the standalone tests
 * simulate a directory fsync that fails after a real, successful unlink --
 * the exact window this function exists to make safe -- without requiring a
 * genuinely faulty filesystem. */
static int
retention_clear_pending(void)
{
	if (unlink(retention_pending_path) != 0)
		return -1;
	if (retention_fsync_dir() != 0)
		return -1;
	if (test_fail_clear_pending_dir_fsync > 0 &&
		--test_fail_clear_pending_dir_fsync == 0)
	{
		errno = EIO;
		return -1;
	}
	return 0;
}

static uint32_t
retention_state_crc(const PsRetentionState *state)
{
	return retention_fnv1a(PS_RETENTION_FNV_INIT, state,
						   offsetof(PsRetentionState, crc));
}

/* Caller holds retention_lock.  Atomic rewrite plus directory fsync makes the
 * expected prefix independent of retention.meta itself. */
static int
retention_write_state(uint64_t nrecords, uint32_t log_hash)
{
	char tmp[4096];
	PsRetentionState state;
	int fd = -1;
	int n;
	int rc = -1;

	n = snprintf(tmp, sizeof(tmp), "%s.tmp", retention_state_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		return -1;
	memset(&state, 0, sizeof(state));
	state.magic = PS_RETENTION_STATE_MAGIC;
	state.version = PS_RETENTION_VERSION;
	state.nrecords = nrecords;
	state.log_hash = log_hash;
	state.crc = retention_state_crc(&state);
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		goto done;
	if (write(fd, &state, sizeof(state)) != (ssize_t) sizeof(state) ||
		fsync(fd) != 0)
		goto done;
	if (close(fd) != 0)
	{
		fd = -1;
		goto done;
	}
	fd = -1;
	if (rename(tmp, retention_state_path) != 0 || retention_fsync_dir() != 0)
		goto done;
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
		unlink(tmp);
	return rc;
}

static int
retention_read_state(PsRetentionState *state)
{
	int fd;
	int rc = -1;
	unsigned char extra;

	fd = open(retention_state_path, O_RDONLY);
	if (fd < 0)
		return errno == ENOENT ? 0 : -1;
	if (read(fd, state, sizeof(*state)) != (ssize_t) sizeof(*state) ||
		read(fd, &extra, 1) != 0 || state->magic != PS_RETENTION_STATE_MAGIC ||
		(state->version != PS_RETENTION_VERSION &&
		 state->version != PS_RETENTION_VERSION_V1) ||
		state->crc != retention_state_crc(state))
	{
		errno = EILSEQ;
		goto done;
	}
	rc = 1;
done:
	if (close(fd) != 0)
		rc = -1;
	return rc;
}

static int
retention_identity_matches(const struct stat *st)
{
	return retention_ino != 0 && st->st_dev == retention_dev &&
		st->st_ino == retention_ino;
}

static int
retention_rollback(int fd, off_t old_size, int created)
{
	int rc = 0;

	if (test_fail_rollback > 0 && --test_fail_rollback == 0)
	{
		errno = EIO;
		rc = -1;
	}
	else if (ftruncate(fd, old_size) != 0)
		rc = -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	if (created && old_size == 0 && rc == 0)
	{
		if (unlink(retention_path) != 0 || retention_fsync_dir() != 0)
			rc = -1;
	}
	return rc;
}

/* Caller holds retention_lock.  If rollback cannot prove that the old prefix
 * is durable, leave the pre-durable guard in place so a restart cannot replay
 * an unacknowledged full DROP as committed. */
static int
retention_abort_append(int fd, off_t old_size, int created)
{
	if (retention_rollback(fd, old_size, created) != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
	}
	else
	{
		/* This is the abort path: the caller always gets -1 here, never an
		 * acknowledged commit, so a clear_pending() failure cannot let a
		 * resurrected marker roll back something already promised durable.
		 * The rollback above already put the log back to old_nrecords/
		 * old_hash, so even if the marker survives (unlink failed) or
		 * reappears after a crash (the directory fsync failed), the next
		 * open's reconciliation finds the log already matching "old" and
		 * performs a safe no-op rollback.  Best effort is enough; nothing
		 * downstream depends on this call succeeding. */
		(void) retention_clear_pending();
	}
	return -1;
}

/* Caller holds retention_lock. */
static int
retention_append(const PsRetentionRecord *rec)
{
	int			fd;
	int			created;
	off_t		old_size;
	uint64_t	old_nrecords;
	uint32_t	old_hash;
	uint32_t	new_hash;

	if (retention_is_poisoned)
		return -1;
	old_nrecords = retention_nrecords;
	old_hash = retention_log_hash;
	new_hash = retention_fnv1a(retention_log_hash, rec, sizeof(*rec));
	if (retention_begin_pending(PS_RETENTION_PENDING_APPEND, old_nrecords,
								old_hash, old_nrecords + 1, new_hash) != 0)
		return -1;
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_APPEND_AFTER_PENDING) != 0)
		return -1;
	created = 0;
	fd = open(retention_path, O_WRONLY | O_APPEND);
	if (fd < 0 && errno == ENOENT)
	{
		fd = open(retention_path, O_WRONLY | O_APPEND | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			created = 1;
	}
	if (fd < 0)
	{
		/* No log byte has been touched yet, so the pending intent's "old"
		 * state is still exactly what is on disk: a resurrected marker
		 * reconciles to a safe no-op rollback regardless of whether this
		 * clear succeeds, and this function always reports the append as
		 * failed either way.  Best effort is enough here. */
		(void) retention_clear_pending();
		return -1;
	}
	old_size = lseek(fd, 0, SEEK_END);
	if (old_size < 0)
	{
		close(fd);
		/* Same reasoning as above: nothing has been written yet. */
		(void) retention_clear_pending();
		return -1;
	}
	/* Detect an externally lost/truncated/replaced log before appending after
	 * the gap.  Replaying such a file could silently omit the last live SET. */
	{
		struct stat st;

		if (fstat(fd, &st) != 0 ||
			(!created && !retention_identity_matches(&st)) ||
			old_size != (off_t) (retention_nrecords * sizeof(*rec)))
		{
			errno = EILSEQ;
			(void) retention_abort_append(fd, old_size, created);
			retention_is_poisoned = 1;
			retention_mark_failed();
			return -1;
		}
		if (created)
		{
			retention_dev = st.st_dev;
			retention_ino = st.st_ino;
		}
	}
	if (write(fd, rec, sizeof(*rec)) != (ssize_t) sizeof(*rec))
		return retention_abort_append(fd, old_size, created);
	if (test_fail_append_after_write > 0 &&
		--test_fail_append_after_write == 0)
	{
		errno = EIO;
		return retention_abort_append(fd, old_size, created);
	}
	if (fsync(fd) != 0)
		return retention_abort_append(fd, old_size, created);
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_APPEND_AFTER_WRITE) != 0)
		return -1;
	if (close(fd) != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		return -1;
	}
	if (created && retention_fsync_dir() != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		return -1;
	}
	if (created && retention_mark_initialized() != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		return -1;
	}
	if (retention_write_state(old_nrecords + 1, new_hash) != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		return -1;
	}
	retention_log_hash = new_hash;
	retention_nrecords++;
	if (retention_clear_pending() != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		return -1;
	}
	return 0;
}

static void
retention_make_record(PsRetentionRecord *rec, uint32_t type,
					  const PsRetentionPin *pin)
{
	memset(rec, 0, sizeof(*rec));
	rec->magic = PS_RETENTION_MAGIC;
	rec->version = PS_RETENTION_VERSION;
	rec->type = type;
	rec->len = sizeof(*rec);
	rec->pin = *pin;
	rec->crc = retention_record_crc(rec);
}

/* Caller holds retention_lock.  Shared by ps_retention_compact() and the
 * fail-safe v1 -> v2 upgrade (retention_rewrite_current()): both replace the
 * entire log with one record per live pin/tombstone plus, if any, the
 * admission-reservation high-water record.  Publish the new log before its
 * v2 committed-prefix state, with the durable pending marker making every
 * interrupted ordering -- including a second crash during recovery itself --
 * fail closed until ps_retention_open() can reconcile it. */
/* The hash the rewritten log will have, computed purely from in-memory state
 * (no I/O) so the pending intent can carry the true post-compaction hash
 * instead of a placeholder.  Caller holds retention_lock, so retention_pins
 * and retention_admission_highwater cannot change between this call and the
 * write loop that reproduces the identical sequence of records below. */
static uint32_t
retention_republish_hash(void)
{
	uint32_t	hash = PS_RETENTION_FNV_INIT;

	for (uint32_t i = 0; i < retention_npins; i++)
	{
		PsRetentionRecord rec;

		retention_make_record(&rec,
						  retention_pin_active(&retention_pins[i]) ?
						  PS_RETENTION_SET : PS_RETENTION_DROP,
						  &retention_pins[i]);
		hash = retention_fnv1a(hash, &rec, sizeof(rec));
	}
	if (retention_admission_highwater != 0)
	{
		PsRetentionPin pin = {0};
		PsRetentionRecord rec;

		pin.admission_seq = retention_admission_highwater;
		retention_make_record(&rec, PS_RETENTION_ADMISSION_RESERVE, &pin);
		hash = retention_fnv1a(hash, &rec, sizeof(rec));
	}
	return hash;
}

static int
retention_republish(void)
{
	char		tmp[4096] = {0};
	int			fd = -1;
	int			rc = -1;
	int			n;
	int			pending = 0;
	int			published = 0;
	uint64_t	old_nrecords = retention_nrecords;
	uint32_t	old_hash = retention_log_hash;
	uint32_t	new_hash = retention_republish_hash();
	uint64_t	new_nrecords = retention_npins +
		(retention_admission_highwater != 0);

	if (retention_is_poisoned)
		goto done;
	if (retention_begin_pending(PS_RETENTION_PENDING_COMPACT, old_nrecords,
								old_hash, new_nrecords, new_hash) != 0)
		goto done;
	pending = 1;
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_COMPACT_AFTER_PENDING) != 0)
		goto done;
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", retention_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		goto done;
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		goto done;
	/* retention_npins/retention_admission_highwater cannot change while
	 * retention_lock is held, so this reproduces exactly the sequence of
	 * records retention_republish_hash() already hashed for begin_pending()
	 * above; new_hash/new_nrecords are not recomputed here. */
	for (uint32_t i = 0; i < retention_npins; i++)
	{
		PsRetentionRecord rec;

		retention_make_record(&rec,
						  retention_pin_active(&retention_pins[i]) ?
						  PS_RETENTION_SET : PS_RETENTION_DROP,
						  &retention_pins[i]);
		if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec))
			goto done;
	}
	if (retention_admission_highwater != 0)
	{
		PsRetentionPin pin = {0};
		PsRetentionRecord rec;

		pin.admission_seq = retention_admission_highwater;
		retention_make_record(&rec, PS_RETENTION_ADMISSION_RESERVE, &pin);
		if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec))
			goto done;
	}
	if (fsync(fd) != 0)
		goto done;
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_COMPACT_AFTER_TMP_SYNC) != 0)
		goto done;
	if (close(fd) != 0)
	{
		fd = -1;
		goto done;
	}
	fd = -1;
	if (rename(tmp, retention_path) != 0)
		goto done;
	published = 1;
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_COMPACT_AFTER_RENAME) != 0)
		goto done;
	if (retention_fsync_dir() != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		goto done;
	}
	{
		struct stat st;

		if (stat(retention_path, &st) != 0)
		{
			retention_is_poisoned = 1;
			retention_mark_failed();
			goto done;
		}
		retention_dev = st.st_dev;
		retention_ino = st.st_ino;
	}
	if (retention_write_state(new_nrecords, new_hash) != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		goto done;
	}
	retention_nrecords = new_nrecords;
	retention_log_hash = new_hash;
	if (ps_fault_probe(PS_FAULT_POINT_RETENTION_COMPACT_AFTER_STATE) != 0)
		goto done;
	if (retention_clear_pending() != 0)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
		goto done;
	}
	pending = 0;
	memset(&retention_compact_retry_at, 0, sizeof(retention_compact_retry_at));
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0 && tmp[0] != '\0')
		unlink(tmp);
	if (rc != 0 && pending && !published)
		/* The rename into place never happened, so retention.meta (if it
		 * even exists) is still exactly the pre-mutation "old" state, and
		 * this function is already about to report failure (rc != 0) to
		 * its own caller: nobody was told the compaction committed.  A
		 * resurrected marker reconciles to a safe no-op rollback either
		 * way, so best effort is enough here too. */
		(void) retention_clear_pending();
	if (rc != 0 && pending && published)
	{
		retention_is_poisoned = 1;
		retention_mark_failed();
	}
	if (rc != 0 && !retention_is_poisoned)
		retention_defer_compact();
	return rc;
}

static int
retention_rewrite_current(void)
{
	return retention_republish();
}

/* Read and validate a durable pending intent.  A file of the wrong size
 * (short, or carrying trailing bytes past the fixed-size record -- fstat
 * catches what an exact-sized read alone cannot), bad magic/version, bad
 * CRC, or an internally inconsistent op is exactly what the pre-intent
 * format's empty guard file (or disk corruption) looks like: reject it the
 * same way that format always failed closed. */
static int
retention_read_pending(PsRetentionPending *out)
{
	int			fd;
	ssize_t		r;
	struct stat	st;

	fd = open(retention_pending_path, O_RDONLY);
	if (fd < 0)
		return -1;
	if (fstat(fd, &st) != 0)
	{
		close(fd);
		return -1;
	}
	r = read(fd, out, sizeof(*out));
	if (close(fd) != 0)
		return -1;
	if (st.st_size != (off_t) sizeof(*out) ||
		r != (ssize_t) sizeof(*out) ||
		out->magic != PS_RETENTION_PENDING_MAGIC ||
		out->version != PS_RETENTION_PENDING_VERSION ||
		retention_pending_crc(out) != out->crc ||
		(out->op != PS_RETENTION_PENDING_APPEND &&
		 out->op != PS_RETENTION_PENDING_COMPACT &&
		 out->op != PS_RETENTION_PENDING_BOOTSTRAP_STATE) ||
		/* An append always grows the log by exactly one record; a
		 * compaction (or the identical v1 -> v2 migration rewrite) rewrites
		 * the whole log down to one record per live pin/tombstone plus an
		 * optional admission record, so new_nrecords is typically *smaller*
		 * than old_nrecords and neither direction is a corruption signal;
		 * a bootstrap never touches the log at all (new_nrecords ==
		 * old_nrecords), so it must be told apart from both. */
		(out->op == PS_RETENTION_PENDING_APPEND &&
		 out->new_nrecords != out->old_nrecords + 1) ||
		(out->op == PS_RETENTION_PENDING_BOOTSTRAP_STATE &&
		 out->new_nrecords != out->old_nrecords))
	{
		errno = EILSEQ;
		return -1;
	}
	return 0;
}

/* Does retention.meta currently carry the legacy v1 header?  Only ever
 * meaningful for a PS_RETENTION_PENDING_COMPACT intent recorded by the v1 ->
 * v2 migration rewrite: a v1 header proves the migration's rename never
 * happened, so the pre-image is untouched and needs no further inspection to
 * roll back to. */
static int
retention_meta_is_legacy_header(int *is_legacy)
{
	int			fd;
	off_t		size;
	uint32_t	header[4];

	*is_legacy = 0;
	fd = open(retention_path, O_RDONLY);
	if (fd < 0)
		return errno == ENOENT ? 0 : -1;
	size = lseek(fd, 0, SEEK_END);
	if (size < 0)
	{
		close(fd);
		return -1;
	}
	if (size >= (off_t) sizeof(header))
	{
		if (pread(fd, header, sizeof(header), 0) != (ssize_t) sizeof(header))
		{
			close(fd);
			return -1;
		}
		*is_legacy = header[0] == PS_RETENTION_MAGIC &&
			header[1] == PS_RETENTION_VERSION_V1 &&
			header[3] == sizeof(PsRetentionRecordV1);
	}
	return close(fd) == 0 ? 0 : -1;
}

/* Validate that the first n on-disk v2 records of retention.meta reproduce
 * hash exactly, and report the file's current total size.  A missing file
 * reads as zero records, matching an interrupted append/compaction that
 * crashed before retention.meta was ever created.  Read-only: never mutates
 * the live registry.  Returns 1 (prefix matches), 0 (it does not), or -1 on
 * an I/O error unrelated to the comparison itself. */
static int
retention_verify_v2_prefix(uint64_t n, uint32_t hash, off_t *size_out)
{
	int			fd;
	off_t		size;
	uint32_t	rolling = PS_RETENTION_FNV_INIT;

	fd = open(retention_path, O_RDONLY);
	if (fd < 0)
	{
		if (errno != ENOENT)
			return -1;
		*size_out = 0;
		return (n == 0 && hash == PS_RETENTION_FNV_INIT) ? 1 : 0;
	}
	size = lseek(fd, 0, SEEK_END);
	if (size < 0)
	{
		close(fd);
		return -1;
	}
	*size_out = size;
	if (size < (off_t) (n * sizeof(PsRetentionRecord)))
	{
		close(fd);
		return 0;
	}
	for (uint64_t i = 0; i < n; i++)
	{
		PsRetentionRecord rec;

		if (pread(fd, &rec, sizeof(rec), (off_t) (i * sizeof(rec))) !=
			(ssize_t) sizeof(rec) || !retention_record_valid(&rec))
		{
			close(fd);
			return 0;
		}
		rolling = retention_fnv1a(rolling, &rec, sizeof(rec));
	}
	if (close(fd) != 0)
		return -1;
	return rolling == hash ? 1 : 0;
}

/* Caller holds retention_lock and has already populated every retention_*
 * path global; retention.pending is known to exist.  Reconciles a durable
 * pending intent left behind by a crash during retention_append(),
 * retention_republish(), or the one-time PS_RETENTION_PENDING_BOOTSTRAP_STATE
 * install in ps_retention_open(): classifies whatever retention.meta actually holds
 * against the intent's recorded pre- and post-mutation (nrecords, hash), and
 * either rolls the mutation back, rolls it forward, or fails closed.  Every
 * step below is safe to redo verbatim after a second crash mid-recovery: it
 * always recomputes the classification from what is currently on disk rather
 * than trusting in-memory progress, so replaying it converges to the same
 * fixed point.  On success, retention.meta and retention.state agree and
 * retention.pending is gone, and the caller's ordinary replay path below
 * proceeds exactly as if nothing had been interrupted. */
static int
retention_recover_pending(void)
{
	PsRetentionPending pending;
	char		tmp_path[4096];
	int			n;
	int			is_legacy = 0;
	int			old_ok = 0;
	int			new_ok = 0;
	off_t		size = 0;

	if (retention_read_pending(&pending) != 0)
	{
		fprintf(stderr,
				"pagestore_retention: %s present: interrupted mutation could "
				"not be reconciled (unreadable or unrecognized intent)\n",
				retention_pending_path);
		errno = EILSEQ;
		return -1;
	}
	n = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", retention_path);
	if (n < 0 || (size_t) n >= sizeof(tmp_path))
		return -1;
	if (unlink(tmp_path) != 0 && errno != ENOENT)
	{
		fprintf(stderr, "pagestore_retention: could not remove %s: %s\n",
				tmp_path, strerror(errno));
		return -1;
	}

	if (pending.op == PS_RETENTION_PENDING_COMPACT)
	{
		if (retention_meta_is_legacy_header(&is_legacy) != 0)
		{
			fprintf(stderr, "pagestore_retention: %s: %s\n", retention_path,
					strerror(errno));
			return -1;
		}
		if (is_legacy)
			old_ok = 1;
		else
		{
			new_ok = retention_verify_v2_prefix(pending.new_nrecords,
												pending.new_hash, &size);
			if (new_ok < 0)
			{
				fprintf(stderr, "pagestore_retention: %s: %s\n",
						retention_path, strerror(errno));
				return -1;
			}
			new_ok = new_ok && size ==
				(off_t) (pending.new_nrecords * sizeof(PsRetentionRecord));
			if (!new_ok)
			{
				old_ok = retention_verify_v2_prefix(pending.old_nrecords,
													pending.old_hash, &size);
				if (old_ok < 0)
				{
					fprintf(stderr, "pagestore_retention: %s: %s\n",
							retention_path, strerror(errno));
					return -1;
				}
				old_ok = old_ok && size == (off_t) (pending.old_nrecords *
													 sizeof(PsRetentionRecord));
			}
		}
	}
	else if (pending.op == PS_RETENTION_PENDING_BOOTSTRAP_STATE)
	{
		off_t		min_size = (off_t) (pending.old_nrecords *
										sizeof(PsRetentionRecord));
		off_t		max_size = min_size + (off_t) sizeof(PsRetentionRecord);
		int			prefix_ok;

		/* Bootstrap never touches the log (new_nrecords == old_nrecords): it
		 * only installs retention.state for the first time over an
		 * unchanged log.  There is no separate "old" to roll back to --
		 * either the log's valid prefix on disk still matches the exact
		 * (nrecords, hash) the intent recorded, in which case the
		 * roll-forward branch below finishes installing the state (after
		 * durably discarding any torn tail past that prefix -- the log
		 * this bootstrap is installing state for predates crash-safe
		 * mutation and can carry the same kind of harmless incomplete
		 * final record ps_retention_open()'s ordinary committed-prefix
		 * replay has always discarded; see the "short final record is
		 * recoverable" contract) -- or it does not, in which case
		 * something changed the log out from under an in-flight bootstrap
		 * and recovery fails closed rather than guess.  A *complete*
		 * trailing record is not given this pass: it is not part of what
		 * this intent recorded, and only an APPEND intent's own in-flight
		 * record gets that benefit of the doubt. */
		prefix_ok = retention_verify_v2_prefix(pending.old_nrecords,
											   pending.old_hash, &size);
		if (prefix_ok < 0)
		{
			fprintf(stderr, "pagestore_retention: %s: %s\n", retention_path,
					strerror(errno));
			return -1;
		}
		new_ok = prefix_ok && size >= min_size && size < max_size;
	}
	else
	{
		off_t		min_size = (off_t) (pending.old_nrecords *
										sizeof(PsRetentionRecord));
		off_t		max_size = min_size + (off_t) sizeof(PsRetentionRecord);
		int			prefix_ok;

		/* An in-place append never rolls forward: whatever retention_append()
		 * returns to its caller, nobody outside this process could have been
		 * told the mutation succeeded before the crash, so dropping a
		 * complete-but-unacknowledged trailing record is always safe and
		 * strictly simpler than trying to prove it was never observed. */
		prefix_ok = retention_verify_v2_prefix(pending.old_nrecords,
											   pending.old_hash, &size);
		if (prefix_ok < 0)
		{
			fprintf(stderr, "pagestore_retention: %s: %s\n", retention_path,
					strerror(errno));
			return -1;
		}
		old_ok = prefix_ok && size >= min_size && size <= max_size;
	}

	if (new_ok)
	{
		int			skip_state = 0;

		if (pending.op == PS_RETENTION_PENDING_COMPACT &&
			pending.old_nrecords == 0 && pending.new_nrecords == 0)
		{
			struct stat mst;

			if (stat(retention_path, &mst) != 0)
			{
				if (errno != ENOENT)
				{
					fprintf(stderr, "pagestore_retention: %s: %s\n",
							retention_path, strerror(errno));
					return -1;
				}
				/* Compacting an already-empty registry down to zero records
				 * is a no-op that retention_republish() still tries to
				 * publish (an empty tmp file renamed into place); a crash
				 * after the intent but before that rename can leave
				 * retention.meta genuinely absent, exactly as if the store
				 * had never been mutated at all (retention_verify_v2_prefix
				 * reports a missing file as a trivial match for zero
				 * records, which is what routed this case here).  Writing
				 * retention.state now would leave a store with state but no
				 * meta, which the ordinary open path below permanently
				 * refuses.  Treat it exactly like a store that was never
				 * durably created: no state, no meta, just clear the
				 * pending marker below. */
				skip_state = 1;
			}
		}
		else if (pending.op == PS_RETENTION_PENDING_BOOTSTRAP_STATE &&
				 size != (off_t) (pending.old_nrecords *
								  sizeof(PsRetentionRecord)))
		{
			/* A torn tail matched the classification above (old_nrecords
			 * records plus a partial final one); durably discard it before
			 * installing state, exactly like the APPEND op's own trailing-
			 * record truncation below, and exactly what the non-crashed
			 * bootstrap path in ps_retention_open() already does.  Every
			 * future open's committed-prefix check requires
			 * retention.meta's size to match nrecords precisely, so the
			 * torn bytes cannot survive past this point. */
			int			fd = open(retention_path, O_WRONLY);
			int			ok = fd >= 0 &&
				ftruncate(fd, (off_t) (pending.old_nrecords *
										sizeof(PsRetentionRecord))) == 0 &&
				fsync(fd) == 0;

			if (fd >= 0 && close(fd) != 0)
				ok = 0;
			if (!ok || retention_fsync_dir() != 0)
			{
				fprintf(stderr,
						"pagestore_retention: could not truncate %s back to "
						"%llu record(s) while reconciling %s\n",
						retention_path,
						(unsigned long long) pending.old_nrecords,
						retention_pending_path);
				return -1;
			}
		}
		if (!skip_state &&
			(retention_fsync_dir() != 0 ||
			 retention_write_state(pending.new_nrecords, pending.new_hash) != 0))
		{
			fprintf(stderr,
					"pagestore_retention: could not roll %s forward to the "
					"post-compaction state (nrecords=%llu) while "
					"reconciling %s\n", retention_path,
					(unsigned long long) pending.new_nrecords,
					retention_pending_path);
			return -1;
		}
	}
	else if (old_ok)
	{
		struct stat mst;
		int			meta_exists = stat(retention_path, &mst) == 0;

		if (!meta_exists && errno != ENOENT)
		{
			fprintf(stderr, "pagestore_retention: %s: %s\n", retention_path,
					strerror(errno));
			return -1;
		}
		if (pending.op == PS_RETENTION_PENDING_APPEND &&
			pending.old_nrecords == 0 && !meta_exists)
		{
			/* Nothing was ever durably created for this store (the crash
			 * landed before its very first record's file was made): leave
			 * it exactly as a never-initialized registry so the ordinary
			 * open path below takes the fresh-store branch instead of
			 * installing a state file with no log to match it. */
		}
		else
		{
			if (pending.op == PS_RETENTION_PENDING_APPEND &&
				size != (off_t) (pending.old_nrecords *
								 sizeof(PsRetentionRecord)))
			{
				int			fd = open(retention_path, O_WRONLY);
				int			ok = fd >= 0 &&
					ftruncate(fd, (off_t) (pending.old_nrecords *
											sizeof(PsRetentionRecord))) == 0 &&
					fsync(fd) == 0;

				if (fd >= 0 && close(fd) != 0)
					ok = 0;
				if (!ok || retention_fsync_dir() != 0)
				{
					fprintf(stderr,
							"pagestore_retention: could not truncate %s back "
							"to %llu record(s) while reconciling %s\n",
							retention_path,
							(unsigned long long) pending.old_nrecords,
							retention_pending_path);
					return -1;
				}
			}
			if (!is_legacy &&
				retention_write_state(pending.old_nrecords,
									  pending.old_hash) != 0)
			{
				fprintf(stderr,
						"pagestore_retention: could not roll %s back to the "
						"pre-mutation state (nrecords=%llu) while "
						"reconciling %s\n", retention_path,
						(unsigned long long) pending.old_nrecords,
						retention_pending_path);
				return -1;
			}
		}
	}
	else
	{
		fprintf(stderr,
				"pagestore_retention: %s present but %s (size=%lld bytes) "
				"matches neither the recorded pre-mutation (nrecords=%llu) "
				"nor post-mutation (nrecords=%llu) state: interrupted %s "
				"could not be reconciled\n", retention_pending_path,
				retention_path, (long long) size,
				(unsigned long long) pending.old_nrecords,
				(unsigned long long) pending.new_nrecords,
				pending.op == PS_RETENTION_PENDING_APPEND ? "append" :
				pending.op == PS_RETENTION_PENDING_BOOTSTRAP_STATE ?
				"state bootstrap" : "compaction");
		errno = EILSEQ;
		return -1;
	}
	if (retention_clear_pending() != 0)
	{
		fprintf(stderr,
				"pagestore_retention: reconciled %s but could not remove "
				"%s: %s\n", retention_path, retention_pending_path,
				strerror(errno));
		return -1;
	}
	return 0;
}

int
ps_retention_open(const char *store_dir)
{
	int			fd = -1;
	int			rc = -1;
	int			n;
	int			state_rc;
	struct stat st;
	PsRetentionState committed;
	off_t		off = 0;
	int			legacy_format = 0;

	pthread_mutex_lock(&retention_lock);
	free(retention_pins);
	retention_pins = NULL;
	retention_npins = 0;
	retention_cap = 0;
	retention_nrecords = 0;
	retention_mutation_epoch = 0;
	retention_admission_highwater = 0;
	retention_log_hash = PS_RETENTION_FNV_INIT;
	retention_is_poisoned = 0;
	retention_dev = 0;
	retention_ino = 0;
	test_fail_append_after_write = 0;
	test_fail_rollback = 0;
	test_fail_clear_pending_dir_fsync = 0;
	n = snprintf(retention_dir, sizeof(retention_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_dir))
	{
		fprintf(stderr, "pagestore_retention: store path too long: %s\n",
				store_dir);
		errno = ENAMETOOLONG;
		goto done;
	}
	n = snprintf(retention_path, sizeof(retention_path), "%s/retention.meta",
				 store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_path))
	{
		errno = ENAMETOOLONG;
		goto done;
	}
	n = snprintf(retention_marker_path, sizeof(retention_marker_path),
				 "%s/retention.initialized", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_marker_path))
	{
		errno = ENAMETOOLONG;
		goto done;
	}
	n = snprintf(retention_pending_path, sizeof(retention_pending_path),
				 "%s/retention.pending", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_pending_path))
	{
		errno = ENAMETOOLONG;
		goto done;
	}
	n = snprintf(retention_pending_tmp_path, sizeof(retention_pending_tmp_path),
				 "%s/retention.pending.tmp", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_pending_tmp_path))
	{
		errno = ENAMETOOLONG;
		goto done;
	}
	n = snprintf(retention_state_path, sizeof(retention_state_path),
				 "%s/retention.state", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_state_path))
	{
		errno = ENAMETOOLONG;
		goto done;
	}
	n = snprintf(retention_failed_path, sizeof(retention_failed_path),
				 "%s/retention.failed", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_failed_path))
	{
		errno = ENAMETOOLONG;
		goto done;
	}
	/* This durable marker -- written both by the immediately preceding
	 * format after an uncertain rollback, and by this format whenever a
	 * *live* process could not prove a mutation committed -- is never
	 * migrated or replayed past.  It is checked before retention.pending so
	 * that a live-process failure can never be silently reconciled away by
	 * the automatic recovery below just because the crash-only guard also
	 * happens to still be present. */
	if (access(retention_failed_path, F_OK) == 0)
	{
		fprintf(stderr,
				"pagestore_retention: %s present: a prior mutation could not "
				"be proven safe and startup is permanently refused\n",
				retention_failed_path);
		errno = EILSEQ;
		goto done;
	}
	if (errno != ENOENT)
	{
		fprintf(stderr, "pagestore_retention: %s: %s\n", retention_failed_path,
				strerror(errno));
		goto done;
	}
	/* retention_begin_pending() publishes retention.pending by renaming a
	 * private retention.pending.tmp into place.  A crash during that
	 * publish can leave the tmp file behind; no log byte, nor
	 * retention.pending itself, can have changed while only the tmp
	 * existed, so it is always safe to remove and is never itself a
	 * reason to refuse to open. */
	if (access(retention_pending_tmp_path, F_OK) == 0)
	{
		fprintf(stderr,
				"pagestore_retention: removing incomplete %s left behind by "
				"an interrupted pending-marker publish\n",
				retention_pending_tmp_path);
		if (unlink(retention_pending_tmp_path) != 0 ||
			retention_fsync_dir() != 0)
		{
			fprintf(stderr, "pagestore_retention: could not remove %s: %s\n",
					retention_pending_tmp_path, strerror(errno));
			goto done;
		}
	}
	else if (errno != ENOENT)
	{
		fprintf(stderr, "pagestore_retention: %s: %s\n",
				retention_pending_tmp_path, strerror(errno));
		goto done;
	}
	if (access(retention_pending_path, F_OK) == 0)
	{
		struct stat pending_st;

		if (stat(retention_pending_path, &pending_st) != 0)
		{
			fprintf(stderr, "pagestore_retention: %s: %s\n",
					retention_pending_path, strerror(errno));
			goto done;
		}
		if (pending_st.st_size < (off_t) sizeof(PsRetentionPending))
		{
			/* Strictly shorter than one complete record: this is exactly
			 * the shape of the *previous* format's retention.pending, an
			 * intentionally empty guard created and fsynced before every
			 * mutation and removed only after the mutation (meta and, for
			 * a DROP, state) was fully durable.  Unlike this format's own
			 * intent record, that guard's presence does not say *where* in
			 * the mutation a crash landed: a crash could have landed
			 * before the mutation ever touched the log (safe to ignore),
			 * but it could equally have landed after retention.meta and
			 * retention.state were both made durable and consistent with
			 * each other, immediately before the guard's own removal --
			 * in which case the mutation (e.g. a DROP) was never
			 * acknowledged to its caller, and silently letting it stand
			 * because meta/state happen to agree could reclaim
			 * still-needed history.  Ordinary meta/state validation
			 * cannot tell these two cases apart, because in both cases
			 * meta and state are internally consistent.  There is
			 * therefore no safe automatic recovery: fail closed, exactly
			 * as the previous format always did, and require a human to
			 * inspect retention.meta/retention.state and confirm no
			 * unacknowledged mutation is present before removing the
			 * marker by hand.  (The atomic tmp+rename publish this format
			 * uses -- see retention_begin_pending() -- means retention.pending
			 * itself can no longer end up short this way; a torn write is
			 * now confined to retention.pending.tmp, which is never the
			 * formal marker and is always safely removed above.) */
			fprintf(stderr,
					"pagestore_retention: %s is %lld bytes: an interrupted "
					"mutation from an earlier version (or a torn marker) "
					"cannot be reconciled automatically; inspect "
					"retention.meta/retention.state and remove the marker "
					"manually only after confirming no unacknowledged "
					"mutation is present\n",
					retention_pending_path, (long long) pending_st.st_size);
			errno = EILSEQ;
			goto done;
		}
		else if (retention_recover_pending() != 0)
			goto done;			/* diagnostic already printed */
	}
	else if (errno != ENOENT)
	{
		fprintf(stderr, "pagestore_retention: %s: %s\n",
				retention_pending_path, strerror(errno));
		goto done;
	}
	state_rc = retention_read_state(&committed);
	if (state_rc < 0)
	{
		fprintf(stderr, "pagestore_retention: %s: %s\n", retention_state_path,
				strerror(errno));
		goto done;
	}
	{
		const char *fail_append = getenv("PS_TEST_FAIL_RETENTION_APPEND_AFTER_WRITE");
		const char *fail_rollback = getenv("PS_TEST_FAIL_RETENTION_ROLLBACK");
		const char *fail_clear_dir_fsync =
			getenv("PS_TEST_FAIL_RETENTION_CLEAR_PENDING_DIR_FSYNC");

		test_fail_append_after_write = fail_append ? atoi(fail_append) : 0;
		test_fail_rollback = fail_rollback ? atoi(fail_rollback) : 0;
		test_fail_clear_pending_dir_fsync =
			fail_clear_dir_fsync ? atoi(fail_clear_dir_fsync) : 0;
	}
	fd = open(retention_path, O_RDWR);
	if (fd < 0)
	{
		if (errno == ENOENT && access(retention_marker_path, F_OK) != 0 &&
			errno == ENOENT && state_rc == 0)
			rc = 0;
		else
			fprintf(stderr, "pagestore_retention: %s: %s\n", retention_path,
					strerror(errno));
		goto done;
	}
	if (fstat(fd, &st) != 0)
	{
		fprintf(stderr, "pagestore_retention: fstat %s: %s\n", retention_path,
				strerror(errno));
		goto done;
	}
	retention_dev = st.st_dev;
	retention_ino = st.st_ino;
	if (st.st_size >= (off_t) (4 * sizeof(uint32_t)))
	{
		uint32_t header[4];

		if (pread(fd, header, sizeof(header), 0) != (ssize_t) sizeof(header) ||
			header[0] != PS_RETENTION_MAGIC)
		{
			fprintf(stderr,
					"pagestore_retention: %s: bad header magic\n",
					retention_path);
			errno = EILSEQ;
			goto done;
		}
		if (header[1] == PS_RETENTION_VERSION_V1 &&
			header[3] == sizeof(PsRetentionRecordV1))
			legacy_format = 1;
		else if (header[1] != PS_RETENTION_VERSION ||
				 header[3] != sizeof(PsRetentionRecord))
		{
			fprintf(stderr,
					"pagestore_retention: %s: unrecognized version %u "
					"record size %u\n", retention_path,
					(unsigned) header[1], (unsigned) header[3]);
			errno = EILSEQ;
			goto done;
		}
	}
	else if (state_rc > 0)
		legacy_format = committed.version == PS_RETENTION_VERSION_V1;
	if (state_rc > 0 &&
		(committed.version == PS_RETENTION_VERSION_V1) != legacy_format)
	{
		fprintf(stderr,
				"pagestore_retention: %s legacy-format flag disagrees with "
				"%s\n", retention_state_path, retention_path);
		errno = EILSEQ;
		goto done;
	}
	while (off + (off_t) (legacy_format ? sizeof(PsRetentionRecordV1) :
									  sizeof(PsRetentionRecord)) <= st.st_size)
	{
		PsRetentionRecord rec;

		if (legacy_format)
		{
			PsRetentionRecordV1 old;

			if (pread(fd, &old, sizeof(old), off) != (ssize_t) sizeof(old) ||
				retention_record_v1_convert(&old, &rec) != 0)
			{
				fprintf(stderr,
						"pagestore_retention: %s: invalid legacy record at "
						"offset %lld\n", retention_path, (long long) off);
				errno = EILSEQ;
				goto done;
			}
			retention_log_hash = retention_fnv1a(retention_log_hash,
											  &old, sizeof(old));
			off += sizeof(old);
		}
		else if (pread(fd, &rec, sizeof(rec), off) != (ssize_t) sizeof(rec) ||
				 !retention_record_valid(&rec))
		{
			fprintf(stderr,
					"pagestore_retention: %s: invalid record at offset "
					"%lld\n", retention_path, (long long) off);
			errno = EILSEQ;
			goto done;			/* a full corrupt record is never discarded */
		}
		if (retention_apply(&rec) != 0)
		{
			fprintf(stderr,
					"pagestore_retention: %s: record at offset %lld could "
					"not be applied\n", retention_path, (long long) off);
			goto done;
		}
		if (!legacy_format)
		{
			retention_log_hash = retention_fnv1a(retention_log_hash,
											  &rec, sizeof(rec));
			off += sizeof(rec);
		}
		retention_nrecords++;
	}
	if (state_rc > 0)
	{
		if (committed.nrecords != retention_nrecords ||
			committed.log_hash != retention_log_hash)
		{
			fprintf(stderr,
					"pagestore_retention: state nrecords %llu != log %llu "
					"(or hash mismatch)\n",
					(unsigned long long) committed.nrecords,
					(unsigned long long) retention_nrecords);
			errno = EILSEQ;
			goto done;
		}
		/* The committed-prefix state proves that a shorter final fragment is
		 * outside the acknowledged log and can be discarded safely. */
		if (off != st.st_size &&
			(ftruncate(fd, off) != 0 || fsync(fd) != 0))
		{
			fprintf(stderr,
					"pagestore_retention: could not truncate %s to its "
					"committed prefix: %s\n", retention_path,
					strerror(errno));
			goto done;
		}
	}
	else if (!legacy_format)
	{
		/* One-time migration from the pre-committed-prefix format: no log
		 * content changes (old == new), only retention.state is installed
		 * for the first time.  This is PS_RETENTION_PENDING_BOOTSTRAP_STATE,
		 * not an APPEND: an APPEND intent always requires new_nrecords ==
		 * old_nrecords + 1 (see retention_read_pending()), so recording this
		 * old-equals-new mutation as an APPEND would make an interrupted
		 * bootstrap permanently unrecoverable -- the very next open would
		 * read back its own pending marker and reject it as corrupt. */
		if (retention_begin_pending(PS_RETENTION_PENDING_BOOTSTRAP_STATE,
									retention_nrecords, retention_log_hash,
									retention_nrecords,
									retention_log_hash) != 0)
		{
			fprintf(stderr,
					"pagestore_retention: could not install %s while "
					"installing the initial %s: %s\n",
					retention_pending_path, retention_state_path,
					strerror(errno));
			goto done;
		}
		if (ps_fault_probe(PS_FAULT_POINT_RETENTION_BOOTSTRAP_AFTER_PENDING) != 0)
			goto done;
		if ((off != st.st_size &&
			 (ftruncate(fd, off) != 0 || fsync(fd) != 0)) ||
			retention_write_state(retention_nrecords, retention_log_hash) != 0 ||
			retention_clear_pending() != 0)
		{
			fprintf(stderr,
					"pagestore_retention: could not install the initial %s: "
					"%s\n", retention_state_path, strerror(errno));
			goto done;
		}
	}
	/* Rewrite all live pins and tombstones only after validating the complete
	 * committed v1 prefix.  The rewrite also installs the v2 state atomically. */
	if (legacy_format && retention_rewrite_current() != 0)
	{
		fprintf(stderr,
				"pagestore_retention: could not migrate %s to the current "
				"format: %s\n", retention_path, strerror(errno));
		goto done;
	}
	if (retention_mark_initialized() != 0)
	{
		fprintf(stderr, "pagestore_retention: could not install %s: %s\n",
				retention_marker_path, strerror(errno));
		goto done;
	}
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0)
	{
		free(retention_pins);
		retention_pins = NULL;
		retention_npins = 0;
		retention_cap = 0;
		retention_is_poisoned = 1;
	}
	else if (retention_new_incarnation(&retention_mutation_epoch) != 0)
	{
		retention_is_poisoned = 1;
		rc = -1;
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

void
ps_retention_close(void)
{
	pthread_mutex_lock(&retention_lock);
	free(retention_pins);
	retention_pins = NULL;
	retention_npins = 0;
	retention_cap = 0;
	retention_nrecords = 0;
	retention_mutation_epoch = 0;
	retention_admission_highwater = 0;
	retention_log_hash = PS_RETENTION_FNV_INIT;
	retention_path[0] = '\0';
	retention_marker_path[0] = '\0';
	retention_pending_path[0] = '\0';
	retention_pending_tmp_path[0] = '\0';
	retention_state_path[0] = '\0';
	retention_dir[0] = '\0';
	retention_dev = 0;
	retention_ino = 0;
	pthread_mutex_unlock(&retention_lock);
}

/* Caller holds retention_lock and has validated pin. */
static int
retention_set_locked(const PsRetentionPin *pin)
{
	PsRetentionRecord rec;
	int			idx;
	int			rc = -1;

	if (retention_is_poisoned)
		return -1;
	idx = retention_find(pin->timeline, pin->owner_kind, pin->owner_id);
	if (idx >= 0 && pin->generation < retention_pins[idx].generation)
	{
		rc = PS_RETENTION_STALE;
		return rc;
	}
	if (idx >= 0 && pin->generation == retention_pins[idx].generation &&
		pin->generation != 0 &&
		!retention_pin_active(&retention_pins[idx]))
	{
		rc = PS_RETENTION_STALE;
		return rc;
	}
	if (idx >= 0 && memcmp(&retention_pins[idx], pin, sizeof(*pin)) == 0)
	{
		if (retention_log_matches())
			rc = PS_RETENTION_OK;	/* exact retry: no log churn */
		else
			retention_is_poisoned = 1;
	}
	else
	{
		if (idx < 0 && retention_reserve(retention_npins + 1) != 0)
			return -1;
		retention_make_record(&rec, PS_RETENTION_SET, pin);
		if (retention_append(&rec) != 0)
			return -1;
		if (idx >= 0)
			retention_pins[idx] = *pin;
		else
			retention_pins[retention_npins++] = *pin;
		retention_mutation_epoch++;
		rc = PS_RETENTION_OK;
	}
	return rc;
}

int
ps_retention_set(const PsRetentionPin *pin)
{
	int			rc;

	if (!retention_pin_valid(pin) || pin->admission_seq == 0)
		return -1;
	pthread_mutex_lock(&retention_lock);
	rc = retention_set_locked(pin);
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_reserve_and_set(const PsRetentionPin *pin)
{
	PsRetentionPin reservation = {0};
	PsRetentionRecord rec;
	int			rc = -1;

	if (!retention_pin_valid(pin) || pin->admission_seq == 0)
		return -1;
	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned ||
		pin->admission_seq <= retention_admission_highwater)
		goto done;
	reservation.admission_seq = pin->admission_seq;
	retention_make_record(&rec, PS_RETENTION_ADMISSION_RESERVE, &reservation);
	if (retention_append(&rec) != 0)
		goto done;
	retention_admission_highwater = pin->admission_seq;
	/* Snapshot/floor readers cannot observe the reservation without its pin. */
	rc = retention_set_locked(pin);
done:
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_reserve_admission_seq(uint64_t admission_seq)
{
	PsRetentionPin pin = {0};
	PsRetentionRecord rec;
	int			rc = -1;

	if (admission_seq == 0 || admission_seq >= UINT64_MAX - 1)
		return -1;
	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		goto done;
	if (admission_seq <= retention_admission_highwater)
	{
		rc = retention_log_matches() ? 0 : -1;
		if (rc != 0)
			retention_is_poisoned = 1;
		goto done;
	}
	pin.admission_seq = admission_seq;
	retention_make_record(&rec, PS_RETENTION_ADMISSION_RESERVE, &pin);
	if (retention_append(&rec) != 0)
		goto done;
	retention_admission_highwater = admission_seq;
	rc = 0;
done:
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_admission_highwater(uint64_t *admission_seq_out)
{
	int			rc = -1;

	if (admission_seq_out == NULL)
		return -1;
	pthread_mutex_lock(&retention_lock);
	if (!retention_is_poisoned)
	{
		*admission_seq_out = retention_admission_highwater;
		rc = 0;
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_drop(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id,
				  uint32_t generation)
{
	PsRetentionPin pin;
	PsRetentionRecord rec;
	int			idx;
	int			rc = -1;

	if (!retention_kind_valid(owner_kind) || owner_id == 0)
		return -1;
	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		goto done;
	idx = retention_find(timeline, owner_kind, owner_id);
	if (idx >= 0 && generation < retention_pins[idx].generation)
	{
		rc = PS_RETENTION_STALE;
		goto done;
	}
	if (idx >= 0 && generation == retention_pins[idx].generation &&
		!retention_pin_active(&retention_pins[idx]))
	{
		if (retention_log_matches())
			rc = PS_RETENTION_OK;	/* exact retry: no log churn */
		else
			retention_is_poisoned = 1;
		goto done;
	}
	memset(&pin, 0, sizeof(pin));
	pin.timeline = timeline;
	pin.owner_kind = owner_kind;
	pin.owner_id = owner_id;
	pin.generation = generation;
	if (idx < 0 && retention_reserve(retention_npins + 1) != 0)
		goto done;
	retention_make_record(&rec, PS_RETENTION_DROP, &pin);
	if (retention_append(&rec) != 0)
		goto done;
	if (idx >= 0)
		retention_pins[idx] = pin;
	else
		retention_pins[retention_npins++] = pin;
	retention_mutation_epoch++;
	rc = PS_RETENTION_OK;
done:
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_count(uint32_t *count_out)
{
	int			rc = 0;

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		rc = -1;
	else if (count_out)
		*count_out = retention_active_count();
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_diagnostic_snapshot(PsRetentionDiagnostic *out)
{
	if (out == NULL)
		return -1;
	pthread_mutex_lock(&retention_lock);
	retention_diagnostic_fill_locked(out);
	pthread_mutex_unlock(&retention_lock);
	return 0;
}

int
ps_retention_get(uint32_t index, PsRetentionPin *pin_out, uint32_t *count_out)
{
	int			rc = 0;

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		rc = -1;
	else
	{
		uint32_t	active = retention_active_count();
		uint32_t	seen = 0;

		if (count_out)
			*count_out = active;
		for (uint32_t i = 0; i < retention_npins; i++)
		{
			if (!retention_pin_active(&retention_pins[i]))
				continue;
			if (seen++ == index)
			{
				if (pin_out)
					*pin_out = retention_pins[i];
				rc = 1;
				break;
			}
		}
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_get_consistent(uint32_t index, uint64_t *epoch_io,
							PsRetentionPin *pin_out, uint32_t *count_out)
{
	int			rc = 0;

	if (epoch_io == NULL)
		return PS_RETENTION_ERROR;
	/* Keep epoch validation and indexed lookup under the same mutex. */
	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		rc = PS_RETENTION_ERROR;
	else if (*epoch_io != 0 && *epoch_io != retention_mutation_epoch)
	{
		*epoch_io = 0;
		rc = PS_RETENTION_STALE;
	}
	else
	{
		uint32_t	active = retention_active_count();
		uint32_t	seen = 0;

		*epoch_io = retention_mutation_epoch;
		if (count_out)
			*count_out = active;
		for (uint32_t i = 0; i < retention_npins; i++)
		{
			if (!retention_pin_active(&retention_pins[i]))
				continue;
			if (seen++ == index)
			{
				if (pin_out)
					*pin_out = retention_pins[i];
				rc = 1;
				break;
			}
		}
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_lookup(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id,
					PsRetentionPin *pin_out)
{
	int			rc = 0;

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		rc = -1;
	else
	{
		int			idx = retention_find(timeline, owner_kind, owner_id);

		if (idx >= 0 && retention_pin_active(&retention_pins[idx]))
		{
			if (pin_out)
				*pin_out = retention_pins[idx];
			rc = 1;
		}
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

/* Any active page-history pin on the timeline at exactly this LSN, whatever
 * its admission sequence. */
int
ps_retention_page_fence_at(uint32_t timeline, uint64_t lsn)
{
	int			rc = 0;

	pthread_mutex_lock(&retention_lock);
	if (!retention_is_poisoned)
	{
		for (uint32_t i = 0; i < retention_npins; i++)
		{
			PsRetentionPin *pin = &retention_pins[i];

			if (retention_pin_active(pin) && pin->timeline == timeline &&
				(pin->resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0 &&
				pin->lsn == lsn)
			{
				rc = 1;
				break;
			}
		}
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_page_fence_active(uint32_t timeline, uint64_t lsn,
							  uint64_t admission_seq)
{
	int			rc = 0;

	pthread_mutex_lock(&retention_lock);
	if (!retention_is_poisoned)
	{
		for (uint32_t i = 0; i < retention_npins; i++)
		{
			PsRetentionPin *pin = &retention_pins[i];

			if (retention_pin_active(pin) && pin->timeline == timeline &&
				(pin->resources & PS_RETENTION_RESOURCE_PAGE_HISTORY) != 0 &&
				pin->lsn == lsn && pin->admission_seq == admission_seq)
			{
				rc = 1;
				break;
			}
		}
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_snapshot(PsRetentionPin *pins, uint32_t capacity,
					  uint32_t *count_out)
{
	int rc = 0;

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned || capacity < retention_active_count())
		rc = -1;
	else
	{
		uint32_t	count = 0;

		for (uint32_t i = 0; i < retention_npins; i++)
			if (retention_pin_active(&retention_pins[i]))
				pins[count++] = retention_pins[i];
		*count_out = count;
	}
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_snapshot_alloc(PsRetentionPin **pins_out, uint32_t *count_out)
{
	PsRetentionPin *snapshot = NULL;
	int			rc = -1;

	if (!pins_out || !count_out)
		return -1;
	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		goto done;
	if (retention_active_count() > 0)
	{
		uint32_t	count = retention_active_count();

		snapshot = malloc((size_t) count * sizeof(*snapshot));
		if (!snapshot)
			goto done;
		count = 0;
		for (uint32_t i = 0; i < retention_npins; i++)
			if (retention_pin_active(&retention_pins[i]))
				snapshot[count++] = retention_pins[i];
		*count_out = count;
	}
	else
		*count_out = 0;
	*pins_out = snapshot;
	rc = 0;
done:
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_snapshot_alloc_with_diagnostic(PsRetentionPin **pins_out,
											 uint32_t *count_out,
											 PsRetentionDiagnostic *diagnostic_out,
											 uint64_t *epoch_out)
{
	PsRetentionPin *snapshot = NULL;
	uint32_t	count = 0;
	int			rc = -1;

	if (pins_out == NULL || count_out == NULL || diagnostic_out == NULL ||
		epoch_out == NULL)
		return -1;
	*pins_out = NULL;
	*count_out = 0;
	*epoch_out = 0;
	memset(diagnostic_out, 0, sizeof(*diagnostic_out));

	pthread_mutex_lock(&retention_lock);
	*epoch_out = retention_mutation_epoch;
	retention_diagnostic_fill_locked(diagnostic_out);
	if (retention_is_poisoned)
	{
		/* The poison bit is the useful result; no caller may consume a partial
		 * pin array as if it were a healthy snapshot. */
		rc = 0;
		goto done;
	}
	count = retention_active_count();
	if (count != 0)
	{
#ifdef PAGESTORE_RETENTION_TEST
		if (retention_test_fail_snapshot_alloc)
			goto done;
#endif
		snapshot = malloc((size_t) count * sizeof(*snapshot));
		if (snapshot == NULL)
			goto done;
		count = 0;
		for (uint32_t i = 0; i < retention_npins; i++)
			if (retention_pin_active(&retention_pins[i]))
				snapshot[count++] = retention_pins[i];
	}
	*pins_out = snapshot;
	*count_out = count;
	rc = 0;
done:
	if (rc != 0)
		free(snapshot);
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_should_compact(void)
{
	int			should;

	pthread_mutex_lock(&retention_lock);
	should = !retention_is_poisoned && retention_nrecords >= 64 &&
		retention_nrecords > 4 * ((uint64_t) retention_npins + 2) &&
		retention_compact_retry_ready();
	pthread_mutex_unlock(&retention_lock);
	return should;
}

int
ps_retention_compact(void)
{
	int			rc;

	pthread_mutex_lock(&retention_lock);
	rc = retention_republish();
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_generation_stale(uint32_t timeline, uint32_t owner_kind,
							  uint64_t owner_id, uint32_t generation)
{
	int			rc = 0;
	int			idx;

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		rc = -1;
	else if ((idx = retention_find(timeline, owner_kind, owner_id)) >= 0 &&
			 ((generation < retention_pins[idx].generation) ||
			  (generation == retention_pins[idx].generation &&
			   generation != 0 &&
			   !retention_pin_active(&retention_pins[idx]))))
		rc = 1;
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

size_t
ps_retention_format_identities(const PsFormatIdentity **out)
{
	static const PsFormatIdentity identities[] = {
		{"retention", "retention.state", PS_RETENTION_STATE_MAGIC, PS_RETENTION_VERSION},
		{"retention", "retention.meta record", PS_RETENTION_MAGIC, PS_RETENTION_VERSION},
	};

	*out = identities;
	return sizeof(identities) / sizeof(identities[0]);
}
