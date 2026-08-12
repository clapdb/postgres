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
 * A durable pending marker is installed before every mutation.  It is removed
 * only after the log and its independently checksummed committed-prefix state
 * are durable, so an uncertain append or rewrite fails closed across restart.
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

#include "pagestore_retention.h"

#define PS_RETENTION_MAGIC		0x4e544552	/* "RETN" */
#define PS_RETENTION_VERSION	1
#define PS_RETENTION_FNV_INIT	2166136261u
#define PS_RETENTION_STATE_MAGIC 0x53544552	/* "RETS" */

typedef enum PsRetentionRecordType
{
	PS_RETENTION_SET = 1,
	PS_RETENTION_DROP = 2,
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
static char retention_state_path[4096];
static char retention_dir[2048];
static PsRetentionPin *retention_pins;
static uint32_t retention_npins;
static uint32_t retention_cap;
static uint64_t retention_nrecords;
static uint32_t retention_log_hash;
static int retention_is_poisoned;
static dev_t retention_dev;
static ino_t retention_ino;
static struct timespec retention_compact_retry_at;
static pthread_mutex_t retention_lock = PTHREAD_MUTEX_INITIALIZER;

/* Standalone-test fault injection; ordinary deployments leave these zero. */
static int test_fail_append_after_write;
static int test_fail_rollback;

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
		pin->lsn != 0;
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
	return 0;
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
	int			idx = retention_find(rec->pin.timeline,
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

/* Caller holds retention_lock.  No log byte may change until this guard and
 * its directory entry are durable. */
static int
retention_begin_pending(void)
{
	int fd;
	int rc = 0;

	fd = open(retention_pending_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
		return -1;
	if (fsync(fd) != 0)
		rc = -1;
	if (close(fd) != 0)
		rc = -1;
	if (rc == 0 && retention_fsync_dir() != 0)
		rc = -1;
	if (rc != 0)
	{
		unlink(retention_pending_path);
		(void) retention_fsync_dir();
	}
	return rc;
}

/* Once unlink succeeds, either the guard removal reaches disk or recovery
 * sees the still-durable guard and fails closed.  Directory fsync is still
 * attempted, but its failure cannot make the committed state unsafe. */
static int
retention_clear_pending(void)
{
	if (unlink(retention_pending_path) != 0)
		return -1;
	(void) retention_fsync_dir();
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
		state->version != PS_RETENTION_VERSION ||
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
		retention_is_poisoned = 1;
	else
		(void) retention_clear_pending();
	return -1;
}

/* Caller holds retention_lock. */
static int
retention_append(const PsRetentionRecord *rec)
{
	int			fd;
	int			created;
	off_t		old_size;

	if (retention_is_poisoned)
		return -1;
	if (retention_begin_pending() != 0)
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
		(void) retention_clear_pending();
		return -1;
	}
	old_size = lseek(fd, 0, SEEK_END);
	if (old_size < 0)
	{
		close(fd);
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
	if (close(fd) != 0)
	{
		retention_is_poisoned = 1;
		return -1;
	}
	if (created && retention_fsync_dir() != 0)
	{
		retention_is_poisoned = 1;
		return -1;
	}
	if (created && retention_mark_initialized() != 0)
	{
		retention_is_poisoned = 1;
		return -1;
	}
	{
		uint32_t new_hash = retention_fnv1a(retention_log_hash, rec, sizeof(*rec));

		if (retention_write_state(retention_nrecords + 1, new_hash) != 0)
		{
			retention_is_poisoned = 1;
			return -1;
		}
		retention_log_hash = new_hash;
	}
	retention_nrecords++;
	if (retention_clear_pending() != 0)
	{
		retention_is_poisoned = 1;
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

int
ps_retention_open(const char *store_dir)
{
	int			fd = -1;
	int			rc = -1;
	int			n;
	int			state_rc;
	struct stat st;
	PsRetentionState committed;
	char		legacy_failed_path[4096];
	off_t		off = 0;

	pthread_mutex_lock(&retention_lock);
	free(retention_pins);
	retention_pins = NULL;
	retention_npins = 0;
	retention_cap = 0;
	retention_nrecords = 0;
	retention_log_hash = PS_RETENTION_FNV_INIT;
	retention_is_poisoned = 0;
	retention_dev = 0;
	retention_ino = 0;
	test_fail_append_after_write = 0;
	test_fail_rollback = 0;
	n = snprintf(retention_dir, sizeof(retention_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_dir))
		goto done;
	n = snprintf(retention_path, sizeof(retention_path), "%s/retention.meta",
				 store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_path))
		goto done;
	n = snprintf(retention_marker_path, sizeof(retention_marker_path),
				 "%s/retention.initialized", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_marker_path))
		goto done;
	n = snprintf(retention_pending_path, sizeof(retention_pending_path),
				 "%s/retention.pending", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_pending_path))
		goto done;
	n = snprintf(retention_state_path, sizeof(retention_state_path),
				 "%s/retention.state", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_state_path))
		goto done;
	n = snprintf(legacy_failed_path, sizeof(legacy_failed_path),
				 "%s/retention.failed", store_dir);
	if (n < 0 || (size_t) n >= sizeof(legacy_failed_path))
		goto done;
	if (access(retention_pending_path, F_OK) == 0 || errno != ENOENT)
	{
		errno = EILSEQ;
		goto done;
	}
	/* The immediately preceding format wrote this permanent guard after an
	 * uncertain rollback.  Never migrate/replay past that evidence. */
	if (access(legacy_failed_path, F_OK) == 0 || errno != ENOENT)
	{
		errno = EILSEQ;
		goto done;
	}
	state_rc = retention_read_state(&committed);
	if (state_rc < 0)
		goto done;
	{
		const char *fail_append = getenv("PS_TEST_FAIL_RETENTION_APPEND_AFTER_WRITE");
		const char *fail_rollback = getenv("PS_TEST_FAIL_RETENTION_ROLLBACK");

		test_fail_append_after_write = fail_append ? atoi(fail_append) : 0;
		test_fail_rollback = fail_rollback ? atoi(fail_rollback) : 0;
	}
	fd = open(retention_path, O_RDWR);
	if (fd < 0)
	{
		if (errno == ENOENT && access(retention_marker_path, F_OK) != 0 &&
			errno == ENOENT && state_rc == 0)
			rc = 0;
		goto done;
	}
	if (fstat(fd, &st) != 0)
		goto done;
	retention_dev = st.st_dev;
	retention_ino = st.st_ino;
	while (off + (off_t) sizeof(PsRetentionRecord) <= st.st_size)
	{
		PsRetentionRecord rec;

		if (pread(fd, &rec, sizeof(rec), off) != (ssize_t) sizeof(rec))
			goto done;
		if (!retention_record_valid(&rec))
		{
			errno = EILSEQ;
			goto done;			/* a full corrupt record is never discarded */
		}
		if (retention_apply(&rec) != 0)
			goto done;
		retention_log_hash = retention_fnv1a(retention_log_hash,
										  &rec, sizeof(rec));
		off += sizeof(rec);
		retention_nrecords++;
	}
	if (state_rc > 0)
	{
		if (committed.nrecords != retention_nrecords ||
			committed.log_hash != retention_log_hash)
		{
			errno = EILSEQ;
			goto done;
		}
		/* The committed-prefix state proves that a shorter final fragment is
		 * outside the acknowledged log and can be discarded safely. */
		if (off != st.st_size &&
			(ftruncate(fd, off) != 0 || fsync(fd) != 0))
			goto done;
	}
	else
	{
		/* One-time migration from the pre-committed-prefix format. */
		if (retention_begin_pending() != 0)
			goto done;
		if ((off != st.st_size &&
			 (ftruncate(fd, off) != 0 || fsync(fd) != 0)) ||
			retention_write_state(retention_nrecords, retention_log_hash) != 0 ||
			retention_clear_pending() != 0)
			goto done;
	}
	if (retention_mark_initialized() != 0)
		goto done;
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
	retention_log_hash = PS_RETENTION_FNV_INIT;
	retention_path[0] = '\0';
	retention_marker_path[0] = '\0';
	retention_pending_path[0] = '\0';
	retention_state_path[0] = '\0';
	retention_dir[0] = '\0';
	retention_dev = 0;
	retention_ino = 0;
	pthread_mutex_unlock(&retention_lock);
}

int
ps_retention_set(const PsRetentionPin *pin)
{
	PsRetentionRecord rec;
	int			idx;
	int			rc = -1;

	if (!retention_pin_valid(pin))
		return -1;
	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		goto done;
	idx = retention_find(pin->timeline, pin->owner_kind, pin->owner_id);
	if (idx >= 0 && pin->generation < retention_pins[idx].generation)
	{
		rc = PS_RETENTION_STALE;
		goto done;
	}
	if (idx >= 0 && pin->generation == retention_pins[idx].generation &&
		pin->generation != 0 &&
		!retention_pin_active(&retention_pins[idx]))
	{
		rc = PS_RETENTION_STALE;
		goto done;
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
			goto done;
		retention_make_record(&rec, PS_RETENTION_SET, pin);
		if (retention_append(&rec) != 0)
			goto done;
		if (idx >= 0)
			retention_pins[idx] = *pin;
		else
			retention_pins[retention_npins++] = *pin;
		rc = PS_RETENTION_OK;
	}
done:
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
ps_retention_should_compact(void)
{
	int			should;

	pthread_mutex_lock(&retention_lock);
	should = !retention_is_poisoned && retention_nrecords >= 64 &&
		retention_nrecords > 4 * ((uint64_t) retention_npins + 1) &&
		retention_compact_retry_ready();
	pthread_mutex_unlock(&retention_lock);
	return should;
}

int
ps_retention_compact(void)
{
	char		tmp[4096] = {0};
	int			fd = -1;
	int			rc = -1;
	int			n;
	int			pending = 0;
	int			published = 0;
	uint32_t	new_hash = PS_RETENTION_FNV_INIT;

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		goto done;
	if (retention_begin_pending() != 0)
		goto done;
	pending = 1;
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", retention_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		goto done;
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		goto done;
	for (uint32_t i = 0; i < retention_npins; i++)
	{
		PsRetentionRecord rec;

		retention_make_record(&rec,
						  retention_pin_active(&retention_pins[i]) ?
						  PS_RETENTION_SET : PS_RETENTION_DROP,
						  &retention_pins[i]);
		if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec))
			goto done;
		new_hash = retention_fnv1a(new_hash, &rec, sizeof(rec));
	}
	if (fsync(fd) != 0)
	{
		close(fd);
		fd = -1;
		goto done;
	}
	if (close(fd) != 0)
	{
		fd = -1;
		goto done;
	}
	fd = -1;
	if (rename(tmp, retention_path) != 0)
		goto done;
	published = 1;
	/* Once rename publishes the new inode, a failed directory fsync means its
	 * durability is unknown.  Do not continue from an in-memory state that a
	 * crash may roll back. */
	if (retention_fsync_dir() != 0)
	{
		retention_is_poisoned = 1;
		goto done;
	}
	{
		struct stat st;

		if (stat(retention_path, &st) != 0)
		{
			retention_is_poisoned = 1;
			goto done;
		}
		retention_dev = st.st_dev;
		retention_ino = st.st_ino;
	}
	if (retention_write_state(retention_npins, new_hash) != 0)
	{
		retention_is_poisoned = 1;
		goto done;
	}
	retention_nrecords = retention_npins;
	retention_log_hash = new_hash;
	if (retention_clear_pending() != 0)
	{
		retention_is_poisoned = 1;
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
		(void) retention_clear_pending();
	if (rc != 0 && pending && published)
		retention_is_poisoned = 1;
	if (rc != 0 && !retention_is_poisoned)
		retention_defer_compact();
	pthread_mutex_unlock(&retention_lock);
	return rc;
}
