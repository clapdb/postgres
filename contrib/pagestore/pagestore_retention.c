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

static char retention_path[4096];
static char retention_dir[2048];
static PsRetentionPin *retention_pins;
static uint32_t retention_npins;
static uint32_t retention_cap;
static uint64_t retention_nrecords;
static int retention_is_poisoned;
static struct timespec retention_compact_retry_at;
static pthread_mutex_t retention_lock = PTHREAD_MUTEX_INITIALIZER;

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
		pin->lsn != 0 && pin->pad == 0;
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
			rec->pin.lsn == 0 && rec->pin.pad == 0;
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
		if ((uint32_t) idx + 1 < retention_npins)
			memmove(&retention_pins[idx], &retention_pins[idx + 1],
					(size_t) (retention_npins - (uint32_t) idx - 1) *
						sizeof(*retention_pins));
		retention_npins--;
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

static void
retention_rollback(int fd, off_t old_size, int created)
{
	if (ftruncate(fd, old_size) != 0)
	{
		/* best effort; the caller poisons this process on every rollback */
	}
	if (fsync(fd) != 0)
	{
		/* best effort; see above */
	}
	if (close(fd) != 0)
	{
		/* best effort; see above */
	}
	if (created && old_size == 0 && unlink(retention_path) == 0)
		(void) retention_fsync_dir();
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
	created = 0;
	fd = open(retention_path, O_WRONLY | O_APPEND);
	if (fd < 0 && errno == ENOENT)
	{
		fd = open(retention_path, O_WRONLY | O_APPEND | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			created = 1;
	}
	if (fd < 0)
		return -1;
	old_size = lseek(fd, 0, SEEK_END);
	if (old_size < 0)
	{
		close(fd);
		return -1;
	}
	/* Detect an externally lost/truncated/replaced log before appending after
	 * the gap.  Replaying such a file could silently omit the last live SET. */
	if (old_size != (off_t) (retention_nrecords * sizeof(*rec)))
	{
		errno = EILSEQ;
		retention_rollback(fd, old_size, created);
		retention_is_poisoned = 1;
		return -1;
	}
	if (write(fd, rec, sizeof(*rec)) != (ssize_t) sizeof(*rec) ||
		fsync(fd) != 0)
	{
		retention_rollback(fd, old_size, created);
		retention_is_poisoned = 1;
		return -1;
	}
	if (close(fd) != 0)
	{
		fd = open(retention_path, O_WRONLY);
		if (fd >= 0)
			retention_rollback(fd, old_size, created);
		retention_is_poisoned = 1;
		return -1;
	}
	if (created && retention_fsync_dir() != 0)
	{
		fd = open(retention_path, O_WRONLY);
		if (fd >= 0)
			retention_rollback(fd, old_size, created);
		retention_is_poisoned = 1;
		return -1;
	}
	retention_nrecords++;
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
	struct stat st;
	off_t		off = 0;

	pthread_mutex_lock(&retention_lock);
	free(retention_pins);
	retention_pins = NULL;
	retention_npins = 0;
	retention_cap = 0;
	retention_nrecords = 0;
	retention_is_poisoned = 0;
	n = snprintf(retention_dir, sizeof(retention_dir), "%s", store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_dir))
		goto done;
	n = snprintf(retention_path, sizeof(retention_path), "%s/retention.meta",
				 store_dir);
	if (n < 0 || (size_t) n >= sizeof(retention_path))
		goto done;
	fd = open(retention_path, O_RDWR);
	if (fd < 0)
	{
		if (errno == ENOENT)
			rc = 0;
		goto done;
	}
	if (fstat(fd, &st) != 0)
		goto done;
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
		off += sizeof(rec);
		retention_nrecords++;
	}
	/* A short tail can only be an uncommitted append.  Keeping a partial DROP
	 * would be unsafe; truncate it before accepting any new mutation. */
	if (off != st.st_size &&
		(ftruncate(fd, off) != 0 || fsync(fd) != 0))
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
	retention_path[0] = '\0';
	retention_dir[0] = '\0';
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
	if (idx >= 0 && memcmp(&retention_pins[idx], pin, sizeof(*pin)) == 0)
	{
		struct stat st;

		if (stat(retention_path, &st) == 0 &&
			st.st_size == (off_t) (retention_nrecords * sizeof(rec)))
			rc = 0;			/* exact retry: no log churn */
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
		rc = 0;
	}
done:
	pthread_mutex_unlock(&retention_lock);
	return rc;
}

int
ps_retention_drop(uint32_t timeline, uint32_t owner_kind, uint64_t owner_id)
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
	if (idx < 0)
	{
		rc = 0;				/* idempotent retry */
		goto done;
	}
	memset(&pin, 0, sizeof(pin));
	pin.timeline = timeline;
	pin.owner_kind = owner_kind;
	pin.owner_id = owner_id;
	retention_make_record(&rec, PS_RETENTION_DROP, &pin);
	if (retention_append(&rec) != 0)
		goto done;
	if ((uint32_t) idx + 1 < retention_npins)
		memmove(&retention_pins[idx], &retention_pins[idx + 1],
				(size_t) (retention_npins - (uint32_t) idx - 1) *
					sizeof(*retention_pins));
	retention_npins--;
	rc = 0;
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
		*count_out = retention_npins;
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
		if (count_out)
			*count_out = retention_npins;
		if (index < retention_npins)
		{
			if (pin_out)
				*pin_out = retention_pins[index];
			rc = 1;
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
	if (retention_is_poisoned || capacity < retention_npins)
		rc = -1;
	else
	{
		memcpy(pins, retention_pins, (size_t) retention_npins * sizeof(*pins));
		*count_out = retention_npins;
	}
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

	pthread_mutex_lock(&retention_lock);
	if (retention_is_poisoned)
		goto done;
	n = snprintf(tmp, sizeof(tmp), "%s.tmp", retention_path);
	if (n < 0 || (size_t) n >= sizeof(tmp))
		goto done;
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		goto done;
	for (uint32_t i = 0; i < retention_npins; i++)
	{
		PsRetentionRecord rec;

		retention_make_record(&rec, PS_RETENTION_SET, &retention_pins[i]);
		if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec))
			goto done;
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
	/* Once rename publishes the new inode, a failed directory fsync means its
	 * durability is unknown.  Do not continue from an in-memory state that a
	 * crash may roll back. */
	if (retention_fsync_dir() != 0)
	{
		retention_is_poisoned = 1;
		goto done;
	}
	retention_nrecords = retention_npins;
	memset(&retention_compact_retry_at, 0, sizeof(retention_compact_retry_at));
	rc = 0;
done:
	if (fd >= 0)
		close(fd);
	if (rc != 0 && tmp[0] != '\0')
		unlink(tmp);
	if (rc != 0 && !retention_is_poisoned)
		retention_defer_compact();
	pthread_mutex_unlock(&retention_lock);
	return rc;
}
