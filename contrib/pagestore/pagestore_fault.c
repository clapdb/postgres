#include "pagestore_fault.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define PS_FAULT_FIELD_MAX 128
#define PS_FAULT_DEFAULT_WATCHDOG_MS 5000U
#define PS_FAULT_MAX_WATCHDOG_MS 300000U
#define PS_FAULT_POLL_NS UINT64_C(10000000)

typedef struct PsFaultEntry
{
	PsFaultPoint point;
	const char *name;
	const char *actions;
	uint64_t target_hit;
	uint64_t max_hit;
} PsFaultEntry;

static const PsFaultEntry fault_catalog[] = {
#define PAGESTORE_FAULT_POINT(symbol, name, target, model, action, hit, max_hit) \
	{PS_FAULT_POINT_##symbol, name, action, hit, max_hit},
#include "pagestore_fault_points.def"
#undef PAGESTORE_FAULT_POINT
};

typedef struct PsFaultState
{
	int initialized;
	int enabled;
	PsFaultPoint point;
	uint64_t target_hit;
	uint64_t hits;
	int dirfd;
	unsigned int watchdog_ms;
	char action[16];
	char scenario[PS_FAULT_FIELD_MAX + 1];
	char seed[PS_FAULT_FIELD_MAX + 1];
	char operation[PS_FAULT_FIELD_MAX + 1];
	int has_metadata;
} PsFaultState;

static PsFaultState fault = {0, 0, PS_FAULT_POINT_INVALID, 0, 0, -1, 0,
	{""}, {""}, {""}, {""}, 0};

static int
write_all(int fd, const char *buffer, size_t length)
{
	size_t written = 0;

	while (written < length)
	{
		ssize_t result = write(fd, buffer + written, length - written);

		if (result < 0 && errno == EINTR)
			continue;
		if (result <= 0)
			return -1;
		written += (size_t) result;
	}
	return 0;
}

static int
component_prefix(const char *a, const char *b)
{
	size_t n = strlen(a);

	if (strcmp(a, "/") == 0)
		return b[0] == '/';
	return strncmp(a, b, n) == 0 && (b[n] == '\0' || b[n] == '/');
}

static int
paths_overlap(const char *a, const char *b)
{
	return component_prefix(a, b) || component_prefix(b, a);
}

static int
absolute_realpath(const char *path, char *out, size_t outlen)
{
	char absolute[PATH_MAX];
	char parent[PATH_MAX];
	char base[PATH_MAX];
	char resolved_parent[PATH_MAX];
	const char *slash;
	int n;

	if (path == NULL || path[0] == '\0' || path[0] != '/')
		return -1;
	if (realpath(path, out) != NULL)
		return strlen(out) < outlen ? 0 : -1;
	if (strlen(path) >= sizeof(absolute))
		return -1;
	memcpy(absolute, path, strlen(path) + 1);
	slash = strrchr(absolute, '/');
	if (slash == absolute)
		return snprintf(out, outlen, "/%s", slash + 1) >= (int) outlen ? -1 : 0;
	n = (int) (slash - absolute);
	if (n <= 0 || (size_t) n >= sizeof(parent) || strlen(slash + 1) >= sizeof(base))
		return -1;
	memcpy(parent, absolute, (size_t) n);
	parent[n] = '\0';
	memcpy(base, slash + 1, strlen(slash + 1) + 1);
	if (realpath(parent, resolved_parent) == NULL)
		return -1;
	n = snprintf(out, outlen, "%s/%s", resolved_parent, base);
	return n < 0 || (size_t) n >= outlen ? -1 : 0;
}

static int
parse_uint64(const char *value, uint64_t *result, int allow_zero)
{
	char *end = NULL;
	unsigned long long parsed;

	if (value == NULL || value[0] == '\0' || value[0] == '-')
		return -1;
	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' ||
		(!allow_zero && parsed == 0) || (uint64_t) parsed != parsed ||
		parsed > INT64_MAX)
		return -1;
	*result = (uint64_t) parsed;
	return 0;
}

static int
parse_watchdog(const char *value, unsigned int *result)
{
	uint64_t parsed;

	if (value == NULL)
	{
		*result = PS_FAULT_DEFAULT_WATCHDOG_MS;
		return 0;
	}
	if (parse_uint64(value, &parsed, 0) != 0 ||
		parsed > PS_FAULT_MAX_WATCHDOG_MS)
		return -1;
	*result = (unsigned int) parsed;
	return 0;
}

static int
valid_utf8(const unsigned char *value, size_t remaining)
{
	while (remaining > 0)
	{
		if (*value <= 0x7f)
		{
			value++;
			remaining--;
			continue;
		}
		if (remaining >= 2 && *value >= 0xc2 && *value <= 0xdf &&
			value[1] >= 0x80 && value[1] <= 0xbf)
		{
			value += 2;
			remaining -= 2;
			continue;
		}
		if (remaining >= 3 &&
			((*value == 0xe0 && value[1] >= 0xa0 && value[1] <= 0xbf) ||
			 (*value >= 0xe1 && *value <= 0xec && value[1] >= 0x80 && value[1] <= 0xbf) ||
			 (*value >= 0xee && *value <= 0xef && value[1] >= 0x80 && value[1] <= 0xbf)) &&
			value[2] >= 0x80 && value[2] <= 0xbf)
		{
			value += 3;
			remaining -= 3;
			continue;
		}
		if (remaining >= 3 && *value == 0xed &&
			value[1] >= 0x80 && value[1] <= 0x9f &&
			value[2] >= 0x80 && value[2] <= 0xbf)
		{
			value += 3;
			remaining -= 3;
			continue;
		}
		if (remaining >= 4 &&
			((*value == 0xf0 && value[1] >= 0x90 && value[1] <= 0xbf) ||
			 (*value >= 0xf1 && *value <= 0xf3 && value[1] >= 0x80 && value[1] <= 0xbf) ||
			 (*value == 0xf4 && value[1] >= 0x80 && value[1] <= 0x8f)) &&
			value[2] >= 0x80 && value[2] <= 0xbf &&
			value[3] >= 0x80 && value[3] <= 0xbf)
		{
			value += 4;
			remaining -= 4;
			continue;
		}
		return 0;
	}
	return 1;
}

static int
copy_identity(const char *value, char *destination, int required)
{
	size_t length;

	if (value == NULL)
	{
		if (required)
			return -1;
		destination[0] = '\0';
		return 0;
	}
	length = strlen(value);
	if (length == 0 || length > PS_FAULT_FIELD_MAX ||
		!valid_utf8((const unsigned char *) value, length))
		return -1;
	for (size_t i = 0; i < length; i++)
		if ((unsigned char) value[i] < 0x20 || value[i] == '"' || value[i] == '\\')
			return -1;
	memcpy(destination, value, length + 1);
	return 0;
}

static int
control_regular(int dirfd, const char *name, int allow_missing)
{
	struct stat st;

	if (fstatat(dirfd, name, &st, AT_SYMLINK_NOFOLLOW) != 0)
		return allow_missing && errno == ENOENT ? 0 : -1;
	return S_ISREG(st.st_mode) ? 0 : -1;
}

static int
control_missing(int dirfd, const char *name)
{
	struct stat st;

	if (fstatat(dirfd, name, &st, AT_SYMLINK_NOFOLLOW) == 0)
		return -1;
	return errno == ENOENT ? 0 : -1;
}

static int
action_allowed(PsFaultPoint point, const char *action)
{
	const char *actions = fault_catalog[point].actions;
	const char *cursor = actions;
	size_t length = strlen(action);

	while (*cursor != '\0')
	{
		const char *separator = strchr(cursor, '|');
		size_t available = separator == NULL ? strlen(cursor) : (size_t) (separator - cursor);

		if (available == length && strncmp(cursor, action, length) == 0)
			return 0;
		if (separator == NULL)
			break;
		cursor = separator + 1;
	}
	return -1;
}

static uint64_t
monotonic_ms(void)
{
	struct timespec now;
	uint64_t seconds;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0)
		return UINT64_MAX;
	seconds = (uint64_t) now.tv_sec;
	if (seconds > UINT64_MAX / UINT64_C(1000))
		return UINT64_MAX;
	seconds *= UINT64_C(1000);
	return seconds > UINT64_MAX - (uint64_t) now.tv_nsec / UINT64_C(1000000) ?
		UINT64_MAX : seconds + (uint64_t) now.tv_nsec / UINT64_C(1000000);
}

static int
report_fault(const char *action, const char *state, unsigned int timeout_ms,
		int append, uint64_t replay_lsn)
{
	const char *filename = append ? "report.jsonl" : "report.tmp";
	char line[1024];
	int fd;
	int n;
	struct stat st;

	fd = openat(fault.dirfd, filename, O_WRONLY | O_CLOEXEC |
		(append ? O_APPEND : O_CREAT | O_EXCL) | O_NOFOLLOW, 0600);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
	{
		if (fd >= 0)
			close(fd);
		if (!append)
			(void) unlinkat(fault.dirfd, filename, 0);
		return -1;
	}
	if (fault.has_metadata)
		n = snprintf(line, sizeof(line),
			"{\"schema\":1,\"name\":\"%s\",\"action\":\"%s\","
			"\"scenario\":\"%s\",\"seed\":%s,"
			"\"hit\":%llu,\"pid\":%ld,\"operation\":\"%s\"",
			ps_fault_name(fault.point), action, fault.scenario, fault.seed,
			(unsigned long long) fault.target_hit,
			(long) getpid(), fault.operation);
	else
		n = snprintf(line, sizeof(line),
			"{\"schema\":1,\"name\":\"%s\",\"action\":\"%s\","
			"\"hit\":%llu,\"pid\":%ld",
			ps_fault_name(fault.point), action,
			(unsigned long long) fault.target_hit, (long) getpid());
	if (n <= 0 || (size_t) n >= sizeof(line))
	{
		close(fd);
		if (!append)
			(void) unlinkat(fault.dirfd, filename, 0);
		return -1;
	}
	if (replay_lsn != 0)
	{
		n += snprintf(line + n, sizeof(line) - (size_t) n,
				",\"replay_lsn\":\"%X/%X\"",
				(uint32_t) (replay_lsn >> 32), (uint32_t) replay_lsn);
		if (n <= 0 || (size_t) n >= sizeof(line))
		{
			close(fd);
			if (!append)
				(void) unlinkat(fault.dirfd, filename, 0);
			return -1;
		}
	}
	if (n > 0 && (size_t) n < sizeof(line) && state != NULL)
		n += snprintf(line + n, sizeof(line) - (size_t) n,
				",\"state\":\"%s\",\"watchdog_ms\":%u", state, timeout_ms);
	if (n > 0 && (size_t) n + 2 < sizeof(line))
	{
		line[n++] = '}';
		line[n++] = '\n';
	}
	if (n <= 0 || (size_t) n >= sizeof(line) || write_all(fd, line, (size_t) n) != 0)
	{
		close(fd);
		if (!append)
			(void) unlinkat(fault.dirfd, filename, 0);
		return -1;
	}
	if (close(fd) != 0)
	{
		if (!append)
			(void) unlinkat(fault.dirfd, filename, 0);
		return -1;
	}
	if (!append && renameat(fault.dirfd, filename, fault.dirfd, "report.jsonl") != 0)
	{
		(void) unlinkat(fault.dirfd, filename, 0);
		return -1;
	}
	return 0;
}

static int
wait_for_release(void)
{
	uint64_t start;
	uint64_t deadline;

	start = monotonic_ms();
	if (start == UINT64_MAX)
		return PS_FAULT_PROBE_PAUSE_TIMEOUT;
	deadline = start > UINT64_MAX - fault.watchdog_ms ?
		UINT64_MAX : start + fault.watchdog_ms;

	for (;;)
	{
		if (monotonic_ms() >= deadline)
			return PS_FAULT_PROBE_PAUSE_TIMEOUT;
		if (control_regular(fault.dirfd, "release", 0) == 0)
			return 0;
		{
			struct timespec delay = {0, (long) PS_FAULT_POLL_NS};
			nanosleep(&delay, NULL);
		}
	}
}

const char *
ps_fault_name(PsFaultPoint point)
{
	if (point < 0 || point >= PS_FAULT_POINT_COUNT)
		return NULL;
	return fault_catalog[point].name;
}

const char *
ps_fault_allowed_actions(PsFaultPoint point)
{
	if (point < 0 || point >= PS_FAULT_POINT_COUNT)
		return NULL;
	return fault_catalog[point].actions;
}

int
ps_fault_lookup(const char *name, PsFaultPoint *point)
{
	if (name != NULL)
		for (size_t i = 0; i < sizeof(fault_catalog) / sizeof(fault_catalog[0]); i++)
			if (strcmp(name, fault_catalog[i].name) == 0)
			{
				if (point != NULL)
					*point = fault_catalog[i].point;
				return 0;
			}
	if (point != NULL)
		*point = PS_FAULT_POINT_INVALID;
	return -1;
}

int
ps_fault_init(const char *store_dir)
{
	const char *name = getenv("PAGESTORE_TEST_FAULT_NAME");
	const char *action = getenv("PAGESTORE_TEST_FAULT_ACTION");
	const char *hit = getenv("PAGESTORE_TEST_FAULT_HIT");
	const char *control = getenv("PAGESTORE_TEST_FAULT_DIR");
	const char *scenario = getenv("PAGESTORE_TEST_FAULT_SCENARIO");
	const char *seed = getenv("PAGESTORE_TEST_FAULT_SEED");
	const char *operation = getenv("PAGESTORE_TEST_FAULT_OPERATION");
	const char *operation_id = getenv("PAGESTORE_TEST_FAULT_OPERATION_ID");
	const char *watchdog = getenv("PAGESTORE_TEST_FAULT_WATCHDOG_MS");
	char control_real[PATH_MAX];
	char store_real[PATH_MAX];
	char seed_canonical[32];
	PsFaultPoint point;
	uint64_t target;
	uint64_t seed_value;
	int any = name != NULL || action != NULL || hit != NULL || control != NULL ||
		scenario != NULL || seed != NULL || operation != NULL || operation_id != NULL ||
		watchdog != NULL;
	int dirfd;
	struct stat st;
	struct stat control_lstat;

	ps_fault_reset();
	fault.initialized = 1;
	if (!any)
		return 0;
	if (seed != NULL)
	{
		int seed_length;

		if (parse_uint64(seed, &seed_value, 1) != 0)
			return -1;
		seed_length = snprintf(seed_canonical, sizeof(seed_canonical), "%llu",
			(unsigned long long) seed_value);
		if (seed_length <= 0 || (size_t) seed_length >= sizeof(seed_canonical) ||
			strcmp(seed, seed_canonical) != 0)
			return -1;
	}
	if (name == NULL || action == NULL || hit == NULL || control == NULL ||
		ps_fault_lookup(name, &point) != 0 || parse_uint64(hit, &target, 0) != 0 ||
		control[0] != '/' || lstat(control, &control_lstat) != 0 ||
		!S_ISDIR(control_lstat.st_mode) ||
		absolute_realpath(control, control_real, sizeof(control_real)) != 0 ||
		absolute_realpath(store_dir, store_real, sizeof(store_real)) != 0 ||
		paths_overlap(control_real, store_real) ||
		action_allowed(point, action) != 0 ||
		(fault_catalog[point].max_hit != 0 && target > fault_catalog[point].max_hit) ||
		copy_identity(scenario, fault.scenario, scenario != NULL) != 0 ||
		copy_identity(seed, fault.seed, seed != NULL) != 0 ||
		copy_identity(operation != NULL ? operation : operation_id, fault.operation,
			operation != NULL || operation_id != NULL) != 0 ||
		(operation != NULL && operation_id != NULL) ||
		((scenario != NULL || seed != NULL || operation != NULL || operation_id != NULL) &&
			(scenario == NULL || seed == NULL || (operation == NULL && operation_id == NULL))) ||
		parse_watchdog(watchdog, &fault.watchdog_ms) != 0 ||
		(strlen(action) >= sizeof(fault.action)) ||
		(strcmp(action, "pause") != 0 && watchdog != NULL))
		return -1;
	memcpy(fault.action, action, strlen(action) + 1);
	fault.target_hit = target;
	fault.has_metadata = scenario != NULL;
	dirfd = open(control_real, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (dirfd < 0 || fstat(dirfd, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		if (dirfd >= 0)
			close(dirfd);
		return -1;
	}
	if (control_regular(dirfd, "arm", 1) != 0 ||
		control_missing(dirfd, "release") != 0 ||
		control_missing(dirfd, "report.tmp") != 0 ||
		control_missing(dirfd, "report.jsonl") != 0)
	{
		close(dirfd);
		return -1;
	}
	if (strncmp(fault_catalog[point].name, "materializer.",
				strlen("materializer.")) == 0 &&
		strcmp(action, "pause") == 0 &&
		fault.watchdog_ms < PS_FAULT_MATERIALIZER_MIN_WATCHDOG_MS)
	{
		close(dirfd);
		return -1;
	}
	fault.point = point;
	__atomic_store_n(&fault.hits, 0, __ATOMIC_RELAXED);
	fault.dirfd = dirfd;
	fault.enabled = 1;
	return 0;
}

int
ps_fault_is_initialized(void)
{
	return fault.initialized;
}

int
ps_fault_query(PsFaultPoint point, PsFaultStatus *status)
{
	if (status == NULL || point < 0 || point >= PS_FAULT_POINT_COUNT)
		return -1;
	status->initialized = fault.initialized;
	status->enabled = fault.enabled && point == fault.point;
	status->reached = status->enabled && __atomic_load_n(&fault.hits, __ATOMIC_ACQUIRE) >= fault.target_hit;
	status->point = status->enabled ? fault.point : PS_FAULT_POINT_INVALID;
	status->target_hit = status->enabled ? fault.target_hit : 0;
	status->hits = status->enabled ? __atomic_load_n(&fault.hits, __ATOMIC_ACQUIRE) : 0;
	return 0;
}

int
ps_fault_probe_at(PsFaultPoint point, uint64_t replay_lsn)
{
	uint64_t observed;
	struct stat st;
	const char *action;
	int result;

	if (!fault.initialized || !fault.enabled || point != fault.point)
		return PS_FAULT_PROBE_INACTIVE;
	if (fstatat(fault.dirfd, "arm", &st, AT_SYMLINK_NOFOLLOW) != 0 ||
		!S_ISREG(st.st_mode))
		return PS_FAULT_PROBE_INACTIVE;
	observed = __atomic_load_n(&fault.hits, __ATOMIC_RELAXED);
	for (;;)
	{
		if (observed >= fault.target_hit)
			return PS_FAULT_PROBE_INACTIVE;
		if (__atomic_compare_exchange_n(&fault.hits, &observed, observed + 1,
				0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
			break;
	}
	if (observed + 1 != fault.target_hit)
		return PS_FAULT_PROBE_INACTIVE;
	action = fault.action;
	if (strcmp(action, "crash") == 0)
	{
		if (report_fault(action, NULL, 0, 0, replay_lsn) != 0)
			_exit(PS_FAULT_REPORT_FAILURE_EXIT);
		_exit(PS_FAULT_CRASH_EXIT);
	}
	if (strcmp(action, "error") == 0)
		return report_fault(action, NULL, 0, 0, replay_lsn) == 0 ? PS_FAULT_PROBE_ERROR : PS_FAULT_PROBE_ERROR;
	if (strcmp(action, "pause") != 0)
		return PS_FAULT_PROBE_ERROR;
	if (report_fault(action, "reached", 0, 0, replay_lsn) != 0)
		return PS_FAULT_PROBE_ERROR;
	result = wait_for_release();
	if (result == PS_FAULT_PROBE_PAUSE_TIMEOUT)
	{
		(void) report_fault(action, "timeout", fault.watchdog_ms, 0, replay_lsn);
		_exit(PS_FAULT_PAUSE_TIMEOUT_EXIT);
	}
	return result;
}

int
ps_fault_probe(PsFaultPoint point)
{
	return ps_fault_probe_at(point, 0);
}

void
ps_fault_reset(void)
{
	if (fault.dirfd >= 0)
		close(fault.dirfd);
	fault.initialized = 0;
	fault.enabled = 0;
	fault.point = PS_FAULT_POINT_INVALID;
	fault.target_hit = 0;
	fault.watchdog_ms = 0;
	fault.action[0] = '\0';
	fault.scenario[0] = '\0';
	fault.seed[0] = '\0';
	fault.operation[0] = '\0';
	fault.has_metadata = 0;
	__atomic_store_n(&fault.hits, 0, __ATOMIC_RELAXED);
	fault.dirfd = -1;
}
