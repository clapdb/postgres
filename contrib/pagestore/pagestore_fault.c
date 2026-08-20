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
#include <unistd.h>

typedef struct PsFaultEntry
{
	PsFaultPoint point;
	const char *name;
	const char *action;
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
} PsFaultState;

static PsFaultState fault = {0, 0, PS_FAULT_POINT_INVALID, 0, 0, -1};

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
parse_hit(const char *value, uint64_t *hit)
{
	char *end = NULL;
	unsigned long long parsed;

	if (value == NULL || value[0] == '\0' || value[0] == '-')
		return -1;
	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' || parsed == 0 ||
		(uint64_t) parsed != parsed || parsed > INT64_MAX)
		return -1;
	*hit = (uint64_t) parsed;
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

const char *
ps_fault_name(PsFaultPoint point)
{
	if (point < 0 || point >= PS_FAULT_POINT_COUNT)
		return NULL;
	return fault_catalog[point].name;
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
	char control_real[PATH_MAX];
	char store_real[PATH_MAX];
	PsFaultPoint point;
	uint64_t target;
	int any = name != NULL || action != NULL || hit != NULL || control != NULL;
	int dirfd;
	struct stat st;
	struct stat control_lstat;

	ps_fault_reset();
	fault.initialized = 1;
	if (!any)
		return 0;
	if (name == NULL || action == NULL || hit == NULL || control == NULL ||
		ps_fault_lookup(name, &point) != 0 ||
		parse_hit(hit, &target) != 0 || control[0] != '/' ||
		lstat(control, &control_lstat) != 0 || !S_ISDIR(control_lstat.st_mode) ||
		absolute_realpath(control, control_real, sizeof(control_real)) != 0 ||
		absolute_realpath(store_dir, store_real, sizeof(store_real)) != 0 ||
		paths_overlap(control_real, store_real))
		return -1;
	if (strcmp(action, fault_catalog[point].action) != 0 ||
		(fault_catalog[point].max_hit != 0 &&
		 target > fault_catalog[point].max_hit))
		return -1;
	dirfd = open(control_real, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (dirfd < 0 || fstat(dirfd, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		if (dirfd >= 0)
			close(dirfd);
		return -1;
	}
	if (control_regular(dirfd, "arm", 1) != 0 ||
		control_missing(dirfd, "report.jsonl") != 0)
	{
		close(dirfd);
		return -1;
	}
	fault.point = point;
	fault.target_hit = target;
	__atomic_store_n(&fault.hits, 0, __ATOMIC_RELAXED);
	fault.dirfd = dirfd;
	fault.enabled = 1;
	return 0;
}

int
ps_fault_probe(PsFaultPoint point)
{
	uint64_t observed;
	int fd;
	char line[256];
	int n;
	struct stat st;

	if (!fault.initialized || !fault.enabled || point != fault.point)
		return 0;
	if (fstatat(fault.dirfd, "arm", &st, AT_SYMLINK_NOFOLLOW) != 0 ||
		!S_ISREG(st.st_mode))
		return 0;
	observed = __atomic_load_n(&fault.hits, __ATOMIC_RELAXED);
	for (;;)
	{
		if (observed >= fault.target_hit)
			return 0;
		if (__atomic_compare_exchange_n(&fault.hits, &observed, observed + 1,
					0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
			break;
	}
	if (observed + 1 != fault.target_hit)
		return 0;
	fd = openat(fault.dirfd, "report.jsonl", O_WRONLY | O_CREAT | O_APPEND |
			O_CLOEXEC | O_NOFOLLOW, 0600);
	if (fd >= 0 && fstat(fd, &st) == 0 && S_ISREG(st.st_mode))
	{
		n = snprintf(line, sizeof(line),
				"{\"schema\":1,\"name\":\"%s\",\"action\":\"crash\","
				"\"hit\":%llu,\"pid\":%ld}\n", ps_fault_name(point),
				(unsigned long long) fault.target_hit, (long) getpid());
		if (n > 0 && (size_t) n < sizeof(line))
			(void) write(fd, line, (size_t) n);
	}
	if (fd >= 0)
		close(fd);
	_exit(88);
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
	__atomic_store_n(&fault.hits, 0, __ATOMIC_RELAXED);
	fault.dirfd = -1;
}
