/*-------------------------------------------------------------------------
 *
 * pagestore_store_owner.c
 *
 * The lease is a persistent .pagestore.lock inode protected by flock().
 * Providers in one process share one open lock and reference count; separate
 * processes contend on the kernel lock.  The child side of fork discards its
 * inherited references and descriptors before it can acquire a new lease.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "pagestore_store_owner.h"

typedef struct PsStoreOwnerEntry
{
	dev_t		root_dev;
	ino_t		root_ino;
	char		root[PATH_MAX];
	int		root_fd;
	int		lock_fd;
	unsigned int refs;
	struct PsStoreOwnerEntry *next;
} PsStoreOwnerEntry;

struct PsStoreOwner
{
	PsStoreOwnerEntry *entry;
	pid_t		pid;
};

static pthread_mutex_t owner_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t owner_atfork_once = PTHREAD_ONCE_INIT;
static PsStoreOwnerEntry *owner_entries;
static int owner_atfork_rc;

static void
owner_prepare(void)
{
	(void) pthread_mutex_lock(&owner_lock);
}

static void
owner_parent(void)
{
	(void) pthread_mutex_unlock(&owner_lock);
}

static void
owner_child(void)
{
	PsStoreOwnerEntry *entry;

	/* Closing this process's descriptor does not release the parent's open-file
	 * description.  Do not free entries: inherited handles still point at them
	 * and release() must be able to identify them as child-invalid handles. */
	for (entry = owner_entries; entry != NULL; entry = entry->next)
	{
		if (entry->lock_fd >= 0)
			(void) close(entry->lock_fd);
		if (entry->root_fd >= 0)
			(void) close(entry->root_fd);
		entry->root_fd = -1;
		entry->lock_fd = -1;
		entry->refs = 0;
	}
	(void) pthread_mutex_unlock(&owner_lock);
}

static void
owner_register_atfork(void)
{
	owner_atfork_rc = pthread_atfork(owner_prepare, owner_parent, owner_child);
}

static int
owner_prepare_root(const char *store_dir, char *root, size_t root_len,
					   struct stat *root_st)
{
	int		n;

	if (store_dir == NULL || store_dir[0] == '\0')
	{
		errno = EINVAL;
		return -1;
	}
	if (mkdir(store_dir, 0700) != 0 && errno != EEXIST)
		return -1;
	if (realpath(store_dir, root) == NULL)
		return -1;
	n = (int) strlen(root);
	if (n <= 0 || (size_t) n >= root_len)
	{
		errno = ENAMETOOLONG;
		return -1;
	}
	if (stat(root, root_st) != 0)
		return -1;
	if (!S_ISDIR(root_st->st_mode))
	{
		errno = ENOTDIR;
		return -1;
	}
	return 0;
}

static int
owner_open_lock(const char *root, const struct stat *root_st, int *root_fd_out)
{
	char		path[PATH_MAX];
	struct stat st;
	int		root_fd;
	int		fd;
	int		n;

	root_fd = open(root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (root_fd < 0)
		return -1;
	if (fstat(root_fd, &st) != 0 || !S_ISDIR(st.st_mode) ||
		st.st_dev != root_st->st_dev || st.st_ino != root_st->st_ino)
	{
		int save_errno = errno;

		close(root_fd);
		errno = save_errno != 0 ? save_errno : EAGAIN;
		return -1;
	}
	n = snprintf(path, sizeof(path), ".pagestore.lock");
	if (n < 0 || (size_t) n >= sizeof(path))
	{
		close(root_fd);
		errno = ENAMETOOLONG;
		return -1;
	}
	fd = openat(root_fd, path,
				O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
	if (fd < 0)
	{
		int save_errno = errno;

		close(root_fd);
		errno = save_errno;
		return -1;
	}
	if (fstat(fd, &st) != 0)
	{
		int save_errno = errno;

		close(fd);
		close(root_fd);
		errno = save_errno;
		return -1;
	}
	if (!S_ISREG(st.st_mode) || st.st_nlink != 1)
	{
		close(fd);
		close(root_fd);
		errno = EINVAL;
		return -1;
	}
	if (flock(fd, LOCK_EX | LOCK_NB) != 0)
	{
		int save_errno = errno;

		close(fd);
		close(root_fd);
		errno = save_errno;
		return -1;
	}
	*root_fd_out = root_fd;
	return fd;
}

int
ps_store_owner_acquire(const char *store_dir, PsStoreOwner **owner_out)
{
	char		root[PATH_MAX];
	struct stat root_st;
	PsStoreOwnerEntry *entry;
	PsStoreOwner *handle;
	int		fd = -1;
	int		root_fd = -1;
	int		save_errno;
	int		new_entry = 0;

	if (owner_out == NULL)
	{
		errno = EINVAL;
		return -1;
	}
	*owner_out = NULL;
	handle = malloc(sizeof(*handle));
	if (handle == NULL)
		return -1;
	if (pthread_once(&owner_atfork_once, owner_register_atfork) != 0 ||
		owner_atfork_rc != 0)
	{
		free(handle);
		errno = EAGAIN;
		return -1;
	}
	if (owner_prepare_root(store_dir, root, sizeof(root), &root_st) != 0)
	{
		save_errno = errno;
		free(handle);
		errno = save_errno;
		return -1;
	}
	if (pthread_mutex_lock(&owner_lock) != 0)
	{
		free(handle);
		errno = EBUSY;
		return -1;
	}
	for (entry = owner_entries; entry != NULL; entry = entry->next)
		if (entry->root_dev == root_st.st_dev && entry->root_ino == root_st.st_ino)
			break;
	if (entry != NULL && entry->refs != 0)
	{
		entry->refs++;
		pthread_mutex_unlock(&owner_lock);
	}
	else
	{
		if (entry == NULL)
		{
			entry = calloc(1, sizeof(*entry));
			if (entry == NULL)
			{
				save_errno = errno;
				pthread_mutex_unlock(&owner_lock);
				free(handle);
				errno = save_errno;
				return -1;
			}
			entry->root_dev = root_st.st_dev;
			entry->root_ino = root_st.st_ino;
			snprintf(entry->root, sizeof(entry->root), "%s", root);
			entry->root_fd = -1;
			entry->lock_fd = -1;
			entry->next = owner_entries;
			owner_entries = entry;
			new_entry = 1;
		}
		fd = owner_open_lock(root, &root_st, &root_fd);
		if (fd < 0)
		{
			save_errno = errno;
			if (new_entry && entry->refs == 0 && entry->lock_fd < 0)
			{
				owner_entries = entry->next;
				free(entry);
			}
			pthread_mutex_unlock(&owner_lock);
			free(handle);
			errno = save_errno;
			return -1;
		}
		entry->root_fd = root_fd;
		entry->lock_fd = fd;
		entry->refs = 1;
		pthread_mutex_unlock(&owner_lock);
	}
	handle->entry = entry;
	handle->pid = getpid();
	*owner_out = handle;
	return 0;
}

const char *
ps_store_owner_root(const PsStoreOwner *owner)
{
	if (owner == NULL || owner->entry == NULL || owner->pid != getpid())
		return NULL;
	return owner->entry->root;
}

int
ps_store_owner_require_current(const PsStoreOwner *owner)
{
	if (owner == NULL || owner->entry == NULL)
	{
		errno = EINVAL;
		return -1;
	}
	if (owner->pid != getpid())
	{
		errno = ECHILD;
		return -1;
	}
	return 0;
}

void
ps_store_owner_release(PsStoreOwner *owner)
{
	PsStoreOwnerEntry **link;

	if (owner == NULL)
		return;
	if (owner->pid != getpid())
	{
		free(owner);
		return;
	}
	if (pthread_mutex_lock(&owner_lock) != 0)
	{
		/* Never mutate the shared entry without its mutex.  A mutex failure is
		 * catastrophic and deliberately leaks this handle/lease rather than
		 * risking an unsynchronised decrement or a premature unlock. */
		return;
	}
	if (owner->entry != NULL && owner->entry->refs > 0)
	{
		owner->entry->refs--;
		if (owner->entry->refs == 0)
		{
			(void) close(owner->entry->lock_fd);
			(void) close(owner->entry->root_fd);
			owner->entry->lock_fd = -1;
			owner->entry->root_fd = -1;
			link = &owner_entries;
			while (*link != owner->entry && *link != NULL)
				link = &(*link)->next;
			if (*link == owner->entry)
			{
				*link = owner->entry->next;
				free(owner->entry);
			}
		}
	}
	pthread_mutex_unlock(&owner_lock);
	free(owner);
}
