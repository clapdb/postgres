/*-------------------------------------------------------------------------
 *
 * pagestore_compat.h
 *	  libc portability shims shared by the standalone pagestore sources.
 *
 * The daemon and its tools are POSIX programs developed on Linux/glibc; this
 * header papers over the few places where macOS differs.  Included only by
 * freestanding sources (no PostgreSQL headers).
 *
 * src/../contrib/pagestore/pagestore_compat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_COMPAT_H
#define PAGESTORE_COMPAT_H

#include <dirent.h>
#include <sys/stat.h>

/* macOS spells the nanosecond stat timestamps st_mtimespec/st_ctimespec. */
#ifdef __APPLE__
#ifndef st_mtim
#define st_mtim st_mtimespec
#endif
#ifndef st_ctim
#define st_ctim st_ctimespec
#endif
#endif

/*
 * Open a directory stream on a descriptor that was dup'd from a long-lived
 * directory descriptor, starting from the first entry.
 *
 * A dup shares its file offset with the original.  glibc's fdopendir() reads
 * lazily, so an unread stream leaves that shared offset at zero; Darwin's
 * prefetches on open, so merely opening and closing one stream moves the
 * shared offset to EOF and the next fdopendir() on a fresh dup returns no
 * entries.  Every caller here wants a complete scan, so rewind explicitly.
 */
static inline DIR *
ps_fdopendir_scan(int fd)
{
	DIR		   *dir = fdopendir(fd);

	if (dir != NULL)
		rewinddir(dir);
	return dir;
}

#endif							/* PAGESTORE_COMPAT_H */
