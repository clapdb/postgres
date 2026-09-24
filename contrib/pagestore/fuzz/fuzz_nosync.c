/*-------------------------------------------------------------------------
 *
 * fuzz_nosync.c
 *	  Neutralize fsync()/fdatasync()/sync_file_range() in the instrumented
 *	  fuzz binary only, via linker --wrap (see fuzz/build.sh's
 *	  -Wl,--wrap=... flags on pagestore_format_fuzz, and only that binary --
 *	  the meson-built pagestore_format_fuzz_replay regression driver links
 *	  none of this and keeps the real calls).
 *
 * Product code (pagestore_core.c, storage_posix.c, pagestore_manifest.c,
 * pagestore_retention.c, ...) calls these routinely -- once or more per
 * ps_core_open()/ps_core_close()/ps_core_maintenance() -- to make a
 * daemon's durability guarantees real.  On tmpfs they are a true no-op for
 * data safety, but each one is still a real syscall with real kernel-side
 * synchronization cost, and per round-1's numbers they dominated each
 * fuzz iteration's wall time (1-2 exec/s despite tmpfs and a trivial
 * store).  This file is never linked into anything except the
 * throughput-sensitive exploration binary, and never changes product
 * code: __wrap_fsync() etc. simply intercept the *symbol* the linker
 * resolves fsync() calls to in that one binary, exactly like the
 * libeatmydata pattern.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

int
__wrap_fsync(int fd)
{
	(void) fd;
	return 0;
}

int
__wrap_fdatasync(int fd)
{
	(void) fd;
	return 0;
}

int
__wrap_sync_file_range(int fd, off_t offset, off_t nbytes, unsigned int flags)
{
	(void) fd;
	(void) offset;
	(void) nbytes;
	(void) flags;
	return 0;
}
