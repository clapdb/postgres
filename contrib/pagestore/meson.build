# Copyright (c) 2022-2025, PostgreSQL Global Development Group

pagestore_sources = files(
  'backend_localsvc.c',
  'backend_passthrough.c',
  'pagestore.c',
)

if host_system == 'windows'
  pagestore_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pagestore',
    '--FILEDESC', 'pagestore - pluggable storage backend smgr shim',])
endif

pagestore = shared_module('pagestore',
  pagestore_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pagestore

# Standalone storage daemon for the "localsvc" backend.  Freestanding: it
# includes only pagestore_ipc.h and libc, no PostgreSQL libraries.
pagestore_daemon = executable('pagestore_daemon',
  files('pagestore_daemon.c', 'pagestore_core.c', 'storage_posix.c'),
  kwargs: default_bin_args,
)
contrib_targets += pagestore_daemon

# Import an existing PGDATA's relation files into the daemon, so a whole
# database can be moved onto the page store.  Freestanding like the daemon.
pagestore_import = executable('pagestore_import',
  files('pagestore_import.c'),
  kwargs: default_bin_args,
)
contrib_targets += pagestore_import

# Reconstruct a standard WAL segment from the store's shipped WAL; usable as a
# restore_command so a recovery instance can replay store-resident WAL.
pagestore_walrestore = executable('pagestore_walrestore',
  files('pagestore_walrestore.c'),
  kwargs: default_bin_args,
)
contrib_targets += pagestore_walrestore

# Standalone test harness: drives the daemon over the shared-memory protocol
# with no PostgreSQL instance.  Run independently of the PG test suites with:
#   meson test -C <build> --suite pagestore
pagestore_test = executable('pagestore_test',
  files('pagestore_test.c'),
  install: false,
)
test('pagestore_standalone',
  pagestore_test,
  args: [pagestore_daemon],
  depends: [pagestore_daemon],
  suite: ['pagestore'],
  timeout: 120,
)
