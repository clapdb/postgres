# Copyright (c) 2022-2025, PostgreSQL Global Development Group

pagestore_sources = files(
  'backend_localsvc.c',
  'backend_passthrough.c',
  'pagestore.c',
  'walredo_client.c',
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
  files('pagestore_daemon.c', 'pagestore_core.c', 'storage_posix.c',
        'pagestore_layer.c', 'pagestore_layer_store.c', 'pagestore_manifest.c',
        'pagestore_memtable.c', 'pagestore_pgcache.c'),
  dependencies: [dependency('threads')],
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

# Throughput probe: forks a daemon, writes a dataset, times reading it back.
# Indicative only (see pagestore_bench.c); freestanding like the daemon.
pagestore_bench = executable('pagestore_bench',
  files('pagestore_bench.c'),
  kwargs: default_bin_args,
)
contrib_targets += pagestore_bench

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

# Unit test for the immutable image-layer file format (no daemon, no PG).
pagestore_layer_test = executable('pagestore_layer_test',
  files('pagestore_layer_test.c', 'pagestore_layer.c', 'pagestore_layer_store.c'),
  install: false,
)
test('pagestore_layer',
  pagestore_layer_test,
  suite: ['pagestore'],
  timeout: 60,
)

# Unit test for the durable layer manifest replay (no daemon, no PG).
pagestore_manifest_test = executable('pagestore_manifest_test',
  files('pagestore_manifest_test.c', 'pagestore_manifest.c', 'pagestore_layer.c',
        'pagestore_layer_store.c'),
  install: false,
)
test('pagestore_manifest',
  pagestore_manifest_test,
  suite: ['pagestore'],
  timeout: 60,
)

# Unit test for crash-safe compaction/GC recovery (no daemon, no PG).
pagestore_gc_test = executable('pagestore_gc_test',
  files('pagestore_gc_test.c', 'pagestore_manifest.c', 'pagestore_layer.c',
        'pagestore_layer_store.c'),
  install: false,
)
test('pagestore_gc',
  pagestore_gc_test,
  suite: ['pagestore'],
  timeout: 60,
)

# Unit test for the scan-resistant materialized-page cache (no daemon, no PG).
pagestore_pgcache_test = executable('pagestore_pgcache_test',
  files('pagestore_pgcache_test.c', 'pagestore_pgcache.c'),
  install: false,
)
test('pagestore_pgcache',
  pagestore_pgcache_test,
  suite: ['pagestore'],
  timeout: 60,
)
