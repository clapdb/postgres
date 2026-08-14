# Copyright (c) 2022-2025, PostgreSQL Global Development Group

pagestore_sources = files(
  'backend_localsvc.c',
  'backend_passthrough.c',
  'pagestore.c',
  'pagestore_control.c',
  'pagestore_slru.c',
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

install_data(
  'pagestore.control',
  'pagestore--1.0.sql',
  'pagestore--1.0--1.1.sql',
  'pagestore--1.1.sql',
  kwargs: contrib_data_args,
)

if host_system != 'windows'
  # The recovery worker is an ordinary PostgreSQL process; this small
  # stdlib-only POSIX control-plane process owns its lifecycle and publishes
  # machine-readable state.
  install_data(
    'pagestore_materializer_supervisor.py',
    install_dir: dir_bin,
    install_mode: 'rwxr-xr-x',
    rename: 'pagestore_materializer_supervisor',
  )
  # This one-shot controller owns the writer/materializer correctness window
  # while selecting and preparing a branch boundary.
  install_data(
    'pagestore_branch_prepare.py',
    install_dir: dir_bin,
    install_mode: 'rwxr-xr-x',
    rename: 'pagestore_branch_prepare',
  )
endif

# Standalone storage daemon for the "localsvc" backend.  Freestanding: it
# includes only pagestore_ipc.h and libc, no PostgreSQL libraries.
pagestore_daemon = executable('pagestore_daemon',
  files('pagestore_daemon.c', 'pagestore_core.c', 'storage_posix.c',
        'pagestore_layer.c', 'pagestore_layer_store.c', 'pagestore_manifest.c',
        'pagestore_memtable.c', 'pagestore_pgcache.c', 'pagestore_prune.c',
        'pagestore_retention.c', 'pagestore_wal_store.c',
        'pagestore_wal_segment.c'),
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

# Restore global/pg_control from the store's mirrored control image, as of a
# branch's fork LSN (PGCONTROL_ON_STORE_DESIGN.md restore protocol).  A
# frontend program: it CRC-verifies ControlFileData via libpgport.
pagestore_control_restore = executable('pagestore_control_restore',
  files('pagestore_control_restore.c'),
  dependencies: [frontend_code],
  kwargs: default_bin_args,
)
contrib_targets += pagestore_control_restore

# Throughput probe: forks a daemon, writes a dataset, times reading it back.
# Indicative only (see pagestore_bench.c); freestanding like the daemon.
pagestore_bench = executable('pagestore_bench',
  files('pagestore_bench.c'),
  kwargs: default_bin_args,
)
contrib_targets += pagestore_bench

# Private, read-only shared-memory inspector for the pagestore harness.  It
# deliberately does not link PostgreSQL libraries or expose mutation commands.
pagestore_inspect = executable('pagestore_inspect',
  files('pagestore_inspect.c'),
  kwargs: default_bin_args,
)
contrib_targets += pagestore_inspect

# Standalone test harness: drives the daemon over the shared-memory protocol
# with no PostgreSQL instance.  Run independently of the PG test suites with:
#   meson test -C <build> --suite pagestore
pagestore_test = executable('pagestore_test',
  files('pagestore_test.c'),
  install: false,
)
test('pagestore_standalone',
  pagestore_test,
  args: [pagestore_daemon, pagestore_inspect],
  depends: [pagestore_daemon, pagestore_inspect],
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

# Unit test for the filesystem-backed remote object provider (no daemon, no PG).
pagestore_layer_store_test = executable('pagestore_layer_store_test',
  files('pagestore_layer_store_test.c', 'pagestore_layer_store.c', 'pagestore_layer.c'),
  install: false,
)
test('pagestore_layer_store',
  pagestore_layer_store_test,
  suite: ['pagestore'],
  timeout: 60,
)

# Integration test for idle layer upload and durable remote manifest state.
pagestore_tiering_test = executable('pagestore_tiering_test',
  files('pagestore_tiering_test.c', 'pagestore_core.c', 'storage_posix.c',
        'pagestore_layer.c', 'pagestore_layer_store.c', 'pagestore_manifest.c',
        'pagestore_memtable.c', 'pagestore_pgcache.c', 'pagestore_prune.c',
        'pagestore_retention.c', 'pagestore_wal_store.c',
        'pagestore_wal_segment.c'),
  dependencies: [dependency('threads')],
  install: false,
)
test('pagestore_tiering',
  pagestore_tiering_test,
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

# Unit test for durable retention pins, replay and churn compaction.
pagestore_retention_test = executable('pagestore_retention_test',
  files('pagestore_retention_test.c', 'pagestore_retention.c'),
  dependencies: [dependency('threads')],
  install: false,
)
test('pagestore_retention',
  pagestore_retention_test,
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

# Pure keep/drop rule for retention-driven image compaction.
pagestore_prune_test = executable('pagestore_prune_test',
  files('pagestore_prune_test.c', 'pagestore_prune.c'),
  install: false,
)
test('pagestore_prune',
  pagestore_prune_test,
  suite: ['pagestore'],
  timeout: 30,
)

# Persisted immutable shipped-WAL segment header and checksum contract.
pagestore_wal_segment_test = executable('pagestore_wal_segment_test',
  files('pagestore_wal_segment_test.c', 'pagestore_wal_segment.c'),
  install: false,
)
test('pagestore_wal_segment',
  pagestore_wal_segment_test,
  suite: ['pagestore'],
  timeout: 30,
)

# Atomic immutable segment publication and cross-segment shipped-WAL reads.
pagestore_wal_store_test = executable('pagestore_wal_store_test',
  files('pagestore_wal_store_test.c', 'pagestore_wal_store.c',
        'pagestore_wal_segment.c'),
  install: false,
)
test('pagestore_wal_store',
  pagestore_wal_store_test,
  suite: ['pagestore'],
  timeout: 30,
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

# The harness input contract is stdlib-only and intentionally fast.  Keep it in
# the regular pagestore suite so scenario/capability changes fail before the
# slower PostgreSQL integration coverage runs.
pagestore_harness_python = find_program('python3')
test('pagestore_harness_plan',
  pagestore_harness_python,
  args: [files('harness/tests/test_plan.py')],
  suite: ['pagestore'],
  timeout: 30,
)
if host_system != 'windows'
  test('pagestore_materializer_supervisor',
    pagestore_harness_python,
    args: [files('harness/tests/test_materializer_supervisor.py')],
    suite: ['pagestore'],
    timeout: 30,
  )
  test('pagestore_branch_prepare',
    pagestore_harness_python,
    args: [files('harness/tests/test_branch_prepare.py')],
    suite: ['pagestore'],
    timeout: 30,
  )
endif
test('pagestore_harness_reader_visibility_plan',
  pagestore_harness_python,
  args: [files('harness/pagestore_harness.py'),
         '--capabilities', files('harness/capabilities.json'),
         '--validate', files('harness/scenarios/reader_visibility.jsonl')],
  suite: ['pagestore'],
  timeout: 30,
)
test('pagestore_harness_writer_lifecycle_plan',
  pagestore_harness_python,
  args: [files('harness/pagestore_harness.py'),
         '--capabilities', files('harness/capabilities.json'),
         '--validate', files('harness/scenarios/writer_lifecycle.jsonl')],
  suite: ['pagestore'],
  timeout: 30,
)
test('pagestore_harness_materializer_lifecycle_plan',
  pagestore_harness_python,
  args: [files('harness/pagestore_harness.py'),
         '--capabilities', files('harness/capabilities.json'),
         '--validate', files('harness/scenarios/materializer_lifecycle.jsonl')],
  suite: ['pagestore'],
  timeout: 30,
)
test('pagestore_harness_daemon_smoke',
  pagestore_harness_python,
  args: [files('harness/pagestore_harness.py'),
         '--capabilities', files('harness/capabilities.json'),
         '--daemon-smoke', files('harness/scenarios/daemon_readiness.jsonl'),
         '--daemon-binary', pagestore_daemon,
         '--inspect-binary', pagestore_inspect],
  depends: [pagestore_daemon, pagestore_inspect],
  suite: ['pagestore'],
  timeout: 30,
)
test('pagestore_harness_daemon_recovery',
  pagestore_harness_python,
  args: [files('harness/pagestore_harness.py'),
         '--capabilities', files('harness/capabilities.json'),
         '--daemon-smoke', files('harness/scenarios/daemon_recovery.jsonl'),
         '--daemon-binary', pagestore_daemon,
         '--inspect-binary', pagestore_inspect],
  depends: [pagestore_daemon, pagestore_inspect],
  suite: ['pagestore'],
  timeout: 30,
)
