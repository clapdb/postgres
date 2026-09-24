#!/usr/bin/env bash
# build.sh -- opt-in build for the pagestore persisted-format fuzz targets.
#
# This never runs as part of the ordinary meson/make build: it is a
# standalone script you invoke by hand (or from CI's fuzz lane) to build
# the libFuzzer-instrumented binary and its non-instrumented replay
# counterpart.  See fuzz_common.c for what is fuzzed and why, and
# run_fuzz.sh for how to actually run it.
#
# Requires a clang with libFuzzer support (clang -fsanitize=fuzzer,address,
# undefined must link).  Usage:
#   contrib/pagestore/fuzz/build.sh [output-dir]
set -euo pipefail

FUZZ_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "$FUZZ_DIR/.." && pwd)"
OUT_DIR="${1:-$FUZZ_DIR/build}"
CC="${CC:-clang}"

mkdir -p "$OUT_DIR"

# The store core is freestanding by design (pagestore_daemon's own comment:
# "includes only pagestore_ipc.h and libc, no PostgreSQL libraries"), which
# is what makes it possible to link it straight into a libFuzzer binary.
# This is the same file list pagestore_daemon/pagestore_format_versions use
# in meson.build.
CORE_SOURCES=(
  pagestore_core.c
  storage_posix.c
  pagestore_layer.c
  pagestore_store_owner.c
  pagestore_layer_store.c
  pagestore_manifest.c
  pagestore_memtable.c
  pagestore_pgcache.c
  pagestore_prune.c
  pagestore_retention.c
  pagestore_wal_store.c
  pagestore_wal_segment.c
  pagestore_walidx_prune.c
  pagestore_walidx_snapshot.c
  pagestore_forkmeta_prune.c
  pagestore_forkmeta_snapshot.c
  pagestore_fault.c
)

SRCS=()
for f in "${CORE_SOURCES[@]}"; do
  SRCS+=("$SRC_DIR/$f")
done

FIXTURE_TGZ="$SRC_DIR/fixtures/posix-mvp-baseline/store.tar.gz"
if [[ ! -f "$FIXTURE_TGZ" ]]; then
  echo "build.sh: missing fixture $FIXTURE_TGZ" >&2
  exit 1
fi

COMMON_ARGS=(
  -g -O1
  -I"$SRC_DIR"
  -DPAGESTORE_ASSERT_CHECKING
  -DPS_FUZZ_FIXTURE_TGZ="\"$FIXTURE_TGZ\""
  -pthread
)

echo "== building instrumented fuzz binary (clang -fsanitize=fuzzer,address,undefined) =="
"$CC" "${COMMON_ARGS[@]}" \
  -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all \
  "${SRCS[@]}" "$FUZZ_DIR/fuzz_common.c" "$FUZZ_DIR/fuzz_target.c" \
  -o "$OUT_DIR/pagestore_format_fuzz"

echo "== building non-instrumented replay driver (plain $CC, no libFuzzer/ASan/UBSan) =="
"$CC" "${COMMON_ARGS[@]}" \
  "${SRCS[@]}" "$FUZZ_DIR/fuzz_common.c" "$FUZZ_DIR/fuzz_standalone_driver.c" \
  -o "$OUT_DIR/pagestore_format_fuzz_replay"

echo "built: $OUT_DIR/pagestore_format_fuzz"
echo "built: $OUT_DIR/pagestore_format_fuzz_replay"
