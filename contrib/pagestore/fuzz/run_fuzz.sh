#!/usr/bin/env bash
# run_fuzz.sh -- run one or more persisted-format fuzz targets for a fixed
# wall-clock duration, in parallel (one process per target -- the targets
# do enough real filesystem/syscall work per iteration, see fuzz_common.c,
# that -fork=N workers on a single target does not help much; spreading
# across the 21 targets is where the 32 cores earn their keep).
#
# Usage:
#   run_fuzz.sh [-d seconds] [-o out-dir] [target ...]
# With no targets listed, runs every target in fuzz_common.c's table.
# Requires fuzz/build.sh to have been run first.
#
# Each target's working corpus starts as a copy of fuzz/corpus/<target>
# (never mutated in place) and any crash/leak/timeout artifact lands in
# <out-dir>/<target>/crashes/, named by libFuzzer's own content hash.
#
# Round 2 (coordinator review) changes from round 1:
#  - Leak detection is back ON (detect_leaks=1) with
#    lsan_suppressions.txt silencing only the one already-documented,
#    already-triaged leak (known-crashes/manifest/
#    leak_memtable_on_post_alloc_open_failure.txt) so a *new* leak still
#    stops the run and gets caught, instead of every run stopping on run #1.
#  - PS_FUZZ_CRC_FIXUP=1 by default (half of iterations get a structure-
#    aware checksum fixup after mutation -- see fuzz_crc_fixup.c -- the
#    other half stay pure mutation); pass -f 0 to disable for an apples-
#    to-apples round-1-style comparison run.
set -euo pipefail

FUZZ_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$FUZZ_DIR/build/pagestore_format_fuzz"
DURATION=720
OUT_DIR="$FUZZ_DIR/run"
CRC_FIXUP="${PS_FUZZ_CRC_FIXUP:-1}"

while getopts "d:o:f:" opt; do
  case "$opt" in
    d) DURATION="$OPTARG" ;;
    o) OUT_DIR="$OPTARG" ;;
    f) CRC_FIXUP="$OPTARG" ;;
    *) echo "usage: $0 [-d seconds] [-o out-dir] [-f 0|1] [target ...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

if [[ ! -x "$BIN" ]]; then
  echo "run_fuzz.sh: $BIN not found; run fuzz/build.sh first" >&2
  exit 1
fi

ALL_TARGETS=(
  manifest forkmeta forkmeta_snapshot_manifest forkmeta_snapshot_checkpoint
  forkmeta_snapshot_tail image_layer page_frontier page_segment
  retention_meta retention_state store_config timelines wal_log
  wal_store_identity wal_segment walidx_frontier walidx_log_epoch
  walidx_log_legacy walidx_watermark walidx_snapshot_manifest
  walidx_snapshot_shard
)
TARGETS=("$@")
if [[ ${#TARGETS[@]} -eq 0 ]]; then
  TARGETS=("${ALL_TARGETS[@]}")
fi

# Per-target -max_len: roughly 2x the largest seed, since a mutated file
# should be allowed to grow past what any fixture happened to record.
max_len_for() {
  case "$1" in
    image_layer) echo 786432 ;;
    wal_segment) echo 2097152 ;;
    page_segment) echo 131072 ;;
    page_frontier) echo 131072 ;;
    wal_log) echo 131072 ;;
    walidx_frontier) echo 65536 ;;
    *) echo 16384 ;;
  esac
}

mkdir -p "$OUT_DIR"
pids=()
for tgt in "${TARGETS[@]}"; do
  work="$OUT_DIR/$tgt"
  rm -rf "$work"
  mkdir -p "$work/crashes"
  cp -r "$FUZZ_DIR/corpus/$tgt" "$work/corpus"
  (
    cd "$work"
    PS_FUZZ_TARGET="$tgt" \
    PS_FUZZ_CRC_FIXUP="$CRC_FIXUP" \
    TMPDIR="${TMPDIR:-/tmp}" \
    ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:halt_on_error=1:allocator_may_return_null=1" \
    UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" \
    LSAN_OPTIONS="suppressions=$FUZZ_DIR/lsan_suppressions.txt" \
    "$BIN" -max_total_time="$DURATION" -max_len="$(max_len_for "$tgt")" \
      -rss_limit_mb=4096 -artifact_prefix=crashes/ \
      corpus/ > run.log 2>&1
    echo "exit_code=$?" >> run.log
  ) &
  pids+=($!)
  echo "started $tgt (pid $!) -> $work"
done

echo "waiting for ${#pids[@]} target(s), duration ${DURATION}s each..."
fail=0
for pid in "${pids[@]}"; do
  wait "$pid" || fail=1
done
echo "all targets finished (some background jobs may report nonzero exit on found crashes; see run.log per target)"
