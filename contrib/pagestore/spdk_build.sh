#!/usr/bin/env bash
#
# Build pagestore_daemon_spdk: the optional SPDK frontend.  It reuses the shared
# brain (pagestore_core.c) and the SPDK storage backend (storage_spdk.c), and
# links against a local SPDK build.  The portable POSIX daemon is built by meson
# / the plain cc line and needs none of this.
#
# SPDK's static libraries must be wrapped in --whole-archive so the env and NVMe
# driver constructors register; DPDK ships as shared libs, so we rpath them in to
# avoid needing LD_LIBRARY_PATH at runtime.
#
# Usage:   contrib/pagestore/spdk_build.sh [output-path]
# Env:     SPDK_DIR (default ~/spdk)
#
set -euo pipefail

SPDK_DIR="${SPDK_DIR:-$HOME/spdk}"
out="${1:-pagestore_daemon_spdk}"
here="$(cd "$(dirname "$0")" && pwd)"

if [ ! -d "$SPDK_DIR/build/lib/pkgconfig" ]; then
	echo "SPDK build not found at $SPDK_DIR (set SPDK_DIR)."
	echo "Build it first: see contrib/pagestore/SPDK_NOTES.md"
	exit 1
fi

export PKG_CONFIG_PATH="$SPDK_DIR/build/lib/pkgconfig:$SPDK_DIR/dpdk/build/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"

cflags="$(pkg-config --cflags spdk_nvme spdk_env_dpdk)"
# SPDK libs need --whole-archive; spdk_syslibs carries the static system deps.
spdk_libs="-Wl,--whole-archive -Wl,--no-as-needed \
$(pkg-config --libs spdk_nvme spdk_env_dpdk) \
-Wl,--no-whole-archive $(pkg-config --libs --static spdk_syslibs)"
rpath="-Wl,-rpath,$SPDK_DIR/build/lib -Wl,-rpath,$SPDK_DIR/dpdk/build/lib"

set -x
cc -O2 -Wall -Wextra -DPAGESTORE_SPDK -I"$here" $cflags \
	-o "$out" \
	"$here/pagestore_daemon_spdk.c" "$here/pagestore_core.c" \
	"$here/storage_spdk.c" "$here/storage_posix.c" \
	"$here/pagestore_layer.c" "$here/pagestore_layer_store.c" \
	"$here/pagestore_manifest.c" \
	$rpath $spdk_libs -lrt
set +x
echo "built $out"
