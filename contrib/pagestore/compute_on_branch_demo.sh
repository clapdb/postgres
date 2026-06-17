#!/usr/bin/env bash
#
# compute_on_branch_demo.sh -- demonstrate a whole PostgreSQL database running
# on the pagestore, branched, with a single compute switching between the main
# timeline and a branch.
#
# This is a manual demo (not a CI test): it needs a full PostgreSQL build and
# runs real initdb / postgres.  Pass the meson build directory as $1.
#
#   contrib/pagestore/compute_on_branch_demo.sh /path/to/build
#
# What it shows:
#   - initdb a cluster, then import its entire relation set into the daemon
#     (timeline 0) so the whole database -- catalogs included -- lives on the
#     page store.
#   - start PostgreSQL with pagestore.route_all=on: every relation is read from
#     and written to the store.
#   - create a branch (instant, copy-on-write), then restart the *same* compute
#     onto the branch by changing pagestore.timeline.  The branch sees a clone
#     of the database; writes on the branch do not affect the main timeline.
#
# Scope / limitations: WAL, pg_control and SLRU (clog) are NOT branched -- they
# stay in the local PGDATA -- so this works for a single compute switching
# timelines across *clean* restarts, not for multiple concurrent computes.
# Independent concurrent computes on different branches need WAL shipping.
#
set -euo pipefail

BUILD=${1:?usage: compute_on_branch_demo.sh <meson-build-dir>}
SRC=$(cd "$(dirname "$0")/../.." && pwd)

# Locate the installed binaries under the meson tmp_install tree by finding
# pg_ctl, rather than hard-coding the install prefix (which varies by build).
PGCTL=$(find "$BUILD/tmp_install" -path '*/bin/pg_ctl' 2>/dev/null | head -1)
if [ -z "$PGCTL" ]; then
	echo "could not find pg_ctl under $BUILD/tmp_install (run 'meson install' / a build that stages tmp_install)" >&2
	exit 1
fi
BIN=$(dirname "$PGCTL")
ROOT=$(dirname "$BIN")
export LD_LIBRARY_PATH="$ROOT/lib64:$ROOT/lib"

DAEMON="$BUILD/contrib/pagestore/pagestore_daemon"
IMPORT="$BUILD/contrib/pagestore/pagestore_import"

DATA=$(mktemp -d)/pgdata
STORE=$(mktemp -d)/store
SHM=/pscob_$$
PORT=54450
P="$BIN/psql -p $PORT -U postgres -tA"

cleanup() {
	"$BIN/pg_ctl" -D "$DATA" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	rm -rf "$(dirname "$DATA")" "$(dirname "$STORE")"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

restart_tl() {  # $1 = timeline
	"$BIN/pg_ctl" -D "$DATA" -m fast -w stop >/dev/null 2>&1
	sed -i "s/^pagestore.timeline = .*/pagestore.timeline = $1/" "$DATA/postgresql.conf"
	"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
}

"$BIN/initdb" -D "$DATA" -U postgres -A trust >/dev/null 2>&1
echo "initdb done"

"$DAEMON" --shm "$SHM" --store "$STORE" >/dev/null 2>&1 &
DPID=$!
sleep 0.5

"$IMPORT" --shm "$SHM" --pgdata "$DATA" >/dev/null 2>&1
echo "whole database imported to page store (timeline 0)"

cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.route_all = on
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.timeline = 0
io_method = sync
port = $PORT
EOF

"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
echo "PostgreSQL running entirely on the page store (timeline 0)"

$P -c "CREATE FUNCTION pagestore_create_branch(int,int,pg_lsn) RETURNS void AS 'pagestore','pagestore_create_branch' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE TABLE demo(id int primary key, note text); INSERT INTO demo VALUES (1,'main-v1'); CHECKPOINT;" >/dev/null
$P -c "SELECT pagestore_create_branch(1, 0, pg_current_wal_lsn());" >/dev/null
echo "branch 1 created at the current LSN"

echo "  [1] timeline 0, initial          : $($P -c 'SELECT note FROM demo WHERE id=1;')   (expect main-v1)"
restart_tl 1
echo "  [2] switch to branch 1 (clone)   : $($P -c 'SELECT note FROM demo WHERE id=1;')   (expect main-v1)"
$P -c "UPDATE demo SET note='branch-v2' WHERE id=1; CHECKPOINT;" >/dev/null
echo "  [3] modify on branch 1           : $($P -c 'SELECT note FROM demo WHERE id=1;')   (expect branch-v2)"
restart_tl 0
echo "  [4] back to timeline 0 (isolated): $($P -c 'SELECT note FROM demo WHERE id=1;')   (expect main-v1)"
restart_tl 1
echo "  [5] branch 1 again (persisted)   : $($P -c 'SELECT note FROM demo WHERE id=1;')   (expect branch-v2)"

echo "compute-on-branch demo OK"
