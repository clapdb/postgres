#!/usr/bin/env bash
#
# branch_boot_test.sh -- end-to-end branch compute boot through the prepared
# branch manifest/install flow, under full routing.
#
# This is the successor to the old single-compute timeline-switch demo.  That
# shortcut edited pagestore.timeline in the same PGDATA, a path that branch
# startup now fails closed on.  Here the
# branch is a real second compute booted the supported way:
#
#   - a parent runs with route_all=on over an imported store (timeline 0),
#     shipping WAL and SLRU snapshots;
#   - pagestore_prepare_branch() seeds the branch SLRUs as of the fork LSN,
#     forks the store timeline, and publishes the durable manifest;
#   - the branch datadir is a copy of the cleanly-stopped parent, with the
#     prepared artifacts installed by pagestore_install_prepared_branch();
#   - the branch boots with pagestore.timeline=1, passes the fail-closed
#     manifest validation, sees exactly the fork-point data, and writes
#     forward on its own timeline isolated from the parent.
#
# Self-asserting: prints "ok"/"FAIL" lines and exits non-zero on any failure.
# Needs a full PostgreSQL build; pass the meson build directory as $1.
#
#   contrib/pagestore/branch_boot_test.sh /path/to/build
#
set -uo pipefail

BUILD=${1:?usage: branch_boot_test.sh <meson-build-dir>}
PGCTL=$(find "$BUILD/tmp_install" -path '*/bin/pg_ctl' -type f 2>/dev/null | head -1)
if [ -z "$PGCTL" ]; then
	echo "FAIL - no tmp_install found under $BUILD/tmp_install (run: meson test -C $BUILD --suite setup)"
	exit 1
fi
BIN=$(dirname "$PGCTL")
ROOT=$(dirname "$BIN")
export LD_LIBRARY_PATH="$ROOT/lib:$ROOT/lib64"
DAEMON="$BUILD/contrib/pagestore/pagestore_daemon"
IMPORT="$BUILD/contrib/pagestore/pagestore_import"

SOCKROOT=$(mktemp -d /tmp/psbboot-sock.XXXXXX)
new_sockdir() {
	mktemp -d "$SOCKROOT/$1.XXXXXX"
}

DATA=$(mktemp -d)/pgdata
STORE=$(mktemp -d)/store
SCRATCH=$(mktemp -d)/walredo
SOCKDIR=$(new_sockdir main)
BRANCHSOCK=$(new_sockdir branch)
SHM=/psbboot_$$

PORT=5432
P="$BIN/psql -h $SOCKDIR -p $PORT -U postgres -tA"
PB="$BIN/psql -h $BRANCHSOCK -p $PORT -U postgres -tA"
fail=0

assert() {  # $1=actual $2=expected $3=message
	if [ "$1" = "$2" ]; then
		echo "ok   - $3"
	else
		echo "FAIL - $3 (got '$1', want '$2')"
		fail=1
	fi
}

cleanup() {
	"$BIN/pg_ctl" -D "$DATA" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${BRANCHDATA:-}" ] && "$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1 || true
	if [ -n "${DPID:-}" ]; then
		kill "$DPID" 2>/dev/null || true
		wait "$DPID" 2>/dev/null || true
	fi
	rm -rf "$(dirname "$DATA")" "$(dirname "$STORE")" "$(dirname "$SCRATCH")" \
		"$BRANCHSOCK" "${BRANCHDATA:+$(dirname "$BRANCHDATA")}" "${PREP:+$(dirname "$PREP")}" \
		"${SCRATCH2:+$(dirname "$SCRATCH2")}" "$SOCKROOT"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

"$BIN/initdb" -D "$DATA" -U postgres -A trust >/dev/null 2>&1
"$BIN/initdb" -D "$SCRATCH" -U postgres -A trust >/dev/null 2>&1
"$DAEMON" --shm "$SHM" --store "$STORE" >/dev/null 2>&1 &
DPID=$!
sleep 0.5

# The whole database -- catalogs included -- must live on the store for a
# branch compute to be viable, so import the initdb'd cluster first.
"$IMPORT" --shm "$SHM" --pgdata "$DATA" >/dev/null 2>&1

cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_all = on
pagestore.walredo_datadir = '$SCRATCH'
pagestore.timeline = 0
io_method = sync
archive_mode = on
archive_library = 'pagestore'
listen_addresses = ''
unix_socket_directories = '$SOCKDIR'
port = $PORT
EOF
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1

$P -c "CREATE EXTENSION pagestore;
       CREATE FUNCTION pagestore_ship_slru_snapshot(text, pg_lsn) RETURNS bigint
        AS 'pagestore','pagestore_ship_slru_snapshot' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_prepare_branch(text, int, int, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_prepare_branch' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_install_prepared_branch(text, text, int, int, pg_lsn) RETURNS void
        AS 'pagestore','pagestore_install_prepared_branch' LANGUAGE C STRICT;
       CREATE TABLE demo(id int primary key, note text);" >/dev/null
$P -c "CHECKPOINT;" >/dev/null

# base cutoff C: ship the SLRU snapshots the prepare replay starts from
bc=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$bc');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_multixact/offsets', '$bc');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_multixact/members', '$bc');" >/dev/null

$P -c "INSERT INTO demo VALUES (1, 'before_fork');" >/dev/null   # commits in (C, L]
$P -c "CHECKPOINT;" >/dev/null                                   # ship the heap page
bL=$($P -c "SELECT pg_current_wal_lsn();")                       # fork LSN L
nxid=$($P -c "SELECT pg_snapshot_xmax(pg_current_snapshot());")

# prepare needs the (C, L] WAL: it replays from the parent's local pg_wal
PREP=$(mktemp -d)/prep
mkdir -p "$PREP"
seeded=$($P -c "SELECT pagestore_prepare_branch('$PREP', 1, 0, '$bc', '$bL',
	'3'::xid, '$nxid'::xid, '1'::xid, '1'::xid, '1'::xid, '1'::xid, 0, 0);")
assert "$([ "${seeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"branch prepared: SLRUs seeded as-of L and store timeline forked ($seeded page(s))"

# The branch datadir is a copy of the cleanly-stopped parent taken at the fork:
# its pg_control/WAL are the parent's at L, and the SLRUs get replaced by the
# prepared artifacts below.  Copy before the parent moves past L so the branch's
# local state matches the fork point.
"$BIN/pg_ctl" -D "$DATA" -m fast -w stop >/dev/null 2>&1
BRANCHDATA=$(mktemp -d)/branch
cp -a "$DATA" "$BRANCHDATA"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1

# parent moves past the fork: the branch must never see this row
$P -c "INSERT INTO demo VALUES (2, 'after_fork'); CHECKPOINT;" >/dev/null

$P -c "SELECT pagestore_install_prepared_branch('$PREP', '$BRANCHDATA', 1, 0, '$bL');" >/dev/null
assert "$([ -f "$BRANCHDATA/pagestore_branch.manifest" ] && echo present)" "present" \
	"prepared branch artifacts installed (manifest published last)"

# the branch is a second live compute: it needs its own wal-redo scratch
# cluster (the helper is a standalone backend holding its datadir's lock
# file, so two computes sharing the parent's scratch would collide)
SCRATCH2=$(mktemp -d)/walredo2
"$BIN/initdb" -D "$SCRATCH2" -U postgres -A trust >/dev/null 2>&1
cat >> "$BRANCHDATA/postgresql.conf" <<EOF
pagestore.timeline = 1
listen_addresses = ''
unix_socket_directories = '$BRANCHSOCK'
port = $PORT
archive_mode = off
pagestore.walredo_datadir = '$SCRATCH2'
EOF
if "$BIN/pg_ctl" -D "$BRANCHDATA" -l "$BRANCHDATA/server.log" -w start >/dev/null 2>&1; then
	echo "ok   - branch compute booted through manifest validation (route_all + timeline 1)"
else
	echo "FAIL - branch compute did not boot; log tail:"
	tail -5 "$BRANCHDATA/server.log" | sed 's/^/       /'
	fail=1
fi

assert "$($PB -c "SELECT note FROM demo WHERE id=1;" 2>/dev/null)" "before_fork" \
	"branch sees the row committed before the fork LSN"
assert "$($PB -c "SELECT count(*) FROM demo WHERE id=2;" 2>/dev/null)" "0" \
	"branch does NOT see the row the parent committed after the fork"
assert "$($PB -c "SELECT pagestore_shipped_wal_lsn();" 2>/dev/null)" "$bL" \
	"empty branch WAL inherits a durable boundary at the fork LSN"

# write forward on the branch timeline, force it to the store, evict buffers by
# restarting, and re-read: the write must have gone to timeline 1, not the parent
$PB -c "INSERT INTO demo VALUES (3, 'branch_local'); CHECKPOINT;" >/dev/null 2>&1
"$BIN/pg_ctl" -D "$BRANCHDATA" -w restart >/dev/null 2>&1
assert "$($PB -c "SELECT note FROM demo WHERE id=3;" 2>/dev/null)" "branch_local" \
	"branch write persisted on its own timeline (survives restart + buffer eviction)"
assert "$($PB -c "SELECT count(*) FROM demo WHERE id=2;" 2>/dev/null)" "0" \
	"post-fork parent row is physically absent on the branch timeline after re-read"
assert "$($P -c "SELECT count(*) FROM demo WHERE id=3;")" "0" \
	"parent is isolated from the branch's write"
assert "$($P -c "SELECT note FROM demo WHERE id=2;")" "after_fork" \
	"parent still sees its own post-fork row"

"$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1

echo "----"
[ "$fail" = 0 ] && echo "branch boot test: PASS" || echo "branch boot test: FAIL"
exit $fail
