#!/usr/bin/env bash
#
# redo_worker_demo.sh -- demonstrate WAL redo in the store (milestone 3b).
#
# A PostgreSQL instance performs archive recovery with its WAL fetched entirely
# from the page store (restore_command = pagestore_walrestore) and its relations
# routed to the store (route_all).  Recovery's own rm_redo reads base pages from
# the store, replays the shipped WAL, and writes the resulting pages back to the
# store -- reusing PostgreSQL's redo wholesale, no rmgr reimplementation.
#
# Self-asserting; needs a full PostgreSQL build.  Pass the meson build dir as $1.
#   contrib/pagestore/redo_worker_demo.sh /path/to/build
#
# Limitation: the redo instance runs with recovery_prefetch=off (the backend's
# recovery-prefetch/AIO path is not wired yet).  See WAL_REDO.md.
#
set -uo pipefail

BUILD=${1:?usage: redo_worker_demo.sh <meson-build-dir>}
PGCTL=$(find "$BUILD/tmp_install" -path '*/bin/pg_ctl' -type f 2>/dev/null | head -1)
[ -z "$PGCTL" ] && { echo "FAIL - no tmp_install (run: meson test -C $BUILD --suite setup)"; exit 1; }
BIN=$(dirname "$PGCTL")
ROOT=$(dirname "$BIN")
export LD_LIBRARY_PATH="$ROOT/lib:$ROOT/lib64"
DAEMON="$BUILD/contrib/pagestore/pagestore_daemon"
IMPORT="$BUILD/contrib/pagestore/pagestore_import"
WALRESTORE="$BUILD/contrib/pagestore/pagestore_walrestore"

D=$(mktemp -d)/pgdata
S=$(mktemp -d)/store
SHM=/psredo_$$
PORT=54470
P="$BIN/psql -h 127.0.0.1 -p $PORT -U postgres -tA"

cleanup() {
	"$BIN/pg_ctl" -D "$D" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	rm -rf "$(dirname "$D")" "$(dirname "$S")"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

mkdir -p "$S"
"$BIN/initdb" -D "$D" -U postgres -A trust >/dev/null 2>&1
"$DAEMON" --shm "$SHM" --store "$S" >/dev/null 2>&1 &
DPID=$!
sleep 0.5
"$IMPORT" --shm "$SHM" --pgdata "$D" >/dev/null 2>&1

cat >> "$D/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.route_all = on
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
io_method = sync
recovery_prefetch = off
archive_mode = on
archive_library = 'pagestore'
port = $PORT
EOF

# 1) writer: a row, a base backup (recovery start point), then a change shipped
"$BIN/pg_ctl" -D "$D" -l "$D/w.log" -w start >/dev/null 2>&1
$P -c "CREATE TABLE t(id int primary key, v text); INSERT INTO t VALUES (1,'base');" >/dev/null
# write the backup label straight into PGDATA (unquoted heredoc expands $D)
"$BIN/psql" -h 127.0.0.1 -p $PORT -U postgres >/dev/null <<SQL
SELECT pg_backup_start('b', fast => true);
\a
\t on
\o $D/backup_label
SELECT labelfile FROM pg_backup_stop();
\o
SQL
$P -c "UPDATE t SET v='changed' WHERE id=1;" >/dev/null

# The UPDATE's WAL must reach the store before recovery, and archiving is
# asynchronous.  Capture the segment the UPDATE landed in, force it to complete,
# and wait until *that* segment is archived -- which implies the backup-start
# segment and everything between are too (the archiver ships in order).  Waiting
# only for the backup-start segment races: the UPDATE is in a later segment that
# may not be shipped yet (this is what intermittently failed in CI).
updseg=$($P -c "SELECT pg_walfile_name(pg_current_wal_lsn());")
for _ in $(seq 1 150); do
	$P -c "SELECT pg_switch_wal();" >/dev/null 2>&1
	last=$($P -c "SELECT last_archived_wal FROM pg_stat_archiver;" 2>/dev/null)
	[[ -n "$last" && ! "$last" < "$updseg" ]] && break	# archived through updseg
	sleep 0.2
done
"$BIN/pg_ctl" -D "$D" -m fast -w stop >/dev/null 2>&1

# 2) turn the instance into a redo worker: recover from the store's WAL only
# (backup_label is already in PGDATA from pg_backup_stop above)
touch "$D/recovery.signal"
echo "restore_command = '$WALRESTORE --shm $SHM --timeline 0 --segsize 16777216 %f %p'" >> "$D/postgresql.conf"
rm -f "$D"/pg_wal/0000000*	# remove local WAL: recovery must fetch from the store

if "$BIN/pg_ctl" -D "$D" -l "$D/r.log" -w start >/dev/null 2>&1; then
	# pg_ctl -w returns once the server accepts connections, which (with hot
	# standby) is during recovery -- so retry the read until replay has caught up
	# to the shipped UPDATE rather than racing ahead of it.
	val=
	for _ in $(seq 1 100); do
		val=$($P -c "SELECT v FROM t WHERE id=1;" 2>/dev/null)
		[ "$val" = "changed" ] && break
		sleep 0.2
	done
	if [ "$val" = "changed" ]; then
		echo "ok   - redo worker recovered 'changed' from store-resident WAL"
		grep -q "restored log file" "$D/r.log" && \
			echo "ok   - WAL was fetched from the store via pagestore_walrestore" || \
			{ echo "FAIL - restore_command was not used"; exit 1; }
		echo "redo worker demo: PASS"
		exit 0
	fi
	echo "FAIL - recovered value is '$val', expected 'changed'"
else
	echo "FAIL - redo worker did not start; recovery log:"
	tail -5 "$D/r.log" 2>/dev/null
fi
exit 1
