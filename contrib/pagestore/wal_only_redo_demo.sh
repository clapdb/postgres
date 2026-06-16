#!/usr/bin/env bash
#
# wal_only_redo_demo.sh -- non-redundant WAL redo (milestone 3d, step 1).
#
# Unlike redo_worker_demo.sh (where the writer page-ships via route_all, so the
# store already has the pages and redo is redundant), here the writer ships
# ONLY WAL (route_all OFF -> its relation pages stay local, never reach the
# store).  A redo worker then materializes the relations into the store purely
# by replaying the shipped WAL -- so the store's pages come from redo, not from
# the compute.  This is the value of WAL redo: a stateless-ish write compute.
#
# Self-asserting; needs a full PostgreSQL build.  Pass the meson build dir as $1.
#
set -uo pipefail

BUILD=${1:?usage: wal_only_redo_demo.sh <meson-build-dir>}
PGCTL=$(find "$BUILD/tmp_install" -path '*/bin/pg_ctl' -type f 2>/dev/null | head -1)
[ -z "$PGCTL" ] && { echo "FAIL - no tmp_install"; exit 1; }
BIN=$(dirname "$PGCTL")
ROOT=$(dirname "$BIN")
export LD_LIBRARY_PATH="$ROOT/lib:$ROOT/lib64"
DAEMON="$BUILD/contrib/pagestore/pagestore_daemon"
IMPORT="$BUILD/contrib/pagestore/pagestore_import"
WALRESTORE="$BUILD/contrib/pagestore/pagestore_walrestore"

D=$(mktemp -d)/pgdata
S=$(mktemp -d)/store
SHM=/pswonly_$$
PORT=54480
P="$BIN/psql -h 127.0.0.1 -p $PORT -U postgres -tA"

cleanup() {
	"$BIN/pg_ctl" -D "$D" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	rm -rf "$(dirname "$D")" "$(dirname "$S")"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

store_size() { du -sb "$S" 2>/dev/null | cut -f1; }

mkdir -p "$S"
"$BIN/initdb" -D "$D" -U postgres -A trust >/dev/null 2>&1
"$DAEMON" --shm "$SHM" --store "$S" >/dev/null 2>&1 &
DPID=$!
sleep 0.5
"$IMPORT" --shm "$SHM" --pgdata "$D" >/dev/null 2>&1   # base = empty cluster

# writer: WAL-only.  route_all OFF -> relation pages stay local, only WAL ships.
cat >> "$D/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_all = off
io_method = sync
recovery_prefetch = off
archive_mode = on
archive_library = 'pagestore'
port = $PORT
EOF

"$BIN/pg_ctl" -D "$D" -l "$D/w.log" -w start >/dev/null 2>&1
# base backup right at the start (recovery start point), before any user table
"$BIN/psql" -h 127.0.0.1 -p $PORT -U postgres >/dev/null <<SQL
SELECT pg_backup_start('b', fast => true);
\a
\t on
\o $D/backup_label
SELECT labelfile FROM pg_backup_stop();
\o
SQL
# create the table and change it; its pages are written LOCALLY, never shipped
$P -c "CREATE TABLE t(id int primary key, v text); INSERT INTO t VALUES (1,'base');" >/dev/null
$P -c "UPDATE t SET v='changed' WHERE id=1;" >/dev/null
startseg=$(sed -n 's/.*(file \([0-9A-F]\{24\}\)).*/\1/p' "$D/backup_label")
for _ in $(seq 1 50); do
	$P -c "SELECT pg_switch_wal();" >/dev/null 2>&1
	[ -f "$D/pg_wal/archive_status/$startseg.done" ] && break
	sleep 0.2
done
# table t's relation file exists locally (the writer did not page-ship it)
relfile=$($P -c "SELECT pg_relation_filepath('t');")
[ -f "$D/$relfile" ] && echo "ok   - writer kept table pages local (no page-ship): $relfile" \
	|| { echo "FAIL - expected local relation file $relfile"; }
"$BIN/pg_ctl" -D "$D" -m fast -w stop >/dev/null 2>&1

before=$(store_size)

# redo worker: now route relations to the store and recover from shipped WAL.
sed -i 's/^pagestore.route_all = off/pagestore.route_all = on/' "$D/postgresql.conf"
touch "$D/recovery.signal"
echo "restore_command = '$WALRESTORE --shm $SHM --timeline 0 --segsize 16777216 %f %p'" >> "$D/postgresql.conf"
rm -f "$D"/pg_wal/0000000*

if "$BIN/pg_ctl" -D "$D" -l "$D/r.log" -w start >/dev/null 2>&1; then
	val=$($P -c "SELECT v FROM t WHERE id=1;" 2>/dev/null)
	after=$(store_size)
	if [ "$val" = "changed" ]; then
		echo "ok   - redo materialized table t into the store from WAL alone (v=changed)"
	else
		echo "FAIL - recovered value '$val', expected 'changed'"; exit 1
	fi
	if [ "${after:-0}" -gt "${before:-0}" ]; then
		echo "ok   - store grew via redo (${before} -> ${after} bytes; pages produced by redo, not the compute)"
	else
		echo "FAIL - store did not grow ($before -> $after); redo produced no pages"; exit 1
	fi
	echo "wal-only redo demo: PASS"
	exit 0
else
	echo "FAIL - redo worker did not start; log:"; tail -5 "$D/r.log" 2>/dev/null
fi
exit 1
