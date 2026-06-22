#!/usr/bin/env bash
#
# integration_test.sh -- in-engine (PostgreSQL + daemon) integration test for
# the pagestore module.  Unlike the standalone test, this exercises the real
# path: the smgr hijack, the localsvc backend talking to the daemon, the COW
# read_at SQL function, and WAL shipping via the archive module.
#
# Self-asserting: prints "ok"/"FAIL" lines and exits non-zero on any failure.
# Needs a full PostgreSQL build; pass the meson build directory as $1.
#
#   contrib/pagestore/integration_test.sh /path/to/build
#
set -uo pipefail

BUILD=${1:?usage: integration_test.sh <meson-build-dir>}
# Locate the temporary install tree robustly (its path depends on the configured
# prefix), by finding pg_ctl under tmp_install.
PGCTL=$(find "$BUILD/tmp_install" -path '*/bin/pg_ctl' -type f 2>/dev/null | head -1)
if [ -z "$PGCTL" ]; then
	echo "FAIL - no tmp_install found under $BUILD/tmp_install (run: meson test -C $BUILD --suite setup)"
	exit 1
fi
BIN=$(dirname "$PGCTL")
ROOT=$(dirname "$BIN")
export LD_LIBRARY_PATH="$ROOT/lib:$ROOT/lib64"
DAEMON="$BUILD/contrib/pagestore/pagestore_daemon"

DATA=$(mktemp -d)/pgdata
TS=$(mktemp -d)/ts
STORE=$(mktemp -d)/store
SHM=/psint_$$
PORT=54460
# connect over TCP: the server's unix-socket directory varies by build/distro,
# but -A trust allows 127.0.0.1, so TCP is portable across environments (CI).
P="$BIN/psql -h 127.0.0.1 -p $PORT -U postgres -tA"
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
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	rm -rf "$(dirname "$DATA")" "$(dirname "$TS")" "$(dirname "$STORE")"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

mkdir -p "$TS"
"$BIN/initdb" -D "$DATA" -U postgres -A trust >/dev/null 2>&1
"$DAEMON" --shm "$SHM" --store "$STORE" >/dev/null 2>&1 &
DPID=$!
sleep 0.5

cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_user_tablespaces = on
io_method = sync
archive_mode = on
archive_library = 'pagestore'
port = $PORT
EOF
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1

# --- 1. localsvc round-trip: a routed table's I/O goes to the daemon --------
$P -c "CREATE TABLESPACE ts LOCATION '$TS';" >/dev/null
$P -c "CREATE TABLE t(id int primary key, v text) TABLESPACE ts;
       INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1,20000) g;
       CHECKPOINT;" >/dev/null
ck1=$($P -c "SELECT md5(string_agg(v,',' ORDER BY id)) FROM t;")
nfiles=$(find "$TS" -type f | wc -l | tr -d ' ')
assert "$nfiles" "0" "routed tablespace has no local relation files (I/O went to daemon)"

"$BIN/pg_ctl" -D "$DATA" -w restart >/dev/null 2>&1   # evict shared_buffers
ck2=$($P -c "SELECT md5(string_agg(v,',' ORDER BY id)) FROM t;")
assert "$ck2" "$ck1" "data intact after restart (read back from daemon)"
assert "$($P -c 'SELECT count(*) FROM t;')" "20000" "row count after restart"

# --- 2. copy-on-write time-travel read -------------------------------------
$P -c "CREATE FUNCTION pagestore_read_at(regclass,int,int,pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_read_at' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE TABLE c(id int, note text) TABLESPACE ts;
       INSERT INTO c VALUES (1,'cow_old'); CHECKPOINT;" >/dev/null
l1=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "UPDATE c SET note='cow_new' WHERE id=1; CHECKPOINT;" >/dev/null
has_old=$($P -c "SELECT position('cow_old'::bytea in pagestore_read_at('c',0,0,'$l1'::pg_lsn))>0;")
has_new=$($P -c "SELECT position('cow_new'::bytea in pagestore_read_at('c',0,0,'$l1'::pg_lsn))=0;")
cur_new=$($P -c "SELECT position('cow_new'::bytea in pagestore_read_at('c',0,0,'FFFFFFFF/FFFFFFFF'))>0;")
assert "$has_old" "t" "as-of read returns the pre-update page (COW retained)"
assert "$has_new" "t" "as-of read does not contain the post-update value"
assert "$cur_new" "t" "current page contains the new value"

# --- 3. WAL shipping: completed segments reach the daemon ------------------
$P -c "CREATE TABLE wgen(x text) TABLESPACE ts;" >/dev/null
for i in 1 2; do
	$P -c "INSERT INTO wgen SELECT repeat('w',100) FROM generate_series(1,50000);
	       SELECT pg_switch_wal();" >/dev/null
done
sleep 2
walsz=$(stat -c %s "$STORE/wal_0" 2>/dev/null || echo 0)
if [ "$walsz" -gt 0 ]; then echo "ok   - WAL shipped to daemon (wal_0 = $walsz bytes)"; else echo "FAIL - no WAL shipped"; fail=1; fi

# --- 4. reconstruct a standard WAL segment from the store (redo step 3a) ----
seg=$(basename "$(ls "$DATA"/pg_wal/archive_status/*.done 2>/dev/null | head -1)" .done)
out=$(mktemp)
if [ -n "$seg" ] && "$BUILD/contrib/pagestore/pagestore_walrestore" \
		--shm "$SHM" --timeline 0 --segsize 16777216 "$seg" "$out"; then
	assert "$(stat -c %s "$out")" "16777216" "restored WAL segment $seg is a full standard segment"
else
	echo "FAIL - walrestore could not reconstruct segment '$seg'"
	fail=1
fi
rm -f "$out"

# --- 5. per-page WAL index: decode WAL (reusing PG's reader) and query it ---
$P -c "CREATE FUNCTION pagestore_index_wal(pg_lsn,pg_lsn) RETURNS void
        AS 'pagestore','pagestore_index_wal' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE FUNCTION pagestore_walidx_count(regclass,int,int) RETURNS int
        AS 'pagestore','pagestore_walidx_count' LANGUAGE C STRICT;" >/dev/null
# write a fresh table and decode just-written WAL (still present in pg_wal)
lsn0=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "CREATE TABLE widx(id int) TABLESPACE ts; INSERT INTO widx SELECT generate_series(1,1000);" >/dev/null
lsn1=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_index_wal('$lsn0', '$lsn1');" >/dev/null
widx=$($P -c "SELECT pagestore_walidx_count('widx', 0, 0);")
if [ "${widx:-0}" -gt 0 ]; then
	echo "ok   - per-page WAL index built by decoding WAL (widx block 0 has $widx records)"
else
	echo "FAIL - per-page WAL index empty for widx block 0 (got '$widx')"
	fail=1
fi

# --- 6. base page image reconstructed from a WAL full-page image (redo 3c-3) -
# pagestore_redo_page returns the newest full-page image <= lsn -- the base a
# single-page redo would then apply deltas onto.  (Applying the deltas needs
# rm_redo; that wal-redo helper is the remaining step.)
$P -c "CREATE FUNCTION pagestore_redo_page(regclass,int,int,pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_redo_page' LANGUAGE C STRICT;" >/dev/null
rlsn0=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "CREATE TABLE rp(id int primary key, v text) TABLESPACE ts;
       INSERT INTO rp VALUES (1,'rp_committed'); CHECKPOINT;" >/dev/null
# first modify after the checkpoint logs a full-page image of rp's block 0
$P -c "UPDATE rp SET v='rp_later' WHERE id=1;" >/dev/null
rlsn1=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_index_wal('$rlsn0', '$rlsn1');" >/dev/null
# reconstruct rp's block 0 image from WAL alone; it carries the committed row
rebuilt=$($P -c "SELECT position('rp_committed'::bytea in pagestore_redo_page('rp',0,0,'$rlsn1')) > 0;")
if [ "$rebuilt" = "t" ]; then
	echo "ok   - base page image reconstructed from a WAL full-page image"
else
	echo "FAIL - could not reconstruct page image from WAL FPI (got '$rebuilt')"
	fail=1
fi

# the base image's end LSN (what a single-page redo seeds the held page with):
# it must be a real LSN <= the query LSN, and the page is the page as-of that LSN.
$P -c "CREATE FUNCTION pagestore_redo_base_lsn(regclass,int,int,pg_lsn) RETURNS pg_lsn
        AS 'pagestore','pagestore_redo_base_lsn' LANGUAGE C STRICT;" >/dev/null
blsn=$($P -tAc "SELECT pagestore_redo_base_lsn('rp',0,0,'$rlsn1');")
in_range=$($P -tAc "SELECT '$blsn'::pg_lsn > '0/0'::pg_lsn AND '$blsn'::pg_lsn <= '$rlsn1'::pg_lsn;")
if [ "$in_range" = "t" ]; then
	echo "ok   - base image end LSN reported ($blsn) for single-page redo seeding"
else
	echo "FAIL - base image end LSN out of range (got '$blsn', query '$rlsn1')"
	fail=1
fi

echo "----"
[ "$fail" = 0 ] && echo "integration test: PASS" || echo "integration test: FAIL"
exit $fail
