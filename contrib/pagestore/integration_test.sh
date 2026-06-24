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
SCRATCH=$(mktemp -d)/walredo	# private throwaway cluster for the wal-redo helper
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
	rm -rf "$(dirname "$DATA")" "$(dirname "$TS")" "$(dirname "$STORE")" \
		"$(dirname "$SCRATCH")"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

mkdir -p "$TS"
"$BIN/initdb" -D "$DATA" -U postgres -A trust >/dev/null 2>&1
"$BIN/initdb" -D "$SCRATCH" -U postgres -A trust >/dev/null 2>&1
"$DAEMON" --shm "$SHM" --store "$STORE" >/dev/null 2>&1 &
DPID=$!
sleep 0.5

cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_user_tablespaces = on
pagestore.walredo_datadir = '$SCRATCH'
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

# --- 7. full single-page redo: materialize a page as of an LSN (redo_page_asof) -
# The base full-page image plus every WAL record after it, replayed through the
# `postgres --wal-redo` helper (rm_redo), must reproduce the live page.
$P -c "CREATE FUNCTION pagestore_redo_page_asof(regclass,int,int,pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_redo_page_asof' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE EXTENSION IF NOT EXISTS pageinspect;" >/dev/null
# A fresh insert-only table: checkpoint then two more inserts give block 0 a
# full-page image (first change after the checkpoint) followed by a pure delta.
# (Insert-only avoids the hint-bit/pruning divergence that makes an updated
# heap page's live image differ cosmetically from a WAL reconstruction.)
a0=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "CREATE TABLE rpa(id int primary key, v text) WITH (autovacuum_enabled=off);
       INSERT INTO rpa VALUES (1,'asof_one');
       CHECKPOINT;
       INSERT INTO rpa VALUES (2,'asof_two');
       INSERT INTO rpa VALUES (3,'asof_three');" >/dev/null
alsn=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_index_wal('$a0', '$alsn');" >/dev/null
# The reconstruction must carry the base full-page image's rows (asof_one,
# asof_two) AND the delta applied after it (asof_three) -- i.e. it really replayed
# base + deltas through rm_redo, not just returned the base.  (We assert content
# rather than a byte-for-byte match: an in-cluster heap page also carries hint
# bits a WAL reconstruction legitimately does not.)
asof_all=$($P -c "SELECT position('asof_one'::bytea   in pagestore_redo_page_asof('rpa',0,0,'$alsn')) > 0
				  AND position('asof_two'::bytea   in pagestore_redo_page_asof('rpa',0,0,'$alsn')) > 0
				  AND position('asof_three'::bytea in pagestore_redo_page_asof('rpa',0,0,'$alsn')) > 0;")
assert "$asof_all" "t" "page materialized as of LSN (base FPI + deltas via rm_redo) has all rows"
# the base image alone would lack the post-FPI delta; confirm it was applied
asof_base=$($P -c "SELECT position('asof_three'::bytea in pagestore_redo_page('rpa',0,0,'$alsn')) > 0;")
assert "$asof_base" "f" "the base FPI alone lacks the delta (so the match above came from redo)"

# --- 8. non-relation object on the store via the PsKey klass discriminator -----
# A non-relation object (klass != RELATION) rides the same store path as a
# relation page, distinguished only by klass; objects of different klass with the
# same id are distinct keys.
$P -c "CREATE FUNCTION pagestore_object_roundtrip(int,int,bytea) RETURNS bytea
        AS 'pagestore','pagestore_object_roundtrip' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE FUNCTION pagestore_object_get(int,int) RETURNS bytea
        AS 'pagestore','pagestore_object_get' LANGUAGE C STRICT;" >/dev/null
# round-trip an SLRU-class object (klass=1) and a relation-class one (klass=0),
# both with id 42 but distinct page content
rt1=$($P -c "SELECT pagestore_object_roundtrip(1, 42, repeat('A',8192)::bytea) = repeat('A',8192)::bytea;")
assert "$rt1" "t" "non-relation (SLRU-class) object round-trips through the store"
rt0=$($P -c "SELECT pagestore_object_roundtrip(0, 42, repeat('B',8192)::bytea) = repeat('B',8192)::bytea;")
assert "$rt0" "t" "relation-class object with the same id round-trips"
# klass isolation: the klass=0 write to id 42 must not have clobbered klass=1
iso=$($P -c "SELECT pagestore_object_get(1, 42) = repeat('A',8192)::bytea;")
assert "$iso" "t" "klass discriminates: same id, different klass = different objects"

# --- 9. liveness: redo_page_asof must not materialize a truncated-away block -----
# A block truncated away (VACUUM truncation -> XLOG_SMGR_TRUNCATE) at/below the
# requested LSN is not live and must not be reconstructed from its stale FPI.
$P -c "CREATE TABLE trunc(id int, v text) WITH (autovacuum_enabled=off);
       INSERT INTO trunc SELECT g, 'row'||g FROM generate_series(1,50) g;
       CHECKPOINT;" >/dev/null
tl0=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "UPDATE trunc SET v=v||'!' WHERE id=1;" >/dev/null  # first change after checkpoint -> FPI of block 0
tl_before=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "DELETE FROM trunc;" >/dev/null
$P -c "VACUUM trunc;" >/dev/null                     # empties block 0 -> truncates it away
tl_after=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_index_wal('$tl0', '$tl_after');" >/dev/null
live_before=$($P -c "SELECT pagestore_redo_page_asof('trunc',0,0,'$tl_before') IS NOT NULL;")
assert "$live_before" "t" "redo_page_asof materializes the block while it is live (before truncation)"
live_after=$($P -c "SELECT pagestore_redo_page_asof('trunc',0,0,'$tl_after') IS NULL;")
assert "$live_after" "t" "redo_page_asof returns NULL for a block truncated away as of the LSN (liveness)"

# --- 14. store-backed redo: replay base+deltas from the store's shipped WAL ------
# With pagestore.redo_wal_from_store on, redo_page_asof reads its WAL records from
# the daemon's shipped per-timeline log instead of local files -- so a compute with
# no local WAL (a fresh branch) can materialize a page.  Ship the segment first.
sw0=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "CREATE TABLE swal(id int, v text) WITH (autovacuum_enabled=off);
       INSERT INTO swal VALUES (1,'sw_base');
       CHECKPOINT;" >/dev/null
$P -c "UPDATE swal SET v='sw_fpi' WHERE id=1;" >/dev/null   # FPI of block 0 after the checkpoint
$P -c "INSERT INTO swal VALUES (2,'sw_delta');" >/dev/null  # a delta on block 0
sw1=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pg_switch_wal();" >/dev/null                  # complete the segment -> shipped to the store
sleep 2
$P -c "SELECT pagestore_index_wal('$sw0', '$sw1');" >/dev/null
sw_store=$($P -c "SET pagestore.redo_wal_from_store = on;
  SELECT position('sw_fpi'::bytea   in pagestore_redo_page_asof('swal',0,0,'$sw1')) > 0
     AND position('sw_delta'::bytea in pagestore_redo_page_asof('swal',0,0,'$sw1')) > 0;" | tail -1)
assert "$sw_store" "t" "redo_page_asof replays base+deltas read from the store's shipped WAL (store-backed reader)"

echo "----"
[ "$fail" = 0 ] && echo "integration test: PASS" || echo "integration test: FAIL"
exit $fail
