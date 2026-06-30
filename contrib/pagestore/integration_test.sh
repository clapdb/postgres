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
PORT2=54461		# a second compute (a branch) booted on the same daemon (step 19)
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
	[ -n "${BRANCHDATA:-}" ] && "$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	rm -rf "$(dirname "$DATA")" "$(dirname "$TS")" "$(dirname "$STORE")" \
		"$(dirname "$SCRATCH")" "${BRANCHDATA:+$(dirname "$BRANCHDATA")}"
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

# --- 15. daemon crash recovery: segment-log recovery of un-flushed writes -------
# Restart the daemon with a flush threshold so high the rows below can never be
# sealed into an image layer -- they live ONLY in the memtable + segment log.  Then
# SIGKILL it (no clean shutdown, so the memtable is lost), restart, and require the
# rows to read back.  They can only come from the segment log, so this fails if
# recovery ever stops scanning segments (e.g. a regression to layer-only rebuild) --
# unlike a shared-daemon test where prior writes could push these into a layer.
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1          # detach the engine before restarting the daemon
kill -9 "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
"$DAEMON" --shm "$SHM" --store "$STORE" --flush-pages 100000000 >/dev/null 2>&1 &  # never flushes -> no layer
DPID=$!
sleep 0.5
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -c "CREATE TABLE crash(id int, v text) TABLESPACE ts;
       INSERT INTO crash SELECT g, 'c'||md5(g::text) FROM generate_series(1,500) g;" >/dev/null
crash_ck=$($P -c "SELECT md5(string_agg(v,',' ORDER BY id)) FROM crash;")
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1          # detach before crashing the daemon
kill -9 "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null      # crash: no ps_core_close() -> memtable lost
"$DAEMON" --shm "$SHM" --store "$STORE" >/dev/null 2>&1 &  # restart -> rebuild the index from the segment log
DPID=$!
sleep 0.5
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
crash_ck2=$($P -c "SELECT md5(string_agg(v,',' ORDER BY id)) FROM crash;")
assert "$crash_ck2" "$crash_ck" "un-flushed rows survive a daemon crash+restart (segment-log recovery)"

# --- 16. SLRU snapshot shipping (M4 step 1): ship clog to the store, keyed by C ----
# CHECKPOINT flushes pg_xact to a clean on-disk image; the single-client test has no
# concurrent commits, so the current LSN bounds it (a valid quiescent cutoff C).  Ship
# it and require: the shipped page reads back as-of C identical to the on-disk page,
# and is NOT visible below C (i.e. it is versioned by C, not a daemon counter).
$P -c "CREATE FUNCTION pagestore_ship_slru_snapshot(text, pg_lsn) RETURNS bigint
        AS 'pagestore','pagestore_ship_slru_snapshot' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_slru_read_at(text, int, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_slru_read_at' LANGUAGE C STRICT;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
cutoff=$($P -c "SELECT pg_current_wal_lsn();")
seg=$($P -c "SELECT name FROM pg_ls_dir('pg_xact') AS name ORDER BY name LIMIT 1;")
pageno=$(( 16#$seg * 32 ))			# first page of the lowest clog segment
shipped=$($P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$cutoff');")
if [ "${shipped:-0}" -gt 0 ]; then
	echo "ok   - shipped $shipped clog page(s) to the store (cutoff $cutoff)"
else
	echo "FAIL - no clog pages shipped"; fail=1
fi
local_md5=$($P -c "SELECT md5(pg_read_binary_file('pg_xact/$seg', 0, 8192));")
store_md5=$($P -c "SELECT md5(pagestore_slru_read_at('pg_xact', $pageno, '$cutoff'));")
assert "$store_md5" "$local_md5" "clog page read from the store as-of C matches the on-disk page"
# below C the page has no version: the read reports a miss (NULL), not a zero page a
# caller could mistake for real all-zero clog state
before_null=$($P -c "SELECT pagestore_slru_read_at('pg_xact', $pageno, '0/1') IS NULL;")
assert "$before_null" "t" "clog snapshot is not visible below its cutoff C (read misses -> NULL)"

# --- 17. SLRU-status applier (M4 step 2): clog reconstruction as-of an LSN ----------
# Snapshot the clog at base C, then commit xidA (<= L) and xidB (> L); both land on the
# same clog page.  Reconstructing as-of L (base snapshot + replay of xact records in
# (C,L]) must show xidA committed but xidB still in progress -- per-record replay, not a
# coalesced page image, is what makes the fork point exact.  At max LSN xidB is committed.
$P -c "CREATE FUNCTION pagestore_clog_status_asof(xid, pg_lsn, pg_lsn) RETURNS int
        AS 'pagestore','pagestore_clog_status_asof' LANGUAGE C STRICT;
       CREATE TABLE clogm(id int);" >/dev/null
# CLOG_XACTS_PER_PAGE = BLCKSZ*4; derive from the server (correct on non-default BLCKSZ)
cxpp=$(( $($P -c "SHOW block_size") * 4 ))
# Make the same-page case deterministic instead of relying on luck: if the next xid is
# within a few slots of a clog page boundary, burn xids to roll onto a fresh page so
# xidA..xidB cannot straddle it (the no-coalescing check needs both on one page).
while [ "$(( $($P -c 'SELECT pg_snapshot_xmax(pg_current_snapshot())') % cxpp ))" -gt "$(( cxpp - 6 ))" ]; do
	$P -c "SELECT txid_current();" >/dev/null
done
# CHECKPOINT *after* burning so those commits are flushed into the on-disk clog the
# snapshot ships -- the base must equal the as-of-base state, not lag it.
$P -c "CHECKPOINT;" >/dev/null                               # flush clog: on-disk == as-of base
base=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$base');" >/dev/null
# data-writing xacts so the commit is sync-flushed (an xid-only xact commits async and
# would not yet be on disk for the no-wait WAL reader)
xidA=$($P -c "WITH w AS (INSERT INTO clogm VALUES (1) RETURNING 1) SELECT pg_current_xact_id();")
L=$($P -c "SELECT pg_current_wal_lsn();")                     # L: after xidA's commit
xidB=$($P -c "WITH w AS (INSERT INTO clogm VALUES (2) RETURNING 1) SELECT pg_current_xact_id();")
assert "$(( xidA / cxpp ))" "$(( xidB / cxpp ))" "applier test setup: xidA and xidB share one clog page"
sA=$($P -c "SELECT pagestore_clog_status_asof('$xidA'::xid, '$base', '$L');")
sB=$($P -c "SELECT pagestore_clog_status_asof('$xidB'::xid, '$base', '$L');")
sBmax=$($P -c "SELECT pagestore_clog_status_asof('$xidB'::xid, '$base', 'FFFFFFFF/FFFFFFFF');")
assert "$sA" "1" "applier: xid committed at/below L is COMMITTED as-of L"
assert "$sB" "0" "applier: xid committed after L is IN-PROGRESS as-of L (no page coalescing)"
assert "$sBmax" "1" "applier: that same xid IS committed when replayed to max LSN"

# --- 18. branch-create clog seeding (M4 step 3) -------------------------------------
# Materialize a new branch's clog as-of L (base snapshot over the fork's xid horizon +
# replay) into a branch dir whose pg_xact already exists, as in an initdb'd datadir, and
# require the published segment to equal the reconstructed as-of-L page.
$P -c "CREATE FUNCTION pagestore_seed_clog(text, pg_lsn, pg_lsn, xid, xid) RETURNS bigint
        AS 'pagestore','pagestore_seed_clog' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_clog_page_asof(int, pg_lsn, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_clog_page_asof' LANGUAGE C STRICT;" >/dev/null
SEEDDIR=$(mktemp -d)
mkdir -p "$SEEDDIR/pg_xact"; : > "$SEEDDIR/pg_xact/0000"   # the initdb default clog to replace
# fork horizon: this cluster never truncates, so oldest is the first normal xid; next_xid
# bounds the highest seeded page (derived from the fork, not the parent's live pg_xact)
nxid=$($P -c "SELECT pg_snapshot_xmax(pg_current_snapshot());")
seeded=$($P -c "SELECT pagestore_seed_clog('$SEEDDIR', '$base', '$L', '3'::xid, '$nxid'::xid);")
if [ "${seeded:-0}" -gt 0 ]; then
	echo "ok   - seeded $seeded clog page(s) into the branch dir as-of L (replacing existing pg_xact)"
else
	echo "FAIL - no clog pages seeded"; fail=1
fi
sseg=$($P -c "SELECT name FROM pg_ls_dir('pg_xact') AS name ORDER BY name LIMIT 1;")
spageno=$(( 16#$sseg * 32 ))
recon_md5=$($P -c "SELECT md5(pagestore_clog_page_asof($spageno, '$base', '$L'));")
seed_md5=$($P -c "SELECT md5(pg_read_binary_file('$SEEDDIR/pg_xact/$sseg', 0, 8192));")
assert "$seed_md5" "$recon_md5" "seeded branch clog page == the reconstructed as-of-L page"
rm -rf "$SEEDDIR"

# --- 19. branch-boot acceptance (M4 step 4): boot a compute on the reconstructed clog ------
# Fork at L between two inserts: row1 commits before L, row2 after L.  A branch booted at L --
# a copy of the parent datadir whose clog is reconstructed as-of L (base snapshot at C + replay
# of (C,L]) and whose relations are served from a store timeline branched at L -- must see row1
# (its xid is committed in the reconstructed clog and its heap version is <= L) but not row2
# (committed after L; that heap version > L is absent from the branch timeline).  And it must
# write forward on its own timeline without the parent seeing it.
$P -c "CREATE FUNCTION pagestore_create_branch(int,int,pg_lsn) RETURNS void
        AS 'pagestore','pagestore_create_branch' LANGUAGE C STRICT;
       CREATE TABLE tb(id int, note text) TABLESPACE ts;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
bc=$($P -c "SELECT pg_current_wal_lsn();")                     # base cutoff C (before row1)
$P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$bc');" >/dev/null
$P -c "INSERT INTO tb VALUES (1,'before_L');" >/dev/null       # T1 commits in (C, L]
bL=$($P -c "SELECT pg_current_wal_lsn();")                     # fork LSN L (after row1)
boxid=$($P -c "SELECT pg_snapshot_xmax(pg_current_snapshot());")
# Reconstruct the branch clog as-of L *now*, while the (C, L] WAL is still present (the
# parent's later stop/restart/checkpoints recycle it).  The base snapshot at C has T1
# in-progress; the replay of (C, L] must mark it committed, so booting on this clog -- not
# the parent's copied one -- is what makes row1 visible.
SEEDOUT=$(mktemp -d)/seedout
seeded_b=$($P -c "SELECT pagestore_seed_clog('$SEEDOUT', '$bc', '$bL', '3'::xid, '$boxid'::xid);")
assert "$([ "${seeded_b:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"branch clog reconstructed via base snapshot + (C,L] replay ($seeded_b page(s))"
# clean-stop the parent so its datadir is consistent at L, copy it, restart, then advance
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1
BRANCHDATA=$(mktemp -d)/branch
cp -a "$DATA" "$BRANCHDATA"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -c "INSERT INTO tb VALUES (2,'after_L'); CHECKPOINT;" >/dev/null   # T2 after L (heap ver > L)
$P -c "SELECT pagestore_create_branch(1, 0, '$bL');" >/dev/null       # store timeline 1 @ L
# Install the reconstructed clog into the branch copy, replacing the copied one, so the
# boot genuinely depends on the seeder's output rather than the parent's as-of-L pg_xact.
rm -rf "$BRANCHDATA/pg_xact"
cp -a "$SEEDOUT/pg_xact" "$BRANCHDATA/pg_xact"
# point the copied datadir at timeline 1 on a distinct port; it reads relations as-of L
cat >> "$BRANCHDATA/postgresql.conf" <<EOF
pagestore.timeline = 1
port = $PORT2
archive_mode = off
EOF
"$BIN/pg_ctl" -D "$BRANCHDATA" -l "$BRANCHDATA/server.log" -w start >/dev/null 2>&1
PB="$BIN/psql -h 127.0.0.1 -p $PORT2 -U postgres -tA"
assert "$($PB -c "SELECT count(*) FROM tb WHERE note='before_L';" 2>/dev/null)" "1" \
	"branch boots on the reconstructed clog and sees the row committed before L"
assert "$($PB -c "SELECT count(*) FROM tb WHERE note='after_L';" 2>/dev/null)" "0" \
	"branch does NOT see the row committed after L"
# write forward, then CHECKPOINT so the page is actually shipped to the daemon on timeline 1
# (not merely dirtied in the branch's buffers), then restart to evict buffers and re-read
# from the store.  This proves the write went forward on tl=1 AND that row2's post-L heap
# version is physically absent there -- not just MVCC-hidden (the branch's first commit
# reuses the same XID the parent used for after_L).
$PB -c "INSERT INTO tb VALUES (3,'branch_local'); CHECKPOINT;" >/dev/null 2>&1
"$BIN/pg_ctl" -D "$BRANCHDATA" -w restart >/dev/null 2>&1
assert "$($PB -c "SELECT count(*) FROM tb WHERE note='branch_local';" 2>/dev/null)" "1" \
	"branch write went forward on its own timeline (survives buffer eviction)"
assert "$($PB -c "SELECT count(*) FROM tb WHERE note='after_L';" 2>/dev/null)" "0" \
	"row committed after L is physically absent from the branch timeline (not just MVCC-hidden)"
assert "$($P -c "SELECT count(*) FROM tb WHERE note='branch_local';")" "0" \
	"parent is isolated from the branch's local write"
"$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1
rm -rf "$(dirname "$SEEDOUT")"

# --- 20. commit-ts applier: reconstruct commit timestamps as-of L ---------------------
# Same shape as the clog applier, for pg_commit_ts: snapshot at C, commit xidA (<= L) and
# xidB (> L); reconstructing as-of L must give xidA its real commit timestamp (matching the
# parent's pg_xact_commit_timestamp) and xidB none -- per-record replay, no coalescing.
$P -c "CREATE FUNCTION pagestore_commit_ts_asof(xid, pg_lsn, pg_lsn, xid) RETURNS timestamptz
        AS 'pagestore','pagestore_commit_ts_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_commit_ts_page_asof(int, pg_lsn, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_commit_ts_page_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_seed_commit_ts(text, pg_lsn, pg_lsn, xid, xid) RETURNS bigint
        AS 'pagestore','pagestore_seed_commit_ts' LANGUAGE C STRICT;" >/dev/null
echo "track_commit_timestamp = on" >> "$DATA/postgresql.conf"   # needs a restart to activate
"$BIN/pg_ctl" -D "$DATA" -w restart >/dev/null 2>&1
$P -c "CREATE TABLE cts(id int) TABLESPACE ts;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
ctsC=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_ship_slru_snapshot('pg_commit_ts', '$ctsC');" >/dev/null
# data-writing xacts so the commit (and its timestamp) is sync-flushed for the WAL reader
ctsA=$($P -c "WITH w AS (INSERT INTO cts VALUES (1) RETURNING 1) SELECT pg_current_xact_id();")
ctsL=$($P -c "SELECT pg_current_wal_lsn();")                      # L: after xidA's commit
ctsB=$($P -c "WITH w AS (INSERT INTO cts VALUES (2) RETURNING 1) SELECT pg_current_xact_id();")
# oldest='3' (below all our xids) disables the horizon check for these baseline assertions
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsC', '$ctsL', '3'::xid) IS NOT NULL;")" "t" \
	"commit-ts: xid committed at/below L has a reconstructed timestamp"
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsC', '$ctsL', '3'::xid) = pg_xact_commit_timestamp('$ctsA'::xid);")" "t" \
	"commit-ts: reconstructed timestamp matches the parent's pg_xact_commit_timestamp"
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsB'::xid, '$ctsC', '$ctsL', '3'::xid) IS NULL;")" "t" \
	"commit-ts: xid committed after L has no timestamp as-of L (no coalescing)"
# the commit-ts horizon masks xidA: with oldest = xidA+1, the lookup returns NULL even
# though xidA's bytes are physically on the reconstructed page (matches the parent's
# oldestCommitTsXid rejection after a truncation / before an activation)
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsC', '$ctsL', ('$ctsA'::xid::text::bigint + 1)::text::xid) IS NULL;")" "t" \
	"commit-ts: a xid below the as-of-L horizon (oldestCommitTsXid) returns NULL despite stale page bytes"
CTSSEED=$(mktemp -d)
cts_next=$(($ctsB + 1))
ctsSeeded=$($P -c "SELECT pagestore_seed_commit_ts('$CTSSEED', '$ctsC', '$ctsL', '3'::xid, '$cts_next'::text::xid);")
assert "$([ "${ctsSeeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"commit-ts seed materialized branch pg_commit_ts as-of L ($ctsSeeded page(s))"
bs=$($P -c "SELECT current_setting('block_size')::int;")
cts_entry_size=10
cts_per_page=$(( bs / cts_entry_size ))
ctsPage=$(( ctsA / cts_per_page ))
ctsSeg=$(printf '%04X' $(( ctsPage / 32 )))
ctsSeedMd5=$($P -c "SELECT md5(pg_read_binary_file('$CTSSEED/pg_commit_ts/$ctsSeg', $(( (ctsPage % 32) * bs )), $bs));")
ctsReconMd5=$($P -c "SELECT md5(pagestore_commit_ts_page_asof($ctsPage, '$ctsC', '$ctsL'));")
assert "$ctsSeedMd5" "$ctsReconMd5" "commit-ts seed page == reconstructed as-of-L page"
rm -rf "$CTSSEED"

# --- 21. multixact offsets applier: reconstruct the multixid->offset map as-of L --------
# A multixact needs two concurrent lockers, so hold a FOR SHARE lock in a background session
# while a second session also locks the row, creating multixact mA.  Reconstructing the
# offsets SLRU as-of L must give mA the same starting member offset the parent recorded on
# disk (a byte-for-byte check against the live pg_multixact/offsets file).
$P -c "CREATE FUNCTION pagestore_multixact_offset_asof(xid, pg_lsn, pg_lsn) RETURNS bigint
        AS 'pagestore','pagestore_multixact_offset_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_multixact_offsets_page_asof(int, pg_lsn, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_multixact_offsets_page_asof' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE TABLE mx(id int primary key, note text) TABLESPACE ts; INSERT INTO mx VALUES (1,'a');" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
mxC=$($P -c "SELECT pg_current_wal_lsn();")                       # base cutoff C
$P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$mxC');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_commit_ts', '$mxC');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_multixact/offsets', '$mxC');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_multixact/members', '$mxC');" >/dev/null
# session A holds a FOR SHARE lock across the second locker
("$BIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tA \
	-c "BEGIN; SELECT id FROM mx WHERE id=1 FOR SHARE; SELECT pg_sleep(8); COMMIT;" >/dev/null 2>&1) &
mxlocker=$!
# wait until A actually holds the ROW lock -- its xid lands in the tuple's xmax -- rather
# than a fixed sleep (or a relation-level RowShareLock that is taken before the tuple lock),
# so a slow host can't let B lock and commit the row alone
for _ in $(seq 1 100); do
	[ "$($P -c "SELECT (xmax <> '0'::xid)::int FROM mx WHERE id=1;" 2>/dev/null)" = "1" ] && break
	sleep 0.1
done
# session B locks the same row while A still holds -> a multixact is created
$P -c "BEGIN; SELECT id FROM mx WHERE id=1 FOR SHARE; COMMIT;" >/dev/null
mA=$($P -c "SELECT xmax FROM mx WHERE id=1;")                     # the row's xmax is the multixact id
$P -c "CHECKPOINT;" >/dev/null
mxL=$($P -c "SELECT pg_current_wal_lsn();")                       # fork LSN L (after mA)
wait "$mxlocker"		# only the locker; a bare wait would also block on the daemon
# confirm mA really is a multixact (its two FOR SHARE members), not a plain xid
mxMembers=$($P -c "SELECT count(*) FROM pg_get_multixact_members('$mA');" 2>/dev/null)
assert "$mxMembers" "2" "a multixact (mA=$mA) was created by two concurrent FOR SHARE lockers"
mxRecon=$($P -c "SELECT pagestore_multixact_offset_asof('$mA'::xid, '$mxC', '$mxL');")  # offset, for step 21
# assert the scalar helper itself (a multixact's member offset is always >= 1; offset 0 is
# skipped), so an error or zero from it fails the test rather than being silently ignored;
# its exact value is then checked transitively below via mPage = mxRecon/mpp
assert "$([ "${mxRecon:-x}" -gt 0 ] 2>/dev/null && echo ok || echo "bad:$mxRecon")" "ok" \
	"multixact offsets: scalar offset_asof(mA) returns a valid (>0) member offset"
# derive the SLRU page geometry from the server's block_size instead of hardcoding 8192:
# MULTIXACT_OFFSETS_PER_PAGE = BLCKSZ/4, and SLRU_PAGES_PER_SEGMENT = 32 (block-size independent)
bs=$($P -c "SELECT current_setting('block_size')::int;")
opp=$(( bs / 4 ))
# byte-for-byte: the reconstructed offsets page == the parent's live pg_multixact/offsets
# file (endian-agnostic, unlike decoding the uint32; also checks the successor slot mA+1)
mxPage=$(( mA / opp ))
mxRP=$($P -c "SELECT md5(pagestore_multixact_offsets_page_asof($mxPage, '$mxC', '$mxL'));")
mxSeg=$(printf '%04X' $(( mxPage / 32 )))
mxLP=$($P -c "SELECT md5(pg_read_binary_file('pg_multixact/offsets/$mxSeg', $(( (mxPage % 32) * bs )), $bs));")
assert "$mxRP" "$mxLP" "multixact offsets: reconstructed page as-of L == the parent's live offsets file"

# --- 21. multixact members applier: reconstruct the offset->member-list page as-of L ----
# mA's offset (from step 20) locates its members page; reconstructing that page as-of L must
# equal the parent's live pg_multixact/members file byte-for-byte.  With the offsets half,
# this resolves mA's members: the page holds its two FOR SHARE lockers at offset mOff.
$P -c "CREATE FUNCTION pagestore_multixact_members_page_asof(int, pg_lsn, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_multixact_members_page_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_seed_multixact(text, pg_lsn, pg_lsn, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_seed_multixact' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_seed_branch_slrus(text, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_seed_branch_slrus' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_prepare_branch(text, int, int, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_prepare_branch' LANGUAGE C STRICT;" >/dev/null
mOff=$mxRecon                                          # mA's first member offset (step 20)
# MULTIXACT_MEMBERS_PER_PAGE = (block_size / MULTIXACT_MEMBERGROUP_SIZE) * members-per-group;
# the group is 4 flag bytes + 4 TransactionIds = 20 bytes, 4 members each (block-size derived)
mpp=$(( (bs / 20) * 4 ))
mPage=$(( mOff / mpp ))
mbRecon=$($P -c "SELECT md5(pagestore_multixact_members_page_asof($mPage, '$mxC', '$mxL'));")
mbSeg=$(printf '%04X' $(( mPage / 32 )))
mbLive=$($P -c "SELECT md5(pg_read_binary_file('pg_multixact/members/$mbSeg', $(( (mPage % 32) * bs )), $bs));")
assert "$mbRecon" "$mbLive" "multixact members: reconstructed page as-of L == the parent's live members file"
# End to end: the parent resolves mA's members from exactly these on-disk pages -- step 20
# proved the offsets page (mA -> mOff) matches the live file, and the line above proves the
# members page (mOff -> the two locker xids) matches it too, both byte-for-byte.  So mA
# resolves to the same members against the reconstruction.  (We compare raw page bytes
# rather than decoding the TransactionIds, which keeps the check endian-agnostic.)
mbParent=$($P -c "SELECT count(*) FROM pg_get_multixact_members('$mA');" 2>/dev/null)
assert "$mbParent" "2" "multixact members: the parent resolves mA to its two members from those pages"
MXSEED=$(mktemp -d)
mxNext=$(( mA + 1 ))
mxSeeded=$($P -c "SELECT pagestore_seed_multixact('$MXSEED', '$mxC', '$mxL', '$mA'::xid, '$mxNext'::text::xid, $mOff, $((mOff + mxMembers)));")
assert "$([ "${mxSeeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"multixact seed materialized offsets+members SLRUs as-of L ($mxSeeded page(s))"
mxSeedOff=$($P -c "SELECT md5(pg_read_binary_file('$MXSEED/pg_multixact/offsets/$mxSeg', $(( (mxPage % 32) * bs )), $bs));")
mxSeedMem=$($P -c "SELECT md5(pg_read_binary_file('$MXSEED/pg_multixact/members/$mbSeg', $(( (mPage % 32) * bs )), $bs));")
assert "$mxSeedOff" "$mxRP" "multixact seed offsets page == reconstructed as-of-L page"
assert "$mxSeedMem" "$mbRecon" "multixact seed members page == reconstructed as-of-L page"
rm -rf "$MXSEED"

# --- 22. branch bootstrap SLRU seeder: one fail-closed entrypoint for all SLRUs --------
# A branch-control-plane caller should not hand-roll separate seed calls for pg_xact,
# pg_commit_ts, and pg_multixact.  Seed them through a single bootstrap helper using one
# fork window and one set of horizons, then require each published SLRU page to match its
# existing per-SLRU reconstruction helper.
BOOTSEED=$(mktemp -d)
bootNext=$($P -c "SELECT pg_snapshot_xmax(pg_current_snapshot());")
bootSeeded=$($P -c "SELECT pagestore_seed_branch_slrus('$BOOTSEED', '$mxC', '$mxL',
	'3'::xid, '$bootNext'::xid, '$ctsA'::xid, '$cts_next'::text::xid, '$mA'::xid, '$mxNext'::xid, $mOff, $((mOff + mxMembers)));")
assert "$([ "${bootSeeded:-0}" -ge 3 ] && echo ok || echo no)" "ok" \
	"branch bootstrap seed materialized pg_xact, pg_commit_ts, and pg_multixact ($bootSeeded page(s))"
bootClogSeg=$(printf '%04X' $(( (ctsA / cxpp) / 32 )))
bootClogPage=$(( (ctsA / cxpp) % 32 ))
bootClogMd5=$($P -c "SELECT md5(pg_read_binary_file('$BOOTSEED/pg_xact/$bootClogSeg', $(( bootClogPage * bs )), $bs));")
bootClogRecon=$($P -c "SELECT md5(pagestore_clog_page_asof($(( ctsA / cxpp )), '$mxC', '$mxL'));")
assert "$bootClogMd5" "$bootClogRecon" "branch bootstrap seed pg_xact page == reconstructed as-of-L page"
bootCtsMd5=$($P -c "SELECT md5(pg_read_binary_file('$BOOTSEED/pg_commit_ts/$ctsSeg', $(( (ctsPage % 32) * bs )), $bs));")
bootCtsRecon=$($P -c "SELECT md5(pagestore_commit_ts_page_asof($ctsPage, '$mxC', '$mxL'));")
assert "$bootCtsMd5" "$bootCtsRecon" "branch bootstrap seed pg_commit_ts page == reconstructed as-of-L page"
bootMxOff=$($P -c "SELECT md5(pg_read_binary_file('$BOOTSEED/pg_multixact/offsets/$mxSeg', $(( (mxPage % 32) * bs )), $bs));")
bootMxMem=$($P -c "SELECT md5(pg_read_binary_file('$BOOTSEED/pg_multixact/members/$mbSeg', $(( (mPage % 32) * bs )), $bs));")
assert "$bootMxOff" "$mxRP" "branch bootstrap seed multixact offsets page == reconstructed as-of-L page"
assert "$bootMxMem" "$mbRecon" "branch bootstrap seed multixact members page == reconstructed as-of-L page"
rm -rf "$BOOTSEED"

# --- 23. branch prepare control-plane entrypoint: seed + fork timeline + manifest -------
# The next layer up should call one control-plane function, not independently remember to
# seed SLRUs, create the store timeline, and persist fork metadata.  Preparing a branch
# must leave a durable manifest next to the seeded SLRUs; that manifest is the handoff
# artifact for the later pg_control/bootstrap step.
PREPSEED=$(mktemp -d)
prepSeeded=$($P -c "SELECT pagestore_prepare_branch('$PREPSEED', 2, 0, '$mxC', '$mxL',
	'3'::xid, '$bootNext'::xid, '$ctsA'::xid, '$cts_next'::text::xid, '$mA'::xid, '$mxNext'::xid, $mOff, $((mOff + mxMembers)));")
assert "$([ "${prepSeeded:-0}" -ge 3 ] && echo ok || echo no)" "ok" \
	"branch prepare seeded all bootstrap SLRUs and forked a store timeline ($prepSeeded page(s))"
manifestHasTimeline=$($P -c "SELECT position('\"new_timeline\": 2' in pg_read_file('$PREPSEED/pagestore_branch.manifest')) > 0;")
assert "$manifestHasTimeline" "t" "branch prepare manifest records the new timeline"
manifestHasFork=$($P -c "SELECT position('\"fork_lsn\": ' in pg_read_file('$PREPSEED/pagestore_branch.manifest')) > 0;")
assert "$manifestHasFork" "t" "branch prepare manifest records the fork LSN"
prepClogMd5=$($P -c "SELECT md5(pg_read_binary_file('$PREPSEED/pg_xact/$bootClogSeg', $(( bootClogPage * bs )), $bs));")
assert "$prepClogMd5" "$bootClogRecon" "branch prepare pg_xact page == reconstructed as-of-L page"
prepMxOff=$($P -c "SELECT md5(pg_read_binary_file('$PREPSEED/pg_multixact/offsets/$mxSeg', $(( (mxPage % 32) * bs )), $bs));")
prepMxMem=$($P -c "SELECT md5(pg_read_binary_file('$PREPSEED/pg_multixact/members/$mbSeg', $(( (mPage % 32) * bs )), $bs));")
assert "$prepMxOff" "$mxRP" "branch prepare multixact offsets page == reconstructed as-of-L page"
assert "$prepMxMem" "$mbRecon" "branch prepare multixact members page == reconstructed as-of-L page"
rm -rf "$PREPSEED"

echo "----"
[ "$fail" = 0 ] && echo "integration test: PASS" || echo "integration test: FAIL"
exit $fail
