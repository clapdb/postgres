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

SOCKROOT=$(mktemp -d /tmp/psint-sock.XXXXXX)
new_sockdir() {
	mktemp -d "$SOCKROOT/$1.XXXXXX"
}

DATA=$(mktemp -d)/pgdata
TS=$(mktemp -d)/ts
STORE=$(mktemp -d)/store
SCRATCH=$(mktemp -d)/walredo	# private throwaway cluster for the wal-redo helper
MAIN_SOCK=$(new_sockdir main)
SHM=/psint_$$
PORT=5432
P="$BIN/psql -h $MAIN_SOCK -p $PORT -U postgres -tA"
fail=0

assert() {  # $1=actual $2=expected $3=message
	if [ "$1" = "$2" ]; then
		echo "ok   - $3"
	else
		echo "FAIL - $3 (got '$1', want '$2')"
		fail=1
	fi
}

wait_daemon_ready() {
	local shm_path="/dev/shm$SHM"
	local expected_magic=$((0x50414753))
	local expected_version=21
	local expected_page_size=8192
	local expected_io_unit=$((256 * 1024))
	local expected_channels=128
	local expected_shards=1
	local magic version page_size io_unit nchannels nshards
	local i

	for ((i = 0; i < 400; i++)); do
		if ! kill -0 "$DPID" 2>/dev/null; then
			echo "FAIL - pagestore daemon exited before publishing shared memory"
			tail -100 "$DATA/daemon.log" 2>/dev/null || true
			exit 1
		fi
		if [ -r "$shm_path" ]; then
			read -r magic version page_size io_unit nchannels nshards < <(
				od -An -tu4 -N24 -w24 "$shm_path" 2>/dev/null
			)
			[ "$magic" = "$expected_magic" ] &&
				[ "$version" = "$expected_version" ] &&
				[ "$page_size" = "$expected_page_size" ] &&
				[ "$io_unit" = "$expected_io_unit" ] &&
				[ "$nchannels" = "$expected_channels" ] &&
				[ "$nshards" = "$expected_shards" ] && return 0
		fi
		sleep 0.05
	done

	echo "FAIL - pagestore daemon did not publish a ready shared-memory header"
	tail -100 "$DATA/daemon.log" 2>/dev/null || true
	exit 1
}

# KEEPTMP=1 keeps the data/store directories for post-mortem debugging
# (servers and daemon are still stopped).
cleanup() {
	"$BIN/pg_ctl" -D "$DATA" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${BRANCHDATA:-}" ] && "$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${READERDATA:-}" ] && "$BIN/pg_ctl" -D "$READERDATA" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${BADREADER:-}" ] && "$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${UNPREPARED:-}" ] && "$BIN/pg_ctl" -D "$UNPREPARED" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	[ -n "${KEEPTMP:-}" ] && { echo "KEEPTMP: DATA=$DATA STORE=$STORE SOCKROOT=$SOCKROOT"; return 0; }
	rm -rf "$(dirname "$DATA")" "$(dirname "$TS")" "$(dirname "$STORE")" \
		"$(dirname "$SCRATCH")" "${BRANCHDATA:+$(dirname "$BRANCHDATA")}" \
		"${READERDATA:+$(dirname "$READERDATA")}" \
		"${BADREADER:+$(dirname "$BADREADER")}" \
		"${UNPREPARED:+$(dirname "$UNPREPARED")}" "$SOCKROOT"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

mkdir -p "$TS"
"$BIN/initdb" -D "$DATA" -U postgres -A trust >/dev/null 2>&1
"$BIN/initdb" -D "$SCRATCH" -U postgres -A trust >/dev/null 2>&1
rm -f "/dev/shm$SHM"
"$DAEMON" --shm "$SHM" --store "$STORE" >>"$DATA/daemon.log" 2>&1 &
DPID=$!
wait_daemon_ready

cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_user_tablespaces = on
pagestore.walredo_datadir = '$SCRATCH'
pagestore.slru_mirror = on
io_method = sync
wal_keep_size = 512MB	# appliers replay (C, L] from local pg_wal across restarts
archive_mode = on
archive_library = 'pagestore'
listen_addresses = ''
unix_socket_directories = '$MAIN_SOCK'
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

# A restore command is also asked for timeline-history files.  The pagestore
# archive intentionally does not retain those auxiliary files, so walrestore
# must report them unavailable (not a hard command error that aborts recovery).
history_out=$(mktemp)
rm -f "$history_out"
"$BUILD/contrib/pagestore/pagestore_walrestore" --shm "$SHM" --timeline 0 --segsize 16777216 \
	00000002.history "$history_out" >/dev/null 2>&1
history_rc=$?
if [ "$history_rc" -eq 1 ] && [ ! -e "$history_out" ]; then
	echo "ok   - walrestore treats an unavailable timeline history file as archive miss"
else
	echo "FAIL - walrestore history-file handling returned $history_rc (output exists: $([ -e "$history_out" ] && echo yes || echo no))"
	fail=1
fi
rm -f "$history_out"

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
rm -f "/dev/shm$SHM"
# Never flush: the crash-recovery rows must remain segment-log-only.
"$DAEMON" --shm "$SHM" --store "$STORE" --flush-pages 100000000 \
	>>"$DATA/daemon.log" 2>&1 &
DPID=$!
wait_daemon_ready
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -c "CREATE TABLE crash(id int, v text) TABLESPACE ts;
       INSERT INTO crash SELECT g, 'c'||md5(g::text) FROM generate_series(1,500) g;" >/dev/null
crash_ck=$($P -c "SELECT md5(string_agg(v,',' ORDER BY id)) FROM crash;")
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1          # detach before crashing the daemon
kill -9 "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null      # crash: no ps_core_close() -> memtable lost
rm -f "/dev/shm$SHM"
# Restart and rebuild the index from the segment log.
"$DAEMON" --shm "$SHM" --store "$STORE" >>"$DATA/daemon.log" 2>&1 &
DPID=$!
wait_daemon_ready
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
$P -c "CREATE FUNCTION pagestore_prepare_branch(text, int, int, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_prepare_branch' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_install_prepared_branch(text, text, int, int, pg_lsn) RETURNS void
        AS 'pagestore','pagestore_install_prepared_branch' LANGUAGE C STRICT;
       CREATE TABLE tb(id int, note text) TABLESPACE ts;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
bc=$($P -c "SELECT pg_current_wal_lsn();")                     # base cutoff C (before row1)
$P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$bc');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_commit_ts', '$bc');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_multixact/offsets', '$bc');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_multixact/members', '$bc');" >/dev/null
$P -c "INSERT INTO tb VALUES (1,'before_L');" >/dev/null       # T1 commits in (C, L]
$P -c "CHECKPOINT;" >/dev/null                                # ship the row1 heap version
bL=$($P -c "SELECT pg_current_wal_lsn();")                     # fork LSN L (after row1)
boxid=$($P -c "SELECT pg_snapshot_xmax(pg_current_snapshot());")
# Prepare the branch as-of L while the (C, L] WAL is still present.  The base
# snapshot at C has T1 in-progress; the replay of (C, L] must mark it
# committed, so booting on the prepared pg_xact -- not the parent's copied one
# -- is what makes row1 visible.
SEEDOUT=$(mktemp -d)
seeded_b=$($P -c "SELECT pagestore_prepare_branch('$SEEDOUT', 1, 0, '$bc', '$bL',
	'3'::xid, '$boxid'::xid, '1'::xid, '1'::xid, '1'::xid, '1'::xid, 0, 0);")
assert "$([ "${seeded_b:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"branch prepared via base snapshot + (C,L] replay ($seeded_b SLRU page(s))"
# commit-ts was never active at this fork (non-normal horizon), so prepare must
# still publish pg_commit_ts -- as the empty fork state -- rather than leaving
# whatever a reused target dir carried
assert "$([ -d "$SEEDOUT/pg_commit_ts" ] && echo present)" "present" \
	"branch prepare publishes an empty pg_commit_ts for an inactive horizon"
# --- branch WAL read-through: the branch serves its pre-fork WAL history ------
# wal_read walks the ancestry: a segment completed before the fork lives only in
# the parent's shipped log, and reading it through the branch timeline must give
# the same bytes the parent's own log serves.
rt_seg=$(basename "$(ls "$DATA"/pg_wal/archive_status/*.done 2>/dev/null | head -1)" .done)
rt0=$(mktemp); rt1=$(mktemp)
if "$BUILD/contrib/pagestore/pagestore_walrestore" --shm "$SHM" --timeline 0 --segsize 16777216 "$rt_seg" "$rt0" >/dev/null 2>&1 \
   && "$BUILD/contrib/pagestore/pagestore_walrestore" --shm "$SHM" --timeline 1 --segsize 16777216 "$rt_seg" "$rt1" >/dev/null 2>&1; then
	assert "$(md5sum < "$rt1" | cut -d' ' -f1)" "$(md5sum < "$rt0" | cut -d' ' -f1)" \
		"branch timeline serves a pre-fork WAL segment identical to the parent's"
else
	echo "FAIL - walrestore could not reconstruct pre-fork segment '$rt_seg' through the branch"; fail=1
fi
rm -f "$rt0" "$rt1"
# Copy the branch datadir before the parent advances past L, so the branch's
# first write reuses the XID that the parent will spend on after_L below.
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1
BRANCHDATA=$(mktemp -d)/branch
cp -a "$DATA" "$BRANCHDATA"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -c "INSERT INTO tb VALUES (2,'after_L'); CHECKPOINT;" >/dev/null   # T2 after L (heap ver > L)
# Install the prepared branch artifacts into the branch copy, replacing copied SLRUs,
# so the boot genuinely depends on all prepare_branch output rather than parent state.
bad_install=$($P -c "SELECT pagestore_install_prepared_branch('$SEEDOUT', '$BRANCHDATA', 2, 0, '$bL');" 2>/dev/null || echo error)
assert "$bad_install" "error" "prepared branch install rejects the wrong branch identity"
BADSEED=$(mktemp -d)
cp "$SEEDOUT/pagestore_branch.manifest" "$BADSEED/pagestore_branch.manifest"
missing_artifact=$($P -c "SELECT pagestore_install_prepared_branch('$BADSEED', '$BRANCHDATA', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$missing_artifact" "error" "prepared branch install rejects a missing pg_xact artifact"
rm -rf "$BADSEED"
# overlapping source/target must be rejected before any target mutation, so a
# same-dir typo cannot unlink the prepared manifest it is installing from
same_dir=$($P -c "SELECT pagestore_install_prepared_branch('$SEEDOUT', '$SEEDOUT', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$same_dir" "error" "prepared branch install rejects identical prepared/target dirs"
nested_dir=$($P -c "SELECT pagestore_install_prepared_branch('$SEEDOUT', '$SEEDOUT/pg_xact/branch', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$nested_dir" "error" "prepared branch install rejects a target nested in the prepared dir"
assert "$([ -f "$SEEDOUT/pagestore_branch.manifest" ] && echo present)" "present" \
	"prepared manifest survives the rejected overlapping installs"
# a relative prepared path (resolved against the backend cwd, the datadir) must
# still be recognized as overlapping an absolute target spelled under it
cp -a "$SEEDOUT" "$DATA/relseed"
rel_nested=$($P -c "SELECT pagestore_install_prepared_branch('relseed', '$DATA/relseed/pg_xact/branch', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$rel_nested" "error" "prepared branch install rejects relative/absolute spellings of overlapping dirs"
rm -rf "$DATA/relseed"
# a symlinked artifact passes a follow-symlink stat but copydir() skips
# symlinks, so preflight must reject it before the target is touched
LNKSEED=$(mktemp -d)/lnkseed
REALOFF=$(mktemp -d)
cp -a "$SEEDOUT" "$LNKSEED"
mv "$LNKSEED/pg_multixact/offsets" "$REALOFF/offsets"
ln -s "$REALOFF/offsets" "$LNKSEED/pg_multixact/offsets"
symlink_artifact=$($P -c "SELECT pagestore_install_prepared_branch('$LNKSEED', '$BRANCHDATA', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$symlink_artifact" "error" "prepared branch install rejects a symlinked pg_multixact/offsets artifact"
rm -rf "$(dirname "$LNKSEED")" "$REALOFF"
# the same applies one level down: a symlinked segment file inside an artifact
# dir would be skipped by copydir(), so the recursive preflight must catch it
LNKSEG=$(mktemp -d)/lnkseg
REALSEG=$(mktemp -d)
cp -a "$SEEDOUT" "$LNKSEG"
seg0=$(ls "$LNKSEG/pg_xact" | head -1)
mv "$LNKSEG/pg_xact/$seg0" "$REALSEG/$seg0"
ln -s "$REALSEG/$seg0" "$LNKSEG/pg_xact/$seg0"
symlink_segment=$($P -c "SELECT pagestore_install_prepared_branch('$LNKSEG', '$BRANCHDATA', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$symlink_segment" "error" "prepared branch install rejects a symlinked pg_xact segment file"
rm -rf "$(dirname "$LNKSEG")" "$REALSEG"
# an existing target manifest naming a different branch identity must be
# rejected before any artifact is touched
BADTARGET=$(mktemp -d)/branch
cp -a "$BRANCHDATA" "$BADTARGET"
sed 's/"new_timeline": 1/"new_timeline": 2/' "$SEEDOUT/pagestore_branch.manifest" > "$BADTARGET/pagestore_branch.manifest"
target_mismatch=$($P -c "SELECT pagestore_install_prepared_branch('$SEEDOUT', '$BADTARGET', 1, 0, '$bL');" 2>/dev/null || echo error)
assert "$target_mismatch" "error" "prepared branch install rejects an existing target manifest for another branch"
rm -rf "$(dirname "$BADTARGET")"
# a branch timeline without a manifest must fail closed at startup: an
# unprepared copy of the parent datadir cannot boot as a branch
UNPREPARED=$(mktemp -d)/branch
cp -a "$BRANCHDATA" "$UNPREPARED"
UNPREPARED_SOCK=$(new_sockdir unprepared)
cat >> "$UNPREPARED/postgresql.conf" <<EOF
pagestore.timeline = 1
listen_addresses = ''
unix_socket_directories = '$UNPREPARED_SOCK'
port = $PORT
archive_mode = off
EOF
if "$BIN/pg_ctl" -D "$UNPREPARED" -l "$UNPREPARED/server.log" -w start >/dev/null 2>&1; then
	echo "FAIL - branch startup accepted an unprepared datadir without a manifest"
	fail=1
	"$BIN/pg_ctl" -D "$UNPREPARED" -m immediate -w stop >/dev/null 2>&1 || true
else
	echo "ok   - branch startup rejects an unprepared datadir without a manifest"
fi
rm -rf "$(dirname "$UNPREPARED")"
UNPREPARED=
# the manifest has no commit-ts horizon, so install must reset the target's
# pg_commit_ts to the empty fork state instead of keeping post-fork leftovers
touch "$BRANCHDATA/pg_commit_ts/STALE"
ok_install=$($P -c "SELECT pagestore_install_prepared_branch('$SEEDOUT', '$BRANCHDATA', 1, 0, '$bL');" >/dev/null 2>&1 && echo ok || echo error)
assert "$ok_install" "ok" "prepared branch install succeeds for the same branch identity"
assert "$([ -e "$BRANCHDATA/pg_commit_ts/STALE" ] && echo stale || echo clean)" "clean" \
	"install resets target pg_commit_ts when the manifest has no commit-ts horizon"
ok_install=$($P -c "SELECT pagestore_install_prepared_branch('$SEEDOUT', '$BRANCHDATA', 1, 0, '$bL');" >/dev/null 2>&1 && echo ok || echo error)
assert "$ok_install" "ok" "prepared branch install is idempotent for the same branch identity"
# This copied parent datadir was not prepared under full routing.  With a
# manifest installed, startup must fail closed instead of accepting a branch
# that would leave default/global tablespaces on local md storage.
BRANCH_SOCK=$(new_sockdir branch)
cat >> "$BRANCHDATA/postgresql.conf" <<EOF
pagestore.timeline = 1
listen_addresses = ''
unix_socket_directories = '$BRANCH_SOCK'
port = $PORT
archive_mode = off
EOF
if "$BIN/pg_ctl" -D "$BRANCHDATA" -l "$BRANCHDATA/server.log" -w start >/dev/null 2>&1; then
	echo "FAIL - branch startup accepted an installed manifest without full routing"
	fail=1
	"$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1 || true
else
	echo "ok   - branch startup rejects an installed manifest without full routing"
fi
rm -rf "$SEEDOUT"

# --- 20. commit-ts applier: reconstruct commit timestamps as-of L ---------------------
# Same shape as the clog applier, for pg_commit_ts: snapshot at C, commit xidA (<= L) and
# xidB (> L); reconstructing as-of L must give xidA its real commit timestamp (matching the
# parent's pg_xact_commit_timestamp) and xidB none -- per-record replay, no coalescing.
$P -c "CREATE FUNCTION pagestore_commit_ts_asof(xid, pg_lsn, pg_lsn, xid) RETURNS timestamptz
        AS 'pagestore','pagestore_commit_ts_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_commit_ts_page_asof(int, pg_lsn, pg_lsn, xid) RETURNS bytea
        AS 'pagestore','pagestore_commit_ts_page_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_seed_commit_ts(text, pg_lsn, pg_lsn, xid, xid) RETURNS bigint
        AS 'pagestore','pagestore_seed_commit_ts' LANGUAGE C STRICT;" >/dev/null
echo "track_commit_timestamp = on" >> "$DATA/postgresql.conf"   # needs a restart to activate
"$BIN/pg_ctl" -D "$DATA" -w restart >/dev/null 2>&1
$P -c "CREATE TABLE cts(id int) TABLESPACE ts;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
ctsC=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "SELECT pagestore_ship_slru_snapshot('pg_commit_ts', '$ctsC');" >/dev/null
$P -c "SELECT pagestore_ship_slru_snapshot('pg_xact', '$ctsC');" >/dev/null   # the all-SLRU seeder (20b) needs a clog base too
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
ctsReconMd5=$($P -c "SELECT md5(pagestore_commit_ts_page_asof($ctsPage, '$ctsC', '$ctsL', '3'::xid));")
assert "$ctsSeedMd5" "$ctsReconMd5" "commit-ts seed page == reconstructed as-of-L page"
rm -rf "$CTSSEED"
# an empty horizon [x, x) has nothing to reconstruct but must still publish the
# artifact (a zeroed bootstrap page), including at a page boundary, where the
# naive page math would see page_hi < page_lo and mis-report XID wraparound
EMPTYCTS=$(mktemp -d)
empty_xid=$(( 2 * cts_per_page ))
empty_seeded=$($P -c "SELECT pagestore_seed_commit_ts('$EMPTYCTS', '$ctsC', '$ctsL', '$empty_xid'::text::xid, '$empty_xid'::text::xid);")
assert "$empty_seeded" "1" "commit-ts seed publishes a bootstrap page for an empty horizon at a page boundary"
assert "$([ -d "$EMPTYCTS/pg_commit_ts" ] && echo present)" "present" \
	"empty-horizon commit-ts artifact directory exists"
rm -rf "$EMPTYCTS"

# --- 20b. commit-ts toggle replay: eras across track_commit_timestamp restarts ----------
# The GUC is PGC_POSTMASTER, so a toggle always crosses a restart: OFF fires
# DeactivateCommitTs (every local segment deleted; a XLOG_PARAMETER_CHANGE lands in
# WAL), ON starts a new era whose nextXid page ActivateCommitTs zeroes WITHOUT WAL.
# The appliers track the era from the base's control image through the toggles.
echo "track_commit_timestamp = off" >> "$DATA/postgresql.conf"
"$BIN/pg_ctl" -D "$DATA" -w restart >/dev/null 2>&1
ctsOffL0=$($P -c "SELECT pg_current_wal_lsn();")              # early in the off era
$P -q -c "BEGIN; INSERT INTO cts VALUES (9); COMMIT;" >/dev/null
ctsOffL=$($P -c "SELECT pg_current_wal_lsn();")               # later in the off era
echo "track_commit_timestamp = on" >> "$DATA/postgresql.conf"
"$BIN/pg_ctl" -D "$DATA" -w restart >/dev/null 2>&1
ctsLmid=$($P -c "SELECT pg_current_wal_lsn();")               # after activation, before any era commit
ctsE2=$($P -c "WITH w AS (INSERT INTO cts VALUES (3) RETURNING 1) SELECT pg_current_xact_id();")
ctsLpre=$($P -c "SELECT pg_current_wal_lsn();")               # era commit done, still pre-checkpoint
$P -c "CHECKPOINT;" >/dev/null                                # publish the era horizon
ctsXA=$($P -c "SELECT oldest_commit_ts_xid FROM pg_control_checkpoint();")
ctsL2=$($P -c "SELECT pg_current_wal_lsn();")
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsC', '$ctsL2', '3'::xid) IS NULL;")" "t" \
	"commit-ts toggle: an era-1 timestamp does not survive the era wipe (horizon disabled)"
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsC', '$ctsL2', '$ctsXA'::xid) = pg_xact_commit_timestamp('$ctsE2'::xid);")" "t" \
	"commit-ts toggle: an era-2 commit reconstructs across the unlogged activation zero"
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsC', '$ctsOffL', '3'::xid);" 2>&1 | grep -c 'off as of the target')" "1" \
	"commit-ts toggle: a target inside the off window fails closed"
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsOffL0', '$ctsOffL', '3'::xid);" 2>&1 | grep -c 'off as of the target')" "1" \
	"commit-ts toggle: an entirely-off window fails closed (no toggle record needed)"
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsA'::xid, '$ctsOffL', '$ctsOffL', '3'::xid);" 2>&1 | grep -c 'off as of the target')" "1" \
	"commit-ts toggle: a read exactly at an off-era LSN fails closed (empty window)"
# the activation page exists (all-zero) on the parent BEFORE any era commit touches it:
# ActivateCommitTs created it without WAL, so the applier materializes it from the horizon
ctsXApage=$(( ctsXA / cts_per_page ))
assert "$($P -c "SELECT pagestore_commit_ts_page_asof($ctsXApage, '$ctsC', '$ctsLmid', '$ctsXA'::xid) = decode(repeat('00', $bs), 'hex');")" "t" \
	"commit-ts toggle: the silently-zeroed activation page is served before any era commit"
# a fork BEFORE the first post-activation checkpoint has no pg_control horizon yet;
# the appliers derive it from the toggle-time control image (Invalid oldest = derive)
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsC', '$ctsLpre', '0'::xid) = pg_xact_commit_timestamp('$ctsE2'::xid);")" "t" \
	"commit-ts toggle: a pre-checkpoint fork derives the activation horizon from the control image"
# a 3-argument SQL wrapper created before the horizon argument existed still calls
# the same C symbol; it must default the horizon instead of reading garbage
$P -c "CREATE FUNCTION pagestore_commit_ts_page_asof3(int, pg_lsn, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_commit_ts_page_asof' LANGUAGE C STRICT;" >/dev/null
assert "$($P -c "SELECT pagestore_commit_ts_page_asof3($ctsXApage, '$ctsC', '$ctsLmid') = decode(repeat('00', $bs), 'hex');")" "t" \
	"commit-ts toggle: the legacy 3-argument page-asof ABI still works (horizon defaulted)"
# the 'oldest' argument is only the LOOKUP filter: disabling it with a tiny xid must
# not poison the activation zero-page derivation (which comes from the control image)
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsC', '$ctsLpre', '3'::xid) = pg_xact_commit_timestamp('$ctsE2'::xid);")" "t" \
	"commit-ts toggle: a disabled lookup horizon does not poison the activation page"
# the all-SLRU convenience seeder normalizes the same way: both-Invalid commit-ts
# horizons on an ACTIVE pre-checkpoint fork must seed, not install an empty dir
$P -c "CREATE OR REPLACE FUNCTION pagestore_seed_branch_slrus(text, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_seed_branch_slrus' LANGUAGE C STRICT;" >/dev/null
ALLSEED=$(mktemp -d)
all_next=$(( ctsE2 + 1 ))
read -r allOldMx allNextMx allNextMOff <<< "$($P -c "SELECT oldest_multi_xid::text || ' ' || next_multixact_id::text || ' ' || next_multi_offset FROM pg_control_checkpoint();")"
all_seeded=$($P -c "SELECT pagestore_seed_branch_slrus('$ALLSEED', '$ctsC', '$ctsLpre', '3'::xid, '$all_next'::text::xid, '0'::xid, '0'::xid, '$allOldMx'::xid, '$allNextMx'::xid, $allNextMOff, $allNextMOff);")
assert "$([ -f "$ALLSEED/pg_commit_ts/$(printf '%04X' $(( (ctsXA / cts_per_page) / 32 )))" ] && echo present || echo absent)" "present" \
	"commit-ts toggle: the all-SLRU seeder treats an active pre-checkpoint fork as seedable"
rm -rf "$ALLSEED"
PRESEED=$(mktemp -d)
tog_next=$(( ctsE2 + 1 ))
pre_seeded=$($P -c "SELECT pagestore_seed_commit_ts('$PRESEED', '$ctsC', '$ctsLpre', '0'::xid, '$tog_next'::text::xid);")
assert "$([ "${pre_seeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"commit-ts toggle: seeding a pre-checkpoint fork with no horizon derives one ($pre_seeded page(s))"
rm -rf "$PRESEED"
TOGSEED=$(mktemp -d)
tog_seeded=$($P -c "SELECT pagestore_seed_commit_ts('$TOGSEED', '$ctsC', '$ctsL2', '$ctsXA'::text::xid, '$tog_next'::text::xid);")
assert "$([ "${tog_seeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"commit-ts toggle: seeding with the era horizon succeeds ($tog_seeded page(s))"
# byte check: if xidA shares the seeded era page, its entry must be all-zero there
togPage=$(( ctsXA / cts_per_page ))
if [ "$(( ctsA / cts_per_page ))" = "$togPage" ]; then
	togSeg=$(printf '%04X' $(( togPage / 32 )))
	togEntryOff=$(( (togPage % 32) * bs + (ctsA % cts_per_page) * cts_entry_size ))
	zeros=$($P -c "SELECT pg_read_binary_file('$TOGSEED/pg_commit_ts/$togSeg', $togEntryOff, $cts_entry_size) = decode(repeat('00', $cts_entry_size), 'hex');")
	assert "$zeros" "t" "commit-ts toggle: the seeded era page holds a zero entry where era-1 bytes were"
fi
rm -rf "$TOGSEED"
# an unrelated restart (another PGC_POSTMASTER parameter changed) emits a
# XLOG_PARAMETER_CHANGE that still carries track_commit_timestamp = true; that
# is NOT a transition, and the appliers must not wipe the era for it
echo "max_connections = 120" >> "$DATA/postgresql.conf"
"$BIN/pg_ctl" -D "$DATA" -w restart >/dev/null 2>&1
ctsL3=$($P -c "SELECT pg_current_wal_lsn();")
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsC', '$ctsL3', '$ctsXA'::xid) = pg_xact_commit_timestamp('$ctsE2'::xid);")" "t" \
	"commit-ts toggle: an unrelated parameter-change restart does not wipe the era"
# an off-era BASE: era state initializes from the base's control image; the in-window
# activation resets once, and the unrelated restart after it must not reset again
assert "$($P -c "SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsOffL', '$ctsL3', '$ctsXA'::xid) = pg_xact_commit_timestamp('$ctsE2'::xid);")" "t" \
	"commit-ts toggle: an off-era base replays the activation once and survives later restarts"

# --- 20c. store-backed WAL for the SLRU appliers -----------------------------------------
# pagestore.redo_wal_from_store redirects the appliers' (C, L] scans to the store's
# shipped WAL log, so prepare can run on a compute with no local WAL.  Ship the
# segments containing the toggle window, then reconstruct with the GUC on and match
# the local-mode answers; a window ending in the current partial segment must fail
# the coverage probe (the store holds completed segments only).
$P -c "SELECT pg_switch_wal();" >/dev/null              # complete the window's segment
$P -q -c "BEGIN; INSERT INTO cts VALUES (4); COMMIT;" >/dev/null   # land in the new one
store_cts=""
for i in 1 2 3 4 5 6 7 8 9 10; do
	store_cts=$($P -q -c "SET pagestore.redo_wal_from_store = on;
	                   SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsC', '$ctsL3', '$ctsXA'::xid) = pg_xact_commit_timestamp('$ctsE2'::xid);" 2>/dev/null)
	[ "$store_cts" = "t" ] && break
	sleep 0.5                                            # archiver ships asynchronously
done
assert "$store_cts" "t" \
	"store-backed WAL: the commit-ts applier reconstructs the toggle window from shipped segments"
STORESEED=$(mktemp -d)
store_seeded=$($P -q -c "SET pagestore.redo_wal_from_store = on;
                      SELECT pagestore_seed_commit_ts('$STORESEED', '$ctsC', '$ctsL3', '$ctsXA'::text::xid, '$tog_next'::text::xid);")
assert "$([ "${store_seeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"store-backed WAL: the commit-ts seeder materializes from shipped segments ($store_seeded page(s))"
rm -rf "$STORESEED"
ctsLpartial=$($P -c "SELECT pg_current_wal_lsn();")     # inside the current partial segment
assert "$($P -c "SET pagestore.redo_wal_from_store = on;
                 SELECT pagestore_commit_ts_asof('$ctsE2'::xid, '$ctsC', '$ctsLpartial', '$ctsXA'::xid);" 2>&1 | grep -c 'WAL ends before the target')" "1" \
	"store-backed WAL: a window ending in the current partial segment fails closed"

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
("$BIN/psql" -h "$MAIN_SOCK" -p $PORT -U postgres -tA \
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
bootNext=$($P -c "SELECT pg_snapshot_xmax(pg_current_snapshot());")
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
# MULTIXACT_OFFSETS_PER_PAGE = BLCKSZ/8 (MultiXactOffset is 64-bit), and
# SLRU_PAGES_PER_SEGMENT = 32 (block-size independent)
bs=$($P -c "SELECT current_setting('block_size')::int;")
opp=$(( bs / 8 ))
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
       CREATE OR REPLACE FUNCTION pagestore_seed_branch_slrus(text, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_seed_branch_slrus' LANGUAGE C STRICT;
       CREATE OR REPLACE FUNCTION pagestore_prepare_branch(text, int, int, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
        AS 'pagestore','pagestore_prepare_branch' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_validate_branch_manifest(text, int, int, pg_lsn) RETURNS bool
        AS 'pagestore','pagestore_validate_branch_manifest' LANGUAGE C STRICT;" >/dev/null
mOff=$mxRecon                                          # mA's first member offset (step 20)
# MULTIXACT_MEMBERS_PER_PAGE = (block_size / MULTIXACT_MEMBERGROUP_SIZE) * members-per-group;
# the group is 4 flag bytes + 4 TransactionIds = 20 bytes, 4 members each (block-size derived)
mpp=$(( (bs / 20) * 4 ))
mPage=$(( mOff / mpp ))
mbRecon=$($P -c "SELECT md5(pagestore_multixact_members_page_asof($mPage, '$mxC', '$mxL'));")
# members is a long-segment-name SLRU (15 hex chars) since MultiXactOffset went 64-bit
mbSeg=$(printf '%015X' $(( mPage / 32 )))
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
read -r ctsOldest ctsNewest <<< "$($P -c "SELECT oldest_commit_ts_xid::text || ' ' || newest_commit_ts_xid::text FROM pg_control_checkpoint();")"
ctsNext=$(( (ctsNewest + 1) & 4294967295 ))
if [ "$ctsNext" -lt 3 ]; then ctsNext=3; fi
bootSeeded=$($P -c "SELECT pagestore_seed_branch_slrus('$BOOTSEED', '$mxC', '$mxL',
	'3'::xid, '$bootNext'::xid,
	'$ctsOldest'::xid, '$ctsNext'::xid,
	'$mA'::xid, '$mxNext'::xid,
	$mOff, $((mOff + mxMembers)));")
assert "$([ "${bootSeeded:-0}" -ge 3 ] && echo ok || echo no)" "ok" \
	"branch bootstrap seed materialized pg_xact, pg_commit_ts, and pg_multixact ($bootSeeded page(s))"
bootClogSeg=$(printf '%04X' $(( (ctsA / cxpp) / 32 )))
bootClogPage=$(( (ctsA / cxpp) % 32 ))
bootClogMd5=$($P -c "SELECT md5(pg_read_binary_file('$BOOTSEED/pg_xact/$bootClogSeg', $(( bootClogPage * bs )), $bs));")
bootClogRecon=$($P -c "SELECT md5(pagestore_clog_page_asof($(( ctsA / cxpp )), '$mxC', '$mxL'));")
assert "$bootClogMd5" "$bootClogRecon" "branch bootstrap seed pg_xact page == reconstructed as-of-L page"
bootCtsMd5=$($P -c "SELECT md5(pg_read_binary_file('$BOOTSEED/pg_commit_ts/$ctsSeg', $(( (ctsPage % 32) * bs )), $bs));")
bootCtsRecon=$($P -c "SELECT md5(pagestore_commit_ts_page_asof($ctsPage, '$mxC', '$mxL', '3'::xid));")
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
assert "$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 2, 0, '$mxL');")" "t" \
	"branch manifest validator accepts the prepared branch identity"
assert "$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 3, 0, '$mxL');")" "f" \
	"branch manifest validator rejects the wrong branch timeline"
cp "$PREPSEED/pagestore_branch.manifest" "$PREPSEED/pagestore_branch.manifest.good"
cat > "$PREPSEED/pagestore_branch.manifest" <<EOF
{ "wrapper": { "format": 1, "new_timeline": 2, "parent_timeline": 0, "fork_lsn": "$mxL" } }
EOF
assert "$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 2, 0, '$mxL');")" "f" \
	"branch manifest validator rejects nested manifest fields"
cat > "$PREPSEED/pagestore_branch.manifest" <<EOF
{ "format": 1, "new_timeline": 2, "parent_timeline": 0, "fork_lsn": "$mxL", "extra": @ }
EOF
assert "$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 2, 0, '$mxL');")" "f" \
	"branch manifest validator rejects malformed JSON"
cat > "$PREPSEED/pagestore_branch.manifest" <<EOF
{ "format": 1, "new_timeline": 2, "new_\u0074imeline": 3, "parent_timeline": 0, "fork_lsn": "$mxL" }
EOF
assert "$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 2, 0, '$mxL');")" "f" \
	"branch manifest validator rejects escaped duplicate keys"
cat > "$PREPSEED/pagestore_branch.manifest" <<EOF
{ "format": 1, "new_timeline": 0, "parent_timeline": 0, "fork_lsn": "$mxL" }
EOF
assert "$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 0, 0, '$mxL');")" "f" \
	"branch manifest validator rejects timeline zero"
printf '{ "format": 1, "new_timeline": 2, "parent_timeline": 0, "fork_lsn": "%s" }\0{ "format": 2 }\n' "$mxL" > "$PREPSEED/pagestore_branch.manifest"
nulManifest=$($P -c "SELECT pagestore_validate_branch_manifest('$PREPSEED', 2, 0, '$mxL');" 2>/dev/null || echo ERROR)
assert "$nulManifest" "ERROR" "branch manifest validator rejects embedded NUL bytes"
mv "$PREPSEED/pagestore_branch.manifest.good" "$PREPSEED/pagestore_branch.manifest"
cp "$PREPSEED/pagestore_branch.manifest" "$BRANCHDATA/pagestore_branch.manifest"
cat >> "$BRANCHDATA/postgresql.conf" <<EOF
pagestore.timeline = 2
listen_addresses = ''
unix_socket_directories = '$BRANCH_SOCK'
port = $PORT
EOF
if "$BIN/pg_ctl" -D "$BRANCHDATA" -l "$BRANCHDATA/server.log" -w start >/dev/null 2>&1; then
	echo "FAIL - branch startup accepted a manifest without full routing"
	fail=1
	"$BIN/pg_ctl" -D "$BRANCHDATA" -m immediate -w stop >/dev/null 2>&1 || true
else
	echo "ok   - branch startup rejects a manifest without full routing"
fi
prepClogMd5=$($P -c "SELECT md5(pg_read_binary_file('$PREPSEED/pg_xact/$bootClogSeg', $(( bootClogPage * bs )), $bs));")
assert "$prepClogMd5" "$bootClogRecon" "branch prepare pg_xact page == reconstructed as-of-L page"
prepMxOff=$($P -c "SELECT md5(pg_read_binary_file('$PREPSEED/pg_multixact/offsets/$mxSeg', $(( (mxPage % 32) * bs )), $bs));")
prepMxMem=$($P -c "SELECT md5(pg_read_binary_file('$PREPSEED/pg_multixact/members/$mbSeg', $(( (mPage % 32) * bs )), $bs));")
assert "$prepMxOff" "$mxRP" "branch prepare multixact offsets page == reconstructed as-of-L page"
assert "$prepMxMem" "$mbRecon" "branch prepare multixact members page == reconstructed as-of-L page"
rm -rf "$PREPSEED"

# --- 26. pg_control mirror: control writes publish LSN-versioned store images ---
# Every UpdateControlFile() queues the just-written image (versioned by the LSN
# of the update that caused it) and ships it at the next post-critical point,
# so a branch cut at L can restore pg_control as of L.
$P -c "CREATE FUNCTION pagestore_control_image_asof(pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_control_image_asof' LANGUAGE C STRICT;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT pagestore_control_image_asof(pg_current_wal_lsn()) IS NOT NULL;")" "t" \
	"store holds a mirrored pg_control image as of now"
assert "$($P -c "SELECT octet_length(pagestore_control_image_asof(pg_current_wal_lsn()));")" "8192" \
	"mirrored control image is exactly PG_CONTROL_FILE_SIZE bytes"
assert "$($P -c "SELECT pagestore_control_image_asof('0/1'::pg_lsn) IS NULL;")" "t" \
	"no mirrored control image below the first update LSN (as-of read is capped)"
# the restore tool installs the mirrored image atomically; right after a
# checkpoint the newest image equals the live pg_control byte-for-byte
RESTOREDIR=$(mktemp -d)
mkdir -p "$RESTOREDIR/global"
if "$BUILD/contrib/pagestore/pagestore_control_restore" --shm "$SHM" --timeline 0 "$RESTOREDIR" >/dev/null; then
	echo "ok   - pagestore_control_restore installed the mirrored control image"
else
	echo "FAIL - pagestore_control_restore failed"; fail=1
fi
if cmp -s "$RESTOREDIR/global/pg_control" "$DATA/global/pg_control"; then
	echo "ok   - restored pg_control equals the live control file byte-for-byte"
else
	echo "FAIL - restored pg_control differs from the live control file"; fail=1
fi
ctrl_ok=$("$BIN/pg_controldata" -D "$RESTOREDIR" >/dev/null 2>&1 && echo ok || echo error)
assert "$ctrl_ok" "ok" "pg_controldata accepts the restored control file"
rm -f "$RESTOREDIR/global/pg_control"
if "$BUILD/contrib/pagestore/pagestore_control_restore" --shm "$SHM" --timeline 0 --lsn 0/1 "$RESTOREDIR" >/dev/null 2>&1; then
	echo "FAIL - restore below the first update LSN should fail closed"; fail=1
else
	echo "ok   - restore below the first update LSN fails closed"
fi
assert "$([ -e "$RESTOREDIR/global/pg_control" ] && echo present || echo absent)" "absent" \
	"failed restore leaves no control file behind"
rm -rf "$RESTOREDIR"
# the daemon's durable WAL retention floor: every mirrored control image ships
# a redo-pointer note, and shipped WAL at/above min(redo) must be retained
$P -c "CREATE FUNCTION pagestore_wal_retain_floor() RETURNS pg_lsn
        AS 'pagestore','pagestore_wal_retain_floor' LANGUAGE C;" >/dev/null
assert "$($P -c "SELECT pagestore_wal_retain_floor() IS NOT NULL;")" "t" \
	"store reports a WAL retention floor once control images exist"
assert "$($P -c "SELECT pagestore_wal_retain_floor() <= (SELECT redo_lsn FROM pg_control_checkpoint());")" "t" \
	"WAL retention floor is at/below the current checkpoint redo pointer"

# --- 27. live SLRU mirror: flushed clog pages publish versioned store images ---
# With pagestore.slru_mirror on, SlruInternalWritePage() stages every flushed
# in-scope SLRU page (bank-lock snapshot + WAL fence) and the post-critical
# drain ships it as PS_KLASS_SLRU_LIVE versioned by the fence -- so another
# compute can later observe this one's committed status.  The keyspace is
# distinct from the PS_KLASS_SLRU seed snapshots (which promise a proven
# clean-as-of-cutoff that flushed images do not have).
$P -c "CREATE FUNCTION pagestore_slru_live_read_at(text, int, pg_lsn) RETURNS bytea
        AS 'pagestore','pagestore_slru_live_read_at' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_slru_mirror_stats(OUT staged int, OUT recapture int, OUT lost bigint, OUT read_served bigint, OUT read_fallback bigint) RETURNS record
        AS 'pagestore','pagestore_slru_mirror_stats' LANGUAGE C STRICT;" >/dev/null
$P -c "CREATE TABLE slru_live(x int);" >/dev/null
LIVEXID=$($P -q -c "BEGIN; INSERT INTO slru_live VALUES (1); SELECT (txid_current() % 4294967296)::bigint; COMMIT;")
$P -c "CHECKPOINT;" >/dev/null   # SimpleLruWriteAll stages; the checkpoint's control flush hook drains
LIVE_CXPP=$(( $($P -c "SHOW block_size") * 4 ))
LIVEPAGE=$((LIVEXID / LIVE_CXPP))
assert "$($P -c "SELECT pagestore_slru_live_read_at('pg_xact', $LIVEPAGE, pg_current_wal_lsn()) IS NOT NULL;")" "t" \
	"store holds a live-mirrored pg_xact page after checkpoint"
assert "$($P -c "SELECT (get_byte(pagestore_slru_live_read_at('pg_xact', $LIVEPAGE, pg_current_wal_lsn()), $(((LIVEXID % LIVE_CXPP) / 4))) >> $(((LIVEXID % 4) * 2))) & 3;")" "1" \
	"live-mirrored clog page carries the committed bit for our xid"
assert "$($P -c "SELECT pagestore_slru_live_read_at('pg_xact', $LIVEPAGE, '0/1'::pg_lsn) IS NULL;")" "t" \
	"no live image below the first fence LSN (as-of read is capped)"
assert "$($P -c "SELECT lost FROM pagestore_slru_mirror_stats();")" "0" \
	"this backend never lost an SLRU capture"
assert "$($P -c "SELECT pagestore_slru_live_read_at('pg_subtrans', 0, pg_current_wal_lsn()) IS NULL;" 2>&1 | grep -c 'not an in-scope')" "1" \
	"out-of-scope SLRUs are excluded, not silently store-backed"

# --- 28. SLRU mirror visibility watermark: contiguous durable prefix only ---
# The watermark (mirrored_status_lsn) advances to a completed checkpoint's
# redo pointer once that checkpoint's control image AND every staged SLRU
# image have durably shipped; a reader on another compute may trust the live
# mirror for status at/below it and no further.
$P -c "CREATE FUNCTION pagestore_slru_mirror_watermark() RETURNS pg_lsn
        AS 'pagestore','pagestore_slru_mirror_watermark' LANGUAGE C;
       CREATE FUNCTION pagestore_slru_mirror_reset_debt() RETURNS bigint
        AS 'pagestore','pagestore_slru_mirror_reset_debt' LANGUAGE C;" >/dev/null
# A never-primed mirror starts with boot debt: pre-enable SLRU history was
# never captured, so the watermark must stay frozen until the operator
# declares the mirror whole.
assert "$($P -c "SELECT pagestore_slru_mirror_watermark() IS NULL;")" "t" \
	"watermark stays frozen until the mirror is primed (boot debt)"
$P -c "SELECT pagestore_slru_mirror_reset_debt();" >/dev/null   # prime it
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT pagestore_slru_mirror_watermark() IS NOT NULL;")" "t" \
	"watermark is set after priming + a completed checkpoint's images shipped"
assert "$($P -c "SELECT pagestore_slru_mirror_watermark() <= (SELECT redo_lsn FROM pg_control_checkpoint());")" "t" \
	"watermark never claims more than the last completed checkpoint's redo"
WM1=$($P -c "SELECT pagestore_slru_mirror_watermark();")
$P -q -c "BEGIN; INSERT INTO slru_live VALUES (2); COMMIT;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT pagestore_slru_mirror_watermark() > '$WM1'::pg_lsn;")" "t" \
	"watermark advances across a traffic + checkpoint cycle"

# --- 28b. mirror debt: an unclean shutdown freezes the watermark for good ---
# A crash may kill processes holding staged-but-unsynced images whose pages
# are clean on local disk and will never be flushed (and thus re-captured)
# again.  That hole cannot be proven re-covered, so after a crash boot the
# watermark must stay frozen -- persistently, via the debt marker -- until an
# operator re-primes the mirror and explicitly resets the debt.
"$BIN/pg_ctl" -D "$DATA" -m immediate -w stop >/dev/null 2>&1
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -q -c "BEGIN; INSERT INTO slru_live VALUES (3); COMMIT;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT pagestore_slru_mirror_watermark() IS NULL;")" "t" \
	"watermark stays frozen after a crash boot (boot debt)"
assert "$(test -f "$DATA/pagestore.slru_mirror_debt" && echo t)" "t" \
	"the boot debt is persisted as a marker file"
assert "$($P -c "SELECT pagestore_slru_mirror_reset_debt() >= 1;")" "t" \
	"reset_debt reports and clears the outstanding losses"
assert "$(test -f "$DATA/pagestore.slru_mirror_debt" || echo t)" "t" \
	"reset_debt removes the marker file"
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT pagestore_slru_mirror_watermark() IS NOT NULL;")" "t" \
	"watermark advances again once the debt is reset"

# --- 29. SLRU live reads: transaction status served from the mirror ---
# Restart the compute with pagestore.slru_live_reads on and the local
# pg_xact segment hidden: startup's clog read and the status lookup for our
# committed xid can then only be answered by the live mirror (gated on the
# published watermark).  A clean shutdown checkpoint ships + publishes.
"$BIN/pg_ctl" -D "$DATA" -m fast stop >/dev/null
echo "pagestore.slru_live_reads = on" >> "$DATA/postgresql.conf"
mv "$DATA/pg_xact/0000" "$DATA/pg_xact/0000.hidden"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null
assert "$($P -c "SELECT txid_status($LIVEXID);")" "committed" \
	"transaction status answered with the local pg_xact segment gone (live mirror serves the read)"
# the segment may have been recreated by post-recovery clog writes; restore
# the original only if it was not
[ -e "$DATA/pg_xact/0000" ] || mv "$DATA/pg_xact/0000.hidden" "$DATA/pg_xact/0000"
rm -f "$DATA/pg_xact/0000.hidden"

# --- 30. SLRU truncation tombstones: durable before local deletion ---
# slru_truncate_hook publishes a durable cutoff tombstone BEFORE any local
# segment is deleted; a live-mirror reader must treat pages below the newest
# tombstone at/below its LSN as dead, whatever images exist.
$P -c "CREATE FUNCTION pagestore_slru_tombstone_asof(text, pg_lsn) RETURNS bigint
        AS 'pagestore','pagestore_slru_tombstone_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_slru_mirror_truncate(text, bigint) RETURNS void
        AS 'pagestore','pagestore_slru_mirror_truncate' LANGUAGE C STRICT;" >/dev/null
# priming (28/28b's reset_debt) re-derives tombstones from local truth: with
# segment 0000 present the published cutoff is 0 -- "nothing dead" -- which
# supersedes nothing but proves the re-derivation ran
assert "$($P -c "SELECT pagestore_slru_tombstone_asof('pg_xact', pg_current_wal_lsn());")" "0" \
	"priming re-derived a nothing-dead tombstone from the local segments"
TOMB_BEFORE=$($P -c "SELECT pg_current_wal_lsn();")
$P -q -c "BEGIN; INSERT INTO slru_live VALUES (3); COMMIT;" >/dev/null	# advance WAL past TOMB_BEFORE
$P -c "SELECT pagestore_slru_mirror_truncate('pg_xact', 1);" >/dev/null
assert "$($P -c "SELECT pagestore_slru_tombstone_asof('pg_xact', pg_current_wal_lsn());")" "1" \
	"tombstone publishes the truncation cutoff page"
assert "$($P -c "SELECT pagestore_slru_tombstone_asof('pg_xact', '$TOMB_BEFORE'::pg_lsn);")" "0" \
	"the pre-truncation as-of still sees only the priming cutoff (as-of read is capped)"
$P -c "SELECT pagestore_slru_mirror_truncate('pg_xact', 3);" >/dev/null
assert "$($P -c "SELECT pagestore_slru_tombstone_asof('pg_xact', pg_current_wal_lsn());")" "3" \
	"a later truncation supersedes the tombstone cutoff"

# --- 31. pinned reader: a compute serves its timeline history at a frozen LSN --------
# READ_CONSISTENCY_DESIGN.md increment 1: pagestore.read_lsn caps every store
# relation read at R (the redo of a durably mirrored checkpoint -- complete by
# construction) and refuses store mutations.  History stays frozen: an update
# checkpointed after R must not be visible to the pinned compute.
# Move the writer to full routing before choosing R.  Import the quiesced local
# default/global catalogs first; the following checkpoint puts both those
# imported pages and the already-routed user-tablespace pages behind R's
# admission fence.
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1
"$BUILD/contrib/pagestore/pagestore_import" --shm "$SHM" --pgdata "$DATA" >/dev/null 2>&1
echo "pagestore.route_all = on" >> "$DATA/postgresql.conf"
echo "max_prepared_transactions = 10" >> "$DATA/postgresql.conf"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -c "CREATE TABLE reader_t(id int primary key, v text) TABLESPACE ts;
       INSERT INTO reader_t VALUES (1, 'v1');
	   CREATE TABLE reader_running(id int primary key) TABLESPACE ts;
	   CREATE TABLE reader_subxid(i int) TABLESPACE ts;
       CREATE SEQUENCE reader_seq;
       CREATE UNLOGGED TABLE reader_unlogged(i int) TABLESPACE ts;
       INSERT INTO reader_unlogged VALUES (1), (2);" >/dev/null
$P -c "CREATE FUNCTION pagestore_prepare_reader(text, int, pg_lsn, pg_lsn, xid, xid, xid, xid, xid, xid, bigint, bigint) RETURNS bigint
         AS 'pagestore','pagestore_prepare_reader' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_install_prepared_reader(text, text, int, pg_lsn) RETURNS void
         AS 'pagestore','pagestore_install_prepared_reader' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_validate_reader_manifest(text, int, pg_lsn) RETURNS bool
         AS 'pagestore','pagestore_validate_reader_manifest' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_mark_reader_catalog_snapshot(text, int, pg_lsn) RETURNS void
         AS 'pagestore','pagestore_mark_reader_catalog_snapshot' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_reader_candidate_lsn() RETURNS pg_lsn
         AS 'pagestore','pagestore_reader_candidate_lsn' LANGUAGE C;
       CREATE FUNCTION pagestore_reader_candidate_generation() RETURNS bigint
         AS 'pagestore','pagestore_reader_candidate_generation' LANGUAGE C;
       CREATE FUNCTION pagestore_reader_effective_lsn() RETURNS pg_lsn
         AS 'pagestore','pagestore_reader_effective_lsn' LANGUAGE C;
       CREATE FUNCTION pagestore_reader_effective_generation() RETURNS bigint
         AS 'pagestore','pagestore_reader_effective_generation' LANGUAGE C;
       CREATE FUNCTION pagestore_publish_reader_snapshot_artifact(text, int, pg_lsn) RETURNS bigint
         AS 'pagestore','pagestore_publish_reader_snapshot_artifact' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_validate_published_reader_snapshot(int, pg_lsn) RETURNS bigint
         AS 'pagestore','pagestore_validate_published_reader_snapshot' LANGUAGE C STRICT;" >/dev/null
# A prepared XID remains in progress across the stopped copy at R.  Its 20000
# released subtransactions exceed the normal snapshot subxid capacity; the
# writer commits it only after the copy, so the reader has no post-R relation
# WAL to replay during startup.
READER_SUBXID_SQL=$(mktemp)
{
	printf 'BEGIN; INSERT INTO reader_running VALUES (1);\n'
	for _ in $(seq 1 20000); do
		printf 'SAVEPOINT s; INSERT INTO reader_subxid VALUES (1); RELEASE SAVEPOINT s;\n'
	done
	printf "PREPARE TRANSACTION 'reader_running_at_r';\n"
} > "$READER_SUBXID_SQL"
$P -f "$READER_SUBXID_SQL" >/dev/null
rm -f "$READER_SUBXID_SQL"
$P -c "CHECKPOINT;" >/dev/null
read -r readerR readerNext readerOldest readerNextMulti readerNextMember readerOldestMulti readerCtsOldest readerCtsNext <<< "$($P -c "
	SELECT redo_lsn || ' ' || split_part(next_xid, ':', 2) || ' ' || oldest_xid || ' ' ||
	       next_multixact_id || ' ' || next_multi_offset || ' ' || oldest_multi_xid || ' ' ||
	       CASE WHEN oldest_commit_ts_xid::text = '0' THEN '1' ELSE oldest_commit_ts_xid::text END || ' ' ||
	       CASE WHEN newest_commit_ts_xid::text = '0' THEN '1' ELSE ((newest_commit_ts_xid::text::bigint + 1) & 4294967295)::text END
	FROM pg_control_checkpoint();")"
# This test has no concurrent catalog-changing workload across the checkpoint,
# so its stopped copy is the control-plane catalog artifact for R.  The
# prepared reader bundle replaces its SLRUs; pg_control is restored
# independently because PostgreSQL reads it before shared_preload_libraries.
"$BIN/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1
READERDATA=$(mktemp -d)/reader
cp -a "$DATA" "$READERDATA"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
$P -c "COMMIT PREPARED 'reader_running_at_r';" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT count(*) FROM reader_running;")" "1" \
	"writer sees the prepared transaction committed after R"
readerRunningXid=$($P -c "SELECT xmin::text FROM reader_running;")
READERPREP=$(mktemp -d)
BADREADERPREP=$(mktemp -d)
readerDbOid=$($P -c "SELECT oid FROM pg_database WHERE datname = current_database();")
mkdir -p "$READERPREP/relmaps/global" "$READERPREP/relmaps/$readerDbOid"
cp "$READERDATA/global/pg_filenode.map" "$READERPREP/relmaps/global/"
cp "$READERDATA/base/$readerDbOid/pg_filenode.map" "$READERPREP/relmaps/$readerDbOid/"
bad_reader_horizon=$($P -c "SELECT pagestore_prepare_reader('$BADREADERPREP', 0, '$bc', '$readerR',
	'$((readerOldest + 1))'::xid, '$readerNext'::xid,
	'$readerCtsOldest'::xid, '$readerCtsNext'::xid,
	'$readerOldestMulti'::xid, '$readerNextMulti'::xid,
	0, $readerNextMember);" >/dev/null 2>&1 && echo ok || echo error)
assert "$bad_reader_horizon" "error" \
	"reader prepare rejects XID horizons that do not match checkpoint R"
rm -rf "$BADREADERPREP"
readerSeeded=$($P -c "SELECT pagestore_prepare_reader('$READERPREP', 0, '$bc', '$readerR',
	'$readerOldest'::xid, '$readerNext'::xid,
	'$readerCtsOldest'::xid, '$readerCtsNext'::xid,
	'$readerOldestMulti'::xid, '$readerNextMulti'::xid,
	0, $readerNextMember);")
assert "$([ "${readerSeeded:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"reader prepare materializes local SLRUs as of checkpoint R"
assert "$($P -c "SELECT pagestore_validate_reader_manifest('$READERPREP', 0, '$readerR');")" "t" \
	"reader manifest records the source timeline and read horizon"
readerSnapshotBlocks=$($P -c "SELECT pagestore_publish_reader_snapshot_artifact('$READERPREP', 0, '$readerR');")
assert "$([ "${readerSnapshotBlocks:-0}" -gt 1 ] && echo ok || echo no)" "ok" \
	"reader snapshot publishes as a multi-block page-store artifact"
# A prepared snapshot is inseparable from its timeline and R identity.  A
# rewritten manifest cannot reuse another timeline's running-XID snapshot.
BRANCHREADERPREP=$(mktemp -d)
cp -a "$READERPREP/." "$BRANCHREADERPREP"
sed -i 's/"timeline": 0/"timeline": 1/' "$BRANCHREADERPREP/pagestore_reader.manifest"
sed -i "/\"timeline\": 1,/a\\  \"parent_timeline\": 0,\\n  \"fork_lsn\": \"$bL\"," \
	"$BRANCHREADERPREP/pagestore_reader.manifest"
BRANCHREADERTARGET=$(mktemp -d)
branch_reader_install=$($P -c "SELECT pagestore_install_prepared_reader('$BRANCHREADERPREP', '$BRANCHREADERTARGET', 1, '$readerR');" \
	>/dev/null 2>&1 && echo ok || echo error)
assert "$branch_reader_install" "error" \
	"reader install rejects a snapshot from a different timeline"
rm -rf "$BRANCHREADERPREP" "$BRANCHREADERTARGET"
# Emulate local recovery having replayed the later commit: newest pg_xact says
# committed, but the fixed running-XID snapshot must retain R's visibility.
if "$BUILD/contrib/pagestore/pagestore_control_restore" --shm "$SHM" --timeline 0 --lsn "$readerR" "$READERDATA" >/dev/null; then
	echo "ok   - reader bootstrap restored pg_control at exact R"
else
	echo "FAIL - reader bootstrap could not restore pg_control at exact R"; fail=1
fi
$P -c "SELECT pagestore_mark_reader_catalog_snapshot('$READERDATA', 0, '$readerR');" >/dev/null
$P -c "SELECT pagestore_install_prepared_reader('$READERPREP', '$READERDATA', 0, '$readerR');" >/dev/null
# Emulate local recovery having replayed the later commit: newest pg_xact says
# committed, but the fixed running-XID snapshot must retain R's visibility.
cp "$DATA/pg_xact/"* "$READERDATA/pg_xact/"
rm -f "$READERDATA/pg_twophase/"*
rm -rf "$READERPREP"
$P -c "UPDATE reader_t SET v = 'v2' WHERE id = 1;" >/dev/null
readerV2Xid=$($P -c "SELECT xmin::text FROM reader_t WHERE id = 1;")
$P -c "CHECKPOINT;" >/dev/null                     # v2 page version ships above R
assert "$($P -c "SELECT v FROM reader_t WHERE id = 1;")" "v2" "writer sees the newest row version"
read -r readerR2 readerNext2 readerOldest2 readerNextMulti2 readerNextMember2 readerOldestMulti2 readerCtsOldest2 readerCtsNext2 <<< "$($P -c "
	SELECT redo_lsn || ' ' || split_part(next_xid, ':', 2) || ' ' || oldest_xid || ' ' ||
	       next_multixact_id || ' ' || next_multi_offset || ' ' || oldest_multi_xid || ' ' ||
	       CASE WHEN oldest_commit_ts_xid::text = '0' THEN '1' ELSE oldest_commit_ts_xid::text END || ' ' ||
	       CASE WHEN newest_commit_ts_xid::text = '0' THEN '1' ELSE ((newest_commit_ts_xid::text::bigint + 1) & 4294967295)::text END
	FROM pg_control_checkpoint();")"
READERPREP2=$(mktemp -d)
mkdir -p "$READERPREP2/relmaps/global" "$READERPREP2/relmaps/$readerDbOid"
cp "$DATA/global/pg_filenode.map" "$READERPREP2/relmaps/global/"
cp "$DATA/base/$readerDbOid/pg_filenode.map" "$READERPREP2/relmaps/$readerDbOid/"
assert "$($P -c "SELECT pagestore_clog_status_asof('$readerV2Xid'::xid, '$bc', '$readerR2');")" "1" \
	"the newer reader horizon reconstructs the v2 transaction as committed"
$P -c "SELECT pagestore_prepare_reader('$READERPREP2', 0, '$bc', '$readerR2',
	'$readerOldest2'::xid, '$readerNext2'::xid,
	'$readerCtsOldest2'::xid, '$readerCtsNext2'::xid,
	'$readerOldestMulti2'::xid, '$readerNextMulti2'::xid,
	0, $readerNextMember2);" >/dev/null
READER_SOCK=$(new_sockdir reader)
cat >> "$READERDATA/postgresql.conf" <<EOF
pagestore.read_lsn = '$readerR'
pagestore.advance_read_lsn = on
archive_mode = off
listen_addresses = ''
unix_socket_directories = '$READER_SOCK'
port = $PORT
EOF
# A pin without its matching artifact must fail closed at startup.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
rm "$BADREADER/pagestore_reader.manifest"
cat >> "$BADREADER/postgresql.conf" <<EOF
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_arch_started=1 || pin_arch_started=0
assert "$pin_arch_started" "0" "pinned start without a reader manifest is refused"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
# A catalog snapshot is part of the reader identity, not an optional control-plane note.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
rm "$BADREADER/pagestore_reader.catalog"
cat >> "$BADREADER/postgresql.conf" <<EOF
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_catalog_started=1 || pin_catalog_started=0
assert "$pin_catalog_started" "0" "pinned start without catalog provenance is refused"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
# Catalog provenance corruption is detected independently of its manifest token.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
printf '\001' | dd of="$BADREADER/pagestore_reader.catalog" bs=1 seek=0 conv=notrunc status=none
cat >> "$BADREADER/postgresql.conf" <<EOF
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_catalog_crc_started=1 || pin_catalog_crc_started=0
assert "$pin_catalog_crc_started" "0" "pinned start with corrupt catalog provenance is refused"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
# A manifest without its CRC-protected running-XID snapshot is incomplete.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
rm "$BADREADER/pagestore_reader.snapshot"
cat >> "$BADREADER/postgresql.conf" <<EOF
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_snapshot_started=1 || pin_snapshot_started=0
assert "$pin_snapshot_started" "0" "pinned start without a running-XID snapshot is refused"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
# Corruption is detected independently of the manifest identity checks.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
printf '\001' | dd of="$BADREADER/pagestore_reader.snapshot" bs=1 seek=0 conv=notrunc status=none
cat >> "$BADREADER/postgresql.conf" <<EOF
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_snapshot_crc_started=1 || pin_snapshot_crc_started=0
assert "$pin_snapshot_crc_started" "0" "pinned start with a corrupt running-XID snapshot is refused"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
# A prepared reader must route default/global relations through the store too;
# otherwise local md pages could expose state newer than the pin.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
cat >> "$BADREADER/postgresql.conf" <<EOF
pagestore.route_all = off
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_route_started=1 || pin_route_started=0
assert "$pin_route_started" "0" "pinned start without full store routing is refused"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
echo "pagestore.route_all = on" >> "$READERDATA/postgresql.conf"
# Startup must reject a reader whose manifest was moved to another timeline.
# Catalog provenance is checked before daemon ancestry, so the copied timeline-0
# artifact prevents the forged timeline-1 manifest from reaching that lookup.
BADREADER=$(mktemp -d)/reader
cp -a "$READERDATA" "$BADREADER"
BADREADER_SOCK=$(new_sockdir badreader)
sed -i 's/"timeline": 0/"timeline": 1/' "$BADREADER/pagestore_reader.manifest"
sed -i "/\"timeline\": 1,/a\\  \"parent_timeline\": 0,\\n  \"fork_lsn\": \"$readerR\"," \
	"$BADREADER/pagestore_reader.manifest"
cat >> "$BADREADER/postgresql.conf" <<EOF
pagestore.timeline = 1
unix_socket_directories = '$BADREADER_SOCK'
EOF
"$BIN/pg_ctl" -D "$BADREADER" -l "$BADREADER/server.log" -w start >/dev/null 2>&1 && pin_branch_started=1 || pin_branch_started=0
assert "$pin_branch_started" "0" "pinned branch reader rejects forged ancestry at startup"
assert "$(grep -c 'reader catalog provenance.*invalid identity' "$BADREADER/server.log" || true)" "1" \
	"pinned branch startup binds catalog provenance to its timeline"
"$BIN/pg_ctl" -D "$BADREADER" -m immediate -w stop >/dev/null 2>&1 || true
rm -rf "$(dirname "$BADREADER")"
BADREADER=
if ! "$BIN/pg_ctl" -D "$READERDATA" -l "$READERDATA/server.log" -w start >/dev/null 2>&1; then
	echo "FAIL - prepared reader did not start"
	tail -100 "$READERDATA/server.log" 2>/dev/null || true
	exit 1
fi
PR="$BIN/psql -X -h $READER_SOCK -At -p $PORT -U postgres postgres"
if ! $PR -c "SELECT 1;" >/dev/null 2>&1; then
	echo "FAIL - prepared reader did not accept connections"
	tail -100 "$READERDATA/server.log" 2>/dev/null || true
	exit 1
fi
assert "$($PR -c "SELECT pagestore_reader_candidate_lsn() > '$readerR'::pg_lsn;")" "t" \
	"advancing reader discovers a newer durable checkpoint horizon"
assert "$($PR -c "SELECT pagestore_reader_candidate_generation() >= 2;")" "t" \
	"a newer candidate receives a new shared read generation"
assert "$($PR -c "SELECT current_setting('pagestore.read_lsn')::pg_lsn = '$readerR'::pg_lsn;")" "t" \
	"candidate discovery does not move the effective view without its snapshot"
assert "$($PR -c "SELECT pagestore_reader_effective_lsn() = '$readerR'::pg_lsn AND pagestore_reader_effective_generation() = 1;")" "t" \
	"the backend keeps its initial effective view generation while the candidate snapshot is absent"
assert "$($PR -c "SELECT pagestore_validate_published_reader_snapshot(0, '$readerR') > 20000;")" "t" \
	"reader loads and validates the exact-R multi-block snapshot from the page store"
missingPublishedSnapshot=$($PR -c "SELECT pagestore_validate_published_reader_snapshot(0, pagestore_reader_candidate_lsn());" \
	>/dev/null 2>&1 && echo ok || echo error)
assert "$missingPublishedSnapshot" "error" \
	"reader rejects a candidate horizon whose snapshot has not been published"
reader_v=$($PR -c "SELECT v FROM reader_t WHERE id = 1;")
if [ "$reader_v" != "v1" ]; then
	tail -100 "$READERDATA/server.log" 2>/dev/null || true
fi
assert "$reader_v" "v1" \
	"pinned reader serves the row as of R (an update checkpointed after R is invisible)"
assert "$($PR -c "SELECT count(*) FROM reader_running;")" "0" \
	"pinned reader keeps a transaction that was running at R invisible after its commit"
assert "$($PR -c "SELECT count(*) FROM reader_subxid;")" "0" \
	"pinned reader keeps subtransactions beyond normal snapshot capacity invisible"
assert "$($PR -c "SELECT pg_visible_in_snapshot('$readerRunningXid'::xid8, pg_current_snapshot());")" "f" \
	"pg_current_snapshot preserves the pinned reader running-XID set"
reader_export=$($PR -c "SELECT pg_export_snapshot();" 2>&1)
assert "$(printf '%s\n' "$reader_export" | grep -c 'cannot export a snapshot on an advancing pagestore reader')" "1" \
	"advancing reader rejects exporting a snapshot without its read view"
assert "$($PR -c "UPDATE reader_t SET v = 'v3' WHERE id = 1;" 2>&1 | grep -c 'not allowed on a pinned reader')" "1" \
	"pinned reader refuses writes"
# the read-only default is advisory on a normal server; on a pinned reader the
# escape itself is refused (transaction_read_only_forced, the recovery model)
assert "$($PR -c "BEGIN; SET TRANSACTION READ WRITE; UPDATE reader_t SET v = 'v3' WHERE id = 1; COMMIT;" 2>&1 \
		| grep -c 'cannot set transaction read-write mode on a read-only instance')" "1" \
	"pinned reader refuses SET TRANSACTION READ WRITE outright"
# write-capable SELECTs and DDL hold against the forced read-only state
assert "$($PR -c "SELECT nextval('reader_seq');" 2>&1 | grep -c 'read-only')" "1" \
	"pinned reader refuses nextval() (side-effecting SELECT)"
assert "$($PR -c "CREATE TABLE reader_ddl(i int);" 2>&1 | grep -c 'read-only')" "1" \
	"pinned reader refuses DDL"
# WAL-less (unlogged) pages carry version LSN 0: checkpoint R does not prove
# them complete, so even an admission fence must fail closed.
assert "$($PR -c "SELECT count(*) FROM reader_unlogged;" 2>&1 | grep -c 'daemon reported error')" "1" \
	"pinned reader refuses WAL-less (unlogged) relation reads"
# CHECKPOINT would make the (exempt) checkpointer insert private WAL
assert "$($PR -c "CHECKPOINT;" 2>&1 | grep -c 'not allowed on a pinned reader')" "1" \
	"pinned reader refuses manual CHECKPOINT"
# EXPLAIN ANALYZE CTAS executes the table creation behind the utility gate
assert "$($PR -c "EXPLAIN (ANALYZE) CREATE TABLE reader_ctas AS SELECT 1;" 2>&1 \
		| grep -c 'not allowed on a pinned reader')" "1" \
	"pinned reader refuses EXPLAIN ANALYZE CREATE TABLE AS"
# prepared-transaction commands are read-only-legal but write XACT WAL in
# critical sections; the utility gate must refuse them cleanly
assert "$($PR -c "COMMIT PREPARED 'nope';" 2>&1 | grep -c 'not allowed on a pinned reader')" "1" \
	"pinned reader refuses COMMIT PREPARED"
# XID assignment is refused at the source (else the commit record would PANIC)
assert "$($PR -c "SELECT pg_current_xact_id();" 2>&1 | grep -c 'cannot assign TransactionIds')" "1" \
	"pinned reader refuses XID assignment (pg_current_xact_id)"
# NOTIFY is read-only-legal but XID-assigning and SLRU-writing
assert "$($PR -c "NOTIFY pinned_chan;" 2>&1 | grep -c 'not allowed on a pinned reader')" "1" \
	"pinned reader refuses NOTIFY"
# VACUUM is legal in read-only transactions and reaches prune/freeze WAL paths: the utility gate must refuse it
assert "$($PR -c "VACUUM reader_t;" 2>&1 | grep -c 'not allowed on a pinned reader')" "1" \
	"pinned reader refuses VACUUM"
readerSnapshotBlocks2=$($P -c "SELECT pagestore_publish_reader_snapshot_artifact('$READERPREP2', 0, '$readerR2');")
assert "$([ "${readerSnapshotBlocks2:-0}" -gt 0 ] && echo ok || echo no)" "ok" \
	"control plane publishes the exact snapshot for the newer reader horizon"
assert "$($PR -c "SELECT pagestore_reader_effective_lsn() = '$readerR2'::pg_lsn AND pagestore_reader_effective_generation() >= 2;")" "t" \
	"the next transaction atomically adopts the published reader view"
assert "$($PR -c "SELECT pg_visible_in_snapshot('$readerV2Xid'::xid8, pg_current_snapshot());")" "t" \
	"the adopted exact-R snapshot treats the v2 transaction as committed"
assert "$($PR -c "SELECT v FROM reader_t WHERE id = 1;")" "v2" \
	"the adopted reader view serves pages from the newer horizon"
assert "$($PR -c "SELECT count(*) FROM reader_running;")" "1" \
	"the adopted exact-R snapshot exposes transactions committed before the newer horizon"
assert "$($PR -c "SELECT pg_last_committed_xact();" 2>&1 | grep -c 'not supported on an advancing pagestore reader')" "1" \
	"the adopted reader does not expose a last-commit cache from another horizon"
assert "$($PR -c "SET max_parallel_workers_per_gather = 4;
	SET debug_parallel_query = on;
	SET min_parallel_table_scan_size = 0;
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	EXPLAIN SELECT count(*) FROM reader_subxid;" | grep -c Gather)" "0" \
	"advancing readers cannot re-enable parallel plans with session settings"
rm -rf "$READERPREP2"
"$BIN/pg_ctl" -D "$READERDATA" -w stop >/dev/null 2>&1
assert "$($P -c "SELECT v FROM reader_t WHERE id = 1;")" "v2" "unpinned compute sees the newest version again"
rm -rf "$(dirname "$READERDATA")"
READERDATA=

# --- 32. as-of fork metadata: NBLOCKS/EXISTS resolve at a horizon ------------
# The store versions fork sizes (page-append growth at each block's pd_lsn,
# LSN-stamped truncate/create/unlink events), so a pinned reader's smgr
# NBLOCKS/EXISTS answer as of R instead of leaking writer-side truncates and
# drops into the frozen view.
$P -c "CREATE FUNCTION pagestore_rel_nblocks_asof(regclass, int, pg_lsn) RETURNS int8
        AS 'pagestore','pagestore_rel_nblocks_asof' LANGUAGE C STRICT;
       CREATE FUNCTION pagestore_rel_exists_asof(regclass, int, pg_lsn) RETURNS bool
        AS 'pagestore','pagestore_rel_exists_asof' LANGUAGE C STRICT;" >/dev/null
preCREATE=$($P -c "SELECT pg_current_wal_lsn();")
$P -c "CREATE TABLE asof_t(id int, filler text) TABLESPACE ts;" >/dev/null
$P -q -c "INSERT INTO asof_t SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g;" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
asofR=$($P -c "SELECT pg_current_wal_lsn();")
szR=$($P -c "SELECT pagestore_rel_nblocks_asof('asof_t', 0, '$asofR'::pg_lsn);")
assert "$($P -c "SELECT pagestore_rel_exists_asof('asof_t', 0, '$preCREATE'::pg_lsn);")" "f" \
	"the fork does not exist at a horizon below its creation"
assert "$($P -c "SELECT $szR > 10;")" "t" "the shipped table has a real page count at R"
$P -q -c "DELETE FROM asof_t WHERE id > 10;" >/dev/null
$P -c "VACUUM asof_t;" >/dev/null	# trims trailing pages: an LSN-stamped store truncate
$P -c "CHECKPOINT;" >/dev/null
szNow=$($P -c "SELECT pagestore_rel_nblocks_asof('asof_t', 0, pg_current_wal_lsn());")
assert "$($P -c "SELECT $szNow < $szR;")" "t" "the newest horizon sees the vacuum-truncated size"
assert "$szNow" "1" \
	"the newest as-of size is the vacuum-truncated one page (rows 1-10 live in block 0)"
assert "$($P -c "SELECT count(*) FROM asof_t;")" "10" \
	"the truncated table still serves its surviving rows"
assert "$($P -c "SELECT pagestore_rel_nblocks_asof('asof_t', 0, '$asofR'::pg_lsn);")" "$szR" \
	"the pre-truncate horizon still sees the pre-truncate size (frozen view)"
# WAL-less (unlogged) pages carry pd_lsn 0; their growth must order at the
# create event's floor, not sort under it and leave the fork looking empty
$P -c "CREATE UNLOGGED TABLE unlogged_t(i int) TABLESPACE ts;" >/dev/null
$P -q -c "INSERT INTO unlogged_t SELECT generate_series(1, 100);" >/dev/null
$P -c "CHECKPOINT;" >/dev/null
assert "$($P -c "SELECT pagestore_rel_nblocks_asof('unlogged_t', 0, pg_current_wal_lsn()) > 0;")" "t" \
	"an unlogged table's WAL-less growth raises the store's newest size"

echo "----"
[ "$fail" = 0 ] && echo "integration test: PASS" || echo "integration test: FAIL"
exit $fail
