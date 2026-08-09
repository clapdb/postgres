#!/usr/bin/env bash
#
# mvp_golden_test.sh -- compose the pagestore MVP data path in one topology.
#
# The test deliberately starts with a WAL-only writer: its relation pages stay
# local while a separate recovery worker replays archived WAL with route_all=on.
# Once the worker has published a durable materialized horizon past the chosen
# workload checkpoint, the test forks at that horizon, restarts the store and
# both parent processes, and boots an independent branch compute.  The parent then advances
# and is materialized past the fork, proving that the branch's ancestry cutoff
# -- rather than timing or a missing parent page -- provides isolation.
#
# Self-asserting; needs a full PostgreSQL build.  Pass the meson build dir as $1.
#
set -uo pipefail

BUILD=${1:?usage: mvp_golden_test.sh <meson-build-dir>}
BUILD=$(CDPATH= cd -- "$BUILD" && pwd) || {
	echo "FAIL - cannot resolve build directory: $BUILD"
	exit 1
}
PGCTL=$(find "$BUILD/tmp_install" -path '*/bin/pg_ctl' -type f 2>/dev/null | head -1)
[ -z "$PGCTL" ] && { echo "FAIL - no tmp_install"; exit 1; }
BIN=$(dirname "$PGCTL")
ROOT=$(dirname "$BIN")
export LD_LIBRARY_PATH="$ROOT/lib:$ROOT/lib64"
DAEMON="$BUILD/contrib/pagestore/pagestore_daemon"
IMPORT="$BUILD/contrib/pagestore/pagestore_import"
INSPECT="$BUILD/contrib/pagestore/pagestore_inspect"
WALRESTORE="$BUILD/contrib/pagestore/pagestore_walrestore"
CONTROLRESTORE="$BUILD/contrib/pagestore/pagestore_control_restore"
BRANCHPREP="$BIN/pagestore_branch_prepare"

TMPROOT=$(mktemp -d)
WRITER="$TMPROOT/writer"
MATERIALIZER="$TMPROOT/materializer"
BRANCH="$TMPROOT/branch"
PREPARED="$TMPROOT/prepared"
STORE="$TMPROOT/store"
BRANCH_SCRATCH="$TMPROOT/branch-walredo"
PRIVATE_SOCKET="$TMPROOT/private-writer-socket"
BRANCH_CONFIG="$TMPROOT/branch-prepare.json"
SHM=/psmvpgolden_$$
WPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
MPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
BPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
PRIVATE_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
WP=("$BIN/psql" -h 127.0.0.1 -p "$WPORT" -U postgres -tA -v ON_ERROR_STOP=1)
MP=("$BIN/psql" -h 127.0.0.1 -p "$MPORT" -U postgres -tA -v ON_ERROR_STOP=1)
BP=("$BIN/psql" -h 127.0.0.1 -p "$BPORT" -U postgres -tA -v ON_ERROR_STOP=1)

cleanup()
{
	"$BIN/pg_ctl" -D "$BRANCH" -m immediate -w stop >/dev/null 2>&1 || true
	"$BIN/pg_ctl" -D "$MATERIALIZER" -m immediate -w stop >/dev/null 2>&1 || true
	"$BIN/pg_ctl" -D "$WRITER" -m immediate -w stop >/dev/null 2>&1 || true
	if [ -n "${DPID:-}" ]; then
		kill "$DPID" 2>/dev/null || true
		wait "$DPID" 2>/dev/null || true
	fi
	rm -rf "$TMPROOT"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

fail()
{
	echo "FAIL - $1"
	echo "writer log:"
	tail -30 "$WRITER/writer.log" 2>/dev/null || true
	echo "materializer log:"
	tail -40 "$MATERIALIZER/materializer.log" 2>/dev/null || true
	echo "branch log:"
	tail -40 "$BRANCH/branch.log" 2>/dev/null || true
	echo "daemon/import log:"
	tail -40 "$TMPROOT/daemon.log" "$TMPROOT/import.log" 2>/dev/null || true
	exit 1
}

assert_eq()
{
	local actual=$1 expected=$2 message=$3

	[ "$actual" = "$expected" ] ||
		fail "$message (got '$actual', expected '$expected')"
	echo "ok   - $message"
}

wal_segment_size()
{
	local data_dir=$1 size=

	size=$("$BIN/pg_controldata" "$data_dir" |
		awk -F': *' '$1 == "Bytes per WAL segment" { print $2; exit }') || return 1
	case "$size" in
		''|*[!0-9]*) return 1 ;;
	esac
	printf '%s\n' "$size"
}

start_daemon()
{
	"$DAEMON" --shm "$SHM" --store "$STORE" >>"$TMPROOT/daemon.log" 2>&1 &
	DPID=$!
	for _ in $(seq 1 200); do
		kill -0 "$DPID" 2>/dev/null || return 1
		if "$INSPECT" --shm "$SHM" health >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.05
	done
	return 1
}

stop_daemon()
{
	[ -n "${DPID:-}" ] || return 0
	kill "$DPID" 2>/dev/null || return 1
	wait "$DPID" || return 1
	DPID=
}

# Finish the segment containing the writer's current durable position and wait
# for archive_library to append it to the store.
archive_current_wal()
{
	local switch_lsn target

	switch_lsn=$("${WP[@]}" -c "SELECT pg_switch_wal();") || return 1
	target=$("${WP[@]}" -c "SELECT pg_walfile_name('$switch_lsn'::pg_lsn - 1);") || return 1
	for _ in $(seq 1 300); do
		[ -f "$WRITER/pg_wal/archive_status/$target.done" ] && return 0
		sleep 0.1
	done
	echo "archive did not mark $target done" >&2
	"${WP[@]}" -c "SELECT archived_count, failed_count, last_archived_wal,
		last_failed_wal FROM pg_stat_archiver;" >&2 || true
	return 1
}

wait_materializer_note()
{
	local id=$1 expected=$2 value=

	for _ in $(seq 1 400); do
		value=$("${MP[@]}" -c "SELECT note FROM mvp_golden WHERE id=$id;" \
			2>/dev/null || true)
		[ "$value" = "$expected" ] && return 0
		sleep 0.1
	done
	echo "materializer returned '${value:-}', expected '$expected' for row $id" >&2
	return 1
}

wait_replay_lsn()
{
	local target=$1 reached=

	for _ in $(seq 1 400); do
		reached=$("${MP[@]}" -c \
			"SELECT pg_last_wal_replay_lsn() >= '$target'::pg_lsn;" \
			2>/dev/null || true)
		[ "$reached" = "t" ] && return 0
		sleep 0.1
	done
	echo "materializer did not replay through $target" >&2
	return 1
}

wait_recovery_paused()
{
	local state=

	for _ in $(seq 1 400); do
		state=$("${MP[@]}" -c "SELECT pg_get_wal_replay_pause_state();" \
			2>/dev/null || true)
		[ "$state" = "paused" ] && return 0
		sleep 0.1
	done
	echo "materializer recovery pause state is '${state:-unknown}'" >&2
	return 1
}

wait_branch_promotion()
{
	local recovering=

	for _ in $(seq 1 400); do
		recovering=$("${BP[@]}" -c "SELECT pg_is_in_recovery();" \
			2>/dev/null || true)
		[ "$recovering" = "f" ] && return 0
		sleep 0.1
	done
	echo "branch recovery state is '${recovering:-unknown}'" >&2
	return 1
}

wait_materialized_lsn()
{
	local target=$1 reached=

	for _ in $(seq 1 400); do
		reached=$("${MP[@]}" -c \
			"SELECT pagestore_materialized_wal_lsn() >= '$target'::pg_lsn;" \
			2>/dev/null || true)
		[ "$reached" = "t" ] && return 0
		sleep 0.1
	done
	echo "durable materialized horizon did not reach $target" >&2
	return 1
}

wait_branch_promoted()
{
	local in_recovery=

	for _ in $(seq 1 400); do
		in_recovery=$("${BP[@]}" -c "SELECT pg_is_in_recovery();" \
			2>/dev/null || true)
		[ "$in_recovery" = "f" ] && return 0
		sleep 0.1
	done
	echo "branch recovery state is '${in_recovery:-unknown}'" >&2
	return 1
}

mkdir -p "$STORE" "$PREPARED"
"$BIN/initdb" -D "$WRITER" -U postgres -A trust >/dev/null 2>&1 ||
	fail "writer initdb failed"
start_daemon || fail "pagestore daemon did not become ready"
"$IMPORT" --shm "$SHM" --pgdata "$WRITER" >"$TMPROOT/import.log" 2>&1 ||
	fail "could not import the base cluster"

cat >> "$WRITER/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_all = off
pagestore.timeline = 0
io_method = sync
recovery_prefetch = try
archive_mode = on
archive_library = 'pagestore'
listen_addresses = '127.0.0.1'
port = $WPORT
EOF

"$BIN/pg_ctl" -D "$WRITER" -l "$WRITER/writer.log" -w start >/dev/null 2>&1 ||
	fail "WAL-only writer did not start"
"${WP[@]}" -c "SELECT 1;" >/dev/null || fail "writer is not accepting connections"

# The worker gets only a physical base.  All later relation contents must come
# from archive recovery and must be written into pagestore by route_all.
"$BIN/pg_basebackup" -h 127.0.0.1 -p "$WPORT" -U postgres \
	-D "$MATERIALIZER" --wal-method=none --checkpoint=fast >/dev/null 2>&1 ||
	fail "could not create the materializer base backup"
materializer_wal_segment_size=$(wal_segment_size "$MATERIALIZER") ||
	fail "could not read materializer WAL segment size"
archive_current_wal || fail "base-backup WAL did not reach pagestore"

cat >> "$MATERIALIZER/postgresql.conf" <<EOF
pagestore.route_all = on
pagestore.materializer = on
archive_mode = off
listen_addresses = '127.0.0.1'
port = $MPORT
hot_standby = on
restore_command = '$WALRESTORE --shm $SHM --timeline 0 --segsize $materializer_wal_segment_size %f %p'
EOF
touch "$MATERIALIZER/standby.signal"
find "$MATERIALIZER/pg_wal" -maxdepth 1 -type f -name '0000000*' -delete

"$BIN/pg_ctl" -D "$MATERIALIZER" -l "$MATERIALIZER/materializer.log" \
	-w start >/dev/null 2>&1 || fail "continuous materializer did not start"
assert_eq "$("${MP[@]}" -c "SELECT pg_is_in_recovery();")" "t" \
	"materializer is a distinct recovery compute"

"${WP[@]}" -c "CREATE EXTENSION pagestore;
	CREATE FUNCTION pagestore_read_at(regclass, int, int, pg_lsn) RETURNS bytea
	 AS 'pagestore','pagestore_read_at' LANGUAGE C STRICT;
	CREATE TABLE mvp_golden(id int primary key, note text);" >/dev/null ||
	fail "could not create the golden test relation"
ddl_checkpoint_lsn=$("${WP[@]}" -c "CHECKPOINT;
	SELECT pg_current_wal_lsn();" | tail -1) ||
	fail "could not checkpoint the committed test DDL"
relfile=$("${WP[@]}" -c "SELECT pg_relation_filepath('mvp_golden');")
[ -f "$WRITER/$relfile" ] || fail "WAL-only writer did not keep its heap local"
echo "ok   - writer keeps relation pages local and ships WAL only"
ddl_xid=$("${WP[@]}" -c "SELECT xmin::text FROM pg_attribute
	WHERE attrelid='mvp_golden'::regclass AND attname='note';") ||
	fail "could not capture the relation-creation xid"
attblock=$("${WP[@]}" -c "SELECT split_part(trim(both '()' from ctid::text), ',', 1)
	FROM pg_attribute WHERE attrelid='mvp_golden'::regclass AND attname='note';") ||
	fail "could not locate the relation's pg_attribute page"

# The installed controller owns both process lifecycles across the correctness
# window.  First retain the direct API's fail-closed assertion, then let the
# controller select C, stop the public writer, establish exact R/E on a private
# socket, archive and materialize through E, pause at L, and capture maps plus
# the prepared branch while no horizon-changing client can interleave.
archive_current_wal || fail "DDL checkpoint WAL did not reach pagestore"
wait_replay_lsn "$ddl_checkpoint_lsn" ||
	fail "materializer did not reach the DDL checkpoint"
if "${MP[@]}" -c "SELECT pagestore_capture_slru_snapshot();" \
	>/dev/null 2>&1; then
	fail "SLRU capture accepted an unpaused recovery worker"
fi
echo "ok   - SLRU capture fails closed before recovery is paused"

"${WP[@]}" -c "INSERT INTO mvp_golden VALUES (1, 'before_fork');" >/dev/null ||
	fail "could not commit the fork-visible row"

cat > "$BRANCH_CONFIG" <<EOF
{
  "schema": 1,
  "pg_ctl": "$BIN/pg_ctl",
  "psql": "$BIN/psql",
  "writer_data_dir": "$WRITER",
  "writer_host": "127.0.0.1",
  "writer_port": $WPORT,
  "writer_log_file": "$WRITER/writer.log",
  "private_socket_dir": "$PRIVATE_SOCKET",
  "private_port": $PRIVATE_PORT,
  "materializer_data_dir": "$MATERIALIZER",
  "materializer_host": "127.0.0.1",
  "materializer_port": $MPORT,
  "prepared_dir": "$PREPARED",
  "new_timeline": 1,
  "parent_timeline": 0,
  "database": "postgres",
  "user": "postgres",
  "poll_interval_ms": 50,
  "progress_timeout_ms": 40000,
  "command_timeout_seconds": 60
}
EOF
branch_receipt=$("$BRANCHPREP" --config "$BRANCH_CONFIG") ||
	fail "serialized branch preparation failed"
IFS='|' read -r receipt_state base_lsn checkpoint_redo checkpoint_lsn \
	fork_lsn seeded <<EOF
$(python3 -c 'import json, sys
r = json.loads(sys.argv[1])
print("|".join(str(r[k]) for k in (
    "state", "base_lsn", "checkpoint_redo_lsn", "checkpoint_end_lsn",
    "fork_lsn", "seeded_slru_pages")))' "$branch_receipt")
EOF
assert_eq "$receipt_state" "complete" \
	"branch controller restored both services after prepare"
[ "${seeded:-0}" -gt 0 ] || fail "branch preparation seeded no SLRU pages"
echo "ok   - serialized branch window selected C=$base_lsn R=$checkpoint_redo E=$checkpoint_lsn L=$fork_lsn"

wait_materializer_note 1 before_fork ||
	fail "materializer did not produce the fork-visible page"

# The restartpoint flushes dirty routed pages before publishing the marker; the
# subsequent read runs with an empty buffer cache and is therefore store-backed.
"$BIN/pg_ctl" -D "$MATERIALIZER" -m fast -w restart >/dev/null 2>&1 ||
	fail "materializer restartpoint/restart failed"
wait_materialized_lsn "$checkpoint_lsn" ||
	fail "workload checkpoint is not covered by durable materialization"
wait_materializer_note 1 before_fork ||
	fail "fork-visible page was not store-visible after materializer restart"
materialized_lsn=$("${MP[@]}" -c "SELECT pagestore_materialized_wal_lsn();")
assert_eq "$materialized_lsn" "$fork_lsn" \
	"controller receipt matches the durable materialized fork"
echo "ok   - durable materialized fork $fork_lsn covers workload checkpoint $checkpoint_lsn"
assert_eq "$("${WP[@]}" -c "SELECT position('note'::bytea in
	pagestore_read_at('pg_attribute', 0, '$attblock', '$fork_lsn')) > 0;")" \
	"t" "materialized catalog page is visible at the fork LSN"

echo "ok   - branch prepared at the materialized fork ($seeded SLRU page(s))"

# Both attached processes must be down while the daemon reinitializes its shm.
"$BIN/pg_ctl" -D "$MATERIALIZER" -m fast -w stop >/dev/null 2>&1 ||
	fail "could not stop the materializer for store restart"
"$BIN/pg_ctl" -D "$WRITER" -m fast -w stop >/dev/null 2>&1 ||
	fail "could not stop the writer at the fork"
stop_daemon || fail "pagestore daemon did not stop cleanly"
start_daemon || fail "pagestore daemon did not recover its durable store"
"$BIN/pg_ctl" -D "$WRITER" -l "$WRITER/writer.log" -w start >/dev/null 2>&1 ||
	fail "writer did not recover after store restart"
"$BIN/pg_ctl" -D "$MATERIALIZER" -l "$MATERIALIZER/materializer.log" \
	-w start >/dev/null 2>&1 || fail "materializer did not recover after store restart"
assert_eq "$("${MP[@]}" -c "SELECT pg_is_in_recovery();")" "t" \
	"store, writer, and materializer restart without losing the prepared branch"

# Advance and materialize the parent after timeline 1 already exists.  The
# child must still fall back to timeline 0 only at or before fork_lsn.
"${WP[@]}" -c "INSERT INTO mvp_golden VALUES (2, 'after_fork');" >/dev/null ||
	fail "could not advance the parent"
"${WP[@]}" -c "CHECKPOINT;" >/dev/null || fail "could not checkpoint the parent"
archive_current_wal || fail "post-fork parent WAL did not reach pagestore"
wait_materializer_note 2 after_fork || fail "post-fork parent page was not materialized"
"$BIN/pg_ctl" -D "$MATERIALIZER" -m fast -w restart >/dev/null 2>&1 ||
	fail "post-fork materializer restart failed"
wait_materializer_note 2 after_fork ||
	fail "post-fork parent page was not store-visible after restart"
echo "ok   - parent advanced and was durably materialized beyond the child fork"

# Portable boot path from a fresh same-build cluster skeleton.  Relation files
# belong to pagestore; the CRC-bound bootstrap artifact installs the source
# cluster's complete default-tablespace relation-map topology and SLRUs after
# exact checkpoint control is restored.  Archive recovery consumes the real
# checkpoint record and promotes at its record end; the store branch itself
# remains cut at the later durable materialized fork.
"$BIN/initdb" -D "$BRANCH" -U postgres -A trust >/dev/null 2>&1 ||
	fail "branch bootstrap initdb failed"
"$CONTROLRESTORE" --shm "$SHM" --timeline 1 --lsn "$checkpoint_redo" \
	--archive-bootstrap \
	"$BRANCH" >/dev/null || fail "could not restore branch checkpoint control"
branch_wal_segment_size=$(wal_segment_size "$BRANCH") ||
	fail "could not read branch WAL segment size"
"${WP[@]}" -c "SELECT pagestore_install_prepared_branch_bootstrap(
	'$PREPARED', '$BRANCH', 1, 0, '$checkpoint_redo', '$checkpoint_lsn',
	'$fork_lsn');" >/dev/null || fail "could not install the portable branch bootstrap"
# Remove the unrelated WAL segment created by the fresh initdb.  The restored
# cluster identity must fetch its checkpoint and all subsequent WAL from store.
find "$BRANCH/pg_wal" -maxdepth 1 -type f -name '0000000*' -delete
"$BIN/initdb" -D "$BRANCH_SCRATCH" -U postgres -A trust >/dev/null 2>&1 ||
	fail "branch WAL-redo scratch initdb failed"
cat >> "$BRANCH/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_all = on
pagestore.timeline = 1
pagestore.walredo_datadir = '$BRANCH_SCRATCH'
io_method = sync
archive_mode = off
listen_addresses = '127.0.0.1'
port = $BPORT
restore_command = '$WALRESTORE --shm $SHM --timeline 1 --segsize $branch_wal_segment_size %f %p'
recovery_target_lsn = '$checkpoint_lsn'
recovery_target_inclusive = on
recovery_target_action = 'promote'
EOF
touch "$BRANCH/recovery.signal"

"$BIN/pg_ctl" -D "$BRANCH" -l "$BRANCH/branch.log" -w start >/dev/null 2>&1 ||
	fail "independent branch compute did not boot"
wait_branch_promotion || fail "portable branch recovery did not promote"
assert_eq "$("${BP[@]}" -c "SELECT pg_is_in_recovery();")" "f" \
	"portable branch recovery promoted from the prepared checkpoint"
assert_eq "$("${BP[@]}" -c "SELECT current_setting('pagestore.route_all');")" "on" \
	"branch compute uses page-store routing, not its copied local heap"
assert_eq "$("${BP[@]}" -c "SELECT pg_xact_status('$ddl_xid'::text::xid8);")" \
	"committed" "branch SLRU marks the relation-creation transaction committed"
assert_eq "$("${BP[@]}" -c "SELECT position('note'::bytea in
	pagestore_read_at('pg_attribute', 0, '$attblock', '$fork_lsn')) > 0;")" \
	"t" "branch can read the materialized pg_attribute page at its fork"
assert_eq "$("${BP[@]}" -c "SELECT note FROM mvp_golden WHERE id=1;")" \
	"before_fork" "branch sees the materialized pre-fork row"
assert_eq "$("${BP[@]}" -c "SELECT count(*) FROM mvp_golden WHERE id=2;")" \
	"0" "branch excludes the materialized post-fork parent row"
assert_eq "$("${BP[@]}" -c "SELECT pagestore_shipped_wal_lsn();")" \
	"$fork_lsn" "branch inherits the durable WAL boundary at its fork"

"${BP[@]}" -c "INSERT INTO mvp_golden VALUES (3, 'branch_local'); CHECKPOINT;" \
	>/dev/null || fail "branch could not write on its own timeline"
"$BIN/pg_ctl" -D "$BRANCH" -m fast -w restart >/dev/null 2>&1 ||
	fail "branch compute restart failed"
assert_eq "$("${BP[@]}" -c "SELECT note FROM mvp_golden WHERE id=3;")" \
	"branch_local" "branch write survives compute restart on timeline 1"
assert_eq "$("${BP[@]}" -c "SELECT count(*) FROM mvp_golden WHERE id=2;")" \
	"0" "branch ancestry cutoff survives compute restart"
assert_eq "$("${WP[@]}" -c "SELECT count(*) FROM mvp_golden WHERE id=3;")" \
	"0" "parent remains isolated from the branch write"

echo "----"
echo "pagestore MVP golden scenario: PASS"
