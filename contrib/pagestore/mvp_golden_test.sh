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
BRANCH_FAULT_CONTROL="$TMPROOT/branch-prepare-fault-control"
SHM=/psmvpgolden_$$
WPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
MPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
BPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
PRIVATE_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
WP=("$BIN/psql" -h 127.0.0.1 -p "$WPORT" -U postgres -tA -v ON_ERROR_STOP=1)
MP=("$BIN/psql" -h 127.0.0.1 -p "$MPORT" -U postgres -tA -v ON_ERROR_STOP=1)
BP=("$BIN/psql" -h 127.0.0.1 -p "$BPORT" -U postgres -tA -v ON_ERROR_STOP=1)
RWP=("$BIN/psql" -h "$PRIVATE_SOCKET" -p "$PRIVATE_PORT" -U postgres -tA -v ON_ERROR_STOP=1)

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
			"SELECT pagestore_ext.pagestore_materialized_wal_lsn() >= '$target'::pg_lsn;" \
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
track_commit_timestamp = on
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
pagestore.retention_owner_id = '1'
pagestore.retention_owner_generation = '1'
archive_mode = off
listen_addresses = '127.0.0.1'
port = $MPORT
hot_standby = on
track_commit_timestamp = on
restore_command = '$WALRESTORE --shm $SHM --timeline 0 --incarnation 1 --segsize $materializer_wal_segment_size %f %p'
EOF
touch "$MATERIALIZER/standby.signal"
find "$MATERIALIZER/pg_wal" -maxdepth 1 -type f -name '0000000*' -delete ||
	fail "could not remove copied materializer WAL"

"$BIN/pg_ctl" -D "$MATERIALIZER" -l "$MATERIALIZER/materializer.log" \
	-w start >/dev/null 2>&1 || fail "continuous materializer did not start"
assert_eq "$("${MP[@]}" -c "SELECT pg_is_in_recovery();")" "t" \
	"materializer is a distinct recovery compute"

"${WP[@]}" -c "CREATE SCHEMA pagestore_ext;
	CREATE EXTENSION pagestore WITH SCHEMA pagestore_ext VERSION '1.0';
	ALTER EXTENSION pagestore UPDATE TO '1.1';
	ALTER EXTENSION pagestore UPDATE TO '1.2';" >/dev/null ||
fail "could not install the extension upgrade chain"
assert_eq "$("${WP[@]}" -c "SELECT extversion FROM pg_extension WHERE extname = 'pagestore';")" "1.2" \
	"extension upgrades from 1.0 through 1.1 to 1.2"
api_count=$("${WP[@]}" -c "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pagestore_ext' AND p.oid IN ('pagestore_ext.pagestore_create_branch_with_incarnation(integer,integer,bigint,pg_lsn)'::regprocedure, 'pagestore_ext.pagestore_prepare_branch_from_control(text,integer,integer,pg_lsn,pg_lsn,pg_lsn,bigint)'::regprocedure, 'pagestore_ext.pagestore_retention_drop_with_incarnation(integer,integer,bigint,bigint,bigint)'::regprocedure);")
assert_eq "$api_count" "3" "1.2 exposes immutable branch and retention control APIs after upgrade"
"${WP[@]}" -c "DROP EXTENSION pagestore; CREATE EXTENSION pagestore WITH SCHEMA pagestore_ext VERSION '1.2';" >/dev/null ||
	fail "could not install a fresh 1.2 extension"
assert_eq "$("${WP[@]}" -c "SELECT extversion FROM pg_extension WHERE extname = 'pagestore';")" "1.2" \
	"fresh extension install uses version 1.2"
"${WP[@]}" -c "CREATE FUNCTION pagestore_read_at(regclass, int, int, pg_lsn) RETURNS bytea
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
if "${MP[@]}" -c "SELECT pagestore_ext.pagestore_capture_slru_snapshot();" \
	>/dev/null 2>&1; then
	fail "SLRU capture accepted an unpaused recovery worker"
fi
echo "ok   - SLRU capture fails closed before recovery is paused"

"${WP[@]}" -c "INSERT INTO mvp_golden VALUES (1, 'before_fork');" >/dev/null ||
	fail "could not commit the fork-visible row"
# Give every SLRU the seeders reconstruct real content before the fork, so the
# seed-versus-recovery comparison covers pg_xact, pg_commit_ts (the writer
# tracks commit timestamps) and both pg_multixact halves: two sessions holding
# key-share locks on the same row at once create a multixact with two members.
"${WP[@]}" -c "BEGIN; SELECT id FROM mvp_golden WHERE id = 1 FOR KEY SHARE; SELECT pg_sleep(30); COMMIT;" \
	>/dev/null 2>&1 &
golden_locker=$!
# wait until the first session observably holds its lock and is sleeping,
# rather than trusting elapsed time on a loaded host
for _ in $(seq 1 100); do
	if [ "$("${WP[@]}" -c "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%pg_sleep(30)%' AND wait_event = 'PgSleep';")" = "1" ]; then
		break
	fi
	sleep 0.1
done
assert_eq "$("${WP[@]}" -c "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'transactionid' AND a.query LIKE '%pg_sleep(30)%';")" "1" \
	"the first key-share holder is in its transaction"
"${WP[@]}" -c "SELECT id FROM mvp_golden WHERE id = 1 FOR KEY SHARE;" >/dev/null ||
	fail "could not take the second key-share lock"
"${WP[@]}" -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%pg_sleep(30)%' AND wait_event = 'PgSleep';" >/dev/null
wait "$golden_locker" 2>/dev/null
# the multixact exists once both lockers overlapped; cancelling the sleep
# aborted the first transaction, which is fine -- the multixact was created
# when the second locker joined, and it is what the SLRUs carry
assert_eq "$("${WP[@]}" -c "SELECT count(*) FROM pg_get_multixact_members((SELECT xmax FROM mvp_golden WHERE id = 1));" 2>/dev/null)" "2" \
	"the overlapping lockers created a two-member multixact on the row"
assert_eq "$("${WP[@]}" -c "SELECT (SELECT count(*) FROM mvp_golden) = 1 AND pg_xact_commit_timestamp((SELECT xmin FROM mvp_golden WHERE id = 1)) IS NOT NULL;")" "t" \
	"multixact and commit-timestamp workload committed before the fork"

mkdir -m 700 "$TMPROOT/controller-authority" ||
	fail "could not create materializer authority directory"
materializer_dev=$(stat -c %d "$MATERIALIZER") ||
	fail "could not inspect materializer device identity"
materializer_ino=$(stat -c %i "$MATERIALIZER") ||
	fail "could not inspect materializer inode identity"
authority_dev=$(stat -c %d "$TMPROOT/controller-authority") ||
	fail "could not inspect authority namespace device identity"
authority_ino=$(stat -c %i "$TMPROOT/controller-authority") ||
	fail "could not inspect authority namespace inode identity"
cat > "$TMPROOT/controller-authority/retention-owner-1.json" <<EOF
{"retention_generation":1,"consumer_data_dir":"$MATERIALIZER","consumer_instance_id":"mvp-golden","consumer_data_dev":$materializer_dev,"consumer_data_ino":$materializer_ino,"authority_namespace_dev":$authority_dev,"authority_namespace_ino":$authority_ino}
EOF

cat > "$BRANCH_CONFIG" <<EOF
{
  "schema": 2,
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
  "retention_authority_dir": "$TMPROOT/controller-authority",
  "retention_owner_id": 1,
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
# Crash the installed controller after it has published the prepared receipt.
# The real writer, materializer, retention registry, and branch-preparation SQL
# are active here; only the process-abort point is injected by the canonical
# fault-control protocol.
mkdir -m 700 "$BRANCH_FAULT_CONTROL" ||
	fail "could not create branch fault control directory"
printf 'arm\n' > "$BRANCH_FAULT_CONTROL/arm" ||
	fail "could not arm branch fault control"
env \
	-u PAGESTORE_TEST_FAULT_OPERATION_ID \
	-u PAGESTORE_TEST_FAULT_WATCHDOG_MS \
	PAGESTORE_TEST_FAULT_NAME=branch_prepare.after_prepared_receipt \
	PAGESTORE_TEST_FAULT_ACTION=crash \
	PAGESTORE_TEST_FAULT_HIT=1 \
	PAGESTORE_TEST_FAULT_DIR="$BRANCH_FAULT_CONTROL" \
	PAGESTORE_TEST_FAULT_SCENARIO=mvp-golden \
	PAGESTORE_TEST_FAULT_SEED=1 \
	PAGESTORE_TEST_FAULT_OPERATION=branch-prepare-crash \
	"$BRANCHPREP" --config "$BRANCH_CONFIG" --verify-seed-against-materializer \
	> "$TMPROOT/branch-crash.stdout" 2> "$TMPROOT/branch-crash.stderr"
branch_crash_status=$?
assert_eq "$branch_crash_status" "88" \
	"installed branch controller aborts with the canonical crash status"
# The seeders' replay of (C, L] onto the base snapshot is compared, page by
# page, with the SLRUs PostgreSQL recovery itself produced through L in the
# paused materializer -- the independent oracle for appliers that mirror
# clog_redo, CommitTsRedo and multixact_redo rather than calling them.
seed_compare=$(grep -o "seeded SLRU pages compared with recovery's at .*" "$TMPROOT/branch-crash.stderr" | tail -1)
[ -n "$seed_compare" ] || fail "branch preparation did not compare its seeded SLRUs with recovery's"
seed_compared=$(printf '%s\n' "$seed_compare" | sed -n 's/.*: \([0-9]*\) reconstructed pages equal.*/\1/p')
[ "${seed_compared:-0}" -gt 0 ] ||
	fail "no reconstructed SLRU page was compared with recovery's: $seed_compare"
# every class the seeders reconstruct must have been compared, not just one
for slru in pg_xact pg_commit_ts pg_multixact/offsets pg_multixact/members; do
	n=$(printf '%s\n' "$seed_compare" | sed -n "s|.*$slru \([0-9]*\).*|\1|p")
	[ "${n:-0}" -gt 0 ] || fail "no reconstructed $slru page was compared with recovery's: $seed_compare"
done
echo "ok   - every reconstructed SLRU page equals the page PostgreSQL recovery produced ($seed_compared pages across pg_xact, pg_commit_ts and both pg_multixact halves)"
python3 - "$BRANCH_FAULT_CONTROL/report.jsonl" <<'PY' || fail "branch crash report is not authentic"
import json
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert report["schema"] == 1
assert report["name"] == "branch_prepare.after_prepared_receipt"
assert report["action"] == "crash"
assert report["hit"] == 1
assert report["scenario"] == "mvp-golden"
assert report["operation"] == "branch-prepare-crash"
assert isinstance(report["pid"], int) and report["pid"] > 0
PY
echo "ok   - installed controller published an authenticated crash report"
python3 - "$PREPARED/pagestore_branch.prepare.json" <<'PY' || fail "crashed branch controller did not leave a prepared journal"
import json
import sys
from pathlib import Path

journal = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert journal["state"] == "prepared"
assert journal["retention_owned"] is True
assert journal["retention_set_attempted"] is True
assert journal["pause_owned"] is True
assert journal["materializer_resumed"] is False
assert journal["writer_owned"] is True
assert journal["restricted_writer_running"] is True
assert journal["writer_restored"] is False
PY
retention_generation=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["retention_generation"])' \
	"$PREPARED/pagestore_branch.prepare.json") ||
	fail "could not read the crashed retention generation"
assert_eq "$("${MP[@]}" -c "SELECT pg_get_wal_replay_pause_state();")" "paused" \
	"crashed controller leaves the materializer paused and fenced"
if "${WP[@]}" -c "SELECT 1;" >/dev/null 2>&1; then
	fail "crashed controller left the public writer reachable"
fi
assert_eq "$("${RWP[@]}" -c "SELECT current_setting('listen_addresses') = ''; ")" "t" \
	"crashed controller leaves only the restricted writer reachable"
assert_eq "$("${MP[@]}" -c "SELECT pagestore_ext.pagestore_retention_owner_lsn(0, 3, 1, $retention_generation) IS NOT NULL;")" "t" \
	"crashed controller leaves the exact branch retention pin installed"

# Remove the one-shot report control before the recovery process.  The second
# controller has no fault environment and therefore cannot consume stale arm
# or report state.
rm -f "$BRANCH_FAULT_CONTROL/arm" \
	"$BRANCH_FAULT_CONTROL/report.jsonl" \
	"$BRANCH_FAULT_CONTROL/report.tmp" ||
	fail "could not clear branch fault control"
rmdir "$BRANCH_FAULT_CONTROL" || fail "branch fault control remained active"

branch_receipt=$(env \
	-u PAGESTORE_TEST_FAULT_NAME \
	-u PAGESTORE_TEST_FAULT_ACTION \
	-u PAGESTORE_TEST_FAULT_HIT \
	-u PAGESTORE_TEST_FAULT_DIR \
	-u PAGESTORE_TEST_FAULT_SCENARIO \
	-u PAGESTORE_TEST_FAULT_SEED \
	-u PAGESTORE_TEST_FAULT_OPERATION \
	-u PAGESTORE_TEST_FAULT_OPERATION_ID \
	-u PAGESTORE_TEST_FAULT_WATCHDOG_MS \
	"$BRANCHPREP" --config "$BRANCH_CONFIG" --verify-seed-against-materializer) ||
	fail "installed branch controller recovery failed"
IFS='|' read -r receipt_state base_lsn checkpoint_redo checkpoint_lsn \
	fork_lsn seeded <<EOF
$(python3 -c 'import json, sys
r = json.loads(sys.argv[1])
print("|".join(str(r[k]) for k in (
    "state", "base_lsn", "checkpoint_redo_lsn", "checkpoint_end_lsn",
    "fork_lsn", "seeded_slru_pages")))' "$branch_receipt")
EOF
assert_eq "$receipt_state" "complete" \
	"second installed controller completed the prepared branch"
[ "${seeded:-0}" -gt 0 ] || fail "branch preparation seeded no SLRU pages"
assert_eq "$("${MP[@]}" -c "SELECT pagestore_ext.pagestore_retention_owner_lsn(0, 3, 1, $retention_generation) IS NULL;")" "t" \
	"recovery controller released the exact temporary retention pin"
assert_eq "$("${MP[@]}" -c "SELECT pg_get_wal_replay_pause_state();")" "not paused" \
	"recovery controller resumed the materializer"
assert_eq "$("${WP[@]}" -c "SELECT NOT pg_is_in_recovery() AND current_setting('listen_addresses') <> ''; ")" "t" \
	"recovery controller restored the normal public writer"
echo "ok   - serialized branch window selected C=$base_lsn R=$checkpoint_redo E=$checkpoint_lsn L=$fork_lsn"
# The persisted-format fixture for the controller's JSON artifacts
# (harness/pagestore_controller_fixture.py --capture) takes what this real
# controller run left behind: its configuration, the completed journal and
# its retention generation authority.
if [ -n "${PAGESTORE_CONTROLLER_FIXTURE_CAPTURE:-}" ]; then
	mkdir -p "$PAGESTORE_CONTROLLER_FIXTURE_CAPTURE/controller" ||
		fail "could not create the controller fixture capture directory"
	cp "$BRANCH_CONFIG" "$PREPARED/pagestore_branch.prepare.json" \
		"$TMPROOT/controller-authority/branch-retention-generation-1.json" \
		"$PAGESTORE_CONTROLLER_FIXTURE_CAPTURE/controller/" ||
		fail "could not capture the controller artifacts"
	echo "ok   - captured the controller artifacts for the fixture"
fi

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
materialized_lsn=$("${MP[@]}" -c "SELECT pagestore_ext.pagestore_materialized_wal_lsn();")
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
"$CONTROLRESTORE" --shm "$SHM" --timeline 1 --incarnation 1 --lsn "$checkpoint_redo" \
	--archive-bootstrap \
	"$BRANCH" >/dev/null || fail "could not restore branch checkpoint control"
branch_wal_segment_size=$(wal_segment_size "$BRANCH") ||
	fail "could not read branch WAL segment size"
# Remove the unrelated WAL segment created by the fresh initdb.  The restored
# cluster identity must fetch its checkpoint and all subsequent WAL from store.
find "$BRANCH/pg_wal" -maxdepth 1 -type f -name '0000000*' -delete ||
	fail "could not remove unrelated branch initdb WAL"
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
track_commit_timestamp = on
restore_command = '$WALRESTORE --shm $SHM --timeline 1 --incarnation 1 --segsize $branch_wal_segment_size %f %p'
recovery_target_lsn = '$checkpoint_lsn'
recovery_target_inclusive = on
recovery_target_action = 'promote'
EOF
touch "$BRANCH/recovery.signal"

# Exercise the portable installer in a real writer backend.  Its process exits
# at the named boundary; the target remains offline until an unarmed retry.
# Each iteration also covers reinstalling an already completed target.
INSTALL_ORACLE="$(dirname "$0")/harness/tests/bootstrap_install_oracle.py"
install_branch_bootstrap()
{
	"${WP[@]}" -c "SELECT pagestore_ext.pagestore_install_prepared_branch_bootstrap(
		'$PREPARED', '$BRANCH', 1, 0, '$checkpoint_redo', '$checkpoint_lsn',
		'$fork_lsn');"
}
for install_phase in after_maps after_slru_remove before_manifest after_manifest; do
	install_control="$TMPROOT/install-$install_phase"
	mkdir -m 700 "$install_control" || fail "could not create install fault control"
	printf 'arm\n' > "$install_control/arm" || fail "could not arm install fault"
	python3 "$INSTALL_ORACLE" snapshot "$PREPARED" "$BRANCH" \
		"$install_control/before.json" || fail "could not snapshot install inputs"
	"$BIN/pg_ctl" -D "$WRITER" -m fast -w stop >/dev/null 2>&1 ||
		fail "could not stop writer before install crash"
	env -u PAGESTORE_TEST_FAULT_OPERATION_ID -u PAGESTORE_TEST_FAULT_WATCHDOG_MS \
		PAGESTORE_TEST_FAULT_NAME="branch_install.$install_phase" \
		PAGESTORE_TEST_FAULT_ACTION=crash PAGESTORE_TEST_FAULT_HIT=1 \
		PAGESTORE_TEST_FAULT_DIR="$install_control" \
		PAGESTORE_TEST_FAULT_SCENARIO=mvp-golden PAGESTORE_TEST_FAULT_SEED=1 \
		PAGESTORE_TEST_FAULT_OPERATION="install-$install_phase" \
		"$BIN/pg_ctl" -D "$WRITER" -l "$WRITER/writer.log" -w start \
		>/dev/null 2>&1 || fail "could not start fault-enabled installer host"
	if install_branch_bootstrap > "$install_control/sql.log" 2>&1; then
		fail "install fault was not reached: $install_phase"
	fi
	install_pid=$(python3 "$INSTALL_ORACLE" report "$install_control/report.jsonl" \
		"branch_install.$install_phase" "install-$install_phase") ||
		fail "install crash report does not match the requested boundary"
	# psql's connection failure is insufficient proof: require the backend's
	# exact PID and the canonical process-abort exit code in the server log.
	for ((attempt=0; attempt<100; attempt++)); do
		if grep -E "(client backend|server process) \(PID $install_pid\) exited with exit code 88" \
			"$WRITER/writer.log" >/dev/null; then
			break
		fi
		sleep 0.1
	done
	[ "$attempt" -lt 100 ] || fail "installer did not exit with fault status 88"
	"$BIN/pg_ctl" -D "$WRITER" -m immediate -w stop >/dev/null 2>&1 ||
		fail "could not stop fault-enabled writer"
	env -u PAGESTORE_TEST_FAULT_NAME -u PAGESTORE_TEST_FAULT_ACTION \
		-u PAGESTORE_TEST_FAULT_HIT -u PAGESTORE_TEST_FAULT_DIR \
		-u PAGESTORE_TEST_FAULT_SCENARIO -u PAGESTORE_TEST_FAULT_SEED \
		-u PAGESTORE_TEST_FAULT_OPERATION -u PAGESTORE_TEST_FAULT_OPERATION_ID \
		-u PAGESTORE_TEST_FAULT_WATCHDOG_MS \
		"$BIN/pg_ctl" -D "$WRITER" -l "$WRITER/writer.log" -w start \
		>/dev/null 2>&1 || fail "could not restart unarmed writer"
	python3 "$INSTALL_ORACLE" unchanged "$PREPARED" "$BRANCH" \
		"$install_control/before.json" || fail "install crash changed its source or control"
	if [ "$install_phase" != after_manifest ]; then
		[ ! -e "$BRANCH/pagestore_branch.manifest" ] ||
			fail "incomplete install published a branch manifest"
		if "$BIN/pg_ctl" -D "$BRANCH" -l "$install_control/startup.log" -w start \
			>/dev/null 2>&1; then
			fail "partially installed branch was admitted"
		fi
		grep -F 'pagestore.timeline requires pagestore_branch.manifest' \
			"$install_control/startup.log" >/dev/null ||
			fail "partial branch failed for a reason other than the manifest fence"
		[ ! -e "$BRANCH/postmaster.pid" ] || fail "failed branch startup remains live"
	else
		python3 "$INSTALL_ORACLE" installed "$PREPARED" "$BRANCH" ||
			fail "post-publication crash left an incomplete installation"
	fi
	install_branch_bootstrap >/dev/null || fail "portable install retry failed"
	python3 "$INSTALL_ORACLE" installed "$PREPARED" "$BRANCH" ||
		fail "retried installation differs from prepared artifacts"
	python3 "$INSTALL_ORACLE" snapshot-installed "$BRANCH" \
		"$install_control/installed.json" || fail "could not snapshot completed install"
	install_branch_bootstrap >/dev/null || fail "repeated portable install failed"
	python3 "$INSTALL_ORACLE" equal-installed "$BRANCH" \
		"$install_control/installed.json" || fail "repeated portable install is not idempotent"
	python3 "$INSTALL_ORACLE" unchanged "$PREPARED" "$BRANCH" \
		"$install_control/before.json" || fail "install retry changed its source or control"
	echo "ok   - portable bootstrap crash/retry: $install_phase"
done

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
assert_eq "$("${BP[@]}" -c "SELECT pagestore_ext.pagestore_shipped_wal_lsn();")" \
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
