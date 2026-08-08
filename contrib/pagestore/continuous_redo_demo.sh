#!/usr/bin/env bash
#
# continuous_redo_demo.sh -- keep a WAL-only writer and redo materializer
# online together, and prove that the materializer follows newly archived WAL.
#
# The writer keeps relation pages local and ships only WAL.  A distinct
# PostgreSQL instance starts from a physical base backup, stays in archive
# recovery with route_all enabled, and materializes replayed relation pages into
# pagestore.  Two updates are archived after the materializer is already live;
# observing both on the recovery instance proves this is continuous following,
# not the one-shot writer-to-recovery handoff covered by wal_only_redo_demo.sh.
#
# Self-asserting; needs a full PostgreSQL build.  Pass the meson build dir as $1.
#
set -uo pipefail

BUILD=${1:?usage: continuous_redo_demo.sh <meson-build-dir>}
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

TMPROOT=$(mktemp -d)
WRITER="$TMPROOT/writer"
REDO="$TMPROOT/redo"
STORE="$TMPROOT/store"
SHM=/pscontinuous_$$
WPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
RPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
WP="$BIN/psql -h 127.0.0.1 -p $WPORT -U postgres -tA"
RP="$BIN/psql -h 127.0.0.1 -p $RPORT -U postgres -tA"

cleanup()
{
	"$BIN/pg_ctl" -D "$REDO" -m immediate -w stop >/dev/null 2>&1 || true
	"$BIN/pg_ctl" -D "$WRITER" -m immediate -w stop >/dev/null 2>&1 || true
	[ -n "${DPID:-}" ] && kill "$DPID" 2>/dev/null || true
	rm -rf "$TMPROOT"
	rm -f "/dev/shm$SHM"
}
trap cleanup EXIT

fail()
{
	echo "FAIL - $1"
	echo "writer log:"
	tail -20 "$WRITER/writer.log" 2>/dev/null || true
	echo "redo log:"
	tail -40 "$REDO/redo.log" 2>/dev/null || true
	echo "daemon/import log:"
	tail -40 "$TMPROOT/daemon.log" "$TMPROOT/import.log" 2>/dev/null || true
	exit 1
}

# Finish the WAL segment containing the writer's current durable position and
# wait until archive_library has appended it to pagestore.
archive_current_wal()
{
	local switch_lsn target

	switch_lsn=$($WP -c "SELECT pg_switch_wal();") || return 1
	target=$($WP -c "SELECT pg_walfile_name('$switch_lsn'::pg_lsn - 1);") || return 1
	for _ in $(seq 1 300); do
		[ -f "$WRITER/pg_wal/archive_status/$target.done" ] && return 0
		sleep 0.1
	done
	echo "archive did not mark $target done" >&2
	$WP -c "SELECT archived_count, failed_count, last_archived_wal,
		last_failed_wal FROM pg_stat_archiver;" >&2 || true
	find "$WRITER/pg_wal/archive_status" -maxdepth 1 -type f -print >&2
	return 1
}

wait_redo_value()
{
	local expected=$1 value=

	for _ in $(seq 1 400); do
		value=$($RP -c "SELECT v FROM continuous_redo WHERE id=1;" 2>/dev/null || true)
		[ "$value" = "$expected" ] && return 0
		sleep 0.1
	done
	echo "redo returned '${value:-}', expected '$expected'" >&2
	return 1
}

wait_replay_lsn()
{
	local target=$1 reached=

	for _ in $(seq 1 400); do
		reached=$($RP -c "SELECT pg_last_wal_replay_lsn() >= '$target'::pg_lsn;" \
			2>/dev/null || true)
		[ "$reached" = "t" ] && return 0
		sleep 0.1
	done
	echo "redo did not replay checkpoint boundary $target" >&2
	return 1
}

wait_materializer_caught_up()
{
	local lag=

	for _ in $(seq 1 400); do
		lag=$($RP -c "SELECT pagestore_materializer_lag_bytes();" 2>/dev/null || true)
		[ "$lag" = "0" ] && return 0
		sleep 0.1
	done
	echo "materializer lag remained '${lag:-unknown}' bytes" >&2
	return 1
}

mkdir -p "$STORE"
"$BIN/initdb" -D "$WRITER" -U postgres -A trust >/dev/null 2>&1 ||
	fail "initdb failed"
"$DAEMON" --shm "$SHM" --store "$STORE" >"$TMPROOT/daemon.log" 2>&1 &
DPID=$!
daemon_ready=0
for _ in $(seq 1 200); do
	kill -0 "$DPID" 2>/dev/null || fail "pagestore daemon exited during startup"
	if "$INSPECT" --shm "$SHM" health >/dev/null 2>&1; then
		daemon_ready=1
		break
	fi
	sleep 0.05
done
[ "$daemon_ready" -eq 1 ] || fail "pagestore daemon did not become ready"
"$IMPORT" --shm "$SHM" --pgdata "$WRITER" >"$TMPROOT/import.log" 2>&1 ||
	fail "could not import the base cluster"

cat >> "$WRITER/postgresql.conf" <<EOF
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '$SHM'
pagestore.route_all = off
io_method = sync
recovery_prefetch = off
archive_mode = on
archive_library = 'pagestore'
listen_addresses = '127.0.0.1'
port = $WPORT
EOF

"$BIN/pg_ctl" -D "$WRITER" -l "$WRITER/writer.log" -w start >/dev/null 2>&1 ||
	fail "WAL-only writer did not start"
$WP -c "SELECT 1;" >/dev/null 2>&1 || fail "WAL-only writer is not accepting connections"

# Take the redo worker's physical base while the writer remains online.  WAL is
# deliberately excluded: recovery must fetch it through pagestore_walrestore.
"$BIN/pg_basebackup" -h 127.0.0.1 -p "$WPORT" -U postgres -D "$REDO" \
	--wal-method=none --checkpoint=fast >/dev/null 2>&1 ||
	fail "could not create the redo worker base backup"
archive_current_wal || fail "base-backup WAL did not reach pagestore"

cat >> "$REDO/postgresql.conf" <<EOF
pagestore.route_all = on
archive_mode = off
listen_addresses = '127.0.0.1'
port = $RPORT
hot_standby = on
restore_command = '$WALRESTORE --shm $SHM --timeline 0 --segsize 16777216 %f %p'
EOF
# Standby mode keeps retrying restore_command when it reaches the current end of
# the archive; recovery.signal alone would promote as soon as WAL is exhausted.
touch "$REDO/standby.signal"
find "$REDO/pg_wal" -maxdepth 1 -type f -name '0000000*' -delete

"$BIN/pg_ctl" -D "$REDO" -l "$REDO/redo.log" -w start >/dev/null 2>&1 ||
	fail "continuous redo worker did not start"
$RP -c "SELECT pg_is_in_recovery();" 2>/dev/null | grep -qx t ||
	fail "redo worker left archive recovery"

# Both changes happen after the redo worker is live.  Completing each segment
# lets archive recovery consume it without stopping either process.
$WP -c "CREATE EXTENSION pagestore;
	CREATE TABLE continuous_redo(id int primary key, v text);
	INSERT INTO continuous_redo VALUES (1, 'first');" >/dev/null ||
	fail "could not create the writer test relation"
if $WP -c "SELECT pagestore_materializer_lag_bytes();" \
		>"$TMPROOT/writer-lag.out" 2>"$TMPROOT/writer-lag.err"; then
	fail "materializer lag API accepted a non-recovery writer"
fi
grep -q "materializer monitoring requires recovery mode" "$TMPROOT/writer-lag.err" ||
	fail "writer lag rejection did not explain the recovery requirement"
echo "ok   - materializer lag API rejects a non-recovery writer"
relfile=$($WP -c "SELECT pg_relation_filepath('continuous_redo');")
[ -f "$WRITER/$relfile" ] || fail "writer did not keep the relation page local"
archive_current_wal || fail "first update WAL did not reach pagestore"
wait_redo_value first || fail "redo worker did not materialize the first value"
echo "ok   - live redo worker materialized the first archived change"

checkpoint_lsn=$($WP -c "UPDATE continuous_redo SET v='second' WHERE id=1;
	CHECKPOINT;
	SELECT pg_current_wal_lsn();" | tail -1) ||
	fail "could not update the writer test relation"
archive_current_wal || fail "second update WAL did not reach pagestore"
wait_redo_value second || fail "redo worker did not follow the second value"
echo "ok   - redo worker continuously followed later archived WAL"
wait_replay_lsn "$checkpoint_lsn" ||
	fail "redo worker did not replay the newer checkpoint"

# A replay LSN alone is not a materialization boundary: the changed page may
# still live only in this standby's dirty buffer cache.  Shutdown recovery
# performs a restartpoint, whose post-flush hook publishes the durable store
# watermark.  Restarting also clears the cache, so the next read proves the
# page itself is store-visible.
"$BIN/pg_ctl" -D "$REDO" -m fast -w restart >/dev/null 2>&1 ||
	fail "redo worker restartpoint/restart failed"
$RP -c "SELECT pg_is_in_recovery();" 2>/dev/null | grep -qx t ||
	fail "redo worker left recovery after restartpoint"
wait_redo_value second || fail "second value was not store-visible after restart"
wait_materializer_caught_up || fail "materializer lag did not return to zero"
shipped_lsn=$($RP -c "SELECT pagestore_shipped_wal_lsn();")
materialized_lsn=$($RP -c "SELECT pagestore_materialized_wal_lsn();")
echo "ok   - materializer reports zero lag (shipped $shipped_lsn, materialized $materialized_lsn)"

[ ! -f "$REDO/$relfile" ] ||
	fail "redo worker created a local heap instead of materializing into pagestore"
echo "ok   - recovery relation has no local heap file (pagestore is authoritative)"
echo "continuous redo demo: PASS"
