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
recovery_prefetch = try
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
pagestore.materializer = on
pagestore.retention_owner_id = '1'
pagestore.retention_owner_generation = '1'
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
# The recovery-start hook is before the first redo record and raises FATAL if
# this durable SET fails.  Reaching hot-standby service above therefore proves
# generation 1 was registered without relying on reserved generation zero.
echo "ok   - materializer retention owner is registered before serving redo"
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
writer_status=$($WP -F '|' -c "SELECT * FROM pagestore_materializer_status();") ||
	fail "writer could not inspect store-observed materializer status"
IFS='|' read -r status_shipped status_materialized status_lag status_release <<EOF
$writer_status
EOF
[ "$status_shipped" = "$shipped_lsn" ] &&
	[ "$status_materialized" = "$materialized_lsn" ] &&
	[ "$status_lag" = 0 ] && [ -z "$status_release" ] ||
	fail "writer materializer status did not report caught-up, unlatch state: $writer_status"
echo "ok   - writer observes caught-up materializer state without claiming the worker role"

# Stop recovery, establish a checkpoint, then advance several segments without
# another checkpoint.  The aligned limiter must finish usable input segments
# but eventually pause.  A later writer checkpoint moves the guaranteed-safe
# archive boundary far enough for recovery to reach a real restartpoint.
$WP -c "ALTER SYSTEM SET pagestore.materializer_max_lag_mb = '1';" >/dev/null ||
	fail "could not configure materializer backpressure"
"$BIN/pg_ctl" -D "$WRITER" -m fast -w stop >/dev/null 2>&1 ||
	fail "writer backpressure stop failed"
"$BIN/pg_ctl" -D "$WRITER" -l "$WRITER/writer.log" -w start >/dev/null 2>&1 ||
	fail "writer backpressure start failed"
"$BIN/pg_ctl" -D "$REDO" -m fast -w stop >/dev/null 2>&1 ||
	fail "could not stop the materializer for backpressure testing"
blocked_target=
$WP -c "CREATE TABLE materializer_backpressure_test(i int);" >/dev/null ||
	fail "could not create the materializer backpressure test relation"
$WP -c "INSERT INTO materializer_backpressure_test VALUES (1); CHECKPOINT;" \
	>/dev/null || fail "could not establish the backlog checkpoint"
$WP -c "SELECT pg_switch_wal();" >/dev/null ||
	fail "could not switch the checkpoint-bearing WAL segment"
for i in 2 3 4 5 6; do
	$WP -c "INSERT INTO materializer_backpressure_test VALUES ($i);" >/dev/null ||
		fail "could not generate materializer backlog segment $i"
	switch_lsn=$($WP -c "SELECT pg_switch_wal();") ||
		fail "could not switch materializer backlog segment $i"
	blocked_target=$($WP -c "SELECT pg_walfile_name('$switch_lsn'::pg_lsn - 1);") ||
		fail "could not identify backlog WAL segment $i"
done
backpressure_seen=0
for _ in $(seq 1 300); do
	if grep -q "archive paused: materializer" "$WRITER/writer.log"; then
		backpressure_seen=1
		break
	fi
	sleep 0.1
done
[ "$backpressure_seen" -eq 1 ] ||
	fail "archiver did not pause for a stalled materializer"
[ -f "$WRITER/pg_wal/archive_status/$blocked_target.ready" ] ||
	fail "materializer backpressure left no WAL segment pending"
writer_status=$($WP -F '|' -c "SELECT * FROM pagestore_materializer_status();") ||
	fail "writer could not inspect stalled materializer status"
IFS='|' read -r status_shipped status_materialized status_lag status_release <<EOF
$writer_status
EOF
[ -n "$status_materialized" ] && [ "${status_lag:-0}" -gt 0 ] &&
	[ -n "$status_release" ] ||
	fail "writer materializer status did not expose stalled release state: $writer_status"
echo "ok   - stalled materializer applies bounded WAL archive backpressure"

# Publish a newer completed checkpoint while archiving is paused.  The limiter
# keeps the earlier release checkpoint latched until the materializer advances,
# then admits complete segments through this next checkpoint.
$WP -c "CHECKPOINT;" >/dev/null ||
	fail "could not publish the backlog release checkpoint"
release_lsn=$($WP -c "SELECT pg_switch_wal();") ||
	fail "could not switch the backlog release checkpoint"
release_target=$($WP -c "SELECT pg_walfile_name('$release_lsn'::pg_lsn - 1);") ||
	fail "could not identify the backlog release segment"
# Wake an archiver that may have entered its longer retry backoff after three
# failures; production would make the same retry without this test nudge.
$WP -c "SELECT pg_reload_conf();" >/dev/null ||
	fail "could not wake the WAL archiver after checkpoint progress"
sleep 2
[ -f "$WRITER/pg_wal/archive_status/$release_target.ready" ] ||
	fail "a later writer checkpoint moved the latched bound before materialization advanced"
"$BIN/pg_ctl" -D "$REDO" -l "$REDO/redo.log" -w start >/dev/null 2>&1 ||
	fail "materializer did not restart after backpressure"
release_replayed=0
for _ in $(seq 1 400); do
	release_value=$($RP -c "SELECT max(i) FROM materializer_backpressure_test;" \
		2>/dev/null || true)
	if [ "${release_value:-0}" -ge 2 ] 2>/dev/null; then
		release_replayed=1
		break
	fi
	sleep 0.1
done
[ "$release_replayed" -eq 1 ] ||
	fail "materializer did not reach the latched release checkpoint"
"$BIN/pg_ctl" -D "$REDO" -m fast -w restart >/dev/null 2>&1 ||
	fail "materializer did not publish progress at the release checkpoint"
$WP -c "SELECT pg_reload_conf();" >/dev/null ||
	fail "could not wake the WAL archiver after materializer progress"
archive_drained=0
for _ in $(seq 1 600); do
	if [ -f "$WRITER/pg_wal/archive_status/$release_target.done" ]; then
		archive_drained=1
		break
	fi
	sleep 0.1
done
[ "$archive_drained" -eq 1 ] ||
	fail "archive backlog did not drain after materializer restart"
backlog_replayed=0
for _ in $(seq 1 400); do
	backlog_value=$($RP -c "SELECT max(i) FROM materializer_backpressure_test;" \
		2>/dev/null || true)
	if [ "$backlog_value" = "6" ]; then
		backlog_replayed=1
		break
	fi
	sleep 0.1
done
[ "$backlog_replayed" -eq 1 ] ||
	fail "materializer did not replay the released archive backlog"
"$BIN/pg_ctl" -D "$REDO" -m fast -w restart >/dev/null 2>&1 ||
	fail "materializer restartpoint after backlog drain failed"
wait_materializer_caught_up ||
	fail "materializer did not catch up after releasing archive backpressure"
echo "ok   - materializer progress releases WAL archive backpressure"

# Recovery alone is not a materializer identity.  Restart with the explicit
# role disabled and prove that a stale marker cannot make this worker pass the
# supervision contract.
"$BIN/pg_ctl" -D "$REDO" -m fast -w stop >/dev/null 2>&1 ||
	fail "could not stop materializer before stale-owner test"
takeover_status=$($WP -c "SELECT pagestore_retention_set(0,2,1,2,6,'$materialized_lsn');")
[ "$takeover_status" = "0" ] || fail "could not publish replacement owner generation"
# Hot-standby startup can briefly make pg_ctl report success before recovery
# reaches the pagestore owner check.  Observe the durable fencing result instead
# of treating that transient readiness as a successful stale takeover.
"$BIN/pg_ctl" -D "$REDO" -l "$REDO/redo.log" -w start >/dev/null 2>&1 || true
stale_fenced=0
for _ in $(seq 1 100); do
	if grep -q "retention generation is stale" "$REDO/redo.log"; then
		stale_fenced=1
		break
	fi
	sleep 0.1
done
[ "$stale_fenced" -eq 1 ] ||
	fail "stale materializer startup did not report owner fencing"
for _ in $(seq 1 100); do
	if ! "$BIN/pg_ctl" -D "$REDO" status >/dev/null 2>&1; then
		break
	fi
	sleep 0.1
done
if "$BIN/pg_ctl" -D "$REDO" status >/dev/null 2>&1; then
	fail "stale materializer generation remained running after controller takeover"
fi
echo "ok   - replacement owner generation fences stale materializer startup"
echo "pagestore.materializer = off" >> "$REDO/postgresql.conf"
"$BIN/pg_ctl" -D "$REDO" -l "$REDO/redo.log" -w start >/dev/null 2>&1 ||
	fail "redo worker role-disabled restart failed"
if $RP -c "SELECT pagestore_materializer_lag_bytes();" \
		>"$TMPROOT/role-lag.out" 2>"$TMPROOT/role-lag.err"; then
	fail "materializer monitoring accepted an undeclared recovery worker"
fi
grep -q "requires pagestore.materializer = on" "$TMPROOT/role-lag.err" ||
	fail "undeclared worker rejection did not explain the materializer role"
echo "ok   - monitoring rejects a recovery worker without the materializer role"

[ ! -f "$REDO/$relfile" ] ||
	fail "redo worker created a local heap instead of materializing into pagestore"
echo "ok   - recovery relation has no local heap file (pagestore is authoritative)"
echo "continuous redo demo: PASS"
