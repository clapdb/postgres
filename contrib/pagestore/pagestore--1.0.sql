/* contrib/pagestore/pagestore--1.0.sql */

CREATE FUNCTION pagestore_shipped_wal_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'pagestore_shipped_wal_lsn'
LANGUAGE C PARALLEL RESTRICTED;

CREATE FUNCTION pagestore_materializer_lag_bytes()
RETURNS bigint
AS 'MODULE_PATHNAME', 'pagestore_materializer_lag_bytes'
LANGUAGE C PARALLEL RESTRICTED;

CREATE FUNCTION pagestore_materialized_wal_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'pagestore_materialized_wal_lsn'
LANGUAGE C PARALLEL RESTRICTED;

CREATE FUNCTION pagestore_materializer_status(
    OUT shipped_wal_lsn pg_lsn,
    OUT materialized_wal_lsn pg_lsn,
    OUT lag_bytes bigint,
    OUT release_checkpoint_lsn pg_lsn)
RETURNS record
AS 'MODULE_PATHNAME', 'pagestore_materializer_status'
LANGUAGE C PARALLEL RESTRICTED;

CREATE FUNCTION pagestore_prepare_branch_from_control(
    target_dir text,
    new_timeline integer,
    parent_timeline integer,
    base_lsn pg_lsn,
    checkpoint_redo pg_lsn,
    fork_lsn pg_lsn)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pagestore_prepare_branch_from_control'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pagestore_capture_slru_snapshot()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'pagestore_capture_slru_snapshot'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION pagestore_install_prepared_branch_bootstrap(
    prepared_dir text,
    target_dir text,
    new_timeline integer,
    parent_timeline integer,
    checkpoint_redo pg_lsn,
    recovery_lsn pg_lsn,
    fork_lsn pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'pagestore_install_prepared_branch_bootstrap'
LANGUAGE C STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION pagestore_shipped_wal_lsn() IS
'end of the durable WAL prefix available to this pagestore timeline';

COMMENT ON FUNCTION pagestore_materializer_lag_bytes() IS
'bytes from this declared pagestore materializer flushed watermark to its durable WAL end';

COMMENT ON FUNCTION pagestore_materialized_wal_lsn() IS
'last restartpoint boundary made durable by this declared pagestore materializer role';

COMMENT ON FUNCTION pagestore_materializer_status() IS
'store-observed materializer progress for writer-side control-plane monitoring';

COMMENT ON FUNCTION pagestore_prepare_branch_from_control(text, integer, integer, pg_lsn, pg_lsn, pg_lsn) IS
'idempotently prepare a materialized branch using bootstrap horizons derived from an exact durable checkpoint';

COMMENT ON FUNCTION pagestore_capture_slru_snapshot() IS
'capture all branch SLRU bases at a cutoff proven by a paused recovery materializer restartpoint';

COMMENT ON FUNCTION pagestore_install_prepared_branch_bootstrap(text, text, integer, integer, pg_lsn, pg_lsn, pg_lsn) IS
'install a prepared branch into a fresh same-build initdb skeleton after exact archive-bootstrap control restore';
