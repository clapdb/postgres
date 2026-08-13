/* contrib/pagestore/pagestore--1.0--1.1.sql */

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

CREATE FUNCTION pagestore_branch_checkpoint(
    OUT checkpoint_redo_lsn pg_lsn,
    OUT checkpoint_end_lsn pg_lsn)
RETURNS record
AS 'MODULE_PATHNAME', 'pagestore_branch_checkpoint'
LANGUAGE C PARALLEL RESTRICTED;

CREATE FUNCTION pagestore_retention_set(
    timeline integer,
    owner_kind integer,
    owner_id bigint,
    generation bigint,
    resources integer,
    lsn pg_lsn)
RETURNS integer
AS 'MODULE_PATHNAME', 'pagestore_retention_set'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pagestore_retention_drop(
    timeline integer,
    owner_kind integer,
    owner_id bigint,
    generation bigint)
RETURNS integer
AS 'MODULE_PATHNAME', 'pagestore_retention_drop'
LANGUAGE C STRICT PARALLEL UNSAFE;

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

COMMENT ON FUNCTION pagestore_prepare_branch_from_control(text, integer, integer, pg_lsn, pg_lsn, pg_lsn) IS
'idempotently prepare a materialized branch using bootstrap horizons derived from an exact durable checkpoint';

COMMENT ON FUNCTION pagestore_capture_slru_snapshot() IS
'capture all branch SLRU bases at a cutoff proven by a paused recovery materializer restartpoint';

COMMENT ON FUNCTION pagestore_branch_checkpoint() IS
'return the exact durable checkpoint boundary selected from a quiesced pagestore writer';

COMMENT ON FUNCTION pagestore_retention_set(integer, integer, bigint, bigint, integer, pg_lsn) IS
'durably create or advance a fenced pagestore retention owner';

COMMENT ON FUNCTION pagestore_retention_drop(integer, integer, bigint, bigint) IS
'durably release a fenced pagestore retention owner generation';

COMMENT ON FUNCTION pagestore_install_prepared_branch_bootstrap(text, text, integer, integer, pg_lsn, pg_lsn, pg_lsn) IS
'install a prepared branch into a fresh same-build initdb skeleton after exact archive-bootstrap control restore';
