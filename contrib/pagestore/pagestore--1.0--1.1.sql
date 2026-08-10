/* Upgrade pagestore extension from 1.0 to 1.1. */

CREATE OR REPLACE FUNCTION pagestore_prepare_branch_from_control(
    target_dir text,
    new_timeline integer,
    parent_timeline integer,
    base_lsn pg_lsn,
    checkpoint_redo pg_lsn,
    fork_lsn pg_lsn)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pagestore_prepare_branch_from_control'
LANGUAGE C STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION pagestore_prepare_branch_from_control(text, integer, integer, pg_lsn, pg_lsn, pg_lsn) IS
'idempotently prepare a materialized branch using bootstrap horizons derived from an exact durable checkpoint';
