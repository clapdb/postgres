/* contrib/pagestore/pagestore--1.1--1.2.sql */

/* Re-state the incarnation-aware control-plane APIs in the deployed target. */
CREATE OR REPLACE FUNCTION pagestore_create_branch_with_incarnation(
    new_timeline integer,
    parent_timeline integer,
    incarnation bigint,
    fork_lsn pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'pagestore_create_branch_with_incarnation'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pagestore_prepare_branch_from_control(
    target_dir text,
    new_timeline integer,
    parent_timeline integer,
    base_lsn pg_lsn,
    checkpoint_redo pg_lsn,
    fork_lsn pg_lsn,
    incarnation bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pagestore_prepare_branch_from_control'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pagestore_retention_drop_with_incarnation(
    timeline integer,
    owner_kind integer,
    owner_id bigint,
    generation bigint,
    incarnation bigint)
RETURNS integer
AS 'MODULE_PATHNAME', 'pagestore_retention_drop_with_incarnation'
LANGUAGE C STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION pagestore_create_branch_with_incarnation(integer, integer, bigint, pg_lsn) IS
'create a branch with an explicitly authorized immutable timeline incarnation';

COMMENT ON FUNCTION pagestore_prepare_branch_from_control(text, integer, integer, pg_lsn, pg_lsn, pg_lsn, bigint) IS
'idempotently prepare an explicitly authorized branch incarnation using bootstrap horizons derived from an exact durable checkpoint';

COMMENT ON FUNCTION pagestore_retention_drop_with_incarnation(integer, integer, bigint, bigint, bigint) IS
'durably release a fenced pagestore retention owner generation under an explicit immutable timeline incarnation';
