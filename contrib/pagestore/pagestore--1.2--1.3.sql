/* contrib/pagestore/pagestore--1.2--1.3.sql */


CREATE FUNCTION pagestore_timeline_state(
    timeline integer,
    OUT state text,
    OUT incarnation bigint)
RETURNS record
AS 'MODULE_PATHNAME', 'pagestore_timeline_state'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pagestore_delete_branch(
    timeline integer,
    incarnation bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'pagestore_delete_branch'
LANGUAGE C STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION pagestore_timeline_state(integer) IS
'lifecycle state (live, deleting, deleted) and incarnation of a store timeline; NULLs if undefined';

COMMENT ON FUNCTION pagestore_delete_branch(integer, bigint) IS
'durably begin deleting a branch timeline fenced by its incarnation; the store reclaims it asynchronously';
