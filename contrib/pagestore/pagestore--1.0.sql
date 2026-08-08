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

COMMENT ON FUNCTION pagestore_shipped_wal_lsn() IS
'end of the durable WAL prefix available to this pagestore timeline';

COMMENT ON FUNCTION pagestore_materializer_lag_bytes() IS
'bytes from this pagestore materializer flushed watermark to its durable WAL end';

COMMENT ON FUNCTION pagestore_materialized_wal_lsn() IS
'last restartpoint boundary whose relation pages are durable in pagestore';
