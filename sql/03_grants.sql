-- Least-privilege grants.
--   etl_writer : may truncate/fill staging, replace rows in serving, write ops.
--   ml_reader  : read-only on serving (no staging, no ops).

GRANT USAGE, CREATE ON SCHEMA staging TO :"etl_user";
GRANT ALL ON ALL TABLES IN SCHEMA staging TO :"etl_user";

GRANT USAGE ON SCHEMA serving TO :"etl_user", :"ml_user";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA serving TO :"etl_user";
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO :"ml_user";

GRANT USAGE ON SCHEMA ops TO :"etl_user";
GRANT SELECT, INSERT, UPDATE ON ops.batch_runs TO :"etl_user";
