-- Roles and schemas. psql variables (:etl_user, :etl_pw, ...) are supplied by
-- sql/init/00_init.sh from environment variables.

CREATE ROLE :"etl_user" LOGIN PASSWORD :'etl_pw';   -- pipeline writer
CREATE ROLE :"ml_user"  LOGIN PASSWORD :'ml_pw';    -- read-only consumer (ML app)

-- Nobody gets implicit rights through PUBLIC.
REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO :"etl_user", :"ml_user";

CREATE SCHEMA staging;   -- transient landing area written by Spark (JDBC)
CREATE SCHEMA serving;   -- published, query-ready tables
CREATE SCHEMA ops;       -- run metadata / data-quality bookkeeping
