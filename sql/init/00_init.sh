#!/bin/bash
# Runs once, on first start of an empty Postgres data directory
# (docker-entrypoint-initdb.d). Passwords come from container env vars and are
# passed to psql as variables, never written to disk or to the SQL files.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v etl_user="$ETL_DB_USER" -v etl_pw="$ETL_DB_PASSWORD" \
     -v ml_user="$ML_DB_USER"   -v ml_pw="$ML_DB_PASSWORD" \
     -v dbname="$POSTGRES_DB" \
     -f /sql/01_roles_schemas.sql \
     -f /sql/02_tables.sql \
     -f /sql/03_grants.sql
