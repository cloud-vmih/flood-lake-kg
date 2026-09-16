#!/bin/sh
set -eu

psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=ON_ERROR_STOP=1 \
    --set=airflow_password="$AIRFLOW_DB_PASSWORD" \
    --set=polaris_password="$POLARIS_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE airflow LOGIN PASSWORD %L', :'airflow_password')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow')
\gexec

SELECT format('CREATE ROLE polaris LOGIN PASSWORD %L', :'polaris_password')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'polaris')
\gexec

SELECT format('CREATE DATABASE airflow OWNER %I', 'airflow')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = 'airflow')
\gexec

SELECT format('CREATE DATABASE polaris OWNER %I', 'polaris')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = 'polaris')
\gexec
SQL
