#!/bin/sh
set -eu
: "${AIRFLOW_DB_PASSWORD:?Run scripts/setup_airflow.py first}"
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set=ON_ERROR_STOP=1 \
  --set=airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE danubepulse_airflow LOGIN PASSWORD %L', :'airflow_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'danubepulse_airflow') \gexec
SELECT format('CREATE DATABASE danubepulse_airflow OWNER danubepulse_airflow')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'danubepulse_airflow') \gexec
SQL
