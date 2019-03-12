#! /bin/bash
set -e

POSTGRES="psql -U postgres"

# create a shared role to read & write general datasets into postgres
echo "Creating database role: airflow"
$POSTGRES <<-EOSQL
CREATE USER airflow WITH
   LOGIN
   NOSUPERUSER
   NOCREATEDB
   NOCREATEROLE
   NOINHERIT
   NOREPLICATION
   PASSWORD 'airflow';
EOSQL
