#!/usr/bin/env sh

amlaidatatests --connection_string=duckdb:// --log_sql_path=./generated_sql/duckdb --dry_run=True
amlaidatatests --connection_string=bigquery:// --log_sql_path=./generated_sql/bigquery --dry_run=True
amlaidatatests --connection_string=snowflake:// --log_sql_path=./generated_sql/snowflake --dry_run=True
