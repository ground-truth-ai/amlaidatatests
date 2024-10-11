#!/usr/bin/env sh

rm ./generated_sql/spark-sql/*
amlaidatatests --connection_string=pyspark:// --log_sql_path=./generated_sql/spark-sql --dry_run=True
rm ./generated_sql/duckdb/*
amlaidatatests --connection_string=duckdb:// --log_sql_path=./generated_sql/duckdb --dry_run=True
rm ./generated_sql/bigquery/*
amlaidatatests --connection_string=bigquery:// --log_sql_path=./generated_sql/bigquery --dry_run=True
rm ./generated_sql/snowflake/*
amlaidatatests --connection_string=snowflake:// --log_sql_path=./generated_sql/snowflake --dry_run=True
