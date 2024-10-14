#!/usr/bin/env bash

# Set an argument to only update a single test if
# a change has only modified a single test. This 
# avoids weirdness due to stochaistic behaviour
# in the data generator

set -e

if [ ! -z $1 ] 
then
    SUBSET="-k $1"
else
    SUBSET=""
fi


rm -f ./generated_sql/spark-sql/*$SUBSET*
amlaidatatests --connection_string=pyspark:// --log_sql_path=./generated_sql/spark-sql --dry_run=True $SUBSET
rm -f ./generated_sql/duckdb/*$SUBSET*
amlaidatatests --connection_string=duckdb:// --log_sql_path=./generated_sql/duckdb --dry_run=True $SUBSET
rm -f ./generated_sql/bigquery/*$SUBSET*
amlaidatatests --connection_string=bigquery:// --log_sql_path=./generated_sql/bigquery --dry_run=True $SUBSET
rm -f ./generated_sql/snowflake/*$SUBSET*
amlaidatatests --connection_string=snowflake:// --log_sql_path=./generated_sql/snowflake --dry_run=True $SUBSET
