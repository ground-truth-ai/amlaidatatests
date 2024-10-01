-- check columns defined are a primary key on the table
SELECT
  COUNT(DISTINCT ("party_supplementary_data_id", "party_id", "validity_start_time")) AS "unique_rows",
  COUNT(*) AS "count"
FROM (
  SELECT
    "t0"."party_supplementary_data_id",
    "t0"."party_id",
    "t0"."validity_start_time"
  FROM "party_supplementary_data" AS "t0"
) AS "t1"
