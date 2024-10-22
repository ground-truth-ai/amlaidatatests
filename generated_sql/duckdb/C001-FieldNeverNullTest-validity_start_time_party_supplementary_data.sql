-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party_supplementary_data" AS "t0"
  WHERE
    "t0"."validity_start_time" IS NULL
) AS "t1"
