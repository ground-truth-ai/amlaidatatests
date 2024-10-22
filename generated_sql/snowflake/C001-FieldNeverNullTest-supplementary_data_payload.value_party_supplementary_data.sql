-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party_supplementary_data" AS "t0"
  WHERE
    CAST(GET("t0"."supplementary_data_payload", 'value') AS DOUBLE) IS NULL
    AND "t0"."supplementary_data_payload" IS NOT NULL
) AS "t1"
