-- Tests: transaction.normalized_booked_amount.units
-- Severity: ERROR
-- Description: No nulls for non-nullable columns
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."transaction" AS "t0"
  WHERE
    CAST(GET("t0"."normalized_booked_amount", 'units') AS BIGINT) IS NULL
    AND "t0"."normalized_booked_amount" IS NOT NULL
) AS "t1"
