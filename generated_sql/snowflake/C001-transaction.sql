-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "transaction" AS "t0"
  WHERE
    CAST(GET("t0"."normalized_booked_amount", 'currency_code') AS VARCHAR) IS NULL
    AND "t0"."normalized_booked_amount" IS NOT NULL
) AS "t1"
