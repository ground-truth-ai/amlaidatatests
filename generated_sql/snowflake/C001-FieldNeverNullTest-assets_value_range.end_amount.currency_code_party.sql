-- Tests: party.assets_value_range.end_amount.currency_code
-- Severity: ERROR
-- Description: No nulls for non-nullable columns
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party" AS "t0"
  WHERE
    CAST(GET(CAST(GET("t0"."assets_value_range", 'end_amount') AS OBJECT), 'currency_code') AS VARCHAR) IS NULL
    AND CAST(GET("t0"."assets_value_range", 'end_amount') AS OBJECT) IS NOT NULL
) AS "t1"
