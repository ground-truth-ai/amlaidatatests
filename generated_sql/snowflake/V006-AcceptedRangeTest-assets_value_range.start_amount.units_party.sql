-- Tests: party.assets_value_range.start_amount.units
-- Severity: ERROR
-- Description: Check all are positive or zero
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party" AS "t0"
  WHERE
    (
      CAST(GET(CAST(GET("t0"."assets_value_range", 'start_amount') AS OBJECT), 'units') AS BIGINT) < 0
    )
    OR FALSE
) AS "t1"
