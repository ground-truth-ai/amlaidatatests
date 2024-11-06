-- Tests: transaction.normalized_booked_amount.units
-- Severity: ERROR
-- Description: All values between 0 and +999,999,999 inclusive
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."transaction" AS "t0"
  WHERE
    (
      CAST(GET("t0"."normalized_booked_amount", 'units') AS BIGINT) < 0
    ) OR FALSE
) AS "t1"
