-- All values between 0 and +999,999,999 inclusive
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party" AS "t0"
  WHERE
    (
      CAST(GET(CAST(GET("t0"."assets_value_range", 'start_amount') AS OBJECT), 'nanos') AS BIGINT) < 0
    )
    OR (
      CAST(GET(CAST(GET("t0"."assets_value_range", 'start_amount') AS OBJECT), 'nanos') AS BIGINT) > 1000000000.0
    )
) AS "t1"
