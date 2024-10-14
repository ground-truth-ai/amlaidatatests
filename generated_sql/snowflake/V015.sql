-- Check all are positive or zero
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."transaction" AS "t0"
  WHERE
    (
      CAST(GET("t0"."normalized_booked_amount", 'nanos') AS BIGINT) < 0
    )
    OR (
      CAST(GET("t0"."normalized_booked_amount", 'nanos') AS BIGINT) > 1000000000.0
    )
) AS "t1"
