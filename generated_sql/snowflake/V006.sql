-- Check all are positive or zero
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "transaction" AS "t0"
  WHERE
    (
      CAST(GET("t0"."normalized_booked_amount", 'units') AS BIGINT) < 0
    ) OR FALSE
) AS "t1"
