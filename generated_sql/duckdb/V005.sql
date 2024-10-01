-- All values between 0 and +999,999,999 inclusive
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "transaction" AS "t0"
  WHERE
    (
      "t0"."normalized_booked_amount"."nanos" < 0
    )
    OR (
      "t0"."normalized_booked_amount"."nanos" > 1000000000.0
    )
) AS "t1"
