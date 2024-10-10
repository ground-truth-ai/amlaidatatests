-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "transaction" AS "t0"
  WHERE
    TRIM("t0"."normalized_booked_amount"."currency_code", '
') = ''
    AND "t0"."normalized_booked_amount" IS NOT NULL
) AS "t1"
