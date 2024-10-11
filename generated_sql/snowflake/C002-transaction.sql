-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "transaction" AS "t0"
  WHERE
    TRIM(
      CAST(GET("t0"."normalized_booked_amount", 'currency_code') AS VARCHAR),
      ' \t\n\r\v\f'
    ) = ''
    AND "t0"."normalized_booked_amount" IS NOT NULL
) AS "t1"
