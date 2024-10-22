-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party" AS "t0"
  WHERE
    TRIM(
      CAST(GET(CAST(GET("t0"."assets_value_range", 'end_amount') AS OBJECT), 'currency_code') AS VARCHAR),
      ' \t\n\r\v\f'
    ) = ''
    AND CAST(GET("t0"."assets_value_range", 'end_amount') AS OBJECT) IS NOT NULL
) AS "t1"
