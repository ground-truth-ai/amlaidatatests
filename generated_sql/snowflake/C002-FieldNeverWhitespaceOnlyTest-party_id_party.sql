-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party" AS "t0"
  WHERE
    TRIM("t0"."party_id", ' \t\n\r\v\f') = ''
) AS "t1"
