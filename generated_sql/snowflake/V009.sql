-- WHEN type='CONSUMER', check all are null
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "party" AS "t0"
  WHERE
    "t0"."type" = 'CONSUMER' AND "t0"."establishment_date" IS NOT NULL
) AS "t1"
