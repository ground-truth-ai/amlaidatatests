-- Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    "t0"."type" AS "field"
  FROM "party" AS "t0"
  WHERE
    NOT (
      "t0"."type" IN ('COMPANY', 'CONSUMER')
    )
) AS "t1"
