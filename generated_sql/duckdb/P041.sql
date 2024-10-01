-- Check >=1 AML_EXIT in table
SELECT
  "t1"."total_rows",
  "t1"."matching_rows",
  "t1"."matching_rows" / "t1"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT(*) FILTER(WHERE
      "t0"."type" = 'AML_EXIT') AS "matching_rows"
  FROM "risk_case_event" AS "t0"
) AS "t1"
