-- All values are before the interval end date (if provided) and before today's date
SELECT
  "t1"."total_rows",
  "t1"."matching_rows",
  "t1"."matching_rows" / "t1"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT(*) FILTER(WHERE
      "t0"."event_time" >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) AS "matching_rows"
  FROM "PLACEHOLDER"."risk_case_event" AS "t0"
) AS "t1"
