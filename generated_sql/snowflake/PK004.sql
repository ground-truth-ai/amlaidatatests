-- check columns defined are a primary key on the table
SELECT
  COUNT(DISTINCT "risk_case_event_id") AS "unique_rows",
  COUNT(*) AS "count"
FROM (
  SELECT
    "t0"."risk_case_event_id"
  FROM "risk_case_event" AS "t0"
) AS "t1"
