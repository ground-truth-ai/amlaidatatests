-- Tests: risk_case_event.risk_case_event_id
-- Severity: ERROR
-- Description: No nulls for non-nullable columns
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."risk_case_event" AS "t0"
  WHERE
    "t0"."risk_case_event_id" IS NULL
) AS "t1"
