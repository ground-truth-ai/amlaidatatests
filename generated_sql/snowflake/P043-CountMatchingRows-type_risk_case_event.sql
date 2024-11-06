-- Tests: risk_case_event.type
-- Severity: ERROR
-- Description: Check >=1 party with AML_SAR
-- Interpretation: If matching_rows = 0, ensure there is at least one party with a SAR for the model to learn from.
SELECT
  "t1"."total_rows",
  "t1"."matching_rows",
  "t1"."matching_rows" / "t1"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT_IF("t0"."type" = 'AML_SAR') AS "matching_rows"
  FROM "PLACEHOLDER"."risk_case_event" AS "t0"
) AS "t1"
