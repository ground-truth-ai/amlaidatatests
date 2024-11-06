-- Tests: risk_case_event.type
-- Severity: ERROR
-- Description: Check >=1 AML_PROCESS_END in table
-- Interpretation: If matching_rows = 0, ensure there is at least one party with a AML_PROCESS_END for the model to learn from.
SELECT
  "t1"."total_rows",
  "t1"."matching_rows",
  "t1"."matching_rows" / "t1"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT(*) FILTER(WHERE
      "t0"."type" = 'AML_PROCESS_END') AS "matching_rows"
  FROM "PLACEHOLDER"."risk_case_event" AS "t0"
) AS "t1"
