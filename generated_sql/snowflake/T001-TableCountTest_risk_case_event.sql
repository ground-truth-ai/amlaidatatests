-- Tests: risk_case_event
-- Severity: ERROR
-- Description: Check all tables are present and not empty, and contains less rows than threshold
-- Interpretation: Note that not all tables are required.
SELECT
  COUNT(*) AS "CountStar(risk_case_event)"
FROM "PLACEHOLDER"."risk_case_event" AS "t0"
