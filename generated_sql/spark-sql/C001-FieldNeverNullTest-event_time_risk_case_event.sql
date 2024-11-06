-- Tests: risk_case_event.event_time
-- Severity: ERROR
-- Description: No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
  WHERE
    `t0`.`event_time` IS NULL
) AS `t1`
