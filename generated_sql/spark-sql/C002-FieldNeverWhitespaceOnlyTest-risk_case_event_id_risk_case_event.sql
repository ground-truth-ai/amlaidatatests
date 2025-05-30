-- Tests: risk_case_event.risk_case_event_id
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
  WHERE
    TRIM(' \t\n\r\v\f' FROM `t0`.`risk_case_event_id`) = ''
) AS `t1`
