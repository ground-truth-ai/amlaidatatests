-- Tests: risk_case_event.event_time
-- Severity: ERROR
-- Description: All values are before the interval end date (if provided) and before today's date
-- Interpretation: If matching_rows > 0, fix validity_start_time after today
SELECT
  `t1`.`total_rows`,
  `t1`.`matching_rows`,
  ieee_divide(`t1`.`matching_rows`, `t1`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(`t0`.`event_time` >= CURRENT_TIMESTAMP()) AS `matching_rows`
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
) AS `t1`
