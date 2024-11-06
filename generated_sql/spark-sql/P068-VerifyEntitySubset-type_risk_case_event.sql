-- Tests: risk_case_event.type
-- Severity: ERROR
-- Description: All AML_SAR events have AML_PROCESS_START for same risk_case_id and party_id
-- Interpretation: If count > 0, ensure all cases with an AML_SAR have an AML_PROCESS_START record
SELECT
  COUNT(*) AS `count`
FROM (
  SELECT
    `t3`.`risk_case_event_id`,
    `t3`.`event_time`,
    `t3`.`type`,
    `t3`.`party_id`,
    `t3`.`risk_case_id`
  FROM (
    SELECT
      *
    FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
    WHERE
      `t0`.`type` = 'AML_SAR'
  ) AS `t3`
  LEFT ANTI JOIN (
    SELECT
      *
    FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
    WHERE
      `t0`.`type` = 'AML_PROCESS_START'
  ) AS `t4`
    ON `t3`.`party_id` = `t4`.`party_id` AND `t3`.`risk_case_id` = `t4`.`risk_case_id`
) AS `t5`
