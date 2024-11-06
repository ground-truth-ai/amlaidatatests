-- Tests: risk_case_event.type
-- Severity: ERROR
-- Description: All AML_EXIT events have AML_PROCESS_START for same risk_case_id and party_id
-- Interpretation: If count > 0, ensure all cases with an AML_EXIT have an AML_PROCESS_START record for the same party_id and risk_case_id.
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
      `t0`.`type` = 'AML_EXIT'
  ) AS `t3`
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM (
        SELECT
          *
        FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
        WHERE
          `t0`.`type` = 'AML_PROCESS_START'
      ) AS `t4`
      WHERE
        `t3`.`party_id` = `t4`.`party_id` AND `t3`.`risk_case_id` = `t4`.`risk_case_id`
    )
) AS `t5`
