-- Tests: risk_case_event.type
-- Severity: WARN
-- Description: Suspicious activity period end but no start for party in risk case
SELECT
  `t2`.`value_cnt`,
  `t2`.`group_count`,
  ieee_divide(`t2`.`value_cnt`, `t2`.`group_count`) AS `proportion`
FROM (
  SELECT
    COUNT(DISTINCT IF(`t1`.`type` = 'AML_SUSPICIOUS_ACTIVITY_START', `t1`.`concat`, NULL)) AS `value_cnt`,
    COUNT(DISTINCT IF(`t1`.`type` = 'AML_SUSPICIOUS_ACTIVITY_END', `t1`.`concat`, NULL)) AS `group_count`
  FROM (
    SELECT
      `t0`.`risk_case_event_id`,
      `t0`.`event_time`,
      `t0`.`type`,
      `t0`.`party_id`,
      `t0`.`risk_case_id`,
      CONCAT(
        CONCAT('', CONCAT('party_id', `t0`.`party_id`)),
        CONCAT('risk_case_id', `t0`.`risk_case_id`)
      ) AS `concat`
    FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
    WHERE
      `t0`.`type` IS NOT NULL
  ) AS `t1`
) AS `t2`
