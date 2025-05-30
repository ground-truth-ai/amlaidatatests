-- Tests: risk_case_event.type
-- Severity: ERROR
-- Description: >1 suspicious activity period start for party in risk case
-- Interpretation: When count > 1, fix for only one suspicious activity period start per case
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t1`.`party_id`,
      `t1`.`risk_case_event_id`,
      COUNT(DISTINCT `t1`.`type`) AS `value_cnt`
    FROM (
      SELECT
        *
      FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
      WHERE
        `t0`.`type` IS NOT NULL AND `t0`.`type` = 'AML_SUSPICIOUS_ACTIVITY_START'
    ) AS `t1`
    GROUP BY
      1,
      2
  ) AS `t2`
  WHERE
    (
      `t2`.`value_cnt` > 1
    ) OR FALSE
) AS `t3`
