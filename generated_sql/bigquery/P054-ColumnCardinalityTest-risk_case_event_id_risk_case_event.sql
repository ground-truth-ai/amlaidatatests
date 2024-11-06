-- Tests: risk_case_event.risk_case_event_id
-- Severity: INFO
-- Description: More than 1000 risk case events for a single case.
-- Interpretation: When count > 1000, why are there so many risk case events for a single case?
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t1`.`risk_case_id`,
      COUNT(DISTINCT `t1`.`risk_case_event_id`) AS `value_cnt`
    FROM (
      SELECT
        *
      FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
      WHERE
        `t0`.`risk_case_event_id` IS NOT NULL
    ) AS `t1`
    GROUP BY
      1
  ) AS `t2`
  WHERE
    (
      `t2`.`value_cnt` > 1000
    ) OR FALSE
) AS `t3`
