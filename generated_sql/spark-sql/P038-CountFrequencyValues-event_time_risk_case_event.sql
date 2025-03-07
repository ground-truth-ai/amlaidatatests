-- Tests: risk_case_event.event_time
-- Severity: WARN
-- Description: Many risk case events have the same event_time
-- Interpretation: When count > 0, why are there so many risk_case_events with the same event_time?
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t2`.`event_time`,
      `t2`.`value_cnt`,
      `t2`.`value_cnt` / SUM(`t2`.`value_cnt`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `proportion`
    FROM (
      SELECT
        `t1`.`event_time`,
        COUNT(*) AS `value_cnt`
      FROM (
        SELECT
          *
        FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
        WHERE
          `t0`.`event_time` IS NOT NULL
      ) AS `t1`
      GROUP BY
        1
    ) AS `t2`
  ) AS `t3`
  WHERE
    (
      `t3`.`value_cnt` > NANVL(100000.0, NULL)
    ) OR CAST(NULL AS BOOLEAN)
) AS `t4`
