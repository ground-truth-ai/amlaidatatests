-- Number of risk case events within risk case
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
      FROM `risk_case_event` AS `t0`
      WHERE
        `t0`.`risk_case_event_id` IS NOT NULL
    ) AS `t1`
    GROUP BY
      1
  ) AS `t2`
  WHERE
    (
      `t2`.`value_cnt` > 5000
    ) OR FALSE
) AS `t3`
