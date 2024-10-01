-- Party with multiple AML_EXIT events
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t1`.`party_id`,
      COUNT(DISTINCT `t1`.`type`) AS `value_cnt`
    FROM (
      SELECT
        *
      FROM `risk_case_event` AS `t0`
      WHERE
        `t0`.`type` IS NOT NULL AND `t0`.`type` = 'AML_EXIT'
    ) AS `t1`
    GROUP BY
      1
  ) AS `t2`
  WHERE
    (
      `t2`.`value_cnt` > 1
    ) OR FALSE
) AS `t3`
