-- <=1 AML_EXIT per risk_case_id and party_id
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t2`.`type`,
      `t2`.`risk_case_id`,
      `t2`.`party_id`,
      `t2`.`value_cnt`,
      ieee_divide(
        `t2`.`value_cnt`,
        SUM(`t2`.`value_cnt`) OVER (PARTITION BY `t2`.`risk_case_id`, `t2`.`party_id` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      ) AS `proportion`
    FROM (
      SELECT
        `t1`.`type`,
        `t1`.`risk_case_id`,
        `t1`.`party_id`,
        COUNT(*) AS `value_cnt`
      FROM (
        SELECT
          *
        FROM `risk_case_event` AS `t0`
        WHERE
          `t0`.`type` IS NOT NULL
      ) AS `t1`
      GROUP BY
        1,
        2,
        3
    ) AS `t2`
  ) AS `t3`
  WHERE
    (
      (
        `t3`.`value_cnt` > 1
      ) OR CAST(NULL AS BOOL)
    )
    AND `t3`.`type` = 'AML_EXIT'
) AS `t4`
