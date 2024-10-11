-- More than 1% of the fields in this table have the same validity_start_time. Warning.
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t4`.`validity_start_time`,
      `t4`.`value_cnt`,
      ieee_divide(
        `t4`.`value_cnt`,
        SUM(`t4`.`value_cnt`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      ) AS `proportion`
    FROM (
      SELECT
        `t3`.`validity_start_time`,
        COUNT(*) AS `value_cnt`
      FROM (
        SELECT
          *
        FROM (
          SELECT
            `t1`.`account_id`,
            `t1`.`party_id`,
            `t1`.`validity_start_time`,
            `t1`.`is_entity_deleted`,
            `t1`.`role`,
            `t1`.`source_system`,
            ROW_NUMBER() OVER (PARTITION BY `t1`.`account_id`, `t1`.`party_id` ORDER BY `t1`.`validity_start_time` DESC) - 1 AS `row_num`
          FROM (
            SELECT
              *
            FROM `PLACEHOLDER`.`account_party_link` AS `t0`
            WHERE
              `t0`.`is_entity_deleted` IN (FALSE, NULL)
          ) AS `t1`
        ) AS `t2`
        WHERE
          `t2`.`row_num` = 0 AND `t2`.`validity_start_time` IS NOT NULL
      ) AS `t3`
      GROUP BY
        1
    ) AS `t4`
  ) AS `t5`
  WHERE
    (
      `t5`.`proportion` >= 0.01
    ) OR CAST(NULL AS BOOL)
) AS `t6`
