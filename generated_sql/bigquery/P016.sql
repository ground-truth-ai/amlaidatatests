-- Check for account with many parties
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t3`.`party_id`,
      COUNT(DISTINCT `t3`.`account_id`) AS `value_cnt`
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
          FROM `account_party_link` AS `t0`
          WHERE
            `t0`.`is_entity_deleted` IN (FALSE, NULL)
        ) AS `t1`
      ) AS `t2`
      WHERE
        `t2`.`row_num` = 0 AND `t2`.`account_id` IS NOT NULL
    ) AS `t3`
    GROUP BY
      1
  ) AS `t4`
  WHERE
    (
      `t4`.`value_cnt` > 1000
    ) OR FALSE
) AS `t5`
