-- An entity has more than 500 validity_start_time changes. Warning.
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t3`.`party_id`,
      `t3`.`party_supplementary_data_id`,
      COUNT(DISTINCT `t3`.`validity_start_time`) AS `value_cnt`
    FROM (
      SELECT
        *
      FROM (
        SELECT
          `t1`.`party_supplementary_data_id`,
          `t1`.`validity_start_time`,
          `t1`.`is_entity_deleted`,
          `t1`.`source_system`,
          `t1`.`party_id`,
          `t1`.`supplementary_data_payload`,
          ROW_NUMBER() OVER (PARTITION BY `t1`.`party_id`, `t1`.`party_supplementary_data_id` ORDER BY `t1`.`validity_start_time` DESC) - 1 AS `row_num`
        FROM (
          SELECT
            *
          FROM `PLACEHOLDER`.`party_supplementary_data` AS `t0`
          WHERE
            `t0`.`is_entity_deleted` IN (FALSE, NULL)
        ) AS `t1`
      ) AS `t2`
      WHERE
        `t2`.`row_num` = 0 AND `t2`.`validity_start_time` IS NOT NULL
    ) AS `t3`
    GROUP BY
      1,
      2
  ) AS `t4`
  WHERE
    (
      `t4`.`value_cnt` > 500
    ) OR FALSE
) AS `t5`
