-- Check for many values
WITH `t3` AS (
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
      FROM `party_supplementary_data` AS `t0`
      WHERE
        `t0`.`is_entity_deleted` IN (FALSE, NULL)
    ) AS `t1`
  ) AS `t2`
  WHERE
    `t2`.`row_num` = 0 AND `t2`.`source_system` IS NOT NULL
)
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      COUNT(DISTINCT `t4`.`source_system`) AS `value_cnt`
    FROM `t3` AS `t4`
  ) AS `t5`
  WHERE
    (
      (
        SELECT
          COUNT(DISTINCT `t4`.`source_system`) AS `CountDistinct_source_system`
        FROM `t3` AS `t4`
      ) > 500
    )
    OR FALSE
) AS `t7`
