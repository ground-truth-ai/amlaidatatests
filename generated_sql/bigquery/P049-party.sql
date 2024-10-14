-- When is_entity_deleted = True, all non-key fields should be left unset or NULL. Warning.
SELECT
  `t4`.`total_rows`,
  `t4`.`matching_rows`,
  ieee_divide(`t4`.`matching_rows`, `t4`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(
      `t3`.`source_system` IS NOT NULL
      OR `t3`.`birth_date` IS NOT NULL
      OR `t3`.`establishment_date` IS NOT NULL
      OR `t3`.`occupation` IS NOT NULL
      OR `t3`.`gender` IS NOT NULL
      OR `t3`.`nationalities` IS NOT NULL
      OR `t3`.`residencies` IS NOT NULL
      OR `t3`.`exit_date` IS NOT NULL
      OR `t3`.`join_date` IS NOT NULL
      OR `t3`.`assets_value_range` IS NOT NULL
      OR `t3`.`civil_status_code` IS NOT NULL
      OR `t3`.`education_level_code` IS NOT NULL
    ) AS `matching_rows`
  FROM (
    SELECT
      *
    FROM (
      SELECT
        `t1`.`party_id`,
        `t1`.`validity_start_time`,
        `t1`.`is_entity_deleted`,
        `t1`.`source_system`,
        `t1`.`type`,
        `t1`.`birth_date`,
        `t1`.`establishment_date`,
        `t1`.`occupation`,
        `t1`.`gender`,
        `t1`.`nationalities`,
        `t1`.`residencies`,
        `t1`.`exit_date`,
        `t1`.`join_date`,
        `t1`.`assets_value_range`,
        `t1`.`civil_status_code`,
        `t1`.`education_level_code`,
        ROW_NUMBER() OVER (PARTITION BY `t1`.`party_id` ORDER BY `t1`.`validity_start_time` DESC) - 1 AS `row_num`
      FROM (
        SELECT
          *
        FROM `PLACEHOLDER`.`party` AS `t0`
        WHERE
          `t0`.`is_entity_deleted` IN (FALSE, NULL)
      ) AS `t1`
    ) AS `t2`
    WHERE
      `t2`.`row_num` = 0
  ) AS `t3`
) AS `t4`
