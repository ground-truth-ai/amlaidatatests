-- More than 10% of transactions have book_time with a round hour or half hour except 00:00:00. Common indicator of a datetime mapping from a source system in a different timezone e.g. transactions book_time=23:30:00 could be assigned to the wrong day.
SELECT
  `t4`.`total_rows`,
  `t4`.`matching_rows`,
  `t4`.`matching_rows` / `t4`.`total_rows` AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    SUM(
      CAST(DATE_FORMAT(`t3`.`validity_start_time`, 'mm:ss') IN ('00:00', '00:30')
      AND (
        DATE_FORMAT(`t3`.`validity_start_time`, 'HH:mm:ss') <> '00:00:00'
      ) AS BIGINT)
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
