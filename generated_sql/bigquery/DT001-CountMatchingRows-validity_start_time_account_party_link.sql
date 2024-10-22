-- All values are before the interval end date (if provided) and before today's date
SELECT
  `t4`.`total_rows`,
  `t4`.`matching_rows`,
  ieee_divide(`t4`.`matching_rows`, `t4`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(DATE(`t3`.`validity_start_time`) > CURRENT_DATE) AS `matching_rows`
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
      `t2`.`row_num` = 0
  ) AS `t3`
) AS `t4`
