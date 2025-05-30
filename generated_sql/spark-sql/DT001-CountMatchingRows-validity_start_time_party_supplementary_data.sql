-- Tests: party_supplementary_data.validity_start_time
-- Severity: ERROR
-- Description: All values are before the interval end date (if provided) and before today's date
-- Interpretation: If matching_rows > 0, fix validity_start_time after today
SELECT
  `t4`.`total_rows`,
  `t4`.`matching_rows`,
  `t4`.`matching_rows` / `t4`.`total_rows` AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    SUM(CAST(`t3`.`validity_start_time` > CURRENT_TIMESTAMP() AS BIGINT)) AS `matching_rows`
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
          NOT (
            `t0`.`is_entity_deleted`
          ) OR (
            `t0`.`is_entity_deleted` IS NULL
          )
      ) AS `t1`
    ) AS `t2`
    WHERE
      `t2`.`row_num` = 0
  ) AS `t3`
) AS `t4`
