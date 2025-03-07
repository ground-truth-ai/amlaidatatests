-- Tests: account_party_link.validity_start_time
-- Severity: WARN
-- Description: More than 10% of transactions have book_time with a round hour or half hour except 00:00:00. Common indicator of a datetime mapping from a source system in a different timezone e.g. transactions book_time=23:30:00 could be assigned to the wrong day.
-- Interpretation: When proportion > 0.10, you should verify timestamps are being correctly mapped. Why are there so many timestamps which are not at midnight but are at exactly a round hour or half hour?
SELECT
  `t4`.`total_rows`,
  `t4`.`matching_rows`,
  ieee_divide(`t4`.`matching_rows`, `t4`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(
      format_datetime('%M:%S', `t3`.`validity_start_time`) IN ('00:00', '00:30')
      AND (
        format_datetime('%H:%M:%S', `t3`.`validity_start_time`) <> '00:00:00'
      )
    ) AS `matching_rows`
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
