-- Tests: transaction.type
-- Severity: WARN
-- Description: CHECK type is completely missing from the transactions.
-- Interpretation: When matching_rows = 0, verify the lack of CHECK transactions. Why are there no CHECK transactions?
SELECT
  `t4`.`total_rows`,
  `t4`.`matching_rows`,
  ieee_divide(`t4`.`matching_rows`, `t4`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(`t3`.`type` = 'CHECK') AS `matching_rows`
  FROM (
    SELECT
      *
    FROM (
      SELECT
        `t1`.`transaction_id`,
        `t1`.`validity_start_time`,
        `t1`.`is_entity_deleted`,
        `t1`.`source_system`,
        `t1`.`type`,
        `t1`.`direction`,
        `t1`.`account_id`,
        `t1`.`counterparty_account`,
        `t1`.`book_time`,
        `t1`.`normalized_booked_amount`,
        ROW_NUMBER() OVER (PARTITION BY `t1`.`transaction_id` ORDER BY `t1`.`validity_start_time` DESC) - 1 AS `row_num`
      FROM (
        SELECT
          *
        FROM `PLACEHOLDER`.`transaction` AS `t0`
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
