-- When is_entity_deleted = True, all non-key fields should be left unset or NULL. Warning.
SELECT
  `t2`.`total_rows`,
  `t2`.`matching_rows`,
  `t2`.`matching_rows` / `t2`.`total_rows` AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    SUM(
      CAST(`t1`.`source_system` IS NOT NULL
      OR `t1`.`account_id` IS NOT NULL
      OR `t1`.`counterparty_account` IS NOT NULL
      OR `t1`.`book_time` IS NOT NULL
      OR `t1`.`normalized_booked_amount` IS NOT NULL AS BIGINT)
    ) AS `matching_rows`
  FROM (
    SELECT
      *
    FROM `PLACEHOLDER`.`transaction` AS `t0`
    WHERE
      `t0`.`is_entity_deleted`
  ) AS `t1`
) AS `t2`
