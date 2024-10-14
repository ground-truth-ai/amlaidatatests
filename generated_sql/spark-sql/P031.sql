-- High number of accounts with no transactions
WITH `t3` AS (
  SELECT
    `t1`.`account_id`
  FROM `PLACEHOLDER`.`account_party_link` AS `t1`
)
SELECT
  `t8`.`missing_key_count` / (
    SELECT
      COUNT(DISTINCT *) AS `CountDistinctStar()`
    FROM `t3` AS `t4`
  ) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `missing_key_count`
  FROM (
    SELECT
      `t5`.`account_id`
    FROM `t3` AS `t5`
    LEFT ANTI JOIN `PLACEHOLDER`.`transaction` AS `t2`
      ON `t5`.`account_id` = `t2`.`account_id`
  ) AS `t7`
) AS `t8`