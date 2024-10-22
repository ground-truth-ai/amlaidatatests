-- More than 20% of accounts in the dataset have no transactions at all.
WITH `t3` AS (
  SELECT
    `t1`.`account_id`
  FROM `PLACEHOLDER`.`account_party_link` AS `t1`
)
SELECT
  ieee_divide(
    `t8`.`missing_key_count`,
    (
      SELECT
        COUNT(DISTINCT CONCAT(to_json_string(`account_id`))) AS `CountDistinctStar`
      FROM `t3` AS `t4`
    )
  ) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `missing_key_count`
  FROM (
    SELECT
      `t5`.`account_id`
    FROM `t3` AS `t5`
    WHERE
      NOT EXISTS(
        SELECT
          1
        FROM `PLACEHOLDER`.`transaction` AS `t2`
        WHERE
          `t5`.`account_id` = `t2`.`account_id`
      )
  ) AS `t7`
) AS `t8`
