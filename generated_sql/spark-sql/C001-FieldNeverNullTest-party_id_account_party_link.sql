-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`account_party_link` AS `t0`
  WHERE
    `t0`.`party_id` IS NULL
) AS `t1`
