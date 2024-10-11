-- All account_id in Transaction exist in AccountPartyLink
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    `t4`.`account_id`
  FROM (
    SELECT
      `t0`.`account_id`
    FROM `PLACEHOLDER`.`transaction` AS `t0`
  ) AS `t4`
  LEFT ANTI JOIN `PLACEHOLDER`.`account_party_link` AS `t2`
    ON `t4`.`account_id` = `t2`.`account_id`
) AS `t5`
