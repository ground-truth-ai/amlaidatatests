-- WHEN type='COMPANY', check no birth_date specified
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    `t0`.`type` = 'COMPANY' AND `t0`.`birth_date` IS NOT NULL
) AS `t1`
