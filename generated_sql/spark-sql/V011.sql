-- WHEN type='COMPANY', check no gender specified
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    `t0`.`type` = 'COMPANY' AND `t0`.`gender` IS NOT NULL
) AS `t1`
