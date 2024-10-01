-- WHEN type='COMPANY', check all are null
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `party` AS `t0`
  WHERE
    `t0`.`type` = 'COMPANY' AND `t0`.`gender` IS NOT NULL
) AS `t1`
