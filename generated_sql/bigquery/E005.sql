-- Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t0`.`type` AS `field`
  FROM `transaction` AS `t0`
  WHERE
    NOT (
      `t0`.`type` IN ('WIRE', 'CASH', 'CHECK', 'CARD', 'OTHER')
    )
) AS `t1`
