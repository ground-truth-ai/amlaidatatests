-- Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t0`.`direction` AS `field`
  FROM `transaction` AS `t0`
  WHERE
    NOT (
      `t0`.`direction` IN ('DEBIT', 'CREDIT')
    )
) AS `t1`
