-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`transaction` AS `t0`
  WHERE
    TRIM(' \t\n\r\v\f' FROM `t0`.`transaction_id`) = ''
) AS `t1`
