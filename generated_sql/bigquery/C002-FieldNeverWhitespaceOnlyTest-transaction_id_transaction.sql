-- Tests: transaction.transaction_id
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`transaction` AS `t0`
  WHERE
    TRIM(`t0`.`transaction_id`, ' \t\n\r\v\f') = ''
) AS `t1`
