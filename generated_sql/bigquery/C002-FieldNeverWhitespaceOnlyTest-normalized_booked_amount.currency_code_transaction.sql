-- Tests: transaction.normalized_booked_amount.currency_code
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`transaction` AS `t0`
  WHERE
    TRIM(`t0`.`normalized_booked_amount`.`currency_code`, ' \t\n\r\v\f') = ''
    AND `t0`.`normalized_booked_amount` IS NOT NULL
) AS `t1`
