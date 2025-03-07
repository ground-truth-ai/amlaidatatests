-- Tests: party.assets_value_range.end_amount.currency_code
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    TRIM(' \t\n\r\v\f' FROM `t0`.`assets_value_range`.`end_amount`.`currency_code`) = ''
    AND (
      `t0`.`assets_value_range`.`end_amount`
    ) IS NOT NULL
) AS `t1`
