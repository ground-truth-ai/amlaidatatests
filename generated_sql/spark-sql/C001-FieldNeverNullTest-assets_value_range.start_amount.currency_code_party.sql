-- Tests: party.assets_value_range.start_amount.currency_code
-- Severity: ERROR
-- Description: No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    (
      `t0`.`assets_value_range`.`start_amount`.`currency_code`
    ) IS NULL
    AND (
      `t0`.`assets_value_range`.`start_amount`
    ) IS NOT NULL
) AS `t1`
