-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `party` AS `t0`
  WHERE
    TRIM(`t0`.`assets_value_range`.`end_amount`.`currency_code`, ' \t\n\r\v\f') = ''
    AND (
      `t0`.`assets_value_range`.`end_amount`
    ) IS NOT NULL
) AS `t1`