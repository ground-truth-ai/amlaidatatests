-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    (
      `t0`.`assets_value_range`.`end_amount`.`units`
    ) IS NULL
    AND (
      `t0`.`assets_value_range`.`end_amount`
    ) IS NOT NULL
) AS `t1`
