-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`transaction` AS `t0`
  WHERE
    (
      `t0`.`normalized_booked_amount`.`units`
    ) IS NULL
    AND `t0`.`normalized_booked_amount` IS NOT NULL
) AS `t1`
