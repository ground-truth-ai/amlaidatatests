-- Check all are positive or zero
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    (
      `t0`.`assets_value_range`.`end_amount`.`units` < 0
    ) OR FALSE
) AS `t1`
