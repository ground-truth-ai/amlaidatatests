-- Check all are positive or zero
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `transaction` AS `t0`
  WHERE
    (
      `t0`.`normalized_booked_amount`.`units` < 0
    ) OR FALSE
) AS `t1`
