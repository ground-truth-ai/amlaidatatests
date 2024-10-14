-- Check all are positive or zero
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`transaction` AS `t0`
  WHERE
    (
      `t0`.`normalized_booked_amount`.`nanos` < 0
    )
    OR (
      `t0`.`normalized_booked_amount`.`nanos` > NANVL(1000000000.0, NULL)
    )
) AS `t1`
