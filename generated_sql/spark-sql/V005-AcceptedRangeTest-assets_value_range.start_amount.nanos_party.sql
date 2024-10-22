-- All values between 0 and +999,999,999 inclusive
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    (
      `t0`.`assets_value_range`.`start_amount`.`nanos` < 0
    )
    OR (
      `t0`.`assets_value_range`.`start_amount`.`nanos` > NANVL(1000000000.0, NULL)
    )
) AS `t1`
