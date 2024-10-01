-- Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t0`.`civil_status_code` AS `field`
  FROM `party` AS `t0`
  WHERE
    NOT (
      `t0`.`civil_status_code` IN ('SINGLE', 'LEGALLY_DIVORCED', 'DIVORCED', 'WIDOW', 'STABLE_UNION', 'SEPARATED', 'UNKNOWN')
    )
) AS `t1`
