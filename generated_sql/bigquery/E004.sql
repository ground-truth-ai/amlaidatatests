-- Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t0`.`role` AS `field`
  FROM `account_party_link` AS `t0`
  WHERE
    NOT (
      `t0`.`role` IN ('PRIMARY_HOLDER', 'SECONDARY_HOLDER', 'SUPPLEMENTARY_HOLDER')
    )
) AS `t1`
