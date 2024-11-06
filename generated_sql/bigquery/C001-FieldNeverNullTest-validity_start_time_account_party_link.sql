-- Tests: account_party_link.validity_start_time
-- Severity: ERROR
-- Description: No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`account_party_link` AS `t0`
  WHERE
    `t0`.`validity_start_time` IS NULL
) AS `t1`
