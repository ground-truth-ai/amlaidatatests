-- Tests: account_party_link.party_id
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`account_party_link` AS `t0`
  WHERE
    TRIM(' \t\n\r\v\f' FROM `t0`.`party_id`) = ''
) AS `t1`
