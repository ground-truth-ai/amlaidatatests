-- Tests: party.type
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    TRIM(' \t\n\r\v\f' FROM `t0`.`type`) = ''
) AS `t1`
