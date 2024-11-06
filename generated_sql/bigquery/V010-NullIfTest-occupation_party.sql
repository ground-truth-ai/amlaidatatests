-- Tests: party.occupation
-- Severity: ERROR
-- Description: WHEN type='COMPANY', check no occupation specified
-- Interpretation: If count > 0, why are there companies with an occupation specified?
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    `t0`.`type` = 'COMPANY' AND `t0`.`occupation` IS NOT NULL
) AS `t1`
