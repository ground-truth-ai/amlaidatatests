-- Tests: party.establishment_date
-- Severity: ERROR
-- Description: WHEN type='CONSUMER', check no establishment_date specified
-- Interpretation: If count > 0, why are the consumers with an establishment_date specified?
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party` AS `t0`
  WHERE
    `t0`.`type` = 'CONSUMER' AND `t0`.`establishment_date` IS NOT NULL
) AS `t1`
