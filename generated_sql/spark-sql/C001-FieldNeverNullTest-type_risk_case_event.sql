-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar()`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
  WHERE
    `t0`.`type` IS NULL
) AS `t1`
