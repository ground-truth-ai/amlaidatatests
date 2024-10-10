-- No nulls for non-nullable columns
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `risk_case_event` AS `t0`
  WHERE
    `t0`.`risk_case_id` IS NULL
) AS `t1`
