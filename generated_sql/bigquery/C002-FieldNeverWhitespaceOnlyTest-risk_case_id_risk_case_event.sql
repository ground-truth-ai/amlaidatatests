-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
  WHERE
    TRIM(`t0`.`risk_case_id`, ' \t\n\r\v\f') = ''
) AS `t1`
