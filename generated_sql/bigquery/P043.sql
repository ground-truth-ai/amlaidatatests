-- Check >=1 party with AML_SAR 
SELECT
  `t1`.`total_rows`,
  `t1`.`matching_rows`,
  ieee_divide(`t1`.`matching_rows`, `t1`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(`t0`.`type` = 'AML_SAR') AS `matching_rows`
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
) AS `t1`