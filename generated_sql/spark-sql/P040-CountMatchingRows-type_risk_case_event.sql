-- Check >=1 AML_PROCESS_END in table
SELECT
  `t1`.`total_rows`,
  `t1`.`matching_rows`,
  `t1`.`matching_rows` / `t1`.`total_rows` AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    SUM(CAST(`t0`.`type` = 'AML_PROCESS_END' AS BIGINT)) AS `matching_rows`
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
) AS `t1`