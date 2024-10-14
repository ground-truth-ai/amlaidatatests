-- check columns defined are a primary key on the table
SELECT
  COUNT(DISTINCT CONCAT(to_json_string(`risk_case_event_id`))) AS `unique_rows`,
  COUNT(*) AS `count`
FROM (
  SELECT
    `t0`.`risk_case_event_id`
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
) AS `t1`
