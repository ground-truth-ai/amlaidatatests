-- check columns defined are a primary key on the table
SELECT
  COUNT(
    DISTINCT CONCAT(to_json_string(`party_id`), to_json_string(`validity_start_time`))
  ) AS `unique_rows`,
  COUNT(*) AS `count`
FROM (
  SELECT
    `t0`.`party_id`,
    `t0`.`validity_start_time`
  FROM `PLACEHOLDER`.`party` AS `t0`
) AS `t1`
