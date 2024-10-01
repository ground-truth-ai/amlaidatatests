-- check no additional fields exist on the table
SELECT
  COUNT(*) AS `CountStar_account_party_link`
FROM `gtai-amlai-sandbox-uat`.`my_bq_input_dataset_5`.`account_party_link` AS `t0`
