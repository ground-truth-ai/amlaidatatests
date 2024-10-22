-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`party_supplementary_data` AS `t0`
  WHERE
    TRIM(`t0`.`party_supplementary_data_id`, ' \t\n\r\v\f') = ''
) AS `t1`
