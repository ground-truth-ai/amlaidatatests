-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM `PLACEHOLDER`.`account_party_link` AS `t0`
  WHERE
    TRIM(`t0`.`account_id`, ' \t\n\r\v\f') = ''
) AS `t1`
