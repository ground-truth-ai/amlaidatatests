-- Check all tables are present and not empty, and contains less rows than threshold
SELECT
  COUNT(*) AS `CountStar_party`
FROM `PLACEHOLDER`.`party` AS `t0`