-- Check all tables are present and not empty, and contains less rows than threshold
SELECT
  COUNT(*) AS `CountStar_party_supplementary_data`
FROM `party_supplementary_data` AS `t0`
