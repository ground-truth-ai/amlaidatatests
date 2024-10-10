-- Check all tables are present and not empty, and contains less rows than threshold
SELECT
  COUNT(*) AS `CountStar_risk_case_event`
FROM `risk_case_event` AS `t0`
