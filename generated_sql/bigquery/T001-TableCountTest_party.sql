-- Tests: party
-- Severity: ERROR
-- Description: Check all tables are present and not empty, and contains less rows than threshold
-- Interpretation: Note that not all tables are required.
SELECT
  COUNT(*) AS `CountStar_party`
FROM `PLACEHOLDER`.`party` AS `t0`
