-- Tests: transaction
-- Severity: ERROR
-- Description: Check all tables are present and not empty, and contains less rows than threshold
-- Interpretation: Note that not all tables are required.
SELECT
  COUNT(*) AS `CountStar_transaction`
FROM `PLACEHOLDER`.`transaction` AS `t0`
