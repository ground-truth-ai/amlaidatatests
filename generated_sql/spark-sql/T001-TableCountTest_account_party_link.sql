-- Tests: account_party_link
-- Severity: ERROR
-- Description: Check all tables are present and not empty, and contains less rows than threshold
-- Interpretation: Note that not all tables are required.
SELECT
  COUNT(*) AS `CountStar(account_party_link)`
FROM `PLACEHOLDER`.`account_party_link` AS `t0`
