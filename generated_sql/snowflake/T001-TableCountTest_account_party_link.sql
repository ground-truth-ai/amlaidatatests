-- Check all tables are present and not empty, and contains less rows than threshold
SELECT
  COUNT(*) AS "CountStar(account_party_link)"
FROM "PLACEHOLDER"."account_party_link" AS "t0"
