-- Check all tables are present and not empty, and contains less rows than threshold
SELECT
  COUNT(*) AS "CountStar(transaction)"
FROM "PLACEHOLDER"."transaction" AS "t0"
