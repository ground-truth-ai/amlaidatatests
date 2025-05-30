-- Tests: account_party_link
-- Severity: WARN
-- Description: More than 50% of accounts in the dataset have no transactions at all.
-- Interpretation: When proportion > 0.5, verify the accounts have all their transactions. Why are there so many accounts with no transactions?
WITH "t3" AS (
  SELECT
    "t1"."account_id"
  FROM "PLACEHOLDER"."account_party_link" AS "t1"
)
SELECT
  "t8"."missing_key_count" / (
    SELECT
      COUNT(DISTINCT ("account_id")) AS "CountDistinctStar()"
    FROM "t3" AS "t4"
  ) AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "missing_key_count"
  FROM (
    SELECT
      "t5"."account_id"
    FROM "t3" AS "t5"
    ANTI JOIN "PLACEHOLDER"."transaction" AS "t2"
      ON "t5"."account_id" = "t2"."account_id"
  ) AS "t7"
) AS "t8"
