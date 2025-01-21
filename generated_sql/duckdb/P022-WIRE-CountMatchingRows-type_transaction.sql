-- Tests: transaction.type
-- Severity: WARN
-- Description: WIRE type is completely missing from the transactions.
-- Interpretation: When matching_rows = 0, verify the lack of WIRE transactions. Why are there no WIRE transactions?
SELECT
  "t4"."total_rows",
  "t4"."matching_rows",
  "t4"."matching_rows" / "t4"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT(*) FILTER(WHERE
      "t3"."type" = 'WIRE') AS "matching_rows"
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "t1"."transaction_id",
        "t1"."validity_start_time",
        "t1"."is_entity_deleted",
        "t1"."source_system",
        "t1"."type",
        "t1"."direction",
        "t1"."account_id",
        "t1"."counterparty_account",
        "t1"."book_time",
        "t1"."normalized_booked_amount",
        ROW_NUMBER() OVER (PARTITION BY "t1"."transaction_id" ORDER BY "t1"."validity_start_time" DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 1 AS "row_num"
      FROM (
        SELECT
          *
        FROM "PLACEHOLDER"."transaction" AS "t0"
        WHERE
          NOT (
            "t0"."is_entity_deleted"
          ) OR (
            "t0"."is_entity_deleted" IS NULL
          )
      ) AS "t1"
    ) AS "t2"
    WHERE
      "t2"."row_num" = 0
  ) AS "t3"
) AS "t4"
