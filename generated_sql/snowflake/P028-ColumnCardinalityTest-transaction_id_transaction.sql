-- Tests: transaction.transaction_id
-- Severity: WARN
-- Description: A single account and counterparty combination have many transactions. Errors at 5 million.
-- Interpretation: When count > 0, verify the transaction mapping. Why does a single account-counterparty pair have so many transactions?
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t3"."account_id",
      CAST(GET("t3"."counterparty_account", 'account_id') AS VARCHAR) AS "counterparty_account.account_id",
      COUNT(DISTINCT "t3"."transaction_id") AS "value_cnt"
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
          ROW_NUMBER() OVER (PARTITION BY "t1"."transaction_id" ORDER BY "t1"."validity_start_time" DESC NULLS LAST) - 1 AS "row_num"
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
        "t2"."row_num" = 0 AND "t2"."transaction_id" IS NOT NULL
    ) AS "t3"
    GROUP BY
      1,
      2
  ) AS "t4"
  WHERE
    (
      "t4"."value_cnt" > 5000000.0
    ) OR FALSE
) AS "t5"
