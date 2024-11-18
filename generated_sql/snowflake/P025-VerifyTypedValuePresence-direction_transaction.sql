-- Tests: transaction.direction
-- Severity: WARN
-- Description: Check the overall ratio of CREDITs to DEBITs by count of transaction_id in this dataset. ERRORs if the ratio of one direction is more than 95%.
-- Interpretation: When proportion > 0.95 or proportion < 0.05, verify the the imbalanced ratio. Why is there a significant imbalance?
SELECT
  "t4"."value_cnt",
  "t4"."group_count",
  "t4"."value_cnt" / "t4"."group_count" AS "proportion"
FROM (
  SELECT
    COUNT(DISTINCT IFF("t3"."direction" = 'DEBIT', "t3"."concat", NULL)) AS "value_cnt",
    COUNT(DISTINCT "t3"."concat") AS "group_count"
  FROM (
    SELECT
      "t2"."transaction_id",
      "t2"."validity_start_time",
      "t2"."is_entity_deleted",
      "t2"."source_system",
      "t2"."type",
      "t2"."direction",
      "t2"."account_id",
      "t2"."counterparty_account",
      "t2"."book_time",
      "t2"."normalized_booked_amount",
      "t2"."row_num",
      CONCAT('', CONCAT('transaction_id', "t2"."transaction_id")) AS "concat"
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
      "t2"."row_num" = 0 AND "t2"."direction" IS NOT NULL
  ) AS "t3"
) AS "t4"
