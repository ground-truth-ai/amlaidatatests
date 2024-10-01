-- All currency codes must be a single normalized currency across the entire dataset
WITH "t3" AS (
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
      FROM "transaction" AS "t0"
      WHERE
        "t0"."is_entity_deleted" IN (FALSE, NULL)
    ) AS "t1"
  ) AS "t2"
  WHERE
    "t2"."row_num" = 0
    AND CAST(GET("t2"."normalized_booked_amount", 'currency_code') AS VARCHAR) IS NOT NULL
)
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      COUNT(DISTINCT CAST(GET("t4"."normalized_booked_amount", 'currency_code') AS VARCHAR)) AS "value_cnt"
    FROM "t3" AS "t4"
  ) AS "t5"
  WHERE
    (
      (
        SELECT
          COUNT(DISTINCT CAST(GET("t4"."normalized_booked_amount", 'currency_code') AS VARCHAR)) AS "CountDistinct(normalized_booked_amount.currency_code)"
        FROM "t3" AS "t4"
      ) > 1
    )
    OR FALSE
) AS "t7"
