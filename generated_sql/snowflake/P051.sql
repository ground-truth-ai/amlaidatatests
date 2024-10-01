-- Large number of transactions have the same value across any transaction type
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t4"."Add(units, Divide(nanos, 1000000000.0))",
      "t4"."type",
      "t4"."value_cnt",
      "t4"."value_cnt" / SUM("t4"."value_cnt") OVER (PARTITION BY "t4"."type" ORDER BY NULL ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        CAST(GET("t3"."normalized_booked_amount", 'units') AS BIGINT) + (
          CAST(GET("t3"."normalized_booked_amount", 'nanos') AS BIGINT) / 1000000000.0
        ) AS "Add(units, Divide(nanos, 1000000000.0))",
        "t3"."type",
        COUNT(*) AS "value_cnt"
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
            FROM "transaction" AS "t0"
            WHERE
              "t0"."is_entity_deleted" IN (FALSE, NULL)
          ) AS "t1"
        ) AS "t2"
        WHERE
          "t2"."row_num" = 0
          AND (
            CAST(GET("t2"."normalized_booked_amount", 'units') AS BIGINT) + (
              CAST(GET("t2"."normalized_booked_amount", 'nanos') AS BIGINT) / 1000000000.0
            )
          ) IS NOT NULL
      ) AS "t3"
      GROUP BY
        1,
        2
    ) AS "t4"
  ) AS "t5"
  WHERE
    (
      "t5"."proportion" >= 0.05
    ) OR CAST(NULL AS BOOLEAN)
) AS "t6"
