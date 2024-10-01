-- Positive examples with no transactions within SUSPICIOUS activity period or for X months prior to AML PROCESS START if suspicious activity period not defined
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t10"."account_id",
      "t10"."party_id",
      "t10"."validity_start_time",
      "t10"."is_entity_deleted",
      "t10"."role",
      "t10"."source_system",
      "t10"."row_num",
      "t8"."risk_case_id",
      "t8"."exits",
      "t8"."sars",
      "t8"."aml_process_start_time",
      "t8"."aml_suspicious_activity_start_time",
      "t8"."aml_suspicious_activity_end_time",
      "t3"."transaction_id",
      "t3"."validity_start_time" AS "validity_start_time_right",
      "t3"."is_entity_deleted" AS "is_entity_deleted_right",
      "t3"."source_system" AS "source_system_right",
      "t3"."type",
      "t3"."direction",
      "t3"."account_id" AS "account_id_right",
      "t3"."counterparty_account",
      "t3"."book_time",
      "t3"."normalized_booked_amount"
    FROM (
      SELECT
        *
      FROM (
        SELECT
          "t4"."account_id",
          "t4"."party_id",
          "t4"."validity_start_time",
          "t4"."is_entity_deleted",
          "t4"."role",
          "t4"."source_system",
          ROW_NUMBER() OVER (PARTITION BY "t4"."account_id", "t4"."party_id" ORDER BY "t4"."validity_start_time" DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 1 AS "row_num"
        FROM (
          SELECT
            *
          FROM "account_party_link" AS "t1"
          WHERE
            "t1"."is_entity_deleted" IN (FALSE, NULL)
        ) AS "t4"
      ) AS "t6"
      WHERE
        "t6"."row_num" = 0
    ) AS "t10"
    INNER JOIN (
      SELECT
        *
      FROM (
        SELECT
          "t2"."party_id",
          "t2"."risk_case_id",
          COUNT(*) FILTER(WHERE
            "t2"."type" = 'AML_EXIT') AS "exits",
          COUNT(*) FILTER(WHERE
            "t2"."type" = 'AML_SAR') AS "sars",
          MIN("t2"."event_time") FILTER(WHERE
            "t2"."type" = 'AML_PROCESS_START') AS "aml_process_start_time",
          MIN("t2"."event_time") FILTER(WHERE
            "t2"."type" = 'AML_SUSPICIOUS_ACTIVITY_START') AS "aml_suspicious_activity_start_time",
          MAX("t2"."event_time") FILTER(WHERE
            "t2"."type" = 'AML_SUSPICIOUS_ACTIVITY_END') AS "aml_suspicious_activity_end_time"
        FROM "risk_case_event" AS "t2"
        GROUP BY
          1,
          2
      ) AS "t5"
      WHERE
        (
          "t5"."exits" > 0
        ) OR (
          "t5"."sars" > 0
        )
    ) AS "t8"
      ON "t10"."party_id" = "t8"."party_id"
    LEFT OUTER JOIN "transaction" AS "t3"
      ON "t10"."account_id" = "t3"."account_id"
      AND CASE
        WHEN "t8"."aml_suspicious_activity_start_time" IS NOT NULL
        THEN "t3"."book_time" BETWEEN "t8"."aml_suspicious_activity_start_time" AND "t8"."aml_suspicious_activity_end_time"
        ELSE "t3"."book_time" BETWEEN "t8"."aml_process_start_time" - INTERVAL '365' DAY AND "t8"."aml_process_start_time"
      END
  ) AS "t11"
  WHERE
    "t11"."transaction_id" IS NULL
) AS "t12"
