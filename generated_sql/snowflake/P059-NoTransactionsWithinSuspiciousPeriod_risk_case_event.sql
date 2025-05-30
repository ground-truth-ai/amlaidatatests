-- Tests: risk_case_event
-- Severity: ERROR
-- Description: Finds parties with risk events (AML_EXIT or AML_SAR) with no transactions within the specified activity period or for 365 days prior to AML PROCESS START
-- Interpretation: When count > 0, why are there AML_EXIT or AML_SAR events without any associated transactional activity prior to the event for the party?
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t12"."risk_case_id",
      COUNT(IFF("t12"."transaction_id" IS NOT NULL, "t12"."transaction_id", NULL)) AS "txn_count"
    FROM (
      SELECT
        "t11"."account_id",
        "t11"."party_id",
        "t11"."first_date",
        "t11"."last_date",
        "t7"."risk_case_id",
        "t7"."exits",
        "t7"."sars",
        "t7"."aml_process_start_time",
        "t7"."aml_suspicious_activity_start_time",
        "t7"."aml_suspicious_activity_end_time",
        "t3"."transaction_id",
        "t3"."validity_start_time",
        "t3"."is_entity_deleted",
        "t3"."source_system",
        "t3"."type",
        "t3"."direction",
        "t3"."account_id" AS "account_id_right",
        "t3"."counterparty_account",
        "t3"."book_time",
        "t3"."normalized_booked_amount"
      FROM (
        SELECT
          "t9"."account_id",
          "t9"."party_id",
          MIN("t9"."first_date") AS "first_date",
          MAX("t9"."last_date") AS "last_date"
        FROM (
          SELECT
            "t8"."account_id",
            "t8"."party_id",
            MIN("t8"."validity_start_time") AS "first_date",
            MAX(
              IFF(
                (
                  "t8"."next_row_validity_start_time" IS NULL
                )
                AND NOT (
                  "t8"."is_entity_deleted"
                ),
                TIMESTAMP_TZ_FROM_PARTS(9995, 1, 1, 0, 0, 0, 0, 'UTC'),
                "t8"."validity_start_time"
              )
            ) AS "last_date"
          FROM (
            SELECT
              "t5"."account_id",
              "t5"."party_id",
              "t5"."validity_start_time",
              "t5"."is_entity_deleted",
              "t5"."previous_entity_deleted",
              "t5"."next_row_validity_start_time",
              IFF(
                (
                  "t5"."previous_row_validity_start_time" IS NULL
                )
                AND "t5"."is_entity_deleted",
                "t5"."validity_start_time",
                "t5"."previous_row_validity_start_time"
              ) AS "previous_row_validity_start_time"
            FROM (
              SELECT
                "t2"."account_id",
                "t2"."party_id",
                "t2"."validity_start_time",
                COALESCE("t2"."is_entity_deleted", FALSE) AS "is_entity_deleted",
                LAG(COALESCE("t2"."is_entity_deleted", FALSE)) OVER (PARTITION BY "t2"."account_id", "t2"."party_id" ORDER BY "t2"."validity_start_time" ASC) AS "previous_entity_deleted",
                LEAD("t2"."validity_start_time") OVER (PARTITION BY "t2"."account_id", "t2"."party_id" ORDER BY "t2"."validity_start_time" ASC) AS "next_row_validity_start_time",
                LAG("t2"."validity_start_time") OVER (PARTITION BY "t2"."account_id", "t2"."party_id" ORDER BY "t2"."validity_start_time" ASC) AS "previous_row_validity_start_time"
              FROM "PLACEHOLDER"."account_party_link" AS "t2"
            ) AS "t5"
            WHERE
              (
                IFF(
                  (
                    "t5"."previous_row_validity_start_time" IS NULL
                  )
                  AND "t5"."is_entity_deleted",
                  "t5"."validity_start_time",
                  "t5"."previous_row_validity_start_time"
                ) IS NULL
              )
              OR (
                "t5"."next_row_validity_start_time" IS NULL
              )
              OR (
                "t5"."is_entity_deleted" <> "t5"."previous_entity_deleted"
              )
          ) AS "t8"
          GROUP BY
            1,
            2
        ) AS "t9"
        GROUP BY
          1,
          2
      ) AS "t11"
      INNER JOIN (
        SELECT
          *
        FROM (
          SELECT
            "t1"."party_id",
            "t1"."risk_case_id",
            COUNT_IF("t1"."type" = 'AML_EXIT') AS "exits",
            COUNT_IF("t1"."type" = 'AML_SAR') AS "sars",
            MIN(IFF("t1"."type" = 'AML_PROCESS_START', "t1"."event_time", NULL)) AS "aml_process_start_time",
            MIN(IFF("t1"."type" = 'AML_SUSPICIOUS_ACTIVITY_START', "t1"."event_time", NULL)) AS "aml_suspicious_activity_start_time",
            MAX(IFF("t1"."type" = 'AML_SUSPICIOUS_ACTIVITY_END', "t1"."event_time", NULL)) AS "aml_suspicious_activity_end_time"
          FROM "PLACEHOLDER"."risk_case_event" AS "t1"
          GROUP BY
            1,
            2
        ) AS "t4"
        WHERE
          (
            "t4"."exits" > 0
          ) OR (
            "t4"."sars" > 0
          )
      ) AS "t7"
        ON "t11"."party_id" = "t7"."party_id"
      LEFT OUTER JOIN "PLACEHOLDER"."transaction" AS "t3"
        ON "t11"."account_id" = "t3"."account_id"
        AND "t3"."book_time" BETWEEN "t11"."first_date" AND "t11"."last_date"
        AND IFF(
          "t7"."aml_suspicious_activity_start_time" IS NULL,
          "t3"."book_time" BETWEEN DATEADD(DAY, -'365', "t7"."aml_process_start_time") AND "t7"."aml_process_start_time",
          "t3"."book_time" BETWEEN "t7"."aml_suspicious_activity_start_time" AND "t7"."aml_suspicious_activity_end_time"
        )
    ) AS "t12"
    GROUP BY
      1
  ) AS "t13"
  WHERE
    "t13"."txn_count" = 0
) AS "t14"
