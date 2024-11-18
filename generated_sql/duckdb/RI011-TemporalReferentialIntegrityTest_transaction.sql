-- Tests: transaction
-- Severity: ERROR
-- Description: All account_id in Transaction exist in AccountPartyLink with valid time ranges. No inconsistencies with validity_start_time or is_entity_deleted detected
WITH "table" AS (
  SELECT
    "t5"."account_id",
    MIN("t5"."book_time") AS "first_date",
    MAX("t5"."book_time") AS "last_date"
  FROM (
    SELECT
      *
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
        ROW_NUMBER() OVER (PARTITION BY "t2"."transaction_id" ORDER BY "t2"."validity_start_time" DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 1 AS "row_num"
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
      ) AS "t2"
    ) AS "t4"
    WHERE
      "t4"."row_num" = 0
  ) AS "t5"
  GROUP BY
    1
), "validation_table" AS (
  SELECT
    "t10"."account_id",
    MIN("t10"."first_date") AS "first_date",
    MAX("t10"."last_date") AS "last_date"
  FROM (
    SELECT
      "t6"."party_id",
      "t6"."account_id",
      MIN("t6"."validity_start_time") AS "first_date",
      MAX(
        CASE
          WHEN (
            "t6"."next_row_validity_start_time" IS NULL
          )
          AND NOT (
            "t6"."is_entity_deleted"
          )
          THEN MAKE_TIMESTAMPTZ(9995, 1, 1, 0, 0, 0.0, 'UTC')
          ELSE "t6"."validity_start_time"
        END
      ) AS "last_date"
    FROM (
      SELECT
        "t3"."party_id",
        "t3"."account_id",
        "t3"."validity_start_time",
        "t3"."is_entity_deleted",
        "t3"."previous_entity_deleted",
        "t3"."next_row_validity_start_time",
        CASE
          WHEN (
            "t3"."previous_row_validity_start_time" IS NULL
          )
          AND "t3"."is_entity_deleted"
          THEN "t3"."validity_start_time"
          ELSE "t3"."previous_row_validity_start_time"
        END AS "previous_row_validity_start_time"
      FROM (
        SELECT
          "t1"."party_id",
          "t1"."account_id",
          "t1"."validity_start_time",
          COALESCE("t1"."is_entity_deleted", FALSE) AS "is_entity_deleted",
          LAG(COALESCE("t1"."is_entity_deleted", FALSE)) OVER (PARTITION BY "t1"."party_id", "t1"."account_id" ORDER BY "t1"."validity_start_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "previous_entity_deleted",
          LEAD("t1"."validity_start_time") OVER (PARTITION BY "t1"."party_id", "t1"."account_id" ORDER BY "t1"."validity_start_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "next_row_validity_start_time",
          LAG("t1"."validity_start_time") OVER (PARTITION BY "t1"."party_id", "t1"."account_id" ORDER BY "t1"."validity_start_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "previous_row_validity_start_time"
        FROM "PLACEHOLDER"."account_party_link" AS "t1"
      ) AS "t3"
      WHERE
        (
          CASE
            WHEN (
              "t3"."previous_row_validity_start_time" IS NULL
            )
            AND "t3"."is_entity_deleted"
            THEN "t3"."validity_start_time"
            ELSE "t3"."previous_row_validity_start_time"
          END IS NULL
        )
        OR (
          "t3"."next_row_validity_start_time" IS NULL
        )
        OR (
          "t3"."is_entity_deleted" <> "t3"."previous_entity_deleted"
        )
    ) AS "t6"
    GROUP BY
      1,
      2
  ) AS "t10"
  GROUP BY
    1
)
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    "t14"."account_id",
    "t14"."first_date" AS "first_date_transaction",
    CASE
      WHEN "t14"."last_date" = MAKE_TIMESTAMPTZ(9995, 1, 1, 0, 0, 0.0, 'UTC')
      THEN NULL
      ELSE "t14"."last_date"
    END AS "last_date_transaction",
    "t14"."account_id_right",
    "t14"."first_date_right" AS "first_date_account_party_link",
    CASE
      WHEN "t14"."last_date_right" = MAKE_TIMESTAMPTZ(9995, 1, 1, 0, 0, 0.0, 'UTC')
      THEN NULL
      ELSE "t14"."last_date_right"
    END AS "last_date_account_party_link"
  FROM (
    SELECT
      "t9"."account_id",
      "t9"."first_date",
      "t9"."last_date",
      "t13"."account_id" AS "account_id_right",
      "t13"."first_date" AS "first_date_right",
      "t13"."last_date" AS "last_date_right"
    FROM "table" AS "t9"
    INNER JOIN "validation_table" AS "t13"
      ON "t9"."account_id" = "t13"."account_id"
      AND (
        NOT (
          "t9"."first_date" BETWEEN "t13"."first_date" AND "t13"."last_date"
        )
        OR NOT (
          "t9"."last_date" BETWEEN "t13"."first_date" AND "t13"."last_date"
        )
      )
  ) AS "t14"
) AS "t15"
