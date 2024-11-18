-- Tests: party.education_level_code
-- Severity: WARN
-- Description: Check for more than 75% of parties with the same education_level_code. Warning.
-- Interpretation: When count >= 1, check education_level_code is correctly mapped as intended.
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t4"."education_level_code",
      "t4"."value_cnt",
      "t4"."value_cnt" / SUM("t4"."value_cnt") OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        "t3"."education_level_code",
        COUNT(*) AS "value_cnt"
      FROM (
        SELECT
          *
        FROM (
          SELECT
            "t1"."party_id",
            "t1"."validity_start_time",
            "t1"."is_entity_deleted",
            "t1"."source_system",
            "t1"."type",
            "t1"."birth_date",
            "t1"."establishment_date",
            "t1"."occupation",
            "t1"."gender",
            "t1"."nationalities",
            "t1"."residencies",
            "t1"."exit_date",
            "t1"."join_date",
            "t1"."assets_value_range",
            "t1"."civil_status_code",
            "t1"."education_level_code",
            ROW_NUMBER() OVER (PARTITION BY "t1"."party_id" ORDER BY "t1"."validity_start_time" DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 1 AS "row_num"
          FROM (
            SELECT
              *
            FROM "PLACEHOLDER"."party" AS "t0"
            WHERE
              NOT (
                "t0"."is_entity_deleted"
              ) OR (
                "t0"."is_entity_deleted" IS NULL
              )
          ) AS "t1"
        ) AS "t2"
        WHERE
          "t2"."row_num" = 0 AND "t2"."education_level_code" IS NOT NULL
      ) AS "t3"
      GROUP BY
        1
    ) AS "t4"
  ) AS "t5"
  WHERE
    (
      "t5"."proportion" >= 0.75
    ) OR CAST(NULL AS BOOLEAN)
) AS "t6"
