-- Tests: party.join_date
-- Severity: WARN
-- Description: Exit date after birth date for party
-- Interpretation: If matching_rows > 0, review matching parties. Why are there parties who joined the bank prior to their birth?
SELECT
  "t4"."total_rows",
  "t4"."matching_rows",
  "t4"."matching_rows" / "t4"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT(*) FILTER(WHERE
      "t3"."join_date" < "t3"."birth_date") AS "matching_rows"
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
      "t2"."row_num" = 0
  ) AS "t3"
) AS "t4"
