-- Many values
WITH "t3" AS (
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
      FROM "party" AS "t0"
      WHERE
        "t0"."is_entity_deleted" IN (FALSE, NULL)
    ) AS "t1"
  ) AS "t2"
  WHERE
    "t2"."row_num" = 0 AND "t2"."source_system" IS NOT NULL
)
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      COUNT(DISTINCT "t4"."source_system") AS "value_cnt"
    FROM "t3" AS "t4"
  ) AS "t5"
  WHERE
    (
      (
        SELECT
          COUNT(DISTINCT "t4"."source_system") AS "CountDistinct(source_system)"
        FROM "t3" AS "t4"
      ) > 500
    )
    OR FALSE
) AS "t7"
