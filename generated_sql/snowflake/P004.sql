-- WHEN type='COMPANY', check for high frequency values
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t4"."establishment_date",
      "t4"."value_cnt",
      "t4"."value_cnt" / SUM("t4"."value_cnt") OVER (ORDER BY NULL ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        "t3"."establishment_date",
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
            ROW_NUMBER() OVER (PARTITION BY "t1"."party_id" ORDER BY "t1"."validity_start_time" DESC NULLS LAST) - 1 AS "row_num"
          FROM (
            SELECT
              *
            FROM "party" AS "t0"
            WHERE
              "t0"."is_entity_deleted" IN (FALSE, NULL)
          ) AS "t1"
        ) AS "t2"
        WHERE
          "t2"."row_num" = 0
          AND "t2"."type" = 'COMPANY'
          AND "t2"."establishment_date" IS NOT NULL
      ) AS "t3"
      GROUP BY
        1
    ) AS "t4"
  ) AS "t5"
  WHERE
    (
      "t5"."proportion" >= 0.01
    ) OR CAST(NULL AS BOOLEAN)
) AS "t6"
