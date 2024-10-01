-- Check for parties with many residencies
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t5"."party_id",
      COUNT(DISTINCT "t5"."residencies"."region_code") AS "value_cnt"
    FROM (
      SELECT
        *
      FROM (
        SELECT
          "t3"."party_id",
          "t3"."validity_start_time",
          "t3"."is_entity_deleted",
          "t3"."source_system",
          "t3"."type",
          "t3"."birth_date",
          "t3"."establishment_date",
          "t3"."occupation",
          "t3"."gender",
          "t3"."nationalities",
          UNNEST("t3"."residencies") AS "residencies",
          "t3"."exit_date",
          "t3"."join_date",
          "t3"."assets_value_range",
          "t3"."civil_status_code",
          "t3"."education_level_code",
          "t3"."row_num"
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
              FROM "party" AS "t0"
              WHERE
                "t0"."is_entity_deleted" IN (FALSE, NULL)
            ) AS "t1"
          ) AS "t2"
          WHERE
            "t2"."row_num" = 0
        ) AS "t3"
      ) AS "t4"
      WHERE
        (
          "t4"."residencies"."region_code"
        ) IS NOT NULL
    ) AS "t5"
    GROUP BY
      1
  ) AS "t6"
  WHERE
    (
      "t6"."value_cnt" > 5
    ) OR FALSE
) AS "t7"
