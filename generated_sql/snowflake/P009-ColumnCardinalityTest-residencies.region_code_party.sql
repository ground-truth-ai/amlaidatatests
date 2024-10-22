-- A party has more than 5 residencies. WARNING.
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t5"."party_id",
      COUNT(DISTINCT CAST(GET("t5"."residencies", 'region_code') AS VARCHAR)) AS "value_cnt"
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
          CAST(NULLIF(IFF(_u.pos = _u_2.pos_2, _u_2."residencies", NULL), '') AS OBJECT) AS "residencies",
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
              ROW_NUMBER() OVER (PARTITION BY "t1"."party_id" ORDER BY "t1"."validity_start_time" DESC NULLS LAST) - 1 AS "row_num"
            FROM (
              SELECT
                *
              FROM "PLACEHOLDER"."party" AS "t0"
              WHERE
                "t0"."is_entity_deleted" IN (FALSE, NULL)
            ) AS "t1"
          ) AS "t2"
          WHERE
            "t2"."row_num" = 0
        ) AS "t3"
        CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(
          0,
          (
            GREATEST(
              ARRAY_SIZE(
                SPLIT(
                  ARRAY_TO_STRING(NULLIF("t3"."residencies", []), 'f7cd627624314ab6b291510b796d90b3'),
                  'f7cd627624314ab6b291510b796d90b3'
                )
              )
            ) - 1
          ) + 1
        ))) AS _u(seq, key, path, index, pos, this)
        CROSS JOIN TABLE(FLATTEN(INPUT => SPLIT(
          ARRAY_TO_STRING(NULLIF("t3"."residencies", []), 'f7cd627624314ab6b291510b796d90b3'),
          'f7cd627624314ab6b291510b796d90b3'
        ))) AS _u_2(seq, key, path, pos_2, "residencies", this)
        WHERE
          _u.pos = _u_2.pos_2
          OR (
            _u.pos > (
              ARRAY_SIZE(
                SPLIT(
                  ARRAY_TO_STRING(NULLIF("t3"."residencies", []), 'f7cd627624314ab6b291510b796d90b3'),
                  'f7cd627624314ab6b291510b796d90b3'
                )
              ) - 1
            )
            AND _u_2.pos_2 = (
              ARRAY_SIZE(
                SPLIT(
                  ARRAY_TO_STRING(NULLIF("t3"."residencies", []), 'f7cd627624314ab6b291510b796d90b3'),
                  'f7cd627624314ab6b291510b796d90b3'
                )
              ) - 1
            )
          )
      ) AS "t4"
      WHERE
        CAST(GET("t4"."residencies", 'region_code') AS VARCHAR) IS NOT NULL
    ) AS "t5"
    GROUP BY
      1
  ) AS "t6"
  WHERE
    (
      "t6"."value_cnt" > 5
    ) OR FALSE
) AS "t7"
