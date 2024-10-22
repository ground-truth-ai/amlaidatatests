-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t0"."party_id",
      "t0"."validity_start_time",
      "t0"."is_entity_deleted",
      "t0"."source_system",
      "t0"."type",
      "t0"."birth_date",
      "t0"."establishment_date",
      "t0"."occupation",
      "t0"."gender",
      "t0"."nationalities",
      CAST(NULLIF(IFF(_u.pos = _u_2.pos_2, _u_2."residencies", NULL), '') AS OBJECT) AS "residencies",
      "t0"."exit_date",
      "t0"."join_date",
      "t0"."assets_value_range",
      "t0"."civil_status_code",
      "t0"."education_level_code"
    FROM "PLACEHOLDER"."party" AS "t0"
    CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(
      0,
      (
        GREATEST(
          ARRAY_SIZE(
            SPLIT(
              ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '7cfa040864b54188ba72b3b0050d1c76'),
              '7cfa040864b54188ba72b3b0050d1c76'
            )
          )
        ) - 1
      ) + 1
    ))) AS _u(seq, key, path, index, pos, this)
    CROSS JOIN TABLE(FLATTEN(INPUT => SPLIT(
      ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '7cfa040864b54188ba72b3b0050d1c76'),
      '7cfa040864b54188ba72b3b0050d1c76'
    ))) AS _u_2(seq, key, path, pos_2, "residencies", this)
    WHERE
      _u.pos = _u_2.pos_2
      OR (
        _u.pos > (
          ARRAY_SIZE(
            SPLIT(
              ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '7cfa040864b54188ba72b3b0050d1c76'),
              '7cfa040864b54188ba72b3b0050d1c76'
            )
          ) - 1
        )
        AND _u_2.pos_2 = (
          ARRAY_SIZE(
            SPLIT(
              ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '7cfa040864b54188ba72b3b0050d1c76'),
              '7cfa040864b54188ba72b3b0050d1c76'
            )
          ) - 1
        )
      )
  ) AS "t1"
  WHERE
    TRIM(CAST(GET("t1"."residencies", 'region_code') AS VARCHAR), ' \t\n\r\v\f') = ''
    AND "t1"."residencies" IS NOT NULL
) AS "t2"
