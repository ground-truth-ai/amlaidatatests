-- No nulls for non-nullable columns
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
      UNNEST("t0"."nationalities") AS "nationalities",
      "t0"."residencies",
      "t0"."exit_date",
      "t0"."join_date",
      "t0"."assets_value_range",
      "t0"."civil_status_code",
      "t0"."education_level_code"
    FROM "PLACEHOLDER"."party" AS "t0"
  ) AS "t1"
  WHERE
    (
      "t1"."nationalities"."region_code"
    ) IS NULL
    AND "t1"."nationalities" IS NOT NULL
) AS "t2"
