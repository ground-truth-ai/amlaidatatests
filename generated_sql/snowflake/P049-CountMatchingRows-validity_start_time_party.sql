-- When is_entity_deleted = True, all non-key fields should be left unset or NULL. Warning.
SELECT
  "t2"."total_rows",
  "t2"."matching_rows",
  "t2"."matching_rows" / "t2"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT_IF(
      "t1"."source_system" IS NOT NULL
      OR "t1"."birth_date" IS NOT NULL
      OR "t1"."establishment_date" IS NOT NULL
      OR "t1"."occupation" IS NOT NULL
      OR "t1"."gender" IS NOT NULL
      OR "t1"."nationalities" IS NOT NULL
      OR "t1"."residencies" IS NOT NULL
      OR "t1"."exit_date" IS NOT NULL
      OR "t1"."join_date" IS NOT NULL
      OR "t1"."assets_value_range" IS NOT NULL
      OR "t1"."civil_status_code" IS NOT NULL
      OR "t1"."education_level_code" IS NOT NULL
    ) AS "matching_rows"
  FROM (
    SELECT
      *
    FROM "PLACEHOLDER"."party" AS "t0"
    WHERE
      "t0"."is_entity_deleted"
  ) AS "t1"
) AS "t2"
