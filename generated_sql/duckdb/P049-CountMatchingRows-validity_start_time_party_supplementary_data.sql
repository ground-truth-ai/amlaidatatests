-- When is_entity_deleted = True, all non-key fields should be left unset or NULL. Warning.
SELECT
  "t2"."total_rows",
  "t2"."matching_rows",
  "t2"."matching_rows" / "t2"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT(*) FILTER(WHERE
      "t1"."source_system" IS NOT NULL OR "t1"."supplementary_data_payload" IS NOT NULL) AS "matching_rows"
  FROM (
    SELECT
      *
    FROM "PLACEHOLDER"."party_supplementary_data" AS "t0"
    WHERE
      "t0"."is_entity_deleted"
  ) AS "t1"
) AS "t2"
