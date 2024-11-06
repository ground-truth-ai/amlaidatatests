-- When is_entity_deleted = True, all non-key fields should be left unset or NULL. Warning.
SELECT
  "t2"."total_rows",
  "t2"."matching_rows",
  "t2"."matching_rows" / "t2"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT_IF("t1"."role" IS NOT NULL OR "t1"."source_system" IS NOT NULL) AS "matching_rows"
  FROM (
    SELECT
      *
    FROM "PLACEHOLDER"."account_party_link" AS "t0"
    WHERE
      "t0"."is_entity_deleted"
  ) AS "t1"
) AS "t2"
