-- Tests: account_party_link
-- Severity: WARN
-- Description: check for entities which are immediately deleted
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t0"."account_id",
      "t0"."party_id",
      "t0"."validity_start_time",
      COALESCE("t0"."is_entity_deleted", FALSE) AS "is_entity_deleted",
      LAG("t0"."validity_start_time") OVER (PARTITION BY "t0"."account_id", "t0"."party_id" ORDER BY "t0"."validity_start_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "previous_row_validity_start_time"
    FROM "PLACEHOLDER"."account_party_link" AS "t0"
  ) AS "t1"
  WHERE
    "t1"."is_entity_deleted" AND "t1"."previous_row_validity_start_time" IS NULL
) AS "t2"
