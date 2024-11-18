-- Tests: account_party_link.source_system
-- Severity: WARN
-- Description: Excessive distinct values of source_system. Warns above 500.
-- Interpretation: When count >= 500, check source system is not populated using party data. Why are there so many distinct values of source_system?
WITH "t3" AS (
  SELECT
    *
  FROM (
    SELECT
      "t1"."account_id",
      "t1"."party_id",
      "t1"."validity_start_time",
      "t1"."is_entity_deleted",
      "t1"."role",
      "t1"."source_system",
      ROW_NUMBER() OVER (PARTITION BY "t1"."account_id", "t1"."party_id" ORDER BY "t1"."validity_start_time" DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 1 AS "row_num"
    FROM (
      SELECT
        *
      FROM "PLACEHOLDER"."account_party_link" AS "t0"
      WHERE
        NOT (
          "t0"."is_entity_deleted"
        ) OR (
          "t0"."is_entity_deleted" IS NULL
        )
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
