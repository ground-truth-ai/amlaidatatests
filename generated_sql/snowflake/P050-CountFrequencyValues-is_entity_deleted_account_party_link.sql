-- Tests: account_party_link.is_entity_deleted
-- Severity: WARN
-- Description: More than 40% of entities have is_entity_deleted = TRUE.
-- Interpretation: When count > 0, you should verify is_entity_deleted is correctly mapped. More than 40% of entities being deleted is a high number.
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t4"."is_entity_deleted",
      "t4"."value_cnt",
      "t4"."value_cnt" / SUM("t4"."value_cnt") OVER (ORDER BY NULL ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        "t3"."is_entity_deleted",
        COUNT(*) AS "value_cnt"
      FROM (
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
            ROW_NUMBER() OVER (PARTITION BY "t1"."account_id", "t1"."party_id" ORDER BY "t1"."validity_start_time" DESC NULLS LAST) - 1 AS "row_num"
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
          "t2"."row_num" = 0 AND "t2"."is_entity_deleted" IS NOT NULL
      ) AS "t3"
      GROUP BY
        1
    ) AS "t4"
  ) AS "t5"
  WHERE
    (
      (
        "t5"."proportion" >= 0.4
      ) OR CAST(NULL AS BOOLEAN)
    )
    AND "t5"."is_entity_deleted" = TRUE
) AS "t6"
