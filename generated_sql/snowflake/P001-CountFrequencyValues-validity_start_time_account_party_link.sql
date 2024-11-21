-- Tests: account_party_link.validity_start_time
-- Severity: WARN
-- Description: More than 1% of the fields in this table have the same validity_start_time.
-- Interpretation: When count > 0, you should verify the validity_start_time is correctly mapped. why is there a very common value?
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t4"."validity_start_time",
      "t4"."value_cnt",
      "t4"."value_cnt" / SUM("t4"."value_cnt") OVER (ORDER BY NULL ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        "t3"."validity_start_time",
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
          "t2"."row_num" = 0 AND "t2"."validity_start_time" IS NOT NULL
      ) AS "t3"
      GROUP BY
        1
    ) AS "t4"
  ) AS "t5"
  WHERE
    (
      "t5"."proportion" >= 0.01
    ) OR CAST(NULL AS BOOLEAN)
) AS "t6"
