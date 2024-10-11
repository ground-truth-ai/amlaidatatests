-- More than 40% of entities have is_entity_deleted = TRUE. Warning.
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
            "t1"."party_supplementary_data_id",
            "t1"."validity_start_time",
            "t1"."is_entity_deleted",
            "t1"."source_system",
            "t1"."party_id",
            "t1"."supplementary_data_payload",
            ROW_NUMBER() OVER (PARTITION BY "t1"."party_id", "t1"."party_supplementary_data_id" ORDER BY "t1"."validity_start_time" DESC NULLS LAST) - 1 AS "row_num"
          FROM (
            SELECT
              *
            FROM "party_supplementary_data" AS "t0"
            WHERE
              "t0"."is_entity_deleted" IN (FALSE, NULL)
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
