-- All values are before the interval end date (if provided) and before today's date
SELECT
  "t4"."total_rows",
  "t4"."matching_rows",
  "t4"."matching_rows" / "t4"."total_rows" AS "proportion"
FROM (
  SELECT
    COUNT(*) AS "total_rows",
    COUNT_IF("t3"."validity_start_time" > CURRENT_TIMESTAMP()) AS "matching_rows"
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
        FROM "PLACEHOLDER"."party_supplementary_data" AS "t0"
        WHERE
          "t0"."is_entity_deleted" IN (FALSE, NULL)
      ) AS "t1"
    ) AS "t2"
    WHERE
      "t2"."row_num" = 0
  ) AS "t3"
) AS "t4"
