-- Tests: account_party_link.party_id
-- Severity: ERROR
-- Description: Check for parties linked to a erroneously high number of accounts. The API will not ingest more than 60000 links per account.
-- Interpretation: When count >= 1, check the account_party_link table. Why are there so many links for a single party?
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t3"."account_id",
      COUNT(DISTINCT "t3"."party_id") AS "value_cnt"
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
        "t2"."row_num" = 0 AND "t2"."party_id" IS NOT NULL
    ) AS "t3"
    GROUP BY
      1
  ) AS "t4"
  WHERE
    (
      "t4"."value_cnt" > 60000
    ) OR FALSE
) AS "t5"
