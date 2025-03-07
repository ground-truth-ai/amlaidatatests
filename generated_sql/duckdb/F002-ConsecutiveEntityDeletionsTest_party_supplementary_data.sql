-- Tests: party_supplementary_data
-- Severity: WARN
-- Description: check for consecutive is_entity_deleted values for the same PK
SELECT
  COUNT(*) FILTER(WHERE
    "t2"."count_per_pk" > 0) AS "CountStar(Greater(count_per_pk, 0))"
FROM (
  SELECT
    "t1"."party_id",
    "t1"."party_supplementary_data_id",
    COUNT(*) AS "count_per_pk"
  FROM (
    SELECT
      *
    FROM "PLACEHOLDER"."party_supplementary_data" AS "t0"
    WHERE
      "t0"."is_entity_deleted"
  ) AS "t1"
  GROUP BY
    1,
    2
) AS "t2"
