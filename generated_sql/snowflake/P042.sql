-- Check not all parties have an AML_EXIT
SELECT
  "t2"."value_cnt",
  "t2"."group_count",
  "t2"."value_cnt" / "t2"."group_count" AS "proportion"
FROM (
  SELECT
    COUNT(DISTINCT IFF("t1"."type" = 'AML_EXIT', "t1"."concat", NULL)) AS "value_cnt",
    COUNT(DISTINCT "t1"."concat") AS "group_count"
  FROM (
    SELECT
      "t0"."risk_case_event_id",
      "t0"."event_time",
      "t0"."type",
      "t0"."party_id",
      "t0"."risk_case_id",
      CONCAT('', CONCAT('party_id', "t0"."party_id")) AS "concat"
    FROM "risk_case_event" AS "t0"
    WHERE
      "t0"."type" IS NOT NULL
  ) AS "t1"
) AS "t2"
