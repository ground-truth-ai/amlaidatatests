-- All AML_SAR and AML_EXIT events have AML_PROCESS_START for same risk_case_id and party_id
SELECT
  "t2"."value_cnt",
  "t2"."group_count",
  "t2"."value_cnt" / "t2"."group_count" AS "proportion"
FROM (
  SELECT
    COUNT(DISTINCT IFF("t1"."type" = 'AML_PROCESS_START', "t1"."concat", NULL)) AS "value_cnt",
    COUNT(
      DISTINCT IFF((
        "t1"."type" = 'AML_SAR'
      ) OR (
        "t1"."type" = 'AML_EXIT'
      ), "t1"."concat", NULL)
    ) AS "group_count"
  FROM (
    SELECT
      "t0"."risk_case_event_id",
      "t0"."event_time",
      "t0"."type",
      "t0"."party_id",
      "t0"."risk_case_id",
      CONCAT(
        CONCAT('', CONCAT('party_id', "t0"."party_id")),
        CONCAT('risk_case_id', "t0"."risk_case_id")
      ) AS "concat"
    FROM "PLACEHOLDER"."risk_case_event" AS "t0"
    WHERE
      "t0"."type" IS NOT NULL
  ) AS "t1"
) AS "t2"