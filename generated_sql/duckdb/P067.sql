-- All AML_SAR and AML_EXIT events have AML_PROCESS_START for same risk_case_id and party_id
SELECT
  "t2"."value_cnt",
  "t2"."group_count",
  "t2"."value_cnt" / "t2"."group_count" AS "proportion"
FROM (
  SELECT
    COUNT(DISTINCT "t1"."concat") FILTER(WHERE
      "t1"."type" = 'AML_PROCESS_START') AS "value_cnt",
    COUNT(DISTINCT "t1"."concat") FILTER(WHERE
      (
        "t1"."type" = 'AML_SAR'
      ) OR (
        "t1"."type" = 'AML_EXIT'
      )) AS "group_count"
  FROM (
    SELECT
      "t0"."risk_case_event_id",
      "t0"."event_time",
      "t0"."type",
      "t0"."party_id",
      "t0"."risk_case_id",
      '' || 'party_id' || "t0"."party_id" || 'risk_case_id' || "t0"."risk_case_id" AS "concat"
    FROM "risk_case_event" AS "t0"
    WHERE
      "t0"."type" IS NOT NULL
  ) AS "t1"
) AS "t2"
