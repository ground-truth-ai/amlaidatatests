-- AML_EXIT after AML_START for risk_case_id, party_id
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t0"."risk_case_id",
      "t0"."party_id",
      MIN("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_PROCESS_START') AS "AML_PROCESS_START_min",
      MAX("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_PROCESS_START') AS "AML_PROCESS_START_max",
      MIN("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_SAR') AS "AML_SAR_min",
      MAX("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_SAR') AS "AML_SAR_max",
      MIN("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_EXIT') AS "AML_EXIT_min",
      MAX("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_EXIT') AS "AML_EXIT_max",
      MIN("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_PROCESS_END') AS "AML_PROCESS_END_min",
      MAX("t0"."event_time") FILTER(WHERE
        "t0"."type" = 'AML_PROCESS_END') AS "AML_PROCESS_END_max"
    FROM "risk_case_event" AS "t0"
    GROUP BY
      1,
      2
  ) AS "t1"
  WHERE
    (
      "t1"."AML_PROCESS_START_max" > "t1"."AML_SAR_min"
    )
    OR (
      "t1"."AML_PROCESS_START_max" > "t1"."AML_EXIT_min"
    )
    OR (
      "t1"."AML_PROCESS_START_max" > "t1"."AML_PROCESS_END_min"
    )
    OR (
      "t1"."AML_SAR_max" > "t1"."AML_EXIT_min"
    )
    OR (
      "t1"."AML_SAR_max" > "t1"."AML_PROCESS_END_min"
    )
    OR (
      "t1"."AML_EXIT_max" > "t1"."AML_PROCESS_END_min"
    )
) AS "t2"
