-- Many risk case events have the same event_time
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t2"."event_time",
      "t2"."value_cnt",
      "t2"."value_cnt" / SUM("t2"."value_cnt") OVER (ORDER BY NULL ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        "t1"."event_time",
        COUNT(*) AS "value_cnt"
      FROM (
        SELECT
          *
        FROM "risk_case_event" AS "t0"
        WHERE
          "t0"."event_time" IS NOT NULL
      ) AS "t1"
      GROUP BY
        1
    ) AS "t2"
  ) AS "t3"
  WHERE
    (
      "t3"."value_cnt" > 100000.0
    ) OR CAST(NULL AS BOOLEAN)
) AS "t4"
