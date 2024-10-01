-- >= 1 month contains txns less than X the monthly average
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t1"."mnth",
      "t1"."cnt",
      "t1"."cnt" / AVG("t1"."cnt") OVER (ORDER BY NULL ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        TO_CHAR(CAST("t0"."book_time" AS TIMESTAMP), 'yyyy-mm') AS "mnth",
        COUNT(*) AS "cnt"
      FROM "transaction" AS "t0"
      GROUP BY
        1
    ) AS "t1"
  ) AS "t2"
  WHERE
    "t2"."proportion" < 0.75
) AS "t3"
