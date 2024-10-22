-- >= 1 month has a transaction volume of less than 40% of the monthly average by volume. Warning.
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
      FROM "PLACEHOLDER"."transaction" AS "t0"
      GROUP BY
        1
    ) AS "t1"
  ) AS "t2"
  WHERE
    "t2"."proportion" < 0.6
) AS "t3"
