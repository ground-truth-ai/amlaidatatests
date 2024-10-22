-- >= 1 month has a transaction volume of less than 66% the monthly average by volume. Error.
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM (
    SELECT
      "t1"."mnth",
      "t1"."cnt",
      "t1"."cnt" / AVG("t1"."cnt") OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "proportion"
    FROM (
      SELECT
        STRFTIME("t0"."book_time", '%Y-%m') AS "mnth",
        COUNT(*) AS "cnt"
      FROM "PLACEHOLDER"."transaction" AS "t0"
      GROUP BY
        1
    ) AS "t1"
  ) AS "t2"
  WHERE
    "t2"."proportion" < 0.33
) AS "t3"
