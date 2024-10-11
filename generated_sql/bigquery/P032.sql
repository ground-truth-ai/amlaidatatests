-- >= 1 month has a transaction volume of less than 40% of the monthly average by volume. Warning. 
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t1`.`mnth`,
      `t1`.`cnt`,
      ieee_divide(
        `t1`.`cnt`,
        AVG(`t1`.`cnt`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      ) AS `proportion`
    FROM (
      SELECT
        format_datetime('%Y-%m', `t0`.`book_time`) AS `mnth`,
        COUNT(*) AS `cnt`
      FROM `PLACEHOLDER`.`transaction` AS `t0`
      GROUP BY
        1
    ) AS `t1`
  ) AS `t2`
  WHERE
    `t2`.`proportion` < 0.6
) AS `t3`