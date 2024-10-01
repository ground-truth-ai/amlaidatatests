-- >= 1 month contains txns less than X the monthly average
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
      FROM `transaction` AS `t0`
      GROUP BY
        1
    ) AS `t1`
  ) AS `t2`
  WHERE
    `t2`.`proportion` < 0.9
) AS `t3`