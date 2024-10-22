-- check for consecutive is_entity_deleted values for the same PK
SELECT
  SUM(CAST(`t2`.`count_per_pk` > 0 AS BIGINT)) AS `CountStar(Greater(count_per_pk, 0))`
FROM (
  SELECT
    `t1`.`transaction_id`,
    COUNT(*) AS `count_per_pk`
  FROM (
    SELECT
      *
    FROM `PLACEHOLDER`.`transaction` AS `t0`
    WHERE
      `t0`.`is_entity_deleted`
  ) AS `t1`
  GROUP BY
    1
) AS `t2`
