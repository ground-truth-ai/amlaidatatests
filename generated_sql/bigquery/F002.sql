-- check for consecutive is_entity_deleted values for the same PK
SELECT
  COUNTIF(`t2`.`count_per_pk` > 0) AS `CountStar_Greater_count_per_pk_0`
FROM (
  SELECT
    `t1`.`transaction_id`,
    COUNT(*) AS `count_per_pk`
  FROM (
    SELECT
      *
    FROM `transaction` AS `t0`
    WHERE
      `t0`.`is_entity_deleted`
  ) AS `t1`
  GROUP BY
    1
) AS `t2`
