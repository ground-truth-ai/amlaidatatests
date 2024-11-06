-- Tests: account_party_link
-- Severity: WARN
-- Description: check for consecutive is_entity_deleted values for the same PK
SELECT
  COUNTIF(`t2`.`count_per_pk` > 0) AS `CountStar_Greater_count_per_pk_0`
FROM (
  SELECT
    `t1`.`account_id`,
    `t1`.`party_id`,
    COUNT(*) AS `count_per_pk`
  FROM (
    SELECT
      *
    FROM `PLACEHOLDER`.`account_party_link` AS `t0`
    WHERE
      `t0`.`is_entity_deleted`
  ) AS `t1`
  GROUP BY
    1,
    2
) AS `t2`
