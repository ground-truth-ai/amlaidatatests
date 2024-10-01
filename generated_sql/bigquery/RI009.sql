-- All party_id in Party exist in AccountPartyLink with valid time ranges. No inconsistencies with validity_start_time or is_entity_deleted detected
WITH `table` AS (
  SELECT
    `t6`.`party_id`,
    MIN(`t6`.`first_date`) AS `first_date`,
    MAX(`t6`.`last_date`) AS `last_date`
  FROM (
    SELECT
      `t4`.`account_id`,
      `t4`.`party_id`,
      MIN(`t4`.`validity_start_time`) AS `first_date`,
      MAX(
        IF(
          (
            `t4`.`next_row_validity_start_time` IS NULL
          )
          AND NOT (
            `t4`.`is_entity_deleted`
          ),
          TIMESTAMP('9995-01-01T00:00:00+00:00'),
          `t4`.`validity_start_time`
        )
      ) AS `last_date`
    FROM (
      SELECT
        `t2`.`account_id`,
        `t2`.`party_id`,
        `t2`.`validity_start_time`,
        `t2`.`is_entity_deleted`,
        `t2`.`previous_entity_deleted`,
        `t2`.`next_row_validity_start_time`,
        IF(
          (
            `t2`.`previous_row_validity_start_time` IS NULL
          )
          AND `t2`.`is_entity_deleted`,
          `t2`.`validity_start_time`,
          `t2`.`previous_row_validity_start_time`
        ) AS `previous_row_validity_start_time`
      FROM (
        SELECT
          `t0`.`account_id`,
          `t0`.`party_id`,
          `t0`.`validity_start_time`,
          COALESCE(`t0`.`is_entity_deleted`, FALSE) AS `is_entity_deleted`,
          LAG(COALESCE(`t0`.`is_entity_deleted`, FALSE)) OVER (PARTITION BY `t0`.`account_id`, `t0`.`party_id` ORDER BY `t0`.`validity_start_time` ASC) AS `previous_entity_deleted`,
          LEAD(`t0`.`validity_start_time`) OVER (PARTITION BY `t0`.`account_id`, `t0`.`party_id` ORDER BY `t0`.`validity_start_time` ASC) AS `next_row_validity_start_time`,
          LAG(`t0`.`validity_start_time`) OVER (PARTITION BY `t0`.`account_id`, `t0`.`party_id` ORDER BY `t0`.`validity_start_time` ASC) AS `previous_row_validity_start_time`
        FROM `account_party_link` AS `t0`
      ) AS `t2`
      WHERE
        (
          IF(
            (
              `t2`.`previous_row_validity_start_time` IS NULL
            )
            AND `t2`.`is_entity_deleted`,
            `t2`.`validity_start_time`,
            `t2`.`previous_row_validity_start_time`
          ) IS NULL
        )
        OR (
          `t2`.`next_row_validity_start_time` IS NULL
        )
        OR (
          `t2`.`is_entity_deleted` <> `t2`.`previous_entity_deleted`
        )
    ) AS `t4`
    GROUP BY
      1,
      2
  ) AS `t6`
  GROUP BY
    1
), `validation_table` AS (
  SELECT
    `t7`.`party_id`,
    MIN(`t7`.`first_date`) AS `first_date`,
    MAX(`t7`.`last_date`) AS `last_date`
  FROM (
    SELECT
      `t5`.`party_id`,
      MIN(`t5`.`validity_start_time`) AS `first_date`,
      MAX(
        IF(
          (
            `t5`.`next_row_validity_start_time` IS NULL
          )
          AND NOT (
            `t5`.`is_entity_deleted`
          ),
          TIMESTAMP('9995-01-01T00:00:00+00:00'),
          `t5`.`validity_start_time`
        )
      ) AS `last_date`
    FROM (
      SELECT
        `t3`.`party_id`,
        `t3`.`validity_start_time`,
        `t3`.`is_entity_deleted`,
        `t3`.`previous_entity_deleted`,
        `t3`.`next_row_validity_start_time`,
        IF(
          (
            `t3`.`previous_row_validity_start_time` IS NULL
          )
          AND `t3`.`is_entity_deleted`,
          `t3`.`validity_start_time`,
          `t3`.`previous_row_validity_start_time`
        ) AS `previous_row_validity_start_time`
      FROM (
        SELECT
          `t1`.`party_id`,
          `t1`.`validity_start_time`,
          COALESCE(`t1`.`is_entity_deleted`, FALSE) AS `is_entity_deleted`,
          LAG(COALESCE(`t1`.`is_entity_deleted`, FALSE)) OVER (PARTITION BY `t1`.`party_id` ORDER BY `t1`.`validity_start_time` ASC) AS `previous_entity_deleted`,
          LEAD(`t1`.`validity_start_time`) OVER (PARTITION BY `t1`.`party_id` ORDER BY `t1`.`validity_start_time` ASC) AS `next_row_validity_start_time`,
          LAG(`t1`.`validity_start_time`) OVER (PARTITION BY `t1`.`party_id` ORDER BY `t1`.`validity_start_time` ASC) AS `previous_row_validity_start_time`
        FROM `party` AS `t1`
      ) AS `t3`
      WHERE
        (
          IF(
            (
              `t3`.`previous_row_validity_start_time` IS NULL
            )
            AND `t3`.`is_entity_deleted`,
            `t3`.`validity_start_time`,
            `t3`.`previous_row_validity_start_time`
          ) IS NULL
        )
        OR (
          `t3`.`next_row_validity_start_time` IS NULL
        )
        OR (
          `t3`.`is_entity_deleted` <> `t3`.`previous_entity_deleted`
        )
    ) AS `t5`
    GROUP BY
      1
  ) AS `t7`
  GROUP BY
    1
)
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t14`.`party_id`,
    `t14`.`first_date` AS `first_date_account_party_link`,
    IF(`t14`.`last_date` = TIMESTAMP('9995-01-01T00:00:00+00:00'), NULL, `t14`.`last_date`) AS `last_date_account_party_link`,
    `t14`.`party_id_right`,
    `t14`.`first_date_right` AS `first_date_party`,
    IF(
      `t14`.`last_date_right` = TIMESTAMP('9995-01-01T00:00:00+00:00'),
      NULL,
      `t14`.`last_date_right`
    ) AS `last_date_party`
  FROM (
    SELECT
      `t12`.`party_id`,
      `t12`.`first_date`,
      `t12`.`last_date`,
      `t13`.`party_id` AS `party_id_right`,
      `t13`.`first_date` AS `first_date_right`,
      `t13`.`last_date` AS `last_date_right`
    FROM `table` AS `t12`
    INNER JOIN `validation_table` AS `t13`
      ON `t12`.`party_id` = `t13`.`party_id`
      AND (
        NOT (
          `t12`.`first_date` BETWEEN `t13`.`first_date` AND `t13`.`last_date`
        )
        OR NOT (
          `t12`.`last_date` BETWEEN `t13`.`first_date` AND `t13`.`last_date`
        )
      )
  ) AS `t14`
) AS `t15`
