-- All party_id in RiskCaseEvent exist in Party
WITH `table` AS (
  SELECT
    `t0`.`risk_case_event_id`,
    `t0`.`party_id`,
    `t0`.`event_time` AS `first_date`,
    `t0`.`event_time` AS `last_date`
  FROM `risk_case_event` AS `t0`
), `validation_table` AS (
  SELECT
    `t7`.`party_id`,
    MIN(`t7`.`first_date`) AS `first_date`,
    MAX(`t7`.`last_date`) AS `last_date`
  FROM (
    SELECT
      `t6`.`party_id`,
      MIN(`t6`.`validity_start_time`) AS `first_date`,
      MAX(
        IF(
          (
            `t6`.`next_row_validity_start_time` IS NULL
          )
          AND NOT (
            `t6`.`is_entity_deleted`
          ),
          TIMESTAMP('9995-01-01T00:00:00+00:00'),
          `t6`.`validity_start_time`
        )
      ) AS `last_date`
    FROM (
      SELECT
        `t5`.`party_id`,
        `t5`.`validity_start_time`,
        `t5`.`is_entity_deleted`,
        `t5`.`previous_entity_deleted`,
        `t5`.`next_row_validity_start_time`,
        IF(
          (
            `t5`.`previous_row_validity_start_time` IS NULL
          )
          AND `t5`.`is_entity_deleted`,
          `t5`.`validity_start_time`,
          `t5`.`previous_row_validity_start_time`
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
      ) AS `t5`
      WHERE
        (
          IF(
            (
              `t5`.`previous_row_validity_start_time` IS NULL
            )
            AND `t5`.`is_entity_deleted`,
            `t5`.`validity_start_time`,
            `t5`.`previous_row_validity_start_time`
          ) IS NULL
        )
        OR (
          `t5`.`next_row_validity_start_time` IS NULL
        )
        OR (
          `t5`.`is_entity_deleted` <> `t5`.`previous_entity_deleted`
        )
    ) AS `t6`
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
    `t11`.`risk_case_event_id`,
    `t11`.`party_id`,
    `t11`.`first_date` AS `first_date_risk_case_event`,
    IF(`t11`.`last_date` = TIMESTAMP('9995-01-01T00:00:00+00:00'), NULL, `t11`.`last_date`) AS `last_date_risk_case_event`,
    `t11`.`party_id_right`,
    `t11`.`first_date_right` AS `first_date_party`,
    IF(
      `t11`.`last_date_right` = TIMESTAMP('9995-01-01T00:00:00+00:00'),
      NULL,
      `t11`.`last_date_right`
    ) AS `last_date_party`
  FROM (
    SELECT
      `t4`.`risk_case_event_id`,
      `t4`.`party_id`,
      `t4`.`first_date`,
      `t4`.`last_date`,
      `t10`.`party_id` AS `party_id_right`,
      `t10`.`first_date` AS `first_date_right`,
      `t10`.`last_date` AS `last_date_right`
    FROM `table` AS `t4`
    INNER JOIN `validation_table` AS `t10`
      ON `t4`.`party_id` = `t10`.`party_id`
      AND (
        NOT (
          `t4`.`first_date` BETWEEN `t10`.`first_date` AND `t10`.`last_date`
        )
        OR NOT (
          `t4`.`last_date` BETWEEN `t10`.`first_date` AND `t10`.`last_date`
        )
      )
  ) AS `t11`
) AS `t12`
