-- All AML_EXIT events have AML_PROCESS_START for same risk_case_id and party_id
SELECT
  COUNT(*) AS `count`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t3`.`risk_case_event_id`,
      `t3`.`event_time`,
      `t3`.`type`,
      `t3`.`party_id`,
      `t3`.`risk_case_id`,
      `t3`.`concat`,
      `t4`.`risk_case_event_id` AS `risk_case_event_id_right`,
      `t4`.`event_time` AS `event_time_right`,
      `t4`.`type` AS `type_right`,
      `t4`.`party_id` AS `party_id_right`,
      `t4`.`risk_case_id` AS `risk_case_id_right`,
      `t4`.`concat` AS `concat_right`
    FROM (
      SELECT
        `t0`.`risk_case_event_id`,
        `t0`.`event_time`,
        `t0`.`type`,
        `t0`.`party_id`,
        `t0`.`risk_case_id`,
        CONCAT(
          CONCAT('', CONCAT('party_id', `t0`.`party_id`)),
          CONCAT('risk_case_id', `t0`.`risk_case_id`)
        ) AS `concat`
      FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
      WHERE
        `t0`.`type` = 'AML_EXIT'
    ) AS `t3`
    LEFT OUTER JOIN (
      SELECT
        `t0`.`risk_case_event_id`,
        `t0`.`event_time`,
        `t0`.`type`,
        `t0`.`party_id`,
        `t0`.`risk_case_id`,
        CONCAT(
          CONCAT('', CONCAT('party_id', `t0`.`party_id`)),
          CONCAT('risk_case_id', `t0`.`risk_case_id`)
        ) AS `concat`
      FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
      WHERE
        `t0`.`type` = 'AML_PROCESS_START'
    ) AS `t4`
      ON `t3`.`concat` = `t4`.`concat`
  ) AS `t5`
  WHERE
    `t5`.`concat_right` IS NULL
) AS `t6`
