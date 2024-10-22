-- No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    *
  FROM (
    SELECT
      `t0`.`party_id`,
      `t0`.`validity_start_time`,
      `t0`.`is_entity_deleted`,
      `t0`.`source_system`,
      `t0`.`type`,
      `t0`.`birth_date`,
      `t0`.`establishment_date`,
      `t0`.`occupation`,
      `t0`.`gender`,
      `t0`.`nationalities`,
      IF(pos = pos_2, `residencies`, NULL) AS `residencies`,
      `t0`.`exit_date`,
      `t0`.`join_date`,
      `t0`.`assets_value_range`,
      `t0`.`civil_status_code`,
      `t0`.`education_level_code`
    FROM `PLACEHOLDER`.`party` AS `t0`
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(`t0`.`residencies`)) - 1)) AS pos
    CROSS JOIN UNNEST(`t0`.`residencies`) AS `residencies` WITH OFFSET AS pos_2
    WHERE
      pos = pos_2
      OR (
        pos > (
          ARRAY_LENGTH(`t0`.`residencies`) - 1
        )
        AND pos_2 = (
          ARRAY_LENGTH(`t0`.`residencies`) - 1
        )
      )
  ) AS `t1`
  WHERE
    TRIM(`t1`.`residencies`.`region_code`, ' \t\n\r\v\f') = ''
    AND `t1`.`residencies` IS NOT NULL
) AS `t2`
