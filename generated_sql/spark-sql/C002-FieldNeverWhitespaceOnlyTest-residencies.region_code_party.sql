-- Tests: party.residencies.region_code
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS `CountStar()`
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
      EXPLODE(`t0`.`residencies`) AS `residencies`,
      `t0`.`exit_date`,
      `t0`.`join_date`,
      `t0`.`assets_value_range`,
      `t0`.`civil_status_code`,
      `t0`.`education_level_code`
    FROM `PLACEHOLDER`.`party` AS `t0`
  ) AS `t1`
  WHERE
    TRIM(' \t\n\r\v\f' FROM `t1`.`residencies`.`region_code`) = ''
    AND `t1`.`residencies` IS NOT NULL
) AS `t2`
