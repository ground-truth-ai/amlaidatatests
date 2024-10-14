-- All party_id in PartySupplementaryData (if supplied) exist in Party
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t4`.`party_id`
  FROM (
    SELECT
      `t0`.`party_id`
    FROM `PLACEHOLDER`.`party_supplementary_data` AS `t0`
  ) AS `t4`
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM `PLACEHOLDER`.`party` AS `t2`
      WHERE
        `t4`.`party_id` = `t2`.`party_id`
    )
) AS `t5`
