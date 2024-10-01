-- Same number of supplementary_data_id for each customer
SELECT
  COUNT(DISTINCT `t5`.`ids`) AS `CountDistinct_ids`
FROM (
  SELECT
    `t4`.`by`,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          ibis_bq_arr_nxpaioiyrnaa3awhtznkowkyca
        FROM UNNEST(`t4`.`ids`) AS ibis_bq_arr_nxpaioiyrnaa3awhtznkowkyca
        ORDER BY
          ibis_bq_arr_nxpaioiyrnaa3awhtznkowkyca
      ),
      '|'
    ) AS `ids`
  FROM (
    SELECT
      `t3`.`party_id` AS `by`,
      ARRAY_AGG(`t3`.`party_supplementary_data_id` IGNORE NULLS) AS `ids`
    FROM (
      SELECT
        *
      FROM (
        SELECT
          `t1`.`party_supplementary_data_id`,
          `t1`.`validity_start_time`,
          `t1`.`is_entity_deleted`,
          `t1`.`source_system`,
          `t1`.`party_id`,
          `t1`.`supplementary_data_payload`,
          ROW_NUMBER() OVER (PARTITION BY `t1`.`party_id`, `t1`.`party_supplementary_data_id` ORDER BY `t1`.`validity_start_time` DESC) - 1 AS `row_num`
        FROM (
          SELECT
            *
          FROM `party_supplementary_data` AS `t0`
          WHERE
            `t0`.`is_entity_deleted` IN (FALSE, NULL)
        ) AS `t1`
      ) AS `t2`
      WHERE
        `t2`.`row_num` = 0
    ) AS `t3`
    GROUP BY
      1
  ) AS `t4`
) AS `t5`
