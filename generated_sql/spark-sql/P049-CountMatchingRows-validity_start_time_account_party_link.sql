-- Tests: account_party_link.validity_start_time
-- Severity: INFO
-- Description: When is_entity_deleted = True, all non-key fields should be left unset or NULL.
-- Interpretation: When matching_rows > 0, it is good practice to avoid populating fields where is_entity_deleted = True to make it clear that these fields should not be populated.
SELECT
  `t2`.`total_rows`,
  `t2`.`matching_rows`,
  `t2`.`matching_rows` / `t2`.`total_rows` AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    SUM(CAST(`t1`.`role` IS NOT NULL OR `t1`.`source_system` IS NOT NULL AS BIGINT)) AS `matching_rows`
  FROM (
    SELECT
      *
    FROM `PLACEHOLDER`.`account_party_link` AS `t0`
    WHERE
      `t0`.`is_entity_deleted`
  ) AS `t1`
) AS `t2`
