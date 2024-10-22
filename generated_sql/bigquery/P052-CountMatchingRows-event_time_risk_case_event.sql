-- More than 10% of transactions have book_time with a round hour or half hour except 00:00:00. Common indicator of a datetime mapping from a source system in a different timezone e.g. transactions book_time=23:30:00 could be assigned to the wrong day.
SELECT
  `t1`.`total_rows`,
  `t1`.`matching_rows`,
  ieee_divide(`t1`.`matching_rows`, `t1`.`total_rows`) AS `proportion`
FROM (
  SELECT
    COUNT(*) AS `total_rows`,
    COUNTIF(
      format_datetime('%M:%S', `t0`.`event_time`) IN ('00:00', '00:30')
      AND (
        format_datetime('%H:%M:%S', `t0`.`event_time`) <> '00:00:00'
      )
    ) AS `matching_rows`
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
) AS `t1`
