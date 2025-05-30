-- Tests: risk_case_event.type
-- Severity: ERROR
-- Description: Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS `CountStar`
FROM (
  SELECT
    `t0`.`type` AS `field`
  FROM `PLACEHOLDER`.`risk_case_event` AS `t0`
  WHERE
    NOT (
      `t0`.`type` IN ('AML_SUSPICIOUS_ACTIVITY_START', 'AML_SUSPICIOUS_ACTIVITY_END', 'AML_PROCESS_START', 'AML_PROCESS_END', 'AML_ALERT_GOOGLE', 'AML_ALERT_LEGACY', 'AML_ALERT_ADHOC', 'AML_ALERT_EXPLORATORY', 'AML_ALERT_EXTERNAL', 'AML_SAR', 'AML_EXIT')
    )
) AS `t1`
