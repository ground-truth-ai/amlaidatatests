-- Tests: party.education_level_code
-- Severity: ERROR
-- Description: Field is constrained to permitted enumerations
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    "t0"."education_level_code" AS "field"
  FROM "PLACEHOLDER"."party" AS "t0"
  WHERE
    NOT (
      "t0"."education_level_code" IN ('LESS_THAN_PRIMARY_EDUCATION', 'PRIMARY_EDUCATION', 'LOWER_SECONDARY_EDUCATION', 'UPPER_SECONDARY_EDUCATION', 'POST_SECONDARY_NON_TERTIARY_EDUCATION', 'SHORT_CYCLE_TERTIARY_EDUCATION', 'BACHELORS_OR_EQUIVALENT', 'MASTERS_OR_EQUIVALENT', 'DOCTORAL_OR_EQUIVALENT', 'NOT_ELSEWHERE_CLASSIFIED', 'UNKNOWN')
    )
) AS "t1"
