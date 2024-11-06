-- Tests: party_supplementary_data.party_id
-- Severity: WARN
-- Description: No empty strings in non-nullable fields
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    *
  FROM "PLACEHOLDER"."party_supplementary_data" AS "t0"
  WHERE
    TRIM("t0"."party_id", ' 	
') = ''
) AS "t1"
