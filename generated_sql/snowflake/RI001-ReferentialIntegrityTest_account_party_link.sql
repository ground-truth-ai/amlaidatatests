-- Tests: account_party_link
-- Severity: ERROR
-- Description: All party_id in AccountPartyLink exist in Party
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    "t4"."party_id"
  FROM (
    SELECT
      "t0"."party_id"
    FROM "PLACEHOLDER"."account_party_link" AS "t0"
  ) AS "t4"
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM "PLACEHOLDER"."party" AS "t2"
      WHERE
        "t4"."party_id" = "t2"."party_id"
    )
) AS "t5"
