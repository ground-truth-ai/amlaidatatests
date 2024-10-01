-- All party_id in Party exist in AccountPartyLink
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    "t4"."party_id"
  FROM (
    SELECT
      "t0"."party_id"
    FROM "party" AS "t0"
  ) AS "t4"
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM "account_party_link" AS "t2"
      WHERE
        "t4"."party_id" = "t2"."party_id"
    )
) AS "t5"
