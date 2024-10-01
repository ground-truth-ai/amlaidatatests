-- All party_id in AccountPartyLink exist in Party
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    "t4"."party_id"
  FROM (
    SELECT
      "t0"."party_id"
    FROM "account_party_link" AS "t0"
  ) AS "t4"
  ANTI JOIN "party" AS "t2"
    ON "t4"."party_id" = "t2"."party_id"
) AS "t5"
