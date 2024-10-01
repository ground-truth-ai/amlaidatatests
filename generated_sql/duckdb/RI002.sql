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
  ANTI JOIN "account_party_link" AS "t2"
    ON "t4"."party_id" = "t2"."party_id"
) AS "t5"
