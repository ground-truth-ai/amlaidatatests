CREATE TABLE aml_ai_input_ds.prediction AS
WITH
-- Get the list of party_ids (100k party_ids)
party_data AS (
  SELECT party_id
  FROM aml_ai_input_ds.party
),
-- Generate a random risk_score between 0 and 1 for each party_id
risk_score_data AS (
  SELECT
    party_id,
    ROUND(RAND(), 4) AS risk_score -- Random float between 0 and 1, rounded to 4 decimal places
  FROM party_data
)
SELECT
  pd.party_id,
  TIMESTAMP('2022-01-01 00:00:00') AS risk_period_end_time, -- Hardcoded timestamp
  rsd.risk_score
FROM
  party_data pd
JOIN
  risk_score_data rsd ON pd.party_id = rsd.party_id;