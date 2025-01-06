CREATE TABLE aml_ai_input_ds.explainability AS
WITH
party_data AS (
  SELECT party_id
  FROM aml_ai_input_ds.party
),

feature_family_data AS (
  SELECT feature_family
  FROM UNNEST([
    'unusual_card_debit_activity',
    'unusual_cash_and_wire_inactivity',
    'unusual_cash_credit_activity',
    'unusual_cash_credit_and_debit_activity',
    'unusual_cash_debit_activity',
    'unusual_counterparty_activity',
    'unusual_credit_activity',
    'unusual_cross_border_wire_activity',
    'unusual_debit_activity',
    'unusual_monthly_proportions_of_different_credit_and_debit_transaction_types',
    'unusual_monthly_proportions_of_different_credit_transaction_types',
    'unusual_monthly_proportions_of_different_debit_transaction_types',
    'unusual_rapid_movement_of_funds',
    'unusual_round_value_wire_activity',
    'unusual_wire_credit_activity',
    'unusual_wire_credit_and_debit_activity',
    'unusual_wire_debit_activity',
    
    'party_supplementary_data_id_1',
    'party_supplementary_data_id_2',
    'party_supplementary_data_id_3',
    'party_supplementary_data_id_4',
    'party_supplementary_data_id_5',
    'party_supplementary_data_id_6',
    'party_supplementary_data_id_7',
    'party_supplementary_data_id_8',
    'party_supplementary_data_id_9',
    'party_supplementary_data_id_10'
  ]) AS feature_family
),

random_attributions AS (
  SELECT
    feature_family,
    ROUND((RAND() * 2) - 1, 4) AS attribution -- Random float between -1 and 1, rounded to 4 decimal places
  FROM feature_family_data
),

final_data AS (
  SELECT
    pd.party_id,
    TIMESTAMP('2022-01-01 00:00:00') AS risk_period_end_time, -- Hardcoded timestamp
    ARRAY_AGG(STRUCT(ra.feature_family, ra.attribution)) AS attributions
  FROM
    party_data pd
  CROSS JOIN
    random_attributions ra
  GROUP BY pd.party_id
)
SELECT * FROM final_data;