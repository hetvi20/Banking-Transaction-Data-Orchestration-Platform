-- transformation/models/staging/stg_fraud_signals.sql
-- ─────────────────────────────────────────────────────────────
-- STAGING: Raw fraud signals → cleaned with risk tiers
-- ─────────────────────────────────────────────────────────────

WITH source AS (
    SELECT * FROM {{ source('raw', 'fraud_signals') }}
),

cleaned AS (
    SELECT
        transaction_id,
        CAST(fraud_score AS DECIMAL(6, 4))           AS fraud_score,
        CAST(is_flagged AS BOOLEAN)                  AS is_flagged,

        -- Risk tier classification
        CASE
            WHEN CAST(fraud_score AS DECIMAL(6,4)) >= 0.90 THEN 'critical'
            WHEN CAST(fraud_score AS DECIMAL(6,4)) >= 0.75 THEN 'high'
            WHEN CAST(fraud_score AS DECIMAL(6,4)) >= 0.50 THEN 'medium'
            WHEN CAST(fraud_score AS DECIMAL(6,4)) >= 0.25 THEN 'low'
            ELSE 'minimal'
        END                                          AS risk_tier,

        fraud_reasons,
        model_version,
        CAST(scored_at AS TIMESTAMP)                 AS scored_at,
        CURRENT_TIMESTAMP()                          AS dbt_updated_at

    FROM source
    WHERE transaction_id IS NOT NULL
)

SELECT * FROM cleaned
