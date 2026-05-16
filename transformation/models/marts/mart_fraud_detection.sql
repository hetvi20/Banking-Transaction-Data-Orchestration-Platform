-- transformation/models/marts/mart_fraud_detection.sql
-- ─────────────────────────────────────────────────────────────
-- MART: Fraud detection summary for Power BI dashboard
-- Granularity: one row per flagged transaction
-- Consumers: Risk team, Power BI Fraud Heatmap
-- ─────────────────────────────────────────────────────────────

WITH enriched AS (
    SELECT * FROM {{ ref('int_transactions_enriched') }}
),

fraud_summary AS (
    SELECT
        transaction_id,
        account_id,
        merchant_id,
        amount_usd,
        currency_code,
        transaction_type,
        transaction_status,
        region,
        channel,
        transaction_at,
        transaction_date,
        fraud_score,
        risk_tier,
        fraud_reasons,
        fraud_model_version,
        txn_count_1h,
        requires_review,
        aml_flag,
        is_large_transaction,

        -- Derived fields for reporting
        CASE risk_tier
            WHEN 'critical' THEN 1
            WHEN 'high'     THEN 2
            WHEN 'medium'   THEN 3
            WHEN 'low'      THEN 4
            ELSE 5
        END                                     AS risk_priority,

        CASE
            WHEN risk_tier IN ('critical', 'high')  THEN 'Immediate Action'
            WHEN risk_tier = 'medium'               THEN 'Monitor'
            ELSE 'No Action'
        END                                     AS recommended_action,

        CURRENT_TIMESTAMP()                     AS mart_updated_at

    FROM enriched
    WHERE is_fraud_flagged = TRUE OR requires_review = TRUE
)

SELECT * FROM fraud_summary
ORDER BY risk_priority ASC, fraud_score DESC
