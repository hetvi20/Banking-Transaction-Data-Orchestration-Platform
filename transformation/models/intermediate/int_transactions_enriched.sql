-- transformation/models/intermediate/int_transactions_enriched.sql
-- ─────────────────────────────────────────────────────────────
-- INTERMEDIATE: Join transactions + fraud signals + accounts
-- Business logic: velocity checks, enrichment, risk scoring
-- ─────────────────────────────────────────────────────────────

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

fraud AS (
    SELECT * FROM {{ ref('stg_fraud_signals') }}
),

-- Velocity check: count transactions per account in last 1 hour
velocity AS (
    SELECT
        account_id,
        transaction_date,
        transaction_hour,
        COUNT(*)                        AS txn_count_1h,
        SUM(amount_usd)                 AS total_amount_1h,
        MAX(amount_usd)                 AS max_amount_1h
    FROM transactions
    GROUP BY account_id, transaction_date, transaction_hour
),

enriched AS (
    SELECT
        t.transaction_id,
        t.account_id,
        t.merchant_id,
        t.amount_usd,
        t.currency_code,
        t.transaction_type,
        t.transaction_status,
        t.region,
        t.channel,
        t.transaction_at,
        t.transaction_date,
        t.transaction_hour,
        t.is_large_transaction,

        -- Fraud enrichment
        COALESCE(f.fraud_score, 0)      AS fraud_score,
        COALESCE(f.is_flagged, FALSE)   AS is_fraud_flagged,
        COALESCE(f.risk_tier, 'minimal') AS risk_tier,
        f.fraud_reasons,
        f.model_version                 AS fraud_model_version,

        -- Velocity enrichment
        v.txn_count_1h,
        v.total_amount_1h,

        -- Composite risk flag
        CASE
            WHEN COALESCE(f.is_flagged, FALSE) = TRUE  THEN TRUE
            WHEN v.txn_count_1h > 20                   THEN TRUE  -- velocity spike
            WHEN t.is_large_transaction = TRUE
             AND COALESCE(f.fraud_score, 0) > 0.4      THEN TRUE
            ELSE FALSE
        END                             AS requires_review,

        -- AML compliance flag
        CASE
            WHEN t.is_large_transaction = TRUE THEN TRUE
            WHEN v.total_amount_1h > 50000     THEN TRUE
            ELSE FALSE
        END                             AS aml_flag,

        t.ingested_at,
        CURRENT_TIMESTAMP()             AS dbt_updated_at

    FROM transactions t
    LEFT JOIN fraud f     ON t.transaction_id = f.transaction_id
    LEFT JOIN velocity v  ON t.account_id     = v.account_id
                         AND t.transaction_date = v.transaction_date
                         AND t.transaction_hour = v.transaction_hour
)

SELECT * FROM enriched
