-- transformation/models/marts/mart_transaction_summary.sql
-- ─────────────────────────────────────────────────────────────
-- MART: Hourly transaction KPIs for Power BI executive dashboard
-- Granularity: region + transaction_type + hour
-- Consumers: Business team, Power BI Real-Time Dashboard
-- ─────────────────────────────────────────────────────────────

WITH enriched AS (
    SELECT * FROM {{ ref('int_transactions_enriched') }}
),

hourly_summary AS (
    SELECT
        transaction_date,
        transaction_hour,
        region,
        transaction_type,
        channel,
        currency_code,

        -- Volume metrics
        COUNT(*)                                    AS transaction_count,
        COUNT(DISTINCT account_id)                  AS unique_accounts,
        COUNT(DISTINCT merchant_id)                 AS unique_merchants,

        -- Amount metrics
        SUM(amount_usd)                             AS total_volume_usd,
        AVG(amount_usd)                             AS avg_transaction_usd,
        MAX(amount_usd)                             AS max_transaction_usd,
        MIN(amount_usd)                             AS min_transaction_usd,

        -- Status breakdown
        COUNT(CASE WHEN transaction_status = 'completed' THEN 1 END) AS completed_count,
        COUNT(CASE WHEN transaction_status = 'failed'    THEN 1 END) AS failed_count,
        COUNT(CASE WHEN transaction_status = 'pending'   THEN 1 END) AS pending_count,

        -- Success rate
        ROUND(
            COUNT(CASE WHEN transaction_status = 'completed' THEN 1 END) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                           AS success_rate_pct,

        -- Fraud metrics
        COUNT(CASE WHEN is_fraud_flagged = TRUE THEN 1 END)  AS fraud_flagged_count,
        COUNT(CASE WHEN aml_flag = TRUE THEN 1 END)          AS aml_flagged_count,
        COUNT(CASE WHEN is_large_transaction = TRUE THEN 1 END) AS large_txn_count,
        SUM(CASE WHEN is_fraud_flagged = TRUE THEN amount_usd ELSE 0 END) AS fraud_exposure_usd,

        CURRENT_TIMESTAMP()                         AS mart_updated_at

    FROM enriched
    GROUP BY
        transaction_date,
        transaction_hour,
        region,
        transaction_type,
        channel,
        currency_code
)

SELECT * FROM hourly_summary
