-- transformation/models/staging/stg_transactions.sql
-- ─────────────────────────────────────────────────────────────
-- STAGING: Raw transactions → cleaned, typed, renamed
-- Source: Azure Data Lake Bronze zone (loaded via Python ingestor)
-- Runs: every 15 minutes via Prefect trigger
-- ─────────────────────────────────────────────────────────────

WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT
        -- Keys
        transaction_id,
        account_id,
        merchant_id,

        -- Amounts
        CAST(amount AS DECIMAL(18, 2))              AS amount_usd,
        UPPER(TRIM(currency))                        AS currency_code,

        -- Categorization
        LOWER(TRIM(transaction_type))                AS transaction_type,
        LOWER(TRIM(status))                          AS transaction_status,
        UPPER(TRIM(region))                          AS region,
        LOWER(TRIM(channel))                         AS channel,

        -- Timestamps
        CAST(timestamp AS TIMESTAMP)                 AS transaction_at,
        DATE(CAST(timestamp AS TIMESTAMP))           AS transaction_date,
        HOUR(CAST(timestamp AS TIMESTAMP))           AS transaction_hour,

        -- Compliance flags
        CASE
            WHEN CAST(amount AS DECIMAL(18,2)) >= {{ var('large_txn_threshold') }}
            THEN TRUE ELSE FALSE
        END                                          AS is_large_transaction,

        -- Metadata
        CAST(ingested_at AS TIMESTAMP)               AS ingested_at,
        CURRENT_TIMESTAMP()                          AS dbt_updated_at

    FROM source
    WHERE
        transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        AND amount IS NOT NULL
        AND CAST(amount AS DECIMAL(18,2)) >= 0  -- exclude negative amounts
)

SELECT * FROM cleaned
