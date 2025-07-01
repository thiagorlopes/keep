{{ config(materialized='table') }}

-- This model combines all transactions from the statements view
-- and assigns a unique request_id.

-- CTE to calculate the most recent statement date per login_id
WITH transactions_with_latest_date AS (
    SELECT
        *,
        -- Cast the date column to TIMESTAMP to ensure correct date arithmetic
        MAX(CAST(date AS TIMESTAMP)) OVER(PARTITION BY login_id) as most_recent_statement_date
    FROM {{ ref('statements') }}
)

,enriched_transactions AS (
    -- Final selection and creation of new date columns and boolean flags
    SELECT
        username,
        email,
        address,
        financial_institution,
        employer_name,
        login_id,
        request_id,
        request_date_time as request_datetime,
        request_status,
        days_detected,
        tag,
        account_name,
        account_number,
        account_type,
        'more than 5 years' AS years_in_business,
        
        -- Safe casting to handle empty strings using the duckdb if() function
        IF(account_balance = '', NULL, CAST(account_balance AS DOUBLE)) AS account_balance,
        CAST(date AS TIMESTAMP) AS date,
        description,
        category,
        subcategory,
        IF(withdrawals = '', NULL, CAST(withdrawals AS DOUBLE)) AS withdrawals,
        IF(deposits = '', NULL, CAST(deposits AS DOUBLE)) AS deposits,
        IF(balance = '', NULL, CAST(balance AS DOUBLE)) AS balance,

        -- Boolean flags for transaction type
        IF(deposits = '', NULL, CAST(deposits AS DOUBLE)) > 0 AS is_revenue,
        IF(withdrawals = '', NULL, CAST(withdrawals AS DOUBLE)) > 0 AS is_debit,

        -- Date columns for metrics calculations downstream
        most_recent_statement_date,
        (most_recent_statement_date - INTERVAL '30' DAY)::DATE AS most_recent_statement_date_minus_30_days,
        (most_recent_statement_date - INTERVAL '60' DAY)::DATE AS most_recent_statement_date_minus_60_days,
        (most_recent_statement_date - INTERVAL '90' DAY)::DATE AS most_recent_statement_date_minus_90_days,
        (most_recent_statement_date - INTERVAL '180' DAY)::DATE AS most_recent_statement_date_minus_180_days,
        (most_recent_statement_date - INTERVAL '365' DAY)::DATE AS most_recent_statement_date_minus_365_days,
    FROM transactions_with_latest_date
)

SELECT * FROM enriched_transactions
