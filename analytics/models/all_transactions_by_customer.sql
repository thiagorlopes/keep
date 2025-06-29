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
-- Final selection and creation of new date columns
SELECT
    username,
    email,
    address,
    financial_institution,
    employer_name,
    login_id,
    request_id,
    request_date_time as request_datetime, -- Renaming for clarity
    request_status,
    days_detected,
    tag,
    account_name,
    account_number,
    account_type,
    
    -- Safe casting to handle empty strings
    CASE WHEN account_balance = '' THEN NULL ELSE CAST(account_balance AS DOUBLE) END AS account_balance,
    CAST(date AS TIMESTAMP) AS date,
    description,
    category,
    subcategory,
    CASE WHEN withdrawals = '' THEN NULL ELSE CAST(withdrawals AS DOUBLE) END AS withdrawals,
    CASE WHEN deposits = '' THEN NULL ELSE CAST(deposits AS DOUBLE) END AS deposits,
    CASE WHEN balance = '' THEN NULL ELSE CAST(balance AS DOUBLE) END AS balance,

    most_recent_statement_date,
    (most_recent_statement_date - INTERVAL '90' DAY)::DATE AS most_recent_statement_date_minus_90_days,
    (most_recent_statement_date - INTERVAL '180' DAY)::DATE AS most_recent_statement_date_minus_180_days
FROM transactions_with_latest_date
