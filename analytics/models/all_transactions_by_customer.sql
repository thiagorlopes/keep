-- This is a dbt model. It will be compiled to a CREATE TABLE statement.
-- The name of the table will be the name of this file: all_transactions_by_customer

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
    account_balance,
    date,
    description,
    category,
    subcategory,
    withdrawals,
    deposits,
    balance,
    most_recent_statement_date,
    (most_recent_statement_date - INTERVAL '90' DAY)::DATE AS most_recent_statement_date_minus_90_days,
    (most_recent_statement_date - INTERVAL '180' DAY)::DATE AS most_recent_statement_date_minus_180_days
FROM transactions_with_latest_date
