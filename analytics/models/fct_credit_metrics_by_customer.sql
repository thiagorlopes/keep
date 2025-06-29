{{ config(materialized='table') }}

WITH daily_aggregates AS (
    -- First, aggregate the raw transactions to a daily level.
    SELECT
        request_id,
        email,
        date,
        -- Use the date calculation columns from the source table
        most_recent_statement_date,
        most_recent_statement_date_minus_90_days,
        most_recent_statement_date_minus_180_days,
        SUM(IF(is_revenue, deposits, 0)) as daily_revenue,
        SUM(IF(is_debit, withdrawals, 0)) as daily_debits
    FROM {{ ref('all_transactions_by_customer') }}
    GROUP BY ALL
)

SELECT
    request_id,
    email,

    -- Revenue Metrics
    SUM(daily_revenue) AS revenue_total,
    SUM(IF(date >= most_recent_statement_date_minus_90_days, daily_revenue, 0)) AS revenue_recent_90_days,
    SUM(IF(date >= most_recent_statement_date_minus_180_days AND date < most_recent_statement_date_minus_90_days, daily_revenue, 0)) AS revenue_91_to_180_days,

    -- Debit Metrics
    SUM(daily_debits) AS debits_total,
    SUM(IF(date >= most_recent_statement_date_minus_90_days, daily_debits, 0)) AS debits_recent_90_days,
    SUM(IF(date >= most_recent_statement_date_minus_180_days AND date < most_recent_statement_date_minus_90_days, daily_debits, 0)) AS debits_91_to_180_days

FROM daily_aggregates
GROUP BY
    request_id,
    email
