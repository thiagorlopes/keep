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
    FROM main.all_transactions_by_customer
    GROUP BY ALL
)

,daily_balances AS (
    SELECT
        request_id,
        email,
        AVG(revised_average_balance) AS revised_average_balance
    FROM main.fct_daily_transactions_by_customer
    GROUP BY ALL
)

,most_recent_balances AS (
    SELECT
        request_id,
        email,
        balance AS most_recent_balance
    FROM main. all_transactions_by_customer
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id, email ORDER BY date DESC) = 1
)

SELECT
    request_id,
    email,

    -- Revenue Metrics
    SUM(daily_revenue) AS revenue_total_credit,
    SUM(daily_revenue) AS revenue_total,
    SUM(IF(date >= most_recent_statement_date_minus_90_days, daily_revenue, 0)) AS revenue_recent_90_days,
    SUM(IF(date >= most_recent_statement_date_minus_180_days AND date < most_recent_statement_date_minus_90_days, daily_revenue, 0)) AS revenue_91_to_180_days,

    -- Debit Metrics
    SUM(daily_debits) AS debits_total,
    SUM(IF(date >= most_recent_statement_date_minus_90_days, daily_debits, 0)) AS debits_recent_90_days,
    SUM(IF(date >= most_recent_statement_date_minus_180_days AND date < most_recent_statement_date_minus_90_days, daily_debits, 0)) AS debits_91_to_180_days,

    -- Placeholder Credit Metrics
    0 AS credit_card_payments,
    0 AS credit_card_recent_90_days,
    0 AS credit_card_91_to_180_days,

    -- Averages and Balances
    revised_average_balance AS average_daily_balance_across_bank_accounts,
    most_recent_balance AS most_recent_balance_across_bank_accounts
FROM daily_aggregates
LEFT JOIN daily_balances USING(request_id, email)
LEFT JOIN most_recent_balances USING(request_id, email)
GROUP BY ALL
