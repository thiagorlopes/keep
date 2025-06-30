{{ config(materialized='table') }}

WITH daily_aggregates AS (
    -- First, aggregate the raw transactions to a daily level.
    SELECT
        request_id,
        email,
        date,
        -- Use the date calculation columns from the source table
        most_recent_statement_date,
        MIN(date) AS earliest_statement_date,
        most_recent_statement_date_minus_90_days,
        most_recent_statement_date_minus_180_days,
        SUM(IF(is_revenue, deposits, 0)) as daily_revenue,
        SUM(IF(is_debit, withdrawals, 0)) as daily_debits,
    FROM main.all_transactions_by_customer
    GROUP BY ALL
)

,daily_balances AS (
    SELECT
        request_id,
        email,
        ROUND(AVG(revised_average_balance), 2) AS average_daily_balance
    FROM main.fct_daily_transactions_by_customer
    WHERE date > most_recent_statement_date_minus_180_days
    GROUP BY 1,2
)

,most_recent_balances AS (
    SELECT
        request_id,
        email,
        balance AS most_recent_balance
    FROM main. all_transactions_by_customer
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id, email ORDER BY date DESC) = 1
)

,daily_revenues AS (
    SELECT
        request_id,
        email,
        ROUND(SUM(daily_revenue) / (DATEDIFF(DAY, MAX(date), MIN(date)) + 1), 2) AS average_daily_revenue
    FROM daily_aggregates
    GROUP BY 1,2
)

,daily_expenses AS (
    SELECT
        request_id,
        email,
        ROUND(SUM(daily_debits) / (DATEDIFF(DAY, MAX(date), MIN(date)) + 1), 2) AS average_daily_expenses
    FROM daily_aggregates
    GROUP BY 1,2
)


SELECT
    dag.request_id,
    dag.email,

    -- Revenue Metrics
    SUM(dag.daily_revenue) AS revenue_total_credit,
    SUM(dag.daily_revenue) AS revenue_total,
    SUM(IF(dag.date >= dag.most_recent_statement_date_minus_90_days, dag.daily_revenue, 0)) AS revenue_recent_90_days,
    SUM(IF(dag.date >= dag.most_recent_statement_date_minus_180_days AND dag.date < dag.most_recent_statement_date_minus_90_days, dag.daily_revenue, 0)) AS revenue_91_to_180_days,

    -- Debit Metrics
    SUM(dag.daily_debits) AS debits_total,
    SUM(IF(dag.date >= dag.most_recent_statement_date_minus_90_days, dag.daily_debits, 0)) AS debits_recent_90_days,
    SUM(IF(dag.date >= dag.most_recent_statement_date_minus_180_days AND dag.date < dag.most_recent_statement_date_minus_90_days, dag.daily_debits, 0)) AS debits_91_to_180_days,

    -- Placeholder Credit Metrics
    0 AS credit_card_payments,
    0 AS credit_card_recent_90_days,
    0 AS credit_card_91_to_180_days,

    -- Averages and Balances
    db.average_daily_balance AS average_daily_balance_across_bank_accounts,
    mrb.most_recent_balance AS most_recent_balance_across_bank_accounts,

    -- Calculations
    SUM(dag.daily_revenue) * 2 AS estimated_annual_revenue,
    db.average_daily_balance,
    drv.average_daily_revenue,
    IF(SUM(dag.daily_revenue) * 2 > mrb.most_recent_balance, SUM(dag.daily_revenue) * 2, mrb.most_recent_balance) AS smart_revenue

FROM daily_aggregates AS dag
LEFT JOIN daily_balances AS db USING(request_id, email)
LEFT JOIN most_recent_balances AS mrb USING(request_id, email)
LEFT JOIN daily_revenues AS drv USING(request_id, email)
GROUP BY ALL
