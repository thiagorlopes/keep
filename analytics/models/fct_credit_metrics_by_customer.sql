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
        most_recent_statement_date_minus_365_days,
        SUM(IF(is_revenue, deposits, 0)) as daily_revenue,
        SUM(IF(is_debit, withdrawals, 0)) as daily_debits
    FROM main.all_transactions_by_customer
    GROUP BY ALL
)

,daily_balances AS (
    SELECT
        request_id,
        email,
        ROUND(AVG(revised_average_balance), 2) AS revised_average_balance
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

,daily_revenues AS (
    SELECT
        request_id,
        email,
        ROUND(SUM(daily_revenue) / (DATEDIFF('day', MIN(date), MAX(date)) + 1), 2) AS average_daily_revenue
    FROM daily_aggregates
    GROUP BY 1,2
)

,weekly_revenues_deduplicated AS (
    SELECT
        request_id,
        email,
        date,
        WEEKOFYEAR(date) AS week_of_year,
        weekly_revenue
    FROM main.fct_daily_transactions_by_customer
    WHERE date >= '2024-01-22'
    ORDER BY date desc

)

,weekly_revenues AS (
    SELECT
        request_id,
        email,
        AVG(weekly_revenue) AS average_weekly_revenue
    FROM weekly_revenues_deduplicated
    GROUP BY 1,2
)

,daily_expenses AS (
    SELECT
        request_id,
        email,
        ROUND(SUM(daily_debits) / (DATEDIFF('day', MIN(date), MAX(date)) + 1), 2) AS average_daily_expense
    FROM daily_aggregates
    GROUP BY 1,2
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
    most_recent_balance AS most_recent_balance_across_bank_accounts,

    -- Calculations
    SUM(dag.daily_revenue) * 2 AS estimated_annual_revenue,
    MAX(db.revised_average_balance) as average_daily_balance,
    MAX(drv.average_daily_revenue) as average_daily_revenue,
    MAX(dxp.average_daily_expense) as average_daily_expense,
    IF(SUM(dag.daily_revenue) * 2 > MAX(mrb.most_recent_balance), SUM(dag.daily_revenue) * 2, MAX(mrb.most_recent_balance)) AS smart_revenue,
    0 AS existing_debt_payments_consideration,
    MAX(wrv.average_weekly_revenue) AS average_weekly_revenue
FROM daily_aggregates AS dag
LEFT JOIN daily_balances AS db USING(request_id, email)
LEFT JOIN most_recent_balances AS mrb USING(request_id, email)
LEFT JOIN daily_revenues AS drv USING(request_id, email)
LEFT JOIN weekly_revenues AS wrv USING(request_id, email)
LEFT JOIN daily_expenses AS dxp USING(request_id, email)
GROUP BY ALL
