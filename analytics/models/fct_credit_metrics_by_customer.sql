{{ config(materialized='table') }}

WITH daily_aggregates AS (
    -- First, aggregate the raw transactions to a daily level.
    SELECT
        request_id,
        email,
        date,
        -- Use the date calculation columns from the source table
        most_recent_statement_date,
        most_recent_statement_date_minus_90d,
        most_recent_statement_date_minus_180d,
        most_recent_statement_date_minus_365d,
        SUM(IF(is_revenue, deposits, 0)) as daily_revenue,
        SUM(IF(is_debit, withdrawals, 0)) as daily_debits
    FROM {{ ref('int_transactions_enriched') }}
    GROUP BY ALL
)

,average_daily_balance_180d AS (
    SELECT
        request_id,
        email,
        ROUND(AVG(revised_average_balance), 2) AS average_daily_balance_180d
    FROM {{ ref('fct_daily_transactions_by_customer') }}
    WHERE date > most_recent_statement_date_minus_180d
    GROUP BY ALL
)

,most_recent_balances AS (
    SELECT
        request_id,
        email,
        balance AS most_recent_balance
    FROM {{ ref('int_transactions_enriched') }}
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

,weekly_revenues AS (
    SELECT
        request_id,
        email,
        ROUND(AVG(weekly_revenue), 2) AS average_weekly_revenue
    FROM (
        SELECT DISTINCT
            request_id,
            email,
            WEEKOFYEAR(date) AS week_of_year,
            weekly_revenue
        FROM {{ ref('fct_daily_transactions_by_customer') }}
        WHERE weekly_revenue IS NOT NULL
    ) AS deduped_weekly_revenues
    GROUP BY 1, 2
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
    SUM(IF(date >= most_recent_statement_date_minus_90d, daily_revenue, 0)) AS revenue_recent_90d,
    SUM(IF(date >= most_recent_statement_date_minus_180d AND date < most_recent_statement_date_minus_90d, daily_revenue, 0)) AS revenue_91_to_180d,

    -- Debit Metrics
    SUM(daily_debits) AS debits_total,
    SUM(IF(date >= most_recent_statement_date_minus_90d, daily_debits, 0)) AS debits_recent_90d,
    SUM(IF(date >= most_recent_statement_date_minus_180d AND date < most_recent_statement_date_minus_90d, daily_debits, 0)) AS debits_91_to_180d,

    -- Placeholder Credit Metrics
    0 AS credit_card_payments,
    0 AS credit_card_recent_90d,
    0 AS credit_card_91_to_180d,

    -- Averages and Balances
    adb.average_daily_balance_180d AS average_daily_balance_across_bank_accounts,
    mrb.most_recent_balance AS most_recent_balance_across_bank_accounts,

    -- Calculations
    SUM(dag.daily_revenue) * 2 AS estimated_annual_revenue,
    MAX(adb.average_daily_balance_180d) as average_daily_balance,
    MAX(drv.average_daily_revenue) as average_daily_revenue,
    MAX(dxp.average_daily_expense) as average_daily_expense,
    IF(SUM(dag.daily_revenue) * 2 > MAX(mrb.most_recent_balance), SUM(dag.daily_revenue) * 2, MAX(mrb.most_recent_balance)) AS smart_revenue,
    0 AS existing_debt_payments_consideration,
    MAX(wrv.average_weekly_revenue) AS average_weekly_revenue
FROM daily_aggregates AS dag
LEFT JOIN average_daily_balance_180d AS adb USING(request_id, email)
LEFT JOIN most_recent_balances AS mrb USING(request_id, email)
LEFT JOIN daily_revenues AS drv USING(request_id, email)
LEFT JOIN weekly_revenues AS wrv USING(request_id, email)
LEFT JOIN daily_expenses AS dxp USING(request_id, email)
GROUP BY ALL
