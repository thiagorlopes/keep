{{ config(materialized='table') }}

-- This model creates a complete, daily time series for each customer over the
-- last 180 days, filling in any missing dates with the last known balance.
    
WITH all_daily_transactions AS (
    SELECT
        username,
        email,
        request_id,
        request_datetime,
        date,
        withdrawals,
        deposits,
        balance,
        most_recent_statement_date,
        most_recent_statement_date_minus_30_days,
        most_recent_statement_date_minus_60_days,
        most_recent_statement_date_minus_90_days,
        most_recent_statement_date_minus_180_days,
        most_recent_statement_date_minus_365_days
    FROM {{ ref('all_transactions_by_customer')}}
)

,dim_calendar AS (
    SELECT *
    FROM main.dim_calendar
)

,customer_date_range AS (
    SELECT
        email,
        request_id,
        
        -- Use 180 days as default for scaffolding, but this can be easily changed
        most_recent_statement_date_minus_180_days AS start_date,
        most_recent_statement_date AS end_date,
        
        -- Include all auxiliary date columns for reference
        most_recent_statement_date,
        most_recent_statement_date_minus_30_days,
        most_recent_statement_date_minus_60_days,
        most_recent_statement_date_minus_90_days,
        most_recent_statement_date_minus_180_days,
        most_recent_statement_date_minus_365_days
    FROM {{ref('all_transactions_by_customer')}}
    GROUP BY ALL
)

,customer_scaffold AS (
    SELECT
        cdr.email,
        cdr.request_id,
        cal.date_day as date
    FROM customer_date_range AS cdr
    CROSS JOIN dim_calendar AS cal
    WHERE cal.date_day > cdr.start_date
        AND cal.date_day <= cdr.end_date
    ORDER By date DESC
)

,padded_transactions AS (
    SELECT
        scf.email,
        scf.request_id,
        scf.date,
        -- Average balance for days with multiple transactions
        ROUND(AVG(trn.balance) OVER(PARTITION BY scf.email, scf.request_id, scf.date), 2) AS average_balance        FROM customer_scaffold AS scf
    LEFT JOIN all_daily_transactions AS trn ON scf.email = trn.email
        AND scf.request_id = trn.request_id
        AND scf.date = trn.date
)

,daily_balances AS (
    SELECT
        email,
        request_id,
        date,
        -- Fill forward the last known balance for days without transactions
        LAST_VALUE(average_balance IGNORE NULLS) OVER(
            PARTITION BY email, request_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS revised_average_balance
    FROM padded_transactions
)
    
,daily_revenue AS (
    SELECT
        email,
        request_id,
        date,
        -- Sum of all withdrawals for the day. This is the "Day Rev" from the sheet.
        -- For now, all withdrawals are considered debits. A CASE statement could add more complex logic.
        SUM(deposits) AS daily_revenue
    FROM all_daily_transactions
    GROUP BY ALL
)

,final_model AS (
    SELECT
        db.email,
        db.request_id,
        db.date,
        db.revised_average_balance,
        COALESCE(dr.daily_revenue, 0) as daily_revenue,
        -- Include auxiliary date columns for reference
        cdr.most_recent_statement_date,
        cdr.most_recent_statement_date_minus_30_days,
        cdr.most_recent_statement_date_minus_60_days,
        cdr.most_recent_statement_date_minus_90_days,
        cdr.most_recent_statement_date_minus_180_days,
        cdr.most_recent_statement_date_minus_365_days
    FROM daily_balances AS db
    LEFT JOIN daily_revenue AS dr ON db.email = dr.email
        AND db.request_id = dr.request_id
        AND db.date = dr.date
    LEFT JOIN customer_date_range AS cdr ON db.email = cdr.email
        AND db.request_id = cdr.request_id
    WHERE db.date > cdr.most_recent_statement_date_minus_180_days
    GROUP BY ALL
)

SELECT
    fm.email,
    fm.request_id,
    fm.date,
    ROUND(fm.revised_average_balance, 2) as revised_average_balance,
    fm.daily_revenue,
    -- Calculate weekly revenue based on the daily revenues using calendar week grouping
    SUM(fm.daily_revenue) OVER (
        PARTITION BY fm.email, fm.request_id, cal.week_of_year, cal.year_number
    ) as weekly_revenue,
    cal.week_of_year as week_number,
    cal.day_of_year,
    -- Auxiliary date columns for flexible analysis
    fm.most_recent_statement_date,
    fm.most_recent_statement_date_minus_30_days,
    fm.most_recent_statement_date_minus_60_days,
    fm.most_recent_statement_date_minus_90_days,
    fm.most_recent_statement_date_minus_180_days,
    fm.most_recent_statement_date_minus_365_days
FROM final_model fm
LEFT JOIN dim_calendar cal ON fm.date = cal.date_day
ORDER BY fm.date DESC
