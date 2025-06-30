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
        is_revenue,
        is_debit,
        most_recent_statement_date,
        most_recent_statement_date_minus_30_days,
        most_recent_statement_date_minus_60_days,
        most_recent_statement_date_minus_90_days,
        most_recent_statement_date_minus_180_days,
        most_recent_statement_date_minus_365_days
    FROM main.all_transactions_by_customer
)

,dim_calendar AS (
    SELECT *
    FROM main.dim_calendar
)

,customer_date_range AS (
    SELECT
        email,
        request_id,
        date,
        
        -- Use 180 days as default for scaffolding, but this can be easily changed
        most_recent_statement_date_minus_365_days AS start_date,
        most_recent_statement_date AS end_date,
        
        -- Include all auxiliary date columns for reference
        most_recent_statement_date,
        most_recent_statement_date_minus_30_days,
        most_recent_statement_date_minus_60_days,
        most_recent_statement_date_minus_90_days,
        most_recent_statement_date_minus_180_days,
        most_recent_statement_date_minus_365_days
    FROM all_daily_transactions
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
        AVG(trn.balance) AS average_balance
    FROM customer_scaffold AS scf
    LEFT JOIN all_daily_transactions AS trn ON scf.email = trn.email
        AND scf.request_id = trn.request_id
        AND scf.date = trn.date
    GROUP BY ALL
)

,daily_balances AS (
    SELECT
        email,
        request_id,
        date,
        average_balance,
        
        -- Fill forward the last known balance for days without transactions
        LAST_VALUE(average_balance IGNORE NULLS) OVER(
            PARTITION BY email, request_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS revised_average_balance
    FROM padded_transactions AS trn
)

,daily_revenue AS (
    SELECT
        email,
        request_id,
        date,
        
        -- Sum of all deposits for the day. This is the "Day Rev" from the sheet.
        SUM(deposits) OVER(PARTITION BY email, request_id, DAYOFYEAR(date)) AS daily_revenue,
    FROM all_daily_transactions AS trn
    WHERE is_revenue
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email, request_id, date) = 1
)

,weekly_revenue AS (
    SELECT
        email,
        request_id,
        date,
        
        -- Sum of all deposits for the day. This is the "Weekly revenue" from the sheet.
        SUM(deposits) OVER(PARTITION BY email, request_id, WEEKOFYEAR(date)) AS weekly_revenue,
    FROM all_daily_transactions
    WHERE is_revenue
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email, request_id, date) = 1
)

,customer_daily_metrics AS (
    SELECT
        db.email,
        db.request_id,
        db.date,
        ROUND(db.revised_average_balance, 2) AS revised_average_balance,

        -- Daily and weekly revenues
        drv.daily_revenue,
        wrv.weekly_revenue,
        
        -- Include auxiliary date columns for reference
        ANY_VALUE(cdr.most_recent_statement_date) OVER (PARTITION BY db.email, db.request_id) AS most_recent_statement_date,
        ANY_VALUE(cdr.most_recent_statement_date_minus_30_days) OVER (PARTITION BY db.email, db.request_id) AS most_recent_statement_date_minus_30_days,
        ANY_VALUE(cdr.most_recent_statement_date_minus_60_days) OVER (PARTITION BY db.email, db.request_id) AS most_recent_statement_date_minus_60_days,
        ANY_VALUE(cdr.most_recent_statement_date_minus_90_days) OVER (PARTITION BY db.email, db.request_id) AS most_recent_statement_date_minus_90_days,
        ANY_VALUE(cdr.most_recent_statement_date_minus_180_days) OVER (PARTITION BY db.email, db.request_id) AS most_recent_statement_date_minus_180_days,
        ANY_VALUE(cdr.most_recent_statement_date_minus_365_days) OVER (PARTITION BY db.email, db.request_id) AS most_recent_statement_date_minus_365_days

    FROM daily_balances AS db
    LEFT JOIN daily_revenue AS drv USING(email, request_id, date)
    LEFT JOIN weekly_revenue AS wrv USING(email, request_id, date)
    LEFT JOIN customer_date_range AS cdr USING(email, request_id, date)
)

SELECT *
FROM customer_daily_metrics
