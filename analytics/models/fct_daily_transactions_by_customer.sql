-- This model creates a complete, daily time series for each customer over the
-- last 180 days, filling in any missing dates with the last known balance.

WITH all_transactions AS (
    SELECT * FROM {{ ref('all_transactions_by_customer') }}
)

,filtered_daily_transactions AS (
    SELECT
        username,
        email,
        request_id,
        request_datetime,
        date,
        withdrawals,
        deposits,
        balance,
        most_recent_statement_date_minus_180_days AS start_date,
        most_recent_statement_date AS end_date
    FROM all_transactions
    WHERE date >= most_recent_statement_date_minus_180_days AND date <= most_recent_statement_date
)

,dim_calendar AS (
    SELECT *
    FROM {{ ref('dim_calendar') }}
)

,customer_date_range AS (
    SELECT
        email,
        request_id,
        start_date,
        end_date
    FROM filtered_daily_transactions
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
        trn.withdrawals,
        trn.deposits,
        trn.balance,
        ROUND(AVG(trn.balance) OVER(PARTITION BY scf.email, scf.request_id, scf.date), 2) AS average_balance
    FROM customer_scaffold AS scf
    LEFT JOIN filtered_daily_transactions AS trn ON scf.email = trn.email
        AND scf.request_id = trn.request_id
        AND scf.date = trn.date
/*        WHERE scf.date >= '2024-02-02' AND scf.date <= '2024-02-05'*/
)

,daily_balances AS (
    SELECT
        email,
        request_id,
        date,
        LAST_VALUE(average_balance IGNORE NULLS) OVER(
            PARTITION BY email, request_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS revised_average_balance
    FROM padded_transactions
)

SELECT *
FROM daily_balances
GROUP BY ALL
ORDER BY date DESC
