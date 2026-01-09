-- Обновление витрины global_metrics
INSERT INTO VT251109CA442B__DWH.global_metrics (
    date_update,
    currency_from, 
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions,
    load_dt
)
WITH 
daily_transactions AS (
    SELECT 
        DATE(transaction_dt) AS transaction_date,
        currency_code,
        account_number_from,
        amount
    FROM VT251109CA442B__STAGING.transactions
    WHERE DATE(transaction_dt) = CURRENT_DATE - 1
      AND status = 'done'
),

aggregated_data AS (
    SELECT 
        transaction_date,
        currency_code,
        COUNT(*) as cnt_txn,
        COUNT(DISTINCT account_number_from) as cnt_accounts,
        SUM(amount) as total_amount
    FROM daily_transactions
    GROUP BY transaction_date, currency_code
)

SELECT 
    ad.transaction_date as date_update,
    ad.currency_code as currency_from,
    ROUND(
        ad.total_amount * COALESCE(
            (SELECT currency_with_div 
             FROM VT251109CA442B__STAGING.currencies 
             WHERE currency_code = ad.currency_code 
               AND currency_code_with = 420  -- USD
               AND DATE(date_update) = ad.transaction_date
             LIMIT 1),
            1
        ) / 100, 
        2
    ) as amount_total,
    ad.cnt_txn as cnt_transactions,
    ROUND(
	    CASE 
	        WHEN ad.cnt_accounts = 0 THEN NULL 
	        ELSE ad.cnt_txn * 1.0 / ad.cnt_accounts 
	    END, 
	    2
	) as avg_transactions_per_account,
    ad.cnt_accounts as cnt_accounts_make_transactions,
    CURRENT_TIMESTAMP as load_dt
FROM aggregated_data ad;