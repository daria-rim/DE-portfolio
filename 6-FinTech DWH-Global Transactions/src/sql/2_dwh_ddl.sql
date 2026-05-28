-- Создание таблицы витрины global_metrics
DROP TABLE IF EXISTS VT251109CA442B__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS VT251109CA442B__DWH.global_metrics (
    date_update DATE,
    currency_from INT,
    amount_total NUMERIC(15,2),
    cnt_transactions INT,
    avg_transactions_per_account NUMERIC(10,2),
    cnt_accounts_make_transactions INT,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PARTITION BY date_update;

-- Создание проекции для витрины global_metrics
CREATE PROJECTION VT251109CA442B__DWH.global_metrics_super
(
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions,
    load_dt
  )
AS
SELECT 
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions,
    load_dt
FROM VT251109CA442B__DWH.global_metrics
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;