-- Создание таблицы для транзакций в STAGING
DROP TABLE IF EXISTS VT251109CA442B__STAGING.transactions;
CREATE TABLE IF NOT EXISTS VT251109CA442B__STAGING.transactions (
    operation_id UUID,
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(30),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP(3)
)
PARTITION BY (transaction_dt::DATE)
GROUP BY calendar_hierarchy_day(transaction_dt::DATE, 3, 2);

-- Создание проекции для transactions
CREATE PROJECTION VT251109CA442B__STAGING.transactions_by_date /*+createtype(A)*/
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
)
AS
    SELECT
        t.operation_id,
        t.account_number_from,
        t.account_number_to,
        t.currency_code,
        t.country,
        t.status,
        t.transaction_type,
        t.amount,
        t.transaction_dt
    FROM VT251109CA442B__STAGING.transactions t
    ORDER BY
        t.transaction_dt,
        t.operation_id
    SEGMENTED BY
        HASH(t.transaction_dt, t.operation_id) ALL NODES
    KSAFE 1;

-- Создание таблицы для курсов валют в STAGING
DROP TABLE IF EXISTS VT251109CA442B__STAGING.currencies;
CREATE TABLE IF NOT EXISTS VT251109CA442B__STAGING.currencies (
    date_update TIMESTAMP(3),
    currency_code INT,
    currency_code_with INT,
    currency_with_div NUMERIC(5,3)
)
PARTITION BY (date_update::DATE)
GROUP BY calendar_hierarchy_day(date_update::DATE, 3, 2);

-- Создание проекции для currencies
CREATE PROJECTION VT251109CA442B__STAGING.currencies_by_date
(
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
AS
    SELECT
        date_update,
        currency_code,
        currency_code_with,
        currency_with_div
    FROM VT251109CA442B__STAGING.currencies
    ORDER BY
        date_update,
        currency_code
    SEGMENTED BY
        HASH(date_update, currency_code) ALL NODES
    KSAFE 1;

-- Создание таблицы для настроек в Vertica
DROP TABLE IF EXISTS VT251109CA442B__STAGING.srv_wf_settings cascade;
CREATE TABLE IF NOT EXISTS VT251109CA442B__STAGING.srv_wf_settings (
    workflow_key VARCHAR(100) PRIMARY KEY,
    workflow_settings VARCHAR(10000)
);

-- Создание проекции для таблицы настроек
CREATE PROJECTION VT251109CA442B__STAGING.srv_wf_settings_super
(
    workflow_key,
    workflow_settings
)
AS
SELECT
    workflow_key, 
    workflow_settings
FROM VT251109CA442B__STAGING.srv_wf_settings
ORDER BY workflow_key
SEGMENTED BY HASH(workflow_key) ALL NODES;