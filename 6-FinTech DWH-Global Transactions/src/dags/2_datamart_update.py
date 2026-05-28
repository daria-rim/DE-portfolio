import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task

from utils.vertica_connect import VerticaConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id="datamart_update_dag",
    schedule_interval="0 3 * * *",
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    tags=["project", "dwh", "datamart", "global_metrics"],
    is_paused_upon_creation=True,
    description="Обновление витрины global_metrics за каждый день",
)
def datamart_update_dag():

    @task
    def update_global_metrics(**context):
        logical_date = context["logical_date"]
        target_date = (logical_date - timedelta(days=1)).date()
        log.info(f"Обновление витрины за дату: {target_date}")

        vertica_conn = VerticaConnectionBuilder.vertica_conn("vertica_dwh")
        
        with vertica_conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
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
                        WHERE DATE(transaction_dt) = %s
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
                            ad.total_amount * COALESCE(c.currency_with_div, 1) / 100, 
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
                    FROM aggregated_data ad
                    LEFT JOIN VT251109CA442B__STAGING.currencies c
                        ON c.currency_code = ad.currency_code
                        AND c.currency_code_with = 420
                        AND DATE(c.date_update) = ad.transaction_date;
                """, (target_date,))
                
                log.info(f"Успешно вставлено строк: {cur.rowcount}")
                return cur.rowcount

    update_global_metrics()


datamart_update_dag()