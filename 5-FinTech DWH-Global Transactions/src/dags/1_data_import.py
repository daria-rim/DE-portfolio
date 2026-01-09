"""
Инкрементальная загрузка транзакций и курсов валют из PostgreSQL в Vertica (STG).
"""

import logging
from airflow.decorators import dag, task
from datetime import datetime

from utils.data_loaders import (
    PostgresReader,
    VerticaSaverTransaction,
    VerticaSaverCurrency,
    TransactionsLoader,
    CurrenciesLoader,
)

log = logging.getLogger(__name__)


@dag(
    dag_id="data_import_dag",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["project", "stg", "vertica", "incremental"],
    is_paused_upon_creation=True,
    description="Инкрементальная загрузка в STG: Postgres → Vertica",
)
def data_import_dag():

    @task
    def load_transactions():
        pg_reader = PostgresReader(pg_conn_id="postgres_source")
        vertica_saver = VerticaSaverTransaction(vertica_conn_id="vertica_dwh")
        loader = TransactionsLoader(pg_reader, vertica_saver, log)
        rows = loader.run_copy()
        log.info(f"Загружено транзакций: {rows}")
        return rows

    @task
    def load_currencies():
        pg_reader = PostgresReader(pg_conn_id="postgres_source")
        vertica_saver = VerticaSaverCurrency(vertica_conn_id="vertica_dwh")
        loader = CurrenciesLoader(pg_reader, vertica_saver, log)
        rows = loader.run_copy()
        log.info(f"Загружено курсов: {rows}")
        return rows

    load_transactions()
    load_currencies()


data_import_dag()