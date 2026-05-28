import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from project.cdm.api_cdm_loader import CourierLedgerLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 1 * * *',
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    tags=['cdm', 'courier_ledger', 'project'],
    is_paused_upon_creation=False
)
def cdm_courier_ledger_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_courier_ledger")
    def load_courier_ledger():
        rest_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_courier_ledger()

    load_courier_ledger()

api_cdm_dag = cdm_courier_ledger_dag()