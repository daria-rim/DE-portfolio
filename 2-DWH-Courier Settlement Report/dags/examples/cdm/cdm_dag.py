import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.cdm.cdm_loader import SettlementReportLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 1 * * *',  # Ежедневно в 1:00
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    tags=['cdm', 'settlement_report'],
    is_paused_upon_creation=False
)
def cdm_settlement_report_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_settlement_report")
    def load_settlement_report():
        loader = SettlementReportLoader(
            pg_origin=dwh_pg_connect,
            pg_dest=dwh_pg_connect,
            log=log
        )
        loader.load_dm_settlement_report()

    load_settlement_report()

cdm_settlement_report_dag = cdm_settlement_report_dag()