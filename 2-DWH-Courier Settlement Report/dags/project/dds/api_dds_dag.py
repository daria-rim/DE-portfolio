import logging

import pendulum
from airflow.decorators import dag, task
from project.dds.dm_couriers_loader import DmCourierLoader
from project.dds.dm_deliveries_loader import DmDeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2025, 8, 18, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'api', 'project'],
    is_paused_upon_creation=True
)
def sprint5_dds_api_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        rest_loader = DmCourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_couriers()

    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        rest_loader = DmDeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_deliveries()

    dm_couriers_dict = load_dm_couriers()
    dm_deliveries_dict = load_dm_deliveries()


    dm_couriers_dict >> dm_deliveries_dict

api_dds_dag = sprint5_dds_api_dag()
