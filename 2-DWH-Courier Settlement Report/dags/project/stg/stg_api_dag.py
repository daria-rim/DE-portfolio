import logging
from logging import Logger
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib import ConnectionBuilder
from psycopg import Connection
from typing import Any, Dict, List
from datetime import datetime, timedelta
import requests
from lib.dict_util import json2str


log = logging.getLogger(__name__)


class PgSaverBase:
    def __init__(self, pg_connect):
        self.pg_connect = pg_connect
        
    def _get_connection(self) -> Connection:
        return self.pg_connect.connection()


class CourierReader:
    def __init__(self, api_url: str, api_headers: Dict):
        self.api_url = api_url
        self.api_headers = api_headers

    def get_couriers(self, offset: int, limit: int) -> List[Dict]:
        params = {
            'sort_field': 'id',
            'sort_direction': 'asc',
            'limit': limit,
            'offset': offset
        }

        response = requests.get(
            f"{self.api_url}/couriers",
            headers=self.api_headers,
            params=params
        )
        response.raise_for_status()
        return response.json()



class DeliveryReader:
    def __init__(self, api_url: str, api_headers: Dict):
        self.api_url = api_url
        self.api_headers = api_headers

    def get_deliveries(self, offset: int, limit: int, date_from: str, date_to: str) -> List[Dict]:
        params = {
            'sort_field': 'id',
            'sort_direction': 'asc',
            'limit': limit,
            'offset': offset,
            'from': date_from,
            'to': date_to
        }
        
        response = requests.get(
            f"{self.api_url}/deliveries",
            headers=self.api_headers,
            params=params
        )
        response.raise_for_status()
        return response.json()


class PgSaverCourier(PgSaverBase):
    def save_object(self, id: str, val: Any):
        str_val = json2str(val)
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.get_couriers(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, NOW())
                    ON CONFLICT (object_id) DO UPDATE
                    SET 
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val
                    }
                )


class PgSaverDelivery(PgSaverBase):
    def save_object(self, id: str, val: Any):
        str_val = json2str(val)
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.get_deliveries(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, NOW())
                    ON CONFLICT (object_id) DO UPDATE
                    SET 
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val
                    }
                )

class CourierLoader:
    _LOG_THRESHOLD = 50
    _LIMIT = 50

    def __init__(self, reader, pg_saver: PgSaverCourier, logger: Logger):
        self.reader = reader
        self.pg_saver = pg_saver
        self.log = logger

    def run_copy(self) -> int:
        offset = 0
        total_processed = 0

        while True:
            couriers = self.reader.get_couriers(offset, self._LIMIT)
            if not couriers:
                break

            for courier in couriers:
                self.pg_saver.save_object(courier['_id'], courier)
                total_processed += 1

                if total_processed % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {total_processed} couriers")
            
            offset += len(couriers)

            if len(couriers) < self._LIMIT:
                break
        
        self.log.info(f"Total processed couriers: {total_processed}")
        return total_processed


class DeliveryLoader:
    _LOG_THRESHOLD = 50
    _LIMIT = 50

    def __init__(self, reader, pg_saver: PgSaverDelivery, logger: Logger):
        self.reader = reader
        self.pg_saver = pg_saver
        self.log = logger

    def run_copy(self) -> int:
        offset = 0
        total_processed = 0
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        date_to = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        while True:
            deliveries = self.reader.get_deliveries(offset, self._LIMIT, date_from, date_to)
            if not deliveries:
                break
            
            for delivery in deliveries:
                self.pg_saver.save_object(delivery['order_id'], delivery)
                total_processed += 1
                
                if total_processed % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {total_processed} deliveries")
            
            offset += len(deliveries)
            if len(deliveries) < self._LIMIT:
                break
        
        self.log.info(f"Total processed deliveries: {total_processed}")
        return total_processed


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'api'],
    is_paused_upon_creation=True
)
def sprint5_stg_api_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    API_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    API_HEADERS = {
        'X-Nickname': 'DariaRim',
        'X-Cohort': '7',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
    }

    @task()
    def load_couriers():
        reader = CourierReader(API_URL, API_HEADERS)
        pg_saver = PgSaverCourier(dwh_pg_connect)
        loader = CourierLoader(reader, pg_saver, log)
        return loader.run_copy()

    @task()
    def load_deliveries():
        reader = DeliveryReader(API_URL, API_HEADERS)
        pg_saver = PgSaverDelivery(dwh_pg_connect)
        loader = DeliveryLoader(reader, pg_saver, log)
        return loader.run_copy()

    courier_loader = load_couriers()
    delivery_loader = load_deliveries()

    courier_loader, delivery_loader

stg_api_dag = sprint5_stg_api_dag()
