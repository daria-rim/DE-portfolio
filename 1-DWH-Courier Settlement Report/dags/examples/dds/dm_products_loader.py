from logging import Logger
from typing import List, Dict, Any
from datetime import datetime

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmProductObj(BaseModel):
    id: int
    restaurant_id: str
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime


class DmProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_products(self, dm_product_threshold: int, limit: int) -> List[DmProductObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductObj)) as cur:
            cur.execute(
                """
                    WITH restaurant_data AS (
                        SELECT 
                            r.id,
                            r.object_value::json->>'_id' as restaurant_id,
                            r.update_ts as active_from,
                            json_array_elements(r.object_value::json->'menu') as product_data
                        FROM stg.ordersystem_restaurants r
                        WHERE r.id > %(threshold)s
                        ORDER BY r.id ASC
                        LIMIT %(limit)s
                    )
                    SELECT
                        rd.id,
                        rd.restaurant_id,
                        rd.product_data->>'_id' as product_id,
                        rd.product_data->>'name' as product_name,
                        (rd.product_data->>'price')::numeric as product_price,
                        rd.active_from,
                        '2099-12-31 00:00:00'::timestamp as active_to
                    FROM restaurant_data rd;
                """,
                {
                    "threshold": dm_product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmProductDestRepository:
    def insert_dm_product(self, conn: Connection, dm_product: DmProductObj, restaurant_id: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(
                        product_id,
                        product_name,
                        product_price,
                        active_from,
                        active_to,
                        restaurant_id
                    )
                    VALUES (
                        %(product_id)s,
                        %(product_name)s,
                        %(product_price)s,
                        %(active_from)s,
                        %(active_to)s,
                        %(restaurant_id)s
                    )
                    ON CONFLICT (product_id) DO UPDATE
                    SET
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to,
                        restaurant_id = EXCLUDED.restaurant_id;
                """,
                {
                    "product_id": dm_product.product_id,
                    "product_name": dm_product.product_name,
                    "product_price": dm_product.product_price,
                    "active_from": dm_product.active_from,
                    "active_to": dm_product.active_to,
                    "restaurant_id": restaurant_id
                },
            )


class DmProductLoader:
    WF_KEY = "dm_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmProductsOriginRepository(pg_origin)
        self.stg = DmProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_products(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Получаем соответствие restaurant_id из stg и id в dds
            restaurant_map = self._get_restaurant_map(conn)

            for dm_product in load_queue:
                if dm_product.restaurant_id in restaurant_map:
                    self.stg.insert_dm_product(conn, dm_product, restaurant_map[dm_product.restaurant_id])

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

    def _get_restaurant_map(self, conn: Connection) -> Dict[str, int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT 
                        restaurant_id,
                        id
                    FROM dds.dm_restaurants;
                """
            )
            return {row[0]: row[1] for row in cur.fetchall()}