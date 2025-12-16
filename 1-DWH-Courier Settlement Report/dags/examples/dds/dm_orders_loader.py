from logging import Logger
from typing import List, Dict, Any
from datetime import datetime

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmOrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    delivery_id: int


class DmOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_orders(self, dm_order_threshold: int, limit: int) -> List[DmOrderObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrderObj)) as cur:
            cur.execute(
                """
                WITH order_data AS (
                    SELECT
                        id,
                        object_id as order_key,
                        object_value::json->>'final_status' as order_status,
                        (object_value::json->'restaurant'->>'id') as restaurant_id_str,
                        (object_value::json->>'date')::timestamp as timestamp,
                        (object_value::json->'user'->>'id') as user_id_str
                    FROM stg.ordersystem_orders
                    WHERE object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                    ORDER BY id ASC
                ),
                delivery_data AS (
                    SELECT
                        od.id,
                        od.order_key,
                        od.order_status,
                        od.restaurant_id_str,
                        od.timestamp,
                        od.user_id_str,
                        object_value::json->>'delivery_id' as delivery_id_str
                    FROM stg.get_deliveries gd
                    INNER JOIN order_data od ON od.order_key = (gd.object_value::json->>'order_id')
                    ORDER BY id ASC
                )
                SELECT
                    dd.id,
                    dd.order_key,
                    dd.order_status,
                    r.id as restaurant_id,
                    t.id as timestamp_id,
                    u.id as user_id,
                    COALESCE(d.id, 0) as delivery_id
                FROM delivery_data dd
                INNER JOIN dds.dm_restaurants r ON dd.restaurant_id_str = r.restaurant_id
                INNER JOIN dds.dm_timestamps t ON dd.timestamp = t.ts
                INNER JOIN dds.dm_users u ON dd.user_id_str = u.user_id
                INNER JOIN dds.dm_deliveries d ON dd.delivery_id_str = d.delivery_id
                WHERE dd.id > %(threshold)s
                ORDER BY dd.id ASC
                LIMIT %(limit)s                    
                """,
                {
                    "threshold": dm_order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            return objs


class DmOrderDestRepository:
    def insert_dm_order(self, conn: Connection, dm_order: DmOrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(
                        order_key,
                        order_status,
                        restaurant_id,
                        timestamp_id,
                        user_id,
                        delivery_id
                    )
                    VALUES (
                        %(order_key)s,
                        %(order_status)s,
                        %(restaurant_id)s,
                        %(timestamp_id)s,
                        %(user_id)s,
                        %(delivery_id)s
                    )
                    ON CONFLICT (order_key) DO UPDATE
                    SET
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id,
                        delivery_id = EXCLUDED.delivery_id;
                """,
                {
                    "order_key": dm_order.order_key,
                    "order_status": dm_order.order_status,
                    "restaurant_id": dm_order.restaurant_id,
                    "timestamp_id": dm_order.timestamp_id,
                    "user_id": dm_order.user_id,
                    "delivery_id": dm_order.delivery_id
                },
            )


class DmOrderLoader:
    WF_KEY = "dm_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrdersOriginRepository(pg_origin)
        self.stg = DmOrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_orders(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dm_order in load_queue:
                self.stg.insert_dm_order(conn, dm_order)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
