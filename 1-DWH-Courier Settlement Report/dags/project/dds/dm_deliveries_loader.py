from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmDeliveryObj(BaseModel):
    id: int
    delivery_id: str
    courier_id: int
    rate: int
    tip_sum: float



class DmDeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_deliveries(self, dm_delivery_threshold: int, limit: int) -> List[DmDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveryObj)) as cur:
            cur.execute(
                """
                WITH delivery_data AS (
                    SELECT 
                        id,
                        object_value::json->>'delivery_id' as delivery_id,
                        object_value::json->>'rate' as rate,
                        object_value::json->>'tip_sum' as tip_sum,
                        object_value::json->>'courier_id' as courier_id
                    FROM stg.get_deliveries
                )
                SELECT
                    d.id,
                    d.delivery_id,
                    c.id as courier_id,
                    d.rate,
                    d.tip_sum,
                    c.id as courier_id
                FROM delivery_data d
                INNER JOIN dds.dm_couriers c ON d.courier_id = c.courier_id
                WHERE d.id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s
                """, {
                    "threshold": dm_delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmDeliveryDestRepository:

    def insert_dm_delivery(self, conn: Connection, dm_delivery: DmDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, courier_id, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "delivery_id": dm_delivery.delivery_id,
                    "courier_id": dm_delivery.courier_id,
                    "rate": dm_delivery.rate,
                    "tip_sum": dm_delivery.tip_sum
                },
            )


class DmDeliveryLoader:
    WF_KEY = "dm_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliveriesOriginRepository(pg_origin)
        self.stg = DmDeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_deliveries(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dm_delivery in load_queue:
                self.stg.insert_dm_delivery(conn, dm_delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
