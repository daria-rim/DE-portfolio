from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmCourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class DmCouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_couriers(self, dm_courier_threshold: int, limit: int) -> List[DmCourierObj]:
        with self._db.client().cursor(row_factory=class_row(DmCourierObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        object_value::json->>'_id' as courier_id,
                        object_value::json->>'name' as courier_name
                    FROM stg.get_couriers
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": dm_courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmCourierDestRepository:

    def insert_dm_courier(self, conn: Connection, dm_courier: DmCourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "courier_id": dm_courier.courier_id,
                    "courier_name": dm_courier.courier_name
                },
            )


class DmCourierLoader:
    WF_KEY = "dm_couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmCouriersOriginRepository(pg_origin)
        self.stg = DmCourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_couriers(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dm_courier in load_queue:
                self.stg.insert_dm_courier(conn, dm_courier)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
