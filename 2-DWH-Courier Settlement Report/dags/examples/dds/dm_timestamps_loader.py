from logging import Logger
from typing import List

from datetime import datetime, date, time
from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmTimestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class DmTimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_timestamps(self, dm_timestamp_threshold: int, limit: int) -> List[DmTimestampObj]:
        with self._db.client().cursor(row_factory=class_row(DmTimestampObj)) as cur:
            cur.execute(
                """
                    SELECT
                        o.id,
                        (o.object_value::json->>'date')::timestamp as ts,
                        EXTRACT(YEAR FROM (o.object_value::json->>'date')::timestamp)::int as year,
                        EXTRACT(MONTH FROM (o.object_value::json->>'date')::timestamp)::int as month,
                        EXTRACT(DAY FROM (o.object_value::json->>'date')::timestamp)::int as day,
                        (o.object_value::json->>'date')::date as date,
                        TO_CHAR((o.object_value::json->>'date')::timestamp, 'HH24:MI:SS') as time
                    FROM stg.ordersystem_orders o
                    WHERE o.id > %(threshold)s
                    AND o.object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                    ORDER BY o.id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": dm_timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmTimestampDestRepository:

    def insert_dm_timestamp(self, conn: Connection, dm_timestamp: DmTimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(
                        ts,
                        year,
                        month,
                        day,
                        date,
                        time
                    )
                    VALUES (
                        %(ts)s,
                        %(year)s,
                        %(month)s,
                        %(day)s,
                        %(date)s,
                        %(time)s
                    )
                    ON CONFLICT (ts) DO UPDATE
                    SET
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                """,
                {
                    "ts": dm_timestamp.ts,
                    "year": dm_timestamp.year,
                    "month": dm_timestamp.month,
                    "day": dm_timestamp.day,
                    "date": dm_timestamp.date,
                    "time": dm_timestamp.time
                },
            )


class DmTimestampLoader:
    WF_KEY = "dm_timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmTimestampsOriginRepository(pg_origin)
        self.stg = DmTimestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_timestamps(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_timestamp in load_queue:
                self.stg.insert_dm_timestamp(conn, dm_timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
