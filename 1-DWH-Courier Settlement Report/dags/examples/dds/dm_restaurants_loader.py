from logging import Logger
from typing import List

from datetime import datetime
from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmRestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class DmRestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_restaurants(self, dm_restaurant_threshold: int, limit: int) -> List[DmRestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(DmRestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT
                        r.id,
                        r.object_value::json->>'_id' as restaurant_id,
                        r.object_value::json->>'name' as restaurant_name,
                        r.update_ts as active_from,
                        '2099-12-31 00:00:00'::timestamp as active_to
                    FROM stg.ordersystem_restaurants r
                    WHERE r.id > %(threshold)s
                    ORDER BY r.id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": dm_restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmRestaurantDestRepository:

    def insert_dm_restaurant(self, conn: Connection, dm_restaurant: DmRestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(
                        restaurant_id, 
                        restaurant_name, 
                        active_from, 
                        active_to
                    )
                    VALUES (
                        %(restaurant_id)s, 
                        %(restaurant_name)s, 
                        %(active_from)s, 
                        %(active_to)s
                    )
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "restaurant_id": dm_restaurant.restaurant_id,
                    "restaurant_name": dm_restaurant.restaurant_name,
                    "active_from": dm_restaurant.active_from,
                    "active_to": dm_restaurant.active_to
                },
            )


class DmRestaurantLoader:
    WF_KEY = "dm_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmRestaurantsOriginRepository(pg_origin)
        self.stg = DmRestaurantDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_restaurants(self):
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
            load_queue = self.origin.list_dm_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_restaurant in load_queue:
                self.stg.insert_dm_restaurant(conn, dm_restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
