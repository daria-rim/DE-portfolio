from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctProductSaleObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class FctProductSalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_product_sales(self, fct_product_sale_threshold: int, limit: int) -> List[FctProductSaleObj]:
        with self._db.client().cursor(row_factory=class_row(FctProductSaleObj)) as cur:
            cur.execute(
                """
                WITH event_data AS (
                    SELECT 
                        id as event_id,
                        (event_value::json->>'order_id') AS order_key,
                        jsonb_array_elements(event_value::jsonb->'product_payments') AS product_data
                    FROM stg.bonussystem_events
                    WHERE event_type = 'bonus_transaction'
                ),
                list_fct_ps AS (
                    SELECT 
                        ed.event_id AS id,
                        p.id AS product_id,
                        o.id AS order_id,
                        (ed.product_data->>'quantity')::int AS count,
                        (ed.product_data->>'price')::numeric(14,2) AS price,
                        (ed.product_data->>'product_cost')::numeric(14,2) AS total_sum,
                        (ed.product_data->>'bonus_payment')::numeric(14,2) AS bonus_payment,
                        (ed.product_data->>'bonus_grant')::numeric(14,2) AS bonus_grant
                    FROM event_data ed
                    JOIN dds.dm_orders o ON ed.order_key = o.order_key
                    JOIN dds.dm_products p ON (ed.product_data->>'product_id') = p.product_id
                )
                SELECT * FROM list_fct_ps
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s
                """,
                {
                    "threshold": fct_product_sale_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctProductSaleDestRepository:
    def insert_fct_product_sale(self, conn: Connection, fct_sale: FctProductSaleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales(
                    product_id,
                    order_id,
                    count,
                    price,
                    total_sum,
                    bonus_payment,
                    bonus_grant
                )
                VALUES (
                    %(product_id)s,
                    %(order_id)s,
                    %(count)s,
                    %(price)s,
                    %(total_sum)s,
                    %(bonus_payment)s,
                    %(bonus_grant)s
                )
                ON CONFLICT (product_id, order_id) DO UPDATE
                SET
                    count = EXCLUDED.count,
                    price = EXCLUDED.price,
                    total_sum = EXCLUDED.total_sum,
                    bonus_payment = EXCLUDED.bonus_payment,
                    bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": fct_sale.product_id,
                    "order_id": fct_sale.order_id,
                    "count": fct_sale.count,
                    "price": fct_sale.price,
                    "total_sum": fct_sale.total_sum,
                    "bonus_payment": fct_sale.bonus_payment,
                    "bonus_grant": fct_sale.bonus_grant
                },
            )


class FctProductSaleLoader:
    WF_KEY = "fct_product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctProductSalesOriginRepository(pg_origin)
        self.stg = FctProductSaleDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct_product_sales(self):
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
            load_queue = self.origin.list_fct_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct_product_sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct_product_sale in load_queue:
                self.stg.insert_fct_product_sale(conn, fct_product_sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
