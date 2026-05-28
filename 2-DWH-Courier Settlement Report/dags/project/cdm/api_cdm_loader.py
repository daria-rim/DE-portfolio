from logging import Logger
from typing import List
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class CourierLedgerObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class CourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_courier_ledger(self) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            cur.execute(
                """
                    WITH courier_stats AS (
                        SELECT 
                            c.courier_id,
                            c.courier_name,
                            t.year as settlement_year,
                            t.month as settlement_month,
                            COUNT(DISTINCT o.id) as orders_count,
                            SUM(fct.total_sum) as orders_total_sum,
                            AVG(d.rate) as rate_avg,
                            SUM(d.tip_sum) as courier_tips_sum
                        FROM 
                            dds.fct_product_sales fct
                        JOIN dds.dm_orders o ON fct.order_id = o.id
                        JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                        JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                        JOIN dds.dm_deliveries d ON o.delivery_id = d.id
                        JOIN dds.dm_couriers c ON d.courier_id = c.id
                        GROUP BY 
                            c.courier_id, c.courier_name, t.year, t.month
                        HAVING COUNT(DISTINCT o.id) > 0
                    ),
                    courier_calculations AS (
                        SELECT 
                            courier_id,
                            courier_name,
                            settlement_year,
                            settlement_month,
                            orders_count,
                            orders_total_sum,
                            rate_avg,
                            courier_tips_sum,
                            orders_total_sum * 0.25 as order_processing_fee,
                            CASE 
                                WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100 * orders_count)
                                WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150 * orders_count)
                                WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175 * orders_count)
                                ELSE GREATEST(orders_total_sum * 0.10, 200 * orders_count)
                            END as courier_order_sum
                        FROM courier_stats
                    )
                    SELECT 
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                    FROM courier_calculations
                    ORDER BY settlement_year DESC, settlement_month DESC, courier_name
                """
            )
            objs = cur.fetchall()
        return objs

class CourierLedgerDestRepository:
    def insert_dm_courier_ledger(self, conn: Connection, report: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_courier_ledger (
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_reward_sum
                )
                VALUES (
                    %(courier_id)s,
                    %(courier_name)s,
                    %(settlement_year)s,
                    %(settlement_month)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(rate_avg)s,
                    %(order_processing_fee)s,
                    %(courier_order_sum)s,
                    %(courier_tips_sum)s,
                    %(courier_reward_sum)s
                )
                ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
                    courier_name = EXCLUDED.courier_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum
                """,
                report.dict()
            )

class CourierLedgerLoader:
    WF_KEY = "dm_courier_ledger_dds_to_cdm_workflow"
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierLedgerOriginRepository(pg_origin)
        self.stg = CourierLedgerDestRepository()
        self.log = log

    def load_dm_courier_ledger(self):
        with self.pg_dest.connection() as conn:
            load_queue = self.origin.list_dm_courier_ledger()
            self.log.info(f"Found {len(load_queue)} courier_ledger to load")
            
            if not load_queue:
                self.log.warning("No data found to load!")
                return
            
            for courier_ledger in load_queue:
                try:
                    self.stg.insert_dm_courier_ledger(conn, courier_ledger)
                except Exception as e:
                    self.log.error(f"Error inserting courier_ledger: {str(e)}")
                    raise
            
            self.log.info(f"Successfully loaded {len(load_queue)} courier_ledger")
