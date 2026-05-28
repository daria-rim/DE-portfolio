from logging import Logger
from datetime import date
from typing import List
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class SettlementReportObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float

class SettlementReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_settlement_report(self) -> List[SettlementReportObj]:
        with self._db.client().cursor(row_factory=class_row(SettlementReportObj)) as cur:
            cur.execute(
                """
                SELECT 
                    r.restaurant_id,
                    r.restaurant_name,
                    t.date AS settlement_date,
                    COUNT(DISTINCT o.id) AS orders_count,
                    SUM(ps.total_sum) AS orders_total_sum,
                    SUM(ps.bonus_payment) AS orders_bonus_payment_sum,
                    SUM(ps.bonus_grant) AS orders_bonus_granted_sum,
                    SUM(ps.total_sum) * 0.25 AS order_processing_fee,
                    SUM(ps.total_sum) - SUM(ps.bonus_payment) - (SUM(ps.total_sum) * 0.25) AS restaurant_reward_sum
                FROM 
                    dds.fct_product_sales ps
                JOIN 
                    dds.dm_orders o ON ps.order_id = o.id
                JOIN 
                    dds.dm_restaurants r ON o.restaurant_id = r.id
                JOIN 
                    dds.dm_timestamps t ON o.timestamp_id = t.id
                WHERE 
                    o.order_status = 'CLOSED'
                GROUP BY 
                    r.restaurant_id,
                    r.restaurant_name,
                    t.date
                HAVING
                    COUNT(DISTINCT o.id) > 0  -- Только даты с заказами
                ORDER BY
                    t.date DESC,
                    r.restaurant_name
                """
            )
            objs = cur.fetchall()
        return objs

class SettlementReportDestRepository:
    def insert_dm_settlement_report(self, conn: Connection, report: SettlementReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report (
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    orders_count,
                    orders_total_sum,
                    orders_bonus_payment_sum,
                    orders_bonus_granted_sum,
                    order_processing_fee,
                    restaurant_reward_sum
                )
                VALUES (
                    %(restaurant_id)s,
                    %(restaurant_name)s,
                    %(settlement_date)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(orders_bonus_payment_sum)s,
                    %(orders_bonus_granted_sum)s,
                    %(order_processing_fee)s,
                    %(restaurant_reward_sum)s
                )
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
                """,
                report.dict()
            )

class SettlementReportLoader:
    WF_KEY = "dm_settlement_report_dds_to_cdm_workflow"
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SettlementReportOriginRepository(pg_origin)
        self.stg = SettlementReportDestRepository()
        self.log = log

    def load_dm_settlement_report(self):
        with self.pg_dest.connection() as conn:
            load_queue = self.origin.list_dm_settlement_report()
            self.log.info(f"Found {len(load_queue)} records to load")
            
            if not load_queue:
                self.log.warning("No data found to load!")
                return
            
            for record in load_queue:
                try:
                    self.stg.insert_dm_settlement_report(conn, record)
                except Exception as e:
                    self.log.error(f"Error inserting record: {str(e)}")
                    raise
            
            self.log.info(f"Successfully loaded {len(load_queue)} records")