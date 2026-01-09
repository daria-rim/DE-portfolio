from typing import Dict, List, Any
from datetime import datetime
import logging

from .pg_connect import ConnectionBuilder
from .vertica_connect import VerticaConnectionBuilder
from .dict_util import json2str
from .etl_classes import VerticaEtlSettingsRepository, EtlSetting


class PostgresReader:
    def __init__(self, pg_conn_id: str = 'postgres_source'):
        self.pg_conn_id = pg_conn_id
        self.pg_connect = ConnectionBuilder.pg_conn(pg_conn_id)

    def get_transactions(self, load_threshold: datetime, limit: int) -> List[Dict]:
        with self.pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt
                    FROM public.transactions
                    WHERE transaction_dt > %s
                    ORDER BY transaction_dt, operation_id
                    LIMIT %s
                """, (load_threshold, limit))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_currencies(self, load_threshold: datetime, limit: int) -> List[Dict]:
        with self.pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        date_update,
                        currency_code,
                        currency_code_with,
                        currency_with_div
                    FROM public.currencies
                    WHERE date_update > %s
                    ORDER BY date_update, currency_code
                    LIMIT %s
                """, (load_threshold, limit))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]


class VerticaSaverTransaction:
    def __init__(self, vertica_conn_id: str = 'vertica_dwh'):
        self.vertica_conn_id = vertica_conn_id
        self.vertica_connect = VerticaConnectionBuilder.vertica_conn(vertica_conn_id)
        
    def get_connection(self):
        return self.vertica_connect.client()

    def save_object(self, conn, val: Dict):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO VT251109CA442B__STAGING.transactions (
                    operation_id, account_number_from, account_number_to,
                    currency_code, country, status, transaction_type,
                    amount, transaction_dt
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    val['operation_id'],
                    val['account_number_from'],
                    val['account_number_to'],
                    val['currency_code'],
                    val['country'],
                    val['status'],
                    val['transaction_type'],
                    val['amount'],
                    val['transaction_dt']
                )
            )


class VerticaSaverCurrency:
    def __init__(self, vertica_conn_id: str = 'vertica_dwh'):
        self.vertica_conn_id = vertica_conn_id
        self.vertica_connect = VerticaConnectionBuilder.vertica_conn(vertica_conn_id)
        
    def get_connection(self):
        return self.vertica_connect.client()

    def save_object(self, conn, val: Dict):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO VT251109CA442B__STAGING.currencies (
                    date_update, currency_code, currency_code_with, currency_with_div
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    val['date_update'],
                    val['currency_code'],
                    val['currency_code_with'],
                    val['currency_with_div']
                )
            )


class BaseLoader:
    def __init__(self, pg_reader: PostgresReader, vertica_saver, logger):
        self.pg_reader = pg_reader
        self.vertica_saver = vertica_saver
        self.settings_repository = VerticaEtlSettingsRepository(
            schema_name="VT251109CA442B__STAGING"
        )
        self.log = logger


class TransactionsLoader(BaseLoader):
    _LOG_THRESHOLD = 1000
    _SESSION_LIMIT = 10000

    WF_KEY = "stg_transactions_postgres_to_vertica_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def run_copy(self) -> int:
        conn = self.vertica_saver.get_connection()
        
        try:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 9, 30).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load transactions from last checkpoint: {last_loaded_ts}")

            load_queue = self.pg_reader.get_transactions(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} transactions to sync")
            
            if not load_queue:
                self.log.info("No new transactions found")
                return 0

            i = 0
            for transaction in load_queue:
                self.vertica_saver.save_object(conn, transaction)
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} transactions of {len(load_queue)}")

            max_ts = max([t["transaction_dt"] for t in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max_ts.isoformat()
            
            self.settings_repository.save_setting(
                conn, 
                self.WF_KEY, 
                wf_setting.workflow_settings
            )

            self.log.info(f"Finished loading {len(load_queue)} transactions. Last checkpoint: {max_ts}")
            return len(load_queue)

        except Exception as e:
            self.log.error(f"Error loading transactions: {e}")
            raise
        finally:
            conn.close()


class CurrenciesLoader(BaseLoader):
    _LOG_THRESHOLD = 500
    _SESSION_LIMIT = 5000

    WF_KEY = "stg_currencies_postgres_to_vertica_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def run_copy(self) -> int:
        conn = self.vertica_saver.get_connection()
        
        try:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 9, 30).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load currencies from last checkpoint: {last_loaded_ts}")

            load_queue = self.pg_reader.get_currencies(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} currency rates to sync")
            
            if not load_queue:
                self.log.info("No new currency rates found")
                return 0

            i = 0
            for currency in load_queue:
                self.vertica_saver.save_object(conn, currency)
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} currency rates of {len(load_queue)}")

            max_ts = max([c["date_update"] for c in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max_ts.isoformat()
            
            self.settings_repository.save_setting(
                conn, 
                self.WF_KEY, 
                wf_setting.workflow_settings
            )

            self.log.info(f"Finished loading {len(load_queue)} currency rates. Last checkpoint: {max_ts}")
            return len(load_queue)

        except Exception as e:
            self.log.error(f"Error loading currencies: {e}")
            raise
        finally:
            conn.close()