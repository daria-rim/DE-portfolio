from datetime import datetime
from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, event_threshold: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                """,
                {
                    "threshold": event_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class EventDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO NOTHING;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


class EventLoader:
    WF_KEY = "example_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"Starting loading events from ID: {last_loaded}")
            
            load_queue = self.origin.list_events(last_loaded)
            self.log.info(f"Found {len(load_queue)} events to load.")
            
            if not load_queue:
                self.log.info("No new events to load.")
                return

            for event in load_queue:
                self.stg.insert_event(conn, event)

            new_last_loaded = max(event.id for event in load_queue)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = new_last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on ID: {new_last_loaded}")