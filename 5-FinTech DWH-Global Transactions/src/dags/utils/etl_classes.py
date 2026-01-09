import json
from typing import Optional, Dict, Any
from .dict_util import json2str

class EtlSetting:
    def __init__(self, id: int, workflow_key: str, workflow_settings: Dict):
        self.id = id
        self.workflow_key = workflow_key
        self.workflow_settings = workflow_settings

class VerticaEtlSettingsRepository:
    def __init__(self, schema_name: str = "VT251109CA442B__STAGING"):
        self.schema_name = schema_name

    def get_setting(self, conn, workflow_key: str) -> Optional['EtlSetting']:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT workflow_settings
                FROM {self.schema_name}.srv_wf_settings
                WHERE workflow_key = %s
            """, (workflow_key,))
            result = cur.fetchone()
            if result:
                settings_dict = json.loads(result[0])
                return EtlSetting(id=0, workflow_key=workflow_key, workflow_settings=settings_dict)
            return None

    def save_setting(self, conn, workflow_key: str, workflow_settings: Dict[str, Any]):
        settings_json = json2str(workflow_settings)
        with conn.cursor() as cur:
            cur.execute(f"""
                MERGE INTO {self.schema_name}.srv_wf_settings AS target
                USING (SELECT %s AS workflow_key, %s AS workflow_settings) AS source
                ON target.workflow_key = source.workflow_key
                WHEN MATCHED THEN
                    UPDATE SET workflow_settings = source.workflow_settings
                WHEN NOT MATCHED THEN
                    INSERT (workflow_key, workflow_settings)
                    VALUES (source.workflow_key, source.workflow_settings)
            """, (workflow_key, settings_json))