from typing import Dict, Optional
import json
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class EtlSettingsRepository:
    def get_setting(self, cursor, etl_key: str) -> Optional[EtlSetting]:
        cursor.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
        obj = cursor.fetchone()

        if not obj:
            return None

        return EtlSetting(
            id=obj[0],
            workflow_key=obj[1],
            workflow_settings=obj[2]  # Превращаем строку JSON в словарь
        )

    def save_setting(self, cursor, workflow_key: str, workflow_settings: str) -> None:
            cursor.execute(
                """
                    INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )
