from logging import Logger
from typing import Callable, Any

from lib.settings_repository import EtlSettingsRepository, EtlSetting
from lib.pg_connect import PgConnect
from lib.dict_util import json2str

class DdsLoader:
    BATCH_LIMIT = 1000

    def __init__(self,
                 reader: Any,
                 settings_repository: EtlSettingsRepository,
                 save_handler: Callable,
                 workflow_key: str,
                 stg_table_name: str,
                 pg_dest: PgConnect,
                 logger: Logger,)->None:
        self.reader = reader
        self.settings_repository = settings_repository
        self.save_handler = save_handler
        self.WF_KEY = workflow_key
        self.stg_table_name = stg_table_name
        self.pg_dest = pg_dest
        self.log = logger
        self.LAST_LOADED_ID_KEY = "last_loaded_id"


    def run_copy(self) -> None:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id = 0, workflow_key = self.WF_KEY, workflow_settings = {self.LAST_LOADED_ID_KEY: -1})
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"starting to lad from last checkpoint = {last_loaded_id}")

            if self.stg_table_name == 'stg.bonussystem_events':
                load_queue = self.reader.get_batch_bonuses(
                    threshold=last_loaded_id,
                    limit=self.BATCH_LIMIT,)
            else:
                load_queue = self.reader.get_batch(
                    table_name = self.stg_table_name,
                    threshold =  last_loaded_id,
                    limit = self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} documents to sync from STG")

            if not load_queue:
                self.log.info("No documents to sync from STG")
                return

            for row in load_queue:
               self.save_handler(conn, row)

            max_id = max([row[0] for row in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max_id
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn,
                wf_setting.workflow_key,
                wf_setting_json)
            self.log.info(f"Load finished. Last checkpoint updated to: {max_id}")
