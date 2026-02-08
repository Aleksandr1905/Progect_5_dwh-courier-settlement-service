from datetime import datetime
from logging import Logger
from typing import Any, List, Dict
from lib.settings_repository import EtlSetting, EtlSettingsRepository
from lib.pg_saver import PgSaver
from lib.pg_connect import PgConnect
from lib.dict_util import json2str


class OrderSystemLoader:
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, reader: Any, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger, workflow_key: str, table_name: str) -> None:
        self.reader = reader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository()
        self.log = logger
        self.WF_KEY = workflow_key
        self.table_name = table_name

    def run_copy(self, limit: int = 10000) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.reader.get_data(last_loaded_ts, limit)
            self.log.info(f"Found {len(load_queue)} documents to sync from restaurants collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, self.table_name, str(d["_id"]), d["update_ts"], d)

                i += 1
                if i % 100 == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing restaurants.")

            new_ts = max([t["update_ts"] for t in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = new_ts.isoformat()

            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {new_ts}")

            return len(load_queue)
