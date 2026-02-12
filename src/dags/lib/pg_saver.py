from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Cursor


class PgSaver:

    def save_object(self, cursor: Cursor, table_name: str, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        cursor.execute(
            f"""
                    INSERT INTO {table_name}(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
            {
                "id": id,
                "val": str_val,
                "update_ts": update_ts
            }
        )
