from lib import PgConnect
from typing import Any, List

class StgReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_batch(self, table_name: str, threshold: int, limit: int) -> List[Any]:
        with self._db.client().cursor() as cur:
            cur.execute(
                f"""
                    Select id, object_id, object_value, update_ts
                    from {table_name}
                    where id > %(threshold)s
                    order by id asc
                    limit %(limit)s""", {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()

    def get_batch_bonuses(self, threshold: int, limit: int) -> List[Any]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, event_ts, event_type, event_value
                from stg.bonussystem_events
                where id > %(threshold)s
                and event_type = 'bonus_transaction'
                order by id asc
                limit %(limit)s""", {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()